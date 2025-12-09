package com.example.libp2psmoke.domain

import android.os.SystemClock
import com.example.libp2psmoke.core.Constants
import com.example.libp2psmoke.core.DexError
import com.example.libp2psmoke.core.ExpiringLruCache
import com.example.libp2psmoke.core.SecureLogger
import com.example.libp2psmoke.core.expiringCache
import com.example.libp2psmoke.dex.BinanceApiClient
import com.example.libp2psmoke.dex.BinanceStreamClient
import com.example.libp2psmoke.dex.BinanceTicker
import com.example.libp2psmoke.dex.DexKlineBucket
import com.example.libp2psmoke.dex.OrderBookEntry
import com.example.libp2psmoke.dex.OkxMarketClient
import com.example.libp2psmoke.dex.toTradingViewJson
import com.example.libp2psmoke.dex.DexKlineScale
import com.example.libp2psmoke.dex.DEFAULT_DEX_SYMBOL
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.launch
import java.math.BigDecimal

/**
 * 行情数据 UseCase
 * 负责从多个数据源获取和缓存市场数据
 */
class MarketDataUseCase(
    private val binanceClient: BinanceApiClient = BinanceApiClient(),
    private val okxClient: OkxMarketClient = OkxMarketClient(),
    private val binanceStreamClient: BinanceStreamClient = BinanceStreamClient()
) {
    companion object {
        private const val TAG = "MarketDataUseCase"
    }
    
    // 缓存
    private val klineCache: ExpiringLruCache<String, List<DexKlineBucket>> = expiringCache(
        maxSize = Constants.Cache.MAX_KLINE_ENTRIES,
        expireAfterMinutes = Constants.Cache.KLINE_EXPIRE_MINUTES
    )
    
    private val klineJsonCache: ExpiringLruCache<String, String> = expiringCache(
        maxSize = Constants.Cache.MAX_KLINE_ENTRIES,
        expireAfterMinutes = Constants.Cache.KLINE_EXPIRE_MINUTES
    )
    
    // 延迟统计
    private val latencyStats = mutableMapOf<String, LatencyStats>()
    private val latencyLock = Any()
    
    // 数据源定义
    private val marketSources = listOf(
        MarketDataSource("Binance", ::fetchBinanceData, ::fetchBinanceDepth),
        MarketDataSource("OKX", ::fetchOkxData, ::fetchOkxDepth)
    )
    
    /**
     * 获取市场快照 (带故障转移)
     */
    suspend fun fetchMarketSnapshot(
        symbol: String,
        interval: String
    ): Result<MarketSnapshot> = coroutineScope {
        // 先检查缓存
        val cached = klineCache.get(interval)
        val cachedJson = klineJsonCache.get(interval)
        
        // 使用 Channel 接收最快的结果
        val channel = kotlinx.coroutines.channels.Channel<TimedResult<MarketSnapshot>>(kotlinx.coroutines.channels.Channel.UNLIMITED)
        val jobs = marketSources.map { source ->
            launch {
                val result = try {
                    measureLatency(source.name) {
                        source.snapshotFetcher(symbol, interval)
                    }
                } catch (t: Throwable) {
                    TimedResult(source.name, 0, Result.failure(DexError.NetworkError("Internal Error: ${t.message}")))
                }
                channel.send(result)
            }
        }

        val failures = mutableListOf<String>()
        var successResult: MarketSnapshot? = null

        // 等待结果，只要有一个成功就返回
        repeat(marketSources.size) {
            val timed = channel.receive()
            if (timed.result.isSuccess) {
                val snapshot = timed.result.getOrThrow().copy(
                    source = timed.sourceName,
                    latencyMs = timed.latencyMs,
                    degradedSources = failures // Note: failures might be incomplete here, but that's acceptable for speed
                )
                
                // 更新缓存
                if (snapshot.klines.isNotEmpty()) {
                    klineCache.put(interval, snapshot.klines)
                    klineJsonCache.put(interval, snapshot.klines.toTradingViewJson())
                }
                
                // Cancel other jobs as we have a winner
                jobs.forEach { it.cancel() }
                return@coroutineScope Result.success(snapshot)
            } else {
                failures.add(timed.sourceName)
            }
        }
        
        // 所有源失败，尝试返回缓存
        if (cached != null && cachedJson != null) {
            SecureLogger.w(TAG, "所有数据源失败，使用缓存")
            return@coroutineScope Result.success(
                MarketSnapshot(
                    ticker = cached.lastOrNull()?.toTicker() ?: emptyTicker(),
                    klines = cached,
                    klinesJson = cachedJson,
                    bids = emptyList(),
                    asks = emptyList(),
                    source = "缓存",
                    latencyMs = -1,
                    degradedSources = failures
                )
            )
        }
        
        // 最后的兜底：生成模拟数据 (Smoke Test Mode)
        SecureLogger.w(TAG, "所有数据源和缓存均失败，生成模拟数据")
        val mockKlines = generateMockKlines(interval)
        return@coroutineScope Result.success(
            MarketSnapshot(
                ticker = mockKlines.last().toTicker(),
                klines = mockKlines,
                klinesJson = mockKlines.toTradingViewJson(),
                bids = generateMockOrderBook(mockKlines.last().close, true),
                asks = generateMockOrderBook(mockKlines.last().close, false),
                source = "Mock (Network Failed)",
                latencyMs = 0,
                degradedSources = failures
            )
        )
    }
    
    /**
     * 获取深度数据 (带故障转移)
     */
    suspend fun fetchDepth(symbol: String): Result<DepthSnapshot> = coroutineScope {
        val results = marketSources.map { source ->
            async {
                measureLatency(source.name) {
                    source.depthFetcher(symbol)
                }
            }
        }
        
        val successes = mutableListOf<TimedResult<DepthSnapshot>>()
        val failures = mutableListOf<String>()
        
        results.forEach { deferred ->
            try {
                val timed = deferred.await()
                if (timed.result.isSuccess) {
                    successes.add(timed)
                } else {
                    failures.add(timed.sourceName)
                }
            } catch (e: Exception) {
                failures.add("Unknown")
            }
        }
        
        val fastest = successes.minByOrNull { it.latencyMs }
        
        if (fastest != null) {
            return@coroutineScope Result.success(
                fastest.result.getOrThrow().copy(
                    source = fastest.sourceName,
                    latencyMs = fastest.latencyMs,
                    degradedSources = failures
                )
            )
        }
        
        Result.failure(DexError.NetworkError("无法获取深度数据"))
    }
    
    /**
     * 获取缓存的 K线 JSON
     */
    fun getCachedKlineJson(interval: String): String? = klineJsonCache.get(interval)
    
    /**
     * 获取缓存的 K线数据
     */
    fun getCachedKlines(interval: String): List<DexKlineBucket>? = klineCache.get(interval)
    
    /**
     * 预热缓存
     */
    suspend fun prefetchIntervals(symbol: String, intervals: List<String>) = coroutineScope {
        intervals.forEach { interval ->
            launch(Dispatchers.IO) {
                try {
                    val klines = binanceClient.fetchKlines(symbol = symbol, interval = interval)
                    if (klines.isNotEmpty()) {
                        klineCache.put(interval, klines)
                        klineJsonCache.put(interval, klines.toTradingViewJson())
                        SecureLogger.d(TAG, "预热 $interval: ${klines.size} 条")
                    }
                } catch (e: Exception) {
                    SecureLogger.d(TAG, "预热 $interval 失败: ${e.message}")
                }
            }
        }
    }
    
    /**
     * 获取延迟统计
     */
    fun getLatencyStats(): List<SourceLatencyInfo> {
        return synchronized(latencyLock) {
            latencyStats.map { (name, stats) ->
                SourceLatencyInfo(
                    source = name,
                    lastLatencyMs = stats.lastLatencyMs,
                    avgLatencyMs = stats.averageLatencyMs,
                    successCount = stats.successCount,
                    failureCount = stats.failureCount
                )
            }
        }
    }
    
    // ═══════════════════════════════════════════════════════════════════
    // 私有方法
    // ═══════════════════════════════════════════════════════════════════
    
    private suspend fun fetchBinanceData(symbol: String, interval: String): Result<MarketSnapshot> = coroutineScope {
        try {
            val tickerDeferred = async { binanceClient.fetchTicker(symbol = symbol) }
            val klinesDeferred = async { binanceClient.fetchKlines(symbol = symbol, interval = interval) }
            val depthDeferred = async { binanceClient.fetchDepth(symbol = symbol) }
            
            val ticker = tickerDeferred.await()
            val klines = klinesDeferred.await()
            val (bids, asks) = depthDeferred.await()
            
            Result.success(MarketSnapshot(
                ticker = ticker,
                klines = klines,
                klinesJson = klines.toTradingViewJson(),
                bids = bids,
                asks = asks,
                source = "Binance"
            ))
        } catch (e: Exception) {
            Result.failure(DexError.from(e))
        }
    }
    
    private suspend fun fetchOkxData(symbol: String, interval: String): Result<MarketSnapshot> = coroutineScope {
        try {
            val tickerDeferred = async { okxClient.fetchTicker() }
            val klinesDeferred = async { okxClient.fetchKlines(interval = interval) }
            val depthDeferred = async { okxClient.fetchDepth() }
            
            val ticker = tickerDeferred.await()
            val klines = klinesDeferred.await()
            val (bids, asks) = depthDeferred.await()
            
            Result.success(MarketSnapshot(
                ticker = ticker,
                klines = klines,
                klinesJson = klines.toTradingViewJson(),
                bids = bids,
                asks = asks,
                source = "OKX"
            ))
        } catch (e: Exception) {
            Result.failure(DexError.from(e))
        }
    }
    
    private suspend fun fetchBinanceDepth(symbol: String): Result<DepthSnapshot> {
        return try {
            val ticker = binanceClient.fetchTicker(symbol = symbol)
            val (bids, asks) = binanceClient.fetchDepth(symbol = symbol)
            Result.success(DepthSnapshot(ticker, bids, asks, "Binance"))
        } catch (e: Exception) {
            Result.failure(DexError.from(e))
        }
    }
    
    private suspend fun fetchOkxDepth(symbol: String): Result<DepthSnapshot> {
        return try {
            val ticker = okxClient.fetchTicker()
            val (bids, asks) = okxClient.fetchDepth()
            Result.success(DepthSnapshot(ticker, bids, asks, "OKX"))
        } catch (e: Exception) {
            Result.failure(DexError.from(e))
        }
    }
    
    private suspend fun <T> measureLatency(
        sourceName: String,
        block: suspend () -> Result<T>
    ): TimedResult<T> {
        val start = SystemClock.elapsedRealtime()
        val result = try {
            block()
        } catch (e: Exception) {
            Result.failure(e)
        }
        val elapsed = SystemClock.elapsedRealtime() - start
        
        synchronized(latencyLock) {
            val stats = latencyStats.getOrPut(sourceName) { LatencyStats() }
            if (result.isSuccess) {
                stats.successCount++
                stats.totalLatencyMs += elapsed
            } else {
                stats.failureCount++
            }
            stats.lastLatencyMs = elapsed
        }
        
        return TimedResult(sourceName, elapsed, result)
    }
    
    private fun DexKlineBucket.toTicker(): BinanceTicker = BinanceTicker(
        lastPrice = close,
        priceChangePercent = BigDecimal.ZERO,
        highPrice = high,
        lowPrice = low,
        volumeBase = volumeBase,
        volumeQuote = BigDecimal.ZERO,
        closeTimeMs = windowStartMs + scale.seconds * 1000
    )
    
    // ═══════════════════════════════════════════════════════════════════
    // 数据类
    // ═══════════════════════════════════════════════════════════════════
    
    data class MarketSnapshot(
        val ticker: BinanceTicker,
        val klines: List<DexKlineBucket>,
        val klinesJson: String = "",
        val bids: List<OrderBookEntry>,
        val asks: List<OrderBookEntry>,
        val source: String,
        val latencyMs: Long = 0,
        val degradedSources: List<String> = emptyList()
    )
    
    data class DepthSnapshot(
        val ticker: BinanceTicker,
        val bids: List<OrderBookEntry>,
        val asks: List<OrderBookEntry>,
        val source: String,
        val latencyMs: Long = 0,
        val degradedSources: List<String> = emptyList()
    )
    
    data class SourceLatencyInfo(
        val source: String,
        val lastLatencyMs: Long,
        val avgLatencyMs: Double,
        val successCount: Int,
        val failureCount: Int
    )
    
    private data class MarketDataSource(
        val name: String,
        val snapshotFetcher: suspend (String, String) -> Result<MarketSnapshot>,
        val depthFetcher: suspend (String) -> Result<DepthSnapshot>
    )
    
    private data class TimedResult<T>(
        val sourceName: String,
        val latencyMs: Long,
        val result: Result<T>
    )
    
    private data class LatencyStats(
        var totalLatencyMs: Long = 0,
        var successCount: Int = 0,
        var failureCount: Int = 0,
        var lastLatencyMs: Long = 0
    ) {
        val averageLatencyMs: Double
            get() = if (successCount > 0) totalLatencyMs.toDouble() / successCount else -1.0
    }
    
    private fun generateMockKlines(interval: String): List<DexKlineBucket> {
        val scale = DexKlineScale.fromLabel(interval)
        val now = System.currentTimeMillis()
        val buckets = mutableListOf<DexKlineBucket>()
        var price = BigDecimal("65000")
        
        for (i in 0 until 100) {
            val time = now - (100 - i) * scale.seconds * 1000
            val change = BigDecimal(Math.random() - 0.5).multiply(BigDecimal("100"))
            val open = price
            val close = price.add(change)
            val high = open.max(close).add(BigDecimal(Math.random() * 50))
            val low = open.min(close).subtract(BigDecimal(Math.random() * 50))
            val volume = BigDecimal(Math.random() * 10)
            
            buckets.add(
                DexKlineBucket(
                    symbol = DEFAULT_DEX_SYMBOL,
                    scale = scale,
                    windowStartMs = time,
                    open = open,
                    high = high,
                    low = low,
                    close = close,
                    volumeBase = volume,
                    tradeCount = (Math.random() * 100).toInt()
                )
            )
            price = close
        }
        return buckets
    }
    
    private fun generateMockOrderBook(midPrice: BigDecimal, isBid: Boolean): List<OrderBookEntry> {
        val entries = mutableListOf<OrderBookEntry>()
        var currentPrice = midPrice
        var cumulative = BigDecimal.ZERO
        
        for (i in 0 until 10) {
            val spread = BigDecimal(i + 1).multiply(BigDecimal("10"))
            currentPrice = if (isBid) midPrice.subtract(spread) else midPrice.add(spread)
            val qty = BigDecimal(Math.random() * 2)
            cumulative = cumulative.add(qty)
            entries.add(OrderBookEntry(currentPrice, qty, cumulative))
        }
        return entries
    }
}

private fun emptyTicker() = BinanceTicker(
    lastPrice = BigDecimal.ZERO,
    priceChangePercent = BigDecimal.ZERO,
    highPrice = BigDecimal.ZERO,
    lowPrice = BigDecimal.ZERO,
    volumeBase = BigDecimal.ZERO,
    volumeQuote = BigDecimal.ZERO,
    closeTimeMs = 0
)

