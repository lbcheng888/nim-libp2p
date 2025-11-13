package com.example.libp2psmoke.viewmodel

import android.annotation.SuppressLint
import android.app.Application
import android.content.Context
import android.net.wifi.WifiManager
import android.os.Build
import android.os.SystemClock
import android.provider.Settings
import android.util.Base64
import android.util.Log
import androidx.lifecycle.AndroidViewModel
import androidx.lifecycle.viewModelScope
import com.example.libp2psmoke.BuildConfig
import com.example.libp2psmoke.dex.BINANCE_INTERVALS
import com.example.libp2psmoke.dex.BINANCE_SPOT_STREAM
import com.example.libp2psmoke.dex.BINANCE_SPOT_SYMBOL
import com.example.libp2psmoke.dex.BinanceApiClient
import com.example.libp2psmoke.dex.BinanceStreamClient
import com.example.libp2psmoke.dex.BinanceTicker
import com.example.libp2psmoke.dex.BscTransferRequest
import com.example.libp2psmoke.dex.BtcRpcConfig
import com.example.libp2psmoke.dex.BtcSwapRequest
import com.example.libp2psmoke.dex.DexRepository
import com.example.libp2psmoke.dex.DexKlineBucket
import com.example.libp2psmoke.dex.DexKlineScale
import com.example.libp2psmoke.dex.DEFAULT_DEX_SYMBOL
import com.example.libp2psmoke.dex.OrderBookEntry
import com.example.libp2psmoke.dex.OkxMarketClient
import com.example.libp2psmoke.model.AttachmentKind
import com.example.libp2psmoke.model.DirectMessage
import com.example.libp2psmoke.model.FeedAttachment
import com.example.libp2psmoke.model.FeedEntry
import com.example.libp2psmoke.model.LanEndpoint
import com.example.libp2psmoke.model.MarketSourceLatency
import com.example.libp2psmoke.model.LivestreamFrame
import com.example.libp2psmoke.model.NodeUiState
import com.example.libp2psmoke.model.PeerState
import com.example.libp2psmoke.native.NimBridge
import java.io.File
import java.math.BigDecimal
import java.math.BigInteger
import java.math.RoundingMode
import java.net.Inet4Address
import java.net.Inet6Address
import java.net.NetworkInterface
import java.util.Locale
import java.util.UUID
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.coroutineScope
import org.json.JSONArray
import org.json.JSONException
import org.json.JSONObject
import org.web3j.crypto.Credentials
import kotlin.text.Charsets
import kotlin.concurrent.thread

class NimNodeViewModel(application: Application) : AndroidViewModel(application) {
    private val appContext = application.applicationContext
    private val dataDir: File = File(appContext.filesDir, "nimlibp2p_node")

    private val _state = MutableStateFlow(NodeUiState())
    val state: StateFlow<NodeUiState> = _state

    private var handle: Long = 0L
   private var pollJob: Job? = null
   private var refreshJob: Job? = null
   private var derivedIdentityPeer: String? = null
   private var multicastLock: WifiManager.MulticastLock? = null
    private var connectivityJob: Job? = null
    private val lastConnectAttempt: MutableMap<String, Long> = mutableMapOf()
    private var binanceDepthJob: Job? = null
    private var binanceReconnectJob: Job? = null
    private val bootstrapMultiaddrs: List<String> = parseConfiguredMultiaddrs(BuildConfig.LIBP2P_BOOTSTRAP_PEERS)
    private val relayMultiaddrs: List<String> = parseConfiguredMultiaddrs(BuildConfig.LIBP2P_RELAY_PEERS)
    private val dexRepository = DexRepository()
    private val binanceClient = BinanceApiClient()
    private val okxClient = OkxMarketClient()
    private val binanceStreamClient = BinanceStreamClient()
    private val binanceRestSymbol = BINANCE_SPOT_SYMBOL
    private val binanceStreamSymbol = BINANCE_SPOT_STREAM
    private val embeddedBootstrap: List<DexKlineBucket>? = loadBootstrapKlines()
    private val marketSources = listOf(
        MarketDataSource(
            name = "Binance",
            snapshotFetcher = { interval -> fetchBinanceSnapshot(interval) },
            depthFetcher = { fetchBinanceDepthOnly() }
        ),
        MarketDataSource(
            name = "OKX",
            snapshotFetcher = { interval -> fetchOkxSnapshot(interval) },
            depthFetcher = { fetchOkxDepthOnly() }
        )
    )
    private val latencyStats = mutableMapOf<String, LatencyAccumulator>()
    private val latencyLock = Any()
    private var bootstrapSeeded = false
    private val dexRpcDefaults = parseDexConfig(BuildConfig.DEX_SETTLEMENT_RPCS)
    private val dexContractDefaults = parseDexConfig(BuildConfig.DEX_USDC_CONTRACTS)
    private val defaultBtcRpcUrlValue = dexRpcDefaults["btcTestnet"] ?: "http://127.0.0.1:18332"
    private val defaultBscRpcUrlValue = dexRpcDefaults["bscTestnet"] ?: "https://bsc-testnet-dataseed.bnbchain.org"
    private val defaultBscContractValue = dexContractDefaults["bscTestnet"] ?: "0xE4140d73e9F09C5f783eC2BD8976cd8256A69AD0"
    private val usdcDecimals = 18

    companion object {
        private const val TAG = "NimNodeViewModel"
        private const val PLATFORM_IDENTITY_SALT = "android-hardware"
        private const val MAX_MESSAGES_PER_PEER = 200
        private const val MAX_FEED_ITEMS = 128
        private const val MAX_LIVE_FRAMES = 128
        private const val MAX_BINANCE_KLINES = 720
        private const val DEMO_IMAGE_BASE64 =
            "iVBORw0KGgoAAAANSUhEUgAAAA4AAAAOCAYAAAAfSC3RAAAAH0lEQVR42mNgGAUEA/n///8fQQ0w" +
                "GoaBEYTEAAGbCAG8LGWErwAAAABJRU5ErkJggg=="
        private const val DEMO_IMAGE_MIME = "image/png"
        private const val BASE64_ALPHABET =
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/="
        private const val BASE64_TRIMMED_SENTINEL = "...<trimmed>"
        private val WHITESPACE_REGEX = "\\s+".toRegex()
        private val FEED_ARRAY_KEYS = arrayOf("media", "attachments", "assets", "content")
        private val FEED_IMAGE_KEYS = arrayOf("cover", "thumbnail", "image")
        private val FEED_SUMMARY_KEYS = arrayOf("body", "summary", "text", "caption", "title")
        private val FEED_URL_KEYS = arrayOf("url", "uri", "href", "src")
        private val FEED_DATA_KEYS = arrayOf("data", "base64", "payload", "blob", "bytes")
    }

    init {
        if (!dataDir.exists()) {
            dataDir.mkdirs()
        }
        seedFromBootstrap()
        viewModelScope.launch {
            dexRepository.swaps().collect { swaps ->
                _state.update { it.copy(dexSwaps = swaps) }
            }
        }
        viewModelScope.launch {
            dexRepository.klines().collect { buckets ->
                _state.update { it.copy(dexKlines = buckets) }
            }
        }
        viewModelScope.launch {
            dexRepository.btcBalance().collect { balance ->
                _state.update { it.copy(btcBalance = balance) }
            }
        }
        viewModelScope.launch {
            bootstrapBinanceFeeds()
        }
        viewModelScope.launch(Dispatchers.Default) {
            startNode()
        }
    }

    override fun onCleared() {
        super.onCleared()
        shutdown()
    }

    fun shutdown() {
        val poll = pollJob
        val refresh = refreshJob
        pollJob = null
        refreshJob = null
        connectivityJob?.cancel()
        connectivityJob = null
        runBlocking {
            try {
                poll?.cancelAndJoin()
            } catch (t: Throwable) {
                Log.w(TAG, "Poll job cancellation failed", t)
            }
            try {
                refresh?.cancelAndJoin()
            } catch (t: Throwable) {
                Log.w(TAG, "Refresh job cancellation failed", t)
            }
        }
        val currentHandle = handle
        if (currentHandle != 0L) {
            try {
                NimBridge.stopNode(currentHandle)
            } catch (t: Throwable) {
                Log.w(TAG, "stopNode failed", t)
            } finally {
                NimBridge.freeNode(currentHandle)
            }
        }
        releaseMulticastLock()
        binanceDepthJob?.cancel()
        binanceDepthJob = null
        binanceReconnectJob?.cancel()
        binanceReconnectJob = null
        binanceStreamClient.stop()
        handle = 0L
        _state.update { it.copy(running = false) }
        lastConnectAttempt.clear()
    }

    fun sendDirectMessage(peerId: String, body: String) {
        val trimmed = body.trim()
        if (trimmed.isEmpty()) return
        val currentHandle = handle
        if (currentHandle == 0L) return
        val localPeer = state.value.localPeerId ?: ""
        val messageId = UUID.randomUUID().toString()
        val envelope = JSONObject().apply {
            put("op", "dm")
            put("mid", messageId)
            put("from", localPeer)
            put("body", trimmed)
            put("timestamp_ms", System.currentTimeMillis())
            put("ackRequested", true)
        }.toString()
        viewModelScope.launch(Dispatchers.Default) {
            val success = NimBridge.sendDirect(
                currentHandle,
                peerId,
                envelope.toByteArray(Charsets.UTF_8)
            )
            val message = DirectMessage(
                peerId = peerId,
                messageId = messageId,
                fromSelf = true,
                body = trimmed,
                timestampMs = System.currentTimeMillis(),
                transport = "nim-direct",
                acked = success
            )
            recordMessage(message)
            if (!success) {
                val err = NimBridge.lastDirectError(currentHandle)
                updateError(err ?: "Direct message delivery failed")
            }
        }
    }

    fun connectPeer(peerId: String) {
        val trimmed = peerId.trim()
        val currentHandle = handle
        if (trimmed.isEmpty() || currentHandle == 0L) return
        if (isPeerAlreadyConnected(trimmed)) {
            Log.i(TAG, "connectPeer: peer=$trimmed already connected")
            return
        }
        viewModelScope.launch(Dispatchers.Default) {
            val (success, message) = connectPeerInternal(trimmed)
            if (!success) {
                updateError(message ?: "Connect peer failed")
            }
        }
    }

    fun connectPeerForChat(peerId: String, onResult: (Boolean, String?) -> Unit) {
        val trimmed = peerId.trim()
        val currentHandle = handle
        if (currentHandle == 0L || trimmed.isEmpty()) {
            onResult(false, "Node not ready")
            return
        }
        if (isPeerAlreadyConnected(trimmed)) {
            onResult(true, null)
            return
        }
        viewModelScope.launch(Dispatchers.Default) {
            val (success, message) = connectPeerInternal(trimmed)
            if (!success) {
                updateError(message ?: "Connect peer failed")
            }
            withContext(Dispatchers.Main) {
                onResult(success, message)
            }
        }
    }

    fun defaultBtcRpcUrl(): String = defaultBtcRpcUrlValue

    fun defaultBscRpcUrl(): String = defaultBscRpcUrlValue

    fun defaultBscContract(): String = defaultBscContractValue

    fun binanceIntervals(): List<String> = BINANCE_INTERVALS

    private suspend fun bootstrapBinanceFeeds() {
        val interval = state.value.binanceInterval
        try {
            refreshBinanceSnapshot(interval)
        } catch (err: Exception) {
            Log.w(TAG, "Initial Binance snapshot failed", err)
        }
        startBinanceStream(interval)
        ensureBinanceDepthLoop()
    }

    private suspend fun refreshBinanceSnapshot(interval: String) {
        _state.update { it.copy(binanceLoading = true, binanceInterval = interval) }
        try {
            val snapshot = fetchSnapshotFromSources(interval)
            val warning = warningForDegraded(snapshot.source, snapshot.degradedSources)
            _state.update {
                it.copy(
                    binanceTicker = snapshot.ticker,
                    binanceKlines = snapshot.klines,
                    binanceInterval = interval,
                    binanceLoading = false,
                    binanceBids = snapshot.bids,
                    binanceAsks = snapshot.asks,
                    binanceLastUpdateMs = snapshot.ticker.closeTimeMs.takeIf { ts -> ts > 0 } ?: System.currentTimeMillis(),
                    marketSource = snapshot.source,
                    marketLatencyMs = snapshot.latencyMs,
                    binanceError = warning
                )
            }
        } catch (err: Exception) {
            _state.update { it.copy(binanceLoading = false, binanceError = err.userFacingMessage()) }
            throw err
        }
    }

    private suspend fun refreshBinanceDepth() {
        try {
            val depth = fetchDepthFromSources()
            val warning = warningForDegraded(depth.source, depth.degradedSources)
            _state.update {
                it.copy(
                    binanceTicker = depth.ticker,
                    binanceBids = depth.bids,
                    binanceAsks = depth.asks,
                    binanceLastUpdateMs = System.currentTimeMillis(),
                    marketSource = depth.source,
                    marketLatencyMs = depth.latencyMs,
                    binanceError = warning
                )
            }
        } catch (err: Exception) {
            Log.w(TAG, "Binance depth refresh failed", err)
            _state.update { it.copy(binanceError = err.userFacingMessage()) }
        }
    }

    private fun ensureBinanceDepthLoop() {
        if (binanceDepthJob?.isActive == true) return
        binanceDepthJob = viewModelScope.launch {
            while (isActive) {
                delay(15_000)
                try {
                    refreshBinanceDepth()
                } catch (err: Exception) {
                    Log.w(TAG, "Binance depth refresh failed", err)
                }
            }
        }
    }

    fun selectBinanceInterval(interval: String) {
        if (interval == state.value.binanceInterval) return
        _state.update { it.copy(binanceInterval = interval, binanceLoading = true) }
        viewModelScope.launch {
            try {
                refreshBinanceSnapshot(interval)
            } catch (err: Exception) {
                Log.w(TAG, "Binance interval switch failed", err)
            } finally {
                startBinanceStream(interval)
            }
        }
    }

    private fun startBinanceStream(interval: String) {
        binanceReconnectJob?.cancel()
        _state.update { it.copy(binanceStreamConnected = false) }
        binanceStreamClient.start(
            symbol = binanceStreamSymbol,
            interval = interval,
            listener = object : BinanceStreamClient.Listener {
            override fun onOpen() {
                viewModelScope.launch {
                    _state.update { it.copy(binanceStreamConnected = true, binanceError = null) }
                }
            }

            override fun onKline(bucket: DexKlineBucket, isFinal: Boolean, eventTimeMs: Long) {
                viewModelScope.launch {
                    handleBinanceStreamBucket(bucket, eventTimeMs)
                }
            }

            override fun onClosed() {
                viewModelScope.launch {
                    _state.update { it.copy(binanceStreamConnected = false) }
                }
            }

            override fun onFailure(throwable: Throwable) {
                viewModelScope.launch {
                    _state.update {
                        it.copy(
                            binanceStreamConnected = false,
                            binanceError = throwable.userFacingMessage()
                        )
                    }
                }
                scheduleBinanceReconnect()
            }
        })
    }

    private fun scheduleBinanceReconnect() {
        binanceReconnectJob?.cancel()
        binanceReconnectJob = viewModelScope.launch {
            delay(2_000)
            startBinanceStream(state.value.binanceInterval)
        }
    }

    private fun handleBinanceStreamBucket(bucket: DexKlineBucket, eventTimeMs: Long) {
        _state.update { current ->
            val merged = mergeBinanceBuckets(current.binanceKlines, bucket)
            val ticker = (current.binanceTicker ?: bucket.toTickerPrototype()).copy(
                lastPrice = bucket.close,
                closeTimeMs = bucket.windowStartMs + bucket.scale.seconds * 1000
            )
            current.copy(
                binanceKlines = merged,
                binanceTicker = ticker,
                binanceLastUpdateMs = if (eventTimeMs > 0) eventTimeMs else System.currentTimeMillis(),
                binanceError = null
            )
        }
    }

    private fun mergeBinanceBuckets(
        existing: List<DexKlineBucket>,
        bucket: DexKlineBucket
    ): List<DexKlineBucket> =
        (listOf(bucket) + existing)
            .distinctBy { it.windowStartMs }
            .sortedByDescending { it.windowStartMs }
            .take(MAX_BINANCE_KLINES)

    private fun DexKlineBucket.toTickerPrototype(): BinanceTicker =
        BinanceTicker(
            lastPrice = close,
            priceChangePercent = BigDecimal.ZERO,
            highPrice = high,
            lowPrice = low,
            volumeBase = volumeBase,
            volumeQuote = BigDecimal.ZERO,
            closeTimeMs = windowStartMs + scale.seconds * 1000
        )

    private fun Throwable.userFacingMessage(): String =
        message?.takeIf { it.isNotBlank() } ?: "无法连接行情服务"

    private fun warningForDegraded(active: String, degraded: List<String>): String? {
        if (degraded.isEmpty()) return null
        val unavailable = degraded.joinToString()
        return "$unavailable 行情源不可用，当前使用 $active"
    }

    private suspend fun fetchSnapshotFromSources(interval: String): MarketSnapshot {
        val results = coroutineScope {
            marketSources.map { source ->
                async {
                    measureSource(source.name) {
                        source.snapshotFetcher(interval)
                    }
                }
            }
        }
        val successes = mutableListOf<MarketSnapshot>()
        val failures = mutableListOf<String>()
        val errors = mutableListOf<Throwable>()
        results.forEach { deferred ->
            val timed = deferred.await()
            val outcome = timed.result
            if (outcome.isSuccess) {
                val snapshot = outcome.getOrThrow().copy(
                    source = timed.source,
                    latencyMs = timed.latencyMs
                )
                successes += snapshot
            } else {
                timed.result.exceptionOrNull()?.let { errors += it }
                failures += timed.source
            }
        }
        successes.minByOrNull { it.latencyMs }?.let { fastest ->
            val degraded = (fastest.degradedSources + failures)
                .filter { it != fastest.source }
                .distinct()
            return fastest.copy(degradedSources = degraded)
        }
        val bootstrap = embeddedBootstrap?.takeIf { it.isNotEmpty() }?.let { buckets ->
            MarketSnapshot(
                ticker = buckets.last().toTickerPrototype(),
                klines = buckets,
                bids = emptyList(),
                asks = emptyList(),
                source = "离线缓存",
                latencyMs = -1L,
                degradedSources = marketSources.map { it.name }
            )
        }
        if (bootstrap != null) {
            _state.update { it.copy(binanceError = "网络不可用，使用离线行情") }
            return bootstrap
        }
        val reason = errors.firstOrNull()?.userFacingMessage() ?: "无行情数据源"
        throw IllegalStateException(reason)
    }

    private suspend fun fetchDepthFromSources(): DepthSnapshot {
        val results = coroutineScope {
            marketSources.map { source ->
                async {
                    measureSource(source.name) {
                        source.depthFetcher()
                    }
                }
            }
        }
        val successes = mutableListOf<DepthSnapshot>()
        val failures = mutableListOf<String>()
        val errors = mutableListOf<Throwable>()
        results.forEach { deferred ->
            val timed = deferred.await()
            val outcome = timed.result
            if (outcome.isSuccess) {
                val snapshot = outcome.getOrThrow().copy(
                    source = timed.source,
                    latencyMs = timed.latencyMs
                )
                successes += snapshot
            } else {
                timed.result.exceptionOrNull()?.let { errors += it }
                failures += timed.source
            }
        }
        successes.minByOrNull { it.latencyMs }?.let { fastest ->
            val degraded = (fastest.degradedSources + failures)
                .filter { it != fastest.source }
                .distinct()
            return fastest.copy(degradedSources = degraded)
        }
        val reason = errors.firstOrNull()?.userFacingMessage() ?: "无行情数据源"
        throw IllegalStateException(reason)
    }

    private suspend fun fetchBinanceSnapshot(interval: String): MarketSnapshot {
        val ticker = binanceClient.fetchTicker(symbol = binanceRestSymbol)
        val klines = binanceClient.fetchKlines(symbol = binanceRestSymbol, interval = interval)
        val (bids, asks) = binanceClient.fetchDepth(symbol = binanceRestSymbol)
        return MarketSnapshot(ticker, klines, bids, asks, "Binance")
    }

    private suspend fun fetchOkxSnapshot(interval: String): MarketSnapshot {
        val ticker = okxClient.fetchTicker()
        val klines = okxClient.fetchKlines(interval = interval)
        val (bids, asks) = okxClient.fetchDepth()
        return MarketSnapshot(ticker, klines, bids, asks, "OKX")
    }

    private suspend fun fetchBinanceDepthOnly(): DepthSnapshot {
        val ticker = binanceClient.fetchTicker(symbol = binanceRestSymbol)
        val (bids, asks) = binanceClient.fetchDepth(symbol = binanceRestSymbol)
        return DepthSnapshot(ticker, bids, asks, "Binance")
    }

    private suspend fun fetchOkxDepthOnly(): DepthSnapshot {
        val ticker = okxClient.fetchTicker()
        val (bids, asks) = okxClient.fetchDepth()
        return DepthSnapshot(ticker, bids, asks, "OKX")
    }

    private fun seedFromBootstrap() {
        if (bootstrapSeeded) return
        val buckets = embeddedBootstrap ?: return
        if (buckets.isEmpty()) return
        bootstrapSeeded = true
        val ticker = buckets.last().toTickerPrototype()
        _state.update { current ->
            if (current.binanceKlines.isNotEmpty()) {
                current
            } else {
                current.copy(
                    binanceKlines = buckets,
                    binanceTicker = ticker,
                    binanceLastUpdateMs = ticker.closeTimeMs,
                    marketSource = "离线缓存",
                    marketLatencyMs = -1L,
                    binanceError = "正在等待实时行情…"
                )
            }
        }
    }

    private fun loadBootstrapKlines(): List<DexKlineBucket>? =
        try {
            val text = appContext.assets.open("kline_bootstrap.json").bufferedReader().use { it.readText() }
            val json = JSONArray(text)
            val buckets = mutableListOf<DexKlineBucket>()
            for (i in 0 until json.length()) {
                val entry = json.getJSONObject(i)
                val timeSeconds = entry.optLong("time")
                val open = entry.asBigDecimal("open")
                val high = entry.asBigDecimal("high")
                val low = entry.asBigDecimal("low")
                val close = entry.asBigDecimal("close")
                val volume = entry.asBigDecimal("volume")
                buckets.add(
                    DexKlineBucket(
                        symbol = DEFAULT_DEX_SYMBOL,
                        scale = DexKlineScale.ONE_MINUTE,
                        windowStartMs = timeSeconds * 1000,
                        open = open,
                        high = high,
                        low = low,
                        close = close,
                        volumeBase = volume,
                        tradeCount = 0
                    )
                )
            }
            buckets.sortedByDescending { it.windowStartMs }
        } catch (err: Exception) {
            Log.w(TAG, "Failed to load bootstrap klines", err)
            null
        }

    private data class MarketSnapshot(
        val ticker: BinanceTicker,
        val klines: List<DexKlineBucket>,
        val bids: List<OrderBookEntry>,
        val asks: List<OrderBookEntry>,
        val source: String,
        val latencyMs: Long = 0L,
        val degradedSources: List<String> = emptyList()
    )

    private data class DepthSnapshot(
        val ticker: BinanceTicker,
        val bids: List<OrderBookEntry>,
        val asks: List<OrderBookEntry>,
        val source: String,
        val latencyMs: Long = 0L,
        val degradedSources: List<String> = emptyList()
    )

    private data class MarketDataSource(
        val name: String,
        val snapshotFetcher: suspend (String) -> MarketSnapshot,
        val depthFetcher: suspend () -> DepthSnapshot
    )

    private data class LatencyAccumulator(
        var totalLatencyMs: Long = 0L,
        var successCount: Int = 0,
        var failureCount: Int = 0,
        var lastLatencyMs: Long = 0L,
        var lastUpdatedMs: Long = 0L
    ) {
        fun toModel(source: String): MarketSourceLatency =
            MarketSourceLatency(
                source = source,
                lastLatencyMs = lastLatencyMs,
                averageLatencyMs = if (successCount > 0) totalLatencyMs.toDouble() / successCount else -1.0,
                successCount = successCount,
                failureCount = failureCount,
                lastUpdatedMs = lastUpdatedMs
            )
    }

    private data class TimedResult<T>(
        val source: String,
        val latencyMs: Long,
        val result: Result<T>
    )

    private suspend fun <T> measureSource(
        source: String,
        block: suspend () -> T
    ): TimedResult<T> {
        val start = SystemClock.elapsedRealtime()
        return try {
            val value = block()
            val elapsed = (SystemClock.elapsedRealtime() - start).coerceAtLeast(0L)
            recordLatencySample(source, elapsed, true)
            TimedResult(source, elapsed, Result.success(value))
        } catch (err: Throwable) {
            val elapsed = (SystemClock.elapsedRealtime() - start).coerceAtLeast(0L)
            recordLatencySample(source, elapsed, false)
            TimedResult(source, elapsed, Result.failure(err))
        }
    }

    private fun recordLatencySample(source: String, latencyMs: Long, success: Boolean) {
        val now = System.currentTimeMillis()
        val snapshot = synchronized(latencyLock) {
            val stats = latencyStats.getOrPut(source) { LatencyAccumulator() }
            if (success) {
                stats.successCount += 1
                stats.totalLatencyMs += latencyMs
            } else {
                stats.failureCount += 1
            }
            stats.lastLatencyMs = latencyMs
            stats.lastUpdatedMs = now
            latencyStats
                .map { (name, acc) -> acc.toModel(name) }
                .sortedBy { it.source }
        }
        _state.update { it.copy(marketLatencies = snapshot) }
    }

    private fun JSONObject.asBigDecimal(key: String): BigDecimal =
        optString(key)?.takeIf { it.isNotBlank() }?.toBigDecimalOrNull() ?: BigDecimal.ZERO

    fun previewBtcFromUsdc(amountText: String): String {
        val ticker = state.value.binanceTicker ?: return ""
        val amount = amountText.trim().toBigDecimalOrNull() ?: return ""
        if (amount <= BigDecimal.ZERO || ticker.lastPrice <= BigDecimal.ZERO) return ""
        val btc = amount.divide(ticker.lastPrice, 8, RoundingMode.HALF_UP)
        return btc.stripTrailingZeros().toPlainString()
    }

    fun submitBtcSettlement(
        orderId: String,
        amountSats: Long,
        targetAddress: String,
        rpcUrl: String,
        rpcUser: String,
        rpcPassword: String
    ) {
        val trimmedOrder = orderId.trim()
        val trimmedAddress = targetAddress.trim()
        if (trimmedOrder.isEmpty()) {
            updateError("订单号不能为空")
            return
        }
        if (amountSats <= 0) {
            updateError("BTC 数量需大于 0")
            return
        }
        if (trimmedAddress.isEmpty()) {
            updateError("目标地址不能为空")
            return
        }
        val config = BtcRpcConfig(
            url = rpcUrl.ifBlank { defaultBtcRpcUrlValue },
            username = rpcUser.trim(),
            password = rpcPassword
        )
        val request = BtcSwapRequest(
            orderId = trimmedOrder,
            amountSats = amountSats,
            targetAddress = trimmedAddress
        )
        viewModelScope.launch(Dispatchers.Default) {
            try {
                dexRepository.submitBtcSwap(config, request)
            } catch (err: Exception) {
                val reason = err.message ?: "unknown error"
                updateError("BTC 结算失败: $reason")
            }
        }
    }

    fun submitBscUsdcTransfer(
        orderId: String,
        amountText: String,
        privateKey: String,
        rpcUrl: String,
        contractAddress: String
    ) {
        val trimmedOrder = orderId.trim()
        if (trimmedOrder.isEmpty()) {
            updateError("订单号不能为空")
            return
        }
        val amount = parseTokenAmount(amountText, usdcDecimals)
        if (amount == null) {
            updateError("无效的 USDC 数量")
            return
        }
        val trimmedKey = privateKey.trim()
        if (trimmedKey.isEmpty()) {
            updateError("私钥不能为空")
            return
        }
        val derivedAddress = deriveBscAddress(trimmedKey)
        if (derivedAddress.isNullOrBlank()) {
            updateError("无法根据私钥推导 BSC 地址")
            return
        }
        _state.update { it.copy(bscWalletAddress = derivedAddress) }
        val request = BscTransferRequest(
            orderId = trimmedOrder,
            rpcUrl = rpcUrl.ifBlank { defaultBscRpcUrlValue },
            contractAddress = contractAddress.ifBlank { defaultBscContractValue },
            privateKey = trimmedKey,
            toAddress = derivedAddress,
            amount = amount,
            decimals = usdcDecimals
        )
        viewModelScope.launch(Dispatchers.Default) {
            try {
                dexRepository.submitBscTransfer(request)
            } catch (err: Exception) {
                val reason = err.message ?: "unknown error"
                updateError("BSC 转账失败: $reason")
            }
        }
    }

    fun bridgeUsdcToBtc(
        bscOrderId: String,
        usdcAmountText: String,
        bscPrivateKey: String,
        bscRpcUrl: String,
        bscContract: String,
        btcOrderId: String,
        btcTargetAddress: String,
        btcRpcUrl: String,
        btcRpcUser: String,
        btcRpcPassword: String
    ) {
        val normalizedBscOrder = bscOrderId.ifBlank { "swap-${System.currentTimeMillis()}" }
        val normalizedBtcOrder = btcOrderId.ifBlank { "btc-${System.currentTimeMillis()}" }
        val trimmedKey = bscPrivateKey.trim()
        if (trimmedKey.isEmpty()) {
            updateError("请提供 BSC 私钥")
            return
        }
        val derivedAddress = deriveBscAddress(trimmedKey)
        if (derivedAddress.isNullOrBlank()) {
            updateError("无法根据私钥推导 BSC 钱包地址")
            return
        }
        val btcAddress = btcTargetAddress.trim()
        if (btcAddress.isEmpty()) {
            updateError("请输入 BTC 目标地址")
            return
        }
        val usdcAmount = parseTokenAmount(usdcAmountText, usdcDecimals)
        if (usdcAmount == null) {
            updateError("无效的 USDC 数量")
            return
        }
        val btcAmountSats = satsFromBinancePrice(usdcAmount)
            ?: dexRepository.estimateBtcFromUsdc(usdcAmount, usdcDecimals)
        if (btcAmountSats <= 0L) {
            updateError("USDC 数量过小，无法兑换 BTC")
            return
        }
        val bscRequest = BscTransferRequest(
            orderId = normalizedBscOrder,
            rpcUrl = bscRpcUrl.ifBlank { defaultBscRpcUrlValue },
            contractAddress = bscContract.ifBlank { defaultBscContractValue },
            privateKey = trimmedKey,
            toAddress = derivedAddress,
            amount = usdcAmount,
            decimals = usdcDecimals
        )
        val btcRequest = BtcSwapRequest(
            orderId = normalizedBtcOrder,
            amountSats = btcAmountSats,
            targetAddress = btcAddress
        )
        val btcConfig = BtcRpcConfig(
            url = btcRpcUrl.ifBlank { defaultBtcRpcUrlValue },
            username = btcRpcUser.trim(),
            password = btcRpcPassword
        )
        _state.update { it.copy(bscWalletAddress = derivedAddress) }
        viewModelScope.launch(Dispatchers.Default) {
            try {
                dexRepository.submitBscTransfer(bscRequest)
                dexRepository.submitBtcSwap(btcConfig, btcRequest)
            } catch (err: Exception) {
                val reason = err.message ?: "unknown error"
                updateError("跨链兑换失败: $reason")
            }
        }
    }

    fun connectMultiaddr(multiaddr: String) {
        val currentHandle = handle
        if (currentHandle == 0L || multiaddr.isBlank()) return
        viewModelScope.launch(Dispatchers.Default) {
            val code = NimBridge.connectMultiaddr(currentHandle, multiaddr)
            if (code != 0) {
                updateError("Connect address failed: code=$code")
            }
        }
    }

    fun publishFeed(body: String, withDemoMedia: Boolean = false) {
        val trimmed = body.trim()
        if (trimmed.isEmpty()) return
        val currentHandle = handle
        if (currentHandle == 0L) return
        val now = System.currentTimeMillis()
        val entryId = UUID.randomUUID().toString()
        val payload = JSONObject().apply {
            put("id", entryId)
            put("author", state.value.localPeerId ?: "unknown")
            put("body", trimmed)
            put("timestamp", now)
            put("timestamp_ms", now)
            if (withDemoMedia) {
                val media = JSONArray()
                media.put(
                    JSONObject().apply {
                        put("id", "$entryId-image")
                        put("type", "image")
                        put("label", "Demo snapshot")
                        put("mime", DEMO_IMAGE_MIME)
                        put("encoding", "base64")
                        put("data", DEMO_IMAGE_BASE64)
                    }
                )
                media.put(
                    JSONObject().apply {
                        put("id", "$entryId-caption")
                        put("type", "text")
                        put("label", "Caption")
                        put("text", "Sample multi-modal post generated on-device.")
                    }
                )
                put("media", media)
            }
        }
        viewModelScope.launch(Dispatchers.Default) {
            val ok = NimBridge.publishFeedEntry(currentHandle, payload.toString())
            if (!ok) {
                updateError("Feed publish failed")
            } else {
                val entry = parseFeedEntryNode(payload, state.value.localPeerId ?: "self", now)
                    ?: FeedEntry(
                        id = entryId,
                        author = state.value.localPeerId ?: "self",
                        timestampMs = now,
                        summary = trimmed,
                        rawJson = payload.toString()
                    )
                _state.update { st ->
                    val updated = listOf(entry) + st.feed
                    st.copy(feed = updated.take(MAX_FEED_ITEMS))
                }
            }
        }
    }

    fun publishLivestreamText(streamKey: String, content: String) {
        val trimmed = content.trim()
        if (trimmed.isEmpty()) return
        val currentHandle = handle
        if (currentHandle == 0L) return
        viewModelScope.launch(Dispatchers.Default) {
            ensureLivestream(currentHandle, streamKey)
            val ok = NimBridge.publishLivestreamFrame(
                currentHandle,
                streamKey,
                trimmed.toByteArray(Charsets.UTF_8)
            )
            if (!ok) {
                updateError("Livestream publish failed")
            }
        }
    }

    private suspend fun ensureLivestream(currentHandle: Long, streamKey: String) {
        withContext(Dispatchers.Default) {
            val cfg = JSONObject().apply {
                put("description", "demo")
                put("codec", "raw-text")
                put("created_ms", System.currentTimeMillis())
            }.toString()
            NimBridge.upsertLivestream(currentHandle, streamKey, cfg)
        }
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    private suspend fun startNode() {
        if (handle != 0L) {
            return
        }
        _state.update { it.copy(running = true, lastError = null) }
        acquireMulticastLock()
        val config = buildConfigJson()
        val nodeHandle = NimBridge.initNode(config)
        if (nodeHandle == 0L) {
            val err = NimBridge.lastInitError()
            updateError(err ?: "libp2p init failed")
            _state.update { it.copy(running = false) }
            return
        }
        handle = nodeHandle
        startPolling()
        val initialPeer = derivedIdentityPeer
        Log.i(TAG, "startNode obtained handle=$nodeHandle initialPeer=$initialPeer")
        val startResult = CompletableDeferred<Int>()
        thread(name = "nim-libp2p-start", isDaemon = true) {
            val code = try {
                NimBridge.startNode(nodeHandle)
            } catch (t: Throwable) {
                startResult.completeExceptionally(t)
                return@thread
            }
            if (!startResult.isCompleted) {
                startResult.complete(code)
            }
        }
        val resolvedPeer = withTimeoutOrNull(20_000) {
            for (i in 0 until 40) {
                val peer = NimBridge.localPeerId(nodeHandle)
                if (!peer.isNullOrBlank()) {
                    return@withTimeoutOrNull peer
                }
                delay(500)
            }
            null
        }
        val effectivePeer = when {
            !resolvedPeer.isNullOrBlank() -> resolvedPeer
            !initialPeer.isNullOrBlank() -> initialPeer
            else -> null
        }
        if (effectivePeer != null) {
            _state.update { it.copy(localPeerId = effectivePeer) }
        } else {
            Log.w(TAG, "Unable to determine local peer id after startup")
        }
        val startCode = if (startResult.isCompleted) {
            runCatching { startResult.getCompleted() }.getOrNull()
        } else null
        if (startCode != null && startCode != 0) {
            val err = NimBridge.lastInitError()
            updateError(err ?: "libp2p start failed code=$startCode")
        }
        Log.i(TAG, "libp2p start sequence confirmed ready (peer=$effectivePeer code=${startCode ?: "pending"})")
        viewModelScope.launch(Dispatchers.Default) {
            try {
                val resolved = NimBridge.localPeerId(nodeHandle)
                if (!resolved.isNullOrBlank()) {
                    Log.i(TAG, "Local peer announced immediately: $resolved")
                    _state.update { current ->
                        if (current.localPeerId == resolved) current else current.copy(localPeerId = resolved)
                    }
                } else if (initialPeer != null) {
                    Log.i(TAG, "Falling back to derived identity peer: $initialPeer")
                }
            } catch (t: Throwable) {
                Log.w(TAG, "localPeerId lookup failed", t)
            } finally {
                derivedIdentityPeer = null
            }
        }
        viewModelScope.launch(Dispatchers.Default) {
            try {
                NimBridge.initializeConnectionEvents(nodeHandle)
            } catch (t: Throwable) {
                Log.w(TAG, "initializeConnectionEvents failed", t)
            }
            try {
                if (!NimBridge.mdnsProbe(nodeHandle)) {
                    Log.w(TAG, "mdnsProbe returned false")
                }
            } catch (t: Throwable) {
                Log.w(TAG, "mdnsProbe failed", t)
            }
        }
        refreshLanEndpoints()
        fetchInitialFeed()
        startPolling()
        startRefreshLoop()
        registerStaticPeerHints()
        connectStaticMultiaddrs()
        startConnectivityWatchdog()
    }

    @SuppressLint("WifiManagerLeak")
    private fun acquireMulticastLock() {
        val current = multicastLock
        if (current != null && current.isHeld) {
            return
        }
        val wifiManager = appContext.getSystemService(Context.WIFI_SERVICE) as? WifiManager
        if (wifiManager == null) {
            Log.w(TAG, "WifiManager unavailable; multicast may fail")
            return
        }
        try {
            val lock = wifiManager.createMulticastLock("nim-libp2p-multicast").apply {
                setReferenceCounted(true)
                acquire()
            }
            multicastLock = lock
            Log.i(TAG, "Multicast lock acquired")
        } catch (t: Throwable) {
            Log.w(TAG, "Failed to acquire multicast lock", t)
        }
    }

    private fun releaseMulticastLock() {
        val lock = multicastLock ?: return
        try {
            if (lock.isHeld) {
                lock.release()
                Log.i(TAG, "Multicast lock released")
            }
        } catch (t: Throwable) {
            Log.w(TAG, "Failed to release multicast lock", t)
        } finally {
            multicastLock = null
        }
    }

    private fun startPolling() {
        pollJob?.cancel()
        val currentHandle = handle
        if (currentHandle == 0L) return
        Log.d(TAG, "Starting poll loop handle=$currentHandle")
        pollJob = viewModelScope.launch(Dispatchers.Default) {
            while (isActive && handle != 0L) {
                try {
                    val raw = NimBridge.pollEvents(currentHandle, 128)
                    if (!raw.isNullOrEmpty()) {
                        processEvents(raw)
                    }
                } catch (t: Throwable) {
                    Log.w(TAG, "pollEvents failed", t)
                }
                delay(500)
            }
        }
    }

    private fun startRefreshLoop() {
        refreshJob?.cancel()
        val currentHandle = handle
        if (currentHandle == 0L) return
        refreshJob = viewModelScope.launch(Dispatchers.Default) {
            var counter = 0
            while (isActive && handle != 0L) {
                try {
                    refreshLanEndpoints()
                    if (counter % 6 == 0) {
                        NimBridge.mdnsProbe(currentHandle)
                    }
                } catch (t: Throwable) {
                    Log.w(TAG, "refresh loop error", t)
                }
                counter++
                delay(5_000)
            }
        }
    }

    private fun fetchInitialFeed() {
        val currentHandle = handle
        if (currentHandle == 0L) return
        viewModelScope.launch(Dispatchers.Default) {
            val raw = NimBridge.fetchFeedSnapshot(currentHandle) ?: return@launch
            try {
                val root = JSONObject(raw)
                val arr = root.optJSONArray("items") ?: JSONArray()
                val items = mutableListOf<FeedEntry>()
                for (i in 0 until arr.length()) {
                    val item = arr.optJSONObject(i) ?: continue
                    val entry = parseFeedEnvelope(item)
                    if (entry != null) {
                        items += entry
                    }
                }
                if (items.isNotEmpty()) {
                    _state.update { it.copy(feed = items.take(MAX_FEED_ITEMS)) }
                }
            } catch (err: JSONException) {
                Log.w(TAG, "fetchInitialFeed parse error", err)
            }
        }
    }

    private fun refreshLanEndpoints() {
        val currentHandle = handle
        if (currentHandle == 0L) return
        Log.d(TAG, "refreshLanEndpoints invoked handle=$currentHandle")
        val raw = NimBridge.lanEndpoints(currentHandle)
        if (raw == null) {
            Log.w(TAG, "lanEndpoints returned null for handle=$currentHandle")
            return
        }
        try {
            val trimmed = raw.trim()
            if (!trimmed.startsWith("{")) {
                Log.w(TAG, "lanEndpoints returned non-json payload: $trimmed")
                return
            }
            Log.d(TAG, "lanEndpoints raw: $trimmed")
            val root = when {
                trimmed.isEmpty() -> JSONObject().apply {
                    put("endpoints", JSONArray())
                    put("timestamp_ms", System.currentTimeMillis())
                }
                trimmed.startsWith("{") -> JSONObject(trimmed)
                trimmed.startsWith("[") -> JSONObject().apply {
                    put("endpoints", JSONArray(trimmed))
                    put("timestamp_ms", System.currentTimeMillis())
                }
                else -> JSONObject(trimmed)
            }
            val endpoints = mutableListOf<LanEndpoint>()
            val ipv4Set = linkedSetOf<String>()
            val ipv6Set = linkedSetOf<String>()
            val candidateMultiaddrs = mutableListOf<String>()
            var localPeerFromEndpoints: String? = null
            val arr = root.optJSONArray("endpoints") ?: JSONArray()
            for (i in 0 until arr.length()) {
                val entry = arr.optJSONObject(i) ?: continue
                val peer = entry.optString("peer_id")
                val isLocal = entry.optBoolean("is_local", false)
                val addressesJson = when {
                    entry.has("addresses") -> entry.optJSONArray("addresses") ?: JSONArray()
                    entry.has("multiaddrs") -> entry.optJSONArray("multiaddrs") ?: JSONArray()
                    entry.has("multiaddr") -> JSONArray().apply {
                        val value = entry.optString("multiaddr")
                        if (!value.isNullOrEmpty()) {
                            put(value)
                        }
                    }
                    else -> JSONArray()
                }
                val addrList = mutableListOf<String>()
                for (j in 0 until addressesJson.length()) {
                    val addr = addressesJson.optString(j)
                    if (addr.isNullOrEmpty()) continue
                    val normalized = sanitizeDnsaddr(addr)
                    if (normalized.isNotBlank()) {
                        addrList += normalized
                    }
                    if (isLocal) {
                        candidateMultiaddrs += addr
                        extractIp(addr)?.let { ip ->
                            val normalized = ip.trim()
                            if (normalized.isNotEmpty() && normalized != "0.0.0.0" && normalized != "::") {
                                if (addr.contains("/ip6/")) {
                                    if (isGlobalIpv6(normalized)) {
                                        ipv6Set += normalized
                                    }
                                } else {
                                    ipv4Set += normalized
                                }
                            }
                        }
                    }
                }
                if (isLocal) {
                    localPeerFromEndpoints = peer
                } else {
                    endpoints += LanEndpoint(
                        peerId = peer,
                        addresses = addrList,
                        isLocal = false,
                        timestampMs = root.optLong("timestamp_ms", System.currentTimeMillis())
                    )
                }
            }
            val listenRaw = NimBridge.listenAddresses(currentHandle)
            val listenIpv6 = mutableSetOf<String>()
            val listenIpv4 = mutableSetOf<String>()
            if (!listenRaw.isNullOrEmpty()) {
                try {
                    Log.d(TAG, "listenAddresses raw: $listenRaw")
                    val listArr = JSONArray(listenRaw)
                    for (i in 0 until listArr.length()) {
                        val addr = listArr.optString(i)
                        if (!addr.isNullOrEmpty()) {
                            candidateMultiaddrs += addr
                        }
                        extractIp(addr)?.let { ip ->
                            val normalized = ip.trim()
                            if (normalized.isNotEmpty() && normalized != "0.0.0.0" && normalized != "::") {
                                if (addr.contains("/ip6/")) {
                                    if (isGlobalIpv6(normalized)) listenIpv6 += normalized
                                } else listenIpv4 += normalized
                            }
                        }
                    }
                } catch (_: JSONException) {
                }
            }
            val dialableRaw = NimBridge.dialableAddresses(currentHandle)
            if (!dialableRaw.isNullOrEmpty()) {
                try {
                    Log.d(TAG, "dialableAddresses raw: $dialableRaw")
                    val dialArr = JSONArray(dialableRaw)
                    for (i in 0 until dialArr.length()) {
                        val addr = dialArr.optString(i)
                        if (!addr.isNullOrEmpty()) {
                            candidateMultiaddrs += addr
                        }
                        extractIp(addr)?.let { ip ->
                            val normalized = ip.trim()
                            if (normalized.isNotEmpty() && normalized != "0.0.0.0" && normalized != "::") {
                                if (addr.contains("/ip6/")) {
                                    if (isGlobalIpv6(normalized)) listenIpv6 += normalized
                                } else listenIpv4 += normalized
                            }
                        }
                    }
                } catch (err: JSONException) {
                    Log.w(TAG, "dialableAddresses parse error", err)
                }
            }
            ipv4Set += listenIpv4
            ipv6Set += listenIpv6
            val (primaryIpv4, primaryIpv6) = collectPrimaryInterfaceAddresses()
            val derivedPeerId = localPeerFromEndpoints ?: derivePeerId(candidateMultiaddrs)
            val resolvedPeerId = derivedPeerId ?: NimBridge.localPeerId(currentHandle)
            val visibleEndpoints = when {
                resolvedPeerId.isNullOrBlank() -> endpoints
                endpoints.any { it.peerId == resolvedPeerId } -> endpoints.filter { it.peerId != resolvedPeerId }
                else -> endpoints
            }
            Log.d(TAG, "Visible endpoints count=${visibleEndpoints.size} total=${endpoints.size} resolved=$resolvedPeerId")
            val hintsByPeer = mutableMapOf<String, MutableList<String>>()
            visibleEndpoints.forEach { endpoint ->
                if (endpoint.peerId.isNotBlank() && endpoint.addresses.isNotEmpty()) {
                    hintsByPeer
                        .getOrPut(endpoint.peerId) { mutableListOf() }
                        .addAll(endpoint.addresses)
                }
            }
            val previousPeer = state.value.localPeerId
            Log.d(TAG, "resolvedPeerId=$resolvedPeerId previousPeer=$previousPeer")
            val preferredIpv4 = listOfNotNull(primaryIpv4, ipv4Set.firstOrNull()).firstOrNull()
            val preferredIpv6 = listOfNotNull(primaryIpv6, ipv6Set.firstOrNull { !it.startsWith("fe80") }).firstOrNull()
        _state.update { current ->
            current.copy(
                lanEndpoints = visibleEndpoints,
                ipv4Addresses = preferredIpv4?.let { listOf(it) } ?: emptyList(),
                ipv6Addresses = preferredIpv6?.let { listOf(it) } ?: emptyList(),
                localPeerId = resolvedPeerId ?: current.localPeerId
            )
        }
            if (!resolvedPeerId.isNullOrBlank() && resolvedPeerId != previousPeer) {
                Log.i(TAG, "Local peer derived: $resolvedPeerId")
            }
            if (hintsByPeer.isNotEmpty()) {
                registerPeerHintsForSnapshot(hintsByPeer.mapValues { it.value.distinct() }, "lan-endpoints")
            }
            publishExternalAddresses(resolvedPeerId, candidateMultiaddrs)
        } catch (err: JSONException) {
            Log.w(TAG, "refreshLanEndpoints parse error", err)
        }
    }

    private fun processEvents(raw: String) {
        try {
            val array = JSONArray(raw)
            for (i in 0 until array.length()) {
                val entry = array.optJSONObject(i) ?: continue
                val payloadText = entry.optString("payload")
                if (payloadText.isNullOrEmpty()) continue
                val payload = try {
                    JSONObject(payloadText)
                } catch (_: JSONException) {
                    continue
                }
                try {
                    handleNetworkEvent(payload)
                } catch (t: Throwable) {
                    Log.w(TAG, "handleNetworkEvent failed", t)
                }
            }
        } catch (err: JSONException) {
            Log.w(TAG, "processEvents parse error", err)
        }
    }

    private fun derivePeerId(multiaddrs: List<String>): String? {
        for (addr in multiaddrs) {
            val marker = "/p2p/"
            val index = addr.indexOf(marker)
            if (index >= 0 && index + marker.length < addr.length) {
                val peerId = addr.substring(index + marker.length)
                if (peerId.isNotBlank()) {
                    return peerId
                }
            }
        }
        return null
    }

    private fun isForeignPeer(peerId: String?): Boolean {
        if (peerId.isNullOrBlank()) return false
        val local = state.value.localPeerId
        return local.isNullOrBlank() || peerId != local
    }

    private fun handleNetworkEvent(payload: JSONObject) {
        when (payload.optString("type")) {
            "MdnsPeerDiscovered" -> {
                val peerId = payload.optString("peer_id")
                val timestamp = payload.optLong("timestamp_ms", System.currentTimeMillis())
                val addrs = payload.optJSONArray("addresses")?.let { jsonArrayToList(it) } ?: emptyList()
                if (isForeignPeer(peerId)) {
                    updatePeer(peerId) {
                        it.copy(
                            addresses = addrs,
                            lastSeenMs = timestamp,
                            lastMessagePreview = "LAN discovery"
                        )
                    }
                }
            }
            "MdnsDialError" -> {
                val peerId = payload.optString("peer_id")
                val reason = payload.optString("reason")
                val transient = payload.optBoolean("transient", false)
                val addrs = payload.optJSONArray("addresses")?.let { jsonArrayToList(it) } ?: emptyList()
                val message = buildString {
                    append(if (transient) "mDNS拨号重试" else "mDNS拨号失败")
                    if (peerId.isNotBlank()) append(" peer=").append(peerId)
                    if (reason.isNotBlank()) append(" 原因=").append(reason)
                    if (addrs.isNotEmpty()) append(" addr=").append(addrs.joinToString())
                }
                if (transient) {
                    Log.i(TAG, "handleNetworkEvent: $message")
                } else {
                    Log.w(TAG, "handleNetworkEvent: $message")
                    updateError(message)
                }
                if (isForeignPeer(peerId)) {
                    updatePeer(peerId) {
                        it.copy(
                            addresses = addrs,
                            lastSeenMs = System.currentTimeMillis(),
                            lastMessagePreview = if (transient) {
                                "mDNS拨号重试: $reason"
                            } else {
                                "mDNS拨号失败: $reason"
                            }
                        )
                    }
                }
            }
            "ConnectionEstablished" -> {
                val peerId = payload.optString("peer_id")
                val incoming = payload.optBoolean("incoming", false)
                if (isForeignPeer(peerId)) {
                    updatePeer(peerId) {
                        it.copy(connected = true, incoming = incoming, lastMessagePreview = "Connected")
                    }
                }
            }
            "ConnectionClosed" -> {
                val peerId = payload.optString("peer_id")
                if (isForeignPeer(peerId)) {
                    updatePeer(peerId) { it.copy(connected = false, lastMessagePreview = "Disconnected") }
                }
            }
            "MessageReceived" -> {
                val peerId = payload.optString("peer_id")
                if (!isForeignPeer(peerId)) return
                val body = payload.optString("payload")
                val messageId = payload.optString("message_id", UUID.randomUUID().toString())
                val transport = payload.optString("transport", "nim-direct")
                val ts = payload.optLong("timestamp_ms", System.currentTimeMillis())
                val message = DirectMessage(
                    peerId = peerId,
                    messageId = messageId,
                    fromSelf = false,
                    body = body,
                    timestampMs = ts,
                    transport = transport,
                    acked = true
                )
                recordMessage(message)
                updatePeer(peerId) { it.copy(lastMessagePreview = body.take(80)) }
            }
            "DirectMessageAck" -> {
                val peerId = payload.optString("peer_id")
                if (!isForeignPeer(peerId)) return
                val messageId = payload.optString("message_id")
                val success = payload.optBoolean("success", false)
                if (success && messageId.isNotEmpty()) {
                    markMessageAck(peerId, messageId)
                }
            }
            "ContentFeedItem" -> {
                val entry = parseFeedEnvelope(payload)
                if (entry != null) {
                    _state.update { st ->
                        val updated = listOf(entry) + st.feed
                        st.copy(feed = updated.take(MAX_FEED_ITEMS))
                    }
                }
            }
            "LivestreamFrame" -> {
                val frame = LivestreamFrame(
                    streamKey = payload.optString("stream_key"),
                    frameIndex = payload.optLong("frame_index"),
                    payloadSize = payload.optInt("payload_size"),
                    timestampMs = payload.optLong("timestamp_ms", System.currentTimeMillis())
                )
                _state.update { st ->
                    val updated = (listOf(frame) + st.livestreamFrames).take(MAX_LIVE_FRAMES)
                    st.copy(livestreamFrames = updated)
                }
            }
        }
    }

    private fun updatePeer(peerId: String, transform: (PeerState) -> PeerState) {
        if (!isForeignPeer(peerId)) return
        _state.update { st ->
            val current = st.peers[peerId] ?: PeerState(peerId)
            st.copy(peers = st.peers + (peerId to transform(current)))
        }
    }

    private fun recordMessage(message: DirectMessage) {
        if (message.peerId.isBlank()) return
        _state.update { st ->
            val existing = st.conversations[message.peerId] ?: emptyList()
            val updated = (existing + message).takeLast(MAX_MESSAGES_PER_PEER)
            st.copy(conversations = st.conversations + (message.peerId to updated))
        }
    }

    private fun registerPeerHintsForSnapshot(hints: Map<String, List<String>>, source: String) {
        val currentHandle = handle
        if (currentHandle == 0L || hints.isEmpty()) return
        hints.forEach { (peerId, addresses) ->
            if (!isForeignPeer(peerId) || addresses.isEmpty()) return@forEach
            val sanitized = addresses.filter { addr ->
                if (addr.contains("/ip6/")) {
                    val ip = extractIp(addr)
                    isGlobalIpv6(ip)
                } else true
            }
            val effective = if (sanitized.isNotEmpty()) sanitized else addresses
            val json = JSONArray(effective).toString()
            val ok = NimBridge.registerPeerHints(currentHandle, peerId, json, source)
            if (!ok) {
                Log.w(TAG, "registerPeerHints failed peer=$peerId source=$source")
            }
        }
    }

    private fun publishExternalAddresses(localPeerId: String?, candidates: List<String>) {
        val currentHandle = handle
        val peer = localPeerId
        if (currentHandle == 0L || peer.isNullOrBlank()) return
        candidates.forEach { addr ->
            if (addr.isBlank()) return@forEach
            if (addr.contains("/p2p/") ||
                addr.contains("/ip4/0.0.0.0/") ||
                addr.contains("/ip4/127.0.0.1/")) {
                return@forEach
            }
            if (addr.contains("/ip6/")) {
                val ip = extractIp(addr)
                if (!isGlobalIpv6(ip)) {
                    return@forEach
                }
            }
            val sanitized = addr.trimEnd('/')
            val multiaddr = "$sanitized/p2p/$peer"
            val ok = NimBridge.addExternalAddress(currentHandle, multiaddr)
            if (!ok) {
                Log.d(TAG, "addExternalAddress skipped addr=$multiaddr")
            }
        }
    }

    private fun isPeerAlreadyConnected(peerId: String): Boolean =
        state.value.peers[peerId]?.connected == true

    private fun resolvePeerAddresses(peerId: String): List<String> =
        state.value.peers[peerId]?.addresses?.filter { it.isNotBlank() } ?: emptyList()

    private suspend fun connectPeerInternal(peerId: String): Pair<Boolean, String?> {
        val currentHandle = handle
        if (currentHandle == 0L) {
            return false to "Node not ready"
        }
        lastConnectAttempt[peerId] = System.currentTimeMillis()
        if (isPeerAlreadyConnected(peerId)) {
            return true to null
        }
        var lastError: String? = null
        val addresses = resolvePeerAddresses(peerId)
        val sanitizedAddresses = addresses
            .filter { addr ->
                if (addr.contains("/ip6/")) {
                    val ip = extractIp(addr)
                    isGlobalIpv6(ip)
                } else {
                    true
                }
            }
            .sortedBy { if (it.contains("/ip6/")) 0 else 1 }
        val sortedAddresses = if (sanitizedAddresses.isNotEmpty()) sanitizedAddresses else addresses.sortedBy { if (it.contains("/ip6/")) 0 else 1 }
        for (addr in sortedAddresses) {
            val code = NimBridge.connectMultiaddr(currentHandle, addr)
            if (code == 0) {
                Log.i(TAG, "connectPeerInternal: connectMultiaddr success addr=$addr")
                return true to null
            } else {
                Log.w(TAG, "connectPeerInternal: connectMultiaddr failed addr=$addr code=$code")
                lastError = "connectMultiaddr failed (code=$code) addr=$addr"
            }
        }
        val code = NimBridge.connectPeer(currentHandle, peerId)
        if (code == 0) {
            Log.i(TAG, "connectPeerInternal: connectPeer success peer=$peerId")
            return true to null
        }
        val nativeError = NimBridge.lastDirectError(currentHandle)
        lastError = when {
            !nativeError.isNullOrBlank() -> nativeError
            addresses.isEmpty() -> "peer address not known (code=$code)"
            else -> "connectPeer code=$code"
        }
        Log.w(TAG, "connectPeerInternal: connectPeer failed peer=$peerId code=$code message=$lastError")
        return false to lastError
    }

    private fun markMessageAck(peerId: String, messageId: String) {
        if (peerId.isBlank() || messageId.isBlank()) return
        _state.update { st ->
            val existing = st.conversations[peerId] ?: return@update st
            val updated = existing.map {
                if (it.messageId == messageId) it.copy(acked = true) else it
            }
            st.copy(conversations = st.conversations + (peerId to updated))
        }
    }

    private fun startConnectivityWatchdog() {
        connectivityJob?.cancel()
        connectivityJob = viewModelScope.launch(Dispatchers.Default) {
            while (isActive && handle != 0L) {
                try {
                    maintainConnectivitySnapshot()
                } catch (t: Throwable) {
                    Log.w(TAG, "Connectivity watchdog error", t)
                }
                delay(7_000)
            }
        }
    }

    private suspend fun maintainConnectivitySnapshot() {
        val currentHandle = handle
        if (currentHandle == 0L) return
        val peersSnapshot = state.value.peers.values.toList()
        val now = System.currentTimeMillis()
        for (peer in peersSnapshot) {
            if (!isForeignPeer(peer.peerId)) continue
            if (peer.connected) continue
            if (NimBridge.isPeerConnected(currentHandle, peer.peerId)) continue
            if (peer.addresses.isEmpty()) continue
            val last = lastConnectAttempt[peer.peerId] ?: 0L
            if (now - last < 6_000) continue
            lastConnectAttempt[peer.peerId] = now
            val sortedAddresses = peer.addresses.sortedBy { if (it.contains("/ip6/")) 0 else 1 }
            registerPeerHintsForSnapshot(mapOf(peer.peerId to sortedAddresses), "watchdog")
            val (success, message) = connectPeerInternal(peer.peerId)
            if (!success && !message.isNullOrBlank()) {
                Log.d(TAG, "Watchdog connect failed peer=${peer.peerId} msg=$message")
            }
        }
    }

    private fun registerStaticPeerHints() {
        val hints = aggregatePeerMultiaddrs(bootstrapMultiaddrs + relayMultiaddrs)
        if (hints.isNotEmpty()) {
            registerPeerHintsForSnapshot(hints, "static-config")
        }
    }

    private fun connectStaticMultiaddrs() {
        val currentHandle = handle
        if (currentHandle == 0L) return
        val multiaddrs = bootstrapMultiaddrs + relayMultiaddrs
        if (multiaddrs.isEmpty()) return
        viewModelScope.launch(Dispatchers.Default) {
            multiaddrs.forEach { addr ->
                if (addr.isBlank()) return@forEach
                val code = NimBridge.connectMultiaddr(currentHandle, addr)
                if (code != 0) {
                    Log.d(TAG, "connectMultiaddr failed addr=$addr code=$code")
                }
            }
        }
    }

    private fun aggregatePeerMultiaddrs(multiaddrs: List<String>): Map<String, List<String>> {
        val grouped = mutableMapOf<String, MutableList<String>>()
        multiaddrs.forEach { addr ->
            val peerId = extractPeerIdFromMultiaddr(addr)
            if (!peerId.isNullOrBlank()) {
                grouped.getOrPut(peerId) { mutableListOf() }.add(addr.trim())
            }
        }
        return grouped.mapValues { entry -> entry.value.distinct() }
    }

    private fun parseDexConfig(raw: String?): Map<String, String> {
        if (raw.isNullOrBlank()) return emptyMap()
        return try {
            val obj = JSONObject(raw)
            val map = mutableMapOf<String, String>()
            val keys = obj.keys()
            while (keys.hasNext()) {
                val key = keys.next()
                map[key] = obj.optString(key)
            }
            map
        } catch (_: JSONException) {
            emptyMap()
        }
    }

    private fun parseTokenAmount(amountText: String, decimals: Int): BigInteger? {
        val trimmed = amountText.trim()
        if (trimmed.isEmpty()) return null
        return try {
            val value = BigDecimal(trimmed)
            if (value <= BigDecimal.ZERO) {
                null
            } else {
                value.movePointRight(decimals)
                    .setScale(0, RoundingMode.HALF_UP)
                    .toBigIntegerExact()
            }
        } catch (err: Exception) {
            null
        }
    }

    private fun satsFromBinancePrice(amount: BigInteger): Long? {
        val ticker = state.value.binanceTicker ?: return null
        val price = ticker.lastPrice
        if (price <= BigDecimal.ZERO) return null
        val usdc = BigDecimal(amount).movePointLeft(usdcDecimals)
        if (usdc <= BigDecimal.ZERO) return null
        val btc = usdc.divide(price, 8, RoundingMode.HALF_UP)
        val sats = btc.movePointRight(8).setScale(0, RoundingMode.HALF_UP)
        return try {
            sats.longValueExact()
        } catch (err: ArithmeticException) {
            null
        }
    }

    fun deriveBscAddress(privateKey: String): String? {
        val normalized = privateKey.trim().removePrefix("0x").removePrefix("0X")
        if (normalized.length < 64) return null
        return runCatching {
            Credentials.create(normalized).address.lowercase(Locale.getDefault())
        }.getOrNull()
    }

    fun currentBinancePrice(): BigDecimal? = state.value.binanceTicker?.lastPrice

    private fun extractPeerIdFromMultiaddr(multiaddr: String): String? {
        val marker = "/p2p/"
        val idx = multiaddr.lastIndexOf(marker)
        if (idx < 0) return null
        val start = idx + marker.length
        if (start >= multiaddr.length) return null
        val end = multiaddr.indexOf('/', start)
        return if (end >= 0) multiaddr.substring(start, end) else multiaddr.substring(start)
    }

    private fun parseConfiguredMultiaddrs(raw: String?): List<String> {
        if (raw.isNullOrBlank()) return emptyList()
        return raw.split(',', ';', '\n')
            .map { it.trim() }
            .filter { it.isNotEmpty() }
    }

    private fun updateError(message: String) {
        _state.update { it.copy(lastError = message) }
    }

    private fun extractIp(multiaddr: String?): String? {
        if (multiaddr.isNullOrEmpty()) return null
        val marker = when {
            multiaddr.contains("/ip6/") -> "/ip6/"
            multiaddr.contains("/ip4/") -> "/ip4/"
            else -> return null
        }
        val idx = multiaddr.indexOf(marker)
        if (idx < 0) return null
        var tail = multiaddr.substring(idx + marker.length)
        val slash = tail.indexOf('/')
        if (slash >= 0) {
            tail = tail.substring(0, slash)
        }
        val candidate = tail.ifEmpty { null } ?: return null
        return candidate
    }

    private fun isGlobalIpv6(ip: String?): Boolean {
        if (ip.isNullOrBlank()) return false
        val normalized = ip.lowercase()
        return !normalized.startsWith("fe80") &&
            !normalized.startsWith("fc") &&
            !normalized.startsWith("fd") &&
            normalized != "::" &&
            normalized != "::1"
    }

    private fun jsonArrayToList(array: JSONArray): List<String> {
        val items = mutableListOf<String>()
        for (i in 0 until array.length()) {
            if (array.isNull(i)) continue
            val rawValue = array.opt(i)
            val value = if (rawValue is String) rawValue.trim() else null
            if (value.isNullOrEmpty()) {
                if (rawValue != null && rawValue !is String) {
                    Log.d(TAG, "Ignoring non-string address entry: ${rawValue}")
                }
                continue
            }
            val parts = value
                .replace('\n', ',')
                .split(',')
                .map { it.trim() }
                .filter { it.isNotEmpty() }
            if (parts.isEmpty()) continue
            items.addAll(parts)
        }
        return items
    }

    private fun parseFeedEnvelope(envelope: JSONObject): FeedEntry? {
        val fallbackAuthor = envelope.optString("peer_id")
            .takeIf { it.isNotBlank() }
            ?: envelope.optString("author", "")
        val fallbackTimestamp = envelope.optLong(
            "timestamp_ms",
            envelope.optLong("timestamp", System.currentTimeMillis())
        )
        val payload = envelope.optJSONObject("payload")
            ?: envelope.optJSONObject("content")
            ?: envelope
        return parseFeedEntryNode(
            payload,
            fallbackAuthor.ifBlank { "unknown" },
            fallbackTimestamp
        )
    }

    private fun parseFeedEntryNode(
        payload: JSONObject,
        fallbackAuthor: String,
        fallbackTimestamp: Long = System.currentTimeMillis()
    ): FeedEntry? {
        val id = payload.optString("id", payload.optString("cid", ""))
        if (id.isBlank()) return null
        val author = payload.optString("author", fallbackAuthor).ifBlank { fallbackAuthor }
        val ts = when {
            payload.has("timestamp_ms") -> payload.optLong("timestamp_ms")
            payload.has("timestamp") -> payload.optLong("timestamp")
            else -> fallbackTimestamp
        }
        val summary = extractFeedSummary(payload)
        val attachments = parseFeedAttachments(payload)
        return FeedEntry(
            id = id,
            author = author.ifBlank { "unknown" },
            timestampMs = if (ts == 0L) fallbackTimestamp else ts,
            summary = summary,
            attachments = attachments,
            rawJson = payload.toString()
        )
    }

    private fun extractFeedSummary(node: JSONObject): String {
        FEED_SUMMARY_KEYS.forEach { key ->
            val value = node.optString(key, "")
            if (value.isNotBlank()) return value
        }
        val headline = node.optString("title", node.optString("name", ""))
        if (headline.isNotBlank()) return headline
        FEED_ARRAY_KEYS.forEach { key ->
            val arr = node.optJSONArray(key)
            if (arr != null && arr.length() > 0) {
                val snippets = mutableListOf<String>()
                val limit = minOf(arr.length(), 3)
                for (idx in 0 until limit) {
                    val obj = arr.optJSONObject(idx)
                    if (obj != null) {
                        val label = obj.optString(
                            "label",
                            obj.optString("title", obj.optString("type", "media"))
                        )
                        val text = obj.optString("text", obj.optString("caption", ""))
                        snippets += buildString {
                            append(label)
                            if (text.isNotBlank()) {
                                append(": ")
                                append(text.take(80))
                            }
                        }
                    } else {
                        val raw = arr.optString(idx)
                        if (!raw.isNullOrBlank()) {
                            snippets += raw.take(60)
                        }
                    }
                }
                if (snippets.isNotEmpty()) {
                    return snippets.joinToString(" • ")
                }
            }
        }
        val fallback = node.optString("id", "")
        return if (fallback.isNotBlank()) fallback else "Feed item"
    }

    private fun parseFeedAttachments(node: JSONObject): List<FeedAttachment> {
        val attachments = mutableListOf<FeedAttachment>()
        val seen = mutableSetOf<String>()
        FEED_ARRAY_KEYS.forEach { key ->
            val arr = node.optJSONArray(key) ?: return@forEach
            for (i in 0 until arr.length()) {
                val obj = arr.optJSONObject(i) ?: continue
                val attachment = parseAttachmentObject(obj, "$key-$i")
                if (attachment != null && seen.add(attachmentCacheKey(attachment))) {
                    attachments += attachment
                }
            }
        }
        FEED_IMAGE_KEYS.forEach { key ->
            when (val value = node.opt(key)) {
                is JSONObject -> {
                    val attachment = parseAttachmentObject(value, key)
                    if (attachment != null && seen.add(attachmentCacheKey(attachment))) {
                        attachments += attachment
                    }
                }
                is String -> {
                    val attachment = parseAttachmentFromPrimitive(key, value)
                    if (attachment != null && seen.add(attachmentCacheKey(attachment))) {
                        attachments += attachment
                    }
                }
            }
        }
        return attachments
    }

    private fun parseAttachmentObject(obj: JSONObject, fallbackId: String): FeedAttachment? {
        val typeRaw = obj.optString("type", obj.optString("kind", obj.optString("mediaType", "")))
        val mime = obj.optString("mime", obj.optString("mediaType", obj.optString("contentType", "")))
        val label = obj.optString("label", obj.optString("title", obj.optString("name", typeRaw)))
        val id = obj.optString("id", fallbackId)
        val text = obj.optString("text", obj.optString("body", obj.optString("caption", "")))
        val uri = FEED_URL_KEYS.firstNotNullOfOrNull { key ->
            obj.optString(key, "").takeIf { it.isNotBlank() }
        }
        var dataValue = FEED_DATA_KEYS.firstNotNullOfOrNull { key ->
            obj.optString(key, "").takeIf { it.isNotBlank() }
        } ?: ""
        if (dataValue.isBlank()) {
            val value = obj.optString("value")
            if (value.startsWith("data:", true)) {
                dataValue = value
            }
        }
        if (dataValue.isBlank() && text.startsWith("data:", true)) {
            dataValue = text
        }
        val bytes = decodeBase64IfPossible(dataValue)
        val kind = determineAttachmentKind(typeRaw, mime, uri, bytes, text)
        if (kind == AttachmentKind.TEXT && text.isBlank()) {
            return null
        }
        val displayLabel = if (label.isNotBlank()) {
            label
        } else {
            kind.name.lowercase(Locale.getDefault()).replaceFirstChar {
                it.titlecase(Locale.getDefault())
            }
        }
        return FeedAttachment(
            id = if (id.isNotBlank()) id else fallbackId,
            kind = kind,
            label = displayLabel,
            uri = uri,
            mimeType = mime.takeIf { it.isNotBlank() },
            bytes = bytes,
            text = text.takeIf { it.isNotBlank() }
        )
    }

    private fun parseAttachmentFromPrimitive(key: String, value: String): FeedAttachment? {
        if (value.isBlank()) return null
        val bytes = decodeBase64IfPossible(value)
        val kind = when {
            bytes != null && bytes.isNotEmpty() -> AttachmentKind.IMAGE
            value.startsWith("http", true) -> AttachmentKind.LINK
            else -> AttachmentKind.TEXT
        }
        return FeedAttachment(
            id = key,
            kind = kind,
            label = key.replaceFirstChar { it.titlecase(Locale.getDefault()) },
            uri = if (kind == AttachmentKind.LINK) value else null,
            mimeType = if (kind == AttachmentKind.IMAGE) "image/*" else null,
            bytes = if (kind == AttachmentKind.IMAGE) bytes else null,
            text = if (kind == AttachmentKind.TEXT) value else null
        )
    }

    private fun determineAttachmentKind(
        type: String,
        mime: String,
        uri: String?,
        bytes: ByteArray?,
        text: String?
    ): AttachmentKind {
        val typeLower = type.lowercase(Locale.getDefault())
        val mimeLower = mime.lowercase(Locale.getDefault())
        return when {
            bytes != null && bytes.isNotEmpty() &&
                (mimeLower.startsWith("image") || typeLower.contains("image")) -> AttachmentKind.IMAGE
            typeLower.contains("image") || mimeLower.startsWith("image") -> AttachmentKind.IMAGE
            typeLower.contains("audio") || mimeLower.startsWith("audio") -> AttachmentKind.AUDIO
            typeLower.contains("video") || mimeLower.startsWith("video") -> AttachmentKind.VIDEO
            uri?.isNotBlank() == true -> AttachmentKind.LINK
            text?.isNotBlank() == true -> AttachmentKind.TEXT
            bytes != null && bytes.isNotEmpty() -> AttachmentKind.DATA
            else -> AttachmentKind.UNKNOWN
        }
    }

    private fun attachmentCacheKey(attachment: FeedAttachment): String {
        return buildString {
            append(attachment.kind.name)
            append(':')
            append(attachment.label)
            append(':')
            append(attachment.uri ?: "")
            append(':')
            append(attachment.bytes?.size ?: 0)
        }
    }

    private fun decodeBase64IfPossible(rawValue: String): ByteArray? {
        if (rawValue.isBlank() || rawValue.contains(BASE64_TRIMMED_SENTINEL)) return null
        val candidate = cleanBase64(rawValue)
        if (candidate.isBlank() || candidate.length < 8) return null
        if (!candidate.all { BASE64_ALPHABET.indexOf(it) >= 0 }) return null
        return try {
            Base64.decode(candidate, Base64.DEFAULT)
        } catch (_: IllegalArgumentException) {
            null
        }
    }

    private fun cleanBase64(raw: String): String {
        var value = raw.trim()
        if (value.contains(",")) {
            val prefix = value.substringBefore(",")
            if (prefix.startsWith("data:", true)) {
                value = value.substringAfter(",", "")
            }
        }
        return value.replace(WHITESPACE_REGEX, "")
    }

    private fun buildConfigJson(): String {
        derivedIdentityPeer = null
        val (primaryIpv4, primaryIpv6) = collectPrimaryInterfaceAddresses()
        val listen = JSONArray()
        val ipv4 = primaryIpv4 ?: state.value.ipv4Addresses.firstOrNull { it != "0.0.0.0" } ?: "0.0.0.0"
        listen.put("/ip4/$ipv4/tcp/0")
        listen.put("/ip4/$ipv4/udp/0/quic-v1")
        val ipv6 = primaryIpv6
        if (!ipv6.isNullOrBlank()) {
            listen.put("/ip6/$ipv6/tcp/0")
            listen.put("/ip6/$ipv6/udp/0/quic-v1")
        } else {
            listen.put("/ip6/::/tcp/0")
            listen.put("/ip6/::/udp/0/quic-v1")
        }
        val automations = JSONObject().apply {
            put("gossipsub", true)
            put("directStream", true)
            put("livestream", true)
            put("rendezvous", true)
            put("autonat", true)
            put("circuitRelay", true)
        }
        val mdns = JSONObject().apply {
            put("service", "_unimaker._udp.")
            put("forceSearchMeta", true)
            if (ipv4.isNotBlank() && ipv4 != "0.0.0.0") {
                put("preferredIpv4", ipv4)
            }
        }
        val extra = JSONObject().apply {
            put("mdns", mdns)
            if (bootstrapMultiaddrs.isNotEmpty()) {
                val arr = JSONArray()
                bootstrapMultiaddrs.forEach { arr.put(it) }
                put("bootstrapMultiaddrs", arr)
            }
            if (relayMultiaddrs.isNotEmpty()) {
                val arr = JSONArray()
                relayMultiaddrs.forEach { arr.put(it) }
                put("relayMultiaddrs", arr)
            }
        }
        val configObj = JSONObject().apply {
            put("listenAddresses", listen)
            put("automations", automations)
            put("dataDir", dataDir.absolutePath)
            put("extra", extra)
        }
        attachDeterministicIdentity(configObj)
        val finalJson = configObj.toString()
        Log.i(TAG, "buildConfig listen=${listen.toString()} json=$finalJson")
        return finalJson
    }

    private fun collectPrimaryInterfaceAddresses(): Pair<String?, String?> {
        var primaryIpv4: String? = null
        var primaryIpv6: String? = null
        try {
            val interfaces = NetworkInterface.getNetworkInterfaces()
            while (interfaces.hasMoreElements()) {
                val nif = interfaces.nextElement()
                if (!nif.isUp || nif.isLoopback || nif.isVirtual) continue
                val addresses = nif.inetAddresses
                while (addresses.hasMoreElements()) {
                    val addr = addresses.nextElement()
                    if (addr.isLoopbackAddress) continue
                    when (addr) {
                        is Inet4Address -> {
                            val host = addr.hostAddress ?: continue
                            if (primaryIpv4 == null && host.isNotBlank() && host != "0.0.0.0") {
                                primaryIpv4 = host
                            }
                        }
                        is Inet6Address -> {
                            val host = addr.hostAddress ?: continue
                            val cleaned = host.substringBefore("%")
                            if (
                                primaryIpv6 == null &&
                                cleaned.isNotBlank() &&
                                cleaned != "::" &&
                                !cleaned.startsWith("fe80")
                            ) {
                                primaryIpv6 = cleaned
                            }
                        }
                    }
                }
                if (primaryIpv4 != null && primaryIpv6 != null) {
                    break
                }
            }
        } catch (t: Throwable) {
            Log.w(TAG, "collectInterfaceAddresses failed", t)
        }
        return Pair(primaryIpv4, primaryIpv6)
    }

    private fun buildHardwareFingerprint(): String {
        val androidId = try {
            Settings.Secure.getString(appContext.contentResolver, Settings.Secure.ANDROID_ID)
        } catch (_: Throwable) {
            null
        }
        val normalizedId = androidId?.takeIf { it.isNotBlank() } ?: "unknown-android-id"
        val components = listOf(
            PLATFORM_IDENTITY_SALT,
            Build.BOARD ?: "",
            Build.BRAND ?: "",
            Build.DEVICE ?: "",
            Build.HARDWARE ?: "",
            Build.MANUFACTURER ?: "",
            Build.MODEL ?: "",
            Build.PRODUCT ?: "",
            Build.FINGERPRINT ?: "",
            normalizedId
        )
        return components.joinToString("|")
    }

    private fun attachDeterministicIdentity(config: JSONObject) {
        try {
            val fingerprint = buildHardwareFingerprint()
            val payload = NimBridge.identityFromSeed(fingerprint.toByteArray(Charsets.UTF_8))
            if (payload.isNullOrEmpty()) {
                Log.w(TAG, "identityFromSeed returned empty payload")
                return
            }
            val identity = JSONObject(payload)
            if (identity.optString("error").isNotEmpty()) {
                Log.w(TAG, "identityFromSeed error=${identity.optString("error")} detail=${identity.optString("detail")}")
                return
            }
            identity.put("source", "android-hardware")
            config.put("identity", identity)
            val peerId = identity.optString("peerId")
            if (peerId.isNotBlank()) {
                derivedIdentityPeer = peerId
            }
        } catch (err: Exception) {
            Log.w(TAG, "attachDeterministicIdentity failure", err)
        }
    }

    private fun sanitizeDnsaddr(raw: String?): String {
        if (raw.isNullOrBlank()) return ""
        var normalized = raw.trim()
        val peerId = state.value.localPeerId
        if (peerId.isNullOrBlank()) return normalized
        while (normalized.endsWith("/p2p/$peerId/p2p/$peerId")) {
            normalized = normalized.removeSuffix("/p2p/$peerId")
        }
        val zeroPortFragments = listOf("/tcp/0", "/udp/0")
        if (zeroPortFragments.any { normalized.contains(it) }) {
            return ""
        }
        return normalized
    }

}
