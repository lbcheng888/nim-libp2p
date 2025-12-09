package com.example.libp2psmoke.dex

import java.io.IOException
import java.math.BigDecimal
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import okhttp3.OkHttpClient
import okhttp3.Request
import org.json.JSONArray
import org.json.JSONObject

class BinanceApiClient(
    private val httpClient: OkHttpClient = OkHttpClient.Builder()
        .connectTimeout(5, java.util.concurrent.TimeUnit.SECONDS)
        .readTimeout(5, java.util.concurrent.TimeUnit.SECONDS)
        .build()
) {
    private val endpoints = listOf(
        "https://data-api.binance.vision",
        "https://api.binance.com",
        "https://api1.binance.com",
        "https://api2.binance.com",
        "https://api3.binance.com"
    )

    suspend fun fetchTicker(symbol: String = BINANCE_SPOT_SYMBOL): BinanceTicker =
        withContext(Dispatchers.IO) {
            callWithFallback {
                val url = "$it/api/v3/ticker/24hr?symbol=$symbol"
                val request = Request.Builder().url(url).get().build()
                httpClient.newCall(request).execute().use { response ->
                    val body = response.body?.string() ?: throw IOException("Empty ticker response")
                    val json = JSONObject(body)
                    BinanceTicker(
                        lastPrice = json.getBigDecimal("lastPrice"),
                        priceChangePercent = json.getBigDecimal("priceChangePercent"),
                        highPrice = json.getBigDecimal("highPrice"),
                        lowPrice = json.getBigDecimal("lowPrice"),
                        volumeBase = json.getBigDecimal("volume"),
                        volumeQuote = json.getBigDecimal("quoteVolume"),
                        closeTimeMs = json.optLong("closeTime")
                    )
                }
            }
        }

    suspend fun fetchKlines(
        symbol: String = BINANCE_SPOT_SYMBOL,
        interval: String = "1m",
        limit: Int = 300
    ): List<DexKlineBucket> = withContext(Dispatchers.IO) {
        callWithFallback {
            val url = "$it/api/v3/klines?symbol=$symbol&interval=$interval&limit=$limit"
            val request = Request.Builder().url(url).get().build()
            httpClient.newCall(request).execute().use { response ->
                val body = response.body?.string() ?: throw IOException("Empty kline response")
                val json = JSONArray(body)
                val buckets = mutableListOf<DexKlineBucket>()
                val scale = DexKlineScale.fromLabel(interval)
                for (i in 0 until json.length()) {
                    val entry = json.getJSONArray(i)
                    val openTime = entry.getLong(0)
                    val open = entry.getBigDecimal(1)
                    val high = entry.getBigDecimal(2)
                    val low = entry.getBigDecimal(3)
                    val close = entry.getBigDecimal(4)
                    val volume = entry.getBigDecimal(5)
                    buckets.add(
                        DexKlineBucket(
                            symbol = DEFAULT_DEX_SYMBOL,
                            scale = scale,
                            windowStartMs = openTime,
                            open = open,
                            high = high,
                            low = low,
                            close = close,
                            volumeBase = volume,
                            tradeCount = entry.optInt(8, 0)
                        )
                    )
                }
                buckets.sortedByDescending { it.windowStartMs }
            }
        }
    }

    suspend fun fetchDepth(
        symbol: String = BINANCE_SPOT_SYMBOL,
        limit: Int = 20
    ): Pair<List<OrderBookEntry>, List<OrderBookEntry>> = withContext(Dispatchers.IO) {
        callWithFallback {
            val url = "$it/api/v3/depth?symbol=$symbol&limit=$limit"
            val request = Request.Builder().url(url).get().build()
            httpClient.newCall(request).execute().use { response ->
                val body = response.body?.string() ?: throw IOException("Empty depth response")
                val json = JSONObject(body)
                val bids = parseDepthArray(json.getJSONArray("bids"))
                val asks = parseDepthArray(json.getJSONArray("asks"))
                bids to asks
            }
        }
    }

    private fun JSONObject.getBigDecimal(key: String): BigDecimal =
        BigDecimal(optString(key, "0"))

    private fun JSONArray.getBigDecimal(index: Int): BigDecimal =
        BigDecimal(optString(index, "0"))

    private fun JSONArray.optInt(index: Int, default: Int): Int =
        if (index < length()) optString(index, null)?.toIntOrNull() ?: default else default

    private fun parseDepthArray(node: JSONArray): List<OrderBookEntry> {
        var cumulative = BigDecimal.ZERO
        val results = mutableListOf<OrderBookEntry>()
        for (i in 0 until node.length()) {
            val entry = node.getJSONArray(i)
            val price = entry.getBigDecimal(0)
            val qty = entry.getBigDecimal(1)
            cumulative = cumulative + qty
            results.add(OrderBookEntry(price = price, quantity = qty, cumulativeQuantity = cumulative))
        }
        return results
    }

    private suspend fun <T> callWithFallback(block: suspend (String) -> T): T = kotlinx.coroutines.coroutineScope {
        val channel = kotlinx.coroutines.channels.Channel<T>(kotlinx.coroutines.channels.Channel.UNLIMITED)
        val jobs = endpoints.map { endpoint ->
            launch {
                try {
                    val result = block(endpoint)
                    channel.send(result)
                } catch (e: Throwable) {
                    // Ignore individual failures
                }
            }
        }

        try {
            // Return the first successful result
            val result = channel.receive()
            jobs.forEach { it.cancel() }
            return@coroutineScope result
        } catch (e: kotlinx.coroutines.channels.ClosedReceiveChannelException) {
            throw IOException("All Binance endpoints failed")
        } finally {
            jobs.forEach { it.cancel() }
        }
    }
}
