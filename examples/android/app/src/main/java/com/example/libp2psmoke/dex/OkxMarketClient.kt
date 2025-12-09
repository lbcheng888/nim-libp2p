package com.example.libp2psmoke.dex

import java.io.IOException
import java.math.BigDecimal
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import okhttp3.OkHttpClient
import okhttp3.Request
import org.json.JSONArray
import org.json.JSONObject

class OkxMarketClient(
    private val httpClient: OkHttpClient = OkHttpClient.Builder()
        .connectTimeout(5, java.util.concurrent.TimeUnit.SECONDS)
        .readTimeout(5, java.util.concurrent.TimeUnit.SECONDS)
        .build()
) {
    suspend fun fetchTicker(instId: String = OKX_SPOT_INST): BinanceTicker =
        withContext(Dispatchers.IO) {
            val url = "$BASE_URL/api/v5/market/ticker?instId=$instId"
            val body = execute(url)
            val data = JSONObject(body).getJSONArray("data")
            if (data.length() == 0) throw IOException("OKX ticker empty")
            val ticker = data.getJSONObject(0)
            BinanceTicker(
                lastPrice = ticker.optBigDecimal("last", BigDecimal.ZERO),
                priceChangePercent = ticker.optBigDecimal("pctChg", BigDecimal.ZERO),
                highPrice = ticker.optBigDecimal("high24h", BigDecimal.ZERO),
                lowPrice = ticker.optBigDecimal("low24h", BigDecimal.ZERO),
                volumeBase = ticker.optBigDecimal("vol24h", BigDecimal.ZERO),
                volumeQuote = ticker.optBigDecimal("volCcy24h", BigDecimal.ZERO),
                closeTimeMs = ticker.optLong("ts")
            )
        }

    suspend fun fetchKlines(
        instId: String = OKX_SPOT_INST,
        interval: String = "1m",
        limit: Int = 300
    ): List<DexKlineBucket> = withContext(Dispatchers.IO) {
        val bar = okxBar(interval)
        val url = "$BASE_URL/api/v5/market/candles?instId=$instId&bar=$bar&limit=$limit"
        val body = execute(url)
        val data = JSONObject(body).getJSONArray("data")
        val buckets = mutableListOf<DexKlineBucket>()
        val scale = DexKlineScale.fromLabel(interval)
        for (i in 0 until data.length()) {
            val entry = data.getJSONArray(i)
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
                    tradeCount = 0
                )
            )
        }
        buckets.sortedByDescending { it.windowStartMs }
    }

    suspend fun fetchDepth(
        instId: String = OKX_SPOT_INST,
        limit: Int = 20
    ): Pair<List<OrderBookEntry>, List<OrderBookEntry>> = withContext(Dispatchers.IO) {
        val url = "$BASE_URL/api/v5/market/books?instId=$instId&sz=$limit"
        val body = execute(url)
        val data = JSONObject(body).getJSONArray("data")
        if (data.length() == 0) throw IOException("OKX depth empty")
        val node = data.getJSONObject(0)
        val bids = parseDepth(node.getJSONArray("bids"))
        val asks = parseDepth(node.getJSONArray("asks"))
        bids to asks
    }

    private fun parseDepth(array: JSONArray): List<OrderBookEntry> {
        var cumulative = BigDecimal.ZERO
        val out = mutableListOf<OrderBookEntry>()
        for (i in 0 until array.length()) {
            val entry = array.getJSONArray(i)
            val price = entry.getBigDecimal(0)
            val qty = entry.getBigDecimal(1)
            cumulative = cumulative + qty
            out.add(
                OrderBookEntry(
                    price = price,
                    quantity = qty,
                    cumulativeQuantity = cumulative
                )
            )
        }
        return out
    }

    private fun okxBar(interval: String): String =
        when (interval.lowercase()) {
            "1s" -> "1s"
            "1m" -> "1m"
            "15m" -> "15m"
            "1h" -> "1H"
            "4h" -> "4H"
            "1d" -> "1D"
            else -> "1m"
        }

    private fun execute(url: String): String {
        val request = Request.Builder().url(url).get().build()
        httpClient.newCall(request).execute().use { resp ->
            val body = resp.body?.string() ?: throw IOException("OKX empty body: $url")
            if (!resp.isSuccessful) {
                throw IOException("OKX HTTP ${resp.code}: $body")
            }
            val json = JSONObject(body)
            if (json.optString("code") != "0") {
                throw IOException("OKX error ${json.optString("code")}: ${json.optString("msg")}")
            }
            return body
        }
    }

    companion object {
        private const val BASE_URL = "https://www.okx.com"
        private const val OKX_SPOT_INST = "BTC-USDC"
    }
}

private fun JSONObject.optBigDecimal(key: String, default: BigDecimal): BigDecimal =
    optString(key)?.takeIf { it.isNotBlank() }?.toBigDecimalOrNull() ?: default

private fun JSONArray.getBigDecimal(index: Int): BigDecimal =
    BigDecimal(optString(index, "0"))
