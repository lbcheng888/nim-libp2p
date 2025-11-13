package com.example.libp2psmoke.dex

import android.util.Log
import java.io.IOException
import java.math.BigDecimal
import java.util.Locale
import java.util.concurrent.TimeUnit
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.Response
import okhttp3.WebSocket
import okhttp3.WebSocketListener
import org.json.JSONObject

class BinanceStreamClient(
    private val endpoints: List<String> = DEFAULT_WS_ENDPOINTS,
    okHttpClient: OkHttpClient? = null
) {
    interface Listener {
        fun onOpen()
        fun onKline(bucket: DexKlineBucket, isFinal: Boolean, eventTimeMs: Long)
        fun onClosed()
        fun onFailure(throwable: Throwable)
    }

    private val client: OkHttpClient =
        okHttpClient ?: OkHttpClient.Builder()
            .pingInterval(20, TimeUnit.SECONDS)
            .build()

    private var webSocket: WebSocket? = null
    private var listener: Listener? = null
    private var currentEndpointIndex = 0
    private var startContext: StartContext? = null

    fun start(
        symbol: String = BINANCE_SPOT_STREAM,
        interval: String,
        listener: Listener
    ) {
        stop()
        this.listener = listener
        this.startContext = StartContext(symbol, interval)
        this.currentEndpointIndex = 0
        connect()
    }

    fun stop() {
        webSocket?.close(1000, "shutdown")
        webSocket = null
        startContext = null
        listener = null
        currentEndpointIndex = 0
    }

    private fun connect() {
        val ctx = startContext ?: return
        val endpoint = endpoints.getOrNull(currentEndpointIndex)
            ?: run {
                listener?.onFailure(IOException("Binance stream endpoints exhausted"))
                return
            }
        val stream = "${ctx.symbol.lowercase(Locale.US)}@kline_${ctx.interval.lowercase(Locale.US)}"
        val url = endpoint.trimEnd('/') + "/$stream"
        val request = Request.Builder().url(url).build()
        Log.i(TAG, "Connecting Binance WS endpoint=$url")
        webSocket = client.newWebSocket(
            request,
            object : WebSocketListener() {
                override fun onOpen(webSocket: WebSocket, response: Response) {
                    listener?.onOpen()
                }

                override fun onMessage(webSocket: WebSocket, text: String) {
                    try {
                        val payload = JSONObject(text)
                        if (payload.optString("e") != "kline") {
                            return
                        }
                        val kline = payload.getJSONObject("k")
                        val bucket = DexKlineBucket(
                            symbol = DEFAULT_DEX_SYMBOL,
                            scale = DexKlineScale.fromLabel(kline.getString("i")),
                            windowStartMs = kline.getLong("t"),
                            open = kline.getBigDecimal("o"),
                            high = kline.getBigDecimal("h"),
                            low = kline.getBigDecimal("l"),
                            close = kline.getBigDecimal("c"),
                            volumeBase = kline.getBigDecimal("v"),
                            tradeCount = kline.optInt("n", 0)
                        )
                        val eventTime = payload.optLong("E", System.currentTimeMillis())
                        val isFinal = kline.optBoolean("x", false)
                        listener?.onKline(bucket, isFinal, eventTime)
                    } catch (err: Throwable) {
                        Log.w(TAG, "Failed to parse Binance stream payload", err)
                    }
                }

                override fun onClosed(webSocket: WebSocket, code: Int, reason: String) {
                    listener?.onClosed()
                }

                override fun onFailure(webSocket: WebSocket, t: Throwable, response: Response?) {
                    Log.w(TAG, "Binance stream endpoint failed (index=$currentEndpointIndex)", t)
                    if (
                        currentEndpointIndex + 1 < endpoints.size &&
                        startContext != null
                    ) {
                        currentEndpointIndex += 1
                        connect()
                    } else {
                        listener?.onFailure(t)
                    }
                }
            }
        )
    }

    companion object {
        private const val TAG = "BinanceStreamClient"
        private val DEFAULT_WS_ENDPOINTS = listOf(
            "wss://stream.binance.com:9443/ws",
            "wss://stream.binance.com/ws",
            "wss://data-stream.binance.vision/ws"
        )
    }

    private data class StartContext(
        val symbol: String,
        val interval: String
    )
}

private fun JSONObject.getBigDecimal(key: String): BigDecimal =
    BigDecimal(optString(key, "0"))
