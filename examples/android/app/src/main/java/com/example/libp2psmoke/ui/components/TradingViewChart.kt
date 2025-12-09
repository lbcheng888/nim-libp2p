package com.example.libp2psmoke.ui.components

import android.annotation.SuppressLint
import android.content.Context
import android.util.Log
import android.webkit.ConsoleMessage
import android.webkit.WebChromeClient
import android.webkit.WebResourceRequest
import android.webkit.WebResourceResponse
import android.webkit.WebView
import android.webkit.WebViewClient
import androidx.compose.runtime.Composable
import androidx.compose.runtime.remember
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.toArgb
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.viewinterop.AndroidView
import androidx.compose.material3.MaterialTheme
import com.example.libp2psmoke.dex.DexKlineBucket
import com.example.libp2psmoke.dex.toTradingViewJson

@SuppressLint("SetJavaScriptEnabled")
@Composable
fun TradingViewChart(
    candles: List<DexKlineBucket>,
    candlesJson: String,
    interval: String,
    streamConnected: Boolean,
    lastUpdateMs: Long,
    marketSource: String,
    latencyMs: Long,
    modifier: Modifier = Modifier
) {
    val context = LocalContext.current
    val bgColor = MaterialTheme.colorScheme.background.toArgb()
    val textColor = MaterialTheme.colorScheme.onBackground.toArgb()
    val gridColor = MaterialTheme.colorScheme.surfaceVariant.toArgb()
    
    val htmlContent = remember(context, bgColor, textColor) { 
        loadTradingViewHtml(context, bgColor, textColor, gridColor) 
    }
    val json = if (candlesJson.length > 2) candlesJson else remember(candles) { candles.toTradingViewJson() }
    val escapedJson = remember(json) { json.escapeForJs() }
    val bridge = remember { TradingViewBridge() }

    AndroidView(
        factory = {
            WebView(context).apply {
                layoutParams = android.view.ViewGroup.LayoutParams(
                    android.view.ViewGroup.LayoutParams.MATCH_PARENT,
                    android.view.ViewGroup.LayoutParams.MATCH_PARENT
                )
                bridge.attachWebView(this)
                settings.javaScriptEnabled = true
                settings.domStorageEnabled = true
                settings.allowFileAccess = true
                settings.allowFileAccessFromFileURLs = true
                settings.allowUniversalAccessFromFileURLs = true
                setLayerType(android.view.View.LAYER_TYPE_SOFTWARE, null)
                setBackgroundColor(bgColor)
                
                webChromeClient = object : WebChromeClient() {
                    override fun onConsoleMessage(consoleMessage: ConsoleMessage): Boolean {
                        Log.d("TradingViewChart", "[WebView] ${consoleMessage.message()}")
                        return super.onConsoleMessage(consoleMessage)
                    }
                }
                webViewClient = object : WebViewClient() {
                    override fun onPageFinished(view: WebView?, url: String?) {
                        super.onPageFinished(view, url)
                        Log.d("TradingViewChart", "onPageFinished: $url")
                        bridge.onPageReady()
                    }

                    override fun shouldInterceptRequest(view: WebView?, request: WebResourceRequest?): WebResourceResponse? {
                        val url = request?.url?.toString() ?: return null
                        if (url.contains("lightweight-charts.standalone.production.js")) {
                            return try {
                                Log.d("TradingViewChart", "Intercepting JS library request")
                                val stream = context.assets.open("lightweight-charts.standalone.production.js")
                                WebResourceResponse("text/javascript", "UTF-8", stream)
                            } catch (e: Exception) {
                                Log.e("TradingViewChart", "Asset load failed", e)
                                null
                            }
                        }
                        return super.shouldInterceptRequest(view, request)
                    }
                }
                
                loadDataWithBaseURL("https://app.assets/", htmlContent, "text/html", "utf-8", null)
            }
        },
        update = {
            Log.d("TradingViewChart", "updateState: payload len=${escapedJson.length} connected=$streamConnected interval=$interval")
            bridge.updateState(escapedJson, streamConnected, lastUpdateMs, marketSource, latencyMs, interval)
            
            val view = it
            val currentTag = view.tag as? String
            val newTag = htmlContent.hashCode().toString()
            if (currentTag != newTag) {
                Log.i("TradingViewChart", "Theme/HTML changed, reloading WebView content...")
                view.tag = newTag
                view.setBackgroundColor(bgColor)
                view.loadDataWithBaseURL("https://app.assets/", htmlContent, "text/html", "utf-8", null)
            } else {
                bridge.flush()
            }
        },
        modifier = modifier
    )
}

private fun String.escapeForJs(): String = this.replace("\\", "\\\\").replace("'", "\\'")

private fun loadTradingViewHtml(context: Context, bgColor: Int, textColor: Int, gridColor: Int): String {
    val bgHex = String.format("#%06X", 0xFFFFFF and bgColor)
    val textHex = String.format("#%06X", 0xFFFFFF and textColor)
    val gridHex = String.format("#%06X", 0xFFFFFF and gridColor)

    return """
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8"/>
            <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no"/>
            <style>
                html, body { width: 100%; height: 100%; margin: 0; padding: 0; background-color: $bgHex; overflow: hidden; }
                body { display: flex; flex-direction: column; }
                #chart-container { flex: 1; width: 100%; position: relative; min-height: 0; }
            </style>
            <script src="https://app.assets/lightweight-charts.standalone.production.js"></script>
        </head>
        <body>
            <div id="chart-container"></div>
            
            <script>
                const log = (msg) => console.log("[JS] " + msg);
                window.onerror = (msg) => log("ERR: " + msg);

                var chart;
                var candleSeries;
                var container = document.getElementById('chart-container');
                
                try {
                    if (typeof LightweightCharts === 'undefined') {
                        throw new Error("LightweightCharts not loaded");
                    }
                    log("Lib Loaded");

                    chart = LightweightCharts.createChart(container, {
                        layout: {
                            backgroundColor: '$bgHex',
                            textColor: '$textHex',
                        },
                        grid: {
                            vertLines: { color: '$gridHex' },
                            horzLines: { color: '$gridHex' },
                        },
                        timeScale: {
                            timeVisible: true,
                            secondsVisible: false,
                        },
                        rightPriceScale: {
                            borderColor: '$gridHex',
                        },
                        crosshair: {
                            mode: LightweightCharts.CrosshairMode.Normal,
                        },
                    });
                    
                    candleSeries = chart.addCandlestickSeries({
                        upColor: '#00D68F',
                        downColor: '#FF4757',
                        borderUpColor: '#00D68F',
                        borderDownColor: '#FF4757',
                        wickUpColor: '#00D68F',
                        wickDownColor: '#FF4757',
                    });
                    
                    log("Chart Created");
                    
                    const resizeObserver = new ResizeObserver(entries => {
                        if (chart && container) {
                            const w = container.clientWidth;
                            const h = container.clientHeight;
                            if (w > 0 && h > 0) {
                                chart.resize(w, h);
                            }
                        }
                    });
                    resizeObserver.observe(container);

                } catch(e) {
                    log("CHART ERROR: " + e.message);
                }

                window.renderCandles = function(jsonData, resetView) {
                    try {
                        const parsed = JSON.parse(jsonData);
                        if (!Array.isArray(parsed)) return;
                        
                        const candles = parsed.map(item => ({
                            time: Number(item.time),
                            open: Number(item.open),
                            high: Number(item.high),
                            low: Number(item.low),
                            close: Number(item.close)
                        }));
                        
                        if (candleSeries) {
                            candleSeries.setData(candles);
                            if (resetView) {
                                log("Resetting time scale for new interval");
                                chart.timeScale().fitContent();
                            }
                        }
                    } catch (e) { 
                        log("RENDER ERR: " + e.message);
                    }
                };
            </script>
        </body>
        </html>
    """.trimIndent()
}

private class TradingViewBridge {
    private var webView: WebView? = null
    private var pageReady = false
    private var payload: String? = null
    private var currentInterval: String? = null
    private var shouldReset: Boolean = false
    
    fun attachWebView(view: WebView) { webView = view }
    fun onPageReady() { pageReady = true; flush() }
    fun updateState(payload: String, connected: Boolean, time: Long, source: String, latency: Long, interval: String) {
        this.payload = payload
        if (this.currentInterval != interval) {
            this.currentInterval = interval
            this.shouldReset = true
        }
    }
    fun flush() {
        if (pageReady && payload != null) {
            val reset = shouldReset
            shouldReset = false
            webView?.post {
                webView?.evaluateJavascript("window.renderCandles('$payload', $reset);", null)
            }
        }
    }
}
