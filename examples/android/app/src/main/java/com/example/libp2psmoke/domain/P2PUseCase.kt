package com.example.libp2psmoke.domain

import com.example.libp2psmoke.BuildConfig
import com.example.libp2psmoke.core.Constants
import com.example.libp2psmoke.core.DexError
import com.example.libp2psmoke.core.SecureLogger
import com.example.libp2psmoke.dex.DexRepositoryV2
import com.example.libp2psmoke.model.DirectMessage
import com.example.libp2psmoke.mixer.MixerSessionStatus
import com.example.libp2psmoke.native.NimBridge
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.json.JSONArray
import org.json.JSONObject
import java.io.File
import java.math.BigDecimal
import java.util.concurrent.ConcurrentHashMap
import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean

/**
 * P2P 网络业务用例
 * 封装所有与 NimBridge 的交互和 P2P 状态管理
 */
class P2PUseCase(
    private val dexRepository: DexRepositoryV2,
    private val dataDir: File
) {
    companion object {
        private const val TAG = "P2PUseCase"
    }
    
    private var handle: Long = 0L
    
    // 状态流
    private val _p2pState = MutableStateFlow(P2PState())
    val p2pState: StateFlow<P2PState> = _p2pState
    
    private val _messages = MutableStateFlow<List<DirectMessage>>(emptyList())
    val messages: StateFlow<List<DirectMessage>> = _messages
    
    private val _mixerSessions = MutableStateFlow<List<MixerSessionStatus>>(emptyList())
    val mixerSessions: StateFlow<List<MixerSessionStatus>> = _mixerSessions

    // Avoid calling back into native synchronously from the native event thread.
    // We track connected peers purely from events to prevent re-entrancy deadlocks.
    private val connectedPeerIds: MutableSet<String> = ConcurrentHashMap.newKeySet()
    private val dexInitialized = AtomicBoolean(false)
    
    /**
     * 启动节点
     */
    fun startNode(
        bootstrapPeers: List<String>,
        relayPeers: List<String>
    ) {
        if (handle != 0L) return
        dexInitialized.set(false)
        
        SecureLogger.i(TAG, "Starting P2P Node...")
        
        try {
            // 配置生成
            val extra = JSONObject().apply {
                put("bootstrapMultiaddrs", JSONArray(bootstrapPeers))
                put("relayMultiaddrs", JSONArray(relayPeers))
            }
            val config = JSONObject().apply {
                put("dataDir", dataDir.absolutePath)
                put("extra", extra)
            }
            
            handle = NimBridge.initNode(config.toString())
            if (handle == 0L) {
                throw DexError.NodeNotReady
            }

            // Register event listener as early as possible to avoid missing startup events.
            NimBridge.setEventListener { topic, payload ->
                handleNativeEvent(topic, payload)
            }

            // Huawei/Android: mDNS has shown severe native memory growth on some builds.
            // Keep it disabled by default; use explicit bootstrap/relay multiaddrs instead.
            runCatching {
                val enabled = BuildConfig.DEBUG && BuildConfig.P2P_ENABLE_MDNS
                val ok = NimBridge.setMdnsEnabled(handle, enabled)
                SecureLogger.i(TAG, "mdns enabled=$enabled -> $ok")
            }.onFailure { e ->
                SecureLogger.w(TAG, "mdns toggle failed", e)
            }
            
            val result = NimBridge.startNode(handle)
            if (result != 0) {
                val lastError = NimBridge.lastInitError().orEmpty().ifBlank { "start failed ($result)" }
                throw IllegalStateException(lastError)
            }
            
            // 更新本地 Peer ID
            val localPeerId = NimBridge.localPeerId(handle)
            _p2pState.update { it.copy(localPeerId = localPeerId, isRunning = true) }
            connectedPeerIds.clear()

            // Enable connection events for observability (best-effort)
            NimBridge.initializeConnectionEvents(handle)

            // Best-effort: explicitly dial bootstrap/relay multiaddrs (native side also tries).
            // This helps device bootstrapping when peer hints / mDNS are unstable.
            (bootstrapPeers + relayPeers)
                .map { it.trim() }
                .filter { it.startsWith("/") }
                .distinct()
                .forEach { addr ->
                    if (!addr.contains("/p2p/")) {
                        SecureLogger.w(TAG, "connectMultiaddr skipped (missing /p2p/ peerId) addr=$addr")
                        return@forEach
                    }
                    val rc = NimBridge.connectMultiaddr(handle, addr)
                    SecureLogger.i(TAG, "connectMultiaddr rc=$rc addr=$addr")
                }

            // Start DEX subsystem (orders/matches/trades)
            startDex()
            
        } catch (e: Exception) {
            SecureLogger.e(TAG, "Failed to start node", e)
            _p2pState.update { it.copy(error = e.message) }
            dexInitialized.set(false)
            if (handle != 0L) {
                try {
                    NimBridge.stopNode(handle)
                } catch (_: Exception) {
                }
                try {
                    NimBridge.freeNode(handle)
                } catch (_: Exception) {
                }
                handle = 0L
            }
        }
    }

    private fun startDex() {
        if (handle == 0L) return
        try {
            val enableSigning = if (BuildConfig.DEBUG) BuildConfig.DEX_ENABLE_SIGNING else true
            val allowUnsigned = if (BuildConfig.DEBUG) BuildConfig.DEX_ALLOW_UNSIGNED else false
            val config = JSONObject().apply {
                put("mode", BuildConfig.DEX_MODE)
                put("enableMixer", true)
                put("enableAutoTrade", BuildConfig.DEX_AUTO_TRADE)
                put("enableAutoMatch", BuildConfig.DEX_MODE.equals("matcher", ignoreCase = true))
                put("allowUnsigned", allowUnsigned)
                put("enableSigning", enableSigning)
            }
            val ok = NimBridge.dexInit(handle, config.toString())
            dexInitialized.set(ok)
            SecureLogger.i(TAG, "dexInit(mode=${BuildConfig.DEX_MODE}) -> $ok")
            if (ok && BuildConfig.DEBUG && BuildConfig.DEX_SELF_TEST) {
                CoroutineScope(Dispatchers.IO).launch {
                    delay(1500)
                    runCatching {
                        submitDexOrder(
                            side = "buy",
                            price = BigDecimal("65000"),
                            amountBase = BigDecimal("0.001"),
                            ttlMs = 60_000
                        )
                    }.onFailure { e ->
                        SecureLogger.w(TAG, "DEX self-test order failed", e)
                    }
                }
            }
        } catch (e: Exception) {
            SecureLogger.e(TAG, "dexInit failed", e)
            dexInitialized.set(false)
        }
    }
    
    /**
     * 停止节点
     */
    fun stopNode() {
        if (handle != 0L) {
            NimBridge.stopNode(handle)
            NimBridge.freeNode(handle)
            handle = 0L
        }
        connectedPeerIds.clear()
        dexInitialized.set(false)
        _p2pState.update { it.copy(isRunning = false, localPeerId = null, connectedPeers = 0, error = null) }
    }
    
    /**
     * 发送直接消息
     */
    suspend fun sendDirectMessage(peerId: String, body: String): Boolean = withContext(Dispatchers.IO) {
        if (handle == 0L) return@withContext false
        
        val messageId = UUID.randomUUID().toString()
        val envelope = JSONObject().apply {
            put("op", "dm")
            put("mid", messageId)
            put("from", _p2pState.value.localPeerId)
            put("body", body)
            put("timestamp_ms", System.currentTimeMillis())
        }.toString()
        
        val success = NimBridge.sendDirect(handle, peerId, envelope.toByteArray())
        
        if (success) {
            val msg = DirectMessage(
                peerId = peerId,
                messageId = messageId,
                fromSelf = true,
                body = body,
                timestampMs = System.currentTimeMillis(),
                transport = "nim-direct",
                acked = false
            )
            _messages.update { it + msg }
        }
        
        success
    }
    
    /**
     * 提交 Mixer 意图
     */
    suspend fun submitMixerIntent(asset: String, amount: Double, hops: Int) = withContext(Dispatchers.IO) {
        if (handle == 0L) return@withContext
        
        val json = JSONObject().apply {
            put("asset", asset)
            put("amount", amount)
            put("hops", hops)
            put("timestamp", System.currentTimeMillis())
            put("useOnion", true)
        }.toString()
        
        NimBridge.dexSubmitMixerIntent(handle, json)
        
        // Optimistic update
        val pending = MixerSessionStatus(
            sessionId = "pending-${System.currentTimeMillis()}",
            state = "Discovery (Onion)",
            participantCount = 1
        )
        _mixerSessions.update { it + pending }
    }
    
    private fun handleNativeEvent(topic: String, payload: String) {
        when (topic) {
            "dex.match" -> handleDexMatch(payload)
            "dex.trade" -> handleDexTrade(payload)
            "dex.mixer" -> handleMixerEvent(payload)
            "p2p.peer" -> handlePeerEvent(payload)
            "network_event" -> handleNetworkEvent(payload)
        }
    }

    private fun handleNetworkEvent(payload: String) {
        if (handle == 0L) return
        val trimmed = payload.trim()
        if (!trimmed.startsWith("{")) return
        try {
            val json = JSONObject(trimmed)
            when (json.optString("type")) {
                "ConnectionEstablished" -> {
                    val peerId = json.optString("peer_id")
                    if (peerId.isNotBlank()) {
                        connectedPeerIds.add(peerId)
                        _p2pState.update { it.copy(connectedPeers = connectedPeerIds.size) }
                    }
                }
                "ConnectionClosed" -> {
                    val peerId = json.optString("peer_id")
                    if (peerId.isNotBlank()) {
                        connectedPeerIds.remove(peerId)
                        _p2pState.update { it.copy(connectedPeers = connectedPeerIds.size) }
                    }
                }
            }
        } catch (_: Exception) {
            // ignore
        }
    }
    
    private fun handleDexMatch(payload: String) {
        val trimmed = payload.trim()
        if (trimmed.startsWith("{")) {
            try {
                val json = JSONObject(trimmed)
                val orderId = json.optString("orderId")
                val baseSymbol = json.optJSONObject("baseAsset")?.optString("symbol").orEmpty()
                val quoteSymbol = json.optJSONObject("quoteAsset")?.optString("symbol").orEmpty()
                val asset = if (baseSymbol.isNotBlank() && quoteSymbol.isNotBlank()) {
                    "$baseSymbol/$quoteSymbol"
                } else {
                    Constants.Dex.DEFAULT_SYMBOL
                }
                val price = json.optString("price").toBigDecimalOrNull() ?: return
                val amount = json.optString("amount").toBigDecimalOrNull() ?: return
                // Do not ingest as trade here; matcher also emits `dex.trade` which drives candles.
                // Keeping this as a lightweight debug hook avoids double-counting.
                SecureLogger.d(TAG, "dexMatch orderId=$orderId asset=$asset price=$price amount=$amount")
            } catch (_: Exception) {
                // fall through to legacy format
            }
            return
        }

        // legacy: match|orderId|matcherPeer|asset|price|amount
        val parts = payload.split("|")
        if (parts.size >= 6) {
            val orderId = parts[1]
            val asset = parts[3]
            val price = parts[4].toBigDecimalOrNull() ?: return
            val amount = parts[5].toBigDecimalOrNull() ?: return
            SecureLogger.d(TAG, "dexMatch(legacy) orderId=$orderId asset=$asset price=$price amount=$amount")
        }
    }

    private fun handleDexTrade(payload: String) {
        val trimmed = payload.trim()
        if (!trimmed.startsWith("{")) return
        try {
            val json = JSONObject(trimmed)
            val orderId = json.optString("orderId")
            val baseSymbol = json.optJSONObject("baseAsset")?.optString("symbol").orEmpty()
            val quoteSymbol = json.optJSONObject("quoteAsset")?.optString("symbol").orEmpty()
            val asset = if (baseSymbol.isNotBlank() && quoteSymbol.isNotBlank()) {
                "$baseSymbol/$quoteSymbol"
            } else {
                Constants.Dex.DEFAULT_SYMBOL
            }
            val price = json.optString("price").toBigDecimalOrNull() ?: return
            val amount = json.optString("amount").toBigDecimalOrNull() ?: return
            val timestampMs = json.optLong("createdAt", System.currentTimeMillis())
            dexRepository.ingestTrade(
                orderId = orderId,
                price = price,
                amount = amount,
                asset = asset,
                timestampMs = timestampMs
            )
        } catch (_: Exception) {
            // ignore
        }
    }

    suspend fun submitDexOrder(
        side: String,
        price: BigDecimal,
        amountBase: BigDecimal,
        ttlMs: Int = 60_000
    ) = withContext(Dispatchers.IO) {
        if (handle == 0L || !_p2pState.value.isRunning) {
            throw DexError.NodeNotReady
        }
        if (!dexInitialized.get()) {
            throw DexError.NodeNotReady
        }
        val json = JSONObject().apply {
            put("side", side.lowercase())
            put("price", price.toPlainString())
            put("amount", amountBase.toPlainString())
            put("ttlMs", ttlMs)
            put("timestamp", System.currentTimeMillis())
            put(
                "baseAsset",
                JSONObject().apply {
                    put("chainId", "BTC")
                    put("symbol", "BTC")
                    put("decimals", 8)
                    put("type", "AssetNative")
                }
            )
            put(
                "quoteAsset",
                JSONObject().apply {
                    put("chainId", "BSC")
                    put("symbol", "USDC")
                    put("decimals", 6)
                    put("type", "AssetERC20")
                }
            )
        }
        val payload = json.toString()
        val ok = NimBridge.dexSubmitOrder(handle, payload)
        if (!ok) {
            val err = NimBridge.lastDirectError(handle).orEmpty().ifBlank { "dexSubmitOrder failed" }
            SecureLogger.w(TAG, "dexSubmitOrder failed err=$err payloadLen=${payload.length}")
            throw DexError.InvalidOrder(orderId = null, reason = err)
        }
        SecureLogger.i(TAG, "dexSubmitOrder ok side=${side.lowercase()} price=${price.toPlainString()} amountBase=${amountBase.toPlainString()}")
    }
    
    private fun handleMixerEvent(payload: String) {
        // sessionId|state|participants|txId
        val parts = payload.split("|")
        if (parts.size >= 3) {
            val session = MixerSessionStatus(
                sessionId = parts[0],
                state = parts[1],
                participantCount = parts[2].toIntOrNull() ?: 0,
                txId = parts.getOrNull(3)
            )
            _mixerSessions.update { list ->
                val idx = list.indexOfFirst { it.sessionId == session.sessionId }
                if (idx >= 0) {
                    list.toMutableList().apply { set(idx, session) }
                } else {
                    list + session
                }
            }
        }
    }
    
    private fun handlePeerEvent(payload: String) {
        // Update peer counts, etc.
    }
    
    data class P2PState(
        val isRunning: Boolean = false,
        val localPeerId: String? = null,
        val connectedPeers: Int = 0,
        val error: String? = null
    )
}
