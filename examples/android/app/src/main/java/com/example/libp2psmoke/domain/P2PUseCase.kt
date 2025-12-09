package com.example.libp2psmoke.domain

import android.util.Log
import com.example.libp2psmoke.core.Constants
import com.example.libp2psmoke.core.DexError
import com.example.libp2psmoke.core.SecureLogger
import com.example.libp2psmoke.dex.DexRepositoryV2
import com.example.libp2psmoke.model.DirectMessage
import com.example.libp2psmoke.mixer.MixerSessionStatus
import com.example.libp2psmoke.model.NodeUiState
import com.example.libp2psmoke.native.NimBridge
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.json.JSONArray
import org.json.JSONObject
import java.io.File
import java.util.UUID

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
    private var pollJob: Job? = null
    
    // 状态流
    private val _p2pState = MutableStateFlow(P2PState())
    val p2pState: StateFlow<P2PState> = _p2pState
    
    private val _messages = MutableStateFlow<List<DirectMessage>>(emptyList())
    val messages: StateFlow<List<DirectMessage>> = _messages
    
    private val _mixerSessions = MutableStateFlow<List<MixerSessionStatus>>(emptyList())
    val mixerSessions: StateFlow<List<MixerSessionStatus>> = _mixerSessions
    
    /**
     * 启动节点
     */
    fun startNode(
        bootstrapPeers: List<String>,
        relayPeers: List<String>
    ) {
        if (handle != 0L) return
        
        SecureLogger.i(TAG, "Starting P2P Node...")
        
        try {
            // 配置生成
            val config = JSONObject().apply {
                put("dataDir", dataDir.absolutePath)
                put("bootstrap", JSONArray(bootstrapPeers))
                put("relays", JSONArray(relayPeers))
                // 更多配置...
            }
            
            handle = NimBridge.initNode(config.toString())
            if (handle == 0L) {
                throw DexError.NodeNotReady
            }
            
            val result = NimBridge.startNode(handle)
            if (result != 0) { // assuming 0 is success, fix based on native code
                // handle error
            }
            
            // 更新本地 Peer ID
            val localPeerId = NimBridge.localPeerId(handle)
            _p2pState.update { it.copy(localPeerId = localPeerId, isRunning = true) }
            
            // 设置事件监听
            NimBridge.setEventListener { topic, payload ->
                handleNativeEvent(topic, payload)
            }
            
            // 启动轮询
            startPolling()
            
        } catch (e: Exception) {
            SecureLogger.e(TAG, "Failed to start node", e)
            _p2pState.update { it.copy(error = e.message) }
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
        pollJob?.cancel()
        pollJob = null
        _p2pState.update { it.copy(isRunning = false) }
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
    
    // 内部处理
    private fun startPolling() {
        pollJob?.cancel()
        pollJob = CoroutineScope(Dispatchers.IO).launch {
            while (isActive && handle != 0L) {
                try {
                    val eventsJson = NimBridge.pollEvents(handle, 50)
                    if (!eventsJson.isNullOrEmpty() && eventsJson != "[]") {
                        val events = JSONArray(eventsJson)
                        for (i in 0 until events.length()) {
                            val event = events.getJSONObject(i)
                            val topic = event.optString("topic")
                            val payload = event.optString("payload")
                            if (topic.isNotEmpty()) {
                                handleNativeEvent(topic, payload)
                            }
                        }
                    }
                } catch (e: Exception) {
                    SecureLogger.e(TAG, "Polling error", e)
                }
                delay(100) // Poll interval
            }
        }
    }
    
    private fun handleNativeEvent(topic: String, payload: String) {
        when (topic) {
            "dex.match" -> handleDexMatch(payload)
            "dex.mixer" -> handleMixerEvent(payload)
            "p2p.peer" -> handlePeerEvent(payload)
        }
    }
    
    private fun handleDexMatch(payload: String) {
        // match|orderId|matcherPeer|asset|price|amount
        val parts = payload.split("|")
        if (parts.size >= 6) {
            val orderId = parts[1]
            val asset = parts[3]
            val price = parts[4].toBigDecimalOrNull() ?: return
            val amount = parts[5].toBigDecimalOrNull() ?: return
            dexRepository.ingestMatch(orderId, price, amount, asset)
        }
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

