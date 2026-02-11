package com.example.libp2psmoke.core

import kotlinx.coroutines.*
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlin.math.min

/**
 * 重连管理器
 * 管理 WebSocket 和网络连接的自动重连
 */
class ReconnectManager(
    private val scope: CoroutineScope,
    private val initialDelayMs: Long = Constants.Network.INITIAL_RECONNECT_DELAY_MS,
    private val maxDelayMs: Long = Constants.Network.MAX_RECONNECT_DELAY_MS,
    private val multiplier: Double = 2.0,
    private val maxAttempts: Int = -1 // -1 = 无限重试
) {
    
    enum class State {
        IDLE,
        CONNECTING,
        CONNECTED,
        DISCONNECTED,
        RECONNECTING,
        FAILED
    }
    
    private val _state = MutableStateFlow(State.IDLE)
    val state: StateFlow<State> = _state
    
    private val _attemptCount = MutableStateFlow(0)
    val attemptCount: StateFlow<Int> = _attemptCount
    
    private var currentDelay = initialDelayMs
    private var reconnectJob: Job? = null
    private var connectAction: (suspend () -> Boolean)? = null
    
    /**
     * 设置连接动作
     */
    fun setConnectAction(action: suspend () -> Boolean) {
        connectAction = action
    }
    
    /**
     * 开始连接
     */
    fun connect() {
        if (_state.value == State.CONNECTING || _state.value == State.CONNECTED) {
            SecureLogger.d(TAG, "已在连接中或已连接，跳过")
            return
        }
        
        cancelReconnect()
        _state.value = State.CONNECTING
        _attemptCount.value = 0
        currentDelay = initialDelayMs
        
        reconnectJob = scope.launch {
            performConnect()
        }
    }
    
    /**
     * 触发重连
     */
    fun scheduleReconnect() {
        if (_state.value == State.CONNECTING || _state.value == State.RECONNECTING) {
            SecureLogger.d(TAG, "已在重连中，跳过")
            return
        }
        
        if (maxAttempts >= 0 && _attemptCount.value >= maxAttempts) {
            SecureLogger.w(TAG, "达到最大重试次数 ($maxAttempts)，停止重连")
            _state.value = State.FAILED
            return
        }
        
        _state.value = State.RECONNECTING
        
        reconnectJob = scope.launch {
            SecureLogger.d(TAG, "计划在 ${currentDelay}ms 后重连 (尝试 ${_attemptCount.value + 1})")
            delay(currentDelay)
            performConnect()
        }
    }
    
    /**
     * 标记已连接
     */
    fun onConnected() {
        _state.value = State.CONNECTED
        _attemptCount.value = 0
        currentDelay = initialDelayMs
    }
    
    /**
     * 标记断开连接
     */
    fun onDisconnected() {
        if (_state.value != State.FAILED) {
            _state.value = State.DISCONNECTED
        }
    }
    
    /**
     * 取消重连
     */
    fun cancelReconnect() {
        reconnectJob?.cancel()
        reconnectJob = null
    }
    
    /**
     * 重置状态
     */
    fun reset() {
        cancelReconnect()
        _state.value = State.IDLE
        _attemptCount.value = 0
        currentDelay = initialDelayMs
    }
    
    private suspend fun performConnect() {
        val action = connectAction ?: run {
            SecureLogger.w(TAG, "未设置连接动作")
            _state.value = State.FAILED
            return
        }
        
        try {
            _attemptCount.value++
            SecureLogger.d(TAG, "执行连接 (尝试 ${_attemptCount.value})")
            
            val success = action()
            
            if (success) {
                onConnected()
            } else {
                // 连接失败，增加延迟并重试
                currentDelay = min((currentDelay * multiplier).toLong(), maxDelayMs)
                scheduleReconnect()
            }
        } catch (e: CancellationException) {
            SecureLogger.d(TAG, "连接被取消")
            throw e
        } catch (e: Exception) {
            SecureLogger.w(TAG, "连接异常", e)
            currentDelay = min((currentDelay * multiplier).toLong(), maxDelayMs)
            scheduleReconnect()
        }
    }
    
    companion object {
        private const val TAG = "ReconnectManager"
    }
}

/**
 * 连接状态监听器
 */
interface ConnectionListener {
    fun onConnected()
    fun onDisconnected()
    fun onReconnecting(attempt: Int, delayMs: Long)
    fun onFailed(reason: String)
}





