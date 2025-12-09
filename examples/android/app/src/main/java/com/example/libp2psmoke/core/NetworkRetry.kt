package com.example.libp2psmoke.core

import kotlinx.coroutines.delay
import kotlin.math.min
import kotlin.math.pow

/**
 * 网络重试工具
 * 提供指数退避重试机制
 */
object NetworkRetry {
    
    /**
     * 重试配置
     */
    data class RetryConfig(
        val maxAttempts: Int = Constants.Network.MAX_RETRY_COUNT,
        val initialDelayMs: Long = Constants.Network.INITIAL_RECONNECT_DELAY_MS,
        val maxDelayMs: Long = Constants.Network.MAX_RECONNECT_DELAY_MS,
        val multiplier: Double = 2.0,
        val shouldRetry: (Throwable) -> Boolean = { true }
    )
    
    /**
     * 默认配置
     */
    val defaultConfig = RetryConfig()
    
    /**
     * 带指数退避的重试执行
     */
    suspend fun <T> withRetry(
        config: RetryConfig = defaultConfig,
        block: suspend (attempt: Int) -> T
    ): T {
        var lastException: Throwable? = null
        var currentDelay = config.initialDelayMs
        
        repeat(config.maxAttempts) { attempt ->
            try {
                return block(attempt)
            } catch (e: Throwable) {
                lastException = e
                
                // 检查是否应该重试
                if (!config.shouldRetry(e)) {
                    throw e
                }
                
                // 最后一次尝试不需要等待
                if (attempt < config.maxAttempts - 1) {
                    SecureLogger.d("NetworkRetry", "尝试 ${attempt + 1} 失败，${currentDelay}ms 后重试")
                    delay(currentDelay)
                    currentDelay = min(
                        (currentDelay * config.multiplier).toLong(),
                        config.maxDelayMs
                    )
                }
            }
        }
        
        throw lastException ?: IllegalStateException("Retry failed without exception")
    }
    
    /**
     * 简化版重试 (使用默认配置)
     */
    suspend fun <T> retry(
        maxAttempts: Int = 3,
        block: suspend () -> T
    ): T = withRetry(defaultConfig.copy(maxAttempts = maxAttempts)) { block() }
    
    /**
     * 可恢复的网络错误判断
     */
    fun isRecoverable(throwable: Throwable): Boolean = when (throwable) {
        is java.net.SocketTimeoutException -> true
        is java.net.ConnectException -> true
        is java.net.UnknownHostException -> false // DNS 错误通常不会自动恢复
        is java.io.IOException -> true
        is DexError.NetworkError -> true
        is DexError.TimeoutError -> true
        is DexError.RpcError -> throwable.code?.let { it >= 500 } ?: false // 5xx 服务器错误可重试
        else -> false
    }
    
    /**
     * 计算指数退避延迟
     */
    fun calculateBackoff(
        attempt: Int,
        initialDelayMs: Long = Constants.Network.INITIAL_RECONNECT_DELAY_MS,
        maxDelayMs: Long = Constants.Network.MAX_RECONNECT_DELAY_MS,
        multiplier: Double = 2.0
    ): Long {
        val delay = initialDelayMs * multiplier.pow(attempt.toDouble())
        return min(delay.toLong(), maxDelayMs)
    }
}

/**
 * 扩展函数：带重试执行
 */
suspend fun <T> retrying(
    maxAttempts: Int = 3,
    block: suspend () -> T
): T = NetworkRetry.retry(maxAttempts, block)

/**
 * 扩展函数：带可恢复性检查的重试
 */
suspend fun <T> retryIfRecoverable(
    maxAttempts: Int = 3,
    block: suspend () -> T
): T = NetworkRetry.withRetry(
    config = NetworkRetry.RetryConfig(
        maxAttempts = maxAttempts,
        shouldRetry = NetworkRetry::isRecoverable
    )
) { block() }





