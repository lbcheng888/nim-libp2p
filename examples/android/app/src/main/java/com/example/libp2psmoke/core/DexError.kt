package com.example.libp2psmoke.core

import java.math.BigDecimal

/**
 * DEX 统一错误类型定义
 * 使用密封类确保错误处理的完整性
 */
sealed class DexError : Exception() {
    
    // ═══════════════════════════════════════════════════════════════════
    // 网络错误
    // ═══════════════════════════════════════════════════════════════════
    
    /** 网络连接失败 */
    data class NetworkError(
        override val message: String,
        val endpoint: String? = null,
        override val cause: Throwable? = null
    ) : DexError()
    
    /** RPC 调用失败 */
    data class RpcError(
        val method: String,
        val code: Int? = null,
        override val message: String,
        override val cause: Throwable? = null
    ) : DexError()
    
    /** 请求超时 */
    data class TimeoutError(
        val operation: String,
        val timeoutMs: Long
    ) : DexError() {
        override val message = "$operation 超时 (${timeoutMs}ms)"
    }
    
    /** WebSocket 连接失败 */
    data class WebSocketError(
        override val message: String,
        val endpoint: String? = null,
        override val cause: Throwable? = null
    ) : DexError()
    
    // ═══════════════════════════════════════════════════════════════════
    // 交易错误
    // ═══════════════════════════════════════════════════════════════════
    
    /** 余额不足 */
    data class InsufficientFunds(
        val required: BigDecimal,
        val available: BigDecimal,
        val asset: String
    ) : DexError() {
        override val message = "余额不足: 需要 $required $asset, 当前 $available $asset"
    }
    
    /** 无效地址 */
    data class InvalidAddress(
        val address: String,
        val chain: String,
        val reason: String? = null
    ) : DexError() {
        override val message = "无效的 $chain 地址: $address" + (reason?.let { " ($it)" } ?: "")
    }
    
    /** 无效金额 */
    data class InvalidAmount(
        val amount: String,
        val reason: String
    ) : DexError() {
        override val message = "无效金额 $amount: $reason"
    }
    
    /** 交易失败 */
    data class TransactionFailed(
        val txHash: String?,
        override val message: String,
        override val cause: Throwable? = null
    ) : DexError()
    
    /** 交易确认超时 */
    data class ConfirmationTimeout(
        val txHash: String,
        val attempts: Int
    ) : DexError() {
        override val message = "交易 $txHash 确认超时 (尝试 $attempts 次)"
    }
    
    /** UTXO 不足 */
    data class NoUtxosAvailable(
        val address: String
    ) : DexError() {
        override val message = "地址 $address 没有可用的 UTXO"
    }
    
    // ═══════════════════════════════════════════════════════════════════
    // 订单错误
    // ═══════════════════════════════════════════════════════════════════
    
    /** 订单无效 */
    data class InvalidOrder(
        val orderId: String?,
        val reason: String
    ) : DexError() {
        override val message = "订单无效" + (orderId?.let { " ($it)" } ?: "") + ": $reason"
    }
    
    /** 订单过期 */
    data class OrderExpired(
        val orderId: String
    ) : DexError() {
        override val message = "订单已过期: $orderId"
    }
    
    /** 签名验证失败 */
    data class SignatureVerificationFailed(
        val orderId: String?,
        val reason: String? = null
    ) : DexError() {
        override val message = "签名验证失败" + (orderId?.let { " (订单: $it)" } ?: "") + (reason?.let { ": $it" } ?: "")
    }
    
    // ═══════════════════════════════════════════════════════════════════
    // 钱包错误
    // ═══════════════════════════════════════════════════════════════════
    
    /** 无效私钥 */
    data class InvalidPrivateKey(
        val reason: String
    ) : DexError() {
        override val message = "无效私钥: $reason"
    }
    
    /** 钱包未初始化 */
    data class WalletNotInitialized(
        val chain: String
    ) : DexError() {
        override val message = "$chain 钱包未初始化"
    }
    
    // ═══════════════════════════════════════════════════════════════════
    // 系统错误
    // ═══════════════════════════════════════════════════════════════════
    
    /** 节点未就绪 */
    object NodeNotReady : DexError() {
        override val message = "P2P 节点未就绪"
    }
    
    /** 配置错误 */
    data class ConfigurationError(
        val field: String,
        val reason: String
    ) : DexError() {
        override val message = "配置错误 [$field]: $reason"
    }
    
    /** 未知错误 */
    data class Unknown(
        override val message: String,
        override val cause: Throwable? = null
    ) : DexError()
    
    // ═══════════════════════════════════════════════════════════════════
    // 工具方法
    // ═══════════════════════════════════════════════════════════════════
    
    /** 获取用户友好的错误消息 */
    fun toUserMessage(): String = when (this) {
        is NetworkError -> "网络连接失败，请检查网络"
        is RpcError -> "服务请求失败: $message"
        is TimeoutError -> "请求超时，请稍后重试"
        is WebSocketError -> "实时连接断开"
        is InsufficientFunds -> message
        is InvalidAddress -> message
        is InvalidAmount -> message
        is TransactionFailed -> "交易失败: $message"
        is ConfirmationTimeout -> "交易确认超时"
        is NoUtxosAvailable -> "账户余额不足"
        is InvalidOrder -> message
        is OrderExpired -> "订单已过期"
        is SignatureVerificationFailed -> "签名验证失败"
        is InvalidPrivateKey -> "私钥格式错误"
        is WalletNotInitialized -> message
        is NodeNotReady -> message
        is ConfigurationError -> message
        is Unknown -> message
    }
    
    companion object {
        /** 从普通异常转换为 DexError */
        fun from(throwable: Throwable): DexError = when (throwable) {
            is DexError -> throwable
            is java.net.SocketTimeoutException -> TimeoutError("网络请求", Constants.Network.REQUEST_TIMEOUT_MS)
            is java.net.UnknownHostException -> NetworkError("无法解析主机", cause = throwable)
            is java.io.IOException -> NetworkError(throwable.message ?: "IO错误", cause = throwable)
            else -> Unknown(throwable.message ?: "未知错误", throwable)
        }
    }
}

/**
 * Result 扩展 - 将 Throwable 转换为 DexError
 */
inline fun <T> Result<T>.mapError(): Result<T> = 
    this.onFailure { throw DexError.from(it) }

/**
 * 安全执行并返回 Result
 */
inline fun <T> runCatchingDex(block: () -> T): Result<T> = 
    runCatching(block).mapError()





