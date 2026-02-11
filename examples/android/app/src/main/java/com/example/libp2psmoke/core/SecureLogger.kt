package com.example.libp2psmoke.core

import android.util.Log
import com.example.libp2psmoke.BuildConfig

/**
 * 安全日志工具
 * - 生产环境自动禁用 DEBUG/VERBOSE 日志
 * - 自动脱敏敏感信息
 * - 防止敏感数据泄露
 */
object SecureLogger {
    
    private val isDebugBuild = BuildConfig.DEBUG
    
    // 敏感字段模式 (正则)
    private val sensitivePatterns = listOf(
        // 私钥模式
        Regex("(privateKey|private_key|privKey|priv_key|secret|secretKey)\\s*[:=]\\s*[\"']?([a-fA-F0-9]{64})[\"']?", RegexOption.IGNORE_CASE),
        Regex("0x[a-fA-F0-9]{64}"), // 完整的 hex 私钥
        // WIF 格式私钥 (BTC)
        Regex("[5KL][1-9A-HJ-NP-Za-km-z]{50,51}"),
        // 助记词模式
        Regex("(mnemonic|seed|phrase)\\s*[:=]\\s*[\"']?([a-z\\s]{20,})[\"']?", RegexOption.IGNORE_CASE),
        // 密码模式
        Regex("(password|passwd|pwd)\\s*[:=]\\s*[\"']?([^\"'\\s]{4,})[\"']?", RegexOption.IGNORE_CASE),
        // API Key 模式
        Regex("(api[_-]?key|apikey|access[_-]?key)\\s*[:=]\\s*[\"']?([a-zA-Z0-9]{16,})[\"']?", RegexOption.IGNORE_CASE),
        // Bearer Token
        Regex("Bearer\\s+[a-zA-Z0-9\\-_\\.]+", RegexOption.IGNORE_CASE)
    )
    
    // 需要脱敏的字段名
    private val sensitiveFields = setOf(
        "privateKey", "private_key", "privKey", "priv_key",
        "secret", "secretKey", "secret_key",
        "password", "passwd", "pwd",
        "mnemonic", "seed", "phrase",
        "apiKey", "api_key", "accessKey", "access_key",
        "authorization", "token"
    )
    
    /**
     * Debug 日志 (仅在 DEBUG 构建中输出)
     */
    fun d(tag: String, message: String) {
        if (isDebugBuild) {
            Log.d(tag, sanitize(message))
        }
    }
    
    /**
     * Info 日志
     */
    fun i(tag: String, message: String) {
        Log.i(tag, sanitize(message))
    }
    
    /**
     * Warning 日志
     */
    fun w(tag: String, message: String, throwable: Throwable? = null) {
        if (throwable != null) {
            Log.w(tag, sanitize(message), throwable)
        } else {
            Log.w(tag, sanitize(message))
        }
    }
    
    /**
     * Error 日志
     */
    fun e(tag: String, message: String, throwable: Throwable? = null) {
        if (throwable != null) {
            Log.e(tag, sanitize(message), throwable)
        } else {
            Log.e(tag, sanitize(message))
        }
    }
    
    /**
     * Verbose 日志 (仅在 DEBUG 构建中输出)
     */
    fun v(tag: String, message: String) {
        if (isDebugBuild) {
            Log.v(tag, sanitize(message))
        }
    }
    
    /**
     * 脱敏处理
     */
    fun sanitize(input: String): String {
        if (!isDebugBuild) {
            // 生产环境进行完整脱敏
            var result = input
            sensitivePatterns.forEach { pattern ->
                result = pattern.replace(result) { match ->
                    maskSensitiveValue(match.value)
                }
            }
            return result
        }
        // Debug 构建仍然进行部分脱敏 (私钥等)
        var result = input
        // 只脱敏最敏感的信息 (私钥)
        result = Regex("0x[a-fA-F0-9]{64}").replace(result) { "0x***REDACTED***" }
        result = Regex("[5KL][1-9A-HJ-NP-Za-km-z]{50,51}").replace(result) { "***WIF_KEY***" }
        return result
    }
    
    /**
     * 安全地记录对象 (自动过滤敏感字段)
     */
    fun logObject(tag: String, prefix: String, obj: Any?, level: Int = Log.DEBUG) {
        if (!isDebugBuild && level <= Log.DEBUG) return
        
        val safeString = when (obj) {
            null -> "null"
            is Map<*, *> -> obj.filterKeys { key ->
                key.toString().lowercase() !in sensitiveFields.map { it.lowercase() }
            }.toString()
            else -> sanitize(obj.toString())
        }
        
        when (level) {
            Log.DEBUG -> d(tag, "$prefix: $safeString")
            Log.INFO -> i(tag, "$prefix: $safeString")
            Log.WARN -> w(tag, "$prefix: $safeString")
            Log.ERROR -> e(tag, "$prefix: $safeString")
            else -> v(tag, "$prefix: $safeString")
        }
    }
    
    /**
     * 脱敏地址 (显示前6后4位)
     */
    fun maskAddress(address: String?): String {
        if (address == null) return "null"
        if (address.length <= 10) return "***"
        return "${address.take(6)}...${address.takeLast(4)}"
    }
    
    /**
     * 脱敏交易哈希 (显示前10后6位)
     */
    fun maskTxHash(hash: String?): String {
        if (hash == null) return "null"
        if (hash.length <= 16) return hash
        return "${hash.take(10)}...${hash.takeLast(6)}"
    }
    
    /**
     * 脱敏 Peer ID
     */
    fun maskPeerId(peerId: String?): String {
        if (peerId == null) return "null"
        if (peerId.length <= 16) return peerId
        return "${peerId.take(8)}...${peerId.takeLast(4)}"
    }
    
    /**
     * 脱敏金额 (生产环境隐藏具体数值)
     */
    fun maskAmount(amount: String?, showInDebug: Boolean = true): String {
        if (amount == null) return "null"
        return if (isDebugBuild && showInDebug) amount else "***"
    }
    
    private fun maskSensitiveValue(value: String): String {
        return when {
            value.length > 20 -> "${value.take(4)}***REDACTED***${value.takeLast(4)}"
            value.length > 8 -> "${value.take(2)}***${value.takeLast(2)}"
            else -> "***"
        }
    }
}

/**
 * 扩展函数：安全日志
 */
fun String.sanitize(): String = SecureLogger.sanitize(this)
fun String.maskAsAddress(): String = SecureLogger.maskAddress(this)
fun String.maskAsTxHash(): String = SecureLogger.maskTxHash(this)
fun String.maskAsPeerId(): String = SecureLogger.maskPeerId(this)





