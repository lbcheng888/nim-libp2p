package com.example.libp2psmoke.dex

import com.example.libp2psmoke.core.Constants
import com.example.libp2psmoke.core.DexError
import com.example.libp2psmoke.core.SecureLogger
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import org.json.JSONArray
import org.json.JSONObject
import java.io.IOException
import java.math.BigInteger
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

/**
 * 基础 RPC 客户端抽象类
 * 提供通用的 JSON-RPC 调用功能
 */
abstract class BaseRpcClient(
    protected val httpClient: OkHttpClient = defaultHttpClient()
) {
    protected val mediaType = "application/json; charset=utf-8".toMediaType()
    protected val requestCounter = AtomicLong(0)
    
    abstract val tag: String
    
    /**
     * 执行 RPC 调用，返回原始结果
     */
    protected suspend fun rpcCall(
        url: String,
        method: String,
        params: JSONArray,
        jsonRpcVersion: String = "2.0",
        authHeader: String? = null,
        timeoutMs: Long = Constants.Network.REQUEST_TIMEOUT_MS
    ): Any? = withContext(Dispatchers.IO) {
        val requestId = requestCounter.incrementAndGet()
        
        val payload = JSONObject().apply {
            put("jsonrpc", jsonRpcVersion)
            put("id", requestId)
            put("method", method)
            put("params", params)
        }
        
        val requestBuilder = Request.Builder()
            .url(url)
            .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
            .header("Accept", "application/json")
            .header("Content-Type", "application/json")
            .post(payload.toString().toRequestBody(mediaType))
        
        authHeader?.let { requestBuilder.header("Authorization", it) }
        
        SecureLogger.d(tag, "RPC Call: $method (id=$requestId) -> $url")
        
        val result = withTimeoutOrNull(timeoutMs) {
            executeRequest(requestBuilder.build(), method)
        }
        
        result ?: throw DexError.TimeoutError(method, timeoutMs)
    }

    /**
     * 执行 RPC 调用，期望返回 JSONObject
     */
    protected suspend fun rpcCallObject(
        url: String,
        method: String,
        params: JSONArray,
        jsonRpcVersion: String = "2.0",
        authHeader: String? = null
    ): JSONObject {
        val result = rpcCall(url, method, params, jsonRpcVersion, authHeader)
        return result as? JSONObject
            ?: throw DexError.RpcError(method, message = "期望 JSON 对象，实际: ${result?.javaClass?.simpleName}")
    }
    
    /**
     * 执行 RPC 调用，期望返回 String
     */
    protected suspend fun rpcCallString(
        url: String,
        method: String,
        params: JSONArray,
        jsonRpcVersion: String = "2.0",
        authHeader: String? = null
    ): String {
        val result = rpcCall(url, method, params, jsonRpcVersion, authHeader)
        return when (result) {
            is String -> result
            is JSONObject -> result.optString("result", "")
            null -> ""
            else -> result.toString()
        }
    }

    /**
     * 执行 HTTP 请求
     */
    private fun executeRequest(request: Request, method: String): Any? {
        val response = try {
            httpClient.newCall(request).execute()
        } catch (err: IOException) {
            SecureLogger.w(tag, "RPC $method 网络错误 (${request.url}): ${err.message}")
            throw DexError.NetworkError("RPC $method 网络错误", request.url.host, err)
        }
        
        response.use { resp ->
            val body = resp.body?.string()
            
            if (body.isNullOrBlank()) {
                throw DexError.RpcError(method, resp.code, "响应体为空")
            }
            
            if (!resp.isSuccessful) {
                SecureLogger.w(tag, "RPC $method HTTP ${resp.code}")
                throw DexError.RpcError(method, resp.code, "HTTP 错误: ${resp.code}")
            }
            
            val json = try {
                JSONObject(body)
            } catch (e: Exception) {
                throw DexError.RpcError(method, message = "无效的 JSON 响应")
            }
            
            // 检查 RPC 错误
            val error = json.opt("error")
            if (error != null && error != JSONObject.NULL) {
                val errObj = error as? JSONObject
                val code = errObj?.optInt("code")
                val message = errObj?.optString("message") ?: "未知 RPC 错误"
                SecureLogger.w(tag, "RPC $method 错误: code=$code, message=$message")
                throw DexError.RpcError(method, code, message)
            }
            
            return json.opt("result")
        }
    }
    
    // ═══════════════════════════════════════════════════════════════════
    // 工具方法
    // ═══════════════════════════════════════════════════════════════════
    
    /**
     * 十六进制字符串转 BigInteger
     */
    protected fun hexToBigInteger(hex: String?): BigInteger {
        if (hex.isNullOrBlank()) return BigInteger.ZERO
        val normalized = hex.removePrefix("0x").removePrefix("0X")
        return if (normalized.isBlank()) BigInteger.ZERO else BigInteger(normalized, 16)
    }
    
    /**
     * 清理私钥格式
     */
    protected fun sanitizePrivateKey(raw: String): String {
        return raw.trim().removePrefix("0x").removePrefix("0X")
    }
    
    /**
     * 清理地址格式
     */
    protected fun sanitizeAddress(raw: String): String {
        val trimmed = raw.trim()
        return if (trimmed.startsWith("0x") || trimmed.startsWith("0X")) {
            "0x" + trimmed.substring(2).lowercase()
        } else {
            "0x" + trimmed.lowercase()
        }
    }
    
    /**
     * 验证私钥长度
     */
    protected fun validatePrivateKey(privateKey: String): String {
        val sanitized = sanitizePrivateKey(privateKey)
        if (sanitized.length < Constants.Bsc.PRIVATE_KEY_HEX_LENGTH) {
            throw DexError.InvalidPrivateKey("私钥长度不足 ${Constants.Bsc.PRIVATE_KEY_HEX_LENGTH} 位 Hex 字符")
        }
        return sanitized
    }
    
    companion object {
        private fun defaultHttpClient(): OkHttpClient = OkHttpClient.Builder()
            .retryOnConnectionFailure(true)
            .connectTimeout(Constants.Network.CONNECT_TIMEOUT_SECONDS, TimeUnit.SECONDS)
            .writeTimeout(Constants.Network.WRITE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
            .readTimeout(Constants.Network.READ_TIMEOUT_SECONDS, TimeUnit.SECONDS)
            .build()
    }
}

// DexRpcException 已在 DexModels.kt 中定义，此处仅作兼容说明
// 建议使用 DexError.RpcError 替代

