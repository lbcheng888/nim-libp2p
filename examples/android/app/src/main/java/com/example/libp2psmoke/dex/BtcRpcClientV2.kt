package com.example.libp2psmoke.dex

import com.example.libp2psmoke.core.Constants
import com.example.libp2psmoke.core.DexError
import com.example.libp2psmoke.core.SecureLogger
import okhttp3.Credentials
import okhttp3.OkHttpClient
import org.json.JSONArray
import org.json.JSONObject
import java.math.BigDecimal

/**
 * BTC RPC 客户端 V2
 * 继承自 BaseRpcClient，使用统一的错误处理和日志
 */
class BtcRpcClientV2(
    httpClient: OkHttpClient = OkHttpClient()
) : BaseRpcClient(httpClient) {
    
    override val tag = "BtcRpcClient"
    
    /**
     * 提交 BTC 交换交易
     * 使用 PSBT (Partially Signed Bitcoin Transaction) 流程
     */
    suspend fun submitSwap(config: BtcRpcConfig, request: BtcSwapRequest): String {
        SecureLogger.i(tag, "发起 BTC 交换: to=${SecureLogger.maskAddress(request.targetAddress)}, sats=${request.amountSats}")
        
        // 验证参数
        if (request.amountSats <= 0) {
            throw DexError.InvalidAmount(request.amountSats.toString(), "金额必须大于 0")
        }
        
        if (request.targetAddress.isBlank()) {
            throw DexError.InvalidAddress(request.targetAddress, "BTC", "地址不能为空")
        }
        
        val authHeader = if (config.username.isNotBlank() || config.password.isNotBlank()) {
            Credentials.basic(config.username, config.password)
        } else null
        
        // 1. 创建资金化的 PSBT
        val psbt = createFundedPsbt(config.url, request, authHeader)
        SecureLogger.d(tag, "PSBT 已创建")
        
        // 2. 处理/签名 PSBT
        val (processedPsbt, hex, complete) = processPsbt(config.url, psbt, authHeader)
        
        // 3. 如果需要，最终化 PSBT
        val finalHex = if (complete && hex.isNotBlank()) {
            hex
        } else {
            finalizePsbt(config.url, processedPsbt, authHeader)
        }
        
        // 4. 广播交易
        val txId = broadcastTransaction(config.url, finalHex, authHeader)
        SecureLogger.i(tag, "交易已广播: ${SecureLogger.maskTxHash(txId)}")
        
        return txId
    }
    
    /**
     * 创建资金化的 PSBT
     */
    private suspend fun createFundedPsbt(
        rpcUrl: String,
        request: BtcSwapRequest,
        authHeader: String?
    ): String {
        val amountBtc = BigDecimal(request.amountSats)
            .movePointLeft(Constants.Btc.DECIMAL_PLACES)
            .stripTrailingZeros()
            .toPlainString()
        
        val outputs = JSONObject().apply {
            put(request.targetAddress, amountBtc)
        }
        
        val options = JSONObject().apply {
            put("fee_rate", 0.0001)
            put("subtractFeeFromOutputs", JSONArray().apply { put(0) })
            put("lockUnspents", false)
        }
        
        val params = JSONArray().apply {
            put(JSONArray())  // inputs (自动选择)
            put(outputs)
            put(0)            // locktime
            put(options)
        }
        
        val result = rpcCallObject(rpcUrl, "walletcreatefundedpsbt", params, "1.0", authHeader)
        
        return result.optString("psbt").takeIf { it.isNotBlank() }
            ?: throw DexError.TransactionFailed(null, "创建 PSBT 失败: 返回空")
    }
    
    /**
     * 处理/签名 PSBT
     */
    private suspend fun processPsbt(
        rpcUrl: String,
        psbt: String,
        authHeader: String?
    ): Triple<String, String, Boolean> {
        val params = JSONArray().apply {
            put(psbt)
            put(true)       // sign
            put("ALL")      // sighash type
            put(true)       // bip32 derivs
            put(true)       // finalize
        }
        
        val result = rpcCallObject(rpcUrl, "walletprocesspsbt", params, "1.0", authHeader)
        
        val processedPsbt = result.optString("psbt", psbt)
        val hex = result.optString("hex", "")
        val complete = result.optBoolean("complete", false)
        
        return Triple(processedPsbt, hex, complete)
    }
    
    /**
     * 最终化 PSBT
     */
    private suspend fun finalizePsbt(
        rpcUrl: String,
        psbt: String,
        authHeader: String?
    ): String {
        val params = JSONArray().apply {
            put(psbt)
            put(true)  // extract
        }
        
        val result = rpcCallObject(rpcUrl, "finalizepsbt", params, "1.0", authHeader)
        
        if (!result.optBoolean("complete", false)) {
            throw DexError.TransactionFailed(null, "PSBT 最终化失败: 签名不完整")
        }
        
        return result.optString("hex").takeIf { it.isNotBlank() }
            ?: throw DexError.TransactionFailed(null, "PSBT 最终化失败: 无交易 hex")
    }
    
    /**
     * 广播交易
     */
    private suspend fun broadcastTransaction(
        rpcUrl: String,
        txHex: String,
        authHeader: String?
    ): String {
        val params = JSONArray().apply { put(txHex) }
        
        val txId = rpcCallString(rpcUrl, "sendrawtransaction", params, "1.0", authHeader)
        
        if (txId.isBlank()) {
            throw DexError.TransactionFailed(null, "广播交易失败: 返回空 txid")
        }
        
        return txId
    }
    
    /**
     * 获取钱包余额
     */
    suspend fun getWalletBalance(config: BtcRpcConfig): BigDecimal {
        val authHeader = if (config.username.isNotBlank() || config.password.isNotBlank()) {
            Credentials.basic(config.username, config.password)
        } else null
        
        val result = rpcCall(config.url, "getbalance", JSONArray(), "1.0", authHeader)
        
        return when (result) {
            is Number -> result.toString().toBigDecimalOrNull() ?: BigDecimal.ZERO
            is String -> result.toBigDecimalOrNull() ?: BigDecimal.ZERO
            else -> BigDecimal.ZERO
        }
    }
    
    /**
     * 获取新地址
     */
    suspend fun getNewAddress(config: BtcRpcConfig, label: String = ""): String {
        val authHeader = if (config.username.isNotBlank() || config.password.isNotBlank()) {
            Credentials.basic(config.username, config.password)
        } else null
        
        val params = JSONArray().apply {
            if (label.isNotBlank()) put(label)
        }
        
        return rpcCallString(config.url, "getnewaddress", params, "1.0", authHeader)
    }
    
    /**
     * 获取区块链信息
     */
    suspend fun getBlockchainInfo(config: BtcRpcConfig): JSONObject {
        val authHeader = if (config.username.isNotBlank() || config.password.isNotBlank()) {
            Credentials.basic(config.username, config.password)
        } else null
        
        return rpcCallObject(config.url, "getblockchaininfo", JSONArray(), "1.0", authHeader)
    }
    
    /**
     * 验证地址
     */
    suspend fun validateAddress(config: BtcRpcConfig, address: String): Boolean {
        val authHeader = if (config.username.isNotBlank() || config.password.isNotBlank()) {
            Credentials.basic(config.username, config.password)
        } else null
        
        val params = JSONArray().apply { put(address) }
        
        return try {
            val result = rpcCallObject(config.url, "validateaddress", params, "1.0", authHeader)
            result.optBoolean("isvalid", false)
        } catch (e: Exception) {
            false
        }
    }
}




