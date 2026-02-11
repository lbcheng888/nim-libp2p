package com.example.libp2psmoke.domain

import com.example.libp2psmoke.core.Constants
import com.example.libp2psmoke.core.DexError
import com.example.libp2psmoke.core.SecureKeyStore
import com.example.libp2psmoke.core.SecureLogger
import com.example.libp2psmoke.core.secureErase
import com.example.libp2psmoke.dex.BscTransferRequest
import com.example.libp2psmoke.dex.BtcRpcConfig
import com.example.libp2psmoke.dex.BtcSwapRequest
import com.example.libp2psmoke.dex.DexRepositoryV2
import com.example.libp2psmoke.dex.DexSwapResult
import com.example.libp2psmoke.dex.BtcAddressType
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.withContext
import org.web3j.crypto.Credentials
import java.math.BigDecimal
import java.math.BigInteger
import java.math.RoundingMode

/**
 * 钱包业务用例
 * 处理跨链交易、余额查询等钱包相关操作
 */
class WalletUseCase(
    private val dexRepository: DexRepositoryV2,
    private val secureKeyStore: SecureKeyStore? = null
) {
    companion object {
        private const val TAG = "WalletUseCase"
    }
    
    /** BTC 余额 */
    val btcBalance: StateFlow<BigDecimal> = dexRepository.btcBalance()
    
    /** 交易历史 */
    val swapHistory: StateFlow<List<DexSwapResult>> = dexRepository.swaps()
    
    /**
     * 发送 BTC 交易 (使用 WIF 私钥)
     */
    suspend fun sendBtc(
        wifPrivateKey: String,
        toAddress: String,
        amountSats: Long,
        orderId: String,
        addressType: BtcAddressType = BtcAddressType.P2PKH
    ): Result<DexSwapResult> = withContext(Dispatchers.IO) {
        SecureLogger.i(TAG, "发起 BTC 转账: ${SecureLogger.maskAddress(toAddress)}, 金额: $amountSats sats")
        
        // 验证参数
        if (amountSats <= 0) {
            return@withContext Result.failure(
                DexError.InvalidAmount(amountSats.toString(), "金额必须大于 0")
            )
        }
        
        if (toAddress.isBlank()) {
            return@withContext Result.failure(
                DexError.InvalidAddress(toAddress, "BTC", "地址不能为空")
            )
        }
        
        try {
            val result = dexRepository.submitBtcTransfer(
                wif = wifPrivateKey,
                toAddress = toAddress,
                amountSats = amountSats,
                orderId = orderId,
                addressType = addressType
            )
            SecureLogger.i(TAG, "BTC 转账成功: ${SecureLogger.maskTxHash(result.txHash)}")
            Result.success(result)
        } catch (e: Exception) {
            SecureLogger.e(TAG, "BTC 转账失败", e)
            Result.failure(DexError.from(e))
        }
    }
    
    /**
     * 发送 BTC 交易 (使用 RPC 配置)
     */
    suspend fun sendBtcViaRpc(
        config: BtcRpcConfig,
        request: BtcSwapRequest
    ): Result<DexSwapResult> = withContext(Dispatchers.IO) {
        SecureLogger.i(TAG, "发起 BTC RPC 交易: ${SecureLogger.maskAddress(request.targetAddress)}")
        
        if (request.amountSats <= 0) {
            return@withContext Result.failure(
                DexError.InvalidAmount(request.amountSats.toString(), "金额必须大于 0")
            )
        }
        
        try {
            val result = dexRepository.submitBtcSwap(config, request)
            Result.success(result)
        } catch (e: Exception) {
            SecureLogger.e(TAG, "BTC RPC 交易失败", e)
            Result.failure(DexError.from(e))
        }
    }
    
    /**
     * 发送 BSC USDC 转账
     */
    suspend fun sendBscUsdc(
        privateKey: CharArray,
        toAddress: String,
        amount: BigInteger,
        orderId: String,
        rpcUrl: String,
        contractAddress: String,
        decimals: Int = Constants.Bsc.USDC_DECIMALS
    ): Result<DexSwapResult> = withContext(Dispatchers.IO) {
        SecureLogger.i(TAG, "发起 BSC USDC 转账: ${SecureLogger.maskAddress(toAddress)}")
        
        // 验证私钥
        // Note: In a real app, we should validate the key format without converting to String if possible.
        // For now, we assume the CharArray is valid hex or WIF.
        if (privateKey.isEmpty() || privateKey.size < Constants.Bsc.PRIVATE_KEY_HEX_LENGTH) {
            return@withContext Result.failure(
                DexError.InvalidPrivateKey("私钥长度不足")
            )
        }
        
        // 验证地址
        if (!isValidEvmAddress(toAddress)) {
            return@withContext Result.failure(
                DexError.InvalidAddress(toAddress, "BSC", "无效的 EVM 地址格式")
            )
        }
        
        val request = BscTransferRequest(
            orderId = orderId,
            rpcUrl = rpcUrl,
            contractAddress = contractAddress,
            privateKey = String(privateKey), // TODO: Update BscTransferRequest to accept CharArray
            toAddress = toAddress,
            amount = amount,
            decimals = decimals
        )
        
        try {
            val result = dexRepository.submitBscTransfer(request)
            SecureLogger.i(TAG, "BSC USDC 转账成功: ${SecureLogger.maskTxHash(result.txHash)}")
            Result.success(result)
        } catch (e: Exception) {
            SecureLogger.e(TAG, "BSC USDC 转账失败", e)
            Result.failure(DexError.from(e))
        }
    }
    
    /**
     * USDC -> BTC 跨链桥接
     */
    suspend fun bridgeUsdcToBtc(
        usdcAmount: BigInteger,
        bscPrivateKey: CharArray,
        btcTargetAddress: String,
        bscRpcUrl: String,
        bscContract: String,
        bridgeVaultAddress: String,
        currentBtcPrice: BigDecimal
    ): Result<BridgeResult> = withContext(Dispatchers.IO) {
        SecureLogger.i(TAG, "发起 USDC->BTC 桥接")
        
        // 1. 计算预计获得的 BTC
        val estimatedBtcSats = dexRepository.estimateBtcFromUsdc(
            usdcAmount,
            Constants.Bsc.USDC_DECIMALS,
            currentBtcPrice
        )
        
        if (estimatedBtcSats <= Constants.Btc.DUST_LIMIT_SATS) {
            return@withContext Result.failure(
                DexError.InvalidAmount(
                    usdcAmount.toString(),
                    "金额过小，预计 BTC 低于粉尘限制"
                )
            )
        }
        
        val orderId = "bridge-${System.currentTimeMillis()}"
        
        // 2. 发送 USDC 到桥接合约
        val usdcResult = sendBscUsdc(
            privateKey = bscPrivateKey,
            toAddress = bridgeVaultAddress,
            amount = usdcAmount,
            orderId = orderId,
            rpcUrl = bscRpcUrl,
            contractAddress = bscContract
        )
        
        if (usdcResult.isFailure) {
            return@withContext Result.failure(usdcResult.exceptionOrNull()!!)
        }
        
        SecureLogger.i(TAG, "USDC 已发送到桥接合约，等待 BTC 释放")
        
        // 3. 返回桥接结果 (BTC 部分需要后端处理)
        Result.success(BridgeResult(
            orderId = orderId,
            usdcTxHash = usdcResult.getOrNull()?.txHash,
            estimatedBtcSats = estimatedBtcSats,
            btcTargetAddress = btcTargetAddress,
            status = BridgeStatus.USDC_SENT
        ))
    }
    
    /**
     * 从私钥推导 BSC 地址
     */
    fun deriveBscAddress(privateKey: String): String? {
        return try {
            val sanitized = privateKey.trim().removePrefix("0x")
            val credentials = Credentials.create(sanitized)
            credentials.address
        } catch (e: Exception) {
            SecureLogger.w(TAG, "推导 BSC 地址失败", e)
            null
        }
    }
    
    /**
     * 解析代币金额
     */
    fun parseTokenAmount(amountText: String, decimals: Int): BigInteger? {
        return try {
            val amount = BigDecimal(amountText.trim())
            if (amount <= BigDecimal.ZERO) return null
            amount.movePointRight(decimals).setScale(0, RoundingMode.DOWN).toBigInteger()
        } catch (e: Exception) {
            null
        }
    }

    /**
     * 查询 BTC 测试网余额（P2WPKH）
     */
    suspend fun fetchBtcTestnetBalance(address: String): BigDecimal {
        return try {
            dexRepository.getBtcBalance(address)
        } catch (e: Exception) {
            SecureLogger.w(TAG, "查询 BTC 余额失败", e)
            BigDecimal.ZERO
        }
    }

    /**
     * 查询 BSC USDC 余额
     */
    suspend fun fetchBscUsdcBalance(
        address: String,
        rpcUrl: String,
        contract: String,
        decimals: Int = Constants.Bsc.USDC_DECIMALS
    ): BigDecimal = withContext(Dispatchers.IO) {
        try {
            val raw = dexRepository.getBscBalance(rpcUrl, contract, address)
            BigDecimal(raw).movePointLeft(decimals)
        } catch (e: Exception) {
            SecureLogger.w(TAG, "查询 BSC USDC 余额失败", e)
            BigDecimal.ZERO
        }
    }
    
    /**
     * 格式化 BTC 金额
     */
    fun formatBtc(sats: Long): String {
        return BigDecimal(sats)
            .movePointLeft(8)
            .stripTrailingZeros()
            .toPlainString()
    }
    
    /**
     * 格式化 USDC 金额
     */
    fun formatUsdc(amount: BigInteger, decimals: Int = Constants.Bsc.USDC_DECIMALS): String {
        return BigDecimal(amount)
            .movePointLeft(decimals)
            .stripTrailingZeros()
            .toPlainString()
    }
    
    /**
     * 估算 USDC 可换 BTC 数量
     */
    fun estimateBtcFromUsdc(usdcAmount: BigInteger, price: BigDecimal): Long {
        return dexRepository.estimateBtcFromUsdc(
            usdcAmount,
            Constants.Bsc.USDC_DECIMALS,
            price
        )
    }
    
    /**
     * 安全存储私钥
     */
    fun storePrivateKey(alias: String, privateKey: String): Boolean {
        return secureKeyStore?.storePrivateKey(alias, privateKey.toCharArray()) ?: false
    }
    
    /**
     * 读取存储的私钥
     */
    fun retrievePrivateKey(alias: String): String? {
        val chars = secureKeyStore?.retrievePrivateKey(alias) ?: return null
        val result = String(chars)
        chars.secureErase()
        return result
    }
    
    /**
     * 检查是否有存储的私钥
     */
    fun hasStoredPrivateKey(alias: String): Boolean {
        return secureKeyStore?.hasPrivateKey(alias) ?: false
    }
    
    // ═══════════════════════════════════════════════════════════════════
    // 私有方法
    // ═══════════════════════════════════════════════════════════════════
    
    private fun isValidEvmAddress(address: String): Boolean {
        val trimmed = address.trim()
        if (!trimmed.startsWith("0x") && !trimmed.startsWith("0X")) {
            return false
        }
        val hex = trimmed.substring(2)
        return hex.length == 40 && hex.all { it.isDigit() || it in 'a'..'f' || it in 'A'..'F' }
    }
    
    // ═══════════════════════════════════════════════════════════════════
    // 数据类
    // ═══════════════════════════════════════════════════════════════════
    
    data class BridgeResult(
        val orderId: String,
        val usdcTxHash: String?,
        val btcTxHash: String? = null,
        val estimatedBtcSats: Long,
        val btcTargetAddress: String,
        val status: BridgeStatus
    )
    
    enum class BridgeStatus {
        PENDING,
        USDC_SENT,
        BTC_RELEASING,
        COMPLETED,
        FAILED
    }
}
