package com.example.libp2psmoke.dex

import com.example.libp2psmoke.core.Constants
import com.example.libp2psmoke.core.DexError
import com.example.libp2psmoke.core.SecureLogger
import kotlinx.coroutines.delay
import okhttp3.OkHttpClient
import org.json.JSONArray
import org.web3j.abi.FunctionEncoder
import org.web3j.abi.datatypes.Address
import org.web3j.abi.datatypes.Function
import org.web3j.abi.datatypes.generated.Uint256
import org.web3j.crypto.Credentials
import org.web3j.crypto.RawTransaction
import org.web3j.crypto.TransactionEncoder
import org.web3j.utils.Numeric
import java.math.BigInteger

/**
 * BSC RPC 客户端 V2
 * 继承自 BaseRpcClient，使用统一的错误处理和日志
 */
class BscRpcClientV2(
    httpClient: OkHttpClient = OkHttpClient()
) : BaseRpcClient(httpClient) {
    
    override val tag = "BscRpcClient"
    
    /**
     * 提交 ERC-20 代币转账
     */
    suspend fun submitTransfer(request: BscTransferRequest): String {
        SecureLogger.i(tag, "发起 BSC 转账: to=${SecureLogger.maskAddress(request.toAddress)}")
        
        // 验证私钥
        val sanitizedPk = validatePrivateKey(request.privateKey)
        
        val credentials = try {
            Credentials.create(sanitizedPk)
        } catch (e: Exception) {
            throw DexError.InvalidPrivateKey("无法解析私钥: ${e.message}")
        }
        
        val fromAddress = credentials.address
        SecureLogger.d(tag, "发送地址: ${SecureLogger.maskAddress(fromAddress)}")
        
        // 获取链参数
        val nonce = getTransactionCount(request.rpcUrl, fromAddress)
        val gasPrice = getGasPrice(request.rpcUrl)
        val chainId = getChainId(request.rpcUrl)
        
        SecureLogger.d(tag, "链参数: nonce=$nonce, gasPrice=$gasPrice, chainId=$chainId")
        
        // 构建 ERC-20 transfer 函数调用
        val function = Function(
            "transfer",
            listOf(
                Address(sanitizeAddress(request.toAddress)),
                Uint256(request.amount)
            ),
            emptyList()
        )
        val encodedFunction = FunctionEncoder.encode(function)
        
        // 创建并签名交易
        val rawTx = RawTransaction.createTransaction(
            nonce,
            gasPrice,
            Constants.Bsc.DEFAULT_GAS_LIMIT,
            sanitizeAddress(request.contractAddress),
            BigInteger.ZERO,
            encodedFunction
        )
        
        val signedTx = TransactionEncoder.signMessage(rawTx, chainId.toLong(), credentials)
        val txHex = Numeric.toHexString(signedTx)
        
        // 发送交易
        val txHash = sendRawTransaction(request.rpcUrl, txHex)
        SecureLogger.i(tag, "交易已发送: ${SecureLogger.maskTxHash(txHash)}")
        
        // 等待确认
        waitForReceipt(request.rpcUrl, txHash)
        SecureLogger.i(tag, "交易已确认: ${SecureLogger.maskTxHash(txHash)}")
        
        return txHash
    }
    
    /**
     * 获取 Chain ID
     */
    private suspend fun getChainId(rpcUrl: String): BigInteger {
        val result = rpcCall(rpcUrl, "eth_chainId", JSONArray())
        val chainId = hexToBigInteger(result as? String)
        
        // 如果返回 0，使用 BSC 测试网默认值
        return if (chainId == BigInteger.ZERO) {
            BigInteger.valueOf(Constants.Bsc.TESTNET_CHAIN_ID)
        } else {
            chainId
        }
    }
    
    /**
     * 获取 Gas Price
     */
    private suspend fun getGasPrice(rpcUrl: String): BigInteger {
        val result = rpcCall(rpcUrl, "eth_gasPrice", JSONArray())
        val gasPrice = hexToBigInteger(result as? String)
        
        // 确保最小 gas price
        return maxOf(gasPrice, Constants.Bsc.MIN_GAS_PRICE)
    }
    
    /**
     * 获取账户 Nonce
     */
    private suspend fun getTransactionCount(rpcUrl: String, address: String): BigInteger {
        val params = JSONArray().apply {
            put(sanitizeAddress(address))
            put("pending")
        }
        val result = rpcCall(rpcUrl, "eth_getTransactionCount", params)
        return hexToBigInteger(result as? String)
    }
    
    /**
     * 发送原始交易
     */
    private suspend fun sendRawTransaction(rpcUrl: String, txHex: String): String {
        val params = JSONArray().apply { put(txHex) }
        val result = rpcCall(rpcUrl, "eth_sendRawTransaction", params)
        
        val txHash = (result as? String)?.takeIf { it.isNotBlank() }
            ?: throw DexError.TransactionFailed(null, "发送交易返回空哈希")
        
        return txHash
    }
    
    /**
     * 等待交易确认
     */
    private suspend fun waitForReceipt(rpcUrl: String, txHash: String) {
        repeat(Constants.Bsc.TX_CONFIRMATION_ATTEMPTS) { attempt ->
            delay(Constants.Bsc.TX_CONFIRMATION_INTERVAL_MS)
            
            val params = JSONArray().apply { put(txHash) }
            val result = rpcCall(rpcUrl, "eth_getTransactionReceipt", params)
            
            if (result is org.json.JSONObject) {
                val receiptTxHash = result.optString("transactionHash", "")
                if (receiptTxHash.equals(txHash, ignoreCase = true)) {
                    // 检查交易状态
                    val status = result.optString("status", "0x1")
                    if (status == "0x0") {
                        throw DexError.TransactionFailed(txHash, "交易执行失败 (reverted)")
                    }
                    return
                }
            }
            
            if (attempt == Constants.Bsc.TX_CONFIRMATION_ATTEMPTS - 1) {
                throw DexError.ConfirmationTimeout(txHash, Constants.Bsc.TX_CONFIRMATION_ATTEMPTS)
            }
        }
    }
    
    /**
     * 获取 ERC-20 余额
     */
    suspend fun getTokenBalance(
        rpcUrl: String,
        tokenContract: String,
        walletAddress: String
    ): BigInteger {
        val function = Function(
            "balanceOf",
            listOf(Address(sanitizeAddress(walletAddress))),
            emptyList()
        )
        val encodedFunction = FunctionEncoder.encode(function)
        
        val params = JSONArray().apply {
            put(org.json.JSONObject().apply {
                put("to", sanitizeAddress(tokenContract))
                put("data", encodedFunction)
            })
            put("latest")
        }
        
        val result = rpcCall(rpcUrl, "eth_call", params)
        return hexToBigInteger(result as? String)
    }
    
    /**
     * 获取原生代币余额 (BNB)
     */
    suspend fun getNativeBalance(rpcUrl: String, address: String): BigInteger {
        val params = JSONArray().apply {
            put(sanitizeAddress(address))
            put("latest")
        }
        val result = rpcCall(rpcUrl, "eth_getBalance", params)
        return hexToBigInteger(result as? String)
    }
}





