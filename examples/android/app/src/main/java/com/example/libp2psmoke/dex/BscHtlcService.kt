package com.example.libp2psmoke.dex

import com.example.libp2psmoke.core.SecureLogger
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.web3j.abi.FunctionEncoder
import org.web3j.abi.datatypes.Address
import org.web3j.abi.datatypes.Function
import org.web3j.abi.datatypes.generated.Bytes32
import org.web3j.abi.datatypes.generated.Uint256
import org.web3j.crypto.Credentials
import org.web3j.crypto.RawTransaction
import org.web3j.crypto.TransactionEncoder
import org.web3j.protocol.Web3j
import org.web3j.protocol.core.DefaultBlockParameterName
import org.web3j.protocol.http.HttpService
import org.web3j.utils.Numeric
import java.math.BigInteger
import java.util.Collections

class BscHtlcService {
    companion object {
        private const val TAG = "BscHtlcService"
        // BSC Testnet RPC
        private const val RPC_URL = "https://bsc-testnet.publicnode.com"
        private const val CHAIN_ID = 97L
        // HTLC Contract Address (Demo)
        private const val CONTRACT_ADDRESS = "0x0000000000000000000000000000000000000000" // Replace with real deployment
    }

    private val web3j by lazy { Web3j.build(HttpService(RPC_URL)) }

    /**
     * Lock USDC on BSC (ERC20 HTLC)
     */
    suspend fun lockUsdc(
        privateKey: String,
        recipientAddress: String,
        secretHash: ByteArray,
        timeout: Long,
        amount: BigInteger
    ): String = withContext(Dispatchers.IO) {
        if (CONTRACT_ADDRESS.startsWith("0x00000000")) {
            SecureLogger.w(TAG, "Using Simulation for BSC Lock (No Contract Configured)")
            kotlinx.coroutines.delay(1000)
            return@withContext "0x" + java.util.UUID.randomUUID().toString().replace("-", "")
        }

        SecureLogger.i(TAG, "Locking USDC on BSC (Real): Recipient=$recipientAddress, Amount=$amount")
        
        val credentials = Credentials.create(privateKey)
        val function = Function(
            "lock",
            listOf(
                Address(recipientAddress),
                Bytes32(secretHash),
                Uint256(timeout),
                Uint256(amount)
            ),
            Collections.emptyList()
        )
        
        sendTransaction(credentials, function)
    }

    /**
     * Redeem USDC on BSC using the secret
     */
    suspend fun redeemUsdc(
        privateKey: String,
        secret: ByteArray,
        contractId: String // In this simple ABI, we might just use ID or Hash
    ): String = withContext(Dispatchers.IO) {
        SecureLogger.i(TAG, "Redeeming USDC on BSC (Real) with secret: ${secret.toHex()}")
        
        val credentials = Credentials.create(privateKey)
        val function = Function(
            "redeem",
            listOf(
                Bytes32(Numeric.hexStringToByteArray(contractId)),
                Bytes32(secret)
            ),
            Collections.emptyList()
        )
        
        sendTransaction(credentials, function)
    }

    /**
     * Refund USDC on BSC after timeout
     */
    suspend fun refundUsdc(
        privateKey: String,
        contractId: String
    ): String = withContext(Dispatchers.IO) {
        SecureLogger.i(TAG, "Refunding USDC on BSC (Real) for contract: $contractId")
        
        val credentials = Credentials.create(privateKey)
        val function = Function(
            "refund",
            listOf(Bytes32(Numeric.hexStringToByteArray(contractId))),
            Collections.emptyList()
        )
        
        sendTransaction(credentials, function)
    }

    private fun sendTransaction(credentials: Credentials, function: Function): String {
        val encodedFunction = FunctionEncoder.encode(function)
        val ethGetTransactionCount = web3j.ethGetTransactionCount(
            credentials.address, DefaultBlockParameterName.PENDING
        ).send()
        val nonce = ethGetTransactionCount.transactionCount
        
        // Simple Gas estimation or hardcoded for demo
        val gasPrice = web3j.ethGasPrice().send().gasPrice
        val gasLimit = BigInteger.valueOf(300_000)
        
        val rawTransaction = RawTransaction.createTransaction(
            nonce,
            gasPrice,
            gasLimit,
            CONTRACT_ADDRESS,
            encodedFunction
        )
        
        val signedMessage = TransactionEncoder.signMessage(rawTransaction, CHAIN_ID, credentials)
        val hexValue = Numeric.toHexString(signedMessage)
        
        val ethSendTransaction = web3j.ethSendRawTransaction(hexValue).send()
        
        if (ethSendTransaction.hasError()) {
            throw Exception("BSC Tx Failed: ${ethSendTransaction.error.message}")
        }
        
        return ethSendTransaction.transactionHash
    }

    private fun ByteArray.toHex(): String = joinToString("") { "%02x".format(it) }
}
