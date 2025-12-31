package com.example.libp2psmoke.dex

import com.example.libp2psmoke.core.SecureLogger
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.web3j.crypto.RawTransaction
import org.web3j.crypto.Sign
import org.web3j.crypto.TransactionEncoder
import org.web3j.protocol.Web3j
import org.web3j.protocol.core.DefaultBlockParameterName
import org.web3j.protocol.http.HttpService
import org.web3j.utils.Numeric
import java.math.BigInteger

class BscMpcService {
    companion object {
        private const val TAG = "BscMpcService"
        private const val RPC_URL = "https://bsc-testnet.publicnode.com"
        private const val CHAIN_ID = 97L // BSC Testnet
    }

    private val web3j by lazy { Web3j.build(HttpService(RPC_URL)) }

    /**
     * Create an unsigned transaction and return the hash to be signed by MPC.
     * Returns Pair<TxHash, RawTransaction>
     */
    suspend fun createUnsignedTx(
        fromAddress: String,
        toAddress: String,
        amountWei: BigInteger
    ): Pair<ByteArray, RawTransaction> = withContext(Dispatchers.IO) {
        val ethGetTransactionCount = web3j.ethGetTransactionCount(
            fromAddress, DefaultBlockParameterName.PENDING
        ).send()
        val nonce = ethGetTransactionCount.transactionCount

        val gasPrice = web3j.ethGasPrice().send().gasPrice
        val gasLimit = BigInteger.valueOf(21000) // Standard transfer

        val rawTransaction = RawTransaction.createEtherTransaction(
            nonce,
            gasPrice,
            gasLimit,
            toAddress,
            amountWei
        )

        // Encode transaction for signing (EIP-155)
        val encodedTx = TransactionEncoder.encode(rawTransaction, CHAIN_ID)
        // Calculate Keccak256 hash of the encoded transaction
        val txHash = org.web3j.crypto.Hash.sha3(encodedTx)

        SecureLogger.i(TAG, "Created Unsigned Tx: Nonce=$nonce, Hash=${Numeric.toHexString(txHash)}")
        
        Pair(txHash, rawTransaction)
    }

    /**
     * Reconstruct the signed transaction using the signature from MPC.
     * The signature (r, s, v) must be provided.
     */
    fun createSignedTx(
        rawTransaction: RawTransaction,
        r: ByteArray,
        s: ByteArray,
        v: Byte
    ): String {
        // EIP-155 Replay Protection: v = ChainID * 2 + 35 + recoveryId
        // The 'v' parameter passed here is expected to be the recoveryId (0 or 1).
        // Normalize in case we receive legacy v (27 or 28)
        val rawV = v.toInt() and 0xFF // unsigned
        var recoveryId = if (rawV >= 27) rawV - 27 else rawV
        
        // Canonicalize S (EIP-2 High S rejection)
        // Secp256k1 N value (order of the curve)
        // Secp256k1 N value (order of the curve)
        // Full hex is: FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEBAAEDCE6AF48A03BBFD25E8CD0364141
        // Using string constructor is safer.
        val curveN = BigInteger("115792089237316195423570985008687907852837564279074904382605163141518161494337") 
        val halfN = curveN.shiftRight(1)
        val sBig = BigInteger(1, s)
        
        var canonicalS = sBig
        if (sBig > halfN) {
            SecureLogger.w(TAG, "Detector High S: Canonicalizing...")
            canonicalS = curveN.subtract(sBig)
            // If we flip S, we must flip V (recoveryId)
            recoveryId = 1 - recoveryId
        }
        
        // Ensure RecoveryID is 0 or 1
        if (recoveryId > 1) {
             SecureLogger.w(TAG, "Unusual Recovery ID: $recoveryId (Raw=$rawV)")
             // Force to range? Or let it fail logic below?
             // Usually implies bad signature if neither 0 nor 1 fits.
        }

        val eip155V = BigInteger.valueOf(CHAIN_ID)
            .multiply(BigInteger.valueOf(2))
            .add(BigInteger.valueOf(35))
            .add(BigInteger.valueOf(recoveryId.toLong()))
            
        val vInt = eip155V.toInt()
        val vByte = byteArrayOf(vInt.toByte())
        val rPadded = Numeric.toBytesPadded(BigInteger(1, r), 32)
        val sPadded = Numeric.toBytesPadded(canonicalS, 32)
        val signatureData = Sign.SignatureData(vByte, rPadded, sPadded)
        val signedMessage = TransactionEncoder.encode(rawTransaction, signatureData)
        return Numeric.toHexString(signedMessage)
    }

    /**
     * Broadcast the signed transaction to the network.
     */
    suspend fun broadcastTx(signedTxHex: String): String = withContext(Dispatchers.IO) {
        val ethSendTransaction = web3j.ethSendRawTransaction(signedTxHex).send()
        
        if (ethSendTransaction.hasError()) {
            throw Exception("BSC MPC Tx Failed: ${ethSendTransaction.error.message}")
        }
        
        val txHash = ethSendTransaction.transactionHash
        SecureLogger.i(TAG, "Broadcasted MPC Tx: $txHash")
        txHash
    }
}
