package com.example.libp2psmoke.dex

import java.io.IOException
import java.math.BigInteger
import java.util.concurrent.atomic.AtomicLong
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.withContext
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import org.json.JSONArray
import org.json.JSONObject
import org.web3j.abi.FunctionEncoder
import org.web3j.abi.datatypes.Address
import org.web3j.abi.datatypes.Function
import org.web3j.abi.datatypes.generated.Uint256
import org.web3j.crypto.Credentials
import org.web3j.crypto.RawTransaction
import org.web3j.crypto.TransactionEncoder
import org.web3j.utils.Numeric

class BscRpcClient(
    private val httpClient: OkHttpClient
) {
    private val mediaType = "application/json".toMediaType()
    private val counter = AtomicLong(1)

    suspend fun submitTransfer(request: BscTransferRequest): String {
        val sanitizedPk = sanitizePrivateKey(request.privateKey)
        if (sanitizedPk.length < 64) {
            throw DexRpcException("Private key must be 64 hex chars")
        }
        val credentials = Credentials.create(sanitizedPk)
        val fromAddress = credentials.address
        val nonce = getTransactionCount(request.rpcUrl, fromAddress)
        val gasPrice = getGasPrice(request.rpcUrl)
        val gasLimit = BigInteger.valueOf(120_000L)
        val chainId = getChainId(request.rpcUrl)
        val function = Function(
            "transfer",
            listOf(Address(sanitizeAddress(request.toAddress)), Uint256(request.amount)),
            emptyList()
        )
        val encoded = FunctionEncoder.encode(function)
        val rawTx = RawTransaction.createTransaction(
            nonce,
            gasPrice,
            gasLimit,
            sanitizeAddress(request.contractAddress),
            BigInteger.ZERO,
            encoded
        )
        val signed = TransactionEncoder.signMessage(rawTx, chainId.toLong(), credentials)
        val hex = Numeric.toHexString(signed)
        val txHash = sendRawTransaction(request.rpcUrl, hex)
        waitForReceipt(request.rpcUrl, txHash)
        return txHash
    }

    private suspend fun getChainId(rpcUrl: String): BigInteger {
        val result = rpcCall(rpcUrl, "eth_chainId", JSONArray())
        return hexToBigInteger(result as? String).takeUnless { it == BigInteger.ZERO }
            ?: BigInteger.valueOf(97) // BSC testnet default
    }

    private suspend fun getGasPrice(rpcUrl: String): BigInteger {
        val result = rpcCall(rpcUrl, "eth_gasPrice", JSONArray())
        val value = hexToBigInteger(result as? String)
        val minimum = BigInteger.valueOf(1_000_000_000L) // 1 gwei
        return if (value < minimum) minimum else value
    }

    private suspend fun getTransactionCount(rpcUrl: String, address: String): BigInteger {
        val params = JSONArray().apply {
            put(sanitizeAddress(address))
            put("pending")
        }
        val result = rpcCall(rpcUrl, "eth_getTransactionCount", params)
        return hexToBigInteger(result as? String)
    }

    private suspend fun sendRawTransaction(rpcUrl: String, hex: String): String {
        val params = JSONArray().apply { put(hex) }
        val result = rpcCall(rpcUrl, "eth_sendRawTransaction", params)
        return (result as? String)?.ifBlank { null }
            ?: throw DexRpcException("eth_sendRawTransaction returned empty hash")
    }

    private suspend fun waitForReceipt(rpcUrl: String, txHash: String) {
        repeat(12) { attempt ->
            delay(1_500)
            val params = JSONArray().apply { put(txHash) }
            val result = rpcCall(rpcUrl, "eth_getTransactionReceipt", params)
            if (result is JSONObject && result.optString("transactionHash").equals(txHash, true)) {
                return
            }
            if (attempt == 11) {
                throw DexRpcException("Receipt for $txHash not found after timeout")
            }
        }
    }

    private suspend fun rpcCall(
        rpcUrl: String,
        method: String,
        params: JSONArray
    ): Any? = withContext(Dispatchers.IO) {
        val payload = JSONObject().apply {
            put("jsonrpc", "2.0")
            put("id", counter.incrementAndGet())
            put("method", method)
            put("params", params)
        }
        val request = Request.Builder()
            .url(rpcUrl)
            .post(payload.toString().toRequestBody(mediaType))
            .build()
        val response = try {
            httpClient.newCall(request).execute()
        } catch (err: IOException) {
            throw DexRpcException("RPC $method network error: ${err.message}", err)
        }
        response.use { resp ->
            val body = resp.body?.string()
                ?: throw DexRpcException("RPC $method returned empty body")
            if (!resp.isSuccessful) {
                throw DexRpcException("RPC $method HTTP ${resp.code} body=$body")
            }
            val json = JSONObject(body)
            val error = json.opt("error")
            if (error != null && error != JSONObject.NULL) {
                val errObj = error as? JSONObject
                val code = errObj?.optInt("code")
                val message = errObj?.optString("message")
                throw DexRpcException("RPC $method error code=$code message=$message")
            }
            json.opt("result")
        }
    }

    private fun sanitizePrivateKey(raw: String): String =
        raw.trim().removePrefix("0x")

    private fun sanitizeAddress(raw: String): String {
        val trimmed = raw.trim()
        return if (trimmed.startsWith("0x") || trimmed.startsWith("0X")) {
            "0x" + trimmed.substring(2).lowercase()
        } else {
            "0x" + trimmed.lowercase()
        }
    }

    private fun hexToBigInteger(hex: String?): BigInteger {
        if (hex.isNullOrBlank()) return BigInteger.ZERO
        val normalized = hex.removePrefix("0x").removePrefix("0X")
        return if (normalized.isBlank()) BigInteger.ZERO else BigInteger(normalized, 16)
    }
}
