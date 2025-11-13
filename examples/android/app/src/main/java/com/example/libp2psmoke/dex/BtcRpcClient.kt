package com.example.libp2psmoke.dex

import java.io.IOException
import java.math.BigDecimal
import java.util.concurrent.atomic.AtomicLong
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import okhttp3.Credentials
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import org.json.JSONArray
import org.json.JSONObject

class BtcRpcClient(
    private val httpClient: OkHttpClient,
) {
    private val mediaType = "application/json; charset=utf-8".toMediaType()
    private val counter = AtomicLong(0)

    suspend fun submitSwap(config: BtcRpcConfig, request: BtcSwapRequest): String {
        if (request.amountSats <= 0) {
            throw DexRpcException("Amount must be positive")
        }
        if (request.targetAddress.isBlank()) {
            throw DexRpcException("Target address is required")
        }
        val amountBtc = BigDecimal(request.amountSats).movePointLeft(8)
        val outputs = JSONObject().apply {
            put(request.targetAddress, amountBtc.stripTrailingZeros().toPlainString())
        }
        val createParams = JSONArray().apply {
            put(JSONArray()) // inputs
            put(outputs)
            put(0) // locktime
            put(JSONObject().apply {
                put("fee_rate", 0.0001)
                put("subtractFeeFromOutputs", JSONArray().apply { put(0) })
                put("lockUnspents", false)
            })
        }
        val created = rpcCallObject(config, "walletcreatefundedpsbt", createParams)
        val psbt = created.optString("psbt")
        if (psbt.isNullOrBlank()) {
            throw DexRpcException("walletcreatefundedpsbt returned empty psbt")
        }

        val processParams = JSONArray().apply {
            put(psbt)
            put(true) // sign
            put("ALL")
            put(true) // include bip32 derivs
            put(true) // finalize
        }
        val processed = rpcCallObject(config, "walletprocesspsbt", processParams)
        var hex = processed.optString("hex")
        val complete = processed.optBoolean("complete", false)
        var finalPsbt = processed.optString("psbt", psbt)
        if (!complete || hex.isNullOrBlank()) {
            val finalizeParams = JSONArray().apply {
                put(finalPsbt)
                put(true)
            }
            val finalized = rpcCallObject(config, "finalizepsbt", finalizeParams)
            if (!finalized.optBoolean("complete", false)) {
                throw DexRpcException("finalizepsbt failed to complete PSBT")
            }
            hex = finalized.optString("hex")
            finalPsbt = finalized.optString("psbt", finalPsbt)
        }
        if (hex.isNullOrBlank()) {
            throw DexRpcException("PSBT hex missing after finalize")
        }
        val sendParams = JSONArray().apply { put(hex) }
        val txId = rpcCallString(config, "sendrawtransaction", sendParams)
        if (txId.isBlank()) {
            throw DexRpcException("sendrawtransaction returned empty txid")
        }
        return txId
    }

    private suspend fun rpcCallObject(
        config: BtcRpcConfig,
        method: String,
        params: JSONArray
    ): JSONObject {
        val result = rpcCall(config, method, params)
        return result as? JSONObject
            ?: throw DexRpcException("$method: expected JSON object result")
    }

    private suspend fun rpcCallString(
        config: BtcRpcConfig,
        method: String,
        params: JSONArray
    ): String {
        val result = rpcCall(config, method, params)
        return when (result) {
            is String -> result
            is JSONObject -> result.optString("result", "")
            else -> result?.toString() ?: ""
        }
    }

    private suspend fun rpcCall(
        config: BtcRpcConfig,
        method: String,
        params: JSONArray
    ): Any? = withContext(Dispatchers.IO) {
        val payload = JSONObject().apply {
            put("jsonrpc", "1.0")
            put("id", counter.incrementAndGet())
            put("method", method)
            put("params", params)
        }
        val requestBuilder = Request.Builder()
            .url(config.url)
            .post(payload.toString().toRequestBody(mediaType))
        val needsAuth = config.username.isNotBlank() || config.password.isNotBlank()
        if (needsAuth) {
            requestBuilder.header(
                "Authorization",
                Credentials.basic(config.username, config.password)
            )
        }
        val response = try {
            httpClient.newCall(requestBuilder.build()).execute()
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
}
