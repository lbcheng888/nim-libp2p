package com.example.libp2psmoke.dex

import android.util.Log
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import org.bitcoinj.core.Address
import org.bitcoinj.core.Coin
import org.bitcoinj.core.Context
import org.bitcoinj.core.DumpedPrivateKey
import org.bitcoinj.core.ECKey
import org.bitcoinj.core.LegacyAddress
import org.bitcoinj.core.NetworkParameters
import org.bitcoinj.core.Transaction
import org.bitcoinj.core.UTXO
import org.bitcoinj.params.TestNet3Params
import org.bitcoinj.script.Script
import org.bitcoinj.script.ScriptBuilder
import org.bitcoinj.core.SegwitAddress
import org.bitcoinj.core.TransactionWitness
import org.bitcoinj.crypto.TransactionSignature
import org.json.JSONArray
import org.json.JSONObject
import java.io.IOException

class BtcWalletManager {
    companion object {
        // Default testnet identity derived from BIP39 mnemonic at m/84'/0'/0'/0/0 (P2WPKH)
        const val DEFAULT_MNEMONIC = "famous decide ceiling way news insect student kiwi forward region obvious focus"
        const val DEFAULT_WIF = "cRZbMHd11MYobRro6edBa4mJDFZ2rThep1DmtHCUTpZFVaQwdauo"
        const val DEFAULT_ADDRESS = "tb1qfcyfvl5yxvhdqvev97psae9znmvwqnufml7s4h"
    }

    private val client = OkHttpClient()
    private val params: NetworkParameters = TestNet3Params.get()
    private val baseUrl = "https://blockstream.info/testnet/api"

    init {
        try {
            if (Context.get() == null) {
                Context.propagate(Context(params))
            }
        } catch (e: Exception) {
            // Ignore if already initialized
        }
    }

    suspend fun sendBtc(
        wif: String,
        toAddress: String,
        amountSats: Long,
        addressType: BtcAddressType = BtcAddressType.P2PKH
    ): String {
        return withContext(Dispatchers.IO) {
            try {
                val key: ECKey = DumpedPrivateKey.fromBase58(params, wif).key
                val sourceAddress: Address = when (addressType) {
                    BtcAddressType.P2PKH -> LegacyAddress.fromKey(params, key)
                    BtcAddressType.P2WPKH -> SegwitAddress.fromKey(params, key)
                }
                
                // 1. Fetch UTXOs
                val utxos = fetchUtxos(sourceAddress.toString())
                if (utxos.isEmpty()) throw Exception("Insufficient funds (No UTXOs)")

                // 2. Create Transaction
                val tx = Transaction(params)
                val amount = Coin.valueOf(amountSats)
                val toAddr = Address.fromString(params, toAddress)
                
                tx.addOutput(amount, toAddr)

                var totalInput = Coin.ZERO
                val inputs = mutableListOf<UTXO>()
                
                for (utxo in utxos) {
                    inputs.add(utxo)
                    totalInput = totalInput.add(utxo.value)
                }
                
                val estimatedFee = Coin.valueOf(1000L * inputs.size)
                val totalNeeded = amount.add(estimatedFee)

                if (totalInput.isLessThan(totalNeeded)) {
                     throw Exception("Insufficient funds: Have ${totalInput.value}, Need $totalNeeded")
                }

                // Add inputs
                inputs.forEach { utxo ->
                    // addInput(Sha256Hash, long, Script)
                    val scriptPubKey = ScriptBuilder.createOutputScript(sourceAddress)
                    val input = tx.addInput(utxo.hash, utxo.index, scriptPubKey)
                    if (addressType.isSegwit) {
                        input.clearScriptBytes() // P2WPKH scriptSig 为空，使用 witness
                    }
                }

                // Change
                val change = totalInput.subtract(amount).subtract(estimatedFee)
                if (change.isGreaterThan(Coin.valueOf(546))) {
                    tx.addOutput(change, sourceAddress)
                }

                // Sign
                if (addressType.isSegwit) {
                    val scriptCode: Script = ScriptBuilder.createP2PKHOutputScript(key)
                    inputs.forEachIndexed { index, utxo ->
                        val sig = tx.calculateWitnessSignature(
                            index,
                            key,
                            scriptCode,
                            utxo.value,
                            Transaction.SigHash.ALL,
                            false
                        )
                        val witness = TransactionWitness(2)
                        witness.setPush(0, sig.encodeToBitcoin())
                        witness.setPush(1, key.pubKey)
                        tx.getInput(index.toLong()).witness = witness
                    }
                } else {
                    for (i in 0 until tx.inputs.size) {
                        val scriptPubKey = ScriptBuilder.createOutputScript(sourceAddress)
                        val sig = tx.calculateSignature(i, key, scriptPubKey.program, Transaction.SigHash.ALL, false)
                        tx.getInput(i.toLong()).scriptSig = ScriptBuilder.createInputScript(sig, key)
                    }
                }

                // 3. Broadcast
                val hex = tx.toHexString()
                broadcastTx(hex)

            } catch (e: Exception) {
                Log.e("BtcWalletManager", "Send failed", e)
                throw e
            }
        }
    }

    private fun fetchUtxos(address: String): List<UTXO> {
        val request = Request.Builder()
            .url("$baseUrl/address/$address/utxo")
            .build()

        client.newCall(request).execute().use { response ->
            if (!response.isSuccessful) throw IOException("Failed to fetch UTXOs: ${response.code}")
            val body = response.body?.string() ?: return emptyList()
            
            val jsonArray = JSONArray(body)
            val list = mutableListOf<UTXO>()
            for (i in 0 until jsonArray.length()) {
                val obj = jsonArray.getJSONObject(i)
                val txid = obj.getString("txid")
                val vout = obj.getLong("vout") // Returns Long
                val value = obj.getLong("value") // Returns Long
                val hash = org.bitcoinj.core.Sha256Hash.wrap(txid)
                
                // Recreate script from address
                val script = ScriptBuilder.createOutputScript(Address.fromString(params, address))
                
                // UTXO(hash, index, value, height, coinbase, script)
                // index is long, value is Coin
                list.add(UTXO(hash, vout, Coin.valueOf(value), 0, false, script))
            }
            return list
        }
    }

    fun broadcastTx(hex: String): String {
        val body = hex.toRequestBody("text/plain".toMediaType())
        val request = Request.Builder()
            .url("$baseUrl/tx")
            .post(body)
            .build()

        client.newCall(request).execute().use { response ->
            if (!response.isSuccessful) {
                 val errorBody = response.body?.string()
                 throw IOException("Broadcast failed: ${response.code} $errorBody")
            }
            return response.body?.string() ?: "" 
        }
    }
    
    fun wifToAddress(wif: String): String? {
        return try {
             val key = DumpedPrivateKey.fromBase58(params, wif).key
             LegacyAddress.fromKey(params, key).toString()
        } catch (e: Exception) {
            null
        }
    }

    suspend fun getBalance(address: String): java.math.BigDecimal = withContext(Dispatchers.IO) {
        val utxos = fetchUtxos(address)
        val total = utxos.fold(Coin.ZERO) { acc, utxo -> acc.add(utxo.value) }
        java.math.BigDecimal(total.value).movePointLeft(8)
    }
}
