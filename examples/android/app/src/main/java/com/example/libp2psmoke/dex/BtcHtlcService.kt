package com.example.libp2psmoke.dex

import com.example.libp2psmoke.core.SecureLogger
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.bitcoinj.core.DumpedPrivateKey
import org.bitcoinj.core.ECKey
import org.bitcoinj.core.NetworkParameters
import org.bitcoinj.params.TestNet3Params

class BtcHtlcService(private val walletManager: BtcWalletManager) {
    companion object {
        private const val TAG = "BtcHtlcService"
    }

    private val params: NetworkParameters = TestNet3Params.get()

    /**
     * Lock BTC in an HTLC (P2WSH)
     */
    suspend fun lockBtc(
        wif: String,
        recipientPubKey: ByteArray,
        secretHash: ByteArray,
        timeout: Long,
        amountSats: Long
    ): String = withContext(Dispatchers.IO) {
        val key = DumpedPrivateKey.fromBase58(params, wif).key
        val senderPubKey = key.pubKey
        
        // 1. Create Redeem Script
        val redeemScript = BtcHtlcScript.createRedeemScript(recipientPubKey, senderPubKey, secretHash, timeout)
        
        // 2. Get P2WSH Address
        val p2wshAddress = BtcHtlcScript.getP2WSHAddress(params, redeemScript)
        SecureLogger.i(TAG, "Generated HTLC Address: $p2wshAddress")
        
        // 3. Send BTC to P2WSH Address
        // Note: We use the standard sendBtc method to fund the script address
        walletManager.sendBtc(wif, p2wshAddress.toString(), amountSats, BtcAddressType.P2WPKH)
    }

    /**
     * Redeem BTC from an HTLC using the secret
     */
    /**
     * Redeem BTC from an HTLC using the secret
     */
    suspend fun redeemBtc(
        wif: String,
        secret: ByteArray,
        senderPubKey: ByteArray,
        timeout: Long,
        txHash: String, // The funding transaction hash
        outputIndex: Long, // The output index of the funding tx
        amountSats: Long
    ): String = withContext(Dispatchers.IO) {
        SecureLogger.i(TAG, "Redeeming BTC (Real) with secret: ${secret.toHex()}")
        
        val key = DumpedPrivateKey.fromBase58(params, wif).key
        val recipientPubKey = key.pubKey
        
        // 1. Reconstruct Redeem Script
        val redeemScript = BtcHtlcScript.createRedeemScript(recipientPubKey, senderPubKey, org.bitcoinj.core.Sha256Hash.hash(secret), timeout)
        
        // 2. Create Transaction
        val tx = org.bitcoinj.core.Transaction(params)
        
        // 3. Add Input (P2WSH)
        // We need the scriptPubKey of the UTXO we are spending
        val p2wshAddress = BtcHtlcScript.getP2WSHAddress(params, redeemScript)
        val scriptPubKey = org.bitcoinj.script.ScriptBuilder.createOutputScript(p2wshAddress)
        
        val input = tx.addInput(
            org.bitcoinj.core.Sha256Hash.wrap(txHash),
            outputIndex,
            scriptPubKey
        )
        // Important: For SegWit, scriptSig in input is empty (handled by addInput usually, but good to be sure)
        input.clearScriptBytes()
        
        // 4. Add Output (Send to self)
        // Estimate fee: 200 sats (simple estimate)
        val fee = 200L
        val amount = amountSats - fee
        if (amount <= 0) throw Exception("Amount too small for fee")
        
        // Send to P2WPKH address derived from key
        val destAddress = org.bitcoinj.core.SegwitAddress.fromKey(params, key)
        tx.addOutput(org.bitcoinj.core.Coin.valueOf(amount), destAddress)
        
        // 5. Sign
        val sig = tx.calculateWitnessSignature(
            0,
            key,
            redeemScript,
            org.bitcoinj.core.Coin.valueOf(amountSats),
            org.bitcoinj.core.Transaction.SigHash.ALL,
            false
        )
        
        // 6. Create Witness
        val witness = BtcHtlcScript.createClaimWitness(
            sig.encodeToBitcoin(),
            secret,
            redeemScript.program
        )
        
        // 7. Set Witness
        input.witness = witness
        
        // 8. Broadcast
        val hex = tx.toHexString()
        SecureLogger.i(TAG, "Broadcasting Redeem Tx: $hex")
        walletManager.broadcastTx(hex)
    }
    
    private fun ByteArray.toHex(): String = joinToString("") { "%02x".format(it) }
}
