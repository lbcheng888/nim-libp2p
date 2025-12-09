package com.example.libp2psmoke.dex

import org.bitcoinj.script.Script
import org.bitcoinj.script.ScriptBuilder
import org.bitcoinj.script.ScriptOpCodes
import org.bitcoinj.core.Sha256Hash
import org.bitcoinj.core.Utils
import java.io.ByteArrayOutputStream

/**
 * Bitcoin HTLC Script Helper (P2WSH)
 * 
 * Script Structure:
 * OP_IF
 *   OP_SHA256 <hash> OP_EQUALVERIFY <recipient_pubkey> OP_CHECKSIG
 * OP_ELSE
 *   <cltv_timeout> OP_CHECKLOCKTIMEVERIFY OP_DROP <sender_pubkey> OP_CHECKSIG
 * OP_ENDIF
 */
object BtcHtlcScript {

    /**
     * Create the Redeem Script for the HTLC.
     * 
     * @param recipientPubKey The public key of the recipient (who claims with secret).
     * @param senderPubKey The public key of the sender (who refunds after timeout).
     * @param secretHash The SHA256 hash of the secret.
     * @param timeout The locktime (block height or timestamp).
     */
    fun createRedeemScript(
        recipientPubKey: ByteArray,
        senderPubKey: ByteArray,
        secretHash: ByteArray,
        timeout: Long
    ): Script {
        val builder = ScriptBuilder()
        
        builder.op(ScriptOpCodes.OP_IF)
        
        // Happy Path: Recipient claims with Secret
        // Stack: <sig> <secret>
        builder.op(ScriptOpCodes.OP_SHA256)
        builder.data(secretHash)
        builder.op(ScriptOpCodes.OP_EQUALVERIFY)
        builder.data(recipientPubKey)
        builder.op(ScriptOpCodes.OP_CHECKSIG)
        
        builder.op(ScriptOpCodes.OP_ELSE)
        
        // Refund Path: Sender refunds after Timeout
        // Stack: <sig>
        builder.number(timeout)
        builder.op(ScriptOpCodes.OP_CHECKLOCKTIMEVERIFY)
        builder.op(ScriptOpCodes.OP_DROP)
        builder.data(senderPubKey)
        builder.op(ScriptOpCodes.OP_CHECKSIG)
        
        builder.op(ScriptOpCodes.OP_ENDIF)
        
        return builder.build()
    }
    
    /**
     * Calculate the P2WSH address for a given redeem script.
     */
    fun getP2WSHAddress(params: org.bitcoinj.core.NetworkParameters, redeemScript: Script): org.bitcoinj.core.Address {
        val scriptHash = Sha256Hash.hash(redeemScript.program)
        return org.bitcoinj.core.SegwitAddress.fromHash(params, scriptHash)
    }
    
    /**
     * Create the Witness for claiming the HTLC (Happy Path).
     * 
     * Witness Stack: <recipient_sig> <secret> <1> <redeem_script>
     */
    fun createClaimWitness(
        signature: ByteArray,
        secret: ByteArray,
        redeemScript: ByteArray
    ): org.bitcoinj.core.TransactionWitness {
        val witness = org.bitcoinj.core.TransactionWitness(4)
        witness.setPush(0, signature)
        witness.setPush(1, secret)
        witness.setPush(2, byteArrayOf(1)) // OP_TRUE to trigger OP_IF
        witness.setPush(3, redeemScript)
        return witness
    }

    /**
     * Create the Witness for refunding the HTLC (Timeout Path).
     * 
     * Witness Stack: <sender_sig> <empty_vector> <redeem_script>
     * Note: The empty vector triggers the OP_ELSE branch in standard Bitcoin script execution for OP_IF/OP_NOTIF,
     * but here we are using OP_IF. 
     * Wait, standard IF logic pops the top element.
     * If top is True (non-zero), execute IF.
     * If top is False (zero), execute ELSE.
     * 
     * So for Refund (ELSE branch), we need to push a 0 (empty array) before the redeem script?
     * No, the script execution happens *after* the stack is populated.
     * 
     * The script starts with OP_IF.
     * If we provide <sig> <secret> <redeem_script> (Witness items are pushed to stack):
     * Stack when script runs: <sig> <secret>
     * OP_IF pops <secret>. If secret is not empty/zero, it enters IF.
     * 
     * If we provide <sig> <0> <redeem_script>:
     * Stack when script runs: <sig> <0>
     * OP_IF pops <0> (False). Enters ELSE.
     */
    fun createRefundWitness(
        signature: ByteArray,
        redeemScript: ByteArray
    ): org.bitcoinj.core.TransactionWitness {
        val witness = org.bitcoinj.core.TransactionWitness(3)
        witness.setPush(0, signature)
        witness.setPush(1, ByteArray(0)) // OP_0 to trigger OP_ELSE
        witness.setPush(2, redeemScript)
        return witness
    }
}
