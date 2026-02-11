package com.example.libp2psmoke.mixer

import com.example.libp2psmoke.dex.DexChain

data class MixerIntent(
    val sessionHint: Long,
    val asset: String,
    val amount: Double,
    val maxDelayMs: Long,
    val proofDigest: String,
    val hopCount: Int,
    val initiatorPk: String,
    val signature: String
)

data class MixerSessionStatus(
    val sessionId: String,
    val state: String, // "Discovery", "Commit", "Shuffle", "Signature", "Finalize", "Completed"
    val participantCount: Int,
    val lastUpdateMs: Long = System.currentTimeMillis(),
    val txId: String? = null
)

data class CoinJoinRequest(
    val asset: String = "BTC",
    val amount: Double = 1.0,
    val hopCount: Int = 3,
    val maxDelayMs: Long = 60000
)

