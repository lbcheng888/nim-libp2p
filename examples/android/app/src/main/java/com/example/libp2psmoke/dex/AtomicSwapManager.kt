package com.example.libp2psmoke.dex

import com.example.libp2psmoke.core.SecureLogger
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.update
import org.bitcoinj.core.Sha256Hash
import java.security.SecureRandom
import java.util.UUID

/**
 * Atomic Swap Manager
 * Manages the state machine of decentralized atomic swaps.
 */
class AtomicSwapManager {
    companion object {
        private const val TAG = "AtomicSwapManager"
        private const val SECRET_SIZE = 32
    }

    private val _activeSwaps = MutableStateFlow<Map<String, AtomicSwapState>>(emptyMap())
    val activeSwaps: StateFlow<Map<String, AtomicSwapState>> = _activeSwaps.asStateFlow()

    private val secureRandom = SecureRandom()

    /**
     * Initiate a new swap (Step 1: Create Secret & Hash)
     */
    fun createSwap(
        role: SwapRole,
        counterpartyAddress: String,
        amount: String,
        pair: String
    ): AtomicSwapState {
        val secret = ByteArray(SECRET_SIZE)
        secureRandom.nextBytes(secret)
        val secretHash = Sha256Hash.of(secret).bytes
        
        val swapId = UUID.randomUUID().toString()
        val state = AtomicSwapState(
            id = swapId,
            role = role,
            status = SwapStatus.INITIATED,
            secret = if (role == SwapRole.INITIATOR) secret else null,
            secretHash = secretHash,
            counterpartyAddress = counterpartyAddress,
            amount = amount,
            pair = pair,
            startTime = System.currentTimeMillis()
        )
        
        _activeSwaps.update { it + (swapId to state) }
        SecureLogger.i(TAG, "Created new swap: $swapId (Role: $role)")
        return state
    }

    fun updateSwapStatus(id: String, status: SwapStatus, txHash: String? = null) {
        _activeSwaps.update { map ->
            val swap = map[id] ?: return@update map
            map + (id to swap.copy(
                status = status, 
                transactions = if (txHash != null) swap.transactions + txHash else swap.transactions
            ))
        }
    }
    
    fun getSwap(id: String): AtomicSwapState? = _activeSwaps.value[id]

    fun clear() {
        _activeSwaps.value = emptyMap()
    }
}

enum class SwapRole {
    INITIATOR, // Creates secret, locks first
    PARTICIPANT // Verifies lock, locks second
}

enum class SwapStatus {
    INITIATED,
    BSC_LOCKED, // Step 1 Done
    BTC_LOCKED, // Step 2 Done
    BTC_REDEEMED, // Step 3 Done (Secret Revealed)
    BSC_REDEEMED, // Step 4 Done (Swap Complete)
    REFUNDED,
    FAILED
}

data class AtomicSwapState(
    val id: String,
    val role: SwapRole,
    val status: SwapStatus,
    val secret: ByteArray?, // Only known by Initiator initially
    val secretHash: ByteArray,
    val counterpartyAddress: String,
    val amount: String,
    val pair: String,
    val startTime: Long,
    val transactions: List<String> = emptyList()
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false
        other as AtomicSwapState
        if (id != other.id) return false
        return true
    }

    override fun hashCode(): Int {
        return id.hashCode()
    }
}
