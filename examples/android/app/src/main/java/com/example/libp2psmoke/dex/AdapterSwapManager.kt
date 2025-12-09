package com.example.libp2psmoke.dex

import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.update
import java.util.UUID

enum class AdapterSwapStatus {
    SETUP,              // Initial state, exchanging keys
    OFFER_SIG_CREATED,  // Alice created Adaptor Signature
    BSC_LOCKED,         // Bob verified sig and locked BSC assets
    BTC_REDEEMED,       // Bob used secret to redeem BTC (revealing valid sig)
    SECRET_EXTRACTED,   // Alice extracted secret from valid sig
    COMPLETED           // Alice claimed BSC assets
}

data class AdapterSwapState(
    val id: String = UUID.randomUUID().toString(),
    val status: AdapterSwapStatus = AdapterSwapStatus.SETUP,
    val role: String = "Initiator (Alice)",
    val secret: String? = null,        // Known by Bob initially
    val paymentPoint: String? = null,  // Public point of secret
    val adaptorSignature: AdapterCrypto.AdaptorSignature? = null,
    val validSignature: String? = null,
    val bscTxHash: String? = null,
    val btcTxHash: String? = null,
    val message: String = "Initializing..."
)

class AdapterSwapManager {
    private val _activeSwap = MutableStateFlow<AdapterSwapState?>(null)
    val activeSwap: StateFlow<AdapterSwapState?> = _activeSwap.asStateFlow()

    fun createSwap(role: String) {
        _activeSwap.value = AdapterSwapState(role = role)
    }

    fun updateStatus(
        status: AdapterSwapStatus, 
        message: String,
        update: (AdapterSwapState) -> AdapterSwapState = { it }
    ) {
        _activeSwap.update { current ->
            current?.let { 
                update(it).copy(status = status, message = message) 
            }
        }
    }
    
    fun clear() {
        _activeSwap.value = null
    }
}
