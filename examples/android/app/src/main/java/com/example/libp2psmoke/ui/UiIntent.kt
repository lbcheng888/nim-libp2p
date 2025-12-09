package com.example.libp2psmoke.ui

import com.example.libp2psmoke.dex.BtcAddressType

/**
 * UI 意图 (UiIntent/UiAction)
 * 代表用户在界面上发起的所有操作
 * 前端只负责发送这些指令，不负责执行逻辑
 */
sealed interface UiIntent {
    
    // ═══════════════════════════════════════════════════════════════════
    // 行情相关
    // ═══════════════════════════════════════════════════════════════════
    data class SelectInterval(val interval: String) : UiIntent
    object RefreshMarket : UiIntent
    
    // ═══════════════════════════════════════════════════════════════════
    // 交易相关
    // ═══════════════════════════════════════════════════════════════════
    data class CalculateSwapEstimation(
        val amount: String, 
        val isBuy: Boolean
    ) : UiIntent
    
    enum class SwapType {
        MPC,        // Contract-less (Default)
        ADAPTER,    // Privacy (Adapter Sig)
        ATOMIC      // Legacy (HTLC)
    }

    data class SubmitBridgeTrade(
        val isBuy: Boolean,
        val amount: String,
        val privateKey: String,
        val targetAddress: String,
        val addressType: BtcAddressType = BtcAddressType.P2WPKH,
        val swapType: SwapType = SwapType.MPC
    ) : UiIntent
    
    data class ClaimAtomicSwap(val swapId: String) : UiIntent
    data class InitiateAdapterSwap(val amount: String) : UiIntent
    object InitiateMpcSwap : UiIntent
    
    // 模拟多链交换 (SwapScreen)
    data class SubmitMultiChainSwap(
        val fromChain: String,
        val toChain: String,
        val fromAsset: String,
        val toAsset: String,
        val amount: String,
        val recipient: String
    ) : UiIntent
    
    // ═══════════════════════════════════════════════════════════════════
    // Mixer 相关
    // ═══════════════════════════════════════════════════════════════════
    data class SubmitMixerIntent(
        val asset: String,
        val amount: String,
        val hops: String
    ) : UiIntent
    
    // ═══════════════════════════════════════════════════════════════════
    // 导航与系统
    // ═══════════════════════════════════════════════════════════════════
    object ClearError : UiIntent
    object ClearSuccess : UiIntent
}
