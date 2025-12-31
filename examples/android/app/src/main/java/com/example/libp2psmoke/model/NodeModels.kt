package com.example.libp2psmoke.model

import com.example.libp2psmoke.BuildConfig
import com.example.libp2psmoke.dex.BinanceTicker
import com.example.libp2psmoke.dex.OrderBookEntry
import com.example.libp2psmoke.dex.DexKlineBucket
import com.example.libp2psmoke.dex.DexSwapResult
import com.example.libp2psmoke.dex.AtomicSwapState
import java.math.BigDecimal

data class LanEndpoint(
    val peerId: String,
    val addresses: List<String>,
    val isLocal: Boolean,
    val timestampMs: Long
)

data class PeerState(
    val peerId: String,
    val addresses: List<String> = emptyList(),
    val lastSeenMs: Long = 0L,
    val connected: Boolean = false,
    val lastMessagePreview: String? = null,
    val incoming: Boolean? = null
)

data class DirectMessage(
    val peerId: String,
    val messageId: String,
    val fromSelf: Boolean,
    val body: String,
    val timestampMs: Long,
    val transport: String,
    val acked: Boolean = false
)

data class FeedEntry(
    val id: String,
    val author: String,
    val timestampMs: Long,
    val summary: String,
    val attachments: List<FeedAttachment> = emptyList(),
    val rawJson: String = ""
)

data class LivestreamFrame(
    val streamKey: String,
    val frameIndex: Long,
    val payloadSize: Int,
    val timestampMs: Long
)

// Extended for Multi-Chain Wallets
data class ChainWallet(
    val chainId: String,      // ETH, BSC, SOL, TRX, BTC
    val address: String,
    val balance: BigDecimal,  // Native currency
    val usdcBalance: BigDecimal = BigDecimal.ZERO, // USDC on this chain
    val symbol: String        // ETH, BNB, SOL, TRX, BTC
)

data class NodeUiState(
    val running: Boolean = false,
    val localPeerId: String? = null,
    val peerCount: Int = 0,
    val ipv4Addresses: List<String> = emptyList(),
    val ipv6Addresses: List<String> = emptyList(),
    val lanEndpoints: List<LanEndpoint> = emptyList(),
    val peers: Map<String, PeerState> = emptyMap(),
    val conversations: Map<String, List<DirectMessage>> = emptyMap(),
    val feed: List<FeedEntry> = emptyList(),
    val livestreamFrames: List<LivestreamFrame> = emptyList(),
    val dexSwaps: List<DexSwapResult> = emptyList(),
    val dexKlines: List<DexKlineBucket> = emptyList(),
    val binanceTicker: BinanceTicker? = null,
    val binanceKlines: List<DexKlineBucket> = emptyList(),
    val binanceKlinesJson: String = "[]",
    val binanceInterval: String = "1m",
    val binanceLoading: Boolean = false,
    val binanceBids: List<OrderBookEntry> = emptyList(),
    val binanceAsks: List<OrderBookEntry> = emptyList(),
    val binanceStreamConnected: Boolean = false,
    val binanceLastUpdateMs: Long = 0L,
    val binanceError: String? = null,
    val marketSource: String = "Binance",
    val marketLatencyMs: Long = 0L,
    val marketLatencies: List<MarketSourceLatency> = emptyList(),
    val marketEnabled: Boolean = BuildConfig.MARKET_AUTOSTART,
    val bootstrapPeersRaw: String = BuildConfig.LIBP2P_BOOTSTRAP_PEERS,
    val relayPeersRaw: String = BuildConfig.LIBP2P_RELAY_PEERS,
    
    // Legacy fields (kept for compat, but prefer multiChainWallets)
    val btcBalance: BigDecimal = BigDecimal.ZERO,
    val bscWalletAddress: String? = null,
    
    // New Multi-Chain Support
    val multiChainWallets: Map<String, ChainWallet> = emptyMap(),
    
    val lastError: String? = null,
    val successMessage: String? = null,
    
    // UI 专用状态 (计算结果)
    val swapEstimation: String = "0",
    val bridgeLoading: Boolean = false,
    
    // Atomic Swap State
    val activeSwap: com.example.libp2psmoke.dex.AtomicSwapState? = null
)

data class MarketSourceLatency(
    val source: String,
    val lastLatencyMs: Long,
    val averageLatencyMs: Double,
    val successCount: Int,
    val failureCount: Int,
    val lastUpdatedMs: Long
)

enum class AttachmentKind {
    IMAGE,
    TEXT,
    AUDIO,
    VIDEO,
    LINK,
    DATA,
    UNKNOWN
}

data class FeedAttachment(
    val id: String,
    val kind: AttachmentKind,
    val label: String,
    val uri: String? = null,
    val mimeType: String? = null,
    val bytes: ByteArray? = null,
    val text: String? = null
)
