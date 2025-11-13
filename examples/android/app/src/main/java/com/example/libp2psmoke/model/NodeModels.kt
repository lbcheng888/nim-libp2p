package com.example.libp2psmoke.model

import com.example.libp2psmoke.dex.BinanceTicker
import com.example.libp2psmoke.dex.OrderBookEntry
import com.example.libp2psmoke.dex.DexKlineBucket
import com.example.libp2psmoke.dex.DexSwapResult
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

data class NodeUiState(
    val running: Boolean = false,
    val localPeerId: String? = null,
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
    val btcBalance: BigDecimal = BigDecimal.ZERO,
    val bscWalletAddress: String? = null,
    val lastError: String? = null
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
