package com.example.libp2psmoke

import android.annotation.SuppressLint
import android.content.Context
import android.graphics.BitmapFactory
import android.os.Bundle
import android.util.Log
import android.webkit.ConsoleMessage
import android.webkit.WebChromeClient
import android.webkit.WebResourceError
import android.webkit.WebResourceRequest
import android.webkit.WebView
import android.webkit.WebViewClient
import androidx.activity.ComponentActivity
import androidx.activity.compose.setContent
import androidx.activity.enableEdgeToEdge
import androidx.compose.foundation.Canvas
import androidx.compose.foundation.Image
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.background
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.heightIn
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.layout.width
import androidx.compose.foundation.layout.widthIn
import androidx.compose.foundation.layout.fillMaxHeight
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.LazyRow
import androidx.compose.foundation.lazy.items
import androidx.compose.material3.Button
import androidx.compose.material3.CircularProgressIndicator
import androidx.compose.material3.ButtonDefaults
import androidx.compose.material3.Card
import androidx.compose.material3.CardDefaults
import androidx.compose.material3.Divider
import androidx.compose.material3.FilterChip
import androidx.compose.material3.ExperimentalMaterial3Api
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.OutlinedTextField
import androidx.compose.material3.Slider
import androidx.compose.material3.SliderDefaults
import androidx.compose.material3.Scaffold
import androidx.compose.material3.SnackbarHost
import androidx.compose.material3.SnackbarHostState
import androidx.compose.material3.Tab
import androidx.compose.material3.TabRow
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.material3.TopAppBar
import androidx.compose.material3.Icon
import androidx.compose.material3.IconButton
import androidx.compose.runtime.Composable
import androidx.compose.runtime.collectAsState
import androidx.compose.runtime.getValue
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.saveable.rememberSaveable
import androidx.compose.runtime.setValue
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.filled.ContentCopy
import androidx.compose.runtime.rememberCoroutineScope
import androidx.lifecycle.viewmodel.compose.viewModel
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.geometry.Offset
import androidx.compose.ui.geometry.Size
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.graphics.ImageBitmap
import androidx.compose.ui.graphics.PathEffect
import androidx.compose.ui.graphics.asImageBitmap
import androidx.compose.ui.graphics.drawscope.DrawScope
import androidx.compose.ui.text.style.TextAlign
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.foundation.text.KeyboardOptions
import androidx.compose.foundation.shape.CircleShape
import androidx.compose.ui.text.input.KeyboardType
import androidx.compose.ui.text.input.PasswordVisualTransformation
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import androidx.compose.ui.draw.alpha
import androidx.compose.ui.draw.clip
import androidx.compose.ui.layout.ContentScale
import androidx.compose.ui.platform.LocalClipboardManager
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.text.AnnotatedString
import androidx.compose.ui.viewinterop.AndroidView
import com.example.libp2psmoke.ui.theme.Libp2pSmokeTheme
import com.example.libp2psmoke.viewmodel.NimNodeViewModel
import com.example.libp2psmoke.model.AttachmentKind
import com.example.libp2psmoke.model.FeedAttachment
import com.example.libp2psmoke.model.MarketSourceLatency
import com.example.libp2psmoke.dex.BinanceTicker
import com.example.libp2psmoke.dex.OrderBookEntry
import com.example.libp2psmoke.dex.DexKlineBucket
import com.example.libp2psmoke.dex.DexSwapResult
import com.example.libp2psmoke.dex.DexSwapStatus
import java.math.BigDecimal
import java.math.RoundingMode
import java.text.SimpleDateFormat
import java.util.Date
import java.util.Locale
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
import kotlin.collections.ArrayDeque
import kotlin.math.abs
import kotlin.math.max
import kotlin.math.min
import kotlin.math.roundToInt

class MainActivity : ComponentActivity() {
    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        enableEdgeToEdge()
        setContent {
            Libp2pSmokeTheme {
                val viewModel: NimNodeViewModel = viewModel()
                val uiState by viewModel.state.collectAsState()
                Libp2pApp(uiState = uiState, viewModel = viewModel)
            }
        }
    }
}

@OptIn(ExperimentalMaterial3Api::class)
@Composable
private fun Libp2pApp(uiState: com.example.libp2psmoke.model.NodeUiState, viewModel: NimNodeViewModel) {
    var selectedTab by rememberSaveable { mutableStateOf(0) }
    var selectedPeerId by rememberSaveable { mutableStateOf<String?>(null) }
    var chatInput by remember { mutableStateOf("") }
    var feedInput by remember { mutableStateOf("") }
    var attachDemoMedia by remember { mutableStateOf(false) }
    var streamKey by rememberSaveable { mutableStateOf("demo-stream") }
    var streamPayload by remember { mutableStateOf("Live frame sample") }

    val snackbarHostState = remember { SnackbarHostState() }
    val coroutineScope = rememberCoroutineScope()

    val peers = uiState.peers.values.sortedByDescending { it.lastSeenMs }
    val peerIds = peers.map { it.peerId }

    LaunchedEffect(peerIds, selectedPeerId) {
        val currentSelection = selectedPeerId
        if (peerIds.isEmpty()) {
            if (currentSelection != null) {
                selectedPeerId = null
            }
        } else if (currentSelection == null || peerIds.none { it == currentSelection }) {
            selectedPeerId = peerIds.first()
        }
    }

    Scaffold(
        modifier = Modifier.fillMaxSize(),
        snackbarHost = { SnackbarHost(snackbarHostState) },
        topBar = {
            TopAppBar(
                title = {
                    Column {
                        Text(
                            text = uiState.localPeerId?.let { "Peer: $it" } ?: "Starting libp2p…",
                            style = MaterialTheme.typography.bodySmall.copy(fontSize = 12.sp, lineHeight = 14.sp),
                            maxLines = 3,
                            overflow = TextOverflow.Ellipsis
                        )
                        val status = when {
                            uiState.lastError != null -> "Error: ${uiState.lastError}"
                            uiState.running -> "Node running"
                            else -> "Node idle"
                        }
                        Text(
                            text = status,
                            style = MaterialTheme.typography.bodySmall,
                            color = if (uiState.lastError != null) MaterialTheme.colorScheme.error else MaterialTheme.colorScheme.onSurfaceVariant
                        )
                    }
                }
            )
        }
    ) { padding ->
        Column(
            modifier = Modifier
                .padding(padding)
                .fillMaxSize()
        ) {
            val tabs = listOf("Overview", "Chat", "Feed", "Live", "DEX")
            TabRow(selectedTabIndex = selectedTab) {
                tabs.forEachIndexed { index, title ->
                    Tab(
                        selected = selectedTab == index,
                        onClick = { selectedTab = index },
                        text = { Text(title) }
                    )
                }
            }
            Divider()
            when (selectedTab) {
                0 -> OverviewTab(
                    uiState = uiState,
                    peers = peers,
                    onConnect = { viewModel.connectPeer(it) },
                    onSelectChat = { peerId ->
                        viewModel.connectPeerForChat(peerId) { success, message ->
                            if (success) {
                                selectedPeerId = peerId
                                selectedTab = 1
                            } else {
                                val notice = message ?: "连接对等节点失败"
                                coroutineScope.launch {
                                    snackbarHostState.showSnackbar(notice)
                                }
                            }
                        }
                    }
                )

                1 -> ChatTab(
                    peerId = selectedPeerId,
                    peers = peers,
                    conversations = uiState.conversations,
                    chatInput = chatInput,
                    onSelectPeer = { selectedPeerId = it },
                    onChatInputChanged = { chatInput = it },
                    onSend = {
                        val peer = selectedPeerId
                        if (!peer.isNullOrBlank() && chatInput.isNotBlank()) {
                            viewModel.sendDirectMessage(peer, chatInput)
                            chatInput = ""
                        }
                    }
                )

                2 -> FeedTab(
                    feedInput = feedInput,
                    feedItems = uiState.feed,
                    attachDemo = attachDemoMedia,
                    onToggleAttachment = { attachDemoMedia = it },
                    onInputChanged = { feedInput = it },
                    onPublish = {
                        if (feedInput.isNotBlank()) {
                            viewModel.publishFeed(feedInput, attachDemoMedia)
                            feedInput = ""
                        }
                    }
                )

                3 -> LiveTab(
                    streamKey = streamKey.ifBlank { uiState.localPeerId?.let { "$it-stream" } ?: "demo-stream" },
                    payload = streamPayload,
                    frames = uiState.livestreamFrames,
                    onStreamKeyChanged = { streamKey = it },
                    onPayloadChanged = { streamPayload = it },
                    onPublish = {
                        val key = streamKey.ifBlank { uiState.localPeerId?.let { "$it-stream" } ?: "demo-stream" }
                        if (streamPayload.isNotBlank()) {
                            viewModel.publishLivestreamText(key, streamPayload)
                            streamPayload = ""
                        }
                    }
                )

                4 -> DexTab(
                    uiState = uiState,
                    viewModel = viewModel,
                    snackbarHostState = snackbarHostState,
                    coroutineScope = coroutineScope
                )
            }
        }
    }
}

@Composable
private fun OverviewTab(
    uiState: com.example.libp2psmoke.model.NodeUiState,
    peers: List<com.example.libp2psmoke.model.PeerState>,
    onConnect: (String) -> Unit,
    onSelectChat: (String) -> Unit
) {
    LazyColumn(
        modifier = Modifier
            .fillMaxSize()
            .padding(16.dp),
        verticalArrangement = Arrangement.spacedBy(12.dp)
    ) {
        item {
            Card {
                Column(modifier = Modifier.padding(16.dp), verticalArrangement = Arrangement.spacedBy(6.dp)) {
                    Text("Local Addresses", style = MaterialTheme.typography.titleMedium)
                    val ipv4Text = uiState.ipv4Addresses.joinToString("\n").ifEmpty { "N/A" }
                    val ipv6Text = uiState.ipv6Addresses.joinToString("\n").ifEmpty { "N/A" }
                    Text(text = "IPv4:\n$ipv4Text")
                    Text(text = "IPv6:\n$ipv6Text")
                }
            }
        }

        item {
            Text(
                text = "Discovered Nodes",
                style = MaterialTheme.typography.titleMedium,
                modifier = Modifier.padding(horizontal = 4.dp)
            )
        }

        items(peers, key = { it.peerId }) { peer ->
            Card {
                Column(modifier = Modifier.padding(16.dp)) {
                    Text(peer.peerId, fontWeight = FontWeight.Bold)
                        val addressText = peer.addresses.joinToString("\n").ifEmpty { "No addresses yet" }
                        Text(addressText, style = MaterialTheme.typography.bodySmall)
                    Spacer(Modifier.height(8.dp))
                    Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                        Button(onClick = { onConnect(peer.peerId) }) {
                            Text(if (peer.connected) "Reconnect" else "Connect")
                        }
                        TextButton(onClick = { onSelectChat(peer.peerId) }) {
                            Text("Chat")
                        }
                    }
                    peer.lastMessagePreview?.let {
                        Spacer(Modifier.height(4.dp))
                        Text("Status: $it", style = MaterialTheme.typography.bodySmall)
                    }
                }
            }
        }
    }
}

@Composable
private fun ChatTab(
    peerId: String?,
    peers: List<com.example.libp2psmoke.model.PeerState>,
    conversations: Map<String, List<com.example.libp2psmoke.model.DirectMessage>>,
    chatInput: String,
    onSelectPeer: (String) -> Unit,
    onChatInputChanged: (String) -> Unit,
    onSend: () -> Unit
) {
    Column(
        modifier = Modifier
            .fillMaxSize()
            .padding(16.dp)
    ) {
        Text("Select peer", style = MaterialTheme.typography.titleMedium)
        LazyRow(
            modifier = Modifier.fillMaxWidth(),
            horizontalArrangement = Arrangement.spacedBy(8.dp)
        ) {
            items(peers, key = { it.peerId }) { peer ->
                FilterChip(
                    selected = peerId == peer.peerId,
                    onClick = { onSelectPeer(peer.peerId) },
                    label = {
                        Text(text = peer.peerId, maxLines = 1, overflow = TextOverflow.Ellipsis)
                    }
                )
            }
        }
        Spacer(Modifier.height(12.dp))
        val messages = peerId?.let { conversations[it] } ?: emptyList()
        LazyColumn(
            modifier = Modifier
                .weight(1f)
                .fillMaxWidth(),
            reverseLayout = true
        ) {
            items(messages.reversed(), key = { it.timestampMs to it.messageId }) { msg ->
                ChatBubble(message = msg)
                Spacer(Modifier.height(8.dp))
            }
        }
        Spacer(Modifier.height(8.dp))
        OutlinedTextField(
            value = chatInput,
            onValueChange = onChatInputChanged,
            modifier = Modifier.fillMaxWidth(),
            label = { Text("Message") },
            maxLines = 4
        )
        Spacer(Modifier.height(8.dp))
        Row(horizontalArrangement = Arrangement.End, modifier = Modifier.fillMaxWidth()) {
            Button(onClick = onSend, enabled = !peerId.isNullOrBlank() && chatInput.isNotBlank()) {
                Text("Send")
            }
        }
    }
}

@Composable
private fun ChatBubble(message: com.example.libp2psmoke.model.DirectMessage) {
    val align = if (message.fromSelf) Alignment.End else Alignment.Start
    val background = if (message.fromSelf) MaterialTheme.colorScheme.primary else MaterialTheme.colorScheme.surfaceVariant
    val contentColor = if (message.fromSelf) MaterialTheme.colorScheme.onPrimary else MaterialTheme.colorScheme.onSurface
    Column(
        modifier = Modifier.fillMaxWidth(),
        horizontalAlignment = align
    ) {
        Column(
            modifier = Modifier
                .background(background, shape = MaterialTheme.shapes.medium)
                .padding(12.dp)
                .fillMaxWidth(0.8f)
        ) {
            Text(message.body, color = contentColor)
            Spacer(Modifier.height(4.dp))
            val time = formatTimestamp(message.timestampMs)
            val status = if (message.acked) "Delivered" else "Pending"
            Text(
                "$time · ${message.transport} · $status",
                style = MaterialTheme.typography.labelSmall,
                color = contentColor.copy(alpha = 0.7f)
            )
        }
    }
}

@Composable
private fun FeedTab(
    feedInput: String,
    feedItems: List<com.example.libp2psmoke.model.FeedEntry>,
    attachDemo: Boolean,
    onToggleAttachment: (Boolean) -> Unit,
    onInputChanged: (String) -> Unit,
    onPublish: () -> Unit
) {
    Column(
        modifier = Modifier
            .fillMaxSize()
            .padding(16.dp)
    ) {
        OutlinedTextField(
            value = feedInput,
            onValueChange = onInputChanged,
            modifier = Modifier.fillMaxWidth(),
            label = { Text("Publish to feed") },
            maxLines = 4
        )
        Spacer(Modifier.height(8.dp))
        Row(
            modifier = Modifier.fillMaxWidth(),
            horizontalArrangement = Arrangement.SpaceBetween,
            verticalAlignment = Alignment.CenterVertically
        ) {
            FilterChip(
                selected = attachDemo,
                onClick = { onToggleAttachment(!attachDemo) },
                label = { Text("Attach demo media") }
            )
            Button(onClick = onPublish, enabled = feedInput.isNotBlank()) {
                Text("Publish")
            }
        }
        Spacer(Modifier.height(16.dp))
        LazyColumn(
            verticalArrangement = Arrangement.spacedBy(12.dp),
            modifier = Modifier.fillMaxSize()
        ) {
            items(feedItems, key = { it.id }) { item ->
                Card {
                    Column(modifier = Modifier.padding(16.dp)) {
                        Text(item.author, fontWeight = FontWeight.Bold)
                        Text(formatTimestamp(item.timestampMs), style = MaterialTheme.typography.bodySmall)
                        Spacer(Modifier.height(4.dp))
                        Text(item.summary.ifBlank { "No summary" })
                        if (item.attachments.isNotEmpty()) {
                            Spacer(Modifier.height(8.dp))
                            item.attachments.forEach { attachment ->
                                AttachmentPreview(attachment)
                                Spacer(Modifier.height(8.dp))
                            }
                        }
                    }
                }
            }
        }
    }
}

@Composable
private fun DexTab(
    uiState: com.example.libp2psmoke.model.NodeUiState,
    viewModel: NimNodeViewModel,
    snackbarHostState: SnackbarHostState,
    coroutineScope: CoroutineScope
) {
    var btcOrderId by rememberSaveable { mutableStateOf("btc-${System.currentTimeMillis()}") }
    var btcAmountSats by rememberSaveable { mutableStateOf("100000") }
    var btcTargetAddress by rememberSaveable { mutableStateOf("") }
    var btcRpcUrl by rememberSaveable { mutableStateOf(viewModel.defaultBtcRpcUrl()) }
    var btcRpcUser by rememberSaveable { mutableStateOf("") }
    var btcRpcPassword by rememberSaveable { mutableStateOf("") }

    var bscOrderId by rememberSaveable { mutableStateOf("swap-${System.currentTimeMillis()}") }
    var bscAmount by rememberSaveable { mutableStateOf("100") }
    var bscPrivateKey by rememberSaveable { mutableStateOf("") }
    var bscRpcUrl by rememberSaveable { mutableStateOf(viewModel.defaultBscRpcUrl()) }
    var bscContract by rememberSaveable { mutableStateOf(viewModel.defaultBscContract()) }

    val notify: (String) -> Unit = { message ->
        coroutineScope.launch {
            snackbarHostState.showSnackbar(message)
        }
    }
    val clipboard = LocalClipboardManager.current
    val derivedAddress = remember(bscPrivateKey) { viewModel.deriveBscAddress(bscPrivateKey) }
    val displayBscAddress = derivedAddress ?: uiState.bscWalletAddress
    val estimatedBtc = viewModel.previewBtcFromUsdc(bscAmount)

    LazyColumn(
        modifier = Modifier
            .fillMaxSize()
            .padding(16.dp),
        verticalArrangement = Arrangement.spacedBy(16.dp)
    ) {
        item {
            BinancePriceCard(
                ticker = uiState.binanceTicker,
                klines = uiState.binanceKlines,
                loading = uiState.binanceLoading,
                intervals = viewModel.binanceIntervals(),
                selectedInterval = uiState.binanceInterval,
                streamConnected = uiState.binanceStreamConnected,
                lastUpdateMs = uiState.binanceLastUpdateMs,
                errorMessage = uiState.binanceError,
                marketSource = uiState.marketSource,
                latencyMs = uiState.marketLatencyMs,
                onSelectInterval = { viewModel.selectBinanceInterval(it) },
                onBuyClick = { summary ->
                    notify(summary.ifBlank { "请在下方输入买入参数以兑换 BTC" })
                },
                onSellClick = { summary ->
                    notify(summary.ifBlank { "请在下方输入卖出参数" })
                }
            )
        }
        item {
            MarketSourceSummaryCard(
                activeSource = uiState.marketSource,
                latencies = uiState.marketLatencies,
                latencyMs = uiState.marketLatencyMs
            )
        }
        item {
            BinanceOrderBook(
                bids = uiState.binanceBids,
                asks = uiState.binanceAsks
            )
        }
        item {
            DexWalletHeader(
                btcBalance = uiState.btcBalance,
                bscAddress = displayBscAddress
            )
        }
        item {
            Card {
                Column(modifier = Modifier.padding(16.dp)) {
                    Text("BTC 测试网结算", style = MaterialTheme.typography.titleMedium, fontWeight = FontWeight.SemiBold)
                    Spacer(Modifier.height(12.dp))
                    OutlinedTextField(
                        value = btcOrderId,
                        onValueChange = { btcOrderId = it },
                        modifier = Modifier.fillMaxWidth(),
                        label = { Text("订单 ID") }
                    )
                    Spacer(Modifier.height(8.dp))
                    OutlinedTextField(
                        value = btcAmountSats,
                        onValueChange = { btcAmountSats = it },
                        modifier = Modifier.fillMaxWidth(),
                        label = { Text("BTC 数量 (sats)") },
                        keyboardOptions = KeyboardOptions(keyboardType = KeyboardType.Number)
                    )
                    Spacer(Modifier.height(8.dp))
                    OutlinedTextField(
                        value = btcTargetAddress,
                        onValueChange = { btcTargetAddress = it },
                        modifier = Modifier.fillMaxWidth(),
                        label = { Text("BTC 目标地址") },
                        placeholder = { Text("tb1q...") }
                    )
                    Spacer(Modifier.height(8.dp))
                    OutlinedTextField(
                        value = btcRpcUrl,
                        onValueChange = { btcRpcUrl = it },
                        modifier = Modifier.fillMaxWidth(),
                        label = { Text("RPC URL") }
                    )
                    Spacer(Modifier.height(8.dp))
                    OutlinedTextField(
                        value = btcRpcUser,
                        onValueChange = { btcRpcUser = it },
                        modifier = Modifier.fillMaxWidth(),
                        label = { Text("RPC 用户名 (可选)") }
                    )
                    Spacer(Modifier.height(8.dp))
                    OutlinedTextField(
                        value = btcRpcPassword,
                        onValueChange = { btcRpcPassword = it },
                        modifier = Modifier.fillMaxWidth(),
                        label = { Text("RPC 密码 (可选)") },
                        visualTransformation = PasswordVisualTransformation(),
                        singleLine = true
                    )
                    Spacer(Modifier.height(12.dp))
                    Row(modifier = Modifier.fillMaxWidth(), horizontalArrangement = Arrangement.End) {
                        Button(
                            onClick = {
                                val effectiveId = btcOrderId.ifBlank { "btc-${System.currentTimeMillis()}" }
                                val sats = btcAmountSats.trim().toLongOrNull()
                                when {
                                    sats == null || sats <= 0 -> notify("请输入有效的 BTC 数量（单位：satoshi）")
                                    btcTargetAddress.isBlank() -> notify("请输入 BTC 目标地址")
                                    else -> {
                                        viewModel.submitBtcSettlement(
                                            orderId = effectiveId,
                                            amountSats = sats,
                                            targetAddress = btcTargetAddress,
                                            rpcUrl = btcRpcUrl,
                                            rpcUser = btcRpcUser,
                                            rpcPassword = btcRpcPassword
                                        )
                                        if (btcOrderId.isBlank()) btcOrderId = effectiveId
                                    }
                                }
                            }
                        ) {
                            Text("提交 BTC 结算")
                        }
                    }
                }
            }
        }
        item {
            Card {
                Column(modifier = Modifier.padding(16.dp)) {
                    Text("BSC 测试网 USDC 转账", style = MaterialTheme.typography.titleMedium, fontWeight = FontWeight.SemiBold)
                    Spacer(Modifier.height(12.dp))
                    OutlinedTextField(
                        value = bscOrderId,
                        onValueChange = { bscOrderId = it },
                        modifier = Modifier.fillMaxWidth(),
                        label = { Text("订单 ID") }
                    )
                    Spacer(Modifier.height(8.dp))
                    OutlinedTextField(
                        value = bscAmount,
                        onValueChange = { bscAmount = it },
                        modifier = Modifier.fillMaxWidth(),
                        label = { Text("USDC 数量") },
                        keyboardOptions = KeyboardOptions(keyboardType = KeyboardType.Decimal)
                    )
                    if (estimatedBtc.isNotBlank()) {
                        Text(
                            text = "≈ $estimatedBtc BTC 按 Binance 实时价",
                            style = MaterialTheme.typography.bodySmall,
                            color = MaterialTheme.colorScheme.onSurfaceVariant
                        )
                    }
                    Spacer(Modifier.height(8.dp))
                    OutlinedTextField(
                        value = bscPrivateKey,
                        onValueChange = { bscPrivateKey = it },
                        modifier = Modifier.fillMaxWidth(),
                        label = { Text("私钥 (hex)") },
                        visualTransformation = PasswordVisualTransformation(),
                        singleLine = true
                    )
                    Spacer(Modifier.height(8.dp))
                    OutlinedTextField(
                        value = displayBscAddress ?: "",
                        onValueChange = {},
                        enabled = false,
                        modifier = Modifier.fillMaxWidth(),
                        label = { Text("BSC 钱包地址 (自动)") },
                        placeholder = { Text("等待私钥") },
                        trailingIcon = {
                            if (!displayBscAddress.isNullOrBlank()) {
                                IconButton(onClick = {
                                    clipboard.setText(AnnotatedString(displayBscAddress))
                                    notify("已复制地址")
                                }) {
                                    Icon(
                                        imageVector = Icons.Filled.ContentCopy,
                                        contentDescription = "复制地址"
                                    )
                                }
                            }
                        }
                    )
                    Spacer(Modifier.height(8.dp))
                    OutlinedTextField(
                        value = bscRpcUrl,
                        onValueChange = { bscRpcUrl = it },
                        modifier = Modifier.fillMaxWidth(),
                        label = { Text("RPC URL") }
                    )
                    Spacer(Modifier.height(8.dp))
                    OutlinedTextField(
                        value = bscContract,
                        onValueChange = { bscContract = it },
                        modifier = Modifier.fillMaxWidth(),
                        label = { Text("USDC 合约地址") }
                    )
                    Spacer(Modifier.height(12.dp))
                    Row(modifier = Modifier.fillMaxWidth(), horizontalArrangement = Arrangement.End) {
                        Button(
                            onClick = {
                                val effectiveId = bscOrderId.ifBlank { "swap-${System.currentTimeMillis()}" }
                                when {
                                    bscAmount.isBlank() -> notify("请输入 USDC 数量")
                                    bscPrivateKey.isBlank() -> notify("请输入 BSC 私钥")
                                    else -> {
                                        viewModel.submitBscUsdcTransfer(
                                            orderId = effectiveId,
                                            amountText = bscAmount,
                                            privateKey = bscPrivateKey,
                                            rpcUrl = bscRpcUrl,
                                            contractAddress = bscContract
                                        )
                                        if (bscOrderId.isBlank()) bscOrderId = effectiveId
                                    }
                                }
                            }
                        ) {
                            Text("提交 USDC 转账")
                        }
                    }
                    Spacer(Modifier.height(8.dp))
                    Button(
                        onClick = {
                            val effectiveBscId = bscOrderId.ifBlank { "swap-${System.currentTimeMillis()}" }
                            val effectiveBtcId = btcOrderId.ifBlank { "btc-${System.currentTimeMillis()}" }
                            when {
                                bscAmount.isBlank() -> notify("请输入 USDC 数量")
                                bscPrivateKey.isBlank() -> notify("请输入 BSC 私钥")
                                derivedAddress.isNullOrBlank() -> notify("无法解析 BSC 钱包地址")
                                btcTargetAddress.isBlank() -> notify("请先填写 BTC 目标地址")
                                else -> {
                                    viewModel.bridgeUsdcToBtc(
                                        bscOrderId = effectiveBscId,
                                        usdcAmountText = bscAmount,
                                        bscPrivateKey = bscPrivateKey,
                                        bscRpcUrl = bscRpcUrl,
                                        bscContract = bscContract,
                                        btcOrderId = effectiveBtcId,
                                        btcTargetAddress = btcTargetAddress,
                                        btcRpcUrl = btcRpcUrl,
                                        btcRpcUser = btcRpcUser,
                                        btcRpcPassword = btcRpcPassword
                                    )
                                    if (bscOrderId.isBlank()) bscOrderId = effectiveBscId
                                    if (btcOrderId.isBlank()) btcOrderId = effectiveBtcId
                                }
                            }
                        },
                        modifier = Modifier.fillMaxWidth(),
                        colors = ButtonDefaults.buttonColors(
                            containerColor = MaterialTheme.colorScheme.primary,
                            contentColor = MaterialTheme.colorScheme.onPrimary
                        )
                    ) {
                        Text("USDC → BTC 跨链兑换")
                    }
                }
            }
        }
        item {
            DexSwapHistory(swaps = uiState.dexSwaps)
        }
        item {
            DexKlineSection(
                buckets = uiState.dexKlines,
                latencies = uiState.marketLatencies,
                activeSource = uiState.marketSource,
                latencyMs = uiState.marketLatencyMs
            )
        }
    }
}

@Composable
private fun DexWalletHeader(
    btcBalance: BigDecimal,
    bscAddress: String?
) {
    val clipboard = LocalClipboardManager.current
    Card {
        Column(modifier = Modifier.padding(16.dp)) {
            Text("资产概览", style = MaterialTheme.typography.titleMedium, fontWeight = FontWeight.SemiBold)
            Spacer(Modifier.height(8.dp))
            Text(
                text = "BTC 余额",
                style = MaterialTheme.typography.bodySmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant
            )
            Text(
                "${formatDecimal(btcBalance)} BTC",
                style = MaterialTheme.typography.headlineSmall,
                fontWeight = FontWeight.Bold
            )
            Spacer(Modifier.height(12.dp))
            Row(verticalAlignment = Alignment.CenterVertically) {
                Column(modifier = Modifier.weight(1f)) {
                    Text("BSC 钱包", style = MaterialTheme.typography.bodySmall, color = MaterialTheme.colorScheme.onSurfaceVariant)
                    Text(
                        bscAddress ?: "等待私钥",
                        style = MaterialTheme.typography.bodyMedium,
                        maxLines = 1,
                        overflow = TextOverflow.Ellipsis
                    )
                }
                if (!bscAddress.isNullOrBlank()) {
                    IconButton(onClick = {
                        clipboard.setText(AnnotatedString(bscAddress))
                    }) {
                        Icon(Icons.Filled.ContentCopy, contentDescription = "复制 BSC 地址")
                    }
                }
            }
        }
    }
}

@Composable
private fun DexSwapHistory(swaps: List<DexSwapResult>) {
    Card {
        Column(modifier = Modifier.padding(16.dp)) {
            Text("结算历史", style = MaterialTheme.typography.titleMedium, fontWeight = FontWeight.SemiBold)
            if (swaps.isEmpty()) {
                Spacer(Modifier.height(8.dp))
                Text("还没有跨链结算记录", color = MaterialTheme.colorScheme.onSurfaceVariant)
            } else {
                swaps.take(10).forEach { swap ->
                    Spacer(Modifier.height(12.dp))
                    Text("${swap.chain.name} · ${swap.orderId}", fontWeight = FontWeight.SemiBold)
                    Text(
                        "状态: ${swap.status}",
                        color = dexStatusColor(swap.status),
                        style = MaterialTheme.typography.bodyMedium
                    )
                    Text("金额: ${swap.amountDisplay}", style = MaterialTheme.typography.bodySmall)
                    swap.txHash?.let { hash ->
                        val preview = if (hash.length > 16) "${hash.take(14)}…" else hash
                        Text("Tx: $preview", style = MaterialTheme.typography.bodySmall)
                    }
                    swap.message?.takeIf { it.isNotBlank() }?.let {
                        Text(it, style = MaterialTheme.typography.bodySmall)
                    }
                    Text(formatTimestamp(swap.timestampMs), style = MaterialTheme.typography.labelSmall)
                }
            }
        }
    }
}

@Composable
private fun DexKlineSection(
    buckets: List<DexKlineBucket>,
    latencies: List<MarketSourceLatency>,
    activeSource: String,
    latencyMs: Long
) {
    Card {
        Column(modifier = Modifier.padding(16.dp)) {
            Text("K 线（BTC/USDC）", style = MaterialTheme.typography.titleMedium, fontWeight = FontWeight.SemiBold)
            if (buckets.isEmpty()) {
                Spacer(Modifier.height(8.dp))
                Text("等待撮合成交…", color = MaterialTheme.colorScheme.onSurfaceVariant)
            } else {
                Spacer(Modifier.height(12.dp))
                TradingViewChart(
                    candles = buckets,
                    streamConnected = true,
                    lastUpdateMs = buckets.first().windowStartMs,
                    marketSource = "撮合引擎",
                    latencyMs = 0L,
                    modifier = Modifier
                        .fillMaxWidth()
                        .height(220.dp)
                )
                Spacer(Modifier.height(16.dp))
                MarketLatencyPanel(
                    latencies = latencies,
                    activeSource = activeSource,
                    latencyMs = latencyMs
                )
                Spacer(Modifier.height(16.dp))
                DexOrderBook(buckets)
                Spacer(Modifier.height(16.dp))
                DexTradeControls(buckets.first().close)
            }
        }
    }
}

@Composable
private fun MarketSourceSummaryCard(
    activeSource: String,
    latencies: List<MarketSourceLatency>,
    latencyMs: Long
) {
    Card {
        Column(modifier = Modifier.padding(16.dp), verticalArrangement = Arrangement.spacedBy(12.dp)) {
            Row(
                modifier = Modifier.fillMaxWidth(),
                horizontalArrangement = Arrangement.SpaceBetween,
                verticalAlignment = Alignment.CenterVertically
            ) {
                Column {
                    Text("多源行情状态", style = MaterialTheme.typography.titleMedium, fontWeight = FontWeight.SemiBold)
                    Text(
                        "优选源：$activeSource",
                        style = MaterialTheme.typography.bodySmall,
                        color = MaterialTheme.colorScheme.onSurfaceVariant
                    )
                }
                LatencyBadge(activeSource = activeSource, latencyMs = latencyMs)
            }
            if (latencies.isEmpty()) {
                Text("正在测速多个行情源…", color = MaterialTheme.colorScheme.onSurfaceVariant)
            } else {
                LazyRow(
                    horizontalArrangement = Arrangement.spacedBy(12.dp),
                    modifier = Modifier.fillMaxWidth()
                ) {
                    items(latencies, key = { it.source }) { sample ->
                        SourceLatencyChip(sample = sample, active = sample.source == activeSource)
                    }
                }
            }
        }
    }
}

@Composable
private fun SourceLatencyChip(sample: MarketSourceLatency, active: Boolean) {
    val background = if (active) {
        MaterialTheme.colorScheme.primary.copy(alpha = 0.12f)
    } else {
        MaterialTheme.colorScheme.surfaceVariant.copy(alpha = 0.6f)
    }
    Card(
        colors = CardDefaults.cardColors(containerColor = background),
        modifier = Modifier.widthIn(min = 160.dp)
    ) {
        Column(
            modifier = Modifier.padding(12.dp),
            verticalArrangement = Arrangement.spacedBy(4.dp)
        ) {
            Text(
                sample.source,
                style = MaterialTheme.typography.bodyMedium,
                fontWeight = if (active) FontWeight.Bold else FontWeight.Medium
            )
            Text(
                "最近 ${formatLatencyDisplay(sample.lastLatencyMs)}",
                style = MaterialTheme.typography.bodySmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant
            )
            Text(
                "均值 ${formatLatencyAverage(sample.averageLatencyMs)} · 成功率 ${sample.successRateText()}",
                style = MaterialTheme.typography.labelSmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant
            )
            Text(
                "更新时间 ${formatTimestamp(sample.lastUpdatedMs)}",
                style = MaterialTheme.typography.labelSmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant
            )
        }
    }
}

@Composable
private fun MarketLatencyPanel(
    latencies: List<MarketSourceLatency>,
    activeSource: String,
    latencyMs: Long
) {
    Column {
        Row(
            modifier = Modifier.fillMaxWidth(),
            horizontalArrangement = Arrangement.SpaceBetween,
            verticalAlignment = Alignment.CenterVertically
        ) {
            Text("行情源监控", style = MaterialTheme.typography.titleSmall, fontWeight = FontWeight.Medium)
            LatencyBadge(activeSource = activeSource, latencyMs = latencyMs)
        }
        Spacer(Modifier.height(8.dp))
        if (latencies.isEmpty()) {
            Text("等待测速…", color = MaterialTheme.colorScheme.onSurfaceVariant)
        } else {
            latencies.forEachIndexed { index, sample ->
                LatencyStatRow(sample = sample, active = sample.source == activeSource)
                if (index < latencies.lastIndex) {
                    Spacer(Modifier.height(8.dp))
                }
            }
        }
    }
}

@Composable
private fun LatencyBadge(activeSource: String, latencyMs: Long) {
    val background = MaterialTheme.colorScheme.primary.copy(alpha = 0.12f)
    Row(
        modifier = Modifier
            .clip(CircleShape)
            .background(background)
            .padding(horizontal = 12.dp, vertical = 6.dp),
        verticalAlignment = Alignment.CenterVertically,
        horizontalArrangement = Arrangement.spacedBy(6.dp)
    ) {
        Box(
            modifier = Modifier
                .size(8.dp)
                .clip(CircleShape)
                .background(MaterialTheme.colorScheme.primary)
        )
        Text(
            text = "$activeSource · ${formatLatencyDisplay(latencyMs)}",
            style = MaterialTheme.typography.labelMedium,
            color = MaterialTheme.colorScheme.primary
        )
    }
}

@Composable
private fun LatencyStatRow(sample: MarketSourceLatency, active: Boolean) {
    val background = if (active) {
        MaterialTheme.colorScheme.primary.copy(alpha = 0.08f)
    } else {
        MaterialTheme.colorScheme.surfaceVariant.copy(alpha = 0.5f)
    }
    Row(
        modifier = Modifier
            .fillMaxWidth()
            .clip(MaterialTheme.shapes.medium)
            .background(background)
            .padding(12.dp),
        horizontalArrangement = Arrangement.SpaceBetween,
        verticalAlignment = Alignment.CenterVertically
    ) {
        Column {
            Text(
                sample.source,
                style = MaterialTheme.typography.bodyMedium,
                fontWeight = if (active) FontWeight.SemiBold else FontWeight.Normal
            )
            val status = if (active) "当前主源" else "候补"
            Text(
                status,
                style = MaterialTheme.typography.labelSmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant
            )
        }
        Column(horizontalAlignment = Alignment.End) {
            Text(
                "最近 ${formatLatencyDisplay(sample.lastLatencyMs)}",
                style = MaterialTheme.typography.bodyMedium,
                fontWeight = FontWeight.Medium
            )
            Text(
                "均值 ${formatLatencyAverage(sample.averageLatencyMs)} · 成功率 ${sample.successRateText()}",
                style = MaterialTheme.typography.labelSmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant
            )
        }
    }
}

@Composable
private fun BinancePriceCard(
    ticker: BinanceTicker?,
    klines: List<DexKlineBucket>,
    loading: Boolean,
    intervals: List<String>,
    selectedInterval: String,
    streamConnected: Boolean,
    lastUpdateMs: Long,
    errorMessage: String?,
    marketSource: String,
    latencyMs: Long,
    onSelectInterval: (String) -> Unit,
    onBuyClick: (String) -> Unit,
    onSellClick: (String) -> Unit
) {
    Card {
        Column(modifier = Modifier.padding(16.dp)) {
            Text("BTC/USDT", style = MaterialTheme.typography.bodySmall, color = MaterialTheme.colorScheme.onSurfaceVariant)
            Row(
                modifier = Modifier.fillMaxWidth(),
                horizontalArrangement = Arrangement.SpaceBetween,
                verticalAlignment = Alignment.CenterVertically
            ) {
                Text(
                    text = "数据源 · $marketSource",
                    style = MaterialTheme.typography.labelSmall,
                    color = MaterialTheme.colorScheme.onSurfaceVariant
                )
                LatencyBadge(activeSource = marketSource, latencyMs = latencyMs)
            }
            val priceText = ticker?.lastPrice?.let { formatDecimal(it) } ?: "--"
            val changePercent = ticker?.priceChangePercent ?: BigDecimal.ZERO
            val changeColor = if (changePercent >= BigDecimal.ZERO) Color(0xFF2EBD85) else Color(0xFFE53935)
            Text(
                priceText,
                style = MaterialTheme.typography.displaySmall,
                color = MaterialTheme.colorScheme.onSurface,
                fontWeight = FontWeight.Bold
            )
            Text(
                if (ticker != null) "${formatDecimal(changePercent)}%" else "--",
                color = changeColor,
                style = MaterialTheme.typography.bodyMedium
            )
            Spacer(Modifier.height(4.dp))
            StreamingStatusBadge(
                connected = streamConnected,
                lastUpdateMs = lastUpdateMs
            )
            errorMessage?.let {
                Spacer(Modifier.height(8.dp))
                Text(
                    text = "行情源异常：$it",
                    color = MaterialTheme.colorScheme.error,
                    style = MaterialTheme.typography.bodySmall
                )
            }
            Spacer(Modifier.height(8.dp))
            Row(horizontalArrangement = Arrangement.spacedBy(12.dp), modifier = Modifier.fillMaxWidth()) {
                StatColumn("24h最高", formatDecimalOrDash(ticker?.highPrice))
                StatColumn("24h最低", formatDecimalOrDash(ticker?.lowPrice))
                StatColumn("成交量(BTC)", formatDecimalOrDash(ticker?.volumeBase))
            }
            Spacer(Modifier.height(8.dp))
            IntervalChips(
                intervals = intervals,
                selectedInterval = selectedInterval,
                onSelect = onSelectInterval
            )
           Spacer(Modifier.height(12.dp))
            if (loading) {
                Row(
                    verticalAlignment = Alignment.CenterVertically,
                    horizontalArrangement = Arrangement.spacedBy(8.dp),
                    modifier = Modifier.padding(vertical = 8.dp)
                ) {
                    CircularProgressIndicator(
                        modifier = Modifier.size(20.dp),
                        strokeWidth = 2.dp
                    )
                    Text("正在拉取 Binance K 线…", color = MaterialTheme.colorScheme.onSurfaceVariant)
                }
            }
            TradingViewChart(
                candles = klines,
                streamConnected = streamConnected,
                lastUpdateMs = lastUpdateMs,
                marketSource = marketSource,
                latencyMs = latencyMs
            )
            Spacer(Modifier.height(12.dp))
            val latestPrice = ticker?.lastPrice ?: klines.firstOrNull()?.close ?: BigDecimal.ZERO
            DexTradeControls(
                latestPrice = latestPrice,
                onBuy = onBuyClick,
                onSell = onSellClick
            )
        }
    }
}

@SuppressLint("SetJavaScriptEnabled")
@Composable
private fun TradingViewChart(
    candles: List<DexKlineBucket>,
    streamConnected: Boolean,
    lastUpdateMs: Long,
    marketSource: String,
    latencyMs: Long,
    modifier: Modifier = Modifier
        .fillMaxWidth()
        .height(280.dp)
) {
    val context = LocalContext.current
    val htmlContent = remember(context) { loadTradingViewHtml(context) }
    var lastNonEmpty by remember { mutableStateOf<List<DexKlineBucket>>(emptyList()) }
    LaunchedEffect(candles) {
        if (candles.isNotEmpty()) {
            lastNonEmpty = candles
        }
    }
    val effectiveCandles = if (candles.isNotEmpty()) candles else lastNonEmpty
    val hasHistoricalData = effectiveCandles.isNotEmpty()
    val json = remember(effectiveCandles) { effectiveCandles.toTradingViewJson() }
    val escapedJson = remember(json) { json.escapeForJs() }
    var webReady by remember { mutableStateOf(false) }
    var webError by remember { mutableStateOf<String?>(null) }
    val bridge = remember {
        TradingViewBridge(
            onReady = { webReady = true },
            onError = { message -> webError = message }
        )
    }
    val showFallback = webError != null || (!webReady && !hasHistoricalData)
    Box(modifier = modifier) {
        if (hasHistoricalData) {
            val fallbackAlpha = if (showFallback) 1f else 0f
            FallbackKlineChart(
                buckets = effectiveCandles,
                modifier = Modifier
                    .fillMaxSize()
                    .alpha(fallbackAlpha)
            )
        } else {
            Box(
                modifier = Modifier.fillMaxSize(),
                contentAlignment = Alignment.Center
            ) {
                Text(
                    "等待行情 K 线…",
                    color = MaterialTheme.colorScheme.onSurfaceVariant
                )
            }
        }
        AndroidView(
            factory = {
                WebView(context).apply {
                    bridge.attachWebView(this)
                    settings.javaScriptEnabled = true
                    settings.domStorageEnabled = true
                    settings.userAgentString = (settings.userAgentString ?: "") + " nim-libp2p-dex"
                    settings.allowFileAccess = true
                    settings.allowFileAccessFromFileURLs = true
                    settings.allowUniversalAccessFromFileURLs = true
                    setBackgroundColor(android.graphics.Color.TRANSPARENT)
                    webChromeClient = object : WebChromeClient() {
                        override fun onConsoleMessage(consoleMessage: ConsoleMessage): Boolean {
                            Log.d("TradingViewChart", "[WebView] ${consoleMessage.messageLevel()} ${consoleMessage.message()}")
                            return super.onConsoleMessage(consoleMessage)
                        }
                    }
                    webViewClient = object : WebViewClient() {
                        override fun onPageFinished(view: WebView?, url: String?) {
                            super.onPageFinished(view, url)
                            bridge.onPageReady()
                        }

                        override fun onReceivedError(
                            view: WebView?,
                            request: android.webkit.WebResourceRequest?,
                            error: android.webkit.WebResourceError?
                        ) {
                            super.onReceivedError(view, request, error)
                            if (request?.isForMainFrame == true) {
                                webError = error?.description?.toString() ?: "WebView error"
                            }
                        }
                    }
                    loadDataWithBaseURL(
                        "file:///android_asset/",
                        htmlContent,
                        "text/html",
                        "utf-8",
                        null
                    )
                }
            },
            update = {
                bridge.updateState(
                    payload = escapedJson,
                    connected = streamConnected,
                    timestamp = lastUpdateMs,
                    source = marketSource,
                    latencyMs = latencyMs
                )
                bridge.flush()
            },
            modifier = Modifier.fillMaxSize()
        )
        webError?.let { message ->
            Box(
                modifier = Modifier
                    .fillMaxSize()
                    .background(MaterialTheme.colorScheme.surfaceVariant.copy(alpha = 0.85f))
                    .padding(12.dp),
                contentAlignment = Alignment.Center
            ) {
                Text(
                    text = "TradingView 加载失败，显示本地图表。\n$message",
                    style = MaterialTheme.typography.labelMedium,
                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                    textAlign = TextAlign.Center
                )
            }
        }
    }
}

@Composable
private fun FallbackKlineChart(
    buckets: List<DexKlineBucket>,
    modifier: Modifier = Modifier
) {
    val display = remember(buckets) { buckets.take(60).reversed() }
    if (display.isEmpty()) {
        Box(modifier = modifier, contentAlignment = Alignment.Center) {
            Text(
                "暂无 K 线数据",
                color = MaterialTheme.colorScheme.onSurfaceVariant
            )
        }
        return
    }
    val ma20 = remember(display) { movingAverage(display, 20) }
    val ma50 = remember(display) { movingAverage(display, 50) }
    val priceMax = display.maxOfOrNull { it.high } ?: return
    val priceMin = display.minOfOrNull { it.low } ?: return
    val rawRange = priceMax.subtract(priceMin)
    val priceRange = if (rawRange <= BigDecimal.ZERO) BigDecimal("0.0001") else rawRange
    val priceMinFloat = priceMin.toFloat()
    val priceRangeFloat = priceRange.toFloat().coerceAtLeast(0.0001f)
    Canvas(modifier = modifier) {
        val candleWidth = if (display.isEmpty()) 0f else size.width / display.size
        val gridColor = Color.White.copy(alpha = 0.08f)
        val gridCount = 4
        val dashEffect = PathEffect.dashPathEffect(floatArrayOf(10f, 10f))
        for (i in 0..gridCount) {
            val y = size.height * (i / gridCount.toFloat())
            drawLine(
                color = gridColor,
                start = Offset(0f, y),
                end = Offset(size.width, y),
                strokeWidth = 1f,
                pathEffect = dashEffect
            )
        }
        display.forEachIndexed { index, bucket ->
            val high = bucket.high
            val low = bucket.low
            val open = bucket.open
            val close = bucket.close
            val candleColor = if (close >= open) Color(0xFF26A69A) else Color(0xFFE53935)
            val xCenter = index * candleWidth + candleWidth / 2f
            val highY = size.height - (((high.toFloat() - priceMinFloat) / priceRangeFloat) * size.height)
            val lowY = size.height - (((low.toFloat() - priceMinFloat) / priceRangeFloat) * size.height)
            val openY = size.height - (((open.toFloat() - priceMinFloat) / priceRangeFloat) * size.height)
            val closeY = size.height - (((close.toFloat() - priceMinFloat) / priceRangeFloat) * size.height)
            drawLine(
                color = candleColor,
                start = Offset(xCenter, highY),
                end = Offset(xCenter, lowY),
                strokeWidth = 2f
            )
            val top = min(openY, closeY)
            val bottom = max(openY, closeY)
            drawRect(
                color = candleColor.copy(alpha = 0.8f),
                topLeft = Offset(xCenter - candleWidth * 0.3f, top),
                size = Size(candleWidth * 0.6f, max(bottom - top, 2f))
            )
        }
        drawMaLine(ma20, priceMinFloat, priceRangeFloat, color = Color(0xFFFFC107))
        drawMaLine(ma50, priceMinFloat, priceRangeFloat, color = Color(0xFF3F51B5))
    }
}

private fun movingAverage(data: List<DexKlineBucket>, period: Int): List<Pair<Int, Float>> {
    if (data.isEmpty() || period <= 1) return emptyList()
    val result = mutableListOf<Pair<Int, Float>>()
    var sum = BigDecimal.ZERO
    val window = ArrayDeque<BigDecimal>()
    data.forEachIndexed { index, bucket ->
        sum += bucket.close
        window.addLast(bucket.close)
        if (window.size > period) {
            sum -= window.removeFirst()
        }
        if (window.size == period) {
            val avg = sum.divide(BigDecimal(period), 8, RoundingMode.HALF_UP)
            result.add(index to avg.toFloat())
        }
    }
    return result
}

private fun DrawScope.drawMaLine(points: List<Pair<Int, Float>>, priceMin: Float, priceRange: Float, color: Color) {
    if (points.isEmpty()) return
    val candleWidth = if (points.isNotEmpty()) size.width / (points.last().first + 1) else size.width
    var previous: Offset? = null
    points.forEach { (index, price) ->
        val x = index * candleWidth + candleWidth / 2f
        val normalized = size.height - (((price - priceMin) / priceRange) * size.height)
        val current = Offset(x, normalized)
        if (previous != null) {
            drawLine(color = color, start = previous!!, end = current, strokeWidth = 3f)
        }
        previous = current
    }
}

@Composable
private fun BinanceOrderBook(bids: List<OrderBookEntry>, asks: List<OrderBookEntry>) {
    Card {
        Column(modifier = Modifier.padding(16.dp)) {
            Text("盘口深度", style = MaterialTheme.typography.titleMedium, fontWeight = FontWeight.SemiBold)
            Spacer(Modifier.height(8.dp))
            Row(Modifier.fillMaxWidth()) {
                Text("卖盘", modifier = Modifier.weight(1f), color = Color(0xFFE53935), style = MaterialTheme.typography.bodySmall)
                Text("买盘", modifier = Modifier.weight(1f), color = Color(0xFF2EBD85), style = MaterialTheme.typography.bodySmall, textAlign = TextAlign.End)
            }
            Spacer(Modifier.height(8.dp))
            val visibleAsks = asks.takeLast(8)
            val askMax = visibleAsks.maxOfOrNull { it.cumulativeQuantity } ?: BigDecimal.ONE
            visibleAsks.forEach { entry ->
                DepthRow(entry, maxQuantity = askMax, isAsk = true)
            }
            Spacer(Modifier.height(8.dp))
            val visibleBids = bids.take(8)
            val bidMax = visibleBids.maxOfOrNull { it.cumulativeQuantity } ?: BigDecimal.ONE
            visibleBids.forEach { entry ->
                DepthRow(entry, maxQuantity = bidMax, isAsk = false)
            }
        }
    }
}

@Composable
private fun DepthRow(entry: OrderBookEntry, maxQuantity: BigDecimal, isAsk: Boolean) {
    val barColor = if (isAsk) Color(0x1AE53935) else Color(0x1A2EBD85)
    val priceColor = if (isAsk) Color(0xFFE53935) else Color(0xFF2EBD85)
    val formattedPrice = formatDecimalOrDash(entry.price)
    val formattedQty = entry.quantity.stripTrailingZeros().toPlainString()
    val ratio = if (maxQuantity > BigDecimal.ZERO) entry.cumulativeQuantity.divide(maxQuantity, 4, RoundingMode.HALF_UP).toFloat() else 0f
    val barWidth = ratio.coerceIn(0f, 1f)
    Box(
        modifier = Modifier
            .fillMaxWidth()
            .height(32.dp)
    ) {
        Box(
            modifier = Modifier
                .fillMaxWidth(barWidth)
                .fillMaxHeight()
                .background(color = barColor, shape = MaterialTheme.shapes.small)
                .align(if (isAsk) Alignment.CenterStart else Alignment.CenterEnd)
        )
        Row(
            modifier = Modifier
                .fillMaxWidth()
                .padding(horizontal = 8.dp),
            horizontalArrangement = Arrangement.SpaceBetween,
            verticalAlignment = Alignment.CenterVertically
        ) {
            if (isAsk) {
                Text(formattedPrice, color = priceColor, style = MaterialTheme.typography.bodyMedium)
                Text("$formattedQty BTC", style = MaterialTheme.typography.bodySmall)
            } else {
                Text("$formattedQty BTC", style = MaterialTheme.typography.bodySmall)
                Text(formattedPrice, color = priceColor, style = MaterialTheme.typography.bodyMedium)
            }
        }
    }
}

@Composable
private fun StreamingStatusBadge(connected: Boolean, lastUpdateMs: Long) {
    val badgeColor = if (connected) Color(0xFF2EBD85) else Color(0xFFE53935)
    val label = if (connected) "实时数据" else "重连中"
    val timeText = if (lastUpdateMs > 0) formatTimestamp(lastUpdateMs) else "--:--:--"
    Row(
        horizontalArrangement = Arrangement.spacedBy(8.dp),
        verticalAlignment = Alignment.CenterVertically
    ) {
        Box(
            modifier = Modifier
                .size(10.dp)
                .clip(CircleShape)
                .background(badgeColor)
        )
        Text(
            "$label · $timeText",
            style = MaterialTheme.typography.bodySmall,
            color = MaterialTheme.colorScheme.onSurfaceVariant
        )
    }
}

@Composable
private fun StatColumn(label: String, value: String) {
    Column {
        Text(label, style = MaterialTheme.typography.bodySmall, color = MaterialTheme.colorScheme.onSurfaceVariant)
        Text(value, style = MaterialTheme.typography.bodyMedium, fontWeight = FontWeight.Medium)
    }
}

@Composable
private fun IntervalChips(
    intervals: List<String>,
    selectedInterval: String,
    onSelect: (String) -> Unit
) {
    Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
        intervals.forEach { interval ->
            FilterChip(
                selected = interval == selectedInterval,
                onClick = { onSelect(interval) },
                label = { Text(interval) }
            )
        }
    }
}


@Composable
private fun DexOrderBook(buckets: List<DexKlineBucket>) {
    val latest = buckets.first()
    val basePrice = latest.close
    val computedStep = basePrice.multiply(BigDecimal("0.001"))
    val step = if (computedStep.compareTo(BigDecimal.ZERO) <= 0) BigDecimal("0.5") else computedStep
    val baseVolume = if (latest.volumeBase.compareTo(BigDecimal.ZERO) <= 0) BigDecimal("0.01") else latest.volumeBase
    val asks = (1..4).map { level ->
        val price = basePrice.add(step.multiply(BigDecimal(level)))
        price to baseVolume.add(BigDecimal(level).multiply(BigDecimal("0.02")))
    }
    val bids = (1..4).map { level ->
        val price = basePrice.subtract(step.multiply(BigDecimal(level)))
        price to baseVolume.add(BigDecimal(level).multiply(BigDecimal("0.015")))
    }
    Text("深度 (仿 Binance)", style = MaterialTheme.typography.titleSmall, fontWeight = FontWeight.Medium)
    Spacer(Modifier.height(8.dp))
    Row {
        Column(modifier = Modifier.weight(1f)) {
            Text("卖盘", color = Color(0xFFE53935), style = MaterialTheme.typography.bodySmall)
            asks.forEach { (price, volume) ->
                Row(
                    modifier = Modifier
                        .fillMaxWidth()
                        .padding(vertical = 4.dp),
                    horizontalArrangement = Arrangement.SpaceBetween
                ) {
                    Text(formatDecimal(price), color = Color(0xFFE53935), style = MaterialTheme.typography.bodyMedium)
                    Text("${formatDecimal(volume)} BTC", style = MaterialTheme.typography.bodySmall)
                }
            }
        }
        Spacer(Modifier.width(12.dp))
        Column(modifier = Modifier.weight(1f)) {
            Text("买盘", color = Color(0xFF26A69A), style = MaterialTheme.typography.bodySmall)
            bids.forEach { (price, volume) ->
                Row(
                    modifier = Modifier
                        .fillMaxWidth()
                        .padding(vertical = 4.dp),
                    horizontalArrangement = Arrangement.SpaceBetween
                ) {
                    Text(formatDecimal(price), color = Color(0xFF26A69A), style = MaterialTheme.typography.bodyMedium)
                    Text("${formatDecimal(volume)} BTC", style = MaterialTheme.typography.bodySmall)
                }
            }
        }
    }
}

@Composable
private fun DexTradeControls(
    latestPrice: BigDecimal,
    onBuy: (String) -> Unit = {},
    onSell: (String) -> Unit = {}
) {
    var orderType by rememberSaveable { mutableStateOf("限价") }
    var priceInput by rememberSaveable { mutableStateOf(formatDecimal(latestPrice)) }
    var amountInput by rememberSaveable { mutableStateOf("0.10") }
    var sliderValue by rememberSaveable { mutableStateOf(0.10f) }

    LaunchedEffect(orderType, latestPrice) {
        if (orderType == "市价") {
            priceInput = formatDecimal(latestPrice)
        }
    }

    val parsedAmount = amountInput.toBigDecimalOrNull() ?: BigDecimal.ZERO
    val parsedPrice = priceInput.toBigDecimalOrNull() ?: BigDecimal.ZERO
    val effectivePrice = if (orderType == "市价") latestPrice else parsedPrice
    val notional = if (effectivePrice > BigDecimal.ZERO) {
        effectivePrice.multiply(parsedAmount).setScale(2, RoundingMode.HALF_UP)
    } else {
        BigDecimal.ZERO
    }
    val isValidOrder = parsedAmount > BigDecimal.ZERO && (effectivePrice > BigDecimal.ZERO || orderType == "市价")

    Text("专业下单", style = MaterialTheme.typography.titleSmall, fontWeight = FontWeight.Medium)
    Spacer(Modifier.height(8.dp))
    Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
        listOf("限价", "市价").forEach { type ->
            FilterChip(
                selected = orderType == type,
                onClick = { orderType = type },
                label = { Text(type) }
            )
        }
    }
    Spacer(Modifier.height(12.dp))
    OutlinedTextField(
        value = priceInput,
        onValueChange = { priceInput = it },
        modifier = Modifier.fillMaxWidth(),
        enabled = orderType == "限价",
        label = { Text("委托价 (USDC)") },
        supportingText = {
            if (orderType == "市价") {
                Text("按最近成交价执行", color = MaterialTheme.colorScheme.tertiary)
            }
        },
        keyboardOptions = KeyboardOptions(keyboardType = KeyboardType.Decimal)
    )
    Spacer(Modifier.height(8.dp))
    OutlinedTextField(
        value = amountInput,
        onValueChange = {
            amountInput = it
            it.toFloatOrNull()?.let { value ->
                sliderValue = value.coerceIn(0f, 1f)
            }
        },
        modifier = Modifier.fillMaxWidth(),
        label = { Text("数量 (BTC)") },
        keyboardOptions = KeyboardOptions(keyboardType = KeyboardType.Decimal)
    )
    Spacer(Modifier.height(12.dp))
    Text("快速调节", style = MaterialTheme.typography.bodySmall, color = MaterialTheme.colorScheme.onSurfaceVariant)
    Slider(
        value = sliderValue,
        onValueChange = { value ->
            sliderValue = value
            val normalized = BigDecimal(value.toDouble()).setScale(3, RoundingMode.HALF_UP)
            amountInput = normalized.stripTrailingZeros().toPlainString()
        },
        valueRange = 0f..1f,
        steps = 9,
        modifier = Modifier.fillMaxWidth(),
        colors = SliderDefaults.colors(
            thumbColor = Color(0xFF2EBD85),
            activeTrackColor = Color(0xFF2EBD85)
        )
    )
    val presetPercents = listOf(
        0.25f to "25%",
        0.5f to "50%",
        0.75f to "75%",
        1f to "100%"
    )
    Row(horizontalArrangement = Arrangement.spacedBy(8.dp), modifier = Modifier.fillMaxWidth()) {
        presetPercents.forEach { (value, label) ->
            FilterChip(
                selected = abs(sliderValue - value) < 0.01f,
                onClick = {
                    sliderValue = value
                    val normalized = BigDecimal(value.toDouble()).setScale(3, RoundingMode.HALF_UP)
                    amountInput = normalized.stripTrailingZeros().toPlainString()
                },
                label = { Text(label) }
            )
        }
    }
    Spacer(Modifier.height(12.dp))
    Row(
        modifier = Modifier.fillMaxWidth(),
        horizontalArrangement = Arrangement.SpaceBetween
    ) {
        Column {
            Text("最新价", style = MaterialTheme.typography.bodySmall, color = MaterialTheme.colorScheme.onSurfaceVariant)
            Text(
                "${formatDecimal(latestPrice)} USDC",
                style = MaterialTheme.typography.bodyMedium,
                fontWeight = FontWeight.Medium
            )
        }
        Column(horizontalAlignment = Alignment.End) {
            Text("预估总额", style = MaterialTheme.typography.bodySmall, color = MaterialTheme.colorScheme.onSurfaceVariant)
            Text(
                "${formatDecimal(notional)} USDC",
                style = MaterialTheme.typography.bodyMedium,
                fontWeight = FontWeight.SemiBold
            )
        }
    }
    Spacer(Modifier.height(12.dp))
    Row(horizontalArrangement = Arrangement.spacedBy(12.dp), modifier = Modifier.fillMaxWidth()) {
        Button(
            onClick = {
                val summary = "买入$orderType · ${formatDecimal(parsedAmount)} BTC @ ${if (orderType == "市价") "市价" else "${formatDecimal(effectivePrice)} USDC"} ≈ ${formatDecimal(notional)} USDC"
                onBuy(summary)
            },
            enabled = isValidOrder,
            modifier = Modifier.weight(1f),
            colors = ButtonDefaults.buttonColors(containerColor = Color(0xFF2EBD85))
        ) {
            Text("买入 BTC")
        }
        Button(
            onClick = {
                val summary = "卖出$orderType · ${formatDecimal(parsedAmount)} BTC @ ${if (orderType == "市价") "市价" else "${formatDecimal(effectivePrice)} USDC"} ≈ ${formatDecimal(notional)} USDC"
                onSell(summary)
            },
            enabled = isValidOrder,
            modifier = Modifier.weight(1f),
            colors = ButtonDefaults.buttonColors(containerColor = Color(0xFFED6A5A))
        ) {
            Text("卖出 BTC")
        }
    }
}

@Composable
private fun dexStatusColor(status: DexSwapStatus): Color =
    when (status) {
        DexSwapStatus.Confirmed -> MaterialTheme.colorScheme.primary
        DexSwapStatus.Pending -> MaterialTheme.colorScheme.tertiary
        DexSwapStatus.Failed -> MaterialTheme.colorScheme.error
        DexSwapStatus.Broadcasting -> MaterialTheme.colorScheme.tertiary
    }

private fun formatDecimal(value: BigDecimal): String =
    value.stripTrailingZeros().toPlainString()

private fun formatDecimalOrDash(value: BigDecimal?): String =
    value?.stripTrailingZeros()?.toPlainString() ?: "--"

private fun formatLatencyDisplay(latencyMs: Long): String =
    if (latencyMs <= 0) "--" else "${latencyMs} ms"

private fun formatLatencyAverage(value: Double): String =
    if (value <= 0) "--" else "${value.roundToInt()} ms"

private fun MarketSourceLatency.successRateText(): String {
    val total = successCount + failureCount
    if (total <= 0) return "--"
    val rate = ((successCount.toDouble() / total) * 100).roundToInt()
    return "$rate%"
}

private fun List<DexKlineBucket>.toTradingViewJson(): String {
    if (isEmpty()) return "[]"
    val ordered = sortedBy { it.windowStartMs }
    val builder = StringBuilder("[")
    ordered.forEachIndexed { index, bucket ->
        if (index > 0) builder.append(',')
        builder.append('{')
        builder.append("\"time\":").append(bucket.windowStartMs / 1000)
        builder.append(',').append("\"open\":\"").append(bucket.open.stripTrailingZeros().toPlainString()).append('\"')
        builder.append(',').append("\"high\":\"").append(bucket.high.stripTrailingZeros().toPlainString()).append('\"')
        builder.append(',').append("\"low\":\"").append(bucket.low.stripTrailingZeros().toPlainString()).append('\"')
        builder.append(',').append("\"close\":\"").append(bucket.close.stripTrailingZeros().toPlainString()).append('\"')
        builder.append(',').append("\"volume\":\"").append(bucket.volumeBase.stripTrailingZeros().toPlainString()).append('\"')
        builder.append('}')
    }
    builder.append(']')
    return builder.toString()
}

private fun String.escapeForJs(): String =
    this.replace("\\", "\\\\").replace("'", "\\'")

private fun loadTradingViewHtml(context: Context): String {
    val html = context.assets.open("tv_chart.html").bufferedReader().use { it.readText() }
    val js = context.assets.open("lightweight-charts.standalone.production.js").bufferedReader().use { it.readText() }
    val sanitizedJs = js.replace("</script>", "<\\/script>")
    return html.replace(
        "<script src=\"lightweight-charts.standalone.production.js\"></script>",
        "<script>$sanitizedJs</script>"
    )
}

private class TradingViewBridge(
    private val onReady: (() -> Unit)? = null,
    private val onError: ((String?) -> Unit)? = null
) {
    private var webView: WebView? = null
    private var pageReady: Boolean = false
    private var payload: String? = null
    private var connected: Boolean = false
    private var lastUpdateMs: Long = 0L
    private var sourceLabel: String = ""
    private var latencyMs: Long = -1L

    fun attachWebView(view: WebView) {
        webView = view
        pageReady = false
    }

    fun onPageReady() {
        pageReady = true
        onReady?.invoke()
        flush()
    }

    fun updateState(
        payload: String,
        connected: Boolean,
        timestamp: Long,
        source: String,
        latencyMs: Long
    ) {
        this.payload = payload
        this.connected = connected
        this.lastUpdateMs = timestamp
        this.sourceLabel = source
        this.latencyMs = latencyMs
    }

    fun flush() {
        val view = webView ?: return
        val data = payload ?: return
        if (!pageReady) return
        val escapedSource = sourceLabel.escapeForJs()
        view.post {
            try {
                view.evaluateJavascript("window.renderCandles('$data');", null)
                view.evaluateJavascript(
                    "window.updateStreamStatus($connected, $lastUpdateMs, '$escapedSource', $latencyMs);",
                    null
                )
            } catch (err: Throwable) {
                onError?.invoke(err.message)
            }
        }
    }
}

@Composable
private fun LiveTab(
    streamKey: String,
    payload: String,
    frames: List<com.example.libp2psmoke.model.LivestreamFrame>,
    onStreamKeyChanged: (String) -> Unit,
    onPayloadChanged: (String) -> Unit,
    onPublish: () -> Unit
) {
    Column(
        modifier = Modifier
            .fillMaxSize()
            .padding(16.dp)
    ) {
        OutlinedTextField(
            value = streamKey,
            onValueChange = onStreamKeyChanged,
            modifier = Modifier.fillMaxWidth(),
            label = { Text("Stream key") }
        )
        Spacer(Modifier.height(8.dp))
        OutlinedTextField(
            value = payload,
            onValueChange = onPayloadChanged,
            modifier = Modifier.fillMaxWidth(),
            label = { Text("Frame payload (text)") },
            maxLines = 4
        )
        Spacer(Modifier.height(8.dp))
        Row(horizontalArrangement = Arrangement.End, modifier = Modifier.fillMaxWidth()) {
            Button(onClick = onPublish, enabled = payload.isNotBlank()) {
                Text("Publish frame")
            }
        }
        Spacer(Modifier.height(16.dp))
        Text("Recent Frames", style = MaterialTheme.typography.titleMedium)
        LazyColumn(
            verticalArrangement = Arrangement.spacedBy(8.dp),
            modifier = Modifier.fillMaxSize()
        ) {
            items(frames, key = { it.timestampMs to it.frameIndex }) { frame ->
                Card {
                    Column(modifier = Modifier.padding(16.dp)) {
                        Text(frame.streamKey, fontWeight = FontWeight.Bold)
                        Text("Frame #${frame.frameIndex} · ${frame.payloadSize} bytes")
                        Text(formatTimestamp(frame.timestampMs), style = MaterialTheme.typography.bodySmall)
                    }
                }
            }
        }
    }
}

@Composable
private fun AttachmentPreview(attachment: FeedAttachment) {
    when (attachment.kind) {
        AttachmentKind.IMAGE -> {
            val bitmap = rememberAttachmentBitmap(attachment.bytes)
            if (bitmap != null) {
                Image(
                    bitmap = bitmap,
                    contentDescription = attachment.label,
                    modifier = Modifier
                        .fillMaxWidth()
                        .heightIn(min = 120.dp, max = 280.dp)
                        .clip(MaterialTheme.shapes.medium),
                    contentScale = ContentScale.Crop
                )
            } else {
                Text("🖼 ${attachment.label} (${attachment.bytes?.size ?: 0} bytes)")
            }
        }

        AttachmentKind.AUDIO -> {
            Text("🔊 ${attachment.label}", style = MaterialTheme.typography.bodyMedium)
            attachment.uri?.let {
                Text(it, style = MaterialTheme.typography.bodySmall, color = MaterialTheme.colorScheme.primary)
            }
        }

        AttachmentKind.VIDEO -> {
            Text("🎬 ${attachment.label}", style = MaterialTheme.typography.bodyMedium)
            attachment.uri?.let {
                Text(it, style = MaterialTheme.typography.bodySmall, color = MaterialTheme.colorScheme.primary)
            }
        }

        AttachmentKind.LINK -> {
            val link = attachment.uri ?: attachment.text ?: ""
            Text("🔗 ${attachment.label}: $link")
        }

        AttachmentKind.TEXT -> {
            Text(attachment.text ?: "", style = MaterialTheme.typography.bodyMedium)
        }

        AttachmentKind.DATA -> {
            Text("📎 ${attachment.label} (${attachment.bytes?.size ?: 0} bytes)")
        }

        AttachmentKind.UNKNOWN -> {
            Text("ℹ️ ${attachment.label}")
        }
    }
}

@Composable
private fun rememberAttachmentBitmap(bytes: ByteArray?): ImageBitmap? {
    return remember(bytes) {
        if (bytes == null || bytes.isEmpty()) {
            null
        } else {
            try {
                BitmapFactory.decodeByteArray(bytes, 0, bytes.size)?.asImageBitmap()
            } catch (_: Exception) {
                null
            }
        }
    }
}

private fun formatTimestamp(timestamp: Long): String {
    return SimpleDateFormat("HH:mm:ss", Locale.getDefault()).format(Date(timestamp))
}
