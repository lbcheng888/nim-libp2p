package com.example.libp2psmoke.ui.screens

import androidx.compose.animation.AnimatedVisibility
import androidx.compose.animation.animateColorAsState
import androidx.compose.animation.core.tween
import androidx.compose.animation.fadeIn
import androidx.compose.animation.fadeOut
import androidx.compose.foundation.background
import androidx.compose.foundation.clickable
import androidx.compose.foundation.horizontalScroll
import androidx.compose.foundation.interaction.MutableInteractionSource
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.verticalScroll
import androidx.compose.foundation.shape.CircleShape
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.filled.ArrowDownward
import androidx.compose.material.icons.filled.Security
import androidx.compose.material.icons.filled.Group
import androidx.compose.material.icons.filled.Check
import com.example.libp2psmoke.ui.theme.AppColors
import com.example.libp2psmoke.ui.theme.MonoFontFamily
import androidx.compose.material.icons.filled.KeyboardArrowDown
import androidx.compose.material.icons.filled.KeyboardArrowUp
import androidx.compose.material.icons.outlined.StarBorder
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.draw.clip
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import com.example.libp2psmoke.ui.theme.DexGreen
import com.example.libp2psmoke.ui.theme.DexPrimary
import com.example.libp2psmoke.ui.theme.MonoFontFamily
import com.example.libp2psmoke.ui.UiIntent
import com.example.libp2psmoke.dex.AtomicSwapState
import com.example.libp2psmoke.BuildConfig
import com.example.libp2psmoke.model.NodeUiState
import com.example.libp2psmoke.ui.components.*
import com.example.libp2psmoke.ui.theme.*
import com.example.libp2psmoke.ui.utils.formatDecimal
import com.example.libp2psmoke.ui.utils.formatDecimalOrDash
import com.example.libp2psmoke.ui.utils.formatVolumeOrDash
import com.example.libp2psmoke.ui.utils.sanitizeDecimalInput
import com.example.libp2psmoke.viewmodel.NimNodeViewModel
import com.example.libp2psmoke.dex.BtcAddressType
import java.math.BigDecimal
import java.math.RoundingMode
import androidx.compose.ui.draw.rotate
import androidx.compose.foundation.text.KeyboardOptions
import androidx.compose.ui.text.input.ImeAction
import androidx.compose.ui.text.input.KeyboardType

@Composable
fun ProDexScreen(
    uiState: NodeUiState,
    viewModel: NimNodeViewModel,
    snackbarHostState: SnackbarHostState
) {
    var showTradeDialog by remember { mutableStateOf(false) }
    var initialTradeSide by remember { mutableStateOf("Buy") }
    val defaultWif = if (BuildConfig.DEBUG) BuildConfig.DEFAULT_BTC_TESTNET_WIF else ""
    val defaultAddress = if (BuildConfig.DEBUG) BuildConfig.DEFAULT_BTC_TESTNET_ADDRESS else ""

    Box(modifier = Modifier.fillMaxSize().background(MaterialTheme.colorScheme.background)) {
        Column(
            modifier = Modifier
                .fillMaxSize()
                .padding(bottom = 80.dp) // Leave space for bottom buttons
                .verticalScroll(rememberScrollState())
        ) {
            // ═══════════════════════════════════════════════════════════════════
            // 顶部工具栏
            // ═══════════════════════════════════════════════════════════════════
            Row(
                modifier = Modifier
                    .fillMaxWidth()
                    .padding(horizontal = 16.dp, vertical = 12.dp),
                horizontalArrangement = Arrangement.SpaceBetween,
                verticalAlignment = Alignment.CenterVertically
            ) {
                Row(verticalAlignment = Alignment.CenterVertically) {
                    // 交易对选择器
                    Row(
                        modifier = Modifier
                            .clip(RoundedCornerShape(8.dp))
                            .background(MaterialTheme.colorScheme.surfaceVariant)
                            .clickable { /* 打开交易对选择器 */ }
                            .padding(horizontal = 12.dp, vertical = 8.dp),
                        verticalAlignment = Alignment.CenterVertically
                    ) {
                        ChainIcon(chainId = "BTC", size = 24.dp)
                        Spacer(Modifier.width(8.dp))
                        Text(
                            "BTC/USDC", 
                            style = MaterialTheme.typography.titleMedium, 
                            color = MaterialTheme.colorScheme.onSurface,
                            fontWeight = FontWeight.Bold
                        )
                        Spacer(Modifier.width(4.dp))
                        Icon(
                            Icons.Default.KeyboardArrowDown,
                            contentDescription = null,
                            tint = MaterialTheme.colorScheme.onSurfaceVariant,
                            modifier = Modifier.size(20.dp)
                        )
                    }
                    Spacer(Modifier.width(12.dp))
                    // 涨跌幅标签
                    val isPositive = true // TODO: 从数据计算
                    Box(
                        modifier = Modifier
                            .clip(RoundedCornerShape(4.dp))
                            .background(
                                if (isPositive) DexGreen.copy(alpha = 0.15f)
                                else DexRed.copy(alpha = 0.15f)
                            )
                            .padding(horizontal = 8.dp, vertical = 4.dp)
                    ) {
                        Text(
                            "+2.45%",
                            color = if (isPositive) DexGreen else DexRed,
                            fontSize = 12.sp,
                            fontWeight = FontWeight.SemiBold,
                            fontFamily = MonoFontFamily
                        )
                    }
                }
                
                Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                    // 实时状态指示器
                    LiveIndicator(isConnected = uiState.binanceStreamConnected)
                    
                    IconButton(
                        onClick = { /* 收藏 */ },
                        modifier = Modifier.size(36.dp)
                    ) {
                        Icon(
                            Icons.Outlined.StarBorder, 
                            "Favorite", 
                            tint = MaterialTheme.colorScheme.onSurfaceVariant,
                            modifier = Modifier.size(20.dp)
                        )
                    }
                }
            }
    
            // ═══════════════════════════════════════════════════════════════════
            // 通知横幅
            // ═══════════════════════════════════════════════════════════════════
            DexNotificationBanner(
                message = uiState.lastError ?: "",
                type = NotificationType.ERROR,
                visible = uiState.lastError != null
            )
            DexNotificationBanner(
                message = uiState.successMessage ?: "",
                type = NotificationType.SUCCESS,
                visible = uiState.successMessage != null
            )
            DexNotificationBanner(
                message = uiState.binanceError ?: "",
                type = NotificationType.WARNING,
                visible = uiState.binanceError != null
            )
    
            // ═══════════════════════════════════════════════════════════════════
            // 价格信息区域
            // ═══════════════════════════════════════════════════════════════════
            val lastPrice = resolveLastPrice(uiState)
            Row(
                modifier = Modifier
                    .fillMaxWidth()
                    .padding(horizontal = 16.dp, vertical = 8.dp)
            ) {
                Column {
                    Text(
                        formatDecimalOrDash(lastPrice),
                        style = PriceTextStyle, 
                        color = DexGreen
                    )
                    Text(
                        "≈ $${formatDecimalOrDash(lastPrice)}",
                        style = MaterialTheme.typography.bodySmall, 
                        color = MaterialTheme.colorScheme.onSurfaceVariant
                    )
                }
                Spacer(Modifier.weight(1f))
                
                // 24小时数据
                Row(horizontalArrangement = Arrangement.spacedBy(24.dp)) {
                    StatColumn(
                        label = "24h High",
                        value = formatDecimalOrDash(uiState.binanceTicker?.highPrice),
                        valueColor = DexGreen
                    )
                    StatColumn(
                        label = "24h Low",
                        value = formatDecimalOrDash(uiState.binanceTicker?.lowPrice),
                        valueColor = DexRed
                    )
                    StatColumn(
                        label = "24h Vol",
                        value = formatVolumeOrDash(uiState.binanceTicker?.volumeBase)
                    )
                }
            }
    
            // ═══════════════════════════════════════════════════════════════════
            // 时间周期选择器
            // ═══════════════════════════════════════════════════════════════════
            Row(
                modifier = Modifier
                    .fillMaxWidth()
                    .padding(horizontal = 8.dp, vertical = 4.dp)
                    .horizontalScroll(rememberScrollState()),
                verticalAlignment = Alignment.CenterVertically
            ) {
                val intervals = viewModel.binanceIntervals()
                intervals.forEach { interval ->
                    val isSelected = interval == uiState.binanceInterval
                    val bgColor by animateColorAsState(
                        targetValue = if (isSelected) DexPrimary.copy(alpha = 0.15f) else Color.Transparent,
                        animationSpec = tween(200),
                        label = "intervalBg"
                    )
                    
                    Box(
                        modifier = Modifier
                            .padding(horizontal = 4.dp)
                            .clip(RoundedCornerShape(6.dp))
                            .background(bgColor)
                            .clickable(
                                interactionSource = remember { MutableInteractionSource() },
                                indication = null
                            ) { viewModel.onEvent(UiIntent.SelectInterval(interval)) }
                            .padding(horizontal = 12.dp, vertical = 6.dp)
                    ) {
                        Text(
                            text = interval,
                            color = if (isSelected) DexPrimary else MaterialTheme.colorScheme.onSurfaceVariant,
                            fontSize = 13.sp,
                            fontWeight = if (isSelected) FontWeight.Bold else FontWeight.Normal
                        )
                    }
                }
            }
    
            // ═══════════════════════════════════════════════════════════════════
            // 图表区域 (固定高度)
            // ═══════════════════════════════════════════════════════════════════
            Box(
                modifier = Modifier
                    .height(400.dp) // Fixed height
                    .fillMaxWidth()
                    .padding(horizontal = 8.dp)
                    .clip(RoundedCornerShape(12.dp))
            ) {
                val chartCandles = uiState.binanceKlines.ifEmpty { uiState.dexKlines }
                val chartCandlesJson = if (uiState.binanceKlines.isNotEmpty()) uiState.binanceKlinesJson else "[]"
                TradingViewChart(
                    candles = chartCandles,
                    candlesJson = chartCandlesJson,
                    interval = uiState.binanceInterval,
                    streamConnected = uiState.binanceStreamConnected,
                    lastUpdateMs = uiState.binanceLastUpdateMs,
                    marketSource = uiState.marketSource,
                    latencyMs = uiState.marketLatencyMs,
                    modifier = Modifier.fillMaxSize()
                )
                
                // 加载指示器 (加载中 或 无数据时显示)
                androidx.compose.animation.AnimatedVisibility(
                    visible = uiState.binanceLoading || (uiState.binanceKlines.isEmpty() && uiState.dexKlines.isEmpty()),
                    enter = fadeIn(),
                    exit = fadeOut(),
                    modifier = Modifier.align(Alignment.Center)
                ) {
                    DexLoadingIndicator(color = DexGreen)
                }
            }
    
            // ═══════════════════════════════════════════════════════════════════
            // 订单簿预览
            // ═══════════════════════════════════════════════════════════════════
            DexCard(
                modifier = Modifier
                    .fillMaxWidth()
                    .padding(horizontal = 8.dp, vertical = 4.dp),
                shape = RoundedCornerShape(12.dp),
                containerColor = MaterialTheme.colorScheme.surface,
                contentPadding = PaddingValues(12.dp)
            ) {
                Row(modifier = Modifier.fillMaxWidth().height(100.dp)) {
                    // Bids (买单)
                    Column(modifier = Modifier.weight(1f)) {
                        Text(
                            "Bids", 
                            fontSize = 11.sp, 
                            color = DexGreen,
                            fontWeight = FontWeight.SemiBold
                        )
                        Spacer(Modifier.height(4.dp))
                        uiState.binanceBids.take(4).forEach { bid ->
                            OrderBookRow(
                                price = formatDecimal(bid.price),
                                amount = formatDecimal(bid.quantity),
                                isBid = true
                            )
                        }
                    }
                    
                    // 中间价格
                    Column(
                        modifier = Modifier.width(80.dp),
                        horizontalAlignment = Alignment.CenterHorizontally
                    ) {
                        Spacer(Modifier.height(16.dp))
                        Text(
                            formatDecimalOrDash(lastPrice),
                            fontFamily = MonoFontFamily,
                            fontSize = 14.sp,
                            fontWeight = FontWeight.Bold,
                            color = DexGreen
                        )
                        Text(
                            "Spread: 0.01%",
                            fontSize = 10.sp,
                            color = MaterialTheme.colorScheme.onSurfaceVariant
                        )
                    }
                    
                    // Asks (卖单)
                    Column(modifier = Modifier.weight(1f)) {
                        Text(
                            "Asks", 
                            fontSize = 11.sp, 
                            color = DexRed,
                            fontWeight = FontWeight.SemiBold
                        )
                        Spacer(Modifier.height(4.dp))
                        uiState.binanceAsks.take(4).forEach { ask ->
                            OrderBookRow(
                                price = formatDecimal(ask.price),
                                amount = formatDecimal(ask.quantity),
                                isBid = false
                            )
                        }
                    }
                }
            }
    
            // ═══════════════════════════════════════════════════════════════════
            // 原子交换状态卡片 (Atomic Swap Status)
            // ═══════════════════════════════════════════════════════════════════
            if (uiState.activeSwap != null) {
                val swap = uiState.activeSwap
                DexCard(
                    modifier = Modifier
                        .fillMaxWidth()
                        .padding(horizontal = 8.dp, vertical = 4.dp),
                    shape = RoundedCornerShape(12.dp),
                    containerColor = MaterialTheme.colorScheme.surfaceVariant.copy(alpha = 0.5f),
                    contentPadding = PaddingValues(16.dp)
                ) {
                    Column {
                        Row(
                            modifier = Modifier.fillMaxWidth(),
                            horizontalArrangement = Arrangement.SpaceBetween,
                            verticalAlignment = Alignment.CenterVertically
                        ) {
                            Text(
                                "Active Atomic Swap",
                                style = MaterialTheme.typography.titleSmall,
                                fontWeight = FontWeight.Bold,
                                color = DexPrimary
                            )
                            Text(
                                swap.status.name,
                                fontSize = 12.sp,
                                fontWeight = FontWeight.Bold,
                                color = if (swap.status == com.example.libp2psmoke.dex.SwapStatus.BTC_LOCKED) DexGreen else MaterialTheme.colorScheme.onSurfaceVariant
                            )
                        }
                        Spacer(Modifier.height(8.dp))
                        Text("ID: ${swap.id.take(8)}...", fontSize = 12.sp, fontFamily = MonoFontFamily)
                        Text("Role: ${swap.role}", fontSize = 12.sp)
                        
                        if (swap.status == com.example.libp2psmoke.dex.SwapStatus.BTC_LOCKED) {
                            Spacer(Modifier.height(12.dp))
                            Button(
                                onClick = { viewModel.onEvent(UiIntent.ClaimAtomicSwap(swap.id)) },
                                colors = ButtonDefaults.buttonColors(containerColor = DexGreen),
                                modifier = Modifier.fillMaxWidth()
                            ) {
                                Text("Claim BTC (Reveal Secret)")
                            }
                        }
                    }
                }
            }

            // ═══════════════════════════════════════════════════════════════════
            // Adapter Swap Status (Privacy Mode)
            // ═══════════════════════════════════════════════════════════════════
            val activeAdapterSwap by viewModel.activeAdapterSwap.collectAsState()
            AdapterSwapStatusCard(activeAdapterSwap)
            
            val activeMpcSwap by viewModel.activeMpcSwap.collectAsState()
            MpcSwapStatusCard(activeMpcSwap)
            
            Spacer(Modifier.height(16.dp))
            
            // Privacy Swap Trigger (Demo) - Removed as integrated into TradeDialog
            
            // Display Valid Signature if available
            activeAdapterSwap?.let { swap ->
                if (swap.validSignature != null) {
                    Spacer(modifier = Modifier.height(4.dp))
                    Text(
                        text = "Valid Sig: ${swap.validSignature.take(20)}...",
                        style = MaterialTheme.typography.labelSmall,
                        color = AppColors.Success,
                        fontFamily = MonoFontFamily
                    )
                }
            }

            // ═══════════════════════════════════════════════════════════════════
            // 交易历史
            // ═══════════════════════════════════════════════════════════════════
            if (uiState.dexSwaps.isNotEmpty()) {
                TransactionHistory(swaps = uiState.dexSwaps)
            }
            
            // Extra spacer for scrolling past buttons
            Spacer(Modifier.height(24.dp))
        } // End Column

        // ═══════════════════════════════════════════════════════════════════
        // 底部操作按钮 (Fixed at bottom)
        // ═══════════════════════════════════════════════════════════════════
        Row(
            modifier = Modifier
                .align(Alignment.BottomCenter)
                .fillMaxWidth()
                .background(MaterialTheme.colorScheme.background.copy(alpha = 0.95f))
                .padding(horizontal = 16.dp, vertical = 12.dp),
            horizontalArrangement = Arrangement.spacedBy(12.dp)
        ) {
            DexBuyButton(
                text = "Buy BTC",
                onClick = { 
                    initialTradeSide = "Buy"
                    showTradeDialog = true 
                },
                modifier = Modifier.weight(1f)
            )
            DexSellButton(
                text = "Sell BTC",
                onClick = { 
                    initialTradeSide = "Sell"
                    showTradeDialog = true
                },
                modifier = Modifier.weight(1f)
            )
        }
    } // End Box

    // 交易弹窗
    if (showTradeDialog) {
        TradeDialog(
            initialSide = initialTradeSide,
            viewModel = viewModel,
            uiState = uiState,
            onDismiss = { showTradeDialog = false }
        )
    }
    
    // 全局加载遮罩 (用于提交交易时)
    if (uiState.bridgeLoading) {
        Box(
            modifier = Modifier
                .fillMaxSize()
                .background(Color.Black.copy(alpha = 0.5f))
                .clickable(enabled = false) {}, // 拦截点击
            contentAlignment = Alignment.Center
        ) {
            Card(
                colors = CardDefaults.cardColors(containerColor = MaterialTheme.colorScheme.surface),
                shape = RoundedCornerShape(16.dp)
            ) {
                Column(
                    modifier = Modifier.padding(24.dp),
                    horizontalAlignment = Alignment.CenterHorizontally
                ) {
                    CircularProgressIndicator(color = DexPrimary)
                    Spacer(Modifier.height(16.dp))
                    Text("Processing Transaction...", fontWeight = FontWeight.Medium)
                }
            }
        }
    }
}

@Composable
private fun StatColumn(
    label: String,
    value: String,
    valueColor: Color = MaterialTheme.colorScheme.onSurface
) {
    Column(horizontalAlignment = Alignment.End) {
        Text(
            label, 
            fontSize = 10.sp, 
            color = MaterialTheme.colorScheme.onSurfaceVariant
        )
        Text(
            value, 
            fontSize = 12.sp, 
            color = valueColor,
            fontFamily = MonoFontFamily,
            fontWeight = FontWeight.Medium
        )
    }
}

@Composable
private fun OrderBookRow(
    price: String,
    amount: String,
    isBid: Boolean
) {
    Row(
        modifier = Modifier
            .fillMaxWidth()
            .padding(vertical = 2.dp),
        horizontalArrangement = Arrangement.SpaceBetween
    ) {
        Text(
            price, 
            fontSize = 11.sp, 
            color = if (isBid) DexGreen else DexRed,
            fontFamily = MonoFontFamily
        )
        Text(
            amount, 
            fontSize = 11.sp, 
            color = MaterialTheme.colorScheme.onSurfaceVariant,
            fontFamily = MonoFontFamily
        )
    }
}

@OptIn(ExperimentalMaterial3Api::class)
@Composable
fun TradeDialog(
    initialSide: String,
    viewModel: NimNodeViewModel,
    uiState: NodeUiState,
    onDismiss: () -> Unit
) {
    var selectedSide by remember { mutableStateOf(initialSide) }
    var amountText by remember { mutableStateOf("") }
    var privateKey by remember(selectedSide) { 
        mutableStateOf(
            if (!BuildConfig.DEBUG) ""
            else if (selectedSide == "Sell") BuildConfig.DEFAULT_BTC_TESTNET_WIF
            else BuildConfig.DEV_PRIVATE_KEY.ifBlank { "" }
        ) 
    }
    var targetAddress by remember {
        mutableStateOf(if (BuildConfig.DEBUG) BuildConfig.DEFAULT_BTC_TESTNET_ADDRESS else "")
    }
    var addressType by remember { mutableStateOf(BtcAddressType.P2WPKH) }
    
    val isBuy = selectedSide == "Buy"
    val primaryColor = if (isBuy) DexGreen else DexRed
    
    val marketPrice = resolveLastPrice(uiState)
    val marketPriceText = remember(marketPrice) { marketPrice?.stripTrailingZeros()?.toPlainString().orEmpty() }
    var priceText by remember { mutableStateOf(marketPriceText) }
    LaunchedEffect(marketPriceText) {
        if (priceText.isBlank() && marketPriceText.isNotBlank()) {
            priceText = marketPriceText
        }
    }

    val price = priceText.toBigDecimalOrNull() ?: marketPrice ?: BigDecimal.ZERO
    val amountMaxDecimals = if (isBuy) 6 else 8
    val estimatedOutput = remember(amountText, price, isBuy) {
        if (price <= BigDecimal.ZERO) return@remember "0"
        val input = amountText.toBigDecimalOrNull() ?: BigDecimal.ZERO
        if (input <= BigDecimal.ZERO) return@remember "0"
        
        if (isBuy) {
            input.divide(price, 8, RoundingMode.HALF_UP).stripTrailingZeros().toPlainString() + " BTC"
        } else {
            input.multiply(price).setScale(2, RoundingMode.HALF_UP).stripTrailingZeros().toPlainString() + " USDC"
        }
    }
    val dexOrderAmountBase = remember(amountText, price, isBuy) {
        if (price <= BigDecimal.ZERO) return@remember BigDecimal.ZERO
        val input = amountText.toBigDecimalOrNull() ?: BigDecimal.ZERO
        if (input <= BigDecimal.ZERO) return@remember BigDecimal.ZERO
        if (isBuy) input.divide(price, 8, RoundingMode.HALF_UP) else input
    }

    ModalBottomSheet(
        onDismissRequest = onDismiss,
        containerColor = MaterialTheme.colorScheme.surface,
        dragHandle = {
            Box(
                modifier = Modifier
                    .padding(vertical = 12.dp)
                    .width(40.dp)
                    .height(4.dp)
                    .clip(RoundedCornerShape(2.dp))
                    .background(MaterialTheme.colorScheme.onSurfaceVariant.copy(alpha = 0.3f))
            )
        },
        shape = RoundedCornerShape(topStart = 24.dp, topEnd = 24.dp)
    ) {
        Column(
            modifier = Modifier
                .fillMaxWidth()
                .navigationBarsPadding()
                .imePadding()
                .verticalScroll(rememberScrollState())
                .padding(horizontal = 20.dp, vertical = 8.dp)
        ) {
            // 标题
            Text(
                text = "Trade BTC/USDC",
                style = MaterialTheme.typography.titleLarge,
                fontWeight = FontWeight.Bold
            )
            Spacer(Modifier.height(16.dp))
            
            // Buy/Sell 切换
            DexSegmentedControl(
                options = listOf("Buy", "Sell"),
                selectedIndex = if (isBuy) 0 else 1,
                onOptionSelected = { selectedSide = if (it == 0) "Buy" else "Sell" },
                modifier = Modifier.fillMaxWidth()
            )
            
            Spacer(Modifier.height(20.dp))
            
            // 输入金额
            DexGlassCard(
                modifier = Modifier.fillMaxWidth(),
                contentPadding = PaddingValues(16.dp)
            ) {
                Text(
                    if (isBuy) "Pay" else "Sell",
                    fontSize = 12.sp,
                    color = MaterialTheme.colorScheme.onSurfaceVariant
                )
                
                // Balance Display
                val balance = if (isBuy) {
                    uiState.multiChainWallets["BSC"]?.usdcBalance ?: BigDecimal.ZERO
                } else {
                    uiState.multiChainWallets["BTC"]?.balance ?: BigDecimal.ZERO
                }
                val symbol = if (isBuy) "USDC" else "BTC"
                
                Row(modifier = Modifier.fillMaxWidth(), horizontalArrangement = Arrangement.End) {
                    Text(
                        "Balance: ${formatDecimal(balance)} $symbol",
                        fontSize = 12.sp,
                        color = DexPrimary,
                        fontFamily = MonoFontFamily
                    )
                }
                
                Spacer(Modifier.height(4.dp))
                Row(verticalAlignment = Alignment.CenterVertically) {
                    OutlinedTextField(
                        value = amountText,
                        onValueChange = { amountText = sanitizeDecimalInput(it, maxDecimals = amountMaxDecimals) },
                        placeholder = { Text("0.00") },
                        modifier = Modifier.weight(1f),
                        textStyle = PriceTextStyle.copy(fontSize = 24.sp),
                        singleLine = true,
                        keyboardOptions = KeyboardOptions(
                            keyboardType = KeyboardType.Decimal,
                            imeAction = ImeAction.Done
                        ),
                        colors = OutlinedTextFieldDefaults.colors(
                            focusedBorderColor = Color.Transparent,
                            unfocusedBorderColor = Color.Transparent,
                            focusedTextColor = MaterialTheme.colorScheme.onSurface,
                            unfocusedTextColor = MaterialTheme.colorScheme.onSurface
                        )
                    )
                    ChainIcon(chainId = if (isBuy) "USDC" else "BTC", size = 32.dp)
                    Spacer(Modifier.width(8.dp))
                    Text(
                        if (isBuy) "USDC" else "BTC",
                        fontWeight = FontWeight.Bold,
                        fontSize = 16.sp
                    )
                }
            }

            Spacer(Modifier.height(12.dp))

            // Limit price (editable) - enables DEX order broadcast even when market data is unavailable
            DexGlassCard(
                modifier = Modifier.fillMaxWidth(),
                contentPadding = PaddingValues(16.dp)
            ) {
                Text(
                    "Limit price",
                    fontSize = 12.sp,
                    color = MaterialTheme.colorScheme.onSurfaceVariant
                )
                Spacer(Modifier.height(8.dp))
                Row(verticalAlignment = Alignment.CenterVertically) {
                    OutlinedTextField(
                        value = priceText,
                        onValueChange = { priceText = sanitizeDecimalInput(it, maxDecimals = 8) },
                        placeholder = { Text(if (marketPriceText.isNotBlank()) marketPriceText else "0.00") },
                        modifier = Modifier.weight(1f),
                        textStyle = PriceTextStyle.copy(fontSize = 20.sp),
                        singleLine = true,
                        keyboardOptions = KeyboardOptions(
                            keyboardType = KeyboardType.Decimal,
                            imeAction = ImeAction.Done
                        ),
                        colors = OutlinedTextFieldDefaults.colors(
                            focusedBorderColor = Color.Transparent,
                            unfocusedBorderColor = Color.Transparent,
                            focusedTextColor = MaterialTheme.colorScheme.onSurface,
                            unfocusedTextColor = MaterialTheme.colorScheme.onSurface
                        )
                    )
                    Spacer(Modifier.width(8.dp))
                    Text(
                        "USDC",
                        fontWeight = FontWeight.Bold,
                        fontSize = 14.sp
                    )
                }
                if (marketPriceText.isNotBlank() && marketPriceText != priceText) {
                    Spacer(Modifier.height(6.dp))
                    Text(
                        text = "Market: $marketPriceText",
                        fontSize = 11.sp,
                        color = MaterialTheme.colorScheme.onSurfaceVariant,
                        fontFamily = MonoFontFamily
                    )
                }
            }
            
            // 箭头
            Box(
                modifier = Modifier
                    .fillMaxWidth()
                    .padding(vertical = 8.dp),
                contentAlignment = Alignment.Center
            ) {
                Box(
                    modifier = Modifier
                        .size(36.dp)
                        .clip(CircleShape)
                        .background(MaterialTheme.colorScheme.surfaceVariant),
                        contentAlignment = Alignment.Center
                ) {
                    Icon(
                        Icons.Default.ArrowDownward,
                        contentDescription = null,
                        tint = primaryColor,
                        modifier = Modifier.size(20.dp)
                    )
                }
            }
            
            // 输出预估
            DexGlassCard(
                modifier = Modifier.fillMaxWidth(),
                contentPadding = PaddingValues(16.dp)
            ) {
                Text(
                    "Receive (estimated)",
                    fontSize = 12.sp,
                    color = MaterialTheme.colorScheme.onSurfaceVariant
                )
                Spacer(Modifier.height(8.dp))
                Row(verticalAlignment = Alignment.CenterVertically) {
                    Text(
                        estimatedOutput.replace(" BTC", "").replace(" USDC", ""),
                        style = PriceTextStyle.copy(fontSize = 24.sp),
                        color = MaterialTheme.colorScheme.onSurface
                    )
                    Spacer(Modifier.weight(1f))
                    ChainIcon(chainId = if (isBuy) "BTC" else "USDC", size = 32.dp)
                    Spacer(Modifier.width(8.dp))
                    Text(
                        if (isBuy) "BTC" else "USDC",
                        fontWeight = FontWeight.Bold,
                        fontSize = 16.sp
                    )
                }
            }
            
            Spacer(Modifier.height(16.dp))
            
            // Swap Type Selector
            var selectedSwapType by remember { mutableStateOf(UiIntent.SwapType.MPC) }
            
            Text(
                text = "Swap Protocol",
                color = MaterialTheme.colorScheme.onSurfaceVariant,
                fontSize = 13.sp,
                fontWeight = FontWeight.Medium
            )
            Spacer(Modifier.height(8.dp))
            Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                SwapTypeChip(
                    label = "Contract-less (MPC)",
                    selected = selectedSwapType == UiIntent.SwapType.MPC,
                    isRecommended = true
                ) { selectedSwapType = UiIntent.SwapType.MPC }
                
                SwapTypeChip(
                    label = "Privacy (Adapter)",
                    selected = selectedSwapType == UiIntent.SwapType.ADAPTER,
                    isRecommended = false
                ) { selectedSwapType = UiIntent.SwapType.ADAPTER }
            }
            Spacer(Modifier.height(16.dp))

            // 高级设置 (可折叠)
            var showAdvanced by remember { mutableStateOf(false) }
            Row(
                modifier = Modifier
                    .fillMaxWidth()
                    .clickable { showAdvanced = !showAdvanced }
                    .padding(vertical = 8.dp),
                horizontalArrangement = Arrangement.SpaceBetween,
                verticalAlignment = Alignment.CenterVertically
            ) {
                Text(
                    "Advanced Settings",
                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                    fontSize = 14.sp
                )
                Icon(
                    if (showAdvanced) Icons.Default.KeyboardArrowUp else Icons.Default.KeyboardArrowDown,
                    contentDescription = null,
                    tint = MaterialTheme.colorScheme.onSurfaceVariant
                )
            }
            
            AnimatedVisibility(visible = showAdvanced) {
                Column {
                    if (!isBuy) {
                        Text(
                            text = "BTC 地址类型",
                            color = MaterialTheme.colorScheme.onSurfaceVariant,
                            fontSize = 13.sp,
                            fontWeight = FontWeight.Medium
                        )
                        Spacer(Modifier.height(8.dp))
                        Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                            AddressTypeChip(
                                label = "P2WPKH (bech32)",
                                selected = addressType == BtcAddressType.P2WPKH
                            ) { addressType = BtcAddressType.P2WPKH }
                            AddressTypeChip(
                                label = "P2PKH (Legacy)",
                                selected = addressType == BtcAddressType.P2PKH
                            ) { addressType = BtcAddressType.P2PKH }
                        }
                        Spacer(Modifier.height(6.dp))
                        Text(
                            text = "SegWit 手续费更低，兼容老钱包可选 Legacy。",
                            fontSize = 11.sp,
                            color = MaterialTheme.colorScheme.onSurfaceVariant
                        )
                        Spacer(Modifier.height(12.dp))
                    }
                    OutlinedTextField(
                        value = privateKey,
                        onValueChange = { privateKey = it },
                        label = { Text(if (isBuy) "BSC Private Key (hex)" else "BTC Private Key") },
                        modifier = Modifier.fillMaxWidth(),
                        singleLine = true,
                        shape = RoundedCornerShape(12.dp)
                    )
                    Spacer(Modifier.height(12.dp))
                    OutlinedTextField(
                        value = targetAddress,
                        onValueChange = { targetAddress = it },
                        label = { Text(if (isBuy) "Receive BTC Address" else "Receive USDC Address") },
                        modifier = Modifier.fillMaxWidth(),
                        singleLine = true,
                        shape = RoundedCornerShape(12.dp)
                    )
                }
            }
            
            Spacer(Modifier.height(24.dp))

            // DEX 订单广播 (P2P) - Demo Path
            val dexOrderEnabled = uiState.running && price > BigDecimal.ZERO && dexOrderAmountBase > BigDecimal.ZERO
            if (isBuy) {
                DexBuyButton(
                    text = "Broadcast DEX Order (P2P)",
                    onClick = {
                        viewModel.onEvent(
                            UiIntent.SubmitDexOrder(
                                side = "buy",
                                price = price.stripTrailingZeros().toPlainString(),
                                amountBase = dexOrderAmountBase.stripTrailingZeros().toPlainString()
                            )
                        )
                        onDismiss()
                    },
                    enabled = dexOrderEnabled,
                    modifier = Modifier.fillMaxWidth().height(52.dp)
                )
            } else {
                DexSellButton(
                    text = "Broadcast DEX Order (P2P)",
                    onClick = {
                        viewModel.onEvent(
                            UiIntent.SubmitDexOrder(
                                side = "sell",
                                price = price.stripTrailingZeros().toPlainString(),
                                amountBase = dexOrderAmountBase.stripTrailingZeros().toPlainString()
                            )
                        )
                        onDismiss()
                    },
                    enabled = dexOrderEnabled,
                    modifier = Modifier.fillMaxWidth().height(52.dp)
                )
            }
            
            // 确认按钮
            if (isBuy) {
                    DexBuyButton(
                        text = "Confirm Buy (${if (selectedSwapType == UiIntent.SwapType.MPC) "MPC" else "Privacy"})",
                        onClick = {
                            viewModel.onEvent(UiIntent.SubmitBridgeTrade(
                                isBuy = true,
                                amount = amountText,
                                privateKey = privateKey,
                                targetAddress = targetAddress,
                                addressType = addressType,
                                swapType = selectedSwapType
                            ))
                            onDismiss()
                        },
                        modifier = Modifier.fillMaxWidth().height(52.dp)
                    )
            } else {
                    DexSellButton(
                        text = "Confirm Sell (${if (selectedSwapType == UiIntent.SwapType.MPC) "MPC" else "Privacy"})",
                        onClick = {
                            viewModel.onEvent(UiIntent.SubmitBridgeTrade(
                                isBuy = false,
                                amount = amountText,
                                privateKey = privateKey,
                                targetAddress = targetAddress,
                                addressType = addressType,
                                swapType = selectedSwapType
                            ))
                            onDismiss()
                        },
                        modifier = Modifier.fillMaxWidth().height(52.dp)
                    )
            }
            
            Spacer(Modifier.height(32.dp))
        }
    }
}

@Composable
private fun AddressTypeChip(
    label: String,
    selected: Boolean,
    onClick: () -> Unit
) {
    AssistChip(
        onClick = onClick,
        label = { Text(label, fontSize = 12.sp) },
        leadingIcon = {
            if (selected) {
                Icon(Icons.Filled.ArrowDownward, contentDescription = null, modifier = Modifier.size(14.dp))
            }
        },
        colors = AssistChipDefaults.assistChipColors(
            containerColor = if (selected) DexPrimary.copy(alpha = 0.12f) else MaterialTheme.colorScheme.surfaceVariant,
            labelColor = if (selected) DexPrimary else MaterialTheme.colorScheme.onSurfaceVariant
        )
    )
}

@Composable
fun TransactionHistory(swaps: List<com.example.libp2psmoke.dex.DexSwapResult>) {
    val context = androidx.compose.ui.platform.LocalContext.current
    
    DexCard(
        modifier = Modifier
            .fillMaxWidth()
            .padding(horizontal = 8.dp, vertical = 4.dp),
        shape = RoundedCornerShape(12.dp),
        containerColor = MaterialTheme.colorScheme.surface,
        contentPadding = PaddingValues(12.dp)
    ) {
        Column {
            Text(
                "Transaction History",
                style = MaterialTheme.typography.titleSmall,
                color = MaterialTheme.colorScheme.onSurface,
                fontWeight = FontWeight.Bold
            )
            Spacer(Modifier.height(8.dp))
            
            swaps.take(5).forEach { swap ->
                val chainLabel = when (swap.chain) {
                    com.example.libp2psmoke.dex.DexChain.BTC_TESTNET -> "BTC Testnet"
                    com.example.libp2psmoke.dex.DexChain.BSC_TESTNET -> "BSC Testnet"
                }
                val explorerUrl = swap.txHash?.let { hash ->
                    when (swap.chain) {
                        com.example.libp2psmoke.dex.DexChain.BTC_TESTNET -> "https://mempool.space/testnet/tx/$hash"
                        com.example.libp2psmoke.dex.DexChain.BSC_TESTNET -> "https://testnet.bscscan.com/tx/$hash"
                    }
                }
                Row(
                    modifier = Modifier
                        .fillMaxWidth()
                        .padding(vertical = 4.dp)
                        .clickable(enabled = explorerUrl != null) {
                            explorerUrl?.let { url ->
                                val intent = android.content.Intent(android.content.Intent.ACTION_VIEW, android.net.Uri.parse(url))
                                context.startActivity(intent)
                            }
                        },
                    verticalAlignment = Alignment.CenterVertically
                ) {
                    // Chain Icon
                    ChainIcon(
                        chainId = if (swap.chain == com.example.libp2psmoke.dex.DexChain.BTC_TESTNET) "BTC" else "BSC",
                        size = 20.dp
                    )
                    Spacer(Modifier.width(8.dp))
                    
                    Column(modifier = Modifier.weight(1f)) {
                        Text(
                            swap.amountDisplay,
                            fontSize = 12.sp,
                            fontWeight = FontWeight.Medium,
                            color = MaterialTheme.colorScheme.onSurface
                        )
                        Text(
                            swap.message ?: swap.status.name,
                            fontSize = 10.sp,
                            color = MaterialTheme.colorScheme.onSurfaceVariant
                        )
                        if (swap.txHash != null) {
                            Spacer(Modifier.height(2.dp))
                            Text(
                                "$chainLabel: ${swap.txHash.take(8)}...${swap.txHash.takeLast(6)}",
                                fontSize = 10.sp,
                                color = DexPrimary
                            )
                        }
                    }
                    
                    // Status & Link Icon
                    Row(verticalAlignment = Alignment.CenterVertically) {
                        Text(
                            swap.status.name,
                            fontSize = 10.sp,
                            color = when (swap.status) {
                                com.example.libp2psmoke.dex.DexSwapStatus.Confirmed -> DexGreen
                                com.example.libp2psmoke.dex.DexSwapStatus.Failed -> DexRed
                                else -> MaterialTheme.colorScheme.primary
                            }
                        )
                        if (swap.txHash != null) {
                            Spacer(Modifier.width(4.dp))
                            Icon(
                                androidx.compose.material.icons.Icons.Default.ArrowDownward, // fallback link icon
                                contentDescription = "Explorer",
                                modifier = Modifier.size(14.dp).rotate(-45f),
                                tint = DexPrimary
                            )
                        }
                    }
                }
                HorizontalDivider(color = MaterialTheme.colorScheme.surfaceVariant.copy(alpha = 0.5f))
            }

            

        }
    }
}

private fun resolveLastPrice(uiState: NodeUiState): BigDecimal? {
    val fromTicker =
        uiState.binanceTicker
            ?.lastPrice
            ?.takeIf { it > BigDecimal.ZERO }
    if (fromTicker != null) return fromTicker

    val fromMarketKline =
        uiState.binanceKlines
            .firstOrNull()
            ?.close
            ?.takeIf { it > BigDecimal.ZERO }
    if (fromMarketKline != null) return fromMarketKline

    return uiState.dexKlines
        .firstOrNull()
        ?.close
        ?.takeIf { it > BigDecimal.ZERO }
}

@Composable
private fun SwapTypeChip(
    label: String,
    selected: Boolean,
    isRecommended: Boolean,
    onClick: () -> Unit
) {
    FilterChip(
        selected = selected,
        onClick = onClick,
        label = { 
            Row(verticalAlignment = Alignment.CenterVertically) {
                Text(label, fontSize = 12.sp)
                if (isRecommended) {
                    Spacer(Modifier.width(4.dp))
                    Text(
                        "RECOMMENDED",
                        fontSize = 8.sp,
                        color = MaterialTheme.colorScheme.primary,
                        fontWeight = FontWeight.Bold
                    )
                }
            }
        },
        leadingIcon = {
            if (selected) {
                Icon(
                    Icons.Filled.Check,
                    contentDescription = null,
                    modifier = Modifier.size(14.dp)
                )
            }
        },
        colors = FilterChipDefaults.filterChipColors(
            selectedContainerColor = DexPrimary.copy(alpha = 0.12f),
            selectedLabelColor = DexPrimary
        ),
        shape = RoundedCornerShape(8.dp)
    )
}
