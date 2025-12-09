package com.example.libp2psmoke.mixer

import androidx.compose.animation.*
import androidx.compose.animation.core.*
import androidx.compose.foundation.background
import androidx.compose.foundation.border
import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.shape.CircleShape
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.foundation.text.KeyboardOptions
import androidx.compose.foundation.verticalScroll
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.filled.*
import androidx.compose.material.icons.outlined.*
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.runtime.saveable.rememberSaveable
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.draw.clip
import androidx.compose.ui.graphics.Brush
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.text.input.KeyboardType
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import com.example.libp2psmoke.ui.UiIntent
import com.example.libp2psmoke.ui.components.*
import com.example.libp2psmoke.ui.theme.*
import com.example.libp2psmoke.viewmodel.NimNodeViewModel
import kotlinx.coroutines.launch

@Composable
fun MixerTab(
    viewModel: NimNodeViewModel,
    snackbarHostState: SnackbarHostState
) {
    var asset by rememberSaveable { mutableStateOf("BTC") }
    var amount by rememberSaveable { mutableStateOf("1.0") }
    var hopCount by rememberSaveable { mutableStateOf("3") }
    val coroutineScope = rememberCoroutineScope()
    
    val sessions by viewModel.mixerSessions.collectAsState()
    
    val notify: (String) -> Unit = { message ->
        coroutineScope.launch {
            snackbarHostState.showSnackbar(message)
        }
    }

    Column(
        modifier = Modifier
            .fillMaxSize()
            .background(MaterialTheme.colorScheme.background)
            .verticalScroll(rememberScrollState())
    ) {
        // ═══════════════════════════════════════════════════════════════════
        // 顶部渐变区域
        // ═══════════════════════════════════════════════════════════════════
        Box(
            modifier = Modifier
                .fillMaxWidth()
                .background(
                    Brush.verticalGradient(
                        colors = listOf(
                            DexAccent.copy(alpha = 0.15f),
                            Color.Transparent
                        )
                    )
                )
                .padding(20.dp)
        ) {
            Column {
                Row(verticalAlignment = Alignment.CenterVertically) {
                    Box(
                        modifier = Modifier
                            .size(48.dp)
                            .clip(CircleShape)
                            .background(
                                Brush.horizontalGradient(
                                    colors = listOf(DexAccent, DexAccentSecondary)
                                )
                            ),
                        contentAlignment = Alignment.Center
                    ) {
                        Icon(
                            Icons.Filled.Security,
                            contentDescription = null,
                            tint = Color.White,
                            modifier = Modifier.size(24.dp)
                        )
                    }
                    Spacer(Modifier.width(16.dp))
                    Column {
                        Text(
                            "CoinJoin Privacy Mixer",
                            style = MaterialTheme.typography.headlineSmall,
                            fontWeight = FontWeight.Bold
                        )
                        Text(
                            "Onion Routing & Chaum-Pedersen Signatures",
                            style = MaterialTheme.typography.bodySmall,
                            color = MaterialTheme.colorScheme.onSurfaceVariant
                        )
                    }
                }
                
                Spacer(Modifier.height(16.dp))
                
                // 安全提示横幅
                Row(
                    modifier = Modifier
                        .fillMaxWidth()
                        .clip(RoundedCornerShape(12.dp))
                        .background(DexAccent.copy(alpha = 0.1f))
                        .border(
                            width = 1.dp,
                            color = DexAccent.copy(alpha = 0.2f),
                            shape = RoundedCornerShape(12.dp)
                        )
                        .padding(12.dp),
                    verticalAlignment = Alignment.CenterVertically
                ) {
                    Icon(
                        Icons.Outlined.Shield,
                        contentDescription = null,
                        tint = DexAccent,
                        modifier = Modifier.size(20.dp)
                    )
                    Spacer(Modifier.width(12.dp))
                    Text(
                        "Your transactions are routed through multiple nodes for maximum privacy",
                        style = MaterialTheme.typography.bodySmall,
                        color = MaterialTheme.colorScheme.onSurface
                    )
                }
            }
        }
        
        // ═══════════════════════════════════════════════════════════════════
        // 新建Mix请求卡片
        // ═══════════════════════════════════════════════════════════════════
        DexGlassCard(
            modifier = Modifier
                .fillMaxWidth()
                .padding(horizontal = 16.dp),
            shape = RoundedCornerShape(20.dp),
            contentPadding = PaddingValues(20.dp)
        ) {
            Row(
                modifier = Modifier.fillMaxWidth(),
                horizontalArrangement = Arrangement.SpaceBetween,
                verticalAlignment = Alignment.CenterVertically
            ) {
                Text(
                    "New Mix Request",
                    style = MaterialTheme.typography.titleMedium,
                    fontWeight = FontWeight.Bold
                )
                // 预设按钮
                OutlinedButton(
                    onClick = {
                        asset = "BTC"
                        amount = "0.5"
                        hopCount = "3"
                    },
                    shape = RoundedCornerShape(8.dp),
                    contentPadding = PaddingValues(horizontal = 12.dp, vertical = 6.dp)
                ) {
                    Icon(
                        Icons.Default.AutoAwesome,
                        contentDescription = null,
                        modifier = Modifier.size(16.dp)
                    )
                    Spacer(Modifier.width(4.dp))
                    Text("Auto-Fill", fontSize = 12.sp)
                }
            }
            
            Spacer(Modifier.height(20.dp))
            
            // 资产选择
            Text(
                "Select Asset",
                style = MaterialTheme.typography.labelMedium,
                color = MaterialTheme.colorScheme.onSurfaceVariant
            )
            Spacer(Modifier.height(8.dp))
            
            Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                listOf("BTC", "ETH", "SOL").forEach { assetOption ->
                    val isSelected = asset == assetOption
                    Box(
                        modifier = Modifier
                            .clip(RoundedCornerShape(12.dp))
                            .background(
                                if (isSelected) DexAccent.copy(alpha = 0.15f)
                                else MaterialTheme.colorScheme.surfaceVariant
                            )
                            .border(
                                width = if (isSelected) 2.dp else 1.dp,
                                color = if (isSelected) DexAccent else Color.Transparent,
                                shape = RoundedCornerShape(12.dp)
                            )
                            .clickable { asset = assetOption }
                            .padding(horizontal = 16.dp, vertical = 12.dp)
                    ) {
                        Row(verticalAlignment = Alignment.CenterVertically) {
                            ChainIcon(assetOption, size = 24.dp)
                            Spacer(Modifier.width(8.dp))
                            Text(
                                assetOption,
                                fontWeight = if (isSelected) FontWeight.Bold else FontWeight.Medium
                            )
                        }
                    }
                }
            }
            
            Spacer(Modifier.height(20.dp))
            
            // 金额输入
            Text(
                "Amount",
                style = MaterialTheme.typography.labelMedium,
                color = MaterialTheme.colorScheme.onSurfaceVariant
            )
            Spacer(Modifier.height(8.dp))
            
            OutlinedTextField(
                value = amount,
                onValueChange = { amount = it },
                placeholder = { Text("0.00") },
                keyboardOptions = KeyboardOptions(keyboardType = KeyboardType.Decimal),
                modifier = Modifier.fillMaxWidth(),
                shape = RoundedCornerShape(12.dp),
                trailingIcon = {
                    Text(
                        asset,
                        fontWeight = FontWeight.Bold,
                        color = MaterialTheme.colorScheme.onSurfaceVariant,
                        modifier = Modifier.padding(end = 12.dp)
                    )
                },
                colors = OutlinedTextFieldDefaults.colors(
                    focusedBorderColor = DexAccent,
                    unfocusedBorderColor = MaterialTheme.colorScheme.outline
                )
            )
            
            Spacer(Modifier.height(20.dp))
            
            // Onion Hops 滑块
            Text(
                "Privacy Level (Onion Hops)",
                style = MaterialTheme.typography.labelMedium,
                color = MaterialTheme.colorScheme.onSurfaceVariant
            )
            Spacer(Modifier.height(8.dp))
            
            Row(
                modifier = Modifier.fillMaxWidth(),
                verticalAlignment = Alignment.CenterVertically
            ) {
                val hops = hopCount.toIntOrNull() ?: 3
                Slider(
                    value = hops.toFloat(),
                    onValueChange = { hopCount = it.toInt().toString() },
                    valueRange = 1f..5f,
                    steps = 3,
                    modifier = Modifier.weight(1f),
                    colors = SliderDefaults.colors(
                        thumbColor = DexAccent,
                        activeTrackColor = DexAccent,
                        inactiveTrackColor = MaterialTheme.colorScheme.surfaceVariant
                    )
                )
                Spacer(Modifier.width(16.dp))
                Box(
                    modifier = Modifier
                        .size(48.dp)
                        .clip(RoundedCornerShape(8.dp))
                        .background(DexAccent.copy(alpha = 0.15f)),
                    contentAlignment = Alignment.Center
                ) {
                    Text(
                        hopCount,
                        fontWeight = FontWeight.Bold,
                        fontSize = 18.sp,
                        color = DexAccent
                    )
                }
            }
            
            // 隐私级别说明
            val privacyLevel = when (hopCount.toIntOrNull() ?: 3) {
                1 -> "Basic" to "Lower fees, faster confirmation"
                2 -> "Standard" to "Balanced privacy and speed"
                3 -> "Enhanced" to "Recommended for most users"
                4 -> "High" to "Maximum privacy, higher fees"
                5 -> "Extreme" to "Maximum anonymity guarantee"
                else -> "Enhanced" to "Recommended"
            }
            
            Spacer(Modifier.height(8.dp))
            Row(
                modifier = Modifier
                    .fillMaxWidth()
                    .clip(RoundedCornerShape(8.dp))
                    .background(MaterialTheme.colorScheme.surfaceVariant.copy(alpha = 0.5f))
                    .padding(12.dp),
                horizontalArrangement = Arrangement.SpaceBetween
            ) {
                Text(
                    "Privacy: ${privacyLevel.first}",
                    fontWeight = FontWeight.Medium,
                    fontSize = 13.sp
                )
                Text(
                    privacyLevel.second,
                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                    fontSize = 12.sp
                )
            }
            
            Spacer(Modifier.height(24.dp))
            
            // Mix 按钮
            Button(
                onClick = {
                    viewModel.onEvent(UiIntent.SubmitMixerIntent(asset, amount, hopCount))
                },
                modifier = Modifier
                    .fillMaxWidth()
                    .height(52.dp),
                shape = RoundedCornerShape(14.dp),
                colors = ButtonDefaults.buttonColors(
                    containerColor = DexAccent
                )
            ) {
                Icon(
                    Icons.Default.Shuffle,
                    contentDescription = null,
                    modifier = Modifier.size(20.dp)
                )
                Spacer(Modifier.width(8.dp))
                Text(
                    "Start Private Mix",
                    fontWeight = FontWeight.Bold,
                    fontSize = 16.sp
                )
            }
        }
        
        Spacer(Modifier.height(24.dp))
        
        // ═══════════════════════════════════════════════════════════════════
        // 活跃会话列表
        // ═══════════════════════════════════════════════════════════════════
        Row(
            modifier = Modifier
                .fillMaxWidth()
                .padding(horizontal = 16.dp),
            horizontalArrangement = Arrangement.SpaceBetween,
            verticalAlignment = Alignment.CenterVertically
        ) {
            Text(
                "Active Sessions",
                style = MaterialTheme.typography.titleMedium,
                fontWeight = FontWeight.Bold
            )
            if (sessions.isNotEmpty()) {
                Text(
                    "${sessions.size} active",
                    color = DexAccent,
                    fontSize = 14.sp,
                    fontWeight = FontWeight.Medium
                )
            }
        }
        
        Spacer(Modifier.height(12.dp))
        
        if (sessions.isEmpty()) {
            // 空状态
            Box(
                modifier = Modifier
                    .fillMaxWidth()
                    .padding(horizontal = 16.dp)
            ) {
                DexEmptyState(
                    icon = Icons.Outlined.Shield,
                    title = "No Active Mix Sessions",
                    subtitle = "Start a new mix to enhance your privacy"
                )
            }
        } else {
            Column(
                modifier = Modifier.padding(horizontal = 16.dp),
                verticalArrangement = Arrangement.spacedBy(12.dp)
            ) {
                sessions.forEach { session ->
                    MixerSessionCard(session)
                }
            }
        }
        
        Spacer(Modifier.height(24.dp))
        
        // ═══════════════════════════════════════════════════════════════════
        // 底部说明
        // ═══════════════════════════════════════════════════════════════════
        DexCard(
            modifier = Modifier
                .fillMaxWidth()
                .padding(horizontal = 16.dp),
            containerColor = MaterialTheme.colorScheme.surfaceVariant.copy(alpha = 0.5f),
            contentPadding = PaddingValues(16.dp)
        ) {
            Row(verticalAlignment = Alignment.Top) {
                Icon(
                    Icons.Outlined.Info,
                    contentDescription = null,
                    tint = MaterialTheme.colorScheme.onSurfaceVariant,
                    modifier = Modifier.size(20.dp)
                )
                Spacer(Modifier.width(12.dp))
                Column {
                    Text(
                        "How CoinJoin Works",
                        fontWeight = FontWeight.Medium,
                        fontSize = 14.sp
                    )
                    Spacer(Modifier.height(4.dp))
                    Text(
                        "Multiple participants combine their transactions into a single joint transaction, making it impossible to trace individual funds. Your privacy is protected by cryptographic blind signatures.",
                        color = MaterialTheme.colorScheme.onSurfaceVariant,
                        fontSize = 12.sp,
                        lineHeight = 18.sp
                    )
                }
            }
        }
        
        Spacer(Modifier.height(32.dp))
    }
}

@Composable
fun MixerSessionCard(session: MixerSessionStatus) {
    val (statusColor, statusIcon) = when(session.state) {
        "Completed" -> DexGreen to Icons.Default.CheckCircle
        "Discovery (Onion)" -> DexOrange to Icons.Default.Explore
        "Mixing" -> DexAccent to Icons.Default.Shuffle
        "Signing" -> DexPrimary to Icons.Default.Draw
        else -> MaterialTheme.colorScheme.primary to Icons.Default.Pending
    }
    
    // 进度动画
    val infiniteTransition = rememberInfiniteTransition(label = "progress")
    val progressAlpha by infiniteTransition.animateFloat(
        initialValue = 0.3f,
        targetValue = 1f,
        animationSpec = infiniteRepeatable(
            animation = tween(1000),
            repeatMode = RepeatMode.Reverse
        ),
        label = "progressAlpha"
    )
    
    DexCard(
        modifier = Modifier.fillMaxWidth(),
        shape = RoundedCornerShape(16.dp),
        contentPadding = PaddingValues(16.dp)
    ) {
        Row(
            modifier = Modifier.fillMaxWidth(),
            verticalAlignment = Alignment.CenterVertically
        ) {
            // 状态图标
            Box(
                modifier = Modifier
                    .size(44.dp)
                    .clip(CircleShape)
                    .background(statusColor.copy(alpha = 0.12f)),
                contentAlignment = Alignment.Center
            ) {
                Icon(
                    statusIcon,
                    contentDescription = null,
                    tint = statusColor,
                    modifier = Modifier.size(24.dp)
                )
            }
            
            Spacer(Modifier.width(12.dp))
            
            Column(modifier = Modifier.weight(1f)) {
                Row(
                    modifier = Modifier.fillMaxWidth(),
                    horizontalArrangement = Arrangement.SpaceBetween,
                    verticalAlignment = Alignment.CenterVertically
                ) {
                    Text(
                        "Session ${session.sessionId.take(8)}...",
                        fontWeight = FontWeight.Bold,
                        style = MaterialTheme.typography.bodyLarge
                    )
                    Box(
                        modifier = Modifier
                            .clip(RoundedCornerShape(6.dp))
                            .background(statusColor.copy(alpha = if (session.state != "Completed") progressAlpha * 0.2f else 0.15f))
                            .padding(horizontal = 10.dp, vertical = 4.dp)
                    ) {
                        Text(
                            session.state,
                            color = statusColor,
                            style = MaterialTheme.typography.labelMedium,
                            fontWeight = FontWeight.Bold
                        )
                    }
                }
                
                Spacer(Modifier.height(4.dp))
                
                Row(verticalAlignment = Alignment.CenterVertically) {
                    Icon(
                        Icons.Outlined.People,
                        contentDescription = null,
                        modifier = Modifier.size(14.dp),
                        tint = MaterialTheme.colorScheme.onSurfaceVariant
                    )
                    Spacer(Modifier.width(4.dp))
                    Text(
                        "${session.participantCount} participants",
                        style = MaterialTheme.typography.bodySmall,
                        color = MaterialTheme.colorScheme.onSurfaceVariant
                    )
                }
            }
        }
        
        if (session.txId != null) {
            Spacer(Modifier.height(12.dp))
            Divider(color = MaterialTheme.colorScheme.outline.copy(alpha = 0.1f))
            Spacer(Modifier.height(12.dp))
            
            Row(
                modifier = Modifier
                    .fillMaxWidth()
                    .clip(RoundedCornerShape(8.dp))
                    .background(MaterialTheme.colorScheme.surfaceVariant.copy(alpha = 0.5f))
                    .padding(12.dp),
                verticalAlignment = Alignment.CenterVertically
            ) {
                Icon(
                    Icons.Outlined.Receipt,
                    contentDescription = null,
                    modifier = Modifier.size(16.dp),
                    tint = DexGreen
                )
                Spacer(Modifier.width(8.dp))
                Column {
                    Text(
                        "Transaction ID",
                        style = MaterialTheme.typography.labelSmall,
                        color = MaterialTheme.colorScheme.onSurfaceVariant
                    )
                    Text(
                        session.txId,
                        style = AddressStyle,
                        color = MaterialTheme.colorScheme.onSurface,
                        maxLines = 1
                    )
                }
                Spacer(Modifier.weight(1f))
                IconButton(
                    onClick = { /* 复制 */ },
                    modifier = Modifier.size(32.dp)
                ) {
                    Icon(
                        Icons.Outlined.ContentCopy,
                        contentDescription = "Copy",
                        modifier = Modifier.size(18.dp),
                        tint = MaterialTheme.colorScheme.onSurfaceVariant
                    )
                }
            }
        }
    }
}
