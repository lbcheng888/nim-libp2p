package com.example.libp2psmoke.ui.screens

import androidx.compose.foundation.background
import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.shape.CircleShape
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.foundation.verticalScroll
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.filled.CallMade
import androidx.compose.material.icons.filled.CallReceived
import androidx.compose.material.icons.filled.History
import androidx.compose.material.icons.filled.SwapHoriz
import androidx.compose.material.icons.filled.TrendingUp
import androidx.compose.material3.*
import androidx.compose.runtime.Composable
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.draw.clip
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.graphics.vector.ImageVector
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import com.example.libp2psmoke.model.ChainWallet
import com.example.libp2psmoke.model.NodeUiState
import com.example.libp2psmoke.ui.components.*
import com.example.libp2psmoke.ui.theme.*
import com.example.libp2psmoke.ui.utils.formatDecimal
import java.math.BigDecimal

@Composable
fun WalletScreen(uiState: NodeUiState) {
    val wallets = uiState.multiChainWallets.values.toList()
    val totalUsd = wallets.fold(BigDecimal.ZERO) { acc, w -> acc.add(w.usdcBalance) }
    val totalBtc = wallets.firstOrNull { it.chainId == "BTC" }?.balance ?: BigDecimal.ZERO
    Column(
        modifier = Modifier
            .fillMaxSize()
            .background(MaterialTheme.colorScheme.background)
            .verticalScroll(rememberScrollState())
    ) {
        // 顶部余额卡片
        DexGlassCard(
            modifier = Modifier
                .fillMaxWidth()
                .padding(16.dp),
            shape = RoundedCornerShape(20.dp),
            contentPadding = PaddingValues(24.dp)
        ) {
            Text(
                "Total Balance",
                color = MaterialTheme.colorScheme.onSurfaceVariant,
                fontSize = 14.sp
            )
            Spacer(Modifier.height(8.dp))
            Text(
                "$${formatDecimal(totalUsd)}",
                style = BalanceStyle,
                color = MaterialTheme.colorScheme.onSurface
            )
            Spacer(Modifier.height(4.dp))
            Row(verticalAlignment = Alignment.CenterVertically) {
                Icon(
                    Icons.Default.TrendingUp,
                    contentDescription = null,
                    tint = DexGreen,
                    modifier = Modifier.size(16.dp)
                )
                Spacer(Modifier.width(4.dp))
                Text(
                    "测试网余额 (USDC) | BTC: ${formatDecimal(totalBtc)}",
                    color = DexGreen,
                    fontSize = 14.sp,
                    fontWeight = FontWeight.Medium
                )
                Text(
                    "",
                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                    fontSize = 14.sp
                )
            }
        }
        
        // 快捷操作
        Row(
            modifier = Modifier
                .fillMaxWidth()
                .padding(horizontal = 16.dp),
            horizontalArrangement = Arrangement.SpaceEvenly
        ) {
            WalletActionButton(
                icon = Icons.Filled.CallMade,
                label = "Deposit",
                color = DexGreen
            )
            WalletActionButton(
                icon = Icons.Filled.CallReceived,
                label = "Withdraw",
                color = DexPrimary
            )
            WalletActionButton(
                icon = Icons.Filled.SwapHoriz,
                label = "Transfer",
                color = DexAccent
            )
            WalletActionButton(
                icon = Icons.Filled.History,
                label = "History",
                color = DexOrange
            )
        }
        
        Spacer(Modifier.height(24.dp))
        
        // 资产列表标题
        Row(
            modifier = Modifier
                .fillMaxWidth()
                .padding(horizontal = 16.dp),
            horizontalArrangement = Arrangement.SpaceBetween,
            verticalAlignment = Alignment.CenterVertically
        ) {
            Text(
                "Assets",
                style = MaterialTheme.typography.titleMedium,
                fontWeight = FontWeight.Bold
            )
            TextButton(onClick = { /* 查看全部 */ }) {
                Text("See All", color = DexPrimary)
            }
        }
        
        Spacer(Modifier.height(8.dp))
        
        // 资产列表
        wallets.ifEmpty { listOf(
            ChainWallet("BTC", "tb1...", BigDecimal("0.00"), BigDecimal.ZERO, "BTC"),
            ChainWallet("BSC", "0x...", BigDecimal.ZERO, BigDecimal("0.00"), "USDC")
        ) }.forEach { wallet ->
            AssetRow(wallet)
        }
        
        Spacer(Modifier.height(24.dp))
    }
}

@Composable
private fun WalletActionButton(
    icon: ImageVector,
    label: String,
    color: Color
) {
    Column(
        horizontalAlignment = Alignment.CenterHorizontally,
        modifier = Modifier
            .clip(RoundedCornerShape(12.dp))
            .clickable { /* Action */ }
            .padding(12.dp)
    ) {
        Box(
            modifier = Modifier
                .size(52.dp)
                .clip(CircleShape)
                .background(color.copy(alpha = 0.12f)),
            contentAlignment = Alignment.Center
        ) {
            Icon(
                icon,
                contentDescription = null,
                tint = color,
                modifier = Modifier.size(24.dp)
            )
        }
        Spacer(Modifier.height(8.dp))
        Text(
            label,
            color = MaterialTheme.colorScheme.onSurfaceVariant,
            fontSize = 12.sp,
            fontWeight = FontWeight.Medium
        )
    }
}

@Composable
fun AssetRow(wallet: ChainWallet) {
    Row(
        modifier = Modifier
            .fillMaxWidth()
            .padding(horizontal = 16.dp, vertical = 6.dp)
            .clip(RoundedCornerShape(12.dp))
            .background(MaterialTheme.colorScheme.surface)
            .clickable { /* 资产详情 */ }
            .padding(16.dp),
        verticalAlignment = Alignment.CenterVertically
    ) {
        ChainIcon(chainId = wallet.chainId, size = 40.dp)
        
        Spacer(Modifier.width(12.dp))
        
        Column(modifier = Modifier.weight(1f)) {
            Text(
                wallet.symbol,
                fontWeight = FontWeight.Bold,
                fontSize = 16.sp,
                color = MaterialTheme.colorScheme.onSurface
            )
            Text(
                wallet.chainId,
                fontSize = 13.sp,
                color = MaterialTheme.colorScheme.onSurfaceVariant
            )
        }
        
        Column(horizontalAlignment = Alignment.End) {
            Text(
                formatDecimal(wallet.balance),
                fontWeight = FontWeight.Bold,
                fontSize = 16.sp,
                fontFamily = MonoFontFamily,
                color = MaterialTheme.colorScheme.onSurface
            )
            if (wallet.usdcBalance > BigDecimal.ZERO) {
                Text(
                    "$${formatDecimal(wallet.usdcBalance)}",
                    fontSize = 13.sp,
                    fontFamily = MonoFontFamily,
                    color = MaterialTheme.colorScheme.onSurfaceVariant
                )
            }
        }
    }
}
