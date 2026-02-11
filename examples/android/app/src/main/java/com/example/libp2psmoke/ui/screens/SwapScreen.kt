package com.example.libp2psmoke.ui.screens

import androidx.compose.foundation.background
import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.shape.CircleShape
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.filled.KeyboardArrowDown
import androidx.compose.material.icons.filled.SwapVert
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.draw.clip
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import com.example.libp2psmoke.model.NodeUiState
import com.example.libp2psmoke.ui.UiIntent
import com.example.libp2psmoke.ui.components.*
import com.example.libp2psmoke.ui.theme.*
import com.example.libp2psmoke.viewmodel.NimNodeViewModel

@Composable
fun SwapScreen(uiState: NodeUiState, viewModel: NimNodeViewModel) {
    var payAmount by remember { mutableStateOf("") }
    var receiveAmount by remember { mutableStateOf("0") }
    
    Column(
        modifier = Modifier
            .fillMaxSize()
            .background(MaterialTheme.colorScheme.background)
            .padding(16.dp)
    ) {
        Text(
            "Cross-Chain Swap",
            style = MaterialTheme.typography.headlineMedium,
            fontWeight = FontWeight.Bold
        )
        Text(
            "Swap assets across different blockchains",
            style = MaterialTheme.typography.bodyMedium,
            color = MaterialTheme.colorScheme.onSurfaceVariant
        )
        
        Spacer(Modifier.height(32.dp))
        
        // Pay 卡片
        DexGlassCard(
            modifier = Modifier.fillMaxWidth(),
            shape = RoundedCornerShape(16.dp),
            contentPadding = PaddingValues(20.dp)
        ) {
            Row(
                modifier = Modifier.fillMaxWidth(),
                horizontalArrangement = Arrangement.SpaceBetween
            ) {
                Text("Pay", color = MaterialTheme.colorScheme.onSurfaceVariant)
                Text(
                    "Balance: 0.5 BTC",
                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                    fontSize = 12.sp
                )
            }
            Spacer(Modifier.height(12.dp))
            Row(verticalAlignment = Alignment.CenterVertically) {
                Row(
                    modifier = Modifier
                        .clip(RoundedCornerShape(20.dp))
                        .background(MaterialTheme.colorScheme.surfaceVariant)
                        .padding(horizontal = 12.dp, vertical = 8.dp),
                    verticalAlignment = Alignment.CenterVertically
                ) {
                    ChainIcon("BTC", 24.dp)
                    Spacer(Modifier.width(8.dp))
                    Text("BTC", fontWeight = FontWeight.Bold)
                    Icon(
                        Icons.Default.KeyboardArrowDown,
                        contentDescription = null,
                        modifier = Modifier.size(20.dp)
                    )
                }
                Spacer(Modifier.weight(1f))
                OutlinedTextField(
                    value = payAmount,
                    onValueChange = { payAmount = it },
                    placeholder = { Text("0.00", style = PriceTextStyle.copy(fontSize = 28.sp)) },
                    textStyle = PriceTextStyle.copy(fontSize = 28.sp),
                    singleLine = true,
                    modifier = Modifier.width(160.dp),
                    colors = OutlinedTextFieldDefaults.colors(
                        focusedBorderColor = Color.Transparent,
                        unfocusedBorderColor = Color.Transparent
                    )
                )
            }
        }
        
        // 交换按钮
        Box(
            modifier = Modifier
                .fillMaxWidth()
                .padding(vertical = 4.dp),
            contentAlignment = Alignment.Center
        ) {
            Box(
                modifier = Modifier
                    .size(44.dp)
                    .clip(CircleShape)
                    .background(DexPrimary)
                    .clickable { /* 交换 */ },
                contentAlignment = Alignment.Center
            ) {
                Icon(
                    Icons.Default.SwapVert,
                    contentDescription = "Swap",
                    tint = Color.White,
                    modifier = Modifier.size(24.dp)
                )
            }
        }
        
        // Receive 卡片
        DexGlassCard(
            modifier = Modifier.fillMaxWidth(),
            shape = RoundedCornerShape(16.dp),
            contentPadding = PaddingValues(20.dp)
        ) {
            Text("Receive", color = MaterialTheme.colorScheme.onSurfaceVariant)
            Spacer(Modifier.height(12.dp))
            Row(verticalAlignment = Alignment.CenterVertically) {
                Row(
                    modifier = Modifier
                        .clip(RoundedCornerShape(20.dp))
                        .background(MaterialTheme.colorScheme.surfaceVariant)
                        .padding(horizontal = 12.dp, vertical = 8.dp),
                    verticalAlignment = Alignment.CenterVertically
                ) {
                    ChainIcon("ETH", 24.dp)
                    Spacer(Modifier.width(8.dp))
                    Text("USDC", fontWeight = FontWeight.Bold)
                    Icon(
                        Icons.Default.KeyboardArrowDown,
                        contentDescription = null,
                        modifier = Modifier.size(20.dp)
                    )
                }
                Spacer(Modifier.weight(1f))
                Text(
                    receiveAmount,
                    style = PriceTextStyle.copy(fontSize = 28.sp),
                    color = MaterialTheme.colorScheme.onSurface
                )
            }
        }
        
        Spacer(Modifier.height(24.dp))
        
        // 交易详情
        DexCard(
            modifier = Modifier.fillMaxWidth(),
            containerColor = MaterialTheme.colorScheme.surfaceVariant.copy(alpha = 0.5f),
            contentPadding = PaddingValues(16.dp)
        ) {
            SwapDetailRow("Exchange Rate", "1 BTC = 97,500 USDC")
            SwapDetailRow("Network Fee", "~$2.50")
            SwapDetailRow("Estimated Time", "~2 mins")
            SwapDetailRow("Slippage", "0.5%")
        }
        
        Spacer(Modifier.weight(1f))
        
        // Swap 按钮
        DexPrimaryButton(
            text = "Swap Now",
            onClick = { 
                viewModel.onEvent(UiIntent.SubmitMultiChainSwap(
                    fromChain = "BTC",
                    toChain = "ETH",
                    fromAsset = "BTC",
                    toAsset = "USDC",
                    amount = payAmount,
                    recipient = "0x..."
                ))
            },
            modifier = Modifier.fillMaxWidth()
        )
    }
}

@Composable
private fun SwapDetailRow(label: String, value: String) {
    Row(
        modifier = Modifier
            .fillMaxWidth()
            .padding(vertical = 6.dp),
        horizontalArrangement = Arrangement.SpaceBetween
    ) {
        Text(
            label,
            color = MaterialTheme.colorScheme.onSurfaceVariant,
            fontSize = 14.sp
        )
        Text(
            value,
            color = MaterialTheme.colorScheme.onSurface,
            fontSize = 14.sp,
            fontWeight = FontWeight.Medium
        )
    }
}
