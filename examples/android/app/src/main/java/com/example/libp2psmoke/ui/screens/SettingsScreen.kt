package com.example.libp2psmoke.ui.screens

import androidx.compose.foundation.background
import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.shape.CircleShape
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.foundation.verticalScroll
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.filled.ChevronRight
import androidx.compose.material.icons.filled.DeleteForever
import androidx.compose.material.icons.filled.Hub
import androidx.compose.material.icons.filled.Key
import androidx.compose.material.icons.filled.Language
import androidx.compose.material.icons.filled.Lock
import androidx.compose.material.icons.filled.Notifications
import androidx.compose.material.icons.filled.Palette
import androidx.compose.material.icons.filled.Speed
import androidx.compose.material3.*
import androidx.compose.runtime.Composable
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.draw.clip
import androidx.compose.ui.graphics.Brush
import androidx.compose.ui.graphics.vector.ImageVector
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import com.example.libp2psmoke.model.NodeUiState
import com.example.libp2psmoke.ui.UiIntent
import com.example.libp2psmoke.ui.components.*
import com.example.libp2psmoke.ui.theme.*
import com.example.libp2psmoke.viewmodel.NimNodeViewModel

@Composable
fun SettingsScreen(uiState: NodeUiState, viewModel: NimNodeViewModel) {
    Column(
        modifier = Modifier
            .fillMaxSize()
            .background(MaterialTheme.colorScheme.background)
            .verticalScroll(rememberScrollState())
            .padding(16.dp)
    ) {
        Text(
            "Settings",
            style = MaterialTheme.typography.headlineMedium,
            fontWeight = FontWeight.Bold
        )
        
        Spacer(Modifier.height(24.dp))

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
        
        // Node Info 卡片
        DexCard(
            modifier = Modifier.fillMaxWidth(),
            shape = RoundedCornerShape(16.dp)
        ) {
            Row(verticalAlignment = Alignment.CenterVertically) {
                Box(
                    modifier = Modifier
                        .size(48.dp)
                        .clip(CircleShape)
                        .background(DexPrimary.copy(alpha = 0.12f)),
                    contentAlignment = Alignment.Center
                ) {
                    Icon(
                        Icons.Default.Hub,
                        contentDescription = null,
                        tint = DexPrimary,
                        modifier = Modifier.size(24.dp)
                    )
                }
                Spacer(Modifier.width(16.dp))
                Column {
                    Text(
                        "Node Status",
                        fontWeight = FontWeight.Bold,
                        fontSize = 16.sp
                    )
                    Text(
                        if (uiState.running) "Running" else "Stopped",
                        color = if (uiState.running) DexGreen else DexRed,
                        fontSize = 14.sp
                    )
                }
            }
            
            Spacer(Modifier.height(16.dp))
            HorizontalDivider(color = MaterialTheme.colorScheme.outline.copy(alpha = 0.1f))
            Spacer(Modifier.height(16.dp))
            
            Text(
                "Peer ID",
                color = MaterialTheme.colorScheme.onSurfaceVariant,
                fontSize = 12.sp
            )
            Spacer(Modifier.height(4.dp))
            Text(
                uiState.localPeerId ?: "Unknown",
                style = AddressStyle,
                color = MaterialTheme.colorScheme.onSurface
            )
            Spacer(Modifier.height(12.dp))
            Text(
                "Connected peers: ${uiState.peerCount}",
                color = MaterialTheme.colorScheme.onSurfaceVariant,
                fontSize = 12.sp
            )
        }
        
        Spacer(Modifier.height(16.dp))
        
        // 设置选项
        SettingsSection(title = "General") {
            SettingsItem(
                icon = Icons.Default.Palette,
                title = "Appearance",
                subtitle = "Dark mode"
            )
            SettingsItem(
                icon = Icons.Default.Language,
                title = "Language",
                subtitle = "English"
            )
            SettingsItem(
                icon = Icons.Default.Notifications,
                title = "Notifications",
                subtitle = "Enabled"
            )
        }
        
        Spacer(Modifier.height(16.dp))
        
        SettingsSection(title = "Security") {
            SettingsItem(
                icon = Icons.Default.Lock,
                title = "Security Settings",
                subtitle = "Biometric enabled"
            )
            SettingsItem(
                icon = Icons.Default.Key,
                title = "Backup Wallet",
                subtitle = "Last backup: Never"
            )
        }
        
        Spacer(Modifier.height(16.dp))
        
        SettingsSection(title = "Network") {
            Column {
                SettingsItem(
                    icon = Icons.Default.Speed,
                    title = "Market data",
                    subtitle = if (uiState.marketEnabled) "Enabled" else "Disabled"
                ) {
                    viewModel.onEvent(UiIntent.SetMarketEnabled(!uiState.marketEnabled))
                }
                HorizontalDivider(color = MaterialTheme.colorScheme.outline.copy(alpha = 0.08f))
                Column(modifier = Modifier.padding(16.dp)) {
                    OutlinedTextField(
                        value = uiState.bootstrapPeersRaw,
                        onValueChange = { viewModel.onEvent(UiIntent.UpdateBootstrapPeers(it)) },
                        label = { Text("Bootstrap peers") },
                        placeholder = { Text("One multiaddr per line, or JSON array") },
                        modifier = Modifier.fillMaxWidth(),
                        minLines = 2,
                        shape = RoundedCornerShape(12.dp)
                    )
                    Spacer(Modifier.height(12.dp))
                    OutlinedTextField(
                        value = uiState.relayPeersRaw,
                        onValueChange = { viewModel.onEvent(UiIntent.UpdateRelayPeers(it)) },
                        label = { Text("Relay peers (optional)") },
                        placeholder = { Text("One multiaddr per line, or JSON array") },
                        modifier = Modifier.fillMaxWidth(),
                        minLines = 2,
                        shape = RoundedCornerShape(12.dp)
                    )
                    Spacer(Modifier.height(12.dp))
                    Button(
                        onClick = { viewModel.onEvent(UiIntent.ApplyNetworkConfig) },
                        modifier = Modifier.fillMaxWidth(),
                        shape = RoundedCornerShape(12.dp)
                    ) {
                        Text("Apply & Restart Node")
                    }
                }
            }
        }
        
        Spacer(Modifier.height(24.dp))
        
        // 危险操作
        OutlinedButton(
            onClick = { viewModel.onEvent(UiIntent.ResetNodeData) },
            modifier = Modifier.fillMaxWidth(),
            colors = ButtonDefaults.outlinedButtonColors(
                contentColor = DexRed
            ),
            border = ButtonDefaults.outlinedButtonBorder(enabled = true).copy(
                brush = Brush.horizontalGradient(listOf(DexRed.copy(alpha = 0.5f), DexRed.copy(alpha = 0.5f)))
            ),
            shape = RoundedCornerShape(12.dp)
        ) {
            Icon(Icons.Default.DeleteForever, contentDescription = null)
            Spacer(Modifier.width(8.dp))
            Text("Reset Node Data")
        }
        
        Spacer(Modifier.height(32.dp))
        
        // 版本信息
        Text(
            "Version 1.0.0 (Build 1)",
            color = MaterialTheme.colorScheme.onSurfaceVariant,
            fontSize = 12.sp,
            modifier = Modifier.align(Alignment.CenterHorizontally)
        )
    }
}

@Composable
private fun SettingsSection(
    title: String,
    content: @Composable ColumnScope.() -> Unit
) {
    Column {
        Text(
            title,
            style = MaterialTheme.typography.labelLarge,
            color = MaterialTheme.colorScheme.onSurfaceVariant,
            modifier = Modifier.padding(bottom = 8.dp)
        )
        DexCard(
            modifier = Modifier.fillMaxWidth(),
            shape = RoundedCornerShape(16.dp),
            contentPadding = PaddingValues(0.dp)
        ) {
            content()
        }
    }
}

@Composable
private fun SettingsItem(
    icon: ImageVector,
    title: String,
    subtitle: String,
    onClick: () -> Unit = {}
) {
    Row(
        modifier = Modifier
            .fillMaxWidth()
            .clickable(onClick = onClick)
            .padding(16.dp),
        verticalAlignment = Alignment.CenterVertically
    ) {
        Icon(
            icon,
            contentDescription = null,
            tint = MaterialTheme.colorScheme.onSurfaceVariant,
            modifier = Modifier.size(24.dp)
        )
        Spacer(Modifier.width(16.dp))
        Column(modifier = Modifier.weight(1f)) {
            Text(
                title,
                fontWeight = FontWeight.Medium,
                fontSize = 16.sp
            )
            Text(
                subtitle,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
                fontSize = 13.sp
            )
        }
        Icon(
            Icons.Default.ChevronRight,
            contentDescription = null,
            tint = MaterialTheme.colorScheme.onSurfaceVariant,
            modifier = Modifier.size(20.dp)
        )
    }
}
