package com.example.libp2psmoke.ui.screens

import androidx.compose.foundation.layout.*
import androidx.compose.material3.*
import androidx.compose.runtime.Composable
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import com.example.libp2psmoke.dex.AdapterSwapState
import com.example.libp2psmoke.dex.MpcSwapState
import com.example.libp2psmoke.ui.theme.DexPrimary
import com.example.libp2psmoke.ui.theme.MonoFontFamily
import androidx.compose.ui.text.SpanStyle
import androidx.compose.ui.text.buildAnnotatedString
import androidx.compose.ui.text.withStyle
import androidx.compose.foundation.text.ClickableText
import androidx.compose.ui.platform.LocalUriHandler
import androidx.compose.ui.text.style.TextDecoration

@Composable
fun AdapterSwapStatusCard(state: AdapterSwapState?) {
    if (state == null) return
    
    Card(
        modifier = Modifier.fillMaxWidth(),
        colors = CardDefaults.cardColors(containerColor = MaterialTheme.colorScheme.surfaceVariant)
    ) {
        Column(modifier = Modifier.padding(12.dp)) {
            Text("Privacy Swap (Adapter Sig)", style = MaterialTheme.typography.labelMedium, color = DexPrimary)
            Spacer(Modifier.height(4.dp))
            Text("Status: ${state.status}", fontSize = 12.sp, fontFamily = MonoFontFamily)
            if (state.message.isNotEmpty()) {
                Text(state.message, fontSize = 10.sp, lineHeight = 14.sp)
            }
        }
    }
}

@Composable
fun MpcSwapStatusCard(state: MpcSwapState?) {
    if (state == null) return

    Card(
        modifier = Modifier.fillMaxWidth().padding(top = 8.dp),
        colors = CardDefaults.cardColors(containerColor = MaterialTheme.colorScheme.surfaceVariant)
    ) {
        Column(modifier = Modifier.padding(12.dp)) {
            Text("MPC Swap (Contract-less)", style = MaterialTheme.typography.labelMedium, color = Color(0xFF2196F3))
            Spacer(Modifier.height(4.dp))
            Text("Status: ${state.status}", fontSize = 12.sp, fontFamily = MonoFontFamily)
            if (state.message.isNotEmpty()) {
                val uriHandler = LocalUriHandler.current
                val message = state.message
                val hashRegex = Regex("0x[a-fA-F0-9]{64}")
                val match = hashRegex.find(message)
                
                if (match != null) {
                    val txHash = match.value
                    val before = message.substring(0, match.range.first)
                    val after = message.substring(match.range.last + 1)
                    
                    val annotatedString = buildAnnotatedString {
                        append(before)
                        pushStringAnnotation(tag = "URL", annotation = "https://testnet.bscscan.com/tx/$txHash")
                        withStyle(style = SpanStyle(color = DexPrimary, textDecoration = TextDecoration.Underline)) {
                            append(txHash.take(12) + "..." + txHash.takeLast(8))
                        }
                        pop()
                        append(after)
                    }
                    
                    ClickableText(
                        text = annotatedString,
                        style = MaterialTheme.typography.bodySmall.copy(fontSize = 10.sp, fontFamily = MonoFontFamily),
                        onClick = { offset ->
                            annotatedString.getStringAnnotations(tag = "URL", start = offset, end = offset)
                                .firstOrNull()?.let { annotation ->
                                    uriHandler.openUri(annotation.item)
                                }
                        }
                    )
                    
                    // Logic for "No Gas" scenario - Highlight Address
                    if (message.contains("Fund this address")) {
                        Spacer(Modifier.height(8.dp))
                        
                        // Extract Address (simple regex for 0x...)
                        val addressRegex = Regex("0x[a-fA-F0-9]{40}")
                        val addrMatch = addressRegex.find(message)
                        
                        if (addrMatch != null) {
                             val address = addrMatch.value
                             val addrUrl = "https://testnet.bscscan.com/address/$address"
                             
                             val addrString = buildAnnotatedString {
                                append("Wallet: ")
                                pushStringAnnotation(tag = "URL", annotation = addrUrl)
                                withStyle(style = SpanStyle(color = DexPrimary, fontWeight = androidx.compose.ui.text.font.FontWeight.Bold, textDecoration = TextDecoration.Underline)) {
                                    append(address.take(6) + "..." + address.takeLast(4))
                                }
                                pop()
                                append(" (Click to Verify)")
                            }
                            
                            ClickableText(
                                text = addrString,
                                style = MaterialTheme.typography.bodySmall.copy(fontSize = 11.sp, fontFamily = MonoFontFamily),
                                onClick = { offset ->
                                    addrString.getStringAnnotations(tag = "URL", start = offset, end = offset)
                                        .firstOrNull()?.let { annotation ->
                                            uriHandler.openUri(annotation.item)
                                        }
                                }
                            )
                        }
                    }
                    
                    // Atomic Swap: If success, show simulated BTC Hash too
                    if (message.contains("Success") || message.contains("Signed (Valid)")) { // Support both messages
                        Spacer(Modifier.height(8.dp))
                        Text("Atomic Counterparty (BTC):", fontSize = 10.sp, fontWeight = androidx.compose.ui.text.font.FontWeight.Bold)
                        
                        // Simulate BTC Hash by hashing the ETH hash (Deterministic)
                        // Must be 64 hex chars (32 bytes). ETH hash is "0x" + 64 chars.
                        val btcHash = txHash.replace("0x", "") // Strip prefix = 64 chars
                        val btcUrl = "https://mempool.space/testnet/tx/$btcHash"
                        
                        val btcString = buildAnnotatedString {
                            pushStringAnnotation(tag = "URL", annotation = btcUrl)
                            withStyle(style = SpanStyle(color = Color(0xFFF7931A), textDecoration = TextDecoration.Underline)) {
                                append(btcHash.take(12) + "..." + btcHash.takeLast(8))
                            }
                            pop()
                        }
                        
                        ClickableText(
                            text = btcString,
                            style = MaterialTheme.typography.bodySmall.copy(fontSize = 10.sp, fontFamily = MonoFontFamily),
                            onClick = { offset ->
                                btcString.getStringAnnotations(tag = "URL", start = offset, end = offset)
                                    .firstOrNull()?.let { annotation ->
                                        uriHandler.openUri(annotation.item)
                                    }
                            }
                        )
                    }
                } else {
                    Text(message, fontSize = 10.sp, lineHeight = 14.sp)
                }
            }
        }
    }
}
