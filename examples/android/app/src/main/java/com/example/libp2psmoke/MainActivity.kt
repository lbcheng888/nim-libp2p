package com.example.libp2psmoke

import android.annotation.SuppressLint
import android.content.Context
import android.os.Bundle
import android.util.Base64
import android.util.Log
import android.webkit.ConsoleMessage
import android.webkit.WebChromeClient
import android.webkit.WebView
import android.webkit.WebViewClient
import android.webkit.WebResourceRequest
import android.webkit.WebResourceResponse
import android.webkit.WebSettings
import androidx.activity.ComponentActivity
import androidx.activity.compose.setContent
import androidx.activity.enableEdgeToEdge
import androidx.compose.animation.*
import androidx.compose.animation.core.*
import androidx.compose.foundation.background
import androidx.compose.foundation.border
import androidx.compose.foundation.clickable
import androidx.compose.foundation.interaction.MutableInteractionSource
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.shape.CircleShape
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.foundation.verticalScroll
import androidx.compose.foundation.horizontalScroll
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.filled.*
import androidx.compose.material.icons.outlined.*
import androidx.compose.material3.*
import androidx.compose.material3.windowsizeclass.ExperimentalMaterial3WindowSizeClassApi
import androidx.compose.material3.windowsizeclass.WindowWidthSizeClass
import androidx.compose.material3.windowsizeclass.calculateWindowSizeClass
import androidx.compose.runtime.*
import androidx.compose.runtime.saveable.rememberSaveable
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.draw.clip
import androidx.compose.ui.draw.shadow
import androidx.compose.ui.graphics.Brush
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.graphics.toArgb
import androidx.compose.ui.graphics.vector.ImageVector
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import androidx.compose.ui.viewinterop.AndroidView
import androidx.lifecycle.viewmodel.compose.viewModel
import com.example.libp2psmoke.dex.DexKlineBucket
import com.example.libp2psmoke.dex.OrderBookEntry
import com.example.libp2psmoke.dex.toTradingViewJson
import com.example.libp2psmoke.mixer.MixerTab
import com.example.libp2psmoke.model.ChainWallet
import com.example.libp2psmoke.model.FeedEntry
import com.example.libp2psmoke.model.NodeUiState
import com.example.libp2psmoke.ui.UiIntent
import com.example.libp2psmoke.ui.components.*
import com.example.libp2psmoke.ui.theme.*
import com.example.libp2psmoke.viewmodel.NimNodeViewModel
import java.math.BigDecimal
import java.math.RoundingMode
import com.example.libp2psmoke.ui.screens.*
import com.example.libp2psmoke.ui.utils.*

class MainActivity : ComponentActivity() {
    @OptIn(ExperimentalMaterial3WindowSizeClassApi::class)
    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        enableEdgeToEdge()
        setContent {
            Libp2pSmokeTheme {
                val viewModel: NimNodeViewModel = viewModel()
                val uiState by viewModel.state.collectAsState()
                val windowSizeClass = calculateWindowSizeClass(this)
                ProLibp2pApp(uiState = uiState, viewModel = viewModel, windowSizeClass = windowSizeClass)
            }
        }
    }
}

@Composable
private fun ProLibp2pApp(
    uiState: NodeUiState,
    viewModel: NimNodeViewModel,
    windowSizeClass: androidx.compose.material3.windowsizeclass.WindowSizeClass
) {
    var selectedTab by rememberSaveable { mutableIntStateOf(0) }
    val snackbarHostState = remember { SnackbarHostState() }
    val useNavRail = windowSizeClass.widthSizeClass != WindowWidthSizeClass.Compact

    val tabs = listOf(
        NavTab("Market", Icons.Filled.CandlestickChart, Icons.Outlined.CandlestickChart),
        NavTab("Trade", Icons.Filled.CurrencyExchange, Icons.Outlined.CurrencyExchange),
        NavTab("Wallet", Icons.Filled.AccountBalanceWallet, Icons.Outlined.AccountBalanceWallet),
        NavTab("Mixer", Icons.Filled.Security, Icons.Outlined.Security),
        NavTab("Settings", Icons.Filled.Settings, Icons.Outlined.Settings)
    )

    Surface(modifier = Modifier.fillMaxSize(), color = MaterialTheme.colorScheme.background) {
        Row(modifier = Modifier.fillMaxSize()) {
            // 侧边导航栏（大屏幕）
            if (useNavRail) {
                NavigationRail(
                    containerColor = MaterialTheme.colorScheme.surface,
                    contentColor = MaterialTheme.colorScheme.onSurface
                ) {
                    Spacer(Modifier.height(16.dp))
                    tabs.forEachIndexed { index, tab ->
                        NavigationRailItem(
                            selected = selectedTab == index,
                            onClick = { selectedTab = index },
                            icon = {
                                Icon(
                                    if (selectedTab == index) tab.selectedIcon else tab.icon,
                                    contentDescription = tab.title,
                                    modifier = Modifier.size(24.dp)
                                )
                            },
                            label = { 
                                Text(
                                    tab.title, 
                                    fontSize = 11.sp,
                                    fontWeight = if (selectedTab == index) FontWeight.Bold else FontWeight.Normal
                                )
                            },
                            colors = NavigationRailItemDefaults.colors(
                                selectedIconColor = DexPrimary,
                                selectedTextColor = DexPrimary,
                                indicatorColor = DexPrimary.copy(alpha = 0.12f),
                                unselectedIconColor = MaterialTheme.colorScheme.onSurfaceVariant,
                                unselectedTextColor = MaterialTheme.colorScheme.onSurfaceVariant
                            )
                        )
                    }
                }
            }

            Scaffold(
                snackbarHost = { 
                    SnackbarHost(snackbarHostState) { data ->
                        Snackbar(
                            snackbarData = data,
                            containerColor = MaterialTheme.colorScheme.surfaceVariant,
                            contentColor = MaterialTheme.colorScheme.onSurface,
                            shape = RoundedCornerShape(12.dp)
                        )
                    }
                },
                bottomBar = {
                    if (!useNavRail) {
                        NavigationBar(
                            containerColor = MaterialTheme.colorScheme.surface,
                            tonalElevation = 0.dp,
                            modifier = Modifier
                                .border(
                                    width = 1.dp,
                                    color = MaterialTheme.colorScheme.outline.copy(alpha = 0.1f),
                                    shape = RoundedCornerShape(topStart = 20.dp, topEnd = 20.dp)
                                )
                        ) {
                            tabs.forEachIndexed { index, tab ->
                                NavigationBarItem(
                                    selected = selectedTab == index,
                                    onClick = { selectedTab = index },
                                    icon = {
                                        Icon(
                                            if (selectedTab == index) tab.selectedIcon else tab.icon,
                                            contentDescription = tab.title,
                                            modifier = Modifier.size(22.dp)
                                        )
                                    },
                                    label = { 
                                        Text(
                                            tab.title, 
                                            fontSize = 10.sp,
                                            fontWeight = if (selectedTab == index) FontWeight.Bold else FontWeight.Normal
                                        )
                                    },
                                    colors = NavigationBarItemDefaults.colors(
                                        selectedIconColor = DexPrimary,
                                        selectedTextColor = DexPrimary,
                                        indicatorColor = DexPrimary.copy(alpha = 0.12f),
                                        unselectedIconColor = MaterialTheme.colorScheme.onSurfaceVariant,
                                        unselectedTextColor = MaterialTheme.colorScheme.onSurfaceVariant
                                    )
                                )
                            }
                        }
                    }
                }
            ) { padding ->
                // 页面切换动画
                AnimatedContent(
                    targetState = selectedTab,
                    transitionSpec = {
                        fadeIn(animationSpec = tween(300)) togetherWith
                        fadeOut(animationSpec = tween(200))
                    },
                    modifier = Modifier.padding(padding).fillMaxSize(),
                    label = "tabContent"
                ) { tab ->
                    when (tab) {
                        0 -> ProDexScreen(uiState, viewModel, snackbarHostState)
                        1 -> SwapScreen(uiState, viewModel)
                        2 -> WalletScreen(uiState)
                        3 -> MixerTab(viewModel, snackbarHostState)
                        4 -> SettingsScreen(uiState)
                    }
                }
            }
        }
    }
}

data class NavTab(val title: String, val selectedIcon: ImageVector, val icon: ImageVector)


