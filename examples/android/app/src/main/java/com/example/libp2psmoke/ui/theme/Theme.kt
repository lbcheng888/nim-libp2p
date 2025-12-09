package com.example.libp2psmoke.ui.theme

import android.app.Activity
import android.os.Build
import android.util.Log
import androidx.compose.animation.animateColorAsState
import androidx.compose.animation.core.tween
import androidx.compose.foundation.isSystemInDarkTheme
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.darkColorScheme
import androidx.compose.material3.lightColorScheme
import androidx.compose.runtime.Composable
import androidx.compose.runtime.SideEffect
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.graphics.toArgb
import androidx.compose.ui.platform.LocalView
import androidx.core.view.WindowCompat

// ═══════════════════════════════════════════════════════════════════
// 专业 DEX 深色主题 - Cyber Finance
// ═══════════════════════════════════════════════════════════════════
private val ProDarkColorScheme = darkColorScheme(
    // 主色
    primary = DexPrimary,
    onPrimary = DexOnPrimary,
    primaryContainer = DexPrimaryDark,
    onPrimaryContainer = DexPrimaryLight,
    
    // 次要色
    secondary = DexGreen,
    onSecondary = Color.White,
    secondaryContainer = DexGreenDark,
    onSecondaryContainer = DexGreenLight,
    
    // 第三色
    tertiary = DexAccent,
    onTertiary = Color.White,
    tertiaryContainer = Color(0xFF4A148C),
    onTertiaryContainer = Color(0xFFE1BEE7),
    
    // 背景
    background = DexDarkBg,
    onBackground = TextPrimary,
    
    // 表面
    surface = DexSurface,
    onSurface = TextPrimary,
    surfaceVariant = DexSurfaceHighlight,
    onSurfaceVariant = TextSecondary,
    surfaceTint = DexPrimary,
    
    // 轮廓
    outline = DexBorder,
    outlineVariant = DexBorderLight,
    
    // 逆色
    inverseSurface = DexLightSurface,
    inverseOnSurface = TextPrimaryLight,
    inversePrimary = DexPrimaryDark,
    
    // 错误
    error = DexRed,
    onError = Color.White,
    errorContainer = DexRedDark,
    onErrorContainer = DexRedLight,
    
    // 其他
    scrim = Color.Black
)

// ═══════════════════════════════════════════════════════════════════
// 专业 DEX 浅色主题
// ═══════════════════════════════════════════════════════════════════
private val ProLightColorScheme = lightColorScheme(
    // 主色
    primary = DexPrimary,
    onPrimary = Color.White,
    primaryContainer = Color(0xFFD1E4FF),
    onPrimaryContainer = DexPrimaryDark,
    
    // 次要色
    secondary = DexGreen,
    onSecondary = Color.White,
    secondaryContainer = Color(0xFFB2DFDB),
    onSecondaryContainer = DexGreenDark,
    
    // 第三色
    tertiary = DexAccent,
    onTertiary = Color.White,
    tertiaryContainer = Color(0xFFE1BEE7),
    onTertiaryContainer = Color(0xFF4A148C),
    
    // 背景
    background = DexLightBg,
    onBackground = TextPrimaryLight,
    
    // 表面
    surface = DexLightSurface,
    onSurface = TextPrimaryLight,
    surfaceVariant = DexLightSurfaceHighlight,
    onSurfaceVariant = TextSecondaryLight,
    surfaceTint = DexPrimary,
    
    // 轮廓
    outline = DexLightBorder,
    outlineVariant = DexLightDivider,
    
    // 逆色
    inverseSurface = DexSurface,
    inverseOnSurface = TextPrimary,
    inversePrimary = DexPrimaryLight,
    
    // 错误
    error = DexRed,
    onError = Color.White,
    errorContainer = Color(0xFFFFDAD6),
    onErrorContainer = DexRedDark,
    
    // 其他
    scrim = Color.Black.copy(alpha = 0.3f)
)

// ═══════════════════════════════════════════════════════════════════
// 主题入口
// ═══════════════════════════════════════════════════════════════════
@Composable
fun Libp2pSmokeTheme(
    darkTheme: Boolean = isSystemInDarkTheme(),
    dynamicColor: Boolean = false, // 禁用动态颜色以保持品牌一致性
    content: @Composable () -> Unit
) {
    SideEffect {
        Log.d("Libp2pTheme", "Applying theme: darkTheme=$darkTheme")
    }
    
    val colorScheme = if (darkTheme) ProDarkColorScheme else ProLightColorScheme
    
    val view = LocalView.current
    if (!view.isInEditMode) {
        SideEffect {
            val window = (view.context as Activity).window
            // 使用更深的背景色让内容更突出
            window.statusBarColor = colorScheme.background.toArgb()
            window.navigationBarColor = colorScheme.background.toArgb()
            WindowCompat.getInsetsController(window, view).apply {
                isAppearanceLightStatusBars = !darkTheme
                isAppearanceLightNavigationBars = !darkTheme
            }
        }
    }

    MaterialTheme(
        colorScheme = colorScheme,
        typography = Typography,
        content = content
    )
}

// ═══════════════════════════════════════════════════════════════════
// 扩展的主题颜色访问
// ═══════════════════════════════════════════════════════════════════
object DexColors {
    val Green @Composable get() = DexGreen
    val GreenLight @Composable get() = DexGreenLight
    val GreenDark @Composable get() = DexGreenDark
    val Red @Composable get() = DexRed
    val RedLight @Composable get() = DexRedLight
    val RedDark @Composable get() = DexRedDark
    val Yellow @Composable get() = DexYellow
    val Orange @Composable get() = DexOrange
    val Accent @Composable get() = DexAccent
    val AccentSecondary @Composable get() = DexAccentSecondary
    
    // 链标识色
    val chainColor: @Composable (String) -> Color = { chainId ->
        when (chainId.uppercase()) {
            "BTC", "BITCOIN" -> ChainBTC
            "ETH", "ETHEREUM" -> ChainETH
            "BSC", "BNB" -> ChainBSC
            "SOL", "SOLANA" -> ChainSOL
            "TRX", "TRON" -> ChainTRX
            "AVAX", "AVALANCHE" -> ChainAVAX
            "MATIC", "POLYGON" -> ChainMATIC
            "ARB", "ARBITRUM" -> ChainARB
            else -> DexAccent
        }
    }
}
