package com.example.libp2psmoke.ui.theme

import androidx.compose.ui.graphics.Brush
import androidx.compose.ui.graphics.Color

// ═══════════════════════════════════════════════════════════════════
// 高级金融应用配色系统 - Cyber Finance Aesthetic
// ═══════════════════════════════════════════════════════════════════

// 主背景色 - 深邃的交易界面
val DexDarkBg = Color(0xFF0D0F14)         // 极深背景 - 更专业
val DexSurface = Color(0xFF161B26)         // 卡片表面
val DexSurfaceHighlight = Color(0xFF1E2636) // 高亮表面
val DexElevated = Color(0xFF252D40)        // 悬浮层

// 买卖核心色 - 优化后的对比度
val DexGreen = Color(0xFF00D68F)           // 买入/上涨 - 更鲜艳
val DexGreenLight = Color(0xFF4FFFC3)      // 绿色高亮
val DexGreenDark = Color(0xFF00A86B)       // 绿色深色
val DexRed = Color(0xFFFF4757)             // 卖出/下跌
val DexRedLight = Color(0xFFFF6B7A)        // 红色高亮
val DexRedDark = Color(0xFFE83F51)         // 红色深色

// 品牌强调色
val DexPrimary = Color(0xFF00A3FF)         // 主品牌色 - 电光蓝
val DexPrimaryLight = Color(0xFF5CC7FF)    // 浅品牌色
val DexPrimaryDark = Color(0xFF0077CC)     // 深品牌色
val DexOnPrimary = Color(0xFFFFFFFF)

// 辅助强调色
val DexAccent = Color(0xFFBB86FC)          // 紫色强调 - 高端感
val DexAccentSecondary = Color(0xFF03DAC6) // 青色强调
val DexYellow = Color(0xFFFFCC00)          // 警告 / BTC
val DexOrange = Color(0xFFFF9500)          // 挂单

// 文字层次
val TextPrimary = Color(0xFFF1F5F9)        // 主要文字 - 更柔和
val TextSecondary = Color(0xFF8896AB)      // 次要文字
val TextTertiary = Color(0xFF5C6A7D)       // 第三级文字
val TextDisabled = Color(0xFF3D4654)       // 禁用文字

// 边框和分割线
val DexBorder = Color(0xFF2A3545)          // 标准边框
val DexBorderLight = Color(0xFF3A4555)     // 浅边框
val DexDivider = Color(0xFF1F2937)         // 分割线

// ═══════════════════════════════════════════════════════════════════
// 浅色主题
// ═══════════════════════════════════════════════════════════════════
val DexLightBg = Color(0xFFF8FAFC)
val DexLightSurface = Color(0xFFFFFFFF)
val DexLightSurfaceHighlight = Color(0xFFF1F5F9)
val DexLightElevated = Color(0xFFE2E8F0)
val TextPrimaryLight = Color(0xFF0F172A)
val TextSecondaryLight = Color(0xFF64748B)
val TextTertiaryLight = Color(0xFF94A3B8)
val DexLightBorder = Color(0xFFE2E8F0)
val DexLightDivider = Color(0xFFF1F5F9)

// ═══════════════════════════════════════════════════════════════════
// 渐变色
// ═══════════════════════════════════════════════════════════════════
val GradientPrimaryHorizontal = Brush.horizontalGradient(
    colors = listOf(Color(0xFF00A3FF), Color(0xFF00D4FF))
)

val GradientGreenVertical = Brush.verticalGradient(
    colors = listOf(Color(0xFF00D68F), Color(0xFF00A86B))
)

val GradientRedVertical = Brush.verticalGradient(
    colors = listOf(Color(0xFFFF4757), Color(0xFFE83F51))
)

val GradientCardBg = Brush.verticalGradient(
    colors = listOf(Color(0xFF161B26), Color(0xFF0D0F14))
)

val GradientCyberPurple = Brush.horizontalGradient(
    colors = listOf(Color(0xFF7C3AED), Color(0xFFDB2777))
)

val GradientGoldAccent = Brush.horizontalGradient(
    colors = listOf(Color(0xFFFFCC00), Color(0xFFFF9500))
)

// ═══════════════════════════════════════════════════════════════════
// 语义化颜色
// ═══════════════════════════════════════════════════════════════════
val ColorSuccess = DexGreen
val ColorError = DexRed
val ColorWarning = DexYellow
val ColorInfo = DexPrimary

// 特殊状态
val ColorPending = Color(0xFFFF9500)       // 等待中
val ColorConfirmed = Color(0xFF00D68F)     // 已确认
val ColorCancelled = Color(0xFF6B7280)     // 已取消

// ═══════════════════════════════════════════════════════════════════
// 链标识颜色
// ═══════════════════════════════════════════════════════════════════
val ChainBTC = Color(0xFFF7931A)
val ChainETH = Color(0xFF627EEA)
val ChainBSC = Color(0xFFF3BA2F)
val ChainSOL = Color(0xFF00FFA3)
val ChainTRX = Color(0xFFFF0013)
val ChainAVAX = Color(0xFFE84142)
val ChainMATIC = Color(0xFF8247E5)
val ChainARB = Color(0xFF28A0F0)

// Legacy colors (保持向后兼容)
val Purple80 = Color(0xFFD0BCFF)
val PurpleGrey80 = Color(0xFFCCC2DC)
val Pink80 = Color(0xFFEFB8C8)
val Purple40 = Color(0xFF6650a4)
val PurpleGrey40 = Color(0xFF625b71)
val Pink40 = Color(0xFF7D5260)

object AppColors {
    val Success = DexGreen
    val Error = DexRed
    val Warning = DexYellow
    val Info = DexPrimary
    val TextSecondary = com.example.libp2psmoke.ui.theme.TextSecondary
    val TextTertiary = com.example.libp2psmoke.ui.theme.TextTertiary
}
