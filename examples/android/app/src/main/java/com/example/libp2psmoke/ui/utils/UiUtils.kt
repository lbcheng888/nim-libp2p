package com.example.libp2psmoke.ui.utils

import java.math.BigDecimal
import java.math.RoundingMode

fun formatDecimal(value: BigDecimal): String = value.stripTrailingZeros().toPlainString()

fun formatDecimalOrDash(value: BigDecimal?): String = value?.stripTrailingZeros()?.toPlainString() ?: "--"

fun formatVolumeOrDash(value: BigDecimal?): String {
    if (value == null) return "--"
    return when {
        value >= BigDecimal(1_000_000) -> "${value.divide(BigDecimal(1_000_000), 2, RoundingMode.HALF_UP)}M"
        value >= BigDecimal(1_000) -> "${value.divide(BigDecimal(1_000), 2, RoundingMode.HALF_UP)}K"
        else -> value.stripTrailingZeros().toPlainString()
    }
}

fun sanitizeDecimalInput(raw: String, maxDecimals: Int? = null): String {
    if (raw.isEmpty()) return ""
    val sb = StringBuilder(raw.length)
    var hasDot = false
    for (ch in raw) {
        when {
            ch in '0'..'9' -> sb.append(ch)
            ch == '.' && !hasDot -> {
                sb.append(ch)
                hasDot = true
            }
        }
    }
    var filtered = sb.toString()
    if (filtered.startsWith(".")) filtered = "0$filtered"
    if (maxDecimals != null && maxDecimals >= 0) {
        val dot = filtered.indexOf('.')
        if (dot >= 0) {
            val end = minOf(filtered.length, dot + 1 + maxDecimals)
            filtered = filtered.substring(0, end)
        }
    }
    return filtered
}
