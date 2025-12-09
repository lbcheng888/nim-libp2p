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
