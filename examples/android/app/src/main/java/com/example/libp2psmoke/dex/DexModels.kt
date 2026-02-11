package com.example.libp2psmoke.dex

import java.math.BigDecimal
import java.math.BigInteger

const val DEFAULT_DEX_SYMBOL = "BTC/USDC"
const val BINANCE_SPOT_SYMBOL = "BTCUSDC"
const val BINANCE_SPOT_STREAM = "btcusdc"
val BINANCE_INTERVALS = listOf("1m", "15m", "1h", "4h", "1d")

enum class DexChain {
    BTC_TESTNET,
    BSC_TESTNET
}

enum class DexSwapStatus {
    Pending,
    Broadcasting,
    Confirmed,
    Failed
}

data class BtcRpcConfig(
    val url: String,
    val username: String,
    val password: String
)

data class BtcSwapRequest(
    val orderId: String,
    val amountSats: Long,
    val targetAddress: String
)

data class BscTransferRequest(
    val orderId: String,
    val rpcUrl: String,
    val contractAddress: String,
    val privateKey: String,
    val toAddress: String,
    val amount: BigInteger,
    val decimals: Int = 18
) {
    val normalizedAmount: BigDecimal
        get() = BigDecimal(amount).movePointLeft(decimals)
}

data class DexSwapResult(
    val chain: DexChain,
    val orderId: String,
    val status: DexSwapStatus,
    val amountDisplay: String,
    val txHash: String? = null,
    val message: String? = null,
    val timestampMs: Long = System.currentTimeMillis()
)

data class DexTrade(
    val orderId: String,
    val symbol: String,
    val price: BigDecimal,
    val amountBase: BigDecimal,
    val timestampMs: Long
)

enum class DexKlineScale(val seconds: Long, val label: String) {
    ONE_SECOND(1, "1s"),
    ONE_MINUTE(60, "1m"),
    FIFTEEN_MINUTES(900, "15m"),
    ONE_HOUR(3600, "1h"),
    FOUR_HOURS(14_400, "4h"),
    ONE_DAY(86_400, "1d");

    companion object {
        fun fromLabel(label: String): DexKlineScale =
            values().firstOrNull { it.label.equals(label, ignoreCase = true) } ?: ONE_MINUTE
    }
}

data class DexKlineBucket(
    val symbol: String,
    val scale: DexKlineScale,
    val windowStartMs: Long,
    val open: BigDecimal,
    val high: BigDecimal,
    val low: BigDecimal,
    val close: BigDecimal,
    val volumeBase: BigDecimal,
    val tradeCount: Int
)

data class DexPartialFill(
    var btcAmountSats: Long? = null,
    var btcTxHash: String? = null,
    var usdcAmount: BigInteger? = null,
    var usdcDecimals: Int? = null,
    var usdcTxHash: String? = null,
    var updatedAtMs: Long = System.currentTimeMillis()
)

class DexRpcException(message: String, cause: Throwable? = null) : Exception(message, cause)

data class BinanceTicker(
    val lastPrice: BigDecimal,
    val priceChangePercent: BigDecimal,
    val highPrice: BigDecimal,
    val lowPrice: BigDecimal,
    val volumeBase: BigDecimal,
    val volumeQuote: BigDecimal,
    val closeTimeMs: Long
)

data class OrderBookEntry(
    val price: BigDecimal,
    val quantity: BigDecimal,
    val cumulativeQuantity: BigDecimal
)

fun List<DexKlineBucket>.toTradingViewJson(): String {
    if (isEmpty()) return "[]"
    val ordered = this.distinctBy { it.windowStartMs }.sortedBy { it.windowStartMs }
    val builder = StringBuilder("[")
    ordered.forEachIndexed { index, bucket ->
        if (index > 0) builder.append(',')
        builder.append("{\"time\":").append(bucket.windowStartMs / 1000)
        builder.append(",\"open\":").append(bucket.open)
        builder.append(",\"high\":").append(bucket.high)
        builder.append(",\"low\":").append(bucket.low)
        builder.append(",\"close\":").append(bucket.close)
        builder.append(",\"volume\":").append(bucket.volumeBase).append("}")
    }
    builder.append(']')
    return builder.toString()
}
