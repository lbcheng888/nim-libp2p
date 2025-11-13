package com.example.libp2psmoke.dex

import java.math.BigDecimal

class DexKlineAggregator(
    private val symbol: String = DEFAULT_DEX_SYMBOL,
    private val scales: List<DexKlineScale> = listOf(
        DexKlineScale.ONE_SECOND,
        DexKlineScale.ONE_MINUTE
    ),
    private val maxBucketsPerScale: Int = 120
) {

    private val lock = Any()
    private val buckets = mutableMapOf<String, MutableBucket>()

    fun ingest(trade: DexTrade): List<DexKlineBucket> = synchronized(lock) {
        val ts = trade.timestampMs
        for (scale in scales) {
            val key = bucketKey(scale, ts)
            val bucket = buckets.getOrPut(key) {
                MutableBucket(
                    symbol = symbol,
                    scale = scale,
                    windowStartMs = alignedWindowStart(scale, ts),
                    open = trade.price,
                    high = trade.price,
                    low = trade.price,
                    close = trade.price,
                    volumeBase = BigDecimal.ZERO,
                    tradeCount = 0
                )
            }
            bucket.applyTrade(trade)
        }
        pruneBucketsLocked()
        snapshotLocked()
    }

    fun snapshot(): List<DexKlineBucket> = synchronized(lock) {
        snapshotLocked()
    }

    private fun snapshotLocked(): List<DexKlineBucket> =
        buckets.values
            .sortedWith(
                compareByDescending<MutableBucket> { it.windowStartMs }
                    .thenBy { it.scale.seconds }
            )
            .map { it.toImmutable() }

    private fun pruneBucketsLocked() {
        val grouped = buckets.values.groupBy { it.scale }
        for ((scale, items) in grouped) {
            if (items.size <= maxBucketsPerScale) continue
            val sorted = items.sortedBy { it.windowStartMs }
            val toRemove = sorted.size - maxBucketsPerScale
            for (i in 0 until toRemove) {
                val bucket = sorted[i]
                buckets.remove(bucketKey(scale, bucket.windowStartMs))
            }
        }
    }

    private fun bucketKey(scale: DexKlineScale, timestampMs: Long): String =
        "${scale.name}:${alignedWindowStart(scale, timestampMs)}"

    private fun alignedWindowStart(scale: DexKlineScale, timestampMs: Long): Long {
        val scaleMs = scale.seconds * 1000
        return (timestampMs / scaleMs) * scaleMs
    }

    private data class MutableBucket(
        val symbol: String,
        val scale: DexKlineScale,
        val windowStartMs: Long,
        var open: BigDecimal,
        var high: BigDecimal,
        var low: BigDecimal,
        var close: BigDecimal,
        var volumeBase: BigDecimal,
        var tradeCount: Int
    ) {
        fun applyTrade(trade: DexTrade) {
            val price = trade.price
            if (tradeCount == 0) {
                open = price
                high = price
                low = price
            } else {
                if (price > high) {
                    high = price
                }
                if (price < low) {
                    low = price
                }
            }
            close = price
            volumeBase = volumeBase.add(trade.amountBase)
            tradeCount += 1
        }

        fun toImmutable(): DexKlineBucket =
            DexKlineBucket(
                symbol = symbol,
                scale = scale,
                windowStartMs = windowStartMs,
                open = open,
                high = high,
                low = low,
                close = close,
                volumeBase = volumeBase,
                tradeCount = tradeCount
            )
    }
}
