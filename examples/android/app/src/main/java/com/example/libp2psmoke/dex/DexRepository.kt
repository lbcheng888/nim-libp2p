package com.example.libp2psmoke.dex

import java.math.BigDecimal
import java.math.BigInteger
import java.math.RoundingMode
import java.util.concurrent.TimeUnit
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.withContext
import okhttp3.OkHttpClient

class DexRepository(
    private val httpClient: OkHttpClient = defaultHttpClient(),
    private val ioDispatcher: CoroutineDispatcher = Dispatchers.IO
) {
    private val btcClient = BtcRpcClient(httpClient)
    private val bscClient = BscRpcClient(httpClient)
    private val aggregator = DexKlineAggregator()
    private val swapState = MutableStateFlow<List<DexSwapResult>>(emptyList())
    private val klineState = MutableStateFlow<List<DexKlineBucket>>(emptyList())
    private val btcBalance = MutableStateFlow(BigDecimal.ZERO)
    private val partials = mutableMapOf<String, DexPartialFill>()
    private val partialLock = Any()

    private val referenceBtcUsdPrice = BigDecimal("68000")

    fun swaps(): StateFlow<List<DexSwapResult>> = swapState
    fun klines(): StateFlow<List<DexKlineBucket>> = klineState
    fun btcBalance(): StateFlow<BigDecimal> = btcBalance

    suspend fun submitBtcSwap(config: BtcRpcConfig, request: BtcSwapRequest): DexSwapResult =
        withContext(ioDispatcher) {
            val amountDisplay = "${formatBtc(request.amountSats)} BTC"
            recordSwap(
                DexSwapResult(
                    chain = DexChain.BTC_TESTNET,
                    orderId = request.orderId,
                    status = DexSwapStatus.Pending,
                    amountDisplay = amountDisplay,
                    message = "Building PSBT"
                )
            )
            try {
                val txHash = btcClient.submitSwap(config, request)
                val result = DexSwapResult(
                    chain = DexChain.BTC_TESTNET,
                    orderId = request.orderId,
                    status = DexSwapStatus.Confirmed,
                    amountDisplay = amountDisplay,
                    txHash = txHash,
                    message = "Broadcasted to testnet"
                )
                recordSwap(result)
                recordBtcPartial(request.orderId, request.amountSats, txHash)
                btcBalance.update { current ->
                    current + BigDecimal(request.amountSats).movePointLeft(8)
                }
                result
            } catch (err: Exception) {
                val failure = DexSwapResult(
                    chain = DexChain.BTC_TESTNET,
                    orderId = request.orderId,
                    status = DexSwapStatus.Failed,
                    amountDisplay = amountDisplay,
                    message = err.message
                )
                recordSwap(failure)
                throw err
            }
        }

    suspend fun submitBscTransfer(request: BscTransferRequest): DexSwapResult =
        withContext(ioDispatcher) {
            val amountDisplay = "${formatUsdc(request.amount, request.decimals)} USDC"
            recordSwap(
                DexSwapResult(
                    chain = DexChain.BSC_TESTNET,
                    orderId = request.orderId,
                    status = DexSwapStatus.Pending,
                    amountDisplay = amountDisplay,
                    message = "Signing ERC-20 transfer"
                )
            )
            try {
                val txHash = bscClient.submitTransfer(request)
                val result = DexSwapResult(
                    chain = DexChain.BSC_TESTNET,
                    orderId = request.orderId,
                    status = DexSwapStatus.Confirmed,
                    amountDisplay = amountDisplay,
                    txHash = txHash,
                    message = "Submitted to BSC testnet"
                )
                recordSwap(result)
                recordBscPartial(request.orderId, request.amount, request.decimals, txHash)
                result
            } catch (err: Exception) {
                val failure = DexSwapResult(
                    chain = DexChain.BSC_TESTNET,
                    orderId = request.orderId,
                    status = DexSwapStatus.Failed,
                    amountDisplay = amountDisplay,
                    message = err.message
                )
                recordSwap(failure)
                throw err
            }
        }

    private fun recordSwap(result: DexSwapResult) {
        swapState.update { current ->
            val filtered = current.filterNot { it.orderId == result.orderId && it.chain == result.chain }
            (listOf(result) + filtered).take(40)
        }
    }

    private fun recordBtcPartial(orderId: String, amountSats: Long, txHash: String) {
        val trade = synchronized(partialLock) {
            val fill = partials.getOrPut(orderId) { DexPartialFill() }
            fill.btcAmountSats = amountSats
            fill.btcTxHash = txHash
            fill.updatedAtMs = System.currentTimeMillis()
            prepareTradeIfReady(orderId, fill)
        }
        trade?.let { publishTrade(it) }
    }

    private fun recordBscPartial(orderId: String, amount: BigInteger, decimals: Int, txHash: String) {
        val trade = synchronized(partialLock) {
            val fill = partials.getOrPut(orderId) { DexPartialFill() }
            fill.usdcAmount = amount
            fill.usdcDecimals = decimals
            fill.usdcTxHash = txHash
            fill.updatedAtMs = System.currentTimeMillis()
            prepareTradeIfReady(orderId, fill)
        }
        trade?.let { publishTrade(it) }
    }

    private fun prepareTradeIfReady(orderId: String, fill: DexPartialFill): DexTrade? {
        cleanupPartialsLocked()
        val btc = fill.btcAmountSats ?: return null
        val usdc = fill.usdcAmount ?: return null
        val decimals = fill.usdcDecimals ?: return null
        val amountBase = BigDecimal(btc).movePointLeft(8)
        val amountQuote = BigDecimal(usdc).movePointLeft(decimals)
        if (amountBase.compareTo(BigDecimal.ZERO) <= 0) {
            return null
        }
        val price = amountQuote.divide(amountBase, 8, RoundingMode.HALF_UP)
        partials.remove(orderId)
        return DexTrade(
            orderId = orderId,
            symbol = DEFAULT_DEX_SYMBOL,
            price = price,
            amountBase = amountBase,
            timestampMs = System.currentTimeMillis()
        )
    }

    private fun publishTrade(trade: DexTrade) {
        val snapshot = aggregator.ingest(trade)
        klineState.value = snapshot
    }

    private fun cleanupPartialsLocked() {
        val cutoff = System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(30)
        val iterator = partials.entries.iterator()
        while (iterator.hasNext()) {
            val entry = iterator.next()
            if (entry.value.updatedAtMs < cutoff) {
                iterator.remove()
            }
        }
    }

    private fun formatBtc(amountSats: Long): String =
        BigDecimal(amountSats).movePointLeft(8).stripTrailingZeros().toPlainString()

    private fun formatUsdc(amount: BigInteger, decimals: Int): String =
        BigDecimal(amount).movePointLeft(decimals).stripTrailingZeros().toPlainString()

    fun estimateBtcFromUsdc(
        amount: BigInteger,
        decimals: Int,
        price: BigDecimal = referenceBtcUsdPrice
    ): Long {
        if (amount <= BigInteger.ZERO || price <= BigDecimal.ZERO) return 0L
        val usdc = BigDecimal(amount).movePointLeft(decimals)
        val btcAmount = usdc.divide(price, 8, RoundingMode.HALF_UP)
        val sats = btcAmount.movePointRight(8).setScale(0, RoundingMode.HALF_UP)
        return try {
            sats.longValueExact()
        } catch (_: ArithmeticException) {
            Long.MAX_VALUE
        }
    }

    companion object {
        private fun defaultHttpClient(): OkHttpClient =
            OkHttpClient.Builder()
                .retryOnConnectionFailure(true)
                .connectTimeout(15, TimeUnit.SECONDS)
                .writeTimeout(30, TimeUnit.SECONDS)
                .readTimeout(30, TimeUnit.SECONDS)
                .build()
    }
}
