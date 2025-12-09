package com.example.libp2psmoke.viewmodel

import android.app.Application
import androidx.lifecycle.AndroidViewModel
import androidx.lifecycle.viewModelScope
import com.example.libp2psmoke.core.Constants
import com.example.libp2psmoke.core.DexError
import com.example.libp2psmoke.core.SecureKeyStore
import com.example.libp2psmoke.core.SecureLogger
import com.example.libp2psmoke.dex.DexRepositoryV2
import com.example.libp2psmoke.dex.BINANCE_INTERVALS
import com.example.libp2psmoke.dex.BINANCE_SPOT_SYMBOL
import com.example.libp2psmoke.domain.MarketDataUseCase
import com.example.libp2psmoke.domain.P2PUseCase
import com.example.libp2psmoke.domain.WalletUseCase
import com.example.libp2psmoke.model.NodeUiState
import com.example.libp2psmoke.ui.UiIntent
import com.example.libp2psmoke.dex.BtcAddressType
import com.example.libp2psmoke.dex.BtcWalletManager
import com.example.libp2psmoke.dex.AtomicSwapState
import com.example.libp2psmoke.model.ChainWallet
import com.example.libp2psmoke.BuildConfig
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import java.io.File
import java.math.BigDecimal
import java.math.RoundingMode

/**
 * NimNodeViewModel V2 (MVI Architecture)
 */
class NimNodeViewModel(application: Application) : AndroidViewModel(application) {
    
    private val _state = MutableStateFlow(NodeUiState())
    val state: StateFlow<NodeUiState> = _state.asStateFlow()
    
    // 基础设施
    private val secureKeyStore = SecureKeyStore(application)
    private val dexRepository = DexRepositoryV2()
    
    // UseCases
    private val marketDataUseCase = MarketDataUseCase()
    private val walletUseCase = WalletUseCase(dexRepository, secureKeyStore)
    private val p2pUseCase = P2PUseCase(dexRepository, File(application.filesDir, "nimlibp2p_node"))
    
    // 状态透传
    val mixerSessions = p2pUseCase.mixerSessions
    
    // DEX State
    val dexSwaps = dexRepository.swaps()
    val activeAtomicSwaps = dexRepository.activeAtomicSwaps
    val activeAdapterSwap = dexRepository.activeAdapterSwap
    val activeMpcSwap = dexRepository.activeMpcSwap
    
    private var marketLoopJob: Job? = null
    
    init {
        dexRepository.init(getApplication())
        initialize()
    }
    
    fun onEvent(event: UiIntent) {
        SecureLogger.d(TAG, "收到意图: $event")
        when (event) {
            is UiIntent.SelectInterval -> handleSelectInterval(event.interval)
            is UiIntent.RefreshMarket -> handleRefreshMarket()
            is UiIntent.CalculateSwapEstimation -> handleCalculateSwap(event.amount, event.isBuy)
            is UiIntent.SubmitBridgeTrade -> handleSubmitBridgeTrade(event)
            is UiIntent.SubmitMultiChainSwap -> handleSubmitMultiChainSwap(event)
            is UiIntent.SubmitMixerIntent -> handleSubmitMixer(event)
            is UiIntent.ClaimAtomicSwap -> handleClaimAtomicSwap(event)
            is UiIntent.InitiateAdapterSwap -> handleInitiateAdapterSwap(event)
            is UiIntent.InitiateMpcSwap -> handleInitiateMpcSwap()
            is UiIntent.ClearError -> _state.update { it.copy(lastError = null) }
            is UiIntent.ClearSuccess -> _state.update { it.copy(successMessage = null) }
        }
    }
    
    // ... (其他处理逻辑保持不变)
    
    private fun handleSelectInterval(interval: String) {
        if (interval == _state.value.binanceInterval) return
        _state.update { it.copy(binanceInterval = interval, binanceLoading = true) }
        val cached = marketDataUseCase.getCachedKlines(interval)
        val cachedJson = marketDataUseCase.getCachedKlineJson(interval)
        if (cached != null && cachedJson != null) {
            _state.update { 
                it.copy(binanceKlines = cached, binanceKlinesJson = cachedJson, binanceLoading = false) 
            }
        }
        handleRefreshMarket()
    }
    
    private fun handleRefreshMarket() {
        viewModelScope.launch {
            val interval = _state.value.binanceInterval
            val result = marketDataUseCase.fetchMarketSnapshot(BINANCE_SPOT_SYMBOL, interval)
            if (result.isSuccess) {
                val snapshot = result.getOrThrow()
                _state.update {
                    it.copy(
                        binanceTicker = snapshot.ticker,
                        binanceKlines = snapshot.klines,
                        binanceKlinesJson = snapshot.klinesJson,
                        binanceBids = snapshot.bids,
                        binanceAsks = snapshot.asks,
                        marketSource = snapshot.source,
                        marketLatencyMs = snapshot.latencyMs,
                        binanceError = null,
                        binanceLoading = false,
                        binanceStreamConnected = true,
                        activeSwap = dexRepository.activeAtomicSwaps.value.values.firstOrNull()
                    )
                }
            } else {
                val error = result.exceptionOrNull()
                _state.update { 
                    it.copy(
                        binanceError = (error as? DexError)?.toUserMessage() ?: error?.message,
                        binanceStreamConnected = false,
                        binanceLoading = false
                    ) 
                }
            }
        }
    }
    
    private fun handleCalculateSwap(amountText: String, isBuy: Boolean) {
        val price = _state.value.binanceTicker?.lastPrice ?: BigDecimal.ZERO
        if (price <= BigDecimal.ZERO) {
            _state.update { it.copy(swapEstimation = "0") }
            return
        }
        val input = amountText.toBigDecimalOrNull() ?: BigDecimal.ZERO
        if (input <= BigDecimal.ZERO) {
            _state.update { it.copy(swapEstimation = "0") }
            return
        }
        val estimation = if (isBuy) {
            val btc = input.divide(price, 8, RoundingMode.HALF_UP)
            "${btc.stripTrailingZeros().toPlainString()} BTC"
        } else {
            val usdc = input.multiply(price).setScale(2, RoundingMode.HALF_UP)
            "${usdc.stripTrailingZeros().toPlainString()} USDC"
        }
        _state.update { it.copy(swapEstimation = estimation) }
    }
    
    private fun handleSubmitBridgeTrade(event: UiIntent.SubmitBridgeTrade) {
        viewModelScope.launch {
            _state.update { it.copy(bridgeLoading = true) }
            try {
                if (event.isBuy) {
                    SecureLogger.d(TAG, "Buy flow: 准备桥接 USDC -> BTC")
                    if (event.privateKey.isBlank()) {
                        updateError("请填写 BSC 私钥")
                        return@launch
                    }
                    if (event.targetAddress.isBlank()) {
                        updateError("请填写接收的 BTC 地址")
                        return@launch
                    }
                    val amount = walletUseCase.parseTokenAmount(event.amount, Constants.Bsc.USDC_DECIMALS)
                    if (amount == null) {
                        updateError("无效金额")
                        return@launch
                    }
                    val bridgeVault = "0x55d398326f99059fF775485246999027B3197955"
                    val currentPrice = _state.value.binanceTicker?.lastPrice ?: BigDecimal("65000")
                    
                    // Route based on SwapType
                    when (event.swapType) {
                        UiIntent.SwapType.MPC -> {
                            SecureLogger.d(TAG, "Initiating MPC Swap (Contract-less)")
                            dexRepository.initiateMpcSwap(
                                isBuy = true,
                                amount = event.amount,
                                counterpartyAddress = event.targetAddress
                            )
                            updateSuccess("MPC Swap Initiated! Waiting for peer...")
                        }
                        UiIntent.SwapType.ADAPTER -> {
                            SecureLogger.d(TAG, "Initiating Adapter Swap (Privacy)")
                            dexRepository.initiateAdapterSwap(
                                isBuy = true,
                                amount = event.amount,
                                counterpartyAddress = event.targetAddress
                            )
                            updateSuccess("Privacy Swap Initiated! Exchanging signatures...")
                        }
                        UiIntent.SwapType.ATOMIC -> {
                            SecureLogger.d(TAG, "Initiating Atomic Swap (Legacy)")
                            val state = dexRepository.initiateAtomicSwap(
                                isBuy = true,
                                amount = event.amount,
                                counterpartyAddress = event.targetAddress
                            )
                            updateSuccess("Atomic Swap Initiated! ID: ${state.id.take(8)}...")
                        }
                    }
                    
                    // Monitor swap state (Generic monitoring or specific based on type)
                    // For now, we rely on the UI observing the respective flows
                    
                } else {
                    if (event.privateKey.isBlank()) {
                        updateError("请填写 BTC 私钥(WIF)")
                        return@launch
                    }
                    if (event.targetAddress.isBlank()) {
                        updateError("请填写接收 USDC 的地址")
                        return@launch
                    }
                    val sats = event.amount.toBigDecimalOrNull()?.movePointRight(8)?.toLong() ?: 0L
                    val result = walletUseCase.sendBtc(
                        wifPrivateKey = event.privateKey,
                        toAddress = event.targetAddress, 
                        amountSats = sats,
                        orderId = "swap-${System.currentTimeMillis()}",
                        addressType = event.addressType
                    )
                    if (result.isSuccess) {
                        updateSuccess("BTC 已发送到桥接地址")
                    } else {
                        handleError(result.exceptionOrNull()!!)
                    }
                }
            } catch (e: Exception) {
                handleError(e)
            } finally {
                _state.update { it.copy(bridgeLoading = false) }
            }
        }
    }
    
    private fun handleSubmitMultiChainSwap(event: UiIntent.SubmitMultiChainSwap) {
        viewModelScope.launch {
            delay(1000)
            updateSuccess("跨链交换请求已提交: ${event.fromChain} -> ${event.toChain}")
        }
    }
    
    private fun handleSubmitMixer(event: UiIntent.SubmitMixerIntent) {
        val amount = event.amount.toDoubleOrNull()
        val hops = event.hops.toIntOrNull()
        if (amount != null && hops != null) {
            viewModelScope.launch {
                p2pUseCase.submitMixerIntent(event.asset, amount, hops)
                updateSuccess("隐私混币请求已提交 (Hops: $hops)")
            }
        } else {
            updateError("输入无效")
        }
    }
    
    private fun handleClaimAtomicSwap(event: UiIntent.ClaimAtomicSwap) {
        viewModelScope.launch {
            _state.update { it.copy(bridgeLoading = true) }
            try {
                val txHash = dexRepository.claimAtomicSwap(event.swapId)
                updateSuccess("Successfully Claimed BTC! Tx: $txHash")
            } catch (e: Exception) {
                handleError(e)
            } finally {
                _state.update { it.copy(bridgeLoading = false) }
            }
        }
    }

    private fun handleInitiateAdapterSwap(event: UiIntent.InitiateAdapterSwap) {
        viewModelScope.launch {
            try {
                _state.update { it.copy(bridgeLoading = true) }
                dexRepository.initiateAdapterSwap(
                    isBuy = true,
                    amount = event.amount,
                    counterpartyAddress = "legacy_button_address"
                )
                _state.update { it.copy(bridgeLoading = false, successMessage = "Adapter Swap Initiated!") }
            } catch (e: Exception) {
                _state.update { it.copy(bridgeLoading = false, lastError = "Swap Failed: ${e.message}") }
            }
        }
    }

    private fun handleInitiateMpcSwap() {
        viewModelScope.launch {
            try {
                dexRepository.initiateMpcSwap(
                    isBuy = true,
                    amount = "0.01",
                    counterpartyAddress = "legacy_button_address"
                )
                _state.update { it.copy(successMessage = "MPC Swap Started!") }
            } catch (e: Exception) {
                handleError(e)
            }
        }
    }
    
    // Initialization
    private fun initialize() {
        collectWalletState()
        collectP2PState()
        startP2PNode()
        startMarketDataLoop()
        startWalletLoop()
        prefetchMarketData()
    }
    
    private fun collectWalletState() {
        viewModelScope.launch {
            walletUseCase.btcBalance.collect { balance ->
                _state.update { 
                    val wallets = it.multiChainWallets.toMutableMap()
                    val btcWallet = wallets["BTC"]
                    if (btcWallet != null) {
                        wallets["BTC"] = btcWallet.copy(balance = balance)
                    }
                    it.copy(btcBalance = balance, multiChainWallets = wallets)
                }
            }
        }
        viewModelScope.launch {
            walletUseCase.swapHistory.collect { history ->
                _state.update { it.copy(dexSwaps = history) }
            }
        }
        // 初始化默认 BTC / BSC 钱包展示
        viewModelScope.launch(Dispatchers.IO) {
            val defaultBtc = ChainWallet(
                chainId = "BTC",
                address = BtcWalletManager.DEFAULT_ADDRESS,
                balance = walletUseCase.fetchBtcTestnetBalance(BtcWalletManager.DEFAULT_ADDRESS),
                usdcBalance = BigDecimal.ZERO,
                symbol = "BTC"
            )
            val defaultBscAddress = walletUseCase.deriveBscAddress(BuildConfig.DEV_PRIVATE_KEY) ?: "0x..."
            val defaultBscUsdc = walletUseCase.fetchBscUsdcBalance(
                address = defaultBscAddress,
                rpcUrl = Constants.Bsc.TESTNET_RPC_URLS.first(),
                contract = Constants.Bsc.USDC_CONTRACT
            )
            _state.update {
                val wallets = it.multiChainWallets.toMutableMap()
                wallets.putIfAbsent("BTC", defaultBtc)
                wallets["BSC"] = ChainWallet(
                    chainId = "BSC",
                    address = defaultBscAddress,
                    balance = BigDecimal.ZERO,
                    usdcBalance = defaultBscUsdc,
                    symbol = "USDC"
                )
                it.copy(multiChainWallets = wallets)
            }
        }
    }
    
    private fun collectP2PState() {
        viewModelScope.launch {
            p2pUseCase.p2pState.collect { p2p ->
                _state.update { 
                    it.copy(
                        running = p2p.isRunning,
                        localPeerId = p2p.localPeerId,
                        peerCount = p2p.connectedPeers
                    )
                }
            }
        }
    }
    
    private fun startMarketDataLoop() {
        marketLoopJob?.cancel()
        marketLoopJob = viewModelScope.launch {
            while (isActive) {
                handleRefreshMarket()
                delay(5000)
            }
        }
    }
    
    private fun startWalletLoop() {
        viewModelScope.launch(Dispatchers.IO) {
            while (isActive) {
                try {
                    // Fetch BTC Balance
                    walletUseCase.fetchBtcTestnetBalance(BtcWalletManager.DEFAULT_ADDRESS)
                    
                    // Fetch BSC Balance
                    val bscAddress = walletUseCase.deriveBscAddress(BuildConfig.DEV_PRIVATE_KEY)
                    if (bscAddress != null) {
                        val usdc = walletUseCase.fetchBscUsdcBalance(
                            address = bscAddress,
                            rpcUrl = Constants.Bsc.TESTNET_RPC_URLS.first(),
                            contract = Constants.Bsc.USDC_CONTRACT
                        )
                        _state.update { state ->
                            val wallets = state.multiChainWallets.toMutableMap()
                            val bscWallet = wallets["BSC"]?.copy(usdcBalance = usdc) 
                                ?: ChainWallet("BSC", bscAddress, BigDecimal.ZERO, usdc, "USDC")
                            wallets["BSC"] = bscWallet
                            state.copy(multiChainWallets = wallets)
                        }
                    }
                } catch (e: Exception) {
                    SecureLogger.w(TAG, "Wallet loop error", e)
                }
                delay(10000) // Poll every 10 seconds
            }
        }
    }
    
    private fun prefetchMarketData() {
        viewModelScope.launch(Dispatchers.IO) {
            marketDataUseCase.prefetchIntervals(BINANCE_SPOT_SYMBOL, BINANCE_INTERVALS)
        }
    }
    
    private fun startP2PNode() {
        viewModelScope.launch(Dispatchers.IO) {
            val bootstrap = listOf("/dnsaddr/bootstrap.libp2p.io") 
            p2pUseCase.startNode(bootstrap, emptyList())
        }
    }
    
    fun binanceIntervals() = BINANCE_INTERVALS
    
    private fun updateError(msg: String) {
        _state.update { it.copy(lastError = msg, successMessage = null) }
        viewModelScope.launch {
            delay(3000)
            _state.update { if (it.lastError == msg) it.copy(lastError = null) else it }
        }
    }
    
    private fun updateSuccess(msg: String) {
        _state.update { it.copy(successMessage = msg, lastError = null) }
        viewModelScope.launch {
            delay(3000)
            _state.update { if (it.successMessage == msg) it.copy(successMessage = null) else it }
        }
    }
    
    private fun handleError(t: Throwable) {
        val error = DexError.from(t)
        updateError(error.toUserMessage())
        SecureLogger.e(TAG, "Operation failed", t)
    }
    
    override fun onCleared() {
        super.onCleared()
        p2pUseCase.stopNode()
    }
    
    companion object {
        private const val TAG = "NimNodeViewModel"
    }
}

private val Constants.Bsc.USDC_CONTRACT get() = "0xE4140d73e9F09C5f783eC2BD8976cd8256A69AD0"
