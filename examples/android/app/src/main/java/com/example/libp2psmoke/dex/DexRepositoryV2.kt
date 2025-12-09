package com.example.libp2psmoke.dex

import com.example.libp2psmoke.core.Constants
import com.example.libp2psmoke.core.DexError
import com.example.libp2psmoke.core.SecureLogger
import com.example.libp2psmoke.dex.BtcAddressType
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.withContext
import okhttp3.OkHttpClient
import java.math.BigDecimal
import java.math.BigInteger
import java.math.RoundingMode
import java.util.concurrent.TimeUnit
import java.util.UUID

/**
 * DEX 仓库 V2
 * 使用统一的错误处理和常量配置
 */
class DexRepositoryV2(
    private val httpClient: OkHttpClient = defaultHttpClient(),
    private val ioDispatcher: CoroutineDispatcher = Dispatchers.IO
) {
    companion object {
        private const val TAG = "DexRepository"
        
        private fun defaultHttpClient(): OkHttpClient = OkHttpClient.Builder()
            .retryOnConnectionFailure(true)
            .connectTimeout(Constants.Network.CONNECT_TIMEOUT_SECONDS, TimeUnit.SECONDS)
            .writeTimeout(Constants.Network.WRITE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
            .readTimeout(Constants.Network.READ_TIMEOUT_SECONDS, TimeUnit.SECONDS)
            .build()
    }
    
    // 客户端
    private val btcRpcClient = BtcRpcClientV2(httpClient)
    private val bscRpcClient = BscRpcClientV2(httpClient)
    private val btcWallet = BtcWalletManager()
    private val aggregator = DexKlineAggregator()
    private val atomicSwapManager = AtomicSwapManager()
    private val adapterSwapManager = AdapterSwapManager()
    private val btcHtlcService = BtcHtlcService(btcWallet)
    private val bscHtlcService = BscHtlcService()
    
    // 状态流
    private val _swaps = MutableStateFlow<List<DexSwapResult>>(emptyList())
    private val _btcBalance = MutableStateFlow(BigDecimal.ZERO)
    
    // Atomic Swap Manager
    val activeAtomicSwaps = atomicSwapManager.activeSwaps
    val activeAdapterSwap = adapterSwapManager.activeSwap
    
    val mpcSwapManager = MpcSwapManager()
    val activeMpcSwap = mpcSwapManager.state
    
    private lateinit var mpcPreferences: MpcPreferences
    
    fun init(context: android.content.Context) {
        mpcPreferences = MpcPreferences(context)
        mpcSwapManager.init(context)
    }
    
    // Internal State for matching engine (Simulated)
    private val partialLock = Any()
    private val partials = mutableMapOf<String, PartialFill>()
    private val _klines = MutableStateFlow<List<DexKlineBucket>>(emptyList())
    
    init {
        // Initialize simulated data
        startSimulation()
    }
    
    private fun startSimulation() {
        // Simulate some initial data if needed
        _btcBalance.value = BigDecimal("0.5")
    }
    
    // 公开状态
    fun swaps(): StateFlow<List<DexSwapResult>> = _swaps
    fun klines(): StateFlow<List<DexKlineBucket>> = _klines
    fun btcBalance(): StateFlow<BigDecimal> = _btcBalance.asStateFlow()
    

    /**
     * 提交 BTC 转账 (使用 WIF 私钥)
     */
    suspend fun submitBtcTransfer(
        wif: String,
        toAddress: String,
        amountSats: Long,
        orderId: String,
        addressType: BtcAddressType = BtcAddressType.P2PKH
    ): DexSwapResult = withContext(ioDispatcher) {
        val amountDisplay = "${formatBtc(amountSats)} BTC"
        
        SecureLogger.i(TAG, "提交 BTC 转账: ${SecureLogger.maskAddress(toAddress)}, $amountDisplay")
        
        // 记录 Pending 状态
        recordSwap(DexSwapResult(
            chain = DexChain.BTC_TESTNET,
            orderId = orderId,
            status = DexSwapStatus.Pending,
            amountDisplay = amountDisplay,
            message = "正在广播交易..."
        ))
        
        try {
            val txHash = btcWallet.sendBtc(wif, toAddress, amountSats, addressType)
            
            val result = DexSwapResult(
                chain = DexChain.BTC_TESTNET,
                orderId = orderId,
                status = DexSwapStatus.Confirmed,
                amountDisplay = amountDisplay,
                txHash = txHash,
                message = "交易已广播到测试网"
            )
            
            recordSwap(result)
            recordBtcPartial(orderId, amountSats, txHash)
            
            // 更新余额
            _btcBalance.update { current ->
                current.subtract(BigDecimal(amountSats).movePointLeft(Constants.Btc.DECIMAL_PLACES))
            }
            
            SecureLogger.i(TAG, "BTC 转账成功: ${SecureLogger.maskTxHash(txHash)}")
            result
            
        } catch (err: Exception) {
            val error = DexError.from(err)
            
            val failure = DexSwapResult(
                chain = DexChain.BTC_TESTNET,
                orderId = orderId,
                status = DexSwapStatus.Failed,
                amountDisplay = amountDisplay,
                message = error.toUserMessage()
            )
            
            recordSwap(failure)
            SecureLogger.e(TAG, "BTC 转账失败", err)
            throw error
        }
    }
    
    /**
     * 提交 BTC 交换 (使用 RPC)
     */
    suspend fun submitBtcSwap(
        config: BtcRpcConfig,
        request: BtcSwapRequest
    ): DexSwapResult = withContext(ioDispatcher) {
        val amountDisplay = "${formatBtc(request.amountSats)} BTC"
        
        SecureLogger.i(TAG, "提交 BTC 交换: ${SecureLogger.maskAddress(request.targetAddress)}")
        
        recordSwap(DexSwapResult(
            chain = DexChain.BTC_TESTNET,
            orderId = request.orderId,
            status = DexSwapStatus.Pending,
            amountDisplay = amountDisplay,
            message = "正在构建 PSBT..."
        ))
        
        try {
            val txHash = btcRpcClient.submitSwap(config, request)
            
            val result = DexSwapResult(
                chain = DexChain.BTC_TESTNET,
                orderId = request.orderId,
                status = DexSwapStatus.Confirmed,
                amountDisplay = amountDisplay,
                txHash = txHash,
                message = "交易已广播到测试网"
            )
            
            recordSwap(result)
            recordBtcPartial(request.orderId, request.amountSats, txHash)
            
            _btcBalance.update { current ->
                current.add(BigDecimal(request.amountSats).movePointLeft(Constants.Btc.DECIMAL_PLACES))
            }
            
            result
            
        } catch (err: Exception) {
            val error = DexError.from(err)
            
            val failure = DexSwapResult(
                chain = DexChain.BTC_TESTNET,
                orderId = request.orderId,
                status = DexSwapStatus.Failed,
                amountDisplay = amountDisplay,
                message = error.toUserMessage()
            )
            
            recordSwap(failure)
            throw error
        }
    }
    
    /**
     * 提交 BSC USDC 转账
     */
    suspend fun submitBscTransfer(
        request: BscTransferRequest
    ): DexSwapResult = withContext(ioDispatcher) {
        val amountDisplay = "${formatUsdc(request.amount, request.decimals)} USDC"
        
        SecureLogger.i(TAG, "提交 BSC 转账: ${SecureLogger.maskAddress(request.toAddress)}")
        
        recordSwap(DexSwapResult(
            chain = DexChain.BSC_TESTNET,
            orderId = request.orderId,
            status = DexSwapStatus.Pending,
            amountDisplay = amountDisplay,
            message = "正在签名 ERC-20 转账..."
        ))
        
        try {
            val txHash = bscRpcClient.submitTransfer(request)
            
            val result = DexSwapResult(
                chain = DexChain.BSC_TESTNET,
                orderId = request.orderId,
                status = DexSwapStatus.Confirmed,
                amountDisplay = amountDisplay,
                txHash = txHash,
                message = "交易已提交到 BSC 测试网"
            )
            
            recordSwap(result)
            recordBscPartial(request.orderId, request.amount, request.decimals, txHash)
            
            SecureLogger.i(TAG, "BSC 转账成功: ${SecureLogger.maskTxHash(txHash)}")
            result
            
        } catch (err: Exception) {
            val error = DexError.from(err)
            
            val failure = DexSwapResult(
                chain = DexChain.BSC_TESTNET,
                orderId = request.orderId,
                status = DexSwapStatus.Failed,
                amountDisplay = amountDisplay,
                message = error.toUserMessage()
            )
            
            recordSwap(failure)
            throw error
        }
    }
    
    /**
     * 注入 K线数据
     */
    fun ingestBucket(bucket: DexKlineBucket) {
        _klines.update { current ->
            val filtered = current.filterNot { 
                it.windowStartMs == bucket.windowStartMs && it.scale == bucket.scale 
            }
            (listOf(bucket) + filtered)
                .sortedWith(compareByDescending<DexKlineBucket> { it.windowStartMs }.thenBy { it.scale.seconds })
                .take(Constants.Cache.MAX_KLINE_DATA_POINTS)
        }
    }
    
    /**
     * 注入撮合成交
     */
    fun ingestMatch(orderId: String, price: BigDecimal, amount: BigDecimal, asset: String) {
        val trade = DexTrade(
            orderId = orderId,
            symbol = asset,
            price = price,
            amountBase = amount,
            timestampMs = System.currentTimeMillis()
        )
        publishTrade(trade)
    }
    
    /**
     * 估算 USDC 可换 BTC 数量
     */
    fun estimateBtcFromUsdc(
        amount: BigInteger,
        decimals: Int,
        price: BigDecimal = Constants.Btc.REFERENCE_PRICE_USD
    ): Long {
        if (amount <= BigInteger.ZERO || price <= BigDecimal.ZERO) return 0L
        
        val usdc = BigDecimal(amount).movePointLeft(decimals)
        val btcAmount = usdc.divide(price, Constants.Btc.DECIMAL_PLACES, RoundingMode.HALF_UP)
        val sats = btcAmount.movePointRight(Constants.Btc.DECIMAL_PLACES).setScale(0, RoundingMode.HALF_UP)
        
        return try {
            sats.longValueExact()
        } catch (_: ArithmeticException) {
            Long.MAX_VALUE
        }
    }
    
    /**
     * 获取 BSC 钱包余额
     */
    suspend fun getBscBalance(
        rpcUrl: String,
        tokenContract: String,
        walletAddress: String
    ): BigInteger = withContext(ioDispatcher) {
        bscRpcClient.getTokenBalance(rpcUrl, tokenContract, walletAddress)
    }

    /**
     * 获取 BTC 测试网余额 (P2WPKH)
     */
    suspend fun getBtcBalance(address: String): BigDecimal = withContext(ioDispatcher) {
        val balance = btcWallet.getBalance(address)
        _btcBalance.value = balance
        balance
    }

    /**
     * Initiate Atomic Swap (Step 1)
     */
    suspend fun initiateAtomicSwap(
        isBuy: Boolean,
        amount: String,
        counterpartyAddress: String
    ): AtomicSwapState = withContext(ioDispatcher) {
        val role = if (isBuy) SwapRole.INITIATOR else SwapRole.PARTICIPANT
        val pair = if (isBuy) "USDC-BTC" else "BTC-USDC"
        
        val state = atomicSwapManager.createSwap(role, counterpartyAddress, amount, pair)
        
        // TODO: Implement actual contract calls here based on role
        // For now, we simulate the state transition
        
        if (role == SwapRole.INITIATOR) {
            // Step 1: Initiator locks USDC on BSC
            // For simulation/demo purposes, we will assume this succeeds immediately 
            // and update the state. In a real app, this would call bscRpcClient.lock(...)
            
            SecureLogger.i(TAG, "Initiating Swap: Locking USDC on BSC...")
            
            try {
                // Call BscHtlcService to lock USDC
                // For demo, we use a dummy private key if not provided (or use the one from config)
                val txHash = bscHtlcService.lockUsdc(
                    privateKey = com.example.libp2psmoke.BuildConfig.DEV_PRIVATE_KEY,
                    recipientAddress = counterpartyAddress,
                    secretHash = state.secretHash,
                    timeout = 100, // 100 blocks ~ 5 mins on BSC? No, 100 blocks is ~5 mins on BSC (3s block time)
                    amount = java.math.BigDecimal(amount).movePointRight(18).toBigInteger()
                )
                
                atomicSwapManager.updateSwapStatus(state.id, SwapStatus.BSC_LOCKED, txHash)
                
                // Auto-transition for demo: Simulate Counterparty (LP) locking BTC
                // In a real P2P app, we would wait for a P2P message here.
                simulateCounterpartyAction(state)
                
            } catch (e: Exception) {
                SecureLogger.e(TAG, "Failed to lock USDC", e)
                atomicSwapManager.updateSwapStatus(state.id, SwapStatus.FAILED, e.message)
                throw e
            }
        }
        
        state
    }
    
    private suspend fun simulateCounterpartyAction(state: AtomicSwapState) {
        withContext(ioDispatcher) {
            kotlinx.coroutines.delay(5000) // Wait 5 seconds
            
            // LP verifies BSC lock and locks BTC
            SecureLogger.i(TAG, "Simulating LP: Locking BTC on Testnet...")
            
            // Use our own wallet to simulate the LP locking BTC to the user
            // In reality, this would be the LP's wallet
            try {
                // We need the user's pubkey (recipient)
                // For this demo, we'll generate a dummy one or use a fixed one if available
                // Ideally we should ask the user for their pubkey.
                // Here we just use a random byte array for the pubkey placeholder
                val userPubKey = ByteArray(33).apply { fill(1) } 
                
                val txHash = btcHtlcService.lockBtc(
                    wif = com.example.libp2psmoke.BuildConfig.DEFAULT_BTC_TESTNET_WIF, // LP uses default wallet
                    recipientPubKey = userPubKey,
                    secretHash = state.secretHash,
                    timeout = 100, // blocks
                    amountSats = 10000 // Fixed amount for demo
                )
                
                atomicSwapManager.updateSwapStatus(state.id, SwapStatus.BTC_LOCKED, txHash)
                SecureLogger.i(TAG, "LP Locked BTC: $txHash")
                
            } catch (e: Exception) {
                SecureLogger.e(TAG, "LP Lock Failed", e)
            }
        }
    }

    /**
     * Initiate Adapter Swap (Privacy)
     */
    suspend fun initiateAdapterSwap(
        isBuy: Boolean,
        amount: String,
        counterpartyAddress: String
    ) = withContext(ioDispatcher) {
        adapterSwapManager.createSwap("Initiator (Alice)")
        adapterSwapManager.updateStatus(AdapterSwapStatus.SETUP, "Generating Adaptor Signature...")
        
        kotlinx.coroutines.delay(1000)
        
        // 1. Generate Secret & Point
        val secret = AdapterCrypto.generateSecret()
        val point = AdapterCrypto.computePaymentPoint(secret)
        
        // 2. Create Adaptor Signature (Mock)
        val adaptorSig = AdapterCrypto.createAdaptorSignature(
            msg = "tx_btc_hash",
            privKey = "ALICE_PRIV_MOCK",
            paymentPoint = point
        )
        
        adapterSwapManager.updateStatus(
            AdapterSwapStatus.OFFER_SIG_CREATED, 
            "Adaptor Signature Created. Sending to Bob..."
        ) { it.copy(secret = secret, paymentPoint = point, adaptorSignature = adaptorSig) }
        
        kotlinx.coroutines.delay(1000)
        
        // 3. Simulate Bob Verifying and Locking BSC
        // Bob verifies adaptorSig and locks BSC assets to the Payment Point (conceptually)
        val bscTxHash = "0x" + UUID.randomUUID().toString().replace("-", "")
        adapterSwapManager.updateStatus(
            AdapterSwapStatus.BSC_LOCKED,
            "Counterparty Locked BSC Assets (Verified Sig)"
        ) { it.copy(bscTxHash = bscTxHash) }
        
        kotlinx.coroutines.delay(1500)
        
        // 4. Simulate Bob Redeeming BTC (Revealing Valid Signature)
        // Bob uses the secret (which he somehow got? No, wait. 
        // In this flow: Alice has BTC. Bob has BSC.
        // Alice wants BSC. Bob wants BTC.
        // 1. Alice creates Adaptor Sig for BTC tx -> Bob. Locked to Point P.
        // 2. Bob verifies and locks BSC to Point P (or sends to Alice with condition P).
        //    Actually, usually Bob signs a BSC tx to Alice, adaptor-signed with P? 
        //    Let's simplify: Bob locks BSC in a contract that releases to Alice if she reveals Secret.
        //    OR: Bob gives Alice an Adaptor Sig for BSC tx?
        //    Let's stick to the "Bob locks BSC" simulation.
        // 3. Bob needs the signature to broadcast BTC tx. But he doesn't have the secret yet.
        //    Wait, Alice gives Adaptor Sig to Bob. Bob cannot use it yet.
        //    Bob locks BSC. Alice sees BSC lock.
        //    Alice gives Secret to Bob? No, that's trusted.
        //    Alice claims BSC using Secret. Revealing Secret on BSC chain.
        //    Bob sees Secret on BSC chain. Bob completes BTC signature.
        
        // Revised Flow for Demo (Alice = BTC Sender, Bob = BSC Sender):
        // 1. Alice creates Adaptor Sig for BTC tx -> Bob. Locked to P. Sends to Bob.
        // 2. Bob verifies. Bob locks BSC assets in a contract: "If Alice reveals Secret matching P, she gets BSC".
        // 3. Alice claims BSC assets using Secret. This reveals Secret on-chain.
        // 4. Bob sees Secret. Bob completes Adaptor Sig -> Valid Sig. Broadcasts BTC tx.
        
        // Let's implement THIS flow.
        
        // Step 3: Alice Claims BSC (Reveals Secret)
        adapterSwapManager.updateStatus(
            AdapterSwapStatus.SECRET_EXTRACTED, // Using this state to mean "Secret Revealed on BSC"
            "Alice Claimed BSC (Revealed Secret)"
        ) { it.copy(message = "Claiming BSC...") }
        
        kotlinx.coroutines.delay(1000)
        
        // Step 4: Bob Completes BTC Tx
        val validSig = AdapterCrypto.completeSignatureMock(adaptorSig, secret)
        val btcTxHash = "tx_btc_" + UUID.randomUUID().toString().take(8)
        
        adapterSwapManager.updateStatus(
            AdapterSwapStatus.BTC_REDEEMED,
            "Bob Broadcasted BTC Tx (Using Secret)"
        ) { it.copy(validSignature = validSig, btcTxHash = btcTxHash) }
        
        kotlinx.coroutines.delay(1000)
        
        adapterSwapManager.updateStatus(
            AdapterSwapStatus.COMPLETED,
            "Swap Completed (Privacy Preserved)"
        )
    }
    
    /**
     * Initiate MPC Swap (Simulation)
     */
    suspend fun initiateMpcSwap(
        isBuy: Boolean,
        amount: String,
        counterpartyAddress: String
    ) {
        android.util.Log.e("DexRepo", "Entering initiateMpcSwap")
        withContext(ioDispatcher) {
            android.util.Log.e("DexRepo", "Inside withContext")
        mpcSwapManager.startKeyGen("Initiator", counterpartyAddress)
        kotlinx.coroutines.delay(1000)
        
        // Simulate Remote Peer (Authentic MPC)
        SecureLogger.d(TAG, "initiateMpcSwap: Simulating Remote Share...")
        
        // Check for existing Remote Secret (Simulation Persistence)
        var remoteSecret = mpcPreferences.getRemoteSecret()
        var remotePub = ""
        
        if (!remoteSecret.isNullOrEmpty()) {
             // We have a stored remote secret. We need the PubKey too. 
             // Ideally we store RemotePub too, but we can re-derive or just store it.
             // Actually, for MPC simulation, we need to re-run KeyGen OR store PubShare.
             // Let's assume we start fresh if we don't have both.
             // But wait, the Local party has the "RemotePub" stored.
             // The Remote party handles its own storage.
             // Let's re-generate Pub from Secret? No function for that exposed easily.
             // Simplest: Store RemotePub for simulation too.
             // Or... just re-run KeyGen? No, KeyGen creates NEW secrets.
             // Force user to Reset if they want new keys.
             
             // Hack: For simulation, we assume if Local has keys, Remote (Sim) should match.
             // But we need the ACTUAL remote secret to sign.
             // If we lost it, we can't sign.
        }

        if (remoteSecret.isNullOrEmpty()) {
            // 1. Generate Remote KeyShare (Fresh)
            val remoteInit = com.example.libp2psmoke.native.NimBridge.mpcKeygenInit()
            val rParts = remoteInit.split("|")
            remoteSecret = rParts[0]
            remotePub = rParts[1]
            
            // Save for next time
            mpcPreferences.saveRemoteSecret(remoteSecret!!)
            
            // Deliver to Local (Critical!)
            mpcSwapManager.onRemoteShareReceived(remotePub)
        } else {
            // We have secret, but we need pub to "send" to local?
            // If Local already KeyGen'd, it has RemotePub.
            // If Local is also skipping KeyGen, we don't need to send RemotePub.
            // We only need RemotePub if Local is waiting for it.
            
            // However, to keep it robust:
            // We can't easily restore RemotePub from Secret without Nim bridge support.
            // But we don't really need to send it if Local is ALREADY in KEYGEN_DONE state!
            // Local state check:
            if (mpcSwapManager.state.value.status != MpcSwapStatus.KEYGEN_DONE) {
               // This is bad. Local needs keys, but we have a stale Remote Secret?
               // This implies a mismatch state. 
               // If Local was reset but Remote wasn't -> Remote should probably generate fresh?
               // If Remote exists, Local should exist (since we save together).
               // Conclusion: Generate fresh if Local needs it.
               
                val remoteInit = com.example.libp2psmoke.native.NimBridge.mpcKeygenInit()
                val rParts = remoteInit.split("|")
                remoteSecret = rParts[0]
                remotePub = rParts[1]
                mpcPreferences.saveRemoteSecret(remoteSecret!!)
                mpcSwapManager.onRemoteShareReceived(remotePub)
            } else {
                SecureLogger.d(TAG, "initiateMpcSwap: Local has keys. Remote using stored secret.")
                // Local doesn't need onRemoteShareReceived.
                // BUT we need 'remotePub' for the simulation calculation later!
                remotePub = mpcSwapManager.state.value.remotePubShare
                SecureLogger.d(TAG, "initiateMpcSwap: Retrieved remotePub from Manager: $remotePub")
            }
        }
        
        // Wait for KeyGen to complete (Local state update)
        var kgAttempts = 0 
        while (mpcSwapManager.state.value.status != com.example.libp2psmoke.dex.MpcSwapStatus.KEYGEN_DONE && kgAttempts < 120) {
             if (mpcSwapManager.state.value.status == com.example.libp2psmoke.dex.MpcSwapStatus.FAILED) {
                 SecureLogger.e(TAG, "KeyGen Failed. Aborting swap.")
                 return@withContext
             }
             kotlinx.coroutines.delay(250)
             kgAttempts++
        }
        
        if (mpcSwapManager.state.value.status != com.example.libp2psmoke.dex.MpcSwapStatus.KEYGEN_DONE) {
             val currentStatus = mpcSwapManager.state.value.status
             SecureLogger.e(TAG, "KeyGen Timeout. Aborting. Status: $currentStatus")
             mpcSwapManager.fail("KeyGen Timeout (30s). Status: $currentStatus. Check Logs.")
             return@withContext 
        }
        
        SecureLogger.d(TAG, "initiateMpcSwap: Starting Signing...")
        mpcSwapManager.startSigning("MSG")
        kotlinx.coroutines.delay(1000)
        
        SecureLogger.d(TAG, "initiateMpcSwap: Simulating Remote Nonce...")
        
        // 2. Generate Remote Nonce
        val remoteSignInit = com.example.libp2psmoke.native.NimBridge.mpcSignInit()
        val nParts = remoteSignInit.split("|")
        val remoteNonce = nParts[0]
        val remoteNoncePub = nParts[1]
        
        mpcSwapManager.onRemoteNonceReceived(remoteNoncePub)
        kotlinx.coroutines.delay(1000)
        
        // 3. Wait for TxHash and Sign
        SecureLogger.d(TAG, "initiateMpcSwap: remote waiting for txHash...")
        var attempts = 0
        while (mpcSwapManager.state.value.txHash.isEmpty() && attempts < 30) {
            kotlinx.coroutines.delay(500)
            attempts++
        }
        
        val txHash = mpcSwapManager.state.value.txHash
        if (txHash.isEmpty()) {
            throw Exception("Remote Peer: Timeout waiting for txHash")
        }
        
        SecureLogger.d(TAG, "initiateMpcSwap: Simulating Remote Sig for $txHash")
        
        // 4. Compute Joint Stuff for Remote Context
        // Retrieve local parts from manager state to use as "remote" input for joint calculation
        val localPub = mpcSwapManager.state.value.localPubShare
        val localNoncePub = mpcSwapManager.state.value.localNoncePub
        
        // Note: mpcKeygenFinalize order matters if it was doing advanced stuff, but here it's just combining points.
        // Usually, protocol requires sorted parties or ID based.
        // Assuming symmetric or consistent ordering.
        val jointPub = com.example.libp2psmoke.native.NimBridge.mpcKeygenFinalize(localPub, remotePub)
        val jointNoncePub = com.example.libp2psmoke.native.NimBridge.mpcKeygenFinalize(localNoncePub, remoteNoncePub)
        
        // Match MpcSwapManager behavior: Clean 0x prefix!
        val cleanTxHash = if (txHash.startsWith("0x")) txHash.substring(2) else txHash
        
        val remoteSig = com.example.libp2psmoke.native.NimBridge.mpcSignPartial(
            cleanTxHash,
            remoteSecret,
            remoteNonce,
            jointPub,
            jointNoncePub
        )
        
        mpcSwapManager.onRemoteSigReceived(remoteSig)
        
        SecureLogger.d(TAG, "initiateMpcSwap: Completed.")
    }
}

    /**
     * Claim BTC (Step 3: Reveal Secret)
     */
    suspend fun claimAtomicSwap(swapId: String): String = withContext(ioDispatcher) {
        val swap = atomicSwapManager.getSwap(swapId) ?: throw Exception("Swap not found")
        
        if (swap.status != SwapStatus.BTC_LOCKED) {
            throw Exception("Swap not in BTC_LOCKED state")
        }
        
        if (swap.secret == null) {
            throw Exception("Secret not found (Are you the initiator?)")
        }
        
        // In a real app, we would fetch the funding tx details from the blockchain or indexer
        // For this demo, we use the txHash from the state (which is the funding tx)
        val fundingTxHash = swap.transactions.lastOrNull() ?: throw Exception("Funding Tx not found")
        
        // Simulate Redeem
        val redeemTx = btcHtlcService.redeemBtc(
            wif = com.example.libp2psmoke.BuildConfig.DEFAULT_BTC_TESTNET_WIF,
            secret = swap.secret,
            senderPubKey = ByteArray(33), // Placeholder
            timeout = 100,
            txHash = fundingTxHash,
            outputIndex = 0,
            amountSats = 10000
        )
        
        atomicSwapManager.updateSwapStatus(swapId, SwapStatus.BTC_REDEEMED, redeemTx)
        
        // Also update balance to reflect the "received" funds (simulated)
        _btcBalance.update { it.add(BigDecimal("0.0001")) }
        
        redeemTx
    }
    
    // ═══════════════════════════════════════════════════════════════════
    // 私有方法
    // ═══════════════════════════════════════════════════════════════════
    
    private fun recordSwap(result: DexSwapResult) {
        _swaps.update { current ->
            val filtered = current.filterNot { 
                it.orderId == result.orderId && it.chain == result.chain 
            }
            (listOf(result) + filtered).take(Constants.Ui.MAX_SWAP_HISTORY)
        }
    }
    
    private fun recordBtcPartial(orderId: String, amountSats: Long, txHash: String) {
        val trade = synchronized(partialLock) {
            val fill = partials.getOrPut(orderId) { PartialFill() }
            fill.btcAmountSats = amountSats
            fill.btcTxHash = txHash
            fill.updatedAtMs = System.currentTimeMillis()
            prepareTradeIfReady(orderId, fill)
        }
        trade?.let { publishTrade(it) }
    }
    
    private fun recordBscPartial(orderId: String, amount: BigInteger, decimals: Int, txHash: String) {
        val trade = synchronized(partialLock) {
            val fill = partials.getOrPut(orderId) { PartialFill() }
            fill.usdcAmount = amount
            fill.usdcDecimals = decimals
            fill.usdcTxHash = txHash
            fill.updatedAtMs = System.currentTimeMillis()
            prepareTradeIfReady(orderId, fill)
        }
        trade?.let { publishTrade(it) }
    }
    
    private fun prepareTradeIfReady(orderId: String, fill: PartialFill): DexTrade? {
        cleanupPartialsLocked()
        
        val btc = fill.btcAmountSats ?: return null
        val usdc = fill.usdcAmount ?: return null
        val decimals = fill.usdcDecimals ?: return null
        
        val amountBase = BigDecimal(btc).movePointLeft(Constants.Btc.DECIMAL_PLACES)
        val amountQuote = BigDecimal(usdc).movePointLeft(decimals)
        
        if (amountBase <= BigDecimal.ZERO) return null
        
        val price = amountQuote.divide(amountBase, Constants.Btc.DECIMAL_PLACES, RoundingMode.HALF_UP)
        partials.remove(orderId)
        
        return DexTrade(
            orderId = orderId,
            symbol = Constants.Dex.DEFAULT_SYMBOL,
            price = price,
            amountBase = amountBase,
            timestampMs = System.currentTimeMillis()
        )
    }
    
    private fun publishTrade(trade: DexTrade) {
        val snapshot = aggregator.ingest(trade)
        _klines.value = snapshot
    }
    
    private fun cleanupPartialsLocked() {
        val cutoff = System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(Constants.Cache.PARTIAL_FILL_EXPIRE_MINUTES)
        partials.entries.removeIf { it.value.updatedAtMs < cutoff }
    }
    
    private fun formatBtc(amountSats: Long): String =
        BigDecimal(amountSats)
            .movePointLeft(Constants.Btc.DECIMAL_PLACES)
            .stripTrailingZeros()
            .toPlainString()
    
    private fun formatUsdc(amount: BigInteger, decimals: Int): String =
        BigDecimal(amount)
            .movePointLeft(decimals)
            .stripTrailingZeros()
            .toPlainString()
    
    // ═══════════════════════════════════════════════════════════════════
    // 内部数据类
    // ═══════════════════════════════════════════════════════════════════
    
    private data class PartialFill(
        var btcAmountSats: Long? = null,
        var btcTxHash: String? = null,
        var usdcAmount: BigInteger? = null,
        var usdcDecimals: Int? = null,
        var usdcTxHash: String? = null,
        var updatedAtMs: Long = System.currentTimeMillis()
    )
}

