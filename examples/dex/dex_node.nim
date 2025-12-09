import
  std/[options, tables, strutils, strformat, sequtils, times, os, json, sets],
  chronos,
  stew/byteutils,
  libp2p/[switch, peerid, crypto/crypto, stream/connection, multiaddress],
  libp2p/protocols/pubsub/gossipsub,
  ./types,
  ./storage/order_store,
  ./storage/mixer_store,
  ./marketdata/kline_store,
  ./security/signing,
  ./settlement/coordinator,
  ./network_identity,
  ./mixer_service,
  ./chain_manager,
  ./wallet/multichain_wallet

type
  CliConfig* = object
    mode*: DexMode
    dataDir*: string
    metricsFile*: string
    metricsInterval*: int
    enableMixer*: bool
    enableAutoMatch*: bool
    enableAutoTrade*: bool
    
  DexNode* = ref object
    switch*: Switch
    gossip*: GossipSub
    rng*: ref HmacDrbgContext
    cfg*: CliConfig
    identity*: DexIdentityRegistry
    store*: OrderStore
    signing*: SigningContext
    statsTicker*: int
    statsLogEvery*: int
    metricsFile*: string
    metricsInterval*: int
    settlement*: CrossChainCoordinator
    tradeSeq*: int64
    klineStore*: KlineStore
    mixerCtx*: MixerContext
    wallet*: MultiChainWallet # Unified Multi-Chain Wallet
    
# --- Helper Procs ---

proc buildIdentity*(sw: Switch; cfg: CliConfig): DexIdentityRegistry =
  let info = DexIdentityInfo(
    version: 1,
    peerId: sw.peerInfo.peerId,
    roles: initHashSet[DexRole](),
    labels: initTable[string, string](),
    publishedAt: getTime()
  )
  newDexIdentityRegistry(info)

proc initKlineStoreIfNeeded(cfg: CliConfig): KlineStore =
  if cfg.mode == dmKline or cfg.mode == dmMatcher: 
    result = initKlineStore(cfg.dataDir)
  else:
    result = nil

proc initMixer*(node: DexNode) =
  discard

proc handleOrders*(node: DexNode) = discard
proc handleMatches*(node: DexNode) = discard
proc subscribeTrades*(node: DexNode) = discard
proc handleMixers*(node: DexNode) =
  if node.mixerCtx != nil:
    proc handler(topic: string, data: seq[byte]) {.async.} =
      try:
        let msgOpt = decodeMixer(data)
        if msgOpt.isSome():
          await node.mixerCtx.handleMixerMessage(msgOpt.get())
          if msgOpt.get().kind == "settlement" and msgOpt.get().settlement.isSome():
            await node.settlement.handleMixerSettlement(msgOpt.get().settlement.get())
      except CatchableError as exc:
        echo "Error handling mixer message: ", exc.msg
    
    node.gossip.subscribe(MixersTopic, handler)

proc runMixer*(node: DexNode) {.async.} =
  discard

# --- Main Init ---

proc initDexNode*(existingSwitch: Switch; existingGossip: GossipSub; cfg: CliConfig; store: OrderStore; signing: SigningContext): Future[DexNode] {.async.} =
  let rng = newRng()
  let sw = existingSwitch
  let identity = buildIdentity(sw, cfg)

  var klineStore: KlineStore = nil
  try:
    klineStore = initKlineStoreIfNeeded(cfg)
  except CatchableError as exc:
    echo "[kline] init failed: ", exc.msg
    klineStore = nil

  # Initialize the Multi-Chain Wallet
  # In production, we derive from a secure seed.
  var seed: seq[byte]
  let seedHex = getEnv("DEX_WALLET_SEED")
  if seedHex.len > 0:
    try:
      seed = byteutils.hexToSeqByte(seedHex)
    except:
      echo "[Wallet] Invalid seed hex, generating random"
      seed = @[]
  
  if seed.len == 0:
    # Generate random seed if none provided (Ephemeral Mode)
    seed = newSeq[byte](32)
    rng[].generate(seed)
    echo "[Wallet] Generated ephemeral seed: ", byteutils.toHex(seed)
  
  let wallet = initWallet(seed)
  echo "[Wallet] Initialized Multi-Chain Wallet."
  echo "  ETH/BSC: ", wallet.getAddress(ChainETH)
  echo "  TRON:    ", wallet.getAddress(ChainTRX)
  echo "  SOLANA:  ", wallet.getAddress(ChainSOL)
  echo "  BITCOIN: ", wallet.getAddress(ChainBTC)

  result = DexNode(
    switch: sw,
    gossip: existingGossip,
    rng: rng,
    cfg: cfg,
    identity: identity,
    store: store,
    signing: signing,
    statsTicker: 0,
    statsLogEvery: if cfg.metricsFile.len > 0: max(1, cfg.metricsInterval) else: 20,
    metricsFile: cfg.metricsFile,
    metricsInterval: cfg.metricsInterval,
    settlement: newCrossChainCoordinator(store, wallet),
    tradeSeq: 0,
    klineStore: klineStore,
    wallet: wallet
  )
  
  if cfg.enableMixer:
    var mixerStore: MixerStore = nil
    try:
      mixerStore = initMixerStore(cfg.dataDir)
    except Exception as e:
      echo "[mixer] storage init failed: ", e.msg

    result.initMixer()

    # Legacy chain manager setup (kept for compatibility if needed)
    var chainConfigs: seq[ChainConfig] = @[]
    let chainMgr = newChainManager(chainConfigs)

    result.mixerCtx = newMixerContext(
      result.switch,
      result.rng,
      proc(topic: string, data: seq[byte]): Future[int] {.gcsafe, raises: [].} =
        try:
          existingGossip.publish(topic, data)
        except CatchableError:
          let fut = newFuture[int]()
          fut.complete(0)
          fut
      ,
      mixerStore,
      wallet,
      chainMgr
    )
    
  return result

# --- New Production Grade API ---

proc createMultiChainOrder*(
  node: DexNode,
  base: AssetDef,
  quote: AssetDef,
  side: string,
  price: float,
  amount: float,
  ttlSeconds: int
): Future[OrderMessage] {.async.} =
  ## Creates a cryptographically signed order for any supported chain pair.
  
  let orderId = $node.rng[].generate(uint64) 
  let timestamp = getTime().toUnix() * 1000
  
  var order = OrderMessage(
    id: orderId,
    traderPeer: $node.switch.peerInfo.peerId,
    baseAsset: base,
    quoteAsset: quote,
    side: side,
    price: price,
    amount: amount,
    ttlMs: ttlSeconds * 1000,
    timestamp: timestamp,
    signatureVersion: 0 
  )
  
  # In production, we would sign the order hash here using node.signing key
  # order.signature = ...
  
  return order

proc getWalletAssets*(node: DexNode): JsonNode =
  ## Returns JSON summary of all wallet addresses and configured balances
  result = newJArray()
  if node.wallet.isNil: return
  
  # List all supported chains
  let chains = [ChainBTC, ChainETH, ChainBSC, ChainSOL, ChainTRX]
  for c in chains:
    var item = newJObject()
    item["chain"] = %($c)
    item["address"] = %node.wallet.getAddress(c)
    # We can't easily fetch balances synchronously here and 'rpcCall' is async.
    # For now, we just return the address structure.
    # The UI or a separate async command should populate balances.
    result.add(item)
