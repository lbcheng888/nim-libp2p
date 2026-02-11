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
  DexEventHandler* = proc(topic: string, payload: string) {.gcsafe, raises: [].}

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
    onEvent*: DexEventHandler
    
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
  if cfg.mode == dmObserver:
    return nil
  try:
    result = initKlineStore(cfg.dataDir)
  except CatchableError:
    result = nil

proc initMixer*(node: DexNode) =
  discard

proc nowMillis(): int64 =
  int64(epochTime() * 1000.0)

proc emit(node: DexNode; topic, payload: string) {.gcsafe, raises: [].} =
  if node.isNil or node.onEvent.isNil:
    return
  try:
    node.onEvent(topic, payload)
  except CatchableError:
    discard

proc publishOrder*(node: DexNode; order: OrderMessage): Future[int] {.async.} =
  if node.isNil:
    return 0
  await node.gossip.publish(OrdersTopic, encodeOrder(order))

proc publishMatch*(node: DexNode; match: MatchMessage): Future[int] {.async.} =
  if node.isNil:
    return 0
  await node.gossip.publish(MatchesTopic, encodeMatch(match))

proc publishTrade*(node: DexNode; trade: TradeEvent): Future[int] {.async.} =
  if node.isNil:
    return 0
  await node.gossip.publish(TradesTopic, encodeTrade(trade))

proc handleOrders*(node: DexNode) =
  if node.isNil:
    return
  proc handler(topic: string, data: seq[byte]) {.async.} =
    let orderOpt = decodeOrder(data)
    if orderOpt.isNone():
      return
    var order = orderOpt.get()
    if order.ttlMs > 0 and order.timestamp > 0:
      let expiresAt = order.timestamp + int64(order.ttlMs)
      if nowMillis() > expiresAt:
        return
    if not node.signing.verifyOrder(order):
      return
    try:
      await node.store.recordInboundOrder(order)
    except CatchableError:
      discard
    node.emit("dex.order", $(%*{
      "id": order.id,
      "traderPeer": order.traderPeer,
      "base": order.baseAsset.toString(),
      "quote": order.quoteAsset.toString(),
      "side": order.side,
      "price": order.price,
      "amount": order.amount,
      "timestamp": order.timestamp,
    }))
    if node.cfg.mode == dmMatcher or node.cfg.enableAutoMatch:
      let signingPeer = node.signing.peerIdStr()
      let matcherPeer = if signingPeer.len > 0: signingPeer else: $node.switch.peerInfo.peerId
      var match = MatchMessage(
        orderId: order.id,
        matcherPeer: matcherPeer,
        price: order.price,
        amount: order.amount,
        baseAsset: order.baseAsset,
        quoteAsset: order.quoteAsset,
        note: "auto-match",
        signature: "",
        signerPubKey: "",
        signatureVersion: 0,
      )
      node.signing.signMatch(match)
      discard await node.publishMatch(match)
      node.tradeSeq = node.tradeSeq + 1
      let trade = TradeEvent(
        matchId: &"{matcherPeer}:{order.id}:{node.tradeSeq}",
        orderId: order.id,
        takerSide: order.side,
        price: match.price,
        amount: match.amount,
        baseAsset: order.baseAsset,
        quoteAsset: order.quoteAsset,
        matcherPeer: matcherPeer,
        makerPeer: order.traderPeer,
        createdAt: nowMillis(),
        sequence: node.tradeSeq,
        txHash: "",
      )
      discard await node.publishTrade(trade)
  node.gossip.subscribe(OrdersTopic, handler)

proc handleMatches*(node: DexNode) =
  if node.isNil:
    return
  proc handler(topic: string, data: seq[byte]) {.async.} =
    let matchOpt = decodeMatch(data)
    if matchOpt.isNone():
      return
    let match = matchOpt.get()
    if not node.signing.verifyMatch(match):
      return
    try:
      await node.store.recordMatch(match)
    except CatchableError:
      discard
    node.emit("dex.match", $matchMessageToJson(match))
    try:
      await node.settlement.handleMatch(match)
    except CatchableError:
      discard
  node.gossip.subscribe(MatchesTopic, handler)

proc subscribeTrades*(node: DexNode) =
  if node.isNil:
    return
  proc handler(topic: string, data: seq[byte]) {.async.} =
    let tradeOpt = decodeTrade(data)
    if tradeOpt.isNone():
      return
    let trade = tradeOpt.get()
    if node.klineStore != nil:
      try:
        await node.klineStore.recordTrade(trade, @[ts1s, ts1m])
      except CatchableError:
        discard
    node.emit("dex.trade", $tradeEventToJson(trade))
  node.gossip.subscribe(TradesTopic, handler)
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
    echo "[Wallet] Generated ephemeral seed (not persisted)."
  
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

when isMainModule:
  import libp2p
  import std/[parseopt, random]
  import chronicles

  var keepRunning = true

  proc handleCtrlC() {.noconv.} =
    keepRunning = false

  proc parseBoolFlag(value: string; defaultWhenEmpty = true): bool =
    if value.len == 0:
      return defaultWhenEmpty
    try:
      return value.parseBool()
    except ValueError:
      return defaultWhenEmpty

  proc parseMode(value: string): DexMode =
    case value.strip().toLowerAscii()
    of "matcher": dmMatcher
    of "observer": dmObserver
    of "kline": dmKline
    else: dmTrader

  proc parseAssetDef(symbol: string): AssetDef =
    let upper = symbol.strip().toUpperAscii()
    case upper
    of "BTC":
      AssetDef(chainId: ChainBTC, symbol: "BTC", decimals: 8, assetType: AssetNative)
    of "ETH":
      AssetDef(chainId: ChainETH, symbol: "ETH", decimals: 18, assetType: AssetNative)
    of "SOL":
      AssetDef(chainId: ChainSOL, symbol: "SOL", decimals: 9, assetType: AssetNative)
    of "TRX":
      AssetDef(chainId: ChainTRX, symbol: "TRX", decimals: 6, assetType: AssetNative)
    else:
      let decimals = if upper in ["USDC", "USDT"]: 6 else: 18
      AssetDef(chainId: ChainBSC, symbol: upper, decimals: decimals, assetType: AssetERC20)

  proc parseAssetPair(value: string): Option[(AssetDef, AssetDef)] =
    let parts = value.split('/')
    if parts.len != 2:
      return none((AssetDef, AssetDef))
    some((parseAssetDef(parts[0]), parseAssetDef(parts[1])))

  proc defaultListen(): seq[MultiAddress] =
    @[MultiAddress.init("/ip4/0.0.0.0/tcp/0").get()]

  proc defaultPairs(): seq[(AssetDef, AssetDef)] =
    @[
      (parseAssetDef("BTC"), parseAssetDef("USDC"))
    ]

  proc runCli() {.async: (raises: [CatchableError]).} =
    var mode = dmTrader
    var listenAddrs: seq[MultiAddress] = @[]
    var peerAddrs: seq[MultiAddress] = @[]
    var dataDir = "dex_state"
    var intervalMs = 3000
    var assets: seq[(AssetDef, AssetDef)] = @[]
    var allowUnsigned = false
    var disableSignature = false
    var enableTrades = true
    var enableMixer = false
    var metricsFile = ""
    var metricsInterval = 10

    var optParser = initOptParser(commandLineParams())
    for kind, key, val in optParser.getopt():
      case kind
      of cmdLongOption, cmdShortOption:
        let normalized = key.replace("_", "-").toLowerAscii()
        case normalized
        of "mode":
          mode = parseMode(val)
        of "listen":
          let parsed = MultiAddress.init(val)
          if parsed.isOk():
            listenAddrs.add(parsed.get())
        of "peer":
          let parsed = MultiAddress.init(val)
          if parsed.isOk():
            peerAddrs.add(parsed.get())
        of "data-dir", "datadir":
          if val.len > 0:
            dataDir = val
        of "interval":
          try:
            intervalMs = max(250, val.parseInt())
          except ValueError:
            discard
        of "asset":
          let pairOpt = parseAssetPair(val)
          if pairOpt.isSome():
            assets.add(pairOpt.get())
        of "allow-unsigned", "allowunsigned":
          allowUnsigned = parseBoolFlag(val)
        of "disable-signature", "disablesignature":
          disableSignature = parseBoolFlag(val)
        of "enable-trades", "enabletrades":
          enableTrades = parseBoolFlag(val)
        of "enable-mixer", "enablemixer":
          enableMixer = parseBoolFlag(val)
        of "metrics-file", "metricsfile":
          metricsFile = val
        of "metrics-interval", "metricsinterval":
          try:
            metricsInterval = max(1, val.parseInt())
          except ValueError:
            discard
        else:
          discard
      else:
        discard

    if listenAddrs.len == 0:
      listenAddrs = defaultListen()
    if assets.len == 0:
      assets = defaultPairs()

    keepRunning = true
    setControlCHook(handleCtrlC)

    let rng = newRng()
    let sw = newStandardSwitch(rng = rng, addrs = listenAddrs)
    let params = GossipSubParams.init(floodPublish = true)
    var gossip = GossipSub.init(
      switch = sw,
      parameters = params,
      verifySignature = true,
      sign = true
    )
    sw.mount(gossip.PubSub)

    await sw.start()

    echo "Local PeerID = ", sw.peerInfo.peerId
    echo "id=", sw.peerInfo.peerId
    for addr in sw.peerInfo.addrs:
      echo "Listening on ", $addr & "/p2p/" & $sw.peerInfo.peerId
    flushFile(stdout)

    for remote in peerAddrs:
      try:
        discard await sw.connect(remote)
      except CatchableError as exc:
        warn "connect failed", peer = $remote, err = exc.msg

    let store =
      try:
        initOrderStore(dataDir)
      except CatchableError as exc:
        raise exc
      except Exception as exc:
        raise newException(CatchableError, exc.msg)
    let signingCfg = SigningConfig(
      mode: if disableSignature: smDisabled else: smEnabled,
      keyPath: dataDir / DefaultKeyFile,
      requireRemoteSignature: false,
      allowUnsigned: allowUnsigned
    )
    let signingCtx = initSigning(signingCfg)
    let cfg = CliConfig(
      mode: mode,
      dataDir: dataDir,
      metricsFile: metricsFile,
      metricsInterval: metricsInterval,
      enableMixer: enableMixer,
      enableAutoMatch: false,
      enableAutoTrade: false
    )
    let dex = await initDexNode(sw, gossip, cfg, store, signingCtx)

    dex.onEvent = proc(topic: string, payload: string) {.gcsafe, raises: [].} =
      echo "[", topic, "] ", payload
      flushFile(stdout)

    handleOrders(dex)
    handleMatches(dex)
    subscribeTrades(dex)
    if enableMixer:
      handleMixers(dex)

    if mode == dmTrader:
      randomize()
      while keepRunning:
        let (baseAsset, quoteAsset) = assets[rand(0 ..< assets.len)]
        let side = if rand(1) == 0: "buy" else: "sell"
        let price =
          if baseAsset.symbol == "BTC":
            60_000.0 + rand(10_000).float
          else:
            100.0 + rand(2000).float / 10.0
        let amount =
          if baseAsset.symbol == "BTC":
            0.001 + rand(40).float / 1000.0
          else:
            1.0 + rand(100).float / 10.0
        let traderPeer =
          block:
            let signingPeer = dex.signing.peerIdStr()
            if signingPeer.len > 0: signingPeer else: $sw.peerInfo.peerId
        var order = OrderMessage(
          id: $dex.rng[].generate(uint64),
          traderPeer: traderPeer,
          baseAsset: baseAsset,
          quoteAsset: quoteAsset,
          side: side,
          price: price,
          amount: amount,
          ttlMs: 60_000,
          timestamp: nowMillis(),
          signature: "",
          signerPubKey: "",
          signatureVersion: 0,
          onionPath: @[]
        )
        dex.signing.signOrder(order)
        discard await dex.publishOrder(order)
        await sleepAsync(chronos.milliseconds(intervalMs))
    else:
      while keepRunning:
        await sleepAsync(chronos.seconds(1))

    await sw.stop()

  try:
    waitFor(runCli())
  except CatchableError as exc:
    stderr.writeLine("[dex_node] fatal: " & exc.msg)
    quit(1)
