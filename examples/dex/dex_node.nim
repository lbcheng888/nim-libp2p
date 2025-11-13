## Minimal DEX network demo built on nim-libp2p.
## Traders broadcast orders over GossipSub and matchers reply with fills.

import std/[json, options, os, parseopt, sequtils, strformat, strutils, tables, times]
import sets
import chronos
import libp2p
import libp2p/builders
import ../../libp2p/protocols/http/http as lpHttp
import libp2p/protocols/pubsub/gossipsub
import libp2p/peerinfo

import ./network_identity
import ./types
import ./storage/order_store
import ./security/signing
import ./settlement/coordinator
import ./marketdata/kline_store

proc ensureDir(path: string) =
  let (dir, _) = splitPath(path)
  if dir.len > 0 and not dirExists(dir):
    createDir(dir)

proc nowMs(): int64 =
  int64(getTime().toUnixFloat() * 1000.0)

type
  CliConfig = object
    mode: DexMode
    listenAddrs: seq[string]
    peers: seq[string]
    relayPeers: seq[string]
    assets: seq[string]
    intervalMs: int
    transports: seq[string]
    enableRelay: bool
    gossipFlood: bool
    identityRoles: seq[DexRole]
    identityLabels: Table[string, string]
    identityVersion: uint16
    requestIdentity: bool
    dataDir: string
    keyPath: string
    disableSignature: bool
    allowUnsigned: bool
    positionLimits: Table[string, float]
    metricsFile: string
    metricsInterval: int
    enableTrades: bool
    klineScales: seq[Timescale]
    klinePruneMinutes: int
    enableMixer: bool
    mixerIntervalMs: int
    mixerSlots: int

  DexNode = ref object
    switch: Switch
    gossip: GossipSub
    rng: ref HmacDrbgContext
    cfg: CliConfig
    identity: DexNetwork
    store: OrderStore
    signing: SigningContext
    statsTicker: int
    statsLogEvery: int = 20
    invalidOrders: int
    invalidMatches: int
    riskDrops: int
    expiredDrops: int
    positionLimits: Table[string, float]
    metricsFile: string
    metricsInterval: int
    settlement: CrossChainCoordinator
    tradeSeq: int64
    klineStore: KlineStore
    mixerService: lpHttp.HttpService
    mixerHandler: lpHttp.HttpHandler
    mixerSeq: int

proc parseBool(value: string, defaultVal: bool): bool =
  if value.len == 0:
    return defaultVal
  let lowered = value.toLowerAscii()
  case lowered
  of "true", "1", "yes", "on":
    true
  of "false", "0", "no", "off":
    false
  else:
    defaultVal

proc ensureRole(cfg: var CliConfig, role: DexRole) =
  if role notin cfg.identityRoles:
    cfg.identityRoles.add(role)

proc parseDexRole(value: string): Option[DexRole] =
  try:
    some(parseEnum[DexRole](value))
  except ValueError:
    none(DexRole)

proc parseIdentityLabel(cfg: var CliConfig, label: string) =
  if label.len == 0:
    return
  let parts = label.split("=", 1)
  if parts.len == 2 and parts[0].len > 0:
    cfg.identityLabels[parts[0]] = parts[1]

proc toRoleSet(roles: seq[DexRole]): RoleSet =
  var set = initHashSet[DexRole]()
  for role in roles:
    set.incl(role)
  set

proc applyModeRole(cfg: var CliConfig) =
  let needed =
    case cfg.mode
    of dmTrader:
      drTrader
    of dmMatcher:
      drMatcher
    of dmKline:
      drKline
    else:
      drObserver
  cfg.ensureRole(needed)

proc parseMode(value: string): DexMode =
  case value.toLowerAscii()
  of "trader":
    dmTrader
  of "matcher":
    dmMatcher
  of "kline":
    dmKline
  else:
    dmObserver

proc parseCli(): CliConfig =
  var cfg = CliConfig(
    mode: dmObserver,
    listenAddrs: @["/ip4/0.0.0.0/tcp/0"],
    peers: @[],
    relayPeers: @[],
    assets: @["ETH/USDT", "BTC/USDT"],
    intervalMs: 4000,
    transports: @["tcp"],
    enableRelay: false,
    gossipFlood: true,
    identityRoles: @[],
    identityLabels: initTable[string, string](),
    identityVersion: 1'u16,
    requestIdentity: true,
    dataDir: "dex_state",
    keyPath: "",
    disableSignature: false,
    allowUnsigned: false,
    positionLimits: initTable[string, float](),
    metricsFile: "",
    metricsInterval: 10,
    enableTrades: true,
    klineScales: @[ts1s, ts1m],
    klinePruneMinutes: 60,
    enableMixer: false,
    mixerIntervalMs: 15000,
    mixerSlots: 3,
  )
  cfg.identityLabels["env"] = "local"
  var customListen = false
  var parser = initOptParser()
  for kind, key, value in parser.getopt():
    case kind
    of cmdArgument:
      cfg.peers.add(key)
    of cmdShortOption, cmdLongOption:
      case key
      of "mode":
        cfg.mode = parseMode(value)
      of "listen":
        if not customListen:
          cfg.listenAddrs.setLen(0)
          customListen = true
        if value.len > 0:
          cfg.listenAddrs.add(value)
      of "peer":
        if value.len > 0:
          cfg.peers.add(value)
      of "asset":
        if value.len > 0:
          cfg.assets.add(value)
      of "interval":
        try:
          let parsed = parseInt(value)
          cfg.intervalMs = if parsed < 100: 100 else: parsed
        except ValueError:
          discard
      of "transport":
        if value.len > 0:
          cfg.transports.add(value)
      of "relay":
        if value.len > 0:
          cfg.relayPeers.add(value)
          cfg.enableRelay = true
      of "enablerelay":
        cfg.enableRelay = parseBool(value, true)
      of "gossipflood":
        cfg.gossipFlood = parseBool(value, cfg.gossipFlood)
      of "identity-role":
        let roleOpt = parseDexRole(value)
        if roleOpt.isSome():
          cfg.identityRoles.add(roleOpt.get())
      of "identity-label":
        cfg.parseIdentityLabel(value)
      of "identity-version":
        try:
          let parsed = parseInt(value)
          if parsed >= 0 and parsed <= high(uint16).int:
            cfg.identityVersion = uint16(parsed)
        except ValueError:
          discard
      of "request-identity":
        cfg.requestIdentity = parseBool(value, cfg.requestIdentity)
      of "data-dir":
        if value.len > 0:
          cfg.dataDir = value
      of "key-path":
        if value.len > 0:
          cfg.keyPath = value
      of "disable-signature":
        cfg.disableSignature = parseBool(value, true)
      of "allow-unsigned":
        cfg.allowUnsigned = parseBool(value, true)
      of "limit":
        if value.len > 0:
          let parts = value.split(":", 1)
          if parts.len == 2:
            try:
              let limitVal = parseFloat(parts[1])
              cfg.positionLimits[parts[0]] = limitVal
            except ValueError:
              echo "[cli] invalid limit: ", value
      of "metrics-file":
        cfg.metricsFile = value
      of "metrics-interval":
        try:
          cfg.metricsInterval = max(1, parseInt(value))
        except ValueError:
          discard
      of "enable-trades":
        cfg.enableTrades = parseBool(value, cfg.enableTrades)
      of "kline-scale":
        if value.len > 0:
          try:
            let parsed = parseInt(value)
            cfg.klineScales.add(Timescale(parsed))
          except ValueError:
            echo "[cli] invalid kline scale: ", value
      of "kline-prune-minutes":
        try:
          cfg.klinePruneMinutes = max(1, parseInt(value))
        except ValueError:
          discard
      of "enable-mixer":
        cfg.enableMixer = parseBool(value, true)
      of "mixer-interval":
        try:
          cfg.mixerIntervalMs = max(1000, parseInt(value))
        except ValueError:
          discard
      of "mixer-slots":
        try:
          cfg.mixerSlots = max(2, parseInt(value))
        except ValueError:
          discard
      else:
        discard
    of cmdEnd:
      discard
  cfg.assets = cfg.assets.filterIt(it.len > 0)
  if cfg.assets.len == 0:
    cfg.assets = @["ETH/USDT"]
  if cfg.transports.len == 0:
    cfg.transports = @["tcp"]
  if cfg.klineScales.len == 0:
    cfg.klineScales = @[ts1s, ts1m]
  cfg.applyModeRole()
  if cfg.identityRoles.len == 0:
    cfg.identityRoles.add(drObserver)
  if cfg.mode == dmKline:
    cfg.ensureRole(drKline)
  result = cfg

proc selectAsset(node: DexNode): string =
  if node.cfg.assets.len == 0:
    return "ETH/USDT"
  let idx = int(node.rng[].generate(uint32) mod uint32(node.cfg.assets.len))
  node.cfg.assets[idx]

proc selectSide(node: DexNode): string =
  if (node.rng[].generate(uint32) mod 2) == 0: "buy" else: "sell"

proc nextOrder(node: DexNode, counter: int): OrderMessage =
  OrderMessage(
    id: $node.switch.peerInfo.peerId & "-" & $counter,
    traderPeer: $node.switch.peerInfo.peerId,
    asset: node.selectAsset(),
    side: node.selectSide(),
    price: 100.0 + float(node.rng[].generate(uint16) mod 5000) / 10.0,
    amount: 1.0 + float(node.rng[].generate(uint16) mod 500) / 100.0,
    ttlMs: node.cfg.intervalMs * 2,
  )

proc matchFromOrder(node: DexNode, order: OrderMessage): MatchMessage =
  MatchMessage(
    orderId: order.id,
    matcherPeer: $node.switch.peerInfo.peerId,
    price: order.price,
    amount: order.amount,
    asset: order.asset,
    note: "synthetic-fill",
  )

proc buildSwitch(rng: ref HmacDrbgContext, cfg: CliConfig): Switch =
  var addresses: seq[MultiAddress]
  for addr in cfg.listenAddrs:
    let parsed = MultiAddress.init(addr)
    if parsed.isErr():
      raise newException(ValueError, "invalid listen address: " & addr)
    addresses.add(parsed.get())

  var builder = SwitchBuilder
    .new()
    .withRng(rng)
    .withAddresses(addresses)
    .withNoise()
    .withYamux()
    .withMplex()
    .withAutonat()
    .withAutonatV2()
    .withSignedPeerRecord()

  var transportSelected = false
  for raw in cfg.transports:
    let transport = raw.toLowerAscii()
    case transport
    of "tcp":
      builder = builder.withTcpTransport()
      transportSelected = true
    of "msquic", "quic":
      when compiles(builder.withMsQuicTransport()):
        builder = builder.withMsQuicTransport()
        transportSelected = true
      elif compiles(builder.withQuicTransport()):
        builder = builder.withQuicTransport()
        transportSelected = true
      else:
        raise newException(ValueError, "msquic/quic transport not available in this build")
    of "ws", "websocket":
      builder = builder.withWsTransport()
      transportSelected = true
    else:
      raise newException(ValueError, "unknown transport: " & raw)
  if not transportSelected:
    builder = builder.withTcpTransport()

  if cfg.enableRelay:
    builder = builder.withCircuitRelay()

  try:
    builder.build()
  except CatchableError as exc:
    raise newException(ValueError, "switch build failed: " & exc.msg)

proc connectToPeers(node: DexNode, targets: seq[string]) {.async.} =
  for target in targets:
    let parsed = parseFullAddress(target)
    if parsed.isErr():
      echo "[connect] invalid multiaddress: ", target
      continue
    let (peerId, base) = parsed.get()
    try:
      await node.switch.connect(peerId, @[base])
      echo "[connect] dialed ", peerId
      if node.cfg.requestIdentity:
        try:
          discard await requestIdentity(node.identity, peerId)
          echo "[identity] cached roles for ", peerId
        except CatchableError as exc:
          echo "[identity] ", peerId, " failed: ", exc.msg
    except CatchableError as exc:
      echo "[connect] ", peerId, " failed: ", exc.msg

proc logAddresses(node: DexNode) =
  echo "[peer] id=", node.switch.peerInfo.peerId
  for ma in node.switch.peerInfo.listenAddrs:
    echo "  listen: ", ma
  let full = node.switch.peerInfo.fullAddrs()
  if full.isOk():
    for ma in full.get():
      echo "  announce: ", ma

proc buildIdentity(sw: Switch, cfg: CliConfig): DexNetwork =
  var labels = cfg.identityLabels
  labels["mode"] = $cfg.mode
  let info = DexIdentityInfo(
    version: cfg.identityVersion,
    peerId: sw.peerInfo.peerId,
    roles: toRoleSet(cfg.identityRoles),
    labels: labels,
    publishedAt: getTime(),
  )
  let registry = newDexIdentityRegistry(info)
  var policy: DexAccessPolicy = initTable[string, RoleSet]()
  policy.allow(OrdersTopic, [drTrader, drMatcher])
  policy.allow(MatchesTopic, [drMatcher, drObserver, drKline])
  policy.allow(TradesTopic, [drMatcher, drKline, drObserver])
  let proto = DexIdentityProtocol.new(registry)
  sw.mount(proto)
  DexNetwork(
    switch: sw,
    registry: registry,
    identityProtocol: proto,
    accessPolicy: policy,
  )

proc initKlineStoreIfNeeded(cfg: CliConfig): KlineStore {.gcsafe, raises: [CatchableError].} =
  if cfg.mode != dmKline and cfg.enableTrades:
    return nil
  initKlineStore(cfg.dataDir)

proc startNode(cfg: CliConfig; store: OrderStore; signing: SigningContext): Future[DexNode] {.async.} =
  let rng = newRng()
  let sw = buildSwitch(rng, cfg)
  let identity = buildIdentity(sw, cfg)
  let gParams = GossipSubParams.init(floodPublish = cfg.gossipFlood)
  var gossip = GossipSub.init(
    switch = sw, parameters = gParams, verifySignature = true, sign = true
  )
  gossip.PubSub.triggerSelf = true
  sw.mount(gossip)

  await sw.start()
  await gossip.start()

  var klineStore: KlineStore = nil
  try:
    klineStore = initKlineStoreIfNeeded(cfg)
  except CatchableError as exc:
    echo "[kline] init failed: ", exc.msg
    klineStore = nil

  result = DexNode(
    switch: sw,
    gossip: gossip,
    rng: rng,
    cfg: cfg,
    identity: identity,
    store: store,
    signing: signing,
    statsTicker: 0,
    statsLogEvery: if cfg.metricsFile.len > 0: max(1, cfg.metricsInterval) else: 20,
    metricsFile: cfg.metricsFile,
    metricsInterval: cfg.metricsInterval,
    settlement: newCrossChainCoordinator(store),
    tradeSeq: 0,
    klineStore: klineStore,
  )
  logAddresses(result)
  let bootstrapPeers = cfg.peers & cfg.relayPeers
  if bootstrapPeers.len > 0:
    await result.connectToPeers(bootstrapPeers)
  if cfg.enableMixer:
    result.initMixer()

proc writeMetrics(node: DexNode; stats: OrderStoreStats) =
  try:
    ensureDir(node.metricsFile)
    var f = open(node.metricsFile, fmWrite)
    defer: f.close()
    f.writeLine("# HELP dex_invalid_orders_total Total invalid orders dropped")
    f.writeLine("# TYPE dex_invalid_orders_total counter")
    f.writeLine("dex_invalid_orders_total " & $node.invalidOrders)
    f.writeLine("# HELP dex_invalid_matches_total Total invalid matches dropped")
    f.writeLine("# TYPE dex_invalid_matches_total counter")
    f.writeLine("dex_invalid_matches_total " & $node.invalidMatches)
    f.writeLine("# HELP dex_risk_drops_total Orders dropped due to risk limits")
    f.writeLine("# TYPE dex_risk_drops_total counter")
    f.writeLine("dex_risk_drops_total " & $node.riskDrops)
    f.writeLine("# HELP dex_expired_orders_total Orders purged due to TTL expiration")
    f.writeLine("# TYPE dex_expired_orders_total counter")
    f.writeLine("dex_expired_orders_total " & $node.expiredDrops)
    f.writeLine("# HELP dex_asset_exposure Outstanding exposure per asset/side")
    f.writeLine("# TYPE dex_asset_exposure gauge")
    for asset, exposure in stats.storeExposure.pairs():
      f.writeLine(
        "dex_asset_exposure{asset=\"" & asset & "\",side=\"buy\"} " & $exposure.buyVolume
      )
      f.writeLine(
        "dex_asset_exposure{asset=\"" & asset & "\",side=\"sell\"} " & $exposure.sellVolume
      )
  except CatchableError as exc:
    echo "[metrics] write failed: ", exc.msg

proc purgeExpiredOrders(node: DexNode) {.async.} =
  if node.store.isNil():
    return
  let removed = await node.store.purgeExpired(int64(getTime().toUnixFloat() * 1000.0))
  if removed > 0:
    node.expiredDrops += removed
    echo "[risk] purged expired orders: ", removed

proc logStoreStats(node: DexNode) {.async.} =
  if node.store.isNil():
    return
  let stats = await node.store.snapshotStats()
  var summary =
    stats.exposure.mapIt(
      &"{it.asset}(buy={it.buyVolume:.2f},sell={it.sellVolume:.2f},matched={it.matchedVolume:.2f})"
    ).join("; ")
  if summary.len == 0:
    summary = "n/a"
  var klineSummary = ""
  if not node.klineStore.isNil():
    klineSummary = node.klineStore.statsSummary()
  let line = &"[store] orders={stats.totalOrders} pending={stats.pendingOrders} matched={stats.matchedOrders} invalidOrders={node.invalidOrders} invalidMatches={node.invalidMatches} riskDrops={node.riskDrops} expiredDrops={node.expiredDrops} exposures=[{summary}] kline=({klineSummary})"
  echo line
  if node.metricsFile.len > 0:
    node.writeMetrics(stats)
    if not node.settlement.isNil():
      echo "[settle/stats] ", node.settlement.statsSummary()
    if not node.klineStore.isNil():
      echo "[kline/stats] ", node.klineStore.statsSummary()

proc bumpStats(node: DexNode) {.async.} =
  if node.statsLogEvery <= 0:
    node.statsLogEvery = 20
  inc node.statsTicker
  if node.statsTicker >= node.statsLogEvery:
    node.statsTicker = 0
    await node.logStoreStats()
    await node.purgeExpiredOrders()

proc persistOrder(node: DexNode; order: OrderMessage) {.async.} =
  if node.store.isNil():
    return
  try:
    await node.store.recordInboundOrder(order)
    await node.bumpStats()
  except CatchableError as exc:
    echo "[store] order persist failed: ", exc.msg

proc persistMatch(node: DexNode; match: MatchMessage) {.async.} =
  if node.store.isNil():
    return
  try:
    await node.store.recordMatch(match)
    await node.bumpStats()
  except CatchableError as exc:
    echo "[store] match persist failed: ", exc.msg

proc requiresRemoteSignature(node: DexNode): bool =
  not node.signing.isNil() and not node.signing.cfg.allowUnsigned

proc verifyOrderSignature(node: DexNode; order: OrderMessage): bool =
  if node.signing.isNil():
    return true
  let valid = node.signing.verifyOrder(order)
  if not valid:
    inc node.invalidOrders
    if requiresRemoteSignature(node):
      echo &"[verify] drop order {order.id} from {order.traderPeer}: invalid signature"
      return false
    echo &"[verify] allow unsigned order {order.id} from {order.traderPeer}"
  true

proc verifyMatchSignature(node: DexNode; match: MatchMessage): bool =
  if node.signing.isNil():
    return true
  let valid = node.signing.verifyMatch(match)
  if not valid:
    inc node.invalidMatches
    if requiresRemoteSignature(node):
      echo &"[verify] drop match {match.orderId} from {match.matcherPeer}: invalid signature"
      return false
    echo &"[verify] allow unsigned match {match.orderId} from {match.matcherPeer}"
  true

proc enforceLimit(node: DexNode; order: OrderMessage): Future[bool] {.async.} =
  if node.store.isNil():
    return true
  if not node.cfg.positionLimits.hasKey(order.asset):
    return true
  let limit = node.cfg.positionLimits[order.asset]
  let exposure = await node.store.exposureFor(order.asset)
  let current =
    if order.side.toLowerAscii() == "buy":
      exposure.buyVolume
    else:
      exposure.sellVolume
  if current + order.amount > limit:
    inc node.riskDrops
    echo &"[risk] drop order {order.id} {order.asset} side={order.side} amount={order.amount:.4f} limit={limit:.4f} current={current:.4f}"
    return false
  true

proc handleOrders(node: DexNode) =
  node.gossip.subscribe(
    OrdersTopic,
    proc(_: string, payload: seq[byte]) {.async.} =
      let orderOpt = decodeOrder(payload)
      if orderOpt.isNone():
        return
      let order = orderOpt.get()
      if not node.verifyOrderSignature(order):
        return
      if not await node.enforceLimit(order):
        return
      echo &"[order] {order.id} {order.side} {order.asset} price={order.price:.2f} amount={order.amount:.4f} peer={order.traderPeer}"
      await node.persistOrder(order)

      if node.cfg.mode == dmMatcher:
        if not node.identity.accessPolicy.checkAccess(node.identity.registry.localInfo, MatchesTopic):
          echo "[acl] matcher cannot publish on " & MatchesTopic
          return
        var match = node.matchFromOrder(order)
        match.asset = order.asset
        if not node.signing.isNil():
          node.signing.signMatch(match)
        await node.persistMatch(match)
        let sent = await node.gossip.publish(MatchesTopic, encodeMatch(match))
        echo &"[match] filled {match.orderId} sent={sent}"
  )

proc publishTrade(node: DexNode; match: MatchMessage) {.async.} =
  if not node.cfg.enableTrades:
    return
  if not node.identity.accessPolicy.checkAccess(node.identity.registry.localInfo, TradesTopic):
    return
  inc node.tradeSeq
  let trade = TradeEvent(
    matchId: match.orderId & "-" & $node.tradeSeq,
    orderId: match.orderId,
    takerSide: "buy",
    price: match.price,
    amount: match.amount,
    asset: match.asset,
    matcherPeer: match.matcherPeer,
    makerPeer: match.matcherPeer,
    createdAt: int64(getTime().toUnixFloat() * 1000.0),
    sequence: node.tradeSeq,
  )
  let sent = await node.gossip.publish(TradesTopic, encodeTrade(trade))
  if sent > 0:
    echo &"[trade] publish match={trade.matchId} seq={trade.sequence} asset={trade.asset}"

proc recordTrade(node: DexNode; trade: TradeEvent) {.async.} =
  if node.klineStore.isNil():
    return
  await node.klineStore.recordTrade(trade, node.cfg.klineScales)

proc logMixerSchedule(node: DexNode; msg: MixerMessage) =
  if msg.schedule.len == 0:
    echo &"[mixer] request {msg.requestId} has empty schedule"
  else:
    echo &"[mixer] plan req={msg.requestId} hops={msg.schedule.join(\" -> \")} note={msg.note}"

proc buildMixerHandler(node: DexNode): lpHttp.HttpHandler =
  proc handler(req: lpHttp.HttpRequest): Future[lpHttp.HttpResponse] {.async.} =
    var headers = initTable[string, string]()
    headers["content-type"] = "application/json"
    if req.path != "/dex/mixer":
      return lpHttp.HttpResponse(status = 404, headers = headers, body = @[])
    if req.verb.toUpperAscii() != "POST":
      return lpHttp.HttpResponse(status = 405, headers = headers, body = @[])
    try:
      let payload =
        if req.body.len == 0:
          newJObject()
        else:
          parseJson(string.fromBytes(req.body))
      let slots =
        if payload.hasKey("slots"):
          max(2, payload["slots"].getInt())
        else:
          max(2, node.cfg.mixerSlots)
      var schedule: seq[string] = @[]
      for i in 0 ..< slots:
        let salt = node.rng[].generate(uint32)
        schedule.add(&"hop-{i}-{salt mod 1000}")
      let resp = %*{
        "status": "ok",
        "requestId":
          if payload.hasKey("requestId"): payload["requestId"].getStr()
          else:
            $node.mixerSeq,
        "peer": $node.switch.peerInfo.peerId,
        "schedule": schedule,
        "note": "local-mixer",
      }
      lpHttp.HttpResponse(status = 200, headers = headers, body = ($resp).toBytes())
    except CatchableError as exc:
      let err = %*{"status": "error", "reason": exc.msg}
      lpHttp.HttpResponse(status = 400, headers = headers, body = ($err).toBytes())
  handler

proc initMixer(node: DexNode) =
  let handler = node.buildMixerHandler()
  let svc = lpHttp.HttpService.new(
    handler, lpHttp.HttpConfig.init(maxRequestBytes = 8_192, maxResponseBytes = 8_192)
  )
  svc.registerRoute("/dex/mixer", handler)
  node.switch.mount(svc)
  node.mixerService = svc
  node.mixerHandler = handler

proc invokeMixer(node: DexNode; msg: MixerMessage): Future[Option[MixerMessage]] {.async.} =
  if node.mixerHandler.isNil():
    return none(MixerMessage)
  var headers = initTable[string, string]()
  headers["content-type"] = "application/json"
  let reqNode = %*{
    "requestId": msg.requestId,
    "slots": if msg.slots > 0: msg.slots else: node.cfg.mixerSlots,
  }
  let request = lpHttp.HttpRequest(
    verb: "POST",
    path: "/dex/mixer",
    headers: headers,
    body: ($reqNode).toBytes(),
  )
  let response = await node.mixerHandler(request)
  if response.status != 200 or response.body.len == 0:
    return none(MixerMessage)
  try:
    let parsed = parseJson(string.fromBytes(response.body))
    var reply = msg
    reply.kind = "match"
    if parsed.hasKey("schedule") and parsed["schedule"].kind == JArray:
      reply.schedule = parsed["schedule"].elems.mapIt(it.getStr())
    reply.note = if parsed.hasKey("note"): parsed["note"].getStr() else: ""
    some(reply)
  except CatchableError:
    none(MixerMessage)

proc broadcastMixerRequest(node: DexNode; seqno: int) {.async.} =
  var msg = MixerMessage(
    kind: "request",
    requestId: $node.switch.peerInfo.peerId & "-mix-" & $seqno,
    initiator: $node.switch.peerInfo.peerId,
    asset: node.selectAsset(),
    amount: 0.25 + float(node.rng[].generate(uint16) mod 250) / 100.0,
    slots: node.cfg.mixerSlots,
    expiresAt: nowMs() + 30_000,
    schedule: @[],
    note: "",
  )
  let payload = encodeMixer(msg)
  let sent = await node.gossip.publish(MixersTopic, payload)
  if sent > 0:
    echo &"[mixer] request {msg.requestId} asset={msg.asset} slots={msg.slots} sent={sent}"

proc handleMixers(node: DexNode) =
  node.gossip.subscribe(
    MixersTopic,
    proc(_: string, payload: seq[byte]) {.async.} =
      let msgOpt = decodeMixer(payload)
      if msgOpt.isNone():
        return
      let msg = msgOpt.get()
      if msg.kind == "match":
        node.logMixerSchedule(msg)
        return
      if msg.initiator == $node.switch.peerInfo.peerId:
        return
      echo &"[mixer] received req={msg.requestId} from {msg.initiator} asset={msg.asset}"
      if node.cfg.enableMixer:
        let respOpt = await node.invokeMixer(msg)
        if respOpt.isSome():
          node.logMixerSchedule(respOpt.get())
  )

proc runMixer(node: DexNode) {.async.} =
  if not node.cfg.enableMixer:
    return
  let delay = chronos.milliseconds(max(1000, node.cfg.mixerIntervalMs))
  while node.cfg.enableMixer:
    inc node.mixerSeq
    await node.broadcastMixerRequest(node.mixerSeq)
    await sleepAsync(delay)

proc subscribeTrades(node: DexNode) =
  node.gossip.subscribe(
    TradesTopic,
    proc(_: string, payload: seq[byte]) {.async.} =
      if node.klineStore.isNil():
        return
      let tradeOpt = decodeTrade(payload)
      if tradeOpt.isNone():
        return
      await node.recordTrade(tradeOpt.get())
  )

proc handleMatches(node: DexNode) =
  node.gossip.subscribe(
    MatchesTopic,
    proc(_: string, payload: seq[byte]) {.async.} =
      let matchOpt = decodeMatch(payload)
      if matchOpt.isNone():
        return
      let match = matchOpt.get()
      if not node.verifyMatchSignature(match):
        return
      echo &"[settlement] order {match.orderId} matched by {match.matcherPeer} amount={match.amount:.4f} price={match.price:.2f}"
      await node.persistMatch(match)
      await node.publishTrade(match)
      if not node.settlement.isNil():
        await node.settlement.handleMatch(match)
  )

proc runTrader(node: DexNode) {.async.} =
  if not node.identity.accessPolicy.checkAccess(node.identity.registry.localInfo, OrdersTopic):
    echo "[acl] trader cannot publish on " & OrdersTopic
    return
  var counter = 0
  let delay = chronos.milliseconds(node.cfg.intervalMs)
  while true:
    inc counter
    var order = node.nextOrder(counter)
    if not node.signing.isNil():
      node.signing.signOrder(order)
    if not await node.enforceLimit(order):
      await sleepAsync(delay)
      continue
    await node.persistOrder(order)
    let payload = encodeOrder(order)
    let sent = await node.gossip.publish(OrdersTopic, payload)
    echo &"[broadcast] order {order.id} -> peers={sent}"

    await sleepAsync(delay)

proc runForever() {.async.} =
  while true:
    await sleepAsync(chronos.minutes(1))

proc main() {.async.} =
  var cfg = parseCli()
  if cfg.keyPath.len == 0:
    cfg.keyPath = cfg.dataDir.joinPath(DefaultKeyFile)
  let store =
    try:
      initOrderStore(cfg.dataDir)
    except CatchableError as exc:
      raise newException(ValueError, "failed to initialize order store: " & exc.msg)
    except Exception as exc:
      raise newException(ValueError, "failed to initialize order store: " & exc.msg)
  let signingCfg = SigningConfig(
    mode: if cfg.disableSignature: smDisabled else: smEnabled,
    keyPath: cfg.keyPath,
    requireRemoteSignature: not cfg.allowUnsigned,
    allowUnsigned: cfg.allowUnsigned,
  )
  let signingCtx = initSigning(signingCfg)
  let node = await startNode(cfg, store, signingCtx)
  handleOrders(node)
  handleMatches(node)
  subscribeTrades(node)
  handleMixers(node)
  
  if node.cfg.mode == dmTrader:
    asyncSpawn node.runTrader()
  if node.cfg.mode == dmKline and not node.klineStore.isNil():
    echo "[kline] aggregation mode enabled"
  if node.cfg.enableMixer:
    asyncSpawn node.runMixer()

  await runForever()

when isMainModule:
  waitFor(main())
