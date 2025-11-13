import std/[sequtils, strformat]
import chronos
import libp2p
import libp2p/builders
import libp2p/protocols/pubsub/gossipsub
import libp2p/peerinfo
import libp2p/peerid
import bridge/config
import bridge/model

type
  EventHandler* = proc(ev: LockEventMessage) {.async.}
  SignatureHandler* = proc(sig: SignatureMessage) {.async.}

  Coordinator* = ref object
    cfg*: BridgeConfig
    switch*: Switch
    gossip*: GossipSub
    eventHandlers*: seq[EventHandler]
    signatureHandlers*: seq[SignatureHandler]

proc localPeerId*(coord: Coordinator): string =
  $coord.switch.peerInfo.peerId

proc logAddresses*(coord: Coordinator) =
  echo "[peer] id=", coord.localPeerId()
  for ma in coord.switch.peerInfo.listenAddrs:
    echo "  listen: ", ma
  let full = coord.switch.peerInfo.fullAddrs()
  if full.isOk():
    for ma in full.get():
      echo "  announce: ", ma

proc connectToPeers*(coord: Coordinator, targets: seq[string]) {.async.} =
  for target in targets:
    let parsed = parseFullAddress(target)
    if parsed.isErr():
      echo "[connect] invalid multiaddress: ", target
      continue
    let (peerId, base) = parsed.get()
    try:
      await coord.switch.connect(peerId, @[base])
      echo "[connect] dialed ", peerId
    except CatchableError as exc:
      echo "[connect] ", peerId, " failed: ", exc.msg

proc buildSwitch(
    rng: ref HmacDrbgContext, cfg: BridgeConfig
): Result[Switch, string] =
  var addresses: seq[MultiAddress]
  let combined = cfg.listenTcp & cfg.listenQuic
  for addr in combined:
    let parsed = MultiAddress.init(addr)
    if parsed.isErr():
      return err("invalid listen address: " & addr)
    addresses.add(parsed.get())

  var builder = SwitchBuilder
    .new()
    .withRng(rng)
    .withAddresses(addresses)
    .withTcpTransport()
    .withNoise()
    .withYamux()
    .withMplex()
    .withAutonat()
    .withSignedPeerRecord()
  when defined(libp2p_msquic_experimental) or defined(libp2p_quic_support):
    if cfg.listenQuic.len > 0:
      when compiles(builder.withMsQuicTransport()):
        builder = builder.withMsQuicTransport()

  try:
    ok(builder.build())
  except CatchableError as exc:
    err("switch build failed: " & exc.msg)

proc dispatchEvent(coord: Coordinator, ev: LockEventMessage) {.async.} =
  for handler in coord.eventHandlers:
    await handler(ev)

proc dispatchSignature(coord: Coordinator, sig: SignatureMessage) {.async.} =
  for handler in coord.signatureHandlers:
    await handler(sig)

proc subscribeTopics(coord: Coordinator) =
  coord.gossip.subscribe(
    EventsTopic,
    proc(_: string, payload: seq[byte]) {.async.} =
      let evOpt = decodeEvent(payload)
      if evOpt.isNone():
        return
      await coord.dispatchEvent(evOpt.get())
  )
  coord.gossip.subscribe(
    SignaturesTopic,
    proc(_: string, payload: seq[byte]) {.async.} =
      let sigOpt = decodeSignature(payload)
      if sigOpt.isNone():
        return
      await coord.dispatchSignature(sigOpt.get())
  )

proc addEventHandler*(coord: Coordinator, handler: EventHandler) =
  coord.eventHandlers.add(handler)

proc addSignatureHandler*(coord: Coordinator, handler: SignatureHandler) =
  coord.signatureHandlers.add(handler)

proc publishEvent*(coord: Coordinator, ev: LockEventMessage): Future[int] {.async.} =
  await coord.gossip.publish(EventsTopic, encodeEvent(ev))

proc publishSignature*(coord: Coordinator, sig: SignatureMessage): Future[int] {.async.} =
  await coord.gossip.publish(SignaturesTopic, encodeSignature(sig))

proc startCoordinator*(cfg: BridgeConfig): Future[Coordinator] {.async.} =
  let rng = newRng()
  let switchRes = buildSwitch(rng, cfg)
  if switchRes.isErr():
    raise newException(ValueError, switchRes.error)
  let sw = switchRes.get()
  let gParams = GossipSubParams.init(floodPublish = true)
  var gossip = GossipSub.init(
    switch = sw, parameters = gParams, verifySignature = true, sign = true
  )
  gossip.PubSub.triggerSelf = true
  sw.mount(gossip)

  var coord = Coordinator(
    cfg: cfg,
    switch: sw,
    gossip: gossip,
    eventHandlers: @[],
    signatureHandlers: @[],
  )

  await sw.start()
  await gossip.start()

  coord.subscribeTopics()
  coord.logAddresses()
  if cfg.peers.len > 0:
    await coord.connectToPeers(cfg.peers)

  coord
