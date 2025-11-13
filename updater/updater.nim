## nim-libp2p mobile updater runtime

import std/[base64, options, sequtils, strutils, tables]
import pkg/results
import chronos
import chronicles
import stew/byteutils
import libp2p
import libp2p/protocols/pubsub/gossipsub
import libp2p/protocols/kademlia/kademlia

import ./config
import ./manifest

logScope:
  topics = "mobile-updater"

type
  ManifestHandler* = proc(
    updater: Updater, manifest: SignedManifest
  ) {.async, gcsafe.}

  ManifestResolver* = proc(pointer: string): Future[Option[SignedManifest]] {.async, gcsafe.}

  Updater* = ref object
    config*: UpdaterConfig
    switch*: Switch
    gossip*: GossipSub
    kad*: KadDHT
    rng*: ref HmacDrbgContext
    manifestHandlers: seq[ManifestHandler]
    manifestCache: Table[string, SignedManifest]
    maintenanceTask: Future[void]
    running: bool
    resolver: ManifestResolver

proc parseMultiAddress(address: string): Result[MultiAddress, string] =
  let res = MultiAddress.init(address)
  if res.isErr:
    return err("invalid MultiAddress: " & address & " -> " & res.error)
  ok(res.get())

proc parsePeerAddress(address: string): Result[(PeerId, seq[MultiAddress]), string] =
  let res = parseFullAddress(address)
  if res.isErr:
    return err("failed to parse bootstrap address: " & address & " -> " & res.error)
  let (peerId, ma) = res.get()
  ok((peerId, @[ma]))

proc defaultManifestResolver(pointer: string): Future[Option[SignedManifest]] {.async, gcsafe.} =
  if pointer.len == 0:
    return none(SignedManifest)
  const Prefix = "manifest:"
  if pointer.startsWith(Prefix):
    let encoded = pointer[Prefix.len .. ^1]
    try:
      let decoded = base64.decode(encoded)
      let signed = SignedManifest.decode(decoded.toBytes())
      if signed.isOk:
        return some(signed.get())
    except ValueError:
      return none(SignedManifest)
  none(SignedManifest)

proc newUpdater*(config: UpdaterConfig): Result[Updater, string] =
  var listenAddrs: seq[MultiAddress]
  for address in config.listenAddrs:
    listenAddrs.add(?parseMultiAddress(address))

  let rng = newRng()
  let builder =
    try:
      newStandardSwitchBuilder(
        addrs = listenAddrs, rng = rng, sendSignedPeerRecord = true
      )
    except LPError as exc:
      return err("failed to build Switch: " & exc.msg)

  let switch =
    try:
      builder.build()
    except LPError as exc:
      return err("failed to initialize Switch: " & exc.msg)

  let gossip =
    GossipSub.init(
      switch = switch,
      triggerSelf = false,
      verifySignature = true,
      sign = true,
      parameters = GossipSubParams.init(),
    )
  switch.mount(gossip.PubSub)

  let kad = KadDHT.new(switch)
  switch.mount(kad)

  let updater = Updater(
    config: config,
    switch: switch,
    gossip: gossip,
    kad: kad,
    rng: rng,
    manifestHandlers: @[],
    manifestCache: initTable[string, SignedManifest](),
    maintenanceTask: nil,
    running: false,
    resolver: defaultManifestResolver,
  )

  ok(updater)

proc setManifestResolver*(updater: Updater, resolver: ManifestResolver) =
  updater.resolver = resolver

proc onManifest*(updater: Updater, handler: ManifestHandler) =
  updater.manifestHandlers.add(handler)

proc latestManifest*(updater: Updater, channel: string): Option[SignedManifest] =
  updater.manifestCache.withValue(channel, manifest):
    return some(manifest[])
  none(SignedManifest)

proc publishManifest*(
    updater: Updater, manifest: SignedManifest
): Future[void] {.async, gcsafe.} =
  if not updater.running:
    warn "updater not started; skipping manifest publish"
    return
  let payload = manifest.encode()
  if payload.isErr:
    error "failed to encode manifest", err = payload.error
    return
  try:
    discard await updater.gossip.publish(
      updater.config.manifestTopic, payload.get()
    )
  except CatchableError as exc:
    error "failed to publish manifest", err = exc.msg

proc applyManifest(updater: Updater, manifest: SignedManifest, source: string) {.async, gcsafe.} =
  let channel = manifest.data.channel
  if channel.len == 0:
    warn "manifest missing channel; ignoring", source = source
    return

  updater.manifestCache.withValue(channel, existing):
    if manifest.data.sequence <= existing[].data.sequence:
      trace "ignoring stale manifest", channel = channel,
        sequence = manifest.data.sequence, prev = existing[].data.sequence,
        source = source
      return

  updater.manifestCache[channel] = manifest
  info "received manifest",
    channel = channel,
    version = manifest.data.version,
    sequence = manifest.data.sequence,
    manifestCid = manifest.data.manifestCid,
    source = source

  for handler in updater.manifestHandlers:
    try:
      await handler(updater, manifest)
    except CancelledError as exc:
      raise exc
    except CatchableError as exc:
      warn "failed to execute manifest handler", err = exc.msg

proc processManifestPayload(
    updater: Updater, payload: seq[byte], source: string
) {.async, gcsafe.} =
  let decoded = SignedManifest.decode(payload)
  if decoded.isErr:
    warn "failed to decode manifest", err = decoded.error, source = source
    return
  await updater.applyManifest(decoded.get(), source)

proc fetchManifestFromKey(updater: Updater, key: string, source: string) {.async, gcsafe.} =
  let res =
    try:
      await updater.kad.getValue(key.toBytes())
    except CancelledError as exc:
      raise exc
    except CatchableError as exc:
      trace "DHT query failed", key = key, err = exc.msg
      return

  if res.isErr:
    trace "DHT miss", key = key, err = res.error
    return

  await updater.processManifestPayload(res.get().value, source)

proc resolveIpns(updater: Updater, name: string) {.async, gcsafe.} =
  let key = if name.startsWith("/ipns/"): name else: "/ipns/" & name
  await updater.fetchManifestFromKey(key, "ipns:" & name)

proc resolveFallback(updater: Updater, key: string) {.async, gcsafe.} =
  await updater.fetchManifestFromKey(key, "dht:" & key)

proc maintenanceLoop(updater: Updater) {.async, gcsafe.} =
  try:
    while updater.running:
      for name in updater.config.ipnsNames:
        await updater.resolveIpns(name)
      for key in updater.config.fallbackKeys:
        await updater.resolveFallback(key)

      let interval =
        if updater.config.ipnsPollInterval <= chronos.ZeroDuration:
          chronos.seconds(15)
        else:
          updater.config.ipnsPollInterval
      await sleepAsync(interval)
  except CancelledError:
    trace "maintenance coroutine cancelled"

proc subscribeManifestTopic(updater: Updater) =
  if updater.config.manifestTopic.len == 0:
    warn "manifestTopic empty; GossipSub not subscribed"
    return

  proc handler(topic: string, data: seq[byte]) {.async, gcsafe.} =
    await updater.processManifestPayload(data, "gossipsub:" & topic)

  updater.gossip.subscribe(updater.config.manifestTopic, handler)

proc connectBootstrapPeers(updater: Updater) {.async, gcsafe.} =
  for address in updater.config.bootstrapPeers:
    let parsed = parsePeerAddress(address)
    if parsed.isErr:
      warn "skipping invalid bootstrap peer", address = address, err = parsed.error
      continue
    let (peerId, addrs) = parsed.get()
    try:
      await updater.switch.connect(peerId, addrs)
      info "connected to bootstrap peer", peerId = peerId, address = $addrs[0]
    except CancelledError as exc:
      raise exc
    except DialFailedError as exc:
      warn "failed to connect to bootstrap peer", address = address, err = exc.msg
    except CatchableError as exc:
      warn "error while connecting to bootstrap peer", address = address, err = exc.msg

proc start*(updater: Updater) {.async, gcsafe.} =
  if updater.running:
    warn "updater already started"
    return

  await updater.switch.start()
  updater.running = true

  updater.subscribeManifestTopic()

  await updater.connectBootstrapPeers()

  updater.maintenanceTask = updater.maintenanceLoop()
  info "updater started", peerId = $updater.switch.peerInfo.peerId

proc stop*(updater: Updater) {.async, gcsafe.} =
  if not updater.running:
    return
  updater.running = false

  if not updater.maintenanceTask.isNil:
    await updater.maintenanceTask.cancelAndWait()
    updater.maintenanceTask = nil

  await updater.switch.stop()
  info "updater stopped"
