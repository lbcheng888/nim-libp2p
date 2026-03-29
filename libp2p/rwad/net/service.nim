import std/[base64, options, strutils]
from std/times import epochTime

import chronos
import chronicles

import ../../builders
import ../../lsmr
import ../../multiaddress
import ../../peerstore
import ../../services/lsmrservice
import ../../switch
import ../../protocols/fetch/fetch
import ../../protocols/fetch/protobuf
import ../../protocols/protocol
import ../../protocols/kademlia/kademlia
import ../../protocols/pubsub/gossipsub
import ../../protocols/rendezvous
import ../../stream/connection

import ../codec
import ../identity
import ../types

const
  BatchTopic* = "rwad/batch/1"
  VoteTopic* = "rwad/vote/1"
  CertificateTopic* = "rwad/certificate/1"
  SocialEventTopic* = "rwad/social/1"
  ReserveEventTopic* = "rwad/reserve/1"
  SubmitBatchCodec* = "/rwad/submit/batch/1"
  SubmitVoteCodec* = "/rwad/submit/vote/1"
  SubmitCertificateCodec* = "/rwad/submit/certificate/1"
  SubmitMaxMessageBytes = 1024 * 512
  SubmitAckMaxBytes = 64 * 1024
  SubmitAck = "ok"

type
  BatchHandler* = proc(network: RwadNetwork, batch: Batch): Future[void] {.closure, gcsafe, raises: [].}
  VoteHandler* = proc(network: RwadNetwork, vote: BatchVote): Future[bool] {.closure, gcsafe, raises: [].}
  CertificateHandler* = proc(network: RwadNetwork, cert: BatchCertificate): Future[void] {.closure, gcsafe, raises: [].}
  EventHandler* = proc(network: RwadNetwork, topic: string, payload: string): Future[void] {.closure, gcsafe, raises: [].}
  FetchLookup* = proc(key: string): Option[seq[byte]] {.gcsafe, raises: [].}

  RwadNetwork* = ref object
    identity*: NodeIdentity
    switch*: Switch
    gossip*: GossipSub
    fetchService*: FetchService
    rendezvous*: RendezVous
    kad*: KadDHT
    submitBatchProtocol*: LPProtocol
    submitVoteProtocol*: LPProtocol
    submitCertificateProtocol*: LPProtocol
    bootstrapAddrs*: seq[string]
    legacyBootstrapAddrs*: seq[string]
    lsmrBootstrapAddrs*: seq[string]
    routingMode*: RoutingPlaneMode
    primaryPlane*: PrimaryRoutingPlane
    batchHandler*: BatchHandler
    voteHandler*: VoteHandler
    certificateHandler*: CertificateHandler
    eventHandler*: EventHandler

proc bytesOf(value: string): seq[byte] =
  result = newSeq[byte](value.len)
  if value.len > 0:
    copyMem(addr result[0], unsafeAddr value[0], value.len)

proc stringOf(data: openArray[byte]): string =
  result = newString(data.len)
  if data.len > 0:
    copyMem(addr result[0], unsafeAddr data[0], data.len)

proc fetchHandlerOf(lookup: FetchLookup): FetchHandler =
  proc(key: string): Future[FetchResponse] {.async.} =
    if lookup.isNil:
      return FetchResponse(status: fsNotFound, data: @[])
    let resolved = lookup(key)
    if resolved.isNone():
      return FetchResponse(status: fsNotFound, data: @[])
    FetchResponse(status: fsOk, data: resolved.get())

proc buildBatchSubmitProtocol(network: RwadNetwork): LPProtocol =
  proc handle(conn: Connection, proto: string) {.async: (raises: [CancelledError]).} =
    try:
      let payload = await conn.readLp(SubmitMaxMessageBytes)
      if not network.batchHandler.isNil:
        await network.batchHandler(network, decodeObj[Batch](stringOf(payload)))
      await conn.writeLp(bytesOf(SubmitAck))
    except CancelledError as exc:
      raise exc
    except CatchableError as exc:
      warn "rwad submit batch protocol failed", codec = proto, err = exc.msg
      try:
        await conn.writeLp(bytesOf(""))
      except CatchableError:
        discard
  LPProtocol.new(@[SubmitBatchCodec], handle)

proc buildVoteSubmitProtocol(network: RwadNetwork): LPProtocol =
  proc handle(conn: Connection, proto: string) {.async: (raises: [CancelledError]).} =
    try:
      let payload = await conn.readLp(SubmitMaxMessageBytes)
      var accepted = false
      if not network.voteHandler.isNil:
        accepted = await network.voteHandler(network, decodeObj[BatchVote](stringOf(payload)))
      if accepted:
        await conn.writeLp(bytesOf(SubmitAck))
      else:
        await conn.writeLp(bytesOf(""))
    except CancelledError as exc:
      raise exc
    except CatchableError as exc:
      warn "rwad submit vote protocol failed", codec = proto, err = exc.msg
      try:
        await conn.writeLp(bytesOf(""))
      except CatchableError:
        discard
  LPProtocol.new(@[SubmitVoteCodec], handle)

proc buildCertificateSubmitProtocol(network: RwadNetwork): LPProtocol =
  proc handle(conn: Connection, proto: string) {.async: (raises: [CancelledError]).} =
    try:
      let payload = await conn.readLp(SubmitMaxMessageBytes)
      if not network.certificateHandler.isNil:
        await network.certificateHandler(network, decodeObj[BatchCertificate](stringOf(payload)))
      await conn.writeLp(bytesOf(SubmitAck))
    except CancelledError as exc:
      raise exc
    except CatchableError as exc:
      warn "rwad submit certificate protocol failed", codec = proto, err = exc.msg
      try:
        await conn.writeLp(bytesOf(""))
      except CatchableError:
        discard
  LPProtocol.new(@[SubmitCertificateCodec], handle)

proc parseListenAddrs(values: seq[string]): seq[MultiAddress] =
  for item in values:
    let parsed = MultiAddress.init(item)
    if parsed.isOk():
      result.add(parsed.get())
  if result.len == 0:
    result.add(MultiAddress.init("/ip4/127.0.0.1/tcp/0").tryGet())

proc appendUnique(target: var seq[string], value: string) =
  if value.len == 0:
    return
  for item in target:
    if item == value:
      return
  target.add(value)

proc appendUnique(target: var seq[MultiAddress], value: MultiAddress) =
  for item in target:
    if item == value:
      return
  target.add(value)

proc stripPeerSuffix(address: string): string =
  for marker in ["/p2p/", "/ipfs/"]:
    let idx = address.rfind(marker)
    if idx >= 0:
      return address[0 ..< idx]
  address

proc bootstrapAddrPeerText(address: string): string =
  for marker in ["/p2p/", "/ipfs/"]:
    let idx = address.rfind(marker)
    if idx >= 0:
      return address[idx + marker.len .. ^1]
  ""

proc effectiveBootstrapAddrs(network: RwadNetwork): seq[string] =
  if network.isNil:
    return @[]
  case network.routingMode
  of RoutingPlaneMode.legacyOnly:
    for raw in network.bootstrapAddrs:
      result.appendUnique(raw)
    for raw in network.legacyBootstrapAddrs:
      result.appendUnique(raw)
  of RoutingPlaneMode.dualStack:
    for raw in network.bootstrapAddrs:
      result.appendUnique(raw)
    for raw in network.legacyBootstrapAddrs:
      result.appendUnique(raw)
    for raw in network.lsmrBootstrapAddrs:
      result.appendUnique(raw)
  of RoutingPlaneMode.lsmrOnly:
    for raw in network.lsmrBootstrapAddrs:
      result.appendUnique(raw)

proc importLsmrBootstrapHints(network: RwadNetwork) =
  if network.isNil or network.switch.isNil:
    return
  let lsmrSvc = getLsmrService(network.switch)
  if lsmrSvc.isNil:
    return

  type AnchorAddrs = tuple[anchor: LsmrAnchor, addrs: seq[MultiAddress]]
  var grouped = initTable[string, AnchorAddrs]()
  for raw in network.lsmrBootstrapAddrs:
    let parsed = MultiAddress.init(raw)
    if parsed.isErr():
      continue
    let rawBase = stripPeerSuffix(raw)
    let rawPeerText = bootstrapAddrPeerText(raw)
    for anchor in lsmrSvc.config.anchors:
      var matched = false
      if rawPeerText.len > 0 and rawPeerText == $anchor.peerId:
        matched = true
      else:
        for configured in anchor.addrs:
          if stripPeerSuffix($configured) == rawBase:
            matched = true
            break
      if not matched:
        continue
      let key = $anchor.peerId
      var entry = grouped.getOrDefault(key, (anchor: anchor, addrs: @[]))
      entry.addrs.appendUnique(parsed.get())
      grouped[key] = entry
      break

  let nowMs = int64(epochTime() * 1000)
  let expiresAtMs = nowMs + (lsmrSvc.config.recordTtl.nanoseconds div 1_000_000)
  for entry in grouped.values():
    discard lsmrSvc.installNearFieldHandshake(NearFieldHandshakeRecord(
      provider: NearFieldBootstrapProvider.nfbpBle,
      networkId: lsmrSvc.config.networkId,
      peerId: entry.anchor.peerId,
      addrs: entry.addrs,
      operatorId: entry.anchor.operatorId,
      regionDigit: entry.anchor.regionDigit,
      attestedPrefix: entry.anchor.attestedPrefix,
      serveDepth: entry.anchor.serveDepth,
      directionMask: entry.anchor.directionMask,
      canIssueRootCert: entry.anchor.canIssueRootCert,
      issuedAtMs: nowMs,
      expiresAtMs: expiresAtMs,
    ))

proc routingPlaneStatus*(network: RwadNetwork): RoutingPlaneStatus =
  if network.isNil:
    return RoutingPlaneStatus()
  result.mode = network.routingMode
  result.primary = network.primaryPlane
  result.shadowMode = network.routingMode == RoutingPlaneMode.dualStack
  if network.switch.isNil or network.switch.peerStore.isNil:
    return
  result.lsmrActiveCertificates = network.switch.peerStore[ActiveLsmrBook].len
  result.lsmrMigrations = network.switch.peerStore[LsmrMigrationBook].len
  result.lsmrIsolations = network.switch.peerStore[LsmrIsolationBook].len

proc newRwadNetwork*(
    identity: NodeIdentity,
    listenAddrs: seq[string],
    bootstrapAddrs: seq[string] = @[],
    legacyBootstrapAddrs: seq[string] = @[],
    lsmrBootstrapAddrs: seq[string] = @[],
    routingMode: RoutingPlaneMode = RoutingPlaneMode.legacyOnly,
    primaryPlane: PrimaryRoutingPlane = PrimaryRoutingPlane.legacy,
    lsmrConfig: Option[LsmrConfig] = none(LsmrConfig),
    fetchLookup: FetchLookup = nil,
    batchHandler: BatchHandler = nil,
    voteHandler: VoteHandler = nil,
    certificateHandler: CertificateHandler = nil,
    eventHandler: EventHandler = nil,
): RwadNetwork =
  if routingMode != RoutingPlaneMode.legacyOnly and lsmrConfig.isNone():
    raise newException(ValueError, "lsmr routing requires lsmrConfig")
  let addresses = parseListenAddrs(listenAddrs)
  var builder = newStandardSwitchBuilder(
    addrs = addresses,
    secureManagers = [SecureProtocol.Noise],
  ).withPrivateKey(identity.privateKey)
  builder = builder.withRoutingPlanes(routingMode, primaryPlane)
  lsmrConfig.withValue(lsmrCfg):
    builder = builder.withLsmr(lsmrCfg)
  let switch = builder.build()
  let gossip = GossipSub.init(switch = switch, triggerSelf = true)
  let fetchService = FetchService.new(fetchHandlerOf(fetchLookup), FetchConfig.init())
  let rendezvous =
    if routingMode == RoutingPlaneMode.lsmrOnly:
      nil
    else:
      RendezVous.new()
  let kad =
    if routingMode == RoutingPlaneMode.lsmrOnly:
      nil
    else:
      KadDHT.new(switch)
  var network = RwadNetwork(
    identity: identity,
    switch: switch,
    gossip: gossip,
    fetchService: fetchService,
    rendezvous: rendezvous,
    kad: kad,
    submitBatchProtocol: nil,
    submitVoteProtocol: nil,
    submitCertificateProtocol: nil,
    bootstrapAddrs: bootstrapAddrs,
    legacyBootstrapAddrs: legacyBootstrapAddrs,
    lsmrBootstrapAddrs: lsmrBootstrapAddrs,
    routingMode: routingMode,
    primaryPlane: primaryPlane,
    batchHandler: batchHandler,
    voteHandler: voteHandler,
    certificateHandler: certificateHandler,
    eventHandler: eventHandler,
  )
  network.submitBatchProtocol = buildBatchSubmitProtocol(network)
  network.submitVoteProtocol = buildVoteSubmitProtocol(network)
  network.submitCertificateProtocol = buildCertificateSubmitProtocol(network)
  switch.mount(gossip)
  switch.mount(fetchService)
  if not rendezvous.isNil:
    switch.mount(rendezvous)
  if not kad.isNil:
    switch.mount(kad)
  switch.mount(network.submitBatchProtocol)
  switch.mount(network.submitVoteProtocol)
  switch.mount(network.submitCertificateProtocol)
  network

proc subscribeTopics(network: RwadNetwork) =
  network.gossip.subscribe(
    BatchTopic,
    proc(topic: string, data: seq[byte]) {.async.} =
      try:
        if not network.batchHandler.isNil:
          await network.batchHandler(network, decodeObj[Batch](stringOf(data)))
      except CatchableError:
        discard
  )
  network.gossip.subscribe(
    VoteTopic,
    proc(topic: string, data: seq[byte]) {.async.} =
      try:
        if not network.voteHandler.isNil:
          discard await network.voteHandler(network, decodeObj[BatchVote](stringOf(data)))
      except CatchableError:
        discard
  )
  network.gossip.subscribe(
    CertificateTopic,
    proc(topic: string, data: seq[byte]) {.async.} =
      try:
        if not network.certificateHandler.isNil:
          await network.certificateHandler(network, decodeObj[BatchCertificate](stringOf(data)))
      except CatchableError:
        discard
  )
  network.gossip.subscribe(
    SocialEventTopic,
    proc(topic: string, data: seq[byte]) {.async.} =
      if not network.eventHandler.isNil:
        await network.eventHandler(network, topic, stringOf(data))
  )
  network.gossip.subscribe(
    ReserveEventTopic,
    proc(topic: string, data: seq[byte]) {.async.} =
      if not network.eventHandler.isNil:
        await network.eventHandler(network, topic, stringOf(data))
  )

proc start*(network: RwadNetwork) {.async.} =
  network.subscribeTopics()
  await network.switch.start()
  if network.routingMode == RoutingPlaneMode.lsmrOnly:
    network.importLsmrBootstrapHints()
    return
  for raw in network.effectiveBootstrapAddrs():
    let parsed = MultiAddress.init(raw)
    if parsed.isErr():
      continue
    try:
      let peerId = await network.switch.connect(parsed.get(), allowUnknownPeerId = true)
      if not network.switch.isNil and not network.switch.peerStore.isNil:
        network.switch.peerStore.addAddressWithTTL(peerId, parsed.get(), 30.minutes)
      let lsmrSvc = getLsmrService(network.switch)
      if not lsmrSvc.isNil:
        let nowMs = int64(epochTime() * 1000)
        for anchor in lsmrSvc.config.anchors:
          if anchor.peerId == peerId:
            discard lsmrSvc.installNearFieldHandshake(NearFieldHandshakeRecord(
              provider: NearFieldBootstrapProvider.nfbpBle,
              networkId: lsmrSvc.config.networkId,
              peerId: anchor.peerId,
              addrs: @[parsed.get()],
              operatorId: anchor.operatorId,
              regionDigit: anchor.regionDigit,
              attestedPrefix: anchor.attestedPrefix,
              serveDepth: anchor.serveDepth,
              directionMask: anchor.directionMask,
              canIssueRootCert: anchor.canIssueRootCert,
              issuedAtMs: nowMs,
              expiresAtMs: nowMs + (lsmrSvc.config.recordTtl.nanoseconds div 1_000_000),
            ))
            break
    except CatchableError:
      discard

proc stop*(network: RwadNetwork) {.async.} =
  if not network.isNil and not network.switch.isNil:
    await network.switch.stop()

proc publishPayload(
    network: RwadNetwork, topic: string, payload: seq[byte]
): Future[void] {.gcsafe, async: (raises: []).} =
  try:
    discard await network.gossip.publish(topic, payload)
  except CatchableError:
    discard

proc completedPublishFuture(): Future[void] {.gcsafe, raises: [].} =
  let fut = newFuture[void]("rwad.publish")
  fut.complete()
  fut

proc publishBatch*(network: RwadNetwork, batch: Batch): Future[void] {.gcsafe, raises: [].} =
  var payload = ""
  try:
    {.cast(gcsafe).}:
      payload = encodeObj(batch)
  except Exception:
    return completedPublishFuture()
  network.publishPayload(BatchTopic, bytesOf(payload))

proc publishVote*(network: RwadNetwork, vote: BatchVote): Future[void] {.gcsafe, raises: [].} =
  var payload = ""
  try:
    {.cast(gcsafe).}:
      payload = encodeObj(vote)
  except Exception:
    return completedPublishFuture()
  network.publishPayload(VoteTopic, bytesOf(payload))

proc publishCertificate*(
    network: RwadNetwork, cert: BatchCertificate
): Future[void] {.gcsafe, raises: [].} =
  var payload = ""
  try:
    {.cast(gcsafe).}:
      payload = encodeObj(cert)
  except Exception:
    return completedPublishFuture()
  network.publishPayload(CertificateTopic, bytesOf(payload))

proc publishSocialEvent*(network: RwadNetwork, payload: string): Future[void] =
  network.publishPayload(SocialEventTopic, bytesOf(payload))

proc publishReserveEvent*(network: RwadNetwork, payload: string): Future[void] =
  network.publishPayload(ReserveEventTopic, bytesOf(payload))

proc fetchRaw*(
    network: RwadNetwork,
    peerIdText: string,
    key: string,
    timeout: Duration = 1.seconds,
    maxAttempts = 1,
    retryDelay: Duration = 50.milliseconds,
): Future[Option[seq[byte]]] {.async: (raises: []).} =
  let peerId = PeerId.init(peerIdText).valueOr:
    return none(seq[byte])
  when defined(rwad_diag):
    echo "RWAD_DIAG fetchRaw start peer=", peerIdText[0 .. min(peerIdText.high, 11)],
      " key=", key[0 .. min(key.high, 47)]
  try:
    let overallTimeoutMs =
      max(
        1'i64,
        timeout.milliseconds().int64 * max(1, maxAttempts).int64 +
        retryDelay.milliseconds().int64 * max(0, maxAttempts - 1).int64 + 100,
      )
    let request = fetch(
      network.switch,
      peerId,
      key,
      timeout = timeout,
      maxResponseBytes = DefaultFetchMessageSize,
      maxAttempts = max(1, maxAttempts),
      retryDelay = retryDelay,
    )
    let ready = await withTimeout(request, chronos.milliseconds(overallTimeoutMs))
    if not ready:
      when defined(rwad_diag):
        echo "RWAD_DIAG fetchRaw timeout peer=", peerIdText[0 .. min(peerIdText.high, 11)],
          " key=", key[0 .. min(key.high, 47)]
      if not request.finished():
        request.cancel()
      return none(seq[byte])
    let response = await request
    when defined(rwad_diag):
      echo "RWAD_DIAG fetchRaw done peer=", peerIdText[0 .. min(peerIdText.high, 11)],
        " key=", key[0 .. min(key.high, 47)],
        " status=", response.status
    if response.status == fsOk:
      return some(response.data)
    if response.status == fsNotFound:
      return none(seq[byte])
  except CatchableError:
    when defined(rwad_diag):
      echo "RWAD_DIAG fetchRaw error peer=", peerIdText[0 .. min(peerIdText.high, 11)],
        " key=", key[0 .. min(key.high, 47)]
    discard
  none(seq[byte])

proc submitPayload(
    network: RwadNetwork,
    peerIdText: string,
    codec: string,
    payload: string,
    timeout: Duration = 1.seconds,
): Future[bool] {.async: (raises: []).} =
  discard PeerId.init(peerIdText).valueOr:
    return false
  let key =
    if codec == SubmitVoteCodec:
      "submitvote:" & encode(payload)
    elif codec == SubmitBatchCodec:
      "submitbatch:" & encode(payload)
    elif codec == SubmitCertificateCodec:
      "submitcert:" & encode(payload)
    else:
      return false
  when defined(rwad_diag):
    echo "RWAD_DIAG submitPayload start peer=", peerIdText[0 .. min(peerIdText.high, 11)],
      " codec=", codec
  try:
    let response = await network.fetchRaw(
      peerIdText,
      key,
      timeout = timeout,
      maxAttempts = 1,
    )
    let ok = response.isSome() and stringOf(response.get()) == SubmitAck
    when defined(rwad_diag):
      echo "RWAD_DIAG submitPayload done peer=", peerIdText[0 .. min(peerIdText.high, 11)],
        " codec=", codec,
        " ack=", ok
    return ok
  except CatchableError:
    when defined(rwad_diag):
      echo "RWAD_DIAG submitPayload error peer=", peerIdText[0 .. min(peerIdText.high, 11)],
        " codec=", codec
    false

proc submitBatch*(
    network: RwadNetwork, peerIdText: string, batch: Batch, timeout: Duration = 1.seconds
): Future[bool] {.async: (raises: []).} =
  var payload = ""
  try:
    {.cast(gcsafe).}:
      payload = encodeObj(batch)
  except Exception:
    return false
  await network.submitPayload(
    peerIdText,
    SubmitBatchCodec,
    payload,
    timeout = timeout,
  )

proc submitVote*(
    network: RwadNetwork, peerIdText: string, vote: BatchVote, timeout: Duration = 1.seconds
): Future[bool] {.async: (raises: []).} =
  var payload = ""
  try:
    {.cast(gcsafe).}:
      payload = encodeObj(vote)
  except Exception:
    return false
  await network.submitPayload(
    peerIdText,
    SubmitVoteCodec,
    payload,
    timeout = timeout,
  )

proc submitCertificate*(
    network: RwadNetwork,
    peerIdText: string,
    cert: BatchCertificate,
    timeout: Duration = 1.seconds,
): Future[bool] {.async: (raises: []).} =
  var payload = ""
  try:
    {.cast(gcsafe).}:
      payload = encodeObj(cert)
  except Exception:
    return false
  await network.submitPayload(
    peerIdText,
    SubmitCertificateCodec,
    payload,
    timeout = timeout,
  )
