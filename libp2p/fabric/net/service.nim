import std/[base64, options, strutils]

import chronos
import chronicles

import ../../builders
import ../../lsmr
import ../../multiaddress
import ../../peerstore
import ../../protocols/fetch/fetch
import ../../protocols/fetch/protobuf
import ../../protocols/kademlia/kademlia
import ../../protocols/pubsub/gossipsub
import ../../protocols/rendezvous
import ../../switch
import ../codec
import ../polar
import ../types
import ../../rwad/execution/state
import ../../rwad/identity

const
  PeerAnnouncementTopic* = "fabric/peer/1"
  EventTopic* = "fabric/event/1"
  AttestationTopic* = "fabric/attestation/1"
  EventCertificateTopic* = "fabric/event-certificate/1"
  CheckpointCandidateTopic* = "fabric/checkpoint-candidate/1"
  CheckpointVoteTopic* = "fabric/checkpoint-vote/1"
  CheckpointCertificateTopic* = "fabric/checkpoint-certificate/1"
  AvoProposalTopic* = "fabric/avo-proposal/1"
  AvoApprovalTopic* = "fabric/avo-approval/1"
  SubmitEventPrefix = "submitevent:"
  SubmitAttestationPrefix = "submitattestation:"
  SubmitEventCertificatePrefix = "submiteventcert:"
  SubmitCheckpointCandidatePrefix = "submitcheckpointcandidate:"
  SubmitCheckpointVotePrefix = "submitcheckpointvote:"
  SubmitCheckpointBundlePrefix = "submitcheckpointbundle:"
  SubmitAck = "ok"

type
  FabricNetworkRef = ref object
    value: FabricNetwork

  PeerHandler* = proc(network: FabricNetwork, item: PeerAnnouncement): Future[void] {.closure, gcsafe, raises: [].}
  EventHandler* = proc(network: FabricNetwork, item: FabricEvent): Future[void] {.closure, gcsafe, raises: [].}
  AttestationHandler* = proc(network: FabricNetwork, item: EventAttestation): Future[void] {.closure, gcsafe, raises: [].}
  EventCertificateHandler* = proc(network: FabricNetwork, item: EventCertificate): Future[void] {.closure, gcsafe, raises: [].}
  CheckpointCandidateHandler* = proc(network: FabricNetwork, item: CheckpointCandidate): Future[void] {.closure, gcsafe, raises: [].}
  CheckpointVoteHandler* = proc(network: FabricNetwork, item: CheckpointVote): Future[void] {.closure, gcsafe, raises: [].}
  CheckpointBundleHandler* = proc(network: FabricNetwork, item: CheckpointBundle): Future[void] {.closure, gcsafe, raises: [].}
  AvoProposalHandler* = proc(network: FabricNetwork, item: AvoProposal): Future[void] {.closure, gcsafe, raises: [].}
  AvoApprovalHandler* = proc(network: FabricNetwork, proposalId: string, validator: string): Future[void] {.closure, gcsafe, raises: [].}
  FetchLookup* = proc(key: string): Option[seq[byte]] {.gcsafe, raises: [].}

  FabricNetwork* = ref object
    identity*: NodeIdentity
    switch*: Switch
    gossip*: GossipSub
    fetchService*: FetchService
    rendezvous*: RendezVous
    kad*: KadDHT
    bootstrapAddrs*: seq[string]
    legacyBootstrapAddrs*: seq[string]
    lsmrBootstrapAddrs*: seq[string]
    routingMode*: RoutingPlaneMode
    primaryPlane*: PrimaryRoutingPlane
    peerHandler*: PeerHandler
    eventHandler*: EventHandler
    attestationHandler*: AttestationHandler
    eventCertificateHandler*: EventCertificateHandler
    checkpointCandidateHandler*: CheckpointCandidateHandler
    checkpointVoteHandler*: CheckpointVoteHandler
    checkpointBundleHandler*: CheckpointBundleHandler
    avoProposalHandler*: AvoProposalHandler
    avoApprovalHandler*: AvoApprovalHandler

proc fetchRaw*(
    network: FabricNetwork,
    peerIdText: string,
    key: string,
    timeout: Duration = 1.seconds,
    maxAttempts = 1,
): Future[Option[seq[byte]]] {.async: (raises: []).}

proc bytesOf(value: string): seq[byte] =
  result = newSeq[byte](value.len)
  if value.len > 0:
    copyMem(addr result[0], unsafeAddr value[0], value.len)

proc completedPublishFuture(): Future[void] {.gcsafe, raises: [].} =
  let fut = newFuture[void]("fabric.publish")
  fut.complete()
  fut

proc stringOf(data: openArray[byte]): string =
  result = newString(data.len)
  if data.len > 0:
    copyMem(addr result[0], unsafeAddr data[0], data.len)

proc safeEncode[T](value: T): Option[string] {.gcsafe, raises: [].} =
  try:
    {.cast(gcsafe).}:
      return some(encodeObj(value))
  except Exception:
    discard
  none(string)

proc safeDecode[T](payload: string): Option[T] {.gcsafe, raises: [].} =
  try:
    {.cast(gcsafe).}:
      return some(decodeObj[T](payload))
  except Exception:
    discard
  none(T)

proc safeDecodeSubmitted[T](key, prefix: string): Option[T] {.gcsafe, raises: [].} =
  try:
    return safeDecode[T](decode(key[prefix.len .. ^1]))
  except CatchableError:
    discard
  none(T)

proc fetchHandlerOf(networkRef: FabricNetworkRef, lookup: FetchLookup): FetchHandler =
  proc(key: string): Future[FetchResponse] {.async.} =
    if key.startsWith(SubmitEventPrefix):
      let item = safeDecodeSubmitted[FabricEvent](key, SubmitEventPrefix)
      if item.isNone():
        return FetchResponse(status: fsError, data: @[])
      if networkRef.value != nil and not networkRef.value.eventHandler.isNil:
        await networkRef.value.eventHandler(networkRef.value, item.get())
        return FetchResponse(status: fsOk, data: bytesOf(SubmitAck))
      return FetchResponse(status: fsNotFound, data: @[])
    if key.startsWith(SubmitAttestationPrefix):
      let item = safeDecodeSubmitted[EventAttestation](key, SubmitAttestationPrefix)
      if item.isNone():
        return FetchResponse(status: fsError, data: @[])
      if networkRef.value != nil and not networkRef.value.attestationHandler.isNil:
        await networkRef.value.attestationHandler(networkRef.value, item.get())
        return FetchResponse(status: fsOk, data: bytesOf(SubmitAck))
      return FetchResponse(status: fsNotFound, data: @[])
    if key.startsWith(SubmitEventCertificatePrefix):
      let item = safeDecodeSubmitted[EventCertificate](key, SubmitEventCertificatePrefix)
      if item.isNone():
        return FetchResponse(status: fsError, data: @[])
      if networkRef.value != nil and not networkRef.value.eventCertificateHandler.isNil:
        await networkRef.value.eventCertificateHandler(networkRef.value, item.get())
        return FetchResponse(status: fsOk, data: bytesOf(SubmitAck))
      return FetchResponse(status: fsNotFound, data: @[])
    if key.startsWith(SubmitCheckpointCandidatePrefix):
      let item = safeDecodeSubmitted[CheckpointCandidate](key, SubmitCheckpointCandidatePrefix)
      if item.isNone():
        return FetchResponse(status: fsError, data: @[])
      if networkRef.value != nil and not networkRef.value.checkpointCandidateHandler.isNil:
        await networkRef.value.checkpointCandidateHandler(networkRef.value, item.get())
        return FetchResponse(status: fsOk, data: bytesOf(SubmitAck))
      return FetchResponse(status: fsNotFound, data: @[])
    if key.startsWith(SubmitCheckpointVotePrefix):
      let item = safeDecodeSubmitted[CheckpointVote](key, SubmitCheckpointVotePrefix)
      if item.isNone():
        return FetchResponse(status: fsError, data: @[])
      if networkRef.value != nil and not networkRef.value.checkpointVoteHandler.isNil:
        await networkRef.value.checkpointVoteHandler(networkRef.value, item.get())
        return FetchResponse(status: fsOk, data: bytesOf(SubmitAck))
      return FetchResponse(status: fsNotFound, data: @[])
    if key.startsWith(SubmitCheckpointBundlePrefix):
      let item = safeDecodeSubmitted[CheckpointBundle](key, SubmitCheckpointBundlePrefix)
      if item.isNone():
        return FetchResponse(status: fsError, data: @[])
      if networkRef.value != nil and not networkRef.value.checkpointBundleHandler.isNil:
        await networkRef.value.checkpointBundleHandler(networkRef.value, item.get())
        return FetchResponse(status: fsOk, data: bytesOf(SubmitAck))
      return FetchResponse(status: fsNotFound, data: @[])
    if lookup.isNil:
      return FetchResponse(status: fsNotFound, data: @[])
    let payload = lookup(key)
    if payload.isNone():
      return FetchResponse(status: fsNotFound, data: @[])
    FetchResponse(status: fsOk, data: payload.get())

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

proc effectiveBootstrapAddrs(network: FabricNetwork): seq[string] =
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

proc routingPlaneStatus*(network: FabricNetwork): RoutingPlaneStatus =
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

proc newFabricNetwork*(
    identity: NodeIdentity,
    listenAddrs: seq[string],
    bootstrapAddrs: seq[string] = @[],
    legacyBootstrapAddrs: seq[string] = @[],
    lsmrBootstrapAddrs: seq[string] = @[],
    routingMode: RoutingPlaneMode = RoutingPlaneMode.legacyOnly,
    primaryPlane: PrimaryRoutingPlane = PrimaryRoutingPlane.legacy,
    lsmrConfig: Option[LsmrConfig] = none(LsmrConfig),
    fetchLookup: FetchLookup = nil,
    peerHandler: PeerHandler = nil,
    eventHandler: EventHandler = nil,
    attestationHandler: AttestationHandler = nil,
    eventCertificateHandler: EventCertificateHandler = nil,
    checkpointCandidateHandler: CheckpointCandidateHandler = nil,
    checkpointVoteHandler: CheckpointVoteHandler = nil,
    checkpointBundleHandler: CheckpointBundleHandler = nil,
    avoProposalHandler: AvoProposalHandler = nil,
    avoApprovalHandler: AvoApprovalHandler = nil,
): FabricNetwork =
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
  let networkRef = FabricNetworkRef(value: nil)
  let fetchService = FetchService.new(fetchHandlerOf(networkRef, fetchLookup), FetchConfig.init())
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
  result = FabricNetwork(
    identity: identity,
    switch: switch,
    gossip: gossip,
    fetchService: fetchService,
    rendezvous: rendezvous,
    kad: kad,
    bootstrapAddrs: bootstrapAddrs,
    legacyBootstrapAddrs: legacyBootstrapAddrs,
    lsmrBootstrapAddrs: lsmrBootstrapAddrs,
    routingMode: routingMode,
    primaryPlane: primaryPlane,
    peerHandler: peerHandler,
    eventHandler: eventHandler,
    attestationHandler: attestationHandler,
    eventCertificateHandler: eventCertificateHandler,
    checkpointCandidateHandler: checkpointCandidateHandler,
    checkpointVoteHandler: checkpointVoteHandler,
    checkpointBundleHandler: checkpointBundleHandler,
    avoProposalHandler: avoProposalHandler,
    avoApprovalHandler: avoApprovalHandler,
  )
  networkRef.value = result
  switch.mount(gossip)
  switch.mount(fetchService)
  if not rendezvous.isNil:
    switch.mount(rendezvous)
  if not kad.isNil:
    switch.mount(kad)

proc subscribeTopics(network: FabricNetwork) =
  network.gossip.subscribe(PeerAnnouncementTopic, proc(topic: string, data: seq[byte]) {.async.} =
    if not network.peerHandler.isNil:
      await network.peerHandler(network, decodeObj[PeerAnnouncement](stringOf(data))))
  network.gossip.subscribe(EventTopic, proc(topic: string, data: seq[byte]) {.async.} =
    if not network.eventHandler.isNil:
      await network.eventHandler(network, decodeObj[FabricEvent](stringOf(data))))
  network.gossip.subscribe(AttestationTopic, proc(topic: string, data: seq[byte]) {.async.} =
    if not network.attestationHandler.isNil:
      await network.attestationHandler(network, decodeObj[EventAttestation](stringOf(data))))
  network.gossip.subscribe(EventCertificateTopic, proc(topic: string, data: seq[byte]) {.async.} =
    if not network.eventCertificateHandler.isNil:
      await network.eventCertificateHandler(network, decodeObj[EventCertificate](stringOf(data))))
  network.gossip.subscribe(CheckpointCandidateTopic, proc(topic: string, data: seq[byte]) {.async.} =
    if not network.checkpointCandidateHandler.isNil:
      await network.checkpointCandidateHandler(network, decodeObj[CheckpointCandidate](stringOf(data))))
  network.gossip.subscribe(CheckpointVoteTopic, proc(topic: string, data: seq[byte]) {.async.} =
    if not network.checkpointVoteHandler.isNil:
      await network.checkpointVoteHandler(network, decodeObj[CheckpointVote](stringOf(data))))
  network.gossip.subscribe(CheckpointCertificateTopic, proc(topic: string, data: seq[byte]) {.async.} =
    if not network.checkpointBundleHandler.isNil:
      await network.checkpointBundleHandler(network, decodeObj[CheckpointBundle](stringOf(data))))
  network.gossip.subscribe(AvoProposalTopic, proc(topic: string, data: seq[byte]) {.async.} =
    if not network.avoProposalHandler.isNil:
      await network.avoProposalHandler(network, decodeObj[AvoProposal](stringOf(data))))
  network.gossip.subscribe(AvoApprovalTopic, proc(topic: string, data: seq[byte]) {.async.} =
    if not network.avoApprovalHandler.isNil:
      let parts = stringOf(data).split(":")
      if parts.len == 2:
        await network.avoApprovalHandler(network, parts[0], parts[1]))

proc start*(network: FabricNetwork) {.async.} =
  network.subscribeTopics()
  await network.switch.start()
  for raw in network.effectiveBootstrapAddrs():
    let parsed = MultiAddress.init(raw)
    if parsed.isErr():
      continue
    try:
      discard await network.switch.connect(parsed.get(), allowUnknownPeerId = true)
    except CatchableError as exc:
      warn "fabric bootstrap connect failed", addr = raw, err = exc.msg

proc stop*(network: FabricNetwork) {.async.} =
  if not network.isNil and not network.switch.isNil:
    await network.switch.stop()

proc publishPayload(network: FabricNetwork, topic: string, payload: string): Future[void] {.async: (raises: []).} =
  try:
    discard await network.gossip.publish(topic, bytesOf(payload))
  except CatchableError:
    discard

proc publishPeerAnnouncement*(network: FabricNetwork, item: PeerAnnouncement): Future[void] =
  let payload = safeEncode(item)
  if payload.isNone():
    return completedPublishFuture()
  network.publishPayload(PeerAnnouncementTopic, payload.get())

proc publishEvent*(network: FabricNetwork, item: FabricEvent): Future[void] =
  let payload = safeEncode(item)
  if payload.isNone():
    return completedPublishFuture()
  network.publishPayload(EventTopic, payload.get())

proc publishAttestation*(network: FabricNetwork, item: EventAttestation): Future[void] =
  let payload = safeEncode(item)
  if payload.isNone():
    return completedPublishFuture()
  network.publishPayload(AttestationTopic, payload.get())

proc publishEventCertificate*(network: FabricNetwork, item: EventCertificate): Future[void] =
  let payload = safeEncode(item)
  if payload.isNone():
    return completedPublishFuture()
  network.publishPayload(EventCertificateTopic, payload.get())

proc publishCheckpointCandidate*(network: FabricNetwork, item: CheckpointCandidate): Future[void] =
  let payload = safeEncode(item)
  if payload.isNone():
    return completedPublishFuture()
  network.publishPayload(CheckpointCandidateTopic, payload.get())

proc publishCheckpointVote*(network: FabricNetwork, item: CheckpointVote): Future[void] =
  let payload = safeEncode(item)
  if payload.isNone():
    return completedPublishFuture()
  network.publishPayload(CheckpointVoteTopic, payload.get())

proc publishCheckpointCertificate*(network: FabricNetwork, item: CheckpointCertificate, snapshot: ChainStateSnapshot): Future[void] =
  let bundle = CheckpointBundle(
    certificate: item,
    snapshot: encodePolarSnapshot(snapshot),
  )
  let payload = safeEncode(bundle)
  if payload.isNone():
    return completedPublishFuture()
  network.publishPayload(CheckpointCertificateTopic, payload.get())

proc publishAvoProposal*(network: FabricNetwork, item: AvoProposal): Future[void] =
  let payload = safeEncode(item)
  if payload.isNone():
    return completedPublishFuture()
  network.publishPayload(AvoProposalTopic, payload.get())

proc publishAvoApproval*(network: FabricNetwork, proposalId, validator: string): Future[void] =
  network.publishPayload(AvoApprovalTopic, proposalId & ":" & validator)

proc submitPayload(
    network: FabricNetwork,
    peerIdText: string,
    prefix: string,
    payload: string,
    timeout: Duration = 1.seconds,
): Future[bool] {.async: (raises: []).} =
  let response = await network.fetchRaw(
    peerIdText,
    prefix & encode(payload),
    timeout = timeout,
    maxAttempts = 1,
  )
  response.isSome() and stringOf(response.get()) == SubmitAck

proc submitEvent*(
    network: FabricNetwork,
    peerIdText: string,
    item: FabricEvent,
    timeout: Duration = 1.seconds,
): Future[bool] {.async: (raises: []).} =
  let payload = safeEncode(item)
  if payload.isNone():
    return false
  await network.submitPayload(peerIdText, SubmitEventPrefix, payload.get(), timeout)

proc submitAttestation*(
    network: FabricNetwork,
    peerIdText: string,
    item: EventAttestation,
    timeout: Duration = 1.seconds,
): Future[bool] {.async: (raises: []).} =
  let payload = safeEncode(item)
  if payload.isNone():
    return false
  await network.submitPayload(peerIdText, SubmitAttestationPrefix, payload.get(), timeout)

proc submitEventCertificate*(
    network: FabricNetwork,
    peerIdText: string,
    item: EventCertificate,
    timeout: Duration = 1.seconds,
): Future[bool] {.async: (raises: []).} =
  let payload = safeEncode(item)
  if payload.isNone():
    return false
  await network.submitPayload(peerIdText, SubmitEventCertificatePrefix, payload.get(), timeout)

proc submitCheckpointCandidate*(
    network: FabricNetwork,
    peerIdText: string,
    item: CheckpointCandidate,
    timeout: Duration = 1.seconds,
): Future[bool] {.async: (raises: []).} =
  let payload = safeEncode(item)
  if payload.isNone():
    return false
  await network.submitPayload(peerIdText, SubmitCheckpointCandidatePrefix, payload.get(), timeout)

proc submitCheckpointVote*(
    network: FabricNetwork,
    peerIdText: string,
    item: CheckpointVote,
    timeout: Duration = 1.seconds,
): Future[bool] {.async: (raises: []).} =
  let payload = safeEncode(item)
  if payload.isNone():
    return false
  await network.submitPayload(peerIdText, SubmitCheckpointVotePrefix, payload.get(), timeout)

proc submitCheckpointBundle*(
    network: FabricNetwork,
    peerIdText: string,
    item: CheckpointBundle,
    timeout: Duration = 1.seconds,
): Future[bool] {.async: (raises: []).} =
  let payload = safeEncode(item)
  if payload.isNone():
    return false
  await network.submitPayload(peerIdText, SubmitCheckpointBundlePrefix, payload.get(), timeout)

proc fetchRaw*(
    network: FabricNetwork,
    peerIdText: string,
    key: string,
    timeout: Duration = 1.seconds,
    maxAttempts = 1,
): Future[Option[seq[byte]]] {.async: (raises: []).} =
  try:
    let peerId = PeerId.init(peerIdText).valueOr:
      return none(seq[byte])
    let response = await fetch(
      network.switch,
      peerId,
      key,
      timeout = timeout,
      maxAttempts = max(1, maxAttempts),
    )
    if response.status == fsOk:
      return some(response.data)
  except CatchableError:
    discard
  none(seq[byte])
