import std/[base64, deques, options, sequtils, strutils, tables]
from std/times import epochTime

import chronos
import chronicles

import ../../builders
import ../../lsmr
import ../../multiaddress
import ../../peerstore
import ../../services/lsmrservice
import ../../protocols/fetch/fetch
import ../../protocols/fetch/protobuf
import ../../protocols/protocol
import ../../protocols/kademlia/kademlia
import ../../protocols/pubsub/gossipsub
import ../../protocols/rendezvous
import ../../stream/connection
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
  SubmitAvoProposalPrefix = "submitavoproposal:"
  SubmitAvoApprovalPrefix = "submitavoapproval:"
  SubmitFabricCodec* = "/fabric/submit/1"
  SubmitMaxMessageBytes = 1024 * 1024
  SubmitAckMaxBytes = 64 * 1024
  SubmitAck = "ok"
  FabricSlowSubmitConnectMs = 750'i64
  FabricSlowSubmitAckMs = 2000'i64
  FabricSubmitAckTimeout = 10.seconds
  FabricSubmitPipelineWindow = 4
  FabricSubmitInboxCapacity = 4096
  FabricSubmitInboxBatchLimit = 64
  FabricSubmitInboxYield = 0.seconds
  FabricSubmitWarmDebounce = 250.milliseconds

type
  FabricSubmitLane* {.pure.} = enum
    fslEvent
    fslAttestation
    fslOther

  FabricSubmitKind {.pure.} = enum
    fskEvent
    fskAttestation
    fskEventCertificate
    fskCheckpointCandidate
    fskCheckpointVote
    fskCheckpointBundle
    fskAvoProposal
    fskAvoApproval

  FabricSubmitEnvelope = object
    itemKey: string
    kind: FabricSubmitKind
    payload: string
    scopePrefix: LsmrPath

  FabricSubmitFrame = object
    items: seq[FabricSubmitEnvelope]

  FabricSubmitAckItem* = object
    itemKey*: string
    accepted*: bool

  FabricSubmitAckFrame* = object
    items*: seq[FabricSubmitAckItem]

  FabricSubmitInboxItem = object
    itemKey: string
    envelope: FabricSubmitEnvelope

  FabricSubmitRequest = ref object
    items: seq[FabricSubmitEnvelope]
    diagItemKey: string
    encoded: seq[byte]
    writtenAtMs: int64
    completion: Future[seq[bool]]

  FabricSubmitSessionState {.pure.} = enum
    fsssCold
    fsssWarming
    fsssReady
    fsssBroken

  FabricSubmitSession = ref object
    peerId: PeerId
    conn: Connection
    lock: AsyncLock
    state: FabricSubmitSessionState
    warmPending: bool
    warmRunner: Future[bool]
    submitQueue: Deque[FabricSubmitRequest]
    inflightQueue: Deque[FabricSubmitRequest]
    submitRunner: Future[void]

  FabricNetworkRef = ref object
    value: FabricNetwork

  PeerHandler* = proc(network: FabricNetwork, item: PeerAnnouncement): Future[void] {.closure, gcsafe, raises: [].}
  EventHandler* = proc(network: FabricNetwork, item: FabricEvent, scopePrefix: LsmrPath): Future[void] {.closure, gcsafe, raises: [].}
  AttestationHandler* = proc(network: FabricNetwork, item: EventAttestation, scopePrefix: LsmrPath): Future[void] {.closure, gcsafe, raises: [].}
  EventCertificateHandler* = proc(network: FabricNetwork, item: EventCertificate, scopePrefix: LsmrPath): Future[void] {.closure, gcsafe, raises: [].}
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
    submitProtocol*: LPProtocol
    rendezvous*: RendezVous
    kad*: KadDHT
    bootstrapAddrs*: seq[string]
    legacyBootstrapAddrs*: seq[string]
    lsmrBootstrapAddrs*: seq[string]
    routingMode*: RoutingPlaneMode
    primaryPlane*: PrimaryRoutingPlane
    submitSessions: Table[string, FabricSubmitSession]
    submitPeerAddrHints: Table[string, seq[MultiAddress]]
    submitWarmRunner: Future[void]
    submitWarmPending: bool
    submitWarmDelayRunner: Future[void]
    submitWarmGeneration: uint64
    submitReadyHook: proc() {.gcsafe, raises: [].}
    submitConnEventHandler: ConnEventHandler
    submitInbox: Deque[FabricSubmitInboxItem]
    submitInboxRunner: Future[void]
    submitInboxPending: bool
    lastSubmitConnectElapsedMs: int64
    maxSubmitConnectElapsedMs: int64
    slowSubmitConnectCount: int64
    lastSubmitAckElapsedMs: int64
    maxSubmitAckElapsedMs: int64
    slowSubmitAckCount: int64
    submitWriteFailureCount: int64
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

proc submitPayload(
    network: FabricNetwork,
    peerIdText: string,
    prefix: string,
    payload: string,
    scopePrefix: LsmrPath = @[],
    timeout: Duration = 1.seconds,
): Future[bool] {.async: (raises: []).}

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

proc diagNowMs(): int64 =
  int64(epochTime() * 1000)

proc closeSubmitConn(conn: Connection): Future[void] {.async: (raises: []).} =
  if conn.isNil:
    return
  try:
    await conn.close()
  except CatchableError:
    discard

proc closeSubmitConnSoon(conn: Connection) {.gcsafe, raises: [].} =
  if conn.isNil:
    return

  proc closeConn() {.async, gcsafe.} =
    try:
      await closeSubmitConn(conn)
    except CancelledError:
      discard

  asyncSpawn closeConn()

proc submitLaneText(lane: FabricSubmitLane): string {.gcsafe, raises: [].} =
  case lane
  of fslEvent:
    "event"
  of fslAttestation:
    "attestation"
  of fslOther:
    "other"

proc submitLaneForKind(kind: FabricSubmitKind): FabricSubmitLane {.gcsafe, raises: [].} =
  case kind
  of fskEvent:
    fslEvent
  of fskAttestation:
    fslAttestation
  of fskEventCertificate,
      fskCheckpointCandidate,
      fskCheckpointVote,
      fskCheckpointBundle,
      fskAvoProposal,
      fskAvoApproval:
    fslOther

proc submitSessionKey(peerId: PeerId, lane: FabricSubmitLane): string {.gcsafe, raises: [].} =
  if peerId.data.len == 0:
    return ""
  $peerId & "|" & submitLaneText(lane)

proc completeSubmitRequest(
    request: FabricSubmitRequest, accepted: seq[bool]
) {.gcsafe, raises: [].} =
  if request.isNil or request.completion.isNil or request.completion.finished():
    return
  request.completion.complete(accepted)

proc failSubmitRequests(
    queue: var Deque[FabricSubmitRequest]
) {.gcsafe, raises: [].} =
  while queue.len > 0:
    let request = queue.popFirst()
    if request.isNil:
      continue
    request.completeSubmitRequest(@[])

proc dropSubmitPeerConnection(
    network: FabricNetwork, peerId: PeerId
): Future[void] {.async: (raises: []).} =
  if network.isNil or network.switch.isNil or peerId.data.len == 0:
    return
  if not network.switch.isConnected(peerId):
    return
  try:
    await network.switch.disconnect(peerId)
  except CancelledError:
    discard
  except CatchableError:
    discard

proc isLsmrSubmitMode(network: FabricNetwork): bool {.gcsafe, raises: [].} =
  if network.isNil:
    return false
  network.routingMode == RoutingPlaneMode.lsmrOnly or
    (network.routingMode == RoutingPlaneMode.dualStack and
      network.primaryPlane == PrimaryRoutingPlane.lsmr)

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

proc submitDiagItemKey(kind: FabricSubmitKind, payload: string): string {.gcsafe, raises: [].} =
  case kind
  of fskEvent:
    let item = safeDecode[FabricEvent](payload)
    if item.isSome():
      return item.get().eventId
  of fskAttestation:
    let item = safeDecode[EventAttestation](payload)
    if item.isSome():
      return item.get().eventId & ":" & $ord(item.get().role) & ":" & item.get().signer
  of fskEventCertificate:
    let item = safeDecode[EventCertificate](payload)
    if item.isSome():
      return item.get().eventId
  of fskCheckpointCandidate:
    let item = safeDecode[CheckpointCandidate](payload)
    if item.isSome():
      return item.get().candidateId
  of fskCheckpointVote:
    let item = safeDecode[CheckpointVote](payload)
    if item.isSome():
      return item.get().candidateId & ":" & item.get().validator
  of fskCheckpointBundle:
    let item = safeDecode[CheckpointBundle](payload)
    if item.isSome():
      return item.get().certificate.checkpointId
  of fskAvoProposal:
    let item = safeDecode[AvoProposal](payload)
    if item.isSome():
      return item.get().proposalId
  of fskAvoApproval:
    return payload
  ""

proc submitDiagBatchKey(items: openArray[FabricSubmitEnvelope]): string {.gcsafe, raises: [].} =
  if items.len == 0:
    return ""
  if items.len == 1:
    return submitDiagItemKey(items[0].kind, items[0].payload)
  let firstKey = submitDiagItemKey(items[0].kind, items[0].payload)
  let lastKey = submitDiagItemKey(items[^1].kind, items[^1].payload)
  "batch[" & $items.len & "]:" & firstKey & ".." & lastKey

proc effectiveSubmitItemKey(envelope: FabricSubmitEnvelope): string {.gcsafe, raises: [].} =
  if envelope.itemKey.len > 0:
    return envelope.itemKey
  submitDiagItemKey(envelope.kind, envelope.payload)

proc acceptedAckCount(acked: openArray[bool]): int {.gcsafe, raises: [].} =
  for accepted in acked:
    if accepted:
      inc result

proc decodeSubmitAckResults(
    items: openArray[FabricSubmitEnvelope], frame: FabricSubmitAckFrame
): tuple[ok: bool, accepted: seq[bool], reason: string] {.gcsafe, raises: [].} =
  if frame.items.len != items.len:
    return (false, @[], "count:" & $frame.items.len & "/" & $items.len)
  for idx, item in items:
    let expectedKey = item.effectiveSubmitItemKey()
    let actualKey = frame.items[idx].itemKey
    if actualKey != expectedKey:
      return (false, @[], "key[" & $idx & "]:" & actualKey & "!=" & expectedKey)
    result.accepted.add(frame.items[idx].accepted)
  result.ok = true

proc emptySubmitAckFrame(): FabricSubmitAckFrame {.gcsafe, raises: [].} =
  FabricSubmitAckFrame(items: @[])

proc processSubmitInboxItem(
    network: FabricNetwork, item: FabricSubmitInboxItem
): Future[void] {.async: (raises: [CancelledError]).}

proc scheduleSubmitInboxDrain(network: FabricNetwork) {.gcsafe, raises: [].}
proc runSubmitSession(
    network: FabricNetwork,
    peerId: PeerId,
    peerIdText: string,
    lane: FabricSubmitLane,
): Future[void] {.async: (raises: []).}
proc scheduleSubmitSession(
    network: FabricNetwork,
    peerId: PeerId,
    peerIdText: string,
    lane: FabricSubmitLane,
) {.gcsafe, raises: [].}

proc decodeSubmitInboxItem(
    network: FabricNetwork, envelope: FabricSubmitEnvelope
): Option[FabricSubmitInboxItem] {.gcsafe, raises: [].} =
  let itemKey = envelope.effectiveSubmitItemKey()
  case envelope.kind
  of fskEvent:
    if network.isNil or network.eventHandler.isNil:
      return none(FabricSubmitInboxItem)
    some(FabricSubmitInboxItem(
      itemKey: itemKey,
      envelope: envelope,
    ))
  of fskAttestation:
    if network.isNil or network.attestationHandler.isNil:
      return none(FabricSubmitInboxItem)
    some(FabricSubmitInboxItem(
      itemKey: itemKey,
      envelope: envelope,
    ))
  of fskEventCertificate:
    if network.isNil or network.eventCertificateHandler.isNil:
      return none(FabricSubmitInboxItem)
    some(FabricSubmitInboxItem(
      itemKey: itemKey,
      envelope: envelope,
    ))
  of fskCheckpointCandidate:
    if network.isNil or network.checkpointCandidateHandler.isNil:
      return none(FabricSubmitInboxItem)
    some(FabricSubmitInboxItem(
      itemKey: itemKey,
      envelope: envelope,
    ))
  of fskCheckpointVote:
    if network.isNil or network.checkpointVoteHandler.isNil:
      return none(FabricSubmitInboxItem)
    some(FabricSubmitInboxItem(
      itemKey: itemKey,
      envelope: envelope,
    ))
  of fskCheckpointBundle:
    if network.isNil or network.checkpointBundleHandler.isNil:
      return none(FabricSubmitInboxItem)
    some(FabricSubmitInboxItem(
      itemKey: itemKey,
      envelope: envelope,
    ))
  of fskAvoProposal:
    if network.isNil or network.avoProposalHandler.isNil:
      return none(FabricSubmitInboxItem)
    some(FabricSubmitInboxItem(
      itemKey: itemKey,
      envelope: envelope,
    ))
  of fskAvoApproval:
    if network.isNil or network.avoApprovalHandler.isNil:
      return none(FabricSubmitInboxItem)
    some(FabricSubmitInboxItem(
      itemKey: itemKey,
      envelope: envelope,
    ))

proc enqueueSubmitInboxItem(
    network: FabricNetwork, item: FabricSubmitInboxItem
): bool {.gcsafe, raises: [].} =
  if network.isNil:
    return false
  if network.submitInbox.len >= FabricSubmitInboxCapacity:
    when defined(fabric_submit_diag):
      echo "fabric-submit inbox-full self=",
        (if network.switch.isNil: "" else: peerIdString(network.switch.peerInfo.peerId)),
        " item=", item.itemKey,
        " queued=", network.submitInbox.len
    return false
  network.submitInbox.addLast(item)
  network.scheduleSubmitInboxDrain()
  true

proc acceptSubmitEnvelope(
    network: FabricNetwork, envelope: FabricSubmitEnvelope
): FabricSubmitAckItem {.gcsafe, raises: [].} =
  let itemKey = envelope.effectiveSubmitItemKey()
  let item = network.decodeSubmitInboxItem(envelope)
  if item.isNone():
    return FabricSubmitAckItem(itemKey: itemKey, accepted: false)
  FabricSubmitAckItem(
    itemKey: itemKey,
    accepted: network.enqueueSubmitInboxItem(item.get()),
  )

proc acceptSubmitFrame(
    network: FabricNetwork, frame: FabricSubmitFrame
): FabricSubmitAckFrame {.gcsafe, raises: [].} =
  if frame.items.len == 0:
    return emptySubmitAckFrame()
  for envelope in frame.items:
    result.items.add(network.acceptSubmitEnvelope(envelope))

proc processSubmitInboxItem(
    network: FabricNetwork, item: FabricSubmitInboxItem
): Future[void] {.async: (raises: [CancelledError]).} =
  try:
    case item.envelope.kind
    of fskEvent:
      let decoded = safeDecode[FabricEvent](item.envelope.payload)
      if decoded.isNone():
        return
      when defined(fabric_lsmr_diag):
        echo "fabric-submit recv-event self=", peerIdString(network.switch.peerInfo.peerId),
          " event=", decoded.get().eventId
      await network.eventHandler(network, decoded.get(), item.envelope.scopePrefix)
      when defined(fabric_lsmr_diag):
        echo "fabric-submit recv-event-done self=", peerIdString(network.switch.peerInfo.peerId),
          " event=", decoded.get().eventId
    of fskAttestation:
      let decoded = safeDecode[EventAttestation](item.envelope.payload)
      if decoded.isNone():
        return
      await network.attestationHandler(network, decoded.get(), item.envelope.scopePrefix)
    of fskEventCertificate:
      let decoded = safeDecode[EventCertificate](item.envelope.payload)
      if decoded.isNone():
        return
      await network.eventCertificateHandler(network, decoded.get(), item.envelope.scopePrefix)
    of fskCheckpointCandidate:
      let decoded = safeDecode[CheckpointCandidate](item.envelope.payload)
      if decoded.isNone():
        return
      await network.checkpointCandidateHandler(network, decoded.get())
    of fskCheckpointVote:
      let decoded = safeDecode[CheckpointVote](item.envelope.payload)
      if decoded.isNone():
        return
      await network.checkpointVoteHandler(network, decoded.get())
    of fskCheckpointBundle:
      let decoded = safeDecode[CheckpointBundle](item.envelope.payload)
      if decoded.isNone():
        return
      await network.checkpointBundleHandler(network, decoded.get())
    of fskAvoProposal:
      let decoded = safeDecode[AvoProposal](item.envelope.payload)
      if decoded.isNone():
        return
      await network.avoProposalHandler(network, decoded.get())
    of fskAvoApproval:
      let parts = item.envelope.payload.split(":")
      if parts.len != 2:
        return
      await network.avoApprovalHandler(network, parts[0], parts[1])
  except CancelledError as exc:
    raise exc
  except CatchableError as exc:
    when defined(fabric_submit_diag):
      echo "fabric-submit inbox-handler-fail self=",
        (if network.switch.isNil: "" else: peerIdString(network.switch.peerInfo.peerId)),
        " item=", item.itemKey,
        " err=", exc.msg

proc drainSubmitInbox(network: FabricNetwork): Future[void] {.async: (raises: []).} =
  if network.isNil:
    return
  while network.submitInbox.len > 0:
    var batch: seq[FabricSubmitInboxItem] = @[]
    while network.submitInbox.len > 0 and batch.len < FabricSubmitInboxBatchLimit:
      batch.add(network.submitInbox.popFirst())
    for item in batch:
      try:
        await network.processSubmitInboxItem(item)
        await sleepAsync(FabricSubmitInboxYield)
      except CancelledError:
        return
      except CatchableError as exc:
        when defined(fabric_submit_diag):
          echo "fabric-submit inbox-fail self=",
            (if network.switch.isNil: "" else: peerIdString(network.switch.peerInfo.peerId)),
            " item=", item.itemKey,
            " err=", exc.msg

proc scheduleSubmitInboxDrain(network: FabricNetwork) {.gcsafe, raises: [].} =
  if network.isNil:
    return
  if not network.submitInboxRunner.isNil and not network.submitInboxRunner.finished():
    network.submitInboxPending = true
    return

  proc drain() {.async, gcsafe.} =
    try:
      await network.drainSubmitInbox()
    except CancelledError:
      discard
    finally:
      if not network.isNil:
        let rerun = network.submitInboxPending or network.submitInbox.len > 0
        network.submitInboxPending = false
        network.submitInboxRunner = nil
        if rerun:
          network.scheduleSubmitInboxDrain()

  network.submitInboxRunner = drain()
  asyncSpawn network.submitInboxRunner

proc fetchHandlerOf(networkRef: FabricNetworkRef, lookup: FetchLookup): FetchHandler =
  proc(key: string): Future[FetchResponse] {.async.} =
    if key.startsWith(SubmitEventPrefix):
      let item = safeDecodeSubmitted[FabricEvent](key, SubmitEventPrefix)
      if item.isNone():
        when defined(fabric_lsmr_diag):
          echo "fabric-net reject-event-decode self=",
            (if networkRef.value.isNil or networkRef.value.switch.isNil: "" else: peerIdString(networkRef.value.switch.peerInfo.peerId))
        return FetchResponse(status: fsError, data: @[])
      if networkRef.value != nil and not networkRef.value.eventHandler.isNil:
        when defined(fabric_lsmr_diag):
          echo "fabric-net recv-event self=", peerIdString(networkRef.value.switch.peerInfo.peerId),
            " event=", item.get().eventId
        await networkRef.value.eventHandler(networkRef.value, item.get(), @[])
        when defined(fabric_lsmr_diag):
          echo "t=", diagNowMs(),
            " fabric-net recv-event-done self=", peerIdString(networkRef.value.switch.peerInfo.peerId),
            " event=", item.get().eventId
        return FetchResponse(status: fsOk, data: bytesOf(SubmitAck))
      when defined(fabric_lsmr_diag):
        echo "fabric-net reject-event-handler self=",
          (if networkRef.value.isNil or networkRef.value.switch.isNil: "" else: peerIdString(networkRef.value.switch.peerInfo.peerId)),
          " nil=", (networkRef.value.isNil or networkRef.value.eventHandler.isNil)
      return FetchResponse(status: fsNotFound, data: @[])
    if key.startsWith(SubmitAttestationPrefix):
      let item = safeDecodeSubmitted[EventAttestation](key, SubmitAttestationPrefix)
      if item.isNone():
        when defined(fabric_lsmr_diag):
          echo "fabric-net reject-att-decode self=",
            (if networkRef.value.isNil or networkRef.value.switch.isNil: "" else: peerIdString(networkRef.value.switch.peerInfo.peerId))
        return FetchResponse(status: fsError, data: @[])
      if networkRef.value != nil and not networkRef.value.attestationHandler.isNil:
        when defined(fabric_lsmr_diag):
          echo "fabric-net recv-att self=", peerIdString(networkRef.value.switch.peerInfo.peerId),
            " event=", item.get().eventId,
            " signer=", item.get().signer
        await networkRef.value.attestationHandler(networkRef.value, item.get(), @[])
        when defined(fabric_lsmr_diag):
          echo "fabric-net recv-att-done self=", peerIdString(networkRef.value.switch.peerInfo.peerId),
            " event=", item.get().eventId,
            " signer=", item.get().signer
        return FetchResponse(status: fsOk, data: bytesOf(SubmitAck))
      when defined(fabric_lsmr_diag):
        echo "fabric-net reject-att-handler self=",
          (if networkRef.value.isNil or networkRef.value.switch.isNil: "" else: peerIdString(networkRef.value.switch.peerInfo.peerId)),
          " nil=", (networkRef.value.isNil or networkRef.value.attestationHandler.isNil)
      return FetchResponse(status: fsNotFound, data: @[])
    if key.startsWith(SubmitEventCertificatePrefix):
      let item = safeDecodeSubmitted[EventCertificate](key, SubmitEventCertificatePrefix)
      if item.isNone():
        when defined(fabric_lsmr_diag):
          echo "fabric-net reject-cert-decode self=",
            (if networkRef.value.isNil or networkRef.value.switch.isNil: "" else: peerIdString(networkRef.value.switch.peerInfo.peerId))
        return FetchResponse(status: fsError, data: @[])
      if networkRef.value != nil and not networkRef.value.eventCertificateHandler.isNil:
        when defined(fabric_lsmr_diag):
          echo "fabric-net recv-cert self=", peerIdString(networkRef.value.switch.peerInfo.peerId),
            " event=", item.get().eventId
        await networkRef.value.eventCertificateHandler(networkRef.value, item.get(), @[])
        when defined(fabric_lsmr_diag):
          echo "fabric-net recv-cert-done self=", peerIdString(networkRef.value.switch.peerInfo.peerId),
            " event=", item.get().eventId
        return FetchResponse(status: fsOk, data: bytesOf(SubmitAck))
      when defined(fabric_lsmr_diag):
        echo "fabric-net reject-cert-handler self=",
          (if networkRef.value.isNil or networkRef.value.switch.isNil: "" else: peerIdString(networkRef.value.switch.peerInfo.peerId)),
          " nil=", (networkRef.value.isNil or networkRef.value.eventCertificateHandler.isNil)
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
    if key.startsWith(SubmitAvoProposalPrefix):
      let item = safeDecodeSubmitted[AvoProposal](key, SubmitAvoProposalPrefix)
      if item.isNone():
        return FetchResponse(status: fsError, data: @[])
      if networkRef.value != nil and not networkRef.value.avoProposalHandler.isNil:
        await networkRef.value.avoProposalHandler(networkRef.value, item.get())
        return FetchResponse(status: fsOk, data: bytesOf(SubmitAck))
      return FetchResponse(status: fsNotFound, data: @[])
    if key.startsWith(SubmitAvoApprovalPrefix):
      let payload = decode(key[SubmitAvoApprovalPrefix.len .. ^1])
      let parts = payload.split(":")
      if parts.len != 2:
        return FetchResponse(status: fsError, data: @[])
      if networkRef.value != nil and not networkRef.value.avoApprovalHandler.isNil:
        await networkRef.value.avoApprovalHandler(networkRef.value, parts[0], parts[1])
        return FetchResponse(status: fsOk, data: bytesOf(SubmitAck))
      return FetchResponse(status: fsNotFound, data: @[])
    if lookup.isNil:
      return FetchResponse(status: fsNotFound, data: @[])
    let payload = lookup(key)
    if payload.isNone():
      return FetchResponse(status: fsNotFound, data: @[])
    FetchResponse(status: fsOk, data: payload.get())

proc buildSubmitProtocol(network: FabricNetwork): LPProtocol =
  proc handle(conn: Connection, proto: string) {.async: (raises: [CancelledError]).} =
    try:
      while true:
        when defined(fabric_lsmr_diag):
          echo "t=", diagNowMs(),
            " fabric-submit handler-read-begin self=",
            (if network.isNil or network.switch.isNil: "" else: peerIdString(network.switch.peerInfo.peerId)),
            " codec=", proto
        let payload = await conn.readLp(SubmitMaxMessageBytes)
        when defined(fabric_lsmr_diag):
          echo "t=", diagNowMs(),
            " fabric-submit handler-read-done self=",
            (if network.isNil or network.switch.isNil: "" else: peerIdString(network.switch.peerInfo.peerId)),
            " codec=", proto,
            " bytes=", payload.len
        if payload.len == 0:
          break
        let raw = stringOf(payload)
        let frame = safeDecode[FabricSubmitFrame](raw)
        let envelope =
          if frame.isSome() and frame.get().items.len > 0:
            none(FabricSubmitEnvelope)
          else:
            safeDecode[FabricSubmitEnvelope](raw)
        let ackFrame =
          if frame.isSome() and frame.get().items.len > 0:
            network.acceptSubmitFrame(frame.get())
          elif envelope.isSome():
            FabricSubmitAckFrame(items: @[network.acceptSubmitEnvelope(envelope.get())])
          else:
            emptySubmitAckFrame()
        when defined(fabric_lsmr_diag):
          echo "t=", diagNowMs(),
            " fabric-submit handler-processed self=",
            (if network.isNil or network.switch.isNil: "" else: peerIdString(network.switch.peerInfo.peerId)),
            " codec=", proto,
            " decoded=", (frame.isSome() and frame.get().items.len > 0) or envelope.isSome(),
            " items=", (if frame.isSome() and frame.get().items.len > 0: frame.get().items.len else: 1),
            " accepted=", ackFrame.items.countIt(it.accepted),
            "/",
            ackFrame.items.len
        let ackPayload = safeEncode(ackFrame)
        if ackPayload.isSome():
          await conn.writeLp(bytesOf(ackPayload.get()))
        else:
          await conn.writeLp(bytesOf(""))
        when defined(fabric_lsmr_diag):
          echo "t=", diagNowMs(),
            " fabric-submit handler-ack-done self=",
            (if network.isNil or network.switch.isNil: "" else: peerIdString(network.switch.peerInfo.peerId)),
            " codec=", proto,
            " accepted=", ackFrame.items.countIt(it.accepted),
            "/",
            ackFrame.items.len
    except CancelledError as exc:
      raise exc
    except LPStreamEOFError:
      when defined(fabric_lsmr_diag):
        echo "fabric-submit handler-eof self=",
          (if network.isNil or network.switch.isNil: "" else: peerIdString(network.switch.peerInfo.peerId)),
          " codec=", proto
    except CatchableError as exc:
      when defined(fabric_lsmr_diag):
        echo "fabric-submit protocol-fail self=",
          (if network.isNil or network.switch.isNil: "" else: peerIdString(network.switch.peerInfo.peerId)),
          " codec=", proto,
          " err=", exc.msg
      try:
        await conn.writeLp(bytesOf(""))
      except CatchableError:
        discard
  LPProtocol.new(@[SubmitFabricCodec], handle)

proc parseListenAddrs(values: seq[string]): seq[MultiAddress] =
  for item in values:
    let parsed = MultiAddress.init(item)
    if parsed.isOk():
      result.add(parsed.get())
  if result.len == 0:
    result.add(MultiAddress.init("/ip4/127.0.0.1/tcp/0").tryGet())

proc getOrCreateSubmitSession(
    network: FabricNetwork, peerId: PeerId, lane: FabricSubmitLane
): FabricSubmitSession {.gcsafe, raises: [].} =
  if network.isNil or peerId.data.len == 0:
    return nil
  let key = submitSessionKey(peerId, lane)
  let existing = network.submitSessions.getOrDefault(key)
  if not existing.isNil:
    return existing
  result = FabricSubmitSession(
    peerId: peerId,
    conn: nil,
    lock: newAsyncLock(),
    state: fsssCold,
    warmPending: false,
    warmRunner: nil,
    submitQueue: initDeque[FabricSubmitRequest](),
    inflightQueue: initDeque[FabricSubmitRequest](),
    submitRunner: nil,
  )
  network.submitSessions[key] = result

proc mergeUniqueAddrs(
    target: var seq[MultiAddress], incoming: openArray[MultiAddress]
) {.gcsafe, raises: [].} =
  var seen = initTable[string, bool]()
  for addr in target:
    seen[$addr] = true
  for addr in incoming:
    let key = $addr
    if seen.hasKey(key):
      continue
    seen[key] = true
    target.add(addr)

proc rememberSubmitPeerAddrs*(
    network: FabricNetwork, peerId: PeerId, addrs: openArray[MultiAddress]
) {.gcsafe, raises: [].} =
  if network.isNil or peerId.data.len == 0 or addrs.len == 0:
    return
  let key = $peerId
  var merged = network.submitPeerAddrHints.getOrDefault(key)
  merged.mergeUniqueAddrs(addrs)
  if merged.len > 0:
    network.submitPeerAddrHints[key] = merged

proc rememberSubmitPeerAddrs*(
    network: FabricNetwork, peerIdText: string, addrs: openArray[MultiAddress]
) {.gcsafe, raises: [].} =
  if network.isNil or peerIdText.len == 0 or addrs.len == 0:
    return
  let peerId = PeerId.init(peerIdText).valueOr:
    return
  network.rememberSubmitPeerAddrs(peerId, addrs)

proc submitDialAddresses(
    network: FabricNetwork, peerId: PeerId
): seq[MultiAddress] {.gcsafe, raises: [].} =
  if network.isNil or peerId.data.len == 0:
    return @[]
  if not network.switch.isNil and not network.switch.peerStore.isNil:
    result.mergeUniqueAddrs(network.switch.peerStore.getAddresses(peerId))
  result.mergeUniqueAddrs(network.submitPeerAddrHints.getOrDefault($peerId))

proc submitSessionReady(session: FabricSubmitSession): bool {.gcsafe, raises: [].} =
  not session.isNil and
    session.state == fsssReady and
    not session.conn.isNil and
    not session.conn.closed and
    not session.conn.atEof

proc submitSessionStateText(state: FabricSubmitSessionState): string {.gcsafe, raises: [].} =
  case state
  of fsssCold:
    "cold"
  of fsssWarming:
    "warming"
  of fsssReady:
    "ready"
  of fsssBroken:
    "broken"

proc submitPeerDiag*(
    network: FabricNetwork, peerIdText: string
): string {.gcsafe, raises: [].} =
  if network.isNil or peerIdText.len == 0:
    return "network=nil"
  let peerId = PeerId.init(peerIdText).valueOr:
    return "peer=invalid"
  var laneStates: seq[string] = @[]
  for lane in [fslEvent, fslAttestation, fslOther]:
    let session = network.submitSessions.getOrDefault(submitSessionKey(peerId, lane))
    if session.isNil:
      laneStates.add(submitLaneText(lane) & "=missing")
    else:
      laneStates.add(
        submitLaneText(lane) &
        ":" & submitSessionStateText(session.state) &
        "/ready=" & $session.submitSessionReady() &
        "/connNil=" & $session.conn.isNil &
        "/closed=" & $(not session.conn.isNil and session.conn.closed) &
        "/eof=" & $(not session.conn.isNil and session.conn.atEof) &
        "/lock=" & $(not session.lock.isNil and session.lock.locked())
      )
  "peer=" & peerIdText &
    " connected=" & $network.switch.isConnected(peerId) &
    " hintAddrs=" & $network.submitPeerAddrHints.getOrDefault($peerId).len &
    " lanes=" & laneStates.join(",")

proc submitPeerReady*(network: FabricNetwork, peerId: PeerId, lane: FabricSubmitLane): bool {.gcsafe, raises: [].} =
  if network.isNil or peerId.data.len == 0:
    return false
  network.switch.isConnected(peerId) and
    network.submitSessions.getOrDefault(submitSessionKey(peerId, lane)).submitSessionReady()

proc submitPeerReady*(network: FabricNetwork, peerIdText: string, lane: FabricSubmitLane): bool {.gcsafe, raises: [].} =
  if network.isNil or peerIdText.len == 0:
    return false
  let peerId = PeerId.init(peerIdText).valueOr:
    return false
  network.submitPeerReady(peerId, lane)

proc submitPeerReady*(network: FabricNetwork, peerId: PeerId): bool {.gcsafe, raises: [].} =
  if network.isNil or peerId.data.len == 0:
    return false
  for lane in [fslEvent, fslAttestation, fslOther]:
    if not network.submitPeerReady(peerId, lane):
      return false
  true

proc submitPeerReady*(network: FabricNetwork, peerIdText: string): bool {.gcsafe, raises: [].} =
  if network.isNil or peerIdText.len == 0:
    return false
  let peerId = PeerId.init(peerIdText).valueOr:
    return false
  network.submitPeerReady(peerId)

proc desiredSubmitPeers(network: FabricNetwork): seq[PeerId] {.gcsafe, raises: [].}

proc submitDesiredPeerStats*(
    network: FabricNetwork
): tuple[ready, total: int, detail: string] {.gcsafe, raises: [].} =
  if network.isNil or network.switch.isNil:
    result.detail = "network=nil"
    return
  let peers = network.desiredSubmitPeers()
  result.total = peers.len
  var states: seq[string] = @[]
  for peerId in peers:
    let ready = network.submitPeerReady(peerId)
    if ready:
      inc result.ready
      states.add(peerIdString(peerId) & "=ready")
    else:
      states.add(network.submitPeerDiag($peerId))
  result.detail = states.join(" | ")

proc setSubmitReadyHook*(
    network: FabricNetwork, hook: proc() {.gcsafe, raises: [].}
) {.gcsafe, raises: [].} =
  if network.isNil:
    return
  network.submitReadyHook = hook

proc scheduleWarmSubmitConnections*(network: FabricNetwork) {.gcsafe, raises: [].}
proc ensureSubmitPeerConnectivity*(
    network: FabricNetwork, peerIdText: string, lane: FabricSubmitLane
) {.gcsafe, raises: [].}
proc ensureSubmitPeerConnectivity*(
    network: FabricNetwork, peerIdText: string
) {.gcsafe, raises: [].}

proc invalidateSubmitSession(
    network: FabricNetwork,
    peerId: PeerId,
    disconnected: bool,
) {.gcsafe, raises: [].} =
  if network.isNil or peerId.data.len == 0:
    return
  for lane in [fslEvent, fslAttestation, fslOther]:
    let session = network.submitSessions.getOrDefault(submitSessionKey(peerId, lane))
    if session.isNil:
      continue
    let conn = session.conn
    session.conn = nil
    session.state = if disconnected: fsssCold else: fsssBroken
    failSubmitRequests(session.submitQueue)
    failSubmitRequests(session.inflightQueue)
    closeSubmitConnSoon(conn)
    if disconnected and network.isLsmrSubmitMode():
      network.ensureSubmitPeerConnectivity($peerId, lane)

proc desiredSubmitPeers(network: FabricNetwork): seq[PeerId] {.gcsafe, raises: [].} =
  if network.isNil or network.switch.isNil or network.switch.peerStore.isNil:
    return @[]
  let selfPeerId = network.switch.peerInfo.peerId
  var seen = initTable[string, bool]()
  template addPeer(peerId: PeerId) =
    block:
      if peerId.data.len == 0 or peerId == selfPeerId:
        break
      let key = $peerId
      if seen.hasKey(key):
        break
      seen[key] = true
      result.add(peerId)
  let view = network.switch.peerStore.topologyNeighborView(selfPeerId)
  if view.isSome():
    let item = view.get()
    for peerId in item.sameCellPeers:
      addPeer(peerId)
    for peerId in item.parentPrefixPeers:
      addPeer(peerId)
    for bucket in item.directionalPeers:
      for peerId in bucket.peers:
        addPeer(peerId)

proc dialSubmitPeer(
    network: FabricNetwork, peerId: PeerId, allowRedial: bool, forceFresh = false
): Future[Connection] {.async: (raises: [CancelledError]).} =
  if network.isNil or network.switch.isNil or peerId.data.len == 0:
    return nil
  try:
    let addrs = network.submitDialAddresses(peerId)
    when defined(fabric_submit_diag) or defined(fabric_lsmr_diag):
      echo "fabric-submit dial-plan self=",
        (if network.switch.isNil: "" else: peerIdString(network.switch.peerInfo.peerId)),
        " peer=", peerIdString(peerId),
        " forceFresh=", forceFresh,
        " connected=", network.switch.isConnected(peerId),
        " addrs=", addrs.len,
        " hints=", network.submitPeerAddrHints.getOrDefault($peerId).len,
        " list=", addrs.mapIt($it).join(",")
    if forceFresh:
      await network.dropSubmitPeerConnection(peerId)
      if addrs.len == 0:
        when defined(fabric_submit_diag) or defined(fabric_lsmr_diag):
          echo "fabric-submit dial-force-fresh-no-addrs self=",
            (if network.switch.isNil: "" else: peerIdString(network.switch.peerInfo.peerId)),
            " peer=", peerIdString(peerId)
        return nil
      when defined(fabric_submit_diag) or defined(fabric_lsmr_diag):
        echo "fabric-submit dial-force-fresh self=",
          (if network.switch.isNil: "" else: peerIdString(network.switch.peerInfo.peerId)),
          " peer=", peerIdString(peerId),
          " addrs=", addrs.len
      return await network.switch.dial(
        peerId,
        addrs,
        @[SubmitFabricCodec],
        forceDial = true,
        reuseConnection = false,
      )
    if network.switch.isConnected(peerId):
      try:
        let stream = await network.switch.connManager.getStream(peerId)
        if stream.isNil:
          return nil
        return await network.switch.dialer.negotiateStream(stream, @[SubmitFabricCodec])
      except CancelledError as exc:
        raise exc
      except CatchableError as exc:
        when defined(fabric_submit_diag) or defined(fabric_lsmr_diag):
          echo "fabric-submit dial-stale-conn self=",
            (if network.switch.isNil: "" else: peerIdString(network.switch.peerInfo.peerId)),
            " peer=", peerIdString(peerId),
            " err=", exc.msg
        if not allowRedial or addrs.len == 0:
          return nil
        await network.dropSubmitPeerConnection(peerId)
        return await network.switch.dial(
          peerId,
          addrs,
          @[SubmitFabricCodec],
          forceDial = true,
          reuseConnection = false,
        )
    if not allowRedial:
      return nil
    if addrs.len > 0:
      return await network.switch.dial(
        peerId,
        addrs,
        @[SubmitFabricCodec],
        forceDial = false,
        reuseConnection = true,
      )
    return nil
  except CancelledError as exc:
    raise exc
  except CatchableError as exc:
    when defined(fabric_submit_diag) or defined(fabric_lsmr_diag):
      echo "fabric-submit dial-fail self=",
        (if network.switch.isNil: "" else: peerIdString(network.switch.peerInfo.peerId)),
        " peer=", peerIdString(peerId),
        " forceFresh=", forceFresh,
        " err=", exc.msg
    return nil

proc ensureSubmitPeerConnectivity*(
    network: FabricNetwork, peerId: PeerId, lane: FabricSubmitLane
): Future[bool] {.async: (raises: [CancelledError]).} =
  if network.isNil or network.switch.isNil or peerId.data.len == 0:
    return false
  let session = network.getOrCreateSubmitSession(peerId, lane)
  if session.isNil or session.lock.isNil:
    return false
  let connectedNow = network.switch.isConnected(peerId)
  if session.submitSessionReady() and not connectedNow:
    var oldConn: Connection = nil
    await session.lock.acquire()
    try:
      oldConn = session.conn
      session.state = fsssCold
      session.conn = nil
    finally:
      try:
        if session.lock.locked():
          session.lock.release()
      except AsyncLockError:
        discard
    if not oldConn.isNil:
      await closeSubmitConn(oldConn)
  let wasReady = session.submitSessionReady() and network.switch.isConnected(peerId)
  if wasReady:
    return true
  if not session.warmRunner.isNil and not session.warmRunner.finished():
    try:
      return await session.warmRunner
    except CancelledError as exc:
      raise exc
    except CatchableError:
      session.warmRunner = nil
      return false

  proc warmOne(): Future[bool] {.async: (raises: [CancelledError]).} =
    var probe: Connection = nil
    var oldConn: Connection = nil
    var connectedNow = false
    var warmed = false
    let startedAtMs = diagNowMs()
    try:
      await session.lock.acquire()
      try:
        if session.submitSessionReady():
          return true
        connectedNow = network.switch.isConnected(peerId)
        session.state = fsssWarming
      finally:
        try:
          if session.lock.locked():
            session.lock.release()
        except AsyncLockError:
          discard

      when defined(fabric_submit_diag):
        echo "fabric-submit warm-enter self=", peerIdString(network.switch.peerInfo.peerId),
          " peer=", peerIdString(peerId),
          " lane=", submitLaneText(lane),
          " connected=", connectedNow,
          " state=", submitSessionStateText(session.state),
          " connNil=", session.conn.isNil,
          " closed=", (if session.conn.isNil: false else: session.conn.closed),
          " eof=", (if session.conn.isNil: false else: session.conn.atEof)
      probe = await network.dialSubmitPeer(
        peerId,
        allowRedial = true,
        forceFresh = false,
      )
      let elapsedMs = diagNowMs() - startedAtMs
      network.lastSubmitConnectElapsedMs = elapsedMs
      if elapsedMs > network.maxSubmitConnectElapsedMs:
        network.maxSubmitConnectElapsedMs = elapsedMs
      if elapsedMs >= FabricSlowSubmitConnectMs:
        inc network.slowSubmitConnectCount
        echo "fabric-submit slow-connect self=", peerIdString(network.switch.peerInfo.peerId),
          " peer=", peerIdString(peerId),
          " lane=", submitLaneText(lane),
          " elapsedMs=", elapsedMs

      await session.lock.acquire()
      try:
        if probe.isNil or probe.closed or probe.atEof or not network.switch.isConnected(peerId):
          session.state = fsssBroken
          session.conn = nil
          when defined(fabric_lsmr_diag):
            echo "fabric-submit warm-stream-fail self=",
              peerIdString(network.switch.peerInfo.peerId),
              " peer=", peerIdString(peerId),
              " t=", diagNowMs()
          when defined(fabric_submit_diag):
            echo "fabric-submit warm-exit self=", peerIdString(network.switch.peerInfo.peerId),
              " peer=", peerIdString(peerId),
              " lane=", submitLaneText(lane),
              " ok=false state=", submitSessionStateText(session.state),
              " ready=", session.submitSessionReady(),
              " connNil=", session.conn.isNil,
              " closed=", (if session.conn.isNil: false else: session.conn.closed),
              " eof=", (if session.conn.isNil: false else: session.conn.atEof)
          return false
        session.state = fsssReady
        oldConn = session.conn
        session.conn = probe
        probe = nil
        session.warmPending = false
        when defined(fabric_lsmr_diag):
          echo "fabric-submit warm-stream-ok self=",
            peerIdString(network.switch.peerInfo.peerId),
            " peer=", peerIdString(peerId),
            " t=", diagNowMs()
        when defined(fabric_submit_diag):
          echo "fabric-submit warm-exit self=", peerIdString(network.switch.peerInfo.peerId),
            " peer=", peerIdString(peerId),
            " lane=", submitLaneText(lane),
            " ok=true state=", submitSessionStateText(session.state),
            " ready=", session.submitSessionReady(),
            " connNil=", session.conn.isNil,
            " closed=", (if session.conn.isNil: false else: session.conn.closed),
            " eof=", (if session.conn.isNil: false else: session.conn.atEof)
        warmed = true
      finally:
        try:
          if session.lock.locked():
            session.lock.release()
        except AsyncLockError:
          discard
      if not oldConn.isNil:
        await closeSubmitConn(oldConn)
      return warmed
    except CancelledError as exc:
      raise exc
    except CatchableError:
      if not probe.isNil:
        await closeSubmitConn(probe)
      try:
        await session.lock.acquire()
        session.state = fsssBroken
        session.conn = nil
      except CatchableError:
        discard
      finally:
        try:
          if session.lock.locked():
            session.lock.release()
        except AsyncLockError:
          discard
      return false

  session.warmRunner = warmOne()
  var ok = false
  try:
    try:
      ok = await session.warmRunner
    except CancelledError as exc:
      raise exc
    except CatchableError:
      ok = false
  finally:
    session.warmRunner = nil
  if ok and not wasReady and not network.submitReadyHook.isNil:
    network.submitReadyHook()
  ok

proc ensureSubmitPeerConnectivity*(
    network: FabricNetwork, peerIdText: string, lane: FabricSubmitLane
) {.gcsafe, raises: [].} =
  if network.isNil or peerIdText.len == 0:
    return
  let peerId = PeerId.init(peerIdText).valueOr:
    return
  proc warm() {.async, gcsafe.} =
    try:
      discard await network.ensureSubmitPeerConnectivity(peerId, lane)
    except CancelledError:
      discard
  asyncSpawn warm()

proc ensureSubmitPeerConnectivity*(
    network: FabricNetwork, peerIdText: string
) {.gcsafe, raises: [].} =
  if network.isNil or peerIdText.len == 0:
    return
  for lane in [fslEvent, fslAttestation, fslOther]:
    network.ensureSubmitPeerConnectivity(peerIdText, lane)

proc warmSubmitConnections*(network: FabricNetwork): Future[void] {.async: (raises: []).} =
  if network.isNil or not network.isLsmrSubmitMode():
    return
  for peerId in network.desiredSubmitPeers():
    for lane in [fslEvent, fslAttestation, fslOther]:
      network.ensureSubmitPeerConnectivity($peerId, lane)

proc scheduleWarmSubmitConnections*(network: FabricNetwork) {.gcsafe, raises: [].} =
  if network.isNil or not network.isLsmrSubmitMode():
    return
  inc network.submitWarmGeneration
  if not network.submitWarmDelayRunner.isNil and not network.submitWarmDelayRunner.finished():
    return

  proc warm() {.async, gcsafe.} =
    try:
      await network.warmSubmitConnections()
    except CancelledError:
      discard
    finally:
      if not network.isNil:
        let rerun = network.submitWarmPending
        network.submitWarmPending = false
        network.submitWarmRunner = nil
        if rerun:
          network.scheduleWarmSubmitConnections()

  proc warmAfterDebounce() {.async, gcsafe.} =
    try:
      while not network.isNil:
        let expectedGeneration = network.submitWarmGeneration
        await sleepAsync(FabricSubmitWarmDebounce)
        if network.isNil:
          return
        if expectedGeneration == network.submitWarmGeneration:
          break
      if network.isNil:
        return
      if not network.submitWarmRunner.isNil and not network.submitWarmRunner.finished():
        network.submitWarmPending = true
        return
      network.submitWarmRunner = warm()
      asyncSpawn network.submitWarmRunner
    except CancelledError:
      discard
    finally:
      if not network.isNil:
        network.submitWarmDelayRunner = nil

  network.submitWarmDelayRunner = warmAfterDebounce()
  asyncSpawn network.submitWarmDelayRunner

proc runSubmitSession(
    network: FabricNetwork,
    peerId: PeerId,
    peerIdText: string,
    lane: FabricSubmitLane,
): Future[void] {.async: (raises: []).} =
  if network.isNil or peerId.data.len == 0:
    return
  let session = network.getOrCreateSubmitSession(peerId, lane)
  if session.isNil or session.lock.isNil:
    return
  var brokenConn: Connection = nil
  try:
    await session.lock.acquire()
    while true:
      if not session.submitSessionReady() or not network.switch.isConnected(peerId):
        session.state = if network.switch.isConnected(peerId): fsssBroken else: fsssCold
        network.ensureSubmitPeerConnectivity(peerIdText, lane)
        break
      let conn = session.conn
      if conn.isNil or conn.closed or conn.atEof:
        brokenConn = conn
        session.conn = nil
        session.state = fsssBroken
        network.ensureSubmitPeerConnectivity(peerIdText, lane)
        break

      while session.inflightQueue.len < FabricSubmitPipelineWindow and session.submitQueue.len > 0:
        let request = session.submitQueue.popFirst()
        if request.isNil:
          continue
        request.writtenAtMs = diagNowMs()
        try:
          await conn.writeLp(request.encoded)
          session.inflightQueue.addLast(request)
        except CancelledError:
          raise
        except LPStreamEOFError:
          request.completeSubmitRequest(@[])
          brokenConn = conn
          session.conn = nil
          session.state = fsssBroken
          failSubmitRequests(session.inflightQueue)
          failSubmitRequests(session.submitQueue)
          network.ensureSubmitPeerConnectivity(peerIdText, lane)
          return
        except CatchableError:
          request.completeSubmitRequest(@[])
          inc network.submitWriteFailureCount
          brokenConn = conn
          session.conn = nil
          session.state = fsssBroken
          failSubmitRequests(session.inflightQueue)
          failSubmitRequests(session.submitQueue)
          network.ensureSubmitPeerConnectivity(peerIdText, lane)
          return

      if session.inflightQueue.len == 0:
        break

      let request = session.inflightQueue.peekFirst()
      let readFuture = conn.readLp(SubmitAckMaxBytes)
      let completed = await withTimeout(readFuture, FabricSubmitAckTimeout)
      if not completed:
        if not readFuture.finished():
          readFuture.cancelSoon()
        brokenConn = conn
        session.conn = nil
        session.state = fsssBroken
        request.completeSubmitRequest(@[])
        discard session.inflightQueue.popFirst()
        failSubmitRequests(session.inflightQueue)
        failSubmitRequests(session.submitQueue)
        network.ensureSubmitPeerConnectivity(peerIdText, lane)
        break
      var ack: seq[byte] = @[]
      try:
        ack = readFuture.read()
      except CatchableError:
        brokenConn = conn
        session.conn = nil
        session.state = fsssBroken
        request.completeSubmitRequest(@[])
        discard session.inflightQueue.popFirst()
        failSubmitRequests(session.inflightQueue)
        failSubmitRequests(session.submitQueue)
        network.ensureSubmitPeerConnectivity(peerIdText, lane)
        break
      let ackElapsedMs = diagNowMs() - request.writtenAtMs
      network.lastSubmitAckElapsedMs = ackElapsedMs
      if ackElapsedMs > network.maxSubmitAckElapsedMs:
        network.maxSubmitAckElapsedMs = ackElapsedMs
      if ackElapsedMs >= FabricSlowSubmitAckMs:
        inc network.slowSubmitAckCount
        echo "fabric-submit slow-ack self=", peerIdString(network.switch.peerInfo.peerId),
          " peer=", peerIdText,
          " lane=", submitLaneText(lane),
          " item=", request.diagItemKey,
          " items=", request.items.len,
          " elapsedMs=", ackElapsedMs
      if ack.len == 0:
        brokenConn = conn
        session.conn = nil
        session.state = fsssBroken
        request.completeSubmitRequest(@[])
        discard session.inflightQueue.popFirst()
        failSubmitRequests(session.inflightQueue)
        failSubmitRequests(session.submitQueue)
        network.ensureSubmitPeerConnectivity(peerIdText, lane)
        break
      let ackFrame = safeDecode[FabricSubmitAckFrame](stringOf(ack))
      if ackFrame.isNone():
        inc network.submitWriteFailureCount
        brokenConn = conn
        session.conn = nil
        session.state = fsssBroken
        request.completeSubmitRequest(@[])
        discard session.inflightQueue.popFirst()
        failSubmitRequests(session.inflightQueue)
        failSubmitRequests(session.submitQueue)
        network.ensureSubmitPeerConnectivity(peerIdText, lane)
        break
      let decodedAck = decodeSubmitAckResults(request.items, ackFrame.get())
      if not decodedAck.ok:
        inc network.submitWriteFailureCount
        brokenConn = conn
        session.conn = nil
        session.state = fsssBroken
        request.completeSubmitRequest(@[])
        discard session.inflightQueue.popFirst()
        failSubmitRequests(session.inflightQueue)
        failSubmitRequests(session.submitQueue)
        network.ensureSubmitPeerConnectivity(peerIdText, lane)
        break
      discard session.inflightQueue.popFirst()
      request.completeSubmitRequest(decodedAck.accepted)
  except CancelledError:
    discard
  finally:
    if not session.isNil:
      session.submitRunner = nil
      if not session.lock.isNil and session.lock.locked():
        try:
          session.lock.release()
        except AsyncLockError:
          discard
      if session.submitQueue.len > 0 and session.submitSessionReady() and network.switch.isConnected(peerId):
        network.scheduleSubmitSession(peerId, peerIdText, lane)
    if not brokenConn.isNil:
      await closeSubmitConn(brokenConn)

proc scheduleSubmitSession(
    network: FabricNetwork,
    peerId: PeerId,
    peerIdText: string,
    lane: FabricSubmitLane,
) {.gcsafe, raises: [].} =
  if network.isNil or peerId.data.len == 0:
    return
  let session = network.getOrCreateSubmitSession(peerId, lane)
  if session.isNil:
    return
  if not session.submitRunner.isNil and not session.submitRunner.finished():
    return
  if session.submitQueue.len == 0 and session.inflightQueue.len == 0:
    return

  proc launch() {.async, gcsafe.} =
    try:
      await network.runSubmitSession(peerId, peerIdText, lane)
    except CancelledError:
      discard

  session.submitRunner = launch()
  asyncSpawn session.submitRunner

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

proc importLsmrBootstrapHints(network: FabricNetwork) =
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

proc submitSlowConnects*(network: FabricNetwork): int64 {.gcsafe, raises: [].} =
  if network.isNil:
    return 0
  network.slowSubmitConnectCount

proc submitLastConnectMs*(network: FabricNetwork): int64 {.gcsafe, raises: [].} =
  if network.isNil:
    return 0
  network.lastSubmitConnectElapsedMs

proc submitMaxConnectMs*(network: FabricNetwork): int64 {.gcsafe, raises: [].} =
  if network.isNil:
    return 0
  network.maxSubmitConnectElapsedMs

proc submitSlowAcks*(network: FabricNetwork): int64 {.gcsafe, raises: [].} =
  if network.isNil:
    return 0
  network.slowSubmitAckCount

proc submitMaxAckMs*(network: FabricNetwork): int64 {.gcsafe, raises: [].} =
  if network.isNil:
    return 0
  network.maxSubmitAckElapsedMs

proc submitWriteFailures*(network: FabricNetwork): int64 {.gcsafe, raises: [].} =
  if network.isNil:
    return 0
  network.submitWriteFailureCount

proc resetSubmitMetrics*(network: FabricNetwork) {.gcsafe, raises: [].} =
  if network.isNil:
    return
  network.lastSubmitConnectElapsedMs = 0
  network.maxSubmitConnectElapsedMs = 0
  network.slowSubmitConnectCount = 0
  network.lastSubmitAckElapsedMs = 0
  network.maxSubmitAckElapsedMs = 0
  network.slowSubmitAckCount = 0
  network.submitWriteFailureCount = 0

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
    submitProtocol: nil,
    rendezvous: rendezvous,
    kad: kad,
    bootstrapAddrs: bootstrapAddrs,
    legacyBootstrapAddrs: legacyBootstrapAddrs,
    lsmrBootstrapAddrs: lsmrBootstrapAddrs,
    routingMode: routingMode,
    primaryPlane: primaryPlane,
    submitSessions: initTable[string, FabricSubmitSession](),
    submitPeerAddrHints: initTable[string, seq[MultiAddress]](),
    submitWarmRunner: nil,
    submitWarmPending: false,
    submitWarmDelayRunner: nil,
    submitWarmGeneration: 0'u64,
    submitInbox: initDeque[FabricSubmitInboxItem](),
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
  result.submitProtocol = buildSubmitProtocol(result)
  switch.mount(gossip)
  switch.mount(fetchService)
  switch.mount(result.submitProtocol)
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
      await network.eventHandler(network, decodeObj[FabricEvent](stringOf(data)), @[]))
  network.gossip.subscribe(AttestationTopic, proc(topic: string, data: seq[byte]) {.async.} =
    if not network.attestationHandler.isNil:
      await network.attestationHandler(network, decodeObj[EventAttestation](stringOf(data)), @[]))
  network.gossip.subscribe(EventCertificateTopic, proc(topic: string, data: seq[byte]) {.async.} =
    if not network.eventCertificateHandler.isNil:
      await network.eventCertificateHandler(network, decodeObj[EventCertificate](stringOf(data)), @[]))
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
  if network.isLsmrSubmitMode():
    network.submitConnEventHandler = proc(
        peerId: PeerId, event: ConnEvent
    ): Future[void] {.gcsafe, async: (raises: [CancelledError]).} =
      if network.isNil or network.switch.isNil:
        return
      if peerId == network.switch.peerInfo.peerId:
        return
      if event.kind == ConnEventKind.Disconnected:
        if not network.switch.isConnected(peerId):
          network.invalidateSubmitSession(peerId, disconnected = true)
    network.switch.addConnEventHandler(
      network.submitConnEventHandler,
      ConnEventKind.Disconnected,
    )
  if not network.isNil and not network.switch.isNil and not network.switch.peerStore.isNil:
    let localAddrs = network.switch.peerInfo.addrs
    if localAddrs.len > 0:
      network.switch.peerStore.setAddresses(network.switch.peerInfo.peerId, localAddrs)
  if network.routingMode == RoutingPlaneMode.lsmrOnly:
    network.importLsmrBootstrapHints()
    return
  for raw in network.effectiveBootstrapAddrs():
    let parsed = MultiAddress.init(raw)
    if parsed.isErr():
      continue
    try:
      let peerId = await network.switch.connect(parsed.get(), allowUnknownPeerId = true)
      when defined(fabric_diag):
        echo "fabric bootstrap connected raw=", raw, " peer=", $peerId
      if not network.switch.isNil and not network.switch.peerStore.isNil:
        network.switch.peerStore.addAddressWithTTL(peerId, parsed.get(), 30.minutes)
      let lsmrSvc = getLsmrService(network.switch)
      when defined(fabric_diag):
        echo "fabric bootstrap lsmrSvc=", (not lsmrSvc.isNil)
      if not lsmrSvc.isNil:
        let nowMs = int64(epochTime() * 1000)
        for anchor in lsmrSvc.config.anchors:
          if anchor.peerId == peerId:
            when defined(fabric_diag):
              echo "fabric bootstrap anchor match peer=", $peerId, " addr=", raw
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
    except CatchableError as exc:
      warn "fabric bootstrap connect failed", addr = raw, err = exc.msg

proc stop*(network: FabricNetwork) {.async.} =
  if not network.isNil and not network.switch.isNil:
    if not network.submitConnEventHandler.isNil:
      network.switch.removeConnEventHandler(
        network.submitConnEventHandler,
        ConnEventKind.Disconnected,
      )
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

proc submitAvoProposal*(
    network: FabricNetwork,
    peerIdText: string,
    item: AvoProposal,
    timeout: Duration = 1.seconds,
): Future[bool] {.async: (raises: []).} =
  let payload = safeEncode(item)
  if payload.isNone():
    return false
  await network.submitPayload(
    peerIdText,
    SubmitAvoProposalPrefix,
    payload.get(),
    timeout = timeout,
  )

proc submitAvoApproval*(
    network: FabricNetwork,
    peerIdText: string,
    proposalId, validator: string,
    timeout: Duration = 1.seconds,
): Future[bool] {.async: (raises: []).} =
  let payload = proposalId & ":" & validator
  if payload.len == 0:
    return false
  await network.submitPayload(
    peerIdText,
    SubmitAvoApprovalPrefix,
    payload,
    timeout = timeout,
  )

proc submitFramePayloadAcked(
    network: FabricNetwork,
    peerId: PeerId,
    peerIdText: string,
    lane: FabricSubmitLane,
    items: seq[FabricSubmitEnvelope],
    diagItemKey: string,
): Future[seq[bool]] {.async: (raises: []).} =
  result = @[]
  let encoded = safeEncode(FabricSubmitFrame(items: items))
  if encoded.isNone():
    return
  let session = network.getOrCreateSubmitSession(peerId, lane)
  if session.isNil:
    return
  if not session.submitSessionReady() or not network.switch.isConnected(peerId):
    network.ensureSubmitPeerConnectivity(peerIdText, lane)
    when defined(fabric_submit_diag) or defined(fabric_lsmr_diag):
      echo "t=", diagNowMs(),
        " fabric-submit not-ready self=", peerIdString(network.switch.peerInfo.peerId),
        " peer=", peerIdText,
        " lane=", submitLaneText(lane),
        " item=", diagItemKey,
        " items=", items.len
    return
  let request = FabricSubmitRequest(
    items: items,
    diagItemKey: diagItemKey,
    encoded: bytesOf(encoded.get()),
    writtenAtMs: 0,
    completion: newFuture[seq[bool]]("fabric.submit.batch"),
  )
  session.submitQueue.addLast(request)
  network.scheduleSubmitSession(peerId, peerIdText, lane)
  try:
    result = await request.completion
  except CancelledError:
    return
  except CatchableError:
    return

proc submitFramePayload(
    network: FabricNetwork,
    peerId: PeerId,
    peerIdText: string,
    lane: FabricSubmitLane,
    items: seq[FabricSubmitEnvelope],
    diagItemKey: string,
): Future[bool] {.async: (raises: []).} =
  let acked = await network.submitFramePayloadAcked(peerId, peerIdText, lane, items, diagItemKey)
  acked.len == items.len and acceptedAckCount(acked) == items.len

proc submitPayload(
    network: FabricNetwork,
    peerIdText: string,
    prefix: string,
    payload: string,
    scopePrefix: LsmrPath = @[],
    timeout: Duration = 1.seconds,
): Future[bool] {.async: (raises: []).} =
  if network.isLsmrSubmitMode():
    let peerId = PeerId.init(peerIdText).valueOr:
      return false
    let kind =
      if prefix == SubmitEventPrefix:
        some(fskEvent)
      elif prefix == SubmitAttestationPrefix:
        some(fskAttestation)
      elif prefix == SubmitEventCertificatePrefix:
        some(fskEventCertificate)
      elif prefix == SubmitCheckpointCandidatePrefix:
        some(fskCheckpointCandidate)
      elif prefix == SubmitCheckpointVotePrefix:
        some(fskCheckpointVote)
      elif prefix == SubmitCheckpointBundlePrefix:
        some(fskCheckpointBundle)
      elif prefix == SubmitAvoProposalPrefix:
        some(fskAvoProposal)
      elif prefix == SubmitAvoApprovalPrefix:
        some(fskAvoApproval)
      else:
        none(FabricSubmitKind)
    if kind.isNone():
      return false
    let diagItemKey = submitDiagItemKey(kind.get(), payload)
    return await submitFramePayload(
      network,
      peerId,
      peerIdText,
      submitLaneForKind(kind.get()),
      @[FabricSubmitEnvelope(
        itemKey: diagItemKey,
        kind: kind.get(),
        payload: payload,
        scopePrefix: scopePrefix,
      )],
      diagItemKey,
    )
  let response = await network.fetchRaw(
    peerIdText,
    prefix & encode(payload),
    timeout = timeout,
    maxAttempts = 1,
  )
  let ok = response.isSome() and stringOf(response.get()) == "ok"
  when defined(fabric_lsmr_diag):
    let selfPeerId =
      if network.isNil or network.switch.isNil or network.switch.peerInfo.isNil:
        ""
      else:
        peerIdString(network.switch.peerInfo.peerId)
    echo "fabric-net submit self=", selfPeerId,
      " t=", diagNowMs(),
      " target=", peerIdText,
      " prefix=", prefix,
      " ack=", ok
  ok

proc submitEvent*(
    network: FabricNetwork,
    peerIdText: string,
    item: FabricEvent,
    scopePrefix: LsmrPath = @[],
    timeout: Duration = 1.seconds,
): Future[bool] {.async: (raises: []).} =
  let payload = safeEncode(item)
  if payload.isNone():
    return false
  await network.submitPayload(
    peerIdText,
    SubmitEventPrefix,
    payload.get(),
    scopePrefix = scopePrefix,
    timeout = timeout,
  )

proc submitEventBatch*(
    network: FabricNetwork,
    peerIdText: string,
    items: seq[tuple[itemKey: string, event: FabricEvent, scopePrefix: LsmrPath]],
): Future[seq[bool]] {.async: (raises: []).} =
  if items.len == 0:
    return @[]
  let peerId = PeerId.init(peerIdText).valueOr:
    return @[]
  var envelopes: seq[FabricSubmitEnvelope] = @[]
  for item in items:
    let payload = safeEncode(item.event)
    if payload.isNone():
      return @[]
    envelopes.add(FabricSubmitEnvelope(
      itemKey: item.itemKey,
      kind: fskEvent,
      payload: payload.get(),
      scopePrefix: item.scopePrefix,
    ))
  await submitFramePayloadAcked(
    network,
    peerId,
    peerIdText,
    fslEvent,
    envelopes,
    submitDiagBatchKey(envelopes),
  )

proc submitAttestation*(
    network: FabricNetwork,
    peerIdText: string,
    item: EventAttestation,
    scopePrefix: LsmrPath = @[],
    timeout: Duration = 1.seconds,
): Future[bool] {.async: (raises: []).} =
  let payload = safeEncode(item)
  if payload.isNone():
    return false
  await network.submitPayload(
    peerIdText,
    SubmitAttestationPrefix,
    payload.get(),
    scopePrefix = scopePrefix,
    timeout = timeout,
  )

proc submitAttestationBatch*(
    network: FabricNetwork,
    peerIdText: string,
    items: seq[tuple[itemKey: string, attestation: EventAttestation, scopePrefix: LsmrPath]],
): Future[seq[bool]] {.async: (raises: []).} =
  if items.len == 0:
    return @[]
  let peerId = PeerId.init(peerIdText).valueOr:
    return @[]
  var envelopes: seq[FabricSubmitEnvelope] = @[]
  for item in items:
    let payload = safeEncode(item.attestation)
    if payload.isNone():
      return @[]
    envelopes.add(FabricSubmitEnvelope(
      itemKey: item.itemKey,
      kind: fskAttestation,
      payload: payload.get(),
      scopePrefix: item.scopePrefix,
    ))
  await submitFramePayloadAcked(
    network,
    peerId,
    peerIdText,
    fslAttestation,
    envelopes,
    submitDiagBatchKey(envelopes),
  )

proc submitEventCertificate*(
    network: FabricNetwork,
    peerIdText: string,
    item: EventCertificate,
    scopePrefix: LsmrPath = @[],
    timeout: Duration = 1.seconds,
): Future[bool] {.async: (raises: []).} =
  let payload = safeEncode(item)
  if payload.isNone():
    return false
  await network.submitPayload(
    peerIdText,
    SubmitEventCertificatePrefix,
    payload.get(),
    scopePrefix = scopePrefix,
    timeout = timeout,
  )

proc submitEventCertificateBatch*(
    network: FabricNetwork,
    peerIdText: string,
    items: seq[tuple[itemKey: string, eventCertificate: EventCertificate, scopePrefix: LsmrPath]],
): Future[seq[bool]] {.async: (raises: []).} =
  if items.len == 0:
    return @[]
  let peerId = PeerId.init(peerIdText).valueOr:
    return @[]
  var envelopes: seq[FabricSubmitEnvelope] = @[]
  for item in items:
    let payload = safeEncode(item.eventCertificate)
    if payload.isNone():
      return @[]
    envelopes.add(FabricSubmitEnvelope(
      itemKey: item.itemKey,
      kind: fskEventCertificate,
      payload: payload.get(),
      scopePrefix: item.scopePrefix,
    ))
  await submitFramePayloadAcked(
    network,
    peerId,
    peerIdText,
    fslOther,
    envelopes,
    submitDiagBatchKey(envelopes),
  )

proc submitCheckpointCandidate*(
    network: FabricNetwork,
    peerIdText: string,
    item: CheckpointCandidate,
    timeout: Duration = 1.seconds,
): Future[bool] {.async: (raises: []).} =
  let payload = safeEncode(item)
  if payload.isNone():
    return false
  await network.submitPayload(
    peerIdText,
    SubmitCheckpointCandidatePrefix,
    payload.get(),
    timeout = timeout,
  )

proc submitCheckpointVote*(
    network: FabricNetwork,
    peerIdText: string,
    item: CheckpointVote,
    timeout: Duration = 1.seconds,
): Future[bool] {.async: (raises: []).} =
  let payload = safeEncode(item)
  if payload.isNone():
    return false
  await network.submitPayload(
    peerIdText,
    SubmitCheckpointVotePrefix,
    payload.get(),
    timeout = timeout,
  )

proc submitCheckpointBundle*(
    network: FabricNetwork,
    peerIdText: string,
    item: CheckpointBundle,
    timeout: Duration = 1.seconds,
): Future[bool] {.async: (raises: []).} =
  let payload = safeEncode(item)
  if payload.isNone():
    return false
  await network.submitPayload(
    peerIdText,
    SubmitCheckpointBundlePrefix,
    payload.get(),
    timeout = timeout,
  )

proc fetchRaw*(
    network: FabricNetwork,
    peerIdText: string,
    key: string,
    timeout: Duration = 1.seconds,
    maxAttempts = 1,
): Future[Option[seq[byte]]] {.async: (raises: []).} =
  try:
    let peerId = PeerId.init(peerIdText).valueOr:
      when defined(fabric_lsmr_diag):
        echo "fabric-net fetch-invalid-peer self=",
          (if network.isNil or network.switch.isNil: "" else: peerIdString(network.switch.peerInfo.peerId)),
          " target=", peerIdText,
          " key=", key
      return none(seq[byte])
    when defined(fabric_lsmr_diag):
      let addrCount =
        if network.isNil or network.switch.isNil or network.switch.peerStore.isNil:
          0
        else:
          network.switch.peerStore.getAddresses(peerId).len
      let connected =
        if network.isNil or network.switch.isNil:
          false
        else:
          network.switch.isConnected(peerId)
      let addrList =
        if network.isNil or network.switch.isNil or network.switch.peerStore.isNil:
          ""
        else:
          network.switch.peerStore.getAddresses(peerId).mapIt($it).join(",")
      echo "fabric-net fetch-begin self=", peerIdString(network.switch.peerInfo.peerId),
        " t=", diagNowMs(),
        " target=", peerIdText,
        " key=", key,
        " connected=", connected,
        " addrs=", addrCount,
        " list=", addrList
    let response = await fetch(
      network.switch,
      peerId,
      key,
      timeout = timeout,
      maxAttempts = max(1, maxAttempts),
    )
    when defined(fabric_lsmr_diag):
      echo "fabric-net fetch-end self=", peerIdString(network.switch.peerInfo.peerId),
        " t=", diagNowMs(),
        " target=", peerIdText,
        " key=", key,
        " status=", ord(response.status),
        " bytes=", response.data.len
    if response.status == fsOk:
      return some(response.data)
  except CatchableError as exc:
    when defined(fabric_lsmr_diag):
      echo "fabric-net fetch-exc self=",
        (if network.isNil or network.switch.isNil: "" else: peerIdString(network.switch.peerInfo.peerId)),
        " t=", diagNowMs(),
        " target=", peerIdText,
        " key=", key,
        " err=", exc.msg
  none(seq[byte])
