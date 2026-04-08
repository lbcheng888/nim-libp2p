import std/[base64, deques, options, sequtils, strutils, tables]
from std/times import epochTime

import chronos
import chronicles

import ../../builders
import ../../lsmr
import ../../multiaddress
import ../../muxers/muxer
import ../../peerstore
import ../../services/lsmrservice
import ../../multistream
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
  FabricSubmitStreamOpenTimeout = 2.seconds
  FabricSubmitGetStreamTimeout = 500.milliseconds
  FabricSubmitBeginProtocolTimeout = 500.milliseconds
  FabricSubmitSelectTimeout = 2.seconds
  FabricSubmitWriteTimeout = 2.seconds
  FabricSubmitDisconnectSettleTimeout = 500.milliseconds
  FabricSubmitDisconnectSettlePoll = 10.milliseconds
  FabricSubmitPipelineWindow = 1
  FabricSubmitInboxCapacity = 4096
  FabricSubmitInboxBatchLimit = 64
  FabricSubmitInboxSliceBudgetMs = 25'i64
  FabricSubmitInboxYield = 0.seconds
  FabricSubmitWarmDebounce = 250.milliseconds
  FabricSubmitStreamPoolWidth = 1

type
  FabricSubmitLane* {.pure.} = enum
    fslEvent
    fslAttestation
    fslOther

  FabricSubmitSessionClass {.pure.} = enum
    fsscEvent
    fsscAttestation
    fsscEventCertificate
    fsscControl

  FabricSubmitPeerCommandKind {.pure.} = enum
    fspckConnConnected
    fspckConnDisconnected
    fspckEnsureLane
    fspckScheduleSession
    fspckWarm
    fspckRepair

  FabricSubmitKind {.pure.} = enum
    fskWarm
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
    requestId: uint64
    items: seq[FabricSubmitEnvelope]

  FabricSubmitAckItem* = object
    itemKey*: string
    accepted*: bool

  FabricSubmitAckFrame* = object
    requestId*: uint64
    items*: seq[FabricSubmitAckItem]

  FabricSubmitInboxItem = object
    itemKey: string
    sourcePeerId: string
    envelope: FabricSubmitEnvelope

  FabricSubmitAcceptDisposition {.pure.} = enum
    fsadReject
    fsadAcknowledgeOnly
    fsadHotQueue
    fsadInboxQueue

  FabricSubmitAcceptPlan = object
    itemKey: string
    disposition: FabricSubmitAcceptDisposition
    item: FabricSubmitInboxItem

  FabricSubmitRequest = ref object
    logicalLane: FabricSubmitLane
    transportClass: FabricSubmitSessionClass
    sessionShard: int
    items: seq[FabricSubmitEnvelope]
    diagItemKey: string
    encoded: seq[byte]
    writtenAtMs: int64
    completion: Future[seq[bool]]

  FabricSubmitRpcBatch = object
    requestId: uint64
    logicalLane: FabricSubmitLane
    transportClass: FabricSubmitSessionClass
    requests: seq[FabricSubmitRequest]
    items: seq[FabricSubmitEnvelope]
    diagItemKey: string
    encoded: seq[byte]

  FabricSubmitPipelineEntry = object
    batch: FabricSubmitRpcBatch
    writtenAtMs: int64

  FabricSubmitPeerCommand = object
    kind: FabricSubmitPeerCommandKind
    lane: FabricSubmitLane
    sessionClass: FabricSubmitSessionClass
    sessionShard: int
    incoming: bool

  FabricSubmitPeerStream = object
    conn: Connection
    generation: uint64
    busy: bool

  FabricSubmitPeerStreamPool = object
    nextSlot: int
    slots: array[FabricSubmitStreamPoolWidth, FabricSubmitPeerStream]

  FabricSubmitPeerConnect = ref object
    lock: AsyncLock
    rpcLock: AsyncLock
    streamLock: AsyncLock
    streams: array[FabricSubmitSessionClass, FabricSubmitPeerStreamPool]
    liveGeneration: uint64
    controlGeneration: uint64
    runner: Future[bool]
    probeRunner: Future[bool]
    connectivityRunner: Future[bool]
    actorConnConnected: bool
    actorConnDisconnected: bool
    actorConnIncoming: bool
    actorWantWarm: bool
    actorWantRepair: bool
    actorWantedSessionShards: array[FabricSubmitSessionClass, uint32]
    actorRunner: Future[void]
    runnerForceRepair: bool
    pendingForceRepair: bool
    suppressedDisconnects: int
    preferForceRepair: bool
    transportBlocked: bool
    transportVerified: bool
    connectedBaseOwnerKnown: bool
    connectedBaseOwnerId: string
    localOwnsConnectedBase: bool

  FabricSubmitSession = ref object
    peerId: PeerId
    transportClass: FabricSubmitSessionClass
    streamSlot: int
    nextRequestId: uint64
    lock: AsyncLock
    openLock: AsyncLock
    queue: Deque[FabricSubmitRequest]
    inflightCount: int
    dispatchPending: bool
    dispatchEvent: AsyncEvent
    dispatchRunner: Future[void]

  FabricControlFetchSessionClass {.pure.} = enum
    fcscSummary
    fcscPeer

  FabricControlFetchSession = ref object
    peerId: PeerId
    sessionClass: FabricControlFetchSessionClass
    lock: AsyncLock
    conn: Connection

  FabricNetworkRef = ref object
    value: FabricNetwork

  PeerHandler* = proc(network: FabricNetwork, item: PeerAnnouncement): Future[void] {.closure, gcsafe, raises: [].}
  EventHandler* = proc(network: FabricNetwork, item: FabricEvent, scopePrefix: LsmrPath, sourcePeerId: string): Future[void] {.closure, gcsafe, raises: [].}
  AttestationHandler* = proc(network: FabricNetwork, item: EventAttestation, scopePrefix: LsmrPath, sourcePeerId: string): Future[void] {.closure, gcsafe, raises: [].}
  EventCertificateHandler* = proc(network: FabricNetwork, item: EventCertificate, scopePrefix: LsmrPath, sourcePeerId: string): Future[void] {.closure, gcsafe, raises: [].}
  CheckpointCandidateHandler* = proc(network: FabricNetwork, item: CheckpointCandidate, sourcePeerId: string): Future[void] {.closure, gcsafe, raises: [].}
  CheckpointVoteHandler* = proc(network: FabricNetwork, item: CheckpointVote, sourcePeerId: string): Future[void] {.closure, gcsafe, raises: [].}
  CheckpointBundleHandler* = proc(network: FabricNetwork, item: CheckpointBundle, sourcePeerId: string): Future[void] {.closure, gcsafe, raises: [].}
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
    submitPeerConnects: Table[string, FabricSubmitPeerConnect]
    submitPeerAddrHints: Table[string, seq[MultiAddress]]
    controlFetchSessions: Table[string, FabricControlFetchSession]
    controlFetchResponseLocks: Table[string, AsyncLock]
    controlFetchResponseFlights: Table[string, Future[FetchResponse]]
    stopping: bool
    submitWarmRunner: Future[void]
    submitWarmPending: bool
    submitWarmDelayRunner: Future[void]
    submitWarmGeneration: uint64
    submitReadyHook: proc() {.gcsafe, raises: [].}
    submitConnEventHandler: ConnEventHandler
    submitHotInbox: Deque[FabricSubmitInboxItem]
    submitInbox: Deque[FabricSubmitInboxItem]
    submitInboxRunner: Future[void]
    submitInboxPending: bool
    submitAcceptDepth: int
    submitAcceptEpoch: uint64
    submitHotDrainActive: bool
    submitHotAckYieldActive: int
    submitHotResumePending: bool
    submitHotBackgroundResumePending: bool
    submitHotAcceptIdleEvent: AsyncEvent
    submitHotAcceptIdleHook: proc() {.gcsafe, raises: [].}
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

proc submitPeerReadyNow*(
    network: FabricNetwork, peerIdText: string, lane: FabricSubmitLane
): bool {.gcsafe, raises: [].}

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

proc controlFetchSessionKey(
    peerId: PeerId, sessionClass: FabricControlFetchSessionClass
): string {.gcsafe, raises: [].} =
  $peerId & ":" & $ord(sessionClass)

proc controlFetchSessionClass(
    key: string
): FabricControlFetchSessionClass {.gcsafe, raises: [].} =
  if key.startsWith("summary:"):
    fcscSummary
  else:
    fcscPeer

proc submitPeerInflightCount(
    network: FabricNetwork, peerId: PeerId
): int {.gcsafe, raises: [].}

proc submitHasLiveInflight*(
    network: FabricNetwork
): bool {.gcsafe, raises: [].}

proc submitHotAcceptBusy*(
    network: FabricNetwork
): bool {.gcsafe, raises: [].}

proc controlFetchPeerMustYield*(
    network: FabricNetwork, peerIdText: string
): bool {.gcsafe, raises: [].}

proc controlFetchSharedLiveSummaryMustYield(
    network: FabricNetwork, peerId: PeerId, key: string
): bool {.gcsafe, raises: [].}

proc controlFetchTargetSummaryMustYield(
    network: FabricNetwork, peerId: PeerId, key: string
): bool {.gcsafe, raises: [].}

proc isControlFetchKey(key: string): bool {.gcsafe, raises: [].} =
  key.startsWith("summary:") or key.startsWith("peer:self") or
    key.startsWith("peer:table")

proc splitControlFetchRequesterKey(
    key: string
): tuple[baseKey: string, requesterPeerId: string] {.gcsafe, raises: [].} =
  const requesterSuffix = "|requester:"
  let requesterIdx = key.find(requesterSuffix)
  if requesterIdx < 0:
    return (key, "")
  let requesterStart = requesterIdx + requesterSuffix.len
  if requesterStart >= key.len:
    return (key[0 ..< requesterIdx], "")
  (key[0 ..< requesterIdx], key[requesterStart .. ^1])

proc controlFetchMustYieldToLive(
    network: FabricNetwork, key: string
): bool {.gcsafe, raises: [].} =
  if network.isNil or not key.startsWith("summary:"):
    return false
  if network.submitHasLiveInflight():
    return true
  let (_, requesterPeerId) = splitControlFetchRequesterKey(key)
  network.controlFetchPeerMustYield(requesterPeerId)

proc controlFetchServerMustYieldToLive(
    network: FabricNetwork, key: string
): bool {.gcsafe, raises: [].} =
  if network.isNil or not key.startsWith("summary:"):
    return false
  if network.submitHotAcceptBusy():
    return true
  if network.controlFetchMustYieldToLive(key):
    return true
  let (_, requesterPeerId) = splitControlFetchRequesterKey(key)
  if requesterPeerId.len == 0:
    return false
  let requesterPeer = PeerId.init(requesterPeerId).valueOr:
    return false
  network.controlFetchTargetSummaryMustYield(requesterPeer, key) or
    network.controlFetchSharedLiveSummaryMustYield(requesterPeer, key)

proc controlFetchPeerMustYield*(
    network: FabricNetwork, peerIdText: string
): bool {.gcsafe, raises: [].} =
  if network.isNil or peerIdText.len == 0:
    return false
  let requesterPeer = PeerId.init(peerIdText).valueOr:
    return false
  network.submitPeerInflightCount(requesterPeer) > 0

proc controlFetchSharedLiveSummaryMustYield(
    network: FabricNetwork, peerId: PeerId, key: string
): bool {.gcsafe, raises: [].} =
  discard network
  discard peerId
  discard key
  false

proc controlFetchTargetSummaryMustYield(
    network: FabricNetwork, peerId: PeerId, key: string
): bool {.gcsafe, raises: [].} =
  if network.isNil or peerId.data.len == 0 or not key.startsWith("summary:"):
    return false
  network.submitPeerInflightCount(peerId) > 0

proc submitNetworkStopping(network: FabricNetwork): bool {.gcsafe, raises: [].} =
  not network.isNil and network.stopping

proc controlFetchResponseLockKey(key: string): string {.gcsafe, raises: [].} =
  let requesterIdx = key.find("|requester:")
  let baseKey =
    if requesterIdx >= 0:
      key[0 ..< requesterIdx]
    else:
      key
  if baseKey == "summary:root" or baseKey == "summary:head" or
      baseKey.startsWith("summary:era:"):
    return baseKey
  ""

proc getOrCreateControlFetchResponseLock(
    network: FabricNetwork, key: string
): AsyncLock {.gcsafe, raises: [].} =
  if network.isNil:
    return nil
  let lockKey = controlFetchResponseLockKey(key)
  if lockKey.len == 0:
    return nil
  let existing = network.controlFetchResponseLocks.getOrDefault(lockKey)
  if not existing.isNil:
    return existing
  result = newAsyncLock()
  network.controlFetchResponseLocks[lockKey] = result

proc closeSubmitConn(conn: Connection): Future[void] {.async: (raises: []).} =
  if conn.isNil:
    return
  try:
    await conn.close()
  except CatchableError:
    discard

proc closeSubmitConnSoon(
    conn: Connection, reason = ""
) {.gcsafe, raises: [].} =
  if conn.isNil:
    return

  proc closeConn() {.async, gcsafe.} =
    try:
      when defined(fabric_submit_diag) or defined(fabric_lsmr_diag):
        if reason.len > 0:
          echo "fabric-submit close-detached-begin reason=", reason
      await closeSubmitConn(conn)
      when defined(fabric_submit_diag) or defined(fabric_lsmr_diag):
        if reason.len > 0:
          echo "fabric-submit close-detached-done reason=", reason
    except CancelledError:
      discard
    except CatchableError:
      discard

  asyncSpawn closeConn()

proc closeControlFetchConn(
    session: FabricControlFetchSession, reason = ""
) {.async: (raises: []).} =
  if session.isNil:
    return
  let conn = session.conn
  session.conn = nil
  if conn.isNil:
    return
  when defined(fabric_lsmr_diag):
    if reason.len > 0:
      echo "fabric-fetch close peer=", $session.peerId,
        " class=", ord(session.sessionClass),
        " reason=", reason
  await closeSubmitConn(conn)

proc selectSubmitSessionMuxer(
    network: FabricNetwork,
    peerId: PeerId,
    sessionClass: FabricSubmitSessionClass,
): Muxer {.gcsafe, raises: [].}

proc openControlFetchConn(
    network: FabricNetwork, peerId: PeerId
): Future[Connection] {.async: (raises: [CancelledError, FetchError]).} =
  if network.submitNetworkStopping():
    raise (ref FetchError)(
      msg: "fabric control fetch stopping", kind: FetchErrorKind.feConfig
    )
  if network.isNil or network.switch.isNil or network.switch.connManager.isNil or
      network.switch.dialer.isNil:
    raise (ref FetchError)(
      msg: "fabric control fetch dial unavailable", kind: FetchErrorKind.feConfig
    )
  let muxer = network.selectSubmitSessionMuxer(peerId, fsscControl)
  if not muxer.isNil and not muxer.connection.isNil and
      not muxer.connection.closed and not muxer.connection.atEof:
    var stream: Connection
    try:
      stream = await network.switch.connManager.getStream(muxer)
      if stream.isNil:
        raise newException(CatchableError, "control fetch stream open returned nil")
      return await network.switch.dialer.negotiateStream(stream, @[FetchCodec])
    except CancelledError as exc:
      await closeSubmitConn(stream)
      raise exc
    except CatchableError as exc:
      await closeSubmitConn(stream)
      when defined(fabric_lsmr_diag):
        echo "fabric-fetch stream-reopen-fail self=",
          (if network.switch.isNil: "" else: peerIdString(network.switch.peerInfo.peerId)),
          " peer=", peerIdString(peerId),
          " err=", exc.msg

  var addrs: seq[MultiAddress] = @[]
  if not network.switch.peerStore.isNil:
    for addr in network.switch.peerStore.getAddresses(peerId):
      if not addrs.contains(addr):
        addrs.add(addr)
  for addr in network.submitPeerAddrHints.getOrDefault($peerId):
    if not addrs.contains(addr):
      addrs.add(addr)
  if addrs.len == 0:
    raise (ref FetchError)(
      msg: "fetch dial failed: no known addresses",
      kind: FetchErrorKind.feDialFailure,
    )

  try:
    return await network.switch.dial(
      peerId,
      addrs,
      @[FetchCodec],
      forceDial = true,
      reuseConnection = false,
    )
  except CancelledError as exc:
    raise exc
  except CatchableError as exc:
    raise (ref FetchError)(
      msg: "fetch dial failed: " & exc.msg,
      parent: exc,
      kind: FetchErrorKind.feDialFailure,
    )

proc submitLaneText(lane: FabricSubmitLane): string {.gcsafe, raises: [].} =
  case lane
  of fslEvent:
    "event"
  of fslAttestation:
    "attestation"
  of fslOther:
    "other"

proc submitKindText(kind: FabricSubmitKind): string {.gcsafe, raises: [].} =
  case kind
  of fskWarm:
    "warm"
  of fskEvent:
    "event"
  of fskAttestation:
    "attestation"
  of fskEventCertificate:
    "eventCertificate"
  of fskCheckpointCandidate:
    "checkpointCandidate"
  of fskCheckpointVote:
    "checkpointVote"
  of fskCheckpointBundle:
    "checkpointBundle"
  of fskAvoProposal:
    "avoProposal"
  of fskAvoApproval:
    "avoApproval"

proc submitBatchKindsText(items: openArray[FabricSubmitEnvelope]): string {.gcsafe, raises: [].} =
  var counts: array[FabricSubmitKind, int]
  for item in items:
    inc counts[item.kind]
  var parts: seq[string] = @[]
  for kind in FabricSubmitKind:
    if counts[kind] > 0:
      parts.add(submitKindText(kind) & "=" & $counts[kind])
  parts.join(",")

proc submitSessionKey(peerId: PeerId, sessionClass: FabricSubmitSessionClass): string {.gcsafe, raises: [].}
proc submitSessionKey(peerId: PeerId, lane: FabricSubmitLane): string {.gcsafe, raises: [].}
proc submitSessionClassHot(
    sessionClass: FabricSubmitSessionClass
): bool {.gcsafe, raises: [].}
proc submitSessionPlaneKind(
    sessionClass: FabricSubmitSessionClass
): PeerPlaneKind {.gcsafe, raises: [].}
proc submitPeerStreamPoolSize(
    sessionClass: FabricSubmitSessionClass
): int {.gcsafe, raises: [].}
proc submitPeerStreamGeneration(
    state: FabricSubmitPeerConnect,
    sessionClass: FabricSubmitSessionClass,
): uint64 {.gcsafe, raises: [].}
proc submitPeerStreamSlotReady(
    state: FabricSubmitPeerConnect,
    sessionClass: FabricSubmitSessionClass,
    slotIdx: int,
): bool {.gcsafe, raises: [].}
proc desiredSubmitPeers(network: FabricNetwork): seq[PeerId] {.gcsafe, raises: [].}
proc submitItemsUniformKind(
    items: openArray[FabricSubmitEnvelope]
): Option[FabricSubmitKind] {.gcsafe, raises: [].}
proc submitPeerStoreAddrs(
    network: FabricNetwork, peerId: PeerId
): seq[MultiAddress] {.gcsafe, raises: [].}
proc submitPeerHintAddrs(
    network: FabricNetwork, peerId: PeerId
): seq[MultiAddress] {.gcsafe, raises: [].}
proc submitPeerAddrs(
    network: FabricNetwork, peerId: PeerId
): seq[MultiAddress] {.gcsafe, raises: [].}
proc submitPeerHasDemand(
    network: FabricNetwork, peerId: PeerId
): bool {.gcsafe, raises: [].}
proc scheduleWarmSubmitPeer(
    network: FabricNetwork, peerId: PeerId
) {.gcsafe, raises: [].}

proc submitBatchCarriesDemand(
    batch: FabricSubmitRpcBatch
): bool {.gcsafe, raises: [].} =
  for item in batch.items:
    if item.kind != fskWarm:
      return true
  false

proc submitBatchNeedsRepair(
    network: FabricNetwork,
    peerId: PeerId,
    stage: string,
    batchCarriesDemand: bool,
): bool {.gcsafe, raises: [].} =
  if network.isNil or network.switch.isNil or peerId.data.len == 0:
    return false
  if stage == "transport-not-ready":
    return false
  let connCount =
    if network.switch.connManager.isNil: 0
    else: network.switch.connManager.connCount(peerId)
  if not network.switch.isConnected(peerId) or connCount == 0:
    return true
  discard batchCarriesDemand
  false

proc submitPeerBaseState(
    network: FabricNetwork, peerId: PeerId
): tuple[connected: bool, connCount: int] {.gcsafe, raises: [].} =
  if network.isNil or network.switch.isNil or peerId.data.len == 0:
    return (false, 0)
  result.connected = network.switch.isConnected(peerId)
  result.connCount =
    if network.switch.connManager.isNil:
      (if result.connected: 1 else: 0)
    else:
      network.switch.connManager.connCount(peerId)

proc submitPeerLiveBaseOwnerState(
    network: FabricNetwork, peerId: PeerId
): tuple[known, localOwns: bool] {.gcsafe, raises: [].} =
  if network.isNil or network.switch.isNil or network.switch.connManager.isNil or
      peerId.data.len == 0:
    return (false, false)
  let plane = network.switch.connManager.reservePlane(peerId, ppkLive, Direction.Out)
  if not plane.ready:
    return (false, false)
  (true, plane.direction == Direction.Out)

proc submitPeerLiveBaseOwnerId(
    network: FabricNetwork, peerId: PeerId
): string {.gcsafe, raises: [].} =
  if network.isNil or network.switch.isNil or network.switch.connManager.isNil or
      peerId.data.len == 0:
    return ""
  network.switch.connManager.planeState(peerId, ppkLive).ownerMuxerId

proc submitPeerLiveBaseOwnerChanged(
    state: FabricSubmitPeerConnect,
    ownerId: string,
    known: bool,
    localOwns: bool,
): bool {.gcsafe, raises: [].} =
  if state.isNil:
    return true
  state.connectedBaseOwnerId != ownerId or
    state.connectedBaseOwnerKnown != known or
    state.localOwnsConnectedBase != localOwns

proc submitPeerSelectedBaseOutgoing(
    network: FabricNetwork, peerId: PeerId
): bool {.gcsafe, raises: [].} =
  if network.isNil or network.switch.isNil or peerId.data.len == 0:
    return false
  if network.switch.connManager.isNil:
    return false
  let plane = network.switch.connManager.planeState(peerId, ppkLive)
  plane.ready and plane.direction == Direction.Out

proc connectionBaseConn(conn: Connection): Connection {.gcsafe, raises: [].} =
  result = conn
  while not result.isNil:
    let wrapped = result.getWrapped()
    if wrapped.isNil or wrapped == result:
      break
    result = wrapped

proc submitPeerHotBaseConn(
    network: FabricNetwork, peerId: PeerId
): Connection {.gcsafe, raises: [].} =
  if network.isNil or peerId.data.len == 0:
    return nil
  let state = network.submitPeerConnects.getOrDefault($peerId)
  if state.isNil:
    return nil
  for sessionClass in [fsscEvent, fsscAttestation]:
    for slotIdx in 0 ..< submitPeerStreamPoolSize(sessionClass):
      if state.submitPeerStreamSlotReady(sessionClass, slotIdx):
        return connectionBaseConn(state.streams[sessionClass].slots[slotIdx].conn)
  nil

proc selectSubmitSessionMuxer(
    network: FabricNetwork,
    peerId: PeerId,
    sessionClass: FabricSubmitSessionClass,
): Muxer {.gcsafe, raises: [].} =
  if network.isNil or network.switch.isNil or network.switch.connManager.isNil or
      peerId.data.len == 0:
    return nil
  let planeKind =
    if submitSessionClassHot(sessionClass):
      ppkLive
    else:
      ppkControl
  network.switch.connManager.selectPlaneMuxer(peerId, planeKind)

proc submitSessionClassLane(
    sessionClass: FabricSubmitSessionClass
): FabricSubmitLane {.gcsafe, raises: [].} =
  case sessionClass
  of fsscEvent:
    fslEvent
  of fsscAttestation:
    fslAttestation
  of fsscEventCertificate, fsscControl:
    fslOther

proc submitSessionClassHot(
    sessionClass: FabricSubmitSessionClass
): bool {.gcsafe, raises: [].} =
  sessionClass == fsscEvent or sessionClass == fsscAttestation

proc submitSessionPlaneKind(
    sessionClass: FabricSubmitSessionClass
): PeerPlaneKind {.gcsafe, raises: [].} =
  if submitSessionClassHot(sessionClass):
    ppkLive
  else:
    ppkControl

proc submitSessionClassText(
    sessionClass: FabricSubmitSessionClass
): string {.gcsafe, raises: [].} =
  case sessionClass
  of fsscEvent:
    "event"
  of fsscAttestation:
    "attestation"
  of fsscEventCertificate:
    "eventCertificate"
  of fsscControl:
    "control"

proc submitSessionClassForKind(
    kind: FabricSubmitKind
): FabricSubmitSessionClass {.gcsafe, raises: [].} =
  case kind
  of fskWarm, fskEvent:
    fsscEvent
  of fskAttestation:
    fsscAttestation
  of fskEventCertificate:
    fsscEventCertificate
  of fskCheckpointCandidate,
      fskCheckpointVote,
      fskCheckpointBundle,
      fskAvoProposal,
      fskAvoApproval:
    fsscControl

proc submitSessionClassForLane(
    lane: FabricSubmitLane
): FabricSubmitSessionClass {.gcsafe, raises: [].} =
  case lane
  of fslEvent:
    fsscEvent
  of fslAttestation:
    fsscAttestation
  of fslOther:
    fsscControl

proc submitSessionClassForItems(
    lane: FabricSubmitLane,
    items: openArray[FabricSubmitEnvelope],
): FabricSubmitSessionClass {.gcsafe, raises: [].} =
  let kind = submitItemsUniformKind(items)
  if kind.isSome():
    return submitSessionClassForKind(kind.get())
  submitSessionClassForLane(lane)

proc submitLaneForKind(kind: FabricSubmitKind): FabricSubmitLane {.gcsafe, raises: [].} =
  submitSessionClassLane(submitSessionClassForKind(kind))

proc submitChunkItemLimit(
    logicalLane, transportLane: FabricSubmitLane
): int {.gcsafe, raises: [].} =
  discard transportLane
  case logicalLane
  of fslEvent:
    16
  of fslAttestation:
    8
  of fslOther:
    8

proc submitItemsUniformKind(
    items: openArray[FabricSubmitEnvelope]
): Option[FabricSubmitKind] {.gcsafe, raises: [].} =
  if items.len == 0:
    return none(FabricSubmitKind)
  let kind = items[0].kind
  for item in items:
    if item.kind != kind:
      return none(FabricSubmitKind)
  some(kind)

proc submitBatchItemLimit(
    logicalLane, transportLane: FabricSubmitLane,
    items: openArray[FabricSubmitEnvelope],
): int {.gcsafe, raises: [].} =
  let baseLimit = submitChunkItemLimit(logicalLane, transportLane)
  let kind = submitItemsUniformKind(items)
  if kind.isSome() and kind.get() == fskEventCertificate:
    return min(baseLimit, 4)
  baseLimit

proc submitBatchKindsCompatible(
    currentItems: openArray[FabricSubmitEnvelope],
    nextItems: openArray[FabricSubmitEnvelope],
): bool {.gcsafe, raises: [].} =
  let currentKind = submitItemsUniformKind(currentItems)
  let nextKind = submitItemsUniformKind(nextItems)
  if currentKind.isSome() and currentKind.get() == fskEventCertificate:
    return nextKind.isSome() and nextKind.get() == fskEventCertificate
  if nextKind.isSome() and nextKind.get() == fskEventCertificate:
    return currentKind.isSome() and currentKind.get() == fskEventCertificate
  true

proc submitSessionShardCount(
    sessionClass: FabricSubmitSessionClass
): int {.gcsafe, raises: [].} =
  case sessionClass
  of fsscEventCertificate:
    FabricSubmitStreamPoolWidth
  else:
    1

proc submitSessionShardBit(shardIdx: int): uint32 {.gcsafe, raises: [].} =
  if shardIdx < 0 or shardIdx >= 32:
    return 0'u32
  1'u32 shl shardIdx

proc submitSessionAllShardBits(
    sessionClass: FabricSubmitSessionClass
): uint32 {.gcsafe, raises: [].} =
  let shardCount = submitSessionShardCount(sessionClass)
  for shardIdx in 0 ..< shardCount:
    result = result or submitSessionShardBit(shardIdx)

proc submitSessionShardQueued(
    wanted: array[FabricSubmitSessionClass, uint32],
    sessionClass: FabricSubmitSessionClass,
    shardIdx: int,
): bool {.gcsafe, raises: [].} =
  (wanted[sessionClass] and submitSessionShardBit(shardIdx)) != 0

proc submitSessionShardsQueued(
    wanted: array[FabricSubmitSessionClass, uint32]
): bool {.gcsafe, raises: [].} =
  for sessionClass in FabricSubmitSessionClass:
    if wanted[sessionClass] != 0:
      return true
  false

proc submitSessionKey(
    peerId: PeerId, sessionClass: FabricSubmitSessionClass, shardIdx: int
): string {.gcsafe, raises: [].} =
  if peerId.data.len == 0:
    return ""
  $peerId & ":" & $ord(sessionClass) & ":" & $shardIdx

proc submitSessionKey(peerId: PeerId, sessionClass: FabricSubmitSessionClass): string {.gcsafe, raises: [].} =
  submitSessionKey(peerId, sessionClass, 0)

proc submitSessionKey(peerId: PeerId, lane: FabricSubmitLane): string {.gcsafe, raises: [].} =
  submitSessionKey(peerId, submitSessionClassForLane(lane))

proc submitPeerSessions(
    network: FabricNetwork, peerId: PeerId
): seq[FabricSubmitSession] {.gcsafe, raises: [].} =
  if network.isNil or peerId.data.len == 0:
    return @[]
  for sessionClass in FabricSubmitSessionClass:
    for shardIdx in 0 ..< submitSessionShardCount(sessionClass):
      let session = network.submitSessions.getOrDefault(
        submitSessionKey(peerId, sessionClass, shardIdx)
      )
      if not session.isNil:
        result.add(session)

proc submitPeerSession(
    network: FabricNetwork,
    peerId: PeerId,
    sessionClass: FabricSubmitSessionClass,
    shardIdx = 0,
): FabricSubmitSession {.gcsafe, raises: [].} =
  if network.isNil or peerId.data.len == 0:
    return nil
  network.submitSessions.getOrDefault(submitSessionKey(peerId, sessionClass, shardIdx))

proc submitPeerLanePendingLen(
    network: FabricNetwork, peerId: PeerId, lane: FabricSubmitLane
): int {.gcsafe, raises: [].} =
  for session in network.submitPeerSessions(peerId):
    if submitSessionClassLane(session.transportClass) == lane:
      inc result, session.queue.len

proc submitPeerInflightCount(
    network: FabricNetwork, peerId: PeerId
): int {.gcsafe, raises: [].} =
  for session in network.submitPeerSessions(peerId):
    inc result, session.inflightCount

proc submitTransportDispatchable(
    network: FabricNetwork, peerId: PeerId, lane: FabricSubmitLane
): bool {.gcsafe, raises: [].}

proc submitHasLiveInflight*(
    network: FabricNetwork
): bool {.gcsafe, raises: [].} =
  if network.isNil:
    return false
  for session in network.submitSessions.values:
    if session.isNil or session.inflightCount <= 0:
      continue
    case session.transportClass
    of fsscEvent, fsscAttestation:
      return true
    else:
      discard
  false

proc submitHotAcceptBusy*(
    network: FabricNetwork
): bool {.gcsafe, raises: [].} =
  if network.isNil:
    return false
  network.submitAcceptDepth > 0 or
    network.submitHotDrainActive or network.submitHotAckYieldActive > 0 or
    network.submitHotResumePending or network.submitHotInbox.len > 0

proc submitAcceptGateActive(
    network: FabricNetwork
): bool {.gcsafe, raises: [].} =
  if network.isNil:
    return false
  network.submitAcceptDepth > 0 or
    network.submitHotAckYieldActive > 0

proc signalSubmitHotAcceptIdle(network: FabricNetwork) {.gcsafe, raises: [].} =
  if network.isNil or network.submitHotAcceptBusy() or
      network.submitHotAcceptIdleHook.isNil and network.submitHotAcceptIdleEvent.isNil:
    return
  if not network.submitHotAcceptIdleEvent.isNil:
    network.submitHotAcceptIdleEvent.fire()
  if network.submitHotAcceptIdleHook.isNil or
      network.submitHotBackgroundResumePending:
    return
  network.submitHotBackgroundResumePending = true

  proc resumeBackground() {.async: (raises: []).} =
    try:
      await sleepAsync(0.seconds)
    except CancelledError:
      discard
    finally:
      if network.isNil:
        return
      network.submitHotBackgroundResumePending = false
      if network.submitHotAcceptBusy() or network.submitHotAcceptIdleHook.isNil:
        return
      network.submitHotAcceptIdleHook()

  asyncSpawn resumeBackground()

proc waitSubmitHotAcceptIdle(
    network: FabricNetwork
): Future[void] {.async: (raises: [CancelledError]).} =
  if network.isNil or network.submitHotAcceptIdleEvent.isNil:
    return
  while network.submitHotAcceptBusy():
    network.submitHotAcceptIdleEvent.clear()
    if not network.submitHotAcceptBusy():
      break
    await network.submitHotAcceptIdleEvent.wait()

proc submitPeerPendingLen(
    network: FabricNetwork, peerId: PeerId
): int {.gcsafe, raises: [].} =
  for session in network.submitPeerSessions(peerId):
    inc result, session.queue.len

proc submitPeerHasDemand(
    network: FabricNetwork, peerId: PeerId
): bool {.gcsafe, raises: [].} =
  if network.isNil or peerId.data.len == 0:
    return false
  for session in network.submitPeerSessions(peerId):
    if session.inflightCount > 0:
      return true
    if session.queue.len > 0:
      return true
  false

proc submitPeerHasLocalRepairClaim(
    state: FabricSubmitPeerConnect
): bool {.gcsafe, raises: [].} =
  not state.isNil and (
    state.preferForceRepair or state.pendingForceRepair or state.runnerForceRepair
  )

proc submitPeerDesired(
    network: FabricNetwork, peerId: PeerId
): bool {.gcsafe, raises: [].} =
  if network.isNil or network.switch.isNil or peerId.data.len == 0:
    return false
  for candidate in network.desiredSubmitPeers():
    if candidate == peerId:
      return true
  false

proc initSubmitLaneQueues(): array[FabricSubmitLane, Deque[FabricSubmitRequest]] {.gcsafe, raises: [].} =
  for lane in FabricSubmitLane:
    result[lane] = initDeque[FabricSubmitRequest]()

proc completeSubmitRequest(
    request: FabricSubmitRequest, accepted: seq[bool]
) {.gcsafe, raises: [].} =
  if request.isNil or request.completion.isNil or request.completion.finished():
    return
  request.completion.complete(accepted)

proc completeSubmitBatch(
    batch: FabricSubmitRpcBatch, accepted: seq[bool]
) {.gcsafe, raises: [].} =
  if batch.requests.len == 0:
    return
  var cursor = 0
  for request in batch.requests:
    if request.isNil:
      continue
    let itemCount = request.items.len
    if itemCount == 0 or cursor + itemCount > accepted.len:
      for pending in batch.requests:
        if not pending.isNil:
          pending.completeSubmitRequest(@[])
      return
    request.completeSubmitRequest(accepted[cursor ..< cursor + itemCount])
    cursor += itemCount
  if cursor != accepted.len:
    for request in batch.requests:
      if not request.isNil:
        request.completeSubmitRequest(@[])

proc failSubmitBatch(batch: FabricSubmitRpcBatch) {.gcsafe, raises: [].} =
  for request in batch.requests:
    if not request.isNil:
      request.completeSubmitRequest(@[])

proc buildSubmitRequest(
    lane: FabricSubmitLane,
    items: seq[FabricSubmitEnvelope],
    diagItemKey: string,
    encoded: seq[byte],
): FabricSubmitRequest {.gcsafe, raises: [].} =
  if items.len == 0 or encoded.len == 0:
    return nil
  FabricSubmitRequest(
    logicalLane: lane,
    transportClass: submitSessionClassForItems(lane, items),
    sessionShard: 0,
    items: items,
    diagItemKey: diagItemKey,
    encoded: encoded,
    writtenAtMs: 0,
    completion: newFuture[seq[bool]]("fabric.submit.batch"),
  )

proc failSubmitRequests(
    queue: var Deque[FabricSubmitRequest]
) {.gcsafe, raises: [].} =
  while queue.len > 0:
    let request = queue.popFirst()
    if request.isNil:
      continue
    request.completeSubmitRequest(@[])

proc submitPendingLen(session: FabricSubmitSession): int {.gcsafe, raises: [].} =
  if session.isNil:
    return 0
  session.queue.len

proc submitHasPending(session: FabricSubmitSession): bool {.gcsafe, raises: [].} =
  not session.isNil and session.submitPendingLen() > 0

proc submitLanePendingLen(
    session: FabricSubmitSession, lane: FabricSubmitLane
): int {.gcsafe, raises: [].} =
  if session.isNil:
    return 0
  if submitSessionClassLane(session.transportClass) == lane:
    session.queue.len
  else:
    0

proc enqueueSubmitRequest(
    session: FabricSubmitSession,
    request: FabricSubmitRequest,
    front = false,
) {.gcsafe, raises: [].} =
  if session.isNil or request.isNil:
    return
  if front:
    session.queue.addFirst(request)
  else:
    session.queue.addLast(request)

proc popSubmitRequest(
    session: FabricSubmitSession
): FabricSubmitRequest {.gcsafe, raises: [].} =
  if session.isNil:
    return nil
  if session.queue.len > 0:
    return session.queue.popFirst()
  nil

proc submitMaxInflight(
    session: FabricSubmitSession
): int {.gcsafe, raises: [].} =
  discard session
  1

proc submitBatchPipelineWindow(
    sessionClass: FabricSubmitSessionClass
): int {.gcsafe, raises: [].} =
  case sessionClass
  of fsscEventCertificate:
    FabricSubmitPipelineWindow
  else:
    1

proc submitPeerBusy(
    network: FabricNetwork, peerId: PeerId
): bool {.gcsafe, raises: [].} =
  if network.isNil or peerId.data.len == 0:
    return false
  for session in network.submitPeerSessions(peerId):
    if not session.openLock.isNil and session.openLock.locked():
      return true
  let state = network.submitPeerConnects.getOrDefault($peerId)
  if not state.isNil:
    for sessionClass in FabricSubmitSessionClass:
      let poolSize =
        case sessionClass
        of fsscEventCertificate:
          FabricSubmitStreamPoolWidth
        else:
          1
      for slotIdx in 0 ..< poolSize:
        if state.streams[sessionClass].slots[slotIdx].busy:
          return true
  if not state.isNil and not state.streamLock.isNil and state.streamLock.locked():
    return true
  false

proc submitPeerStreamPoolSize(
    sessionClass: FabricSubmitSessionClass
): int {.gcsafe, raises: [].} =
  submitSessionShardCount(sessionClass)

proc submitPeerStreamGeneration(
    state: FabricSubmitPeerConnect,
    sessionClass: FabricSubmitSessionClass,
): uint64 {.gcsafe, raises: [].} =
  if state.isNil:
    return 0
  case submitSessionPlaneKind(sessionClass)
  of ppkLive:
    state.liveGeneration
  of ppkControl:
    state.controlGeneration

proc submitPeerStreamSlotReady(
    state: FabricSubmitPeerConnect,
    sessionClass: FabricSubmitSessionClass,
    slotIdx: int,
): bool {.gcsafe, raises: [].} =
  not state.isNil and
    slotIdx >= 0 and
    slotIdx < submitPeerStreamPoolSize(sessionClass) and
    not state.streams[sessionClass].slots[slotIdx].conn.isNil and
    not state.streams[sessionClass].slots[slotIdx].conn.closed and
    not state.streams[sessionClass].slots[slotIdx].conn.atEof and
    state.streams[sessionClass].slots[slotIdx].generation ==
      state.submitPeerStreamGeneration(sessionClass)

proc submitPeerStreamReady(
    state: FabricSubmitPeerConnect, sessionClass: FabricSubmitSessionClass
): bool {.gcsafe, raises: [].} =
  if state.isNil:
    return false
  for slotIdx in 0 ..< submitPeerStreamPoolSize(sessionClass):
    if state.submitPeerStreamSlotReady(sessionClass, slotIdx):
      return true
  false

proc submitPeerStreamReady(
    network: FabricNetwork, peerId: PeerId, sessionClass: FabricSubmitSessionClass
): bool {.gcsafe, raises: [].} =
  if network.isNil or peerId.data.len == 0:
    return false
  let state = network.submitPeerConnects.getOrDefault($peerId)
  state.submitPeerStreamReady(sessionClass)

proc submitPeerHotSessionNeedsWarm(
    state: FabricSubmitPeerConnect,
    wanted: array[FabricSubmitSessionClass, uint32]
): bool {.gcsafe, raises: [].} =
  if state.isNil:
    return false
  for sessionClass in [fsscEvent, fsscAttestation]:
    if wanted[sessionClass] != 0 and
        not state.submitPeerStreamReady(sessionClass):
      return true
  false

proc submitHotSessionNeedsDirectProbe(
    network: FabricNetwork,
    peerId: PeerId,
    sessionClass: FabricSubmitSessionClass,
): bool {.gcsafe, raises: [].} =
  if network.isNil or network.switch.isNil or peerId.data.len == 0:
    return false
  if not submitSessionClassHot(sessionClass):
    return false
  if not network.switch.isConnected(peerId):
    return false
  let state = network.submitPeerConnects.getOrDefault($peerId)
  if state.isNil:
    return true
  (not state.transportVerified) or
    (not state.submitPeerStreamReady(sessionClass))

proc clearSubmitPeerStream(
    network: FabricNetwork,
    peerId: PeerId,
    sessionClass: FabricSubmitSessionClass,
    reason: string,
) {.gcsafe, raises: [].} =
  if network.isNil or peerId.data.len == 0:
    return
  let state = network.submitPeerConnects.getOrDefault($peerId)
  if state.isNil:
    return
  for slotIdx in 0 ..< submitPeerStreamPoolSize(sessionClass):
    let conn = state.streams[sessionClass].slots[slotIdx].conn
    state.streams[sessionClass].slots[slotIdx].conn = nil
    state.streams[sessionClass].slots[slotIdx].generation = 0
    state.streams[sessionClass].slots[slotIdx].busy = false
    if not conn.isNil:
      closeSubmitConnSoon(conn, reason)

proc clearSubmitPeerStreams(
    network: FabricNetwork, peerId: PeerId, reason: string
) {.gcsafe, raises: [].} =
  if network.isNil or peerId.data.len == 0:
    return
  for sessionClass in FabricSubmitSessionClass:
    network.clearSubmitPeerStream(peerId, sessionClass, reason)

proc getOrCreateSubmitPeerConnect(
    network: FabricNetwork, peerId: PeerId
): FabricSubmitPeerConnect {.gcsafe, raises: [].}
proc dialSubmitPeer(
    network: FabricNetwork, peerId: PeerId, allowRedial: bool
): Future[Connection] {.async: (raises: [CancelledError]).}
proc dialSubmitExistingStream(
    network: FabricNetwork,
    peerId: PeerId,
    sessionClass = fsscEvent,
    escalateBlocked = true,
): Future[Connection] {.async: (raises: [CancelledError]).}

proc reserveSubmitSessionStream(
    network: FabricNetwork,
    peerId: PeerId,
    session: FabricSubmitSession,
    escalateBlocked = true,
): Future[tuple[conn: Connection, slotIdx: int, reused: bool]]
    {.async: (raises: [CancelledError]).} =
  if network.isNil or peerId.data.len == 0 or session.isNil:
    return (nil, -1, false)
  let state = network.getOrCreateSubmitPeerConnect(peerId)
  var lockHeld = false
  var chosenSlot = -1
  var reservedGeneration = 0'u64
  var staleConn: Connection = nil
  try:
    if not state.isNil and not state.streamLock.isNil:
      await state.streamLock.acquire()
      lockHeld = true
    let poolSize = submitPeerStreamPoolSize(session.transportClass)
    chosenSlot =
      if session.streamSlot >= 0 and session.streamSlot < poolSize:
        session.streamSlot
      else:
        0
    if not state.streams[session.transportClass].slots[chosenSlot].busy and
        state.submitPeerStreamSlotReady(session.transportClass, chosenSlot):
      state.streams[session.transportClass].slots[chosenSlot].busy = true
      return (
        state.streams[session.transportClass].slots[chosenSlot].conn,
        chosenSlot,
        true,
      )
    elif state.streams[session.transportClass].slots[chosenSlot].busy:
      return (nil, -1, false)
    else:
      reservedGeneration = state.submitPeerStreamGeneration(session.transportClass)
      staleConn = state.streams[session.transportClass].slots[chosenSlot].conn
      state.streams[session.transportClass].slots[chosenSlot].conn = nil
      state.streams[session.transportClass].slots[chosenSlot].generation = reservedGeneration
      state.streams[session.transportClass].slots[chosenSlot].busy = true
  finally:
    if lockHeld:
      try:
        if state.streamLock.locked():
          state.streamLock.release()
      except AsyncLockError:
        discard
  if not staleConn.isNil:
    closeSubmitConnSoon(staleConn, "stream-stale")
  let stream = await network.dialSubmitExistingStream(
    peerId,
    session.transportClass,
    escalateBlocked = escalateBlocked,
  )
  var installHeld = false
  var installed = false
  try:
    if not state.isNil and not state.streamLock.isNil:
      await state.streamLock.acquire()
      installHeld = true
    if chosenSlot >= 0 and chosenSlot < submitPeerStreamPoolSize(session.transportClass):
      let slot = addr state.streams[session.transportClass].slots[chosenSlot]
      let reservationOwned =
        slot.busy and slot.conn.isNil and slot.generation == reservedGeneration
      if reservationOwned and not stream.isNil and
          reservedGeneration == state.submitPeerStreamGeneration(session.transportClass):
        slot.conn = stream
        slot.generation = state.submitPeerStreamGeneration(session.transportClass)
        slot.busy = true
        installed = true
      elif reservationOwned:
        slot.conn = nil
        slot.generation = 0
        slot.busy = false
  finally:
    if installHeld:
      try:
        if state.streamLock.locked():
          state.streamLock.release()
      except AsyncLockError:
        discard
  if installed:
    return (stream, chosenSlot, false)
  if not stream.isNil:
    closeSubmitConnSoon(stream, "stream-open-abort")
  (nil, -1, false)

proc releaseSubmitSessionStream(
    network: FabricNetwork,
    peerId: PeerId,
    sessionClass: FabricSubmitSessionClass,
    slotIdx: int,
    stream: Connection,
    reason = "",
) {.gcsafe, raises: [].} =
  if network.isNil or peerId.data.len == 0 or slotIdx < 0:
    if reason.len > 0 and not stream.isNil:
      closeSubmitConnSoon(stream, reason)
    return
  let state = network.submitPeerConnects.getOrDefault($peerId)
  if state.isNil or slotIdx >= submitPeerStreamPoolSize(sessionClass):
    if reason.len > 0 and not stream.isNil:
      closeSubmitConnSoon(stream, reason)
    return
  state.streams[sessionClass].slots[slotIdx].busy = false
  if reason.len == 0:
    return
  var detached: Connection = nil
  if state.streams[sessionClass].slots[slotIdx].conn == stream:
    detached = state.streams[sessionClass].slots[slotIdx].conn
    state.streams[sessionClass].slots[slotIdx].conn = nil
    state.streams[sessionClass].slots[slotIdx].generation = 0
  elif not stream.isNil:
    detached = stream
  if not detached.isNil:
    closeSubmitConnSoon(detached, reason)

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

proc awaitSubmitFutureWithin[T](
    fut: Future[T], timeout: Duration, timeoutMsg: string
): Future[T] {.async: (raises: [CancelledError, LPError]).} =
  let timeoutFut = sleepAsync(timeout)
  let winner = await race(cast[FutureBase](fut), cast[FutureBase](timeoutFut))
  if winner == cast[FutureBase](timeoutFut):
    fut.cancelSoon()
    raise (ref LPError)(msg: timeoutMsg)
  timeoutFut.cancelSoon()
  try:
    return await fut
  except CancelledError as exc:
    raise exc
  except CatchableError as exc:
    raise (ref LPError)(msg: timeoutMsg & ": " & exc.msg, parent: exc)

proc submitDiagItemKey(kind: FabricSubmitKind, payload: string): string {.gcsafe, raises: [].} =
  case kind
  of fskWarm:
    return payload
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

proc stableSubmitShardHash(key: string): uint32 {.gcsafe, raises: [].} =
  var hash = 2166136261'u32
  for ch in key:
    hash = (hash xor uint32(ord(ch))) * 16777619'u32
  hash

proc submitRequestShard(
    sessionClass: FabricSubmitSessionClass,
    diagItemKey: string,
    items: openArray[FabricSubmitEnvelope],
): int {.gcsafe, raises: [].} =
  let shardCount = submitSessionShardCount(sessionClass)
  if shardCount <= 1:
    return 0
  var shardKey = diagItemKey
  if shardKey.len == 0 and items.len > 0:
    shardKey = effectiveSubmitItemKey(items[0])
  if shardKey.len == 0:
    return 0
  int(stableSubmitShardHash(shardKey) mod uint32(shardCount))

proc acceptedAckCount(acked: openArray[bool]): int {.gcsafe, raises: [].} =
  for accepted in acked:
    if accepted:
      inc result

proc decodeSubmitAckResults(
    items: openArray[FabricSubmitEnvelope], frame: FabricSubmitAckFrame
): tuple[ok: bool, accepted: seq[bool], reason: string] {.gcsafe, raises: [].}
proc setSubmitPeerTransportVerified(
    network: FabricNetwork,
    peerId: PeerId,
    verified: bool,
    reason: string,
) {.gcsafe, raises: [].}

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

proc emptySubmitAckFrame(requestId = 0'u64): FabricSubmitAckFrame {.gcsafe, raises: [].} =
  FabricSubmitAckFrame(requestId: requestId, items: @[])

proc encodeSubmitFrame(
    requestId: uint64, items: openArray[FabricSubmitEnvelope]
): Option[string] {.gcsafe, raises: [].} =
  safeEncode(FabricSubmitFrame(requestId: requestId, items: @items))

proc splitSubmitFrameChunks(
    items: seq[FabricSubmitEnvelope], maxItems = high(int)
): seq[seq[FabricSubmitEnvelope]] {.gcsafe, raises: [].} =
  if items.len == 0:
    return @[]
  var current: seq[FabricSubmitEnvelope] = @[]
  for item in items:
    if current.len >= maxItems:
      result.add(current)
      current = @[]
    var candidate = current
    candidate.add(item)
    let encodedCandidate = encodeSubmitFrame(0, candidate)
    if encodedCandidate.isSome() and bytesOf(encodedCandidate.get()).len <= SubmitMaxMessageBytes:
      current = candidate
      continue
    if current.len > 0:
      result.add(current)
      current = @[]
    let encodedSingle = encodeSubmitFrame(0, @[item])
    if encodedSingle.isNone() or bytesOf(encodedSingle.get()).len > SubmitMaxMessageBytes:
      return @[]
    current.add(item)
  if current.len > 0:
    result.add(current)

proc processSubmitInboxItem(
    network: FabricNetwork, item: FabricSubmitInboxItem
): Future[void] {.async: (raises: [CancelledError]).}

proc scheduleSubmitInboxDrain(network: FabricNetwork) {.gcsafe, raises: [].}
proc scheduleSubmitHotResume(network: FabricNetwork) {.gcsafe, raises: [].}
proc runSubmitRequestRpc(
    network: FabricNetwork,
    peerId: PeerId,
    peerIdText: string,
    batch: FabricSubmitRpcBatch,
): Future[void] {.async: (raises: [CancelledError]).}
proc dispatchSubmitSessionDirect(
    network: FabricNetwork,
    peerId: PeerId,
    peerIdText: string,
    sessionClass: FabricSubmitSessionClass,
    sessionShard: int = 0,
): Future[void] {.async: (raises: [CancelledError]).}
proc signalSubmitSessionDispatch(
    network: FabricNetwork,
    peerId: PeerId,
    peerIdText: string,
    sessionClass: FabricSubmitSessionClass,
    sessionShard: int = 0,
): Future[void] {.async: (raises: [CancelledError]).}
proc scheduleSubmitSession(
    network: FabricNetwork,
    peerId: PeerId,
    peerIdText: string,
    sessionClass: FabricSubmitSessionClass,
    sessionShard: int = 0,
) {.gcsafe, raises: [].}
proc scheduleSubmitSession(
    network: FabricNetwork,
    peerId: PeerId,
    peerIdText: string,
    lane: FabricSubmitLane,
) {.gcsafe, raises: [].}

proc buildSubmitRpcBatch(
    session: FabricSubmitSession,
    firstRequest: FabricSubmitRequest,
): FabricSubmitRpcBatch {.gcsafe, raises: [].} =
  if session.isNil or firstRequest.isNil:
    return
  result.logicalLane = firstRequest.logicalLane
  result.transportClass = session.transportClass
  result.requests = @[firstRequest]
  result.items = firstRequest.items
  let transportLane = submitSessionClassLane(result.transportClass)
  let maxItems = submitBatchItemLimit(
    result.logicalLane,
    transportLane,
    result.items,
  )
  while result.items.len < maxItems:
    if session.queue.len == 0:
      break
    let nextRequest = session.queue.popFirst()
    if nextRequest.isNil:
      break
    if not submitBatchKindsCompatible(result.items, nextRequest.items):
      session.queue.addFirst(nextRequest)
      break
    if nextRequest.items.len == 0 or result.items.len + nextRequest.items.len > maxItems:
      session.queue.addFirst(nextRequest)
      break
    var candidateItems = result.items
    for item in nextRequest.items:
      candidateItems.add(item)
    let encodedCandidate = encodeSubmitFrame(0, candidateItems)
    if encodedCandidate.isNone() or bytesOf(encodedCandidate.get()).len > SubmitMaxMessageBytes:
      session.queue.addFirst(nextRequest)
      break
    result.requests.add(nextRequest)
    result.items = candidateItems
  result.diagItemKey = submitDiagBatchKey(result.items)
  result.requestId = session.nextRequestId
  inc session.nextRequestId
  let encoded = encodeSubmitFrame(result.requestId, result.items)
  if encoded.isSome():
    result.encoded = bytesOf(encoded.get())

proc popNextSubmitRpcBatch(
    session: FabricSubmitSession,
): FabricSubmitRpcBatch {.gcsafe, raises: [].} =
  if session.isNil:
    return
  let firstRequest = session.popSubmitRequest()
  if firstRequest.isNil:
    return
  result = session.buildSubmitRpcBatch(firstRequest)
  if result.requests.len == 0 or result.items.len == 0 or result.encoded.len == 0:
    result.failSubmitBatch()
    result = FabricSubmitRpcBatch()

proc decodeSubmitInboxItem(
    network: FabricNetwork, envelope: FabricSubmitEnvelope, sourcePeerId: string
): Option[FabricSubmitInboxItem] {.gcsafe, raises: [].} =
  let itemKey = envelope.effectiveSubmitItemKey()
  case envelope.kind
  of fskWarm:
    none(FabricSubmitInboxItem)
  of fskEvent:
    if network.isNil or network.eventHandler.isNil:
      return none(FabricSubmitInboxItem)
    some(FabricSubmitInboxItem(
      itemKey: itemKey,
      sourcePeerId: sourcePeerId,
      envelope: envelope,
    ))
  of fskAttestation:
    if network.isNil or network.attestationHandler.isNil:
      return none(FabricSubmitInboxItem)
    some(FabricSubmitInboxItem(
      itemKey: itemKey,
      sourcePeerId: sourcePeerId,
      envelope: envelope,
    ))
  of fskEventCertificate:
    if network.isNil or network.eventCertificateHandler.isNil:
      return none(FabricSubmitInboxItem)
    some(FabricSubmitInboxItem(
      itemKey: itemKey,
      sourcePeerId: sourcePeerId,
      envelope: envelope,
    ))
  of fskCheckpointCandidate:
    if network.isNil or network.checkpointCandidateHandler.isNil:
      return none(FabricSubmitInboxItem)
    some(FabricSubmitInboxItem(
      itemKey: itemKey,
      sourcePeerId: sourcePeerId,
      envelope: envelope,
    ))
  of fskCheckpointVote:
    if network.isNil or network.checkpointVoteHandler.isNil:
      return none(FabricSubmitInboxItem)
    some(FabricSubmitInboxItem(
      itemKey: itemKey,
      sourcePeerId: sourcePeerId,
      envelope: envelope,
    ))
  of fskCheckpointBundle:
    if network.isNil or network.checkpointBundleHandler.isNil:
      return none(FabricSubmitInboxItem)
    some(FabricSubmitInboxItem(
      itemKey: itemKey,
      sourcePeerId: sourcePeerId,
      envelope: envelope,
    ))
  of fskAvoProposal:
    if network.isNil or network.avoProposalHandler.isNil:
      return none(FabricSubmitInboxItem)
    some(FabricSubmitInboxItem(
      itemKey: itemKey,
      sourcePeerId: sourcePeerId,
      envelope: envelope,
    ))
  of fskAvoApproval:
    if network.isNil or network.avoApprovalHandler.isNil:
      return none(FabricSubmitInboxItem)
    some(FabricSubmitInboxItem(
      itemKey: itemKey,
      sourcePeerId: sourcePeerId,
      envelope: envelope,
    ))

proc submitInboxLen(network: FabricNetwork): int {.gcsafe, raises: [].} =
  if network.isNil:
    return 0
  network.submitHotInbox.len + network.submitInbox.len

proc submitHotAcceptEligible(
    network: FabricNetwork, item: FabricSubmitInboxItem
): bool {.gcsafe, raises: [].} =
  if network.isNil or item.sourcePeerId.len == 0:
    return false
  case item.envelope.kind
  of fskEvent:
    network.submitPeerReadyNow(item.sourcePeerId, fslEvent)
  of fskAttestation:
    network.submitPeerReadyNow(item.sourcePeerId, fslAttestation)
  else:
    false

proc planSubmitAcceptance(
    network: FabricNetwork, envelope: FabricSubmitEnvelope, sourcePeerId: string
): FabricSubmitAcceptPlan {.gcsafe, raises: [].} =
  result.itemKey = envelope.effectiveSubmitItemKey()
  if envelope.kind == fskWarm:
    result.disposition = fsadAcknowledgeOnly
    return
  let item = network.decodeSubmitInboxItem(envelope, sourcePeerId)
  if item.isNone():
    result.disposition = fsadReject
    return
  result.item = item.get()
  if network.submitHotAcceptEligible(result.item):
    result.disposition = fsadHotQueue
  else:
    result.disposition = fsadInboxQueue

proc enqueueSubmitInboxItem(
    network: FabricNetwork, item: FabricSubmitInboxItem, hot: bool
): bool {.gcsafe, raises: [].} =
  if network.isNil:
    return false
  let queued = network.submitInboxLen()
  if queued >= FabricSubmitInboxCapacity:
    when defined(fabric_submit_diag):
      echo "fabric-submit inbox-full self=",
        (if network.switch.isNil: "" else: peerIdString(network.switch.peerInfo.peerId)),
        " item=", item.itemKey,
        " queued=", queued
    return false
  if hot:
    network.submitHotInbox.addLast(item)
  else:
    network.submitInbox.addLast(item)
  if network.submitAcceptDepth > 0:
    network.submitInboxPending = true
  else:
    network.scheduleSubmitInboxDrain()
  true

proc acceptSubmitEnvelope(
    network: FabricNetwork, envelope: FabricSubmitEnvelope, sourcePeerId: string
): Future[FabricSubmitAckItem] {.async: (raises: [CancelledError]).} =
  let plan = network.planSubmitAcceptance(envelope, sourcePeerId)
  case plan.disposition
  of fsadReject:
    return FabricSubmitAckItem(itemKey: plan.itemKey, accepted: false)
  of fsadAcknowledgeOnly:
    return FabricSubmitAckItem(itemKey: plan.itemKey, accepted: true)
  of fsadHotQueue:
    return FabricSubmitAckItem(
      itemKey: plan.itemKey,
      accepted: network.enqueueSubmitInboxItem(plan.item, hot = true),
    )
  of fsadInboxQueue:
    return FabricSubmitAckItem(
      itemKey: plan.itemKey,
      accepted: network.enqueueSubmitInboxItem(plan.item, hot = false),
    )

proc acceptSubmitFrame(
    network: FabricNetwork, frame: FabricSubmitFrame, sourcePeerId: string
): Future[FabricSubmitAckFrame] {.async: (raises: [CancelledError]).} =
  if frame.items.len == 0:
    return emptySubmitAckFrame(frame.requestId)
  if not network.isNil and sourcePeerId.len > 0:
      let sourcePeer = PeerId.init(sourcePeerId).valueOr:
        PeerId()
      if sourcePeer.data.len > 0:
        network.setSubmitPeerTransportVerified(
          sourcePeer, true, "recv-submit-frame"
        )
  for envelope in frame.items:
    result.items.add(await network.acceptSubmitEnvelope(envelope, sourcePeerId))
  result.requestId = frame.requestId

proc processSubmitInboxItem(
    network: FabricNetwork, item: FabricSubmitInboxItem
): Future[void] {.async: (raises: [CancelledError]).} =
  try:
    case item.envelope.kind
    of fskWarm:
      return
    of fskEvent:
      let decoded = safeDecode[FabricEvent](item.envelope.payload)
      if decoded.isNone():
        return
      when defined(fabric_lsmr_diag):
        echo "fabric-submit recv-event self=", peerIdString(network.switch.peerInfo.peerId),
          " event=", decoded.get().eventId
      await network.eventHandler(
        network,
        decoded.get(),
        item.envelope.scopePrefix,
        item.sourcePeerId,
      )
      when defined(fabric_lsmr_diag):
        echo "fabric-submit recv-event-done self=", peerIdString(network.switch.peerInfo.peerId),
          " event=", decoded.get().eventId
    of fskAttestation:
      let decoded = safeDecode[EventAttestation](item.envelope.payload)
      if decoded.isNone():
        return
      when defined(fabric_lsmr_diag):
        echo "fabric-submit recv-att self=", peerIdString(network.switch.peerInfo.peerId),
          " event=", decoded.get().eventId,
          " role=", ord(decoded.get().role),
          " signer=", decoded.get().signer
      await network.attestationHandler(
        network,
        decoded.get(),
        item.envelope.scopePrefix,
        item.sourcePeerId,
      )
      when defined(fabric_lsmr_diag):
        echo "fabric-submit recv-att-done self=", peerIdString(network.switch.peerInfo.peerId),
          " event=", decoded.get().eventId,
          " role=", ord(decoded.get().role),
          " signer=", decoded.get().signer
    of fskEventCertificate:
      let decoded = safeDecode[EventCertificate](item.envelope.payload)
      if decoded.isNone():
        return
      when defined(fabric_lsmr_diag):
        echo "fabric-submit recv-cert self=", peerIdString(network.switch.peerInfo.peerId),
          " event=", decoded.get().eventId
      await network.eventCertificateHandler(
        network,
        decoded.get(),
        item.envelope.scopePrefix,
        item.sourcePeerId,
      )
      when defined(fabric_lsmr_diag):
        echo "fabric-submit recv-cert-done self=", peerIdString(network.switch.peerInfo.peerId),
          " event=", decoded.get().eventId
    of fskCheckpointCandidate:
      let decoded = safeDecode[CheckpointCandidate](item.envelope.payload)
      if decoded.isNone():
        return
      await network.checkpointCandidateHandler(network, decoded.get(), item.sourcePeerId)
    of fskCheckpointVote:
      let decoded = safeDecode[CheckpointVote](item.envelope.payload)
      if decoded.isNone():
        return
      await network.checkpointVoteHandler(network, decoded.get(), item.sourcePeerId)
    of fskCheckpointBundle:
      let decoded = safeDecode[CheckpointBundle](item.envelope.payload)
      if decoded.isNone():
        return
      await network.checkpointBundleHandler(network, decoded.get(), item.sourcePeerId)
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
  let sliceStartedAtMs = diagNowMs()
  var processed = 0
  var hotResumeNeeded = false
  try:
    while network.submitInboxLen() > 0 and processed < FabricSubmitInboxBatchLimit:
      let acceptEpoch = network.submitAcceptEpoch
      if network.submitAcceptGateActive():
        network.submitInboxPending = true
        break
      if processed > 0 and diagNowMs() - sliceStartedAtMs >= FabricSubmitInboxSliceBudgetMs:
        when defined(fabric_submit_diag) or defined(fabric_lsmr_diag):
          echo "fabric-submit inbox-yield self=",
            (if network.switch.isNil: "" else: peerIdString(network.switch.peerInfo.peerId)),
            " processed=", processed,
            " queued=", network.submitInboxLen()
        break
      let fromHot = network.submitHotInbox.len > 0
      let item =
        if fromHot:
          network.submitHotInbox.popFirst()
        else:
          network.submitInbox.popFirst()
      if network.submitAcceptEpoch != acceptEpoch or network.submitAcceptGateActive():
        if fromHot:
          network.submitHotInbox.addFirst(item)
        else:
          network.submitInbox.addFirst(item)
        network.submitInboxPending = true
        break
      let hotBatchBoundary = fromHot and network.submitHotInbox.len == 0
      if fromHot:
        network.submitHotDrainActive = true
      inc processed
      try:
        let itemStartedAtMs = diagNowMs()
        when defined(fabric_lsmr_diag):
          echo "t=", itemStartedAtMs,
            " fabric-submit inbox-item-begin self=",
            (if network.switch.isNil: "" else: peerIdString(network.switch.peerInfo.peerId)),
            " kind=", submitKindText(item.envelope.kind),
            " item=", item.itemKey,
            " source=", item.sourcePeerId,
            " queued=", network.submitInboxLen()
        await network.processSubmitInboxItem(item)
        when defined(fabric_lsmr_diag):
          echo "t=", diagNowMs(),
            " fabric-submit inbox-item-done self=",
            (if network.switch.isNil: "" else: peerIdString(network.switch.peerInfo.peerId)),
            " kind=", submitKindText(item.envelope.kind),
            " item=", item.itemKey,
            " source=", item.sourcePeerId,
            " elapsedMs=", diagNowMs() - itemStartedAtMs,
            " queued=", network.submitInboxLen()
        await sleepAsync(FabricSubmitInboxYield)
      except CancelledError:
        return
      except CatchableError as exc:
        when defined(fabric_submit_diag):
          echo "fabric-submit inbox-fail self=",
            (if network.switch.isNil: "" else: peerIdString(network.switch.peerInfo.peerId)),
            " item=", item.itemKey,
            " err=", exc.msg
      if hotBatchBoundary:
        hotResumeNeeded = true
        break
  finally:
    if not network.isNil:
      network.submitHotDrainActive = false
      if hotResumeNeeded:
        network.scheduleSubmitHotResume()
      else:
        network.signalSubmitHotAcceptIdle()

proc scheduleSubmitInboxDrain(network: FabricNetwork) {.gcsafe, raises: [].} =
  if network.isNil:
    return
  if network.submitAcceptGateActive():
    network.submitInboxPending = true
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
        let rerun = network.submitInboxPending or network.submitInboxLen() > 0
        network.submitInboxPending = false
        network.submitInboxRunner = nil
        if rerun:
          network.scheduleSubmitInboxDrain()

  network.submitInboxRunner = drain()
  asyncSpawn network.submitInboxRunner

proc scheduleSubmitHotResume(network: FabricNetwork) {.gcsafe, raises: [].} =
  if network.isNil:
    return
  network.submitHotResumePending = true

proc resumeSubmitHotIfPending(network: FabricNetwork) {.gcsafe, raises: [].} =
  if network.isNil or not network.submitHotResumePending:
    return
  network.submitHotResumePending = false
  if network.submitInboxLen() == 0:
    network.signalSubmitHotAcceptIdle()
    return
  network.scheduleSubmitInboxDrain()

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
        await networkRef.value.eventHandler(networkRef.value, item.get(), @[], "")
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
        await networkRef.value.attestationHandler(networkRef.value, item.get(), @[], "")
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
        await networkRef.value.eventCertificateHandler(networkRef.value, item.get(), @[], "")
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
        await networkRef.value.checkpointCandidateHandler(networkRef.value, item.get(), "")
        return FetchResponse(status: fsOk, data: bytesOf(SubmitAck))
      return FetchResponse(status: fsNotFound, data: @[])
    if key.startsWith(SubmitCheckpointVotePrefix):
      let item = safeDecodeSubmitted[CheckpointVote](key, SubmitCheckpointVotePrefix)
      if item.isNone():
        return FetchResponse(status: fsError, data: @[])
      if networkRef.value != nil and not networkRef.value.checkpointVoteHandler.isNil:
        await networkRef.value.checkpointVoteHandler(networkRef.value, item.get(), "")
        return FetchResponse(status: fsOk, data: bytesOf(SubmitAck))
      return FetchResponse(status: fsNotFound, data: @[])
    if key.startsWith(SubmitCheckpointBundlePrefix):
      let item = safeDecodeSubmitted[CheckpointBundle](key, SubmitCheckpointBundlePrefix)
      if item.isNone():
        return FetchResponse(status: fsError, data: @[])
      if networkRef.value != nil and not networkRef.value.checkpointBundleHandler.isNil:
        await networkRef.value.checkpointBundleHandler(networkRef.value, item.get(), "")
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
    let network = networkRef.value
    if network.controlFetchServerMustYieldToLive(key):
      when defined(fabric_lsmr_diag):
        echo "fabric-net control-yield self=",
          (if network.isNil or network.switch.isNil: "" else: peerIdString(network.switch.peerInfo.peerId)),
          " requester=", splitControlFetchRequesterKey(key).requesterPeerId,
          " key=", key
      return FetchResponse(status: fsError, data: @[])
    let responseLock =
      if network.isNil:
        nil
      else:
        network.getOrCreateControlFetchResponseLock(key)
    let flightKey = controlFetchResponseLockKey(key)
    if responseLock.isNil or flightKey.len == 0:
      let payload = lookup(key)
      if payload.isNone():
        return FetchResponse(status: fsNotFound, data: @[])
      return FetchResponse(status: fsOk, data: payload.get())

    var responseFlight: Future[FetchResponse]
    var owner = false
    await responseLock.acquire()
    try:
      responseFlight = network.controlFetchResponseFlights.getOrDefault(flightKey)
      if responseFlight.isNil or responseFlight.finished():
        responseFlight = newFuture[FetchResponse]()
        network.controlFetchResponseFlights[flightKey] = responseFlight
        owner = true
    finally:
      try:
        if responseLock.locked():
          responseLock.release()
      except AsyncLockError:
        discard

    if not owner:
      return await responseFlight

    let response =
      try:
        let payload = lookup(key)
        if payload.isNone():
          FetchResponse(status: fsNotFound, data: @[])
        else:
          FetchResponse(status: fsOk, data: payload.get())
      except CatchableError:
        FetchResponse(status: fsError, data: @[])

    responseFlight.complete(response)

    await responseLock.acquire()
    try:
      if network.controlFetchResponseFlights.getOrDefault(flightKey) == responseFlight:
        network.controlFetchResponseFlights.del(flightKey)
    finally:
      try:
        if responseLock.locked():
          responseLock.release()
      except AsyncLockError:
        discard

    response

proc buildSubmitProtocol(network: FabricNetwork): LPProtocol =
  proc handle(conn: Connection, proto: string) {.async: (raises: [CancelledError]).} =
    try:
      let sourcePeerId = peerIdString(conn.peerId)
      let sourcePeer = PeerId.init(sourcePeerId).valueOr:
        PeerId()
      if sourcePeer.data.len > 0:
        network.setSubmitPeerTransportVerified(
          sourcePeer, true, "recv-submit-stream"
        )
      while true:
        if not network.isNil:
          network.resumeSubmitHotIfPending()
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
          return
        let raw = stringOf(payload)
        let processStartedAtMs = diagNowMs()
        let frame = safeDecode[FabricSubmitFrame](raw)
        let envelope =
          if frame.isSome() and frame.get().items.len > 0:
            none(FabricSubmitEnvelope)
          else:
            safeDecode[FabricSubmitEnvelope](raw)
        var ackFrame = emptySubmitAckFrame()
        var submitAcceptHeld = false
        try:
          if not network.isNil:
            inc network.submitAcceptEpoch
            inc network.submitAcceptDepth
            submitAcceptHeld = true
          ackFrame =
            if frame.isSome() and frame.get().items.len > 0:
              await network.acceptSubmitFrame(frame.get(), sourcePeerId)
            elif envelope.isSome():
              FabricSubmitAckFrame(
                requestId: 0,
                items: @[await network.acceptSubmitEnvelope(envelope.get(), sourcePeerId)],
              )
            else:
              emptySubmitAckFrame()
          let processElapsedMs = diagNowMs() - processStartedAtMs
          when defined(fabric_submit_diag) or defined(fabric_lsmr_diag):
            if processElapsedMs >= 100:
              echo "fabric-submit handler-slow-process self=",
                (if network.isNil or network.switch.isNil: "" else: peerIdString(network.switch.peerInfo.peerId)),
                " codec=", proto,
                " bytes=", payload.len,
                " items=", (if frame.isSome() and frame.get().items.len > 0: frame.get().items.len else: 1),
                " accepted=", ackFrame.items.countIt(it.accepted),
                "/",
                ackFrame.items.len,
                " elapsedMs=", processElapsedMs
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
          let ackWriteStartedAtMs = diagNowMs()
          when defined(fabric_lsmr_diag):
            echo "t=", ackWriteStartedAtMs,
              " fabric-submit handler-ack-begin self=",
              (if network.isNil or network.switch.isNil: "" else: peerIdString(network.switch.peerInfo.peerId)),
              " codec=", proto,
              " accepted=", ackFrame.items.countIt(it.accepted),
              "/",
              ackFrame.items.len
          let ackPayload = safeEncode(ackFrame)
          if ackPayload.isSome():
            await conn.writeLp(bytesOf(ackPayload.get()))
          else:
            await conn.writeLp(bytesOf(""))
          if not network.isNil:
            if submitAcceptHeld and network.submitAcceptDepth > 0:
              dec network.submitAcceptDepth
              submitAcceptHeld = false
            inc network.submitHotAckYieldActive
          when defined(fabric_lsmr_diag):
            echo "t=", diagNowMs(),
              " fabric-submit handler-ack-done self=",
              (if network.isNil or network.switch.isNil: "" else: peerIdString(network.switch.peerInfo.peerId)),
              " codec=", proto,
              " accepted=", ackFrame.items.countIt(it.accepted),
              "/",
              ackFrame.items.len,
              " elapsedMs=", diagNowMs() - ackWriteStartedAtMs
          if not network.isNil:
            if network.submitHotAckYieldActive > 0:
              dec network.submitHotAckYieldActive
            network.scheduleSubmitHotResume()
        finally:
          if submitAcceptHeld and not network.isNil and network.submitAcceptDepth > 0:
            dec network.submitAcceptDepth
    except CancelledError as exc:
      raise exc
    except LPStreamEOFError:
      discard
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
    network: FabricNetwork,
    peerId: PeerId,
    sessionClass: FabricSubmitSessionClass,
    shardIdx = 0,
): FabricSubmitSession {.gcsafe, raises: [].} =
  if network.isNil or peerId.data.len == 0:
    return nil
  let boundedShard =
    if shardIdx >= 0 and shardIdx < submitSessionShardCount(sessionClass):
      shardIdx
    else:
      0
  let key = submitSessionKey(peerId, sessionClass, boundedShard)
  let existing = network.submitSessions.getOrDefault(key)
  if not existing.isNil:
    return existing
  result = FabricSubmitSession(
    peerId: peerId,
    transportClass: sessionClass,
    streamSlot: boundedShard,
    nextRequestId: 1,
    lock: newAsyncLock(),
    openLock: newAsyncLock(),
    queue: initDeque[FabricSubmitRequest](),
    inflightCount: 0,
    dispatchPending: false,
    dispatchEvent: newAsyncEvent(),
    dispatchRunner: nil,
  )
  network.submitSessions[key] = result

proc getOrCreateSubmitSession(
    network: FabricNetwork, peerId: PeerId, lane: FabricSubmitLane
): FabricSubmitSession {.gcsafe, raises: [].} =
  network.getOrCreateSubmitSession(peerId, submitSessionClassForLane(lane), 0)

proc getOrCreateControlFetchSession(
    network: FabricNetwork,
    peerId: PeerId,
    sessionClass: FabricControlFetchSessionClass,
): FabricControlFetchSession {.gcsafe, raises: [].} =
  if network.isNil or peerId.data.len == 0:
    return nil
  let key = controlFetchSessionKey(peerId, sessionClass)
  let existing = network.controlFetchSessions.getOrDefault(key)
  if not existing.isNil:
    return existing
  result = FabricControlFetchSession(
    peerId: peerId,
    sessionClass: sessionClass,
    lock: newAsyncLock(),
    conn: nil,
  )
  network.controlFetchSessions[key] = result

proc getOrCreateSubmitPeerConnect(
    network: FabricNetwork, peerId: PeerId
): FabricSubmitPeerConnect {.gcsafe, raises: [].} =
  if network.isNil or peerId.data.len == 0:
    return nil
  let key = $peerId
  let existing = network.submitPeerConnects.getOrDefault(key)
  if not existing.isNil:
    return existing
  result = FabricSubmitPeerConnect(
    lock: newAsyncLock(),
    rpcLock: newAsyncLock(),
    streamLock: newAsyncLock(),
    streams: default(array[FabricSubmitSessionClass, FabricSubmitPeerStreamPool]),
    liveGeneration: 0,
    controlGeneration: 0,
    runner: nil,
    probeRunner: nil,
    connectivityRunner: nil,
    actorConnConnected: false,
    actorConnDisconnected: false,
    actorConnIncoming: false,
    actorWantWarm: false,
    actorWantRepair: false,
    actorWantedSessionShards: default(array[FabricSubmitSessionClass, uint32]),
    actorRunner: nil,
    runnerForceRepair: false,
    pendingForceRepair: false,
    suppressedDisconnects: 0,
    preferForceRepair: false,
    transportBlocked: false,
    transportVerified: false,
    connectedBaseOwnerKnown: false,
    connectedBaseOwnerId: "",
    localOwnsConnectedBase: false,
  )
  network.submitPeerConnects[key] = result

proc setSubmitPeerForceRepairHint(
    network: FabricNetwork,
    peerId: PeerId,
    enabled: bool,
    reason: string,
) {.gcsafe, raises: [].} =
  if network.isNil or network.switch.isNil or peerId.data.len == 0:
    return
  let state = network.getOrCreateSubmitPeerConnect(peerId)
  if state.isNil or state.preferForceRepair == enabled:
    return
  state.preferForceRepair = enabled
  when defined(fabric_submit_diag) or defined(fabric_lsmr_diag):
    echo "fabric-submit repair-hint self=",
      peerIdString(network.switch.peerInfo.peerId),
      " peer=", peerIdString(peerId),
      " enabled=", enabled,
      " reason=", reason

proc setSubmitPeerTransportBlocked(
    network: FabricNetwork,
    peerId: PeerId,
    blocked: bool,
    reason: string,
) {.gcsafe, raises: [].} =
  if network.isNil or network.switch.isNil or peerId.data.len == 0:
    return
  let state = network.getOrCreateSubmitPeerConnect(peerId)
  if state.isNil or state.transportBlocked == blocked:
    if blocked and not state.isNil:
      state.transportVerified = false
    return
  state.transportBlocked = blocked
  if blocked:
    state.transportVerified = false
  when defined(fabric_submit_diag) or defined(fabric_lsmr_diag):
    echo "fabric-submit transport-blocked self=",
      peerIdString(network.switch.peerInfo.peerId),
      " peer=", peerIdString(peerId),
      " blocked=", blocked,
      " reason=", reason

proc setSubmitPeerTransportVerified(
    network: FabricNetwork,
    peerId: PeerId,
    verified: bool,
    reason: string,
) {.gcsafe, raises: [].} =
  if network.isNil or network.switch.isNil or peerId.data.len == 0:
    return
  let state = network.getOrCreateSubmitPeerConnect(peerId)
  if state.isNil:
    return
  if verified:
    state.transportBlocked = false
    state.preferForceRepair = false
  if state.transportVerified == verified:
    return
  state.transportVerified = verified
  when defined(fabric_submit_diag) or defined(fabric_lsmr_diag):
    echo "fabric-submit transport-verified self=",
      peerIdString(network.switch.peerInfo.peerId),
      " peer=", peerIdString(peerId),
      " verified=", verified,
      " reason=", reason

proc clearSubmitPeerRepairWait(
    network: FabricNetwork,
    peerId: PeerId,
    reason: string,
    preserveVerified = false,
): Future[void] {.async: (raises: [CancelledError]).} =
  if network.isNil or network.switch.isNil or peerId.data.len == 0:
    return
  let state = network.getOrCreateSubmitPeerConnect(peerId)
  if state.isNil or state.lock.isNil:
    return
  var clearedPending = false
  var clearedSuppressed = 0
  var clearedPrefer = false
  await state.lock.acquire()
  try:
    if state.pendingForceRepair:
      state.pendingForceRepair = false
      clearedPending = true
    if state.suppressedDisconnects > 0:
      clearedSuppressed = state.suppressedDisconnects
      state.suppressedDisconnects = 0
    if state.suppressedDisconnects == 0 and
        (state.runner.isNil or state.runner.finished()):
      state.runnerForceRepair = false
    if state.transportBlocked:
      state.transportBlocked = false
    if not preserveVerified and state.transportVerified:
      state.transportVerified = false
    if state.preferForceRepair:
      state.preferForceRepair = false
      clearedPrefer = true
  finally:
    try:
      if state.lock.locked():
        state.lock.release()
    except AsyncLockError:
      discard
  if clearedPending or clearedSuppressed > 0 or clearedPrefer:
    when defined(fabric_submit_diag) or defined(fabric_lsmr_diag):
      echo "fabric-submit repair-wait-clear self=",
        peerIdString(network.switch.peerInfo.peerId),
        " peer=", peerIdString(peerId),
        " reason=", reason,
        " pending=", clearedPending,
        " suppressed=", clearedSuppressed,
        " prefer=", clearedPrefer

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

proc submitAddrTexts(addrs: openArray[MultiAddress]): string {.gcsafe, raises: [].} =
  if addrs.len == 0:
    return ""
  var texts: seq[string] = @[]
  for addr in addrs:
    texts.add($addr)
  texts.join(",")

proc submitPeerStoreAddrs(
    network: FabricNetwork, peerId: PeerId
): seq[MultiAddress] {.gcsafe, raises: [].} =
  if network.isNil or network.switch.isNil or network.switch.peerStore.isNil or
      peerId.data.len == 0:
    return @[]
  result.mergeUniqueAddrs(network.switch.peerStore.getAddresses(peerId))

proc submitPeerHintAddrs(
    network: FabricNetwork, peerId: PeerId
): seq[MultiAddress] {.gcsafe, raises: [].} =
  if network.isNil or peerId.data.len == 0:
    return @[]
  result.mergeUniqueAddrs(network.submitPeerAddrHints.getOrDefault($peerId))

proc submitPeerAddrs(
    network: FabricNetwork, peerId: PeerId
): seq[MultiAddress] {.gcsafe, raises: [].} =
  if network.isNil or peerId.data.len == 0:
    return @[]
  result = network.submitPeerStoreAddrs(peerId)
  result.mergeUniqueAddrs(network.submitPeerHintAddrs(peerId))

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
    when defined(fabric_submit_diag) or defined(fabric_lsmr_diag):
      echo "fabric-submit remember-addrs self=",
        (if network.switch.isNil: "" else: peerIdString(network.switch.peerInfo.peerId)),
        " peer=", peerIdString(peerId),
        " incoming=", addrs.len,
        " merged=", merged.len,
        " incomingList=", submitAddrTexts(addrs),
        " mergedList=", submitAddrTexts(merged)

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
  result = network.submitPeerStoreAddrs(peerId)
  result.mergeUniqueAddrs(network.submitPeerHintAddrs(peerId))

proc submitSessionReady(session: FabricSubmitSession): bool {.gcsafe, raises: [].} =
  not session.isNil

proc submitTransportLane(
    network: FabricNetwork, peerId: PeerId, lane: FabricSubmitLane
): FabricSubmitLane {.gcsafe, raises: [].} =
  discard network
  discard peerId
  lane

proc submitTransportDispatchable(
    network: FabricNetwork, peerId: PeerId, lane: FabricSubmitLane
): bool {.gcsafe, raises: [].} =
  if network.isNil or network.switch.isNil or peerId.data.len == 0:
    return false
  if not network.switch.isConnected(peerId):
    return false
  let state = network.submitPeerConnects.getOrDefault($peerId)
  if state.isNil or state.transportBlocked or network.switch.connManager.isNil:
    return false
  case lane
  of fslEvent:
    state.submitPeerStreamReady(fsscEvent) and
      network.switch.connManager.planeReady(peerId, ppkLive)
  of fslAttestation:
    state.submitPeerStreamReady(fsscAttestation) and
      network.switch.connManager.planeReady(peerId, ppkLive)
  of fslOther:
    state.transportVerified and
      network.switch.connManager.planeReady(peerId, ppkControl) and
      not network.switch.connManager.planeSharedWithLive(peerId, ppkControl)

proc submitTransportReady(
    network: FabricNetwork, peerId: PeerId, lane: FabricSubmitLane
): bool {.gcsafe, raises: [].} =
  network.submitTransportDispatchable(peerId, lane)

proc submitPeerNeedsWarm(
    network: FabricNetwork, peerId: PeerId
): bool {.gcsafe, raises: [].} =
  if network.isNil or network.switch.isNil or peerId.data.len == 0:
    return false
  not network.switch.isConnected(peerId)

proc hasDesiredUnverifiedSubmitPeer(
    network: FabricNetwork
): bool {.gcsafe, raises: [].} =
  if network.isNil or network.switch.isNil:
    return false
  for peerId in network.desiredSubmitPeers():
    if network.submitPeerNeedsWarm(peerId):
      return true
  false

proc submitPeerOwnsRepair(
    network: FabricNetwork, peerId: PeerId
): bool {.gcsafe, raises: [].} =
  if network.isNil or network.switch.isNil or peerId.data.len == 0:
    return false
  let liveOwnerId = network.submitPeerLiveBaseOwnerId(peerId)
  let liveOwner = network.submitPeerLiveBaseOwnerState(peerId)
  let state = network.submitPeerConnects.getOrDefault($peerId)
  if liveOwner.known:
    if not state.isNil:
      state.connectedBaseOwnerKnown = true
      state.connectedBaseOwnerId = liveOwnerId
      state.localOwnsConnectedBase = liveOwner.localOwns
    return liveOwner.localOwns
  if not state.isNil and state.connectedBaseOwnerKnown and state.connectedBaseOwnerId.len > 0:
    return state.localOwnsConnectedBase
  peerIdString(network.switch.peerInfo.peerId) <= peerIdString(peerId)

proc waitSubmitPeerDisconnectSettled(
    network: FabricNetwork,
    peerId: PeerId,
): Future[bool] {.async: (raises: [CancelledError]).} =
  if network.isNil or network.switch.isNil or peerId.data.len == 0 or network.stopping:
    return false
  let startedAtMs = diagNowMs()
  while true:
    let connCount =
      if network.switch.connManager.isNil: 0
      else: network.switch.connManager.connCount(peerId)
    let connected = network.switch.isConnected(peerId)
    if connCount == 0:
      when defined(fabric_submit_diag) or defined(fabric_lsmr_diag):
        echo "fabric-submit base-disconnect-settle-drained self=",
          peerIdString(network.switch.peerInfo.peerId),
          " peer=", peerIdString(peerId),
          " elapsedMs=", diagNowMs() - startedAtMs
      return false
    if connected:
      when defined(fabric_submit_diag) or defined(fabric_lsmr_diag):
        echo "fabric-submit base-disconnect-settle-reconnected self=",
          peerIdString(network.switch.peerInfo.peerId),
          " peer=", peerIdString(peerId),
          " connCount=", connCount,
          " elapsedMs=", diagNowMs() - startedAtMs
      return true
    if diagNowMs() - startedAtMs >= FabricSubmitDisconnectSettleTimeout.milliseconds:
      when defined(fabric_submit_diag) or defined(fabric_lsmr_diag):
        echo "fabric-submit base-disconnect-settle-timeout self=",
          peerIdString(network.switch.peerInfo.peerId),
          " peer=", peerIdString(peerId),
          " connCount=", connCount,
          " elapsedMs=", diagNowMs() - startedAtMs
      return connCount > 0
    await sleepAsync(FabricSubmitDisconnectSettlePoll)

proc submitPeerReadyNow*(
    network: FabricNetwork, peerId: PeerId, lane: FabricSubmitLane
): bool {.gcsafe, raises: [].} =
  network.submitTransportDispatchable(peerId, lane)

proc submitPeerReadyNow*(
    network: FabricNetwork, peerIdText: string, lane: FabricSubmitLane
): bool {.gcsafe, raises: [].} =
  if network.isNil or peerIdText.len == 0:
    return false
  let peerId = PeerId.init(peerIdText).valueOr:
    return false
  network.submitPeerReadyNow(peerId, lane)

proc submitPeerReadyNow*(
    network: FabricNetwork, peerId: PeerId
): bool {.gcsafe, raises: [].} =
  if network.isNil or peerId.data.len == 0:
    return false
  network.submitTransportDispatchable(peerId, fslEvent)

proc submitPeerReadyNow*(
    network: FabricNetwork, peerIdText: string
): bool {.gcsafe, raises: [].} =
  if network.isNil or peerIdText.len == 0:
    return false
  let peerId = PeerId.init(peerIdText).valueOr:
    return false
  network.submitPeerReadyNow(peerId)

proc submitPeerDiag*(
    network: FabricNetwork, peerIdText: string
): string {.gcsafe, raises: [].} =
  if network.isNil or peerIdText.len == 0:
    return "network=nil"
  let peerId = PeerId.init(peerIdText).valueOr:
    return "peer=invalid"
  let state = network.submitPeerConnects.getOrDefault($peerId)
  var laneStates: seq[string] = @[]
  for lane in [fslEvent, fslAttestation, fslOther]:
    let queued = network.submitPeerLanePendingLen(peerId, lane)
    laneStates.add(
      submitLaneText(lane) &
      ":queued=" & $queued
    )
  "peer=" & peerIdText &
    " connected=" & $network.switch.isConnected(peerId) &
    " repairing=" & $(not state.isNil and
      ((not state.runner.isNil and not state.runner.finished() and state.runnerForceRepair) or
      state.pendingForceRepair or state.suppressedDisconnects > 0 or
      state.preferForceRepair)) &
    " blocked=" & $(not state.isNil and state.transportBlocked) &
    " verified=" & $(not state.isNil and state.transportVerified) &
    " hintAddrs=" & $network.submitPeerAddrHints.getOrDefault($peerId).len &
    " inflight=" & $network.submitPeerInflightCount(peerId) &
    " dispatch=" & $network.submitPeerSessions(peerId).anyIt(
      not it.dispatchRunner.isNil and not it.dispatchRunner.finished()
    ) &
    " lanes=" & laneStates.join(",")

proc probeSubmitTransport(
    network: FabricNetwork, peerId: PeerId
): Future[bool] {.async: (raises: [CancelledError]).}
proc ensureSubmitPeerConnected*(
    network: FabricNetwork,
    peerId: PeerId,
    forceRepair = false,
    forceLocalRepair = false,
): Future[bool] {.async: (raises: [CancelledError]).}

proc includeSubmitSessionClasses(
    target: var array[FabricSubmitSessionClass, uint32], lane: FabricSubmitLane
) {.gcsafe, raises: [].} =
  case lane
  of fslEvent:
    target[fsscEvent] = target[fsscEvent] or submitSessionAllShardBits(fsscEvent)
  of fslAttestation:
    target[fsscAttestation] = target[fsscAttestation] or submitSessionAllShardBits(fsscAttestation)
  of fslOther:
    target[fsscEventCertificate] =
      target[fsscEventCertificate] or submitSessionAllShardBits(fsscEventCertificate)
    target[fsscControl] = target[fsscControl] or submitSessionAllShardBits(fsscControl)

proc runSubmitPeerActor(
    network: FabricNetwork, peerId: PeerId
): Future[void] {.async: (raises: [CancelledError]).} =
  if network.isNil or network.switch.isNil or peerId.data.len == 0:
    return
  let state = network.getOrCreateSubmitPeerConnect(peerId)
  if state.isNil or state.lock.isNil:
    return
  let peerIdText = peerIdString(peerId)
  while not network.isNil:
    var wantedSessionShards: array[FabricSubmitSessionClass, uint32]
    var wantWarm = false
    var wantRepair = false
    var sawConnected = false
    var sawDisconnected = false
    var connectedIncoming = false
    await state.lock.acquire()
    try:
      wantedSessionShards = state.actorWantedSessionShards
      wantWarm = state.actorWantWarm
      wantRepair = state.actorWantRepair
      sawConnected = state.actorConnConnected
      sawDisconnected = state.actorConnDisconnected
      connectedIncoming = state.actorConnIncoming
      state.actorWantedSessionShards = default(array[FabricSubmitSessionClass, uint32])
      state.actorWantWarm = false
      state.actorWantRepair = false
      state.actorConnConnected = false
      state.actorConnDisconnected = false
      state.actorConnIncoming = false
      if not submitSessionShardsQueued(wantedSessionShards) and
          not wantWarm and not wantRepair and
          not sawConnected and not sawDisconnected:
        state.actorRunner = nil
        return
    finally:
      try:
        if state.lock.locked():
          state.lock.release()
      except AsyncLockError:
        discard

    if sawDisconnected:
      let livePlane = network.switch.connManager.planeState(peerId, ppkLive)
      let controlPlane = network.switch.connManager.planeState(peerId, ppkControl)
      let liveOwnerId = livePlane.ownerMuxerId
      let liveOwner = (
        known: livePlane.ready,
        localOwns: livePlane.ready and livePlane.direction == Direction.Out,
      )
      var ownerChanged = false
      var liveChanged = false
      var controlChanged = false
      await state.lock.acquire()
      try:
        ownerChanged = state.submitPeerLiveBaseOwnerChanged(
          liveOwnerId,
          liveOwner.known,
          liveOwner.localOwns,
        )
        liveChanged = state.liveGeneration != livePlane.generation
        controlChanged = state.controlGeneration != controlPlane.generation
        state.liveGeneration = livePlane.generation
        state.controlGeneration = controlPlane.generation
        state.connectedBaseOwnerId = liveOwnerId
        state.connectedBaseOwnerKnown = liveOwner.known
        state.localOwnsConnectedBase = liveOwner.localOwns
      finally:
        try:
          if state.lock.locked():
            state.lock.release()
        except AsyncLockError:
          discard
      when defined(fabric_submit_diag) or defined(fabric_lsmr_diag):
        echo "fabric-submit conn-event-disconnected self=",
          peerIdString(network.switch.peerInfo.peerId),
          " peer=", peerIdText,
          " ownerKnown=", liveOwner.known,
          " localOwnsBase=", liveOwner.localOwns,
          " ownerChanged=", ownerChanged,
          " liveChanged=", liveChanged,
          " controlChanged=", controlChanged,
          " actor=true"
      if ownerChanged:
        network.clearSubmitPeerStreams(peerId, "actor-disconnected")
        await network.clearSubmitPeerRepairWait(
          peerId,
          "conn-event-disconnected",
          preserveVerified = false,
        )
        if network.submitPeerHasDemand(peerId):
          wantRepair = true
      elif controlChanged:
        network.clearSubmitPeerStream(peerId, fsscEventCertificate, "actor-control-disconnected")
        network.clearSubmitPeerStream(peerId, fsscControl, "actor-control-disconnected")

    if sawConnected:
      let livePlane = network.switch.connManager.planeState(peerId, ppkLive)
      let controlPlane = network.switch.connManager.planeState(peerId, ppkControl)
      let liveOwnerId = livePlane.ownerMuxerId
      let liveOwner = (
        known: livePlane.ready,
        localOwns: livePlane.ready and livePlane.direction == Direction.Out,
      )
      var ownerChanged = false
      var liveChanged = false
      var controlChanged = false
      await state.lock.acquire()
      try:
        ownerChanged = state.submitPeerLiveBaseOwnerChanged(
          liveOwnerId,
          liveOwner.known,
          liveOwner.localOwns,
        )
        liveChanged = state.liveGeneration != livePlane.generation
        controlChanged = state.controlGeneration != controlPlane.generation
        state.liveGeneration = livePlane.generation
        state.controlGeneration = controlPlane.generation
        state.connectedBaseOwnerId = liveOwnerId
        state.connectedBaseOwnerKnown = liveOwner.known
        state.localOwnsConnectedBase = liveOwner.localOwns
      finally:
        try:
          if state.lock.locked():
            state.lock.release()
        except AsyncLockError:
          discard
      when defined(fabric_submit_diag) or defined(fabric_lsmr_diag):
        echo "fabric-submit conn-event-connected self=",
          peerIdString(network.switch.peerInfo.peerId),
          " peer=", peerIdText,
          " incoming=", connectedIncoming,
          " ownerKnown=", liveOwner.known,
          " localOwnsBase=", liveOwner.localOwns,
          " ownerChanged=", ownerChanged,
          " liveChanged=", liveChanged,
          " controlChanged=", controlChanged,
          " actor=true"
      await network.clearSubmitPeerRepairWait(
        peerId,
        "conn-event-connected",
        preserveVerified = true,
      )
      network.setSubmitPeerForceRepairHint(peerId, false, "conn-event-connected")
      if ownerChanged:
        wantWarm = true
      elif controlChanged:
        network.clearSubmitPeerStream(peerId, fsscEventCertificate, "actor-control-connected")
        network.clearSubmitPeerStream(peerId, fsscControl, "actor-control-connected")
      if not network.submitReadyHook.isNil:
        network.submitReadyHook()

    for session in network.submitPeerSessions(peerId):
      if not session.isNil and session.submitHasPending():
        wantedSessionShards[session.transportClass] =
          wantedSessionShards[session.transportClass] or
          submitSessionShardBit(session.streamSlot)

    let activeDemand = network.submitPeerHasDemand(peerId)
    let needBase =
      wantWarm or wantRepair or activeDemand or
      submitSessionShardsQueued(wantedSessionShards) or
      network.submitPeerDesired(peerId)

    if needBase and
        (not network.switch.isConnected(peerId) or state.transportBlocked or wantRepair):
      discard await network.ensureSubmitPeerConnected(
        peerId,
        forceRepair = wantRepair or state.transportBlocked,
        forceLocalRepair = activeDemand or wantRepair,
      )

    let hotSessionNeedsWarm = state.submitPeerHotSessionNeedsWarm(wantedSessionShards)
    if network.switch.isConnected(peerId) and
        (wantWarm or
        (submitSessionShardsQueued(wantedSessionShards) and
        (not state.transportVerified or hotSessionNeedsWarm))):
      discard await network.probeSubmitTransport(peerId)

    for sessionClass in FabricSubmitSessionClass:
      let wantedShards = wantedSessionShards[sessionClass]
      if wantedShards == 0:
        continue
      let lane = submitSessionClassLane(sessionClass)
      for shardIdx in 0 ..< submitSessionShardCount(sessionClass):
        if (wantedShards and submitSessionShardBit(shardIdx)) == 0:
          continue
        let session = network.submitPeerSession(peerId, sessionClass, shardIdx)
        if session.isNil or not session.submitHasPending():
          continue
        if network.submitTransportDispatchable(peerId, lane):
          await network.signalSubmitSessionDispatch(
            peerId,
            peerIdText,
            sessionClass,
            shardIdx,
          )

proc enqueueSubmitPeerCommand(
    network: FabricNetwork,
    peerId: PeerId,
    command: FabricSubmitPeerCommand,
): Future[void] {.async: (raises: [CancelledError]).} =
  if network.isNil or peerId.data.len == 0 or network.stopping:
    return
  let state = network.getOrCreateSubmitPeerConnect(peerId)
  if state.isNil or state.lock.isNil:
    return
  var runnerToStart: Future[void] = nil
  await state.lock.acquire()
  try:
    case command.kind
    of fspckConnConnected:
      state.actorConnConnected = true
      state.actorConnIncoming = command.incoming
    of fspckConnDisconnected:
      state.actorConnDisconnected = true
    of fspckEnsureLane:
      state.actorWantedSessionShards.includeSubmitSessionClasses(command.lane)
    of fspckScheduleSession:
      state.actorWantedSessionShards[command.sessionClass] =
        state.actorWantedSessionShards[command.sessionClass] or
        submitSessionShardBit(command.sessionShard)
    of fspckWarm:
      state.actorWantWarm = true
    of fspckRepair:
      state.actorWantRepair = true
    if state.actorRunner.isNil or state.actorRunner.finished():
      state.actorRunner = network.runSubmitPeerActor(peerId)
      runnerToStart = state.actorRunner
  finally:
    try:
      if state.lock.locked():
        state.lock.release()
    except AsyncLockError:
      discard
  if not runnerToStart.isNil:
    asyncSpawn runnerToStart

proc scheduleSubmitPeerCommand(
    network: FabricNetwork,
    peerId: PeerId,
    command: FabricSubmitPeerCommand,
) {.gcsafe, raises: [].} =
  if network.isNil or peerId.data.len == 0 or network.stopping:
    return
  proc enqueue() {.async, gcsafe.} =
    try:
      await network.enqueueSubmitPeerCommand(peerId, command)
    except CancelledError:
      discard
  asyncSpawn enqueue()

proc ensureSubmitPeerConnectivity*(
    network: FabricNetwork, peerIdText: string, lane: FabricSubmitLane
) {.gcsafe, raises: [].}
proc prepareStop*(network: FabricNetwork) {.async.}

proc submitPeerReady*(network: FabricNetwork, peerId: PeerId, lane: FabricSubmitLane): bool {.gcsafe, raises: [].} =
  if network.isNil or peerId.data.len == 0:
    return false
  let ready = network.submitTransportDispatchable(peerId, lane)
  if network.isLsmrSubmitMode() and not ready and not network.stopping:
    network.ensureSubmitPeerConnectivity($peerId, lane)
  ready

proc submitPeerReady*(network: FabricNetwork, peerIdText: string, lane: FabricSubmitLane): bool {.gcsafe, raises: [].} =
  if network.isNil or peerIdText.len == 0:
    return false
  let peerId = PeerId.init(peerIdText).valueOr:
    return false
  network.submitPeerReady(peerId, lane)

proc submitPeerReady*(network: FabricNetwork, peerId: PeerId): bool {.gcsafe, raises: [].} =
  if network.isNil or peerId.data.len == 0:
    return false
  network.submitPeerReady(peerId, fslEvent)

proc submitPeerReady*(network: FabricNetwork, peerIdText: string): bool {.gcsafe, raises: [].} =
  if network.isNil or peerIdText.len == 0:
    return false
  let peerId = PeerId.init(peerIdText).valueOr:
    return false
  network.submitPeerReady(peerId)

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
    let ready = network.submitPeerReadyNow(peerId)
    if ready:
      inc result.ready
      states.add(peerIdString(peerId) & "=ready")
    else:
      states.add(network.submitPeerDiag($peerId))
  result.detail = states.join(" | ")

proc submitLaneDesiredPeerStats*(
    network: FabricNetwork, lane: FabricSubmitLane
): tuple[ready, total: int, detail: string] {.gcsafe, raises: [].} =
  if network.isNil or network.switch.isNil:
    result.detail = "network=nil"
    return
  let peers = network.desiredSubmitPeers()
  result.total = peers.len
  var states: seq[string] = @[]
  for peerId in peers:
    let ready = network.submitPeerReadyNow(peerId, lane)
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
    network: FabricNetwork, peerIdText: string
) {.gcsafe, raises: [].}
proc schedulePendingSubmitRepair(
    network: FabricNetwork, peerId: PeerId, peerIdText: string
) {.gcsafe, raises: [].}

proc invalidateSubmitSession(
    network: FabricNetwork,
    peerId: PeerId,
    disconnected: bool,
) {.async: (raises: [CancelledError]).} =
  if network.isNil or peerId.data.len == 0:
    return
  if network.stopping:
    if disconnected:
      network.clearSubmitPeerStreams(peerId, "invalidate-stopping")
    return
  let sessions = network.submitPeerSessions(peerId)
  let state = network.getOrCreateSubmitPeerConnect(peerId)
  var intentionalDisconnect = false
  if disconnected and not state.isNil and not state.lock.isNil:
    await state.lock.acquire()
    try:
      if state.suppressedDisconnects > 0:
        dec state.suppressedDisconnects
        intentionalDisconnect = true
    finally:
      try:
        if state.lock.locked():
          state.lock.release()
      except AsyncLockError:
        discard
  when defined(fabric_submit_diag) or defined(fabric_lsmr_diag):
    echo "fabric-submit invalidate-session self=",
      (if network.switch.isNil: "" else: peerIdString(network.switch.peerInfo.peerId)),
      " peer=", peerIdString(peerId),
      " disconnected=", disconnected,
      " intentional=", intentionalDisconnect,
      " queued=", network.submitPeerPendingLen(peerId),
      " inflight=", network.submitPeerInflightCount(peerId)
  if disconnected:
    network.clearSubmitPeerStreams(peerId, "invalidate-disconnected")
  if intentionalDisconnect:
    when defined(fabric_submit_diag) or defined(fabric_lsmr_diag):
      echo "fabric-submit invalidate-skip-intentional self=",
        (if network.switch.isNil: "" else: peerIdString(network.switch.peerInfo.peerId)),
        " peer=", peerIdString(peerId)
    return
  let hasSubmitWork = sessions.anyIt(it.submitHasPending() or it.inflightCount > 0)
  let shouldHoldDesiredOwnerRepair =
    disconnected and not hasSubmitWork and
    network.submitPeerDesired(peerId) and
    network.submitPeerOwnsRepair(peerId)
  if disconnected and not hasSubmitWork:
    if shouldHoldDesiredOwnerRepair:
      when defined(fabric_submit_diag) or defined(fabric_lsmr_diag):
        echo "fabric-submit invalidate-idle-owner-repair self=",
          (if network.switch.isNil: "" else: peerIdString(network.switch.peerInfo.peerId)),
          " peer=", peerIdString(peerId)
    else:
      when defined(fabric_submit_diag) or defined(fabric_lsmr_diag):
        echo "fabric-submit invalidate-idle-skip self=",
          (if network.switch.isNil: "" else: peerIdString(network.switch.peerInfo.peerId)),
          " peer=", peerIdString(peerId)
        echo "fabric-submit invalidate-idle-hold self=",
          (if network.switch.isNil: "" else: peerIdString(network.switch.peerInfo.peerId)),
          " peer=", peerIdString(peerId)
      return
  if disconnected or hasSubmitWork:
    network.ensureSubmitPeerConnectivity($peerId)

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

proc dialSubmitExistingStream(
    network: FabricNetwork,
    peerId: PeerId,
    sessionClass = fsscEvent,
    escalateBlocked = true,
): Future[Connection] {.async: (raises: [CancelledError]).} =
  if network.isNil or network.switch.isNil or network.switch.connManager.isNil:
    return nil
  var stream: Connection = nil
  let muxer = network.selectSubmitSessionMuxer(peerId, sessionClass)
  let startedAtMs = diagNowMs()
  var stage = "get-stream"
  var getStreamMs = 0'i64
  var beginProtoMs = 0'i64
  var selectMs = 0'i64
  var beganProtocol = false
  template classifyExistingDialFailure(failReason: string) =
    block:
      let baseState = network.submitPeerBaseState(peerId)
      if baseState.connected and baseState.connCount > 0:
        when defined(fabric_submit_diag) or defined(fabric_lsmr_diag):
          echo "fabric-submit dial-existing-live-base-stale self=",
            (if network.switch.isNil: "" else: peerIdString(network.switch.peerInfo.peerId)),
            " peer=", peerIdString(peerId),
            " stage=", stage,
            " connected=", baseState.connected,
            " connCount=", baseState.connCount,
            " reason=", failReason
        network.setSubmitPeerTransportBlocked(peerId, true, failReason)
        network.setSubmitPeerForceRepairHint(
          peerId, true, "dial-existing-live-base-" & failReason
        )
      elif escalateBlocked:
        network.setSubmitPeerTransportBlocked(
          peerId, true, failReason
        )
      else:
        network.setSubmitPeerTransportVerified(
          peerId, false, failReason
        )
  try:
    when defined(fabric_submit_diag) or defined(fabric_lsmr_diag):
      echo "fabric-submit dial-existing-stage self=",
        (if network.switch.isNil: "" else: peerIdString(network.switch.peerInfo.peerId)),
        " peer=", peerIdString(peerId),
        " session=", submitSessionClassText(sessionClass),
        " stage=", stage,
        " timeoutMs=", FabricSubmitGetStreamTimeout.milliseconds
    stream = await awaitSubmitFutureWithin(
      network.switch.connManager.getStream(muxer),
      FabricSubmitGetStreamTimeout,
      "submit getStream timeout",
    )
    getStreamMs = diagNowMs() - startedAtMs
    if stream.isNil:
      when defined(fabric_submit_diag) or defined(fabric_lsmr_diag):
        echo "fabric-submit dial-existing-nil-stream self=",
          (if network.switch.isNil: "" else: peerIdString(network.switch.peerInfo.peerId)),
          " peer=", peerIdString(peerId),
          " session=", submitSessionClassText(sessionClass),
          " stage=", stage,
          " elapsedMs=", getStreamMs
      if escalateBlocked:
        classifyExistingDialFailure("dial-existing-nil-stream")
      else:
        network.setSubmitPeerTransportVerified(
          peerId, false, "dial-existing-nil-stream"
        )
      return nil
    stage = "begin-protocol"
    when defined(fabric_submit_diag) or defined(fabric_lsmr_diag):
      echo "fabric-submit dial-existing-stage self=",
        (if network.switch.isNil: "" else: peerIdString(network.switch.peerInfo.peerId)),
        " peer=", peerIdString(peerId),
        " session=", submitSessionClassText(sessionClass),
        " stage=", stage,
        " timeoutMs=", FabricSubmitBeginProtocolTimeout.milliseconds
    await awaitSubmitFutureWithin(
      stream.beginProtocolNegotiation(),
      FabricSubmitBeginProtocolTimeout,
      "submit beginProtocol timeout",
    )
    beganProtocol = true
    beginProtoMs = diagNowMs() - startedAtMs
    try:
      stage = "multistream-select"
      when defined(fabric_submit_diag) or defined(fabric_lsmr_diag):
        echo "fabric-submit dial-existing-stage self=",
          (if network.switch.isNil: "" else: peerIdString(network.switch.peerInfo.peerId)),
          " peer=", peerIdString(peerId),
          " session=", submitSessionClassText(sessionClass),
          " stage=", stage,
          " warnMs=", FabricSubmitSelectTimeout.milliseconds
      # MultistreamSelect 自己已经有读写超时，这里不能再外包一个更短的总超时，
      # 否则会把合法但偏慢的握手误杀成 stream-open-fail。
      let selected = await MultistreamSelect.select(stream, @[SubmitFabricCodec])
      selectMs = diagNowMs() - startedAtMs
      if selected != SubmitFabricCodec:
        closeSubmitConnSoon(stream, "dial-existing-select-mismatch")
        if escalateBlocked:
          classifyExistingDialFailure("dial-existing-select-mismatch")
        else:
          network.setSubmitPeerTransportVerified(
            peerId, false, "dial-existing-select-mismatch"
          )
        return nil
      let totalMs = diagNowMs() - startedAtMs
      when defined(fabric_submit_diag) or defined(fabric_lsmr_diag):
        if totalMs >= FabricSlowSubmitConnectMs:
          echo "fabric-submit dial-existing-slow self=",
            (if network.switch.isNil: "" else: peerIdString(network.switch.peerInfo.peerId)),
            " peer=", peerIdString(peerId),
            " session=", submitSessionClassText(sessionClass),
            " totalMs=", totalMs,
            " getStreamMs=", getStreamMs,
            " beginProtocolMs=", beginProtoMs - getStreamMs,
            " selectMs=", selectMs - beginProtoMs
        if selectMs - beginProtoMs >= FabricSubmitSelectTimeout.milliseconds:
          echo "fabric-submit dial-existing-select-slow self=",
            (if network.switch.isNil: "" else: peerIdString(network.switch.peerInfo.peerId)),
            " peer=", peerIdString(peerId),
            " session=", submitSessionClassText(sessionClass),
            " totalMs=", totalMs,
            " selectMs=", selectMs - beginProtoMs,
            " warnMs=", FabricSubmitSelectTimeout.milliseconds
      return stream
    finally:
      if beganProtocol and not stream.isNil:
        stream.endProtocolNegotiation()
  except CancelledError as exc:
    if not stream.isNil:
      closeSubmitConnSoon(stream, "dial-existing-cancelled")
    raise exc
  except CatchableError as exc:
    let baseState = network.submitPeerBaseState(peerId)
    when defined(fabric_submit_diag) or defined(fabric_lsmr_diag):
      echo "fabric-submit dial-existing-fail self=",
        (if network.switch.isNil: "" else: peerIdString(network.switch.peerInfo.peerId)),
        " peer=", peerIdString(peerId),
        " session=", submitSessionClassText(sessionClass),
        " stage=", stage,
        " elapsedMs=", diagNowMs() - startedAtMs,
        " getStreamMs=", getStreamMs,
        " beginProtocolMs=", (if beginProtoMs > 0: beginProtoMs - getStreamMs else: 0),
        " selectMs=", (if selectMs > 0 and beginProtoMs > 0: selectMs - beginProtoMs else: 0),
        " connected=", baseState.connected,
        " connCount=", baseState.connCount,
        " err=", exc.msg
    if escalateBlocked:
      classifyExistingDialFailure("dial-existing-fail-" & stage)
    else:
      network.setSubmitPeerTransportVerified(
        peerId, false, "dial-existing-fail-" & stage
      )
    if not stream.isNil:
      closeSubmitConnSoon(stream, "dial-existing-fail")
    return nil

proc probeSubmitTransport(
    network: FabricNetwork, peerId: PeerId
): Future[bool] {.async: (raises: [CancelledError]).} =
  if network.isNil or network.switch.isNil or peerId.data.len == 0:
    return false
  let state = network.getOrCreateSubmitPeerConnect(peerId)
  if state.isNil or state.lock.isNil:
    return false
  var runner: Future[bool] = nil

  proc runProbe(): Future[bool] {.async: (raises: [CancelledError]).} =
    if not network.switch.isConnected(peerId):
      network.setSubmitPeerTransportVerified(peerId, false, "probe-disconnected")
      return false
    if network.submitPeerBusy(peerId):
      when defined(fabric_submit_diag) or defined(fabric_lsmr_diag):
        echo "fabric-submit probe-deferred-busy self=",
          peerIdString(network.switch.peerInfo.peerId),
          " peer=", peerIdString(peerId),
          " ready=", network.submitTransportReady(peerId, fslEvent)
      return network.submitTransportReady(peerId, fslEvent)
    let session = network.getOrCreateSubmitSession(peerId, fslEvent)
    if session.isNil or session.openLock.isNil:
      network.setSubmitPeerTransportVerified(peerId, false, "probe-session-missing")
      return false
    let selfPeerText =
      if network.switch.isNil: ""
      else: peerIdString(network.switch.peerInfo.peerId)
    proc probeKeepSessionStream(
        sessionClass: FabricSubmitSessionClass,
        failReason: string,
    ): Future[bool] {.async: (raises: [CancelledError]).} =
      let probeKey = "warm:" & selfPeerText & ":" & submitSessionClassText(sessionClass)
      let encoded = encodeSubmitFrame(0, @[FabricSubmitEnvelope(
        itemKey: probeKey,
        kind: fskWarm,
        payload: selfPeerText,
        scopePrefix: @[],
      )])
      if encoded.isNone():
        return false
      let probeSession = network.getOrCreateSubmitSession(peerId, sessionClass)
      if probeSession.isNil or probeSession.openLock.isNil:
        return false
      let keepWarmStream = true
      var stream: Connection = nil
      var openLockHeld = false
      var reservedSlot = -1
      var keepStream = false
      try:
        await probeSession.openLock.acquire()
        openLockHeld = true
        if network.submitPeerStreamReady(peerId, sessionClass):
          return true
        let reserved = await network.reserveSubmitSessionStream(
          peerId,
          probeSession,
          escalateBlocked = true,
        )
        stream = reserved.conn
        reservedSlot = reserved.slotIdx
        if reservedSlot < 0 and network.submitPeerStreamReady(peerId, sessionClass):
          return true
        if stream.isNil or stream.closed or stream.atEof:
          return false
        await stream.writeLp(bytesOf(encoded.get()))
        let readFuture = stream.readLp(SubmitAckMaxBytes)
        let completed = await withTimeout(readFuture, FabricSubmitAckTimeout)
        if not completed:
          if not readFuture.finished():
            readFuture.cancelSoon()
          return false
        let ack = readFuture.read()
        if ack.len == 0:
          return false
        let ackFrame = safeDecode[FabricSubmitAckFrame](stringOf(ack))
        if ackFrame.isNone() or ackFrame.get().requestId != 0 or
            ackFrame.get().items.len != 1 or
            ackFrame.get().items[0].itemKey != probeKey or
            not ackFrame.get().items[0].accepted:
          return false
        if keepWarmStream:
          keepStream = true
          stream = nil
          when defined(fabric_submit_diag) or defined(fabric_lsmr_diag):
            echo "fabric-submit probe-keep self=",
              selfPeerText,
              " peer=", peerIdString(peerId),
              " session=", submitSessionClassText(sessionClass)
        return true
      except CancelledError as exc:
        raise exc
      except CatchableError as exc:
        when defined(fabric_submit_diag) or defined(fabric_lsmr_diag):
          echo "fabric-submit probe-fail self=",
            selfPeerText,
            " peer=", peerIdString(peerId),
            " session=", submitSessionClassText(sessionClass),
            " reason=", failReason,
            " err=", exc.msg
        return false
      finally:
        if openLockHeld:
          try:
            if probeSession.openLock.locked():
              probeSession.openLock.release()
          except AsyncLockError:
            discard
        if reservedSlot >= 0:
          network.releaseSubmitSessionStream(
            peerId,
            sessionClass,
            reservedSlot,
            stream,
            if keepStream: "" else: "probe-finally",
          )
        elif not stream.isNil:
          closeSubmitConnSoon(stream, "probe-finally")
    try:
      if not await probeKeepSessionStream(fsscEvent, "probe-event-stream"):
        network.setSubmitPeerTransportVerified(peerId, false, "probe-stream-invalid")
        return false
      if not await probeKeepSessionStream(fsscAttestation, "probe-attestation-stream"):
        network.setSubmitPeerTransportVerified(peerId, false, "probe-attestation-stream-invalid")
        return false
      if not await probeKeepSessionStream(fsscEventCertificate, "probe-cert-stream"):
        network.setSubmitPeerTransportVerified(peerId, false, "probe-cert-stream-invalid")
        return false
      discard await probeKeepSessionStream(fsscControl, "probe-control-stream")
      network.setSubmitPeerTransportVerified(peerId, true, "probe-ok")
      return true
    except CancelledError as exc:
      raise exc
    except CatchableError as exc:
      when defined(fabric_submit_diag) or defined(fabric_lsmr_diag):
        echo "fabric-submit probe-fail self=",
          selfPeerText,
          " peer=", peerIdString(peerId),
          " err=", exc.msg
      network.setSubmitPeerTransportVerified(peerId, false, "probe-exception")
      return false

  await state.lock.acquire()
  try:
    if network.switch.isConnected(peerId) and state.transportVerified:
      return true
    if state.probeRunner.isNil or state.probeRunner.finished():
      state.probeRunner = runProbe()
    runner = state.probeRunner
  finally:
    try:
      if state.lock.locked():
        state.lock.release()
    except AsyncLockError:
      discard

  try:
    return await runner
  except CancelledError as exc:
    raise exc
  except CatchableError:
    return false
  finally:
    await state.lock.acquire()
    try:
      if state.probeRunner == runner and runner.finished():
        state.probeRunner = nil
    finally:
      try:
        if state.lock.locked():
          state.lock.release()
      except AsyncLockError:
        discard

proc dialSubmitPeer(
    network: FabricNetwork, peerId: PeerId, allowRedial: bool
): Future[Connection] {.async: (raises: [CancelledError]).} =
  if network.isNil or network.switch.isNil or peerId.data.len == 0:
    return nil
  try:
    let peerStoreAddrs = network.submitPeerStoreAddrs(peerId)
    let hintAddrs = network.submitPeerHintAddrs(peerId)
    var addrs = peerStoreAddrs
    addrs.mergeUniqueAddrs(hintAddrs)
    let peerStoreList = submitAddrTexts(peerStoreAddrs)
    let hintList = submitAddrTexts(hintAddrs)
    let dialList = submitAddrTexts(addrs)
    let connected = network.switch.isConnected(peerId)
    let selectedBaseOutgoing = network.submitPeerSelectedBaseOutgoing(peerId)
    when defined(fabric_submit_diag) or defined(fabric_lsmr_diag):
      echo "fabric-submit dial-plan self=",
        (if network.switch.isNil: "" else: peerIdString(network.switch.peerInfo.peerId)),
        " peer=", peerIdString(peerId),
        " stage=dial-submit-peer",
        " connected=", connected,
        " selectedBaseOutgoing=", selectedBaseOutgoing,
        " addrs=", addrs.len,
        " peerStoreAddrs=", peerStoreAddrs.len,
        " hintAddrs=", hintAddrs.len,
        " peerStoreList=", peerStoreList,
        " hintList=", hintList,
        " list=", dialList
    if connected:
      return await network.dialSubmitExistingStream(peerId)
    if not allowRedial:
      when defined(fabric_submit_diag) or defined(fabric_lsmr_diag):
        echo "fabric-submit dial-skip-no-redial self=",
          (if network.switch.isNil: "" else: peerIdString(network.switch.peerInfo.peerId)),
          " peer=", peerIdString(peerId)
      return nil
    if addrs.len == 0:
      when defined(fabric_submit_diag) or defined(fabric_lsmr_diag):
        echo "fabric-submit dial-no-addrs self=",
          (if network.switch.isNil: "" else: peerIdString(network.switch.peerInfo.peerId)),
          " peer=", peerIdString(peerId)
      return nil
    return await network.switch.dial(
      peerId,
      addrs,
      @[SubmitFabricCodec],
    )
  except CancelledError as exc:
    raise exc
  except CatchableError as exc:
    when defined(fabric_submit_diag) or defined(fabric_lsmr_diag):
      echo "fabric-submit dial-fail self=",
        (if network.switch.isNil: "" else: peerIdString(network.switch.peerInfo.peerId)),
        " peer=", peerIdString(peerId),
        " err=", exc.msg
    return nil

proc ensureSubmitPeerConnected*(
    network: FabricNetwork,
    peerId: PeerId,
    forceRepair = false,
    forceLocalRepair = false,
): Future[bool] {.async: (raises: [CancelledError]).} =
  if network.isNil or network.switch.isNil or peerId.data.len == 0:
    return false
  let state = network.getOrCreateSubmitPeerConnect(peerId)
  if state.isNil or state.lock.isNil:
    return false
  let liveOwnerId = network.submitPeerLiveBaseOwnerId(peerId)
  let liveOwner = network.submitPeerLiveBaseOwnerState(peerId)
  state.connectedBaseOwnerId = liveOwnerId
  state.connectedBaseOwnerKnown = liveOwner.known
  state.localOwnsConnectedBase = liveOwner.localOwns
  var effectiveForceRepair = forceRepair
  if state.preferForceRepair:
    effectiveForceRepair = true
  if state.transportBlocked:
    effectiveForceRepair = true
  let ownsRepair =
    if liveOwner.known: liveOwner.localOwns
    else: network.submitPeerOwnsRepair(peerId)
  let activeDemand = network.submitPeerHasDemand(peerId)
  let localRepairClaim = submitPeerHasLocalRepairClaim(state)
  let connectedNow = network.switch.isConnected(peerId)
  let knownBaseOwner = liveOwner.known
  if knownBaseOwner and not connectedNow and not ownsRepair:
    when defined(fabric_submit_diag) or defined(fabric_lsmr_diag):
      echo "fabric-submit base-repair-wait-current-owner self=",
        peerIdString(network.switch.peerInfo.peerId),
        " peer=", peerIdString(peerId),
        " blocked=", state.transportBlocked,
        " activeDemand=", activeDemand,
        " localClaim=", localRepairClaim,
        " forceLocal=", forceLocalRepair
    return false
  if effectiveForceRepair and connectedNow and not ownsRepair:
    when defined(fabric_submit_diag) or defined(fabric_lsmr_diag):
      echo "fabric-submit base-repair-wait-owner-connected self=",
        peerIdString(network.switch.peerInfo.peerId),
        " peer=", peerIdString(peerId),
        " blocked=", state.transportBlocked,
        " activeDemand=", activeDemand,
        " localClaim=", localRepairClaim,
        " forceLocal=", forceLocalRepair
    return true
  if effectiveForceRepair and not forceLocalRepair and not ownsRepair and
      not activeDemand and not localRepairClaim:
    when defined(fabric_submit_diag) or defined(fabric_lsmr_diag):
      echo "fabric-submit base-repair-wait-owner self=",
        peerIdString(network.switch.peerInfo.peerId),
        " peer=", peerIdString(peerId),
        " connected=", network.switch.isConnected(peerId),
        " blocked=", state.transportBlocked,
        " activeDemand=", activeDemand,
        " localClaim=", localRepairClaim
    return network.switch.isConnected(peerId)
  if effectiveForceRepair and not ownsRepair and
      (forceLocalRepair or activeDemand or localRepairClaim):
    when defined(fabric_submit_diag) or defined(fabric_lsmr_diag):
      echo "fabric-submit base-repair-local-demand self=",
        peerIdString(network.switch.peerInfo.peerId),
        " peer=", peerIdString(peerId),
        " connected=", network.switch.isConnected(peerId),
        " blocked=", state.transportBlocked,
        " activeDemand=", activeDemand,
        " localClaim=", localRepairClaim,
        " forceLocal=", forceLocalRepair
  let repairSession =
    network.submitPeerSession(peerId, fsscEvent)
  if effectiveForceRepair and not repairSession.isNil and not repairSession.lock.isNil:
    var inflight = 0
    await repairSession.lock.acquire()
    try:
      inflight = network.submitPeerInflightCount(peerId)
    finally:
      try:
        if repairSession.lock.locked():
          repairSession.lock.release()
      except AsyncLockError:
        discard
    let connectedNow = network.switch.isConnected(peerId)
    let blockedNow = state.transportBlocked
    if inflight > 0:
      var alreadyPending = false
      await state.lock.acquire()
      try:
        alreadyPending = state.pendingForceRepair
        state.pendingForceRepair = true
      finally:
        try:
          if state.lock.locked():
            state.lock.release()
        except AsyncLockError:
          discard
      when defined(fabric_submit_diag) or defined(fabric_lsmr_diag):
        if not alreadyPending:
          echo "fabric-submit base-repair-deferred self=",
            peerIdString(network.switch.peerInfo.peerId),
            " peer=", peerIdString(peerId),
            " inflight=", inflight,
            " connected=", connectedNow,
            " blocked=", blockedNow
      return connectedNow

  proc connectOne(repair: bool): Future[bool] {.async: (raises: [CancelledError]).} =
    let peerStoreAddrs = network.submitPeerStoreAddrs(peerId)
    let hintAddrs = network.submitPeerHintAddrs(peerId)
    var addrs = peerStoreAddrs
    addrs.mergeUniqueAddrs(hintAddrs)
    let peerStoreList = submitAddrTexts(peerStoreAddrs)
    let hintList = submitAddrTexts(hintAddrs)
    let dialList = submitAddrTexts(addrs)
    let startedAtMs = diagNowMs()
    try:
      when defined(fabric_submit_diag) or defined(fabric_lsmr_diag):
        echo "fabric-submit dial-plan self=",
          peerIdString(network.switch.peerInfo.peerId),
          " peer=", peerIdString(peerId),
          " stage=base-connect",
          " repair=", repair,
          " connected=", network.switch.isConnected(peerId),
          " addrs=", addrs.len,
          " peerStoreAddrs=", peerStoreAddrs.len,
          " hintAddrs=", hintAddrs.len,
          " peerStoreList=", peerStoreList,
          " hintList=", hintList,
          " list=", dialList
      if network.switch.isConnected(peerId):
        if repair:
          let ownsRepairNow = network.submitPeerOwnsRepair(peerId)
          when defined(fabric_submit_diag) or defined(fabric_lsmr_diag):
            echo "fabric-submit base-repair-skip-connected self=",
              peerIdString(network.switch.peerInfo.peerId),
              " peer=", peerIdString(peerId),
              " blocked=", state.transportBlocked,
              " ownsRepair=", ownsRepairNow,
              " ownerKnown=", state.connectedBaseOwnerKnown,
              " localOwnsBase=", state.localOwnsConnectedBase
          if state.transportBlocked and ownsRepairNow:
            await state.lock.acquire()
            try:
              inc state.suppressedDisconnects
            finally:
              try:
                if state.lock.locked():
                  state.lock.release()
              except AsyncLockError:
                discard
            when defined(fabric_submit_diag) or defined(fabric_lsmr_diag):
              echo "fabric-submit base-repair-replace-blocked self=",
                peerIdString(network.switch.peerInfo.peerId),
                " peer=", peerIdString(peerId),
                " ownerKnown=", state.connectedBaseOwnerKnown,
                " localOwnsBase=", state.localOwnsConnectedBase
            try:
              await network.switch.disconnect(peerId)
            except CancelledError as exc:
              raise exc
            except CatchableError:
              await state.lock.acquire()
              try:
                if state.suppressedDisconnects > 0:
                  dec state.suppressedDisconnects
              finally:
                try:
                  if state.lock.locked():
                    state.lock.release()
                except AsyncLockError:
                  discard
              raise
            let reconnectedDuringSettle =
              await network.waitSubmitPeerDisconnectSettled(peerId)
            if reconnectedDuringSettle:
              when defined(fabric_submit_diag) or defined(fabric_lsmr_diag):
                echo "fabric-submit base-repair-skip-reconnected self=",
                  peerIdString(network.switch.peerInfo.peerId),
                  " peer=", peerIdString(peerId)
              network.setSubmitPeerTransportVerified(peerId, false, "base-reconnect-raced")
              return true
          else:
            await network.clearSubmitPeerRepairWait(
              peerId,
              "base-repair-skip-connected",
              preserveVerified = false,
            )
            network.setSubmitPeerTransportVerified(
              peerId, false, "base-repair-skip-connected"
            )
            return true
        else:
          return true
      if addrs.len == 0:
        when defined(fabric_submit_diag) or defined(fabric_lsmr_diag):
          echo "fabric-submit base-no-addrs self=",
            peerIdString(network.switch.peerInfo.peerId),
            " peer=", peerIdString(peerId)
        return false
      await network.switch.connect(
        peerId,
        addrs,
        forceDial = repair,
        reuseConnection = not repair,
      )
      let connectedNow = network.switch.isConnected(peerId)
      let elapsedMs = diagNowMs() - startedAtMs
      network.lastSubmitConnectElapsedMs = elapsedMs
      if elapsedMs > network.maxSubmitConnectElapsedMs:
        network.maxSubmitConnectElapsedMs = elapsedMs
      if elapsedMs >= FabricSlowSubmitConnectMs:
        inc network.slowSubmitConnectCount
        echo "fabric-submit slow-base-connect self=",
          peerIdString(network.switch.peerInfo.peerId),
          " peer=", peerIdString(peerId),
          " elapsedMs=", elapsedMs,
          " repair=", repair
      if not connectedNow:
        when defined(fabric_submit_diag) or defined(fabric_lsmr_diag):
          echo "fabric-submit base-connect-lost self=",
            peerIdString(network.switch.peerInfo.peerId),
            " peer=", peerIdString(peerId),
            " repair=", repair
        return false
      when defined(fabric_submit_diag) or defined(fabric_lsmr_diag):
        echo "fabric-submit base-connect-ok self=",
          peerIdString(network.switch.peerInfo.peerId),
          " peer=", peerIdString(peerId),
          " repair=", repair,
          " connected=", connectedNow
      await network.clearSubmitPeerRepairWait(
        peerId,
        "base-connect-ok",
        preserveVerified = false,
      )
      network.setSubmitPeerTransportVerified(peerId, false, "base-connect-ok")
      return connectedNow
    except CancelledError as exc:
      raise exc
    except CatchableError as exc:
      when defined(fabric_submit_diag) or defined(fabric_lsmr_diag):
        echo "fabric-submit base-connect-fail self=",
          peerIdString(network.switch.peerInfo.peerId),
          " peer=", peerIdString(peerId),
          " repair=", repair,
          " addrs=", addrs.len,
          " peerStoreAddrs=", peerStoreAddrs.len,
          " hintAddrs=", hintAddrs.len,
          " peerStoreList=", peerStoreList,
          " hintList=", hintList,
          " list=", dialList,
          " err=", exc.msg
      return false

  var runner: Future[bool] = nil
  var rerunForceRepair = false
  await state.lock.acquire()
  try:
    if network.switch.isConnected(peerId) and not effectiveForceRepair:
      return true
    if not state.runner.isNil and not state.runner.finished():
      if effectiveForceRepair and not state.runnerForceRepair:
        state.pendingForceRepair = true
        when defined(fabric_submit_diag) or defined(fabric_lsmr_diag):
          echo "fabric-submit base-repair-queued self=",
            peerIdString(network.switch.peerInfo.peerId),
            " peer=", peerIdString(peerId)
    else:
      state.pendingForceRepair = false
      state.runner = connectOne(effectiveForceRepair)
      state.runnerForceRepair = effectiveForceRepair
    runner = state.runner
  finally:
    try:
      if state.lock.locked():
        state.lock.release()
    except AsyncLockError:
      discard

  try:
    discard await runner
  except CancelledError as exc:
    raise exc
  except CatchableError:
    discard
  finally:
    await state.lock.acquire()
    try:
      if state.runner == runner:
        rerunForceRepair = state.pendingForceRepair and not network.switch.isConnected(peerId)
        state.runner = nil
        state.runnerForceRepair = false
        state.pendingForceRepair = false
    finally:
      try:
        if state.lock.locked():
          state.lock.release()
      except AsyncLockError:
        discard
  if rerunForceRepair:
    when defined(fabric_submit_diag) or defined(fabric_lsmr_diag):
      echo "fabric-submit base-repair-rerun self=",
        peerIdString(network.switch.peerInfo.peerId),
        " peer=", peerIdString(peerId)
    return await network.ensureSubmitPeerConnected(peerId, forceRepair = true)
  network.switch.isConnected(peerId)

proc schedulePendingSubmitRepair(
    network: FabricNetwork, peerId: PeerId, peerIdText: string
) {.gcsafe, raises: [].} =
  if network.isNil or network.switch.isNil or peerId.data.len == 0 or network.stopping:
    return
  let state = network.getOrCreateSubmitPeerConnect(peerId)
  if state.isNil:
    return
  if not state.transportBlocked and not state.preferForceRepair:
    return
  when defined(fabric_submit_diag) or defined(fabric_lsmr_diag):
    echo "fabric-submit base-repair-drain-trigger self=",
      peerIdString(network.switch.peerInfo.peerId),
      " peer=", peerIdText
  network.scheduleSubmitPeerCommand(peerId, FabricSubmitPeerCommand(
    kind: fspckRepair,
    lane: fslEvent,
    sessionClass: fsscEvent,
    sessionShard: 0,
    incoming: false,
  ))

proc ensureSubmitPeerConnectivity*(
    network: FabricNetwork, peerId: PeerId, lane: FabricSubmitLane
): Future[bool] {.async: (raises: [CancelledError]).} =
  if network.isNil or network.switch.isNil or peerId.data.len == 0 or network.stopping:
    return false
  await network.enqueueSubmitPeerCommand(peerId, FabricSubmitPeerCommand(
    kind: fspckEnsureLane,
    lane: lane,
    sessionClass: fsscEvent,
    sessionShard: 0,
    incoming: false,
  ))
  return network.submitTransportReady(peerId, lane)

proc ensureSubmitPeerConnectivity*(
    network: FabricNetwork, peerIdText: string, lane: FabricSubmitLane
) {.gcsafe, raises: [].} =
  if network.isNil or peerIdText.len == 0 or network.stopping:
    return
  let peerId = PeerId.init(peerIdText).valueOr:
    return
  network.scheduleSubmitPeerCommand(peerId, FabricSubmitPeerCommand(
    kind: fspckEnsureLane,
    lane: lane,
    sessionClass: fsscEvent,
    sessionShard: 0,
    incoming: false,
  ))

proc ensureSubmitPeerConnectivity*(
    network: FabricNetwork, peerIdText: string
) {.gcsafe, raises: [].} =
  if network.isNil or peerIdText.len == 0:
    return
  network.ensureSubmitPeerConnectivity(peerIdText, fslEvent)

proc warmSubmitConnections*(network: FabricNetwork): Future[void] {.async: (raises: [CancelledError]).} =
  if network.isNil or not network.isLsmrSubmitMode() or network.stopping:
    return
  for peerId in network.desiredSubmitPeers():
    if network.switch.isNil:
      continue
    await network.enqueueSubmitPeerCommand(peerId, FabricSubmitPeerCommand(
      kind: fspckWarm,
      lane: fslEvent,
      sessionClass: fsscEvent,
      sessionShard: 0,
      incoming: false,
    ))
    for session in network.submitPeerSessions(peerId):
      if session.isNil or not session.submitHasPending():
        continue
      await network.enqueueSubmitPeerCommand(peerId, FabricSubmitPeerCommand(
        kind: fspckScheduleSession,
        lane: submitSessionClassLane(session.transportClass),
        sessionClass: session.transportClass,
        sessionShard: session.streamSlot,
        incoming: false,
      ))
  if not network.isNil and network.hasDesiredUnverifiedSubmitPeer():
    when defined(fabric_submit_diag) or defined(fabric_lsmr_diag):
      echo "fabric-submit warm-rerun-disconnected self=",
        peerIdString(network.switch.peerInfo.peerId)
    network.submitWarmPending = true

proc scheduleWarmSubmitPeer(
    network: FabricNetwork, peerId: PeerId
) {.gcsafe, raises: [].} =
  if network.isNil or network.switch.isNil or not network.isLsmrSubmitMode() or
      peerId.data.len == 0 or network.stopping:
    return
  network.scheduleSubmitPeerCommand(peerId, FabricSubmitPeerCommand(
    kind: fspckWarm,
    lane: fslEvent,
    sessionClass: fsscEvent,
    sessionShard: 0,
    incoming: false,
  ))

proc scheduleWarmSubmitConnections*(network: FabricNetwork) {.gcsafe, raises: [].} =
  if network.isNil or not network.isLsmrSubmitMode() or network.stopping:
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

proc runSubmitRequestRpc(
    network: FabricNetwork,
    peerId: PeerId,
    peerIdText: string,
    batch: FabricSubmitRpcBatch,
): Future[void] {.async: (raises: [CancelledError]).} =
  if network.isNil or peerId.data.len == 0:
    return
  if batch.requests.len == 0 or batch.items.len == 0 or batch.encoded.len == 0:
    return
  let sessionShard = batch.requests[0].sessionShard
  let session = network.getOrCreateSubmitSession(
    peerId,
    batch.transportClass,
    sessionShard,
  )
  let state = network.getOrCreateSubmitPeerConnect(peerId)
  if session.isNil or state.isNil:
    return
  var currentBatch = batch
  var stream: Connection = nil
  var forceRepair = false
  var writtenAtMs = 0'i64
  var ackReadStartedAtMs = 0'i64
  var reservedSlot = -1
  let rpcStartedAtMs = diagNowMs()
  let pipelineWindow = submitBatchPipelineWindow(batch.transportClass)
  let stickyDrain = pipelineWindow > 1
  var queuedBatch = batch
  var hasQueuedBatch = true
  var inflightBatches = initDeque[FabricSubmitPipelineEntry]()
  var drainedInflight = false
  template dropSubmitStream(reason: string) =
    block:
      if reservedSlot >= 0:
        network.releaseSubmitSessionStream(
          peerId,
          currentBatch.transportClass,
          reservedSlot,
          stream,
          reason,
        )
      elif not stream.isNil:
        closeSubmitConnSoon(stream, reason)
      reservedSlot = -1
      stream = nil
  template markSubmitFailure(stage: string) =
    currentBatch.failSubmitBatch()
    let queuedDemand = network.submitPeerHasDemand(peerId)
    let batchCarriesDemand = submitBatchCarriesDemand(currentBatch)
    let shouldRepair = network.submitBatchNeedsRepair(
      peerId, stage, batchCarriesDemand
    )
    if shouldRepair:
      network.setSubmitPeerForceRepairHint(peerId, true, "batch-" & stage)
      forceRepair = true
      when defined(fabric_submit_diag) or defined(fabric_lsmr_diag):
        echo "fabric-submit repair-promoted self=",
          peerIdString(network.switch.peerInfo.peerId),
          " peer=", peerIdText,
          " lane=", submitLaneText(currentBatch.logicalLane),
          " kinds=", submitBatchKindsText(currentBatch.items),
          " stage=", stage,
          " item=", currentBatch.diagItemKey,
          " items=", currentBatch.items.len,
          " batchDemand=", batchCarriesDemand,
          " queuedDemand=", queuedDemand,
          " connected=", network.switch.isConnected(peerId)
    else:
      when defined(fabric_submit_diag) or defined(fabric_lsmr_diag):
        echo "fabric-submit repair-suppressed self=",
          peerIdString(network.switch.peerInfo.peerId),
          " peer=", peerIdText,
          " lane=", submitLaneText(currentBatch.logicalLane),
          " kinds=", submitBatchKindsText(currentBatch.items),
          " stage=", stage,
          " item=", currentBatch.diagItemKey,
          " items=", currentBatch.items.len,
          " batchDemand=", batchCarriesDemand,
          " queuedDemand=", queuedDemand,
          " connected=", network.switch.isConnected(peerId)
    network.ensureSubmitPeerConnectivity(
      peerIdText,
      submitSessionClassLane(currentBatch.transportClass),
    )
  proc failOutstandingBatches() =
    while inflightBatches.len > 0:
      let failed = inflightBatches.popFirst()
      failed.batch.failSubmitBatch()

  proc popQueuedBatch(): Future[FabricSubmitRpcBatch] {.async: (raises: [CancelledError]).} =
    if session.isNil:
      return
    await session.lock.acquire()
    try:
      if not session.submitHasPending():
        return
      return session.popNextSubmitRpcBatch()
    finally:
      try:
        if session.lock.locked():
          session.lock.release()
      except AsyncLockError:
        discard

  proc writeBatch(batchToWrite: FabricSubmitRpcBatch): Future[bool] {.async: (raises: [CancelledError]).} =
    if batchToWrite.requests.len == 0 or batchToWrite.items.len == 0 or batchToWrite.encoded.len == 0:
      return false
    currentBatch = batchToWrite
    writtenAtMs = diagNowMs()
    try:
      when defined(fabric_submit_diag) or defined(fabric_lsmr_diag):
        echo "t=", writtenAtMs,
          " fabric-submit write-begin self=",
          peerIdString(network.switch.peerInfo.peerId),
          " peer=", peerIdText,
          " lane=", submitLaneText(currentBatch.logicalLane),
          " kinds=", submitBatchKindsText(currentBatch.items),
          " item=", currentBatch.diagItemKey,
          " items=", currentBatch.items.len,
          " bytes=", currentBatch.encoded.len
      let writeFuture = stream.writeLp(currentBatch.encoded)
      let wrote = await withTimeout(writeFuture, FabricSubmitWriteTimeout)
      if not wrote:
        if not writeFuture.finished():
          writeFuture.cancelSoon()
        when defined(fabric_submit_diag) or defined(fabric_lsmr_diag):
          echo "fabric-submit write-timeout self=",
            peerIdString(network.switch.peerInfo.peerId),
            " peer=", peerIdText,
            " lane=", submitLaneText(currentBatch.logicalLane),
            " kinds=", submitBatchKindsText(currentBatch.items),
            " item=", currentBatch.diagItemKey,
            " items=", currentBatch.items.len,
            " timeoutMs=", FabricSubmitWriteTimeout.milliseconds
        dropSubmitStream("write-timeout")
        markSubmitFailure("write-timeout")
        failOutstandingBatches()
        return false
      when defined(fabric_submit_diag) or defined(fabric_lsmr_diag):
        echo "t=", diagNowMs(),
          " fabric-submit write-done self=",
          peerIdString(network.switch.peerInfo.peerId),
          " peer=", peerIdText,
          " lane=", submitLaneText(currentBatch.logicalLane),
          " kinds=", submitBatchKindsText(currentBatch.items),
          " item=", currentBatch.diagItemKey,
          " items=", currentBatch.items.len,
          " elapsedMs=", diagNowMs() - writtenAtMs
      inflightBatches.addLast(FabricSubmitPipelineEntry(
        batch: currentBatch,
        writtenAtMs: writtenAtMs,
      ))
      return true
    except CancelledError:
      raise
    except CatchableError as exc:
      when defined(fabric_submit_diag) or defined(fabric_lsmr_diag):
        echo "fabric-submit write-fail self=",
          peerIdString(network.switch.peerInfo.peerId),
          " peer=", peerIdText,
          " lane=", submitLaneText(currentBatch.logicalLane),
          " kinds=", submitBatchKindsText(currentBatch.items),
          " item=", currentBatch.diagItemKey,
          " items=", currentBatch.items.len,
          " err=", exc.msg
      dropSubmitStream("write-fail")
      inc network.submitWriteFailureCount
      markSubmitFailure("write-fail")
      failOutstandingBatches()
      return false
  try:
    if not network.submitTransportDispatchable(
        peerId, submitSessionClassLane(currentBatch.transportClass)
    ):
      markSubmitFailure("transport-not-ready")
      return
    if network.switch.isConnected(peerId) and
        not network.submitPeerSelectedBaseOutgoing(peerId):
      when defined(fabric_submit_diag) or defined(fabric_lsmr_diag):
        echo "fabric-submit selected-base-incoming self=",
          peerIdString(network.switch.peerInfo.peerId),
          " peer=", peerIdText,
          " lane=", submitLaneText(currentBatch.logicalLane)
    let reserved = await network.reserveSubmitSessionStream(
      peerId,
      session,
      escalateBlocked = true,
    )
    stream = reserved.conn
    reservedSlot = reserved.slotIdx
    if reserved.reused and not stream.isNil:
      when defined(fabric_submit_diag) or defined(fabric_lsmr_diag):
        echo "fabric-submit stream-reuse self=",
          peerIdString(network.switch.peerInfo.peerId),
          " peer=", peerIdText,
          " lane=", submitLaneText(currentBatch.logicalLane),
          " kinds=", submitBatchKindsText(currentBatch.items),
          " item=", currentBatch.diagItemKey,
          " items=", currentBatch.items.len
    if stream.isNil:
      when defined(fabric_submit_diag) or defined(fabric_lsmr_diag):
        echo "fabric-submit stream-open-fail self=",
          peerIdString(network.switch.peerInfo.peerId),
          " peer=", peerIdText,
          " lane=", submitLaneText(currentBatch.logicalLane),
          " kinds=", submitBatchKindsText(currentBatch.items),
          " item=", currentBatch.diagItemKey,
          " items=", currentBatch.items.len
      markSubmitFailure("stream-open-fail")
      return
    let connectElapsedMs = diagNowMs() - rpcStartedAtMs
    network.lastSubmitConnectElapsedMs = connectElapsedMs
    if connectElapsedMs > network.maxSubmitConnectElapsedMs:
      network.maxSubmitConnectElapsedMs = connectElapsedMs
    if connectElapsedMs >= FabricSlowSubmitConnectMs:
      inc network.slowSubmitConnectCount
      echo "fabric-submit slow-connect self=", peerIdString(network.switch.peerInfo.peerId),
        " peer=", peerIdText,
        " lane=", submitLaneText(currentBatch.logicalLane),
        " kinds=", submitBatchKindsText(currentBatch.items),
        " item=", currentBatch.diagItemKey,
        " items=", currentBatch.items.len,
        " elapsedMs=", connectElapsedMs
    if stream.isNil or stream.closed or stream.atEof:
      when defined(fabric_submit_diag) or defined(fabric_lsmr_diag):
        echo "fabric-submit stream-invalid self=",
          peerIdString(network.switch.peerInfo.peerId),
          " peer=", peerIdText,
          " lane=", submitLaneText(currentBatch.logicalLane),
          " kinds=", submitBatchKindsText(currentBatch.items),
          " item=", currentBatch.diagItemKey,
          " items=", currentBatch.items.len,
          " nil=", stream.isNil,
          " closed=", (not stream.isNil and stream.closed),
          " eof=", (not stream.isNil and stream.atEof)
      dropSubmitStream("stream-invalid")
      markSubmitFailure("stream-invalid")
      return

    while true:
      while inflightBatches.len < pipelineWindow:
        let nextBatch =
          if hasQueuedBatch:
            hasQueuedBatch = false
            queuedBatch
          elif stickyDrain and network.submitTransportDispatchable(
              peerId, submitSessionClassLane(currentBatch.transportClass)
          ):
            await popQueuedBatch()
          else:
            FabricSubmitRpcBatch()
        if nextBatch.requests.len == 0 or nextBatch.items.len == 0 or nextBatch.encoded.len == 0:
          break
        if not await writeBatch(nextBatch):
          return

      if inflightBatches.len == 0:
        break

      let ackTarget = inflightBatches.peekFirst()
      currentBatch = ackTarget.batch
      ackReadStartedAtMs = diagNowMs()
      when defined(fabric_submit_diag) or defined(fabric_lsmr_diag):
        echo "t=", ackReadStartedAtMs,
          " fabric-submit ack-read-begin self=",
          peerIdString(network.switch.peerInfo.peerId),
          " peer=", peerIdText,
          " lane=", submitLaneText(currentBatch.logicalLane),
          " kinds=", submitBatchKindsText(currentBatch.items),
          " item=", currentBatch.diagItemKey,
          " items=", currentBatch.items.len
      let readFuture = stream.readLp(SubmitAckMaxBytes)
      let completed = await withTimeout(readFuture, FabricSubmitAckTimeout)
      if not completed:
        if not readFuture.finished():
          readFuture.cancelSoon()
        dropSubmitStream("ack-timeout")
        currentBatch = inflightBatches.popFirst().batch
        when defined(fabric_submit_diag) or defined(fabric_lsmr_diag):
          echo "fabric-submit ack-timeout self=",
            peerIdString(network.switch.peerInfo.peerId),
            " peer=", peerIdText,
            " lane=", submitLaneText(currentBatch.logicalLane),
            " kinds=", submitBatchKindsText(currentBatch.items),
            " item=", currentBatch.diagItemKey,
            " items=", currentBatch.items.len,
            " timeoutMs=", FabricSubmitAckTimeout.milliseconds
        markSubmitFailure("ack-timeout")
        failOutstandingBatches()
        return

      var ack: seq[byte] = @[]
      try:
        ack = readFuture.read()
      except CatchableError as exc:
        dropSubmitStream("ack-read-fail")
        currentBatch = inflightBatches.popFirst().batch
        when defined(fabric_submit_diag) or defined(fabric_lsmr_diag):
          echo "fabric-submit ack-read-fail self=",
            peerIdString(network.switch.peerInfo.peerId),
            " peer=", peerIdText,
            " lane=", submitLaneText(currentBatch.logicalLane),
            " kinds=", submitBatchKindsText(currentBatch.items),
            " item=", currentBatch.diagItemKey,
            " items=", currentBatch.items.len,
            " err=", exc.msg
        markSubmitFailure("ack-read-fail")
        failOutstandingBatches()
        return
      let ackEntry = inflightBatches.popFirst()
      currentBatch = ackEntry.batch
      when defined(fabric_submit_diag) or defined(fabric_lsmr_diag):
        echo "t=", diagNowMs(),
          " fabric-submit ack-read-done self=",
          peerIdString(network.switch.peerInfo.peerId),
          " peer=", peerIdText,
          " lane=", submitLaneText(currentBatch.logicalLane),
          " kinds=", submitBatchKindsText(currentBatch.items),
          " item=", currentBatch.diagItemKey,
          " items=", currentBatch.items.len,
          " bytes=", ack.len,
          " elapsedMs=", diagNowMs() - ackReadStartedAtMs

      let ackElapsedMs = diagNowMs() - ackEntry.writtenAtMs
      network.lastSubmitAckElapsedMs = ackElapsedMs
      if ackElapsedMs > network.maxSubmitAckElapsedMs:
        network.maxSubmitAckElapsedMs = ackElapsedMs
      if ackElapsedMs >= FabricSlowSubmitAckMs:
        inc network.slowSubmitAckCount
        echo "t=", diagNowMs(),
          " fabric-submit slow-ack self=", peerIdString(network.switch.peerInfo.peerId),
          " peer=", peerIdText,
          " lane=", submitLaneText(currentBatch.logicalLane),
          " kinds=", submitBatchKindsText(currentBatch.items),
          " item=", currentBatch.diagItemKey,
          " items=", currentBatch.items.len,
          " elapsedMs=", ackElapsedMs

      if ack.len == 0:
        when defined(fabric_submit_diag) or defined(fabric_lsmr_diag):
          echo "fabric-submit ack-empty self=",
            peerIdString(network.switch.peerInfo.peerId),
            " peer=", peerIdText,
            " lane=", submitLaneText(currentBatch.logicalLane),
            " kinds=", submitBatchKindsText(currentBatch.items),
            " item=", currentBatch.diagItemKey,
            " items=", currentBatch.items.len
        dropSubmitStream("ack-empty")
        markSubmitFailure("ack-empty")
        failOutstandingBatches()
        return

      let ackFrame = safeDecode[FabricSubmitAckFrame](stringOf(ack))
      if ackFrame.isNone():
        when defined(fabric_submit_diag) or defined(fabric_lsmr_diag):
          echo "fabric-submit ack-decode-fail self=",
            peerIdString(network.switch.peerInfo.peerId),
            " peer=", peerIdText,
            " lane=", submitLaneText(currentBatch.logicalLane),
            " kinds=", submitBatchKindsText(currentBatch.items),
            " item=", currentBatch.diagItemKey,
            " items=", currentBatch.items.len,
            " bytes=", ack.len
        dropSubmitStream("ack-decode-fail")
        inc network.submitWriteFailureCount
        markSubmitFailure("ack-decode-fail")
        failOutstandingBatches()
        return
      if ackFrame.get().requestId != currentBatch.requestId:
        when defined(fabric_submit_diag) or defined(fabric_lsmr_diag):
          echo "fabric-submit ack-request-mismatch self=",
            peerIdString(network.switch.peerInfo.peerId),
            " peer=", peerIdText,
            " lane=", submitLaneText(currentBatch.logicalLane),
            " item=", currentBatch.diagItemKey,
            " expected=", currentBatch.requestId,
            " actual=", ackFrame.get().requestId
        dropSubmitStream("ack-request-mismatch")
        inc network.submitWriteFailureCount
        markSubmitFailure("ack-request-mismatch")
        failOutstandingBatches()
        return

      let decodedAck = decodeSubmitAckResults(currentBatch.items, ackFrame.get())
      if not decodedAck.ok:
        when defined(fabric_submit_diag) or defined(fabric_lsmr_diag):
          echo "fabric-submit ack-invalid self=",
            peerIdString(network.switch.peerInfo.peerId),
            " peer=", peerIdText,
            " lane=", submitLaneText(currentBatch.logicalLane),
            " kinds=", submitBatchKindsText(currentBatch.items),
            " item=", currentBatch.diagItemKey,
            " items=", currentBatch.items.len,
            " ackItems=", ackFrame.get().items.len
        dropSubmitStream("ack-invalid")
        inc network.submitWriteFailureCount
        markSubmitFailure("ack-invalid")
        failOutstandingBatches()
        return

      currentBatch.completeSubmitBatch(decodedAck.accepted)
      if not stickyDrain and inflightBatches.len == 0:
        break
  except CancelledError:
    if reservedSlot >= 0 and not stream.isNil:
      dropSubmitStream("rpc-cancelled")
    discard
  finally:
    if reservedSlot >= 0:
      network.releaseSubmitSessionStream(
        peerId,
        currentBatch.transportClass,
        reservedSlot,
        stream,
      )
      reservedSlot = -1
      stream = nil
    var shouldSchedule = false
    var inflightAfter = 0
    var queuedAfter = 0
    await session.lock.acquire()
    try:
      if session.inflightCount > 0:
        dec session.inflightCount
      drainedInflight = session.inflightCount == 0
      inflightAfter = session.inflightCount
      queuedAfter = session.submitPendingLen()
    finally:
      try:
        if session.lock.locked():
          session.lock.release()
      except AsyncLockError:
        discard
    shouldSchedule = queuedAfter > 0
    when defined(fabric_submit_diag) or defined(fabric_lsmr_diag):
      echo "fabric-submit rpc-exit self=",
        peerIdString(network.switch.peerInfo.peerId),
        " peer=", peerIdText,
        " lane=", submitLaneText(currentBatch.logicalLane),
        " item=", currentBatch.diagItemKey,
        " inflight=", inflightAfter,
        " queued=", queuedAfter,
        " forceRepair=", forceRepair
    if forceRepair:
      network.schedulePendingSubmitRepair(peerId, peerIdText)
    if drainedInflight:
      network.schedulePendingSubmitRepair(peerId, peerIdText)
    if shouldSchedule:
      await network.signalSubmitSessionDispatch(
        peerId,
        peerIdText,
        session.transportClass,
        session.streamSlot,
      )

proc signalSubmitSessionDispatch(
    network: FabricNetwork,
    peerId: PeerId,
    peerIdText: string,
    sessionClass: FabricSubmitSessionClass,
    sessionShard = 0,
): Future[void] {.async: (raises: [CancelledError]).} =
  if network.isNil or peerId.data.len == 0:
    return
  let session = network.getOrCreateSubmitSession(peerId, sessionClass, sessionShard)
  if session.isNil:
    return
  var queued = 0
  var runnerAlive = false
  await session.lock.acquire()
  try:
    queued = session.submitPendingLen()
    if queued > 0:
      session.dispatchPending = true
    runnerAlive =
      not session.dispatchRunner.isNil and
      not session.dispatchRunner.finished()
  finally:
    try:
      if session.lock.locked():
        session.lock.release()
    except AsyncLockError:
      discard
  if queued == 0:
    return
  if runnerAlive:
    if not session.dispatchEvent.isNil:
      session.dispatchEvent.fire()
    return
  await network.dispatchSubmitSessionDirect(
    peerId,
    peerIdText,
    sessionClass,
    sessionShard,
  )

proc dispatchSubmitSessionDirect(
    network: FabricNetwork,
    peerId: PeerId,
    peerIdText: string,
    sessionClass: FabricSubmitSessionClass,
    sessionShard = 0,
): Future[void] {.async: (raises: [CancelledError]).} =
  if network.isNil or peerId.data.len == 0:
    return
  let lane = submitSessionClassLane(sessionClass)
  let session = network.getOrCreateSubmitSession(peerId, sessionClass, sessionShard)
  let state = network.getOrCreateSubmitPeerConnect(peerId)
  if session.isNil:
    return
  var queued = 0
  var inflight = 0
  await session.lock.acquire()
  try:
    queued = session.submitPendingLen()
    inflight = session.inflightCount
    if queued > 0:
      session.dispatchPending = true
  finally:
    try:
      if session.lock.locked():
        session.lock.release()
    except AsyncLockError:
      discard
  if queued == 0:
    when defined(fabric_submit_diag) or defined(fabric_lsmr_diag):
      echo "fabric-submit dispatch-stop self=",
        (if network.switch.isNil: "" else: peerIdString(network.switch.peerInfo.peerId)),
        " peer=", peerIdText,
        " reason=no-pending"
    return
  if not network.submitTransportDispatchable(peerId, lane):
    if network.submitHotSessionNeedsDirectProbe(peerId, sessionClass):
      discard await network.probeSubmitTransport(peerId)
    if network.submitTransportDispatchable(peerId, lane):
      discard
    else:
      network.scheduleSubmitSession(
        peerId,
        peerIdText,
        sessionClass,
        sessionShard,
      )
      when defined(fabric_submit_diag) or defined(fabric_lsmr_diag):
        let state = network.submitPeerConnects.getOrDefault($peerId)
        echo "fabric-submit dispatch-stop self=",
          (if network.switch.isNil: "" else: peerIdString(network.switch.peerInfo.peerId)),
          " peer=", peerIdText,
          " reason=not-dispatchable",
          " connected=", network.switch.isConnected(peerId),
          " blocked=", (if state.isNil: false else: state.transportBlocked),
          " inflight=", inflight,
          " queued=", queued
      return
  if not session.dispatchRunner.isNil and not session.dispatchRunner.finished():
    if not session.dispatchEvent.isNil:
      session.dispatchEvent.fire()
    when defined(fabric_submit_diag) or defined(fabric_lsmr_diag):
      echo "fabric-submit dispatch-stop self=",
        (if network.switch.isNil: "" else: peerIdString(network.switch.peerInfo.peerId)),
        " peer=", peerIdText,
        " reason=runner-alive",
        " inflight=", inflight,
        " queued=", queued
    return

  proc launch() {.async, gcsafe.} =
    while not network.isNil:
      var shouldWait = false
      await session.lock.acquire()
      try:
        if session.dispatchPending:
          session.dispatchPending = false
        else:
          shouldWait = true
      finally:
        try:
          if session.lock.locked():
            session.lock.release()
        except AsyncLockError:
          discard
      if shouldWait:
        if session.dispatchEvent.isNil:
          return
        await session.dispatchEvent.wait()
        session.dispatchEvent.clear()
        continue
      while not network.isNil:
        var launchBatch: FabricSubmitRpcBatch
        var hasBatch = false
        var dispatchReason = "none"
        var dispatchQueued = 0
        var dispatchInflight = 0
        await session.lock.acquire()
        try:
          if session.inflightCount >= session.submitMaxInflight():
            dispatchReason = "inflight-full"
          elif not session.submitHasPending():
            dispatchReason = "drained"
          elif not network.submitTransportDispatchable(peerId, lane):
            dispatchReason = "not-dispatchable"
          else:
            let batch = session.popNextSubmitRpcBatch()
            if batch.requests.len == 0 or batch.items.len == 0 or batch.encoded.len == 0:
              dispatchReason = "no-batch"
            else:
              inc session.inflightCount
              launchBatch = batch
              hasBatch = true
          dispatchQueued = session.submitPendingLen()
          dispatchInflight = session.inflightCount
        finally:
          try:
            if session.lock.locked():
              session.lock.release()
          except AsyncLockError:
            discard

        if not hasBatch:
          if dispatchReason == "not-dispatchable" and
              network.submitHotSessionNeedsDirectProbe(peerId, sessionClass):
            let repaired = await network.probeSubmitTransport(peerId)
            if repaired and network.submitTransportDispatchable(peerId, lane):
              continue
          if dispatchReason == "not-dispatchable":
            network.scheduleSubmitSession(
              peerId,
              peerIdText,
              sessionClass,
              sessionShard,
            )
          when defined(fabric_submit_diag) or defined(fabric_lsmr_diag):
            let state = network.submitPeerConnects.getOrDefault($peerId)
            echo "fabric-submit dispatch-stop self=",
              (if network.switch.isNil: "" else: peerIdString(network.switch.peerInfo.peerId)),
              " peer=", peerIdText,
              " reason=", dispatchReason,
              " connected=", network.switch.isConnected(peerId),
              " blocked=", (if state.isNil: false else: state.transportBlocked),
              " inflight=", dispatchInflight,
              " queued=", dispatchQueued
          break
        when defined(fabric_submit_diag) or defined(fabric_lsmr_diag):
          echo "fabric-submit dispatch-launch self=",
            (if network.switch.isNil: "" else: peerIdString(network.switch.peerInfo.peerId)),
            " peer=", peerIdText,
            " batches=1",
            " inflight=", dispatchInflight,
            " queued=", dispatchQueued

        try:
          await network.runSubmitRequestRpc(peerId, peerIdText, launchBatch)
        except CancelledError:
          return
        continue

  session.dispatchRunner = launch()
  asyncSpawn session.dispatchRunner
  if not session.dispatchEvent.isNil:
    session.dispatchEvent.fire()

proc scheduleSubmitSession(
    network: FabricNetwork,
    peerId: PeerId,
    peerIdText: string,
    sessionClass: FabricSubmitSessionClass,
    sessionShard = 0,
) {.gcsafe, raises: [].} =
  discard peerIdText
  if network.isNil or peerId.data.len == 0:
    return
  network.scheduleSubmitPeerCommand(peerId, FabricSubmitPeerCommand(
    kind: fspckScheduleSession,
    lane: submitSessionClassLane(sessionClass),
    sessionClass: sessionClass,
    sessionShard: sessionShard,
    incoming: false,
  ))

proc scheduleSubmitSession(
    network: FabricNetwork,
    peerId: PeerId,
    peerIdText: string,
    lane: FabricSubmitLane,
) {.gcsafe, raises: [].} =
  case lane
  of fslEvent:
    network.scheduleSubmitSession(peerId, peerIdText, fsscEvent, 0)
  of fslAttestation:
    network.scheduleSubmitSession(peerId, peerIdText, fsscAttestation, 0)
  of fslOther:
    for shardIdx in 0 ..< submitSessionShardCount(fsscEventCertificate):
      network.scheduleSubmitSession(peerId, peerIdText, fsscEventCertificate, shardIdx)
    network.scheduleSubmitSession(peerId, peerIdText, fsscControl, 0)

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
  let lsmrSvc = getLsmrService(network.switch)
  if not lsmrSvc.isNil:
    let lsmrStatus = lsmrSvc.routingPlaneStatus()
    result.lsmrMinWitnessQuorum = lsmrStatus.lsmrMinWitnessQuorum
    result.lsmrWitnessRequests = lsmrStatus.lsmrWitnessRequests
    result.lsmrWitnessSuccess = lsmrStatus.lsmrWitnessSuccess
    result.lsmrWitnessQuorumFailure = lsmrStatus.lsmrWitnessQuorumFailure
    result.lsmrKnownSyncPeers = lsmrStatus.lsmrKnownSyncPeers
    result.lsmrDialableSyncPeers = lsmrStatus.lsmrDialableSyncPeers
    result.lsmrOverlayDesiredPeers = lsmrStatus.lsmrOverlayDesiredPeers
    result.lsmrLocalCertReady = lsmrStatus.lsmrLocalCertReady
    result.lsmrLastRefreshReason = lsmrStatus.lsmrLastRefreshReason
    result.lsmrUndialableSyncPeers = lsmrStatus.lsmrUndialableSyncPeers
    result.lsmrOverlayDesiredPeerIds = lsmrStatus.lsmrOverlayDesiredPeerIds

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
    submitHotAcceptIdleHook: proc() {.gcsafe, raises: [].} = nil,
): FabricNetwork =
  if routingMode != RoutingPlaneMode.legacyOnly and lsmrConfig.isNone():
    raise newException(ValueError, "lsmr routing requires lsmrConfig")
  let addresses = parseListenAddrs(listenAddrs)
  var builder = newStandardSwitchBuilder(
    addrs = addresses,
    secureManagers = [SecureProtocol.Noise],
    maxConnsPerPeer = 2,
    transportFlags = {ServerFlags.TcpNoDelay},
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
    submitPeerConnects: initTable[string, FabricSubmitPeerConnect](),
    submitPeerAddrHints: initTable[string, seq[MultiAddress]](),
    controlFetchSessions: initTable[string, FabricControlFetchSession](),
    controlFetchResponseLocks: initTable[string, AsyncLock](),
    controlFetchResponseFlights: initTable[string, Future[FetchResponse]](),
    stopping: false,
    submitWarmRunner: nil,
    submitWarmPending: false,
    submitWarmDelayRunner: nil,
    submitWarmGeneration: 0'u64,
    submitHotInbox: initDeque[FabricSubmitInboxItem](),
    submitInbox: initDeque[FabricSubmitInboxItem](),
    submitInboxPending: false,
    submitAcceptEpoch: 0'u64,
    submitHotDrainActive: false,
    submitHotAckYieldActive: 0,
    submitHotResumePending: false,
    submitHotBackgroundResumePending: false,
    submitHotAcceptIdleEvent: newAsyncEvent(),
    submitHotAcceptIdleHook: submitHotAcceptIdleHook,
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
      await network.eventHandler(network, decodeObj[FabricEvent](stringOf(data)), @[], ""))
  network.gossip.subscribe(AttestationTopic, proc(topic: string, data: seq[byte]) {.async.} =
    if not network.attestationHandler.isNil:
      await network.attestationHandler(network, decodeObj[EventAttestation](stringOf(data)), @[], ""))
  network.gossip.subscribe(EventCertificateTopic, proc(topic: string, data: seq[byte]) {.async.} =
    if not network.eventCertificateHandler.isNil:
      await network.eventCertificateHandler(network, decodeObj[EventCertificate](stringOf(data)), @[], ""))
  network.gossip.subscribe(CheckpointCandidateTopic, proc(topic: string, data: seq[byte]) {.async.} =
    if not network.checkpointCandidateHandler.isNil:
      await network.checkpointCandidateHandler(network, decodeObj[CheckpointCandidate](stringOf(data)), ""))
  network.gossip.subscribe(CheckpointVoteTopic, proc(topic: string, data: seq[byte]) {.async.} =
    if not network.checkpointVoteHandler.isNil:
      await network.checkpointVoteHandler(network, decodeObj[CheckpointVote](stringOf(data)), ""))
  network.gossip.subscribe(CheckpointCertificateTopic, proc(topic: string, data: seq[byte]) {.async.} =
    if not network.checkpointBundleHandler.isNil:
      await network.checkpointBundleHandler(network, decodeObj[CheckpointBundle](stringOf(data)), ""))
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
      let state = network.getOrCreateSubmitPeerConnect(peerId)
      if event.kind == ConnEventKind.Connected:
        if not state.isNil:
          await network.enqueueSubmitPeerCommand(peerId, FabricSubmitPeerCommand(
            kind: fspckConnConnected,
            lane: fslEvent,
            sessionClass: fsscEvent,
            sessionShard: 0,
            incoming: event.incoming,
          ))
        network.scheduleWarmSubmitConnections()
        return
      if event.kind == ConnEventKind.Disconnected:
        if not state.isNil:
          await network.enqueueSubmitPeerCommand(peerId, FabricSubmitPeerCommand(
            kind: fspckConnDisconnected,
            lane: fslEvent,
            sessionClass: fsscEvent,
            sessionShard: 0,
            incoming: false,
          ))
    network.switch.addConnEventHandler(
      network.submitConnEventHandler,
      ConnEventKind.Connected,
    )
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
  if network.isNil:
    return
  await network.prepareStop()
  if not network.switch.isNil:
    if not network.submitConnEventHandler.isNil:
      network.switch.removeConnEventHandler(
        network.submitConnEventHandler,
        ConnEventKind.Connected,
      )
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

proc prepareStop*(network: FabricNetwork) {.async.} =
  if network.isNil or network.stopping:
    return
  network.stopping = true
  network.submitWarmPending = false
  for session in network.controlFetchSessions.values():
    if session.isNil or session.lock.isNil:
      continue
    await session.lock.acquire()
    try:
      await session.closeControlFetchConn("network-prepare-stop")
    finally:
      try:
        if session.lock.locked():
          session.lock.release()
      except AsyncLockError:
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
  let transportLane = network.submitTransportLane(peerId, lane)
  let maxItems = submitBatchItemLimit(lane, transportLane, items)
  let chunks = splitSubmitFrameChunks(
    items,
    maxItems = maxItems,
  )
  if chunks.len == 0:
    return

  type SubmitChunkPlan = object
    items: seq[FabricSubmitEnvelope]
    encoded: seq[byte]
    transportClass: FabricSubmitSessionClass
    transportLane: FabricSubmitLane
    sessionShard: int

  var plans: seq[SubmitChunkPlan] = @[]
  for idx, chunk in chunks:
    let encoded = encodeSubmitFrame(0, chunk)
    if encoded.isNone():
      return
    let chunkTransportClass = submitSessionClassForItems(lane, chunk)
    let chunkTransportLane = submitSessionClassLane(chunkTransportClass)
    plans.add(SubmitChunkPlan(
      items: chunk,
      encoded: bytesOf(encoded.get()),
      transportClass: chunkTransportClass,
      transportLane: chunkTransportLane,
      sessionShard: submitRequestShard(
        chunkTransportClass,
        diagItemKey,
        chunk,
      ),
    ))
    when defined(fabric_submit_diag):
      if chunks.len > 1:
        echo "fabric-submit chunk self=", peerIdString(network.switch.peerInfo.peerId),
          " peer=", peerIdText,
          " lane=", submitLaneText(lane),
          " via=", submitLaneText(chunkTransportLane),
          " chunk=", idx + 1,
          "/",
          chunks.len,
          " item=", diagItemKey,
          " items=", chunk.len,
          " bytes=", bytesOf(encoded.get()).len
  if not network.switch.isConnected(peerId):
    network.ensureSubmitPeerConnectivity(peerIdText, lane)
    when defined(fabric_submit_diag) or defined(fabric_lsmr_diag):
      let firstTransportLane =
        if plans.len > 0: plans[0].transportLane
        else: transportLane
      echo "t=", diagNowMs(),
        " fabric-submit not-ready self=", peerIdString(network.switch.peerInfo.peerId),
        " peer=", peerIdText,
        " lane=", submitLaneText(lane),
        " via=", submitLaneText(firstTransportLane),
        " item=", diagItemKey,
        " items=", items.len
    return

  var requests: seq[FabricSubmitRequest] = @[]
  var sessions: seq[FabricSubmitSession] = @[]
  for plan in plans:
    let request = buildSubmitRequest(
      lane,
      plan.items,
      diagItemKey,
      plan.encoded,
    )
    if request.isNil:
      for queued in requests:
        if not queued.isNil:
          queued.completeSubmitRequest(@[])
      return
    let session = network.getOrCreateSubmitSession(
      peerId,
      plan.transportClass,
      plan.sessionShard,
    )
    if session.isNil:
      for queued in requests:
        if not queued.isNil:
          queued.completeSubmitRequest(@[])
      request.completeSubmitRequest(@[])
      return
    request.sessionShard = plan.sessionShard
    requests.add(request)
    sessions.add(session)
  if requests.len == 0:
    return
  var scheduledShards: array[FabricSubmitSessionClass, uint32]
  for idx, request in requests:
    let session = sessions[idx]
    session.enqueueSubmitRequest(request)
    scheduledShards[request.transportClass] =
      scheduledShards[request.transportClass] or
      submitSessionShardBit(request.sessionShard)
  for sessionClass in FabricSubmitSessionClass:
    let shardBits = scheduledShards[sessionClass]
    if shardBits == 0:
      continue
    for shardIdx in 0 ..< submitSessionShardCount(sessionClass):
      if (shardBits and submitSessionShardBit(shardIdx)) == 0:
        continue
      let lane = submitSessionClassLane(sessionClass)
      if not network.submitTransportDispatchable(peerId, lane) and
          network.submitHotSessionNeedsDirectProbe(peerId, sessionClass):
        try:
          discard await network.probeSubmitTransport(peerId)
        except CancelledError:
          discard
      if network.submitTransportDispatchable(peerId, lane):
        try:
          await network.signalSubmitSessionDispatch(
            peerId,
            peerIdText,
            sessionClass,
            shardIdx,
          )
        except CancelledError:
          discard
      else:
        network.scheduleSubmitSession(
          peerId,
          peerIdText,
          sessionClass,
          shardIdx,
        )

  for idx, request in requests:
    let chunkAcked =
      try:
        await request.completion
      except CancelledError:
        @[]
      except CatchableError:
        @[]
    for accepted in chunkAcked:
      result.add(accepted)
    if chunkAcked.len != request.items.len or
        acceptedAckCount(chunkAcked) != request.items.len:
      break

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

proc fetchControlRaw(
    network: FabricNetwork,
    peerId: PeerId,
    key: string,
    timeout: Duration,
    maxAttempts: int,
): Future[FetchResponse] {.async: (raises: [CancelledError, FetchError]).} =
  if network.isNil or network.switch.isNil or network.stopping:
    raise (ref FetchError)(
      msg: "fabric control fetch network unavailable", kind: FetchErrorKind.feConfig
    )
  if network.controlFetchMustYieldToLive(key):
    raise (ref FetchError)(
      msg: "fabric control fetch requester live-inflight busy",
      kind: FetchErrorKind.feConfig,
    )
  let session = network.getOrCreateControlFetchSession(
    peerId,
    controlFetchSessionClass(key),
  )
  if session.isNil:
    raise (ref FetchError)(
      msg: "fabric control fetch session unavailable", kind: FetchErrorKind.feConfig
    )
  if network.controlFetchTargetSummaryMustYield(peerId, key):
    raise (ref FetchError)(
      msg: "fabric control fetch live-inflight busy",
      kind: FetchErrorKind.feConfig,
    )
  if network.controlFetchSharedLiveSummaryMustYield(peerId, key):
    raise (ref FetchError)(
      msg: "fabric control fetch shared-live-plane busy",
      kind: FetchErrorKind.feConfig,
    )

  var attempt = 0
  var lastErr: ref FetchError
  while attempt < maxAttempts:
    inc attempt
    await session.lock.acquire()
    try:
      if not session.conn.isNil:
        await session.closeControlFetchConn("single-shot")
      let conn = await network.openControlFetchConn(peerId)
      try:
        if network.controlFetchMustYieldToLive(key):
          raise (ref FetchError)(
            msg: "fabric control fetch requester live-inflight busy",
            kind: FetchErrorKind.feConfig,
          )
        if network.controlFetchTargetSummaryMustYield(peerId, key):
          raise (ref FetchError)(
            msg: "fabric control fetch live-inflight busy",
            kind: FetchErrorKind.feConfig,
          )
        if network.controlFetchSharedLiveSummaryMustYield(peerId, key):
          raise (ref FetchError)(
            msg: "fabric control fetch shared-live-plane busy",
            kind: FetchErrorKind.feConfig,
          )
        return await fetchOnConn(
          conn,
          key,
          timeout,
          DefaultFetchMessageSize,
        )
      finally:
        await closeSubmitConn(conn)
    except CancelledError as exc:
      raise exc
    except FetchError as err:
      lastErr = err
    finally:
      try:
        if session.lock.locked():
          session.lock.release()
      except AsyncLockError:
        discard

    if lastErr.isNil or attempt >= maxAttempts:
      break

  if lastErr.isNil:
    raise (ref FetchError)(
      msg: "fabric control fetch failed without error", kind: FetchErrorKind.feConfig
    )
  raise lastErr

proc fetchRaw*(
    network: FabricNetwork,
    peerIdText: string,
    key: string,
    timeout: Duration = 1.seconds,
    maxAttempts = 1,
): Future[Option[seq[byte]]] {.async: (raises: []).} =
  if network.isNil or network.stopping:
    return none(seq[byte])
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
    let attempts = max(1, maxAttempts)
    let response =
      if key.isControlFetchKey():
        await network.fetchControlRaw(
          peerId,
          key,
          timeout,
          attempts,
        )
      else:
        await fetch(
          network.switch,
          peerId,
          key,
          timeout = timeout,
          maxAttempts = attempts,
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
