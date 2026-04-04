import std/[algorithm, deques, heapqueue, json, jsonutils, options, os, sequtils, sets, strutils, tables]
from std/times import epochTime

import chronos

import ../lsmr
import ../multiaddress
import ../peerinfo
import ../peerstore
import ../services/lsmrservice
import ../switch
import ../rwad/codec
import ../rwad/execution/[content, name, staking, state, treasury]
import ../rwad/identity
import ../rwad/types
import ./net/service
import ./polar
import ./storage/store
import ./adapter
import ./codec
import ./types

const
  FabricWorkBatchLimit = 2
  FabricInboundDrainBatchLimit = FabricWorkBatchLimit
  FabricMaintenanceBatchLimit = FabricWorkBatchLimit
  FabricInboundSliceBudgetMs = 5'i64
  FabricMaintenanceSliceBudgetMs = 10'i64
  FabricRoutingSnapshotSyncTimeout = 2.seconds
  FabricRoutingSnapshotResyncMs = 2_000'i64
  FabricInboundDrainYield = 1.milliseconds
  FabricOutboundYield = 0.seconds
  FabricMaintenanceYield = 1.milliseconds
  FabricSubmitPumpPollLimit = 5
  FabricAntiEntropySummaryResyncMs = 1_000'i64
  FabricSlowInboundSliceMs = 500'i64
  FabricSlowMaintenanceSliceMs = 500'i64
  FabricSlowCheckpointAdvanceMs = 500'i64

type
  InboundMessageKind = enum
    imPeerAnnouncement
    imEvent
    imAttestation
    imAntiEntropyPull
    imEventCertificate
    imCheckpointCandidate
    imCheckpointVote
    imCheckpointBundle
    imAvoProposal
    imAvoApproval

  InboundMessage = object
    relayScope: LsmrPath
    sourcePeerId: string
    case kind: InboundMessageKind
    of imPeerAnnouncement:
      peerAnnouncement: PeerAnnouncement
    of imEvent:
      event: FabricEvent
    of imAttestation:
      attestation: EventAttestation
    of imAntiEntropyPull:
      antiEntropyPullItem: AntiEntropyPullWorkItem
      antiEntropyPullPayload: seq[byte]
    of imEventCertificate:
      eventCertificate: EventCertificate
    of imCheckpointCandidate:
      checkpointCandidate: CheckpointCandidate
    of imCheckpointVote:
      checkpointVote: CheckpointVote
    of imCheckpointBundle:
      checkpointBundle: CheckpointBundle
    of imAvoProposal:
      avoProposal: AvoProposal
    of imAvoApproval:
      proposalId: string
      validator: string

  OutboundSubmitPriority = enum
    ospEvent
    ospAttestation
    ospOther

  OutboundSubmitKind = enum
    oskProc
    oskEvent
    oskAttestation
    oskEventCertificate

  OutboundSubmit = ref object
    itemKey: string
    targetPeerId: string
    priority: OutboundSubmitPriority
    scopePrefix: LsmrPath
    case submitKind: OutboundSubmitKind
    of oskProc:
      submit: proc(): Future[bool] {.closure, gcsafe, raises: [].}
    of oskEvent:
      event: FabricEvent
    of oskAttestation:
      attestation: EventAttestation
    of oskEventCertificate:
      eventCertificate: EventCertificate
    onSuccess: proc() {.closure, gcsafe, raises: [].}
    onFailure: proc() {.closure, gcsafe, raises: [].}

  OutboundPeerSession = ref object
    eventQueue: Deque[OutboundSubmit]
    attestationQueue: Deque[OutboundSubmit]
    otherQueue: Deque[OutboundSubmit]
    runners: array[OutboundSubmitPriority, seq[Future[void]]]
    blockedOnReady: array[OutboundSubmitPriority, bool]

  DeliveryLedger = object
    remainingTargets: HashSet[string]
    inflightTargets: HashSet[string]
    deliveredTargets: HashSet[string]

  PendingEventCertificateTicket = object
    eventId: string
    clock: GanzhiClock
    known: bool
    epoch: uint64

  PendingCertificationEventTicket = object
    eventId: string
    clock: GanzhiClock
    known: bool
    epoch: uint64

  AuthorGatherStatus = enum
    agsWaitingParticipants
    agsWaitingWitnesses
    agsCertReady
    agsCertified
    agsRetired

  AntiEntropyDomain = enum
    aedEvent
    aedAttParticipant
    aedAttWitness
    aedEventCert
    aedCheckpointCandidate
    aedCheckpointVote
    aedCheckpointBundle

  AntiEntropyPullWorkItem = object
    domain: AntiEntropyDomain
    key: string
    causedByEventId: string
    targetPeerId: string
    clock: GanzhiClock

  EraCheckpointCandidatePhase = enum
    ecpPending
    ecpReady
    ecpPublished
    ecpCertified

  EraCheckpointVotePhase = enum
    evpPending
    evpCollecting
    evpQuorum

  EraCheckpointBundlePhase = enum
    ebpPending
    ebpPublished
    ebpCompleted

  EraCheckpointState = object
    eventTotal: int
    isolatedTotal: int
    certifiedTotal: int
    candidatePhase: EraCheckpointCandidatePhase
    votePhase: EraCheckpointVotePhase
    bundlePhase: EraCheckpointBundlePhase
    candidateId: string
    voteBitmap: HashSet[string]
    bundleTargetsRemaining: HashSet[string]
    firstMissingEventId: string
    minTick: int
    maxTick: int

  AntiEntropyEraDigest = object
    era: uint64
    digest: string

  AntiEntropyTickDigest = object
    tick: uint64
    digest: string

  AntiEntropyDomainRootSummary = object
    domain: string
    maxClock: GanzhiClock
    eras: seq[AntiEntropyEraDigest]

  AntiEntropyRootSummary = object
    domains: seq[AntiEntropyDomainRootSummary]

  AntiEntropyEraSummary = object
    domain: string
    era: uint64
    maxClock: GanzhiClock
    ticks: seq[AntiEntropyTickDigest]

  AntiEntropyTickSummary = object
    domain: string
    era: uint64
    tick: uint64
    items: seq[string]

  EventCertificationProgress = object
    clock: GanzhiClock
    participantAttestations: Table[string, EventAttestation]
    witnessAttestations: Table[string, EventAttestation]
    witnessAccounts: HashSet[string]
    witnessPeerIds: HashSet[string]
    missingWitnessAccounts: HashSet[string]
    witnessRoutes: Table[string, RoutingPeer]
    reachableWitnesses: int
    witnessThreshold: int
    status: AuthorGatherStatus

  RelayPlan = tuple[route: RoutingPeer, scope: LsmrPath]

  ReplayStateCacheEntry = object
    digest: string
    snapshot: ChainStateSnapshot

  FabricNode* = ref object
    config*: FabricNodeConfig
    identity*: NodeIdentity
    genesis*: GenesisSpec
    peers*: OrderedTable[string, FabricNode]
    routingPeers*: OrderedTable[string, RoutingPeer]
    routingAnnouncements*: OrderedTable[string, PeerAnnouncement]
    routeAnnouncementVersion: uint64
    routeAnnouncementDigest: string
    routeAnnouncementCreatedAtMs: int64
    store*: FabricStore
    network*: FabricNetwork
    running*: bool
    events*: OrderedTable[string, FabricEvent]
    eventAttestations*: Table[string, seq[EventAttestation]]
    attestationsByKey*: Table[string, EventAttestation]
    eventCertificates*: OrderedTable[string, EventCertificate]
    eventCertificationProgress*: Table[string, EventCertificationProgress]
    pendingCertificationEvents*: HashSet[string]
    pendingCertificationEventFrontier*: HeapQueue[PendingCertificationEventTicket]
    pendingCertificationEventEpochs*: Table[string, uint64]
    pendingCertificationEventNextEpoch: uint64
    pendingWitnessPullsByPeer*: Table[string, Deque[AntiEntropyPullWorkItem]]
    pendingWitnessPullKeysByPeer: Table[string, HashSet[string]]
    inflightWitnessPullByPeer*: Table[string, AntiEntropyPullWorkItem]
    witnessPullRunners: Table[string, Future[void]]
    antiEntropySummaryRunners: Table[string, Future[void]]
    antiEntropySummaryNextRefreshAtMs: Table[string, int64]
    certificationObservedOverlayDigest: string
    certificationObservedLatestCheckpointId: string
    deferredAttestations*: Table[string, seq[EventAttestation]]
    deferredEventCertificates*: Table[string, seq[EventCertificate]]
    isolatedEvents*: OrderedTable[string, IsolationRecord]
    children*: Table[string, seq[string]]
    avoProposals*: OrderedTable[string, AvoProposal]
    avoApprovals*: Table[string, seq[string]]
    avoAdoptions*: OrderedTable[string, AvoAdoptionRecord]
    checkpointCandidates*: OrderedTable[string, CheckpointCandidate]
    checkpointVotes*: Table[string, seq[CheckpointVote]]
    checkpoints*: OrderedTable[string, CheckpointCertificate]
    checkpointSnapshots*: OrderedTable[string, ChainStateSnapshot]
    checkpointBundleCache*: Table[string, CheckpointBundle]
    liveState*: ChainState
    liveAppliedEvents*: HashSet[string]
    latestCheckpointId*: string
    inbound*: Deque[InboundMessage]
    inboundScheduled*: bool
    inboundPending: bool
    inboundRunner: Future[void]
    slowInboundSliceCount: int64
    maxInboundSliceElapsedMs: int64
    connectedHandler*: ConnEventHandler
    syncingRoutingPeers*: HashSet[string]
    routingSnapshotWantedGenerations*: Table[string, uint64]
    routingSnapshotSyncedGenerations*: Table[string, uint64]
    routingSnapshotNextRefreshAtMs*: Table[string, int64]
    maintenanceScheduled*: bool
    maintenanceImmediateScheduled*: bool
    maintenanceGeneration*: uint64
    maintenancePending: bool
    maintenanceImmediatePending: bool
    maintenanceRunner: Future[void]
    slowMaintenanceSliceCount: int64
    maxMaintenanceSliceElapsedMs: int64
    checkpointAdvancePending: bool
    checkpointAdvanceRunner: Future[void]
    certificationGatherPending: bool
    certificationGatherRunner: Future[void]
    forwardedEvents*: Table[string, HashSet[string]]
    forwardedAttestations*: Table[string, HashSet[string]]
    forwardedEventCertificates*: Table[string, HashSet[string]]
    forwardedCheckpointCandidates*: Table[string, HashSet[string]]
    forwardedCheckpointVotes*: Table[string, HashSet[string]]
    forwardedCheckpointBundles*: Table[string, HashSet[string]]
    forwardedAvoProposals*: Table[string, HashSet[string]]
    forwardedAvoApprovals*: Table[string, HashSet[string]]
    pendingEvents*: HashSet[string]
    pendingEventScopes*: Table[string, seq[LsmrPath]]
    pendingAttestations*: HashSet[string]
    routeBlockedAttestations*: HashSet[string]
    pendingAttestationScopes*: Table[string, seq[LsmrPath]]
    pendingEventCertificates*: HashSet[string]
    pendingEventCertificateScopes*: Table[string, seq[LsmrPath]]
    pendingEventCertificateFrontier*: HeapQueue[PendingEventCertificateTicket]
    pendingEventCertificateEpochs*: Table[string, uint64]
    pendingEventCertificateNextEpoch: uint64
    pendingCheckpointCandidates*: HashSet[string]
    pendingCheckpointVotes*: HashSet[string]
    pendingCheckpointBundles*: HashSet[string]
    eraCheckpointStates*: Table[uint64, EraCheckpointState]
    pendingAvoProposals*: HashSet[string]
    pendingAvoApprovals*: HashSet[string]
    eventDeliveries*: Table[string, DeliveryLedger]
    attestationDeliveries*: Table[string, DeliveryLedger]
    eventCertificateDeliveries*: Table[string, DeliveryLedger]
    checkpointCandidateDeliveries*: Table[string, DeliveryLedger]
    checkpointVoteDeliveries*: Table[string, DeliveryLedger]
    checkpointBundleDeliveries*: Table[string, DeliveryLedger]
    eventMaintenanceCursor: int
    attestationMaintenanceCursor: int
    eventCertificateMaintenanceCursor: int
    checkpointCandidateMaintenanceCursor: int
    checkpointVoteMaintenanceCursor: int
    checkpointBundleMaintenanceCursor: int
    avoProposalMaintenanceCursor: int
    avoApprovalMaintenanceCursor: int
    lsmrOverlayDigest*: string
    lsmrSubmitReadyDigest*: string
    lsmrDeliveryDigest*: string
    lsmrDeliveryGeneration: uint64
    lsmrObservedDeliveryGeneration: uint64
    lsmrObservedLatestCheckpointId: string
    lsmrDataPlaneEnabled: bool
    outboundSessions*: Table[string, OutboundPeerSession]
    replayStateCache*: Table[uint64, ReplayStateCacheEntry]

proc restoreFromStore(node: FabricNode)
proc selfAnnouncement(node: FabricNode): PeerAnnouncement
proc knownPeerAnnouncements(node: FabricNode): seq[PeerAnnouncement]
proc normalizeRoutingAddrs(values: openArray[string]): seq[string] {.gcsafe, raises: [].}
proc peerAnnouncementVersion(item: PeerAnnouncement): uint64 {.gcsafe, raises: [].}
proc wantRoutingSnapshotSync(
    node: FabricNode, peerIdText: string, immediate = false
) {.gcsafe, raises: [].}
proc fetchLookupRaw(node: FabricNode, key: string): Option[seq[byte]] {.gcsafe, raises: [].}
proc enqueueInbound(node: FabricNode, message: sink InboundMessage) {.gcsafe, raises: [].}
proc scheduleInboundDrain(node: FabricNode) {.gcsafe, raises: [].}
proc runInboundDrain(node: FabricNode): Future[void] {.async: (raises: []).}
proc drainInbound(node: FabricNode): bool {.raises: [].}
proc hasCheckpointForCandidate(
    node: FabricNode, candidateId: string
): bool {.gcsafe, raises: [].}
proc bootstrapRoutingSnapshot(node: FabricNode): Future[seq[PeerAnnouncement]] {.async: (raises: []).}
proc syncRoutingSnapshotTask(node: FabricNode, peerIdText: string): Future[void] {.async: (raises: []).}
proc ensureRoutingSnapshotSync(node: FabricNode, peerIdText: string)
proc clearRoutingSnapshotSync(node: FabricNode, peerIdText: string)
proc refreshUnsyncedRoutingSnapshots(node: FabricNode)
proc antiEntropySummaryTask(node: FabricNode, peerIdText: string): Future[void] {.async: (raises: []).}
proc ensureAntiEntropySummarySync(node: FabricNode, peerIdText: string) {.gcsafe, raises: [].}
proc clearAntiEntropySummarySync(node: FabricNode, peerIdText: string) {.gcsafe, raises: [].}
proc refreshAntiEntropySummarySync(node: FabricNode) {.gcsafe, raises: [].}
proc connectedPeerIds(node: FabricNode): seq[string] {.gcsafe, raises: [].}
proc routingSnapshotGateReady(node: FabricNode): bool {.gcsafe, raises: [].}
proc latestCheckpointEra(node: FabricNode): uint64 {.gcsafe, raises: [].}
proc checkpointFrontierBlockingEra(
    node: FabricNode
): tuple[hasBlockingEra: bool, era: uint64, firstMissingEventId: string] {.gcsafe, raises: [].}
proc deliveryNeedsAttempt(
    ledgerBook: Table[string, DeliveryLedger], itemKey, targetKey: string
): bool {.gcsafe, raises: [].}
proc hasOutstandingDelivery(
    ledgerBook: Table[string, DeliveryLedger], itemKey: string
): bool {.gcsafe, raises: [].}
proc maintenanceLoop(node: FabricNode, initialDelayMs: int) {.async: (raises: []).}
proc maintenanceStep(node: FabricNode): bool {.raises: [].}
proc scheduleMaintenance(node: FabricNode, immediate = false) {.gcsafe, raises: [].}
proc scheduleStateMaintenance(node: FabricNode) {.gcsafe, raises: [].}
proc scheduleCertificationGather(node: FabricNode) {.gcsafe, raises: [].}
proc scheduleCertificationGatherRunner(node: FabricNode) {.gcsafe, raises: [].}
proc certificationGatherLoop(node: FabricNode) {.async: (raises: []).}
proc checkpointAdvanceReady(node: FabricNode): bool {.gcsafe, raises: [].}
proc scheduleCheckpointAdvance(node: FabricNode) {.gcsafe, raises: [].}
proc scheduleCheckpointAdvanceRunner(node: FabricNode) {.gcsafe, raises: [].}
proc checkpointAdvanceLoop(node: FabricNode) {.async: (raises: []).}
proc scheduleReadyOutboundSessions(node: FabricNode) {.gcsafe, raises: [].}
proc enableLsmrDataPlane*(node: FabricNode) {.gcsafe, raises: [].}
proc submitWarmReady(node: FabricNode): bool {.gcsafe, raises: [].}
proc shouldPromoteLatestCheckpoint(
    node: FabricNode, checkpointId: string, snapshot: ChainStateSnapshot
): bool {.gcsafe, raises: [].}
proc eventHasRunnableLsmrDelivery(
    node: FabricNode, event: FabricEvent, scopePrefixes: seq[LsmrPath]
): bool
proc eventDependentsMayRelay(node: FabricNode, event: FabricEvent): bool
proc eventAuthorRoute(
    node: FabricNode, event: FabricEvent
): Option[RoutingPeer] {.gcsafe, raises: [].}
proc eventCertificateRelayPlans(
    node: FabricNode, scopePrefixes: seq[LsmrPath] = @[]
): seq[RelayPlan]
proc pruneCheckpointedEventCertificateDeliveries(node: FabricNode) {.gcsafe, raises: [].}
proc disseminateEventCertificateWithRelayPlans(
    node: FabricNode,
    event: FabricEvent,
    cert: EventCertificate,
    relayPlans: openArray[RelayPlan],
    scopePrefixes: seq[LsmrPath] = @[],
)
proc allRoutingPaths(node: FabricNode): seq[LsmrPath]
proc lsmrRelayRoutes(
    node: FabricNode, targetPaths: openArray[LsmrPath], scopePrefixes: openArray[LsmrPath] = []
): seq[RelayPlan]
proc attestationHasRunnableLsmrDelivery(
    node: FabricNode, event: FabricEvent, att: EventAttestation
): bool
proc eventCertificateHasRunnableLsmrDelivery(
    node: FabricNode, event: FabricEvent, cert: EventCertificate
): bool
proc eventCertificateHasRunnableLsmrDelivery(
    node: FabricNode,
    event: FabricEvent,
    cert: EventCertificate,
    relayPlans: openArray[RelayPlan],
): bool
proc attestationRequiresRelay(
    node: FabricNode, event: FabricEvent, att: EventAttestation
): bool {.gcsafe, raises: [].}
proc attestationForKey(
    node: FabricNode, key: string
): Option[EventAttestation] {.gcsafe, raises: [].}
proc maybeCertifyEvent(node: FabricNode, event: FabricEvent, component: seq[FabricNode]): bool
proc clearCertificationEventState(
    node: FabricNode, eventId: string
) {.gcsafe, raises: [].}
proc certificationEventTracked(
    node: FabricNode, eventId: string
): bool {.gcsafe, raises: [].}
proc refreshEventCertificationProgressState(
    node: FabricNode, eventId: string
) {.gcsafe, raises: [].}
proc initEraCheckpointState(): EraCheckpointState {.gcsafe, raises: [].}
proc refreshEraCheckpointStates(node: FabricNode) {.gcsafe, raises: [].}
proc checkpointEraState(
    node: FabricNode, era: uint64
): EraCheckpointState {.gcsafe, raises: [].}
proc refreshCertificationEvent(
    node: FabricNode, eventId: string, force = false
): bool {.gcsafe, raises: [].}
proc witnessCount(
    node: FabricNode,
    event: FabricEvent,
    component: seq[FabricNode],
): tuple[reachable: int, threshold: int, signed: int, witnessAtts: seq[EventAttestation]] {.gcsafe, raises: [].}
proc disseminateEventCertificate(
    node: FabricNode,
    event: FabricEvent,
    cert: EventCertificate,
    scopePrefixes: seq[LsmrPath] = @[],
)
proc noteLsmrDeliveryMutation(node: FabricNode) {.gcsafe, raises: [].}
proc logMaintenanceError(
    node: FabricNode, phase, itemKey: string, exc: ref Exception
) {.gcsafe, raises: [].}
proc hasPending(
    session: OutboundPeerSession, priority: OutboundSubmitPriority
): bool {.gcsafe, raises: [].}
proc hasRunning(
    session: OutboundPeerSession, priority: OutboundSubmitPriority
): bool {.gcsafe, raises: [].}
proc ensureEraCandidates(node: FabricNode, component: seq[FabricNode]): bool
proc advanceCheckpointState(node: FabricNode, component: seq[FabricNode]): bool
proc pendingCheckpointAdvanceCandidates(node: FabricNode): seq[CheckpointCandidate]
proc advanceCheckpointCandidate(node: FabricNode, candidate: CheckpointCandidate): bool
proc enqueueOutbound(
    node: FabricNode,
    itemKey: string,
    targetPeerId: string,
    priority: OutboundSubmitPriority,
    submit: proc(): Future[bool] {.closure, gcsafe, raises: [].},
    onSuccess: proc() {.closure, gcsafe, raises: [].},
    onFailure: proc() {.closure, gcsafe, raises: [].},
)
proc usesLsmrPrimary(node: FabricNode): bool {.gcsafe, raises: [].}

proc diagNowMs(): int64 {.gcsafe, raises: [].} =
  int64(epochTime() * 1000)

proc normalizeRoutingAddrs(values: openArray[string]): seq[string] {.gcsafe, raises: [].} =
  var seen = initHashSet[string]()
  for raw in values:
    if raw.len == 0 or raw in seen:
      continue
    seen.incl(raw)
    result.add(raw)
  result.sort(system.cmp[string])

proc peerAnnouncementVersion(item: PeerAnnouncement): uint64 {.gcsafe, raises: [].} =
  if item.routeVersion > 0'u64:
    return item.routeVersion
  if item.createdAt > 0:
    return item.createdAt.uint64
  0'u64

proc routingPeerFromAnnouncement(item: PeerAnnouncement): RoutingPeer {.gcsafe, raises: [].} =
  RoutingPeer(
    account: item.account,
    peerId: item.peerId,
    path: item.path,
    addrs: normalizeRoutingAddrs(item.addrs),
  )

proc routeAnnouncementDigest(
    account, peerIdText, publicKey: string, path: LsmrPath, addrs: openArray[string]
): string {.gcsafe, raises: [].} =
  hashHex(account & "\n" & peerIdText & "\n" & encodeObj(path) & "\n" &
    normalizeRoutingAddrs(addrs).join("\n") & "\n" & publicKey)

proc wantRoutingSnapshotSync(
    node: FabricNode, peerIdText: string, immediate = false
) {.gcsafe, raises: [].} =
  if node.isNil or peerIdText.len == 0:
    return
  let synced = node.routingSnapshotSyncedGenerations.getOrDefault(peerIdText, 0'u64)
  let wanted = node.routingSnapshotWantedGenerations.getOrDefault(peerIdText, 0'u64)
  node.routingSnapshotWantedGenerations[peerIdText] = max(synced, wanted) + 1'u64
  node.routingSnapshotNextRefreshAtMs[peerIdText] =
    if immediate: 0'i64 else: diagNowMs() + FabricRoutingSnapshotResyncMs
  if node.running:
    node.scheduleMaintenance(immediate = true)

proc noteInboundSlice(node: FabricNode, elapsedMs: int64) {.gcsafe, raises: [].} =
  if node.isNil or elapsedMs < 0:
    return
  if elapsedMs > node.maxInboundSliceElapsedMs:
    node.maxInboundSliceElapsedMs = elapsedMs
  if elapsedMs >= FabricSlowInboundSliceMs:
    inc node.slowInboundSliceCount
    echo "fabric-inbound slow-slice account=", node.identity.account,
      " elapsedMs=", elapsedMs,
      " inboundRemain=", node.inbound.len

proc noteMaintenanceSlice(node: FabricNode, elapsedMs: int64) {.gcsafe, raises: [].} =
  if node.isNil or elapsedMs < 0:
    return
  if elapsedMs > node.maxMaintenanceSliceElapsedMs:
    node.maxMaintenanceSliceElapsedMs = elapsedMs
  if elapsedMs >= FabricSlowMaintenanceSliceMs:
    inc node.slowMaintenanceSliceCount
    echo "fabric-maintenance slow-slice account=", node.identity.account,
      " elapsedMs=", elapsedMs,
      " pendingEvents=", node.pendingEvents.len,
      " pendingAttestations=", node.pendingAttestations.len,
      " pendingEventCerts=", node.pendingEventCertificates.len

proc maintenanceShouldYield(
    node: FabricNode, startedAtMs: int64, phase: string, itemKey = ""
): bool {.gcsafe, raises: [].} =
  if node.isNil or startedAtMs <= 0:
    return false
  let elapsedMs = diagNowMs() - startedAtMs
  if elapsedMs < FabricMaintenanceSliceBudgetMs:
    return false
  when defined(fabric_submit_diag):
    echo "maintenance-yield account=", node.identity.account,
      " phase=", phase,
      " item=", itemKey,
      " elapsedMs=", elapsedMs,
      " pendingEvents=", node.pendingEvents.len,
      " pendingAttestations=", node.pendingAttestations.len,
      " pendingEventCerts=", node.pendingEventCertificates.len,
      " pendingCandidates=", node.pendingCheckpointCandidates.len,
      " pendingVotes=", node.pendingCheckpointVotes.len,
      " pendingBundles=", node.pendingCheckpointBundles.len
  true

proc defaultIdentityPath*(dataDir: string): string =
  dataDir / "identity.json"

proc defaultGenesisPath*(dataDir: string): string =
  dataDir / "genesis.json"

proc createDefaultGenesis*(identity: NodeIdentity, chainId = "fabric-local"): GenesisSpec =
  var params = defaultChainParams()
  params.chainId = chainId
  GenesisSpec(
    params: params,
    validators: @[
      ValidatorInfo(
        address: identity.account,
        peerId: peerIdString(identity.peerId),
        publicKey: encodePublicKeyHex(identity.publicKey),
        stake: 10_000,
        delegatedStake: 0,
        active: true,
        jailed: false,
        slashedAmount: 0,
        lastUpdatedAt: 0,
      )
    ],
    balances: @[
      KeyValueU64(key: params.rewardPoolAccount, value: 500_000),
      KeyValueU64(key: identity.account, value: 500_000),
    ],
    names: @[],
    contents: @[],
    mintedSupply: 1_000_000,
  )

proc saveGenesis*(path: string, genesis: GenesisSpec) =
  createDir(parentDir(path))
  writeFile(path, encodeObj(genesis))

proc loadGenesis*(path: string): GenesisSpec =
  decodeObj[GenesisSpec](readFile(path))

proc sortedUnique(items: openArray[string]): seq[string] =
  for item in items:
    if item.len > 0 and item notin result:
      result.add(item)
  result.sort(system.cmp[string])

proc appendUnique(target: var seq[string], value: string) =
  if value.len == 0:
    return
  for item in target:
    if item == value:
      return
  target.add(value)

proc enqueuePending(pending: var HashSet[string], key: string): bool =
  if key.len == 0 or key in pending:
    return false
  pending.incl(key)
  true

proc `<`(a, b: PendingEventCertificateTicket): bool =
  if a.known != b.known:
    return a.known and not b.known
  if a.known and b.known:
    let order = clockCmp(a.clock, b.clock)
    if order != 0:
      return order < 0
  if a.eventId != b.eventId:
    return a.eventId < b.eventId
  a.epoch < b.epoch

proc `<`(a, b: PendingCertificationEventTicket): bool =
  if a.known != b.known:
    return a.known and not b.known
  if a.known and b.known:
    let order = clockCmp(a.clock, b.clock)
    if order != 0:
      return order < 0
  if a.eventId != b.eventId:
    return a.eventId < b.eventId
  a.epoch < b.epoch

proc antiEntropyDomainLabel(
    domain: AntiEntropyDomain
): string {.gcsafe, raises: [].} =
  case domain
  of aedEvent:
    "event"
  of aedAttParticipant:
    "att_participant"
  of aedAttWitness:
    "att_witness"
  of aedEventCert:
    "eventcert"
  of aedCheckpointCandidate:
    "checkpoint_candidate"
  of aedCheckpointVote:
    "checkpoint_vote"
  of aedCheckpointBundle:
    "checkpoint_bundle"

proc parseAntiEntropyDomain(
    value: string
): Option[AntiEntropyDomain] {.gcsafe, raises: [].} =
  case value
  of "event":
    some(aedEvent)
  of "att_participant":
    some(aedAttParticipant)
  of "att_witness":
    some(aedAttWitness)
  of "eventcert":
    some(aedEventCert)
  of "checkpoint_candidate":
    some(aedCheckpointCandidate)
  of "checkpoint_vote":
    some(aedCheckpointVote)
  of "checkpoint_bundle":
    some(aedCheckpointBundle)
  else:
    none(AntiEntropyDomain)

proc antiEntropyDigest(parts: openArray[string]): string {.gcsafe, raises: [].} =
  hashHex(parts.join("|"))

proc antiEntropyCandidateClock(
    candidate: CheckpointCandidate
): GanzhiClock {.gcsafe, raises: [].} =
  GanzhiClock(era: candidate.era, tick: 0)

proc antiEntropyVoteClock(
    node: FabricNode, vote: CheckpointVote
): GanzhiClock {.gcsafe, raises: [].} =
  if node.isNil:
    return GanzhiClock(era: 0, tick: 0)
  let candidate = node.checkpointCandidates.getOrDefault(vote.candidateId)
  if candidate.candidateId.len == 0:
    return GanzhiClock(era: 0, tick: 0)
  antiEntropyCandidateClock(candidate)

proc antiEntropyBundleClock(
    cert: CheckpointCertificate
): GanzhiClock {.gcsafe, raises: [].} =
  GanzhiClock(era: cert.candidate.era, tick: 0)

proc attestationFetchKey(
    eventId: string, role: AttestationRole, signer: string
): string {.gcsafe, raises: [].}
proc parseAttestationRole(
    value: string
): Option[AttestationRole] {.gcsafe, raises: [].}
proc attestationIndexKey(
    eventId: string, role: AttestationRole, signer: string
): string {.gcsafe, raises: [].}
proc attestationIndexKeyFromLookup(
    key: string
): string {.gcsafe, raises: [].}
proc antiEntropyKeySuffix(key, prefix: string): string {.inline, gcsafe, raises: [].}
proc missingEventCertificatePulls(
    node: FabricNode
): seq[AntiEntropyPullWorkItem] {.gcsafe, raises: [].}
proc checkpointVoteForKey(
    node: FabricNode, key: string
): Option[CheckpointVote] {.gcsafe, raises: [].}

proc antiEntropyDomainEntries(
    node: FabricNode, domain: AntiEntropyDomain
): seq[tuple[clock: GanzhiClock, key: string]] {.gcsafe, raises: [].} =
  if node.isNil:
    return @[]
  case domain
  of aedEvent:
    for eventId, event in node.events.pairs:
      result.add((event.clock, "event:" & eventId))
  of aedAttParticipant, aedAttWitness:
    let wantedRole =
      if domain == aedAttParticipant: arParticipant else: arWitness
    for eventId, atts in node.eventAttestations.pairs:
      let event = node.events.getOrDefault(eventId)
      if event.eventId.len == 0:
        continue
      let clock = event.clock
      for att in atts:
        if att.role == wantedRole:
          result.add((clock, attestationFetchKey(att.eventId, att.role, att.signer)))
  of aedEventCert:
    for eventId, cert in node.eventCertificates.pairs:
      result.add((cert.clock, "eventcert:" & eventId))
  of aedCheckpointCandidate:
    for candidateId, candidate in node.checkpointCandidates.pairs:
      result.add((antiEntropyCandidateClock(candidate), "checkpoint-candidate:" & candidateId))
  of aedCheckpointVote:
    for _, votes in node.checkpointVotes.pairs:
      for vote in votes:
        result.add((
          antiEntropyVoteClock(node, vote),
          "checkpoint-vote:" & vote.candidateId & ":" & vote.validator,
        ))
  of aedCheckpointBundle:
    for checkpointId, cert in node.checkpoints.pairs:
      result.add((antiEntropyBundleClock(cert), "checkpoint-bundle:" & checkpointId))
  result.sort(proc(
      a, b: tuple[clock: GanzhiClock, key: string]
  ): int =
    result = clockCmp(a.clock, b.clock)
    if result == 0:
      result = cmp(a.key, b.key)
  )

proc antiEntropyRootSummary(
    node: FabricNode
): AntiEntropyRootSummary {.gcsafe, raises: [].} =
  for domain in AntiEntropyDomain:
    let entries = node.antiEntropyDomainEntries(domain)
    var eraParts = initTable[uint64, seq[string]]()
    var maxClock = GanzhiClock(era: 0, tick: 0)
    for entry in entries:
      maxClock = maxClock(maxClock, entry.clock)
      eraParts.mgetOrPut(entry.clock.era, @[]).add($entry.clock.tick & ":" & entry.key)
    var eras: seq[AntiEntropyEraDigest] = @[]
    for era in eraParts.keys.toSeq().sorted(system.cmp[uint64]):
      eras.add(AntiEntropyEraDigest(
        era: era,
        digest: antiEntropyDigest(eraParts.getOrDefault(era)),
      ))
    result.domains.add(AntiEntropyDomainRootSummary(
      domain: antiEntropyDomainLabel(domain),
      maxClock: maxClock,
      eras: eras,
    ))

proc antiEntropyEraSummary(
    node: FabricNode, domain: AntiEntropyDomain, era: uint64
): AntiEntropyEraSummary {.gcsafe, raises: [].} =
  result.domain = antiEntropyDomainLabel(domain)
  result.era = era
  var tickParts = initTable[uint64, seq[string]]()
  for entry in node.antiEntropyDomainEntries(domain):
    if entry.clock.era != era:
      continue
    result.maxClock = maxClock(result.maxClock, entry.clock)
    tickParts.mgetOrPut(uint64(entry.clock.tick), @[]).add(entry.key)
  for tick in tickParts.keys.toSeq().sorted(system.cmp[uint64]):
    result.ticks.add(AntiEntropyTickDigest(
      tick: tick,
      digest: antiEntropyDigest(tickParts.getOrDefault(tick)),
    ))

proc antiEntropyTickSummary(
    node: FabricNode, domain: AntiEntropyDomain, era: uint64, tick: uint64
): AntiEntropyTickSummary {.gcsafe, raises: [].} =
  result.domain = antiEntropyDomainLabel(domain)
  result.era = era
  result.tick = tick
  for entry in node.antiEntropyDomainEntries(domain):
    if entry.clock.era == era and uint64(entry.clock.tick) == tick:
      result.items.add(entry.key)
  result.items.sort(system.cmp[string])

proc antiEntropyLocalHasKey(
    node: FabricNode, domain: AntiEntropyDomain, key: string
): bool {.gcsafe, raises: [].} =
  if node.isNil or key.len == 0:
    return false
  case domain
  of aedEvent:
    let eventId = antiEntropyKeySuffix(key, "event:")
    eventId.len > 0 and eventId in node.events
  of aedAttParticipant, aedAttWitness:
    node.attestationForKey(key).isSome()
  of aedEventCert:
    let eventId = antiEntropyKeySuffix(key, "eventcert:")
    eventId.len > 0 and eventId in node.eventCertificates
  of aedCheckpointCandidate:
    let candidateId = antiEntropyKeySuffix(key, "checkpoint-candidate:")
    candidateId.len > 0 and candidateId in node.checkpointCandidates
  of aedCheckpointVote:
    node.checkpointVoteForKey(
      antiEntropyKeySuffix(key, "checkpoint-vote:")
    ).isSome()
  of aedCheckpointBundle:
    let checkpointId = antiEntropyKeySuffix(key, "checkpoint-bundle:")
    checkpointId.len > 0 and checkpointId in node.checkpoints

proc antiEntropyPullItem(
    node: FabricNode,
    targetPeerId: string,
    domain: AntiEntropyDomain,
    key: string,
    era: uint64,
    tick: uint64,
): Option[AntiEntropyPullWorkItem] {.gcsafe, raises: [].} =
  if node.isNil or targetPeerId.len == 0 or key.len == 0:
    return none(AntiEntropyPullWorkItem)
  let clock = GanzhiClock(era: era, tick: uint8(min(tick, uint64(high(uint8)))))
  case domain
  of aedEvent:
    some(AntiEntropyPullWorkItem(
      domain: domain,
      key: key,
      causedByEventId: antiEntropyKeySuffix(key, "event:"),
      targetPeerId: targetPeerId,
      clock: clock,
    ))
  of aedAttParticipant, aedAttWitness:
    let parts = key.split(":")
    if parts.len != 4:
      return none(AntiEntropyPullWorkItem)
    some(AntiEntropyPullWorkItem(
      domain: domain,
      key: key,
      causedByEventId: parts[1],
      targetPeerId: targetPeerId,
      clock: clock,
    ))
  of aedEventCert:
    let eventId = antiEntropyKeySuffix(key, "eventcert:")
    if eventId.len == 0:
      return none(AntiEntropyPullWorkItem)
    some(AntiEntropyPullWorkItem(
      domain: domain,
      key: key,
      causedByEventId: eventId,
      targetPeerId: targetPeerId,
      clock: clock,
    ))
  of aedCheckpointCandidate, aedCheckpointVote, aedCheckpointBundle:
    some(AntiEntropyPullWorkItem(
      domain: domain,
      key: key,
      causedByEventId: "",
      targetPeerId: targetPeerId,
      clock: clock,
    ))

proc dropPendingEventCertificate(
    node: FabricNode, eventId: string, clearScopes = false
) {.gcsafe, raises: [].} =
  if node.isNil or eventId.len == 0:
    return
  node.pendingEventCertificates.excl(eventId)
  if clearScopes and eventId in node.pendingEventCertificateScopes:
    node.pendingEventCertificateScopes.del(eventId)
  if eventId in node.pendingEventCertificateEpochs:
    node.pendingEventCertificateEpochs.del(eventId)

proc markPendingEventCertificate(
    node: FabricNode, eventId: string
): bool {.gcsafe, raises: [].} =
  if node.isNil or eventId.len == 0 or eventId in node.pendingEventCertificates:
    return false
  node.pendingEventCertificates.incl(eventId)
  true

proc touchPendingEventCertificate(
    node: FabricNode, eventId: string, force = false
): bool {.gcsafe, raises: [].} =
  if node.isNil or eventId.len == 0:
    return false
  let alreadyPending = eventId in node.pendingEventCertificates
  let alreadyQueued = eventId in node.pendingEventCertificateEpochs
  if alreadyPending and alreadyQueued and not force:
    return false
  discard node.markPendingEventCertificate(eventId)
  inc node.pendingEventCertificateNextEpoch
  let epoch = node.pendingEventCertificateNextEpoch
  node.pendingEventCertificateEpochs[eventId] = epoch
  var ticket = PendingEventCertificateTicket(
    eventId: eventId,
    epoch: epoch,
  )
  if eventId in node.events:
    ticket.known = true
    ticket.clock = node.events.getOrDefault(eventId).clock
  node.pendingEventCertificateFrontier.push(ticket)
  true

proc drainPendingEventCertificateFrontier(
    node: FabricNode, limit: int
): seq[string] {.gcsafe, raises: [].} =
  if node.isNil or limit <= 0:
    return @[]
  var seen = initHashSet[string]()
  while node.pendingEventCertificateFrontier.len > 0 and result.len < limit:
    let ticket = node.pendingEventCertificateFrontier.pop()
    if ticket.eventId.len == 0 or ticket.eventId in seen:
      continue
    if ticket.eventId notin node.pendingEventCertificates:
      continue
    if node.pendingEventCertificateEpochs.getOrDefault(ticket.eventId) != ticket.epoch:
      continue
    node.pendingEventCertificateEpochs.del(ticket.eventId)
    seen.incl(ticket.eventId)
    result.add(ticket.eventId)

proc attestationFetchKey(
    eventId: string, role: AttestationRole, signer: string
): string {.gcsafe, raises: [].} =
  if eventId.len == 0 or signer.len == 0:
    return ""
  "attestation:" & eventId & ":" & $role & ":" & signer

proc attestationIndexKey(
    eventId: string, role: AttestationRole, signer: string
): string {.gcsafe, raises: [].} =
  if eventId.len == 0 or signer.len == 0:
    return ""
  eventId & ":" & $ord(role) & ":" & signer

proc attestationIndexKeyFromLookup(
    key: string
): string {.gcsafe, raises: [].} =
  if key.len == 0:
    return ""
  if not key.startsWith("attestation:"):
    return key
  let parts = key.split(":")
  if parts.len != 4:
    return ""
  let role = parseAttestationRole(parts[2])
  if role.isNone():
    return ""
  attestationIndexKey(parts[1], role.get(), parts[3])

proc parseAttestationRole(
    value: string
): Option[AttestationRole] {.gcsafe, raises: [].} =
  case value
  of $arParticipant:
    some(arParticipant)
  of $arWitness:
    some(arWitness)
  of $arIsolation:
    some(arIsolation)
  else:
    none(AttestationRole)

proc dropPendingCertificationEvent(
    node: FabricNode, eventId: string
) {.gcsafe, raises: [].} =
  if node.isNil or eventId.len == 0:
    return
  node.pendingCertificationEvents.excl(eventId)
  if eventId in node.pendingCertificationEventEpochs:
    node.pendingCertificationEventEpochs.del(eventId)

proc markPendingCertificationEvent(
    node: FabricNode, eventId: string
): bool {.gcsafe, raises: [].} =
  if node.isNil or eventId.len == 0 or eventId in node.pendingCertificationEvents:
    return false
  node.pendingCertificationEvents.incl(eventId)
  true

proc touchPendingCertificationEvent(
    node: FabricNode, eventId: string, force = false
): bool {.gcsafe, raises: [].} =
  if node.isNil or eventId.len == 0:
    return false
  let alreadyPending = eventId in node.pendingCertificationEvents
  let alreadyQueued = eventId in node.pendingCertificationEventEpochs
  if alreadyPending and alreadyQueued and not force:
    return false
  discard node.markPendingCertificationEvent(eventId)
  inc node.pendingCertificationEventNextEpoch
  let epoch = node.pendingCertificationEventNextEpoch
  node.pendingCertificationEventEpochs[eventId] = epoch
  var ticket = PendingCertificationEventTicket(
    eventId: eventId,
    epoch: epoch,
  )
  if eventId in node.events:
    ticket.known = true
    ticket.clock = node.events.getOrDefault(eventId).clock
  node.pendingCertificationEventFrontier.push(ticket)
  true

proc drainPendingCertificationEventFrontier(
    node: FabricNode, limit: int
): seq[string] {.gcsafe, raises: [].} =
  if node.isNil or limit <= 0:
    return @[]
  var seen = initHashSet[string]()
  while node.pendingCertificationEventFrontier.len > 0 and result.len < limit:
    let ticket = node.pendingCertificationEventFrontier.pop()
    if ticket.eventId.len == 0 or ticket.eventId in seen:
      continue
    if ticket.eventId notin node.pendingCertificationEvents:
      continue
    if node.pendingCertificationEventEpochs.getOrDefault(ticket.eventId) != ticket.epoch:
      continue
    node.pendingCertificationEventEpochs.del(ticket.eventId)
    seen.incl(ticket.eventId)
    result.add(ticket.eventId)

proc antiEntropyPullOwnerKey(
    targetPeerId: string, domain: AntiEntropyDomain
): string {.gcsafe, raises: [].} =
  if targetPeerId.len == 0:
    return ""
  targetPeerId & "|" & antiEntropyDomainLabel(domain)

proc witnessPullQueuedForPeer(
    node: FabricNode, targetPeerId: string, domain: AntiEntropyDomain, key: string
): bool {.gcsafe, raises: [].} =
  if node.isNil or targetPeerId.len == 0 or key.len == 0:
    return false
  let ownerKey = antiEntropyPullOwnerKey(targetPeerId, domain)
  key in node.pendingWitnessPullKeysByPeer.getOrDefault(
    ownerKey,
    initHashSet[string](),
  )

proc witnessPullInflightForPeer(
    node: FabricNode, targetPeerId: string, domain: AntiEntropyDomain, key: string
): bool {.gcsafe, raises: [].} =
  if node.isNil or targetPeerId.len == 0 or key.len == 0:
    return false
  let ownerKey = antiEntropyPullOwnerKey(targetPeerId, domain)
  if ownerKey notin node.inflightWitnessPullByPeer:
    return false
  node.inflightWitnessPullByPeer.getOrDefault(ownerKey).key == key

proc witnessPullActive(
    node: FabricNode, domain: AntiEntropyDomain, key: string
): bool {.gcsafe, raises: [].} =
  if node.isNil or key.len == 0:
    return false
  for queue in node.pendingWitnessPullsByPeer.values:
    for item in queue:
      if item.domain == domain and item.key == key:
        return true
  for item in node.inflightWitnessPullByPeer.values:
    if item.domain == domain and item.key == key:
      return true
  false

proc enqueueWitnessPullWorkItem(
    node: FabricNode, item: AntiEntropyPullWorkItem
): bool {.gcsafe, raises: [].} =
  if node.isNil or item.targetPeerId.len == 0 or item.key.len == 0:
    return false
  let ownerKey = antiEntropyPullOwnerKey(item.targetPeerId, item.domain)
  if ownerKey.len == 0:
    return false
  if node.witnessPullActive(item.domain, item.key):
    return false
  var queue = node.pendingWitnessPullsByPeer.getOrDefault(
    ownerKey,
    initDeque[AntiEntropyPullWorkItem](),
  )
  queue.addLast(item)
  node.pendingWitnessPullsByPeer[ownerKey] = queue
  var keys = node.pendingWitnessPullKeysByPeer.getOrDefault(
    ownerKey,
    initHashSet[string](),
  )
  keys.incl(item.key)
  node.pendingWitnessPullKeysByPeer[ownerKey] = keys
  true

proc clearPendingWitnessPullKey(
    node: FabricNode, ownerKey, key: string
) {.gcsafe, raises: [].} =
  if node.isNil or ownerKey.len == 0 or key.len == 0 or
      ownerKey notin node.pendingWitnessPullKeysByPeer:
    return
  var keys = node.pendingWitnessPullKeysByPeer.getOrDefault(
    ownerKey,
    initHashSet[string](),
  )
  keys.excl(key)
  if keys.len == 0:
    node.pendingWitnessPullKeysByPeer.del(ownerKey)
  else:
    node.pendingWitnessPullKeysByPeer[ownerKey] = keys

proc clearQueuedWitnessPullsForEvent(
    node: FabricNode, eventId: string
) {.gcsafe, raises: [].} =
  if node.isNil or eventId.len == 0:
    return
  for ownerKey in node.pendingWitnessPullsByPeer.keys.toSeq():
    var queued = node.pendingWitnessPullsByPeer.getOrDefault(
      ownerKey,
      initDeque[AntiEntropyPullWorkItem](),
    )
    var kept = initDeque[AntiEntropyPullWorkItem]()
    var changed = false
    while queued.len > 0:
      let item = queued.popFirst()
      if item.causedByEventId == eventId:
        node.clearPendingWitnessPullKey(ownerKey, item.key)
        changed = true
        continue
      kept.addLast(item)
    if not changed:
      continue
    if kept.len == 0:
      node.pendingWitnessPullsByPeer.del(ownerKey)
    else:
      node.pendingWitnessPullsByPeer[ownerKey] = kept

proc clearQueuedWitnessPullByKey(
    node: FabricNode, key: string
) {.gcsafe, raises: [].} =
  if node.isNil or key.len == 0:
    return
  for ownerKey in node.pendingWitnessPullsByPeer.keys.toSeq():
    var queued = node.pendingWitnessPullsByPeer.getOrDefault(
      ownerKey,
      initDeque[AntiEntropyPullWorkItem](),
    )
    var kept = initDeque[AntiEntropyPullWorkItem]()
    var changed = false
    while queued.len > 0:
      let item = queued.popFirst()
      if item.key == key:
        node.clearPendingWitnessPullKey(ownerKey, item.key)
        changed = true
        continue
      kept.addLast(item)
    if not changed:
      continue
    if kept.len == 0:
      node.pendingWitnessPullsByPeer.del(ownerKey)
    else:
      node.pendingWitnessPullsByPeer[ownerKey] = kept

proc clearInflightWitnessPullsForEvent(
    node: FabricNode, eventId: string
) {.gcsafe, raises: [].} =
  if node.isNil or eventId.len == 0:
    return
  for ownerKey in node.inflightWitnessPullByPeer.keys.toSeq():
    let item = node.inflightWitnessPullByPeer.getOrDefault(ownerKey)
    if item.causedByEventId == eventId:
      node.inflightWitnessPullByPeer.del(ownerKey)

proc antiEntropyKeySuffix(key, prefix: string): string {.inline, gcsafe, raises: [].} =
  if key.startsWith(prefix):
    key[prefix.len .. ^1]
  else:
    ""

proc witnessPullStillNeeded(
    node: FabricNode, item: AntiEntropyPullWorkItem
): bool {.gcsafe, raises: [].} =
  if node.isNil or item.key.len == 0:
    return false
  case item.domain
  of aedEvent:
    let eventId = antiEntropyKeySuffix(item.key, "event:")
    eventId.len > 0 and eventId notin node.events
  of aedAttParticipant, aedAttWitness:
    if item.causedByEventId.len == 0 or
        item.causedByEventId notin node.events or
        not node.certificationEventTracked(item.causedByEventId):
      return false
    node.refreshEventCertificationProgressState(item.causedByEventId)
    if item.causedByEventId notin node.eventCertificationProgress:
      return false
    let progress = node.eventCertificationProgress.getOrDefault(item.causedByEventId)
    let parts = item.key.split(":")
    if parts.len != 4:
      return false
    let signer = parts[3]
    if signer.len == 0:
      return false
    if item.domain == aedAttParticipant:
      if progress.status != agsWaitingParticipants:
        return false
      if signer in progress.participantAttestations:
        return false
    else:
      if progress.status != agsWaitingWitnesses and progress.status != agsCertReady:
        return false
      if signer notin progress.missingWitnessAccounts:
        return false
    node.attestationForKey(item.key).isNone()
  of aedEventCert:
    let eventId = antiEntropyKeySuffix(item.key, "eventcert:")
    if eventId.len == 0 or eventId in node.eventCertificates:
      return false
    for current in node.missingEventCertificatePulls():
      if current.targetPeerId == item.targetPeerId and current.key == item.key:
        return true
    false
  of aedCheckpointCandidate:
    let candidateId = antiEntropyKeySuffix(item.key, "checkpoint-candidate:")
    candidateId.len > 0 and candidateId notin node.checkpointCandidates
  of aedCheckpointVote:
    node.checkpointVoteForKey(
      antiEntropyKeySuffix(item.key, "checkpoint-vote:")
    ).isNone()
  of aedCheckpointBundle:
    let checkpointId = antiEntropyKeySuffix(item.key, "checkpoint-bundle:")
    checkpointId.len > 0 and checkpointId notin node.checkpoints

proc sortedKeys(pending: HashSet[string]): seq[string] =
  result = pending.toSeq()
  result.sort(system.cmp[string])

proc attestationKey(att: EventAttestation): string {.gcsafe, raises: [].} =
  att.eventId & ":" & $ord(att.role) & ":" & att.signer

proc checkpointVoteKey(vote: CheckpointVote): string {.gcsafe, raises: [].} =
  vote.candidateId & ":" & vote.validator

proc avoApprovalKey(proposalId, validator: string): string {.gcsafe, raises: [].} =
  proposalId & ":" & validator

proc mergeScopePrefixes(
    scopeBook: var Table[string, seq[LsmrPath]], key: string, scopes: openArray[LsmrPath]
): bool {.gcsafe, raises: [].}
proc pendingScopePrefixes(
    scopeBook: Table[string, seq[LsmrPath]], key: string
): seq[LsmrPath] {.gcsafe, raises: [].}
proc clearPendingScopePrefixes(
    scopeBook: var Table[string, seq[LsmrPath]], key: string
) {.gcsafe, raises: [].}
proc pathKey(path: LsmrPath): string {.gcsafe, raises: [].}
proc completeEventDelivery(node: FabricNode, eventId: string) {.gcsafe, raises: [].}
proc completeAttestationDelivery(
    node: FabricNode, attestationKey: string
) {.gcsafe, raises: [].}
proc completeEventCertificateDelivery(
    node: FabricNode, eventId: string
) {.gcsafe, raises: [].}
proc completeCheckpointCandidateDelivery(
    node: FabricNode, candidateId: string
) {.gcsafe, raises: [].}
proc completeCheckpointVoteDelivery(
    node: FabricNode, voteKey: string
) {.gcsafe, raises: [].}
proc completeCheckpointBundleDelivery(
    node: FabricNode, checkpointId: string
) {.gcsafe, raises: [].}
proc attestationRelayRoutes(
    node: FabricNode, event: FabricEvent, att: EventAttestation
): seq[tuple[route: RoutingPeer, scope: LsmrPath]]
proc attestationNeedsRelay(
    node: FabricNode, event: FabricEvent, att: EventAttestation
): bool {.gcsafe, raises: [].}

proc initDeliveryLedger(): DeliveryLedger {.gcsafe, raises: [].} =
  DeliveryLedger(
    remainingTargets: initHashSet[string](),
    inflightTargets: initHashSet[string](),
    deliveredTargets: initHashSet[string](),
  )

proc deliveryTargetKey(targetPeerId: string, scopePrefix: LsmrPath = @[]): string {.gcsafe, raises: [].} =
  if targetPeerId.len == 0:
    return ""
  if scopePrefix.len == 0:
    return targetPeerId
  targetPeerId & "|" & scopePrefix.pathKey()

proc submitAckKey(itemKey: string, scopePrefix: LsmrPath = @[]): string {.gcsafe, raises: [].} =
  if itemKey.len == 0:
    return ""
  if scopePrefix.len == 0:
    return itemKey
  itemKey & "|" & scopePrefix.pathKey()

proc rotatePendingKeys(
    keys: seq[string], cursor: var int, limit: int
): seq[string] {.gcsafe, raises: [].} =
  if keys.len == 0 or limit <= 0:
    cursor = 0
    return @[]
  let start =
    if cursor <= 0:
      0
    else:
      cursor mod keys.len
  let count = min(limit, keys.len)
  for offset in 0 ..< count:
    result.add(keys[(start + offset) mod keys.len])
  cursor = (start + count) mod keys.len

proc registerDeliveryTarget(
    ledgerBook: var Table[string, DeliveryLedger], itemKey, targetKey: string
): bool {.gcsafe, raises: [].} =
  if itemKey.len == 0 or targetKey.len == 0:
    return false
  if itemKey notin ledgerBook:
    ledgerBook[itemKey] = initDeliveryLedger()
  let ledger = ledgerBook.getOrDefault(itemKey, initDeliveryLedger())
  if targetKey notin ledger.deliveredTargets and targetKey notin ledger.inflightTargets:
    ledgerBook.mgetOrPut(itemKey, initDeliveryLedger()).remainingTargets.incl(targetKey)
    return true
  false

proc beginInflightDelivery(
    ledgerBook: var Table[string, DeliveryLedger], itemKey, targetKey: string
): bool {.gcsafe, raises: [].} =
  if itemKey.len == 0 or targetKey.len == 0:
    return false
  if itemKey notin ledgerBook:
    ledgerBook[itemKey] = initDeliveryLedger()
  let ledger = ledgerBook.getOrDefault(itemKey, initDeliveryLedger())
  if targetKey in ledger.deliveredTargets or targetKey in ledger.inflightTargets:
    return false
  ledgerBook.mgetOrPut(itemKey, initDeliveryLedger()).remainingTargets.excl(targetKey)
  ledgerBook.mgetOrPut(itemKey, initDeliveryLedger()).inflightTargets.incl(targetKey)
  true

proc failInflightDelivery(
    ledgerBook: var Table[string, DeliveryLedger], itemKey, targetKey: string
) {.gcsafe, raises: [].} =
  if itemKey.len == 0 or targetKey.len == 0:
    return
  if itemKey notin ledgerBook:
    ledgerBook[itemKey] = initDeliveryLedger()
  ledgerBook.mgetOrPut(itemKey, initDeliveryLedger()).inflightTargets.excl(targetKey)
  let ledger = ledgerBook.getOrDefault(itemKey, initDeliveryLedger())
  if targetKey notin ledger.deliveredTargets:
    ledgerBook.mgetOrPut(itemKey, initDeliveryLedger()).remainingTargets.incl(targetKey)

proc acknowledgeDelivery(
    ledgerBook: var Table[string, DeliveryLedger], itemKey, targetKey: string
): bool {.gcsafe, raises: [].} =
  if itemKey.len == 0 or targetKey.len == 0:
    return false
  if itemKey notin ledgerBook:
    return true
  ledgerBook.mgetOrPut(itemKey, initDeliveryLedger()).inflightTargets.excl(targetKey)
  ledgerBook.mgetOrPut(itemKey, initDeliveryLedger()).remainingTargets.excl(targetKey)
  ledgerBook.mgetOrPut(itemKey, initDeliveryLedger()).deliveredTargets.incl(targetKey)
  let ledger = ledgerBook.getOrDefault(itemKey, initDeliveryLedger())
  ledger.remainingTargets.len == 0 and ledger.inflightTargets.len == 0

proc clearDeliveryState(
    ledgerBook: var Table[string, DeliveryLedger], itemKey: string
) {.gcsafe, raises: [].} =
  if itemKey.len > 0 and itemKey in ledgerBook:
    ledgerBook.del(itemKey)

proc deliveryTargetMatchesPeer(targetKey, peerIdText: string): bool {.gcsafe, raises: [].} =
  targetKey == peerIdText or targetKey.startsWith(peerIdText & "|")

proc pruneObsoleteDeliveryTargets(
    ledgerBook: var Table[string, DeliveryLedger],
    itemKey: string,
    keepTargets: HashSet[string],
): int {.gcsafe, raises: [].} =
  if itemKey.len == 0 or itemKey notin ledgerBook:
    return 0
  var ledger = ledgerBook.getOrDefault(itemKey, initDeliveryLedger())
  for targetKey in ledger.remainingTargets.toSeq():
    if targetKey notin keepTargets:
      ledger.remainingTargets.excl(targetKey)
      inc result
  for targetKey in ledger.inflightTargets.toSeq():
    if targetKey notin keepTargets:
      ledger.inflightTargets.excl(targetKey)
      inc result
  if result == 0:
    return 0
  if ledger.remainingTargets.len == 0 and ledger.inflightTargets.len == 0 and
      ledger.deliveredTargets.len == 0:
    ledgerBook.del(itemKey)
  else:
    ledgerBook[itemKey] = ledger

proc acknowledgePeerDelivery(
    ledgerBook: var Table[string, DeliveryLedger], itemKey, peerIdText: string
): bool {.gcsafe, raises: [].} =
  if itemKey.len == 0 or peerIdText.len == 0 or itemKey notin ledgerBook:
    return false
  var ledger = ledgerBook.getOrDefault(itemKey, initDeliveryLedger())
  var changed = false
  for targetKey in ledger.remainingTargets.toSeq():
    if deliveryTargetMatchesPeer(targetKey, peerIdText):
      ledger.remainingTargets.excl(targetKey)
      ledger.deliveredTargets.incl(targetKey)
      changed = true
  for targetKey in ledger.inflightTargets.toSeq():
    if deliveryTargetMatchesPeer(targetKey, peerIdText):
      ledger.inflightTargets.excl(targetKey)
      ledger.deliveredTargets.incl(targetKey)
      changed = true
  if not changed:
    return false
  if ledger.remainingTargets.len == 0 and ledger.inflightTargets.len == 0 and
      ledger.deliveredTargets.len == 0:
    ledgerBook.del(itemKey)
  else:
    ledgerBook[itemKey] = ledger
  ledger.remainingTargets.len == 0 and ledger.inflightTargets.len == 0

proc observeGlobalPeerDelivery(
    ledgerBook: var Table[string, DeliveryLedger], itemKey, peerIdText: string
): bool {.gcsafe, raises: [].} =
  if itemKey.len == 0 or peerIdText.len == 0:
    return false
  let deliveredKey = deliveryTargetKey(peerIdText)
  var ledger = ledgerBook.getOrDefault(itemKey, initDeliveryLedger())
  var changed = false
  for targetKey in ledger.remainingTargets.toSeq():
    if deliveryTargetMatchesPeer(targetKey, peerIdText):
      ledger.remainingTargets.excl(targetKey)
      ledger.deliveredTargets.incl(targetKey)
      changed = true
  for targetKey in ledger.inflightTargets.toSeq():
    if deliveryTargetMatchesPeer(targetKey, peerIdText):
      ledger.inflightTargets.excl(targetKey)
      ledger.deliveredTargets.incl(targetKey)
      changed = true
  if deliveredKey notin ledger.deliveredTargets:
    ledger.deliveredTargets.incl(deliveredKey)
    changed = true
  if not changed:
    return false
  ledgerBook[itemKey] = ledger
  true

proc observeInboundPeerDelivery(
    ledgerBook: var Table[string, DeliveryLedger], itemKey, peerIdText: string
): tuple[observed: bool, settledOutstanding: bool] {.gcsafe, raises: [].} =
  if itemKey.len == 0 or peerIdText.len == 0:
    return (observed: false, settledOutstanding: false)
  let hadOutstanding =
    if itemKey in ledgerBook:
      let ledger = ledgerBook.getOrDefault(itemKey, initDeliveryLedger())
      ledger.remainingTargets.len > 0 or ledger.inflightTargets.len > 0
    else:
      false
  let observed = observeGlobalPeerDelivery(ledgerBook, itemKey, peerIdText)
  let hasOutstanding =
    if itemKey in ledgerBook:
      let ledger = ledgerBook.getOrDefault(itemKey, initDeliveryLedger())
      ledger.remainingTargets.len > 0 or ledger.inflightTargets.len > 0
    else:
      false
  (
    observed: observed,
    settledOutstanding: hadOutstanding and not hasOutstanding,
  )

proc observeInboundTargetDelivery(
    ledgerBook: var Table[string, DeliveryLedger], itemKey, targetKey: string
): tuple[observed: bool, settledOutstanding: bool] {.gcsafe, raises: [].} =
  if itemKey.len == 0 or targetKey.len == 0:
    return (observed: false, settledOutstanding: false)
  let hadOutstanding =
    if itemKey in ledgerBook:
      let ledger = ledgerBook.getOrDefault(itemKey, initDeliveryLedger())
      ledger.remainingTargets.len > 0 or ledger.inflightTargets.len > 0
    else:
      false
  if itemKey notin ledgerBook:
    ledgerBook[itemKey] = initDeliveryLedger()
  var ledger = ledgerBook.getOrDefault(itemKey, initDeliveryLedger())
  var observed = false
  if targetKey in ledger.remainingTargets:
    ledger.remainingTargets.excl(targetKey)
    observed = true
  if targetKey in ledger.inflightTargets:
    ledger.inflightTargets.excl(targetKey)
    observed = true
  if targetKey notin ledger.deliveredTargets:
    ledger.deliveredTargets.incl(targetKey)
    observed = true
  if not observed:
    return (observed: false, settledOutstanding: false)
  ledgerBook[itemKey] = ledger
  let hasOutstanding = ledger.remainingTargets.len > 0 or ledger.inflightTargets.len > 0
  (
    observed: true,
    settledOutstanding: hadOutstanding and not hasOutstanding,
  )

proc hasOutstandingDelivery(
    ledgerBook: Table[string, DeliveryLedger], itemKey: string
): bool {.gcsafe, raises: [].} =
  if itemKey.len == 0 or itemKey notin ledgerBook:
    return false
  let ledger = ledgerBook.getOrDefault(itemKey, initDeliveryLedger())
  ledger.remainingTargets.len > 0 or ledger.inflightTargets.len > 0

proc activeDeliveryKeys(
    pendingBook: HashSet[string],
    ledgerBook: Table[string, DeliveryLedger]
): seq[string] {.gcsafe, raises: [].} =
  var seen = initHashSet[string]()
  for key in pendingBook:
    if key.len == 0 or key in seen:
      continue
    seen.incl(key)
    result.add(key)
  for key, ledger in ledgerBook.pairs:
    if key.len == 0 or key in seen:
      continue
    if ledger.remainingTargets.len == 0 and ledger.inflightTargets.len == 0:
      continue
    seen.incl(key)
    result.add(key)
  result.sort(system.cmp[string])

proc hasOutstandingDeliveries(
    ledgerBook: Table[string, DeliveryLedger]
): bool {.gcsafe, raises: [].} =
  for _, ledger in ledgerBook.pairs:
    if ledger.remainingTargets.len > 0 or ledger.inflightTargets.len > 0:
      return true
  false

proc deliveredToPeer(
    ledgerBook: Table[string, DeliveryLedger], itemKey, targetPeerId: string
): bool {.gcsafe, raises: [].} =
  if itemKey.len == 0 or targetPeerId.len == 0 or itemKey notin ledgerBook:
    return false
  let ledger = ledgerBook.getOrDefault(itemKey, initDeliveryLedger())
  for targetKey in ledger.deliveredTargets:
    if targetKey == targetPeerId or targetKey.startsWith(targetPeerId & "|"):
      return true
  return false

proc deliveryActiveToPeer(
    ledgerBook: Table[string, DeliveryLedger], itemKey, targetPeerId: string
): bool {.gcsafe, raises: [].} =
  if deliveredToPeer(ledgerBook, itemKey, targetPeerId):
    return true
  if itemKey.len == 0 or targetPeerId.len == 0 or itemKey notin ledgerBook:
    return false
  let ledger = ledgerBook.getOrDefault(itemKey, initDeliveryLedger())
  for targetKey in ledger.inflightTargets:
    if targetKey == targetPeerId or targetKey.startsWith(targetPeerId & "|"):
      return true
  false

proc inflightDeliveryCountToPeer(
    ledgerBook: Table[string, DeliveryLedger], targetPeerId: string
): int {.gcsafe, raises: [].} =
  if targetPeerId.len == 0:
    return 0
  for _, ledger in ledgerBook.pairs:
    for targetKey in ledger.inflightTargets:
      if deliveryTargetMatchesPeer(targetKey, targetPeerId):
        inc result

proc hasDeliveryTargets(
    ledgerBook: Table[string, DeliveryLedger], itemKey: string
): bool {.gcsafe, raises: [].} =
  if itemKey.len == 0 or itemKey notin ledgerBook:
    return false
  let ledger = ledgerBook.getOrDefault(itemKey, initDeliveryLedger())
  ledger.remainingTargets.len > 0 or
    ledger.inflightTargets.len > 0 or
    ledger.deliveredTargets.len > 0

proc deliveryNeedsAttempt(
    ledgerBook: Table[string, DeliveryLedger], itemKey, targetKey: string
): bool {.gcsafe, raises: [].} =
  if itemKey.len == 0 or targetKey.len == 0:
    return false
  if itemKey notin ledgerBook:
    return true
  let ledger = ledgerBook.getOrDefault(itemKey, initDeliveryLedger())
  targetKey notin ledger.deliveredTargets and targetKey notin ledger.inflightTargets

proc finalizeScopedPending(
    pendingBook: var HashSet[string],
    scopeBook: var Table[string, seq[LsmrPath]],
    key: string,
    remainingScopes: openArray[LsmrPath],
) {.gcsafe, raises: [].} =
  if key.len == 0:
    return
  scopeBook.clearPendingScopePrefixes(key)
  discard pendingBook.enqueuePending(key)
  discard scopeBook.mergeScopePrefixes(key, remainingScopes)

proc finalizeGlobalPending(
    pendingBook: var HashSet[string],
    scopeBook: var Table[string, seq[LsmrPath]],
    key: string,
    deferred: bool,
) {.gcsafe, raises: [].} =
  if key.len == 0:
    return
  scopeBook.clearPendingScopePrefixes(key)
  if deferred:
    discard pendingBook.enqueuePending(key)

proc submitLane(priority: OutboundSubmitPriority): FabricSubmitLane {.gcsafe, raises: [].} =
  case priority
  of ospEvent:
    fslEvent
  of ospAttestation:
    fslAttestation
  of ospOther:
    fslOther

proc outboundRunnerPoolSize(priority: OutboundSubmitPriority): int {.gcsafe, raises: [].} =
  case priority
  of ospEvent:
    1
  of ospAttestation:
    1
  of ospOther:
    1

proc outboundBatchLimit(priority: OutboundSubmitPriority): int {.gcsafe, raises: [].} =
  case priority
  of ospEvent:
    8
  of ospAttestation:
    8
  of ospOther:
    4

proc lsmrSubmitRouteReady(
    node: FabricNode, targetPeerId: string, priority: OutboundSubmitPriority
): bool {.gcsafe, raises: [].} =
  not node.isNil and not node.network.isNil and
    node.network.submitPeerReady(targetPeerId, submitLane(priority))

proc lsmrSubmitRouteReady(node: FabricNode, targetPeerId: string): bool {.gcsafe, raises: [].} =
  not node.isNil and not node.network.isNil and node.network.submitPeerReadyNow(targetPeerId)

proc eventCertificateSubmitCreditAvailable(
    node: FabricNode, targetPeerId: string
): bool {.gcsafe, raises: [].} =
  not node.isNil and targetPeerId.len > 0 and
    inflightDeliveryCountToPeer(node.eventCertificateDeliveries, targetPeerId) <
      outboundBatchLimit(ospOther)

proc ensureLsmrSubmitRoute(
    node: FabricNode, targetPeerId: string, priority: OutboundSubmitPriority
) {.gcsafe, raises: [].} =
  if node.isNil or node.network.isNil or targetPeerId.len == 0:
    return
  node.network.ensureSubmitPeerConnectivity(targetPeerId, submitLane(priority))

proc enableLsmrDataPlane*(node: FabricNode) {.gcsafe, raises: [].} =
  if node.isNil or not node.running or node.network.isNil or not node.usesLsmrPrimary():
    return
  if node.lsmrDataPlaneEnabled:
    return
  node.lsmrDataPlaneEnabled = true
  node.scheduleReadyOutboundSessions()
  node.scheduleMaintenance(immediate = true)

proc submitWarmReady(node: FabricNode): bool {.gcsafe, raises: [].} =
  if node.isNil or not node.running or node.network.isNil or not node.usesLsmrPrimary() or
      not node.lsmrDataPlaneEnabled:
    return false
  let activeCerts = node.network.routingPlaneStatus().lsmrActiveCertificates
  activeCerts >= max(1, node.connectedPeerIds().len + 1)

proc attestationForKey(
    node: FabricNode, key: string
): Option[EventAttestation] {.gcsafe, raises: [].} =
  if node.isNil or key.len == 0:
    return none(EventAttestation)
  let indexKey = attestationIndexKeyFromLookup(key)
  if indexKey.len == 0 or indexKey notin node.attestationsByKey:
    return none(EventAttestation)
  some(node.attestationsByKey.getOrDefault(indexKey))

proc checkpointVoteForKey(
    node: FabricNode, key: string
): Option[CheckpointVote] {.gcsafe, raises: [].} =
  let parts = key.split(":")
  if parts.len != 2:
    return none(CheckpointVote)
  let candidateId = parts[0]
  let validator = parts[1]
  for vote in node.checkpointVotes.getOrDefault(candidateId, @[]):
    if vote.validator == validator:
      return some(vote)
  none(CheckpointVote)

proc clearEventAttestationDeliveries(
    node: FabricNode, eventId: string
) {.gcsafe, raises: [].} =
  for att in node.eventAttestations.getOrDefault(eventId, @[]):
    node.completeAttestationDelivery(att.attestationKey())
  for key in node.pendingAttestations.toSeq():
    if key.startsWith(eventId & ":"):
      node.completeAttestationDelivery(key)
  for key in node.routeBlockedAttestations.toSeq():
    if key.startsWith(eventId & ":"):
      node.completeAttestationDelivery(key)

proc completeEventDelivery(
    node: FabricNode, eventId: string
) {.gcsafe, raises: [].} =
  if node.isNil or eventId.len == 0:
    return
  node.pendingEvents.excl(eventId)
  node.pendingEventScopes.clearPendingScopePrefixes(eventId)
  clearDeliveryState(node.eventDeliveries, eventId)
  node.noteLsmrDeliveryMutation()
  if eventId in node.forwardedEvents:
    node.forwardedEvents.del(eventId)

proc completeAttestationDelivery(
    node: FabricNode, attestationKey: string
) {.gcsafe, raises: [].} =
  if node.isNil or attestationKey.len == 0:
    return
  node.pendingAttestations.excl(attestationKey)
  node.routeBlockedAttestations.excl(attestationKey)
  node.pendingAttestationScopes.clearPendingScopePrefixes(attestationKey)
  clearDeliveryState(node.attestationDeliveries, attestationKey)
  node.noteLsmrDeliveryMutation()
  if attestationKey in node.forwardedAttestations:
    node.forwardedAttestations.del(attestationKey)

proc completeEventCertificateDelivery(
    node: FabricNode, eventId: string
) {.gcsafe, raises: [].} =
  if node.isNil or eventId.len == 0:
    return
  node.dropPendingEventCertificate(eventId, clearScopes = true)
  clearDeliveryState(node.eventCertificateDeliveries, eventId)
  node.noteLsmrDeliveryMutation()
  if eventId in node.forwardedEventCertificates:
    node.forwardedEventCertificates.del(eventId)

proc pruneCheckpointedEventCertificateDeliveries(node: FabricNode) {.gcsafe, raises: [].} =
  if node.isNil or not node.usesLsmrPrimary():
    return
  let checkpointKnown =
    node.latestCheckpointId.len > 0 and node.latestCheckpointId in node.checkpoints
  if not checkpointKnown:
    return
  let checkpointEra = node.checkpoints.getOrDefault(node.latestCheckpointId).candidate.era
  for eventId in node.eventCertificateDeliveries.keys.toSeq():
    let ledger = node.eventCertificateDeliveries.getOrDefault(eventId, initDeliveryLedger())
    if ledger.remainingTargets.len == 0 and ledger.inflightTargets.len == 0:
      continue
    if eventId notin node.events or eventId notin node.eventCertificates or
        eventId in node.isolatedEvents:
      node.completeEventCertificateDelivery(eventId)
      continue
    if node.events.getOrDefault(eventId).clock.era < checkpointEra:
      node.completeEventCertificateDelivery(eventId)

proc completeCheckpointCandidateDelivery(
    node: FabricNode, candidateId: string
) {.gcsafe, raises: [].} =
  if node.isNil or candidateId.len == 0:
    return
  node.pendingCheckpointCandidates.excl(candidateId)
  clearDeliveryState(node.checkpointCandidateDeliveries, candidateId)
  if candidateId in node.forwardedCheckpointCandidates:
    node.forwardedCheckpointCandidates.del(candidateId)

proc completeCheckpointVoteDelivery(
    node: FabricNode, voteKey: string
) {.gcsafe, raises: [].} =
  if node.isNil or voteKey.len == 0:
    return
  node.pendingCheckpointVotes.excl(voteKey)
  clearDeliveryState(node.checkpointVoteDeliveries, voteKey)
  if voteKey in node.forwardedCheckpointVotes:
    node.forwardedCheckpointVotes.del(voteKey)

proc completeCheckpointBundleDelivery(
    node: FabricNode, checkpointId: string
) {.gcsafe, raises: [].} =
  if node.isNil or checkpointId.len == 0:
    return
  node.pendingCheckpointBundles.excl(checkpointId)
  clearDeliveryState(node.checkpointBundleDeliveries, checkpointId)
  if checkpointId in node.forwardedCheckpointBundles:
    node.forwardedCheckpointBundles.del(checkpointId)

proc pruneSupersededCheckpointBundles(
    node: FabricNode, keepCheckpointId: string
) {.gcsafe, raises: [].} =
  if node.isNil:
    return
  for checkpointId in node.pendingCheckpointBundles.toSeq():
    if checkpointId != keepCheckpointId:
      node.completeCheckpointBundleDelivery(checkpointId)
  for checkpointId in node.checkpointBundleDeliveries.keys.toSeq():
    if checkpointId != keepCheckpointId:
      node.completeCheckpointBundleDelivery(checkpointId)
  for checkpointId in node.forwardedCheckpointBundles.keys.toSeq():
    if checkpointId != keepCheckpointId:
      node.completeCheckpointBundleDelivery(checkpointId)

proc clearCheckpointVotePending(
    node: FabricNode, candidateId: string
) {.gcsafe, raises: [].} =
  for key in node.pendingCheckpointVotes.toSeq():
    if key.startsWith(candidateId & ":"):
      node.pendingCheckpointVotes.excl(key)

proc forwardedContains(
    book: Table[string, HashSet[string]], itemKey, peerIdText: string
): bool {.gcsafe, raises: [].} =
  if itemKey.len == 0 or peerIdText.len == 0:
    return false
  let peers = book.getOrDefault(itemKey, initHashSet[string]())
  peerIdText in peers

proc markForwarded(
    book: var Table[string, HashSet[string]], itemKey, peerIdText: string
) {.gcsafe, raises: [].} =
  if itemKey.len == 0 or peerIdText.len == 0:
    return
  var peers = book.getOrDefault(itemKey, initHashSet[string]())
  peers.incl(peerIdText)
  book[itemKey] = peers

proc clearForwarded(
    book: var Table[string, HashSet[string]], itemKey, peerIdText: string
) {.gcsafe, raises: [].} =
  if itemKey.len == 0 or peerIdText.len == 0 or itemKey notin book:
    return
  var peers = book.getOrDefault(itemKey, initHashSet[string]())
  peers.excl(peerIdText)
  if peers.len == 0:
    book.del(itemKey)
  else:
    book[itemKey] = peers

proc clearForwardedPeer(
    book: var Table[string, HashSet[string]], itemKey, peerIdText: string
) {.gcsafe, raises: [].} =
  if itemKey.len == 0 or peerIdText.len == 0 or itemKey notin book:
    return
  var peers = book.getOrDefault(itemKey, initHashSet[string]())
  for targetKey in peers.toSeq():
    if deliveryTargetMatchesPeer(targetKey, peerIdText):
      peers.excl(targetKey)
  if peers.len == 0:
    book.del(itemKey)
  else:
    book[itemKey] = peers

proc getOrCreateOutboundSession(node: FabricNode, targetPeerId: string): OutboundPeerSession =
  result = node.outboundSessions.getOrDefault(targetPeerId)
  if result.isNil:
    result = OutboundPeerSession(
      eventQueue: initDeque[OutboundSubmit](),
      attestationQueue: initDeque[OutboundSubmit](),
      otherQueue: initDeque[OutboundSubmit](),
      runners: [
        newSeq[Future[void]](outboundRunnerPoolSize(ospEvent)),
        newSeq[Future[void]](outboundRunnerPoolSize(ospAttestation)),
        newSeq[Future[void]](outboundRunnerPoolSize(ospOther)),
      ],
    )
    node.outboundSessions[targetPeerId] = result

proc hasPending(session: OutboundPeerSession): bool {.gcsafe, raises: [].} =
  not session.isNil and
    (session.eventQueue.len > 0 or session.attestationQueue.len > 0 or session.otherQueue.len > 0)

proc hasPending(
    session: OutboundPeerSession, priority: OutboundSubmitPriority
): bool {.gcsafe, raises: [].} =
  if session.isNil:
    return false
  case priority
  of ospEvent:
    session.eventQueue.len > 0
  of ospAttestation:
    session.attestationQueue.len > 0
  of ospOther:
    session.otherQueue.len > 0

proc hasPendingLocalAsyncWork(node: FabricNode): bool {.gcsafe, raises: [].} =
  if node.isNil or not node.running:
    return false
  if node.inbound.len > 0 or node.inboundPending or node.maintenancePending or
      node.maintenanceImmediatePending:
    return true
  for _, session in node.outboundSessions.pairs:
    if session.hasPending():
      return true
    for priority in low(OutboundSubmitPriority) .. high(OutboundSubmitPriority):
      for runner in session.runners[priority]:
        if not runner.isNil and not runner.finished():
          return true
  false

proc hasRunning(
    session: OutboundPeerSession, priority: OutboundSubmitPriority
): bool {.gcsafe, raises: [].} =
  if session.isNil:
    return false
  for runner in session.runners[priority]:
    if not runner.isNil and not runner.finished():
      return true
  false

proc peerOutboundBusy(
    node: FabricNode, targetPeerId: string
): bool {.gcsafe, raises: [].} =
  if node.isNil or targetPeerId.len == 0:
    return false
  let session = node.outboundSessions.getOrDefault(targetPeerId)
  if session.isNil:
    return false
  if session.hasPending():
    return true
  for priority in low(OutboundSubmitPriority) .. high(OutboundSubmitPriority):
    if session.hasRunning(priority):
      return true
  false

proc otherOutboundOwnerActive(node: FabricNode): bool {.gcsafe, raises: [].} =
  if node.isNil:
    return false
  for _, session in node.outboundSessions.pairs:
    if session.hasRunning(ospOther):
      return true
  false

proc pendingOtherPeerIds(node: FabricNode): seq[string] {.gcsafe, raises: [].} =
  if node.isNil:
    return @[]
  for targetPeerId, session in node.outboundSessions.pairs:
    if session.hasPending(ospOther):
      result.add(targetPeerId)
  result.sort(system.cmp[string])

proc nextOtherOutboundPeer(node: FabricNode, afterPeerId = ""): string {.gcsafe, raises: [].} =
  let peerIds = node.pendingOtherPeerIds()
  if peerIds.len == 0:
    return ""
  var start = 0
  if afterPeerId.len > 0:
    for idx, peerId in peerIds:
      if peerId == afterPeerId:
        start = (idx + 1) mod peerIds.len
        break
  for offset in 0 ..< peerIds.len:
    let peerId = peerIds[(start + offset) mod peerIds.len]
    let session = node.outboundSessions.getOrDefault(peerId)
    if session.isNil or not session.hasPending(ospOther):
      continue
    if not session.blockedOnReady[ospOther] or
        not node.usesLsmrPrimary() or
        node.lsmrSubmitRouteReady(peerId, ospOther):
      return peerId
  ""

proc popNext(session: OutboundPeerSession): OutboundSubmit {.gcsafe, raises: [].} =
  if session.isNil:
    return nil
  if session.eventQueue.len > 0:
    return session.eventQueue.popFirst()
  if session.attestationQueue.len > 0:
    return session.attestationQueue.popFirst()
  if session.otherQueue.len > 0:
    return session.otherQueue.popFirst()
  nil

proc popBatch(
    node: FabricNode, session: OutboundPeerSession, priority: OutboundSubmitPriority
): seq[OutboundSubmit] {.gcsafe, raises: [].} =
  if node.isNil or session.isNil:
    return @[]
  let batchLimit = outboundBatchLimit(priority)
  case priority
  of ospEvent:
    while session.eventQueue.len > 0 and result.len < batchLimit:
      result.add(session.eventQueue.popFirst())
  of ospAttestation:
    while session.attestationQueue.len > 0 and result.len < batchLimit:
      result.add(session.attestationQueue.popFirst())
  of ospOther:
    if session.otherQueue.len == 0:
      return @[]
    result.add(session.otherQueue.popFirst())
    if result[0].submitKind == oskEventCertificate:
      while session.otherQueue.len > 0 and
          result.len < batchLimit and
          session.otherQueue.peekFirst().submitKind == oskEventCertificate:
        result.add(session.otherQueue.popFirst())

proc pushBatchFront(
    session: OutboundPeerSession, items: openArray[OutboundSubmit]
) {.gcsafe, raises: [].} =
  if session.isNil or items.len == 0:
    return
  for idx in countdown(items.high, 0):
    let item = items[idx]
    case item.priority
    of ospEvent:
      session.eventQueue.addFirst(item)
    of ospAttestation:
      session.attestationQueue.addFirst(item)
    of ospOther:
      session.otherQueue.addFirst(item)

proc runOutboundSession(
    node: FabricNode, targetPeerId: string, priority: OutboundSubmitPriority, runnerIndex: int
): Future[void] {.async: (raises: []).}

proc scheduleNextOtherOutbound(node: FabricNode, afterPeerId = "") {.gcsafe, raises: [].} =
  if node.isNil or not node.running or node.otherOutboundOwnerActive():
    return
  let targetPeerId = node.nextOtherOutboundPeer(afterPeerId)
  if targetPeerId.len == 0:
    return
  let session = node.outboundSessions.getOrDefault(targetPeerId)
  if session.isNil or session.runners[ospOther].len == 0:
    return
  if not session.runners[ospOther][0].isNil and not session.runners[ospOther][0].finished():
    return
  session.runners[ospOther][0] = node.runOutboundSession(targetPeerId, ospOther, 0)
  asyncSpawn session.runners[ospOther][0]

proc scheduleOutboundSession(
    node: FabricNode, targetPeerId: string, priority: OutboundSubmitPriority
) {.gcsafe, raises: [].} =
  if node.isNil or not node.running:
    return
  if priority == ospOther:
    node.scheduleNextOtherOutbound()
    return
  if targetPeerId.len == 0:
    return

  proc launch(udata: pointer) {.gcsafe, raises: [].} =
    let node = cast[FabricNode](udata)
    if node.isNil or not node.running:
      return
    let session = node.outboundSessions.getOrDefault(targetPeerId)
    if session.isNil or not session.hasPending(priority):
      return
    if session.blockedOnReady[priority] and node.usesLsmrPrimary() and
        (node.network.isNil or not node.network.submitPeerReady(targetPeerId, submitLane(priority))):
      return
    session.blockedOnReady[priority] = false
    for idx in 0 ..< session.runners[priority].len:
      if not session.hasPending(priority):
        break
      if not session.runners[priority][idx].isNil and not session.runners[priority][idx].finished():
        continue
      let runnerIndex = idx
      session.runners[priority][runnerIndex] =
        node.runOutboundSession(targetPeerId, priority, runnerIndex)
      asyncSpawn session.runners[priority][runnerIndex]

  callSoon(launch, cast[pointer](node))
proc runOutboundSession(
    node: FabricNode, targetPeerId: string, priority: OutboundSubmitPriority, runnerIndex: int
): Future[void] {.async: (raises: []).} =
  if node.isNil:
    return
  let session = node.getOrCreateOutboundSession(targetPeerId)
  let singleBatchOwner = priority == ospOther
  while node.running and session.hasPending(priority):
    if node.usesLsmrPrimary() and
        (node.network.isNil or not node.network.submitPeerReady(targetPeerId, submitLane(priority))):
      session.blockedOnReady[priority] = true
      node.ensureLsmrSubmitRoute(targetPeerId, priority)
      when defined(fabric_submit_diag):
        echo "submit-track sender=", node.identity.account,
          " target=", targetPeerId,
          " lane=", $ord(priority),
          " blocked=not-ready"
      break
    session.blockedOnReady[priority] = false
    let items = node.popBatch(session, priority)
    if items.len == 0:
      break
    let item = items[0]
    try:
      var acceptedItems: seq[bool] = @[]
      let accepted =
        case item.submitKind
        of oskProc:
          let ok = await item.submit()
          acceptedItems = @[ok]
          ok
        of oskEvent:
          var batch: seq[tuple[itemKey: string, event: FabricEvent, scopePrefix: LsmrPath]] = @[]
          for batchItem in items:
            batch.add((
              itemKey: batchItem.itemKey,
              event: batchItem.event,
              scopePrefix: batchItem.scopePrefix,
            ))
          acceptedItems = await node.network.submitEventBatch(item.targetPeerId, batch)
          acceptedItems.len == batch.len
        of oskAttestation:
          var batch: seq[tuple[itemKey: string, attestation: EventAttestation, scopePrefix: LsmrPath]] = @[]
          for batchItem in items:
            batch.add((
              itemKey: batchItem.itemKey,
              attestation: batchItem.attestation,
              scopePrefix: batchItem.scopePrefix,
            ))
          acceptedItems = await node.network.submitAttestationBatch(item.targetPeerId, batch)
          acceptedItems.len == batch.len
        of oskEventCertificate:
          var batch: seq[tuple[itemKey: string, eventCertificate: EventCertificate, scopePrefix: LsmrPath]] = @[]
          for batchItem in items:
            batch.add((
              itemKey: batchItem.itemKey,
              eventCertificate: batchItem.eventCertificate,
              scopePrefix: batchItem.scopePrefix,
            ))
          acceptedItems = await node.network.submitEventCertificateBatch(item.targetPeerId, batch)
          acceptedItems.len == batch.len
      if node.usesLsmrPrimary() and acceptedItems.len == 0 and
          (node.network.isNil or not node.network.submitPeerReady(item.targetPeerId, submitLane(priority))):
        for batchItem in items:
          if not batchItem.onFailure.isNil:
            batchItem.onFailure()
        session.pushBatchFront(items)
        session.blockedOnReady[priority] = true
        node.ensureLsmrSubmitRoute(item.targetPeerId, priority)
        when defined(fabric_submit_diag):
          echo "submit-track sender=", node.identity.account,
            " item=", item.itemKey,
            " target=", item.targetPeerId,
            " lane=", $ord(priority),
            " batchItems=", items.len,
            " blocked=not-ready-requeue"
        break
      when defined(fabric_submit_diag):
        echo "submit-track sender=", node.identity.account,
          " item=", item.itemKey,
          " target=", item.targetPeerId,
          " lane=", $ord(priority),
          " batchItems=", items.len,
          " acked=", acceptedItems.countIt(it),
          "/",
          items.len,
          " accepted=", accepted
      for idx, batchItem in items:
        let batchAccepted = idx < acceptedItems.len and acceptedItems[idx]
        if batchAccepted:
          if not batchItem.onSuccess.isNil:
            batchItem.onSuccess()
        else:
          if not batchItem.onFailure.isNil:
            batchItem.onFailure()
    except CatchableError:
      if node.usesLsmrPrimary() and
          (node.network.isNil or not node.network.submitPeerReady(item.targetPeerId, submitLane(priority))):
        for batchItem in items:
          if not batchItem.onFailure.isNil:
            batchItem.onFailure()
        session.pushBatchFront(items)
        session.blockedOnReady[priority] = true
        node.ensureLsmrSubmitRoute(item.targetPeerId, priority)
        when defined(fabric_submit_diag):
          echo "submit-track sender=", node.identity.account,
            " item=", item.itemKey,
            " target=", item.targetPeerId,
            " lane=", $ord(priority),
            " batchItems=", items.len,
            " blocked=exception-requeue"
        break
      when defined(fabric_submit_diag):
        echo "submit-track sender=", node.identity.account,
          " item=", item.itemKey,
          " target=", item.targetPeerId,
          " lane=", $ord(priority),
          " batchItems=", items.len,
          " accepted=false exception=true"
      for batchItem in items:
        if not batchItem.onFailure.isNil:
          batchItem.onFailure()
    if singleBatchOwner:
      break
    if session.hasPending(priority):
      try:
        await sleepAsync(FabricOutboundYield)
      except CancelledError:
        break
  let current = node.outboundSessions.getOrDefault(targetPeerId)
  if current.isNil:
    return
  if runnerIndex >= 0 and runnerIndex < current.runners[priority].len:
    current.runners[priority][runnerIndex] = nil
  if priority == ospOther:
    node.scheduleNextOtherOutbound(targetPeerId)
    if not current.hasPending() and
        not current.hasRunning(ospEvent) and
        not current.hasRunning(ospAttestation) and
        not current.hasRunning(ospOther):
      node.outboundSessions.del(targetPeerId)
    return
  if current.hasPending(priority):
    if current.blockedOnReady[priority] and node.usesLsmrPrimary() and
        (node.network.isNil or not node.network.submitPeerReady(targetPeerId, submitLane(priority))):
      return
    node.scheduleOutboundSession(targetPeerId, priority)
    return
  if not current.hasPending() and
      not current.hasRunning(ospEvent) and
      not current.hasRunning(ospAttestation) and
      not current.hasRunning(ospOther):
    node.outboundSessions.del(targetPeerId)

proc enqueueOutbound(
    node: FabricNode,
    itemKey: string,
    targetPeerId: string,
    priority: OutboundSubmitPriority,
    submit: proc(): Future[bool] {.closure, gcsafe, raises: [].},
    onSuccess: proc() {.closure, gcsafe, raises: [].},
    onFailure: proc() {.closure, gcsafe, raises: [].},
) =
  if node.isNil or not node.running or targetPeerId.len == 0:
    return
  let session = node.getOrCreateOutboundSession(targetPeerId)
  let item = OutboundSubmit(
    itemKey: itemKey,
    targetPeerId: targetPeerId,
    priority: priority,
    scopePrefix: @[],
    submitKind: oskProc,
    submit: submit,
    onSuccess: onSuccess,
    onFailure: onFailure,
  )
  case priority
  of ospEvent:
    session.eventQueue.addLast(item)
  of ospAttestation:
    session.attestationQueue.addLast(item)
  of ospOther:
    session.otherQueue.addLast(item)
  node.scheduleOutboundSession(targetPeerId, priority)

proc bufferDeferredAttestation(node: FabricNode, att: EventAttestation): bool =
  if node.isNil or att.eventId.len == 0:
    return false
  var existing = node.deferredAttestations.getOrDefault(att.eventId, @[])
  if existing.anyIt(it.attestationKey() == att.attestationKey()):
    return false
  existing.add(att)
  node.deferredAttestations[att.eventId] = existing
  true

proc bufferDeferredEventCertificate(node: FabricNode, cert: EventCertificate): bool =
  if node.isNil or cert.eventId.len == 0:
    return false
  var existing = node.deferredEventCertificates.getOrDefault(cert.eventId, @[])
  if existing.anyIt(it.certificateId == cert.certificateId):
    return false
  existing.add(cert)
  node.deferredEventCertificates[cert.eventId] = existing
  true

proc replayDeferredAttestations(node: FabricNode, eventId: string)
proc replayDeferredEventCertificates(node: FabricNode, eventId: string)
proc registerEventCertificate(
    node: FabricNode, cert: EventCertificate, enqueuePending = true
): bool
proc activateDependentDeliveries(node: FabricNode, eventId: string) {.gcsafe, raises: [].}
proc maybeActivateDependentDeliveries(node: FabricNode, eventId: string)

proc enqueueEventOutbound(
    node: FabricNode,
    itemKey: string,
    targetPeerId: string,
    scopePrefix: LsmrPath,
    event: FabricEvent,
    onSuccess: proc() {.closure, gcsafe, raises: [].},
    onFailure: proc() {.closure, gcsafe, raises: [].},
) =
  if node.isNil or not node.running or targetPeerId.len == 0:
    return
  let session = node.getOrCreateOutboundSession(targetPeerId)
  let item = OutboundSubmit(
    itemKey: itemKey,
    targetPeerId: targetPeerId,
    priority: ospEvent,
    scopePrefix: scopePrefix,
    submitKind: oskEvent,
    event: event,
    onSuccess: onSuccess,
    onFailure: onFailure,
  )
  session.eventQueue.addLast(item)
  node.scheduleOutboundSession(targetPeerId, ospEvent)

proc enqueueAttestationOutbound(
    node: FabricNode,
    itemKey: string,
    targetPeerId: string,
    scopePrefix: LsmrPath,
    attestation: EventAttestation,
    onSuccess: proc() {.closure, gcsafe, raises: [].},
    onFailure: proc() {.closure, gcsafe, raises: [].},
) =
  if node.isNil or not node.running or targetPeerId.len == 0:
    return
  let session = node.getOrCreateOutboundSession(targetPeerId)
  let item = OutboundSubmit(
    itemKey: itemKey,
    targetPeerId: targetPeerId,
    priority: ospAttestation,
    scopePrefix: scopePrefix,
    submitKind: oskAttestation,
    attestation: attestation,
    onSuccess: onSuccess,
    onFailure: onFailure,
  )
  session.attestationQueue.addLast(item)
  node.scheduleOutboundSession(targetPeerId, ospAttestation)

proc enqueueEventCertificateOutbound(
    node: FabricNode,
    itemKey: string,
    targetPeerId: string,
    scopePrefix: LsmrPath,
    eventCertificate: EventCertificate,
    onSuccess: proc() {.closure, gcsafe, raises: [].},
    onFailure: proc() {.closure, gcsafe, raises: [].},
) =
  if node.isNil or not node.running or targetPeerId.len == 0:
    return
  let session = node.getOrCreateOutboundSession(targetPeerId)
  let item = OutboundSubmit(
    itemKey: itemKey,
    targetPeerId: targetPeerId,
    priority: ospOther,
    scopePrefix: scopePrefix,
    submitKind: oskEventCertificate,
    eventCertificate: eventCertificate,
    onSuccess: onSuccess,
    onFailure: onFailure,
  )
  session.otherQueue.addLast(item)
  node.scheduleOutboundSession(targetPeerId, ospOther)

template submitTracked(
    node: FabricNode,
    book: untyped,
    pendingBook: untyped,
    itemKey: string,
    targetPeerId: string,
    submitProc: untyped,
) =
  if targetPeerId.len > 0 and not node.book.forwardedContains(itemKey, targetPeerId):
    node.book.markForwarded(itemKey, targetPeerId)
    proc handleFailure() {.gcsafe, raises: [].} =
      node.book.clearForwarded(itemKey, targetPeerId)
      if node.pendingBook.enqueuePending(itemKey):
        node.scheduleStateMaintenance()
    node.enqueueOutbound(
      itemKey,
      targetPeerId,
      ospOther,
      submitProc,
      nil,
      handleFailure,
    )

proc makeEventSubmitProc(
    network: FabricNetwork,
    targetPeerId: string,
    item: FabricEvent,
    scopePrefix: LsmrPath,
): proc(): Future[bool] {.closure, gcsafe, raises: [].} =
  let capturedNetwork = network
  let capturedTargetPeerId = targetPeerId
  let capturedItem = item
  let capturedScopePrefix = scopePrefix
  result = proc(): Future[bool] {.closure, gcsafe, raises: [].} =
    capturedNetwork.submitEvent(
      capturedTargetPeerId,
      capturedItem,
      scopePrefix = capturedScopePrefix,
    )

proc makeAttestationSubmitProc(
    network: FabricNetwork,
    targetPeerId: string,
    item: EventAttestation,
    scopePrefix: LsmrPath,
): proc(): Future[bool] {.closure, gcsafe, raises: [].} =
  let capturedNetwork = network
  let capturedTargetPeerId = targetPeerId
  let capturedItem = item
  let capturedScopePrefix = scopePrefix
  result = proc(): Future[bool] {.closure, gcsafe, raises: [].} =
    capturedNetwork.submitAttestation(
      capturedTargetPeerId,
      capturedItem,
      scopePrefix = capturedScopePrefix,
    )

proc makeEventCertificateSubmitProc(
    network: FabricNetwork,
    targetPeerId: string,
    item: EventCertificate,
    scopePrefix: LsmrPath,
): proc(): Future[bool] {.closure, gcsafe, raises: [].} =
  let capturedNetwork = network
  let capturedTargetPeerId = targetPeerId
  let capturedItem = item
  let capturedScopePrefix = scopePrefix
  result = proc(): Future[bool] {.closure, gcsafe, raises: [].} =
    capturedNetwork.submitEventCertificate(
      capturedTargetPeerId,
      capturedItem,
      scopePrefix = capturedScopePrefix,
    )

proc makeCheckpointCandidateSubmitProc(
    network: FabricNetwork,
    targetPeerId: string,
    item: CheckpointCandidate,
): proc(): Future[bool] {.closure, gcsafe, raises: [].} =
  let capturedNetwork = network
  let capturedTargetPeerId = targetPeerId
  let capturedItem = item
  result = proc(): Future[bool] {.closure, gcsafe, raises: [].} =
    capturedNetwork.submitCheckpointCandidate(capturedTargetPeerId, capturedItem)

proc makeCheckpointVoteSubmitProc(
    network: FabricNetwork,
    targetPeerId: string,
    item: CheckpointVote,
): proc(): Future[bool] {.closure, gcsafe, raises: [].} =
  let capturedNetwork = network
  let capturedTargetPeerId = targetPeerId
  let capturedItem = item
  result = proc(): Future[bool] {.closure, gcsafe, raises: [].} =
    capturedNetwork.submitCheckpointVote(capturedTargetPeerId, capturedItem)

proc makeCheckpointBundleSubmitProc(
    network: FabricNetwork,
    targetPeerId: string,
    item: CheckpointBundle,
): proc(): Future[bool] {.closure, gcsafe, raises: [].} =
  let capturedNetwork = network
  let capturedTargetPeerId = targetPeerId
  let capturedItem = item
  result = proc(): Future[bool] {.closure, gcsafe, raises: [].} =
    capturedNetwork.submitCheckpointBundle(capturedTargetPeerId, capturedItem)

proc makeAvoProposalSubmitProc(
    network: FabricNetwork,
    targetPeerId: string,
    item: AvoProposal,
): proc(): Future[bool] {.closure, gcsafe, raises: [].} =
  let capturedNetwork = network
  let capturedTargetPeerId = targetPeerId
  let capturedItem = item
  result = proc(): Future[bool] {.closure, gcsafe, raises: [].} =
    capturedNetwork.submitAvoProposal(capturedTargetPeerId, capturedItem)

proc makeAvoApprovalSubmitProc(
    network: FabricNetwork,
    targetPeerId: string,
    proposalId, validator: string,
): proc(): Future[bool] {.closure, gcsafe, raises: [].} =
  let capturedNetwork = network
  let capturedTargetPeerId = targetPeerId
  let capturedProposalId = proposalId
  let capturedValidator = validator
  result = proc(): Future[bool] {.closure, gcsafe, raises: [].} =
    capturedNetwork.submitAvoApproval(
      capturedTargetPeerId,
      capturedProposalId,
      capturedValidator,
    )

proc bytesOf(value: string): seq[byte] =
  result = newSeq[byte](value.len)
  if value.len > 0:
    copyMem(addr result[0], unsafeAddr value[0], value.len)

proc stringOf(data: openArray[byte]): string =
  result = newString(data.len)
  if data.len > 0:
    copyMem(addr result[0], unsafeAddr data[0], data.len)

proc checkpointRaw(cert: CheckpointCertificate): seq[byte] =
  bytesOf(encodeObj(cert))

proc cachedCheckpointBundle(
    node: FabricNode, cert: CheckpointCertificate, snapshot: ChainStateSnapshot
): CheckpointBundle =
  if not node.isNil and cert.checkpointId in node.checkpointBundleCache:
    return node.checkpointBundleCache[cert.checkpointId]
  result = CheckpointBundle(
    certificate: cert,
    snapshot: encodePolarSnapshot(snapshot),
  )
  if not node.isNil and cert.checkpointId.len > 0:
    node.checkpointBundleCache[cert.checkpointId] = result

proc eventById(node: FabricNode, eventId: string): Option[FabricEvent] =
  if eventId.len > 0 and node.events.hasKey(eventId):
    return some(node.events[eventId])
  none(FabricEvent)

proc checkpointById(node: FabricNode, checkpointId: string): Option[CheckpointCertificate] =
  if checkpointId.len > 0 and node.checkpoints.hasKey(checkpointId):
    return some(node.checkpoints[checkpointId])
  none(CheckpointCertificate)

proc componentNodes(start: FabricNode): seq[FabricNode] =
  var visited = initHashSet[string]()
  var pending: seq[FabricNode] = @[start]
  while pending.len > 0:
    let node = pending.pop()
    if node.identity.account in visited:
      continue
    visited.incl(node.identity.account)
    result.add(node)
    for _, peer in node.peers.pairs:
      if peer.identity.account notin visited:
        pending.add(peer)

proc isNeighbor(a, b: LsmrPath): bool =
  if a.len == 0 or b.len == 0:
    return false
  let levels = min(a.len, b.len)
  lsmrDistance(a, b) <= levels * 2

proc routeScope(node: FabricNode, event: FabricEvent, component: seq[FabricNode]): seq[FabricNode] =
  let witnessAccounts = event.witnessRoutes.mapIt(it.account).filterIt(it.len > 0)
  let accounts = sortedUnique(event.participants & witnessAccounts & event.witnessSet)
  for peer in component:
    if peer.identity.account in accounts:
      result.add(peer)
  if result.len == 0:
    result = @[node]

proc selfRoutingPeer(node: FabricNode): RoutingPeer =
  var addrs: seq[string] = @[]
  if not node.network.isNil and not node.network.switch.isNil and not node.network.switch.peerInfo.isNil:
    for addr in node.network.switch.peerInfo.addrs:
      addrs.add($addr)
  elif node.config.listenAddrs.len > 0:
    addrs = node.config.listenAddrs
  RoutingPeer(
    account: node.identity.account,
    peerId: peerIdString(node.identity.peerId),
    path: node.config.lsmrPath,
    addrs: normalizeRoutingAddrs(addrs),
  )

proc routeAddrs(node: FabricNode, peerIdText: string): seq[string] =
  if node.isNil or node.network.isNil or node.network.switch.isNil or peerIdText.len == 0:
    return @[]
  let peerId = PeerId.init(peerIdText).valueOr:
    return @[]
  for addr in node.network.switch.peerStore.getAddresses(peerId):
    result.add($addr)
  result = normalizeRoutingAddrs(result)

proc routeFromActiveCertificate(
    node: FabricNode, account, peerIdText: string
): Option[RoutingPeer] =
  if node.isNil or node.network.isNil or node.network.switch.isNil or peerIdText.len == 0:
    return none(RoutingPeer)
  let peerId = PeerId.init(peerIdText).valueOr:
    return none(RoutingPeer)
  if not node.network.switch.peerStore[ActiveLsmrBook].contains(peerId):
    return none(RoutingPeer)
  let record = node.network.switch.peerStore[ActiveLsmrBook][peerId]
  some(RoutingPeer(
    account: account,
    peerId: peerIdText,
    path: record.data.certifiedPrefix(),
    addrs: node.routeAddrs(peerIdText),
  ))

proc routingPeerForAccount(node: FabricNode, account: string): Option[RoutingPeer] =
  if account.len == 0:
    return none(RoutingPeer)
  if account == node.identity.account:
    return some(node.selfRoutingPeer())
  if account in node.routingPeers:
    let route = node.routingPeers.getOrDefault(account)
    if route.peerId.len > 0:
      let certified = node.routeFromActiveCertificate(account, route.peerId)
      if certified.isSome():
        return certified
    if route.peerId.len > 0 and route.path.len > 0:
      return some(route)
  if node.liveState.validators.hasKey(account):
    let info = node.liveState.validators.getOrDefault(account)
    if info.peerId.len > 0:
      return node.routeFromActiveCertificate(account, info.peerId)
  none(RoutingPeer)

proc routingPeerForPeerId(node: FabricNode, peerIdText: string): Option[RoutingPeer] =
  if peerIdText.len == 0:
    return none(RoutingPeer)
  if peerIdText == peerIdString(node.identity.peerId):
    return some(node.selfRoutingPeer())
  for account, route in node.routingPeers.pairs:
    if route.peerId == peerIdText:
      let certified = node.routeFromActiveCertificate(account, peerIdText)
      if certified.isSome():
        return certified
      if route.path.len > 0:
        return some(route)
      return none(RoutingPeer)
  for account, info in node.liveState.validators.pairs:
    if info.peerId == peerIdText:
      return node.routeFromActiveCertificate(account, peerIdText)
  node.routeFromActiveCertificate("", peerIdText)

proc routeMatchesSelf(node: FabricNode, route: RoutingPeer): bool =
  route.peerId == peerIdString(node.identity.peerId) or
    (route.account.len > 0 and route.account == node.identity.account)

proc usesLsmrPrimary(node: FabricNode): bool {.gcsafe, raises: [].} =
  not node.isNil and node.config.primaryPlane == PrimaryRoutingPlane.lsmr

proc noteLsmrDeliveryMutation(node: FabricNode) {.gcsafe, raises: [].} =
  if node.isNil or not node.usesLsmrPrimary():
    return
  inc node.lsmrDeliveryGeneration

proc scheduleStateMaintenance(node: FabricNode) {.gcsafe, raises: [].} =
  if node.isNil:
    return
  node.scheduleMaintenance(immediate = node.usesLsmrPrimary())

proc scheduleCertificationGather(node: FabricNode) {.gcsafe, raises: [].} =
  if node.isNil or not node.usesLsmrPrimary():
    return
  let wasPending = node.certificationGatherPending
  node.certificationGatherPending = true
  when defined(fabric_submit_diag):
    if not wasPending:
      echo "certification-gather pending account=", node.identity.account
  node.scheduleCertificationGatherRunner()

proc scheduleCertificationGatherRunner(node: FabricNode) {.gcsafe, raises: [].} =
  if node.isNil or not node.running or not node.certificationGatherPending:
    return
  if not node.certificationGatherRunner.isNil and
      not node.certificationGatherRunner.finished():
    return
  proc runner() {.async, gcsafe.} =
    try:
      await node.certificationGatherLoop()
    except CancelledError:
      discard
  node.certificationGatherRunner = runner()
  asyncSpawn node.certificationGatherRunner

proc certificationGatherLoop(node: FabricNode) {.async: (raises: []).} =
  if node.isNil:
    return
  try:
    await sleepAsync(FabricMaintenanceYield)
    while node.running and node.certificationGatherPending:
      node.certificationGatherPending = false
      node.refreshPendingCertificationEvents()
      node.prunePendingCertificationState()
      let pendingCertificationCount = node.pendingCertificationEvents.len
      let certificationEventIds =
        node.drainPendingCertificationEventFrontier(FabricMaintenanceBatchLimit)
      if pendingCertificationCount > certificationEventIds.len:
        node.certificationGatherPending = true
      for idx, eventId in certificationEventIds:
        if idx > 0:
          await sleepAsync(FabricMaintenanceYield)
        if not node.running:
          break
        if not node.certificationEventTracked(eventId):
          node.clearCertificationEventState(eventId)
          continue
        let event = node.events.getOrDefault(eventId)
        if not node.eventParticipantSatisfied(event):
          for item in node.certificationMissingParticipantPulls(eventId):
            if node.witnessPullStillNeeded(item):
              discard node.enqueueWitnessPullWorkItem(item)
              node.ensureWitnessPullRunner(item.targetPeerId, item.domain)
        else:
          let witnessInfo = node.witnessCount(event, @[node])
          if witnessInfo.signed >= witnessInfo.threshold:
            if node.maybeCertifyEvent(event, @[node]):
              node.clearCertificationEventState(eventId)
          else:
            for item in node.certificationMissingWitnessPulls(eventId):
              if node.witnessPullStillNeeded(item):
                discard node.enqueueWitnessPullWorkItem(item)
                node.ensureWitnessPullRunner(item.targetPeerId, item.domain)
      if node.certificationGatherPending:
        await sleepAsync(FabricMaintenanceYield)
  except CancelledError:
    discard
  finally:
    if not node.isNil:
      node.certificationGatherRunner = nil
      if node.running and node.certificationGatherPending:
        node.scheduleCertificationGatherRunner()

proc scheduleCheckpointAdvance(node: FabricNode) {.gcsafe, raises: [].} =
  if node.isNil:
    return
  let wasPending = node.checkpointAdvancePending
  node.checkpointAdvancePending = true
  when defined(fabric_submit_diag):
    if not wasPending:
      echo "checkpoint-advance pending account=", node.identity.account
  node.scheduleCheckpointAdvanceRunner()
  node.scheduleStateMaintenance()

proc scheduleCheckpointAdvanceRunner(node: FabricNode) {.gcsafe, raises: [].} =
  if node.isNil or not node.running or not node.checkpointAdvancePending:
    return
  if not node.checkpointAdvanceRunner.isNil and not node.checkpointAdvanceRunner.finished():
    return
  proc runner() {.async, gcsafe.} =
    try:
      await node.checkpointAdvanceLoop()
    except CancelledError:
      discard
  node.checkpointAdvanceRunner = runner()
  asyncSpawn node.checkpointAdvanceRunner

proc checkpointAdvanceLoop(node: FabricNode) {.async: (raises: []).} =
  if node.isNil:
    return
  try:
    await sleepAsync(FabricMaintenanceYield)
    while node.running and node.checkpointAdvancePending:
      if not node.checkpointAdvanceReady():
        break
      node.checkpointAdvancePending = false
      var candidates: seq[CheckpointCandidate] = @[]
      try:
        {.cast(gcsafe).}:
          discard node.ensureEraCandidates(@[node])
          candidates = node.pendingCheckpointAdvanceCandidates()
      except Exception as exc:
        node.logMaintenanceError("checkpoint-advance", "", exc)
      if candidates.len == 0:
        continue
      for idx, candidate in candidates:
        if idx > 0:
          await sleepAsync(FabricMaintenanceYield)
        if not node.running:
          break
        if not node.checkpointAdvanceReady():
          node.checkpointAdvancePending = true
          break
        try:
          {.cast(gcsafe).}:
            discard node.advanceCheckpointCandidate(candidate)
        except Exception as exc:
          node.logMaintenanceError("checkpoint-advance", candidate.candidateId, exc)
      if node.checkpointAdvancePending:
        await sleepAsync(FabricMaintenanceYield)
  except CancelledError:
    discard
  finally:
    if not node.isNil:
      node.checkpointAdvanceRunner = nil
      if node.running and node.checkpointAdvancePending and node.checkpointAdvanceReady():
        node.scheduleCheckpointAdvanceRunner()

proc scheduleReadyOutboundSessions(node: FabricNode) {.gcsafe, raises: [].} =
  if node.isNil or not node.running:
    return
  var wantsOther = false
  for targetPeerId, session in node.outboundSessions.mpairs:
    if session.isNil:
      continue
    for priority in [ospEvent, ospAttestation]:
      if session.hasPending(priority):
        node.scheduleOutboundSession(targetPeerId, priority)
    wantsOther = wantsOther or session.hasPending(ospOther)
  if wantsOther:
    node.scheduleNextOtherOutbound()

proc currentLsmrOverlayDigest(node: FabricNode): string =
  if node.isNil or node.network.isNil or node.network.switch.isNil or
      node.network.switch.peerStore.isNil:
    return ""
  var entries: seq[string] = @[]
  for peerId, record in node.network.switch.peerStore[ActiveLsmrBook].book.pairs:
    entries.add($peerId & "=" & $record.data.certifiedPrefix())
  entries.sort(system.cmp[string])
  entries.join("|")

proc currentLsmrSubmitReadyDigest(node: FabricNode): string =
  if node.isNil or not node.usesLsmrPrimary() or node.network.isNil or
      node.network.switch.isNil:
    return ""
  var seen = initHashSet[string]()
  var entries: seq[string] = @[]
  for peerId in node.network.switch.connectedPeers(Direction.Out) &
      node.network.switch.connectedPeers(Direction.In):
    let peerIdText = $peerId
    if peerIdText in seen:
      continue
    seen.incl(peerIdText)
    entries.add(
      peerIdText &
      "=e" & $(if node.network.submitPeerReadyNow(peerIdText, fslEvent): 1 else: 0) &
      "a" & $(if node.network.submitPeerReadyNow(peerIdText, fslAttestation): 1 else: 0) &
      "o" & $(if node.network.submitPeerReadyNow(peerIdText, fslOther): 1 else: 0)
    )
  entries.sort(system.cmp[string])
  entries.join("|")

proc currentLsmrDeliveryDigest(node: FabricNode): string =
  if node.isNil or not node.usesLsmrPrimary():
    return ""
  var entries: seq[string] = @[]
  for itemKey, ledger in node.eventDeliveries.pairs:
    entries.add(
      "e:" & itemKey &
      ":r=" & $ledger.remainingTargets.len &
      ":i=" & $ledger.inflightTargets.len &
      ":d=" & $ledger.deliveredTargets.len
    )
  for itemKey, ledger in node.attestationDeliveries.pairs:
    entries.add(
      "a:" & itemKey &
      ":r=" & $ledger.remainingTargets.len &
      ":i=" & $ledger.inflightTargets.len &
      ":d=" & $ledger.deliveredTargets.len
    )
  for itemKey, ledger in node.eventCertificateDeliveries.pairs:
    entries.add(
      "c:" & itemKey &
      ":r=" & $ledger.remainingTargets.len &
      ":i=" & $ledger.inflightTargets.len &
      ":d=" & $ledger.deliveredTargets.len
    )
  entries.sort(system.cmp[string])
  entries.join("|")

proc refreshLsmrPending(node: FabricNode) =
  if node.isNil or not node.usesLsmrPrimary():
    return
  let overlayDigest = node.currentLsmrOverlayDigest()
  let submitReadyDigest = node.currentLsmrSubmitReadyDigest()
  let deliveryGeneration = node.lsmrDeliveryGeneration
  let latestCheckpointId = node.latestCheckpointId
  if overlayDigest == node.lsmrOverlayDigest and
      submitReadyDigest == node.lsmrSubmitReadyDigest and
      deliveryGeneration == node.lsmrObservedDeliveryGeneration and
      latestCheckpointId == node.lsmrObservedLatestCheckpointId:
    return
  node.lsmrOverlayDigest = overlayDigest
  node.lsmrSubmitReadyDigest = submitReadyDigest
  node.lsmrObservedDeliveryGeneration = deliveryGeneration
  node.lsmrObservedLatestCheckpointId = latestCheckpointId
  let checkpointKnown =
    node.latestCheckpointId.len > 0 and node.latestCheckpointId in node.checkpoints
  let checkpointEra =
    if checkpointKnown: node.checkpoints[node.latestCheckpointId].candidate.era else: 0'u64
  var eventsQueued = 0
  var eventsPaused = 0
  var attestationsQueued = 0
  var attestationsPaused = 0
  var certificatesQueued = 0
  var certificatesPaused = 0
  var candidatesQueued = 0
  var votesQueued = 0
  var checkpointsQueued = 0
  var proposalsQueued = 0
  var approvalsQueued = 0
  for eventId in activeDeliveryKeys(node.pendingEvents, node.eventDeliveries):
    if eventId notin node.events:
      continue
    let event = node.events[eventId]
    if eventId in node.isolatedEvents:
      continue
    if eventId in node.eventCertificates:
      continue
    if checkpointKnown and event.clock.era < checkpointEra:
      continue
    if node.eventHasRunnableLsmrDelivery(
        event,
        node.pendingEventScopes.pendingScopePrefixes(eventId),
    ):
      if node.pendingEvents.enqueuePending(eventId):
        inc eventsQueued
    else:
      if eventId in node.pendingEvents:
        inc eventsPaused
  for key in activeDeliveryKeys(node.pendingAttestations, node.attestationDeliveries):
    let att = node.attestationForKey(key)
    if att.isNone() or att.get().eventId notin node.events:
      node.completeAttestationDelivery(key)
      continue
    let eventId = att.get().eventId
    if eventId in node.isolatedEvents or eventId in node.eventCertificates:
      node.completeAttestationDelivery(key)
      continue
    if checkpointKnown and node.events[eventId].clock.era < checkpointEra:
      node.completeAttestationDelivery(key)
      continue
    if node.attestationHasRunnableLsmrDelivery(node.events[eventId], att.get()):
      if node.pendingAttestations.enqueuePending(key):
        inc attestationsQueued
    else:
      if key in node.pendingAttestations:
        inc attestationsPaused
  for key in sortedKeys(node.routeBlockedAttestations):
    let att = node.attestationForKey(key)
    if att.isNone() or att.get().eventId notin node.events:
      node.completeAttestationDelivery(key)
      continue
    let eventId = att.get().eventId
    if eventId in node.isolatedEvents or eventId in node.eventCertificates:
      node.completeAttestationDelivery(key)
      continue
    if checkpointKnown and node.events[eventId].clock.era < checkpointEra:
      node.completeAttestationDelivery(key)
      continue
    if not node.attestationRequiresRelay(node.events[eventId], att.get()):
      node.completeAttestationDelivery(key)
      continue
    if node.attestationNeedsRelay(node.events[eventId], att.get()):
      node.routeBlockedAttestations.excl(key)
      if node.pendingAttestations.enqueuePending(key):
        inc attestationsQueued
    else:
      inc attestationsPaused
  for eventId in activeDeliveryKeys(
      node.pendingEventCertificates,
      node.eventCertificateDeliveries,
  ):
    if eventId notin node.events:
      node.completeEventCertificateDelivery(eventId)
      continue
    let event = node.events[eventId]
    if eventId notin node.eventCertificates or eventId in node.isolatedEvents:
      node.completeEventCertificateDelivery(eventId)
      continue
    if checkpointKnown and event.clock.era < checkpointEra:
      node.completeEventCertificateDelivery(eventId)
      continue
    if node.eventCertificateHasRunnableLsmrDelivery(
        event,
        node.eventCertificates[eventId],
    ):
      if node.touchPendingEventCertificate(eventId):
        inc certificatesQueued
    else:
      if node.eventCertificateDeliveries.hasDeliveryTargets(eventId) and
          not node.eventCertificateDeliveries.hasOutstandingDelivery(eventId):
        node.completeEventCertificateDelivery(eventId)
      elif eventId in node.pendingEventCertificates:
        inc certificatesPaused
  for candidateId, candidate in node.checkpointCandidates.pairs:
    if node.hasCheckpointForCandidate(candidateId):
      continue
    if node.pendingCheckpointCandidates.enqueuePending(candidate.candidateId):
      inc candidatesQueued
  for candidateId, votes in node.checkpointVotes.pairs:
    if node.hasCheckpointForCandidate(candidateId):
      continue
    for vote in votes:
      if node.pendingCheckpointVotes.enqueuePending(vote.checkpointVoteKey()):
        inc votesQueued
  let relayCheckpointId = node.latestCheckpointId
  for checkpointId, _ in node.checkpoints.pairs:
    if checkpointId notin node.checkpointSnapshots:
      continue
    if relayCheckpointId.len > 0 and checkpointId != relayCheckpointId:
      node.completeCheckpointBundleDelivery(checkpointId)
      continue
    if node.pendingCheckpointBundles.enqueuePending(checkpointId):
      inc checkpointsQueued
  for proposalId, _ in node.avoProposals.pairs:
    if node.pendingAvoProposals.enqueuePending(proposalId):
      inc proposalsQueued
  for proposalId, validators in node.avoApprovals.pairs:
    for validator in validators:
      if node.pendingAvoApprovals.enqueuePending(avoApprovalKey(proposalId, validator)):
        inc approvalsQueued
  when defined(fabric_lsmr_diag) or defined(fabric_submit_diag):
    echo "lsmr-overlay-refresh account=", node.identity.account,
      " events=", eventsQueued,
      " pausedEvents=", eventsPaused,
      " attestations=", attestationsQueued,
      " pausedAttestations=", attestationsPaused,
      " certs=", certificatesQueued,
      " pausedCerts=", certificatesPaused,
      " candidates=", candidatesQueued,
      " votes=", votesQueued,
      " checkpoints=", checkpointsQueued,
      " proposals=", proposalsQueued,
      " approvals=", approvalsQueued,
      " readyPeers=", submitReadyDigest

proc pruneStaleLsmrDeliveries(node: FabricNode) {.gcsafe, raises: [].} =
  if node.isNil or not node.usesLsmrPrimary():
    return
  let checkpointEra =
    if node.latestCheckpointId.len > 0 and node.latestCheckpointId in node.checkpoints:
      node.checkpoints.getOrDefault(node.latestCheckpointId).candidate.era
    else:
      0'u64
  var prunedEvents = 0
  var prunedAttestations = 0
  var prunedCertificates = 0
  for eventId in node.eventDeliveries.keys.toSeq():
    let ledger = node.eventDeliveries.getOrDefault(eventId, initDeliveryLedger())
    if ledger.remainingTargets.len == 0 and ledger.inflightTargets.len == 0:
      continue
    if eventId notin node.events or eventId in node.isolatedEvents:
      node.completeEventDelivery(eventId)
      inc prunedEvents
      continue
    let event = node.events.getOrDefault(eventId)
    if event.clock.era < checkpointEra:
      node.completeEventDelivery(eventId)
      inc prunedEvents
  for key in node.attestationDeliveries.keys.toSeq():
    let ledger = node.attestationDeliveries.getOrDefault(key, initDeliveryLedger())
    if ledger.remainingTargets.len == 0 and ledger.inflightTargets.len == 0:
      continue
    let att = node.attestationForKey(key)
    if att.isNone():
      node.completeAttestationDelivery(key)
      inc prunedAttestations
      continue
    let eventId = att.get().eventId
    if eventId notin node.events or eventId in node.isolatedEvents or
        eventId in node.eventCertificates:
      node.completeAttestationDelivery(key)
      inc prunedAttestations
      continue
    if node.events.getOrDefault(eventId).clock.era < checkpointEra:
      node.completeAttestationDelivery(key)
      inc prunedAttestations
  for eventId in node.eventCertificateDeliveries.keys.toSeq():
    let ledger = node.eventCertificateDeliveries.getOrDefault(eventId, initDeliveryLedger())
    if ledger.remainingTargets.len == 0 and ledger.inflightTargets.len == 0:
      continue
    if eventId notin node.events or eventId notin node.eventCertificates or
        eventId in node.isolatedEvents:
      node.completeEventCertificateDelivery(eventId)
      inc prunedCertificates
      continue
    if node.events.getOrDefault(eventId).clock.era < checkpointEra:
      node.completeEventCertificateDelivery(eventId)
      inc prunedCertificates
  when defined(fabric_submit_diag) or defined(fabric_lsmr_diag):
    if prunedEvents > 0 or prunedAttestations > 0 or prunedCertificates > 0:
      echo "lsmr-stale-prune account=", node.identity.account,
        " events=", prunedEvents,
        " attestations=", prunedAttestations,
        " certs=", prunedCertificates

proc pruneLsmrPending(node: FabricNode) =
  if node.isNil or not node.usesLsmrPrimary():
    return
  var pausedEvents = 0
  var pausedAttestations = 0
  var pausedCertificates = 0
  let checkpointKnown =
    node.latestCheckpointId.len > 0 and node.latestCheckpointId in node.checkpoints
  let checkpointEra =
    if checkpointKnown:
      node.checkpoints.getOrDefault(node.latestCheckpointId).candidate.era
    else:
      0'u64
  for eventId in node.pendingEvents.toSeq():
    if eventId notin node.events or eventId in node.isolatedEvents or
        eventId in node.eventCertificates:
      continue
    if not node.eventHasRunnableLsmrDelivery(
        node.events[eventId],
        node.pendingEventScopes.pendingScopePrefixes(eventId),
    ):
      node.pendingEvents.excl(eventId)
      inc pausedEvents
  for key in node.pendingAttestations.toSeq():
    let att = node.attestationForKey(key)
    if att.isNone() or att.get().eventId notin node.events or
        att.get().eventId in node.eventCertificates:
      node.completeAttestationDelivery(key)
      inc pausedAttestations
      continue
    if checkpointKnown and node.events[att.get().eventId].clock.era < checkpointEra:
      node.completeAttestationDelivery(key)
      inc pausedAttestations
      continue
    if not node.attestationHasRunnableLsmrDelivery(node.events[att.get().eventId], att.get()):
      node.pendingAttestations.excl(key)
      inc pausedAttestations
  for eventId in node.pendingEventCertificates.toSeq():
    if eventId notin node.events or eventId notin node.eventCertificates or
        eventId in node.isolatedEvents:
      node.completeEventCertificateDelivery(eventId)
      inc pausedCertificates
      continue
    let event = node.events[eventId]
    if checkpointKnown and node.events[eventId].clock.era < checkpointEra:
      node.completeEventCertificateDelivery(eventId)
      inc pausedCertificates
      continue
    if not node.eventCertificateHasRunnableLsmrDelivery(
        event,
        node.eventCertificates[eventId],
    ):
      if node.eventCertificateDeliveries.hasDeliveryTargets(eventId) and
          not node.eventCertificateDeliveries.hasOutstandingDelivery(eventId):
        node.completeEventCertificateDelivery(eventId)
      else:
        node.dropPendingEventCertificate(eventId)
      inc pausedCertificates
  when defined(fabric_submit_diag) or defined(fabric_lsmr_diag):
    if pausedEvents > 0 or pausedAttestations > 0 or pausedCertificates > 0:
      echo "lsmr-pending-prune account=", node.identity.account,
        " events=", pausedEvents,
        " attestations=", pausedAttestations,
        " certs=", pausedCertificates

proc uniqueRoutingPeers(routes: openArray[RoutingPeer]): seq[RoutingPeer] =
  var seenPeerIds = initHashSet[string]()
  var seenAccounts = initHashSet[string]()
  for route in routes:
    if route.peerId.len > 0:
      if route.peerId in seenPeerIds:
        continue
      seenPeerIds.incl(route.peerId)
    elif route.account.len > 0:
      if route.account in seenAccounts:
        continue
      seenAccounts.incl(route.account)
    else:
      continue
    result.add(route)

proc installRouteAddrs(node: FabricNode, routes: openArray[RoutingPeer]) =
  if node.isNil or node.network.isNil or node.network.switch.isNil:
    return
  for route in routes:
    if route.peerId.len == 0 or route.addrs.len == 0:
      continue
    let peerId = PeerId.init(route.peerId).valueOr:
      continue
    var addrs: seq[MultiAddress] = @[]
    for raw in route.addrs:
      let parsed = MultiAddress.init(raw)
      if parsed.isOk():
        addrs.add(parsed.get())
    if addrs.len > 0:
      node.network.switch.peerStore.setAddresses(peerId, addrs)
      node.network.rememberSubmitPeerAddrs(peerId, addrs)

proc eventWitnessRoutes(event: FabricEvent): seq[RoutingPeer] =
  if event.witnessRoutes.len > 0:
    return uniqueRoutingPeers(event.witnessRoutes)
  for witness in event.witnessSet:
    result.add(RoutingPeer(account: witness, peerId: "", path: @[], addrs: @[]))

proc eventWitnessAccounts(event: FabricEvent): seq[string] =
  result = event.witnessSet
  for route in event.eventWitnessRoutes():
    if route.account.len > 0 and route.account notin result:
      result.add(route.account)
  result.sort(system.cmp[string])

proc eventWitnessPeerIds(event: FabricEvent): seq[string] =
  for route in event.eventWitnessRoutes():
    if route.peerId.len > 0 and route.peerId notin result:
      result.add(route.peerId)
  result.sort(system.cmp[string])

proc nodeIsEventWitness(
    node: FabricNode, event: FabricEvent
): bool {.gcsafe, raises: [].} =
  if node.isNil:
    return false
  if node.identity.account in event.eventWitnessAccounts():
    return true
  event.eventWitnessRoutes().anyIt(node.routeMatchesSelf(it))

proc initEventCertificationProgress(event: FabricEvent): EventCertificationProgress =
  let witnessAccounts = event.eventWitnessAccounts()
  let reachableWitnesses =
    if event.eventWitnessRoutes().len > 0:
      event.eventWitnessRoutes().len
    else:
      witnessAccounts.len
  result = EventCertificationProgress(
    clock: event.clock,
    participantAttestations: initTable[string, EventAttestation](),
    witnessAttestations: initTable[string, EventAttestation](),
    witnessAccounts: initHashSet[string](),
    witnessPeerIds: initHashSet[string](),
    missingWitnessAccounts: initHashSet[string](),
    witnessRoutes: initTable[string, RoutingPeer](),
    reachableWitnesses: reachableWitnesses,
    witnessThreshold: max(1, (reachableWitnesses + 1) div 2),
    status: agsWaitingParticipants,
  )
  for account in witnessAccounts:
    result.witnessAccounts.incl(account)
    result.missingWitnessAccounts.incl(account)
  for peerId in event.eventWitnessPeerIds():
    result.witnessPeerIds.incl(peerId)
  for route in event.eventWitnessRoutes():
    if route.account.len > 0:
      result.witnessRoutes[route.account] = route

proc ensureEventCertificationProgress(
    node: FabricNode, eventId: string
) {.gcsafe, raises: [].} =
  if node.isNil or eventId.len == 0 or eventId notin node.events or
      eventId in node.eventCertificationProgress:
    return
  node.eventCertificationProgress[eventId] =
    initEventCertificationProgress(node.events.getOrDefault(eventId))

proc certificationCheckpointEra(node: FabricNode): uint64 {.gcsafe, raises: [].}

proc refreshEventCertificationProgressState(
    node: FabricNode, eventId: string
) {.gcsafe, raises: [].} =
  if node.isNil or eventId.len == 0 or eventId notin node.events:
    return
  node.ensureEventCertificationProgress(eventId)
  if eventId notin node.eventCertificationProgress:
    return
  let event = node.events.getOrDefault(eventId)
  var progress = node.eventCertificationProgress.getOrDefault(eventId)
  progress.clock = event.clock
  progress.missingWitnessAccounts = initHashSet[string]()
  for account in progress.witnessAccounts:
    var witnessed = false
    for _, att in progress.witnessAttestations.pairs:
      if att.signer == account:
        witnessed = true
        break
    if not witnessed:
      progress.missingWitnessAccounts.incl(account)
  let checkpointEra = node.certificationCheckpointEra()
  if eventId in node.isolatedEvents or
      (checkpointEra > 0'u64 and event.clock.era < checkpointEra):
    progress.status = agsRetired
  elif eventId in node.eventCertificates:
    progress.status = agsCertified
  elif progress.participantAttestations.len < event.participants.len:
    progress.status = agsWaitingParticipants
  elif progress.witnessAttestations.len >= progress.witnessThreshold:
    progress.status = agsCertReady
  else:
    progress.status = agsWaitingWitnesses
  node.eventCertificationProgress[eventId] = progress

proc certificationWitnessIdentity(
    node: FabricNode, eventId: string, att: EventAttestation
): string =
  if node.isNil or eventId.len == 0 or eventId notin node.eventCertificationProgress:
    return ""
  if att.signer.len > 0 and
      att.signer in node.eventCertificationProgress[eventId].witnessAccounts:
    return "a:" & att.signer
  if att.signerPeerId.len > 0 and
      att.signerPeerId in node.eventCertificationProgress[eventId].witnessPeerIds:
    return "p:" & att.signerPeerId
  ""

proc noteEventCertificationAttestation(
    node: FabricNode, att: EventAttestation
) =
  if node.isNil or att.eventId.len == 0 or att.eventId notin node.events:
    return
  node.ensureEventCertificationProgress(att.eventId)
  if att.eventId notin node.eventCertificationProgress:
    return
  case att.role
  of arParticipant:
    if att.signer in node.events[att.eventId].participants:
      node.eventCertificationProgress[att.eventId].participantAttestations[att.signer] = att
  of arWitness:
    let witnessId = node.certificationWitnessIdentity(att.eventId, att)
    if witnessId.len > 0 and
        witnessId notin node.eventCertificationProgress[att.eventId].witnessAttestations:
      node.eventCertificationProgress[att.eventId].witnessAttestations[witnessId] = att
  of arIsolation:
    discard
  node.refreshEventCertificationProgressState(att.eventId)

proc maxKnownClock(node: FabricNode): GanzhiClock =
  for _, cert in node.eventCertificates.pairs:
    result = maxClock(result, cert.clock)

proc certifiedEventIds(node: FabricNode): seq[string] =
  for eventId, _ in node.eventCertificates.pairs:
    result.add(eventId)
  result.sort(system.cmp[string])

proc eventOrder(a, b: FabricEvent): int =
  result = clockCmp(a.clock, b.clock)
  if result == 0:
    result = cmp(a.eventId, b.eventId)

proc pendingEventKeys(node: FabricNode): seq[string] =
  result = node.pendingEvents.toSeq()
  result.sort(proc(a, b: string): int =
    let aKnown = a in node.events
    let bKnown = b in node.events
    if aKnown and bKnown:
      result = eventOrder(node.events[a], node.events[b])
      if result == 0:
        result = cmp(a, b)
    elif aKnown:
      result = -1
    elif bKnown:
      result = 1
    else:
      result = cmp(a, b)
  )

proc pendingAttestationKeys(node: FabricNode): seq[string] =
  result = node.pendingAttestations.toSeq()
  result.sort(proc(a, b: string): int =
    let aAtt = node.attestationForKey(a)
    let bAtt = node.attestationForKey(b)
    if aAtt.isSome() and bAtt.isSome() and
        aAtt.get().eventId in node.events and bAtt.get().eventId in node.events:
      result = eventOrder(node.events[aAtt.get().eventId], node.events[bAtt.get().eventId])
      if result == 0:
        result = cmp(ord(aAtt.get().role), ord(bAtt.get().role))
      if result == 0:
        result = cmp(aAtt.get().signer, bAtt.get().signer)
      if result == 0:
        result = cmp(a, b)
    elif aAtt.isSome():
      result = -1
    elif bAtt.isSome():
      result = 1
    else:
      result = cmp(a, b)
  )

proc certifiedEvents(node: FabricNode, eraLimit = high(uint64)): seq[FabricEvent] =
  for eventId in node.certifiedEventIds():
    let event = node.events[eventId]
    if event.clock.era <= eraLimit:
      result.add(event)
  result.sort(eventOrder)

proc initEraCheckpointState(): EraCheckpointState {.gcsafe, raises: [].} =
  EraCheckpointState(
    eventTotal: 0,
    isolatedTotal: 0,
    certifiedTotal: 0,
    candidatePhase: ecpPending,
    votePhase: evpPending,
    bundlePhase: ebpPending,
    candidateId: "",
    voteBitmap: initHashSet[string](),
    bundleTargetsRemaining: initHashSet[string](),
    firstMissingEventId: "",
    minTick: -1,
    maxTick: -1,
  )

proc checkpointIdForCandidate(
    node: FabricNode, candidateId: string
): string {.gcsafe, raises: [].} =
  if node.isNil or candidateId.len == 0:
    return ""
  for checkpointId, cert in node.checkpoints.pairs:
    if cert.candidate.candidateId == candidateId:
      return checkpointId

proc refreshEraCheckpointStates(node: FabricNode) {.gcsafe, raises: [].} =
  if node.isNil:
    return
  node.eraCheckpointStates.clear()
  var orderedEvents: seq[FabricEvent] = @[]
  for _, event in node.events.pairs:
    orderedEvents.add(event)
  orderedEvents.sort(eventOrder)
  for event in orderedEvents:
    let era = event.clock.era
    var state = node.eraCheckpointStates.getOrDefault(era, initEraCheckpointState())
    if event.eventId in node.isolatedEvents:
      inc state.isolatedTotal
      node.eraCheckpointStates[era] = state
      continue
    inc state.eventTotal
    if state.minTick < 0 or int(event.clock.tick) < state.minTick:
      state.minTick = int(event.clock.tick)
    if state.maxTick < 0 or int(event.clock.tick) > state.maxTick:
      state.maxTick = int(event.clock.tick)
    if event.eventId in node.eventCertificates:
      inc state.certifiedTotal
    elif state.firstMissingEventId.len == 0:
      state.firstMissingEventId = event.eventId
    node.eraCheckpointStates[era] = state
  for candidateId, candidate in node.checkpointCandidates.pairs:
    let era = candidate.era
    var state = node.eraCheckpointStates.getOrDefault(era, initEraCheckpointState())
    if state.candidateId.len == 0 or candidateId < state.candidateId:
      state.candidateId = candidateId
    if node.hasCheckpointForCandidate(candidateId):
      state.candidateId = candidateId
      state.candidatePhase = ecpCertified
      node.eraCheckpointStates[era] = state
      continue
    if candidateId in node.pendingCheckpointCandidates or
        hasOutstandingDelivery(node.checkpointCandidateDeliveries, candidateId):
      state.candidatePhase = ecpPublished
    elif state.candidatePhase == ecpPending:
      state.candidatePhase = ecpReady
    node.eraCheckpointStates[era] = state
  for era in node.eraCheckpointStates.keys.toSeq():
    var state = node.eraCheckpointStates.getOrDefault(era, initEraCheckpointState())
    let candidateId = state.candidateId
    if candidateId.len == 0:
      continue
    for vote in node.checkpointVotes.getOrDefault(candidateId, @[]):
      state.voteBitmap.incl(vote.validator)
    if node.hasCheckpointForCandidate(candidateId):
      state.votePhase = evpQuorum
    elif state.voteBitmap.len > 0:
      state.votePhase = evpCollecting
    let checkpointId = node.checkpointIdForCandidate(candidateId)
    if checkpointId.len == 0:
      node.eraCheckpointStates[era] = state
      continue
    if checkpointId in node.checkpointBundleDeliveries:
      let ledger = node.checkpointBundleDeliveries.getOrDefault(
        checkpointId,
        initDeliveryLedger(),
      )
      for targetKey in ledger.remainingTargets:
        state.bundleTargetsRemaining.incl(targetKey)
      for targetKey in ledger.inflightTargets:
        state.bundleTargetsRemaining.incl(targetKey)
    if hasOutstandingDelivery(node.checkpointBundleDeliveries, checkpointId) or
        checkpointId in node.pendingCheckpointBundles:
      state.bundlePhase = ebpPublished
    else:
      state.bundlePhase = ebpCompleted
    node.eraCheckpointStates[era] = state

proc checkpointEraState(
    node: FabricNode, era: uint64
): EraCheckpointState {.gcsafe, raises: [].} =
  if node.isNil:
    return initEraCheckpointState()
  node.eraCheckpointStates.getOrDefault(era, initEraCheckpointState())

proc replayStateDigest(events: openArray[FabricEvent]): string =
  if events.len == 0:
    return ""
  hashHex(events.mapIt(it.eventId).join(":"))

proc knownEvents(node: FabricNode): seq[FabricEvent] =
  for _, event in node.events.pairs:
    result.add(event)
  result.sort(eventOrder)

proc replayState(node: FabricNode, eraLimit = high(uint64)): ChainState =
  let ordered = node.certifiedEvents(eraLimit)
  let digest = replayStateDigest(ordered)
  if node.replayStateCache.hasKey(eraLimit):
    let cached = node.replayStateCache[eraLimit]
    if cached.digest == digest:
      return fromSnapshot(cached.snapshot)
  result = newChainState(node.genesis)
  var applied = initHashSet[string]()
  var remaining = ordered.len
  while remaining > 0:
    var progressed = false
    for event in ordered:
      if event.eventId in applied:
        continue
      var parentsReady = true
      for parent in event.parents:
        if parent in node.eventCertificates and parent notin applied:
          parentsReady = false
          break
      if not parentsReady:
        continue
      discard result.applyEventToState(event)
      applied.incl(event.eventId)
      dec remaining
      progressed = true
    if not progressed:
      raise newException(ValueError, "certified event DAG has unresolved parents")
  node.replayStateCache[eraLimit] = ReplayStateCacheEntry(
    digest: digest,
    snapshot: result.snapshot(),
  )

proc liveEventParentsReady(node: FabricNode, eventId: string): bool =
  if node.isNil or eventId.len == 0 or eventId notin node.events:
    return false
  for parent in node.events[eventId].parents:
    if parent in node.eventCertificates and parent notin node.liveAppliedEvents:
      return false
  true

proc advanceLiveApplyFrom(node: FabricNode, rootEventId: string) =
  if node.isNil or rootEventId.len == 0:
    return
  var queue = initDeque[string]()
  var enqueued = initHashSet[string]()

  proc enqueue(eventId: string) =
    if eventId.len == 0 or eventId in enqueued or
        eventId notin node.eventCertificates or eventId in node.liveAppliedEvents:
      return
    enqueued.incl(eventId)
    queue.addLast(eventId)

  enqueue(rootEventId)
  while queue.len > 0:
    let eventId = queue.popFirst()
    if eventId notin node.events or eventId notin node.eventCertificates or
        eventId in node.liveAppliedEvents:
      continue
    if not node.liveEventParentsReady(eventId):
      continue
    let event = node.events[eventId]
    when defined(fabric_diag):
      echo "live-apply-start ", node.identity.account, " event=", eventId
    discard node.liveState.applyEventToState(event)
    node.liveAppliedEvents.incl(eventId)
    when defined(fabric_diag):
      echo "live-apply-done ", node.identity.account, " event=", eventId
    for childEventId in node.children.getOrDefault(eventId, @[]):
      enqueue(childEventId)

proc applyPendingLiveEvents(node: FabricNode) =
  for eventId in node.certifiedEventIds():
    node.advanceLiveApplyFrom(eventId)

proc validatorWeight(state: ChainState, address: string): uint64 =
  if state.validators.hasKey(address):
    return state.validators[address].validatorVotingPower()
  0

proc eventAttestationWeight(node: FabricNode): uint64 =
  if node.liveState.validators.hasKey(node.identity.account):
    return node.liveState.validators[node.identity.account].validatorVotingPower()
  1

proc attestationBy(node: FabricNode, eventId, signer: string, role: AttestationRole): bool =
  attestationKey(EventAttestation(eventId: eventId, signer: signer, role: role)) in node.attestationsByKey

proc localAttestation(node: FabricNode, eventId, signer: string, role: AttestationRole): Option[EventAttestation] =
  node.attestationForKey(
    attestationKey(EventAttestation(eventId: eventId, signer: signer, role: role))
  )

proc verifyAttestation(att: EventAttestation): bool =
  if att.signerPublicKey.len == 0 or att.signature.len == 0:
    return false
  let publicKey = PublicKey.init(att.signerPublicKey).valueOr:
    return false
  if accountFromPublicKey(publicKey) != att.signer:
    return false
  verifyBytes(publicKey, att.attestationSigningBytes(), att.signature)

proc makeAttestation(node: FabricNode, event: FabricEvent, role: AttestationRole): EventAttestation =
  result = EventAttestation(
    eventId: event.eventId,
    signer: node.identity.account,
    signerPublicKey: encodePublicKeyHex(node.identity.publicKey),
    signerPeerId: peerIdString(node.identity.peerId),
    role: role,
    weight: node.eventAttestationWeight(),
    issuedAt: max(event.createdAt, int64(node.maxKnownClock().era * 60'u64 + node.maxKnownClock().tick.uint64)),
    signature: "",
  )
  result.signature = signBytes(node.identity.privateKey, result.attestationSigningBytes())

proc voteBy(node: FabricNode, candidateId, validator: string): bool =
  for vote in node.checkpointVotes.getOrDefault(candidateId, @[]):
    if vote.validator == validator:
      return true
  false

proc makeCheckpointVote(node: FabricNode, candidate: CheckpointCandidate, stateAtEra: ChainState): CheckpointVote =
  result = CheckpointVote(
    candidateId: candidate.candidateId,
    validator: node.identity.account,
    validatorPublicKey: encodePublicKeyHex(node.identity.publicKey),
    validatorPeerId: peerIdString(node.identity.peerId),
    weight: stateAtEra.validatorWeight(node.identity.account),
    issuedAt: candidate.createdAt,
    signature: "",
  )
  result.signature = signBytes(node.identity.privateKey, result.checkpointVoteSigningBytes())

proc verifyCheckpointVote(vote: CheckpointVote): bool =
  if vote.validatorPublicKey.len == 0 or vote.signature.len == 0:
    return false
  let publicKey = PublicKey.init(vote.validatorPublicKey).valueOr:
    return false
  if accountFromPublicKey(publicKey) != vote.validator:
    return false
  verifyBytes(publicKey, vote.checkpointVoteSigningBytes(), vote.signature)

proc registerAttestation(
    node: FabricNode, att: EventAttestation, enqueuePending = true
): bool =
  if att.eventId notin node.events:
    return node.bufferDeferredAttestation(att)
  if not verifyAttestation(att):
    when defined(fabric_submit_diag):
      echo "att-stage reject-invalid account=", node.identity.account,
        " event=", att.eventId,
        " role=", ord(att.role),
        " signer=", att.signer
    return false
  let fetchKey = attestationFetchKey(att.eventId, att.role, att.signer)
  if att.role == arParticipant or att.role == arWitness:
    node.clearQueuedWitnessPullByKey(fetchKey)
  if node.attestationBy(att.eventId, att.signer, att.role):
    when defined(fabric_submit_diag):
      echo "att-stage skip-duplicate account=", node.identity.account,
        " event=", att.eventId,
        " role=", ord(att.role),
        " signer=", att.signer
    return false
  var existing = node.eventAttestations.getOrDefault(att.eventId, @[])
  existing.add(att)
  node.eventAttestations[att.eventId] = existing
  node.attestationsByKey[att.attestationKey()] = att
  node.noteEventCertificationAttestation(att)
  if not node.store.isNil:
    node.store.persistAttestation(att)
  when defined(fabric_submit_diag):
    echo "att-stage install account=", node.identity.account,
      " event=", att.eventId,
      " role=", ord(att.role),
      " signer=", att.signer,
      " total=", existing.len
  let event = node.events[att.eventId]
  if enqueuePending and att.eventId notin node.eventCertificates and
      node.attestationRequiresRelay(event, att):
    if node.attestationNeedsRelay(event, att):
      discard node.pendingAttestations.enqueuePending(att.attestationKey())
    else:
      discard node.routeBlockedAttestations.enqueuePending(att.attestationKey())
    when defined(fabric_submit_diag):
      echo "att-stage enqueue account=", node.identity.account,
        " event=", att.eventId,
        " role=", ord(att.role),
        " signer=", att.signer,
        " pending=", node.pendingAttestations.len
  discard node.refreshCertificationEvent(att.eventId, force = true)
  node.scheduleStateMaintenance()
  true

proc registerCandidate(node: FabricNode, candidate: CheckpointCandidate): bool =
  if node.checkpointCandidates.hasKey(candidate.candidateId):
    return false
  when defined(fabric_checkpoint_diag):
    echo "checkpoint-candidate ", node.identity.account,
      " era=", candidate.era,
      " id=", candidate.candidateId
  node.checkpointCandidates[candidate.candidateId] = candidate
  if not node.store.isNil:
    node.store.persistCheckpointCandidate(candidate)
  discard node.pendingCheckpointCandidates.enqueuePending(candidate.candidateId)
  node.scheduleCheckpointAdvance()
  true

proc registerCheckpointVote(node: FabricNode, vote: CheckpointVote): bool =
  if vote.candidateId notin node.checkpointCandidates:
    return false
  if not verifyCheckpointVote(vote):
    return false
  if node.voteBy(vote.candidateId, vote.validator):
    return false
  when defined(fabric_checkpoint_diag):
    echo "checkpoint-vote ", node.identity.account,
      " candidate=", vote.candidateId,
      " validator=", vote.validator,
      " weight=", vote.weight
  var existing = node.checkpointVotes.getOrDefault(vote.candidateId, @[])
  existing.add(vote)
  node.checkpointVotes[vote.candidateId] = existing
  if not node.store.isNil:
    node.store.persistCheckpointVote(vote)
  discard node.pendingCheckpointVotes.enqueuePending(vote.checkpointVoteKey())
  node.scheduleCheckpointAdvance()
  true

proc registerCheckpoint(node: FabricNode, cert: CheckpointCertificate, snapshot: ChainStateSnapshot): bool =
  if node.checkpoints.hasKey(cert.checkpointId):
    return false
  when defined(fabric_checkpoint_diag):
    echo "checkpoint-cert ", node.identity.account,
      " era=", cert.candidate.era,
      " id=", cert.checkpointId,
      " votes=", cert.votes.len,
      " quorum=", cert.quorumWeight
  node.checkpoints[cert.checkpointId] = cert
  node.checkpointSnapshots[cert.checkpointId] = snapshot
  let bundle = node.cachedCheckpointBundle(cert, snapshot)
  if node.shouldPromoteLatestCheckpoint(cert.checkpointId, snapshot):
    node.latestCheckpointId = cert.checkpointId
  node.pruneSupersededCheckpointBundles(node.latestCheckpointId)
  if not node.store.isNil:
    node.store.persistCheckpointRaw(cert, checkpointRaw(cert), bundle.snapshot)
  node.pendingCheckpointCandidates.excl(cert.candidate.candidateId)
  node.completeCheckpointCandidateDelivery(cert.candidate.candidateId)
  node.clearCheckpointVotePending(cert.candidate.candidateId)
  for vote in cert.votes:
    node.completeCheckpointVoteDelivery(vote.checkpointVoteKey())
  discard node.pendingCheckpointBundles.enqueuePending(cert.checkpointId)
  for eventId, event in node.events.pairs:
    if event.clock.era < cert.candidate.era:
      node.clearCertificationEventState(eventId)
      node.completeEventDelivery(eventId)
      node.clearEventAttestationDeliveries(eventId)
      if eventId in node.eventCertificates:
        node.completeEventCertificateDelivery(eventId)
  for proposalId in cert.candidate.adoptedProposalIds:
    if not node.avoAdoptions.hasKey(proposalId):
      let adoption = AvoAdoptionRecord(
        proposalId: proposalId,
        adoptedInCheckpointId: cert.checkpointId,
        activatesAtEra: cert.candidate.era + 1,
        adoptedAt: cert.createdAt,
      )
      node.avoAdoptions[proposalId] = adoption
      if not node.store.isNil:
        node.store.persistAvoAdoption(adoption)
  node.scheduleStateMaintenance()
  true

proc registerCheckpointFetched(
  node: FabricNode,
  cert: CheckpointCertificate,
  snapshot: ChainStateSnapshot,
  checkpointRaw, snapshotRaw: seq[byte],
): bool =
  if node.checkpoints.hasKey(cert.checkpointId):
    return false
  node.checkpoints[cert.checkpointId] = cert
  node.checkpointSnapshots[cert.checkpointId] = snapshot
  node.checkpointBundleCache[cert.checkpointId] = CheckpointBundle(
    certificate: cert,
    snapshot: snapshotRaw,
  )
  if node.shouldPromoteLatestCheckpoint(cert.checkpointId, snapshot):
    node.latestCheckpointId = cert.checkpointId
  node.pruneSupersededCheckpointBundles(node.latestCheckpointId)
  if not node.store.isNil:
    node.store.persistCheckpointRaw(cert, checkpointRaw, snapshotRaw)
  node.pendingCheckpointCandidates.excl(cert.candidate.candidateId)
  node.completeCheckpointCandidateDelivery(cert.candidate.candidateId)
  node.clearCheckpointVotePending(cert.candidate.candidateId)
  for vote in cert.votes:
    node.completeCheckpointVoteDelivery(vote.checkpointVoteKey())
  discard node.pendingCheckpointBundles.enqueuePending(cert.checkpointId)
  for eventId, event in node.events.pairs:
    if event.clock.era < cert.candidate.era:
      node.clearCertificationEventState(eventId)
      node.completeEventDelivery(eventId)
      node.clearEventAttestationDeliveries(eventId)
      if eventId in node.eventCertificates:
        node.completeEventCertificateDelivery(eventId)
  for proposalId in cert.candidate.adoptedProposalIds:
    if not node.avoAdoptions.hasKey(proposalId):
      node.avoAdoptions[proposalId] = AvoAdoptionRecord(
        proposalId: proposalId,
        adoptedInCheckpointId: cert.checkpointId,
        activatesAtEra: cert.candidate.era + 1,
        adoptedAt: cert.createdAt,
      )
  node.scheduleStateMaintenance()
  true

proc registerAvoProposal(node: FabricNode, proposal: AvoProposal): bool =
  if node.avoProposals.hasKey(proposal.proposalId):
    return false
  node.avoProposals[proposal.proposalId] = proposal
  if not node.store.isNil:
    node.store.persistAvoProposal(proposal)
  discard node.pendingAvoProposals.enqueuePending(proposal.proposalId)
  node.scheduleStateMaintenance()
  true

proc registerAvoApproval(node: FabricNode, proposalId, validator: string): bool =
  if proposalId notin node.avoProposals:
    return false
  var approvals = node.avoApprovals.getOrDefault(proposalId, @[])
  if validator in approvals:
    return false
  approvals.add(validator)
  approvals.sort(system.cmp[string])
  node.avoApprovals[proposalId] = approvals
  if not node.store.isNil:
    node.store.persistAvoApproval(proposalId, validator)
  discard node.pendingAvoApprovals.enqueuePending(avoApprovalKey(proposalId, validator))
  node.scheduleStateMaintenance()
  true

proc frontierDigest(events: seq[FabricEvent]): EventFrontier =
  var childSet = initHashSet[string]()
  for event in events:
    for parent in event.parents:
      childSet.incl(parent)
    result.maxClock = maxClock(result.maxClock, event.clock)
  for event in events:
    if event.eventId notin childSet:
      result.certifiedEventIds.add(event.eventId)
      result.era = max(result.era, event.clock.era)
  result.certifiedEventIds.sort(system.cmp[string])
  result.frontierDigest = hashHex(result.certifiedEventIds.join(":"))

proc checkpointState(node: FabricNode, checkpointId = ""): Option[ChainState] =
  let targetId =
    if checkpointId.len > 0: checkpointId
    else: node.latestCheckpointId
  if targetId.len == 0 or targetId notin node.checkpointSnapshots:
    return none(ChainState)
  some(fromSnapshot(node.checkpointSnapshots[targetId]))

proc hasCheckpointForCandidate(
    node: FabricNode, candidateId: string
): bool {.gcsafe, raises: [].} =
  for _, checkpoint in node.checkpoints.pairs:
    if checkpoint.candidate.candidateId == candidateId:
      return true
  false

proc activeAvoSetForEra(node: FabricNode, era: uint64): seq[string] =
  for proposalId, adoption in node.avoAdoptions.pairs:
    if adoption.activatesAtEra <= era:
      result.add(proposalId)
  result.sort(system.cmp[string])

proc approvalWeight(node: FabricNode, proposalId: string, stateAtEra: ChainState): uint64 =
  for validator in node.avoApprovals.getOrDefault(proposalId, @[]):
    result += stateAtEra.validatorWeight(validator)

proc buildCheckpointCandidate(node: FabricNode, era: uint64): CheckpointCandidate =
  let stateAtEra = node.replayState(era)
  let frontier = frontierDigest(node.certifiedEvents(era))
  var adopted: seq[string] = @[]
  for proposalId, proposal in node.avoProposals.pairs:
    if proposal.checkpointStartEra <= era and era <= proposal.checkpointEndEra:
      if node.approvalWeight(proposalId, stateAtEra) >= stateAtEra.quorumWeight():
        adopted.add(proposalId)
  result = CheckpointCandidate(
    candidateId: "",
    era: era,
    frontierDigest: frontier.frontierDigest,
    projectionRoots: stateAtEra.projectionRoots(),
    activeContractRoots: allSystemContractRoots(),
    activeAvoSet: node.activeAvoSetForEra(era),
    adoptedProposalIds: sortedUnique(adopted),
    isolatedEventIds: node.isolatedEvents.keys.toSeq.sorted(system.cmp[string]),
    snapshotRoot: hashHex(encodeObj(stateAtEra.snapshot())),
    validatorSetRoot: hashHex(encodeObj(stateAtEra.snapshot().validators)),
    createdAt: int64(era * 60),
  )
  result.candidateId = computeCheckpointCandidateId(result)

proc checkpointCandidateIdForEra(
    node: FabricNode, era: uint64
): string {.gcsafe, raises: [].} =
  for candidateId, candidate in node.checkpointCandidates.pairs:
    if candidate.era == era:
      return candidateId
  ""

proc checkpointCandidateIdsForEra(
    node: FabricNode, era: uint64
): seq[string] {.gcsafe, raises: [].} =
  for candidateId, candidate in node.checkpointCandidates.pairs:
    if candidate.era == era:
      result.add(candidateId)

proc checkpointEraReadiness(
    node: FabricNode, era: uint64
): tuple[
    ready: bool,
    hasCertified: bool,
    eraEvents: int,
    certified: int,
    isolated: int,
    firstMissing: string,
    minTick: int,
    maxTick: int,
  ] =
  node.refreshEraCheckpointStates()
  let state = node.checkpointEraState(era)
  result.eraEvents = state.eventTotal
  result.certified = state.certifiedTotal
  result.isolated = state.isolatedTotal
  result.firstMissing = state.firstMissingEventId
  result.minTick = state.minTick
  result.maxTick = state.maxTick
  result.hasCertified = state.certifiedTotal > 0
  result.ready = state.firstMissingEventId.len == 0 and result.hasCertified

proc eraReadyForCheckpointCandidate(node: FabricNode, era: uint64): bool =
  node.checkpointEraReadiness(era).ready

proc validateEventInvariants(node: FabricNode, event: FabricEvent): Option[IsolationRecord] =
  if event.contractRoot notin allSystemContractRoots():
    return some(IsolationRecord(
      isolationId: "",
      eventId: event.eventId,
      kind: ikInvalidContractRoot,
      reason: "unknown contract root",
      witnesses: @[],
      createdAt: event.createdAt,
    ))
  if not verifyTx(event.legacyTx):
    return some(IsolationRecord(
      isolationId: "",
      eventId: event.eventId,
      kind: ikInvalidParticipantSignature,
      reason: "legacy tx signature invalid",
      witnesses: @[],
      createdAt: event.createdAt,
    ))
  try:
    node.liveState.verifyContractProjection(event)
  except ValueError as exc:
    return some(IsolationRecord(
      isolationId: "",
      eventId: event.eventId,
      kind: ikInvalidContractRoot,
      reason: exc.msg,
      witnesses: @[],
      createdAt: event.createdAt,
    ))
  for _, existing in node.events.pairs:
    if existing.eventId == event.eventId:
      continue
    if existing.legacyTx.sender == event.legacyTx.sender and existing.legacyTx.nonce == event.legacyTx.nonce:
      let kind =
        if event.contractRoot == CreditContractRoot or
            event.contractRoot == TreasuryContractRoot or
            event.contractRoot == StakingContractRoot or
            event.contractRoot == LedgerContractRoot:
          ikDoubleSpend
        else:
          ikDoubleThread
      return some(IsolationRecord(
        isolationId: "",
        eventId: event.eventId,
        kind: kind,
        reason: "sender nonce already used by another event",
        witnesses: @[],
        createdAt: event.createdAt,
      ))
    if existing.threadId == event.threadId and clockCmp(existing.clock, event.clock) >= 0:
      return some(IsolationRecord(
        isolationId: "",
        eventId: event.eventId,
        kind: ikReverseClock,
        reason: "thread clock regressed or duplicated",
        witnesses: @[],
        createdAt: event.createdAt,
      ))
  none(IsolationRecord)

proc registerIsolation(node: FabricNode, record: IsolationRecord): bool =
  if node.isolatedEvents.hasKey(record.eventId):
    return false
  var normalized = record
  normalized.isolationId = computeIsolationId(normalized)
  node.isolatedEvents[record.eventId] = normalized
  if not node.store.isNil:
    node.store.persistIsolation(normalized)
  node.completeEventDelivery(record.eventId)
  node.clearEventAttestationDeliveries(record.eventId)
  node.completeEventCertificateDelivery(record.eventId)
  node.deferredAttestations.del(record.eventId)
  node.deferredEventCertificates.del(record.eventId)
  node.clearCertificationEventState(record.eventId)
  true

proc replayDeferredAttestations(node: FabricNode, eventId: string) =
  if node.isNil or eventId.len == 0 or eventId notin node.deferredAttestations:
    return
  let buffered = node.deferredAttestations.getOrDefault(eventId, @[])
  node.deferredAttestations.del(eventId)
  for att in buffered:
    discard node.registerAttestation(att, enqueuePending = false)

proc replayDeferredEventCertificates(node: FabricNode, eventId: string) =
  if node.isNil or eventId.len == 0 or eventId notin node.deferredEventCertificates:
    return
  let buffered = node.deferredEventCertificates.getOrDefault(eventId, @[])
  node.deferredEventCertificates.del(eventId)
  for cert in buffered:
    discard node.registerEventCertificate(cert, enqueuePending = false)

proc registerEvent(
    node: FabricNode, event: FabricEvent, enqueuePending = true
): bool =
  when defined(fabric_lsmr_diag):
    echo "fabric-node register-event self=", node.identity.account,
      " event=", event.eventId,
      " duplicate=", node.events.hasKey(event.eventId),
      " isolated=", node.isolatedEvents.hasKey(event.eventId)
  when defined(fabric_submit_diag):
    echo "register-stage begin account=", node.identity.account, " event=", event.eventId
  if node.events.hasKey(event.eventId) or node.isolatedEvents.hasKey(event.eventId):
    when defined(fabric_submit_diag):
      echo "register-stage duplicate account=", node.identity.account, " event=", event.eventId
    return false
  when defined(fabric_submit_diag):
    echo "register-stage validate account=", node.identity.account, " event=", event.eventId
  let isolation = node.validateEventInvariants(event)
  if isolation.isSome():
    when defined(fabric_submit_diag):
      echo "register-stage isolated account=", node.identity.account, " event=", event.eventId,
        " kind=", ord(isolation.get().kind)
    discard node.registerIsolation(isolation.get())
    return false
  when defined(fabric_submit_diag):
    echo "register-stage install-route account=", node.identity.account, " event=", event.eventId
  node.installRouteAddrs(event.participantRoutes)
  node.installRouteAddrs(event.eventWitnessRoutes())
  node.events[event.eventId] = event
  node.eventCertificationProgress[event.eventId] = initEventCertificationProgress(event)
  when defined(fabric_submit_diag):
    echo "register-stage persist account=", node.identity.account, " event=", event.eventId
  if not node.store.isNil:
    node.store.persistEvent(event)
  when defined(fabric_submit_diag):
    echo "register-stage children account=", node.identity.account, " event=", event.eventId
  for parent in event.parents:
    var existing = node.children.getOrDefault(parent, @[])
    if event.eventId notin existing:
      existing.add(event.eventId)
    node.children[parent] = existing
  node.replayDeferredAttestations(event.eventId)
  node.replayDeferredEventCertificates(event.eventId)
  if enqueuePending:
    discard node.pendingEvents.enqueuePending(event.eventId)
  discard node.refreshCertificationEvent(event.eventId, force = true)
  node.scheduleStateMaintenance()
  when defined(fabric_submit_diag):
    echo "register-stage done account=", node.identity.account, " event=", event.eventId
  true

proc activateDependentDeliveries(node: FabricNode, eventId: string) {.gcsafe, raises: [].} =
  if node.isNil or eventId.len == 0:
    return
  if eventId notin node.events or eventId in node.isolatedEvents:
    return
  let event = node.events.getOrDefault(eventId)
  var changed = false
  if eventId in node.eventCertificates:
    discard node.markPendingEventCertificate(eventId)
    changed = true
  else:
    for att in node.eventAttestations.getOrDefault(eventId, @[]):
      if node.attestationRequiresRelay(event, att):
        if node.attestationNeedsRelay(event, att):
          changed = node.pendingAttestations.enqueuePending(att.attestationKey()) or changed
        else:
          changed = node.routeBlockedAttestations.enqueuePending(att.attestationKey()) or changed
  if changed:
    when defined(fabric_submit_diag):
      echo "dependent-gate release account=", node.identity.account,
        " event=", eventId,
        " cert=", (eventId in node.eventCertificates)
    node.scheduleMaintenance(immediate = true)

proc maybeActivateDependentDeliveries(node: FabricNode, eventId: string) =
  if node.isNil or eventId.len == 0 or eventId notin node.events or eventId in node.isolatedEvents:
    return
  let event = node.events.getOrDefault(eventId)
  if not node.eventDependentsMayRelay(event):
    when defined(fabric_submit_diag):
      echo "dependent-gate hold account=", node.identity.account,
        " event=", eventId
    return
  node.activateDependentDeliveries(eventId)

proc registerEventCertificate(
    node: FabricNode, cert: EventCertificate, enqueuePending = true
): bool =
  when defined(fabric_diag):
    echo "register-cert-start ", node.identity.account, " event=", cert.eventId
  if cert.eventId notin node.events:
    when defined(fabric_submit_diag):
      echo "cert-stage defer-missing-event account=", node.identity.account,
        " event=", cert.eventId
    return node.bufferDeferredEventCertificate(cert)
  if node.eventCertificates.hasKey(cert.eventId):
    when defined(fabric_diag):
      echo "register-cert-skip ", node.identity.account, " event=", cert.eventId
    when defined(fabric_submit_diag):
      echo "cert-stage skip-known account=", node.identity.account,
        " event=", cert.eventId,
        " total=", node.eventCertificates.len
    return false
  for parent in node.events[cert.eventId].parents:
    if parent in node.events and parent notin node.eventCertificates:
      when defined(fabric_diag):
        echo "register-cert-parent-missing ", node.identity.account, " event=", cert.eventId, " parent=", parent
      when defined(fabric_submit_diag):
        echo "cert-stage defer-parent account=", node.identity.account,
          " event=", cert.eventId,
          " parent=", parent
      return node.bufferDeferredEventCertificate(cert)
  node.eventCertificates[cert.eventId] = cert
  when defined(fabric_submit_diag):
    echo "cert-stage installed account=", node.identity.account,
      " event=", cert.eventId,
      " total=", node.eventCertificates.len
  when defined(fabric_checkpoint_diag):
    echo "event-cert ", node.identity.account,
      " total=", node.eventCertificates.len,
      " era=", cert.clock.era,
      " tick=", cert.clock.tick,
      " event=", cert.eventId
  when defined(fabric_diag):
    echo "register-cert-before-live ", node.identity.account, " event=", cert.eventId
  node.advanceLiveApplyFrom(cert.eventId)
  when defined(fabric_diag):
    echo "register-cert-after-live ", node.identity.account, " event=", cert.eventId
  if not node.store.isNil:
    node.store.persistEventCertificate(cert)
  for childEventId in node.children.getOrDefault(cert.eventId, @[]):
    node.replayDeferredEventCertificates(childEventId)
  when defined(fabric_submit_diag):
    echo "cert-stage register account=", node.identity.account,
      " event=", cert.eventId,
      " pendingEvents-before=", node.pendingEvents.len,
      " pendingAtts-before=", node.pendingAttestations.len,
      " lsmrPrimary=", node.usesLsmrPrimary()
  node.clearEventAttestationDeliveries(cert.eventId)
  node.deferredAttestations.del(cert.eventId)
  node.clearCertificationEventState(cert.eventId)
  when defined(fabric_submit_diag):
    echo "cert-stage register account=", node.identity.account,
      " event=", cert.eventId,
      " pendingEvents-after=", node.pendingEvents.len,
      " pendingAtts-after=", node.pendingAttestations.len
  if enqueuePending:
    discard node.markPendingEventCertificate(cert.eventId)
  else:
    node.maybeActivateDependentDeliveries(cert.eventId)
  node.scheduleCheckpointAdvance()
  true

proc eventParticipantSatisfied(node: FabricNode, event: FabricEvent): bool =
  node.ensureEventCertificationProgress(event.eventId)
  if event.eventId notin node.eventCertificationProgress:
    return false
  let progress = node.eventCertificationProgress.getOrDefault(event.eventId)
  for participant in event.participants:
    if participant notin progress.participantAttestations:
      return false
  true

proc witnessCount(node: FabricNode, event: FabricEvent, component: seq[FabricNode]): tuple[reachable: int, threshold: int, signed: int, witnessAtts: seq[EventAttestation]] {.gcsafe, raises: [].} =
  discard component
  node.ensureEventCertificationProgress(event.eventId)
  if event.eventId notin node.eventCertificationProgress:
    return
  let progress = node.eventCertificationProgress.getOrDefault(event.eventId)
  result.reachable = progress.reachableWitnesses
  result.threshold = progress.witnessThreshold
  var witnessIds = progress.witnessAttestations.keys.toSeq()
  witnessIds.sort(system.cmp[string])
  for witnessId in witnessIds:
    let att = progress.witnessAttestations.getOrDefault(witnessId)
    if att.eventId.len > 0:
      result.witnessAtts.add(att)
  result.signed = result.witnessAtts.len

proc certificationCheckpointEra(node: FabricNode): uint64 {.gcsafe, raises: [].} =
  if node.isNil or node.latestCheckpointId.len == 0 or
      node.latestCheckpointId notin node.checkpoints:
    return 0'u64
  node.checkpoints.getOrDefault(node.latestCheckpointId).candidate.era

proc certificationEventTracked(
    node: FabricNode, eventId: string
): bool {.gcsafe, raises: [].} =
  if node.isNil or not node.usesLsmrPrimary() or
      eventId.len == 0 or eventId notin node.events:
    return false
  if eventId in node.isolatedEvents or eventId in node.eventCertificates:
    return false
  let event = node.events.getOrDefault(eventId)
  if event.author != node.identity.account:
    return false
  node.refreshEventCertificationProgressState(eventId)
  if eventId notin node.eventCertificationProgress:
    return false
  let progress = node.eventCertificationProgress.getOrDefault(eventId)
  progress.status == agsWaitingParticipants or
    progress.status == agsWaitingWitnesses or
    progress.status == agsCertReady

proc clearCertificationEventState(
    node: FabricNode, eventId: string
) {.gcsafe, raises: [].} =
  if node.isNil or eventId.len == 0:
    return
  node.dropPendingCertificationEvent(eventId)
  node.clearQueuedWitnessPullsForEvent(eventId)
  node.clearInflightWitnessPullsForEvent(eventId)

proc refreshCertificationEvent(
    node: FabricNode, eventId: string, force = false
): bool {.gcsafe, raises: [].} =
  if node.isNil or eventId.len == 0:
    return false
  if not node.certificationEventTracked(eventId):
    node.clearCertificationEventState(eventId)
    return false
  result = node.touchPendingCertificationEvent(eventId, force = force)
  if result:
    node.scheduleCertificationGather()

proc refreshPendingCertificationEvents(node: FabricNode) {.gcsafe, raises: [].} =
  if node.isNil or not node.usesLsmrPrimary():
    return
  let overlayDigest = node.lsmrOverlayDigest
  let latestCheckpointId = node.latestCheckpointId
  if overlayDigest == node.certificationObservedOverlayDigest and
      latestCheckpointId == node.certificationObservedLatestCheckpointId:
    return
  node.certificationObservedOverlayDigest = overlayDigest
  node.certificationObservedLatestCheckpointId = latestCheckpointId
  for eventId, event in node.events.pairs:
    if event.author == node.identity.account:
      discard node.refreshCertificationEvent(eventId, force = true)

proc prunePendingCertificationState(node: FabricNode) {.gcsafe, raises: [].} =
  if node.isNil:
    return
  for eventId in node.pendingCertificationEvents.toSeq():
    if not node.certificationEventTracked(eventId):
      node.clearCertificationEventState(eventId)
  for ownerKey in node.pendingWitnessPullsByPeer.keys.toSeq():
    var queued = node.pendingWitnessPullsByPeer.getOrDefault(
      ownerKey,
      initDeque[AntiEntropyPullWorkItem](),
    )
    var kept = initDeque[AntiEntropyPullWorkItem]()
    var changed = false
    while queued.len > 0:
      let item = queued.popFirst()
      if not node.witnessPullStillNeeded(item):
        node.clearPendingWitnessPullKey(ownerKey, item.key)
        changed = true
        continue
      kept.addLast(item)
    if not changed:
      continue
    if kept.len == 0:
      node.pendingWitnessPullsByPeer.del(ownerKey)
    else:
      node.pendingWitnessPullsByPeer[ownerKey] = kept
  for ownerKey in node.inflightWitnessPullByPeer.keys.toSeq():
    let item = node.inflightWitnessPullByPeer.getOrDefault(ownerKey)
    if not node.witnessPullStillNeeded(item):
      node.inflightWitnessPullByPeer.del(ownerKey)

proc certificationMissingWitnessPulls(
    node: FabricNode, eventId: string
): seq[AntiEntropyPullWorkItem] {.gcsafe, raises: [].} =
  if node.isNil or not node.certificationEventTracked(eventId):
    return @[]
  let event = node.events.getOrDefault(eventId)
  node.refreshEventCertificationProgressState(eventId)
  if eventId notin node.eventCertificationProgress:
    return @[]
  let progress = node.eventCertificationProgress.getOrDefault(eventId)
  let missingNeeded = progress.witnessThreshold - progress.witnessAttestations.len
  if missingNeeded <= 0:
    return @[]
  for witnessAccount in progress.missingWitnessAccounts.toSeq().sorted(system.cmp[string]):
    if witnessAccount.len == 0 or witnessAccount == node.identity.account:
      continue
    var route = none(RoutingPeer)
    if witnessAccount in progress.witnessRoutes and
        progress.witnessRoutes.getOrDefault(witnessAccount).peerId.len > 0:
      route = some(progress.witnessRoutes.getOrDefault(witnessAccount))
    if route.isNone():
      try:
        route = node.routingPeerForAccount(witnessAccount)
      except CatchableError:
        route = none(RoutingPeer)
    if route.isNone():
      continue
    let target = route.get()
    if target.peerId.len == 0 or node.routeMatchesSelf(target):
      continue
    result.add(AntiEntropyPullWorkItem(
      domain: aedAttWitness,
      key: attestationFetchKey(eventId, arWitness, witnessAccount),
      causedByEventId: eventId,
      targetPeerId: target.peerId,
      clock: event.clock,
    ))
    if result.len >= missingNeeded:
      break

proc certificationMissingParticipantPulls(
    node: FabricNode, eventId: string
): seq[AntiEntropyPullWorkItem] {.gcsafe, raises: [].} =
  if node.isNil or not node.certificationEventTracked(eventId):
    return @[]
  let event = node.events.getOrDefault(eventId)
  node.refreshEventCertificationProgressState(eventId)
  if eventId notin node.eventCertificationProgress:
    return @[]
  let progress = node.eventCertificationProgress.getOrDefault(eventId)
  if progress.status != agsWaitingParticipants:
    return @[]
  for participant in event.participants.sorted(system.cmp[string]):
    if participant.len == 0 or participant == node.identity.account or
        participant in progress.participantAttestations:
      continue
    var route = none(RoutingPeer)
    try:
      route = node.routingPeerForAccount(participant)
    except CatchableError:
      route = none(RoutingPeer)
    if route.isNone():
      continue
    let target = route.get()
    if target.peerId.len == 0 or node.routeMatchesSelf(target):
      continue
    result.add(AntiEntropyPullWorkItem(
      domain: aedAttParticipant,
      key: attestationFetchKey(eventId, arParticipant, participant),
      causedByEventId: eventId,
      targetPeerId: target.peerId,
      clock: event.clock,
    ))

proc missingEventCertificatePulls(
    node: FabricNode
): seq[AntiEntropyPullWorkItem] {.gcsafe, raises: [].} =
  if node.isNil:
    return @[]
  node.refreshEraCheckpointStates()
  let checkpointKnown =
    node.latestCheckpointId.len > 0 and node.latestCheckpointId in node.checkpoints
  let checkpointEra =
    if checkpointKnown:
      node.latestCheckpointEra()
    else:
      0'u64
  var seen = initHashSet[string]()
  var frontierEventIds: seq[string] = @[]
  for era in node.eraCheckpointStates.keys.toSeq().sorted(system.cmp[uint64]):
    let state = node.checkpointEraState(era)
    if (checkpointKnown and era < checkpointEra) or state.firstMissingEventId.len == 0:
      continue
    frontierEventIds.add(state.firstMissingEventId)
    seen.incl(state.firstMissingEventId)
  for eventId in frontierEventIds:
    if eventId notin node.events or eventId in node.eventCertificates or
        eventId in node.isolatedEvents:
      continue
    let event = node.events.getOrDefault(eventId)
    let authorRoute = node.eventAuthorRoute(event)
    if authorRoute.isNone():
      continue
    let target = authorRoute.get()
    if target.peerId.len == 0 or node.routeMatchesSelf(target):
      continue
    result.add(AntiEntropyPullWorkItem(
      domain: aedEventCert,
      key: "eventcert:" & eventId,
      causedByEventId: eventId,
      targetPeerId: target.peerId,
      clock: event.clock,
    ))
  result.sort(proc(a, b: AntiEntropyPullWorkItem): int =
    result = clockCmp(a.clock, b.clock)
    if result == 0:
      result = cmp(a.causedByEventId, b.causedByEventId)
      if result == 0:
        result = cmp(a.targetPeerId, b.targetPeerId)
  )

proc witnessPullFetchKey(
    item: AntiEntropyPullWorkItem
): string {.gcsafe, raises: [].} =
  item.key

proc applyAntiEntropyPullPayload(
    node: FabricNode, item: AntiEntropyPullWorkItem, payload: seq[byte]
): bool {.raises: [].} =
  if node.isNil or payload.len == 0:
    return false
  try:
    let text = stringOf(payload)
    case item.domain
    of aedEvent:
      return node.registerEvent(decodeObj[FabricEvent](text), enqueuePending = false)
    of aedAttParticipant, aedAttWitness:
      let att = decodeObj[EventAttestation](text)
      if attestationFetchKey(att.eventId, att.role, att.signer) != item.key:
        return false
      return node.registerAttestation(att, enqueuePending = false)
    of aedEventCert:
      return node.registerEventCertificate(
        decodeObj[EventCertificate](text),
        enqueuePending = false,
      )
    of aedCheckpointCandidate:
      return node.registerCandidate(decodeObj[CheckpointCandidate](text))
    of aedCheckpointVote:
      return node.registerCheckpointVote(decodeObj[CheckpointVote](text))
    of aedCheckpointBundle:
      let bundle = decodeObj[CheckpointBundle](text)
      return node.registerCheckpointFetched(
        bundle.certificate,
        decodePolarSnapshot(bundle.snapshot),
        checkpointRaw(bundle.certificate),
        bundle.snapshot,
      )
  except Exception:
    return false

proc witnessPullTask(node: FabricNode, ownerKey: string): Future[void] {.async: (raises: []).} =
  try:
    while not node.isNil and node.running:
      if ownerKey in node.inflightWitnessPullByPeer:
        break
      if ownerKey notin node.pendingWitnessPullsByPeer:
        break
      var queue = node.pendingWitnessPullsByPeer.getOrDefault(
        ownerKey,
        initDeque[AntiEntropyPullWorkItem](),
      )
      if queue.len == 0:
        node.pendingWitnessPullsByPeer.del(ownerKey)
        break
      let item = queue.peekFirst()
      discard queue.popFirst()
      if queue.len == 0:
        node.pendingWitnessPullsByPeer.del(ownerKey)
      else:
        node.pendingWitnessPullsByPeer[ownerKey] = queue
      node.clearPendingWitnessPullKey(ownerKey, item.key)
      node.inflightWitnessPullByPeer[ownerKey] = item
      try:
        if node.witnessPullStillNeeded(item):
          let key = item.witnessPullFetchKey()
          if key.len > 0 and not node.network.isNil:
            when defined(fabric_submit_diag):
              echo "anti-entropy exact-pull account=", node.identity.account,
                " peer=", item.targetPeerId,
                " domain=", antiEntropyDomainLabel(item.domain),
                " key=", key
            let payload = await node.network.fetchRaw(
              item.targetPeerId,
              key,
              timeout = 1.seconds,
              maxAttempts = 1,
            )
            if payload.isSome() and node.witnessPullStillNeeded(item):
              node.enqueueInbound(InboundMessage(
                kind: imAntiEntropyPull,
                antiEntropyPullItem: item,
                antiEntropyPullPayload: payload.get(),
              ))
      except CancelledError:
        raise
      except CatchableError:
        discard
      finally:
        let inflight = node.inflightWitnessPullByPeer.getOrDefault(ownerKey)
        if inflight.key == item.key:
          node.inflightWitnessPullByPeer.del(ownerKey)
        if item.causedByEventId.len > 0 and
            node.refreshCertificationEvent(item.causedByEventId, force = true):
          node.scheduleStateMaintenance()
  except CancelledError:
    discard
  finally:
    if not node.isNil:
      if ownerKey in node.witnessPullRunners:
        node.witnessPullRunners.del(ownerKey)
      if node.running and ownerKey in node.pendingWitnessPullsByPeer and
          node.pendingWitnessPullsByPeer.getOrDefault(ownerKey).len > 0:
        let runner = node.witnessPullTask(ownerKey)
        node.witnessPullRunners[ownerKey] = runner
        asyncSpawn runner

proc ensureWitnessPullRunner(
    node: FabricNode, targetPeerId: string, domain: AntiEntropyDomain
) {.gcsafe, raises: [].} =
  let ownerKey = antiEntropyPullOwnerKey(targetPeerId, domain)
  if node.isNil or not node.running or ownerKey.len == 0 or
      ownerKey notin node.pendingWitnessPullsByPeer or
      node.pendingWitnessPullsByPeer.getOrDefault(ownerKey).len == 0:
    return
  if ownerKey in node.witnessPullRunners:
    let runner = node.witnessPullRunners.getOrDefault(ownerKey)
    if not runner.isNil and not runner.finished():
      return
  let runner = node.witnessPullTask(ownerKey)
  node.witnessPullRunners[ownerKey] = runner
  asyncSpawn runner

proc maybeCertifyEvent(node: FabricNode, event: FabricEvent, component: seq[FabricNode]): bool =
  when defined(fabric_diag):
    echo "maybe-cert-start ", node.identity.account, " event=", event.eventId
  when defined(fabric_submit_diag):
    echo "maybe-cert-stage start account=", node.identity.account, " event=", event.eventId
  if node.usesLsmrPrimary() and event.author != node.identity.account:
    when defined(fabric_submit_diag):
      echo "maybe-cert-stage skip-non-author account=", node.identity.account,
        " event=", event.eventId,
        " author=", event.author
    return false
  if event.eventId in node.eventCertificates:
    when defined(fabric_diag):
      echo "maybe-cert-skip-known ", node.identity.account, " event=", event.eventId
    when defined(fabric_submit_diag):
      echo "maybe-cert-stage known account=", node.identity.account, " event=", event.eventId
    return false
  if not node.eventParticipantSatisfied(event):
    when defined(fabric_diag):
      echo "maybe-cert-missing-participant ", node.identity.account, " event=", event.eventId
    when defined(fabric_submit_diag):
      echo "maybe-cert-stage missing-participant account=", node.identity.account, " event=", event.eventId
    return false
  let witnessInfo = node.witnessCount(event, component)
  when defined(fabric_diag):
    echo "maybe-cert-witness ", node.identity.account, " event=", event.eventId,
      " signed=", witnessInfo.signed, " threshold=", witnessInfo.threshold
  when defined(fabric_submit_diag):
    echo "maybe-cert-stage witness account=", node.identity.account,
      " event=", event.eventId,
      " signed=", witnessInfo.signed,
      " threshold=", witnessInfo.threshold,
      " reachable=", witnessInfo.reachable
  if witnessInfo.signed < witnessInfo.threshold:
    when defined(fabric_diag):
      echo "maybe-cert-missing-witness ", node.identity.account, " event=", event.eventId
    when defined(fabric_submit_diag):
      echo "maybe-cert-stage missing-witness account=", node.identity.account, " event=", event.eventId
    return false
  var participantAtts: seq[EventAttestation] = @[]
  for participant in event.participants:
    for att in node.eventAttestations.getOrDefault(event.eventId, @[]):
      if att.role == arParticipant and att.signer == participant:
        participantAtts.add(att)
        break
  let cert = EventCertificate(
    certificateId: "",
    eventId: event.eventId,
    threadId: event.threadId,
    clock: event.clock,
    participantAttestations: participantAtts,
    witnessAttestations: witnessInfo.witnessAtts,
    reachableWitnesses: witnessInfo.reachable,
    witnessThreshold: witnessInfo.threshold,
    createdAt: event.createdAt,
  )
  var normalized = cert
  normalized.certificateId = computeEventCertificateId(normalized)
  when defined(fabric_diag):
    echo "maybe-cert-register ", node.identity.account, " event=", event.eventId
  when defined(fabric_submit_diag):
    echo "maybe-cert-stage register account=", node.identity.account, " event=", event.eventId
  let registered = node.registerEventCertificate(normalized, enqueuePending = false)
  when defined(fabric_submit_diag):
    echo "maybe-cert-stage result account=", node.identity.account,
      " event=", event.eventId,
      " registered=", registered,
      " certs=", node.eventCertificates.len
  registered

proc advanceSingleEvent(
    node: FabricNode, event: FabricEvent, touchedAttestationKeys: var seq[string]
) =
  let role =
    if node.identity.account in event.participants:
      some(arParticipant)
    elif node.nodeIsEventWitness(event):
      some(arWitness)
    else:
      none(AttestationRole)
  if role.isSome():
    let existing = node.localAttestation(event.eventId, node.identity.account, role.get())
    if existing.isNone():
      let att = node.makeAttestation(event, role.get())
      when defined(fabric_submit_diag):
        echo "advance-single att-create account=", node.identity.account,
          " event=", event.eventId,
          " role=", ord(role.get())
      if node.registerAttestation(att, enqueuePending = false):
        if node.attestationRequiresRelay(event, att):
          if node.attestationNeedsRelay(event, att):
            touchedAttestationKeys.appendUnique(att.attestationKey())
          else:
            discard node.routeBlockedAttestations.enqueuePending(att.attestationKey())

proc shareAttestation(nodes: seq[FabricNode], event: FabricEvent, att: EventAttestation): bool =
  var changed = false
  for peer in nodes:
    changed = peer.registerAttestation(att) or changed
  changed

proc shareCandidate(nodes: seq[FabricNode], candidate: CheckpointCandidate): bool =
  var changed = false
  for peer in nodes:
    changed = peer.registerCandidate(candidate) or changed
  changed

proc shareCheckpointVote(nodes: seq[FabricNode], vote: CheckpointVote): bool =
  var changed = false
  for peer in nodes:
    changed = peer.registerCheckpointVote(vote) or changed
  changed

proc shareCheckpoint(
    nodes: seq[FabricNode],
    cert: CheckpointCertificate,
    snapshot: ChainStateSnapshot,
    snapshotRaw: seq[byte],
): bool =
  var changed = false
  let bundle = CheckpointBundle(certificate: cert, snapshot: snapshotRaw)
  for peer in nodes:
    if cert.checkpointId.len > 0 and cert.checkpointId notin peer.checkpointBundleCache:
      peer.checkpointBundleCache[cert.checkpointId] = bundle
    changed = peer.registerCheckpoint(cert, snapshot) or changed
  changed

proc shareAvoProposal(nodes: seq[FabricNode], proposal: AvoProposal): bool =
  var changed = false
  for peer in nodes:
    changed = peer.registerAvoProposal(proposal) or changed
  changed

proc shareAvoApproval(nodes: seq[FabricNode], proposalId, validator: string): bool =
  var changed = false
  for peer in nodes:
    changed = peer.registerAvoApproval(proposalId, validator) or changed
  changed

proc publishFuture(node: FabricNode, fut: Future[void]) =
  if not fut.isNil:
    asyncSpawn fut

proc submitFuture(node: FabricNode, fut: Future[bool]) =
  if fut.isNil:
    return
  proc awaitSubmit() {.async: (raises: []).} =
    try:
      discard await fut
    except CatchableError:
      discard
  asyncSpawn awaitSubmit()

proc routePeers(node: FabricNode, event: FabricEvent): seq[RoutingPeer] =
  for route in event.participantRoutes:
    if route.account.len > 0 and route.peerId.len > 0 and route.account notin result.mapIt(it.account):
      result.add(route)
  for route in event.eventWitnessRoutes():
    if route.peerId.len == 0:
      continue
    if route.account.len > 0:
      if route.account notin result.mapIt(it.account):
        result.add(route)
    elif route.peerId notin result.mapIt(it.peerId):
      result.add(route)

proc eventAuthorRoute(
    node: FabricNode, event: FabricEvent
): Option[RoutingPeer] {.gcsafe, raises: [].} =
  if node.isNil:
    return none(RoutingPeer)
  if event.author == node.identity.account or
      event.authorPeerId == peerIdString(node.identity.peerId):
    return some(node.selfRoutingPeer())
  if event.authorPeerId.len > 0:
    let routed = node.routingPeerForPeerId(event.authorPeerId)
    if routed.isSome():
      var route = routed.get()
      if route.account.len == 0:
        route.account = event.author
      if route.path.len == 0:
        route.path = event.routingPath
      return some(route)
    return some(RoutingPeer(
      account: event.author,
      peerId: event.authorPeerId,
      path: event.routingPath,
      addrs: node.routeAddrs(event.authorPeerId),
    ))
  none(RoutingPeer)

proc allRoutingPeers(node: FabricNode): seq[RoutingPeer] =
  if node.usesLsmrPrimary():
    if not node.network.isNil and not node.network.switch.isNil:
      var seenPeerIds = initHashSet[string]()
      for peerId, record in node.network.switch.peerStore[ActiveLsmrBook].book.pairs:
        let peerIdText = $peerId
        if peerIdText.len == 0 or peerIdText in seenPeerIds:
          continue
        let item = RoutingPeer(
          account: "",
          peerId: peerIdText,
          path: record.data.certifiedPrefix(),
          addrs: node.routeAddrs(peerIdText),
        )
        if node.routeMatchesSelf(item):
          continue
        seenPeerIds.incl(peerIdText)
        result.add(item)
    return
  var seenPeerIds = initHashSet[string]()
  var seenAccounts = initHashSet[string]()
  for _, route in node.routingPeers.pairs:
    if route.account.len == 0 or route.peerId.len == 0:
      continue
    if node.routeMatchesSelf(route):
      continue
    if route.peerId in seenPeerIds or route.account in seenAccounts:
      continue
    seenPeerIds.incl(route.peerId)
    seenAccounts.incl(route.account)
    result.add(route)
  if not node.network.isNil and not node.network.switch.isNil:
    for peerId, _ in node.network.switch.peerStore[ActiveLsmrBook].book.pairs:
      let route = node.routingPeerForPeerId($peerId)
      if route.isNone():
        continue
      let item = route.get()
      if item.peerId.len == 0:
        continue
      if node.routeMatchesSelf(item):
        continue
      if item.peerId in seenPeerIds:
        continue
      if item.account.len > 0:
        seenAccounts.incl(item.account)
      seenPeerIds.incl(item.peerId)
      result.add(item)

proc validatorPeers(node: FabricNode, stateAtEra: ChainState): seq[RoutingPeer] =
  var seenPeerIds = initHashSet[string]()
  var seenAccounts = initHashSet[string]()
  for address, info in stateAtEra.validators.pairs:
    if not info.active or info.jailed:
      continue
    let route = node.routingPeerForAccount(address)
    if route.isNone():
      continue
    let item = route.get()
    if item.account.len == 0 or item.peerId.len == 0:
      continue
    if node.routeMatchesSelf(item):
      continue
    if item.peerId in seenPeerIds or item.account in seenAccounts:
      continue
    seenPeerIds.incl(item.peerId)
    seenAccounts.incl(item.account)
    result.add(item)

proc pathKey(path: LsmrPath): string =
  for idx, digit in path:
    if idx > 0:
      result.add('.')
    result.add($digit)

proc appendUniquePath(paths: var seq[LsmrPath], value: LsmrPath) =
  let key = value.pathKey()
  for existing in paths:
    if existing.pathKey() == key:
      return
  paths.add(value)

proc appendUniqueText(values: var seq[string], value: string) =
  if value.len == 0:
    return
  for existing in values:
    if existing == value:
      return
  values.add(value)

proc mergeScopePrefixes(
    scopeBook: var Table[string, seq[LsmrPath]], key: string, scopes: openArray[LsmrPath]
): bool {.gcsafe, raises: [].} =
  if key.len == 0:
    return false
  var existing = scopeBook.getOrDefault(key, @[])
  let beforeLen = existing.len
  for scope in scopes:
    if scope.len == 0:
      continue
    existing.appendUniquePath(scope)
  if existing.len == 0:
    if key in scopeBook:
      scopeBook.del(key)
    return false
  scopeBook[key] = existing
  existing.len != beforeLen

proc pendingScopePrefixes(
    scopeBook: Table[string, seq[LsmrPath]], key: string
): seq[LsmrPath] {.gcsafe, raises: [].} =
  if key.len == 0 or key notin scopeBook:
    return @[]
  scopeBook.getOrDefault(key, @[])

proc clearPendingScopePrefixes(
    scopeBook: var Table[string, seq[LsmrPath]], key: string
) {.gcsafe, raises: [].} =
  if key.len > 0 and key in scopeBook:
    scopeBook.del(key)

proc filterTargetPathsByScope(
    targetPaths: openArray[LsmrPath], scopePrefixes: openArray[LsmrPath]
): seq[LsmrPath] =
  var seen = initHashSet[string]()
  if scopePrefixes.len == 0:
    for targetPath in targetPaths:
      if targetPath.len == 0:
        continue
      let key = targetPath.pathKey()
      if key in seen:
        continue
      seen.incl(key)
      result.add(targetPath)
    return
  for targetPath in targetPaths:
    if targetPath.len == 0:
      continue
    let key = targetPath.pathKey()
    if key in seen:
      continue
    for scopePrefix in scopePrefixes:
      if scopePrefix.len == 0 or pathStartsWith(targetPath, scopePrefix):
        seen.incl(key)
        result.add(targetPath)
        break

template queueScopedSubmit(
    node: FabricNode,
    book: untyped,
    pendingBook: untyped,
    scopeBook: untyped,
    itemKey: string,
    targetPeerId: string,
    scopePrefix: LsmrPath,
    priority: OutboundSubmitPriority,
    submitProc: untyped,
) =
  let forwardKey =
    if scopePrefix.len > 0:
      targetPeerId & "|" & scopePrefix.pathKey()
    else:
      targetPeerId
  if targetPeerId.len > 0 and not book.forwardedContains(itemKey, forwardKey):
    book.markForwarded(itemKey, forwardKey)
    let queuedSubmit = submitProc
    proc handleFailure() {.gcsafe, raises: [].} =
      book.clearForwarded(itemKey, forwardKey)
      if not scopeBook.hasKey(itemKey) and itemKey notin pendingBook:
        return
      discard pendingBook.enqueuePending(itemKey)
      discard scopeBook.mergeScopePrefixes(itemKey, @[scopePrefix])
      node.scheduleStateMaintenance()
    node.enqueueOutbound(
      itemKey,
      targetPeerId,
      priority,
      queuedSubmit,
      nil,
      handleFailure,
    )

template queueScopedEventSubmit(
    node: FabricNode,
    forwardedBook: untyped,
    deliveryBook: untyped,
    pendingBook: untyped,
    scopeBook: untyped,
    itemKey: string,
    targetPeerId: string,
    scopePrefix: LsmrPath,
    event: FabricEvent,
) =
  let targetKey = deliveryTargetKey(targetPeerId, scopePrefix)
  let ackKey = submitAckKey(itemKey, scopePrefix)
  if targetPeerId.len > 0 and beginInflightDelivery(deliveryBook, itemKey, targetKey):
    node.noteLsmrDeliveryMutation()
    markForwarded(forwardedBook, itemKey, targetKey)
    proc handleSuccess() {.gcsafe, raises: [].} =
      clearForwarded(forwardedBook, itemKey, targetKey)
      node.noteLsmrDeliveryMutation()
      if acknowledgeDelivery(deliveryBook, itemKey, targetKey):
        pendingBook.excl(itemKey)
        clearPendingScopePrefixes(scopeBook, itemKey)
        node.scheduleStateMaintenance()
      node.activateDependentDeliveries(itemKey)
    proc handleFailure() {.gcsafe, raises: [].} =
      clearForwarded(forwardedBook, itemKey, targetKey)
      if not hasOutstandingDelivery(deliveryBook, itemKey) and itemKey notin pendingBook:
        return
      failInflightDelivery(deliveryBook, itemKey, targetKey)
      node.noteLsmrDeliveryMutation()
      if hasOutstandingDelivery(deliveryBook, itemKey):
        discard pendingBook.enqueuePending(itemKey)
      discard mergeScopePrefixes(scopeBook, itemKey, @[scopePrefix])
      node.scheduleStateMaintenance()
    node.enqueueEventOutbound(
      ackKey,
      targetPeerId,
      scopePrefix,
      event,
      handleSuccess,
      handleFailure,
    )

template queueScopedAttestationSubmit(
    node: FabricNode,
    forwardedBook: untyped,
    deliveryBook: untyped,
    pendingBook: untyped,
    scopeBook: untyped,
    itemKey: string,
    targetPeerId: string,
    scopePrefix: LsmrPath,
    attestation: EventAttestation,
) =
  let targetKey = deliveryTargetKey(targetPeerId, scopePrefix)
  let ackKey = submitAckKey(itemKey, scopePrefix)
  if targetPeerId.len > 0 and beginInflightDelivery(deliveryBook, itemKey, targetKey):
    node.noteLsmrDeliveryMutation()
    markForwarded(forwardedBook, itemKey, targetKey)
    proc handleSuccess() {.gcsafe, raises: [].} =
      clearForwarded(forwardedBook, itemKey, targetKey)
      node.noteLsmrDeliveryMutation()
      if acknowledgeDelivery(deliveryBook, itemKey, targetKey):
        node.completeAttestationDelivery(itemKey)
      else:
        discard pendingBook.enqueuePending(itemKey)
      node.scheduleStateMaintenance()
    proc handleFailure() {.gcsafe, raises: [].} =
      clearForwarded(forwardedBook, itemKey, targetKey)
      if not hasOutstandingDelivery(deliveryBook, itemKey) and itemKey notin pendingBook:
        return
      failInflightDelivery(deliveryBook, itemKey, targetKey)
      node.noteLsmrDeliveryMutation()
      if hasOutstandingDelivery(deliveryBook, itemKey):
        discard pendingBook.enqueuePending(itemKey)
      discard mergeScopePrefixes(scopeBook, itemKey, @[scopePrefix])
      node.scheduleStateMaintenance()
    node.enqueueAttestationOutbound(
      ackKey,
      targetPeerId,
      scopePrefix,
      attestation,
      handleSuccess,
      handleFailure,
    )

template queueScopedEventCertificateSubmit(
    node: FabricNode,
    forwardedBook: untyped,
    deliveryBook: untyped,
    pendingBook: untyped,
    scopeBook: untyped,
    itemKey: string,
    targetPeerId: string,
    scopePrefix: LsmrPath,
    eventCertificate: EventCertificate,
) =
  let targetKey = deliveryTargetKey(targetPeerId, scopePrefix)
  let ackKey = submitAckKey(itemKey, scopePrefix)
  if targetPeerId.len > 0 and beginInflightDelivery(deliveryBook, itemKey, targetKey):
    node.noteLsmrDeliveryMutation()
    markForwarded(forwardedBook, itemKey, targetKey)
    proc handleSuccess() {.gcsafe, raises: [].} =
      clearForwarded(forwardedBook, itemKey, targetKey)
      node.noteLsmrDeliveryMutation()
      if acknowledgeDelivery(deliveryBook, itemKey, targetKey):
        node.completeEventCertificateDelivery(itemKey)
      else:
        discard node.markPendingEventCertificate(itemKey)
      node.scheduleStateMaintenance()
    proc handleFailure() {.gcsafe, raises: [].} =
      clearForwarded(forwardedBook, itemKey, targetKey)
      if not hasOutstandingDelivery(deliveryBook, itemKey) and itemKey notin pendingBook:
        return
      failInflightDelivery(deliveryBook, itemKey, targetKey)
      node.noteLsmrDeliveryMutation()
      if hasOutstandingDelivery(deliveryBook, itemKey):
        discard node.markPendingEventCertificate(itemKey)
      discard mergeScopePrefixes(scopeBook, itemKey, @[scopePrefix])
      node.scheduleStateMaintenance()
    node.enqueueEventCertificateOutbound(
      ackKey,
      targetPeerId,
      scopePrefix,
      eventCertificate,
      handleSuccess,
      handleFailure,
    )

template queueGlobalAckedSubmit(
    node: FabricNode,
    forwardedBook: untyped,
    deliveryBook: untyped,
    pendingBook: untyped,
    itemKey: string,
    targetPeerId: string,
    priority: OutboundSubmitPriority,
    submitProc: untyped,
) =
  let targetKey = deliveryTargetKey(targetPeerId)
  if targetPeerId.len > 0 and beginInflightDelivery(deliveryBook, itemKey, targetKey):
    markForwarded(forwardedBook, itemKey, targetKey)
    proc handleSuccess() {.gcsafe, raises: [].} =
      clearForwarded(forwardedBook, itemKey, targetKey)
      if acknowledgeDelivery(deliveryBook, itemKey, targetKey):
        pendingBook.excl(itemKey)
        clearDeliveryState(deliveryBook, itemKey)
        node.scheduleStateMaintenance()
    proc handleFailure() {.gcsafe, raises: [].} =
      clearForwarded(forwardedBook, itemKey, targetKey)
      if not hasOutstandingDelivery(deliveryBook, itemKey) and itemKey notin pendingBook:
        return
      failInflightDelivery(deliveryBook, itemKey, targetKey)
      if hasOutstandingDelivery(deliveryBook, itemKey):
        discard pendingBook.enqueuePending(itemKey)
        node.scheduleStateMaintenance()
    node.enqueueOutbound(
      itemKey,
      targetPeerId,
      priority,
      submitProc,
      handleSuccess,
      handleFailure,
    )

proc makeEventRelaySubmit(
    node: FabricNode, targetPeerId: string, event: FabricEvent, relayScope: LsmrPath
): proc(): Future[bool] {.closure, gcsafe, raises: [].} =
  let fixedTargetPeerId = targetPeerId
  let fixedEvent = event
  let fixedRelayScope = relayScope
  result = proc(): Future[bool] {.closure, gcsafe, raises: [].} =
    node.network.submitEvent(
      fixedTargetPeerId,
      fixedEvent,
      scopePrefix = fixedRelayScope,
    )

proc makeAttestationRelaySubmit(
    node: FabricNode, targetPeerId: string, att: EventAttestation, relayScope: LsmrPath
): proc(): Future[bool] {.closure, gcsafe, raises: [].} =
  let fixedTargetPeerId = targetPeerId
  let fixedAtt = att
  let fixedRelayScope = relayScope
  result = proc(): Future[bool] {.closure, gcsafe, raises: [].} =
    node.network.submitAttestation(
      fixedTargetPeerId,
      fixedAtt,
      scopePrefix = fixedRelayScope,
    )

proc makeEventCertificateRelaySubmit(
    node: FabricNode, targetPeerId: string, cert: EventCertificate, relayScope: LsmrPath
): proc(): Future[bool] {.closure, gcsafe, raises: [].} =
  let fixedTargetPeerId = targetPeerId
  let fixedCert = cert
  let fixedRelayScope = relayScope
  result = proc(): Future[bool] {.closure, gcsafe, raises: [].} =
    node.network.submitEventCertificate(
      fixedTargetPeerId,
      fixedCert,
      scopePrefix = fixedRelayScope,
    )

proc uniqueTargetPaths(paths: openArray[LsmrPath]): seq[LsmrPath] =
  var seen = initHashSet[string]()
  for path in paths:
    if path.len == 0:
      continue
    let key = path.pathKey()
    if key in seen:
      continue
    seen.incl(key)
    result.add(path)

proc relayTargetPaths(
    defaultTargets: openArray[LsmrPath], scopePrefixes: openArray[LsmrPath]
): seq[LsmrPath] =
  if scopePrefixes.len > 0:
    return uniqueTargetPaths(scopePrefixes)
  uniqueTargetPaths(defaultTargets)

proc routingPaths(routes: openArray[RoutingPeer]): seq[LsmrPath] =
  for route in routes:
    if route.path.len > 0:
      result.add(route.path)
  result = uniqueTargetPaths(result)

proc scopedRelayPlans(
    routes: openArray[RoutingPeer]
): seq[tuple[route: RoutingPeer, scope: LsmrPath]] =
  for route in routes:
    result.add((route: route, scope: @[]))

proc uniqueRelayRoutesByPeer(
    routes: openArray[tuple[route: RoutingPeer, scope: LsmrPath]]
): seq[tuple[route: RoutingPeer, scope: LsmrPath]] =
  var seenPeerIds = initHashSet[string]()
  for item in routes:
    if item.route.peerId.len == 0 or item.route.peerId in seenPeerIds:
      continue
    seenPeerIds.incl(item.route.peerId)
    result.add((route: item.route, scope: @[]))

proc eventCertificateRelayPlans(
    node: FabricNode, scopePrefixes: seq[LsmrPath] = @[]
): seq[RelayPlan] =
  if node.isNil:
    return @[]
  if node.usesLsmrPrimary():
    return node.lsmrRelayRoutes(
      relayTargetPaths(node.allRoutingPaths(), scopePrefixes),
      scopePrefixes,
    )
  @[]

proc eventTargetPaths(event: FabricEvent): seq[LsmrPath] =
  result = routingPaths(event.participantRoutes)
  for path in routingPaths(event.eventWitnessRoutes()):
    if path notin result:
      result.add(path)
  result = uniqueTargetPaths(result)

proc allRoutingPaths(node: FabricNode): seq[LsmrPath] =
  result = routingPaths(node.allRoutingPeers())

proc validatorPaths(node: FabricNode, stateAtEra: ChainState): seq[LsmrPath] =
  result = routingPaths(node.validatorPeers(stateAtEra))

proc latestLocalSenderParent(
    node: FabricNode, sender: string, nonce: uint64
): Option[string] {.gcsafe, raises: [].} =
  var bestNonce = 0'u64
  var bestEventId = ""
  for eventId, event in node.events.pairs:
    if eventId in node.isolatedEvents:
      continue
    if event.legacyTx.sender != sender:
      continue
    if event.legacyTx.nonce >= nonce:
      continue
    if bestEventId.len == 0 or event.legacyTx.nonce > bestNonce:
      bestNonce = event.legacyTx.nonce
      bestEventId = eventId
  if bestEventId.len == 0:
    return none(string)
  some(bestEventId)

proc latestLocalThreadParent(
    node: FabricNode, threadId: EventThreadId, sender: string, nonce: uint64
): Option[string] {.gcsafe, raises: [].} =
  var bestNonce = 0'u64
  var bestEventId = ""
  for eventId, event in node.events.pairs:
    if eventId in node.isolatedEvents:
      continue
    if event.threadId != threadId or event.legacyTx.sender != sender:
      continue
    if event.legacyTx.nonce >= nonce:
      continue
    if bestEventId.len == 0 or event.legacyTx.nonce > bestNonce:
      bestNonce = event.legacyTx.nonce
      bestEventId = eventId
  if bestEventId.len == 0:
    return none(string)
  some(bestEventId)

proc lsmrRelayRoutes(
    node: FabricNode, targetPaths: openArray[LsmrPath], scopePrefixes: openArray[LsmrPath] = []
): seq[tuple[route: RoutingPeer, scope: LsmrPath]] =
  if node.isNil or node.network.isNil or node.network.switch.isNil:
    return @[]
  let peerStore = node.network.switch.peerStore
  if peerStore.isNil:
    return @[]
  let localPath =
    if peerStore[ActiveLsmrBook].contains(node.identity.peerId):
      peerStore[ActiveLsmrBook][node.identity.peerId].data.certifiedPrefix()
    else:
      node.config.lsmrPath
  for targetPath in filterTargetPathsByScope(targetPaths, scopePrefixes):
    if targetPath.len == 0 or targetPath == localPath:
      continue
    let nextHop = peerStore.routeNextHop(node.identity.peerId, targetPath)
    if nextHop.isNone():
      when defined(fabric_submit_diag) or defined(fabric_lsmr_diag):
        echo "lsmr-route self=", node.identity.account,
          " target=", targetPath,
          " next=none"
      continue
    let route = node.routingPeerForPeerId($nextHop.get())
    when defined(fabric_submit_diag) or defined(fabric_lsmr_diag):
      echo "lsmr-route self=", node.identity.account,
        " target=", targetPath,
        " next=", $nextHop.get(),
        " routeKnown=", route.isSome(),
        " routeSelf=", (if route.isSome(): node.routeMatchesSelf(route.get()) else: false)
    if route.isSome() and not node.routeMatchesSelf(route.get()):
      let peerRoute = route.get()
      let routeKey =
        if peerRoute.peerId.len > 0:
          peerRoute.peerId
        else:
          peerRoute.account
      let scopeKey = targetPath.pathKey()
      if result.allIt(
          (((if it.route.peerId.len > 0: it.route.peerId else: it.route.account) != routeKey) or
          it.scope.pathKey() != scopeKey)
      ):
        result.add((route: peerRoute, scope: targetPath))

proc eventHasRunnableLsmrDelivery(
    node: FabricNode, event: FabricEvent, scopePrefixes: seq[LsmrPath]
): bool =
  if node.isNil:
    return false
  if not node.usesLsmrPrimary():
    return true
  let routes = node.lsmrRelayRoutes(relayTargetPaths(node.allRoutingPaths(), scopePrefixes))
  var keepTargets = initHashSet[string]()
  for route in routes:
    if route.route.account == node.identity.account or route.route.peerId.len == 0:
      continue
    keepTargets.incl(deliveryTargetKey(route.route.peerId, route.scope))
  let pruned = pruneObsoleteDeliveryTargets(node.eventDeliveries, event.eventId, keepTargets)
  if pruned > 0:
    node.noteLsmrDeliveryMutation()
  when defined(fabric_submit_diag):
    if pruned > 0:
      echo "delivery-prune account=", node.identity.account,
        " kind=event item=", event.eventId,
        " pruned=", pruned,
        " keep=", keepTargets.len
  for route in routes:
    if route.route.account == node.identity.account or route.route.peerId.len == 0:
      continue
    let targetKey = deliveryTargetKey(route.route.peerId, route.scope)
    if not deliveryNeedsAttempt(node.eventDeliveries, event.eventId, targetKey):
      continue
    if node.lsmrSubmitRouteReady(route.route.peerId, ospEvent):
      return true
  false

proc eventDependentsMayRelay(node: FabricNode, event: FabricEvent): bool =
  if node.isNil:
    return false
  if not node.usesLsmrPrimary():
    return true
  if event.eventId in node.eventCertificates:
    return true
  let routes = node.lsmrRelayRoutes(
    relayTargetPaths(
      node.allRoutingPaths(),
      node.pendingEventScopes.pendingScopePrefixes(event.eventId),
    )
  )
  var hasDownstream = false
  for route in routes:
    if route.route.account == node.identity.account or route.route.peerId.len == 0:
      continue
    hasDownstream = true
    break
  if not hasDownstream:
    return true
  if event.eventId notin node.eventDeliveries:
    return false
  let ledger = node.eventDeliveries.getOrDefault(event.eventId, initDeliveryLedger())
  ledger.inflightTargets.len > 0 or ledger.deliveredTargets.len > 0

proc attestationRelayRoutes(
    node: FabricNode, event: FabricEvent, att: EventAttestation
): seq[tuple[route: RoutingPeer, scope: LsmrPath]] =
  if node.isNil:
    return @[]
  if not node.usesLsmrPrimary():
    return scopedRelayPlans(node.routePeers(event))
  case att.role
  of arParticipant, arWitness:
    let authorRoute = node.eventAuthorRoute(event)
    if authorRoute.isSome() and not node.routeMatchesSelf(authorRoute.get()):
      result.add((route: authorRoute.get(), scope: newSeq[uint8]()))
  of arIsolation:
    let stateAtEra = node.replayState(event.clock.era)
    result = uniqueRelayRoutesByPeer(node.lsmrRelayRoutes(node.validatorPaths(stateAtEra)))

proc attestationRequiresRelay(
    node: FabricNode, event: FabricEvent, att: EventAttestation
): bool {.gcsafe, raises: [].} =
  if node.isNil or not node.usesLsmrPrimary():
    return true
  case att.role
  of arParticipant, arWitness:
    event.author != node.identity.account
  of arIsolation:
    true

proc attestationNeedsRelay(
    node: FabricNode, event: FabricEvent, att: EventAttestation
): bool {.gcsafe, raises: [].} =
  if node.isNil or not node.usesLsmrPrimary():
    return true
  case att.role
  of arParticipant, arWitness:
    let authorRoute = node.eventAuthorRoute(event)
    authorRoute.isSome() and not node.routeMatchesSelf(authorRoute.get())
  of arIsolation:
    true

proc attestationHasRunnableLsmrDelivery(
    node: FabricNode, event: FabricEvent, att: EventAttestation
): bool =
  if node.isNil:
    return false
  if not node.usesLsmrPrimary():
    return true
  let routes = node.attestationRelayRoutes(event, att)
  var keepTargets = initHashSet[string]()
  for route in routes:
    if route.route.account == node.identity.account or route.route.peerId.len == 0:
      continue
    keepTargets.incl(deliveryTargetKey(route.route.peerId))
  let pruned = pruneObsoleteDeliveryTargets(node.attestationDeliveries, att.attestationKey(), keepTargets)
  if pruned > 0:
    node.noteLsmrDeliveryMutation()
  when defined(fabric_submit_diag):
    if pruned > 0:
      echo "delivery-prune account=", node.identity.account,
        " kind=attestation item=", att.attestationKey(),
        " pruned=", pruned,
        " keep=", keepTargets.len
  for route in routes:
    if route.route.account == node.identity.account or route.route.peerId.len == 0:
      continue
    let targetPeerId = route.route.peerId
    let targetKey = deliveryTargetKey(targetPeerId)
    if not deliveryNeedsAttempt(node.attestationDeliveries, att.attestationKey(), targetKey):
      continue
    if node.lsmrSubmitRouteReady(targetPeerId, ospAttestation):
      return true
  false

proc eventCertificateHasRunnableLsmrDelivery(
    node: FabricNode, event: FabricEvent, cert: EventCertificate
): bool =
  let relayPlans =
    node.eventCertificateRelayPlans(
      node.pendingEventCertificateScopes.pendingScopePrefixes(cert.eventId)
    )
  node.eventCertificateHasRunnableLsmrDelivery(event, cert, relayPlans)

proc eventCertificateHasRunnableLsmrDelivery(
    node: FabricNode,
    event: FabricEvent,
    cert: EventCertificate,
    relayPlans: openArray[RelayPlan],
): bool =
  if node.isNil:
    return false
  if not node.usesLsmrPrimary():
    return true
  if not node.eventDependentsMayRelay(event):
    return false
  let requireEventDeliveryGate = node.eventDeliveries.hasOutstandingDelivery(event.eventId)
  var keepTargets = initHashSet[string]()
  for route in relayPlans:
    if route.route.account == node.identity.account or route.route.peerId.len == 0:
      continue
    let targetPeerId = route.route.peerId
    if requireEventDeliveryGate and
        not deliveredToPeer(node.eventDeliveries, event.eventId, targetPeerId):
      continue
    keepTargets.incl(deliveryTargetKey(targetPeerId, route.scope))
  let pruned = pruneObsoleteDeliveryTargets(node.eventCertificateDeliveries, cert.eventId, keepTargets)
  if pruned > 0:
    node.noteLsmrDeliveryMutation()
  when defined(fabric_submit_diag):
    if pruned > 0:
      echo "delivery-prune account=", node.identity.account,
        " kind=event-certificate item=", cert.eventId,
        " pruned=", pruned,
        " keep=", keepTargets.len
  for route in relayPlans:
    if route.route.account == node.identity.account or route.route.peerId.len == 0:
      continue
    let targetPeerId = route.route.peerId
    if requireEventDeliveryGate and
        not deliveredToPeer(node.eventDeliveries, event.eventId, targetPeerId):
      continue
    let targetKey = deliveryTargetKey(targetPeerId, route.scope)
    if not deliveryNeedsAttempt(node.eventCertificateDeliveries, cert.eventId, targetKey):
      continue
    if not node.eventCertificateSubmitCreditAvailable(targetPeerId):
      continue
    if node.lsmrSubmitRouteReady(targetPeerId, ospOther):
      return true
  false

proc latestCheckpointEra(node: FabricNode): uint64 {.gcsafe, raises: [].} =
  if node.isNil:
    return 0'u64
  if node.latestCheckpointId.len > 0 and node.latestCheckpointId in node.checkpoints:
    return node.checkpoints.getOrDefault(node.latestCheckpointId).candidate.era
  0'u64

proc checkpointSnapshotTxCount(snapshot: ChainStateSnapshot): uint64 {.gcsafe, raises: [].} =
  for item in snapshot.nonces:
    result += item.value

proc shouldPromoteLatestCheckpoint(
    node: FabricNode, checkpointId: string, snapshot: ChainStateSnapshot
): bool {.gcsafe, raises: [].} =
  if node.isNil or checkpointId.len == 0 or checkpointId notin node.checkpoints:
    return false
  if node.latestCheckpointId.len == 0 or node.latestCheckpointId notin node.checkpoints or
      node.latestCheckpointId notin node.checkpointSnapshots:
    return true
  let currentCert = node.checkpoints.getOrDefault(node.latestCheckpointId)
  let nextCert = node.checkpoints.getOrDefault(checkpointId)
  if currentCert.candidate.era != nextCert.candidate.era:
    return currentCert.candidate.era < nextCert.candidate.era
  let currentSnapshot = node.checkpointSnapshots.getOrDefault(node.latestCheckpointId)
  let currentTxCount = currentSnapshot.checkpointSnapshotTxCount()
  let nextTxCount = snapshot.checkpointSnapshotTxCount()
  if currentTxCount != nextTxCount:
    return currentTxCount < nextTxCount
  if currentSnapshot.contents.len != snapshot.contents.len:
    return currentSnapshot.contents.len < snapshot.contents.len
  if currentSnapshot.executionReceipts.len != snapshot.executionReceipts.len:
    return currentSnapshot.executionReceipts.len < snapshot.executionReceipts.len
  if currentSnapshot.creditPositions.len != snapshot.creditPositions.len:
    return currentSnapshot.creditPositions.len < snapshot.creditPositions.len
  if currentSnapshot.nameRecords.len != snapshot.nameRecords.len:
    return currentSnapshot.nameRecords.len < snapshot.nameRecords.len
  false

proc checkpointLiveRelayOutstanding(
    node: FabricNode
): tuple[events, attestations, certificates: int] {.gcsafe, raises: [].} =
  if node.isNil:
    return
  let checkpointEra = node.latestCheckpointEra()
  for eventId, ledger in node.eventDeliveries.pairs:
    if ledger.remainingTargets.len == 0 and ledger.inflightTargets.len == 0:
      continue
    if eventId notin node.events or eventId in node.isolatedEvents:
      continue
    let event = node.events.getOrDefault(eventId)
    if event.clock.era < checkpointEra or eventId in node.eventCertificates:
      continue
    inc result.events
  for attestationKey, ledger in node.attestationDeliveries.pairs:
    if ledger.remainingTargets.len == 0 and ledger.inflightTargets.len == 0:
      continue
    let att = node.attestationForKey(attestationKey)
    if att.isNone():
      continue
    let eventId = att.get().eventId
    if eventId notin node.events or eventId in node.isolatedEvents:
      continue
    let event = node.events.getOrDefault(eventId)
    if event.clock.era < checkpointEra or eventId in node.eventCertificates:
      continue
    inc result.attestations
  for eventId, ledger in node.eventCertificateDeliveries.pairs:
    if ledger.remainingTargets.len == 0 and ledger.inflightTargets.len == 0:
      continue
    if eventId notin node.events or eventId notin node.eventCertificates or
        eventId in node.isolatedEvents:
      continue
    let event = node.events.getOrDefault(eventId)
    if event.clock.era < checkpointEra:
      continue
    inc result.certificates

proc checkpointRelevantEvent(
    node: FabricNode, eventId: string, checkpointEra: uint64
): bool {.gcsafe, raises: [].} =
  not node.isNil and eventId.len > 0 and eventId in node.events and
    eventId notin node.isolatedEvents and
    node.events.getOrDefault(eventId).clock.era >= checkpointEra

proc checkpointRelevantCertificationEvent(
    node: FabricNode, eventId: string, checkpointEra: uint64
): bool {.gcsafe, raises: [].} =
  if node.isNil or eventId.len == 0 or not node.checkpointRelevantEvent(eventId, checkpointEra):
    return false
  if not node.certificationEventTracked(eventId):
    return false
  node.refreshEventCertificationProgressState(eventId)
  if eventId notin node.eventCertificationProgress:
    return false
  let progress = node.eventCertificationProgress.getOrDefault(eventId)
  progress.status != agsCertified and progress.status != agsRetired

proc checkpointRelevantAttestationDelivery(
    node: FabricNode, key: string, checkpointEra: uint64
): bool {.gcsafe, raises: [].} =
  if node.isNil or key.len == 0:
    return false
  let att = node.attestationForKey(key)
  if att.isNone():
    return false
  let eventId = att.get().eventId
  node.checkpointRelevantEvent(eventId, checkpointEra) and eventId notin node.eventCertificates

proc checkpointRelevantEventCertificateDelivery(
    node: FabricNode, eventId: string, checkpointEra: uint64
): bool {.gcsafe, raises: [].} =
  node.checkpointRelevantEvent(eventId, checkpointEra) and eventId in node.eventCertificates

proc checkpointRelevantWitnessPull(
    node: FabricNode, item: AntiEntropyPullWorkItem, checkpointEra: uint64
): bool {.gcsafe, raises: [].} =
  if node.isNil or item.key.len == 0:
    return false
  case item.domain
  of aedAttParticipant, aedAttWitness:
    node.checkpointRelevantCertificationEvent(item.causedByEventId, checkpointEra) and
      node.witnessPullStillNeeded(item)
  of aedEventCert:
    let eventId =
      if item.causedByEventId.len > 0:
        item.causedByEventId
      else:
        antiEntropyKeySuffix(item.key, "eventcert:")
    eventId.len > 0 and node.checkpointRelevantEvent(eventId, checkpointEra) and
      eventId notin node.eventCertificates and node.witnessPullStillNeeded(item)
  else:
    false

proc antiEntropyMissingEventNeeded(node: FabricNode): bool {.gcsafe, raises: [].} =
  if node.isNil:
    return false
  let blocking = node.checkpointFrontierBlockingEra()
  blocking.hasBlockingEra and blocking.firstMissingEventId.len > 0 and
    blocking.firstMissingEventId notin node.events

proc antiEntropyCheckpointCandidateNeeded(node: FabricNode): bool {.gcsafe, raises: [].} =
  if node.isNil:
    return false
  node.refreshEraCheckpointStates()
  let checkpointEra = node.latestCheckpointEra()
  for era in node.eraCheckpointStates.keys.toSeq().sorted(system.cmp[uint64]):
    if era < checkpointEra:
      continue
    let state = node.checkpointEraState(era)
    if state.eventTotal == 0 or state.firstMissingEventId.len > 0:
      continue
    if state.candidateId.len == 0:
      return true
  false

proc antiEntropyCheckpointVoteNeeded(node: FabricNode): bool {.gcsafe, raises: [].} =
  if node.isNil:
    return false
  node.refreshEraCheckpointStates()
  let checkpointEra = node.latestCheckpointEra()
  for era in node.eraCheckpointStates.keys.toSeq().sorted(system.cmp[uint64]):
    if era < checkpointEra:
      continue
    let state = node.checkpointEraState(era)
    if state.eventTotal == 0 or state.firstMissingEventId.len > 0:
      continue
    if state.candidateId.len == 0 or node.hasCheckpointForCandidate(state.candidateId):
      continue
    if state.votePhase != evpQuorum:
      return true
  false

proc antiEntropyCheckpointBundleNeeded(node: FabricNode): bool {.gcsafe, raises: [].} =
  if node.isNil:
    return false
  node.refreshEraCheckpointStates()
  let checkpointEra = node.latestCheckpointEra()
  for era in node.eraCheckpointStates.keys.toSeq().sorted(system.cmp[uint64]):
    if era < checkpointEra:
      continue
    let state = node.checkpointEraState(era)
    if state.eventTotal == 0 or state.firstMissingEventId.len > 0:
      continue
    if state.candidateId.len == 0 or node.hasCheckpointForCandidate(state.candidateId):
      continue
    if state.votePhase == evpQuorum and state.bundlePhase != ebpCompleted:
      return true
  false

proc checkpointOwnerOutstanding(
    node: FabricNode
): tuple[
    pendingEvents, pendingAttestations, pendingEventCerts: int,
    pendingCertificationEvents, pendingWitnessPulls: int,
    liveEvents, liveAttestations, liveCertificates: int,
  ] {.gcsafe, raises: [].} =
  if node.isNil:
    return
  let checkpointEra = node.latestCheckpointEra()
  for eventId in node.pendingEvents:
    if node.checkpointRelevantEvent(eventId, checkpointEra) and
        eventId notin node.eventCertificates:
      inc result.pendingEvents
  for key in node.pendingAttestations:
    if node.checkpointRelevantAttestationDelivery(key, checkpointEra):
      inc result.pendingAttestations
  for key in node.routeBlockedAttestations:
    if node.checkpointRelevantAttestationDelivery(key, checkpointEra):
      inc result.pendingAttestations
  for eventId in node.pendingEventCertificates:
    if node.checkpointRelevantEventCertificateDelivery(eventId, checkpointEra):
      inc result.pendingEventCerts
  for eventId in node.pendingCertificationEvents:
    if node.checkpointRelevantCertificationEvent(eventId, checkpointEra):
      inc result.pendingCertificationEvents
  for queue in node.pendingWitnessPullsByPeer.values:
    for item in queue:
      if node.checkpointRelevantWitnessPull(item, checkpointEra):
        inc result.pendingWitnessPulls
  for item in node.inflightWitnessPullByPeer.values:
    if node.checkpointRelevantWitnessPull(item, checkpointEra):
      inc result.pendingWitnessPulls
  let liveOutstanding = node.checkpointLiveRelayOutstanding()
  result.liveEvents = liveOutstanding.events
  result.liveAttestations = liveOutstanding.attestations
  result.liveCertificates = liveOutstanding.certificates

proc checkpointFrontierBlockingEra(
    node: FabricNode
): tuple[hasBlockingEra: bool, era: uint64, firstMissingEventId: string] {.
    gcsafe, raises: [].} =
  if node.isNil:
    return
  node.refreshEraCheckpointStates()
  let checkpointEra = node.latestCheckpointEra()
  for era in node.eraCheckpointStates.keys.toSeq().sorted(system.cmp[uint64]):
    if era < checkpointEra:
      continue
    let state = node.checkpointEraState(era)
    if state.eventTotal == 0 or state.firstMissingEventId.len == 0:
      continue
    return (true, era, state.firstMissingEventId)

proc checkpointOwnerFrontierClear(
    node: FabricNode
): bool {.gcsafe, raises: [].} =
  if node.isNil:
    return false
  node.refreshPendingCertificationEvents()
  node.prunePendingCertificationState()
  if node.checkpointFrontierBlockingEra().hasBlockingEra:
    return false
  let outstanding = node.checkpointOwnerOutstanding()
  outstanding.pendingEvents == 0 and
    outstanding.pendingAttestations == 0 and
    outstanding.pendingEventCerts == 0 and
    outstanding.pendingCertificationEvents == 0 and
    outstanding.pendingWitnessPulls == 0 and
    outstanding.liveEvents == 0 and
    outstanding.liveAttestations == 0 and
    outstanding.liveCertificates == 0

proc checkpointAdvanceReady(node: FabricNode): bool {.gcsafe, raises: [].} =
  not node.isNil and node.checkpointOwnerFrontierClear()

proc disseminateEvent(
    node: FabricNode,
    event: FabricEvent,
    scopePrefixes: seq[LsmrPath] = @[],
    excludePeerIds: seq[string] = @[],
) =
  let component = node.componentNodes()
  let scope = node.routeScope(event, component)
  if not node.usesLsmrPrimary():
    for peer in scope:
      if peer.identity.account != node.identity.account:
        discard peer.registerEvent(event)
  if not node.network.isNil and node.running:
    if not node.usesLsmrPrimary():
      node.publishFuture(node.network.publishEvent(event))
    let routes =
      if node.usesLsmrPrimary():
        node.lsmrRelayRoutes(relayTargetPaths(node.allRoutingPaths(), scopePrefixes))
      else:
        scopedRelayPlans(node.routePeers(event))
    when defined(fabric_lsmr_diag):
      echo "event-route account=", node.identity.account,
        " event=", event.eventId,
        " routes=", routes.mapIt(it.route.peerId & "@" & $it.scope).join(",")
    for route in routes:
      if route.route.account != node.identity.account:
        if node.usesLsmrPrimary():
          let targetPeerId = route.route.peerId
          if targetPeerId.len == 0 or targetPeerId in excludePeerIds:
            continue
          let relayScope = route.scope
          let targetKey = deliveryTargetKey(targetPeerId, relayScope)
          if registerDeliveryTarget(node.eventDeliveries, event.eventId, targetKey):
            node.noteLsmrDeliveryMutation()
          block:
            let submitTargetPeerId = targetPeerId
            let submitRelayScope = relayScope
            let submitEvent = event
            node.queueScopedEventSubmit(
              node.forwardedEvents,
              node.eventDeliveries,
              node.pendingEvents,
              node.pendingEventScopes,
              submitEvent.eventId,
              submitTargetPeerId,
              submitRelayScope,
              submitEvent,
            )
        else:
          node.submitFuture(node.network.submitEvent(route.route.peerId, event))
    if node.usesLsmrPrimary():
      node.pendingEvents.excl(event.eventId)

proc disseminateAttestation(
    node: FabricNode,
    event: FabricEvent,
    att: EventAttestation,
    scopePrefixes: seq[LsmrPath] = @[],
) =
  discard scopePrefixes
  let component = node.componentNodes()
  let scope = node.routeScope(event, component)
  if not node.usesLsmrPrimary():
    discard shareAttestation(scope, event, att)
  if not node.network.isNil and node.running:
    if not node.usesLsmrPrimary():
      node.publishFuture(node.network.publishAttestation(att))
    let routes =
      if node.usesLsmrPrimary():
        node.attestationRelayRoutes(event, att)
      else:
        scopedRelayPlans(node.routePeers(event))
    when defined(fabric_lsmr_diag):
      echo "att-route account=", node.identity.account,
        " event=", att.eventId,
        " signer=", att.signer,
        " role=", ord(att.role),
        " routes=", routes.mapIt(it.route.peerId & "@" & $it.scope).join(",")
    for route in routes:
      if route.route.account != node.identity.account:
        if node.usesLsmrPrimary():
          let targetPeerId = route.route.peerId
          if targetPeerId.len == 0:
            continue
          let targetKey = deliveryTargetKey(targetPeerId)
          if registerDeliveryTarget(node.attestationDeliveries, att.attestationKey(), targetKey):
            node.noteLsmrDeliveryMutation()
          block:
            let submitTargetPeerId = targetPeerId
            let submitAtt = att
            node.queueScopedAttestationSubmit(
              node.forwardedAttestations,
              node.attestationDeliveries,
              node.pendingAttestations,
              node.pendingAttestationScopes,
              submitAtt.attestationKey(),
              submitTargetPeerId,
              newSeq[uint8](),
              submitAtt,
            )
        else:
          node.submitFuture(node.network.submitAttestation(route.route.peerId, att))
    if node.usesLsmrPrimary() and
        not node.attestationDeliveries.hasOutstandingDelivery(att.attestationKey()):
      node.pendingAttestations.excl(att.attestationKey())

proc disseminateEventCertificateWithRelayPlans(
    node: FabricNode,
    event: FabricEvent,
    cert: EventCertificate,
    relayPlans: openArray[RelayPlan],
    scopePrefixes: seq[LsmrPath] = @[],
) =
  discard scopePrefixes
  let component = node.componentNodes()
  let scope = node.routeScope(event, component)
  if not node.usesLsmrPrimary():
    for peer in scope:
      if peer.identity.account != node.identity.account:
        discard peer.registerEventCertificate(cert)
  if not node.network.isNil and node.running:
    if not node.usesLsmrPrimary():
      node.publishFuture(node.network.publishEventCertificate(cert))
    let routes =
      if node.usesLsmrPrimary(): @relayPlans
      else: scopedRelayPlans(node.routePeers(event))
    when defined(fabric_lsmr_diag):
      echo "cert-route account=", node.identity.account,
        " event=", cert.eventId,
        " routes=", routes.mapIt(it.route.peerId & "@" & $it.scope).join(",")
    let requireEventDeliveryGate = node.eventDeliveries.hasOutstandingDelivery(cert.eventId)
    var deferred = false
    for route in routes:
      if route.route.account != node.identity.account:
        let targetPeerId = route.route.peerId
        let routeScope = route.scope
        if node.usesLsmrPrimary():
          if targetPeerId.len == 0:
            continue
          if requireEventDeliveryGate and
              not deliveredToPeer(node.eventDeliveries, cert.eventId, targetPeerId):
            deferred = true
            continue
          node.queueScopedEventCertificateSubmit(
            node.forwardedEventCertificates,
            node.eventCertificateDeliveries,
            node.pendingEventCertificates,
            node.pendingEventCertificateScopes,
            cert.eventId,
            targetPeerId,
            routeScope,
            cert,
          )
        else:
          node.submitFuture(node.network.submitEventCertificate(targetPeerId, cert))
    if node.usesLsmrPrimary():
      if not deferred and not node.eventCertificateDeliveries.hasOutstandingDelivery(cert.eventId):
        node.completeEventCertificateDelivery(cert.eventId)

proc disseminateEventCertificate(
    node: FabricNode,
    event: FabricEvent,
    cert: EventCertificate,
    scopePrefixes: seq[LsmrPath] = @[],
) =
  var certScopes = node.pendingEventCertificateScopes.pendingScopePrefixes(cert.eventId)
  for scopePrefix in scopePrefixes:
    certScopes.appendUniquePath(scopePrefix)
  let relayPlans =
    if node.usesLsmrPrimary():
      node.eventCertificateRelayPlans(certScopes)
    else:
      @[]
  node.disseminateEventCertificateWithRelayPlans(
    event,
    cert,
    relayPlans,
    scopePrefixes,
  )

proc disseminateCheckpointCandidate(node: FabricNode, candidate: CheckpointCandidate) =
  discard shareCandidate(node.componentNodes(), candidate)
  if not node.network.isNil and node.running:
    if not node.usesLsmrPrimary():
      node.publishFuture(node.network.publishCheckpointCandidate(candidate))
    let stateAtEra = node.replayState(candidate.era)
    let routes =
      if node.usesLsmrPrimary():
        node.lsmrRelayRoutes(node.validatorPaths(stateAtEra))
      else:
        scopedRelayPlans(node.validatorPeers(stateAtEra))
    for route in routes:
      if route.route.account != node.identity.account:
        if node.usesLsmrPrimary():
          let targetPeerId = route.route.peerId
          let targetKey = deliveryTargetKey(targetPeerId)
          discard registerDeliveryTarget(
            node.checkpointCandidateDeliveries,
            candidate.candidateId,
            targetKey,
          )
          block:
            let submitTargetPeerId = targetPeerId
            let submitCandidate = candidate
            node.queueGlobalAckedSubmit(
              node.forwardedCheckpointCandidates,
              node.checkpointCandidateDeliveries,
              node.pendingCheckpointCandidates,
              submitCandidate.candidateId,
              submitTargetPeerId,
              ospOther,
              makeCheckpointCandidateSubmitProc(
                node.network,
                submitTargetPeerId,
                submitCandidate,
              ),
            )
        else:
          node.submitFuture(node.network.submitCheckpointCandidate(route.route.peerId, candidate))
    if node.usesLsmrPrimary() and
        not node.checkpointCandidateDeliveries.hasOutstandingDelivery(candidate.candidateId):
      node.completeCheckpointCandidateDelivery(candidate.candidateId)

proc disseminateCheckpointVote(node: FabricNode, vote: CheckpointVote) =
  discard shareCheckpointVote(node.componentNodes(), vote)
  if not node.network.isNil and node.running:
    if not node.usesLsmrPrimary():
      node.publishFuture(node.network.publishCheckpointVote(vote))
    let stateAtEra = node.replayState(node.checkpointCandidates[vote.candidateId].era)
    let routes =
      if node.usesLsmrPrimary():
        node.lsmrRelayRoutes(node.validatorPaths(stateAtEra))
      else:
        scopedRelayPlans(node.validatorPeers(stateAtEra))
    for route in routes:
      if route.route.account != node.identity.account:
        if node.usesLsmrPrimary():
          let targetPeerId = route.route.peerId
          let voteKey = vote.checkpointVoteKey()
          let targetKey = deliveryTargetKey(targetPeerId)
          discard registerDeliveryTarget(
            node.checkpointVoteDeliveries,
            voteKey,
            targetKey,
          )
          block:
            let submitTargetPeerId = targetPeerId
            let submitVote = vote
            node.queueGlobalAckedSubmit(
              node.forwardedCheckpointVotes,
              node.checkpointVoteDeliveries,
              node.pendingCheckpointVotes,
              voteKey,
              submitTargetPeerId,
              ospOther,
              makeCheckpointVoteSubmitProc(
                node.network,
                submitTargetPeerId,
                submitVote,
              ),
            )
        else:
          node.submitFuture(node.network.submitCheckpointVote(route.route.peerId, vote))
    if node.usesLsmrPrimary() and
        not node.checkpointVoteDeliveries.hasOutstandingDelivery(vote.checkpointVoteKey()):
      node.completeCheckpointVoteDelivery(vote.checkpointVoteKey())

proc disseminateCheckpoint(node: FabricNode, cert: CheckpointCertificate, snapshot: ChainStateSnapshot) =
  let bundle = node.cachedCheckpointBundle(cert, snapshot)
  discard shareCheckpoint(node.componentNodes(), cert, snapshot, bundle.snapshot)
  if not node.network.isNil and node.running:
    if not node.usesLsmrPrimary():
      node.publishFuture(node.network.publishCheckpointCertificate(cert, snapshot))
    let routes =
      if node.usesLsmrPrimary():
        node.lsmrRelayRoutes(node.allRoutingPaths())
      else:
        scopedRelayPlans(node.allRoutingPeers())
    for route in routes:
      if route.route.account != node.identity.account:
        if node.usesLsmrPrimary():
          let targetPeerId = route.route.peerId
          let targetKey = deliveryTargetKey(targetPeerId)
          discard registerDeliveryTarget(
            node.checkpointBundleDeliveries,
            cert.checkpointId,
            targetKey,
          )
          block:
            let submitTargetPeerId = targetPeerId
            let submitBundle = bundle
            let submitCheckpointId = cert.checkpointId
            node.queueGlobalAckedSubmit(
              node.forwardedCheckpointBundles,
              node.checkpointBundleDeliveries,
              node.pendingCheckpointBundles,
              submitCheckpointId,
              submitTargetPeerId,
              ospOther,
              makeCheckpointBundleSubmitProc(
                node.network,
                submitTargetPeerId,
                submitBundle,
              ),
            )
        else:
          node.submitFuture(node.network.submitCheckpointBundle(route.route.peerId, bundle))
    if node.usesLsmrPrimary() and
        not node.checkpointBundleDeliveries.hasOutstandingDelivery(cert.checkpointId):
      node.completeCheckpointBundleDelivery(cert.checkpointId)

proc disseminateAvoProposal(node: FabricNode, proposal: AvoProposal) =
  discard shareAvoProposal(node.componentNodes(), proposal)
  if not node.network.isNil and node.running:
    if not node.usesLsmrPrimary():
      node.publishFuture(node.network.publishAvoProposal(proposal))
    else:
      let routes = node.lsmrRelayRoutes(node.validatorPaths(node.liveState))
      for route in routes:
        if route.route.account != node.identity.account:
          let targetPeerId = route.route.peerId
          block:
            let submitTargetPeerId = targetPeerId
            let submitProposal = proposal
            node.submitTracked(
              forwardedAvoProposals,
              pendingAvoProposals,
              submitProposal.proposalId,
              submitTargetPeerId,
              makeAvoProposalSubmitProc(
                node.network,
                submitTargetPeerId,
                submitProposal,
              ),
            )
      if routes.len > 0:
        node.pendingAvoProposals.excl(proposal.proposalId)

proc disseminateAvoApproval(node: FabricNode, proposalId, validator: string) =
  discard shareAvoApproval(node.componentNodes(), proposalId, validator)
  if not node.network.isNil and node.running:
    if not node.usesLsmrPrimary():
      node.publishFuture(node.network.publishAvoApproval(proposalId, validator))
    else:
      let routes = node.lsmrRelayRoutes(node.validatorPaths(node.liveState))
      for route in routes:
        if route.route.account != node.identity.account:
          let targetPeerId = route.route.peerId
          block:
            let submitTargetPeerId = targetPeerId
            let submitProposalId = proposalId
            let submitValidator = validator
            node.submitTracked(
              forwardedAvoApprovals,
              pendingAvoApprovals,
              submitProposalId & ":" & submitValidator,
              submitTargetPeerId,
              makeAvoApprovalSubmitProc(
                node.network,
                submitTargetPeerId,
                submitProposalId,
                submitValidator,
              ),
            )
      if routes.len > 0:
        node.pendingAvoApprovals.excl(avoApprovalKey(proposalId, validator))

proc maybeCreateCheckpoint(node: FabricNode, candidate: CheckpointCandidate): Option[(CheckpointCertificate, ChainStateSnapshot)] =
  if candidate.candidateId notin node.checkpointVotes:
    return none((CheckpointCertificate, ChainStateSnapshot))
  let stateAtEra = node.replayState(candidate.era)
  var totalWeight = 0'u64
  for vote in node.checkpointVotes[candidate.candidateId]:
    totalWeight += vote.weight
  if totalWeight < stateAtEra.quorumWeight():
    when defined(fabric_checkpoint_diag):
      echo "checkpoint-wait ", node.identity.account,
        " era=", candidate.era,
        " candidate=", candidate.candidateId,
        " total=", totalWeight,
        " quorum=", stateAtEra.quorumWeight()
    return none((CheckpointCertificate, ChainStateSnapshot))
  let snapshot = stateAtEra.snapshot()
  var cert = CheckpointCertificate(
    checkpointId: "",
    candidate: candidate,
    votes: node.checkpointVotes[candidate.candidateId],
    quorumWeight: totalWeight,
    createdAt: candidate.createdAt,
  )
  cert.checkpointId = computeCheckpointId(cert)
  some((cert, snapshot))

proc advanceCheckpointState(node: FabricNode, component: seq[FabricNode]): bool =
  if node.isNil:
    return false
  let startedAtMs = diagNowMs()
  result = node.ensureEraCandidates(component) or result
  for candidate in node.pendingCheckpointAdvanceCandidates():
    result = node.advanceCheckpointCandidate(candidate) or result
  let elapsedMs = diagNowMs() - startedAtMs
  if elapsedMs >= FabricSlowCheckpointAdvanceMs:
    echo "checkpoint-advance slow account=", node.identity.account,
      " elapsedMs=", elapsedMs,
      " candidates=", node.checkpointCandidates.len,
      " checkpoints=", node.checkpoints.len,
      " certified=", node.eventCertificates.len

proc pendingCheckpointAdvanceCandidates(node: FabricNode): seq[CheckpointCandidate] =
  if node.isNil:
    return @[]
  for _, candidate in node.checkpointCandidates.pairs:
    if node.hasCheckpointForCandidate(candidate.candidateId):
      continue
    result.add(candidate)
  result.sort(proc(a, b: CheckpointCandidate): int =
    result = cmp(a.era, b.era)
    if result == 0:
      result = cmp(a.createdAt, b.createdAt)
    if result == 0:
      result = cmp(a.candidateId, b.candidateId)
  )

proc advanceCheckpointCandidate(
    node: FabricNode, candidate: CheckpointCandidate
): bool =
  if node.isNil or candidate.candidateId.len == 0 or
      node.hasCheckpointForCandidate(candidate.candidateId):
    return false
  let readiness = node.checkpointEraReadiness(candidate.era)
  if not readiness.ready:
    when defined(fabric_submit_diag):
      echo "checkpoint-candidate wait-local-ready account=", node.identity.account,
        " era=", candidate.era,
        " candidate=", candidate.candidateId,
        " eraEvents=", readiness.eraEvents,
        " certified=", readiness.certified,
        " isolated=", readiness.isolated,
        " firstMissing=", readiness.firstMissing
    return false
  let stateAtEra = node.replayState(candidate.era)
  if stateAtEra.validators.hasKey(node.identity.account) and
      stateAtEra.validators[node.identity.account].active and
      not stateAtEra.validators[node.identity.account].jailed and
      not node.voteBy(candidate.candidateId, node.identity.account):
    let vote = node.makeCheckpointVote(candidate, stateAtEra)
    result = node.registerCheckpointVote(vote) or result
  let checkpoint = node.maybeCreateCheckpoint(candidate)
  if checkpoint.isSome():
    result = node.registerCheckpoint(checkpoint.get()[0], checkpoint.get()[1]) or result

proc ensureEraCandidates(node: FabricNode, component: seq[FabricNode]): bool =
  var changed = false
  let certs = node.certifiedEvents()
  if certs.len == 0:
    return false
  let maxEra = certs[^1].clock.era
  for era in 0'u64 .. maxEra:
    let readiness = node.checkpointEraReadiness(era)
    if not readiness.ready:
      when defined(fabric_submit_diag):
        echo "checkpoint-candidate hold account=", node.identity.account,
          " era=", era,
          " maxEra=", maxEra,
          " eraEvents=", readiness.eraEvents,
          " certified=", readiness.certified,
          " isolated=", readiness.isolated,
          " hasCertified=", readiness.hasCertified,
          " firstMissing=", readiness.firstMissing,
          " tickRange=", readiness.minTick, "..", readiness.maxTick
      continue
    let candidate = node.buildCheckpointCandidate(era)
    if node.hasCheckpointForCandidate(candidate.candidateId):
      continue
    let existingCandidateIds = node.checkpointCandidateIdsForEra(era)
    if existingCandidateIds.len == 1 and existingCandidateIds[0] == candidate.candidateId:
      continue
    for existingCandidateId in existingCandidateIds:
      if existingCandidateId == candidate.candidateId:
        continue
      when defined(fabric_submit_diag):
        echo "checkpoint-candidate refresh account=", node.identity.account,
          " era=", era,
          " old=", existingCandidateId,
          " new=", candidate.candidateId
      node.checkpointCandidates.del(existingCandidateId)
      node.completeCheckpointCandidateDelivery(existingCandidateId)
      for vote in node.checkpointVotes.getOrDefault(existingCandidateId, @[]):
        node.completeCheckpointVoteDelivery(vote.checkpointVoteKey())
      node.checkpointVotes.del(existingCandidateId)
      node.clearCheckpointVotePending(existingCandidateId)
    when defined(fabric_checkpoint_diag):
      echo "checkpoint-build ", node.identity.account,
        " era=", era,
        " maxEra=", maxEra,
        " frontier=", candidate.frontierDigest
    discard component
    if candidate.candidateId notin node.checkpointCandidates:
      changed = node.registerCandidate(candidate) or changed
  changed

proc advanceNode(node: FabricNode, component: seq[FabricNode]): bool =
  var changed = false
  for _, event in node.events.pairs:
    let role =
      if node.identity.account in event.participants: some(arParticipant)
      elif node.nodeIsEventWitness(event): some(arWitness)
      else: none(AttestationRole)
    if role.isSome():
      let existing = node.localAttestation(event.eventId, node.identity.account, role.get())
      if existing.isNone():
        let att = node.makeAttestation(event, role.get())
        when defined(fabric_submit_diag):
          echo "advance-stage att-create account=", node.identity.account,
            " event=", event.eventId,
            " role=", ord(role.get())
        when defined(fabric_diag):
          echo "advance-att ", node.identity.account, " event=", event.eventId, " role=", ord(role.get())
        changed = node.registerAttestation(att) or changed
  for _, event in node.events.pairs:
    if node.usesLsmrPrimary():
      changed = node.refreshCertificationEvent(event.eventId) or changed
    else:
      let scope = node.routeScope(event, component)
      if node.maybeCertifyEvent(event, scope):
        when defined(fabric_submit_diag):
          echo "advance-stage certify account=", node.identity.account, " event=", event.eventId
        when defined(fabric_diag):
          echo "advance-cert ", node.identity.account, " event=", event.eventId
        changed = true
  changed = node.advanceCheckpointState(component) or changed
  changed

proc converge(node: FabricNode) =
  let component = node.componentNodes()
  var changed = true
  var loops = 0
  while changed:
    inc loops
    when defined(fabric_submit_diag):
      echo "converge-stage loop-start account=", node.identity.account, " loop=", loops
    when defined(fabric_diag):
      echo "converge-loop ", node.identity.account, " loop=", loops
    changed = false
    for peer in component:
      let peerChanged = peer.advanceNode(component)
      when defined(fabric_submit_diag):
        echo "converge-stage loop-peer account=", node.identity.account,
          " loop=", loops,
          " peer=", peer.identity.account,
          " changed=", peerChanged
      changed = peerChanged or changed
    when defined(fabric_submit_diag):
      echo "converge-stage loop-end account=", node.identity.account, " loop=", loops, " changed=", changed

proc advanceTouchedInbound(
    node: FabricNode,
    eventIds: openArray[string],
    touchedAttestationKeys: var seq[string],
) =
  for eventId in eventIds.sortedUnique():
    if eventId.len == 0 or eventId notin node.events or eventId in node.isolatedEvents:
      continue
    let event = node.events[eventId]
    node.advanceSingleEvent(event, touchedAttestationKeys)
    if node.usesLsmrPrimary():
      discard node.refreshCertificationEvent(eventId, force = true)
    else:
      discard node.maybeCertifyEvent(event, @[node])
    if eventId notin node.eventCertificates:
      {.cast(gcsafe).}:
        node.maybeActivateDependentDeliveries(eventId)

proc flushTouchedLsmr(
    node: FabricNode,
    relayEventIds: openArray[string],
    eventScopes: Table[string, seq[LsmrPath]],
    eventSourcePeers: Table[string, seq[string]],
    attestationKeys: openArray[string],
) =
  if node.isNil or not node.running or not node.usesLsmrPrimary():
    return
  for eventId in relayEventIds.sortedUnique():
    if eventId.len == 0 or eventId notin node.events or eventId in node.isolatedEvents:
      continue
    let event = node.events[eventId]
    let relayScopes = eventScopes.pendingScopePrefixes(eventId)
    when defined(fabric_submit_diag):
      echo "flush-stage event account=", node.identity.account,
        " event=", eventId,
        " scope=", $relayScopes
    node.disseminateEvent(
      event,
      relayScopes,
      eventSourcePeers.getOrDefault(eventId, @[]),
    )
  for key in attestationKeys.sortedUnique():
    let att = node.attestationForKey(key)
    if att.isNone() or att.get().eventId notin node.events:
      continue
    when defined(fabric_submit_diag):
      echo "flush-stage att account=", node.identity.account,
        " event=", att.get().eventId,
        " role=", ord(att.get().role),
        " scope=@[]"
    let event = node.events[att.get().eventId]
    if att.get().eventId in node.eventCertificates or not node.attestationNeedsRelay(event, att.get()):
      continue
    node.disseminateAttestation(event, att.get())

proc advanceLocalAcceptedEvent(
    node: FabricNode,
    eventId: string,
) =
  if node.isNil or eventId.len == 0 or eventId notin node.events or eventId in node.isolatedEvents:
    return
  var touchedAttestationKeys: seq[string] = @[]
  let touchedEvents = @[eventId]
  let touchedEventScopes = initTable[string, seq[LsmrPath]]()
  let touchedEventSourcePeers = initTable[string, seq[string]]()
  when defined(fabric_submit_diag):
    echo "submit-stage local-advance account=", node.identity.account,
      " event=", eventId
  node.advanceTouchedInbound(
    touchedEvents,
    touchedAttestationKeys,
  )
  node.flushTouchedLsmr(
    touchedEvents,
    touchedEventScopes,
    touchedEventSourcePeers,
    touchedAttestationKeys,
  )

proc scheduleInboundDrain(node: FabricNode) {.gcsafe, raises: [].} =
  if node.isNil:
    return
  if not node.inboundRunner.isNil and not node.inboundRunner.finished():
    node.inboundPending = true
    return
  node.inboundScheduled = true
  node.inboundPending = false
  node.inboundRunner = node.runInboundDrain()
  asyncSpawn node.inboundRunner

proc runInboundDrain(node: FabricNode): Future[void] {.async: (raises: []).} =
  if node.isNil:
    return
  try:
    await sleepAsync(FabricInboundDrainYield)
    while node.running:
      let startedAtMs = diagNowMs()
      let hasMore =
        try:
          {.cast(gcsafe).}:
            node.drainInbound()
        except Exception:
          false
      node.noteInboundSlice(diagNowMs() - startedAtMs)
      if not hasMore and not node.inboundPending:
        break
      node.inboundPending = false
      await sleepAsync(FabricInboundDrainYield)
  except CancelledError:
    discard
  finally:
    if not node.isNil:
      node.inboundScheduled = false
      node.inboundRunner = nil
      let rerun = node.inboundPending or node.inbound.len > 0
      node.inboundPending = false
      if node.running and rerun:
        node.scheduleInboundDrain()

proc enqueueInbound(node: FabricNode, message: sink InboundMessage) {.gcsafe, raises: [].} =
  node.inbound.addLast(message)
  node.scheduleInboundDrain()

proc selfAnnouncement(node: FabricNode): PeerAnnouncement =
  let route = node.selfRoutingPeer()
  let publicKey = encodePublicKeyHex(node.identity.publicKey)
  let digest = routeAnnouncementDigest(
    route.account,
    route.peerId,
    publicKey,
    route.path,
    route.addrs,
  )
  if node.routeAnnouncementVersion == 0'u64:
    node.routeAnnouncementVersion = 1'u64
    node.routeAnnouncementCreatedAtMs = diagNowMs()
  elif digest != node.routeAnnouncementDigest:
    inc node.routeAnnouncementVersion
    node.routeAnnouncementCreatedAtMs = diagNowMs()
  node.routeAnnouncementDigest = digest
  result = PeerAnnouncement(
    account: route.account,
    peerId: route.peerId,
    path: route.path,
    addrs: route.addrs,
    publicKey: publicKey,
    routeVersion: node.routeAnnouncementVersion,
    createdAt: node.routeAnnouncementCreatedAtMs,
  )
  result.signature = signBytes(
    node.identity.privateKey,
    result.peerAnnouncementSigningBytes(),
  )
  node.routingAnnouncements[result.account] = result
  node.routingPeers[result.account] = route

proc knownPeerAnnouncements(node: FabricNode): seq[PeerAnnouncement] =
  let selfItem = node.selfAnnouncement()
  result.add(selfItem)
  for account, item in node.routingAnnouncements.pairs:
    if account == selfItem.account:
      continue
    result.add(item)
  result.sort(proc(a, b: PeerAnnouncement): int = cmp(a.account, b.account))

proc fetchLookupRaw(node: FabricNode, key: string): Option[seq[byte]] {.gcsafe, raises: [].} =
  try:
    if key == "peer:self":
      return some(bytesOf(encodeObj(node.selfAnnouncement())))
    if key == "peer:table":
      return some(bytesOf(encodeObj(node.knownPeerAnnouncements())))
    if key == "summary:root":
      return some(bytesOf(encodeObj(node.antiEntropyRootSummary())))
    if key.startsWith("summary:era:"):
      let parts = key.split(":")
      if parts.len != 4:
        return none(seq[byte])
      let domain = parseAntiEntropyDomain(parts[2])
      if domain.isNone():
        return none(seq[byte])
      let era = parseUInt(parts[3])
      return some(bytesOf(encodeObj(node.antiEntropyEraSummary(domain.get(), era))))
    if key.startsWith("summary:tick:"):
      let parts = key.split(":")
      if parts.len != 5:
        return none(seq[byte])
      let domain = parseAntiEntropyDomain(parts[2])
      if domain.isNone():
        return none(seq[byte])
      let era = parseUInt(parts[3])
      let tick = parseUInt(parts[4])
      return some(bytesOf(encodeObj(
        node.antiEntropyTickSummary(domain.get(), era, tick)
      )))
    if node.store.isNil:
      return none(seq[byte])
    if key == "checkpoint:latest":
      return node.store.loadLatestCheckpointRaw()
    if key.startsWith("checkpoint:"):
      return node.store.loadCheckpointRaw(key["checkpoint:".len .. ^1])
    if key.startsWith("snapshot:"):
      return node.store.loadCheckpointSnapshotRaw(key["snapshot:".len .. ^1])
    if key.startsWith("event:"):
      return node.store.loadEventRaw(key["event:".len .. ^1])
    if key.startsWith("attestation:"):
      let parts = key.split(":")
      if parts.len != 4:
        return none(seq[byte])
      let role = parseAttestationRole(parts[2])
      if role.isNone():
        return none(seq[byte])
      return node.store.loadAttestationRaw(parts[1], role.get(), parts[3])
    if key.startsWith("eventcert:"):
      return node.store.loadEventCertificateRaw(key["eventcert:".len .. ^1])
    if key.startsWith("checkpoint-candidate:"):
      return node.store.loadCheckpointCandidateRaw(key["checkpoint-candidate:".len .. ^1])
    if key.startsWith("checkpoint-vote:"):
      let parts = key.split(":")
      if parts.len != 3:
        return none(seq[byte])
      return node.store.loadCheckpointVoteRaw(parts[1], parts[2])
    if key.startsWith("checkpoint-bundle:"):
      return node.store.loadCheckpointBundleRaw(key["checkpoint-bundle:".len .. ^1])
  except Exception:
    discard
  none(seq[byte])

proc drainInbound(node: FabricNode): bool {.raises: [].} =
  if node.isNil or not node.running:
    return false
  var changed = false
  var checkpointStateChanged = false
  var touchedEvents: seq[string] = @[]
  var touchedRelayEvents: seq[string] = @[]
  var touchedEventScopes = initTable[string, seq[LsmrPath]]()
  var touchedEventSourcePeers = initTable[string, seq[string]]()
  var touchedAttestationKeys: seq[string] = @[]
  let sliceStartedAtMs = diagNowMs()
  var processed = 0
  while processed < FabricInboundDrainBatchLimit:
    if node.inbound.len == 0:
      break
    if processed > 0 and diagNowMs() - sliceStartedAtMs >= FabricInboundSliceBudgetMs:
      when defined(fabric_submit_diag) or defined(fabric_lsmr_diag):
        echo "fabric-inbound slice-yield account=", node.identity.account,
          " processed=", processed,
          " remain=", node.inbound.len
      break
    let message = node.inbound.popFirst()
    inc processed
    when defined(fabric_diag):
      echo "drain ", node.identity.account, " kind=", ord(message.kind), " remain=", len(node.inbound)
    try:
      case message.kind
      of imPeerAnnouncement:
        var item = message.peerAnnouncement
        item.addrs = normalizeRoutingAddrs(item.addrs)
        if item.account.len == 0 or item.peerId.len == 0 or
            item.publicKey.len == 0 or item.signature.len == 0 or
            item.account == node.identity.account:
          continue
        let publicKey = PublicKey.init(item.publicKey).valueOr:
          continue
        if accountFromPublicKey(publicKey) != item.account:
          continue
        let announcedPeerId = PeerId.init(item.peerId).valueOr:
          continue
        if not announcedPeerId.match(publicKey):
          continue
        if not verifyBytes(publicKey, item.peerAnnouncementSigningBytes(), item.signature):
          continue
        let existing = node.routingAnnouncements.getOrDefault(item.account, PeerAnnouncement())
        if existing.account.len > 0 and
            item.peerAnnouncementVersion() <= existing.peerAnnouncementVersion():
          continue
        node.routingAnnouncements[item.account] = item
        node.routingPeers[item.account] = item.routingPeerFromAnnouncement()
        if not node.network.isNil and not node.network.switch.isNil:
          var addrs: seq[MultiAddress] = @[]
          for raw in item.addrs:
            let parsed = MultiAddress.init(raw)
            if parsed.isOk():
              addrs.add(parsed.get())
          if addrs.len > 0:
            node.network.switch.peerStore.setAddresses(announcedPeerId, addrs)
            node.network.rememberSubmitPeerAddrs(announcedPeerId, addrs)
      of imEvent:
        let registered = node.registerEvent(message.event, enqueuePending = false)
        let eventCertified = message.event.eventId in node.eventCertificates
        var deliveryObserved = false
        if message.sourcePeerId.len > 0:
          clearForwardedPeer(node.forwardedEvents, message.event.eventId, message.sourcePeerId)
          let deliveryObservation = observeInboundPeerDelivery(
            node.eventDeliveries,
            message.event.eventId,
            message.sourcePeerId,
          )
          if deliveryObservation.observed:
            node.noteLsmrDeliveryMutation()
            deliveryObserved = true
            if deliveryObservation.settledOutstanding:
              node.pendingEvents.excl(message.event.eventId)
              node.pendingEventScopes.clearPendingScopePrefixes(message.event.eventId)
            when defined(fabric_submit_diag):
              echo "delivery-observed account=", node.identity.account,
                " kind=event item=", message.event.eventId,
                " source=", message.sourcePeerId
        let scopeChanged =
          if message.relayScope.len > 0 and not eventCertified:
            discard node.pendingEvents.enqueuePending(message.event.eventId)
            let changed =
              node.pendingEventScopes.mergeScopePrefixes(
                message.event.eventId,
                @[message.relayScope],
              )
            if changed:
              discard touchedEventScopes.mergeScopePrefixes(
                message.event.eventId,
                @[message.relayScope],
              )
            changed
          else:
            false
        if message.sourcePeerId.len > 0:
          var sources = touchedEventSourcePeers.getOrDefault(message.event.eventId, @[])
          sources.appendUniqueText(message.sourcePeerId)
          touchedEventSourcePeers[message.event.eventId] = sources
        changed = registered or scopeChanged or deliveryObserved or changed
        if not eventCertified:
          touchedEvents.appendUnique(message.event.eventId)
        if not eventCertified and (registered or scopeChanged):
          touchedRelayEvents.appendUnique(message.event.eventId)
      of imAttestation:
        let registered = node.registerAttestation(message.attestation, enqueuePending = false)
        let eventCertified = message.attestation.eventId in node.eventCertificates
        var deliveryObserved = false
        if message.sourcePeerId.len > 0:
          let attKey = message.attestation.attestationKey()
          clearForwardedPeer(node.forwardedAttestations, attKey, message.sourcePeerId)
          let attObservation = observeInboundPeerDelivery(
            node.attestationDeliveries,
            attKey,
            message.sourcePeerId,
          )
          if attObservation.observed:
            node.noteLsmrDeliveryMutation()
            deliveryObserved = true
            if attObservation.settledOutstanding:
              node.pendingAttestations.excl(attKey)
              node.pendingAttestationScopes.clearPendingScopePrefixes(attKey)
            when defined(fabric_submit_diag):
              echo "delivery-observed account=", node.identity.account,
                  " kind=attestation item=", attKey,
                  " source=", message.sourcePeerId
          clearForwardedPeer(node.forwardedEvents, message.attestation.eventId, message.sourcePeerId)
          let eventObservation = observeInboundPeerDelivery(
            node.eventDeliveries,
            message.attestation.eventId,
            message.sourcePeerId,
          )
          if eventObservation.observed:
            node.noteLsmrDeliveryMutation()
            deliveryObserved = true
            if eventObservation.settledOutstanding:
              node.pendingEvents.excl(message.attestation.eventId)
              node.pendingEventScopes.clearPendingScopePrefixes(message.attestation.eventId)
            when defined(fabric_submit_diag):
              echo "delivery-observed account=", node.identity.account,
                " kind=event-from-att item=", message.attestation.eventId,
                " source=", message.sourcePeerId
        changed = registered or deliveryObserved or changed
        if not eventCertified:
          touchedEvents.appendUnique(message.attestation.eventId)
      of imAntiEntropyPull:
        let installed = node.applyAntiEntropyPullPayload(
          message.antiEntropyPullItem,
          message.antiEntropyPullPayload,
        )
        when defined(fabric_submit_diag):
          echo "anti-entropy exact-apply account=", node.identity.account,
            " domain=", antiEntropyDomainLabel(message.antiEntropyPullItem.domain),
            " key=", message.antiEntropyPullItem.key,
            " installed=", installed
        changed = installed or changed
        if installed and message.antiEntropyPullItem.causedByEventId.len > 0:
          touchedEvents.appendUnique(message.antiEntropyPullItem.causedByEventId)
      of imEventCertificate:
        let registered = node.registerEventCertificate(
          message.eventCertificate,
          enqueuePending = false,
        )
        let scopeChanged =
          if message.relayScope.len > 0:
            discard node.markPendingEventCertificate(message.eventCertificate.eventId)
            node.pendingEventCertificateScopes.mergeScopePrefixes(
              message.eventCertificate.eventId,
              @[message.relayScope],
            )
          else:
            false
        var deliveryObserved = false
        if message.sourcePeerId.len > 0:
          let certTargetKey =
            deliveryTargetKey(message.sourcePeerId, message.relayScope)
          let certObservation =
            if message.relayScope.len > 0:
              clearForwarded(
                node.forwardedEventCertificates,
                message.eventCertificate.eventId,
                certTargetKey,
              )
              observeInboundTargetDelivery(
                node.eventCertificateDeliveries,
                message.eventCertificate.eventId,
                certTargetKey,
              )
            else:
              clearForwardedPeer(
                node.forwardedEventCertificates,
                message.eventCertificate.eventId,
                message.sourcePeerId,
              )
              observeInboundPeerDelivery(
                node.eventCertificateDeliveries,
                message.eventCertificate.eventId,
                message.sourcePeerId,
              )
          if certObservation.observed:
            node.noteLsmrDeliveryMutation()
            deliveryObserved = true
            if certObservation.settledOutstanding:
              node.completeEventCertificateDelivery(
                message.eventCertificate.eventId,
              )
            when defined(fabric_submit_diag):
              echo "delivery-observed account=", node.identity.account,
                  " kind=event-certificate item=", message.eventCertificate.eventId,
                  " source=", message.sourcePeerId
          clearForwardedPeer(node.forwardedEvents, message.eventCertificate.eventId, message.sourcePeerId)
          let eventObservation = observeInboundPeerDelivery(
            node.eventDeliveries,
            message.eventCertificate.eventId,
            message.sourcePeerId,
          )
          if eventObservation.observed:
            node.noteLsmrDeliveryMutation()
            deliveryObserved = true
            if eventObservation.settledOutstanding:
              node.pendingEvents.excl(message.eventCertificate.eventId)
              node.pendingEventScopes.clearPendingScopePrefixes(message.eventCertificate.eventId)
            when defined(fabric_submit_diag):
              echo "delivery-observed account=", node.identity.account,
                " kind=event-from-cert item=", message.eventCertificate.eventId,
                " source=", message.sourcePeerId
        changed = registered or scopeChanged or deliveryObserved or changed
      of imCheckpointCandidate:
        let candidateId = message.checkpointCandidate.candidateId
        let registered = node.registerCandidate(message.checkpointCandidate)
        var deliveryObserved = false
        if message.sourcePeerId.len > 0:
          clearForwardedPeer(
            node.forwardedCheckpointCandidates,
            candidateId,
            message.sourcePeerId,
          )
          deliveryObserved = observeGlobalPeerDelivery(
            node.checkpointCandidateDeliveries,
            candidateId,
            message.sourcePeerId,
          ) or deliveryObserved
          if not node.checkpointCandidateDeliveries.hasOutstandingDelivery(candidateId):
            node.completeCheckpointCandidateDelivery(candidateId)
          when defined(fabric_submit_diag):
            if deliveryObserved:
              echo "delivery-observed account=", node.identity.account,
                " kind=checkpoint-candidate item=", candidateId,
                " source=", message.sourcePeerId
        changed = registered or deliveryObserved or changed
        checkpointStateChanged = true
      of imCheckpointVote:
        let voteKey = message.checkpointVote.checkpointVoteKey()
        let registered = node.registerCheckpointVote(message.checkpointVote)
        var deliveryObserved = false
        if message.sourcePeerId.len > 0:
          clearForwardedPeer(
            node.forwardedCheckpointVotes,
            voteKey,
            message.sourcePeerId,
          )
          deliveryObserved = observeGlobalPeerDelivery(
            node.checkpointVoteDeliveries,
            voteKey,
            message.sourcePeerId,
          ) or deliveryObserved
          if not node.checkpointVoteDeliveries.hasOutstandingDelivery(voteKey):
            node.completeCheckpointVoteDelivery(voteKey)
          when defined(fabric_submit_diag):
            if deliveryObserved:
              echo "delivery-observed account=", node.identity.account,
                " kind=checkpoint-vote item=", voteKey,
                " source=", message.sourcePeerId
        changed = registered or deliveryObserved or changed
        checkpointStateChanged = true
      of imCheckpointBundle:
        let bundle = message.checkpointBundle
        var deliveryObserved = false
        if message.sourcePeerId.len > 0:
          clearForwardedPeer(
            node.forwardedCheckpointBundles,
            bundle.certificate.checkpointId,
            message.sourcePeerId,
          )
          deliveryObserved = observeGlobalPeerDelivery(
            node.checkpointBundleDeliveries,
            bundle.certificate.checkpointId,
            message.sourcePeerId,
          ) or deliveryObserved
          if not node.checkpointBundleDeliveries.hasOutstandingDelivery(bundle.certificate.checkpointId):
            node.completeCheckpointBundleDelivery(bundle.certificate.checkpointId)
          when defined(fabric_submit_diag):
            if deliveryObserved:
              echo "delivery-observed account=", node.identity.account,
                " kind=checkpoint-bundle item=", bundle.certificate.checkpointId,
                " source=", message.sourcePeerId
        if bundle.snapshot.len > 0 and bundle.certificate.checkpointId notin node.checkpoints:
          node.checkpointBundleCache[bundle.certificate.checkpointId] = bundle
          let registered = node.registerCheckpoint(
            bundle.certificate,
            decodePolarSnapshot(bundle.snapshot),
          )
          changed = registered or deliveryObserved or changed
          checkpointStateChanged = registered or checkpointStateChanged
        else:
          changed = deliveryObserved or changed
        checkpointStateChanged = true
      of imAvoProposal:
        changed = node.registerAvoProposal(message.avoProposal) or changed
        checkpointStateChanged = true
      of imAvoApproval:
        changed = node.registerAvoApproval(message.proposalId, message.validator) or changed
        checkpointStateChanged = true
    except Exception:
      discard
  if changed:
    try:
      if node.usesLsmrPrimary():
        when defined(fabric_lsmr_diag):
          echo "drain-stage advance-local account=", node.identity.account,
            " touched=", touchedEvents.len,
            " inboundRemain=", node.inbound.len
        node.advanceTouchedInbound(
          touchedEvents,
          touchedAttestationKeys,
        )
        node.flushTouchedLsmr(
          touchedRelayEvents,
          touchedEventScopes,
          touchedEventSourcePeers,
          touchedAttestationKeys,
        )
        if checkpointStateChanged:
          node.scheduleCheckpointAdvance()
      else:
        when defined(fabric_diag):
          echo "converge-start ", node.identity.account
        node.converge()
        when defined(fabric_diag):
          echo "converge-done ", node.identity.account
    except Exception:
      discard
  node.inbound.len > 0


proc bootstrapRoutingSnapshot(node: FabricNode): Future[seq[PeerAnnouncement]] {.async: (raises: []).} =
  if node.network.isNil:
    return @[]
  var bootstrapAddrs: seq[string] = @[]
  case node.config.routingMode
  of RoutingPlaneMode.legacyOnly:
    for raw in node.config.bootstrapAddrs:
      bootstrapAddrs.appendUnique(raw)
    for raw in node.config.legacyBootstrapAddrs:
      bootstrapAddrs.appendUnique(raw)
  of RoutingPlaneMode.dualStack:
    for raw in node.config.bootstrapAddrs:
      bootstrapAddrs.appendUnique(raw)
    for raw in node.config.legacyBootstrapAddrs:
      bootstrapAddrs.appendUnique(raw)
    for raw in node.config.lsmrBootstrapAddrs:
      bootstrapAddrs.appendUnique(raw)
  of RoutingPlaneMode.lsmrOnly:
    for raw in node.config.lsmrBootstrapAddrs:
      bootstrapAddrs.appendUnique(raw)
  for raw in bootstrapAddrs:
    let parsed = parseFullAddress(raw)
    if parsed.isErr():
      continue
    let peerId = $parsed.get()[0]
    let payload = await node.network.fetchRaw(peerId, "peer:table")
    if payload.isNone():
      continue
    try:
      let peers = decodeObj[seq[PeerAnnouncement]](stringOf(payload.get()))
      for peer in peers:
        result.add(peer)
    except Exception:
      discard

proc syncRoutingSnapshotTask(node: FabricNode, peerIdText: string): Future[void] {.async: (raises: []).} =
  var synced = false
  try:
    if node.isNil or node.network.isNil or not node.running or peerIdText.len == 0:
      return
    let peerId = PeerId.init(peerIdText).valueOr:
      return
    let payload = await node.network.fetchRaw(
      peerIdText,
      "peer:table",
      timeout = FabricRoutingSnapshotSyncTimeout,
      maxAttempts = 1,
    )
    if payload.isSome():
      let peers = decodeObj[seq[PeerAnnouncement]](stringOf(payload.get()))
      for peer in peers:
        node.enqueueInbound(InboundMessage(kind: imPeerAnnouncement, peerAnnouncement: peer))
      synced = true
      when defined(fabric_submit_diag) or defined(fabric_lsmr_diag):
        echo "routing-sync ok account=", node.identity.account,
          " peer=", peerIdText,
          " peers=", peers.len
      return
    when defined(fabric_submit_diag) or defined(fabric_lsmr_diag):
      echo "routing-sync miss account=", node.identity.account,
        " peer=", peerIdText,
        " connected=",
        (if node.network.isNil or node.network.switch.isNil: false else: node.network.switch.isConnected(peerId))
  except CatchableError:
    discard
  finally:
    if not node.isNil:
      node.syncingRoutingPeers.excl(peerIdText)
      if peerIdText.len > 0:
        if synced:
          node.routingSnapshotSyncedGenerations[peerIdText] =
            node.routingSnapshotWantedGenerations.getOrDefault(peerIdText, 0'u64)
        node.routingSnapshotNextRefreshAtMs[peerIdText] =
          diagNowMs() + FabricRoutingSnapshotResyncMs

proc refreshUnsyncedRoutingSnapshots(node: FabricNode) =
  if node.isNil or node.network.isNil or node.network.switch.isNil or not node.running:
    return
  let nowMs = diagNowMs()
  for peerIdText in node.connectedPeerIds():
    if peerIdText == peerIdString(node.identity.peerId):
      continue
    let nextRefreshAtMs =
      node.routingSnapshotNextRefreshAtMs.getOrDefault(peerIdText, 0'i64)
    let synced = node.routingSnapshotSyncedGenerations.getOrDefault(peerIdText, 0'u64)
    let wanted = node.routingSnapshotWantedGenerations.getOrDefault(peerIdText, 0'u64)
    if wanted == 0'u64:
      node.wantRoutingSnapshotSync(peerIdText, immediate = true)
    if peerIdText in node.syncingRoutingPeers:
      continue
    let currentSynced =
      node.routingSnapshotSyncedGenerations.getOrDefault(peerIdText, 0'u64)
    let currentWanted =
      node.routingSnapshotWantedGenerations.getOrDefault(peerIdText, 0'u64)
    if currentSynced >= currentWanted:
      continue
    if nextRefreshAtMs > 0 and nowMs < nextRefreshAtMs:
      continue
    node.ensureRoutingSnapshotSync(peerIdText)

proc antiEntropyNeedsSync(node: FabricNode): bool {.gcsafe, raises: [].} =
  if node.isNil:
    return false
  if node.antiEntropyMissingEventNeeded():
    return true
  for queue in node.pendingWitnessPullsByPeer.values:
    if queue.len > 0:
      return true
  if node.inflightWitnessPullByPeer.len > 0:
    return true
  for eventId in node.pendingCertificationEvents:
    if not node.certificationEventTracked(eventId):
      continue
    if node.certificationMissingParticipantPulls(eventId).len > 0:
      return true
    if node.certificationMissingWitnessPulls(eventId).len > 0:
      return true
  if node.missingEventCertificatePulls().len > 0:
    return true
  if node.antiEntropyCheckpointCandidateNeeded():
    return true
  if node.antiEntropyCheckpointVoteNeeded():
    return true
  if node.antiEntropyCheckpointBundleNeeded():
    return true
  false

proc antiEntropyDomainNeeded(
    node: FabricNode, domain: AntiEntropyDomain
): bool {.gcsafe, raises: [].} =
  if node.isNil:
    return false
  case domain
  of aedEvent:
    node.antiEntropyMissingEventNeeded()
  of aedAttParticipant, aedAttWitness, aedEventCert:
    for queue in node.pendingWitnessPullsByPeer.values:
      for item in queue:
        if item.domain == domain:
          return true
    for item in node.inflightWitnessPullByPeer.values:
      if item.domain == domain:
        return true
    if domain == aedEventCert:
      return node.missingEventCertificatePulls().len > 0
    for eventId in node.pendingCertificationEvents:
      if not node.certificationEventTracked(eventId):
        continue
      let missing =
        if domain == aedAttParticipant:
          node.certificationMissingParticipantPulls(eventId)
        else:
          node.certificationMissingWitnessPulls(eventId)
      if missing.len > 0:
        return true
    false
  of aedCheckpointCandidate:
    node.antiEntropyCheckpointCandidateNeeded()
  of aedCheckpointVote:
    node.antiEntropyCheckpointVoteNeeded()
  of aedCheckpointBundle:
    node.antiEntropyCheckpointBundleNeeded()

proc antiEntropySummaryTargetPeers(node: FabricNode): seq[string] {.gcsafe, raises: [].} =
  if node.isNil:
    return @[]
  var seen = initHashSet[string]()
  for peerIdText in node.connectedPeerIds():
    if peerIdText.len == 0 or peerIdText == peerIdString(node.identity.peerId) or
        peerIdText in seen:
      continue
    seen.incl(peerIdText)
    result.add(peerIdText)
  result.sort(system.cmp[string])

proc antiEntropyPeerDomainActive(
    node: FabricNode, peerIdText: string, domain: AntiEntropyDomain
): bool {.gcsafe, raises: [].} =
  if node.isNil or peerIdText.len == 0:
    return false
  let ownerKey = antiEntropyPullOwnerKey(peerIdText, domain)
  if ownerKey in node.inflightWitnessPullByPeer:
    return true
  ownerKey in node.pendingWitnessPullsByPeer and
    node.pendingWitnessPullsByPeer.getOrDefault(ownerKey).len > 0

proc clearAntiEntropySummarySync(
    node: FabricNode, peerIdText: string
) {.gcsafe, raises: [].} =
  if node.isNil or peerIdText.len == 0:
    return
  if peerIdText in node.antiEntropySummaryRunners:
    let runner = node.antiEntropySummaryRunners.getOrDefault(peerIdText)
    if not runner.isNil and not runner.finished():
      runner.cancelSoon()
    node.antiEntropySummaryRunners.del(peerIdText)

proc ensureAntiEntropySummarySync(
    node: FabricNode, peerIdText: string
) {.gcsafe, raises: [].} =
  if node.isNil or node.network.isNil or not node.running or peerIdText.len == 0:
    return
  if peerIdText == peerIdString(node.identity.peerId):
    return
  let nextRefreshAtMs =
    node.antiEntropySummaryNextRefreshAtMs.getOrDefault(peerIdText, 0'i64)
  if nextRefreshAtMs > 0 and diagNowMs() < nextRefreshAtMs:
    return
  if peerIdText in node.antiEntropySummaryRunners:
    let runner = node.antiEntropySummaryRunners.getOrDefault(peerIdText)
    if not runner.isNil and not runner.finished():
      return
    node.antiEntropySummaryRunners.del(peerIdText)
  let runner = node.antiEntropySummaryTask(peerIdText)
  node.antiEntropySummaryRunners[peerIdText] = runner
  asyncSpawn runner

proc antiEntropySummaryTask(
    node: FabricNode, peerIdText: string
): Future[void] {.async: (raises: []).} =
  var enqueuedAny = false
  try:
    if node.isNil or node.network.isNil or not node.running or peerIdText.len == 0:
      return
    let rootPayload = await node.network.fetchRaw(
      peerIdText,
      "summary:root",
      timeout = 1.seconds,
      maxAttempts = 1,
    )
    if rootPayload.isNone():
      return
    let remoteRoot = decodeObj[AntiEntropyRootSummary](stringOf(rootPayload.get()))
    let localRoot = node.antiEntropyRootSummary()
    var localDomainByName = initTable[string, AntiEntropyDomainRootSummary]()
    for item in localRoot.domains:
      localDomainByName[item.domain] = item
    for remoteDomain in remoteRoot.domains:
      let domainOpt = parseAntiEntropyDomain(remoteDomain.domain)
      if domainOpt.isNone():
        continue
      let domain = domainOpt.get()
      if not node.antiEntropyDomainNeeded(domain):
        continue
      if node.antiEntropyPeerDomainActive(peerIdText, domain):
        continue
      let localDomain = localDomainByName.getOrDefault(
        remoteDomain.domain,
        AntiEntropyDomainRootSummary(
          domain: remoteDomain.domain,
          maxClock: GanzhiClock(era: 0, tick: 0),
          eras: @[],
        ),
      )
      var localEraDigests = initTable[uint64, string]()
      for item in localDomain.eras:
        localEraDigests[item.era] = item.digest
      for remoteEra in remoteDomain.eras:
        if localEraDigests.getOrDefault(remoteEra.era) == remoteEra.digest:
          continue
        when defined(fabric_submit_diag):
          echo "anti-entropy summary-mismatch account=", node.identity.account,
            " peer=", peerIdText,
            " domain=", remoteDomain.domain,
            " era=", remoteEra.era
        let eraPayload = await node.network.fetchRaw(
          peerIdText,
          "summary:era:" & remoteDomain.domain & ":" & $remoteEra.era,
          timeout = 1.seconds,
          maxAttempts = 1,
        )
        if eraPayload.isNone():
          continue
        let remoteEraSummary = decodeObj[AntiEntropyEraSummary](stringOf(eraPayload.get()))
        let localEraSummary = node.antiEntropyEraSummary(domain, remoteEra.era)
        var localTickDigests = initTable[uint64, string]()
        for item in localEraSummary.ticks:
          localTickDigests[item.tick] = item.digest
        for remoteTick in remoteEraSummary.ticks:
          if localTickDigests.getOrDefault(remoteTick.tick) == remoteTick.digest:
            continue
          let tickPayload = await node.network.fetchRaw(
            peerIdText,
            "summary:tick:" & remoteDomain.domain & ":" & $remoteEra.era & ":" &
              $remoteTick.tick,
            timeout = 1.seconds,
            maxAttempts = 1,
          )
          if tickPayload.isNone():
            continue
          let remoteTickSummary = decodeObj[AntiEntropyTickSummary](stringOf(tickPayload.get()))
          when defined(fabric_submit_diag):
            echo "anti-entropy tick-diff account=", node.identity.account,
              " peer=", peerIdText,
              " domain=", remoteDomain.domain,
              " era=", remoteEra.era,
              " tick=", remoteTick.tick,
              " items=", remoteTickSummary.items.len
          for key in remoteTickSummary.items:
            if node.antiEntropyLocalHasKey(domain, key):
              continue
            if node.witnessPullActive(domain, key):
              continue
            let item = node.antiEntropyPullItem(
              peerIdText,
              domain,
              key,
              remoteEra.era,
              remoteTick.tick,
            )
            if item.isNone():
              continue
            if not node.witnessPullStillNeeded(item.get()):
              continue
            if node.enqueueWitnessPullWorkItem(item.get()):
              enqueuedAny = true
            node.ensureWitnessPullRunner(item.get().targetPeerId, item.get().domain)
            when defined(fabric_submit_diag):
              echo "anti-entropy exact-pull account=", node.identity.account,
                " peer=", peerIdText,
                " domain=", remoteDomain.domain,
                " key=", key
  except CancelledError:
    discard
  except CatchableError:
    discard
  finally:
    if not node.isNil:
      node.antiEntropySummaryNextRefreshAtMs[peerIdText] =
        if enqueuedAny: 0'i64 else: diagNowMs() + FabricAntiEntropySummaryResyncMs
      node.antiEntropySummaryRunners.del(peerIdText)

proc refreshAntiEntropySummarySync(node: FabricNode) {.gcsafe, raises: [].} =
  if node.isNil or not node.running or node.network.isNil or not node.usesLsmrPrimary():
    return
  if not node.antiEntropyNeedsSync():
    for peerIdText in node.antiEntropySummaryRunners.keys.toSeq():
      node.clearAntiEntropySummarySync(peerIdText)
    return
  let targetPeers = node.antiEntropySummaryTargetPeers()
  let targetSet = targetPeers.toHashSet()
  for peerIdText in node.antiEntropySummaryRunners.keys.toSeq():
    if peerIdText notin targetSet:
      node.clearAntiEntropySummarySync(peerIdText)
  for peerIdText in targetPeers:
    node.ensureAntiEntropySummarySync(peerIdText)

proc routingSnapshotGateReady(node: FabricNode): bool {.gcsafe, raises: [].} =
  if node.isNil or node.network.isNil or node.network.switch.isNil or
      node.network.switch.peerStore.isNil:
    return false
  if not node.usesLsmrPrimary():
    return true
  let connected = node.connectedPeerIds().len
  let active = node.network.switch.peerStore[ActiveLsmrBook].book.len
  active >= connected + 1

proc clearRoutingSnapshotSync(node: FabricNode, peerIdText: string) =
  if node.isNil or peerIdText.len == 0:
    return
  node.wantRoutingSnapshotSync(peerIdText, immediate = true)

proc ensureRoutingSnapshotSync(node: FabricNode, peerIdText: string) =
  if node.isNil or node.network.isNil or not node.running or peerIdText.len == 0:
    return
  if peerIdText == peerIdString(node.identity.peerId):
    return
  if peerIdText in node.syncingRoutingPeers:
    return
  if node.routingSnapshotSyncedGenerations.getOrDefault(peerIdText, 0'u64) >=
      node.routingSnapshotWantedGenerations.getOrDefault(peerIdText, 1'u64):
    return
  node.syncingRoutingPeers.incl(peerIdText)
  asyncSpawn node.syncRoutingSnapshotTask(peerIdText)

proc connectedPeerIds(node: FabricNode): seq[string] {.gcsafe, raises: [].} =
  if node.isNil or node.network.isNil or node.network.switch.isNil:
    return @[]
  var seen = initHashSet[string]()
  for peerId in node.network.switch.connectedPeers(Direction.Out) &
      node.network.switch.connectedPeers(Direction.In):
    let peerIdText = $peerId
    if peerIdText in seen:
      continue
    seen.incl(peerIdText)
    result.add(peerIdText)

proc maintenanceStep(node: FabricNode): bool {.raises: [].} =
  if node.isNil or not node.running:
    return false
  try:
    let stepStartedAtMs = diagNowMs()
    if node.drainInbound():
      result = true
    if node.maintenanceShouldYield(stepStartedAtMs, "drain-inbound"):
      return true
    node.refreshUnsyncedRoutingSnapshots()
    node.refreshAntiEntropySummarySync()
    node.pruneStaleLsmrDeliveries()
    node.refreshLsmrPending()
    node.refreshPendingCertificationEvents()
    node.prunePendingCertificationState()
    let checkpointKnown =
      node.latestCheckpointId.len > 0 and node.latestCheckpointId in node.checkpoints
    let checkpointEra =
      if checkpointKnown:
        node.checkpoints.getOrDefault(node.latestCheckpointId).candidate.era
      else:
        0'u64
    var budgetExhausted = false
    when defined(fabric_submit_diag):
      if node.pendingEvents.len > 0 or
          node.pendingAttestations.len > 0 or
          node.pendingEventCertificates.len > 0 or
          node.pendingCheckpointCandidates.len > 0 or
          node.pendingCheckpointVotes.len > 0 or
          node.pendingCheckpointBundles.len > 0:
        echo "maintenance-stage account=", node.identity.account,
          " pendingEvents=", node.pendingEvents.len,
          " pendingAttestations=", node.pendingAttestations.len,
          " pendingEventCerts=", node.pendingEventCertificates.len,
          " pendingCandidates=", node.pendingCheckpointCandidates.len,
          " pendingVotes=", node.pendingCheckpointVotes.len,
          " pendingCheckpoints=", node.pendingCheckpointBundles.len
    let eventKeys = node.pendingEventKeys()
    if eventKeys.len > FabricMaintenanceBatchLimit:
      budgetExhausted = true
    for eventId in rotatePendingKeys(
        eventKeys,
        node.eventMaintenanceCursor,
        FabricMaintenanceBatchLimit,
    ):
      if eventId notin node.events or eventId in node.isolatedEvents:
        node.completeEventDelivery(eventId)
        continue
      if node.latestCheckpointId.len > 0 and node.latestCheckpointId in node.checkpoints and
          node.events[eventId].clock.era < node.checkpoints[node.latestCheckpointId].candidate.era:
        node.completeEventDelivery(eventId)
        continue
      try:
        node.disseminateEvent(
          node.events[eventId],
          node.pendingEventScopes.pendingScopePrefixes(eventId),
        )
      except Exception as exc:
        node.logMaintenanceError("event", eventId, exc)
      if node.maintenanceShouldYield(stepStartedAtMs, "event", eventId):
        return true
    for item in node.missingEventCertificatePulls():
      if node.witnessPullStillNeeded(item):
        discard node.enqueueWitnessPullWorkItem(item)
        node.ensureWitnessPullRunner(item.targetPeerId, item.domain)
    if node.maintenanceShouldYield(stepStartedAtMs, "eventcert-pull"):
      return true
    let pendingAttestationKeys = node.pendingAttestationKeys()
    let pendingAttestationCount = pendingAttestationKeys.len
    let attestationKeys = rotatePendingKeys(
      pendingAttestationKeys,
      node.attestationMaintenanceCursor,
      FabricMaintenanceBatchLimit,
    )
    if pendingAttestationCount > attestationKeys.len:
      budgetExhausted = true
    for key in attestationKeys:
      let att = node.attestationForKey(key)
      if att.isNone() or att.get().eventId notin node.events:
        node.completeAttestationDelivery(key)
        continue
      if att.get().eventId in node.eventCertificates:
        node.completeAttestationDelivery(key)
        continue
      if node.usesLsmrPrimary() and
          node.latestCheckpointId.len > 0 and
          node.latestCheckpointId in node.checkpoints and
          node.events[att.get().eventId].clock.era < node.checkpoints[node.latestCheckpointId].candidate.era:
        node.completeAttestationDelivery(key)
        continue
      try:
        node.disseminateAttestation(node.events[att.get().eventId], att.get())
      except Exception as exc:
        node.logMaintenanceError("attestation", key, exc)
      if node.maintenanceShouldYield(stepStartedAtMs, "attestation", key):
        return true
    node.pruneCheckpointedEventCertificateDeliveries()
    let pendingEventCertificateCount = node.pendingEventCertificates.len
    let eventCertificateKeys =
      node.drainPendingEventCertificateFrontier(FabricMaintenanceBatchLimit)
    if pendingEventCertificateCount > eventCertificateKeys.len:
      budgetExhausted = true
    for eventId in eventCertificateKeys:
      if eventId notin node.eventCertificates or eventId notin node.events:
        node.completeEventCertificateDelivery(eventId)
        continue
      if node.latestCheckpointId.len > 0 and node.latestCheckpointId in node.checkpoints and
          node.events[eventId].clock.era < node.checkpoints[node.latestCheckpointId].candidate.era:
        node.completeEventCertificateDelivery(eventId)
        continue
      try:
        node.disseminateEventCertificate(
          node.events[eventId],
          node.eventCertificates[eventId],
        )
      except Exception as exc:
        node.logMaintenanceError("event-certificate", eventId, exc)
      if node.maintenanceShouldYield(stepStartedAtMs, "event-certificate", eventId):
        return true
    node.pruneLsmrPending()
    node.pruneStaleLsmrDeliveries()
    let checkpointOwnerOutstanding = node.checkpointOwnerOutstanding()
    let checkpointBlocking = node.checkpointFrontierBlockingEra()
    let checkpointReady = node.checkpointAdvanceReady()
    if not checkpointReady:
      when defined(fabric_submit_diag):
        if node.checkpointAdvancePending or
            node.pendingCheckpointCandidates.len > 0 or
            node.pendingCheckpointVotes.len > 0 or
            node.pendingCheckpointBundles.len > 0 or
            node.pendingAvoProposals.len > 0 or
            node.pendingAvoApprovals.len > 0:
          echo "checkpoint-gate wait account=", node.identity.account,
            " pendingEvents=", checkpointOwnerOutstanding.pendingEvents,
            " pendingAttestations=", checkpointOwnerOutstanding.pendingAttestations,
            " pendingEventCerts=", checkpointOwnerOutstanding.pendingEventCerts,
            " pendingCertificationEvents=", checkpointOwnerOutstanding.pendingCertificationEvents,
            " pendingWitnessPulls=", checkpointOwnerOutstanding.pendingWitnessPulls,
            " eventOutstanding=", checkpointOwnerOutstanding.liveEvents,
            " attOutstanding=", checkpointOwnerOutstanding.liveAttestations,
            " certOutstanding=", checkpointOwnerOutstanding.liveCertificates,
            " blockingEra=",
            (if checkpointBlocking.hasBlockingEra: $checkpointBlocking.era else: "-"),
            " firstMissing=",
            (if checkpointBlocking.hasBlockingEra: checkpointBlocking.firstMissingEventId else: ""),
            " pendingCandidates=", node.pendingCheckpointCandidates.len,
            " pendingVotes=", node.pendingCheckpointVotes.len,
            " pendingBundles=", node.pendingCheckpointBundles.len
    else:
      when defined(fabric_submit_diag):
        if node.eventDeliveries.hasOutstandingDeliveries() or
            node.attestationDeliveries.hasOutstandingDeliveries() or
            node.eventCertificateDeliveries.hasOutstandingDeliveries():
          echo "checkpoint-gate ignore-stale-ledger account=", node.identity.account,
            " eventOutstanding=", node.eventDeliveries.hasOutstandingDeliveries(),
            " attOutstanding=", node.attestationDeliveries.hasOutstandingDeliveries(),
            " certOutstanding=", node.eventCertificateDeliveries.hasOutstandingDeliveries(),
            " liveEventOutstanding=", checkpointOwnerOutstanding.liveEvents,
            " liveAttOutstanding=", checkpointOwnerOutstanding.liveAttestations,
            " liveCertOutstanding=", checkpointOwnerOutstanding.liveCertificates,
            " pendingEvents=", checkpointOwnerOutstanding.pendingEvents,
            " pendingAttestations=", checkpointOwnerOutstanding.pendingAttestations,
            " pendingEventCerts=", checkpointOwnerOutstanding.pendingEventCerts,
            " pendingCertificationEvents=", checkpointOwnerOutstanding.pendingCertificationEvents,
            " pendingWitnessPulls=", checkpointOwnerOutstanding.pendingWitnessPulls
    if node.checkpointAdvancePending:
      if checkpointReady:
        when defined(fabric_submit_diag):
          echo "checkpoint-advance dispatch account=", node.identity.account
        node.scheduleCheckpointAdvanceRunner()
        result = true
      else:
        when defined(fabric_submit_diag):
          echo "checkpoint-advance wait account=", node.identity.account,
            " pendingEvents=", node.pendingEvents.len,
            " pendingAttestations=", node.pendingAttestations.len,
            " pendingEventCerts=", node.pendingEventCertificates.len
      if node.maintenanceShouldYield(stepStartedAtMs, "checkpoint-advance"):
        return true
    if checkpointReady:
      let checkpointCandidateKeys = node.pendingCheckpointCandidates.sortedKeys()
      if checkpointCandidateKeys.len > FabricMaintenanceBatchLimit:
        budgetExhausted = true
      for candidateId in rotatePendingKeys(
          checkpointCandidateKeys,
          node.checkpointCandidateMaintenanceCursor,
          FabricMaintenanceBatchLimit,
      ):
        if candidateId notin node.checkpointCandidates or node.hasCheckpointForCandidate(candidateId):
          node.completeCheckpointCandidateDelivery(candidateId)
          continue
        try:
          node.disseminateCheckpointCandidate(node.checkpointCandidates[candidateId])
        except Exception as exc:
          node.logMaintenanceError("checkpoint-candidate", candidateId, exc)
      let checkpointVoteKeys = node.pendingCheckpointVotes.sortedKeys()
      if checkpointVoteKeys.len > FabricMaintenanceBatchLimit:
        budgetExhausted = true
      for key in rotatePendingKeys(
          checkpointVoteKeys,
          node.checkpointVoteMaintenanceCursor,
          FabricMaintenanceBatchLimit,
      ):
        let vote = node.checkpointVoteForKey(key)
        if vote.isNone() or node.hasCheckpointForCandidate(vote.get().candidateId):
          node.completeCheckpointVoteDelivery(key)
          continue
        try:
          node.disseminateCheckpointVote(vote.get())
        except Exception as exc:
          node.logMaintenanceError("checkpoint-vote", key, exc)
      let checkpointBundleKeys = node.pendingCheckpointBundles.sortedKeys()
      if checkpointBundleKeys.len > FabricMaintenanceBatchLimit:
        budgetExhausted = true
      for checkpointId in rotatePendingKeys(
          checkpointBundleKeys,
          node.checkpointBundleMaintenanceCursor,
          FabricMaintenanceBatchLimit,
      ):
        if checkpointId notin node.checkpoints or checkpointId notin node.checkpointSnapshots:
          node.completeCheckpointBundleDelivery(checkpointId)
          continue
        try:
          node.disseminateCheckpoint(node.checkpoints[checkpointId], node.checkpointSnapshots[checkpointId])
        except Exception as exc:
          node.logMaintenanceError("checkpoint-bundle", checkpointId, exc)
        if node.maintenanceShouldYield(stepStartedAtMs, "checkpoint-bundle", checkpointId):
          return true
      let avoProposalKeys = node.pendingAvoProposals.sortedKeys()
      if avoProposalKeys.len > FabricMaintenanceBatchLimit:
        budgetExhausted = true
      for proposalId in rotatePendingKeys(
          avoProposalKeys,
          node.avoProposalMaintenanceCursor,
          FabricMaintenanceBatchLimit,
      ):
        if proposalId notin node.avoProposals:
          node.pendingAvoProposals.excl(proposalId)
          continue
        try:
          node.disseminateAvoProposal(node.avoProposals[proposalId])
        except Exception as exc:
          node.logMaintenanceError("avo-proposal", proposalId, exc)
        if node.maintenanceShouldYield(stepStartedAtMs, "avo-proposal", proposalId):
          return true
      let avoApprovalKeys = node.pendingAvoApprovals.sortedKeys()
      if avoApprovalKeys.len > FabricMaintenanceBatchLimit:
        budgetExhausted = true
      for key in rotatePendingKeys(
          avoApprovalKeys,
          node.avoApprovalMaintenanceCursor,
          FabricMaintenanceBatchLimit,
      ):
        let parts = key.split(":")
        if parts.len != 2 or parts[0] notin node.avoProposals or
            parts[1] notin node.avoApprovals.getOrDefault(parts[0], @[]):
          node.pendingAvoApprovals.excl(key)
          continue
        try:
          node.disseminateAvoApproval(parts[0], parts[1])
        except Exception as exc:
          node.logMaintenanceError("avo-approval", key, exc)
        if node.maintenanceShouldYield(stepStartedAtMs, "avo-approval", key):
          return true
    if budgetExhausted and node.running:
      when defined(fabric_submit_diag):
        echo "maintenance-continue account=", node.identity.account,
          " pendingEvents=", node.pendingEvents.len,
          " pendingAttestations=", node.pendingAttestations.len,
          " pendingEventCerts=", node.pendingEventCertificates.len
    result = result or budgetExhausted
  except Exception as exc:
    node.logMaintenanceError("maintenance-step", "", exc)
    return false

proc logMaintenanceError(
    node: FabricNode, phase, itemKey: string, exc: ref Exception
) {.gcsafe, raises: [].} =
  if node.isNil or exc.isNil:
    return
  echo "maintenance-error account=", node.identity.account,
    " phase=", phase,
    " item=", itemKey,
    " err=", exc.msg

proc maintenanceLoop(node: FabricNode, initialDelayMs: int) {.async: (raises: []).} =
  if node.isNil:
    return
  try:
    if initialDelayMs > 0:
      await sleepAsync(initialDelayMs)
    await sleepAsync(FabricMaintenanceYield)
    while node.running:
      if not node.maintenancePending and not node.maintenanceImmediatePending:
        break
      node.maintenancePending = false
      node.maintenanceImmediatePending = false
      let startedAtMs = diagNowMs()
      let needsContinue =
        try:
          {.cast(gcsafe).}:
            node.maintenanceStep()
        except Exception:
          false
      node.noteMaintenanceSlice(diagNowMs() - startedAtMs)
      if needsContinue:
        node.maintenanceImmediatePending = true
      if not node.maintenancePending and not node.maintenanceImmediatePending:
        break
      await sleepAsync(FabricMaintenanceYield)
  except CancelledError:
    discard
  finally:
    if not node.isNil:
      node.maintenanceScheduled = false
      node.maintenanceImmediateScheduled = false
      node.maintenanceRunner = nil
      if node.running and (node.maintenancePending or node.maintenanceImmediatePending):
        node.scheduleMaintenance(immediate = node.maintenanceImmediatePending)

proc scheduleMaintenance(node: FabricNode, immediate = false) {.gcsafe, raises: [].} =
  if node.isNil or not node.running:
    return
  if immediate:
    node.maintenanceImmediatePending = true
  node.maintenancePending = true
  if not node.maintenanceRunner.isNil and not node.maintenanceRunner.finished():
    return
  node.maintenanceScheduled = true
  node.maintenanceImmediateScheduled = immediate
  let initialDelayMs = if immediate: 0 else: 10
  proc runner() {.async, gcsafe.} =
    try:
      await node.maintenanceLoop(initialDelayMs)
    except CancelledError:
      discard
  node.maintenanceRunner = runner()
  asyncSpawn node.maintenanceRunner

proc newFabricNode*(identity: NodeIdentity, genesis: GenesisSpec, lsmrPath: LsmrPath = @[]): FabricNode =
  result = FabricNode(
    config: FabricNodeConfig(
      dataDir: "",
      identityPath: "",
      genesisPath: "",
      listenAddrs: @[],
      bootstrapAddrs: @[],
      legacyBootstrapAddrs: @[],
      lsmrBootstrapAddrs: @[],
      lsmrPath: lsmrPath,
      routingMode: RoutingPlaneMode.legacyOnly,
      primaryPlane: PrimaryRoutingPlane.legacy,
      lsmrConfig: none(LsmrConfig),
    ),
    identity: identity,
    genesis: genesis,
    peers: initOrderedTable[string, FabricNode](),
    routingPeers: initOrderedTable[string, RoutingPeer](),
    routingAnnouncements: initOrderedTable[string, PeerAnnouncement](),
    routeAnnouncementVersion: 0'u64,
    routeAnnouncementDigest: "",
    routeAnnouncementCreatedAtMs: 0'i64,
    store: nil,
    network: nil,
    running: false,
    events: initOrderedTable[string, FabricEvent](),
    eventAttestations: initTable[string, seq[EventAttestation]](),
    attestationsByKey: initTable[string, EventAttestation](),
    eventCertificates: initOrderedTable[string, EventCertificate](),
    eventCertificationProgress: initTable[string, EventCertificationProgress](),
    pendingCertificationEvents: initHashSet[string](),
    pendingCertificationEventFrontier: initHeapQueue[PendingCertificationEventTicket](),
    pendingCertificationEventEpochs: initTable[string, uint64](),
    pendingCertificationEventNextEpoch: 0'u64,
    pendingWitnessPullsByPeer: initTable[string, Deque[AntiEntropyPullWorkItem]](),
    pendingWitnessPullKeysByPeer: initTable[string, HashSet[string]](),
    inflightWitnessPullByPeer: initTable[string, AntiEntropyPullWorkItem](),
    witnessPullRunners: initTable[string, Future[void]](),
    antiEntropySummaryRunners: initTable[string, Future[void]](),
    antiEntropySummaryNextRefreshAtMs: initTable[string, int64](),
    certificationObservedOverlayDigest: "",
    certificationObservedLatestCheckpointId: "",
    deferredAttestations: initTable[string, seq[EventAttestation]](),
    deferredEventCertificates: initTable[string, seq[EventCertificate]](),
    isolatedEvents: initOrderedTable[string, IsolationRecord](),
    children: initTable[string, seq[string]](),
    avoProposals: initOrderedTable[string, AvoProposal](),
    avoApprovals: initTable[string, seq[string]](),
    avoAdoptions: initOrderedTable[string, AvoAdoptionRecord](),
    checkpointCandidates: initOrderedTable[string, CheckpointCandidate](),
    checkpointVotes: initTable[string, seq[CheckpointVote]](),
    checkpoints: initOrderedTable[string, CheckpointCertificate](),
    checkpointSnapshots: initOrderedTable[string, ChainStateSnapshot](),
    liveState: newChainState(genesis),
    liveAppliedEvents: initHashSet[string](),
    latestCheckpointId: "",
    inbound: initDeque[InboundMessage](),
    inboundScheduled: false,
    inboundPending: false,
    inboundRunner: nil,
    slowInboundSliceCount: 0,
    maxInboundSliceElapsedMs: 0,
    connectedHandler: nil,
    syncingRoutingPeers: initHashSet[string](),
    routingSnapshotWantedGenerations: initTable[string, uint64](),
    routingSnapshotSyncedGenerations: initTable[string, uint64](),
    routingSnapshotNextRefreshAtMs: initTable[string, int64](),
    maintenanceScheduled: false,
    maintenanceImmediateScheduled: false,
    maintenanceGeneration: 0'u64,
    maintenancePending: false,
    maintenanceImmediatePending: false,
    maintenanceRunner: nil,
    slowMaintenanceSliceCount: 0,
    maxMaintenanceSliceElapsedMs: 0,
    checkpointAdvancePending: false,
    checkpointAdvanceRunner: nil,
    forwardedEvents: initTable[string, HashSet[string]](),
    forwardedAttestations: initTable[string, HashSet[string]](),
    forwardedEventCertificates: initTable[string, HashSet[string]](),
    forwardedCheckpointCandidates: initTable[string, HashSet[string]](),
    forwardedCheckpointVotes: initTable[string, HashSet[string]](),
    forwardedCheckpointBundles: initTable[string, HashSet[string]](),
    forwardedAvoProposals: initTable[string, HashSet[string]](),
    forwardedAvoApprovals: initTable[string, HashSet[string]](),
    pendingEvents: initHashSet[string](),
    pendingEventScopes: initTable[string, seq[LsmrPath]](),
    pendingAttestations: initHashSet[string](),
    routeBlockedAttestations: initHashSet[string](),
    pendingAttestationScopes: initTable[string, seq[LsmrPath]](),
    pendingEventCertificates: initHashSet[string](),
    pendingEventCertificateScopes: initTable[string, seq[LsmrPath]](),
    pendingEventCertificateFrontier: initHeapQueue[PendingEventCertificateTicket](),
    pendingEventCertificateEpochs: initTable[string, uint64](),
    pendingEventCertificateNextEpoch: 0'u64,
    pendingCheckpointCandidates: initHashSet[string](),
    pendingCheckpointVotes: initHashSet[string](),
    pendingCheckpointBundles: initHashSet[string](),
    eraCheckpointStates: initTable[uint64, EraCheckpointState](),
    pendingAvoProposals: initHashSet[string](),
    pendingAvoApprovals: initHashSet[string](),
    eventDeliveries: initTable[string, DeliveryLedger](),
    attestationDeliveries: initTable[string, DeliveryLedger](),
    eventCertificateDeliveries: initTable[string, DeliveryLedger](),
    checkpointCandidateDeliveries: initTable[string, DeliveryLedger](),
    checkpointVoteDeliveries: initTable[string, DeliveryLedger](),
    checkpointBundleDeliveries: initTable[string, DeliveryLedger](),
    checkpointBundleCache: initTable[string, CheckpointBundle](),
    eventMaintenanceCursor: 0,
    attestationMaintenanceCursor: 0,
    eventCertificateMaintenanceCursor: 0,
    checkpointCandidateMaintenanceCursor: 0,
    checkpointVoteMaintenanceCursor: 0,
    checkpointBundleMaintenanceCursor: 0,
    avoProposalMaintenanceCursor: 0,
    avoApprovalMaintenanceCursor: 0,
    lsmrOverlayDigest: "",
    lsmrSubmitReadyDigest: "",
    lsmrDeliveryDigest: "",
    lsmrDeliveryGeneration: 0'u64,
    lsmrObservedDeliveryGeneration: 0'u64,
    lsmrObservedLatestCheckpointId: "",
    lsmrDataPlaneEnabled: false,
    outboundSessions: initTable[string, OutboundPeerSession](),
    replayStateCache: initTable[uint64, ReplayStateCacheEntry](),
  )
  result.routingPeers[identity.account] = RoutingPeer(
    account: identity.account,
    peerId: peerIdString(identity.peerId),
    path: lsmrPath,
    addrs: @[],
  )

proc newFabricNode*(config: FabricNodeConfig): FabricNode =
  let identity = loadIdentity(config.identityPath)
  let genesis = loadGenesis(config.genesisPath)
  result = newFabricNode(identity, genesis, config.lsmrPath)
  result.config = config
  discard result.selfAnnouncement()
  if config.dataDir.len > 0:
    result.store = newFabricStore(config.dataDir / "fabric")
    result.restoreFromStore()
  let node = result
  if config.listenAddrs.len > 0:
    result.network = newFabricNetwork(
      identity = identity,
      listenAddrs = config.listenAddrs,
      bootstrapAddrs = config.bootstrapAddrs,
      legacyBootstrapAddrs = config.legacyBootstrapAddrs,
      lsmrBootstrapAddrs = config.lsmrBootstrapAddrs,
      routingMode = config.routingMode,
      primaryPlane = config.primaryPlane,
      lsmrConfig = config.lsmrConfig,
      fetchLookup = proc(key: string): Option[seq[byte]] {.gcsafe, raises: [].} =
        node.fetchLookupRaw(key),
      peerHandler = proc(network: FabricNetwork, item: PeerAnnouncement): Future[void] {.gcsafe, raises: [].} =
        proc handle() {.async, gcsafe.} =
          discard network
          node.enqueueInbound(InboundMessage(kind: imPeerAnnouncement, peerAnnouncement: item))
        handle(),
      eventHandler = proc(
          network: FabricNetwork, item: FabricEvent, scopePrefix: LsmrPath, sourcePeerId: string
      ): Future[void] {.gcsafe, raises: [].} =
        proc handle() {.async, gcsafe.} =
          discard network
          when defined(fabric_lsmr_diag):
            echo "fabric-node enqueue-event self=", node.identity.account,
              " event=", item.eventId,
              " scope=", $scopePrefix
          node.enqueueInbound(InboundMessage(
            kind: imEvent,
            relayScope: scopePrefix,
            sourcePeerId: sourcePeerId,
            event: item,
          ))
        handle(),
      attestationHandler = proc(
          network: FabricNetwork, item: EventAttestation, scopePrefix: LsmrPath, sourcePeerId: string
      ): Future[void] {.gcsafe, raises: [].} =
        proc handle() {.async, gcsafe.} =
          discard network
          node.enqueueInbound(InboundMessage(
            kind: imAttestation,
            relayScope: scopePrefix,
            sourcePeerId: sourcePeerId,
            attestation: item,
          ))
        handle(),
      eventCertificateHandler = proc(
          network: FabricNetwork, item: EventCertificate, scopePrefix: LsmrPath, sourcePeerId: string
      ): Future[void] {.gcsafe, raises: [].} =
        proc handle() {.async, gcsafe.} =
          discard network
          node.enqueueInbound(InboundMessage(
            kind: imEventCertificate,
            relayScope: scopePrefix,
            sourcePeerId: sourcePeerId,
            eventCertificate: item,
          ))
        handle(),
      checkpointCandidateHandler = proc(
          network: FabricNetwork, item: CheckpointCandidate, sourcePeerId: string
      ): Future[void] {.gcsafe, raises: [].} =
        proc handle() {.async, gcsafe.} =
          discard network
          node.enqueueInbound(InboundMessage(
            kind: imCheckpointCandidate,
            sourcePeerId: sourcePeerId,
            checkpointCandidate: item,
          ))
        handle(),
      checkpointVoteHandler = proc(
          network: FabricNetwork, item: CheckpointVote, sourcePeerId: string
      ): Future[void] {.gcsafe, raises: [].} =
        proc handle() {.async, gcsafe.} =
          discard network
          node.enqueueInbound(InboundMessage(
            kind: imCheckpointVote,
            sourcePeerId: sourcePeerId,
            checkpointVote: item,
          ))
        handle(),
      checkpointBundleHandler = proc(
          network: FabricNetwork, item: CheckpointBundle, sourcePeerId: string
      ): Future[void] {.gcsafe, raises: [].} =
        proc handle() {.async, gcsafe.} =
          discard network
          node.enqueueInbound(InboundMessage(
            kind: imCheckpointBundle,
            sourcePeerId: sourcePeerId,
            checkpointBundle: item,
          ))
        handle(),
      avoProposalHandler = proc(network: FabricNetwork, item: AvoProposal): Future[void] {.gcsafe, raises: [].} =
        proc handle() {.async, gcsafe.} =
          discard network
          node.enqueueInbound(InboundMessage(kind: imAvoProposal, avoProposal: item))
        handle(),
      avoApprovalHandler = proc(network: FabricNetwork, proposalId: string, validator: string): Future[void] {.gcsafe, raises: [].} =
        proc handle() {.async, gcsafe.} =
          discard network
          node.enqueueInbound(InboundMessage(
            kind: imAvoApproval,
            proposalId: proposalId,
            validator: validator,
          ))
        handle(),
    )

proc start*(node: FabricNode) {.async: (raises: [], gcsafe: false).} =
  if node.running:
    return
  node.running = true
  if not node.network.isNil:
    try:
      await node.network.start()
      node.network.switch.setTopologyChangedHook(proc() {.gcsafe, raises: [].} =
        if not node.isNil and node.running and node.usesLsmrPrimary():
          node.network.scheduleWarmSubmitConnections()
          node.scheduleReadyOutboundSessions()
          node.scheduleMaintenance(immediate = true)
      )
      node.network.setSubmitReadyHook(proc() {.gcsafe, raises: [].} =
        if not node.isNil and node.running and node.usesLsmrPrimary():
          node.scheduleReadyOutboundSessions()
          node.scheduleMaintenance(immediate = true)
      )
      node.connectedHandler = proc(
          peerId: PeerId, event: ConnEvent
      ): Future[void] {.gcsafe, async: (raises: [CancelledError]).} =
        if event.kind != ConnEventKind.Connected:
          return
        if peerId == node.identity.peerId:
          return
        node.clearRoutingSnapshotSync($peerId)
        try:
          await node.network.publishPeerAnnouncement(node.selfAnnouncement())
        except CatchableError:
          discard
      node.network.switch.addConnEventHandler(node.connectedHandler, ConnEventKind.Connected)
      for peer in await node.bootstrapRoutingSnapshot():
        node.enqueueInbound(InboundMessage(kind: imPeerAnnouncement, peerAnnouncement: peer))
      await node.network.publishPeerAnnouncement(node.selfAnnouncement())
      if node.usesLsmrPrimary():
        for peerIdText in node.connectedPeerIds():
          node.clearRoutingSnapshotSync(peerIdText)
        node.network.scheduleWarmSubmitConnections()
    except CatchableError:
      discard
  if node.usesLsmrPrimary():
    node.refreshPendingCertificationEvents()
    node.prunePendingCertificationState()
    node.scheduleCertificationGather()
  node.scheduleMaintenance(immediate = true)

proc stop*(node: FabricNode) {.async: (raises: [], gcsafe: false).} =
  node.running = false
  node.maintenanceScheduled = false
  node.maintenanceImmediateScheduled = false
  node.inboundPending = false
  node.certificationGatherPending = false
  node.maintenancePending = false
  node.maintenanceImmediatePending = false
  node.checkpointAdvancePending = false
  if not node.inboundRunner.isNil and not node.inboundRunner.finished():
    node.inboundRunner.cancelSoon()
  if not node.certificationGatherRunner.isNil and
      not node.certificationGatherRunner.finished():
    node.certificationGatherRunner.cancelSoon()
  if not node.maintenanceRunner.isNil and not node.maintenanceRunner.finished():
    node.maintenanceRunner.cancelSoon()
  if not node.checkpointAdvanceRunner.isNil and not node.checkpointAdvanceRunner.finished():
    node.checkpointAdvanceRunner.cancelSoon()
  for runner in node.witnessPullRunners.values:
    if not runner.isNil and not runner.finished():
      runner.cancelSoon()
  for runner in node.antiEntropySummaryRunners.values:
    if not runner.isNil and not runner.finished():
      runner.cancelSoon()
  if not node.connectedHandler.isNil and not node.network.isNil and not node.network.switch.isNil:
    node.network.switch.removeConnEventHandler(node.connectedHandler, ConnEventKind.Connected)
    node.connectedHandler = nil
  if not node.network.isNil and not node.network.switch.isNil:
    node.network.switch.setTopologyChangedHook(nil)
  if not node.network.isNil:
    node.network.setSubmitReadyHook(nil)
  if not node.network.isNil:
    try:
      await node.network.stop()
    except CatchableError:
      discard
  if not node.store.isNil:
    node.store.close()

proc routingPlaneStatus*(node: FabricNode): RoutingPlaneStatus =
  if node.isNil:
    return RoutingPlaneStatus()
  if not node.network.isNil:
    return node.network.routingPlaneStatus()
  RoutingPlaneStatus(
    mode: node.config.routingMode,
    primary: node.config.primaryPlane,
    shadowMode: node.config.routingMode == RoutingPlaneMode.dualStack,
  )

proc connectPeer*(node, peer: FabricNode) =
  if peer.identity.account == node.identity.account:
    return
  node.peers[peer.identity.account] = peer
  peer.peers[node.identity.account] = node
  node.routingPeers[peer.identity.account] = RoutingPeer(
    account: peer.identity.account,
    peerId: peerIdString(peer.identity.peerId),
    path: peer.config.lsmrPath,
    addrs: peer.selfRoutingPeer().addrs,
  )
  node.routingAnnouncements[peer.identity.account] = peer.selfAnnouncement()
  peer.routingPeers[node.identity.account] = RoutingPeer(
    account: node.identity.account,
    peerId: peerIdString(node.identity.peerId),
    path: node.config.lsmrPath,
    addrs: node.selfRoutingPeer().addrs,
  )
  peer.routingAnnouncements[node.identity.account] = node.selfAnnouncement()

proc restoreFromStore(node: FabricNode) =
  if node.store.isNil:
    return
  for proposal in node.store.loadAllAvoProposals():
    discard node.registerAvoProposal(proposal)
  for approval in node.store.loadAvoApprovals():
    discard node.registerAvoApproval(approval[0], approval[1])
  for adoption in node.store.loadAllAvoAdoptions():
    node.avoAdoptions[adoption.proposalId] = adoption
  for event in node.store.loadAllEvents():
    discard node.registerEvent(event, enqueuePending = false)
  for att in node.store.loadAllAttestations():
    discard node.registerAttestation(att, enqueuePending = false)
  for isolation in node.store.loadAllIsolations():
    node.isolatedEvents[isolation.eventId] = isolation
  let certs = node.store.loadAllEventCertificates().sorted(proc(a, b: EventCertificate): int =
    result = clockCmp(a.clock, b.clock)
    if result == 0:
      result = cmp(a.eventId, b.eventId)
  )
  for cert in certs:
    discard node.registerEventCertificate(cert, enqueuePending = false)
  for candidate in node.store.loadAllCheckpointCandidates():
    discard node.registerCandidate(candidate)
  for vote in node.store.loadAllCheckpointVotes():
    discard node.registerCheckpointVote(vote)
  for checkpoint in node.store.loadAllCheckpoints():
    let snapshot = node.store.loadCheckpointSnapshot(checkpoint.checkpointId)
    if snapshot.isSome():
      discard node.registerCheckpoint(checkpoint, snapshot.get())

proc checkpointStateSnapshot(node: FabricNode, checkpointId = ""): Option[ChainStateSnapshot] =
  let targetId =
    if checkpointId.len > 0: checkpointId
    else: node.latestCheckpointId
  if targetId.len == 0 or targetId notin node.checkpointSnapshots:
    return none(ChainStateSnapshot)
  some(node.checkpointSnapshots[targetId])

proc buildEvent(node: FabricNode, tx: Tx): FabricEvent =
  when defined(fabric_submit_diag):
    echo "build-stage descriptor sender=", tx.sender, " nonce=", tx.nonce
  let descriptor = node.liveState.descriptorFor(tx)
  let threadId = node.liveState.threadIdFor(tx)
  when defined(fabric_submit_diag):
    echo "build-stage parents sender=", tx.sender, " nonce=", tx.nonce
  var parents = frontierDigest(node.certifiedEvents()).certifiedEventIds
  let senderParent = node.latestLocalSenderParent(tx.sender, tx.nonce)
  if senderParent.isSome() and senderParent.get() notin parents:
    parents.add(senderParent.get())
  let localParent = node.latestLocalThreadParent(threadId, tx.sender, tx.nonce)
  if localParent.isSome() and localParent.get() notin parents:
    parents.add(localParent.get())
  parents.sort(system.cmp[string])
  var parentClock = GanzhiClock()
  for parentId in parents:
    if parentId in node.events:
      parentClock = maxClock(parentClock, node.events[parentId].clock)
  when defined(fabric_submit_diag):
    echo "build-stage participants sender=", tx.sender, " nonce=", tx.nonce
  var participantRoutes = @[node.selfRoutingPeer()]
  for participant in descriptor.participants:
    if participant == node.identity.account:
      continue
    let route = node.routingPeerForAccount(participant)
    if route.isSome():
      participantRoutes.add(route.get())
  participantRoutes = uniqueRoutingPeers(participantRoutes)
  let participantPaths = participantRoutes.mapIt(it.path)
  when defined(fabric_submit_diag):
    echo "build-stage witnesses-live sender=", tx.sender, " nonce=", tx.nonce
  var witnessRoutes: seq[RoutingPeer] = @[]
  for account, info in node.liveState.validators.pairs:
    if account in descriptor.participants or not info.active or info.jailed:
      continue
    let route = node.routingPeerForAccount(account)
    if route.isNone():
      continue
    for participantPath in participantPaths:
        if isNeighbor(route.get().path, participantPath):
          witnessRoutes.add(route.get())
          break
  if not node.usesLsmrPrimary():
    when defined(fabric_submit_diag):
      echo "build-stage witnesses-routing sender=", tx.sender, " nonce=", tx.nonce
    for account, routingPeer in node.routingPeers.pairs:
      if account in descriptor.participants:
        continue
      if witnessRoutes.anyIt(
        (it.peerId.len > 0 and it.peerId == routingPeer.peerId) or
        (it.peerId.len == 0 and it.account == routingPeer.account)
      ):
        continue
      for participantPath in participantPaths:
          if isNeighbor(routingPeer.path, participantPath):
            witnessRoutes.add(routingPeer)
            break
    when defined(fabric_submit_diag):
      echo "build-stage witnesses-active sender=", tx.sender, " nonce=", tx.nonce
    if not node.network.isNil and not node.network.switch.isNil:
      for peerId, record in node.network.switch.peerStore[ActiveLsmrBook].book.pairs:
        let peerIdText = $peerId
        let route = node.routingPeerForPeerId(peerIdText)
        if route.isNone():
          continue
        if route.get().account == node.identity.account or route.get().account in descriptor.participants:
          continue
        if witnessRoutes.anyIt(
          (it.peerId.len > 0 and it.peerId == route.get().peerId) or
          (it.peerId.len == 0 and route.get().account.len > 0 and it.account == route.get().account)
        ):
          continue
        for participantPath in participantPaths:
          if isNeighbor(route.get().path, participantPath):
            witnessRoutes.add(route.get())
            break
  witnessRoutes = uniqueRoutingPeers(witnessRoutes)
  let witnessSet = witnessRoutes.mapIt(it.account).filterIt(it.len > 0).sortedUnique()
  when defined(fabric_submit_diag):
    echo "build-stage finalize sender=", tx.sender, " nonce=", tx.nonce,
      " parents=", parents.len,
      " participants=", participantRoutes.len,
      " witnesses=", witnessRoutes.len
  result = FabricEvent(
    eventId: "",
    threadId: threadId,
    author: tx.sender,
    authorPublicKey: tx.senderPublicKey,
    authorPeerId: peerIdString(node.identity.peerId),
    contractRoot: descriptor.contractRoot,
    entrypoint: descriptor.entrypoint,
    args: tx.payload,
    legacyTx: tx,
    participants: descriptor.participants,
    participantRoutes: participantRoutes,
    witnessRoutes: witnessRoutes,
    witnessSet: witnessSet,
    parents: parents,
    clock: if parents.len == 0: GanzhiClock(era: 0, tick: 0) else: nextClock(parentClock),
    routingPath: node.config.lsmrPath,
    createdAt: tx.timestamp,
  )
  result.eventId = computeEventId(result)

proc authorEventHasPendingWitnessPull(
    node: FabricNode, eventId: string
): bool {.gcsafe, raises: [].} =
  if node.isNil or eventId.len == 0:
    return false
  for queue in node.pendingWitnessPullsByPeer.values:
    for item in queue:
      if item.causedByEventId == eventId:
        return true
  for item in node.inflightWitnessPullByPeer.values:
    if item.causedByEventId == eventId:
      return true
  false

proc localAuthorEventSettled(
    node: FabricNode, eventId: string
): bool {.gcsafe, raises: [].} =
  if node.isNil or eventId.len == 0 or eventId notin node.events:
    return true
  if eventId in node.isolatedEvents:
    return true
  if eventId in node.pendingCertificationEvents or
      node.authorEventHasPendingWitnessPull(eventId):
    return false
  if eventId notin node.eventCertificates:
    return false
  true

proc submitTx*(node: FabricNode, tx: Tx): SubmitEventResult =
  when defined(fabric_submit_diag):
    echo "submit-stage start sender=", tx.sender, " nonce=", tx.nonce
  if node.usesLsmrPrimary():
    node.enableLsmrDataPlane()
  if node.usesLsmrPrimary():
    if node.inbound.len > 0:
      node.scheduleInboundDrain()
  else:
    discard node.drainInbound()
  when defined(fabric_submit_diag):
    echo "submit-stage drained sender=", tx.sender, " nonce=", tx.nonce
  let event = node.buildEvent(tx)
  result.eventId = event.eventId
  result.accepted = node.registerEvent(event)
  when defined(fabric_submit_diag):
    echo "submit-stage registered sender=", tx.sender, " nonce=", tx.nonce,
      " accepted=", result.accepted, " event=", result.eventId
  if result.accepted:
    if node.usesLsmrPrimary():
      node.advanceLocalAcceptedEvent(event.eventId)
      when defined(fabric_submit_diag):
        echo "submit-stage converged sender=", tx.sender, " nonce=", tx.nonce,
          " mode=local"
  else:
    node.converge()
    when defined(fabric_submit_diag):
      echo "submit-stage converged sender=", tx.sender, " nonce=", tx.nonce
  if node.usesLsmrPrimary():
    # `submitTx()` is synchronous but it schedules real network/inbound work.
    # Pump multiple turns so `callSoon` outbound launch, batch ack resume,
    # and co-located inbound/maintenance work can all make progress.
    var pumped = 0
    while pumped < FabricSubmitPumpPollLimit and node.hasPendingLocalAsyncWork():
      poll()
      inc pumped
    if result.accepted:
      while not node.localAuthorEventSettled(event.eventId):
        doAssert node.hasPendingLocalAsyncWork(),
          "author event stalled before settle: " & event.eventId
        poll()
  result.certified = event.eventId in node.eventCertificates
  for checkpointId, checkpoint in node.checkpoints.pairs:
    if checkpoint.candidate.era >= event.clock.era:
      result.checkpointIds.add(checkpointId)

proc fabricStatus*(node: FabricNode): FabricStatusSnapshot =
  when defined(fabric_diag):
    echo "status-start ", node.identity.account, " inbound=", len(node.inbound)
  discard node.drainInbound()
  when defined(fabric_diag):
    echo "status-drained ", node.identity.account, " inbound=", len(node.inbound)
  let frontier = frontierDigest(node.certifiedEvents())
  when defined(fabric_diag):
    echo "status-frontier ", node.identity.account, " certs=", node.eventCertificates.len
  FabricStatusSnapshot(
    chainId: node.genesis.params.chainId,
    localAccount: node.identity.account,
    localPeerId: peerIdString(node.identity.peerId),
    lsmrPath: node.config.lsmrPath,
    routingStatus: node.routingPlaneStatus(),
    eventCount: node.events.len,
    certifiedEventCount: node.eventCertificates.len,
    checkpointCount: node.checkpoints.len,
    latestCheckpointId: node.latestCheckpointId,
    latestCheckpointEra:
      if node.latestCheckpointId.len > 0 and node.latestCheckpointId in node.checkpoints:
        node.checkpoints[node.latestCheckpointId].candidate.era
      else:
        0,
    latestFrontierDigest: frontier.frontierDigest,
    activeContractRoots: allSystemContractRoots(),
    activeAvoSet:
      if node.latestCheckpointId.len > 0 and node.latestCheckpointId in node.checkpoints:
        node.activeAvoSetForEra(node.checkpoints[node.latestCheckpointId].candidate.era + 1)
      else:
        @[],
  )

proc slowInboundSlices*(node: FabricNode): int64 {.gcsafe, raises: [].} =
  if node.isNil:
    return -1
  node.slowInboundSliceCount

proc maxInboundSliceMs*(node: FabricNode): int64 {.gcsafe, raises: [].} =
  if node.isNil:
    return -1
  node.maxInboundSliceElapsedMs

proc slowMaintenanceSlices*(node: FabricNode): int64 {.gcsafe, raises: [].} =
  if node.isNil:
    return -1
  node.slowMaintenanceSliceCount

proc maxMaintenanceSliceMs*(node: FabricNode): int64 {.gcsafe, raises: [].} =
  if node.isNil:
    return -1
  node.maxMaintenanceSliceElapsedMs

proc getEvent*(node: FabricNode, eventId: string): Option[FabricEvent] =
  discard node.drainInbound()
  node.eventById(eventId)

proc getEventCertificate*(node: FabricNode, eventId: string): Option[EventCertificate] =
  discard node.drainInbound()
  if node.eventCertificates.hasKey(eventId):
    return some(node.eventCertificates[eventId])
  none(EventCertificate)

proc getCheckpoint*(node: FabricNode, checkpointId = ""): Option[CheckpointCertificate] =
  discard node.drainInbound()
  let targetId = if checkpointId.len > 0: checkpointId else: node.latestCheckpointId
  node.checkpointById(targetId)

proc snapshotState(node: FabricNode, checkpointId = ""): Option[ChainState] =
  node.checkpointState(checkpointId)

proc publishTasks*(node: FabricNode, checkpointId = ""): seq[PublishTaskSnapshot] =
  discard node.drainInbound()
  let state = node.snapshotState(checkpointId)
  if state.isNone():
    return @[]
  for _, record in state.get().contents.pairs:
    result.add(record.publishTaskSnapshot())
  result.sort(proc(a, b: PublishTaskSnapshot): int = cmp(b.updatedAt, a.updatedAt))

proc feedSnapshot*(node: FabricNode, limit = 50, checkpointId = ""): seq[SocialFeedItemSnapshot] =
  discard node.drainInbound()
  let state = node.snapshotState(checkpointId)
  if state.isSome():
    return state.get().socialFeedSnapshot(limit)
  @[]

proc contentDetail*(node: FabricNode, contentId: string, checkpointId = ""): Option[SocialContentDetailSnapshot] =
  discard node.drainInbound()
  let state = node.snapshotState(checkpointId)
  if state.isNone():
    return none(SocialContentDetailSnapshot)
  if contentId notin state.get().contents:
    return none(SocialContentDetailSnapshot)
  some(state.get().socialContentDetailSnapshot(contentId))

proc reportList*(node: FabricNode, contentId = "", checkpointId = ""): seq[ReportTicket] =
  discard node.drainInbound()
  let state = node.snapshotState(checkpointId)
  if state.isSome():
    return state.get().reportList(contentId)
  @[]

proc creditBalance*(node: FabricNode, owner: string, checkpointId = ""): Option[CreditBalanceSnapshot] =
  discard node.drainInbound()
  let state = node.snapshotState(checkpointId)
  if state.isNone():
    return none(CreditBalanceSnapshot)
  some(state.get().creditBalanceSnapshot(owner))

proc mintIntentStatus*(node: FabricNode, mintIntentId: string, checkpointId = ""): Option[MintIntentStatus] =
  discard node.drainInbound()
  let state = node.snapshotState(checkpointId)
  if state.isNone() or mintIntentId notin state.get().mintIntents:
    return none(MintIntentStatus)
  some(state.get().mintIntentStatusSnapshot(mintIntentId))

proc resolveName*(node: FabricNode, name: string, nowTs: int64, checkpointId = ""): Option[NameRecordSnapshot] =
  discard node.drainInbound()
  let state = node.snapshotState(checkpointId)
  if state.isNone():
    return none(NameRecordSnapshot)
  state.get().resolveNameSnapshot(name, nowTs)

proc queryProjection*(node: FabricNode, projection: string, checkpointId = ""): JsonNode =
  discard node.drainInbound()
  let snapshot = node.checkpointStateSnapshot(checkpointId)
  if snapshot.isNone():
    return newJNull()
  let snap = snapshot.get()
  case projection.toLowerAscii()
  of "state":
    toJson(snap)
  of "ledger":
    %*{
      "params": snap.params,
      "totalRwadSupply": snap.totalRwadSupply,
      "nonces": snap.nonces,
      "rwadBalances": snap.rwadBalances,
    }
  of "content":
    %*{"contents": snap.contents}
  of "governance":
    %*{
      "reports": snap.reports,
      "moderationCases": snap.moderationCases,
      "tombstones": snap.tombstones,
    }
  of "credit":
    %*{"creditPositions": snap.creditPositions}
  of "treasury":
    %*{
      "mintIntents": snap.mintIntents,
      "executionReceipts": snap.executionReceipts,
      "custodySnapshots": snap.custodySnapshots,
      "reserveProofs": snap.reserveProofs,
    }
  of "name":
    %*{"nameRecords": snap.nameRecords}
  of "staking":
    %*{
      "validators": snap.validators,
      "delegations": snap.delegations,
    }
  else:
    newJNull()

proc getContractRoot*(kind: TxKind): tuple[contractRoot: string, entrypoint: string] =
  (contractRootFor(kind), entrypointFor(kind))

proc listAvoProposals*(node: FabricNode): seq[AvoProposal] =
  discard node.drainInbound()
  for _, proposal in node.avoProposals.pairs:
    result.add(proposal)
  result.sort(proc(a, b: AvoProposal): int = cmp(a.createdAt, b.createdAt))

proc submitAvoProposal*(
    node: FabricNode,
    targetContractRoot: string,
    targetEntrypoint: string,
    triggerSummary: string,
    rewriteSummary: string,
    proofBundleHash: string,
    checkpointStartEra: uint64,
    checkpointEndEra: uint64,
): AvoProposal =
  discard node.drainInbound()
  result = AvoProposal(
    proposalId: "",
    bundle: AvoRuleBundle(
      bundleId: hashHex(targetContractRoot & ":" & targetEntrypoint & ":" & proofBundleHash),
      targetContractRoot: targetContractRoot,
      targetEntrypoint: targetEntrypoint,
      triggerSummary: triggerSummary,
      rewriteSummary: rewriteSummary,
      proofBundleHash: proofBundleHash,
    ),
    checkpointStartEra: checkpointStartEra,
    checkpointEndEra: checkpointEndEra,
    proposer: node.identity.account,
    createdAt: int64(node.events.len + node.checkpoints.len + 1),
  )
  result.proposalId = computeAvoProposalId(result)
  discard node.registerAvoProposal(result)

proc approveAvoProposal*(node: FabricNode, proposalId: string): bool =
  discard node.drainInbound()
  if proposalId notin node.avoProposals:
    return false
  if node.identity.account in node.avoApprovals.getOrDefault(proposalId, @[]):
    return false
  discard node.registerAvoApproval(proposalId, node.identity.account)
  node.converge()
  true
