import std/[algorithm, deques, json, jsonutils, options, os, sequtils, sets, strutils, tables]

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

type
  InboundMessageKind = enum
    imPeerAnnouncement
    imEvent
    imAttestation
    imEventCertificate
    imCheckpointCandidate
    imCheckpointVote
    imCheckpointBundle
    imAvoProposal
    imAvoApproval

  InboundMessage = object
    case kind: InboundMessageKind
    of imPeerAnnouncement:
      peerAnnouncement: PeerAnnouncement
    of imEvent:
      event: FabricEvent
    of imAttestation:
      attestation: EventAttestation
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

  FabricNode* = ref object
    config*: FabricNodeConfig
    identity*: NodeIdentity
    genesis*: GenesisSpec
    peers*: OrderedTable[string, FabricNode]
    routingPeers*: OrderedTable[string, RoutingPeer]
    store*: FabricStore
    network*: FabricNetwork
    running*: bool
    events*: OrderedTable[string, FabricEvent]
    eventAttestations*: Table[string, seq[EventAttestation]]
    eventCertificates*: OrderedTable[string, EventCertificate]
    isolatedEvents*: OrderedTable[string, IsolationRecord]
    children*: Table[string, seq[string]]
    avoProposals*: OrderedTable[string, AvoProposal]
    avoApprovals*: Table[string, seq[string]]
    avoAdoptions*: OrderedTable[string, AvoAdoptionRecord]
    checkpointCandidates*: OrderedTable[string, CheckpointCandidate]
    checkpointVotes*: Table[string, seq[CheckpointVote]]
    checkpoints*: OrderedTable[string, CheckpointCertificate]
    checkpointSnapshots*: OrderedTable[string, ChainStateSnapshot]
    liveState*: ChainState
    liveAppliedEvents*: HashSet[string]
    latestCheckpointId*: string
    inbound*: Deque[InboundMessage]
    inboundScheduled*: bool
    connectedHandler*: ConnEventHandler
    syncingRoutingPeers*: HashSet[string]
    maintenanceScheduled*: bool

proc restoreFromStore(node: FabricNode)
proc selfAnnouncement(node: FabricNode): PeerAnnouncement
proc knownPeerAnnouncements(node: FabricNode): seq[PeerAnnouncement]
proc fetchLookupRaw(node: FabricNode, key: string): Option[seq[byte]] {.gcsafe, raises: [].}
proc enqueueInbound(node: FabricNode, message: sink InboundMessage) {.gcsafe, raises: [].}
proc scheduleInboundDrain(node: FabricNode) {.gcsafe, raises: [].}
proc drainInbound(node: FabricNode)
proc bootstrapRoutingSnapshot(node: FabricNode): Future[seq[PeerAnnouncement]] {.async: (raises: []).}
proc syncRoutingSnapshotTask(node: FabricNode, peerIdText: string): Future[void] {.async: (raises: []).}
proc ensureRoutingSnapshotSync(node: FabricNode, peerIdText: string)
proc maintenanceStep(node: FabricNode)
proc scheduleMaintenance(node: FabricNode) {.gcsafe, raises: [].}

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

proc bytesOf(value: string): seq[byte] =
  result = newSeq[byte](value.len)
  if value.len > 0:
    copyMem(addr result[0], unsafeAddr value[0], value.len)

proc stringOf(data: openArray[byte]): string =
  result = newString(data.len)
  if data.len > 0:
    copyMem(addr result[0], unsafeAddr data[0], data.len)

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
  RoutingPeer(
    account: node.identity.account,
    peerId: peerIdString(node.identity.peerId),
    path: node.config.lsmrPath,
  )

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
  ))

proc routingPeerForAccount(node: FabricNode, account: string): Option[RoutingPeer] =
  if account.len == 0:
    return none(RoutingPeer)
  if account == node.identity.account:
    return some(node.selfRoutingPeer())
  if account in node.routingPeers:
    let route = node.routingPeers[account]
    if route.peerId.len > 0 and route.path.len > 0:
      return some(route)
  if node.liveState.validators.hasKey(account):
    let info = node.liveState.validators[account]
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
      if route.path.len > 0:
        return some(route)
      return node.routeFromActiveCertificate(account, peerIdText)
  for account, info in node.liveState.validators.pairs:
    if info.peerId == peerIdText:
      return node.routeFromActiveCertificate(account, peerIdText)
  node.routeFromActiveCertificate("", peerIdText)

proc routeMatchesSelf(node: FabricNode, route: RoutingPeer): bool =
  route.peerId == peerIdString(node.identity.peerId) or
    (route.account.len > 0 and route.account == node.identity.account)

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

proc eventWitnessRoutes(event: FabricEvent): seq[RoutingPeer] =
  if event.witnessRoutes.len > 0:
    return uniqueRoutingPeers(event.witnessRoutes)
  for witness in event.witnessSet:
    result.add(RoutingPeer(account: witness, peerId: "", path: @[]))

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

proc certifiedEvents(node: FabricNode, eraLimit = high(uint64)): seq[FabricEvent] =
  for eventId in node.certifiedEventIds():
    let event = node.events[eventId]
    if event.clock.era <= eraLimit:
      result.add(event)
  result.sort(eventOrder)

proc replayState(node: FabricNode, eraLimit = high(uint64)): ChainState =
  result = newChainState(node.genesis)
  let ordered = node.certifiedEvents(eraLimit)
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

proc applyPendingLiveEvents(node: FabricNode) =
  let ordered = node.certifiedEvents()
  for event in ordered:
    if event.eventId in node.liveAppliedEvents:
      continue
    when defined(fabric_diag):
      echo "live-apply-start ", node.identity.account, " event=", event.eventId
    var parentsReady = true
    for parent in event.parents:
      if parent in node.eventCertificates and parent notin node.liveAppliedEvents:
        parentsReady = false
        break
    if not parentsReady:
      raise newException(ValueError, "live state received out-of-order certified event")
    discard node.liveState.applyEventToState(event)
    node.liveAppliedEvents.incl(event.eventId)
    when defined(fabric_diag):
      echo "live-apply-done ", node.identity.account, " event=", event.eventId

proc validatorWeight(state: ChainState, address: string): uint64 =
  if state.validators.hasKey(address):
    return state.validators[address].validatorVotingPower()
  0

proc eventAttestationWeight(node: FabricNode): uint64 =
  if node.liveState.validators.hasKey(node.identity.account):
    return node.liveState.validators[node.identity.account].validatorVotingPower()
  1

proc attestationBy(node: FabricNode, eventId, signer: string, role: AttestationRole): bool =
  for att in node.eventAttestations.getOrDefault(eventId, @[]):
    if att.signer == signer and att.role == role:
      return true
  false

proc localAttestation(node: FabricNode, eventId, signer: string, role: AttestationRole): Option[EventAttestation] =
  for att in node.eventAttestations.getOrDefault(eventId, @[]):
    if att.signer == signer and att.role == role:
      return some(att)
  none(EventAttestation)

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

proc registerAttestation(node: FabricNode, att: EventAttestation): bool =
  if att.eventId notin node.events:
    return false
  if not verifyAttestation(att):
    return false
  if node.attestationBy(att.eventId, att.signer, att.role):
    return false
  var existing = node.eventAttestations.getOrDefault(att.eventId, @[])
  existing.add(att)
  node.eventAttestations[att.eventId] = existing
  if not node.store.isNil:
    node.store.persistAttestation(att)
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
  if node.latestCheckpointId.len == 0 or
      node.checkpoints[node.latestCheckpointId].candidate.era < cert.candidate.era:
    node.latestCheckpointId = cert.checkpointId
  if not node.store.isNil:
    node.store.persistCheckpoint(cert, snapshot)
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
  true

proc registerCheckpointFetched(
  node: FabricNode,
  cert: CheckpointCertificate,
  snapshot: ChainStateSnapshot,
  checkpointRaw, snapshotRaw: seq[byte],
): bool {.gcsafe.} =
  if node.checkpoints.hasKey(cert.checkpointId):
    return false
  node.checkpoints[cert.checkpointId] = cert
  node.checkpointSnapshots[cert.checkpointId] = snapshot
  if node.latestCheckpointId.len == 0 or
      node.checkpoints[node.latestCheckpointId].candidate.era < cert.candidate.era:
    node.latestCheckpointId = cert.checkpointId
  if not node.store.isNil:
    node.store.persistCheckpointRaw(cert, checkpointRaw, snapshotRaw)
  for proposalId in cert.candidate.adoptedProposalIds:
    if not node.avoAdoptions.hasKey(proposalId):
      node.avoAdoptions[proposalId] = AvoAdoptionRecord(
        proposalId: proposalId,
        adoptedInCheckpointId: cert.checkpointId,
        activatesAtEra: cert.candidate.era + 1,
        adoptedAt: cert.createdAt,
      )
  true

proc registerAvoProposal(node: FabricNode, proposal: AvoProposal): bool =
  if node.avoProposals.hasKey(proposal.proposalId):
    return false
  node.avoProposals[proposal.proposalId] = proposal
  if not node.store.isNil:
    node.store.persistAvoProposal(proposal)
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

proc hasCheckpointForCandidate(node: FabricNode, candidateId: string): bool =
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
  true

proc registerEvent(node: FabricNode, event: FabricEvent): bool =
  if node.events.hasKey(event.eventId) or node.isolatedEvents.hasKey(event.eventId):
    return false
  let isolation = node.validateEventInvariants(event)
  if isolation.isSome():
    discard node.registerIsolation(isolation.get())
    return false
  node.events[event.eventId] = event
  if not node.store.isNil:
    node.store.persistEvent(event)
  for parent in event.parents:
    var existing = node.children.getOrDefault(parent, @[])
    if event.eventId notin existing:
      existing.add(event.eventId)
    node.children[parent] = existing
  true

proc registerEventCertificate(node: FabricNode, cert: EventCertificate): bool =
  when defined(fabric_diag):
    echo "register-cert-start ", node.identity.account, " event=", cert.eventId
  if cert.eventId notin node.events or node.eventCertificates.hasKey(cert.eventId):
    when defined(fabric_diag):
      echo "register-cert-skip ", node.identity.account, " event=", cert.eventId
    return false
  for parent in node.events[cert.eventId].parents:
    if parent in node.events and parent notin node.eventCertificates:
      when defined(fabric_diag):
        echo "register-cert-parent-missing ", node.identity.account, " event=", cert.eventId, " parent=", parent
      return false
  node.eventCertificates[cert.eventId] = cert
  when defined(fabric_checkpoint_diag):
    echo "event-cert ", node.identity.account,
      " total=", node.eventCertificates.len,
      " era=", cert.clock.era,
      " tick=", cert.clock.tick,
      " event=", cert.eventId
  when defined(fabric_diag):
    echo "register-cert-before-live ", node.identity.account, " event=", cert.eventId
  node.applyPendingLiveEvents()
  when defined(fabric_diag):
    echo "register-cert-after-live ", node.identity.account, " event=", cert.eventId
  if not node.store.isNil:
    node.store.persistEventCertificate(cert)
  true

proc eventParticipantSatisfied(node: FabricNode, event: FabricEvent): bool =
  for participant in event.participants:
    var found = false
    for att in node.eventAttestations.getOrDefault(event.eventId, @[]):
      if att.role == arParticipant and att.signer == participant:
        found = true
        break
    if not found:
      return false
  true

proc witnessCount(node: FabricNode, event: FabricEvent, component: seq[FabricNode]): tuple[reachable: int, threshold: int, signed: int, witnessAtts: seq[EventAttestation]] =
  discard component
  let witnessRoutes = event.eventWitnessRoutes()
  let witnessPeerIds = event.eventWitnessPeerIds()
  let witnessAccounts = event.eventWitnessAccounts()
  result.reachable =
    if witnessRoutes.len > 0:
      witnessRoutes.len
    else:
      witnessAccounts.len
  result.threshold = max(1, (result.reachable + 1) div 2)
  for att in node.eventAttestations.getOrDefault(event.eventId, @[]):
    if att.role != arWitness:
      continue
    let matchesWitness =
      (att.signerPeerId.len > 0 and att.signerPeerId in witnessPeerIds) or
      att.signer in witnessAccounts
    if matchesWitness and
        result.witnessAtts.allIt(
          (it.signerPeerId.len > 0 and it.signerPeerId != att.signerPeerId) or
          (it.signerPeerId.len == 0 and it.signer != att.signer)
        ):
      inc result.signed
      result.witnessAtts.add(att)

proc maybeCertifyEvent(node: FabricNode, event: FabricEvent, component: seq[FabricNode]): bool =
  when defined(fabric_diag):
    echo "maybe-cert-start ", node.identity.account, " event=", event.eventId
  if event.eventId in node.eventCertificates:
    when defined(fabric_diag):
      echo "maybe-cert-skip-known ", node.identity.account, " event=", event.eventId
    return false
  if not node.eventParticipantSatisfied(event):
    when defined(fabric_diag):
      echo "maybe-cert-missing-participant ", node.identity.account, " event=", event.eventId
    return false
  let witnessInfo = node.witnessCount(event, component)
  when defined(fabric_diag):
    echo "maybe-cert-witness ", node.identity.account, " event=", event.eventId,
      " signed=", witnessInfo.signed, " threshold=", witnessInfo.threshold
  if witnessInfo.signed < witnessInfo.threshold:
    when defined(fabric_diag):
      echo "maybe-cert-missing-witness ", node.identity.account, " event=", event.eventId
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
  node.registerEventCertificate(normalized)

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

proc shareCheckpoint(nodes: seq[FabricNode], cert: CheckpointCertificate, snapshot: ChainStateSnapshot): bool =
  var changed = false
  for peer in nodes:
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

proc allRoutingPeers(node: FabricNode): seq[RoutingPeer] =
  for _, route in node.routingPeers.pairs:
    if route.account.len > 0 and route.peerId.len > 0 and route.account notin result.mapIt(it.account):
      result.add(route)

proc validatorPeers(node: FabricNode, stateAtEra: ChainState): seq[RoutingPeer] =
  for address, info in stateAtEra.validators.pairs:
    if not info.active or info.jailed:
      continue
    if address in node.routingPeers:
      let route = node.routingPeers[address]
      if route.account.len > 0 and route.peerId.len > 0 and route.account notin result.mapIt(it.account):
        result.add(route)

proc disseminateEvent(node: FabricNode, event: FabricEvent) =
  let component = node.componentNodes()
  let scope = node.routeScope(event, component)
  for peer in scope:
    if peer.identity.account != node.identity.account:
      discard peer.registerEvent(event)
  if not node.network.isNil and node.running:
    node.publishFuture(node.network.publishEvent(event))
    for route in node.routePeers(event):
      if route.account != node.identity.account:
        node.submitFuture(node.network.submitEvent(route.peerId, event))

proc disseminateAttestation(node: FabricNode, event: FabricEvent, att: EventAttestation) =
  let component = node.componentNodes()
  let scope = node.routeScope(event, component)
  discard shareAttestation(scope, event, att)
  if not node.network.isNil and node.running:
    node.publishFuture(node.network.publishAttestation(att))
    for route in node.routePeers(event):
      if route.account != node.identity.account:
        node.submitFuture(node.network.submitAttestation(route.peerId, att))

proc disseminateEventCertificate(node: FabricNode, event: FabricEvent, cert: EventCertificate) =
  let component = node.componentNodes()
  let scope = node.routeScope(event, component)
  for peer in scope:
    if peer.identity.account != node.identity.account:
      discard peer.registerEventCertificate(cert)
  if not node.network.isNil and node.running:
    node.publishFuture(node.network.publishEventCertificate(cert))
    for route in node.routePeers(event):
      if route.account != node.identity.account:
        node.submitFuture(node.network.submitEventCertificate(route.peerId, cert))

proc disseminateCheckpointCandidate(node: FabricNode, candidate: CheckpointCandidate) =
  discard shareCandidate(node.componentNodes(), candidate)
  if not node.network.isNil and node.running:
    node.publishFuture(node.network.publishCheckpointCandidate(candidate))
    let stateAtEra = node.replayState(candidate.era)
    for route in node.validatorPeers(stateAtEra):
      if route.account != node.identity.account:
        node.submitFuture(node.network.submitCheckpointCandidate(route.peerId, candidate))

proc disseminateCheckpointVote(node: FabricNode, vote: CheckpointVote) =
  discard shareCheckpointVote(node.componentNodes(), vote)
  if not node.network.isNil and node.running:
    node.publishFuture(node.network.publishCheckpointVote(vote))
    let stateAtEra = node.replayState(node.checkpointCandidates[vote.candidateId].era)
    for route in node.validatorPeers(stateAtEra):
      if route.account != node.identity.account:
        node.submitFuture(node.network.submitCheckpointVote(route.peerId, vote))

proc disseminateCheckpoint(node: FabricNode, cert: CheckpointCertificate, snapshot: ChainStateSnapshot) =
  discard shareCheckpoint(node.componentNodes(), cert, snapshot)
  if not node.network.isNil and node.running:
    node.publishFuture(node.network.publishCheckpointCertificate(cert, snapshot))
    let bundle = CheckpointBundle(
      certificate: cert,
      snapshot: encodePolarSnapshot(snapshot),
    )
    for route in node.allRoutingPeers():
      if route.account != node.identity.account:
        node.submitFuture(node.network.submitCheckpointBundle(route.peerId, bundle))

proc disseminateAvoProposal(node: FabricNode, proposal: AvoProposal) =
  discard shareAvoProposal(node.componentNodes(), proposal)
  if not node.network.isNil and node.running:
    node.publishFuture(node.network.publishAvoProposal(proposal))

proc disseminateAvoApproval(node: FabricNode, proposalId, validator: string) =
  discard shareAvoApproval(node.componentNodes(), proposalId, validator)
  if not node.network.isNil and node.running:
    node.publishFuture(node.network.publishAvoApproval(proposalId, validator))

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

proc ensureEraCandidates(node: FabricNode, component: seq[FabricNode]): bool =
  var changed = false
  let certs = node.certifiedEvents()
  if certs.len == 0:
    return false
  let maxEra = certs[^1].clock.era
  if maxEra == 0:
    return false
  for era in 0'u64 ..< maxEra:
    var exists = false
    for _, checkpoint in node.checkpoints.pairs:
      if checkpoint.candidate.era == era:
        exists = true
        break
    if exists:
      continue
    var candidateExists = false
    for _, candidate in node.checkpointCandidates.pairs:
      if candidate.era == era:
        candidateExists = true
        break
    if candidateExists:
      continue
    let candidate = node.buildCheckpointCandidate(era)
    when defined(fabric_checkpoint_diag):
      echo "checkpoint-build ", node.identity.account,
        " era=", era,
        " maxEra=", maxEra,
        " frontier=", candidate.frontierDigest
    discard component
    node.disseminateCheckpointCandidate(candidate)
    changed = true
  changed

proc advanceNode(node: FabricNode, component: seq[FabricNode]): bool =
  var changed = false
  for _, event in node.events.pairs:
    let scope = node.routeScope(event, component)
    let role =
      if node.identity.account in event.participants: some(arParticipant)
      elif event.eventWitnessRoutes().anyIt(node.routeMatchesSelf(it)): some(arWitness)
      else: none(AttestationRole)
    if node.identity.account == event.author and event.eventId notin node.eventCertificates:
      node.disseminateEvent(event)
    if role.isSome():
      let existing = node.localAttestation(event.eventId, node.identity.account, role.get())
      if existing.isNone():
        let att = node.makeAttestation(event, role.get())
        when defined(fabric_diag):
          echo "advance-att ", node.identity.account, " event=", event.eventId, " role=", ord(role.get())
        node.disseminateAttestation(event, att)
        changed = true
      elif event.eventId notin node.eventCertificates:
        node.disseminateAttestation(event, existing.get())
    discard scope
  for _, event in node.events.pairs:
    let scope = node.routeScope(event, component)
    if node.maybeCertifyEvent(event, scope):
      when defined(fabric_diag):
        echo "advance-cert ", node.identity.account, " event=", event.eventId
      let cert = node.eventCertificates[event.eventId]
      node.disseminateEventCertificate(event, cert)
      changed = true
    elif event.eventId in node.eventCertificates:
      node.disseminateEventCertificate(event, node.eventCertificates[event.eventId])
  changed = node.ensureEraCandidates(component) or changed
  for _, candidate in node.checkpointCandidates.pairs:
    if node.hasCheckpointForCandidate(candidate.candidateId):
      continue
    let stateAtEra = node.replayState(candidate.era)
    if stateAtEra.validators.hasKey(node.identity.account) and
        stateAtEra.validators[node.identity.account].active and
        not stateAtEra.validators[node.identity.account].jailed and
        not node.voteBy(candidate.candidateId, node.identity.account):
      let vote = node.makeCheckpointVote(candidate, stateAtEra)
      node.disseminateCheckpointVote(vote)
      changed = true
    let checkpoint = node.maybeCreateCheckpoint(candidate)
    if checkpoint.isSome():
      node.disseminateCheckpoint(checkpoint.get()[0], checkpoint.get()[1])
      changed = true
  changed

proc converge(node: FabricNode) =
  let component = node.componentNodes()
  var changed = true
  var loops = 0
  while changed:
    inc loops
    when defined(fabric_diag):
      echo "converge-loop ", node.identity.account, " loop=", loops
    changed = false
    for peer in component:
      changed = peer.advanceNode(component) or changed

proc scheduleInboundDrain(node: FabricNode) {.gcsafe, raises: [].} =
  if node.isNil or node.inboundScheduled:
    return
  node.inboundScheduled = true
  proc drainCallback(udata: pointer) {.gcsafe, raises: [].} =
    let node = cast[FabricNode](udata)
    if node.isNil:
      return
    node.inboundScheduled = false
    try:
      {.cast(gcsafe).}:
        node.drainInbound()
    except Exception:
      discard
    if node.inbound.len > 0:
      node.scheduleInboundDrain()
  callSoon(drainCallback, cast[pointer](node))

proc enqueueInbound(node: FabricNode, message: sink InboundMessage) {.gcsafe, raises: [].} =
  node.inbound.addLast(message)
  node.scheduleInboundDrain()

proc selfAnnouncement(node: FabricNode): PeerAnnouncement =
  var addrs: seq[string] = @[]
  if not node.network.isNil and not node.network.switch.isNil and not node.network.switch.peerInfo.isNil:
    for addr in node.network.switch.peerInfo.addrs:
      addrs.add($addr)
  PeerAnnouncement(
    account: node.identity.account,
    peerId: peerIdString(node.identity.peerId),
    path: node.config.lsmrPath,
    addrs: addrs,
    publicKey: encodePublicKeyHex(node.identity.publicKey),
    createdAt: int64(node.events.len + node.checkpoints.len + 1),
  )

proc knownPeerAnnouncements(node: FabricNode): seq[PeerAnnouncement] =
  result.add(node.selfAnnouncement())
  for account, peer in node.routingPeers.pairs:
    if account == node.identity.account:
      continue
    var addrs: seq[string] = @[]
    if not node.network.isNil and not node.network.switch.isNil:
      let peerId = PeerId.init(peer.peerId).valueOr:
        result.add(PeerAnnouncement(
          account: peer.account,
          peerId: peer.peerId,
          path: peer.path,
          addrs: @[],
          publicKey: "",
          createdAt: 0,
        ))
        continue
      for addr in node.network.switch.peerStore.getAddresses(peerId):
        addrs.add($addr)
    result.add(PeerAnnouncement(
      account: peer.account,
      peerId: peer.peerId,
      path: peer.path,
      addrs: addrs,
      publicKey: "",
      createdAt: 0,
    ))
  result.sort(proc(a, b: PeerAnnouncement): int = cmp(a.account, b.account))

proc fetchLookupRaw(node: FabricNode, key: string): Option[seq[byte]] {.gcsafe, raises: [].} =
  try:
    if key == "peer:self":
      return some(bytesOf(encodeObj(node.selfAnnouncement())))
    if key == "peer:table":
      return some(bytesOf(encodeObj(node.knownPeerAnnouncements())))
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
    if key.startsWith("eventcert:"):
      return node.store.loadEventCertificateRaw(key["eventcert:".len .. ^1])
  except Exception:
    discard
  none(seq[byte])

proc drainInbound(node: FabricNode) =
  var changed = false
  var processed = 0
  while node.inbound.len > 0 and processed < 256:
    inc processed
    let message = node.inbound.popFirst()
    when defined(fabric_diag):
      echo "drain ", node.identity.account, " kind=", ord(message.kind), " remain=", len(node.inbound)
    try:
      case message.kind
      of imPeerAnnouncement:
        let item = message.peerAnnouncement
        let existing = node.routingPeers.getOrDefault(item.account, RoutingPeer())
        let isNew =
          item.account notin node.routingPeers or
          existing.peerId != item.peerId or
          existing.path != item.path
        node.routingPeers[item.account] = RoutingPeer(
          account: item.account,
          peerId: item.peerId,
          path: item.path,
        )
        if not node.network.isNil and not node.network.switch.isNil:
          let announcedPeerId = PeerId.init(item.peerId).valueOr:
            if isNew and node.running and not node.network.isNil and item.account != node.identity.account:
              node.publishFuture(node.network.publishPeerAnnouncement(item))
            continue
          var addrs: seq[MultiAddress] = @[]
          for raw in item.addrs:
            let parsed = MultiAddress.init(raw)
            if parsed.isOk():
              addrs.add(parsed.get())
          if addrs.len > 0:
            node.network.switch.peerStore.setAddresses(announcedPeerId, addrs)
        if isNew and node.running and not node.network.isNil and item.account != node.identity.account:
          node.publishFuture(node.network.publishPeerAnnouncement(item))
          node.ensureRoutingSnapshotSync(item.peerId)
      of imEvent:
        changed = node.registerEvent(message.event) or changed
      of imAttestation:
        changed = node.registerAttestation(message.attestation) or changed
      of imEventCertificate:
        changed = node.registerEventCertificate(message.eventCertificate) or changed
      of imCheckpointCandidate:
        changed = node.registerCandidate(message.checkpointCandidate) or changed
      of imCheckpointVote:
        changed = node.registerCheckpointVote(message.checkpointVote) or changed
      of imCheckpointBundle:
        let bundle = message.checkpointBundle
        if bundle.snapshot.len > 0:
          changed = node.registerCheckpoint(
            bundle.certificate,
            decodePolarSnapshot(bundle.snapshot),
          ) or changed
      of imAvoProposal:
        changed = node.registerAvoProposal(message.avoProposal) or changed
      of imAvoApproval:
        changed = node.registerAvoApproval(message.proposalId, message.validator) or changed
    except Exception:
      discard
  if changed:
    try:
      when defined(fabric_diag):
        echo "converge-start ", node.identity.account
      node.converge()
      when defined(fabric_diag):
        echo "converge-done ", node.identity.account
    except Exception:
      discard
  if node.inbound.len > 0 and node.running:
    node.scheduleInboundDrain()

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
  try:
    if node.isNil or node.network.isNil or not node.running or peerIdText.len == 0:
      return
    let payload = await node.network.fetchRaw(
      peerIdText,
      "peer:table",
      timeout = 1.seconds,
      maxAttempts = 2,
    )
    if payload.isNone():
      return
    let peers = decodeObj[seq[PeerAnnouncement]](stringOf(payload.get()))
    for peer in peers:
      node.enqueueInbound(InboundMessage(kind: imPeerAnnouncement, peerAnnouncement: peer))
  except CatchableError:
    discard
  finally:
    if not node.isNil:
      node.syncingRoutingPeers.excl(peerIdText)

proc ensureRoutingSnapshotSync(node: FabricNode, peerIdText: string) =
  if node.isNil or node.network.isNil or not node.running or peerIdText.len == 0:
    return
  if peerIdText == peerIdString(node.identity.peerId):
    return
  if peerIdText in node.syncingRoutingPeers:
    return
  node.syncingRoutingPeers.incl(peerIdText)
  asyncSpawn node.syncRoutingSnapshotTask(peerIdText)

proc maintenanceStep(node: FabricNode) =
  if node.isNil or not node.running:
    return
  try:
    node.drainInbound()
    for _, event in node.events.pairs:
      if event.eventId in node.eventCertificates:
        try:
          node.disseminateEventCertificate(event, node.eventCertificates[event.eventId])
        except Exception:
          discard
        continue
      if event.author == node.identity.account:
        try:
          node.disseminateEvent(event)
        except Exception:
          discard
      for role in [arParticipant, arWitness]:
        let att = node.localAttestation(event.eventId, node.identity.account, role)
        if att.isSome():
          try:
            node.disseminateAttestation(event, att.get())
          except Exception:
            discard
    for _, candidate in node.checkpointCandidates.pairs:
      if not node.hasCheckpointForCandidate(candidate.candidateId):
        try:
          node.disseminateCheckpointCandidate(candidate)
        except Exception:
          discard
    for _, votes in node.checkpointVotes.pairs:
      for vote in votes:
        try:
          node.disseminateCheckpointVote(vote)
        except Exception:
          discard
    for checkpointId, cert in node.checkpoints.pairs:
      if checkpointId in node.checkpointSnapshots:
        try:
          node.disseminateCheckpoint(cert, node.checkpointSnapshots[checkpointId])
        except Exception:
          discard
  except Exception:
    discard

proc scheduleMaintenance(node: FabricNode) {.gcsafe, raises: [].} =
  if node.isNil or node.maintenanceScheduled or not node.running:
    return
  node.maintenanceScheduled = true

  proc maintenanceTick() {.async, gcsafe.} =
    try:
      await sleepAsync(250)
    except CancelledError:
      discard
    if node.isNil:
      return
    node.maintenanceScheduled = false
    try:
      {.cast(gcsafe).}:
        node.maintenanceStep()
    except Exception:
      discard
    if node.running:
      node.scheduleMaintenance()

  asyncSpawn maintenanceTick()

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
    store: nil,
    network: nil,
    running: false,
    events: initOrderedTable[string, FabricEvent](),
    eventAttestations: initTable[string, seq[EventAttestation]](),
    eventCertificates: initOrderedTable[string, EventCertificate](),
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
    connectedHandler: nil,
    syncingRoutingPeers: initHashSet[string](),
    maintenanceScheduled: false,
  )
  result.routingPeers[identity.account] = RoutingPeer(
    account: identity.account,
    peerId: peerIdString(identity.peerId),
    path: lsmrPath,
  )

proc newFabricNode*(config: FabricNodeConfig): FabricNode =
  let identity = loadIdentity(config.identityPath)
  let genesis = loadGenesis(config.genesisPath)
  result = newFabricNode(identity, genesis, config.lsmrPath)
  result.config = config
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
      eventHandler = proc(network: FabricNetwork, item: FabricEvent): Future[void] {.gcsafe, raises: [].} =
        proc handle() {.async, gcsafe.} =
          discard network
          node.enqueueInbound(InboundMessage(kind: imEvent, event: item))
        handle(),
      attestationHandler = proc(network: FabricNetwork, item: EventAttestation): Future[void] {.gcsafe, raises: [].} =
        proc handle() {.async, gcsafe.} =
          discard network
          node.enqueueInbound(InboundMessage(kind: imAttestation, attestation: item))
        handle(),
      eventCertificateHandler = proc(network: FabricNetwork, item: EventCertificate): Future[void] {.gcsafe, raises: [].} =
        proc handle() {.async, gcsafe.} =
          discard network
          node.enqueueInbound(InboundMessage(kind: imEventCertificate, eventCertificate: item))
        handle(),
      checkpointCandidateHandler = proc(network: FabricNetwork, item: CheckpointCandidate): Future[void] {.gcsafe, raises: [].} =
        proc handle() {.async, gcsafe.} =
          discard network
          node.enqueueInbound(InboundMessage(kind: imCheckpointCandidate, checkpointCandidate: item))
        handle(),
      checkpointVoteHandler = proc(network: FabricNetwork, item: CheckpointVote): Future[void] {.gcsafe, raises: [].} =
        proc handle() {.async, gcsafe.} =
          discard network
          node.enqueueInbound(InboundMessage(kind: imCheckpointVote, checkpointVote: item))
        handle(),
      checkpointBundleHandler = proc(network: FabricNetwork, item: CheckpointBundle): Future[void] {.gcsafe, raises: [].} =
        proc handle() {.async, gcsafe.} =
          discard network
          node.enqueueInbound(InboundMessage(kind: imCheckpointBundle, checkpointBundle: item))
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
      if node.config.routingMode != RoutingPlaneMode.legacyOnly and
          not node.network.switch.isNil:
        try:
          discard await node.network.switch.refreshCoordinateRecord()
        except CatchableError:
          discard
      node.connectedHandler = proc(
          peerId: PeerId, event: ConnEvent
      ): Future[void] {.gcsafe, async: (raises: [CancelledError]).} =
        if event.kind != ConnEventKind.Connected:
          return
        if peerId == node.identity.peerId:
          return
        try:
          await node.network.publishPeerAnnouncement(node.selfAnnouncement())
        except CatchableError:
          discard
      node.network.switch.addConnEventHandler(node.connectedHandler, ConnEventKind.Connected)
      for peer in await node.bootstrapRoutingSnapshot():
        node.enqueueInbound(InboundMessage(kind: imPeerAnnouncement, peerAnnouncement: peer))
      if node.config.routingMode != RoutingPlaneMode.legacyOnly and
          not node.network.switch.isNil:
        try:
          discard await node.network.switch.refreshCoordinateRecord()
        except CatchableError:
          discard
      await node.network.publishPeerAnnouncement(node.selfAnnouncement())
    except CatchableError:
      discard
  node.scheduleMaintenance()

proc stop*(node: FabricNode) {.async: (raises: [], gcsafe: false).} =
  node.running = false
  node.maintenanceScheduled = false
  if not node.connectedHandler.isNil and not node.network.isNil and not node.network.switch.isNil:
    node.network.switch.removeConnEventHandler(node.connectedHandler, ConnEventKind.Connected)
    node.connectedHandler = nil
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
  )
  peer.routingPeers[node.identity.account] = RoutingPeer(
    account: node.identity.account,
    peerId: peerIdString(node.identity.peerId),
    path: node.config.lsmrPath,
  )

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
    discard node.registerEvent(event)
  for att in node.store.loadAllAttestations():
    discard node.registerAttestation(att)
  for isolation in node.store.loadAllIsolations():
    node.isolatedEvents[isolation.eventId] = isolation
  let certs = node.store.loadAllEventCertificates().sorted(proc(a, b: EventCertificate): int =
    result = clockCmp(a.clock, b.clock)
    if result == 0:
      result = cmp(a.eventId, b.eventId)
  )
  for cert in certs:
    discard node.registerEventCertificate(cert)
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
  let descriptor = node.liveState.descriptorFor(tx)
  let parents = frontierDigest(node.certifiedEvents()).certifiedEventIds
  var parentClock = GanzhiClock()
  for parentId in parents:
    if parentId in node.events:
      parentClock = maxClock(parentClock, node.events[parentId].clock)
  var participantRoutes = @[node.selfRoutingPeer()]
  for participant in descriptor.participants:
    if participant == node.identity.account:
      continue
    let route = node.routingPeerForAccount(participant)
    if route.isSome():
      participantRoutes.add(route.get())
  participantRoutes = uniqueRoutingPeers(participantRoutes)
  let participantPaths = participantRoutes.mapIt(it.path)
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
  result = FabricEvent(
    eventId: "",
    threadId: node.liveState.threadIdFor(tx),
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

proc submitTx*(node: FabricNode, tx: Tx): SubmitEventResult =
  node.drainInbound()
  let event = node.buildEvent(tx)
  result.eventId = event.eventId
  result.accepted = node.registerEvent(event)
  if result.accepted:
    node.disseminateEvent(event)
    node.converge()
    result.certified = event.eventId in node.eventCertificates
    for checkpointId, checkpoint in node.checkpoints.pairs:
      if checkpoint.candidate.era >= event.clock.era:
        result.checkpointIds.add(checkpointId)

proc fabricStatus*(node: FabricNode): FabricStatusSnapshot =
  when defined(fabric_diag):
    echo "status-start ", node.identity.account, " inbound=", len(node.inbound)
  node.drainInbound()
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

proc getEvent*(node: FabricNode, eventId: string): Option[FabricEvent] =
  node.drainInbound()
  node.eventById(eventId)

proc getEventCertificate*(node: FabricNode, eventId: string): Option[EventCertificate] =
  node.drainInbound()
  if node.eventCertificates.hasKey(eventId):
    return some(node.eventCertificates[eventId])
  none(EventCertificate)

proc getCheckpoint*(node: FabricNode, checkpointId = ""): Option[CheckpointCertificate] =
  node.drainInbound()
  let targetId = if checkpointId.len > 0: checkpointId else: node.latestCheckpointId
  node.checkpointById(targetId)

proc snapshotState(node: FabricNode, checkpointId = ""): Option[ChainState] =
  node.checkpointState(checkpointId)

proc publishTasks*(node: FabricNode, checkpointId = ""): seq[PublishTaskSnapshot] =
  node.drainInbound()
  let state = node.snapshotState(checkpointId)
  if state.isNone():
    return @[]
  for _, record in state.get().contents.pairs:
    result.add(record.publishTaskSnapshot())
  result.sort(proc(a, b: PublishTaskSnapshot): int = cmp(b.updatedAt, a.updatedAt))

proc feedSnapshot*(node: FabricNode, limit = 50, checkpointId = ""): seq[SocialFeedItemSnapshot] =
  node.drainInbound()
  let state = node.snapshotState(checkpointId)
  if state.isSome():
    return state.get().socialFeedSnapshot(limit)
  @[]

proc contentDetail*(node: FabricNode, contentId: string, checkpointId = ""): Option[SocialContentDetailSnapshot] =
  node.drainInbound()
  let state = node.snapshotState(checkpointId)
  if state.isNone():
    return none(SocialContentDetailSnapshot)
  if contentId notin state.get().contents:
    return none(SocialContentDetailSnapshot)
  some(state.get().socialContentDetailSnapshot(contentId))

proc reportList*(node: FabricNode, contentId = "", checkpointId = ""): seq[ReportTicket] =
  node.drainInbound()
  let state = node.snapshotState(checkpointId)
  if state.isSome():
    return state.get().reportList(contentId)
  @[]

proc creditBalance*(node: FabricNode, owner: string, checkpointId = ""): Option[CreditBalanceSnapshot] =
  node.drainInbound()
  let state = node.snapshotState(checkpointId)
  if state.isNone():
    return none(CreditBalanceSnapshot)
  some(state.get().creditBalanceSnapshot(owner))

proc mintIntentStatus*(node: FabricNode, mintIntentId: string, checkpointId = ""): Option[MintIntentStatus] =
  node.drainInbound()
  let state = node.snapshotState(checkpointId)
  if state.isNone() or mintIntentId notin state.get().mintIntents:
    return none(MintIntentStatus)
  some(state.get().mintIntentStatusSnapshot(mintIntentId))

proc resolveName*(node: FabricNode, name: string, nowTs: int64, checkpointId = ""): Option[NameRecordSnapshot] =
  node.drainInbound()
  let state = node.snapshotState(checkpointId)
  if state.isNone():
    return none(NameRecordSnapshot)
  state.get().resolveNameSnapshot(name, nowTs)

proc queryProjection*(node: FabricNode, projection: string, checkpointId = ""): JsonNode =
  node.drainInbound()
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
  node.drainInbound()
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
  node.drainInbound()
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
  node.disseminateAvoProposal(result)

proc approveAvoProposal*(node: FabricNode, proposalId: string): bool =
  node.drainInbound()
  if proposalId notin node.avoProposals:
    return false
  if node.identity.account in node.avoApprovals.getOrDefault(proposalId, @[]):
    return false
  node.disseminateAvoApproval(proposalId, node.identity.account)
  node.converge()
  true
