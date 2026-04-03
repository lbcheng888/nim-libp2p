import std/[json, options]

import ../lsmr
import ../rwad/types

type
  AttestationRole* = enum
    arParticipant
    arWitness
    arIsolation

  IsolationKind* = enum
    ikDoubleSpend
    ikDoubleThread
    ikReverseClock
    ikInvalidParticipantSignature
    ikInvalidContractRoot

  ProjectionKind* = enum
    pkLedger
    pkContent
    pkCredit
    pkTreasury
    pkName
    pkStaking
    pkGovernance

  GanzhiClock* = object
    era*: uint64
    tick*: uint8

  EventThreadId* = object
    contractRoot*: string
    owner*: string
    lane*: string

  ParticipantSet* = seq[string]

  ProjectionRoot* = object
    kind*: ProjectionKind
    contractRoot*: string
    root*: string

  RoutingPeer* = object
    account*: string
    peerId*: string
    path*: LsmrPath
    addrs*: seq[string]

  PeerAnnouncement* = object
    account*: string
    peerId*: string
    path*: LsmrPath
    addrs*: seq[string]
    publicKey*: string
    routeVersion*: uint64
    createdAt*: int64
    signature*: string

  EventAttestation* = object
    eventId*: string
    signer*: string
    signerPublicKey*: string
    signerPeerId*: string
    role*: AttestationRole
    weight*: uint64
    issuedAt*: int64
    signature*: string

  FabricEvent* = object
    eventId*: string
    threadId*: EventThreadId
    author*: string
    authorPublicKey*: string
    authorPeerId*: string
    contractRoot*: string
    entrypoint*: string
    args*: JsonNode
    legacyTx*: Tx
    participants*: ParticipantSet
    participantRoutes*: seq[RoutingPeer]
    witnessRoutes*: seq[RoutingPeer]
    witnessSet*: seq[string]
    parents*: seq[string]
    clock*: GanzhiClock
    routingPath*: LsmrPath
    createdAt*: int64

  EventCertificate* = object
    certificateId*: string
    eventId*: string
    threadId*: EventThreadId
    clock*: GanzhiClock
    participantAttestations*: seq[EventAttestation]
    witnessAttestations*: seq[EventAttestation]
    reachableWitnesses*: int
    witnessThreshold*: int
    createdAt*: int64

  EventFrontier* = object
    era*: uint64
    certifiedEventIds*: seq[string]
    frontierDigest*: string
    maxClock*: GanzhiClock

  AvoRuleBundle* = object
    bundleId*: string
    targetContractRoot*: string
    targetEntrypoint*: string
    triggerSummary*: string
    rewriteSummary*: string
    proofBundleHash*: string

  AvoProposal* = object
    proposalId*: string
    bundle*: AvoRuleBundle
    checkpointStartEra*: uint64
    checkpointEndEra*: uint64
    proposer*: string
    createdAt*: int64

  AvoAdoptionRecord* = object
    proposalId*: string
    adoptedInCheckpointId*: string
    activatesAtEra*: uint64
    adoptedAt*: int64

  IsolationRecord* = object
    isolationId*: string
    eventId*: string
    kind*: IsolationKind
    reason*: string
    witnesses*: seq[EventAttestation]
    createdAt*: int64

  CheckpointVote* = object
    candidateId*: string
    validator*: string
    validatorPublicKey*: string
    validatorPeerId*: string
    weight*: uint64
    issuedAt*: int64
    signature*: string

  CheckpointCandidate* = object
    candidateId*: string
    era*: uint64
    frontierDigest*: string
    projectionRoots*: seq[ProjectionRoot]
    activeContractRoots*: seq[string]
    activeAvoSet*: seq[string]
    adoptedProposalIds*: seq[string]
    isolatedEventIds*: seq[string]
    snapshotRoot*: string
    validatorSetRoot*: string
    createdAt*: int64

  CheckpointCertificate* = object
    checkpointId*: string
    candidate*: CheckpointCandidate
    votes*: seq[CheckpointVote]
    quorumWeight*: uint64
    createdAt*: int64

  CheckpointBundle* = object
    certificate*: CheckpointCertificate
    snapshot*: seq[byte]

  FabricStatusSnapshot* = object
    chainId*: string
    localAccount*: string
    localPeerId*: string
    lsmrPath*: LsmrPath
    routingStatus*: RoutingPlaneStatus
    eventCount*: int
    certifiedEventCount*: int
    checkpointCount*: int
    latestCheckpointId*: string
    latestCheckpointEra*: uint64
    latestFrontierDigest*: string
    activeContractRoots*: seq[string]
    activeAvoSet*: seq[string]

  SubmitEventResult* = object
    eventId*: string
    accepted*: bool
    certified*: bool
    checkpointIds*: seq[string]

  SystemContractDescriptor* = object
    contractRoot*: string
    entrypoint*: string
    subjectId*: string
    participants*: ParticipantSet

  FabricNodeConfig* = object
    dataDir*: string
    identityPath*: string
    genesisPath*: string
    listenAddrs*: seq[string]
    bootstrapAddrs*: seq[string]
    legacyBootstrapAddrs*: seq[string]
    lsmrBootstrapAddrs*: seq[string]
    lsmrPath*: LsmrPath
    routingMode*: RoutingPlaneMode
    primaryPlane*: PrimaryRoutingPlane
    lsmrConfig*: Option[LsmrConfig]
