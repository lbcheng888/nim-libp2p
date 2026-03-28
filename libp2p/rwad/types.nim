import std/[json, options]

type
  TxKind* = enum
    txContentPublishIntent
    txContentManifestCommit
    txContentReportSubmit
    txContentReviewDecide
    txContentTombstoneIssue
    txContentTombstoneRestore
    txCreditIssue
    txCreditFreeze
    txCreditExpire
    txCreditConvert
    txTreasuryMintIntentCreate
    txTreasuryExecutionReceiptSubmit
    txTreasuryCustodySnapshotSubmit
    txTreasuryReserveProofSubmit
    txRwadMintFinalize
    txNameRegister
    txNameRenew
    txNameTransfer
    txStake
    txDelegate
    txValidatorRegister
    txSlash
    txParamUpdate

  ModerationClass* = enum
    mcC0
    mcC1
    mcC2
    mcC3

  ModerationAction* = enum
    maBlockAndPreserve
    maHoldForProReview
    maPassToPool

  DistributionTag* = enum
    dtSafeForHome
    dtLowDistribution
    dtCommercialRisk
    dtSpamOrDuplicate
    dtMisleading
    dtRequireProReview

  ReviewChannel* = enum
    rcStep0
    rcRed
    rcBlue
    rcProfessional
    rcCommunity

  ModerationCaseStatus* = enum
    mcsOpen
    mcsResolved
    mcsEscalated

  CreditStatus* = enum
    csOpen
    csFrozen
    csExpired
    csConverted

  MintIntentState* = enum
    misIntentCreated
    misFundsReceived
    misDexExecuting
    misDexSettled
    misCustodyPending
    misCustodyAttested
    misMinted
    misFinalized
    misFailed

  NameStatus* = enum
    nsActive
    nsGrace
    nsFrozen
    nsExpired

  SlashingReason* = enum
    srDoubleSign
    srOffline
    srInvalidCertificate

  KeyValueU64* = object
    key*: string
    value*: uint64

  KeyValueString* = object
    key*: string
    value*: string

  ManifestChunk* = object
    index*: int
    cid*: string
    chunkHash*: string
    size*: int
    mime*: string

  CapabilityTicket* = object
    ticketId*: string
    reader*: string
    expiresAt*: int64
    readCountLimit*: int
    allowForward*: bool
    allowPreviewOnly*: bool

  PublishIntent* = object
    contentId*: string
    author*: string
    title*: string
    body*: string
    kind*: string
    manifestCid*: string
    createdAt*: int64
    accessPolicy*: string
    previewCid*: string
    seqNo*: uint64

  ContentManifest* = object
    version*: uint32
    manifestCid*: string
    contentHash*: string
    totalSize*: int64
    chunkSize*: int
    mime*: string
    createdAt*: int64
    snapshotAt*: int64
    previewCid*: string
    thumbnailCid*: string
    accessPolicy*: string
    originHolder*: string
    readOnly*: bool
    authorSignature*: string
    chunks*: seq[ManifestChunk]
    tickets*: seq[CapabilityTicket]

  SourceGraph* = object
    sourceUri*: string
    parentContentIds*: seq[string]
    relatedNames*: seq[string]
    importedFrom*: seq[string]

  LabelClaim* = object
    label*: string
    source*: string
    score*: float
    evidenceHash*: string

  ReportTicket* = object
    reportId*: string
    contentId*: string
    reporter*: string
    channel*: ReviewChannel
    category*: string
    reason*: string
    evidenceHash*: string
    createdAt*: int64
    depositLocked*: uint64

  ModerationCase* = object
    caseId*: string
    contentId*: string
    openedBy*: string
    channel*: ReviewChannel
    classification*: ModerationClass
    status*: ModerationCaseStatus
    decision*: ModerationAction
    labels*: seq[DistributionTag]
    reason*: string
    reviewers*: seq[string]
    updatedAt*: int64

  DistributionDecision* = object
    decisionId*: string
    contentId*: string
    moderationClass*: ModerationClass
    moderationAction*: ModerationAction
    tags*: seq[DistributionTag]
    visibleInHome*: bool
    visibleInDetail*: bool
    visibleInFeed*: bool
    reason*: string
    updatedAt*: int64

  TombstoneRecord* = object
    tombstoneId*: string
    contentId*: string
    contentHash*: string
    scope*: string
    reasonCode*: string
    issuedBy*: string
    reason*: string
    basisType*: string
    executionReceipt*: string
    issuedAt*: int64
    restoredAt*: int64
    active*: bool
    restoreAllowed*: bool
    restoreDecisionId*: string

  ContentRecord* = object
    contentId*: string
    author*: string
    title*: string
    body*: string
    publishIntent*: PublishIntent
    manifest*: ContentManifest
    sourceGraph*: SourceGraph
    labels*: seq[LabelClaim]
    reportIds*: seq[string]
    moderationCaseId*: string
    distribution*: DistributionDecision
    tombstoneId*: string
    createdAt*: int64
    updatedAt*: int64

  CreditPosition* = object
    positionId*: string
    owner*: string
    sourceType*: string
    sourceId*: string
    amount*: uint64
    available*: uint64
    issuedAt*: int64
    expiresAt*: int64
    convertibleAfter*: int64
    budgetEpoch*: uint64
    status*: CreditStatus
    freezeReason*: string

  DepositReceipt* = object
    depositId*: string
    mintIntentId*: string
    chainId*: string
    token*: string
    txHash*: string
    logIndex*: uint32
    amount*: uint64
    receivedAt*: int64

  ExecutionReceipt* = object
    receiptId*: string
    mintIntentId*: string
    legIndex*: int
    assetOut*: string
    amountOut*: uint64
    avgPxUsd*: uint64
    venue*: string
    evidenceHash*: string
    settledAt*: int64

  CustodySnapshot* = object
    snapshotId*: string
    epochId*: uint64
    proofHash*: string
    totalCustodyUsd*: uint64
    totalLiabilitiesUsd*: uint64
    goldAllocatedUsd*: uint64
    goldPendingUsd*: uint64
    coveredIntentIds*: seq[string]
    signatures*: seq[string]
    createdAt*: int64

  ReserveProof* = object
    proofId*: string
    epochId*: uint64
    reserveProofHash*: string
    netNavDeltaUsd*: uint64
    coveredIntentIds*: seq[string]
    custodySnapshotIds*: seq[string]
    signatures*: seq[string]
    submittedAt*: int64

  MintRequest* = object
    mintIntentId*: string
    reserveProofHash*: string
    amount*: uint64
    toAccount*: string
    requestedAt*: int64

  TokenRequest* = object
    requestId*: string
    owner*: string
    amount*: uint64
    reason*: string
    requestedAt*: int64

  MintIntent* = object
    mintIntentId*: string
    userAccount*: string
    inAsset*: string
    inAmount*: uint64
    basketPolicy*: string
    basketWeights*: seq[uint32]
    maxSlippageBp*: uint32
    ackIrreversible*: bool
    createdAt*: int64
    depositId*: string
    executionReceiptIds*: seq[string]
    custodySnapshotId*: string
    reserveProofHash*: string
    mintedAmount*: uint64
    status*: MintIntentState
    failureReason*: string

  NameRecord* = object
    name*: string
    owner*: string
    targetAccount*: string
    targetContentId*: string
    resolvedCid*: string
    registeredAt*: int64
    expiresAt*: int64
    graceUntil*: int64
    status*: NameStatus
    frozen*: bool

  Delegation* = object
    delegator*: string
    validator*: string
    amount*: uint64
    updatedAt*: int64

  ValidatorInfo* = object
    address*: string
    peerId*: string
    publicKey*: string
    stake*: uint64
    delegatedStake*: uint64
    active*: bool
    jailed*: bool
    slashedAmount*: uint64
    lastUpdatedAt*: int64

  SlashEvidence* = object
    offender*: string
    round*: uint64
    batchA*: string
    batchB*: string
    reason*: SlashingReason

  Tx* = object
    txId*: string
    kind*: TxKind
    sender*: string
    senderPublicKey*: string
    nonce*: uint64
    epoch*: uint64
    timestamp*: int64
    payload*: JsonNode
    signature*: string

  Batch* = object
    batchId*: string
    proposer*: string
    proposerPublicKey*: string
    epoch*: uint64
    round*: uint64
    parents*: seq[string]
    transactions*: seq[Tx]
    txIds*: seq[string]
    createdAt*: int64
    signature*: string

  CertificateVote* = object
    voter*: string
    signature*: string
    weight*: uint64

  BatchVote* = object
    batchId*: string
    proposer*: string
    epoch*: uint64
    round*: uint64
    batchHash*: string
    vote*: CertificateVote

  BatchCertificate* = object
    certificateId*: string
    batchId*: string
    proposer*: string
    epoch*: uint64
    round*: uint64
    batchHash*: string
    votes*: seq[CertificateVote]
    quorumWeight*: uint64
    createdAt*: int64

  DagVertex* = object
    batch*: Batch
    certificate*: BatchCertificate

  Checkpoint* = object
    checkpointId*: string
    height*: uint64
    epoch*: uint64
    stateRoot*: string
    validatorRoot*: string
    createdAt*: int64

  FinalizedBlock* = object
    blockId*: string
    height*: uint64
    epoch*: uint64
    round*: uint64
    leaderBatchId*: string
    orderedBatchIds*: seq[string]
    batches*: seq[Batch]
    transactions*: seq[Tx]
    stateRoot*: string
    checkpoint*: Checkpoint
    createdAt*: int64

  AuditEvent* = object
    eventId*: string
    category*: string
    subjectId*: string
    action*: string
    payload*: JsonNode
    createdAt*: int64

  ChainParams* = object
    chainId*: string
    epochLengthRounds*: uint64
    quorumNumerator*: uint64
    quorumDenominator*: uint64
    maxBatchTxs*: int
    maxBatchBytes*: int
    checkpointEveryBlocks*: uint64
    nameTtlSeconds*: int64
    nameGraceSeconds*: int64
    reviewDeposit*: uint64
    blueReviewDeposit*: uint64
    goldPendingHaircutBp*: uint32
    mintFeeBp*: uint32
    rewardPoolAccount*: string

  GenesisSpec* = object
    params*: ChainParams
    validators*: seq[ValidatorInfo]
    balances*: seq[KeyValueU64]
    names*: seq[NameRecord]
    contents*: seq[ContentRecord]
    mintedSupply*: uint64

  PublishTaskSnapshot* = object
    contentId*: string
    author*: string
    manifestCid*: string
    moderationClass*: ModerationClass
    moderationDecision*: ModerationAction
    visibleInFeed*: bool
    visibleInDetail*: bool
    reason*: string
    updatedAt*: int64

  SocialFeedItemSnapshot* = object
    contentId*: string
    author*: string
    title*: string
    manifestCid*: string
    previewCid*: string
    tags*: seq[DistributionTag]
    createdAt*: int64

  SocialContentDetailSnapshot* = object
    contentId*: string
    author*: string
    title*: string
    body*: string
    manifest*: ContentManifest
    sourceGraph*: SourceGraph
    labels*: seq[LabelClaim]
    reports*: seq[ReportTicket]
    moderationCase*: Option[ModerationCase]
    distribution*: DistributionDecision
    tombstone*: Option[TombstoneRecord]
    visibleInDetail*: bool

  CreditBalanceSnapshot* = object
    owner*: string
    totalIssued*: uint64
    totalAvailable*: uint64
    totalFrozen*: uint64
    totalExpired*: uint64
    totalConverted*: uint64
    positions*: seq[CreditPosition]

  MintIntentStatus* = object
    mintIntentId*: string
    userAccount*: string
    status*: MintIntentState
    reserveProofHash*: string
    mintedAmount*: uint64
    failureReason*: string
    executionReceipts*: seq[ExecutionReceipt]
    custodySnapshot*: Option[CustodySnapshot]
    reserveProof*: Option[ReserveProof]

  NameRecordSnapshot* = object
    name*: string
    owner*: string
    targetAccount*: string
    targetContentId*: string
    resolvedCid*: string
    expiresAt*: int64
    graceUntil*: int64
    status*: NameStatus

const TxKindSlugs* = [
  "content_publish_intent",
  "content_manifest_commit",
  "content_report_submit",
  "content_review_decide",
  "content_tombstone_issue",
  "content_tombstone_restore",
  "credit_issue",
  "credit_freeze",
  "credit_expire",
  "credit_convert",
  "treasury_mint_intent_create",
  "treasury_execution_receipt_submit",
  "treasury_custody_snapshot_submit",
  "treasury_reserve_proof_submit",
  "rwad_mint_finalize",
  "name_register",
  "name_renew",
  "name_transfer",
  "stake",
  "delegate",
  "validator_register",
  "slash",
  "param_update",
]

proc slug*(kind: TxKind): string =
  TxKindSlugs[ord(kind)]

proc parseTxKind*(value: string): Option[TxKind] =
  for kind in TxKind:
    if value == slug(kind):
      return some(kind)
  none(TxKind)

proc defaultChainParams*(): ChainParams =
  ChainParams(
    chainId: "rwad-local",
    epochLengthRounds: 32,
    quorumNumerator: 2,
    quorumDenominator: 3,
    maxBatchTxs: 128,
    maxBatchBytes: 1_048_576,
    checkpointEveryBlocks: 1,
    nameTtlSeconds: 365'i64 * 24 * 60 * 60,
    nameGraceSeconds: 30'i64 * 24 * 60 * 60,
    reviewDeposit: 10,
    blueReviewDeposit: 1,
    goldPendingHaircutBp: 5_000,
    mintFeeBp: 50,
    rewardPoolAccount: "reward_pool",
  )
