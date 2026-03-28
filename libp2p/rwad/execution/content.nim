import std/[algorithm, options, sequtils, strutils, tables]

import ../codec
import ../types
import ./state

const
  C0Signals = ["illegal", "terror", "porn", "fraud", "scam", "violence"]
  C1Signals = ["minor_risk", "hotspot", "mobilization", "sensitive", "ip_dispute"]
  C2Signals = ["spam", "duplicate", "ad", "commercial", "misleading", "low_quality"]

type
  BlobReadKind* = enum
    brkHead
    brkChunk
    brkRange

proc containsAny(text: string, needles: openArray[string]): bool =
  let lower = text.toLowerAscii()
  let tokens = lower.split({' ', '\t', '\n', '\r', ',', '.', ':', ';', '/', '-', '(', ')', '[', ']'})
  for needle in needles:
    if needle in tokens:
      return true
  false

proc dedupeTags(tags: seq[DistributionTag]): seq[DistributionTag] =
  for tag in tags:
    if tag notin result:
      result.add(tag)

proc normalizedAccessPolicy(value: string): string =
  value.strip().toLowerAscii()

proc validateChunkLayout(manifest: ContentManifest) =
  if manifest.chunks.len == 0:
    raise newException(ValueError, "manifest must include chunks")
  if manifest.chunkSize <= 0:
    raise newException(ValueError, "manifest chunkSize must be positive")
  var total = 0'i64
  for idx, chunk in manifest.chunks:
    if chunk.index != idx:
      raise newException(ValueError, "manifest chunk indexes must be contiguous")
    if chunk.cid.len == 0 or chunk.chunkHash.len == 0:
      raise newException(ValueError, "manifest chunk cid and hash are required")
    if chunk.size <= 0:
      raise newException(ValueError, "manifest chunk size must be positive")
    total += int64(chunk.size)
  if manifest.totalSize != total:
    raise newException(ValueError, "manifest totalSize does not match chunk sizes")

proc validateTickets(manifest: ContentManifest) =
  var ticketIds: seq[string] = @[]
  for ticket in manifest.tickets:
    if ticket.ticketId.len == 0:
      raise newException(ValueError, "capability ticket id is required")
    if ticket.ticketId in ticketIds:
      raise newException(ValueError, "duplicate capability ticket: " & ticket.ticketId)
    ticketIds.add(ticket.ticketId)
    if ticket.reader.len == 0:
      raise newException(ValueError, "capability ticket reader is required")
    if ticket.readCountLimit < 0:
      raise newException(ValueError, "capability ticket readCountLimit must be non-negative")

proc validatePublishIntent(intent: PublishIntent) =
  if intent.contentId.len == 0:
    raise newException(ValueError, "contentId is required")
  if intent.author.len == 0:
    raise newException(ValueError, "author is required")
  if intent.kind.len == 0:
    raise newException(ValueError, "content kind is required")
  if intent.manifestCid.len == 0:
    raise newException(ValueError, "manifestCid is required")
  if normalizedAccessPolicy(intent.accessPolicy).len == 0:
    raise newException(ValueError, "accessPolicy is required")
  if intent.createdAt <= 0:
    raise newException(ValueError, "createdAt must be positive")

proc validateManifest*(manifest: ContentManifest) =
  if manifest.version == 0:
    raise newException(ValueError, "manifest version is required")
  if manifest.manifestCid.len == 0:
    raise newException(ValueError, "manifestCid is required")
  if manifest.contentHash.len == 0:
    raise newException(ValueError, "contentHash is required")
  if manifest.mime.len == 0:
    raise newException(ValueError, "manifest mime is required")
  if normalizedAccessPolicy(manifest.accessPolicy).len == 0:
    raise newException(ValueError, "manifest accessPolicy is required")
  if manifest.originHolder.len == 0:
    raise newException(ValueError, "manifest originHolder is required")
  if not manifest.readOnly:
    raise newException(ValueError, "manifest must be readOnly")
  if manifest.authorSignature.len == 0:
    raise newException(ValueError, "manifest authorSignature is required")
  if manifest.createdAt <= 0 or manifest.snapshotAt <= 0:
    raise newException(ValueError, "manifest timestamps must be positive")
  if manifest.snapshotAt < manifest.createdAt:
    raise newException(ValueError, "manifest snapshotAt must be >= createdAt")
  validateChunkLayout(manifest)
  validateTickets(manifest)

proc findCapabilityTicket*(
    manifest: ContentManifest, reader, ticketId: string, at: int64
): Option[CapabilityTicket] =
  for ticket in manifest.tickets:
    if ticket.ticketId != ticketId or ticket.reader != reader:
      continue
    if ticket.expiresAt > 0 and at > ticket.expiresAt:
      return none(CapabilityTicket)
    return some(ticket)
  none(CapabilityTicket)

proc canReadManifest*(
    manifest: ContentManifest,
    reader, ticketId: string,
    at: int64,
    kind: BlobReadKind,
): bool =
  let policy = normalizedAccessPolicy(manifest.accessPolicy)
  if policy == "public":
    return true
  if reader.len == 0 or ticketId.len == 0:
    return false
  let ticket = manifest.findCapabilityTicket(reader, ticketId, at)
  if ticket.isNone():
    return false
  if kind in {brkChunk, brkRange} and ticket.get().allowPreviewOnly:
    return false
  true

proc buildDecision(
    contentId: string,
    classification: ModerationClass,
    action: ModerationAction,
    tags: seq[DistributionTag],
    reason: string,
    updatedAt: int64,
    tombstoned = false,
): DistributionDecision =
  var visibleHome = false
  var visibleFeed = false
  var visibleDetail = false
  case action
  of maPassToPool:
    visibleDetail = not tombstoned
    visibleHome = not tombstoned
    visibleFeed = not tombstoned
    for tag in tags:
      if tag in {dtLowDistribution, dtCommercialRisk, dtSpamOrDuplicate, dtMisleading, dtRequireProReview}:
        visibleHome = false
        visibleFeed = false
  of maHoldForProReview:
    discard
  of maBlockAndPreserve:
    discard
  DistributionDecision(
    decisionId: hashHex(contentId & ":" & $updatedAt & ":" & reason),
    contentId: contentId,
    moderationClass: classification,
    moderationAction: action,
    tags: dedupeTags(tags),
    visibleInHome: visibleHome,
    visibleInDetail: visibleDetail,
    visibleInFeed: visibleFeed,
    reason: reason,
    updatedAt: updatedAt,
  )

proc classifyStep0*(
    intent: PublishIntent, labels: seq[LabelClaim], riskSignals: seq[string]
): tuple[classification: ModerationClass, action: ModerationAction, reason: string,
         tags: seq[DistributionTag]] =
  let joinedLabels = labels.mapIt(it.label).join(" ")
  let joinedSignals = riskSignals.join(" ")
  let text = intent.title & " " & intent.body & " " & joinedLabels & " " & joinedSignals
  if containsAny(text, C0Signals):
    return (mcC0, maBlockAndPreserve, "c0_signal", @[dtRequireProReview])
  if containsAny(text, C1Signals):
    return (mcC1, maHoldForProReview, "c1_signal", @[dtRequireProReview, dtMisleading])
  if containsAny(text, C2Signals):
    return (mcC2, maHoldForProReview, "c2_signal", @[dtCommercialRisk, dtRequireProReview])
  (mcC3, maPassToPool, "step0_pass", @[dtSafeForHome])

proc latestActiveTombstone(state: ChainState, contentId: string): Option[TombstoneRecord] =
  var latest: Option[TombstoneRecord] = none(TombstoneRecord)
  for _, item in state.tombstones.pairs:
    if item.contentId == contentId and item.active:
      if latest.isNone() or latest.get().issuedAt < item.issuedAt:
        latest = some(item)
  latest

proc contentVisibleInFeed*(record: ContentRecord): bool =
  record.distribution.visibleInFeed and record.tombstoneId.len == 0

proc contentVisibleInDetail*(record: ContentRecord): bool =
  record.distribution.visibleInDetail and record.tombstoneId.len == 0

proc applyContentPublish*(
    state: ChainState,
    intent: PublishIntent,
    sourceGraph: SourceGraph,
    labels: seq[LabelClaim],
    riskSignals: seq[string],
) =
  validatePublishIntent(intent)
  let step0 = classifyStep0(intent, labels, riskSignals)
  let now = intent.createdAt
  var manifest = ContentManifest(
    version: 1,
    manifestCid: intent.manifestCid,
    contentHash: "",
    totalSize: 0,
    chunkSize: 0,
    mime: "",
    createdAt: now,
    snapshotAt: now,
    previewCid: intent.previewCid,
    thumbnailCid: "",
    accessPolicy: normalizedAccessPolicy(intent.accessPolicy),
    originHolder: intent.author,
    readOnly: true,
    authorSignature: "",
    chunks: @[],
    tickets: @[],
  )
  let distribution = buildDecision(
    intent.contentId,
    step0.classification,
    step0.action,
    step0.tags,
    step0.reason,
    now,
  )
  let record = ContentRecord(
    contentId: intent.contentId,
    author: intent.author,
    title: intent.title,
    body: intent.body,
    publishIntent: intent,
    manifest: manifest,
    sourceGraph: sourceGraph,
    labels: labels,
    reportIds: @[],
    moderationCaseId: "",
    distribution: distribution,
    tombstoneId: "",
    createdAt: now,
    updatedAt: now,
  )
  state.contents[intent.contentId] = record

proc applyManifestCommit*(
    state: ChainState,
    contentId: string,
    manifest: ContentManifest,
    sourceGraph: Option[SourceGraph],
    labels: seq[LabelClaim],
    at: int64,
) =
  if contentId notin state.contents:
    raise newException(ValueError, "unknown content: " & contentId)
  validateManifest(manifest)
  var record = state.contents[contentId]
  if record.publishIntent.manifestCid.len > 0 and manifest.manifestCid != record.publishIntent.manifestCid:
    raise newException(ValueError, "manifestCid does not match publish intent")
  if normalizedAccessPolicy(record.publishIntent.accessPolicy).len > 0 and
      normalizedAccessPolicy(manifest.accessPolicy) != normalizedAccessPolicy(record.publishIntent.accessPolicy):
    raise newException(ValueError, "manifest accessPolicy does not match publish intent")
  if record.manifest.contentHash.len > 0 and record.manifest != manifest:
    raise newException(ValueError, "content manifest is immutable once committed")
  record.manifest = manifest
  if sourceGraph.isSome():
    record.sourceGraph = sourceGraph.get()
  if labels.len > 0:
    record.labels = labels
  record.updatedAt = at
  state.contents[contentId] = record

proc applyReport*(
    state: ChainState,
    ticket: ReportTicket,
) =
  if ticket.contentId notin state.contents:
    raise newException(ValueError, "unknown content: " & ticket.contentId)
  let minDeposit =
    case ticket.channel
    of rcBlue:
      state.params.blueReviewDeposit
    of rcCommunity:
      state.params.reviewDeposit
    else:
      0'u64
  if ticket.depositLocked < minDeposit:
    raise newException(ValueError, "insufficient report deposit")
  state.reports[ticket.reportId] = ticket
  var record = state.contents[ticket.contentId]
  record.reportIds.add(ticket.reportId)
  let severe = ticket.category.toLowerAscii() in ["illegal", "terror", "porn", "fraud"]
  let classification = if severe: mcC0 elif ticket.channel == rcRed: mcC1 else: mcC2
  let action = if severe: maBlockAndPreserve else: maHoldForProReview
  let tags =
    if severe: @[dtRequireProReview]
    elif ticket.channel == rcBlue: @[dtCommercialRisk, dtRequireProReview]
    else: @[dtRequireProReview]
  let caseId = hashHex(ticket.contentId & ":" & ticket.reportId)
  let caseItem = ModerationCase(
    caseId: caseId,
    contentId: ticket.contentId,
    openedBy: ticket.reporter,
    channel: ticket.channel,
    classification: classification,
    status: mcsOpen,
    decision: action,
    labels: tags,
    reason: ticket.reason,
    reviewers: @[],
    updatedAt: ticket.createdAt,
  )
  state.moderationCases[caseId] = caseItem
  record.moderationCaseId = caseId
  record.distribution = buildDecision(
    record.contentId,
    classification,
    action,
    tags,
    "report_opened",
    ticket.createdAt,
    latestActiveTombstone(state, record.contentId).isSome(),
  )
  record.updatedAt = ticket.createdAt
  state.contents[record.contentId] = record

proc applyReview*(
    state: ChainState,
    contentId: string,
    caseId: string,
    reviewer: string,
    classification: ModerationClass,
    action: ModerationAction,
    tags: seq[DistributionTag],
    reason: string,
    at: int64,
) =
  if contentId notin state.contents:
    raise newException(ValueError, "unknown content: " & contentId)
  var record = state.contents[contentId]
  var caseItem =
    if caseId.len > 0 and caseId in state.moderationCases:
      state.moderationCases[caseId]
    else:
      ModerationCase(
        caseId: hashHex(contentId & ":" & reviewer & ":" & $at),
        contentId: contentId,
        openedBy: reviewer,
        channel: rcProfessional,
        classification: classification,
        status: mcsOpen,
        decision: action,
        labels: @[],
        reason: "",
        reviewers: @[],
        updatedAt: at,
      )
  if caseItem.channel in {rcBlue, rcCommunity}:
    if classification in {mcC0, mcC1}:
      raise newException(ValueError, "community review cannot classify legal-risk content")
    if action == maBlockAndPreserve:
      raise newException(ValueError, "community review cannot issue block decisions")
  caseItem.classification = classification
  caseItem.decision = action
  caseItem.labels = dedupeTags(tags)
  caseItem.status = mcsResolved
  caseItem.reason = reason
  caseItem.updatedAt = at
  if reviewer notin caseItem.reviewers:
    caseItem.reviewers.add(reviewer)
  state.moderationCases[caseItem.caseId] = caseItem
  record.moderationCaseId = caseItem.caseId
  record.distribution = buildDecision(
    contentId,
    classification,
    action,
    caseItem.labels,
    reason,
    at,
    latestActiveTombstone(state, record.contentId).isSome(),
  )
  record.updatedAt = at
  state.contents[contentId] = record

proc applyTombstoneIssue*(
    state: ChainState,
    contentId: string,
    tombstoneId: string,
    issuedBy: string,
    reason: string,
    scope: string,
    reasonCode: string,
    basisType: string,
    executionReceipt: string,
    restoreAllowed: bool,
    at: int64,
) =
  if contentId notin state.contents:
    raise newException(ValueError, "unknown content: " & contentId)
  if reason.len == 0:
    raise newException(ValueError, "tombstone reason is required")
  let item = TombstoneRecord(
    tombstoneId: tombstoneId,
    contentId: contentId,
    contentHash: state.contents[contentId].manifest.contentHash,
    scope: if scope.len > 0: scope else: "content_visibility",
    reasonCode: if reasonCode.len > 0: reasonCode else: reason,
    issuedBy: issuedBy,
    reason: reason,
    basisType: if basisType.len > 0: basisType else: "platform_rule",
    executionReceipt: executionReceipt,
    issuedAt: at,
    restoredAt: 0,
    active: true,
    restoreAllowed: restoreAllowed,
    restoreDecisionId: "",
  )
  state.tombstones[tombstoneId] = item
  var record = state.contents[contentId]
  record.tombstoneId = tombstoneId
  record.distribution = buildDecision(
    contentId,
    record.distribution.moderationClass,
    record.distribution.moderationAction,
    record.distribution.tags,
    "tombstoned:" & reason,
    at,
    tombstoned = true,
  )
  record.updatedAt = at
  state.contents[contentId] = record

proc applyTombstoneRestore*(
    state: ChainState,
    contentId: string,
    restoreDecisionId: string,
    at: int64,
) =
  if contentId notin state.contents:
    raise newException(ValueError, "unknown content: " & contentId)
  let tombstoneOpt = latestActiveTombstone(state, contentId)
  if tombstoneOpt.isSome():
    var tombstone = tombstoneOpt.get()
    if not tombstone.restoreAllowed:
      raise newException(ValueError, "tombstone restore is not allowed")
    tombstone.active = false
    tombstone.restoredAt = at
    tombstone.restoreDecisionId = restoreDecisionId
    state.tombstones[tombstone.tombstoneId] = tombstone
  var record = state.contents[contentId]
  record.tombstoneId = ""
  record.distribution = buildDecision(
    contentId,
    record.distribution.moderationClass,
    record.distribution.moderationAction,
    record.distribution.tags,
    "restored",
    at,
  )
  record.updatedAt = at
  state.contents[contentId] = record

proc publishTaskSnapshot*(record: ContentRecord): PublishTaskSnapshot =
  PublishTaskSnapshot(
    contentId: record.contentId,
    author: record.author,
    manifestCid: record.manifest.manifestCid,
    moderationClass: record.distribution.moderationClass,
    moderationDecision: record.distribution.moderationAction,
    visibleInFeed: record.distribution.visibleInFeed and record.tombstoneId.len == 0,
    visibleInDetail: record.distribution.visibleInDetail and record.tombstoneId.len == 0,
    reason: record.distribution.reason,
    updatedAt: record.updatedAt,
  )

proc socialFeedSnapshot*(state: ChainState, limit = 50): seq[SocialFeedItemSnapshot] =
  var items: seq[ContentRecord] = @[]
  for _, record in state.contents.pairs:
    if record.distribution.visibleInFeed and record.tombstoneId.len == 0:
      items.add(record)
  items.sort(proc(a, b: ContentRecord): int = cmp(b.createdAt, a.createdAt))
  for idx, record in items:
    if limit > 0 and idx >= limit:
      break
    result.add(SocialFeedItemSnapshot(
      contentId: record.contentId,
      author: record.author,
      title: record.title,
      manifestCid: record.manifest.manifestCid,
      previewCid: record.manifest.previewCid,
      tags: record.distribution.tags,
      createdAt: record.createdAt,
    ))

proc reportList*(state: ChainState, contentId = ""): seq[ReportTicket] =
  for _, item in state.reports.pairs:
    if contentId.len == 0 or item.contentId == contentId:
      result.add(item)
  result.sort(proc(a, b: ReportTicket): int = cmp(b.createdAt, a.createdAt))

proc tombstoneForContent*(state: ChainState, contentId: string): Option[TombstoneRecord] =
  latestActiveTombstone(state, contentId)

proc socialContentDetailSnapshot*(state: ChainState, contentId: string): SocialContentDetailSnapshot =
  if contentId notin state.contents:
    raise newException(ValueError, "unknown content: " & contentId)
  let record = state.contents[contentId]
  let caseOpt =
    if record.moderationCaseId.len > 0 and record.moderationCaseId in state.moderationCases:
      some(state.moderationCases[record.moderationCaseId])
    else:
      none(ModerationCase)
  result = SocialContentDetailSnapshot(
    contentId: record.contentId,
    author: record.author,
    title: record.title,
    body: record.body,
    manifest: record.manifest,
    sourceGraph: record.sourceGraph,
    labels: record.labels,
    reports: state.reportList(contentId),
    moderationCase: caseOpt,
    distribution: record.distribution,
    tombstone: state.tombstoneForContent(contentId),
    visibleInDetail: record.distribution.visibleInDetail and record.tombstoneId.len == 0,
  )
