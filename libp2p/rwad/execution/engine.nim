import std/[json, jsonutils, options, sequtils, strutils, tables]

import ../codec
import ../identity
import ../types
import ./content
import ./credit
import ./name
import ./staking
import ./state
import ./treasury

proc jStr(payload: JsonNode, key: string, default = ""): string =
  if payload.kind == JObject and payload.hasKey(key):
    payload[key].getStr(default)
  else:
    default

proc jBool(payload: JsonNode, key: string, default = false): bool =
  if payload.kind == JObject and payload.hasKey(key):
    payload[key].getBool(default)
  else:
    default

proc jInt(payload: JsonNode, key: string, default = 0'i64): int64 =
  if payload.kind == JObject and payload.hasKey(key):
    payload[key].getInt(default)
  else:
    default

proc jU64(payload: JsonNode, key: string, default = 0'u64): uint64 =
  uint64(max(0'i64, jInt(payload, key, default.int64)))

proc jU32(payload: JsonNode, key: string, default = 0'u32): uint32 =
  uint32(max(0'i64, jInt(payload, key, default.int64)))

proc jStringSeq(payload: JsonNode, key: string): seq[string] =
  if payload.kind == JObject and payload.hasKey(key) and payload[key].kind == JArray:
    for item in payload[key].items:
      result.add(item.getStr())

proc parseReviewChannel(value: string): ReviewChannel =
  case value.toLowerAscii()
  of "red":
    rcRed
  of "blue":
    rcBlue
  of "community":
    rcCommunity
  of "professional", "pro":
    rcProfessional
  else:
    rcStep0

proc parseModerationClass(value: string): ModerationClass =
  case value.toUpperAscii()
  of "C0":
    mcC0
  of "C1":
    mcC1
  of "C2":
    mcC2
  else:
    mcC3

proc parseModerationAction(value: string): ModerationAction =
  case value.toUpperAscii()
  of "BLOCK_AND_PRESERVE", "BLOCK":
    maBlockAndPreserve
  of "HOLD_FOR_PRO_REVIEW", "HOLD":
    maHoldForProReview
  else:
    maPassToPool

proc parseDistributionTag(value: string): DistributionTag =
  case value.toUpperAscii()
  of "LOW_DISTRIBUTION":
    dtLowDistribution
  of "COMMERCIAL_RISK":
    dtCommercialRisk
  of "SPAM_OR_DUPLICATE":
    dtSpamOrDuplicate
  of "MISLEADING":
    dtMisleading
  of "REQUIRE_PRO_REVIEW":
    dtRequireProReview
  else:
    dtSafeForHome

proc parseTags(payload: JsonNode, key = "tags"): seq[DistributionTag] =
  for item in jStringSeq(payload, key):
    result.add(parseDistributionTag(item))

proc event(category, subjectId, action: string, payload: JsonNode, createdAt: int64): AuditEvent =
  result = AuditEvent(
    eventId: "",
    category: category,
    subjectId: subjectId,
    action: action,
    payload: payload,
    createdAt: createdAt,
  )
  result.eventId = computeEventId(result)

proc coerceObj[T](payload: JsonNode, key = ""): T =
  if key.len > 0 and payload.kind == JObject and payload.hasKey(key):
    jsonTo(payload[key], T)
  else:
    jsonTo(payload, T)

proc validateNonce*(state: ChainState, tx: Tx) =
  let expected = state.nonceOf(tx.sender) + 1
  if tx.nonce != expected:
    raise newException(ValueError, "nonce mismatch for " & tx.sender &
      ": expected " & $expected & " got " & $tx.nonce)

proc applyTx*(state: ChainState, tx: Tx): seq[AuditEvent] =
  if not verifyTx(tx):
    raise newException(ValueError, "invalid tx signature: " & tx.txId)
  state.validateNonce(tx)

  case tx.kind
  of txContentPublishIntent:
    var intent = coerceObj[PublishIntent](tx.payload, "intent")
    intent.author = tx.sender
    if intent.contentId.len == 0:
      intent.contentId = tx.txId
    if intent.createdAt == 0:
      intent.createdAt = tx.timestamp
    let sourceGraph =
      if tx.payload.kind == JObject and tx.payload.hasKey("sourceGraph"):
        some(coerceObj[SourceGraph](tx.payload, "sourceGraph"))
      else:
        none(SourceGraph)
    let labels =
      if tx.payload.kind == JObject and tx.payload.hasKey("labels"):
        jsonTo(tx.payload["labels"], seq[LabelClaim])
      else:
        @[]
    let riskSignals = jStringSeq(tx.payload, "riskSignals")
    state.applyContentPublish(intent, sourceGraph.get(SourceGraph()), labels, riskSignals)
    result.add(event("content", intent.contentId, "publish_intent", tx.payload, tx.timestamp))
  of txContentManifestCommit:
    let contentId = jStr(tx.payload, "contentId")
    let manifest = coerceObj[ContentManifest](tx.payload, "manifest")
    let sourceGraph =
      if tx.payload.kind == JObject and tx.payload.hasKey("sourceGraph"):
        some(coerceObj[SourceGraph](tx.payload, "sourceGraph"))
      else:
        none(SourceGraph)
    let labels =
      if tx.payload.kind == JObject and tx.payload.hasKey("labels"):
        jsonTo(tx.payload["labels"], seq[LabelClaim])
      else:
        @[]
    state.applyManifestCommit(contentId, manifest, sourceGraph, labels, tx.timestamp)
    result.add(event("content", contentId, "manifest_commit", tx.payload, tx.timestamp))
  of txContentReportSubmit:
    let contentId = jStr(tx.payload, "contentId")
    let ticket = ReportTicket(
      reportId: if jStr(tx.payload, "reportId").len > 0: jStr(tx.payload, "reportId")
                else: hashHex(contentId & ":" & tx.sender & ":" & $tx.timestamp),
      contentId: contentId,
      reporter: tx.sender,
      channel: parseReviewChannel(jStr(tx.payload, "channel", "blue")),
      category: jStr(tx.payload, "category", "community"),
      reason: jStr(tx.payload, "reason"),
      evidenceHash: jStr(tx.payload, "evidenceHash"),
      createdAt: tx.timestamp,
      depositLocked: jU64(tx.payload, "depositLocked"),
    )
    state.applyReport(ticket)
    result.add(event("governance", ticket.reportId, "report_submit", tx.payload, tx.timestamp))
  of txContentReviewDecide:
    let contentId = jStr(tx.payload, "contentId")
    state.applyReview(
      contentId = contentId,
      caseId = jStr(tx.payload, "caseId"),
      reviewer = tx.sender,
      classification = parseModerationClass(jStr(tx.payload, "classification", "C3")),
      action = parseModerationAction(jStr(tx.payload, "decision", "PASS_TO_POOL")),
      tags = parseTags(tx.payload),
      reason = jStr(tx.payload, "reason"),
      at = tx.timestamp,
    )
    result.add(event("governance", contentId, "review_decide", tx.payload, tx.timestamp))
  of txContentTombstoneIssue:
    let contentId = jStr(tx.payload, "contentId")
    let tombstoneId =
      if jStr(tx.payload, "tombstoneId").len > 0: jStr(tx.payload, "tombstoneId")
      else: hashHex(contentId & ":" & tx.sender & ":" & $tx.timestamp)
    state.applyTombstoneIssue(
      contentId = contentId,
      tombstoneId = tombstoneId,
      issuedBy = tx.sender,
      reason = jStr(tx.payload, "reason"),
      scope = jStr(tx.payload, "scope"),
      reasonCode = jStr(tx.payload, "reasonCode"),
      basisType = jStr(tx.payload, "basisType"),
      executionReceipt = jStr(tx.payload, "executionReceipt"),
      restoreAllowed = jBool(tx.payload, "restoreAllowed", true),
      at = tx.timestamp,
    )
    result.add(event("governance", tombstoneId, "tombstone_issue", tx.payload, tx.timestamp))
  of txContentTombstoneRestore:
    let contentId = jStr(tx.payload, "contentId")
    state.applyTombstoneRestore(contentId, jStr(tx.payload, "restoreDecisionId"), tx.timestamp)
    result.add(event("governance", contentId, "tombstone_restore", tx.payload, tx.timestamp))
  of txCreditIssue:
    let position = CreditPosition(
      positionId: if jStr(tx.payload, "positionId").len > 0: jStr(tx.payload, "positionId")
                  else: hashHex(tx.txId & ":credit"),
      owner: if jStr(tx.payload, "owner").len > 0: jStr(tx.payload, "owner") else: tx.sender,
      sourceType: jStr(tx.payload, "sourceType", "content"),
      sourceId: jStr(tx.payload, "sourceId"),
      amount: jU64(tx.payload, "amount"),
      available: jU64(tx.payload, "amount"),
      issuedAt: tx.timestamp,
      expiresAt: jInt(tx.payload, "expiresAt"),
      convertibleAfter: jInt(tx.payload, "convertibleAfter"),
      budgetEpoch: jU64(tx.payload, "budgetEpoch"),
      status: csOpen,
      freezeReason: "",
    )
    state.applyCreditIssue(position)
    result.add(event("credit", position.positionId, "credit_issue", tx.payload, tx.timestamp))
  of txCreditFreeze:
    let positionId = jStr(tx.payload, "positionId")
    state.applyCreditFreeze(positionId, jStr(tx.payload, "reason"))
    result.add(event("credit", positionId, "credit_freeze", tx.payload, tx.timestamp))
  of txCreditExpire:
    let positionId = jStr(tx.payload, "positionId")
    state.applyCreditExpire(positionId)
    result.add(event("credit", positionId, "credit_expire", tx.payload, tx.timestamp))
  of txCreditConvert:
    let positionId = jStr(tx.payload, "positionId")
    state.applyCreditConvert(positionId, jU64(tx.payload, "amount"))
    result.add(event("credit", positionId, "credit_convert", tx.payload, tx.timestamp))
  of txTreasuryMintIntentCreate:
    let intent = MintIntent(
      mintIntentId: if jStr(tx.payload, "mintIntentId").len > 0: jStr(tx.payload, "mintIntentId")
                    else: hashHex(tx.txId & ":mint"),
      userAccount: if jStr(tx.payload, "userAccount").len > 0: jStr(tx.payload, "userAccount") else: tx.sender,
      inAsset: jStr(tx.payload, "inAsset", "USDT"),
      inAmount: jU64(tx.payload, "inAmount"),
      basketPolicy: jStr(tx.payload, "basketPolicy", "ETF_1_1_1"),
      basketWeights: jStringSeq(tx.payload, "basketWeights").mapIt(uint32(parseInt(it))),
      maxSlippageBp: jU32(tx.payload, "maxSlippageBp"),
      ackIrreversible: jBool(tx.payload, "ackIrreversible", true),
      createdAt: tx.timestamp,
      depositId: jStr(tx.payload, "depositId"),
      executionReceiptIds: @[],
      custodySnapshotId: "",
      reserveProofHash: "",
      mintedAmount: 0,
      status: misIntentCreated,
      failureReason: "",
    )
    state.applyMintIntent(intent)
    result.add(event("treasury", intent.mintIntentId, "mint_intent_create", tx.payload, tx.timestamp))
  of txTreasuryExecutionReceiptSubmit:
    let receipt = ExecutionReceipt(
      receiptId: if jStr(tx.payload, "receiptId").len > 0: jStr(tx.payload, "receiptId")
                 else: hashHex(tx.txId & ":receipt"),
      mintIntentId: jStr(tx.payload, "mintIntentId"),
      legIndex: int(jInt(tx.payload, "legIndex")),
      assetOut: jStr(tx.payload, "assetOut"),
      amountOut: jU64(tx.payload, "amountOut"),
      avgPxUsd: jU64(tx.payload, "avgPxUsd"),
      venue: jStr(tx.payload, "venue"),
      evidenceHash: jStr(tx.payload, "evidenceHash"),
      settledAt: tx.timestamp,
    )
    state.applyExecutionReceipt(receipt)
    result.add(event("treasury", receipt.receiptId, "execution_receipt_submit", tx.payload, tx.timestamp))
  of txTreasuryCustodySnapshotSubmit:
    var snapshot = coerceObj[CustodySnapshot](tx.payload, "snapshot")
    if snapshot.snapshotId.len == 0:
      snapshot.snapshotId = hashHex(tx.txId & ":custody")
    if snapshot.createdAt == 0:
      snapshot.createdAt = tx.timestamp
    state.applyCustodySnapshot(snapshot)
    result.add(event("treasury", snapshot.snapshotId, "custody_snapshot_submit", tx.payload, tx.timestamp))
  of txTreasuryReserveProofSubmit:
    var proof = coerceObj[ReserveProof](tx.payload, "proof")
    if proof.reserveProofHash.len == 0:
      proof.reserveProofHash = hashHex(tx.txId & ":reserve")
    if proof.submittedAt == 0:
      proof.submittedAt = tx.timestamp
    state.applyReserveProof(proof)
    result.add(event("treasury", proof.reserveProofHash, "reserve_proof_submit", tx.payload, tx.timestamp))
  of txRwadMintFinalize:
    let mintIntentId = jStr(tx.payload, "mintIntentId")
    let toAccount =
      if jStr(tx.payload, "toAccount").len > 0: jStr(tx.payload, "toAccount")
      elif mintIntentId in state.mintIntents: state.mintIntents[mintIntentId].userAccount
      else: tx.sender
    state.applyMintFinalize(
      mintIntentId,
      jStr(tx.payload, "reserveProofHash"),
      jU64(tx.payload, "amount"),
      toAccount,
    )
    result.add(event("treasury", mintIntentId, "rwad_mint_finalize", tx.payload, tx.timestamp))
  of txNameRegister:
    state.applyNameRegister(
      name = jStr(tx.payload, "name"),
      owner = tx.sender,
      targetAccount = if jStr(tx.payload, "targetAccount").len > 0: jStr(tx.payload, "targetAccount") else: tx.sender,
      targetContentId = jStr(tx.payload, "targetContentId"),
      resolvedCid = jStr(tx.payload, "resolvedCid"),
      nowTs = tx.timestamp,
      ttlSeconds = jInt(tx.payload, "ttlSeconds"),
    )
    result.add(event("name", normalizeName(jStr(tx.payload, "name")), "name_register", tx.payload, tx.timestamp))
  of txNameRenew:
    let name = jStr(tx.payload, "name")
    state.applyNameRenew(name, tx.sender, tx.timestamp, jInt(tx.payload, "ttlSeconds"))
    result.add(event("name", normalizeName(name), "name_renew", tx.payload, tx.timestamp))
  of txNameTransfer:
    let name = jStr(tx.payload, "name")
    state.applyNameTransfer(
      name,
      tx.sender,
      jStr(tx.payload, "newOwner"),
      jStr(tx.payload, "targetAccount"),
      tx.timestamp,
    )
    result.add(event("name", normalizeName(name), "name_transfer", tx.payload, tx.timestamp))
  of txStake:
    state.applyStake(tx.sender, jStr(tx.payload, "validator", tx.sender), jU64(tx.payload, "amount"), tx.timestamp)
    result.add(event("staking", tx.sender, "stake", tx.payload, tx.timestamp))
  of txDelegate:
    state.applyDelegate(tx.sender, jStr(tx.payload, "validator"), jU64(tx.payload, "amount"), tx.timestamp)
    result.add(event("staking", tx.sender, "delegate", tx.payload, tx.timestamp))
  of txValidatorRegister:
    state.applyValidatorRegister(
      address = tx.sender,
      peerId = jStr(tx.payload, "peerId"),
      publicKey = if jStr(tx.payload, "publicKey").len > 0: jStr(tx.payload, "publicKey") else: tx.senderPublicKey,
      stake = jU64(tx.payload, "stake"),
      at = tx.timestamp,
    )
    result.add(event("staking", tx.sender, "validator_register", tx.payload, tx.timestamp))
  of txSlash:
    state.applySlash(
      offender = jStr(tx.payload, "offender"),
      reason = parseSlashingReason(jStr(tx.payload, "reason")),
      amount = jU64(tx.payload, "amount"),
      at = tx.timestamp,
    )
    result.add(event("staking", jStr(tx.payload, "offender"), "slash", tx.payload, tx.timestamp))
  of txParamUpdate:
    let params = coerceObj[ChainParams](tx.payload, "params")
    state.applyParamUpdate(params)
    result.add(event("chain", params.chainId, "param_update", tx.payload, tx.timestamp))

  state.setNonce(tx.sender, tx.nonce)
  state.refreshStateRoot()

proc applyBatch*(state: ChainState, batch: Batch): seq[AuditEvent] =
  for tx in batch.transactions:
    result.add(state.applyTx(tx))

proc applyBatchesToHeight*(
    state: ChainState,
    batches: seq[Batch],
    height: uint64,
    epoch: uint64,
    blockId: string,
): seq[AuditEvent] =
  for batch in batches:
    result.add(state.applyBatch(batch))
  state.height = height
  state.epoch = epoch
  state.lastBlockId = blockId
  state.refreshStateRoot()
