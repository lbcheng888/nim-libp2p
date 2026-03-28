import std/[options, tables]

import ./state
import ../types

const
  BasketAssetOrder = ["BTC", "GOLD", "HKD_STABLE"]

proc validateBasketWeights(weights: seq[uint32]) =
  if weights.len == 0:
    return
  if weights.len != BasketAssetOrder.len:
    raise newException(ValueError, "basketWeights must contain exactly 3 legs")
  var total = 0'u64
  for weight in weights:
    if weight == 0:
      raise newException(ValueError, "basketWeights must be positive")
    total += weight.uint64
  if total != 10_000'u64:
    raise newException(ValueError, "basketWeights must sum to 10000")

proc attestedNavUsd(state: ChainState, snapshot: CustodySnapshot): uint64 =
  if snapshot.totalCustodyUsd < snapshot.totalLiabilitiesUsd:
    raise newException(ValueError, "custody snapshot liabilities exceed custody")
  let haircut =
    (snapshot.goldPendingUsd * state.params.goldPendingHaircutBp.uint64) div 10_000'u64
  let effectiveCustody = snapshot.totalCustodyUsd - min(snapshot.totalCustodyUsd, haircut)
  if effectiveCustody <= snapshot.totalLiabilitiesUsd:
    return 0
  effectiveCustody - snapshot.totalLiabilitiesUsd

proc applyMintIntent*(state: ChainState, intent: MintIntent) =
  if state.mintIntents.hasKey(intent.mintIntentId):
    raise newException(ValueError, "duplicate mint intent: " & intent.mintIntentId)
  if intent.userAccount.len == 0:
    raise newException(ValueError, "mint intent userAccount is required")
  if intent.inAsset notin ["USDT", "USDC"]:
    raise newException(ValueError, "unsupported mint asset: " & intent.inAsset)
  if intent.inAmount == 0:
    raise newException(ValueError, "mint intent amount must be positive")
  if not intent.ackIrreversible:
    raise newException(ValueError, "mint intent must acknowledge irreversible execution")
  if intent.basketPolicy.len == 0:
    raise newException(ValueError, "basketPolicy is required")
  validateBasketWeights(intent.basketWeights)
  state.mintIntents[intent.mintIntentId] = intent

proc applyExecutionReceipt*(state: ChainState, receipt: ExecutionReceipt) =
  if state.executionReceipts.hasKey(receipt.receiptId):
    raise newException(ValueError, "duplicate execution receipt: " & receipt.receiptId)
  if not state.mintIntents.hasKey(receipt.mintIntentId):
    raise newException(ValueError, "unknown mint intent: " & receipt.mintIntentId)
  if receipt.legIndex < 0 or receipt.legIndex >= BasketAssetOrder.len:
    raise newException(ValueError, "invalid execution leg index")
  if receipt.assetOut.len == 0 or receipt.venue.len == 0 or receipt.evidenceHash.len == 0:
    raise newException(ValueError, "execution receipt is incomplete")
  if receipt.amountOut == 0:
    raise newException(ValueError, "execution receipt amountOut must be positive")
  var intent = state.mintIntents[receipt.mintIntentId]
  if intent.status notin {misIntentCreated, misFundsReceived, misDexExecuting, misDexSettled}:
    raise newException(ValueError, "mint intent is not accepting execution receipts")
  for existingId in intent.executionReceiptIds:
    if existingId notin state.executionReceipts:
      continue
    let existing = state.executionReceipts[existingId]
    if existing.legIndex == receipt.legIndex:
      raise newException(ValueError, "duplicate execution leg")
    if existing.assetOut == receipt.assetOut:
      raise newException(ValueError, "duplicate execution asset")
  state.executionReceipts[receipt.receiptId] = receipt
  if receipt.receiptId notin intent.executionReceiptIds:
    intent.executionReceiptIds.add(receipt.receiptId)
  if intent.executionReceiptIds.len >= BasketAssetOrder.len:
    intent.status = misDexSettled
  elif intent.executionReceiptIds.len > 0:
    intent.status = misDexExecuting
  state.mintIntents[intent.mintIntentId] = intent

proc applyCustodySnapshot*(state: ChainState, snapshot: CustodySnapshot) =
  if snapshot.snapshotId.len == 0:
    raise newException(ValueError, "custody snapshot id is required")
  if snapshot.proofHash.len == 0:
    raise newException(ValueError, "custody snapshot proofHash is required")
  if snapshot.signatures.len == 0:
    raise newException(ValueError, "custody snapshot signatures are required")
  if snapshot.goldAllocatedUsd + snapshot.goldPendingUsd > snapshot.totalCustodyUsd:
    raise newException(ValueError, "custody snapshot gold legs exceed total custody")
  discard state.attestedNavUsd(snapshot)
  for intentId in snapshot.coveredIntentIds:
    if not state.mintIntents.hasKey(intentId):
      raise newException(ValueError, "unknown mint intent in custody snapshot: " & intentId)
    let intent = state.mintIntents[intentId]
    if intent.executionReceiptIds.len != BasketAssetOrder.len or intent.status notin {misDexSettled, misCustodyPending, misCustodyAttested}:
      raise newException(ValueError, "custody snapshot requires fully settled execution")
  state.custodySnapshots[snapshot.snapshotId] = snapshot
  for intentId in snapshot.coveredIntentIds:
    if state.mintIntents.hasKey(intentId):
      var intent = state.mintIntents[intentId]
      intent.custodySnapshotId = snapshot.snapshotId
      intent.status = misCustodyPending
      state.mintIntents[intentId] = intent

proc applyReserveProof*(state: ChainState, proof: ReserveProof) =
  if proof.reserveProofHash.len == 0:
    raise newException(ValueError, "reserve proof hash is required")
  if proof.signatures.len == 0:
    raise newException(ValueError, "reserve proof signatures are required")
  if proof.custodySnapshotIds.len == 0:
    raise newException(ValueError, "reserve proof must reference custody snapshots")
  var attestedCoverage = 0'u64
  for snapshotId in proof.custodySnapshotIds:
    if not state.custodySnapshots.hasKey(snapshotId):
      raise newException(ValueError, "unknown custody snapshot: " & snapshotId)
    let snapshot = state.custodySnapshots[snapshotId]
    if snapshot.epochId != proof.epochId:
      raise newException(ValueError, "reserve proof epoch mismatch")
    attestedCoverage += state.attestedNavUsd(snapshot)
  if proof.netNavDeltaUsd == 0:
    raise newException(ValueError, "reserve proof netNavDeltaUsd must be positive")
  if proof.netNavDeltaUsd > attestedCoverage:
    raise newException(ValueError, "reserve proof exceeds attested custody")
  for intentId in proof.coveredIntentIds:
    if not state.mintIntents.hasKey(intentId):
      raise newException(ValueError, "unknown mint intent in reserve proof: " & intentId)
    let intent = state.mintIntents[intentId]
    if intent.custodySnapshotId.len == 0:
      raise newException(ValueError, "reserve proof requires custody snapshot")
    if intent.custodySnapshotId notin proof.custodySnapshotIds:
      raise newException(ValueError, "reserve proof does not cover custody snapshot")
  state.reserveProofs[proof.reserveProofHash] = proof
  for intentId in proof.coveredIntentIds:
    if state.mintIntents.hasKey(intentId):
      var intent = state.mintIntents[intentId]
      intent.reserveProofHash = proof.reserveProofHash
      intent.status = misCustodyAttested
      state.mintIntents[intentId] = intent

proc applyMintFinalize*(
    state: ChainState,
    mintIntentId: string,
    reserveProofHash: string,
    amount: uint64,
    toAccount: string,
) =
  if not state.mintIntents.hasKey(mintIntentId):
    raise newException(ValueError, "unknown mint intent: " & mintIntentId)
  if not state.reserveProofs.hasKey(reserveProofHash):
    raise newException(ValueError, "unknown reserve proof: " & reserveProofHash)
  let proof = state.reserveProofs[reserveProofHash]
  if mintIntentId notin proof.coveredIntentIds:
    raise newException(ValueError, "reserve proof does not cover intent")
  var intent = state.mintIntents[mintIntentId]
  if intent.status != misCustodyAttested:
    raise newException(ValueError, "mint intent is not attested")
  if intent.reserveProofHash.len > 0 and intent.reserveProofHash != reserveProofHash:
    raise newException(ValueError, "reserve proof mismatch")
  if amount == 0:
    raise newException(ValueError, "mint amount must be positive")
  if intent.custodySnapshotId.len == 0 or not state.custodySnapshots.hasKey(intent.custodySnapshotId):
    raise newException(ValueError, "mint intent is missing custody snapshot")
  let snapshot = state.custodySnapshots[intent.custodySnapshotId]
  if intent.custodySnapshotId notin proof.custodySnapshotIds:
    raise newException(ValueError, "reserve proof does not cover custody snapshot")
  if mintIntentId notin snapshot.coveredIntentIds:
    raise newException(ValueError, "custody snapshot does not cover intent")
  if proof.netNavDeltaUsd > 0 and state.attestedNavUsd(snapshot) == 0:
    raise newException(ValueError, "custody snapshot has no attested nav")
  intent.reserveProofHash = reserveProofHash
  intent.mintedAmount = amount
  intent.status = misMinted
  state.mintIntents[mintIntentId] = intent
  state.adjustRwadBalance(toAccount, int64(amount))
  state.totalRwadSupply += amount

proc mintIntentStatusSnapshot*(state: ChainState, mintIntentId: string): MintIntentStatus =
  if not state.mintIntents.hasKey(mintIntentId):
    raise newException(ValueError, "unknown mint intent: " & mintIntentId)
  let intent = state.mintIntents[mintIntentId]
  var receipts: seq[ExecutionReceipt] = @[]
  for receiptId in intent.executionReceiptIds:
    if state.executionReceipts.hasKey(receiptId):
      receipts.add(state.executionReceipts[receiptId])
  let custody =
    if intent.custodySnapshotId.len > 0 and state.custodySnapshots.hasKey(intent.custodySnapshotId):
      some(state.custodySnapshots[intent.custodySnapshotId])
    else:
      none(CustodySnapshot)
  let proof =
    if intent.reserveProofHash.len > 0 and state.reserveProofs.hasKey(intent.reserveProofHash):
      some(state.reserveProofs[intent.reserveProofHash])
    else:
      none(ReserveProof)
  MintIntentStatus(
    mintIntentId: intent.mintIntentId,
    userAccount: intent.userAccount,
    status: intent.status,
    reserveProofHash: intent.reserveProofHash,
    mintedAmount: intent.mintedAmount,
    failureReason: intent.failureReason,
    executionReceipts: receipts,
    custodySnapshot: custody,
    reserveProof: proof,
  )
