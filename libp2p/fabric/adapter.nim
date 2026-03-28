import std/[algorithm, json, options, sequtils, tables]

import ../rwad/codec
import ../rwad/execution/[content, credit, engine as rwad_engine, name, staking, state, treasury]
import ../rwad/identity
import ../rwad/types
import ./types

const
  LedgerContractRoot* = "fabric://system/ledger/" & "26A0F1D3A1EF8D67"
  ContentContractRoot* = "fabric://system/content/" & "1E2F4A3B6C7D8E90"
  CreditContractRoot* = "fabric://system/credit/" & "91A2B3C4D5E6F708"
  TreasuryContractRoot* = "fabric://system/treasury/" & "A1B2C3D4E5F60718"
  NameContractRoot* = "fabric://system/name/" & "BC30D9A14F1206C8"
  StakingContractRoot* = "fabric://system/staking/" & "CF31A29E76D405B1"
  GovernanceContractRoot* = "fabric://system/governance/" & "D9210AF43BC587E1"
  AvoContractRoot* = "fabric://system/avo/" & "EE1135B2A98C7014"

proc allSystemContractRoots*(): seq[string] =
  @[
    LedgerContractRoot,
    ContentContractRoot,
    CreditContractRoot,
    TreasuryContractRoot,
    NameContractRoot,
    StakingContractRoot,
    GovernanceContractRoot,
    AvoContractRoot,
  ]

proc jStr(payload: JsonNode, key: string, default = ""): string =
  if payload.kind == JObject and payload.hasKey(key):
    payload[key].getStr(default)
  else:
    default

proc jStringSeq(payload: JsonNode, key: string): seq[string] =
  if payload.kind == JObject and payload.hasKey(key) and payload[key].kind == JArray:
    for item in payload[key].items:
      result.add(item.getStr())

proc sortedUnique(items: openArray[string]): seq[string] =
  for item in items:
    if item.len > 0 and item notin result:
      result.add(item)
  result.sort(system.cmp[string])

proc contractRootFor*(kind: TxKind): string =
  case kind
  of txContentPublishIntent, txContentManifestCommit:
    ContentContractRoot
  of txContentReportSubmit, txContentReviewDecide, txContentTombstoneIssue, txContentTombstoneRestore, txParamUpdate:
    GovernanceContractRoot
  of txCreditIssue, txCreditFreeze, txCreditExpire, txCreditConvert:
    CreditContractRoot
  of txTreasuryMintIntentCreate, txTreasuryExecutionReceiptSubmit,
      txTreasuryCustodySnapshotSubmit, txTreasuryReserveProofSubmit, txRwadMintFinalize:
    TreasuryContractRoot
  of txNameRegister, txNameRenew, txNameTransfer:
    NameContractRoot
  of txStake, txDelegate, txValidatorRegister, txSlash:
    StakingContractRoot

proc entrypointFor*(kind: TxKind): string =
  slug(kind)

proc lookupCreditOwner(state: ChainState, positionId, defaultOwner: string): string =
  if positionId.len > 0 and state.creditPositions.hasKey(positionId):
    return state.creditPositions[positionId].owner
  defaultOwner

proc lookupMintUsers(state: ChainState, mintIntentIds: openArray[string]): seq[string] =
  for intentId in mintIntentIds:
    if intentId.len > 0 and state.mintIntents.hasKey(intentId):
      let user = state.mintIntents[intentId].userAccount
      if user.len > 0 and user notin result:
        result.add(user)

proc lookupMintUser(state: ChainState, mintIntentId, defaultUser: string): string =
  if mintIntentId.len > 0 and state.mintIntents.hasKey(mintIntentId):
    return state.mintIntents[mintIntentId].userAccount
  defaultUser

proc lookupNameOwner(state: ChainState, rawName, defaultOwner: string): string =
  let name = normalizeName(rawName)
  if name.len > 0 and state.nameRecords.hasKey(name):
    return state.nameRecords[name].owner
  defaultOwner

proc descriptorFor*(state: ChainState, tx: Tx): SystemContractDescriptor =
  result.contractRoot = contractRootFor(tx.kind)
  result.entrypoint = entrypointFor(tx.kind)
  case tx.kind
  of txContentPublishIntent:
    result.subjectId = jStr(tx.payload, "contentId", tx.txId)
    if tx.payload.kind == JObject and tx.payload.hasKey("intent"):
      result.subjectId = jStr(tx.payload["intent"], "contentId", tx.txId)
    result.participants = @[tx.sender]
  of txContentManifestCommit:
    result.subjectId = jStr(tx.payload, "contentId", tx.txId)
    result.participants = @[tx.sender]
  of txContentReportSubmit:
    result.subjectId = jStr(tx.payload, "contentId", tx.txId)
    result.participants = @[tx.sender]
  of txContentReviewDecide, txContentTombstoneIssue, txContentTombstoneRestore:
    result.subjectId = jStr(tx.payload, "contentId", tx.txId)
    result.participants = @[tx.sender]
  of txCreditIssue:
    let owner =
      if jStr(tx.payload, "owner").len > 0: jStr(tx.payload, "owner")
      else: tx.sender
    result.subjectId = jStr(tx.payload, "positionId", tx.txId)
    result.participants = sortedUnique([tx.sender, owner])
  of txCreditFreeze, txCreditExpire, txCreditConvert:
    let positionId = jStr(tx.payload, "positionId")
    let owner = lookupCreditOwner(state, positionId, tx.sender)
    result.subjectId = positionId
    result.participants = sortedUnique([tx.sender, owner])
  of txTreasuryMintIntentCreate:
    let user =
      if jStr(tx.payload, "userAccount").len > 0: jStr(tx.payload, "userAccount")
      else: tx.sender
    result.subjectId = jStr(tx.payload, "mintIntentId", tx.txId)
    result.participants = sortedUnique([tx.sender, user])
  of txTreasuryExecutionReceiptSubmit:
    let mintIntentId = jStr(tx.payload, "mintIntentId")
    let user = lookupMintUser(state, mintIntentId, tx.sender)
    result.subjectId = jStr(tx.payload, "receiptId", tx.txId)
    result.participants = sortedUnique([tx.sender, user])
  of txTreasuryCustodySnapshotSubmit:
    var covered = jStringSeq(tx.payload, "coveredIntentIds")
    if tx.payload.kind == JObject and tx.payload.hasKey("snapshot"):
      covered = jStringSeq(tx.payload["snapshot"], "coveredIntentIds")
    result.subjectId =
      if tx.payload.kind == JObject and tx.payload.hasKey("snapshot"):
        jStr(tx.payload["snapshot"], "snapshotId", tx.txId)
      else:
        tx.txId
    result.participants = sortedUnique(@[tx.sender] & lookupMintUsers(state, covered))
  of txTreasuryReserveProofSubmit:
    var covered = jStringSeq(tx.payload, "coveredIntentIds")
    if tx.payload.kind == JObject and tx.payload.hasKey("proof"):
      covered = jStringSeq(tx.payload["proof"], "coveredIntentIds")
    result.subjectId =
      if tx.payload.kind == JObject and tx.payload.hasKey("proof"):
        jStr(tx.payload["proof"], "reserveProofHash", tx.txId)
      else:
        tx.txId
    result.participants = sortedUnique(@[tx.sender] & lookupMintUsers(state, covered))
  of txRwadMintFinalize:
    let mintIntentId = jStr(tx.payload, "mintIntentId")
    let user =
      if jStr(tx.payload, "toAccount").len > 0: jStr(tx.payload, "toAccount")
      else: lookupMintUser(state, mintIntentId, tx.sender)
    result.subjectId = mintIntentId
    result.participants = sortedUnique([tx.sender, user])
  of txNameRegister:
    let target =
      if jStr(tx.payload, "targetAccount").len > 0: jStr(tx.payload, "targetAccount")
      else: tx.sender
    result.subjectId = normalizeName(jStr(tx.payload, "name"))
    result.participants = sortedUnique([tx.sender, target])
  of txNameRenew:
    let owner = lookupNameOwner(state, jStr(tx.payload, "name"), tx.sender)
    result.subjectId = normalizeName(jStr(tx.payload, "name"))
    result.participants = sortedUnique([tx.sender, owner])
  of txNameTransfer:
    let owner = lookupNameOwner(state, jStr(tx.payload, "name"), tx.sender)
    let newOwner = jStr(tx.payload, "newOwner")
    let target = jStr(tx.payload, "targetAccount")
    result.subjectId = normalizeName(jStr(tx.payload, "name"))
    result.participants = sortedUnique([tx.sender, owner, newOwner, target])
  of txStake:
    let validator = jStr(tx.payload, "validator", tx.sender)
    result.subjectId = validator
    result.participants = sortedUnique([tx.sender, validator])
  of txDelegate:
    let validator = jStr(tx.payload, "validator")
    result.subjectId = validator
    result.participants = sortedUnique([tx.sender, validator])
  of txValidatorRegister:
    result.subjectId = tx.sender
    result.participants = @[tx.sender]
  of txSlash:
    let offender = jStr(tx.payload, "offender")
    result.subjectId = offender
    result.participants = sortedUnique([tx.sender, offender])
  of txParamUpdate:
    result.subjectId = state.params.chainId
    result.participants = @[tx.sender]

proc threadIdFor*(state: ChainState, tx: Tx): EventThreadId =
  let descriptor = descriptorFor(state, tx)
  EventThreadId(
    contractRoot: descriptor.contractRoot,
    owner: tx.sender,
    lane: descriptor.subjectId,
  )

proc verifyContractProjection*(state: ChainState, event: FabricEvent) =
  let descriptor = descriptorFor(state, event.legacyTx)
  if descriptor.contractRoot != event.contractRoot or descriptor.entrypoint != event.entrypoint:
    raise newException(ValueError, "event contract root or entrypoint mismatch")
  if sortedUnique(descriptor.participants) != sortedUnique(event.participants):
    raise newException(ValueError, "event participants mismatch")

proc applyEventToState*(state: ChainState, event: FabricEvent): seq[AuditEvent] =
  if not verifyTx(event.legacyTx):
    raise newException(ValueError, "invalid legacy tx signature: " & event.legacyTx.txId)
  state.verifyContractProjection(event)
  result = state.applyTx(event.legacyTx)

proc rootFor(kind: ProjectionKind, contractRoot: string, payload: string): ProjectionRoot =
  ProjectionRoot(kind: kind, contractRoot: contractRoot, root: hashHex(payload))

proc projectionRoots*(state: ChainState): seq[ProjectionRoot] =
  let snap = state.snapshot()
  result = @[
    rootFor(pkLedger, LedgerContractRoot,
      encodeObj(%*{
        "params": snap.params,
        "totalRwadSupply": snap.totalRwadSupply,
        "nonces": snap.nonces,
        "rwadBalances": snap.rwadBalances,
      })),
    rootFor(pkContent, ContentContractRoot,
      encodeObj(%*{
        "contents": snap.contents,
      })),
    rootFor(pkGovernance, GovernanceContractRoot,
      encodeObj(%*{
        "reports": snap.reports,
        "moderationCases": snap.moderationCases,
        "tombstones": snap.tombstones,
      })),
    rootFor(pkCredit, CreditContractRoot,
      encodeObj(%*{
        "creditPositions": snap.creditPositions,
      })),
    rootFor(pkTreasury, TreasuryContractRoot,
      encodeObj(%*{
        "mintIntents": snap.mintIntents,
        "executionReceipts": snap.executionReceipts,
        "custodySnapshots": snap.custodySnapshots,
        "reserveProofs": snap.reserveProofs,
      })),
    rootFor(pkName, NameContractRoot,
      encodeObj(%*{
        "nameRecords": snap.nameRecords,
      })),
    rootFor(pkStaking, StakingContractRoot,
      encodeObj(%*{
        "validators": snap.validators,
        "delegations": snap.delegations,
      })),
  ]
