import std/[algorithm, options, sequtils, tables]

import ../codec
import ../types

type
  ChainState* = ref object
    params*: ChainParams
    height*: uint64
    epoch*: uint64
    lastBlockId*: string
    stateRoot*: string
    totalRwadSupply*: uint64
    nonces*: Table[string, uint64]
    rwadBalances*: Table[string, uint64]
    validators*: OrderedTable[string, ValidatorInfo]
    delegations*: OrderedTable[string, Delegation]
    contents*: OrderedTable[string, ContentRecord]
    reports*: OrderedTable[string, ReportTicket]
    moderationCases*: OrderedTable[string, ModerationCase]
    tombstones*: OrderedTable[string, TombstoneRecord]
    creditPositions*: OrderedTable[string, CreditPosition]
    mintIntents*: OrderedTable[string, MintIntent]
    executionReceipts*: OrderedTable[string, ExecutionReceipt]
    custodySnapshots*: OrderedTable[string, CustodySnapshot]
    reserveProofs*: OrderedTable[string, ReserveProof]
    nameRecords*: OrderedTable[string, NameRecord]

  ChainStateSnapshot* = object
    params*: ChainParams
    height*: uint64
    epoch*: uint64
    lastBlockId*: string
    stateRoot*: string
    totalRwadSupply*: uint64
    nonces*: seq[KeyValueU64]
    rwadBalances*: seq[KeyValueU64]
    validators*: seq[ValidatorInfo]
    delegations*: seq[Delegation]
    contents*: seq[ContentRecord]
    reports*: seq[ReportTicket]
    moderationCases*: seq[ModerationCase]
    tombstones*: seq[TombstoneRecord]
    creditPositions*: seq[CreditPosition]
    mintIntents*: seq[MintIntent]
    executionReceipts*: seq[ExecutionReceipt]
    custodySnapshots*: seq[CustodySnapshot]
    reserveProofs*: seq[ReserveProof]
    nameRecords*: seq[NameRecord]

proc computeStateRoot*(state: ChainState): string

proc sortedPairs(table: Table[string, uint64]): seq[KeyValueU64] =
  for key, value in table.pairs:
    result.add(KeyValueU64(key: key, value: value))
  result.sort(proc(a, b: KeyValueU64): int = cmp(a.key, b.key))

proc sortedValues[T](table: OrderedTable[string, T]): seq[T] =
  for _, value in table.pairs:
    result.add(value)

proc newChainState*(genesis: GenesisSpec): ChainState =
  result = ChainState(
    params: genesis.params,
    height: 0,
    epoch: 0,
    lastBlockId: "",
    stateRoot: "",
    totalRwadSupply: genesis.mintedSupply,
    nonces: initTable[string, uint64](),
    rwadBalances: initTable[string, uint64](),
    validators: initOrderedTable[string, ValidatorInfo](),
    delegations: initOrderedTable[string, Delegation](),
    contents: initOrderedTable[string, ContentRecord](),
    reports: initOrderedTable[string, ReportTicket](),
    moderationCases: initOrderedTable[string, ModerationCase](),
    tombstones: initOrderedTable[string, TombstoneRecord](),
    creditPositions: initOrderedTable[string, CreditPosition](),
    mintIntents: initOrderedTable[string, MintIntent](),
    executionReceipts: initOrderedTable[string, ExecutionReceipt](),
    custodySnapshots: initOrderedTable[string, CustodySnapshot](),
    reserveProofs: initOrderedTable[string, ReserveProof](),
    nameRecords: initOrderedTable[string, NameRecord](),
  )
  for item in genesis.validators:
    result.validators[item.address] = item
  for item in genesis.balances:
    result.rwadBalances[item.key] = item.value
  for item in genesis.names:
    result.nameRecords[item.name] = item
  for item in genesis.contents:
    result.contents[item.contentId] = item
  result.stateRoot = result.computeStateRoot()

proc snapshot*(state: ChainState): ChainStateSnapshot =
  ChainStateSnapshot(
    params: state.params,
    height: state.height,
    epoch: state.epoch,
    lastBlockId: state.lastBlockId,
    stateRoot: state.stateRoot,
    totalRwadSupply: state.totalRwadSupply,
    nonces: sortedPairs(state.nonces),
    rwadBalances: sortedPairs(state.rwadBalances),
    validators: sortedValues(state.validators),
    delegations: sortedValues(state.delegations),
    contents: sortedValues(state.contents),
    reports: sortedValues(state.reports),
    moderationCases: sortedValues(state.moderationCases),
    tombstones: sortedValues(state.tombstones),
    creditPositions: sortedValues(state.creditPositions),
    mintIntents: sortedValues(state.mintIntents),
    executionReceipts: sortedValues(state.executionReceipts),
    custodySnapshots: sortedValues(state.custodySnapshots),
    reserveProofs: sortedValues(state.reserveProofs),
    nameRecords: sortedValues(state.nameRecords),
  )

proc computeStateRoot*(state: ChainState): string =
  var snap = state.snapshot()
  snap.lastBlockId = ""
  snap.stateRoot = ""
  hashHex(encodeObj(snap))

proc refreshStateRoot*(state: ChainState) =
  state.stateRoot = state.computeStateRoot()

proc fromSnapshot*(snapshot: ChainStateSnapshot): ChainState =
  result = ChainState(
    params: snapshot.params,
    height: snapshot.height,
    epoch: snapshot.epoch,
    lastBlockId: snapshot.lastBlockId,
    stateRoot: snapshot.stateRoot,
    totalRwadSupply: snapshot.totalRwadSupply,
    nonces: initTable[string, uint64](),
    rwadBalances: initTable[string, uint64](),
    validators: initOrderedTable[string, ValidatorInfo](),
    delegations: initOrderedTable[string, Delegation](),
    contents: initOrderedTable[string, ContentRecord](),
    reports: initOrderedTable[string, ReportTicket](),
    moderationCases: initOrderedTable[string, ModerationCase](),
    tombstones: initOrderedTable[string, TombstoneRecord](),
    creditPositions: initOrderedTable[string, CreditPosition](),
    mintIntents: initOrderedTable[string, MintIntent](),
    executionReceipts: initOrderedTable[string, ExecutionReceipt](),
    custodySnapshots: initOrderedTable[string, CustodySnapshot](),
    reserveProofs: initOrderedTable[string, ReserveProof](),
    nameRecords: initOrderedTable[string, NameRecord](),
  )
  for item in snapshot.nonces:
    result.nonces[item.key] = item.value
  for item in snapshot.rwadBalances:
    result.rwadBalances[item.key] = item.value
  for item in snapshot.validators:
    result.validators[item.address] = item
  for item in snapshot.delegations:
    result.delegations[item.delegator & "->" & item.validator] = item
  for item in snapshot.contents:
    result.contents[item.contentId] = item
  for item in snapshot.reports:
    result.reports[item.reportId] = item
  for item in snapshot.moderationCases:
    result.moderationCases[item.caseId] = item
  for item in snapshot.tombstones:
    result.tombstones[item.tombstoneId] = item
  for item in snapshot.creditPositions:
    result.creditPositions[item.positionId] = item
  for item in snapshot.mintIntents:
    result.mintIntents[item.mintIntentId] = item
  for item in snapshot.executionReceipts:
    result.executionReceipts[item.receiptId] = item
  for item in snapshot.custodySnapshots:
    result.custodySnapshots[item.snapshotId] = item
  for item in snapshot.reserveProofs:
    result.reserveProofs[item.reserveProofHash] = item
  for item in snapshot.nameRecords:
    result.nameRecords[item.name] = item

proc nonceOf*(state: ChainState, address: string): uint64 =
  state.nonces.getOrDefault(address, 0)

proc rwadBalanceOf*(state: ChainState, address: string): uint64 =
  state.rwadBalances.getOrDefault(address, 0)

proc adjustRwadBalance*(state: ChainState, address: string, delta: int64) =
  let current = int64(state.rwadBalances.getOrDefault(address, 0))
  let next = current + delta
  doAssert next >= 0
  state.rwadBalances[address] = uint64(next)

proc setNonce*(state: ChainState, address: string, nonce: uint64) =
  state.nonces[address] = nonce

proc creditBalanceSnapshot*(state: ChainState, owner: string): CreditBalanceSnapshot =
  result.owner = owner
  for _, position in state.creditPositions.pairs:
    if position.owner != owner:
      continue
    result.positions.add(position)
    result.totalIssued += position.amount
    case position.status
    of csOpen:
      result.totalAvailable += position.available
    of csFrozen:
      result.totalFrozen += position.available
    of csExpired:
      result.totalExpired += position.available
    of csConverted:
      result.totalConverted += position.amount

proc latestCheckpoint*(state: ChainState): Checkpoint =
  result = Checkpoint(
    checkpointId: "",
    height: state.height,
    epoch: state.epoch,
    stateRoot: state.stateRoot,
    validatorRoot: hashHex(encodeObj(sortedValues(state.validators))),
    createdAt: state.height.int64,
  )
  result.checkpointId = computeCheckpointId(result)
