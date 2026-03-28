import std/[algorithm, options, sequtils, sets, strutils, tables]

import ../codec
import ../identity
import ../types
import ../execution/engine as exec_engine
import ../execution/staking
import ../execution/state
import ../../crypto/crypto

type
  ConsensusStatusSnapshot* = object
    currentRound*: uint64
    proposedRound*: uint64
    lastCommittedRound*: uint64
    lastCommittedHeight*: uint64
    quorumWeight*: uint64
    validatorCount*: int
    txPoolSize*: int
    certifiedCount*: int
    pendingBatchCount*: int

  RoundResult* = object
    batch*: Batch
    certificate*: Option[BatchCertificate]
    finalized*: seq[FinalizedBlock]

  ConsensusEngine* = ref object
    state*: ChainState
    identity*: NodeIdentity
    txPool*: OrderedTable[string, Tx]
    batches*: OrderedTable[string, Batch]
    votesByBatch*: Table[string, OrderedTable[string, CertificateVote]]
    certificates*: OrderedTable[string, BatchCertificate]
    certificateByBatch*: Table[string, string]
    certsByRound*: Table[uint64, seq[string]]
    committed*: HashSet[string]
    proposerRoundIndex*: Table[string, string]
    slashing*: seq[SlashEvidence]
    currentRound*: uint64
    proposedRound*: uint64
    lastCommittedRound*: uint64
    lastCommittedHeight*: uint64
    finalizedBlocks*: seq[FinalizedBlock]
    blockAudits*: Table[string, seq[AuditEvent]]

proc bytesOf(value: string): seq[byte] =
  result = newSeq[byte](value.len)
  if value.len > 0:
    copyMem(addr result[0], unsafeAddr value[0], value.len)

proc votePayload(batch: Batch): seq[byte] =
  bytesOf(batch.batchId & ":" & $batch.epoch & ":" & $batch.round)

proc proposerWeight(state: ChainState, address: string): uint64 =
  if address notin state.validators:
    return 0
  state.validators[address].validatorVotingPower()

proc newConsensusEngine*(state: ChainState, identity: NodeIdentity): ConsensusEngine =
  ConsensusEngine(
    state: state,
    identity: identity,
    txPool: initOrderedTable[string, Tx](),
    batches: initOrderedTable[string, Batch](),
    votesByBatch: initTable[string, OrderedTable[string, CertificateVote]](),
    certificates: initOrderedTable[string, BatchCertificate](),
    certificateByBatch: initTable[string, string](),
    certsByRound: initTable[uint64, seq[string]](),
    committed: initHashSet[string](),
    proposerRoundIndex: initTable[string, string](),
    slashing: @[],
    currentRound: 0,
    proposedRound: 0,
    lastCommittedRound: 0,
    lastCommittedHeight: state.height,
    finalizedBlocks: @[],
    blockAudits: initTable[string, seq[AuditEvent]](),
  )

proc statusSnapshot*(engine: ConsensusEngine): ConsensusStatusSnapshot =
  ConsensusStatusSnapshot(
    currentRound: engine.currentRound,
    proposedRound: engine.proposedRound,
    lastCommittedRound: engine.lastCommittedRound,
    lastCommittedHeight: engine.lastCommittedHeight,
    quorumWeight: engine.state.quorumWeight(),
    validatorCount: engine.state.activeValidators().len,
    txPoolSize: engine.txPool.len,
    certifiedCount: engine.certificates.len,
    pendingBatchCount: engine.batches.len - engine.committed.len,
  )

proc addTx*(engine: ConsensusEngine, tx: Tx) =
  if not verifyTx(tx):
    raise newException(ValueError, "invalid tx signature: " & tx.txId)
  engine.txPool[tx.txId] = tx

proc hasBatch*(engine: ConsensusEngine, batchId: string): bool =
  batchId in engine.batches

proc hasCertificate*(engine: ConsensusEngine, certificateId: string): bool =
  certificateId in engine.certificates

proc hasCertificateForBatch*(engine: ConsensusEngine, batchId: string): bool =
  batchId in engine.certificateByBatch

proc hasVote*(engine: ConsensusEngine, batchId, voter: string): bool =
  if batchId notin engine.votesByBatch:
    return false
  voter in engine.votesByBatch[batchId]

proc certifiedRoundWeight*(engine: ConsensusEngine, round: uint64): uint64 =
  var seen = initHashSet[string]()
  for certId in engine.certsByRound.getOrDefault(round, @[]):
    if certId notin engine.certificates:
      continue
    let cert = engine.certificates[certId]
    if cert.proposer in seen:
      continue
    result += proposerWeight(engine.state, cert.proposer)
    seen.incl(cert.proposer)

proc hasCertifiedRoundQuorum*(engine: ConsensusEngine, round: uint64): bool =
  engine.certifiedRoundWeight(round) >= engine.state.quorumWeight()

proc nextParents(engine: ConsensusEngine, round: uint64): seq[string] =
  if round <= 1:
    return @[]
  let parentRound = round - 1
  for certId in engine.certsByRound.getOrDefault(parentRound, @[]):
    if certId in engine.certificates:
      result.add(engine.certificates[certId].batchId)

proc selectBatchTxs(engine: ConsensusEngine): seq[Tx] =
  var count = 0
  for _, tx in engine.txPool.pairs:
    if count >= engine.state.params.maxBatchTxs:
      break
    result.add(tx)
    inc count

proc registerBatch(engine: ConsensusEngine, batch: Batch) =
  let key = batch.proposer & ":" & $batch.round
  if key in engine.proposerRoundIndex and engine.proposerRoundIndex[key] != batch.batchId:
    engine.slashing.add(SlashEvidence(
      offender: batch.proposer,
      round: batch.round,
      batchA: engine.proposerRoundIndex[key],
      batchB: batch.batchId,
      reason: srDoubleSign,
    ))
  engine.proposerRoundIndex[key] = batch.batchId
  engine.batches[batch.batchId] = batch

proc buildBatch*(engine: ConsensusEngine): Batch =
  let txs = engine.selectBatchTxs()
  let round = engine.proposedRound + 1
  result = Batch(
    batchId: "",
    proposer: engine.identity.account,
    proposerPublicKey: encodePublicKeyHex(engine.identity.publicKey),
    epoch: round div max(1'u64, engine.state.params.epochLengthRounds),
    round: round,
    parents: engine.nextParents(round),
    transactions: txs,
    txIds: txs.mapIt(it.txId),
    createdAt: if txs.len > 0: txs[^1].timestamp else: int64(round),
    signature: "",
  )
  signBatch(engine.identity, result)
  for tx in txs:
    engine.txPool.del(tx.txId)
  engine.proposedRound = round
  engine.registerBatch(result)

proc createVote*(engine: ConsensusEngine, batch: Batch, identity: NodeIdentity): CertificateVote =
  let weight =
    if identity.account in engine.state.validators:
      engine.state.validators[identity.account].validatorVotingPower()
    else:
      0
  if weight == 0:
    raise newException(ValueError, "non-validator cannot vote")
  CertificateVote(
    voter: identity.account,
    signature: signBytes(identity.privateKey, votePayload(batch)),
    weight: weight,
  )

proc buildCertificate*(engine: ConsensusEngine, batch: Batch, votes: seq[CertificateVote]): BatchCertificate =
  var orderedVotes = votes
  orderedVotes.sort(proc(a, b: CertificateVote): int =
    cmp(a.voter, b.voter)
  )
  var total = 0'u64
  var seen = initHashSet[string]()
  for vote in orderedVotes:
    if vote.voter notin seen:
      total += vote.weight
      seen.incl(vote.voter)
  result = BatchCertificate(
    certificateId: "",
    batchId: batch.batchId,
    proposer: batch.proposer,
    epoch: batch.epoch,
    round: batch.round,
    batchHash: computeBatchId(batch),
    votes: orderedVotes,
    quorumWeight: total,
    createdAt: batch.createdAt,
  )
  result.certificateId = computeCertificateId(result)

proc verifyVote(engine: ConsensusEngine, batch: Batch, vote: CertificateVote): bool =
  if vote.voter notin engine.state.validators:
    return false
  let validator = engine.state.validators[vote.voter]
  if not validator.active or validator.jailed:
    return false
  if vote.weight != validator.validatorVotingPower():
    return false
  let publicKey = PublicKey.init(validator.publicKey).valueOr:
    return false
  verifyBytes(publicKey, votePayload(batch), vote.signature)

proc verifyCertificate*(engine: ConsensusEngine, cert: BatchCertificate): bool =
  if cert.batchId notin engine.batches:
    return false
  let batch = engine.batches[cert.batchId]
  if not verifyBatch(batch):
    return false
  if computeBatchId(batch) != cert.batchHash:
    return false
  var seen = initHashSet[string]()
  var total = 0'u64
  for vote in cert.votes:
    if vote.voter in seen:
      return false
    if not engine.verifyVote(batch, vote):
      return false
    total += vote.weight
    seen.incl(vote.voter)
  total >= engine.state.quorumWeight() and total == cert.quorumWeight

proc localVote*(engine: ConsensusEngine, batch: Batch): BatchVote =
  BatchVote(
    batchId: batch.batchId,
    proposer: batch.proposer,
    epoch: batch.epoch,
    round: batch.round,
    batchHash: computeBatchId(batch),
    vote: engine.createVote(batch, engine.identity),
  )

proc ingestVote*(engine: ConsensusEngine, signedVote: BatchVote): Option[BatchCertificate] =
  if signedVote.batchId notin engine.batches:
    raise newException(ValueError, "missing batch for vote: " & signedVote.batchId)
  let batch = engine.batches[signedVote.batchId]
  if batch.proposer != signedVote.proposer or
      batch.epoch != signedVote.epoch or
      batch.round != signedVote.round:
    raise newException(ValueError, "vote metadata mismatch: " & signedVote.batchId)
  let batchHash = computeBatchId(batch)
  if signedVote.batchHash != batchHash or batch.batchId != batchHash:
    raise newException(ValueError, "vote batch hash mismatch: " & signedVote.batchId)
  if not engine.verifyVote(batch, signedVote.vote):
    raise newException(ValueError, "invalid vote signature: " & signedVote.vote.voter)
  if batch.batchId in engine.certificateByBatch:
    return none(BatchCertificate)
  var batchVotes = engine.votesByBatch.getOrDefault(
    batch.batchId,
    initOrderedTable[string, CertificateVote](),
  )
  if signedVote.vote.voter in batchVotes:
    return none(BatchCertificate)
  batchVotes[signedVote.vote.voter] = signedVote.vote
  engine.votesByBatch[batch.batchId] = batchVotes
  var votes: seq[CertificateVote] = @[]
  var total = 0'u64
  for _, vote in batchVotes.pairs:
    votes.add(vote)
    total += vote.weight
  if total < engine.state.quorumWeight():
    return none(BatchCertificate)
  some(engine.buildCertificate(batch, votes))

proc certifiedVerticesAt(engine: ConsensusEngine, round: uint64): seq[DagVertex] =
  for certId in engine.certsByRound.getOrDefault(round, @[]):
    if certId in engine.certificates:
      let cert = engine.certificates[certId]
      if cert.batchId in engine.batches:
        result.add(DagVertex(batch: engine.batches[cert.batchId], certificate: cert))

proc collectSubdag(engine: ConsensusEngine, batchId: string, acc: var OrderedTable[string, Batch]) =
  if batchId in acc or batchId in engine.committed or batchId notin engine.batches:
    return
  let batch = engine.batches[batchId]
  for parent in batch.parents:
    engine.collectSubdag(parent, acc)
  acc[batchId] = batch

proc orderedSubdag(engine: ConsensusEngine, leaderBatchId: string): seq[Batch] =
  var collected = initOrderedTable[string, Batch]()
  engine.collectSubdag(leaderBatchId, collected)
  for _, batch in collected.pairs:
    result.add(batch)
  result.sort(proc(a, b: Batch): int =
    result = cmp(a.round, b.round)
    if result == 0:
      result = cmp(a.proposer, b.proposer)
    if result == 0:
      result = cmp(a.batchId, b.batchId)
  )

proc supportWeight(engine: ConsensusEngine, leaderBatchId: string, childRound: uint64): uint64 =
  var seen = initHashSet[string]()
  for vertex in engine.certifiedVerticesAt(childRound):
    if leaderBatchId in vertex.batch.parents:
      for vote in vertex.certificate.votes:
        if vote.voter notin seen:
          result += vote.weight
          seen.incl(vote.voter)

proc finalizeLeader(engine: ConsensusEngine, childRound: uint64): Option[FinalizedBlock] =
  if childRound == 0:
    return none(FinalizedBlock)
  let leaderRound = childRound - 1
  let leaderAddress = engine.state.leaderAddressForRound(leaderRound)
  if leaderAddress.len == 0:
    return none(FinalizedBlock)
  var leaderVertex: Option[DagVertex] = none(DagVertex)
  for vertex in engine.certifiedVerticesAt(leaderRound):
    if vertex.batch.proposer == leaderAddress:
      leaderVertex = some(vertex)
      break
  if leaderVertex.isNone():
    return none(FinalizedBlock)
  let leader = leaderVertex.get()
  if leader.batch.batchId in engine.committed:
    return none(FinalizedBlock)
  if engine.supportWeight(leader.batch.batchId, childRound) < engine.state.quorumWeight():
    return none(FinalizedBlock)
  let ordered = engine.orderedSubdag(leader.batch.batchId)
  if ordered.len == 0:
    return none(FinalizedBlock)
  let nextHeight = engine.state.height + 1
  let epoch = leader.batch.epoch
  let audits = engine.state.applyBatchesToHeight(ordered, nextHeight, epoch, "")
  var checkpoint = engine.state.latestCheckpoint()
  var blk = FinalizedBlock(
    blockId: "",
    height: nextHeight,
    epoch: epoch,
    round: childRound,
    leaderBatchId: leader.batch.batchId,
    orderedBatchIds: ordered.mapIt(it.batchId),
    batches: ordered,
    transactions: ordered.mapIt(it.transactions).concat(),
    stateRoot: engine.state.stateRoot,
    checkpoint: checkpoint,
    createdAt: leader.certificate.createdAt,
  )
  blk.blockId = computeBlockId(blk)
  engine.state.lastBlockId = blk.blockId
  checkpoint = engine.state.latestCheckpoint()
  blk.checkpoint = checkpoint
  blk.blockId = computeBlockId(blk)
  for item in ordered:
    engine.committed.incl(item.batchId)
  engine.lastCommittedRound = leaderRound
  engine.lastCommittedHeight = nextHeight
  engine.finalizedBlocks.add(blk)
  engine.blockAudits[blk.blockId] = audits
  some(blk)

proc maybeFinalize*(engine: ConsensusEngine, observedRound: uint64): seq[FinalizedBlock] =
  var childRound = max(1'u64, engine.lastCommittedRound + 2)
  while childRound <= observedRound:
    let blk = engine.finalizeLeader(childRound)
    if blk.isSome():
      result.add(blk.get())
    inc childRound

proc ingestBatch*(engine: ConsensusEngine, batch: Batch): bool =
  if batch.batchId in engine.batches:
    return false
  if not verifyBatch(batch):
    raise newException(ValueError, "invalid batch signature: " & batch.batchId)
  for tx in batch.transactions:
    if not verifyTx(tx):
      raise newException(ValueError, "invalid tx in batch: " & tx.txId)
  engine.registerBatch(batch)
  result = true

proc ingestCertificate*(engine: ConsensusEngine, cert: BatchCertificate): seq[FinalizedBlock] =
  if cert.batchId in engine.certificateByBatch:
    return @[]
  if not engine.verifyCertificate(cert):
    raise newException(ValueError, "invalid certificate: " & cert.certificateId)
  engine.certificates[cert.certificateId] = cert
  engine.certificateByBatch[cert.batchId] = cert.certificateId
  var roundCerts = engine.certsByRound.getOrDefault(cert.round, @[])
  if cert.certificateId notin roundCerts:
    roundCerts.add(cert.certificateId)
  engine.certsByRound[cert.round] = roundCerts
  engine.currentRound = max(engine.currentRound, cert.round)
  engine.maybeFinalize(cert.round)

proc produceRound*(engine: ConsensusEngine): RoundResult =
  if engine.identity.account notin engine.state.validators:
    raise newException(ValueError, "local identity is not a validator")
  let batch = engine.buildBatch()
  result.batch = batch
  let cert = engine.ingestVote(engine.localVote(batch))
  if cert.isSome():
    result.certificate = cert
    result.finalized = engine.ingestCertificate(cert.get())
  else:
    result.certificate = none(BatchCertificate)
    result.finalized = @[]

proc advanceToFinality*(engine: ConsensusEngine, maxRounds = 4): seq[FinalizedBlock] =
  for _ in 0 ..< maxRounds:
    let roundResult = engine.produceRound()
    result.add(roundResult.finalized)
    if engine.txPool.len == 0 and roundResult.finalized.len > 0:
      break

proc latestBlocks*(engine: ConsensusEngine, limit = 20): seq[FinalizedBlock] =
  let start = max(0, engine.finalizedBlocks.len - limit)
  for idx in start ..< engine.finalizedBlocks.len:
    result.add(engine.finalizedBlocks[idx])
