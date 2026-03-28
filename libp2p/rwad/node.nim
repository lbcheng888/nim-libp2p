import std/[base64, json, options, os, sets, strutils, tables]

import chronos
import chronicles
import pebble/kv

import ../lsmr
import ./codec
import ./consensus/engine as consensus_engine
import ./execution/content
import ./execution/credit
import ./execution/name
import ./execution/staking
import ./execution/state
import ./execution/treasury
import ./identity
import ./net/service
import ./storage/store
import ./types

type
  ChainStatusSnapshot* = object
    chainId*: string
    height*: uint64
    epoch*: uint64
    stateRoot*: string
    totalSupply*: uint64
    lastBlockId*: string
    localAccount*: string
    localPeerId*: string
    validatorCount*: int
    routingStatus*: RoutingPlaneStatus

  SubmitTxResult* = object
    txId*: string
    accepted*: bool
    finalizedBlockIds*: seq[string]
    finalizedHeights*: seq[uint64]

  RwadNodeConfig* = object
    dataDir*: string
    listenAddrs*: seq[string]
    bootstrapAddrs*: seq[string]
    legacyBootstrapAddrs*: seq[string]
    lsmrBootstrapAddrs*: seq[string]
    identityPath*: string
    genesisPath*: string
    consensusTickMs*: int
    routingMode*: RoutingPlaneMode
    primaryPlane*: PrimaryRoutingPlane
    lsmrConfig*: Option[LsmrConfig]

  RwadNode* = ref object
    config*: RwadNodeConfig
    identity*: NodeIdentity
    genesis*: GenesisSpec
    state*: ChainState
    store*: ChainStore
    consensus*: ConsensusEngine
    network*: RwadNetwork
    running*: bool
    scheduledRounds*: int
    consensusTask*: Future[void]
    backgroundTasks*: seq[Future[void]]
    collectingVotes*: HashSet[string]
    fetchingVotes*: HashSet[string]
    fetchingRounds*: HashSet[string]
    fetchingCertificates*: HashSet[string]
    submittingVotes*: HashSet[string]
    submittingBatches*: HashSet[string]

  VoteAcceptResult = object
    accepted: bool
    isNew: bool
    needsCertFetch: bool

proc bytesOf(value: string): seq[byte] =
  result = newSeq[byte](value.len)
  if value.len > 0:
    copyMem(addr result[0], unsafeAddr value[0], value.len)

proc stringOf(data: openArray[byte]): string =
  result = newString(data.len)
  if data.len > 0:
    copyMem(addr result[0], unsafeAddr data[0], data.len)

proc defaultIdentityPath*(dataDir: string): string =
  dataDir / "identity.json"

proc defaultGenesisPath*(dataDir: string): string =
  dataDir / "genesis.json"

proc createDefaultGenesis*(identity: NodeIdentity, chainId = "rwad-local"): GenesisSpec =
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

proc concatBlob(node: RwadNode, manifestCid: string): Option[seq[byte]] =
  let manifest = node.store.loadBlobHead(manifestCid)
  if manifest.isNone():
    return none(seq[byte])
  var data: seq[byte] = @[]
  for chunk in manifest.get().chunks:
    let bytes = node.store.loadBlobChunk(manifestCid, chunk.index)
    if bytes.isNone():
      return none(seq[byte])
    data.add(bytes.get())
  some(data)

proc decodeAccessPart(value: string): string =
  if value.len == 0:
    return ""
  try:
    decode(value)
  except CatchableError:
    ""

proc authorizeBlobManifest(
    node: RwadNode,
    manifestCid: string,
    reader = "",
    ticketId = "",
    nowTs = 0'i64,
    kind = brkHead,
): Option[ContentManifest] {.gcsafe, raises: [].} =
  try:
    let manifest = node.store.loadBlobHead(manifestCid)
    if manifest.isNone():
      return none(ContentManifest)
    if not manifest.get().canReadManifest(reader, ticketId, nowTs, kind):
      return none(ContentManifest)
    if ticketId.len > 0 and reader.len > 0:
      let ticket = manifest.get().findCapabilityTicket(reader, ticketId, nowTs)
      if ticket.isNone():
        return none(ContentManifest)
      if ticket.get().readCountLimit > 0:
        let used = node.store.blobTicketReadCount(manifestCid, ticketId, reader)
        if used >= ticket.get().readCountLimit:
          return none(ContentManifest)
        discard node.store.recordBlobTicketRead(manifestCid, ticketId, reader)
    manifest
  except CatchableError:
    none(ContentManifest)

proc safeLoadVote(node: RwadNode, batchId, voter: string): Option[BatchVote] {.gcsafe, raises: [].} =
  try:
    node.store.loadVote(batchId, voter)
  except CatchableError:
    none(BatchVote)

proc safeLoadBatchByRound(node: RwadNode, proposer: string, round: uint64): Option[Batch] {.gcsafe, raises: [].} =
  try:
    node.store.loadBatchByRound(proposer, round)
  except CatchableError:
    none(Batch)

proc safeLoadCertificateByBatch(node: RwadNode, batchId: string): Option[BatchCertificate] {.gcsafe, raises: [].} =
  try:
    node.store.loadCertificateByBatch(batchId)
  except CatchableError:
    none(BatchCertificate)

proc fetchRawBounded(
    node: RwadNode,
    peerId: string,
    key: string,
    timeout: Duration = 1.seconds,
    maxAttempts = 1,
): Future[Option[seq[byte]]] {.async: (raises: []).} =
  if node.network.isNil or not node.running or peerId.len == 0:
    return none(seq[byte])
  try:
    let fut = node.network.fetchRaw(
      peerId,
      key,
      timeout = timeout,
      maxAttempts = maxAttempts,
    )
    let ready = await withTimeout(fut, timeout)
    if not ready:
      if not fut.finished():
        fut.cancel()
      return none(seq[byte])
    return await fut
  except CancelledError:
    return none(seq[byte])

proc acceptCertificate(node: RwadNode, cert: BatchCertificate, publishCert = false): seq[FinalizedBlock]
proc acceptBatch(node: RwadNode, batch: Batch, publishVote = false, publishCert = false): bool
proc driveConsensus(node: RwadNode, maxSteps = 1): seq[FinalizedBlock]
proc acceptKnownVote(
    node: RwadNode, vote: BatchVote, publishCert = false, relayVote = false
): VoteAcceptResult

proc fetchLookupRaw(node: RwadNode, key: string): Option[seq[byte]] {.gcsafe, raises: [].} =
  try:
    if key.startsWith("manifest:"):
      let payload = node.store.kv.get(ksTask, "blobhead:" & key[9 .. ^1])
      let parts = key.split(":")
      if parts.len == 2 and payload.isSome():
        return some(bytesOf(payload.get()))
      if parts.len == 5:
        let manifest = node.authorizeBlobManifest(
          manifestCid = parts[1],
          reader = decodeAccessPart(parts[2]),
          ticketId = decodeAccessPart(parts[3]),
          nowTs = parts[4].parseBiggestInt().int64,
          kind = brkHead,
        )
        if manifest.isSome():
          return some(bytesOf(encodeObj(manifest.get())))
    elif key.startsWith("chunk:"):
      let parts = key.split(":")
      if parts.len == 3:
        let manifest = node.authorizeBlobManifest(parts[1], kind = brkChunk)
        if manifest.isSome():
          return node.store.kv.getBytes(ksTask, "blobchunk:" & parts[1] & ":" & parts[2])
      elif parts.len == 6:
        let manifest = node.authorizeBlobManifest(
          manifestCid = parts[1],
          reader = decodeAccessPart(parts[3]),
          ticketId = decodeAccessPart(parts[4]),
          nowTs = parts[5].parseBiggestInt().int64,
          kind = brkChunk,
        )
        if manifest.isSome():
          return node.store.kv.getBytes(ksTask, "blobchunk:" & parts[1] & ":" & parts[2])
    elif key.startsWith("range:"):
      let parts = key.split(":")
      if parts.len == 4:
        let head = node.authorizeBlobManifest(parts[1], kind = brkRange)
        if head.isNone():
          return none(seq[byte])
        var allBytes: seq[byte] = @[]
        for chunk in head.get().chunks:
          let bytes = node.store.kv.getBytes(ksTask, "blobchunk:" & parts[1] & ":" & $chunk.index)
          if bytes.isNone():
            return none(seq[byte])
          allBytes.add(bytes.get())
        let blob = some(allBytes)
        if blob.isSome():
          let startPos = max(0, parts[2].parseInt())
          let endPos = min(blob.get().len, parts[3].parseInt())
          if startPos < endPos:
            return some(blob.get()[startPos ..< endPos])
      elif parts.len == 7:
        let head = node.authorizeBlobManifest(
          manifestCid = parts[1],
          reader = decodeAccessPart(parts[4]),
          ticketId = decodeAccessPart(parts[5]),
          nowTs = parts[6].parseBiggestInt().int64,
          kind = brkRange,
        )
        if head.isNone():
          return none(seq[byte])
        var allBytes: seq[byte] = @[]
        for chunk in head.get().chunks:
          let bytes = node.store.kv.getBytes(ksTask, "blobchunk:" & parts[1] & ":" & $chunk.index)
          if bytes.isNone():
            return none(seq[byte])
          allBytes.add(bytes.get())
        let startPos = max(0, parts[2].parseInt())
        let endPos = min(allBytes.len, parts[3].parseInt())
        if startPos < endPos:
          return some(allBytes[startPos ..< endPos])
    elif key.startsWith("batch:"):
      let payload = node.store.kv.get(ksTask, "batch:" & key[6 .. ^1])
      if payload.isSome():
        return some(bytesOf(payload.get()))
    elif key.startsWith("batchbyround:"):
      let parts = key.split(":")
      if parts.len == 3:
        let round = parts[2].parseUInt()
        let payload = node.store.kv.get(ksTask, "batchround:" & parts[1] & ":" & align($round, 20, '0'))
        if payload.isSome():
          return some(bytesOf(payload.get()))
    elif key.startsWith("cert:"):
      let payload = node.store.kv.get(ksTask, "cert:" & key[5 .. ^1])
      if payload.isSome():
        return some(bytesOf(payload.get()))
    elif key.startsWith("certbybatch:"):
      let payload = node.store.kv.get(ksTask, "certbatch:" & key[12 .. ^1])
      if payload.isSome():
        return some(bytesOf(payload.get()))
    elif key.startsWith("tx:"):
      let payload = node.store.kv.get(ksTask, "tx:" & key[3 .. ^1])
      if payload.isSome():
        return some(bytesOf(payload.get()))
    elif key.startsWith("vote:"):
      let parts = key.split(":")
      if parts.len == 3:
        let payload = node.store.kv.get(ksTask, "vote:" & parts[1] & ":" & parts[2])
        if payload.isSome():
          return some(bytesOf(payload.get()))
    elif key.startsWith("submitvote:"):
      let payload = key["submitvote:".len .. ^1]
      let vote = decodeObj[BatchVote](decode(payload))
      var accepted = false
      {.cast(gcsafe).}:
        try:
          let outcome = node.acceptKnownVote(vote, publishCert = true, relayVote = true)
          accepted = outcome.accepted
        except Exception:
          discard
      if accepted:
        return some(bytesOf("ok"))
      return none(seq[byte])
    elif key.startsWith("submitbatch:"):
      let payload = key["submitbatch:".len .. ^1]
      let batch = decodeObj[Batch](decode(payload))
      var accepted = false
      {.cast(gcsafe).}:
        try:
          accepted = acceptBatch(node, batch, publishVote = true, publishCert = true)
        except Exception:
          discard
      if accepted or node.consensus.hasBatch(batch.batchId):
        return some(bytesOf("ok"))
      return none(seq[byte])
    elif key.startsWith("submitcert:"):
      let payload = key["submitcert:".len .. ^1]
      let cert = decodeObj[BatchCertificate](decode(payload))
      var accepted = false
      {.cast(gcsafe).}:
        try:
          if cert.batchId in node.consensus.batches:
            discard node.acceptCertificate(cert)
            accepted = node.consensus.hasCertificateForBatch(cert.batchId)
        except Exception:
          discard
      if accepted:
        return some(bytesOf("ok"))
      return none(seq[byte])
    elif key.startsWith("block:"):
      let payload = node.store.kv.get(ksTask, "block:" & align(key[6 .. ^1], 20, '0'))
      if payload.isSome():
        return some(bytesOf(payload.get()))
  except CatchableError:
    discard
  none(seq[byte])

proc persistBlockArtifacts(node: RwadNode, blk: FinalizedBlock) =
  node.store.persistBlock(blk, node.state.snapshot())
  let audits = node.consensus.blockAudits.getOrDefault(blk.blockId, @[])
  for audit in audits:
    node.store.persistAudit(audit)

proc handleNetworkBatch(
    node: RwadNode, batch: Batch
): Future[void] {.gcsafe, raises: [], async: (raises: []).}
proc handleNetworkVote(
    node: RwadNode, vote: BatchVote
): Future[bool] {.gcsafe, raises: [], async: (raises: []).}
proc handleNetworkCertificate(
    node: RwadNode, cert: BatchCertificate
): Future[void] {.gcsafe, raises: [], async: (raises: []).}
proc handleNetworkEvent(
    node: RwadNode, topic: string, payload: string
): Future[void] {.gcsafe, raises: [], async: (raises: []).}
proc spawnBackgroundTask(node: RwadNode, fut: Future[void])
proc collectVotesForBatch(
    node: RwadNode, batch: Batch, attempts = 160, delayMs = 250
): Future[bool] {.async: (raises: []).}
proc fetchVoteFromValidator(
    node: RwadNode, peerId: string, batchId: string, voter: string
): Future[void] {.async: (raises: []).}
proc ensureVoteFetcher(
    node: RwadNode, peerId: string, batchId: string, voter: string
)
proc collectVotesTask(node: RwadNode, batch: Batch): Future[void] {.async: (raises: []).}
proc ensureVoteSubmit(
    node: RwadNode, peerId: string, vote: BatchVote, attempts = 80, delayMs = 150
)
proc ensureVoteSubmitToProposer(
    node: RwadNode, vote: BatchVote, attempts = 80, delayMs = 150
)
proc ensureBatchSubmit(
    node: RwadNode, peerId: string, batch: Batch, attempts = 20, delayMs = 250
)
proc fetchBatchByRoundFromProposer(
    node: RwadNode, proposer: string, round: uint64, attempts = 80, delayMs = 250
): Future[bool] {.async: (raises: []).}
proc fetchRoundBatchesTask(node: RwadNode, round: uint64): Future[void] {.async: (raises: []).}
proc fetchMissingRoundTask(
    node: RwadNode, proposer: string, round: uint64, key: string
): Future[void] {.async: (raises: []).}
proc fetchMissingCertificateTask(
    node: RwadNode, proposer: string, batchId: string, key: string
): Future[void] {.async: (raises: []).}
proc maintenanceLoop(node: RwadNode): Future[void] {.async: (raises: []).}
proc submitVoteToValidator(
    node: RwadNode, peerId: string, vote: BatchVote
    , attempts = 80, delayMs = 150
): Future[bool] {.async: (raises: []).}
proc submitVoteToValidatorTask(
    node: RwadNode, peerId: string, vote: BatchVote
    , attempts = 80, delayMs = 150
): Future[void] {.async: (raises: []).}
proc submitVoteToValidators(
    node: RwadNode, vote: BatchVote, attempts = 80, delayMs = 150
): Future[void] {.async: (raises: []).}
proc submitVoteTask(node: RwadNode, vote: BatchVote, attempts = 80, delayMs = 150): Future[void] {.async: (raises: []).}
proc publishBatchTask(node: RwadNode, batch: Batch): Future[void] {.gcsafe, raises: [].}
proc publishVoteTask(node: RwadNode, vote: BatchVote): Future[void] {.gcsafe, raises: [].}
proc publishCertificateTask(node: RwadNode, cert: BatchCertificate): Future[void] {.gcsafe, raises: [].}
proc submitCertificateToValidator(
    node: RwadNode, peerId: string, cert: BatchCertificate
    , attempts = 20, delayMs = 250
): Future[void] {.async: (raises: []).}
proc submitCertificateToValidators(
    node: RwadNode, cert: BatchCertificate, attempts = 20, delayMs = 250
): Future[void] {.async: (raises: []).}
proc submitBatchToValidator(
    node: RwadNode, peerId: string, batch: Batch
    , attempts = 20, delayMs = 250
): Future[void] {.async: (raises: []).}
proc submitBatchToValidators(
    node: RwadNode, batch: Batch, attempts = 20, delayMs = 250
): Future[void] {.async: (raises: []).}

proc acceptKnownVote(
    node: RwadNode, vote: BatchVote, publishCert = false, relayVote = false
): VoteAcceptResult =
  if not node.consensus.hasBatch(vote.batchId):
    return VoteAcceptResult(accepted: false)
  result.accepted = true
  result.isNew = not node.consensus.hasVote(vote.batchId, vote.vote.voter)
  if result.isNew:
    node.store.persistVote(vote)
  let cert = node.consensus.ingestVote(vote)
  if cert.isSome():
    discard node.acceptCertificate(cert.get(), publishCert = publishCert)
    if node.scheduledRounds > 0:
      discard node.driveConsensus(1)
  else:
    result.needsCertFetch = vote.round >= 2 and not node.consensus.hasCertificateForBatch(vote.batchId)
  if result.isNew and relayVote and not node.network.isNil:
    node.spawnBackgroundTask(node.publishVoteTask(vote))
    node.ensureVoteSubmitToProposer(vote)

proc newRwadNode*(config: RwadNodeConfig): RwadNode =
  createDir(config.dataDir)
  let identity = loadIdentity(config.identityPath)
  let genesis = loadGenesis(config.genesisPath)
  let store = newChainStore(config.dataDir)
  let checkpoint = store.loadLatestCheckpoint()
  let state =
    if checkpoint.isSome():
      checkpoint.get().fromSnapshot()
    else:
      newChainState(genesis)
  let consensus = newConsensusEngine(state, identity)
  var node = RwadNode(
    config: config,
    identity: identity,
    genesis: genesis,
    state: state,
    store: store,
    consensus: consensus,
    network: nil,
    running: false,
    scheduledRounds: 0,
    consensusTask: nil,
    backgroundTasks: @[],
    collectingVotes: initHashSet[string](),
    fetchingVotes: initHashSet[string](),
    fetchingRounds: initHashSet[string](),
    fetchingCertificates: initHashSet[string](),
    submittingVotes: initHashSet[string](),
    submittingBatches: initHashSet[string](),
  )
  let network = newRwadNetwork(
    identity = identity,
    listenAddrs = config.listenAddrs,
    bootstrapAddrs = config.bootstrapAddrs,
    legacyBootstrapAddrs = config.legacyBootstrapAddrs,
    lsmrBootstrapAddrs = config.lsmrBootstrapAddrs,
    routingMode = config.routingMode,
    primaryPlane = config.primaryPlane,
    lsmrConfig = config.lsmrConfig,
    fetchLookup = proc(key: string): Option[seq[byte]] {.gcsafe, raises: [].} =
      fetchLookupRaw(node, key),
    batchHandler = proc(network: RwadNetwork, batch: Batch): Future[void] {.gcsafe, raises: [].} =
      discard network
      return handleNetworkBatch(node, batch),
    voteHandler = proc(network: RwadNetwork, vote: BatchVote): Future[bool] {.gcsafe, raises: [].} =
      discard network
      return handleNetworkVote(node, vote),
    certificateHandler = proc(network: RwadNetwork, cert: BatchCertificate): Future[void] {.gcsafe, raises: [].} =
      discard network
      return handleNetworkCertificate(node, cert),
    eventHandler = proc(network: RwadNetwork, topic: string, payload: string): Future[void] {.gcsafe, raises: [].} =
      discard network
      return handleNetworkEvent(node, topic, payload),
  )
  node.network = network
  node

proc scheduleRounds(node: RwadNode, count = 1) =
  node.scheduledRounds = min(32, node.scheduledRounds + max(0, count))

proc validatorPeerId(node: RwadNode, address: string): string =
  node.state.validators.getOrDefault(address, ValidatorInfo()).peerId

proc pruneBackgroundTasks(node: RwadNode) =
  var pending: seq[Future[void]] = @[]
  for fut in node.backgroundTasks:
    if fut.isNil or fut.finished():
      continue
    pending.add(fut)
  node.backgroundTasks = pending

proc spawnBackgroundTask(node: RwadNode, fut: Future[void]) =
  if fut.isNil:
    return
  node.pruneBackgroundTasks()
  node.backgroundTasks.add(fut)
  asyncSpawn fut

proc ensureVoteCollector(node: RwadNode, batch: Batch) =
  if node.network.isNil or not node.running:
    return
  if node.consensus.hasCertificateForBatch(batch.batchId):
    return
  if batch.batchId in node.collectingVotes:
    return
  node.collectingVotes.incl(batch.batchId)
  node.spawnBackgroundTask(node.collectVotesTask(batch))

proc fetchVoteRetryTask(
    node: RwadNode, peerId: string, batchId: string, voter: string, key: string
): Future[void] {.async: (raises: []).} =
  try:
    await node.fetchVoteFromValidator(peerId, batchId, voter)
  finally:
    node.fetchingVotes.excl(key)

proc ensureVoteFetcher(
    node: RwadNode, peerId: string, batchId: string, voter: string
) =
  if node.network.isNil or not node.running or peerId.len == 0:
    return
  let key = peerId & ":" & batchId & ":" & voter
  if key in node.fetchingVotes:
    return
  node.fetchingVotes.incl(key)
  node.spawnBackgroundTask(node.fetchVoteRetryTask(peerId, batchId, voter, key))

proc ensureRoundFetcher(node: RwadNode, proposer: string, round: uint64) =
  if node.network.isNil or not node.running:
    return
  if proposer.len == 0 or proposer == node.identity.account:
    return
  let key = proposer & ":" & $round
  if key in node.fetchingRounds:
    return
  node.fetchingRounds.incl(key)
  node.spawnBackgroundTask(node.fetchMissingRoundTask(proposer, round, key))

proc ensureCertificateFetcher(node: RwadNode, proposer: string, batchId: string) =
  if node.network.isNil or not node.running:
    return
  if proposer.len == 0:
    return
  let key = proposer & ":" & batchId
  if key in node.fetchingCertificates:
    return
  node.fetchingCertificates.incl(key)
  node.spawnBackgroundTask(node.fetchMissingCertificateTask(proposer, batchId, key))

proc submitVoteRetryTask(
    node: RwadNode, peerId: string, vote: BatchVote, key: string, attempts = 80, delayMs = 150
): Future[void] {.async: (raises: []).} =
  try:
    discard await node.submitVoteToValidator(
      peerId,
      vote,
      attempts = attempts,
      delayMs = delayMs,
    )
  finally:
    node.submittingVotes.excl(key)

proc ensureVoteSubmit(
    node: RwadNode, peerId: string, vote: BatchVote, attempts = 80, delayMs = 150
) =
  if node.network.isNil or not node.running or peerId.len == 0:
    return
  if node.consensus.hasCertificateForBatch(vote.batchId):
    return
  let key = peerId & ":" & vote.batchId & ":" & vote.vote.voter
  if key in node.submittingVotes:
    return
  node.submittingVotes.incl(key)
  node.spawnBackgroundTask(
    node.submitVoteRetryTask(
      peerId,
      vote,
      key,
      attempts = attempts,
      delayMs = delayMs,
      )
  )

proc ensureVoteSubmitToProposer(
    node: RwadNode, vote: BatchVote, attempts = 80, delayMs = 150
) =
  if node.network.isNil or not node.running:
    return
  if vote.proposer.len == 0:
    return
  if vote.proposer == node.identity.account or vote.proposer == vote.vote.voter:
    return
  let proposerPeerId = node.validatorPeerId(vote.proposer)
  if proposerPeerId.len == 0:
    return
  node.ensureVoteSubmit(
    proposerPeerId,
    vote,
    attempts = attempts,
    delayMs = delayMs,
  )

proc submitBatchRetryTask(
    node: RwadNode, peerId: string, batch: Batch, key: string, attempts = 20, delayMs = 250
): Future[void] {.async: (raises: []).} =
  try:
    await node.submitBatchToValidator(
      peerId,
      batch,
      attempts = attempts,
      delayMs = delayMs,
    )
  finally:
    node.submittingBatches.excl(key)

proc ensureBatchSubmit(
    node: RwadNode, peerId: string, batch: Batch, attempts = 20, delayMs = 250
) =
  if node.network.isNil or not node.running or peerId.len == 0:
    return
  if node.consensus.hasCertificateForBatch(batch.batchId):
    return
  let key = peerId & ":" & batch.batchId
  if key in node.submittingBatches:
    return
  node.submittingBatches.incl(key)
  node.spawnBackgroundTask(
    node.submitBatchRetryTask(
      peerId,
      batch,
      key,
      attempts = attempts,
      delayMs = delayMs,
    )
  )

proc cancelBackgroundTasks(node: RwadNode) {.async.} =
  node.pruneBackgroundTasks()
  if node.backgroundTasks.len == 0:
    return
  var cancels: seq[Future[void]] = @[]
  for fut in node.backgroundTasks.mitems:
    if fut.isNil or fut.finished():
      continue
    let cancel = fut.cancelAndWait()
    if cancel != nil:
      cancels.add(cancel)
  if cancels.len > 0:
    try:
      let grouped = allFutures(cancels)
      if grouped != nil:
        await grouped.wait(2.seconds)
    except CatchableError:
      discard
  node.backgroundTasks.setLen(0)

proc persistBatchEnvelope(node: RwadNode, batch: Batch) =
  for tx in batch.transactions:
    node.store.persistTx(tx)
  node.store.persistBatch(batch)

proc hasPendingTransactionBatches(node: RwadNode): bool =
  for batchId, batch in node.consensus.batches.pairs:
    if batch.transactions.len == 0:
      continue
    if batchId notin node.consensus.committed:
      return true
  false

proc batchHasTransactions(node: RwadNode, batchId: string): bool =
  if batchId in node.consensus.batches:
    return node.consensus.batches.getOrDefault(batchId, Batch()).transactions.len > 0
  let batch = node.store.loadBatch(batchId)
  batch.isSome() and batch.get().transactions.len > 0

proc acceptCertificate(node: RwadNode, cert: BatchCertificate, publishCert = false): seq[FinalizedBlock] =
  let hadCertificate = node.consensus.hasCertificateForBatch(cert.batchId)
  let finalized = node.consensus.ingestCertificate(cert)
  when defined(rwad_diag):
    echo "RWAD_DIAG acceptCert node=", node.identity.account[0 .. min(node.identity.account.high, 7)],
      " proposer=", cert.proposer[0 .. min(cert.proposer.high, 7)],
      " round=", cert.round,
      " batch=", cert.batchId[0 .. min(cert.batchId.high, 7)],
      " hadCert=", hadCertificate,
      " finalized=", finalized.len,
      " currentRound=", node.consensus.currentRound,
      " proposedRound=", node.consensus.proposedRound
  if cert.batchId in node.consensus.batches:
    node.store.persistCertificate(cert)
  for blk in finalized:
    node.persistBlockArtifacts(blk)
  if not hadCertificate and not node.network.isNil:
    node.spawnBackgroundTask(node.network.publishCertificate(cert))
    node.spawnBackgroundTask(
      node.submitCertificateToValidators(
        cert,
        attempts = 8,
        delayMs = max(250, node.config.consensusTickMs),
      )
    )
  if not hadCertificate and node.hasPendingTransactionBatches():
    node.scheduleRounds(1)
  if not hadCertificate and node.scheduledRounds > 0:
    discard node.driveConsensus(1)
  if not hadCertificate and not node.network.isNil:
    node.spawnBackgroundTask(node.fetchRoundBatchesTask(cert.round))
    node.spawnBackgroundTask(node.fetchRoundBatchesTask(cert.round + 1))
  result = finalized

proc castLocalVote(node: RwadNode, batch: Batch, publishVote = false, publishCert = false): seq[FinalizedBlock] =
  if node.identity.account notin node.state.validators:
    when defined(rwad_diag):
      echo "RWAD_DIAG castLocalVote skip non-validator node=", node.identity.account[0 .. min(node.identity.account.high, 7)],
        " proposer=", batch.proposer[0 .. min(batch.proposer.high, 7)],
        " round=", batch.round,
        " batch=", batch.batchId[0 .. min(batch.batchId.high, 7)]
    return @[]
  if node.consensus.hasVote(batch.batchId, node.identity.account):
    when defined(rwad_diag):
      echo "RWAD_DIAG castLocalVote skip duplicate node=", node.identity.account[0 .. min(node.identity.account.high, 7)],
        " proposer=", batch.proposer[0 .. min(batch.proposer.high, 7)],
        " round=", batch.round,
        " batch=", batch.batchId[0 .. min(batch.batchId.high, 7)]
    return @[]
  let vote = node.consensus.localVote(batch)
  node.store.persistVote(vote)
  when defined(rwad_diag):
    echo "RWAD_DIAG castLocalVote node=", node.identity.account[0 .. min(node.identity.account.high, 7)],
      " proposer=", batch.proposer[0 .. min(batch.proposer.high, 7)],
      " round=", batch.round,
      " batch=", batch.batchId[0 .. min(batch.batchId.high, 7)],
      " txs=", batch.transactions.len
  let cert = node.consensus.ingestVote(vote)
  if publishVote and not node.network.isNil:
    node.spawnBackgroundTask(node.network.publishVote(vote))
    node.ensureVoteSubmitToProposer(vote)
  if cert.isSome():
    return node.acceptCertificate(cert.get(), publishCert = publishCert)
  @[]

proc acceptBatch(node: RwadNode, batch: Batch, publishVote = false, publishCert = false): bool =
  let isNew = node.consensus.ingestBatch(batch)
  if not isNew:
    when defined(rwad_diag):
      echo "RWAD_DIAG acceptBatch duplicate node=", node.identity.account[0 .. min(node.identity.account.high, 7)],
        " proposer=", batch.proposer[0 .. min(batch.proposer.high, 7)],
        " round=", batch.round,
        " batch=", batch.batchId[0 .. min(batch.batchId.high, 7)]
    return false
  node.persistBatchEnvelope(batch)
  when defined(rwad_diag):
    echo "RWAD_DIAG acceptBatch new node=", node.identity.account[0 .. min(node.identity.account.high, 7)],
      " proposer=", batch.proposer[0 .. min(batch.proposer.high, 7)],
      " round=", batch.round,
      " batch=", batch.batchId[0 .. min(batch.batchId.high, 7)],
      " txs=", batch.transactions.len
  if batch.round > node.consensus.proposedRound:
    node.scheduleRounds(int(batch.round - node.consensus.proposedRound))
  if not node.network.isNil:
    node.spawnBackgroundTask(node.publishBatchTask(batch))
  discard node.castLocalVote(batch, publishVote = publishVote, publishCert = publishCert)
  node.ensureVoteCollector(batch)
  true

proc driveConsensus(node: RwadNode, maxSteps = 1): seq[FinalizedBlock] =
  if node.identity.account notin node.state.validators:
    return @[]
  for _ in 0 ..< maxSteps:
    let needsCatchupProposal = node.consensus.proposedRound < node.consensus.currentRound
    if node.consensus.txPool.len == 0 and node.scheduledRounds <= 0 and not needsCatchupProposal:
      break
    let nextRound = node.consensus.proposedRound + 1
    if nextRound > 1 and not node.consensus.hasCertifiedRoundQuorum(nextRound - 1):
      if not node.network.isNil:
        node.spawnBackgroundTask(node.fetchRoundBatchesTask(nextRound - 1))
      when defined(rwad_diag):
        echo "RWAD_DIAG driveConsensus blocked node=", node.identity.account[0 .. min(node.identity.account.high, 7)],
          " nextRound=", nextRound,
          " currentRound=", node.consensus.currentRound,
          " proposedRound=", node.consensus.proposedRound,
          " scheduled=", node.scheduledRounds,
          " txPool=", node.consensus.txPool.len
      break
    if node.scheduledRounds > 0:
      dec node.scheduledRounds
    let batch = node.consensus.buildBatch()
    when defined(rwad_diag):
      echo "RWAD_DIAG driveConsensus build node=", node.identity.account[0 .. min(node.identity.account.high, 7)],
        " round=", batch.round,
        " batch=", batch.batchId[0 .. min(batch.batchId.high, 7)],
        " txs=", batch.transactions.len,
        " parents=", batch.parents.len,
        " currentRound=", node.consensus.currentRound,
        " proposedRound=", node.consensus.proposedRound
    node.persistBatchEnvelope(batch)
    if not node.network.isNil:
      node.spawnBackgroundTask(node.network.publishBatch(batch))
    result.add(node.castLocalVote(batch, publishVote = true, publishCert = true))
    node.ensureVoteCollector(batch)

proc start*(node: RwadNode) {.async.} =
  node.running = true
  await node.network.start()
  if node.consensusTask.isNil or node.consensusTask.finished():
    node.consensusTask = node.maintenanceLoop()
    asyncSpawn node.consensusTask

proc stop*(node: RwadNode) {.async.} =
  node.running = false
  if not node.consensusTask.isNil and not node.consensusTask.finished():
    try:
      let cancel = node.consensusTask.cancelAndWait()
      if cancel != nil:
        await cancel
    except CatchableError:
      discard
  node.consensusTask = nil
  await node.cancelBackgroundTasks()
  if not node.network.isNil:
    await node.network.stop()
  if not node.store.isNil:
    node.store.close()

proc routingPlaneStatus*(node: RwadNode): RoutingPlaneStatus =
  if node.isNil:
    return RoutingPlaneStatus()
  if not node.network.isNil:
    return node.network.routingPlaneStatus()
  RoutingPlaneStatus(
    mode: node.config.routingMode,
    primary: node.config.primaryPlane,
    shadowMode: node.config.routingMode == RoutingPlaneMode.dualStack,
  )

proc submitTx*(node: RwadNode, tx: Tx): SubmitTxResult =
  try:
    node.store.persistTx(tx)
    node.consensus.addTx(tx)
    node.scheduleRounds(1)
    result = SubmitTxResult(txId: tx.txId, accepted: true, finalizedBlockIds: @[], finalizedHeights: @[])
    let finalized = node.driveConsensus(2)
    for blk in finalized:
      result.finalizedBlockIds.add(blk.blockId)
      result.finalizedHeights.add(blk.height)
  except CatchableError:
    result = SubmitTxResult(txId: tx.txId, accepted: false, finalizedBlockIds: @[], finalizedHeights: @[])

proc fetchBatchFromProposer(node: RwadNode, proposer, batchId: string): Future[bool] {.async: (raises: []).} =
  if node.consensus.hasBatch(batchId):
    return true
  if node.network.isNil:
    return false
  var providers: seq[string] = @[]
  var seen = initHashSet[string]()
  let proposerPeerId = node.validatorPeerId(proposer)
  if proposerPeerId.len > 0 and proposerPeerId notin seen:
    providers.add(proposerPeerId)
    seen.incl(proposerPeerId)
  for validator in node.state.activeValidators():
    if validator.peerId.len == 0 or validator.address == node.identity.account:
      continue
    if validator.peerId in seen:
      continue
    providers.add(validator.peerId)
    seen.incl(validator.peerId)
  if providers.len == 0:
    return false
  for peerId in providers:
    let payload = await node.fetchRawBounded(
      peerId,
      "batch:" & batchId,
      timeout = 1.seconds,
      maxAttempts = 1,
    )
    if payload.isNone():
      continue
    try:
      let batch = decodeObj[Batch](stringOf(payload.get()))
      var known = false
      {.cast(gcsafe).}:
        discard node.acceptBatch(batch, publishVote = true, publishCert = true)
        if node.scheduledRounds > 0:
          discard node.driveConsensus(1)
        known = node.consensus.hasBatch(batchId)
      if known:
        return true
    except Exception:
      discard
  false

proc fetchCertificateFromProposer(
    node: RwadNode, proposer, batchId: string, attempts = 80, delayMs = 250
): Future[bool] {.async: (raises: []).} =
  if node.consensus.hasCertificateForBatch(batchId):
    when defined(rwad_diag):
      echo "RWAD_DIAG fetchCert cached node=", node.identity.account[0 .. min(node.identity.account.high, 7)],
        " proposer=", proposer[0 .. min(proposer.high, 7)],
        " batch=", batchId[0 .. min(batchId.high, 7)]
    return true
  if node.network.isNil or not node.running:
    return false
  var providers: seq[string] = @[]
  var seen = initHashSet[string]()
  let proposerPeerId = node.validatorPeerId(proposer)
  if proposerPeerId.len > 0 and proposerPeerId notin seen:
    providers.add(proposerPeerId)
    seen.incl(proposerPeerId)
  for validator in node.state.activeValidators():
    if validator.peerId.len == 0 or validator.address == node.identity.account:
      continue
    if validator.peerId in seen:
      continue
    providers.add(validator.peerId)
    seen.incl(validator.peerId)
  if providers.len == 0:
    return false
  for attempt in 0 ..< max(1, attempts):
    if not node.running:
      return false
    var sawPayload = false
    for peerId in providers:
      let payload = await node.fetchRawBounded(
        peerId,
        "certbybatch:" & batchId,
        timeout = 1.seconds,
        maxAttempts = 1,
      )
      if payload.isNone():
        continue
      sawPayload = true
      when defined(rwad_diag):
        echo "RWAD_DIAG fetchCert payload node=", node.identity.account[0 .. min(node.identity.account.high, 7)],
          " proposer=", proposer[0 .. min(proposer.high, 7)],
          " batch=", batchId[0 .. min(batchId.high, 7)],
          " attempt=", attempt
      try:
        let cert = decodeObj[BatchCertificate](stringOf(payload.get()))
        var known = false
        {.cast(gcsafe).}:
          discard node.acceptCertificate(cert)
          known = node.consensus.hasCertificateForBatch(batchId)
        if known:
          return true
      except Exception:
        discard
    if not sawPayload and attempt == 0 and defined(rwad_diag):
      echo "RWAD_DIAG fetchCert miss node=", node.identity.account[0 .. min(node.identity.account.high, 7)],
        " proposer=", proposer[0 .. min(proposer.high, 7)],
        " batch=", batchId[0 .. min(batchId.high, 7)]
    if attempt + 1 < attempts:
      try:
        await sleepAsync(delayMs.milliseconds)
      except CancelledError:
        return false
  false

proc fetchVoteFromValidator(
    node: RwadNode, peerId: string, batchId: string, voter: string
): Future[void] {.async: (raises: []).} =
  if not node.running:
    return
  when defined(rwad_diag):
    echo "RWAD_DIAG fetchVote try node=", node.identity.account[0 .. min(node.identity.account.high, 7)],
      " voter=", voter[0 .. min(voter.high, 7)],
      " batch=", batchId[0 .. min(batchId.high, 7)]
  let payload = await node.fetchRawBounded(
    peerId,
    "vote:" & batchId & ":" & voter,
    timeout = 1.seconds,
    maxAttempts = 1,
  )
  if payload.isNone():
    when defined(rwad_diag):
      echo "RWAD_DIAG fetchVote miss node=", node.identity.account[0 .. min(node.identity.account.high, 7)],
        " voter=", voter[0 .. min(voter.high, 7)],
        " batch=", batchId[0 .. min(batchId.high, 7)]
    return
  when defined(rwad_diag):
    echo "RWAD_DIAG fetchVote hit node=", node.identity.account[0 .. min(node.identity.account.high, 7)],
      " voter=", voter[0 .. min(voter.high, 7)],
      " batch=", batchId[0 .. min(batchId.high, 7)]
  try:
    let vote = decodeObj[BatchVote](stringOf(payload.get()))
    var outcome: VoteAcceptResult
    {.cast(gcsafe).}:
      try:
        outcome = node.acceptKnownVote(vote, publishCert = true, relayVote = true)
      except Exception:
        discard
    if outcome.needsCertFetch:
      node.ensureCertificateFetcher(vote.proposer, vote.batchId)
  except Exception:
    discard

proc collectVotesForBatch(
    node: RwadNode, batch: Batch, attempts = 160, delayMs = 250
): Future[bool] {.async: (raises: []).} =
  if node.consensus.hasCertificateForBatch(batch.batchId):
    return true
  if node.network.isNil or not node.running:
    return false
  let validators = node.state.activeValidators()
  for attempt in 0 ..< max(1, attempts):
    if not node.running:
      return false
    var missingVoters = 0
    for validator in validators:
      let hasLocalVote =
        validator.address in node.consensus.votesByBatch.getOrDefault(
          batch.batchId,
          initOrderedTable[string, CertificateVote](),
        )
      if hasLocalVote:
        continue
      inc missingVoters
      if validator.address == node.identity.account or validator.peerId.len == 0:
        continue
      node.ensureVoteFetcher(validator.peerId, batch.batchId, validator.address)
    when defined(rwad_diag):
      if attempt == 0 or missingVoters > 0:
        echo "RWAD_DIAG collectVotes node=", node.identity.account[0 .. min(node.identity.account.high, 7)],
          " proposer=", batch.proposer[0 .. min(batch.proposer.high, 7)],
          " round=", batch.round,
          " batch=", batch.batchId[0 .. min(batch.batchId.high, 7)],
          " missingVoters=", missingVoters,
          " attempt=", attempt
    if node.consensus.hasCertificateForBatch(batch.batchId):
      return true
    if attempt + 1 < attempts:
      try:
        await sleepAsync(delayMs.milliseconds)
      except CancelledError:
        return false
  node.consensus.hasCertificateForBatch(batch.batchId)

proc collectVotesTask(node: RwadNode, batch: Batch): Future[void] {.async: (raises: []).} =
  try:
    discard await node.collectVotesForBatch(batch)
  finally:
    node.collectingVotes.excl(batch.batchId)

proc fetchBatchByRoundFromProposer(
    node: RwadNode, proposer: string, round: uint64, attempts = 80, delayMs = 250
): Future[bool] {.async: (raises: []).} =
  if node.network.isNil or not node.running:
    return false
  for batch in node.consensus.batches.values:
    if batch.proposer == proposer and batch.round == round:
      when defined(rwad_diag):
        echo "RWAD_DIAG fetchRound cached node=", node.identity.account[0 .. min(node.identity.account.high, 7)],
          " proposer=", proposer[0 .. min(proposer.high, 7)],
          " round=", round,
          " batch=", batch.batchId[0 .. min(batch.batchId.high, 7)]
      return true
  var providers: seq[string] = @[]
  var seen = initHashSet[string]()
  let proposerPeerId = node.validatorPeerId(proposer)
  if proposerPeerId.len > 0 and proposerPeerId notin seen:
    providers.add(proposerPeerId)
    seen.incl(proposerPeerId)
  for validator in node.state.activeValidators():
    if validator.peerId.len == 0 or validator.address == node.identity.account:
      continue
    if validator.peerId in seen:
      continue
    providers.add(validator.peerId)
    seen.incl(validator.peerId)
  if providers.len == 0:
    return false
  for attempt in 0 ..< max(1, attempts):
    if not node.running:
      return false
    var sawPayload = false
    for peerId in providers:
      let payload = await node.fetchRawBounded(
        peerId,
        "batchbyround:" & proposer & ":" & $round,
        timeout = 1.seconds,
        maxAttempts = 1,
      )
      if payload.isNone():
        continue
      sawPayload = true
      when defined(rwad_diag):
        echo "RWAD_DIAG fetchRound payload node=", node.identity.account[0 .. min(node.identity.account.high, 7)],
          " proposer=", proposer[0 .. min(proposer.high, 7)],
          " round=", round,
          " attempt=", attempt
      try:
        let batch = decodeObj[Batch](stringOf(payload.get()))
        var known = false
        {.cast(gcsafe).}:
          discard node.acceptBatch(batch, publishVote = true, publishCert = true)
          if node.scheduledRounds > 0:
            discard node.driveConsensus(1)
          known = node.consensus.hasBatch(batch.batchId)
        if known:
          if batch.round >= 2 and not node.consensus.hasCertificateForBatch(batch.batchId):
            node.ensureCertificateFetcher(batch.proposer, batch.batchId)
          return true
      except Exception:
        discard
    if not sawPayload and attempt == 0 and defined(rwad_diag):
      echo "RWAD_DIAG fetchRound miss node=", node.identity.account[0 .. min(node.identity.account.high, 7)],
        " proposer=", proposer[0 .. min(proposer.high, 7)],
        " round=", round
    if attempt + 1 < attempts:
      try:
        await sleepAsync(delayMs.milliseconds)
      except CancelledError:
        return false
  false

proc fetchRoundBatchesTask(node: RwadNode, round: uint64): Future[void] {.async: (raises: []).} =
  if not node.running:
    return
  let validators = node.state.activeValidators()
  for validator in validators:
    if validator.address == node.identity.account or validator.peerId.len == 0:
      continue
    node.ensureRoundFetcher(validator.address, round)

proc fetchMissingRoundTask(
    node: RwadNode, proposer: string, round: uint64, key: string
): Future[void] {.async: (raises: []).} =
  try:
    discard await node.fetchBatchByRoundFromProposer(proposer, round)
  finally:
    node.fetchingRounds.excl(key)

proc fetchMissingCertificateTask(
    node: RwadNode, proposer: string, batchId: string, key: string
): Future[void] {.async: (raises: []).} =
  try:
    discard await node.fetchCertificateFromProposer(proposer, batchId)
  finally:
    node.fetchingCertificates.excl(key)

proc maintenanceLoop(node: RwadNode): Future[void] {.async: (raises: []).} =
  let tickMs = max(250, node.config.consensusTickMs)
  while node.running:
    var pending: seq[Batch] = @[]
    var replayVotes: seq[BatchVote] = @[]
    var directVotes: seq[BatchVote] = @[]
    var directBatches: seq[(string, Batch)] = @[]
    var missingRounds: seq[(string, uint64)] = @[]
    var missingCertificates: seq[(string, string)] = @[]
    {.cast(gcsafe).}:
      try:
        if node.scheduledRounds > 0 or node.consensus.txPool.len > 0:
          discard node.driveConsensus(1)
      except Exception:
        discard
      let validators = node.state.activeValidators()
      var targetRound = max(node.consensus.currentRound, node.consensus.proposedRound)
      for _, batch in node.consensus.batches.pairs:
        targetRound = max(targetRound, batch.round)
      for _, cert in node.consensus.certificates.pairs:
        targetRound = max(targetRound, cert.round)
      if node.consensus.currentRound > 0:
        targetRound = max(targetRound, node.consensus.currentRound + 1)
      for _, batch in node.consensus.batches.pairs:
        if batch.batchId in node.consensus.certificateByBatch:
          continue
        let knownVotes = node.consensus.votesByBatch.getOrDefault(
          batch.batchId,
          initOrderedTable[string, CertificateVote](),
        )
        if node.identity.account notin knownVotes:
          try:
            discard node.castLocalVote(batch, publishVote = true, publishCert = true)
          except Exception:
            discard
      if targetRound > 0:
        for validator in validators:
          for round in 1'u64 .. targetRound:
            var known = false
            for batch in node.consensus.batches.values:
              if batch.proposer == validator.address and batch.round == round:
                known = true
                break
            if not known:
              known = node.safeLoadBatchByRound(validator.address, round).isSome()
            if not known:
              missingRounds.add((validator.address, round))
              break
      for batchId, batch in node.consensus.batches.pairs:
        if batchId notin node.consensus.certificateByBatch:
          pending.add(batch)
          if node.safeLoadCertificateByBatch(batch.batchId).isNone():
            missingCertificates.add((batch.proposer, batch.batchId))
          for validator in validators:
            let storedVote = node.safeLoadVote(batch.batchId, validator.address)
            if storedVote.isSome():
              replayVotes.add(storedVote.get())
          let batchVotes = node.consensus.votesByBatch.getOrDefault(
            batch.batchId,
            initOrderedTable[string, CertificateVote](),
          )
          if batch.proposer == node.identity.account:
            for validator in validators:
              if validator.address == node.identity.account or validator.peerId.len == 0:
                continue
              if validator.address notin batchVotes:
                directBatches.add((validator.peerId, batch))
          let localVote = batchVotes.getOrDefault(node.identity.account, CertificateVote())
          if localVote.voter.len > 0:
            let vote = BatchVote(
              batchId: batch.batchId,
              proposer: batch.proposer,
              epoch: batch.epoch,
              round: batch.round,
              batchHash: computeBatchId(batch),
              vote: localVote,
            )
            directVotes.add(vote)
    for batch in pending:
      if batch.proposer == node.identity.account and not node.network.isNil:
        node.spawnBackgroundTask(node.publishBatchTask(batch))
      node.ensureVoteCollector(batch)
    for vote in replayVotes:
      var outcome: VoteAcceptResult
      {.cast(gcsafe).}:
        try:
          outcome = node.acceptKnownVote(vote, publishCert = true)
        except Exception:
          discard
      if outcome.needsCertFetch:
        node.ensureCertificateFetcher(vote.proposer, vote.batchId)
    for item in directBatches:
      node.ensureBatchSubmit(
        item[0],
        item[1],
        attempts = 8,
        delayMs = tickMs,
      )
    for vote in directVotes:
      node.ensureVoteSubmitToProposer(
        vote,
        attempts = 8,
        delayMs = tickMs,
      )
    for item in missingRounds:
      node.ensureRoundFetcher(item[0], item[1])
    for item in missingCertificates:
      node.ensureCertificateFetcher(item[0], item[1])
    try:
      await sleepAsync(tickMs.milliseconds)
    except CancelledError:
      return

proc submitVoteToValidator(
    node: RwadNode, peerId: string, vote: BatchVote
    , attempts = 80, delayMs = 150
): Future[bool] {.gcsafe, async: (raises: []).} =
  if node.network.isNil or not node.running or peerId.len == 0:
    return false
  when defined(rwad_diag):
    echo "RWAD_DIAG submitVote start node=", node.identity.account[0 .. min(node.identity.account.high, 7)],
      " proposer=", vote.proposer[0 .. min(vote.proposer.high, 7)],
      " round=", vote.round,
      " voter=", vote.vote.voter[0 .. min(vote.vote.voter.high, 7)],
      " batch=", vote.batchId[0 .. min(vote.batchId.high, 7)]
  try:
    for attempt in 0 ..< max(1, attempts):
      if not node.running:
        return false
      let ack = await node.network.submitVote(
        peerId,
        vote,
        timeout = 1.seconds,
      )
      when defined(rwad_diag):
        echo "RWAD_DIAG submitVote node=", node.identity.account[0 .. min(node.identity.account.high, 7)],
          " proposer=", vote.proposer[0 .. min(vote.proposer.high, 7)],
          " round=", vote.round,
          " voter=", vote.vote.voter[0 .. min(vote.vote.voter.high, 7)],
          " batch=", vote.batchId[0 .. min(vote.batchId.high, 7)],
          " ack=", ack,
          " attempt=", attempt
      if ack:
        return true
      if attempt + 1 < attempts:
        try:
          await sleepAsync(delayMs.milliseconds)
        except CancelledError:
          return false
    return false
  except Exception:
    return false

proc submitVoteToValidators(
    node: RwadNode, vote: BatchVote, attempts = 80, delayMs = 150
): Future[void] {.gcsafe, async: (raises: []).} =
  if node.network.isNil or not node.running:
    return
  let validators = node.state.activeValidators()
  var submits: seq[Future[void]] = @[]
  for validator in validators:
    if validator.peerId.len == 0:
      continue
    if validator.address == node.identity.account or validator.address == vote.vote.voter:
      continue
    submits.add(
      node.submitVoteToValidatorTask(
        validator.peerId,
        vote,
        attempts = attempts,
        delayMs = delayMs,
      )
    )
  if submits.len == 0:
    return
  try:
    await allFutures(submits)
  except CancelledError:
    return

proc submitVoteToValidatorTask(
    node: RwadNode, peerId: string, vote: BatchVote
    , attempts = 80, delayMs = 150
): Future[void] {.gcsafe, async: (raises: []).} =
  discard await node.submitVoteToValidator(peerId, vote, attempts = attempts, delayMs = delayMs)

proc submitVoteTask(
    node: RwadNode, vote: BatchVote, attempts = 80, delayMs = 150
): Future[void] {.gcsafe, async: (raises: []).} =
  await node.submitVoteToValidators(vote, attempts = attempts, delayMs = delayMs)

proc completedFuture(): Future[void] {.raises: [].} =
  let fut = newFuture[void]("rwad.completed")
  fut.complete()
  fut

proc publishBatchTask(node: RwadNode, batch: Batch): Future[void] {.gcsafe, raises: [].} =
  if node.network.isNil or not node.running:
    return completedFuture()
  try:
    node.network.publishBatch(batch)
  except CatchableError:
    completedFuture()

proc publishVoteTask(node: RwadNode, vote: BatchVote): Future[void] {.gcsafe, raises: [].} =
  if node.network.isNil or not node.running:
    return completedFuture()
  try:
    node.network.publishVote(vote)
  except CatchableError:
    completedFuture()

proc publishCertificateTask(node: RwadNode, cert: BatchCertificate): Future[void] {.gcsafe, raises: [].} =
  if node.network.isNil or not node.running:
    return completedFuture()
  try:
    node.network.publishCertificate(cert)
  except CatchableError:
    completedFuture()

proc submitCertificateToValidator(
    node: RwadNode, peerId: string, cert: BatchCertificate
    , attempts = 20, delayMs = 250
): Future[void] {.gcsafe, async: (raises: []).} =
  if node.network.isNil or not node.running or peerId.len == 0:
    return
  when defined(rwad_diag):
    echo "RWAD_DIAG submitCert start node=", node.identity.account[0 .. min(node.identity.account.high, 7)],
      " proposer=", cert.proposer[0 .. min(cert.proposer.high, 7)],
      " round=", cert.round,
      " batch=", cert.batchId[0 .. min(cert.batchId.high, 7)]
  for attempt in 0 ..< max(1, attempts):
    if not node.running:
      return
    let ack = await node.network.submitCertificate(
      peerId,
      cert,
      timeout = 1.seconds,
    )
    when defined(rwad_diag):
      echo "RWAD_DIAG submitCert node=", node.identity.account[0 .. min(node.identity.account.high, 7)],
        " proposer=", cert.proposer[0 .. min(cert.proposer.high, 7)],
        " round=", cert.round,
        " batch=", cert.batchId[0 .. min(cert.batchId.high, 7)],
        " ack=", ack,
        " attempt=", attempt
    if ack:
      return
    if attempt + 1 < attempts:
      try:
        await sleepAsync(delayMs.milliseconds)
      except CancelledError:
        return

proc submitCertificateToValidators(
    node: RwadNode, cert: BatchCertificate, attempts = 20, delayMs = 250
): Future[void] {.gcsafe, async: (raises: []).} =
  if node.network.isNil or not node.running:
    return
  let validators = node.state.activeValidators()
  var submits: seq[Future[void]] = @[]
  for validator in validators:
    if validator.address == node.identity.account or validator.peerId.len == 0:
      continue
    submits.add(
      node.submitCertificateToValidator(
        validator.peerId,
        cert,
        attempts = attempts,
        delayMs = delayMs,
      )
    )
  if submits.len > 0:
    try:
      await allFutures(submits)
    except CancelledError:
      return

proc submitBatchToValidator(
    node: RwadNode, peerId: string, batch: Batch
    , attempts = 20, delayMs = 250
): Future[void] {.gcsafe, async: (raises: []).} =
  if node.network.isNil or not node.running or peerId.len == 0:
    return
  when defined(rwad_diag):
    echo "RWAD_DIAG submitBatch start node=", node.identity.account[0 .. min(node.identity.account.high, 7)],
      " proposer=", batch.proposer[0 .. min(batch.proposer.high, 7)],
      " round=", batch.round,
      " batch=", batch.batchId[0 .. min(batch.batchId.high, 7)]
  for attempt in 0 ..< max(1, attempts):
    if not node.running:
      return
    let ack = await node.network.submitBatch(
      peerId,
      batch,
      timeout = 1.seconds,
    )
    when defined(rwad_diag):
      echo "RWAD_DIAG submitBatch node=", node.identity.account[0 .. min(node.identity.account.high, 7)],
        " proposer=", batch.proposer[0 .. min(batch.proposer.high, 7)],
        " round=", batch.round,
        " batch=", batch.batchId[0 .. min(batch.batchId.high, 7)],
        " ack=", ack,
        " attempt=", attempt
    if ack:
      return
    if attempt + 1 < attempts:
      try:
        await sleepAsync(delayMs.milliseconds)
      except CancelledError:
        return

proc submitBatchToValidators(
    node: RwadNode, batch: Batch, attempts = 20, delayMs = 250
): Future[void] {.gcsafe, async: (raises: []).} =
  if node.network.isNil or not node.running:
    return
  let validators = node.state.activeValidators()
  var submits: seq[Future[void]] = @[]
  for validator in validators:
    if validator.address == node.identity.account or validator.peerId.len == 0:
      continue
    submits.add(
      node.submitBatchToValidator(
        validator.peerId,
        batch,
        attempts = attempts,
        delayMs = delayMs,
      )
    )
  if submits.len > 0:
    try:
      await allFutures(submits)
    except CancelledError:
      return

proc handleNetworkBatch(node: RwadNode, batch: Batch): Future[void] {.gcsafe, raises: [], async: (raises: []).} =
  try:
    var needsCertFetch = false
    {.cast(gcsafe).}:
      discard node.acceptBatch(batch, publishVote = true, publishCert = true)
      if node.scheduledRounds > 0:
        discard node.driveConsensus(1)
      needsCertFetch = batch.round >= 2 and not node.consensus.hasCertificateForBatch(batch.batchId)
    if needsCertFetch:
      node.ensureCertificateFetcher(batch.proposer, batch.batchId)
  except Exception as exc:
    warn "rwad batch handler failed",
      batchId = batch.batchId,
      proposer = batch.proposer,
      round = batch.round,
      err = exc.msg

proc handleNetworkVote(node: RwadNode, vote: BatchVote): Future[bool] {.gcsafe, raises: [], async: (raises: []).} =
  try:
    if not node.consensus.hasBatch(vote.batchId):
      when defined(rwad_diag):
        echo "RWAD_DIAG handleVote fetchBatch node=", node.identity.account[0 .. min(node.identity.account.high, 7)],
          " proposer=", vote.proposer[0 .. min(vote.proposer.high, 7)],
          " round=", vote.round,
          " batch=", vote.batchId[0 .. min(vote.batchId.high, 7)],
          " voter=", vote.vote.voter[0 .. min(vote.vote.voter.high, 7)]
      let fetched = await node.fetchBatchFromProposer(vote.proposer, vote.batchId)
      if not fetched:
        when defined(rwad_diag):
          echo "RWAD_DIAG handleVote missingBatch node=", node.identity.account[0 .. min(node.identity.account.high, 7)],
            " proposer=", vote.proposer[0 .. min(vote.proposer.high, 7)],
            " round=", vote.round,
            " batch=", vote.batchId[0 .. min(vote.batchId.high, 7)],
            " voter=", vote.vote.voter[0 .. min(vote.vote.voter.high, 7)]
        return false
    var outcome: VoteAcceptResult
    {.cast(gcsafe).}:
      outcome = node.acceptKnownVote(vote, publishCert = true, relayVote = true)
    when defined(rwad_diag):
      echo "RWAD_DIAG handleVote accepted node=", node.identity.account[0 .. min(node.identity.account.high, 7)],
        " proposer=", vote.proposer[0 .. min(vote.proposer.high, 7)],
        " round=", vote.round,
        " batch=", vote.batchId[0 .. min(vote.batchId.high, 7)],
        " voter=", vote.vote.voter[0 .. min(vote.vote.voter.high, 7)],
        " accepted=", outcome.accepted,
        " isNew=", outcome.isNew,
        " needsCertFetch=", outcome.needsCertFetch
    if outcome.needsCertFetch:
      node.ensureCertificateFetcher(vote.proposer, vote.batchId)
    return outcome.accepted
  except Exception as exc:
    warn "rwad vote handler failed",
      batchId = vote.batchId,
      proposer = vote.proposer,
      round = vote.round,
      voter = vote.vote.voter,
      err = exc.msg
  false

proc handleNetworkCertificate(
    node: RwadNode, cert: BatchCertificate
): Future[void] {.gcsafe, raises: [], async: (raises: []).} =
  try:
    if not node.consensus.hasBatch(cert.batchId):
      let fetched = await node.fetchBatchFromProposer(cert.proposer, cert.batchId)
      if not fetched:
        return
    {.cast(gcsafe).}:
      discard node.acceptCertificate(cert)
      if node.scheduledRounds > 0:
        discard node.driveConsensus(1)
  except Exception as exc:
    warn "rwad certificate handler failed",
      batchId = cert.batchId,
      proposer = cert.proposer,
      round = cert.round,
      certificateId = cert.certificateId,
      err = exc.msg

proc handleNetworkEvent(
    node: RwadNode, topic: string, payload: string
): Future[void] {.gcsafe, raises: [], async: (raises: []).} =
  discard node
  discard topic
  discard payload

proc storeBlob*(node: RwadNode, manifest: ContentManifest, chunksBase64: seq[string]) =
  manifest.validateManifest()
  if manifest.chunks.len != chunksBase64.len:
    raise newException(ValueError, "blob chunk count does not match manifest")
  var total = 0'i64
  var decodedChunks: seq[seq[byte]] = @[]
  for idx, item in chunksBase64:
    let bytes = bytesOf(decode(item))
    if idx >= manifest.chunks.len:
      raise newException(ValueError, "blob chunk index out of bounds")
    if bytes.len != manifest.chunks[idx].size:
      raise newException(ValueError, "blob chunk size does not match manifest")
    if hashHex(bytes) != manifest.chunks[idx].chunkHash:
      raise newException(ValueError, "blob chunk hash does not match manifest")
    total += int64(bytes.len)
    decodedChunks.add(bytes)
  if total != manifest.totalSize:
    raise newException(ValueError, "blob total size does not match manifest")
  node.store.storeBlobHead(manifest)
  for idx, bytes in decodedChunks:
    node.store.storeBlobChunk(manifest.manifestCid, idx, bytes)

proc blobHead*(
    node: RwadNode,
    manifestCid: string,
    reader = "",
    ticketId = "",
    nowTs = 0'i64,
): Option[ContentManifest] =
  node.authorizeBlobManifest(manifestCid, reader, ticketId, nowTs, brkHead)

proc blobChunkBase64*(
    node: RwadNode,
    manifestCid: string,
    index: int,
    reader = "",
    ticketId = "",
    nowTs = 0'i64,
): Option[string] =
  if node.authorizeBlobManifest(manifestCid, reader, ticketId, nowTs, brkChunk).isNone():
    return none(string)
  let chunk = node.store.loadBlobChunk(manifestCid, index)
  if chunk.isNone():
    return none(string)
  some(encode(stringOf(chunk.get())))

proc blobRangeBase64*(
    node: RwadNode,
    manifestCid: string,
    startPos: int,
    endPos: int,
    reader = "",
    ticketId = "",
    nowTs = 0'i64,
): Option[string] =
  if startPos < 0 or endPos <= startPos:
    return none(string)
  let blob = node.concatBlob(manifestCid)
  if blob.isNone():
    return none(string)
  if node.authorizeBlobManifest(manifestCid, reader, ticketId, nowTs, brkRange).isNone():
    return none(string)
  let cappedEnd = min(endPos, blob.get().len)
  if startPos >= cappedEnd:
    return none(string)
  some(encode(stringOf(blob.get()[startPos ..< cappedEnd])))

proc chainStatus*(node: RwadNode): ChainStatusSnapshot =
  ChainStatusSnapshot(
    chainId: node.state.params.chainId,
    height: node.state.height,
    epoch: node.state.epoch,
    stateRoot: node.state.stateRoot,
    totalSupply: node.state.totalRwadSupply,
    lastBlockId: node.state.lastBlockId,
    localAccount: node.identity.account,
    localPeerId: peerIdString(node.identity.peerId),
    validatorCount: node.state.activeValidators().len,
    routingStatus: node.routingPlaneStatus(),
  )

proc consensusStatus*(node: RwadNode): ConsensusStatusSnapshot =
  node.consensus.statusSnapshot()

proc getBlock*(node: RwadNode, height: uint64): Option[FinalizedBlock] =
  node.store.loadBlock(height)

proc getBatch*(node: RwadNode, batchId: string): Option[Batch] =
  if batchId in node.consensus.batches:
    return some(node.consensus.batches[batchId])
  node.store.loadBatch(batchId)

proc getCertificate*(node: RwadNode, certificateId: string): Option[BatchCertificate] =
  if certificateId in node.consensus.certificates:
    return some(node.consensus.certificates[certificateId])
  node.store.loadCertificate(certificateId)

proc feedSnapshot*(node: RwadNode, limit = 50): seq[SocialFeedItemSnapshot] =
  node.state.socialFeedSnapshot(limit)

proc contentDetail*(node: RwadNode, contentId: string): SocialContentDetailSnapshot =
  node.state.socialContentDetailSnapshot(contentId)

proc publishTasks*(node: RwadNode): seq[PublishTaskSnapshot] =
  for _, record in node.state.contents.pairs:
    result.add(record.publishTaskSnapshot())

proc reportList*(node: RwadNode, contentId = ""): seq[ReportTicket] =
  node.state.reportList(contentId)

proc creditBalance*(node: RwadNode, owner: string): CreditBalanceSnapshot =
  state.creditBalanceSnapshot(node.state, owner)

proc mintIntentStatus*(node: RwadNode, mintIntentId: string): MintIntentStatus =
  node.state.mintIntentStatusSnapshot(mintIntentId)

proc resolveName*(node: RwadNode, name: string, nowTs: int64): Option[NameRecordSnapshot] =
  node.state.resolveNameSnapshot(name, nowTs)
