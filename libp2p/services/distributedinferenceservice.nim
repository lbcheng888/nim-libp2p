{.push raises: [].}

import std/[algorithm, json, options, sequtils, strutils, tables]
from std/times import epochTime

import chronos, chronicles

import ../cid
import ../delegatedrouting
import ../delegatedrouting/store
import ../multiaddress
import ../peerid
import ../peerstore
import ../protocols/bitswap/[bitswap, client, store]
import ../protocols/protocol
import ../stream/connection
import ../stream/lpstream
import ../switch
import ./noderesourceservice

const
  DistributedInferenceCodec* = "/cheng-infer/dispatch/1.0.0"
  DefaultDistributedInferenceMaxMessageBytes* = 64 * 1024

logScope:
  topics = "libp2p distributed-inference"

type
  InferenceLeaseState* {.pure.} = enum
    ilsPending,
    ilsRunning,
    ilsRejected,
    ilsCompleted,
    ilsCancelled,
    ilsFailed

  TaskResultStatus* {.pure.} = enum
    trsSuccess,
    trsRejected,
    trsFailed,
    trsCancelled

  TaskSpec* = object
    taskId*: string
    stickyKey*: string
    modelId*: string
    artifactCids*: seq[string]
    inputCids*: seq[string]
    providerHints*: seq[PeerId]
    requireGpu*: bool
    minMemoryBytes*: int64
    minVramBytes*: int64
    minDiskAvailableBytes*: int64
    minBandwidthBps*: int64
    leaseTtlMs*: int64

  TaskLease* = object
    leaseId*: string
    taskId*: string
    workerPeer*: PeerId
    state*: InferenceLeaseState
    expiresAtMs*: int64
    warmCids*: seq[string]
    missingCids*: seq[string]

  TaskResult* = object
    taskId*: string
    status*: TaskResultStatus
    workerPeer*: PeerId
    outputCids*: seq[string]
    errorMessage*: string

  TaskWarmupSummary* = object
    readyCids*: seq[string]
    missingCids*: seq[string]
    providerUsed*: seq[PeerId]

  DistributedInferenceExecutor* = proc(task: TaskSpec): Future[TaskResult] {.
    gcsafe, raises: [], closure
  .}

  DistributedInferenceConfig* = object
    codec*: string
    requestTimeout*: Duration
    maxRequestBytes*: int
    maxResponseBytes*: int
    defaultLeaseTtl*: Duration
    heartbeatGrace*: Duration

  TaskRuntime = ref object
    conn: Connection

  DispatchOutcomeKind = enum
    dokRetryableFailure,
    dokAccepted,
    dokTerminalFailure

  DispatchOutcome = object
    kind: DispatchOutcomeKind
    result: TaskResult

  DistributedInferenceService* = ref object of Service
    switch*: Switch
    config*: DistributedInferenceConfig
    executor*: DistributedInferenceExecutor
    store*: BitswapBlockStore
    protocol: LPProtocol
    mounted: bool
    leases: Table[string, TaskLease]
    stickyWorkers: Table[string, PeerId]
    active: Table[string, TaskRuntime]

proc bytesToString(data: seq[byte]): string =
  if data.len == 0:
    return ""
  result = newString(data.len)
  copyMem(addr result[0], unsafeAddr data[0], data.len)

proc stringToBytes(text: string): seq[byte] =
  if text.len == 0:
    return @[]
  result = newSeq[byte](text.len)
  copyMem(addr result[0], text.cstring, text.len)

proc nowMillis(): int64 =
  int64(epochTime() * 1000)

proc durationToMillis(value: Duration): int64 =
  value.nanoseconds div 1_000_000

proc jsonGetStr(node: JsonNode, key: string, defaultValue = ""): string =
  if not node.isNil and node.kind == JObject:
    let value = node.getOrDefault(key)
    if not value.isNil and value.kind == JString:
      return value.getStr()
  defaultValue

proc jsonGetBool(node: JsonNode, key: string, defaultValue = false): bool =
  if node.isNil or node.kind != JObject:
    return defaultValue
  let value = node.getOrDefault(key)
  if value.isNil:
    return defaultValue
  case value.kind
  of JBool:
    value.getBool()
  of JInt:
    value.getInt() != 0
  else:
    defaultValue

proc jsonGetInt64(node: JsonNode, key: string, defaultValue = 0'i64): int64 =
  if node.isNil or node.kind != JObject:
    return defaultValue
  let value = node.getOrDefault(key)
  if value.isNil:
    return defaultValue
  case value.kind
  of JInt:
    value.getInt().int64
  of JFloat:
    int64(value.getFloat())
  else:
    defaultValue

proc parsePeerId(text: string): Option[PeerId] =
  var peerId: PeerId
  if peerId.init(text):
    return some(peerId)
  none(PeerId)

proc parsePeerList(node: JsonNode, key: string): seq[PeerId] =
  if node.kind != JObject:
    return @[]
  let values = node.getOrDefault(key)
  if values.isNil or values.kind != JArray:
    return @[]
  for child in values:
    if child.kind != JString:
      continue
    let peerOpt = parsePeerId(child.getStr())
    if peerOpt.isSome():
      result.add(peerOpt.get())

proc encodePeerList(peers: openArray[PeerId]): JsonNode =
  result = newJArray()
  for peerId in peers:
    if peerId.data.len > 0:
      result.add(%($peerId))

proc parseStringList(node: JsonNode, key: string): seq[string] =
  if node.kind != JObject:
    return @[]
  let values = node.getOrDefault(key)
  if values.isNil or values.kind != JArray:
    return @[]
  for child in values:
    if child.kind == JString:
      result.add(child.getStr())

proc encodeStringList(values: openArray[string]): JsonNode =
  result = newJArray()
  for value in values:
    result.add(%value)

proc leaseStateToString(state: InferenceLeaseState): string =
  case state
  of ilsPending:
    "pending"
  of ilsRunning:
    "running"
  of ilsRejected:
    "rejected"
  of ilsCompleted:
    "completed"
  of ilsCancelled:
    "cancelled"
  of ilsFailed:
    "failed"

proc parseLeaseState(text: string): Option[InferenceLeaseState] =
  case text.toLowerAscii()
  of "pending":
    some(ilsPending)
  of "running":
    some(ilsRunning)
  of "rejected":
    some(ilsRejected)
  of "completed":
    some(ilsCompleted)
  of "cancelled":
    some(ilsCancelled)
  of "failed":
    some(ilsFailed)
  else:
    none(InferenceLeaseState)

proc resultStatusToString(status: TaskResultStatus): string =
  case status
  of trsSuccess:
    "success"
  of trsRejected:
    "rejected"
  of trsFailed:
    "failed"
  of trsCancelled:
    "cancelled"

proc parseResultStatus(text: string): Option[TaskResultStatus] =
  case text.toLowerAscii()
  of "success":
    some(trsSuccess)
  of "rejected":
    some(trsRejected)
  of "failed":
    some(trsFailed)
  of "cancelled":
    some(trsCancelled)
  else:
    none(TaskResultStatus)

proc resultState(status: TaskResultStatus): InferenceLeaseState =
  case status
  of trsSuccess:
    ilsCompleted
  of trsRejected:
    ilsRejected
  of trsCancelled:
    ilsCancelled
  of trsFailed:
    ilsFailed

proc init*(
    _: type DistributedInferenceConfig,
    codec = DistributedInferenceCodec,
    requestTimeout = 5.seconds,
    maxRequestBytes = DefaultDistributedInferenceMaxMessageBytes,
    maxResponseBytes = DefaultDistributedInferenceMaxMessageBytes,
    defaultLeaseTtl = 60.seconds,
    heartbeatGrace = 5.seconds,
): DistributedInferenceConfig =
  DistributedInferenceConfig(
    codec: codec,
    requestTimeout: requestTimeout,
    maxRequestBytes: max(1, maxRequestBytes),
    maxResponseBytes: max(1, maxResponseBytes),
    defaultLeaseTtl: defaultLeaseTtl,
    heartbeatGrace: heartbeatGrace,
  )

proc new*(
    T: typedesc[DistributedInferenceService],
    config: DistributedInferenceConfig = DistributedInferenceConfig.init(),
    store: BitswapBlockStore = nil,
    executor: DistributedInferenceExecutor = nil,
): T =
  T(
    config: config,
    executor: executor,
    store: store,
    leases: initTable[string, TaskLease](),
    stickyWorkers: initTable[string, PeerId](),
    active: initTable[string, TaskRuntime](),
  )

proc getDistributedInferenceService*(switch: Switch): DistributedInferenceService =
  if switch.isNil:
    return nil
  for service in switch.services:
    if service of DistributedInferenceService:
      return DistributedInferenceService(service)

proc normalizeTaskSpec(
    svc: DistributedInferenceService, task: TaskSpec
): TaskSpec =
  result = task
  if result.leaseTtlMs <= 0:
    result.leaseTtlMs = durationToMillis(svc.config.defaultLeaseTtl)

proc isValidTaskSpec(task: TaskSpec): bool =
  if task.taskId.len == 0 or task.modelId.len == 0:
    return false
  if task.minMemoryBytes < 0 or task.minVramBytes < 0 or
      task.minDiskAvailableBytes < 0 or task.minBandwidthBps < 0:
    return false
  if task.leaseTtlMs < 0:
    return false
  true

proc makeResult(
    taskId: string,
    status: TaskResultStatus,
    workerPeer: PeerId = PeerId(),
    outputCids: seq[string] = @[],
    errorMessage = "",
): TaskResult =
  TaskResult(
    taskId: taskId,
    status: status,
    workerPeer: workerPeer,
    outputCids: outputCids,
    errorMessage: errorMessage,
  )

proc toJson(task: TaskSpec): JsonNode =
  result = %*{
    "taskId": task.taskId,
    "stickyKey": task.stickyKey,
    "modelId": task.modelId,
    "requireGpu": task.requireGpu,
    "minMemoryBytes": task.minMemoryBytes,
    "minVramBytes": task.minVramBytes,
    "minDiskAvailableBytes": task.minDiskAvailableBytes,
    "minBandwidthBps": task.minBandwidthBps,
    "leaseTtlMs": task.leaseTtlMs,
  }
  result["artifactCids"] = encodeStringList(task.artifactCids)
  result["inputCids"] = encodeStringList(task.inputCids)
  result["providerHints"] = encodePeerList(task.providerHints)

proc decodeTaskSpec(node: JsonNode): Option[TaskSpec] =
  if node.isNil or node.kind != JObject:
    return none(TaskSpec)
  let task = TaskSpec(
    taskId: jsonGetStr(node, "taskId"),
    stickyKey: jsonGetStr(node, "stickyKey"),
    modelId: jsonGetStr(node, "modelId"),
    artifactCids: parseStringList(node, "artifactCids"),
    inputCids: parseStringList(node, "inputCids"),
    providerHints: parsePeerList(node, "providerHints"),
    requireGpu: jsonGetBool(node, "requireGpu"),
    minMemoryBytes: jsonGetInt64(node, "minMemoryBytes"),
    minVramBytes: jsonGetInt64(node, "minVramBytes"),
    minDiskAvailableBytes: jsonGetInt64(node, "minDiskAvailableBytes"),
    minBandwidthBps: jsonGetInt64(node, "minBandwidthBps"),
    leaseTtlMs: jsonGetInt64(node, "leaseTtlMs"),
  )
  if not isValidTaskSpec(task):
    return none(TaskSpec)
  for cidText in task.artifactCids:
    if Cid.init(cidText).isErr:
      return none(TaskSpec)
  for cidText in task.inputCids:
    if Cid.init(cidText).isErr:
      return none(TaskSpec)
  some(task)

proc toJson(lease: TaskLease): JsonNode =
  result = %*{
    "leaseId": lease.leaseId,
    "taskId": lease.taskId,
    "state": leaseStateToString(lease.state),
    "expiresAtMs": lease.expiresAtMs,
  }
  if lease.workerPeer.data.len > 0:
    result["workerPeer"] = %($lease.workerPeer)
  else:
    result["workerPeer"] = newJNull()
  result["warmCids"] = encodeStringList(lease.warmCids)
  result["missingCids"] = encodeStringList(lease.missingCids)

proc decodeLease(node: JsonNode): Option[TaskLease] =
  if node.isNil or node.kind != JObject:
    return none(TaskLease)
  let stateOpt = parseLeaseState(jsonGetStr(node, "state"))
  if stateOpt.isNone():
    return none(TaskLease)
  var workerPeer: PeerId
  discard workerPeer.init(jsonGetStr(node, "workerPeer"))
  let lease = TaskLease(
    leaseId: jsonGetStr(node, "leaseId"),
    taskId: jsonGetStr(node, "taskId"),
    workerPeer: workerPeer,
    state: stateOpt.get(),
    expiresAtMs: jsonGetInt64(node, "expiresAtMs"),
    warmCids: parseStringList(node, "warmCids"),
    missingCids: parseStringList(node, "missingCids"),
  )
  if lease.leaseId.len == 0 or lease.taskId.len == 0:
    return none(TaskLease)
  some(lease)

proc toJson(resultValue: TaskResult): JsonNode =
  result = %*{
    "taskId": resultValue.taskId,
    "status": resultStatusToString(resultValue.status),
    "errorMessage": resultValue.errorMessage,
  }
  if resultValue.workerPeer.data.len > 0:
    result["workerPeer"] = %($resultValue.workerPeer)
  else:
    result["workerPeer"] = newJNull()
  result["outputCids"] = encodeStringList(resultValue.outputCids)

proc decodeResult(node: JsonNode): Option[TaskResult] =
  if node.isNil or node.kind != JObject:
    return none(TaskResult)
  let statusOpt = parseResultStatus(jsonGetStr(node, "status"))
  if statusOpt.isNone():
    return none(TaskResult)
  var workerPeer: PeerId
  discard workerPeer.init(jsonGetStr(node, "workerPeer"))
  let resultValue = TaskResult(
    taskId: jsonGetStr(node, "taskId"),
    status: statusOpt.get(),
    workerPeer: workerPeer,
    outputCids: parseStringList(node, "outputCids"),
    errorMessage: jsonGetStr(node, "errorMessage"),
  )
  if resultValue.taskId.len == 0:
    return none(TaskResult)
  some(resultValue)

proc encodeLeaseRequest(task: TaskSpec): seq[byte] =
  stringToBytes($(%*{"op": "lease_request", "spec": toJson(task)}))

proc encodeLeaseAccept(lease: TaskLease): seq[byte] =
  stringToBytes($(%*{"op": "lease_accept", "lease": toJson(lease)}))

proc encodeLeaseReject(lease: TaskLease, errorMessage: string): seq[byte] =
  stringToBytes($(%*{"op": "lease_reject", "lease": toJson(lease), "errorMessage": errorMessage}))

proc encodeLeaseHeartbeat(lease: TaskLease): seq[byte] =
  stringToBytes($(%*{"op": "lease_heartbeat", "lease": toJson(lease)}))

proc encodeLeaseComplete(resultValue: TaskResult): seq[byte] =
  stringToBytes($(%*{"op": "lease_complete", "result": toJson(resultValue)}))

proc encodeLeaseCancel(taskId: string, errorMessage = "cancelled"): seq[byte] =
  stringToBytes($(%*{"op": "lease_cancel", "taskId": taskId, "errorMessage": errorMessage}))

proc encodeError(errorMessage: string): seq[byte] =
  stringToBytes($(%*{"op": "error", "errorMessage": errorMessage}))

proc decodeOp(payload: seq[byte]): tuple[op: string, node: JsonNode] =
  if payload.len == 0:
    return ("", nil)
  try:
    let node = parseJson(bytesToString(payload))
    if node.kind != JObject:
      return ("", nil)
    (jsonGetStr(node, "op"), node)
  except CatchableError:
    ("", nil)

proc dedupPeers(peers: openArray[PeerId]): seq[PeerId] =
  for peer in peers:
    if peer.data.len == 0:
      continue
    if result.anyIt(it == peer):
      continue
    result.add(peer)

proc getInferenceLease*(
    svc: DistributedInferenceService, taskId: string
): Option[TaskLease] =
  if svc.isNil or taskId.len == 0:
    return none(TaskLease)
  try:
    some(svc.leases[taskId])
  except KeyError:
    none(TaskLease)

proc getInferenceLease*(
    switch: Switch, taskId: string
): Option[TaskLease] =
  let svc = getDistributedInferenceService(switch)
  if svc.isNil:
    return none(TaskLease)
  svc.getInferenceLease(taskId)

proc openConn(
    svc: DistributedInferenceService, peerId: PeerId
): Future[Connection] {.async: (raises: [CancelledError]).} =
  try:
    if svc.switch.isConnected(peerId):
      return await svc.switch.dial(peerId, @[svc.config.codec])

    let protocols = svc.switch.peerStore[ProtoBook][peerId]
    if svc.config.codec notin protocols:
      return nil
    let addrs = svc.switch.peerStore.getAddresses(peerId)
    if addrs.len == 0:
      return nil
    return await svc.switch.dial(peerId, addrs, @[svc.config.codec], forceDial = false)
  except CancelledError as exc:
    raise exc
  except CatchableError:
    return nil

proc peerIsEligible(svc: DistributedInferenceService, peerId: PeerId): bool =
  if svc.isNil or svc.switch.isNil or peerId.data.len == 0:
    return false
  if peerId == svc.switch.peerInfo.peerId:
    return true
  if svc.switch.isConnected(peerId):
    return true
  let protocols = svc.switch.peerStore[ProtoBook][peerId]
  NodeResourceCodec in protocols or svc.config.codec in protocols

proc candidateWorkers(
    svc: DistributedInferenceService, task: TaskSpec
): seq[NodeResourceSnapshot] =
  let resourceSvc = getNodeResourceService(svc.switch)
  if resourceSvc.isNil:
    return @[]
  result = resourceSvc.findResourceCandidates(
    requireGpu = task.requireGpu,
    minMemoryBytes = task.minMemoryBytes,
    minVramBytes = task.minVramBytes,
    minDiskAvailableBytes = task.minDiskAvailableBytes,
    minBandwidthBps = task.minBandwidthBps,
  )
  result.keepItIf(svc.peerIsEligible(it.peerId))
  if task.stickyKey.len > 0 and svc.stickyWorkers.hasKey(task.stickyKey):
    let sticky = svc.stickyWorkers.getOrDefault(task.stickyKey)
    for idx, snapshot in result.pairs():
      if snapshot.peerId == sticky:
        let selected = snapshot
        result.delete(idx)
        result.insert(selected, 0)
        break

proc registerProviderAddresses(
    svc: DistributedInferenceService, record: DelegatedProviderRecord
) =
  if svc.isNil or svc.switch.isNil or record.peerId.data.len == 0 or record.addresses.len == 0:
    return
  svc.switch.peerStore.addAddressesWithTTL(
    record.peerId, record.addresses, DefaultDiscoveredAddressTTL
  )

proc tryFetchFromPeer(
    svc: DistributedInferenceService, peerId: PeerId, cid: Cid
): Future[bool] {.async: (raises: [CancelledError]).} =
  if svc.isNil or svc.store.isNil() or peerId.data.len == 0:
    return false
  let session = BitswapSession.new(
    svc.switch,
    peerId,
    store = svc.store,
    config = BitswapClientConfig.init(
      requestTimeout = svc.config.requestTimeout,
      maxAttempts = 1,
      enableLedger = false,
      maxMissingRetries = 0,
    ),
  )
  defer:
    await session.close()
  try:
    let blockOpt = await session.requestBlock(cid)
    blockOpt.isSome()
  except CancelledError as exc:
    raise exc
  except CatchableError:
    false

proc delegatedProviders(
    svc: DistributedInferenceService, cid: Cid
): Future[seq[DelegatedProviderRecord]] {.async: (raises: [CancelledError]).} =
  var deduped: seq[DelegatedProviderRecord] = @[]
  if svc.isNil or svc.switch.isNil:
    return @[]
  if not svc.switch.delegatedRoutingStore.isNil:
    for record in await svc.switch.delegatedRoutingStore.getProviders($cid):
      result.add(record)
  if svc.switch.hasDelegatedRouting():
    try:
      for record in await svc.switch.findDelegatedProviders(cid):
        result.add(record)
    except DelegatedRoutingError:
      discard
  for record in result:
    if deduped.anyIt(it.peerId == record.peerId):
      continue
    deduped.add(record)
  deduped

proc runExecutor(
    svc: DistributedInferenceService, task: TaskSpec
): Future[TaskResult] {.async: (raises: [CancelledError]).} =
  let execProc: DistributedInferenceExecutor =
    if svc.executor.isNil:
      proc(task: TaskSpec): Future[TaskResult] {.async, gcsafe, closure.} =
        await sleepAsync(25.milliseconds)
        TaskResult(
          taskId: task.taskId,
          status: trsSuccess,
          outputCids: task.inputCids,
        )
    else:
      svc.executor
  try:
    var resultValue = await execProc(task)
    if resultValue.taskId.len == 0:
      resultValue.taskId = task.taskId
    if resultValue.workerPeer.data.len == 0 and not svc.switch.isNil:
      resultValue.workerPeer = svc.switch.peerInfo.peerId
    resultValue
  except CancelledError as exc:
    raise exc
  except CatchableError as exc:
    makeResult(
      task.taskId,
      trsFailed,
      workerPeer = svc.switch.peerInfo.peerId,
      errorMessage = exc.msg,
    )

proc prewarmArtifacts*(
    svc: DistributedInferenceService, task: TaskSpec
): Future[TaskWarmupSummary] {.async: (raises: [CancelledError]).} =
  if svc.isNil:
    result.missingCids = task.artifactCids
    return
  if task.artifactCids.len == 0:
    return
  if svc.store.isNil():
    result.missingCids = task.artifactCids
    return

  let hints = dedupPeers(task.providerHints)
  for cidText in task.artifactCids:
    let cidRes = Cid.init(cidText)
    if cidRes.isErr:
      result.missingCids.add(cidText)
      continue
    let cid = cidRes.get()
    if await svc.store.hasBlock(cid):
      result.readyCids.add(cidText)
      continue

    var fetched = false
    for peerId in hints:
      if await svc.tryFetchFromPeer(peerId, cid):
        result.readyCids.add(cidText)
        result.providerUsed = dedupPeers(result.providerUsed & @[peerId])
        fetched = true
        break
    if fetched:
      continue

    for record in await svc.delegatedProviders(cid):
      if record.schema.len > 0 and record.schema != "bitswap" and record.schema != "peer":
        continue
      if record.protocols.len > 0 and BitswapCodec notin record.protocols:
        continue
      svc.registerProviderAddresses(record)
      if await svc.tryFetchFromPeer(record.peerId, cid):
        result.readyCids.add(cidText)
        result.providerUsed = dedupPeers(result.providerUsed & @[record.peerId])
        fetched = true
        break
    if not fetched:
      result.missingCids.add(cidText)

proc prewarmArtifacts*(
    switch: Switch, task: TaskSpec
): Future[TaskWarmupSummary] {.async: (raises: [CancelledError]).} =
  let svc = getDistributedInferenceService(switch)
  if svc.isNil:
    return TaskWarmupSummary(missingCids: task.artifactCids)
  await svc.prewarmArtifacts(task)

proc waitForLeaseCancel(
    svc: DistributedInferenceService, conn: Connection, taskId: string
): Future[bool] {.async: (raises: [CancelledError]).} =
  while true:
    let payload =
      try:
        await conn.readLp(svc.config.maxRequestBytes)
      except LPStreamError:
        return false
    let (op, node) = decodeOp(payload)
    if op == "lease_cancel" and jsonGetStr(node, "taskId") == taskId:
      return true

proc handleWorkerSession(
    svc: DistributedInferenceService, conn: Connection
): Future[void] {.async: (raises: [CancelledError]).} =
  try:
    let firstPayload = await conn.readLp(svc.config.maxRequestBytes)
    let (op, node) = decodeOp(firstPayload)
    if op != "lease_request":
      await conn.writeLp(encodeError("invalid lease request"))
      return

    let taskOpt = decodeTaskSpec(node.getOrDefault("spec"))
    if taskOpt.isNone():
      await conn.writeLp(encodeError("invalid task spec"))
      return

    let task = svc.normalizeTaskSpec(taskOpt.get())
    let warmup = await svc.prewarmArtifacts(task)
    let baseLease = TaskLease(
      leaseId: task.taskId & ":" & $svc.switch.peerInfo.peerId & ":" & $nowMillis(),
      taskId: task.taskId,
      workerPeer: svc.switch.peerInfo.peerId,
      state: ilsRunning,
      expiresAtMs: nowMillis() + task.leaseTtlMs,
      warmCids: warmup.readyCids,
      missingCids: warmup.missingCids,
    )
    if warmup.missingCids.len > 0:
      var rejected = baseLease
      rejected.state = ilsRejected
      svc.leases[task.taskId] = rejected
      await conn.writeLp(encodeLeaseReject(rejected, "missing required artifacts"))
      return

    svc.leases[task.taskId] = baseLease
    await conn.writeLp(encodeLeaseAccept(baseLease))
    await conn.writeLp(encodeLeaseHeartbeat(baseLease))

    let execFuture = svc.runExecutor(task)
    let cancelFuture = svc.waitForLeaseCancel(conn, task.taskId)
    let heartbeatIntervalMs = max(2_000'i64, task.leaseTtlMs div 3)
    var nextHeartbeatAt = nowMillis() + heartbeatIntervalMs
    var cancelled = false

    while not execFuture.finished():
      if cancelFuture.finished():
        let cancelRequested =
          try:
            await cancelFuture
          except LPStreamError:
            false
        if cancelRequested:
          cancelled = true
          execFuture.cancelSoon()
          break
        return
      if not execFuture.finished() and nowMillis() >= nextHeartbeatAt:
        var heartbeatLease = svc.leases.getOrDefault(task.taskId, baseLease)
        heartbeatLease.state = ilsRunning
        heartbeatLease.expiresAtMs = nowMillis() + task.leaseTtlMs
        svc.leases[task.taskId] = heartbeatLease
        await conn.writeLp(encodeLeaseHeartbeat(heartbeatLease))
        nextHeartbeatAt = nowMillis() + heartbeatIntervalMs
      await sleepAsync(100.milliseconds)

    var resultValue =
      if cancelled:
        makeResult(
          task.taskId,
          trsCancelled,
          workerPeer = svc.switch.peerInfo.peerId,
          errorMessage = "cancelled",
        )
      else:
        await execFuture

    if resultValue.workerPeer.data.len == 0:
      resultValue.workerPeer = svc.switch.peerInfo.peerId
    svc.leases[task.taskId] = TaskLease(
      leaseId: baseLease.leaseId,
      taskId: task.taskId,
      workerPeer: svc.switch.peerInfo.peerId,
      state: resultState(resultValue.status),
      expiresAtMs: nowMillis(),
      warmCids: warmup.readyCids,
      missingCids: warmup.missingCids,
    )
    await conn.writeLp(encodeLeaseComplete(resultValue))
    try:
      await conn.close()
    except CatchableError:
      discard
    if not cancelFuture.finished():
      discard await withTimeout(cancelFuture, 100.milliseconds)
  except LPStreamError:
    return

proc buildProtocol(svc: DistributedInferenceService): LPProtocol =
  let handler: LPProtoHandler =
    proc(conn: Connection, proto: string) {.async: (raises: [CancelledError]).} =
      try:
        await svc.handleWorkerSession(conn)
      except CancelledError as exc:
        raise exc
      except LPStreamError:
        discard
      except CatchableError as exc:
        debug "distributed inference handler failed", error = exc.msg
        try:
          await conn.writeLp(encodeError(exc.msg))
        except CatchableError:
          discard
  LPProtocol.new(@[svc.config.codec], handler)

proc readMessage(
    svc: DistributedInferenceService,
    conn: Connection,
    timeout: Duration,
): Future[seq[byte]] {.async: (raises: [CancelledError]).} =
  let readFuture = conn.readLp(svc.config.maxResponseBytes)
  let completed = await withTimeout(readFuture, timeout)
  if not completed:
    if not readFuture.finished():
      readFuture.cancelSoon()
    return @[]
  try:
    await readFuture
  except LPStreamError:
    @[]

proc tryDispatchToWorker(
    svc: DistributedInferenceService, peerId: PeerId, task: TaskSpec
): Future[DispatchOutcome] {.async: (raises: [CancelledError]).} =
  let conn = await svc.openConn(peerId)
  if conn.isNil:
    return DispatchOutcome(
      kind: dokRetryableFailure,
      result: makeResult(task.taskId, trsFailed, workerPeer = peerId, errorMessage = "dial failed"),
    )

  svc.active[task.taskId] = TaskRuntime(conn: conn)
  defer:
    try:
      await conn.close()
    except CatchableError:
      discard
    if svc.active.hasKey(task.taskId):
      svc.active.del(task.taskId)

  try:
    await conn.writeLp(encodeLeaseRequest(task))
  except LPStreamError:
    return DispatchOutcome(
      kind: dokRetryableFailure,
      result: makeResult(task.taskId, trsFailed, workerPeer = peerId, errorMessage = "write failed"),
    )

  let firstPayload = await svc.readMessage(conn, svc.config.requestTimeout)
  if firstPayload.len == 0:
    return DispatchOutcome(
      kind: dokRetryableFailure,
      result: makeResult(task.taskId, trsFailed, workerPeer = peerId, errorMessage = "lease accept timeout"),
    )
  let (op, node) = decodeOp(firstPayload)
  case op
  of "lease_accept":
    let leaseOpt = decodeLease(node.getOrDefault("lease"))
    if leaseOpt.isNone():
      return DispatchOutcome(
        kind: dokRetryableFailure,
        result: makeResult(task.taskId, trsFailed, workerPeer = peerId, errorMessage = "invalid lease accept"),
      )
    var lease = leaseOpt.get()
    lease.state = ilsRunning
    svc.leases[task.taskId] = lease
    if task.stickyKey.len > 0:
      svc.stickyWorkers[task.stickyKey] = peerId
  of "lease_reject":
    let leaseOpt = decodeLease(node.getOrDefault("lease"))
    if leaseOpt.isSome():
      svc.leases[task.taskId] = leaseOpt.get()
    return DispatchOutcome(
      kind: dokRetryableFailure,
      result: makeResult(
        task.taskId,
        trsRejected,
        workerPeer = peerId,
        errorMessage = jsonGetStr(node, "errorMessage", "lease rejected"),
      ),
    )
  else:
    return DispatchOutcome(
      kind: dokRetryableFailure,
      result: makeResult(
        task.taskId,
        trsFailed,
        workerPeer = peerId,
        errorMessage = jsonGetStr(node, "errorMessage", "unexpected worker response"),
      ),
    )

  let waitTimeout =
    int(task.leaseTtlMs + durationToMillis(svc.config.heartbeatGrace)).milliseconds
  while true:
    let payload = await svc.readMessage(conn, waitTimeout)
    if payload.len == 0:
      let resultValue = makeResult(
        task.taskId,
        trsFailed,
        workerPeer = peerId,
        errorMessage = "worker disconnected after accept",
      )
      svc.leases[task.taskId] = TaskLease(
        leaseId: svc.leases.getOrDefault(task.taskId).leaseId,
        taskId: task.taskId,
        workerPeer: peerId,
        state: ilsFailed,
        expiresAtMs: nowMillis(),
        warmCids: svc.leases.getOrDefault(task.taskId).warmCids,
        missingCids: svc.leases.getOrDefault(task.taskId).missingCids,
      )
      return DispatchOutcome(kind: dokTerminalFailure, result: resultValue)
    let (messageOp, node) = decodeOp(payload)
    case messageOp
    of "lease_heartbeat":
      let leaseOpt = decodeLease(node.getOrDefault("lease"))
      if leaseOpt.isSome():
        var lease = leaseOpt.get()
        lease.state = ilsRunning
        svc.leases[task.taskId] = lease
    of "lease_complete":
      let resultOpt = decodeResult(node.getOrDefault("result"))
      if resultOpt.isNone():
        return DispatchOutcome(
          kind: dokTerminalFailure,
          result: makeResult(task.taskId, trsFailed, workerPeer = peerId, errorMessage = "invalid task result"),
        )
      let resultValue = resultOpt.get()
      let previous = svc.leases.getOrDefault(task.taskId)
      svc.leases[task.taskId] = TaskLease(
        leaseId: previous.leaseId,
        taskId: task.taskId,
        workerPeer: peerId,
        state: resultState(resultValue.status),
        expiresAtMs: nowMillis(),
        warmCids: previous.warmCids,
        missingCids: previous.missingCids,
      )
      return DispatchOutcome(kind: dokAccepted, result: resultValue)
    of "error":
      return DispatchOutcome(
        kind: dokTerminalFailure,
        result: makeResult(
          task.taskId,
          trsFailed,
          workerPeer = peerId,
          errorMessage = jsonGetStr(node, "errorMessage", "worker error"),
        ),
      )
    else:
      discard

proc submitInferenceTask*(
    svc: DistributedInferenceService, task: TaskSpec
): Future[TaskResult] {.async: (raises: [CancelledError]).} =
  if svc.isNil or svc.switch.isNil:
    return makeResult(task.taskId, trsFailed, errorMessage = "distributed inference unavailable")
  let normalized = svc.normalizeTaskSpec(task)
  if not isValidTaskSpec(normalized):
    return makeResult(normalized.taskId, trsFailed, errorMessage = "invalid task spec")

  for cidText in normalized.artifactCids:
    if Cid.init(cidText).isErr:
      return makeResult(normalized.taskId, trsFailed, errorMessage = "invalid artifact cid")
  for cidText in normalized.inputCids:
    if Cid.init(cidText).isErr:
      return makeResult(normalized.taskId, trsFailed, errorMessage = "invalid input cid")

  let resourceSvc = getNodeResourceService(svc.switch)
  if resourceSvc.isNil:
    return makeResult(normalized.taskId, trsFailed, errorMessage = "node resource service unavailable")

  await resourceSvc.hostNodeResourceUpdateAll()
  let candidates = svc.candidateWorkers(normalized)
  if candidates.len == 0:
    return makeResult(normalized.taskId, trsFailed, errorMessage = "no eligible workers")

  let attempts = min(2, candidates.len)
  var lastResult = makeResult(normalized.taskId, trsFailed, errorMessage = "dispatch failed")
  for idx in 0 ..< attempts:
    let peerId = candidates[idx].peerId
    let outcome = await svc.tryDispatchToWorker(peerId, normalized)
    lastResult = outcome.result
    if outcome.kind == dokRetryableFailure and idx + 1 < attempts:
      continue
    return outcome.result
  lastResult

proc submitInferenceTask*(
    switch: Switch, task: TaskSpec
): Future[TaskResult] {.async: (raises: [CancelledError]).} =
  let svc = getDistributedInferenceService(switch)
  if svc.isNil:
    return makeResult(task.taskId, trsFailed, errorMessage = "distributed inference unavailable")
  await svc.submitInferenceTask(task)

proc cancelInferenceTask*(
    svc: DistributedInferenceService, taskId: string
): Future[bool] {.async: (raises: [CancelledError]).} =
  if svc.isNil or taskId.len == 0 or not svc.active.hasKey(taskId):
    return false
  let runtime = svc.active.getOrDefault(taskId)
  try:
    await runtime.conn.writeLp(encodeLeaseCancel(taskId))
  except CatchableError:
    discard
  try:
    await runtime.conn.close()
  except CatchableError:
    discard
  let previous = svc.leases.getOrDefault(taskId)
  svc.leases[taskId] = TaskLease(
    leaseId: previous.leaseId,
    taskId: taskId,
    workerPeer: previous.workerPeer,
    state: ilsCancelled,
    expiresAtMs: nowMillis(),
    warmCids: previous.warmCids,
    missingCids: previous.missingCids,
  )
  true

proc cancelInferenceTask*(
    switch: Switch, taskId: string
): Future[bool] {.async: (raises: [CancelledError]).} =
  let svc = getDistributedInferenceService(switch)
  if svc.isNil:
    return false
  await svc.cancelInferenceTask(taskId)

method setup*(
    svc: DistributedInferenceService, switch: Switch
): Future[bool] {.async: (raises: [CancelledError]).} =
  let hasBeenSetup = await procCall Service(svc).setup(switch)
  if hasBeenSetup:
    svc.switch = switch
    if svc.store.isNil() and not switch.bitswap.isNil:
      svc.store = switch.bitswap.store
    if svc.protocol.isNil:
      svc.protocol = svc.buildProtocol()
    if switch.isStarted() and not svc.protocol.started:
      await svc.protocol.start()
    if not svc.mounted:
      try:
        switch.mount(svc.protocol)
        svc.mounted = true
      except CatchableError as exc:
        warn "failed to mount distributed inference protocol", error = exc.msg
        discard await procCall Service(svc).stop(switch)
        return false
    await svc.run(switch)
  hasBeenSetup

method run*(
    svc: DistributedInferenceService, switch: Switch
) {.async: (raises: [CancelledError]).} =
  discard

method stop*(
    svc: DistributedInferenceService, switch: Switch
): Future[bool] {.async: (raises: [CancelledError]).} =
  let hasBeenStopped = await procCall Service(svc).stop(switch)
  if hasBeenStopped and not svc.protocol.isNil and svc.protocol.started:
    await svc.protocol.stop()
  hasBeenStopped

proc startDistributedInferenceService*(
    switch: Switch,
    config: DistributedInferenceConfig = DistributedInferenceConfig.init(),
    store: BitswapBlockStore = nil,
    executor: DistributedInferenceExecutor = nil,
): Future[DistributedInferenceService] {.async: (raises: [CancelledError]).} =
  if switch.isNil:
    return nil
  discard await startNodeResourceService(switch)
  var svc = getDistributedInferenceService(switch)
  if svc.isNil:
    svc = DistributedInferenceService.new(
      config = config,
      store = store,
      executor = executor,
    )
    switch.services.add(svc)
  elif not store.isNil():
    svc.store = store
  if switch.isStarted():
    discard await svc.setup(switch)
  svc

{.pop.}
