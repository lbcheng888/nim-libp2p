import std/[base64, json, options, os, strutils, tables]

import pebble/batch/db as pebble_db
import pebble/batch/types as batch_types
import pebble/batch/batch as pebble_batch
import pebble/core/types as pebble_core
import pebble/mem/types as mem_types
import pebble/wal/writer as wal_writer
import pebble/wal/reader as wal_reader
import pebble/runtime/resource_manager as resource_manager
import pebble/runtime/executor as pebble_executor
import pebble/compaction/scheduler as compaction_scheduler
import pebble/compaction/types as compaction_types
import pebble/wal/types as wal_types

type
  KVWriteKind* = enum
    kvPut, kvDelete

  KeySpace* = enum
    ksEvent, ksSignature, ksTask, ksIndex, ksCustom

  KVWrite* = object
    kind*: KVWriteKind
    space*: KeySpace
    key*: string
    value*: string

  PebbleKVResourceConfig* = object
    memSoftLimitBytes*: int
    memHardLimitBytes*: int
    throttleEnabled*: bool
    throttleStallLimitBytes*: int
    throttleReleaseTargetBytes*: int
    throttleBaseDelayMs*: int
    throttleMaxDelayMs*: int
    resourceManager*: resource_manager.ResourceManager
    resourceKind*: resource_manager.ResourceKind

  PebbleKVWalConfig* = object
    path*: string
    format*: wal_types.WireFormat
    logNumber*: uint32
    maxSegmentBytes*: int64
    autoSyncOffsets*: bool
    autoSyncBytes*: int64
    maxPendingAutoSyncs*: int
    executor*: pebble_executor.Executor

  PebbleKVCompactionConfig* = object
    scheduler*: compaction_scheduler.CompactionScheduler
    flushTrigger*: mem_types.FlushTrigger
    flushExecutor*: pebble_executor.Executor

  PebbleKVConfig* = object
    dataPath*: string
    resource*: PebbleKVResourceConfig
    wal*: PebbleKVWalConfig
    compaction*: PebbleKVCompactionConfig
    maxConcurrentCommits*: int

  PebbleMemStats* = object
    approxBytes*: int
    softLimitBytes*: int
    hardLimitBytes*: int
    frozen*: bool
    throttleActive*: bool
    lastThrottleDelayMs*: int
    utilization*: float

  PebbleWalStats* = object
    segmentIndex*: int
    segmentPath*: string
    segmentBytes*: int64
    totalBytes*: int64
    pendingAutoSyncs*: int
    bytesSinceSync*: int64

  PebbleCompactionStats* = object
    hasScheduler*: bool
    activeJobs*: int
    pendingJobs*: int
    metrics*: compaction_types.CompactionMetrics

  PebbleKVStats* = object
    mem*: PebbleMemStats
    wal*: PebbleWalStats
    compaction*: PebbleCompactionStats
    resourceUtilization*: float

  PebbleKV* = ref object
    db: pebble_db.DB
    memCfg: mem_types.MemTableConfig
    wal: wal_writer.WalWriter
    basePath: string
    resourceManager: resource_manager.ResourceManager
    compactionScheduler: compaction_scheduler.CompactionScheduler
    cfg: PebbleKVConfig

  KVPair* = object
    ## 逻辑键值对，移除 KeySpace 前缀后返回业务字段。
    id*: string
    value*: string

  KVScanPage* = object
    ## 前缀扫描分页结果，包含游标与快照元数据。
    entries*: seq[KVPair]
    hasMore*: bool
    nextCursor*: Option[string]
    snapshot*: batch_types.Snapshot

  KVSnapIterator* = ref object
    ## 基于快照的只读迭代器，保证遍历顺序稳定。
    space*: KeySpace
    prefix*: string
    snapshot*: batch_types.Snapshot
    entries: seq[KVPair]
    idx: int

  PebbleWriteBatchState* = enum
    kvBatchOpen,
    kvBatchCommitted,
    kvBatchAborted

  PebbleWriteBatch* = ref object
    ## 封装 Pebble 批处理写入，支持多 key 原子提交与回放日志导出。
    kv: PebbleKV
    inner: pebble_batch.BatchBase
    log: seq[KVWrite]
    state: PebbleWriteBatchState
    lastSeq: pebble_core.SequenceNumber

proc withPut*(space: KeySpace; id: string; value: string): KVWrite
proc withDelete*(space: KeySpace; id: string): KVWrite

const
  KeyPrefixes = ["event:", "sig:", "task:", "idx:", ""]
  KeySpaceNames = ["event", "signature", "task", "index", "custom"]
  WriteKindNames = ["put", "delete"]
  zeroSnapshot = batch_types.Snapshot(seq: pebble_core.toSequence(0))

proc readUint32Le(data: seq[byte]; idx: var int): uint32 =
  if idx < 0 or idx + 4 > data.len:
    raise newException(ValueError, "wal payload truncated (uint32)")
  result = uint32(data[idx]) or (uint32(data[idx + 1]) shl 8) or
           (uint32(data[idx + 2]) shl 16) or (uint32(data[idx + 3]) shl 24)
  inc idx, 4

proc readUint64Le(data: seq[byte]; idx: var int): uint64 =
  if idx < 0 or idx + 8 > data.len:
    raise newException(ValueError, "wal payload truncated (uint64)")
  result = 0
  for offset in 0 ..< 8:
    result = result or (uint64(data[idx + offset]) shl (offset * 8))
  inc idx, 8

proc readBytes(data: seq[byte]; idx: var int; count: int): string =
  if count < 0 or idx < 0 or idx + count > data.len:
    raise newException(ValueError, "wal payload truncated (bytes)")
  if count == 0:
    return ""
  result = newString(count)
  copyMem(addr result[0], unsafeAddr data[idx], count)
  inc idx, count

proc decodeWalRecord(payload: seq[byte]): seq[mem_types.ApplyEntry] =
  if payload.len < 4:
    return @[]
  var idx = 0
  try:
    let count = int(readUint32Le(payload, idx))
    for _ in 0 ..< count:
      if idx >= payload.len:
        break
      let kindVal = payload[idx]
      inc idx
      if kindVal > ord(high(mem_types.MemValueKind)):
        raise newException(ValueError, "unknown wal kind")
      let seqVal = readUint64Le(payload, idx)
      let keyLen = int(readUint32Le(payload, idx))
      let keyBytes = readBytes(payload, idx, keyLen)
      let valueLen = int(readUint32Le(payload, idx))
      let valueBytes = readBytes(payload, idx, valueLen)
      result.add(mem_types.ApplyEntry(
        key: pebble_core.toKey(keyBytes),
        seq: pebble_core.toSequence(seqVal),
        kind: mem_types.MemValueKind(kindVal),
        value: valueBytes
      ))
  except ValueError:
    result = @[]

proc loadWalEntries(path: string): seq[seq[mem_types.ApplyEntry]] =
  if path.len == 0 or not path.fileExists():
    return @[]
  var reader: wal_reader.WalReader
  try:
    reader = wal_reader.newWalReader(path)
  except CatchableError:
    return @[]
  try:
    while true:
      let recordOpt = reader.readNext()
      if recordOpt.isNone():
        break
      let record = recordOpt.get()
      let entries = decodeWalRecord(record.payload)
      if entries.len > 0:
        result.add(entries)
  finally:
    reader.close()

proc sanitizeFragment(value: string): string =
  if value.len == 0:
    return ""
  var buf = newSeq[char]()
  for ch in value:
    if ch.isAlphaNumeric() or ch in {'_', '-', '.'}:
      buf.add(ch)
    else:
      buf.add('_')
  buf.join("")

proc sanitizeId(id: string): string =
  let normalized = sanitizeFragment(id)
  if normalized.len == 0:
    return "key"
  normalized

proc buildKey(space: KeySpace; id: string): string =
  KeyPrefixes[ord(space)] & sanitizeId(id)

proc toKey(space: KeySpace; id: string): pebble_core.Key =
  pebble_core.toKey(buildKey(space, id))

proc toByteString(data: openArray[byte]): string =
  ## 将任意字节序列拷贝为 Nim 字符串，保留二进制内容。
  result = newString(data.len)
  if data.len > 0:
    copyMem(addr result[0], unsafeAddr data[0], data.len)

proc toByteSeq(data: string): seq[byte] =
  ## 将 Nim 字符串无损转换为字节序列，便于二进制处理。
  result = newSeq[byte](data.len)
  if data.len > 0:
    copyMem(addr result[0], unsafeAddr data[0], data.len)

proc keySpaceName*(space: KeySpace): string =
  ## 返回 KeySpace 对应的人类可读名称，便于日志与序列化。
  KeySpaceNames[ord(space)]

proc parseKeySpace(name: string): Option[KeySpace] =
  ## 依据名称解析 KeySpace，大小写不敏感。
  for idx, candidate in KeySpaceNames:
    if name.cmpIgnoreCase(candidate) == 0:
      return some(KeySpace(idx))
  none(KeySpace)

proc kvWriteKindName(kind: KVWriteKind): string =
  WriteKindNames[ord(kind)]

proc parseKVWriteKind(name: string): Option[KVWriteKind] =
  for idx, candidate in WriteKindNames:
    if name.cmpIgnoreCase(candidate) == 0:
      return some(KVWriteKind(idx))
  none(KVWriteKind)

proc encodeValueBase64(value: string): string =
  if value.len == 0:
    return ""
  base64.encode(toByteSeq(value))

proc decodeValueBase64(encoded: string): Option[string] =
  if encoded.len == 0:
    return some("")
  try:
    let bytes = base64.decode(encoded)
    some(toByteString(toByteSeq(bytes)))
  except CatchableError:
    none(string)

proc encodeWrite*(write: KVWrite): JsonNode =
  ## 将 KVWrite 转换为 JSON，value 以 base64 表示，确保跨语言兼容。
  result = newJObject()
  result["kind"] = %kvWriteKindName(write.kind)
  result["space"] = %keySpaceName(write.space)
  result["key"] = %write.key
  if write.kind == kvPut or write.value.len > 0:
    result["value"] = %encodeValueBase64(write.value)

proc decodeWrite*(node: JsonNode): Option[KVWrite] =
  ## 从 JSON 恢复 KVWrite，失败时返回 none。
  if node.isNil() or node.kind != JObject:
    return none(KVWrite)
  if not node.hasKey("kind") or not node.hasKey("space") or not node.hasKey("key"):
    return none(KVWrite)
  if node["kind"].kind != JString or node["space"].kind != JString or node["key"].kind != JString:
    return none(KVWrite)
  let kindOpt = parseKVWriteKind(node["kind"].getStr())
  if kindOpt.isNone():
    return none(KVWrite)
  let spaceOpt = parseKeySpace(node["space"].getStr())
  if spaceOpt.isNone():
    return none(KVWrite)
  let key = node["key"].getStr()
  var value = ""
  if node.hasKey("value"):
    if node["value"].kind != JString:
      return none(KVWrite)
    let decoded = decodeValueBase64(node["value"].getStr())
    if decoded.isNone():
      return none(KVWrite)
    value = decoded.get()
  elif kindOpt.get() == kvPut:
    ## kvPut 需要 value 字段，否则日志不完整。
    return none(KVWrite)
  some(KVWrite(kind: kindOpt.get(), space: spaceOpt.get(), key: key, value: value))

proc encodeWritesToJson*(writes: openArray[KVWrite]): JsonNode =
  ## 批量编码 KVWrite 日志为 JSON 数组，便于网络传输或持久化。
  result = newJArray()
  for op in writes:
    result.add(encodeWrite(op))

proc decodeWritesFromJson*(payload: JsonNode): Option[seq[KVWrite]] =
  ## 批量解析 JSON 数组为 KVWrite 序列，若存在非法条目则返回 none。
  if payload.isNil() or payload.kind != JArray:
    return none(seq[KVWrite])
  var writes: seq[KVWrite] = @[]
  for item in payload.elems:
    let decoded = decodeWrite(item)
    if decoded.isNone():
      return none(seq[KVWrite])
    writes.add(decoded.get())
  some(writes)

proc encodeWritesToString*(writes: openArray[KVWrite]): string =
  ## 将 KVWrite 序列编码为紧凑 JSON 字符串，默认不换行。
  $encodeWritesToJson(writes)

proc decodeWritesFromString*(payload: string): Option[seq[KVWrite]] =
  ## 从 JSON 字符串解析 KVWrite 序列，容错所有解析错误。
  try:
    let node = parseJson(payload)
    decodeWritesFromJson(node)
  except CatchableError:
    none(seq[KVWrite])

proc defaultResourceConfig(): PebbleKVResourceConfig =
  PebbleKVResourceConfig(
    memSoftLimitBytes: 2 * 1024 * 1024,
    memHardLimitBytes: 8 * 1024 * 1024,
    throttleEnabled: true,
    throttleStallLimitBytes: 0,
    throttleReleaseTargetBytes: 0,
    throttleBaseDelayMs: 5,
    throttleMaxDelayMs: 50,
    resourceManager: nil,
    resourceKind: resource_manager.resMemoryBytes
  )

proc defaultWalConfig(): PebbleKVWalConfig =
  PebbleKVWalConfig(
    path: "",
    format: wal_types.wireRecyclable,
    logNumber: 0'u32,
    maxSegmentBytes: 0,
    autoSyncOffsets: true,
    autoSyncBytes: int64(256 * 1024),
    maxPendingAutoSyncs: 8,
    executor: nil
  )

proc defaultPebbleKVConfig*(path: string = "pebble-data"): PebbleKVConfig =
  result.dataPath = if path.len == 0: "pebble-data" else: path
  result.resource = defaultResourceConfig()
  result.wal = defaultWalConfig()
  result.compaction = PebbleKVCompactionConfig(
    scheduler: nil,
    flushTrigger: nil,
    flushExecutor: nil
  )
  result.maxConcurrentCommits = 4

proc normalizeResource(cfg: var PebbleKVResourceConfig): resource_manager.ResourceManager =
  if cfg.memHardLimitBytes <= 0:
    cfg.memSoftLimitBytes = 0
    cfg.resourceManager = nil
    return nil
  if cfg.memSoftLimitBytes <= 0 or cfg.memSoftLimitBytes > cfg.memHardLimitBytes:
    cfg.memSoftLimitBytes = cfg.memHardLimitBytes
  var rm = cfg.resourceManager
  if rm.isNil():
    rm = resource_manager.newResourceManager()
  rm.setQuota(cfg.resourceKind,
              cfg.memSoftLimitBytes.int64,
              cfg.memHardLimitBytes.int64)
  cfg.resourceManager = rm
  rm

proc buildThrottle(cfg: PebbleKVResourceConfig): mem_types.ThrottleConfig =
  var stall = cfg.throttleStallLimitBytes
  if stall <= 0:
    stall = cfg.memSoftLimitBytes
  var baseDelay = cfg.throttleBaseDelayMs
  if baseDelay <= 0:
    baseDelay = 1
  var maxDelay = cfg.throttleMaxDelayMs
  if maxDelay < baseDelay:
    maxDelay = baseDelay
  var release = cfg.throttleReleaseTargetBytes
  mem_types.ThrottleConfig(
    enabled: cfg.throttleEnabled,
    stallLimitBytes: stall,
    releaseTargetBytes: release,
    baseDelayMs: baseDelay,
    maxDelayMs: maxDelay
  )

proc ensureWalPath(cfg: var PebbleKVWalConfig; base: string) =
  if cfg.path.len == 0:
    cfg.path = base / "bridge.wal"

proc initMemConfig(cfg: PebbleKVConfig;
                   rm: resource_manager.ResourceManager): mem_types.MemTableConfig =
  var throttle = buildThrottle(cfg.resource)
  mem_types.MemTableConfig(
    comparator: nil,
    mergeOperator: nil,
    softLimitBytes: cfg.resource.memSoftLimitBytes,
    hardLimitBytes: cfg.resource.memHardLimitBytes,
    resourceManager: rm,
    resourceKind: cfg.resource.resourceKind,
    flushExecutor: cfg.compaction.flushExecutor,
    flushTrigger: cfg.compaction.flushTrigger,
    throttleConfig: throttle
  )

proc newPebbleKV*(cfg: PebbleKVConfig): PebbleKV =
  var normalized = cfg
  if normalized.dataPath.len == 0:
    normalized.dataPath = "pebble-data"
  createDir(normalized.dataPath)
  normalized.wal.ensureWalPath(normalized.dataPath)
  let walReplay = loadWalEntries(normalized.wal.path)
  let rm = normalizeResource(normalized.resource)
  var memCfg = initMemConfig(normalized, rm)
  let wal = wal_writer.newWalWriter(
    normalized.wal.path,
    normalized.wal.format,
    normalized.wal.logNumber,
    normalized.wal.maxSegmentBytes,
    nil,
    normalized.wal.executor,
    normalized.wal.autoSyncOffsets,
    normalized.wal.autoSyncBytes,
    normalized.wal.maxPendingAutoSyncs
  )
  var dbCfg = batch_types.DBConfig(
    memConfig: memCfg,
    maxConcurrentCommits: normalized.maxConcurrentCommits,
    walWriter: wal,
  )
  let db = pebble_db.initDB(dbCfg)
  if walReplay.len > 0:
    for entries in walReplay.items():
      if entries.len == 0:
        continue
      var replayBatch = pebble_batch.newBatch(memCfg)
      for entry in entries.items():
        case entry.kind
        of mem_types.memValueSet:
          replayBatch.put(entry.key, entry.value)
        of mem_types.memValueDelete:
          replayBatch.delete(entry.key)
        of mem_types.memValueMerge:
          if mem_types.hasMerge(memCfg):
            replayBatch.merge(entry.key, entry.value)
      if not replayBatch.isEmpty():
        try:
          discard db.commit(replayBatch)
        except CatchableError:
          discard
  PebbleKV(
    db: db,
    memCfg: memCfg,
    wal: wal,
    basePath: normalized.dataPath,
    resourceManager: rm,
    compactionScheduler: normalized.compaction.scheduler,
    cfg: normalized
  )

proc newPebbleKV*(path: string = "pebble-data"): PebbleKV =
  newPebbleKV(defaultPebbleKVConfig(path))

proc close*(kv: PebbleKV) =
  if kv.isNil():
    return
  kv.db.close()
  if not kv.wal.isNil():
    kv.wal.close()

proc put*(kv: PebbleKV; space: KeySpace; id: string; value: string;
          opts: batch_types.WriteOptions = batch_types.defaultWriteOptions) =
  var batch = pebble_batch.newBatch(kv.memCfg)
  batch.put(toKey(space, id), value)
  discard kv.db.commit(batch, opts)

proc putBytes*(kv: PebbleKV; space: KeySpace; id: string;
               value: openArray[byte];
               opts: batch_types.WriteOptions = batch_types.defaultWriteOptions) =
  ## 写入二进制 payload 的便捷封装。
  kv.put(space, id, toByteString(value), opts)

proc delete*(kv: PebbleKV; space: KeySpace; id: string;
             opts: batch_types.WriteOptions = batch_types.defaultWriteOptions) =
  var batch = pebble_batch.newBatch(kv.memCfg)
  batch.delete(toKey(space, id))
  discard kv.db.commit(batch, opts)

proc get*(kv: PebbleKV; space: KeySpace; id: string): Option[string] =
  kv.db.get(toKey(space, id))

proc get*(kv: PebbleKV; space: KeySpace; id: string;
          snapshot: batch_types.Snapshot): Option[string] =
  if pebble_core.toUint64(snapshot.seq) == 0:
    kv.db.get(toKey(space, id))
  else:
    kv.db.get(toKey(space, id), snapshot)

proc getBytes*(kv: PebbleKV; space: KeySpace; id: string;
               snapshot: batch_types.Snapshot = zeroSnapshot): Option[seq[byte]] =
  let raw = kv.get(space, id, snapshot)
  if raw.isNone():
    return none(seq[byte])
  some(toByteSeq(raw.get()))

proc applyBatch*(kv: PebbleKV; ops: openArray[KVWrite];
                 opts: batch_types.WriteOptions = batch_types.defaultWriteOptions) =
  var batch = pebble_batch.newBatch(kv.memCfg)
  for op in ops:
    case op.kind
    of kvPut:
      batch.put(toKey(op.space, op.key), op.value)
    of kvDelete:
      batch.delete(toKey(op.space, op.key))
  discard kv.db.commit(batch, opts)

proc ensureBatch(batch: PebbleWriteBatch) =
  if batch.isNil() or batch.kv.isNil():
    raise newException(ValueError, "PebbleWriteBatch 未初始化")

proc ensureOpen(batch: PebbleWriteBatch) =
  batch.ensureBatch()
  if batch.state != kvBatchOpen:
    case batch.state
    of kvBatchCommitted:
      raise newException(ValueError, "PebbleWriteBatch 已提交")
    of kvBatchAborted:
      raise newException(ValueError, "PebbleWriteBatch 已回滚")
    else:
      raise newException(ValueError, "PebbleWriteBatch 状态异常")

proc newWriteBatch*(kv: PebbleKV;
                    indexed = false;
                    snapshot: batch_types.Snapshot = zeroSnapshot): PebbleWriteBatch =
  if kv.isNil():
    raise newException(ValueError, "PebbleKV 未初始化")
  new(result)
  result.kv = kv
  result.inner = kv.db.newBatch(indexed, snapshot)
  result.log = @[]
  result.state = kvBatchOpen
  result.lastSeq = pebble_core.toSequence(0)

proc appendLog(batch: PebbleWriteBatch; op: KVWrite) =
  if batch.log.len == 0:
    batch.log = @[op]
  else:
    batch.log.add(op)

proc put*(batch: PebbleWriteBatch; space: KeySpace; id: string; value: string) =
  batch.ensureOpen()
  batch.inner.put(toKey(space, id), value)
  batch.appendLog(withPut(space, id, value))

proc put*(batch: PebbleWriteBatch; space: KeySpace; id: string;
          value: openArray[byte]) =
  ## 二进制写入快捷方式，自动归档入回放日志。
  batch.put(space, id, toByteString(value))

proc delete*(batch: PebbleWriteBatch; space: KeySpace; id: string) =
  batch.ensureOpen()
  batch.inner.delete(toKey(space, id))
  batch.appendLog(withDelete(space, id))

proc replay*(batch: PebbleWriteBatch; writes: openArray[KVWrite]) =
  ## 依据历史写入日志重放批处理，便于 WAL 回放或失败重试。
  batch.ensureOpen()
  batch.inner.reset()
  if batch.log.len > 0:
    batch.log.setLen(0)
  for op in writes:
    case op.kind
    of kvPut:
      batch.inner.put(toKey(op.space, op.key), op.value)
    of kvDelete:
      batch.inner.delete(toKey(op.space, op.key))
    batch.appendLog(op)

proc len*(batch: PebbleWriteBatch): int =
  if batch.isNil():
    return 0
  batch.log.len

proc isEmpty*(batch: PebbleWriteBatch): bool =
  batch.len == 0

proc writes*(batch: PebbleWriteBatch): seq[KVWrite] =
  if batch.isNil():
    return @[]
  batch.log

proc commit*(batch: PebbleWriteBatch;
             opts: batch_types.WriteOptions = batch_types.defaultWriteOptions): pebble_core.SequenceNumber =
  batch.ensureOpen()
  let seq = batch.kv.db.commit(batch.inner, opts)
  batch.state = kvBatchCommitted
  batch.lastSeq = seq
  result = seq

proc committed*(batch: PebbleWriteBatch): bool =
  if batch.isNil():
    return false
  batch.state == kvBatchCommitted

proc rollback*(batch: PebbleWriteBatch) =
  if batch.isNil():
    return
  if batch.state != kvBatchOpen:
    return
  batch.inner.reset()
  batch.state = kvBatchAborted
  batch.lastSeq = pebble_core.toSequence(0)

proc aborted*(batch: PebbleWriteBatch): bool =
  if batch.isNil():
    return false
  batch.state == kvBatchAborted

proc committedSequence*(batch: PebbleWriteBatch): pebble_core.SequenceNumber =
  if batch.isNil():
    return pebble_core.toSequence(0)
  batch.lastSeq

proc withWriteBatch*(kv: PebbleKV;
                     body: proc (batch: PebbleWriteBatch) {.closure.};
                     opts: batch_types.WriteOptions = batch_types.defaultWriteOptions): pebble_core.SequenceNumber =
  if kv.isNil():
    raise newException(ValueError, "PebbleKV 未初始化")
  let batch = kv.newWriteBatch()
  try:
    body(batch)
    case batch.state
    of kvBatchCommitted:
      return batch.lastSeq
    of kvBatchAborted:
      return pebble_core.toSequence(0)
    of kvBatchOpen:
      return batch.commit(opts)
  except:
    batch.rollback()
    raise

proc replayWrites*(kv: PebbleKV;
                   writes: openArray[KVWrite];
                   opts: batch_types.WriteOptions = batch_types.defaultWriteOptions;
                   dedupe = true): pebble_core.SequenceNumber =
  ## 使用 `KVWrite` 日志重放提交，`dedupe=true` 时若目标状态已满足则跳过写入。
  if kv.isNil():
    raise newException(ValueError, "PebbleKV 未初始化")
  if writes.len == 0:
    return pebble_core.toSequence(0)
  if dedupe:
    var targetStates = initTable[(KeySpace, string), Option[string]]()
    for op in writes:
      let key = (op.space, sanitizeId(op.key))
      case op.kind
      of kvPut:
        targetStates[key] = some(op.value)
      of kvDelete:
        targetStates[key] = none(string)
    var allMatch = true
    for key, desired in targetStates.pairs():
      let current = kv.get(key[0], key[1])
      if desired.isSome():
        if current.isNone() or current.get() != desired.get():
          allMatch = false
          break
      else:
        if current.isSome():
          allMatch = false
          break
    if allMatch:
      return pebble_core.toSequence(0)
  let batch = kv.newWriteBatch()
  for op in writes:
    case op.kind
    of kvPut:
      batch.put(op.space, op.key, op.value)
    of kvDelete:
      batch.delete(op.space, op.key)
  if batch.isEmpty():
    return pebble_core.toSequence(0)
  batch.commit(opts)

proc stats*(kv: PebbleKV): PebbleKVStats =
  if kv.isNil():
    return PebbleKVStats()
  var memSnapshot = mem_types.MemTableStats()
  try:
    memSnapshot = kv.db.memtableStats()
  except CatchableError:
    memSnapshot = mem_types.MemTableStats()
  result.mem.approxBytes = memSnapshot.approxBytes
  result.mem.softLimitBytes = memSnapshot.softLimitBytes
  result.mem.hardLimitBytes = memSnapshot.hardLimitBytes
  result.mem.frozen = memSnapshot.frozen
  result.mem.throttleActive = memSnapshot.throttleActive
  result.mem.lastThrottleDelayMs = memSnapshot.lastThrottleDelayMs
  if memSnapshot.hardLimitBytes > 0:
    var ratio = memSnapshot.approxBytes.float / memSnapshot.hardLimitBytes.float
    if ratio > 1.0:
      ratio = 1.0
    result.mem.utilization = ratio
  let walSnapshot =
    if kv.wal.isNil():
      wal_writer.WalWriterStats()
    else:
      kv.wal.stats()
  result.wal.segmentIndex = walSnapshot.segmentIndex
  result.wal.segmentPath = walSnapshot.segmentPath
  result.wal.segmentBytes = walSnapshot.segmentBytes
  result.wal.totalBytes = walSnapshot.totalBytes
  result.wal.pendingAutoSyncs = walSnapshot.pendingAutoSyncs
  result.wal.bytesSinceSync = walSnapshot.bytesSinceSync
  if not kv.resourceManager.isNil():
    result.resourceUtilization = kv.resourceManager.utilization(kv.memCfg.resourceKind)
  else:
    result.resourceUtilization = 0.0
  result.compaction.metrics = compaction_types.CompactionMetrics()
  if not kv.compactionScheduler.isNil():
    result.compaction.hasScheduler = true
    result.compaction.activeJobs = kv.compactionScheduler.activeCount()
    result.compaction.pendingJobs = kv.compactionScheduler.pendingCount()
    result.compaction.metrics = kv.compactionScheduler.metricsSnapshot()

proc setCompactionScheduler*(kv: PebbleKV;
                             scheduler: compaction_scheduler.CompactionScheduler) =
  if kv.isNil():
    return
  kv.compactionScheduler = scheduler
  kv.cfg.compaction.scheduler = scheduler

proc resourceManager*(kv: PebbleKV): resource_manager.ResourceManager =
  if kv.isNil():
    return nil
  kv.resourceManager

proc config*(kv: PebbleKV): PebbleKVConfig =
  if kv.isNil():
    return defaultPebbleKVConfig()
  kv.cfg

proc withPut*(space: KeySpace; id: string; value: string): KVWrite =
  KVWrite(kind: kvPut, space: space, key: id, value: value)

proc withPutBytes*(space: KeySpace; id: string;
                   value: openArray[byte]): KVWrite =
  withPut(space, id, toByteString(value))

proc withDelete*(space: KeySpace; id: string): KVWrite =
  KVWrite(kind: kvDelete, space: space, key: id, value: "")

proc spacePrefix(space: KeySpace): string =
  KeyPrefixes[ord(space)]

proc stripSpacePrefix(space: KeySpace; rawKey: string): Option[string] =
  let prefix = spacePrefix(space)
  if rawKey.startsWith(prefix):
    let sliced = rawKey[prefix.len ..< rawKey.len]
    some(sliced)
  else:
    none(string)

proc ensureSnapshot(kv: PebbleKV; snapshot: batch_types.Snapshot): batch_types.Snapshot =
  if pebble_core.toUint64(snapshot.seq) == 0:
    kv.db.latestSnapshot()
  else:
    snapshot

proc scanPrefix*(kv: PebbleKV;
                 space: KeySpace;
                 prefix = "";
                 startAfter = "";
                 limit = 0;
                 snapshot: batch_types.Snapshot = zeroSnapshot): KVScanPage =
  ## 基于 KeySpace+前缀的分页扫描，支持快照一致性与游标。
  let effectiveSnapshot = kv.ensureSnapshot(snapshot)
  let comparator = kv.db.comparator()
  let sanitizedPrefix = sanitizeFragment(prefix)
  let sanitizedCursor = sanitizeFragment(startAfter)
  let keyPrefix = spacePrefix(space)
  let fullPrefix = keyPrefix & sanitizedPrefix
  var cursorKey: Option[pebble_core.Key]
  if startAfter.len > 0:
    cursorKey = some(pebble_core.toKey(keyPrefix & sanitizedCursor))
  let materialized = kv.db.materializeSnapshot(effectiveSnapshot)
  let fetchLimit = if limit > 0: limit + 1 else: 0
  var collected: seq[KVPair] = @[]
  for entry in materialized:
    let rawKey = entry.key.toBytes()
    if not rawKey.startsWith(keyPrefix):
      continue
    if sanitizedPrefix.len > 0 and not rawKey.startsWith(fullPrefix):
      continue
    if cursorKey.isSome() and comparator(entry.key, cursorKey.get()) <= 0:
      continue
    let logical = rawKey[keyPrefix.len ..< rawKey.len]
    collected.add(KVPair(id: logical, value: entry.value))
    if fetchLimit > 0 and collected.len >= fetchLimit:
      break
  var hasMore = false
  if limit > 0 and collected.len > limit:
    hasMore = true
    collected.setLen(limit)
  result.entries = collected
  result.hasMore = hasMore
  if hasMore and collected.len > 0:
    result.nextCursor = some(collected[^1].id)
  else:
    result.nextCursor = none(string)
  result.snapshot = effectiveSnapshot

proc seek*(kv: PebbleKV;
           space: KeySpace;
           id: string;
           snapshot: batch_types.Snapshot = zeroSnapshot): Option[KVPair] =
  ## 定位到首个大于等于指定 ID 的键值对。
  let effectiveSnapshot = kv.ensureSnapshot(snapshot)
  let sanitizedId = sanitizeFragment(id)
  let prefix = spacePrefix(space)
  let targetKey = pebble_core.toKey(prefix & sanitizedId)
  let comparator = kv.db.comparator()
  for entry in kv.db.materializeSnapshot(effectiveSnapshot):
    if comparator(entry.key, targetKey) < 0:
      continue
    let rawKey = entry.key.toBytes()
    if not rawKey.startsWith(prefix):
      continue
    let logical = stripSpacePrefix(space, rawKey)
    if logical.isSome():
      return some(KVPair(id: logical.get(), value: entry.value))
  none(KVPair)

proc newSnapshotIterator*(kv: PebbleKV;
                          space: KeySpace;
                          prefix = "";
                          startAfter = "";
                          snapshot: batch_types.Snapshot = zeroSnapshot): KVSnapIterator =
  ## 构建快照迭代器，遍历顺序固定且不受后续写入影响。
  let page = kv.scanPrefix(space, prefix, startAfter, 0, snapshot)
  new(result)
  result.space = space
  result.prefix = sanitizeFragment(prefix)
  result.snapshot = page.snapshot
  result.entries = page.entries
  result.idx = -1

proc len*(it: KVSnapIterator): int =
  if it.isNil(): 0 else: it.entries.len

proc reset*(it: KVSnapIterator) =
  if it.isNil():
    return
  it.idx = -1

proc valid*(it: KVSnapIterator): bool =
  (not it.isNil()) and it.idx >= 0 and it.idx < it.entries.len

proc current*(it: KVSnapIterator): KVPair =
  if not it.valid():
    raise newException(IndexError, "iterator is not positioned on a valid entry")
  it.entries[it.idx]

proc first*(it: KVSnapIterator): bool =
  if it.isNil():
    return false
  if it.entries.len == 0:
    it.idx = -1
    return false
  it.idx = 0
  true

proc next*(it: KVSnapIterator): bool =
  if it.isNil():
    return false
  if it.entries.len == 0:
    it.idx = -1
    return false
  if it.idx < 0:
    it.idx = 0
    return true
  if it.idx + 1 >= it.entries.len:
    it.idx = it.entries.len
    return false
  inc it.idx
  true

proc prev*(it: KVSnapIterator): bool =
  if it.isNil():
    return false
  if it.entries.len == 0:
    it.idx = -1
    return false
  if it.idx <= 0:
    it.idx = -1
    return false
  dec it.idx
  true

proc seek*(it: KVSnapIterator; id: string; inclusive = true): bool =
  if it.isNil():
    return false
  if it.entries.len == 0:
    it.idx = -1
    return false
  let target = sanitizeFragment(id)
  var low = 0
  var high = it.entries.len
  while low < high:
    let mid = (low + high) div 2
    let cmpRes = cmp(it.entries[mid].id, target)
    if cmpRes < 0 or (cmpRes == 0 and not inclusive):
      low = mid + 1
    else:
      high = mid
  if low >= it.entries.len:
    it.idx = it.entries.len
    return false
  it.idx = low
  true

proc key*(it: KVSnapIterator): string =
  it.current().id

proc value*(it: KVSnapIterator): string =
  it.current().value

proc valueBytes*(it: KVSnapIterator): seq[byte] =
  toByteSeq(it.value())

proc valueBytes*(pair: KVPair): seq[byte] =
  toByteSeq(pair.value)

iterator snapshotIterator*(kv: PebbleKV;
                           space: KeySpace;
                           prefix = "";
                           startAfter = "";
                           limit = 0;
                           snapshot: batch_types.Snapshot = zeroSnapshot): KVPair =
  ## 基于扫描 API 的简易迭代器，便于 for-in 场景使用。
  let page = kv.scanPrefix(space, prefix, startAfter, limit, snapshot)
  for entry in page.entries:
    yield entry
