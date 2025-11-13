# DB、Snapshot 与批处理提交流水线实现。

import std/[locks, options, os, times, sets]

import pebble/core/types
import pebble/mem

import pebble/batch/types as batch_types
import pebble/batch/batch as batch_module
import pebble/runtime/executor as exec
import pebble/wal/writer as wal_writer

type
  BatchBase = batch_module.BatchBase
  Batch = batch_module.Batch
  IndexedBatch = batch_module.IndexedBatch
  BatchError = batch_types.BatchError
  BackpressureInfo = batch_types.BackpressureInfo
  BackpressureReason = batch_types.BackpressureReason
  BackpressureHandler = batch_types.BackpressureHandler
  WriteOptions = batch_types.WriteOptions
  Snapshot = batch_types.Snapshot
  DBConfig = batch_types.DBConfig
  WalWriter = wal_writer.WalWriter
  WalAppendOptions = wal_writer.WalAppendOptions
  WalAppendResult = wal_writer.WalAppendResult
  SnapshotEntry* = tuple[key: Key, value: string]

type
  DB* = ref object of RootObj
    cfg: DBConfig
    mem: MemTable
    lock: Lock
    cond: Cond
    nextSeq: uint64
    inflight: int
    closed: bool

proc initDB*(cfg: DBConfig): DB =
  var normalized = cfg
  normalized.ensureMemConfig()
  if normalized.memConfig.comparator.isNil():
    normalized.memConfig.comparator = compareBytewise
  let mem = initMemTable(normalized.memConfig)
  new(result)
  result.cfg = normalized
  result.mem = mem
  initLock(result.lock)
  initCond(result.cond)
  result.nextSeq = 0
  result.inflight = 0
  result.closed = false

proc ensureOpen(db: DB) =
  if db.isNil() or db.closed:
    raise newException(BatchError, "database is closed")

proc latestSnapshot*(db: DB): Snapshot =
  db.ensureOpen()
  acquire(db.lock)
  let seq = toSequence(db.nextSeq)
  release(db.lock)
  Snapshot(seq: seq)

proc get*(db: DB; key: Key; snapshot: Snapshot): Option[string] =
  db.ensureOpen()
  acquire(db.lock)
  let seq = if toUint64(snapshot.seq) == 0: toSequence(db.nextSeq) else: snapshot.seq
  let res = db.mem.get(key, seq)
  release(db.lock)
  res

proc get*(db: DB; key: Key): Option[string] =
  db.get(key, db.latestSnapshot())

proc approxMemtableBytes*(db: DB): int =
  db.ensureOpen()
  acquire(db.lock)
  let bytes = db.mem.approxBytes
  release(db.lock)
  bytes

proc memtableStats*(db: DB): MemTableStats =
  db.ensureOpen()
  acquire(db.lock)
  let snapshot = db.mem.stats()
  release(db.lock)
  snapshot

proc sendNudge(handler: BackpressureHandler;
               reason: BackpressureReason;
               batch: BatchBase;
               memBytes: int) =
  if handler.isNil():
    return
  handler(BackpressureInfo(
    reason: reason,
    queuedOps: batch.len,
    queuedBytes: batch.estimatedBytes(),
    memApproxBytes: memBytes
  ))

proc isZero(opts: WriteOptions): bool =
  (not opts.sync) and (not opts.disableWAL) and opts.nudge.isNil() and
  opts.timeoutMs == 0 and opts.priority == memPriorityDefault

proc resolveOptions(db: DB; opts: WriteOptions): WriteOptions =
  result = opts
  if result.isZero():
    result = db.cfg.defaultOptions
  if result.nudge.isNil():
    result.nudge = db.cfg.backpressure
  if result.priority == memPriorityDefault:
    result.priority = db.cfg.defaultOptions.priority

proc newBatch*(db: DB;
               indexed = false;
               snapshot: Snapshot = Snapshot(seq: toSequence(0))): BatchBase =
  db.ensureOpen()
  if indexed:
    let snap = if toUint64(snapshot.seq) == 0: db.latestSnapshot() else: snapshot
    let resolver = proc (key: Key; snap: Snapshot): Option[string] {.gcsafe.} =
      db.get(key, snap)
    result = batch_module.newIndexedBatch(db.cfg.memConfig, resolver, snap)
  else:
    result = batch_module.newBatch(db.cfg.memConfig)

proc close*(db: DB) =
  if db.isNil():
    return
  acquire(db.lock)
  db.closed = true
  broadcast(db.cond)
  release(db.lock)

proc commit*(db: DB; batch: BatchBase;
             opts: WriteOptions = defaultWriteOptions): SequenceNumber =
  db.ensureOpen()
  if batch.isNil():
    raise newException(BatchError, "batch must not be nil")
  var options = db.resolveOptions(opts)
  if batch.isCommitted():
    raise newException(BatchError, "batch has already been committed")
  let opCount = batch.len
  db.ensureOpen()
  acquire(db.lock)
  if db.cfg.maxConcurrentCommits > 0:
    let hasTimeout = options.timeoutMs > 0
    var deadline = 0.0
    if hasTimeout:
      deadline = epochTime() + (options.timeoutMs.float / 1000.0)
    while db.inflight >= db.cfg.maxConcurrentCommits:
      let currentBytes = db.mem.approxBytes
      sendNudge(options.nudge, bpMemStall, batch, currentBytes)
      if hasTimeout:
        let remaining = deadline - epochTime()
        if remaining <= 0:
          release(db.lock)
          raise newException(BatchError, "commit wait timed out")
        release(db.lock)
        let sleepMs = max(1, min(options.timeoutMs, int(remaining * 1000.0)))
        sleep(sleepMs)
        acquire(db.lock)
        db.ensureOpen()
      else:
        wait(db.cond, db.lock)
        db.ensureOpen()
  if db.mem.needsFlush() or db.mem.readyForFlush():
    sendNudge(options.nudge, bpMemSoftLimit, batch, db.mem.approxBytes)
  if opCount > 0:
    let estimatedBytes = batch.estimatedBytes()
    var throttleDelay = db.mem.admitWrite(options.priority, estimatedBytes)
    while throttleDelay > 0:
      let currentBytes = db.mem.approxBytes
      sendNudge(options.nudge, bpMemStall, batch, currentBytes)
      release(db.lock)
      sleep(throttleDelay)
      acquire(db.lock)
      db.ensureOpen()
      throttleDelay = db.mem.admitWrite(options.priority, estimatedBytes)
  if opCount == 0:
    release(db.lock)
    batch.markCommitted()
    return toSequence(db.nextSeq)
  db.ensureOpen()
  let prevSeq = db.nextSeq
  let startSeqU = prevSeq + 1
  db.nextSeq = prevSeq + uint64(opCount)
  inc db.inflight
  let startSeq = toSequence(startSeqU)
  let entries = batch_module.toApplyEntries(batch, startSeq)
  let walWriter = db.cfg.walWriter
  let walEnabled = not walWriter.isNil() and (not options.disableWAL) and entries.len > 0
  var walPayload: seq[byte] = @[]
  if walEnabled:
    walPayload = batch_module.toWalPayload(batch, entries)
  var committed = false
  try:
    if walEnabled:
      var walError: ref CatchableError = nil
      var walResult: WalAppendResult
      release(db.lock)
      try:
        walResult = walWriter.append(
          walPayload,
          WalAppendOptions(requireSync: options.sync)
        )
      except CatchableError as err:
        walError = err
      acquire(db.lock)
      db.ensureOpen()
      if not walError.isNil():
        raise newException(BatchError, "wal append failed: " & walError.msg)
      if options.sync and not walResult.syncHandle.isNil():
        release(db.lock)
        exec.wait(walResult.syncHandle)
        acquire(db.lock)
        db.ensureOpen()
    db.mem.applyBatch(entries)
    if db.mem.needsFlush() or db.mem.readyForFlush():
      sendNudge(options.nudge, bpMemSoftLimit, batch, db.mem.approxBytes)
    committed = true
    let lastSeqU = startSeqU + uint64(entries.len) - 1
    let lastSeq = toSequence(lastSeqU)
    batch.markCommitted()
    result = lastSeq
  finally:
    dec db.inflight
    signal(db.cond)
    if not committed:
      db.nextSeq = prevSeq
    release(db.lock)

proc apply*(db: DB; batch: BatchBase;
            opts: WriteOptions = defaultWriteOptions): SequenceNumber =
  ## Apply 的语义与 commit 等价，提供与 Go API 一致的命名。
  db.commit(batch, opts)

proc comparator*(db: DB): Comparator =
  ## 暴露底层 MemTable 的比较器，供上层扫描逻辑复用排序语义。
  db.ensureOpen()
  db.cfg.memConfig.comparator

proc materializeSnapshot*(db: DB; snapshot: Snapshot): seq[SnapshotEntry] =
  ## 在给定快照下导出去重后的有序键值视图，适配读取场景。
  db.ensureOpen()
  acquire(db.lock)
  try:
    let effectiveSeq =
      if toUint64(snapshot.seq) == 0:
        db.nextSeq
      else:
        toUint64(snapshot.seq)
    var seen = initHashSet[string]()
    for record in db.mem.flushIterator():
      if toUint64(record.seq) > effectiveSeq:
        continue
      let keyBytes = record.key.toBytes()
      if seen.contains(keyBytes):
        continue
      seen.incl(keyBytes)
      if record.kind == memValueDelete:
        continue
      result.add((record.key, record.value))
  finally:
    release(db.lock)
