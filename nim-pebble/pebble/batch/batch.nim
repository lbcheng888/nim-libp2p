# 批处理与 IndexedBatch 实现，支持写入收集、Apply 以及批内读。

import std/[options]

import pebble/core/types
import pebble/mem

import pebble/batch/types as batch_types

const
  entryOverhead = 16

type
  BatchOp = batch_types.BatchOp
  BatchError = batch_types.BatchError
  Snapshot = batch_types.Snapshot

type
  BatchResolver* = proc (key: Key; snapshot: Snapshot): Option[string]
                     {.gcsafe, raises: [CatchableError].}

  BatchBase* = ref object of RootObj
    ops: seq[BatchOp]
    approxBytes: int
    sealed: bool

  Batch* = ref object of BatchBase

  IndexedBatch* = ref object of BatchBase
    index*: MemTable
    resolver*: BatchResolver
    snapshot*: Snapshot
    nextSeq*: uint64

iterator entries*(batch: BatchBase): BatchOp =
  if not batch.isNil():
    for op in batch.ops.items():
      yield op

proc len*(batch: BatchBase): int =
  if batch.isNil():
    return 0
  batch.ops.len

proc isEmpty*(batch: BatchBase): bool =
  batch.len == 0

proc estimatedBytes*(batch: BatchBase): int =
  if batch.isNil():
    return 0
  batch.approxBytes

proc isCommitted*(batch: BatchBase): bool =
  if batch.isNil():
    return false
  batch.sealed

proc markCommitted*(batch: BatchBase) =
  if batch.isNil():
    return
  batch.sealed = true

proc reset*(batch: BatchBase) =
  if batch.isNil():
    return
  batch.ops.setLen(0)
  batch.approxBytes = 0
  batch.sealed = false
  if batch of IndexedBatch:
    let idx = cast[IndexedBatch](batch)
    if not idx.index.isNil():
      idx.index.clear()
    idx.nextSeq = 0

proc ensureMutable(batch: BatchBase) =
  if batch.isNil():
    raise newException(BatchError, "batch is nil")
  if batch.sealed:
    raise newException(BatchError,
      "batch has been committed/applied; call reset before reusing")

proc cloneForIndex(cfg: MemTableConfig): MemTableConfig =
  result = cfg
  result.softLimitBytes = 0
  result.hardLimitBytes = 0
  result.resourceManager = nil

proc newBatch*(cfg: MemTableConfig): Batch =
  new(result)
  result.ops = @[]
  result.approxBytes = 0
  result.sealed = false

proc newIndexedBatch*(cfg: MemTableConfig;
                      resolver: BatchResolver;
                      snapshot: Snapshot): IndexedBatch =
  let indexCfg = cloneForIndex(cfg)
  new(result)
  result.ops = @[]
  result.approxBytes = 0
  result.sealed = false
  result.resolver = resolver
  result.snapshot = snapshot
  result.index = initMemTable(indexCfg)
  result.nextSeq = 0

proc calcEntryBytes(op: BatchOp): int =
  result = op.key.toBytes().len + op.value.len + entryOverhead

proc appendIndex(batch: IndexedBatch; op: BatchOp) =
  if batch.isNil() or batch.index.isNil():
    return
  inc batch.nextSeq
  let entry = ApplyEntry(
    key: op.key,
    seq: toSequence(batch.nextSeq),
    kind: op.kind,
    value: op.value
  )
  batch.index.applyBatch([entry])

proc appendOp(batch: BatchBase; op: BatchOp) =
  batch.ensureMutable()
  batch.ops.add(op)
  batch.approxBytes += calcEntryBytes(op)
  if batch of IndexedBatch:
    appendIndex(cast[IndexedBatch](batch), op)

proc writeUint32(buf: var seq[byte]; value: uint32) =
  buf.add(byte(value and 0xFF'u32))
  buf.add(byte((value shr 8) and 0xFF'u32))
  buf.add(byte((value shr 16) and 0xFF'u32))
  buf.add(byte((value shr 24) and 0xFF'u32))

proc writeUint64(buf: var seq[byte]; value: uint64) =
  buf.add(byte(value and 0xFF'u64))
  buf.add(byte((value shr 8) and 0xFF'u64))
  buf.add(byte((value shr 16) and 0xFF'u64))
  buf.add(byte((value shr 24) and 0xFF'u64))
  buf.add(byte((value shr 32) and 0xFF'u64))
  buf.add(byte((value shr 40) and 0xFF'u64))
  buf.add(byte((value shr 48) and 0xFF'u64))
  buf.add(byte((value shr 56) and 0xFF'u64))

proc writeString(buf: var seq[byte]; str: string) =
  for ch in str.items():
    buf.add(byte(ord(ch)))

proc toWalPayload*(batch: BatchBase;
                   entries: openArray[ApplyEntry]): seq[byte] =
  ## 将批处理转换为 WAL 记录格式，包含序列号、键值长度与内容。
  if entries.len == 0:
    return @[]
  result = @[]
  result.writeUint32(uint32(entries.len))
  for entry in entries.items():
    result.add(byte(entry.kind.ord))
    result.writeUint64(toUint64(entry.seq))
    let keyBytes = entry.key.toBytes()
    result.writeUint32(uint32(keyBytes.len))
    result.writeString(keyBytes)
    result.writeUint32(uint32(entry.value.len))
    result.writeString(entry.value)

proc put*(batch: BatchBase; key: Key; value: string) =
  let op = BatchOp(key: key, value: value, kind: memValueSet)
  batch.appendOp(op)

proc delete*(batch: BatchBase; key: Key) =
  let op = BatchOp(key: key, value: "", kind: memValueDelete)
  batch.appendOp(op)

proc merge*(batch: BatchBase; key: Key; operand: string) =
  let op = BatchOp(key: key, value: operand, kind: memValueMerge)
  batch.appendOp(op)

proc apply*(dest: BatchBase; src: BatchBase) =
  if src.isNil():
    return
  for op in src.entries():
    dest.appendOp(op)

proc toApplyEntries*(batch: BatchBase;
                     startSeq: SequenceNumber): seq[ApplyEntry] =
  if batch.isNil() or batch.len == 0:
    return @[]
  result = newSeq[ApplyEntry](batch.len)
  var seqVal = toUint64(startSeq)
  for idx in 0 ..< batch.ops.len:
    let op = batch.ops[idx]
    result[idx] = ApplyEntry(
      key: op.key,
      seq: toSequence(seqVal),
      kind: op.kind,
      value: op.value
    )
    inc seqVal

proc stats*(batch: BatchBase): batch_types.BatchStats =
  batch_types.BatchStats(ops: batch.len, bytes: batch.estimatedBytes())

proc snapshot*(batch: IndexedBatch): Snapshot =
  if batch.isNil():
    return Snapshot(seq: toSequence(0))
  batch.snapshot

proc get*(batch: IndexedBatch; key: Key; snapshot: Snapshot): Option[string] =
  if batch.isNil():
    return none(string)
  let local = batch.index.get(key)
  if local.isSome():
    return local
  if not batch.resolver.isNil():
    return batch.resolver(key, snapshot)
  none(string)

proc get*(batch: IndexedBatch; key: Key): Option[string] =
  if batch.isNil():
    return none(string)
  batch.get(key, batch.snapshot)
