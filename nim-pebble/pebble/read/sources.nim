# 将 Batch、MemTable 与 SSTable 映射为可合并的读源。

import std/[algorithm, options]

import pebble/core/types
import pebble/mem
import pebble/read/types
import pebble/sstable
import pebble/batch/batch as batch_module

const
  defaultBatchPriority = -10
  defaultMemPriority = 0
  defaultSSTPriority = 10

proc collectMemEntries(mt: MemTable;
                       snapshot: SequenceNumber): seq[ReadSourceEntry] =
  if mt.isNil():
    return @[]

  var entries: seq[ReadSourceEntry] = @[]
  let snapshotValue = toUint64(snapshot)
  var currentKey: Key
  var currentKeyBytes = ""
  var currentBase: Option[string] = none(string)
  var currentOperands: seq[string] = @[]
  var hasCurrent = false
  var hasVisible = false
  var doneForKey = false

  proc finalizeCurrent() =
    if not hasCurrent or not hasVisible:
      return
    var finalValue: Option[string]
    if currentOperands.len > 0:
      if not mt.cfg.hasMerge():
        raise newException(ReadError,
          "memtable merge operand encountered without configured merge operator")
      var ordered = currentOperands
      ordered.reverse()
      finalValue = mt.cfg.mergeOperator(currentKey, currentBase, ordered)
    else:
      finalValue = currentBase
    entries.add(ReadSourceEntry(key: currentKey, value: finalValue))

  for rec in mt.flushIterator():
    let keyBytes = rec.key.toBytes()
    if (not hasCurrent) or keyBytes != currentKeyBytes:
      finalizeCurrent()
      currentKey = rec.key
      currentKeyBytes = keyBytes
      currentBase = none(string)
      currentOperands.setLen(0)
      hasCurrent = true
      hasVisible = false
      doneForKey = false

    if doneForKey:
      continue
    if toUint64(rec.seq) > snapshotValue:
      continue
    hasVisible = true
    case rec.kind
    of memValueSet:
      currentBase = some(rec.value)
      doneForKey = true
    of memValueDelete:
      currentBase = none(string)
      doneForKey = true
    of memValueMerge:
      currentOperands.add(rec.value)

  finalizeCurrent()
  entries

proc memReadSource*(mt: MemTable;
                    snapshot: SequenceNumber = toSequence(high(uint64));
                    priority = defaultMemPriority): ReadSource =
  let entries = collectMemEntries(mt, snapshot)
  newReadSource(entries, priority, readSourceMem)

proc batchReadSource*(batch: batch_module.BatchBase;
                      comparator: Comparator;
                      mergeOp: MergeOperator = nil;
                      priority = defaultBatchPriority): ReadSource =
  if batch.isNil() or batch.len == 0:
    return newReadSource(@[], priority, readSourceBatch)

  var effectiveComp = comparator
  if effectiveComp.isNil():
    effectiveComp = compareBytewise

  if batch of batch_module.IndexedBatch:
    let indexed = cast[batch_module.IndexedBatch](batch)
    if indexed.index.isNil():
      return newReadSource(@[], priority, readSourceBatch)
    let entries = collectMemEntries(indexed.index, toSequence(high(uint64)))
    return newReadSource(entries, priority, readSourceBatch)

  var hasMerge = false
  for op in batch.entries():
    if op.kind == memValueMerge:
      hasMerge = true
      break
  if hasMerge and mergeOp.isNil():
    raise newException(ReadError,
      "batch contains merge operations; provide merge operator for materialization")

  var cfg = MemTableConfig()
  cfg.comparator = effectiveComp
  cfg.mergeOperator = mergeOp
  cfg.softLimitBytes = 0
  cfg.hardLimitBytes = 0
  cfg.resourceManager = nil

  let temp = initMemTable(cfg)
  let applyEntries = batch_module.toApplyEntries(batch, toSequence(1))
  if applyEntries.len > 0:
    temp.applyBatch(applyEntries)
  let entries = collectMemEntries(temp, toSequence(high(uint64)))
  newReadSource(entries, priority, readSourceBatch)

proc sstReadSource*(reader: TableReader;
                    priority = defaultSSTPriority): ReadSource =
  var entries: seq[ReadSourceEntry] = @[]
  for entry in reader.scan():
    entries.add(ReadSourceEntry(key: entry.key, value: some(entry.value)))
  newReadSource(entries, priority, readSourceSST)

proc customReadSource*(entries: seq[ReadSourceEntry];
                       priority: int): ReadSource =
  newReadSource(entries, priority, readSourceCustom)
