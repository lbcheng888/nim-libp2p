# MemTable 实现：负责处理内存写入、批量应用与 flush 迭代。

import std/[algorithm, json, options, strutils]

import pebble/core/types
import pebble/mem/types
import pebble/runtime/executor
import pebble/runtime/resource_manager

const entryOverhead = 16

proc defaultComparator(): Comparator =
  compareBytewise

proc initMemTable*(cfg: MemTableConfig): MemTable =
  ## 构建新的 MemTable，若未指定比较器则回退到字节序比较。
  new(result)
  var normalized = cfg
  normalized.throttleConfig.normalize()
  result.cfg = normalized
  if result.cfg.comparator.isNil():
    result.cfg.comparator = defaultComparator()
  result.comparator = result.cfg.comparator
  result.approxBytes = 0
  result.records = @[]
  result.bytesUsed = 0
  result.lastSeq = toSequence(0)
  result.tickets = @[]
  result.frozen = false
  result.flushScheduled = false
  result.throttleActive = false
  result.lastThrottleDelayMs = 0

proc verifySequence(mt: MemTable; seq: SequenceNumber) =
  if toUint64(seq) < toUint64(mt.lastSeq):
    raise newException(MemError, "sequence numbers must be monotonically increasing")

proc triggerFlush(mt: MemTable) =
  if mt.cfg.flushTrigger.isNil():
    return
  if mt.flushScheduled:
    return
  mt.flushScheduled = true
  let target: Flushable = mt
  if mt.cfg.flushExecutor.isNil():
    mt.cfg.flushTrigger(target)
  else:
    discard mt.cfg.flushExecutor.submit(proc () {.gcsafe.} =
      mt.cfg.flushTrigger(target)
    )

proc markFrozenForFlush(mt: MemTable) =
  if not mt.frozen:
    mt.frozen = true
  mt.triggerFlush()

proc computeStallLimit(mt: MemTable): int =
  if mt.cfg.throttleConfig.stallLimitBytes > 0:
    return mt.cfg.throttleConfig.stallLimitBytes
  mt.cfg.softLimitBytes

proc computeReleaseTarget(mt: MemTable; stallLimit: int): int =
  if mt.cfg.throttleConfig.releaseTargetBytes > 0:
    return mt.cfg.throttleConfig.releaseTargetBytes
  if stallLimit <= 0:
    return 0
  let reduction = max(1, stallLimit div 4) # 25% 回落阈值
  max(0, stallLimit - reduction)

proc reserveBytes(mt: MemTable; bytes: int) =
  if bytes <= 0:
    return
  let projected = mt.bytesUsed + bytes
  if mt.cfg.hardLimitBytes > 0 and projected > mt.cfg.hardLimitBytes:
    raise newException(MemError, "memtable capacity exceeded (hard limit)")
  if not mt.cfg.resourceManager.isNil():
    let ticket = acquireQuota(mt.cfg.resourceManager, mt.cfg.resourceKind, bytes.int64)
    mt.tickets.add(ticket)
  mt.bytesUsed = projected
  mt.approxBytes = projected
  if not mt.cfg.resourceManager.isNil() and
     isSoftExceeded(mt.cfg.resourceManager, mt.cfg.resourceKind):
    mt.markFrozenForFlush()

proc checkWritable(mt: MemTable) =
  if mt.frozen:
    raise newException(MemError, "memtable is frozen and cannot accept new writes")

proc appendRecord(mt: MemTable; key: Key; seq: SequenceNumber;
                  kind: MemValueKind; value: string) =
  mt.checkWritable()
  mt.verifySequence(seq)
  let sizeBytes = key.toBytes().len + value.len + entryOverhead
  mt.reserveBytes(sizeBytes)
  mt.records.add(MemRecord(key: key, seq: seq, kind: kind, value: value))
  if toUint64(seq) >= toUint64(mt.lastSeq):
    mt.lastSeq = seq
  if mt.cfg.softLimitBytes > 0 and mt.bytesUsed >= mt.cfg.softLimitBytes:
    mt.markFrozenForFlush()

proc put*(mt: MemTable; key: Key; value: string; seq: SequenceNumber) =
  mt.appendRecord(key, seq, memValueSet, value)

proc delete*(mt: MemTable; key: Key; seq: SequenceNumber) =
  mt.appendRecord(key, seq, memValueDelete, "")

proc merge*(mt: MemTable; key: Key; operand: string; seq: SequenceNumber) =
  if not mt.cfg.hasMerge():
    raise newException(MemError, "merge requested without configured merge operator")
  mt.appendRecord(key, seq, memValueMerge, operand)

proc applyBatch*(mt: MemTable; entries: openArray[ApplyEntry]) =
  for entry in entries.items():
    case entry.kind
    of memValueSet:
      mt.put(entry.key, entry.value, entry.seq)
    of memValueDelete:
      mt.delete(entry.key, entry.seq)
    of memValueMerge:
      mt.merge(entry.key, entry.value, entry.seq)

proc admitWrite*(mt: MemTable;
                 priority: MemWritePriority;
                 incomingBytes: int): int =
  ## 根据内存水位和写入优先级计算应当施加的等待时间（毫秒）。
  if mt.isNil() or incomingBytes <= 0:
    return 0
  let cfg = mt.cfg.throttleConfig
  let stallLimit = mt.computeStallLimit()
  if not cfg.throttleEnabled() or stallLimit <= 0:
    if mt.throttleActive:
      let releaseTarget = mt.computeReleaseTarget(max(stallLimit, 0))
      if releaseTarget <= 0 or mt.bytesUsed <= releaseTarget:
        mt.throttleActive = false
        mt.lastThrottleDelayMs = 0
    return 0
  let projected = mt.bytesUsed + incomingBytes
  let releaseTarget = mt.computeReleaseTarget(stallLimit)
  if projected < stallLimit:
    if mt.throttleActive and (releaseTarget <= 0 or projected <= releaseTarget):
      mt.throttleActive = false
      mt.lastThrottleDelayMs = 0
    return 0
  mt.triggerFlush()
  var effectivePriority = priority
  if effectivePriority == memPriorityDefault:
    effectivePriority = memPriorityNormal
  var scaleValue = cfg.priorityScale[effectivePriority]
  if scaleValue <= 0.0:
    scaleValue = 1.0
  let baseDelay = max(1, cfg.baseDelayMs)
  let maxDelay = max(baseDelay, cfg.maxDelayMs)
  let over = projected - stallLimit
  var ratio = over.float / max(1, stallLimit).float
  if ratio > 1.0:
    ratio = 1.0
  let dynamic = int(ratio * float(maxDelay - baseDelay) * scaleValue)
  var delay = baseDelay + dynamic
  delay = max(baseDelay, delay)
  delay = min(maxDelay, delay)
  mt.throttleActive = true
  mt.lastThrottleDelayMs = delay
  result = delay

proc toUint(seq: SequenceNumber): uint64 {.inline.} =
  toUint64(seq)

proc mergeResolve(mt: MemTable; key: Key; base: Option[string];
                  operands: seq[string]): Option[string] =
  if operands.len == 0:
    return base
  if not mt.cfg.hasMerge():
    raise newException(MemError, "merge resolution requested without operator")
  var ordered = operands
  ordered.reverse() # 从旧到新应用。
  result = mt.cfg.mergeOperator(key, base, ordered)

proc get*(mt: MemTable; key: Key;
          snapshot: SequenceNumber = toSequence(high(uint64))): Option[string] =
  let snapValue = snapshot.toUint()
  var operands: seq[string] = @[]
  var base: Option[string] = none(string)
  var seen = false
  for idx in countdown(mt.records.len - 1, 0):
    let record = mt.records[idx]
    if mt.comparator(key, record.key) != 0:
      continue
    if record.seq.toUint() > snapValue:
      continue
    seen = true
    case record.kind
    of memValueSet:
      base = some(record.value)
      break
    of memValueDelete:
      base = none(string)
      break
    of memValueMerge:
      operands.add(record.value)
  if not seen:
    return none(string)
  if operands.len == 0:
    return base
  result = mt.mergeResolve(key, base, operands)

proc freeze*(mt: MemTable) =
  mt.frozen = true

proc readyForFlush*(mt: MemTable): bool =
  mt.frozen

proc needsFlush*(mt: MemTable): bool =
  if mt.cfg.softLimitBytes > 0 and mt.bytesUsed >= mt.cfg.softLimitBytes:
    return true
  if not mt.cfg.resourceManager.isNil():
    return isSoftExceeded(mt.cfg.resourceManager, mt.cfg.resourceKind)
  false

proc stats*(mt: MemTable): MemTableStats =
  if mt.isNil():
    return MemTableStats()
  MemTableStats(
    approxBytes: mt.approxBytes,
    softLimitBytes: mt.cfg.softLimitBytes,
    hardLimitBytes: mt.cfg.hardLimitBytes,
    frozen: mt.frozen,
    throttleActive: mt.throttleActive,
    lastThrottleDelayMs: mt.lastThrottleDelayMs
  )

proc clear*(mt: MemTable) =
  if not mt.cfg.resourceManager.isNil():
    for ticket in mt.tickets.items():
      release(mt.cfg.resourceManager, ticket)
  mt.tickets.setLen(0)
  mt.records.setLen(0)
  mt.bytesUsed = 0
  mt.approxBytes = 0
  mt.frozen = false
  mt.lastSeq = toSequence(0)
  mt.flushScheduled = false
  mt.throttleActive = false
  mt.lastThrottleDelayMs = 0

proc markFlushCompleted*(mt: MemTable) =
  ## 手动重置 flush 状态，用于外部调度器在完成刷新后复位写路径。
  if mt.isNil():
    return
  mt.flushScheduled = false
  mt.frozen = false
  mt.throttleActive = false
  mt.lastThrottleDelayMs = 0

iterator flushIterator*(mt: MemTable): MemRecord =
  if mt.records.len > 0:
    var data = newSeq[MemRecord](mt.records.len)
    for idx in 0 ..< mt.records.len:
      data[idx] = mt.records[idx]
    data.sort(proc (a, b: MemRecord): int =
      let cmp = mt.comparator(a.key, b.key)
      if cmp != 0:
        return cmp
      let seqA = a.seq.toUint()
      let seqB = b.seq.toUint()
      if seqA == seqB:
        return 0
      if seqA > seqB:
        return -1
      1
    )
    for record in data.items():
      yield record

proc exportFrozen*(mt: MemTable; label = ""): FrozenMemTable =
  if mt.isNil():
    raise newException(MemError, "cannot export nil memtable")
  if not mt.readyForFlush():
    raise newException(MemError, "memtable must be frozen before export")
  result.label = label
  result.approxBytes = mt.approxBytes
  result.softLimitBytes = mt.cfg.softLimitBytes
  result.hardLimitBytes = mt.cfg.hardLimitBytes
  result.lastSeq = toUint64(mt.lastSeq)
  result.throttleActive = mt.throttleActive
  result.throttleDelayMs = mt.lastThrottleDelayMs
  var aggregated: array[ResourceKind, int64]
  for ticket in mt.tickets.items():
    aggregated[ticket.kind] += ticket.amount
  for kind in ResourceKind:
    let outstanding = aggregated[kind]
    if outstanding != 0:
      result.resources.add(FrozenResourceState(
        kind: kind,
        outstandingBytes: outstanding
      ))
  for rec in mt.flushIterator():
    result.entries.add(FrozenMemEntry(
      key: rec.key.toBytes(),
      seq: toUint64(rec.seq),
      kind: rec.kind,
      value: rec.value
    ))
  result.totalEntries = result.entries.len

proc toJsonNode*(snapshot: FrozenMemTable): JsonNode =
  var node = newJObject()
  node["label"] = newJString(snapshot.label)
  node["approxBytes"] = newJInt(BiggestInt(snapshot.approxBytes))
  node["totalEntries"] = newJInt(BiggestInt(snapshot.totalEntries))
  node["softLimitBytes"] = newJInt(BiggestInt(snapshot.softLimitBytes))
  node["hardLimitBytes"] = newJInt(BiggestInt(snapshot.hardLimitBytes))
  node["lastSeq"] = newJString($snapshot.lastSeq)
  node["throttleActive"] = newJBool(snapshot.throttleActive)
  node["throttleDelayMs"] = newJInt(BiggestInt(snapshot.throttleDelayMs))
  var resArr = newJArray()
  for res in snapshot.resources.items():
    var resNode = newJObject()
    resNode["kind"] = newJString(resourceKindToString(res.kind))
    resNode["outstandingBytes"] = newJString($res.outstandingBytes)
    resArr.add(resNode)
  node["resources"] = resArr
  var entriesArr = newJArray()
  for entry in snapshot.entries.items():
    var entryNode = newJObject()
    entryNode["key"] = newJString(entry.key)
    entryNode["seq"] = newJString($entry.seq)
    entryNode["kind"] = newJString(memValueKindToString(entry.kind))
    entryNode["value"] = newJString(entry.value)
    entriesArr.add(entryNode)
  node["entries"] = entriesArr
  node

proc parseUInt64String(value: string): uint64 =
  let parsed = parseBiggestUInt(value)
  uint64(parsed)

proc parseInt64String(value: string): int64 =
  let parsed = parseBiggestInt(value)
  int64(parsed)

proc fromJsonNode*(node: JsonNode): FrozenMemTable =
  if node.kind != JObject:
    raise newException(ValueError, "frozen memtable export must be an object")
  if node.hasKey("label"):
    result.label = node["label"].getStr()
  if node.hasKey("approxBytes"):
    result.approxBytes = int(node["approxBytes"].getInt())
  if node.hasKey("totalEntries"):
    result.totalEntries = int(node["totalEntries"].getInt())
  if node.hasKey("softLimitBytes"):
    result.softLimitBytes = int(node["softLimitBytes"].getInt())
  if node.hasKey("hardLimitBytes"):
    result.hardLimitBytes = int(node["hardLimitBytes"].getInt())
  if node.hasKey("lastSeq"):
    let lastSeqNode = node["lastSeq"]
    case lastSeqNode.kind
    of JString:
      result.lastSeq = parseUInt64String(lastSeqNode.getStr())
    of JInt:
      result.lastSeq = uint64(lastSeqNode.getInt())
    else:
      raise newException(ValueError, "unsupported lastSeq encoding")
  if node.hasKey("throttleActive"):
    result.throttleActive = node["throttleActive"].getBool()
  if node.hasKey("throttleDelayMs"):
    result.throttleDelayMs = int(node["throttleDelayMs"].getInt())
  if node.hasKey("resources"):
    for resNode in node["resources"].items():
      let kindStr = resNode["kind"].getStr()
      let amountNode = resNode["outstandingBytes"]
      var amount: int64
      case amountNode.kind
      of JString:
        amount = parseInt64String(amountNode.getStr())
      of JInt:
        amount = int64(amountNode.getInt())
      else:
        raise newException(ValueError, "invalid outstandingBytes encoding")
      result.resources.add(FrozenResourceState(
        kind: resourceKindFromString(kindStr),
        outstandingBytes: amount
      ))
  if node.hasKey("entries"):
    for entryNode in node["entries"].items():
      let keyStr = entryNode["key"].getStr()
      let seqNode = entryNode["seq"]
      var seqValue: uint64
      case seqNode.kind
      of JString:
        seqValue = parseUInt64String(seqNode.getStr())
      of JInt:
        seqValue = uint64(seqNode.getInt())
      else:
        raise newException(ValueError, "invalid seq encoding")
      let kindStr = entryNode["kind"].getStr()
      let valueStr = entryNode["value"].getStr()
      result.entries.add(FrozenMemEntry(
        key: keyStr,
        seq: seqValue,
        kind: memValueKindFromString(kindStr),
        value: valueStr
      ))
  result.totalEntries = result.entries.len

proc toJsonString*(snapshot: FrozenMemTable; pretty = false): string =
  let node = snapshot.toJsonNode()
  if pretty:
    node.pretty()
  else:
    $node

proc fromJsonString*(payload: string): FrozenMemTable =
  let node = parseJson(payload)
  fromJsonNode(node)

proc writeFrozen*(snapshot: FrozenMemTable; path: string; pretty = true) =
  let data = snapshot.toJsonString(pretty)
  writeFile(path, data)

proc readFrozen*(path: string): FrozenMemTable =
  let payload = readFile(path)
  fromJsonString(payload)
