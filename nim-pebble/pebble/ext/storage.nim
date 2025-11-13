# 范围键存储结构，实现覆盖查询与范围删除下推辅助。

import std/[algorithm, options, sequtils, tables]

import pebble/core/types
import pebble/ext/types

type
  RangeKeyStorage* = ref object
    comparator: Comparator
    spans: seq[RangeSpan]

proc ensureComparator(comp: Comparator): Comparator =
  if comp.isNil():
    return compareBytewise
  comp

proc newRangeKeyStorage*(comparator: Comparator = nil): RangeKeyStorage =
  new(result)
  result.comparator = ensureComparator(comparator)
  result.spans = @[]

proc cmp(storage: RangeKeyStorage; a, b: Key): int {.inline.} =
  storage.comparator(a, b)

proc overlaps(storage: RangeKeyStorage;
              a, b: RangeSpan): bool =
  storage.cmp(a.upper, b.start) > 0 and storage.cmp(b.upper, a.start) > 0

proc combineSpans(storage: RangeKeyStorage;
                  a, b: RangeSpan): RangeSpan =
  var merged = RangeSpan(
    start: (if storage.cmp(a.start, b.start) <= 0: a.start else: b.start),
    upper: (if storage.cmp(a.upper, b.upper) >= 0: a.upper else: b.upper),
    keys: @[]
  )
  merged.keys.add(a.keys)
  merged.keys.add(b.keys)
  merged.sortKeys()
  merged

proc mergeAround(storage: RangeKeyStorage; idx: int) =
  if storage.isNil() or idx < 0 or idx >= storage.spans.len:
    return
  var current = storage.spans[idx]
  var pos = idx

  # 向左合并覆盖片段。
  while pos > 0:
    let prev = storage.spans[pos - 1]
    if not storage.overlaps(prev, current):
      break
    current = storage.combineSpans(prev, current)
    storage.spans.delete(pos)
    storage.spans[pos - 1] = current
    dec pos

  storage.spans[pos] = current

  # 向右合并覆盖片段。
  var next = pos + 1
  while next < storage.spans.len:
    let candidate = storage.spans[next]
    if not storage.overlaps(candidate, current):
      break
    current = storage.combineSpans(current, candidate)
    storage.spans[pos] = current
    storage.spans.delete(next)
  storage.spans[pos] = current

proc addSpan*(storage: RangeKeyStorage; span: RangeSpan) =
  if storage.isNil():
    raise newException(RangeKeyError, "storage is not initialised")
  var normalized = span
  normalized.sortKeys()
  normalized.ensureValidBounds(storage.comparator)

  var insertIdx = 0
  while insertIdx < storage.spans.len and
        storage.cmp(storage.spans[insertIdx].start, normalized.start) <= 0:
    inc insertIdx
  storage.spans.insert(normalized, insertIdx)
  storage.mergeAround(insertIdx)

proc addRangeKeySet*(storage: RangeKeyStorage;
                     start, upper: Key;
                     suffix: RangeKeySuffix;
                     seq: SequenceNumber;
                     value: string) =
  var span = initRangeSpan(start, upper, [initRangeKeySet(seq, suffix, value)])
  storage.addSpan(span)

proc addRangeKeyUnset*(storage: RangeKeyStorage;
                       start, upper: Key;
                       suffix: RangeKeySuffix;
                       seq: SequenceNumber) =
  var span = initRangeSpan(start, upper, [initRangeKeyUnset(seq, suffix)])
  storage.addSpan(span)

proc addRangeDeletion*(storage: RangeKeyStorage;
                       start, upper: Key;
                       seq: SequenceNumber) =
  ## 插入范围删除 tombstone，供范围覆盖判断与下推使用。
  var span = initRangeSpan(start, upper, [initRangeKeyDelete(seq)])
  storage.addSpan(span)

proc len*(storage: RangeKeyStorage): int =
  if storage.isNil():
    return 0
  storage.spans.len

iterator spans*(storage: RangeKeyStorage): RangeSpan =
  if not storage.isNil():
    for span in storage.spans.items():
      yield span

proc kindPriority(kind: RangeKeyKind): int {.inline.} =
  case kind
  of rangeKeySet: 3
  of rangeKeyUnset: 2
  of rangeKeyDelete: 1

proc preferNewCandidate(newAtom, existing: RangeKeyAtom): bool =
  let newSeq = toUint64(newAtom.seq)
  let currentSeq = toUint64(existing.seq)
  if newSeq != currentSeq:
    return newSeq > currentSeq
  let newPriority = kindPriority(newAtom.kind)
  let currentPriority = kindPriority(existing.kind)
  if newPriority != currentPriority:
    return newPriority > currentPriority
  cmp(newAtom.suffixBytes(), existing.suffixBytes()) > 0

proc findSpanIndex(storage: RangeKeyStorage; key: Key): int =
  if storage.isNil():
    return -1
  for idx, span in storage.spans.pairs():
    if span.contains(key, storage.comparator):
      return idx
  -1

proc resolveSpan(storage: RangeKeyStorage;
                 span: RangeSpan;
                 snapshot: SequenceNumber): RangeLookupResult =
  var deleteAtom: Option[RangeKeyAtom] = none(RangeKeyAtom)
  var perSuffix = initTable[string, RangeKeyAtom]()
  let snapValue = toUint64(snapshot)
  for atom in span.keys.items():
    let atomSeq = toUint64(atom.seq)
    if atomSeq > snapValue:
      continue
    case atom.kind
    of rangeKeyDelete:
      if deleteAtom.isNone() or preferNewCandidate(atom, deleteAtom.get()):
        deleteAtom = some(atom)
    of rangeKeySet, rangeKeyUnset:
      if not atom.hasSuffix():
        continue
      let suffixKey = atom.suffixBytes()
      if perSuffix.hasKey(suffixKey):
        if preferNewCandidate(atom, perSuffix[suffixKey]):
          perSuffix[suffixKey] = atom
      else:
        perSuffix[suffixKey] = atom
  var sets: seq[RangeKeyAtom] = @[]
  for atom in perSuffix.values():
    if atom.kind != rangeKeySet:
      continue
    if deleteAtom.isSome() and
       toUint64(deleteAtom.get().seq) >= toUint64(atom.seq):
      continue
    sets.add(atom)
  sets.sort(proc (a, b: RangeKeyAtom): int =
    cmp(a.suffixBytes(), b.suffixBytes())
  )
  initRangeLookupResult(deleteAtom, sets)

proc lookup*(storage: RangeKeyStorage;
             key: Key;
             snapshot: SequenceNumber): RangeLookupResult =
  ## 返回在给定 snapshot 下，覆盖指定 key 的范围键最终状态。
  if storage.isNil():
    return initRangeLookupResult(none(RangeKeyAtom), @[])
  let idx = storage.findSpanIndex(key)
  if idx < 0:
    return initRangeLookupResult(none(RangeKeyAtom), @[])
  storage.resolveSpan(storage.spans[idx], snapshot)

proc resolveSuffix*(storage: RangeKeyStorage;
                    key: Key;
                    suffix: RangeKeySuffix;
                    snapshot: SequenceNumber): Option[RangeKeyAtom] =
  let view = storage.lookup(key, snapshot)
  view.findSuffix(suffix)

proc valueForSuffix*(storage: RangeKeyStorage;
                     key: Key;
                     suffix: RangeKeySuffix;
                     snapshot: SequenceNumber): Option[string] =
  let atomOpt = storage.resolveSuffix(key, suffix, snapshot)
  if atomOpt.isSome():
    return some(atomOpt.get().value)
  none(string)

proc coveringDelete*(storage: RangeKeyStorage;
                     key: Key;
                     snapshot: SequenceNumber): Option[RangeKeyAtom] =
  ## 返回在给定可见序列号下，覆盖该 key 的最大序列号范围删除。
  let view = storage.lookup(key, snapshot)
  if view.delete.isSome():
    return view.delete
  none(RangeKeyAtom)

proc masksPoint*(storage: RangeKeyStorage;
                 key: Key;
                 snapshot: SequenceNumber): bool =
  ## 判断 point key 是否被范围删除覆盖，用于读路径/压实提前过滤。
  storage.coveringDelete(key, snapshot).isSome()

proc clear*(storage: RangeKeyStorage) =
  if storage.isNil():
    return
  storage.spans.setLen(0)

proc compact*(storage: RangeKeyStorage; snapshot: SequenceNumber) =
  ## 针对给定可见序列号压实范围键：去除被覆盖的 set/unset，合并连续 delete。
  if storage.isNil():
    return
  var result: seq[RangeSpan] = @[]
  for span in storage.spans.items():
    var bestDelete: Option[RangeKeyAtom] = none(RangeKeyAtom)
    var latestPerSuffix = initTable[string, RangeKeyAtom]()
    for atom in span.keys.items():
      if toUint64(atom.seq) > toUint64(snapshot):
        continue
      case atom.kind
      of rangeKeyDelete:
        if bestDelete.isNone() or toUint64(atom.seq) > toUint64(bestDelete.get().seq):
          bestDelete = some(atom)
      of rangeKeySet, rangeKeyUnset:
        if not atom.hasSuffix():
          continue
        let suffix = atom.suffixBytes()
        if latestPerSuffix.hasKey(suffix):
          if toUint64(atom.seq) > toUint64(latestPerSuffix[suffix].seq):
            latestPerSuffix[suffix] = atom
        else:
          latestPerSuffix[suffix] = atom
    var mergedKeys: seq[RangeKeyAtom] = @[]
    if bestDelete.isSome():
      mergedKeys.add(bestDelete.get())
    for atom in latestPerSuffix.values():
      if bestDelete.isSome() and toUint64(atom.seq) <= toUint64(bestDelete.get().seq):
        continue
      mergedKeys.add(atom)
    if mergedKeys.len == 0:
      continue
    mergedKeys.sort(compareAtoms)
    var newSpan = RangeSpan(start: span.start, upper: span.upper, keys: mergedKeys)
    newSpan.sortKeys()
    if result.len > 0:
      var prev = result[^1]
      let sameKeys = prev.keys.len == newSpan.keys.len and
        prev.keys.zip(newSpan.keys).all(proc (pair: (RangeKeyAtom, RangeKeyAtom)): bool =
          pair[0].kind == pair[1].kind and
          pair[0].suffixBytes() == pair[1].suffixBytes() and
          toUint64(pair[0].seq) == toUint64(pair[1].seq))
      if sameKeys and storage.cmp(prev.upper, newSpan.start) >= 0:
        if storage.cmp(prev.upper, newSpan.upper) < 0:
          prev.upper = newSpan.upper
        result[^1] = prev
        continue
    result.add(newSpan)
  storage.spans = result
