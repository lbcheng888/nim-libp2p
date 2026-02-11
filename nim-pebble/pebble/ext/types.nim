# 范围键与范围删除的公共类型与基础操作。

import std/[algorithm, options, sequtils, strformat, strutils]

import pebble/core/types

type
  RangeKeyError* = object of CatchableError

  RangeKeyKind* = enum
    rangeKeySet,
    rangeKeyUnset,
    rangeKeyDelete

  RangeKeySuffix* = distinct string

  RangeKeyAtom* = object
    kind*: RangeKeyKind
    seq*: SequenceNumber
    suffix*: Option[RangeKeySuffix]
    value*: string

  RangeSpan* = object
    start*: Key
    upper*: Key
    keys*: seq[RangeKeyAtom]

  RangeLookupResult* = object
    ## 代表在给定快照下某个点 key 所能观察到的范围键状态。
    delete*: Option[RangeKeyAtom]
    sets*: seq[RangeKeyAtom]

proc toSuffix*(bytes: string): RangeKeySuffix =
  RangeKeySuffix(bytes)

proc toBytes*(suffix: RangeKeySuffix): string =
  string(suffix)

proc hasSuffix*(atom: RangeKeyAtom): bool {.inline.} =
  atom.suffix.isSome()

proc suffixBytes*(atom: RangeKeyAtom): string =
  if atom.hasSuffix():
    return atom.suffix.get().toBytes()
  ""

proc initRangeKeySet*(seq: SequenceNumber;
                      suffix: RangeKeySuffix;
                      value: string): RangeKeyAtom =
  RangeKeyAtom(
    kind: rangeKeySet,
    seq: seq,
    suffix: some(suffix),
    value: value
  )

proc initRangeKeyUnset*(seq: SequenceNumber;
                        suffix: RangeKeySuffix): RangeKeyAtom =
  RangeKeyAtom(
    kind: rangeKeyUnset,
    seq: seq,
    suffix: some(suffix),
    value: ""
  )

proc initRangeKeyDelete*(seq: SequenceNumber): RangeKeyAtom =
  RangeKeyAtom(
    kind: rangeKeyDelete,
    seq: seq,
    suffix: none(RangeKeySuffix),
    value: ""
  )

proc initRangeSpan*(start, upper: Key;
                    keys: openArray[RangeKeyAtom] = []): RangeSpan =
  RangeSpan(
    start: start,
    upper: upper,
    keys: keys.toSeq()
  )

proc compareAtoms*(a, b: RangeKeyAtom): int =
  let seqA = toUint64(a.seq)
  let seqB = toUint64(b.seq)
  if seqA != seqB:
    return (if seqA > seqB: -1 else: 1)
  if a.kind != b.kind:
    return cmp(a.kind.int, b.kind.int)
  cmp(a.suffixBytes(), b.suffixBytes())

proc sortKeys*(keys: var seq[RangeKeyAtom]) =
  keys.sort(compareAtoms)

proc sortKeys*(span: var RangeSpan) =
  span.keys.sortKeys()

proc addKey*(span: var RangeSpan; key: RangeKeyAtom) =
  span.keys.add(key)
  span.sortKeys()

proc ensureValidBounds*(span: RangeSpan;
                        comparator: Comparator = nil) =
  let cmp = if comparator.isNil(): compareBytewise else: comparator
  if cmp(span.start, span.upper) >= 0:
    raise newException(RangeKeyError,
      "range span upper bound must be greater than start")

proc describe*(span: RangeSpan): string =
  var parts: seq[string] = @[]
  for key in span.keys.items():
    let suffixPart = if key.hasSuffix(): " suffix=" & key.suffixBytes() else: ""
    parts.add(fmt"{key.kind}@{toUint64(key.seq)}{suffixPart}")
  fmt"[{span.start.toBytes()} .. {span.upper.toBytes()}) => " &
    parts.join(", ")

proc contains*(span: RangeSpan; key: Key;
               comparator: Comparator = nil): bool =
  ## 判断半开区间 [start, upper) 是否覆盖指定 key。
  let cmp = if comparator.isNil(): compareBytewise else: comparator
  cmp(span.start, key) <= 0 and cmp(key, span.upper) < 0

proc initRangeLookupResult*(delete: Option[RangeKeyAtom];
                            sets: openArray[RangeKeyAtom]): RangeLookupResult =
  RangeLookupResult(delete: delete, sets: sets.toSeq())

proc isEmpty*(res: RangeLookupResult): bool {.inline.} =
  res.delete.isNone() and res.sets.len == 0

proc hasDelete*(res: RangeLookupResult): bool {.inline.} =
  res.delete.isSome()

proc findSuffix*(res: RangeLookupResult;
                 suffix: RangeKeySuffix): Option[RangeKeyAtom] =
  ## 返回在指定后缀下可见的范围键原子。
  let target = suffix.toBytes()
  let deleteSeq =
    if res.hasDelete(): toUint64(res.delete.get().seq) else: 0'u64
  for atom in res.sets.items():
    if atom.hasSuffix() and atom.suffixBytes() == target:
      if res.hasDelete() and deleteSeq >= toUint64(atom.seq):
        return none(RangeKeyAtom)
      return some(atom)
  none(RangeKeyAtom)
