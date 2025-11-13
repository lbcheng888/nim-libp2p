# Core type definitions shared across the Pebble Nim rewrite.
#
# This module exposes strongly typed keys, sequence numbers, and comparator
# registration APIs so higher-level subsystems can maintain consistent ordering
# semantics with the Go implementation.

import std/[options, tables, hashes]

type
  KeyKind* = enum
    keyKindUnknown,
    keyKindValue,
    keyKindDeletion,
    keyKindRangeDeletion,
    keyKindRangeKeySet,
    keyKindRangeKeyUnset,
    keyKindRangeKeyDelete

  Key* = distinct string

  SequenceNumber* = distinct uint64

  Comparator* = proc (a, b: Key): int {.gcsafe, raises: [].}

  ComparatorRegistry* = ref object
    defaultComp*: Comparator
    registry*: Table[string, Comparator]

proc toKey*(bytes: string): Key =
  ## Wrap raw bytes with the strong Key type.
  Key(bytes)

proc toBytes*(key: Key): string =
  ## Convert a Key back to its byte representation.
  string(key)

proc toSequence*(value: uint64): SequenceNumber =
  SequenceNumber(value)

proc toUint64*(seq: SequenceNumber): uint64 =
  uint64(seq)

proc `$`*(key: Key): string =
  key.toBytes()

proc hash*(key: Key): Hash =
  hash(key.toBytes())

proc compareBytewise*(a, b: Key): int {.gcsafe, raises: [].} =
  ## Default comparator matching Pebble's bytewise ordering.
  let astr = a.toBytes()
  let bstr = b.toBytes()
  if astr == bstr:
    return 0
  if astr < bstr:
    return -1
  result = 1

proc newComparatorRegistry*(defaultComp = compareBytewise): ComparatorRegistry =
  ComparatorRegistry(defaultComp: defaultComp, registry: initTable[string, Comparator]())

proc registerComparator*(registry: ComparatorRegistry, name: string, comp: Comparator) =
  registry.registry[name] = comp

proc getComparator*(registry: ComparatorRegistry, name: string): Option[Comparator] =
  if registry.registry.hasKey(name):
    some(registry.registry[name])
  else:
    none(Comparator)

proc resolveComparator*(registry: ComparatorRegistry, name = ""): Comparator =
  ## Resolve comparator by name or fall back to the default comparator.
  if name.len == 0:
    return registry.defaultComp
  if registry.registry.hasKey(name):
    return registry.registry[name]
  raise newException(ValueError, "unknown comparator: " & name)
