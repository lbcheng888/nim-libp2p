# 读路径子系统的公共类型。

import std/options

import pebble/core/types

type
  ReadError* = object of CatchableError

  ReadSourceKind* = enum
    readSourceMem,
    readSourceSST,
    readSourceCustom,
    readSourceBatch

  ReadSourceEntry* = object
    key*: Key
    value*: Option[string]

  ReadSource* = object
    entries*: seq[ReadSourceEntry]
    priority*: int
    kind*: ReadSourceKind

  MaterializedEntry* = object
    key*: Key
    value*: string
    sourcePriority*: int
    sourceKind*: ReadSourceKind

  IteratorBounds* = object
    lower*: Option[Key]
    upper*: Option[Key]

  ReadRange* = IteratorBounds

  ReadIteratorConfig* = object
    comparator*: Comparator
    bounds*: IteratorBounds
    prefix*: Option[string]
    segmentSize*: int
    prefetchDistance*: int
    maxCachedSegments*: int

proc newReadSource*(entries: seq[ReadSourceEntry];
                    priority: int;
                    kind: ReadSourceKind): ReadSource =
  ReadSource(entries: entries, priority: priority, kind: kind)

proc withBounds*(lower: Option[Key]; upper: Option[Key]): IteratorBounds =
  IteratorBounds(lower: lower, upper: upper)

proc newReadRange*(lower: Option[Key]; upper: Option[Key]): ReadRange =
  ReadRange(lower: lower, upper: upper)

proc emptyRange*(): ReadRange =
  ReadRange(lower: none(Key), upper: none(Key))
