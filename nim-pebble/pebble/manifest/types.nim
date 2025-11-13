# Manifest 子系统通用类型定义。

import std/strformat

import pebble/core/types

type
  ManifestError* = object of CatchableError

  VersionId* = distinct uint64
  FileNumber* = distinct uint64
  Level* = int

  FileMetadata* = object
    fileNum*: FileNumber
    sizeBytes*: uint64
    smallest*: Key
    largest*: Key
    smallestSeq*: SequenceNumber
    largestSeq*: SequenceNumber

  FileReference* = object
    level*: Level
    fileNum*: FileNumber

proc toVersionId*(value: uint64): VersionId =
  VersionId(value)

proc toUint64*(id: VersionId): uint64 =
  uint64(id)

proc inc*(id: var VersionId) =
  id = VersionId(id.toUint64() + 1'u64)

proc toFileNumber*(value: uint64): FileNumber =
  FileNumber(value)

proc toUint64*(value: FileNumber): uint64 =
  uint64(value)

proc `$`*(value: FileNumber): string =
  $value.toUint64()

proc `<`*(a, b: FileNumber): bool =
  a.toUint64() < b.toUint64()

proc `==`*(a, b: FileNumber): bool =
  a.toUint64() == b.toUint64()

proc overlaps*(a, b: FileMetadata): bool =
  ## 判断两个文件元数据的键区间是否有交集。
  let aSmall = a.smallest.toBytes()
  let aLarge = a.largest.toBytes()
  let bSmall = b.smallest.toBytes()
  let bLarge = b.largest.toBytes()
  not (aLarge < bSmall or bLarge < aSmall)

proc describe*(meta: FileMetadata): string =
  fmt"file={meta.fileNum},size={meta.sizeBytes},keys=[{meta.smallest.toBytes()}..{meta.largest.toBytes()}],seq=[{meta.smallestSeq.toUint64()}..{meta.largestSeq.toUint64()}]"

proc validateRange*(meta: FileMetadata) =
  let smallest = meta.smallest.toBytes()
  let largest = meta.largest.toBytes()
  if smallest.len == 0 or largest.len == 0:
    raise newException(ManifestError, "文件键区间不能为空")
  if largest < smallest:
    raise newException(ManifestError, "largest key 小于 smallest key")
