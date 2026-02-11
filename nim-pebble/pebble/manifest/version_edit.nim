# VersionEdit 表示一次对 VersionSet 的增量修改。

import std/options

import pebble/core/types
import pebble/manifest/types

type
  VersionEdit* = object
    logNumber*: Option[uint64]
    prevLogNumber*: Option[uint64]
    nextFileNumber*: Option[uint64]
    lastSeqNum*: Option[SequenceNumber]
    comparatorName*: Option[string]
    deletedFiles*: seq[FileReference]
    addedFiles*: seq[tuple[level: Level, metadata: FileMetadata]]

proc initVersionEdit*(): VersionEdit =
  VersionEdit()

proc isEmpty*(edit: VersionEdit): bool =
  edit.logNumber.isNone() and edit.prevLogNumber.isNone() and
    edit.nextFileNumber.isNone() and edit.lastSeqNum.isNone() and
    edit.comparatorName.isNone() and edit.deletedFiles.len == 0 and
    edit.addedFiles.len == 0

proc setLogNumber*(edit: var VersionEdit; value: uint64) =
  edit.logNumber = some(value)

proc setPrevLogNumber*(edit: var VersionEdit; value: uint64) =
  edit.prevLogNumber = some(value)

proc setNextFileNumber*(edit: var VersionEdit; value: uint64) =
  edit.nextFileNumber = some(value)

proc setLastSequence*(edit: var VersionEdit; seq: SequenceNumber) =
  edit.lastSeqNum = some(seq)

proc setComparatorName*(edit: var VersionEdit; name: string) =
  edit.comparatorName = some(name)

proc addFile*(edit: var VersionEdit; level: Level; metadata: FileMetadata) =
  metadata.validateRange()
  edit.addedFiles.add((level: level, metadata: metadata))

proc deleteFile*(edit: var VersionEdit; level: Level; fileNum: FileNumber) =
  edit.deletedFiles.add(FileReference(level: level, fileNum: fileNum))

