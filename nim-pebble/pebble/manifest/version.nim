# Version 表示一组有序的层级文件以及全局序列状态。

import std/[algorithm, options, sequtils, tables]

import pebble/core/types
import pebble/manifest/types
import pebble/manifest/version_edit

type
  LevelFiles = seq[FileMetadata]

  Version* = ref object
    id*: VersionId
    logNumber*: uint64
    prevLogNumber*: uint64
    nextFileNumber*: uint64
    lastSeqNum*: SequenceNumber
    comparatorName*: string
    levels*: Table[Level, LevelFiles]

proc initVersion*(comparatorName = "bytewise"): Version =
  Version(
    id: toVersionId(0),
    logNumber: 0,
    prevLogNumber: 0,
    nextFileNumber: 1,
    lastSeqNum: toSequence(0),
    comparatorName: comparatorName,
    levels: initTable[Level, LevelFiles]()
  )

proc clone*(version: Version): Version =
  if version.isNil():
    return nil
  var copy = Version(
    id: version.id,
    logNumber: version.logNumber,
    prevLogNumber: version.prevLogNumber,
    nextFileNumber: version.nextFileNumber,
    lastSeqNum: version.lastSeqNum,
    comparatorName: version.comparatorName,
    levels: initTable[Level, LevelFiles]()
  )
  for level, files in version.levels.pairs():
    copy.levels[level] = files
  copy

proc assignLevel(version: Version; level: Level; files: LevelFiles) =
  if files.len == 0:
    if version.levels.hasKey(level):
      version.levels.del(level)
  else:
    version.levels[level] = files

proc levelFiles*(version: Version; level: Level): seq[FileMetadata] =
  version.levels.getOrDefault(level, @[])

proc levels*(version: Version): seq[Level] =
  for level in version.levels.keys():
    result.add(level)
  result.sort()

proc touchNextFileNumber(version: Version; meta: FileMetadata) =
  let fileValue = meta.fileNum.toUint64()
  if fileValue + 1 > version.nextFileNumber:
    version.nextFileNumber = fileValue + 1

proc touchSequence(version: Version; meta: FileMetadata) =
  let largestSeq = meta.largestSeq.toUint64()
  if largestSeq > version.lastSeqNum.toUint64():
    version.lastSeqNum = toSequence(largestSeq)

proc insertFile(version: Version; level: Level; metadata: FileMetadata) =
  var files = version.levelFiles(level)
  let allowOverlap = level == 0
  if not allowOverlap:
    for existing in files:
      if existing.overlaps(metadata):
        raise newException(ManifestError,
          "Level " & $level & " 文件区间重叠: " &
          existing.describe() & " vs " & metadata.describe())
  files.add(metadata)
  if files.len > 1:
    files.sort(proc (a, b: FileMetadata): int =
      cmp(a.smallest.toBytes(), b.smallest.toBytes()))
  version.assignLevel(level, files)
  version.touchNextFileNumber(metadata)
  version.touchSequence(metadata)

proc removeFile(version: Version; level: Level; fileNum: FileNumber) =
  var files = version.levelFiles(level)
  if files.len == 0:
    return
  files = files.filterIt(it.fileNum != fileNum)
  version.assignLevel(level, files)

proc applyEdit*(base: Version; edit: VersionEdit): Version =
  if base.isNil():
    raise newException(ManifestError, "无法在空版本上应用 VersionEdit")
  var next = base.clone()
  if edit.comparatorName.isSome():
    let name = edit.comparatorName.get()
    if next.comparatorName.len > 0 and next.comparatorName != name:
      raise newException(ManifestError, "Comparator 变更不匹配")
    next.comparatorName = name
  if edit.logNumber.isSome():
    next.logNumber = edit.logNumber.get()
  if edit.prevLogNumber.isSome():
    next.prevLogNumber = edit.prevLogNumber.get()
  if edit.nextFileNumber.isSome():
    next.nextFileNumber = edit.nextFileNumber.get()
  if edit.lastSeqNum.isSome():
    next.lastSeqNum = edit.lastSeqNum.get()
  for delRef in edit.deletedFiles:
    next.removeFile(delRef.level, delRef.fileNum)
  for entry in edit.addedFiles:
    next.insertFile(entry.level, entry.metadata)
  next

proc totalFiles*(version: Version): int =
  for files in version.levels.values():
    result += files.len

proc levelEntries*(version: Version): seq[tuple[level: Level, files: seq[FileMetadata]]] =
  for level, files in version.levels.pairs():
    result.add((level: level, files: files))
  result.sort(proc (a, b: tuple[level: Level, files: seq[FileMetadata]]): int =
    cmp(a.level, b.level))
