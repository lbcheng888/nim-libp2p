# Manifest 文件的简单存储实现，基于本地文件系统的追加写策略。

import std/[json, options, os, strutils, tables]

import pebble/core/types
import pebble/manifest/types
import pebble/manifest/version
import pebble/manifest/version_edit

type
  ManifestFormatVersion* = enum
    manifestFormatV1 = 1,
    manifestFormatV2 = 2

  ManifestRecordKind* = enum
    recordSnapshot,
    recordEdit

  ManifestRecord* = object
    formatVersion*: ManifestFormatVersion
    case kind*: ManifestRecordKind
    of recordSnapshot:
      version*: Version
    of recordEdit:
      edit*: VersionEdit

  ManifestStore* = ref object
    path*: string
    formatVersion*: ManifestFormatVersion

proc initManifestStore*(path: string;
                        formatVersion: ManifestFormatVersion = manifestFormatV1): ManifestStore =
  new(result)
  result.path = path
  result.formatVersion = formatVersion

template ensureStore(store: ManifestStore) =
  if store.isNil():
    raise newException(ManifestError, "manifest store 未初始化")

proc ensureParentDir(path: string) =
  let parent = splitPath(path).head
  if parent.len > 0 and not dirExists(parent):
    createDir(parent)

proc newJsonString(value: uint64): JsonNode =
  newJString($value)

proc parseUint64(node: JsonNode; field: string): uint64 =
  if not node.hasKey(field):
    raise newException(ManifestError, "manifest 字段缺失: " & field)
  let value = node[field]
  case value.kind
  of JString:
    result = parseUInt(value.getStr())
  of JInt:
    result = uint64(value.getBiggestInt())
  else:
    raise newException(ManifestError, "字段 " & field & " 不是无符号整数")

proc fileMetaToJson(meta: FileMetadata): JsonNode =
  result = newJObject()
  result["fileNum"] = newJsonString(meta.fileNum.toUint64())
  result["sizeBytes"] = newJsonString(meta.sizeBytes)
  result["smallest"] = newJString(meta.smallest.toBytes())
  result["largest"] = newJString(meta.largest.toBytes())
  result["smallestSeq"] = newJsonString(meta.smallestSeq.toUint64())
  result["largestSeq"] = newJsonString(meta.largestSeq.toUint64())

proc fileMetaFromJson(node: JsonNode): FileMetadata =
  if node.kind != JObject:
    raise newException(ManifestError, "文件元数据必须是对象")
  let fileNum = parseUint64(node, "fileNum")
  let sizeBytes = parseUint64(node, "sizeBytes")
  let smallest = node["smallest"].getStr().toKey()
  let largest = node["largest"].getStr().toKey()
  let smallestSeq = parseUint64(node, "smallestSeq")
  let largestSeq = parseUint64(node, "largestSeq")
  result = FileMetadata(
    fileNum: toFileNumber(fileNum),
    sizeBytes: sizeBytes,
    smallest: smallest,
    largest: largest,
    smallestSeq: toSequence(smallestSeq),
    largestSeq: toSequence(largestSeq)
  )
  result.validateRange()

proc versionToJson(version: Version): JsonNode =
  result = newJObject()
  result["id"] = newJsonString(version.id.toUint64())
  result["logNumber"] = newJsonString(version.logNumber)
  result["prevLogNumber"] = newJsonString(version.prevLogNumber)
  result["nextFileNumber"] = newJsonString(version.nextFileNumber)
  result["lastSeqNum"] = newJsonString(version.lastSeqNum.toUint64())
  result["comparatorName"] = newJString(version.comparatorName)
  var levelsNode = newJArray()
  for entry in version.levelEntries():
    var levelObj = newJObject()
    levelObj["level"] = newJInt(entry.level)
    var filesNode = newJArray()
    for meta in entry.files:
      filesNode.add(fileMetaToJson(meta))
    levelObj["files"] = filesNode
    levelsNode.add(levelObj)
  result["levels"] = levelsNode

proc versionFromJson(node: JsonNode): Version =
  if node.kind != JObject:
    raise newException(ManifestError, "version 节点必须是对象")
  var version = initVersion()
  version.id = toVersionId(parseUint64(node, "id"))
  version.logNumber = parseUint64(node, "logNumber")
  version.prevLogNumber = parseUint64(node, "prevLogNumber")
  version.nextFileNumber = parseUint64(node, "nextFileNumber")
  version.lastSeqNum = toSequence(parseUint64(node, "lastSeqNum"))
  version.comparatorName = node["comparatorName"].getStr()
  version.levels = initTable[Level, seq[FileMetadata]]()
  var maxFile = version.nextFileNumber
  var maxSeq = version.lastSeqNum.toUint64()
  if node.hasKey("levels"):
    for levelNode in node["levels"]:
      let level = Level(levelNode["level"].getInt())
      var files = newSeq[FileMetadata]()
      if levelNode.hasKey("files"):
        for metaNode in levelNode["files"]:
          let meta = fileMetaFromJson(metaNode)
          files.add(meta)
          let fileVal = meta.fileNum.toUint64()
          if fileVal + 1 > maxFile:
            maxFile = fileVal + 1
          let seqVal = meta.largestSeq.toUint64()
          if seqVal > maxSeq:
            maxSeq = seqVal
        version.levels[level] = files
  version.nextFileNumber = maxFile
  version.lastSeqNum = toSequence(maxSeq)
  version

proc editToJson(edit: VersionEdit): JsonNode =
  result = newJObject()
  if edit.logNumber.isSome():
    result["logNumber"] = newJsonString(edit.logNumber.get())
  if edit.prevLogNumber.isSome():
    result["prevLogNumber"] = newJsonString(edit.prevLogNumber.get())
  if edit.nextFileNumber.isSome():
    result["nextFileNumber"] = newJsonString(edit.nextFileNumber.get())
  if edit.lastSeqNum.isSome():
    result["lastSeqNum"] = newJsonString(edit.lastSeqNum.get().toUint64())
  if edit.comparatorName.isSome():
    result["comparatorName"] = newJString(edit.comparatorName.get())
  if edit.deletedFiles.len > 0:
    var arr = newJArray()
    for item in edit.deletedFiles:
      var obj = newJObject()
      obj["level"] = newJInt(item.level)
      obj["fileNum"] = newJsonString(item.fileNum.toUint64())
      arr.add(obj)
    result["deletedFiles"] = arr
  if edit.addedFiles.len > 0:
    var arr = newJArray()
    for item in edit.addedFiles:
      var obj = newJObject()
      obj["level"] = newJInt(item.level)
      obj["metadata"] = fileMetaToJson(item.metadata)
      arr.add(obj)
    result["addedFiles"] = arr

proc editFromJson(node: JsonNode): VersionEdit =
  if node.kind != JObject:
    raise newException(ManifestError, "edit 节点必须是对象")
  var edit = initVersionEdit()
  if node.hasKey("logNumber"):
    edit.setLogNumber(parseUint64(node, "logNumber"))
  if node.hasKey("prevLogNumber"):
    edit.setPrevLogNumber(parseUint64(node, "prevLogNumber"))
  if node.hasKey("nextFileNumber"):
    edit.setNextFileNumber(parseUint64(node, "nextFileNumber"))
  if node.hasKey("lastSeqNum"):
    edit.setLastSequence(toSequence(parseUint64(node, "lastSeqNum")))
  if node.hasKey("comparatorName"):
    edit.setComparatorName(node["comparatorName"].getStr())
  if node.hasKey("deletedFiles"):
    for item in node["deletedFiles"]:
      let level = Level(item["level"].getInt())
      let fileNum = toFileNumber(parseUint64(item, "fileNum"))
      edit.deleteFile(level, fileNum)
  if node.hasKey("addedFiles"):
    for item in node["addedFiles"]:
      let level = Level(item["level"].getInt())
      let metadata = fileMetaFromJson(item["metadata"])
      edit.addFile(level, metadata)
  edit

proc recordToJson*(record: ManifestRecord): JsonNode =
  result = newJObject()
  if record.formatVersion != manifestFormatV1:
    result["formatVersion"] = newJInt(ord(record.formatVersion))
  case record.kind
  of recordSnapshot:
    result["kind"] = newJString("snapshot")
    result["version"] = versionToJson(record.version)
  of recordEdit:
    result["kind"] = newJString("edit")
    result["edit"] = editToJson(record.edit)

proc recordFromJson*(node: JsonNode): ManifestRecord =
  if node.kind != JObject:
    raise newException(ManifestError, "manifest 记录必须是对象")
  var formatVersion = manifestFormatV1
  if node.hasKey("formatVersion"):
    formatVersion = ManifestFormatVersion(node["formatVersion"].getInt())
  let kind = node["kind"].getStr()
  if kind == "snapshot":
    result = ManifestRecord(formatVersion: formatVersion,
                            kind: recordSnapshot,
                            version: versionFromJson(node["version"]))
  elif kind == "edit":
    result = ManifestRecord(formatVersion: formatVersion,
                            kind: recordEdit,
                            edit: editFromJson(node["edit"]))
  else:
    raise newException(ManifestError, "未知的 manifest 记录类型: " & kind)

proc appendRecord(path: string; record: ManifestRecord) =
  ensureParentDir(path)
  let data = $recordToJson(record) & "\n"
  var file = open(path, fmAppend)
  defer: file.close()
  file.write(data)

proc overwriteWithSnapshot(path: string; version: Version; formatVersion: ManifestFormatVersion) =
  ensureParentDir(path)
  let content = $recordToJson(
    ManifestRecord(formatVersion: formatVersion,
                   kind: recordSnapshot,
                   version: version.clone())
  ) & "\n"
  writeFile(path, content)

proc manifestExists*(store: ManifestStore): bool =
  ensureStore(store)
  fileExists(store.path)

proc appendEdit*(store: ManifestStore; edit: VersionEdit) =
  ensureStore(store)
  appendRecord(store.path, ManifestRecord(formatVersion: store.formatVersion,
                                          kind: recordEdit,
                                          edit: edit))

proc writeSnapshot*(store: ManifestStore; version: Version) =
  ensureStore(store)
  overwriteWithSnapshot(store.path, version, store.formatVersion)

proc readAllRecords*(store: ManifestStore): seq[ManifestRecord] =
  ensureStore(store)
  var detectedVersion = manifestFormatV1
  var seenVersion = false
  if not store.manifestExists():
    return @[]
  let content = readFile(store.path)
  for line in content.splitLines():
    if line.len == 0:
      continue
    let node = parseJson(line)
    let record = recordFromJson(node)
    result.add(record)
    if not seenVersion:
      detectedVersion = record.formatVersion
      seenVersion = true
    elif record.formatVersion != detectedVersion:
      raise newException(ManifestError, "manifest 存在混合格式版本记录")
  store.formatVersion = detectedVersion

proc sync*(store: ManifestStore) =
  if store.isNil():
    return
  discard

proc close*(store: ManifestStore) =
  if store.isNil():
    return
  discard
