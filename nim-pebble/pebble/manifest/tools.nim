# Manifest 迁移工具集：支持格式升级/降级与兼容性校验。

import std/[json, os, strformat, strutils]

import pebble/core/types
import pebble/manifest/store
import pebble/manifest/types
import pebble/manifest/version

type
  IssueKind* = enum
    issueError,
    issueWarning

  ManifestCompatibilityIssue* = object
    kind*: IssueKind
    message*: string

  ManifestCompatibilityReport* = object
    compatible*: bool
    detectedFormat*: ManifestFormatVersion
    targetFormat*: ManifestFormatVersion
    issues*: seq[ManifestCompatibilityIssue]

proc manifestFormatName*(format: ManifestFormatVersion): string =
  case format
  of manifestFormatV1: "v1"
  of manifestFormatV2: "v2"

proc addIssue(report: var ManifestCompatibilityReport; kind: IssueKind; message: string) =
  report.compatible = report.compatible and kind == issueWarning
  report.issues.add(ManifestCompatibilityIssue(kind: kind, message: message))

proc detectManifestFormat*(manifestPath: string): ManifestFormatVersion =
  if not fileExists(manifestPath):
    return manifestFormatV1
  let content = readFile(manifestPath)
  for line in content.splitLines():
    if line.len == 0:
      continue
    try:
      let node = parseJson(line)
      if node.kind == JObject and node.hasKey("formatVersion"):
        return ManifestFormatVersion(node["formatVersion"].getInt())
      return manifestFormatV1
    except CatchableError:
      return manifestFormatV1
  manifestFormatV1

proc verifyLevels(version: Version; report: var ManifestCompatibilityReport) =
  for entry in version.levelEntries():
    if entry.level <= 0:
      continue
    var prevLargest = ""
    var first = true
    for meta in entry.files:
      let smallest = meta.smallest.toBytes()
      if not first:
        if prevLargest >= smallest:
          report.addIssue(issueError,
            fmt"Level {entry.level} 文件区间重叠: {meta.describe()}")
          break
      prevLargest = meta.largest.toBytes()
      first = false

proc checkManifestCompatibility*(manifestPath: string;
                                 targetFormat: ManifestFormatVersion): ManifestCompatibilityReport =
  var report = ManifestCompatibilityReport(
    compatible: true,
    detectedFormat: manifestFormatV1,
    targetFormat: targetFormat,
    issues: @[]
  )
  var records: seq[ManifestRecord]
  try:
    let store = initManifestStore(manifestPath)
    records = store.readAllRecords()
    report.detectedFormat = store.formatVersion
  except CatchableError as err:
    report.compatible = false
    report.issues.add(ManifestCompatibilityIssue(kind: issueError, message: err.msg))
    return report

  if records.len == 0:
    report.addIssue(issueError, "manifest 文件为空，缺少 snapshot 记录")
    return report

  var current: Version = nil
  var seenSnapshot = false
  try:
    for record in records:
      case record.kind
      of recordSnapshot:
        current = record.version.clone()
        seenSnapshot = true
      of recordEdit:
        if current.isNil():
          raise newException(ManifestError, "回放 edit 前缺少 snapshot")
        current = current.applyEdit(record.edit)
    if not seenSnapshot:
      raise newException(ManifestError, "manifest 未包含 snapshot 记录")
  except CatchableError as err:
    report.addIssue(issueError, err.msg)
    return report

  if targetFormat == manifestFormatV1 and report.detectedFormat != manifestFormatV1:
    report.addIssue(issueError,
      fmt"manifest 使用 {manifestFormatName(report.detectedFormat)} 格式，无法降级到 {manifestFormatName(targetFormat)}")

  if report.compatible and not current.isNil():
    verifyLevels(current, report)

  report

proc rewriteManifest(records: seq[ManifestRecord];
                     outputPath: string;
                     targetFormat: ManifestFormatVersion) =
  var outStore = initManifestStore(outputPath, targetFormat)
  var wroteSnapshot = false
  for record in records:
    case record.kind
    of recordSnapshot:
      outStore.writeSnapshot(record.version)
      wroteSnapshot = true
    of recordEdit:
      if not wroteSnapshot:
        raise newException(ManifestError, "manifest 缺少 snapshot，无法重写")
      outStore.appendEdit(record.edit)
  outStore.sync()
  outStore.close()

proc convertManifestFormat*(inputPath, outputPath: string;
                            targetFormat: ManifestFormatVersion): ManifestCompatibilityReport =
  var report = checkManifestCompatibility(inputPath, targetFormat)
  if not report.compatible:
    return report
  let inputStore = initManifestStore(inputPath)
  let records = inputStore.readAllRecords()
  try:
    rewriteManifest(records, outputPath, targetFormat)
  except CatchableError as err:
    report.addIssue(issueError, err.msg)
  report

proc upgradeManifest*(inputPath, outputPath: string): ManifestCompatibilityReport =
  convertManifestFormat(inputPath, outputPath, manifestFormatV2)

proc downgradeManifest*(inputPath, outputPath: string): ManifestCompatibilityReport =
  convertManifestFormat(inputPath, outputPath, manifestFormatV1)
