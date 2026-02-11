import std/[json, options, os, strutils]

import pebble/core/types as pebble_core
import pebble/kv

type
  DumpOptions* = object
    dataPath*: string
    space*: KeySpace
    prefix*: string
    startAfter*: string
    limit*: int

  DumpResult* = object
    entries*: seq[KVPair]
    hasMore*: bool
    nextCursor*: Option[string]
    snapshotSeq*: uint64

  InspectOptions* = object
    dataPath*: string
    includeCounts*: bool

  SpaceCount* = object
    space*: KeySpace
    count*: int

  InspectResult* = object
    config*: PebbleKVConfig
    stats*: PebbleKVStats
    counts*: seq[SpaceCount]

  BackupOptions* = object
    dataPath*: string
    outputPath*: string
    overwrite*: bool

  BackupResult* = object
    filesCopied*: int
    directoriesCreated*: int

  RepairOptions* = object
    dataPath*: string
    logPath*: string
    dedupe*: bool
    dryRun*: bool

  RepairResult* = object
    appliedOps*: int
    skippedOps*: int
    sequence*: uint64
    dryRun*: bool

  HealthOptions* = object
    dataPath*: string
    sampleLimit*: int
    includeCounts*: bool

  HealthResult* = object
    ok*: bool
    warnings*: seq[string]
    stats*: PebbleKVStats
    counts*: seq[SpaceCount]
    orphanIndexes*: seq[string]

const
  DefaultSampleLimit* = 10

proc sanitizeFragment(value: string): string =
  if value.len == 0:
    return ""
  var buf = newSeq[char]()
  for ch in value:
    if ch.isAlphaNumeric() or ch in {'_', '-', '.'}:
      buf.add(ch)
    else:
      buf.add('_')
  buf.join("")

proc openPebble(path: string): PebbleKV =
  let normalized = if path.len == 0: "pebble-data" else: path
  newPebbleKV(normalized)

proc runDump*(opts: DumpOptions): DumpResult =
  let kv = openPebble(opts.dataPath)
  defer: kv.close()
  let page = kv.scanPrefix(opts.space, opts.prefix, opts.startAfter, opts.limit)
  result.entries = page.entries
  result.hasMore = page.hasMore
  result.nextCursor = page.nextCursor
  result.snapshotSeq = pebble_core.toUint64(page.snapshot.seq)

proc runInspect*(opts: InspectOptions): InspectResult =
  let kv = openPebble(opts.dataPath)
  defer: kv.close()
  result.config = kv.config()
  result.stats = kv.stats()
  if opts.includeCounts:
    for space in KeySpace:
      let page = kv.scanPrefix(space, "", "", 0)
      result.counts.add(SpaceCount(space: space, count: page.entries.len))

proc ensureDestination(path: string; overwrite: bool) =
  if dirExists(path):
    if not overwrite:
      raise newException(IOError, "destination already exists: " & path)
    removeDir(path)
  createDir(path)

proc copyTree(source, dest: string; counter: var BackupResult) =
  for kind, relPath in walkDir(source, relative = true):
    let srcPath = source / relPath
    let dstPath = dest / relPath
    case kind
    of pcDir:
      if not dirExists(dstPath):
        createDir(dstPath)
        inc counter.directoriesCreated
    of pcFile:
      copyFile(srcPath, dstPath)
      inc counter.filesCopied
    else:
      discard

proc runBackup*(opts: BackupOptions): BackupResult =
  if opts.dataPath.len == 0:
    raise newException(IOError, "data path must be provided")
  if not dirExists(opts.dataPath):
    raise newException(IOError, "data path does not exist: " & opts.dataPath)
  ensureDestination(opts.outputPath, opts.overwrite)
  copyTree(opts.dataPath, opts.outputPath, result)

proc parseKeySpace*(value: string): KeySpace =
  let lowered = value.toLowerAscii()
  case lowered
  of "event", "events":
    ksEvent
  of "sig", "signature", "signatures":
    ksSignature
  of "task", "tasks":
    ksTask
  of "index", "idx":
    ksIndex
  of "custom":
    ksCustom
  else:
    raise newException(ValueError, "unknown key space: " & value)

proc parseKind(value: string): KVWriteKind =
  let lowered = value.toLowerAscii()
  case lowered
  of "put", "set", "insert", "upsert":
    kvPut
  of "del", "delete", "remove":
    kvDelete
  else:
    raise newException(ValueError, "unknown write kind: " & value)

proc nodeToWrite(node: JsonNode): KVWrite =
  if node.kind != JObject:
    raise newException(ValueError, "repair log entries must be JSON objects")
  if not node.hasKey("kind"):
    raise newException(ValueError, "repair log entry missing 'kind'")
  if not node.hasKey("space"):
    raise newException(ValueError, "repair log entry missing 'space'")
  let space = parseKeySpace(node["space"].getStr())
  let kind = parseKind(node["kind"].getStr())
  let key = if node.hasKey("key"): node["key"].getStr() else: ""
  let value = if node.hasKey("value"): node["value"].getStr() else: ""
  case kind
  of kvPut:
    KVWrite(kind: kind, space: space, key: key, value: value)
  of kvDelete:
    KVWrite(kind: kind, space: space, key: key, value: "")

proc loadRepairLog(path: string): seq[KVWrite] =
  if path.len == 0:
    raise newException(ValueError, "repair log path must be provided")
  if not path.fileExists():
    raise newException(IOError, "repair log file not found: " & path)
  let content = readFile(path)
  let trimmed = content.strip()
  if trimmed.len == 0:
    return @[]
  if trimmed[0] == '[':
    let node = parseJson(trimmed)
    if node.kind != JArray:
      raise newException(ValueError, "repair log array is malformed")
    for child in node.items():
      result.add(nodeToWrite(child))
  else:
    for line in trimmed.splitLines():
      let raw = line.strip()
      if raw.len == 0:
        continue
      let node = parseJson(raw)
      result.add(nodeToWrite(node))

proc runRepair*(opts: RepairOptions): RepairResult =
  let writes = loadRepairLog(opts.logPath)
  if writes.len == 0:
    result.dryRun = opts.dryRun
    return result
  if opts.dryRun:
    result.dryRun = true
    result.skippedOps = 0
    result.appliedOps = writes.len
    return result
  let kv = openPebble(opts.dataPath)
  defer: kv.close()
  let seq = kv.replayWrites(writes, dedupe = opts.dedupe)
  result.sequence = pebble_core.toUint64(seq)
  if pebble_core.toUint64(seq) == 0:
    result.skippedOps = writes.len
  else:
    result.appliedOps = writes.len

proc countSpace(kv: PebbleKV; space: KeySpace): int =
  let page = kv.scanPrefix(space, "", "", 0)
  page.entries.len

proc verifyIndex(kv: PebbleKV;
                 sampleLimit: int;
                 warnings: var seq[string];
                 orphans: var seq[string]) =
  let limit = if sampleLimit <= 0: DefaultSampleLimit else: sampleLimit
  let page = kv.scanPrefix(ksIndex, "", "", limit)
  for entry in page.entries:
    if entry.id.len == 0:
      warnings.add("index entry with empty key observed")
      continue
    if entry.value.len == 0:
      continue
    let target = entry.value
    var hasOwner = false
    for space in [ksEvent, ksSignature, ksTask]:
      if kv.get(space, target).isSome():
        hasOwner = true
        break
    if not hasOwner:
      warnings.add("orphan index entry: " & entry.id)
      orphans.add(entry.id)

proc runHealth*(opts: HealthOptions): HealthResult =
  if opts.dataPath.len == 0:
    raise newException(IOError, "data path must be provided")
  if not dirExists(opts.dataPath):
    raise newException(IOError, "data path does not exist: " & opts.dataPath)
  let kv = openPebble(opts.dataPath)
  defer: kv.close()
  result.stats = kv.stats()
  let limit = if opts.sampleLimit <= 0: DefaultSampleLimit else: opts.sampleLimit
  for space in KeySpace:
    let page = kv.scanPrefix(space, "", "", limit)
    if opts.includeCounts:
      result.counts.add(SpaceCount(space: space, count: page.entries.len))
    for entry in page.entries:
      if entry.id.len == 0:
        result.warnings.add("empty key detected in " & $space)
      elif entry.id != sanitizeFragment(entry.id):
        result.warnings.add("non-normalized key '" & entry.id & "' in " & $space)
  verifyIndex(kv, limit, result.warnings, result.orphanIndexes)
  if result.stats.mem.hardLimitBytes > 0 and
     result.stats.mem.approxBytes > result.stats.mem.hardLimitBytes:
    result.warnings.add("memtable usage exceeds configured hard limit")
  result.ok = result.warnings.len == 0
