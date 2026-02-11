import std/[formatfloat, json, options, os, strformat, strutils]

import pebble/kv
import pebble/tooling/cli_core
import pebble/tooling/kv_ops

type
  DumpCliOptions = object
    dataPath: string
    space: KeySpace
    prefix: string
    startAfter: string
    limit: int
    hexOutput: bool

  InspectCliOptions = object
    dataPath: string
    includeCounts: bool
    outputJson: bool

  BackupCliOptions = object
    dataPath: string
    outputPath: string
    overwrite: bool

  RepairCliOptions = object
    dataPath: string
    logPath: string
    dedupe: bool
    dryRun: bool

  HealthCliOptions = object
    dataPath: string
    sampleLimit: int
    verbose: bool

proc renderSpace(space: KeySpace): string =
  case space
  of ksEvent: "event"
  of ksSignature: "signature"
  of ksTask: "task"
  of ksIndex: "index"
  of ksCustom: "custom"

proc parseKeySpaceArg(value: string): KeySpace =
  parseKeySpace(value)

proc parseDumpArgs(args: seq[string]): DumpCliOptions =
  result.space = ksEvent
  result.limit = 0
  for arg in args:
    if arg.startsWith("--path="):
      result.dataPath = arg.split("=", 1)[1]
    elif arg.startsWith("--space="):
      result.space = parseKeySpaceArg(arg.split("=", 1)[1])
    elif arg.startsWith("--prefix="):
      result.prefix = arg.split("=", 1)[1]
    elif arg.startsWith("--start-after="):
      result.startAfter = arg.split("=", 1)[1]
    elif arg.startsWith("--limit="):
      let raw = arg.split("=", 1)[1]
      try:
        result.limit = raw.parseInt()
      except ValueError:
        raiseCliError("invalid --limit value: " & raw)
      if result.limit < 0:
        raiseCliError("--limit must be non-negative")
    elif arg == "--hex":
      result.hexOutput = true
    elif arg in ["-h", "--help"]:
      raiseCliError("")
    elif arg.startsWith("--"):
      raiseCliError("unknown flag: " & arg)
    else:
      raiseCliError("unexpected argument: " & arg)

proc toHexString(data: string): string =
  if data.len == 0:
    return ""
  var parts = newSeq[string]()
  for ch in data:
    parts.add(fmt"{ord(ch):02x}")
  parts.join("")

proc renderDump(out: DumpResult; opts: DumpCliOptions) =
  echo "Snapshot sequence: ", out.snapshotSeq
  if out.entries.len == 0:
    echo "No entries found."
    if out.hasMore:
      if out.nextCursor.isSome():
        echo "More data available; rerun with --start-after=", out.nextCursor.get()
    return
  for entry in out.entries:
    let value = if opts.hexOutput: toHexString(entry.value) else: entry.value.escape()
    echo "  ", entry.id, " -> ", value
  if out.hasMore:
    if out.nextCursor.isSome():
      echo "More entries available; next cursor: ", out.nextCursor.get()
    else:
      echo "More entries available."

proc dumpHandler(ctx: CliContext; args: seq[string]) =
  let opts =
    try:
      parseDumpArgs(args)
    except CliError as err:
      if err.msg.len == 0:
        let prefix = (ctx.commandPath & @["kv", "dump"]).join(" ")
        echo "Usage:"
        echo "  ", prefix, " [--path=<dir>] [--space=<event|signature|task|index|custom>]",
             " [--prefix=<prefix>] [--start-after=<cursor>] [--limit=N] [--hex]"
        echo "Dump key/value pairs from Pebble KV storage."
        return
      raise err
  let result =
    try:
      runDump(DumpOptions(
        dataPath: opts.dataPath,
        space: opts.space,
        prefix: opts.prefix,
        startAfter: opts.startAfter,
        limit: opts.limit
      ))
    except CatchableError as err:
      raiseCliError("failed to dump Pebble KV: " & err.msg)
  renderDump(result, opts)

proc parseInspectArgs(args: seq[string]): InspectCliOptions =
  for arg in args:
    if arg.startsWith("--path="):
      result.dataPath = arg.split("=", 1)[1]
    elif arg == "--counts":
      result.includeCounts = true
    elif arg == "--json":
      result.outputJson = true
    elif arg in ["-h", "--help"]:
      raiseCliError("")
    elif arg.startsWith("--"):
      raiseCliError("unknown flag: " & arg)
    else:
      raiseCliError("unexpected argument: " & arg)

proc renderInspect(result: InspectResult; opts: InspectCliOptions) =
  if opts.outputJson:
    var node = %*{
      "config": {
        "dataPath": result.config.dataPath,
        "maxConcurrentCommits": result.config.maxConcurrentCommits
      },
      "stats": {
        "mem": {
          "approxBytes": result.stats.mem.approxBytes,
          "softLimitBytes": result.stats.mem.softLimitBytes,
          "hardLimitBytes": result.stats.mem.hardLimitBytes,
          "utilization": result.stats.mem.utilization
        },
        "wal": {
          "segmentIndex": result.stats.wal.segmentIndex,
          "segmentPath": result.stats.wal.segmentPath,
          "segmentBytes": result.stats.wal.segmentBytes,
          "totalBytes": result.stats.wal.totalBytes,
          "pendingAutoSyncs": result.stats.wal.pendingAutoSyncs,
          "bytesSinceSync": result.stats.wal.bytesSinceSync
        },
        "compaction": {
          "hasScheduler": result.stats.compaction.hasScheduler,
          "activeJobs": result.stats.compaction.activeJobs,
          "pendingJobs": result.stats.compaction.pendingJobs,
          "metrics": {
            "totalJobs": result.stats.compaction.metrics.totalJobs,
            "bytesRead": result.stats.compaction.metrics.bytesRead,
            "bytesWritten": result.stats.compaction.metrics.bytesWritten,
            "lastWriteAmp": result.stats.compaction.metrics.lastWriteAmp,
            "maxWriteAmp": result.stats.compaction.metrics.maxWriteAmp,
            "avgWriteAmp": result.stats.compaction.metrics.avgWriteAmp,
            "readAmpSamples": result.stats.compaction.metrics.readAmpSamples
          }
        },
        "resourceUtilization": result.stats.resourceUtilization
      }
    }
    if opts.includeCounts:
      var countsNode = newJArray()
      for item in result.counts:
        countsNode.add(%*{
          "space": renderSpace(item.space),
          "count": item.count
        })
      node["counts"] = countsNode
    echo node.pretty()
    return
  echo "Pebble KV config:"
  echo "  data path: ", result.config.dataPath
  echo "  max concurrent commits: ", result.config.maxConcurrentCommits
  echo "Resource utilization: ",
       formatFloat(result.stats.resourceUtilization * 100.0, ffDecimal, 2), "%"
  echo "MemTable:"
  echo "  approx bytes: ", result.stats.mem.approxBytes
  echo "  limits (soft/hard): ", result.stats.mem.softLimitBytes, " / ",
       result.stats.mem.hardLimitBytes
  echo "  utilization: ", formatFloat(result.stats.mem.utilization * 100.0, ffDecimal, 2), "%"
  echo "WAL:"
  echo "  active segment: ", result.stats.wal.segmentPath
  echo "  segment bytes: ", result.stats.wal.segmentBytes
  echo "  total bytes: ", result.stats.wal.totalBytes
  echo "  pending auto syncs: ", result.stats.wal.pendingAutoSyncs
  echo "Compaction:"
  echo "  scheduler: ", (if result.stats.compaction.hasScheduler: "enabled" else: "disabled")
  echo "  active jobs: ", result.stats.compaction.activeJobs
  echo "  pending jobs: ", result.stats.compaction.pendingJobs
  if result.stats.compaction.metrics.totalJobs > 0:
    echo "  total jobs: ", result.stats.compaction.metrics.totalJobs
    echo "  bytes read/written: ", result.stats.compaction.metrics.bytesRead,
         " / ", result.stats.compaction.metrics.bytesWritten
    echo "  write amp (last/max/avg): ",
         formatFloat(result.stats.compaction.metrics.lastWriteAmp, ffDecimal, 2), " / ",
         formatFloat(result.stats.compaction.metrics.maxWriteAmp, ffDecimal, 2), " / ",
         formatFloat(result.stats.compaction.metrics.avgWriteAmp, ffDecimal, 2)
  if opts.includeCounts and result.counts.len > 0:
    echo "Key space counts:"
    for item in result.counts:
      echo "  ", renderSpace(item.space), ": ", item.count

proc inspectHandler(ctx: CliContext; args: seq[string]) =
  let opts =
    try:
      parseInspectArgs(args)
    except CliError as err:
      if err.msg.len == 0:
        let prefix = (ctx.commandPath & @["kv", "inspect"]).join(" ")
        echo "Usage:"
        echo "  ", prefix, " [--path=<dir>] [--counts] [--json]"
        echo "Inspect Pebble KV configuration and runtime stats."
        return
      raise err
  let result =
    try:
      runInspect(InspectOptions(
        dataPath: opts.dataPath,
        includeCounts: opts.includeCounts
      ))
    except CatchableError as err:
      raiseCliError("failed to inspect Pebble KV: " & err.msg)
  renderInspect(result, opts)

proc parseBackupArgs(args: seq[string]): BackupCliOptions =
  for arg in args:
    if arg.startsWith("--path="):
      result.dataPath = arg.split("=", 1)[1]
    elif arg.startsWith("--output="):
      result.outputPath = arg.split("=", 1)[1]
    elif arg == "--overwrite":
      result.overwrite = true
    elif arg in ["-h", "--help"]:
      raiseCliError("")
    elif arg.startsWith("--"):
      raiseCliError("unknown flag: " & arg)
    else:
      raiseCliError("unexpected argument: " & arg)
  if result.outputPath.len == 0:
    raiseCliError("missing required flag: --output=<dir>")

proc backupHandler(ctx: CliContext; args: seq[string]) =
  let opts =
    try:
      parseBackupArgs(args)
    except CliError as err:
      if err.msg.len == 0:
        let prefix = (ctx.commandPath & @["kv", "backup"]).join(" ")
        echo "Usage:"
        echo "  ", prefix, " --output=<dir> [--path=<dir>] [--overwrite]"
        echo "Create a snapshot-style backup of the Pebble KV directory."
        return
      raise err
  let result =
    try:
      runBackup(BackupOptions(
        dataPath: opts.dataPath,
        outputPath: opts.outputPath,
        overwrite: opts.overwrite
      ))
    except CatchableError as err:
      raiseCliError("failed to backup Pebble KV: " & err.msg)
  echo "Backup completed:"
  echo "  directories created: ", result.directoriesCreated
  echo "  files copied: ", result.filesCopied
  echo "  destination: ", opts.outputPath

proc parseRepairArgs(args: seq[string]): RepairCliOptions =
  result.dedupe = true
  for arg in args:
    if arg.startsWith("--path="):
      result.dataPath = arg.split("=", 1)[1]
    elif arg.startsWith("--log="):
      result.logPath = arg.split("=", 1)[1]
    elif arg == "--no-dedupe":
      result.dedupe = false
    elif arg == "--dry-run":
      result.dryRun = true
    elif arg in ["-h", "--help"]:
      raiseCliError("")
    elif arg.startsWith("--"):
      raiseCliError("unknown flag: " & arg)
    else:
      raiseCliError("unexpected argument: " & arg)
  if result.logPath.len == 0:
    raiseCliError("missing required flag: --log=<file>")

proc repairHandler(ctx: CliContext; args: seq[string]) =
  let opts =
    try:
      parseRepairArgs(args)
    except CliError as err:
      if err.msg.len == 0:
        let prefix = (ctx.commandPath & @["kv", "repair"]).join(" ")
        echo "Usage:"
        echo "  ", prefix, " --log=<file> [--path=<dir>] [--dry-run] [--no-dedupe]"
        echo "Replay KVWrite logs against Pebble KV storage."
        return
      raise err
  let result =
    try:
      runRepair(RepairOptions(
        dataPath: opts.dataPath,
        logPath: opts.logPath,
        dedupe: opts.dedupe,
        dryRun: opts.dryRun
      ))
    except CatchableError as err:
      raiseCliError("failed to replay repair log: " & err.msg)
  if result.dryRun:
    echo "Dry-run parsed ", result.appliedOps, " operations."
    return
  if result.sequence == 0:
    echo "Replay completed: no changes applied (possibly deduped)."
  else:
    echo "Replay applied ", result.appliedOps, " operations."
    echo "Committed at sequence ", result.sequence

proc parseHealthArgs(args: seq[string]): HealthCliOptions =
  result.sampleLimit = DefaultSampleLimit
  for arg in args:
    if arg.startsWith("--path="):
      result.dataPath = arg.split("=", 1)[1]
    elif arg.startsWith("--limit="):
      let raw = arg.split("=", 1)[1]
      try:
        result.sampleLimit = raw.parseInt()
      except ValueError:
        raiseCliError("invalid --limit value: " & raw)
      if result.sampleLimit < 0:
        raiseCliError("--limit must be non-negative")
    elif arg == "--verbose":
      result.verbose = true
    elif arg in ["-h", "--help"]:
      raiseCliError("")
    elif arg.startsWith("--"):
      raiseCliError("unknown flag: " & arg)
    else:
      raiseCliError("unexpected argument: " & arg)

proc renderHealth(result: HealthResult; opts: HealthCliOptions) =
  if result.ok:
    echo "Health check: OK"
  else:
    echo "Health check: WARN"
  if opts.verbose or result.warnings.len > 0:
    if result.warnings.len == 0:
      echo "Warnings: none"
    else:
      echo "Warnings:"
      for warn in result.warnings:
        echo "  ", warn
  echo "Mem usage: ", result.stats.mem.approxBytes, " / ",
       result.stats.mem.hardLimitBytes, " (approx / hard limit)"
  if result.orphanIndexes.len > 0:
    echo "Orphan index keys:"
    for key in result.orphanIndexes:
      echo "  ", key
  if opts.verbose and result.counts.len > 0:
    echo "Sample counts:"
    for item in result.counts:
      echo "  ", renderSpace(item.space), ": ", item.count

proc healthHandler(ctx: CliContext; args: seq[string]) =
  let opts =
    try:
      parseHealthArgs(args)
    except CliError as err:
      if err.msg.len == 0:
        let prefix = (ctx.commandPath & @["kv", "health"]).join(" ")
        echo "Usage:"
        echo "  ", prefix, " [--path=<dir>] [--limit=N] [--verbose]"
        echo "Run Pebble KV health checks and report warnings."
        return
      raise err
  let result =
    try:
      runHealth(HealthOptions(
        dataPath: opts.dataPath,
        sampleLimit: opts.sampleLimit,
        includeCounts: opts.verbose
      ))
    except CatchableError as err:
      raiseCliError("failed to run health check: " & err.msg)
  renderHealth(result, opts)

proc registerKvCommands*(root: CliCommand) =
  let group = newCliCommand(
    name = "kv",
    summary = "Pebble KV maintenance toolkit"
  )
  let dumpCmd = newCliCommand("dump", "Stream key/value pairs from PebbleKV", dumpHandler)
  let inspectCmd = newCliCommand("inspect", "Inspect configuration and runtime stats", inspectHandler)
  let backupCmd = newCliCommand("backup", "Backup the Pebble KV directory", backupHandler)
  let repairCmd = newCliCommand("repair", "Replay KVWrite logs into Pebble KV", repairHandler)
  let healthCmd = newCliCommand("health", "Perform basic health checks", healthHandler)
  group.addSubcommand(dumpCmd)
  group.addSubcommand(inspectCmd)
  group.addSubcommand(backupCmd)
  group.addSubcommand(repairCmd)
  group.addSubcommand(healthCmd)
  root.addSubcommand(group)
