import std/[json, options, os, sequtils, strutils, times, unittest]

import pebble/core/types as pebble_core
import pebble/kv
import pebble/tooling/kv_ops

proc makeTempDir(): string =
  var attempt = 0
  while true:
    let stamp = int64(epochTime() * 1_000_000.0)
    let candidate = getTempDir() / ("pebble_cli_ops_" & $stamp & "_" &
      $getCurrentProcessId() & "_" & $attempt)
    if not dirExists(candidate):
      createDir(candidate)
      return candidate
    inc attempt

suite "Pebble CLI operations":
  test "runDump returns inserted entries":
    let root = makeTempDir()
    var kv = newPebbleKV(root)
    try:
      kv.put(ksEvent, "evt-1", "value-1")
      kv.put(ksEvent, "evt-2", "value-2")
      kv.close()
      kv = nil
      let result = runDump(DumpOptions(
        dataPath: root,
        space: ksEvent,
        prefix: "",
        startAfter: "",
        limit: 0
      ))
      check result.entries.len == 2
      let ids = result.entries.mapIt(it.id)
      check ids.contains("evt-1")
      check ids.contains("evt-2")
    finally:
      if not kv.isNil():
        kv.close()
      removeDir(root)

  test "runInspect reports stats and counts":
    let root = makeTempDir()
    var kv = newPebbleKV(root)
    try:
      kv.put(ksEvent, "evt-stat", "value")
      kv.put(ksIndex, "pending:evt-stat", "evt-stat")
      kv.close()
      kv = nil
      let result = runInspect(InspectOptions(
        dataPath: root,
        includeCounts: true
      ))
      check result.config.dataPath == root
      let eventCount = result.counts.filterIt(it.space == ksEvent)
      check eventCount.len == 1
      check eventCount[0].count >= 1
      check result.stats.mem.hardLimitBytes >= 0
      check result.stats.resourceUtilization >= 0.0
      check result.stats.compaction.activeJobs >= 0
    finally:
      if not kv.isNil():
        kv.close()
      removeDir(root)

  test "runBackup copies data and allows reopen":
    let root = makeTempDir()
    let backupDir = makeTempDir()
    let kv = newPebbleKV(root)
    try:
      kv.put(ksTask, "task-1", "payload-1")
    finally:
      kv.close()
    removeDir(backupDir)
    let result = runBackup(BackupOptions(
      dataPath: root,
      outputPath: backupDir,
      overwrite: false
    ))
    check result.filesCopied > 0
    check dirExists(backupDir)
    let reopened = newPebbleKV(backupDir)
    try:
      let task = reopened.get(ksTask, "task-1")
      check task.isSome()
      check task.get() == "payload-1"
    finally:
      reopened.close()
      removeDir(backupDir)
      removeDir(root)

  test "runRepair replays KVWrite logs and dedupe skips second pass":
    let root = makeTempDir()
    let logPath = root / "repair_log.json"
    let entries = %*[
      {"kind": "put", "space": "event", "key": "evt-repair", "value": "payload"},
      {"kind": "put", "space": "index", "key": "pending:evt-repair", "value": "evt-repair"}
    ]
    writeFile(logPath, entries.pretty())
    let dry = runRepair(RepairOptions(
      dataPath: root,
      logPath: logPath,
      dedupe: true,
      dryRun: true
    ))
    check dry.dryRun
    check dry.appliedOps == 2
    let applied = runRepair(RepairOptions(
      dataPath: root,
      logPath: logPath,
      dedupe: true,
      dryRun: false
    ))
    check applied.sequence > 0
    let kv = newPebbleKV(root)
    try:
      let evt = kv.get(ksEvent, "evt-repair")
      check evt.isSome()
      check evt.get() == "payload"
      let idx = kv.get(ksIndex, "pending:evt-repair")
      check idx.isSome()
      check idx.get() == "evt-repair"
    finally:
      kv.close()
    let deduped = runRepair(RepairOptions(
      dataPath: root,
      logPath: logPath,
      dedupe: true,
      dryRun: false
    ))
    check deduped.sequence == 0
    check deduped.skippedOps == 2
    removeDir(root)

  test "runHealth detects orphan index entries":
    let root = makeTempDir()
    var kv = newPebbleKV(root)
    try:
      kv.applyBatch([
        withPut(ksIndex, "pending_missing", "missing-event")
      ])
      kv.close()
      kv = nil
      let result = runHealth(HealthOptions(
        dataPath: root,
        sampleLimit: 5,
        includeCounts: true
      ))
      check not result.ok
      check result.orphanIndexes.contains("pending_missing")
      check result.warnings.len > 0
    finally:
      if not kv.isNil():
        kv.close()
      removeDir(root)
