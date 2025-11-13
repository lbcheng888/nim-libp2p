import std/[os, strutils, times, unittest]

import pebble/kv
import pebble/runtime/resource_manager
import pebble/runtime/executor
import pebble/compaction/planner
import pebble/compaction/scheduler
import pebble/compaction/types

proc makeTempDir(): string =
  var attempt = 0
  while true:
    let stamp = int64(epochTime() * 1_000_000.0)
    let candidate = getTempDir() / ("pebblekv_res_" & $stamp & "_" & $getCurrentProcessId() & "_" & $attempt)
    if not dirExists(candidate):
      createDir(candidate)
      return candidate
    inc attempt

suite "PebbleKV resource & compaction integration":
  test "resource limits reflected in stats and config snapshot":
    var cfg = defaultPebbleKVConfig()
    cfg.dataPath = makeTempDir()
    cfg.resource.memSoftLimitBytes = 64 * 1024
    cfg.resource.memHardLimitBytes = 128 * 1024
    cfg.resource.throttleEnabled = true
    cfg.resource.throttleBaseDelayMs = 2
    cfg.resource.throttleMaxDelayMs = 10
    cfg.wal.autoSyncBytes = int64(4 * 1024)
    let kv = newPebbleKV(cfg)
    try:
      for i in 0 ..< 128:
        kv.put(ksEvent, "evt-" & $i, "payload-" & $i)
      let stats = kv.stats()
      check stats.mem.softLimitBytes == cfg.resource.memSoftLimitBytes
      check stats.mem.hardLimitBytes == cfg.resource.memHardLimitBytes
      check stats.mem.approxBytes > 0
      check stats.resourceUtilization > 0.0
      check stats.wal.segmentPath.len > 0
      let liveCfg = kv.config()
      check liveCfg.resource.memSoftLimitBytes == cfg.resource.memSoftLimitBytes
      check liveCfg.wal.autoSyncBytes == cfg.wal.autoSyncBytes
    finally:
      kv.close()
      removeDir(cfg.dataPath)

  test "compaction scheduler attachment surfaces in stats":
    var cfg = defaultPebbleKVConfig()
    cfg.dataPath = makeTempDir()
    let kv = newPebbleKV(cfg)
    try:
      let plannerCfg = CompactionPlannerConfig(
        l0CompactionTrigger: 4,
        maxCompactionBytes: 1_048_576,
        minCompactionBytes: 0,
        maxOutputFileBytes: 0,
        levelSizeTargets: @[],
        grandparentOverlapSoftLimit: 0
      )
      let planner = initCompactionPlanner(plannerCfg)
      let exec = initExecutor(name = "compaction-test", mode = execSynchronous, maxWorkers = 1)
      let handler: CompactionJobHandler = proc (plan: CompactionPlan): CompactionResult =
        CompactionResult(bytesRead: 0, bytesWritten: 0, readAmp: 0.0, durationMs: 0, outputFiles: @[])
      let schedCfg = CompactionSchedulerConfig(
        maxConcurrentJobs: 1,
        acquireAmount: 0,
        eventBufferLimit: 16,
        executor: exec,
        resourceManager: nil,
        resourceKind: resource_manager.resCompactionTokens
      )
      let scheduler = initCompactionScheduler(planner, handler, schedCfg)
      kv.setCompactionScheduler(scheduler)
      let stats = kv.stats()
      check stats.compaction.hasScheduler
      check stats.compaction.activeJobs == 0
      check stats.compaction.pendingJobs == 0
      var handles: seq[TaskHandle] = @[]
      exec.shutdown(handles)
    finally:
      kv.close()
      removeDir(cfg.dataPath)
