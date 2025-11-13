# 通用类型定义：服务于压实调度（Compaction）子系统。
# 覆盖调度计划、作业指标、事件与策略配置等结构。

import std/[json, options, sequtils, strformat, times]

import pebble/core/types
import pebble/manifest/types

type
  CompactionError* = object of CatchableError

  CompactionPriority* = enum
    priorityLow,
    priorityNormal,
    priorityHigh,
    priorityCritical

  LevelInput* = object
    ## 同一层级的输入文件集合。
    level*: Level
    files*: seq[FileMetadata]
    bytes*: int64

  OutputShard* = object
    ## 输出文件分片的期望尺寸及边界。
    index*: int
    targetBytes*: int64
    smallest*: Option[Key]
    largest*: Option[Key]

  CompactionPlan* = object
    ## 单次压实作业的完整计划。
    id*: uint64
    strategy*: string
    inputs*: seq[LevelInput]
    outputLevel*: Level
    estimatedBytes*: int64
    priority*: CompactionPriority
    score*: float
    createdAt*: DateTime
    outputShards*: seq[OutputShard]

  CompactionPlannerConfig* = object
    ## Planner 行为配置，覆盖触发阈值与容量约束。
    l0CompactionTrigger*: int
    maxCompactionBytes*: int64
    minCompactionBytes*: int64
    maxOutputFileBytes*: int64
    levelSizeTargets*: seq[int64]
    grandparentOverlapSoftLimit*: int64

  CompactionStrategyName* = string

  CompactionTelemetry* = object
    ## 调试信息：记录各层评分及被选中的策略。
    levelScores*: seq[tuple[level: Level, score: float]]
    selectedStrategy*: Option[string]
    reason*: Option[string]

  CompactionResult* = object
    ## 作业执行结果，用于指标统计与事件输出。
    bytesRead*: int64
    bytesWritten*: int64
    readAmp*: float
    durationMs*: int
    outputFiles*: seq[FileMetadata]

  CompactionMetrics* = object
    totalJobs*: int
    bytesRead*: int64
    bytesWritten*: int64
    lastWriteAmp*: float
    maxWriteAmp*: float
    avgWriteAmp*: float
    readAmpSamples*: seq[float]

  CompactionEventKind* = enum
    compactionStart,
    compactionFinish,
    compactionFailure

  CompactionEvent* = object
    kind*: CompactionEventKind
    planId*: uint64
    timestampMs*: int64
    message*: string
    payload*: JsonNode

  CompactionJobHandler* = proc (plan: CompactionPlan): CompactionResult {.gcsafe.}

proc initLevelInput*(level: Level; files: seq[FileMetadata]): LevelInput =
  var total: int64 = 0
  for file in files.items():
    total += file.sizeBytes.int64
  LevelInput(level: level, files: files, bytes: total)

proc aggregateBytes*(inputs: seq[LevelInput]): int64 =
  var total: int64 = 0
  for input in inputs.items():
    total += input.bytes
  total

proc levelTarget*(cfg: CompactionPlannerConfig; level: Level): int64 =
  if level < 0:
    return 0
  if level < cfg.levelSizeTargets.len:
    return cfg.levelSizeTargets[level]
  if cfg.levelSizeTargets.len == 0:
    return 0
  cfg.levelSizeTargets[^1]

proc assignPriority*(score: float): CompactionPriority =
  if score >= 2.0:
    return priorityCritical
  if score >= 1.5:
    return priorityHigh
  if score >= 1.0:
    return priorityNormal
  priorityLow

proc writeAmplification*(outcome: CompactionResult): float =
  if outcome.bytesRead <= 0:
    return 0.0
  outcome.bytesWritten.float / outcome.bytesRead.float

proc nowTimestampMs*(): int64 =
  (now().toTime().toUnixFloat() * 1000.0).int64

proc toJson*(plan: CompactionPlan): JsonNode =
  ## 序列化计划，便于调试与事件输出。
  result = newJObject()
  result["id"] = %plan.id
  result["strategy"] = %plan.strategy
  result["outputLevel"] = %plan.outputLevel
  result["score"] = %plan.score
  result["priority"] = %plan.priority.ord
  result["estimatedBytes"] = %plan.estimatedBytes
  var inputs = newJArray()
  for input in plan.inputs.items():
    var entry = newJObject()
    entry["level"] = %input.level
    entry["bytes"] = %input.bytes
    var files = newJArray()
    for meta in input.files.items():
      var node = newJObject()
      node["file"] = %meta.fileNum.toUint64()
      node["bytes"] = %meta.sizeBytes
      node["smallest"] = %meta.smallest.toBytes()
      node["largest"] = %meta.largest.toBytes()
      files.add(node)
    entry["files"] = files
    inputs.add(entry)
  result["inputs"] = inputs
  var shards = newJArray()
  for shard in plan.outputShards.items():
    var node = newJObject()
    node["index"] = %shard.index
    node["targetBytes"] = %shard.targetBytes
    if shard.smallest.isSome():
      node["smallest"] = %shard.smallest.get().toBytes()
    if shard.largest.isSome():
      node["largest"] = %shard.largest.get().toBytes()
    shards.add(node)
  result["outputShards"] = shards

proc defaultTelemetry*(): CompactionTelemetry =
  CompactionTelemetry(levelScores: @[],
                      selectedStrategy: none(string),
                      reason: none(string))
