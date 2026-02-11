# CompactionPlanner：生成压实计划，封装策略拆分、层级评分与输出分片。

import std/[algorithm, math, options, sequtils, strformat, times]

import pebble/compaction/types
import pebble/core/types
import pebble/manifest/types
import pebble/manifest/version

type
  PlannerContext* = object
    version*: Version
    cfg*: CompactionPlannerConfig
    comparator*: Comparator

  CompactionStrategy* = proc (ctx: PlannerContext;
                              telemetry: var CompactionTelemetry): Option[CompactionPlan] {.gcsafe.}

  StrategyEntry = object
    name: string
    fn: CompactionStrategy

  CompactionPlanner* = ref object
    cfg: CompactionPlannerConfig
    comparator: Comparator
    strategies: seq[StrategyEntry]
    nextId: uint64

proc ensureComparator(comp: Comparator): Comparator =
  if comp.isNil():
    return compareBytewise
  comp

proc flattenInputs(inputs: seq[LevelInput]): seq[FileMetadata] =
  for input in inputs.items():
    result.add(input.files)

proc sortFilesByKey(files: var seq[FileMetadata]; comparator: Comparator) =
  files.sort(proc (a, b: FileMetadata): int =
    let cmpSmall = comparator(a.smallest, b.smallest)
    if cmpSmall != 0:
      return cmpSmall
    comparator(a.largest, b.largest)
  )

proc updateSmallest(target: var Option[Key]; candidate: Key; comparator: Comparator) =
  if target.isNone() or comparator(candidate, target.get()) < 0:
    target = some(candidate)

proc updateLargest(target: var Option[Key]; candidate: Key; comparator: Comparator) =
  if target.isNone() or comparator(candidate, target.get()) > 0:
    target = some(candidate)

proc buildOutputShards(planner: CompactionPlanner; plan: var CompactionPlan) =
  if plan.estimatedBytes <= 0 or plan.inputs.len == 0:
    plan.outputShards = @[]
    return
  var files = flattenInputs(plan.inputs)
  if files.len == 0:
    plan.outputShards = @[]
    return
  let comparator = planner.comparator
  files.sortFilesByKey(comparator)
  let limit =
    if planner.cfg.maxOutputFileBytes <= 0:
      high(int64)
    else:
      planner.cfg.maxOutputFileBytes
  let unlimited = limit == high(int64)
  var shards: seq[OutputShard] = @[]
  var shardIndex = 0
  var current: OutputShard
  var hasCurrent = false
  for meta in files.items():
    let size = meta.sizeBytes.int64
    if hasCurrent and (not unlimited) and
       current.targetBytes >= limit and current.targetBytes + size > limit:
      shards.add(current)
      inc shardIndex
      hasCurrent = false
    if not hasCurrent:
      current = OutputShard(index: shardIndex,
                            targetBytes: 0,
                            smallest: none(Key),
                            largest: none(Key))
      hasCurrent = true
    updateSmallest(current.smallest, meta.smallest, comparator)
    updateLargest(current.largest, meta.largest, comparator)
    current.targetBytes += size
  if hasCurrent and current.targetBytes > 0:
    shards.add(current)
  plan.outputShards = shards

proc ptrRange(files: seq[FileMetadata]): tuple[smallest, largest: Key] =
  if files.len == 0:
    return (smallest: toKey(""), largest: toKey(""))
  var minKey = files[0].smallest.toBytes()
  var maxKey = files[0].largest.toBytes()
  for meta in files.items():
    let s = meta.smallest.toBytes()
    let l = meta.largest.toBytes()
    if s < minKey:
      minKey = s
    if l > maxKey:
      maxKey = l
  (smallest: toKey(minKey), largest: toKey(maxKey))

proc overlapsRange(meta: FileMetadata;
                   smallest, largest: Key;
                   comparator: Comparator): bool =
  let cmpStart = comparator(meta.largest, smallest)
  let cmpEnd = comparator(meta.smallest, largest)
  not (cmpStart < 0 or cmpEnd > 0)

proc selectContiguousRun(files: seq[FileMetadata]; maxBytes: int64): seq[FileMetadata] =
  if files.len == 0:
    return @[]
  if maxBytes <= 0:
    return files
  var start = 0
  var windowBytes: int64 = 0
  var best: tuple[lo, hi: int, bytes: int64] = (0, 0, 0)
  for hi in 0 ..< files.len:
    windowBytes += files[hi].sizeBytes.int64
    while windowBytes > maxBytes and start <= hi:
      windowBytes -= files[start].sizeBytes.int64
      inc start
    if windowBytes > best.bytes:
      best = (start, hi, windowBytes)
  if best.bytes == 0:
    return @[files[0]]
  result = files[best.lo .. best.hi]

proc ensureMinBytes(plan: CompactionPlan; cfg: CompactionPlannerConfig): bool =
  if cfg.minCompactionBytes <= 0:
    return true
  plan.estimatedBytes >= cfg.minCompactionBytes

proc finalizePlan(planner: CompactionPlanner; plan: var CompactionPlan) =
  planner.nextId = planner.nextId + 1
  plan.id = planner.nextId
  plan.createdAt = now()
  plan.estimatedBytes = aggregateBytes(plan.inputs)
  plan.priority = assignPriority(plan.score)
  planner.buildOutputShards(plan)

proc strategyL0(ctx: PlannerContext;
                telemetry: var CompactionTelemetry): Option[CompactionPlan] =
  let l0 = ctx.version.levelFiles(0)
  if l0.len == 0 or ctx.cfg.l0CompactionTrigger <= 0:
    return none(CompactionPlan)
  let score = l0.len.float / ctx.cfg.l0CompactionTrigger.float
  telemetry.levelScores.add((level: 0, score: score))
  if score < 1.0:
    return none(CompactionPlan)
  let range = ptrRange(l0)
  let l1 = ctx.version.levelFiles(1).filterIt(
    overlapsRange(it, range.smallest, range.largest, ctx.comparator))
  var inputs: seq[LevelInput] = @[
    initLevelInput(0, l0),
  ]
  if l1.len > 0:
    inputs.add(initLevelInput(1, l1))
  var plan = CompactionPlan(
    strategy: "l0-flush",
    inputs: inputs,
    outputLevel: 1,
    score: score
  )
  telemetry.reason = some("L0 文件数超阈值")
  some(plan)

proc strategyLevels(ctx: PlannerContext;
                    telemetry: var CompactionTelemetry): Option[CompactionPlan] =
  var bestLevel = -1
  var bestScore = 0.0
  let entries = ctx.version.levelEntries()
  for entry in entries.items():
    if entry.level <= 0:
      continue
    let bytes = entry.files.foldl(a + b.sizeBytes.int64, 0.int64)
    let target = ctx.cfg.levelTarget(entry.level)
    if target <= 0:
      continue
    let score = bytes.float / target.float
    telemetry.levelScores.add((level: entry.level, score: score))
    if score > bestScore:
      bestScore = score
      bestLevel = entry.level
  if bestLevel < 0 or bestScore < 1.0:
    return none(CompactionPlan)
  let files = ctx.version.levelFiles(bestLevel)
  let selected = selectContiguousRun(files, ctx.cfg.maxCompactionBytes)
  if selected.len == 0:
    return none(CompactionPlan)
  let range = ptrRange(selected)
  let nextLevel = bestLevel + 1
  let sibling = ctx.version.levelFiles(nextLevel).filterIt(
    overlapsRange(it, range.smallest, range.largest, ctx.comparator))
  var inputs: seq[LevelInput] = @[initLevelInput(bestLevel, selected)]
  if sibling.len > 0:
    inputs.add(initLevelInput(nextLevel, sibling))
  var plan = CompactionPlan(
    strategy: fmt"lvl-{bestLevel}-to-{nextLevel}",
    inputs: inputs,
    outputLevel: nextLevel,
    score: bestScore
  )
  telemetry.reason = some(
    fmt"Level {bestLevel} 超出目标容量 ({bestScore:0.2f}x)")
  some(plan)

proc initCompactionPlanner*(cfg: CompactionPlannerConfig;
                            comparator: Comparator = nil): CompactionPlanner =
  let planner = CompactionPlanner(
    cfg: cfg,
    comparator: ensureComparator(comparator),
    strategies: @[],
    nextId: 0
  )
  result = planner
  result.strategies.add(StrategyEntry(name: "l0-flush", fn: strategyL0))
  result.strategies.add(StrategyEntry(name: "level-priority", fn: strategyLevels))

proc registerStrategy*(planner: CompactionPlanner;
                       name: string;
                       fn: CompactionStrategy) =
  planner.strategies.add(StrategyEntry(name: name, fn: fn))

proc nextPlan*(planner: CompactionPlanner;
               version: Version;
               telemetry: var CompactionTelemetry): Option[CompactionPlan] =
  let ctx = PlannerContext(
    version: version,
    cfg: planner.cfg,
    comparator: planner.comparator
  )
  for entry in planner.strategies.items():
    var localTelemetry = defaultTelemetry()
    let planOpt = entry.fn(ctx, localTelemetry)
    telemetry.levelScores.add(localTelemetry.levelScores)
    if planOpt.isNone():
      continue
    var plan = planOpt.get()
    if plan.strategy.len == 0:
      plan.strategy = entry.name
    plan.score = max(plan.score, 0.0)
    planner.finalizePlan(plan)
    if not ensureMinBytes(plan, planner.cfg):
      continue
    telemetry.selectedStrategy = some(plan.strategy)
    if telemetry.reason.isNone() and localTelemetry.reason.isSome():
      telemetry.reason = localTelemetry.reason
    return some(plan)
  none(CompactionPlan)

proc nextPlan*(planner: CompactionPlanner; version: Version): Option[CompactionPlan] =
  var telemetry = defaultTelemetry()
  planner.nextPlan(version, telemetry)

proc computeLsmScore*(planner: CompactionPlanner;
                      version: Version): seq[tuple[level: Level, score: float]] =
  var telemetry = defaultTelemetry()
  discard planner.nextPlan(version, telemetry)
  telemetry.levelScores
