# CompactionScheduler：协调压实计划执行，管理资源配额、事件记录与并发度。

import std/[json, locks, options, sequtils, strformat, strutils, tables]

import pebble/compaction/planner
import pebble/compaction/types
import pebble/runtime/executor
import pebble/runtime/resource_manager
import pebble/manifest/version
import pebble/manifest/types
import pebble/core/types

type
  CompactionSchedulerConfig* = object
    maxConcurrentJobs*: int
    acquireAmount*: int64
    eventBufferLimit*: int
    executor*: Executor
    resourceManager*: ResourceManager
    resourceKind*: ResourceKind

  TicketResult = object
    granted: bool
    ticket: Option[ResourceTicket]

  ActiveJob = ref object
    planId: uint64
    handle: TaskHandle
    ticket: Option[ResourceTicket]

  CompactionScheduler* = ref object
    cfg: CompactionSchedulerConfig
    planner*: CompactionPlanner
    handler*: CompactionJobHandler
    lock: Lock
    active: seq[ActiveJob]
    pending: seq[CompactionPlan]
    metrics*: CompactionMetrics
    events*: seq[CompactionEvent]
    planRegistry: Table[uint64, CompactionPlan]
    planResults: Table[uint64, CompactionResult]

proc normalizeConfig(cfg: CompactionSchedulerConfig): CompactionSchedulerConfig =
  result = cfg
  if result.maxConcurrentJobs <= 0:
    result.maxConcurrentJobs = 1
  if result.eventBufferLimit <= 0:
    result.eventBufferLimit = 256

proc requestTicket(sched: CompactionScheduler): TicketResult =
  let rm = sched.cfg.resourceManager
  if rm.isNil() or sched.cfg.acquireAmount <= 0:
    return TicketResult(granted: true, ticket: none(ResourceTicket))
  if rm.isSoftExceeded(sched.cfg.resourceKind):
    return TicketResult(granted: false, ticket: none(ResourceTicket))
  try:
    let ticket = acquireQuota(rm,
                              sched.cfg.resourceKind,
                              sched.cfg.acquireAmount)
    TicketResult(granted: true, ticket: some(ticket))
  except ResourceError:
    TicketResult(granted: false, ticket: none(ResourceTicket))

proc releaseTicket(sched: CompactionScheduler; ticket: Option[ResourceTicket]) =
  if ticket.isSome() and not sched.cfg.resourceManager.isNil():
    sched.cfg.resourceManager.release(ticket.get())

proc trimEvents(sched: CompactionScheduler) =
  if sched.events.len <= sched.cfg.eventBufferLimit:
    return
  let overflow = sched.events.len - sched.cfg.eventBufferLimit
  sched.events.delete(0, overflow - 1)

proc recordEventLocked(sched: CompactionScheduler;
                       kind: CompactionEventKind;
                       planId: uint64;
                       message: string;
                       payload: JsonNode) =
  var node = payload
  if node.isNil():
    node = newJObject()
  let event = CompactionEvent(
    kind: kind,
    planId: planId,
    timestampMs: nowTimestampMs(),
    message: message,
    payload: node
  )
  sched.events.add(event)
  sched.trimEvents()

proc recordPlanLocked(sched: CompactionScheduler; plan: CompactionPlan) =
  sched.planRegistry[plan.id] = plan

proc initCompactionScheduler*(planner: CompactionPlanner;
                              handler: CompactionJobHandler;
                              cfg: CompactionSchedulerConfig): CompactionScheduler =
  if planner.isNil():
    raise newException(CompactionError, "planner 未初始化")
  if handler.isNil():
    raise newException(CompactionError, "handler 未设置")
  if cfg.executor.isNil():
    raise newException(CompactionError, "scheduler 需要执行器 (executor)")
  let normalized = normalizeConfig(cfg)
  result = CompactionScheduler(
    cfg: normalized,
    planner: planner,
    handler: handler,
    active: @[],
    pending: @[],
    metrics: CompactionMetrics(),
    events: @[],
    planRegistry: initTable[uint64, CompactionPlan](),
    planResults: initTable[uint64, CompactionResult]()
  )
  initLock(result.lock)

proc launchJobLocked(sched: CompactionScheduler;
                     plan: CompactionPlan;
                     ticket: Option[ResourceTicket]) {.gcsafe.}

proc finishJob*(sched: CompactionScheduler;
                plan: CompactionPlan;
                ticket: Option[ResourceTicket];
                success: bool;
                resultOpt: Option[CompactionResult];
                errOpt: Option[string]) {.gcsafe.}

proc schedulePending(sched: CompactionScheduler): Option[CompactionPlan] {.gcsafe.}

proc scheduleNew(sched: CompactionScheduler;
                 version: Version): Option[CompactionPlan]

proc scheduleNext*(sched: CompactionScheduler;
                   version: Version): Option[CompactionPlan] =
  acquire(sched.lock)
  defer: release(sched.lock)
  if sched.active.len >= sched.cfg.maxConcurrentJobs:
    return none(CompactionPlan)
  let pendingPlan = sched.schedulePending()
  if pendingPlan.isSome():
    return pendingPlan
  sched.scheduleNew(version)

proc schedulePending(sched: CompactionScheduler): Option[CompactionPlan] {.gcsafe.} =
  if sched.pending.len == 0:
    return none(CompactionPlan)
  if sched.active.len >= sched.cfg.maxConcurrentJobs:
    return none(CompactionPlan)
  let plan = sched.pending[0]
  let ticketRes = requestTicket(sched)
  if not ticketRes.granted:
    return none(CompactionPlan)
  sched.pending.delete(0)
  sched.launchJobLocked(plan, ticketRes.ticket)
  some(plan)

proc scheduleNew(sched: CompactionScheduler;
                 version: Version): Option[CompactionPlan] =
  var telemetry = defaultTelemetry()
  let planOpt = sched.planner.nextPlan(version, telemetry)
  if planOpt.isNone():
    return none(CompactionPlan)
  let plan = planOpt.get()
  let ticketRes = requestTicket(sched)
  if not ticketRes.granted:
    sched.recordPlanLocked(plan)
    sched.pending.add(plan)
    return none(CompactionPlan)
  sched.launchJobLocked(plan, ticketRes.ticket)
  some(plan)

proc buildStartMessage(plan: CompactionPlan): string =
  var inputs = plan.inputs.mapIt(fmt"L{it.level}({it.files.len})")
  let joined = inputs.join("+")
  fmt"plan {plan.id} start {joined} -> L{plan.outputLevel} ({plan.strategy})"

proc buildSuccessMessage(plan: CompactionPlan;
                         stats: CompactionResult): string =
  fmt"plan {plan.id} finished bytesIn={stats.bytesRead} bytesOut={stats.bytesWritten}"

proc buildFailureMessage(plan: CompactionPlan; reason: string): string =
  if reason.len == 0:
    return fmt"plan {plan.id} failed"
  fmt"plan {plan.id} failed: {reason}"

proc launchJobLocked(sched: CompactionScheduler;
                     plan: CompactionPlan;
                     ticket: Option[ResourceTicket]) {.gcsafe.} =
  sched.recordPlanLocked(plan)
  let payload = toJson(plan)
  sched.recordEventLocked(compactionStart, plan.id,
                          buildStartMessage(plan), payload)
  let handler = sched.handler
  let executor = sched.cfg.executor
  let ticketCopy = ticket
  let planCopy = plan
  let job = ActiveJob(planId: plan.id,
                      handle: nil,
                      ticket: ticket)
  sched.active.add(job)
  var handle: TaskHandle = nil
  var submitError = none(string)
  release(sched.lock)
  try:
    handle = executor.submit(proc () {.gcsafe.} =
      var resultOpt = none(CompactionResult)
      var errOpt = none(string)
      var success = false
      try:
        let res = handler(planCopy)
        resultOpt = some(res)
        success = true
      except CatchableError as err:
        errOpt = some(err.msg)
      sched.finishJob(planCopy, ticketCopy, success,
                      resultOpt, errOpt)
    )
  except CatchableError as err:
    submitError = some(err.msg)
  finally:
    acquire(sched.lock)
  if submitError.isSome():
    if sched.active.len > 0:
      var removeIdx = -1
      for i in 0 ..< sched.active.len:
        if sched.active[i].planId == plan.id:
          removeIdx = i
          break
      if removeIdx >= 0:
        sched.active.delete(removeIdx)
    if ticket.isSome():
      sched.releaseTicket(ticket)
    var failPayload = toJson(plan)
    let reason = submitError.get()
    failPayload["error"] = %reason
    sched.recordEventLocked(compactionFailure, plan.id,
                            buildFailureMessage(plan, reason),
                            failPayload)
    raise newException(CompactionError,
                       fmt"executor 提交压实任务失败: {reason}")
  if not job.isNil():
    job.handle = handle

proc updateMetricsLocked(sched: CompactionScheduler;
                         stats: CompactionResult) =
  sched.metrics.totalJobs += 1
  sched.metrics.bytesRead += stats.bytesRead
  sched.metrics.bytesWritten += stats.bytesWritten
  let writeAmp = writeAmplification(stats)
  sched.metrics.lastWriteAmp = writeAmp
  if writeAmp > sched.metrics.maxWriteAmp:
    sched.metrics.maxWriteAmp = writeAmp
  if sched.metrics.bytesRead > 0:
    let denom = max(1.0, sched.metrics.bytesRead.float)
    sched.metrics.avgWriteAmp = sched.metrics.bytesWritten.float / denom
  if stats.readAmp >= 0:
    sched.metrics.readAmpSamples.add(stats.readAmp)
    if sched.metrics.readAmpSamples.len > 128:
      sched.metrics.readAmpSamples.delete(0)

proc resultPayload(plan: CompactionPlan;
                   stats: CompactionResult): JsonNode =
  var node = toJson(plan)
  var resultNode = newJObject()
  resultNode["bytesRead"] = %stats.bytesRead
  resultNode["bytesWritten"] = %stats.bytesWritten
  resultNode["readAmp"] = %stats.readAmp
  resultNode["writeAmp"] = %writeAmplification(stats)
  var files = newJArray()
  for meta in stats.outputFiles.items():
    var fn = newJObject()
    fn["file"] = %meta.fileNum.toUint64()
    fn["bytes"] = %meta.sizeBytes
    fn["smallest"] = %meta.smallest.toBytes()
    fn["largest"] = %meta.largest.toBytes()
    files.add(fn)
  resultNode["outputFiles"] = files
  node["result"] = resultNode
  node

proc finishJob*(sched: CompactionScheduler;
                plan: CompactionPlan;
                ticket: Option[ResourceTicket];
                success: bool;
                resultOpt: Option[CompactionResult];
                errOpt: Option[string]) {.gcsafe.} =
  acquire(sched.lock)
  defer: release(sched.lock)
  var releasedTicket = ticket
  var idx = -1
  for i, job in sched.active.pairs():
    if job.planId == plan.id:
      idx = i
      releasedTicket = job.ticket
      break
  if idx >= 0:
    let job = sched.active[idx]
    sched.active.delete(idx)
  if releasedTicket.isSome():
    sched.releaseTicket(releasedTicket)
  if success and resultOpt.isSome():
    let result = resultOpt.get()
    sched.planResults[plan.id] = result
    sched.updateMetricsLocked(result)
    let msg = buildSuccessMessage(plan, result)
    let payload = resultPayload(plan, result)
    sched.recordEventLocked(compactionFinish, plan.id, msg, payload)
  else:
    var reason = ""
    if errOpt.isSome():
      reason = errOpt.get()
    let msg = buildFailureMessage(plan, reason)
    var payload = toJson(plan)
    if reason.len > 0:
      payload["error"] = %reason
    sched.recordEventLocked(compactionFailure, plan.id, msg, payload)
  discard sched.schedulePending()

proc drain*(sched: CompactionScheduler) =
  while true:
    var handles: seq[TaskHandle] = @[]
    acquire(sched.lock)
    for job in sched.active.items():
      if not job.handle.isNil():
        handles.add(job.handle)
    release(sched.lock)
    if handles.len == 0:
      acquire(sched.lock)
      let done = sched.active.len == 0
      release(sched.lock)
      if done:
        break
    for handle in handles.items():
      handle.wait()
    acquire(sched.lock)
    let finished = sched.active.len == 0
    release(sched.lock)
    if finished:
      break

proc pendingCount*(sched: CompactionScheduler): int =
  acquire(sched.lock)
  let count = sched.pending.len
  release(sched.lock)
  count

proc activeCount*(sched: CompactionScheduler): int =
  acquire(sched.lock)
  let count = sched.active.len
  release(sched.lock)
  count

proc metricsSnapshot*(sched: CompactionScheduler): CompactionMetrics =
  acquire(sched.lock)
  let snapshot = sched.metrics
  release(sched.lock)
  snapshot

proc eventsSnapshot*(sched: CompactionScheduler): seq[CompactionEvent] =
  acquire(sched.lock)
  let snapshot = sched.events
  release(sched.lock)
  snapshot

proc pendingPlans*(sched: CompactionScheduler): seq[CompactionPlan] =
  acquire(sched.lock)
  let plans = sched.pending
  release(sched.lock)
  plans

proc planFor*(sched: CompactionScheduler; planId: uint64): Option[CompactionPlan] =
  acquire(sched.lock)
  let found = sched.planRegistry.hasKey(planId)
  var plan: CompactionPlan
  if found:
    plan = sched.planRegistry[planId]
  release(sched.lock)
  if found:
    some(plan)
  else:
    none(CompactionPlan)

proc resultFor*(sched: CompactionScheduler; planId: uint64): Option[CompactionResult] =
  acquire(sched.lock)
  let found = sched.planResults.hasKey(planId)
  var result: CompactionResult
  if found:
    result = sched.planResults[planId]
  release(sched.lock)
  if found:
    some(result)
  else:
    none(CompactionResult)

proc goStyleLogLines*(sched: CompactionScheduler): seq[string] =
  acquire(sched.lock)
  var lines: seq[string] = @[]
  for event in sched.events.items():
    let plan = sched.planRegistry.getOrDefault(event.planId, CompactionPlan())
    case event.kind
    of compactionStart:
      var inputs = plan.inputs.mapIt(fmt"L{it.level}({it.files.len})")
      let joined = inputs.join("+")
      lines.add(fmt"[JOB {event.planId}] started {joined} -> L{plan.outputLevel}")
    of compactionFinish:
      if sched.planResults.hasKey(event.planId):
        let res = sched.planResults[event.planId]
        lines.add(fmt"[JOB {event.planId}] success bytes-in={res.bytesRead} bytes-out={res.bytesWritten}")
      else:
        lines.add(fmt"[JOB {event.planId}] success")
    of compactionFailure:
      var reason = ""
      if event.payload.kind == JObject and event.payload.hasKey("error"):
        reason = event.payload["error"].getStr()
      if reason.len > 0:
        lines.add(fmt"[JOB {event.planId}] failed {reason}")
      else:
        lines.add(fmt"[JOB {event.planId}] failed")
  release(sched.lock)
  lines

proc goStyleLog*(sched: CompactionScheduler): string =
  sched.goStyleLogLines().join("\n")

proc eventLogJson*(sched: CompactionScheduler): JsonNode =
  let events = sched.eventsSnapshot()
  var arr = newJArray()
  for event in events.items():
    var node = newJObject()
    node["kind"] = %event.kind.ord
    node["planId"] = %event.planId
    node["timestampMs"] = %event.timestampMs
    node["message"] = %event.message
    if not event.payload.isNil():
      node["payload"] = event.payload
    arr.add(node)
  arr
