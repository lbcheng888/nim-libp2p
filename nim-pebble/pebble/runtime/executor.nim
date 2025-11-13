# Threading and task execution primitives for the Pebble Nim rewrite.
# Implements a lightweight worker pool on top of Nim's threading primitives.

import std/[cpuinfo, locks, math, os, sequtils, strformat]

import pebble/obs

type
  ExecutorMode* = enum
    execThreadPool,
    execSynchronous

  TaskProc* = proc () {.gcsafe.}

  TaskHandle* = ref object
    lock: Lock
    cond: Cond
    completed: bool
    executorName: string
    obs: Observability

  TaskEnvelope = ref object
    task: TaskProc
    handle: TaskHandle

  WorkerArg = object
    queue: ptr seq[TaskEnvelope]
    lock: ptr Lock
    cond: ptr Cond
    shutdownFlag: ptr bool

  Executor* = ref object
    name*: string
    mode*: ExecutorMode
    maxWorkers*: int
    observability*: Observability
    queue: seq[TaskEnvelope]
    queueLock: Lock
    queueCond: Cond
    shutdownFlag: bool
    threads: seq[Thread[WorkerArg]]

proc newTaskHandle(executorName: string; obs: Observability): TaskHandle =
  result = TaskHandle(completed: false,
                      executorName: executorName,
                      obs: obs)
  initLock(result.lock)
  initCond(result.cond)

proc markCompleted(handle: TaskHandle) =
  acquire(handle.lock)
  handle.completed = true
  signal(handle.cond)
  release(handle.lock)
  if not handle.obs.isNil():
    handle.obs.metrics.incCounter("pebble_executor_tasks_total",
                                  "执行器任务统计",
                                  @[("executor", handle.executorName),
                                    ("status", "completed")])
    handle.obs.publishEvent(obsCustom,
                            &"executor '{handle.executorName}' task finished",
                            @[("executor", handle.executorName)])

proc wait*(handle: TaskHandle) =
  if handle.isNil():
    return
  acquire(handle.lock)
  while not handle.completed:
    wait(handle.cond, handle.lock)
  release(handle.lock)

proc isPending*(handle: TaskHandle): bool =
  not handle.isNil() and not handle.completed

proc workerLoop(arg: WorkerArg) {.thread.} =
  while true:
    var env: TaskEnvelope = nil
    acquire(arg.lock[])
    while arg.queue[].len == 0 and not arg.shutdownFlag[]:
      wait(arg.cond[], arg.lock[])
    if arg.queue[].len > 0:
      env = arg.queue[][0]
      arg.queue[].delete(0)
    let shouldStop = arg.shutdownFlag[] and env.isNil()
    release(arg.lock[])
    if shouldStop:
      break
    if env.isNil():
      continue
    try:
      if env.task != nil:
        env.task()
    finally:
      env.handle.markCompleted()

proc initExecutor*(name = "default";
                   mode: ExecutorMode = execThreadPool;
                   maxWorkers = max(1, countProcessors());
                   observability: Observability = nil): Executor =
  var exec = Executor(name: name, mode: mode, maxWorkers: maxWorkers,
                      observability: observability,
                      queue: @[])
  if not exec.observability.isNil():
    exec.observability.metrics.declareMetric(MetricDescriptor(
      name: "pebble_executor_tasks_total",
      help: "执行器任务统计",
      kind: mkCounter,
      labelNames: @["executor", "status"],
      unit: ""
    ))
    exec.observability.metrics.declareMetric(MetricDescriptor(
      name: "pebble_executor_queue_depth",
      help: "执行器队列深度",
      kind: mkGauge,
      labelNames: @["executor"],
      unit: ""
    ))
  if mode == execThreadPool:
    initLock(exec.queueLock)
    initCond(exec.queueCond)
    for _ in 0 ..< maxWorkers:
      var thr: Thread[WorkerArg]
      exec.threads.add(thr)
    let shared = WorkerArg(queue: addr exec.queue,
                           lock: addr exec.queueLock,
                           cond: addr exec.queueCond,
                           shutdownFlag: addr exec.shutdownFlag)
    for idx in 0 ..< exec.threads.len:
      createThread(exec.threads[idx], workerLoop, shared)
  exec

proc submit*(exec: Executor; task: TaskProc): TaskHandle =
  case exec.mode
  of execSynchronous:
    if not exec.observability.isNil():
      exec.observability.metrics.incCounter("pebble_executor_tasks_total",
                                            "执行器任务统计",
                                            @[("executor", exec.name),
                                              ("status", "scheduled")])
      exec.observability.metrics.setGauge("pebble_executor_queue_depth",
                                          "执行器队列深度",
                                          @[("executor", exec.name)],
                                          0.0)
      exec.observability.publishEvent(obsCustom,
                                      &"executor '{exec.name}' scheduled task",
                                      @[("executor", exec.name)])
    task()
    result = newTaskHandle(exec.name, exec.observability)
    result.markCompleted()
  of execThreadPool:
    let handle = newTaskHandle(exec.name, exec.observability)
    let env = TaskEnvelope(task: task, handle: handle)
    acquire(exec.queueLock)
    exec.queue.add(env)
    signal(exec.queueCond)
    release(exec.queueLock)
    if not exec.observability.isNil():
      exec.observability.metrics.incCounter("pebble_executor_tasks_total",
                                            "执行器任务统计",
                                            @[("executor", exec.name),
                                              ("status", "scheduled")])
      exec.observability.metrics.setGauge("pebble_executor_queue_depth",
                                          "执行器队列深度",
                                          @[("executor", exec.name)],
                                          exec.queue.len.float)
      exec.observability.publishEvent(obsCustom,
                                      &"executor '{exec.name}' scheduled task",
                                      @[("executor", exec.name)])
    result = handle

proc shutdown*(exec: Executor; handles: var seq[TaskHandle]) =
  for handle in handles:
    handle.wait()
  handles.setLen(0)
  if exec.mode == execThreadPool:
    acquire(exec.queueLock)
    exec.shutdownFlag = true
    broadcast(exec.queueCond)
    release(exec.queueLock)
    for thr in exec.threads.mitems():
      joinThread(thr)
    exec.threads.setLen(0)
