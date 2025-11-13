# Metamorphic runner：随机化执行 DSL 场景，生成状态快照供 QA 对比。

import std/[options, random, sequtils, strformat]

import pebble/batch
import pebble/batch/interop
import pebble/core/types
import pebble/qa/dsl

type
  QARunnerConfig* = object
    iterations*: int = 1
    seed*: int = 0
    freshDbEachIteration*: bool = true
    makeDb*: proc (): DB {.gcsafe.}

  QAIterationResult* = object
    iteration*: int
    state*: BatchState

  QARunResult* = object
    iterations*: seq[QAIterationResult]

proc latestState*(runResult: QARunResult): BatchState =
  if runResult.iterations.len == 0:
    return @[]
  runResult.iterations[^1].state

proc runScenario*(actions: seq[QAAction];
                  cfg: QARunnerConfig = QARunnerConfig()): QARunResult =
  if actions.len == 0 or cfg.iterations <= 0:
    return QARunResult(iterations: @[])
  let dbFactory =
    if cfg.makeDb.isNil():
      proc (): DB =
        initDB(DBConfig())
    else:
      cfg.makeDb
  var rng = initRand(cfg.seed)
  var output = QARunResult(iterations: @[])
  var sharedDb: DB = nil
  var persistentState: BatchState = @[]
  for iter in 0 ..< cfg.iterations:
    if cfg.freshDbEachIteration or sharedDb.isNil():
      if not sharedDb.isNil():
        sharedDb.close()
      sharedDb = dbFactory()
      if sharedDb.isNil():
        raise newException(BatchError, "QA runner DB factory returned nil")
      if cfg.freshDbEachIteration:
        persistentState = @[]
    var sequence = actions
    shuffle(rng, sequence)
    var state =
      if cfg.freshDbEachIteration: @[]
      else: persistentState
    let db = sharedDb
    for action in sequence.items():
      let batch = db.newBatch()
      case action.kind
      of qaPut:
        let value = action.value.get()
        batch.put(action.key.toKey(), value)
        setValue(state, action.key, value)
      of qaDelete:
        batch.delete(action.key.toKey())
        if hasKey(state, action.key):
          delKey(state, action.key)
      of qaMerge:
        let operand = action.value.get()
        batch.merge(action.key.toKey(), operand)
      discard db.commit(batch)
      if action.kind == qaMerge:
        let merged = db.get(action.key.toKey())
        if merged.isSome():
          setValue(state, action.key, merged.get())
        elif hasKey(state, action.key):
          delKey(state, action.key)
    if cfg.freshDbEachIteration:
      db.close()
      sharedDb = nil
    else:
      persistentState = state
    output.iterations.add(QAIterationResult(iteration: iter, state: state))
  if not sharedDb.isNil():
    sharedDb.close()
  output
