# 批处理子系统的互操作辅助：解析操作日志、应用到 Nim DB 并生成状态快照。

import std/[algorithm, json, options, os, sequtils, sets, strformat, strutils]

import pebble/batch/db as batch_db
import pebble/batch/batch as batch_impl
import pebble/batch/types as batch_types
import pebble/core/types

type
  BatchOpKind* = enum
    bopPut,
    bopDelete,
    bopMerge

  BatchOperation* = object
    kind*: BatchOpKind
    key*: string
    value*: string

  BatchState* = seq[(string, string)]

proc findKey(state: BatchState; key: string): int =
  for idx, entry in state.pairs():
    if entry[0] == key:
      return idx
  -1

proc hasKey*(state: BatchState; key: string): bool =
  state.findKey(key) >= 0

proc setValue*(state: var BatchState; key, value: string) =
  let idx = state.findKey(key)
  if idx >= 0:
    state[idx] = (key, value)
  else:
    state.add((key, value))

proc delKey*(state: var BatchState; key: string) =
  let idx = state.findKey(key)
  if idx >= 0:
    state.delete(idx)

proc getValue*(state: BatchState; key: string): Option[string] =
  let idx = state.findKey(key)
  if idx >= 0:
    some(state[idx][1])
  else:
    none(string)

proc getOrDefault*(state: BatchState; key: string; fallback = ""): string =
  let idx = state.findKey(key)
  if idx >= 0:
    state[idx][1]
  else:
    fallback

iterator keyValuePairs*(state: BatchState): tuple[key: string, value: string] =
  for entry in state.items():
    yield (entry[0], entry[1])

proc cloneState*(state: BatchState): BatchState =
  result = @[]
  for entry in state.items():
    result.add(entry)

proc opKindToString*(kind: BatchOpKind): string =
  case kind
  of bopPut: "put"
  of bopDelete: "delete"
  of bopMerge: "merge"

proc parseOpKind(kind: string): BatchOpKind =
  let lower = kind.toLowerAscii()
  case lower
  of "put": bopPut
  of "delete", "del": bopDelete
  of "merge": bopMerge
  else:
    raise newException(ValueError, "未知批处理操作类型: " & kind)

proc loadOperationsJson*(node: JsonNode): seq[BatchOperation] =
  if node.kind != JArray:
    raise newException(ValueError, "操作日志必须是数组")
  for entry in node.elems:
    if entry.kind != JObject:
      raise newException(ValueError, "操作日志元素必须是对象")
    if not entry.hasKey("op") or not entry.hasKey("key"):
      raise newException(ValueError, "操作缺少必需字段(op/key)")
    let kind = parseOpKind(entry["op"].getStr())
    let key = entry["key"].getStr()
    let value =
      if entry.hasKey("value"):
        entry["value"].getStr()
      else:
        ""
    result.add(BatchOperation(kind: kind, key: key, value: value))

proc loadOperationsFile*(path: string): seq[BatchOperation] =
  if not fileExists(path):
    raise newException(IOError, "操作文件不存在: " & path)
  let node = parseJson(readFile(path))
  loadOperationsJson(node)

proc applyOperations*(db: batch_db.DB; ops: openArray[BatchOperation]; state: var BatchState) =
  for op in ops:
    let batch = db.newBatch()
    case op.kind
    of bopPut:
      batch.put(op.key.toKey(), op.value)
    of bopDelete:
      batch.delete(op.key.toKey())
    of bopMerge:
      batch.merge(op.key.toKey(), op.value)
    discard db.commit(batch)
    case op.kind
    of bopPut, bopMerge:
      let current = db.get(op.key.toKey())
      if current.isSome():
        setValue(state, op.key, current.get())
      else:
        if state.hasKey(op.key):
          state.delKey(op.key)
    of bopDelete:
      if state.hasKey(op.key):
        state.delKey(op.key)
  discard

proc applyOperations*(db: batch_db.DB; ops: openArray[BatchOperation]): BatchState =
  var state: BatchState = @[]
  applyOperations(db, ops, state)
  state

proc stateToJson*(state: BatchState): JsonNode =
  var arr = newJArray()
  var entries = cloneState(state)
  entries.sort(proc (a, b: (string, string)): int = cmp(a[0], b[0]))
  for entry in entries.items():
    let (key, value) = entry
    var node = newJObject()
    node["key"] = newJString(key)
    node["value"] = newJString(value)
    arr.add(node)
  arr

proc jsonToState*(node: JsonNode): BatchState =
  if node.isNil():
    return @[]
  if node.kind != JArray:
    raise newException(ValueError, "状态快照必须是数组")
  var table: BatchState = @[]
  for entry in node.elems:
    if entry.kind != JObject or not entry.hasKey("key"):
      raise newException(ValueError, "状态条目必须包含键")
    let key = entry["key"].getStr()
    let value =
      if entry.hasKey("value"):
        entry["value"].getStr()
      else:
        ""
    setValue(table, key, value)
  table

proc diffStates*(lhs, rhs: BatchState): seq[string] =
  var unionKeys = initHashSet[string]()
  for entry in lhs.items():
    unionKeys.incl(entry[0])
  for entry in rhs.items():
    unionKeys.incl(entry[0])
  var keys = toSeq(unionKeys)
  keys.sort()
  for key in keys:
    let leftVal = lhs.getValue(key)
    let rightVal = rhs.getValue(key)
    if leftVal.isNone() and rightVal.isNone():
      continue
    if leftVal.isNone() or rightVal.isNone():
      let side = if leftVal.isSome(): "lhs" else: "rhs"
      result.add(fmt"Key '{key}' 仅存在于 {side}")
    else:
      if leftVal.get() != rightVal.get():
        result.add(fmt"Key '{key}' 值不一致 lhs='{leftVal.get()}' rhs='{rightVal.get()}'")
