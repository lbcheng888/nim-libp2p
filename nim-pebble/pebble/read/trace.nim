# 读路径迭代轨迹捕获与 Go/Nim 对比辅助模块。
#
# 提供：
# - `ReadTraceOp`：描述一次迭代操作（first/next/seek 等）。
# - `traceIterator`：执行一系列操作并记录每一步的可见状态。
# - `diffTraces`：比对两份迭代轨迹输出差异。
# - JSON 序列化/反序列化辅助，便于落地 CLI 工具或与 Go 端对接。

import std/[json, options, strformat, strutils]

import pebble/core/types
import pebble/read/iterators
import pebble/read/types

type
  TraceOpKind* = enum
    traceOpFirst,
    traceOpLast,
    traceOpNext,
    traceOpPrev,
    traceOpSeekGE,
    traceOpSeekLT,
    traceOpSeekPrefixGE,
    traceOpSetBounds,
    traceOpClearBounds,
    traceOpSetPrefix,
    traceOpClearPrefix,
    traceOpRange

  ReadTraceOp* = object
    kind*: TraceOpKind
    key*: Option[string]
    prefix*: Option[string]
    lower*: Option[string]
    upper*: Option[string]

  RangeTraceEntry* = object
    key*: string
    value*: string
    sourcePriority*: int
    sourceKind*: string

  ReadTraceState* = object
    count*: int
    valid*: bool
    key*: Option[string]
    value*: Option[string]
    sourcePriority*: Option[int]
    sourceKind*: Option[string]

  ReadTraceStep* = object
    op*: TraceOpKind
    ok*: bool
    state*: ReadTraceState
    rangeEntries*: seq[RangeTraceEntry]

  ReadTrace* = object
    initialState*: ReadTraceState
    steps*: seq[ReadTraceStep]

const
  traceOpNames: array[TraceOpKind, string] = [
    "first",
    "last",
    "next",
    "prev",
    "seekGE",
    "seekLT",
    "seekPrefixGE",
    "setBounds",
    "clearBounds",
    "setPrefix",
    "clearPrefix",
    "range"
  ]

func opKindToString(kind: TraceOpKind): string =
  traceOpNames[kind]

func parseTraceOpKind*(name: string): TraceOpKind =
  for kind, kindName in traceOpNames.pairs():
    if name.toLowerAscii() == kindName.toLowerAscii():
      return kind
  raise newException(ValueError, "未知的迭代操作: " & name)

func sourceKindToString(kind: ReadSourceKind): string =
  case kind
  of readSourceMem:
    "mem"
  of readSourceSST:
    "sst"
  of readSourceCustom:
    "custom"
  of readSourceBatch:
    "batch"

func optionToString(opt: Option[string]): string =
  if opt.isSome(): opt.get() else: "∅"

func optionToString(opt: Option[int]): string =
  if opt.isSome(): $opt.get() else: "∅"

proc captureState(it: ReadIterator): ReadTraceState =
  result.count = it.count()
  result.valid = it.valid()
  if result.valid:
    let entry = it.currentEntry()
    result.key = some(entry.key.toBytes())
    result.value = some(entry.value)
    result.sourcePriority = some(entry.sourcePriority)
    result.sourceKind = some(sourceKindToString(entry.sourceKind))
  else:
    result.key = none(string)
    result.value = none(string)
    result.sourcePriority = none(int)
    result.sourceKind = none(string)

proc ensureKey(op: ReadTraceOp): string =
  if op.key.isNone():
    raise newException(ValueError, fmt"{op.kind.opKindToString()} 操作需要提供 key")
  op.key.get()

proc ensurePrefix(op: ReadTraceOp): string =
  if op.prefix.isNone():
    raise newException(ValueError, fmt"{op.kind.opKindToString()} 操作需要提供 prefix")
  op.prefix.get()

proc toKeyOption(value: Option[string]): Option[Key] =
  if value.isSome():
    some(value.get().toKey())
  else:
    none(Key)

proc applyOp(it: ReadIterator; op: ReadTraceOp): ReadTraceStep =
  var ok = true
  var rangeEntries: seq[RangeTraceEntry] = @[]
  case op.kind
  of traceOpFirst:
    ok = it.first()
  of traceOpLast:
    ok = it.last()
  of traceOpNext:
    ok = it.next()
  of traceOpPrev:
    ok = it.prev()
  of traceOpSeekGE:
    ok = it.seekGE(op.ensureKey().toKey())
  of traceOpSeekLT:
    ok = it.seekLT(op.ensureKey().toKey())
  of traceOpSeekPrefixGE:
    let prefix = op.ensurePrefix()
    let keyBytes = op.ensureKey()
    ok = it.seekPrefixGE(prefix, keyBytes.toKey())
  of traceOpSetBounds:
    let lowerKey = toKeyOption(op.lower)
    let upperKey = toKeyOption(op.upper)
    it.setBounds(lowerKey, upperKey)
  of traceOpClearBounds:
    it.clearBounds()
  of traceOpSetPrefix:
    it.setPrefix(op.prefix)
  of traceOpClearPrefix:
    it.clearPrefix()
  of traceOpRange:
    let lowerKey = toKeyOption(op.lower)
    let upperKey = toKeyOption(op.upper)
    let effectiveRange =
      if lowerKey.isSome() or upperKey.isSome():
        newReadRange(lowerKey, upperKey)
      else:
        emptyRange()
    for entry in it.range(effectiveRange, op.prefix):
      rangeEntries.add(RangeTraceEntry(
        key: entry.key.toBytes(),
        value: entry.value,
        sourcePriority: entry.sourcePriority,
        sourceKind: sourceKindToString(entry.sourceKind)
      ))
  ReadTraceStep(
    op: op.kind,
    ok: ok,
    state: captureState(it),
    rangeEntries: rangeEntries
  )

proc newTraceOp*(kind: TraceOpKind;
                 key = none(string);
                 prefix = none(string);
                 lower = none(string);
                 upper = none(string)): ReadTraceOp =
  ReadTraceOp(kind: kind, key: key, prefix: prefix, lower: lower, upper: upper)

proc traceIterator*(it: ReadIterator;
                    ops: openArray[ReadTraceOp]): ReadTrace =
  result.initialState = captureState(it)
  for op in ops.items():
    result.steps.add(applyOp(it, op))

proc setOptString(node: var JsonNode; field: string; value: Option[string]) =
  if value.isSome():
    node[field] = %value.get()
  else:
    node[field] = newJNull()

proc setOptInt(node: var JsonNode; field: string; value: Option[int]) =
  if value.isSome():
    node[field] = %value.get()
  else:
    node[field] = newJNull()

proc traceOpToJson*(op: ReadTraceOp): JsonNode =
  result = newJObject()
  result["op"] = %op.kind.opKindToString()
  setOptString(result, "key", op.key)
  setOptString(result, "prefix", op.prefix)
  setOptString(result, "lower", op.lower)
  setOptString(result, "upper", op.upper)

proc rangeEntryToJson(entry: RangeTraceEntry): JsonNode =
  result = %*{
    "key": entry.key,
    "value": entry.value,
    "sourcePriority": entry.sourcePriority,
    "sourceKind": entry.sourceKind
  }

proc stateToJson(state: ReadTraceState): JsonNode =
  result = newJObject()
  result["count"] = %state.count
  result["valid"] = %state.valid
  setOptString(result, "key", state.key)
  setOptString(result, "value", state.value)
  setOptInt(result, "sourcePriority", state.sourcePriority)
  setOptString(result, "sourceKind", state.sourceKind)

proc stepToJson(step: ReadTraceStep): JsonNode =
  result = newJObject()
  result["op"] = %step.op.opKindToString()
  result["ok"] = %step.ok
  result["state"] = stateToJson(step.state)
  result["rangeEntries"] = newJArray()
  for entry in step.rangeEntries.items():
    result["rangeEntries"].add(rangeEntryToJson(entry))

proc readTraceToJson*(trace: ReadTrace): JsonNode =
  result = newJObject()
  result["initialState"] = stateToJson(trace.initialState)
  result["steps"] = newJArray()
  for step in trace.steps.items():
    result["steps"].add(stepToJson(step))

proc getOptString(node: JsonNode; field: string): Option[string] =
  if node.hasKey(field):
    let value = node[field]
    case value.kind
    of JNull:
      none(string)
    of JString:
      some(value.getStr())
    else:
      raise newException(ValueError,
        fmt"字段 {field} 需要 string/null，实际 {value.kind}")
  else:
    none(string)

proc getOptInt(node: JsonNode; field: string): Option[int] =
  if node.hasKey(field):
    let value = node[field]
    case value.kind
    of JNull:
      none(int)
    of JInt:
      some(value.getInt())
    of JFloat:
      some(int(value.getFloat()))
    else:
      raise newException(ValueError,
        fmt"字段 {field} 需要 int/null，实际 {value.kind}")
  else:
    none(int)

proc traceOpFromJson*(node: JsonNode): ReadTraceOp =
  if node.kind != JObject:
    raise newException(ValueError, "迭代操作需要对象表示")
  if not node.hasKey("op"):
    raise newException(ValueError, "缺少 op 字段")
  let opName = node["op"].getStr()
  result.kind = parseTraceOpKind(opName)
  result.key = getOptString(node, "key")
  result.prefix = getOptString(node, "prefix")
  result.lower = getOptString(node, "lower")
  result.upper = getOptString(node, "upper")

proc stateFromJson(node: JsonNode): ReadTraceState =
  if node.kind != JObject:
    raise newException(ValueError, "状态需要对象表示")
  if not node.hasKey("count") or not node.hasKey("valid"):
    raise newException(ValueError, "状态缺少 count 或 valid 字段")
  result.count = node["count"].getInt()
  result.valid = node["valid"].getBool()
  result.key = getOptString(node, "key")
  result.value = getOptString(node, "value")
  result.sourcePriority = getOptInt(node, "sourcePriority")
  result.sourceKind = getOptString(node, "sourceKind")

proc rangeEntryFromJson(node: JsonNode): RangeTraceEntry =
  if node.kind != JObject:
    raise newException(ValueError, "范围条目需要对象表示")
  if not node.hasKey("key") or not node.hasKey("value"):
    raise newException(ValueError, "范围条目缺少 key 或 value")
  result.key = node["key"].getStr()
  result.value = node["value"].getStr()
  if node.hasKey("sourcePriority"):
    result.sourcePriority = node["sourcePriority"].getInt()
  if node.hasKey("sourceKind"):
    result.sourceKind = node["sourceKind"].getStr()

proc stepFromJson(node: JsonNode): ReadTraceStep =
  if node.kind != JObject:
    raise newException(ValueError, "轨迹步骤需要对象表示")
  if not node.hasKey("op") or not node.hasKey("ok") or not node.hasKey("state"):
    raise newException(ValueError, "轨迹步骤缺少必需字段")
  result.op = parseTraceOpKind(node["op"].getStr())
  result.ok = node["ok"].getBool()
  result.state = stateFromJson(node["state"])
  if node.hasKey("rangeEntries"):
    for child in node["rangeEntries"].getElems():
      result.rangeEntries.add(rangeEntryFromJson(child))

proc readTraceFromJson*(node: JsonNode): ReadTrace =
  if node.kind != JObject:
    raise newException(ValueError, "轨迹需要对象表示")
  if not node.hasKey("initialState"):
    raise newException(ValueError, "轨迹缺少 initialState")
  result.initialState = stateFromJson(node["initialState"])
  if node.hasKey("steps"):
    for child in node["steps"].getElems():
      result.steps.add(stepFromJson(child))

proc compareStates(refState, candState: ReadTraceState;
                   context, refLabel, candLabel: string;
                   diffs: var seq[string]) =
  if refState.count != candState.count:
    diffs.add(fmt"{context}: {refLabel} count={refState.count}, {candLabel} count={candState.count}")
  if refState.valid != candState.valid:
    diffs.add(fmt"{context}: {refLabel} valid={refState.valid}, {candLabel} valid={candState.valid}")
  if refState.key != candState.key:
    diffs.add(fmt"{context}: {refLabel} key={optionToString(refState.key)}, {candLabel} key={optionToString(candState.key)}")
  if refState.value != candState.value:
    diffs.add(fmt"{context}: {refLabel} value={optionToString(refState.value)}, {candLabel} value={optionToString(candState.value)}")
  if refState.sourcePriority != candState.sourcePriority:
    diffs.add(fmt"{context}: {refLabel} sourcePriority={optionToString(refState.sourcePriority)}, {candLabel} sourcePriority={optionToString(candState.sourcePriority)}")
  if refState.sourceKind != candState.sourceKind:
    diffs.add(fmt"{context}: {refLabel} sourceKind={optionToString(refState.sourceKind)}, {candLabel} sourceKind={optionToString(candState.sourceKind)}")

proc compareRangeEntries(refEntries, candEntries: seq[RangeTraceEntry];
                         context, refLabel, candLabel: string;
                         diffs: var seq[string]) =
  if refEntries.len != candEntries.len:
    diffs.add(fmt"{context}: 范围条目数量不同 {refLabel}={refEntries.len}, {candLabel}={candEntries.len}")
  let limit = min(refEntries.len, candEntries.len)
  for idx in 0 ..< limit:
    let r = refEntries[idx]
    let c = candEntries[idx]
    if r.key != c.key or r.value != c.value or
       r.sourcePriority != c.sourcePriority or
       r.sourceKind != c.sourceKind:
      diffs.add(fmt"{context}: range[{idx}] {refLabel}=(key={r.key}, value={r.value}, priority={r.sourcePriority}, kind={r.sourceKind}) | {candLabel}=(key={c.key}, value={c.value}, priority={c.sourcePriority}, kind={c.sourceKind})")

proc diffTraces*(reference, candidate: ReadTrace;
                 referenceLabel = "go";
                 candidateLabel = "nim"): seq[string] =
  compareStates(reference.initialState, candidate.initialState,
                "初始状态", referenceLabel, candidateLabel, result)
  if reference.steps.len != candidate.steps.len:
    result.add(fmt"步骤数量不同 {referenceLabel}={reference.steps.len}, {candidateLabel}={candidate.steps.len}")
  let limit = min(reference.steps.len, candidate.steps.len)
  for idx in 0 ..< limit:
    let refStep = reference.steps[idx]
    let candStep = candidate.steps[idx]
    let ctx = fmt"步骤 {idx}({refStep.op.opKindToString()})"
    if refStep.op != candStep.op:
      result.add(fmt"{ctx}: 操作类型不同 {referenceLabel}={refStep.op.opKindToString()}, {candidateLabel}={candStep.op.opKindToString()}")
    if refStep.ok != candStep.ok:
      result.add(fmt"{ctx}: ok 不同 {referenceLabel}={refStep.ok}, {candidateLabel}={candStep.ok}")
    compareStates(refStep.state, candStep.state, ctx, referenceLabel, candidateLabel, result)
    compareRangeEntries(refStep.rangeEntries, candStep.rangeEntries,
                        ctx, referenceLabel, candidateLabel, result)
