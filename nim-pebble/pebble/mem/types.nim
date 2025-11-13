# 内存写路径公共类型与接口定义。

import std/[options]

import pebble/core/types
import pebble/runtime/executor
import pebble/runtime/resource_manager

type
  MemError* = object of CatchableError

  MergeOperator* = proc (key: Key;
                         existing: Option[string];
                         operands: seq[string]): Option[string]
                        {.gcsafe, raises: [].}

  MemValueKind* = enum
    memValueSet,
    memValueDelete,
    memValueMerge

  MemRecord* = object
    key*: Key
    seq*: SequenceNumber
    kind*: MemValueKind
    value*: string

  ApplyEntry* = object
    key*: Key
    seq*: SequenceNumber
    kind*: MemValueKind
    value*: string

  Flushable* = ref object of RootObj
    comparator*: Comparator
    approxBytes*: int
    frozen*: bool

  FlushTrigger* = proc (target: Flushable) {.gcsafe, raises: [].}

  MemWritePriority* = enum
    memPriorityDefault,
    memPriorityLow,
    memPriorityNormal,
    memPriorityHigh

  ThrottleConfig* = object
    enabled*: bool
    stallLimitBytes*: int
    releaseTargetBytes*: int
    baseDelayMs*: int
    maxDelayMs*: int
    priorityScale*: array[memPriorityLow .. memPriorityHigh, float]

  MemTableConfig* = object
    comparator*: Comparator
    mergeOperator*: MergeOperator
    softLimitBytes*: int
    hardLimitBytes*: int
    resourceManager*: ResourceManager
    resourceKind*: ResourceKind
    flushExecutor*: Executor
    flushTrigger*: FlushTrigger
    throttleConfig*: ThrottleConfig

  MemTable* = ref object of Flushable
    cfg*: MemTableConfig
    records*: seq[MemRecord]
    bytesUsed*: int
    lastSeq*: SequenceNumber
    tickets*: seq[ResourceTicket]
    flushScheduled*: bool
    throttleActive*: bool
    lastThrottleDelayMs*: int

  FrozenResourceState* = object
    kind*: ResourceKind
    outstandingBytes*: int64

  FrozenMemEntry* = object
    key*: string
    seq*: uint64
    kind*: MemValueKind
    value*: string

  FrozenMemTable* = object
    label*: string
    approxBytes*: int
    totalEntries*: int
    softLimitBytes*: int
    hardLimitBytes*: int
    lastSeq*: uint64
    throttleActive*: bool
    throttleDelayMs*: int
    resources*: seq[FrozenResourceState]
    entries*: seq[FrozenMemEntry]
  MemTableStats* = object
    approxBytes*: int
    softLimitBytes*: int
    hardLimitBytes*: int
    frozen*: bool
    throttleActive*: bool
    lastThrottleDelayMs*: int

proc hasMerge*(cfg: MemTableConfig): bool {.inline.} =
  not cfg.mergeOperator.isNil()

proc normalize*(cfg: var ThrottleConfig) =
  ## 填充节流配置的默认值，避免后续计算出现零除或无穷等待。
  if cfg.priorityScale[memPriorityLow] == 0.0 and
     cfg.priorityScale[memPriorityNormal] == 0.0 and
     cfg.priorityScale[memPriorityHigh] == 0.0:
    cfg.priorityScale[memPriorityLow] = 1.0
    cfg.priorityScale[memPriorityNormal] = 1.0
    cfg.priorityScale[memPriorityHigh] = 0.5
  if cfg.baseDelayMs <= 0:
    cfg.baseDelayMs = 1
  if cfg.maxDelayMs <= 0:
    cfg.maxDelayMs = cfg.baseDelayMs

proc throttleEnabled*(cfg: ThrottleConfig): bool {.inline.} =
  cfg.enabled and (cfg.stallLimitBytes > 0 or cfg.releaseTargetBytes > 0)

proc resourceKindToString*(kind: ResourceKind): string {.inline.} =
  case kind
  of resMemoryBytes:
    "resMemoryBytes"
  of resDiskBytes:
    "resDiskBytes"
  of resFileHandles:
    "resFileHandles"
  of resCompactionTokens:
    "resCompactionTokens"

proc resourceKindFromString*(value: string): ResourceKind =
  case value
  of "resMemoryBytes":
    resMemoryBytes
  of "resDiskBytes":
    resDiskBytes
  of "resFileHandles":
    resFileHandles
  of "resCompactionTokens":
    resCompactionTokens
  else:
    raise newException(ValueError, "unknown resource kind: " & value)

proc memValueKindToString*(kind: MemValueKind): string {.inline.} =
  case kind
  of memValueSet:
    "set"
  of memValueDelete:
    "delete"
  of memValueMerge:
    "merge"

proc memValueKindFromString*(value: string): MemValueKind =
  case value
  of "set", "memValueSet":
    memValueSet
  of "delete", "del", "memValueDelete":
    memValueDelete
  of "merge", "memValueMerge":
    memValueMerge
  else:
    raise newException(ValueError, "unknown mem value kind: " & value)
