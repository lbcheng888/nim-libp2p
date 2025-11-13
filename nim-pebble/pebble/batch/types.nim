# 批处理子系统公共类型与选项定义。

import pebble/core/types
import pebble/mem/types
import pebble/wal/writer as wal_writer

type
  BatchError* = object of CatchableError

  BatchOpKind* = MemValueKind

  BatchOp* = object
    key*: Key
    value*: string
    kind*: BatchOpKind

  BackpressureReason* = enum
    bpNone,
    bpMemSoftLimit,
    bpMemStall

  BackpressureInfo* = object
    reason*: BackpressureReason
    queuedOps*: int
    queuedBytes*: int
    memApproxBytes*: int

  BackpressureHandler* = proc (info: BackpressureInfo) {.gcsafe, raises: [].}

  WritePriority* = MemWritePriority

  WriteOptions* = object
    sync*: bool
    disableWAL*: bool
    nudge*: BackpressureHandler
    timeoutMs*: int
    priority*: WritePriority

const
  defaultWriteOptions*: WriteOptions =
    WriteOptions(sync: false,
                 disableWAL: false,
                 nudge: nil,
                 timeoutMs: 0,
                 priority: memPriorityDefault)

type
  Snapshot* = object
    seq*: SequenceNumber

  DBConfig* = object
    memConfig*: MemTableConfig
    maxConcurrentCommits*: int
    defaultOptions*: WriteOptions
    backpressure*: BackpressureHandler
    walWriter*: wal_writer.WalWriter

  BatchStats* = object
    ops*: int
    bytes*: int

proc ensureMemConfig*(cfg: var DBConfig) =
  ## 若未显式提供默认写入选项，则填充默认值并保持配置一致。
  if cfg.defaultOptions.nudge.isNil() and not cfg.backpressure.isNil():
    cfg.defaultOptions.nudge = cfg.backpressure
  if cfg.defaultOptions.priority == memPriorityDefault:
    cfg.defaultOptions.priority = memPriorityNormal
