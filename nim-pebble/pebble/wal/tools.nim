# WAL 工具集：提供简单的校验与差分能力，支撑双写对比流程。

import std/[options, sequtils, strformat]

import pebble/wal/reader
import pebble/wal/types

proc compareWAL*(leftPath, rightPath: string): (bool, seq[string]) =
  ## 顺序比对两个 WAL 文件，返回是否一致以及差异描述。
  var leftReader = newWalReader(leftPath)
  var rightReader = newWalReader(rightPath)
  try:
    var diffs: seq[string] = @[]
    var index = 0
    while true:
      let leftRec = leftReader.readNext()
      let rightRec = rightReader.readNext()
      if leftRec.isNone() and rightRec.isNone():
        return (diffs.len == 0, diffs)
      if leftRec.isNone() or rightRec.isNone():
        diffs.add(fmt"record {index}: count mismatch (left {leftRec.isSome()}, right {rightRec.isSome()})")
        return (false, diffs)
      let l = leftRec.get()
      let r = rightRec.get()
      if l.payload.len != r.payload.len or l.payload != r.payload:
        diffs.add(fmt"record {index}: payload mismatch (left len={l.payload.len}, right len={r.payload.len})")
      if l.metadata.logNumber != r.metadata.logNumber:
        diffs.add(fmt"record {index}: log number mismatch left={l.metadata.logNumber} right={r.metadata.logNumber}")
      if l.metadata.syncOffset != r.metadata.syncOffset:
        diffs.add(fmt"record {index}: sync offset mismatch left={l.metadata.syncOffset} right={r.metadata.syncOffset}")
      inc index
    (diffs.len == 0, diffs)
  finally:
    leftReader.close()
    rightReader.close()

proc dumpWAL*(path: string): seq[WalReadResult] =
  ## 将 WAL 中的所有记录读出为数组，便于调试或测试。
  var reader = newWalReader(path)
  try:
    reader.readAll()
  finally:
    reader.close()
