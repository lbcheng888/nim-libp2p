import std/[options, os, strutils, times, unittest]

import pebble/batch/types as batch_types
import pebble/core/types as pebble_core
import pebble/kv

proc makeTempDir(): string =
  var attempt = 0
  while true:
    let stamp = int64(epochTime() * 1_000_000.0)
    let candidate = getTempDir() / ("pebblekv_basic_" & $stamp & "_" & $getCurrentProcessId() & "_" & $attempt)
    if not dirExists(candidate):
      createDir(candidate)
      return candidate
    inc attempt

suite "PebbleKV basic CRUD":
  test "put/get/delete with binary payloads and sync options":
    let root = makeTempDir()
    let kv = newPebbleKV(root)
    let syncOpts = batch_types.WriteOptions(sync: true)
    let payload = @[byte 0x00, 0x7F, 0xFF, 0x10, 0x80]
    try:
      kv.putBytes(ksEvent, "bin-event", payload, syncOpts)
      let fetched = kv.getBytes(ksEvent, "bin-event")
      check fetched.isSome()
      check fetched.get() == payload
      kv.delete(ksEvent, "bin-event", syncOpts)
      check kv.get(ksEvent, "bin-event").isNone()
    finally:
      kv.close()
      removeDir(root)

  test "applyBatch handles mixed string/binary writes and deletes":
    let root = makeTempDir()
    let kv = newPebbleKV(root)
    let syncOpts = batch_types.WriteOptions(sync: true)
    let sigPayload = @[byte 1, 2, 3, 4]
    try:
      kv.applyBatch([
        withPut(ksEvent, "evt-1", "value-1"),
        withPutBytes(ksSignature, "sig-raw", sigPayload)
      ], syncOpts)
      let evt = kv.get(ksEvent, "evt-1")
      check evt.isSome()
      check evt.get() == "value-1"
      let gotSig = kv.getBytes(ksSignature, "sig-raw")
      check gotSig.isSome()
      check gotSig.get() == sigPayload
      kv.applyBatch([
        withDelete(ksEvent, "evt-1")
      ], syncOpts)
      check kv.get(ksEvent, "evt-1").isNone()
    finally:
      kv.close()
      removeDir(root)

  test "write batch logs binary operations and commits atomically":
    let root = makeTempDir()
    let kv = newPebbleKV(root)
    let syncOpts = batch_types.WriteOptions(sync: true)
    let binPayload = @[byte 0xDE, 0xAD, 0xBE, 0xEF]
    try:
      let batch = kv.newWriteBatch()
      batch.put(ksTask, "task-001", binPayload)
      batch.put(ksTask, "task-002", "task-value")
      check batch.len == 2
      let seq = batch.commit(syncOpts)
      check batch.committed()
      check pebble_core.toUint64(seq) > 0
      let task1 = kv.getBytes(ksTask, "task-001")
      check task1.isSome()
      check task1.get() == binPayload
      let task2 = kv.get(ksTask, "task-002")
      check task2.isSome()
      check task2.get() == "task-value"
      check batch.writes().len == 2
    finally:
      kv.close()
      removeDir(root)

  test "withWriteBatch auto commits multi-key writes and respects manual rollback":
    let root = makeTempDir()
    let kv = newPebbleKV(root)
    try:
      let seq = kv.withWriteBatch(proc (wb: PebbleWriteBatch) =
        wb.put(ksEvent, "evt-atom", "value-atom")
        wb.put(ksIndex, "cursor", "idx-val")
      )
      check pebble_core.toUint64(seq) > 0
      let evt = kv.get(ksEvent, "evt-atom")
      check evt.isSome()
      check evt.get() == "value-atom"
      let idx = kv.get(ksIndex, "cursor")
      check idx.isSome()
      check idx.get() == "idx-val"
      expect ValueError:
        discard kv.withWriteBatch(proc (wb: PebbleWriteBatch) =
          wb.put(ksEvent, "evt-fail", "value")
          raise newException(ValueError, "fail batch")
        )
      check kv.get(ksEvent, "evt-fail").isNone()
    finally:
      kv.close()
      removeDir(root)

  test "replayWrites skips duplicates when dedupe enabled and can force reapply":
    let root = makeTempDir()
    let kv = newPebbleKV(root)
    var captured: seq[KVWrite] = @[]
    try:
      let firstSeq = kv.withWriteBatch(proc (wb: PebbleWriteBatch) =
        wb.put(ksTask, "task-replay", "payload-1")
        wb.put(ksIndex, "task-index", "task-replay")
        captured = wb.writes()
      )
      check pebble_core.toUint64(firstSeq) > 0
      check captured.len == 2
      let dedupeSeq = kv.replayWrites(captured)
      check pebble_core.toUint64(dedupeSeq) == 0
      let forcedSeq = kv.replayWrites(captured, dedupe = false)
      check pebble_core.toUint64(forcedSeq) > 0
      let taskVal = kv.get(ksTask, "task-replay")
      check taskVal.isSome()
      check taskVal.get() == "payload-1"
    finally:
      kv.close()
      removeDir(root)

  test "PebbleWriteBatch.replay rebuilds batch from captured log":
    let root = makeTempDir()
    let kv = newPebbleKV(root)
    var log: seq[KVWrite] = @[]
    try:
      discard kv.withWriteBatch(proc (wb: PebbleWriteBatch) =
        wb.put(ksSignature, "sig-log", "sig-val")
        log = wb.writes()
      )
      kv.delete(ksSignature, "sig-log")
      let replayBatch = kv.newWriteBatch()
      replayBatch.replay(log)
      check replayBatch.writes() == log
      let seq = replayBatch.commit()
      check pebble_core.toUint64(seq) > 0
      let sigVal = kv.get(ksSignature, "sig-log")
      check sigVal.isSome()
      check sigVal.get() == "sig-val"
    finally:
      kv.close()
      removeDir(root)

  test "write log JSON encode/decode roundtrip":
    var log = @[
      withPut(ksEvent, "evt-json", "value-json"),
      withDelete(ksIndex, "cursor-1"),
      withPutBytes(ksSignature, "sig-bin", @[byte 0x00, 0x11, 0x7F, 0xFF])
    ]
    let encodedNode = encodeWritesToJson(log)
    let decodedNode = decodeWritesFromJson(encodedNode)
    check decodedNode.isSome()
    check decodedNode.get() == log

    let encodedStr = encodeWritesToString(log)
    let decodedStr = decodeWritesFromString(encodedStr)
    check decodedStr.isSome()
    check decodedStr.get() == log

    check decodeWritesFromString("{").isNone()
