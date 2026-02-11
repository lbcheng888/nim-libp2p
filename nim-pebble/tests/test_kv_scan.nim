import std/[unittest, os, options, strutils, times]

import pebble/core/types as pebble_core
import pebble/kv

proc makeTempDir(): string =
  var attempt = 0
  while true:
    let stamp = int64(epochTime() * 1_000_000.0)
    let candidate = getTempDir() / ("pebblekv_test_" & $stamp & "_" & $getCurrentProcessId() & "_" & $attempt)
    if not dirExists(candidate):
      createDir(candidate)
      return candidate
    inc attempt

proc setupKv(totalEvents: int; extraSignatures: int): tuple[kv: PebbleKV, path: string] =
  let root = makeTempDir()
  let kv = newPebbleKV(root)
  for i in 0 ..< totalEvents:
    kv.put(ksEvent, "evt-" & $i, "payload-" & $i)
  for i in 0 ..< extraSignatures:
    kv.put(ksSignature, "sig-" & $i, "sigval-" & $i)
  (kv: kv, path: root)

suite "PebbleKV prefix iteration":
  test "scanPrefix paginates >10K entries with consistent ordering":
    let ctx = setupKv(12000, 32)
    let kv = ctx.kv
    try:
      let page1 = kv.scanPrefix(ksEvent, prefix = "evt-", startAfter = "", limit = 1000)
      check page1.entries.len == 1000
      check page1.hasMore
      check page1.entries[0].id == "evt-0"
      check page1.entries[^1].id == "evt-999"
      check page1.nextCursor.isSome()
      var prev = ""
      for pair in page1.entries:
        if prev.len > 0:
          check prev <= pair.id
        prev = pair.id
        let parts = pair.id.split('-')
        check parts.len == 2
        let idx = parts[1].parseInt()
        check pair.value == "payload-" & $idx
      let cursor = page1.nextCursor.get()
      let page2 = kv.scanPrefix(ksEvent, prefix = "evt-", startAfter = cursor, limit = 1000, snapshot = page1.snapshot)
      check page2.entries.len == 1000
      check page2.entries[0].id == "evt-1000"
      check page2.entries[^1].id == "evt-1999"
      check pebble_core.toUint64(page2.snapshot.seq) == pebble_core.toUint64(page1.snapshot.seq)
      let finalPage = kv.scanPrefix(ksEvent, prefix = "evt-", startAfter = "evt-10999", limit = 1000)
      check finalPage.entries.len == 1000
      check not finalPage.hasMore
      check finalPage.nextCursor.isNone()
      check finalPage.entries[0].id == "evt-11000"
      check finalPage.entries[^1].id == "evt-11999"
    finally:
      kv.close()
      removeDir(ctx.path)

  test "seek returns exact and next-greater matches within keyspace":
    let ctx = setupKv(12050, 64)
    let kv = ctx.kv
    try:
      let exact = kv.seek(ksEvent, "evt-5000")
      check exact.isSome()
      check exact.get().id == "evt-5000"
      check exact.get().value == "payload-5000"
      let nextGreater = kv.seek(ksEvent, "evt-5000z")
      check nextGreater.isSome()
      check nextGreater.get().id == "evt-5001"
      let noMatch = kv.seek(ksEvent, "evt-40000")
      check noMatch.isNone()
      let signatureSeek = kv.seek(ksSignature, "sig-50")
      check signatureSeek.isSome()
      check signatureSeek.get().id == "sig-50"
    finally:
      kv.close()
      removeDir(ctx.path)

  test "snapshot iterators remain stable across concurrent writes":
    let ctx = setupKv(12000, 0)
    let kv = ctx.kv
    try:
      let basePage = kv.scanPrefix(ksEvent, prefix = "evt-", limit = 100)
      check basePage.entries.len == 100
      let baseSnapshot = basePage.snapshot
      kv.put(ksEvent, "evt-20000", "payload-20000")
      let stableView = kv.scanPrefix(ksEvent, prefix = "evt-", limit = 0, snapshot = baseSnapshot)
      check stableView.entries.len == 12000
      let freshView = kv.scanPrefix(ksEvent, prefix = "evt-", limit = 0)
      check freshView.entries.len == 12001
      var iter = kv.newSnapshotIterator(ksEvent, prefix = "evt-", snapshot = baseSnapshot)
      check iter.first()
      check iter.key() == "evt-0"
      check iter.value() == "payload-0"
      check iter.seek("evt-0500")
      check iter.key() == "evt-0500"
      check iter.next()
      check iter.key() == "evt-0501"
      check iter.seek("evt-0500", inclusive = false)
      check iter.key() == "evt-0501"
      check not iter.seek("evt-20000")
      var limited = 0
      for _ in kv.snapshotIterator(ksEvent, prefix = "evt-119", limit = 5, snapshot = baseSnapshot):
        inc limited
      check limited == 5
    finally:
      kv.close()
      removeDir(ctx.path)
