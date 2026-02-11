import std/[json, options, os, sequtils, strformat, strutils, tables, times]
import unittest

import bridge/model
import bridge/services/zkproof
import bridge/storage
import pebble/core/types as pebble_core

proc sampleRecord(id: string): EventRecord =
  var ev = LockEventMessage(
    schemaVersion: EventSchemaVersion,
    eventId: id,
    watcherPeer: "watcher-1",
    asset: "ETH",
    amount: 42.0,
    sourceChain: "chainA",
    targetChain: "chainB",
    sourceHeight: 1,
    targetHeight: 2,
    proofKey: id & "-proof",
    proofBlob: "",
    proofDigest: "",
    eventHash: "",
  )
  discard attachProof(ev, %*{"btcAddress": "tb1qdemo", "amountUsdc": ev.amount})
  ev.eventHash = computeEventHash(ev)
  EventRecord(
    event: ev,
    status: esPending,
    signatures: initOrderedTable[string, SignatureMessage](),
    updatedAt: nowEpoch(),
  )

suite "bridge storage pebble batch":
  test "pebble batch commits event and index together":
    let tempDir = getTempDir() / ("bridge_pebble_" & $nowEpoch())
    var store = initStore(sbPebble, tempDir)
    defer:
      store.close()

    var rec = sampleRecord("evt-1")
    store.put(rec)
    check store.listPending().len == 1

    # idempotent put should not duplicate pending index
    store.put(rec)
    check store.listPending().len == 1

    let sig = SignatureMessage(
      eventId: rec.event.eventId,
      signerPeer: "signer-1",
      proofKey: rec.event.proofKey,
      proofDigest: rec.event.proofDigest,
      signature: "sig-1",
    )
    store.appendSignature(rec.event.eventId, sig)

    var stored = store.getRecord(rec.event.eventId)
    check stored.isSome()
    check stored.get().signatures.len == 1

    # duplicate signature from same signer should be merged
    store.appendSignature(rec.event.eventId, sig)
    stored = store.getRecord(rec.event.eventId)
    check stored.isSome()
    check stored.get().signatures.len == 1

    store.updateStatus(rec.event.eventId, esExecuted)
    check store.listPending().len == 0

    store.close()

    var reopened = initStore(sbPebble, tempDir)
    defer:
      reopened.close()

    let persisted = reopened.getRecord(rec.event.eventId)
    check persisted.isSome()
    check persisted.get().status == esExecuted
    check reopened.listPending().len == 0

  test "pebble pending scan paginates via prefix index":
    let tempDir = getTempDir() / ("bridge_pebble_scan_" & $nowEpoch())
    var store = initStore(sbPebble, tempDir)
    defer:
      store.close()

    for i in 0 ..< 5:
      var rec = sampleRecord("evt-scan-" & $i)
      store.put(rec)

    let page1 = store.scanPending(limit = 2)
    check page1.events.len == 2
    check page1.hasMore
    check page1.nextCursor.isSome()
    check page1.snapshot.isSome()

    let cursor = page1.nextCursor.get()
    let snap = page1.snapshot
    let page2 = store.scanPending(startAfter = cursor, limit = 2, snapshot = snap)
    check page2.events.len == 2
    check page2.snapshot.isSome()
    check pebble_core.toUint64(page2.snapshot.get().seq) == pebble_core.toUint64(page1.snapshot.get().seq)

    store.updateStatus(page1.events[0].event.eventId, esExecuted)
    let refreshed = store.scanPending(limit = 0)
    check refreshed.events.len == 4

  test "captured event logs replay to fresh store":
    let baseDir = getTempDir() / ("bridge_event_logs_" & $nowEpoch())
    var store = initStore(sbPebble, baseDir)
    defer:
      store.close()

    var rec = sampleRecord("evt-log-1")
    let putLog = store.putWithLog(rec)
    check putLog.len > 0

    let sig = SignatureMessage(
      eventId: rec.event.eventId,
      signerPeer: "signer-log",
      proofKey: rec.event.proofKey,
      proofDigest: rec.event.proofDigest,
      signature: "sig-log"
    )
    let sigLog = store.appendSignatureWithLog(rec.event.eventId, sig)
    check sigLog.len > 0

    let statusLog = store.updateStatusWithLog(rec.event.eventId, esExecuted)
    check statusLog.len > 0

    var replayStore = initStore(sbPebble, baseDir & "_replay")
    defer:
      replayStore.close()

    let putSeq = replayStore.replayWrites(putLog, dedupe = false)
    check pebble_core.toUint64(putSeq) > 0
    let sigSeq = replayStore.replayWrites(sigLog, dedupe = false)
    check pebble_core.toUint64(sigSeq) > 0
    let statusSeq = replayStore.replayWrites(statusLog, dedupe = false)
    check pebble_core.toUint64(statusSeq) > 0

    let restored = replayStore.getRecord(rec.event.eventId)
    check restored.isSome()
    let replayed = restored.get()
    check replayed.status == esExecuted
    check replayed.signatures.len == 1
    check replayed.signatures.hasKey("signer-log")
    check replayStore.listPending().len == 0

proc tempTaskDir(tag: string): string =
  getTempDir() / ("bridge_tasks_" & tag & "_" & $nowEpoch())

proc currentMs(): int64 =
  (epochTime() * 1000.0).int64

suite "bridge storage task queue":
  test "enqueue ack persists across restarts":
    let tempDir = tempTaskDir("persist")
    var store = initStore(sbPebble, tempDir)
    defer:
      if not store.isNil():
        store.close()

    let payload = %* {"kind": "demo"}
    let task = store.enqueueTask(payload)

    var leases = store.dequeueTasks(1, visibilityTimeoutMs = 20)
    check leases.len == 1
    check leases[0].taskId == task.taskId
    check store.ackTask(task.taskId, leases[0].receipt)

    var stored = store.getTask(task.taskId)
    check stored.isSome()
    check stored.get().status == tsDone
    check stored.get().payload == payload

    store.close()
    store = nil

    var reopened = initStore(sbPebble, tempDir)
    defer:
      reopened.close()

    stored = reopened.getTask(task.taskId)
    check stored.isSome()
    check stored.get().status == tsDone
    check stored.get().payload == payload

  test "task status index tracks lifecycle transitions":
    let tempDir = tempTaskDir("status_index")
    var store = initStore(sbPebble, tempDir)
    defer:
      store.close()

    let t1 = store.enqueueTask(%* {"job": "idx-1"})
    let t2 = store.enqueueTask(%* {"job": "idx-2"}, delayMs = 60)

    var readyPage = store.scanTasks(tsReady)
    check readyPage.tasks.len == 2
    var readyIds = readyPage.tasks.mapIt(it.taskId)
    check readyIds.len == 2
    check readyIds[0] == t1.taskId
    check readyIds.find(t2.taskId) != -1

    let leases = store.dequeueTasks(1, visibilityTimeoutMs = 40)
    check leases.len == 1
    check leases[0].taskId == t1.taskId

    var runningPage = store.scanTasks(tsRunning)
    check runningPage.tasks.len == 1
    check runningPage.tasks[0].taskId == t1.taskId

    readyPage = store.scanTasks(tsReady)
    check readyPage.tasks.len == 1
    check readyPage.tasks[0].taskId == t2.taskId

    check store.scanTasks(tsDone).tasks.len == 0

    check store.ackTask(t1.taskId, leases[0].receipt)

    var donePage = store.scanTasks(tsDone)
    check donePage.tasks.len == 1
    check donePage.tasks[0].taskId == t1.taskId

    check store.scanTasks(tsRunning).tasks.len == 0

    let failTask = store.enqueueTask(%* {"job": "idx-fail"}, maxAttempts = 1)
    var failLease = store.dequeueTasks(1, visibilityTimeoutMs = 5)
    check failLease.len == 1
    check failLease[0].taskId == failTask.taskId

    sleep(30)
    discard store.dequeueTasks(1, visibilityTimeoutMs = 5)

    let failedRec = store.getTask(failTask.taskId)
    check failedRec.isSome()
    check failedRec.get().status == tsFailed

    let failedPage = store.scanTasks(tsFailed)
    check failedPage.tasks.len >= 1
    check failedPage.tasks.mapIt(it.taskId).find(failTask.taskId) != -1

  test "visibility timeout reschedules and fails after max attempts":
    let tempDir = tempTaskDir("auto_retry")
    var store = initStore(sbPebble, tempDir)
    defer:
      store.close()

    let payload = %* {"job": 1}
    let task = store.enqueueTask(payload, maxAttempts = 2)

    var leases = store.dequeueTasks(1, visibilityTimeoutMs = 10)
    check leases.len == 1
    check leases[0].taskId == task.taskId

    sleep(35)

    leases = store.dequeueTasks(1, visibilityTimeoutMs = 10)
    check leases.len == 1
    check leases[0].taskId == task.taskId
    check leases[0].attempts == 2

    sleep(35)

    leases = store.dequeueTasks(1, visibilityTimeoutMs = 10)
    check leases.len == 0

    let failed = store.getTask(task.taskId)
    check failed.isSome()
    check failed.get().status == tsFailed
    check failed.get().attempts == 2
    check failed.get().lastError == "max attempts exceeded"

  test "manual retry resets attempts and enforces delay":
    let tempDir = tempTaskDir("manual_retry")
    var store = initStore(sbPebble, tempDir)
    defer:
      store.close()

    let task = store.enqueueTask(%* {"job": "x"}, maxAttempts = 1)

    var leases = store.dequeueTasks(1, visibilityTimeoutMs = 10)
    check leases.len == 1
    check leases[0].taskId == task.taskId

    sleep(30)
    discard store.dequeueTasks(1, visibilityTimeoutMs = 10)

    var recOpt = store.getTask(task.taskId)
    check recOpt.isSome()
    check recOpt.get().status == tsFailed

    let ok = store.retryTask(task.taskId, reason = "manual retry", resetAttempts = true, delayMs = 80)
    check ok

    recOpt = store.getTask(task.taskId)
    check recOpt.isSome()
    var rec = recOpt.get()
    check rec.status == tsReady
    check rec.attempts == 0
    check rec.lastError == "manual retry"
    let visibleAt = rec.visibleAtMs
    let nowVal = currentMs()
    check visibleAt >= nowVal + 50

    leases = store.dequeueTasks(1, visibilityTimeoutMs = 15)
    check leases.len == 0

    sleep(120)

    leases = store.dequeueTasks(1, visibilityTimeoutMs = 15)
    check leases.len == 1
    check leases[0].taskId == task.taskId
    let receipt = leases[0].receipt
    check store.ackTask(task.taskId, receipt)

    recOpt = store.getTask(task.taskId)
    check recOpt.isSome()
    check recOpt.get().status == tsDone

  test "bulk enqueue/dequeue handles burst load and restart recovery":
    let tempDir = tempTaskDir("bulk_load")
    var store = initStore(sbPebble, tempDir)
    defer:
      if not store.isNil():
        store.close()

    let total = 120
    for i in 0 ..< total:
      discard store.enqueueTask(%* {"batch": i})

    var processed: seq[string] = @[]
    var rounds = 0

    while processed.len < total and rounds < total * 4:
      let leases = store.dequeueTasks(16, visibilityTimeoutMs = 25)
      if leases.len == 0:
        sleep(5)
      else:
        for lease in leases:
          check processed.find(lease.taskId) == -1
          processed.add(lease.taskId)
          check store.ackTask(lease.taskId, lease.receipt)
      inc rounds

    check processed.len == total

    for id in processed:
      let recOpt = store.getTask(id)
      check recOpt.isSome()
      check recOpt.get().status == tsDone

    store.close()
    store = nil

    var reopened = initStore(sbPebble, tempDir)
    defer:
      reopened.close()

    for id in processed:
      let recOpt = reopened.getTask(id)
      check recOpt.isSome()
      check recOpt.get().status == tsDone

  test "task mutation logs support idempotent replay":
    let tempDir = tempTaskDir("txn_logs")
    var store = initStore(sbPebble, tempDir)
    defer:
      store.close()

    let payload = %* {"job": "with-log"}
    let (task, enqueueLog) = store.enqueueTaskWithLog(payload)
    check enqueueLog.len > 0

    let (leases, dequeueLog) = store.dequeueTasksWithLog(1, visibilityTimeoutMs = 40)
    check leases.len == 1
    check dequeueLog.len > 0
    let receipt = leases[0].receipt

    let (acked, ackLog) = store.ackTaskWithLog(task.taskId, receipt)
    check acked
    check ackLog.len > 0

    let replayDir = tempTaskDir("txn_logs_replay")
    var replayStore = initStore(sbPebble, replayDir)
    defer:
      replayStore.close()

    let enqueueSeq = replayStore.replayWrites(enqueueLog, dedupe = false)
    check pebble_core.toUint64(enqueueSeq) > 0
    let dequeueSeq = replayStore.replayWrites(dequeueLog, dedupe = false)
    check pebble_core.toUint64(dequeueSeq) > 0
    let ackSeq = replayStore.replayWrites(ackLog, dedupe = false)
    check pebble_core.toUint64(ackSeq) > 0

    let restored = replayStore.getTask(task.taskId)
    check restored.isSome()
    let replayed = restored.get()
    check replayed.status == tsDone
    check replayed.attempts == 1
    check replayed.receipt.len == 0
