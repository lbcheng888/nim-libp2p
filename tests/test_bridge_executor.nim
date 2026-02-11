import std/[json, options, sequtils, strformat, tables, times]

import bridge/config as bridge_config
import bridge/model as bridge_model
import bridge/services/executorservice as executor_service
import bridge/services/zkproof
import bridge/storage as bridge_storage

proc stitchProof(ev: var bridge_model.LockEventMessage): ZkProofEnvelope =
  attachProof(ev, %*{"btcAddress": "tb1qsampler", "amountUsdc": ev.amount})

proc sampleRecord(id: string): bridge_model.EventRecord =
  var ev = bridge_model.LockEventMessage(
    schemaVersion: bridge_model.EventSchemaVersion,
    eventId: id,
    watcherPeer: "watcher-1",
    asset: "ETH",
    amount: 1.0,
    sourceChain: "chainA",
    targetChain: "chainB",
    sourceHeight: 1,
    targetHeight: 2,
    proofKey: id & "-proof",
    proofBlob: "",
    proofDigest: "",
    eventHash: "",
  )
  discard stitchProof(ev)
  ev.eventHash = bridge_model.computeEventHash(ev)
  bridge_model.EventRecord(
    event: ev,
    status: bridge_model.esPending,
    signatures: initOrderedTable[string, bridge_model.SignatureMessage](),
    updatedAt: bridge_model.nowEpoch(),
  )

proc sampleSignature(eventId, signer, digest: string): bridge_model.SignatureMessage =
  bridge_model.SignatureMessage(
    eventId: eventId,
    signerPeer: signer,
    proofKey: eventId & "-proof",
    proofDigest: digest,
    signature: signer & "-sig-" & eventId,
  )

proc baseConfig(threshold = 1): bridge_config.BridgeConfig =
  var cfg = bridge_config.defaultConfig()
  cfg.mode = bridge_model.bmExecutor
  cfg.storageBackend = bridge_storage.sbInMemory
  cfg.signatureThreshold = threshold
  cfg.executorIntervalMs = 200
  cfg.executorBatchSize = 8
  cfg.executorTaskVisibilityMs = bridge_storage.DefaultVisibilityMs
  cfg.executorTaskMaxAttempts = bridge_storage.DefaultMaxAttempts
  cfg

proc testFinalizeOnce() =
  var store = bridge_storage.initStore(bridge_storage.sbInMemory, "")
  defer:
    store.close()
  var rec = sampleRecord("evt-exec-1")
  store.put(rec)
  store.appendSignature(
    rec.event.eventId,
    sampleSignature(rec.event.eventId, "signer-1", rec.event.proofDigest)
  )
  var finalized: seq[string] = @[]
  let hooks = executor_service.ExecutorHooks(
    finalize: proc(ev: bridge_model.LockEventMessage; sigs: seq[bridge_model.SignatureMessage]) {.closure, raises: [].} =
      finalized.add(ev.eventId)
      doAssert sigs.len == 1
  )
  let cfg = baseConfig(threshold = 1)
  let svc = executor_service.newExecutorService(nil, store, cfg, hooks = hooks)
  svc.processOnce()
  doAssert finalized == @["evt-exec-1"]
  let stored = store.getRecord("evt-exec-1")
  doAssert stored.isSome()
  doAssert stored.get().status == bridge_model.esExecuted

proc testBelowThreshold() =
  var store = bridge_storage.initStore(bridge_storage.sbInMemory, "")
  defer:
    store.close()
  var rec = sampleRecord("evt-exec-2")
  store.put(rec)
  store.appendSignature(
    rec.event.eventId,
    sampleSignature(rec.event.eventId, "signer-1", rec.event.proofDigest)
  )
  var finalized: seq[string] = @[]
  let hooks = executor_service.ExecutorHooks(
    finalize: proc(ev: bridge_model.LockEventMessage; sigs: seq[bridge_model.SignatureMessage]) {.closure, raises: [].} =
      finalized.add(ev.eventId)
  )
  var cfg = baseConfig(threshold = 2)
  let svc = executor_service.newExecutorService(nil, store, cfg, hooks = hooks)
  svc.processOnce()
  doAssert finalized.len == 0
  let stored = store.getRecord("evt-exec-2")
  doAssert stored.isSome()
  doAssert stored.get().status == bridge_model.esPending

when isMainModule:
  testFinalizeOnce()
  testBelowThreshold()
