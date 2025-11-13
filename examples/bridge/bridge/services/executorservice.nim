import std/[json, options, sequtils, strformat, tables]
import chronos
import nimcrypto/sha2
import stew/byteutils
import ../../../../libp2p/crypto/crypto
import ../../../../libp2p/peerid as lpPeerId
import ../../../../libp2p/peerinfo as lpPeerInfo
import ../../../../libp2p/protocols/onionpay
import bridge/[config, model, storage]
import bridge/services/coordinator

const
  DefaultRetryMs = 3000
  MinSigThreshold = 1

type
  ExecutorHooks* = object
    finalize*: proc(ev: LockEventMessage; sigs: seq[SignatureMessage]) {.closure, raises: [].}

  ExecutorService* = ref object
    coord: Coordinator
    store: EventStore
    cfg: BridgeConfig
    retryMs: int
    hooks: ExecutorHooks
    threshold: int
    intervalMs: int
    batchSize: int

proc defaultHooks*(): ExecutorHooks =
  result.finalize = proc(ev: LockEventMessage; sigs: seq[SignatureMessage]) {.closure, raises: [].} =
    echo fmt"[exec] finalize event {ev.eventId} sigs={sigs.len}"

proc newExecutorService*(
    coord: Coordinator; store: EventStore; cfg: BridgeConfig;
    retryMs = DefaultRetryMs; hooks: ExecutorHooks = defaultHooks()
): ExecutorService =
  var effectiveRetry = retryMs
  if cfg.executorRetryMs > 0:
    effectiveRetry = cfg.executorRetryMs
  ExecutorService(
    coord: coord,
    store: store,
    cfg: cfg,
    retryMs: effectiveRetry,
    hooks: hooks,
    threshold: max(MinSigThreshold, cfg.signatureThreshold),
    intervalMs: max(500, (if cfg.executorIntervalMs > 0: cfg.executorIntervalMs else: effectiveRetry)),
    batchSize: max(1, cfg.executorBatchSize),
  )

proc collectSignatures(rec: EventRecord): seq[SignatureMessage] =
  for key, value in pairs(rec.signatures):
    discard key
    result.add(value)

proc toTaskPayload(rec: EventRecord): JsonNode =
  %*{
    "event": eventToJson(rec.event),
    "signatures": rec.signatures.values.toSeq().mapIt(signatureToJson(it))
  }

proc enqueueFinalizeTask(svc: ExecutorService; rec: EventRecord) =
  let payload = toTaskPayload(rec)
  let (_, log) = svc.store.enqueueTaskWithLog(
    payload,
    delayMs = 0,
    visibilityTimeoutMs = svc.cfg.executorTaskVisibilityMs,
    maxAttempts = svc.cfg.executorTaskMaxAttempts,
    taskId = rec.event.eventId
  )
  if log.len > 0 and svc.cfg.storageBackend == sbPebble:
    discard svc.store.replayWrites(log, dedupe = true)

proc pushPeer[T](peers: var seq[T], peer: T) =
  peers.setLen(peers.len + 1)
  peers[^1] = peer

proc gatherOnionPeers(svc: ExecutorService): seq[string] =
  var peers: seq[string] = @[]
  if svc.coord.isNil() or svc.coord.switch.isNil():
    return peers
  peers.add($svc.coord.switch.peerInfo.peerId)
  for entry in svc.cfg.peers:
    let parsed = lpPeerInfo.parseFullAddress(entry)
    if parsed.isOk():
      peers.add($parsed.get()[0])
  if peers.len == 0:
    let rng = newRng()
    for _ in 0 ..< max(1, svc.cfg.onionDemoHops):
      let pidRes = lpPeerId.PeerId.random(rng)
      if pidRes.isOk():
        peers.add($pidRes.get())
  peers

proc hopSecret(ev: LockEventMessage; idx: int): array[32, byte] =
  let material = (ev.eventId & ":" & $idx & ":" & ev.proofDigest).toBytes()
  let digest = sha256.digest(material)
  for i in 0 ..< result.len:
    result[i] = digest.data[i mod digest.data.len]

proc logOnionDemo(svc: ExecutorService; ev: LockEventMessage; sigs: seq[SignatureMessage]) =
  if not svc.cfg.enableOnionDemo:
    return
  let peerLabels = gatherOnionPeers(svc)
  if peerLabels.len == 0:
    return
  let hopCount = max(2, svc.cfg.onionDemoHops)
  var route = OnionRoute(id: randomOnionRouteId(), hops: @[])
  for i in 0 ..< hopCount:
    let label = peerLabels[i mod peerLabels.len]
    var hopPeer: lpPeerId.PeerId
    let parsed = lpPeerId.PeerId.init(label)
    if parsed.isOk():
      hopPeer = parsed.get()
    else:
      let randPeer = lpPeerId.PeerId.random()
      if randPeer.isErr():
        return
      hopPeer = randPeer.get()
    route.hops.add(OnionRouteHop(peer: hopPeer, secret: hopSecret(ev, i), ttl: uint16(5 + i)))
  let payloadNode = %*{
    "eventId": ev.eventId,
    "sigCount": sigs.len,
    "asset": ev.asset,
    "digest": ev.proofDigest,
  }
  let payloadBytes = ($payloadNode).toBytes()
  let packetRes = buildOnionPacket(route, payloadBytes)
  if packetRes.isErr():
    echo fmt"[executor-onion] build failed event={ev.eventId}"
    return
  var packet = packetRes.get()
  var validRoute = true
  for hop in route.hops:
    let peelRes = peelOnionLayer(packet, hop.secret)
    if peelRes.isErr():
      validRoute = false
      break
    if peelRes.get().isFinal:
      break
  let exitPeer = route.hops[^1].peer
  let digestShort =
    if ev.proofDigest.len > 8: ev.proofDigest[0 ..< 8] else: ev.proofDigest
  echo fmt"[executor-onion] event={ev.eventId} hops={route.hops.len} exit={shortLog(exitPeer)} digest={digestShort} ok={validRoute}"

proc handlePending(svc: ExecutorService; rec: EventRecord) =
  let sigs = collectSignatures(rec)
  if sigs.len < svc.threshold:
    return
  svc.enqueueFinalizeTask(rec)

proc finalizeTask(svc: ExecutorService; lease: TaskLease) =
  if lease.payload.isNil() or not lease.payload.hasKey("event"):
    try:
      discard svc.store.retryTask(
        lease.taskId,
        lease.receipt,
        delayMs = svc.retryMs,
        reason = "missing payload"
      )
    except CatchableError:
      echo fmt"[executor] retryTask failed for {lease.taskId} (missing payload)"
    except Exception as exc:
      echo fmt"[executor] retryTask exception for {lease.taskId}: {exc.msg}"
    return
  let eventOpt = model.eventFromJson(lease.payload["event"])
  if eventOpt.isNone():
    try:
      discard svc.store.retryTask(
        lease.taskId,
        lease.receipt,
        delayMs = svc.retryMs,
        reason = "decode event failed"
      )
    except CatchableError:
      echo fmt"[executor] retryTask decode failed for {lease.taskId}"
    except Exception as exc:
      echo fmt"[executor] retryTask exception for {lease.taskId}: {exc.msg}"
    return
  var sigs: seq[SignatureMessage] = @[]
  if lease.payload.hasKey("signatures") and lease.payload["signatures"].kind == JArray:
    for node in lease.payload["signatures"]:
      let sigOpt = signatureFromJson(node)
      if sigOpt.isSome():
        sigs.add(sigOpt.get())
  try:
    svc.hooks.finalize(eventOpt.get(), sigs)
    svc.logOnionDemo(eventOpt.get(), sigs)
  except CatchableError as exc:
    echo fmt"[executor] finalize失败 event={eventOpt.get().eventId} err={exc.msg}"
    try:
      discard svc.store.retryTask(
        lease.taskId,
        lease.receipt,
        delayMs = svc.retryMs,
        reason = exc.msg
      )
    except CatchableError:
      echo fmt"[executor] retryTask失败 event={eventOpt.get().eventId}"
    except Exception as retryExc:
      echo fmt"[executor] retry exception event={eventOpt.get().eventId}: {retryExc.msg}"
    return
  try:
    discard svc.store.ackTask(lease.taskId, lease.receipt)
    svc.store.updateStatus(eventOpt.get().eventId, esExecuted)
  except CatchableError:
    echo fmt"[executor] failed to ack or update status for task ", lease.taskId

proc pumpTasks(svc: ExecutorService) =
  let leases = svc.store.dequeueTasks(svc.batchSize, visibilityTimeoutMs = svc.cfg.executorTaskVisibilityMs)
  for lease in leases:
    svc.finalizeTask(lease)

proc processOnce*(svc: ExecutorService) =
  ## 单次调度循环，便于测试与可控驱动。
  if svc.isNil() or svc.store.isNil():
    return
  try:
    for rec in svc.store.listPending():
      svc.handlePending(rec)
    svc.pumpTasks()
  except Exception as exc:
    echo "[executor] processOnce 捕获错误: ", exc.msg
