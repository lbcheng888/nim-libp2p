## Simplified bridge demo using nim-libp2p GossipSub for bridge coordination.
## Watchers emit lock events with embedded proofs, signers broadcast aggregated signatures.

when not defined(bridge_disable_pebble):
  {.define: bridge_disable_pebble.}

import std/[options, sequtils, strformat, strutils, tables]
import json
import chronos
import libp2p/crypto/crypto
import stew/byteutils
import bridge/config
import bridge/model
import bridge/storage
import bridge/services/coordinator
import bridge/services/watcherservice
import bridge/services/signerservice
import bridge/services/executorservice
import bridge/services/bscwatcher
import bridge/services/zkproof
import bridge/executor/btcsettlement

type
  BridgeNode = ref object
    coord: Coordinator
    rng: ref HmacDrbgContext
    cfg: BridgeConfig
    store: EventStore
    watcher: WatcherService
    bscWatcher: BscWatcher
    signer: SignerService
    executor: ExecutorService

proc onEvent(node: BridgeNode, ev: LockEventMessage): Future[void] {.async.}
proc onSignature(node: BridgeNode, sig: SignatureMessage): Future[void] {.async.}

proc selectAsset(node: BridgeNode): string =
  if node.cfg.assets.len == 0:
    return "ETH"
  let idx = int(node.rng[].generate(uint32) mod uint32(node.cfg.assets.len))
  node.cfg.assets[idx]

proc selectChains(node: BridgeNode): (string, string) =
  if node.cfg.chains.len < 2:
    return ("chainA", "chainB")
  let first = int(node.rng[].generate(uint32) mod uint32(node.cfg.chains.len))
  var offset = int(node.rng[].generate(uint32) mod uint32(node.cfg.chains.len - 1)) + 1
  let second = (first + offset) mod node.cfg.chains.len
  (node.cfg.chains[first], node.cfg.chains[second])

proc nextEvent(node: BridgeNode, counter: int): LockEventMessage =
  let (src, dst) = node.selectChains()
  let key = node.coord.localPeerId() & "-proof-" & $counter
  let proof = byteutils.toHex(
    ("proof:" & key & ":" & $node.rng[].generate(uint64)).toBytes()
  )
  var ev = LockEventMessage(
    eventId: node.coord.localPeerId() & "-evt-" & $counter,
    watcherPeer: node.coord.localPeerId(),
    schemaVersion: EventSchemaVersion,
    asset: node.selectAsset(),
    amount: 0.5 + float(node.rng[].generate(uint16) mod 200) / 50.0,
    sourceChain: src,
    targetChain: dst,
    sourceHeight: node.rng[].generate(uint64).int64,
    targetHeight: node.rng[].generate(uint64).int64,
    proofKey: key,
    proofBlob: "",
    proofDigest: "",
    eventHash: "",
  )
  let payload = %*{"rawProof": proof, "counter": counter}
  discard attachProof(ev, payload)
  ev.eventHash = computeEventHash(ev)
  result = ev

proc startNode(cfg: BridgeConfig): Future[BridgeNode] {.async.} =
  let rng = newRng()
  let coord = await startCoordinator(cfg)

  var node = BridgeNode(
    coord: coord,
    rng: rng,
    cfg: cfg,
    store: initStore(cfg.storageBackend, cfg.storagePath),
    watcher: nil,
    bscWatcher: nil,
    signer: nil,
    executor: nil,
  )

  if cfg.mode == bmWatcher:
    node.watcher = newWatcherService(coord, node.store, cfg, rng)
    let bscOpt = newBscWatcher(coord, node.store, cfg)
    if bscOpt.isSome():
      node.bscWatcher = bscOpt.get()
  if cfg.mode == bmSigner:
    node.signer = newSignerService(coord, node.store, cfg)
  if cfg.mode == bmExecutor or cfg.mode == bmExecutor:
    let hooks =
      if cfg.btcRpcUrl.len > 0:
        newBridgeExecutorHooks(cfg)
      else:
        defaultHooks()
    node.executor = newExecutorService(coord, node.store, cfg, hooks = hooks)

  node.coord.addEventHandler(
    proc(ev: LockEventMessage) {.async.} =
      await node.onEvent(ev)
  )
  node.coord.addSignatureHandler(
    proc(sig: SignatureMessage) {.async.} =
      await node.onSignature(sig)
  )

  result = node

proc persistEvent(node: BridgeNode, ev: LockEventMessage) {.gcsafe, raises: [].} =
  try:
    node.store.put(
      EventRecord(
        event: ev,
        status: esPending,
        signatures: initOrderedTable[string, SignatureMessage](),
        updatedAt: nowEpoch(),
      )
    )
  except CatchableError:
    echo fmt"[store] failed to persist event {ev.eventId}"

proc onEvent(node: BridgeNode, ev: LockEventMessage) {.async, gcsafe.} =
  echo fmt"[event] {ev.eventId} {ev.asset} amount={ev.amount:.4f} {ev.sourceChain}->{ev.targetChain} watcher={ev.watcherPeer}"
  node.persistEvent(ev)

  if node.signer != nil:
    await node.signer.handleEvent(ev)
  if node.executor != nil:
    node.executor.processOnce()

proc onSignature(node: BridgeNode, sig: SignatureMessage) {.async, gcsafe.} =
  try:
    node.store.appendSignature(sig.eventId, sig)
  except CatchableError:
    echo fmt"[store] failed to append signature from {sig.signerPeer}"
  echo fmt"[execute] event {sig.eventId} signer={sig.signerPeer} digest={sig.proofDigest} signature={sig.signature}"
  if node.executor != nil:
    node.executor.processOnce()

proc runForever() {.async.} =
  while true:
    await sleepAsync(1.minutes)

proc main() {.async.} =
  let cfg = parseCli()
  let node = await startNode(cfg)
  if node.cfg.mode == bmWatcher:
    if not node.watcher.isNil():
      asyncSpawn node.watcher.start()
    if not node.bscWatcher.isNil():
      asyncSpawn node.bscWatcher.start()
    if node.watcher.isNil() and node.bscWatcher.isNil():
      echo "[watcher] service unavailable"
  if node.executor != nil:
    node.executor.processOnce()

  await runForever()

when isMainModule:
  waitFor(main())
