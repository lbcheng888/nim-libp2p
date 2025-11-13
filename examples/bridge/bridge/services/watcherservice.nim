import std/[json, sequtils, strformat, tables]
import chronos
import libp2p/crypto/crypto
import stew/byteutils
import bridge/[config, model, storage]
import bridge/services/coordinator
import bridge/services/zkproof

const MinIntervalMs = 100

type
  WatcherService* = ref object
    coord: Coordinator
    store: EventStore
    cfg: BridgeConfig
    rng: ref HmacDrbgContext
    counter: int

proc newWatcherService*(coord: Coordinator; store: EventStore;
                        cfg: BridgeConfig; rng: ref HmacDrbgContext): WatcherService =
  WatcherService(coord: coord, store: store, cfg: cfg, rng: rng, counter: 0)

proc randIndex(svc: WatcherService; upper: int): int =
  if upper <= 0:
    return 0
  int(svc.rng[].generate(uint32) mod uint32(upper))

proc selectAsset(svc: WatcherService): string =
  if svc.cfg.assets.len == 0:
    return "ETH"
  svc.cfg.assets[svc.randIndex(svc.cfg.assets.len)]

proc selectChains(svc: WatcherService): (string, string) =
  if svc.cfg.chains.len < 2:
    return ("chainA", "chainB")
  let first = svc.randIndex(svc.cfg.chains.len)
  let offset = int(svc.rng[].generate(uint32) mod uint32(max(svc.cfg.chains.len - 1, 1))) + 1
  let second = (first + offset) mod svc.cfg.chains.len
  (svc.cfg.chains[first], svc.cfg.chains[second])

proc persistEvent(store: EventStore; ev: LockEventMessage) {.raises: [].} =
  try:
    store.put(
      EventRecord(
        event: ev,
        status: esPending,
        signatures: initOrderedTable[string, SignatureMessage](),
        updatedAt: nowEpoch(),
      ),
    )
  except Exception as exc:
    echo fmt"[store] failed to persist event {ev.eventId}: {exc.msg}"

proc nextEvent(svc: WatcherService): LockEventMessage =
  let (src, dst) = svc.selectChains()
  let idx = svc.counter
  let key = svc.coord.localPeerId() & "-proof-" & $idx
  let proof = byteutils.toHex(("proof:" & key & ":" & $svc.rng[].generate(uint64)).toBytes())
  var ev = LockEventMessage(
    eventId: svc.coord.localPeerId() & "-evt-" & $idx,
    watcherPeer: svc.coord.localPeerId(),
    schemaVersion: EventSchemaVersion,
    asset: svc.selectAsset(),
    amount: 0.5 + float(svc.rng[].generate(uint16) mod 200) / 50.0,
    sourceChain: src,
    targetChain: dst,
    sourceHeight: int64(svc.rng[].generate(uint64)),
    targetHeight: int64(svc.rng[].generate(uint64)),
    proofKey: key,
    proofBlob: "",
    proofDigest: "",
    eventHash: "",
  )
  let payload = %*{"rawProof": proof, "counter": idx, "watcher": ev.watcherPeer}
  discard attachProof(ev, payload)
  ev.eventHash = computeEventHash(ev)
  ev

proc start*(svc: WatcherService) {.async.} =
  let delay = chronos.milliseconds(max(svc.cfg.intervalMs, MinIntervalMs))
  while true:
    inc svc.counter
    let ev = svc.nextEvent()
    persistEvent(svc.store, ev)
    let sent = await svc.coord.publishEvent(ev)
    echo fmt"[emit] event {ev.eventId} -> peers={sent}"
    await sleepAsync(delay)
