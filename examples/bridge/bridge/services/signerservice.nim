import std/strformat
import chronos
import bridge/[config, model, storage]
import bridge/services/coordinator
import bridge/services/zkproof

type
  SignerHooks* = object
    verify*: proc(ev: LockEventMessage): bool {.gcsafe, raises: [].}
    sign*: proc(ev: LockEventMessage; peerId: string): SignatureMessage {.gcsafe, raises: [].}

  SignerService* = ref object
    coord: Coordinator
    store: EventStore
    cfg: BridgeConfig
    hooks: SignerHooks

proc defaultHooks(): SignerHooks =
  result.verify = proc(ev: LockEventMessage): bool {.gcsafe.} =
    if verifyEventProof(ev):
      true
    else:
      ev.eventHash.len > 0 and ev.proofBlob.len > 0
  result.sign = proc(ev: LockEventMessage; peerId: string): SignatureMessage {.gcsafe.} =
    let digestSource =
      if ev.proofDigest.len > 0:
        ev.proofDigest
      else:
        shortDigest(ev.proofBlob)
    SignatureMessage(
      eventId: ev.eventId,
      signerPeer: peerId,
      proofKey: ev.proofKey,
      proofDigest: digestSource,
      signature: peerId & "-sig-" & ev.eventId,
    )

proc newSignerService*(coord: Coordinator; store: EventStore; cfg: BridgeConfig;
                       hooks: SignerHooks = defaultHooks()): SignerService =
  SignerService(coord: coord, store: store, cfg: cfg, hooks: hooks)

proc handleEvent*(svc: SignerService; ev: LockEventMessage) {.async.} =
  if not svc.hooks.verify(ev):
    echo fmt"[sign] reject {ev.eventId}"
    return
  let signature = svc.hooks.sign(ev, svc.coord.localPeerId())
  try:
    svc.store.appendSignature(ev.eventId, signature)
  except CatchableError:
    echo fmt"[store] failed to append sig for {ev.eventId}"
  let sent = await svc.coord.publishSignature(signature)
  echo fmt"[sign] event {ev.eventId} digest={signature.proofDigest} sent={sent}"
