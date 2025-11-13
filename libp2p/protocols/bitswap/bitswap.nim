# Nim-LibP2P
# Copyright (c)
# Licensed under either of
#  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
#  * MIT license ([LICENSE-MIT](LICENSE-MIT))
# at your option.
# This file may not be copied, modified, or distributed except according to
# those terms.

{.push raises: [].}

import std/[options]
import chronos, chronicles

import ../../stream/connection
import ../../protocols/protocol
import ../../cid
import ../../vbuffer
import ../../peerid
import ./protobuf
import ./store
import ./ledger

const
  BitswapCodec* = "/ipfs/bitswap/1.2.0"
  DefaultMaxMessageBytes* = 4 * 1024 * 1024

logScope:
  topics = "libp2p bitswap"

type
  BitswapBlockProvider* = proc(cid: Cid): Future[Option[seq[byte]]] {.
    gcsafe, raises: []
  .}

  BitswapConfig* = object
    maxMessageBytes*: int
    sendDontHave*: bool
    advertiseHave*: bool
    enableLedger*: bool

  BitswapService* = ref object of LPProtocol
    provider*: BitswapBlockProvider
    config*: BitswapConfig
    store*: BitswapBlockStore
    ledger*: BitswapLedger

proc init*(
    _: type BitswapConfig,
    maxMessageBytes: int = DefaultMaxMessageBytes,
    sendDontHave: bool = true,
    advertiseHave: bool = true,
    enableLedger: bool = true,
): BitswapConfig =
  BitswapConfig(
    maxMessageBytes: maxMessageBytes,
    sendDontHave: sendDontHave,
    advertiseHave: advertiseHave,
    enableLedger: enableLedger,
  )

proc validate(config: BitswapConfig) =
  if config.maxMessageBytes <= 0:
    raise newException(Defect, "bitswap maxMessageBytes must be positive")

proc cidPrefix(cid: Cid): Option[seq[byte]] =
  let mhRes = cid.mhash()
  if mhRes.isErr:
    return none(seq[byte])
  let mh = mhRes.get()
  var prefixBuf = initVBuffer()

  if cid.version() == CIDv1:
    prefixBuf.writeVarint(1'u64)
    prefixBuf.writeVarint(uint64(int(cid.mcodec)))

  if mh.dpos > 0:
    prefixBuf.writeArray(mh.data.buffer.toOpenArray(0, mh.dpos - 1))

  prefixBuf.finish()
  some(prefixBuf.buffer)

proc new*(
    T: type BitswapService,
    provider: BitswapBlockProvider = nil,
    config: BitswapConfig = BitswapConfig.init(),
    store: BitswapBlockStore = nil,
    ledger: BitswapLedger = nil,
): T {.public.} =
  if provider.isNil and store.isNil:
    raise newException(Defect, "bitswap requires provider or store")
  validate(config)
  let ledgerInst =
    if not config.enableLedger:
      nil
    elif not ledger.isNil:
      ledger
    else:
      BitswapLedger.new()
  let svc = BitswapService(
    provider: provider, config: config, store: store, ledger: ledgerInst
  )
  svc.init()
  svc

proc ledger*(svc: BitswapService): BitswapLedger =
  svc.ledger

proc buildResponse(
    svc: BitswapService, peerId: PeerId, wantlist: Wantlist
): Future[Option[(BitswapMessage, BitswapLedgerDelta)]] {.async.} =
  var response = BitswapMessage(
    wantlist: none(Wantlist),
    blocks: @[],
    payloads: @[],
    blockPresences: @[],
    pendingBytes: none(int32),
  )
  var hasData = false
  var delta = BitswapLedgerDelta()

  if not svc.ledger.isNil and peerId.len > 0 and wantlist.entries.len > 0:
    await svc.ledger.recordWants(
      peerId, wantlist.entries.len, BitswapLedgerDirection.bldReceived
    )

  for entry in wantlist.entries:
    if entry.cancel:
      continue

    var blockDataOpt: Option[seq[byte]] = none(seq[byte])
    try:
      if not svc.provider.isNil():
        blockDataOpt = await svc.provider(entry.cid)
      elif not svc.store.isNil():
        blockDataOpt = await svc.store.getBlock(entry.cid)
    except CatchableError as exc:
      trace "bitswap provider raised", cid = $entry.cid, err = exc.msg
      continue

    case entry.wantType
    of WantType.wtHave:
      let hasBlock = blockDataOpt.isSome
      if hasBlock and svc.config.advertiseHave:
        response.blockPresences.add(
          BlockPresence(cid: entry.cid, presenceType: BlockPresenceType.bpHave)
        )
        delta.haveCount.inc()
        hasData = true
      elif (not hasBlock) and entry.sendDontHave and svc.config.sendDontHave:
        response.blockPresences.add(
          BlockPresence(cid: entry.cid, presenceType: BlockPresenceType.bpDontHave)
        )
        delta.dontHaveCount.inc()
        hasData = true
    of WantType.wtBlock:
      if blockDataOpt.isSome:
        let blockData = blockDataOpt.get()
        response.blocks.add(blockData)
        let prefixOpt = cidPrefix(entry.cid)
        prefixOpt.withValue(prefix):
          response.payloads.add(BlockPayload(prefix: prefix, data: blockData))
        response.blockPresences.add(
          BlockPresence(cid: entry.cid, presenceType: BlockPresenceType.bpHave)
        )
        delta.blocks.inc()
        delta.bytes += uint64(blockData.len)
        delta.haveCount.inc()
        hasData = true
      elif entry.sendDontHave and svc.config.sendDontHave:
        response.blockPresences.add(
          BlockPresence(cid: entry.cid, presenceType: BlockPresenceType.bpDontHave)
        )
        delta.dontHaveCount.inc()
        hasData = true

  if hasData:
    some((response, delta))
  else:
    none((BitswapMessage, BitswapLedgerDelta))

method init*(svc: BitswapService) =
  proc handle(conn: Connection, proto: string) {.async: (raises: [CancelledError]).} =
    trace "handling bitswap request", conn
    try:
      let payload = await conn.readLp(svc.config.maxMessageBytes)
      let msgOpt = decodeBitswapMessage(payload)
      if msgOpt.isNone():
        trace "failed decoding bitswap message", conn
        return

      let msg = msgOpt.get()
      if msg.wantlist.isNone():
        trace "bitswap message without wantlist", conn
        return

      let wantlist = msg.wantlist.get()
      let respOpt = await svc.buildResponse(conn.peerId, wantlist)
      respOpt.withValue(respData):
        let (resp, delta) = respData
        let encoded = encode(resp)
        try:
          await conn.writeLp(encoded)
          if not svc.ledger.isNil and conn.peerId.len > 0:
            await svc.ledger.recordDelta(
              conn.peerId, delta, BitswapLedgerDirection.bldSent
            )
        except LPStreamError as exc:
          trace "failed sending bitswap response", conn, err = exc.msg
    except CancelledError as exc:
      trace "bitswap handler cancelled", conn
      raise exc
    except LPStreamLimitError as exc:
      trace "bitswap read limit exceeded", conn, err = exc.msg
    except LPStreamError as exc:
      trace "bitswap stream error", conn, err = exc.msg
    except CatchableError as exc:
      trace "bitswap handler error", conn, err = exc.msg
    finally:
      await conn.close()

  svc.handler = handle
  svc.codec = BitswapCodec

{.pop.}
