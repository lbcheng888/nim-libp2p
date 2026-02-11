# Nim-LibP2P
# Copyright (c) 2023 Status Research & Development GmbH
# Licensed under either of
#  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
#  * MIT license ([LICENSE-MIT](LICENSE-MIT))
# at your option.
# This file may not be copied, modified, or distributed except according to
# those terms.

## `Identify <https://docs.libp2p.io/concepts/protocols/#identify>`_ and
## `Push Identify <https://docs.libp2p.io/concepts/protocols/#identify-push>`_ implementation

{.push raises: [].}

import std/[sequtils, options, strutils, sugar, tables]
import results, chronos, chronicles
import
  ../protobuf/minprotobuf,
  ../bandwidthmanager,
  ../peerinfo,
  ../stream/connection,
  ../peerid,
  ../crypto/crypto,
  ../multiaddress,
  ../multicodec,
  ../protocols/protocol,
  ../utility,
  ../errors,
  ../observedaddrmanager

export observedaddrmanager

logScope:
  topics = "libp2p identify"

const
  IdentifyCodec* = "/ipfs/id/1.0.0"
  IdentifyPushCodec* = "/ipfs/id/push/1.0.0"
  ProtoVersion* = "ipfs/0.1.0"
  AgentVersion* = "nim-libp2p/0.0.1"
  BandwidthMetadataKey* = "libp2p.bandwidth"

type
  IdentifyError* = object of LPError
  IdentityNoMatchError* = object of IdentifyError
  IdentityInvalidMsgError* = object of IdentifyError
  IdentifyNoPubKeyError* = object of IdentifyError

  IdentifyInfo* {.public.} = object
    pubkey*: Option[PublicKey]
    peerId*: PeerId
    addrs*: seq[MultiAddress]
    observedAddr*: Option[MultiAddress]
    protoVersion*: Option[string]
    agentVersion*: Option[string]
    protos*: seq[string]
    signedPeerRecord*: Option[Envelope]
    metadata*: Table[string, seq[byte]]
    bandwidthAnnouncement*: Opt[BandwidthSnapshot]

  Identify* = ref object of LPProtocol
    peerInfo*: PeerInfo
    sendSignedPeerRecord*: bool
    observedAddrManager*: ObservedAddrManager
    bandwidthManager*: BandwidthManager

  IdentifyPushHandler* = proc(peer: PeerId, newInfo: IdentifyInfo): Future[void] {.
    gcsafe, raises: [], public
  .}

  IdentifyPush* = ref object of LPProtocol
    identifyHandler: IdentifyPushHandler
    bandwidthManager*: BandwidthManager

chronicles.expandIt(IdentifyInfo):
  pubkey = ($it.pubkey).shortLog
  addresses = it.addrs.map(x => $x).join(",")
  protocols = it.protos.map(x => $x).join(",")
  observable_address = $it.observedAddr
  proto_version = it.protoVersion.get("None")
  agent_version = it.agentVersion.get("None")
  signedPeerRecord =
    # The SPR contains the same data as the identify message
    # would be cumbersome to log
    if it.signedPeerRecord.isSome(): "Some" else: "None"
  metadata_keys =
    if it.metadata.len > 0: it.metadata.keys.toSeq().join(",") else: "None"
  bandwidth =
    if it.bandwidthAnnouncement.isSome():
      let snap = it.bandwidthAnnouncement.get()
      "ts=" & $snap.timestamp & ",in=" & $snap.totalInbound &
        ",out=" & $snap.totalOutbound & ",inRate=" & $snap.inboundRateBps &
        ",outRate=" & $snap.outboundRateBps
    else:
      "None"

proc encodeMetadataEntry(key: string, value: openArray[byte]): ProtoBuffer {.raises: [].} =
  var metaPb = initProtoBuffer()
  metaPb.write(1, key)
  metaPb.write(2, value)
  metaPb.finish()
  metaPb

proc encodeBandwidth(snapshot: BandwidthSnapshot): seq[byte] {.raises: [].} =
  var pb = initProtoBuffer()
  pb.write(1, snapshot.timestamp)
  pb.write(2, snapshot.totalInbound)
  pb.write(3, snapshot.totalOutbound)
  pb.write(4, snapshot.inboundRateBps)
  pb.write(5, snapshot.outboundRateBps)
  result = pb.toBytes()

proc decodeMetadataEntry(buf: seq[byte]): Opt[(string, seq[byte])] =
  var metaPb = initProtoBuffer(buf)
  var key: string
  if not metaPb.getField(1, key).valueOr(false):
    return Opt.none((string, seq[byte]))
  var value: seq[byte] = @[]
  let valueRes = metaPb.getField(2, value)
  if valueRes.isErr():
    return Opt.none((string, seq[byte]))
  Opt.some((key, value))

proc decodeBandwidth(data: seq[byte]): Opt[BandwidthSnapshot] =
  var pb = initProtoBuffer(data)
  var snapshot = BandwidthSnapshot()
  if not pb.getField(1, snapshot.timestamp).valueOr(false):
    return Opt.none(BandwidthSnapshot)
  if pb.getField(2, snapshot.totalInbound).isErr():
    return Opt.none(BandwidthSnapshot)
  if pb.getField(3, snapshot.totalOutbound).isErr():
    return Opt.none(BandwidthSnapshot)
  if pb.getField(4, snapshot.inboundRateBps).isErr():
    return Opt.none(BandwidthSnapshot)
  if pb.getField(5, snapshot.outboundRateBps).isErr():
    return Opt.none(BandwidthSnapshot)
  Opt.some(snapshot)

proc collectBandwidthMetadata(manager: BandwidthManager): seq[(string, seq[byte])] =
  if manager.isNil:
    return @[]
  let snapshot = manager.snapshot()
  let encoded = encodeBandwidth(snapshot)
  @[(BandwidthMetadataKey, encoded)]

proc collectMetadata(p: Identify): seq[(string, seq[byte])] =
  collectBandwidthMetadata(p.bandwidthManager)

proc collectMetadata(p: IdentifyPush): seq[(string, seq[byte])] =
  collectBandwidthMetadata(p.bandwidthManager)

proc encodeMsg(
    peerInfo: PeerInfo,
    observedAddr: Opt[MultiAddress],
    sendSpr: bool,
    metadata: seq[(string, seq[byte])] = @[],
): ProtoBuffer {.raises: [].} =
  result = initProtoBuffer()

  let pkey = peerInfo.publicKey

  result.write(1, pkey.getBytes().expect("valid key"))
  for ma in peerInfo.addrs:
    result.write(2, ma.data.buffer)
  for proto in peerInfo.protocols:
    result.write(3, proto)
  observedAddr.withValue(observed):
    result.write(4, observed.data.buffer)
  let protoVersion = ProtoVersion
  result.write(5, protoVersion)
  let agentVersion =
    if peerInfo.agentVersion.len <= 0: AgentVersion else: peerInfo.agentVersion
  result.write(6, agentVersion)

  ## Optionally populate signedPeerRecord field.
  ## See https://github.com/libp2p/go-libp2p/blob/ddf96ce1cfa9e19564feb9bd3e8269958bbc0aba/p2p/protocol/identify/pb/identify.proto for reference.
  if sendSpr:
    peerInfo.signedPeerRecord.envelope.encode().toOpt().withValue(sprBuff):
      result.write(8, sprBuff)

  for (key, value) in metadata:
    let metaPb = encodeMetadataEntry(key, value)
    result.write(9, metaPb)

  result.finish()

proc decodeMsg*(buf: seq[byte]): Opt[IdentifyInfo] =
  var
    iinfo: IdentifyInfo
    pubkey: PublicKey
    oaddr: MultiAddress
    protoVersion: string
    agentVersion: string
    signedPeerRecord: SignedPeerRecord

  iinfo.metadata = initTable[string, seq[byte]]()

  var pb = initProtoBuffer(buf)
  if ?pb.getField(1, pubkey).toOpt():
    iinfo.pubkey = some(pubkey)
    if ?pb.getField(8, signedPeerRecord).toOpt() and
        pubkey == signedPeerRecord.envelope.publicKey:
      iinfo.signedPeerRecord = some(signedPeerRecord.envelope)
  discard ?pb.getRepeatedField(2, iinfo.addrs).toOpt()
  discard ?pb.getRepeatedField(3, iinfo.protos).toOpt()
  if ?pb.getField(4, oaddr).toOpt():
    iinfo.observedAddr = some(oaddr)
  if ?pb.getField(5, protoVersion).toOpt():
    iinfo.protoVersion = some(protoVersion)
  if ?pb.getField(6, agentVersion).toOpt():
    iinfo.agentVersion = some(agentVersion)

  var metadataEntries: seq[seq[byte]]
  discard ?pb.getRepeatedField(9, metadataEntries).toOpt()
  for entry in metadataEntries:
    decodeMetadataEntry(entry).withValue(meta):
      let (key, value) = meta
      iinfo.metadata[key] = value
      if key == BandwidthMetadataKey:
        iinfo.bandwidthAnnouncement = decodeBandwidth(value)

  Opt.some(iinfo)

proc new*(
    T: typedesc[Identify],
    peerInfo: PeerInfo,
    sendSignedPeerRecord = false,
    observedAddrManager = ObservedAddrManager.new(),
    bandwidthManager: BandwidthManager = nil,
): T =
  let identify = T(
    peerInfo: peerInfo,
    sendSignedPeerRecord: sendSignedPeerRecord,
    observedAddrManager: observedAddrManager,
    bandwidthManager: bandwidthManager,
  )
  identify.init()
  identify

method init*(p: Identify) =
  proc handle(conn: Connection, proto: string) {.async: (raises: [CancelledError]).} =
    try:
      trace "handling identify request", conn
      let metadata = p.collectMetadata()
      var pb = encodeMsg(p.peerInfo, conn.observedAddr, p.sendSignedPeerRecord, metadata)
      await conn.writeLp(pb.buffer)
    except CancelledError as exc:
      trace "cancelled identify handler"
      raise exc
    except CatchableError as exc:
      trace "exception in identify handler", description = exc.msg, conn
    finally:
      trace "exiting identify handler", conn
      await conn.closeWithEOF()

  p.handler = handle
  p.codec = IdentifyCodec

proc identify*(
    self: Identify, conn: Connection, remotePeerId: PeerId
): Future[IdentifyInfo] {.
    async: (
      raises:
        [IdentityInvalidMsgError, IdentityNoMatchError, LPStreamError, CancelledError]
    )
.} =
  trace "initiating identify", conn
  var message = await conn.readLp(64 * 1024)
  if len(message) == 0:
    trace "identify: Empty message received!", conn
    raise newException(IdentityInvalidMsgError, "Empty message received!")

  var info = decodeMsg(message).valueOr:
    raise newException(IdentityInvalidMsgError, "Incorrect message received!")
  debug "identify: decoded message", conn, info
  let
    pubkey = info.pubkey.valueOr:
      raise newException(IdentityInvalidMsgError, "No pubkey in identify")
    peer = PeerId.init(pubkey).valueOr:
      raise newException(IdentityInvalidMsgError, $error)

  if peer != remotePeerId:
    trace "Peer ids don't match", remote = peer, local = remotePeerId
    raise newException(IdentityNoMatchError, "Peer ids don't match")
  info.peerId = peer

  info.observedAddr.withValue(observed):
    # Currently, we use the ObservedAddrManager only to find our dialable external NAT address. Therefore, addresses
    # like "...\p2p-circuit\p2p\..." and "\p2p\..." are not useful to us.
    if observed.contains(multiCodec("p2p-circuit")).get(false) or
        P2PPattern.matchPartial(observed):
      trace "Not adding address to ObservedAddrManager.", observed
    elif not self.observedAddrManager.addObservation(observed):
      trace "Observed address is not valid.", observedAddr = observed
  return info

proc new*(
    T: typedesc[IdentifyPush],
    handler: IdentifyPushHandler = nil,
    bandwidthManager: BandwidthManager = nil,
): T {.public.} =
  ## Create a IdentifyPush protocol. `handler` will be called every time
  ## a peer sends us new `PeerInfo`
  let identifypush = T(identifyHandler: handler, bandwidthManager: bandwidthManager)
  identifypush.init()
  identifypush

proc init*(p: IdentifyPush) =
  proc handle(conn: Connection, proto: string) {.async: (raises: [CancelledError]).} =
    trace "handling identify push", conn
    try:
      var message = await conn.readLp(64 * 1024)

      var identInfo = decodeMsg(message).valueOr:
        raise newException(IdentityInvalidMsgError, "Incorrect message received!")
      debug "identify push: decoded message", conn, identInfo

      identInfo.pubkey.withValue(pubkey):
        let receivedPeerId = PeerId.init(pubkey).tryGet()
        if receivedPeerId != conn.peerId:
          raise newException(IdentityNoMatchError, "Peer ids don't match")
        identInfo.peerId = receivedPeerId

      trace "triggering peer event", peerInfo = conn.peerId
      if not isNil(p.identifyHandler):
        await p.identifyHandler(conn.peerId, identInfo)
    except CancelledError as exc:
      trace "cancelled identify push handler"
      raise exc
    except CatchableError as exc:
      info "exception in identify push handler", description = exc.msg, conn
    finally:
      trace "exiting identify push handler", conn
      await conn.closeWithEOF()

  p.handler = handle
  p.codec = IdentifyPushCodec

proc push*(
    p: IdentifyPush, peerInfo: PeerInfo, conn: Connection
) {.public, async: (raises: [CancelledError, LPStreamError]).} =
  ## Send new `peerInfo`s to a connection
  let metadata = p.collectMetadata()
  var pb = encodeMsg(peerInfo, conn.observedAddr, true, metadata)
  await conn.writeLp(pb.buffer)

proc setBandwidthManager*(p: IdentifyPush, manager: BandwidthManager) {.public.} =
  p.bandwidthManager = manager
