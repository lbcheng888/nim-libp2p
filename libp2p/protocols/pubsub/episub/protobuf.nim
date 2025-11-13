# Nim-LibP2P Episub control messages
{.push raises: [].}

import results
import ../../../protobuf/minprotobuf
import ../../../peerid

const
  TypeField = 1'u
  PeerField = 2'u
  TtlField = 3'u
  PeersField = 4'u

type
  EpisubControlType* = enum
    estJoin = 0
    estNeighbor = 1
    estForwardJoin = 2
    estDisconnect = 3
    estGetNodes = 4
    estNodes = 5

  EpisubControl* = object
    kind*: EpisubControlType
    peer*: PeerId
    ttl*: uint32
    peers*: seq[PeerId]

proc encode*(msg: EpisubControl): seq[byte] =
  var pb = ProtoBuffer.init()
  pb.varintField(TypeField, msg.kind.ord.uint64)
  if msg.peer.len > 0:
    pb.bytesField(PeerField, msg.peer.data)
  if msg.ttl != 0:
    pb.varintField(TtlField, msg.ttl.uint64)
  if msg.peers.len > 0:
    for p in msg.peers:
      pb.bytesField(PeersField, p.data)
  result = pb.toBytes()

proc decode*(payload: seq[byte]): Opt[EpisubControl] =
  var kindSet = false
  var ctrl = EpisubControl(peers: @[])
  var pb = initProtoBuffer(payload)
  while true:
    let field = pb.readFieldNumber()
    if field == 0'u:
      break
    case field
    of TypeField:
      let value = pb.readVarint().int
      if value < ord(low(EpisubControlType)) or value > ord(high(EpisubControlType)):
        return Opt.none(EpisubControl)
      ctrl.kind = EpisubControlType(value)
      kindSet = true
    of PeerField:
      let bytes = pb.readBytes()
      let pid = PeerId.init(bytes).valueOr:
        return Opt.none(EpisubControl)
      ctrl.peer = pid
    of TtlField:
      ctrl.ttl = uint32(pb.readVarint())
    of PeersField:
      let bytes = pb.readBytes()
      let pid = PeerId.init(bytes).valueOr:
        return Opt.none(EpisubControl)
      ctrl.peers.add(pid)
    else:
      pb.skipValue()
  if not kindSet:
    return Opt.none(EpisubControl)
  Opt.some(ctrl)
