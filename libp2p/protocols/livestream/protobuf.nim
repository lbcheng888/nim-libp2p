# Nim-LibP2P
# Copyright (c)
# Licensed under either of
#  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
#  * MIT license ([LICENSE-MIT](LICENSE-MIT))
# at your option.

{.push raises: [].}

import std/options
import ../../protobuf/minprotobuf

const
  SequenceField = 1'u
  TimestampField = 2'u
  KeyframeField = 3'u
  LayerField = 4'u
  PayloadField = 5'u
  FragmentIndexField = 6'u
  FragmentCountField = 7'u

  AckSequenceField = 1'u
  AckTimestampField = 2'u
  AckPeerField = 3'u
  AckLayerField = 4'u
  AckBufferedField = 5'u
  AckDroppedField = 6'u
  AckRequestedLayerField = 7'u
  AckLatencyField = 8'u

type
  LiveSegment* = object
    sequence*: uint64
    timestamp*: uint64
    keyframe*: bool
    layer*: uint32
    payload*: seq[byte]
    fragmentIndex*: uint32
    fragmentCount*: uint32

  LiveAck* = object
    sequence*: uint64
    timestamp*: uint64
    peer*: string
    layer*: uint32
    bufferedSegments*: uint32
    droppedSegments*: uint32
    requestedLayer*: uint32
    latencyMs*: uint32

proc encode*(segment: LiveSegment): seq[byte] =
  var pb = ProtoBuffer.init(cap = segment.payload.len + 32)
  pb.varintField(SequenceField, segment.sequence)
  pb.varintField(TimestampField, segment.timestamp)
  if segment.keyframe:
    pb.varintField(KeyframeField, 1'u64)
  pb.varintField(LayerField, segment.layer.uint64)
  if segment.fragmentCount > 0:
    pb.varintField(FragmentIndexField, segment.fragmentIndex.uint64)
    pb.varintField(FragmentCountField, segment.fragmentCount.uint64)
  pb.bytesField(PayloadField, segment.payload)
  result = pb.toBytes()

proc decode*(data: seq[byte]): Option[LiveSegment] =
  var seqSet = false
  var tsSet = false
  var seg = LiveSegment()
  var pb = initProtoBuffer(data)
  while true:
    let field = pb.readFieldNumber()
    if field == 0'u:
      break
    case field
    of SequenceField:
      seg.sequence = pb.readVarint()
      seqSet = true
    of TimestampField:
      seg.timestamp = pb.readVarint()
      tsSet = true
    of KeyframeField:
      seg.keyframe = pb.readVarint() != 0
    of LayerField:
      seg.layer = uint32(pb.readVarint())
    of FragmentIndexField:
      seg.fragmentIndex = uint32(pb.readVarint())
    of FragmentCountField:
      seg.fragmentCount = uint32(pb.readVarint())
    of PayloadField:
      seg.payload = pb.readBytes()
    else:
      pb.skipValue()

  if not seqSet:
    return none(LiveSegment)
  if not tsSet:
    seg.timestamp = 0
  some(seg)

proc encodeAck*(ack: LiveAck): seq[byte] =
  var pb = ProtoBuffer.init(cap = ack.peer.len + 16)
  pb.varintField(AckSequenceField, ack.sequence)
  pb.varintField(AckTimestampField, ack.timestamp)
  if ack.peer.len > 0:
    pb.stringField(AckPeerField, ack.peer)
  pb.varintField(AckLayerField, ack.layer.uint64)
  pb.varintField(AckBufferedField, ack.bufferedSegments.uint64)
  pb.varintField(AckDroppedField, ack.droppedSegments.uint64)
  pb.varintField(AckRequestedLayerField, ack.requestedLayer.uint64)
  pb.varintField(AckLatencyField, ack.latencyMs.uint64)
  result = pb.toBytes()

proc decodeAck*(data: seq[byte]): Option[LiveAck] =
  var seqSet = false
  var ack = LiveAck()
  var pb = initProtoBuffer(data)
  while true:
    let field = pb.readFieldNumber()
    if field == 0'u:
      break
    case field
    of AckSequenceField:
      ack.sequence = pb.readVarint()
      seqSet = true
    of AckTimestampField:
      ack.timestamp = pb.readVarint()
    of AckPeerField:
      ack.peer = pb.readString()
    of AckLayerField:
      ack.layer = uint32(pb.readVarint())
    of AckBufferedField:
      ack.bufferedSegments = uint32(pb.readVarint())
    of AckDroppedField:
      ack.droppedSegments = uint32(pb.readVarint())
    of AckRequestedLayerField:
      ack.requestedLayer = uint32(pb.readVarint())
    of AckLatencyField:
      ack.latencyMs = uint32(pb.readVarint())
    else:
      pb.skipValue()
  if not seqSet:
    return none(LiveAck)
  some(ack)
