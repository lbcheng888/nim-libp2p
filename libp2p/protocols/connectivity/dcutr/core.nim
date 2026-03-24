# Nim-LibP2P
# Copyright (c) 2023 Status Research & Development GmbH
# Licensed under either of
#  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
#  * MIT license ([LICENSE-MIT](LICENSE-MIT))
# at your option.
# This file may not be copied, modified, or distributed except according to
# those terms.

{.push raises: [].}

import std/sequtils

import chronos
import stew/objects

import ../../../multiaddress, ../../../errors, ../../../stream/connection
import ../../../protobuf/minprotobuf

export multiaddress

const
  DcutrCodec* = "/libp2p/dcutr"
  DcutrMsgMaxSize* = 4 * 1024

type
  MsgType* = enum
    Connect = 100
    Sync = 300

  DcutrMsg* = object
    msgType*: MsgType
    addrs*: seq[MultiAddress]

  DcutrError* = object of LPError

  DcutrAttemptOutcome* = enum
    DcutrConnected
    DcutrLocalUnsupportedAddrs
    DcutrRemoteUnsupportedAddrs

proc encode*(msg: DcutrMsg): ProtoBuffer =
  result = initProtoBuffer()
  result.write(1, msg.msgType.uint)
  for addr in msg.addrs:
    result.write(2, addr)
  result.finish()

proc decode*(_: typedesc[DcutrMsg], buf: seq[byte]): DcutrMsg {.raises: [DcutrError].} =
  var
    msgTypeOrd: uint32
    dcutrMsg: DcutrMsg
  var pb = initProtoBuffer(buf)
  var r1 = pb.getField(1, msgTypeOrd)
  let r2 = pb.getRepeatedField(2, dcutrMsg.addrs)
  if r1.isErr or r2.isErr or not checkedEnumAssign(dcutrMsg.msgType, msgTypeOrd):
    raise newException(DcutrError, "Received malformed message")
  return dcutrMsg

proc send*(
    conn: Connection, msgType: MsgType, addrs: seq[MultiAddress]
) {.async: (raises: [CancelledError, LPStreamError]).} =
  let pb = DcutrMsg(msgType: msgType, addrs: addrs).encode()
  await conn.writeLp(pb.buffer)

proc expectMsgType*(msg: DcutrMsg, expected: MsgType) {.raises: [DcutrError].} =
  if msg.msgType != expected:
    raise newException(
      DcutrError,
      "Unexpected message type, expected " & $expected & " but got " & $msg.msgType,
    )

proc addrsForLog*(addrs: openArray[MultiAddress], limit = 4): seq[string] =
  if limit <= 0:
    return @[]
  for addr in addrs:
    if result.len >= limit:
      break
    result.add($addr)

proc getHolePunchableAddrs*(
    addrs: seq[MultiAddress], maxAddrs = 0
): seq[MultiAddress] {.raises: [LPError].} =
  var result = newSeq[MultiAddress]()
  for a in addrs:
    # Accept both bare transport addresses and the same addresses advertised with a trailing /p2p/<peer-id>.
    if [TCP, mapAnd(TCP, P2PPattern)].anyIt(it.match(a)):
      result.add(a[0 .. 1].tryGet())
    elif [
      QUIC,
      QUIC_V1,
      mapAnd(QUIC, P2PPattern),
      mapAnd(QUIC_V1, P2PPattern),
    ].anyIt(it.match(a)):
      result.add(a[0 .. 2].tryGet())
    else:
      continue
    if maxAddrs > 0 and result.len >= maxAddrs:
      break
  return result
