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

import ../../cid
import ../../protobuf/minprotobuf

const
  MsgWantlistField = 1'u
  MsgBlocksField = 2'u
  MsgPayloadField = 3'u
  MsgBlockPresenceField = 4'u
  MsgPendingBytesField = 5'u

  WantlistEntriesField = 1'u
  WantlistFullField = 2'u

  EntryBlockField = 1'u
  EntryPriorityField = 2'u
  EntryCancelField = 3'u
  EntryWantTypeField = 4'u
  EntrySendDontHaveField = 5'u

  PayloadPrefixField = 1'u
  PayloadDataField = 2'u

  PresenceCidField = 1'u
  PresenceTypeField = 2'u

type
  WantType* {.pure.} = enum
    wtBlock = 0
    wtHave = 1

  BlockPresenceType* {.pure.} = enum
    bpHave = 0
    bpDontHave = 1

  WantEntry* = object
    cid*: Cid
    priority*: int32
    cancel*: bool
    wantType*: WantType
    sendDontHave*: bool

  Wantlist* = object
    entries*: seq[WantEntry]
    full*: bool

  BlockPayload* = object
    prefix*: seq[byte]
    data*: seq[byte]

  BlockPresence* = object
    cid*: Cid
    presenceType*: BlockPresenceType

  BitswapMessage* = object
    wantlist*: Option[Wantlist]
    blocks*: seq[seq[byte]]
    payloads*: seq[BlockPayload]
    blockPresences*: seq[BlockPresence]
    pendingBytes*: Option[int32]

proc decodeWantEntry(data: seq[byte]): Option[WantEntry] =
  var pb = initProtoBuffer(data)
  var cidBytes: seq[byte] = @[]
  var priority = 1'i32
  var cancel = false
  var wantType = WantType.wtBlock
  var sendDontHave = false

  while true:
    let field = pb.readFieldNumber()
    if field == 0'u:
      break
    case field
    of EntryBlockField:
      cidBytes = pb.readBytes()
    of EntryPriorityField:
      priority = cast[int32](pb.readVarint())
    of EntryCancelField:
      cancel = pb.readVarint() != 0
    of EntryWantTypeField:
      let raw = pb.readVarint()
      if raw <= uint64(ord(high(WantType))):
        wantType = WantType(raw)
    of EntrySendDontHaveField:
      sendDontHave = pb.readVarint() != 0
    else:
      pb.skipValue()

  if cidBytes.len == 0:
    return none(WantEntry)

  let cidRes = Cid.init(cidBytes)
  if cidRes.isErr:
    return none(WantEntry)

  some(
    WantEntry(
      cid: cidRes.get(),
      priority: priority,
      cancel: cancel,
      wantType: wantType,
      sendDontHave: sendDontHave,
    )
  )

proc decodeWantlist(data: seq[byte]): Option[Wantlist] =
  var pb = initProtoBuffer(data)
  var entries: seq[WantEntry] = @[]
  var full = false
  var sawFull = false

  while true:
    let field = pb.readFieldNumber()
    if field == 0'u:
      break
    case field
    of WantlistEntriesField:
      let entryBytes = pb.readBytes()
      let entryOpt = decodeWantEntry(entryBytes)
      entryOpt.withValue(entry):
        entries.add(entry)
    of WantlistFullField:
      full = pb.readVarint() != 0
      sawFull = true
    else:
      pb.skipValue()

  some(Wantlist(entries: entries, full: if sawFull: full else: false))

proc decodeBlockPresence(data: seq[byte]): Option[BlockPresence] =
  var pb = initProtoBuffer(data)
  var cidBytes: seq[byte] = @[]
  var presType = BlockPresenceType.bpHave
  var hasType = false

  while true:
    let field = pb.readFieldNumber()
    if field == 0'u:
      break
    case field
    of PresenceCidField:
      cidBytes = pb.readBytes()
    of PresenceTypeField:
      let raw = pb.readVarint()
      if raw <= uint64(ord(high(BlockPresenceType))):
        presType = BlockPresenceType(raw)
        hasType = true
    else:
      pb.skipValue()

  if cidBytes.len == 0:
    return none(BlockPresence)

  let cidRes = Cid.init(cidBytes)
  if cidRes.isErr:
    return none(BlockPresence)

  some(
    BlockPresence(
      cid: cidRes.get(), presenceType: if hasType: presType else: BlockPresenceType.bpHave
    )
  )

proc decodeBlockPayload(data: seq[byte]): BlockPayload =
  var pb = initProtoBuffer(data)
  var prefix: seq[byte] = @[]
  var body: seq[byte] = @[]

  while true:
    let field = pb.readFieldNumber()
    if field == 0'u:
      break
    case field
    of PayloadPrefixField:
      prefix = pb.readBytes()
    of PayloadDataField:
      body = pb.readBytes()
    else:
      pb.skipValue()

  BlockPayload(prefix: prefix, data: body)

proc decodeBitswapMessage*(payload: seq[byte]): Option[BitswapMessage] =
  var pb = initProtoBuffer(payload)
  var wantlistOpt = none(Wantlist)
  var blocks: seq[seq[byte]] = @[]
  var payloads: seq[BlockPayload] = @[]
  var presences: seq[BlockPresence] = @[]
  var pendingOpt = none(int32)

  while true:
    let field = pb.readFieldNumber()
    if field == 0'u:
      break
    case field
    of MsgWantlistField:
      let wantBytes = pb.readBytes()
      let wlOpt = decodeWantlist(wantBytes)
      wlOpt.withValue(wl):
        wantlistOpt = some(wl)
    of MsgBlocksField:
      blocks.add(pb.readBytes())
    of MsgPayloadField:
      let payloadBytes = pb.readBytes()
      payloads.add(decodeBlockPayload(payloadBytes))
    of MsgBlockPresenceField:
      let presenceBytes = pb.readBytes()
      let presenceOpt = decodeBlockPresence(presenceBytes)
      presenceOpt.withValue(presence):
        presences.add(presence)
    of MsgPendingBytesField:
      pendingOpt = some(cast[int32](pb.readVarint()))
    else:
      pb.skipValue()

  some(
    BitswapMessage(
      wantlist: wantlistOpt,
      blocks: blocks,
      payloads: payloads,
      blockPresences: presences,
      pendingBytes: pendingOpt,
    )
  )

proc encode*(entry: WantEntry): seq[byte] =
  var pb = ProtoBuffer.init(cap = 32)
  pb.bytesField(EntryBlockField, entry.cid.data.buffer)
  pb.varintField(EntryPriorityField, uint64(entry.priority))
  if entry.cancel:
    pb.varintField(EntryCancelField, 1)
  if entry.wantType != WantType.wtBlock:
    pb.varintField(EntryWantTypeField, uint64(entry.wantType.ord))
  if entry.sendDontHave:
    pb.varintField(EntrySendDontHaveField, 1)
  result = pb.toBytes()

proc encode*(wantlist: Wantlist): seq[byte] =
  var pb = ProtoBuffer.init(cap = 64)
  for entry in wantlist.entries:
    pb.bytesField(WantlistEntriesField, encode(entry))
  if wantlist.full:
    pb.varintField(WantlistFullField, 1)
  result = pb.toBytes()

proc encode*(payload: BlockPayload): seq[byte] =
  var pb = ProtoBuffer.init(cap = payload.data.len + payload.prefix.len + 16)
  if payload.prefix.len > 0:
    pb.bytesField(PayloadPrefixField, payload.prefix)
  if payload.data.len > 0:
    pb.bytesField(PayloadDataField, payload.data)
  result = pb.toBytes()

proc encode*(presence: BlockPresence): seq[byte] =
  var pb = ProtoBuffer.init(cap = 48)
  pb.bytesField(PresenceCidField, presence.cid.data.buffer)
  pb.varintField(PresenceTypeField, uint64(presence.presenceType.ord))
  result = pb.toBytes()

proc encode*(msg: BitswapMessage): seq[byte] =
  var pb = ProtoBuffer.init(cap = 128)
  msg.wantlist.withValue(wl):
    pb.bytesField(MsgWantlistField, encode(wl))
  for blockData in msg.blocks:
    pb.bytesField(MsgBlocksField, blockData)
  for payload in msg.payloads:
    pb.bytesField(MsgPayloadField, encode(payload))
  for presence in msg.blockPresences:
    pb.bytesField(MsgBlockPresenceField, encode(presence))
  msg.pendingBytes.withValue(pending):
    pb.varintField(MsgPendingBytesField, uint64(pending))
  result = pb.toBytes()

{.pop.}
