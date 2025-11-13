# Nim-LibP2P
# Copyright (c)
# Licensed under either of
#  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
#  * MIT license ([LICENSE-MIT](LICENSE-MIT))
# at your option.
# This file may not be copied, modified, or distributed except according to
# those terms.

{.push raises: [].}

import std/[algorithm, options, tables, sequtils]

import ../../protobuf/minprotobuf

const
  MsgCompleteListField = 1'u
  MsgRequestsField = 2'u
  MsgResponsesField = 3'u
  MsgBlocksField = 4'u

  RequestIdField = 1'u
  RequestRootField = 2'u
  RequestSelectorField = 3'u
  RequestExtensionsField = 4'u
  RequestPriorityField = 5'u
  RequestCancelField = 6'u
  RequestUpdateField = 7'u

  ResponseIdField = 1'u
  ResponseStatusField = 2'u
  ResponseExtensionsField = 3'u
  ResponseMetadataField = 4'u

  BlockPrefixField = 1'u
  BlockDataField = 2'u

  EntryKeyField = 1'u
  EntryValueField = 2'u

  MetadataLinkField = 1'u
  MetadataActionField = 2'u

type
  GraphSyncLinkAction* {.pure.} = enum
    gslaPresent = 0
    gslaDuplicateNotSent = 1
    gslaMissing = 2
    gslaDuplicateDagSkipped = 3

  GraphSyncMetadataEntry* = object
    link*: seq[byte]
    action*: GraphSyncLinkAction

  GraphSyncRequest* = object
    id*: int32
    root*: seq[byte]
    selector*: seq[byte]
    extensions*: Table[string, seq[byte]]
    priority*: int32
    cancel*: bool
    update*: bool

  GraphSyncResponse* = object
    id*: int32
    status*: int32
    metadata*: seq[GraphSyncMetadataEntry]
    extensions*: Table[string, seq[byte]]

  GraphSyncBlock* = object
    prefix*: seq[byte]
    data*: seq[byte]

  GraphSyncMessage* = object
    completeRequestList*: bool
    requests*: seq[GraphSyncRequest]
    responses*: seq[GraphSyncResponse]
    blocks*: seq[GraphSyncBlock]

proc decodeExtensionEntry(data: seq[byte]): Option[(string, seq[byte])] =
  var pb = initProtoBuffer(data)
  var key = ""
  var value: seq[byte] = @[]
  var hasKey = false

  while true:
    let field = pb.readFieldNumber()
    if field == 0'u:
      break
    case field
    of EntryKeyField:
      key = pb.readString()
      hasKey = key.len > 0
    of EntryValueField:
      value = pb.readBytes()
    else:
      pb.skipValue()

  if not hasKey:
    return none((string, seq[byte]))

  some((key, value))

proc decodeRequest(data: seq[byte]): Option[GraphSyncRequest] =
  var pb = initProtoBuffer(data)
  var id = 0'i32
  var root: seq[byte] = @[]
  var selector: seq[byte] = @[]
  var extensions = initTable[string, seq[byte]]()
  var priority = 0'i32
  var cancel = false
  var update = false
  var hasId = false

  while true:
    let field = pb.readFieldNumber()
    if field == 0'u:
      break
    case field
    of RequestIdField:
      id = cast[int32](pb.readVarint())
      hasId = true
    of RequestRootField:
      root = pb.readBytes()
    of RequestSelectorField:
      selector = pb.readBytes()
    of RequestExtensionsField:
      let entryBytes = pb.readBytes()
      let entryOpt = decodeExtensionEntry(entryBytes)
      entryOpt.withValue(entry):
        let (key, value) = entry
        extensions[key] = value
    of RequestPriorityField:
      priority = cast[int32](pb.readVarint())
    of RequestCancelField:
      cancel = pb.readVarint() != 0
    of RequestUpdateField:
      update = pb.readVarint() != 0
    else:
      pb.skipValue()

  if not hasId:
    return none(GraphSyncRequest)

  some(
    GraphSyncRequest(
      id: id,
      root: root,
      selector: selector,
      extensions: extensions,
      priority: priority,
      cancel: cancel,
      update: update,
    )
  )

proc decodeMetadataEntry(data: seq[byte]): Option[GraphSyncMetadataEntry] =
  var pb = initProtoBuffer(data)
  var link: seq[byte] = @[]
  var action = GraphSyncLinkAction.gslaPresent
  var hasLink = false

  while true:
    let field = pb.readFieldNumber()
    if field == 0'u:
      break
    case field
    of MetadataLinkField:
      link = pb.readBytes()
      hasLink = link.len > 0
    of MetadataActionField:
      let raw = pb.readVarint()
      let lowVal = ord(low(GraphSyncLinkAction))
      let highVal = ord(high(GraphSyncLinkAction))
      if raw >= uint64(lowVal) and raw <= uint64(highVal):
        action = GraphSyncLinkAction(raw)
    else:
      pb.skipValue()

  if not hasLink:
    return none(GraphSyncMetadataEntry)

  some(GraphSyncMetadataEntry(link: link, action: action))

proc decodeResponse(data: seq[byte]): Option[GraphSyncResponse] =
  var pb = initProtoBuffer(data)
  var id = 0'i32
  var status = 0'i32
  var metadata = newSeq[GraphSyncMetadataEntry]()
  var extensions = initTable[string, seq[byte]]()
  var hasId = false
  var hasStatus = false

  while true:
    let field = pb.readFieldNumber()
    if field == 0'u:
      break
    case field
    of ResponseIdField:
      id = cast[int32](pb.readVarint())
      hasId = true
    of ResponseStatusField:
      status = cast[int32](pb.readVarint())
      hasStatus = true
    of ResponseMetadataField:
      let mdBytes = pb.readBytes()
      let mdOpt = decodeMetadataEntry(mdBytes)
      mdOpt.withValue(md):
        metadata.add(md)
    of ResponseExtensionsField:
      let entryBytes = pb.readBytes()
      let entryOpt = decodeExtensionEntry(entryBytes)
      entryOpt.withValue(entry):
        let (key, value) = entry
        extensions[key] = value
    else:
      pb.skipValue()

  if not (hasId and hasStatus):
    return none(GraphSyncResponse)

  some(
    GraphSyncResponse(id: id, status: status, metadata: metadata, extensions: extensions)
  )

proc decodeBlock(data: seq[byte]): Option[GraphSyncBlock] =
  var pb = initProtoBuffer(data)
  var prefix: seq[byte] = @[]
  var blockData: seq[byte] = @[]
  var hasPrefix = false
  var hasData = false

  while true:
    let field = pb.readFieldNumber()
    if field == 0'u:
      break
    case field
    of BlockPrefixField:
      prefix = pb.readBytes()
      hasPrefix = prefix.len > 0
    of BlockDataField:
      blockData = pb.readBytes()
      hasData = blockData.len > 0
    else:
      pb.skipValue()

  if not (hasPrefix and hasData):
    return none(GraphSyncBlock)

  some(GraphSyncBlock(prefix: prefix, data: blockData))

proc decodeGraphSyncMessage*(payload: seq[byte]): Option[GraphSyncMessage] =
  var pb = initProtoBuffer(payload)
  var complete = false
  var sawComplete = false
  var requests: seq[GraphSyncRequest] = @[]
  var responses: seq[GraphSyncResponse] = @[]
  var blocks: seq[GraphSyncBlock] = @[]

  while true:
    let field = pb.readFieldNumber()
    if field == 0'u:
      break
    case field
    of MsgCompleteListField:
      complete = pb.readVarint() != 0
      sawComplete = true
    of MsgRequestsField:
      let reqBytes = pb.readBytes()
      let reqOpt = decodeRequest(reqBytes)
      reqOpt.withValue(req):
        requests.add(req)
    of MsgResponsesField:
      let respBytes = pb.readBytes()
      let respOpt = decodeResponse(respBytes)
      respOpt.withValue(resp):
        responses.add(resp)
    of MsgBlocksField:
      let blockBytes = pb.readBytes()
      let blockOpt = decodeBlock(blockBytes)
      blockOpt.withValue(blk):
        blocks.add(blk)
    else:
      pb.skipValue()

  some(
    GraphSyncMessage(
      completeRequestList: if sawComplete: complete else: false,
      requests: requests,
      responses: responses,
      blocks: blocks,
    )
  )

proc encodeExtensionEntry(key: string, value: seq[byte]): seq[byte] =
  var pb = ProtoBuffer.init(cap = key.len + value.len + 16)
  pb.stringField(EntryKeyField, key)
  if value.len > 0:
    pb.bytesField(EntryValueField, value)
  result = pb.toBytes()

proc encode*(metadata: GraphSyncMetadataEntry): seq[byte] =
  var pb = ProtoBuffer.init(cap = metadata.link.len + 8)
  pb.bytesField(MetadataLinkField, metadata.link)
  pb.varintField(MetadataActionField, uint64(metadata.action))
  result = pb.toBytes()

proc encode*(req: GraphSyncRequest): seq[byte] =
  var pb = ProtoBuffer.init(cap = 64 + req.root.len + req.selector.len)
  pb.varintField(RequestIdField, uint64(req.id))
  if req.root.len > 0:
    pb.bytesField(RequestRootField, req.root)
  if req.selector.len > 0:
    pb.bytesField(RequestSelectorField, req.selector)
  if req.extensions.len > 0:
    var keys = req.extensions.keys.toSeq()
    keys.sort()
    for key in keys:
      let value = req.extensions.getOrDefault(key, @[])
      pb.bytesField(RequestExtensionsField, encodeExtensionEntry(key, value))
  if req.priority != 0:
    pb.varintField(RequestPriorityField, uint64(req.priority))
  if req.cancel:
    pb.varintField(RequestCancelField, 1)
  if req.update:
    pb.varintField(RequestUpdateField, 1)
  result = pb.toBytes()

proc encode*(resp: GraphSyncResponse): seq[byte] =
  var pb = ProtoBuffer.init(cap = 64)
  pb.varintField(ResponseIdField, uint64(resp.id))
  pb.varintField(ResponseStatusField, uint64(resp.status))
  if resp.metadata.len > 0:
    for md in resp.metadata:
      pb.bytesField(ResponseMetadataField, encode(md))
  if resp.extensions.len > 0:
    var keys = resp.extensions.keys.toSeq()
    keys.sort()
    for key in keys:
      let value = resp.extensions.getOrDefault(key, @[])
      pb.bytesField(ResponseExtensionsField, encodeExtensionEntry(key, value))
  result = pb.toBytes()

proc encode*(blk: GraphSyncBlock): seq[byte] =
  var pb = ProtoBuffer.init(cap = blk.prefix.len + blk.data.len + 16)
  pb.bytesField(BlockPrefixField, blk.prefix)
  pb.bytesField(BlockDataField, blk.data)
  result = pb.toBytes()

proc encode*(msg: GraphSyncMessage): seq[byte] =
  var pb = ProtoBuffer.init(cap = 128)
  if msg.completeRequestList:
    pb.varintField(MsgCompleteListField, 1)
  for req in msg.requests:
    pb.bytesField(MsgRequestsField, encode(req))
  for resp in msg.responses:
    pb.bytesField(MsgResponsesField, encode(resp))
  for blk in msg.blocks:
    pb.bytesField(MsgBlocksField, encode(blk))
  result = pb.toBytes()

{.pop.}
