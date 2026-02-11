# Nim-LibP2P
# Copyright (c) 2025
# Licensed under either of
#  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
#  * MIT license ([LICENSE-MIT](LICENSE-MIT))
# at your option.

{.push raises: [].}

import std/options
import ../../protobuf/minprotobuf

const
  IdentifierField = 1'u
  StatusField = 1'u
  DataField = 2'u

type
  FetchStatus* = enum
    fsOk = 0
    fsNotFound = 1
    fsError = 2
    fsTooLarge = 3

  FetchRequest* = object
    identifier*: string

  FetchResponse* = object
    status*: FetchStatus
    data*: seq[byte]

proc encode*(req: FetchRequest): seq[byte] =
  var pb = ProtoBuffer.init(cap = len(req.identifier) + 8)
  pb.stringField(IdentifierField, req.identifier)
  result = pb.toBytes()

proc decodeFetchRequest*(payload: seq[byte]): Option[FetchRequest] =
  var ident = ""
  var pb = initProtoBuffer(payload)
  while true:
    let field = pb.readFieldNumber()
    if field == 0'u:
      break
    if field == IdentifierField:
      ident = pb.readString()
    else:
      pb.skipValue()
  if ident.len == 0:
    return none(FetchRequest)
  some(FetchRequest(identifier: ident))

proc encode*(resp: FetchResponse): seq[byte] =
  var pb = ProtoBuffer.init(cap = resp.data.len + 8)
  pb.varintField(StatusField, uint64(resp.status.ord))
  if resp.data.len > 0:
    pb.bytesField(DataField, resp.data)
  result = pb.toBytes()

proc decodeFetchResponse*(payload: seq[byte]): Option[FetchResponse] =
  var status = fsError
  var data: seq[byte] = @[]
  var statusSet = false
  var pb = initProtoBuffer(payload)
  while true:
    let field = pb.readFieldNumber()
    if field == 0'u:
      break
    case field
    of StatusField:
      let code = int(pb.readVarint())
      if code < ord(low(FetchStatus)) or code > ord(high(FetchStatus)):
        return none(FetchResponse)
      status = FetchStatus(code)
      statusSet = true
    of DataField:
      data = pb.readBytes()
    else:
      pb.skipValue()
  if not statusSet:
    return none(FetchResponse)
  some(FetchResponse(status: status, data: data))
