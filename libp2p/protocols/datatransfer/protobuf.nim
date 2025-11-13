# Nim-LibP2P
# Copyright (c)
# Licensed under either of
#  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
#  * MIT license ([LICENSE-MIT](LICENSE-MIT))
# at your option.

{.push raises: [].}

import std/[options]

import ../../cid
import ../../protobuf/minprotobuf

const
  TransferIdField = 1'u
  KindField = 2'u
  PausedField = 3'u
  RequestField = 4'u
  ResponseField = 5'u
  ControlField = 6'u
  ErrorField = 7'u
  ExtensionField = 8'u

  ReqIsPullField = 1'u
  ReqVoucherTypeField = 2'u
  ReqVoucherField = 3'u
  ReqSelectorField = 4'u
  ReqBaseCidField = 5'u

  RespAcceptedField = 1'u
  RespStatusField = 2'u
  RespPauseField = 3'u

  CtrlTypeField = 1'u
  CtrlStatusField = 2'u

  ErrCodeField = 1'u
  ErrMessageField = 2'u

  ExtNameField = 1'u
  ExtDataField = 2'u

type
  DataTransferMessageKind* {.pure.} = enum
    dtmkRequest = 0
    dtmkResponse = 1
    dtmkControl = 2
    dtmkError = 3

  DataTransferControlType* {.pure.} = enum
    dtctUnknown = 0
    dtctOpen = 1
    dtctRestart = 2
    dtctCancel = 3
    dtctComplete = 4
    dtctPause = 5
    dtctResume = 6

  DataTransferExtension* = object
    name*: string
    data*: seq[byte]

  DataTransferRequest* = object
    isPull*: bool
    voucherType*: string
    voucher*: seq[byte]
    selector*: seq[byte]
    baseCid*: Option[Cid]

  DataTransferResponse* = object
    accepted*: bool
    status*: Option[string]
    pause*: bool

  DataTransferControl* = object
    controlType*: DataTransferControlType
    status*: Option[string]

  DataTransferError* = object
    code*: int32
    message*: string

  DataTransferMessage* = object
    transferId*: uint64
    paused*: bool
    kind*: DataTransferMessageKind
    request*: Option[DataTransferRequest]
    response*: Option[DataTransferResponse]
    control*: Option[DataTransferControl]
    error*: Option[DataTransferError]
    extensions*: seq[DataTransferExtension]

proc encodeRequest(req: DataTransferRequest): seq[byte] =
  var pb = ProtoBuffer.init(
    cap = req.voucher.len + req.selector.len + req.voucherType.len + 16
  )
  if req.isPull:
    pb.varintField(ReqIsPullField, 1)
  if req.voucherType.len > 0:
    pb.stringField(ReqVoucherTypeField, req.voucherType)
  if req.voucher.len > 0:
    pb.bytesField(ReqVoucherField, req.voucher)
  if req.selector.len > 0:
    pb.bytesField(ReqSelectorField, req.selector)
  req.baseCid.withValue(cid):
    pb.bytesField(ReqBaseCidField, cid.data.buffer)
  result = pb.toBytes()

proc decodeRequest(data: seq[byte]): Option[DataTransferRequest] =
  var pb = initProtoBuffer(data)
  var isPull = false
  var voucherType = ""
  var voucher: seq[byte] = @[]
  var selector: seq[byte] = @[]
  var baseCidOpt = none(Cid)

  while true:
    let field = pb.readFieldNumber()
    if field == 0'u:
      break
    case field
    of ReqIsPullField:
      isPull = pb.readVarint() != 0
    of ReqVoucherTypeField:
      voucherType = pb.readString()
    of ReqVoucherField:
      voucher = pb.readBytes()
    of ReqSelectorField:
      selector = pb.readBytes()
    of ReqBaseCidField:
      let raw = pb.readBytes()
      let cidRes = Cid.init(raw)
      if cidRes.isErr():
        return none(DataTransferRequest)
      baseCidOpt = some(cidRes.get())
    else:
      pb.skipValue()

  some(
    DataTransferRequest(
      isPull: isPull,
      voucherType: voucherType,
      voucher: voucher,
      selector: selector,
      baseCid: baseCidOpt,
    )
  )

proc encodeResponse(resp: DataTransferResponse): seq[byte] =
  let statusLen =
    if resp.status.isSome(): resp.status.get().len else: 0
  var pb = ProtoBuffer.init(cap = statusLen + 8)
  if resp.accepted:
    pb.varintField(RespAcceptedField, 1)
  resp.status.withValue(status):
    if status.len > 0:
      pb.stringField(RespStatusField, status)
  if resp.pause:
    pb.varintField(RespPauseField, 1)
  result = pb.toBytes()

proc decodeResponse(data: seq[byte]): Option[DataTransferResponse] =
  var pb = initProtoBuffer(data)
  var accepted = false
  var pause = false
  var statusOpt = none(string)

  while true:
    let field = pb.readFieldNumber()
    if field == 0'u:
      break
    case field
    of RespAcceptedField:
      accepted = pb.readVarint() != 0
    of RespStatusField:
      statusOpt = some(pb.readString())
    of RespPauseField:
      pause = pb.readVarint() != 0
    else:
      pb.skipValue()

  some(
    DataTransferResponse(accepted: accepted, status: statusOpt, pause: pause)
  )

proc encodeControl(ctrl: DataTransferControl): seq[byte] =
  let statusLen =
    if ctrl.status.isSome(): ctrl.status.get().len else: 0
  var pb = ProtoBuffer.init(cap = statusLen + 8)
  if ctrl.controlType != DataTransferControlType.dtctUnknown:
    pb.varintField(CtrlTypeField, uint64(ctrl.controlType.ord))
  ctrl.status.withValue(status):
    if status.len > 0:
      pb.stringField(CtrlStatusField, status)
  result = pb.toBytes()

proc decodeControl(data: seq[byte]): Option[DataTransferControl] =
  var pb = initProtoBuffer(data)
  var controlType = DataTransferControlType.dtctUnknown
  var statusOpt = none(string)
  var hasType = false

  while true:
    let field = pb.readFieldNumber()
    if field == 0'u:
      break
    case field
    of CtrlTypeField:
      let raw = pb.readVarint()
      if raw <= uint64(ord(high(DataTransferControlType))):
        controlType = DataTransferControlType(raw)
        hasType = true
      else:
        return none(DataTransferControl)
    of CtrlStatusField:
      statusOpt = some(pb.readString())
    else:
      pb.skipValue()

  if not hasType:
    controlType = DataTransferControlType.dtctUnknown

  some(DataTransferControl(controlType: controlType, status: statusOpt))

proc encodeError(err: DataTransferError): seq[byte] =
  var pb = ProtoBuffer.init(cap = err.message.len + 8)
  pb.varintField(ErrCodeField, uint64(err.code))
  if err.message.len > 0:
    pb.stringField(ErrMessageField, err.message)
  result = pb.toBytes()

proc decodeError(data: seq[byte]): Option[DataTransferError] =
  var pb = initProtoBuffer(data)
  var code = 0'i32
  var hasCode = false
  var message = ""

  while true:
    let field = pb.readFieldNumber()
    if field == 0'u:
      break
    case field
    of ErrCodeField:
      code = int32(pb.readVarint())
      hasCode = true
    of ErrMessageField:
      message = pb.readString()
    else:
      pb.skipValue()

  if not hasCode:
    return none(DataTransferError)

  some(DataTransferError(code: code, message: message))

proc encodeExtension(ext: DataTransferExtension): seq[byte] =
  var pb = ProtoBuffer.init(cap = ext.name.len + ext.data.len + 4)
  if ext.name.len > 0:
    pb.stringField(ExtNameField, ext.name)
  if ext.data.len > 0:
    pb.bytesField(ExtDataField, ext.data)
  result = pb.toBytes()

proc decodeExtension(data: seq[byte]): Option[DataTransferExtension] =
  var pb = initProtoBuffer(data)
  var name = ""
  var payload: seq[byte] = @[]
  var hasName = false
  var sawData = false

  while true:
    let field = pb.readFieldNumber()
    if field == 0'u:
      break
    case field
    of ExtNameField:
      name = pb.readString()
      hasName = true
    of ExtDataField:
      payload = pb.readBytes()
      sawData = true
    else:
      pb.skipValue()

  if not hasName:
    return none(DataTransferExtension)

  if not sawData:
    payload = @[]

  some(DataTransferExtension(name: name, data: payload))

proc encode*(msg: DataTransferMessage): seq[byte] =
  var pb = ProtoBuffer.init(cap = 64)
  pb.varintField(TransferIdField, msg.transferId)
  pb.varintField(KindField, uint64(msg.kind.ord))
  if msg.paused:
    pb.varintField(PausedField, 1)

  msg.request.withValue(req):
    pb.bytesField(RequestField, encodeRequest(req))
  msg.response.withValue(resp):
    pb.bytesField(ResponseField, encodeResponse(resp))
  msg.control.withValue(ctrl):
    pb.bytesField(ControlField, encodeControl(ctrl))
  msg.error.withValue(err):
    pb.bytesField(ErrorField, encodeError(err))

  if msg.extensions.len > 0:
    for ext in msg.extensions:
      let encoded = encodeExtension(ext)
      if encoded.len > 0:
        pb.bytesField(ExtensionField, encoded)

  result = pb.toBytes()

proc decodeDataTransferMessage*(payload: seq[byte]): Option[DataTransferMessage] =
  var pb = initProtoBuffer(payload)
  var transferId = 0'u64
  var hasTransferId = false
  var kind = DataTransferMessageKind.dtmkRequest
  var hasKind = false
  var paused = false
  var requestOpt = none(DataTransferRequest)
  var responseOpt = none(DataTransferResponse)
  var controlOpt = none(DataTransferControl)
  var errorOpt = none(DataTransferError)
  var extensions: seq[DataTransferExtension] = @[]

  while true:
    let field = pb.readFieldNumber()
    if field == 0'u:
      break
    case field
    of TransferIdField:
      transferId = pb.readVarint()
      hasTransferId = true
    of KindField:
      let raw = pb.readVarint()
      if raw <= uint64(ord(high(DataTransferMessageKind))):
        kind = DataTransferMessageKind(raw)
        hasKind = true
      else:
        return none(DataTransferMessage)
    of PausedField:
      paused = pb.readVarint() != 0
    of RequestField:
      let decoded = decodeRequest(pb.readBytes())
      if decoded.isNone():
        return none(DataTransferMessage)
      requestOpt = decoded
    of ResponseField:
      let decoded = decodeResponse(pb.readBytes())
      if decoded.isNone():
        return none(DataTransferMessage)
      responseOpt = decoded
    of ControlField:
      let decoded = decodeControl(pb.readBytes())
      if decoded.isNone():
        return none(DataTransferMessage)
      controlOpt = decoded
    of ErrorField:
      let decoded = decodeError(pb.readBytes())
      if decoded.isNone():
        return none(DataTransferMessage)
      errorOpt = decoded
    of ExtensionField:
      let decoded = decodeExtension(pb.readBytes())
      decoded.withValue(ext):
        extensions.add(ext)
    else:
      pb.skipValue()

  if not hasTransferId or not hasKind:
    return none(DataTransferMessage)

  some(
    DataTransferMessage(
      transferId: transferId,
      paused: paused,
      kind: kind,
      request: requestOpt,
      response: responseOpt,
      control: controlOpt,
      error: errorOpt,
      extensions: extensions,
    )
  )

{.pop.}
