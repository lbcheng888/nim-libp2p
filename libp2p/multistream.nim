# Nim-LibP2P
# Copyright (c) 2023-2024 Status Research & Development GmbH
# Licensed under either of
#  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
#  * MIT license ([LICENSE-MIT](LICENSE-MIT))
# at your option.
# This file may not be copied, modified, or distributed except according to
# those terms.

{.push raises: [].}

import std/[strutils, sequtils, tables]
import chronos, chronicles, stew/byteutils
import varint
import stream/connection, protocols/protocol, resourcemanager, protobuf/minprotobuf

logScope:
  topics = "libp2p multistream"

const
  MsgSize = 1024
  CodecV1* = "/multistream/1.0.0"
  CodecV2* = "/multistream/2.0.0"
  Codec* = CodecV1
  ProtocolSelectVersion = 1'u32

  Na = "na\n"
  Ls = "ls\n"

type
  Matcher* = proc(proto: string): bool {.gcsafe, raises: [].}

  MultiStreamError* = object of LPError

  ProtocolSelectStatus = enum
    psSuccess,
    psNoMatch,
    psUnsupported

  HandlerHolder* = ref object
    protos*: seq[string]
    protocol*: LPProtocol
    match*: Matcher
    openedStreams: CountTable[PeerId]

  MultistreamSelect* = ref object of RootObj
    handlers*: seq[HandlerHolder]
    codec*: string
    resourceManager*: ResourceManager

  ProtocolSelectMessage = object
    version: uint32
    protocols: seq[string]

proc encodeProtocolSelect(protocols: openArray[string]): ProtoBuffer =
  var pb = initProtoBuffer()
  pb.write(1, ProtocolSelectVersion.uint)
  for proto in protocols:
    var nested = initProtoBuffer()
    nested.write(1, proto)
    nested.finish()
    pb.write(2, nested)
  pb.finish()
  result = pb

proc decodeProtocolSelect(data: seq[byte]): ProtoResult[ProtocolSelectMessage] =
  var pb = initProtoBuffer(data)
  var msg = ProtocolSelectMessage(version: 0, protocols: @[])
  var haveVersion = false

  while true:
    let field = pb.readFieldNumber()
    if field == 0:
      break

    case field
    of 1:
      let value = pb.readVarint()
      msg.version = uint32(value)
      haveVersion = true
    of 2:
      let nestedBytes = pb.readBytes()
      if nestedBytes.len == 0:
        continue
      var nested = initProtoBuffer(nestedBytes)
      var protoName = ""
      while true:
        let nestedField = nested.readFieldNumber()
        if nestedField == 0:
          break
        case nestedField
        of 1:
          protoName = nested.readString()
        else:
          nested.skipValue()
      if protoName.len > 0:
        msg.protocols.add(protoName)
    else:
      pb.skipValue()

  if not haveVersion:
    return err(ProtoError.RequiredFieldMissing)

  ok(msg)

proc encodeProtocolListing(protos: openArray[string]): seq[byte] =
  var payload: seq[byte] = @[]
  for proto in protos:
    let entry = (proto & "\n").toBytes()
    let lengthBytes = PB.toBytes(entry.len().uint64)
    for b in lengthBytes:
      payload.add(b)
    payload.add(entry)
  payload.add(byte('\n'))
  payload

proc decodeProtocolListing*(data: seq[byte]): seq[string] =
  var
    idx = 0
    parsed = newSeq[string]()
    ok = true

  while idx < data.len and ok:
    if data[idx] == byte('\n'):
      while idx < data.len and data[idx] == byte('\n'):
        inc idx
      ok = idx == data.len
      break

    var
      fieldLen: uint64
      consumed: int
    let res = PB.getUVarint(data.toOpenArray(idx, data.high), consumed, fieldLen)
    if res.isErr or consumed <= 0:
      ok = false
      break
    idx += consumed
    if idx + int(fieldLen) > data.len:
      ok = false
      break

    var proto = string.fromBytes(data[idx ..< idx + int(fieldLen)])
    idx += int(fieldLen)
    if not proto.endsWith("\n"):
      ok = false
      break
    proto.removeSuffix("\n")
    parsed.add(proto)

  if ok and idx == data.len:
    return parsed

  parsed.setLen(0)
  let legacy = string.fromBytes(data)
  for entry in legacy.split("\n"):
    if entry.len() > 0:
      parsed.add(entry)
  parsed

proc new*(
    T: typedesc[MultistreamSelect], resourceManager: ResourceManager = nil
): T =
  T(codec: Codec, resourceManager: resourceManager)

proc setResourceManager*(m: MultistreamSelect, manager: ResourceManager) =
  m.resourceManager = manager

template validateSuffix(str: string): untyped =
  if str.endsWith("\n"):
    str.removeSuffix("\n")
  else:
    raise (ref MultiStreamError)(msg: "MultistreamSelect failed, malformed message")

proc acquireStreamPermitResult(
    manager: ResourceManager, conn: Connection, proto: string, dir: Direction
): tuple[ok: bool, permit: ResourcePermit, err: string] =
  if manager.isNil:
    return (true, nil, "")
  let res = manager.acquireStream(conn.peerId, proto, dir)
  if res.isErr:
    return (false, nil, res.error)
  (true, res.unsafeGet(), "")

proc releasePermit(permit: var ResourcePermit) =
  if not permit.isNil:
    permit.release()
    permit = nil

proc performDialerHandshake(
    conn: Connection, attemptV2: bool
): Future[MultistreamVersion] {.
    async: (raises: [CancelledError, LPStreamError, MultiStreamError])
.} =
  trace "initiating handshake", conn, attemptV2
  await conn.writeLp(CodecV1 & "\n")

  if attemptV2:
    trace "requesting multistream v2", conn
    await conn.writeLp(CodecV2 & "\n")

  let response1 = await conn.readLp(MsgSize)
  var handshake = string.fromBytes(response1)
  validateSuffix(handshake)

  if handshake != CodecV1:
    notice "handshake failed", conn, codec = handshake
    raise (ref MultiStreamError)(msg: "MultistreamSelect handshake failed")

  var version = msv1

  if attemptV2:
    let response2 = await conn.readLp(MsgSize)
    var v2Response = string.fromBytes(response2)
    validateSuffix(v2Response)

    if v2Response == CodecV2:
      trace "multistream v2 acknowledged", conn
      version = msv2
    elif v2Response == "na":
      trace "multistream v2 rejected by remote", conn
      version = msv1
    else:
      debug "unexpected multistream response after v2 request",
        conn, response = v2Response
      version = msv1

  conn.multistreamVersion = version
  trace "multistream handshake success", conn, version = version
  version

proc attemptProtocolSelectDialer(
    conn: Connection,
    manager: ResourceManager,
    proto: seq[string],
    firstPermit: ResourcePermit,
): Future[
    tuple[
      status: ProtocolSelectStatus,
      selected: string,
      firstPermit: ResourcePermit
    ]
  ] {.async: (raises: [CancelledError, LPStreamError, MultiStreamError]).} =
  var currentPermit = firstPermit

  if proto.len == 0:
    return (psNoMatch, "", currentPermit)

  let psMessage = encodeProtocolSelect(proto)
  await conn.writeLp(psMessage.buffer)

  let response = await conn.readLp(MsgSize)
  if response.len == 0 or response[0] == byte('/'):
    trace "protocol select remote fallback detected", conn
    return (psUnsupported, "", currentPermit)

  let decoded = decodeProtocolSelect(response)
  if decoded.isErr():
    debug "protocol select decode failed", conn, err = decoded.error
    return (psUnsupported, "", currentPermit)

  let message = decoded.get()
  if message.version != ProtocolSelectVersion:
    debug "protocol select version mismatch", conn, version = message.version
    return (psUnsupported, "", currentPermit)

  if message.protocols.len == 0:
    trace "protocol select returned no matches", conn
    releasePermit(currentPermit)
    currentPermit = nil
    return (psNoMatch, "", currentPermit)

  let selected = message.protocols[0]
  let idx = proto.find(selected)
  if idx < 0:
    debug "protocol select returned unexpected protocol", conn, protocol = selected
    return (psUnsupported, "", currentPermit)

  var permit: ResourcePermit = nil
  if idx == 0:
    if currentPermit.isNil:
      let permitAttempt = acquireStreamPermitResult(manager, conn, selected, Direction.Out)
      if not permitAttempt.ok:
        raise (ref MultiStreamError)(
          msg: "Resource limits reached while opening stream: " & permitAttempt.err
        )
      permit = permitAttempt.permit
    else:
      permit = currentPermit
  else:
    if not currentPermit.isNil:
      releasePermit(currentPermit)
      currentPermit = nil
    let permitAttempt = acquireStreamPermitResult(manager, conn, selected, Direction.Out)
    if not permitAttempt.ok:
      raise (ref MultiStreamError)(
        msg: "Resource limits reached while opening stream: " & permitAttempt.err
      )
    permit = permitAttempt.permit

  currentPermit = nil
  conn.protocol = selected
  if not permit.isNil:
    manager.attachPermit(permit, conn)

  trace "protocol select success", conn, protocol = selected
  (psSuccess, selected, currentPermit)

proc select*(
    m: MultistreamSelect | type MultistreamSelect, conn: Connection, proto: seq[string]
): Future[string] {.async: (raises: [CancelledError, LPStreamError, MultiStreamError]).} =
  trace "initiating multistream negotiation", conn, candidates = proto
  ## select a remote protocol
  let manager =
    when m is typedesc[MultistreamSelect]:
      ResourceManager(nil)
    elif compiles(m.resourceManager):
      m.resourceManager
    else:
      ResourceManager(nil)

  var firstPermit: ResourcePermit = nil
  var firstUsed = false

  if proto.len() > 0:
    let permitAttempt =
      acquireStreamPermitResult(manager, conn, proto[0], Direction.Out)
    if not permitAttempt.ok:
      raise (ref MultiStreamError)(
        msg: "Resource limits reached while opening stream: " & permitAttempt.err
      )
    firstPermit = permitAttempt.permit
    firstUsed = true

  let attemptV2 =
    case conn.multistreamVersion
    of msv1:
      false
    of msv2:
      true
    else:
      true

  let negotiatedVersion = await performDialerHandshake(conn, attemptV2)

  if proto.len() == 0:
    return (if negotiatedVersion == msv2: CodecV2 else: CodecV1)

  if negotiatedVersion == msv2:
    let psResult =
      await attemptProtocolSelectDialer(conn, manager, proto, firstPermit)
    firstPermit = psResult.firstPermit
    case psResult.status
    of psSuccess:
      return psResult.selected
    of psNoMatch:
      return ""
    of psUnsupported:
      trace "protocol select unavailable, falling back to multistream v1", conn
      conn.multistreamVersion = msv1

  var startIdx = 0
  if firstUsed:
    let firstProto = proto[0]
    trace "selecting proto", conn, proto = firstProto
    try:
      await conn.writeLp(firstProto & "\n")
    except CancelledError as exc:
      releasePermit(firstPermit)
      raise exc
    except LPStreamError as exc:
      releasePermit(firstPermit)
      raise exc

    var response = string.fromBytes(await conn.readLp(MsgSize))
    validateSuffix(response)

    if response == firstProto:
      trace "successfully selected", conn, proto = firstProto
      conn.protocol = firstProto
      manager.attachPermit(firstPermit, conn)
      firstPermit = nil
      return firstProto

    trace "first protocol rejected by remote",
      conn, requested = firstProto, received = response
    releasePermit(firstPermit)
    firstPermit = nil
    startIdx = 1
  else:
    releasePermit(firstPermit)
    firstPermit = nil
    startIdx = 0

  if proto.len > startIdx:
    let protos = proto[startIdx ..< proto.len()]
    trace "selecting one of several protos", conn, protos = protos
    for p in protos:
      let permitAttempt =
        acquireStreamPermitResult(manager, conn, p, Direction.Out)
      if not permitAttempt.ok:
        trace "skipping protocol due to local resource limits",
          conn, protocol = p, err = permitAttempt.err
        continue
      var permit = permitAttempt.permit
      trace "selecting proto", conn, proto = p
      try:
        await conn.writeLp(p & "\n")
      except CancelledError as exc:
        releasePermit(permit)
        raise exc
      except LPStreamError as exc:
        releasePermit(permit)
        raise exc

      var response = string.fromBytes(await conn.readLp(MsgSize))
      validateSuffix(response)
      if response == p:
        trace "selected protocol", conn, protocol = response
        conn.protocol = response
        if not permit.isNil:
          manager.attachPermit(permit, conn)
        return response

      trace "protocol rejected by remote", conn, requested = p, received = response
      releasePermit(permit)

  ""

proc select*(
    m: MultistreamSelect | type MultistreamSelect, conn: Connection, proto: string
): Future[bool] {.async: (raises: [CancelledError, LPStreamError, MultiStreamError]).} =
  if proto.len > 0:
    (await m.select(conn, @[proto])) == proto
  else:
    let negotiated = await m.select(conn, @[])
    negotiated == CodecV1 or negotiated == CodecV2

proc select*(
    m: MultistreamSelect, conn: Connection
): Future[bool] {.
    async: (raises: [CancelledError, LPStreamError, MultiStreamError], raw: true)
.} =
  m.select(conn, "")

proc list*(
    m: MultistreamSelect, conn: Connection
): Future[seq[string]] {.
    async: (raises: [CancelledError, LPStreamError, MultiStreamError])
.} =
  ## list remote protos requests on connection
  if not await m.select(conn):
    return

  await conn.writeLp(Ls) # send ls

  let payload = await conn.readLp(MsgSize)
  result = decodeProtocolListing(payload)

proc handleProtocolSelect(
    conn: Connection, protos: seq[string], matchers: seq[Matcher], first: seq[byte]
): Future[string] {.async: (raises: [CancelledError, LPStreamError, MultiStreamError]).}

proc handleMultistreamV1Loop(
    conn: Connection,
    protos: seq[string],
    matchers: seq[Matcher],
    pending: seq[byte],
    handshaked: bool,
): Future[string] {.async: (raises: [CancelledError, LPStreamError, MultiStreamError]).} =
  var messages: seq[seq[byte]]
  var isHandshaked = handshaked

  if pending.len > 0:
    messages.add(pending)

  while not conn.atEof:
    let raw =
      if messages.len > 0:
        let msg = messages[0]
        messages.delete(0)
        msg
      else:
        await conn.readLp(MsgSize)
    var ms = string.fromBytes(raw)
    validateSuffix(ms)

    trace "handle: got request", conn, ms
    if ms.len() <= 0:
      trace "handle: invalid proto", conn
      await conn.writeLp(Na)
      continue

    case ms
    of "ls":
      trace "handle: listing protos", conn
      await conn.writeLp(encodeProtocolListing(protos))
    of CodecV1:
      if not isHandshaked:
        await conn.writeLp(CodecV1 & "\n")
        isHandshaked = true
      else:
        trace "handle: sending `na` for duplicate handshake while handshaked", conn
        await conn.writeLp(Na)
    elif ms in protos or matchers.anyIt(it(ms)):
      trace "found handler", conn, protocol = ms
      await conn.writeLp(ms & "\n")
      conn.protocol = ms
      return ms
    else:
      trace "no handlers", conn, protocol = ms
      await conn.writeLp(Na)

  ""

proc handleMultistreamV2(
    conn: Connection,
    protos: seq[string],
    matchers: seq[Matcher],
    pending: seq[byte],
): Future[string] {.async: (raises: [CancelledError, LPStreamError, MultiStreamError]).} =
  var message = pending
  if message.len == 0:
    message = await conn.readLp(MsgSize)
  await handleProtocolSelect(conn, protos, matchers, message)

proc processListenerHandshake(
    conn: Connection, firstMessage: seq[byte]
): Future[tuple[version: MultistreamVersion, pending: seq[byte]]] {.
    async: (raises: [CancelledError, LPStreamError, MultiStreamError])
.} =
  if firstMessage.len == 0:
    raise (ref MultiStreamError)(
      msg: "MultistreamSelect handling failed, empty handshake"
    )

  var ms = string.fromBytes(firstMessage)
  validateSuffix(ms)
  if ms != CodecV1:
    debug "expected handshake message", conn, instead = ms
    raise (ref MultiStreamError)(
      msg: "MultistreamSelect handling failed, invalid first message"
    )

  await conn.writeLp(CodecV1 & "\n")

  var pending: seq[byte] = @[]
  var version = msv1

  let nextMessage = await conn.readLp(MsgSize)
  if nextMessage.len > 0:
    var nextStr = string.fromBytes(nextMessage)
    validateSuffix(nextStr)
    if nextStr == CodecV2:
      trace "acknowledging multistream v2", conn
      await conn.writeLp(CodecV2 & "\n")
      version = msv2
    else:
      pending = nextMessage

  conn.multistreamVersion = version
  (version, pending)

proc handleProtocolSelect(
    conn: Connection, protos: seq[string], matchers: seq[Matcher], first: seq[byte]
): Future[string] {.async: (raises: [CancelledError, LPStreamError, MultiStreamError]).} =
  if first.len == 0:
    raise (ref MultiStreamError)(
      msg: "Protocol Select handling failed, empty negotiation message"
    )

  let decoded = decodeProtocolSelect(first)
  if decoded.isErr():
    debug "protocol select decode failed", conn
    raise (ref MultiStreamError)(
      msg: "Protocol Select decode failed: " & $decoded.error
    )

  let message = decoded.get()
  if message.version != ProtocolSelectVersion:
    debug "unsupported protocol select version", conn, version = message.version
    raise (ref MultiStreamError)(
      msg: "Unsupported Protocol Select version: " & $message.version
    )
  conn.multistreamVersion = msv2

  var selected = ""
  for candidate in message.protocols:
    if candidate in protos or matchers.anyIt(it(candidate)):
      selected = candidate
      break

  if selected.len == 0:
    trace "protocol select: no matching handlers", conn
    let response = encodeProtocolSelect(@[])
    await conn.writeLp(response.buffer)
    return ""

  trace "protocol select: found handler", conn, protocol = selected
  let response = encodeProtocolSelect(@[selected])
  await conn.writeLp(response.buffer)
  conn.protocol = selected
  selected

proc handle*(
    _: type MultistreamSelect,
    conn: Connection,
    protos: seq[string],
    matchers = newSeq[Matcher](),
    active: bool = false,
): Future[string] {.async: (raises: [CancelledError, LPStreamError, MultiStreamError]).} =
  trace "Starting multistream negotiation", conn, handshaked = active

  let negotiated =
    if active:
      case conn.multistreamVersion
      of msv2:
        await handleMultistreamV2(conn, protos, matchers, @[])
      else:
        await handleMultistreamV1Loop(conn, protos, matchers, @[], handshaked = true)
    else:
      let first = await conn.readLp(MsgSize)
      if first.len > 0 and first[0] == byte('/'):
        let (version, pending) = await processListenerHandshake(conn, first)
        case version
        of msv2:
          await handleMultistreamV2(conn, protos, matchers, pending)
        else:
          await handleMultistreamV1Loop(conn, protos, matchers, pending, handshaked = true)
      else:
        await handleProtocolSelect(conn, protos, matchers, first)

  if negotiated.len > 0:
    return negotiated

  ""

proc handle*(
    m: MultistreamSelect, conn: Connection, active: bool = false
) {.async: (raises: [CancelledError]).} =
  trace "Starting multistream handler", conn, handshaked = active
  var
    protos: seq[string]
    matchers: seq[Matcher]
  for h in m.handlers:
    if h.match != nil:
      matchers.add(h.match)
    for proto in h.protos:
      protos.add(proto)

  try:
    let ms = await MultistreamSelect.handle(conn, protos, matchers, active)
    for h in m.handlers:
      if (h.match != nil and h.match(ms)) or h.protos.contains(ms):
        trace "found handler", conn, protocol = ms

        var protocolHolder = h
        if not m.resourceManager.isNil:
          let permitAttempt =
            acquireStreamPermitResult(m.resourceManager, conn, ms, Direction.In)
          if not permitAttempt.ok:
            debug "Dropping inbound stream due to resource limits",
              conn, protocol = ms, err = permitAttempt.err
            await conn.close()
            return
          if not permitAttempt.permit.isNil:
            m.resourceManager.attachPermit(permitAttempt.permit, conn)

        let maxIncomingStreams = protocolHolder.protocol.maxIncomingStreams
        let currentStreams = protocolHolder.openedStreams.getOrDefault(conn.peerId)
        if maxIncomingStreams > 0 and currentStreams >= maxIncomingStreams:
          debug "Max streams for protocol reached, blocking new stream",
            conn, protocol = ms, maxIncomingStreams
          return
        protocolHolder.openedStreams.inc(conn.peerId)
        try:
          await protocolHolder.protocol.handler(conn, ms)
        finally:
          protocolHolder.openedStreams.inc(conn.peerId, -1)
          if protocolHolder.openedStreams.getOrDefault(conn.peerId) <= 0:
            protocolHolder.openedStreams.del(conn.peerId)
        return
    debug "no handlers", conn, ms
  except CancelledError as exc:
    raise exc
  except CatchableError as exc:
    trace "Exception in multistream", conn, description = exc.msg
  finally:
    await conn.close()

  trace "Stopped multistream handler", conn

proc addHandler*(
    m: MultistreamSelect,
    codecs: seq[string],
    protocol: LPProtocol,
    matcher: Matcher = nil,
) =
  trace "registering protocols", protos = codecs
  m.handlers.add(
    HandlerHolder(
      protos: codecs,
      protocol: protocol,
      match: matcher,
      openedStreams: initCountTable[PeerId](),
    )
  )

proc addHandler*(
    m: MultistreamSelect, codec: string, protocol: LPProtocol, matcher: Matcher = nil
) =
  addHandler(m, @[codec], protocol, matcher)

proc addHandler*[E](
    m: MultistreamSelect,
    codec: string,
    handler:
      LPProtoHandler |
      proc(conn: Connection, proto: string): InternalRaisesFuture[void, E],
    matcher: Matcher = nil,
) =
  ## helper to allow registering pure handlers
  trace "registering proto handler", proto = codec
  let protocol = new LPProtocol
  protocol.codec = codec
  protocol.handler = handler

  m.handlers.add(
    HandlerHolder(
      protos: @[codec],
      protocol: protocol,
      match: matcher,
      openedStreams: initCountTable[PeerId](),
    )
  )

proc start*(m: MultistreamSelect) {.async: (raises: [CancelledError]).} =
  let futs = m.handlers.mapIt(it.protocol.start())
  try:
    await allFutures(futs)
    for fut in futs:
      await fut
  except CancelledError as exc:
    var pending: seq[Future[void].Raising([])]
    doAssert m.handlers.len == futs.len, "Handlers modified while starting"
    for i, fut in futs:
      if not fut.finished:
        pending.add fut.cancelAndWait()
      elif fut.completed:
        pending.add m.handlers[i].protocol.stop()
      else:
        static:
          doAssert typeof(fut).E is (CancelledError,)
    await noCancel allFutures(pending)
    raise exc

proc stop*(m: MultistreamSelect) {.async: (raises: []).} =
  let futs = m.handlers.mapIt(it.protocol.stop())
  await noCancel allFutures(futs)
  for fut in futs:
    await fut
