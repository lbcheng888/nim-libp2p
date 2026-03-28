{.push raises: [].}

import std/[json, options, os, strutils, tables, uri]
import chronos

import ../../stream/lpstream
import ../../utility
import ../msquicdriver as msquicdrv
import ../msquicstream
import ../qpackhuffman
import "../nim-msquic/api/event_model" as msevents
import "../nim-msquic/tls/common" as mstlstypes
import ./control

template h3Safe(body: untyped) =
  {.cast(gcsafe).}:
    body

const
  NimTsnetH3Prefix* = "/nim-tsnet-control/v1"
  H3ConnectTimeout = 5.seconds
  H3PeerSettingsTimeout = 1.seconds
  H3ResponseFrameTimeout = 5.seconds
  MsQuicAddressFamilyUnspecified = 0'u16
  MsQuicAddressFamilyIpv4 = 2'u16
  MsQuicAddressFamilyIpv6 = 23'u16
  Http3FrameData = 0'u64
  Http3FrameHeaders = 1'u64
  Http3FrameSettings = 4'u64
  Http3StreamTypeControl = 0'u64
  Http3StreamTypeQpackEncoder = 2'u64
  Http3StreamTypeQpackDecoder = 3'u64
  Http3SettingsQpackMaxTableCapacity = 0x1'u64
  Http3SettingsMaxFieldSectionSize = 0x6'u64
  Http3SettingsQpackBlockedStreams = 0x7'u64

type
  TsnetH3Endpoint* = object
    url*: string
    scheme*: string
    host*: string
    authority*: string
    pathWithQuery*: string
    port*: uint16

  TsnetH3Response = object
    status: int
    headers: Table[string, string]
    body: seq[byte]

  TsnetH3StreamCursor = ref object
    stream: MsQuicStream
    buffer: seq[byte]

proc bytesToString(bytes: openArray[byte]): string =
  result = newString(bytes.len)
  for i, value in bytes:
    result[i] = char(value)

proc stringToBytes(text: string): seq[byte] =
  result = newSeqOfCap[byte](text.len)
  for ch in text:
    result.add(byte(ord(ch)))

proc defaultH3Port(scheme: string): uint16 =
  if scheme.toLowerAscii() == "http":
    80'u16
  else:
    4443'u16

proc detectAddressFamily(host: string): uint16 =
  if host.len == 0:
    return MsQuicAddressFamilyUnspecified
  if host.contains(':'):
    return MsQuicAddressFamilyIpv6
  let parts = host.split('.')
  if parts.len != 4:
    return MsQuicAddressFamilyUnspecified
  for part in parts:
    if part.len == 0 or part.len > 3:
      return MsQuicAddressFamilyUnspecified
    for ch in part:
      if ch < '0' or ch > '9':
        return MsQuicAddressFamilyUnspecified
    try:
      let value = parseInt(part)
      if value < 0 or value > 255:
        return MsQuicAddressFamilyUnspecified
    except CatchableError:
      return MsQuicAddressFamilyUnspecified
  MsQuicAddressFamilyIpv4

proc defaultCaBundleFile(): Option[string] =
  for candidate in [
    "/etc/ssl/cert.pem",
    "/etc/ssl/certs/ca-certificates.crt",
    "/etc/pki/tls/certs/ca-bundle.crt",
    "/etc/ssl/ca-bundle.pem"
  ]:
    if fileExists(candidate):
      return some(candidate)
  none(string)

proc bracketAuthorityHost(host: string): string =
  if host.contains(":") and not host.startsWith("["):
    "[" & host & "]"
  else:
    host

proc h3BaseUri(controlUrl: string): Result[Uri, string] =
  let normalized = normalizeControlUrl(controlUrl)
  if normalized.len == 0:
    return err("tsnet controlUrl is empty")
  try:
    var parsed = parseUri(normalized)
    if parsed.scheme.len == 0:
      parsed.scheme = "https"
    if parsed.hostname.len == 0:
      return err("tsnet controlUrl is missing a hostname")
    parsed.path = NimTsnetH3Prefix
    parsed.query = ""
    parsed.anchor = ""
    if parsed.port.len == 0 or parsed.port.parseInt() != 4443:
      parsed.port = "4443"
    ok(parsed)
  except CatchableError as exc:
    err("failed to parse tsnet controlUrl " & normalized & ": " & exc.msg)

proc endpointFromUri(parsed: Uri, pathWithQuery: string): TsnetH3Endpoint =
  let portValue =
    if parsed.port.len > 0:
      try:
        uint16(parsed.port.parseInt())
      except CatchableError:
        defaultH3Port(parsed.scheme)
    else:
      defaultH3Port(parsed.scheme)
  let authorityHost = bracketAuthorityHost(parsed.hostname)
  let authority =
    if (parsed.scheme == "https" and portValue == 443'u16) or
        (parsed.scheme == "http" and portValue == 80'u16):
      authorityHost
    else:
      authorityHost & ":" & $portValue
  TsnetH3Endpoint(
    url: parsed.scheme & "://" & authority & pathWithQuery,
    scheme: parsed.scheme,
    host: parsed.hostname,
    authority: authority,
    pathWithQuery: pathWithQuery,
    port: portValue
  )

proc nimH3BaseUrl*(controlUrl: string): string =
  let parsed = h3BaseUri(controlUrl).valueOr:
    return ""
  let endpoint = endpointFromUri(parsed, NimTsnetH3Prefix)
  endpoint.url

proc nimH3HealthUrl*(controlUrl: string): string =
  let parsed = h3BaseUri(controlUrl).valueOr:
    return ""
  endpointFromUri(parsed, NimTsnetH3Prefix & "/health").url

proc nimH3KeyUrl*(controlUrl: string, capabilityVersion: int): string =
  let parsed = h3BaseUri(controlUrl).valueOr:
    return ""
  endpointFromUri(
    parsed,
    NimTsnetH3Prefix & "/key?v=" & $capabilityVersion
  ).url

proc h3Endpoint(
    controlUrl: string,
    route: string,
    query = ""
): Result[TsnetH3Endpoint, string] =
  let parsed = h3BaseUri(controlUrl).valueOr:
    return err(error)
  var pathWithQuery = NimTsnetH3Prefix
  if route.len > 0 and route[0] == '/':
    pathWithQuery &= route
  elif route.len > 0:
    pathWithQuery &= "/" & route
  if query.len > 0:
    pathWithQuery &= "?" & query
  ok(endpointFromUri(parsed, pathWithQuery))

proc appendBytes(dst: var seq[byte], src: openArray[byte]) =
  dst.setLen(dst.len + src.len)
  if src.len > 0:
    copyMem(addr dst[dst.len - src.len], unsafeAddr src[0], src.len)

proc dropPrefix(bytes: var seq[byte], count: int) =
  if count <= 0:
    return
  if count >= bytes.len:
    bytes.setLen(0)
    return
  var trimmed = newSeq[byte](bytes.len - count)
  copyMem(addr trimmed[0], addr bytes[count], trimmed.len)
  bytes = trimmed

proc safeCloseStream(
    handle: msquicdrv.MsQuicTransportHandle,
    streamPtr: pointer,
    streamState: msquicdrv.MsQuicStreamState
) {.gcsafe, raises: [].} =
  if handle.isNil or streamPtr.isNil or streamState.isNil:
    return
  try:
    h3Safe:
      msquicdrv.closeStream(handle, streamPtr, streamState)
  except CatchableError:
    discard

proc safeShutdownConnection(
    handle: msquicdrv.MsQuicTransportHandle,
    connPtr: pointer
) {.gcsafe, raises: [].} =
  if handle.isNil or connPtr.isNil:
    return
  try:
    h3Safe:
      discard msquicdrv.shutdownConnection(handle, connPtr)
  except CatchableError:
    discard

proc safeCloseConnection(
    handle: msquicdrv.MsQuicTransportHandle,
    connPtr: pointer,
    connState: msquicdrv.MsQuicConnectionState
) {.gcsafe, raises: [].} =
  if handle.isNil or connPtr.isNil or connState.isNil:
    return
  try:
    h3Safe:
      msquicdrv.closeConnection(handle, connPtr, connState)
  except CatchableError:
    discard

proc safeShutdownRuntime(handle: msquicdrv.MsQuicTransportHandle) {.gcsafe, raises: [].} =
  if handle.isNil:
    return
  try:
    h3Safe:
      msquicdrv.shutdown(handle)
  except CatchableError:
    discard

proc safeStreamId(
    streamState: msquicdrv.MsQuicStreamState
): Result[uint64, string] {.gcsafe, raises: [].} =
  if streamState.isNil:
    return err("nil H3 stream state")
  try:
    h3Safe:
      result = msquicdrv.streamId(streamState)
  except CatchableError as exc:
    return err("failed to query H3 stream id: " & exc.msg)

proc encodeQuicVarInt(value: uint64): seq[byte] =
  if value < (1'u64 shl 6):
    result = @[byte(value)]
  elif value < (1'u64 shl 14):
    result = @[
      byte(0x40'u8 or byte((value shr 8) and 0x3f)),
      byte(value and 0xff)
    ]
  elif value < (1'u64 shl 30):
    result = @[
      byte(0x80'u8 or byte((value shr 24) and 0x3f)),
      byte((value shr 16) and 0xff),
      byte((value shr 8) and 0xff),
      byte(value and 0xff)
    ]
  else:
    result = @[
      byte(0xC0'u8 or byte((value shr 56) and 0x3f)),
      byte((value shr 48) and 0xff),
      byte((value shr 40) and 0xff),
      byte((value shr 32) and 0xff),
      byte((value shr 24) and 0xff),
      byte((value shr 16) and 0xff),
      byte((value shr 8) and 0xff),
      byte(value and 0xff)
    ]

proc decodeQuicVarIntFrom(data: openArray[byte], pos: var int): uint64 {.raises: [ValueError].} =
  if pos >= data.len:
    raise newException(ValueError, "unexpected EOF decoding QUIC varint")
  let first = data[pos]
  inc pos
  case first shr 6
  of 0:
    uint64(first and 0x3f)
  of 1:
    if pos + 1 > data.len:
      raise newException(ValueError, "unexpected EOF decoding 2-byte QUIC varint")
    let b1 = data[pos]
    inc pos
    (uint64(first and 0x3f) shl 8) or uint64(b1)
  of 2:
    if pos + 3 > data.len:
      raise newException(ValueError, "unexpected EOF decoding 4-byte QUIC varint")
    let b1 = data[pos]
    let b2 = data[pos + 1]
    let b3 = data[pos + 2]
    inc pos, 3
    (uint64(first and 0x3f) shl 24) or
      (uint64(b1) shl 16) or
      (uint64(b2) shl 8) or
      uint64(b3)
  else:
    if pos + 7 > data.len:
      raise newException(ValueError, "unexpected EOF decoding 8-byte QUIC varint")
    let b1 = data[pos]
    let b2 = data[pos + 1]
    let b3 = data[pos + 2]
    let b4 = data[pos + 3]
    let b5 = data[pos + 4]
    let b6 = data[pos + 5]
    let b7 = data[pos + 6]
    inc pos, 7
    (uint64(first and 0x3f) shl 56) or
      (uint64(b1) shl 48) or
      (uint64(b2) shl 40) or
      (uint64(b3) shl 32) or
      (uint64(b4) shl 24) or
      (uint64(b5) shl 16) or
      (uint64(b6) shl 8) or
      uint64(b7)

proc initCursor(stream: MsQuicStream): TsnetH3StreamCursor =
  TsnetH3StreamCursor(stream: stream, buffer: @[])

proc discardStream(stream: MsQuicStream) {.async: (raises: []).} =
  if stream.isNil:
    return
  var cursor = initCursor(stream)
  try:
    while true:
      let chunk = await cursor.stream.read()
      if chunk.len == 0:
        break
  except CatchableError:
    discard
  finally:
    try:
      await noCancel stream.closeImpl()
    except CatchableError:
      discard

proc ensureBytes(cursor: TsnetH3StreamCursor, needed: int)
    {.async: (raises: [CancelledError, LPStreamError]).} =
  while cursor.buffer.len < needed:
    let chunk = await cursor.stream.read()
    if chunk.len == 0:
      raise newLPStreamEOFError()
    cursor.buffer.appendBytes(chunk)

proc readQuicVarInt(
    cursor: TsnetH3StreamCursor
): Future[uint64] {.async: (raises: [CancelledError, LPStreamError, ValueError]).} =
  await cursor.ensureBytes(1)
  let first = cursor.buffer[0]
  let required =
    case first shr 6
    of 0: 1
    of 1: 2
    of 2: 4
    else: 8
  await cursor.ensureBytes(required)
  var pos = 0
  let value = decodeQuicVarIntFrom(cursor.buffer.toOpenArray(0, required - 1), pos)
  cursor.buffer.dropPrefix(required)
  value

proc readBytes(
    cursor: TsnetH3StreamCursor,
    count: int
): Future[seq[byte]] {.async: (raises: [CancelledError, LPStreamError]).} =
  if count <= 0:
    return @[]
  await cursor.ensureBytes(count)
  result = newSeq[byte](count)
  copyMem(addr result[0], addr cursor.buffer[0], count)
  cursor.buffer.dropPrefix(count)

proc encodePrefixedInt(prefixBase: byte, prefixBits: int, value: uint64): seq[byte] =
  let mask = (1 shl prefixBits) - 1
  var first = prefixBase
  if value < mask.uint64:
    first = first or byte(value)
    result.add(first)
  else:
    first = first or byte(mask)
    result.add(first)
    var extra = value - mask.uint64
    while true:
      var b = byte(extra and 0x7f)
      extra = extra shr 7
      if extra != 0:
        result.add(b or 0x80)
      else:
        result.add(b)
        break

proc decodePrefixedInt(
    firstValue: byte,
    prefixBits: int,
    data: seq[byte],
    pos: var int
): uint64 {.raises: [ValueError].} =
  let mask = (1 shl prefixBits) - 1
  var value = uint64(firstValue and byte(mask))
  if value < mask.uint64:
    return value
  var multiplier = 0
  var extra = 0'u64
  while true:
    if pos >= data.len:
      raise newException(ValueError, "unexpected EOF decoding prefixed int")
    let b = data[pos]
    inc pos
    extra += uint64(b and 0x7f) shl multiplier
    if (b and 0x80) == 0:
      break
    multiplier += 7
  mask.uint64 + extra

proc encodeStringLiteralCustom(
    prefixBits: int,
    prefixBase: byte,
    value: string
): seq[byte] {.gcsafe, raises: [].} =
  let mask = (1 shl prefixBits) - 1
  let encodedHuffman = qpackHuffmanEncode(value)
  let useHuffman = encodedHuffman.len < value.len
  let payloadLen =
    if useHuffman: encodedHuffman.len.uint64
    else: value.len.uint64
  let huffmanBit =
    if useHuffman: byte(1 shl prefixBits)
    else: 0'u8
  var first = prefixBase
  if payloadLen < mask.uint64:
    first = first or huffmanBit or byte(payloadLen)
    result.add(first)
  else:
    first = first or huffmanBit or byte(mask)
    result.add(first)
    var extra = payloadLen - mask.uint64
    while true:
      var b = byte(extra and 0x7f)
      extra = extra shr 7
      if extra != 0:
        result.add(b or 0x80)
      else:
        result.add(b)
        break
  if useHuffman:
    result.add(encodedHuffman)
  else:
    for ch in value:
      result.add(byte(ch))

proc encodeStringLiteral(prefixBits: int, value: string): seq[byte] {.gcsafe, raises: [].} =
  encodeStringLiteralCustom(prefixBits, 0, value)

proc qpackHuffmanDecodeSafe(data: openArray[byte]): string {.gcsafe, raises: [ValueError].} =
  {.cast(gcsafe).}:
    qpackHuffmanDecode(data)

proc decodeStringLiteral(
    data: seq[byte],
    pos: var int,
    prefixBits: int
): string {.gcsafe, raises: [ValueError].} =
  if pos >= data.len:
    raise newException(ValueError, "unexpected EOF decoding string literal")
  let first = data[pos]
  inc pos
  let huffman = ((first shr prefixBits) and 0x1) == 1
  let mask = (1 shl prefixBits) - 1
  var length = uint64(first and mask.byte)
  if length == mask.uint64:
    var multiplier = 0
    while true:
      if pos >= data.len:
        raise newException(ValueError, "unexpected EOF decoding string literal length")
      let b = data[pos]
      inc pos
      length += uint64(b and 0x7f) shl multiplier
      if (b and 0x80) == 0:
        break
      multiplier += 7
  if pos + int(length) > data.len:
    raise newException(ValueError, "unexpected EOF decoding string literal payload")
  let endPos = pos + int(length)
  if huffman:
    result = qpackHuffmanDecodeSafe(data.toOpenArray(pos, endPos - 1))
    pos = endPos
  else:
    result = newString(int(length))
    var idx = 0
    while pos < endPos:
      result[idx] = char(data[pos])
      inc idx
      inc pos

proc qpackIndexedStatic(index: uint64): seq[byte] =
  encodePrefixedInt(0b11000000'u8, 6, index)

proc qpackLiteralWithNameRefStatic(index: uint64, value: string): seq[byte] =
  result = encodePrefixedInt(0b01010000'u8, 4, index)
  result.add(encodeStringLiteral(7, value))

proc qpackLiteralWithLiteralName(name, value: string): seq[byte] =
  result = encodeStringLiteralCustom(3, 0b00100000'u8, name)
  result.add(encodeStringLiteral(7, value))

proc encodeHeadersFrame(payload: seq[byte]): seq[byte] =
  result = encodeQuicVarInt(Http3FrameHeaders)
  result.appendBytes(encodeQuicVarInt(uint64(payload.len)))
  result.appendBytes(payload)

proc encodeDataFrame(payload: seq[byte]): seq[byte] =
  result = encodeQuicVarInt(Http3FrameData)
  result.appendBytes(encodeQuicVarInt(uint64(payload.len)))
  result.appendBytes(payload)

proc qpackStaticHeader(index: uint64): (string, string) {.raises: [ValueError].} =
  case index
  of 0'u64: (":authority", "")
  of 1'u64: (":path", "/")
  of 15'u64: (":method", "CONNECT")
  of 17'u64: (":method", "GET")
  of 20'u64: (":method", "POST")
  of 22'u64: (":scheme", "http")
  of 23'u64: (":scheme", "https")
  of 25'u64: (":status", "200")
  of 27'u64: (":status", "404")
  of 67'u64: (":status", "400")
  of 71'u64: (":status", "500")
  else:
    raise newException(ValueError, "unsupported static table index: " & $index)

proc decodeHeadersBlock(payload: seq[byte]): Table[string, string] {.raises: [ValueError].} =
  var pos = 0
  if payload.len < 2:
    raise newException(ValueError, "invalid QPACK field section")
  discard decodePrefixedInt(payload[pos], 8, payload, pos)
  discard decodePrefixedInt(payload[pos] and 0x7f, 7, payload, pos)
  result = initTable[string, string]()
  while pos < payload.len:
    let first = payload[pos]
    inc pos
    if (first and 0x80) != 0:
      let isStatic = ((first shr 6) and 0x1) == 1
      let index = decodePrefixedInt(first and 0x3f, 6, payload, pos)
      if not isStatic:
        raise newException(ValueError, "dynamic table references are not supported")
      let entry = qpackStaticHeader(index)
      result[entry[0]] = entry[1]
    elif (first and 0xC0) == 0x40:
      let isStatic = ((first shr 4) and 0x1) == 1
      if not isStatic:
        raise newException(ValueError, "dynamic table references are not supported")
      let index = decodePrefixedInt(first and 0x0f, 4, payload, pos)
      let entry = qpackStaticHeader(index)
      result[entry[0]] = decodeStringLiteral(payload, pos, 7)
    elif (first and 0xE0) == 0x20:
      dec pos
      let name = decodeStringLiteral(payload, pos, 3)
      let value = decodeStringLiteral(payload, pos, 7)
      result[name] = value
    else:
      raise newException(ValueError, "unsupported QPACK representation in headers block")

proc buildSettingsPayload(): seq[byte] =
  result.appendBytes(encodeQuicVarInt(Http3SettingsQpackMaxTableCapacity))
  result.appendBytes(encodeQuicVarInt(0))
  result.appendBytes(encodeQuicVarInt(Http3SettingsMaxFieldSectionSize))
  result.appendBytes(encodeQuicVarInt(65535))
  result.appendBytes(encodeQuicVarInt(Http3SettingsQpackBlockedStreams))
  result.appendBytes(encodeQuicVarInt(0))

proc buildRequestHeaders(
    httpMethod: string,
    endpoint: TsnetH3Endpoint,
    hasBody: bool,
    bodyLen: int
): seq[byte] =
  result.add(@[0x00'u8, 0x00'u8])
  let normalizedMethod = httpMethod.strip().toUpperAscii()
  case normalizedMethod
  of "GET":
    result.appendBytes(qpackIndexedStatic(17))
  of "POST":
    result.appendBytes(qpackIndexedStatic(20))
  else:
    result.appendBytes(qpackLiteralWithLiteralName(":method", normalizedMethod))
  result.appendBytes(qpackIndexedStatic(23))
  result.appendBytes(qpackLiteralWithNameRefStatic(1, endpoint.pathWithQuery))
  result.appendBytes(qpackLiteralWithNameRefStatic(0, endpoint.authority))
  result.appendBytes(qpackLiteralWithLiteralName("accept", "application/json"))
  if hasBody:
    result.appendBytes(qpackLiteralWithLiteralName("content-type", "application/json"))
    result.appendBytes(qpackLiteralWithLiteralName("content-length", $bodyLen))

proc awaitConnected(connState: msquicdrv.MsQuicConnectionState)
    {.async: (raises: [CancelledError, CatchableError]).} =
  while true:
    let event =
      try:
        await msquicdrv.nextConnectionEvent(connState)
      except msquicdrv.MsQuicEventQueueClosed:
        raise newException(ValueError, "HTTP/3 connection closed before CONNECTED")
    case event.kind
    of msevents.ceConnected:
      return
    of msevents.ceShutdownInitiated, msevents.ceShutdownComplete:
      raise newException(ValueError, "HTTP/3 connection shutdown while dialing")
    else:
      discard

proc createOutboundStream(
    handle: msquicdrv.MsQuicTransportHandle,
    connPtr: pointer,
    connState: msquicdrv.MsQuicConnectionState,
    unidirectional: bool
): Result[MsQuicStream, string] =
  let flags = if unidirectional: 0x0001'u32 else: 0'u32
  let created =
    msquicdrv.createStream(
      handle,
      connPtr,
      flags = flags,
      connectionState = connState
    )
  if created.error.len > 0 or created.state.isNone or created.stream.isNil:
    return err(
      "failed to create H3 stream: " &
      (if created.error.len > 0: created.error else: "unknown error")
    )
  let startErr = msquicdrv.startStream(handle, created.stream)
  if startErr.len > 0:
    safeCloseStream(handle, created.stream, created.state.get())
    return err("failed to start H3 stream: " & startErr)
  try:
    ok(newMsQuicStream(created.state.get(), handle, Direction.Out))
  except LPStreamError as exc:
    safeCloseStream(handle, created.stream, created.state.get())
    err("failed to wrap outbound H3 stream: " & exc.msg)

proc awaitIncomingUniStream(
    handle: msquicdrv.MsQuicTransportHandle,
    connState: msquicdrv.MsQuicConnectionState
): Future[MsQuicStream] {.async: (raises: [CancelledError, CatchableError]).} =
  while true:
    var pending = msquicdrv.popPendingStreamState(connState)
    if pending.isNone:
      pending = some(await msquicdrv.awaitPendingStreamState(connState))
    if pending.isNone:
      continue
    let streamState = pending.get()
    if streamState.isNil or streamState.stream.isNil:
      continue
    let idRes = safeStreamId(streamState)
    if idRes.isErr():
      safeCloseStream(handle, cast[pointer](streamState.stream), streamState)
      continue
    let isUnidirectional = (idRes.get() and 0x2'u64) != 0'u64
    if not isUnidirectional:
      safeCloseStream(handle, cast[pointer](streamState.stream), streamState)
      continue
    try:
      return newMsQuicStream(streamState, handle, Direction.In)
    except LPStreamError:
      safeCloseStream(handle, cast[pointer](streamState.stream), streamState)
      continue

proc readPeerSettings(
    handle: msquicdrv.MsQuicTransportHandle,
    connState: msquicdrv.MsQuicConnectionState
): Future[void] {.async: (raises: [CancelledError, CatchableError]).} =
  while true:
    let stream = await awaitIncomingUniStream(handle, connState)
    var cursor = initCursor(stream)
    let streamType =
      try:
        await cursor.readQuicVarInt()
      except LPStreamEOFError:
        await noCancel stream.closeImpl()
        continue
    if streamType == Http3StreamTypeControl:
      while true:
        let frameType =
          try:
            await cursor.readQuicVarInt()
          except LPStreamEOFError:
            raise newException(ValueError, "HTTP/3 peer control stream closed before SETTINGS")
        let frameLen =
          try:
            await cursor.readQuicVarInt()
          except LPStreamEOFError:
            raise newException(ValueError, "HTTP/3 peer control stream truncated")
        discard await cursor.readBytes(int(frameLen))
        if frameType == Http3FrameSettings:
          await noCancel stream.closeImpl()
          return
    elif streamType in [Http3StreamTypeQpackEncoder, Http3StreamTypeQpackDecoder]:
      asyncSpawn discardStream(stream)
      continue
    else:
      await noCancel stream.closeImpl()

proc executeH3Request(
    endpoint: TsnetH3Endpoint,
    httpMethod: string,
    bodyNode: JsonNode = nil
): Result[TsnetH3Response, string] =
  let cfg = msquicdrv.MsQuicTransportConfig(
    alpns: @["h3"],
    appName: "nim-tsnet-control-h3"
  )
  let (handle, initErr) = msquicdrv.initMsQuicTransport(cfg)
  if handle.isNil:
    return err("failed to initialize builtin H3 runtime: " & initErr)
  defer:
    safeShutdownRuntime(handle)

  let clientTlsCfg = mstlstypes.TlsConfig(
    role: mstlstypes.tlsClient,
    alpns: @["h3"],
    transportParameters: @[],
    serverName: some(endpoint.host),
    certificatePem: none(string),
    privateKeyPem: none(string),
    certificateFile: none(string),
    privateKeyFile: none(string),
    privateKeyPassword: none(string),
    pkcs12File: none(string),
    pkcs12Data: none(seq[uint8]),
    pkcs12Password: none(string),
    certificateHash: none(mstlstypes.TlsCertificateHash),
    certificateStore: none(string),
    certificateStoreFlags: 0'u32,
    certificateContext: none(pointer),
    caCertificateFile: defaultCaBundleFile(),
    resumptionTicket: none(seq[uint8]),
    enableZeroRtt: false,
    useSharedSessionCache: true,
    disableCertificateValidation: false,
    requireClientAuth: false,
    enableOcsp: false,
    indicateCertificateReceived: false,
    deferCertificateValidation: false,
    useBuiltinCertificateValidation: false,
    allowedCipherSuites: none(uint32),
    tempDirectory: none(string)
  )
  let credErr = msquicdrv.loadCredential(handle, clientTlsCfg)
  if credErr.len > 0:
    return err("failed to load H3 client credential: " & credErr)

  let dialed = msquicdrv.dialConnection(
    handle,
    endpoint.host,
    endpoint.port,
    addressFamily = detectAddressFamily(endpoint.host)
  )
  if dialed.error.len > 0 or dialed.connection.isNil or dialed.state.isNone:
    return err("failed to dial H3 control endpoint " & endpoint.url & ": " & dialed.error)
  let connPtr = dialed.connection
  let connState = dialed.state.get()
  defer:
    safeShutdownConnection(handle, connPtr)
    safeCloseConnection(handle, connPtr, connState)

  try:
    let connectFuture = awaitConnected(connState)
    let connected =
      try:
        waitFor connectFuture.withTimeout(H3ConnectTimeout)
      except CancelledError as exc:
        return err("H3 connect cancelled for " & endpoint.url & ": " & exc.msg)
    if not connected:
      return err("H3 connect timed out for " & endpoint.url)
  except CatchableError as exc:
    return err("H3 connect failed for " & endpoint.url & ": " & exc.msg)

  let controlStream = createOutboundStream(handle, connPtr, connState, true).valueOr:
    return err(error)
  defer:
    try:
      waitFor noCancel controlStream.closeImpl()
    except CatchableError:
      discard
  try:
    var settingsFrame = encodeQuicVarInt(Http3FrameSettings)
    let settingsPayload = buildSettingsPayload()
    settingsFrame.appendBytes(encodeQuicVarInt(uint64(settingsPayload.len)))
    settingsFrame.appendBytes(settingsPayload)
    var controlPayload = encodeQuicVarInt(Http3StreamTypeControl)
    controlPayload.appendBytes(settingsFrame)
    waitFor controlStream.write(controlPayload)
    waitFor controlStream.sendFin()
  except CatchableError as exc:
    return err("failed to send H3 control stream: " & exc.msg)

  block readSettings:
    try:
      let settingsFuture = readPeerSettings(handle, connState)
      let settingsReady =
        try:
          waitFor settingsFuture.withTimeout(H3PeerSettingsTimeout)
        except CancelledError:
          false
      if not settingsReady:
        break readSettings
    except CatchableError as exc:
      return err("failed to read H3 peer settings: " & exc.msg)

  let requestStream = createOutboundStream(handle, connPtr, connState, false).valueOr:
    return err(error)
  defer:
    try:
      waitFor noCancel requestStream.closeImpl()
    except CatchableError:
      discard
  let bodyText =
    if bodyNode.isNil or bodyNode.kind == JNull:
      ""
    else:
      $bodyNode
  let bodyBytes =
    if bodyText.len == 0:
      @[]
    else:
      stringToBytes(bodyText)
  let headersBlock =
    buildRequestHeaders(
      httpMethod,
      endpoint,
      hasBody = bodyBytes.len > 0,
      bodyLen = bodyBytes.len
    )
  var payload = encodeHeadersFrame(headersBlock)
  if bodyBytes.len > 0:
    payload.appendBytes(encodeDataFrame(bodyBytes))
  try:
    waitFor requestStream.write(payload)
    waitFor requestStream.sendFin()
  except CatchableError as exc:
    return err("failed to send H3 request " & endpoint.url & ": " & exc.msg)

  var cursor = initCursor(requestStream)
  var response = TsnetH3Response(status: 0, headers: initTable[string, string](), body: @[])
  while true:
    let frameTypeFuture = cursor.readQuicVarInt()
    let frameTypeReady =
      try:
        waitFor frameTypeFuture.withTimeout(H3ResponseFrameTimeout)
      except CancelledError as exc:
        return err("H3 response frame wait cancelled at " & endpoint.url & ": " & exc.msg)
    if not frameTypeReady:
      return err("timed out waiting for H3 response frame at " & endpoint.url)
    let frameType =
      try:
        waitFor frameTypeFuture
      except LPStreamEOFError:
        break
      except CatchableError as exc:
        return err("failed to read H3 response frame type: " & exc.msg)
    let frameLenFuture = cursor.readQuicVarInt()
    let frameLenReady =
      try:
        waitFor frameLenFuture.withTimeout(H3ResponseFrameTimeout)
      except CancelledError as exc:
        return err("H3 response length wait cancelled at " & endpoint.url & ": " & exc.msg)
    if not frameLenReady:
      return err("timed out waiting for H3 response frame length at " & endpoint.url)
    let frameLen =
      try:
        waitFor frameLenFuture
      except CatchableError as exc:
        return err("failed to read H3 response frame length: " & exc.msg)
    let framePayload =
      try:
        let payloadFuture = cursor.readBytes(int(frameLen))
        let payloadReady =
          try:
            waitFor payloadFuture.withTimeout(H3ResponseFrameTimeout)
          except CancelledError as exc:
            return err("H3 response payload wait cancelled at " & endpoint.url & ": " & exc.msg)
        if not payloadReady:
          return err("timed out waiting for H3 response payload at " & endpoint.url)
        waitFor payloadFuture
      except CatchableError as exc:
        return err("failed to read H3 response frame payload: " & exc.msg)
    case frameType
    of Http3FrameHeaders:
      let headers =
        try:
          decodeHeadersBlock(framePayload)
        except CatchableError as exc:
          return err("failed to decode H3 response headers: " & exc.msg)
      for key, value in headers.pairs:
        response.headers[key] = value
      if headers.hasKey(":status"):
        try:
          response.status = headers[":status"].parseInt()
        except CatchableError:
          response.status = 0
    of Http3FrameData:
      response.body.appendBytes(framePayload)
    else:
      discard
  ok(response)

proc h3JsonResponse(
    controlUrl: string,
    httpMethod: string,
    route: string,
    query = "",
    bodyNode: JsonNode = nil
): Result[JsonNode, string] =
  let endpoint = h3Endpoint(controlUrl, route, query).valueOr:
    return err(error)
  let response = executeH3Request(endpoint, httpMethod, bodyNode).valueOr:
    return err(error)
  if response.status < 200 or response.status >= 300:
    let bodyText =
      if response.body.len == 0: ""
      else:
        try:
          bytesToString(response.body)
        except Exception:
          ""
    return err(
      "H3 control request failed with HTTP " & $response.status & " at " &
      endpoint.url & (if bodyText.len > 0: ": " & bodyText else: "")
    )
  try:
    if response.body.len == 0:
      return ok(newJObject())
    ok(parseJson(bytesToString(response.body)))
  except CatchableError as exc:
    err("failed to parse H3 control JSON response from " & endpoint.url & ": " & exc.msg)

proc fetchControlServerKeyH3*(
    controlUrl: string,
    capabilityVersion: int
): Result[TsnetControlServerKey, string] =
  let payload = h3JsonResponse(
    controlUrl,
    "GET",
    "key",
    "v=" & $capabilityVersion
  ).valueOr:
    return err(error)
  parseControlServerKey(
    payload,
    capabilityVersion = capabilityVersion
  )

proc fetchControlServerKeyH3*(
    controlUrl: string,
    capabilityVersions: openArray[int]
): Result[TsnetControlServerKey, string] =
  var failures: seq[string] = @[]
  for version in capabilityVersions:
    let fetched = fetchControlServerKeyH3(controlUrl, version)
    if fetched.isOk():
      return fetched
    failures.add(fetched.error)
  if failures.len == 0:
    return err("no tsnet H3 capability versions were provided")
  err(failures.join("; "))

proc fetchControlServerKeyH3*(controlUrl: string): Result[TsnetControlServerKey, string] =
  fetchControlServerKeyH3(controlUrl, TsnetDefaultControlCapabilityVersions)

proc registerNodeH3*(controlUrl: string, request: JsonNode): Result[JsonNode, string] =
  let payload = h3JsonResponse(
    controlUrl,
    "POST",
    "register",
    bodyNode = %*{"request": request}
  ).valueOr:
    return err(error)
  ok(payload)

proc mapPollH3*(controlUrl: string, request: JsonNode): Result[JsonNode, string] =
  let payload = h3JsonResponse(
    controlUrl,
    "POST",
    "map",
    bodyNode = %*{"request": request}
  ).valueOr:
    return err(error)
  ok(payload)

proc healthH3*(controlUrl: string): Result[JsonNode, string] =
  h3JsonResponse(controlUrl, "GET", "health")

proc fetchControlServerKeyH3Safe*(
    controlUrl: string
): Result[TsnetControlServerKey, string] {.gcsafe, raises: [].} =
  h3Safe:
    result = fetchControlServerKeyH3(controlUrl)

proc registerNodeH3Safe*(
    controlUrl: string,
    request: JsonNode
): Result[JsonNode, string] {.gcsafe, raises: [].} =
  h3Safe:
    result = registerNodeH3(controlUrl, request)

proc mapPollH3Safe*(
    controlUrl: string,
    request: JsonNode
): Result[JsonNode, string] {.gcsafe, raises: [].} =
  h3Safe:
    result = mapPollH3(controlUrl, request)
