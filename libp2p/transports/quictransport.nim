when defined(libp2p_msquic_experimental):
  {.error: "OpenSSL QUIC transport is no longer available when enabling libp2p_msquic_experimental. Please remove -d:libp2p_quic_support and switch to MsQuic transport.".}

import std/[sequtils, strutils, base64, options, sets, tables, os]
import chronos
import chronicles
import metrics
import msquicwrapper as quic
import ./msquicdriver as msquicdrv
import ./webtransport_common
import "nim-msquic/api/event_model" as msevents
import "nim-msquic/api/api_impl" as msapi
import results
import ../multiaddress
import ../multicodec
import ../multihash
import ../multibase
import ../stream/connection
import ../wire
import ../muxers/muxer
import ../upgrademngrs/upgrade
import ./transport as baseTransport
import tls/certificate
import ../bandwidthmanager

export multiaddress
export multicodec
export connection
export transport

when defined(metrics):
  declareGauge libp2p_quic_active_driver,
    "Current QUIC transport driver", ["driver"]
  declareCounter libp2p_quic_driver_switch_total,
    "QUIC driver selection events", ["driver", "reason"]
  declareCounter libp2p_quic_msquic_failures_total,
    "MsQuic driver failure events", ["stage", "reason"]

logScope:
  topics = "libp2p quictransport"

template registerMsQuicSessionUnsafe(conn, alpns: untyped) =
  {.cast(gcsafe).}:
    quic.registerMsQuicSession(conn, alpns)

template unregisterMsQuicSessionUnsafe(conn: untyped) =
  {.cast(gcsafe).}:
    quic.unregisterMsQuicSession(conn)

template msquicSafe(body: untyped) =
  try:
    {.cast(gcsafe).}:
      body
  except Exception:
    discard

type
  P2PConnection = connection.Connection
  QuicConnection = quic.Connection
  QuicTransportError* = object of baseTransport.TransportError
  QuicTransportDialError* = object of baseTransport.TransportDialError
  QuicTransportAcceptStopped* = object of QuicTransportError

const
  alpn = "libp2p"
  DefaultWebtransportCerthashHistory* = 2
const
  webtransportCodec = multiCodec("webtransport")
  certhashCodec = multiCodec("certhash")
  p2pCodec = multiCodec("p2p")
  p2pCircuitCodec = multiCodec("p2p-circuit")
  defaultWebtransportPath = "/.well-known/libp2p-webtransport"
  defaultWebtransportQuery = "?type=noise"
  defaultWebtransportDraft = "draft02"
  MsQuicDriverEnvVar = "NIM_LIBP2P_QUIC_DRIVER"
  MsQuicDialEventTimeout = 250.milliseconds
  MsQuicDialMaxEvents = 12

type
  QuicDriverKind = enum
    qdkOpenSSL
    qdkMsQuic

  QuicDriverPreference = enum
    qdpAuto
    qdpMsQuicOnly
    qdpOpenSSLOnly

  StreamCursor = ref object
    stream: Stream
    buffer: seq[byte]
    eof: bool

  WebtransportRejectionReason* = enum
    wrrSessionLimit
    wrrMissingConnectProtocol
    wrrMissingDatagram
    wrrMissingSessionAccept
    wrrConnectRejected
    wrrInvalidRequest

  WebtransportRejectionStats* = object
    sessionLimit*: uint64
    missingConnectProtocol*: uint64
    missingDatagram*: uint64
    missingSessionAccept*: uint64
    connectRejected*: uint64
    invalidRequest*: uint64

proc driverLabel(kind: QuicDriverKind): string {.inline.} =
  case kind
  of qdkOpenSSL: "openssl"
  of qdkMsQuic: "msquic"

proc increment(
    stats: var WebtransportRejectionStats,
    reason: WebtransportRejectionReason,
    amount: uint64 = 1,
) {.inline.} =
  case reason
  of wrrSessionLimit:
    stats.sessionLimit += amount
  of wrrMissingConnectProtocol:
    stats.missingConnectProtocol += amount
  of wrrMissingDatagram:
    stats.missingDatagram += amount
  of wrrMissingSessionAccept:
    stats.missingSessionAccept += amount
  of wrrConnectRejected:
    stats.connectRejected += amount
  of wrrInvalidRequest:
    stats.invalidRequest += amount

proc count*(
    stats: WebtransportRejectionStats, reason: WebtransportRejectionReason
): uint64 {.inline.} =
  case reason
  of wrrSessionLimit:
    stats.sessionLimit
  of wrrMissingConnectProtocol:
    stats.missingConnectProtocol
  of wrrMissingDatagram:
    stats.missingDatagram
  of wrrMissingSessionAccept:
    stats.missingSessionAccept
  of wrrConnectRejected:
    stats.connectRejected
  of wrrInvalidRequest:
    stats.invalidRequest

proc webtransportRejectionReasonLabel*(
    reason: WebtransportRejectionReason
): string {.inline.} =
  case reason
  of wrrSessionLimit:
    "session_limit"
  of wrrMissingConnectProtocol:
    "missing_connect_protocol"
  of wrrMissingDatagram:
    "missing_datagram"
  of wrrMissingSessionAccept:
    "missing_session_accept"
  of wrrConnectRejected:
    "connect_status_rejected"
  of wrrInvalidRequest:
    "invalid_request"

proc classifyWebtransportHandshakeError(
    message: string
): WebtransportRejectionReason {.inline.} =
  if message.contains("session limit"):
    wrrSessionLimit
  elif message.contains("CONNECT protocol"):
    wrrMissingConnectProtocol
  elif message.contains("HTTP/3 datagrams"):
    wrrMissingDatagram
  elif message.contains("does not accept WebTransport sessions"):
    wrrMissingSessionAccept
  elif message.startsWith("webtransport CONNECT rejected"):
    wrrConnectRejected
  else:
    wrrInvalidRequest

proc initCursor(stream: Stream): StreamCursor =
  StreamCursor(stream: stream, buffer: @[], eof: false)

proc ensureBytes(
    cursor: StreamCursor, count: int
): Future[void] {.async: (raises: [CancelledError, QuicError, QuicTransportError]).} =
  if cursor.isNil:
    raise (ref QuicTransportError)(msg: "stream cursor is nil")
  while cursor.buffer.len < count and not cursor.eof:
    let chunk = await cursor.stream.read()
    if chunk.len == 0:
      cursor.eof = true
    else:
      cursor.buffer.add(chunk)
  if cursor.buffer.len < count:
    raise (ref QuicTransportError)(
      msg: "unexpected EOF while reading HTTP/3 payload"
    )

proc readBytes(
    cursor: StreamCursor, count: int
): Future[seq[byte]] {.async: (raises: [CancelledError, QuicError, QuicTransportError]).} =
  await cursor.ensureBytes(count)
  result = cursor.buffer[0 ..< count]
  cursor.buffer.delete(0, count - 1)

proc readByte(
    cursor: StreamCursor
): Future[byte] {.async: (raises: [CancelledError, QuicError, QuicTransportError]).} =
  let bytes = await cursor.readBytes(1)
  bytes[0]

proc encodeQuicVarInt(value: uint64): seq[byte] =
  if value < 0x40'u64:
    result = @[byte(value)]
  elif value < 0x4000'u64:
    result = @[
      byte(0x40 or ((value shr 8) and 0x3f)),
      byte(value and 0xff),
    ]
  elif value < 0x40000000'u64:
    result = @[
      byte(0x80 or ((value shr 24) and 0x3f)),
      byte((value shr 16) and 0xff),
      byte((value shr 8) and 0xff),
      byte(value and 0xff),
    ]
  elif value < 0x4000000000000000'u64:
    result = @[
      byte(0xc0 or ((value shr 56) and 0x3f)),
      byte((value shr 48) and 0xff),
      byte((value shr 40) and 0xff),
      byte((value shr 32) and 0xff),
      byte((value shr 24) and 0xff),
      byte((value shr 16) and 0xff),
      byte((value shr 8) and 0xff),
      byte(value and 0xff),
    ]
  else:
    raiseAssert "value out of range for QUIC varint"

proc decodeQuicVarIntFrom(
    data: openArray[byte], pos: var int
): uint64 {.raises: [QuicTransportError].} =
  if pos >= data.len:
    raise (ref QuicTransportError)(msg: "unexpected EOF decoding varint")
  let first = data[pos]
  inc pos
  let prefix = first shr 6
  case prefix
  of 0:
    uint64(first and 0x3f)
  of 1:
    if pos >= data.len:
      raise (ref QuicTransportError)(msg: "unexpected EOF decoding varint")
    let b1 = data[pos]
    inc pos
    (uint64(first and 0x3f) shl 8) or uint64(b1)
  of 2:
    if pos + 3 > data.len:
      raise (ref QuicTransportError)(msg: "unexpected EOF decoding varint")
    let b1 = data[pos]
    let b2 = data[pos + 1]
    let b3 = data[pos + 2]
    inc pos, 3
    (uint64(first and 0x3f) shl 24) or
      (uint64(b1) shl 16) or (uint64(b2) shl 8) or uint64(b3)
  else:
    if pos + 7 > data.len:
      raise (ref QuicTransportError)(msg: "unexpected EOF decoding varint")
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

proc readQuicVarInt(
    cursor: StreamCursor
): Future[uint64] {.async: (raises: [CancelledError, QuicError, QuicTransportError]).} =
  await cursor.ensureBytes(1)
  let first = cursor.buffer[0]
  let prefix = first shr 6
  let required =
    case prefix
    of 0: 1
    of 1: 2
    of 2: 4
    else: 8
  await cursor.ensureBytes(required)
  var pos = 0
  let value = decodeQuicVarIntFrom(cursor.buffer[0 ..< required], pos)
  cursor.buffer.delete(0, required - 1)
  value

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
        b = b or 0x80
        result.add(b)
      else:
        result.add(b)
        break

proc decodePrefixedInt(
    firstValue: byte, prefixBits: int, data: seq[byte], pos: var int
): uint64 {.raises: [QuicTransportError].} =
  let mask = (1 shl prefixBits) - 1
  var value = uint64(firstValue and byte(mask))
  if value < mask.uint64:
    return value
  var multiplier = 0
  var extra = 0'u64
  while true:
    if pos >= data.len:
      raise (ref QuicTransportError)(msg: "unexpected EOF decoding prefixed int")
    let b = data[pos]
    inc pos
    extra += uint64(b and 0x7f) shl multiplier
    if (b and 0x80) == 0:
      break
    multiplier += 7
  mask.uint64 + extra

proc encodeStringLiteralCustom(
    prefixBits: int, prefixBase: byte, value: string
): seq[byte] =
  let mask = (1 shl prefixBits) - 1
  let length = value.len.uint64
  var first = prefixBase
  if length < mask.uint64:
    first = first or byte(length)
    result.add(first)
  else:
    first = first or byte(mask)
    result.add(first)
    var extra = length - mask.uint64
    while true:
      var b = byte(extra and 0x7f)
      extra = extra shr 7
      if extra != 0:
        result.add(b or 0x80)
      else:
        result.add(b)
        break
  for ch in value:
    result.add(byte(ch))

proc encodeStringLiteral(prefixBits: int, value: string): seq[byte] =
  encodeStringLiteralCustom(prefixBits, 0, value)

proc decodeStringLiteral(
    data: seq[byte], pos: var int, prefixBits: int
): string {.raises: [QuicTransportError].} =
  if pos >= data.len:
    raise (ref QuicTransportError)(msg: "unexpected EOF decoding string literal")
  let first = data[pos]
  inc pos
  let huffman = ((first shr prefixBits) and 0x1) == 1
  if huffman:
    raise (ref QuicTransportError)(
      msg: "Huffman-encoded QPACK strings are not supported"
    )
  let mask = (1 shl prefixBits) - 1
  var length = uint64(first and mask.byte)
  if length == mask.uint64:
    var multiplier = 0
    while true:
      if pos >= data.len:
        raise (ref QuicTransportError)(
          msg: "unexpected EOF decoding string literal length"
        )
      let b = data[pos]
      inc pos
      length += uint64(b and 0x7f) shl multiplier
      if (b and 0x80) == 0:
        break
      multiplier += 7
  if pos + int(length) > data.len:
    raise (ref QuicTransportError)(
      msg: "unexpected EOF decoding string literal payload"
    )
  let endPos = pos + int(length)
  var s = newString(int(length))
  var idx = 0
  while pos < endPos:
    s[idx] = char(data[pos])
    inc idx
    inc pos
  s

proc qpackIndexedStatic(index: uint64): seq[byte] =
  encodePrefixedInt(0b11000000'u8, 6, index)

proc qpackLiteralWithNameRefStatic(index: uint64, value: string): seq[byte] =
  result = encodePrefixedInt(0b01010000'u8, 4, index)
  result.add(encodeStringLiteral(7, value))

proc qpackLiteralWithLiteralName(name, value: string): seq[byte] =
  let nameBytes = encodeStringLiteralCustom(3, 0b00100000'u8, name)
  let valueBytes = encodeStringLiteral(7, value)
  result = nameBytes
  result.add(valueBytes)

proc appendBytes(target: var seq[byte], data: seq[byte]) =
  for b in data:
    target.add(b)

proc encodeHeadersFrame(payload: seq[byte]): seq[byte] =
  result = encodeQuicVarInt(http3FrameTypeHeaders)
  result.appendBytes(encodeQuicVarInt(uint64(payload.len)))
  result.appendBytes(payload)

proc qpackStaticHeader(index: uint64): (string, string) {.raises: [QuicTransportError].} =
  case index
  of 0'u64:
    (":authority", "")
  of 1'u64:
    (":path", "/")
  of 15'u64:
    (":method", "CONNECT")
  of 17'u64:
    (":method", "GET")
  of 22'u64:
    (":scheme", "http")
  of 23'u64:
    (":scheme", "https")
  of 24'u64:
    (":status", "103")
  of 25'u64:
    (":status", "200")
  of 26'u64:
    (":status", "304")
  of 27'u64:
    (":status", "404")
  of 28'u64:
    (":status", "503")
  of 63'u64:
    (":status", "100")
  of 64'u64:
    (":status", "204")
  of 65'u64:
    (":status", "206")
  of 66'u64:
    (":status", "302")
  of 67'u64:
    (":status", "400")
  of 68'u64:
    (":status", "403")
  of 69'u64:
    (":status", "421")
  of 70'u64:
    (":status", "425")
  of 71'u64:
    (":status", "500")
  else:
    raise (ref QuicTransportError)(
      msg: "unsupported static table index: " & $index
    )

proc decodeHeadersBlock(
    payload: seq[byte]
): Table[string, string] {.raises: [QuicTransportError].} =
  var pos = 0
  if payload.len < 2:
    raise (ref QuicTransportError)(msg: "invalid QPACK field section")

  let requiredFirst = payload[pos]
  inc pos
  discard decodePrefixedInt(requiredFirst, 8, payload, pos)

  let baseFirst = payload[pos]
  inc pos
  discard decodePrefixedInt(baseFirst and 0x7f, 7, payload, pos)

  result = initTable[string, string]()
  while pos < payload.len:
    let first = payload[pos]
    inc pos
    if (first and 0x80) != 0:
      let isStatic = ((first shr 6) and 0x1) == 1
      let index = decodePrefixedInt(first and 0x3f, 6, payload, pos)
      if not isStatic:
        raise (ref QuicTransportError)(
          msg: "dynamic table references are not supported"
        )
      let entry = qpackStaticHeader(index)
      result[entry[0]] = entry[1]
    elif (first and 0xC0) == 0x40:
      let isStatic = ((first shr 4) and 0x1) == 1
      if not isStatic:
        raise (ref QuicTransportError)(
          msg: "dynamic table references are not supported"
        )
      let index = decodePrefixedInt(first and 0x0f, 4, payload, pos)
      let entry = qpackStaticHeader(index)
      let value = decodeStringLiteral(payload, pos, 7)
      result[entry[0]] = value
    elif (first and 0xE0) == 0x20:
      dec pos
      let name = decodeStringLiteral(payload, pos, 3)
      let value = decodeStringLiteral(payload, pos, 7)
      result[name] = value
    else:
      raise (ref QuicTransportError)(
        msg: "unsupported QPACK representation in headers block"
      )

proc makeClientHeadersBlock(info: WebtransportHandshakeInfo): seq[byte] =
  result.add(@[0x00'u8, 0x00'u8])
  result.appendBytes(qpackIndexedStatic(15)) # :method CONNECT
  result.appendBytes(qpackIndexedStatic(23)) # :scheme https
  result.appendBytes(qpackLiteralWithNameRefStatic(1, info.path))
  result.appendBytes(qpackLiteralWithNameRefStatic(0, info.authority))
  result.appendBytes(qpackLiteralWithLiteralName(":protocol", "webtransport"))
  if info.origin.len > 0:
    result.appendBytes(qpackLiteralWithLiteralName("origin", info.origin))
  result.appendBytes(
    qpackLiteralWithLiteralName("sec-webtransport-http3-draft", info.draft)
  )

proc makeServerHeadersBlock(draft: string): seq[byte] =
  result.add(@[0x00'u8, 0x00'u8])
  result.appendBytes(qpackIndexedStatic(25)) # :status 200
  if draft.len > 0:
    result.appendBytes(
      qpackLiteralWithLiteralName("sec-webtransport-http3-draft", draft)
    )

proc buildSettingsPayload(info: WebtransportHandshakeInfo): seq[byte] =
  result.appendBytes(encodeQuicVarInt(http3SettingsEnableConnectProtocol))
  result.appendBytes(encodeQuicVarInt(1))
  result.appendBytes(encodeQuicVarInt(http3SettingsH3Datagram))
  result.appendBytes(encodeQuicVarInt(1))
  result.appendBytes(
    encodeQuicVarInt(http3SettingsWebtransportMaxSessions)
  )
  result.appendBytes(encodeQuicVarInt(uint64(info.maxSessions)))

proc parseSettingsPayload(payload: seq[byte]): Http3Settings {.
    raises: [QuicTransportError]
.} =
  var pos = 0
  result.maxSessions = 0
  while pos < payload.len:
    let id = decodeQuicVarIntFrom(payload, pos)
    if pos > payload.len:
      raise (ref QuicTransportError)(
        msg: "malformed HTTP/3 settings payload"
      )
    let value = decodeQuicVarIntFrom(payload, pos)
    case id
    of http3SettingsEnableConnectProtocol:
      result.enableConnectProtocol = value != 0
    of http3SettingsH3Datagram:
      result.enableDatagram = value != 0
    of http3SettingsWebtransportMaxSessions:
      result.maxSessions = uint32(value and high(uint32))
    else:
      discard

proc readSettingsFrame(
    cursor: StreamCursor
): Future[Http3Settings] {.async: (raises: [CancelledError, QuicError, QuicTransportError]).} =
  if cursor.isNil:
    raise (ref QuicTransportError)(msg: "nil stream cursor while reading settings frame")
  while true:
    let frameType = await cursor.readQuicVarInt()
    let length = await cursor.readQuicVarInt()
    let payload = await cursor.readBytes(int(length))
    if frameType == http3FrameTypeSettings:
      return parseSettingsPayload(payload)

proc discardStream(stream: Stream) {.async: (raises: []).} =
  var cursor = initCursor(stream)
  try:
    while true:
      let chunk = await cursor.stream.read()
      if chunk.len == 0:
        break
  except CancelledError, QuicError:
    discard

type QuicUpgrade = ref object of Upgrade

type CertGenerator* =
  proc(kp: KeyPair): CertificateX509 {.gcsafe, raises: [TLSCertificateError].}

type QuicListenerState = object
  listener: Listener
  webtransport: bool

type QuicTransport* = ref object of Transport
  listeners: seq[QuicListenerState]
  acceptFuts: seq[Future[QuicConnection].Raising([QuicError, CancelledError])]
  client: Opt[QuicClient]
  privateKey: PrivateKey
  connections: seq[P2PConnection]
  rng: ref HmacDrbgContext
  certGenerator: CertGenerator
  certificate: CertificateX509
  certificateInitialized: bool
  certificateDer: seq[byte]
  webtransportCerthash: string
  webtransportCerthashHistory: seq[string]
  webtransportMaxCerthashHistory: int
  webtransportMaxSessions: uint32
  webtransportPath: string
  webtransportQuery: string
  webtransportDraft: string
  webtransportSessionLock: AsyncLock
  webtransportActiveSessions: uint32
  webtransportRejections: WebtransportRejectionStats
  driverPreference: QuicDriverPreference
  msquicHandle: msquicdrv.MsQuicTransportHandle
  msquicInitAttempted: bool
  msquicInitError: string
  msquicLastDialError: string
  msquicSuppressed: bool
  currentDriver: QuicDriverKind

proc detectDriverPreference(): QuicDriverPreference {.gcsafe.} =
  let raw = getEnv(MsQuicDriverEnvVar).strip()
  let defaultPref =
    when defined(libp2p_msquic_experimental):
      qdpMsQuicOnly
    else:
      qdpAuto
  if raw.len == 0:
    return defaultPref
  let lowered = raw.toLowerAscii()
  case lowered
  of "msquic", "ms-quic":
    qdpMsQuicOnly
  of "openssl", "open-ssl":
    qdpOpenSSLOnly
  of "auto":
    qdpAuto
  else:
    trace "Unknown QUIC driver preference, falling back to auto", value = raw
    defaultPref

proc recordDriverSwitch(kind: QuicDriverKind; reason: string) =
  when defined(metrics):
    libp2p_quic_driver_switch_total.inc(labelValues = [driverLabel(kind), reason])
    for other in QuicDriverKind:
      libp2p_quic_active_driver.set(
        (if other == kind: 1.0 else: 0.0),
        labelValues = [driverLabel(other)]
      )

proc recordMsQuicFailure(stage, reason: string) =
  when defined(metrics):
    libp2p_quic_msquic_failures_total.inc(labelValues = [stage, reason])

proc categorizeDialReason(reason: string): string =
  let lower = reason.toLowerAscii()
  if lower.len == 0:
    "unknown"
  elif lower.contains("timeout"):
    "timeout"
  elif lower.contains("shutdown"):
    "shutdown"
  elif lower.contains("missing runtime state"):
    "missing_state"
  elif lower.contains("established"):
    "connected"
  elif lower.contains("raised"):
    "exception"
  else:
    "other"

proc setDriver(self: QuicTransport; kind: QuicDriverKind; reason: string) {.gcsafe.} =
  self.currentDriver = kind
  recordDriverSwitch(kind, reason)

proc ensureMsQuicDriver(self: QuicTransport) {.raises: [].} =
  when defined(libp2p_msquic_experimental):
    if self.msquicInitAttempted:
      return
    self.msquicInitAttempted = true
    if self.driverPreference == qdpOpenSSLOnly:
      self.msquicInitError = "MsQuic disabled by preference"
      self.setDriver(qdkOpenSSL, "preference")
      return
    var handle: msquicdrv.MsQuicTransportHandle = nil
    var err = ""
    try:
      {.cast(gcsafe).}:
        let res = msquicdrv.initMsQuicTransport()
        handle = res.handle
        err = res.error
    except Exception as exc:
      self.msquicInitError = "MsQuic init raised: " & exc.msg
      trace "MsQuic runtime unavailable", error = self.msquicInitError
      recordMsQuicFailure("init", "exception")
      self.setDriver(qdkOpenSSL, "init_failed")
      return
    if handle.isNil:
      self.msquicInitError =
        if err.len > 0: err else: "failed to initialise MsQuic transport handle"
      trace "MsQuic runtime unavailable", error = self.msquicInitError
      let reason =
        if err.len > 0: "error"
        else: "unknown"
      recordMsQuicFailure("init", reason)
      self.setDriver(qdkOpenSSL, "init_failed")
    else:
      self.msquicHandle = handle
      self.msquicInitError = ""
      self.setDriver(qdkMsQuic, "init_success")
      trace "MsQuic runtime initialised"
  else:
    self.msquicInitError = "MsQuic runtime disabled at compile time"

proc msquicAvailable(self: QuicTransport): bool {.raises: [].} =
  when defined(libp2p_msquic_experimental):
    if not self.msquicInitAttempted:
      self.ensureMsQuicDriver()
    not self.msquicHandle.isNil
  else:
    false

proc preferMsQuic(self: QuicTransport): bool {.raises: [].} =
  when not defined(libp2p_msquic_experimental):
    return false
  try:
    case self.driverPreference
    of qdpOpenSSLOnly:
      self.setDriver(qdkOpenSSL, "preference")
      false
    of qdpMsQuicOnly:
      if self.msquicAvailable():
        self.setDriver(qdkMsQuic, "preference")
        true
      else:
        recordMsQuicFailure("dial", "msquic_unavailable")
        self.setDriver(qdkOpenSSL, "msquic_unavailable")
        false
    of qdpAuto:
      let available = self.msquicAvailable() and not self.msquicSuppressed
      if available:
        self.setDriver(qdkMsQuic, "auto")
      else:
        let reason =
          if self.msquicSuppressed: "suppressed"
          elif self.msquicAvailable(): "msquic_unavailable"
          else: "msquic_unavailable"
        self.setDriver(qdkOpenSSL, reason)
      available
  except CatchableError as exc:
    trace "MsQuic preference check failed", error = exc.msg
    self.setDriver(qdkOpenSSL, "preference_error")
    false

proc ensureCertificate(
    self: QuicTransport
): CertificateX509 {.raises: [QuicTransportError], gcsafe.}

proc computeWebtransportRequestTarget(transport: QuicTransport): string {.inline.} =
  if transport.webtransportQuery.len == 0:
    transport.webtransportPath
  else:
    transport.webtransportPath & transport.webtransportQuery

proc streamIsUnidirectional(stream: Stream): bool {.inline.} =
  when compiles(stream.isUnidirectional()):
    stream.isUnidirectional()
  else:
    false

proc connectionSendDatagram(conn: QuicConnection, data: seq[byte]): bool {.inline.} =
  when compiles(conn.sendDatagram(data)):
    conn.sendDatagram(data)
  else:
    discard conn
    discard data
    false

proc releaseLockSafe(lock: AsyncLock) {.inline.} =
  try:
    lock.release()
  except AsyncLockError:
    discard

proc streamIdentifier(stream: Stream): uint64 {.inline.} =
  when compiles(stream.id()):
    uint64(stream.id())
  else:
    0'u64

proc bytesToString(data: seq[byte]): string {.inline.} =
  result = newString(data.len)
  if data.len > 0:
    copyMem(addr result[0], unsafeAddr data[0], data.len)

when compiles(block:
      var probe: QuicConnection
      discard probe.incomingDatagram()):
  template connectionIncomingDatagram(conn: QuicConnection): untyped =
    conn.incomingDatagram()
else:
  proc connectionIncomingDatagram(_: QuicConnection): Future[seq[byte]] =
    result = newFuture[seq[byte]]()
    result.complete(@[])

# utility helpers for handshake metadata
proc extractHostPort(ma: MultiAddress): (string, string) =
  let text = ma.toString().valueOr:
    return ("", "")
  if text.len == 0:
    return ("", "")
  let parts = text.split('/')
  var host = ""
  var port = ""
  var idx = 0
  while idx < parts.len:
    let token = parts[idx]
    case token
    of "ip4", "ip6", "dns", "dns4", "dns6", "dnsaddr":
      if idx + 1 < parts.len:
        host = parts[idx + 1]
        inc idx
    of "udp", "tcp":
      if idx + 1 < parts.len:
        port = parts[idx + 1]
        inc idx
    else:
      discard
    inc idx
  (host, port)

proc makeClientHandshakeInfo(
    transport: QuicTransport, baseAddr: MultiAddress, hostname: string
): WebtransportHandshakeInfo =
  let (hostValue, portValue) = extractHostPort(baseAddr)
  let selectedHost = if hostname.len > 0: hostname else: hostValue
  var authorityHost = selectedHost
  if authorityHost.contains(':') and not authorityHost.startsWith("["):
    authorityHost = "[" & authorityHost & "]"
  let authority = normaliseAuthority(authorityHost, portValue)
  let requestTarget = computeWebtransportRequestTarget(transport)
  WebtransportHandshakeInfo(
    mode: wtmClient,
    authority: authority,
    path: requestTarget,
    origin: normaliseOrigin(authority),
    draft: transport.webtransportDraft,
    maxSessions: transport.webtransportMaxSessions,
  )

proc makeServerHandshakeInfo(transport: QuicTransport): WebtransportHandshakeInfo =
  let requestTarget = computeWebtransportRequestTarget(transport)
  WebtransportHandshakeInfo(
    mode: wtmServer,
    authority: "",
    path: requestTarget,
    origin: "",
    draft: transport.webtransportDraft,
    maxSessions: transport.webtransportMaxSessions,
  )

# Stream
type QuicStream* = ref object of P2PConnection
  stream: Stream
  cached: seq[byte]

proc new(
    _: type QuicStream,
    stream: Stream,
    oaddr: Opt[MultiAddress],
    laddr: Opt[MultiAddress],
    peerId: PeerId,
    initialCache: seq[byte] = @[],
): QuicStream =
  let quicstream =
    QuicStream(
      stream: stream,
      observedAddr: oaddr,
      localAddr: laddr,
      peerId: peerId,
      cached: initialCache,
    )
  procCall P2PConnection(quicstream).initStream()
  quicstream

proc buildLpStreamError(
    exc: ref quic.QuicError, direction: Direction, ctx: string
): ref LPStreamError =
  var infoParts: seq[string] = @[]
  if exc.hasStreamState:
    infoParts.add("quic_state=" & quic.streamStateToString(exc.streamState))
  if exc.hasStreamAppError:
    infoParts.add("app_error=" & $exc.streamAppError)
  if exc.streamId != 0:
    infoParts.add("stream=" & $exc.streamId)
  let details =
    if infoParts.len > 0:
      " (" & infoParts.join(" ") & ")"
    else:
      ""

  result =
    if exc.hasStreamState:
      let state = exc.streamState
      case state
      of quic.SSL_STREAM_STATE_RESET_REMOTE:
        let err = newLPStreamResetError()
        err.parent = exc
        err
      of quic.SSL_STREAM_STATE_RESET_LOCAL:
        let err = newLPStreamClosedError()
        err.parent = exc
        err
      of quic.SSL_STREAM_STATE_CONN_CLOSED:
        newLPStreamConnDownError(exc)
      of quic.SSL_STREAM_STATE_FINISHED:
        let err =
          if direction == Direction.In:
            newLPStreamRemoteClosedError()
          else:
            newLPStreamClosedError()
        err.parent = exc
        err
      of quic.SSL_STREAM_STATE_WRONG_DIR:
        (ref LPStreamError)(msg: ctx & ": stream direction mismatch" & details & " - " & exc.msg, parent: exc)
      else:
        (ref LPStreamError)(msg: ctx & ": " & exc.msg & details, parent: exc)
    else:
      (ref LPStreamError)(msg: ctx & ": " & exc.msg & details, parent: exc)

  # For errors created above without customized message, append detail info.
  if result.msg.len == 0:
    result.msg = ctx & ": " & exc.msg & details
  elif not result.msg.contains(exc.msg):
    result.msg &= details & " - " & exc.msg
  elif details.len > 0 and not result.msg.contains(details):
    result.msg &= details

  setQuicInfo(
    result,
    exc.hasStreamState,
    exc.streamState,
    exc.hasStreamAppError,
    exc.streamAppError,
    exc.streamId != 0,
    exc.streamId,
  )

method getWrapped*(self: QuicStream): P2PConnection =
  self

method readOnce*(
    stream: QuicStream, pbytes: pointer, nbytes: int
): Future[int] {.async: (raises: [CancelledError, LPStreamError]).} =
  if stream.cached.len == 0:
    try:
      stream.cached = await stream.stream.read()
      if stream.cached.len == 0:
        raise newLPStreamEOFError()
    except QuicError as exc:
      raise buildLpStreamError(exc, Direction.In, "error in readOnce")

  let toRead = min(nbytes, stream.cached.len)
  copyMem(pbytes, addr stream.cached[0], toRead)
  stream.cached = stream.cached[toRead ..^ 1]
  libp2p_network_bytes.inc(toRead.int64, labelValues = ["in"])
  if toRead > 0 and not stream.bandwidthManager.isNil and stream.peerId.len > 0:
    await stream.bandwidthManager.awaitLimit(
      stream.peerId, Direction.In, toRead, stream.protocol
    )
    stream.bandwidthManager.record(
      stream.peerId, Direction.In, toRead, stream.protocol
    )
  return toRead

{.push warning[LockLevel]: off.}
method write*(
    stream: QuicStream, bytes: seq[byte]
) {.async: (raises: [CancelledError, LPStreamError]).} =
  try:
    if not stream.bandwidthManager.isNil and stream.peerId.len > 0 and bytes.len > 0:
      await stream.bandwidthManager.awaitLimit(
        stream.peerId, Direction.Out, bytes.len, stream.protocol
      )
    await stream.stream.write(bytes)
    libp2p_network_bytes.inc(bytes.len.int64, labelValues = ["out"])
    if not stream.bandwidthManager.isNil and stream.peerId.len > 0 and bytes.len > 0:
      stream.bandwidthManager.record(
        stream.peerId, Direction.Out, bytes.len, stream.protocol
      )
  except QuicError as exc:
    raise buildLpStreamError(exc, Direction.Out, "error in quic stream write")

{.pop.}

method closeWrite*(stream: QuicStream) {.async: (raises: []).} =
  ## Close the write side of the QUIC stream
  try:
    await stream.stream.closeWrite()
  except CancelledError, QuicError:
    discard

method closeImpl*(stream: QuicStream) {.async: (raises: []).} =
  try:
    await stream.stream.close()
  except CancelledError, QuicError:
    discard
  await procCall P2PConnection(stream).closeImpl()

# Session
type QuicSession* = ref object of P2PConnection
  connection: QuicConnection
  streams: seq[QuicStream]
  isWebtransport: bool
  webtransportSessionId: uint64
  webtransportReady: bool
  webtransportRequestStream: Stream
  webtransportAuthority: string
  webtransportPath: string
  webtransportDraft: string
  webtransportHandshakeStart: Option[Moment]
  webtransportHandshakeReadyAt: Option[Moment]

method close*(session: QuicSession) {.async: (raises: [], gcsafe: false).} =
  for s in session.streams:
    await s.close()
  if session.isWebtransport and not session.webtransportRequestStream.isNil:
    try:
      await session.webtransportRequestStream.close()
    except CatchableError:
      discard
  unregisterMsQuicSessionUnsafe(session.connection)
  safeClose(session.connection)
  await procCall P2PConnection(session).close()

proc getStream*(
    session: QuicSession, direction = Direction.In
): Future[QuicStream] {.async: (raises: [QuicTransportError], gcsafe: false).} =
  var stream: Stream
  var initialCache: seq[byte] = @[]
  try:
    case direction
    of Direction.In:
      stream = await session.connection.incomingStream()
      if session.isWebtransport:
        if not session.webtransportReady:
          raise (ref QuicTransportError)(
            msg: "webtransport session not initialised"
          )
        var cursor = initCursor(stream)
        let sessionId = await cursor.readQuicVarInt()
        if sessionId != session.webtransportSessionId:
          asyncSpawn discardStream(stream)
          raise (ref QuicTransportError)(
            msg: "webtransport stream associated with unexpected session"
          )
        initialCache = cursor.buffer
    of Direction.Out:
      stream = await session.connection.openStream()
      if session.isWebtransport:
        if not session.webtransportReady:
          raise (ref QuicTransportError)(
            msg: "webtransport session not initialised"
          )
        await stream.write(encodeQuicVarInt(session.webtransportSessionId))
      else:
        await stream.write(@[]) # ensure stream is created
  except CancelledError as exc:
    raise (ref QuicTransportError)(msg: "cancelled getStream: " & exc.msg, parent: exc)
  except QuicError as exc:
    raise (ref QuicTransportError)(msg: "error in getStream: " & exc.msg, parent: exc)

  let qs =
    QuicStream.new(
      stream, session.observedAddr, session.localAddr, session.peerId, initialCache
    )
  when defined(libp2p_agents_metrics):
    qs.shortAgent = session.shortAgent

  session.streams.add(qs)
  return qs

proc sendDatagram*(session: QuicSession, data: seq[byte]): bool =
  if session.connection.isNil():
    return false
  connectionSendDatagram(session.connection, data)

proc recvDatagram*(
    session: QuicSession
): Future[seq[byte]] {.async.} =
  if session.connection.isNil():
    return @[]
  when compiles(connectionIncomingDatagram(session.connection)):
    await connectionIncomingDatagram(session.connection)
  else:
    return @[]

method getWrapped*(self: QuicSession): P2PConnection =
  self

proc performWebtransportHandshake(
    transport: QuicTransport,
    session: QuicSession,
    info: WebtransportHandshakeInfo
): Future[void] {.async: (raises: [CancelledError, QuicError, QuicTransportError]).} =
  session.isWebtransport = true
  session.webtransportAuthority = info.authority
  session.webtransportPath = info.path
  session.webtransportDraft = info.draft
  session.webtransportHandshakeStart = some(Moment.now())
  session.webtransportHandshakeReadyAt = none(Moment)

  let conn = session.connection

  try:
    let controlOut = await conn.openStream(unidirectional = true)
    await controlOut.write(encodeQuicVarInt(http3StreamTypeControl))
    let settingsPayload = buildSettingsPayload(info)
    var settingsFrame = encodeQuicVarInt(http3FrameTypeSettings)
    settingsFrame.appendBytes(encodeQuicVarInt(uint64(settingsPayload.len)))
    settingsFrame.appendBytes(settingsPayload)
    await controlOut.write(settingsFrame)

    var remoteSettings = Http3Settings()
    case info.mode
    of wtmClient:
      var settingsReceived = false
      while not settingsReceived:
        let incoming = await conn.incomingStream()
        if streamIsUnidirectional(incoming):
          var cursor = initCursor(incoming)
          let streamType = await cursor.readQuicVarInt()
          case streamType
          of http3StreamTypeControl:
            remoteSettings = await readSettingsFrame(cursor)
            settingsReceived = true
          else:
            asyncSpawn discardStream(incoming)
        else:
          asyncSpawn discardStream(incoming)

      let validation = settingsValidationError(remoteSettings)
      if validation.isSome:
        raise (ref QuicTransportError)(msg: validation.get())

      let requestStream = await conn.openStream()
      let headersBlock = makeClientHeadersBlock(info)
      let headersFrame = encodeHeadersFrame(headersBlock)
      await requestStream.write(headersFrame)
      await requestStream.closeWrite()

      var cursor = initCursor(requestStream)
      let respType = await cursor.readQuicVarInt()
      if respType != http3FrameTypeHeaders:
        raise (ref QuicTransportError)(msg: "unexpected frame on CONNECT stream")
      let respLen = await cursor.readQuicVarInt()
      let payload = await cursor.readBytes(int(respLen))
      let headers = decodeHeadersBlock(payload)
      if headers.getOrDefault(":status", "") != "200":
        raise (ref QuicTransportError)(
          msg: "webtransport CONNECT rejected with status " &
            headers.getOrDefault(":status", "unknown")
        )
      let draft = headers.getOrDefault("sec-webtransport-http3-draft", info.draft)
      session.webtransportDraft = draft
      session.webtransportSessionId = streamIdentifier(requestStream)
      session.webtransportRequestStream = requestStream
      session.webtransportReady = true
      session.webtransportHandshakeReadyAt = some(Moment.now())

    of wtmServer:
      var settingsReceived = false
      var requestStream: Stream
      while not (settingsReceived and not requestStream.isNil):
        let incoming = await conn.incomingStream()
        if streamIsUnidirectional(incoming):
          var cursor = initCursor(incoming)
          let streamType = await cursor.readQuicVarInt()
          case streamType
          of http3StreamTypeControl:
            remoteSettings = await readSettingsFrame(cursor)
            settingsReceived = true
          of http3StreamTypeQpackEncoder, http3StreamTypeQpackDecoder:
            asyncSpawn discardStream(incoming)
          else:
            asyncSpawn discardStream(incoming)
        else:
          if requestStream.isNil:
            requestStream = incoming
          else:
            asyncSpawn discardStream(incoming)

      let validation = settingsValidationError(remoteSettings)
      if validation.isSome:
        raise (ref QuicTransportError)(msg: validation.get())

      var cursor = initCursor(requestStream)
      let frameType = await cursor.readQuicVarInt()
      if frameType != http3FrameTypeHeaders:
        raise (ref QuicTransportError)(msg: "expected HEADERS frame on CONNECT")
      let frameLen = await cursor.readQuicVarInt()
      let payload = await cursor.readBytes(int(frameLen))
      let headers = decodeHeadersBlock(payload)
      if headers.getOrDefault(":method", "") != "CONNECT":
        raise (ref QuicTransportError)(msg: "CONNECT request missing")
      if headers.getOrDefault(":protocol", "") != "webtransport":
        raise (ref QuicTransportError)(
          msg: "CONNECT request does not target webtransport"
        )
      session.webtransportAuthority = headers.getOrDefault(
        ":authority", session.webtransportAuthority
      )
      session.webtransportPath = headers.getOrDefault(
        ":path", session.webtransportPath
      )
      let draft = headers.getOrDefault("sec-webtransport-http3-draft", info.draft)
      session.webtransportDraft = draft

      let responseBlock = makeServerHeadersBlock(draft)
      let responseFrame = encodeHeadersFrame(responseBlock)
      await requestStream.write(responseFrame)
      await requestStream.closeWrite()

      session.webtransportSessionId = streamIdentifier(requestStream)
      session.webtransportRequestStream = requestStream
      session.webtransportReady = true
      session.webtransportHandshakeReadyAt = some(Moment.now())
  except QuicTransportError as exc:
    if not transport.isNil:
      transport.webtransportRejections.increment(
        classifyWebtransportHandshakeError(exc.msg)
      )
    raise exc

# Muxer
type QuicMuxer = ref object of Muxer
  quicSession: QuicSession
  handleFut: Future[void]

when defined(libp2p_agents_metrics):
  method setShortAgent*(m: QuicMuxer, shortAgent: string) =
    m.quicSession.shortAgent = shortAgent
    for s in m.quicSession.streams:
      s.shortAgent = shortAgent
    m.connection.shortAgent = shortAgent

method newStream*(
    m: QuicMuxer, name: string = "", lazy: bool = false
): Future[P2PConnection] {.
    async: (raises: [CancelledError, LPStreamError, MuxerError])
.} =
  try:
    return await m.quicSession.getStream(Direction.Out)
  except QuicTransportError as exc:
    raise newException(MuxerError, "error in newStream: " & exc.msg, exc)

method handle*(m: QuicMuxer): Future[void] {.async: (raises: []).} =
  proc handleStream(chann: QuicStream) {.async: (raises: []).} =
    ## call the muxer stream handler for this channel
    ##
    await m.streamHandler(chann)
    trace "finished handling stream"
    doAssert(chann.closed, "connection not closed by handler!")

  try:
    while not m.quicSession.atEof:
      let stream = await m.quicSession.getStream(Direction.In)
      asyncSpawn handleStream(stream)
  except QuicTransportError as exc:
    trace "Exception in quic handler", msg = exc.msg

proc sendDatagram*(m: QuicMuxer, data: seq[byte]): bool =
  if m.quicSession.isNil():
    return false
  m.quicSession.sendDatagram(data)

proc recvDatagram*(
    m: QuicMuxer
): Future[seq[byte]] {.async.} =
  if m.quicSession.isNil():
    return @[]
  await m.quicSession.recvDatagram()

method close*(m: QuicMuxer) {.async: (raises: []).} =
  try:
    await m.quicSession.close()
    m.handleFut.cancelSoon()
  except CatchableError as exc:
    discard

# Transport
type
  WebtransportSessionSnapshot* = object
    ## Lightweight snapshot of an active WebTransport session exposed by the QUIC transport.
    peerId*: PeerId
    sessionId*: uint64
    ready*: bool
    authority*: string
    path*: string
    draft*: string
    observedAddr*: Opt[MultiAddress]
    localAddr*: Opt[MultiAddress]
    handshakeStart*: Option[Moment]
    handshakeReady*: Option[Moment]

proc recordWebtransportRejection(
    transport: QuicTransport, reason: WebtransportRejectionReason
) {.inline.} =
  if transport.isNil:
    return
  transport.webtransportRejections.increment(reason)

proc webtransportRejectionStats*(
    transport: QuicTransport
): WebtransportRejectionStats {.inline.} =
  if transport.isNil:
    return WebtransportRejectionStats()
  transport.webtransportRejections

proc getWebtransportHistoryLimit(self: QuicTransport): int =
  if self.webtransportMaxCerthashHistory < 1:
    DefaultWebtransportCerthashHistory
  else:
    self.webtransportMaxCerthashHistory

proc syncWebtransportCerthash(self: QuicTransport) =
  if self.webtransportCerthashHistory.len > 0:
    self.webtransportCerthash = self.webtransportCerthashHistory[0]
  else:
    self.webtransportCerthash = ""

proc enforceWebtransportHistoryLimit(self: QuicTransport) =
  let limit = self.getWebtransportHistoryLimit()
  if self.webtransportCerthashHistory.len > limit:
    self.webtransportCerthashHistory.setLen(limit)
  self.syncWebtransportCerthash()

proc addWebtransportCerthash(self: QuicTransport, hash: string) =
  if hash.len == 0:
    return
  if self.webtransportCerthashHistory.len == 0 or self.webtransportCerthashHistory[0] != hash:
    self.webtransportCerthashHistory.insert(hash, 0)
    var idx = 1
    while idx < self.webtransportCerthashHistory.len:
      if self.webtransportCerthashHistory[idx] == hash:
        self.webtransportCerthashHistory.delete(idx)
      else:
        inc idx
  self.enforceWebtransportHistoryLimit()

proc reserveWebtransportSlot(
    transport: QuicTransport
): Future[void] {.async: (raises: [CancelledError, QuicTransportError]).} =
  try:
    await transport.webtransportSessionLock.acquire()
  except AsyncLockError as exc:
    raise (ref QuicTransportError)(
      msg: "failed to acquire webtransport session lock", parent: exc
    )
  try:
    if transport.webtransportActiveSessions >= transport.webtransportMaxSessions:
      trace "webtransport session limit reached",
        active = transport.webtransportActiveSessions,
        limit = transport.webtransportMaxSessions
      transport.recordWebtransportRejection(wrrSessionLimit)
      raise (ref QuicTransportError)(
        msg: "webtransport session limit reached"
      )
    inc transport.webtransportActiveSessions
  finally:
    releaseLockSafe(transport.webtransportSessionLock)

proc releaseWebtransportSlot(transport: QuicTransport) {.async: (raises: []).} =
  try:
    await transport.webtransportSessionLock.acquire()
  except CancelledError:
    return
  except AsyncLockError as exc:
    trace "failed to acquire webtransport session lock for release", error = exc.msg
    return
  try:
    if transport.webtransportActiveSessions > 0'u32:
      dec transport.webtransportActiveSessions
  finally:
    releaseLockSafe(transport.webtransportSessionLock)

proc currentWebtransportCerthash*(
    self: QuicTransport
): string {.public, raises: [QuicTransportError], gcsafe.} =
  ## Return the currently active WebTransport certhash, generating the certificate
  ## on demand if it has not been initialised yet.
  discard self.ensureCertificate()
  self.webtransportCerthash

proc currentWebtransportCerthashHistory*(
    self: QuicTransport
): seq[string] {.public, raises: [QuicTransportError], gcsafe.} =
  ## Return a copy of the certhash history currently exposed to WebTransport
  ## clients, preserving insertion order from newest ➜ oldest.
  discard self.ensureCertificate()
  for hash in self.webtransportCerthashHistory:
    if hash.len == 0:
      continue
    result.add(hash)

proc isPemCertificate(data: seq[byte]): bool =
  const header = "-----BEGIN CERTIFICATE-----"
  if data.len < header.len:
    return false
  for idx, ch in header:
    if data[idx] != byte(ch):
      return false
  true

proc certificateToDer(cert: CertificateX509): Result[seq[byte], string] =
  if cert.certificate.len == 0:
    return err("empty certificate payload")
  if isPemCertificate(cert.certificate):
    let text = bytesToString(cert.certificate)
    var body = newStringOfCap(text.len)
    for line in text.splitLines():
      let trimmed = line.strip()
      if trimmed.len == 0 or trimmed.startsWith("-----"):
        continue
      body.add(trimmed)
    try:
      let decoded = base64.decode(body)
      var bytes = newSeq[byte](decoded.len)
      for i, ch in decoded:
        bytes[i] = byte(ch)
      ok(bytes)
    except CatchableError as exc:
      err("invalid PEM payload: " & exc.msg)
  else:
    ok(cert.certificate)

proc computeWebtransportCerthash(data: seq[byte]): Result[string, string] =
  if data.len == 0:
    return err("empty certificate data")
  let mh = MultiHash.digest("sha2-256", data).valueOr:
    return err($error)
  let encoded = MultiBase.encode("base64url", mh.data.buffer).valueOr:
    return err(error)
  ok(encoded)

proc ensureCertificate(
    self: QuicTransport
): CertificateX509 {.raises: [QuicTransportError], gcsafe.} =
  if not self.certificateInitialized:
    let pubkey = self.privateKey.getPublicKey().valueOr:
      raise (ref QuicTransportError)(
        msg: "failed to obtain public key for certificate: " & $error
      )
    let cert =
      try:
        self.certGenerator(KeyPair(seckey: self.privateKey, pubkey: pubkey))
      except TLSCertificateError as exc:
        raise (ref QuicTransportError)(
          msg: "failed to generate TLS certificate: " & exc.msg, parent: exc
        )

    self.certificate = cert
    self.certificateInitialized = true

    self.certificateDer = certificateToDer(cert).valueOr:
      trace "failed to normalise certificate to DER", error = error
      @[]

    if self.certificateDer.len > 0:
      let hashRes = computeWebtransportCerthash(self.certificateDer)
      if hashRes.isOk:
        self.webtransportCerthashHistory.setLen(0)
        self.addWebtransportCerthash(hashRes.get())
      else:
        self.webtransportCerthashHistory.setLen(0)
        self.syncWebtransportCerthash()
        trace "failed to compute webtransport certhash", error = hashRes.error
    else:
      self.webtransportCerthashHistory.setLen(0)
      self.syncWebtransportCerthash()
  self.certificate

proc splitTransportAddress(
    ma: MultiAddress
): MaResult[(MultiAddress, bool, seq[seq[byte]])] =
  ## Split a multiaddress into the transport prefix (up to `/quic-v1`) and whether
  ## it contains a trailing `/webtransport` marker. For WebTransport addresses the
  ## optional `/certhash` components are returned so the caller can validate the
  ## remote certificate against the advertised hashes.
  var prefixParts: seq[MultiAddress]
  let total = ?len(ma)
  var idx = 0
  var hasWebtransport = false
  var certHashes: seq[seq[byte]] = @[]

  while idx < total:
    let part = ?ma[idx]
    let code = ?part.protoCode()

    if hasWebtransport:
      if code == certhashCodec:
        certHashes.add(?part.protoArgument())
        inc idx
        continue
      if code == p2pCodec or code == p2pCircuitCodec:
        break
      break

    if code == webtransportCodec:
      hasWebtransport = true
      inc idx
      continue
    if code == certhashCodec or code == p2pCodec or code == p2pCircuitCodec:
      break
    prefixParts.add(part)
    inc idx

  if prefixParts.len == 0:
    return err("multiaddress: missing transport prefix")

  var base = prefixParts[0]
  for i in 1 ..< prefixParts.len:
    base = concat(base, prefixParts[i]).valueOr:
      return err("multiaddress: " & error)

  ok((base, hasWebtransport, certHashes))

proc makeCertificateVerifier(): CertificateVerifier =
  proc certificateVerifier(serverName: string, certificatesDer: seq[seq[byte]]): bool =
    if certificatesDer.len != 1:
      trace "CertificateVerifier: expected one certificate in the chain",
        cert_count = certificatesDer.len
      return false

    let cert =
      try:
        parse(certificatesDer[0])
      except CertificateParsingError as e:
        trace "CertificateVerifier: failed to parse certificate", msg = e.msg
        return false

    return cert.verify()

  return newCustomCertificateVerifier(certificateVerifier)

proc defaultCertGenerator(
    kp: KeyPair
): CertificateX509 {.gcsafe, raises: [TLSCertificateError].} =
  return generateX509(kp, encodingFormat = EncodingFormat.PEM)

proc new*(_: type QuicTransport, u: Upgrade, privateKey: PrivateKey): QuicTransport =
  let self = QuicTransport(
    listeners: @[],
    acceptFuts: @[],
    client: Opt.none(QuicClient),
    upgrader: QuicUpgrade(ms: u.ms),
    privateKey: privateKey,
    certGenerator: defaultCertGenerator,
    webtransportMaxCerthashHistory: DefaultWebtransportCerthashHistory,
    webtransportMaxSessions: 128'u32,
    webtransportPath: defaultWebtransportPath,
    webtransportQuery: defaultWebtransportQuery,
    webtransportDraft: defaultWebtransportDraft,
    webtransportSessionLock: newAsyncLock(),
    webtransportActiveSessions: 0'u32,
    driverPreference: detectDriverPreference(),
    msquicHandle: nil,
    msquicInitAttempted: false,
    msquicInitError: "",
    msquicLastDialError: "",
    msquicSuppressed: false,
    currentDriver: qdkOpenSSL,
  )
  procCall Transport(self).initialize()
  self.setDriver(qdkOpenSSL, "init")
  self

proc new*(
    _: type QuicTransport,
    u: Upgrade,
    privateKey: PrivateKey,
    certGenerator: CertGenerator,
): QuicTransport =
  let self = QuicTransport(
    listeners: @[],
    acceptFuts: @[],
    client: Opt.none(QuicClient),
    upgrader: QuicUpgrade(ms: u.ms),
    privateKey: privateKey,
    certGenerator: certGenerator,
    webtransportMaxCerthashHistory: DefaultWebtransportCerthashHistory,
    webtransportMaxSessions: 128'u32,
    webtransportPath: defaultWebtransportPath,
    webtransportQuery: defaultWebtransportQuery,
    webtransportDraft: defaultWebtransportDraft,
    webtransportSessionLock: newAsyncLock(),
    webtransportActiveSessions: 0'u32,
    driverPreference: detectDriverPreference(),
    msquicHandle: nil,
    msquicInitAttempted: false,
    msquicInitError: "",
    msquicLastDialError: "",
    msquicSuppressed: false,
    currentDriver: qdkOpenSSL,
  )
  procCall Transport(self).initialize()
  self.setDriver(qdkOpenSSL, "init")
  self

method handles*(transport: QuicTransport, address: MultiAddress): bool {.raises: [].} =
  if not procCall Transport(transport).handles(address):
    return false
  if QUIC_V1.match(address):
    return true
  WebTransport.match(address)

proc makeConfig(self: QuicTransport): TLSConfig =
  let cert = self.ensureCertificate()
  TLSConfig.init(
    cert.certificate, cert.privateKey, @[alpn], Opt.some(makeCertificateVerifier())
  )

proc getRng(self: QuicTransport): ref HmacDrbgContext =
  if self.rng.isNil:
    self.rng = newRng()

  return self.rng

proc setWebtransportCerthashHistoryLimit*(
    transport: QuicTransport, limit: int
) =
  let sanitized = if limit < 1: 1 else: limit
  transport.webtransportMaxCerthashHistory = sanitized
  transport.enforceWebtransportHistoryLimit()

proc loadWebtransportCerthashHistory*(
    transport: QuicTransport, history: openArray[string]
) =
  var seen = initHashSet[string]()
  var entries: seq[string] = @[]
  for raw in history:
    let trimmed = raw.strip()
    if trimmed.len == 0 or seen.contains(trimmed):
      continue
    seen.incl(trimmed)
    entries.add(trimmed)
  if entries.len == 0:
    return
  var combined: seq[string] = @[]
  let current = transport.webtransportCerthashHistory.len > 0
  if current:
    combined.add(transport.webtransportCerthashHistory[0])
  for hash in entries:
    if combined.anyIt(it == hash):
      continue
    combined.add(hash)
  transport.webtransportCerthashHistory = combined
  transport.enforceWebtransportHistoryLimit()

proc setWebtransportMaxSessions*(
    transport: QuicTransport, value: uint32
) {.public.} =
  transport.webtransportMaxSessions = if value == 0: 1'u32 else: value

proc setWebtransportPath*(
    transport: QuicTransport, path: string
) {.public.} =
  var cleaned = path.strip()
  let question = cleaned.find('?')
  var extractedQuery = ""
  if question >= 0:
    extractedQuery = cleaned[(question + 1) ..< cleaned.len]
    cleaned = cleaned[0 ..< question]
  if cleaned.len == 0:
    transport.webtransportPath = defaultWebtransportPath
  else:
    if not cleaned.startsWith("/"):
      cleaned = "/" & cleaned
    transport.webtransportPath = cleaned
  if question >= 0:
    if extractedQuery.len == 0:
      transport.webtransportQuery = ""
    else:
      transport.webtransportQuery = "?" & extractedQuery

proc setWebtransportQuery*(
    transport: QuicTransport, query: string
) {.public.} =
  var trimmed = query.strip()
  if trimmed.len == 0 or trimmed == "?":
    transport.webtransportQuery = ""
  else:
    if not trimmed.startsWith("?"):
      trimmed = "?" & trimmed
    transport.webtransportQuery = trimmed

proc setWebtransportDraft*(
    transport: QuicTransport, draft: string
) {.public.} =
  let trimmed = draft.strip()
  if trimmed.len == 0:
    transport.webtransportDraft = defaultWebtransportDraft
  else:
    transport.webtransportDraft = trimmed

proc webtransportPath*(transport: QuicTransport): string {.public.} =
  transport.webtransportPath

proc webtransportQuery*(transport: QuicTransport): string {.public.} =
  transport.webtransportQuery

proc webtransportDraft*(transport: QuicTransport): string {.public.} =
  transport.webtransportDraft

proc webtransportRequestTarget*(transport: QuicTransport): string {.public.} =
  computeWebtransportRequestTarget(transport)

proc currentWebtransportMaxSessions*(
    transport: QuicTransport
): uint32 {.public.} =
  ## Return the configured maximum number of concurrent WebTransport
  ## sessions for this QUIC transport.
  transport.webtransportMaxSessions

proc webtransportSessionSnapshots*(
    transport: QuicTransport
): seq[WebtransportSessionSnapshot] {.public.} =
  ## Return a copy of the active WebTransport session list for debugging and observability.
  ## Each snapshot captures the session identifier, negotiated target and address metadata.
  let conns = transport.connections.mapIt(it)
  for conn in conns:
    if conn.isNil or not (conn of QuicSession):
      continue
    let session = QuicSession(conn)
    if not session.isWebtransport:
      continue
    result.add(
      WebtransportSessionSnapshot(
        peerId: session.peerId,
        sessionId: session.webtransportSessionId,
        ready: session.webtransportReady,
        authority: session.webtransportAuthority,
        path: session.webtransportPath,
        draft: session.webtransportDraft,
        observedAddr: session.observedAddr,
        localAddr: session.localAddr,
        handshakeStart: session.webtransportHandshakeStart,
        handshakeReady: session.webtransportHandshakeReadyAt,
      )
    )

proc rotateCertificate*(
    transport: QuicTransport,
    keepHistory: int = DefaultWebtransportCerthashHistory,
): Future[void] {.
    async: (raises: [baseTransport.TransportError, LPError, CancelledError])
.} =
  let sanitized = if keepHistory < 1: 1 else: keepHistory
  let wasRunning = transport.running
  let listenAddrs = transport.addrs

  var previousHashes: seq[string] = @[]
  for hash in transport.webtransportCerthashHistory:
    previousHashes.add(hash)

  let previousCertificate = transport.certificate
  let previousCertificateDer = transport.certificateDer
  let previousInitialized = transport.certificateInitialized
  let previousCerthash = transport.webtransportCerthash
  let previousHistoryLimit = transport.webtransportMaxCerthashHistory

  if wasRunning:
    await transport.stop()

  transport.webtransportMaxCerthashHistory = sanitized
  transport.certificateInitialized = false
  transport.certificate = CertificateX509()
  transport.certificateDer = @[]
  transport.webtransportCerthashHistory.setLen(0)
  transport.syncWebtransportCerthash()

  try:
    discard transport.ensureCertificate()
    if previousHashes.len > 0:
      for idx in countdown(previousHashes.high, 0):
        transport.addWebtransportCerthash(previousHashes[idx])
    transport.enforceWebtransportHistoryLimit()
    if wasRunning:
      await transport.start(listenAddrs)
  except CatchableError as exc:
    transport.certificateInitialized = previousInitialized
    transport.certificate = previousCertificate
    transport.certificateDer = previousCertificateDer
    transport.webtransportCerthashHistory = previousHashes
    transport.webtransportCerthash = previousCerthash
    transport.webtransportMaxCerthashHistory = previousHistoryLimit
    if wasRunning:
      try:
        await transport.start(listenAddrs)
      except CatchableError as startExc:
        let msg =
          "failed to restore quic transport after rotation failure: " & startExc.msg
        raise (ref QuicTransportError)(msg: msg, parent: startExc)
    raise (ref QuicTransportError)(
      msg: "failed to rotate certificate: " & exc.msg, parent: exc
    )

method start*(
    self: QuicTransport, addrs: seq[MultiAddress]
) {.async: (raises: [LPError, baseTransport.TransportError, CancelledError]).} =
  doAssert self.listeners.len == 0, "start() already called"

  if addrs.len == 0:
    raise (ref QuicTransportError)(msg: "no listen addresses provided")

  var newListeners: seq[QuicListenerState] = @[]
  var advertisedAddrs: seq[MultiAddress] = @[]

  proc cleanupListeners() {.async: (raises: []).} =
    for state in newListeners:
      if not state.listener.isNil:
        try:
          await noCancel state.listener.stop()
        except CatchableError:
          discard
        state.listener.destroy()
    newListeners.setLen(0)

  try:
    for ma in addrs:
      let (baseAddr, hasWebtransport, _) = splitTransportAddress(ma).valueOr:
        raise (ref QuicTransportError)(
          msg: "invalid listen address: " & error
        )

      let transportAddr =
        try:
          initTAddress(baseAddr).tryGet
        except LPError as exc:
          raise (ref QuicTransportError)(
            msg: "transport error in quic start: " & exc.msg, parent: exc
          )

      let listener =
        QuicServer.init(self.makeConfig(), rng = self.getRng()).listen(transportAddr)
      let socketAddr = MultiAddress.init(listener.localAddress(), IPPROTO_UDP).valueOr:
        raise (ref QuicTransportError)(
          msg: "failed to convert local address: " & error
        )
      let quicSuffix = MultiAddress.init("/quic-v1").valueOr:
        raise (ref QuicTransportError)(msg: "failed to init quic suffix: " & error)

      var advertised = concat(socketAddr, quicSuffix).valueOr:
        raise (ref QuicTransportError)(
          msg: "failed to compose advertised address: " & error
        )

      if hasWebtransport:
        let wtSuffix = MultiAddress.init("/webtransport").valueOr:
          raise (ref QuicTransportError)(
            msg: "failed to init webtransport suffix: " & error
          )
        advertised = concat(advertised, wtSuffix).valueOr:
          raise (ref QuicTransportError)(
            msg: "failed to append webtransport suffix: " & error
          )
        self.enforceWebtransportHistoryLimit()
        for hash in self.webtransportCerthashHistory:
          if hash.len == 0:
            continue
          let chRes = MultiAddress.init("/certhash/" & hash)
          if chRes.isErr:
            trace "failed to init certhash suffix for advertised address",
              error = chRes.error
            continue
          let appended = concat(advertised, chRes.get())
          if appended.isOk:
            advertised = appended.get()
          else:
            trace "failed to append certhash suffix to advertised address",
              error = appended.error

      advertisedAddrs.add(advertised)
      newListeners.add(QuicListenerState(listener: listener, webtransport: hasWebtransport))
  except QuicConfigError as exc:
    await cleanupListeners()
    raiseAssert "invalid quic setup: " & $exc.msg
  except TLSCertificateError as exc:
    await cleanupListeners()
    raise (ref QuicTransportError)(
      msg: "tlscert error in quic start: " & exc.msg, parent: exc
    )
  except QuicError as exc:
    await cleanupListeners()
    raise
      (ref QuicTransportError)(msg: "quicerror in quic start: " & exc.msg, parent: exc)
  except TransportOsError as exc:
    await cleanupListeners()
    raise (ref QuicTransportError)(
      msg: "transport error in quic start: " & exc.msg, parent: exc
    )
  except QuicTransportError as exc:
    await cleanupListeners()
    raise exc

  try:
    await procCall Transport(self).start(advertisedAddrs)
  except CatchableError as exc:
    await cleanupListeners()
    raise (ref QuicTransportError)(
      msg: "failed to start quic transport: " & exc.msg, parent: exc
    )

  self.listeners = newListeners
  self.acceptFuts.setLen(0)
  for state in self.listeners:
    self.acceptFuts.add(state.listener.accept())
  self.addrs = advertisedAddrs

method stop*(transport: QuicTransport) {.async: (raises: []).} =
  let conns = transport.connections[0 .. ^1]
  for c in conns:
    await c.close()

  if transport.acceptFuts.len > 0:
    for fut in transport.acceptFuts:
      if not fut.isNil and not fut.completed():
        fut.cancel()
    transport.acceptFuts.setLen(0)

  for state in transport.listeners:
    if not state.listener.isNil:
      try:
        await state.listener.stop()
      except CatchableError as exc:
        trace "Error shutting down Quic transport", description = exc.msg
      state.listener.destroy()
  transport.listeners.setLen(0)

  transport.client = Opt.none(QuicClient)
  await procCall Transport(transport).stop()

proc wrapConnection(
    transport: QuicTransport,
    connection: QuicConnection,
    asWebtransport: bool,
    handshakeInfo: WebtransportHandshakeInfo
): Future[QuicSession] {.async: (raises: [TransportOsError, CancelledError, QuicError, QuicTransportError], gcsafe: false).} =
  var msquicCleanupNeeded = false
  defer:
    if msquicCleanupNeeded:
      unregisterMsQuicSessionUnsafe(connection)

  var observedAddr: MultiAddress
  var localAddr: MultiAddress
  try:
    observedAddr =
      MultiAddress.init(connection.remoteAddress(), IPPROTO_UDP).get() &
      MultiAddress.init("/quic-v1").get()
    localAddr =
      MultiAddress.init(connection.localAddress(), IPPROTO_UDP).get() &
      MultiAddress.init("/quic-v1").get()
    if asWebtransport:
      let wtSuffix = MultiAddress.init("/webtransport").get()
      observedAddr &= wtSuffix
      localAddr &= wtSuffix
      transport.enforceWebtransportHistoryLimit()
      for hash in transport.webtransportCerthashHistory:
        if hash.len == 0:
          continue
        let chRes = MultiAddress.init("/certhash/" & hash)
        if chRes.isOk:
          let chSuffix = chRes.get()
          observedAddr &= chSuffix
          localAddr &= chSuffix
        else:
          trace "failed to init certhash suffix for wrapped connection",
            error = chRes.error
  except MaError as e:
    raiseAssert "Multiaddr Error" & e.msg

  registerMsQuicSessionUnsafe(connection, @[alpn])
  msquicCleanupNeeded = true

  let session = QuicSession(
    connection: connection,
    streams: @[],
    observedAddr: Opt.some(observedAddr),
    localAddr: Opt.some(localAddr),
    webtransportHandshakeStart: none(Moment),
    webtransportHandshakeReadyAt: none(Moment),
  )
  session.initStream()

  transport.connections.add(session)
  msquicCleanupNeeded = false

  proc onClose() {.async: (raises: []).} =
    await noCancel session.join()
    if session.isWebtransport:
      await noCancel transport.releaseWebtransportSlot()
      session.isWebtransport = false
    transport.connections.keepItIf(it != session)
    trace "Cleaned up client"

  asyncSpawn onClose()

  if asWebtransport:
    var slotReserved = false
    try:
      await transport.reserveWebtransportSlot()
      slotReserved = true
      await performWebtransportHandshake(transport, session, handshakeInfo)
    except CancelledError as exc:
      if slotReserved:
        session.isWebtransport = false
        await noCancel transport.releaseWebtransportSlot()
      await noCancel session.close()
      raise exc
    except CatchableError as exc:
      if slotReserved:
        session.isWebtransport = false
        await noCancel transport.releaseWebtransportSlot()
      await noCancel session.close()
      raise (ref QuicTransportError)(
        msg: "webtransport handshake failed: " & exc.msg, parent: exc
      )

  session

method accept*(
    self: QuicTransport
): Future[connection.Connection] {.
    async: (raises: [baseTransport.TransportError, CancelledError], gcsafe: false)
} =
  if not self.running:
    raise newException(QuicTransportAcceptStopped, "Quic transport stopped")

  if self.listeners.len == 0:
    raise newException(QuicTransportAcceptStopped, "no listeners configured")

  proc cancelAcceptFuts() =
    for fut in self.acceptFuts:
      if not fut.isNil and not fut.completed():
        fut.cancel()

  if self.acceptFuts.len == 0:
    self.acceptFuts = self.listeners.mapIt(it.listener.accept())

  let finished =
    try:
      await one(self.acceptFuts)
    except ValueError:
      raiseAssert "accept futures should not be empty"
    except CancelledError as exc:
      cancelAcceptFuts()
      raise exc

  let index = self.acceptFuts.find(finished)
  doAssert index >= 0, "finished accept future not found"
  self.acceptFuts[index] = self.listeners[index].listener.accept()

  try:
    let connection = await finished
    let info = makeServerHandshakeInfo(self)
    return await self.wrapConnection(connection, self.listeners[index].webtransport, info)
  except QuicError as exc:
    debug "Quic Error", description = exc.msg
    try:
      stderr.writeLine("[quictransport.accept] QuicError: " & exc.msg)
    except IOError:
      discard
  except TransportOsError as exc:
    debug "OS Error", description = exc.msg
    try:
      stderr.writeLine("[quictransport.accept] OSError: " & exc.msg)
    except IOError:
      discard

proc tryDialMsQuic(
    self: QuicTransport,
    hostname: string,
    baseAddr: MultiAddress,
    useWebtransport: bool
): Future[Option[connection.Connection]] {.async: (raises: [CancelledError, CatchableError], gcsafe: false).} =
  if not self.msquicAvailable():
    return none(connection.Connection)

  let (host, portStr) = extractHostPort(baseAddr)
  if host.len == 0 or portStr.len == 0:
    self.msquicLastDialError = "MsQuic dial missing host or port information"
    return none(connection.Connection)

  var portValue: int
  try:
    portValue = parseInt(portStr)
  except ValueError:
    self.msquicLastDialError = "MsQuic dial invalid port number: " & portStr
    return none(connection.Connection)
  if portValue < 0 or portValue > high(uint16).int:
    self.msquicLastDialError = "MsQuic dial port out of range: " & portStr
    return none(connection.Connection)
  let port = uint16(portValue)
  let sni = if hostname.len > 0: hostname else: host

  when not defined(libp2p_msquic_experimental):
    self.msquicLastDialError = "MsQuic experimental dial disabled"
    return none(connection.Connection)
  else:
    let (connHandle, stateOpt, err) = block:
      var tmp: tuple[
        connection: msapi.HQUIC,
        state: Option[msquicdrv.MsQuicConnectionState],
        error: string
      ]
      try:
        {.cast(gcsafe).}:
          tmp = msquicdrv.dialConnection(
            self.msquicHandle,
            sni,
            port,
            queueLimit = self.msquicHandle.config.eventQueueLimit,
            pollInterval = MsQuicDialEventTimeout
          )
      except Exception as exc:
        tmp = (nil, none(msquicdrv.MsQuicConnectionState), "MsQuic dial raised: " & exc.msg)
      tmp
    var state: msquicdrv.MsQuicConnectionState = nil
    if stateOpt.isSome:
      state = stateOpt.get()
    if err.len > 0 or connHandle.isNil:
      self.msquicLastDialError =
        if err.len > 0: err else: "MsQuic dial returned nil connection"
      trace "MsQuic dial failed", reason = self.msquicLastDialError, host = host, port = port
      if not connHandle.isNil:
        msquicSafe:
          discard msquicdrv.shutdownConnection(self.msquicHandle, connHandle)
          msquicdrv.closeConnection(self.msquicHandle, connHandle, state)
      return none(connection.Connection)

    var connected = false
    var fallbackReason = ""
    when declared(msquicdrv.nextConnectionEvent):
      if state.isNil:
        fallbackReason = "MsQuic dial missing runtime state"
      else:
        var attempt = 0
        while attempt < MsQuicDialMaxEvents:
          inc attempt
          let eventFuture = state.nextConnectionEvent()
          let completed = await eventFuture.withTimeout(MsQuicDialEventTimeout)
          if not completed:
            eventFuture.cancel()
            fallbackReason = "timeout waiting for MsQuic connection event"
            break
          let event = await eventFuture
          case event.kind
          of msevents.ceConnected:
            connected = true
            fallbackReason = "MsQuic connection established (experimental fallback)"
            break
          of msevents.ceShutdownInitiated:
            fallbackReason = "MsQuic connection shutdown initiated"
            break
          of msevents.ceShutdownComplete:
            fallbackReason = "MsQuic connection shutdown complete"
            break
          else:
            discard
        if not connected and fallbackReason.len == 0:
          fallbackReason = "MsQuic dial no terminal event after " & $MsQuicDialMaxEvents & " iterations"
    else:
      fallbackReason = "MsQuic experimental driver does not expose event queue"

    msquicSafe:
      discard msquicdrv.shutdownConnection(self.msquicHandle, connHandle)
      msquicdrv.closeConnection(self.msquicHandle, connHandle, state)
    state = nil

    if fallbackReason.len == 0:
      fallbackReason = "MsQuic dial fallback without diagnostic"
    self.msquicLastDialError = fallbackReason

    if connected:
      info "MsQuic dial handshake succeeded, falling back to OpenSSL", host = host, port = port
    else:
      trace "MsQuic dial fallback", reason = fallbackReason, host = host, port = port, webtransport = useWebtransport

    return none(connection.Connection)

method dial*(
    self: QuicTransport,
    hostname: string,
    address: MultiAddress,
    peerId: Opt[PeerId] = Opt.none(PeerId),
): Future[connection.Connection] {.
    async: (raises: [baseTransport.TransportError, CancelledError], gcsafe: false)
.} =
  let (baseAddr, useWebtransport, certHashes) = splitTransportAddress(address).valueOr:
    raise newException(
      QuicTransportDialError, "error in quic dial: invalid address: " & error
    )

  let taAddress =
    try:
      initTAddress(baseAddr).tryGet
    except LPError as e:
      raise newException(
        QuicTransportDialError, "error in quic dial: invald address: " & e.msg, e
      )

  if self.driverPreference == qdpMsQuicOnly and not self.msquicAvailable():
    let reason =
      if self.msquicInitError.len > 0: self.msquicInitError else: "MsQuic runtime unavailable"
    raise newException(
      QuicTransportDialError, "MsQuic dial unavailable: " & reason
    )

  if self.preferMsQuic():
    var msquicFailed = false
    try:
      let msquicResult = await self.tryDialMsQuic(hostname, baseAddr, useWebtransport)
      if msquicResult.isSome:
        return msquicResult.get()
      msquicFailed = true
    except CatchableError as exc:
      self.msquicLastDialError = "MsQuic dial raised: " & exc.msg
      msquicFailed = true
    if msquicFailed:
      let dialReason = categorizeDialReason(self.msquicLastDialError)
      recordMsQuicFailure("dial", dialReason)
      if self.driverPreference == qdpMsQuicOnly:
        let reason =
          if self.msquicLastDialError.len > 0: self.msquicLastDialError
          else: "MsQuic dial fallback without diagnostic"
        self.setDriver(qdkOpenSSL, "fallback")
        raise newException(QuicTransportDialError, "MsQuic dial failed: " & reason)
      if self.driverPreference == qdpAuto:
        self.msquicSuppressed = true
        self.setDriver(qdkOpenSSL, "fallback")

  try:
    if not self.client.isSome:
      self.client = Opt.some(QuicClient.init(self.makeConfig(), rng = self.getRng()))

    let client = self.client.get()
    let quicConnection = await client.dial(taAddress)
    registerMsQuicSessionUnsafe(quicConnection, client.tls.alpn)
    var msquicCleanupNeeded = true
    defer:
      if msquicCleanupNeeded:
        unregisterMsQuicSessionUnsafe(quicConnection)

    if certHashes.len > 0:
      let certificates = quicConnection.certificates()
      if certificates.len == 0:
        safeClose(quicConnection)
        raise newException(
          QuicTransportDialError,
          "error in quic dial: missing remote certificate for certhash validation",
        )

      let leaf = certificates[0]
      var matched = false
      for payload in certHashes:
        let expected = MultiHash.init(payload).valueOr:
          safeClose(quicConnection)
          raise newException(
            QuicTransportDialError,
            "error in quic dial: invalid certhash payload: " & $error,
          )
        let codecName = $expected.mcodec
        let computed = MultiHash.digest(codecName, leaf).valueOr:
          safeClose(quicConnection)
          raise newException(
            QuicTransportDialError,
            "error in quic dial: certhash digest failure: " & $error,
          )
        if computed == expected:
          matched = true
          break

      if not matched:
        safeClose(quicConnection)
        raise newException(
          QuicTransportDialError, "error in quic dial: remote certificate certhash mismatch"
        )

    let handshakeInfo =
      if useWebtransport:
        makeClientHandshakeInfo(self, baseAddr, hostname)
      else:
        makeServerHandshakeInfo(self)
    let session = await self.wrapConnection(quicConnection, useWebtransport, handshakeInfo)
    msquicCleanupNeeded = false
    return session
  except QuicConfigError as e:
    raise newException(
      QuicTransportDialError, "error in quic dial: invalid tls config:" & e.msg, e
    )
  except TLSCertificateError as e:
    raise newException(
      QuicTransportDialError, "error in quic dial: tls certificate error:" & e.msg, e
    )
  except TransportOsError as e:
    raise newException(QuicTransportDialError, "error in quic dial:" & e.msg, e)
  except QuicError as e:
    raise newException(QuicTransportDialError, "error in quic dial:" & e.msg, e)

method upgrade*(
    self: QuicTransport, conn: P2PConnection, peerId: Opt[PeerId]
): Future[Muxer] {.async: (raises: [CancelledError, LPError]).} =
  let qs = QuicSession(conn)
  qs.peerId =
    if peerId.isSome:
      peerId.get()
    else:
      let certificates = qs.connection.certificates()
      let cert = parse(certificates[0])
      cert.peerId()

  let muxer = QuicMuxer(quicSession: qs, connection: conn)
  muxer.streamHandler = proc(conn: P2PConnection) {.async: (raises: []).} =
    trace "Starting stream handler"
    try:
      await self.upgrader.ms.handle(conn) # handle incoming connection
    except CancelledError as exc:
      return
    except CatchableError as exc:
      trace "exception in stream handler", conn, msg = exc.msg
    finally:
      await conn.closeWithEOF()
      trace "Stream handler done", conn
  muxer.handleFut = muxer.handle()
  return muxer
