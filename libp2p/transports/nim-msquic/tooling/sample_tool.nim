## Nim 版 sample 工具：重建 `src/tools/sample` 的握手演示与诊断输出。

import std/[monotimes, options, sequtils, strformat, strutils]

import "../core/mod"
import "../api/diagnostics_model"

type
  SampleRole* = enum
    srClient
    srServer

  SampleTraceEntry* = object
    stage*: string
    detail*: string

  SampleConfig* = object
    role*: SampleRole
    applicationName*: string
    peer*: string
    alpn*: string
    message*: string
    enableDatagram*: bool

  SampleResult* = object
    success*: bool
    negotiatedAlpn*: string
    datagramEnabled*: bool
    trace*: seq[SampleTraceEntry]
    diagnostics*: seq[string]
    handshakePacket*: Option[PreparedPacket]

  SampleSession = object
    config: SampleConfig
    clientCid: ConnectionId
    serverCid: ConnectionId
    client: QuicConnection
    server: QuicConnection
    trace: seq[SampleTraceEntry]
    diagnostics: seq[string]
    packet: Option[PreparedPacket]

proc diagKindName(kind: DiagnosticsEventKind): string =
  case kind
  of diagRegistrationOpened: "diagRegistrationOpened"
  of diagRegistrationShutdown: "diagRegistrationShutdown"
  of diagConfigurationLoaded: "diagConfigurationLoaded"
  of diagConnectionParamSet: "diagConnectionParamSet"
  of diagConnectionStarted: "diagConnectionStarted"
  of diagConnectionEvent: "diagConnectionEvent"

proc initSampleConfig*(role: SampleRole; peer: string = "localhost";
    message: string = "hello from nim sample"): SampleConfig =
  SampleConfig(
    role: role,
    applicationName: (if role == srClient: "nim-sample-client" else: "nim-sample-server"),
    peer: peer,
    alpn: "hq-interop",
    message: message,
    enableDatagram: false)

proc toBytes(text: string): seq[uint8] =
  result = newSeq[uint8](text.len)
  for idx, ch in text:
    result[idx] = uint8(ord(ch) and 0xFF)

proc logStage(session: var SampleSession; stage, detail: string) =
  session.trace.add SampleTraceEntry(stage: stage, detail: detail)

proc registerDiagnostics(session: var SampleSession) =
  clearDiagnosticsHooks()
  let captured = addr session
  registerDiagnosticsHook(proc (event: DiagnosticsEvent) {.gcsafe.} =
    if captured == nil:
      return
    var note: string
    if event.note.len == 0:
      note = diagKindName(event.kind)
    else:
      note = diagKindName(event.kind) & ": " & event.note
    captured[].diagnostics.add(note))

proc finalizeDiagnostics() =
  clearDiagnosticsHooks()

proc initSession(config: SampleConfig): SampleSession =
  let clientCid = initConnectionId(@[0xC1'u8, 0x01'u8, 0x02'u8, 0x03'u8, 0x04'u8, 0x05'u8])
  let serverCid = initConnectionId(@[0x5E'u8, 0x52'u8, 0x51'u8, 0x40'u8, 0x41'u8, 0x42'u8])
  let version = QuicVersion(1)
  SampleSession(
    config: config,
    clientCid: clientCid,
    serverCid: serverCid,
    client: newConnection(crClient, clientCid, serverCid, version),
    server: newConnection(crServer, serverCid, clientCid, version),
    trace: @[],
    diagnostics: @[],
    packet: none(PreparedPacket))

proc formatPacket(packet: PreparedPacket): string =
  case packet.header.form
  of pfLongHeader:
    fmt"long {packet.header.longHdr.packetType} pn={packet.header.longHdr.packetNumber} crypto={packet.payloadLength}B frames={packet.frames.len}"
  of pfShortHeader:
    fmt"short pn={packet.header.shortHdr.packetNumber} crypto={packet.payloadLength}B frames={packet.frames.len}"

proc formatPacket(handle: PreparedPacketHandle): string =
  case handle.header.form
  of pfLongHeader:
    fmt"long {handle.header.longHdr.packetType} pn={handle.header.longHdr.packetNumber} crypto={handle.payloadLength}B frames={handle.frameCount}"
  of pfShortHeader:
    fmt"short pn={handle.header.shortHdr.packetNumber} crypto={handle.payloadLength}B frames={handle.frameCount}"

proc clientHandshake(session: var SampleSession) =
  session.logStage("registration", fmt"app={session.config.applicationName} peer={session.config.peer}")
  emitDiagnostics(DiagnosticsEvent(
    kind: diagRegistrationOpened,
    handle: cast[pointer](session.client),
    paramId: 0,
    note: session.config.applicationName))

  if session.config.enableDatagram:
    session.client.model.stateFlags.incl(qcfReliableResetNegotiated)
    session.logStage("settings", "datagram path enabled (simulation)")

  let payload =
    if session.config.message.len == 0:
      @[0x01'u8, 0x00'u8, 0xFD'u8, 0xE0'u8]
    else:
      toBytes(session.config.message)

  session.client.queueInitialCrypto(payload)
  session.logStage("client.queueCrypto", fmt"bytes={payload.len}")

  let packetHandle = session.client.buildInitialPacket()
  session.packet = some(packetHandle.toSnapshot())
  session.logStage("client.initial", formatPacket(packetHandle))
  emitDiagnostics(DiagnosticsEvent(
    kind: diagConnectionStarted,
    handle: cast[pointer](session.client),
    paramId: 0,
    note: fmt"{session.config.peer}:443 alpn={session.config.alpn}"))

  session.client.onHandshakeAck(ceInitial)
  let succeeded = session.client.handshakeSucceeded()
  if succeeded:
    session.logStage("client.handshakeComplete", "Initial flight acknowledged")
    emitDiagnostics(DiagnosticsEvent(
      kind: diagConnectionEvent,
      handle: cast[pointer](session.client),
      paramId: 0,
      note: "handshake complete"))
  else:
    session.logStage("client.handshakeFailed", "handshake did not complete")

proc buildHandshakePacket(conn: QuicConnection; data: openArray[uint8]): PreparedPacketHandle =
  var chunk: CryptoChunk
  var sender = conn.handshakeSender
  sender.enqueueCryptoData(data)
  if not sender.emitChunk(1200, chunk):
    raise newException(ValueError, "No handshake crypto available")
  assert chunk.epoch == ceHandshake
  conn.handshakeSender = sender
  conn.model.stateFlags.incl(qcfHandshakeStarted)
  conn.model.setPhaseFromFlags()
  let start = getMonoTime()
  let packet = conn.packetPool.acquirePacket()
  let packetNumber = conn.model.packetSpaces[ceHandshake].nextPacketNumber()
  let header = conn.model.buildLongHeader(lhtHandshake, packetNumber)
  packet.setHeader(header)
  packet.setMetadata(chunk.epoch, packetNumber)
  discard packet.appendCryptoFrame(chunk.offset, chunk.data)
  let elapsed = getMonoTime() - start
  conn.packetPool.recordBuild(packet, elapsed)
  conn.sentPackets.add(packet)
  packet

proc serverHandshake(session: var SampleSession) =
  session.logStage("listener", fmt"app={session.config.applicationName} alpn={session.config.alpn}")
  emitDiagnostics(DiagnosticsEvent(
    kind: diagRegistrationOpened,
    handle: cast[pointer](session.server),
    paramId: 0,
    note: session.config.applicationName))

  let responsePayload =
    if session.config.message.len == 0:
      @[0x02'u8, 0x01'u8, 0xAA'u8, 0xBB'u8]
    else:
      toBytes(session.config.message)
  let packetHandle = session.server.buildHandshakePacket(responsePayload)
  session.packet = some(packetHandle.toSnapshot())
  session.logStage("server.handshakeFlight", formatPacket(packetHandle))
  emitDiagnostics(DiagnosticsEvent(
    kind: diagConnectionEvent,
    handle: cast[pointer](session.server),
    paramId: 0,
    note: "handshake flight sent"))

  session.server.onHandshakeAck(ceHandshake)
  if session.server.handshakeSucceeded():
    session.logStage("server.handshakeComplete", "Server flight acknowledged")
    emitDiagnostics(DiagnosticsEvent(
      kind: diagConnectionEvent,
      handle: cast[pointer](session.server),
      paramId: 0,
      note: "server handshake complete"))
  else:
    session.logStage("server.handshakeFailed", "handshake did not complete")

proc buildResult(session: SampleSession): SampleResult =
  SampleResult(
    success: (session.config.role == srClient and session.client.handshakeSucceeded()) or
             (session.config.role == srServer and session.server.handshakeSucceeded()),
    negotiatedAlpn: session.config.alpn,
    datagramEnabled: session.config.enableDatagram,
    trace: session.trace,
    diagnostics: session.diagnostics,
    handshakePacket: session.packet)

proc runSample*(config: SampleConfig): SampleResult =
  var session = initSession(config)
  registerDiagnostics(session)
  case config.role
  of srClient:
    session.clientHandshake()
  of srServer:
    session.serverHandshake()
  finalizeDiagnostics()
  buildResult(session)
