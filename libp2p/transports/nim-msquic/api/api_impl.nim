## MsQuic 最小 API 表实现，覆盖 `MsQuicOpenVersion` 所需的核心句柄/回调。

import std/tables
import std/sequtils
import std/endians
import std/times
import chronos
import chronicles

from "../core/mod" import ConnectionId, QuicConnection, QuicVersion,
    initConnectionId, newConnection, ConnectionRole, crClient, crServer,
    CryptoEpoch, ceInitial, ceOneRtt, nextPacketNumber, recordAckedPacket,
    initiatePathChallenge, completePathValidation, PathState
from "../congestion/common" import CongestionAlgorithm, caCubic, caBbr,
    AckEventSnapshot, LossEventSnapshot, ackTypeAckImmediate,
    MaxAckDelayDefaultMs, PersistentCongestionThreshold
import "../congestion/ack_tracker_model" as qack
import "../congestion/loss_detection_model" as qloss
import "../congestion/controller" as qcc
from "./common" import QuicHandleKind, qhkRegistration, qhkConfiguration,
    qhkConnection, qhkStream
import ./event_model
import ./diagnostics_model
import ./settings_model
import ./param_catalog
import ../protocol/protocol_core except ConnectionId
from ../protocol/protocol_core as proto import nil
import ../protocol/tls_core as tls
import libp2p/crypto/curve25519

when compiles((proc () {.noGc.} = discard)):
  {.pragma: quicApiHot, inline, noGc.}
else:
  {.pragma: quicApiHot, inline.}

type
  PendingStreamChunk = object
    offset: uint64
    payload: seq[byte]
    fin: bool
    clientContext: pointer

  PendingDatagram = object
    payload: seq[byte]
    clientContext: pointer

  SentFrameKind = enum
    sfkStream
    sfkDatagram
    sfkAck
    sfkCrypto
    sfkPathChallenge
    sfkPathResponse

  SentPacketMeta = object
    packetNumber: uint64
    epoch: CryptoEpoch
    ackEliciting: bool
    packetLength: uint16
    sentTimeUs: uint64
    frameKind: SentFrameKind
    stream: pointer
    streamOffset: uint64
    streamPayload: seq[byte]
    streamFin: bool
    clientContext: pointer

  BuiltinResumptionEntry = object
    ticket: seq[byte]
    data: seq[byte]
    issuedAtUs: uint64
    expiresAtUs: uint64

  QUIC_STATUS* = uint32
  BOOLEAN* = uint8
  QUIC_EXECUTION_PROFILE* = uint32
  QUIC_CONNECTION_SHUTDOWN_FLAGS* = uint32
  QUIC_STREAM_OPEN_FLAGS* = uint32
  QUIC_STREAM_START_FLAGS* = uint32
  QUIC_STREAM_SHUTDOWN_FLAGS* = uint32
  QUIC_SEND_FLAGS* = uint32
  QUIC_SEND_RESUMPTION_FLAGS* = uint32
  QUIC_ADDRESS_FAMILY* = uint16
  QUIC_UINT62* = uint64
  QUIC_TLS_ALERT_CODES* = uint16
  HQUIC* = pointer

  QuicBuffer* {.bycopy.} = object
    Length*: uint32
    Buffer*: ptr uint8

  QuicRegistrationConfigC* {.bycopy.} = object
    AppName*: cstring
    ExecutionProfile*: QUIC_EXECUTION_PROFILE

  QuicConnectionCallback* = proc(connection: HQUIC; context: pointer;
      event: pointer): QUIC_STATUS {.cdecl.}
  QuicStreamCallback* = proc(stream: HQUIC; context: pointer;
      event: pointer): QUIC_STATUS {.cdecl.}
  QuicListenerCallback* = proc(listener: HQUIC; context: pointer;
      event: pointer): QUIC_STATUS {.cdecl.}

  QuicSetContextFn* = proc(handle: HQUIC; context: pointer) {.cdecl, quicApiHot.}
  QuicGetContextFn* = proc(handle: HQUIC): pointer {.cdecl, quicApiHot.}
  QuicSetCallbackHandlerFn* = proc(handle: HQUIC; handler: pointer;
      context: pointer) {.cdecl, quicApiHot.}
  QuicSetParamFn* = proc(handle: HQUIC; param: uint32; bufferLength: uint32;
      buffer: pointer): QUIC_STATUS {.cdecl.}
  QuicGetParamFn* = proc(handle: HQUIC; param: uint32; bufferLength: ptr uint32;
      buffer: pointer): QUIC_STATUS {.cdecl.}
  QuicRegistrationOpenFn* = proc(config: ptr QuicRegistrationConfigC;
      registration: ptr HQUIC): QUIC_STATUS {.cdecl.}
  QuicRegistrationCloseFn* = proc(registration: HQUIC) {.cdecl.}
  QuicRegistrationShutdownFn* = proc(registration: HQUIC;
      flags: QUIC_CONNECTION_SHUTDOWN_FLAGS; errorCode: QUIC_UINT62) {.cdecl.}
  QuicConfigurationOpenFn* = proc(registration: HQUIC;
      alpnBuffers: ptr QuicBuffer; alpnBufferCount: uint32; settings: pointer;
      settingsSize: uint32; context: pointer; configuration: ptr HQUIC): QUIC_STATUS {.cdecl.}
  QuicConfigurationCloseFn* = proc(configuration: HQUIC) {.cdecl.}
  QuicConfigurationLoadCredentialFn* = proc(configuration: HQUIC;
      credential: pointer): QUIC_STATUS {.cdecl.}
  QuicListenerOpenFn* = proc(registration: HQUIC; handler: pointer;
      context: pointer; listener: ptr HQUIC): QUIC_STATUS {.cdecl.}
  QuicListenerCloseFn* = proc(listener: HQUIC) {.cdecl.}
  QuicListenerStartFn* = proc(listener: HQUIC; alpn: ptr QuicBuffer;
      alpnCount: uint32; address: pointer): QUIC_STATUS {.cdecl.}
  QuicListenerStopFn* = proc(listener: HQUIC) {.cdecl.}
  QuicConnectionOpenFn* = proc(registration: HQUIC; handler: QuicConnectionCallback;
      context: pointer; connection: ptr HQUIC): QUIC_STATUS {.cdecl.}
  QuicConnectionCloseFn* = proc(connection: HQUIC) {.cdecl.}
  QuicConnectionShutdownFn* = proc(connection: HQUIC;
      flags: QUIC_CONNECTION_SHUTDOWN_FLAGS; errorCode: QUIC_UINT62) {.cdecl.}
  QuicConnectionStartFn* = proc(connection: HQUIC; configuration: HQUIC;
      family: QUIC_ADDRESS_FAMILY; serverName: cstring;
      serverPort: uint16): QUIC_STATUS {.cdecl.}
  QuicConnectionSetConfigurationFn* = proc(connection: HQUIC;
      configuration: HQUIC): QUIC_STATUS {.cdecl.}
  QuicConnectionSendResumptionFn* = proc(connection: HQUIC;
      flags: QUIC_SEND_RESUMPTION_FLAGS; dataLength: uint16;
      data: ptr uint8): QUIC_STATUS {.cdecl.}
  QuicStreamOpenFn* = proc(connection: HQUIC; flags: QUIC_STREAM_OPEN_FLAGS;
      handler: QuicStreamCallback; context: pointer; stream: ptr HQUIC): QUIC_STATUS {.cdecl.}
  QuicStreamCloseFn* = proc(stream: HQUIC) {.cdecl.}
  QuicStreamStartFn* = proc(stream: HQUIC;
      flags: QUIC_STREAM_START_FLAGS): QUIC_STATUS {.cdecl.}
  QuicStreamShutdownFn* = proc(stream: HQUIC;
      flags: QUIC_STREAM_SHUTDOWN_FLAGS; errorCode: QUIC_UINT62): QUIC_STATUS {.cdecl.}
  QuicStreamSendFn* = proc(stream: HQUIC; buffers: ptr QuicBuffer;
      bufferCount: uint32; flags: QUIC_SEND_FLAGS;
      clientContext: pointer): QUIC_STATUS {.cdecl.}
  QuicStreamReceiveCompleteFn* = proc(stream: HQUIC; bufferLength: uint64) {.cdecl.}
  QuicStreamReceiveSetEnabledFn* = proc(stream: HQUIC;
      enabled: BOOLEAN): QUIC_STATUS {.cdecl.}
  QuicDatagramSendFn* = proc(connection: HQUIC; buffers: ptr QuicBuffer;
      bufferCount: uint32; flags: QUIC_SEND_FLAGS;
      clientContext: pointer): QUIC_STATUS {.cdecl.}
  QuicConnectionResumptionCompleteFn* = proc(connection: HQUIC;
      result: BOOLEAN): QUIC_STATUS {.cdecl.}
  QuicConnectionCertificateCompleteFn* = proc(connection: HQUIC;
      result: BOOLEAN; tlsAlert: QUIC_TLS_ALERT_CODES): QUIC_STATUS {.cdecl.}
  QuicConnectionOpenInPartitionFn* = proc(registration: HQUIC; partition: uint16;
      handler: QuicConnectionCallback; context: pointer;
      connection: ptr HQUIC): QUIC_STATUS {.cdecl.}
  QuicStreamProvideReceiveBuffersFn* = proc(stream: HQUIC; bufferCount: uint32;
      buffers: ptr QuicBuffer): QUIC_STATUS {.cdecl.}

  QuicApiTable* {.bycopy.} = object
    SetContext*: QuicSetContextFn
    GetContext*: QuicGetContextFn
    SetCallbackHandler*: QuicSetCallbackHandlerFn
    SetParam*: QuicSetParamFn
    GetParam*: QuicGetParamFn
    RegistrationOpen*: QuicRegistrationOpenFn
    RegistrationClose*: QuicRegistrationCloseFn
    RegistrationShutdown*: QuicRegistrationShutdownFn
    ConfigurationOpen*: QuicConfigurationOpenFn
    ConfigurationClose*: QuicConfigurationCloseFn
    ConfigurationLoadCredential*: QuicConfigurationLoadCredentialFn
    ListenerOpen*: QuicListenerOpenFn
    ListenerClose*: QuicListenerCloseFn
    ListenerStart*: QuicListenerStartFn
    ListenerStop*: QuicListenerStopFn
    ConnectionOpen*: QuicConnectionOpenFn
    ConnectionClose*: QuicConnectionCloseFn
    ConnectionShutdown*: QuicConnectionShutdownFn
    ConnectionStart*: QuicConnectionStartFn
    ConnectionSetConfiguration*: QuicConnectionSetConfigurationFn
    ConnectionSendResumptionTicket*: QuicConnectionSendResumptionFn
    StreamOpen*: QuicStreamOpenFn
    StreamClose*: QuicStreamCloseFn
    StreamStart*: QuicStreamStartFn
    StreamShutdown*: QuicStreamShutdownFn
    StreamSend*: QuicStreamSendFn
    StreamReceiveComplete*: QuicStreamReceiveCompleteFn
    StreamReceiveSetEnabled*: QuicStreamReceiveSetEnabledFn
    DatagramSend*: QuicDatagramSendFn
    ConnectionResumptionTicketValidationComplete*: QuicConnectionResumptionCompleteFn
    ConnectionCertificateValidationComplete*: QuicConnectionCertificateCompleteFn
    ConnectionOpenInPartition*: QuicConnectionOpenInPartitionFn
    StreamProvideReceiveBuffers*: QuicStreamProvideReceiveBuffersFn

const
  QUIC_STATUS_SUCCESS* = QUIC_STATUS(0)
  QUIC_STATUS_OUT_OF_MEMORY* = QUIC_STATUS(12)          ## 对应 `ENOMEM`
  QUIC_STATUS_INVALID_PARAMETER* = QUIC_STATUS(22)      ## 对应 `EINVAL`
  QUIC_STATUS_NOT_SUPPORTED* = QUIC_STATUS(95)          ## 对应 `EOPNOTSUPP`
  QUIC_STATUS_INVALID_STATE* = QUIC_STATUS(200)         ## 近似 `ENOTRECOVERABLE`
  QUIC_STATUS_INTERNAL_ERROR* = QUIC_STATUS(201)        ## Generic internal error
  QUIC_TLS_ALERT_CODE_SUCCESS* = QUIC_TLS_ALERT_CODES(0xFFFF)
  QUIC_CONNECTION_EVENT_CONNECTED* = 0'u32
  QUIC_CONNECTION_EVENT_PEER_STREAM_STARTED* = 6'u32

type
  QuicConnectionEventConnectedPayload* {.bycopy.} = object
    SessionResumed*: BOOLEAN
    NegotiatedAlpnLength*: uint8
    Reserved*: uint16
    NegotiatedAlpn*: ptr uint8

  QuicConnectionEvent* {.bycopy.} = object
    Type*: uint32
    Padding*: uint32
    Data*: array[48, uint8]

  QuicStreamEvent* {.bycopy.} = object
    Type*: uint32
    Padding*: uint32
    Data*: array[96, uint8]

  QuicStreamEventStartCompletePayload* {.bycopy.} = object
    Status*: QUIC_STATUS
    Id*: QUIC_UINT62
    Flags*: uint8
    Reserved*: array[7, uint8]

  QuicStreamEventSendCompletePayload* {.bycopy.} = object
    Canceled*: BOOLEAN
    Reserved*: array[7, uint8]
    ClientContext*: pointer

  QuicStreamEventPeerAbortedPayload* {.bycopy.} = object
    ErrorCode*: QUIC_UINT62
    Reserved*: uint16

  QuicStreamEventSendShutdownCompletePayload* {.bycopy.} = object
    Graceful*: BOOLEAN
    Reserved*: array[7, uint8]

  QuicStreamEventShutdownCompletePayload* {.bycopy.} = object
    ConnectionShutdown*: BOOLEAN
    Flags*: uint8
    Reserved*: array[6, uint8]
    ConnectionErrorCode*: QUIC_UINT62
    ConnectionCloseStatus*: QUIC_STATUS

  QuicListenerEvent* {.bycopy.} = object
    Type*: uint32
    Padding*: uint32
    Data*: array[32, uint8]

  QuicListenerEventNewConnectionPayload* {.bycopy.} = object
    Info*: pointer
    Connection*: HQUIC

  QuicConnectionEventPeerStreamStartedPayload* {.bycopy.} = object
    Stream*: HQUIC
    Flags*: QUIC_STREAM_OPEN_FLAGS

  QuicStreamEventReceivePayload* {.bycopy.} = object
    AbsoluteOffset*: uint64
    TotalBufferLength*: uint64
    Buffers*: pointer
    BufferCount*: uint32
    Flags*: uint32

  QuicListenerEventStopCompletePayload* {.bycopy.} = object
    Flags*: uint8
    Reserved*: array[7, uint8]

  QuicListenerEventDosModePayload* {.bycopy.} = object
    Flags*: uint8
    Reserved*: array[7, uint8]

  GlobalExecutionConfigHeader* {.bycopy.} = object
    Flags*: uint32
    PollingIdleTimeoutUs*: uint32
    ProcessorCount*: uint32

  RegistrationProfile = enum
    rpLowLatency
    rpMaxThroughput
    rpScavenger
    rpRealTime

  QuicHandleState = ref object of RootObj
    kind: QuicHandleKind
    context: pointer

  RegistrationState = ref object of QuicHandleState
    appName: string
    profile: RegistrationProfile
    shuttingDown: bool

  ConfigurationState = ref object of QuicHandleState
    registration: RegistrationState
    alpns: seq[string]
    credentialLoaded: bool

  StreamState = ref object of QuicHandleState
    connection: ConnectionState # Reference to parent connection
    callback: QuicStreamCallback
    callbackContext: pointer
    eventHandlers: seq[StreamEventHandler]
    started: bool
    closed: bool
    sendBuffer: seq[byte]
    sentOffset: uint64
    finRequested: bool
    finSent: bool
    streamId: uint64
    pendingChunks: seq[PendingStreamChunk]

  ListenerState = ref object of QuicHandleState
    registration: RegistrationState
    callback: QuicListenerCallback
    callbackContext: pointer
    started: bool
    stopped: bool
    transport: DatagramTransport
    acceptedConnections: Table[string, ConnectionState]

  ConnectionState = ref object of QuicHandleState
    registration: RegistrationState
    configuration: ConfigurationState
    callback: QuicConnectionCallback
    callbackContext: pointer
    started: bool
    serverName: string
    serverPort: uint16
    localCid: ConnectionId
    peerCid: ConnectionId
    quicConn: QuicConnection
    eventHandlers: seq[ConnectionEventHandler]
    datagramReceiveEnabled: bool
    datagramSendEnabled: bool
    streamSchedulingScheme: uint32
    settingsOverlay: QuicSettingsOverlay
    congestionAlgorithm: CongestionAlgorithm
    closeReason: string
    disable1RttEncryption: bool
    transport: DatagramTransport
    remoteAddress: TransportAddress

    initialSecrets: InitialSecrets
    oneRttKeys: TrafficSecrets # Placeholder for Short Header keys
    clientPrivateKey: Curve25519Key
    transcript: seq[byte]
    handshakeComplete: bool
    sessionResumed: bool
    resumptionTicket: seq[byte]
    resumptionData: seq[byte]
    incomingStreams: Table[uint64, StreamState]
    ackTracker: qack.AckTrackerModel
    lossDetection: qloss.LossDetectionModel
    congestionController: qcc.CongestionController
    sentPackets: seq[SentPacketMeta]
    pendingDatagrams: seq[PendingDatagram]
    nextLocalBidiStreamId: uint64
    nextLocalUniStreamId: uint64
    latestAckedPacket: uint64
    localStreams: seq[StreamState]
    smoothedRttUs: uint64
    minRttUs: uint64
    latestRttUs: uint64
    rttVarianceUs: uint64
    pathRemoteAddrs: Table[uint8, TransportAddress]
    pathRemoteKeys: Table[uint8, string]
    remotePathIds: Table[string, uint8]
    activeRemoteKey: string

const
  DatagramReceiveNotes = ["receive=false", "receive=true"]
  DatagramSendNotes = ["send=false", "send=true"]
  DefaultCongestionDatagramBytes = 1200'u32
  SendFlagAllowZeroRtt = 0x0001'u32
  SendFlagFin = 0x0004'u32
  StreamOpenFlagUnidirectional = 0x0001'u32
  DefaultSmoothedRttUs = 100_000'u64
  DefaultRttVarianceUs = 50_000'u64
  MinProbeTimeoutUs = 50_000'u64
  BuiltinResumptionTicketMagic = ['N'.byte, 'Q'.byte, 'T'.byte, 'K'.byte]
  BuiltinResumptionTicketVersion = 1'u8
  BuiltinResumptionTicketLifetimeUs = 10'u64 * 60'u64 * 1_000_000'u64
  QUIC_STREAM_EVENT_START_COMPLETE = 0'u32
  QUIC_STREAM_EVENT_RECEIVE = 1'u32
  QUIC_STREAM_EVENT_SEND_COMPLETE = 2'u32
  QUIC_STREAM_EVENT_PEER_SEND_SHUTDOWN = 3'u32
  QUIC_STREAM_EVENT_PEER_SEND_ABORTED = 4'u32
  QUIC_STREAM_EVENT_PEER_RECEIVE_ABORTED = 5'u32
  QUIC_STREAM_EVENT_SEND_SHUTDOWN_COMPLETE = 6'u32
  QUIC_STREAM_EVENT_SHUTDOWN_COMPLETE = 7'u32
  QUIC_STREAM_EVENT_IDEAL_SEND_BUFFER_SIZE = 8'u32
  QUIC_STREAM_EVENT_PEER_ACCEPTED = 9'u32
  QUIC_STREAM_EVENT_CANCEL_ON_LOSS = 10'u32
  QUIC_STREAM_EVENT_RECEIVE_BUFFER_NEEDED = 11'u32
  QUIC_LISTENER_EVENT_NEW_CONNECTION = 0'u32
  QUIC_LISTENER_EVENT_STOP_COMPLETE = 1'u32
  QUIC_LISTENER_EVENT_DOS_MODE_CHANGED = 2'u32
  QUIC_STREAM_SHUTDOWN_FLAG_GRACEFUL* = 0x0001'u32
  QUIC_STREAM_SHUTDOWN_FLAG_ABORT_SEND* = 0x0002'u32
  QUIC_STREAM_SHUTDOWN_FLAG_ABORT_RECEIVE* = 0x0004'u32

var gBuiltinResumptionCache = initTable[string, BuiltinResumptionEntry]()

proc nowMicros(): uint64 =
  uint64(epochTime() * 1_000_000.0)

proc appendUint16BE(buf: var seq[byte]; value: uint16) {.inline.} =
  buf.add(byte((value shr 8) and 0xFF'u16))
  buf.add(byte(value and 0xFF'u16))

proc appendUint64BE(buf: var seq[byte]; value: uint64) {.inline.} =
  for shift in countdown(56, 0, 8):
    buf.add(byte((value shr shift) and 0xFF'u64))

proc readUint16BE(data: openArray[byte]; pos: var int): uint16 {.inline.} =
  if pos + 2 > data.len:
    raise newException(ValueError, "short u16")
  result = (uint16(data[pos]) shl 8) or uint16(data[pos + 1])
  pos += 2

proc readUint64BE(data: openArray[byte]; pos: var int): uint64 {.inline.} =
  if pos + 8 > data.len:
    raise newException(ValueError, "short u64")
  result = 0'u64
  for _ in 0 ..< 8:
    result = (result shl 8) or uint64(data[pos])
    inc pos

type
  BuiltinResumptionTicketMeta = object
    version: uint8
    expiresAtUs: uint64
    serverName: string
    serverPort: uint16
    alpn: string
    data: seq[byte]

proc alpnForConnection(conn: ConnectionState): string {.gcsafe.} =
  if conn.isNil or conn.configuration.isNil or conn.configuration.alpns.len == 0:
    ""
  else:
    conn.configuration.alpns[0]

proc resumptionCacheKey(serverName: string; serverPort: uint16; alpn: string): string {.gcsafe.} =
  serverName & "|" & $serverPort & "|" & alpn

proc resumptionCacheKey(conn: ConnectionState): string {.gcsafe.} =
  if conn.isNil:
    return ""
  resumptionCacheKey(conn.serverName, conn.serverPort, alpnForConnection(conn))

proc parseBuiltinResumptionTicket(ticket: openArray[byte];
    meta: var BuiltinResumptionTicketMeta): bool {.gcsafe.} =
  if ticket.len < 4 + 1 + 8 + 2 + 1 + 1 + 2:
    return false
  for i in 0 ..< BuiltinResumptionTicketMagic.len:
    if ticket[i] != BuiltinResumptionTicketMagic[i]:
      return false
  var pos = BuiltinResumptionTicketMagic.len
  try:
    meta.version = ticket[pos]
    inc pos
    if meta.version != BuiltinResumptionTicketVersion:
      return false
    meta.expiresAtUs = readUint64BE(ticket, pos)
    meta.serverPort = readUint16BE(ticket, pos)
    let serverNameLen = int(ticket[pos]); inc pos
    let alpnLen = int(ticket[pos]); inc pos
    let dataLen = int(readUint16BE(ticket, pos))
    if pos + serverNameLen + alpnLen + dataLen > ticket.len:
      return false
    meta.serverName =
      if serverNameLen == 0: ""
      else: cast[string](newString(serverNameLen))
    if serverNameLen > 0:
      meta.serverName.setLen(serverNameLen)
      copyMem(addr meta.serverName[0], unsafeAddr ticket[pos], serverNameLen)
      pos += serverNameLen
    meta.alpn =
      if alpnLen == 0: ""
      else: cast[string](newString(alpnLen))
    if alpnLen > 0:
      meta.alpn.setLen(alpnLen)
      copyMem(addr meta.alpn[0], unsafeAddr ticket[pos], alpnLen)
      pos += alpnLen
    meta.data = @[]
    if dataLen > 0:
      meta.data = newSeq[byte](dataLen)
      copyMem(addr meta.data[0], unsafeAddr ticket[pos], dataLen)
      pos += dataLen
    true
  except ValueError:
    false

proc builtinResumptionEntryValid(entry: BuiltinResumptionEntry; serverName: string;
    serverPort: uint16; alpn: string): bool {.gcsafe.} =
  if entry.ticket.len == 0:
    return false
  let nowUs = nowMicros()
  if entry.expiresAtUs > 0 and nowUs >= entry.expiresAtUs:
    return false
  var meta: BuiltinResumptionTicketMeta
  if not parseBuiltinResumptionTicket(entry.ticket, meta):
    return false
  if meta.expiresAtUs > 0 and nowUs >= meta.expiresAtUs:
    return false
  meta.serverName == serverName and meta.serverPort == serverPort and meta.alpn == alpn

proc loadBuiltinResumptionEntry(serverName: string; serverPort: uint16; alpn: string): BuiltinResumptionEntry =
  let key = resumptionCacheKey(serverName, serverPort, alpn)
  if key.len == 0 or not gBuiltinResumptionCache.hasKey(key):
    return
  let entry = gBuiltinResumptionCache[key]
  if not builtinResumptionEntryValid(entry, serverName, serverPort, alpn):
    gBuiltinResumptionCache.del(key)
    return
  result = entry

proc storeBuiltinResumptionEntry(conn: ConnectionState; ticket: seq[byte];
    data: seq[byte] = @[]) =
  if conn.isNil or ticket.len == 0:
    return
  let key = resumptionCacheKey(conn)
  if key.len == 0:
    return
  var meta: BuiltinResumptionTicketMeta
  let expiresAtUs =
    if parseBuiltinResumptionTicket(ticket, meta): meta.expiresAtUs
    else: nowMicros() + BuiltinResumptionTicketLifetimeUs
  gBuiltinResumptionCache[key] = BuiltinResumptionEntry(
    ticket: ticket,
    data: data,
    issuedAtUs: nowMicros(),
    expiresAtUs: expiresAtUs
  )

proc initialBidiStreamId(role: ConnectionRole): uint64 =
  if role == crClient: 0'u64 else: 1'u64

proc initialUniStreamId(role: ConnectionRole): uint64 =
  if role == crClient: 2'u64 else: 3'u64

proc initLocalStreamIds(conn: ConnectionState) =
  if conn.isNil or conn.quicConn.isNil:
    return
  conn.nextLocalBidiStreamId = initialBidiStreamId(conn.quicConn.role)
  conn.nextLocalUniStreamId = initialUniStreamId(conn.quicConn.role)

proc allocLocalStreamId(conn: ConnectionState; unidirectional: bool): uint64 =
  if conn.isNil or conn.quicConn.isNil:
    return 0'u64
  if unidirectional:
    result = conn.nextLocalUniStreamId
    conn.nextLocalUniStreamId += 4'u64
  else:
    result = conn.nextLocalBidiStreamId
    conn.nextLocalBidiStreamId += 4'u64

proc prependPendingChunk(stream: StreamState; chunk: PendingStreamChunk) {.gcsafe.} =
  if stream.isNil:
    return
  if stream.pendingChunks.len > 0:
    let head = stream.pendingChunks[0]
    if head.offset == chunk.offset and head.fin == chunk.fin and head.payload == chunk.payload:
      return
  stream.pendingChunks.insert(chunk, 0)

proc recordSentPacket(conn: ConnectionState; meta: SentPacketMeta) {.gcsafe.} =
  if conn.isNil:
    return
  conn.sentPackets.add(meta)
  let packet = qloss.PacketRecord(
    packetNumber: meta.packetNumber,
    encryptEpoch: meta.epoch,
    sentTimeUs: meta.sentTimeUs,
    ackEliciting: meta.ackEliciting,
    packetLength: meta.packetLength
  )
  qloss.onPacketSent(conn.lossDetection, packet)

proc buildBuiltinResumptionTicket(conn: ConnectionState; data: seq[byte] = @[]): seq[byte] {.gcsafe.} =
  if conn.isNil:
    return @[]
  let serverName = conn.serverName
  let alpn = alpnForConnection(conn)
  let nowUs = nowMicros()
  let expiresAtUs = nowUs + BuiltinResumptionTicketLifetimeUs
  let cappedDataLen = min(data.len, high(uint16).int)
  let serverNameLen = min(serverName.len, 255)
  let alpnLen = min(alpn.len, 255)
  result = @[]
  result.add(BuiltinResumptionTicketMagic)
  result.add(BuiltinResumptionTicketVersion)
  result.appendUint64BE(expiresAtUs)
  result.appendUint16BE(conn.serverPort)
  result.add(byte(serverNameLen))
  result.add(byte(alpnLen))
  result.appendUint16BE(uint16(cappedDataLen))
  if serverNameLen > 0:
    for i in 0 ..< serverNameLen:
      result.add(byte(serverName[i].ord and 0xFF))
  if alpnLen > 0:
    for i in 0 ..< alpnLen:
      result.add(byte(alpn[i].ord and 0xFF))
  if cappedDataLen > 0:
    result.add(data[0 ..< cappedDataLen])

proc buildPathChallengeData(conn: ConnectionState; pathId: uint8): array[8, byte] {.gcsafe.} =
  var seed = nowMicros() xor conn.latestAckedPacket xor uint64(pathId)
  for i in 0 .. 7:
    result[i] = byte((seed shr (i * 8)) and 0xFF'u64)

proc remoteAddressKey(remote: TransportAddress): string {.gcsafe.}

proc registerPathRemote(conn: ConnectionState; pathId: uint8;
    remote: TransportAddress) {.gcsafe.} =
  if conn.isNil:
    return
  let remoteKey = remoteAddressKey(remote)
  if remoteKey.len == 0:
    return
  conn.pathRemoteAddrs[pathId] = remote
  conn.pathRemoteKeys[pathId] = remoteKey
  conn.remotePathIds[remoteKey] = pathId

proc activateValidatedPath(conn: ConnectionState; pathId: uint8) {.gcsafe.} =
  if conn.isNil or conn.quicConn.isNil:
    return
  if conn.pathRemoteAddrs.hasKey(pathId):
    conn.remoteAddress = conn.pathRemoteAddrs[pathId]
  if conn.pathRemoteKeys.hasKey(pathId):
    conn.activeRemoteKey = conn.pathRemoteKeys[pathId]
  {.cast(gcsafe).}:
    conn.quicConn.model.completePathValidation(pathId, true)

proc findPathState(conn: ConnectionState; pathId: uint8;
    path: var PathState): bool {.gcsafe.} =
  if conn.isNil or conn.quicConn.isNil:
    return false
  for item in conn.quicConn.model.paths:
    if item.pathId == pathId:
      path = item
      return true
  false

proc nextPathId(conn: ConnectionState): uint8 {.gcsafe.} =
  if conn.isNil or conn.quicConn.isNil:
    return 0'u8
  var highest = -1
  for path in conn.quicConn.model.paths:
    if int(path.pathId) > highest:
      highest = int(path.pathId)
  uint8(min(highest + 1, high(uint8).int))

proc sendOneRttPacket(conn: ConnectionState; payload: seq[byte];
    frameKind: SentFrameKind; ackEliciting: bool;
    stream: StreamState = nil; streamOffset = 0'u64;
    streamPayload: seq[byte] = @[]; streamFin = false;
    clientContext: pointer = nil;
    targetRemote: ptr TransportAddress = nil): uint64 {.gcsafe.}

proc markActivePathValidated(conn: ConnectionState; pathId: uint8) {.gcsafe.} =
  if conn.isNil or conn.quicConn.isNil:
    return
  conn.activateValidatedPath(pathId)

proc maybeInitiatePathMigration(conn: ConnectionState; remote: TransportAddress) {.gcsafe.} =
  if conn.isNil or conn.quicConn.isNil or not conn.settingsOverlay.migrationEnabled:
    return
  let remoteKey = remoteAddressKey(remote)
  if remoteKey.len == 0 or remoteKey == conn.activeRemoteKey:
    return
  if conn.remotePathIds.hasKey(remoteKey):
    let knownPathId = conn.remotePathIds[remoteKey]
    var knownPath: PathState
    if conn.findPathState(knownPathId, knownPath):
      if knownPath.isValidated:
        conn.activateValidatedPath(knownPathId)
        return
      if knownPath.challengeOutstanding:
        return
  let pathId =
    if conn.remotePathIds.hasKey(remoteKey): conn.remotePathIds[remoteKey]
    else: conn.nextPathId()
  let challenge = buildPathChallengeData(conn, pathId)
  conn.registerPathRemote(pathId, remote)
  {.cast(gcsafe).}:
    conn.quicConn.model.initiatePathChallenge(pathId, challenge)
  discard conn.sendOneRttPacket(
    proto.encodePathChallengeFrame(challenge),
    sfkPathChallenge,
    true,
    targetRemote = unsafeAddr remote
  )
  {.cast(gcsafe).}:
    emitDiagnostics(DiagnosticsEvent(
      kind: diagConnectionEvent,
      handle: cast[pointer](conn),
      note: "path migration challenge path=" & $pathId
    ))

proc applyPathResponse(conn: ConnectionState; response: array[8, byte]) {.gcsafe.} =
  if conn.isNil or conn.quicConn.isNil:
    return
  for path in conn.quicConn.model.paths:
    if path.challengeData == response:
      conn.activateValidatedPath(path.pathId)
      {.cast(gcsafe).}:
        emitDiagnostics(DiagnosticsEvent(
          kind: diagConnectionEvent,
          handle: cast[pointer](conn),
          note: "path validated path=" & $path.pathId
        ))
      break

proc findSentPacket(conn: ConnectionState; packetNumber: uint64; epoch: CryptoEpoch;
    meta: var SentPacketMeta): bool {.gcsafe.} =
  if conn.isNil:
    return false
  for sent in conn.sentPackets:
    if sent.epoch == epoch and sent.packetNumber == packetNumber:
      meta = sent
      return true
  false

proc packetAckedByFrame(ack: proto.AckFrame; packetNumber: uint64): bool {.gcsafe.} =
  if ack.ranges.len == 0:
    return packetNumber <= ack.largestAcked
  for ackRange in ack.ranges:
    if packetNumber >= ackRange.smallest and packetNumber <= ackRange.largest:
      return true
  false

proc currentProbeTimeoutUs(conn: ConnectionState): uint64 {.gcsafe.} =
  if conn.isNil:
    return MinProbeTimeoutUs
  let smoothedRtt =
    if conn.smoothedRttUs == 0: DefaultSmoothedRttUs
    else: conn.smoothedRttUs
  let rttVariance =
    if conn.rttVarianceUs == 0: max(smoothedRtt div 2, DefaultRttVarianceUs)
    else: conn.rttVarianceUs
  let ackDelayUs = MaxAckDelayDefaultMs.uint64 * 1_000'u64
  let timeoutUs = qloss.computeProbeTimeout(conn.lossDetection, smoothedRtt, rttVariance) + ackDelayUs
  max(timeoutUs, MinProbeTimeoutUs)

proc detectTimedOutLosses(conn: ConnectionState) {.gcsafe.} =
  if conn.isNil or conn.sentPackets.len == 0:
    return
  let nowUs = nowMicros()
  let probeTimeoutUs = conn.currentProbeTimeoutUs()
  var remaining: seq[SentPacketMeta] = @[]
  var largestLost = 0'u64
  var lostBytes = 0'u32
  for meta in conn.sentPackets:
    if meta.ackEliciting and nowUs > meta.sentTimeUs and
        nowUs - meta.sentTimeUs >= probeTimeoutUs:
      qloss.markPacketLost(conn.lossDetection, meta.packetNumber)
      if meta.packetNumber > largestLost:
        largestLost = meta.packetNumber
      lostBytes = lostBytes + meta.packetLength.uint32
      if meta.frameKind == sfkStream and not meta.stream.isNil:
        let stream = cast[StreamState](meta.stream)
        stream.prependPendingChunk(PendingStreamChunk(
          offset: meta.streamOffset,
          payload: meta.streamPayload,
          fin: meta.streamFin,
          clientContext: meta.clientContext
        ))
      {.cast(gcsafe).}:
        emitDiagnostics(DiagnosticsEvent(
          kind: diagConnectionEvent,
          handle: cast[pointer](conn),
          note: "loss timeout pn=" & $meta.packetNumber & " pto=" & $probeTimeoutUs
        ))
    else:
      remaining.add(meta)
  conn.sentPackets = remaining
  if lostBytes > 0:
    qloss.onProbeTimeoutFired(conn.lossDetection)
    let loss = LossEventSnapshot(
      largestPacketNumberLost: largestLost,
      largestSentPacketNumber: conn.lossDetection.largestSentPacketNumber,
      retransmittableBytesLost: lostBytes,
      persistentCongestion: conn.lossDetection.probeCount >= PersistentCongestionThreshold.uint16
    )
    qcc.onLost(conn.congestionController, loss)
    {.cast(gcsafe).}:
      emitDiagnostics(DiagnosticsEvent(
        kind: diagConnectionEvent,
        handle: cast[pointer](conn),
        note: "pto fired lostBytes=" & $lostBytes & " probeCount=" & $conn.lossDetection.probeCount
      ))

proc flushStream(state: StreamState) {.gcsafe.}
proc flushPendingDatagrams(conn: ConnectionState) {.gcsafe.}

proc getHandleFast(handle: HQUIC): QuicHandleState {.quicApiHot.} =
  if handle.isNil:
    return nil
  cast[QuicHandleState](handle)

proc connectionFromHandleFast(handle: HQUIC): ConnectionState {.quicApiHot.} =
  let base = getHandleFast(handle)
  if base.isNil or base.kind != qhkConnection:
    return nil
  ConnectionState(base)

proc registrationFromHandleFast(handle: HQUIC): RegistrationState {.quicApiHot.} =
  let base = getHandleFast(handle)
  if base.isNil or base.kind != qhkRegistration:
    return nil
  RegistrationState(base)

proc configurationFromHandleFast(handle: HQUIC): ConfigurationState {.quicApiHot.} =
  let base = getHandleFast(handle)
  if base.isNil or base.kind != qhkConfiguration:
    return nil
  ConfigurationState(base)

proc streamFromHandleFast(handle: HQUIC): StreamState {.quicApiHot.} =
  let base = getHandleFast(handle)
  if base.isNil or base.kind != qhkStream:
    return nil
  StreamState(base)

proc listenerFromHandleFast(handle: HQUIC): ListenerState {.quicApiHot.} =
  let base = getHandleFast(handle)
  if base.isNil or base.kind != qhkListener:
    return nil
  ListenerState(base)

proc emitConnectionEvent(state: ConnectionState; event: var ConnectionEvent) =
  event.connection = cast[HQUIC](state)
  if state.eventHandlers.len > 0:
    for handler in state.eventHandlers:
      if handler != nil:
        handler(event)
  let diagNote = (if event.note.len > 0: event.note else: $event.kind)
  emitDiagnostics(DiagnosticsEvent(
    kind: diagConnectionEvent,
    handle: cast[pointer](state),
    paramId: event.paramId,
    note: diagNote))

proc emitStreamEvent(state: StreamState; event: var StreamEvent) =
  event.stream = cast[HQUIC](state)
  if state.eventHandlers.len > 0:
    for handler in state.eventHandlers:
      if handler != nil:
        handler(event)

proc updateDatagramState(state: ConnectionState; param: uint32; enabled: bool;
    isReceive: bool) {.inline.} =
  if state.isNil:
    return
  let note = if isReceive:
      DatagramReceiveNotes[ord(enabled)]
    else:
      DatagramSendNotes[ord(enabled)]
  if isReceive:
    state.datagramReceiveEnabled = enabled
  else:
    state.datagramSendEnabled = enabled
  var ev = ConnectionEvent(
    kind: ceDatagramStateChanged,
    paramId: param,
    boolValue: enabled,
    note: note)
  emitConnectionEvent(state, ev)
  emitDiagnostics(DiagnosticsEvent(
    kind: diagConnectionParamSet,
    handle: cast[pointer](state),
    paramId: param,
    note: note))

proc algorithmToRaw(algorithm: CongestionAlgorithm): uint16 {.inline.} =
  case algorithm
  of caCubic:
    0'u16
  of caBbr:
    1'u16

proc rawToAlgorithm(raw: uint16; algorithm: var CongestionAlgorithm): bool {.inline.} =
  case raw
  of 0'u16:
    algorithm = caCubic
    true
  of 1'u16:
    algorithm = caBbr
    true
  else:
    false

proc algorithmNote(algorithm: CongestionAlgorithm): string {.inline.} =
  case algorithm
  of caCubic:
    "congestion=CUBIC"
  of caBbr:
    "congestion=BBR"

var
  gHandleRegistry = initTable[HQUIC, QuicHandleState]()
  gApiTableInstance: QuicApiTable
  gApiTableRefCount: int
  gNextConnectionId: uint32 = 1
type
  GlobalExecutionConfigState* = object
    flags*: uint32
    pollingIdleTimeoutUs*: uint32
    processors*: seq[uint16]
    applied*: bool

var gGlobalExecutionConfig: GlobalExecutionConfigState

proc storeHandle(state: QuicHandleState): HQUIC =
  let raw = cast[HQUIC](state)
  gHandleRegistry[raw] = state
  raw

proc fetchHandle(handle: HQUIC): QuicHandleState =
  if handle.isNil:
    return nil
  gHandleRegistry.getOrDefault(handle, nil)

proc releaseHandle(handle: HQUIC) =
  if handle.isNil:
    return
  gHandleRegistry.del(handle)

proc generateConnectionIds(): (ConnectionId, ConnectionId) =
  let base = gNextConnectionId
  inc gNextConnectionId
  let clientBytes = @[
    uint8((base shr 0) and 0xFF),
    uint8((base shr 8) and 0xFF),
    uint8((base shr 16) and 0xFF),
    uint8((base shr 24) and 0xFF)
  ]
  let serverBytes = @[
    uint8((base shr 4) and 0xFF),
    uint8((base shr 12) and 0xFF),
    uint8((base shr 20) and 0xFF),
    uint8((base shr 28) and 0xFF)
  ]
  (initConnectionId(clientBytes), initConnectionId(serverBytes))

proc toRegistration(handle: HQUIC): RegistrationState =
  let fast = registrationFromHandleFast(handle)
  if not fast.isNil:
    return fast
  let base = fetchHandle(handle)
  if base.isNil or base.kind != qhkRegistration:
    return nil
  RegistrationState(base)

proc toConfiguration(handle: HQUIC): ConfigurationState =
  let fast = configurationFromHandleFast(handle)
  if not fast.isNil:
    return fast
  let base = fetchHandle(handle)
  if base.isNil or base.kind != qhkConfiguration:
    return nil
  ConfigurationState(base)

proc toConnection(handle: HQUIC): ConnectionState =
  let fast = connectionFromHandleFast(handle)
  if not fast.isNil:
    return fast
  let base = fetchHandle(handle)
  if base.isNil or base.kind != qhkConnection:
    return nil
  ConnectionState(base)

proc toStream(handle: HQUIC): StreamState =
  let fast = streamFromHandleFast(handle)
  if not fast.isNil:
    return fast
  let base = fetchHandle(handle)
  if base.isNil or base.kind != qhkStream:
    return nil
  StreamState(base)

proc toListener(handle: HQUIC): ListenerState =
  let fast = listenerFromHandleFast(handle)
  if not fast.isNil:
    return fast
  let base = fetchHandle(handle)
  if base.isNil or base.kind != qhkListener:
    return nil
  ListenerState(base)

proc initConnectedEvent(alpnSource: string; sessionResumed = false): QuicConnectionEvent =
  result.Type = QUIC_CONNECTION_EVENT_CONNECTED
  result.Padding = 0
  var connected: QuicConnectionEventConnectedPayload
  connected.SessionResumed = BOOLEAN(if sessionResumed: 1 else: 0)
  var negotiatedLen = alpnSource.len
  if negotiatedLen > 255:
    negotiatedLen = 255
  connected.NegotiatedAlpnLength = uint8(negotiatedLen)
  connected.Reserved = 0
  if alpnSource.len == 0:
    connected.NegotiatedAlpn = nil
  else:
    connected.NegotiatedAlpn = cast[ptr uint8](alpnSource.cstring)
  system.copyMem(addr result.Data[0], unsafeAddr connected, sizeof(connected))

proc cidBytes(cid: ConnectionId): seq[byte] =
  if cid.length == 0:
    return @[]
  result = newSeq[byte](int(cid.length))
  for i in 0 ..< int(cid.length):
    result[i] = cid.bytes[i]

proc remoteAddressKey(remote: TransportAddress): string {.gcsafe.} =
  $remote

proc connectionSendMaterial(conn: ConnectionState): tuple[key, iv, hp: seq[byte]] =
  if conn.isNil or conn.quicConn.isNil:
    return (@[], @[], @[])
  if conn.quicConn.role == crServer:
    (conn.oneRttKeys.serverKey, conn.oneRttKeys.serverIv, conn.oneRttKeys.serverHp)
  else:
    (conn.oneRttKeys.clientKey, conn.oneRttKeys.clientIv, conn.oneRttKeys.clientHp)

proc connectionReceiveMaterial(conn: ConnectionState): tuple[key, iv, hp: seq[byte]] =
  if conn.isNil or conn.quicConn.isNil:
    return (@[], @[], @[])
  if conn.quicConn.role == crServer:
    (conn.oneRttKeys.clientKey, conn.oneRttKeys.clientIv, conn.oneRttKeys.clientHp)
  else:
    (conn.oneRttKeys.serverKey, conn.oneRttKeys.serverIv, conn.oneRttKeys.serverHp)

proc emitNativeConnected(state: ConnectionState) =
  if state.isNil or state.callback.isNil:
    return
  let negotiatedAlpn =
    if state.configuration.isNil or state.configuration.alpns.len == 0: ""
    else: state.configuration.alpns[0]
  var event = initConnectedEvent(negotiatedAlpn, state.sessionResumed)
  discard state.callback(cast[HQUIC](state), state.callbackContext, addr event)

proc emitListenerNewConnection(state: ListenerState; connection: HQUIC) =
  if state.isNil or state.callback.isNil or connection.isNil:
    return
  var native = QuicListenerEvent(Type: QUIC_LISTENER_EVENT_NEW_CONNECTION, Padding: 0)
  var payload = QuicListenerEventNewConnectionPayload(
    Info: nil,
    Connection: connection
  )
  copyMem(addr native.Data[0], addr payload, sizeof(payload))
  discard state.callback(cast[HQUIC](state), state.callbackContext, addr native)

proc emitPeerStreamStarted(state: ConnectionState; stream: StreamState) =
  if state.isNil or stream.isNil:
    return
  let streamHandle = cast[HQUIC](stream)
  if not state.callback.isNil:
    var native = QuicConnectionEvent(Type: QUIC_CONNECTION_EVENT_PEER_STREAM_STARTED, Padding: 0)
    var payload = QuicConnectionEventPeerStreamStartedPayload(
      Stream: streamHandle,
      Flags: QUIC_STREAM_OPEN_FLAGS(0)
    )
    copyMem(addr native.Data[0], addr payload, sizeof(payload))
    discard state.callback(cast[HQUIC](state), state.callbackContext, addr native)
  var ev = ConnectionEvent(
    kind: cePeerStreamStarted,
    stream: streamHandle,
    streamFlags: 0'u32,
    streamIsUnidirectional: false,
    note: "peer stream started")
  emitConnectionEvent(state, ev)

proc emitStreamReceive(state: StreamState; offset: uint64; payloadData: seq[byte]; fin: bool) =
  if state.isNil:
    return
  if not state.callback.isNil:
    var native = QuicStreamEvent(Type: QUIC_STREAM_EVENT_RECEIVE, Padding: 0)
    var buffer = QuicBuffer(
      Length: uint32(payloadData.len),
      Buffer: (if payloadData.len == 0: nil else: cast[ptr uint8](unsafeAddr payloadData[0]))
    )
    var payload = QuicStreamEventReceivePayload(
      AbsoluteOffset: offset,
      TotalBufferLength: uint64(payloadData.len),
      Buffers: (if payloadData.len == 0: nil else: cast[pointer](addr buffer)),
      BufferCount: (if payloadData.len == 0: 0'u32 else: 1'u32),
      Flags: (if fin: 0x01'u32 else: 0'u32)
    )
    copyMem(addr native.Data[0], addr payload, sizeof(payload))
    discard state.callback(cast[HQUIC](state), state.callbackContext, addr native)
  var ev = StreamEvent(
    kind: seReceive,
    absoluteOffset: offset,
    totalBufferLength: uint64(payloadData.len),
    bufferCount: (if payloadData.len == 0: 0'u32 else: 1'u32),
    flags: (if fin: 0x01'u32 else: 0'u32),
    payload: payloadData
  )
  emitStreamEvent(state, ev)
  if fin:
    if not state.callback.isNil:
      var native = QuicStreamEvent(Type: QUIC_STREAM_EVENT_PEER_SEND_SHUTDOWN, Padding: 0)
      discard state.callback(cast[HQUIC](state), state.callbackContext, addr native)
    var shutdownEv = StreamEvent(kind: sePeerSendShutdown)
    emitStreamEvent(state, shutdownEv)

proc sendOneRttPacket(conn: ConnectionState; payload: seq[byte];
    frameKind: SentFrameKind; ackEliciting: bool;
    stream: StreamState = nil; streamOffset = 0'u64;
    streamPayload: seq[byte] = @[]; streamFin = false;
    clientContext: pointer = nil;
    targetRemote: ptr TransportAddress = nil): uint64 {.gcsafe.} =
  if conn.isNil or conn.transport.isNil or not conn.handshakeComplete:
    return 0'u64
  conn.detectTimedOutLosses()
  if ackEliciting and not qcc.canSend(conn.congestionController):
    {.cast(gcsafe).}:
      emitDiagnostics(DiagnosticsEvent(
        kind: diagConnectionEvent,
        handle: cast[pointer](conn),
        note: "TX blocked by congestion controller"
      ))
    return 0'u64
  let nowUs = nowMicros()
  if ackEliciting:
    let allowance = qcc.sendAllowance(conn.congestionController, nowUs)
    if allowance > 0 and uint64(payload.len) > allowance:
      {.cast(gcsafe).}:
        emitDiagnostics(DiagnosticsEvent(
          kind: diagConnectionEvent,
          handle: cast[pointer](conn),
          note: "TX exceeds send allowance=" & $allowance
        ))
      return 0'u64
  let sendMat = connectionSendMaterial(conn)
  if sendMat.key.len < 16 or sendMat.iv.len < 12 or sendMat.hp.len < 16:
    return 0'u64

  let pn = uint64(conn.quicConn.model.packetSpaces[ceOneRtt].nextPacketNumber())
  let destCid = cidBytes(conn.peerCid)
  var header: seq[byte] = @[]
  header.add(0x43'u8)
  header.add(destCid)
  let pnOffset = header.len
  proto.writeUint32(header, uint32(pn))

  var tag: array[16, byte]
  let ciphertext = tls.encryptPacket(sendMat.key, sendMat.iv, pn, header, payload, tag)
  var packet = header & ciphertext & @tag

  if packet.len >= pnOffset + 4 + 16:
    let sample = packet[pnOffset + 4 ..< pnOffset + 4 + 16]
    var pnSlice: array[4, byte]
    for i in 0 .. 3:
      pnSlice[i] = packet[pnOffset + i]
    tls.applyHeaderProtection(sendMat.hp, sample, packet[0], pnSlice)
    for i in 0 .. 3:
      packet[pnOffset + i] = pnSlice[i]

  let sendRemote =
    if targetRemote.isNil: conn.remoteAddress
    else: targetRemote[]
  asyncCheck conn.transport.sendTo(sendRemote, packet)
  conn.recordSentPacket(SentPacketMeta(
    packetNumber: pn,
    epoch: ceOneRtt,
    ackEliciting: ackEliciting,
    packetLength: uint16(min(packet.len, high(uint16).int)),
    sentTimeUs: nowUs,
    frameKind: frameKind,
    stream: cast[pointer](stream),
    streamOffset: streamOffset,
    streamPayload: streamPayload,
    streamFin: streamFin,
    clientContext: clientContext
  ))
  qcc.onPacketSent(
    conn.congestionController,
    uint32(min(packet.len, high(uint32).int)),
    ackEliciting,
    false,
    nowUs
  )
  {.cast(gcsafe).}:
    emitDiagnostics(DiagnosticsEvent(
      kind: diagConnectionEvent,
      handle: cast[pointer](conn),
      note: "TX 1-RTT pn=" & $pn & " kind=" & $frameKind
    ))
  pn

proc applyAckFrame(conn: ConnectionState; ack: proto.AckFrame; epoch: CryptoEpoch) {.gcsafe.} =
  if conn.isNil:
    return
  var remaining: seq[SentPacketMeta] = @[]
  var ackedBytes = 0'u32
  var ackedMeta: SentPacketMeta
  var ackedMetaFound = conn.findSentPacket(ack.largestAcked, epoch, ackedMeta)
  for meta in conn.sentPackets:
    if meta.epoch == epoch and packetAckedByFrame(ack, meta.packetNumber):
      ackedBytes += meta.packetLength.uint32
    else:
      remaining.add(meta)
  conn.sentPackets = remaining
  conn.quicConn.model.packetSpaces[epoch].recordAckedPacket(ack.largestAcked)
  conn.latestAckedPacket = max(conn.latestAckedPacket, ack.largestAcked)
  let nowUs = nowMicros()
  if ackedMetaFound and ackedMeta.sentTimeUs > 0 and nowUs >= ackedMeta.sentTimeUs:
    let sampleRtt = nowUs - ackedMeta.sentTimeUs
    conn.latestRttUs = sampleRtt
    if conn.minRttUs == 0 or sampleRtt < conn.minRttUs:
      conn.minRttUs = sampleRtt
    if conn.smoothedRttUs == 0:
      conn.smoothedRttUs = sampleRtt
      conn.rttVarianceUs = sampleRtt div 2
    else:
      let rttDelta =
        if conn.smoothedRttUs > sampleRtt: conn.smoothedRttUs - sampleRtt
        else: sampleRtt - conn.smoothedRttUs
      conn.rttVarianceUs = ((3 * conn.rttVarianceUs) + rttDelta) div 4
      conn.smoothedRttUs = ((7 * conn.smoothedRttUs) + sampleRtt) div 8
  let ackEvent = AckEventSnapshot(
    timeNow: nowUs,
    largestAck: ack.largestAcked,
    largestSentPacketNumber: conn.lossDetection.largestSentPacketNumber,
    totalAckedRetransmittableBytes: conn.lossDetection.totalBytesAcked + uint64(ackedBytes),
    ackedRetransmittableBytes: ackedBytes,
    smoothedRtt: max(conn.smoothedRttUs, 1'u64),
    minRtt: max(conn.minRttUs, 1'u64),
    oneWayDelay: 0'u64,
    adjustedAckTime: nowUs,
    implicitAck: false,
    hasLoss: false,
    largestAckAppLimited: false,
    minRttValid: true
  )
  qloss.onAckReceived(conn.lossDetection, ackEvent)
  qcc.onAcked(conn.congestionController, ackEvent)
  {.cast(gcsafe).}:
    emitDiagnostics(DiagnosticsEvent(
      kind: diagConnectionEvent,
      handle: cast[pointer](conn),
      note: "RX ACK largest=" & $ack.largestAcked
    ))

proc sendAckFrame(conn: ConnectionState; largestAcked: uint64;
    targetRemote: ptr TransportAddress = nil) {.gcsafe.} =
  if conn.isNil or not conn.handshakeComplete:
    return
  let ackPayload = proto.encodeAckFrame(largestAcked, 0'u64)
  discard conn.sendOneRttPacket(ackPayload, sfkAck, false, targetRemote = targetRemote)

proc flushPendingDatagrams(conn: ConnectionState) {.gcsafe.} =
  if conn.isNil or not conn.handshakeComplete or conn.pendingDatagrams.len == 0:
    return
  var pending = conn.pendingDatagrams
  conn.pendingDatagrams = @[]
  for datagram in pending:
    let frame = proto.encodeDatagramFrame(datagram.payload, true)
    discard conn.sendOneRttPacket(
      frame,
      sfkDatagram,
      true,
      clientContext = datagram.clientContext
    )

proc msquicSetContext(handle: HQUIC; context: pointer) {.cdecl, quicApiHot.} =
  let state = getHandleFast(handle)
  if state.isNil:
    return
  state.context = context

proc msquicGetContext(handle: HQUIC): pointer {.cdecl, quicApiHot.} =
  let state = getHandleFast(handle)
  if state.isNil:
    return nil
  state.context

proc streamEmitEvent(state: StreamState; eventType: uint32;
    build: proc (buffer: ptr uint8) {.gcsafe.} = nil) =
  if state.isNil or state.callback.isNil:
    return
  var native = QuicStreamEvent(Type: eventType, Padding: 0)
  if not build.isNil:
    build(addr native.Data[0])
  discard state.callback(cast[HQUIC](state), state.callbackContext, addr native)

proc listenerEmitEvent(state: ListenerState; eventType: uint32;
    build: proc (buffer: ptr uint8) {.gcsafe.} = nil) =
  if state.isNil or state.callback.isNil:
    return
  var native = QuicListenerEvent(Type: eventType, Padding: 0)
  if not build.isNil:
    build(addr native.Data[0])
  discard state.callback(cast[HQUIC](state), state.callbackContext, addr native)

proc msquicSetCallbackHandler(handle: HQUIC; handler: pointer;
    context: pointer) {.cdecl, quicApiHot.} =
  let state = getHandleFast(handle)
  if state.isNil:
    return
  state.context = context
  case state.kind
  of qhkConnection:
    let conn = ConnectionState(state)
    conn.callback = cast[QuicConnectionCallback](handler)
    conn.callbackContext = context
  of qhkStream:
    let stream = StreamState(state)
    stream.callback = cast[QuicStreamCallback](handler)
    stream.callbackContext = context
  of qhkListener:
    let listener = ListenerState(state)
    listener.callback = cast[QuicListenerCallback](handler)
    listener.callbackContext = context
  else:
    discard

proc msquicSetParam(handle: HQUIC; param: uint32; bufferLength: uint32;
    buffer: pointer): QUIC_STATUS {.cdecl.} =
  if handle.isNil and param != QUIC_PARAM_GLOBAL_EXECUTION_CONFIG:
    return QUIC_STATUS_INVALID_PARAMETER
  case param
  of QUIC_PARAM_GLOBAL_EXECUTION_CONFIG:
    if buffer.isNil or bufferLength < QUIC_GLOBAL_EXECUTION_CONFIG_MIN_SIZE:
      return QUIC_STATUS_INVALID_PARAMETER
    var header: GlobalExecutionConfigHeader
    copyMem(addr header, buffer, sizeof(GlobalExecutionConfigHeader))
    let expectedLength = QUIC_GLOBAL_EXECUTION_CONFIG_MIN_SIZE +
      header.ProcessorCount.uint32 * sizeof(uint16).uint32
    if bufferLength < expectedLength:
      return QUIC_STATUS_INVALID_PARAMETER
    if header.ProcessorCount > 256'u32:
      return QUIC_STATUS_INVALID_PARAMETER
    gGlobalExecutionConfig.flags = header.Flags
    gGlobalExecutionConfig.pollingIdleTimeoutUs = header.PollingIdleTimeoutUs
    gGlobalExecutionConfig.processors.setLen(header.ProcessorCount.int)
    if header.ProcessorCount > 0:
      let baseAddr = cast[uint](buffer) + cast[uint](QUIC_GLOBAL_EXECUTION_CONFIG_MIN_SIZE)
      let arrayPtr = cast[ptr UncheckedArray[uint16]](cast[pointer](baseAddr))
      for idx in 0 ..< header.ProcessorCount.int:
        gGlobalExecutionConfig.processors[idx] = arrayPtr[][idx]
    gGlobalExecutionConfig.applied = true
    QUIC_STATUS_SUCCESS
  of QUIC_PARAM_CONN_DATAGRAM_RECEIVE_ENABLED:
    if buffer.isNil or bufferLength < 1:
      return QUIC_STATUS_INVALID_PARAMETER
    var state = connectionFromHandleFast(handle)
    if state.isNil:
      state = toConnection(handle)
    if state.isNil:
      return QUIC_STATUS_INVALID_PARAMETER
    let enabled = cast[ptr uint8](buffer)[] != 0
    updateDatagramState(state, param, enabled, true)
    QUIC_STATUS_SUCCESS
  of QUIC_PARAM_CONN_DATAGRAM_SEND_ENABLED:
    if buffer.isNil or bufferLength < 1:
      return QUIC_STATUS_INVALID_PARAMETER
    var state = connectionFromHandleFast(handle)
    if state.isNil:
      state = toConnection(handle)
    if state.isNil:
      return QUIC_STATUS_INVALID_PARAMETER
    let enabled = cast[ptr uint8](buffer)[] != 0
    updateDatagramState(state, param, enabled, false)
    QUIC_STATUS_SUCCESS
  of QUIC_PARAM_CONN_STREAM_SCHEDULING_SCHEME:
    let state = toConnection(handle)
    if state.isNil or buffer.isNil or bufferLength < 4:
      return QUIC_STATUS_INVALID_PARAMETER
    state.streamSchedulingScheme = cast[ptr uint32](buffer)[]
    var ev = ConnectionEvent(
      kind: ceParameterUpdated,
      paramId: param,
      note: "stream scheduling=" & $state.streamSchedulingScheme)
    emitConnectionEvent(state, ev)
    emitDiagnostics(DiagnosticsEvent(
      kind: diagConnectionParamSet,
      handle: handle,
      paramId: param,
      note: "stream scheduling=" & $state.streamSchedulingScheme))
    QUIC_STATUS_SUCCESS
  of QUIC_PARAM_CONN_CONGESTION_CONTROL_ALGORITHM:
    if buffer.isNil or bufferLength < sizeof(uint16).uint32:
      return QUIC_STATUS_INVALID_PARAMETER
    var state = connectionFromHandleFast(handle)
    if state.isNil:
      state = toConnection(handle)
    if state.isNil:
      return QUIC_STATUS_INVALID_PARAMETER
    var algorithm: CongestionAlgorithm
    let raw = cast[ptr uint16](buffer)[]
    if not rawToAlgorithm(raw, algorithm):
      return QUIC_STATUS_INVALID_PARAMETER
    if state.congestionAlgorithm == algorithm:
      return QUIC_STATUS_SUCCESS
    state.congestionAlgorithm = algorithm
    qcc.switchAlgorithm(state.congestionController, algorithm)
    let note = algorithmNote(algorithm)
    var ev = ConnectionEvent(
      kind: ceParameterUpdated,
      paramId: param,
      note: note)
    emitConnectionEvent(state, ev)
    emitDiagnostics(DiagnosticsEvent(
      kind: diagConnectionParamSet,
      handle: cast[pointer](state),
      paramId: param,
      note: note))
    QUIC_STATUS_SUCCESS
  of QUIC_PARAM_CONN_CLOSE_REASON_PHRASE:
    let state = toConnection(handle)
    if state.isNil:
      return QUIC_STATUS_INVALID_PARAMETER
    if buffer.isNil:
      state.closeReason = ""
      return QUIC_STATUS_SUCCESS
    var reason = newString(int(bufferLength))
    if bufferLength > 0:
      copyMem(addr reason[0], buffer, int(bufferLength))
    state.closeReason = reason
    var ev = ConnectionEvent(
      kind: ceParameterUpdated,
      paramId: param,
      note: "close reason updated")
    emitConnectionEvent(state, ev)
    emitDiagnostics(DiagnosticsEvent(
      kind: diagConnectionParamSet,
      handle: handle,
      paramId: param,
      note: "close reason updated"))
    QUIC_STATUS_SUCCESS
  of QUIC_PARAM_CONN_DISABLE_1RTT_ENCRYPTION:
    let state = toConnection(handle)
    if state.isNil or buffer.isNil or bufferLength < 1:
      return QUIC_STATUS_INVALID_PARAMETER
    state.disable1RttEncryption = cast[ptr uint8](buffer)[] != 0
    var ev = ConnectionEvent(
      kind: ceParameterUpdated,
      paramId: param,
      boolValue: state.disable1RttEncryption,
      note: "disable 1rtt")
    emitConnectionEvent(state, ev)
    emitDiagnostics(DiagnosticsEvent(
      kind: diagConnectionParamSet,
      handle: handle,
      paramId: param,
      note: "disable1rtt=" & $(state.disable1RttEncryption)))
    QUIC_STATUS_SUCCESS
  of QUIC_PARAM_CONN_SETTINGS:
    let state = toConnection(handle)
    if state.isNil or buffer.isNil or bufferLength < sizeof(QuicSettingsOverlay).uint32:
      return QUIC_STATUS_INVALID_PARAMETER
    var overlay: QuicSettingsOverlay
    copyMem(addr overlay, buffer, sizeof(QuicSettingsOverlay))
    state.settingsOverlay = overlay
    state.datagramReceiveEnabled = overlay.datagramReceiveEnabled
    var ev = ConnectionEvent(
      kind: ceSettingsApplied,
      paramId: param,
      boolValue: overlay.datagramReceiveEnabled,
      note: "settings applied")
    emitConnectionEvent(state, ev)
    emitDiagnostics(DiagnosticsEvent(
      kind: diagConnectionParamSet,
      handle: handle,
      paramId: param,
      note: "settings applied"))
    QUIC_STATUS_SUCCESS
  else:
    if bufferLength == 0 or buffer.isNil:
      return QUIC_STATUS_INVALID_PARAMETER
    QUIC_STATUS_NOT_SUPPORTED

proc msquicGetParam(handle: HQUIC; param: uint32; bufferLength: ptr uint32;
    buffer: pointer): QUIC_STATUS {.cdecl.} =
  if handle.isNil or bufferLength.isNil:
    return QUIC_STATUS_INVALID_PARAMETER
  case param
  of QUIC_PARAM_CONN_DATAGRAM_RECEIVE_ENABLED:
    let state = toConnection(handle)
    if state.isNil:
      return QUIC_STATUS_INVALID_PARAMETER
    let required = 1'u32
    if buffer.isNil or bufferLength[] < required:
      bufferLength[] = required
      return QUIC_STATUS_SUCCESS
    cast[ptr uint8](buffer)[] = (if state.datagramReceiveEnabled: 1'u8 else: 0'u8)
    bufferLength[] = required
    QUIC_STATUS_SUCCESS
  of QUIC_PARAM_CONN_DATAGRAM_SEND_ENABLED:
    let state = toConnection(handle)
    if state.isNil:
      return QUIC_STATUS_INVALID_PARAMETER
    let required = 1'u32
    if buffer.isNil or bufferLength[] < required:
      bufferLength[] = required
      return QUIC_STATUS_SUCCESS
    cast[ptr uint8](buffer)[] = (if state.datagramSendEnabled: 1'u8 else: 0'u8)
    bufferLength[] = required
    QUIC_STATUS_SUCCESS
  of QUIC_PARAM_CONN_STREAM_SCHEDULING_SCHEME:
    let state = toConnection(handle)
    if state.isNil:
      return QUIC_STATUS_INVALID_PARAMETER
    let required = 4'u32
    if buffer.isNil or bufferLength[] < required:
      bufferLength[] = required
      return QUIC_STATUS_SUCCESS
    cast[ptr uint32](buffer)[] = state.streamSchedulingScheme
    bufferLength[] = required
    QUIC_STATUS_SUCCESS
  of QUIC_PARAM_CONN_CONGESTION_CONTROL_ALGORITHM:
    let state = toConnection(handle)
    if state.isNil:
      return QUIC_STATUS_INVALID_PARAMETER
    let required = sizeof(uint16).uint32
    if buffer.isNil or bufferLength[] < required:
      bufferLength[] = required
      return QUIC_STATUS_SUCCESS
    cast[ptr uint16](buffer)[] = algorithmToRaw(state.congestionAlgorithm)
    bufferLength[] = required
    QUIC_STATUS_SUCCESS
  of QUIC_PARAM_CONN_CLOSE_REASON_PHRASE:
    let state = toConnection(handle)
    if state.isNil:
      return QUIC_STATUS_INVALID_PARAMETER
    let required = uint32(state.closeReason.len)
    let requested = bufferLength[]
    bufferLength[] = required
    if buffer.isNil or required == 0:
      return QUIC_STATUS_SUCCESS
    if requested < required:
      return QUIC_STATUS_INVALID_PARAMETER
    copyMem(buffer, addr state.closeReason[0], int(required))
    QUIC_STATUS_SUCCESS
  of QUIC_PARAM_CONN_DISABLE_1RTT_ENCRYPTION:
    let state = toConnection(handle)
    if state.isNil:
      return QUIC_STATUS_INVALID_PARAMETER
    let required = 1'u32
    if buffer.isNil or bufferLength[] < required:
      bufferLength[] = required
      return QUIC_STATUS_SUCCESS
    cast[ptr uint8](buffer)[] = (if state.disable1RttEncryption: 1'u8 else: 0'u8)
    bufferLength[] = required
    QUIC_STATUS_SUCCESS
  of QUIC_PARAM_CONN_SETTINGS:
    let state = toConnection(handle)
    if state.isNil:
      return QUIC_STATUS_INVALID_PARAMETER
    let required = sizeof(QuicSettingsOverlay).uint32
    let requested = bufferLength[]
    bufferLength[] = required
    if buffer.isNil:
      return QUIC_STATUS_SUCCESS
    if requested < required:
      return QUIC_STATUS_INVALID_PARAMETER
    copyMem(buffer, addr state.settingsOverlay, sizeof(QuicSettingsOverlay))
    QUIC_STATUS_SUCCESS
  of QUIC_PARAM_STREAM_ID:
    let state = toStream(handle)
    if state.isNil:
      return QUIC_STATUS_INVALID_PARAMETER
    let required = sizeof(uint64).uint32
    if buffer.isNil or bufferLength[] < required:
      bufferLength[] = required
      return QUIC_STATUS_SUCCESS
    cast[ptr uint64](buffer)[] = state.streamId
    bufferLength[] = required
    QUIC_STATUS_SUCCESS
  else:
    QUIC_STATUS_NOT_SUPPORTED

proc msquicRegistrationOpen(config: ptr QuicRegistrationConfigC;
    registration: ptr HQUIC): QUIC_STATUS {.cdecl.} =
  if registration.isNil:
    return QUIC_STATUS_INVALID_PARAMETER
  var profile = rpLowLatency
  var appName = ""
  if not config.isNil:
    if not config.AppName.isNil:
      appName = $config.AppName
    case config.ExecutionProfile
    of QUIC_EXECUTION_PROFILE(1):
      profile = rpMaxThroughput
    of QUIC_EXECUTION_PROFILE(2):
      profile = rpScavenger
    of QUIC_EXECUTION_PROFILE(3):
      profile = rpRealTime
    else:
      profile = rpLowLatency
  let state = RegistrationState(kind: qhkRegistration)
  state.profile = profile
  state.appName = appName
  let raw = storeHandle(state)
  registration[] = raw
  emitDiagnostics(DiagnosticsEvent(
    kind: diagRegistrationOpened,
    handle: raw,
    note: appName))
  QUIC_STATUS_SUCCESS

proc msquicRegistrationClose(registration: HQUIC) {.cdecl.} =
  releaseHandle(registration)

proc msquicRegistrationShutdown(registration: HQUIC;
    flags: QUIC_CONNECTION_SHUTDOWN_FLAGS; errorCode: QUIC_UINT62) {.cdecl.} =
  let state = toRegistration(registration)
  if state.isNil:
    return
  state.shuttingDown = true
  discard flags
  discard errorCode
  emitDiagnostics(DiagnosticsEvent(
    kind: diagRegistrationShutdown,
    handle: registration,
    note: "flags=" & $flags & " error=" & $errorCode))

proc msquicConfigurationOpen(registration: HQUIC;
    alpnBuffers: ptr QuicBuffer; alpnBufferCount: uint32; settings: pointer;
    settingsSize: uint32; context: pointer; configuration: ptr HQUIC): QUIC_STATUS {.cdecl.} =
  discard settings
  discard settingsSize
  discard context
  if configuration.isNil:
    return QUIC_STATUS_INVALID_PARAMETER
  let reg = toRegistration(registration)
  if reg.isNil:
    return QUIC_STATUS_INVALID_PARAMETER
  let state = ConfigurationState(kind: qhkConfiguration)
  state.registration = reg
  state.alpns = newSeq[string]()
  if alpnBufferCount > 0 and not alpnBuffers.isNil:
    let bufferArray = cast[ptr UncheckedArray[QuicBuffer]](alpnBuffers)
    for i in 0'u32 ..< alpnBufferCount:
      let buf = bufferArray[][int(i)]
      if buf.Length == 0 or buf.Buffer.isNil:
        state.alpns.add("")
      else:
        let data = cast[ptr UncheckedArray[uint8]](buf.Buffer)
        let strLen = int(buf.Length)
        var text = newString(strLen)
        system.copyMem(addr text[0], addr data[][0], strLen)
        state.alpns.add(text)
  let raw = storeHandle(state)
  configuration[] = raw
  QUIC_STATUS_SUCCESS

proc msquicConfigurationClose(configuration: HQUIC) {.cdecl.} =
  releaseHandle(configuration)

proc msquicConfigurationLoadCredential(configuration: HQUIC;
    credential: pointer): QUIC_STATUS {.cdecl.} =
  let state = toConfiguration(configuration)
  if state.isNil:
    return QUIC_STATUS_INVALID_PARAMETER
  if credential.isNil:
    return QUIC_STATUS_INVALID_PARAMETER
  state.credentialLoaded = true
  emitDiagnostics(DiagnosticsEvent(
    kind: diagConfigurationLoaded,
    handle: configuration,
    note: "credential loaded"))
  QUIC_STATUS_SUCCESS

proc msquicListenerOpen(registration: HQUIC; handler: pointer;
    context: pointer; listener: ptr HQUIC): QUIC_STATUS {.cdecl.} =
  warn "msquicListenerOpen entry"
  if listener.isNil:
    return QUIC_STATUS_INVALID_PARAMETER
  let reg = toRegistration(registration)
  if reg.isNil:
    return QUIC_STATUS_INVALID_PARAMETER
  let cb =
    if handler.isNil:
      cast[QuicListenerCallback](nil)
    else:
      cast[QuicListenerCallback](handler)
  let state = ListenerState(kind: qhkListener)
  state.registration = reg
  state.callback = cb
  state.callbackContext = context
  state.context = context
  state.started = false
  state.stopped = false
  state.acceptedConnections = initTable[string, ConnectionState]()
  listener[] = storeHandle(state)
  QUIC_STATUS_SUCCESS

proc msquicListenerClose(listener: HQUIC) {.cdecl.} =
  let state = toListener(listener)
  if not state.isNil:
    if not state.transport.isNil:
      asyncCheck state.transport.closeWait()
      state.transport = nil
    if not state.stopped:
      listenerEmitEvent(state, QUIC_LISTENER_EVENT_STOP_COMPLETE, proc (buf: ptr uint8) {.gcsafe.} =
        var payload = QuicListenerEventStopCompletePayload(
          Flags: 0'u8,
          Reserved: [uint8(0), 0, 0, 0, 0, 0, 0]
        )
        system.copyMem(buf, unsafeAddr payload, sizeof(payload))
      )
      state.stopped = true
    state.acceptedConnections.clear()
  releaseHandle(listener)

proc parsePacketNumber(pnSlice: array[4, byte]): uint32 =
  var tmp = pnSlice
  var value: uint32
  bigEndian32(addr value, addr tmp)
  value

proc newAcceptedConnection(state: ListenerState; remote, local: TransportAddress;
    clientCidSeq, serverCidSeq: seq[byte]): ConnectionState =
  let conn = ConnectionState(kind: qhkConnection)
  conn.registration = state.registration
  conn.callback = nil
  conn.callbackContext = nil
  conn.context = nil
  conn.started = true
  conn.serverName = ""
  conn.serverPort = 0
  conn.localCid = initConnectionId(serverCidSeq)
  conn.peerCid = initConnectionId(clientCidSeq)
  conn.quicConn = newConnection(crServer, conn.localCid, conn.peerCid, QuicVersion(1))
  conn.eventHandlers = @[]
  conn.datagramReceiveEnabled = false
  conn.datagramSendEnabled = false
  conn.streamSchedulingScheme = 0
  conn.settingsOverlay = defaultQuicSettingsOverlay()
  conn.congestionAlgorithm = caCubic
  conn.closeReason = ""
  conn.disable1RttEncryption = false
  conn.transport = state.transport
  conn.remoteAddress = remote
  conn.initialSecrets = tls.deriveInitialSecrets(serverCidSeq)
  conn.transcript = @[]
  conn.handshakeComplete = false
  conn.sessionResumed = false
  conn.resumptionTicket = @[]
  conn.resumptionData = @[]
  conn.incomingStreams = initTable[uint64, StreamState]()
  conn.ackTracker = qack.initAckTracker()
  conn.lossDetection = qloss.initLossDetectionModel()
  conn.congestionController = qcc.initCongestionController(caCubic, DefaultCongestionDatagramBytes)
  conn.sentPackets = @[]
  conn.pendingDatagrams = @[]
  conn.latestAckedPacket = 0'u64
  conn.localStreams = @[]
  conn.smoothedRttUs = 0'u64
  conn.minRttUs = 0'u64
  conn.latestRttUs = 0'u64
  conn.rttVarianceUs = 0'u64
  conn.pathRemoteAddrs = initTable[uint8, TransportAddress]()
  conn.pathRemoteKeys = initTable[uint8, string]()
  conn.remotePathIds = initTable[string, uint8]()
  conn.activeRemoteKey = remoteAddressKey(remote)
  conn.registerPathRemote(0'u8, remote)
  discard storeHandle(conn)
  conn.initLocalStreamIds()
  conn

proc ensureIncomingStream(conn: ConnectionState; streamId: uint64): StreamState =
  if conn.isNil:
    return nil
  if conn.incomingStreams.hasKey(streamId):
    return conn.incomingStreams[streamId]
  let stream = StreamState(kind: qhkStream)
  stream.connection = conn
  stream.callback = nil
  stream.callbackContext = nil
  stream.context = nil
  stream.eventHandlers = @[]
  stream.started = true
  stream.closed = false
  stream.sendBuffer = @[]
  stream.sentOffset = 0
  stream.finRequested = false
  stream.finSent = false
  stream.streamId = streamId
  stream.pendingChunks = @[]
  discard storeHandle(stream)
  conn.incomingStreams[streamId] = stream
  emitPeerStreamStarted(conn, stream)
  stream

proc findLocalStream(conn: ConnectionState; streamId: uint64): StreamState {.gcsafe.} =
  if conn.isNil:
    return nil
  for stream in conn.localStreams:
    if not stream.isNil and stream.streamId == streamId:
      return stream
  nil

proc findAnyStream(conn: ConnectionState; streamId: uint64): StreamState {.gcsafe.} =
  if conn.isNil:
    return nil
  let local = conn.findLocalStream(streamId)
  if not local.isNil:
    return local
  if conn.incomingStreams.hasKey(streamId):
    return conn.incomingStreams[streamId]
  nil

proc emitPeerSendAborted(state: StreamState; errorCode: uint64) {.gcsafe.} =
  if state.isNil:
    return
  if not state.callback.isNil:
    var native = QuicStreamEvent(Type: QUIC_STREAM_EVENT_PEER_SEND_ABORTED, Padding: 0)
    var payload = QuicStreamEventPeerAbortedPayload(
      ErrorCode: QUIC_UINT62(errorCode),
      Reserved: 0'u16
    )
    copyMem(addr native.Data[0], addr payload, sizeof(payload))
    {.cast(gcsafe).}:
      discard state.callback(cast[HQUIC](state), state.callbackContext, addr native)
  var ev = StreamEvent(kind: sePeerSendAborted, errorCode: errorCode)
  {.cast(gcsafe).}:
    emitStreamEvent(state, ev)

proc emitPeerReceiveAborted(state: StreamState; errorCode: uint64) {.gcsafe.} =
  if state.isNil:
    return
  if not state.callback.isNil:
    var native = QuicStreamEvent(Type: QUIC_STREAM_EVENT_PEER_RECEIVE_ABORTED, Padding: 0)
    var payload = QuicStreamEventPeerAbortedPayload(
      ErrorCode: QUIC_UINT62(errorCode),
      Reserved: 0'u16
    )
    copyMem(addr native.Data[0], addr payload, sizeof(payload))
    {.cast(gcsafe).}:
      discard state.callback(cast[HQUIC](state), state.callbackContext, addr native)
  var ev = StreamEvent(kind: sePeerReceiveAborted, errorCode: errorCode)
  {.cast(gcsafe).}:
    emitStreamEvent(state, ev)

proc handleOneRttPayload(conn: ConnectionState; remote: TransportAddress;
    packetNumber: uint64; plaintext: seq[byte]) =
  if conn.isNil:
    return
  conn.maybeInitiatePathMigration(remote)
  qack.trackIncomingPacket(conn.ackTracker, packetNumber)
  let nowUs = nowMicros()
  var pos = 0
  var ackEliciting = false
  while pos < plaintext.len:
    let frameType = plaintext[pos]
    if (frameType and 0xF8'u8) == 0x08'u8:
      ackEliciting = true
      let frame = proto.parseStreamFrame(plaintext, pos)
      let stream = ensureIncomingStream(conn, frame.streamId)
      if not stream.isNil:
        emitStreamReceive(stream, frame.offset, frame.data, frame.fin)
    elif frameType == 0x30'u8 or frameType == 0x31'u8:
      ackEliciting = true
      let frame = proto.parseDatagramFrame(plaintext, pos)
      if conn.datagramReceiveEnabled:
        var ev = ConnectionEvent(
          kind: ceDatagramReceived,
          datagramPayload: frame.data,
          datagramFlags: (if frame.containsLength: 1'u32 else: 0'u32),
          note: "datagram received"
        )
        emitConnectionEvent(conn, ev)
    elif frameType == 0x06'u8:
      ackEliciting = true
      inc pos
      discard proto.parseCryptoFrame(plaintext, pos)
    elif frameType == 0x04'u8:
      ackEliciting = true
      let frame = proto.parseResetStreamFrame(plaintext, pos)
      let stream = conn.findAnyStream(frame.streamId)
      if not stream.isNil:
        stream.closed = true
        stream.finRequested = true
        stream.finSent = true
        emitPeerSendAborted(stream, frame.applicationErrorCode)
    elif frameType == 0x05'u8:
      ackEliciting = true
      let frame = proto.parseStopSendingFrame(plaintext, pos)
      let stream = conn.findAnyStream(frame.streamId)
      if not stream.isNil:
        emitPeerReceiveAborted(stream, frame.applicationErrorCode)
    elif frameType == 0x1A'u8:
      ackEliciting = true
      let frame = proto.parsePathChallengeFrame(plaintext, pos)
      discard conn.sendOneRttPacket(
        proto.encodePathResponseFrame(frame.data),
        sfkPathResponse,
        true,
        targetRemote = unsafeAddr remote
      )
    elif frameType == 0x1B'u8:
      ackEliciting = true
      let frame = proto.parsePathResponseFrame(plaintext, pos)
      conn.applyPathResponse(frame.data)
    elif frameType == 0x02'u8 or frameType == 0x03'u8:
      let ack = proto.parseAckFrame(plaintext, pos)
      conn.applyAckFrame(ack, ceOneRtt)
    elif frameType == 0x01'u8 or frameType == 0x00'u8:
      if frameType == 0x01'u8:
        ackEliciting = true
      inc pos
    else:
      inc pos
  if ackEliciting:
    qack.markForAck(conn.ackTracker, packetNumber, nowUs, ackTypeAckImmediate)
    conn.sendAckFrame(packetNumber, unsafeAddr remote)

proc connectionMatchesDestCid(conn: ConnectionState; data: seq[byte]): bool {.gcsafe.} =
  if conn.isNil or data.len < 2:
    return false
  let cidLen = int(conn.localCid.length)
  if cidLen <= 0 or data.len < 1 + cidLen:
    return false
  let expected = cidBytes(conn.localCid)
  if expected.len != cidLen:
    return false
  for i in 0 ..< cidLen:
    if data[1 + i] != expected[i]:
      return false
  true

proc findAcceptedConnectionByDestCid(state: ListenerState;
    data: seq[byte]): ConnectionState {.gcsafe.} =
  if state.isNil:
    return nil
  for conn in state.acceptedConnections.values:
    if conn.connectionMatchesDestCid(data):
      return conn
  nil

proc handleShortHeaderPacket(conn: ConnectionState; remote: TransportAddress; data: seq[byte]) =
  if conn.isNil or not conn.handshakeComplete:
    return
  let cidLen = int(conn.localCid.length)
  let pnOffset = 1 + cidLen
  if data.len < pnOffset + 4 + 16:
    return

  var mutableData = data
  var pnSlice: array[4, byte]
  for i in 0 .. 3:
    pnSlice[i] = mutableData[pnOffset + i]

  let receiveMat = connectionReceiveMaterial(conn)
  if receiveMat.hp.len < 16 or receiveMat.key.len < 16 or receiveMat.iv.len < 12:
    return

  tls.removeHeaderProtection(
    receiveMat.hp,
    mutableData[pnOffset + 4 ..< pnOffset + 4 + 16],
    mutableData[0],
    pnSlice
  )
  for i in 0 .. 3:
    mutableData[pnOffset + i] = pnSlice[i]

  let pnVal = parsePacketNumber(pnSlice)
  let aad = mutableData[0 ..< pnOffset + 4]
  let ciphertext = mutableData[pnOffset + 4 ..< mutableData.len - 16]
  let recTag = mutableData[^16 .. ^1]
  let plaintext =
    if conn.disable1RttEncryption:
      ciphertext
    else:
      tls.decryptPacket(receiveMat.key, receiveMat.iv, uint64(pnVal), aad, ciphertext, recTag)
  if plaintext.len == 0 and ciphertext.len > 0:
    return
  handleOneRttPayload(conn, remote, uint64(pnVal), plaintext)

proc handleInitialPacket(state: ListenerState; remote, local: TransportAddress; data: seq[byte]) =
  let header = proto.parseUnprotectedHeader(data)
  if header.destConnectionId.len == 0 or header.srcConnectionId.len == 0:
    return
  let initialSecrets = tls.deriveInitialSecrets(header.destConnectionId)
  let pnOffset = header.payloadOffset
  if pnOffset + 4 + 16 > data.len:
    return

  var mutableData = data
  var pnSlice: array[4, byte]
  for i in 0 .. 3:
    pnSlice[i] = mutableData[pnOffset + i]

  tls.removeHeaderProtection(
    initialSecrets.clientHp,
    mutableData[pnOffset + 4 ..< pnOffset + 4 + 16],
    mutableData[0],
    pnSlice
  )
  for i in 0 .. 3:
    mutableData[pnOffset + i] = pnSlice[i]

  let pnVal = parsePacketNumber(pnSlice)
  let aad = mutableData[0 ..< pnOffset + 4]
  let ciphertext = mutableData[pnOffset + 4 ..< mutableData.len - 16]
  let recTag = mutableData[^16 .. ^1]
  let plaintext = tls.decryptPacket(
    initialSecrets.clientKey,
    initialSecrets.clientIv,
    uint64(pnVal),
    aad,
    ciphertext,
    recTag
  )
  if plaintext.len == 0:
    return

  var cryptoData: seq[byte] = @[]
  var pos = 0
  while pos < plaintext.len:
    let frameType = plaintext[pos]
    if frameType == 0x06'u8:
      inc pos
      let frame = proto.parseCryptoFrame(plaintext, pos)
      if frame.data.len > 0:
        cryptoData = frame.data
        break
    else:
      inc pos
  if cryptoData.len == 0:
    return

  let remoteKey = remoteAddressKey(remote)
  var conn = state.acceptedConnections.getOrDefault(remoteKey, nil)
  let isNewConnection = conn.isNil
  if isNewConnection:
    conn = newAcceptedConnection(state, remote, local, header.srcConnectionId, header.destConnectionId)
    conn.initialSecrets = initialSecrets
    state.acceptedConnections[remoteKey] = conn
    emitListenerNewConnection(state, cast[HQUIC](conn))

  if not conn.handshakeComplete:
    let clientKeyShare = tls.findClientKeyShare(cryptoData)
    if clientKeyShare.len != 32:
      return
    let serverKeyShare = tls.generateKeyShare()
    let serverHello = tls.encodeServerHello(serverKeyShare.publicKey.getBytes())
    conn.transcript = cryptoData & serverHello
    let sharedSecret = tls.computeSharedSecret(serverKeyShare.privateKey, clientKeyShare)
    let helloHash = tls.hashTranscript(conn.transcript)
    let handshakeSecrets = tls.deriveHandshakeSecrets(sharedSecret, helloHash)
    conn.oneRttKeys = tls.deriveApplicationSecrets(handshakeSecrets.clientSecret, helloHash)

    var cryptoFrame: seq[byte] = @[0x06'u8]
    proto.writeVarInt(cryptoFrame, 0'u64)
    proto.writeVarInt(cryptoFrame, uint64(serverHello.len))
    cryptoFrame.add(serverHello)

    let destCid = cidBytes(conn.peerCid)
    let srcCid = cidBytes(conn.localCid)
    var headerBuf: seq[byte] = @[0xC3'u8]
    headerBuf.writeUint32(1'u32)
    headerBuf.add(byte(destCid.len))
    headerBuf.add(destCid)
    headerBuf.add(byte(srcCid.len))
    headerBuf.add(srcCid)
    headerBuf.writeVarInt(0'u64)
    proto.writeVarInt(headerBuf, 4'u64 + uint64(cryptoFrame.len + 16))
    let responsePnOffset = headerBuf.len
    let responsePn = uint64(conn.quicConn.model.packetSpaces[ceInitial].nextPacketNumber())
    headerBuf.writeUint32(uint32(responsePn))

    var tag: array[16, byte]
    let encrypted = tls.encryptPacket(
      conn.initialSecrets.serverKey,
      conn.initialSecrets.serverIv,
      responsePn,
      headerBuf,
      cryptoFrame,
      tag
    )
    var packet = headerBuf & encrypted & @tag
    if packet.len >= responsePnOffset + 4 + 16:
      let sample = packet[responsePnOffset + 4 ..< responsePnOffset + 4 + 16]
      var responsePn: array[4, byte]
      for i in 0 .. 3:
        responsePn[i] = packet[responsePnOffset + i]
      tls.applyHeaderProtection(conn.initialSecrets.serverHp, sample, packet[0], responsePn)
      for i in 0 .. 3:
        packet[responsePnOffset + i] = responsePn[i]

    asyncCheck conn.transport.sendTo(conn.remoteAddress, packet)
    conn.handshakeComplete = true
    conn.markActivePathValidated(0)
    for stream in conn.localStreams:
      flushStream(stream)
    flushPendingDatagrams(conn)
    emitNativeConnected(conn)
    var ev = ConnectionEvent(kind: ceConnected, negotiatedAlpn: "", note: "server handshake complete")
    emitConnectionEvent(conn, ev)
    emitDiagnostics(DiagnosticsEvent(
      kind: diagConnectionEvent,
      handle: cast[pointer](conn),
      note: "ServerHello sent. 1-RTT enabled."))

proc listenerOnReceive(state: ListenerState; transp: DatagramTransport; remote: TransportAddress;
    local: TransportAddress; data: seq[byte]) {.async.} =
  if state.isNil:
    return
  try:
    let packet = proto.decodePacket(data)
    if packet.isLongHeader and packet.longHeader.packetType == ptInitial:
      {.cast(gcsafe).}:
        emitDiagnostics(DiagnosticsEvent(
          kind: diagRegistrationOpened,
          handle: cast[pointer](state),
          note: "RX Initial len=" & $data.len))
      {.cast(gcsafe).}:
        handleInitialPacket(state, remote, local, data)
    else:
      let remoteKey = remoteAddressKey(remote)
      var conn = state.acceptedConnections.getOrDefault(remoteKey, nil)
      if conn.isNil:
        conn = state.findAcceptedConnectionByDestCid(data)
        if not conn.isNil:
          state.acceptedConnections[remoteKey] = conn
      if not conn.isNil:
        {.cast(gcsafe).}:
          handleShortHeaderPacket(conn, remote, data)
  except Exception:
    discard

proc msquicListenerStart(listener: HQUIC; alpn: ptr QuicBuffer;
    alpnCount: uint32; address: pointer): QUIC_STATUS {.cdecl.} =
  let state = toListener(listener)
  if state.isNil:
    return QUIC_STATUS_INVALID_PARAMETER
  warn "msquicListenerStart entry", addressNil=address.isNil
  
  # Decode address (assume pointer to QuicAddr/SockAddr)
  # For now, we hardcode binding to 0.0.0.0:0 or a known port for testing if address is nil
  # In MsQuic, address is pointer to QUIC_ADDR
  echo "[api_impl] msquicListenerStart entry. Address nil? ", address.isNil
  var bindPort = 0
  if not address.isNil:
    # TODO: Proper sockaddr parsing. For now assuming IPv4 port at offset 2 (sin_port)
    # This is a HACK for Phase 1.
    let family = cast[ptr uint16](address)[]
    echo "[api_impl] Address family: ", family
    if family == 2: # AF_INET
      let portPtr = cast[ptr uint16](cast[uint](address) + 2)
      var netPort: uint16
      bigEndian16(addr netPort, portPtr)
      bindPort = int(netPort)
      echo "[api_impl] Parsed IPv4 port: ", bindPort
    else:
      warn "Unknown family, ignoring port"
  
  try:
    warn "Attempting to bind 0.0.0.0", port=bindPort
    state.transport = newDatagramTransport(
      (proc (transp: DatagramTransport, remote: TransportAddress) {.async.} =
        let data = transp.getMessage()
        asyncCheck listenerOnReceive(state, transp, remote, transp.localAddress, data)),
      local = initTAddress("0.0.0.0", Port(bindPort))
    )
    warn "Bind success!"
    state.started = true
    # Log successful binding
    emitDiagnostics(DiagnosticsEvent(
      kind: diagRegistrationOpened,
      handle: listener,
      note: "Bound to port " & $bindPort))
      
    QUIC_STATUS_SUCCESS
  except CatchableError as exc:
    echo "[api_impl] Bind FAILED: ", exc.msg
    state.transport = nil
    emitDiagnostics(DiagnosticsEvent(
      kind: diagRegistrationOpened,
      handle: listener,
      note: "Bind failed: " & exc.msg))
    QUIC_STATUS_INTERNAL_ERROR

proc msquicListenerStop(listener: HQUIC) {.cdecl.} =
  let state = toListener(listener)
  if state.isNil:
    return
  if not state.stopped:
    listenerEmitEvent(state, QUIC_LISTENER_EVENT_STOP_COMPLETE, proc (buf: ptr uint8) {.gcsafe.} =
      var payload = QuicListenerEventStopCompletePayload(
        Flags: 0'u8,
        Reserved: [uint8(0), 0, 0, 0, 0, 0, 0]
      )
      system.copyMem(buf, unsafeAddr payload, sizeof(payload))
    )
    state.stopped = true

proc msquicConnectionOpen(registration: HQUIC; handler: QuicConnectionCallback;
    context: pointer; connection: ptr HQUIC): QUIC_STATUS {.cdecl.} =
  if connection.isNil:
    return QUIC_STATUS_INVALID_PARAMETER
  let reg = toRegistration(registration)
  if reg.isNil:
    return QUIC_STATUS_INVALID_PARAMETER
  let state = ConnectionState(kind: qhkConnection)
  state.registration = reg
  state.callback = handler
  state.callbackContext = context
  state.context = context
  state.eventHandlers = @[]
  state.streamSchedulingScheme = 0
  state.settingsOverlay = defaultQuicSettingsOverlay()
  state.datagramReceiveEnabled = state.settingsOverlay.datagramReceiveEnabled
  state.datagramSendEnabled = false
  state.congestionAlgorithm = caCubic
  state.closeReason = ""
  state.disable1RttEncryption = false
  state.handshakeComplete = false
  state.sessionResumed = false
  state.resumptionTicket = @[]
  state.resumptionData = @[]
  state.incomingStreams = initTable[uint64, StreamState]()
  state.ackTracker = qack.initAckTracker()
  state.lossDetection = qloss.initLossDetectionModel()
  state.congestionController = qcc.initCongestionController(caCubic, DefaultCongestionDatagramBytes)
  state.sentPackets = @[]
  state.pendingDatagrams = @[]
  state.nextLocalBidiStreamId = 0'u64
  state.nextLocalUniStreamId = 2'u64
  state.latestAckedPacket = 0'u64
  state.localStreams = @[]
  state.smoothedRttUs = 0'u64
  state.minRttUs = 0'u64
  state.latestRttUs = 0'u64
  state.rttVarianceUs = 0'u64
  state.pathRemoteAddrs = initTable[uint8, TransportAddress]()
  state.pathRemoteKeys = initTable[uint8, string]()
  state.remotePathIds = initTable[string, uint8]()
  state.activeRemoteKey = ""
  let raw = storeHandle(state)
  connection[] = raw
  QUIC_STATUS_SUCCESS

proc msquicConnectionClose(connection: HQUIC) {.cdecl.} =
  let state = toConnection(connection)
  if not state.isNil:
    if not state.transport.isNil:
      asyncCheck state.transport.closeWait()
      state.transport = nil
    var ev = ConnectionEvent(kind: ceShutdownComplete, note: state.closeReason)
    emitConnectionEvent(state, ev)
    state.eventHandlers.setLen(0)
    emitDiagnostics(DiagnosticsEvent(
      kind: diagConnectionEvent,
      handle: connection,
      note: "shutdown complete"))
  releaseHandle(connection)

proc msquicConnectionShutdown(connection: HQUIC;
    flags: QUIC_CONNECTION_SHUTDOWN_FLAGS; errorCode: QUIC_UINT62) {.cdecl.} =
  let state = toConnection(connection)
  if state.isNil:
    return
  discard flags
  var ev = ConnectionEvent(kind: ceShutdownInitiated, errorCode: errorCode)
  emitConnectionEvent(state, ev)
  emitDiagnostics(DiagnosticsEvent(
    kind: diagConnectionEvent,
    handle: connection,
    note: "shutdown initiated error=" & $errorCode))
  state.started = false

proc msquicConnectionSetConfiguration(connection: HQUIC;
    configuration: HQUIC): QUIC_STATUS {.cdecl.} =
  let state = toConnection(connection)
  let config = toConfiguration(configuration)
  if state.isNil or config.isNil:
    return QUIC_STATUS_INVALID_PARAMETER
  state.configuration = config
  QUIC_STATUS_SUCCESS

proc msquicConnectionSendResumption(connection: HQUIC;
    flags: QUIC_SEND_RESUMPTION_FLAGS; dataLength: uint16;
    data: ptr uint8): QUIC_STATUS {.cdecl.} =
  let state = toConnection(connection)
  if state.isNil:
    return QUIC_STATUS_INVALID_PARAMETER
  discard flags
  if dataLength > 0'u16 and data.isNil:
    return QUIC_STATUS_INVALID_PARAMETER
  var customData: seq[byte] = @[]
  if dataLength > 0'u16:
    customData = newSeq[byte](int(dataLength))
    copyMem(addr customData[0], data, int(dataLength))
  let ticket = buildBuiltinResumptionTicket(state, customData)
  if ticket.len == 0:
    return QUIC_STATUS_INVALID_STATE
  state.resumptionTicket = ticket
  state.resumptionData = customData
  storeBuiltinResumptionEntry(state, ticket, customData)
  QUIC_STATUS_SUCCESS

proc attachConfiguration(state: ConnectionState; configuration: ConfigurationState): QUIC_STATUS =
  if configuration.isNil:
    return QUIC_STATUS_INVALID_PARAMETER
  state.configuration = configuration
  QUIC_STATUS_SUCCESS

proc msquicConnectionStart(connection: HQUIC; configuration: HQUIC;
    family: QUIC_ADDRESS_FAMILY; serverName: cstring;
    serverPort: uint16): QUIC_STATUS {.cdecl.} =
  let state = toConnection(connection)
  if state.isNil:
    return QUIC_STATUS_INVALID_PARAMETER
  if state.configuration.isNil and not configuration.isNil:
    let cfg = toConfiguration(configuration)
    if cfg.isNil:
      return QUIC_STATUS_INVALID_PARAMETER
    state.configuration = cfg
  if state.configuration.isNil:
    return QUIC_STATUS_INVALID_STATE
  if not state.configuration.credentialLoaded:
    return QUIC_STATUS_INVALID_STATE
  if state.started:
    return QUIC_STATUS_INVALID_STATE
  let (clientCid, serverCid) = generateConnectionIds()
  state.localCid = clientCid
  state.peerCid = serverCid
  state.serverName = if serverName.isNil: "" else: $serverName
  state.serverPort = serverPort
  let cachedResumption = loadBuiltinResumptionEntry(
    state.serverName,
    state.serverPort,
    alpnForConnection(state)
  )
  state.sessionResumed = cachedResumption.ticket.len > 0
  state.resumptionTicket = cachedResumption.ticket
  state.resumptionData = cachedResumption.data
  let version = QuicVersion(1)
  state.quicConn = newConnection(crClient, clientCid, serverCid, version)
  state.initLocalStreamIds()
  discard family
  state.started = true
  state.datagramReceiveEnabled = state.settingsOverlay.datagramReceiveEnabled
  
  # Initialize Transport (Client)
  var transportEnabled = false
  try:
    # Resolve remote address
    # For Phase 1, we assume serverName is an IP string. DNS resolution is skipped for now/handled by caller if needed
    try:
      state.remoteAddress = initTAddress(state.serverName, Port(state.serverPort))
      state.activeRemoteKey = remoteAddressKey(state.remoteAddress)
      state.registerPathRemote(0'u8, state.remoteAddress)
      state.transport = newDatagramTransport(
        (proc (transp: DatagramTransport, remote: TransportAddress) {.async.} =
          let data = transp.getMessage()
          if data.len == 0:
            return

          try:
            var header = proto.parseUnprotectedHeader(data)
            if (header.firstByte and 0x80) != 0:
              let pType = (header.firstByte and 0x30) shr 4
              if pType == 0:
                var mutableData = data
                let pnOffset = header.payloadOffset
                if pnOffset + 4 + 16 > mutableData.len:
                  return
                var pnSlice: array[4, byte]
                for i in 0 .. 3:
                  pnSlice[i] = mutableData[pnOffset + i]

                tls.removeHeaderProtection(
                  state.initialSecrets.serverHp,
                  mutableData[pnOffset + 4 ..< pnOffset + 4 + 16],
                  mutableData[0],
                  pnSlice
                )
                for i in 0 .. 3:
                  mutableData[pnOffset + i] = pnSlice[i]

                let pnVal = parsePacketNumber(pnSlice)
                let aadLen = pnOffset + 4
                let aad = mutableData[0 ..< aadLen]
                let ciphertext = mutableData[aadLen ..< mutableData.len - 16]
                let recTag = mutableData[^16 .. ^1]

                let plaintext = tls.decryptPacket(
                  state.initialSecrets.serverKey,
                  state.initialSecrets.serverIv,
                  uint64(pnVal),
                  aad,
                  ciphertext,
                  recTag
                )

                if plaintext.len > 0:
                  {.cast(gcsafe).}:
                    emitDiagnostics(DiagnosticsEvent(
                      kind: diagConnectionEvent,
                      handle: cast[HQUIC](state),
                      note: "RX Initial Valid"
                    ))
                  var pos = 0
                  while pos < plaintext.len:
                    let fType = plaintext[pos]
                    if fType == 0x06:
                      inc pos
                      let crypto = proto.parseCryptoFrame(plaintext, pos)
                      if not state.handshakeComplete:
                        let serverHelloKey = tls.findServerKeyShare(crypto.data)
                        if serverHelloKey.len == 32:
                          {.cast(gcsafe).}:
                            emitDiagnostics(DiagnosticsEvent(
                              kind: diagConnectionEvent,
                              handle: cast[HQUIC](state),
                              note: "RX ServerHello KeyShare"
                            ))

                          state.transcript.add(crypto.data)
                          let sharedSecret = tls.computeSharedSecret(state.clientPrivateKey, serverHelloKey)
                          let helloHash = tls.hashTranscript(state.transcript)
                          let handshakeSecrets = tls.deriveHandshakeSecrets(sharedSecret, helloHash)
                          let trafficSecrets =
                            tls.deriveApplicationSecrets(handshakeSecrets.clientSecret, helloHash)

                          state.oneRttKeys = trafficSecrets
                          state.handshakeComplete = true
                          state.markActivePathValidated(0)
                          for stream in state.localStreams:
                            flushStream(stream)
                          flushPendingDatagrams(state)
                          let negotiatedAlpn =
                            if state.configuration.isNil or state.configuration.alpns.len == 0: ""
                            else: state.configuration.alpns[0]
                          {.cast(gcsafe).}:
                            emitNativeConnected(state)
                            var connectedEvent = ConnectionEvent(
                              kind: ceConnected,
                              sessionResumed: state.sessionResumed,
                              negotiatedAlpn: negotiatedAlpn,
                              note: "client handshake complete")
                            emitConnectionEvent(state, connectedEvent)
                          {.cast(gcsafe).}:
                            if state.resumptionTicket.len == 0:
                              state.resumptionTicket = buildBuiltinResumptionTicket(state)
                            storeBuiltinResumptionEntry(state, state.resumptionTicket, state.resumptionData)
                          {.cast(gcsafe).}:
                            emitDiagnostics(DiagnosticsEvent(
                              kind: diagConnectionEvent,
                              handle: cast[HQUIC](state),
                              note: "Handshake Keys Derived. 1-RTT Enabled."
                            ))
                    elif fType == 0x02:
                      break
                    else:
                      inc pos
            else:
              {.cast(gcsafe).}:
                handleShortHeaderPacket(state, remote, data)
          except Exception:
            discard
        ),
        local = initTAddress("0.0.0.0", Port(0))
      )
      transportEnabled = true
    except CatchableError:
      state.remoteAddress = TransportAddress()
      state.transport = nil

    # Phase 3: Send Client Initial (ClientHello)
    let destCid = @(state.peerCid.bytes)[0 ..< int(state.peerCid.length)] 
    let srcCid = @(state.localCid.bytes)[0 ..< int(state.localCid.length)]
    state.initialSecrets = tls.deriveInitialSecrets(destCid)
    
    let keyShare = tls.generateKeyShare()
    state.clientPrivateKey = keyShare.privateKey
    let clientHello = tls.encodeClientHello(destCid, keyShare)
    state.transcript = clientHello # Store
    
    # CRYPTO Frame
    var cryptoFrame: seq[byte] = @[]
    cryptoFrame.add(0x06'u8)
    proto.writeVarInt(cryptoFrame, 0'u64)
    proto.writeVarInt(cryptoFrame, uint64(clientHello.len))
    cryptoFrame.add(clientHello)
    
    # Padding
    var payload = cryptoFrame
    let minSize = 1200
    # Header estimate: 1 + 4 + 1+Dst + 1+Src + 1(TokenLen) + 2(Len) + 4(PN) = ~22 + CIDs
    let headerEst = 22 + destCid.len + srcCid.len
    let padLen = minSize - headerEst - payload.len - 16 # Tag
    if padLen > 0:
      for i in 0 ..< padLen: payload.add(0x00'u8)
      
    # Construct Header (Unprotected)
    var headerBuf: seq[byte] = @[]
    let firstByte = 0xC3'u8 # Initial
    headerBuf.add(firstByte)
    headerBuf.writeUint32(1'u32)
    headerBuf.add(byte(destCid.len)); headerBuf.add(destCid)
    headerBuf.add(byte(srcCid.len)); headerBuf.add(srcCid)
    headerBuf.writeVarInt(uint64(0)) # Token Len 0
    
    let totalPayloadLen = payload.len + 16 # + Tag
    var lenField: seq[byte] = @[]
    proto.writeVarInt(lenField, 4 + uint64(totalPayloadLen)) # PN Len(4) + Body
    headerBuf.add(lenField)
    
    let pnOffset = headerBuf.len
    let initialPn = uint64(state.quicConn.model.packetSpaces[ceInitial].nextPacketNumber())
    headerBuf.writeUint32(uint32(initialPn))
    
    # Encrypt
    var tag: array[16, byte]
    let ciphertext = tls.encryptPacket(state.initialSecrets.clientKey, state.initialSecrets.clientIv, 
                                       initialPn, headerBuf, payload, tag)
    
    var packet = headerBuf & ciphertext & @tag
    
    # Header Protection
    if packet.len >= pnOffset + 4 + 16:
      let sample = packet[pnOffset + 4 ..< pnOffset + 4 + 16]
      var pnSlice: array[4, byte]
      for i in 0..3: pnSlice[i] = packet[pnOffset+i]
      tls.applyHeaderProtection(state.initialSecrets.clientHp, sample, packet[0], pnSlice)
      for i in 0..3: packet[pnOffset+i] = pnSlice[i]
      
    if transportEnabled and not state.transport.isNil:
      asyncCheck state.transport.sendTo(state.remoteAddress, packet)
      emitDiagnostics(DiagnosticsEvent(kind: diagConnectionEvent, handle: connection, note: "TX Initial Encrypted"))
    else:
      emitDiagnostics(DiagnosticsEvent(
        kind: diagConnectionEvent,
        handle: connection,
        note: "TX Initial Skipped (non-IP endpoint)"
      ))

  except CatchableError as exc:
    state.transport = nil
    emitDiagnostics(DiagnosticsEvent(
       kind: diagConnectionStarted,
       handle: connection,
       note: "Transport init failed: " & exc.msg))
    return QUIC_STATUS_INTERNAL_ERROR

  emitDiagnostics(DiagnosticsEvent(
    kind: diagConnectionStarted,
    handle: connection,
    note: state.serverName & ":" & $state.serverPort))
  QUIC_STATUS_SUCCESS

proc msquicStreamOpen(connection: HQUIC; flags: QUIC_STREAM_OPEN_FLAGS;
    handler: QuicStreamCallback; context: pointer; stream: ptr HQUIC): QUIC_STATUS {.cdecl.} =
  if stream.isNil:
    return QUIC_STATUS_INVALID_PARAMETER
  let connState = toConnection(connection)
  if connState.isNil:
    return QUIC_STATUS_INVALID_PARAMETER
  let state = StreamState(kind: qhkStream)
  state.callback = handler
  state.callbackContext = context
  state.context = context
  state.eventHandlers = @[]
  state.started = false
  state.closed = false
  state.connection = connState # Set parent connection
  state.streamId = connState.allocLocalStreamId((flags and StreamOpenFlagUnidirectional) != 0)
  state.pendingChunks = @[]
  stream[] = storeHandle(state)
  connState.localStreams.add(state)
  streamEmitEvent(state, QUIC_STREAM_EVENT_PEER_ACCEPTED)
  QUIC_STATUS_SUCCESS

proc msquicStreamClose(stream: HQUIC) {.cdecl.} =
  let state = toStream(stream)
  if not state.isNil and not state.closed:
    streamEmitEvent(state, QUIC_STREAM_EVENT_SHUTDOWN_COMPLETE, proc (buf: ptr uint8) {.gcsafe.} =
      var payload = QuicStreamEventShutdownCompletePayload(
        ConnectionShutdown: BOOLEAN(0),
        Flags: 0'u8,
        Reserved: [uint8(0),0,0,0,0,0],
        ConnectionErrorCode: QUIC_UINT62(0),
        ConnectionCloseStatus: QUIC_STATUS_SUCCESS
      )
      system.copyMem(buf, unsafeAddr payload, sizeof(payload))
    )
    state.closed = true
  releaseHandle(stream)

proc msquicStreamStart(stream: HQUIC;
    flags: QUIC_STREAM_START_FLAGS): QUIC_STATUS {.cdecl.} =
  let state = toStream(stream)
  if state.isNil:
    return QUIC_STATUS_INVALID_PARAMETER
  discard flags
  if state.started:
    return QUIC_STATUS_SUCCESS
  state.started = true
  streamEmitEvent(state, QUIC_STREAM_EVENT_START_COMPLETE, proc (buf: ptr uint8) {.gcsafe.} =
    var payload = QuicStreamEventStartCompletePayload(
      Status: QUIC_STATUS_SUCCESS,
      Id: QUIC_UINT62(state.streamId),
      Flags: 0'u8,
      Reserved: [uint8(0),0,0,0,0,0,0]
    )
    system.copyMem(buf, unsafeAddr payload, sizeof(payload))
  )
  QUIC_STATUS_SUCCESS

proc msquicStreamShutdown(stream: HQUIC;
    flags: QUIC_STREAM_SHUTDOWN_FLAGS; errorCode: QUIC_UINT62): QUIC_STATUS {.cdecl.} =
  let state = toStream(stream)
  if state.isNil:
    return QUIC_STATUS_INVALID_PARAMETER
  let graceful = (flags and QUIC_STREAM_SHUTDOWN_FLAG_GRACEFUL) != 0
  let abortSend = (flags and QUIC_STREAM_SHUTDOWN_FLAG_ABORT_SEND) != 0
  let abortReceive = (flags and QUIC_STREAM_SHUTDOWN_FLAG_ABORT_RECEIVE) != 0
  if graceful:
    state.finRequested = true
    flushStream(state)
  if abortSend and not state.connection.isNil and state.connection.handshakeComplete:
    let finalSize =
      if state.finSent or state.finRequested: state.sentOffset
      else: state.sentOffset
    discard state.connection.sendOneRttPacket(
      proto.encodeResetStreamFrame(state.streamId, uint64(errorCode), finalSize),
      sfkStream,
      true,
      stream = state,
      streamOffset = finalSize,
      streamPayload = @[],
      streamFin = true
    )
  if abortReceive and not state.connection.isNil and state.connection.handshakeComplete:
    discard state.connection.sendOneRttPacket(
      proto.encodeStopSendingFrame(state.streamId, uint64(errorCode)),
      sfkStream,
      true,
      stream = state,
      streamOffset = state.sentOffset,
      streamPayload = @[],
      streamFin = state.finSent
    )
  streamEmitEvent(state, QUIC_STREAM_EVENT_SEND_SHUTDOWN_COMPLETE, proc (buf: ptr uint8) {.gcsafe.} =
    var payload = QuicStreamEventSendShutdownCompletePayload(
      Graceful: BOOLEAN(if graceful and not abortSend: 1 else: 0),
      Reserved: [uint8(0),0,0,0,0,0,0]
    )
    system.copyMem(buf, unsafeAddr payload, sizeof(payload))
  )
  if abortSend or abortReceive:
    state.closed = true
  if graceful or abortSend or abortReceive:
    streamEmitEvent(state, QUIC_STREAM_EVENT_SHUTDOWN_COMPLETE, proc (buf: ptr uint8) {.gcsafe.} =
      var payload = QuicStreamEventShutdownCompletePayload(
        ConnectionShutdown: BOOLEAN(0),
        Flags: 0'u8,
        Reserved: [uint8(0),0,0,0,0,0],
        ConnectionErrorCode: QUIC_UINT62(0),
        ConnectionCloseStatus: QUIC_STATUS_SUCCESS
      )
      system.copyMem(buf, unsafeAddr payload, sizeof(payload))
    )
  QUIC_STATUS_SUCCESS

proc flushStream(state: StreamState) {.gcsafe.} =
  if state.isNil or state.connection.isNil:
    return
  let conn = state.connection

  if state.pendingChunks.len == 0:
    return

  let chunk = state.pendingChunks[0]
  if not conn.handshakeComplete:
    return
  let streamFrame = proto.encodeStreamFrame(
    state.streamId,
    chunk.payload,
    chunk.offset,
    chunk.fin
  )
  let pn = conn.sendOneRttPacket(
    streamFrame,
    sfkStream,
    true,
    stream = state,
    streamOffset = chunk.offset,
    streamPayload = chunk.payload,
    streamFin = chunk.fin,
    clientContext = chunk.clientContext
  )
  if pn == 0'u64:
    return
  state.pendingChunks.delete(0)
  if chunk.fin:
    state.finSent = true

proc msquicStreamSend(stream: HQUIC; buffers: ptr QuicBuffer;
    bufferCount: uint32; flags: QUIC_SEND_FLAGS;
    clientContext: pointer): QUIC_STATUS {.cdecl.} =
  let state = toStream(stream)
  if state.isNil:
    return QUIC_STATUS_INVALID_PARAMETER
  if (flags and SendFlagAllowZeroRtt) != 0 and not state.connection.handshakeComplete and
      not state.connection.sessionResumed and state.connection.resumptionTicket.len == 0:
    return QUIC_STATUS_NOT_SUPPORTED

  var payload: seq[byte] = @[]
  if bufferCount > 0 and not buffers.isNil:
    let bufArray = cast[ptr UncheckedArray[QuicBuffer]](buffers)
    for i in 0 ..< bufferCount:
      let qb = bufArray[i]
      if qb.Length > 0 and not qb.Buffer.isNil:
        let src = qb.Buffer
        let len = int(qb.Length)
        let start = payload.len
        payload.setLen(start + len)
        copyMem(addr payload[start], src, len)

  let fin = (flags and SendFlagFin) != 0
  state.pendingChunks.add(PendingStreamChunk(
    offset: state.sentOffset,
    payload: payload,
    fin: fin,
    clientContext: clientContext
  ))
  state.sentOffset += uint64(payload.len)
  if fin:
    state.finRequested = true

  flushStream(state)

  streamEmitEvent(state, QUIC_STREAM_EVENT_SEND_COMPLETE, proc (buf: ptr uint8) {.gcsafe.} =
    var payload = QuicStreamEventSendCompletePayload(
      Canceled: BOOLEAN(0),
      Reserved: [uint8(0),0,0,0,0,0,0],
      ClientContext: clientContext
    )
    system.copyMem(buf, unsafeAddr payload, sizeof(payload))
  )
  QUIC_STATUS_SUCCESS

proc msquicStreamReceiveComplete(stream: HQUIC;
    bufferLength: uint64) {.cdecl.} =
  discard stream
  discard bufferLength

proc msquicStreamReceiveSetEnabled(stream: HQUIC;
    enabled: BOOLEAN): QUIC_STATUS {.cdecl.} =
  let state = toStream(stream)
  if state.isNil:
    return QUIC_STATUS_INVALID_PARAMETER
  discard enabled
  QUIC_STATUS_SUCCESS

proc msquicDatagramSend(connection: HQUIC; buffers: ptr QuicBuffer;
    bufferCount: uint32; flags: QUIC_SEND_FLAGS;
    clientContext: pointer): QUIC_STATUS {.cdecl.} =
  let state = toConnection(connection)
  if state.isNil:
    return QUIC_STATUS_INVALID_PARAMETER
  if not state.datagramSendEnabled:
    return QUIC_STATUS_INVALID_STATE
  var payload: seq[byte] = @[]
  if bufferCount > 0 and not buffers.isNil:
    let bufArray = cast[ptr UncheckedArray[QuicBuffer]](buffers)
    for i in 0 ..< bufferCount:
      let qb = bufArray[i]
      if qb.Length == 0 or qb.Buffer.isNil:
        continue
      let start = payload.len
      let len = int(qb.Length)
      payload.setLen(start + len)
      copyMem(addr payload[start], qb.Buffer, len)
  if payload.len == 0:
    return QUIC_STATUS_SUCCESS
  if not state.handshakeComplete:
    if (flags and SendFlagAllowZeroRtt) == 0 or
        (not state.sessionResumed and state.resumptionTicket.len == 0):
      return QUIC_STATUS_NOT_SUPPORTED
    state.pendingDatagrams.add(PendingDatagram(
      payload: payload,
      clientContext: clientContext
    ))
    return QUIC_STATUS_SUCCESS
  let frame = proto.encodeDatagramFrame(payload, true)
  let pn = state.sendOneRttPacket(
    frame,
    sfkDatagram,
    true,
    clientContext = clientContext
  )
  if pn == 0'u64:
    return QUIC_STATUS_INVALID_STATE
  var nimEvent = ConnectionEvent(
    kind: ceParameterUpdated,
    paramId: QUIC_PARAM_CONN_DATAGRAM_SEND_ENABLED,
    note: "datagram send pn=" & $pn,
    userContext: state.context
  )
  emitConnectionEvent(state, nimEvent)
  QUIC_STATUS_SUCCESS

proc msquicConnectionResumptionComplete(connection: HQUIC;
    completionResult: BOOLEAN): QUIC_STATUS {.cdecl.} =
  discard connection
  discard completionResult
  QUIC_STATUS_SUCCESS

proc msquicConnectionCertificateComplete(connection: HQUIC;
    completionResult: BOOLEAN; tlsAlert: QUIC_TLS_ALERT_CODES): QUIC_STATUS {.cdecl.} =
  discard connection
  discard completionResult
  discard tlsAlert
  QUIC_STATUS_SUCCESS

proc msquicConnectionOpenInPartition(registration: HQUIC; partition: uint16;
    handler: QuicConnectionCallback; context: pointer;
    connection: ptr HQUIC): QUIC_STATUS {.cdecl.} =
  discard partition
  msquicConnectionOpen(registration, handler, context, connection)

proc msquicStreamProvideReceiveBuffers(stream: HQUIC; bufferCount: uint32;
    buffers: ptr QuicBuffer): QUIC_STATUS {.cdecl.} =
  discard stream
  discard bufferCount
  discard buffers
  QUIC_STATUS_NOT_SUPPORTED

proc registerConnectionEventHandler*(connection: HQUIC; handler: ConnectionEventHandler) {.exportc.} =
  let state = toConnection(connection)
  if state.isNil or handler.isNil:
    return
  if state.eventHandlers.len == 0:
    state.eventHandlers = @[]
  state.eventHandlers.add(handler)

proc registerStreamEventHandler*(stream: HQUIC; handler: StreamEventHandler) {.exportc.} =
  let state = toStream(stream)
  if state.isNil or handler.isNil:
    return
  if state.eventHandlers.len == 0:
    state.eventHandlers = @[]
  state.eventHandlers.add(handler)

proc clearStreamEventHandlers*(stream: HQUIC) {.exportc.} =
  let state = toStream(stream)
  if state.isNil:
    return
  state.eventHandlers.setLen(0)

proc clearConnectionEventHandlers*(connection: HQUIC) {.exportc.} =
  let state = toConnection(connection)
  if state.isNil:
    return
  state.eventHandlers.setLen(0)

proc applySettingsOverlay*(connection: HQUIC; overlay: QuicSettingsOverlay): QUIC_STATUS {.exportc.} =
  var temp = overlay
  msquicSetParam(connection, QUIC_PARAM_CONN_SETTINGS, sizeof(temp).uint32, addr temp)

proc getConnectionSettingsOverlay*(connection: HQUIC; overlay: var QuicSettingsOverlay): bool {.exportc.} =
  let state = toConnection(connection)
  if state.isNil:
    return false
  overlay = state.settingsOverlay
  true

proc getConnectionDatagramState*(connection: HQUIC; receiveEnabled: var bool; sendEnabled: var bool): bool {.exportc.} =
  let state = toConnection(connection)
  if state.isNil:
    return false
  receiveEnabled = state.datagramReceiveEnabled
  sendEnabled = state.datagramSendEnabled
  true

proc getConnectionCongestionWindow*(connection: HQUIC; windowBytes: var uint64): bool {.exportc.} =
  let state = toConnection(connection)
  if state.isNil:
    return false
  windowBytes = qcc.congestionWindowBytes(state.congestionController)
  true

proc getConnectionRttStats*(connection: HQUIC; latestRttUs: var uint64;
    smoothedRttUs: var uint64; minRttUs: var uint64;
    rttVarianceUs: var uint64): bool {.exportc.} =
  let state = toConnection(connection)
  if state.isNil:
    return false
  latestRttUs = state.latestRttUs
  smoothedRttUs = state.smoothedRttUs
  minRttUs = state.minRttUs
  rttVarianceUs = state.rttVarianceUs
  true

proc getConnectionProbeCount*(connection: HQUIC; probeCount: var uint16): bool {.exportc.} =
  let state = toConnection(connection)
  if state.isNil:
    return false
  probeCount = state.lossDetection.probeCount
  true

proc expireBuiltinResumptionEntryForTest*(serverName: cstring; serverPort: uint16;
    alpn: cstring): bool {.exportc.} =
  let serverNameText = if serverName.isNil: "" else: $serverName
  let alpnText = if alpn.isNil: "" else: $alpn
  let key = resumptionCacheKey(serverNameText, serverPort, alpnText)
  if key.len == 0 or not gBuiltinResumptionCache.hasKey(key):
    return false
  var entry = gBuiltinResumptionCache[key]
  entry.expiresAtUs = 1'u64
  gBuiltinResumptionCache[key] = entry
  true

proc runConnectionLossRecoveryTick*(connection: HQUIC): bool {.exportc.} =
  let state = toConnection(connection)
  if state.isNil:
    return false
  state.detectTimedOutLosses()
  true

proc getConnectionActivePathId*(connection: HQUIC; pathId: var uint8): bool {.exportc.} =
  let state = toConnection(connection)
  if state.isNil or state.quicConn.isNil:
    return false
  pathId = state.quicConn.model.migration.activePathId
  true

proc getConnectionKnownPathCount*(connection: HQUIC; pathCount: var uint8): bool {.exportc.} =
  let state = toConnection(connection)
  if state.isNil or state.quicConn.isNil:
    return false
  pathCount = uint8(min(state.quicConn.model.paths.len, high(uint8).int))
  true

proc getConnectionPathState*(connection: HQUIC; pathId: uint8;
    isActive: var bool; isValidated: var bool;
    challengeOutstanding: var bool; responsePending: var bool): bool {.exportc.} =
  let state = toConnection(connection)
  if state.isNil or state.quicConn.isNil:
    return false
  for path in state.quicConn.model.paths:
    if path.pathId == pathId:
      isActive = path.isActive
      isValidated = path.isValidated
      challengeOutstanding = path.challengeOutstanding
      responsePending = path.responsePending
      return true
  false

proc triggerConnectionMigrationProbe*(connection: HQUIC; host: cstring;
    port: uint16; pathId: var uint8): bool {.exportc.} =
  let state = toConnection(connection)
  if state.isNil or state.quicConn.isNil or host.isNil:
    return false
  let remote = initTAddress($host, Port(port))
  state.maybeInitiatePathMigration(remote)
  let remoteKey = remoteAddressKey(remote)
  if remoteKey.len == 0 or not state.remotePathIds.hasKey(remoteKey):
    return false
  pathId = state.remotePathIds[remoteKey]
  true

proc confirmConnectionValidatedPath*(connection: HQUIC; pathId: uint8): bool {.exportc.} =
  let state = toConnection(connection)
  if state.isNil or state.quicConn.isNil:
    return false
  if not state.pathRemoteAddrs.hasKey(pathId):
    return false
  state.activateValidatedPath(pathId)
  true

proc MsQuicSetContextShim*(handle: HQUIC; context: pointer) {.exportc, cdecl, quicApiHot.} =
  msquicSetContext(handle, context)

proc MsQuicGetContextShim*(handle: HQUIC): pointer {.exportc, cdecl, quicApiHot.} =
  msquicGetContext(handle)

proc MsQuicEnableDatagramReceiveShim*(connection: HQUIC; enable: BOOLEAN): QUIC_STATUS {.exportc, cdecl.} =
  var value = enable
  result = msquicSetParam(connection, QUIC_PARAM_CONN_DATAGRAM_RECEIVE_ENABLED, 1'u32, addr value)

proc MsQuicEnableDatagramSendShim*(connection: HQUIC; enable: BOOLEAN): QUIC_STATUS {.exportc, cdecl.} =
  var value = enable
  result = msquicSetParam(connection, QUIC_PARAM_CONN_DATAGRAM_SEND_ENABLED, 1'u32, addr value)

proc MsQuicGetDatagramReceiveShim*(connection: HQUIC; enable: ptr BOOLEAN): QUIC_STATUS {.exportc, cdecl.} =
  if enable.isNil:
    return QUIC_STATUS_INVALID_PARAMETER
  var length = 1'u32
  let status = msquicGetParam(connection, QUIC_PARAM_CONN_DATAGRAM_RECEIVE_ENABLED, addr length, enable)
  if status != QUIC_STATUS_SUCCESS:
    return status
  if length != 1'u32:
    return QUIC_STATUS_INVALID_STATE
  QUIC_STATUS_SUCCESS

proc MsQuicGetDatagramSendShim*(connection: HQUIC; enable: ptr BOOLEAN): QUIC_STATUS {.exportc, cdecl.} =
  if enable.isNil:
    return QUIC_STATUS_INVALID_PARAMETER
  var length = 1'u32
  let status = msquicGetParam(connection, QUIC_PARAM_CONN_DATAGRAM_SEND_ENABLED, addr length, enable)
  if status != QUIC_STATUS_SUCCESS:
    return status
  if length != 1'u32:
    return QUIC_STATUS_INVALID_STATE
  QUIC_STATUS_SUCCESS

proc initApiTable() =
  gApiTableInstance = QuicApiTable(
    SetContext: msquicSetContext,
    GetContext: msquicGetContext,
    SetCallbackHandler: msquicSetCallbackHandler,
    SetParam: msquicSetParam,
    GetParam: msquicGetParam,
    RegistrationOpen: msquicRegistrationOpen,
    RegistrationClose: msquicRegistrationClose,
    RegistrationShutdown: msquicRegistrationShutdown,
    ConfigurationOpen: msquicConfigurationOpen,
    ConfigurationClose: msquicConfigurationClose,
    ConfigurationLoadCredential: msquicConfigurationLoadCredential,
    ListenerOpen: msquicListenerOpen,
    ListenerClose: msquicListenerClose,
    ListenerStart: msquicListenerStart,
    ListenerStop: msquicListenerStop,
    ConnectionOpen: msquicConnectionOpen,
    ConnectionClose: msquicConnectionClose,
    ConnectionShutdown: msquicConnectionShutdown,
    ConnectionStart: msquicConnectionStart,
    ConnectionSetConfiguration: msquicConnectionSetConfiguration,
    ConnectionSendResumptionTicket: msquicConnectionSendResumption,
    StreamOpen: msquicStreamOpen,
    StreamClose: msquicStreamClose,
    StreamStart: msquicStreamStart,
    StreamShutdown: msquicStreamShutdown,
    StreamSend: msquicStreamSend,
    StreamReceiveComplete: msquicStreamReceiveComplete,
    StreamReceiveSetEnabled: msquicStreamReceiveSetEnabled,
    DatagramSend: msquicDatagramSend,
    ConnectionResumptionTicketValidationComplete: msquicConnectionResumptionComplete,
    ConnectionCertificateValidationComplete: msquicConnectionCertificateComplete,
    ConnectionOpenInPartition: msquicConnectionOpenInPartition,
    StreamProvideReceiveBuffers: msquicStreamProvideReceiveBuffers
  )

proc MsQuicOpenVersion*(version: uint32; apiTable: ptr pointer): QUIC_STATUS {.exportc, cdecl.} =
  warn "MsQuicOpenVersion entry", version=version
  if apiTable.isNil:
    warn "MsQuicOpenVersion invalid param"
    return QUIC_STATUS_INVALID_PARAMETER
  if version < 2'u32:
    warn "MsQuicOpenVersion unsupported version", version=version
    return QUIC_STATUS_NOT_SUPPORTED
  if gApiTableInstance.SetContext.isNil:
    warn "MsQuicOpenVersion initializing api table"
    try:
      initApiTable()
    except Exception as exc:
      warn "MsQuicOpenVersion initApiTable raised", msg=exc.msg
      return QUIC_STATUS_INVALID_STATE
  inc gApiTableRefCount
  apiTable[] = cast[pointer](addr gApiTableInstance)
  warn "MsQuicOpenVersion success"
  QUIC_STATUS_SUCCESS

proc MsQuicClose*(table: pointer) {.exportc, cdecl.} =
  discard table
  if gApiTableRefCount > 0:
    dec gApiTableRefCount
  if gApiTableRefCount == 0:
    gHandleRegistry.clear()
    gBuiltinResumptionCache.clear()
    gGlobalExecutionConfig = GlobalExecutionConfigState()

proc getGlobalExecutionConfigState*(): GlobalExecutionConfigState =
  gGlobalExecutionConfig
