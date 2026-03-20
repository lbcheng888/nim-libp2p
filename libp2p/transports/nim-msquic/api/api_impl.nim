## MsQuic 最小 API 表实现，覆盖 `MsQuicOpenVersion` 所需的核心句柄/回调。

import std/tables
import std/sequtils
import std/endians
import std/times
import std/strutils
when defined(windows):
  import std/winlean
else:
  import std/posix
  when not declared(Ifaddrs):
    type
      Ifaddrs {.importc: "struct ifaddrs", header: "<ifaddrs.h>", bycopy.} = object
        ifa_next: ptr Ifaddrs
        ifa_name: cstring
        ifa_flags: cuint
        ifa_addr: ptr SockAddr
        ifa_netmask: ptr SockAddr
        ifa_dstaddr: ptr SockAddr
        ifa_data: pointer
  when not declared(getifaddrs):
    proc getifaddrs(ifap: ptr ptr Ifaddrs): cint {.importc, header: "<ifaddrs.h>".}
  when not declared(freeifaddrs):
    proc freeifaddrs(ifa: ptr Ifaddrs) {.importc, header: "<ifaddrs.h>".}
import chronos
import chronicles

from "../core/mod" import ConnectionId, QuicConnection, QuicVersion,
    initConnectionId, newConnection, ConnectionRole, crClient, crServer,
    CryptoEpoch, ceInitial, ceOneRtt, nextPacketNumber, recordAckedPacket,
    initiatePathChallenge, completePathValidation, updateHandshakeState, PathState,
    registerStatelessReset, unregisterStatelessReset,
    configurePreferredAddress, PreferredAddressState,
    MaxCidLength
from "../congestion/common" import CongestionAlgorithm, caCubic, caBbr,
    AckEventSnapshot, LossEventSnapshot, ackTypeAckImmediate, ackTypeAckEliciting,
    MaxAckDelayDefaultMs, MinAckSendNumber, PersistentCongestionThreshold, PacketReorderThreshold,
    timeReorderThreshold
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
    targetRemote: TransportAddress
    zeroRttDispatched: bool

  PendingDatagram = object
    payload: seq[byte]
    clientContext: pointer
    targetRemote: TransportAddress
    zeroRttDispatched: bool

  PendingControlFrame = object
    epoch: CryptoEpoch
    payload: seq[byte]
    frameKind: SentFrameKind
    ackEliciting: bool
    targetRemote: TransportAddress

  SentFrameKind* = enum
    sfkStream
    sfkResetStream
    sfkStopSending
    sfkDatagram
    sfkPing
    sfkAck
    sfkCrypto
    sfkConnectionIdUpdate
    sfkPathChallenge
    sfkPathResponse
    sfkHandshakeDone
    sfkConnectionClose

  SentPacketMeta = object
    packetNumber: uint64
    epoch: CryptoEpoch
    ackEliciting: bool
    appLimited: bool
    packetLength: uint16
    sentTimeUs: uint64
    totalBytesSentAtSend: uint64
    frameKind: SentFrameKind
    framePayload: seq[byte]
    stream: pointer
    streamOffset: uint64
    streamPayload: seq[byte]
    streamFin: bool
    clientContext: pointer
    targetRemote: TransportAddress

  BuiltinResumptionEntry = object
    ticket: seq[byte]
    data: seq[byte]
    issuedAtUs: uint64
    expiresAtUs: uint64
    maxEarlyData: uint32

  BuiltinZeroRttReplayEntry = object
    expiresAtUs: uint64

  IssuedConnectionIdState = object
    sequence: uint64
    cid: ConnectionId
    resetToken: array[16, uint8]
    retired: bool

  PeerConnectionIdState = object
    sequence: uint64
    cid: ConnectionId
    resetToken: array[16, uint8]
    retired: bool

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

  QuicStreamEventReceiveBufferNeededPayload* {.bycopy.} = object
    BufferLengthNeeded*: uint64

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
    receiveBuffersProvided: uint64

  ListenerState = ref object of QuicHandleState
    registration: RegistrationState
    callback: QuicListenerCallback
    callbackContext: pointer
    started: bool
    stopped: bool
    alpns: seq[string]
    transport: DatagramTransport
    localAddress: TransportAddress
    acceptedConnections: Table[string, ConnectionState]
    pendingZeroRttPackets: Table[string, seq[seq[byte]]]

  ConnectionState = ref object of QuicHandleState
    registration: RegistrationState
    configuration: ConfigurationState
    callback: QuicConnectionCallback
    callbackContext: pointer
    started: bool
    isClient: bool
    partitionId: uint16
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
    localAddress: TransportAddress
    remoteAddress: TransportAddress

    initialSecrets: InitialSecrets
    handshakeKeys: InitialSecrets
    oneRttKeys: TrafficSecrets # Placeholder for Short Header keys
    zeroRttMaterial: tls.ZeroRttMaterial
    handshakeClientSecret: seq[byte]
    handshakeServerSecret: seq[byte]
    clientPrivateKey: Curve25519Key
    transcript: seq[byte]
    handshakeComplete: bool
    peerAddressValidated: bool
    localTransportParams: tls.QuicTransportParameters
    peerTransportParams: tls.QuicTransportParameters
    sessionResumed: bool
    resumptionTicket: seq[byte]
    resumptionData: seq[byte]
    zeroRttMaxData: uint32
    zeroRttSentBytes: uint64
    zeroRttAcceptDeadlineUs: uint64
    incomingStreams: Table[uint64, StreamState]
    ackTracker: qack.AckTrackerModel
    pendingAckRemotes: array[CryptoEpoch, TransportAddress]
    lossDetection: qloss.LossDetectionModel
    congestionController: qcc.CongestionController
    sentPackets: seq[SentPacketMeta]
    pendingDatagrams: seq[PendingDatagram]
    pendingControlFrames: seq[PendingControlFrame]
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
    peerCidSequence: uint64
    activePeerCidSequence: uint64
    localCidSequence: uint64
    activeLocalCidSequence: uint64
    advertisedPeerCids: seq[PeerConnectionIdState]
    issuedLocalCids: seq[IssuedConnectionIdState]
    pendingAutoLocalCidAdvertisement: bool
    inLossDetectionTick: bool

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
  BuiltinResumptionTicketVersion = 3'u8
  BuiltinResumptionTicketLifetimeUs = 10'u64 * 60'u64 * 1_000_000'u64
  BuiltinZeroRttAcceptanceWindowUs = 60'u64 * 1_000_000'u64
  BuiltinTicketClockSkewToleranceUs = 5'u64 * 1_000_000'u64
  BuiltinTicketAgeToleranceUs = 10'u64 * 1_000_000'u64
  BuiltinDefaultMaxEarlyData = 16_384'u32
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
var gBuiltinZeroRttReplayCache = initTable[string, BuiltinZeroRttReplayEntry]()
var gForceRejectBuiltinZeroRttForTest = false
var gDropBuiltinEarlyDataRequestForTest = false
var gDropBuiltinTicketAgeForTest = false
var gDropBuiltinResumptionBinderForTest = false
var gTamperBuiltinResumptionBinderForTest = false
var gTamperBuiltinResumptionTicketForTest = false
var gHasOverrideBuiltinTicketServerNameForTest = false
var gOverrideBuiltinTicketServerNameForTest = ""
var gHasOverrideBuiltinTicketServerPortForTest = false
var gOverrideBuiltinTicketServerPortForTest = 0'u16
var gHasOverrideBuiltinTicketAlpnForTest = false
var gOverrideBuiltinTicketAlpnForTest = ""
var gHasOverrideBuiltinTicketAgeDeltaMsForTest = false
var gOverrideBuiltinTicketAgeDeltaMsForTest = 0'i64
var gHasOverrideBuiltinNewSessionTicketLifetimeSecForTest = false
var gOverrideBuiltinNewSessionTicketLifetimeSecForTest = 600'u32
var gHasOverrideBuiltinNewSessionTicketMaxEarlyDataForTest = false
var gOverrideBuiltinNewSessionTicketMaxEarlyDataForTest = BuiltinDefaultMaxEarlyData
var gBuiltinTicketAgeAddSalt = 0'u32

proc generateSingleConnectionId(): ConnectionId {.gcsafe.}
proc cidBytes(cid: ConnectionId): seq[byte] {.gcsafe.}
proc currentProbeTimeoutUs(conn: ConnectionState; epoch: CryptoEpoch = ceOneRtt): uint64 {.gcsafe.}
proc updateLossDetectionTimer(conn: ConnectionState) {.gcsafe.}
proc sendProbeForEpoch(conn: ConnectionState; epoch: CryptoEpoch): uint8 {.gcsafe.}
proc currentBuiltinNewSessionTicketLifetimeSec(): uint32 {.gcsafe.}
proc currentBuiltinNewSessionTicketMaxEarlyData(conn: ConnectionState): uint32 {.gcsafe.}
proc flushPendingControlFrames(conn: ConnectionState) {.gcsafe.}
proc sendOneRttPacket(conn: ConnectionState; payload: seq[byte];
    frameKind: SentFrameKind; ackEliciting: bool;
    stream: StreamState = nil; streamOffset = 0'u64;
    streamPayload: seq[byte] = @[]; streamFin = false;
    clientContext: pointer = nil;
    targetRemote: ptr TransportAddress = nil): uint64 {.gcsafe.}

proc nowMicros(): uint64 =
  uint64(epochTime() * 1_000_000.0)

proc appendUint16BE(buf: var seq[byte]; value: uint16) {.inline.} =
  buf.add(byte((value shr 8) and 0xFF'u16))
  buf.add(byte(value and 0xFF'u16))

proc appendUint32BE(buf: var seq[byte]; value: uint32) {.inline.} =
  for shift in countdown(24, 0, 8):
    buf.add(byte((value shr shift) and 0xFF'u32))

proc appendUint64BE(buf: var seq[byte]; value: uint64) {.inline.} =
  for shift in countdown(56, 0, 8):
    buf.add(byte((value shr shift) and 0xFF'u64))

proc readUint16BE(data: openArray[byte]; pos: var int): uint16 {.inline.} =
  if pos + 2 > data.len:
    raise newException(ValueError, "short u16")
  result = (uint16(data[pos]) shl 8) or uint16(data[pos + 1])
  pos += 2

proc readUint32BE(data: openArray[byte]; pos: var int): uint32 {.inline.} =
  if pos + 4 > data.len:
    raise newException(ValueError, "short u32")
  result = 0'u32
  for _ in 0 ..< 4:
    result = (result shl 8) or uint32(data[pos])
    inc pos

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
    issuedAtUs: uint64
    expiresAtUs: uint64
    serverName: string
    serverPort: uint16
    alpn: string
    ticketAgeAdd: uint32
    maxEarlyData: uint32
    data: seq[byte]

proc alpnForConnection(conn: ConnectionState): string {.gcsafe.} =
  if conn.isNil or conn.configuration.isNil or conn.configuration.alpns.len == 0:
    ""
  else:
    conn.configuration.alpns[0]

proc parseAlpnBuffers(alpnBuffers: ptr QuicBuffer; alpnBufferCount: uint32): seq[string] =
  result = @[]
  if alpnBufferCount == 0 or alpnBuffers.isNil:
    return
  let bufferArray = cast[ptr UncheckedArray[QuicBuffer]](alpnBuffers)
  for i in 0'u32 ..< alpnBufferCount:
    let buf = bufferArray[][int(i)]
    if buf.Length == 0 or buf.Buffer.isNil:
      result.add("")
    else:
      let data = cast[ptr UncheckedArray[uint8]](buf.Buffer)
      let strLen = int(buf.Length)
      var text = newString(strLen)
      system.copyMem(addr text[0], addr data[][0], strLen)
      result.add(text)

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
    if meta.version != 1'u8 and meta.version != 2'u8 and
        meta.version != BuiltinResumptionTicketVersion:
      return false
    if meta.version >= 3'u8:
      meta.issuedAtUs = readUint64BE(ticket, pos)
      meta.expiresAtUs = readUint64BE(ticket, pos)
      meta.serverPort = readUint16BE(ticket, pos)
      meta.maxEarlyData = readUint32BE(ticket, pos)
      meta.ticketAgeAdd = readUint32BE(ticket, pos)
    elif meta.version >= 2'u8:
      meta.issuedAtUs = readUint64BE(ticket, pos)
      meta.expiresAtUs = readUint64BE(ticket, pos)
      meta.serverPort = readUint16BE(ticket, pos)
      meta.maxEarlyData = readUint32BE(ticket, pos)
      meta.ticketAgeAdd = 0'u32
    else:
      meta.expiresAtUs = readUint64BE(ticket, pos)
      meta.serverPort = readUint16BE(ticket, pos)
      meta.issuedAtUs =
        if meta.expiresAtUs > BuiltinResumptionTicketLifetimeUs:
          meta.expiresAtUs - BuiltinResumptionTicketLifetimeUs
        else:
          0'u64
      meta.maxEarlyData = BuiltinDefaultMaxEarlyData
      meta.ticketAgeAdd = 0'u32
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

proc builtinZeroRttDeadline(meta: BuiltinResumptionTicketMeta): uint64 {.gcsafe.} =
  let windowDeadline =
    if meta.issuedAtUs == 0'u64:
      0'u64
    else:
      meta.issuedAtUs + BuiltinZeroRttAcceptanceWindowUs
  if meta.expiresAtUs == 0'u64:
    return windowDeadline
  if windowDeadline == 0'u64:
    return meta.expiresAtUs
  min(meta.expiresAtUs, windowDeadline)

proc builtinTicketTimeValid(meta: BuiltinResumptionTicketMeta;
    nowUs = nowMicros()): bool {.gcsafe.} =
  if meta.issuedAtUs > 0'u64 and meta.issuedAtUs > nowUs + BuiltinTicketClockSkewToleranceUs:
    return false
  if meta.expiresAtUs > 0'u64:
    if meta.issuedAtUs > 0'u64 and meta.expiresAtUs <= meta.issuedAtUs:
      return false
    if nowUs >= meta.expiresAtUs:
      return false
  true

proc builtinZeroRttEligible(meta: BuiltinResumptionTicketMeta; nowUs = nowMicros()): bool {.gcsafe.} =
  if not builtinTicketTimeValid(meta, nowUs):
    return false
  if meta.maxEarlyData == 0'u32:
    return false
  let deadline = builtinZeroRttDeadline(meta)
  deadline == 0'u64 or nowUs < deadline

proc computeBuiltinTicketAge(nowUs: uint64; meta: BuiltinResumptionTicketMeta): uint32 {.gcsafe.} =
  if meta.issuedAtUs == 0'u64 or nowUs <= meta.issuedAtUs:
    return meta.ticketAgeAdd
  let ageMs = min((nowUs - meta.issuedAtUs) div 1_000'u64, uint64(high(uint32)))
  uint32(ageMs) + meta.ticketAgeAdd

proc builtinTicketAgeAccepted(meta: BuiltinResumptionTicketMeta;
    obfuscatedTicketAge: uint32; agePresent: bool;
    nowUs = nowMicros()): bool {.gcsafe.} =
  if meta.version < 3'u8:
    return true
  if not agePresent or meta.issuedAtUs == 0'u64:
    return false
  let actualAgeUs = if nowUs > meta.issuedAtUs: nowUs - meta.issuedAtUs else: 0'u64
  let actualAgeMs = min(actualAgeUs div 1_000'u64, uint64(high(uint32)))
  let apparentAgeMs = uint64(obfuscatedTicketAge - meta.ticketAgeAdd)
  let diff =
    if actualAgeMs >= apparentAgeMs: actualAgeMs - apparentAgeMs
    else: apparentAgeMs - actualAgeMs
  diff <= (BuiltinTicketAgeToleranceUs div 1_000'u64)

proc normalizeTicketServerName(host: string): string {.gcsafe.} =
  if host.len >= 2 and host[0] == '[' and host[^1] == ']':
    host[1 .. ^2]
  else:
    host

proc builtinTicketServerNameAccepted(meta: BuiltinResumptionTicketMeta;
    local: TransportAddress): bool {.gcsafe.} =
  if meta.serverName.len == 0:
    return true
  let localHost = normalizeTicketServerName(local.host())
  if localHost.len == 0 or localHost == "0.0.0.0" or localHost == "::":
    return true
  meta.serverName == localHost

proc bytesToHex(data: openArray[byte]): string =
  const HexChars = "0123456789abcdef"
  result = newStringOfCap(data.len * 2)
  for b in data:
    result.add(HexChars[int((b shr 4) and 0x0F)])
    result.add(HexChars[int(b and 0x0F)])

proc pruneBuiltinZeroRttReplayCache(nowUs: uint64 = nowMicros()) =
  var stale: seq[string] = @[]
  for key, entry in gBuiltinZeroRttReplayCache.pairs:
    if entry.expiresAtUs > 0'u64 and entry.expiresAtUs <= nowUs:
      stale.add(key)
  for key in stale:
    gBuiltinZeroRttReplayCache.del(key)

proc claimBuiltinZeroRttReplay(ticket: openArray[byte]; binder: openArray[byte];
    expiresAtUs: uint64): bool =
  if ticket.len == 0 or binder.len == 0:
    return false
  let nowUs = nowMicros()
  pruneBuiltinZeroRttReplayCache(nowUs)
  let key = bytesToHex(ticket)
  if gBuiltinZeroRttReplayCache.hasKey(key):
    return false
  gBuiltinZeroRttReplayCache[key] = BuiltinZeroRttReplayEntry(
    expiresAtUs: (if expiresAtUs > nowUs: expiresAtUs else: nowUs + BuiltinResumptionTicketLifetimeUs)
  )
  true

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
  if not builtinTicketTimeValid(meta, nowUs):
    return false
  (meta.serverName.len == 0 or meta.serverName == serverName) and
    (meta.serverPort == 0'u16 or meta.serverPort == serverPort) and
    (meta.alpn.len == 0 or meta.alpn == alpn)

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
    data: seq[byte] = @[]; ticketLifetimeSec: int64 = -1'i64) =
  if conn.isNil or ticket.len == 0:
    return
  let key = resumptionCacheKey(conn)
  if key.len == 0:
    return
  var meta: BuiltinResumptionTicketMeta
  var expiresAtUs =
    if parseBuiltinResumptionTicket(ticket, meta): meta.expiresAtUs
    else: nowMicros() + BuiltinResumptionTicketLifetimeUs
  if ticketLifetimeSec >= 0'i64:
    let lifetimeExpiry =
      nowMicros() + uint64(ticketLifetimeSec) * 1_000_000'u64
    if expiresAtUs == 0'u64 or lifetimeExpiry < expiresAtUs:
      expiresAtUs = lifetimeExpiry
  gBuiltinResumptionCache[key] = BuiltinResumptionEntry(
    ticket: ticket,
    data: data,
    issuedAtUs: (if meta.issuedAtUs > 0'u64: meta.issuedAtUs else: nowMicros()),
    expiresAtUs: expiresAtUs,
    maxEarlyData: meta.maxEarlyData
  )

proc writeSockAddr(addrValue: TransportAddress; bufferLength: ptr uint32;
    buffer: pointer): QUIC_STATUS {.gcsafe.} =
  if bufferLength.isNil:
    return QUIC_STATUS_INVALID_PARAMETER
  if addrValue.family == AddressFamily.None:
    return QUIC_STATUS_INVALID_STATE
  var storage: Sockaddr_storage
  var slen: SockLen
  try:
    toSAddr(addrValue, storage, slen)
  except CatchableError:
    return QUIC_STATUS_INVALID_PARAMETER
  let required = uint32(slen)
  let requested = bufferLength[]
  bufferLength[] = required
  if buffer.isNil:
    return QUIC_STATUS_SUCCESS
  if requested < required:
    return QUIC_STATUS_INVALID_PARAMETER
  zeroMem(buffer, int(requested))
  copyMem(buffer, addr storage, int(slen))
  QUIC_STATUS_SUCCESS

proc parseSockAddr(address: pointer; outAddr: var TransportAddress): bool {.gcsafe.} =
  if address.isNil:
    return false
  try:
    let sa = cast[ptr SockAddr](address)
    let family = cint(sa.sa_family)
    if family == cint(posix.AF_INET):
      var storage: Sockaddr_storage
      copyMem(addr storage, address, sizeof(Sockaddr_in))
      var slen = SockLen(sizeof(Sockaddr_in))
      fromSAddr(addr storage, slen, outAddr)
      true
    elif family == cint(posix.AF_INET6):
      var storage: Sockaddr_storage
      copyMem(addr storage, address, sizeof(Sockaddr_in6))
      var slen = SockLen(sizeof(Sockaddr_in6))
      fromSAddr(addr storage, slen, outAddr)
      true
    else:
      false
  except CatchableError:
    false

proc ackRemoteEpoch(epoch: CryptoEpoch): CryptoEpoch {.gcsafe.} =
  case epoch
  of ceZeroRtt, ceOneRtt:
    ceOneRtt
  else:
    epoch

proc setPendingAckRemote(conn: ConnectionState; epoch: CryptoEpoch;
    remote: TransportAddress) {.gcsafe.} =
  if conn.isNil or remote.family == AddressFamily.None:
    return
  conn.pendingAckRemotes[ackRemoteEpoch(epoch)] = remote

proc clearPendingAckRemote(conn: ConnectionState; epoch: CryptoEpoch) {.gcsafe.} =
  if conn.isNil:
    return
  conn.pendingAckRemotes[ackRemoteEpoch(epoch)] =
    TransportAddress(family: AddressFamily.None)

proc pendingAckRemote(conn: ConnectionState; epoch: CryptoEpoch): ptr TransportAddress {.gcsafe.} =
  if conn.isNil:
    return nil
  let ackEpoch = ackRemoteEpoch(epoch)
  if conn.pendingAckRemotes[ackEpoch].family == AddressFamily.None:
    return nil
  addr conn.pendingAckRemotes[ackEpoch]

proc remoteAddressKey(remote: TransportAddress): string {.gcsafe.}
proc registerPathRemote(conn: ConnectionState; pathId: uint8;
    remote: TransportAddress) {.gcsafe.}
proc maybeInitiatePathMigration(conn: ConnectionState;
    remote: TransportAddress) {.gcsafe.}
proc sendAckFrame(conn: ConnectionState; largestAcked: uint64; epoch: CryptoEpoch;
    targetRemote: ptr TransportAddress = nil): bool {.gcsafe.}

proc maybeFlushPendingOneRttAckOnRemoteChange(conn: ConnectionState;
    remote: TransportAddress): bool {.gcsafe.} =
  if conn.isNil or not conn.handshakeComplete or remote.family == AddressFamily.None:
    return false
  let pendingRemote = conn.pendingAckRemote(ceOneRtt)
  if pendingRemote.isNil or pendingRemote[].family == AddressFamily.None:
    return false
  if remoteAddressKey(pendingRemote[]) == remoteAddressKey(remote):
    return false
  let hasPendingAck =
    qack.packetNumbersToAck(conn.ackTracker, ceOneRtt).len > 0 or
    qack.ackElicitingPacketsToAck(conn.ackTracker, ceOneRtt) > 0'u16
  if not hasPendingAck:
    return false
  discard conn.sendAckFrame(
    qack.largestPacketNumberAcked(conn.ackTracker, ceOneRtt),
    ceOneRtt,
    pendingRemote
  )
  true

proc setListenerLocalAddress(state: ListenerState;
    addrValue: TransportAddress): QUIC_STATUS {.gcsafe.} =
  if state.isNil:
    return QUIC_STATUS_INVALID_PARAMETER
  if addrValue.family == AddressFamily.None:
    return QUIC_STATUS_INVALID_PARAMETER
  if state.started and not state.transport.isNil:
    return QUIC_STATUS_INVALID_STATE
  state.localAddress = addrValue
  {.cast(gcsafe).}:
    emitDiagnostics(DiagnosticsEvent(
      kind: diagRegistrationOpened,
      handle: cast[pointer](state),
      note: "listener local address updated to " & $state.localAddress))
  QUIC_STATUS_SUCCESS

proc setConnectionLocalAddress(state: ConnectionState;
    addrValue: TransportAddress): QUIC_STATUS {.gcsafe.} =
  if state.isNil:
    return QUIC_STATUS_INVALID_PARAMETER
  if addrValue.family == AddressFamily.None:
    return QUIC_STATUS_INVALID_PARAMETER
  if not state.transport.isNil:
    return QUIC_STATUS_INVALID_STATE
  state.localAddress = addrValue
  {.cast(gcsafe).}:
    emitDiagnostics(DiagnosticsEvent(
      kind: diagConnectionParamSet,
      handle: cast[pointer](state),
      paramId: QUIC_PARAM_CONN_LOCAL_ADDRESS,
      note: "local address updated to " & $state.localAddress))
  QUIC_STATUS_SUCCESS

proc setConnectionRemoteAddress(state: ConnectionState;
    addrValue: TransportAddress): QUIC_STATUS {.gcsafe.} =
  if state.isNil:
    return QUIC_STATUS_INVALID_PARAMETER
  if addrValue.family == AddressFamily.None:
    return QUIC_STATUS_INVALID_PARAMETER
  state.remoteAddress = addrValue
  let remoteKey = remoteAddressKey(addrValue)
  if remoteKey.len > 0 and
      not state.quicConn.isNil and
      not state.transport.isNil and
      state.started and
      state.handshakeComplete and
      state.settingsOverlay.migrationEnabled and
      remoteKey != state.activeRemoteKey:
    let activeRemote = state.pathRemoteAddrs.getOrDefault(
      state.quicConn.model.migration.activePathId,
      TransportAddress(family: AddressFamily.None))
    state.remoteAddress = activeRemote
    state.maybeInitiatePathMigration(addrValue)
    {.cast(gcsafe).}:
      emitDiagnostics(DiagnosticsEvent(
        kind: diagConnectionParamSet,
        handle: cast[pointer](state),
        paramId: QUIC_PARAM_CONN_REMOTE_ADDRESS,
        note: "remote address migration candidate=" & $addrValue))
    return QUIC_STATUS_SUCCESS
  if remoteKey.len > 0:
    state.activeRemoteKey = remoteKey
    if not state.quicConn.isNil:
      let activePathId = state.quicConn.model.migration.activePathId
      state.registerPathRemote(activePathId, addrValue)
  {.cast(gcsafe).}:
    emitDiagnostics(DiagnosticsEvent(
      kind: diagConnectionParamSet,
      handle: cast[pointer](state),
      paramId: QUIC_PARAM_CONN_REMOTE_ADDRESS,
      note: "remote address updated to " & $state.remoteAddress))
  QUIC_STATUS_SUCCESS

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
  for existing in stream.pendingChunks:
    if existing.offset == chunk.offset and existing.fin == chunk.fin and
        existing.payload == chunk.payload and
        existing.targetRemote == chunk.targetRemote:
      return
  stream.pendingChunks.insert(chunk, 0)

proc samePendingControlFrame(lhs, rhs: PendingControlFrame): bool {.inline, gcsafe.} =
  lhs.epoch == rhs.epoch and
    lhs.frameKind == rhs.frameKind and
    lhs.ackEliciting == rhs.ackEliciting and
    lhs.payload == rhs.payload and
    lhs.targetRemote == rhs.targetRemote

proc prependPendingControlFrame(conn: ConnectionState;
    frame: PendingControlFrame) {.gcsafe.} =
  if conn.isNil:
    return
  for existing in conn.pendingControlFrames:
    if samePendingControlFrame(existing, frame):
      return
  conn.pendingControlFrames.insert(frame, 0)

proc latestAckElicitingSentTime(conn: ConnectionState; epoch: CryptoEpoch): uint64 {.gcsafe.} =
  if conn.isNil:
    return 0'u64
  var found = false
  var bestSentTimeUs = 0'u64
  var bestPn = 0'u64
  for meta in conn.sentPackets:
    if meta.epoch != epoch or not meta.ackEliciting:
      continue
    if not found or meta.sentTimeUs > bestSentTimeUs or
        (meta.sentTimeUs == bestSentTimeUs and meta.packetNumber > bestPn):
      bestSentTimeUs = meta.sentTimeUs
      bestPn = meta.packetNumber
      found = true
  if found: bestSentTimeUs else: 0'u64

proc newestAckElicitingSentPacket(conn: ConnectionState; epoch: CryptoEpoch;
    meta: var SentPacketMeta): bool {.gcsafe.} =
  if conn.isNil:
    return false
  var found = false
  var bestSentTimeUs = 0'u64
  var bestPn = 0'u64
  for sent in conn.sentPackets:
    if sent.epoch != epoch or not sent.ackEliciting:
      continue
    if not found or sent.sentTimeUs > bestSentTimeUs or
        (sent.sentTimeUs == bestSentTimeUs and sent.packetNumber > bestPn):
      meta = sent
      bestSentTimeUs = sent.sentTimeUs
      bestPn = sent.packetNumber
      found = true
  found

proc selectLossDetectionSchedule(conn: ConnectionState; epoch: var CryptoEpoch;
    timerUs: var uint64; dueToLossTime: var bool): bool {.gcsafe.} =
  if conn.isNil or conn.quicConn.isNil:
    return false
  var found = false
  for candidate in CryptoEpoch:
    let lossTime = conn.quicConn.model.packetSpaces[candidate].lossTime
    if lossTime == 0'u64:
      continue
    if not found or lossTime < timerUs or (lossTime == timerUs and ord(candidate) < ord(epoch)):
      found = true
      epoch = candidate
      timerUs = lossTime
      dueToLossTime = true
  if found:
    return true
  for candidate in CryptoEpoch:
    let latestSentUs = conn.latestAckElicitingSentTime(candidate)
    if latestSentUs == 0'u64:
      continue
    let ptoUs = conn.currentProbeTimeoutUs(candidate)
    let candidateTimer = latestSentUs + ptoUs
    if not found or candidateTimer < timerUs or (candidateTimer == timerUs and ord(candidate) < ord(epoch)):
      found = true
      epoch = candidate
      timerUs = candidateTimer
      dueToLossTime = false
  found

proc recordSentPacket(conn: ConnectionState; meta: SentPacketMeta) {.gcsafe.} =
  if conn.isNil:
    return
  conn.sentPackets.add(meta)
  let packet = qloss.PacketRecord(
    packetNumber: meta.packetNumber,
    encryptEpoch: meta.epoch,
    sentTimeUs: meta.sentTimeUs,
    totalBytesSentAtSend: meta.totalBytesSentAtSend,
    ackEliciting: meta.ackEliciting,
    packetLength: meta.packetLength
  )
  qloss.onPacketSent(conn.lossDetection, packet)
  conn.updateLossDetectionTimer()

proc updateLossDetectionTimer(conn: ConnectionState) {.gcsafe.} =
  if conn.isNil or conn.quicConn.isNil:
    return
  var epoch = ceInitial
  var nextTimer = 0'u64
  var dueToLossTime = false
  if conn.selectLossDetectionSchedule(epoch, nextTimer, dueToLossTime):
    conn.quicConn.model.timers.lossDetectionTimer = nextTimer
  else:
    conn.quicConn.model.timers.lossDetectionTimer = 0'u64

proc buildBuiltinResumptionTicket(serverName: string; serverPort: uint16;
    alpn: string; data: seq[byte] = @[]; issuedAtUs = 0'u64;
    expiresAtUs = 0'u64; maxEarlyData = BuiltinDefaultMaxEarlyData;
    ticketAgeAdd = 0'u32; preserveTicketAgeAdd = false): seq[byte] {.gcsafe.} =
  let nowUs = nowMicros()
  let ticketIssuedAtUs = if issuedAtUs == 0'u64: nowUs else: issuedAtUs
  let ticketExpiresAtUs =
    if expiresAtUs == 0'u64:
      ticketIssuedAtUs + BuiltinResumptionTicketLifetimeUs
    else:
      expiresAtUs
  let ticketAgeAddValue =
    if preserveTicketAgeAdd:
      ticketAgeAdd
    else:
      block:
        inc(gBuiltinTicketAgeAddSalt)
        uint32(((ticketIssuedAtUs shr 12) xor
          (ticketIssuedAtUs shr 37) xor
          uint64(serverPort) xor
          uint64(gBuiltinTicketAgeAddSalt) xor
          0xA5A55A5A'u64) and uint64(high(uint32)))
  let cappedDataLen = min(data.len, high(uint16).int)
  let serverNameLen = min(serverName.len, 255)
  let alpnLen = min(alpn.len, 255)
  result = @[]
  result.add(BuiltinResumptionTicketMagic)
  result.add(BuiltinResumptionTicketVersion)
  result.appendUint64BE(ticketIssuedAtUs)
  result.appendUint64BE(ticketExpiresAtUs)
  result.appendUint16BE(serverPort)
  result.appendUint32BE(maxEarlyData)
  result.appendUint32BE(ticketAgeAddValue)
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

proc buildBuiltinResumptionTicket(conn: ConnectionState; data: seq[byte] = @[]): seq[byte] {.gcsafe.} =
  if conn.isNil:
    return @[]
  buildBuiltinResumptionTicket(
    conn.serverName,
    conn.serverPort,
    alpnForConnection(conn),
    data
  )

proc tamperBuiltinResumptionTicket(ticket: seq[byte]): seq[byte] {.gcsafe.} =
  result = ticket
  if result.len > 0:
    result[^1] = result[^1] xor 0x5A'u8

proc overrideBuiltinResumptionTicketServerName(ticket: seq[byte];
    serverName: string): seq[byte] {.gcsafe.} =
  var meta: BuiltinResumptionTicketMeta
  if not parseBuiltinResumptionTicket(ticket, meta):
    return ticket
  buildBuiltinResumptionTicket(
    serverName,
    meta.serverPort,
    meta.alpn,
    meta.data,
    issuedAtUs = meta.issuedAtUs,
    expiresAtUs = meta.expiresAtUs,
    maxEarlyData = meta.maxEarlyData,
    ticketAgeAdd = meta.ticketAgeAdd,
    preserveTicketAgeAdd = true
  )

proc overrideBuiltinResumptionTicketServerPort(ticket: seq[byte];
    serverPort: uint16): seq[byte] {.gcsafe.} =
  var meta: BuiltinResumptionTicketMeta
  if not parseBuiltinResumptionTicket(ticket, meta):
    return ticket
  buildBuiltinResumptionTicket(
    meta.serverName,
    serverPort,
    meta.alpn,
    meta.data,
    issuedAtUs = meta.issuedAtUs,
    expiresAtUs = meta.expiresAtUs,
    maxEarlyData = meta.maxEarlyData,
    ticketAgeAdd = meta.ticketAgeAdd,
    preserveTicketAgeAdd = true
  )

proc overrideBuiltinResumptionTicketAlpn(ticket: seq[byte];
    alpn: string): seq[byte] {.gcsafe.} =
  var meta: BuiltinResumptionTicketMeta
  if not parseBuiltinResumptionTicket(ticket, meta):
    return ticket
  buildBuiltinResumptionTicket(
    meta.serverName,
    meta.serverPort,
    alpn,
    meta.data,
    issuedAtUs = meta.issuedAtUs,
    expiresAtUs = meta.expiresAtUs,
    maxEarlyData = meta.maxEarlyData,
    ticketAgeAdd = meta.ticketAgeAdd,
    preserveTicketAgeAdd = true
  )

proc applyBuiltinTicketAgeDelta(obfuscatedAge: uint32; deltaMs: int64): uint32 {.gcsafe.} =
  if deltaMs >= 0'i64:
    let widened = uint64(obfuscatedAge) + uint64(deltaMs)
    uint32(min(widened, uint64(high(uint32))))
  else:
    let offset = uint64(-deltaMs)
    if offset >= uint64(obfuscatedAge):
      0'u32
    else:
      uint32(uint64(obfuscatedAge) - offset)

proc buildPathChallengeData(conn: ConnectionState; pathId: uint8): array[8, byte] {.gcsafe.} =
  var seed = nowMicros() xor conn.latestAckedPacket xor uint64(pathId)
  for i in 0 .. 7:
    result[i] = byte((seed shr (i * 8)) and 0xFF'u64)

proc buildStatelessResetToken(conn: ConnectionState; sequence: uint64;
    cid: ConnectionId): array[16, uint8] {.gcsafe.} =
  let localLen = int(conn.localCid.length)
  let peerLen = int(conn.peerCid.length)
  let cidLen = int(cid.length)
  for i in 0 ..< 16:
    let seqByte = uint8((sequence shr ((i mod 8) * 8)) and 0xFF'u64)
    let localByte = if localLen == 0: 0'u8 else: conn.localCid.bytes[i mod localLen]
    let peerByte = if peerLen == 0: 0'u8 else: conn.peerCid.bytes[(i * 3) mod peerLen]
    let cidByte = if cidLen == 0: 0'u8 else: cid.bytes[(i * 5) mod cidLen]
    result[i] = seqByte xor localByte xor peerByte xor cidByte xor uint8(i * 17)

proc syncLocalCidWithModel(conn: ConnectionState) {.gcsafe.} =
  if conn.isNil or conn.quicConn.isNil:
    return
  conn.quicConn.model.connectionId = conn.localCid

proc syncPeerCidWithModel(conn: ConnectionState) {.gcsafe.} =
  if conn.isNil or conn.quicConn.isNil:
    return
  conn.quicConn.model.peerConnectionId = conn.peerCid

proc sameCid(lhs, rhs: ConnectionId): bool {.gcsafe.} =
  if lhs.length != rhs.length:
    return false
  for i in 0 ..< int(lhs.length):
    if lhs.bytes[i] != rhs.bytes[i]:
      return false
  true

proc recordIssuedLocalCid(conn: ConnectionState; sequence: uint64;
    cid: ConnectionId; retired = false;
    resetToken = default(array[16, uint8])) {.gcsafe.} =
  if conn.isNil:
    return
  for item in conn.issuedLocalCids.mitems:
    if item.sequence == sequence:
      item.cid = cid
      item.resetToken = resetToken
      item.retired = retired
      return
  conn.issuedLocalCids.add(IssuedConnectionIdState(
    sequence: sequence,
    cid: cid,
    resetToken: resetToken,
    retired: retired
  ))

proc recordAdvertisedPeerCid(conn: ConnectionState; sequence: uint64;
    cid: ConnectionId; resetToken: array[16, uint8];
    retired = false) {.gcsafe.} =
  if conn.isNil:
    return
  for item in conn.advertisedPeerCids.mitems:
    if item.sequence == sequence:
      item.cid = cid
      item.resetToken = resetToken
      item.retired = item.retired or retired
      return
  conn.advertisedPeerCids.add(PeerConnectionIdState(
    sequence: sequence,
    cid: cid,
    resetToken: resetToken,
    retired: retired
  ))

proc markIssuedLocalCidRetired(conn: ConnectionState; sequence: uint64): bool {.gcsafe.} =
  if conn.isNil:
    return false
  for item in conn.issuedLocalCids.mitems:
    if item.sequence == sequence:
      if not conn.quicConn.isNil:
        {.cast(gcsafe).}:
          conn.quicConn.model.unregisterStatelessReset(item.resetToken)
      item.retired = true
      return true
  false

proc retireAdvertisedPeerCid(conn: ConnectionState; sequence: uint64): bool {.gcsafe.} =
  if conn.isNil:
    return false
  for item in conn.advertisedPeerCids.mitems:
    if item.sequence == sequence:
      if item.retired:
        return false
      if not conn.quicConn.isNil:
        {.cast(gcsafe).}:
          conn.quicConn.model.unregisterStatelessReset(item.resetToken)
      item.retired = true
      return true
  false

proc retireAdvertisedPeerCidsPriorTo(conn: ConnectionState; sequence: uint64) {.gcsafe.} =
  if conn.isNil:
    return
  for item in conn.advertisedPeerCids.mitems:
    if item.sequence < sequence:
      item.retired = true

proc findReplacementLocalCid(conn: ConnectionState; sequence: var uint64;
    cid: var ConnectionId): bool {.gcsafe.} =
  if conn.isNil:
    return false
  var found = false
  var bestSeq = 0'u64
  for item in conn.issuedLocalCids:
    if item.retired or item.sequence == conn.activeLocalCidSequence:
      continue
    if not found or item.sequence > bestSeq:
      found = true
      bestSeq = item.sequence
      cid = item.cid
  if found:
    sequence = bestSeq
  found

proc findReplacementPeerCid(conn: ConnectionState; sequence: var uint64;
    cid: var ConnectionId; resetToken: var array[16, uint8]): bool {.gcsafe.} =
  if conn.isNil:
    return false
  var found = false
  var bestSeq = 0'u64
  for item in conn.advertisedPeerCids:
    if item.retired or item.sequence == conn.activePeerCidSequence:
      continue
    if not found or item.sequence > bestSeq:
      found = true
      bestSeq = item.sequence
      cid = item.cid
      resetToken = item.resetToken
  if found:
    sequence = bestSeq
  found

proc sendRetirePeerConnectionId(conn: ConnectionState; sequence: uint64): bool {.gcsafe.} =
  if conn.isNil or conn.quicConn.isNil:
    return false
  if not conn.retireAdvertisedPeerCid(sequence):
    return false
  let frame = proto.encodeRetireConnectionIdFrame(sequence)
  if conn.handshakeComplete and not conn.transport.isNil:
    discard conn.sendOneRttPacket(frame, sfkConnectionIdUpdate, true)
    {.cast(gcsafe).}:
      emitDiagnostics(DiagnosticsEvent(
        kind: diagConnectionEvent,
        handle: cast[pointer](conn),
        note: "TX RETIRE_CONNECTION_ID seq=" & $sequence
      ))
  else:
    {.cast(gcsafe).}:
      emitDiagnostics(DiagnosticsEvent(
        kind: diagConnectionEvent,
        handle: cast[pointer](conn),
        note: "peer CID retired locally seq=" & $sequence & " (frame deferred)"
      ))
  true

proc retirePeerConnectionIdsPriorTo(conn: ConnectionState; sequence: uint64;
    keepSequence: uint64) {.gcsafe.} =
  if conn.isNil:
    return
  var retireList: seq[uint64] = @[]
  for item in conn.advertisedPeerCids:
    if not item.retired and item.sequence < sequence and item.sequence != keepSequence:
      retireList.add(item.sequence)
  for itemSequence in retireList:
    discard conn.sendRetirePeerConnectionId(itemSequence)

proc localCidAccepted(conn: ConnectionState; cid: ConnectionId): bool {.gcsafe.} =
  if conn.isNil:
    return false
  if sameCid(conn.localCid, cid):
    return true
  for item in conn.issuedLocalCids:
    if not item.retired and sameCid(item.cid, cid):
      return true
  false

proc initialPacketMatchesConnection(conn: ConnectionState;
    srcCidBytes, destCidBytes: openArray[byte]): bool {.gcsafe.} =
  if conn.isNil:
    return false
  sameCid(conn.peerCid, initConnectionId(srcCidBytes)) and
    sameCid(conn.localCid, initConnectionId(destCidBytes))

proc removeAcceptedConnection(state: ListenerState; conn: ConnectionState) {.gcsafe.} =
  if state.isNil or conn.isNil:
    return
  var staleKeys: seq[string] = @[]
  for key, value in state.acceptedConnections.pairs:
    if value == conn:
      staleKeys.add(key)
  for key in staleKeys:
    state.acceptedConnections.del(key)

proc acceptedConnectionCanonicalKey(conn: ConnectionState): string {.gcsafe.} =
  if conn.isNil:
    return ""
  "conn:" & $cast[uint](conn)

proc registerAcceptedConnection(state: ListenerState; remoteKey: string;
    conn: ConnectionState) {.gcsafe.} =
  if state.isNil or conn.isNil:
    return
  let canonicalKey = acceptedConnectionCanonicalKey(conn)
  if canonicalKey.len > 0:
    state.acceptedConnections[canonicalKey] = conn
  if remoteKey.len > 0:
    state.acceptedConnections[remoteKey] = conn

proc emitConnectionEvent(state: ConnectionState; event: var ConnectionEvent) {.gcsafe.}

proc matchesStatelessResetToken(conn: ConnectionState; data: openArray[byte]): bool {.gcsafe.} =
  if conn.isNil or conn.quicConn.isNil or data.len < 16:
    return false
  let tokenStart = data.len - 16
  for token in conn.quicConn.model.migration.statelessResetTokens:
    var matched = true
    for i in 0 ..< 16:
      if data[tokenStart + i] != token[i]:
        matched = false
        break
    if matched:
      return true
  false

proc handleStatelessReset(conn: ConnectionState; remote: TransportAddress;
    owner: ListenerState = nil): bool {.gcsafe.} =
  if conn.isNil:
    return false
  conn.started = false
  conn.handshakeComplete = false
  conn.closeReason = "stateless reset"
  if not owner.isNil:
    owner.removeAcceptedConnection(conn)
  var initiated = ConnectionEvent(
    kind: ceShutdownInitiated,
    errorCode: 0'u64,
    note: "stateless reset"
  )
  emitConnectionEvent(conn, initiated)
  var completed = ConnectionEvent(
    kind: ceShutdownComplete,
    errorCode: 0'u64,
    note: "stateless reset"
  )
  emitConnectionEvent(conn, completed)
  {.cast(gcsafe).}:
    emitDiagnostics(DiagnosticsEvent(
      kind: diagConnectionEvent,
      handle: cast[pointer](conn),
      note: "RX Stateless Reset"
    ))
  true

proc handleRemoteConnectionClose(conn: ConnectionState; errorCode: uint64;
    reasonPhrase: string; applicationClose: bool;
    owner: ListenerState = nil) {.gcsafe.} =
  if conn.isNil:
    return
  let note =
    if reasonPhrase.len > 0:
      reasonPhrase
    elif applicationClose:
      "application close"
    else:
      "transport close"
  conn.started = false
  conn.handshakeComplete = false
  conn.closeReason = reasonPhrase
  if not owner.isNil:
    owner.removeAcceptedConnection(conn)
  if not conn.transport.isNil:
    asyncCheck conn.transport.closeWait()
    conn.transport = nil
  var initiated = ConnectionEvent(
    kind: ceShutdownInitiated,
    errorCode: errorCode,
    note: note
  )
  emitConnectionEvent(conn, initiated)
  var completed = ConnectionEvent(
    kind: ceShutdownComplete,
    errorCode: errorCode,
    note: note
  )
  emitConnectionEvent(conn, completed)
  {.cast(gcsafe).}:
    emitDiagnostics(DiagnosticsEvent(
      kind: diagConnectionEvent,
      handle: cast[pointer](conn),
      note: "RX CONNECTION_CLOSE error=" & $errorCode & " app=" & $applicationClose
    ))

proc findAcceptedConnectionByStatelessResetToken(state: ListenerState;
    data: seq[byte]): ConnectionState {.gcsafe.} =
  if state.isNil:
    return nil
  for conn in state.acceptedConnections.values:
    if conn.matchesStatelessResetToken(data):
      return conn
  nil

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

proc adoptPreferredAddressPeerCid(conn: ConnectionState) {.gcsafe.} =
  if conn.isNil or conn.quicConn.isNil:
    return
  let preferred = conn.quicConn.model.migration.preferredAddress
  if not preferred.hasPreferred or preferred.cid.length == 0:
    return
  let previousActiveSeq = conn.activePeerCidSequence
  var sequence = conn.activePeerCidSequence
  var found = false
  for item in conn.advertisedPeerCids:
    if sameCid(item.cid, preferred.cid):
      sequence = item.sequence
      found = true
      break
  if not found:
    sequence = max(conn.peerCidSequence, conn.activePeerCidSequence) + 1'u64
    conn.recordAdvertisedPeerCid(sequence, preferred.cid, preferred.statelessResetToken)
    if sequence > conn.peerCidSequence:
      conn.peerCidSequence = sequence
    {.cast(gcsafe).}:
      conn.quicConn.model.registerStatelessReset(preferred.statelessResetToken)
  conn.activePeerCidSequence = sequence
  conn.peerCid = preferred.cid
  conn.syncPeerCidWithModel()
  if previousActiveSeq != conn.activePeerCidSequence:
    discard conn.sendRetirePeerConnectionId(previousActiveSeq)

proc activateValidatedPath(conn: ConnectionState; pathId: uint8) {.gcsafe.} =
  if conn.isNil or conn.quicConn.isNil:
    return
  if conn.pathRemoteAddrs.hasKey(pathId):
    conn.remoteAddress = conn.pathRemoteAddrs[pathId]
  if conn.pathRemoteKeys.hasKey(pathId):
    conn.activeRemoteKey = conn.pathRemoteKeys[pathId]
  if pathId == conn.quicConn.model.migration.preferredPathId:
    conn.adoptPreferredAddressPeerCid()
  {.cast(gcsafe).}:
    conn.quicConn.model.completePathValidation(pathId, true)

proc sendLocalNewConnectionId(conn: ConnectionState; retirePriorTo: uint64 = 0'u64): bool {.gcsafe.} =
  if conn.isNil or conn.quicConn.isNil:
    return false
  let newCid = generateSingleConnectionId()
  conn.localCidSequence += 1'u64
  let token = conn.buildStatelessResetToken(conn.localCidSequence, newCid)
  conn.recordIssuedLocalCid(conn.localCidSequence, newCid, resetToken = token)
  {.cast(gcsafe).}:
    conn.quicConn.model.registerStatelessReset(token)
  let frame = proto.encodeNewConnectionIdFrame(
    conn.localCidSequence,
    retirePriorTo,
    cidBytes(newCid),
    token
  )
  if conn.handshakeComplete and not conn.transport.isNil:
    discard conn.sendOneRttPacket(frame, sfkConnectionIdUpdate, true)
    {.cast(gcsafe).}:
      emitDiagnostics(DiagnosticsEvent(
        kind: diagConnectionEvent,
        handle: cast[pointer](conn),
        note: "TX NEW_CONNECTION_ID seq=" & $conn.localCidSequence &
          " retirePriorTo=" & $retirePriorTo
      ))
  else:
    {.cast(gcsafe).}:
      emitDiagnostics(DiagnosticsEvent(
        kind: diagConnectionEvent,
        handle: cast[pointer](conn),
        note: "local CID advertised seq=" & $conn.localCidSequence &
          " (frame deferred)"
      ))
  true

proc applyPeerNewConnectionId(conn: ConnectionState;
    frame: proto.NewConnectionIdFrame): bool {.gcsafe.} =
  if conn.isNil or conn.quicConn.isNil:
    return false
  if frame.connectionId.len == 0 or frame.connectionId.len > MaxCidLength.int:
    return false
  if frame.retirePriorTo > frame.sequence:
    return false
  var resetToken: array[16, uint8]
  for i in 0 ..< 16:
    resetToken[i] = frame.statelessResetToken[i]
  let newCid = initConnectionId(frame.connectionId)
  let previousActiveSeq = conn.activePeerCidSequence
  conn.recordAdvertisedPeerCid(frame.sequence, newCid, resetToken)
  for item in conn.advertisedPeerCids:
    if item.sequence == frame.sequence:
      if not item.retired:
        {.cast(gcsafe).}:
          conn.quicConn.model.registerStatelessReset(resetToken)
      break
  if frame.sequence >= conn.peerCidSequence:
    conn.peerCidSequence = frame.sequence
  if frame.sequence >= conn.activePeerCidSequence:
    conn.activePeerCidSequence = frame.sequence
    conn.peerCid = newCid
    conn.syncPeerCidWithModel()
  elif frame.retirePriorTo > conn.activePeerCidSequence:
    var replacementSeq = 0'u64
    var replacementCid: ConnectionId
    var replacementToken: array[16, uint8]
    if conn.findReplacementPeerCid(replacementSeq, replacementCid, replacementToken):
      conn.activePeerCidSequence = replacementSeq
      conn.peerCid = replacementCid
      conn.syncPeerCidWithModel()
  if frame.retirePriorTo > 0'u64:
    conn.retirePeerConnectionIdsPriorTo(frame.retirePriorTo, conn.activePeerCidSequence)
  if previousActiveSeq != conn.activePeerCidSequence:
    discard conn.sendRetirePeerConnectionId(previousActiveSeq)
  {.cast(gcsafe).}:
    emitDiagnostics(DiagnosticsEvent(
      kind: diagConnectionEvent,
      handle: cast[pointer](conn),
      note: "RX NEW_CONNECTION_ID seq=" & $frame.sequence &
        " retirePriorTo=" & $frame.retirePriorTo &
        " cidLen=" & $frame.connectionId.len
    ))
  true

proc applyPeerRetireConnectionId(conn: ConnectionState; sequence: uint64): bool {.gcsafe.} =
  if conn.isNil or conn.quicConn.isNil:
    return false
  if sequence > conn.localCidSequence:
    return false
  if not conn.markIssuedLocalCidRetired(sequence):
    return false
  if sequence == conn.activeLocalCidSequence:
    var replacementSeq = 0'u64
    var replacementCid: ConnectionId
    if not conn.findReplacementLocalCid(replacementSeq, replacementCid):
      return false
    conn.activeLocalCidSequence = replacementSeq
    conn.localCid = replacementCid
    conn.syncLocalCidWithModel()
  {.cast(gcsafe).}:
    emitDiagnostics(DiagnosticsEvent(
      kind: diagConnectionEvent,
      handle: cast[pointer](conn),
      note: "RX RETIRE_CONNECTION_ID seq=" & $sequence
    ))
  true

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

proc sendZeroRttPacket(conn: ConnectionState; payload: seq[byte];
    frameKind: SentFrameKind; stream: StreamState = nil; streamOffset = 0'u64;
    streamPayload: seq[byte] = @[]; streamFin = false;
    clientContext: pointer = nil;
    targetRemote: ptr TransportAddress = nil): uint64 {.gcsafe.}
proc maybeSendPendingOneRttAck(conn: ConnectionState;
    targetRemote: ptr TransportAddress = nil): bool {.gcsafe.}
proc maybeSendExpiredOneRttAck(conn: ConnectionState; nowUs: uint64;
    targetRemote: ptr TransportAddress = nil): bool {.gcsafe.}
proc completeServerHandshake(conn: ConnectionState; diagnosticsNote: string)

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

proc findNewestAckedAckElicitingPacket(conn: ConnectionState; ack: proto.AckFrame;
    epoch: CryptoEpoch; meta: var SentPacketMeta): bool {.gcsafe.} =
  if conn.isNil:
    return false
  var found = false
  var bestSentTimeUs = 0'u64
  var bestPn = 0'u64
  for sent in conn.sentPackets:
    if sent.epoch != epoch or not sent.ackEliciting:
      continue
    if not packetAckedByFrame(ack, sent.packetNumber):
      continue
    if not found or sent.sentTimeUs > bestSentTimeUs or
        (sent.sentTimeUs == bestSentTimeUs and sent.packetNumber > bestPn):
      meta = sent
      bestSentTimeUs = sent.sentTimeUs
      bestPn = sent.packetNumber
      found = true
  found

proc currentProbeTimeoutUs(conn: ConnectionState; epoch: CryptoEpoch = ceOneRtt): uint64 {.gcsafe.} =
  if conn.isNil:
    return MinProbeTimeoutUs
  let smoothedRtt =
    if conn.smoothedRttUs == 0: DefaultSmoothedRttUs
    else: conn.smoothedRttUs
  let rttVariance =
    if conn.rttVarianceUs == 0: max(smoothedRtt div 2, DefaultRttVarianceUs)
    else: conn.rttVarianceUs
  var ackDelayUs = 0'u64
  if epoch == ceOneRtt and conn.handshakeComplete:
    ackDelayUs = conn.peerTransportParams.maxAckDelayMs * 1_000'u64
    if ackDelayUs == 0'u64:
      ackDelayUs = MaxAckDelayDefaultMs.uint64 * 1_000'u64
  let timeoutUs = qloss.computeProbeTimeout(conn.lossDetection, smoothedRtt, rttVariance, epoch) + ackDelayUs
  max(timeoutUs, MinProbeTimeoutUs)

proc currentLossDelayUs(conn: ConnectionState): uint64 {.gcsafe.} =
  if conn.isNil:
    return timeReorderThreshold(DefaultSmoothedRttUs)
  let observedRtt =
    max(conn.latestRttUs, max(conn.smoothedRttUs, conn.minRttUs))
  let latestOrSmoothed =
    if observedRtt == 0'u64: DefaultSmoothedRttUs
    else: observedRtt
  max(timeReorderThreshold(latestOrSmoothed), 1'u64)

proc persistentCongestionWindowUs(conn: ConnectionState; epoch: CryptoEpoch): uint64 {.gcsafe.} =
  if conn.isNil:
    return MinProbeTimeoutUs * PersistentCongestionThreshold.uint64
  let ptoUs = conn.currentProbeTimeoutUs(epoch)
  max(ptoUs * PersistentCongestionThreshold.uint64, ptoUs)

proc isPersistentCongestion(conn: ConnectionState; epoch: CryptoEpoch; earliestSentUs,
    latestSentUs: uint64): bool {.gcsafe.} =
  if conn.isNil or earliestSentUs == high(uint64) or latestSentUs < earliestSentUs:
    return false
  (latestSentUs - earliestSentUs) >= conn.persistentCongestionWindowUs(epoch)

proc requeueLostPacket(conn: ConnectionState; meta: SentPacketMeta) {.gcsafe.} =
  if conn.isNil:
    return
  case meta.frameKind
  of sfkStream:
    if not meta.stream.isNil:
      let stream = cast[StreamState](meta.stream)
      stream.prependPendingChunk(PendingStreamChunk(
        offset: meta.streamOffset,
        payload: meta.streamPayload,
        fin: meta.streamFin,
        clientContext: meta.clientContext,
        targetRemote: meta.targetRemote
      ))
  of sfkResetStream, sfkStopSending, sfkCrypto, sfkConnectionIdUpdate,
      sfkPathChallenge, sfkPathResponse, sfkHandshakeDone, sfkConnectionClose:
    conn.prependPendingControlFrame(PendingControlFrame(
      epoch: meta.epoch,
      payload: meta.framePayload,
      frameKind: meta.frameKind,
      ackEliciting: meta.ackEliciting,
      targetRemote: meta.targetRemote
    ))
  else:
    discard

proc appendUniqueLostStream(streams: var seq[StreamState];
    stream: StreamState) {.gcsafe.} =
  if stream.isNil:
    return
  for existing in streams:
    if existing == stream:
      return
  streams.add(stream)

proc flushStream(state: StreamState) {.gcsafe.}

proc detectAckDrivenLosses(conn: ConnectionState; ack: proto.AckFrame; epoch: CryptoEpoch;
    nowUs: uint64; lostBytes: var uint32; largestLost: var uint64;
    persistentCongestion: var bool): uint64 {.gcsafe.} =
  if conn.isNil:
    return 0'u64
  let lossDelayUs = conn.currentLossDelayUs()
  var nextLossTime = 0'u64
  var remaining: seq[SentPacketMeta] = @[]
  var lostStreams: seq[StreamState] = @[]
  var earliestLostSentUs = high(uint64)
  var latestLostSentUs = 0'u64
  for meta in conn.sentPackets:
    if meta.epoch == epoch:
      if packetAckedByFrame(ack, meta.packetNumber):
        continue
      if meta.ackEliciting and meta.packetNumber < ack.largestAcked:
        let packetThresholdLoss =
          ack.largestAcked >= meta.packetNumber + PacketReorderThreshold.uint64
        let lossDeadlineUs = meta.sentTimeUs + lossDelayUs
        let timeThresholdLoss =
          lossDeadlineUs >= meta.sentTimeUs and nowUs >= lossDeadlineUs
        if packetThresholdLoss or timeThresholdLoss:
          qloss.markPacketLost(conn.lossDetection, meta.packetNumber, epoch)
          if meta.packetNumber > largestLost:
            largestLost = meta.packetNumber
          lostBytes = lostBytes + meta.packetLength.uint32
          earliestLostSentUs = min(earliestLostSentUs, meta.sentTimeUs)
          latestLostSentUs = max(latestLostSentUs, meta.sentTimeUs)
          conn.requeueLostPacket(meta)
          if meta.frameKind == sfkStream and not meta.stream.isNil:
            lostStreams.appendUniqueLostStream(cast[StreamState](meta.stream))
          {.cast(gcsafe).}:
            emitDiagnostics(DiagnosticsEvent(
              kind: diagConnectionEvent,
              handle: cast[pointer](conn),
              note: "ACK-driven loss pn=" & $meta.packetNumber &
                " epoch=" & $epoch &
                " packetThreshold=" & $packetThresholdLoss &
                " timeThreshold=" & $timeThresholdLoss
            ))
          continue
        if nextLossTime == 0'u64 or lossDeadlineUs < nextLossTime:
          nextLossTime = lossDeadlineUs
    remaining.add(meta)
  conn.sentPackets = remaining
  if not conn.quicConn.isNil:
    conn.quicConn.model.packetSpaces[epoch].lossTime = nextLossTime
  persistentCongestion = conn.isPersistentCongestion(epoch, earliestLostSentUs, latestLostSentUs)
  conn.flushPendingControlFrames()
  for stream in lostStreams:
    flushStream(stream)
  conn.updateLossDetectionTimer()
  nextLossTime

proc detectTimedOutLosses(conn: ConnectionState) {.gcsafe.} =
  if conn.isNil or conn.inLossDetectionTick:
    if not conn.isNil and not conn.quicConn.isNil:
      conn.quicConn.model.timers.lossDetectionTimer = 0'u64
    return
  conn.inLossDetectionTick = true
  defer:
    conn.inLossDetectionTick = false
  let nowUs = nowMicros()
  discard conn.maybeSendExpiredOneRttAck(nowUs)
  if conn.sentPackets.len == 0:
    if not conn.quicConn.isNil:
      conn.quicConn.model.timers.lossDetectionTimer = 0'u64
    return
  var selectedEpoch = ceInitial
  var scheduledUs = 0'u64
  var dueToLossTime = false
  if not conn.selectLossDetectionSchedule(selectedEpoch, scheduledUs, dueToLossTime):
    conn.updateLossDetectionTimer()
    return
  if scheduledUs != 0'u64 and nowUs < scheduledUs:
    conn.updateLossDetectionTimer()
    return
  let probeTimeoutUs = conn.currentProbeTimeoutUs(selectedEpoch)
  let lossDelayUs = conn.currentLossDelayUs()
  if not dueToLossTime:
    qloss.onProbeTimeoutFired(conn.lossDetection, selectedEpoch)
    qcc.onProbeTimeout(conn.congestionController)
    qcc.grantExemptions(conn.congestionController, 2'u8)
    let probeCountSent = conn.sendProbeForEpoch(selectedEpoch)
    {.cast(gcsafe).}:
      emitDiagnostics(DiagnosticsEvent(
        kind: diagConnectionEvent,
        handle: cast[pointer](conn),
        note: "pto fired epoch=" & $selectedEpoch &
          " probesSent=" & $probeCountSent &
          " probeCount=" & $conn.lossDetection.probeCount &
          " epochProbeCount=" & $conn.lossDetection.probeCountByEpoch[selectedEpoch]
      ))
    conn.updateLossDetectionTimer()
    return
  var remaining: seq[SentPacketMeta] = @[]
  var lostStreams: seq[StreamState] = @[]
  var largestLost = 0'u64
  var lostBytes = 0'u32
  var earliestLostSentUs = high(uint64)
  var latestLostSentUs = 0'u64
  for meta in conn.sentPackets:
    if meta.epoch == selectedEpoch and meta.ackEliciting and
        nowUs >= meta.sentTimeUs and nowUs - meta.sentTimeUs >= lossDelayUs:
      qloss.markPacketLost(conn.lossDetection, meta.packetNumber, selectedEpoch)
      if meta.packetNumber > largestLost:
        largestLost = meta.packetNumber
      lostBytes = lostBytes + meta.packetLength.uint32
      earliestLostSentUs = min(earliestLostSentUs, meta.sentTimeUs)
      latestLostSentUs = max(latestLostSentUs, meta.sentTimeUs)
      conn.requeueLostPacket(meta)
      if meta.frameKind == sfkStream and not meta.stream.isNil:
        lostStreams.appendUniqueLostStream(cast[StreamState](meta.stream))
      {.cast(gcsafe).}:
        emitDiagnostics(DiagnosticsEvent(
          kind: diagConnectionEvent,
          handle: cast[pointer](conn),
          note: "loss timer fired pn=" & $meta.packetNumber &
            " epoch=" & $selectedEpoch &
            " mode=loss" &
            " threshold=" & $lossDelayUs
        ))
    else:
      remaining.add(meta)
  conn.sentPackets = remaining
  if not conn.quicConn.isNil:
    conn.quicConn.model.packetSpaces[selectedEpoch].lossTime = 0'u64
  if lostBytes > 0:
    let persistentCongestion =
      conn.isPersistentCongestion(selectedEpoch, earliestLostSentUs, latestLostSentUs)
    let loss = LossEventSnapshot(
      largestPacketNumberLost: largestLost,
      largestSentPacketNumber: conn.lossDetection.largestSentPacketNumber,
      retransmittableBytesLost: lostBytes,
      persistentCongestion: persistentCongestion
    )
    qcc.onLost(conn.congestionController, loss)
    {.cast(gcsafe).}:
      emitDiagnostics(DiagnosticsEvent(
        kind: diagConnectionEvent,
        handle: cast[pointer](conn),
        note: "loss timer summary epoch=" & $selectedEpoch &
          " mode=loss" &
          " lostBytes=" & $lostBytes &
          " probeCount=" & $conn.lossDetection.probeCount &
          " epochProbeCount=" & $conn.lossDetection.probeCountByEpoch[selectedEpoch] &
          " persistent=" & $persistentCongestion
      ))
  conn.flushPendingControlFrames()
  for stream in lostStreams:
    flushStream(stream)
  conn.updateLossDetectionTimer()

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

proc emitConnectionEvent(state: ConnectionState; event: var ConnectionEvent) {.gcsafe.} =
  event.connection = cast[HQUIC](state)
  if state.eventHandlers.len > 0:
    for handler in state.eventHandlers:
      if handler != nil:
        handler(event)
  let diagNote = (if event.note.len > 0: event.note else: $event.kind)
  {.cast(gcsafe).}:
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

proc generateSingleConnectionId(): ConnectionId {.gcsafe.} =
  let base = gNextConnectionId
  inc gNextConnectionId
  initConnectionId(@[
    uint8((base shr 0) and 0xFF),
    uint8((base shr 8) and 0xFF),
    uint8((base shr 16) and 0xFF),
    uint8((base shr 24) and 0xFF)
  ])

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

proc cidBytes(cid: ConnectionId): seq[byte] {.gcsafe.} =
  if cid.length == 0:
    return @[]
  result = newSeq[byte](int(cid.length))
  for i in 0 ..< int(cid.length):
    result[i] = cid.bytes[i]

proc remoteAddressKey(remote: TransportAddress): string {.gcsafe.} =
  $remote

proc wildcardLocalForRemote(remote: TransportAddress): TransportAddress {.gcsafe.} =
  case remote.family
  of AddressFamily.IPv6:
    initTAddress("::", Port(0))
  of AddressFamily.IPv4:
    initTAddress("0.0.0.0", Port(0))
  else:
    initTAddress("0.0.0.0", Port(0))

proc localBindMatchesRemote(localAddr, remoteAddr: TransportAddress): bool {.gcsafe.} =
  if localAddr.family == AddressFamily.None or remoteAddr.family == AddressFamily.None:
    return false
  localAddr.family == remoteAddr.family

proc isRoutableGlobalIpv6(ip: string): bool {.gcsafe.} =
  let normalized = ip.strip().toLowerAscii()
  if normalized.len == 0:
    return false
  if normalized in ["::", "::1", "0:0:0:0:0:0:0:0"]:
    return false
  if normalized.startsWith("fe80:") or normalized.startsWith("fc") or normalized.startsWith("fd"):
    return false
  if normalized.startsWith("ff"):
    return false
  true

proc preferredIpv6LocalBindForDial(): TransportAddress {.gcsafe.} =
  when defined(macosx):
    when declared(getifaddrs):
      var ifap: ptr Ifaddrs = nil
      if getifaddrs(addr ifap) == 0:
        defer:
          freeifaddrs(ifap)
        var cursor = ifap
        while cursor != nil:
          when declared(cursor.ifa_flags) and declared(IFF_LOOPBACK):
            if (cursor.ifa_flags and IFF_LOOPBACK) != 0:
              cursor = cursor.ifa_next
              continue
          let addrPtr = cursor.ifa_addr
          when declared(posix.AF_INET6):
            if addrPtr != nil and cint(addrPtr.sa_family) == posix.AF_INET6:
              let sin6 = cast[ptr Sockaddr_in6](addrPtr)
              var buffer: array[64, char]
              when declared(inet_ntop):
                if inet_ntop(posix.AF_INET6, addr sin6.sin6_addr, cast[ptr char](addr buffer[0]), buffer.len.cint) != nil:
                  var ip = $cast[cstring](addr buffer[0])
                  let zoneIdx = ip.find('%')
                  if zoneIdx >= 0:
                    ip = ip[0 ..< zoneIdx]
                  if isRoutableGlobalIpv6(ip):
                    return initTAddress(ip, Port(0))
          cursor = cursor.ifa_next
  TransportAddress(family: AddressFamily.None)

proc selectClientLocalBind(state: ConnectionState): TransportAddress {.gcsafe.} =
  if state.isNil:
    return TransportAddress(family: AddressFamily.None)
  if state.remoteAddress.family == AddressFamily.None:
    if state.localAddress.family != AddressFamily.None:
      return state.localAddress
    return wildcardLocalForRemote(state.remoteAddress)
  if localBindMatchesRemote(state.localAddress, state.remoteAddress):
    return state.localAddress
  if state.remoteAddress.family == AddressFamily.IPv6:
    let preferredIpv6 = preferredIpv6LocalBindForDial()
    if preferredIpv6.family != AddressFamily.None:
      state.localAddress = preferredIpv6
      return preferredIpv6
  result = wildcardLocalForRemote(state.remoteAddress)
  if result.family != AddressFamily.None:
    state.localAddress = result

proc buildCryptoFrame(data: seq[byte]): seq[byte] =
  result = @[0x06'u8]
  proto.writeVarInt(result, 0'u64)
  proto.writeVarInt(result, uint64(data.len))
  result.add(data)

proc byteSeqEqual(lhs, rhs: openArray[byte]): bool =
  if lhs.len != rhs.len:
    return false
  for idx in 0 ..< lhs.len:
    if lhs[idx] != rhs[idx]:
      return false
  true

proc connectionZeroRttMaterialReady(conn: ConnectionState): bool =
  not conn.isNil and
    conn.zeroRttMaterial.key.len >= 16 and
    conn.zeroRttMaterial.iv.len >= 12 and
    conn.zeroRttMaterial.hp.len >= 16 and
    conn.zeroRttMaxData > 0'u32 and
    (conn.zeroRttAcceptDeadlineUs == 0'u64 or nowMicros() < conn.zeroRttAcceptDeadlineUs)

proc applyZeroRttTicketState(conn: ConnectionState; ticket: openArray[byte]): bool {.gcsafe.} =
  if conn.isNil or ticket.len == 0:
    return false
  var meta: BuiltinResumptionTicketMeta
  if not parseBuiltinResumptionTicket(ticket, meta):
    return false
  conn.zeroRttMaterial = tls.ZeroRttMaterial()
  conn.zeroRttSentBytes = 0'u64
  conn.zeroRttMaxData = meta.maxEarlyData
  conn.zeroRttAcceptDeadlineUs = builtinZeroRttDeadline(meta)
  if not builtinZeroRttEligible(meta):
    return false
  conn.zeroRttMaterial = tls.deriveBuiltinZeroRttMaterial(ticket)
  true

proc zeroRttBudgetAvailable(conn: ConnectionState; nextPayloadBytes: int): bool {.gcsafe.} =
  if conn.isNil or nextPayloadBytes < 0:
    return false
  if conn.zeroRttMaxData == 0'u32:
    return false
  if conn.zeroRttAcceptDeadlineUs > 0'u64 and nowMicros() >= conn.zeroRttAcceptDeadlineUs:
    return false
  conn.zeroRttSentBytes + uint64(nextPayloadBytes) <= uint64(conn.zeroRttMaxData)

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

proc emitReceiveBufferNeeded(state: StreamState; needed: uint64) =
  if state.isNil:
    return
  if not state.callback.isNil:
    var native = QuicStreamEvent(Type: QUIC_STREAM_EVENT_RECEIVE_BUFFER_NEEDED, Padding: 0)
    var payload = QuicStreamEventReceiveBufferNeededPayload(
      BufferLengthNeeded: needed
    )
    copyMem(addr native.Data[0], addr payload, sizeof(payload))
    discard state.callback(cast[HQUIC](state), state.callbackContext, addr native)
  var ev = StreamEvent(kind: seReceiveBufferNeeded, bufferLengthNeeded: needed)
  emitStreamEvent(state, ev)

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
  if payloadData.len > 0:
    let payloadLen = uint64(payloadData.len)
    if state.receiveBuffersProvided > payloadLen:
      state.receiveBuffersProvided -= payloadLen
    else:
      state.receiveBuffersProvided = 0'u64
      emitReceiveBufferNeeded(state, payloadLen)
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
  if not conn.inLossDetectionTick:
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
  let ciphertext =
    if conn.disable1RttEncryption:
      payload
    else:
      tls.encryptPacket(sendMat.key, sendMat.iv, pn, header, payload, tag)
  var packet = header & ciphertext
  if conn.disable1RttEncryption:
    packet.add(newSeq[byte](16))
  else:
    packet.add(@tag)

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
  let packetLength = uint16(min(packet.len, high(uint16).int))
  conn.recordSentPacket(SentPacketMeta(
    packetNumber: pn,
    epoch: ceOneRtt,
    ackEliciting: ackEliciting,
    appLimited: false,
    packetLength: packetLength,
    sentTimeUs: nowUs,
    totalBytesSentAtSend: conn.lossDetection.totalBytesSent + uint64(packetLength),
    frameKind: frameKind,
    framePayload: payload,
    stream: cast[pointer](stream),
    streamOffset: streamOffset,
    streamPayload: streamPayload,
    streamFin: streamFin,
    clientContext: clientContext,
    targetRemote: sendRemote
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

proc sendZeroRttPacket(conn: ConnectionState; payload: seq[byte];
    frameKind: SentFrameKind; stream: StreamState = nil; streamOffset = 0'u64;
    streamPayload: seq[byte] = @[]; streamFin = false;
    clientContext: pointer = nil;
    targetRemote: ptr TransportAddress = nil): uint64 {.gcsafe.} =
  if conn.isNil or conn.transport.isNil or not conn.connectionZeroRttMaterialReady() or
      not conn.zeroRttBudgetAvailable(payload.len):
    if not conn.isNil:
      {.cast(gcsafe).}:
        emitDiagnostics(DiagnosticsEvent(
          kind: diagConnectionEvent,
          handle: cast[pointer](conn),
          note: "TX 0-RTT suppressed budget/window kind=" & $frameKind
        ))
    return 0'u64
  if not conn.inLossDetectionTick:
    conn.detectTimedOutLosses()
  if not qcc.canSend(conn.congestionController):
    {.cast(gcsafe).}:
      emitDiagnostics(DiagnosticsEvent(
        kind: diagConnectionEvent,
        handle: cast[pointer](conn),
        note: "TX 0-RTT blocked by congestion controller"
      ))
    return 0'u64
  let nowUs = nowMicros()
  let allowance = qcc.sendAllowance(conn.congestionController, nowUs)
  if allowance > 0 and uint64(payload.len) > allowance:
    {.cast(gcsafe).}:
      emitDiagnostics(DiagnosticsEvent(
        kind: diagConnectionEvent,
        handle: cast[pointer](conn),
        note: "TX 0-RTT exceeds send allowance=" & $allowance
      ))
    return 0'u64
  let pn = uint64(conn.quicConn.model.packetSpaces[ceZeroRtt].nextPacketNumber())
  let destCid = cidBytes(conn.peerCid)
  let srcCid = cidBytes(conn.localCid)
  var header: seq[byte] = @[0xD3'u8]
  header.writeUint32(1'u32)
  header.add(byte(destCid.len))
  header.add(destCid)
  header.add(byte(srcCid.len))
  header.add(srcCid)
  proto.writeVarInt(header, 4'u64 + uint64(payload.len + 16))
  let pnOffset = header.len
  header.writeUint32(uint32(pn))

  var tag: array[16, byte]
  let ciphertext = tls.encryptPacket(
    conn.zeroRttMaterial.key,
    conn.zeroRttMaterial.iv,
    pn,
    header,
    payload,
    tag
  )
  var packet = header & ciphertext & @tag
  if packet.len >= pnOffset + 4 + 16:
    let sample = packet[pnOffset + 4 ..< pnOffset + 4 + 16]
    var pnSlice: array[4, byte]
    for i in 0 .. 3:
      pnSlice[i] = packet[pnOffset + i]
    tls.applyHeaderProtection(conn.zeroRttMaterial.hp, sample, packet[0], pnSlice)
    for i in 0 .. 3:
      packet[pnOffset + i] = pnSlice[i]

  let sendRemote =
    if targetRemote.isNil: conn.remoteAddress
    else: targetRemote[]
  asyncCheck conn.transport.sendTo(sendRemote, packet)
  let packetLength = uint16(min(packet.len, high(uint16).int))
  conn.recordSentPacket(SentPacketMeta(
    packetNumber: pn,
    epoch: ceZeroRtt,
    ackEliciting: true,
    appLimited: false,
    packetLength: packetLength,
    sentTimeUs: nowUs,
    totalBytesSentAtSend: conn.lossDetection.totalBytesSent + uint64(packetLength),
    frameKind: frameKind,
    framePayload: payload,
    stream: cast[pointer](stream),
    streamOffset: streamOffset,
    streamPayload: streamPayload,
    streamFin: streamFin,
    clientContext: clientContext,
    targetRemote: sendRemote
  ))
  qcc.onPacketSent(
    conn.congestionController,
    uint32(min(packet.len, high(uint32).int)),
    true,
    false,
    nowUs
  )
  {.cast(gcsafe).}:
    emitDiagnostics(DiagnosticsEvent(
      kind: diagConnectionEvent,
      handle: cast[pointer](conn),
      note: "TX 0-RTT pn=" & $pn & " kind=" & $frameKind
    ))
  conn.zeroRttSentBytes += uint64(payload.len)
  pn

proc sendInitialPacket(conn: ConnectionState; payload: seq[byte];
    frameKind: SentFrameKind; ackEliciting: bool;
    targetRemote: ptr TransportAddress = nil): uint64 {.gcsafe.} =
  if conn.isNil or conn.transport.isNil:
    return 0'u64
  if not conn.inLossDetectionTick:
    conn.detectTimedOutLosses()
  if ackEliciting and not qcc.canSend(conn.congestionController):
    {.cast(gcsafe).}:
      emitDiagnostics(DiagnosticsEvent(
        kind: diagConnectionEvent,
        handle: cast[pointer](conn),
        note: "TX Initial blocked by congestion controller"
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
          note: "TX Initial exceeds send allowance=" & $allowance
        ))
      return 0'u64
  let sendMat: tuple[key, iv, hp: seq[byte]] =
    if conn.quicConn.isNil:
      (@[], @[], @[])
    elif conn.quicConn.role == crServer:
      (conn.initialSecrets.serverKey, conn.initialSecrets.serverIv, conn.initialSecrets.serverHp)
    else:
      (conn.initialSecrets.clientKey, conn.initialSecrets.clientIv, conn.initialSecrets.clientHp)
  if sendMat.key.len < 16 or sendMat.iv.len < 12 or sendMat.hp.len < 16:
    return 0'u64
  let pn = uint64(conn.quicConn.model.packetSpaces[ceInitial].nextPacketNumber())
  var packet = proto.encodeInitialPacket(
    cidBytes(conn.peerCid),
    cidBytes(conn.localCid),
    @[],
    payload,
    uint32(pn)
  )
  let header = proto.parseUnprotectedHeader(packet)
  let pnOffset = header.payloadOffset
  if pnOffset + 4 > packet.len:
    return 0'u64
  let aad = packet[0 ..< pnOffset + 4]
  let plaintext = packet[pnOffset + 4 .. ^1]
  var tag: array[16, byte]
  let ciphertext = tls.encryptPacket(sendMat.key, sendMat.iv, pn, aad, plaintext, tag)
  packet.setLen(pnOffset + 4)
  packet.add(ciphertext)
  packet.add(@tag)
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
  let packetLength = uint16(min(packet.len, high(uint16).int))
  conn.recordSentPacket(SentPacketMeta(
    packetNumber: pn,
    epoch: ceInitial,
    ackEliciting: ackEliciting,
    appLimited: false,
    packetLength: packetLength,
    sentTimeUs: nowUs,
    totalBytesSentAtSend: conn.lossDetection.totalBytesSent + uint64(packetLength),
    frameKind: frameKind,
    framePayload: payload,
    targetRemote: sendRemote
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
      note: "TX Initial pn=" & $pn & " kind=" & $frameKind
    ))
  pn

proc sendHandshakePacket(conn: ConnectionState; payload: seq[byte];
    frameKind: SentFrameKind; ackEliciting: bool;
    targetRemote: ptr TransportAddress = nil): uint64 {.gcsafe.} =
  if conn.isNil or conn.transport.isNil or conn.handshakeKeys.clientKey.len < 16:
    return 0'u64
  if not conn.inLossDetectionTick:
    conn.detectTimedOutLosses()
  if ackEliciting and not qcc.canSend(conn.congestionController):
    {.cast(gcsafe).}:
      emitDiagnostics(DiagnosticsEvent(
        kind: diagConnectionEvent,
        handle: cast[pointer](conn),
        note: "TX Handshake blocked by congestion controller"
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
          note: "TX Handshake exceeds send allowance=" & $allowance
        ))
      return 0'u64
  let sendMat: tuple[key, iv, hp: seq[byte]] =
    if conn.quicConn.isNil:
      (@[], @[], @[])
    elif conn.quicConn.role == crServer:
      (conn.handshakeKeys.serverKey, conn.handshakeKeys.serverIv, conn.handshakeKeys.serverHp)
    else:
      (conn.handshakeKeys.clientKey, conn.handshakeKeys.clientIv, conn.handshakeKeys.clientHp)
  if sendMat.key.len < 16 or sendMat.iv.len < 12 or sendMat.hp.len < 16:
    return 0'u64
  let pn = uint64(conn.quicConn.model.packetSpaces[ceHandshake].nextPacketNumber())
  var packet = proto.encodeHandshakePacket(
    cidBytes(conn.peerCid),
    cidBytes(conn.localCid),
    payload,
    uint32(pn)
  )
  let header = proto.parseUnprotectedHeader(packet)
  let pnOffset = header.payloadOffset
  if pnOffset + 4 > packet.len:
    return 0'u64
  let aad = packet[0 ..< pnOffset + 4]
  let plaintext = packet[pnOffset + 4 .. ^1]
  var tag: array[16, byte]
  let ciphertext = tls.encryptPacket(sendMat.key, sendMat.iv, pn, aad, plaintext, tag)
  packet.setLen(pnOffset + 4)
  packet.add(ciphertext)
  packet.add(@tag)
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
  let packetLength = uint16(min(packet.len, high(uint16).int))
  conn.recordSentPacket(SentPacketMeta(
    packetNumber: pn,
    epoch: ceHandshake,
    ackEliciting: ackEliciting,
    appLimited: false,
    packetLength: packetLength,
    sentTimeUs: nowUs,
    totalBytesSentAtSend: conn.lossDetection.totalBytesSent + uint64(packetLength),
    frameKind: frameKind,
    framePayload: payload,
    targetRemote: sendRemote
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
      note: "TX Handshake pn=" & $pn & " kind=" & $frameKind
    ))
  pn

proc applyAckFrame(conn: ConnectionState; ack: proto.AckFrame; epoch: CryptoEpoch) {.gcsafe.} =
  if conn.isNil:
    return
  var ackedBytes = 0'u32
  var ackedMeta: SentPacketMeta
  var ackedMetaFound = conn.findNewestAckedAckElicitingPacket(ack, epoch, ackedMeta)
  var ackedPacketNumbers: seq[uint64] = @[]
  for meta in conn.sentPackets:
    if meta.epoch == epoch and packetAckedByFrame(ack, meta.packetNumber):
      ackedPacketNumbers.add(meta.packetNumber)
  for packetNumber in ackedPacketNumbers:
    discard qloss.markPacketAcked(conn.lossDetection, packetNumber, epoch, ackedBytes)
  let nowUs = nowMicros()
  var ackDelayUs = 0'u64
  if epoch == ceOneRtt and conn.handshakeComplete:
    let exponent = uint64(min(conn.peerTransportParams.ackDelayExponent, 20'u64))
    ackDelayUs = ack.delay shl exponent
    var maxAckDelayUs = conn.peerTransportParams.maxAckDelayMs * 1_000'u64
    if maxAckDelayUs == 0'u64:
      maxAckDelayUs = MaxAckDelayDefaultMs.uint64 * 1_000'u64
    if ackDelayUs > maxAckDelayUs:
      ackDelayUs = maxAckDelayUs
  if ackedMetaFound and ackedMeta.sentTimeUs > 0 and nowUs >= ackedMeta.sentTimeUs:
    var sampleRtt = nowUs - ackedMeta.sentTimeUs
    if epoch == ceOneRtt and conn.minRttUs > 0 and sampleRtt > conn.minRttUs + ackDelayUs:
      sampleRtt -= ackDelayUs
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
  var lostBytes = 0'u32
  var largestLost = 0'u64
  var persistentCongestion = false
  discard conn.detectAckDrivenLosses(
    ack, epoch, nowUs, lostBytes, largestLost, persistentCongestion)
  if not conn.quicConn.isNil:
    conn.quicConn.model.packetSpaces[epoch].recordAckedPacket(ack.largestAcked)
  conn.latestAckedPacket = max(conn.latestAckedPacket, ack.largestAcked)
  let ackEvent = AckEventSnapshot(
    timeNow: nowUs,
    ackEpoch: epoch,
    largestAck: ack.largestAcked,
    largestSentPacketNumber: conn.lossDetection.largestSentPacketNumber,
    timeOfLargestAckedPacketSent: ackedMeta.sentTimeUs,
    totalBytesSentAtLargestAck: ackedMeta.totalBytesSentAtSend,
    totalAckedRetransmittableBytes: conn.lossDetection.totalBytesAcked + uint64(ackedBytes),
    ackedRetransmittableBytes: ackedBytes,
    smoothedRtt: max(conn.smoothedRttUs, 1'u64),
    minRtt: max(conn.minRttUs, 1'u64),
    oneWayDelay: 0'u64,
    adjustedAckTime: nowUs - min(nowUs, ackDelayUs),
    implicitAck: false,
    hasLoss: lostBytes > 0,
    largestAckAppLimited: ackedMetaFound and ackedMeta.appLimited,
    minRttValid: true
  )
  let preserveEpochProbeBackoff =
    epoch == ceInitial and conn.isClient and not conn.peerAddressValidated and
    ackedBytes > 0'u32
  let savedEpochProbeCount =
    if preserveEpochProbeBackoff:
      conn.lossDetection.probeCountByEpoch[epoch]
    else:
      0'u16
  qloss.onAckReceived(conn.lossDetection, ackEvent)
  if preserveEpochProbeBackoff:
    conn.lossDetection.probeCountByEpoch[epoch] = savedEpochProbeCount
    conn.lossDetection.probeCount = 0'u16
    for candidate in CryptoEpoch:
      conn.lossDetection.probeCount =
        max(conn.lossDetection.probeCount, conn.lossDetection.probeCountByEpoch[candidate])
  qcc.onAcked(conn.congestionController, ackEvent)
  if lostBytes > 0:
    let loss = LossEventSnapshot(
      largestPacketNumberLost: largestLost,
      largestSentPacketNumber: conn.lossDetection.largestSentPacketNumber,
      retransmittableBytesLost: lostBytes,
      persistentCongestion: persistentCongestion
    )
    qcc.onLost(conn.congestionController, loss)
  {.cast(gcsafe).}:
    emitDiagnostics(DiagnosticsEvent(
      kind: diagConnectionEvent,
      handle: cast[pointer](conn),
      note: "RX ACK largest=" & $ack.largestAcked &
        " ackedBytes=" & $ackedBytes &
        " lostBytes=" & $lostBytes &
        " persistent=" & $persistentCongestion
    ))
  for stream in conn.localStreams:
    flushStream(stream)
  flushPendingDatagrams(conn)
  conn.updateLossDetectionTimer()
  if epoch == ceOneRtt and conn.handshakeComplete and conn.pendingAutoLocalCidAdvertisement:
    if conn.sendLocalNewConnectionId():
      conn.pendingAutoLocalCidAdvertisement = false
      {.cast(gcsafe).}:
        emitDiagnostics(DiagnosticsEvent(
          kind: diagConnectionEvent,
          handle: cast[pointer](conn),
          note: "auto local CID advertised after post-handshake ACK"
        ))

proc sendAckFrame(conn: ConnectionState; largestAcked: uint64; epoch: CryptoEpoch;
    targetRemote: ptr TransportAddress = nil): bool {.gcsafe.} =
  if conn.isNil:
    return false
  let effectiveTargetRemote =
    if targetRemote.isNil: conn.pendingAckRemote(epoch)
    else: targetRemote
  var ackDelay = 0'u64
  if epoch == ceOneRtt and conn.handshakeComplete and
      qack.largestPacketNumberRecvTime(conn.ackTracker, epoch) > 0'u64:
    let nowUs = nowMicros()
    let recvTimeUs = qack.largestPacketNumberRecvTime(conn.ackTracker, epoch)
    if nowUs > recvTimeUs:
      ackDelay = nowUs - recvTimeUs
    var maxAckDelayUs = conn.localTransportParams.maxAckDelayMs * 1_000'u64
    if maxAckDelayUs == 0'u64:
      maxAckDelayUs = MaxAckDelayDefaultMs.uint64 * 1_000'u64
    if ackDelay > maxAckDelayUs:
      ackDelay = maxAckDelayUs
    let exponent = uint64(min(conn.localTransportParams.ackDelayExponent, 20'u64))
    ackDelay = ackDelay shr exponent
  var ackRanges: seq[proto.AckRange] = @[]
  for packetRange in qack.packetNumbersToAck(conn.ackTracker, epoch):
    ackRanges.add(proto.AckRange(
      smallest: packetRange.first,
      largest: packetRange.last
    ))
  let ackPayload =
    if ackRanges.len > 0:
      proto.encodeAckFrame(ackRanges, ackDelay)
    else:
      @[]
  let sentCountBefore = conn.sentPackets.len
  let hadPendingAck =
    qack.ackElicitingPacketsToAck(conn.ackTracker, epoch) > 0'u16 or
    qack.packetNumbersToAck(conn.ackTracker, epoch).len > 0
  if not hadPendingAck or ackPayload.len == 0:
    return false
  if hadPendingAck:
    qack.setAlreadyWrittenAckFrame(conn.ackTracker, epoch, true)
  case epoch
  of ceInitial:
    discard conn.sendInitialPacket(ackPayload, sfkAck, false, targetRemote = effectiveTargetRemote)
  of ceHandshake:
    discard conn.sendHandshakePacket(ackPayload, sfkAck, false, targetRemote = effectiveTargetRemote)
  of ceOneRtt:
    if not conn.handshakeComplete:
      if hadPendingAck:
        qack.setAlreadyWrittenAckFrame(conn.ackTracker, epoch, false)
      return
    discard conn.sendOneRttPacket(ackPayload, sfkAck, false, targetRemote = effectiveTargetRemote)
  else:
    discard
  if conn.sentPackets.len > sentCountBefore:
    qack.consumePendingAck(conn.ackTracker, epoch)
    conn.clearPendingAckRemote(epoch)
  elif hadPendingAck:
    qack.setAlreadyWrittenAckFrame(conn.ackTracker, epoch, false)
  conn.sentPackets.len > sentCountBefore

proc maybeSendPendingOneRttAck(conn: ConnectionState;
    targetRemote: ptr TransportAddress = nil): bool {.gcsafe.} =
  if conn.isNil or not conn.handshakeComplete:
    return false
  if qack.ackElicitingPacketsToAck(conn.ackTracker, ceOneRtt) == 0'u16:
    return false
  result = conn.sendAckFrame(
    qack.largestPacketNumberAcked(conn.ackTracker, ceOneRtt),
    ceOneRtt,
    targetRemote)

proc maybeSendExpiredOneRttAck(conn: ConnectionState; nowUs: uint64;
    targetRemote: ptr TransportAddress = nil): bool {.gcsafe.} =
  if conn.isNil or not conn.handshakeComplete:
    return false
  if qack.ackElicitingPacketsToAck(conn.ackTracker, ceOneRtt) == 0'u16:
    return false
  if qack.alreadyWrittenAckFrame(conn.ackTracker, ceOneRtt):
    return false
  let deadlineUs = qack.ackDeadlineUs(conn.ackTracker, ceOneRtt)
  if deadlineUs == 0'u64 or nowUs < deadlineUs:
    return false
  result = conn.sendAckFrame(
    qack.largestPacketNumberAcked(conn.ackTracker, ceOneRtt),
    ceOneRtt,
    targetRemote)

proc completeServerHandshake(conn: ConnectionState; diagnosticsNote: string) =
  if conn.isNil or conn.handshakeComplete:
    return
  conn.handshakeComplete = true
  conn.markActivePathValidated(0)
  for stream in conn.localStreams:
    flushStream(stream)
  flushPendingDatagrams(conn)
  emitNativeConnected(conn)
  let negotiatedAlpn =
    if conn.configuration.isNil or conn.configuration.alpns.len == 0: ""
    else: conn.configuration.alpns[0]
  var connectedEvent = ConnectionEvent(
    kind: ceConnected,
    sessionResumed: conn.sessionResumed,
    negotiatedAlpn: negotiatedAlpn,
    note: "server handshake complete"
  )
  when defined(msquic_diag_test):
    {.cast(gcsafe).}:
      emitDiagnostics(DiagnosticsEvent(
        kind: diagConnectionEvent,
        handle: cast[pointer](conn),
        note: "server emit ceConnected resumed=" & $conn.sessionResumed
      ))
  emitConnectionEvent(conn, connectedEvent)
  if conn.resumptionTicket.len == 0:
    conn.resumptionTicket = buildBuiltinResumptionTicket(conn, conn.resumptionData)
  var postHandshakePayload = proto.encodeHandshakeDoneFrame()
  if conn.resumptionTicket.len > 0:
    let ticketLifetimeSec = currentBuiltinNewSessionTicketLifetimeSec()
    let ticketMaxEarlyData = currentBuiltinNewSessionTicketMaxEarlyData(conn)
    var ticketMeta: BuiltinResumptionTicketMeta
    let haveTicketMeta = parseBuiltinResumptionTicket(conn.resumptionTicket, ticketMeta)
    let issuedAtUs = nowMicros()
    let expiresAtUs =
      if ticketLifetimeSec == 0'u32: issuedAtUs
      else: issuedAtUs + uint64(ticketLifetimeSec) * 1_000_000'u64
    conn.resumptionTicket = buildBuiltinResumptionTicket(
      if haveTicketMeta and ticketMeta.serverName.len > 0:
        ticketMeta.serverName
      else:
        conn.serverName,
      if haveTicketMeta and ticketMeta.serverPort != 0'u16:
        ticketMeta.serverPort
      else:
        conn.serverPort,
      if haveTicketMeta and ticketMeta.alpn.len > 0:
        ticketMeta.alpn
      else:
        alpnForConnection(conn),
      if haveTicketMeta: ticketMeta.data else: conn.resumptionData,
      issuedAtUs = issuedAtUs,
      expiresAtUs = expiresAtUs,
      maxEarlyData = ticketMaxEarlyData
    )
    var refreshedTicketMeta: BuiltinResumptionTicketMeta
    discard parseBuiltinResumptionTicket(conn.resumptionTicket, refreshedTicketMeta)
    {.cast(gcsafe).}:
      storeBuiltinResumptionEntry(
        conn,
        conn.resumptionTicket,
        conn.resumptionData,
        int64(ticketLifetimeSec)
      )
    postHandshakePayload.add(buildCryptoFrame(
      tls.encodeNewSessionTicket(
        conn.resumptionTicket,
        maxEarlyData = ticketMaxEarlyData,
        ticketAgeAdd = refreshedTicketMeta.ticketAgeAdd,
        ticketLifetimeSec = ticketLifetimeSec
      )
    ))
  discard conn.sendOneRttPacket(postHandshakePayload, sfkHandshakeDone, true)
  discard conn.maybeSendPendingOneRttAck()
  conn.pendingAutoLocalCidAdvertisement = true
  {.cast(gcsafe).}:
    emitDiagnostics(DiagnosticsEvent(
      kind: diagConnectionEvent,
      handle: cast[pointer](conn),
      note: diagnosticsNote
    ))

proc currentLocalAckDelayMs(conn: ConnectionState): uint32 {.gcsafe.} =
  if conn.isNil:
    return MaxAckDelayDefaultMs
  let configured = uint32(min(conn.localTransportParams.maxAckDelayMs, high(uint32).uint64))
  if configured == 0'u32: MaxAckDelayDefaultMs else: configured

proc sendProbeForEpoch(conn: ConnectionState; epoch: CryptoEpoch): uint8 {.gcsafe.} =
  if conn.isNil:
    return 0'u8
  let genericProbePayload = @[0x01'u8] # PING
  var probeMeta: SentPacketMeta
  let haveProbeMeta = conn.newestAckElicitingSentPacket(epoch, probeMeta)
  let reuseOriginalFrame =
    haveProbeMeta and probeMeta.framePayload.len > 0 and probeMeta.frameKind != sfkDatagram
  let probePayload =
    if reuseOriginalFrame: probeMeta.framePayload
    else: genericProbePayload
  let probeFrameKind =
    if reuseOriginalFrame: probeMeta.frameKind
    else: sfkPing
  let probeStream =
    if reuseOriginalFrame and probeMeta.frameKind == sfkStream:
      cast[StreamState](probeMeta.stream)
    else:
      nil
  let probeStreamOffset =
    if reuseOriginalFrame and probeMeta.frameKind == sfkStream:
      probeMeta.streamOffset
    else:
      0'u64
  let probeStreamPayload =
    if reuseOriginalFrame and probeMeta.frameKind == sfkStream:
      probeMeta.streamPayload
    else:
      @[]
  let probeStreamFin =
    reuseOriginalFrame and probeMeta.frameKind == sfkStream and probeMeta.streamFin
  let probeClientContext =
    if reuseOriginalFrame and probeMeta.frameKind in {sfkStream, sfkDatagram}:
      probeMeta.clientContext
    else:
      nil
  for _ in 0 ..< 2:
    let packetNumber =
      case epoch
      of ceInitial:
        conn.sendInitialPacket(
          probePayload,
          probeFrameKind,
          true,
          (if haveProbeMeta and probeMeta.targetRemote.family != AddressFamily.None:
            addr probeMeta.targetRemote
          else:
            nil)
        )
      of ceHandshake:
        conn.sendHandshakePacket(
          probePayload,
          probeFrameKind,
          true,
          (if haveProbeMeta and probeMeta.targetRemote.family != AddressFamily.None:
            addr probeMeta.targetRemote
          else:
            nil)
        )
      of ceZeroRtt:
        let probeTarget =
          if haveProbeMeta and probeMeta.targetRemote.family != AddressFamily.None:
            addr probeMeta.targetRemote
          else:
            nil
        conn.sendZeroRttPacket(
          probePayload,
          probeFrameKind,
          stream = probeStream,
          streamOffset = probeStreamOffset,
          streamPayload = probeStreamPayload,
          streamFin = probeStreamFin,
          clientContext = probeClientContext,
          targetRemote = probeTarget
        )
      of ceOneRtt:
        if not conn.handshakeComplete:
          0'u64
        else:
          let probeTarget =
            if haveProbeMeta and probeMeta.targetRemote.family != AddressFamily.None:
            addr probeMeta.targetRemote
          else:
            nil
          conn.sendOneRttPacket(
            probePayload,
            probeFrameKind,
            true,
            stream = probeStream,
            streamOffset = probeStreamOffset,
            streamPayload = probeStreamPayload,
            streamFin = probeStreamFin,
            clientContext = probeClientContext,
            targetRemote = probeTarget
          )
    if packetNumber == 0'u64:
      break
    inc result

proc flushPendingDatagrams(conn: ConnectionState) {.gcsafe.} =
  if conn.isNil or not conn.handshakeComplete or conn.pendingDatagrams.len == 0:
    return
  var pending = conn.pendingDatagrams
  conn.pendingDatagrams = @[]
  for datagram in pending:
    if conn.quicConn.model.handshake.zeroRttAccepted and datagram.zeroRttDispatched:
      continue
    let frame = proto.encodeDatagramFrame(datagram.payload, true)
    var targetRemote = datagram.targetRemote
    discard conn.sendOneRttPacket(
      frame,
      sfkDatagram,
      true,
      clientContext = datagram.clientContext,
      targetRemote =
        (if targetRemote.family == AddressFamily.None: nil
        else: unsafeAddr targetRemote)
    )

proc flushPendingControlFrames(conn: ConnectionState) {.gcsafe.} =
  if conn.isNil or conn.pendingControlFrames.len == 0:
    return
  var pending = conn.pendingControlFrames
  conn.pendingControlFrames = @[]
  for frame in pending:
    var targetRemote = frame.targetRemote
    let targetPtr =
      if targetRemote.family == AddressFamily.None: nil
      else: addr targetRemote
    let packetNumber =
      case frame.epoch
      of ceInitial:
        conn.sendInitialPacket(frame.payload, frame.frameKind, frame.ackEliciting, targetPtr)
      of ceHandshake:
        conn.sendHandshakePacket(frame.payload, frame.frameKind, frame.ackEliciting, targetPtr)
      of ceOneRtt:
        conn.sendOneRttPacket(
          frame.payload,
          frame.frameKind,
          frame.ackEliciting,
          targetRemote = targetPtr
        )
      of ceZeroRtt:
        0'u64
    if packetNumber == 0'u64:
      conn.pendingControlFrames.add(frame)

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
  of QUIC_PARAM_LISTENER_LOCAL_ADDRESS:
    let state = toListener(handle)
    if state.isNil or buffer.isNil:
      return QUIC_STATUS_INVALID_PARAMETER
    var addrValue: TransportAddress
    if not parseSockAddr(buffer, addrValue):
      return QUIC_STATUS_INVALID_PARAMETER
    setListenerLocalAddress(state, addrValue)
  of QUIC_PARAM_CONN_LOCAL_ADDRESS:
    let state = toConnection(handle)
    if state.isNil or buffer.isNil:
      return QUIC_STATUS_INVALID_PARAMETER
    var addrValue: TransportAddress
    if not parseSockAddr(buffer, addrValue):
      return QUIC_STATUS_INVALID_PARAMETER
    setConnectionLocalAddress(state, addrValue)
  of QUIC_PARAM_CONN_REMOTE_ADDRESS:
    let state = toConnection(handle)
    if state.isNil or buffer.isNil:
      return QUIC_STATUS_INVALID_PARAMETER
    var addrValue: TransportAddress
    if not parseSockAddr(buffer, addrValue):
      return QUIC_STATUS_INVALID_PARAMETER
    setConnectionRemoteAddress(state, addrValue)
  else:
    if bufferLength == 0 or buffer.isNil:
      return QUIC_STATUS_INVALID_PARAMETER
    QUIC_STATUS_NOT_SUPPORTED

proc msquicGetParam(handle: HQUIC; param: uint32; bufferLength: ptr uint32;
    buffer: pointer): QUIC_STATUS {.cdecl.} =
  if handle.isNil or bufferLength.isNil:
    return QUIC_STATUS_INVALID_PARAMETER
  case param
  of QUIC_PARAM_LISTENER_LOCAL_ADDRESS:
    let state = toListener(handle)
    if state.isNil:
      return QUIC_STATUS_INVALID_PARAMETER
    writeSockAddr(state.localAddress, bufferLength, buffer)
  of QUIC_PARAM_CONN_LOCAL_ADDRESS:
    let state = toConnection(handle)
    if state.isNil:
      return QUIC_STATUS_INVALID_PARAMETER
    writeSockAddr(state.localAddress, bufferLength, buffer)
  of QUIC_PARAM_CONN_REMOTE_ADDRESS:
    let state = toConnection(handle)
    if state.isNil:
      return QUIC_STATUS_INVALID_PARAMETER
    writeSockAddr(state.remoteAddress, bufferLength, buffer)
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
  state.alpns = parseAlpnBuffers(alpnBuffers, alpnBufferCount)
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
  state.alpns = @[]
  state.localAddress = TransportAddress(family: AddressFamily.None)
  state.acceptedConnections = initTable[string, ConnectionState]()
  state.pendingZeroRttPackets = initTable[string, seq[seq[byte]]]()
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
  if state.alpns.len > 0:
    conn.configuration = ConfigurationState(
      kind: qhkConfiguration,
      registration: state.registration,
      alpns: state.alpns,
      credentialLoaded: true
    )
  conn.callback = nil
  conn.callbackContext = nil
  conn.context = nil
  conn.started = true
  conn.isClient = false
  conn.partitionId = 0'u16
  conn.serverName = ""
  conn.serverPort = uint16(local.port)
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
  conn.localAddress = local
  conn.remoteAddress = remote
  conn.initialSecrets = tls.deriveInitialSecrets(serverCidSeq)
  conn.handshakeKeys = InitialSecrets()
  conn.zeroRttMaterial = tls.ZeroRttMaterial()
  conn.handshakeClientSecret = @[]
  conn.handshakeServerSecret = @[]
  conn.transcript = @[]
  conn.handshakeComplete = false
  conn.peerAddressValidated = true
  conn.localTransportParams = tls.defaultQuicTransportParameters(serverCidSeq)
  conn.peerTransportParams = tls.QuicTransportParameters()
  conn.sessionResumed = false
  conn.resumptionTicket = @[]
  conn.resumptionData = @[]
  conn.zeroRttMaxData = 0'u32
  conn.zeroRttSentBytes = 0'u64
  conn.zeroRttAcceptDeadlineUs = 0'u64
  conn.incomingStreams = initTable[uint64, StreamState]()
  conn.ackTracker = qack.initAckTracker()
  conn.lossDetection = qloss.initLossDetectionModel()
  conn.congestionController = qcc.initCongestionController(caCubic, DefaultCongestionDatagramBytes)
  conn.sentPackets = @[]
  conn.pendingDatagrams = @[]
  conn.pendingControlFrames = @[]
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
  conn.peerCidSequence = 0'u64
  conn.activePeerCidSequence = 0'u64
  conn.localCidSequence = 0'u64
  conn.activeLocalCidSequence = 0'u64
  conn.advertisedPeerCids = @[]
  conn.issuedLocalCids = @[]
  conn.recordAdvertisedPeerCid(0'u64, conn.peerCid, default(array[16, uint8]))
  conn.recordIssuedLocalCid(0'u64, conn.localCid)
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
  stream.receiveBuffersProvided = 0'u64
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

proc isLocalStreamId(conn: ConnectionState; streamId: uint64): bool {.gcsafe.} =
  if conn.isNil or conn.quicConn.isNil:
    return false
  let initiatorBit = streamId and 0x1'u64
  if conn.quicConn.role == crClient:
    initiatorBit == 0'u64
  else:
    initiatorBit == 1'u64

proc resolveStreamState(conn: ConnectionState; streamId: uint64): StreamState =
  if conn.isNil:
    return nil
  if conn.isLocalStreamId(streamId):
    return conn.findAnyStream(streamId)
  ensureIncomingStream(conn, streamId)

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

proc payloadContainsAckElicitingFrame(plaintext: openArray[byte]): bool {.gcsafe.} =
  var pos = 0
  while pos < plaintext.len:
    let frameType = plaintext[pos]
    if (frameType and 0xF8'u8) == 0x08'u8 or
        frameType == 0x30'u8 or frameType == 0x31'u8 or
        frameType == 0x06'u8 or frameType == 0x04'u8 or
        frameType == 0x05'u8 or frameType == 0x1A'u8 or
        frameType == 0x1B'u8 or frameType == 0x18'u8 or
        frameType == 0x19'u8 or frameType == 0x1E'u8 or
        frameType == 0x01'u8:
      return true
    elif frameType == 0x02'u8 or frameType == 0x03'u8:
      discard proto.parseAckFrame(plaintext, pos)
    elif frameType == 0x00'u8:
      inc pos
    else:
      inc pos
  false

proc handleOneRttPayload(conn: ConnectionState; remote: TransportAddress;
    packetNumber: uint64; plaintext: seq[byte]) =
  if conn.isNil:
    return
  proc maybeCompleteServerHandshakeFromOneRtt() =
    if conn.quicConn.isNil or conn.quicConn.role != crServer or conn.handshakeComplete:
      return
    let note =
      if conn.sessionResumed:
        "Resumed server handshake confirmed by 1-RTT packet."
      else:
        "Server handshake confirmed by 1-RTT packet."
    conn.completeServerHandshake(note)
  conn.maybeInitiatePathMigration(remote)
  let previousLargest = qack.largestPacketNumberAcked(conn.ackTracker, ceOneRtt)
  if qack.wasPacketReceived(conn.ackTracker, ceOneRtt, packetNumber):
    let nowUs = nowMicros()
    {.cast(gcsafe).}:
      emitDiagnostics(DiagnosticsEvent(
        kind: diagConnectionEvent,
        handle: cast[pointer](conn),
        note: "RX duplicate 1-RTT pn=" & $packetNumber
      ))
    if payloadContainsAckElicitingFrame(plaintext):
      conn.setPendingAckRemote(ceOneRtt, remote)
      qack.markForAck(conn.ackTracker, ceOneRtt, packetNumber, nowUs, ackTypeAckImmediate)
      discard conn.sendAckFrame(packetNumber, ceOneRtt, unsafeAddr remote)
    return
  qack.trackIncomingPacket(conn.ackTracker, ceOneRtt, packetNumber)
  let nowUs = nowMicros()
  let reordered =
    previousLargest > 0'u64 and
    (packetNumber < previousLargest or
      (packetNumber > previousLargest and packetNumber != previousLargest + 1'u64))
  var pos = 0
  var ackEliciting = false
  while pos < plaintext.len:
    let frameType = plaintext[pos]
    if (frameType and 0xF8'u8) == 0x08'u8:
      ackEliciting = true
      maybeCompleteServerHandshakeFromOneRtt()
      let frame = proto.parseStreamFrame(plaintext, pos)
      let stream = resolveStreamState(conn, frame.streamId)
      if not stream.isNil:
        emitStreamReceive(stream, frame.offset, frame.data, frame.fin)
    elif frameType == 0x30'u8 or frameType == 0x31'u8:
      ackEliciting = true
      maybeCompleteServerHandshakeFromOneRtt()
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
      let frame = proto.parseCryptoFrame(plaintext, pos)
      if not conn.handshakeComplete and frame.data.len > 0 and conn.handshakeClientSecret.len > 0:
        let messages = tls.splitHandshakeMessages(frame.data)
        when defined(msquic_diag_test):
          var msgTypes: seq[string] = @[]
          for message in messages:
            msgTypes.add($message.msgType)
          {.cast(gcsafe).}:
            emitDiagnostics(DiagnosticsEvent(
              kind: diagConnectionEvent,
              handle: cast[pointer](conn),
              note: "Handshake CRYPTO messages=" & $msgTypes
            ))
        for message in messages:
          if message.msgType != 0x14'u8:
            continue
          let transcriptHash = tls.hashTranscript(conn.transcript)
          if not tls.verifyFinished(conn.handshakeClientSecret, transcriptHash, message.raw):
            {.cast(gcsafe).}:
              emitDiagnostics(DiagnosticsEvent(
                kind: diagConnectionEvent,
                handle: cast[pointer](conn),
                note: "Client Finished verify failed."
              ))
            continue
          conn.transcript.add(message.raw)
          conn.completeServerHandshake(
            "Client Finished verified. 1-RTT confirmed; local CID advertisement queued."
          )
          break
      elif conn.handshakeComplete and frame.data.len > 0:
        let messages = tls.splitHandshakeMessages(frame.data)
        for message in messages:
          if message.msgType != tls.HandshakeTypeNewSessionTicket:
            continue
          let ticket = tls.parseNewSessionTicket(message.raw)
          if not ticket.present or ticket.ticket.len == 0:
            continue
          conn.resumptionTicket = ticket.ticket
          var ticketMeta: BuiltinResumptionTicketMeta
          if parseBuiltinResumptionTicket(ticket.ticket, ticketMeta):
            conn.resumptionData = ticketMeta.data
            let issuedAtUs =
              if ticketMeta.issuedAtUs > 0'u64: ticketMeta.issuedAtUs
              else: nowMicros()
            let expiresAtUs =
              if ticket.ticketLifetimeSec == 0'u32: issuedAtUs
              else: issuedAtUs + uint64(ticket.ticketLifetimeSec) * 1_000_000'u64
            if ticket.maxEarlyData != ticketMeta.maxEarlyData or
                expiresAtUs != ticketMeta.expiresAtUs or
                ticket.ticketAgeAdd != ticketMeta.ticketAgeAdd:
              conn.resumptionTicket = buildBuiltinResumptionTicket(
                ticketMeta.serverName,
                ticketMeta.serverPort,
                ticketMeta.alpn,
                ticketMeta.data,
                issuedAtUs = issuedAtUs,
                expiresAtUs = expiresAtUs,
                maxEarlyData = ticket.maxEarlyData,
                ticketAgeAdd = ticket.ticketAgeAdd,
                preserveTicketAgeAdd = true
              )
          storeBuiltinResumptionEntry(
            conn,
            conn.resumptionTicket,
            conn.resumptionData,
            int64(ticket.ticketLifetimeSec)
          )
          {.cast(gcsafe).}:
            emitDiagnostics(DiagnosticsEvent(
              kind: diagConnectionEvent,
              handle: cast[pointer](conn),
              note: "Session ticket received len=" & $ticket.ticket.len
            ))
    elif frameType == 0x04'u8:
      ackEliciting = true
      maybeCompleteServerHandshakeFromOneRtt()
      let frame = proto.parseResetStreamFrame(plaintext, pos)
      let stream = conn.findAnyStream(frame.streamId)
      if not stream.isNil:
        stream.closed = true
        stream.finRequested = true
        stream.finSent = true
        emitPeerSendAborted(stream, frame.applicationErrorCode)
    elif frameType == 0x05'u8:
      ackEliciting = true
      maybeCompleteServerHandshakeFromOneRtt()
      let frame = proto.parseStopSendingFrame(plaintext, pos)
      let stream = conn.findAnyStream(frame.streamId)
      if not stream.isNil:
        emitPeerReceiveAborted(stream, frame.applicationErrorCode)
    elif frameType == 0x1A'u8:
      ackEliciting = true
      maybeCompleteServerHandshakeFromOneRtt()
      let frame = proto.parsePathChallengeFrame(plaintext, pos)
      discard conn.sendOneRttPacket(
        proto.encodePathResponseFrame(frame.data),
        sfkPathResponse,
        true,
        targetRemote = unsafeAddr remote
      )
    elif frameType == 0x1B'u8:
      ackEliciting = true
      maybeCompleteServerHandshakeFromOneRtt()
      let frame = proto.parsePathResponseFrame(plaintext, pos)
      conn.applyPathResponse(frame.data)
    elif frameType == 0x18'u8:
      ackEliciting = true
      maybeCompleteServerHandshakeFromOneRtt()
      let frame = proto.parseNewConnectionIdFrame(plaintext, pos)
      discard conn.applyPeerNewConnectionId(frame)
    elif frameType == 0x19'u8:
      ackEliciting = true
      maybeCompleteServerHandshakeFromOneRtt()
      let frame = proto.parseRetireConnectionIdFrame(plaintext, pos)
      discard conn.applyPeerRetireConnectionId(frame.sequence)
    elif frameType == 0x1E'u8:
      ackEliciting = true
      let frame = proto.parseHandshakeDoneFrame(plaintext, pos)
      if frame.present and not conn.isNil and not conn.quicConn.isNil:
        conn.quicConn.model.updateHandshakeState(
          retryUsed = conn.quicConn.model.handshake.retryUsed,
          confirmed = true,
          peerParamsValid = conn.quicConn.model.handshake.peerTransportParamsValid,
          zeroRttAccepted = conn.quicConn.model.handshake.zeroRttAccepted
        )
        {.cast(gcsafe).}:
          emitDiagnostics(DiagnosticsEvent(
            kind: diagConnectionEvent,
            handle: cast[pointer](conn),
            note: "HANDSHAKE_DONE received."
          ))
    elif frameType == 0x1C'u8 or frameType == 0x1D'u8:
      let frame = proto.parseConnectionCloseFrame(plaintext, pos)
      conn.handleRemoteConnectionClose(
        frame.errorCode,
        frame.reasonPhrase,
        frame.applicationClose
      )
      return
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
    discard conn.maybeFlushPendingOneRttAckOnRemoteChange(remote)
    conn.setPendingAckRemote(ceOneRtt, remote)
    qack.markForAck(
      conn.ackTracker,
      ceOneRtt,
      packetNumber,
      nowUs,
      ackTypeAckEliciting,
      conn.currentLocalAckDelayMs()
    )
    if qack.shouldSendAck(
        conn.ackTracker,
        ceOneRtt,
        MinAckSendNumber,
        ackTypeAckEliciting,
        reordered,
        nowUs
      ):
      discard conn.sendAckFrame(packetNumber, ceOneRtt, unsafeAddr remote)

proc handleZeroRttPayload(conn: ConnectionState; remote: TransportAddress;
    packetNumber: uint64; plaintext: seq[byte]) =
  if conn.isNil:
    return
  if qack.wasPacketReceived(conn.ackTracker, ceZeroRtt, packetNumber):
    let nowUs = nowMicros()
    {.cast(gcsafe).}:
      emitDiagnostics(DiagnosticsEvent(
        kind: diagConnectionEvent,
        handle: cast[pointer](conn),
        note: "RX duplicate 0-RTT pn=" & $packetNumber
      ))
    if payloadContainsAckElicitingFrame(plaintext):
      conn.setPendingAckRemote(ceZeroRtt, remote)
      qack.markForAck(conn.ackTracker, ceZeroRtt, packetNumber, nowUs, ackTypeAckImmediate)
      discard conn.maybeSendPendingOneRttAck(unsafeAddr remote)
    return
  qack.trackIncomingPacket(conn.ackTracker, ceZeroRtt, packetNumber)
  let nowUs = nowMicros()
  var pos = 0
  var ackEliciting = false
  while pos < plaintext.len:
    let frameType = plaintext[pos]
    if (frameType and 0xF8'u8) == 0x08'u8:
      ackEliciting = true
      let frame = proto.parseStreamFrame(plaintext, pos)
      let stream = resolveStreamState(conn, frame.streamId)
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
          note: "0-rtt datagram received"
        )
        emitConnectionEvent(conn, ev)
    elif frameType == 0x01'u8 or frameType == 0x00'u8:
      if frameType == 0x01'u8:
        ackEliciting = true
      inc pos
    else:
      inc pos
  {.cast(gcsafe).}:
    emitDiagnostics(DiagnosticsEvent(
      kind: diagConnectionEvent,
      handle: cast[pointer](conn),
      note: "RX 0-RTT pn=" & $packetNumber
    ))
  if ackEliciting:
    conn.setPendingAckRemote(ceZeroRtt, remote)
    qack.markForAck(conn.ackTracker, ceZeroRtt, packetNumber, nowUs, ackTypeAckImmediate)
    discard conn.maybeSendPendingOneRttAck(unsafeAddr remote)

proc handleZeroRttPacket(conn: ConnectionState; remote: TransportAddress; data: seq[byte]) =
  if conn.isNil or not conn.connectionZeroRttMaterialReady():
    return
  let header = proto.parseUnprotectedHeader(data)
  let pnOffset = header.payloadOffset
  if pnOffset + 4 + 16 > data.len:
    return
  var mutableData = data
  var pnSlice: array[4, byte]
  for i in 0 .. 3:
    pnSlice[i] = mutableData[pnOffset + i]
  tls.removeHeaderProtection(
    conn.zeroRttMaterial.hp,
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
    conn.zeroRttMaterial.key,
    conn.zeroRttMaterial.iv,
    uint64(pnVal),
    aad,
    ciphertext,
    recTag
  )
  if plaintext.len == 0 and ciphertext.len > 0:
    return
  conn.maybeInitiatePathMigration(remote)
  handleZeroRttPayload(conn, remote, uint64(pnVal), plaintext)

proc connectionMatchesDestCid(conn: ConnectionState; data: seq[byte]): bool {.gcsafe.} =
  if conn.isNil or data.len < 2:
    return false
  let header = proto.parseUnprotectedHeader(data)
  if (header.firstByte and 0x80'u8) != 0'u8:
    let incomingCid = initConnectionId(header.destConnectionId)
    return conn.localCidAccepted(incomingCid)
  let cidLen = int(conn.localCid.length)
  if cidLen <= 0 or data.len < 1 + cidLen:
    return false
  var shortCid = newSeq[byte](cidLen)
  for i in 0 ..< cidLen:
    shortCid[i] = data[1 + i]
  let incomingCid = initConnectionId(shortCid)
  conn.localCidAccepted(incomingCid)

proc findAcceptedConnectionByDestCid(state: ListenerState;
    data: seq[byte]): ConnectionState {.gcsafe.} =
  if state.isNil:
    return nil
  for conn in state.acceptedConnections.values:
    if conn.connectionMatchesDestCid(data):
      return conn
  nil

proc queuePendingZeroRtt(state: ListenerState; remote: TransportAddress; data: seq[byte]) =
  if state.isNil:
    return
  let key = remoteAddressKey(remote)
  if key.len == 0:
    return
  var packets = state.pendingZeroRttPackets.getOrDefault(key, @[])
  packets.add(data)
  state.pendingZeroRttPackets[key] = packets

proc pendingZeroRttPacketBytes(state: ListenerState; remote: TransportAddress): uint64 {.gcsafe.} =
  if state.isNil:
    return 0'u64
  let key = remoteAddressKey(remote)
  if key.len == 0 or not state.pendingZeroRttPackets.hasKey(key):
    return 0'u64
  for packet in state.pendingZeroRttPackets[key]:
    result += uint64(packet.len)

proc replayPendingZeroRtt(state: ListenerState; conn: ConnectionState; remote: TransportAddress) =
  if state.isNil or conn.isNil:
    return
  let key = remoteAddressKey(remote)
  if key.len == 0 or not state.pendingZeroRttPackets.hasKey(key):
    return
  let packets = state.pendingZeroRttPackets[key]
  state.pendingZeroRttPackets.del(key)
  for packet in packets:
    handleZeroRttPacket(conn, remote, packet)

proc handleShortHeaderPacket(conn: ConnectionState; remote: TransportAddress; data: seq[byte]) =
  if conn.isNil:
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
    discard conn.handleStatelessReset(remote)
    return
  handleOneRttPayload(conn, remote, uint64(pnVal), plaintext)

proc handleHandshakePacket(conn: ConnectionState; remote: TransportAddress; data: seq[byte]) =
  if conn.isNil:
    return
  let nowUs = nowMicros()
  let header = proto.parseUnprotectedHeader(data)
  let pnOffset = header.payloadOffset
  if pnOffset + 4 + 16 > data.len:
    return
  let receiveMat: tuple[key, iv, hp: seq[byte]] =
    if conn.quicConn.isNil:
      (@[], @[], @[])
    elif conn.quicConn.role == crServer:
      (conn.handshakeKeys.clientKey, conn.handshakeKeys.clientIv, conn.handshakeKeys.clientHp)
    else:
      (conn.handshakeKeys.serverKey, conn.handshakeKeys.serverIv, conn.handshakeKeys.serverHp)
  if receiveMat.key.len < 16 or receiveMat.iv.len < 12 or receiveMat.hp.len < 16:
    return
  var mutableData = data
  var pnSlice: array[4, byte]
  for i in 0 .. 3:
    pnSlice[i] = mutableData[pnOffset + i]
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
  let plaintext = tls.decryptPacket(
    receiveMat.key,
    receiveMat.iv,
    uint64(pnVal),
    aad,
    ciphertext,
    recTag
  )
  if plaintext.len == 0 and ciphertext.len > 0:
    return
  if conn.isClient:
    conn.peerAddressValidated = true
  if qack.wasPacketReceived(conn.ackTracker, ceHandshake, uint64(pnVal)):
    {.cast(gcsafe).}:
      emitDiagnostics(DiagnosticsEvent(
        kind: diagConnectionEvent,
        handle: cast[pointer](conn),
        note: "RX duplicate Handshake pn=" & $pnVal
      ))
    if payloadContainsAckElicitingFrame(plaintext):
      qack.markForAck(conn.ackTracker, ceHandshake, uint64(pnVal), nowUs, ackTypeAckImmediate)
      discard conn.sendAckFrame(uint64(pnVal), ceHandshake, unsafeAddr remote)
    return
  qack.trackIncomingPacket(conn.ackTracker, ceHandshake, uint64(pnVal))
  var ackEliciting = false
  var pos = 0
  while pos < plaintext.len:
    let frameType = plaintext[pos]
    if frameType == 0x06'u8:
      ackEliciting = true
      inc pos
      let frame = proto.parseCryptoFrame(plaintext, pos)
      if not conn.handshakeComplete and frame.data.len > 0 and conn.handshakeClientSecret.len > 0:
        let messages = tls.splitHandshakeMessages(frame.data)
        when defined(msquic_diag_test):
          var msgTypes: seq[string] = @[]
          for message in messages:
            msgTypes.add($message.msgType)
          {.cast(gcsafe).}:
            emitDiagnostics(DiagnosticsEvent(
              kind: diagConnectionEvent,
              handle: cast[pointer](conn),
              note: "Handshake packet CRYPTO messages=" & $msgTypes
            ))
        for message in messages:
          if message.msgType != 0x14'u8:
            continue
          let transcriptHash = tls.hashTranscript(conn.transcript)
          if conn.quicConn.role == crServer:
            if not tls.verifyFinished(conn.handshakeClientSecret, transcriptHash, message.raw):
              {.cast(gcsafe).}:
                emitDiagnostics(DiagnosticsEvent(
                  kind: diagConnectionEvent,
                  handle: cast[pointer](conn),
                  note: "Client Finished verify failed via Handshake packet."
                ))
              continue
            conn.transcript.add(message.raw)
            conn.completeServerHandshake(
              "Client Finished verified via Handshake packet; local CID advertisement queued."
            )
            break
          else:
            if not tls.verifyFinished(conn.handshakeServerSecret, transcriptHash, message.raw):
              continue
            conn.transcript.add(message.raw)
            let handshakeHash = tls.hashTranscript(conn.transcript)
            conn.oneRttKeys = tls.deriveApplicationSecrets(conn.handshakeClientSecret, handshakeHash)
            conn.handshakeComplete = true
            let clientFinished = tls.encodeFinished(conn.handshakeClientSecret, handshakeHash)
            conn.markActivePathValidated(0)
            discard conn.sendHandshakePacket(buildCryptoFrame(clientFinished), sfkCrypto, true)
            for stream in conn.localStreams:
              flushStream(stream)
            flushPendingDatagrams(conn)
            let negotiatedAlpn =
              if conn.configuration.isNil or conn.configuration.alpns.len == 0: ""
              else: conn.configuration.alpns[0]
            {.cast(gcsafe).}:
              emitNativeConnected(conn)
              var connectedEvent = ConnectionEvent(
                kind: ceConnected,
                sessionResumed: conn.sessionResumed,
                negotiatedAlpn: negotiatedAlpn,
                note: "client handshake complete"
              )
              emitConnectionEvent(conn, connectedEvent)
              emitDiagnostics(DiagnosticsEvent(
                kind: diagConnectionEvent,
                handle: cast[pointer](conn),
                note: "Server Finished verified via Handshake packet."
              ))
            break
        when defined(msquic_diag_test):
          if messages.len == 0:
            {.cast(gcsafe).}:
              emitDiagnostics(DiagnosticsEvent(
                kind: diagConnectionEvent,
                handle: cast[pointer](conn),
                note: "Handshake packet CRYPTO had no decodable messages."
              ))
    elif frameType == 0x02'u8 or frameType == 0x03'u8:
      let ack = proto.parseAckFrame(plaintext, pos)
      conn.applyAckFrame(ack, ceHandshake)
    elif frameType == 0x1C'u8 or frameType == 0x1D'u8:
      let frame = proto.parseConnectionCloseFrame(plaintext, pos)
      conn.handleRemoteConnectionClose(
        frame.errorCode,
        frame.reasonPhrase,
        frame.applicationClose
      )
      return
    elif frameType == 0x01'u8 or frameType == 0x00'u8:
      if frameType == 0x01'u8:
        ackEliciting = true
      inc pos
    else:
      inc pos
  if ackEliciting:
    qack.markForAck(conn.ackTracker, ceHandshake, uint64(pnVal), nowUs, ackTypeAckImmediate)
    discard conn.sendAckFrame(uint64(pnVal), ceHandshake, unsafeAddr remote)

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
    elif frameType == 0x01'u8:
      inc pos
    elif frameType == 0x00'u8:
      inc pos
    elif frameType == 0x02'u8 or frameType == 0x03'u8:
      let frame = proto.parseAckFrame(plaintext, pos)
      let remoteKey = remoteAddressKey(remote)
      let ackConn =
        block:
          let byCid = state.findAcceptedConnectionByDestCid(data)
          if not byCid.isNil: byCid
          else: state.acceptedConnections.getOrDefault(remoteKey, nil)
      if not ackConn.isNil:
        ackConn.applyAckFrame(frame, ceInitial)
    elif frameType == 0x1C'u8 or frameType == 0x1D'u8:
      let frame = proto.parseConnectionCloseFrame(plaintext, pos)
      let remoteKey = remoteAddressKey(remote)
      let closeConn =
        block:
          let byCid = state.findAcceptedConnectionByDestCid(data)
          if not byCid.isNil: byCid
          else: state.acceptedConnections.getOrDefault(remoteKey, nil)
      if not closeConn.isNil:
        closeConn.handleRemoteConnectionClose(
          frame.errorCode,
          frame.reasonPhrase,
          frame.applicationClose,
          state
        )
      return
    else:
      inc pos
  if cryptoData.len == 0:
    return

  let remoteKey = remoteAddressKey(remote)
  var conn = state.acceptedConnections.getOrDefault(remoteKey, nil)
  if not conn.isNil and
      not conn.initialPacketMatchesConnection(header.srcConnectionId, header.destConnectionId):
    let byCid = state.findAcceptedConnectionByDestCid(data)
    if not byCid.isNil:
      conn = byCid
      state.registerAcceptedConnection(remoteKey, conn)
  let isNewConnection =
    conn.isNil or
    not conn.initialPacketMatchesConnection(header.srcConnectionId, header.destConnectionId)
  if isNewConnection:
    conn = newAcceptedConnection(state, remote, local, header.srcConnectionId, header.destConnectionId)
    conn.initialSecrets = initialSecrets
    state.registerAcceptedConnection(remoteKey, conn)
    emitListenerNewConnection(state, cast[HQUIC](conn))
  elif qack.wasPacketReceived(conn.ackTracker, ceInitial, uint64(pnVal)):
    emitDiagnostics(DiagnosticsEvent(
      kind: diagConnectionEvent,
      handle: cast[pointer](conn),
      note: "RX duplicate Initial pn=" & $pnVal
    ))
    if payloadContainsAckElicitingFrame(plaintext):
      qack.markForAck(conn.ackTracker, ceInitial, uint64(pnVal), nowMicros(), ackTypeAckImmediate)
      discard conn.sendAckFrame(uint64(pnVal), ceInitial, unsafeAddr remote)
    return
  qack.trackIncomingPacket(conn.ackTracker, ceInitial, uint64(pnVal))

  if not conn.handshakeComplete:
    let handshakeMessages = tls.splitHandshakeMessages(cryptoData)
    if handshakeMessages.len == 0 or handshakeMessages[0].msgType != 0x01'u8:
      return
    let clientHello = handshakeMessages[0].raw
    let clientParams = tls.parseQuicTransportParameters(clientHello)
    if not clientParams.present or
        not byteSeqEqual(clientParams.initialSourceConnectionId, header.srcConnectionId):
      emitDiagnostics(DiagnosticsEvent(
        kind: diagConnectionEvent,
        handle: cast[pointer](conn),
        note: "Client transport parameters missing or invalid."
      ))
      return
    conn.peerTransportParams = clientParams
    let offered = tls.extractBuiltinResumptionOffer(clientHello)
    var acceptedResumption = false
    var acceptedZeroRtt = false
    if offered.ticket.len > 0:
      var ticketMeta: BuiltinResumptionTicketMeta
      if parseBuiltinResumptionTicket(offered.ticket, ticketMeta):
        let listenerAlpn =
          if state.alpns.len == 0: ""
          else: state.alpns[0]
        let localPort = uint16(local.port)
        let nowUs = nowMicros()
        let zeroRttQueuedBytes = state.pendingZeroRttPacketBytes(remote)
        acceptedResumption =
          builtinTicketTimeValid(ticketMeta, nowUs) and
          builtinTicketAgeAccepted(
            ticketMeta,
            offered.obfuscatedTicketAge,
            offered.agePresent,
            nowUs
          ) and
          builtinTicketServerNameAccepted(ticketMeta, local) and
          (ticketMeta.serverPort == 0'u16 or ticketMeta.serverPort == localPort) and
          (listenerAlpn.len == 0 or ticketMeta.alpn.len == 0 or ticketMeta.alpn == listenerAlpn) and
          tls.verifyBuiltinResumptionBinder(clientHello, offered.ticket, offered.binder)
        if acceptedResumption:
          conn.sessionResumed = true
          conn.resumptionTicket = offered.ticket
          conn.resumptionData = ticketMeta.data
          conn.zeroRttMaxData = ticketMeta.maxEarlyData
          conn.zeroRttSentBytes = 0'u64
          conn.zeroRttAcceptDeadlineUs = builtinZeroRttDeadline(ticketMeta)
          acceptedZeroRtt =
            offered.earlyDataRequested and
            builtinZeroRttEligible(ticketMeta, nowUs) and
            zeroRttQueuedBytes <= uint64(ticketMeta.maxEarlyData) and
            not gForceRejectBuiltinZeroRttForTest and
            claimBuiltinZeroRttReplay(
              offered.ticket,
              offered.binder,
              conn.zeroRttAcceptDeadlineUs
            )
          if acceptedZeroRtt:
            conn.zeroRttMaterial = tls.deriveBuiltinZeroRttMaterial(offered.ticket)
            conn.quicConn.model.handshake.zeroRttAccepted = true
      when defined(msquic_diag_test):
        {.cast(gcsafe).}:
          emitDiagnostics(DiagnosticsEvent(
            kind: diagConnectionEvent,
            handle: cast[pointer](conn),
            note: "server resumption accepted=" & $acceptedResumption &
              " zeroRtt=" & $acceptedZeroRtt
          ))
    let clientKeyShare = tls.findClientKeyShare(clientHello)
    if clientKeyShare.len != 32:
      return
    let serverKeyShare = tls.generateKeyShare()
    conn.localTransportParams = tls.defaultQuicTransportParameters(cidBytes(conn.localCid))
    let serverHello = tls.encodeServerHello(
      serverKeyShare.publicKey.getBytes(),
      conn.localTransportParams,
      conn.sessionResumed,
      acceptedZeroRtt
    )
    let transcriptBeforeFinished = clientHello & serverHello
    let sharedSecret = tls.computeSharedSecret(serverKeyShare.privateKey, clientKeyShare)
    let helloHash = tls.hashTranscript(transcriptBeforeFinished)
    let handshakeSecrets = tls.deriveHandshakeSecrets(sharedSecret, helloHash)
    conn.handshakeKeys = handshakeSecrets
    conn.handshakeClientSecret = handshakeSecrets.clientSecret
    conn.handshakeServerSecret = handshakeSecrets.serverSecret
    let serverFinished = tls.encodeFinished(handshakeSecrets.serverSecret, helloHash)
    conn.transcript = transcriptBeforeFinished & serverFinished
    let appHash = tls.hashTranscript(conn.transcript)
    conn.oneRttKeys = tls.deriveApplicationSecrets(handshakeSecrets.clientSecret, appHash)

    let cryptoFrame = buildCryptoFrame(serverHello)

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
    discard conn.sendHandshakePacket(buildCryptoFrame(serverFinished), sfkCrypto, true)
    if conn.quicConn.model.handshake.zeroRttAccepted:
      state.replayPendingZeroRtt(conn, remote)
    emitDiagnostics(DiagnosticsEvent(
      kind: diagConnectionEvent,
      handle: cast[pointer](conn),
      note: "Server flight sent. Waiting for client Finished."))

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
    elif packet.isLongHeader and packet.longHeader.packetType == ptHandshake:
      let conn = state.findAcceptedConnectionByDestCid(data)
      if not conn.isNil:
        {.cast(gcsafe).}:
          emitDiagnostics(DiagnosticsEvent(
            kind: diagRegistrationOpened,
            handle: cast[pointer](state),
            note: "RX Handshake len=" & $data.len))
        {.cast(gcsafe).}:
          handleHandshakePacket(conn, remote, data)
    else:
      if packet.isLongHeader and packet.longHeader.packetType == pt0RTT:
        let remoteKey = remoteAddressKey(remote)
        var conn = state.acceptedConnections.getOrDefault(remoteKey, nil)
        if not conn.isNil and not conn.connectionMatchesDestCid(data):
          conn = nil
        if conn.isNil:
          conn = state.findAcceptedConnectionByDestCid(data)
          if not conn.isNil:
            state.registerAcceptedConnection(remoteKey, conn)
        if conn.isNil or not conn.quicConn.model.handshake.zeroRttAccepted or
            not conn.connectionZeroRttMaterialReady():
          state.queuePendingZeroRtt(remote, data)
        else:
          {.cast(gcsafe).}:
            handleZeroRttPacket(conn, remote, data)
        return
      let remoteKey = remoteAddressKey(remote)
      var conn = state.acceptedConnections.getOrDefault(remoteKey, nil)
      if not conn.isNil and not conn.connectionMatchesDestCid(data):
        conn = nil
      if conn.isNil:
        conn = state.findAcceptedConnectionByDestCid(data)
        if not conn.isNil:
          state.registerAcceptedConnection(remoteKey, conn)
      if not conn.isNil:
        {.cast(gcsafe).}:
          handleShortHeaderPacket(conn, remote, data)
      else:
        let resetConn = state.findAcceptedConnectionByStatelessResetToken(data)
        if not resetConn.isNil:
          discard resetConn.handleStatelessReset(remote, state)
  except Exception:
    discard

proc msquicListenerStart(listener: HQUIC; alpn: ptr QuicBuffer;
    alpnCount: uint32; address: pointer): QUIC_STATUS {.cdecl.} =
  let state = toListener(listener)
  if state.isNil:
    return QUIC_STATUS_INVALID_PARAMETER
  state.alpns = parseAlpnBuffers(alpn, alpnCount)
  warn "msquicListenerStart entry", addressNil = address.isNil
  var bindAddress =
    if state.localAddress.family != AddressFamily.None:
      state.localAddress
    else:
      initTAddress("0.0.0.0", Port(0))
  if not address.isNil:
    discard parseSockAddr(address, bindAddress)
  
  try:
    state.transport = newDatagramTransport(
      (proc (transp: DatagramTransport, remote: TransportAddress) {.async.} =
        let data = transp.getMessage()
        asyncCheck listenerOnReceive(state, transp, remote, transp.localAddress, data)),
      local = bindAddress
    )
    state.localAddress = state.transport.localAddress
    state.started = true
    emitDiagnostics(DiagnosticsEvent(
      kind: diagRegistrationOpened,
      handle: listener,
      note: "Bound to " & $state.localAddress))
      
    QUIC_STATUS_SUCCESS
  except CatchableError as exc:
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
  state.isClient = true
  state.eventHandlers = @[]
  state.streamSchedulingScheme = 0
  state.settingsOverlay = defaultQuicSettingsOverlay()
  state.datagramReceiveEnabled = state.settingsOverlay.datagramReceiveEnabled
  state.datagramSendEnabled = false
  state.congestionAlgorithm = caCubic
  state.closeReason = ""
  state.disable1RttEncryption = false
  state.partitionId = 0'u16
  state.zeroRttMaterial = tls.ZeroRttMaterial()
  state.handshakeKeys = InitialSecrets()
  state.handshakeClientSecret = @[]
  state.handshakeServerSecret = @[]
  state.handshakeComplete = false
  state.peerAddressValidated = false
  state.localTransportParams = tls.QuicTransportParameters()
  state.peerTransportParams = tls.QuicTransportParameters()
  state.sessionResumed = false
  state.resumptionTicket = @[]
  state.resumptionData = @[]
  state.zeroRttMaxData = 0'u32
  state.zeroRttSentBytes = 0'u64
  state.zeroRttAcceptDeadlineUs = 0'u64
  state.localAddress = TransportAddress(family: AddressFamily.None)
  state.remoteAddress = TransportAddress(family: AddressFamily.None)
  state.incomingStreams = initTable[uint64, StreamState]()
  state.ackTracker = qack.initAckTracker()
  state.lossDetection = qloss.initLossDetectionModel()
  state.congestionController = qcc.initCongestionController(caCubic, DefaultCongestionDatagramBytes)
  state.sentPackets = @[]
  state.pendingDatagrams = @[]
  state.pendingControlFrames = @[]
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
  state.peerCidSequence = 0'u64
  state.activePeerCidSequence = 0'u64
  state.localCidSequence = 0'u64
  state.activeLocalCidSequence = 0'u64
  state.advertisedPeerCids = @[]
  state.issuedLocalCids = @[]
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
  let closePayload = proto.encodeConnectionCloseFrame(
    uint64(errorCode),
    reasonPhrase = state.closeReason,
    applicationClose = true
  )
  if not state.transport.isNil:
    if state.handshakeComplete:
      discard state.sendOneRttPacket(closePayload, sfkConnectionClose, false)
    elif state.handshakeKeys.clientKey.len > 0 and state.handshakeKeys.clientIv.len > 0:
      discard state.sendHandshakePacket(closePayload, sfkConnectionClose, false)
    elif state.initialSecrets.clientKey.len > 0 and state.initialSecrets.clientIv.len > 0:
      discard state.sendInitialPacket(closePayload, sfkConnectionClose, false)
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
  if flags != QUIC_SEND_RESUMPTION_FLAGS(0):
    return QUIC_STATUS_NOT_SUPPORTED
  if not state.started or state.configuration.isNil or not state.configuration.credentialLoaded:
    return QUIC_STATUS_INVALID_STATE
  if dataLength > 0'u16 and data.isNil:
    return QUIC_STATUS_INVALID_PARAMETER
  var customData: seq[byte] = @[]
  if dataLength > 0'u16:
    customData = newSeq[byte](int(dataLength))
    copyMem(addr customData[0], data, int(dataLength))
  let ticketLifetimeSec = currentBuiltinNewSessionTicketLifetimeSec()
  let ticketMaxEarlyData = currentBuiltinNewSessionTicketMaxEarlyData(state)
  let issuedAtUs = nowMicros()
  let expiresAtUs =
    if ticketLifetimeSec == 0'u32: issuedAtUs
    else: issuedAtUs + uint64(ticketLifetimeSec) * 1_000_000'u64
  let ticket = buildBuiltinResumptionTicket(
    state.serverName,
    state.serverPort,
    alpnForConnection(state),
    customData,
    issuedAtUs = issuedAtUs,
    expiresAtUs = expiresAtUs,
    maxEarlyData = ticketMaxEarlyData
  )
  if ticket.len == 0:
    return QUIC_STATUS_INVALID_STATE
  state.resumptionTicket = ticket
  state.resumptionData = customData
  storeBuiltinResumptionEntry(state, ticket, customData, int64(ticketLifetimeSec))
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
  state.localCidSequence = 0'u64
  state.peerCidSequence = 0'u64
  state.activePeerCidSequence = 0'u64
  state.activeLocalCidSequence = 0'u64
  state.advertisedPeerCids = @[]
  state.issuedLocalCids = @[]
  state.recordAdvertisedPeerCid(0'u64, serverCid, default(array[16, uint8]))
  state.recordIssuedLocalCid(0'u64, clientCid)
  state.serverName = if serverName.isNil: "" else: $serverName
  state.serverPort = serverPort
  let cachedResumption = loadBuiltinResumptionEntry(
    state.serverName,
    state.serverPort,
    alpnForConnection(state)
  )
  state.sessionResumed = false
  state.resumptionTicket = cachedResumption.ticket
  state.resumptionData = cachedResumption.data
  state.zeroRttMaxData = 0'u32
  state.zeroRttSentBytes = 0'u64
  state.zeroRttAcceptDeadlineUs = 0'u64
  discard state.applyZeroRttTicketState(state.resumptionTicket)
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
                        let messages = tls.splitHandshakeMessages(crypto.data)
                        var serverHelloRaw: seq[byte] = @[]
                        for message in messages:
                          case message.msgType
                          of 0x02'u8:
                            serverHelloRaw = message.raw
                          else:
                            discard
                        let serverHelloKey = tls.findServerKeyShare(serverHelloRaw)
                        if serverHelloKey.len == 32:
                          {.cast(gcsafe).}:
                            emitDiagnostics(DiagnosticsEvent(
                              kind: diagConnectionEvent,
                              handle: cast[HQUIC](state),
                              note: "RX ServerHello KeyShare"
                            ))

                          let serverParams = tls.parseQuicTransportParameters(serverHelloRaw)
                          let expectedServerCid = cidBytes(state.peerCid)
                          if not serverParams.present or
                              not byteSeqEqual(serverParams.initialSourceConnectionId, expectedServerCid):
                            {.cast(gcsafe).}:
                              emitDiagnostics(DiagnosticsEvent(
                                kind: diagConnectionEvent,
                                handle: cast[HQUIC](state),
                                note: "Server transport parameters missing or invalid."
                              ))
                            continue
                          state.peerTransportParams = serverParams
                          state.sessionResumed = tls.serverHelloSessionResumed(serverHelloRaw)
                          state.quicConn.model.handshake.zeroRttAccepted =
                            tls.serverHelloZeroRttAccepted(serverHelloRaw)
                          let transcriptBeforeFinished = state.transcript & serverHelloRaw
                          let sharedSecret = tls.computeSharedSecret(state.clientPrivateKey, serverHelloKey)
                          let helloHash = tls.hashTranscript(transcriptBeforeFinished)
                          let handshakeSecrets = tls.deriveHandshakeSecrets(sharedSecret, helloHash)
                          state.handshakeKeys = handshakeSecrets
                          state.handshakeClientSecret = handshakeSecrets.clientSecret
                          state.handshakeServerSecret = handshakeSecrets.serverSecret
                          state.transcript = transcriptBeforeFinished
                    elif fType == 0x02 or fType == 0x03:
                      let ack = proto.parseAckFrame(plaintext, pos)
                      state.applyAckFrame(ack, ceInitial)
                    else:
                      inc pos
              elif pType == 2:
                {.cast(gcsafe).}:
                  handleHandshakePacket(state, remote, data)
            else:
              {.cast(gcsafe).}:
                handleShortHeaderPacket(state, remote, data)
          except Exception:
            discard
        ),
        local = state.selectClientLocalBind()
      )
      state.localAddress = state.transport.localAddress
      transportEnabled = true
    except CatchableError:
      state.remoteAddress = TransportAddress()
      state.transport = nil

    # Phase 3: Send Client Initial (ClientHello)
    let destCid = @(state.peerCid.bytes)[0 ..< int(state.peerCid.length)] 
    let srcCid = @(state.localCid.bytes)[0 ..< int(state.localCid.length)]
    state.initialSecrets = tls.deriveInitialSecrets(destCid)
    state.localTransportParams = tls.defaultQuicTransportParameters(srcCid)
    
    let keyShare = tls.generateKeyShare()
    state.clientPrivateKey = keyShare.privateKey
    let offeredResumptionTicket =
      if gTamperBuiltinResumptionTicketForTest and state.resumptionTicket.len > 0:
        tamperBuiltinResumptionTicket(state.resumptionTicket)
      elif gHasOverrideBuiltinTicketServerNameForTest and state.resumptionTicket.len > 0:
        overrideBuiltinResumptionTicketServerName(
          state.resumptionTicket,
          gOverrideBuiltinTicketServerNameForTest
        )
      elif gHasOverrideBuiltinTicketServerPortForTest and state.resumptionTicket.len > 0:
        overrideBuiltinResumptionTicketServerPort(
          state.resumptionTicket,
          gOverrideBuiltinTicketServerPortForTest
        )
      elif gHasOverrideBuiltinTicketAlpnForTest and state.resumptionTicket.len > 0:
        overrideBuiltinResumptionTicketAlpn(
          state.resumptionTicket,
          gOverrideBuiltinTicketAlpnForTest
        )
      else:
        state.resumptionTicket
    let requestBuiltinEarlyData =
      not gDropBuiltinEarlyDataRequestForTest and
      connectionZeroRttMaterialReady(state)
    var includeBuiltinTicketAge = false
    var builtinTicketAge = 0'u32
    if offeredResumptionTicket.len > 0:
      var ticketMeta: BuiltinResumptionTicketMeta
      if parseBuiltinResumptionTicket(offeredResumptionTicket, ticketMeta):
        includeBuiltinTicketAge =
          ticketMeta.version >= 3'u8 and
          not gDropBuiltinTicketAgeForTest
        builtinTicketAge = computeBuiltinTicketAge(nowMicros(), ticketMeta)
        if gHasOverrideBuiltinTicketAgeDeltaMsForTest:
          builtinTicketAge = applyBuiltinTicketAgeDelta(
            builtinTicketAge,
            gOverrideBuiltinTicketAgeDeltaMsForTest
          )
    let clientHello =
      if (gDropBuiltinEarlyDataRequestForTest or
          gDropBuiltinResumptionBinderForTest or
          gTamperBuiltinResumptionBinderForTest or
          gTamperBuiltinResumptionTicketForTest or
          gHasOverrideBuiltinTicketServerNameForTest or
          gHasOverrideBuiltinTicketServerPortForTest or
          gHasOverrideBuiltinTicketAlpnForTest or
          gHasOverrideBuiltinTicketAgeDeltaMsForTest) and
          state.resumptionTicket.len > 0:
        if gDropBuiltinResumptionBinderForTest:
          tls.encodeClientHelloWithBuiltinResumptionOverride(
            srcCid,
            keyShare,
            state.localTransportParams,
            @[],
            offeredResumptionTicket,
            false,
            requestBuiltinEarlyData,
            includeBuiltinTicketAge,
            builtinTicketAge
          )
        else:
          let binderSourceTicket =
            if gTamperBuiltinResumptionTicketForTest:
              state.resumptionTicket
            else:
              offeredResumptionTicket
          tls.encodeClientHelloWithBuiltinResumptionOverride(
            srcCid,
            keyShare,
            state.localTransportParams,
            binderSourceTicket,
            offeredResumptionTicket,
            gTamperBuiltinResumptionBinderForTest,
            requestBuiltinEarlyData,
            includeBuiltinTicketAge,
            builtinTicketAge
          )
      else:
        tls.encodeClientHello(
          srcCid,
          keyShare,
          state.localTransportParams,
          state.resumptionTicket,
          requestBuiltinEarlyData,
          includeBuiltinTicketAge,
          builtinTicketAge
        )
    state.transcript = clientHello # Store
    
    # CRYPTO Frame
    let cryptoFrame = buildCryptoFrame(clientHello)
    
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
      sfkResetStream,
      true,
      stream = state,
      streamOffset = finalSize,
      streamPayload = @[],
      streamFin = true
    )
  if abortReceive and not state.connection.isNil and state.connection.handshakeComplete:
    discard state.connection.sendOneRttPacket(
      proto.encodeStopSendingFrame(state.streamId, uint64(errorCode)),
      sfkStopSending,
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

  while state.pendingChunks.len > 0 and conn.handshakeComplete and
      conn.quicConn.model.handshake.zeroRttAccepted and
      state.pendingChunks[0].zeroRttDispatched:
    state.pendingChunks.delete(0)
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
    clientContext = chunk.clientContext,
    targetRemote =
      (if chunk.targetRemote.family == AddressFamily.None: nil
      else: unsafeAddr chunk.targetRemote)
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
    clientContext: clientContext,
    targetRemote: TransportAddress(family: AddressFamily.None),
    zeroRttDispatched: false
  ))
  state.sentOffset += uint64(payload.len)
  if fin:
    state.finRequested = true

  if not state.connection.handshakeComplete and
      (flags and SendFlagAllowZeroRtt) != 0 and
      (state.connection.sessionResumed or state.connection.resumptionTicket.len > 0):
    let zeroRttFrame = proto.encodeStreamFrame(
      state.streamId,
      payload,
      state.pendingChunks[^1].offset,
      fin
    )
    if state.connection.sendZeroRttPacket(
        zeroRttFrame,
        sfkStream,
        stream = state,
        streamOffset = state.pendingChunks[^1].offset,
        streamPayload = payload,
        streamFin = fin,
        clientContext = clientContext
      ) > 0'u64:
      state.pendingChunks[^1].zeroRttDispatched = true
      return QUIC_STATUS_SUCCESS

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
      clientContext: clientContext,
      targetRemote: state.remoteAddress,
      zeroRttDispatched: false
    ))
    let zeroRttFrame = proto.encodeDatagramFrame(payload, true)
    if state.sendZeroRttPacket(
        zeroRttFrame,
        sfkDatagram,
        clientContext = clientContext,
        targetRemote = unsafeAddr state.pendingDatagrams[^1].targetRemote
      ) > 0'u64:
      state.pendingDatagrams[^1].zeroRttDispatched = true
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
  if gGlobalExecutionConfig.applied and gGlobalExecutionConfig.processors.len > 0 and
      int(partition) >= gGlobalExecutionConfig.processors.len:
    return QUIC_STATUS_INVALID_PARAMETER
  let status = msquicConnectionOpen(registration, handler, context, connection)
  if status != QUIC_STATUS_SUCCESS or connection.isNil or connection[].isNil:
    return status
  let state = toConnection(connection[])
  if state.isNil:
    return QUIC_STATUS_INVALID_STATE
  state.partitionId = partition
  emitDiagnostics(DiagnosticsEvent(
    kind: diagConnectionEvent,
    handle: connection[],
    note: "connection partition=" & $partition
  ))
  QUIC_STATUS_SUCCESS

proc msquicStreamProvideReceiveBuffers(stream: HQUIC; bufferCount: uint32;
    buffers: ptr QuicBuffer): QUIC_STATUS {.cdecl.} =
  let state = toStream(stream)
  if state.isNil:
    return QUIC_STATUS_INVALID_PARAMETER
  if bufferCount > 0 and buffers.isNil:
    return QUIC_STATUS_INVALID_PARAMETER
  var total = 0'u64
  if bufferCount > 0 and not buffers.isNil:
    let bufArray = cast[ptr UncheckedArray[QuicBuffer]](buffers)
    for i in 0 ..< int(bufferCount):
      total += uint64(bufArray[i].Length)
  state.receiveBuffersProvided += total
  QUIC_STATUS_SUCCESS

proc registerConnectionEventHandler*(connection: HQUIC; handler: ConnectionEventHandler) {.exportc.} =
  let state = toConnection(connection)
  if state.isNil or handler.isNil:
    return
  if state.eventHandlers.len == 0:
    state.eventHandlers = @[]
  state.eventHandlers.add(handler)
  if state.handshakeComplete:
    let negotiatedAlpn =
      if state.configuration.isNil or state.configuration.alpns.len == 0: ""
      else: state.configuration.alpns[0]
    var connectedEvent = ConnectionEvent(
      kind: ceConnected,
      sessionResumed: state.sessionResumed,
      negotiatedAlpn: negotiatedAlpn,
      note: "connected replay"
    )
    connectedEvent.connection = connection
    handler(connectedEvent)

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

proc getConnectionCongestionAllowanceForTest*(connection: HQUIC;
    allowanceBytes: var uint64): bool {.exportc.} =
  let state = toConnection(connection)
  if state.isNil:
    return false
  allowanceBytes = qcc.sendAllowance(state.congestionController, nowMicros())
  true

proc setConnectionCongestionBytesInFlightForTest*(connection: HQUIC;
    bytesInFlight: uint32): bool {.exportc.} =
  let state = toConnection(connection)
  if state.isNil:
    return false
  case state.congestionAlgorithm
  of caCubic:
    state.congestionController.cubic.bytesInFlight = bytesInFlight
    state.congestionController.cubic.bytesInFlightMax =
      max(state.congestionController.cubic.bytesInFlightMax, bytesInFlight)
  of caBbr:
    state.congestionController.bbr.bytesInFlight = bytesInFlight
    state.congestionController.bbr.bytesInFlightMax =
      max(state.congestionController.bbr.bytesInFlightMax, bytesInFlight)
  true

proc setConnectionCongestionAlgorithmForTest*(connection: HQUIC;
    algorithm: CongestionAlgorithm): bool {.exportc.} =
  let state = toConnection(connection)
  if state.isNil:
    return false
  if state.congestionAlgorithm == algorithm:
    return true
  state.congestionAlgorithm = algorithm
  qcc.switchAlgorithm(state.congestionController, algorithm)
  true

proc setConnectionBbrAppLimitedForTest*(connection: HQUIC; enabled: bool): bool {.exportc.} =
  let state = toConnection(connection)
  if state.isNil:
    return false
  state.congestionController.bbr.bandwidthFilter.appLimited = enabled
  true

proc getConnectionBbrAppLimitedForTest*(connection: HQUIC; enabled: var bool): bool {.exportc.} =
  let state = toConnection(connection)
  if state.isNil:
    return false
  enabled = state.congestionController.bbr.bandwidthFilter.appLimited
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

proc getConnectionProbeCountForEpoch*(connection: HQUIC; epoch: CryptoEpoch;
    probeCount: var uint16): bool {.exportc.} =
  let state = toConnection(connection)
  if state.isNil:
    return false
  probeCount = state.lossDetection.probeCountByEpoch[epoch]
  true

proc setConnectionProbeCountForEpochForTest*(connection: HQUIC; epoch: CryptoEpoch;
    probeCount: uint16): bool {.exportc.} =
  let state = toConnection(connection)
  if state.isNil:
    return false
  state.lossDetection.probeCountByEpoch[epoch] = probeCount
  state.lossDetection.probeCount = 0'u16
  for candidate in CryptoEpoch:
    state.lossDetection.probeCount =
      max(state.lossDetection.probeCount, state.lossDetection.probeCountByEpoch[candidate])
  true

proc expireBuiltinResumptionEntryForTest*(serverName: cstring; serverPort: uint16;
    alpn: cstring): bool {.exportc.} =
  let serverNameText = if serverName.isNil: "" else: $serverName
  let alpnText = if alpn.isNil: "" else: $alpn
  let key = resumptionCacheKey(serverNameText, serverPort, alpnText)
  if key.len == 0:
    return false
  if not gBuiltinResumptionCache.hasKey(key):
    # Test helper semantics are idempotent: an already-invalidated / absent entry
    # is treated as successfully expired.
    return true
  var entry = gBuiltinResumptionCache[key]
  entry.expiresAtUs = 1'u64
  gBuiltinResumptionCache[key] = entry
  true

proc setBuiltinResumptionZeroRttProfileForTest*(serverName: cstring; serverPort: uint16;
    alpn: cstring; issuedAtUs: uint64; maxEarlyData: uint32): bool {.exportc.} =
  let serverNameText = if serverName.isNil: "" else: $serverName
  let alpnText = if alpn.isNil: "" else: $alpn
  let key = resumptionCacheKey(serverNameText, serverPort, alpnText)
  if key.len == 0 or not gBuiltinResumptionCache.hasKey(key):
    return false
  var entry = gBuiltinResumptionCache[key]
  var meta: BuiltinResumptionTicketMeta
  if not parseBuiltinResumptionTicket(entry.ticket, meta):
    return false
  let nextIssuedAtUs =
    if issuedAtUs > 0'u64: issuedAtUs
    elif meta.issuedAtUs > 0'u64: meta.issuedAtUs
    else: nowMicros()
  entry.ticket = buildBuiltinResumptionTicket(
    meta.serverName,
    meta.serverPort,
    meta.alpn,
    meta.data,
    issuedAtUs = nextIssuedAtUs,
    expiresAtUs = meta.expiresAtUs,
    maxEarlyData = maxEarlyData,
    ticketAgeAdd = meta.ticketAgeAdd,
    preserveTicketAgeAdd = true
  )
  entry.issuedAtUs = nextIssuedAtUs
  entry.maxEarlyData = maxEarlyData
  gBuiltinResumptionCache[key] = entry
  true

proc setBuiltinResumptionTicketAlpnForTest*(serverName: cstring; serverPort: uint16;
    alpn: cstring; ticketAlpn: cstring): bool {.exportc.} =
  let serverNameText = if serverName.isNil: "" else: $serverName
  let alpnText = if alpn.isNil: "" else: $alpn
  let ticketAlpnText = if ticketAlpn.isNil: "" else: $ticketAlpn
  let key = resumptionCacheKey(serverNameText, serverPort, alpnText)
  if key.len == 0 or not gBuiltinResumptionCache.hasKey(key):
    return false
  var entry = gBuiltinResumptionCache[key]
  var meta: BuiltinResumptionTicketMeta
  if not parseBuiltinResumptionTicket(entry.ticket, meta):
    return false
  entry.ticket = buildBuiltinResumptionTicket(
    meta.serverName,
    meta.serverPort,
    ticketAlpnText,
    meta.data,
    issuedAtUs = meta.issuedAtUs,
    expiresAtUs = meta.expiresAtUs,
    maxEarlyData = meta.maxEarlyData,
    ticketAgeAdd = meta.ticketAgeAdd,
    preserveTicketAgeAdd = true
  )
  gBuiltinResumptionCache[key] = entry
  true

proc getBuiltinResumptionTicketForTest*(serverName: cstring; serverPort: uint16;
    alpn: cstring; ticket: var seq[byte]): bool =
  let serverNameText = if serverName.isNil: "" else: $serverName
  let alpnText = if alpn.isNil: "" else: $alpn
  let key = resumptionCacheKey(serverNameText, serverPort, alpnText)
  if key.len == 0 or not gBuiltinResumptionCache.hasKey(key):
    return false
  ticket = gBuiltinResumptionCache[key].ticket
  true

proc getBuiltinResumptionTicketMetaForTest*(serverName: cstring; serverPort: uint16;
    alpn: cstring; issuedAtUs: var uint64; expiresAtUs: var uint64;
    maxEarlyData: var uint32): bool =
  let serverNameText = if serverName.isNil: "" else: $serverName
  let alpnText = if alpn.isNil: "" else: $alpn
  let key = resumptionCacheKey(serverNameText, serverPort, alpnText)
  if key.len == 0 or not gBuiltinResumptionCache.hasKey(key):
    return false
  var meta: BuiltinResumptionTicketMeta
  if not parseBuiltinResumptionTicket(gBuiltinResumptionCache[key].ticket, meta):
    return false
  issuedAtUs = meta.issuedAtUs
  expiresAtUs = meta.expiresAtUs
  maxEarlyData = meta.maxEarlyData
  true

proc getBuiltinResumptionTicketAgeAddForTest*(serverName: cstring; serverPort: uint16;
    alpn: cstring; ticketAgeAdd: var uint32): bool =
  let serverNameText = if serverName.isNil: "" else: $serverName
  let alpnText = if alpn.isNil: "" else: $alpn
  let key = resumptionCacheKey(serverNameText, serverPort, alpnText)
  if key.len == 0 or not gBuiltinResumptionCache.hasKey(key):
    return false
  var meta: BuiltinResumptionTicketMeta
  if not parseBuiltinResumptionTicket(gBuiltinResumptionCache[key].ticket, meta):
    return false
  ticketAgeAdd = meta.ticketAgeAdd
  true

proc setBuiltinResumptionTicketForTest*(serverName: cstring; serverPort: uint16;
    alpn: cstring; ticket: seq[byte]): bool =
  let serverNameText = if serverName.isNil: "" else: $serverName
  let alpnText = if alpn.isNil: "" else: $alpn
  let key = resumptionCacheKey(serverNameText, serverPort, alpnText)
  if key.len == 0 or ticket.len == 0:
    return false
  var meta: BuiltinResumptionTicketMeta
  if not parseBuiltinResumptionTicket(ticket, meta):
    return false
  gBuiltinResumptionCache[key] = BuiltinResumptionEntry(
    ticket: ticket,
    data: meta.data,
    issuedAtUs: (if meta.issuedAtUs > 0'u64: meta.issuedAtUs else: nowMicros()),
    expiresAtUs: meta.expiresAtUs,
    maxEarlyData: meta.maxEarlyData
  )
  true

proc clearBuiltinZeroRttReplayCacheForTest*() =
  gBuiltinZeroRttReplayCache.clear()

proc setForceRejectBuiltinZeroRttForTest*(enabled: bool) =
  gForceRejectBuiltinZeroRttForTest = enabled

proc setDropBuiltinEarlyDataRequestForTest*(enabled: bool) =
  gDropBuiltinEarlyDataRequestForTest = enabled

proc setDropBuiltinTicketAgeForTest*(enabled: bool) =
  gDropBuiltinTicketAgeForTest = enabled

proc setDropBuiltinResumptionBinderForTest*(enabled: bool) =
  gDropBuiltinResumptionBinderForTest = enabled

proc setTamperBuiltinResumptionBinderForTest*(enabled: bool) =
  gTamperBuiltinResumptionBinderForTest = enabled

proc setTamperBuiltinResumptionTicketForTest*(enabled: bool) =
  gTamperBuiltinResumptionTicketForTest = enabled

proc setOverrideBuiltinTicketServerNameForTest*(serverName: cstring) =
  gHasOverrideBuiltinTicketServerNameForTest = true
  gOverrideBuiltinTicketServerNameForTest =
    if serverName.isNil: ""
    else: $serverName

proc clearOverrideBuiltinTicketServerNameForTest*() =
  gHasOverrideBuiltinTicketServerNameForTest = false
  gOverrideBuiltinTicketServerNameForTest = ""

proc setOverrideBuiltinTicketServerPortForTest*(serverPort: uint16) =
  gHasOverrideBuiltinTicketServerPortForTest = true
  gOverrideBuiltinTicketServerPortForTest = serverPort

proc clearOverrideBuiltinTicketServerPortForTest*() =
  gHasOverrideBuiltinTicketServerPortForTest = false
  gOverrideBuiltinTicketServerPortForTest = 0'u16

proc setOverrideBuiltinTicketAlpnForTest*(alpn: cstring) =
  gHasOverrideBuiltinTicketAlpnForTest = true
  gOverrideBuiltinTicketAlpnForTest =
    if alpn.isNil: ""
    else: $alpn

proc clearOverrideBuiltinTicketAlpnForTest*() =
  gHasOverrideBuiltinTicketAlpnForTest = false
  gOverrideBuiltinTicketAlpnForTest = ""

proc setOverrideBuiltinTicketAgeDeltaMsForTest*(deltaMs: int64) =
  gHasOverrideBuiltinTicketAgeDeltaMsForTest = true
  gOverrideBuiltinTicketAgeDeltaMsForTest = deltaMs

proc clearOverrideBuiltinTicketAgeDeltaMsForTest*() =
  gHasOverrideBuiltinTicketAgeDeltaMsForTest = false
  gOverrideBuiltinTicketAgeDeltaMsForTest = 0'i64

proc setOverrideBuiltinNewSessionTicketLifetimeSecForTest*(ticketLifetimeSec: uint32) =
  gHasOverrideBuiltinNewSessionTicketLifetimeSecForTest = true
  gOverrideBuiltinNewSessionTicketLifetimeSecForTest = ticketLifetimeSec

proc clearOverrideBuiltinNewSessionTicketLifetimeSecForTest*() =
  gHasOverrideBuiltinNewSessionTicketLifetimeSecForTest = false
  gOverrideBuiltinNewSessionTicketLifetimeSecForTest = 600'u32

proc setOverrideBuiltinNewSessionTicketMaxEarlyDataForTest*(maxEarlyData: uint32) =
  gHasOverrideBuiltinNewSessionTicketMaxEarlyDataForTest = true
  gOverrideBuiltinNewSessionTicketMaxEarlyDataForTest = maxEarlyData

proc clearOverrideBuiltinNewSessionTicketMaxEarlyDataForTest*() =
  gHasOverrideBuiltinNewSessionTicketMaxEarlyDataForTest = false
  gOverrideBuiltinNewSessionTicketMaxEarlyDataForTest = BuiltinDefaultMaxEarlyData

proc claimBuiltinZeroRttReplayForTest*(ticket: seq[byte]; binder: seq[byte];
    expiresAtUs: uint64): bool =
  claimBuiltinZeroRttReplay(ticket, binder, expiresAtUs)

proc seedConnectionIdsForTest*(connection: HQUIC; localCidBytes: seq[byte];
    peerCidBytes: seq[byte]; isClient = true): bool =
  let state = toConnection(connection)
  if state.isNil:
    return false
  if localCidBytes.len == 0 or localCidBytes.len > MaxCidLength.int or
      peerCidBytes.len == 0 or peerCidBytes.len > MaxCidLength.int:
    return false
  state.localCid = initConnectionId(localCidBytes)
  state.peerCid = initConnectionId(peerCidBytes)
  state.localCidSequence = 0'u64
  state.peerCidSequence = 0'u64
  state.activePeerCidSequence = 0'u64
  state.activeLocalCidSequence = 0'u64
  state.advertisedPeerCids = @[]
  state.issuedLocalCids = @[]
  state.recordAdvertisedPeerCid(0'u64, state.peerCid, default(array[16, uint8]))
  state.recordIssuedLocalCid(0'u64, state.localCid)
  state.quicConn = newConnection(
    (if isClient: crClient else: crServer),
    state.localCid,
    state.peerCid,
    QuicVersion(1)
  )
  state.isClient = isClient
  state.peerAddressValidated = not isClient
  state.initLocalStreamIds()
  true

proc getConnectionPeerCidBytesForTest*(connection: HQUIC): seq[byte] =
  let state = toConnection(connection)
  if state.isNil:
    return @[]
  cidBytes(state.peerCid)

proc getConnectionActivePeerCidSequenceForTest*(connection: HQUIC;
    sequence: var uint64): bool =
  let state = toConnection(connection)
  if state.isNil:
    return false
  sequence = state.activePeerCidSequence
  true

proc getConnectionPeerCidCatalogCountForTest*(connection: HQUIC;
    count: var uint32): bool =
  let state = toConnection(connection)
  if state.isNil:
    return false
  count = uint32(state.advertisedPeerCids.len)
  true

proc getConnectionRetiredPeerCidCountForTest*(connection: HQUIC;
    count: var uint32): bool =
  let state = toConnection(connection)
  if state.isNil:
    return false
  var retired = 0'u32
  for item in state.advertisedPeerCids:
    if item.retired:
      inc(retired)
  count = retired
  true

proc getConnectionLocalCidBytesForTest*(connection: HQUIC): seq[byte] =
  let state = toConnection(connection)
  if state.isNil:
    return @[]
  cidBytes(state.localCid)

proc getConnectionStatelessResetTokenCountForTest*(connection: HQUIC; count: var uint32): bool =
  let state = toConnection(connection)
  if state.isNil or state.quicConn.isNil:
    return false
  count = uint32(state.quicConn.model.migration.statelessResetTokens.len)
  true

proc getConnectionSessionResumedForTest*(connection: HQUIC; resumed: var bool): bool =
  let state = toConnection(connection)
  if state.isNil:
    return false
  resumed = state.sessionResumed
  true

proc setConnectionPeerAddressValidatedForTest*(connection: HQUIC;
    validated: bool): bool {.exportc.} =
  let state = toConnection(connection)
  if state.isNil:
    return false
  state.peerAddressValidated = validated
  true

proc getConnectionZeroRttBudgetForTest*(connection: HQUIC; maxData: var uint32;
    sentBytes: var uint64; deadlineUs: var uint64): bool =
  let state = toConnection(connection)
  if state.isNil:
    return false
  maxData = state.zeroRttMaxData
  sentBytes = state.zeroRttSentBytes
  deadlineUs = state.zeroRttAcceptDeadlineUs
  true

proc getConnectionSentPacketCountForTest*(connection: HQUIC; count: var uint32): bool =
  let state = toConnection(connection)
  if state.isNil:
    return false
  count = uint32(state.sentPackets.len)
  true

proc getConnectionLastSentFramePayloadLenForTest*(connection: HQUIC;
    payloadLen: var uint32): bool =
  let state = toConnection(connection)
  if state.isNil or state.sentPackets.len == 0:
    return false
  payloadLen = uint32(state.sentPackets[^1].framePayload.len)
  true

proc getConnectionLastSentFrameKindForTest*(connection: HQUIC;
    frameKind: var SentFrameKind): bool =
  let state = toConnection(connection)
  if state.isNil or state.sentPackets.len == 0:
    return false
  frameKind = state.sentPackets[^1].frameKind
  true

proc getConnectionLastSentFramePayloadForTest*(connection: HQUIC;
    payload: var seq[byte]): bool =
  let state = toConnection(connection)
  if state.isNil or state.sentPackets.len == 0:
    return false
  payload = state.sentPackets[^1].framePayload
  true

proc getConnectionLastSentTargetRemoteForTest*(connection: HQUIC;
    host: var string; port: var uint16): bool =
  let state = toConnection(connection)
  if state.isNil or state.sentPackets.len == 0:
    return false
  let remote = state.sentPackets[^1].targetRemote
  if remote.family == AddressFamily.None:
    return false
  host = $remote
  port = uint16(remote.port)
  true

proc getConnectionSentFrameKindAtForTest*(connection: HQUIC; index: uint32;
    frameKind: var SentFrameKind): bool =
  let state = toConnection(connection)
  if state.isNil:
    return false
  let idx = int(index)
  if idx < 0 or idx >= state.sentPackets.len:
    return false
  frameKind = state.sentPackets[idx].frameKind
  true

proc getConnectionSentTargetRemoteAtForTest*(connection: HQUIC; index: uint32;
    host: var string; port: var uint16): bool =
  let state = toConnection(connection)
  if state.isNil:
    return false
  let idx = int(index)
  if idx < 0 or idx >= state.sentPackets.len:
    return false
  let remote = state.sentPackets[idx].targetRemote
  if remote.family == AddressFamily.None:
    return false
  host = $remote
  port = uint16(remote.port)
  true

proc getConnectionSentFramePayloadAtForTest*(connection: HQUIC; index: uint32;
    payload: var seq[byte]): bool =
  let state = toConnection(connection)
  if state.isNil:
    return false
  let idx = int(index)
  if idx < 0 or idx >= state.sentPackets.len:
    return false
  payload = state.sentPackets[idx].framePayload
  true

proc setConnectionRemoteAddressForTest*(connection: HQUIC; remoteHost: cstring;
    remotePort: uint16): bool =
  let state = toConnection(connection)
  if state.isNil or remoteHost.isNil or remoteHost.len == 0:
    return false
  let remote = initTAddress($remoteHost, Port(remotePort))
  if remote.family == AddressFamily.None:
    return false
  state.remoteAddress = remote
  state.activeRemoteKey = remoteAddressKey(remote)
  true

proc setConnectionPendingAckRemoteForTest*(connection: HQUIC; epoch: CryptoEpoch;
    remoteHost: cstring; remotePort: uint16): bool =
  let state = toConnection(connection)
  if state.isNil or remoteHost.isNil or remoteHost.len == 0:
    return false
  let remote = initTAddress($remoteHost, Port(remotePort))
  if remote.family == AddressFamily.None:
    return false
  state.setPendingAckRemote(epoch, remote)
  true

proc getConnectionPendingAckRemoteForTest*(connection: HQUIC; epoch: CryptoEpoch;
    host: var string; port: var uint16): bool =
  let state = toConnection(connection)
  if state.isNil:
    return false
  let remote = state.pendingAckRemote(epoch)
  if remote.isNil or remote[].family == AddressFamily.None:
    return false
  host = $remote[]
  port = uint16(remote[].port)
  true

proc createAcceptedConnectionForTest*(listener: HQUIC;
    remoteHost: cstring; remotePort: uint16;
    localHost: cstring; localPort: uint16;
    clientCidPtr: ptr uint8; clientCidLen: uint32;
    serverCidPtr: ptr uint8; serverCidLen: uint32;
    connection: var HQUIC): bool =
  let state = toListener(listener)
  if state.isNil or remoteHost.isNil or localHost.isNil or
      clientCidPtr.isNil or serverCidPtr.isNil:
    return false
  if clientCidLen == 0'u32 or serverCidLen == 0'u32:
    return false
  var clientCid = newSeq[byte](clientCidLen.int)
  var serverCid = newSeq[byte](serverCidLen.int)
  copyMem(addr clientCid[0], clientCidPtr, clientCid.len)
  copyMem(addr serverCid[0], serverCidPtr, serverCid.len)
  let remote = initTAddress($remoteHost, Port(remotePort))
  let local = initTAddress($localHost, Port(localPort))
  if remote.family == AddressFamily.None or local.family == AddressFamily.None:
    return false
  let conn = newAcceptedConnection(state, remote, local, clientCid, serverCid)
  state.registerAcceptedConnection(remoteAddressKey(remote), conn)
  connection = cast[HQUIC](conn)
  true

proc getListenerAcceptedConnectionUniqueCountForTest*(listener: HQUIC;
    count: var uint32): bool =
  let state = toListener(listener)
  if state.isNil:
    return false
  var seen: seq[uint] = @[]
  for conn in state.acceptedConnections.values:
    let key = cast[uint](conn)
    if key notin seen:
      seen.add(key)
  count = uint32(seen.len)
  true

proc findListenerAcceptedConnectionByDestCidForTest*(listener: HQUIC;
    destCidPtr: ptr uint8; destCidLen: uint32; connection: var HQUIC): bool =
  let state = toListener(listener)
  if state.isNil or destCidPtr.isNil or destCidLen == 0'u32:
    return false
  var destCid = newSeq[byte](destCidLen.int)
  copyMem(addr destCid[0], destCidPtr, destCid.len)
  let incomingCid = initConnectionId(destCid)
  for conn in state.acceptedConnections.values:
    if not conn.isNil and conn.localCidAccepted(incomingCid):
      connection = cast[HQUIC](conn)
      return true
  false

proc resolveListenerAcceptedConnectionForRemoteAndDestCidForTest*(listener: HQUIC;
    remoteHost: cstring; remotePort: uint16;
    destCidPtr: ptr uint8; destCidLen: uint32; connection: var HQUIC): bool =
  let state = toListener(listener)
  if state.isNil or remoteHost.isNil or remoteHost.len == 0 or
      destCidPtr.isNil or destCidLen == 0'u32:
    return false
  let remote = initTAddress($remoteHost, Port(remotePort))
  if remote.family == AddressFamily.None:
    return false
  var destCid = newSeq[byte](destCidLen.int)
  copyMem(addr destCid[0], destCidPtr, destCid.len)
  let incomingCid = initConnectionId(destCid)
  let remoteKey = remoteAddressKey(remote)
  var conn = state.acceptedConnections.getOrDefault(remoteKey, nil)
  if not conn.isNil and not conn.localCidAccepted(incomingCid):
    conn = nil
  if conn.isNil:
    for candidate in state.acceptedConnections.values:
      if not candidate.isNil and candidate.localCidAccepted(incomingCid):
        conn = candidate
        break
    if not conn.isNil:
      state.registerAcceptedConnection(remoteKey, conn)
  if conn.isNil:
    return false
  connection = cast[HQUIC](conn)
  true

proc receiveZeroRttPayloadForTest*(connection: HQUIC; packetNumber: uint64;
    payload: seq[byte]; remoteHost: cstring; remotePort: uint16): bool =
  let state = toConnection(connection)
  if state.isNil:
    return false
  var remote =
    if remoteHost.isNil or remoteHost.len == 0:
      state.remoteAddress
    else:
      initTAddress($remoteHost, Port(remotePort))
  if remote.family == AddressFamily.None:
    return false
  state.handleZeroRttPayload(remote, packetNumber, payload)
  true

proc receiveOneRttPayloadForTest*(connection: HQUIC; packetNumber: uint64;
    payload: seq[byte]; remoteHost: cstring; remotePort: uint16): bool =
  let state = toConnection(connection)
  if state.isNil:
    return false
  var remote =
    if remoteHost.isNil or remoteHost.len == 0:
      state.remoteAddress
    else:
      initTAddress($remoteHost, Port(remotePort))
  if remote.family == AddressFamily.None:
    return false
  state.handleOneRttPayload(remote, packetNumber, payload)
  true

proc flushPendingOneRttAckForTest*(connection: HQUIC; remoteHost: cstring = nil;
    remotePort: uint16 = 0'u16): bool =
  let state = toConnection(connection)
  if state.isNil:
    return false
  var remotePtr: ptr TransportAddress = nil
  var remote: TransportAddress
  if not remoteHost.isNil and remoteHost.len > 0:
    remote = initTAddress($remoteHost, Port(remotePort))
    if remote.family == AddressFamily.None:
      return false
    remotePtr = addr remote
  state.maybeSendPendingOneRttAck(remotePtr)

proc getConnectionPendingAckStateForTest*(connection: HQUIC;
    pendingAckRanges: var uint32; ackElicitingPacketsToAck: var uint16;
    alreadyWrittenAckFrame: var bool;
    epoch: CryptoEpoch = ceOneRtt): bool =
  let state = toConnection(connection)
  if state.isNil:
    return false
  pendingAckRanges = uint32(qack.packetNumbersToAck(state.ackTracker, epoch).len)
  ackElicitingPacketsToAck = qack.ackElicitingPacketsToAck(state.ackTracker, epoch)
  alreadyWrittenAckFrame = qack.alreadyWrittenAckFrame(state.ackTracker, epoch)
  true

proc getConnectionPendingAckDeadlineForTest*(connection: HQUIC;
    deadlineUs: var uint64; epoch: CryptoEpoch = ceOneRtt): bool =
  let state = toConnection(connection)
  if state.isNil:
    return false
  deadlineUs = qack.ackDeadlineUs(state.ackTracker, epoch)
  true

proc markPendingAckForTest*(connection: HQUIC; packetNumber: uint64;
    recvTimeUs: uint64 = 0'u64; epoch: CryptoEpoch = ceOneRtt): bool =
  let state = toConnection(connection)
  if state.isNil:
    return false
  let effectiveRecvTime =
    if recvTimeUs > 0'u64: recvTimeUs
    else: nowMicros()
  qack.markForAck(state.ackTracker, epoch, packetNumber, effectiveRecvTime, ackTypeAckImmediate)
  true

proc markDelayedPendingAckForTest*(connection: HQUIC; packetNumber: uint64;
    recvTimeUs: uint64 = 0'u64; ackDelayMs: uint32 = MaxAckDelayDefaultMs;
    epoch: CryptoEpoch = ceOneRtt): bool =
  let state = toConnection(connection)
  if state.isNil:
    return false
  let effectiveRecvTime =
    if recvTimeUs > 0'u64: recvTimeUs
    else: nowMicros()
  qack.markForAck(
    state.ackTracker,
    epoch,
    packetNumber,
    effectiveRecvTime,
    ackTypeAckEliciting,
    ackDelayMs
  )
  true

proc sendAckForTest*(connection: HQUIC; epoch: CryptoEpoch; remoteHost: cstring = nil;
    remotePort: uint16 = 0'u16): bool =
  let state = toConnection(connection)
  if state.isNil:
    return false
  var remote =
    if remoteHost.isNil or remoteHost.len == 0:
      state.remoteAddress
    else:
      initTAddress($remoteHost, Port(remotePort))
  if remote.family == AddressFamily.None:
    remote = state.remoteAddress
  if remote.family == AddressFamily.None:
    return false
  result = state.sendAckFrame(
    qack.largestPacketNumberAcked(state.ackTracker, epoch),
    epoch,
    unsafeAddr remote
  )

proc getConnectionClientHelloRequestsEarlyDataForTest*(connection: HQUIC;
    requested: var bool): bool =
  let state = toConnection(connection)
  if state.isNil or state.transcript.len == 0:
    return false
  requested = tls.clientHelloRequestsEarlyData(state.transcript)
  true

proc getConnectionResumptionTicketMetaForTest*(connection: HQUIC;
    issuedAtUs: var uint64; expiresAtUs: var uint64; maxEarlyData: var uint32): bool =
  let state = toConnection(connection)
  if state.isNil or state.resumptionTicket.len == 0:
    return false
  var meta: BuiltinResumptionTicketMeta
  if not parseBuiltinResumptionTicket(state.resumptionTicket, meta):
    return false
  issuedAtUs = meta.issuedAtUs
  expiresAtUs = meta.expiresAtUs
  maxEarlyData = meta.maxEarlyData
  true

proc getConnectionResumptionTicketAgeAddForTest*(connection: HQUIC;
    ticketAgeAdd: var uint32): bool =
  let state = toConnection(connection)
  if state.isNil or state.resumptionTicket.len == 0:
    return false
  var meta: BuiltinResumptionTicketMeta
  if not parseBuiltinResumptionTicket(state.resumptionTicket, meta):
    return false
  ticketAgeAdd = meta.ticketAgeAdd
  true

proc getConnectionLossTimeForTest*(connection: HQUIC; epoch: CryptoEpoch; lossTimeUs: var uint64): bool =
  let state = toConnection(connection)
  if state.isNil or state.quicConn.isNil:
    return false
  lossTimeUs = state.quicConn.model.packetSpaces[epoch].lossTime
  true

proc getConnectionLossDetectionTimerForTest*(connection: HQUIC;
    timerUs: var uint64): bool =
  let state = toConnection(connection)
  if state.isNil or state.quicConn.isNil:
    return false
  timerUs = state.quicConn.model.timers.lossDetectionTimer
  true

proc getConnectionLossDetectionEpochForTest*(connection: HQUIC;
    epoch: var CryptoEpoch; dueToLossTime: var bool): bool =
  let state = toConnection(connection)
  if state.isNil or state.quicConn.isNil:
    return false
  var timerUs = 0'u64
  state.selectLossDetectionSchedule(epoch, timerUs, dueToLossTime)

proc getConnectionLossFlightForTest*(connection: HQUIC; epoch: CryptoEpoch;
    packetsInFlight: var uint32; bytesInFlight: var uint64): bool =
  let state = toConnection(connection)
  if state.isNil:
    return false
  packetsInFlight = state.lossDetection.packetsInFlightByEpoch[epoch]
  bytesInFlight = state.lossDetection.bytesInFlightByEpoch[epoch]
  true

proc getConnectionLossAckStateForTest*(connection: HQUIC;
    timeOfLastAckedPacketSent: var uint64;
    totalBytesSentAtLastAck: var uint64): bool =
  let state = toConnection(connection)
  if state.isNil:
    return false
  timeOfLastAckedPacketSent = state.lossDetection.timeOfLastAckedPacketSent
  totalBytesSentAtLastAck = state.lossDetection.totalBytesSentAtLastAck
  true

proc setConnectionLossTimeForTest*(connection: HQUIC; epoch: CryptoEpoch;
    lossTimeUs: uint64): bool =
  let state = toConnection(connection)
  if state.isNil or state.quicConn.isNil:
    return false
  state.quicConn.model.packetSpaces[epoch].lossTime = lossTimeUs
  true

proc setConnectionRttStatsForTest*(connection: HQUIC; latestRttUs,
    smoothedRttUs, minRttUs, rttVarianceUs: uint64): bool =
  let state = toConnection(connection)
  if state.isNil:
    return false
  state.latestRttUs = latestRttUs
  state.smoothedRttUs = smoothedRttUs
  state.minRttUs = minRttUs
  state.rttVarianceUs = rttVarianceUs
  true

proc setConnectionHandshakeStateForTest*(connection: HQUIC;
    handshakeComplete: bool): bool =
  let state = toConnection(connection)
  if state.isNil:
    return false
  state.handshakeComplete = handshakeComplete
  true

proc completeServerHandshakeForTest*(connection: HQUIC): bool {.exportc.} =
  let state = toConnection(connection)
  if state.isNil:
    return false
  state.completeServerHandshake("test server handshake complete")
  true

proc prepareConnectionPacketSendForTest*(connection: HQUIC; epoch: CryptoEpoch): bool {.exportc.} =
  let state = toConnection(connection)
  if state.isNil:
    return false
  if state.quicConn.isNil or state.localCid.length == 0'u8 or state.peerCid.length == 0'u8:
    if not seedConnectionIdsForTest(connection, @[0x01'u8, 0x02, 0x03, 0x04], @[0xAA'u8, 0xBB, 0xCC, 0xDD]):
      return false
  if state.transport.isNil:
    proc discardPacket(transp: DatagramTransport; raddr: TransportAddress): Future[void] {.async: (raises: []).} =
      discard transp
      discard raddr
    state.transport = newDatagramTransport(discardPacket)
    state.localAddress = state.transport.localAddress
  if state.remoteAddress.family == AddressFamily.None or state.remoteAddress.port == Port(0):
    state.remoteAddress = initTAddress("127.0.0.1", Port(44444))
  if state.serverName.len == 0:
    state.serverName = "127.0.0.1"
  if state.serverPort == 0'u16:
    state.serverPort = uint16(state.remoteAddress.port)
  case epoch
  of ceInitial:
    state.initialSecrets = tls.deriveInitialSecrets(cidBytes(state.peerCid))
  of ceHandshake:
    var sharedSecret = newSeq[byte](32)
    var helloHash = newSeq[byte](32)
    for idx in 0 ..< 32:
      sharedSecret[idx] = byte(0x41 + idx)
      helloHash[idx] = byte(0x61 + idx)
    state.handshakeKeys = tls.deriveHandshakeSecrets(sharedSecret, helloHash)
  of ceZeroRtt:
    let ticket = buildBuiltinResumptionTicket(
      state.serverName,
      state.serverPort,
      "test-alpn",
      @[],
      issuedAtUs = nowMicros(),
      maxEarlyData = 16_384'u32
    )
    state.resumptionTicket = ticket
    state.sessionResumed = true
    discard state.applyZeroRttTicketState(ticket)
  of ceOneRtt:
    var appSecret = newSeq[byte](32)
    for idx in 0 ..< 32:
      appSecret[idx] = byte(0x21 + idx)
    state.oneRttKeys = tls.deriveApplicationSecrets(appSecret, appSecret)
    state.handshakeComplete = true
  else:
    discard
  true

proc sendConnectionEpochPacketForTest*(connection: HQUIC; epoch: CryptoEpoch;
    payloadLen: uint32 = 1200'u32; ackEliciting = true): bool {.exportc.} =
  let state = toConnection(connection)
  if state.isNil or payloadLen == 0'u32:
    return false
  if not prepareConnectionPacketSendForTest(connection, epoch):
    return false
  let payload = newSeq[byte](int(payloadLen))
  let packetNumber =
    case epoch
    of ceInitial:
      state.sendInitialPacket(payload, sfkCrypto, ackEliciting)
    of ceHandshake:
      state.sendHandshakePacket(payload, sfkCrypto, ackEliciting)
    of ceZeroRtt:
      state.sendZeroRttPacket(payload, sfkDatagram)
    of ceOneRtt:
      state.sendOneRttPacket(payload, sfkDatagram, ackEliciting)
  packetNumber > 0'u64

proc setConnectionPeerMaxAckDelayForTest*(connection: HQUIC;
    maxAckDelayMs: uint64): bool =
  let state = toConnection(connection)
  if state.isNil:
    return false
  state.peerTransportParams.maxAckDelayMs = maxAckDelayMs
  true

proc setConnectionPeerAckDelayExponentForTest*(connection: HQUIC;
    exponent: uint8): bool =
  let state = toConnection(connection)
  if state.isNil:
    return false
  state.peerTransportParams.ackDelayExponent = exponent
  true

proc setConnectionLocalMaxAckDelayForTest*(connection: HQUIC;
    maxAckDelayMs: uint64): bool =
  let state = toConnection(connection)
  if state.isNil:
    return false
  state.localTransportParams.maxAckDelayMs = maxAckDelayMs
  true

proc setConnectionLocalAckDelayExponentForTest*(connection: HQUIC;
    exponent: uint64): bool =
  let state = toConnection(connection)
  if state.isNil:
    return false
  state.localTransportParams.ackDelayExponent = exponent
  true

proc setConnectionLargestAckRecvTimeForTest*(connection: HQUIC;
    recvTimeUs: uint64; epoch: CryptoEpoch = ceOneRtt): bool =
  let state = toConnection(connection)
  if state.isNil:
    return false
  qack.setLargestPacketNumberRecvTime(state.ackTracker, epoch, recvTimeUs)
  true

proc getConnectionCurrentProbeTimeoutForTest*(connection: HQUIC; epoch: CryptoEpoch;
    timeoutUs: var uint64): bool =
  let state = toConnection(connection)
  if state.isNil:
    return false
  timeoutUs = state.currentProbeTimeoutUs(epoch)
  true

proc recomputeConnectionLossDetectionTimerForTest*(connection: HQUIC): bool =
  let state = toConnection(connection)
  if state.isNil:
    return false
  state.updateLossDetectionTimer()
  true

proc seedSentPacketForTest*(connection: HQUIC; epoch: CryptoEpoch; packetNumber: uint64;
    sentTimeUs: uint64; packetLength: uint16 = 1200'u16;
    ackEliciting = true; frameKind = sfkStream; appLimited = false;
    framePayload: seq[byte] = @[];
    targetRemote = TransportAddress(family: AddressFamily.None)): bool =
  let state = toConnection(connection)
  if state.isNil or state.quicConn.isNil:
    return false
  let meta = SentPacketMeta(
    packetNumber: packetNumber,
    epoch: epoch,
    ackEliciting: ackEliciting,
    appLimited: appLimited,
    packetLength: packetLength,
    sentTimeUs: sentTimeUs,
    totalBytesSentAtSend: state.lossDetection.totalBytesSent + uint64(packetLength),
    frameKind: frameKind,
    framePayload: framePayload,
    targetRemote: targetRemote
  )
  state.sentPackets.add(meta)
  if packetNumber > state.quicConn.model.packetSpaces[epoch].largestSent:
    state.quicConn.model.packetSpaces[epoch].largestSent = packetNumber
  qloss.onPacketSent(state.lossDetection, qloss.PacketRecord(
    packetNumber: packetNumber,
    encryptEpoch: epoch,
    sentTimeUs: sentTimeUs,
    ackEliciting: ackEliciting,
    packetLength: packetLength
  ))
  state.updateLossDetectionTimer()
  true

proc attachSentPacketStreamHandleForTest*(connection: HQUIC; packetNumber: uint64;
    epoch: CryptoEpoch; stream: HQUIC): bool =
  let conn = toConnection(connection)
  let streamState = toStream(stream)
  if conn.isNil or streamState.isNil:
    return false
  for idx in 0 ..< conn.sentPackets.len:
    if conn.sentPackets[idx].packetNumber == packetNumber and
        conn.sentPackets[idx].epoch == epoch:
      conn.sentPackets[idx].stream = cast[pointer](streamState)
      if conn.sentPackets[idx].frameKind == sfkStream and
          conn.sentPackets[idx].framePayload.len > 0:
        try:
          var pos = 0
          let frame = proto.parseStreamFrame(conn.sentPackets[idx].framePayload, pos)
          conn.sentPackets[idx].streamOffset = frame.offset
          conn.sentPackets[idx].streamPayload = frame.data
          conn.sentPackets[idx].streamFin = frame.fin
        except ValueError:
          return false
      return true
  false

proc applyAckForTest*(connection: HQUIC; epoch: CryptoEpoch; largestAcked: uint64;
    delay: uint64 = 0'u64): bool =
  let state = toConnection(connection)
  if state.isNil:
    return false
  state.applyAckFrame(proto.AckFrame(
    largestAcked: largestAcked,
    delay: delay,
    rangeCount: 0'u64,
    firstRange: 0'u64,
    ranges: @[proto.AckRange(smallest: largestAcked, largest: largestAcked)]
  ), epoch)
  true

proc applyAckRangesForTest*(connection: HQUIC; epoch: CryptoEpoch;
    ranges: openArray[(uint64, uint64)]; delay: uint64 = 0'u64): bool =
  let state = toConnection(connection)
  if state.isNil or ranges.len == 0:
    return false
  var ackRanges: seq[proto.AckRange] = @[]
  var largestAcked = 0'u64
  for entry in ranges:
    let smallest = min(entry[0], entry[1])
    let largest = max(entry[0], entry[1])
    ackRanges.add(proto.AckRange(smallest: smallest, largest: largest))
    if largest > largestAcked:
      largestAcked = largest
  state.applyAckFrame(proto.AckFrame(
    largestAcked: largestAcked,
    delay: delay,
    rangeCount: uint64(max(ackRanges.len - 1, 0)),
    firstRange: 0'u64,
    ranges: ackRanges
  ), epoch)
  true

proc advertiseLocalConnectionIdForTest*(connection: HQUIC; retirePriorTo: uint64 = 0'u64): bool =
  let state = toConnection(connection)
  if state.isNil:
    return false
  state.sendLocalNewConnectionId(retirePriorTo)

proc applyPeerNewConnectionIdForTest*(connection: HQUIC; sequence: uint64;
    retirePriorTo: uint64; cid: seq[byte];
    statelessResetToken: array[16, uint8]): bool =
  let state = toConnection(connection)
  if state.isNil:
    return false
  var tokenBytes: array[16, byte]
  for i in 0 ..< 16:
    tokenBytes[i] = statelessResetToken[i]
  state.applyPeerNewConnectionId(proto.NewConnectionIdFrame(
    sequence: sequence,
    retirePriorTo: retirePriorTo,
    connectionId: cid,
    statelessResetToken: tokenBytes
  ))

proc applyPeerRetireConnectionIdForTest*(connection: HQUIC; sequence: uint64): bool =
  let state = toConnection(connection)
  if state.isNil:
    return false
  state.applyPeerRetireConnectionId(sequence)

proc processStatelessResetForTest*(connection: HQUIC; token: array[16, uint8]): bool =
  let state = toConnection(connection)
  if state.isNil:
    return false
  var packet = newSeq[byte](21)
  for i in 0 ..< packet.len - 16:
    packet[i] = byte(0x40 + i)
  for i in 0 ..< 16:
    packet[packet.len - 16 + i] = token[i]
  state.handleStatelessReset(TransportAddress())

proc matchesStatelessResetTokenForTest*(connection: HQUIC;
    token: array[16, uint8]): bool =
  let state = toConnection(connection)
  if state.isNil:
    return false
  var packet = newSeq[byte](21)
  for i in 0 ..< packet.len - 16:
    packet[i] = byte(0x40 + i)
  for i in 0 ..< 16:
    packet[packet.len - 16 + i] = token[i]
  state.matchesStatelessResetToken(packet)

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

proc getConnectionPendingChallengeCountForTest*(connection: HQUIC;
    count: var uint32): bool =
  let state = toConnection(connection)
  if state.isNil or state.quicConn.isNil:
    return false
  count = uint32(state.quicConn.model.migration.pendingChallenges.len)
  true

proc prependPendingControlFrameForTest*(connection: HQUIC; epoch: CryptoEpoch;
    frameKind: SentFrameKind; payload: seq[byte];
    ackEliciting = true;
    targetRemote = TransportAddress(family: AddressFamily.None)): bool =
  let state = toConnection(connection)
  if state.isNil:
    return false
  state.prependPendingControlFrame(PendingControlFrame(
    epoch: epoch,
    payload: payload,
    frameKind: frameKind,
    ackEliciting: ackEliciting,
    targetRemote: targetRemote
  ))
  true

proc getConnectionPendingControlFrameCountForTest*(connection: HQUIC;
    count: var uint32): bool =
  let state = toConnection(connection)
  if state.isNil:
    return false
  count = uint32(state.pendingControlFrames.len)
  true

proc prependPendingStreamChunkForTest*(stream: HQUIC; offset: uint64;
    payload: seq[byte]; fin = false;
    targetRemote = TransportAddress(family: AddressFamily.None)): bool =
  let state = toStream(stream)
  if state.isNil:
    return false
  state.prependPendingChunk(PendingStreamChunk(
    offset: offset,
    payload: payload,
    fin: fin,
    clientContext: nil,
    targetRemote: targetRemote,
    zeroRttDispatched: false
  ))
  true

proc flushPendingDatagramsForTest*(connection: HQUIC): bool =
  let state = toConnection(connection)
  if state.isNil:
    return false
  let beforeCount = state.sentPackets.len
  flushPendingDatagrams(state)
  state.sentPackets.len > beforeCount

proc flushStreamForTest*(stream: HQUIC): bool =
  let state = toStream(stream)
  if state.isNil:
    return false
  let beforeCount =
    if state.connection.isNil: 0
    else: state.connection.sentPackets.len
  flushStream(state)
  if state.connection.isNil:
    return false
  state.connection.sentPackets.len > beforeCount

proc getStreamPendingChunkCountForTest*(stream: HQUIC; count: var uint32): bool =
  let state = toStream(stream)
  if state.isNil:
    return false
  count = uint32(state.pendingChunks.len)
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

proc getConnectionPathChallengeDataForTest*(connection: HQUIC; pathId: uint8;
    data: var array[8, uint8]): bool =
  let state = toConnection(connection)
  if state.isNil or state.quicConn.isNil:
    return false
  for path in state.quicConn.model.paths:
    if path.pathId == pathId:
      data = path.challengeData
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

proc applyPathResponseForTest*(connection: HQUIC; response: array[8, uint8]): bool =
  let state = toConnection(connection)
  if state.isNil or state.quicConn.isNil:
    return false
  state.applyPathResponse(response)
  true

proc configureConnectionPreferredAddress*(connection: HQUIC; host: cstring;
    port: uint16; cidBytesPtr: ptr uint8; cidBytesLen: uint32;
    resetTokenPtr: ptr uint8): bool {.exportc.} =
  let state = toConnection(connection)
  if state.isNil or state.quicConn.isNil or host.isNil:
    return false
  if cidBytesLen == 0'u32 or cidBytesLen > MaxCidLength.uint32 or resetTokenPtr.isNil:
    return false
  let remote = initTAddress($host, Port(port))
  let pathId = state.nextPathId()
  state.registerPathRemote(pathId, remote)
  var cid = newSeq[byte](cidBytesLen.int)
  copyMem(addr cid[0], cidBytesPtr, cid.len)
  var resetToken: array[16, uint8]
  copyMem(addr resetToken[0], resetTokenPtr, 16)
  state.quicConn.model.configurePreferredAddress(
    PreferredAddressState(
      ipv4Address: $host,
      ipv4Port: port,
      ipv6Address: "",
      ipv6Port: 0,
      hasPreferred: true,
      cid: initConnectionId(cid),
      statelessResetToken: resetToken
    ),
    pathId
  )
  true

proc triggerPreferredAddressMigration*(connection: HQUIC; pathId: var uint8): bool {.exportc.} =
  let state = toConnection(connection)
  if state.isNil or state.quicConn.isNil:
    return false
  let preferred = state.quicConn.model.migration.preferredAddress
  let preferredPathId = state.quicConn.model.migration.preferredPathId
  if not preferred.hasPreferred or not state.pathRemoteAddrs.hasKey(preferredPathId):
    return false
  state.maybeInitiatePathMigration(state.pathRemoteAddrs[preferredPathId])
  pathId = preferredPathId
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
proc currentBuiltinNewSessionTicketLifetimeSec(): uint32 {.gcsafe.} =
  if gHasOverrideBuiltinNewSessionTicketLifetimeSecForTest:
    gOverrideBuiltinNewSessionTicketLifetimeSecForTest
  else:
    600'u32

proc currentBuiltinNewSessionTicketMaxEarlyData(conn: ConnectionState): uint32 {.gcsafe.} =
  if gHasOverrideBuiltinNewSessionTicketMaxEarlyDataForTest:
    return gOverrideBuiltinNewSessionTicketMaxEarlyDataForTest
  if not conn.isNil and conn.resumptionTicket.len > 0:
    var meta: BuiltinResumptionTicketMeta
    if parseBuiltinResumptionTicket(conn.resumptionTicket, meta):
      return meta.maxEarlyData
  BuiltinDefaultMaxEarlyData
