## MsQuic 最小 API 表实现，覆盖 `MsQuicOpenVersion` 所需的核心句柄/回调。

import std/tables
import std/sequtils
import std/endians
import chronos
import chronicles

from "../core/mod" import ConnectionId, QuicConnection, QuicVersion,
    initConnectionId, newConnection, ConnectionRole, crClient
from "../congestion/common" import CongestionAlgorithm, caCubic, caBbr
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
    started: bool
    closed: bool
    sendBuffer: seq[byte]
    sentOffset: uint64
    finRequested: bool
    finSent: bool

  ListenerState = ref object of QuicHandleState
    registration: RegistrationState
    callback: QuicListenerCallback
    callbackContext: pointer
    started: bool
    stopped: bool
    transport: DatagramTransport

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

const
  DatagramReceiveNotes = ["receive=false", "receive=true"]
  DatagramSendNotes = ["send=false", "send=true"]
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

proc initConnectedEvent(alpnSource: string): QuicConnectionEvent =
  result.Type = QUIC_CONNECTION_EVENT_CONNECTED
  result.Padding = 0
  var connected: QuicConnectionEventConnectedPayload
  connected.SessionResumed = BOOLEAN(0)
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
  releaseHandle(listener)

proc listenerOnReceive(transp: DatagramTransport, remote: TransportAddress,
                         local: TransportAddress, data: seq[byte]) {.async.} =
  try:
    let packet = proto.decodePacket(data)
    if packet.isLongHeader:
      {.cast(gcsafe).}:
        emitDiagnostics(DiagnosticsEvent(
          kind: diagRegistrationOpened,
          handle: cast[HQUIC](nil), # No handle context easily avail here yet
          note: "RX Core: Long Header Pkt Type=" & $packet.longHeader.packetType & " Len=" & $data.len))
    else:
       {.cast(gcsafe).}:
         emitDiagnostics(DiagnosticsEvent(
          kind: diagRegistrationOpened,
          handle: cast[HQUIC](nil),
          note: "RX Core: Short Header Pkt Len=" & $data.len))
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
        asyncCheck listenerOnReceive(transp, remote, transp.localAddress, data)),
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
  discard connection
  discard flags
  discard dataLength
  discard data
  QUIC_STATUS_NOT_SUPPORTED

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
  let version = QuicVersion(1)
  state.quicConn = newConnection(crClient, clientCid, serverCid, version)
  discard family
  state.started = true
  state.datagramReceiveEnabled = state.settingsOverlay.datagramReceiveEnabled
  
  # Initialize Transport (Client)
  try:
    # Resolve remote address
    # For Phase 1, we assume serverName is an IP string. DNS resolution is skipped for now/handled by caller if needed
    var remoteAddr: TransportAddress
    try:
      # Try parsing as IP
      remoteAddr = initTAddress(state.serverName, Port(state.serverPort))
    except CatchableError:
       # Fallback or error - simplistic handling
       emitDiagnostics(DiagnosticsEvent(
         kind: diagConnectionStarted,
         handle: connection,
         note: "Failed to parse IP: " & state.serverName))
       return QUIC_STATUS_INVALID_PARAMETER

    state.remoteAddress = remoteAddr
    
    state.transport = newDatagramTransport(
      (proc (transp: DatagramTransport, remote: TransportAddress) {.async.} =
        # Client receive loop
        let data = transp.getMessage()
        if data.len == 0: return
        
        try:
          # Phase 4: Receiver Logic
          var header = proto.parseUnprotectedHeader(data)
          # Initial Packet? (Long Header, Type 0x0)
          if (header.firstByte and 0x80) != 0:
             let pType = (header.firstByte and 0x30) shr 4
             if pType == 0: # Initial
               # Decrypt Logic
               var mutableData = data
               let pnOffset = header.payloadOffset
               if pnOffset + 4 + 16 > mutableData.len: return
               var pnSlice: array[4, byte]
               for i in 0..3: pnSlice[i] = mutableData[pnOffset+i]
               
               tls.removeHeaderProtection(state.initialSecrets.serverHp, 
                                          mutableData[pnOffset+4 ..< pnOffset+4+16], 
                                          mutableData[0], 
                                          pnSlice)
               
               var pnVal: uint32
               bigEndian32(addr pnVal, addr pnSlice)
               
               let aadLen = pnOffset + 4
               let aad = mutableData[0 ..< aadLen]
               let ciphertext = mutableData[aadLen ..< mutableData.len - 16]
               let recTag = mutableData[^16 .. ^1]
               
               let plaintext = tls.decryptPacket(state.initialSecrets.serverKey, state.initialSecrets.serverIv,
                                                 uint64(pnVal), aad, ciphertext, recTag)
                                               
               if plaintext.len > 0:
                 {.cast(gcsafe).}:
                   emitDiagnostics(DiagnosticsEvent(kind: diagConnectionEvent, handle: cast[HQUIC](state), note: "RX Initial Valid"))
                 # Parse Frames
                 var pos = 0
                 while pos < plaintext.len:
                   # Simplistic frame parser
                   # We know CRYPTO frame is what we want.
                   # Frame type could be PING(1), ACK(2), CRYPTO(6)
                   let fType = plaintext[pos]
                   if fType == 0x06: # CRYPTO
                     pos += 1
                     let crypto = proto.parseCryptoFrame(plaintext, pos)
                     if not state.handshakeComplete:
                       let serverHelloKey = tls.findServerKeyShare(crypto.data)
                       if serverHelloKey.len == 32:
                         {.cast(gcsafe).}:
                           emitDiagnostics(DiagnosticsEvent(kind: diagConnectionEvent, handle: cast[HQUIC](state), note: "RX ServerHello KeyShare"))
                         
                         # Hash Transcript: ClientHello + ServerHello
                         # Note: simplistic assumption that crypto.data IS the ServerHello (single fragment)
                         state.transcript.add(crypto.data)
                         
                         let sharedSecret = tls.computeSharedSecret(state.clientPrivateKey, serverHelloKey)
                         let helloHash = tls.hashTranscript(state.transcript)
                         
                         # Derive Keys
                         let handshakeSecrets = tls.deriveHandshakeSecrets(sharedSecret, helloHash)
                         
                         # For 1-RTT, we need Handshake Hash (ClientHello..Finished).
                         # We are skipping verifying Finished/EncryptedExtensions for Skeleton.
                         # We pretend Handshake Hash ~ Hello Hash for deriving Application Keys immediately.
                         # This is technically WRONG (missing Finished hash), but allows us to Derive Keys.
                         let trafficSecrets = tls.deriveApplicationSecrets(handshakeSecrets.clientSecret, helloHash)
                         
                         state.oneRttKeys = trafficSecrets
                         state.handshakeComplete = true
                         {.cast(gcsafe).}:
                           emitDiagnostics(DiagnosticsEvent(kind: diagConnectionEvent, handle: cast[HQUIC](state), note: "Handshake Keys Derived. 1-RTT Enabled."))
                   elif fType == 0x02:
                     # ACK Frame
                     # pos += ... need parseAckFrame? 
                     # For skeleton, just skip rest of packet if we can't parse perfectly.
                     # But CRYPTO might be AFTER ACK.
                     # We need robust parsing.
                     # Skip ACK? ACK has variable length.
                     # We lack `parseAckFrame` that returns length.
                     # HACK: If we see ACK, we assume it's small or we break loop?
                     # Let's hope CRYPTO is first. (Usually is).
                     break 
                   else:
                     pos += 1
          else:
             # Short Header or other
             # If Short Header, we could try decrypt using oneRttKeys if available
             discard
        except Exception:
           discard
      ),
      local = initTAddress("0.0.0.0", Port(0))
    )

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
    headerBuf.writeUint32(0'u32) # PN=0
    
    # Encrypt
    var tag: array[16, byte]
    let ciphertext = tls.encryptPacket(state.initialSecrets.clientKey, state.initialSecrets.clientIv, 
                                       0'u64, headerBuf, payload, tag)
    
    var packet = headerBuf & ciphertext & @tag
    
    # Header Protection
    if packet.len >= pnOffset + 4 + 16:
      let sample = packet[pnOffset + 4 ..< pnOffset + 4 + 16]
      var pnSlice: array[4, byte]
      for i in 0..3: pnSlice[i] = packet[pnOffset+i]
      tls.applyHeaderProtection(state.initialSecrets.clientHp, sample, packet[0], pnSlice)
      for i in 0..3: packet[pnOffset+i] = pnSlice[i]
      
    asyncCheck state.transport.sendTo(state.remoteAddress, packet)
    emitDiagnostics(DiagnosticsEvent(kind: diagConnectionEvent, handle: connection, note: "TX Initial Encrypted"))

  except CatchableError as exc:
    state.transport = nil
    emitDiagnostics(DiagnosticsEvent(
       kind: diagConnectionStarted,
       handle: connection,
       note: "Transport init failed: " & exc.msg))
    return QUIC_STATUS_INTERNAL_ERROR

  if not state.callback.isNil:
    let negotiatedAlpn =
      if state.configuration.alpns.len == 0: "" else: state.configuration.alpns[0]
    var event = initConnectedEvent(negotiatedAlpn)
    discard state.callback(connection, state.callbackContext, addr event)
  let negotiatedAlpn =
    if state.configuration.alpns.len == 0: "" else: state.configuration.alpns[0]
  var nimEvent = ConnectionEvent(
    kind: ceConnected,
    sessionResumed: false,
    negotiatedAlpn: negotiatedAlpn)
  emitConnectionEvent(state, nimEvent)
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
  state.started = false
  state.closed = false
  state.connection = connState # Set parent connection
  stream[] = storeHandle(state)
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
      Id: QUIC_UINT62(0),
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
  discard flags
  discard errorCode
  streamEmitEvent(state, QUIC_STREAM_EVENT_SEND_SHUTDOWN_COMPLETE)
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
  QUIC_STATUS_SUCCESS

proc flushStream(state: StreamState) =
  if state.connection.isNil: return
  let conn = state.connection
  
  if state.sendBuffer.len == 0 and not (state.finRequested and not state.finSent):
    return

  # Construct STREAM Frame
  var payload = state.sendBuffer # Move/Copy
  state.sendBuffer.setLen(0)
  
  # Stream ID? We don't have a real ID yet. Mock ID=0 if client initiated bidirectional.
  # TODO: Real ID management.
  let streamId = 0'u64 
  let fin = state.finRequested
  let streamFrame = proto.encodeStreamFrame(streamId, payload, state.sentOffset, fin)
  
  state.sentOffset += uint64(payload.len)
  if fin: state.finSent = true

  # Construct Short Header Packet (1-RTT)
  if not conn.handshakeComplete:
    # Buffer until handshake complete
    # Restore buffer? 
    # Logic: we consumed buffer into `payload`. We should push it back if we can't send.
    # OR: just check check complete at top.
    # BUT: we already extracted data.
    # Better: check at top.
    # Reverting extraction:
    state.sendBuffer = payload & state.sendBuffer # Prepend (inefficient but safe)
    return

  let pn = 1'u32 # TODO: Real PN
  let destCid = @(conn.peerCid.bytes)[0 ..< int(conn.peerCid.length)] 
  
  # Payload
  var pktPayload = streamFrame
  
  # Headers
  var header: seq[byte] = @[]
  header.add(0x43'u8) # Short Header
  header.add(destCid)
  let pnOffset = header.len
  proto.writeUint32(header, pn)
  
  # Encrypt
  var tag: array[16, byte]
  let ciphertext = tls.encryptPacket(conn.oneRttKeys.clientKey, conn.oneRttKeys.clientIv, 
                                     uint64(pn), header, pktPayload, tag)
                                     
  var packet = header & ciphertext & @tag
  
  # Header Protection
  if packet.len >= pnOffset + 4 + 16:
      let sample = packet[pnOffset + 4 ..< pnOffset + 4 + 16]
      var pnSlice: array[4, byte]
      for i in 0..3: pnSlice[i] = packet[pnOffset+i]
      tls.applyHeaderProtection(conn.oneRttKeys.clientHp, sample, packet[0], pnSlice)
      for i in 0..3: packet[pnOffset+i] = pnSlice[i]

  asyncCheck conn.transport.sendTo(conn.remoteAddress, packet)
  {.cast(gcsafe).}:
    emitDiagnostics(DiagnosticsEvent(
       kind: diagConnectionEvent,
       handle: cast[HQUIC](conn),
       note: "TX Stream 1-RTT Encrypted len=" & $packet.len))

proc msquicStreamSend(stream: HQUIC; buffers: ptr QuicBuffer;
    bufferCount: uint32; flags: QUIC_SEND_FLAGS;
    clientContext: pointer): QUIC_STATUS {.cdecl.} =
  let state = toStream(stream)
  if state.isNil:
    return QUIC_STATUS_INVALID_PARAMETER
  
  # 1. Buffer Data
  if bufferCount > 0 and not buffers.isNil:
    let bufArray = cast[ptr UncheckedArray[QuicBuffer]](buffers)
    for i in 0 ..< bufferCount:
      let qb = bufArray[i]
      if qb.Length > 0 and not qb.Buffer.isNil:
        let src = qb.Buffer
        let len = int(qb.Length)
        let paramBuf = newSeq[byte](len)
        copyMem(unsafeAddr paramBuf[0], src, len)
        state.sendBuffer.add(paramBuf)
  
  # 2. Check FIN
  if (flags and 1) != 0: # QUIC_SEND_FLAG_FIN = 1 (usually)
     state.finRequested = true

  # 3. Flush
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
  discard buffers
  discard bufferCount
  discard flags
  discard clientContext
  var nimEvent = ConnectionEvent(
    kind: ceParameterUpdated,
    paramId: QUIC_PARAM_CONN_DATAGRAM_SEND_ENABLED,
    note: "datagram send enqueued",
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
    gGlobalExecutionConfig = GlobalExecutionConfigState()

proc getGlobalExecutionConfigState*(): GlobalExecutionConfigState =
  gGlobalExecutionConfig
