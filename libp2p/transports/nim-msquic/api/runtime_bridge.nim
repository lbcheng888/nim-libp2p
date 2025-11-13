## MsQuic 运行时桥接器：把 Nim 端事件处理器接线到真实 MsQuic 回调。

import std/[tables, strformat]
import results

from ./ffi_loader import MsQuicRuntime, MsQuicLoadOptions, loadMsQuic,
    unloadMsQuic, asQuicApiTable
from ./api_impl import QuicApiTable, QUIC_STATUS, QUIC_STATUS_SUCCESS,
    QUIC_STATUS_INVALID_PARAMETER, QUIC_STATUS_INVALID_STATE,
    QUIC_STATUS_NOT_SUPPORTED, BOOLEAN, HQUIC, QuicConnectionCallback,
    QuicStreamCallback, QuicRegistrationConfigC, QuicBuffer, QUIC_STREAM_OPEN_FLAGS,
    QUIC_STREAM_START_FLAGS, QUIC_STREAM_SHUTDOWN_FLAGS, QUIC_SEND_FLAGS,
    QUIC_CONNECTION_SHUTDOWN_FLAGS, QUIC_UINT62, QuicConnectionEvent,
    QuicConnectionEventConnectedPayload, QUIC_CONNECTION_EVENT_CONNECTED
from ./param_catalog import QUIC_PARAM_CONN_DATAGRAM_SEND_ENABLED,
    QUIC_PARAM_CONN_DATAGRAM_RECEIVE_ENABLED, QUIC_PARAM_STREAM_ID
import ./event_model

const
  QUIC_CONNECTION_EVENT_SHUTDOWN_INITIATED_BY_TRANSPORT = 1'u32
  QUIC_CONNECTION_EVENT_SHUTDOWN_INITIATED_BY_PEER = 2'u32
  QUIC_CONNECTION_EVENT_SHUTDOWN_COMPLETE = 3'u32
  QUIC_CONNECTION_EVENT_PEER_STREAM_STARTED = 6'u32
  QUIC_CONNECTION_EVENT_DATAGRAM_STATE_CHANGED = 10'u32
  QUIC_CONNECTION_EVENT_DATAGRAM_RECEIVED = 11'u32
  QUIC_CONNECTION_EVENT_DATAGRAM_SEND_STATE_CHANGED = 12'u32

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

type
  QuicConnectionEventShutdownTransportPayload {.bycopy.} = object
    Status*: QUIC_STATUS
    ErrorCode*: QUIC_UINT62

  QuicConnectionEventShutdownPeerPayload {.bycopy.} = object
    ErrorCode*: QUIC_UINT62

  QuicConnectionEventDatagramStatePayload {.bycopy.} = object
    SendEnabled*: BOOLEAN
    MaxSendLength*: uint16
    Reserved*: uint16

  QuicConnectionEventDatagramSendStatePayload {.bycopy.} = object
    ClientContext*: pointer
    State*: uint32
    Reserved*: uint32

  QuicConnectionEventDatagramReceivedPayload {.bycopy.} = object
    Buffer*: ptr QuicBuffer
    Flags*: uint32

  QuicConnectionEventPeerStreamStartedPayload {.bycopy.} = object
    Stream*: HQUIC
    Flags*: QUIC_STREAM_OPEN_FLAGS

  QuicStreamEvent* {.bycopy.} = object
    Type*: uint32
    Padding*: uint32
    Data*: array[96, uint8]

  QuicStreamEventStartCompletePayload {.bycopy.} = object
    Status*: QUIC_STATUS
    Id*: QUIC_UINT62
    Flags*: uint8
    Reserved*: array[7, uint8]

  QuicStreamEventReceivePayload {.bycopy.} = object
    AbsoluteOffset*: uint64
    TotalBufferLength*: uint64
    Buffers*: pointer
    BufferCount*: uint32
    Flags*: uint32

  QuicStreamEventSendCompletePayload {.bycopy.} = object
    Canceled*: BOOLEAN
    Reserved*: array[7, uint8]
    ClientContext*: pointer

  QuicStreamEventPeerAbortedPayload {.bycopy.} = object
    ErrorCode*: QUIC_UINT62
    Reserved*: uint16

  QuicStreamEventSendShutdownCompletePayload {.bycopy.} = object
    Graceful*: BOOLEAN
    Reserved*: array[7, uint8]

  QuicStreamEventShutdownCompletePayload {.bycopy.} = object
    ConnectionShutdown*: BOOLEAN
    Flags*: uint8
    Reserved*: array[6, uint8]
    ConnectionErrorCode*: QUIC_UINT62
    ConnectionCloseStatus*: QUIC_STATUS

  QuicStreamEventIdealSendBufferPayload {.bycopy.} = object
    ByteCount*: uint64

  QuicStreamEventCancelOnLossPayload {.bycopy.} = object
    ErrorCode*: QUIC_UINT62
    Reserved*: uint16

  QuicStreamEventReceiveBufferNeededPayload {.bycopy.} = object
    BufferLengthNeeded*: uint64

  QuicListenerEvent* {.bycopy.} = object
    Type*: uint32
    Padding*: uint32
    Data*: array[32, uint8]

  QuicListenerEventNewConnectionPayload {.bycopy.} = object
    Info*: pointer
    Connection*: HQUIC

  QuicListenerEventStopCompletePayload {.bycopy.} = object
    Flags*: uint8
    Reserved*: array[7, uint8]

  QuicListenerEventDosModePayload {.bycopy.} = object
    Flags*: uint8
    Reserved*: array[7, uint8]

  RuntimeBridge* = ref RuntimeBridgeObj

  ConnectionCallbackContext = ref object
    bridge: RuntimeBridge
    handler: ConnectionEventHandler
    userContext: pointer

  StreamCallbackContext = ref object
    bridge: RuntimeBridge
    handler: StreamEventHandler
    userContext: pointer

  ListenerCallbackContext = ref object
    bridge: RuntimeBridge
    handler: ListenerEventHandler
    userContext: pointer

  RuntimeBridgeObj = object
    runtime: MsQuicRuntime
    api: ptr QuicApiTable
    ownsRuntime: bool
    connectionContexts: Table[HQUIC, ConnectionCallbackContext]
    streamContexts: Table[HQUIC, StreamCallbackContext]
    listenerContexts: Table[HQUIC, ListenerCallbackContext]

proc boolFromFlag(value: uint8; mask: uint8): bool {.inline.} =
  (value and mask) != 0

proc copyAlpn(payload: ptr QuicConnectionEventConnectedPayload): string =
  if payload.isNil or payload.NegotiatedAlpnLength == 0'u8 or
      payload.NegotiatedAlpn.isNil:
    return ""
  let length = int(payload.NegotiatedAlpnLength)
  result = newString(length)
  copyMem(addr result[0], payload.NegotiatedAlpn, length)

proc convertConnectionEvent(connection: HQUIC;
    eventPtr: ptr QuicConnectionEvent; userContext: pointer): ConnectionEvent =
  result = ConnectionEvent(
    connection: connection,
    kind: ceUnknown,
    userContext: userContext
  )
  if eventPtr.isNil:
    return
  case eventPtr.Type
  of QUIC_CONNECTION_EVENT_CONNECTED:
    let payload = cast[ptr QuicConnectionEventConnectedPayload](addr eventPtr.Data[0])
    result.kind = ceConnected
    if not payload.isNil:
      result.sessionResumed = payload.SessionResumed != 0
      result.negotiatedAlpn = copyAlpn(payload)
  of QUIC_CONNECTION_EVENT_SHUTDOWN_INITIATED_BY_TRANSPORT:
    let payload = cast[ptr QuicConnectionEventShutdownTransportPayload](addr eventPtr.Data[0])
    result.kind = ceShutdownInitiated
    result.note = "transport"
    if not payload.isNil:
      result.status = payload.Status
      result.errorCode = payload.ErrorCode
  of QUIC_CONNECTION_EVENT_SHUTDOWN_INITIATED_BY_PEER:
    let payload = cast[ptr QuicConnectionEventShutdownPeerPayload](addr eventPtr.Data[0])
    result.kind = ceShutdownInitiated
    result.note = "peer"
    if not payload.isNil:
      result.errorCode = payload.ErrorCode
  of QUIC_CONNECTION_EVENT_SHUTDOWN_COMPLETE:
    result.kind = ceShutdownComplete
    let flags = cast[ptr uint8](addr eventPtr.Data[0])[]
    result.handshakeCompleted = boolFromFlag(flags, 0x01)
    result.peerAcknowledgedShutdown = boolFromFlag(flags, 0x02)
    result.appCloseInProgress = boolFromFlag(flags, 0x04)
  of QUIC_CONNECTION_EVENT_DATAGRAM_STATE_CHANGED:
    let payload = cast[ptr QuicConnectionEventDatagramStatePayload](addr eventPtr.Data[0])
    result.kind = ceDatagramStateChanged
    result.paramId = QUIC_PARAM_CONN_DATAGRAM_SEND_ENABLED
    if not payload.isNil:
      result.boolValue = payload.SendEnabled != 0
      result.maxSendLength = payload.MaxSendLength
      result.note = fmt"datagram send enabled={result.boolValue} max={result.maxSendLength}"
  of QUIC_CONNECTION_EVENT_DATAGRAM_RECEIVED:
    let payload = cast[ptr QuicConnectionEventDatagramReceivedPayload](addr eventPtr.Data[0])
    result.kind = ceDatagramReceived
    result.paramId = QUIC_PARAM_CONN_DATAGRAM_RECEIVE_ENABLED
    if not payload.isNil and not payload.Buffer.isNil:
      let buffer = payload.Buffer[]
      if buffer.Length > 0'u32 and not buffer.Buffer.isNil:
        let length = int(buffer.Length)
        if length > 0:
          result.datagramPayload = newSeq[byte](length)
          copyMem(addr result.datagramPayload[0], buffer.Buffer, length)
      result.datagramFlags = payload.Flags
    else:
      result.datagramPayload = @[]
      result.datagramFlags = 0
    result.note = "datagram received"
  of QUIC_CONNECTION_EVENT_DATAGRAM_SEND_STATE_CHANGED:
    let payload = cast[ptr QuicConnectionEventDatagramSendStatePayload](addr eventPtr.Data[0])
    result.kind = ceDatagramStateChanged
    if not payload.isNil:
      result.userContext = payload.ClientContext
      result.uintValue = payload.State.uint64
      result.note = fmt"datagram send state={payload.State}"
  of QUIC_CONNECTION_EVENT_PEER_STREAM_STARTED:
    let payload = cast[ptr QuicConnectionEventPeerStreamStartedPayload](addr eventPtr.Data[0])
    result.kind = cePeerStreamStarted
    if not payload.isNil:
      result.stream = cast[pointer](payload.Stream)
      result.streamFlags = uint32(payload.Flags)
      result.streamIsUnidirectional =
        (uint32(payload.Flags) and 0x0001'u32) != 0'u32
    else:
      result.note = "peer stream started without payload"
  else:
    result.note = fmt"unsupported connection event type {eventPtr.Type}"

proc convertStreamEvent(stream: HQUIC; eventPtr: ptr QuicStreamEvent;
    userContext: pointer): StreamEvent =
  result = StreamEvent(
    stream: stream,
    kind: seUnknown,
    userContext: userContext
  )
  if eventPtr.isNil:
    return
  case eventPtr.Type
  of QUIC_STREAM_EVENT_START_COMPLETE:
    let payload = cast[ptr QuicStreamEventStartCompletePayload](addr eventPtr.Data[0])
    result.kind = seStartComplete
    if not payload.isNil:
      result.status = payload.Status
      result.id = payload.Id
      result.peerAccepted = boolFromFlag(payload.Flags, 0x01)
  of QUIC_STREAM_EVENT_RECEIVE:
    let payload = cast[ptr QuicStreamEventReceivePayload](addr eventPtr.Data[0])
    result.kind = seReceive
    if not payload.isNil:
      result.absoluteOffset = payload.AbsoluteOffset
      result.totalBufferLength = payload.TotalBufferLength
      result.bufferCount = payload.BufferCount
      result.flags = payload.Flags
      if payload.TotalBufferLength > 0'u64 and payload.BufferCount > 0'u32 and
          not payload.Buffers.isNil:
        let buffers = cast[ptr UncheckedArray[QuicBuffer]](payload.Buffers)
        var remaining = int(payload.TotalBufferLength)
        if remaining < 0:
          remaining = 0
        result.payload = newSeq[byte](0)
        for idx in 0 ..< int(payload.BufferCount):
          let buf = buffers[idx]
          if buf.Length == 0 or buf.Buffer.isNil:
            continue
          let chunkLen = min(remaining, int(buf.Length))
          if chunkLen <= 0:
            break
          let start = result.payload.len
          result.payload.setLen(start + chunkLen)
          copyMem(addr result.payload[start], buf.Buffer, chunkLen)
          remaining = max(0, remaining - chunkLen)
        if remaining > 0 and result.payload.len < int(payload.TotalBufferLength):
          result.payload.setLen(int(payload.TotalBufferLength))
  of QUIC_STREAM_EVENT_SEND_COMPLETE:
    let payload = cast[ptr QuicStreamEventSendCompletePayload](addr eventPtr.Data[0])
    result.kind = seSendComplete
    if not payload.isNil:
      result.cancelled = payload.Canceled != 0
      result.clientContext = payload.ClientContext
  of QUIC_STREAM_EVENT_PEER_SEND_SHUTDOWN:
    result.kind = sePeerSendShutdown
  of QUIC_STREAM_EVENT_PEER_SEND_ABORTED:
    let payload = cast[ptr QuicStreamEventPeerAbortedPayload](addr eventPtr.Data[0])
    result.kind = sePeerSendAborted
    if not payload.isNil:
      result.errorCode = payload.ErrorCode
  of QUIC_STREAM_EVENT_PEER_RECEIVE_ABORTED:
    let payload = cast[ptr QuicStreamEventPeerAbortedPayload](addr eventPtr.Data[0])
    result.kind = sePeerReceiveAborted
    if not payload.isNil:
      result.errorCode = payload.ErrorCode
  of QUIC_STREAM_EVENT_SEND_SHUTDOWN_COMPLETE:
    let payload = cast[ptr QuicStreamEventSendShutdownCompletePayload](addr eventPtr.Data[0])
    result.kind = seSendShutdownComplete
    if not payload.isNil:
      result.graceful = payload.Graceful != 0
  of QUIC_STREAM_EVENT_SHUTDOWN_COMPLETE:
    let payload = cast[ptr QuicStreamEventShutdownCompletePayload](addr eventPtr.Data[0])
    result.kind = seShutdownComplete
    if not payload.isNil:
      result.connectionShutdown = payload.ConnectionShutdown != 0
      result.appCloseInProgress = boolFromFlag(payload.Flags, 0x01)
      result.connectionShutdownByApp = boolFromFlag(payload.Flags, 0x02)
      result.connectionClosedRemotely = boolFromFlag(payload.Flags, 0x04)
      result.connectionErrorCode = payload.ConnectionErrorCode
      result.connectionCloseStatus = payload.ConnectionCloseStatus
  of QUIC_STREAM_EVENT_IDEAL_SEND_BUFFER_SIZE:
    let payload = cast[ptr QuicStreamEventIdealSendBufferPayload](addr eventPtr.Data[0])
    result.kind = seIdealSendBufferSize
    if not payload.isNil:
      result.byteCount = payload.ByteCount
  of QUIC_STREAM_EVENT_PEER_ACCEPTED:
    result.kind = sePeerAccepted
  of QUIC_STREAM_EVENT_CANCEL_ON_LOSS:
    let payload = cast[ptr QuicStreamEventCancelOnLossPayload](addr eventPtr.Data[0])
    result.kind = seCancelOnLoss
    if not payload.isNil:
      result.errorCode = payload.ErrorCode
  of QUIC_STREAM_EVENT_RECEIVE_BUFFER_NEEDED:
    let payload = cast[ptr QuicStreamEventReceiveBufferNeededPayload](addr eventPtr.Data[0])
    result.kind = seReceiveBufferNeeded
    if not payload.isNil:
      result.bufferLengthNeeded = payload.BufferLengthNeeded
  else:
    result.note = fmt"unsupported stream event type {eventPtr.Type}"

proc convertListenerEvent(listener: HQUIC; eventPtr: ptr QuicListenerEvent;
    userContext: pointer): ListenerEvent =
  result = ListenerEvent(
    listener: listener,
    kind: leUnknown,
    userContext: userContext
  )
  if eventPtr.isNil:
    return
  case eventPtr.Type
  of QUIC_LISTENER_EVENT_NEW_CONNECTION:
    let payload = cast[ptr QuicListenerEventNewConnectionPayload](addr eventPtr.Data[0])
    result.kind = leNewConnection
    if not payload.isNil:
      result.info = payload.Info
      result.connection = payload.Connection
  of QUIC_LISTENER_EVENT_STOP_COMPLETE:
    let payload = cast[ptr QuicListenerEventStopCompletePayload](addr eventPtr.Data[0])
    result.kind = leStopComplete
    if not payload.isNil:
      result.appCloseInProgress = boolFromFlag(payload.Flags, 0x01)
  of QUIC_LISTENER_EVENT_DOS_MODE_CHANGED:
    let payload = cast[ptr QuicListenerEventDosModePayload](addr eventPtr.Data[0])
    result.kind = leDosModeChanged
    if not payload.isNil:
      result.dosModeEnabled = boolFromFlag(payload.Flags, 0x01)
  else:
    result.note = fmt"unsupported listener event type {eventPtr.Type}"

proc initRuntimeBridge*(runtime: MsQuicRuntime;
    ownsRuntime: bool = false): RuntimeBridge =
  if runtime.isNil:
    raise newException(ValueError, "MsQuic 运行时未初始化")
  let apiTable = asQuicApiTable[QuicApiTable](runtime)
  if apiTable.isNil:
    raise newException(ValueError, "无法获取 QUIC_API_TABLE")
  if apiTable.SetCallbackHandler.isNil:
    raise newException(ValueError, "MsQuic API 缺少 SetCallbackHandler")
  if apiTable.ConnectionOpen.isNil:
    raise newException(ValueError, "MsQuic API 缺少 ConnectionOpen")
  RuntimeBridge(runtime: runtime,
    api: apiTable,
    ownsRuntime: ownsRuntime,
    connectionContexts: initTable[HQUIC, ConnectionCallbackContext](),
    streamContexts: initTable[HQUIC, StreamCallbackContext](),
    listenerContexts: initTable[HQUIC, ListenerCallbackContext]())

proc loadRuntimeBridge*(options: MsQuicLoadOptions = MsQuicLoadOptions()): RuntimeBridge =
  let loadRes = loadMsQuic(options)
  if not loadRes.success:
    raise newException(OSError, loadRes.error)
  initRuntimeBridge(loadRes.runtime, ownsRuntime = true)

proc getApiTable*(bridge: RuntimeBridge): ptr QuicApiTable {.inline.} =
  if bridge.isNil:
    return nil
  bridge.api

proc getRuntime*(bridge: RuntimeBridge): MsQuicRuntime {.inline.} =
  if bridge.isNil:
    return nil
  bridge.runtime

proc storeConnectionContext(bridge: RuntimeBridge; connection: HQUIC;
    ctx: ConnectionCallbackContext) =
  if bridge.isNil or connection.isNil:
    return
  bridge.connectionContexts[connection] = ctx

proc dropConnectionContext(bridge: RuntimeBridge; connection: HQUIC) =
  if bridge.isNil or connection.isNil:
    return
  if bridge.connectionContexts.contains(connection):
    bridge.connectionContexts.del(connection)

proc storeStreamContext(bridge: RuntimeBridge; stream: HQUIC;
    ctx: StreamCallbackContext) =
  if bridge.isNil or stream.isNil:
    return
  bridge.streamContexts[stream] = ctx

proc dropStreamContext(bridge: RuntimeBridge; stream: HQUIC) =
  if bridge.isNil or stream.isNil:
    return
  if bridge.streamContexts.contains(stream):
    bridge.streamContexts.del(stream)

proc storeListenerContext(bridge: RuntimeBridge; listener: HQUIC;
    ctx: ListenerCallbackContext) =
  if bridge.isNil or listener.isNil:
    return
  bridge.listenerContexts[listener] = ctx

proc dropListenerContext(bridge: RuntimeBridge; listener: HQUIC) =
  if bridge.isNil or listener.isNil:
    return
  if bridge.listenerContexts.contains(listener):
    bridge.listenerContexts.del(listener)

proc connectionCallbackShim(connection: HQUIC; context: pointer;
    event: pointer): QUIC_STATUS {.cdecl.} =
  var ctx: ConnectionCallbackContext = nil
  if not context.isNil:
    ctx = cast[ConnectionCallbackContext](context)
  if ctx.isNil or ctx.bridge.isNil:
    return QUIC_STATUS_SUCCESS
  let bridge = ctx.bridge
  let eventPtr = cast[ptr QuicConnectionEvent](event)
  let nimEvent = convertConnectionEvent(connection, eventPtr, ctx.userContext)
  if not ctx.handler.isNil:
    ctx.handler(nimEvent)
  if nimEvent.kind == ceShutdownComplete:
    bridge.dropConnectionContext(connection)
  QUIC_STATUS_SUCCESS

proc streamCallbackShim(stream: HQUIC; context: pointer;
    event: pointer): QUIC_STATUS {.cdecl.} =
  var ctx: StreamCallbackContext = nil
  if not context.isNil:
    ctx = cast[StreamCallbackContext](context)
  if ctx.isNil or ctx.bridge.isNil:
    return QUIC_STATUS_SUCCESS
  let bridge = ctx.bridge
  let eventPtr = cast[ptr QuicStreamEvent](event)
  let nimEvent = convertStreamEvent(stream, eventPtr, ctx.userContext)
  if not ctx.handler.isNil:
    ctx.handler(nimEvent)
  case nimEvent.kind
  of seShutdownComplete, seSendShutdownComplete:
    bridge.dropStreamContext(stream)
  else:
    discard
  QUIC_STATUS_SUCCESS

proc listenerCallbackShim(listener: HQUIC; context: pointer;
    event: pointer): QUIC_STATUS {.cdecl.} =
  var ctx: ListenerCallbackContext = nil
  if not context.isNil:
    ctx = cast[ListenerCallbackContext](context)
  if ctx.isNil or ctx.bridge.isNil:
    return QUIC_STATUS_SUCCESS
  let bridge = ctx.bridge
  let eventPtr = cast[ptr QuicListenerEvent](event)
  let nimEvent = convertListenerEvent(listener, eventPtr, ctx.userContext)
  if not ctx.handler.isNil:
    ctx.handler(nimEvent)
  if nimEvent.kind == leStopComplete:
    bridge.dropListenerContext(listener)
  QUIC_STATUS_SUCCESS

proc openRegistration*(bridge: RuntimeBridge; config: ptr QuicRegistrationConfigC;
    registration: var HQUIC): QUIC_STATUS =
  registration = nil
  if bridge.isNil or bridge.api.isNil:
    return QUIC_STATUS_INVALID_STATE
  if bridge.api.RegistrationOpen.isNil:
    return QUIC_STATUS_NOT_SUPPORTED
  bridge.api.RegistrationOpen(config, addr registration)

proc closeRegistration*(bridge: RuntimeBridge; registration: HQUIC) =
  if bridge.isNil or bridge.api.isNil or bridge.api.RegistrationClose.isNil:
    return
  if not registration.isNil:
    bridge.api.RegistrationClose(registration)

proc openConfiguration*(bridge: RuntimeBridge; registration: HQUIC;
    alpnBuffers: ptr QuicBuffer; alpnCount: uint32; settings: pointer;
    settingsSize: uint32; context: pointer; configuration: var HQUIC): QUIC_STATUS =
  configuration = nil
  if bridge.isNil or bridge.api.isNil:
    return QUIC_STATUS_INVALID_STATE
  if bridge.api.ConfigurationOpen.isNil:
    return QUIC_STATUS_NOT_SUPPORTED
  bridge.api.ConfigurationOpen(registration, alpnBuffers, alpnCount,
    settings, settingsSize, context, addr configuration)

proc closeConfiguration*(bridge: RuntimeBridge; configuration: HQUIC) =
  if bridge.isNil or bridge.api.isNil or bridge.api.ConfigurationClose.isNil:
    return
  if not configuration.isNil:
    bridge.api.ConfigurationClose(configuration)

proc openConnection*(bridge: RuntimeBridge; registration: HQUIC;
    handler: ConnectionEventHandler; connection: var HQUIC;
    userContext: pointer = nil): QUIC_STATUS =
  connection = nil
  if bridge.isNil or bridge.api.isNil:
    return QUIC_STATUS_INVALID_STATE
  if handler.isNil:
    return QUIC_STATUS_INVALID_PARAMETER
  if bridge.api.ConnectionOpen.isNil:
    return QUIC_STATUS_NOT_SUPPORTED
  let ctx = ConnectionCallbackContext(
    bridge: bridge,
    handler: handler,
    userContext: userContext
  )
  var tmp: HQUIC = nil
  let status = bridge.api.ConnectionOpen(
    registration,
    connectionCallbackShim,
    cast[pointer](ctx),
    addr tmp)
  if status != QUIC_STATUS_SUCCESS:
    return status
  if tmp.isNil:
    return QUIC_STATUS_INVALID_STATE
  bridge.storeConnectionContext(tmp, ctx)
  bridge.api.SetCallbackHandler(tmp, cast[pointer](connectionCallbackShim), cast[pointer](ctx))
  connection = tmp
  QUIC_STATUS_SUCCESS

proc adoptConnection*(bridge: RuntimeBridge; connection: HQUIC;
    handler: ConnectionEventHandler; userContext: pointer = nil): QUIC_STATUS =
  if bridge.isNil or bridge.api.isNil:
    return QUIC_STATUS_INVALID_STATE
  if connection.isNil or handler.isNil:
    return QUIC_STATUS_INVALID_PARAMETER
  let ctx = ConnectionCallbackContext(
    bridge: bridge,
    handler: handler,
    userContext: userContext
  )
  bridge.storeConnectionContext(connection, ctx)
  bridge.api.SetCallbackHandler(connection, cast[pointer](connectionCallbackShim), cast[pointer](ctx))
  QUIC_STATUS_SUCCESS

proc shutdownConnection*(bridge: RuntimeBridge; connection: HQUIC;
    flags: QUIC_CONNECTION_SHUTDOWN_FLAGS; errorCode: QUIC_UINT62 = 0): QUIC_STATUS =
  if bridge.isNil or bridge.api.isNil:
    return QUIC_STATUS_INVALID_STATE
  if connection.isNil:
    return QUIC_STATUS_INVALID_PARAMETER
  if bridge.api.ConnectionShutdown.isNil:
    return QUIC_STATUS_NOT_SUPPORTED
  bridge.api.ConnectionShutdown(connection, flags, errorCode)

proc closeConnection*(bridge: RuntimeBridge; connection: HQUIC) =
  if bridge.isNil or connection.isNil:
    return
  bridge.dropConnectionContext(connection)
  if bridge.api.isNil or bridge.api.ConnectionClose.isNil:
    return
  bridge.api.ConnectionClose(connection)

proc openStream*(bridge: RuntimeBridge; connection: HQUIC;
    flags: QUIC_STREAM_OPEN_FLAGS; handler: StreamEventHandler;
    stream: var HQUIC; userContext: pointer = nil): QUIC_STATUS =
  stream = nil
  if bridge.isNil or bridge.api.isNil:
    return QUIC_STATUS_INVALID_STATE
  if handler.isNil:
    return QUIC_STATUS_INVALID_PARAMETER
  if bridge.api.StreamOpen.isNil:
    return QUIC_STATUS_NOT_SUPPORTED
  let ctx = StreamCallbackContext(
    bridge: bridge,
    handler: handler,
    userContext: userContext
  )
  var tmp: HQUIC = nil
  let status = bridge.api.StreamOpen(
    connection,
    flags,
    streamCallbackShim,
    cast[pointer](ctx),
    addr tmp)
  if status != QUIC_STATUS_SUCCESS:
    return status
  if tmp.isNil:
    return QUIC_STATUS_INVALID_STATE
  bridge.storeStreamContext(tmp, ctx)
  bridge.api.SetCallbackHandler(tmp, cast[pointer](streamCallbackShim), cast[pointer](ctx))
  stream = tmp
  QUIC_STATUS_SUCCESS

proc adoptStream*(bridge: RuntimeBridge; stream: HQUIC;
    handler: StreamEventHandler; userContext: pointer = nil): QUIC_STATUS =
  if bridge.isNil or bridge.api.isNil:
    return QUIC_STATUS_INVALID_STATE
  if stream.isNil or handler.isNil:
    return QUIC_STATUS_INVALID_PARAMETER
  let ctx = StreamCallbackContext(
    bridge: bridge,
    handler: handler,
    userContext: userContext
  )
  bridge.storeStreamContext(stream, ctx)
  bridge.api.SetCallbackHandler(stream, cast[pointer](streamCallbackShim), cast[pointer](ctx))
  QUIC_STATUS_SUCCESS

proc closeStream*(bridge: RuntimeBridge; stream: HQUIC) =
  if bridge.isNil or stream.isNil:
    return
  bridge.dropStreamContext(stream)
  if bridge.api.isNil or bridge.api.StreamClose.isNil:
    return
  bridge.api.StreamClose(stream)

proc streamId*(bridge: RuntimeBridge; stream: HQUIC): Result[uint64, string] =
  if bridge.isNil or bridge.api.isNil or bridge.api.GetParam.isNil:
    return err("MsQuic runtime bridge unavailable")
  if stream.isNil:
    return err("MsQuic stream handle unavailable")
  var length = uint32(sizeof(uint64))
  var value: uint64
  let status =
    try:
      bridge.api.GetParam(stream, QUIC_PARAM_STREAM_ID, addr length, addr value)
    except Exception as exc:
      return err("MsQuic StreamGetParam raised: " & exc.msg)
  if status != QUIC_STATUS_SUCCESS:
    return err(fmt"MsQuic StreamGetParam failed: 0x{status:08x}")
  ok(value)

proc startStream*(bridge: RuntimeBridge; stream: HQUIC;
    flags: QUIC_STREAM_START_FLAGS): QUIC_STATUS =
  if bridge.isNil or bridge.api.isNil:
    return QUIC_STATUS_INVALID_STATE
  if stream.isNil:
    return QUIC_STATUS_INVALID_PARAMETER
  if bridge.api.StreamStart.isNil:
    return QUIC_STATUS_NOT_SUPPORTED
  bridge.api.StreamStart(stream, flags)

proc shutdownStream*(bridge: RuntimeBridge; stream: HQUIC;
    flags: QUIC_STREAM_SHUTDOWN_FLAGS; errorCode: QUIC_UINT62 = 0): QUIC_STATUS =
  if bridge.isNil or bridge.api.isNil:
    return QUIC_STATUS_INVALID_STATE
  if stream.isNil:
    return QUIC_STATUS_INVALID_PARAMETER
  if bridge.api.StreamShutdown.isNil:
    return QUIC_STATUS_NOT_SUPPORTED
  bridge.api.StreamShutdown(stream, flags, errorCode)

proc openListener*(bridge: RuntimeBridge; registration: HQUIC;
    handler: ListenerEventHandler; listener: var HQUIC;
    userContext: pointer = nil): QUIC_STATUS =
  listener = nil
  if bridge.isNil or bridge.api.isNil:
    return QUIC_STATUS_INVALID_STATE
  if handler.isNil:
    return QUIC_STATUS_INVALID_PARAMETER
  if bridge.api.ListenerOpen.isNil:
    return QUIC_STATUS_NOT_SUPPORTED
  let ctx = ListenerCallbackContext(
    bridge: bridge,
    handler: handler,
    userContext: userContext
  )
  var tmp: HQUIC = nil
  let status = bridge.api.ListenerOpen(
    registration,
    listenerCallbackShim,
    cast[pointer](ctx),
    addr tmp)
  if status != QUIC_STATUS_SUCCESS:
    return status
  if tmp.isNil:
    return QUIC_STATUS_INVALID_STATE
  bridge.storeListenerContext(tmp, ctx)
  bridge.api.SetCallbackHandler(tmp, cast[pointer](listenerCallbackShim), cast[pointer](ctx))
  listener = tmp
  QUIC_STATUS_SUCCESS

proc startListener*(bridge: RuntimeBridge; listener: HQUIC;
    alpns: ptr QuicBuffer = nil; alpnCount: uint32 = 0;
    address: pointer = nil): QUIC_STATUS =
  if bridge.isNil or bridge.api.isNil:
    return QUIC_STATUS_INVALID_STATE
  if listener.isNil:
    return QUIC_STATUS_INVALID_PARAMETER
  if bridge.api.ListenerStart.isNil:
    return QUIC_STATUS_NOT_SUPPORTED
  bridge.api.ListenerStart(listener, alpns, alpnCount, address)

proc stopListener*(bridge: RuntimeBridge; listener: HQUIC): QUIC_STATUS =
  if bridge.isNil or bridge.api.isNil:
    return QUIC_STATUS_INVALID_STATE
  if listener.isNil:
    return QUIC_STATUS_INVALID_PARAMETER
  if bridge.api.ListenerStop.isNil:
    return QUIC_STATUS_NOT_SUPPORTED
  bridge.api.ListenerStop(listener)

proc closeListener*(bridge: RuntimeBridge; listener: HQUIC) =
  if bridge.isNil or listener.isNil:
    return
  bridge.dropListenerContext(listener)
  if bridge.api.isNil or bridge.api.ListenerClose.isNil:
    return
  bridge.api.ListenerClose(listener)

proc sendStream*(bridge: RuntimeBridge; stream: HQUIC; buffers: ptr QuicBuffer;
    bufferCount: uint32; flags: QUIC_SEND_FLAGS; clientContext: pointer): QUIC_STATUS =
  if bridge.isNil or bridge.api.isNil:
    return QUIC_STATUS_INVALID_STATE
  if stream.isNil:
    return QUIC_STATUS_INVALID_PARAMETER
  if bridge.api.StreamSend.isNil:
    return QUIC_STATUS_NOT_SUPPORTED
  bridge.api.StreamSend(stream, buffers, bufferCount, flags, clientContext)

proc sendDatagram*(bridge: RuntimeBridge; connection: HQUIC; buffers: ptr QuicBuffer;
    bufferCount: uint32; flags: QUIC_SEND_FLAGS; clientContext: pointer): QUIC_STATUS =
  if bridge.isNil or bridge.api.isNil:
    return QUIC_STATUS_INVALID_STATE
  if connection.isNil:
    return QUIC_STATUS_INVALID_PARAMETER
  if bridge.api.DatagramSend.isNil:
    return QUIC_STATUS_NOT_SUPPORTED
  bridge.api.DatagramSend(connection, buffers, bufferCount, flags, clientContext)

proc close*(bridge: var RuntimeBridge) =
  if bridge.isNil:
    return
  bridge.connectionContexts.clear()
  bridge.streamContexts.clear()
  bridge.listenerContexts.clear()
  if bridge.ownsRuntime:
    var runtime = bridge.runtime
    unloadMsQuic(runtime)
  bridge.runtime = nil
  bridge.api = nil
  bridge = nil
