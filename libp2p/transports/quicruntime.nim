## 统一 QUIC runtime 元信息抽象。
##
## 当前仓库里的纯 Nim QUIC runtime，就是编译期 builtin 的
## MsQuic-compatible `nim-msquic` 实现；并不存在独立接入的
## `libp2p/nim-quic` 第二实现。
##
## transport 与 Diagnostics 需要一个统一的 runtime 视图，至少能区分：
## - 编译期 builtin 的纯 Nim MsQuic-compatible runtime
## - 通过 `libmsquic.so` / `libmsquic.dylib` / `msquic.dll` 加载的原生
##   MsQuic runtime

import std/[locks, options, strutils, tables]
import chronos

when defined(libp2p_msquic_experimental):
  import ./msquicdriver as msquicdrv
  import ./msquicruntime
  import "nim-msquic/api/event_model" as msevents
  import "nim-msquic/api/runtime_bridge" as msruntime
  import "nim-msquic/api/ffi_loader" as msffi
  export msquicdrv.MsQuicLoadOptions
  export msquicdrv.MsQuicTransportConfig
  export msquicdrv.MsQuicTransportHandle
  export msquicdrv.MsQuicConnectionState
  export msquicdrv.MsQuicConnectionPathState
  export msquicdrv.MsQuicListenerState
  export msquicdrv.MsQuicStreamState
  export msquicdrv.MsQuicEventQueueClosed
  export msquicdrv.DefaultEventQueueLimit
  export msquicdrv.DefaultEventPollInterval
  export msquicdrv.MsQuicListenerPreconfiguredNote
  export msquicdrv.initMsQuicTransport
  export msquicdrv.loadCredential
  export msquicdrv.adoptConnection
  export msquicdrv.shutdownConnection
  export msquicdrv.setConnectionBoolParam
  export msquicdrv.enableConnectionDatagramReceive
  export msquicdrv.enableConnectionDatagramSend
  export msquicdrv.sendDatagram
  export msquicdrv.startStream
  export msquicdrv.shutdownStream
  export msquicdrv.startListener
  export msquicdrv.stopListener
  export msquicdrv.getListenerAddress
  export msquicdrv.nextListenerEvent
  export msquicdrv.nextConnectionEvent
  export msquicdrv.nextStreamEvent
  export msquicdrv.pushStreamReceive
  export msquicdrv.waitStreamStart
  export msquicdrv.readStream
  export msquicdrv.writeStream
  export msquicdrv.writeStreamAndWait
  export msquicdrv.beginSendFin
  export msquicdrv.streamId
  export msquicdrv.peekPendingStreamState
  export msquicdrv.popPendingStreamState
  export msquicdrv.restorePendingStreamState
  export msquicdrv.awaitPendingStreamState
  export msquicdrv.takePendingConnection
  export msquicdrv.getConnectionRemoteAddress
  export msquicdrv.getConnectionLocalAddress
  export msquicdrv.connectionHandshakeComplete
  export msquicdrv.connectionCloseReason
  export msquicdrv.activeConnectionPathId
  export msquicdrv.knownConnectionPathCount
  export msquicdrv.connectionPathState
  export msquicdrv.triggerConnectionMigrationProbe
  export msquicdrv.confirmConnectionValidatedPath
  export msquicdrv.isLocalInitiated
  export msquicdrv.handoffReadWaiters
  export msquicdrv.markStreamStartComplete

type
  QuicRuntimeKind* = enum
    qrkUnavailable,
    qrkMsQuicBuiltin,
    qrkMsQuicNative

  QuicRuntimeInfo* = object
    kind*: QuicRuntimeKind
    implementation*: string
    path*: string
    requestedVersion*: uint32
    negotiatedVersion*: uint32
    compileTimeBuiltin*: bool
    loaded*: bool

  QuicConnectionEventKind* = enum
    qceConnected
    qceShutdownInitiated
    qceShutdownComplete
    qcePeerStreamStarted
    qceDatagramStateChanged
    qceDatagramReceived
    qceSettingsApplied
    qceParameterUpdated
    qceUnknown

  QuicConnectionEvent* = object
    connection*: pointer
    kind*: QuicConnectionEventKind
    sessionResumed*: bool
    negotiatedAlpn*: string
    errorCode*: uint64
    status*: uint32
    paramId*: uint32
    boolValue*: bool
    maxSendLength*: uint16
    handshakeCompleted*: bool
    peerAcknowledgedShutdown*: bool
    appCloseInProgress*: bool
    userContext*: pointer
    uintValue*: uint64
    note*: string
    stream*: pointer
    streamFlags*: uint32
    streamIsUnidirectional*: bool
    datagramPayload*: seq[byte]
    datagramFlags*: uint32

  QuicStreamEventKind* = enum
    qseStartComplete
    qseReceive
    qseSendComplete
    qsePeerSendShutdown
    qsePeerSendAborted
    qsePeerReceiveAborted
    qseSendShutdownComplete
    qseShutdownComplete
    qseIdealSendBufferSize
    qseCancelOnLoss
    qsePeerAccepted
    qseReceiveBufferNeeded
    qseUnknown

  QuicStreamEvent* = object
    stream*: pointer
    kind*: QuicStreamEventKind
    status*: uint32
    id*: uint64
    absoluteOffset*: uint64
    totalBufferLength*: uint64
    bufferCount*: uint32
    flags*: uint32
    cancelled*: bool
    peerAccepted*: bool
    graceful*: bool
    connectionShutdown*: bool
    appCloseInProgress*: bool
    connectionShutdownByApp*: bool
    connectionClosedRemotely*: bool
    connectionErrorCode*: uint64
    connectionCloseStatus*: uint32
    errorCode*: uint64
    clientContext*: pointer
    byteCount*: uint64
    bufferLengthNeeded*: uint64
    userContext*: pointer
    note*: string
    payload*: seq[byte]

  QuicListenerEventKind* = enum
    qleNewConnection
    qleStopComplete
    qleDosModeChanged
    qleUnknown

  QuicListenerEvent* = object
    listener*: pointer
    kind*: QuicListenerEventKind
    connection*: pointer
    info*: pointer
    dosModeEnabled*: bool
    appCloseInProgress*: bool
    userContext*: pointer
    note*: string

when defined(libp2p_msquic_experimental):
  type
    QuicRuntimeBuiltinPolicy* = msffi.MsQuicBuiltinPolicy
    QuicRuntimeConfig* = msquicdrv.MsQuicTransportConfig
    QuicRuntimeHandle* = msquicdrv.MsQuicTransportHandle
    QuicRuntimeConnectionHandler* = proc(event: QuicConnectionEvent) {.gcsafe.}
    QuicRuntimeStreamHandler* = proc(event: QuicStreamEvent) {.gcsafe.}
    QuicRuntimeListenerHandler* = proc(event: QuicListenerEvent) {.gcsafe.}
    QuicRuntimeConnectionState* = msquicdrv.MsQuicConnectionState
    QuicRuntimeListenerState* = msquicdrv.MsQuicListenerState
    QuicRuntimeStreamState* = msquicdrv.MsQuicStreamState
    QuicRuntimeEventQueueClosed* = msquicdrv.MsQuicEventQueueClosed
    QuicRuntimePreference* = enum
      qrpAuto
      qrpNativeOnly
      qrpBuiltinPreferred
      qrpBuiltinOnly

    QuicRuntimeHandlerEntry = object
      handle: pointer
      context: pointer

    QuicRuntimeConnectionHandlerContext = object
      handler: QuicRuntimeConnectionHandler
      userContext: pointer

    QuicRuntimeStreamHandlerContext = object
      handler: QuicRuntimeStreamHandler
      userContext: pointer

    QuicRuntimeListenerHandlerContext = object
      handler: QuicRuntimeListenerHandler
      userContext: pointer

    QuicRuntimeConnectionHandlerAdapter = object
      handler: msquicdrv.MsQuicConnectionHandler
      userContext: pointer
      context: ptr QuicRuntimeConnectionHandlerContext

    QuicRuntimeStreamHandlerAdapter = object
      handler: msquicdrv.MsQuicStreamHandler
      userContext: pointer
      context: ptr QuicRuntimeStreamHandlerContext

    QuicRuntimeListenerHandlerAdapter = object
      handler: msquicdrv.MsQuicListenerHandler
      userContext: pointer
      context: ptr QuicRuntimeListenerHandlerContext

  var quicRuntimeHandlerLock: Lock
  var quicRuntimeConnectionHandlers = initTable[pointer, QuicRuntimeHandlerEntry]()
  var quicRuntimeConnectionContexts = initTable[pointer, pointer]()
  var quicRuntimeStreamHandlers = initTable[pointer, QuicRuntimeHandlerEntry]()
  var quicRuntimeStreamContexts = initTable[pointer, pointer]()
  var quicRuntimeListenerHandlers = initTable[pointer, QuicRuntimeHandlerEntry]()
  var quicRuntimeListenerContexts = initTable[pointer, pointer]()

  once initLock(quicRuntimeHandlerLock)

  template withQuicRuntimeHandlerLock(body: untyped) =
    acquire(quicRuntimeHandlerLock)
    try:
      body
    finally:
      release(quicRuntimeHandlerLock)

  template quicRuntimeSafe(body: untyped) =
    {.cast(gcsafe).}:
      body

proc kindLabel*(kind: QuicRuntimeKind): string {.inline, gcsafe.} =
  case kind
  of qrkUnavailable:
    "unavailable"
  of qrkMsQuicBuiltin:
    "msquic_builtin"
  of qrkMsQuicNative:
    "msquic_native"

proc kindLabel*(kind: QuicConnectionEventKind): string {.inline, gcsafe.} =
  case kind
  of qceConnected:
    "connected"
  of qceShutdownInitiated:
    "shutdown_initiated"
  of qceShutdownComplete:
    "shutdown_complete"
  of qcePeerStreamStarted:
    "peer_stream_started"
  of qceDatagramStateChanged:
    "datagram_state_changed"
  of qceDatagramReceived:
    "datagram_received"
  of qceSettingsApplied:
    "settings_applied"
  of qceParameterUpdated:
    "parameter_updated"
  of qceUnknown:
    "unknown"

proc kindLabel*(kind: QuicStreamEventKind): string {.inline, gcsafe.} =
  case kind
  of qseStartComplete:
    "start_complete"
  of qseReceive:
    "receive"
  of qseSendComplete:
    "send_complete"
  of qsePeerSendShutdown:
    "peer_send_shutdown"
  of qsePeerSendAborted:
    "peer_send_aborted"
  of qsePeerReceiveAborted:
    "peer_receive_aborted"
  of qseSendShutdownComplete:
    "send_shutdown_complete"
  of qseShutdownComplete:
    "shutdown_complete"
  of qseIdealSendBufferSize:
    "ideal_send_buffer_size"
  of qseCancelOnLoss:
    "cancel_on_loss"
  of qsePeerAccepted:
    "peer_accepted"
  of qseReceiveBufferNeeded:
    "receive_buffer_needed"
  of qseUnknown:
    "unknown"

proc kindLabel*(kind: QuicListenerEventKind): string {.inline, gcsafe.} =
  case kind
  of qleNewConnection:
    "new_connection"
  of qleStopComplete:
    "stop_complete"
  of qleDosModeChanged:
    "dos_mode_changed"
  of qleUnknown:
    "unknown"

proc isPureNimRuntime*(info: QuicRuntimeInfo): bool {.inline, gcsafe.} =
  info.kind == qrkMsQuicBuiltin

when defined(libp2p_msquic_experimental):
  ## 运行时 bridge 用固定路径哨兵标识 builtin 纯 Nim runtime。保留既有字符串，
  ## 避免破坏已有 diagnostics/FFI 输出。
  const BuiltinRuntimeName = "builtin-nim-quic-native"

  proc toQuicEvent*(event: msevents.ConnectionEvent): QuicConnectionEvent {.inline, gcsafe.}
  proc toQuicEvent*(event: msevents.StreamEvent): QuicStreamEvent {.inline, gcsafe.}
  proc toQuicEvent*(event: msevents.ListenerEvent): QuicListenerEvent {.inline, gcsafe.}

  proc unwrapConnectionUserContext(rawUserContext: pointer): pointer {.gcsafe.} =
    if rawUserContext.isNil:
      return nil
    result = rawUserContext
    quicRuntimeSafe:
      withQuicRuntimeHandlerLock:
        if quicRuntimeConnectionContexts.hasKey(rawUserContext):
          result = quicRuntimeConnectionContexts.getOrDefault(rawUserContext)

  proc unwrapStreamUserContext(rawUserContext: pointer): pointer {.gcsafe.} =
    if rawUserContext.isNil:
      return nil
    result = rawUserContext
    quicRuntimeSafe:
      withQuicRuntimeHandlerLock:
        if quicRuntimeStreamContexts.hasKey(rawUserContext):
          result = quicRuntimeStreamContexts.getOrDefault(rawUserContext)

  proc unwrapListenerUserContext(rawUserContext: pointer): pointer {.gcsafe.} =
    if rawUserContext.isNil:
      return nil
    result = rawUserContext
    quicRuntimeSafe:
      withQuicRuntimeHandlerLock:
        if quicRuntimeListenerContexts.hasKey(rawUserContext):
          result = quicRuntimeListenerContexts.getOrDefault(rawUserContext)

  proc freeConnectionHandlerContext(context: ptr QuicRuntimeConnectionHandlerContext) {.inline, gcsafe.} =
    if not context.isNil:
      deallocShared(context)

  proc freeStreamHandlerContext(context: ptr QuicRuntimeStreamHandlerContext) {.inline, gcsafe.} =
    if not context.isNil:
      deallocShared(context)

  proc freeListenerHandlerContext(context: ptr QuicRuntimeListenerHandlerContext) {.inline, gcsafe.} =
    if not context.isNil:
      deallocShared(context)

  proc registerConnectionHandler(
      state: QuicRuntimeConnectionState, handle: QuicRuntimeHandle,
      context: ptr QuicRuntimeConnectionHandlerContext
  ) {.gcsafe.} =
    if state.isNil or context.isNil:
      return
    let stateKey = cast[pointer](state)
    let contextKey = cast[pointer](context)
    var oldContext: pointer = nil
    quicRuntimeSafe:
      withQuicRuntimeHandlerLock:
        let oldEntry = quicRuntimeConnectionHandlers.getOrDefault(stateKey)
        if not oldEntry.context.isNil:
          oldContext = oldEntry.context
          quicRuntimeConnectionContexts.del(oldEntry.context)
        quicRuntimeConnectionHandlers[stateKey] = QuicRuntimeHandlerEntry(
          handle: cast[pointer](handle),
          context: contextKey
        )
        quicRuntimeConnectionContexts[contextKey] = context.userContext
    if not oldContext.isNil:
      freeConnectionHandlerContext(
        cast[ptr QuicRuntimeConnectionHandlerContext](oldContext)
      )

  proc registerStreamHandler(
      state: QuicRuntimeStreamState, handle: QuicRuntimeHandle,
      context: ptr QuicRuntimeStreamHandlerContext
  ) {.gcsafe.} =
    if state.isNil or context.isNil:
      return
    let stateKey = cast[pointer](state)
    let contextKey = cast[pointer](context)
    var oldContext: pointer = nil
    quicRuntimeSafe:
      withQuicRuntimeHandlerLock:
        let oldEntry = quicRuntimeStreamHandlers.getOrDefault(stateKey)
        if not oldEntry.context.isNil:
          oldContext = oldEntry.context
          quicRuntimeStreamContexts.del(oldEntry.context)
        quicRuntimeStreamHandlers[stateKey] = QuicRuntimeHandlerEntry(
          handle: cast[pointer](handle),
          context: contextKey
        )
        quicRuntimeStreamContexts[contextKey] = context.userContext
    if not oldContext.isNil:
      freeStreamHandlerContext(cast[ptr QuicRuntimeStreamHandlerContext](oldContext))

  proc registerListenerHandler(
      state: QuicRuntimeListenerState, handle: QuicRuntimeHandle,
      context: ptr QuicRuntimeListenerHandlerContext
  ) {.gcsafe.} =
    if state.isNil or context.isNil:
      return
    let stateKey = cast[pointer](state)
    let contextKey = cast[pointer](context)
    var oldContext: pointer = nil
    quicRuntimeSafe:
      withQuicRuntimeHandlerLock:
        let oldEntry = quicRuntimeListenerHandlers.getOrDefault(stateKey)
        if not oldEntry.context.isNil:
          oldContext = oldEntry.context
          quicRuntimeListenerContexts.del(oldEntry.context)
        quicRuntimeListenerHandlers[stateKey] = QuicRuntimeHandlerEntry(
          handle: cast[pointer](handle),
          context: contextKey
        )
        quicRuntimeListenerContexts[contextKey] = context.userContext
    if not oldContext.isNil:
      freeListenerHandlerContext(
        cast[ptr QuicRuntimeListenerHandlerContext](oldContext)
      )

  proc unregisterConnectionHandler(state: QuicRuntimeConnectionState) {.gcsafe.} =
    if state.isNil:
      return
    let stateKey = cast[pointer](state)
    var contextKey: pointer = nil
    quicRuntimeSafe:
      withQuicRuntimeHandlerLock:
        let entry = quicRuntimeConnectionHandlers.getOrDefault(stateKey)
        if not entry.context.isNil:
          contextKey = entry.context
          quicRuntimeConnectionHandlers.del(stateKey)
          quicRuntimeConnectionContexts.del(entry.context)
    if not contextKey.isNil:
      freeConnectionHandlerContext(
        cast[ptr QuicRuntimeConnectionHandlerContext](contextKey)
      )

  proc unregisterStreamHandler(state: QuicRuntimeStreamState) {.gcsafe.} =
    if state.isNil:
      return
    let stateKey = cast[pointer](state)
    var contextKey: pointer = nil
    quicRuntimeSafe:
      withQuicRuntimeHandlerLock:
        let entry = quicRuntimeStreamHandlers.getOrDefault(stateKey)
        if not entry.context.isNil:
          contextKey = entry.context
          quicRuntimeStreamHandlers.del(stateKey)
          quicRuntimeStreamContexts.del(entry.context)
    if not contextKey.isNil:
      freeStreamHandlerContext(cast[ptr QuicRuntimeStreamHandlerContext](contextKey))

  proc unregisterListenerHandler(state: QuicRuntimeListenerState) {.gcsafe.} =
    if state.isNil:
      return
    let stateKey = cast[pointer](state)
    var contextKey: pointer = nil
    quicRuntimeSafe:
      withQuicRuntimeHandlerLock:
        let entry = quicRuntimeListenerHandlers.getOrDefault(stateKey)
        if not entry.context.isNil:
          contextKey = entry.context
          quicRuntimeListenerHandlers.del(stateKey)
          quicRuntimeListenerContexts.del(entry.context)
    if not contextKey.isNil:
      freeListenerHandlerContext(
        cast[ptr QuicRuntimeListenerHandlerContext](contextKey)
      )

  proc releaseRuntimeHandlers(handle: QuicRuntimeHandle) {.gcsafe.} =
    if handle.isNil:
      return
    let handleKey = cast[pointer](handle)
    var connectionContexts: seq[pointer] = @[]
    var connectionStates: seq[pointer] = @[]
    var streamContexts: seq[pointer] = @[]
    var streamStates: seq[pointer] = @[]
    var listenerContexts: seq[pointer] = @[]
    var listenerStates: seq[pointer] = @[]
    quicRuntimeSafe:
      withQuicRuntimeHandlerLock:
        for stateKey, entry in quicRuntimeConnectionHandlers.pairs:
          if entry.handle == handleKey:
            connectionStates.add(stateKey)
            connectionContexts.add(entry.context)
        for stateKey in connectionStates:
          let entry = quicRuntimeConnectionHandlers.getOrDefault(stateKey)
          if not entry.context.isNil:
            quicRuntimeConnectionContexts.del(entry.context)
            quicRuntimeConnectionHandlers.del(stateKey)
        for stateKey, entry in quicRuntimeStreamHandlers.pairs:
          if entry.handle == handleKey:
            streamStates.add(stateKey)
            streamContexts.add(entry.context)
        for stateKey in streamStates:
          let entry = quicRuntimeStreamHandlers.getOrDefault(stateKey)
          if not entry.context.isNil:
            quicRuntimeStreamContexts.del(entry.context)
            quicRuntimeStreamHandlers.del(stateKey)
        for stateKey, entry in quicRuntimeListenerHandlers.pairs:
          if entry.handle == handleKey:
            listenerStates.add(stateKey)
            listenerContexts.add(entry.context)
        for stateKey in listenerStates:
          let entry = quicRuntimeListenerHandlers.getOrDefault(stateKey)
          if not entry.context.isNil:
            quicRuntimeListenerContexts.del(entry.context)
            quicRuntimeListenerHandlers.del(stateKey)
    for context in connectionContexts:
      freeConnectionHandlerContext(
        cast[ptr QuicRuntimeConnectionHandlerContext](context)
      )
    for context in streamContexts:
      freeStreamHandlerContext(cast[ptr QuicRuntimeStreamHandlerContext](context))
    for context in listenerContexts:
      freeListenerHandlerContext(
        cast[ptr QuicRuntimeListenerHandlerContext](context)
      )

  proc quicRuntimeConnectionEventRelay(event: msevents.ConnectionEvent) {.gcsafe.} =
    if event.userContext.isNil:
      return
    let context = cast[ptr QuicRuntimeConnectionHandlerContext](event.userContext)
    if context.isNil or context.handler.isNil:
      return
    context.handler(toQuicEvent(event))

  proc quicRuntimeStreamEventRelay(event: msevents.StreamEvent) {.gcsafe.} =
    if event.userContext.isNil:
      return
    let context = cast[ptr QuicRuntimeStreamHandlerContext](event.userContext)
    if context.isNil or context.handler.isNil:
      return
    context.handler(toQuicEvent(event))

  proc quicRuntimeListenerEventRelay(event: msevents.ListenerEvent) {.gcsafe.} =
    if event.userContext.isNil:
      return
    let context = cast[ptr QuicRuntimeListenerHandlerContext](event.userContext)
    if context.isNil or context.handler.isNil:
      return
    context.handler(toQuicEvent(event))

  proc initConnectionHandlerAdapter(
      handler: QuicRuntimeConnectionHandler, userContext: pointer
  ): QuicRuntimeConnectionHandlerAdapter {.gcsafe.} =
    if handler.isNil:
      return QuicRuntimeConnectionHandlerAdapter(
        handler: nil,
        userContext: userContext,
        context: nil
      )
    let context = cast[ptr QuicRuntimeConnectionHandlerContext](
      allocShared0(sizeof(QuicRuntimeConnectionHandlerContext))
    )
    context.handler = handler
    context.userContext = userContext
    QuicRuntimeConnectionHandlerAdapter(
      handler: quicRuntimeConnectionEventRelay,
      userContext: cast[pointer](context),
      context: context
    )

  proc initStreamHandlerAdapter(
      handler: QuicRuntimeStreamHandler, userContext: pointer
  ): QuicRuntimeStreamHandlerAdapter {.gcsafe.} =
    if handler.isNil:
      return QuicRuntimeStreamHandlerAdapter(
        handler: nil,
        userContext: userContext,
        context: nil
      )
    let context = cast[ptr QuicRuntimeStreamHandlerContext](
      allocShared0(sizeof(QuicRuntimeStreamHandlerContext))
    )
    context.handler = handler
    context.userContext = userContext
    QuicRuntimeStreamHandlerAdapter(
      handler: quicRuntimeStreamEventRelay,
      userContext: cast[pointer](context),
      context: context
    )

  proc initListenerHandlerAdapter(
      handler: QuicRuntimeListenerHandler, userContext: pointer
  ): QuicRuntimeListenerHandlerAdapter {.gcsafe.} =
    if handler.isNil:
      return QuicRuntimeListenerHandlerAdapter(
        handler: nil,
        userContext: userContext,
        context: nil
      )
    let context = cast[ptr QuicRuntimeListenerHandlerContext](
      allocShared0(sizeof(QuicRuntimeListenerHandlerContext))
    )
    context.handler = handler
    context.userContext = userContext
    QuicRuntimeListenerHandlerAdapter(
      handler: quicRuntimeListenerEventRelay,
      userContext: cast[pointer](context),
      context: context
    )

  proc toQuicEventKind*(kind: msevents.ConnectionEventKind): QuicConnectionEventKind {.inline, gcsafe.} =
    case kind
    of msevents.ceConnected:
      qceConnected
    of msevents.ceShutdownInitiated:
      qceShutdownInitiated
    of msevents.ceShutdownComplete:
      qceShutdownComplete
    of msevents.cePeerStreamStarted:
      qcePeerStreamStarted
    of msevents.ceDatagramStateChanged:
      qceDatagramStateChanged
    of msevents.ceDatagramReceived:
      qceDatagramReceived
    of msevents.ceSettingsApplied:
      qceSettingsApplied
    of msevents.ceParameterUpdated:
      qceParameterUpdated
    of msevents.ceUnknown:
      qceUnknown

  proc toQuicEventKind*(kind: msevents.StreamEventKind): QuicStreamEventKind {.inline, gcsafe.} =
    case kind
    of msevents.seStartComplete:
      qseStartComplete
    of msevents.seReceive:
      qseReceive
    of msevents.seSendComplete:
      qseSendComplete
    of msevents.sePeerSendShutdown:
      qsePeerSendShutdown
    of msevents.sePeerSendAborted:
      qsePeerSendAborted
    of msevents.sePeerReceiveAborted:
      qsePeerReceiveAborted
    of msevents.seSendShutdownComplete:
      qseSendShutdownComplete
    of msevents.seShutdownComplete:
      qseShutdownComplete
    of msevents.seIdealSendBufferSize:
      qseIdealSendBufferSize
    of msevents.seCancelOnLoss:
      qseCancelOnLoss
    of msevents.sePeerAccepted:
      qsePeerAccepted
    of msevents.seReceiveBufferNeeded:
      qseReceiveBufferNeeded
    of msevents.seUnknown:
      qseUnknown

  proc toQuicEventKind*(kind: msevents.ListenerEventKind): QuicListenerEventKind {.inline, gcsafe.} =
    case kind
    of msevents.leNewConnection:
      qleNewConnection
    of msevents.leStopComplete:
      qleStopComplete
    of msevents.leDosModeChanged:
      qleDosModeChanged
    of msevents.leUnknown:
      qleUnknown

  proc toQuicEvent*(event: msevents.ConnectionEvent): QuicConnectionEvent {.inline, gcsafe.} =
    QuicConnectionEvent(
      connection: event.connection,
      kind: toQuicEventKind(event.kind),
      sessionResumed: event.sessionResumed,
      negotiatedAlpn: event.negotiatedAlpn,
      errorCode: event.errorCode,
      status: event.status,
      paramId: event.paramId,
      boolValue: event.boolValue,
      maxSendLength: event.maxSendLength,
      handshakeCompleted: event.handshakeCompleted,
      peerAcknowledgedShutdown: event.peerAcknowledgedShutdown,
      appCloseInProgress: event.appCloseInProgress,
      userContext: unwrapConnectionUserContext(event.userContext),
      uintValue: event.uintValue,
      note: event.note,
      stream: event.stream,
      streamFlags: event.streamFlags,
      streamIsUnidirectional: event.streamIsUnidirectional,
      datagramPayload: event.datagramPayload,
      datagramFlags: event.datagramFlags
    )

  proc toQuicEvent*(event: msevents.StreamEvent): QuicStreamEvent {.inline, gcsafe.} =
    QuicStreamEvent(
      stream: event.stream,
      kind: toQuicEventKind(event.kind),
      status: event.status,
      id: event.id,
      absoluteOffset: event.absoluteOffset,
      totalBufferLength: event.totalBufferLength,
      bufferCount: event.bufferCount,
      flags: event.flags,
      cancelled: event.cancelled,
      peerAccepted: event.peerAccepted,
      graceful: event.graceful,
      connectionShutdown: event.connectionShutdown,
      appCloseInProgress: event.appCloseInProgress,
      connectionShutdownByApp: event.connectionShutdownByApp,
      connectionClosedRemotely: event.connectionClosedRemotely,
      connectionErrorCode: event.connectionErrorCode,
      connectionCloseStatus: event.connectionCloseStatus,
      errorCode: event.errorCode,
      clientContext: event.clientContext,
      byteCount: event.byteCount,
      bufferLengthNeeded: event.bufferLengthNeeded,
      userContext: unwrapStreamUserContext(event.userContext),
      note: event.note,
      payload: event.payload
    )

  proc toQuicEvent*(event: msevents.ListenerEvent): QuicListenerEvent {.inline, gcsafe.} =
    QuicListenerEvent(
      listener: event.listener,
      kind: toQuicEventKind(event.kind),
      connection: event.connection,
      info: event.info,
      dosModeEnabled: event.dosModeEnabled,
      appCloseInProgress: event.appCloseInProgress,
      userContext: unwrapListenerUserContext(event.userContext),
      note: event.note
    )

  proc initUnavailableInfo(): QuicRuntimeInfo {.inline, gcsafe.} =
    QuicRuntimeInfo(
      kind: qrkUnavailable,
      implementation: kindLabel(qrkUnavailable),
      path: "",
      requestedVersion: 0'u32,
      negotiatedVersion: 0'u32,
      compileTimeBuiltin: msffi.CompileTimeBuiltinMsQuic,
      loaded: false
    )

  proc quicRuntimeInfo*(bridge: msruntime.RuntimeBridge): QuicRuntimeInfo {.gcsafe.} =
    if bridge.isNil:
      return initUnavailableInfo()
    let runtime = bridge.getRuntime()
    if runtime.isNil:
      return initUnavailableInfo()
    let rawPath = runtime.path.strip()
    result = QuicRuntimeInfo(
      path: rawPath,
      requestedVersion: uint32(runtime.versionRequested),
      negotiatedVersion: uint32(runtime.versionNegotiated),
      compileTimeBuiltin: msffi.CompileTimeBuiltinMsQuic,
      loaded: true
    )
    if rawPath == BuiltinRuntimeName:
      result.kind = qrkMsQuicBuiltin
    else:
      result.kind = qrkMsQuicNative
    result.implementation = kindLabel(result.kind)

  proc quicRuntimeInfo*(handle: msquicdrv.MsQuicTransportHandle): QuicRuntimeInfo {.gcsafe.} =
    if handle.isNil:
      return initUnavailableInfo()
    quicRuntimeInfo(handle.bridge)

  proc currentQuicRuntimeInfo*(): QuicRuntimeInfo =
    quicRuntimeInfo(msquicruntime.currentMsQuicBridge())

  proc createListener*(
      handle: QuicRuntimeHandle,
      handler: QuicRuntimeListenerHandler = nil,
      userContext: pointer = nil,
      address: pointer = nil,
      queueLimit: int = 0,
      pollInterval: Duration = DefaultEventPollInterval
  ): tuple[
      listener: pointer, state: Option[QuicRuntimeListenerState], error: string
    ] {.raises: [].} =
    let adapter = initListenerHandlerAdapter(handler, userContext)
    result = msquicdrv.createListener(
      handle,
      adapter.handler,
      adapter.userContext,
      address,
      queueLimit,
      pollInterval
    )
    if adapter.context.isNil:
      return
    if result.error.len > 0 or result.state.isNone:
      freeListenerHandlerContext(adapter.context)
    else:
      registerListenerHandler(result.state.get(), handle, adapter.context)

  proc attachIncomingConnection*(
      handle: QuicRuntimeHandle,
      connection: pointer,
      handler: QuicRuntimeConnectionHandler = nil,
      userContext: pointer = nil,
      queueLimit: int = 0,
      pollInterval: Duration = DefaultEventPollInterval
  ): tuple[state: Option[QuicRuntimeConnectionState], error: string] {.raises: [].} =
    let adapter = initConnectionHandlerAdapter(handler, userContext)
    result = msquicdrv.attachIncomingConnection(
      handle,
      connection,
      adapter.handler,
      adapter.userContext,
      queueLimit,
      pollInterval
    )
    if adapter.context.isNil:
      return
    if result.error.len > 0 or result.state.isNone:
      freeConnectionHandlerContext(adapter.context)
    else:
      registerConnectionHandler(result.state.get(), handle, adapter.context)

  proc attachIncomingConnectionAdopted*(
      handle: QuicRuntimeHandle,
      connection: pointer,
      handler: QuicRuntimeConnectionHandler = nil,
      userContext: pointer = nil,
      queueLimit: int = 0,
      pollInterval: Duration = DefaultEventPollInterval
  ): tuple[state: Option[QuicRuntimeConnectionState], error: string] {.raises: [].} =
    let adapter = initConnectionHandlerAdapter(handler, userContext)
    result = msquicdrv.attachIncomingConnectionAdopted(
      handle,
      connection,
      adapter.handler,
      adapter.userContext,
      queueLimit,
      pollInterval
    )
    if adapter.context.isNil:
      return
    if result.error.len > 0 or result.state.isNone:
      freeConnectionHandlerContext(adapter.context)
    else:
      registerConnectionHandler(result.state.get(), handle, adapter.context)

  proc dialConnection*(
      handle: QuicRuntimeHandle,
      serverName: string,
      port: uint16,
      handler: QuicRuntimeConnectionHandler = nil,
      userContext: pointer = nil,
      addressFamily: uint16 = 0,
      queueLimit: int = 0,
      pollInterval: Duration = DefaultEventPollInterval
  ): tuple[
      connection: pointer, state: Option[QuicRuntimeConnectionState], error: string
    ] {.raises: [].} =
    let adapter = initConnectionHandlerAdapter(handler, userContext)
    result = msquicdrv.dialConnection(
      handle,
      serverName,
      port,
      adapter.handler,
      adapter.userContext,
      addressFamily,
      queueLimit,
      pollInterval
    )
    if adapter.context.isNil:
      return
    if result.error.len > 0 or result.state.isNone:
      freeConnectionHandlerContext(adapter.context)
    else:
      registerConnectionHandler(result.state.get(), handle, adapter.context)

  proc createStream*(
      handle: QuicRuntimeHandle,
      connection: pointer,
      handler: QuicRuntimeStreamHandler = nil,
      flags: uint32 = 0'u32,
      userContext: pointer = nil,
      connectionState: QuicRuntimeConnectionState = nil,
      queueLimit: int = 0,
      pollInterval: Duration = DefaultEventPollInterval
  ): tuple[stream: pointer, state: Option[QuicRuntimeStreamState], error: string] {.raises: [].} =
    let adapter = initStreamHandlerAdapter(handler, userContext)
    result = msquicdrv.createStream(
      handle,
      connection,
      adapter.handler,
      flags,
      adapter.userContext,
      connectionState,
      queueLimit,
      pollInterval
    )
    if adapter.context.isNil:
      return
    if result.error.len > 0 or result.state.isNone:
      freeStreamHandlerContext(adapter.context)
    else:
      registerStreamHandler(result.state.get(), handle, adapter.context)

  proc adoptStream*(
      handle: QuicRuntimeHandle,
      stream: pointer,
      connectionState: QuicRuntimeConnectionState,
      handler: QuicRuntimeStreamHandler = nil,
      userContext: pointer = nil,
      queueLimit: int = 0,
      pollInterval: Duration = DefaultEventPollInterval
  ): tuple[state: Option[QuicRuntimeStreamState], error: string] {.raises: [].} =
    let adapter = initStreamHandlerAdapter(handler, userContext)
    result = msquicdrv.adoptStream(
      handle,
      stream,
      connectionState,
      adapter.handler,
      adapter.userContext,
      queueLimit,
      pollInterval
    )
    if adapter.context.isNil:
      return
    if result.error.len > 0 or result.state.isNone:
      freeStreamHandlerContext(adapter.context)
    else:
      registerStreamHandler(result.state.get(), handle, adapter.context)

  proc nextQuicConnectionEvent*(
      state: msquicdrv.MsQuicConnectionState
  ): Future[QuicConnectionEvent] {.async.} =
    try:
      return toQuicEvent(await msquicdrv.nextConnectionEvent(state))
    except QuicRuntimeEventQueueClosed as exc:
      unregisterConnectionHandler(state)
      raise exc

  proc nextQuicStreamEvent*(
      state: msquicdrv.MsQuicStreamState
  ): Future[QuicStreamEvent] {.async.} =
    try:
      return toQuicEvent(await msquicdrv.nextStreamEvent(state))
    except QuicRuntimeEventQueueClosed as exc:
      unregisterStreamHandler(state)
      raise exc

  proc nextQuicListenerEvent*(
      state: msquicdrv.MsQuicListenerState
  ): Future[QuicListenerEvent] {.async.} =
    try:
      return toQuicEvent(await msquicdrv.nextListenerEvent(state))
    except QuicRuntimeEventQueueClosed as exc:
      unregisterListenerHandler(state)
      raise exc

  proc closeConnection*(
      handle: QuicRuntimeHandle, connection: pointer,
      state: QuicRuntimeConnectionState = nil
  ) {.inline, raises: [].} =
    msquicdrv.closeConnection(handle, connection, state)

  proc closeListener*(
      handle: QuicRuntimeHandle, listener: pointer,
      state: QuicRuntimeListenerState = nil
  ) {.inline, raises: [].} =
    msquicdrv.closeListener(handle, listener, state)

  proc closeStream*(
      handle: QuicRuntimeHandle, stream: pointer,
      state: QuicRuntimeStreamState = nil
  ) {.inline, raises: [].} =
    msquicdrv.closeStream(handle, stream, state)

  proc shutdown*(handle: QuicRuntimeHandle) {.raises: [].} =
    try:
      msquicdrv.shutdown(handle)
    finally:
      releaseRuntimeHandlers(handle)

  proc setRuntimePreference*(
      options: var msffi.MsQuicLoadOptions, preference: QuicRuntimePreference
  ) {.inline, gcsafe.} =
    discard preference
    ## Repository policy: QUIC runtime is always the in-repo builtin implementation.
    ## Do not load libmsquic/libssl-backed native runtimes even if explicitly requested.
    options.explicitPath = ""
    options.allowFallback = false
    options.builtinPolicy = msffi.mbpOnly

  proc setRuntimePreference*(
      cfg: var msquicdrv.MsQuicTransportConfig, preference: QuicRuntimePreference
  ) {.inline, gcsafe.} =
    cfg.loadOptions.setRuntimePreference(preference)

  proc useAutoRuntime*(cfg: var msquicdrv.MsQuicTransportConfig) {.inline, gcsafe.} =
    cfg.setRuntimePreference(qrpAuto)

  proc useNativeRuntime*(cfg: var msquicdrv.MsQuicTransportConfig) {.inline, gcsafe.} =
    cfg.setRuntimePreference(qrpNativeOnly)

  proc preferBuiltinRuntime*(cfg: var msquicdrv.MsQuicTransportConfig) {.inline, gcsafe.} =
    cfg.setRuntimePreference(qrpBuiltinPreferred)

  proc useBuiltinRuntime*(cfg: var msquicdrv.MsQuicTransportConfig) {.inline, gcsafe.} =
    cfg.setRuntimePreference(qrpBuiltinOnly)

  proc initQuicRuntime*(
      cfg: QuicRuntimeConfig = QuicRuntimeConfig()
  ): tuple[handle: QuicRuntimeHandle, error: string] {.inline.} =
    msquicdrv.initMsQuicTransport(cfg)
else:
  proc currentQuicRuntimeInfo*(): QuicRuntimeInfo =
    QuicRuntimeInfo(
      kind: qrkUnavailable,
      implementation: kindLabel(qrkUnavailable),
      path: "",
      requestedVersion: 0'u32,
      negotiatedVersion: 0'u32,
      compileTimeBuiltin: false,
      loaded: false
    )
