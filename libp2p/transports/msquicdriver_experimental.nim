import std/[deques, locks, options, sequtils, strformat, strutils, tables]
import stew/byteutils

import chronos
import chronos/threadsync
import chronicles
import results

import ./msquicruntime
import "nim-msquic/api/runtime_bridge" as msruntime
import "nim-msquic/api/api_impl" as msapi
import "nim-msquic/api/settings_model" as mssettings
import "nim-msquic/api/event_model" as msevents
import "nim-msquic/api/tls_bridge" as mstls
import "nim-msquic/tls/common" as mstlstypes
import "nim-msquic/api/param_catalog" as msparams
import std/[posix]

export msquicruntime.MsQuicLoadOptions

logScope:
  topics = "libp2p msquicdriver"

const
  DefaultEventQueueLimit* = 0
  DefaultEventPollInterval* = 5.milliseconds
  DefaultAlpn = "libp2p"
  DefaultAppName = "nim-libp2p"
  InetAddrStrLen = 16
  Inet6AddrStrLen = 46
  MsQuicListenerPreconfiguredNote* = "msquic:preconfigured"
  DefaultPeerBidiStreamCount = 16'u16
  DefaultPeerUnidiStreamCount = 16'u16
  QuicSettingHandshakeIdleTimeoutFlag = 1'u64 shl 1
  QuicSettingIdleTimeoutFlag = 1'u64 shl 2
  QuicSettingKeepAliveIntervalFlag = 1'u64 shl 16
  QuicSettingPeerBidiStreamFlag = 1'u64 shl 18
  QuicSettingPeerUnidiStreamFlag = 1'u64 shl 19
  QuicStreamStartImmediateFlag = 0x0001'u32
  QuicStatusPending = msapi.QUIC_STATUS(
    when defined(windows):
      0x000703e5'u32
    else:
      0xFFFFFFFE'u32
  )

type
  MsQuicTransportConfig* = object
    loadOptions*: MsQuicLoadOptions
    alpns*: seq[string] = @[DefaultAlpn]
    eventQueueLimit*: int = DefaultEventQueueLimit
    eventPollInterval*: Duration = DefaultEventPollInterval
    handshakeIdleTimeoutMs*: uint64 = 0'u64
    idleTimeoutMs*: uint64 = 0'u64
    keepAliveIntervalMs*: uint32 = 0'u32
    peerBidiStreamCount*: uint16 = DefaultPeerBidiStreamCount
    peerUnidiStreamCount*: uint16 = DefaultPeerUnidiStreamCount
    appName*: string
    executionProfile*: uint32
    clientBindHost*: string
    clientBindPort*: uint16

  MsQuicSettings = object
    isSetFlags: uint64
    maxBytesPerKey: uint64
    handshakeIdleTimeoutMs: uint64
    idleTimeoutMs: uint64
    mtuDiscoverySearchCompleteTimeoutUs: uint64
    tlsClientMaxSendBuffer: uint32
    tlsServerMaxSendBuffer: uint32
    streamRecvWindowDefault: uint32
    streamRecvBufferDefault: uint32
    connFlowControlWindow: uint32
    maxWorkerQueueDelayUs: uint32
    maxStatelessOperations: uint32
    initialWindowPackets: uint32
    sendIdleTimeoutMs: uint32
    initialRttMs: uint32
    maxAckDelayMs: uint32
    disconnectTimeoutMs: uint32
    keepAliveIntervalMs: uint32
    congestionControlAlgorithm: uint16
    peerBidiStreamCount: uint16
    peerUnidiStreamCount: uint16
    maxBindingStatelessOperations: uint16
    statelessOperationExpirationMs: uint16
    minimumMtu: uint16
    maximumMtu: uint16
    sendBufferingEnabled: uint8
    maxOperationsPerDrain: uint8
    mtuDiscoveryMissingProbeCount: uint8
    destCidUpdateIdleTimeoutMs: uint32
    flags: uint64
    streamRecvWindowBidiLocalDefault: uint32
    streamRecvWindowBidiRemoteDefault: uint32
    streamRecvWindowUnidiDefault: uint32

  MsQuicTransportHandle* = ref object
    bridge*: RuntimeBridge
    registration*: msapi.HQUIC
    configuration*: msapi.HQUIC
    clientConfiguration*: msapi.HQUIC
    alpns*: seq[string]
    alpnBuffers*: seq[msapi.QuicBuffer]
    config*: MsQuicTransportConfig
    tlsBinding*: mstls.TlsCredentialBinding
    clientTlsBinding*: mstls.TlsCredentialBinding
    retainLock: Lock
    retainLockInit: bool
    activeConnectionStates: seq[MsQuicConnectionState]
    activeListenerStates: seq[MsQuicListenerState]
    activeStreamStates: seq[MsQuicStreamState]
    retainedConnectionStates: seq[MsQuicConnectionState]
    retainedListenerStates: seq[MsQuicListenerState]
    retainedStreamStates: seq[MsQuicStreamState]
    closed*: bool

  MsQuicConnectionHandler* = proc(event: msevents.ConnectionEvent) {.gcsafe.}
  MsQuicStreamHandler* = proc(event: msevents.StreamEvent) {.gcsafe.}
  MsQuicListenerHandler* = proc(event: msevents.ListenerEvent) {.gcsafe.}

  MsQuicConnectionState* = ref object
    handle*: MsQuicTransportHandle
    connection*: msapi.HQUIC
    queueLimit*: int
    pollInterval*: Duration
    lock: Lock
    lockInit: bool
    queue: Deque[MsQuicQueuedConnectionEvent]
    waiters: seq[Future[msevents.ConnectionEvent]]
    signal: ThreadSignalPtr
    signalLoop: Future[void]
    closed*: bool
    retainedForLateCallbacks: bool
    clientContext*: pointer
    externalHandler*: MsQuicConnectionHandler
    droppedEvents*: uint64
    pendingStreams: Table[pointer, MsQuicStreamState]
    pendingStreamOrder: Deque[pointer]
    knownStreams: Table[pointer, MsQuicStreamState]
    knownStreamsById: Table[uint64, MsQuicStreamState]
    pendingStreamWaiters: seq[Future[MsQuicStreamState]]

  MsQuicListenerState* = ref object
    handle*: MsQuicTransportHandle
    listener*: msapi.HQUIC
    queueLimit*: int
    pollInterval*: Duration
    lock: Lock
    lockInit: bool
    queue: Deque[MsQuicQueuedListenerEvent]
    waiters: seq[Future[msevents.ListenerEvent]]
    signal: ThreadSignalPtr
    signalLoop: Future[void]
    closed*: bool
    retainedForLateCallbacks: bool
    userContext*: pointer
    externalHandler*: MsQuicListenerHandler
    droppedEvents*: uint64
    pendingConnections: Table[pointer, MsQuicConnectionState]
  MsQuicPendingSend = ref object
    payload: seq[byte]
    buffer: msapi.QuicBuffer
    clientContext: pointer
    waiter: Future[void]
  MsQuicReadChunk = object
    payload: seq[byte]
    receiveLen: int
  MsQuicStreamState* = ref object
    handle*: MsQuicTransportHandle
    connection*: msapi.HQUIC
    stream*: msapi.HQUIC
    connectionState*: MsQuicConnectionState
    queueLimit*: int
    pollInterval*: Duration
    lock: Lock
    lockInit: bool
    queue: Deque[MsQuicQueuedStreamEvent]
    waiters: seq[Future[msevents.StreamEvent]]
    signal: ThreadSignalPtr
    signalLoop: Future[void]
    closed*: bool
    retainedForLateCallbacks: bool
    userContext*: pointer
    externalHandler*: MsQuicStreamHandler
    droppedEvents*: uint64
    readQueue*: Deque[MsQuicReadChunk]
    readWaiters*: seq[Future[seq[byte]]]
    pendingSends*: seq[MsQuicPendingSend]
    localInitiated: bool
    startComplete: bool
    startWaiters: seq[Future[void]]
    sendShutdownRequested: bool
    peerSendShutdown: bool
    streamIdKnown: bool
    streamIdCached: uint64
  MsQuicEventQueueClosed* = object of CatchableError
  MsTlsConfig = mstlstypes.TlsConfig

  MsQuicConnectionPathState* = object
    pathId*: uint8
    active*: bool
    validated*: bool
    challengeOutstanding*: bool
    responsePending*: bool

  MsQuicSharedText = object
    data: ptr char
    len: int

  MsQuicSharedBytes = object
    data: ptr uint8
    len: int

  MsQuicQueuedConnectionEvent = object
    connection: pointer
    kind: msevents.ConnectionEventKind
    sessionResumed: bool
    negotiatedAlpn: MsQuicSharedText
    errorCode: uint64
    status: uint32
    paramId: uint32
    boolValue: bool
    maxSendLength: uint16
    handshakeCompleted: bool
    peerAcknowledgedShutdown: bool
    appCloseInProgress: bool
    userContext: pointer
    uintValue: uint64
    note: MsQuicSharedText
    stream: pointer
    streamFlags: uint32
    streamIsUnidirectional: bool
    datagramPayload: MsQuicSharedBytes
    datagramFlags: uint32

  MsQuicQueuedStreamEvent = object
    stream: pointer
    kind: msevents.StreamEventKind
    status: uint32
    id: uint64
    absoluteOffset: uint64
    totalBufferLength: uint64
    bufferCount: uint32
    flags: uint32
    cancelled: bool
    peerAccepted: bool
    graceful: bool
    connectionShutdown: bool
    appCloseInProgress: bool
    connectionShutdownByApp: bool
    connectionClosedRemotely: bool
    connectionErrorCode: uint64
    connectionCloseStatus: uint32
    errorCode: uint64
    clientContext: pointer
    byteCount: uint64
    bufferLengthNeeded: uint64
    userContext: pointer
    note: MsQuicSharedText
    payload: MsQuicSharedBytes

  MsQuicQueuedListenerEvent = object
    listener: pointer
    kind: msevents.ListenerEventKind
    connection: pointer
    info: pointer
    dosModeEnabled: bool
    appCloseInProgress: bool
    userContext: pointer
    note: MsQuicSharedText

proc setConnectionLocalAddress*(handle: MsQuicTransportHandle; connection: pointer;
    address: TransportAddress): string {.raises: [].}
proc setConnectionRemoteAddress*(handle: MsQuicTransportHandle; connection: pointer;
    address: TransportAddress): string {.raises: [].}

proc close(state: MsQuicStreamState) {.raises: [].}
proc newMsQuicStreamState(handle: MsQuicTransportHandle; connection: msapi.HQUIC;
    queueLimit: int; pollInterval: Duration;
    handler: MsQuicStreamHandler; userContext: pointer;
    connState: MsQuicConnectionState; localInitiated: bool
  ): Result[MsQuicStreamState, string]
proc msquicStreamEventRelay(event: msevents.StreamEvent) {.gcsafe.}
proc triggerStreamDelivery(state: MsQuicStreamState)

proc failPendingSendWaiters(state: MsQuicStreamState; msg: string) {.gcsafe, raises: [].}

template withStateLock(state: typed; body: untyped) =
  when compiles(state.lock):
    acquire(state.lock)
    try:
      body
    finally:
      release(state.lock)

template withRetainLock(handle: MsQuicTransportHandle; body: untyped) =
  if handle.isNil or not handle.retainLockInit:
    body
  else:
    acquire(handle.retainLock)
    try:
      body
    finally:
      release(handle.retainLock)

proc retainForLateCallbacks(handle: MsQuicTransportHandle; state: MsQuicConnectionState) =
  if handle.isNil or state.isNil:
    return
  handle.withRetainLock:
    handle.retainedConnectionStates.add(state)

proc retainForLateCallbacks(handle: MsQuicTransportHandle; state: MsQuicListenerState) =
  if handle.isNil or state.isNil:
    return
  handle.withRetainLock:
    handle.retainedListenerStates.add(state)

proc retainForLateCallbacks(handle: MsQuicTransportHandle; state: MsQuicStreamState) =
  if handle.isNil or state.isNil:
    return
  handle.withRetainLock:
    handle.retainedStreamStates.add(state)

proc registerActiveState(handle: MsQuicTransportHandle; state: MsQuicConnectionState) =
  if handle.isNil or state.isNil:
    return
  handle.withRetainLock:
    if handle.activeConnectionStates.find(state) < 0:
      handle.activeConnectionStates.add(state)

proc registerActiveState(handle: MsQuicTransportHandle; state: MsQuicListenerState) =
  if handle.isNil or state.isNil:
    return
  handle.withRetainLock:
    if handle.activeListenerStates.find(state) < 0:
      handle.activeListenerStates.add(state)

proc registerActiveState(handle: MsQuicTransportHandle; state: MsQuicStreamState) =
  if handle.isNil or state.isNil:
    return
  handle.withRetainLock:
    if handle.activeStreamStates.find(state) < 0:
      handle.activeStreamStates.add(state)

proc unregisterActiveState(handle: MsQuicTransportHandle; state: MsQuicConnectionState) =
  if handle.isNil or state.isNil:
    return
  handle.withRetainLock:
    let idx = handle.activeConnectionStates.find(state)
    if idx >= 0:
      handle.activeConnectionStates.delete(idx)

proc unregisterActiveState(handle: MsQuicTransportHandle; state: MsQuicListenerState) =
  if handle.isNil or state.isNil:
    return
  handle.withRetainLock:
    let idx = handle.activeListenerStates.find(state)
    if idx >= 0:
      handle.activeListenerStates.delete(idx)

proc unregisterActiveState(handle: MsQuicTransportHandle; state: MsQuicStreamState) =
  if handle.isNil or state.isNil:
    return
  handle.withRetainLock:
    let idx = handle.activeStreamStates.find(state)
    if idx >= 0:
      handle.activeStreamStates.delete(idx)

proc resolvedAlpns(cfg: MsQuicTransportConfig): seq[string] =
  if cfg.alpns.len == 0:
    @[DefaultAlpn]
  else:
    var resultSeq: seq[string] = @[]
    for value in cfg.alpns:
      if value.len > 0:
        resultSeq.add(value)
    if resultSeq.len == 0:
      @[DefaultAlpn]
    else:
      resultSeq

proc buildAlpnBuffers(alpns: seq[string]): seq[msapi.QuicBuffer] =
  result = newSeq[msapi.QuicBuffer](alpns.len)
  for i, alpn in alpns:
    if alpn.len == 0:
      result[i] = msapi.QuicBuffer(Length: 0'u32, Buffer: nil)
    else:
      result[i] = msapi.QuicBuffer(
        Length: uint32(alpn.len),
        Buffer: cast[ptr uint8](alpn.cstring)
      )

proc safeCloseConfiguration(bridge: RuntimeBridge; configuration: msapi.HQUIC) {.gcsafe, inline.} =
  if bridge.isNil or configuration.isNil:
    return
  {.gcsafe.}:
    try:
      msruntime.closeConfiguration(bridge, configuration)
    except Exception:
      discard

proc safeCloseRegistration(bridge: RuntimeBridge; registration: msapi.HQUIC) {.gcsafe, inline.} =
  if bridge.isNil or registration.isNil:
    return
  {.gcsafe.}:
    try:
      msruntime.closeRegistration(bridge, registration)
    except Exception:
      discard

proc safeCloseConnection(bridge: RuntimeBridge; connection: msapi.HQUIC) {.gcsafe, inline.} =
  if bridge.isNil or connection.isNil:
    return
  {.gcsafe.}:
    try:
      msruntime.closeConnection(bridge, connection)
    except Exception:
      discard

proc safeShutdownConnection(
    bridge: RuntimeBridge; connection: msapi.HQUIC; flags: uint32 = 0'u32; errorCode: uint64 = 0'u64
) {.gcsafe, inline.} =
  if bridge.isNil or connection.isNil:
    return
  {.gcsafe.}:
    try:
      discard msruntime.shutdownConnection(bridge, connection, flags, errorCode)
    except Exception:
      discard

proc safeCloseListener(bridge: RuntimeBridge; listener: msapi.HQUIC) {.gcsafe, inline.} =
  if bridge.isNil or listener.isNil:
    return
  {.gcsafe.}:
    try:
      msruntime.closeListener(bridge, listener)
    except Exception:
      discard

proc safeCloseStream(bridge: RuntimeBridge; stream: msapi.HQUIC) {.gcsafe, inline.} =
  if bridge.isNil or stream.isNil:
    return
  {.gcsafe.}:
    try:
      msruntime.closeStream(bridge, stream)
    except Exception:
      discard

proc startListener*(handle: MsQuicTransportHandle; listener: pointer;
    alpns: ptr msapi.QuicBuffer = nil; alpnCount: uint32 = 0;
    address: pointer = nil): string {.raises: [].}

template closeSignal(state: typed) =
  when compiles(state.signal):
    if state.signal != nil:
      let _ = state.signal.close()
      state.signal = nil

proc toSharedText(value: string): MsQuicSharedText {.raises: [].} =
  if value.len <= 0:
    return
  result.len = value.len
  result.data = cast[ptr char](allocShared0(result.len + 1))
  if result.data.isNil:
    result.len = 0
    return
  copyMem(result.data, unsafeAddr value[0], result.len)

proc toSharedBytes(value: seq[byte]): MsQuicSharedBytes {.raises: [].} =
  if value.len <= 0:
    return
  result.len = value.len
  result.data = cast[ptr uint8](allocShared(result.len))
  if result.data.isNil:
    result.len = 0
    return
  copyMem(result.data, unsafeAddr value[0], result.len)

proc materializeAndFree(value: var MsQuicSharedText): string {.raises: [].} =
  if value.data.isNil or value.len <= 0:
    value.data = nil
    value.len = 0
    return ""
  result = newString(value.len)
  copyMem(addr result[0], value.data, value.len)
  deallocShared(value.data)
  value.data = nil
  value.len = 0

proc materializeAndFree(value: var MsQuicSharedBytes): seq[byte] {.raises: [].} =
  if value.data.isNil or value.len <= 0:
    value.data = nil
    value.len = 0
    return @[]
  result = newSeq[byte](value.len)
  copyMem(addr result[0], value.data, value.len)
  deallocShared(value.data)
  value.data = nil
  value.len = 0

proc release(value: var MsQuicSharedText) {.raises: [].} =
  if not value.data.isNil:
    deallocShared(value.data)
    value.data = nil
  value.len = 0

proc release(value: var MsQuicSharedBytes) {.raises: [].} =
  if not value.data.isNil:
    deallocShared(value.data)
    value.data = nil
  value.len = 0

proc toQueuedEvent(event: msevents.ConnectionEvent): MsQuicQueuedConnectionEvent {.raises: [].} =
  result.connection = event.connection
  result.kind = event.kind
  result.sessionResumed = event.sessionResumed
  result.negotiatedAlpn = toSharedText(event.negotiatedAlpn)
  result.errorCode = event.errorCode
  result.status = event.status
  result.paramId = event.paramId
  result.boolValue = event.boolValue
  result.maxSendLength = event.maxSendLength
  result.handshakeCompleted = event.handshakeCompleted
  result.peerAcknowledgedShutdown = event.peerAcknowledgedShutdown
  result.appCloseInProgress = event.appCloseInProgress
  result.userContext = event.userContext
  result.uintValue = event.uintValue
  result.note = toSharedText(event.note)
  result.stream = event.stream
  result.streamFlags = event.streamFlags
  result.streamIsUnidirectional = event.streamIsUnidirectional
  result.datagramPayload = toSharedBytes(event.datagramPayload)
  result.datagramFlags = event.datagramFlags

proc materializeAndFree(event: var MsQuicQueuedConnectionEvent): msevents.ConnectionEvent {.raises: [].} =
  result.connection = event.connection
  result.kind = event.kind
  result.sessionResumed = event.sessionResumed
  result.negotiatedAlpn = materializeAndFree(event.negotiatedAlpn)
  result.errorCode = event.errorCode
  result.status = event.status
  result.paramId = event.paramId
  result.boolValue = event.boolValue
  result.maxSendLength = event.maxSendLength
  result.handshakeCompleted = event.handshakeCompleted
  result.peerAcknowledgedShutdown = event.peerAcknowledgedShutdown
  result.appCloseInProgress = event.appCloseInProgress
  result.userContext = event.userContext
  result.uintValue = event.uintValue
  result.note = materializeAndFree(event.note)
  result.stream = event.stream
  result.streamFlags = event.streamFlags
  result.streamIsUnidirectional = event.streamIsUnidirectional
  result.datagramPayload = materializeAndFree(event.datagramPayload)
  result.datagramFlags = event.datagramFlags

proc release(event: var MsQuicQueuedConnectionEvent) {.raises: [].} =
  release(event.negotiatedAlpn)
  release(event.note)
  release(event.datagramPayload)

proc toQueuedEvent(event: msevents.StreamEvent): MsQuicQueuedStreamEvent {.raises: [].} =
  result.stream = event.stream
  result.kind = event.kind
  result.status = event.status
  result.id = event.id
  result.absoluteOffset = event.absoluteOffset
  result.totalBufferLength = event.totalBufferLength
  result.bufferCount = event.bufferCount
  result.flags = event.flags
  result.cancelled = event.cancelled
  result.peerAccepted = event.peerAccepted
  result.graceful = event.graceful
  result.connectionShutdown = event.connectionShutdown
  result.appCloseInProgress = event.appCloseInProgress
  result.connectionShutdownByApp = event.connectionShutdownByApp
  result.connectionClosedRemotely = event.connectionClosedRemotely
  result.connectionErrorCode = event.connectionErrorCode
  result.connectionCloseStatus = event.connectionCloseStatus
  result.errorCode = event.errorCode
  result.clientContext = event.clientContext
  result.byteCount = event.byteCount
  result.bufferLengthNeeded = event.bufferLengthNeeded
  result.userContext = event.userContext
  result.note = toSharedText(event.note)
  result.payload = toSharedBytes(event.payload)

proc materializeAndFree(event: var MsQuicQueuedStreamEvent): msevents.StreamEvent {.raises: [].} =
  result.stream = event.stream
  result.kind = event.kind
  result.status = event.status
  result.id = event.id
  result.absoluteOffset = event.absoluteOffset
  result.totalBufferLength = event.totalBufferLength
  result.bufferCount = event.bufferCount
  result.flags = event.flags
  result.cancelled = event.cancelled
  result.peerAccepted = event.peerAccepted
  result.graceful = event.graceful
  result.connectionShutdown = event.connectionShutdown
  result.appCloseInProgress = event.appCloseInProgress
  result.connectionShutdownByApp = event.connectionShutdownByApp
  result.connectionClosedRemotely = event.connectionClosedRemotely
  result.connectionErrorCode = event.connectionErrorCode
  result.connectionCloseStatus = event.connectionCloseStatus
  result.errorCode = event.errorCode
  result.clientContext = event.clientContext
  result.byteCount = event.byteCount
  result.bufferLengthNeeded = event.bufferLengthNeeded
  result.userContext = event.userContext
  result.note = materializeAndFree(event.note)
  result.payload = materializeAndFree(event.payload)

proc release(event: var MsQuicQueuedStreamEvent) {.raises: [].} =
  release(event.note)
  release(event.payload)

proc toQueuedEvent(event: msevents.ListenerEvent): MsQuicQueuedListenerEvent {.raises: [].} =
  result.listener = event.listener
  result.kind = event.kind
  result.connection = event.connection
  result.info = event.info
  result.dosModeEnabled = event.dosModeEnabled
  result.appCloseInProgress = event.appCloseInProgress
  result.userContext = event.userContext
  result.note = toSharedText(event.note)

proc materializeAndFree(event: var MsQuicQueuedListenerEvent): msevents.ListenerEvent {.raises: [].} =
  result.listener = event.listener
  result.kind = event.kind
  result.connection = event.connection
  result.info = event.info
  result.dosModeEnabled = event.dosModeEnabled
  result.appCloseInProgress = event.appCloseInProgress
  result.userContext = event.userContext
  result.note = materializeAndFree(event.note)

proc release(event: var MsQuicQueuedListenerEvent) {.raises: [].} =
  release(event.note)

proc failWaiters(state: MsQuicConnectionState) =
  var waiters: seq[Future[msevents.ConnectionEvent]]
  state.withStateLock:
    if state.waiters.len == 0:
      return
    waiters = state.waiters
    state.waiters.setLen(0)
  if waiters.len == 0:
    return
  for fut in waiters:
    if fut.isNil or fut.finished():
      continue
    let err = newException(MsQuicEventQueueClosed, "MsQuic connection event queue closed")
    fut.fail(err)

proc deliverConnectionEvents(state: MsQuicConnectionState) =
  while true:
    var fut: Future[msevents.ConnectionEvent] = nil
    var queuedEvent: MsQuicQueuedConnectionEvent
    var hasQueuedEvent = false
    var shouldFail = false
    state.withStateLock:
      if state.queue.len > 0 and state.waiters.len > 0:
        var nextEvent = state.queue.popFirst()
        queuedEvent = move(nextEvent)
        hasQueuedEvent = true
        fut = state.waiters[0]
        state.waiters.delete(0)
      elif state.closed and state.waiters.len > 0:
        shouldFail = true
      else:
        return
    if not fut.isNil:
      if not fut.finished():
        fut.complete(materializeAndFree(queuedEvent))
      elif hasQueuedEvent:
        release(queuedEvent)
    elif shouldFail:
      failWaiters(state)
      return

proc connectionSignalPump(state: MsQuicConnectionState): Future[void] {.async.} =
  try:
    while true:
      if state.isNil or state.closed:
        break
      if state.signal.isNil:
        await sleepAsync(state.pollInterval)
        continue
      try:
        await state.signal.wait()
      except CancelledError:
        break
      except Exception as exc:
        trace "MsQuic signal wait failed", err = exc.msg
        break
      deliverConnectionEvents(state)
  finally:
    failWaiters(state)
    closeSignal(state)
    state.signalLoop = nil

proc ensureConnectionSignalLoop(state: MsQuicConnectionState) =
  if state.signalLoop.isNil:
    let fut = connectionSignalPump(state)
    state.signalLoop = fut
    asyncSpawn fut

proc triggerConnectionDelivery(state: MsQuicConnectionState) =
  state.ensureConnectionSignalLoop()
  if state.signal.isNil:
    deliverConnectionEvents(state)
    return
  let res = state.signal.fireSync()
  if res.isErr():
    trace "MsQuic signal fire failed", err = res.error
    deliverConnectionEvents(state)

proc enqueueConnectionEvent(state: MsQuicConnectionState; event: msevents.ConnectionEvent) =
  var shouldSignal = false
  var dropped = false
  var droppedEvent: MsQuicQueuedConnectionEvent
  var hasDroppedEvent = false
  state.withStateLock:
    if state.closed:
      return
    if state.queueLimit > 0 and state.queue.len >= state.queueLimit:
      var oldest = state.queue.popFirst()
      droppedEvent = move(oldest)
      hasDroppedEvent = true
      inc state.droppedEvents
      dropped = true
    state.queue.addLast(toQueuedEvent(event))
    if state.waiters.len > 0:
      shouldSignal = true
  when defined(libp2p_msquic_debug):
    if event.kind == msevents.cePeerStreamStarted:
      warn "MsQuic enqueue peer stream event",
        stream = cast[uint64](event.stream),
        connection = cast[uint64](state.connection),
        waiters = state.waiters.len
  if dropped:
    trace "MsQuic event queue full, dropping oldest event",
      queueLimit = state.queueLimit, droppedEvents = state.droppedEvents
  if hasDroppedEvent:
    release(droppedEvent)
  if shouldSignal:
    triggerConnectionDelivery(state)

proc handleIncomingConnectionEvent(state: MsQuicConnectionState;
    rawEvent: msevents.ConnectionEvent) =
  if state.isNil:
    return
  var forwarded = rawEvent
  forwarded.userContext = state.clientContext
  if not state.externalHandler.isNil:
    try:
      state.externalHandler(forwarded)
    except Exception as exc:
      trace "MsQuic external handler raised", err = exc.msg
  enqueueConnectionEvent(state, forwarded)

proc cacheStreamId(state: MsQuicStreamState) {.gcsafe, raises: [].} =
  if state.isNil or state.streamIdKnown or state.handle.isNil or
      state.handle.bridge.isNil or state.stream.isNil:
    return
  var idRes: Result[uint64, string]
  {.cast(gcsafe).}:
    idRes = state.handle.bridge.streamId(state.stream)
  if idRes.isOk():
    state.streamIdCached = idRes.get()
    state.streamIdKnown = true

proc storePendingStream(state: MsQuicConnectionState; stream: pointer;
    streamState: MsQuicStreamState; prepend = false) =
  if state.isNil or stream.isNil or streamState.isNil:
    return
  cacheStreamId(streamState)
  var waiter: Future[MsQuicStreamState] = nil
  var queueHandle = false
  state.withStateLock:
    state.knownStreams[stream] = streamState
    if streamState.streamIdKnown:
      state.knownStreamsById[streamState.streamIdCached] = streamState
    var idx = 0
    while idx < state.pendingStreamWaiters.len:
      let candidate = state.pendingStreamWaiters[idx]
      if candidate.isNil or candidate.finished():
        state.pendingStreamWaiters.delete(idx)
      else:
        waiter = candidate
        state.pendingStreamWaiters.delete(idx)
        break
    if waiter.isNil:
      queueHandle = not state.pendingStreams.hasKey(stream)
      state.pendingStreams[stream] = streamState
      if queueHandle:
        if prepend:
          state.pendingStreamOrder.addFirst(stream)
        else:
          state.pendingStreamOrder.addLast(stream)
  when defined(libp2p_msquic_debug):
    warn "MsQuic pending stream stored",
      stream = cast[uint64](stream),
      connection = cast[uint64](state.connection)
  if not waiter.isNil and not waiter.finished():
    waiter.complete(streamState)

proc takePendingStream(state: MsQuicConnectionState; stream: pointer):
    Option[MsQuicStreamState] =
  if state.isNil or stream.isNil:
    return none(MsQuicStreamState)
  var result: Option[MsQuicStreamState]
  state.withStateLock:
    let pending = state.pendingStreams.getOrDefault(stream)
    if not pending.isNil:
      result = some(pending)
      state.pendingStreams.del(stream)
  when defined(libp2p_msquic_debug):
    if result.isSome:
      warn "MsQuic pending stream taken",
        stream = cast[uint64](stream),
        connection = cast[uint64](state.connection)
  result

proc lookupKnownStream(
    state: MsQuicConnectionState;
    stream: pointer
): MsQuicStreamState {.raises: [].} =
  if state.isNil or stream.isNil:
    return nil
  state.withStateLock:
    result = state.knownStreams.getOrDefault(stream)

proc lookupKnownStreamById*(
    state: MsQuicConnectionState;
    streamId: uint64
): MsQuicStreamState {.gcsafe, raises: [].} =
  if state.isNil:
    return nil
  state.withStateLock:
    result = state.knownStreamsById.getOrDefault(streamId)

proc unregisterKnownStream(
    state: MsQuicConnectionState;
    stream: pointer
) {.raises: [].} =
  if state.isNil or stream.isNil:
    return
  var known: MsQuicStreamState = nil
  state.withStateLock:
    known = state.knownStreams.getOrDefault(stream)
    state.knownStreams.del(stream)
    state.pendingStreams.del(stream)
    if not known.isNil and known.streamIdKnown:
      let current = state.knownStreamsById.getOrDefault(known.streamIdCached)
      if current == known:
        state.knownStreamsById.del(known.streamIdCached)

proc peekPendingStreamState*(state: MsQuicConnectionState): Option[MsQuicStreamState] =
  if state.isNil:
    return none(MsQuicStreamState)
  var result: Option[MsQuicStreamState]
  state.withStateLock:
    while state.pendingStreamOrder.len > 0:
      let streamPtr = state.pendingStreamOrder.peekFirst()
      let pending = state.pendingStreams.getOrDefault(streamPtr)
      if pending.isNil:
        discard state.pendingStreamOrder.popFirst()
        continue
      result = some(pending)
      break
  result

proc popPendingStreamState*(state: MsQuicConnectionState): Option[MsQuicStreamState] =
  if state.isNil:
    return none(MsQuicStreamState)
  var result: Option[MsQuicStreamState]
  state.withStateLock:
    while state.pendingStreamOrder.len > 0:
      let streamPtr = state.pendingStreamOrder.popFirst()
      let pending = state.pendingStreams.getOrDefault(streamPtr)
      if pending.isNil:
        continue
      result = some(pending)
      state.pendingStreams.del(streamPtr)
      break
  result

proc restorePendingStreamState*(
    state: MsQuicConnectionState,
    streamState: MsQuicStreamState
) {.gcsafe, raises: [].} =
  if state.isNil or streamState.isNil or streamState.stream.isNil:
    return
  streamState.connectionState = state
  storePendingStream(state, cast[pointer](streamState.stream), streamState, prepend = true)

proc isLocalInitiated*(state: MsQuicStreamState): bool {.inline.} =
  if state.isNil:
    return false
  state.localInitiated

proc close(state: MsQuicConnectionState) =
  if state.isNil:
    return
  let handle = state.handle
  var hadWaiters = false
  var pending: seq[MsQuicStreamState]
  var streamWaiters: seq[Future[MsQuicStreamState]]
  var shouldRetain = false
  state.withStateLock:
    if state.closed:
      return
    state.closed = true
    if not state.retainedForLateCallbacks:
      state.retainedForLateCallbacks = true
      shouldRetain = true
    hadWaiters = state.waiters.len > 0
    if state.pendingStreams.len > 0:
      pending = toSeq(state.pendingStreams.values)
      state.pendingStreams.clear()
      state.pendingStreamOrder.clear()
    else:
      state.pendingStreamOrder.clear()
    if state.knownStreamsById.len > 0:
      state.knownStreamsById.clear()
    while state.queue.len > 0:
      var queuedEvent = state.queue.popFirst()
      release(queuedEvent)
    if state.pendingStreamWaiters.len > 0:
      streamWaiters = state.pendingStreamWaiters
      state.pendingStreamWaiters.setLen(0)
  if not handle.isNil:
    handle.unregisterActiveState(state)
  if shouldRetain and not handle.isNil:
    handle.retainForLateCallbacks(state)
  if hadWaiters:
    triggerConnectionDelivery(state)
  deliverConnectionEvents(state)
  failWaiters(state)
  if streamWaiters.len > 0:
    let err = newException(MsQuicEventQueueClosed, "MsQuic connection state closed")
    for fut in streamWaiters:
      if fut.isNil or fut.finished():
        continue
      fut.fail(err)
  for streamState in pending:
    if streamState.isNil:
      continue
    if not streamState.stream.isNil and not state.handle.isNil:
      safeCloseStream(state.handle.bridge, streamState.stream)
    streamState.close()
  let loopActive = (not state.signalLoop.isNil) and (not state.signalLoop.finished())
  if loopActive:
    state.signalLoop.cancel()
  closeSignal(state)
  # MsQuic/runtime callbacks can still race in after we mark the state closed.
  # Keep the lock alive for the lifetime of the ref object to avoid late
  # callbacks touching a destroyed mutex on Android bionic.

proc newMsQuicConnectionState(handle: MsQuicTransportHandle;
    queueLimit: int; pollInterval: Duration;
    handler: MsQuicConnectionHandler; userContext: pointer
  ): Result[MsQuicConnectionState, string] =
  var state = MsQuicConnectionState(
    handle: handle,
    connection: nil,
    queueLimit: queueLimit,
    pollInterval: pollInterval,
    queue: initDeque[MsQuicQueuedConnectionEvent](),
    waiters: @[],
    signal: nil,
    signalLoop: nil,
    closed: false,
    retainedForLateCallbacks: false,
    clientContext: userContext,
    externalHandler: handler,
    droppedEvents: 0,
    pendingStreams: initTable[pointer, MsQuicStreamState](),
    pendingStreamOrder: initDeque[pointer](),
    knownStreams: initTable[pointer, MsQuicStreamState](),
    knownStreamsById: initTable[uint64, MsQuicStreamState](),
    pendingStreamWaiters: @[]
  )
  initLock(state.lock)
  state.lockInit = true
  let signalRes = ThreadSignalPtr.new()
  if signalRes.isErr():
    close(state)
    return err("failed to initialize MsQuic thread signal: " & signalRes.error)
  state.signal = signalRes.get()
  state.ensureConnectionSignalLoop()
  handle.registerActiveState(state)
  ok(state)

proc nextConnectionEvent*(state: MsQuicConnectionState):
    Future[msevents.ConnectionEvent] =
  if state.isNil:
    let fut = Future[msevents.ConnectionEvent].Raising(
      [CancelledError, MsQuicEventQueueClosed]
    ).init(
      "msquic.connectionEvent.nil"
    )
    fut.fail(newException(MsQuicEventQueueClosed, "MsQuic connection state unavailable"))
    return fut

  let fut = Future[msevents.ConnectionEvent].Raising(
    [CancelledError, MsQuicEventQueueClosed]
  ).init(
    "msquic.connectionEvent.next"
  )
  proc cancellation(udata: pointer) {.gcsafe, raises: [].} =
    if state.isNil:
      return
    state.withStateLock:
      let idx = state.waiters.find(fut)
      if idx >= 0:
        state.waiters.delete(idx)
  fut.cancelCallback = cancellation

  var queuedImmediate: MsQuicQueuedConnectionEvent
  var hasImmediate = false
  var shouldFail = false
  state.withStateLock:
    if state.closed and state.queue.len == 0:
      shouldFail = true
    elif state.queue.len > 0:
      var nextEvent = state.queue.popFirst()
      queuedImmediate = move(nextEvent)
      hasImmediate = true
    else:
      state.waiters.add(fut)

  if shouldFail:
    fut.fail(newException(MsQuicEventQueueClosed, "MsQuic connection state closed"))
  elif hasImmediate:
    fut.complete(materializeAndFree(queuedImmediate))
  else:
    state.ensureConnectionSignalLoop()
  fut

proc awaitPendingStreamState*(state: MsQuicConnectionState):
    Future[MsQuicStreamState] =
  let fut = Future[MsQuicStreamState].Raising(
    [CancelledError, MsQuicEventQueueClosed]
  ).init("msquic.pendingStream")
  if state.isNil:
    fut.fail(newException(MsQuicEventQueueClosed, "MsQuic connection state unavailable"))
    return fut
  proc cancellation(udata: pointer) {.gcsafe, raises: [].} =
    if state.isNil:
      return
    state.withStateLock:
      let idx = state.pendingStreamWaiters.find(fut)
      if idx >= 0:
        state.pendingStreamWaiters.delete(idx)
  fut.cancelCallback = cancellation
  var immediate: Option[MsQuicStreamState]
  var shouldFail = false
  state.withStateLock:
    if state.closed:
      shouldFail = true
    elif state.pendingStreamOrder.len > 0:
      while state.pendingStreamOrder.len > 0:
        let streamPtr = state.pendingStreamOrder.popFirst()
        let pending = state.pendingStreams.getOrDefault(streamPtr)
        if pending.isNil:
          continue
        immediate = some(pending)
        state.pendingStreams.del(streamPtr)
        break
    else:
      state.pendingStreamWaiters.add(fut)
  when defined(libp2p_msquic_debug):
    if immediate.isSome:
      warn "MsQuic pending stream waiter immediate",
        stream = cast[uint64](immediate.get().stream),
        connection = cast[uint64](state.connection)
    elif not shouldFail:
      warn "MsQuic pending stream waiter queued",
        waiters = state.pendingStreamWaiters.len,
        connection = cast[uint64](state.connection)
  if shouldFail:
    fut.fail(newException(MsQuicEventQueueClosed, "MsQuic connection state closed"))
  elif immediate.isSome:
    fut.complete(immediate.get())
  fut

proc msquicConnectionEventRelay(event: msevents.ConnectionEvent) {.gcsafe.} =
  if event.userContext.isNil:
    return
  let state = cast[MsQuicConnectionState](event.userContext)
  if state.isNil:
    return
  var dispatchEvent = event
  var shouldDispatch = true
  if event.kind == msevents.cePeerStreamStarted and not event.stream.isNil:
    when defined(ohos):
      warn "MsQuic peer stream started",
        stream = cast[uint64](event.stream),
        flags = event.streamFlags
    when defined(libp2p_msquic_debug):
      warn "MsQuic peer stream started", stream = cast[uint64](event.stream),
        flags = event.streamFlags
    let handle = state.handle
    if not handle.isNil and not handle.bridge.isNil:
      let knownStream = lookupKnownStream(state, event.stream)
      if not knownStream.isNil:
        when defined(libp2p_msquic_debug):
          warn "MsQuic duplicate peer stream start ignored",
            stream = cast[uint64](event.stream),
            connection = cast[uint64](state.connection)
        shouldDispatch = false
      else:
        var streamState: MsQuicStreamState = nil
        let stateRes = block:
          var tmp: Result[MsQuicStreamState, string]
          {.cast(gcsafe).}:
            tmp = newMsQuicStreamState(
              handle,
              state.connection,
              handle.config.eventQueueLimit,
              handle.config.eventPollInterval,
              handler = nil,
              userContext = nil,
              connState = state,
              localInitiated = false
            )
          tmp
        if stateRes.isErr():
          warn "MsQuic peer stream state init failed", err = stateRes.error
          shouldDispatch = false
        else:
          streamState = stateRes.get()
          streamState.stream = cast[msapi.HQUIC](event.stream)
          var adoptStatus = msapi.QUIC_STATUS_INTERNAL_ERROR
          try:
            {.cast(gcsafe).}:
              adoptStatus = msruntime.adoptStream(
                handle.bridge,
                streamState.stream,
                msquicStreamEventRelay,
                cast[pointer](streamState)
              )
          except Exception as exc:
            warn "MsQuic adoptStream raised in peer stream", err = exc.msg
          if adoptStatus != msapi.QUIC_STATUS_SUCCESS:
            warn "MsQuic adoptStream failed in peer stream", status = adoptStatus
            {.cast(gcsafe).}:
              safeCloseStream(handle.bridge, streamState.stream)
            {.cast(gcsafe).}:
              streamState.close()
            shouldDispatch = false
          else:
            storePendingStream(state, event.stream, streamState)
            when defined(ohos):
              warn "MsQuic pending peer stream stored",
                stream = cast[uint64](event.stream),
                connection = cast[uint64](state.connection)
    else:
      shouldDispatch = false
  if event.kind in {msevents.ceShutdownInitiated, msevents.ceShutdownComplete}:
    warn "MsQuic connection shutdown event", kind = event.kind, note = event.note,
      status = event.status, errorCode = event.errorCode
  {.cast(gcsafe).}:
    if shouldDispatch:
      handleIncomingConnectionEvent(state, dispatchEvent)

proc failStreamWaiters(state: MsQuicStreamState) =
  var waiters: seq[Future[msevents.StreamEvent]]
  state.withStateLock:
    if state.waiters.len == 0:
      return
    waiters = state.waiters
    state.waiters.setLen(0)
  if waiters.len == 0:
    return
  for fut in waiters:
    if fut.isNil or fut.finished():
      continue
    let err = newException(MsQuicEventQueueClosed, "MsQuic stream event queue closed")
    fut.fail(err)

proc failStreamReadWaiters(state: MsQuicStreamState) =
  var waiters: seq[Future[seq[byte]]]
  state.withStateLock:
    if state.readWaiters.len == 0:
      return
    waiters = state.readWaiters
    state.readWaiters.setLen(0)
  if waiters.len == 0:
    return
  for fut in waiters:
    if fut.isNil or fut.finished():
      continue
    let err = newException(MsQuicEventQueueClosed, "MsQuic stream read queue closed")
    fut.fail(err)

proc pruneFinishedStreamWaiters(state: MsQuicStreamState) {.raises: [].} =
  if state.isNil:
    return
  state.withStateLock:
    var idx = 0
    while idx < state.waiters.len:
      let fut = state.waiters[idx]
      if fut.isNil or fut.finished():
        state.waiters.delete(idx)
      else:
        inc idx

proc pruneFinishedStreamReadWaiters(state: MsQuicStreamState) {.raises: [].} =
  if state.isNil:
    return
  state.withStateLock:
    var idx = 0
    while idx < state.readWaiters.len:
      let fut = state.readWaiters[idx]
      if fut.isNil or fut.finished():
        state.readWaiters.delete(idx)
      else:
        inc idx

proc completeStreamReceive(state: MsQuicStreamState; receiveLen: int)
    {.raises: [].} =
  if state.isNil or receiveLen <= 0:
    return
  if state.handle.isNil or state.handle.bridge.isNil or state.stream.isNil:
    return
  let api = msruntime.getApiTable(state.handle.bridge)
  if api.isNil or api.StreamReceiveComplete.isNil:
    return
  try:
    api.StreamReceiveComplete(state.stream, uint64(receiveLen))
  except Exception as exc:
    trace "MsQuic StreamReceiveComplete raised", err = exc.msg

proc deliverStreamEvents(state: MsQuicStreamState) =
  while true:
    var fut: Future[msevents.StreamEvent] = nil
    var queuedEvent: MsQuicQueuedStreamEvent
    var hasQueuedEvent = false
    var shouldFail = false
    pruneFinishedStreamWaiters(state)
    state.withStateLock:
      if state.queue.len > 0 and state.waiters.len > 0:
        var nextEvent = state.queue.popFirst()
        queuedEvent = move(nextEvent)
        hasQueuedEvent = true
        fut = state.waiters[0]
        state.waiters.delete(0)
      elif state.closed and state.waiters.len > 0:
        shouldFail = true
      else:
        return
    if not fut.isNil:
      if not fut.finished():
        fut.complete(materializeAndFree(queuedEvent))
      elif hasQueuedEvent:
        release(queuedEvent)
    elif shouldFail:
      failStreamWaiters(state)
      return

proc deliverStreamReads(state: MsQuicStreamState) =
  while true:
    var fut: Future[seq[byte]] = nil
    var chunk: MsQuicReadChunk
    var hasChunk = false
    var shouldFail = false
    var queuedLen = 0
    var waiterLen = 0
    pruneFinishedStreamReadWaiters(state)
    state.withStateLock:
      if state.readQueue.len > 0 and state.readWaiters.len > 0:
        var nextChunk = state.readQueue.popFirst()
        chunk = move(nextChunk)
        hasChunk = true
        fut = state.readWaiters[0]
        state.readWaiters.delete(0)
        queuedLen = state.readQueue.len
        waiterLen = state.readWaiters.len
      elif state.closed and state.readWaiters.len > 0:
        shouldFail = true
      else:
        return
    if hasChunk and not fut.isNil:
      when defined(libp2p_msquic_debug):
        warn "MsQuic stream read waiter fulfilled",
          stream = cast[uint64](state.stream),
          bytes = chunk.payload.len,
          receiveLen = chunk.receiveLen,
          queued = queuedLen,
          waiters = waiterLen
      completeStreamReceive(state, chunk.receiveLen)
      if not fut.finished():
        fut.complete(chunk.payload)
    elif shouldFail:
      failStreamReadWaiters(state)
      return

proc handoffReadWaiters*(
    source: MsQuicStreamState, target: MsQuicStreamState
) {.raises: [].} =
  if source.isNil or target.isNil or source == target:
    return
  var waiters: seq[Future[seq[byte]]]
  source.withStateLock:
    if source.readWaiters.len > 0:
      waiters = source.readWaiters
      source.readWaiters.setLen(0)
  if waiters.len == 0:
    return
  target.withStateLock:
    for fut in waiters:
      if fut.isNil or fut.finished():
        continue
      target.readWaiters.add(fut)
  try:
    triggerStreamDelivery(target)
  except Exception as exc:
    trace "MsQuic triggerStreamDelivery during handoff raised", err = exc.msg

proc streamSignalPump(state: MsQuicStreamState): Future[void] {.async.} =
  try:
    while true:
      if state.isNil or state.closed:
        break
      if state.signal.isNil:
        await sleepAsync(state.pollInterval)
        deliverStreamEvents(state)
        {.cast(gcsafe).}:
          deliverStreamReads(state)
        continue
      try:
        await state.signal.wait()
      except CancelledError:
        break
      except Exception as exc:
        trace "MsQuic stream signal wait failed", err = exc.msg
        break
      deliverStreamEvents(state)
      {.cast(gcsafe).}:
        deliverStreamReads(state)
  finally:
    deliverStreamEvents(state)
    {.cast(gcsafe).}:
      deliverStreamReads(state)
    failStreamWaiters(state)
    failStreamReadWaiters(state)
    closeSignal(state)
    state.signalLoop = nil

proc ensureStreamSignalLoop(state: MsQuicStreamState) =
  if state.isNil or state.closed:
    return
  if state.signalLoop.isNil:
    let fut = streamSignalPump(state)
    state.signalLoop = fut
    asyncSpawn fut

proc triggerStreamDelivery(state: MsQuicStreamState) =
  if state.isNil or state.closed:
    return
  if state.signalLoop.isNil:
    state.ensureStreamSignalLoop()
  if state.signal.isNil:
    deliverStreamEvents(state)
    deliverStreamReads(state)
    return
  if state.signalLoop.isNil:
    deliverStreamEvents(state)
    deliverStreamReads(state)
    return
  let res = state.signal.fireSync()
  if res.isErr():
    trace "MsQuic stream signal fire failed", err = res.error
    deliverStreamEvents(state)
    deliverStreamReads(state)

proc enqueueStreamEvent(state: MsQuicStreamState; event: msevents.StreamEvent) =
  var shouldSignal = false
  var dropped = false
  var droppedEvent: MsQuicQueuedStreamEvent
  var hasDroppedEvent = false
  state.withStateLock:
    if state.closed:
      return
    if state.queueLimit > 0 and state.queue.len >= state.queueLimit:
      var oldest = state.queue.popFirst()
      droppedEvent = move(oldest)
      hasDroppedEvent = true
      inc state.droppedEvents
      dropped = true
    state.queue.addLast(toQueuedEvent(event))
    if state.waiters.len > 0:
      shouldSignal = true
  if dropped:
    trace "MsQuic stream event queue full, dropping oldest event",
      queueLimit = state.queueLimit, droppedEvents = state.droppedEvents
  if hasDroppedEvent:
    release(droppedEvent)
  if shouldSignal:
    triggerStreamDelivery(state)

proc handleIncomingStreamEvent(state: MsQuicStreamState;
    rawEvent: msevents.StreamEvent) =
  if state.isNil:
    return
  state.withStateLock:
    if state.closed:
      return
  var forwarded = rawEvent
  forwarded.userContext = state.userContext
  var completedSend: MsQuicPendingSend = nil
  if forwarded.kind == msevents.seSendComplete and not forwarded.clientContext.isNil:
    completedSend = cast[MsQuicPendingSend](forwarded.clientContext)
    forwarded.clientContext = completedSend.clientContext
  if forwarded.kind == msevents.seStartComplete:
    when defined(ohos):
      warn "MsQuic stream start complete", stream = cast[uint64](state.stream)
    when defined(libp2p_msquic_debug):
      warn "MsQuic stream start complete", stream = cast[uint64](state.stream)
    var startWaiters: seq[Future[void]]
    state.withStateLock:
      state.startComplete = true
      if state.startWaiters.len > 0:
        startWaiters = state.startWaiters
        state.startWaiters.setLen(0)
    if startWaiters.len > 0:
      for fut in startWaiters:
        if fut.isNil or fut.finished():
          continue
        fut.complete()
  if forwarded.kind == msevents.seReceive:
    when defined(ohos):
      warn "MsQuic stream receive event",
        stream = cast[uint64](state.stream),
        buffers = forwarded.bufferCount,
        totalLen = forwarded.totalBufferLength
  if not state.externalHandler.isNil:
    try:
      state.externalHandler(forwarded)
    except Exception as exc:
      trace "MsQuic stream external handler raised", err = exc.msg
  if forwarded.kind == msevents.seReceive:
    var queueEvent = forwarded
    queueEvent.payload = @[]
    queueEvent.totalBufferLength = forwarded.totalBufferLength
    var hasEventWaiters = false
    state.withStateLock:
      hasEventWaiters = state.waiters.len > 0
    if hasEventWaiters:
      enqueueStreamEvent(state, queueEvent)
  else:
    enqueueStreamEvent(state, forwarded)

  if forwarded.kind == msevents.seReceive:
    var payloadData: seq[byte] = @[]
    if forwarded.payload.len > 0:
      payloadData = newSeqUninit[byte](forwarded.payload.len)
      copyMem(addr payloadData[0], unsafeAddr forwarded.payload[0], forwarded.payload.len)
    elif forwarded.totalBufferLength > 0'u64:
      let fallbackLen = int(forwarded.totalBufferLength)
      if fallbackLen > 0:
        payloadData = newSeq[byte](fallbackLen)
    when defined(libp2p_msquic_debug):
      warn "MsQuic stream receive event", stream = cast[uint64](state.stream),
        bytes = payloadData.len, total = forwarded.totalBufferLength,
        buffers = forwarded.bufferCount, flags = forwarded.flags
      if payloadData.len > 0:
        let previewLen = min(payloadData.len, 64)
        let preview = payloadData[0 ..< previewLen]
        warn "MsQuic stream receive payload",
          stream = cast[uint64](state.stream),
          bytes = payloadData.len,
          hex = preview.toHex()
    let receiveLen =
      if payloadData.len > 0:
        payloadData.len
      else:
        int(forwarded.totalBufferLength)
    var shouldNotify = false
    var queuedLen = 0
    var waiterLen = 0
    state.withStateLock:
      state.readQueue.addLast(
        MsQuicReadChunk(payload: move(payloadData), receiveLen: receiveLen)
      )
      shouldNotify = state.readWaiters.len > 0
      queuedLen = state.readQueue.len
      waiterLen = state.readWaiters.len
    when defined(libp2p_msquic_debug):
      warn "MsQuic stream queued read",
        stream = cast[uint64](state.stream),
        bytes = payloadData.len,
        receiveLen = receiveLen,
        queued = queuedLen,
        waiters = waiterLen
    if shouldNotify:
      triggerStreamDelivery(state)
  elif forwarded.kind == msevents.seSendComplete:
    var matchedPending = false
    state.withStateLock:
      if not completedSend.isNil:
        for idx in 0 ..< state.pendingSends.len:
          if state.pendingSends[idx] == completedSend:
            state.pendingSends.delete(idx)
            matchedPending = true
            break
      if not matchedPending:
        completedSend = nil
      if completedSend.isNil and state.pendingSends.len > 0:
        completedSend = state.pendingSends[0]
        state.pendingSends.delete(0)
    if completedSend.isNil or completedSend.waiter.isNil:
      return
    var waiterFinished = false
    try:
      waiterFinished = completedSend.waiter.finished()
    except CatchableError:
      return
    except Exception:
      return
    if not waiterFinished:
      if forwarded.cancelled or forwarded.status != msapi.QUIC_STATUS_SUCCESS:
        completedSend.waiter.fail(
          newException(
            CatchableError,
            fmt"MsQuic send completion failed: status=0x{forwarded.status:08x} cancelled={forwarded.cancelled}"
          )
        )
      else:
        completedSend.waiter.complete()
  elif forwarded.kind == msevents.sePeerSendShutdown:
    var shouldNotify = false
    state.withStateLock:
      if not state.peerSendShutdown:
        state.peerSendShutdown = true
        state.readQueue.addLast(MsQuicReadChunk(payload: @[], receiveLen: 0))
        shouldNotify = state.readWaiters.len > 0
    if shouldNotify:
      triggerStreamDelivery(state)

proc close(state: MsQuicStreamState) {.raises: [].} =
  if state.isNil:
    return
  let handle = state.handle
  let connectionState = state.connectionState
  let streamHandle = state.stream
  var hadWaiters = false
  var startWaiters: seq[Future[void]]
  var shouldRetain = false
  state.withStateLock:
    if state.closed:
      return
    state.closed = true
    if not state.retainedForLateCallbacks:
      state.retainedForLateCallbacks = true
      shouldRetain = true
    hadWaiters = state.waiters.len > 0
    if state.startWaiters.len > 0:
      startWaiters = state.startWaiters
      state.startWaiters.setLen(0)
    while state.queue.len > 0:
      var queuedEvent = state.queue.popFirst()
      release(queuedEvent)
  if not handle.isNil:
    handle.unregisterActiveState(state)
  if not connectionState.isNil and not streamHandle.isNil:
    unregisterKnownStream(connectionState, cast[pointer](streamHandle))
  if shouldRetain and not handle.isNil:
    handle.retainForLateCallbacks(state)
  if hadWaiters:
    triggerStreamDelivery(state)
  if startWaiters.len > 0:
    for fut in startWaiters:
      if fut.isNil or fut.finished():
        continue
      fut.complete()
  deliverStreamEvents(state)
  deliverStreamReads(state)
  failStreamWaiters(state)
  failStreamReadWaiters(state)
  failPendingSendWaiters(state, "MsQuic stream state closed")
  let loopActive = (not state.signalLoop.isNil) and (not state.signalLoop.finished())
  if loopActive:
    state.signalLoop.cancel()
  closeSignal(state)
  # Late runtime callbacks can still observe the connection state after close.
  # Do not deinitialize the lock here; let process teardown reclaim it.

proc markStreamStartComplete*(state: MsQuicStreamState) {.raises: [].} =
  if state.isNil:
    return
  var startWaiters: seq[Future[void]]
  state.withStateLock:
    if state.startComplete or state.closed:
      return
    state.startComplete = true
    if state.startWaiters.len > 0:
      startWaiters = state.startWaiters
      state.startWaiters.setLen(0)
  for fut in startWaiters:
    if fut.isNil or fut.finished():
      continue
    fut.complete()

proc failPendingSendWaiters(state: MsQuicStreamState; msg: string) {.gcsafe, raises: [].} =
  if state.isNil:
    return
  var waiters: seq[Future[void]] = @[]
  state.withStateLock:
    for pending in state.pendingSends:
      if pending.isNil or pending.waiter.isNil or pending.waiter.finished():
        continue
      waiters.add(pending.waiter)
    state.pendingSends.setLen(0)
  for fut in waiters:
    fut.fail(newException(CatchableError, msg))

proc newMsQuicStreamState(handle: MsQuicTransportHandle; connection: msapi.HQUIC;
    queueLimit: int; pollInterval: Duration;
    handler: MsQuicStreamHandler; userContext: pointer;
    connState: MsQuicConnectionState; localInitiated: bool
  ): Result[MsQuicStreamState, string] =
  var state = MsQuicStreamState(
    handle: handle,
    connection: connection,
    stream: nil,
    queueLimit: queueLimit,
    pollInterval: pollInterval,
    queue: initDeque[MsQuicQueuedStreamEvent](),
    waiters: @[],
    signal: nil,
    signalLoop: nil,
    closed: false,
    retainedForLateCallbacks: false,
    connectionState: connState,
    userContext: userContext,
    externalHandler: handler,
    droppedEvents: 0,
    readQueue: initDeque[MsQuicReadChunk](),
    readWaiters: @[],
    pendingSends: @[],
    localInitiated: localInitiated,
    startComplete: not localInitiated,
    startWaiters: @[],
    sendShutdownRequested: false,
    peerSendShutdown: false,
    streamIdKnown: false,
    streamIdCached: 0'u64
  )
  initLock(state.lock)
  state.lockInit = true
  let signalRes = ThreadSignalPtr.new()
  if signalRes.isErr():
    close(state)
    return err("failed to initialize MsQuic stream signal: " & signalRes.error)
  state.signal = signalRes.get()
  state.ensureStreamSignalLoop()
  handle.registerActiveState(state)
  ok(state)

proc nextStreamEvent*(state: MsQuicStreamState): Future[msevents.StreamEvent] =
  if state.isNil:
    let fut = Future[msevents.StreamEvent].Raising(
      [CancelledError, MsQuicEventQueueClosed]
    ).init("msquic.streamEvent.nil")
    fut.fail(newException(MsQuicEventQueueClosed, "MsQuic stream state unavailable"))
    return fut

  let fut = Future[msevents.StreamEvent].Raising(
    [CancelledError, MsQuicEventQueueClosed]
  ).init("msquic.streamEvent.next")
  proc cancellation(udata: pointer) {.gcsafe, raises: [].} =
    if state.isNil:
      return
    state.withStateLock:
      let idx = state.waiters.find(fut)
      if idx >= 0:
        state.waiters.delete(idx)
  fut.cancelCallback = cancellation

  var queuedImmediate: MsQuicQueuedStreamEvent
  var hasImmediate = false
  var shouldFail = false
  pruneFinishedStreamWaiters(state)
  state.withStateLock:
    if state.closed and state.queue.len == 0:
      shouldFail = true
    elif state.queue.len > 0:
      var nextEvent = state.queue.popFirst()
      queuedImmediate = move(nextEvent)
      hasImmediate = true
    else:
      state.waiters.add(fut)

  if shouldFail:
    fut.fail(newException(MsQuicEventQueueClosed, "MsQuic stream state closed"))
  elif hasImmediate:
    fut.complete(materializeAndFree(queuedImmediate))
  else:
    state.ensureStreamSignalLoop()
  fut

proc msquicStreamEventRelay(event: msevents.StreamEvent) {.gcsafe.} =
  if event.userContext.isNil:
    return
  let state = cast[MsQuicStreamState](event.userContext)
  if state.isNil:
    return
  {.cast(gcsafe).}:
    handleIncomingStreamEvent(state, event)

proc failListenerWaiters(state: MsQuicListenerState) =
  var waiters: seq[Future[msevents.ListenerEvent]]
  state.withStateLock:
    if state.waiters.len == 0:
      return
    waiters = state.waiters
    state.waiters.setLen(0)
  if waiters.len == 0:
    return
  for fut in waiters:
    if fut.isNil or fut.finished():
      continue
    let err = newException(MsQuicEventQueueClosed, "MsQuic listener event queue closed")
    fut.fail(err)

proc deliverListenerEvents(state: MsQuicListenerState) =
  while true:
    var fut: Future[msevents.ListenerEvent] = nil
    var queuedEvent: MsQuicQueuedListenerEvent
    var hasQueuedEvent = false
    var shouldFail = false
    state.withStateLock:
      if state.queue.len > 0 and state.waiters.len > 0:
        var nextEvent = state.queue.popFirst()
        queuedEvent = move(nextEvent)
        hasQueuedEvent = true
        fut = state.waiters[0]
        state.waiters.delete(0)
      elif state.closed and state.waiters.len > 0:
        shouldFail = true
      else:
        return
    if not fut.isNil:
      if not fut.finished():
        fut.complete(materializeAndFree(queuedEvent))
      elif hasQueuedEvent:
        release(queuedEvent)
    elif shouldFail:
      failListenerWaiters(state)
      return

proc listenerSignalPump(state: MsQuicListenerState): Future[void] {.async.} =
  try:
    while true:
      if state.isNil or state.closed:
        break
      if state.signal.isNil:
        await sleepAsync(state.pollInterval)
        continue
      try:
        await state.signal.wait()
      except CancelledError:
        break
      except Exception as exc:
        trace "MsQuic listener signal wait failed", err = exc.msg
        break
      deliverListenerEvents(state)
  finally:
    failListenerWaiters(state)
    closeSignal(state)
    state.signalLoop = nil

proc ensureListenerSignalLoop(state: MsQuicListenerState) =
  if state.signalLoop.isNil:
    let fut = listenerSignalPump(state)
    state.signalLoop = fut
    asyncSpawn fut

proc triggerListenerDelivery(state: MsQuicListenerState) =
  state.ensureListenerSignalLoop()
  if state.signal.isNil:
    deliverListenerEvents(state)
    return
  let res = state.signal.fireSync()
  if res.isErr():
    trace "MsQuic listener signal fire failed", err = res.error
    deliverListenerEvents(state)

proc enqueueListenerEvent(state: MsQuicListenerState; event: msevents.ListenerEvent) =
  var shouldSignal = false
  var dropped = false
  var droppedEvent: MsQuicQueuedListenerEvent
  var hasDroppedEvent = false
  state.withStateLock:
    if state.closed:
      return
    if state.queueLimit > 0 and state.queue.len >= state.queueLimit:
      var oldest = state.queue.popFirst()
      droppedEvent = move(oldest)
      hasDroppedEvent = true
      inc state.droppedEvents
      dropped = true
    state.queue.addLast(toQueuedEvent(event))
    if state.waiters.len > 0:
      shouldSignal = true
  if dropped:
    trace "MsQuic listener event queue full, dropping oldest event",
      queueLimit = state.queueLimit, droppedEvents = state.droppedEvents
  if hasDroppedEvent:
    release(droppedEvent)
  if shouldSignal:
    triggerListenerDelivery(state)

proc handleIncomingListenerEvent(state: MsQuicListenerState;
    rawEvent: msevents.ListenerEvent) =
  if state.isNil:
    return
  var forwarded = rawEvent
  forwarded.userContext = state.userContext
  if not state.externalHandler.isNil:
    try:
      state.externalHandler(forwarded)
    except Exception as exc:
      trace "MsQuic listener external handler raised", err = exc.msg
  enqueueListenerEvent(state, forwarded)

proc storePendingConnection(state: MsQuicListenerState; connection: pointer;
    connState: MsQuicConnectionState) =
  if state.isNil or connection.isNil or connState.isNil:
    return
  state.withStateLock:
    state.pendingConnections[connection] = connState

proc takePendingConnection*(state: MsQuicListenerState; connection: pointer):
    Option[MsQuicConnectionState] =
  if state.isNil or connection.isNil:
    return none(MsQuicConnectionState)
  var result: Option[MsQuicConnectionState]
  state.withStateLock:
    let pending = state.pendingConnections.getOrDefault(connection)
    if not pending.isNil:
      result = some(pending)
      state.pendingConnections.del(connection)
  result

proc close(state: MsQuicListenerState) =
  if state.isNil:
    return
  let handle = state.handle
  var hadWaiters = false
  var pending: seq[MsQuicConnectionState]
  var shouldRetain = false
  state.withStateLock:
    if state.closed:
      return
    state.closed = true
    if not state.retainedForLateCallbacks:
      state.retainedForLateCallbacks = true
      shouldRetain = true
    hadWaiters = state.waiters.len > 0
    if state.pendingConnections.len > 0:
      pending = toSeq(state.pendingConnections.values)
      state.pendingConnections.clear()
    while state.queue.len > 0:
      var queuedEvent = state.queue.popFirst()
      release(queuedEvent)
  if not handle.isNil:
    handle.unregisterActiveState(state)
  if shouldRetain and not handle.isNil:
    handle.retainForLateCallbacks(state)
  if hadWaiters:
    triggerListenerDelivery(state)
  deliverListenerEvents(state)
  failListenerWaiters(state)
  for connState in pending:
    if connState.isNil:
      continue
    if not connState.connection.isNil and not state.handle.isNil:
      safeCloseConnection(state.handle.bridge, connState.connection)
    connState.close()
  let loopActive = (not state.signalLoop.isNil) and (not state.signalLoop.finished())
  if loopActive:
    state.signalLoop.cancel()
  closeSignal(state)
  # Listener callbacks can outlive logical close; keep the lock valid until the
  # ref object is reclaimed instead of destroying it eagerly.

proc newMsQuicListenerState(handle: MsQuicTransportHandle; queueLimit: int;
    pollInterval: Duration; handler: MsQuicListenerHandler; userContext: pointer
  ): Result[MsQuicListenerState, string] =
  var state = MsQuicListenerState(
    handle: handle,
    listener: nil,
    queueLimit: queueLimit,
    pollInterval: pollInterval,
    queue: initDeque[MsQuicQueuedListenerEvent](),
    waiters: @[],
    signal: nil,
    signalLoop: nil,
    closed: false,
    retainedForLateCallbacks: false,
    userContext: userContext,
    externalHandler: handler,
    droppedEvents: 0,
    pendingConnections: initTable[pointer, MsQuicConnectionState]()
  )
  initLock(state.lock)
  state.lockInit = true
  let signalRes = ThreadSignalPtr.new()
  if signalRes.isErr():
    close(state)
    return err("failed to initialize MsQuic listener signal: " & signalRes.error)
  state.signal = signalRes.get()
  state.ensureListenerSignalLoop()
  handle.registerActiveState(state)
  ok(state)

proc nextListenerEvent*(state: MsQuicListenerState): Future[msevents.ListenerEvent] =
  if state.isNil:
    let fut = Future[msevents.ListenerEvent].Raising(
      [CancelledError, MsQuicEventQueueClosed]
    ).init("msquic.listenerEvent.nil")
    fut.fail(newException(MsQuicEventQueueClosed, "MsQuic listener state unavailable"))
    return fut

  let fut = Future[msevents.ListenerEvent].Raising(
    [CancelledError, MsQuicEventQueueClosed]
  ).init("msquic.listenerEvent.next")

  var queuedImmediate: MsQuicQueuedListenerEvent
  var hasImmediate = false
  var shouldFail = false
  state.withStateLock:
    if state.closed and state.queue.len == 0:
      shouldFail = true
    elif state.queue.len > 0:
      var nextEvent = state.queue.popFirst()
      queuedImmediate = move(nextEvent)
      hasImmediate = true
    else:
      state.waiters.add(fut)

  if shouldFail:
    fut.fail(newException(MsQuicEventQueueClosed, "MsQuic listener state closed"))
  elif hasImmediate:
    fut.complete(materializeAndFree(queuedImmediate))
  else:
    state.ensureListenerSignalLoop()
  fut

proc msquicListenerEventRelay(event: msevents.ListenerEvent) {.gcsafe.} =
  if event.userContext.isNil:
    return
  let state = cast[MsQuicListenerState](event.userContext)
  if state.isNil:
    return
  var dispatchEvent = event
  var shouldDispatch = true
  if event.kind == msevents.leNewConnection and not event.connection.isNil:
    let handle = state.handle
    if not handle.isNil and not handle.bridge.isNil:
      var connState: MsQuicConnectionState = nil
      let stateRes = block:
        var tmp: Result[MsQuicConnectionState, string]
        {.cast(gcsafe).}:
          tmp = newMsQuicConnectionState(
            handle,
            handle.config.eventQueueLimit,
            handle.config.eventPollInterval,
            handler = nil,
            userContext = nil
          )
        tmp
      if stateRes.isErr():
        warn "MsQuic incoming connection state init failed", err = stateRes.error
      else:
        connState = stateRes.get()
      if not connState.isNil:
        var adoptStatus = msapi.QUIC_STATUS_INTERNAL_ERROR
        try:
          {.cast(gcsafe).}:
            adoptStatus = msruntime.adoptConnection(
              handle.bridge,
              cast[msapi.HQUIC](event.connection),
              msquicConnectionEventRelay,
              cast[pointer](connState)
            )
        except Exception as exc:
          warn "MsQuic adoptConnection raised in listener", err = exc.msg
        if adoptStatus != msapi.QUIC_STATUS_SUCCESS:
          warn "MsQuic adoptConnection failed in listener", status = adoptStatus
          {.cast(gcsafe).}:
            safeCloseConnection(handle.bridge, cast[msapi.HQUIC](event.connection))
          {.cast(gcsafe).}:
            connState.close()
          shouldDispatch = false
        else:
          let api = msruntime.getApiTable(handle.bridge)
          if api.isNil or api.ConnectionSetConfiguration.isNil:
            warn "MsQuic API missing ConnectionSetConfiguration"
            {.cast(gcsafe).}:
              safeCloseConnection(handle.bridge, cast[msapi.HQUIC](event.connection))
            {.cast(gcsafe).}:
              connState.close()
          else:
            var cfgStatus: msapi.QUIC_STATUS
            try:
              {.cast(gcsafe).}:
                cfgStatus = api.ConnectionSetConfiguration(
                  cast[msapi.HQUIC](event.connection),
                  handle.configuration
                )
            except Exception as exc:
              warn "MsQuic ConnectionSetConfiguration raised", err = exc.msg
              {.cast(gcsafe).}:
                safeCloseConnection(handle.bridge, cast[msapi.HQUIC](event.connection))
              {.cast(gcsafe).}:
                connState.close()
              cfgStatus = msapi.QUIC_STATUS_INTERNAL_ERROR
            if cfgStatus == msapi.QUIC_STATUS_SUCCESS or
                cfgStatus == msapi.QUIC_STATUS_INVALID_PARAMETER or
                cfgStatus == QuicStatusPending:
              connState.connection = cast[msapi.HQUIC](event.connection)
              storePendingConnection(state, event.connection, connState)
              dispatchEvent.note = MsQuicListenerPreconfiguredNote
            else:
              warn "MsQuic ConnectionSetConfiguration failed in listener",
                status = cfgStatus
              {.cast(gcsafe).}:
                safeCloseConnection(handle.bridge, cast[msapi.HQUIC](event.connection))
              {.cast(gcsafe).}:
                connState.close()
              shouldDispatch = false
      else:
        shouldDispatch = false
    else:
      shouldDispatch = false
  {.cast(gcsafe).}:
    if shouldDispatch:
      handleIncomingListenerEvent(state, dispatchEvent)

proc pushStreamReceive*(state: MsQuicStreamState; payload: seq[byte]) {.raises: [].} =
  if state.isNil or state.closed:
    return
  var event = msevents.StreamEvent(
    stream: cast[pointer](state.stream),
    kind: msevents.seReceive,
    absoluteOffset: 0,
    totalBufferLength: uint64(payload.len),
    bufferCount: if payload.len > 0: 1'u32 else: 0'u32,
    flags: 0,
    userContext: state.userContext,
    note: "synthetic receive"
  )
  var shouldNotify = false
  state.withStateLock:
    state.readQueue.addLast(MsQuicReadChunk(payload: payload, receiveLen: 0))
    shouldNotify = state.readWaiters.len > 0
  enqueueStreamEvent(state, event)
  if shouldNotify:
    triggerStreamDelivery(state)

proc waitStreamStart*(state: MsQuicStreamState): Future[void].Raising([CancelledError]) =
  let fut = Future[void].Raising([CancelledError]).init("msquic.stream.start")
  if state.isNil:
    fut.complete()
    return fut
  proc cancellation(udata: pointer) {.gcsafe, raises: [].} =
    if state.isNil:
      return
    state.withStateLock:
      let idx = state.startWaiters.find(fut)
      if idx >= 0:
        state.startWaiters.delete(idx)
  fut.cancelCallback = cancellation
  var immediate = false
  state.withStateLock:
    if state.startComplete or not state.localInitiated or state.closed:
      immediate = true
    else:
      state.startWaiters.add(fut)
  if immediate:
    fut.complete()
  fut

proc readStream*(state: MsQuicStreamState): Future[seq[byte]] {.gcsafe.} =
  if state.isNil:
    let fut = Future[seq[byte]].init("msquic.stream.read")
    fut.fail(newException(MsQuicEventQueueClosed, "MsQuic stream closed"))
    return fut
  let fut = Future[seq[byte]].init("msquic.stream.read")
  proc cancellation(udata: pointer) {.gcsafe, raises: [].} =
    if state.isNil:
      return
    state.withStateLock:
      let idx = state.readWaiters.find(fut)
      if idx >= 0:
        state.readWaiters.delete(idx)
  fut.cancelCallback = cancellation
  var immediateChunk: MsQuicReadChunk
  var hasImmediate = false
  var shouldFail = false
  var shouldArmSignalLoop = false
  var queuedLen = 0
  var waiterLen = 0
  pruneFinishedStreamReadWaiters(state)
  state.withStateLock:
    if state.readQueue.len > 0:
      var nextChunk = state.readQueue.popFirst()
      immediateChunk = move(nextChunk)
      hasImmediate = true
      queuedLen = state.readQueue.len
      waiterLen = state.readWaiters.len
    elif state.closed:
      shouldFail = true
    else:
      state.readWaiters.add(fut)
      shouldArmSignalLoop = true
      queuedLen = state.readQueue.len
      waiterLen = state.readWaiters.len
  if hasImmediate:
    let chunk = move(immediateChunk)
    when defined(ohos):
      warn "MsQuic stream read immediate",
        stream = cast[uint64](state.stream),
        bytes = chunk.payload.len,
        receiveLen = chunk.receiveLen,
        queued = queuedLen,
        waiters = waiterLen
    when defined(libp2p_msquic_debug):
      warn "MsQuic stream read immediate",
        stream = cast[uint64](state.stream),
        bytes = chunk.payload.len,
        receiveLen = chunk.receiveLen,
        queued = queuedLen,
        waiters = waiterLen
    {.cast(gcsafe).}:
      completeStreamReceive(state, chunk.receiveLen)
    fut.complete(chunk.payload)
  elif shouldFail:
    fut.fail(newException(MsQuicEventQueueClosed, "MsQuic stream closed"))
  elif defined(ohos):
    warn "MsQuic stream read waiting",
      stream = cast[uint64](state.stream),
      queued = queuedLen,
      waiters = waiterLen
  elif defined(libp2p_msquic_debug):
    warn "MsQuic stream read waiting",
      stream = cast[uint64](state.stream),
      queued = queuedLen,
      waiters = waiterLen
  if shouldArmSignalLoop:
    state.ensureStreamSignalLoop()
  fut

proc enqueueStreamWrite(
    state: MsQuicStreamState;
    data: seq[byte];
    flags: uint32 = 0'u32;
    clientContext: pointer = nil
): tuple[error: string, waiter: Future[void]] {.gcsafe, raises: [].} =
  if state.isNil or state.closed:
    return ("MsQuic stream closed", nil)
  let handle = state.handle
  if handle.isNil or handle.bridge.isNil:
    return ("MsQuic transport handle unavailable", nil)

  var pending = MsQuicPendingSend(
    payload: data,
    clientContext: clientContext,
    waiter: Future[void].init("msquic.stream.sendComplete")
  )
  var bufferPtr: ptr msapi.QuicBuffer = nil
  var bufferCount: uint32 = 0
  let sendContext = cast[pointer](pending)

  state.withStateLock:
    state.pendingSends.add(pending)
    if pending.payload.len > 0:
      pending.buffer = msapi.QuicBuffer(
        Length: uint32(pending.payload.len),
        Buffer: cast[ptr uint8](addr pending.payload[0])
      )
      bufferPtr = addr pending.buffer
      bufferCount = 1

  let status =
    try:
      {.cast(gcsafe).}:
        msruntime.sendStream(
          handle.bridge,
          state.stream,
          bufferPtr,
          bufferCount,
          msapi.QUIC_SEND_FLAGS(flags),
          sendContext
        )
    except CatchableError as exc:
      state.withStateLock:
        for idx in 0 ..< state.pendingSends.len:
          if state.pendingSends[idx] == pending:
            state.pendingSends.delete(idx)
            break
      if not pending.waiter.isNil and not pending.waiter.finished():
        pending.waiter.fail(
          newException(CatchableError, "MsQuic StreamSend raised: " & exc.msg)
        )
      return ("MsQuic StreamSend raised: " & exc.msg, nil)
    except Exception as exc:
      state.withStateLock:
        for idx in 0 ..< state.pendingSends.len:
          if state.pendingSends[idx] == pending:
            state.pendingSends.delete(idx)
            break
      if not pending.waiter.isNil and not pending.waiter.finished():
        pending.waiter.fail(
          newException(CatchableError, "MsQuic StreamSend raised: " & exc.msg)
        )
      return ("MsQuic StreamSend raised: " & exc.msg, nil)
  when defined(ohos):
    warn "MsQuic StreamSend status", stream = cast[uint64](state.stream),
      status = status,
      bytes = data.len,
      flags = flags
  when defined(libp2p_msquic_debug):
    warn "MsQuic StreamSend status", stream = cast[uint64](state.stream),
      status = status

  if status != msapi.QUIC_STATUS_SUCCESS and status != QuicStatusPending:
    state.withStateLock:
      for idx in 0 ..< state.pendingSends.len:
        if state.pendingSends[idx] == pending:
          state.pendingSends.delete(idx)
          break
    if not pending.waiter.isNil and not pending.waiter.finished():
      pending.waiter.fail(
        newException(CatchableError, fmt"MsQuic StreamSend failed: 0x{status:08x}")
      )
    return (fmt"MsQuic StreamSend failed: 0x{status:08x}", nil)
  ("", pending.waiter)

proc writeStream*(state: MsQuicStreamState; data: seq[byte];
    flags: uint32 = 0'u32; clientContext: pointer = nil): string {.gcsafe, raises: [].} =
  enqueueStreamWrite(state, data, flags, clientContext).error

proc writeStreamAndWait*(state: MsQuicStreamState; data: seq[byte];
    flags: uint32 = 0'u32; clientContext: pointer = nil): Future[string]
    {.async: (raises: [CancelledError]).} =
  if state.isNil or state.closed:
    return "MsQuic stream closed"
  let (err, waiter) =
    try:
      enqueueStreamWrite(state, data, flags, clientContext)
    except CatchableError as exc:
      ("MsQuic StreamSend raised: " & exc.msg, nil)
    except Exception as exc:
      ("MsQuic StreamSend raised: " & exc.msg, nil)
  if err.len > 0:
    return err
  if waiter.isNil:
    return ""
  try:
    await waiter
  except CatchableError as exc:
    return exc.msg
  ""

proc streamId*(state: MsQuicStreamState): Result[uint64, string] {.gcsafe, raises: [].} =
  if state.isNil or state.handle.isNil or state.handle.bridge.isNil or state.stream.isNil:
    return err("MsQuic stream state unavailable")
  if state.streamIdKnown:
    return ok(state.streamIdCached)
  var idRes: Result[uint64, string]
  {.cast(gcsafe).}:
    idRes = state.handle.bridge.streamId(state.stream)
  if idRes.isOk():
    state.streamIdCached = idRes.get()
    state.streamIdKnown = true
  idRes

proc releaseRegistration(handle: MsQuicTransportHandle) {.gcsafe, raises: [].} =
  if handle.bridge.isNil:
    return
  if not handle.clientConfiguration.isNil:
    safeCloseConfiguration(handle.bridge, handle.clientConfiguration)
    handle.clientConfiguration = nil
  if not handle.configuration.isNil:
    safeCloseConfiguration(handle.bridge, handle.configuration)
    handle.configuration = nil
  if not handle.registration.isNil:
    safeCloseRegistration(handle.bridge, handle.registration)
    handle.registration = nil

proc shutdown*(handle: MsQuicTransportHandle) {.raises: [].} =
  if handle.isNil or handle.closed:
    return
  handle.closed = true
  var activeConnections: seq[MsQuicConnectionState] = @[]
  var activeListeners: seq[MsQuicListenerState] = @[]
  var activeStreams: seq[MsQuicStreamState] = @[]
  var retainedConnections: seq[MsQuicConnectionState] = @[]
  var retainedListeners: seq[MsQuicListenerState] = @[]
  var retainedStreams: seq[MsQuicStreamState] = @[]
  handle.withRetainLock:
    activeConnections = handle.activeConnectionStates
    activeListeners = handle.activeListenerStates
    activeStreams = handle.activeStreamStates
    retainedConnections = handle.retainedConnectionStates
    retainedListeners = handle.retainedListenerStates
    retainedStreams = handle.retainedStreamStates
  for state in activeStreams:
    if state.isNil:
      continue
    if not state.stream.isNil and not handle.bridge.isNil:
      safeCloseStream(handle.bridge, state.stream)
    state.close()
  for state in activeConnections:
    if state.isNil:
      continue
    if not state.connection.isNil and not handle.bridge.isNil:
      safeShutdownConnection(handle.bridge, state.connection)
      safeCloseConnection(handle.bridge, state.connection)
    state.close()
  for state in activeListeners:
    if state.isNil:
      continue
    if not state.listener.isNil and not handle.bridge.isNil:
      safeCloseListener(handle.bridge, state.listener)
    state.close()
  for state in retainedStreams:
    if not state.isNil:
      state.close()
  for state in retainedConnections:
    if not state.isNil:
      state.close()
  for state in retainedListeners:
    if not state.isNil:
      state.close()
  if not handle.tlsBinding.isNil:
    handle.tlsBinding.cleanup()
    handle.tlsBinding = nil
  if not handle.clientTlsBinding.isNil:
    handle.clientTlsBinding.cleanup()
    handle.clientTlsBinding = nil
  releaseRegistration(handle)
  if not handle.bridge.isNil:
    releaseMsQuicBridge(handle.bridge)
    handle.bridge = nil
  handle.withRetainLock:
    handle.activeConnectionStates.setLen(0)
    handle.activeListenerStates.setLen(0)
    handle.activeStreamStates.setLen(0)
    handle.retainedConnectionStates.setLen(0)
    handle.retainedListenerStates.setLen(0)
    handle.retainedStreamStates.setLen(0)

proc buildMsQuicSettings(cfg: MsQuicTransportConfig): MsQuicSettings =
  var settings: MsQuicSettings
  if cfg.handshakeIdleTimeoutMs > 0'u64:
    settings.isSetFlags = settings.isSetFlags or QuicSettingHandshakeIdleTimeoutFlag
    settings.handshakeIdleTimeoutMs = cfg.handshakeIdleTimeoutMs
  if cfg.idleTimeoutMs > 0'u64:
    settings.isSetFlags = settings.isSetFlags or QuicSettingIdleTimeoutFlag
    settings.idleTimeoutMs = cfg.idleTimeoutMs
  if cfg.keepAliveIntervalMs > 0'u32:
    settings.isSetFlags = settings.isSetFlags or QuicSettingKeepAliveIntervalFlag
    settings.keepAliveIntervalMs = cfg.keepAliveIntervalMs
  let bidi =
    if cfg.peerBidiStreamCount > 0'u16:
      cfg.peerBidiStreamCount
    else:
      DefaultPeerBidiStreamCount
  let unidi =
    if cfg.peerUnidiStreamCount > 0'u16:
      cfg.peerUnidiStreamCount
    else:
      DefaultPeerUnidiStreamCount
  settings.isSetFlags =
    settings.isSetFlags or
    QuicSettingPeerBidiStreamFlag or
    QuicSettingPeerUnidiStreamFlag
  settings.peerBidiStreamCount = bidi
  settings.peerUnidiStreamCount = unidi
  settings

proc buildSettingsOverlay(cfg: MsQuicTransportConfig): mssettings.QuicSettingsOverlay =
  result = mssettings.defaultQuicSettingsOverlay()
  if cfg.handshakeIdleTimeoutMs > 0'u64:
    result.handshakeIdleTimeoutMs = cfg.handshakeIdleTimeoutMs
  if cfg.idleTimeoutMs > 0'u64:
    result.idleTimeoutMs = cfg.idleTimeoutMs
  if cfg.keepAliveIntervalMs > 0'u32:
    result.keepAliveIntervalMs = cfg.keepAliveIntervalMs
  result.peerBidiStreamCount =
    if cfg.peerBidiStreamCount > 0'u16:
      cfg.peerBidiStreamCount
    else:
      DefaultPeerBidiStreamCount
  result.peerUnidiStreamCount =
    if cfg.peerUnidiStreamCount > 0'u16:
      cfg.peerUnidiStreamCount
    else:
      DefaultPeerUnidiStreamCount

proc initMsQuicTransport*(cfg: MsQuicTransportConfig = MsQuicTransportConfig()):
    tuple[handle: MsQuicTransportHandle, error: string] {.raises: [].} =
  let bridgeRes = acquireMsQuicBridge(cfg.loadOptions)
  if not bridgeRes.success:
    return (nil, bridgeRes.error)
  let bridge = bridgeRes.bridge

  var registration: msapi.HQUIC = nil
  var configuration: msapi.HQUIC = nil
  var clientConfiguration: msapi.HQUIC = nil
  var alpnBuffers: seq[msapi.QuicBuffer]
  var settings = buildMsQuicSettings(cfg)
  let overlay = buildSettingsOverlay(cfg)

  let alpns = resolvedAlpns(cfg)
  alpnBuffers = buildAlpnBuffers(alpns)

  var regConfig = msapi.QuicRegistrationConfigC(
    AppName: (if cfg.appName.len > 0: cfg.appName.cstring else: DefaultAppName.cstring),
    ExecutionProfile: msapi.QUIC_EXECUTION_PROFILE(cfg.executionProfile)
  )

  try:
    let regStatus = msruntime.openRegistration(bridge, addr regConfig, registration)
    if regStatus != msapi.QUIC_STATUS_SUCCESS or registration.isNil:
      releaseMsQuicBridge(bridge)
      return (nil, fmt"MsQuic registration failed: 0x{regStatus:08x}")
  except Exception as exc:
    releaseMsQuicBridge(bridge)
    return (nil, "MsQuic registration raised: " & exc.msg)

  let alpnPtr = if alpnBuffers.len == 0: nil else: addr alpnBuffers[0]
  try:
    let cfgStatus = msruntime.openConfiguration(
      bridge,
      registration,
      alpnPtr,
      uint32(alpnBuffers.len),
      addr settings,
      sizeof(settings).uint32,
      nil,
      configuration
    )
    if cfgStatus != msapi.QUIC_STATUS_SUCCESS or configuration.isNil:
      safeCloseRegistration(bridge, registration)
      releaseMsQuicBridge(bridge)
      return (nil, fmt"MsQuic configuration failed: 0x{cfgStatus:08x}")
    let overlayStatus = msapi.applyConfigurationSettingsOverlay(configuration, overlay)
    if overlayStatus != msapi.QUIC_STATUS_SUCCESS:
      safeCloseConfiguration(bridge, configuration)
      safeCloseRegistration(bridge, registration)
      releaseMsQuicBridge(bridge)
      return (nil, fmt"MsQuic configuration overlay failed: 0x{overlayStatus:08x}")
  except Exception as exc:
    safeCloseRegistration(bridge, registration)
    releaseMsQuicBridge(bridge)
    return (nil, "MsQuic configuration raised: " & exc.msg)

  try:
    let cfgStatus = msruntime.openConfiguration(
      bridge,
      registration,
      alpnPtr,
      uint32(alpnBuffers.len),
      addr settings,
      sizeof(settings).uint32,
      nil,
      clientConfiguration
    )
    if cfgStatus != msapi.QUIC_STATUS_SUCCESS or clientConfiguration.isNil:
      safeCloseConfiguration(bridge, configuration)
      safeCloseRegistration(bridge, registration)
      releaseMsQuicBridge(bridge)
      return (nil, fmt"MsQuic client configuration failed: 0x{cfgStatus:08x}")
    let overlayStatus = msapi.applyConfigurationSettingsOverlay(clientConfiguration, overlay)
    if overlayStatus != msapi.QUIC_STATUS_SUCCESS:
      safeCloseConfiguration(bridge, clientConfiguration)
      safeCloseConfiguration(bridge, configuration)
      safeCloseRegistration(bridge, registration)
      releaseMsQuicBridge(bridge)
      return (nil, fmt"MsQuic client configuration overlay failed: 0x{overlayStatus:08x}")
  except Exception as exc:
    safeCloseConfiguration(bridge, configuration)
    safeCloseRegistration(bridge, registration)
    releaseMsQuicBridge(bridge)
    return (nil, "MsQuic client configuration raised: " & exc.msg)

  let apiTable = msruntime.getApiTable(bridge)
  if apiTable.isNil or apiTable.ConfigurationLoadCredential.isNil:
    safeCloseConfiguration(bridge, configuration)
    safeCloseRegistration(bridge, registration)
    releaseMsQuicBridge(bridge)
    return (nil, "MsQuic API missing ConfigurationLoadCredential")

  let handle = MsQuicTransportHandle(
    bridge: bridge,
    registration: registration,
    configuration: configuration,
    clientConfiguration: clientConfiguration,
    alpns: alpns,
    alpnBuffers: alpnBuffers,
    config: cfg,
    tlsBinding: nil,
    clientTlsBinding: nil,
    retainLockInit: false,
    activeConnectionStates: @[],
    activeListenerStates: @[],
    activeStreamStates: @[],
    retainedConnectionStates: @[],
    retainedListenerStates: @[],
    retainedStreamStates: @[],
    closed: false
  )
  initLock(handle.retainLock)
  handle.retainLockInit = true
  (handle, "")

proc ensureClientCredentialLoaded(handle: MsQuicTransportHandle): string {.raises: [].} =
  if handle.isNil or handle.bridge.isNil or handle.clientConfiguration.isNil or handle.closed:
    return "MsQuic client configuration unavailable"
  if not handle.clientTlsBinding.isNil:
    return ""

  let clientCfg = mstlstypes.TlsConfig(
    role: mstlstypes.tlsClient,
    alpns: resolvedAlpns(handle.config),
    transportParameters: @[],
    serverName: none(string),
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
    caCertificateFile: none(string),
    resumptionTicket: none(seq[uint8]),
    enableZeroRtt: false,
    useSharedSessionCache: true,
    disableCertificateValidation: false,
    requireClientAuth: false,
    enableOcsp: false,
    indicateCertificateReceived: true,
    deferCertificateValidation: false,
    useBuiltinCertificateValidation: false,
    allowedCipherSuites: none(uint32),
    tempDirectory: none(string)
  )

  let clientBinding =
    try:
      mstls.newTlsCredentialBinding(clientCfg)
    except CatchableError as exc:
      return "MsQuic client credential binding failed: " & exc.msg
    except Exception as exc:
      return "MsQuic client credential binding raised: " & exc.msg

  let clientCredPtr = mstls.credentialConfigPtr(clientBinding)
  if clientCredPtr.isNil:
    clientBinding.cleanup()
    return "MsQuic client credential config unavailable"

  let apiTable = msruntime.getApiTable(handle.bridge)
  if apiTable.isNil or apiTable.ConfigurationLoadCredential.isNil:
    clientBinding.cleanup()
    return "MsQuic API missing ConfigurationLoadCredential"

  let clientStatus =
    try:
      apiTable.ConfigurationLoadCredential(
        handle.clientConfiguration,
        cast[pointer](clientCredPtr)
      )
    except Exception as exc:
      clientBinding.cleanup()
      return "MsQuic client credential load raised: " & exc.msg
  if clientStatus != msapi.QUIC_STATUS_SUCCESS:
    clientBinding.cleanup()
    return fmt"MsQuic client credential load failed: 0x{clientStatus:08x}"

  handle.clientTlsBinding = clientBinding
  ""

proc loadCredential*(handle: MsQuicTransportHandle; cfg: MsTlsConfig;
    tempDir: string = ""): string {.raises: [].} =
  if handle.isNil or handle.bridge.isNil or handle.configuration.isNil or handle.closed:
    return "MsQuic transport handle unavailable"
  if handle.clientConfiguration.isNil:
    return "MsQuic client configuration unavailable"
  var effectiveTempDir = tempDir
  when compiles(cfg.tempDirectory):
    if cfg.tempDirectory.isSome and cfg.tempDirectory.get().len > 0:
      effectiveTempDir = cfg.tempDirectory.get()
  let binding =
    try:
      mstls.newTlsCredentialBinding(cfg, tempDir = effectiveTempDir)
    except CatchableError as exc:
      return "MsQuic credential binding failed: " & exc.msg
    except Exception as exc:
      return "MsQuic credential binding raised: " & exc.msg
  let credPtr = mstls.credentialConfigPtr(binding)
  if credPtr.isNil:
    binding.cleanup()
    return "MsQuic credential config unavailable"
  let apiTable = msruntime.getApiTable(handle.bridge)
  if apiTable.isNil or apiTable.ConfigurationLoadCredential.isNil:
    binding.cleanup()
    return "MsQuic API missing ConfigurationLoadCredential"
  let status =
    try:
      apiTable.ConfigurationLoadCredential(
        handle.configuration,
        cast[pointer](credPtr)
      )
    except Exception as exc:
      binding.cleanup()
      return "MsQuic credential load raised: " & exc.msg
  if status != msapi.QUIC_STATUS_SUCCESS:
    binding.cleanup()
    return fmt"MsQuic credential load failed: 0x{status:08x}"

  var clientCfg = cfg
  clientCfg.role = mstlstypes.tlsClient
  clientCfg.requireClientAuth = false
  clientCfg.disableCertificateValidation = false
  clientCfg.indicateCertificateReceived = true

  let clientBinding =
    try:
      mstls.newTlsCredentialBinding(clientCfg, tempDir = effectiveTempDir)
    except CatchableError as exc:
      binding.cleanup()
      return "MsQuic client credential binding failed: " & exc.msg
    except Exception as exc:
      binding.cleanup()
      return "MsQuic client credential binding raised: " & exc.msg

  let clientCredPtr = mstls.credentialConfigPtr(clientBinding)
  if clientCredPtr.isNil:
    binding.cleanup()
    clientBinding.cleanup()
    return "MsQuic client credential config unavailable"

  let clientStatus =
    try:
      apiTable.ConfigurationLoadCredential(
        handle.clientConfiguration,
        cast[pointer](clientCredPtr)
      )
    except Exception as exc:
      binding.cleanup()
      clientBinding.cleanup()
      return "MsQuic client credential load raised: " & exc.msg
  if clientStatus != msapi.QUIC_STATUS_SUCCESS:
    binding.cleanup()
    clientBinding.cleanup()
    return fmt"MsQuic client credential load failed: 0x{clientStatus:08x}"

  if not handle.tlsBinding.isNil:
    handle.tlsBinding.cleanup()
  if not handle.clientTlsBinding.isNil:
    handle.clientTlsBinding.cleanup()
  handle.tlsBinding = binding
  handle.clientTlsBinding = clientBinding
  ""

proc adoptConnection*(handle: MsQuicTransportHandle; connection: pointer;
    handler: MsQuicConnectionHandler; userContext: pointer = nil): string {.raises: [].} =
  if handle.isNil or handle.bridge.isNil:
    return "MsQuic transport handle unavailable"
  if connection.isNil:
    return "MsQuic adoptConnection received nil connection"
  let status =
    try:
      msruntime.adoptConnection(
        handle.bridge,
        cast[msapi.HQUIC](connection),
        handler,
        userContext
      )
    except Exception as exc:
      return "MsQuic adoptConnection raised: " & exc.msg
  if status != msapi.QUIC_STATUS_SUCCESS:
    return fmt"MsQuic adoptConnection failed: 0x{status:08x}"
  ""

proc createListener*(handle: MsQuicTransportHandle;
    handler: MsQuicListenerHandler = nil; userContext: pointer = nil;
    address: pointer = nil; queueLimit: int = 0;
    pollInterval: Duration = DefaultEventPollInterval):
    tuple[listener: pointer, state: Option[MsQuicListenerState], error: string] {.raises: [].} =
  if handle.isNil or handle.bridge.isNil or handle.closed:
    return (nil, none(MsQuicListenerState), "MsQuic transport handle unavailable")

  let effectiveQueueLimit =
    if queueLimit > 0: queueLimit else: handle.config.eventQueueLimit
  let effectivePoll =
    if pollInterval > 0.nanoseconds: pollInterval else: handle.config.eventPollInterval

  let stateRes = newMsQuicListenerState(
    handle,
    effectiveQueueLimit,
    effectivePoll,
    handler,
    userContext
  )
  if stateRes.isErr():
    return (nil, none(MsQuicListenerState), stateRes.error)
  var state = stateRes.get()

  var listenerHandle: msapi.HQUIC = nil
  let status =
    try:
      msruntime.openListener(
        handle.bridge,
        handle.registration,
        msquicListenerEventRelay,
        listenerHandle,
        cast[pointer](state)
      )
    except Exception as exc:
      safeCloseListener(handle.bridge, listenerHandle)
      state.close()
      return (nil, none(MsQuicListenerState), "MsQuic ListenerOpen raised: " & exc.msg)
  if status != msapi.QUIC_STATUS_SUCCESS or listenerHandle.isNil:
    safeCloseListener(handle.bridge, listenerHandle)
    state.close()
    return (nil, none(MsQuicListenerState),
            fmt"MsQuic ListenerOpen failed: 0x{status:08x}")

  state.listener = listenerHandle
  state.userContext = userContext

  if not address.isNil:
    let startErr = handle.startListener(cast[pointer](listenerHandle), address = address)
    if startErr.len > 0:
      safeCloseListener(handle.bridge, listenerHandle)
      state.listener = nil
      state.close()
      return (nil, none(MsQuicListenerState), startErr)

  (cast[pointer](listenerHandle), some(state), "")

proc attachIncomingConnection*(handle: MsQuicTransportHandle; connection: pointer;
    handler: MsQuicConnectionHandler = nil; userContext: pointer = nil;
    queueLimit: int = 0; pollInterval: Duration = DefaultEventPollInterval):
    tuple[state: Option[MsQuicConnectionState], error: string] {.raises: [].} =
  if handle.isNil or handle.bridge.isNil or handle.closed:
    return (none(MsQuicConnectionState), "MsQuic transport handle unavailable")
  if connection.isNil:
    return (none(MsQuicConnectionState), "MsQuic attach connection requires non-nil handle")

  let effectiveQueueLimit =
    if queueLimit > 0: queueLimit else: handle.config.eventQueueLimit
  let effectivePoll =
    if pollInterval > 0.nanoseconds: pollInterval else: handle.config.eventPollInterval

  let stateRes = newMsQuicConnectionState(
    handle,
    effectiveQueueLimit,
    effectivePoll,
    handler = handler,
    userContext = userContext
  )
  if stateRes.isErr():
    return (none(MsQuicConnectionState), stateRes.error)
  var state = stateRes.get()

  let adoptErr = block:
    var msg = ""
    try:
      msg = handle.adoptConnection(connection, msquicConnectionEventRelay, cast[pointer](state))
    except Exception as exc:
      msg = "MsQuic adoptConnection raised: " & exc.msg
    msg
  if adoptErr.len > 0:
    safeCloseConnection(handle.bridge, cast[msapi.HQUIC](connection))
    state.close()
    return (none(MsQuicConnectionState), adoptErr)

  let api = msruntime.getApiTable(handle.bridge)
  if api.isNil or api.ConnectionSetConfiguration.isNil:
    safeCloseConnection(handle.bridge, cast[msapi.HQUIC](connection))
    state.close()
    return (none(MsQuicConnectionState), "MsQuic API missing ConnectionSetConfiguration")

  let hconn = cast[msapi.HQUIC](connection)
  var cfgStatus: msapi.QUIC_STATUS
  try:
    cfgStatus = api.ConnectionSetConfiguration(
      hconn,
      handle.configuration
    )
  except Exception as exc:
    safeCloseConnection(handle.bridge, hconn)
    state.close()
    return (none(MsQuicConnectionState), "MsQuic ConnectionSetConfiguration raised: " & exc.msg)
  if cfgStatus != msapi.QUIC_STATUS_SUCCESS:
    if cfgStatus == msapi.QUIC_STATUS_INVALID_PARAMETER or
        cfgStatus == QuicStatusPending:
      trace "MsQuic ConnectionSetConfiguration already applied", status = cfgStatus
    else:
      safeCloseConnection(handle.bridge, hconn)
      state.close()
      return (none(MsQuicConnectionState),
              fmt"MsQuic ConnectionSetConfiguration failed: 0x{cfgStatus:08x}")

  state.connection = hconn
  (some(state), "")

proc attachIncomingConnectionAdopted*(handle: MsQuicTransportHandle; connection: pointer;
    handler: MsQuicConnectionHandler = nil; userContext: pointer = nil;
    queueLimit: int = 0; pollInterval: Duration = DefaultEventPollInterval):
    tuple[state: Option[MsQuicConnectionState], error: string] {.raises: [].} =
  if handle.isNil or handle.bridge.isNil or handle.closed:
    return (none(MsQuicConnectionState), "MsQuic transport handle unavailable")
  if connection.isNil:
    return (none(MsQuicConnectionState), "MsQuic attach connection requires non-nil handle")

  let effectiveQueueLimit =
    if queueLimit > 0: queueLimit else: handle.config.eventQueueLimit
  let effectivePoll =
    if pollInterval > 0.nanoseconds: pollInterval else: handle.config.eventPollInterval

  let stateRes = newMsQuicConnectionState(
    handle,
    effectiveQueueLimit,
    effectivePoll,
    handler = handler,
    userContext = userContext
  )
  if stateRes.isErr():
    return (none(MsQuicConnectionState), stateRes.error)
  var state = stateRes.get()

  let adoptErr = block:
    var msg = ""
    try:
      msg = handle.adoptConnection(connection, msquicConnectionEventRelay, cast[pointer](state))
    except Exception as exc:
      msg = "MsQuic adoptConnection raised: " & exc.msg
    msg
  if adoptErr.len > 0:
    safeCloseConnection(handle.bridge, cast[msapi.HQUIC](connection))
    state.close()
    return (none(MsQuicConnectionState), adoptErr)

  state.connection = cast[msapi.HQUIC](connection)
  (some(state), "")

proc dialConnection*(handle: MsQuicTransportHandle; serverName: string; port: uint16;
    handler: MsQuicConnectionHandler = nil; userContext: pointer = nil;
    addressFamily: uint16 = 0; queueLimit: int = 0;
    pollInterval: Duration = DefaultEventPollInterval;
    transportHost: string = ""):
    tuple[connection: pointer, state: Option[MsQuicConnectionState], error: string] {.raises: [].} =
  if handle.isNil or handle.bridge.isNil or handle.closed:
    return (nil, none(MsQuicConnectionState), "MsQuic transport handle unavailable")
  if serverName.len == 0:
    return (nil, none(MsQuicConnectionState), "MsQuic dial requires non-empty serverName")
  let dialHost =
    if transportHost.len > 0: transportHost
    else: serverName
  if dialHost.len == 0:
    return (nil, none(MsQuicConnectionState), "MsQuic dial requires non-empty transport host")

  let effectiveQueueLimit =
    if queueLimit > 0: queueLimit else: handle.config.eventQueueLimit
  let effectivePoll =
    if pollInterval > 0.nanoseconds: pollInterval else: handle.config.eventPollInterval

  let api = msruntime.getApiTable(handle.bridge)
  if api.isNil:
    return (nil, none(MsQuicConnectionState), "MsQuic API table unavailable")
  if api.ConnectionSetConfiguration.isNil or api.ConnectionStart.isNil:
    return (nil, none(MsQuicConnectionState), "MsQuic API missing connection functions")

  let stateRes = newMsQuicConnectionState(
    handle,
    effectiveQueueLimit,
    effectivePoll,
    handler,
    userContext
  )
  if stateRes.isErr():
    return (nil, none(MsQuicConnectionState), stateRes.error)
  var state = stateRes.get()

  var connection: msapi.HQUIC = nil
  trace "MsQuic dial ConnectionOpen begin", server = serverName, port = port, family = addressFamily
  try:
    let connStatus = msruntime.openConnection(
      handle.bridge,
      handle.registration,
      msquicConnectionEventRelay,
      connection,
      cast[pointer](state)
    )
    trace "MsQuic dial ConnectionOpen status", status = connStatus
    if connStatus != msapi.QUIC_STATUS_SUCCESS or connection.isNil:
      safeCloseConnection(handle.bridge, connection)
      state.close()
      return (nil, none(MsQuicConnectionState), fmt"MsQuic ConnectionOpen failed: 0x{connStatus:08x}")
  except Exception as exc:
    safeCloseConnection(handle.bridge, connection)
    state.close()
    return (nil, none(MsQuicConnectionState), "MsQuic ConnectionOpen raised: " & exc.msg)

  state.connection = connection

  let dialConfig =
    if handle.clientConfiguration.isNil: handle.configuration
    else: handle.clientConfiguration
  if dialConfig == handle.clientConfiguration:
    let clientCredErr = ensureClientCredentialLoaded(handle)
    if clientCredErr.len > 0:
      safeCloseConnection(handle.bridge, connection)
      state.close()
      return (nil, none(MsQuicConnectionState), clientCredErr)

  let bindHost = handle.config.clientBindHost.strip()
  if bindHost.len > 0 and bindHost != "0.0.0.0" and bindHost != "::":
    var bindAddr = TransportAddress(family: AddressFamily.None)
    try:
      bindAddr = initTAddress(bindHost, Port(handle.config.clientBindPort))
    except CatchableError:
      bindAddr = TransportAddress(family: AddressFamily.None)
    if bindAddr.family == AddressFamily.None:
      safeShutdownConnection(handle.bridge, connection, 0'u32, 0'u64)
      safeCloseConnection(handle.bridge, connection)
      state.close()
      return (nil, none(MsQuicConnectionState), "MsQuic client bind host invalid: " & bindHost)
    let bindErr = setConnectionLocalAddress(handle, cast[pointer](connection), bindAddr)
    if bindErr.len > 0:
      safeShutdownConnection(handle.bridge, connection, 0'u32, 0'u64)
      safeCloseConnection(handle.bridge, connection)
      state.close()
      return (nil, none(MsQuicConnectionState), bindErr)
    trace "MsQuic dial local bind applied", bindHost = bindHost, bindPort = handle.config.clientBindPort

  var remoteAddr = TransportAddress(family: AddressFamily.None)
  try:
    remoteAddr = initTAddress(dialHost, Port(port))
  except CatchableError:
    remoteAddr = TransportAddress(family: AddressFamily.None)
  if remoteAddr.family == AddressFamily.None:
    safeShutdownConnection(handle.bridge, connection, 0'u32, 0'u64)
    safeCloseConnection(handle.bridge, connection)
    state.close()
    return (nil, none(MsQuicConnectionState), "MsQuic dial transport host invalid: " & dialHost)
  let remoteErr =
    setConnectionRemoteAddress(handle, cast[pointer](connection), remoteAddr)
  if remoteErr.len > 0:
    safeShutdownConnection(handle.bridge, connection, 0'u32, 0'u64)
    safeCloseConnection(handle.bridge, connection)
    state.close()
    return (nil, none(MsQuicConnectionState), remoteErr)
  trace "MsQuic dial remote target applied",
    server = serverName,
    transportHost = dialHost,
    port = port

  let family = msapi.QUIC_ADDRESS_FAMILY(addressFamily)
  try:
    trace "MsQuic dial ConnectionStart begin", server = serverName, port = port, family = addressFamily
    let statusStart = api.ConnectionStart(
      connection,
      dialConfig,
      family,
      serverName.cstring,
      port
    )
    trace "MsQuic dial ConnectionStart status", status = statusStart
    if statusStart != msapi.QUIC_STATUS_SUCCESS and statusStart != QuicStatusPending:
      safeShutdownConnection(handle.bridge, connection, 0'u32, 0'u64)
      safeCloseConnection(handle.bridge, connection)
      state.close()
      return (nil, none(MsQuicConnectionState),
              fmt"MsQuic ConnectionStart failed: 0x{statusStart:08x}")
  except Exception as exc:
    safeShutdownConnection(handle.bridge, connection, 0'u32, 0'u64)
    safeCloseConnection(handle.bridge, connection)
    state.close()
    return (nil, none(MsQuicConnectionState), "MsQuic ConnectionStart raised: " & exc.msg)

  (cast[pointer](connection), some(state), "")

proc createStream*(handle: MsQuicTransportHandle; connection: pointer;
    handler: MsQuicStreamHandler = nil;
    flags: uint32 = 0'u32; userContext: pointer = nil;
    connectionState: MsQuicConnectionState = nil;
    queueLimit: int = 0; pollInterval: Duration = DefaultEventPollInterval):
    tuple[stream: pointer, state: Option[MsQuicStreamState], error: string] {.raises: [].} =
  if handle.isNil or handle.bridge.isNil or handle.closed:
    return (nil, none(MsQuicStreamState), "MsQuic transport handle unavailable")
  if connection.isNil:
    return (nil, none(MsQuicStreamState), "MsQuic stream requires non-nil connection handle")

  let connHandle = cast[msapi.HQUIC](connection)
  let effectiveQueueLimit =
    if queueLimit > 0: queueLimit else: handle.config.eventQueueLimit
  let effectivePoll =
    if pollInterval > 0.nanoseconds: pollInterval else: handle.config.eventPollInterval

  let stateRes = newMsQuicStreamState(
    handle,
    connHandle,
    effectiveQueueLimit,
    effectivePoll,
    handler,
    userContext,
    connectionState,
    localInitiated = true
  )
  if stateRes.isErr():
    return (nil, none(MsQuicStreamState), stateRes.error)
  var state = stateRes.get()

  var streamHandle: msapi.HQUIC = nil
  try:
    let status = msruntime.openStream(
      handle.bridge,
      connHandle,
      msapi.QUIC_STREAM_OPEN_FLAGS(flags),
      msquicStreamEventRelay,
      streamHandle,
      cast[pointer](state)
    )
    if status != msapi.QUIC_STATUS_SUCCESS or streamHandle.isNil:
      state.close()
      return (nil, none(MsQuicStreamState),
              fmt"MsQuic StreamOpen failed: 0x{status:08x}")
  except Exception as exc:
    state.close()
    return (nil, none(MsQuicStreamState), "MsQuic StreamOpen raised: " & exc.msg)

  state.stream = streamHandle
  state.connectionState = connectionState
  state.ensureStreamSignalLoop()
  (cast[pointer](streamHandle), some(state), "")

proc adoptStream*(handle: MsQuicTransportHandle; stream: pointer;
    connectionState: MsQuicConnectionState;
    handler: MsQuicStreamHandler = nil; userContext: pointer = nil;
    queueLimit: int = 0; pollInterval: Duration = DefaultEventPollInterval):
    tuple[state: Option[MsQuicStreamState], error: string] {.raises: [].} =
  if handle.isNil or handle.bridge.isNil or handle.closed:
    return (none(MsQuicStreamState), "MsQuic transport handle unavailable")
  if stream.isNil:
    return (none(MsQuicStreamState), "MsQuic adoptStream requires non-nil stream handle")
  if connectionState.isNil or connectionState.connection.isNil:
    return (none(MsQuicStreamState), "MsQuic adoptStream requires active connection state")

  let pending = takePendingStream(connectionState, stream)
  if pending.isSome:
    let streamState = pending.get()
    if not handler.isNil:
      streamState.externalHandler = handler
    if not userContext.isNil:
      streamState.userContext = userContext
    streamState.ensureStreamSignalLoop()
    return (some(streamState), "")

  let effectiveQueueLimit =
    if queueLimit > 0: queueLimit else: handle.config.eventQueueLimit
  let effectivePoll =
    if pollInterval > 0.nanoseconds: pollInterval else: handle.config.eventPollInterval

  let stateRes = newMsQuicStreamState(
    handle,
    connectionState.connection,
    effectiveQueueLimit,
    effectivePoll,
    handler,
    userContext,
    connectionState,
    localInitiated = false
  )
  if stateRes.isErr():
    return (none(MsQuicStreamState), stateRes.error)
  var state = stateRes.get()

  let adoptStatus =
    try:
      msruntime.adoptStream(
        handle.bridge,
        cast[msapi.HQUIC](stream),
        msquicStreamEventRelay,
        cast[pointer](state)
      )
    except Exception as exc:
      state.close()
      return (none(MsQuicStreamState), "MsQuic StreamAdopt raised: " & exc.msg)

  if adoptStatus != msapi.QUIC_STATUS_SUCCESS:
    state.close()
    return (
      none(MsQuicStreamState),
      fmt"MsQuic StreamAdopt failed: 0x{adoptStatus:08x}"
    )

  state.stream = cast[msapi.HQUIC](stream)
  state.connectionState = connectionState
  state.ensureStreamSignalLoop()
  (some(state), "")

proc sendDatagram*(handle: MsQuicTransportHandle; connection: pointer;
    payload: openArray[byte]; flags: uint32 = 0'u32;
    clientContext: pointer = nil): string =
  if handle.isNil or handle.bridge.isNil or connection.isNil:
    return "MsQuic transport handle unavailable"
  var storage: seq[uint8] = @[]
  var buffer = msapi.QuicBuffer(Length: 0'u32, Buffer: nil)
  if payload.len > 0:
    storage = @payload
    buffer.Length = uint32(storage.len)
    buffer.Buffer = cast[ptr uint8](addr storage[0])
  var bufferPtr: ptr msapi.QuicBuffer = if buffer.Length == 0: nil else: addr buffer
  let status =
    try:
      msruntime.sendDatagram(
        handle.bridge,
        cast[msapi.HQUIC](connection),
        bufferPtr,
        if bufferPtr.isNil: 0'u32 else: 1'u32,
        msapi.QUIC_SEND_FLAGS(flags),
        clientContext
      )
    except Exception as exc:
      return "MsQuic DatagramSend raised: " & exc.msg
  if status != msapi.QUIC_STATUS_SUCCESS:
    return fmt"MsQuic DatagramSend failed: 0x{status:08x}"
  ""

proc startListener*(handle: MsQuicTransportHandle; listener: pointer;
    alpns: ptr msapi.QuicBuffer = nil; alpnCount: uint32 = 0;
    address: pointer = nil): string {.raises: [].} =
  if handle.isNil or handle.bridge.isNil or listener.isNil:
    return "MsQuic transport handle unavailable"
  let overlayStatus =
    try:
      msapi.applyListenerSettingsOverlay(
        cast[msapi.HQUIC](listener),
        buildSettingsOverlay(handle.config)
      )
    except Exception as exc:
      return "MsQuic listener overlay raised: " & exc.msg
  if overlayStatus != msapi.QUIC_STATUS_SUCCESS:
    return fmt"MsQuic listener overlay failed: 0x{overlayStatus:08x}"
  let status =
    try:
      msruntime.startListener(
        handle.bridge,
        cast[msapi.HQUIC](listener),
        alpns,
        alpnCount,
        address
      )
    except Exception as exc:
      return "MsQuic ListenerStart raised: " & exc.msg
  if status != msapi.QUIC_STATUS_SUCCESS:
    return fmt"MsQuic ListenerStart failed: 0x{status:08x}"
  ""

proc stopListener*(handle: MsQuicTransportHandle; listener: pointer): string {.raises: [].} =
  if handle.isNil or handle.bridge.isNil or listener.isNil:
    return "MsQuic transport handle unavailable"
  let status =
    try:
      msruntime.stopListener(handle.bridge, cast[msapi.HQUIC](listener))
    except Exception as exc:
      return "MsQuic ListenerStop raised: " & exc.msg
  if status != msapi.QUIC_STATUS_SUCCESS:
    return fmt"MsQuic ListenerStop failed: 0x{status:08x}"
  ""

proc getListenerAddress*(handle: MsQuicTransportHandle; listener: pointer):
    Result[TransportAddress, string] {.gcsafe, raises: [].} =
  if handle.isNil or handle.bridge.isNil or listener.isNil:
    return err("MsQuic transport handle unavailable")

  var addrStorage: SockAddr_storage
  var addrLen = uint32(sizeof(SockAddr_storage))

  let status =
    try:
      msruntime.getListenerParam(
        handle.bridge,
        cast[msapi.HQUIC](listener),
        msparams.QUIC_PARAM_LISTENER_LOCAL_ADDRESS,
        addr addrStorage,
        addrLen
      )
    except Exception as exc:
      return err("MsQuic ListenerGetParam raised: " & exc.msg)

  if status != msapi.QUIC_STATUS_SUCCESS:
    trace "MsQuic ListenerGetParam failed", status = status
    return err(fmt"MsQuic ListenerGetParam failed: 0x{status:08x}")
  
  trace "MsQuic ListenerGetParam ok", addrLen = addrLen
  if addrLen == 0:
    return err("MsQuic returned empty address length")

  try:
    let sa = cast[ptr SockAddr](addr addrStorage)
    let family = cint(sa.sa_family)
    if family == posix.AF_INET:
      let sin = cast[ptr SockAddr_in](addr addrStorage)
      var hostBuf: array[InetAddrStrLen, char]
      if inet_ntop(posix.AF_INET, addr sin.sin_addr, cast[cstring](addr hostBuf[0]),
                   int32(hostBuf.len)) == nil:
        return err("inet_ntop failed for IPv4")
      let host = $cast[cstring](addr hostBuf[0])
      let port = ntohs(sin.sin_port)
      return ok(initTAddress(host, Port(int(port))))
    if family == posix.AF_INET6:
      let sin6 = cast[ptr SockAddr_in6](addr addrStorage)
      var hostBuf: array[Inet6AddrStrLen, char]
      if inet_ntop(posix.AF_INET6, addr sin6.sin6_addr, cast[cstring](addr hostBuf[0]),
                   int32(hostBuf.len)) == nil:
        return err("inet_ntop failed for IPv6")
      let host = $cast[cstring](addr hostBuf[0])
      let port = ntohs(sin6.sin6_port)
      return ok(initTAddress(host, Port(int(port))))
    err("Unsupported address family: " & $family)
  except Exception as exc:
    err("Failed to convert SockAddr: " & exc.msg)

proc getConnectionAddress(
    handle: MsQuicTransportHandle; connection: pointer; paramId: uint32
): Result[TransportAddress, string] {.gcsafe, raises: [].} =
  if handle.isNil or handle.bridge.isNil or connection.isNil:
    return err("MsQuic transport handle unavailable")

  var addrStorage: SockAddr_storage
  var addrLen = uint32(sizeof(SockAddr_storage))

  let status =
    try:
      msruntime.getConnectionParam(
        handle.bridge,
        cast[msapi.HQUIC](connection),
        paramId,
        addr addrStorage,
        addrLen
      )
    except Exception as exc:
      return err("MsQuic ConnectionGetParam raised: " & exc.msg)

  if status != msapi.QUIC_STATUS_SUCCESS:
    trace "MsQuic ConnectionGetParam failed", status = status
    return err(fmt"MsQuic ConnectionGetParam failed: 0x{status:08x}")

  if addrLen == 0:
    return err("MsQuic returned empty address length")

  try:
    let sa = cast[ptr SockAddr](addr addrStorage)
    let family = cint(sa.sa_family)
    if family == posix.AF_INET:
      let sin = cast[ptr SockAddr_in](addr addrStorage)
      var hostBuf: array[InetAddrStrLen, char]
      if inet_ntop(posix.AF_INET, addr sin.sin_addr, cast[cstring](addr hostBuf[0]),
                   int32(hostBuf.len)) == nil:
        return err("inet_ntop failed for IPv4")
      let host = $cast[cstring](addr hostBuf[0])
      if host == "0.0.0.0":
        return err("MsQuic returned unspecified IPv4 address")
      let port = ntohs(sin.sin_port)
      return ok(initTAddress(host, Port(int(port))))
    if family == posix.AF_INET6:
      let sin6 = cast[ptr SockAddr_in6](addr addrStorage)
      var hostBuf: array[Inet6AddrStrLen, char]
      if inet_ntop(posix.AF_INET6, addr sin6.sin6_addr, cast[cstring](addr hostBuf[0]),
                   int32(hostBuf.len)) == nil:
        return err("inet_ntop failed for IPv6")
      let host = $cast[cstring](addr hostBuf[0])
      if host == "::":
        return err("MsQuic returned unspecified IPv6 address")
      let port = ntohs(sin6.sin6_port)
      return ok(initTAddress(host, Port(int(port))))
    err("Unsupported address family: " & $family)
  except Exception as exc:
    err("Failed to convert SockAddr: " & exc.msg)

proc getConnectionRemoteAddress*(handle: MsQuicTransportHandle; connection: pointer):
    Result[TransportAddress, string] {.gcsafe, raises: [].} =
  getConnectionAddress(handle, connection, msparams.QUIC_PARAM_CONN_REMOTE_ADDRESS)

proc getConnectionLocalAddress*(handle: MsQuicTransportHandle; connection: pointer):
    Result[TransportAddress, string] {.gcsafe, raises: [].} =
  getConnectionAddress(handle, connection, msparams.QUIC_PARAM_CONN_LOCAL_ADDRESS)

proc connectionHandshakeComplete*(
    state: MsQuicConnectionState
): bool {.gcsafe, raises: [].} =
  if state.isNil or state.handle.isNil or state.handle.bridge.isNil or state.connection.isNil:
    return false
  {.cast(gcsafe).}:
    result = msruntime.connectionHandshakeComplete(state.handle.bridge, state.connection)

proc connectionCloseReason*(
    state: MsQuicConnectionState
): string {.gcsafe, raises: [].} =
  if state.isNil or state.handle.isNil or state.handle.bridge.isNil or state.connection.isNil:
    return ""
  {.cast(gcsafe).}:
    result = msruntime.connectionCloseReason(state.handle.bridge, state.connection)

proc builtinPathApiAvailable(
    handle: MsQuicTransportHandle
): bool {.inline, gcsafe, raises: [].} =
  not handle.isNil and
    not handle.bridge.isNil and
    msruntime.getRuntime(handle.bridge).path == "builtin-nim-quic-native"

proc activeConnectionPathId*(
    state: MsQuicConnectionState
): Result[uint8, string] {.gcsafe, raises: [].} =
  if state.isNil or state.handle.isNil or state.connection.isNil:
    return err("MsQuic connection state unavailable")
  if not builtinPathApiAvailable(state.handle):
    return err("MsQuic builtin path migration APIs unavailable")
  var pathId = high(uint8)
  let ok = block:
    var success = false
    {.cast(gcsafe).}:
      success = msapi.getConnectionActivePathId(state.connection, pathId)
    success
  if not ok:
    return err("MsQuic getConnectionActivePathId failed")
  ok(pathId)

proc knownConnectionPathCount*(
    state: MsQuicConnectionState
): Result[uint8, string] {.gcsafe, raises: [].} =
  if state.isNil or state.handle.isNil or state.connection.isNil:
    return err("MsQuic connection state unavailable")
  if not builtinPathApiAvailable(state.handle):
    return err("MsQuic builtin path migration APIs unavailable")
  var count = 0'u8
  let ok = block:
    var success = false
    {.cast(gcsafe).}:
      success = msapi.getConnectionKnownPathCount(state.connection, count)
    success
  if not ok:
    return err("MsQuic getConnectionKnownPathCount failed")
  ok(count)

proc connectionPathState*(
    state: MsQuicConnectionState,
    pathId: uint8
): Result[MsQuicConnectionPathState, string] {.gcsafe, raises: [].} =
  if state.isNil or state.handle.isNil or state.connection.isNil:
    return err("MsQuic connection state unavailable")
  if not builtinPathApiAvailable(state.handle):
    return err("MsQuic builtin path migration APIs unavailable")
  var active = false
  var validated = false
  var challengeOutstanding = false
  var responsePending = false
  let ok = block:
    var success = false
    {.cast(gcsafe).}:
      success = msapi.getConnectionPathState(
        state.connection,
        pathId,
        active,
        validated,
        challengeOutstanding,
        responsePending
      )
    success
  if not ok:
    return err("MsQuic getConnectionPathState failed for path " & $pathId)
  ok(MsQuicConnectionPathState(
    pathId: pathId,
    active: active,
    validated: validated,
    challengeOutstanding: challengeOutstanding,
    responsePending: responsePending
  ))

proc triggerConnectionMigrationProbe*(
    state: MsQuicConnectionState,
    host: string,
    port: uint16
): Result[uint8, string] {.gcsafe, raises: [].} =
  if state.isNil or state.handle.isNil or state.connection.isNil:
    return err("MsQuic connection state unavailable")
  if host.strip().len == 0:
    return err("MsQuic path migration probe requires non-empty host")
  if not builtinPathApiAvailable(state.handle):
    return err("MsQuic builtin path migration APIs unavailable")
  var pathId = high(uint8)
  let probeRes = block:
    var success = false
    var failure = ""
    try:
      {.cast(gcsafe).}:
        success = msapi.triggerConnectionMigrationProbe(
          state.connection,
          host.cstring,
          port,
          pathId
        )
    except Exception as exc:
      failure = exc.msg
    (success, failure)
  if not probeRes[0]:
    return err(
      if probeRes[1].len > 0:
        "MsQuic triggerConnectionMigrationProbe raised for " &
          host & ":" & $port & ": " & probeRes[1]
      else:
        "MsQuic triggerConnectionMigrationProbe failed for " &
          host & ":" & $port
    )
  ok(pathId)

proc confirmConnectionValidatedPath*(
    state: MsQuicConnectionState,
    pathId: uint8
): Result[void, string] {.gcsafe, raises: [].} =
  if state.isNil or state.handle.isNil or state.connection.isNil:
    return err("MsQuic connection state unavailable")
  if not builtinPathApiAvailable(state.handle):
    return err("MsQuic builtin path migration APIs unavailable")
  let confirmRes = block:
    var success = false
    var failure = ""
    try:
      {.cast(gcsafe).}:
        success = msapi.confirmConnectionValidatedPath(state.connection, pathId)
    except Exception as exc:
      failure = exc.msg
    (success, failure)
  if not confirmRes[0]:
    return err("MsQuic confirmConnectionValidatedPath failed for path " & $pathId)
  ok()

proc setConnectionAddressParam(
    handle: MsQuicTransportHandle;
    connection: pointer;
    paramId: uint32;
    address: TransportAddress
): string {.raises: [].} =
  if handle.isNil or handle.bridge.isNil or connection.isNil:
    return "MsQuic transport handle unavailable"
  if address.family == AddressFamily.None:
    return "MsQuic address unavailable"
  let api = msruntime.getApiTable(handle.bridge)
  if api.isNil or api.SetParam.isNil:
    return "MsQuic API missing SetParam"

  var addrStorage: SockAddr_storage
  var addrLen: SockLen
  try:
    toSAddr(address, addrStorage, addrLen)
  except CatchableError as exc:
    return "MsQuic address conversion failed: " & exc.msg

  let status =
    try:
      api.SetParam(
        cast[msapi.HQUIC](connection),
        paramId,
        uint32(addrLen),
        addr addrStorage
      )
    except Exception as exc:
      return "MsQuic ConnectionSetParam raised: " & exc.msg
  if status != msapi.QUIC_STATUS_SUCCESS:
    return fmt"MsQuic ConnectionSetParam failed: 0x{status:08x}"
  ""

proc setConnectionBoolParam*(
    handle: MsQuicTransportHandle;
    connection: pointer;
    paramId: uint32;
    enabled: bool
): string {.gcsafe, raises: [].} =
  if handle.isNil or handle.bridge.isNil or connection.isNil:
    return "MsQuic transport handle unavailable"
  let api = msruntime.getApiTable(handle.bridge)
  if api.isNil or api.SetParam.isNil:
    return "MsQuic API missing SetParam"

  var value = if enabled: uint8(1) else: uint8(0)
  var status = msapi.QUIC_STATUS_SUCCESS
  {.gcsafe.}:
    try:
      status = api.SetParam(
        cast[msapi.HQUIC](connection),
        paramId,
        uint32(sizeof(value)),
        addr value
      )
    except Exception as exc:
      return "MsQuic ConnectionSetParam raised: " & exc.msg
  if status != msapi.QUIC_STATUS_SUCCESS:
    return fmt"MsQuic ConnectionSetParam failed: 0x{status:08x}"
  ""

proc enableConnectionDatagramReceive*(
    handle: MsQuicTransportHandle; connection: pointer; enabled = true
): string {.inline, gcsafe, raises: [].} =
  setConnectionBoolParam(
    handle,
    connection,
    msparams.QUIC_PARAM_CONN_DATAGRAM_RECEIVE_ENABLED,
    enabled
  )

proc enableConnectionDatagramSend*(
    handle: MsQuicTransportHandle; connection: pointer; enabled = true
): string {.inline, gcsafe, raises: [].} =
  setConnectionBoolParam(
    handle,
    connection,
    msparams.QUIC_PARAM_CONN_DATAGRAM_SEND_ENABLED,
    enabled
  )

proc setConnectionLocalAddress*(handle: MsQuicTransportHandle; connection: pointer;
    address: TransportAddress): string {.raises: [].} =
  setConnectionAddressParam(handle, connection, msparams.QUIC_PARAM_CONN_LOCAL_ADDRESS, address)

proc setConnectionRemoteAddress*(handle: MsQuicTransportHandle; connection: pointer;
    address: TransportAddress): string {.raises: [].} =
  setConnectionAddressParam(handle, connection, msparams.QUIC_PARAM_CONN_REMOTE_ADDRESS, address)

proc startStream*(handle: MsQuicTransportHandle; stream: pointer;
    flags: uint32 = 0'u32): string {.raises: [].} =
  if handle.isNil or handle.bridge.isNil or stream.isNil:
    return "MsQuic transport handle unavailable"
  let effectiveFlags =
    if flags == 0'u32: QuicStreamStartImmediateFlag else: flags
  let status =
    try:
      msruntime.startStream(
        handle.bridge,
        cast[msapi.HQUIC](stream),
        msapi.QUIC_STREAM_START_FLAGS(effectiveFlags)
      )
    except Exception as exc:
      return "MsQuic StreamStart raised: " & exc.msg
  if status != msapi.QUIC_STATUS_SUCCESS and status != QuicStatusPending:
    return fmt"MsQuic StreamStart failed: 0x{status:08x}"
  ""

proc shutdownStream*(handle: MsQuicTransportHandle; stream: pointer;
    flags: uint32 = msapi.QUIC_STREAM_SHUTDOWN_FLAG_GRACEFUL;
    errorCode: uint64 = 0'u64;
    state: MsQuicStreamState = nil): string {.gcsafe, raises: [].} =
  if handle.isNil or handle.bridge.isNil or stream.isNil:
    return "MsQuic transport handle unavailable"
  if not state.isNil:
    var alreadyRequested = false
    state.withStateLock:
      if state.closed:
        alreadyRequested = true
      elif (flags and msapi.QUIC_STREAM_SHUTDOWN_FLAG_GRACEFUL) != 0'u32:
        alreadyRequested = state.sendShutdownRequested
        if not alreadyRequested:
          state.sendShutdownRequested = true
    if alreadyRequested:
      return ""
  let status =
    try:
      var tmpStatus = msapi.QUIC_STATUS_INTERNAL_ERROR
      {.cast(gcsafe).}:
        tmpStatus = msruntime.shutdownStream(
          handle.bridge,
          cast[msapi.HQUIC](stream),
          msapi.QUIC_STREAM_SHUTDOWN_FLAGS(flags),
          msapi.QUIC_UINT62(errorCode)
        )
      tmpStatus
    except Exception as exc:
      return "MsQuic StreamShutdown raised: " & exc.msg
  if status != msapi.QUIC_STATUS_SUCCESS:
    return fmt"MsQuic StreamShutdown failed: 0x{status:08x}"
  ""

proc beginSendFin*(state: MsQuicStreamState): bool {.gcsafe, raises: [].} =
  if state.isNil:
    return false
  state.withStateLock:
    if state.closed or state.sendShutdownRequested:
      return false
    state.sendShutdownRequested = true
  true

proc closeStream*(handle: MsQuicTransportHandle; stream: pointer;
    state: MsQuicStreamState = nil) {.raises: [].} =
  if not state.isNil:
    state.close()
  if handle.isNil or handle.bridge.isNil or stream.isNil:
    return
  try:
    msruntime.closeStream(handle.bridge, cast[msapi.HQUIC](stream))
  except Exception:
    discard

proc shutdownConnection*(handle: MsQuicTransportHandle; connection: pointer;
    flags: uint32 = 0'u32; errorCode: uint64 = 0'u64): string {.raises: [].} =
  if handle.isNil or handle.bridge.isNil or connection.isNil:
    return "MsQuic transport handle unavailable"
  let status =
    try:
      msruntime.shutdownConnection(
        handle.bridge,
        cast[msapi.HQUIC](connection),
        flags,
        errorCode
      )
    except Exception as exc:
      return "MsQuic ConnectionShutdown raised: " & exc.msg
  if status != msapi.QUIC_STATUS_SUCCESS:
    return fmt"MsQuic ConnectionShutdown failed: 0x{status:08x}"
  ""

proc closeConnection*(handle: MsQuicTransportHandle; connection: pointer;
    state: MsQuicConnectionState = nil) {.raises: [].} =
  if not state.isNil:
    trace "MsQuic closeConnection state close begin", conn = cast[uint64](connection)
    state.close()
    trace "MsQuic closeConnection state close done", conn = cast[uint64](connection)
  if handle.isNil or handle.bridge.isNil or connection.isNil:
    return
  try:
    trace "MsQuic closeConnection runtime close begin", conn = cast[uint64](connection)
    msruntime.closeConnection(handle.bridge, cast[msapi.HQUIC](connection))
    trace "MsQuic closeConnection runtime close done", conn = cast[uint64](connection)
  except Exception:
    discard

proc closeListener*(handle: MsQuicTransportHandle; listener: pointer;
    state: MsQuicListenerState = nil) {.raises: [].} =
  if not state.isNil:
    state.close()
  if handle.isNil or handle.bridge.isNil or listener.isNil:
    return
  try:
    msruntime.closeListener(handle.bridge, cast[msapi.HQUIC](listener))
  except Exception:
    discard

proc listenerTransportActiveForTest*(listener: pointer): bool =
  if listener.isNil:
    return false
  msapi.listenerTransportActiveForTest(cast[msapi.HQUIC](listener))

proc createAcceptedConnectionForTest*(listener: pointer;
    remoteHost: cstring; remotePort: uint16;
    localHost: cstring; localPort: uint16;
    clientCidPtr: ptr uint8; clientCidLen: uint32;
    serverCidPtr: ptr uint8; serverCidLen: uint32;
    connection: var pointer): bool =
  var hquic: msapi.HQUIC = nil
  let ok = msapi.createAcceptedConnectionForTest(
    cast[msapi.HQUIC](listener),
    remoteHost,
    remotePort,
    localHost,
    localPort,
    clientCidPtr,
    clientCidLen,
    serverCidPtr,
    serverCidLen,
    hquic
  )
  connection = cast[pointer](hquic)
  ok
