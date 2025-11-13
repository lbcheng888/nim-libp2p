import std/[deques, locks, options, sequtils, strformat]

import chronos
import chronos/threadsync
import chronicles
import results

import ./msquicruntime
import "nim-msquic/api/runtime_bridge" as msruntime
import "nim-msquic/api/api_impl" as msapi
import "nim-msquic/api/event_model" as msevents
import "nim-msquic/api/tls_bridge" as mstls
import "nim-msquic/tls/common" as mstlstypes

export msquicruntime.MsQuicLoadOptions

logScope:
  topics = "libp2p msquicdriver"

const
  DefaultEventQueueLimit* = 0
  DefaultEventPollInterval* = 5.milliseconds
  DefaultAlpn = "libp2p"
  DefaultAppName = "nim-libp2p"

type
  MsQuicTransportConfig* = object
    loadOptions*: MsQuicLoadOptions
    alpns*: seq[string] = @[DefaultAlpn]
    eventQueueLimit*: int = DefaultEventQueueLimit
    eventPollInterval*: Duration = DefaultEventPollInterval
    appName*: string
    executionProfile*: uint32

  MsQuicTransportHandle* = ref object
    bridge*: RuntimeBridge
    registration*: msapi.HQUIC
    configuration*: msapi.HQUIC
    alpns*: seq[string]
    alpnBuffers*: seq[msapi.QuicBuffer]
    config*: MsQuicTransportConfig
    tlsBinding*: mstls.TlsCredentialBinding
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
    queue: Deque[msevents.ConnectionEvent]
    waiters: seq[Future[msevents.ConnectionEvent]]
    signal: ThreadSignalPtr
    signalLoop: Future[void]
    closed*: bool
    clientContext*: pointer
    externalHandler*: MsQuicConnectionHandler
    droppedEvents*: uint64

  MsQuicListenerState* = ref object
    handle*: MsQuicTransportHandle
    listener*: msapi.HQUIC
    queueLimit*: int
    pollInterval*: Duration
    lock: Lock
    lockInit: bool
    queue: Deque[msevents.ListenerEvent]
    waiters: seq[Future[msevents.ListenerEvent]]
    signal: ThreadSignalPtr
    signalLoop: Future[void]
    closed*: bool
    userContext*: pointer
    externalHandler*: MsQuicListenerHandler
    droppedEvents*: uint64
  MsQuicStreamState* = ref object
    handle*: MsQuicTransportHandle
    connection*: msapi.HQUIC
    stream*: msapi.HQUIC
    connectionState*: MsQuicConnectionState
    queueLimit*: int
    pollInterval*: Duration
    lock: Lock
    lockInit: bool
    queue: Deque[msevents.StreamEvent]
    waiters: seq[Future[msevents.StreamEvent]]
    signal: ThreadSignalPtr
    signalLoop: Future[void]
    closed*: bool
    userContext*: pointer
    externalHandler*: MsQuicStreamHandler
    droppedEvents*: uint64
    readQueue*: Deque[seq[byte]]
    readWaiters*: seq[Future[seq[byte]]]
    pendingSends*: seq[seq[byte]]
  MsQuicEventQueueClosed* = object of CatchableError
  MsTlsConfig = mstlstypes.TlsConfig

template withStateLock(state: typed; body: untyped) =
  when compiles(state.lock):
    acquire(state.lock)
    try:
      body
    finally:
      release(state.lock)

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

proc safeCloseConfiguration(bridge: RuntimeBridge; configuration: msapi.HQUIC) {.inline.} =
  if bridge.isNil or configuration.isNil:
    return
  try:
    msruntime.closeConfiguration(bridge, configuration)
  except Exception:
    discard

proc safeCloseRegistration(bridge: RuntimeBridge; registration: msapi.HQUIC) {.inline.} =
  if bridge.isNil or registration.isNil:
    return
  try:
    msruntime.closeRegistration(bridge, registration)
  except Exception:
    discard

proc safeCloseConnection(bridge: RuntimeBridge; connection: msapi.HQUIC) {.inline.} =
  if bridge.isNil or connection.isNil:
    return
  try:
    msruntime.closeConnection(bridge, connection)
  except Exception:
    discard

proc safeShutdownConnection(
    bridge: RuntimeBridge; connection: msapi.HQUIC; flags: uint32 = 0'u32; errorCode: uint64 = 0'u64
) {.inline.} =
  if bridge.isNil or connection.isNil:
    return
  try:
    discard msruntime.shutdownConnection(bridge, connection, flags, errorCode)
  except Exception:
    discard

proc safeCloseListener(bridge: RuntimeBridge; listener: msapi.HQUIC) {.inline.} =
  if bridge.isNil or listener.isNil:
    return
  try:
    msruntime.closeListener(bridge, listener)
  except Exception:
    discard

proc safeCloseStream(bridge: RuntimeBridge; stream: msapi.HQUIC) {.inline.} =
  if bridge.isNil or stream.isNil:
    return
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
    var event: msevents.ConnectionEvent
    var shouldFail = false
    state.withStateLock:
      if state.queue.len > 0 and state.waiters.len > 0:
        event = state.queue.popFirst()
        fut = state.waiters[0]
        state.waiters.delete(0)
      elif state.closed and state.waiters.len > 0:
        shouldFail = true
      else:
        return
    if not fut.isNil:
      if not fut.finished():
        fut.complete(event)
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
  state.withStateLock:
    if state.closed:
      return
    if state.queueLimit > 0 and state.queue.len >= state.queueLimit:
      discard state.queue.popFirst()
      inc state.droppedEvents
      dropped = true
    state.queue.addLast(event)
    if state.waiters.len > 0:
      shouldSignal = true
  if dropped:
    trace "MsQuic event queue full, dropping oldest event",
      queueLimit = state.queueLimit, droppedEvents = state.droppedEvents
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

proc close(state: MsQuicConnectionState) =
  if state.isNil:
    return
  var hadWaiters = false
  state.withStateLock:
    if state.closed:
      return
    state.closed = true
    hadWaiters = state.waiters.len > 0
  if hadWaiters:
    triggerConnectionDelivery(state)
  deliverConnectionEvents(state)
  failWaiters(state)
  let loopActive = (not state.signalLoop.isNil) and (not state.signalLoop.finished())
  if loopActive:
    state.signalLoop.cancel()
  else:
    closeSignal(state)
  if state.lockInit:
    deinitLock(state.lock)
    state.lockInit = false

proc newMsQuicConnectionState(handle: MsQuicTransportHandle;
    queueLimit: int; pollInterval: Duration;
    handler: MsQuicConnectionHandler; userContext: pointer
  ): Result[MsQuicConnectionState, string] =
  var state = MsQuicConnectionState(
    handle: handle,
    connection: nil,
    queueLimit: queueLimit,
    pollInterval: pollInterval,
    queue: initDeque[msevents.ConnectionEvent](),
    waiters: @[],
    signal: nil,
    signalLoop: nil,
    closed: false,
    clientContext: userContext,
    externalHandler: handler,
    droppedEvents: 0
  )
  initLock(state.lock)
  state.lockInit = true
  let signalRes = ThreadSignalPtr.new()
  if signalRes.isErr():
    close(state)
    return err("failed to initialize MsQuic thread signal: " & signalRes.error)
  state.signal = signalRes.get()
  state.ensureConnectionSignalLoop()
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

  var immediate: Option[msevents.ConnectionEvent]
  var shouldFail = false
  state.withStateLock:
    if state.closed and state.queue.len == 0:
      shouldFail = true
    elif state.queue.len > 0:
      immediate = some(state.queue.popFirst())
    else:
      state.waiters.add(fut)

  if shouldFail:
    fut.fail(newException(MsQuicEventQueueClosed, "MsQuic connection state closed"))
  elif immediate.isSome:
    fut.complete(immediate.get())
  else:
    state.ensureConnectionSignalLoop()
  fut

proc msquicConnectionEventRelay(event: msevents.ConnectionEvent) {.gcsafe.} =
  if event.userContext.isNil:
    return
  let state = cast[MsQuicConnectionState](event.userContext)
  if state.isNil:
    return
  {.cast(gcsafe).}:
    handleIncomingConnectionEvent(state, event)

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

proc deliverStreamEvents(state: MsQuicStreamState) =
  while true:
    var fut: Future[msevents.StreamEvent] = nil
    var event: msevents.StreamEvent
    var shouldFail = false
    state.withStateLock:
      if state.queue.len > 0 and state.waiters.len > 0:
        event = state.queue.popFirst()
        fut = state.waiters[0]
        state.waiters.delete(0)
      elif state.closed and state.waiters.len > 0:
        shouldFail = true
      else:
        return
    if not fut.isNil:
      if not fut.finished():
        fut.complete(event)
    elif shouldFail:
      failStreamWaiters(state)
      return

proc streamSignalPump(state: MsQuicStreamState): Future[void] {.async.} =
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
        trace "MsQuic stream signal wait failed", err = exc.msg
        break
      deliverStreamEvents(state)
  finally:
    failStreamWaiters(state)
    closeSignal(state)
    state.signalLoop = nil

proc ensureStreamSignalLoop(state: MsQuicStreamState) =
  if state.signalLoop.isNil:
    let fut = streamSignalPump(state)
    state.signalLoop = fut
    asyncSpawn fut

proc triggerStreamDelivery(state: MsQuicStreamState) =
  state.ensureStreamSignalLoop()
  if state.signal.isNil:
    deliverStreamEvents(state)
    return
  let res = state.signal.fireSync()
  if res.isErr():
    trace "MsQuic stream signal fire failed", err = res.error
    deliverStreamEvents(state)

proc enqueueStreamEvent(state: MsQuicStreamState; event: msevents.StreamEvent) =
  var shouldSignal = false
  var dropped = false
  state.withStateLock:
    if state.closed:
      return
    if state.queueLimit > 0 and state.queue.len >= state.queueLimit:
      discard state.queue.popFirst()
      inc state.droppedEvents
      dropped = true
    state.queue.addLast(event)
    if state.waiters.len > 0:
      shouldSignal = true
  if dropped:
    trace "MsQuic stream event queue full, dropping oldest event",
      queueLimit = state.queueLimit, droppedEvents = state.droppedEvents
  if shouldSignal:
    triggerStreamDelivery(state)

proc handleIncomingStreamEvent(state: MsQuicStreamState;
    rawEvent: msevents.StreamEvent) =
  if state.isNil:
    return
  var forwarded = rawEvent
  forwarded.userContext = state.userContext
  if not state.externalHandler.isNil:
    try:
      state.externalHandler(forwarded)
    except Exception as exc:
      trace "MsQuic stream external handler raised", err = exc.msg
  enqueueStreamEvent(state, forwarded)

  if forwarded.kind == msevents.seReceive:
    var payloadData = forwarded.payload
    if payloadData.len == 0 and forwarded.totalBufferLength > 0'u64:
      let fallbackLen = int(forwarded.totalBufferLength)
      if fallbackLen > 0:
        payloadData = newSeq[byte](fallbackLen)
    state.withStateLock:
      state.readQueue.addLast(payloadData)
      if state.readWaiters.len > 0:
        let waiter = state.readWaiters[0]
        state.readWaiters.delete(0)
        if not waiter.finished():
          waiter.complete(payloadData)
  elif forwarded.kind == msevents.seSendComplete:
    state.withStateLock:
      if state.pendingSends.len > 0:
        state.pendingSends.delete(0)

proc close(state: MsQuicStreamState) =
  if state.isNil:
    return
  var hadWaiters = false
  state.withStateLock:
    if state.closed:
      return
    state.closed = true
    hadWaiters = state.waiters.len > 0
  if hadWaiters:
    triggerStreamDelivery(state)
  deliverStreamEvents(state)
  failStreamWaiters(state)
  let loopActive = (not state.signalLoop.isNil) and (not state.signalLoop.finished())
  if loopActive:
    state.signalLoop.cancel()
  else:
    closeSignal(state)
  if state.lockInit:
    deinitLock(state.lock)
    state.lockInit = false

proc newMsQuicStreamState(handle: MsQuicTransportHandle; connection: msapi.HQUIC;
    queueLimit: int; pollInterval: Duration;
    handler: MsQuicStreamHandler; userContext: pointer;
    connState: MsQuicConnectionState
  ): Result[MsQuicStreamState, string] =
  var state = MsQuicStreamState(
    handle: handle,
    connection: connection,
    stream: nil,
    queueLimit: queueLimit,
    pollInterval: pollInterval,
    queue: initDeque[msevents.StreamEvent](),
    waiters: @[],
    signal: nil,
    signalLoop: nil,
    closed: false,
    connectionState: connState,
    userContext: userContext,
    externalHandler: handler,
    droppedEvents: 0,
    readQueue: initDeque[seq[byte]](),
    readWaiters: @[],
    pendingSends: @[]
  )
  initLock(state.lock)
  state.lockInit = true
  let signalRes = ThreadSignalPtr.new()
  if signalRes.isErr():
    close(state)
    return err("failed to initialize MsQuic stream signal: " & signalRes.error)
  state.signal = signalRes.get()
  state.ensureStreamSignalLoop()
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

  var immediate: Option[msevents.StreamEvent]
  var shouldFail = false
  state.withStateLock:
    if state.closed and state.queue.len == 0:
      shouldFail = true
    elif state.queue.len > 0:
      immediate = some(state.queue.popFirst())
    else:
      state.waiters.add(fut)

  if shouldFail:
    fut.fail(newException(MsQuicEventQueueClosed, "MsQuic stream state closed"))
  elif immediate.isSome:
    fut.complete(immediate.get())
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
    var event: msevents.ListenerEvent
    var shouldFail = false
    state.withStateLock:
      if state.queue.len > 0 and state.waiters.len > 0:
        event = state.queue.popFirst()
        fut = state.waiters[0]
        state.waiters.delete(0)
      elif state.closed and state.waiters.len > 0:
        shouldFail = true
      else:
        return
    if not fut.isNil:
      if not fut.finished():
        fut.complete(event)
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
  state.withStateLock:
    if state.closed:
      return
    if state.queueLimit > 0 and state.queue.len >= state.queueLimit:
      discard state.queue.popFirst()
      inc state.droppedEvents
      dropped = true
    state.queue.addLast(event)
    if state.waiters.len > 0:
      shouldSignal = true
  if dropped:
    trace "MsQuic listener event queue full, dropping oldest event",
      queueLimit = state.queueLimit, droppedEvents = state.droppedEvents
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

proc close(state: MsQuicListenerState) =
  if state.isNil:
    return
  var hadWaiters = false
  state.withStateLock:
    if state.closed:
      return
    state.closed = true
    hadWaiters = state.waiters.len > 0
  if hadWaiters:
    triggerListenerDelivery(state)
  deliverListenerEvents(state)
  failListenerWaiters(state)
  let loopActive = (not state.signalLoop.isNil) and (not state.signalLoop.finished())
  if loopActive:
    state.signalLoop.cancel()
  else:
    closeSignal(state)
  if state.lockInit:
    deinitLock(state.lock)
    state.lockInit = false

proc newMsQuicListenerState(handle: MsQuicTransportHandle; queueLimit: int;
    pollInterval: Duration; handler: MsQuicListenerHandler; userContext: pointer
  ): Result[MsQuicListenerState, string] =
  var state = MsQuicListenerState(
    handle: handle,
    listener: nil,
    queueLimit: queueLimit,
    pollInterval: pollInterval,
    queue: initDeque[msevents.ListenerEvent](),
    waiters: @[],
    signal: nil,
    signalLoop: nil,
    closed: false,
    userContext: userContext,
    externalHandler: handler,
    droppedEvents: 0
  )
  initLock(state.lock)
  state.lockInit = true
  let signalRes = ThreadSignalPtr.new()
  if signalRes.isErr():
    close(state)
    return err("failed to initialize MsQuic listener signal: " & signalRes.error)
  state.signal = signalRes.get()
  state.ensureListenerSignalLoop()
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

  var immediate: Option[msevents.ListenerEvent]
  var shouldFail = false
  state.withStateLock:
    if state.closed and state.queue.len == 0:
      shouldFail = true
    elif state.queue.len > 0:
      immediate = some(state.queue.popFirst())
    else:
      state.waiters.add(fut)

  if shouldFail:
    fut.fail(newException(MsQuicEventQueueClosed, "MsQuic listener state closed"))
  elif immediate.isSome:
    fut.complete(immediate.get())
  else:
    state.ensureListenerSignalLoop()
  fut

proc msquicListenerEventRelay(event: msevents.ListenerEvent) {.gcsafe.} =
  if event.userContext.isNil:
    return
  let state = cast[MsQuicListenerState](event.userContext)
  if state.isNil:
    return
  {.cast(gcsafe).}:
    handleIncomingListenerEvent(state, event)

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
  state.withStateLock:
    state.readQueue.addLast(payload)
    if state.readWaiters.len > 0:
      let waiter = state.readWaiters[0]
      state.readWaiters.delete(0)
      if not waiter.finished():
        waiter.complete(payload)
  enqueueStreamEvent(state, event)

proc readStream*(state: MsQuicStreamState): Future[seq[byte]] =
  if state.isNil or state.closed:
    let fut = Future[seq[byte]].init("msquic.stream.read")
    fut.fail(newException(MsQuicEventQueueClosed, "MsQuic stream closed"))
    return fut
  let fut = Future[seq[byte]].init("msquic.stream.read")
  var immediate: Option[seq[byte]]
  state.withStateLock:
    if state.readQueue.len > 0:
      immediate = some(state.readQueue.popFirst())
    else:
      state.readWaiters.add(fut)
  if immediate.isSome:
    fut.complete(immediate.get())
  fut

proc writeStream*(state: MsQuicStreamState; data: seq[byte];
    flags: uint32 = 0'u32; clientContext: pointer = nil): string =
  if state.isNil or state.closed:
    return "MsQuic stream closed"
  let handle = state.handle
  if handle.isNil or handle.bridge.isNil:
    return "MsQuic transport handle unavailable"

  var storage = data
  var bufferPtr: ptr msapi.QuicBuffer = nil
  var bufferCount: uint32 = 0
  var sendBuffers: array[1, msapi.QuicBuffer]

  state.withStateLock:
    state.pendingSends.add(storage)
    let lastIdx = state.pendingSends.len - 1
    if state.pendingSends[lastIdx].len > 0:
      sendBuffers[0] = msapi.QuicBuffer(
        Length: uint32(state.pendingSends[lastIdx].len),
        Buffer: cast[ptr uint8](addr state.pendingSends[lastIdx][0])
      )
      bufferPtr = addr sendBuffers[0]
      bufferCount = 1

  let status = msruntime.sendStream(
    handle.bridge,
    state.stream,
    bufferPtr,
    bufferCount,
    msapi.QUIC_SEND_FLAGS(flags),
    clientContext
  )

  if status != msapi.QUIC_STATUS_SUCCESS:
    state.withStateLock:
      if state.pendingSends.len > 0:
        state.pendingSends.setLen(state.pendingSends.len - 1)
    return fmt"MsQuic StreamSend failed: 0x{status:08x}"
  ""

proc streamId*(state: MsQuicStreamState): Result[uint64, string] {.raises: [].} =
  if state.isNil or state.handle.isNil or state.handle.bridge.isNil or state.stream.isNil:
    return err("MsQuic stream state unavailable")
  state.handle.bridge.streamId(state.stream)

proc releaseRegistration(handle: MsQuicTransportHandle) {.raises: [].} =
  if handle.bridge.isNil:
    return
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
  if not handle.tlsBinding.isNil:
    handle.tlsBinding.cleanup()
    handle.tlsBinding = nil
  releaseRegistration(handle)
  if not handle.bridge.isNil:
    releaseMsQuicBridge(handle.bridge)
    handle.bridge = nil

proc initMsQuicTransport*(cfg: MsQuicTransportConfig = MsQuicTransportConfig()):
    tuple[handle: MsQuicTransportHandle, error: string] {.raises: [].} =
  let bridgeRes = acquireMsQuicBridge(cfg.loadOptions)
  if not bridgeRes.success:
    return (nil, bridgeRes.error)
  let bridge = bridgeRes.bridge

  var registration: msapi.HQUIC = nil
  var configuration: msapi.HQUIC = nil
  var alpnBuffers: seq[msapi.QuicBuffer]

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
      nil,
      0'u32,
      nil,
      configuration
    )
    if cfgStatus != msapi.QUIC_STATUS_SUCCESS or configuration.isNil:
      safeCloseRegistration(bridge, registration)
      releaseMsQuicBridge(bridge)
      return (nil, fmt"MsQuic configuration failed: 0x{cfgStatus:08x}")
  except Exception as exc:
    safeCloseRegistration(bridge, registration)
    releaseMsQuicBridge(bridge)
    return (nil, "MsQuic configuration raised: " & exc.msg)

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
    alpns: alpns,
    alpnBuffers: alpnBuffers,
    config: cfg,
    tlsBinding: nil,
    closed: false
  )
  (handle, "")

proc loadCredential*(handle: MsQuicTransportHandle; cfg: MsTlsConfig;
    tempDir: string = ""): string {.raises: [].} =
  if handle.isNil or handle.bridge.isNil or handle.configuration.isNil or handle.closed:
    return "MsQuic transport handle unavailable"
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
  if not handle.tlsBinding.isNil:
    handle.tlsBinding.cleanup()
  handle.tlsBinding = binding
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
    handler = nil,
    userContext = nil
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
    safeCloseConnection(handle.bridge, hconn)
    state.close()
    return (none(MsQuicConnectionState),
            fmt"MsQuic ConnectionSetConfiguration failed: 0x{cfgStatus:08x}")

  state.connection = hconn
  (some(state), "")

proc dialConnection*(handle: MsQuicTransportHandle; serverName: string; port: uint16;
    handler: MsQuicConnectionHandler = nil; userContext: pointer = nil;
    addressFamily: uint16 = 0; queueLimit: int = 0;
    pollInterval: Duration = DefaultEventPollInterval):
    tuple[connection: pointer, state: Option[MsQuicConnectionState], error: string] {.raises: [].} =
  if handle.isNil or handle.bridge.isNil or handle.closed:
    return (nil, none(MsQuicConnectionState), "MsQuic transport handle unavailable")
  if serverName.len == 0:
    return (nil, none(MsQuicConnectionState), "MsQuic dial requires non-empty serverName")

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
  try:
    let connStatus = msruntime.openConnection(
      handle.bridge,
      handle.registration,
      msquicConnectionEventRelay,
      connection,
      cast[pointer](state)
    )
    if connStatus != msapi.QUIC_STATUS_SUCCESS or connection.isNil:
      safeCloseConnection(handle.bridge, connection)
      state.close()
      return (nil, none(MsQuicConnectionState), fmt"MsQuic ConnectionOpen failed: 0x{connStatus:08x}")
  except Exception as exc:
    safeCloseConnection(handle.bridge, connection)
    state.close()
    return (nil, none(MsQuicConnectionState), "MsQuic ConnectionOpen raised: " & exc.msg)

  state.connection = connection

  try:
    let setCfgStatus = api.ConnectionSetConfiguration(
      connection,
      handle.configuration
    )
    if setCfgStatus != msapi.QUIC_STATUS_SUCCESS:
      safeCloseConnection(handle.bridge, connection)
      state.close()
      return (nil, none(MsQuicConnectionState),
              fmt"MsQuic ConnectionSetConfiguration failed: 0x{setCfgStatus:08x}")
  except Exception as exc:
    safeCloseConnection(handle.bridge, connection)
    state.close()
    return (nil, none(MsQuicConnectionState), "MsQuic ConnectionSetConfiguration raised: " & exc.msg)

  let family = msapi.QUIC_ADDRESS_FAMILY(addressFamily)
  try:
    let statusStart = api.ConnectionStart(
      connection,
      handle.configuration,
      family,
      serverName.cstring,
      port
    )
    if statusStart != msapi.QUIC_STATUS_SUCCESS:
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
    connectionState
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
    connectionState
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

proc startStream*(handle: MsQuicTransportHandle; stream: pointer;
    flags: uint32 = 0'u32): string {.raises: [].} =
  if handle.isNil or handle.bridge.isNil or stream.isNil:
    return "MsQuic transport handle unavailable"
  let status =
    try:
      msruntime.startStream(
        handle.bridge,
        cast[msapi.HQUIC](stream),
        msapi.QUIC_STREAM_START_FLAGS(flags)
      )
    except Exception as exc:
      return "MsQuic StreamStart raised: " & exc.msg
  if status != msapi.QUIC_STATUS_SUCCESS:
    return fmt"MsQuic StreamStart failed: 0x{status:08x}"
  ""

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
    state.close()
  if handle.isNil or handle.bridge.isNil or connection.isNil:
    return
  try:
    msruntime.closeConnection(handle.bridge, cast[msapi.HQUIC](connection))
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
