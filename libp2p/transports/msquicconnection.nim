## MsQuicConnection：基于 msquicdriver 提供的连接/流状态构建 libp2p Connection。
##
## 当前连接仍保留一个“主”流以兼容既有 Connection API，但内部已经维护
## 统一的活动流注册表，供 WebTransport/native mux/inbound peer stream 共享
## 生命周期与清理逻辑。

when not defined(libp2p_msquic_experimental):
  {.error: "MsQuicConnection requires -d:libp2p_msquic_experimental".}

import std/[deques, options, sequtils, strutils]
import stew/byteutils
import results
import chronos, chronicles, metrics

import ../stream/connection
import ../multiaddress
import ../bandwidthmanager
import ./quicruntime as msquicdrv
import ./msquicstream
import ./webtransport_common

logScope:
  topics = "libp2p msquicconnection"

const
  MsQuicConnectionReadRetryAttempts = 4
  MsQuicConnectionReadRetryDelay = 50.milliseconds

template msquicSafe(body: untyped) =
  {.cast(gcsafe).}:
    body

type
  MsQuicManagedStream = object
    handle: pointer
    state: msquicdrv.MsQuicStreamState
    wrapper: MsQuicStream
    role: string
    primary: bool

  MsQuicConnection* = ref object of Connection
    handle: msquicdrv.MsQuicTransportHandle
    connHandle: pointer
    connState: msquicdrv.MsQuicConnectionState
    streamHandle: pointer
    streamState: msquicdrv.MsQuicStreamState
    cached: seq[byte]
    monitor*: Future[void]
    handshakeInfo*: Option[WebtransportHandshakeInfo]
    http3Settings*: Option[Http3Settings]
    webtransportMode*: Option[WebtransportMode]
    isWebtransport*: bool
    webtransportControlSend*: Option[MsQuicStream]
    webtransportControlRecv*: Option[MsQuicStream]
    webtransportRequestStream*: Option[MsQuicStream]
    webtransportSessionId*: uint64
    webtransportReady*: bool
    webtransportHandshakeStart*: Option[Moment]
    webtransportHandshakeReadyAt*: Option[Moment]
    webtransportAuthority*: string
    webtransportPath*: string
    webtransportDraft*: string
    webtransportSlotReserved*: bool
    datagramQueue: Deque[seq[byte]]
    datagramWaiters: seq[Future[seq[byte]]]
    datagramSendEnabled*: bool
    datagramMaxSend*: uint16
    sessionResumed*: bool
    nativeMuxActive*: bool
    activeStreams: seq[MsQuicManagedStream]
    protocolGate: AsyncLock
    onClosed: proc(conn: MsQuicConnection): Future[void] {.gcsafe.}
    registered*: bool

proc beginWebtransportHandshake*(
    conn: MsQuicConnection, info: WebtransportHandshakeInfo
) =
  if conn.isNil:
    return
  conn.webtransportHandshakeStart = some(Moment.now())
  conn.webtransportHandshakeReadyAt = none(Moment)
  conn.webtransportAuthority = info.authority
  conn.webtransportPath = info.path
  conn.webtransportDraft = info.draft
  conn.webtransportReady = false
  conn.webtransportSessionId = 0'u64

proc completeWebtransportHandshake*(
    conn: MsQuicConnection, info: WebtransportHandshakeInfo, sessionId: uint64
) =
  if conn.isNil:
    return
  conn.webtransportAuthority = info.authority
  conn.webtransportPath = info.path
  conn.webtransportDraft = info.draft
  conn.webtransportSessionId = sessionId
  conn.webtransportReady = true
  conn.webtransportHandshakeReadyAt = some(Moment.now())

proc msquicConnError(msg: string; parent: ref Exception = nil): ref LPStreamError =
  (ref LPStreamError)(msg: msg, parent: parent)

proc ensureStream(conn: MsQuicConnection) =
  if conn.isNil or conn.streamState.isNil or conn.streamHandle.isNil:
    raise newLPStreamConnDownError()

proc setHandshakeInfo*(
    conn: MsQuicConnection, info: WebtransportHandshakeInfo
) =
  if conn.isNil:
    return
  conn.handshakeInfo = some(info)
  conn.webtransportMode = some(info.mode)
  conn.isWebtransport = true

proc setRemoteSettings*(
    conn: MsQuicConnection, settings: Http3Settings
) =
  if conn.isNil:
    return
  conn.http3Settings = some(settings)

proc handshakeInfo*(conn: MsQuicConnection): Option[WebtransportHandshakeInfo] =
  if conn.isNil:
    return none(WebtransportHandshakeInfo)
  conn.handshakeInfo

proc remoteSettings*(conn: MsQuicConnection): Option[Http3Settings] =
  if conn.isNil:
    return none(Http3Settings)
  conn.http3Settings

proc activateNativeMux*(conn: MsQuicConnection) =
  if conn.isNil:
    return
  conn.nativeMuxActive = true

proc sharedProtocolGate*(conn: MsQuicConnection): AsyncLock {.inline, gcsafe, raises: [].} =
  if conn.isNil:
    return nil
  conn.protocolGate

proc releaseProtocolGate(conn: MsQuicConnection) {.gcsafe, raises: [].} =
  if conn.isNil or conn.protocolGate.isNil:
    return
  try:
    conn.protocolGate.release()
  except AsyncLockError:
    discard

proc shouldRetryCancelledRead(conn: MsQuicConnection): bool {.gcsafe, raises: [].} =
  if conn.isNil or conn.connState.isNil:
    return false
  try:
    let handshakeComplete = msquicdrv.connectionHandshakeComplete(conn.connState)
    let closeReason = msquicdrv.connectionCloseReason(conn.connState)
    handshakeComplete and closeReason.len == 0
  except CatchableError:
    false
  except Exception:
    false

proc detachPrimaryStreamState*(
    conn: MsQuicConnection
): tuple[state: msquicdrv.MsQuicStreamState, localInitiated: bool] {.gcsafe.} =
  if conn.isNil or conn.streamState.isNil:
    return (nil, false)
  result.state = conn.streamState
  result.localInitiated = msquicdrv.isLocalInitiated(conn.streamState)
  var kept: seq[MsQuicManagedStream] = @[]
  for entry in conn.activeStreams:
    if entry.state != conn.streamState:
      kept.add(entry)
  conn.activeStreams = kept
  conn.streamHandle = nil
  conn.streamState = nil
  conn.cached.setLen(0)

proc registerManagedStream(
    conn: MsQuicConnection,
    handle: pointer,
    state: msquicdrv.MsQuicStreamState,
    role: string,
    wrapper: MsQuicStream = nil,
    primary = false
) =
  if conn.isNil or handle.isNil or state.isNil:
    return
  var replaced = false
  for entry in conn.activeStreams.mitems():
    if entry.state == state or entry.handle == handle:
      entry.handle = handle
      entry.state = state
      entry.wrapper = wrapper
      entry.role = role
      entry.primary = primary
      replaced = true
    elif primary and entry.primary:
      entry.primary = false
  if not replaced:
    if primary:
      for entry in conn.activeStreams.mitems():
        entry.primary = false
    conn.activeStreams.add(MsQuicManagedStream(
      handle: handle,
      state: state,
      wrapper: wrapper,
      role: role,
      primary: primary
    ))

proc closeManagedStreams(conn: MsQuicConnection) {.async: (raises: []).} =
  if conn.isNil or conn.activeStreams.len == 0:
    return
  for entry in conn.activeStreams.mitems():
    if not entry.wrapper.isNil:
      try:
        await entry.wrapper.closeImpl()
      except CatchableError as exc:
        trace "MsQuic managed stream close raised", err = exc.msg, role = entry.role
      entry.state = nil
      entry.handle = nil
      entry.wrapper = nil
      continue
    if entry.handle.isNil or entry.state.isNil:
      continue
    try:
      msquicSafe:
        msquicdrv.closeStream(conn.handle, entry.handle, entry.state)
    except Exception as exc:
      trace "MsQuic managed state close raised", err = exc.msg, role = entry.role
    entry.handle = nil
    entry.state = nil
  conn.activeStreams.setLen(0)
  conn.streamHandle = nil
  conn.streamState = nil
  conn.cached.setLen(0)

proc shutdownConnection(conn: MsQuicConnection) {.gcsafe, raises: [].} =
  if conn.connHandle.isNil:
    return
  try:
    msquicSafe:
      discard msquicdrv.shutdownConnection(conn.handle, conn.connHandle)
      msquicdrv.closeConnection(conn.handle, conn.connHandle, conn.connState)
  except Exception as exc:
    trace "MsQuic connection shutdown raised", err = exc.msg
  conn.connHandle = nil
  conn.connState = nil

proc closeWebtransportStreams(conn: MsQuicConnection) {.async.} =
  if conn.isNil:
    return
  if conn.webtransportControlSend.isSome:
    let stream = conn.webtransportControlSend.get()
    try:
      await noCancel stream.closeImpl()
    except CatchableError:
      discard
    conn.webtransportControlSend = none(MsQuicStream)
  if conn.webtransportControlRecv.isSome:
    let stream = conn.webtransportControlRecv.get()
    try:
      await noCancel stream.closeImpl()
    except CatchableError:
      discard
    conn.webtransportControlRecv = none(MsQuicStream)
  if conn.webtransportRequestStream.isSome:
    let stream = conn.webtransportRequestStream.get()
    try:
      await noCancel stream.closeImpl()
    except CatchableError:
      discard
    conn.webtransportRequestStream = none(MsQuicStream)
  conn.webtransportReady = false
  conn.webtransportSessionId = 0'u64

proc enqueueDatagram(conn: MsQuicConnection; payload: seq[byte]) =
  if conn.isNil:
    return
  if conn.datagramWaiters.len > 0:
    let waiter = conn.datagramWaiters[0]
    conn.datagramWaiters.delete(0)
    if not waiter.finished():
      waiter.complete(payload)
  else:
    conn.datagramQueue.addLast(payload)

proc failDatagramWaiters(conn: MsQuicConnection) =
  if conn.isNil:
    return
  if conn.datagramWaiters.len > 0:
    for fut in conn.datagramWaiters:
      if fut.isNil or fut.finished():
        continue
      fut.fail(newLPStreamConnDownError())
    conn.datagramWaiters.setLen(0)
  if conn.datagramQueue.len > 0:
    conn.datagramQueue = initDeque[seq[byte]]()

proc monitorConnection(conn: MsQuicConnection): Future[void] {.async.} =
  if conn.isNil or conn.connState.isNil:
    return
  try:
    while true:
      let fut = conn.connState.nextQuicConnectionEvent()
      let completed = await fut.withTimeout(1.seconds)
      if not completed:
        fut.cancel()
        continue
      let event =
        try:
          await fut
        except msquicdrv.QuicRuntimeEventQueueClosed:
          break
      case event.kind
      of msquicdrv.qceConnected:
        conn.sessionResumed = event.sessionResumed
        trace "MsQuic connection established", resumed = event.sessionResumed
      of msquicdrv.qcePeerStreamStarted:
        trace "MsQuic connection queued peer stream",
          peerId = conn.peerId,
          hasPrimary = not conn.streamState.isNil
      of msquicdrv.qceShutdownInitiated, msquicdrv.qceShutdownComplete:
        trace "MsQuic connection shutdown event", note = event.note
        break
      of msquicdrv.qceDatagramStateChanged:
        conn.datagramSendEnabled = event.boolValue
        if event.maxSendLength > 0'u16:
          conn.datagramMaxSend = event.maxSendLength
      of msquicdrv.qceDatagramReceived:
        conn.enqueueDatagram(event.datagramPayload)
      else:
        discard
  except CancelledError:
    discard
  except Exception as exc:
    trace "MsQuic connection monitor raised", err = exc.msg
  finally:
    conn.failDatagramWaiters()
    await noCancel closeWebtransportStreams(conn)
    await closeManagedStreams(conn)
    shutdownConnection(conn)
    if not conn.onClosed.isNil:
      try:
        await conn.onClosed(conn)
      except Exception as exc:
        trace "MsQuic onClosed handler raised", err = exc.msg
      conn.onClosed = nil

proc newMsQuicConnection*(
    handle: msquicdrv.MsQuicTransportHandle,
    connHandle: pointer,
    connState: msquicdrv.MsQuicConnectionState,
    primaryStream: pointer = nil,
    primaryStreamState: msquicdrv.MsQuicStreamState = nil,
    createPrimaryStream = true,
    observed: Opt[MultiAddress] = Opt.none(MultiAddress),
    local: Opt[MultiAddress] = Opt.none(MultiAddress),
    onClosed: proc(conn: MsQuicConnection): Future[void] {.gcsafe.} = nil
): MsQuicConnection {.gcsafe.} =
  if handle.isNil or connHandle.isNil or connState.isNil:
    raise msquicConnError("MsQuic connection requires valid handle/state")
  var streamHandle: pointer = nil
  var streamState: msquicdrv.MsQuicStreamState = nil
  if createPrimaryStream:
    var streamStateOpt: Option[msquicdrv.MsQuicStreamState]
    var streamErr = ""
    if not primaryStreamState.isNil:
      streamState = primaryStreamState
      streamHandle =
        if primaryStream.isNil:
          cast[pointer](streamState.stream)
        else:
          primaryStream
      if streamHandle.isNil or streamState.stream.isNil:
        raise msquicConnError("MsQuic primary stream state unavailable")
    elif not primaryStream.isNil:
      streamHandle = primaryStream
      try:
        msquicSafe:
          let res = msquicdrv.adoptStream(handle, primaryStream, connState)
          streamStateOpt = res.state
          streamErr = res.error
      except Exception as exc:
        streamStateOpt = none(msquicdrv.MsQuicStreamState)
        streamErr = "MsQuic adoptStream raised: " & exc.msg
    else:
      try:
        let res = block:
          var tmp: tuple[
            stream: pointer,
            state: Option[msquicdrv.MsQuicStreamState],
            error: string
          ]
          msquicSafe:
            tmp = msquicdrv.createStream(
              handle,
              connHandle,
              connectionState = connState
            )
          tmp
        streamHandle = res.stream
        streamStateOpt = res.state
        streamErr = res.error
      except Exception as exc:
        streamHandle = nil
        streamStateOpt = none(msquicdrv.MsQuicStreamState)
        streamErr = "MsQuic createStream raised: " & exc.msg
    if streamState.isNil:
      if streamErr.len > 0 or streamStateOpt.isNone or streamHandle.isNil:
        raise msquicConnError("MsQuic stream unavailable: " & streamErr)
      streamState = streamStateOpt.get()
    when defined(libp2p_msquic_debug):
      let origin = if primaryStream.isNil: "local" else: "peer"
      warn "MsQuic connection stream selected",
        origin = origin,
        stream = cast[uint64](streamHandle)
    if primaryStream.isNil and primaryStreamState.isNil:
      var startErr = ""
      try:
        msquicSafe:
          startErr = msquicdrv.startStream(handle, streamHandle)
      except Exception as exc:
        startErr = "MsQuic startStream raised: " & exc.msg
      if startErr.len > 0:
        try:
          msquicSafe:
            msquicdrv.closeStream(handle, streamHandle, streamState)
        except Exception as exc:
          trace "MsQuic stream cleanup raised", err = exc.msg
        raise msquicConnError("MsQuic stream start failed: " & startErr)
      msquicSafe:
        msquicdrv.markStreamStartComplete(streamState)

  result = MsQuicConnection(
    handle: handle,
    connHandle: connHandle,
    connState: connState,
    streamHandle: streamHandle,
    streamState: streamState,
    cached: @[],
    observedAddr: observed,
    localAddr: local,
    handshakeInfo: none(WebtransportHandshakeInfo),
    http3Settings: none(Http3Settings),
    webtransportMode: none(WebtransportMode),
    isWebtransport: false,
    webtransportControlSend: none(MsQuicStream),
    webtransportControlRecv: none(MsQuicStream),
    webtransportRequestStream: none(MsQuicStream),
    webtransportSessionId: 0'u64,
    webtransportReady: false,
    webtransportHandshakeStart: none(Moment),
    webtransportHandshakeReadyAt: none(Moment),
    webtransportAuthority: "",
    webtransportPath: "",
    webtransportDraft: "",
    webtransportSlotReserved: false,
    datagramQueue: initDeque[seq[byte]](),
    datagramWaiters: @[],
    datagramSendEnabled: false,
    datagramMaxSend: 0'u16,
    sessionResumed: false,
    nativeMuxActive: false,
    activeStreams: @[],
    protocolGate: newAsyncLock(),
    onClosed: onClosed,
    registered: false
  )
  if not streamHandle.isNil and not streamState.isNil:
    result.registerManagedStream(
      streamHandle,
      streamState,
      role = if primaryStream.isNil: "bootstrap_outbound" else: "bootstrap_inbound",
      primary = true
    )
  result.objName = "MsQuicConnection"
  result.initStream()
  result.multistreamVersion = msv1
  let monitorFut = monitorConnection(result)
  result.monitor = monitorFut
  asyncSpawn monitorFut

proc sendDatagram*(
    conn: MsQuicConnection, payload: seq[byte]
): bool =
  if conn.isNil or conn.handle.isNil or conn.connHandle.isNil:
    return false
  if not conn.datagramSendEnabled:
    trace "MsQuic datagram send disabled"
    return false
  if conn.datagramMaxSend > 0'u16 and payload.len > conn.datagramMaxSend.int:
    trace "MsQuic datagram exceeds max send length",
      max = conn.datagramMaxSend, attempted = payload.len
    return false
  let err = msquicdrv.sendDatagram(conn.handle, conn.connHandle, payload)
  if err.len > 0:
    trace "MsQuic sendDatagram failed", error = err
    return false
  true

proc recvDatagram*(conn: MsQuicConnection): Future[seq[byte]] {.async.} =
  if conn.isNil or conn.connHandle.isNil or conn.connState.isNil:
    return @[]
  if conn.datagramQueue.len > 0:
    let payload = conn.datagramQueue.popFirst()
    return payload
  let fut = Future[seq[byte]].init("msquic.connection.datagram.recv")
  conn.datagramWaiters.add(fut)
  await fut

proc openMsQuicStream*(
    conn: MsQuicConnection,
    unidirectional: bool,
    dir: Direction = Direction.Out
): MsQuicStream {.gcsafe.} =
  if conn.isNil or conn.handle.isNil or conn.connHandle.isNil or conn.connState.isNil:
    raise msquicConnError("MsQuic connection not initialised")
  let flags = if unidirectional: 0x0001'u32 else: 0'u32
  var res: tuple[
    stream: pointer,
    state: Option[msquicdrv.MsQuicStreamState],
    error: string
  ]
  msquicSafe:
    res = msquicdrv.createStream(
      conn.handle,
      conn.connHandle,
      flags = flags,
      connectionState = conn.connState
    )
  if res.error.len > 0 or res.state.isNone or res.stream.isNil:
    raise msquicConnError(
      "MsQuic stream unavailable: " & (if res.error.len > 0: res.error else: "unknown error")
    )
  let state = res.state.get()
  var startErr = ""
  msquicSafe:
    startErr = msquicdrv.startStream(conn.handle, res.stream)
  if startErr.len > 0:
    msquicSafe:
      msquicdrv.closeStream(conn.handle, res.stream, state)
    raise msquicConnError("MsQuic stream start failed: " & startErr)
  msquicSafe:
    msquicdrv.markStreamStartComplete(state)
  let stream = newMsQuicStream(
    state,
    conn.handle,
    dir,
    peerId = conn.peerId,
    protocol = conn.protocol,
    # Each QUIC substream negotiates its own protocol. Sharing one gate across
    # the whole connection can deadlock bidirectional identify/DM handshakes.
    protocolGate = newAsyncLock()
  )
  if not conn.bandwidthManager.isNil:
    stream.setBandwidthManager(conn.bandwidthManager)
  conn.registerManagedStream(
    res.stream,
    state,
    role = if unidirectional: "outbound_unidi" else: "outbound_bidi",
    wrapper = stream
  )
  stream

proc adoptMsQuicStream*(
    conn: MsQuicConnection,
    streamPtr: pointer,
    unidirectional: bool
): MsQuicStream {.gcsafe.} =
  if conn.isNil or conn.handle.isNil or conn.connState.isNil:
    raise msquicConnError("MsQuic connection not initialised")
  if streamPtr.isNil:
    raise msquicConnError("MsQuic adoptStream requires valid pointer")
  var res: tuple[
    state: Option[msquicdrv.MsQuicStreamState],
    error: string
  ]
  msquicSafe:
    res = msquicdrv.adoptStream(
      conn.handle,
      streamPtr,
      conn.connState
    )
  if res.error.len > 0 or res.state.isNone:
    raise msquicConnError(
      "MsQuic adoptStream failed: " & (if res.error.len > 0: res.error else: "unknown error")
    )
  let state = res.state.get()
  let stream = newMsQuicStream(
    state,
    conn.handle,
    if unidirectional: Direction.In else: Direction.In,
    peerId = conn.peerId,
    protocol = conn.protocol,
    # Inbound peer streams must not block on an unrelated outbound stream's
    # protocol negotiation on the same QUIC connection.
    protocolGate = newAsyncLock()
  )
  if not conn.bandwidthManager.isNil:
    stream.setBandwidthManager(conn.bandwidthManager)
  conn.registerManagedStream(
    streamPtr,
    state,
    role = if unidirectional: "inbound_unidi" else: "inbound_bidi",
    wrapper = stream
  )
  stream

method getWrapped*(conn: MsQuicConnection): Connection {.gcsafe.} =
  conn

method beginProtocolNegotiation*(
    conn: MsQuicConnection
): Future[void] {.gcsafe, async: (raises: [CancelledError]).} =
  if conn.isNil or conn.protocolGate.isNil:
    return
  await conn.protocolGate.acquire()

method endProtocolNegotiation*(conn: MsQuicConnection) {.gcsafe.} =
  conn.releaseProtocolGate()

proc connectionState*(conn: MsQuicConnection): msquicdrv.MsQuicConnectionState {.inline.} =
  if conn.isNil:
    return nil
  conn.connState

proc transportHandle*(conn: MsQuicConnection): msquicdrv.MsQuicTransportHandle {.inline.} =
  if conn.isNil:
    return nil
  conn.handle

method readOnce*(
    conn: MsQuicConnection, pbytes: pointer, nbytes: int
): Future[int] {.async: (raises: [CancelledError, LPStreamError]).} =
  if nbytes <= 0:
    return 0
  conn.ensureStream()
  await msquicdrv.waitStreamStart(conn.streamState)
  if conn.cached.len == 0:
    var chunk: seq[byte]
    var attempt = 0
    while true:
      var readFuture: Future[seq[byte]]
      try:
        msquicSafe:
          readFuture = msquicdrv.readStream(conn.streamState)
        chunk = await readFuture
        break
      except msquicdrv.QuicRuntimeEventQueueClosed as exc:
        raise newLPStreamConnDownError(exc)
      except CancelledError as exc:
        if attempt < MsQuicConnectionReadRetryAttempts and
            conn.shouldRetryCancelledRead():
          inc attempt
          when defined(ohos) or defined(libp2p_msquic_debug):
            warn "MsQuic connection readOnce retrying cancelled read",
              stream = cast[uint64](conn.streamHandle),
              attempt = attempt,
              err = exc.msg
          await sleepAsync(MsQuicConnectionReadRetryDelay)
          continue
        raise msquicConnError("MsQuic read failed: " & exc.msg, exc)
      except CatchableError as exc:
        if exc.msg.contains("Future operation cancelled") and
            attempt < MsQuicConnectionReadRetryAttempts and
            conn.shouldRetryCancelledRead():
          inc attempt
          when defined(ohos) or defined(libp2p_msquic_debug):
            warn "MsQuic connection readOnce retrying spurious read failure",
              stream = cast[uint64](conn.streamHandle),
              attempt = attempt,
              err = exc.msg
          await sleepAsync(MsQuicConnectionReadRetryDelay)
          continue
        raise msquicConnError("MsQuic read failed: " & exc.msg, exc)
      except Exception as exc:
        raise msquicConnError("MsQuic read failed: " & exc.msg, exc)
    if chunk.len == 0:
      raise newLPStreamEOFError()
    when defined(libp2p_msquic_debug):
      let previewLen = min(chunk.len, 64)
      let preview = chunk[0 ..< previewLen]
      warn "MsQuic connection read",
        stream = cast[uint64](conn.streamHandle),
        bytes = chunk.len,
        hex = byteutils.toHex(preview)
    conn.cached = chunk
  let toRead = min(nbytes, conn.cached.len)
  copyMem(pbytes, addr conn.cached[0], toRead)
  if toRead < conn.cached.len:
    conn.cached.delete(0, toRead - 1)
  else:
    conn.cached.setLen(0)
  connection.libp2p_network_bytes.inc(toRead.int64, labelValues = ["in"])
  if not conn.bandwidthManager.isNil and conn.peerId.len > 0 and toRead > 0:
    try:
      await conn.bandwidthManager.awaitLimit(conn.peerId, Direction.In, toRead, conn.protocol)
      conn.bandwidthManager.record(conn.peerId, Direction.In, toRead, conn.protocol)
    except Exception as exc:
      raise msquicConnError("bandwidth manager failed: " & exc.msg, exc)
  toRead

method write*(
    conn: MsQuicConnection, bytes: seq[byte]
){.async: (raises: [CancelledError, LPStreamError]).} =
  if bytes.len == 0:
    return
  conn.ensureStream()
  await msquicdrv.waitStreamStart(conn.streamState)
  if not conn.bandwidthManager.isNil and conn.peerId.len > 0:
    try:
      await conn.bandwidthManager.awaitLimit(conn.peerId, Direction.Out, bytes.len, conn.protocol)
    except Exception as exc:
      raise msquicConnError("bandwidth manager failed: " & exc.msg, exc)
  var err = ""
  try:
    msquicSafe:
      err = await msquicdrv.writeStreamAndWait(conn.streamState, bytes)
  except CatchableError as exc:
    err = "MsQuic writeStream raised: " & exc.msg
  except Exception as exc:
    err = "MsQuic writeStream raised: " & exc.msg
  if err.len > 0:
    raise msquicConnError("MsQuic send failed: " & err)
  connection.libp2p_network_bytes.inc(bytes.len.int64, labelValues = ["out"])
  if not conn.bandwidthManager.isNil and conn.peerId.len > 0:
    conn.bandwidthManager.record(conn.peerId, Direction.Out, bytes.len, conn.protocol)

method closeWrite*(conn: MsQuicConnection) {.async: (raises: []).} =
  if conn.isNil or conn.streamState.isNil or conn.streamHandle.isNil:
    return
  try:
    await msquicdrv.waitStreamStart(conn.streamState)
  except CatchableError:
    discard
  try:
    if msquicdrv.beginSendFin(conn.streamState):
      let err = await msquicdrv.writeStreamAndWait(conn.streamState, @[], flags = 0x0004'u32)
      if err.len > 0:
        discard msquicdrv.shutdownStream(
          conn.handle,
          conn.streamHandle,
          state = conn.streamState
        )
  except CatchableError:
    try:
      discard msquicdrv.shutdownStream(
        conn.handle,
        conn.streamHandle,
        state = conn.streamState
      )
    except CatchableError:
      discard

method closeImpl*(conn: MsQuicConnection) {.async: (raises: []).} =
  if conn.isNil:
    return
  when defined(libp2p_msquic_debug):
    warn "MsQuic connection closeImpl begin",
      peerId = conn.peerId,
      hasMonitor = not conn.monitor.isNil,
      monitorFinished = (if conn.monitor.isNil: true else: conn.monitor.finished())
  if not conn.monitor.isNil and not conn.monitor.finished():
    try:
      await conn.monitor.cancelAndWait()
    except CatchableError as exc:
      trace "MsQuic connection monitor cancellation raised", err = exc.msg
  else:
    try:
      await noCancel closeWebtransportStreams(conn)
    except CatchableError as exc:
      trace "MsQuic close webtransport streams raised", err = exc.msg
    await closeManagedStreams(conn)
    shutdownConnection(conn)
  conn.monitor = nil
  await procCall Connection(conn).closeImpl()
  when defined(libp2p_msquic_debug):
    warn "MsQuic connection closeImpl done", peerId = conn.peerId
