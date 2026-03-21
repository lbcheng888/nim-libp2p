## MsQuicConnection：基于 msquicdriver 提供的连接/流状态构建 libp2p Connection。
##
## 当前实现将 MsQuic 连接视作“单路” LPStream，方便尽快在 Transport 层
## 验证拨号链路；后续可在此基础上并行推进 WebTransport/多路复用语义。

when not defined(libp2p_msquic_experimental):
  {.error: "MsQuicConnection requires -d:libp2p_msquic_experimental".}

import std/[options, sequtils, deques]
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

template msquicSafe(body: untyped) =
  {.cast(gcsafe).}:
    body

type
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
    nativeMuxActive*: bool
    shadowStreams: seq[(pointer, msquicdrv.MsQuicStreamState)]
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

proc promotePendingPeerStream(conn: MsQuicConnection) {.gcsafe.}

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

proc detachPrimaryStreamState*(
    conn: MsQuicConnection
): tuple[state: msquicdrv.MsQuicStreamState, localInitiated: bool] {.gcsafe.} =
  if conn.isNil or conn.streamState.isNil:
    return (nil, false)
  result.state = conn.streamState
  result.localInitiated = msquicdrv.isLocalInitiated(conn.streamState)
  conn.streamHandle = nil
  conn.streamState = nil
  conn.cached.setLen(0)

proc closePrimaryStream(conn: MsQuicConnection) {.gcsafe, raises: [].} =
  if conn.streamHandle.isNil:
    return
  try:
    msquicSafe:
      msquicdrv.closeStream(conn.handle, conn.streamHandle, conn.streamState)
  except Exception as exc:
    trace "MsQuic closeStream raised", err = exc.msg
  conn.streamHandle = nil
  conn.streamState = nil
  conn.cached.setLen(0)

proc closeShadowStreams(conn: MsQuicConnection) {.gcsafe, raises: [].} =
  if conn.isNil or conn.shadowStreams.len == 0:
    return
  for (streamHandle, streamState) in conn.shadowStreams:
    if streamHandle.isNil:
      continue
    try:
      msquicSafe:
        msquicdrv.closeStream(conn.handle, streamHandle, streamState)
    except Exception as exc:
      trace "MsQuic close shadow stream raised", err = exc.msg
  conn.shadowStreams.setLen(0)

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
        trace "MsQuic connection established", resumed = event.sessionResumed
      of msquicdrv.qcePeerStreamStarted:
        conn.promotePendingPeerStream()
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
    closePrimaryStream(conn)
    closeShadowStreams(conn)
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
    observed: Opt[MultiAddress] = Opt.none(MultiAddress),
    local: Opt[MultiAddress] = Opt.none(MultiAddress),
    onClosed: proc(conn: MsQuicConnection): Future[void] {.gcsafe.} = nil
): MsQuicConnection {.gcsafe.} =
  if handle.isNil or connHandle.isNil or connState.isNil:
    raise msquicConnError("MsQuic connection requires valid handle/state")
  var streamHandle: pointer
  var streamStateOpt: Option[msquicdrv.MsQuicStreamState]
  var streamErr = ""
  if not primaryStream.isNil:
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
  if streamErr.len > 0 or streamStateOpt.isNone or streamHandle.isNil:
    raise msquicConnError("MsQuic stream unavailable: " & streamErr)
  let streamState = streamStateOpt.get()
  when defined(libp2p_msquic_debug):
    let origin = if primaryStream.isNil: "local" else: "peer"
    warn "MsQuic connection stream selected",
      origin = origin,
      stream = cast[uint64](streamHandle)
  if primaryStream.isNil:
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
    nativeMuxActive: false,
    shadowStreams: @[],
    onClosed: onClosed,
    registered: false
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
    protocol = conn.protocol
  )
  if not conn.bandwidthManager.isNil:
    stream.setBandwidthManager(conn.bandwidthManager)
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
    protocol = conn.protocol
  )
  if not conn.bandwidthManager.isNil:
    stream.setBandwidthManager(conn.bandwidthManager)
  stream

method getWrapped*(conn: MsQuicConnection): Connection =
  conn

proc connectionState*(conn: MsQuicConnection): msquicdrv.MsQuicConnectionState {.inline.} =
  if conn.isNil:
    return nil
  conn.connState

proc transportHandle*(conn: MsQuicConnection): msquicdrv.MsQuicTransportHandle {.inline.} =
  if conn.isNil:
    return nil
  conn.handle

proc promotePendingPeerStream(conn: MsQuicConnection) {.gcsafe.} =
  if conn.isNil or conn.nativeMuxActive or conn.isWebtransport or
      conn.connState.isNil or conn.streamState.isNil:
    return
  if not msquicdrv.isLocalInitiated(conn.streamState):
    return

  while true:
    let pendingOpt = block:
      var tmp: Option[msquicdrv.MsQuicStreamState]
      msquicSafe:
        tmp = msquicdrv.popPendingStreamState(conn.connState)
      tmp
    if pendingOpt.isNone:
      return
    let pending = pendingOpt.get()
    if pending.isNil or pending.stream.isNil:
      continue

    let idRes = block:
      var tmp: Result[uint64, string]
      msquicSafe:
        tmp = msquicdrv.streamId(pending)
      tmp
    if idRes.isOk and ((idRes.get() and 0x2'u64) != 0'u64):
      try:
        msquicSafe:
          msquicdrv.closeStream(conn.handle, cast[pointer](pending.stream), pending)
      except Exception as exc:
        trace "MsQuic close unexpected unidirectional peer stream raised", err = exc.msg
      continue

    let previousHandle = conn.streamHandle
    let previousState = conn.streamState
    if not previousHandle.isNil and not previousState.isNil:
      conn.shadowStreams.add((previousHandle, previousState))
    conn.streamHandle = cast[pointer](pending.stream)
    conn.streamState = pending
    conn.cached.setLen(0)
    if not previousState.isNil:
      msquicSafe:
        msquicdrv.handoffReadWaiters(previousState, pending)
    when defined(libp2p_msquic_debug):
      warn "MsQuic connection promoted peer stream as primary",
        stream = cast[uint64](conn.streamHandle),
        shadowCount = conn.shadowStreams.len
    return


method readOnce*(
    conn: MsQuicConnection, pbytes: pointer, nbytes: int
): Future[int] {.async: (raises: [CancelledError, LPStreamError]).} =
  if nbytes <= 0:
    return 0
  conn.ensureStream()
  conn.promotePendingPeerStream()
  await msquicdrv.waitStreamStart(conn.streamState)
  if conn.cached.len == 0:
    var chunk: seq[byte]
    var readFuture: Future[seq[byte]]
    try:
      msquicSafe:
        readFuture = msquicdrv.readStream(conn.streamState)
      chunk = await readFuture
    except msquicdrv.QuicRuntimeEventQueueClosed as exc:
      raise newLPStreamConnDownError(exc)
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
  conn.promotePendingPeerStream()
  await msquicdrv.waitStreamStart(conn.streamState)
  if not conn.bandwidthManager.isNil and conn.peerId.len > 0:
    try:
      await conn.bandwidthManager.awaitLimit(conn.peerId, Direction.Out, bytes.len, conn.protocol)
    except Exception as exc:
      raise msquicConnError("bandwidth manager failed: " & exc.msg, exc)
  var err = ""
  try:
    msquicSafe:
      err = msquicdrv.writeStream(conn.streamState, bytes)
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
    closePrimaryStream(conn)
    closeShadowStreams(conn)
    shutdownConnection(conn)
  conn.monitor = nil
  await procCall Connection(conn).closeImpl()
  when defined(libp2p_msquic_debug):
    warn "MsQuic connection closeImpl done", peerId = conn.peerId
