# Nim-LibP2P
# Copyright (c)
# Licensed under either of
#  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
#  * MIT license ([LICENSE-MIT](LICENSE-MIT))
# at your option.
# This file may not be copied, modified, or distributed except according to
# those terms.

{.push raises: [].}

import std/[json, options, tables, sequtils, strutils]
import pkg/[chronicles, chronos, results, stew/base58]
import chronos/apps/http/[httpserver, httpclient]

import
  ./transport,
  ../multiaddress,
  ../multicodec,
  ../peerid,
  ../bandwidthmanager,
  ../stream/connection,
  ../upgrademngrs/upgrade,
  ../wire

when not defined(libp2p_quic_support):
  {.error: "WebRTC direct transport requires -d:libp2p_quic_support".}

# The libdatachannel Nim bindings do not expose the complete C surface yet.
# We reuse their base definitions and extend the missing procedures here.
import libdatachannel/[bindings, peerconnection]

proc bytesToString(bytes: openArray[byte]): string =
  ## Convert byte buffer into string preserving binary content.
  result = newString(bytes.len)
  if bytes.len > 0:
    copyMem(result.cstring, unsafeAddr bytes[0], bytes.len)

const
  MaxSdpSize = 8192
  DefaultDataChannelLabel = "libp2p"
  DefaultGatherTimeout = 5.seconds
  DefaultHandshakeTimeout = 15.seconds
  DefaultMaxMessageSize = 16_384
  webRtcHttpCodec = multiCodec("http")
  webRtcLegacyCodec = multiCodec("p2p-webrtc-direct")
  webRtcDirectCodec = multiCodec("webrtc-direct")
  certhashCodec = multiCodec("certhash")

type
  WebRtcIceServer* = object
    urls*: seq[string]
    username*: string
    credential*: string

  WebRtcDirectConfig* = object
    iceServers*: seq[WebRtcIceServer]
    bindAddress*: string
    portRange*: tuple[beginning, ending: uint16]
    enableIceTcp*: bool
    gatherTimeout*: Duration
    handshakeTimeout*: Duration
    maxMessageSize*: int
    mtu*: int
    transportPolicy*: RtcTransportPolicy

  WebRtcSessionKind = enum
    wsskDialer
    wsskListener

  SessionDescription = object
    sdp: string
    kind: string

  WebRtcSession = ref object
    transport: WebRtcDirectTransport
    pc: PeerConnection
    kind: WebRtcSessionKind
    gatherEvent: AsyncEvent
    gatherDone: bool
    channelReady: Future[WebRtcStream]
    gatherError: string
    localAddr: Opt[MultiAddress]
    remoteAddr: Opt[MultiAddress]
    dir: Direction
    timer: Future[void]

  WebRtcStream* = ref object of Connection
    session: WebRtcSession
    dcId: cint
    queue: AsyncQueue[seq[byte]]
    cached: seq[byte]
    openEvent: AsyncEvent
    closedEvent: AsyncEvent
    closed: bool
    maxMessageSize: int

  WebRtcDirectTransport* = ref object of Transport
    cfg: WebRtcDirectConfig
    httpServers: seq[HttpServerRef]
    acceptQueue: AsyncQueue[WebRtcStream]
    sessions: Table[cint, WebRtcSession]
    channels: Table[cint, WebRtcStream]
    listenAddrs: seq[MultiAddress]

  WebRtcDirectError* = object of transport.TransportError

# ------------------------- libdatachannel bindings -------------------------

proc rtcCreateOffer(pc: cint, buffer: cstring, size: cint): cint {.importc,
    dynlib: libdatachannel_dll.}
proc rtcCreateAnswer(pc: cint, buffer: cstring, size: cint): cint {.importc,
    dynlib: libdatachannel_dll.}
proc rtcGetLocalDescription(pc: cint, buffer: cstring, size: cint): cint {.importc,
    dynlib: libdatachannel_dll.}
proc rtcGetLocalDescriptionType(pc: cint, buffer: cstring, size: cint): cint {.
    importc, dynlib: libdatachannel_dll.}
proc rtcGetRemoteDescription(pc: cint, buffer: cstring, size: cint): cint {.importc,
    dynlib: libdatachannel_dll.}
proc rtcSetMessageCallback*(id: cint, cb: rtcMessageCallbackFunc): cint {.importc,
    dynlib: libdatachannel_dll.}

# ------------------------------ utilities ----------------------------------

logScope:
  topics = "libp2p webrtcdirect"

proc defaultConfig*(): WebRtcDirectConfig =
  WebRtcDirectConfig(
    iceServers: @[],
    bindAddress: "",
    portRange: (0'u16, 0'u16),
    enableIceTcp: false,
    gatherTimeout: DefaultGatherTimeout,
    handshakeTimeout: DefaultHandshakeTimeout,
    maxMessageSize: DefaultMaxMessageSize,
    mtu: 0,
    transportPolicy: RtcTransportPolicy.RTC_TRANSPORT_POLICY_ALL,
  )


proc encodeSignal(desc: SessionDescription): string =
  let payload = %*{"type": desc.kind, "sdp": desc.sdp}
  let raw = $payload
  var rawBytes = newSeq[byte](raw.len)
  if raw.len > 0:
    copyMem(addr rawBytes[0], raw.cstring, raw.len)
  base58.encode(Base58, rawBytes)

proc decodeSignal(encoded: string): Result[SessionDescription, string] =
  var decoded: seq[byte]
  try:
    decoded = base58.decode(Base58, encoded)
  except Base58Error as exc:
    return err("base58 decode failed: " & exc.msg)
  let text = bytesToString(decoded)
  let node =
    try:
      parseJson(text)
    except CatchableError as exc:
      return err("invalid signal payload: " & exc.msg)
  let typeNode = node.getOrDefault("type")
  let sdpNode = node.getOrDefault("sdp")
  if typeNode.isNil or sdpNode.isNil or
      typeNode.kind != JsonNodeKind.JString or sdpNode.kind != JsonNodeKind.JString:
    return err("missing type or sdp")
  ok(SessionDescription(sdp: sdpNode.getStr, kind: typeNode.getStr))

proc toRtcConfiguration(
    cfg: WebRtcDirectConfig
): rtcConfiguration =
  var collected: seq[string] = @[]
  for srv in cfg.iceServers:
    for url in srv.urls:
      if url.len > 0:
        collected.add(url)

  let iceArray =
    if collected.len > 0: allocCStringArray(collected) else: nil

  rtcConfiguration(
    iceServers: iceArray,
    iceServersCount: if iceArray == nil: 0 else: collected.len.cint,
    bindAddress:
      if cfg.bindAddress.len > 0: cfg.bindAddress.cstring else: nil,
    certificateType: RtcCertificateType.RTC_CERTIFICATE_DEFAULT,
    iceTransportPolicy: cfg.transportPolicy,
    enableIceTcp: cfg.enableIceTcp,
    disableAutoNegotiation: false,
    portRangeBegin: cfg.portRange.beginning,
    portRangeEnd: cfg.portRange.ending,
    mtu: cfg.mtu.cint,
    maxMessageSize: cfg.maxMessageSize.cint,
  )

proc initStream(
    stream: WebRtcStream,
    session: WebRtcSession,
    dcId: cint,
    dir: Direction,
    observed: Opt[MultiAddress],
    local: Opt[MultiAddress],
    maxMessageSize: int,
) =
  stream.session = session
  stream.dcId = dcId
  stream.queue = newAsyncQueue[seq[byte]]()
  stream.cached = @[]
  stream.openEvent = newAsyncEvent()
  stream.closedEvent = newAsyncEvent()
  stream.closed = false
  stream.maxMessageSize = maxMessageSize

  stream.peerId = default(PeerId)
  stream.transportDir = dir
  stream.observedAddr = observed
  stream.localAddr = local
  stream.timeout = DefaultConnectionTimeout
  stream.timeoutHandler = nil

  procCall Connection(stream).initStream()

# --------------------------- callbacks helpers -----------------------------

proc streamFromPtr(rawPtr: pointer): WebRtcStream =
  cast[WebRtcStream](rawPtr)

proc sessionFromPtr(rawPtr: pointer): WebRtcSession =
  cast[WebRtcSession](rawPtr)

proc handleStreamClosed(stream: WebRtcStream) =
  if stream.closed:
    return
  stream.closed = true
  proc finalizeClose {.async.} =
    try:
      await stream.queue.put(@[])
    except CancelledError:
      discard
    stream.closedEvent.fire()

  asyncSpawn(finalizeClose())

proc channelOpenCallback(dc: cint, userPtr: pointer) =
  let stream = streamFromPtr(userPtr)
  stream.openEvent.fire()

proc channelClosedCallback(dc: cint, userPtr: pointer) =
  let stream = streamFromPtr(userPtr)
  handleStreamClosed(stream)

proc channelErrorCallback(dc: cint, error: cstring, userPtr: pointer) =
  let stream = streamFromPtr(userPtr)
  trace "WebRTC DataChannel error", dc, err = $error
  handleStreamClosed(stream)

proc channelMessageCallback(
    dc: cint, message: cstring, size: cint, userPtr: pointer
) =
  if size <= 0:
    return
  let stream = streamFromPtr(userPtr)
  if stream.closed:
    return
  var data = newSeq[byte](size)
  copyMem(addr data[0], message, size)
  try:
    stream.queue.addLastNoWait(data)
  except AsyncQueueFullError:
    warn "WebRTC DataChannel queue full, dropping message", dc, size

proc dataChannelArrived(pc: cint, dc: cint, userPtr: pointer) =
  let session = sessionFromPtr(userPtr)
  if not session.transport.sessions.hasKey(pc):
    trace "DataChannel for unknown session", pc
    return

  let stream = WebRtcStream(
    session: session,
    dcId: dc,
    queue: newAsyncQueue[seq[byte]](),
    cached: @[],
    openEvent: newAsyncEvent(),
    closedEvent: newAsyncEvent(),
    closed: false,
    maxMessageSize: session.transport.cfg.maxMessageSize,
  )

  initStream(
    stream,
    session,
    dc,
    session.dir,
    session.remoteAddr,
    session.localAddr,
    session.transport.cfg.maxMessageSize,
  )

  rtcSetUserPointer(dc, cast[pointer](stream))
  discard rtcSetOpenCallback(dc, channelOpenCallback)
  discard rtcSetClosedCallback(dc, channelClosedCallback)
  discard rtcSetErrorCallback(dc, channelErrorCallback)
  discard rtcSetMessageCallback(dc, channelMessageCallback)

  if not session.channelReady.finished:
    session.channelReady.complete(stream)

proc iceStateCallback(pc: cint, state: rtcState, userPtr: pointer) =
  let session = sessionFromPtr(userPtr)
  trace "ICE state update", pc, state = state.int
  if state == rtcState.RTC_FAILED:
    session.gatherError = "ICE negotiation failed"
    session.gatherEvent.fire()
  elif state == rtcState.RTC_CONNECTED:
    session.gatherEvent.fire()

proc gatheringStateCallback(
    pc: cint, state: rtcGatheringState, userPtr: pointer
) =
  let session = sessionFromPtr(userPtr)
  if session.gatherDone:
    return
  if state == rtcGatheringState.RTC_GATHERING_COMPLETE:
    session.gatherDone = true
    session.gatherEvent.fire()

# ------------------------------ WebRTC stream ------------------------------

method getWrapped*(stream: WebRtcStream): Connection =
  nil

method readOnce*(
    stream: WebRtcStream, pbytes: pointer, nbytes: int
): Future[int] {.async: (raises: [CancelledError, LPStreamError]).} =
  if stream.closed and stream.cached.len == 0:
    raise newLPStreamEOFError()

  if stream.cached.len == 0:
    try:
      stream.cached = await stream.queue.popFirst()
    except AsyncQueueEmptyError:
      raise newLPStreamEOFError()
    except CancelledError as exc:
      raise (ref LPStreamError)(msg: "read cancelled", parent: exc)

  let amount = min(nbytes, stream.cached.len)
  if amount == 0:
    if stream.closed:
      raise newLPStreamEOFError()
    return 0

  copyMem(pbytes, addr stream.cached[0], amount)
  if amount == stream.cached.len:
    stream.cached.setLen(0)
  else:
    stream.cached = stream.cached[amount .. ^1]

  if not stream.bandwidthManager.isNil and stream.peerId.len > 0:
    stream.bandwidthManager.record(
      stream.peerId, Direction.In, amount, stream.protocol
    )
  amount

method write*(
    stream: WebRtcStream, data: seq[byte]
): Future[void] {.async: (raises: [CancelledError, LPStreamError]).} =
  if stream.closed:
    raise newLPStreamEOFError()

  if data.len == 0:
    return

  var offset = 0
  while offset < data.len:
    let chunk = min(stream.maxMessageSize, data.len - offset)
    let res = rtcSendMessage(
      stream.dcId, cast[cstring](unsafeAddr data[offset]), chunk.cint
    )
    if res != chunk.cint:
      raise
        (ref LPStreamError)(
          msg: "failed sending WebRTC message, code=" & $res
        )
    offset += chunk

  if not stream.bandwidthManager.isNil and stream.peerId.len > 0:
    stream.bandwidthManager.record(
      stream.peerId, Direction.Out, data.len, stream.protocol
    )

method closeImpl*(stream: WebRtcStream): Future[void] {.async: (raises: []).} =
  handleStreamClosed(stream)
  if not stream.session.isNil:
    try:
      discard rtcDeleteDataChannel(stream.dcId)
    except CatchableError:
      discard
  await procCall Connection(stream).closeImpl()

# ------------------------------ session utils ------------------------------

proc cancelTimer(session: WebRtcSession) =
  if not session.timer.isNil:
    session.timer.cancelSoon()
    session.timer = nil

proc closePeer(session: WebRtcSession) =
  if session.isNil:
    return
  cancelTimer(session)
  try:
    session.pc.close()
  except CatchableError as exc:
    trace "Failed closing PeerConnection", err = exc.msg

proc makeSession(
    transport: WebRtcDirectTransport,
    kind: WebRtcSessionKind,
    dir: Direction,
    localAddr: Opt[MultiAddress],
    remoteAddr: Opt[MultiAddress],
): Result[WebRtcSession, string] =
  let rtcCfg = toRtcConfiguration(transport.cfg)
  var pc: PeerConnection
  try:
    pc = newPeerConnection(rtcCfg)
  except CatchableError as exc:
    return err("failed to create PeerConnection: " & exc.msg)

  let session = WebRtcSession(
    transport: transport,
    pc: pc,
    kind: kind,
    gatherEvent: newAsyncEvent(),
    gatherDone: false,
    channelReady: newFuture[WebRtcStream]("webrtc.channel"),
    gatherError: "",
    localAddr: localAddr,
    remoteAddr: remoteAddr,
    dir: dir,
    timer: nil,
  )

  transport.sessions[pc.id] = session
  rtcSetUserPointer(pc.id, cast[pointer](session))
  discard rtcSetGatheringStateChangeCallback(pc.id, gatheringStateCallback)
  discard rtcSetStateChangeCallback(pc.id, iceStateCallback)
  discard rtcSetDataChannelCallback(pc.id, dataChannelArrived)
  ok(session)

proc waitForGather(session: WebRtcSession): Future[void] {.async.} =
  if session.gatherDone:
    return
  let timeout = session.transport.cfg.gatherTimeout
  let fired = await session.gatherEvent.wait().withTimeout(timeout)
  if not fired:
    if session.gatherDone:
      return
    raise newException(
      LPError,
      "WebRTC ICE gathering timeout (" & $timeout & ")"
    )
  if session.gatherError.len > 0:
    raise newException(LPError, session.gatherError)

proc waitForChannel(session: WebRtcSession): Future[WebRtcStream] {.async.} =
  let timeout = session.transport.cfg.handshakeTimeout
  session.timer = sleepAsync(timeout)
  try:
    if not await session.channelReady.withTimeout(timeout):
      raise newException(
        LPError, "WebRTC data channel not opened within " & $timeout
      )
    return await session.channelReady
  finally:
    cancelTimer(session)

proc generateOffer(session: WebRtcSession): Result[SessionDescription, string] =
  var buffer = newString(MaxSdpSize)
  let res = rtcCreateOffer(session.pc.id, buffer.cstring, buffer.len.cint)
  if res < 0:
    return err("rtcCreateOffer failed: " & $res)
  let length = res
  setLen(buffer, length)
  discard rtcSetLocalDescription(session.pc.id, nil)

  var typeBuf = newString(32)
  let typeLen =
    rtcGetLocalDescriptionType(session.pc.id, typeBuf.cstring, typeBuf.len.cint)
  if typeLen < 0:
    return err("rtcGetLocalDescriptionType failed: " & $typeLen)
  setLen(typeBuf, typeLen)
  ok(SessionDescription(sdp: buffer, kind: typeBuf))

proc generateAnswer(
    session: WebRtcSession, offer: SessionDescription
): Result[SessionDescription, string] =
  let setRes = rtcSetRemoteDescription(
    session.pc.id, offer.sdp.cstring, offer.kind.cstring
  )
  if setRes != RTC_ERR_SUCCESS:
    return err("rtcSetRemoteDescription failed: " & $setRes)

  var buffer = newString(MaxSdpSize)
  let res = rtcCreateAnswer(session.pc.id, buffer.cstring, buffer.len.cint)
  if res < 0:
    return err("rtcCreateAnswer failed: " & $res)

  discard rtcSetLocalDescription(session.pc.id, nil)
  setLen(buffer, res)

  var typeBuf = newString(32)
  let typeLen =
    rtcGetLocalDescriptionType(session.pc.id, typeBuf.cstring, typeBuf.len.cint)
  if typeLen < 0:
    return err("rtcGetLocalDescriptionType failed: " & $typeLen)
  setLen(typeBuf, typeLen)
  ok(SessionDescription(sdp: buffer, kind: typeBuf))

proc applyAnswer(session: WebRtcSession, answer: SessionDescription): Result[
    void, string] =
  let res = rtcSetRemoteDescription(
    session.pc.id, answer.sdp.cstring, answer.kind.cstring
  )
  if res != RTC_ERR_SUCCESS:
    return err("rtcSetRemoteDescription failed: " & $res)
  ok()

proc openDialerChannel(session: WebRtcSession): Result[WebRtcStream, string] =
  let dcId = rtcCreateDataChannel(session.pc.id, DefaultDataChannelLabel.cstring)
  if dcId < 0:
    return err("rtcCreateDataChannel failed: " & $dcId)

  let stream = WebRtcStream(
    session: session,
    dcId: dcId,
    queue: newAsyncQueue[seq[byte]](),
    cached: @[],
    openEvent: newAsyncEvent(),
    closedEvent: newAsyncEvent(),
    closed: false,
    maxMessageSize: session.transport.cfg.maxMessageSize,
  )

  initStream(
    stream,
    session,
    dcId,
    session.dir,
    session.remoteAddr,
    session.localAddr,
    session.transport.cfg.maxMessageSize,
  )

  rtcSetUserPointer(dcId, cast[pointer](stream))
  discard rtcSetOpenCallback(dcId, channelOpenCallback)
  discard rtcSetClosedCallback(dcId, channelClosedCallback)
  discard rtcSetErrorCallback(dcId, channelErrorCallback)
  discard rtcSetMessageCallback(dcId, channelMessageCallback)

  if not session.channelReady.finished:
    session.channelReady.complete(stream)
  ok(stream)

# ----------------------------- HTTP handling -------------------------------

proc respondJson(
    request: HttpRequestRef, code: HttpCode, body: string
): Future[HttpResponseRef] {.async.} =
  let headers = HttpTable.init(@[("Content-Type", "text/plain"), ("Access-Control-Allow-Origin", "*")])
  await request.respond(code, body, headers)

proc safeRespondJson(
    request: HttpRequestRef, code: HttpCode, body: string
): Future[HttpResponseRef] {.async: (raises: [CancelledError]).} =
  try:
    let resp = await respondJson(request, code, body)
    return resp
  except CatchableError as exc:
    trace "Failed to send HTTP response", err = exc.msg
    let response = request.getResponse()
    response.status = Http500
    response

proc parseSignalParameter(request: HttpRequestRef): Result[string, string] =
  let values = request.query.getList("signal")
  if values.len > 0:
    return ok(values[0])
  err("missing signal query parameter")

proc handleListenerRequest(
    self: WebRtcDirectTransport,
    listenMa: MultiAddress,
    request: HttpRequestRef,
): Future[HttpResponseRef] {.async: (raises: [CancelledError]).} =
  var session: WebRtcSession = nil
  try:
    if request.meth != MethodGet:
      return await safeRespondJson(request, Http405, "method not allowed")
    let signalParam = parseSignalParameter(request).valueOr:
      return await safeRespondJson(request, Http400, "missing signal parameter")

    let offer = decodeSignal(signalParam).valueOr:
      return await safeRespondJson(
        request, Http400, "invalid signal payload: " & error
      )

    let sessionRes = makeSession(
      self, WebRtcSessionKind.wsskListener, Direction.In,
      Opt.some(listenMa), Opt.none(MultiAddress)
    )
    if sessionRes.isErr():
      return await safeRespondJson(request, Http500, sessionRes.error)
    session = sessionRes.get()

    let answer = generateAnswer(session, offer).valueOr:
      session.closePeer()
      return await safeRespondJson(request, Http500, error)

    let applyRes = applyAnswer(session, answer)
    if applyRes.isErr():
      session.closePeer()
      return await safeRespondJson(request, Http500, applyRes.error)

    proc listenerFlow {.async.} =
      try:
        await waitForGather(session)
        let stream = await waitForChannel(session)
        await self.acceptQueue.addLast(stream)
      except CatchableError as exc:
        trace "Listener session failed", err = exc.msg
        session.closePeer()

    asyncSpawn(listenerFlow())

    return await safeRespondJson(request, Http200, encodeSignal(answer))
  except CatchableError as exc:
    if not session.isNil:
      session.closePeer()
    trace "Listener request failed", err = exc.msg
    return await safeRespondJson(request, Http500, "internal server error")

proc httpHandler(
    self: WebRtcDirectTransport, listenMa: MultiAddress
): HttpProcessCallback2 =
  proc handler(reqFence: RequestFence): Future[HttpResponseRef] {.
      async: (raises: [CancelledError]).} =
    if reqFence.isErr():
      return defaultResponse(reqFence.error())
    let request = reqFence.get()
    await self.handleListenerRequest(listenMa, request)
  handler

# ---------------------------- transport methods ----------------------------

proc new*(
    _: type WebRtcDirectTransport,
    upgrade: Upgrade,
    _: PrivateKey,
    cfg: WebRtcDirectConfig = defaultConfig(),
): WebRtcDirectTransport =
  let transport = WebRtcDirectTransport(
    cfg: cfg,
    httpServers: @[],
    acceptQueue: newAsyncQueue[WebRtcStream](),
    sessions: initTable[cint, WebRtcSession](),
    channels: initTable[cint, WebRtcStream](),
    listenAddrs: @[],
  )
  procCall Transport(transport).initialize()
  transport.upgrader = upgrade
  transport.networkReachability = NetworkReachability.Reachable
  transport

method handles*(
    self: WebRtcDirectTransport, address: MultiAddress
): bool {.raises: [].} =
  if not procCall Transport(self).handles(address):
    return false
  let protocols = address.protocols.valueOr:
    return false
  result = protocols.anyIt(
    it == webRtcDirectCodec or it == webRtcLegacyCodec or it == webRtcHttpCodec
  )

method start*(
    self: WebRtcDirectTransport, addrs: seq[MultiAddress]
): Future[void] {.
    async: (raises: [LPError, transport.TransportError, CancelledError])
.} =
  if self.httpServers.len > 0:
    return

  for addr in addrs:
    let tAddress = addr.initTAddress().valueOr:
      raise (ref WebRtcDirectError)(
        msg: "invalid listen address: " & error, parent: nil
      )
    let handler = self.httpHandler(addr)
    let serverRes = HttpServerRef.new(
      address = tAddress,
      processCallback = handler,
      serverFlags = {HttpServerFlags.Http11Pipeline},
    )
    if serverRes.isErr():
      raise (ref WebRtcDirectError)(
        msg: "failed to create HTTP server: " & serverRes.error(), parent: nil
      )
    let server = serverRes.get()
    server.start()
    self.httpServers.add(server)

  self.listenAddrs = addrs
  await procCall Transport(self).start(addrs)

method stop*(self: WebRtcDirectTransport) {.async: (raises: []).} =
  for server in self.httpServers:
    try:
      await server.stop()
      await server.closeWait()
    except CatchableError:
      discard
  self.httpServers.setLen(0)
  await procCall Transport(self).stop()

method accept*(
    self: WebRtcDirectTransport
): Future[Connection] {.
    async: (raises: [transport.TransportError, CancelledError])
.} =
  try:
    let stream = await self.acceptQueue.popFirst()
    return stream
  except AsyncQueueEmptyError:
    raise newException(transport.TransportError, "Transport closed")

proc httpSignalUrl(address: MultiAddress): Result[string, string] =
  var host = ""
  var port = ""
  for partRes in address.items():
    let part = ?partRes
    let code = ?part.protoCode()
    let partStr = part.toString().valueOr("")
    let segments = partStr.split('/')
    let arg =
      if segments.len > 0: segments[^1]
      else: ""
    case code
    of multiCodec("ip4"), multiCodec("ip6"), multiCodec("dns4"),
        multiCodec("dns6"), multiCodec("dnsaddr"):
      host = arg
    of multiCodec("tcp"):
      port = arg
    else:
      discard
  if host.len == 0 or port.len == 0:
    return err("address missing host or port: " & $address)
  ok("http://" & host & ":" & port & "/?signal=")

method dial*(
    self: WebRtcDirectTransport,
    hostname: string,
    address: MultiAddress,
    peerId: Opt[PeerId] = Opt.none(PeerId),
): Future[Connection] {.
    async: (raises: [transport.TransportError, CancelledError])
.} =
  let url = httpSignalUrl(address).valueOr:
    raise (ref WebRtcDirectError)(
      msg: "unsupported multiaddress: " & error, parent: nil
    )

  let sessionRes = makeSession(
    self, WebRtcSessionKind.wsskDialer, Direction.Out,
    Opt.none(MultiAddress), Opt.some(address)
  )
  if sessionRes.isErr():
    raise (ref WebRtcDirectError)(msg: sessionRes.error, parent: nil)
  let session = sessionRes.get()

  discard openDialerChannel(session).valueOr:
    session.closePeer()
    raise (ref WebRtcDirectError)(msg: error, parent: nil)

  let offer = generateOffer(session).valueOr:
    session.closePeer()
    raise (ref WebRtcDirectError)(msg: error, parent: nil)

  let encoded = encodeSignal(offer)
  let httpSession = HttpSessionRef.new()
  let request = HttpClientRequestRef.get(httpSession, url & encoded).valueOr:
    session.closePeer()
    raise (ref WebRtcDirectError)(
      msg: "failed to create HTTP request", parent: nil
    )

  let response =
    try:
      await request.send()
    except HttpError as exc:
      session.closePeer()
      raise (ref WebRtcDirectError)(
        msg: "WebRTC HTTP signaling failed: " & exc.msg, parent: exc
      )

  let status = response.status
  if status != Http200.int:
    session.closePeer()
    raise (ref WebRtcDirectError)(
      msg: "remote rejected offer: status=" & $status, parent: nil
    )

  let answerBytes =
    try:
      await response.getBodyBytes()
    except HttpError as exc:
      session.closePeer()
      raise (ref WebRtcDirectError)(
        msg: "failed to read HTTP response body: " & exc.msg, parent: exc
      )
  let answerText = bytesToString(answerBytes)
  let answer = decodeSignal(answerText).valueOr:
    session.closePeer()
    raise (ref WebRtcDirectError)(msg: error, parent: nil)

  let applyResult = applyAnswer(session, answer)
  if applyResult.isErr():
    session.closePeer()
    raise (ref WebRtcDirectError)(msg: applyResult.error, parent: nil)

  try:
    await waitForGather(session)
    let stream = await waitForChannel(session)
    return stream
  except CatchableError as exc:
    session.closePeer()
    raise (ref WebRtcDirectError)(msg: exc.msg, parent: exc)

{.pop.}
