## MsQuic 流适配：桥接 msquicdriver 的事件队列与 LPStream 接口。

import std/[sequtils, strformat, strutils]
import results
import chronos, chronicles

import ../peerid
import ../stream/lpstream
from ../stream/connection import libp2p_network_bytes
import ../bandwidthmanager
import ./quicruntime as msquicdrv
template msquicSafe(body: untyped) =
  {.cast(gcsafe).}:
    body

logScope:
  topics = "libp2p msquicstream"

type
  MsQuicStream* = ref object of LPStream
    state: msquicdrv.MsQuicStreamState
    handle: msquicdrv.MsQuicTransportHandle
    peerId: PeerId
    protocol*: string
    protocolGate: AsyncLock
    bandwidth*: BandwidthManager
    cached: seq[byte]
    activity: bool

const
  MsQuicStreamReadChunk = 16 * 1024
  MsQuicReadRetryAttempts = 4
  MsQuicReadRetryDelay = 50.milliseconds

proc msquicStreamError(msg: string; parent: ref Exception = nil): ref LPStreamError =
  (ref LPStreamError)(msg: msg, parent: parent)

template recordNetworkBytes(direction: string; amount: int64) =
  when compiles(libp2p_network_bytes.inc(0'i64, labelValues = ["in"])):
    libp2p_network_bytes.inc(amount, labelValues = [direction])
  else:
    discard

proc newMsQuicStream*(
    state: msquicdrv.MsQuicStreamState,
    handle: msquicdrv.MsQuicTransportHandle,
    dir: Direction,
    peerId: PeerId = PeerId(),
    protocol: string = "",
    protocolGate: AsyncLock = nil
): MsQuicStream =
  if state.isNil or handle.isNil:
    raise msquicStreamError("MsQuic stream requires valid state/handle")
  result = MsQuicStream(
    state: state,
    handle: handle,
    peerId: peerId,
    protocol: protocol,
    protocolGate: protocolGate,
    cached: @[]
  )
  result.dir = dir
  result.objName = "MsQuicStream"
  result.initStream()

proc setBandwidthManager*(stream: MsQuicStream; manager: BandwidthManager) =
  stream.bandwidth = manager

proc takeCachedBytes*(stream: MsQuicStream): seq[byte] {.gcsafe, raises: [].} =
  if stream.isNil or stream.cached.len == 0:
    return @[]
  swap(result, stream.cached)

proc newCachedMsQuicStreamForTests*(payload: seq[byte]): MsQuicStream {.gcsafe, raises: [].} =
  MsQuicStream(cached: payload)

proc restoreCachedBytes*(stream: MsQuicStream; payload: seq[byte]) {.gcsafe, raises: [].} =
  if stream.isNil:
    return
  if payload.len == 0:
    stream.cached.setLen(0)
  elif stream.cached.len == 0:
    stream.cached = payload
  else:
    var merged = newSeqOfCap[byte](payload.len + stream.cached.len)
    merged.add(payload)
    merged.add(stream.cached)
    stream.cached = merged

proc ensureOpen(stream: MsQuicStream) =
  if stream.isNil or stream.state.isNil:
    raise newLPStreamConnDownError()

proc releaseProtocolGate*(stream: MsQuicStream) {.gcsafe, raises: [].} =
  if stream.isNil or stream.protocolGate.isNil:
    return
  try:
    stream.protocolGate.release()
  except AsyncLockError:
    discard

proc shouldRetryCancelledRead(stream: MsQuicStream): bool {.gcsafe, raises: [].} =
  if stream.isNil or stream.state.isNil or stream.state.closed:
    return false
  let connState = stream.state.connectionState
  if connState.isNil or connState.closed:
    return false
  try:
    let handshakeComplete = msquicdrv.connectionHandshakeComplete(connState)
    let closeReason = msquicdrv.connectionCloseReason(connState)
    handshakeComplete and closeReason.len == 0
  except CatchableError:
    false
  except Exception:
    false

proc connectionUsable*(stream: MsQuicStream): bool {.gcsafe, raises: [].} =
  if stream.isNil or stream.state.isNil or stream.state.closed:
    return false
  let connState = stream.state.connectionState
  if connState.isNil or connState.closed:
    return false
  try:
    let handshakeComplete = msquicdrv.connectionHandshakeComplete(connState)
    let closeReason = msquicdrv.connectionCloseReason(connState)
    handshakeComplete and closeReason.len == 0
  except CatchableError:
    false
  except Exception:
    false

proc shouldRetryReadFailure*(
    stream: MsQuicStream,
    errMsg: string
): bool {.gcsafe, raises: [].} =
  if errMsg.len == 0:
    return false
  if errMsg.contains("Future operation cancelled") or
      errMsg.toLowerAscii().contains("cancelled"):
    return stream.shouldRetryCancelledRead()
  false

method getWrapped*(stream: MsQuicStream): LPStream =
  stream

method beginProtocolNegotiation*(
    stream: MsQuicStream
): Future[void] {.gcsafe, async: (raises: [CancelledError]).} =
  if stream.isNil or stream.protocolGate.isNil:
    return
  await stream.protocolGate.acquire()

method endProtocolNegotiation*(stream: MsQuicStream) {.gcsafe.} =
  stream.releaseProtocolGate()

method readOnce*(
    stream: MsQuicStream, pbytes: pointer, nbytes: int
): Future[int] {.async: (raises: [CancelledError, LPStreamError]).} =
  if nbytes <= 0:
    return 0
  stream.ensureOpen()
  if stream.cached.len == 0:
    var chunk: seq[byte]
    var attempt = 0
    while true:
      try:
        when defined(ohos):
          warn "MsQuic readOnce begin",
            stream = cast[uint64](stream.state.stream),
            requested = nbytes,
            cached = stream.cached.len,
            attempt = attempt + 1
        chunk = await msquicdrv.readStream(stream.state)
        when defined(ohos):
          warn "MsQuic readOnce chunk",
            stream = cast[uint64](stream.state.stream),
            bytes = chunk.len
        break
      except msquicdrv.QuicRuntimeEventQueueClosed as exc:
        when defined(ohos):
          warn "MsQuic readOnce queue closed",
            stream = cast[uint64](stream.state.stream),
            err = exc.msg
        raise newLPStreamConnDownError(exc)
      except CancelledError as exc:
        if attempt < MsQuicReadRetryAttempts and stream.shouldRetryCancelledRead():
          inc attempt
          when defined(ohos) or defined(libp2p_msquic_debug):
            warn "MsQuic readOnce retrying cancelled read",
              stream = cast[uint64](stream.state.stream),
              attempt = attempt,
              err = exc.msg
          await sleepAsync(MsQuicReadRetryDelay)
          continue
        when defined(ohos):
          warn "MsQuic readOnce cancelled",
            stream = cast[uint64](stream.state.stream),
            err = exc.msg
        raise msquicStreamError("MsQuic read failed: " & exc.msg, exc)
      except CatchableError as exc:
        if exc.msg.contains("Future operation cancelled") and
            attempt < MsQuicReadRetryAttempts and
            stream.shouldRetryCancelledRead():
          inc attempt
          when defined(ohos) or defined(libp2p_msquic_debug):
            warn "MsQuic readOnce retrying spurious read failure",
              stream = cast[uint64](stream.state.stream),
              attempt = attempt,
              err = exc.msg
          await sleepAsync(MsQuicReadRetryDelay)
          continue
        when defined(ohos):
          warn "MsQuic readOnce failed",
            stream = cast[uint64](stream.state.stream),
            err = exc.msg
        raise msquicStreamError("MsQuic read failed: " & exc.msg, exc)
    if chunk.len == 0:
      when defined(ohos):
        warn "MsQuic readOnce eof",
          stream = cast[uint64](stream.state.stream)
      raise newLPStreamEOFError()
    swap(stream.cached, chunk)
  let toRead = min(nbytes, stream.cached.len)
  copyMem(pbytes, addr stream.cached[0], toRead)
  if toRead < stream.cached.len:
    stream.cached.delete(0, toRead - 1)
  else:
    stream.cached.setLen(0)
  recordNetworkBytes("in", toRead.int64)
  if not stream.bandwidth.isNil and stream.peerId.len > 0 and toRead > 0:
    await stream.bandwidth.awaitLimit(stream.peerId, Direction.In, toRead, stream.protocol)
    stream.bandwidth.record(stream.peerId, Direction.In, toRead, stream.protocol)
  stream.activity = true
  toRead

method read*(
    stream: MsQuicStream
): Future[seq[byte]] {.async: (raises: [CancelledError, LPStreamError]).} =
  stream.ensureOpen()
  if stream.cached.len > 0:
    var chunk: seq[byte] = @[]
    swap(chunk, stream.cached)
    return chunk
  var buf = newSeqUninit[byte](MsQuicStreamReadChunk)
  let readCount = await stream.readOnce(addr buf[0], MsQuicStreamReadChunk)
  if readCount == 0:
    return @[]
  buf.setLen(readCount)
  buf

method write*(
    stream: MsQuicStream, bytes: seq[byte]
) {.async: (raises: [CancelledError, LPStreamError]).} =
  stream.ensureOpen()
  if bytes.len == 0:
    return
  await msquicdrv.waitStreamStart(stream.state)
  if not stream.bandwidth.isNil and stream.peerId.len > 0:
    await stream.bandwidth.awaitLimit(stream.peerId, Direction.Out, bytes.len, stream.protocol)
  let err =
    try:
      var message = ""
      msquicSafe:
        message = await msquicdrv.writeStreamAndWait(stream.state, bytes)
      message
    except CatchableError as exc:
      raise msquicStreamError("MsQuic send raised: " & exc.msg, exc)
    except Exception as exc:
      raise msquicStreamError("MsQuic send raised: " & exc.msg)
  if err.len > 0:
    raise msquicStreamError("MsQuic send failed: " & err)
  if not stream.bandwidth.isNil and stream.peerId.len > 0:
    stream.bandwidth.record(stream.peerId, Direction.Out, bytes.len, stream.protocol)
  recordNetworkBytes("out", bytes.len.int64)
  stream.activity = true

proc closeState(stream: MsQuicStream) =
  if stream.state.isNil:
    return
  var nativeStream: pointer = nil
  when declared(stream.state.stream):
    nativeStream = cast[pointer](stream.state.stream)
  msquicSafe:
    msquicdrv.closeStream(stream.handle, nativeStream, stream.state)
  stream.state = nil

proc closeNow*(stream: MsQuicStream) {.gcsafe, raises: [].} =
  if stream.isNil:
    return
  stream.closeState()

method closeWrite*(stream: MsQuicStream) {.async: (raises: []).} =
  if stream.isNil or stream.state.isNil:
    return
  try:
    await msquicdrv.waitStreamStart(stream.state)
  except CatchableError:
    discard
  try:
    if msquicdrv.beginSendFin(stream.state):
      discard await msquicdrv.writeStreamAndWait(stream.state, @[], flags = 0x0004'u32)
  except CatchableError:
    var nativeStream: pointer = nil
    when declared(stream.state.stream):
      nativeStream = cast[pointer](stream.state.stream)
    try:
      discard msquicdrv.shutdownStream(
        stream.handle,
        nativeStream,
        state = stream.state
      )
    except CatchableError:
      discard

method closeImpl*(stream: MsQuicStream) {.async: (raises: []).} =
  stream.closeNow()

proc sendFin*(stream: MsQuicStream) {.async: (raises: [CancelledError, LPStreamError]).} =
  stream.ensureOpen()
  await msquicdrv.waitStreamStart(stream.state)
  var errMsg = ""
  try:
    msquicSafe:
      errMsg = await msquicdrv.writeStreamAndWait(stream.state, @[], flags = 0x0004'u32)
  except CatchableError as exc:
    raise msquicStreamError("MsQuic send FIN raised: " & exc.msg, exc)
  except Exception as exc:
    raise msquicStreamError("MsQuic send FIN raised: " & exc.msg)
  if errMsg.len > 0:
    raise msquicStreamError("MsQuic send FIN failed: " & errMsg)

proc streamId*(stream: MsQuicStream): Result[uint64, string] =
  if stream.isNil or stream.state.isNil:
    return err("MsQuic stream unavailable")
  var res: Result[uint64, string]
  msquicSafe:
    res = msquicdrv.streamId(stream.state)
  res
