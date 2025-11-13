## MsQuic 流适配：桥接 msquicdriver 的事件队列与 LPStream 接口。

import std/[sequtils, strformat]
import results
import chronos, chronicles

import ../peerid
import ../stream/lpstream
from ../stream/connection import libp2p_network_bytes
import ../bandwidthmanager
import ./msquicdriver as msquicdrv
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
    bandwidth*: BandwidthManager
    cached: seq[byte]
    activity: bool

const
  MsQuicStreamReadChunk = 16 * 1024

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
    protocol: string = ""
): MsQuicStream =
  if state.isNil or handle.isNil:
    raise msquicStreamError("MsQuic stream requires valid state/handle")
  result = MsQuicStream(
    state: state,
    handle: handle,
    peerId: peerId,
    protocol: protocol,
    cached: @[]
  )
  result.dir = dir
  result.objName = "MsQuicStream"
  result.initStream()

proc setBandwidthManager*(stream: MsQuicStream; manager: BandwidthManager) =
  stream.bandwidth = manager

proc ensureOpen(stream: MsQuicStream) =
  if stream.isNil or stream.state.isNil:
    raise newLPStreamConnDownError()

method getWrapped*(stream: MsQuicStream): LPStream =
  stream

method readOnce*(
    stream: MsQuicStream, pbytes: pointer, nbytes: int
): Future[int] {.async: (raises: [CancelledError, LPStreamError]).} =
  if nbytes <= 0:
    return 0
  stream.ensureOpen()
  if stream.cached.len == 0:
    var chunk: seq[byte]
    try:
      chunk = await msquicdrv.readStream(stream.state)
    except msquicdrv.MsQuicEventQueueClosed as exc:
      raise newLPStreamConnDownError(exc)
    except CatchableError as exc:
      raise msquicStreamError("MsQuic read failed: " & exc.msg, exc)
    if chunk.len == 0:
      raise newLPStreamEOFError()
    stream.cached = chunk
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
    let chunk = stream.cached
    stream.cached = @[]
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
  if not stream.bandwidth.isNil and stream.peerId.len > 0:
    await stream.bandwidth.awaitLimit(stream.peerId, Direction.Out, bytes.len, stream.protocol)
  let err =
    try:
      var message = ""
      msquicSafe:
        message = msquicdrv.writeStream(stream.state, bytes)
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

method closeWrite*(stream: MsQuicStream) {.async: (raises: []).} =
  stream.closeState()

method closeImpl*(stream: MsQuicStream) {.async: (raises: []).} =
  stream.closeState()

proc sendFin*(stream: MsQuicStream) {.async: (raises: [LPStreamError]).} =
  stream.ensureOpen()
  var errMsg = ""
  try:
    msquicSafe:
      errMsg = msquicdrv.writeStream(stream.state, @[], flags = 0x0004'u32)
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
