{.used.}

import chronos
import stew/byteutils
import ../libp2p/multistream
import ../libp2p/protocols/protocol
import ../libp2p/stream/connection
import ../libp2p/varint
import ./helpers

{.push raises: [].}

type DuplicateHandshakeStream = ref object of Connection
  step*: int

type CoalescedListenerStream = ref object of Connection
  payload*: seq[byte]
  offset*: int

method readOnce*(
    s: DuplicateHandshakeStream, pbytes: pointer, nbytes: int
): Future[int] {.async: (raises: [CancelledError, LPStreamError], raw: true).} =
  let fut = newFuture[int]()
  case s.step
  of 1:
    var buf = newSeq[byte](1)
    buf[0] = 19
    copyMem(pbytes, addr buf[0], buf.len())
    s.step = 2
    fut.complete(buf.len())
  of 2:
    var buf = "/multistream/1.0.0\n"
    copyMem(pbytes, addr buf[0], buf.len())
    s.step = 3
    fut.complete(buf.len())
  of 3:
    var buf = newSeq[byte](1)
    buf[0] = 19
    copyMem(pbytes, addr buf[0], buf.len())
    s.step = 4
    fut.complete(buf.len())
  of 4:
    var buf = "/multistream/1.0.0\n"
    copyMem(pbytes, addr buf[0], buf.len())
    s.step = 5
    fut.complete(buf.len())
  of 5:
    var buf = newSeq[byte](1)
    buf[0] = 18
    copyMem(pbytes, addr buf[0], buf.len())
    s.step = 6
    fut.complete(buf.len())
  of 6:
    var buf = "/test/proto/1.0.0\n"
    copyMem(pbytes, addr buf[0], buf.len())
    fut.complete(buf.len())
  else:
    copyMem(pbytes, cstring("\0x3na\n"), "\0x3na\n".len())
    fut.complete("\0x3na\n".len())
  fut

method write*(
    s: DuplicateHandshakeStream, msg: seq[byte]
): Future[void] {.async: (raises: [CancelledError, LPStreamError], raw: true).} =
  let fut = newFuture[void]()
  fut.complete()
  fut

method close(s: DuplicateHandshakeStream): Future[void] {.async: (raises: [], raw: true).} =
  s.isClosed = true
  s.isEof = true
  let fut = newFuture[void]()
  fut.complete()
  fut

proc newDuplicateHandshakeStream(): DuplicateHandshakeStream =
  new result
  result.step = 1

proc lpFrame(payload: string): seq[byte] =
  let body = payload.toBytes()
  let prefix = PB.toBytes(body.len.uint64)
  result = newSeqOfCap[byte](prefix.len + body.len)
  for b in prefix:
    result.add(b)
  result.add(body)

method readOnce*(
    s: CoalescedListenerStream, pbytes: pointer, nbytes: int
): Future[int] {.async: (raises: [CancelledError, LPStreamError], raw: true).} =
  let fut = newFuture[int]()
  if s.offset >= s.payload.len:
    s.isEof = true
    fut.complete(0)
    return fut
  let remaining = s.payload.len - s.offset
  let toRead = min(remaining, nbytes)
  copyMem(pbytes, unsafeAddr s.payload[s.offset], toRead)
  s.offset += toRead
  if s.offset >= s.payload.len:
    s.isEof = true
  fut.complete(toRead)
  fut

method write*(
    s: CoalescedListenerStream, msg: seq[byte]
): Future[void] {.async: (raises: [CancelledError, LPStreamError], raw: true).} =
  let fut = newFuture[void]()
  fut.complete()
  fut

method close(s: CoalescedListenerStream): Future[void] {.async: (raises: [], raw: true).} =
  s.isClosed = true
  s.isEof = true
  let fut = newFuture[void]()
  fut.complete()
  fut

proc newCoalescedListenerStream(): CoalescedListenerStream =
  new result
  result.payload =
    lpFrame("/multistream/1.0.0\n") &
    lpFrame("/test/proto/1.0.0\n") &
    lpFrame("hello")
  result.offset = 0

suite "Multistream stability":
  teardown:
    checkTrackers()

  asyncTest "listener tolerates duplicate v1 handshake before protocol":
    let ms = MultistreamSelect.new()
    let conn = newDuplicateHandshakeStream()

    var protocol: LPProtocol = new LPProtocol
    proc testHandler(
        conn: Connection, proto: string
    ): Future[void] {.async: (raises: [CancelledError]).} =
      check proto == "/test/proto/1.0.0"
      await conn.close()

    protocol.handler = testHandler
    ms.addHandler("/test/proto/1.0.0", protocol)
    await ms.handle(conn)

  asyncTest "listener preserves coalesced app payload after protocol negotiation":
    let ms = MultistreamSelect.new()
    let conn = newCoalescedListenerStream()

    var protocol: LPProtocol = new LPProtocol
    proc testHandler(
        conn: Connection, proto: string
    ): Future[void] {.async: (raises: [CancelledError]).} =
      check proto == "/test/proto/1.0.0"
      try:
        let payload = await conn.readLp(64)
        check string.fromBytes(payload) == "hello"
      except LPStreamError as exc:
        checkpoint exc.msg
        check false
      await conn.close()

    protocol.handler = testHandler
    ms.addHandler("/test/proto/1.0.0", protocol)
    await ms.handle(conn)
