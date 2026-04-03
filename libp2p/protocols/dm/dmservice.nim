import std/[algorithm, json, options, sequtils, sets, strutils, tables]
from std/times import epochTime
import chronicles
import chronos
import ../../peerid
import ../../peerstore
import ../../multistream
import ../../muxers/muxer
import ../../switch
import ../protocol
import ../../stream/connection
import ../../multiaddress

const
  DirectMessageCodec* = "/unimaker/dm/1.0.0"
  DefaultDirectMessageMaxBytes* = 2 * 1024 * 1024
  DefaultDirectAckMaxBytes* = 64 * 1024

type
  DirectMessage* = object
    id*: uint64
    sender*: PeerId
    payload*: seq[byte]

  DirectMessageSessionLifecycle* {.pure.} = enum
    DmSessionLive
    DmSessionClosing
    DmSessionClosed
    DmSessionFailed

  DirectMessageSessionActivity* {.pure.} = enum
    DmActivityReaderAttached
    DmActivityHandlingPayload
    DmActivityWritingFrame
    DmActivityAwaitingAck

  DirectMessageFrameKind* {.pure.} = enum
    DmFrameNone
    DmFrameMessage
    DmFrameAck
    DmFrameUnknown

  DirectMessageSessionSnapshot* = object
    peer*: PeerId
    peerKey*: string
    connKey*: int
    carrierConnKey*: int
    lifecycle*: DirectMessageSessionLifecycle
    activeActivities*: set[DirectMessageSessionActivity]
    activityDepths*: array[DirectMessageSessionActivity, int]
    lastInboundFrame*: DirectMessageFrameKind
    lastOutboundFrame*: DirectMessageFrameKind
    lastMessageId*: string
    lastError*: string
    updatedAtMs*: int64

  DirectAckWaiter = ref object
    future: Future[bool]
    error: string

  DirectMessageSession = ref object
    peer: PeerId
    peerKey: string
    conn: Connection
    connKey: int
    carrierConnKey: int
    writeLock: AsyncLock
    readerTask: Future[void]
    readerReady: Future[void]
    readerActive: bool
    lifecycle: DirectMessageSessionLifecycle
    activityDepths: array[DirectMessageSessionActivity, int]
    lastInboundFrame: DirectMessageFrameKind
    lastOutboundFrame: DirectMessageFrameKind
    lastMessageId: string
    lastError: string
    updatedAtMs: int64

  DirectMessageHandler* = proc(msg: DirectMessage) {.async.}
  DirectMessageSendProgress* = proc(stage: string, detail: JsonNode) {.gcsafe, raises: [].}
  DirectMessageWarmResult* = object
    ok*: bool
    err*: string
    connKey*: int
    reusedLiveConnection*: bool
    readerActive*: bool

  DirectMessageService* = ref object of LPProtocol
    switch*: Switch
    selfPeer*: PeerId
    messageHandler*: DirectMessageHandler
    maxMessageBytes*: int
    maxAckBytes*: int
    ackWaiters: Table[string, DirectAckWaiter]
    sessions: Table[string, Table[int, DirectMessageSession]]
    sessionsByConn: Table[int, DirectMessageSession]
    sessionsByCarrierConn: Table[int, DirectMessageSession]
    dialLocks: Table[string, AsyncLock]

logScope:
  topics = "libp2p directmsg"

proc bytesToString(data: seq[byte]): string =
  if data.len == 0:
    return ""
  result = newString(data.len)
  copyMem(addr result[0], unsafeAddr data[0], data.len)

proc stringToBytes(text: string): seq[byte] =
  if text.len == 0:
    return @[]
  result = newSeq[byte](text.len)
  copyMem(addr result[0], text.cstring, text.len)

proc nowMillis(): int64 =
  int64(epochTime() * 1000)

proc awaitFutureWithin[T](
    fut: Future[T],
    timeout: Duration,
    timeoutMsg: string
): Future[T] {.async.} =
  let timeoutFut = sleepAsync(timeout)
  let winner = await race(cast[FutureBase](fut), cast[FutureBase](timeoutFut))
  if winner == cast[FutureBase](timeoutFut):
    fut.cancelSoon()
    raise (ref LPError)(msg: timeoutMsg)
  timeoutFut.cancelSoon()
  try:
    return await fut
  except CancelledError as exc:
    raise exc
  except CatchableError as exc:
    raise exc

proc payloadPreview(data: seq[byte], maxChars = 160): string =
  if data.len == 0:
    return ""
  let limit = min(data.len, maxChars)
  result = newStringOfCap(limit + 3)
  for i in 0 ..< limit:
    let ch = char(data[i])
    case ch
    of '\n', '\r', '\t':
      result.add(' ')
    else:
      if ord(ch) >= 32 and ord(ch) <= 126:
        result.add(ch)
      else:
        result.add('.')
  if data.len > limit:
    result.add("...")

proc closeConnSoon(conn: Connection) =
  if conn.isNil:
    return
  proc runner() {.async.} =
    try:
      await conn.close()
    except CatchableError:
      discard
  asyncSpawn runner()

proc closeConnLater(conn: Connection; delay: Duration) =
  if conn.isNil:
    return
  proc runner() {.async.} =
    try:
      await sleepAsync(delay)
      await conn.close()
    except CatchableError:
      discard
  asyncSpawn runner()

proc newDirectAckWaiter(): DirectAckWaiter =
  DirectAckWaiter(
    future: newFuture[bool]("libp2p.directmsg.ack"),
    error: "",
  )

proc releaseAsyncLock(lock: AsyncLock) =
  if lock.isNil:
    return
  try:
    lock.release()
  except AsyncLockError:
    discard

proc directPeerKey(peer: PeerId): string =
  try:
    $peer
  except CatchableError:
    ""

proc directConnKey(conn: Connection): int =
  if conn.isNil:
    return 0
  cast[int](conn)

proc touchSession(
    session: DirectMessageSession,
    messageId = "",
    error = ""
) =
  if session.isNil:
    return
  if messageId.len > 0:
    session.lastMessageId = messageId
  if error.len > 0:
    session.lastError = error
  session.updatedAtMs = nowMillis()

proc activeSessionActivities(
    session: DirectMessageSession
): set[DirectMessageSessionActivity] =
  if session.isNil:
    return {}
  for activity in DirectMessageSessionActivity:
    if session.activityDepths[activity] > 0:
      result.incl(activity)

proc canTransitionLifecycle(
    current: DirectMessageSessionLifecycle,
    next: DirectMessageSessionLifecycle
): bool =
  if current == next:
    return true
  case current
  of DmSessionLive:
    next in {DmSessionClosing, DmSessionClosed, DmSessionFailed}
  of DmSessionClosing:
    next in {DmSessionClosed, DmSessionFailed}
  of DmSessionFailed:
    next == DmSessionClosed
  of DmSessionClosed:
    false

proc transitionSessionLifecycle(
    session: DirectMessageSession,
    next: DirectMessageSessionLifecycle,
    messageId = "",
    error = ""
) =
  if session.isNil:
    return
  if not canTransitionLifecycle(session.lifecycle, next):
    raiseAssert(
      "invalid direct message session transition current=" & $session.lifecycle &
      " next=" & $next &
      (if messageId.len > 0: " mid=" & messageId else: "") &
      (if error.len > 0: " err=" & error else: "")
    )
  session.lifecycle = next
  touchSession(session, messageId, error)

proc enterSessionActivity(
    session: DirectMessageSession,
    activity: DirectMessageSessionActivity,
    messageId = "",
    error = ""
) =
  if session.isNil:
    return
  inc session.activityDepths[activity]
  touchSession(session, messageId, error)

proc leaveSessionActivity(
    session: DirectMessageSession,
    activity: DirectMessageSessionActivity,
    messageId = "",
    error = ""
) =
  if session.isNil:
    return
  if session.activityDepths[activity] <= 0:
    raiseAssert(
      "invalid direct message session activity exit activity=" & $activity &
      (if messageId.len > 0: " mid=" & messageId else: "") &
      (if error.len > 0: " err=" & error else: "")
    )
  dec session.activityDepths[activity]
  touchSession(session, messageId, error)

proc recordInboundFrame(
    session: DirectMessageSession,
    frame: DirectMessageFrameKind,
    messageId = ""
) =
  if session.isNil:
    return
  session.lastInboundFrame = frame
  touchSession(session, messageId)

proc recordOutboundFrame(
    session: DirectMessageSession,
    frame: DirectMessageFrameKind,
    messageId = ""
) =
  if session.isNil:
    return
  session.lastOutboundFrame = frame
  touchSession(session, messageId)

proc failSession(
    session: DirectMessageSession,
    error: string,
    messageId = ""
) =
  if session.isNil:
    return
  if session.lifecycle in {DmSessionClosed, DmSessionFailed}:
    touchSession(session, messageId, error)
    return
  transitionSessionLifecycle(session, DmSessionFailed, messageId, error)

proc isLiveSession(session: DirectMessageSession): bool =
  not session.isNil and
    session.lifecycle == DmSessionLive and
    not session.conn.isNil and
    not session.conn.closed

proc toSessionControlSnapshot(
    session: DirectMessageSession
): DirectMessageSessionSnapshot =
  if session.isNil:
    return
  result = DirectMessageSessionSnapshot(
    peer: session.peer,
    peerKey: session.peerKey,
    connKey: session.connKey,
    carrierConnKey: session.carrierConnKey,
    lifecycle: session.lifecycle,
    activeActivities: activeSessionActivities(session),
    activityDepths: session.activityDepths,
    lastInboundFrame: session.lastInboundFrame,
    lastOutboundFrame: session.lastOutboundFrame,
    lastMessageId: session.lastMessageId,
    lastError: session.lastError,
    updatedAtMs: session.updatedAtMs,
  )

proc attachSessionToPeerIndex(
    svc: DirectMessageService,
    session: DirectMessageSession,
    peer: PeerId,
    peerKey: string
) {.raises: [].} =
  if svc.isNil or session.isNil or session.connKey == 0 or peerKey.len == 0:
    return
  session.peer = peer
  session.peerKey = peerKey
  var bucket: Table[int, DirectMessageSession]
  if not svc.sessions.pop(peerKey, bucket):
    bucket = initTable[int, DirectMessageSession]()
  bucket[session.connKey] = session
  svc.sessions[peerKey] = bucket

proc attachSessionToCarrierIndex(
    svc: DirectMessageService,
    session: DirectMessageSession,
    carrierConnKey: int
) {.raises: [].} =
  if svc.isNil or session.isNil or carrierConnKey == 0:
    return
  if svc.sessionsByCarrierConn.hasKey(carrierConnKey):
    let current = svc.sessionsByCarrierConn.getOrDefault(carrierConnKey)
    if current == session:
      session.carrierConnKey = carrierConnKey
      return
    if not current.isNil and current.carrierConnKey == carrierConnKey:
      current.carrierConnKey = 0
  session.carrierConnKey = carrierConnKey
  svc.sessionsByCarrierConn[carrierConnKey] = session

proc refreshSessionPeerIndex(
    svc: DirectMessageService,
    session: DirectMessageSession
) {.raises: [].} =
  if svc.isNil or session.isNil or session.conn.isNil or session.peerKey.len > 0:
    return
  let peer = session.conn.peerId
  let peerKey = directPeerKey(peer)
  if peerKey.len == 0:
    return
  svc.attachSessionToPeerIndex(session, peer, peerKey)

proc removeSession(
    svc: DirectMessageService,
    session: DirectMessageSession
) {.raises: [].} =
  if svc.isNil or session.isNil or session.connKey == 0:
    return
  if svc.sessionsByConn.hasKey(session.connKey):
    let current = svc.sessionsByConn.getOrDefault(session.connKey)
    if current == session:
      svc.sessionsByConn.del(session.connKey)
  if session.carrierConnKey != 0 and svc.sessionsByCarrierConn.hasKey(session.carrierConnKey):
    let current = svc.sessionsByCarrierConn.getOrDefault(session.carrierConnKey)
    if current == session:
      svc.sessionsByCarrierConn.del(session.carrierConnKey)
  if session.peerKey.len > 0:
    var bucket: Table[int, DirectMessageSession]
    if svc.sessions.pop(session.peerKey, bucket):
      if bucket.hasKey(session.connKey):
        let current = bucket.getOrDefault(session.connKey)
        if current == session:
          bucket.del(session.connKey)
      if bucket.len > 0:
        svc.sessions[session.peerKey] = bucket

proc closeSessionConn(session: DirectMessageSession): Future[void] {.async: (raises: [CancelledError]).} =
  if session.isNil:
    return
  if session.conn.isNil:
    if session.lifecycle == DmSessionClosing or session.lifecycle == DmSessionFailed:
      transitionSessionLifecycle(session, DmSessionClosed, session.lastMessageId, session.lastError)
    return
  if session.lifecycle == DmSessionLive:
    transitionSessionLifecycle(session, DmSessionClosing, session.lastMessageId, session.lastError)
  let conn = session.conn
  session.conn = nil
  try:
    await conn.close()
  except CancelledError as exc:
    raise exc
  except CatchableError:
    discard
  finally:
    if session.lifecycle in {DmSessionClosing, DmSessionFailed}:
      transitionSessionLifecycle(session, DmSessionClosed, session.lastMessageId, session.lastError)

proc storeSession(
    svc: DirectMessageService,
    conn: Connection,
    carrierConn: Connection = nil
): DirectMessageSession {.raises: [].} =
  if svc.isNil or conn.isNil:
    return nil
  let connKey = directConnKey(conn)
  if connKey == 0:
    return nil
  let carrierConnKey =
    if not carrierConn.isNil:
      directConnKey(carrierConn)
    else:
      connKey
  let peer = conn.peerId
  let peerKey = directPeerKey(peer)
  if svc.sessionsByConn.hasKey(connKey):
    let existing = svc.sessionsByConn.getOrDefault(connKey)
    if not existing.isNil:
      if existing.conn.isNil:
        existing.conn = conn
      if existing.carrierConnKey == 0 and carrierConnKey != 0:
        svc.attachSessionToCarrierIndex(existing, carrierConnKey)
      if existing.peerKey.len == 0 and peerKey.len > 0:
        svc.attachSessionToPeerIndex(existing, peer, peerKey)
      elif existing.peerKey.len == 0:
        existing.peer = peer
      return existing
    svc.sessionsByConn.del(connKey)
  result = DirectMessageSession(
    peer: peer,
    peerKey: peerKey,
    conn: conn,
    connKey: connKey,
    carrierConnKey: carrierConnKey,
    writeLock: newAsyncLock(),
    readerTask: nil,
    readerReady: nil,
    readerActive: false,
    lifecycle: DmSessionLive,
    activityDepths: default(array[DirectMessageSessionActivity, int]),
    lastInboundFrame: DmFrameNone,
    lastOutboundFrame: DmFrameNone,
    lastMessageId: "",
    lastError: "",
    updatedAtMs: nowMillis(),
  )
  svc.sessionsByConn[connKey] = result
  if carrierConnKey != 0:
    svc.attachSessionToCarrierIndex(result, carrierConnKey)
  if peerKey.len > 0:
    svc.attachSessionToPeerIndex(result, peer, peerKey)

proc getSessionByConn(
    svc: DirectMessageService,
    conn: Connection
): DirectMessageSession {.raises: [].} =
  if svc.isNil or conn.isNil:
    return nil
  let connKey = directConnKey(conn)
  if connKey == 0:
    return nil
  result = svc.sessionsByConn.getOrDefault(connKey)
  if not result.isNil:
    svc.refreshSessionPeerIndex(result)

proc getSessionByCarrierConn(
    svc: DirectMessageService,
    conn: Connection
): DirectMessageSession {.raises: [].} =
  if svc.isNil or conn.isNil:
    return nil
  let carrierConnKey = directConnKey(conn)
  if carrierConnKey == 0:
    return nil
  result = svc.sessionsByCarrierConn.getOrDefault(carrierConnKey)
  if not result.isNil:
    svc.refreshSessionPeerIndex(result)

proc getLiveSessionByConn(
    svc: DirectMessageService,
    conn: Connection
): DirectMessageSession {.raises: [].} =
  let session = svc.getSessionByConn(conn)
  if isLiveSession(session):
    return session
  let carrierSession = svc.getSessionByCarrierConn(conn)
  if isLiveSession(carrierSession):
    return carrierSession
  nil

proc storeOrRefreshSession(
    svc: DirectMessageService,
    conn: Connection,
    carrierConn: Connection = nil
): DirectMessageSession {.raises: [].} =
  result = svc.storeSession(conn, carrierConn)
  if not result.isNil:
    svc.refreshSessionPeerIndex(result)

proc getLiveSession(
    svc: DirectMessageService,
    peer: PeerId
): DirectMessageSession =
  if svc.isNil:
    return nil
  let peerKey = directPeerKey(peer)
  if peerKey.len == 0:
    return nil
  let sessions = svc.sessions.getOrDefault(peerKey)
  for _, session in sessions.pairs:
    svc.refreshSessionPeerIndex(session)
    if result.isNil:
      if isLiveSession(session):
        result = session

proc getLiveSessionByConnKey(
    svc: DirectMessageService,
    connKey: int
): DirectMessageSession =
  if svc.isNil or connKey == 0:
    return nil
  let session = svc.sessionsByConn.getOrDefault(connKey)
  if not session.isNil and isLiveSession(session):
    return session

proc hasLiveSessionConnKey*(
    svc: DirectMessageService,
    connKey: int
): bool {.gcsafe, raises: [].} =
  not svc.getLiveSessionByConnKey(connKey).isNil

proc sessionReaderRunning(
    session: DirectMessageSession
): bool {.raises: [].} =
  if session.isNil:
    return false
  if session.readerTask.isNil:
    return false
  not session.readerTask.finished()

proc sessionReaderReady(
    session: DirectMessageSession
): bool {.raises: [].} =
  if not sessionReaderRunning(session):
    return false
  if session.readerReady.isNil:
    return session.readerActive
  session.readerReady.finished() and not session.readerReady.failed

proc getOrCreateDialLock(
    svc: DirectMessageService,
    peer: PeerId
): AsyncLock =
  let peerKey = directPeerKey(peer)
  if peerKey.len == 0:
    return nil
  if not svc.dialLocks.hasKey(peerKey):
    svc.dialLocks[peerKey] = newAsyncLock()
  svc.dialLocks.getOrDefault(peerKey)

proc activeSessionCountForPeer*(
    svc: DirectMessageService,
    peer: PeerId
): int {.gcsafe, raises: [].} =
  if svc.isNil:
    return 0
  let peerKey = directPeerKey(peer)
  if peerKey.len == 0:
    return 0
  let sessions = svc.sessions.getOrDefault(peerKey)
  if sessions.len == 0:
    return 0
  for _, item in sessions.pairs:
    if isLiveSession(item):
      inc result

proc sessionObjectSnapshot(
    svc: DirectMessageService
): seq[DirectMessageSession] {.raises: [].} =
  if svc.isNil:
    return @[]
  for _, session in svc.sessionsByConn.pairs:
    if not session.isNil:
      result.add(session)

proc sessionControlSnapshots*(
    svc: DirectMessageService
): seq[DirectMessageSessionSnapshot] {.gcsafe, raises: [].} =
  let sessions = sessionObjectSnapshot(svc)
  for session in sessions:
    if not session.isNil:
      result.add(toSessionControlSnapshot(session))

proc peerSessionControlSnapshots*(
    svc: DirectMessageService,
    peer: PeerId
): seq[DirectMessageSessionSnapshot] {.gcsafe, raises: [].} =
  let peerKey = directPeerKey(peer)
  if peerKey.len == 0:
    return @[]
  for snapshot in sessionControlSnapshots(svc):
    if snapshot.peerKey == peerKey:
      result.add(snapshot)

proc failAckWaiters(
    svc: DirectMessageService,
    error: string
) {.raises: [].} =
  if svc.isNil or svc.ackWaiters.len == 0:
    return
  let mids = toSeq(svc.ackWaiters.keys)
  for mid in mids:
    let waiter = svc.ackWaiters.getOrDefault(mid)
    if waiter.isNil:
      continue
    waiter.error = error
    if not waiter.future.isNil and not waiter.future.finished():
      waiter.future.complete(false)
  svc.ackWaiters.clear()

proc isRetryableStaleConnError(exc: ref CatchableError): bool =
  exc of LPStreamConnDownError or
    exc of LPStreamClosedError or
    exc of LPStreamResetError

proc notifySendProgress(
    progress: DirectMessageSendProgress,
    stage: string,
    detail: JsonNode = newJNull()
) {.gcsafe, raises: [].} =
  if progress.isNil:
    return
  try:
    progress(stage, detail)
  except CatchableError:
    discard

proc jsonGetStr(node: JsonNode, key: string, defaultVal = ""): string =
  if node.kind == JObject and node.hasKey(key) and node[key].kind == JString:
    return node[key].getStr()
  defaultVal

proc jsonGetBool(node: JsonNode, key: string, defaultVal = false): bool =
  if node.kind == JObject and node.hasKey(key):
    let item = node[key]
    case item.kind
    of JBool:
      return item.getBool()
    of JInt, JFloat:
      return item.getInt() != 0
    else:
      discard
  defaultVal

when defined(libp2p_msquic_experimental):
  proc synthesizeQuicAddressText(addrText: string): Option[string] =
    let lower = addrText.toLowerAscii()
    if lower.len == 0:
      return none(string)
    if not lower.contains("/tcp/") or lower.contains("/quic"):
      return none(string)
    let tcpIdx = lower.find("/tcp/")
    if tcpIdx < 0:
      return none(string)
    let after = addrText[(tcpIdx + 5) ..< addrText.len]
    var portPart = ""
    var i = 0
    while i < after.len and after[i].isDigit:
      portPart.add(after[i])
      inc i
    if portPart.len == 0:
      return none(string)
    let suffix = if i < after.len: after[i ..< after.len] else: ""
    let prefix = addrText[0 ..< tcpIdx]
    some(prefix & "/udp/" & portPart & "/quic-v1" & suffix)

proc directDialAddrPriority(addrText: string): int =
  let lower = addrText.toLowerAscii()
  let isTsnet = lower.contains("/tsnet")
  let hasTcp = lower.contains("/tcp/")
  let hasQuic = lower.contains("/quic")
  let hasUdp = lower.contains("/udp/")

  if isTsnet:
    if hasTcp:
      return 0
    if hasQuic:
      return 1
    if hasUdp:
      return 2
    return 3

  if hasTcp:
    return 0
  if hasQuic:
    return 1
  if hasUdp:
    return 2
  3

proc orderDirectDialAddrs*(addrs: seq[MultiAddress]): seq[MultiAddress] =
  var expanded: seq[MultiAddress] = @[]
  var seen = initHashSet[string]()
  for ma in addrs:
    let text = $ma
    if text.len == 0:
      continue
    if not seen.contains(text):
      expanded.add(ma)
      seen.incl(text)
  if expanded.len <= 1:
    return expanded
  var indexed: seq[(int, int, MultiAddress)] = @[]
  for idx, ma in expanded:
    indexed.add((directDialAddrPriority($ma), idx, ma))
  indexed.sort(proc(a, b: (int, int, MultiAddress)): int =
    result = system.cmp(a[0], b[0])
    if result == 0:
      result = system.cmp(a[1], b[1])
  )
  indexed.mapIt(it[2])

proc buildAckPayload(
    svc: DirectMessageService,
    mid: string,
    success: bool,
    error: string
): seq[byte] =
  var ackEnvelope = newJObject()
  ackEnvelope["op"] = %"ack"
  ackEnvelope["mid"] = %mid
  var fromPeer = ""
  try:
    fromPeer = $svc.selfPeer
  except CatchableError:
    fromPeer = ""
  ackEnvelope["from"] = %fromPeer
  ackEnvelope["timestamp_ms"] = %nowMillis()
  ackEnvelope["ack"] = %success
  if error.len > 0:
    ackEnvelope["error"] = %error
  stringToBytes($ackEnvelope)

proc parseAckPayload(payload: seq[byte]): tuple[mid: string, success: bool, error: string, ok: bool] =
  result = (mid: "", success: false, error: "", ok: false)
  if payload.len == 0:
    return
  try:
    let raw = bytesToString(payload)
    let node = parseJson(raw)
    if node.kind != JObject:
      return
    result.mid = jsonGetStr(node, "mid")
    result.success = jsonGetBool(node, "ack", true)
    result.error = jsonGetStr(node, "error")
    result.ok = result.mid.len > 0
  except CatchableError:
    discard

proc sendAuthoritativeAck(
    svc: DirectMessageService,
    session: DirectMessageSession,
    payload: seq[byte],
    messageId: string,
    timeout: Duration
): Future[(bool, string)] {.gcsafe, raises: [].}

proc runSession(
    svc: DirectMessageService,
    session: DirectMessageSession
): Future[void] {.async: (raises: [CancelledError]).}

proc startSessionReader(
    svc: DirectMessageService,
    session: DirectMessageSession
) =
  if svc.isNil or session.isNil or sessionReaderRunning(session):
    return
  session.readerReady = newFuture[void]("libp2p.directmsg.readerReady")
  let task = svc.runSession(session)
  session.readerTask = task
  asyncSpawn(task)

proc ensureSessionReaderReady(
    svc: DirectMessageService,
    session: DirectMessageSession,
    timeout: Duration,
    messageId = ""
): Future[void] {.async: (raises: [CancelledError, LPError]).} =
  if svc.isNil or session.isNil or session.conn.isNil:
    raise (ref LPError)(msg: "direct message reader unavailable")
  if sessionReaderReady(session):
    return
  if not sessionReaderRunning(session):
    svc.startSessionReader(session)
  let ready = session.readerReady
  if ready.isNil:
    if session.readerActive:
      return
    raise (ref LPError)(msg: "direct message reader attach future missing")
  try:
    await awaitFutureWithin(
      ready,
      timeout,
      "direct message reader attach timeout",
    )
  except CancelledError as exc:
    raise exc
  except CatchableError as exc:
    let attachErr =
      if exc.msg.len > 0:
        exc.msg
      else:
        "direct message reader attach failed"
    failSession(session, attachErr, messageId)
    try:
      await closeSessionConn(session)
    except CatchableError:
      discard
    raise (ref LPError)(msg: attachErr)
  if ready.failed:
    let attachErr =
      if not ready.error.isNil and ready.error.msg.len > 0:
        ready.error.msg
      else:
        "direct message reader attach failed"
    failSession(session, attachErr, messageId)
    try:
      await closeSessionConn(session)
    except CatchableError:
      discard
    raise (ref LPError)(msg: attachErr)

proc sendFrameOnSession(
    session: DirectMessageSession,
    payload: seq[byte],
    timeout: Duration,
    timeoutMsg: string,
    frameKind = DmFrameUnknown,
    messageId = ""
): Future[(bool, string)] {.async: (raises: [CancelledError]).} =
  if session.isNil or session.conn.isNil:
    return (false, "direct message session unavailable")
  if timeout <= ZeroDuration:
    return (false, timeoutMsg)
  enterSessionActivity(session, DmActivityWritingFrame, messageId)
  if frameKind != DmFrameNone:
    recordOutboundFrame(session, frameKind, messageId)
  try:
    await session.writeLock.acquire()
    let activeConn = session.conn
    if activeConn.isNil or activeConn.closed:
      return (false, "direct message session closed")
    await awaitFutureWithin(
      activeConn.writeLp(payload),
      timeout,
      timeoutMsg,
    )
    (true, "")
  except CancelledError as exc:
    raise exc
  except CatchableError as exc:
    failSession(session, exc.msg, messageId)
    try:
      await closeSessionConn(session)
    except CatchableError:
      discard
    (false, exc.msg)
  finally:
    releaseAsyncLock(session.writeLock)
    leaveSessionActivity(session, DmActivityWritingFrame, messageId)

proc handleIncomingPayload(
    svc: DirectMessageService,
    session: DirectMessageSession,
    payload: seq[byte]
) {.async: (raises: [CancelledError]).} =
  enterSessionActivity(session, DmActivityHandlingPayload)
  var ackRequested = false
  var messageId = ""
  let remotePeer = if session.isNil: "" else: session.peerKey
  try:
    if payload.len == 0:
      warn "direct message empty payload", peer = remotePeer
      return

    let rawPrefix = payloadPreview(payload)
    warn "direct message parse begin", peer = remotePeer, size = payload.len, prefix = rawPrefix
    var parsedEnvelope = newJNull()
    let rawPayload =
      try:
        bytesToString(payload)
      except CatchableError:
        ""
    if rawPayload.len > 0:
      try:
        parsedEnvelope = parseJson(rawPayload)
        if parsedEnvelope.kind == JObject:
          let op = jsonGetStr(parsedEnvelope, "op").toLowerAscii()
          ackRequested = jsonGetBool(parsedEnvelope, "ackRequested", false)
          messageId = jsonGetStr(parsedEnvelope, "mid")
          let frameKind =
            case op
            of "dm": DmFrameMessage
            of "ack": DmFrameAck
            of "": DmFrameUnknown
            else: DmFrameUnknown
          recordInboundFrame(session, frameKind, messageId)
          warn "direct message parse ok",
            peer = remotePeer,
            op = op,
            mid = messageId,
            ackRequested = ackRequested,
            senderPeer = jsonGetStr(parsedEnvelope, "from"),
            prefix = rawPrefix
          if ackRequested:
            warn "direct message requests ack", peer = remotePeer, mid = messageId
          if op == "ack":
            let ackResult = parseAckPayload(payload)
            if ackResult.ok:
              if svc.ackWaiters.hasKey(ackResult.mid):
                let waiter = svc.ackWaiters.getOrDefault(ackResult.mid)
                svc.ackWaiters.del(ackResult.mid)
                if not waiter.isNil:
                  waiter.error = ackResult.error
                  if not waiter.future.isNil and not waiter.future.finished():
                    waiter.future.complete(ackResult.success)
              elif not ackResult.success and ackResult.error.len > 0:
                warn "direct message unexpected ack without waiter",
                  peer = remotePeer,
                  mid = ackResult.mid,
                  err = ackResult.error
            else:
              warn "direct message invalid ack payload", peer = remotePeer, prefix = rawPrefix
            return
        else:
          warn "direct message parse non object",
            peer = remotePeer, kind = $parsedEnvelope.kind, prefix = rawPrefix
      except CatchableError as exc:
        touchSession(session, error = exc.msg)
        warn "direct message parse failed", peer = remotePeer, err = exc.msg, prefix = rawPrefix
        ackRequested = false
        messageId = ""
    else:
      warn "direct message raw payload unavailable", peer = remotePeer, size = payload.len, prefix = rawPrefix

    var directMsg = DirectMessage(
      id: 0,
      sender: (if session.isNil: PeerId() else: session.peer),
      payload: payload
    )
    var senderPeer = ""
    try:
      senderPeer = $directMsg.sender
    except CatchableError:
      senderPeer = ""

    var handlerSucceeded = false
    try:
      warn "direct message handler begin",
        peer = remotePeer,
        senderPeer = senderPeer,
        mid = messageId,
        ackRequested = ackRequested,
        bytes = directMsg.payload.len
      await svc.messageHandler(directMsg)
      handlerSucceeded = true
      warn "direct message handler done",
        peer = remotePeer,
        senderPeer = senderPeer,
        mid = messageId,
        ackRequested = ackRequested
    except CatchableError as exc:
      touchSession(session, messageId, exc.msg)
      if ackRequested and messageId.len > 0:
        let ackBytes = buildAckPayload(svc, messageId, false, exc.msg)
        try:
          warn "direct message sending failure ack", peer = remotePeer, mid = messageId
          let ackSend = await svc.sendAuthoritativeAck(session, ackBytes, messageId, 3.seconds)
          if ackSend[0]:
            warn "direct message failure ack sent", peer = remotePeer, mid = messageId
          else:
            warn "direct message failure ack delivery failed",
              peer = remotePeer,
              mid = messageId,
              err = ackSend[1]
        except CatchableError as sendErr:
          warn "direct ack send failed", peer = remotePeer, err = sendErr.msg
      warn "direct message handler error",
        peer = remotePeer,
        senderPeer = senderPeer,
        mid = messageId,
        ackRequested = ackRequested,
        err = exc.msg

    if handlerSucceeded and ackRequested and messageId.len > 0:
      let ackBytes = buildAckPayload(svc, messageId, true, "")
      try:
        warn "direct message sending ack", peer = remotePeer, mid = messageId
        let ackSend = await svc.sendAuthoritativeAck(session, ackBytes, messageId, 3.seconds)
        if ackSend[0]:
          warn "direct message ack sent", peer = remotePeer, mid = messageId
        else:
          warn "direct message ack delivery failed",
            peer = remotePeer,
            mid = messageId,
            err = ackSend[1]
      except CatchableError as sendErr:
        warn "direct ack send failed", peer = remotePeer, err = sendErr.msg
    elif ackRequested:
      warn "direct message requested ack but missing message id", peer = remotePeer
  finally:
    leaveSessionActivity(session, DmActivityHandlingPayload, messageId)

proc runSession(
    svc: DirectMessageService,
    session: DirectMessageSession
): Future[void] {.async: (raises: [CancelledError]).} =
  let remotePeer = if session.isNil: "" else: session.peerKey
  if svc.isNil or session.isNil or session.conn.isNil:
    return
  svc.refreshSessionPeerIndex(session)
  session.readerActive = true
  enterSessionActivity(session, DmActivityReaderAttached)
  if not session.readerReady.isNil and not session.readerReady.finished():
    session.readerReady.complete()
  try:
    while not session.conn.isNil and not session.conn.closed:
      svc.refreshSessionPeerIndex(session)
      warn "direct message readLp begin", peer = remotePeer, maxBytes = svc.maxMessageBytes
      let payload = await session.conn.readLp(svc.maxMessageBytes)
      warn "direct message readLp done", peer = remotePeer, size = payload.len
      if payload.len == 0:
        warn "direct message eof", peer = remotePeer
        break
      await svc.handleIncomingPayload(session, payload)
  except CancelledError as exc:
    raise exc
  except LPStreamError as lpErr:
    failSession(session, lpErr.msg, session.lastMessageId)
    warn "direct message stream error", peer = remotePeer, err = lpErr.msg
  except CatchableError as exc:
    failSession(session, exc.msg, session.lastMessageId)
    warn "direct message handler failed", peer = remotePeer, err = exc.msg
  finally:
    if not session.readerReady.isNil and not session.readerReady.finished():
      let attachErr =
        if session.lastError.len > 0:
          session.lastError
        else:
          "direct message reader exited before attach"
      session.readerReady.fail((ref LPError)(msg: attachErr))
    session.readerActive = false
    leaveSessionActivity(session, DmActivityReaderAttached, session.lastMessageId, session.lastError)
    session.readerTask = nil
    session.readerReady = nil
    svc.removeSession(session)
    try:
      await closeSessionConn(session)
    except CatchableError:
      discard

proc handleIncoming(
    svc: DirectMessageService, conn: Connection
) {.async: (raises: [CancelledError]).} =
  let session = svc.storeSession(conn)
  if session.isNil:
    try:
      await conn.close()
    except CatchableError:
      discard
    return
  await svc.runSession(session)

proc newDirectMessageService*(
    switch: Switch,
    selfPeer: PeerId,
    handler: DirectMessageHandler,
    codec = DirectMessageCodec,
    maxMessageBytes = DefaultDirectMessageMaxBytes,
    maxAckBytes = DefaultDirectAckMaxBytes
): DirectMessageService =
  let svc = DirectMessageService(
    switch: switch,
    selfPeer: selfPeer,
    messageHandler: handler,
    maxMessageBytes: maxMessageBytes,
    maxAckBytes: maxAckBytes,
    ackWaiters: initTable[string, DirectAckWaiter](),
    sessions: initTable[string, Table[int, DirectMessageSession]](),
    sessionsByConn: initTable[int, DirectMessageSession](),
    sessionsByCarrierConn: initTable[int, DirectMessageSession](),
    dialLocks: initTable[string, AsyncLock](),
  )
  svc.codec = codec
  svc.codecs = @[codec]

  proc handle(conn: Connection, proto: string) {.async: (raises: [CancelledError]), gcsafe, closure.} =
    await svc.handleIncoming(conn)

  svc.handler = handle
  svc

method stop*(svc: DirectMessageService): Future[void] {.async: (raises: []).} =
  if svc.isNil:
    return

  svc.started = false
  let sessions = svc.sessionObjectSnapshot()
  svc.sessions = initTable[string, Table[int, DirectMessageSession]]()
  svc.sessionsByConn = initTable[int, DirectMessageSession]()
  svc.sessionsByCarrierConn = initTable[int, DirectMessageSession]()
  svc.dialLocks = initTable[string, AsyncLock]()
  svc.failAckWaiters("direct message service stopped")

  var readerTasks: seq[Future[void]] = @[]
  for session in sessions:
    if session.isNil:
      continue
    let task = session.readerTask
    if not task.isNil and not task.finished():
      task.cancelSoon()
      readerTasks.add(task)

  for session in sessions:
    try:
      await closeSessionConn(session)
    except CatchableError:
      discard

  for task in readerTasks:
    try:
      await task
    except CatchableError:
      discard

proc ensureSessionFromConnection(
    svc: DirectMessageService,
    activeConn: Connection,
    carrierConn: Connection,
    startReaderDetached: bool
): DirectMessageSession =
  let stored = svc.storeOrRefreshSession(activeConn, carrierConn)
  if stored.isNil:
    return nil
  if startReaderDetached and not sessionReaderRunning(stored):
    svc.startSessionReader(stored)
  stored

proc establishOutboundSession(
    svc: DirectMessageService,
    peer: PeerId,
    timeout: Duration,
    preferredAddrs: seq[MultiAddress] = @[],
    verifiedMuxer: Muxer = nil,
    preferredConnKey = 0,
    reuseVerifiedLiveConnection = false,
    progress: DirectMessageSendProgress = nil,
    messageId = "",
    allowExisting = true,
    startReaderDetached = false,
): Future[tuple[session: DirectMessageSession, connFromLive: bool, dialErr: string]] {.async.} =
  if svc.isNil or svc.switch.isNil:
    return (nil, false, "direct message service unavailable")

  let establishDeadline = Moment.now() + timeout

  proc remainingEstablishBudget(): Duration =
    let remaining = establishDeadline - Moment.now()
    if remaining > ZeroDuration:
      remaining
    else:
      ZeroDuration

  var storedAddrs: seq[MultiAddress] = @[]
  try:
    let store = svc.switch.peerStore
    if not store.isNil:
      let addrBook = store[AddressBook]
      if addrBook.book.hasKey(peer):
        storedAddrs = addrBook.book.getOrDefault(peer)
  except CatchableError:
    storedAddrs = @[]

  let orderedAddrs =
    if preferredAddrs.len > 0:
      orderDirectDialAddrs(preferredAddrs)
    elif storedAddrs.len > 0:
      orderDirectDialAddrs(storedAddrs)
    else:
      @[]

  proc hasLiveConn(): bool =
    (not svc.switch.connManager.isNil) and svc.switch.connManager.connCount(peer) > 0

  let hasVerifiedMuxer =
    not verifiedMuxer.isNil and
    not verifiedMuxer.connection.isNil and
    not verifiedMuxer.connection.closed
  let allowVerifiedExistingSession =
    preferredAddrs.len > 0 and
    reuseVerifiedLiveConnection
  let forbidFreshDialAfterVerifiedReuse =
    preferredAddrs.len > 0 and
    hasVerifiedMuxer and
    reuseVerifiedLiveConnection
  let useExplicitFreshDial =
    preferredAddrs.len > 0 and
    not hasVerifiedMuxer and
    not reuseVerifiedLiveConnection
  let allowGenericExisting =
    allowExisting and
    preferredAddrs.len == 0 and
    not hasVerifiedMuxer and
    not reuseVerifiedLiveConnection

  if preferredConnKey > 0:
    let preferredSession = svc.getLiveSessionByConnKey(preferredConnKey)
    if not preferredSession.isNil:
      if startReaderDetached:
        await svc.ensureSessionReaderReady(
          preferredSession,
          remainingEstablishBudget(),
          messageId,
        )
      notifySendProgress(progress, "dm_session_reuse", %*{
        "dmPhase": "session_reuse",
        "peer": $peer,
        "messageId": messageId,
        "reason": "preferred_conn_key",
        "connKey": preferredConnKey,
      })
      return (preferredSession, false, "")

  if hasVerifiedMuxer:
    let verifiedSession = svc.getLiveSessionByConn(verifiedMuxer.connection)
    if not verifiedSession.isNil:
      if startReaderDetached:
        await svc.ensureSessionReaderReady(
          verifiedSession,
          remainingEstablishBudget(),
          messageId,
        )
      notifySendProgress(progress, "dm_session_reuse", %*{
        "dmPhase": "session_reuse",
        "peer": $peer,
        "messageId": messageId,
        "reason": "verified_muxer_conn",
      })
      return (verifiedSession, true, "")

  if allowVerifiedExistingSession:
    let verifiedPeerSession = svc.getLiveSession(peer)
    if not verifiedPeerSession.isNil:
      if startReaderDetached:
        await svc.ensureSessionReaderReady(
          verifiedPeerSession,
          remainingEstablishBudget(),
          messageId,
        )
      notifySendProgress(progress, "dm_session_reuse", %*{
        "dmPhase": "session_reuse",
        "peer": $peer,
        "messageId": messageId,
        "reason": "verified_peer_session",
      })
      return (verifiedPeerSession, true, "")

  if allowGenericExisting:
    let existing = svc.getLiveSession(peer)
    if not existing.isNil:
      if startReaderDetached:
        await svc.ensureSessionReaderReady(
          existing,
          remainingEstablishBudget(),
          messageId,
        )
      notifySendProgress(progress, "dm_session_reuse", %*{
        "dmPhase": "session_reuse",
        "peer": $peer,
        "messageId": messageId,
      })
      return (existing, false, "")

  let dialLock = svc.getOrCreateDialLock(peer)
  var conn: Connection = nil
  var connFromLive = false
  var dialErr = ""

  proc dialFreshConnection() {.async.} =
    if orderedAddrs.len == 0:
      return
    try:
      notifySendProgress(progress, "dm_fresh_dial_begin", %*{
        "dmPhase": "fresh_dial",
        "peer": $peer,
        "codec": svc.codec,
        "addressCount": orderedAddrs.len,
        "source": (if preferredAddrs.len > 0: "preferred" else: "peerstore"),
      })
      debug "direct message fresh dial begin",
        peer = $peer, codec = svc.codec, addrs = orderedAddrs.len,
        source = (if preferredAddrs.len > 0: "preferred" else: "peerstore")
      conn = await svc.switch.dial(
        peer,
        orderedAddrs,
        @[svc.codec],
        forceDial = true,
        reuseConnection = false,
      )
      notifySendProgress(progress, "dm_fresh_dial_ready", %*{
        "dmPhase": "fresh_dial_ready",
        "peer": $peer,
        "codec": svc.codec,
        "addressCount": orderedAddrs.len,
      })
      debug "direct message fresh dial succeeded",
        peer = $peer, codec = svc.codec, addrs = orderedAddrs.len
      connFromLive = false
      dialErr = ""
    except CancelledError as exc:
      raise exc
    except CatchableError as exc:
      dialErr = exc.msg
      notifySendProgress(progress, "dm_fresh_dial_failed", %*{
        "dmPhase": "fresh_dial_failed",
        "peer": $peer,
        "codec": svc.codec,
        "addressCount": orderedAddrs.len,
        "error": dialErr,
      })
      warn "direct message fresh dial failed", peer = $peer, err = dialErr

  if not dialLock.isNil:
    await dialLock.acquire()
  try:
    if preferredConnKey > 0:
      let preferredSession = svc.getLiveSessionByConnKey(preferredConnKey)
      if not preferredSession.isNil:
        if startReaderDetached:
          await svc.ensureSessionReaderReady(
            preferredSession,
            remainingEstablishBudget(),
            messageId,
          )
        notifySendProgress(progress, "dm_session_reuse", %*{
          "dmPhase": "session_reuse",
          "peer": $peer,
          "messageId": messageId,
          "reason": "preferred_conn_key",
          "connKey": preferredConnKey,
        })
        return (preferredSession, false, "")

    if hasVerifiedMuxer:
      let verifiedSession = svc.getLiveSessionByConn(verifiedMuxer.connection)
      if not verifiedSession.isNil:
        if startReaderDetached:
          await svc.ensureSessionReaderReady(
            verifiedSession,
            remainingEstablishBudget(),
            messageId,
          )
        notifySendProgress(progress, "dm_session_reuse", %*{
          "dmPhase": "session_reuse",
          "peer": $peer,
          "messageId": messageId,
          "reason": "verified_muxer_conn",
        })
        return (verifiedSession, true, "")

    if allowVerifiedExistingSession:
      let verifiedPeerSession = svc.getLiveSession(peer)
      if not verifiedPeerSession.isNil:
        if startReaderDetached:
          await svc.ensureSessionReaderReady(
            verifiedPeerSession,
            remainingEstablishBudget(),
            messageId,
          )
        notifySendProgress(progress, "dm_session_reuse", %*{
          "dmPhase": "session_reuse",
          "peer": $peer,
          "messageId": messageId,
          "reason": "verified_peer_session",
        })
        return (verifiedPeerSession, true, "")

    if allowGenericExisting:
      let existing = svc.getLiveSession(peer)
      if not existing.isNil:
        if startReaderDetached:
          await svc.ensureSessionReaderReady(
            existing,
            remainingEstablishBudget(),
            messageId,
          )
        notifySendProgress(progress, "dm_session_reuse", %*{
          "dmPhase": "session_reuse",
          "peer": $peer,
          "messageId": messageId,
        })
        return (existing, false, "")

    if (hasLiveConn() or hasVerifiedMuxer) and not useExplicitFreshDial:
      let liveDialBudget =
        if timeout <= 4.seconds:
          timeout
        else:
          4.seconds
      let liveDialDeadline = Moment.now() + liveDialBudget
      let liveDirection =
        if hasVerifiedMuxer:
          "exact_verified_muxer"
        elif preferredAddrs.len > 0 and reuseVerifiedLiveConnection:
          "outbound_verified"
        else:
          "any_live"
      var liveFailureStage = "stream_open"
      try:
        notifySendProgress(progress, "dm_conn_live_begin", %*{
          "dmPhase": "reuse_live_connection",
          "peer": $peer,
          "codec": svc.codec,
          "timeoutMs": liveDialBudget.milliseconds.int,
          "addressCount": orderedAddrs.len,
          "preferredRoute": preferredAddrs.len > 0,
          "verifiedMuxerProvided": hasVerifiedMuxer,
          "verifiedLiveConnection": reuseVerifiedLiveConnection,
          "directionPreference": liveDirection,
        })
        if hasVerifiedMuxer:
          debug "direct message reusing exact verified muxer for explicit route",
            peer = $peer, codec = svc.codec, addrs = orderedAddrs.len
        elif preferredAddrs.len > 0 and reuseVerifiedLiveConnection:
          debug "direct message reusing verified live connection for explicit route",
            peer = $peer, codec = svc.codec, addrs = orderedAddrs.len
        debug "direct message live connection stream/select begin",
          peer = $peer, codec = svc.codec, timeoutMs = liveDialBudget.milliseconds.int,
          directionPreference = liveDirection
        var liveConn: Connection = nil
        var adoptedLiveConn = false
        try:
          let streamTimeoutMs = max((liveDialDeadline - Moment.now()).milliseconds.int64, 0'i64)
          notifySendProgress(progress, "dm_conn_live_stream_begin", %*{
            "dmPhase": "reuse_live_connection_stream_begin",
            "peer": $peer,
            "codec": svc.codec,
            "timeoutMs": streamTimeoutMs,
            "directionPreference": liveDirection,
          })
          if streamTimeoutMs <= 0:
            raise (ref LPError)(msg: "live connection dial timeout")
          if hasVerifiedMuxer:
            liveConn = await awaitFutureWithin(
              svc.switch.connManager.getStream(verifiedMuxer),
              liveDialDeadline - Moment.now(),
              "live connection dial timeout",
            )
          elif preferredAddrs.len > 0 and reuseVerifiedLiveConnection:
            liveConn = await awaitFutureWithin(
              svc.switch.connManager.getStream(peer, Direction.Out),
              liveDialDeadline - Moment.now(),
              "live connection dial timeout",
            )
          else:
            liveConn = await awaitFutureWithin(
              svc.switch.connManager.getStream(peer),
              liveDialDeadline - Moment.now(),
              "live connection dial timeout",
            )
          if liveConn.isNil:
            raise (ref LPError)(msg: "live connection stream unavailable")
          notifySendProgress(progress, "dm_conn_live_stream_ready", %*{
            "dmPhase": "reuse_live_connection_stream_ready",
            "peer": $peer,
            "codec": svc.codec,
            "directionPreference": liveDirection,
          })
          liveFailureStage = "protocol_select"
          let selectTimeoutMs = max((liveDialDeadline - Moment.now()).milliseconds.int64, 0'i64)
          notifySendProgress(progress, "dm_conn_live_select_begin", %*{
            "dmPhase": "reuse_live_connection_select_begin",
            "peer": $peer,
            "codec": svc.codec,
            "timeoutMs": selectTimeoutMs,
          })
          if selectTimeoutMs <= 0:
            raise (ref LPError)(msg: "live connection dial timeout")
          await awaitFutureWithin(
            liveConn.beginProtocolNegotiation(),
            liveDialDeadline - Moment.now(),
            "live connection negotiation begin timeout",
          )
          try:
            let selected = await awaitFutureWithin(
              MultistreamSelect.select(liveConn, svc.codec),
              liveDialDeadline - Moment.now(),
              "live connection protocol select timeout",
            )
            if not selected:
              raise (ref LPError)(msg: "live connection protocol select rejected")
          finally:
            liveConn.endProtocolNegotiation()
          notifySendProgress(progress, "dm_conn_live_select_ready", %*{
            "dmPhase": "reuse_live_connection_select_ready",
            "peer": $peer,
            "codec": svc.codec,
          })
          conn = liveConn
          adoptedLiveConn = true
          connFromLive = true
          dialErr = ""
          notifySendProgress(progress, "dm_conn_live_ready", %*{
            "dmPhase": "reuse_live_connection_ready",
            "peer": $peer,
            "codec": svc.codec,
            "addressCount": orderedAddrs.len,
            "preferredRoute": preferredAddrs.len > 0,
            "verifiedMuxerProvided": hasVerifiedMuxer,
            "verifiedLiveConnection": reuseVerifiedLiveConnection,
            "directionPreference": liveDirection,
          })
          debug "direct message reused live connection",
            peer = $peer, codec = svc.codec, directionPreference = liveDirection
        finally:
          if not adoptedLiveConn and not liveConn.isNil:
            try:
              await liveConn.closeWithEOF()
            except CatchableError:
              discard
      except CancelledError as exc:
        raise exc
      except CatchableError as exc:
        dialErr = exc.msg
        notifySendProgress(progress, "dm_conn_live_failed", %*{
          "dmPhase": "reuse_live_connection_failed",
          "peer": $peer,
          "codec": svc.codec,
          "error": dialErr,
          "failureStage": liveFailureStage,
          "directionPreference": liveDirection,
        })
        warn "direct message live connection dial failed", peer = $peer, err = dialErr
    elif hasLiveConn() and useExplicitFreshDial:
      notifySendProgress(progress, "dm_conn_live_skipped", %*{
        "dmPhase": "reuse_live_connection_skipped",
        "peer": $peer,
        "codec": svc.codec,
        "reason": "preferred_addrs",
        "addressCount": orderedAddrs.len,
      })
      debug "direct message skipping live connection reuse for explicit route",
        peer = $peer, codec = svc.codec, addrs = orderedAddrs.len

    if conn.isNil and orderedAddrs.len > 0 and not forbidFreshDialAfterVerifiedReuse:
      await dialFreshConnection()

    if conn.isNil and dialErr.len == 0 and not hasLiveConn():
      dialErr =
        if orderedAddrs.len == 0:
          "no stored addresses"
        elif forbidFreshDialAfterVerifiedReuse:
          "verified live muxer unavailable"
        else:
          "no existing connection"

    if conn.isNil:
      notifySendProgress(progress, "dm_connection_missing", %*{
        "dmPhase": "connection_missing",
        "peer": $peer,
        "addressCount": orderedAddrs.len,
        "error": (if dialErr.len > 0: dialErr else: "dial failed without fallback"),
      })
      return (nil, connFromLive, dialErr)

    let created = svc.ensureSessionFromConnection(
      conn,
      (if hasVerifiedMuxer: verifiedMuxer.connection else: nil),
      startReaderDetached,
    )
    if created.isNil:
      dialErr = "direct message session unavailable"
      return (nil, connFromLive, dialErr)
    if startReaderDetached:
      await svc.ensureSessionReaderReady(
        created,
        remainingEstablishBudget(),
        messageId,
      )
    (created, connFromLive, dialErr)
  finally:
    releaseAsyncLock(dialLock)

proc warmSessionDetailed*(
    svc: DirectMessageService,
    peer: PeerId,
    timeout: Duration = 4.seconds,
    preferredAddrs: seq[MultiAddress] = @[],
    verifiedMuxer: Muxer = nil,
    preferredConnKey = 0,
    reuseVerifiedLiveConnection = false,
    progress: DirectMessageSendProgress = nil
): Future[DirectMessageWarmResult] {.async.} =
  if svc.isNil or svc.switch.isNil:
    return DirectMessageWarmResult(
      ok: false,
      err: "direct message service unavailable",
    )
  let warmRes = await svc.establishOutboundSession(
    peer,
    timeout,
    preferredAddrs = preferredAddrs,
    verifiedMuxer = verifiedMuxer,
    preferredConnKey = preferredConnKey,
    reuseVerifiedLiveConnection = reuseVerifiedLiveConnection,
    progress = progress,
    allowExisting = preferredAddrs.len == 0 and verifiedMuxer.isNil,
    startReaderDetached = false,
  )
  if warmRes.session.isNil:
    let errMsg =
      if warmRes.dialErr.len > 0:
        warmRes.dialErr
      else:
        "direct message session unavailable"
    notifySendProgress(progress, "dm_session_warm_failed", %*{
      "dmPhase": "session_warm_failed",
      "peer": $peer,
        "error": errMsg,
      })
    return DirectMessageWarmResult(
      ok: false,
      err: errMsg,
    )
  notifySendProgress(progress, "dm_session_warm_ready", %*{
    "dmPhase": "session_warm_ready",
    "peer": $peer,
    "readerActive": warmRes.session.readerActive,
    "reusedLiveConnection": warmRes.connFromLive,
    "connKey": warmRes.session.connKey,
  })
  debug "direct message session warmed",
    peer = $peer, readerActive = warmRes.session.readerActive,
    reusedLiveConnection = warmRes.connFromLive
  DirectMessageWarmResult(
    ok: true,
    err: "",
    connKey: warmRes.session.connKey,
    reusedLiveConnection: warmRes.connFromLive,
    readerActive: warmRes.session.readerActive,
  )

proc warmSession*(
    svc: DirectMessageService,
    peer: PeerId,
    timeout: Duration = 4.seconds,
    preferredAddrs: seq[MultiAddress] = @[],
    verifiedMuxer: Muxer = nil,
    preferredConnKey = 0,
    reuseVerifiedLiveConnection = false,
    progress: DirectMessageSendProgress = nil
): Future[(bool, string)] {.async.} =
  let warmRes = await svc.warmSessionDetailed(
    peer,
    timeout,
    preferredAddrs = preferredAddrs,
    verifiedMuxer = verifiedMuxer,
    preferredConnKey = preferredConnKey,
    reuseVerifiedLiveConnection = reuseVerifiedLiveConnection,
    progress = progress,
  )
  (warmRes.ok, warmRes.err)

proc send*(
    svc: DirectMessageService,
    peer: PeerId,
    payload: seq[byte],
    ackRequested: bool,
    messageId: string,
    timeout: Duration = 12.seconds,
    preferredAddrs: seq[MultiAddress] = @[],
    verifiedMuxer: Muxer = nil,
    preferredConnKey = 0,
    reuseVerifiedLiveConnection = false,
    progress: DirectMessageSendProgress = nil
): Future[(bool, string)] {.async.} =
  if svc.isNil or svc.switch.isNil:
    return (false, "direct message service unavailable")

  let sendDeadline = Moment.now() + timeout
  var session: DirectMessageSession = nil
  var dialErr = ""
  var connFromLive = false

  proc sendOnSession(activeSession: DirectMessageSession): Future[(bool, string, bool)] {.async.} =
    proc remainingSendBudget(): Duration =
      let remaining = sendDeadline - Moment.now()
      if remaining > ZeroDuration:
        remaining
      else:
        ZeroDuration

    var ackWaiter: DirectAckWaiter = nil
    if ackRequested:
      if messageId.len == 0:
        return (false, "missing message id", false)
      ackWaiter = newDirectAckWaiter()
      if svc.ackWaiters.hasKey(messageId):
        svc.ackWaiters.del(messageId)
      svc.ackWaiters[messageId] = ackWaiter

    try:
      let writeBudget = remainingSendBudget()
      notifySendProgress(progress, "dm_write_begin", %*{
        "dmPhase": "write_begin",
        "peer": $peer,
        "bytes": payload.len,
        "ackRequested": ackRequested,
        "messageId": messageId,
        "timeoutMs": writeBudget.milliseconds.int,
      })
      debug "direct message writeLp begin",
        peer = $peer, bytes = payload.len, ackRequested = ackRequested, mid = messageId
      if writeBudget <= ZeroDuration:
        raise (ref LPError)(msg: "direct message write timeout")
      let writeRes = await sendFrameOnSession(
        activeSession,
        payload,
        writeBudget,
        "direct message write timeout",
        frameKind = DmFrameMessage,
        messageId = messageId,
      )
      if not writeRes[0]:
        raise (ref LPError)(msg: writeRes[1])
      notifySendProgress(progress, "dm_write_done", %*{
        "dmPhase": "write_done",
        "peer": $peer,
        "bytes": payload.len,
        "ackRequested": ackRequested,
        "messageId": messageId,
      })
      debug "direct message writeLp done",
        peer = $peer, bytes = payload.len, ackRequested = ackRequested, mid = messageId
      debug "direct message payload sent", peer = $peer, bytes = payload.len, ackRequested = ackRequested, mid = messageId
      if not sessionReaderRunning(activeSession):
        svc.startSessionReader(activeSession)
        notifySendProgress(progress, "dm_reader_attach", %*{
          "dmPhase": "reader_attach",
          "peer": $peer,
          "messageId": messageId,
        })
    except CatchableError as exc:
      notifySendProgress(progress, "dm_write_failed", %*{
        "dmPhase": "write_failed",
        "peer": $peer,
        "messageId": messageId,
        "error": exc.msg,
        "retryableFreshDial": isRetryableStaleConnError(exc) or exc.msg == "direct message write timeout",
      })
      warn "direct message write path failed", peer = $peer, mid = messageId, err = exc.msg
      try:
        await closeSessionConn(activeSession)
      except CatchableError:
        discard
      svc.removeSession(activeSession)
      if ackRequested and messageId.len > 0 and svc.ackWaiters.hasKey(messageId):
        svc.ackWaiters.del(messageId)
      return (
        false,
        exc.msg,
        isRetryableStaleConnError(exc) or exc.msg == "direct message write timeout"
      )

    if not ackRequested or messageId.len == 0:
      return (true, "", false)

    let ackBudget = remainingSendBudget()
    let timeoutFuture = sleepAsync(ackBudget)
    enterSessionActivity(activeSession, DmActivityAwaitingAck, messageId)
    notifySendProgress(progress, "dm_ack_wait", %*{
      "dmPhase": "ack_wait",
      "peer": $peer,
      "messageId": messageId,
      "timeoutMs": ackBudget.milliseconds.int,
    })
    debug "direct message ack wait begin",
      peer = $peer, mid = messageId, timeoutMs = ackBudget.milliseconds.int
    if ackBudget <= ZeroDuration:
      leaveSessionActivity(activeSession, DmActivityAwaitingAck, messageId, "ack timeout")
      if svc.ackWaiters.hasKey(messageId):
        svc.ackWaiters.del(messageId)
      return (false, "ack timeout", false)
    let timedOut =
      try:
        discard await race(ackWaiter.future, timeoutFuture)
        not ackWaiter.future.finished()
      except CancelledError as exc:
        raise exc
      except CatchableError as exc:
        notifySendProgress(progress, "dm_ack_wait_failed", %*{
          "dmPhase": "ack_wait_failed",
          "peer": $peer,
          "messageId": messageId,
          "error": exc.msg,
          "retryableFreshDial": isRetryableStaleConnError(exc),
        })
        warn "direct message ack wait failed", peer = $peer, err = exc.msg
        try:
          await closeSessionConn(activeSession)
        except CatchableError:
          discard
        svc.removeSession(activeSession)
        if svc.ackWaiters.hasKey(messageId):
          svc.ackWaiters.del(messageId)
        return (false, exc.msg, isRetryableStaleConnError(exc))
      finally:
        if not timeoutFuture.finished():
          timeoutFuture.cancelSoon()
        leaveSessionActivity(activeSession, DmActivityAwaitingAck, messageId)

    if timedOut:
      if svc.ackWaiters.hasKey(messageId):
        svc.ackWaiters.del(messageId)
      notifySendProgress(progress, "dm_ack_timeout", %*{
        "dmPhase": "ack_timeout",
        "peer": $peer,
        "messageId": messageId,
        "timeoutMs": ackBudget.milliseconds.int,
        "error": "ack timeout",
      })
      debug "direct message ack timeout", peer = $peer, mid = messageId, timeoutMs = ackBudget.milliseconds.int
      return (false, "ack timeout", false)

    try:
      discard await ackWaiter.future
    except CatchableError as exc:
      notifySendProgress(progress, "dm_ack_read_failed", %*{
        "dmPhase": "ack_read_failed",
        "peer": $peer,
        "messageId": messageId,
        "error": exc.msg,
        "retryableFreshDial": isRetryableStaleConnError(exc),
      })
      if svc.ackWaiters.hasKey(messageId):
        svc.ackWaiters.del(messageId)
      return (false, exc.msg, isRetryableStaleConnError(exc))

    if svc.ackWaiters.hasKey(messageId):
      svc.ackWaiters.del(messageId)
    if not ackWaiter.future.read():
      let ackErr =
        if ackWaiter.error.len > 0: ackWaiter.error
        else: "ack failed"
      warn "direct message ack reported failure", peer = $peer, mid = messageId, err = ackErr
      notifySendProgress(progress, "dm_ack_failed", %*{
        "dmPhase": "ack_failed",
        "peer": $peer,
        "messageId": messageId,
        "error": ackErr,
      })
      return (false, ackErr, false)

    notifySendProgress(progress, "dm_ack_done", %*{
      "dmPhase": "ack_done",
      "peer": $peer,
      "messageId": messageId,
    })
    debug "direct message ack received", peer = $peer, mid = messageId
    (true, "", false)

  let established = await svc.establishOutboundSession(
    peer,
    timeout,
    preferredAddrs = preferredAddrs,
    verifiedMuxer = verifiedMuxer,
    preferredConnKey = preferredConnKey,
    reuseVerifiedLiveConnection = reuseVerifiedLiveConnection,
    progress = progress,
    messageId = messageId,
    allowExisting = true,
    startReaderDetached = true,
  )
  session = established.session
  connFromLive = established.connFromLive
  dialErr = established.dialErr
  if session.isNil:
    debug "direct message dial aborted without fallback", peer = $peer, err = dialErr
    return (false, if dialErr.len > 0: dialErr else: "dial failed without fallback")

  var sendResult = await sendOnSession(session)
  if not sendResult[0] and connFromLive and preferredAddrs.len > 0 and sendResult[2]:
    notifySendProgress(progress, "dm_live_stale_retry", %*{
      "dmPhase": "live_stale_retry",
      "peer": $peer,
      "messageId": messageId,
      "error": sendResult[1],
      "addressCount": preferredAddrs.len,
    })
    warn "direct message live connection stale, retrying fresh dial",
      peer = $peer, mid = messageId, err = sendResult[1]
    svc.removeSession(session)
    try:
      await closeSessionConn(session)
    except CatchableError:
      discard
    let retryEstablished = await svc.establishOutboundSession(
      peer,
      timeout,
      preferredAddrs = preferredAddrs,
      verifiedMuxer = nil,
      preferredConnKey = 0,
      reuseVerifiedLiveConnection = false,
      progress = progress,
      messageId = messageId,
      allowExisting = false,
      startReaderDetached = true,
    )
    session = retryEstablished.session
    connFromLive = retryEstablished.connFromLive
    dialErr = retryEstablished.dialErr
    if session.isNil:
      return (false, if dialErr.len > 0: dialErr else: sendResult[1])
    sendResult = await sendOnSession(session)

  (sendResult[0], sendResult[1])

proc sendAuthoritativeAck(
    svc: DirectMessageService,
    session: DirectMessageSession,
    payload: seq[byte],
    messageId: string,
    timeout: Duration
): Future[(bool, string)] {.gcsafe, raises: [].} =
  {.cast(gcsafe).}:
    if session.isNil:
      let failed = newFuture[(bool, string)]("libp2p.directmsg.ack.unavailable")
      failed.complete((false, "ack session unavailable"))
      return failed
    let ackFuture = newFuture[(bool, string)]("libp2p.directmsg.ack.same_session")
    proc runner() {.async.} =
      try:
        let writeRes = await sendFrameOnSession(
          session,
          payload,
          timeout,
          "direct message ack write timeout",
          frameKind = DmFrameAck,
          messageId = messageId,
        )
        ackFuture.complete(writeRes)
      except CatchableError as exc:
        ackFuture.complete((false, exc.msg))
    asyncSpawn runner()
    return ackFuture
