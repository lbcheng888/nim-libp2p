import std/[algorithm, json, options, sequtils, sets, strutils, tables]
from std/times import epochTime
import chronicles
import chronos
import ../../peerid
import ../../peerstore
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

  DirectMessageHandler* = proc(msg: DirectMessage) {.async.}
  DirectMessageSendProgress* = proc(stage: string, detail: JsonNode) {.gcsafe, raises: [].}

  DirectMessageService* = ref object of LPProtocol
    switch*: Switch
    selfPeer*: PeerId
    messageHandler*: DirectMessageHandler
    maxMessageBytes*: int
    maxAckBytes*: int

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

proc closeWriteAndCloseLater(
    conn: Connection,
    closeWriteDelay: Duration,
    closeDelay: Duration
) =
  if conn.isNil:
    return
  proc runner() {.async.} =
    try:
      await sleepAsync(closeWriteDelay)
      await conn.closeWrite()
    except CatchableError:
      discard
    try:
      let remaining = closeDelay - closeWriteDelay
      if remaining > ZeroDuration:
        await sleepAsync(remaining)
      await conn.close()
    except CatchableError:
      discard
  asyncSpawn runner()

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

proc handleIncoming(
    svc: DirectMessageService, conn: Connection
) {.async: (raises: [CancelledError]).} =
  var ackRequested = false
  var messageId = ""
  var remotePeer = ""
  var ackSent = false
  try:
    remotePeer = $conn.peerId
  except CatchableError:
    remotePeer = ""

  try:
    debug "direct message readLp begin", peer = remotePeer, maxBytes = svc.maxMessageBytes
    let payload = await conn.readLp(svc.maxMessageBytes)
    debug "direct message readLp done", peer = remotePeer, size = payload.len
    if payload.len == 0:
      debug "direct message empty payload", peer = remotePeer
      return

    debug "direct message received", peer = remotePeer, size = payload.len
    var parsedEnvelope: JsonNode
    let rawPayload =
      try:
        bytesToString(payload)
      except CatchableError:
        ""
    if rawPayload.len > 0:
      try:
        parsedEnvelope = parseJson(rawPayload)
        if parsedEnvelope.kind == JObject:
          ackRequested = jsonGetBool(parsedEnvelope, "ackRequested", false)
          messageId = jsonGetStr(parsedEnvelope, "mid")
          if ackRequested:
            debug "direct message requests ack", peer = remotePeer, mid = messageId
      except CatchableError:
        ackRequested = false
        messageId = ""

    var directMsg = DirectMessage(id: 0, sender: conn.peerId, payload: payload)
    try:
      await svc.messageHandler(directMsg)
    except CatchableError as exc:
      if ackRequested and messageId.len > 0:
        let ackBytes = buildAckPayload(svc, messageId, false, exc.msg)
        try:
          debug "direct message sending failure ack", peer = remotePeer, mid = messageId
          await conn.writeLp(ackBytes)
          debug "direct message failure ack sent", peer = remotePeer, mid = messageId
          ackSent = true
        except CatchableError as sendErr:
          warn "direct ack send failed", peer = remotePeer, err = sendErr.msg
      debug "direct message handler error", peer = remotePeer, err = exc.msg

    if ackRequested and messageId.len > 0:
      let ackBytes = buildAckPayload(svc, messageId, true, "")
      try:
        debug "direct message sending ack", peer = remotePeer, mid = messageId
        await conn.writeLp(ackBytes)
        debug "direct message ack sent", peer = remotePeer, mid = messageId
        ackSent = true
      except CatchableError as sendErr:
        warn "direct ack send failed", peer = remotePeer, err = sendErr.msg
    elif ackRequested:
      warn "direct message requested ack but missing message id", peer = remotePeer
  except CancelledError as exc:
    raise exc
  except LPStreamError as lpErr:
    debug "direct message stream error", peer = remotePeer, err = lpErr.msg
  except CatchableError as exc:
    debug "direct message handler failed", peer = remotePeer, err = exc.msg
  finally:
    try:
      debug "direct message handler closeWrite begin", peer = remotePeer
      await conn.closeWrite()
      debug "direct message handler closeWrite done", peer = remotePeer
    except CatchableError:
      discard
    if ackRequested and ackSent:
      debug "direct message handler delayed close scheduled", peer = remotePeer, mid = messageId
      closeConnLater(conn, 1.seconds)
    else:
      try:
        debug "direct message handler close begin", peer = remotePeer
        await conn.close()
        debug "direct message handler close done", peer = remotePeer
      except CatchableError:
        discard

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
  )
  svc.codec = codec
  svc.codecs = @[codec]

  proc handle(conn: Connection, proto: string) {.async: (raises: [CancelledError]), gcsafe, closure.} =
    await svc.handleIncoming(conn)

  svc.handler = handle
  svc

proc send*(
    svc: DirectMessageService,
    peer: PeerId,
    payload: seq[byte],
    ackRequested: bool,
    messageId: string,
    timeout: Duration = 12.seconds,
    preferredAddrs: seq[MultiAddress] = @[],
    progress: DirectMessageSendProgress = nil
): Future[(bool, string)] {.async.} =
  if svc.isNil or svc.switch.isNil:
    return (false, "direct message service unavailable")

  var conn: Connection = nil
  var dialErr = ""
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
  let hasConn =
    (not svc.switch.connManager.isNil) and svc.switch.connManager.connCount(peer) > 0
  if hasConn:
    try:
      let liveDialBudget =
        if timeout <= 4.seconds:
          timeout
        else:
          4.seconds
      notifySendProgress(progress, "dm_conn_live_begin", %*{
        "dmPhase": "reuse_live_connection",
        "peer": $peer,
        "codec": svc.codec,
        "timeoutMs": liveDialBudget.milliseconds.int,
      })
      debug "direct message live connection dial begin",
        peer = $peer, codec = svc.codec, timeoutMs = liveDialBudget.milliseconds.int
      let liveDialFuture = svc.switch.dial(peer, @[svc.codec])
      let liveTimeoutFuture = sleepAsync(liveDialBudget)
      let liveTimedOut =
        try:
          discard await race(liveDialFuture, liveTimeoutFuture)
          not liveDialFuture.finished()
        finally:
          if not liveTimeoutFuture.finished():
            liveTimeoutFuture.cancelSoon()
      if liveTimedOut:
        dialErr = "live connection dial timeout"
        if not liveDialFuture.finished():
          liveDialFuture.cancelSoon()
        notifySendProgress(progress, "dm_conn_live_failed", %*{
          "dmPhase": "reuse_live_connection_failed",
          "peer": $peer,
          "codec": svc.codec,
          "timeoutMs": liveDialBudget.milliseconds.int,
          "error": dialErr,
        })
        warn "direct message live connection dial timed out",
          peer = $peer, codec = svc.codec, timeoutMs = liveDialBudget.milliseconds.int
      else:
        conn = await liveDialFuture
        notifySendProgress(progress, "dm_conn_live_ready", %*{
          "dmPhase": "reuse_live_connection_ready",
          "peer": $peer,
          "codec": svc.codec,
        })
        debug "direct message reused live connection", peer = $peer, codec = svc.codec
    except CancelledError as exc:
      raise exc
    except CatchableError as exc:
      dialErr = exc.msg
      notifySendProgress(progress, "dm_conn_live_failed", %*{
        "dmPhase": "reuse_live_connection_failed",
        "peer": $peer,
        "codec": svc.codec,
        "error": dialErr,
      })
      warn "direct message live connection dial failed", peer = $peer, err = dialErr

  if conn.isNil and orderedAddrs.len > 0:
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

  if conn.isNil and dialErr.len == 0 and not hasConn:
    dialErr = "no existing connection"

  if conn.isNil:
    notifySendProgress(progress, "dm_connection_missing", %*{
      "dmPhase": "connection_missing",
      "peer": $peer,
      "addressCount": orderedAddrs.len,
      "error": (if dialErr.len > 0: dialErr else: "dial failed without fallback"),
    })
    if orderedAddrs.len == 0:
      debug "direct message dial aborted without fallback; no stored addresses", peer = $peer
      return (false, if dialErr.len > 0: dialErr else: "no stored addresses")
    debug "direct message dial aborted without fallback", peer = $peer, err = dialErr
    return (false, if dialErr.len > 0: dialErr else: "dial failed without fallback")

  try:
    notifySendProgress(progress, "dm_write_begin", %*{
      "dmPhase": "write_begin",
      "peer": $peer,
      "bytes": payload.len,
      "ackRequested": ackRequested,
      "messageId": messageId,
    })
    debug "direct message writeLp begin",
      peer = $peer, bytes = payload.len, ackRequested = ackRequested, mid = messageId
    await conn.writeLp(payload)
    notifySendProgress(progress, "dm_write_done", %*{
      "dmPhase": "write_done",
      "peer": $peer,
      "bytes": payload.len,
      "ackRequested": ackRequested,
      "messageId": messageId,
    })
    debug "direct message writeLp done",
      peer = $peer, bytes = payload.len, ackRequested = ackRequested, mid = messageId
    if not ackRequested:
      debug "direct message closeWrite begin", peer = $peer, mid = messageId
      await conn.closeWrite()
      debug "direct message closeWrite done", peer = $peer, mid = messageId
      debug "direct message delayed close scheduled", peer = $peer, mid = messageId
      closeConnLater(conn, 1200.milliseconds)
    debug "direct message payload sent", peer = $peer, bytes = payload.len, ackRequested = ackRequested, mid = messageId
  except CatchableError as exc:
    notifySendProgress(progress, "dm_write_failed", %*{
      "dmPhase": "write_failed",
      "peer": $peer,
      "messageId": messageId,
      "error": exc.msg,
    })
    warn "direct message write path failed", peer = $peer, mid = messageId, err = exc.msg
    try:
      await conn.close()
    except CatchableError:
      discard
    return (false, exc.msg)

  if not ackRequested or messageId.len == 0:
    return (true, "")

  let ackFuture = conn.readLp(svc.maxAckBytes)
  let timeoutFuture = sleepAsync(timeout)
  notifySendProgress(progress, "dm_ack_wait", %*{
    "dmPhase": "ack_wait",
    "peer": $peer,
    "messageId": messageId,
    "timeoutMs": timeout.milliseconds.int,
  })
  debug "direct message ack wait begin",
    peer = $peer, mid = messageId, timeoutMs = timeout.milliseconds.int
  let timedOut =
    try:
      discard await race(ackFuture, timeoutFuture)
      not ackFuture.finished()
    except CancelledError as exc:
      raise exc
    except CatchableError as exc:
      notifySendProgress(progress, "dm_ack_wait_failed", %*{
        "dmPhase": "ack_wait_failed",
        "peer": $peer,
        "messageId": messageId,
        "error": exc.msg,
      })
      warn "direct message ack wait failed", peer = $peer, err = exc.msg
      try:
        await conn.close()
      except CatchableError:
        discard
      return (false, exc.msg)
    finally:
      if not timeoutFuture.finished():
        timeoutFuture.cancelSoon()

  if timedOut:
    notifySendProgress(progress, "dm_ack_timeout", %*{
      "dmPhase": "ack_timeout",
      "peer": $peer,
      "messageId": messageId,
      "timeoutMs": timeout.milliseconds.int,
      "error": "ack timeout",
    })
    debug "direct message ack timeout", peer = $peer, mid = messageId, timeoutMs = timeout.milliseconds.int
    closeConnSoon(conn)
    return (false, "ack timeout")

  var ackBytes: seq[byte]
  try:
    ackBytes = await ackFuture
  except CatchableError as exc:
    notifySendProgress(progress, "dm_ack_read_failed", %*{
      "dmPhase": "ack_read_failed",
      "peer": $peer,
      "messageId": messageId,
      "error": exc.msg,
    })
    closeConnSoon(conn)
    return (false, exc.msg)

  let ackResult = parseAckPayload(ackBytes)
  try:
    debug "direct message post-ack closeWrite begin", peer = $peer, mid = messageId
    await conn.closeWrite()
    debug "direct message post-ack closeWrite done", peer = $peer, mid = messageId
  except CatchableError as closeErr:
    warn "direct message post-ack closeWrite failed", peer = $peer, err = closeErr.msg
  closeConnSoon(conn)

  if not ackResult.ok or ackResult.mid != messageId:
    notifySendProgress(progress, "dm_ack_invalid", %*{
      "dmPhase": "ack_invalid",
      "peer": $peer,
      "messageId": messageId,
      "receivedMessageId": ackResult.mid,
      "error": "invalid ack payload",
    })
    warn "direct message received invalid ack", peer = $peer, expected = messageId, received = ackResult.mid
    return (false, "invalid ack payload")

  if not ackResult.success:
    if ackResult.error.len > 0:
      warn "direct message ack reported failure", peer = $peer, mid = messageId, err = ackResult.error
    notifySendProgress(progress, "dm_ack_failed", %*{
      "dmPhase": "ack_failed",
      "peer": $peer,
      "messageId": messageId,
      "error": (if ackResult.error.len > 0: ackResult.error else: "ack failed"),
    })
    return (false, if ackResult.error.len > 0: ackResult.error else: "ack failed")

  notifySendProgress(progress, "dm_ack_done", %*{
    "dmPhase": "ack_done",
    "peer": $peer,
    "messageId": messageId,
  })
  debug "direct message ack received", peer = $peer, mid = messageId
  (true, "")
