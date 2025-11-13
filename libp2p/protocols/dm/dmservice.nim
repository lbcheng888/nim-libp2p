import std/[json, options, strutils, tables]
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
  DefaultDirectMessageMaxBytes* = 512 * 1024
  DefaultDirectAckMaxBytes* = 64 * 1024

type
  DirectMessage* = object
    id*: uint64
    sender*: PeerId
    payload*: seq[byte]

  DirectMessageHandler* = proc(msg: DirectMessage) {.async.}

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
  try:
    remotePeer = $conn.peerId
  except CatchableError:
    remotePeer = ""

  try:
    let payload = await conn.readLp(svc.maxMessageBytes)
    if payload.len == 0:
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
          await conn.writeLp(ackBytes)
        except CatchableError as sendErr:
          warn "direct ack send failed", peer = remotePeer, err = sendErr.msg
      debug "direct message handler error", peer = remotePeer, err = exc.msg

    if ackRequested and messageId.len > 0:
      let ackBytes = buildAckPayload(svc, messageId, true, "")
      try:
        debug "direct message sending ack", peer = remotePeer, mid = messageId
        await conn.writeLp(ackBytes)
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
      await conn.close()
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
    timeout: Duration = 12.seconds
): Future[(bool, string)] {.async.} =
  if svc.isNil or svc.switch.isNil:
    return (false, "direct message service unavailable")

  var conn: Connection = nil
  try:
    conn = await svc.switch.dial(peer, @[svc.codec])
    debug "direct message dialed peer", peer = $peer, codec = svc.codec
  except CancelledError as exc:
    raise exc
  except CatchableError as exc:
    warn "direct message dial failed", peer = $peer, err = exc.msg
    let errLower = exc.msg.toLowerAscii()
    if "unable to select sub-protocol" in errLower or "error in newstream" in errLower or "error in new stream" in errLower:
      var fallbackConn: Connection = nil
      var dialAddrs: seq[MultiAddress] = @[]
      try:
        if not svc.switch.connManager.isNil:
          debug "direct message dropping peer before fallback", peer = $peer
          await svc.switch.connManager.dropPeer(peer)
      except CatchableError as dropErr:
        warn "direct message dropPeer failed", peer = $peer, err = dropErr.msg
      try:
        let store = svc.switch.peerStore
        if not store.isNil:
          let addrBook = store[AddressBook]
          if addrBook.book.hasKey(peer):
            dialAddrs = addrBook.book.getOrDefault(peer)
      except CatchableError:
        discard
      if dialAddrs.len == 0:
        debug "direct message fallback has no stored addresses", peer = $peer
        return (false, exc.msg)
      try:
        fallbackConn = await svc.switch.dial(peer, dialAddrs, @[svc.codec], forceDial = true)
        conn = fallbackConn
        debug "direct message fallback dial succeeded", peer = $peer, codec = svc.codec
      except CatchableError as fallbackErr:
        warn "direct message fallback dial failed", peer = $peer, err = fallbackErr.msg
        return (false, exc.msg)
    else:
      return (false, exc.msg)

  try:
    await conn.writeLp(payload)
    debug "direct message payload sent", peer = $peer, bytes = payload.len, ackRequested = ackRequested, mid = messageId
  except CatchableError as exc:
    try:
      await conn.close()
    except CatchableError:
      discard
    return (false, exc.msg)

  if not ackRequested or messageId.len == 0:
    try:
      await conn.close()
    except CatchableError:
      discard
    return (true, "")

  let ackFuture = conn.readLp(svc.maxAckBytes)
  let completed =
    try:
      await withTimeout(ackFuture, timeout)
    except CancelledError as exc:
      raise exc
    except CatchableError as exc:
      warn "direct message ack wait failed", peer = $peer, err = exc.msg
      try:
        await conn.close()
      except CatchableError:
        discard
      return (false, exc.msg)

  if not completed:
    debug "direct message ack timeout", peer = $peer, mid = messageId, timeoutMs = timeout.milliseconds.int
    try:
      await conn.close()
    except CatchableError:
      discard
    return (false, "ack timeout")

  var ackBytes: seq[byte]
  try:
    ackBytes = await ackFuture
  except CatchableError as exc:
    try:
      await conn.close()
    except CatchableError:
      discard
    return (false, exc.msg)

  let ackResult = parseAckPayload(ackBytes)
  try:
    await conn.close()
  except CatchableError:
    discard

  if not ackResult.ok or ackResult.mid != messageId:
    warn "direct message received invalid ack", peer = $peer, expected = messageId, received = ackResult.mid
    return (false, "invalid ack payload")

  if not ackResult.success:
    if ackResult.error.len > 0:
      warn "direct message ack reported failure", peer = $peer, mid = messageId, err = ackResult.error
    return (false, if ackResult.error.len > 0: ackResult.error else: "ack failed")

  debug "direct message ack received", peer = $peer, mid = messageId
  (true, "")
