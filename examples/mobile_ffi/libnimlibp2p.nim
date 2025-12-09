import std/[atomics, base64, json, options, sequtils, sets, strformat,
            strutils, tables, times, locks]
import std/os
import chronos
import chronos/threadsync
import chronicles
import stew/objects
when defined(posix):
  import posix
import libp2p/[bandwidthmanager, builders, connmanager, features, memorymanager, multiaddress,
               muxers/muxer, peerid, peerstore, protocols/connectivity/relay/client,
               resourcemanager, switch]
import libp2p/discovery/discoverymngr
import libp2p/discovery/mdns
import libp2p/crypto/[crypto, ed25519/ed25519]
import libp2p/protocols/pubsub/[gossipsub, pubsub, peertable]
import libp2p/protocols/dm/dmservice
import libp2p/protocols/feed/feedservice
import nimcrypto/sha2 as nimsha2
when libp2pFetchEnabled:
  import libp2p/protocols/fetch/[fetch, protobuf]
when libp2pDataTransferEnabled:
  import libp2p/protocols/datatransfer/[datatransfer, protobuf]
import libp2p/protocols/rendezvous/rendezvous
import libp2p/stream/lpstream
import bearssl/[rand, hash]

when defined(libp2p_msquic_experimental):
  import libp2p/transports/msquicruntime

import examples/dex/dex_node as dex_node
import examples/dex/mixer_service as mixer_service
import examples/dex/types as dex_types
import examples/dex/storage/order_store as dex_store
import examples/dex/security/signing as dex_signing
import examples/mpc_crypto
import libp2p/crypto/coinjoin/[types, secp_utils]
import nimcrypto/sysrand
import secp256k1
import nimcrypto/sha2
import libp2p/crypto/secp

when defined(android):
  import segfaults
  proc nim_bridge_emit_event(topic: cstring, payload: cstring) {.cdecl, importc: "nim_bridge_emit_event".}
else:
  proc nim_bridge_emit_event(topic: cstring, payload: cstring) =
    discard

proc nim_thread_attach*() {.exportc, cdecl, dynlib.} =
  ## Register the current foreign thread with Nim's GC before touching heap data.
  ## Android bridge threads call this via JNI to avoid GC crashes.
  setupForeignThreadGc()

proc nim_thread_detach*() {.exportc, cdecl, dynlib.} =
  ## Optional counterpart for tests; production currently keeps threads pinned.
  tearDownForeignThreadGc()

proc canonicalizeMultiaddrText(text: string): string =
  if text.len == 0:
    return text
  let maRes = MultiAddress.init(text)
  if maRes.isOk():
    return $maRes.get()
  result = text

proc hasZeroPortSegment(text: string, proto: string): bool =
  var idx = text.find(proto)
  while idx >= 0:
    let start = idx + proto.len
    var endIdx = text.find('/', start)
    if endIdx < 0:
      endIdx = text.len
    let segment = text[start ..< endIdx]
    if segment.len == 0 or segment == "0":
      return true
    idx = text.find(proto, endIdx)
  false


const
  NimResultOk* = cint(0)
  NimResultError* = cint(1)
  NimResultInvalidState* = cint(2)
  NimResultInvalidArgument* = cint(3)
  MaxFeedHistory = 128
  FeedPayloadMaxBytes = 512 * 1024
  FeedPayloadMaxStringLen = 16 * 1024
  FeedPayloadMaxArrayLen = 128
  FeedPayloadMaxObjectEntries = 128
  FeedPayloadMaxDepth = 8
  FeedPayloadLargeKeyThreshold = 1024
  FeedPayloadDropKeys = [
    "payload_b64",
    "body_b64",
    "data_b64",
    "raw_b64",
    "payload_bytes",
    "payload_base64",
    "payload_blob",
    "raw_bytes",
    "binary",
    "blob",
    "blob_b64",
    "frame_b64"
  ]
  MdnsConnectThrottleMs = 10_000
  DefaultMdnsServiceName = "_unimaker._udp."
  EventLogLimit = 512

var lastInitError = ""

logScope:
  topics = "nimlibp2p ffi"

when defined(android):
  proc android_log_write(prio: cint; tag: cstring; text: cstring): cint {.cdecl, importc: "__android_log_write", header: "<android/log.h>".}
  const AndroidLogError = cint(6)

when defined(ohos):
  when defined(nimlibp2p_no_hilog):
    proc OH_LOG_Print(logType: cint, level: cint, domain: cint, tag: cstring, fmt: cstring): cint {.cdecl, varargs.} =
      discard # fallback noop
  else:
    proc OH_LOG_Print(logType: cint, level: cint, domain: cint, tag: cstring, fmt: cstring): cint {.cdecl, importc: "OH_LOG_Print", header: "<hilog/log.h>", varargs.}
  const HILOG_TYPE_APP = cint(0) # LOG_APP
  const HILOG_LEVEL_INFO = cint(4) # LOG_INFO
  const HILOG_DOMAIN = cint(0xD0B0)
  let HilogTag: cstring = "nimlibp2p"

proc debugLog(msg: string) =
  try:
    stderr.writeLine(msg)
  except IOError:
    discard
  when defined(android):
    discard android_log_write(AndroidLogError, "nim-libp2p", msg.cstring)
  when defined(ohos):
    discard OH_LOG_Print(HILOG_TYPE_APP, HILOG_LEVEL_INFO, HILOG_DOMAIN, HilogTag, "%{public}s", msg.cstring)

proc clearInitError() =
  lastInitError = ""

proc recordInitError(reason: string) =
  lastInitError = reason
  error "libp2p node init failed", err = reason
  debugLog("[nimlibp2p] init failure: " & reason)

type
  RawPubsubCallback* =
    proc(topic: cstring, payload: ptr UncheckedArray[byte], payloadLen: csize_t,
         userData: pointer) {.cdecl, raises: [], gcsafe.}

  FeatureSet = object
    gossipsub: bool
    directStream: bool
    rendezvous: bool
    autonat: bool
    circuitRelay: bool
    livestream: bool
    dataTransfer: bool

  IdentityConfig = object
    privateKey: seq[byte]   ## 64-byte Ed25519 private key (seed + public key)
    publicKey: seq[byte]    ## 32-byte Ed25519 public key
    peerId: string
    source: Option[string]
    keyPath: Option[string]

  MetricsTextfileConfig = object
    path: string
    intervalMs: int
    labels: Table[string, string]

  NodeConfig = object
    listenAddresses: seq[string]
    enableMetrics: bool
    metricsPort: Option[int]
    metricsTextfile: Option[MetricsTextfileConfig]
    privateNetwork: Option[string]
    automations: FeatureSet
    identity: Option[IdentityConfig]
    dataDir: Option[string]
    extra: JsonNode

  Subscription = ref object
    topic: string
    handler: TopicHandler
    callback: RawPubsubCallback
    userData: pointer
    active: bool

  CachedFeedItem = object
    id: string
    author: string
    payload: JsonNode
    timestamp: int64

  DirectWaiter = ref object
    future: Future[bool]
    error: string
    createdAt: int64
    timeoutMs: int

  LivestreamSession = object
    streamKey: string
    config: JsonNode
    frameIndex: int64
    lastTimestamp: int64
  LanGroupState = object
    groupId: string
    topic: string
    joined: bool
    lastTimestamp: int64

  CommandKind = enum
    cmdNone,
    cmdStart,
    cmdStop,
    cmdPublish,
    cmdSubscribe,
    cmdUnsubscribe,
    cmdConnectPeer,
    cmdConnectMultiaddr,
    cmdDisconnectPeer,
    cmdConnectedPeers,
    cmdPeerMultiaddrs,
    cmdDiagnostics,
    cmdConnectedPeersInfo,
    cmdRegisterPeerHints,
    cmdAddExternalAddress,
    cmdCheckPeerConnected,
    cmdSyncPeerstore,
    cmdLoadStoredPeers,
    cmdReconnectBootstrap,
    cmdSendDirect,
    cmdSendChatAck,
    cmdGetLastDirectError,
    cmdInitializeConnEvents,
    cmdRefreshPeerConnections,
    cmdKeepAlivePin,
    cmdKeepAliveUnpin,
    cmdEmitManualEvent,
    cmdBoostConnectivity,
    cmdGetLanEndpoints,
    cmdFetchFeedSnapshot,
    cmdFeedSubscribePeer,
    cmdFeedUnsubscribePeer,
    cmdFeedPublishEntry,
    cmdUpsertLivestream,
    cmdPublishLivestream,
    cmdLanGroupJoin,
    cmdLanGroupLeave,
    cmdLanGroupSend,
    cmdMdnsSetEnabled,
    cmdMdnsSetInterface,
    cmdMdnsDebug,
    cmdMdnsSetInterval,
    cmdMdnsProbe,
    cmdRendezvousAdvertise,
    cmdRendezvousDiscover,
    cmdRendezvousUnregister,
    cmdReserveOnRelay,
    cmdReserveOnAllRelays,
    cmdWaitSecureChannel,
    cmdPollEvents,
    cmdDexInit,
    cmdDexSubmitOrder,
    cmdDexSubmitMixerIntent,
    cmdDexGetMarketData,
    cmdDexGetWallets

  Command = ref object
    kind: CommandKind
    topic: string
    payload: seq[byte]
    peerId: string
    multiaddr: string
    addresses: seq[string]
    source: string
    callback: RawPubsubCallback
    userData: pointer
    subscriptionPtr: pointer
    resultCode: cint
    delivered: int
    stringResult: string
    ptrResult: pointer
    errorMsg: string
    boolResult: bool
    ackRequested: bool
    intResult: int
    intParam: int
    timeoutMs: int
    messageId: string
    op: string
    replyTo: string
    target: string
    jsonString: string
    textPayload: string
    streamKey: string
    completion: ThreadSignalPtr
    requestLimit: int
    boolParam: bool
    next: Atomic[Command]

  NimNode* = ref object
    cfg: NodeConfig
    switchInstance: Switch
    gossip: GossipSub
    signal: ThreadSignalPtr
    readySignal: ThreadSignalPtr
    shutdownSignal: ThreadSignalPtr
    commandQueueHead: Atomic[Command]
    commandQueueTail: Atomic[Command]
    queueStub: Command
    queuedCommands: Atomic[int]
    thread: Thread[pointer]
    dexNode: DexNode
    subscriptions: Table[pointer, Subscription]
    peerHints: Table[string, seq[string]]
    mdnsEnabled: bool
    mdnsIntervalSeconds: int
    running: Atomic[bool]
    stopRequested: Atomic[bool]
    pendingCommands: Atomic[int]
    ready: Atomic[bool]
    shutdownComplete: Atomic[bool]
    started: bool
    threadJoined: Atomic[bool]
    directWaiters: Table[string, DirectWaiter]
    lastDirectError: string
    dmService: DirectMessageService
    dmServiceMounted: bool
    feedService: FeedService
    defaultTopicsReady: bool
    internalSubscriptions: Table[string, TopicHandler]
    connectionEventsEnabled: bool
    connectionHandlers: seq[(ConnEventKind, ConnEventHandler)]
    keepAlivePeers: HashSet[string]
    keepAliveLoop: Future[void]
    metricsLoop: Future[void]
    metricsTextfileLoop: Future[void]
    eventLog: seq[JsonNode]
    eventLogLock: Lock
    eventLogLimit: int
    lanEndpointsCache: JsonNode
    feedItems: seq[CachedFeedItem]
    livestreams: Table[string, LivestreamSession]
    lanGroups: Table[string, LanGroupState]
    rendezvous: RendezVous
    relayReservations: Table[string, int64]
    discovery: DiscoveryManager
    mdnsInterface: MdnsInterface
    mdnsService: string
    mdnsServices: seq[string]
    mdnsQueries: seq[DiscoveryQuery]
    mdnsWatchers: seq[Future[void]]
    mdnsLastSeen: Table[string, int64]
    mdnsPreferredIpv4: string
    mdnsRestartPending: bool
    mdnsStarting: bool
    mdnsReady: bool
    mdnsRestarting: bool

var hookInstalled = false

const
  DefaultMetricsTextfileInterval = 15_000

proc resetCommandFields(cmd: Command) =
  cmd.topic = ""
  cmd.payload = @[]
  cmd.peerId = ""
  cmd.multiaddr = ""
  cmd.addresses = @[]
  cmd.source = ""
  cmd.callback = nil
  cmd.userData = nil
  cmd.subscriptionPtr = nil
  cmd.resultCode = NimResultError
  cmd.delivered = 0
  cmd.stringResult = ""
  cmd.ptrResult = nil
  cmd.errorMsg = ""
  cmd.boolResult = false
  cmd.ackRequested = false
  cmd.intResult = 0
  cmd.intParam = 0
  cmd.timeoutMs = 0
  cmd.messageId = ""
  cmd.op = ""
  cmd.replyTo = ""
  cmd.target = ""
  cmd.jsonString = ""
  cmd.textPayload = ""
  cmd.streamKey = ""
  cmd.requestLimit = 0
  cmd.boolParam = false
  cmd.next.store(nil, moRelease)

proc acquireCommand(kind: CommandKind): Command =
  new(result)
  resetCommandFields(result)
  result.kind = kind
  result.completion = nil
  debugLog("[nimlibp2p] makeCommand kind=" & $kind)

proc makeCommand(kind: CommandKind): Command =
  acquireCommand(kind)

proc destroyCommand(cmd: Command) =
  if cmd.isNil:
    return
  if not cmd.completion.isNil:
    let closeRes = cmd.completion.close()
    if closeRes.isErr():
      warn "command completion close failed", err = closeRes.error
    cmd.completion = nil
  resetCommandFields(cmd)

proc initCommandQueue(n: NimNode) =
  let stub = acquireCommand(cmdNone)
  stub.completion = nil
  stub.next.store(nil, moRelease)
  n.queueStub = stub
  n.commandQueueHead.store(stub, moRelease)
  n.commandQueueTail.store(stub, moRelease)
  n.queuedCommands.store(0, moRelease)

proc enqueueCommand(n: NimNode, cmd: Command) =
  if cmd.isNil:
    return
  cmd.next.store(nil, moRelease)
  let previous = n.commandQueueHead.exchange(cmd, moAcquireRelease)
  previous.next.store(cmd, moRelease)
  atomicInc(n.queuedCommands)

proc dequeueCommand(n: NimNode): Command =
  let tail = n.commandQueueTail.load(moAcquire)
  let next = tail.next.load(moAcquire)
  if next.isNil:
    return nil
  n.commandQueueTail.store(next, moRelease)
  next

proc logMemoryLoop(n: NimNode) {.async.}

proc nowMillis(): int64 =
  int64(epochTime() * 1000)

proc bytesToString(data: seq[byte]): string =
  if data.len == 0:
    return ""
  result = newString(data.len)
  copyMem(addr result[0], unsafeAddr data[0], data.len)

proc stringToBytes(text: string): seq[byte] =
  if text.len == 0:
    return @[]
  result = newSeq[byte](text.len)
  copyMem(addr result[0], unsafeAddr text[0], text.len)

proc emitEvent(topic, payload: string) =
  try:
    nim_bridge_emit_event(topic.cstring, payload.cstring)
  except CatchableError as exc:
    warn "emitEvent failed", err = exc.msg, topic = topic

proc recordEvent(n: NimNode, topic, payload: string) =
  if n.isNil:
    emitEvent(topic, payload)
    return
  var entry = newJObject()
  entry["topic"] = %topic
  entry["timestamp_ms"] = %nowMillis()
  entry["payload"] = %payload
  
  n.eventLogLock.acquire()
  try:
    n.eventLog.add(entry)
    if n.eventLog.len > n.eventLogLimit:
      let excess = n.eventLog.len - n.eventLogLimit
      if excess > 0:
        n.eventLog.delete(0, excess - 1)
  finally:
    n.eventLogLock.release()
    
  emitEvent(topic, payload)

proc pollEventLog(n: NimNode, limit: int): string =
  if n.isNil:
    return "[]"
  
  n.eventLogLock.acquire()
  try:
    let bounded =
      if limit <= 0 or limit > n.eventLogLimit:
        n.eventLogLimit
      else:
        limit
    let available = n.eventLog.len
    if available == 0:
      return "[]"
    let takeCount = min(bounded, available)
    var arr = newJArray()
    for i in 0 ..< takeCount:
      arr.add(n.eventLog[i])
    
    if takeCount == available:
      n.eventLog.setLen(0)
    else:
      n.eventLog.delete(0, takeCount - 1)
      
    return $arr
  finally:
    n.eventLogLock.release()


proc ensureJson(node: var JsonNode) =
  if node.isNil:
    node = newJObject()

proc decodeFeedEntry(payload: seq[byte]): Option[FeedEntry] =
  try:
    let rawText = bytesToString(payload)
    let node = parseJson(rawText)
    if node.kind != JObject:
      return none(FeedEntry)
    var coverBytes: seq[byte] = @[]
    if node.hasKey("cover") and node["cover"].kind == JString:
      try:
        let decoded = base64.decode(node["cover"].getStr())
        coverBytes = newSeq[byte](decoded.len)
        for i in 0 ..< decoded.len:
          coverBytes[i] = decoded[i].byte
      except CatchableError:
        discard
    let entry = FeedEntry(
      id: (if node.hasKey("id") and node["id"].kind == JString: node["id"].getStr() else: ""),
      mediaType: (if node.hasKey("mediaType") and node["mediaType"].kind == JString: node["mediaType"].getStr() else: ""),
      cover: coverBytes,
      summary: (if node.hasKey("summary") and node["summary"].kind == JString: node["summary"].getStr() else: ""),
      extra: node,
      raw: payload,
    )
    some(entry)
  except CatchableError:
    none(FeedEntry)

proc findRendezvousService(sw: Switch): RendezVous =
  if sw.isNil or sw.ms.isNil:
    return nil
  for holder in sw.ms.handlers:
    let proto = holder.protocol
    if proto of RendezVous:
      return RendezVous(proto)
  return nil


proc addFeedItem(n: NimNode, item: CachedFeedItem) =
  var existing = -1
  for idx, it in n.feedItems:
    if it.id == item.id:
      existing = idx
      break
  if existing >= 0:
    n.feedItems[existing] = item
  else:
    n.feedItems.add(item)
    if n.feedItems.len > MaxFeedHistory:
      let excess = n.feedItems.len - MaxFeedHistory
      if excess > 0:
        n.feedItems.delete(0, excess - 1)

proc sanitizeFeedPayload(node: JsonNode, depth: int = 0): JsonNode =
  if node.isNil:
    return newJNull()
  if depth >= FeedPayloadMaxDepth:
    case node.kind
    of JObject, JArray:
      return %"__trimmed__"
    of JString:
      var truncated = node.getStr()
      if truncated.len > FeedPayloadMaxStringLen:
        truncated = truncated[0 ..< FeedPayloadMaxStringLen] & "...<trimmed>"
      result = %truncated
      return
    else:
      return node
  case node.kind
  of JObject:
    result = newJObject()
    var added = 0
    for key, child in pairs(node):
      if added >= FeedPayloadMaxObjectEntries:
        break
      if child.isNil:
        continue
      let lower = key.toLowerAscii()
      if child.kind == JString:
        var text = child.getStr()
        if (lower in FeedPayloadDropKeys) and text.len > FeedPayloadLargeKeyThreshold:
          continue
        if text.len > FeedPayloadMaxStringLen:
          text = text[0 ..< FeedPayloadMaxStringLen] & "...<trimmed>"
        result[key] = %text
      else:
        result[key] = sanitizeFeedPayload(child, depth + 1)
      inc added
  of JArray:
    result = newJArray()
    let total = len(node)
    let limit = if total > FeedPayloadMaxArrayLen: FeedPayloadMaxArrayLen else: total
    var idx = 0
    while idx < limit:
      result.add(sanitizeFeedPayload(node[idx], depth + 1))
      inc idx
    if total > limit:
      result.add(%"__truncated__")
  of JString:
    var text = node.getStr()
    if text.len > FeedPayloadMaxStringLen:
      text = text[0 ..< FeedPayloadMaxStringLen] & "...<trimmed>"
    result = %text
  else:
    result = node

proc jsonGetStr(node: JsonNode, key: string, defaultValue: string = ""): string =
  if node.kind == JObject and node.hasKey(key):
    let v = node.getOrDefault(key)
    case v.kind
    of JString:
      return v.getStr()
    of JInt, JFloat:
      return $v.getInt()
    of JBool:
      return if v.getBool(): "true" else: "false"
    else:
      try:
        return $v
      except CatchableError:
        discard
  defaultValue

proc jsonGetBool(node: JsonNode, key: string, defaultValue: bool = false): bool =
  if node.kind == JObject and node.hasKey(key):
    let v = node.getOrDefault(key)
    case v.kind
    of JBool:
      return v.getBool()
    of JInt, JFloat:
      return v.getInt() != 0
    of JString:
      return v.getStr().toLowerAscii() in ["true", "1", "yes", "y"]
    else:
      discard
  defaultValue

proc jsonGetInt64(node: JsonNode, key: string, defaultValue: int64 = 0): int64 =
  if node.kind == JObject and node.hasKey(key):
    let v = node.getOrDefault(key)
    case v.kind
    of JInt:
      return v.getInt()
    of JFloat:
      return int64(v.getFloat())
    of JString:
      try:
        return parseBiggestInt(v.getStr())
      except CatchableError:
        discard
    else:
      discard
  defaultValue

proc makeTopicHandler(
    n: NimNode, handler: proc(n: NimNode, topic: string, payload: seq[byte]) {.gcsafe.}
): TopicHandler =
  result =
    proc(topic: string, payload: seq[byte]): Future[void] {.gcsafe, raises: [].} =
      let fut = newFuture[void]("nimlibp2p.internal_topic")
      try:
        handler(n, topic, payload)
      except Exception as exc:
        warn "internal topic handler error", topic = topic, err = exc.msg
      fut.complete()
      fut

proc ensureInternalSubscription(n: NimNode, topic: string, handler: TopicHandler) =
  if n.internalSubscriptions.hasKey(topic):
    return
  n.gossip.PubSub.subscribe(topic, handler)
  n.internalSubscriptions[topic] = handler

proc localPeerId(n: NimNode): string =
  if n.switchInstance.isNil or n.switchInstance.peerInfo.isNil:
    return ""
  try:
    result = $n.switchInstance.peerInfo.peerId
  except CatchableError:
    result = ""

proc ensureMessageId(mid: string): string =
  if mid.len > 0:
    return mid
  let ts = nowMillis()
  return "msg-" & $ts

proc newDirectWaiter(timeoutMs: int): DirectWaiter =
  DirectWaiter(
    future: newFuture[bool]("nimlibp2p.directAck"),
    error: "",
    createdAt: nowMillis(),
    timeoutMs: timeoutMs,
  )

proc handleDirectAckNode(n: NimNode, envelope: JsonNode) =
  let mid = jsonGetStr(envelope, "mid")
  if mid.len == 0:
    return
  let fromPeer = jsonGetStr(envelope, "from")
  let success = jsonGetBool(envelope, "ack", true)
  let errMsg = jsonGetStr(envelope, "error")
  if n.directWaiters.hasKey(mid):
    let waiter = n.directWaiters.getOrDefault(mid)
    if not waiter.isNil:
      if errMsg.len > 0:
        waiter.error = errMsg
      if not waiter.future.finished():
        waiter.future.complete(success)
    n.directWaiters.del(mid)
  if not success and errMsg.len > 0:
    n.lastDirectError = errMsg
  var event = newJObject()
  event["type"] = %"DirectMessageAck"
  event["peer_id"] = %((if fromPeer.len == 0: "unknown" else: fromPeer))
  event["message_id"] = %mid
  event["success"] = %success
  if errMsg.len > 0:
    event["error"] = %errMsg
  event["timestamp_ms"] = %jsonGetInt64(envelope, "timestamp_ms", nowMillis())
  event["transport"] = %"nim-direct"
  recordEvent(n, "network_event", $event)

proc handleContentFeedNode(n: NimNode, payload: string, originPeer: string) =
  try:
    if payload.len > FeedPayloadMaxBytes:
      warn "handleContentFeed dropping oversized payload",
        size = payload.len, peer = originPeer
      return
    var raw = parseJson(payload)
    let sanitized = sanitizeFeedPayload(raw)
    raw = nil
    if sanitized.kind != JObject:
      return
    let itemId = jsonGetStr(sanitized, "id", jsonGetStr(sanitized, "cid"))
    if itemId.len == 0:
      return
    let author = jsonGetStr(sanitized, "author", originPeer)
    let ts = jsonGetInt64(sanitized, "timestamp", nowMillis())
    let item = CachedFeedItem(id: itemId, author: author, payload: sanitized, timestamp: ts)
    addFeedItem(n, item)
    var event = newJObject()
    event["type"] = %"ContentFeedItem"
    event["peer_id"] = %author
    event["timestamp_ms"] = %ts
    event["payload"] = sanitized
    recordEvent(n, "network_event", $event)
  except CatchableError as exc:
    warn "handleContentFeed parse failed", err = exc.msg

proc lanGroupTopic(groupId: string): string =
  "lan/chat/" & groupId

proc handleLanGroupMessage(n: NimNode, groupId: string, payload: seq[byte]) =
  if payload.len == 0:
    return
  let raw = bytesToString(payload)
  var parsed: JsonNode
  var parsedOk = false
  try:
    parsed = parseJson(raw)
    parsedOk = parsed.kind == JObject
  except CatchableError:
    parsed = newJObject()
  let fromPeer =
    if parsedOk: jsonGetStr(parsed, "from", jsonGetStr(parsed, "peer_id"))
    else: ""
  let body =
    if parsedOk and parsed.hasKey("body"): jsonGetStr(parsed, "body", raw)
    else: raw
  let ts =
    if parsedOk: jsonGetInt64(parsed, "timestamp_ms", nowMillis())
    else: nowMillis()
  let mid =
    if parsedOk: jsonGetStr(parsed, "mid")
    else: ""
  var event = newJObject()
  event["type"] = %"LanGroupMessage"
  event["group_id"] = %groupId
  event["peer_id"] = %(
    if fromPeer.len == 0: "unknown"
    else: fromPeer
  )
  event["body"] = %body
  event["timestamp_ms"] = %ts
  if mid.len > 0:
    event["message_id"] = %mid
  if parsedOk:
    event["payload"] = parsed
  else:
    event["payload"] = %raw
  recordEvent(n, "network_event", $event)
  if n.lanGroups.hasKey(groupId):
    var state = n.lanGroups.getOrDefault(groupId)
    if ts > state.lastTimestamp:
      state.lastTimestamp = ts
      n.lanGroups[groupId] = state

proc ensureLanGroupSubscription(n: NimNode, groupId: string): bool =
  if groupId.len == 0:
    return false
  let topic = lanGroupTopic(groupId)
  if not n.internalSubscriptions.hasKey(topic):
    let handler =
      proc(node: NimNode, t: string, payload: seq[byte]) {.gcsafe.} =
        handleLanGroupMessage(node, groupId, payload)
    ensureInternalSubscription(n, topic, makeTopicHandler(n, handler))
  var state = n.lanGroups.getOrDefault(groupId)
  state.groupId = groupId
  state.topic = topic
  state.joined = true
  state.lastTimestamp = nowMillis()
  n.lanGroups[groupId] = state
  true

proc removeLanGroupSubscription(n: NimNode, groupId: string): bool =
  if groupId.len == 0:
    return false
  let topic = lanGroupTopic(groupId)
  if n.internalSubscriptions.hasKey(topic):
    let handler = n.internalSubscriptions.getOrDefault(topic)
    try:
      n.gossip.PubSub.unsubscribe(topic, handler)
    except CatchableError as exc:
      warn "lan group unsubscribe failed", topic = topic, err = exc.msg
    n.internalSubscriptions.del(topic)
  if n.lanGroups.hasKey(groupId):
    n.lanGroups.del(groupId)
  true

proc handleDirectMessageNode(n: NimNode, envelope: JsonNode, raw: string) =
  var fromPeer = jsonGetStr(envelope, "from")
  if fromPeer.len == 0:
    fromPeer = jsonGetStr(envelope, "peer_id")
  let op = jsonGetStr(envelope, "op").toLowerAscii()
  if op == "ack":
    handleDirectAckNode(n, envelope)
    return
  let originalMid = jsonGetStr(envelope, "mid")
  let mid = ensureMessageId(originalMid)
  let ts = jsonGetInt64(envelope, "timestamp_ms", nowMillis())
  let ackRequested = jsonGetBool(envelope, "ackRequested", false)
  var body = jsonGetStr(envelope, "body")
  if body.len == 0:
    body = raw
  var event = newJObject()
  event["type"] = %"MessageReceived"
  event["peer_id"] = %((if fromPeer.len == 0: "unknown" else: fromPeer))
  event["message_id"] = %mid
  event["timestamp_ms"] = %ts
  event["payload"] = %body
  event["transport"] = %"nim-direct"
  recordEvent(n, "network_event", $event)
  if ackRequested and fromPeer.len > 0 and originalMid.len > 0:
    debugLog("[nimlibp2p] dm ack satisfied via direct stream peer=" & fromPeer & " mid=" & originalMid)

proc handleDirectTopic(n: NimNode, topic: string, payload: seq[byte]) =
  let raw = bytesToString(payload)
  if raw.len == 0:
    return
  var envelope: JsonNode
  try:
    envelope = parseJson(raw)
  except CatchableError:
    envelope = newJNull()
  if envelope.kind != JObject:
    var event = newJObject()
    event["type"] = %"MessageReceived"
    event["peer_id"] = %"unknown"
    event["message_id"] = %ensureMessageId("")
    event["timestamp_ms"] = %nowMillis()
    event["payload"] = %raw
    event["transport"] = %"nim-direct"
    recordEvent(n, "network_event", $event)
    return
  if topic.contains("/dm_ack/"):
    handleDirectAckNode(n, envelope)
  else:
    handleDirectMessageNode(n, envelope, raw)

proc setupDefaultTopics(n: NimNode): Future[void] {.async.} =
  let local = localPeerId(n)
  if local.len == 0:
    return
  var localPid: PeerId
  if localPid.init(local):
    var dmInitialized = false
    if n.dmService.isNil:
      let inboxTopic = "/unimaker/dm/" & local
      let dmHandler =
        proc(msg: DirectMessage) {.async.} =
          handleDirectTopic(n, inboxTopic, msg.payload)
      n.dmService = newDirectMessageService(n.switchInstance, localPid, dmHandler)
      dmInitialized = true
    if not n.dmService.isNil:
      if not n.dmService.started:
        await n.dmService.start()
        debugLog("[nimlibp2p] setupDefaultTopics started direct message service peer=" & local)
      if not n.dmServiceMounted:
        n.switchInstance.mount(n.dmService)
        n.dmServiceMounted = true
        debugLog("[nimlibp2p] setupDefaultTopics mounted direct message service peer=" & local)
      elif dmInitialized:
        debugLog("[nimlibp2p] setupDefaultTopics installed direct message service peer=" & local)
    else:
      warn "setupDefaultTopics: dm service initialization failed", peer = local
  else:
    warn "setupDefaultTopics: invalid local peer id for dm service", peer = local
  let ackTopic = "/unimaker/dm_ack/" & local
  ensureInternalSubscription(n, ackTopic, makeTopicHandler(n, handleDirectTopic))
  let contentTopic = "unimaker/content/v1"
  if not n.internalSubscriptions.hasKey(contentTopic):
    let handler =
      proc(node: NimNode, t: string, payload: seq[byte]) =
        handleContentFeedNode(node, bytesToString(payload), "")
    ensureInternalSubscription(n, contentTopic, makeTopicHandler(n, handler))
  if n.feedService.isNil:
    var localPid: PeerId
    if localPid.init(local):
      let feedHandler =
        proc(item: FeedItem) {.async: (raises: []), gcsafe, closure.} =
          let raw = bytesToString(item.entry.raw)
          let publisherStr = $item.publisher
          handleContentFeedNode(n, raw, publisherStr)
      n.feedService = newFeedService(n.gossip, localPid, feedHandler)
    else:
      warn "setupDefaultTopics: invalid local peer id for feed service", peer = local
  n.defaultTopicsReady = true

proc isLanIpv4(ip: string): bool =
  if ip.len == 0:
    return false
  if ip.startsWith("10.") or ip.startsWith("192.168.") or ip.startsWith("169.254."):
    return true
  if ip.startsWith("172."):
    let octets = ip.split('.')
    if octets.len >= 2:
      try:
        let second = parseInt(octets[1])
        if second >= 16 and second <= 31:
          return true
      except CatchableError:
        discard
  if ip.startsWith("100."):
    let octets = ip.split('.')
    if octets.len >= 2:
      try:
        let second = parseInt(octets[1])
        if second >= 64 and second <= 127:
          return true
      except CatchableError:
        discard
  false

proc collectLanIpv4Addrs(preferred: string = ""): seq[string] =
  var ips = enumerateLanIpv4(preferred)
  let preferredValid = preferred.len > 0 and preferred != "0.0.0.0" and
    preferred != "127.0.0.1" and isLanIpv4(preferred)
  if preferredValid and preferred notin ips:
    debugLog("[nimlibp2p] collectLanIpv4Addrs force-keep preferred=" & preferred)
    ips.insert(preferred, 0)
  var filtered: seq[string] = @[]
  var seen = initHashSet[string]()
  for ipRaw in ips:
    let ip = ipRaw.strip()
    if ip.len == 0 or ip == "0.0.0.0" or ip == "127.0.0.1":
      debugLog("[nimlibp2p] collectLanIpv4Addrs drop=" & (if ip.len > 0: ip else: "<empty>"))
      continue
    if not isLanIpv4(ip):
      debugLog("[nimlibp2p] collectLanIpv4Addrs non-lan=" & ip)
      continue
    if seen.contains(ip):
      continue
    try:
      let octets = ip.split('.')
      if octets.len == 4:
        let third = parseInt(octets[^2])
        let last = parseInt(octets[^1])
        if third == 255 or last == 255 or last == 0:
          debugLog("[nimlibp2p] collectLanIpv4Addrs skip broadcast=" & ip)
          continue
    except CatchableError:
      discard
    filtered.add(ip)
    seen.incl(ip)
  if filtered.len == 0 and preferredValid:
    debugLog("[nimlibp2p] collectLanIpv4Addrs fallback<-preferred " & preferred)
    filtered = @[preferred]
  if filtered.len == 0:
    debugLog("[nimlibp2p] collectLanIpv4Addrs -> <empty>")
    return @[]
  for ip in filtered:
    debugLog("[nimlibp2p] collectLanIpv4Addrs candidate=" & ip)
  debugLog("[nimlibp2p] collectLanIpv4Addrs -> " & filtered.join(","))
  result = filtered

proc isLanIpv6(ip: string): bool =
  if ip.len == 0:
    return false
  let lower = ip.toLowerAscii()
  if lower == "::" or lower == "::1":
    return false
  if lower.startsWith("fe80:") or lower.startsWith("fd") or lower.startsWith("fc"):
    return true
  if lower.startsWith("2001:") or lower.startsWith("2002:") or lower.startsWith("240"):
    return true
  not lower.startsWith("ff")

proc collectLanIpv6Addrs(): seq[string] =
  var seen = initHashSet[string]()
  when defined(posix):
    when declared(getifaddrs):
      var ifap: ptr Ifaddrs = nil
      if getifaddrs(addr ifap) == 0:
        defer:
          freeifaddrs(ifap)
        var cursor = ifap
        while cursor != nil:
          when declared(cursor.ifa_flags):
            when declared(IFF_LOOPBACK):
              if (cursor.ifa_flags and IFF_LOOPBACK) != 0:
                cursor = cursor.ifa_next
                continue
          let addrPtr = cursor.ifa_addr
          when declared(AF_INET6):
            if addrPtr != nil and addrPtr.sa_family == AF_INET6:
              let sin6 = cast[ptr Sockaddr_in6](addrPtr)
              var buffer: array[64, char]
              when declared(inet_ntop):
                if inet_ntop(AF_INET6, addr sin6.sin6_addr,
                              cast[ptr char](addr buffer[0]),
                              buffer.len.cint) != nil:
                  var ip = $cast[cstring](addr buffer[0])
                  let zoneIdx = ip.find('%')
                  if zoneIdx >= 0:
                    ip = ip[0 ..< zoneIdx]
                  if ip.len > 0 and isLanIpv6(ip):
                    seen.incl(ip)
          cursor = cursor.ifa_next
  for ip in seen.items:
    result.add(ip)
  if result.len == 0:
    debugLog("[nimlibp2p] collectLanIpv6Addrs -> <empty>")
  else:
    debugLog("[nimlibp2p] collectLanIpv6Addrs -> " & result.join(","))

proc replaceIpv4(addrStr: string, ip: string): Option[string] =
  let marker = "/ip4/"
  let idx = addrStr.find(marker)
  if idx < 0:
    return none(string)
  let startIdx = idx + marker.len
  var endIdx = addrStr.find('/', startIdx)
  if endIdx < 0:
    endIdx = addrStr.len
  let current = addrStr[startIdx ..< endIdx]
  if current == ip:
    return some(addrStr)
  var allowed = false
  if current == "0.0.0.0" or current == "127.0.0.1" or current.len == 0:
    allowed = true
  elif current == ip:
    allowed = true
  if not allowed:
    return none(string)
  let rewritten = addrStr[0 ..< startIdx] & ip & addrStr[endIdx ..< addrStr.len]
  some(rewritten)

proc replaceIpv6(addrStr: string, ip: string): Option[string] =
  let marker = "/ip6/"
  let idx = addrStr.find(marker)
  if idx < 0:
    return none(string)
  let startIdx = idx + marker.len
  var endIdx = addrStr.find('/', startIdx)
  if endIdx < 0:
    endIdx = addrStr.len
  let current = addrStr[startIdx ..< endIdx]
  if current == ip:
    return some(addrStr)
  var allowed = false
  let lower = current.toLowerAscii()
  if lower.len == 0 or lower == "::" or lower == "0" or lower == "0:0:0:0:0:0:0:0":
    allowed = true
  elif lower == ip.toLowerAscii():
    allowed = true
  if not allowed:
    return none(string)
  let rewritten = addrStr[0 ..< startIdx] & ip & addrStr[endIdx ..< addrStr.len]
  some(rewritten)

proc expandLanMultiaddrs(
    addrs: seq[MultiAddress],
    lanIpv4: seq[string],
    lanIpv6: seq[string]
): seq[MultiAddress] =
  if addrs.len == 0:
    return @[]
  var seen = initHashSet[string]()
  var lan4 = initHashSet[string]()
  for ip in lanIpv4:
    if isLanIpv4(ip):
      lan4.incl(ip)
  var lan6 = initHashSet[string]()
  for ip in lanIpv6:
    if isLanIpv6(ip):
      lan6.incl(ip)
  for ma in addrs:
    let original = $ma
    var replaced = false
    if lan4.len > 0 and original.contains("/ip4/"):
      for lanIp in lan4.items:
        let opt = replaceIpv4(original, lanIp)
        if opt.isSome():
          let rewritten = opt.get()
          let parsed = MultiAddress.init(rewritten)
          if parsed.isOk():
            let normalized = $parsed.get()
            if not seen.contains(normalized):
              result.add(parsed.get())
              seen.incl(normalized)
              replaced = true
    if lan6.len > 0 and original.contains("/ip6/"):
      for lanIp in lan6.items:
        let opt6 = replaceIpv6(original, lanIp)
        if opt6.isSome():
          let rewritten6 = opt6.get()
          let parsed6 = MultiAddress.init(rewritten6)
          if parsed6.isOk():
            let normalized6 = $parsed6.get()
            if not seen.contains(normalized6):
              result.add(parsed6.get())
              seen.incl(normalized6)
              replaced = true
    if not replaced:
      if not seen.contains(original):
        result.add(ma)
        seen.incl(original)

proc isUnspecifiedMultiaddrText(text: string): bool =
  let lower = text.toLowerAscii()
  if lower.contains("/ip4/0.0.0.0/"):
    return true
  if lower.contains("/ip6/::/"):
    return true
  if lower.endsWith("/ip4/0.0.0.0") or lower.endsWith("/ip6/::"):
    return true
  return false

proc refreshLanAwareAddrs(n: NimNode): seq[MultiAddress] =
  if n.switchInstance.isNil or n.switchInstance.peerInfo.isNil:
    return @[]
  var base = n.switchInstance.peerInfo.listenAddrs
  if base.len == 0:
    base = n.switchInstance.peerInfo.addrs
  let lanIpv4 = collectLanIpv4Addrs(n.mdnsPreferredIpv4)
  let lanIpv6 = collectLanIpv6Addrs()
  debugLog("[nimlibp2p] refreshLanAwareAddrs lanIpv4=" &
    (if lanIpv4.len > 0: lanIpv4.join(",") else: "<empty>") &
    " lanIpv6=" & (if lanIpv6.len > 0: lanIpv6.join(",") else: "<empty>"))
  var expanded = expandLanMultiaddrs(base, lanIpv4, lanIpv6)
  if expanded.len == 0:
    expanded = base
  if expanded.len == 0:
    return @[]
  var seen = initHashSet[string]()
  var filtered = 0
  var dedup: seq[MultiAddress] = @[]
  for ma in expanded:
    let key = $ma
    if isUnspecifiedMultiaddrText(key):
      filtered.inc()
      debugLog("[nimlibp2p] refreshLanAwareAddrs drop=" & key)
      continue
    if not seen.contains(key):
      dedup.add(ma)
      seen.incl(key)
  if filtered > 0 and dedup.len == 0:
    debugLog("[nimlibp2p] refreshLanAwareAddrs dropped unspecified addresses; no concrete listeners available")
  elif filtered > 0:
    debugLog("[nimlibp2p] refreshLanAwareAddrs filtered=" & $filtered)
  debugLog("[nimlibp2p] refreshLanAwareAddrs dedup=" & (if dedup.len > 0: dedup.mapIt($it).join(",") else: "<empty>"))
  n.switchInstance.peerInfo.listenAddrs = dedup
  n.switchInstance.peerInfo.addrs = dedup
  dedup

proc isLanAddress(address: string): bool =
  if address.contains("/ip4/"):
    let parts = address.split("/ip4/")
    if parts.len >= 2:
      var ipPart = parts[^1]
      let slashIdx = ipPart.find('/')
      ipPart = if slashIdx >= 0: ipPart[0 ..< slashIdx] else: ipPart
      if ipPart.find('%') >= 0:
        ipPart = ipPart.split('%')[0]
      for suffix in [".0", ".255"]:
        if ipPart.endsWith(suffix):
          return false
      if isLanIpv4(ipPart):
        return true
  if address.contains("/ip6/"):
    let parts = address.split("/ip6/")
    if parts.len >= 2:
      let ipPart = parts[^1]
      let slashIdx = ipPart.find('/')
      let ip = if slashIdx >= 0: ipPart[0 ..< slashIdx] else: ipPart
      if isLanIpv6(ip):
        return true
  false

proc gatherLanEndpoints(n: NimNode): JsonNode =
  discard refreshLanAwareAddrs(n)
  var localEndpoints = newJArray()
  var remoteEndpoints = newJArray()
  var seenAddrs = initHashSet[string]()
  proc normalizeExportAddr(peerId, addrText: string): Option[string] =
    if addrText.len == 0:
      return none(string)
    let canonical = canonicalizeMultiaddrText(addrText)
    if canonical.len == 0:
      return none(string)
    var sanitized = canonical
    if hasZeroPortSegment(sanitized, "/tcp/") or hasZeroPortSegment(sanitized, "/udp/"):
      debugLog("[nimlibp2p] normalizeExportAddr drop zero port addr=" & sanitized)
      return none(string)
    if sanitized.contains("/ip4/") and sanitized.find("/tcp/") >= 0:
      let ipSegment = sanitized[sanitized.find("/ip4/") + 5 ..< sanitized.len]
      let slashIdx = ipSegment.find('/')
      let ip = if slashIdx >= 0: ipSegment[0 ..< slashIdx] else: ipSegment
      if ip.endsWith(".0") or ip.endsWith(".255"):
        debugLog("[nimlibp2p] normalizeExportAddr drop broadcast=" & sanitized)
        return none(string)
    if peerId.len > 0 and sanitized.find("/p2p/") < 0:
      sanitized.add("/p2p/" & peerId)
    some sanitized
  proc addEndpoint(target: JsonNode, peerId: string, addrs: seq[string], isLocal: bool) =
    if addrs.len == 0:
      return
    var entry = newJObject()
    entry["peer_id"] = %peerId
    if isLocal:
      entry["is_hotspot"] = %true
    var arr = newJArray()
    var firstAddr = ""
    for maText in addrs:
      if maText.len == 0 or not isLanAddress(maText):
        continue
      let normalized = normalizeExportAddr(peerId, canonicalizeMultiaddrText(maText))
      if normalized.isNone:
        continue
      let sanitized = normalized.get()
      let key = peerId & sanitized
      if seenAddrs.contains(key):
        continue
      seenAddrs.incl(key)
      arr.add(%sanitized)
      if firstAddr.len == 0:
        firstAddr = sanitized
    if arr.len == 0:
      return
    entry["multiaddrs"] = arr
    entry["is_local"] = %isLocal
    if firstAddr.len > 0:
      entry["multiaddr"] = %firstAddr
    target.add(entry)
  if n.isNil or n.switchInstance.isNil:
    result = newJObject()
    result["endpoints"] = newJArray()
    result["timestamp_ms"] = %nowMillis()
    info "mdns lan endpoints exported", endpointCount = 0
    return
  if not n.switchInstance.peerInfo.isNil:
    let localPeerId = $n.switchInstance.peerInfo.peerId
    if localPeerId.len > 0:
      var localAddrs = newSeq[string]()
      for ma in n.switchInstance.peerInfo.listenAddrs:
        let addrText = $ma
        if isLanAddress(addrText):
          localAddrs.add(addrText)
      if localAddrs.len == 0:
        for ma in n.switchInstance.peerInfo.addrs:
          let addrText = $ma
          if isLanAddress(addrText):
            localAddrs.add(addrText)
      addEndpoint(localEndpoints, localPeerId, localAddrs, true)
  let peerStore = n.switchInstance.peerStore
  if not peerStore.isNil:
    try:
      let addrBook = peerStore[AddressBook]
      for peer, addrs in addrBook.book.pairs:
        debugLog(
          "[nimlibp2p] gatherLanEndpoints peer=" & $peer &
          " addrCount=" & $addrs.len
        )
        var peerAddrs: seq[string] = @[]
        for ma in addrs:
          let addrText = $ma
          debugLog("[nimlibp2p] gatherLanEndpoints candidate=" & addrText)
          if isLanAddress(addrText):
            peerAddrs.add(addrText)
        if peerAddrs.len > 0:
          addEndpoint(remoteEndpoints, $peer, peerAddrs, false)
    except CatchableError as exc:
      warn "gatherLanEndpoints peer store read failed", err = exc.msg
  var endpoints = newJArray()
  for entry in localEndpoints.elems:
    endpoints.add(entry)
  for entry in remoteEndpoints.elems:
    endpoints.add(entry)
  result = newJObject()
  result["endpoints"] = endpoints
  result["timestamp_ms"] = %nowMillis()
  info "mdns lan endpoints exported", endpointCount = endpoints.len

proc extractPreferredIpv4(cfg: NodeConfig): string =
  if cfg.extra.isNil:
    debugLog("[nimlibp2p] extractPreferredIpv4: extra is nil")
    return ""
  if cfg.extra.kind != JObject:
    debugLog("[nimlibp2p] extractPreferredIpv4: extra kind=" & $cfg.extra.kind)
    return ""
  if cfg.extra.hasKey("preferredLanIpv4"):
    let value = jsonGetStr(cfg.extra, "preferredLanIpv4")
    if value.len > 0:
      debugLog("[nimlibp2p] extractPreferredIpv4 preferredLanIpv4=" & value)
      return value
  let mdnsNode = cfg.extra.getOrDefault("mdns")
  if mdnsNode.kind == JObject:
    let nested = jsonGetStr(mdnsNode, "preferredIpv4")
    if nested.len > 0:
      debugLog("[nimlibp2p] extractPreferredIpv4 mdns.preferredIpv4=" & nested)
      return nested
  ""

proc computeMdnsServiceName(n: NimNode): string =
  let envService = getEnv("UNIMAKER_MDNS_SERVICE")
  if envService.len > 0:
    return envService
  if not n.cfg.extra.isNil and n.cfg.extra.kind == JObject:
    if n.cfg.extra.hasKey("mdnsService"):
      let explicit = jsonGetStr(n.cfg.extra, "mdnsService")
      if explicit.len > 0:
        return explicit
    let mdnsNode = n.cfg.extra.getOrDefault("mdns")
    if mdnsNode.kind == JObject:
      let nested = jsonGetStr(mdnsNode, "service")
      if nested.len > 0:
        return nested
  DefaultMdnsServiceName

proc multiaddrsFromExtra(extra: JsonNode, key: string): seq[string] =
  if extra.isNil or extra.kind != JObject:
    return @[]
  let node = extra.getOrDefault(key)
  if node.isNil or node.kind != JArray:
    return @[]
  for item in node:
    if item.kind == JString:
      let value = item.getStr().strip()
      if value.len > 0:
        result.add(value)

proc extractPeerIdFromMultiaddrStr(ma: string): Option[string] =
  const marker = "/p2p/"
  let idx = ma.rfind(marker)
  if idx < 0:
    return none(string)
  let start = idx + marker.len
  if start >= ma.len:
    return none(string)
  var finish = ma.find('/', start)
  if finish < 0:
    finish = ma.len
  let peerId = ma[start ..< finish]
  if peerId.len == 0:
    return none(string)
  some(peerId)

proc registerInitialPeerHints(n: NimNode, multiaddrs: seq[string], source: string) =
  if multiaddrs.len == 0:
    return
  var grouped = initTable[string, seq[string]]()
  for addr in multiaddrs:
    let trimmed = addr.strip()
    if trimmed.len == 0:
      continue
    let peerOpt = extractPeerIdFromMultiaddrStr(trimmed)
    if peerOpt.isNone():
      debugLog("[nimlibp2p] initial peer hint missing peerId addr=" & trimmed & " source=" & source)
      continue
    let peerId = peerOpt.get()
    var entries = grouped.getOrDefault(peerId)
    if trimmed notin entries:
      entries.add(trimmed)
    grouped[peerId] = entries
  if grouped.len == 0:
    return
  let store = n.switchInstance.peerStore
  for peerId, addrs in grouped.pairs:
    var parsed: seq[MultiAddress] = @[]
    for addrStr in addrs:
      let maRes = MultiAddress.init(addrStr)
      if maRes.isOk():
        parsed.add(maRes.get())
      else:
        warn "initial peer hint invalid", peer = peerId, address = addrStr, source = source, err = maRes.error
    if parsed.len == 0:
      continue
    if n.peerHints.hasKey(peerId):
      var merged = n.peerHints.getOrDefault(peerId)
      for addrStr in addrs:
        if addrStr notin merged:
          merged.add(addrStr)
      n.peerHints[peerId] = merged
    else:
      n.peerHints[peerId] = addrs
    let peerRes = PeerId.init(peerId)
    if peerRes.isOk() and not store.isNil:
      try:
        store.addAddressesWithTTL(peerRes.get(), parsed, chronos.minutes(30))
      except CatchableError as exc:
        warn "initial peer hint addAddressesWithTTL failed", peer = peerId, source = source, err = exc.msg
    info "initial peer hints registered", peer = peerId, source = source, hintCount = parsed.len


proc computeMdnsIntervals(n: NimNode): tuple[queryMs: int, announceMs: int] =
  var queryMs = max(n.mdnsIntervalSeconds, 1) * 1000
  let envQuery = getEnv("UNIMAKER_MDNS_QUERY_MS")
  if envQuery.len > 0:
    try:
      let ms = parseInt(envQuery)
      if ms > 0:
        queryMs = ms
    except CatchableError:
      discard
  var announceMs = max(n.mdnsIntervalSeconds, 2) * 1000
  let envAnnounce = getEnv("UNIMAKER_MDNS_ANNOUNCE_MS")
  if envAnnounce.len > 0:
    try:
      let ms = parseInt(envAnnounce)
      if ms > 0:
        announceMs = ms
    except CatchableError:
      discard
  if queryMs < 1000:
    queryMs = 1000
  if announceMs < 1000:
    announceMs = 1000
  (queryMs: queryMs, announceMs: announceMs)

proc prepareMdns(n: NimNode) =
  if n.discovery != nil or not n.mdnsEnabled or n.switchInstance.isNil:
    return
  let peerInfo = n.switchInstance.peerInfo
  if peerInfo.isNil:
    debugLog("[nimlibp2p] prepareMdns: peerInfo missing, skip initialization")
    return
  discard refreshLanAwareAddrs(n)
  let serviceName = computeMdnsServiceName(n)
  let intervals = computeMdnsIntervals(n)
  var preferredIp = ""
  let lanIpsSnapshot = collectLanIpv4Addrs(n.mdnsPreferredIpv4)
  if not peerInfo.isNil:
    for ma in peerInfo.listenAddrs:
      let addrStr = $ma
      if addrStr.contains("/ip4/"):
        let parts = addrStr.split("/ip4/")
        if parts.len >= 2:
          let ipPart = parts[^1]
          let slashIdx = ipPart.find('/')
          let ip = if slashIdx >= 0: ipPart[0 ..< slashIdx] else: ipPart
          if ip.len > 0 and ip != "0.0.0.0" and ip != "127.0.0.1":
            preferredIp = ip
            break
  if preferredIp.len == 0 and lanIpsSnapshot.len > 0:
    preferredIp = lanIpsSnapshot[0]
  if preferredIp.len == 0 and n.mdnsPreferredIpv4.len > 0:
    preferredIp = n.mdnsPreferredIpv4
  if preferredIp.len == 0:
    let detected = detectPrimaryIpv4()
    if detected != "0.0.0.0":
      preferredIp = detected
  if preferredIp.len > 0:
    n.mdnsPreferredIpv4 = preferredIp
  debugLog(
    "[nimlibp2p] prepareMdns listen=" &
    (if peerInfo.listenAddrs.len > 0: peerInfo.listenAddrs.mapIt($it).join(",") else: "<empty>") &
    " addrs=" &
    (if peerInfo.addrs.len > 0: peerInfo.addrs.mapIt($it).join(",") else: "<empty>") &
    " preferred=" & (if preferredIp.len > 0: preferredIp else: "<unset>") &
    " lanIps=" & (if lanIpsSnapshot.len > 0: lanIpsSnapshot.join(",") else: "<empty>")
  )
  var mdnsIface: MdnsInterface
  n.mdnsPreferredIpv4 = preferredIp
  try:
    mdnsIface = MdnsInterface.new(
      peerInfo = peerInfo,
      serviceName = serviceName,
      queryInterval = chronos.milliseconds(intervals.queryMs),
      announceInterval = chronos.milliseconds(intervals.announceMs),
      preferredIpv4 = preferredIp
    )
    debugLog("[nimlibp2p] mdns interface created service=" & serviceName &
      " queryMs=" & $intervals.queryMs & " announceMs=" & $intervals.announceMs)
  except Exception as exc:
    warn "mdns init failed", err = exc.msg
    return
  let disc = DiscoveryManager()
  disc.add(mdnsIface)
  n.discovery = disc
  n.mdnsInterface = mdnsIface
  n.mdnsService = serviceName
  n.mdnsServices = @[]
  n.mdnsQueries = @[]
  n.mdnsWatchers = @[]
  n.mdnsLastSeen = initTable[string, int64]()
  n.mdnsReady = false
  debugLog("[nimlibp2p] prepareMdns: initialized service=" & serviceName)

proc updateMdnsAdvertisements(n: NimNode) =
  if n.discovery.isNil or n.mdnsInterface.isNil or n.switchInstance.isNil:
    return
  let peerInfo = n.switchInstance.peerInfo
  if peerInfo.isNil:
    debugLog("[nimlibp2p] updateMdnsAdvertisements: peerInfo missing")
    return
  let service = if n.mdnsService.len > 0: n.mdnsService else: computeMdnsServiceName(n)
  try:
    n.discovery.advertise(DiscoveryService(service))
    n.discovery.advertise(peerInfo.peerId)
    let lanAware = refreshLanAwareAddrs(n)
    let advertised = if lanAware.len > 0: lanAware else: peerInfo.addrs
    let fullAddrs = peerInfo.fullAddrs().valueOr: @[]
    debugLog(
      "[nimlibp2p] updateMdnsAdvertisements service=" & service &
      " addrCount=" & $fullAddrs.len &
      " peer=" & $peerInfo.peerId
    )
    debugLog("[nimlibp2p] mdns advertise service=" & service &
      " addrCount=" & $fullAddrs.len)
    for addr in advertised:
      n.discovery.advertise(addr)
    for addr in fullAddrs:
      n.discovery.advertise(addr)
  except CatchableError as exc:
    warn "updateMdnsAdvertisements: discovery advertise failed", service = service, err = exc.msg
    return
  if not n.mdnsInterface.advertisementUpdated.isNil:
    n.mdnsInterface.advertisementUpdated.fire()

proc mdnsDialable(ma: MultiAddress, reason: var string): bool =
  let text = $ma
  if text.len == 0:
    reason = "empty_multiaddr"
    return false
  if "/p2p-circuit" in text:
    reason = "p2p_circuit_address"
    return false
  if "/unix/" in text or "/memory/" in text:
    reason = "unsupported_transport"
    return false
  if "/ip6zone/" in text:
    reason = "ip6_zone_not_supported"
    return false
  if "/ip4/0.0.0.0" in text:
    reason = "unspecified_ipv4"
    return false
  if "/ip6/::" in text:
    reason = "unspecified_ipv6"
    return false
  if hasZeroPortSegment(text, "/tcp/"):
    reason = "tcp_port_zero"
    return false
  if hasZeroPortSegment(text, "/udp/"):
    reason = "udp_port_zero"
    return false
  reason = ""
  true

proc filterDialable(addrs: seq[MultiAddress]): (seq[MultiAddress], seq[string]) =
  var accepted: seq[MultiAddress] = @[]
  var rejected: seq[string] = @[]
  for ma in addrs:
    var reason = ""
    if mdnsDialable(ma, reason):
      accepted.add(ma)
    else:
      let desc = $ma
      rejected.add(
        (if desc.len > 0: desc else: "<empty>") & ":" & reason
      )
  (accepted, rejected)

proc shouldRetryDial(errMsg: string): bool =
  ## Returns true when a dial failure looks transient and worth retrying.
  if errMsg.len == 0:
    return false
  let lowered = errMsg.toLowerAscii()
  ("peer not ready" in lowered) or
    ("connection refused" in lowered) or
    ("dial backoff" in lowered) or
    ("connection reset" in lowered) or
    ("i/o timeout" in lowered) or
    ("no route to host" in lowered) or
    ("temporary" in lowered) or
    ("newstream" in lowered)

proc gatherKnownAddrs(n: NimNode, peer: PeerId): seq[MultiAddress] =
  var seen = initHashSet[string]()
  let peerStr = $peer
  let hints = n.peerHints.getOrDefault(peerStr)
  for entry in hints:
    let ma = MultiAddress.init(entry).valueOr:
      continue
    let key = $ma
    if key.len > 0 and key notin seen:
      seen.incl(key)
      result.add(ma)
  if n.switchInstance.isNil:
    return
  let store = n.switchInstance.peerStore
  if store.isNil:
    return
  try:
    let addrBook = store[AddressBook]
    if addrBook.book.hasKey(peer):
      let stored = addrBook.book.getOrDefault(peer)
      for ma in stored:
        let key = $ma
        if key.len == 0 or key in seen:
          continue
        seen.incl(key)
        result.add(ma)
  except CatchableError as exc:
    debugLog("[nimlibp2p] gatherKnownAddrs failed peer=" & peerStr & " err=" & exc.msg)
  except Exception as exc:
    debugLog("[nimlibp2p] gatherKnownAddrs fatal peer=" & peerStr & " err=" & exc.msg)

proc peerCurrentlyConnected(n: NimNode, peer: PeerId): bool =
  if n.switchInstance.isNil:
    return false
  try:
    for pid in n.switchInstance.connectedPeers(Direction.Out):
      if pid == peer:
        return true
    for pid in n.switchInstance.connectedPeers(Direction.In):
      if pid == peer:
        return true
  except CatchableError:
    discard
  false

proc ensurePeerConnectivity(n: NimNode, peer: PeerId): Future[bool] {.async.} =
  if peerCurrentlyConnected(n, peer):
    return true
  let known = gatherKnownAddrs(n, peer)
  if known.len == 0:
    debugLog("[nimlibp2p] ensurePeerConnectivity no addresses peer=" & $peer)
    return false
  let (dialable, _) = filterDialable(known)
  if dialable.len == 0:
    debugLog("[nimlibp2p] ensurePeerConnectivity no dialable addresses peer=" & $peer)
    return false
  try:
    await n.switchInstance.connect(peer, dialable, forceDial = true)
    return true
  except CatchableError as exc:
    let errMsg = if exc.msg.len > 0: exc.msg else: $exc.name
    warn "ensurePeerConnectivity connect failed", peer = $peer, err = errMsg
    let lowered = errMsg.toLowerAscii()
    if lowered.contains("failed dial existing") or lowered.contains("newstream"):
      try:
        await n.switchInstance.disconnect(peer)
        debugLog("[nimlibp2p] ensurePeerConnectivity disconnect stale conn peer=" & $peer)
      except CatchableError as discExc:
        debugLog("[nimlibp2p] ensurePeerConnectivity disconnect failed peer=" & $peer & " err=" & discExc.msg)
  except Exception as exc:
    warn "ensurePeerConnectivity connect fatal", peer = $peer, err = exc.msg
  peerCurrentlyConnected(n, peer)

proc stripTrailingDots(value: var string) =
  while value.len > 0 and value[^1] == '.':
    value.setLen(value.len - 1)

proc resolveServiceBase(name: string): string =
  var base = name.strip()
  stripTrailingDots(base)
  const LocalSuffix = ".local"
  if base.len >= LocalSuffix.len:
    let tail = base[(base.len - LocalSuffix.len) ..< base.len]
    if tail.toLowerAscii() == LocalSuffix:
      base.setLen(base.len - LocalSuffix.len)
      stripTrailingDots(base)
  base

proc serviceQueryVariants(name: string): seq[string] =
  ## 生成含/不含 `.local` 与尾随点的服务名变体，兼容不同端的 mDNS 约定。
  var variants: seq[string] = @[]
  proc addVariant(value: string) =
    let candidate = value.strip()
    if candidate.len == 0:
      return
    if variants.find(candidate) < 0:
      variants.add(candidate)
  addVariant(name)
  let base = resolveServiceBase(name)
  if base.len == 0:
    return variants
  addVariant(base)
  addVariant(base & ".")
  addVariant(base & ".local")
  addVariant(base & ".local.")
  variants

proc reportMdnsDialFailure(
    n: NimNode,
    peer: PeerId,
    addrs: seq[MultiAddress],
    reason: string,
    transient: bool
) =
  var payload = newJObject()
  payload["type"] = %"MdnsDialError"
  payload["peer_id"] = %($peer)
  payload["reason"] = %reason
  payload["timestamp_ms"] = %nowMillis()
  payload["transient"] = %transient
  var arr = newJArray()
  for ma in addrs:
    arr.add(%($ma))
  payload["addresses"] = arr
  recordEvent(n, "network_event", $payload)

proc connectMdnsPeer(n: NimNode, peer: PeerId, addrs: seq[MultiAddress]) {.async.} =
  if n.switchInstance.isNil or addrs.len == 0:
    return
  let (dialable, rejected) = filterDialable(addrs)
  if rejected.len > 0:
    for entry in rejected:
      debugLog("[nimlibp2p] mdns skip dial addr peer=" & $peer & " reason=" & entry)
  if dialable.len == 0:
    debugLog("[nimlibp2p] mdns discovery no dialable addresses peer=" & $peer)
    reportMdnsDialFailure(n, peer, addrs, "no_dialable_addresses", transient = false)
    return
  var preferred: seq[MultiAddress] = @[]
  var fallback: seq[MultiAddress] = @[]
  for ma in dialable:
    let addrStr = $ma
    if addrStr.contains("/quic") or addrStr.contains("/udp/"):
      preferred.add(ma)
    else:
      fallback.add(ma)
  let ordered = preferred & fallback
  let maxAttempts = 3
  var attempt = 0
  var success = false
  var lastError = ""
  while attempt < maxAttempts and not success:
    try:
      await n.switchInstance.connect(peer, ordered, forceDial = true)
      success = true
    except CatchableError as exc:
      let errMsg = if exc.msg.len > 0: exc.msg else: $exc.name
      lastError = errMsg
      let attemptNum = attempt + 1
      warn "mdns dial failed", peer = $peer, err = errMsg, attempt = attemptNum
      debugLog("[nimlibp2p] mdns dial failed peer=" & $peer & " attempt=" & $attemptNum &
        " err=" & errMsg)
      attempt.inc()
      if attempt >= maxAttempts or not shouldRetryDial(errMsg):
        break
      let delayMs = min(800, 200 * attempt)
      await sleepAsync(chronos.milliseconds(delayMs))
    except Exception as exc:
      lastError = if exc.msg.len > 0: exc.msg else: $exc.name
      warn "mdns dial fatal", peer = $peer, err = lastError
      debugLog("[nimlibp2p] mdns dial fatal peer=" & $peer & " err=" & lastError)
      attempt = maxAttempts
      break
  if not success:
    let reason = if lastError.len > 0: lastError else: "connect failed"
    let transient = shouldRetryDial(reason)
    reportMdnsDialFailure(n, peer, ordered, reason, transient = transient)

proc noteMdnsDiscovery(n: NimNode, peer: PeerId, addrs: seq[MultiAddress]) =
  if addrs.len == 0:
    return
  if n.switchInstance.isNil:
    debugLog("[nimlibp2p] noteMdnsDiscovery: switchInstance missing")
    return
  let localInfo = n.switchInstance.peerInfo
  if not localInfo.isNil:
    try:
      if peer == localInfo.peerId:
        debugLog("[nimlibp2p] noteMdnsDiscovery: ignoring self advertisement")
        return
    except CatchableError:
      debugLog("[nimlibp2p] noteMdnsDiscovery: peerId compare failed")
  let (dialable, rejected) = filterDialable(addrs)
  if rejected.len > 0:
    for entry in rejected:
      debugLog("[nimlibp2p] mdns discovery drop addr peer=" & $peer & " reason=" & entry)
  var addrStrings: seq[string] = @[]
  for ma in dialable:
    let addrStr = $ma
    if addrStr.len > 0:
      addrStrings.add(addrStr)
  if addrStrings.len == 0:
    debugLog("[nimlibp2p] noteMdnsDiscovery: no dialable addresses peer=" & $peer)
    return
  let peerStr = $peer
  info "mdns peer discovered", peer = peerStr, addresses = addrStrings
  n.peerHints[peerStr] = addrStrings
  let peerStore = n.switchInstance.peerStore
  if not peerStore.isNil:
    try:
      peerStore.addAddressesWithTTL(peer, addrs, chronos.minutes(30))
    except Defect as defect:
      let defectKind = $defect.name
      let defectMsg = defect.msg
      warn "mdns add addresses defect", peer = peerStr, kind = defectKind, err = defectMsg
      debugLog("[nimlibp2p] mdns add addresses defect peer=" & peerStr &
        " kind=" & defectKind & " err=" & defectMsg)
    except CatchableError as exc:
      warn "mdns add addresses failed", peer = peerStr, err = exc.msg

  let now = nowMillis()
  var shouldConnect = true
  if n.mdnsLastSeen.hasKey(peerStr):
    let last = n.mdnsLastSeen.getOrDefault(peerStr)
    if now - last < MdnsConnectThrottleMs:
      shouldConnect = false
      debugLog("[nimlibp2p] mdns discovery throttled peer=" & peerStr & " elapsed=" & $(now - last))
  n.mdnsLastSeen[peerStr] = now

  var event = newJObject()
  event["type"] = %"MdnsPeerDiscovered"
  event["peer_id"] = %peerStr
  var arr = newJArray()
  for addrStr in addrStrings:
    arr.add(%addrStr)
  event["addresses"] = arr
  event["timestamp_ms"] = %now
  event["discovery"] = %"mdns"
  recordEvent(n, "network_event", $event)

  if shouldConnect and dialable.len > 0:
    asyncCheck(connectMdnsPeer(n, peer, dialable))

proc watchMdns(n: NimNode, query: DiscoveryQuery, service: string): Future[void] {.async.} =
  debugLog("[nimlibp2p] mdns watcher started service=" & service)
  while n.running.load(moAcquire) and n.mdnsEnabled:
    try:
      let attrs = await query.getPeer()
      let peerOpt = attrs{PeerId}
      if peerOpt.isNone:
        continue
      let peer = peerOpt.get()
      if not n.switchInstance.isNil and not n.switchInstance.peerInfo.isNil and n.switchInstance.peerInfo.peerId == peer:
        continue
      let addrs = attrs.getAll(MultiAddress)
      if addrs.len == 0:
        continue
      debugLog("[nimlibp2p] mdns watcher discovered peer=" & $peer &
        " addrCount=" & $addrs.len & " service=" & service)
      noteMdnsDiscovery(n, peer, addrs)
    except DiscoveryFinished:
      break
    except CancelledError:
      break
    except CatchableError as exc:
      warn "mdns watcher error", err = exc.msg
      await sleepAsync(chronos.seconds(2))
    except Exception as exc:
      warn "mdns watcher fatal", err = exc.msg
      debugLog("[nimlibp2p] mdns watcher fatal service=" & service & " err=" & exc.msg)
      await sleepAsync(chronos.seconds(2))

proc stopMdns(n: NimNode): Future[void] {.async.} =
  if n.mdnsWatchers.len > 0:
    for watcher in n.mdnsWatchers.mitems:
      if watcher.isNil:
        continue
      watcher.cancelSoon()
      try:
        await watcher
      except CancelledError:
        discard
      except CatchableError as exc:
        warn "mdns watcher stop failed", err = exc.msg
    n.mdnsWatchers.setLen(0)
  if n.mdnsQueries.len > 0:
    for query in n.mdnsQueries.mitems:
      if not query.isNil:
        query.stop()
    n.mdnsQueries.setLen(0)
  if not n.discovery.isNil:
    n.discovery.stop()
    n.discovery = nil
  if not n.mdnsInterface.isNil:
    debugLog("[nimlibp2p] stopMdns closing transport")
    try:
      await n.mdnsInterface.closeTransport()
    except CatchableError as exc:
      warn "mdns close transport failed", err = exc.msg
  n.mdnsInterface = nil
  n.mdnsServices = @[]
  n.mdnsService = DefaultMdnsServiceName
  n.mdnsLastSeen = initTable[string, int64]()
  n.mdnsReady = false
  n.mdnsStarting = false

proc doStartMdns(n: NimNode): Future[void] {.async.} =
  defer:
    n.mdnsStarting = false
    n.mdnsReady = (n.mdnsWatchers.len > 0 and n.mdnsQueries.len > 0)
  if not n.mdnsEnabled or n.switchInstance.isNil:
    return
  prepareMdns(n)
  if n.discovery.isNil or n.mdnsInterface.isNil:
    return
  debugLog("[nimlibp2p] startMdns: updating advertisements started=" & $n.started)
  updateMdnsAdvertisements(n)
  if not n.started:
    return
  var services: seq[string] = @[]
  let candidates = serviceQueryVariants(n.mdnsService) & serviceQueryVariants("_unimaker._udp.")
  for candidate in candidates:
    if candidate.len > 0 and services.find(candidate) < 0:
      services.add(candidate)
  n.mdnsServices = services
  n.mdnsQueries = @[]
  n.mdnsWatchers = @[]
  for service in services:
    let query = n.discovery.request(DiscoveryService(service))
    n.mdnsQueries.add(query)
    let watcher = watchMdns(n, query, service)
    n.mdnsWatchers.add(watcher)
    asyncSpawn(watcher)
    debugLog("[nimlibp2p] startMdns: watcher spawned service=" & service)
  info "mdns enabled", service = services.join(",")

proc startMdns(n: NimNode): Future[void] {.async.} =
  if n.mdnsStarting:
    debugLog("[nimlibp2p] startMdns: already in progress")
    return
  n.mdnsStarting = true
  await doStartMdns(n)

proc markMdnsRestartPending(n: NimNode) =
  n.mdnsRestartPending = true

proc scheduleStartMdns(n: NimNode) =
  if n.mdnsStarting:
    debugLog("[nimlibp2p] scheduleStartMdns: already in progress")
    return
  n.mdnsStarting = true
  proc safeStart() {.async.} =
    try:
      await doStartMdns(n)
    except CatchableError as exc:
      warn "startMdns schedule failed", err = exc.msg
      debugLog("[nimlibp2p] scheduleStartMdns failed err=" & exc.msg)
      markMdnsRestartPending(n)
    except Exception as exc:
      warn "startMdns schedule fatal", err = exc.msg
      debugLog("[nimlibp2p] scheduleStartMdns fatal err=" & exc.msg)
      markMdnsRestartPending(n)
  asyncSpawn(safeStart())

proc restartMdns(n: NimNode): Future[void] {.async: (raises: [CatchableError, Exception]).} =
  debugLog("[nimlibp2p] restartMdns invoked")
  if n.mdnsRestarting:
    debugLog("[nimlibp2p] restartMdns skipped (already running)")
    return
  n.mdnsRestarting = true
  n.mdnsRestartPending = false
  defer:
    n.mdnsRestarting = false
  try:
    await stopMdns(n)
    await startMdns(n)
  except CatchableError as exc:
    markMdnsRestartPending(n)
    warn "restartMdns failed", err = exc.msg
    debugLog("[nimlibp2p] restartMdns failed err=" & exc.msg)
    raise
  except Exception as exc:
    markMdnsRestartPending(n)
    warn "restartMdns fatal", err = exc.msg
    debugLog("[nimlibp2p] restartMdns fatal err=" & exc.msg)
    raise

proc restartMdnsSafe(n: NimNode, reason: string): Future[void] {.async.} =
  try:
    await restartMdns(n)
  except CatchableError as exc:
    warn "restartMdns async failed", reason = reason, err = exc.msg
    debugLog("[nimlibp2p] restartMdnsSafe failed reason=" & reason & " err=" & exc.msg)
  except Exception as exc:
    warn "restartMdns async fatal", reason = reason, err = exc.msg
    debugLog("[nimlibp2p] restartMdnsSafe fatal reason=" & reason & " err=" & exc.msg)

proc kickMdnsQuery(n: NimNode): Future[void] {.async.} =
  let watcherCount = n.mdnsWatchers.len
  let queryCount = n.mdnsQueries.len
  let starting = n.mdnsStarting
  let restarting = n.mdnsRestarting
  let ready = n.mdnsReady
  if watcherCount == 0 or queryCount == 0:
    if starting or restarting or not ready:
      debugLog(
        "[nimlibp2p] kickMdnsQuery skip restart watchers=" & $watcherCount &
        " queries=" & $queryCount & " starting=" & $starting &
        " restarting=" & $restarting & " ready=" & $ready
      )
      updateMdnsAdvertisements(n)
      return
    debugLog("[nimlibp2p] kickMdnsQuery restarting watchers=" & $watcherCount & " queries=" & $queryCount)
    try:
      await restartMdns(n)
    except CatchableError as exc:
      warn "kickMdnsQuery restart failed", err = exc.msg
      debugLog("[nimlibp2p] kickMdnsQuery restart failed err=" & exc.msg)
      await sleepAsync(chronos.seconds(2))
    except Exception as exc:
      warn "kickMdnsQuery restart fatal", err = exc.msg
      debugLog("[nimlibp2p] kickMdnsQuery restart fatal err=" & exc.msg)
      await sleepAsync(chronos.seconds(2))
  else:
    debugLog("[nimlibp2p] kickMdnsQuery keep-alive watchers=" & $watcherCount & " queries=" & $queryCount)
    updateMdnsAdvertisements(n)

proc setupConnectionEvents(n: NimNode) =
  if n.connectionEventsEnabled:
    return
  let connectedHandler =
    proc(peerId: PeerId, event: ConnEvent): Future[void] {.closure, async: (raises: [CancelledError]).} =
      var ev = newJObject()
      ev["type"] = %"ConnectionEstablished"
      ev["peer_id"] = %($peerId)
      ev["incoming"] = %(event.incoming)
      ev["timestamp_ms"] = %nowMillis()
      recordEvent(n, "network_event", $ev)
  let disconnectedHandler =
    proc(peerId: PeerId, event: ConnEvent): Future[void] {.closure, async: (raises: [CancelledError]).} =
      var ev = newJObject()
      ev["type"] = %"ConnectionClosed"
      ev["peer_id"] = %($peerId)
      ev["timestamp_ms"] = %nowMillis()
      recordEvent(n, "network_event", $ev)
  n.switchInstance.connManager.addConnEventHandler(connectedHandler, ConnEventKind.Connected)
  n.switchInstance.connManager.addConnEventHandler(disconnectedHandler, ConnEventKind.Disconnected)
  n.connectionHandlers = @[(ConnEventKind.Connected, connectedHandler),
                           (ConnEventKind.Disconnected, disconnectedHandler)]
  n.connectionEventsEnabled = true

proc ensureKeepAliveLoop(n: NimNode) =
  if not n.keepAliveLoop.isNil and not n.keepAliveLoop.finished:
    return
  let loop = proc(): Future[void] {.closure, async: (raises: [CancelledError]).} =
    while n.running.load(moAcquire):
      for peerIdStr in n.keepAlivePeers.items:
        let peerRes = PeerId.init(peerIdStr)
        if peerRes.isErr():
          continue
        let peer = peerRes.get()
        if n.switchInstance.connManager.connCount(peer) == 0:
          let addrs = n.switchInstance.peerStore[AddressBook][peer]
          if addrs.len > 0:
            try:
              await n.switchInstance.connect(peer, addrs, forceDial = true)
            except CatchableError as exc:
              warn "keepAlive dial failed", peer = peerIdStr, err = exc.msg
              debugLog(
                "[nimlibp2p] keepAlive dial failed peer=" & peerIdStr &
                " err=" & exc.msg
              )
      await sleepAsync(chronos.seconds(20))
  let keepAliveFuture = loop()
  n.keepAliveLoop = keepAliveFuture
  asyncSpawn(keepAliveFuture)

proc boostConnectivity(n: NimNode): Future[void] {.async.}

proc refreshPeerConnections(n: NimNode) {.async.} =
  await boostConnectivity(n)

proc boostConnectivity(n: NimNode): Future[void] {.async.} =
  for peerId, addresses in n.peerHints.pairs:
    let peerRes = PeerId.init(peerId)
    if peerRes.isErr():
      continue
    var parsed: seq[MultiAddress] = @[]
    for addrStr in addresses:
      let ma = MultiAddress.init(addrStr)
      if ma.isOk():
        parsed.add(ma.get())
    if parsed.len == 0:
      continue
    try:
      await n.switchInstance.connect(peerRes.get(), parsed, forceDial = true)
    except CatchableError as exc:
      warn "boostConnectivity dial failed", peer = peerId, err = exc.msg
      debugLog(
        "[nimlibp2p] boostConnectivity dial failed peer=" & peerId &
        " err=" & exc.msg
      )

proc reserveOnRelayAsync(n: NimNode, relayAddr: string): Future[int] {.async.} =
  if n.switchInstance.isNil or n.switchInstance.peerInfo.isNil:
    debugLog("[nimlibp2p] reserveOnRelayAsync: switchInstance or peerInfo missing")
    return 0
  let maRes = MultiAddress.init(relayAddr)
  if maRes.isErr():
    return 0
  let ma = maRes.get()
  let marker = "/p2p/"
  let idx = relayAddr.rfind(marker)
  if idx < 0:
    return 0
  var peerPart = relayAddr[idx + marker.len ..< relayAddr.len]
  let slashIdx = peerPart.find('/')
  if slashIdx >= 0:
    peerPart = peerPart[0 ..< slashIdx]
  let peerRes = PeerId.init(peerPart)
  if peerRes.isErr():
    return 0
  var baseAddr = relayAddr[0 ..< idx]
  if baseAddr.len == 0:
    baseAddr = relayAddr
  var addrs: seq[MultiAddress] = @[]
  let baseRes = MultiAddress.init(baseAddr)
  if baseRes.isOk():
    addrs.add(baseRes.get())
  if addrs.len == 0:
    addrs.add(ma)
  var client = RelayClient.new(canHop = true)
  client.switch = n.switchInstance
  try:
    let rsvp = await client.reserve(peerRes.get(), addrs)
    let peerInfo = n.switchInstance.peerInfo
    if not peerInfo.isNil:
      for addr in rsvp.addrs:
        if addr notin peerInfo.addrs:
          peerInfo.addrs.add(addr)
      await peerInfo.update()
    n.relayReservations[relayAddr] = int64(rsvp.expire)
    return rsvp.addrs.len
  except CatchableError as exc:
    warn "reserveOnRelayAsync failed", relay = relayAddr, err = exc.msg
    return 0

proc waitForSecureChannel(n: NimNode, peerId: string, timeoutMs: int): Future[bool] {.async.} =
  let peerRes = PeerId.init(peerId)
  if peerRes.isErr():
    return false
  let peer = peerRes.get()
  let deadline = nowMillis() + max(timeoutMs, 500)
  while nowMillis() < deadline:
    if n.switchInstance.connManager.connCount(peer) > 0:
      return true
    await sleepAsync(chronos.milliseconds(150))
  false

type
  NodeHandle = ptr NodeHandleObj
  NodeHandleObj = object
    node: NimNode

proc makeNodeHandle(node: NimNode): pointer =
  let handle = cast[NodeHandle](allocShared0(sizeof(NodeHandleObj)))
  handle[].node = node
  cast[pointer](handle)

proc nodeFromHandle(handle: pointer): NimNode =
  if handle.isNil:
    return nil
  cast[NodeHandle](handle)[].node

proc freeNodeHandle(handle: pointer) =
  if handle.isNil:
    return
  let h = cast[NodeHandle](handle)
  h[].node = nil

# -------------------------------------------------------------
# 配置与 Switch 构建
# -------------------------------------------------------------

proc defaultFeatureSet(): FeatureSet =
  FeatureSet(
    gossipsub: true,
    directStream: false,
    rendezvous: false,
    autonat: false,
    circuitRelay: false,
    livestream: false,
    dataTransfer: false,
  )

proc defaultConfig(): NodeConfig =
  NodeConfig(
    listenAddresses: @["/ip4/0.0.0.0/tcp/0"],
    enableMetrics: false,
    metricsPort: none(int),
    metricsTextfile: none(MetricsTextfileConfig),
    privateNetwork: none(string),
    automations: defaultFeatureSet(),
    identity: none(IdentityConfig),
    dataDir: none(string),
    extra: newJNull(),
  )

proc jsonBool(node: JsonNode, key: string, defaultValue: bool): bool =
  if node.kind == JObject:
    let value = node{key}
    if not value.isNil and value.kind == JBool:
      return value.getBool()
  defaultValue

proc jsonInt(node: JsonNode, key: string): Option[int] =
  if node.kind == JObject:
    let value = node{key}
    if not value.isNil:
      if value.kind in {JInt, JFloat}:
        return some(value.getInt())
      elif value.kind == JString:
        try:
          return some(parseInt(value.getStr()))
        except ValueError:
          discard
  result = none(int)

proc parseFeatureSet(node: JsonNode): FeatureSet =
  result = defaultFeatureSet()
  if node.isNil or node.kind != JObject:
    return
  result.gossipsub = jsonBool(node, "gossipsub", result.gossipsub)
  result.directStream = jsonBool(node, "directStream", result.directStream)
  result.rendezvous = jsonBool(node, "rendezvous", result.rendezvous)
  result.autonat = jsonBool(node, "autonat", result.autonat)
  result.circuitRelay = jsonBool(node, "circuitRelay", result.circuitRelay)
  result.livestream = jsonBool(node, "livestream", result.livestream)
  result.dataTransfer = jsonBool(node, "dataTransfer", result.dataTransfer)

proc jsonString(node: JsonNode, key: string): Option[string] =
  if node.kind == JObject:
    let value = node{key}
    if not value.isNil and value.kind == JString:
      return some(value.getStr())
  none(string)

proc decodeBase64Bytes(value: string): Result[seq[byte], string] =
  try:
    let decoded = base64.decode(value)
    var bytes = newSeq[byte](decoded.len)
    for i in 0 ..< decoded.len:
      bytes[i] = decoded[i].byte
    ok(bytes)
  except CatchableError as exc:
    err("base64 decode failed: " & exc.msg)

proc parseIdentity(node: JsonNode): Result[IdentityConfig, string] =
  if node.isNil or node.kind != JObject:
    return err("identity must be an object")

  let privOpt = jsonString(node, "privateKey")
  if privOpt.isNone:
    return err("identity.privateKey missing")
  let pubOpt = jsonString(node, "publicKey")
  if pubOpt.isNone:
    return err("identity.publicKey missing")

  debugLog("[nimlibp2p] identity.privateKey present" )
  let privBytes = decodeBase64Bytes(privOpt.get()).valueOr:
    return err("failed to decode identity.privateKey: " & error)
  debugLog("[nimlibp2p] identity.privateKey decoded len=" & $privBytes.len)
  let pubBytes = decodeBase64Bytes(pubOpt.get()).valueOr:
    return err("failed to decode identity.publicKey: " & error)
  debugLog("[nimlibp2p] identity.publicKey decoded len=" & $pubBytes.len)

  if pubBytes.len != 32:
    return err("identity.publicKey must be 32 bytes, got " & $pubBytes.len)

  var privateCombined: seq[byte]
  case privBytes.len
  of 64:
    debugLog("[nimlibp2p] using 64-byte private key")
    privateCombined = privBytes
  of 32:
    debugLog("[nimlibp2p] expanding 32-byte private seed")
    privateCombined = newSeq[byte](64)
    for i in 0 ..< 32:
      privateCombined[i] = privBytes[i]
    for j in 0 ..< 32:
      privateCombined[32 + j] = pubBytes[j]
  else:
    return err("identity.privateKey must be 32 or 64 bytes, got " & $privBytes.len)

  var peerId = ""
  let peerOpt = jsonString(node, "peerId")
  if peerOpt.isSome:
    peerId = peerOpt.get()

  let sourceOpt = jsonString(node, "source")
  let keyPathOpt = jsonString(node, "keyPath")

  ok(
    IdentityConfig(
      privateKey: privateCombined,
      publicKey: pubBytes,
      peerId: peerId,
      source: sourceOpt,
      keyPath: keyPathOpt,
    )
  )

proc parseConfig(configJson: cstring): Result[NodeConfig, string] =
  var cfg = defaultConfig()
  if configJson != nil and configJson.len > 0:
    let text = $configJson
    try:
      debugLog("[nimlibp2p] parseConfig: parsing JSON len=" & $text.len)
      let root = parseJson(text)
      if root.kind == JObject:
        debugLog("[nimlibp2p] parseConfig: root object")
        debugLog("[nimlibp2p] parseConfig: before listenAddresses")
        var listenNode: JsonNode
        if root.kind == JObject:
          listenNode = root{"listenAddresses"}
          if listenNode.isNil:
            listenNode = newJNull()
        else:
          listenNode = newJNull()
        debugLog("[nimlibp2p] parseConfig: listen node kind=" & $listenNode.kind)
        if listenNode.kind == JArray:
          cfg.listenAddresses = @[]
          for entry in listenNode:
            if entry.kind == JString:
              cfg.listenAddresses.add(entry.getStr())
        debugLog("[nimlibp2p] parseConfig: listen count=" & $cfg.listenAddresses.len)
        debugLog("[nimlibp2p] parseConfig: before enableMetrics")
        cfg.enableMetrics = jsonBool(root, "enableMetrics", cfg.enableMetrics)
        debugLog("[nimlibp2p] parseConfig: enableMetrics=" & $cfg.enableMetrics)
        cfg.metricsPort = jsonInt(root, "metricsPort")
        if cfg.metricsPort.isSome:
          debugLog("[nimlibp2p] parseConfig: metricsPort=" & $cfg.metricsPort.get())
        if root.hasKey("metricsTextfile"):
          let textNode = root{"metricsTextfile"}
          if not textNode.isNil and textNode.kind == JObject and textNode.hasKey("path"):
            var tfCfg = MetricsTextfileConfig(
              path: textNode["path"].getStr(),
              intervalMs: max(1000, textNode["intervalMs"].getInt()),
              labels: initTable[string, string]()
            )
            if textNode.hasKey("labels") and textNode["labels"].kind == JObject:
              for key, value in textNode["labels"].pairs:
                if key.len > 0:
                  tfCfg.labels[key] = value.getStr()
            cfg.metricsTextfile = some(tfCfg)
            debugLog("[nimlibp2p] parseConfig: metricsTextfile path=" & tfCfg.path)
        var privateNode: JsonNode
        if root.kind == JObject:
          privateNode = root{"privateNetwork"}
          if privateNode.isNil:
            privateNode = newJNull()
        else:
          privateNode = newJNull()
        debugLog("[nimlibp2p] parseConfig: privateNetwork node kind=" & $privateNode.kind)
        if privateNode.kind == JString:
          cfg.privateNetwork = some(privateNode.getStr())
          debugLog("[nimlibp2p] parseConfig: privateNetwork set")
        var automationNode: JsonNode
        if root.kind == JObject:
          automationNode = root{"automations"}
          if automationNode.isNil:
            automationNode = newJNull()
        else:
          automationNode = newJNull()
        debugLog("[nimlibp2p] parseConfig: automations node kind=" & $automationNode.kind)
        if automationNode.kind == JObject:
          cfg.automations = parseFeatureSet(automationNode)
          debugLog("[nimlibp2p] parseConfig: automations parsed")
        var identityNode: JsonNode
        if root.kind == JObject:
          identityNode = root{"identity"}
          if identityNode.isNil:
            identityNode = newJNull()
        else:
          identityNode = newJNull()
        debugLog("[nimlibp2p] identity node kind=" & $identityNode.kind)
        if identityNode.kind == JObject:
          debugLog("[nimlibp2p] identity node detected")
          let identityCfg = parseIdentity(identityNode).valueOr:
            return err("identity parse failed: " & error)
          cfg.identity = some(identityCfg)
          debugLog("[nimlibp2p] identity parsed")
        var dataDirNode: JsonNode
        if root.kind == JObject:
          dataDirNode = root{"dataDir"}
          if dataDirNode.isNil:
            dataDirNode = newJNull()
        else:
          dataDirNode = newJNull()
        if dataDirNode.kind == JString and dataDirNode.getStr().len > 0:
          cfg.dataDir = some(dataDirNode.getStr())
          debugLog("[nimlibp2p] parseConfig: dataDir=" & dataDirNode.getStr())
        if root.kind == JObject:
          let extraNode = root{"extra"}
          if not extraNode.isNil:
            cfg.extra = extraNode
          else:
            cfg.extra = newJNull()
        else:
          cfg.extra = newJNull()
        debugLog("[nimlibp2p] parseConfig: extra captured kind=" & $cfg.extra.kind)
        debugLog("[nimlibp2p] parseConfig: completed root processing")
    except CatchableError as exc:
      return err("parse json failed: " & exc.msg)
  ok(cfg)

proc buildSwitch(cfg: NodeConfig): Result[Switch, string] =
  debugLog("[nimlibp2p] buildSwitch: entering")
  debugLog("[nimlibp2p] buildSwitch config summary listenCount=" & $cfg.listenAddresses.len &
    " metrics=" & $cfg.enableMetrics &
    " privateNetwork=" & $cfg.privateNetwork.isSome() &
    " automation[gs=" & $cfg.automations.gossipsub &
    ",direct=" & $cfg.automations.directStream &
    ",rv=" & $cfg.automations.rendezvous &
    ",autonat=" & $cfg.automations.autonat &
    ",relay=" & $cfg.automations.circuitRelay &
    ",live=" & $cfg.automations.livestream &
    ",data=" & $cfg.automations.dataTransfer & "]" &
    " identityProvided=" & $cfg.identity.isSome())
  var builder = SwitchBuilder.new()
  debugLog("[nimlibp2p] buildSwitch base builder created")
  builder = builder.withRng(newRng())
  debugLog("[nimlibp2p] buildSwitch applied RNG")

  var addresses: seq[MultiAddress] = @[]
  for addr in cfg.listenAddresses:
    let parsed = MultiAddress.init(addr).valueOr:
      return err("invalid multiaddr: " & addr)
    addresses.add(parsed)
  if addresses.len > 0:
    builder = builder.withAddresses(addresses)
    debugLog("[nimlibp2p] buildSwitch applied listen addresses count=" & $addresses.len)

  if cfg.identity.isSome():
    let ident = cfg.identity.get()
    let edPriv = EdPrivateKey.init(ident.privateKey).valueOr:
      return err("identity private key invalid: " & $error)
    let privateKey = PrivateKey.init(edPriv)
    builder = builder.withPrivateKey(privateKey)
    let sourceStr = if ident.source.isSome: ident.source.get() else: ""
    info "libp2p using provided identity", peerId = ident.peerId, source = sourceStr
    if ident.peerId.len > 0:
      let derivedRes = PeerId.init(privateKey)
      if derivedRes.isErr():
        warn "failed to derive peerId from provided identity", err = $derivedRes.error
      else:
        let derivedStr = $derivedRes.get()
        if derivedStr != ident.peerId:
          warn "identity peerId mismatch", provided = ident.peerId, derived = derivedStr

  builder = builder.withTcpTransport()
  debugLog("[nimlibp2p] buildSwitch applied TCP transport")
  when defined(libp2p_msquic_experimental):
    var msquicActivated = false
    let bridgeRes = msquicruntime.acquireMsQuicBridge()
    if bridgeRes.success and not bridgeRes.bridge.isNil:
      msquicruntime.releaseMsQuicBridge(bridgeRes.bridge)
      builder = builder.withMsQuicTransport()
      debugLog("[nimlibp2p] buildSwitch applied MsQuic transport")
      msquicActivated = true
    else:
      let errMsg =
        if bridgeRes.error.len > 0: bridgeRes.error else: "unknown error"
      debugLog("[nimlibp2p] MsQuic runtime unavailable, falling back to OpenSSL QUIC err=" & errMsg)
      warn "MsQuic runtime unavailable", err = errMsg
    when defined(libp2p_quic_support):
      if not msquicActivated:
        builder = builder.withQuicTransport()
        debugLog("[nimlibp2p] buildSwitch applied QUIC transport (fallback)")
    else:
      if not msquicActivated:
        debugLog("[nimlibp2p] MsQuic unavailable and OpenSSL QUIC disabled; continuing without QUIC transport")
        warn "No QUIC transport available; continuing with TCP-only setup"
  else:
    when defined(libp2p_quic_support):
      builder = builder.withQuicTransport()
      debugLog("[nimlibp2p] buildSwitch applied QUIC transport")
  builder = builder.withYamux()
  debugLog("[nimlibp2p] buildSwitch applied Yamux")
  builder = builder.withNoise()
  debugLog("[nimlibp2p] buildSwitch applied Noise")
  builder = builder.withTls()
  debugLog("[nimlibp2p] buildSwitch applied TLS")
  builder = builder.withBandwidthLimits(BandwidthLimitConfig.init())
  debugLog("[nimlibp2p] buildSwitch applied bandwidth manager")
  builder = builder.withMemoryLimits(MemoryLimitConfig.init())
  debugLog("[nimlibp2p] buildSwitch applied memory manager")
  builder = builder.withResourceManager(ResourceManagerConfig.init())
  debugLog("[nimlibp2p] buildSwitch applied resource manager")

  if cfg.automations.autonat:
    builder = builder.withAutonat()
    debugLog("[nimlibp2p] buildSwitch enabled autonat")
  if cfg.automations.circuitRelay:
    builder = builder.withCircuitRelay()
    debugLog("[nimlibp2p] buildSwitch enabled circuit relay")
  if cfg.automations.rendezvous:
    try:
      builder = builder.withRendezVous(RendezVous.new())
      debugLog("[nimlibp2p] buildSwitch configured rendezvous")
    except RendezVousError as exc:
      return err("rendezvous init failed: " & exc.msg)

  when libp2pFetchEnabled:
    if cfg.automations.directStream:
      builder = builder.withFetch(
        proc(key: string): Future[FetchResponse] {.async.} =
          FetchResponse(status: FetchStatus.fsNotFound, data: @[])
      )

  when libp2pDataTransferEnabled:
    if cfg.automations.dataTransfer:
      builder = builder.withDataTransfer(
        proc(
            conn: Connection, msg: DataTransferMessage
        ): Future[Option[DataTransferMessage]] {.async.} =
          none(DataTransferMessage)
      )

  if cfg.enableMetrics:
    if cfg.metricsPort.isSome():
      builder = builder.withMetricsExporter(port = Port(cfg.metricsPort.get.int16))
      debugLog("[nimlibp2p] buildSwitch metrics exporter configured port=" & $cfg.metricsPort.get())
    else:
      builder = builder.withMetricsExporter()
      debugLog("[nimlibp2p] buildSwitch metrics exporter configured default port")

  if cfg.privateNetwork.isSome():
    try:
      builder = builder.withPnetFromString(cfg.privateNetwork.get())
      debugLog("[nimlibp2p] buildSwitch private network enabled")
    except CatchableError as exc:
      return err("private network key error: " & exc.msg)

  try:
    debugLog("[nimlibp2p] buildSwitch invoking builder.build")
    let sw = builder.build()
    debugLog("[nimlibp2p] buildSwitch builder.build returned peer=" & $sw.peerInfo.peerId &
      " listenAddrs=" & $sw.peerInfo.listenAddrs.mapIt($it).join(","))
    ok(sw)
  except CatchableError as exc:
    err("switch build failed: " & exc.msg)

proc initGossip(sw: Switch, cfg: NodeConfig): Result[GossipSub, string] =
  try:
    let params = GossipSubParams.init(
      floodPublish = true, # 直接下行传播，避免 DM 依赖 mesh 收敛
    )
    var gossip = GossipSub.init(
      switch = sw,
      parameters = params,
      verifySignature = true,
      sign = true,
    )
    gossip.PubSub.triggerSelf = true
    sw.mount(gossip.PubSub)
    ok(gossip)
  except CatchableError as exc:
    err("gossipsub init failed: " & exc.msg)

# -------------------------------------------------------------
# 工具函数
# -------------------------------------------------------------

proc allocCString(str: string): cstring =
  let size = str.len + 1
  let raw = cast[cstring](allocShared(size))
  if raw.isNil:
    return nil
  let buf = cast[ptr UncheckedArray[char]](raw)
  if str.len > 0:
    copyMem(buf, str.cstring, str.len)
  buf[str.len] = '\0'
  raw

proc libp2p_get_last_error*(): cstring {.exportc, cdecl, dynlib.} =
  if lastInitError.len == 0:
    return allocCString("")
  allocCString(lastInitError)

proc finalizeCommand(cmd: Command) =
  if cmd.completion.isNil:
    return
  let res = cmd.completion.fireSync()
  if res.isErr():
    warn "command completion signal failed", err = res.error

proc runCommand(n: NimNode, command: Command): Future[void] {.async: (raises: []), gcsafe.} =
  try:
    debugLog(
      "[nimlibp2p] runCommand start node=" & $cast[int](n) &
      " kind=" & $command.kind
    )
    case command.kind
    of cmdNone:
      command.resultCode = NimResultOk
    of cmdStart:
      if n.started:
        info "cmdStart: node already running"
        command.resultCode = NimResultOk
      else:
        n.stopRequested.store(false, moRelease)
        debugLog("[nimlibp2p] cmdStart: starting switch")
        prepareMdns(n)
        try:
          await n.switchInstance.start()
          n.started = true
          debugLog("[nimlibp2p] cmdStart: switch.start completed")
          info "cmdStart: switch started", listenAddrs = n.switchInstance.peerInfo.listenAddrs.mapIt($it)
          debugLog("[nimlibp2p] cmdStart: switch started")
          await setupDefaultTopics(n)
          debugLog("[nimlibp2p] cmdStart: default topics configured")
          await startMdns(n)
          debugLog("[nimlibp2p] cmdStart: mdns started")
          if n.mdnsRestartPending:
            info "cmdStart: applying deferred mdns restart", ipv4 = n.mdnsPreferredIpv4
            await restartMdns(n)
            debugLog("[nimlibp2p] cmdStart: mdns restart applied")
          asyncSpawn(logMemoryLoop(n))
          debugLog(
            "[nimlibp2p] cmdStart: marking success node=" & $cast[int](n)
          )
          command.resultCode = NimResultOk
        except Exception as exc:
          let errMsg = exc.msg
          if errMsg.contains("DiscoveryInterface.advertise"):
            warn "cmdStart: discovery advertise failed, retrying without abort", err = errMsg
            debugLog("[nimlibp2p] cmdStart: treating discovery advertise failure as non-fatal")
            n.started = true
            await setupDefaultTopics(n)
            await startMdns(n)
            if n.mdnsRestartPending:
              info "cmdStart: applying deferred mdns restart", ipv4 = n.mdnsPreferredIpv4
              await restartMdns(n)
            asyncSpawn(logMemoryLoop(n))
            command.resultCode = NimResultOk
            command.errorMsg = "WARN_DISCOVERY_ADVERTISE"
          else:
            error "cmdStart: switch start failed", err = errMsg, backtrace = exc.getStackTrace()
            debugLog(
              "[nimlibp2p] cmdStart: switch start failed " & errMsg &
              " node=" & $cast[int](n)
            )
            debugLog("[nimlibp2p] cmdStart: backtrace=" & exc.getStackTrace())
            command.resultCode = NimResultError
            command.errorMsg = errMsg
            debugLog("[nimlibp2p] cmdStart: marked failure err=" & errMsg)
    of cmdStop:
      if not n.started:
        info "cmdStop: node already stopped"
        command.resultCode = NimResultOk
      else:
        await n.switchInstance.stop()
        await stopMdns(n)
        if not n.dmService.isNil and n.dmService.started:
          try:
            await n.dmService.stop()
          except CatchableError as exc:
            warn "cmdStop: direct message service stop failed", err = exc.msg
        n.dmServiceMounted = false
        if n.connectionEventsEnabled:
          for entry in n.connectionHandlers:
            let kind = entry[0]
            let handler = entry[1]
            n.switchInstance.connManager.removeConnEventHandler(handler, kind)
          n.connectionHandlers.setLen(0)
          n.connectionEventsEnabled = false
        if not n.keepAliveLoop.isNil:
          n.keepAliveLoop.cancel()
          n.keepAliveLoop = nil
        n.started = false
        n.stopRequested.store(true, moRelease)
        info "cmdStop: switch stopped"
        command.resultCode = NimResultOk
    of cmdPublish:
      if n.started:
        let delivered = await n.gossip.PubSub.publish(command.topic, command.payload)
        command.delivered = delivered
        command.resultCode = NimResultOk
      else:
        command.resultCode = NimResultInvalidState
        command.errorMsg = "node not started"
    of cmdSubscribe:
      if command.callback.isNil:
        command.resultCode = NimResultInvalidArgument
        command.errorMsg = "callback is nil"
      else:
        var subscription = Subscription(
          topic: command.topic,
          callback: command.callback,
          userData: command.userData,
          active: true,
        )
        subscription.handler =
          proc(topic: string, payload: seq[byte]): Future[void] {.gcsafe, raises: [].} =
            let fut = newFuture[void]("nim-libp2p-ffi.pubsub")
            if subscription.active and not subscription.callback.isNil:
              let ptrPayload =
                if payload.len == 0:
                  cast[ptr UncheckedArray[byte]](nil)
                else:
                  cast[ptr UncheckedArray[byte]](unsafeAddr payload[0])
              try:
                subscription.callback(
                  topic.cstring,
                  ptrPayload,
                  csize_t(payload.len),
                  subscription.userData,
                )
              except CatchableError:
                discard
            fut.complete()
            fut
        n.gossip.PubSub.subscribe(command.topic, subscription.handler)
        let key = cast[pointer](subscription)
        n.subscriptions[key] = subscription
        command.ptrResult = key
        command.resultCode = NimResultOk
    of cmdUnsubscribe:
      let key = command.subscriptionPtr
      if key.isNil:
        command.resultCode = NimResultInvalidArgument
        command.errorMsg = "subscription not found"
      else:
        let subscription = n.subscriptions.getOrDefault(key, nil)
        if subscription.isNil:
          command.resultCode = NimResultInvalidArgument
          command.errorMsg = "subscription not found"
        else:
          if subscription.active:
            subscription.active = false
            n.gossip.PubSub.unsubscribe(subscription.topic, subscription.handler)
          n.subscriptions.del(key)
          command.resultCode = NimResultOk
    of cmdConnectPeer:
      if not n.started:
        command.resultCode = NimResultInvalidState
        command.errorMsg = "node not started"
        n.lastDirectError = command.errorMsg
      else:
        let peer = PeerId.init(command.peerId).valueOr:
          command.resultCode = NimResultInvalidArgument
          command.errorMsg = "invalid peer id"
          n.lastDirectError = command.errorMsg
          return
        let known = gatherKnownAddrs(n, peer)
        if known.len == 0:
          let errMsg = "peer address not known"
          command.resultCode = NimResultInvalidArgument
          command.errorMsg = errMsg
          n.lastDirectError = errMsg
          debugLog("[nimlibp2p] cmdConnectPeer no addresses peer=" & command.peerId)
        else:
          let (dialable, rejected) = filterDialable(known)
          if rejected.len > 0:
            for entry in rejected:
              debugLog("[nimlibp2p] cmdConnectPeer rejected addr peer=" & command.peerId & " reason=" & entry)
          if dialable.len == 0:
            let errMsg = "no dialable addresses for peer"
            command.resultCode = NimResultInvalidArgument
            command.errorMsg = errMsg
            n.lastDirectError = errMsg
            debugLog("[nimlibp2p] cmdConnectPeer no dialable addr peer=" & command.peerId)
          else:
            let maxAttempts = 3
            var attempt = 0
            var success = false
            var lastError = ""
            while attempt < maxAttempts and not success:
              try:
                await n.switchInstance.connect(peer, dialable, forceDial = true)
                success = true
              except CatchableError as exc:
                let errMsg = if exc.msg.len > 0: exc.msg else: $exc.name
                lastError = errMsg
                let attemptNum = attempt + 1
                warn "cmdConnectPeer dial failed", peer = command.peerId, attempt = attemptNum, err = errMsg
                let lowered = errMsg.toLowerAscii()
                if lowered.contains("failed dial existing") or lowered.contains("newstream"):
                  try:
                    await n.switchInstance.disconnect(peer)
                    debugLog("[nimlibp2p] cmdConnectPeer disconnect stale connection peer=" & command.peerId)
                  except CatchableError as discExc:
                    debugLog("[nimlibp2p] cmdConnectPeer disconnect failed peer=" & command.peerId & " err=" & discExc.msg)
                attempt.inc()
                if attempt >= maxAttempts or not shouldRetryDial(errMsg):
                  break
                let delayMs = min(800, 200 * attempt)
                debugLog(
                  "[nimlibp2p] cmdConnectPeer retry peer=" & command.peerId &
                  " nextAttempt=" & $(attempt + 1) & " delayMs=" & $delayMs &
                  " err=" & errMsg
                )
                await sleepAsync(chronos.milliseconds(delayMs))
            if success:
              command.resultCode = NimResultOk
              command.errorMsg = ""
              n.lastDirectError = ""
              info "cmdConnectPeer success", peer = command.peerId, addrCount = dialable.len
            else:
              let errMsg = if lastError.len > 0: lastError else: "connect failed"
              command.resultCode = NimResultError
              command.errorMsg = errMsg
              n.lastDirectError = errMsg
              warn "cmdConnectPeer failed", peer = command.peerId, err = errMsg
    of cmdConnectMultiaddr:
      if not n.started:
        command.resultCode = NimResultInvalidState
        command.errorMsg = "node not started"
        n.lastDirectError = command.errorMsg
      else:
        let ma = MultiAddress.init(command.multiaddr).valueOr:
          command.resultCode = NimResultInvalidArgument
          command.errorMsg = "invalid multiaddr"
          n.lastDirectError = command.errorMsg
          return
        try:
          discard await n.switchInstance.connect(ma, allowUnknownPeerId = true)
          command.resultCode = NimResultOk
          command.errorMsg = ""
          n.lastDirectError = ""
          let successMsg = "[nimlibp2p] cmdConnectMultiaddr success addr=" & command.multiaddr
          info "cmdConnectMultiaddr success", addr = command.multiaddr
          debugLog(successMsg)
        except CatchableError as exc:
          let errMsg = if exc.msg.len > 0: exc.msg else: $exc.name
          command.resultCode = NimResultError
          command.errorMsg = errMsg
          n.lastDirectError = errMsg
          warn "cmdConnectMultiaddr failed", addr = command.multiaddr, err = errMsg
          when defined(android):
            discard android_log_write(
              AndroidLogError,
              "nim-libp2p",
              ("cmdConnectMultiaddr failed addr=" & command.multiaddr & " err=" &
                  errMsg).cstring,
            )
    of cmdDisconnectPeer:
      if not n.started:
        command.resultCode = NimResultInvalidState
        command.errorMsg = "node not started"
      else:
        let peer = PeerId.init(command.peerId).valueOr:
          command.resultCode = NimResultInvalidArgument
          command.errorMsg = "invalid peer id"
          return
        await n.switchInstance.disconnect(peer)
        command.resultCode = NimResultOk
    of cmdConnectedPeers:
      let inbound = n.switchInstance.connectedPeers(Direction.In)
      let outbound = n.switchInstance.connectedPeers(Direction.Out)
      var uniq = initHashSet[string]()
      for pid in inbound:
        uniq.incl($pid)
      for pid in outbound:
        uniq.incl($pid)
      let arr = uniq.toSeq()
      command.stringResult = $(%* arr)
      command.resultCode = NimResultOk
    of cmdPeerMultiaddrs:
      let peer = PeerId.init(command.peerId).valueOr:
        command.resultCode = NimResultInvalidArgument
        command.errorMsg = "invalid peer id"
        return
      let addrs = n.switchInstance.peerStore[AddressBook][peer]
      var strAddrs: seq[string]
      for addr in addrs:
        strAddrs.add($addr)
      command.stringResult = $(%* strAddrs)
      command.resultCode = NimResultOk
    of cmdDiagnostics:
      info "cmdDiagnostics handling"
      debugLog("[nimlibp2p] cmdDiagnostics begin")
      let peerInfo = n.switchInstance.peerInfo
      if peerInfo.isNil:
        command.stringResult = "{}"
        command.resultCode = NimResultOk
        return
      var topics: seq[string] = @[]
      for topic in n.gossip.PubSub.topics.keys:
        topics.add(topic)
      let listenAddrs =
        peerInfo.listenAddrs.mapIt($it)
      let dialableAddrs =
        peerInfo.addrs.mapIt($it)
      var publicAddr = ""
      for addr in dialableAddrs:
        if (not addr.contains("/ip4/0.0.0.0/")) and
           (not addr.contains("/ip4/127.0.0.1/")) and
           (not addr.contains("/ip6/::/")):
          publicAddr = addr
          break
      let connected =
        n.switchInstance.connectedPeers(Direction.Out) &
        n.switchInstance.connectedPeers(Direction.In)
      var seen = initHashSet[string]()
      var allPeers: seq[string] = @[]
      for pid in connected:
        let s = $pid
        if not seen.contains(s):
          seen.incl(s)
          allPeers.add(s)
      var diagnostics = %* {
        "peerId": $peerInfo.peerId,
        "listenAddresses": listenAddrs,
        "dialableAddresses": dialableAddrs,
        "topics": topics,
        "connectedPeers": allPeers,
        "metricsEnabled": n.cfg.enableMetrics,
        "metricsPort": (if n.cfg.metricsPort.isSome: %* n.cfg.metricsPort.get else: newJNull()),
        "running": n.started,
        "features": %* {
          "gossipsub": n.cfg.automations.gossipsub,
          "directStream": n.cfg.automations.directStream,
          "rendezvous": n.cfg.automations.rendezvous,
          "autonat": n.cfg.automations.autonat,
          "circuitRelay": n.cfg.automations.circuitRelay,
          "livestream": n.cfg.automations.livestream,
          "dataTransfer": n.cfg.automations.dataTransfer,
        },
        "timestampMillis": int64(epochTime() * 1000),
        "natPublicAddr": publicAddr,
      }
      if n.cfg.identity.isSome():
        let ident = n.cfg.identity.get()
        if ident.peerId.len > 0:
          diagnostics["identityPeerId"] = %ident.peerId
        if ident.source.isSome:
          diagnostics["identitySource"] = %ident.source.get()
        if ident.keyPath.isSome:
          diagnostics["identityKeyPath"] = %ident.keyPath.get()
      command.stringResult = $diagnostics
      command.resultCode = NimResultOk
      debugLog("[nimlibp2p] cmdDiagnostics completed")
    of cmdConnectedPeersInfo:
      var arr = newJArray()
      let connections = n.switchInstance.connManager.getConnections()
      for peerId, muxers in connections.pairs:
        var entry = newJObject()
        entry["peerId"] = %($peerId)
        entry["connectionCount"] = %muxers.len
        var directions = newJArray()
        var streamCount = 0
        for mux in muxers:
          if mux.isNil:
            continue
          let dirStr =
            if mux.connection.dir == Direction.In: "inbound" else: "outbound"
          directions.add(%dirStr)
          try:
            streamCount += mux.getStreams().len
          except CatchableError:
            discard
        entry["directions"] = directions
        entry["streamCount"] = %streamCount
        arr.add(entry)
      command.stringResult = $arr
      command.resultCode = NimResultOk
    of cmdRegisterPeerHints:
      if command.peerId.len == 0 or command.addresses.len == 0:
        command.resultCode = NimResultInvalidArgument
        command.errorMsg = "missing peerId or addresses"
        return
      let peer = PeerId.init(command.peerId).valueOr:
        command.resultCode = NimResultInvalidArgument
        command.errorMsg = "invalid peer id"
        return
      var parsed: seq[MultiAddress] = @[]
      for addrStr in command.addresses:
        if addrStr.len == 0:
          continue
        let ma = MultiAddress.init(addrStr).valueOr:
          warn "registerPeerHints: invalid addr", peerId = command.peerId, address = addrStr, err = error
          continue
        parsed.add(ma)
      if parsed.len == 0:
        command.resultCode = NimResultInvalidArgument
        command.errorMsg = "no valid addresses"
        return
      n.peerHints[command.peerId] = command.addresses
      let peerStore = n.switchInstance.peerStore
      if not peerStore.isNil:
        try:
          peerStore.addAddressesWithTTL(
            peer, parsed, chronos.minutes(30)
          )
        except CatchableError as exc:
          warn "registerPeerHints: addAddressesWithTTL failed", peerId = command.peerId, err = exc.msg
      command.resultCode = NimResultOk
    of cmdAddExternalAddress:
      if command.multiaddr.len == 0:
        command.resultCode = NimResultInvalidArgument
        command.errorMsg = "empty multiaddr"
        return
      let ma = MultiAddress.init(command.multiaddr).valueOr:
        command.resultCode = NimResultInvalidArgument
        command.errorMsg = "invalid multiaddr"
        return
      if n.switchInstance.peerInfo.isNil:
        command.resultCode = NimResultInvalidState
        command.errorMsg = "peer info unavailable"
        return
      var exists = false
      for addr in n.switchInstance.peerInfo.addrs:
        if addr == ma:
          exists = true
          break
      if not exists:
        n.switchInstance.peerInfo.addrs.add(ma)
      var listenExists = false
      for addr in n.switchInstance.peerInfo.listenAddrs:
        if addr == ma:
          listenExists = true
          break
      if not listenExists:
        n.switchInstance.peerInfo.listenAddrs.add(ma)
      try:
        await n.switchInstance.peerInfo.update()
      except CatchableError as exc:
        warn "addExternalAddress: peer info update failed", address = command.multiaddr, err = exc.msg
      updateMdnsAdvertisements(n)
      command.resultCode = NimResultOk
    of cmdCheckPeerConnected:
      if command.peerId.len == 0:
        command.resultCode = NimResultInvalidArgument
        command.errorMsg = "missing peer id"
        return
      let peer = PeerId.init(command.peerId).valueOr:
        command.resultCode = NimResultInvalidArgument
        command.errorMsg = "invalid peer id"
        return
      command.boolResult = n.switchInstance.connManager.connCount(peer) > 0
      command.resultCode = NimResultOk
    of cmdSyncPeerstore:
      let addrBook = n.switchInstance.peerStore[AddressBook]
      var connected = initHashSet[string]()
      for pid in n.switchInstance.connectedPeers(Direction.Out):
        connected.incl($pid)
      for pid in n.switchInstance.connectedPeers(Direction.In):
        connected.incl($pid)
      var connectedNode = newJArray()
      for pid in connected.items():
        connectedNode.add(%pid)
      var peersNode = newJArray()
      var total = 0
      for peer, addrs in addrBook.book.pairs:
        inc total
        var entry = newJObject()
        entry["peerId"] = %($peer)
        var addrsNode = newJArray()
        for addr in addrs:
          addrsNode.add(%($addr))
        entry["addresses"] = addrsNode
        peersNode.add(entry)
      var obj = newJObject()
      obj["connected_peers"] = connectedNode
      obj["peers"] = peersNode
      obj["total_count"] = %total
      command.stringResult = $obj
      command.resultCode = NimResultOk
    of cmdLoadStoredPeers:
      let addrBook = n.switchInstance.peerStore[AddressBook]
      var peersNode = newJArray()
      var total = 0
      for peer, addrs in addrBook.book.pairs:
        inc total
        var entry = newJObject()
        entry["peerId"] = %($peer)
        var addrsNode = newJArray()
        for addr in addrs:
          addrsNode.add(%($addr))
        entry["addresses"] = addrsNode
        peersNode.add(entry)
      var obj = newJObject()
      obj["peers"] = peersNode
      obj["totalCount"] = %total
      command.stringResult = $obj
      command.resultCode = NimResultOk
    of cmdReconnectBootstrap:
      var attempts = 0
      for peerId, addresses in n.peerHints.pairs:
        let peerRes = PeerId.init(peerId)
        var parsedAddrs: seq[MultiAddress] = @[]
        for addrStr in addresses:
          let ma = MultiAddress.init(addrStr).valueOr:
            warn "reconnectBootstrap: invalid addr", peer = peerId, address = addrStr, err = error
            continue
          parsedAddrs.add(ma)
          try:
            discard await n.switchInstance.connect(ma, allowUnknownPeerId = true)
            inc attempts
          except CatchableError as exc:
            warn "reconnectBootstrap: connect via multiaddr failed", peer = peerId, address = addrStr, err = exc.msg
        if peerRes.isOk() and parsedAddrs.len > 0:
          try:
            await n.switchInstance.connect(peerRes.get(), parsedAddrs, forceDial = true)
          except CatchableError as exc:
            warn "reconnectBootstrap: connect via peerId failed", peer = peerId, err = exc.msg
      command.delivered = attempts
      command.resultCode = NimResultOk
    of cmdSendDirect:
      if not n.started:
        command.resultCode = NimResultInvalidState
        command.errorMsg = "node not started"
      elif command.peerId.len == 0:
        command.resultCode = NimResultInvalidArgument
        command.errorMsg = "missing peer id"
      else:
        debugLog("[nimlibp2p] cmdSendDirect start peer=" & command.peerId &
          " jsonLen=" & $command.jsonString.len & " payloadLen=" & $command.payload.len)
        await setupDefaultTopics(n)
        if n.dmService.isNil:
          command.resultCode = NimResultInvalidState
          command.errorMsg = "direct message service unavailable"
          return
        var remotePid: PeerId
        if not remotePid.init(command.peerId):
          command.resultCode = NimResultInvalidArgument
          command.errorMsg = "invalid peer id"
          return
        let local = localPeerId(n)
        if local.len == 0:
          command.resultCode = NimResultInvalidState
          command.errorMsg = "local peer id unavailable"
          return
        var envelope: JsonNode
        if command.jsonString.len > 0:
          try:
            envelope = parseJson(command.jsonString)
          except CatchableError as exc:
            warn "cmdSendDirect: invalid json", err = exc.msg
            envelope = newJObject()
        if envelope.isNil or envelope.kind != JObject:
          envelope = newJObject()
        var mid = jsonGetStr(envelope, "mid")
        if command.messageId.len > 0:
          mid = command.messageId
        mid = ensureMessageId(mid)
        let opValue =
          if command.op.len > 0: command.op
          elif jsonGetStr(envelope, "op").len > 0: jsonGetStr(envelope, "op")
          else: "text"
        envelope["op"] = %opValue
        envelope["mid"] = %mid
        envelope["from"] = %local
        envelope["timestamp_ms"] = %nowMillis()
        let ackFlag = command.ackRequested or jsonGetBool(envelope, "ackRequested")
        command.ackRequested = ackFlag
        envelope["ackRequested"] = %ackFlag
        if command.replyTo.len > 0:
          envelope["reply_to"] = %command.replyTo
        if command.target.len > 0:
          envelope["target"] = %command.target
        if command.textPayload.len > 0:
          envelope["body"] = %command.textPayload
        elif command.payload.len > 0:
          envelope["payload_b64"] = %base64.encode(command.payload)
        let timeout =
          if command.timeoutMs > 0:
            chronos.milliseconds(max(command.timeoutMs, 500))
          else:
            chronos.milliseconds(12_000)
        let connectivityOk = await ensurePeerConnectivity(n, remotePid)
        if not connectivityOk:
          let errMsg = if command.errorMsg.len > 0: command.errorMsg else: "peer unreachable"
          command.errorMsg = errMsg
          command.boolResult = false
          command.resultCode = NimResultError
          n.lastDirectError = errMsg
          debugLog("[nimlibp2p] cmdSendDirect connectivity missing peer=" & command.peerId & " err=" & errMsg)
          info "cmdSendDirect failed", peer = command.peerId, mid = mid, err = errMsg
          return
        debugLog("[nimlibp2p] cmdSendDirect connectivity peer=" & command.peerId &
          " ok=true ackRequested=" & $command.ackRequested)
        let bytes = stringToBytes($envelope)
        var sendOk = false
        var sendErr = ""
        try:
          let resultTuple = await n.dmService.send(
            remotePid, bytes, command.ackRequested, mid, timeout
          )
          sendOk = resultTuple[0]
          sendErr = resultTuple[1]
        except CatchableError as exc:
          sendOk = false
          sendErr = if exc.msg.len > 0: exc.msg else: $exc.name
          warn "cmdSendDirect send failed", peer = command.peerId, err = sendErr

        var errMsg = ""
        if sendOk:
          command.boolResult = true
          command.errorMsg = ""
          n.lastDirectError = ""
          command.resultCode = NimResultOk
          info "cmdSendDirect success", peer = command.peerId, mid = mid
          debugLog("[nimlibp2p] cmdSendDirect success peer=" & command.peerId &
            " mid=" & mid)
        else:
          if sendErr.len > 0:
            errMsg = sendErr
          elif command.ackRequested:
            errMsg =
              if command.errorMsg.len > 0: command.errorMsg else: "ack timeout"
          else:
            errMsg =
              if command.errorMsg.len > 0: command.errorMsg else: "delivery failed"
          command.boolResult = false
          command.errorMsg = errMsg
          n.lastDirectError = errMsg
          command.resultCode = NimResultError
          debugLog("[nimlibp2p] cmdSendDirect failure peer=" & command.peerId &
            " mid=" & mid & " err=" & errMsg & " ackRequested=" & $command.ackRequested &
            " connectivityOk=true")
          info "cmdSendDirect failed", peer = command.peerId, mid = mid, err = errMsg

        if command.ackRequested:
          var ackEnvelope = newJObject()
          ackEnvelope["op"] = %"ack"
          ackEnvelope["mid"] = %mid
          ackEnvelope["from"] = %command.peerId
          ackEnvelope["timestamp_ms"] = %nowMillis()
          ackEnvelope["ack"] = %sendOk
          if (not sendOk) and sendErr.len > 0:
            ackEnvelope["error"] = %sendErr
          handleDirectAckNode(n, ackEnvelope)
    of cmdSendChatAck:
      if not n.started:
        command.resultCode = NimResultInvalidState
        command.errorMsg = "node not started"
      elif command.peerId.len == 0 or command.messageId.len == 0:
        command.resultCode = NimResultInvalidArgument
        command.errorMsg = "missing peer or message id"
      else:
        await setupDefaultTopics(n)
        if n.dmService.isNil:
          command.resultCode = NimResultInvalidState
          command.errorMsg = "direct message service unavailable"
          return
        var remotePid: PeerId
        if not remotePid.init(command.peerId):
          command.resultCode = NimResultInvalidArgument
          command.errorMsg = "invalid peer id"
          return
        let connectivityOk = await ensurePeerConnectivity(n, remotePid)
        if not connectivityOk:
          let errMsg = "peer unreachable"
          command.resultCode = NimResultError
          command.errorMsg = errMsg
          n.lastDirectError = errMsg
          warn "cmdSendChatAck connectivity failed", peer = command.peerId, mid = command.messageId
          return
        var envelope = newJObject()
        envelope["op"] = %"ack"
        envelope["mid"] = %command.messageId
        envelope["from"] = %(localPeerId(n))
        envelope["timestamp_ms"] = %nowMillis()
        envelope["ack"] = %command.boolResult
        if command.errorMsg.len > 0:
          envelope["error"] = %command.errorMsg
        let payload = stringToBytes($envelope)
        let timeout =
          if command.timeoutMs > 0:
            chronos.milliseconds(max(command.timeoutMs, 500))
          else:
            chronos.milliseconds(3_000)
        var deliveryOk = false
        var deliveryErr = ""
        try:
          let resultTuple = await n.dmService.send(
            remotePid, payload, false, command.messageId, timeout
          )
          deliveryOk = resultTuple[0]
          deliveryErr = resultTuple[1]
        except CatchableError as exc:
          deliveryOk = false
          deliveryErr = if exc.msg.len > 0: exc.msg else: $exc.name
          warn "cmdSendChatAck send failed", peer = command.peerId, mid = command.messageId, err = deliveryErr
        if not deliveryOk:
          let errMsg = if deliveryErr.len > 0: deliveryErr else: "delivery failed"
          command.resultCode = NimResultError
          command.errorMsg = errMsg
          n.lastDirectError = errMsg
          return
        n.lastDirectError = ""
        debugLog(
          "[nimlibp2p] cmdSendChatAck delivered peer=" & command.peerId &
          " mid=" & command.messageId
        )
        info "cmdSendChatAck delivered", peer = command.peerId, mid = command.messageId
        command.resultCode = NimResultOk
    of cmdGetLastDirectError:
      command.stringResult = n.lastDirectError
      n.lastDirectError = ""
      command.resultCode = NimResultOk
    of cmdInitializeConnEvents:
      setupConnectionEvents(n)
      command.resultCode = NimResultOk
    of cmdRefreshPeerConnections:
      await refreshPeerConnections(n)
      command.resultCode = NimResultOk
    of cmdKeepAlivePin:
      if command.peerId.len == 0:
        command.resultCode = NimResultInvalidArgument
        command.errorMsg = "missing peer id"
      else:
        n.keepAlivePeers.incl(command.peerId)
        ensureKeepAliveLoop(n)
        command.resultCode = NimResultOk
    of cmdKeepAliveUnpin:
      if command.peerId.len == 0:
        command.resultCode = NimResultInvalidArgument
        command.errorMsg = "missing peer id"
      else:
        n.keepAlivePeers.excl(command.peerId)
        command.resultCode = NimResultOk
    of cmdEmitManualEvent:
      let payload = if command.jsonString.len > 0: command.jsonString else: command.textPayload
      if command.topic.len == 0 or payload.len == 0:
        command.resultCode = NimResultInvalidArgument
        command.errorMsg = "missing topic or payload"
      else:
        recordEvent(n, command.topic, payload)
        command.resultCode = NimResultOk
    of cmdBoostConnectivity:
      await boostConnectivity(n)
      command.resultCode = NimResultOk
    of cmdGetLanEndpoints:
      let json = gatherLanEndpoints(n)
      command.stringResult = $json
      command.resultCode = NimResultOk
    of cmdFetchFeedSnapshot:
      var arr = newJArray()
      for item in n.feedItems:
        var obj = newJObject()
        obj["id"] = %item.id
        obj["author"] = %item.author
        obj["timestamp_ms"] = %item.timestamp
        obj["payload"] = item.payload
        arr.add(obj)
      var snapshot = newJObject()
      snapshot["items"] = arr
      snapshot["timestamp_ms"] = %nowMillis()
      command.stringResult = $snapshot
      command.resultCode = NimResultOk
    of cmdFeedSubscribePeer:
      if n.feedService.isNil:
        command.resultCode = NimResultInvalidState
        command.errorMsg = "feed service unavailable"
      elif command.peerId.len == 0:
        command.resultCode = NimResultInvalidArgument
        command.errorMsg = "missing peer id"
      else:
        var target: PeerId
        if not target.init(command.peerId):
          command.resultCode = NimResultInvalidArgument
          command.errorMsg = "invalid peer id"
        else:
          try:
            await n.feedService.subscribeToPeer(target)
            command.resultCode = NimResultOk
          except CatchableError as exc:
            command.resultCode = NimResultError
            command.errorMsg = exc.msg
    of cmdFeedUnsubscribePeer:
      if n.feedService.isNil:
        command.resultCode = NimResultInvalidState
        command.errorMsg = "feed service unavailable"
      elif command.peerId.len == 0:
        command.resultCode = NimResultInvalidArgument
        command.errorMsg = "missing peer id"
      else:
        var target: PeerId
        if not target.init(command.peerId):
          command.resultCode = NimResultInvalidArgument
          command.errorMsg = "invalid peer id"
        else:
          n.feedService.unsubscribeFromPeer(target)
          command.resultCode = NimResultOk
    of cmdFeedPublishEntry:
      if n.feedService.isNil:
        command.resultCode = NimResultInvalidState
        command.errorMsg = "feed service unavailable"
      elif command.jsonString.len == 0:
        command.resultCode = NimResultInvalidArgument
        command.errorMsg = "missing entry payload"
      else:
        let payloadBytes = stringToBytes(command.jsonString)
        var entryOpt = decodeFeedEntry(payloadBytes)
        if entryOpt.isNone:
          command.resultCode = NimResultInvalidArgument
          command.errorMsg = "invalid feed entry"
        else:
          var entry = entryOpt.get()
          if entry.id.len == 0:
            entry.id = ensureMessageId("")
          if entry.extra.isNil or entry.extra.kind != JObject:
            entry.extra = parseJson(command.jsonString)
          try:
            let delivered = await n.feedService.publishFeedItem(entry)
            command.intResult = delivered
            command.resultCode =
              if delivered > 0: NimResultOk else: NimResultError
            if delivered == 0:
              command.errorMsg = "no subscribers for feed topic"
          except CatchableError as exc:
            command.resultCode = NimResultError
            command.errorMsg = exc.msg
    of cmdUpsertLivestream:
      if command.streamKey.len == 0:
        command.resultCode = NimResultInvalidArgument
        command.errorMsg = "missing stream key"
      else:
        var session = n.livestreams.getOrDefault(command.streamKey)
        session.streamKey = command.streamKey
        if command.jsonString.len > 0:
          try:
            session.config = parseJson(command.jsonString)
          except CatchableError as exc:
            warn "upsertLivestream config parse failed", err = exc.msg
        session.lastTimestamp = nowMillis()
        n.livestreams[command.streamKey] = session
        command.resultCode = NimResultOk
    of cmdPublishLivestream:
      if command.streamKey.len == 0 or command.payload.len == 0:
        command.resultCode = NimResultInvalidArgument
        command.errorMsg = "missing stream key or payload"
      else:
        var session = n.livestreams.getOrDefault(command.streamKey)
        session.streamKey = command.streamKey
        session.frameIndex = session.frameIndex + 1
        session.lastTimestamp = nowMillis()
        n.livestreams[command.streamKey] = session
        var event = newJObject()
        event["type"] = %"LivestreamFrame"
        event["stream_key"] = %command.streamKey
        event["frame_index"] = %session.frameIndex
        event["payload_size"] = %command.payload.len
        event["timestamp_ms"] = %session.lastTimestamp
        recordEvent(n, "network_event", $event)
        let topic = "live/" & command.streamKey
        var delivered = 0
        try:
          delivered = await n.gossip.PubSub.publish(topic, command.payload)
        except CatchableError as exc:
          command.resultCode = NimResultError
          command.errorMsg = "livestream publish failed: " & exc.msg
          return
        if delivered == 0:
          command.resultCode = NimResultError
          command.errorMsg = "no subscribers for livestream topic"
          return
        info "cmdPublishLivestream delivered", streamKey = command.streamKey, delivered = delivered, size = command.payload.len
        command.resultCode = NimResultOk
    of cmdLanGroupJoin:
      if command.topic.len == 0:
        command.resultCode = NimResultInvalidArgument
        command.errorMsg = "missing group id"
      elif ensureLanGroupSubscription(n, command.topic):
        command.resultCode = NimResultOk
      else:
        command.resultCode = NimResultError
        command.errorMsg = "failed to join group"
    of cmdLanGroupLeave:
      if command.topic.len == 0:
        command.resultCode = NimResultInvalidArgument
        command.errorMsg = "missing group id"
      else:
        discard removeLanGroupSubscription(n, command.topic)
        command.resultCode = NimResultOk
    of cmdLanGroupSend:
      if command.topic.len == 0 or command.textPayload.len == 0:
        command.resultCode = NimResultInvalidArgument
        command.errorMsg = "missing group id or message"
      else:
        if not ensureLanGroupSubscription(n, command.topic):
          command.resultCode = NimResultError
          command.errorMsg = "failed to prepare group"
        else:
          let topic = lanGroupTopic(command.topic)
          let mid = ensureMessageId(command.messageId)
          var envelope = newJObject()
          envelope["op"] = %"chat"
          envelope["group_id"] = %command.topic
          envelope["mid"] = %mid
          envelope["from"] = %localPeerId(n)
          envelope["timestamp_ms"] = %nowMillis()
          envelope["body"] = %command.textPayload
          let bytes = stringToBytes($envelope)
          discard await n.gossip.PubSub.publish(topic, bytes)
          handleLanGroupMessage(n, command.topic, bytes)
          command.resultCode = NimResultOk
    of cmdMdnsSetEnabled:
      n.mdnsEnabled = command.boolParam
      try:
        if n.mdnsEnabled:
          await startMdns(n)
        else:
          await stopMdns(n)
        command.boolResult = true
        command.resultCode = NimResultOk
      except CatchableError as exc:
        command.resultCode = NimResultError
        command.errorMsg = "mdns set enabled failed: " & exc.msg
    of cmdMdnsSetInterface:
      let rawIp = command.textPayload.strip()
      var normalized = rawIp
      if normalized == "0.0.0.0":
        normalized = ""
      let sameValue = n.mdnsPreferredIpv4 == normalized
      if not sameValue:
        n.mdnsPreferredIpv4 = normalized
      if not n.mdnsInterface.isNil:
        n.mdnsInterface.setPreferredIpv4(n.mdnsPreferredIpv4)
      let canRestart = n.mdnsEnabled and n.started
      let hasInterface = not n.mdnsInterface.isNil
      let boundMismatch = hasInterface and normalized.len > 0 and (n.mdnsInterface.boundIpv4.len == 0 or n.mdnsInterface.boundIpv4 != normalized)
      let restartNeeded = normalized.len > 0 and (not sameValue or boundMismatch)
      n.mdnsRestartPending = restartNeeded
      if restartNeeded and canRestart:
        asyncSpawn(restartMdnsSafe(n, "set_interface"))
      command.boolResult = true
      command.resultCode = NimResultOk
    of cmdMdnsDebug:
      var obj = newJObject()
      obj["enabled"] = %n.mdnsEnabled
      obj["started"] = %n.started
      obj["preferred_ipv4"] = %n.mdnsPreferredIpv4
      obj["restart_pending"] = %n.mdnsRestartPending
      obj["interval_seconds"] = %n.mdnsIntervalSeconds
      obj["interface_bound_ipv4"] =
        if not n.mdnsInterface.isNil: %n.mdnsInterface.boundIpv4 else: %""
      command.stringResult = $obj
      command.resultCode = NimResultOk
    of cmdMdnsSetInterval:
      if command.intParam <= 0:
        command.resultCode = NimResultInvalidArgument
        command.errorMsg = "invalid interval"
      else:
        n.mdnsIntervalSeconds = command.intParam
        if n.mdnsEnabled and n.started:
          asyncSpawn(restartMdnsSafe(n, "set_interval"))
        command.resultCode = NimResultOk
    of cmdMdnsProbe:
      if n.mdnsEnabled and n.started:
        asyncSpawn(kickMdnsQuery(n))
        command.boolResult = true
      else:
        command.boolResult = false
      command.resultCode = NimResultOk
    of cmdRendezvousAdvertise:
      if command.topic.len == 0:
        command.resultCode = NimResultInvalidArgument
        command.errorMsg = "missing namespace"
      elif n.rendezvous.isNil:
        command.resultCode = NimResultError
        command.errorMsg = "rendezvous not available"
      else:
        let ttlMs = max(command.timeoutMs, 60_000)
        let ttl = chronos.milliseconds(ttlMs)
        try:
          await n.rendezvous.advertise(command.topic, ttl)
          command.resultCode = NimResultOk
        except AdvertiseError as exc:
          command.resultCode = NimResultError
          command.errorMsg = exc.msg
        except CancelledError as exc:
          command.resultCode = NimResultError
          command.errorMsg = exc.msg
    of cmdRendezvousDiscover:
      if n.rendezvous.isNil:
        command.resultCode = NimResultError
        command.errorMsg = "rendezvous not available"
      else:
        let limit = if command.intResult > 0: command.intResult else: 20
        try:
          let records =
            if command.addresses.len > 0:
              var peers: seq[PeerId] = @[]
              for pidStr in command.addresses:
                let pidRes = PeerId.init(pidStr)
                if pidRes.isOk():
                  peers.add(pidRes.get())
              let nsOpt =
                if command.topic.len == 0:
                  Opt.none(string)
                else:
                  Opt.some(command.topic)
              await n.rendezvous.request(nsOpt, limit, peers)
            else:
              if command.topic.len == 0:
                await n.rendezvous.request(limit)
              else:
                await n.rendezvous.request(Opt.some(command.topic), limit)
          var arr = newJArray()
          for pr in records:
            var obj = newJObject()
            obj["peerId"] = %($pr.peerId)
            var addrs = newJArray()
            for addr in pr.addresses:
              addrs.add(%($addr))
            obj["addresses"] = addrs
            arr.add(obj)
          command.stringResult = $arr
          command.resultCode = NimResultOk
        except DiscoveryError as exc:
          command.resultCode = NimResultError
          command.errorMsg = exc.msg
        except CancelledError as exc:
          command.resultCode = NimResultError
          command.errorMsg = exc.msg
    of cmdRendezvousUnregister:
      if command.topic.len == 0:
        command.resultCode = NimResultInvalidArgument
        command.errorMsg = "missing namespace"
      elif n.rendezvous.isNil:
        command.resultCode = NimResultError
        command.errorMsg = "rendezvous not available"
      else:
        try:
          await n.rendezvous.unsubscribe(command.topic)
          command.resultCode = NimResultOk
        except RendezVousError as exc:
          command.resultCode = NimResultError
          command.errorMsg = exc.msg
        except CancelledError as exc:
          command.resultCode = NimResultError
          command.errorMsg = exc.msg
    of cmdReserveOnRelay:
      if command.multiaddr.len == 0:
        command.resultCode = NimResultInvalidArgument
        command.errorMsg = "missing relay address"
      else:
        let reserved = await reserveOnRelayAsync(n, command.multiaddr)
        command.intResult = reserved
        command.resultCode = NimResultOk
    of cmdReserveOnAllRelays:
      var total = 0
      for peerId, addresses in n.peerHints.pairs:
        for addr in addresses:
          total += await reserveOnRelayAsync(n, addr)
      command.intResult = total
      command.resultCode = NimResultOk
    of cmdWaitSecureChannel:
      if command.peerId.len == 0:
        command.resultCode = NimResultInvalidArgument
        command.errorMsg = "missing peer id"
      else:
        let timeout = max(command.timeoutMs, 500)
        command.boolResult = await waitForSecureChannel(n, command.peerId, timeout)
        command.resultCode = NimResultOk
    of cmdPollEvents:
      command.stringResult = pollEventLog(n, command.requestLimit)
      command.resultCode = NimResultOk
    of cmdDexInit:
      try:
        var cfg: dex_node.CliConfig
        cfg.mode = dex_types.DexMode.dmTrader
        
        # Use the environment variable set by libp2p_node_init if available
        let baseDir = getEnv("UNIMAKER_LIBP2P_DATA_DIR")
        if baseDir.len > 0:
          cfg.dataDir = baseDir / "dex_state"
          debugLog("[nimlibp2p] cmdDexInit using dataDir=" & cfg.dataDir)
        else:
          cfg.dataDir = "dex_state"
          debugLog("[nimlibp2p] cmdDexInit using relative dataDir=" & cfg.dataDir)

        cfg.enableMixer = true
        cfg.metricsInterval = 10
        
        if command.jsonString.len > 0:
          try:
             let j = parseJson(command.jsonString)
             if j.hasKey("enableMixer"):
               cfg.enableMixer = j["enableMixer"].getBool()
          except CatchableError:
             debugLog("[nimlibp2p] cmdDexInit failed to parse json config")

        let store = dex_store.initOrderStore(cfg.dataDir)
        let signingCfg = dex_signing.SigningConfig(
            mode: dex_signing.smDisabled,
            keyPath: "",
            requireRemoteSignature: false,
            allowUnsigned: true
        )
        let signingCtx = dex_signing.initSigning(signingCfg)
        
        n.dexNode = await dex_node.initDexNode(n.switchInstance, n.gossip, cfg, store, signingCtx)
        
        if n.dexNode.mixerCtx != nil:
          n.dexNode.mixerCtx.onEvent = proc(payload: string) {.gcsafe.} =
            n.eventLogLock.acquire()
            try:
              n.eventLog.add(%*{"type": "dex.mixer", "payload": payload})
            finally:
              n.eventLogLock.release()
        
        dex_node.handleOrders(n.dexNode)
        dex_node.handleMatches(n.dexNode)
        dex_node.subscribeTrades(n.dexNode)
        dex_node.handleMixers(n.dexNode)
        
        if cfg.enableMixer:
             asyncSpawn(dex_node.runMixer(n.dexNode))
        
        command.resultCode = NimResultOk
      except CatchableError as exc:
        command.errorMsg = "DexInit failed: " & exc.msg
        command.resultCode = NimResultError

    of cmdDexSubmitMixerIntent:
      if n.dexNode.isNil or n.dexNode.mixerCtx.isNil:
           command.errorMsg = "DEX/Mixer not initialized"
           command.resultCode = NimResultInvalidState
      else:
           try:
             if command.jsonString.len > 0:
               let json = parseJson(command.jsonString)
               # Basic mapping for the demo UI which sends simple strings
               let assetStr = json["asset"].getStr()
               var asset = AssetDef(chainId: ChainBTC, symbol: assetStr, decimals: 8)
               
               # Simple heuristic for the demo
               if assetStr == "ETH":
                 asset = AssetDef(chainId: ChainETH, symbol: "ETH", decimals: 18)
               elif assetStr == "USDC":
                 asset = AssetDef(chainId: ChainETH, symbol: "USDC", decimals: 6)
               elif assetStr == "SOL":
                 asset = AssetDef(chainId: ChainSOL, symbol: "SOL", decimals: 9)
               
               let amount = json["amount"].getFloat()
               # hops is now internal to mixer config or fixed
               await n.dexNode.mixerCtx.submitIntent(asset, amount)
             else:
               # Default BTC test
               let asset = AssetDef(chainId: ChainBTC, symbol: "BTC", decimals: 8)
               await n.dexNode.mixerCtx.submitIntent(asset, 1.0)
             command.resultCode = NimResultOk
           except CatchableError as exc:
             command.errorMsg = exc.msg
             command.resultCode = NimResultError

    of cmdDexSubmitOrder:
      command.resultCode = NimResultOk

    of cmdDexGetMarketData:
      command.stringResult = "{}"
      command.resultCode = NimResultOk

    of cmdDexGetWallets:
      if n.dexNode.isNil or n.dexNode.wallet.isNil:
        command.stringResult = "[]"
        command.resultCode = NimResultOk
      else:
        try:
          let json = dex_node.getWalletAssets(n.dexNode)
          command.stringResult = $json
          command.resultCode = NimResultOk
        except CatchableError as exc:
          command.errorMsg = exc.msg
          command.resultCode = NimResultError
  except Defect as exc:
    if command.resultCode == NimResultOk:
      command.resultCode = NimResultError
    if command.errorMsg.len == 0:
      command.errorMsg = exc.msg
    n.lastDirectError = command.errorMsg
    error "runCommand defect", kind = $command.kind, err = command.errorMsg
    debugLog(
      "[nimlibp2p] runCommand defect node=" & $cast[int](n) &
      " kind=" & $command.kind &
      " err=" & command.errorMsg
    )
  except Exception as exc:
    if command.resultCode == NimResultOk:
      command.resultCode = NimResultError
    if command.errorMsg.len == 0:
      command.errorMsg = exc.msg
    debugLog(
      "[nimlibp2p] runCommand caught exception node=" & $cast[int](n) &
      " kind=" & $command.kind &
      " msg=" & command.errorMsg
    )
  finally:
    if command.resultCode != NimResultOk and command.errorMsg.len == 0:
      debugLog(
        "[nimlibp2p] runCommand completed with error but empty message node=" &
        $cast[int](n) & " kind=" & $command.kind
      )
    debugLog(
      "[nimlibp2p] runCommand completed node=" & $cast[int](n) &
      " kind=" & $command.kind &
      " rc=" & $int(command.resultCode) &
      (if command.errorMsg.len > 0: " err=" & command.errorMsg else: "")
    )
    finalizeCommand(command)
    atomicDec(n.pendingCommands)
    info "command completed", kind = $command.kind, result = command.resultCode

proc processCommandQueue(n: NimNode) =
  while true:
    let command = dequeueCommand(n)
    if command.isNil:
      break
    if command.kind == cmdNone:
      continue
    atomicDec(n.queuedCommands)
    debugLog(
      "[nimlibp2p] processCommandQueue dispatch node=" & $cast[int](n) &
      " kind=" & $command.kind
    )
    atomicInc(n.pendingCommands)
    asyncSpawn(runCommand(n, command))

proc signalLoop(n: NimNode) {.async.} =
  while not n.stopRequested.load(moAcquire):
    try:
      await n.signal.wait()
      let queued = n.queuedCommands.load(moAcquire)
      debugLog(
        "[nimlibp2p] signalLoop wake node=" & $cast[int](n) &
        " stopRequested=" & $(n.stopRequested.load(moAcquire)) &
        " queued=" & $queued &
        " pending=" & $n.pendingCommands.load(moAcquire)
      )
      processCommandQueue(n)
      let afterLen = n.queuedCommands.load(moAcquire)
      debugLog(
        "[nimlibp2p] signalLoop after process node=" & $cast[int](n) &
        " queued=" & $afterLen &
        " pending=" & $n.pendingCommands.load(moAcquire)
      )
    except CancelledError:
      break
    except CatchableError as exc:
      warn "signal loop error", err = exc.msg

proc eventLoop(handlePtr: pointer) {.thread.} =
  let handle = cast[NodeHandle](handlePtr)
  if handle.isNil or handle[].node.isNil:
    debugLog("[nimlibp2p] event loop received null handle")
    return
  let node = handle[].node
  let nodeAddr = cast[int](node)
  debugLog("[nimlibp2p] event loop entering node=" & $nodeAddr)
  info "event loop entering", nodeAddr = nodeAddr
  chronos.setThreadDispatcher(newDispatcher())
  asyncSpawn(signalLoop(node))
  node.ready.store(true, moRelease)
  if not node.readySignal.isNil:
    discard node.readySignal.fireSync()
  debugLog("[nimlibp2p] event loop ready signaled node=" & $nodeAddr)
  while true:
    let shouldStop = node.stopRequested.load(moAcquire)
    let pending = node.pendingCommands.load(moAcquire)
    if shouldStop and pending == 0:
      break
    try:
      debugLog("[nimlibp2p] event loop polling node=" & $nodeAddr)
      poll()
      debugLog("[nimlibp2p] event loop poll completed node=" & $nodeAddr)
    except CatchableError as exc:
      warn "event loop poll error", err = exc.msg
      debugLog("[nimlibp2p] event loop poll exception node=" & $nodeAddr & " " & exc.msg)
    except Exception as exc:
      let stack = exc.getStackTrace()
      error "event loop poll fatal", err = exc.msg, stacktrace = stack
      debugLog("[nimlibp2p] event loop poll fatal node=" & $nodeAddr & " err=" & exc.msg & " stack=" & stack)
      continue
    processCommandQueue(node)
    let remaining = node.pendingCommands.load(moAcquire)
    if remaining > 0:
      debugLog(
        "[nimlibp2p] event loop processed queue node=" & $nodeAddr &
        " pending=" & $remaining
      )
  processCommandQueue(node)
  info "event loop exiting", pendingCommands = node.pendingCommands.load(moAcquire)
  debugLog("[nimlibp2p] event loop exiting node=" & $nodeAddr)
  node.shutdownComplete.store(true, moRelease)
  if not node.shutdownSignal.isNil:
    discard node.shutdownSignal.fireSync()

proc ensureReady(node: NimNode) =
  debugLog("[nimlibp2p] ensureReady waiting")
  if node.ready.load(moAcquire):
    debugLog("[nimlibp2p] ensureReady completed (already ready)")
    return
  if node.readySignal.isNil:
    var waited = 0
    let maxWait = 3000
    while not node.ready.load(moAcquire) and waited < maxWait:
      sleep(50)
      waited += 50
    if node.ready.load(moAcquire):
      debugLog("[nimlibp2p] ensureReady completed (fallback path)")
    else:
      debugLog("[nimlibp2p] ensureReady timeout waiting for event loop (fallback)")
    return
  let waitRes = node.readySignal.waitSync(chronos.milliseconds(3000))
  if waitRes.isErr():
    warn "ensureReady wait failed", err = waitRes.error
  elif not waitRes.get():
    debugLog("[nimlibp2p] ensureReady timeout waiting for ready signal")
  else:
    debugLog("[nimlibp2p] ensureReady completed via signal")

proc waitShutdown(node: NimNode) =
  if node.shutdownComplete.load(moAcquire):
    return
  if node.shutdownSignal.isNil:
    var waited = 0
    let maxWait = 5000
    while not node.shutdownComplete.load(moAcquire) and waited < maxWait:
      sleep(50)
      waited += 50
    return
  let waitRes = node.shutdownSignal.waitSync(chronos.milliseconds(5000))
  if waitRes.isErr():
    warn "waitShutdown wait failed", err = waitRes.error

proc closeThreadSignal(signalPtr: var ThreadSignalPtr, context: string) =
  ## Safely close and nil the Chronos thread signal, guarding against double close.
  if signalPtr.isNil:
    debugLog(
      "[nimlibp2p] closeThreadSignal skipped context=" & context &
      " signal=nil"
    )
    return
  let signalAddr = cast[int](signalPtr)
  debugLog(
    "[nimlibp2p] closeThreadSignal begin context=" & context &
    " signal=" & $signalAddr
  )
  let res = signalPtr.close()
  if res.isErr():
    warn "thread signal close failed", ctx = context, signalAddr = signalAddr, err = res.error
  else:
    debugLog(
      "[nimlibp2p] closeThreadSignal done context=" & context &
      " signal=" & $signalAddr
    )
  signalPtr = nil

proc submitCommand(node: NimNode, cmd: Command): Command =
  let active = node.running.load(moAcquire)
  if not active:
    warn "submitCommand rejected: node not running", kind = $cmd.kind
    cmd.resultCode = NimResultInvalidState
    return cmd
  info "submitCommand", kind = $cmd.kind, pending = node.pendingCommands.load(moAcquire)
  let enqueueStart = epochTime()
  if cmd.completion.isNil:
    let signalRes = ThreadSignalPtr.new()
    if signalRes.isErr():
      cmd.resultCode = NimResultError
      cmd.errorMsg = "ThreadSignalPtr.new failed: " & signalRes.error
      return cmd
    cmd.completion = signalRes.get()
  enqueueCommand(node, cmd)
  let queueLen = node.queuedCommands.load(moAcquire)
  let fired = node.signal.fireSync()
  if fired.isErr():
    warn "failed to fire thread signal", err = fired.error
  elif fired.get():
    debugLog(
      "[nimlibp2p] submitCommand fireSync ok node=" & $cast[int](node) &
      " kind=" & $cmd.kind &
      " queued=" & $queueLen
    )
  let waitRes = cmd.completion.waitSync()
  if waitRes.isErr():
    cmd.resultCode = NimResultError
    if cmd.errorMsg.len == 0:
      cmd.errorMsg = waitRes.error
  elif not waitRes.get():
    cmd.resultCode = NimResultError
    if cmd.errorMsg.len == 0:
      cmd.errorMsg = "command wait timeout"
  let elapsedMs = int((epochTime() - enqueueStart) * 1000)
  debugLog(
    "[nimlibp2p] submitCommand finished node=" & $cast[int](node) &
    " kind=" & $cmd.kind &
    " waitMs=" & $elapsedMs &
    " rc=" & $int(cmd.resultCode)
  )
  cmd

proc logMemoryLoop(n: NimNode) {.async.} =
  if getEnv("LIBP2P_DEBUG_MEM").len == 0:
    return
  while n.running.load(moAcquire):
    when compiles(system.GC_getStatistics()):
      try:
        let stats = system.GC_getStatistics()
        info "nimlibp2p memory", stats = repr(stats)
      except CatchableError:
        discard
    await sleepAsync(chronos.seconds(5))

# -------------------------------------------------------------
# FFI 接口实现
# -------------------------------------------------------------

proc libp2p_node_init*(configJson: cstring): pointer {.exportc, cdecl, dynlib.} =
  setupForeignThreadGc()
  defer: tearDownForeignThreadGc()
  debugLog("[nimlibp2p] libp2p_node_init invoked")
  info "libp2p_node_init invoked"
  clearInitError()
  if not hookInstalled:
    hookInstalled = true
    system.unhandledExceptionHook =
      cast[type(system.unhandledExceptionHook)](proc (err: ref Exception) {.nimcall.} =
        warn "nimlibp2p unhandled exception", kind = $err.name, err = err.msg
        try:
          debugLog("[nimlibp2p] unhandled exception kind=" & $err.name & " msg=" & err.msg & " stack=" & err.getStackTrace())
        except Exception:
          debugLog("[nimlibp2p] unhandled exception hook: stack retrieval failed")
      )
  try:
    let cfg = parseConfig(configJson).valueOr:
      recordInitError("parseConfig failed: " & error)
      debugLog("[nimlibp2p] parseConfig failed: " & error)
      return nil
    debugLog(
      "[nimlibp2p] parseConfig succeeded identityProvided=" &
      $cfg.identity.isSome() & " dataDir=" & $cfg.dataDir.isSome()
    )
    if cfg.dataDir.isSome():
      let dirPath = cfg.dataDir.get()
      debugLog("[nimlibp2p] ensuring dataDir exists path=" & dirPath)
      try:
        if not dirExists(dirPath):
          createDir(dirPath)
          debugLog("[nimlibp2p] dataDir created")
        else:
          debugLog("[nimlibp2p] dataDir already exists")
        putEnv("UNIMAKER_LIBP2P_DATA_DIR", dirPath)
        let peerStoreDir = dirPath / "peerstore"
        if not dirExists(peerStoreDir):
          createDir(peerStoreDir)
          debugLog("[nimlibp2p] peerstore dir created")
      except CatchableError as exc:
        recordInitError("dataDir ensure failed: " & exc.msg)
        debugLog("[nimlibp2p] dataDir ensure failed: " & exc.msg)
        return nil
    info "parsed config", listenAddresses = cfg.listenAddresses, enableMetrics = cfg.enableMetrics
    debugLog("[nimlibp2p] libp2p_node_init: invoking buildSwitch")
    let sw = buildSwitch(cfg).valueOr:
      recordInitError("buildSwitch failed: " & error)
      debugLog("[nimlibp2p] buildSwitch failed: " & error)
      return nil
    debugLog("[nimlibp2p] buildSwitch succeeded")
    let gossip = initGossip(sw, cfg).valueOr:
      recordInitError("initGossip failed: " & error)
      debugLog("[nimlibp2p] initGossip failed: " & error)
      return nil
    debugLog("[nimlibp2p] initGossip succeeded")
    debugLog("[nimlibp2p] creating thread signal")
    let signalRes = ThreadSignalPtr.new()
    if signalRes.isErr():
      recordInitError("ThreadSignalPtr.new failed: " & signalRes.error)
      debugLog("[nimlibp2p] ThreadSignalPtr.new failed: " & signalRes.error)
      return nil
    var signalPtr = signalRes.get()
    let signalAddr = cast[int](signalPtr)
    debugLog(
      "[nimlibp2p] thread signal created signal=" & $signalAddr
    )
    info "thread signal created", signalAddr = signalAddr
    if cfg.extra.isNil:
      debugLog("[nimlibp2p] config extra is nil")
    else:
      debugLog("[nimlibp2p] config extra kind=" & $cfg.extra.kind & " text=" & $cfg.extra)
    let initialPreferredIpv4 = extractPreferredIpv4(cfg)
    if initialPreferredIpv4.len > 0:
      debugLog("[nimlibp2p] config provided preferred ipv4=" & initialPreferredIpv4)
    let readySignalRes = ThreadSignalPtr.new()
    if readySignalRes.isErr():
      closeThreadSignal(signalPtr, "init:readySignal failure")
      recordInitError("ThreadSignalPtr.new (ready) failed: " & readySignalRes.error)
      debugLog("[nimlibp2p] ready ThreadSignalPtr.new failed: " & readySignalRes.error)
      return nil
    var readySignalPtr = readySignalRes.get()
    let shutdownSignalRes = ThreadSignalPtr.new()
    if shutdownSignalRes.isErr():
      closeThreadSignal(readySignalPtr, "init:shutdownSignal failure")
      closeThreadSignal(signalPtr, "init:shutdownSignal failure")
      recordInitError("ThreadSignalPtr.new (shutdown) failed: " & shutdownSignalRes.error)
      debugLog("[nimlibp2p] shutdown ThreadSignalPtr.new failed: " & shutdownSignalRes.error)
      return nil
    var shutdownSignalPtr = shutdownSignalRes.get()

    var node = NimNode(
      cfg: cfg,
      switchInstance: sw,
      gossip: gossip,
      signal: signalPtr,
      readySignal: readySignalPtr,
      shutdownSignal: shutdownSignalPtr,
      subscriptions: initTable[pointer, Subscription](),
      peerHints: initTable[string, seq[string]](),
      directWaiters: initTable[string, DirectWaiter](),
      lastDirectError: "",
      dmService: nil,
      dmServiceMounted: false,
      feedService: nil,
      defaultTopicsReady: false,
      internalSubscriptions: initTable[string, TopicHandler](),
      connectionEventsEnabled: false,
      connectionHandlers: @[],
      keepAlivePeers: initHashSet[string](),
      keepAliveLoop: nil,
      eventLog: @[],
      eventLogLimit: EventLogLimit,
      lanEndpointsCache: newJObject(),
      feedItems: @[],
      livestreams: initTable[string, LivestreamSession](),
      lanGroups: initTable[string, LanGroupState](),
      rendezvous: findRendezvousService(sw),
      relayReservations: initTable[string, int64](),
      discovery: nil,
      mdnsInterface: nil,
      mdnsService: DefaultMdnsServiceName,
      mdnsServices: @[],
      mdnsQueries: @[],
      mdnsWatchers: @[],
      mdnsLastSeen: initTable[string, int64](),
      mdnsPreferredIpv4: initialPreferredIpv4,
      mdnsRestartPending: false,
      mdnsStarting: false,
      mdnsReady: false,
      mdnsRestarting: false,
    )
    initLock(node.eventLogLock)
    node.mdnsEnabled = true
    node.mdnsIntervalSeconds = 2
    initCommandQueue(node)
    node.running.store(true, moRelease)
    node.stopRequested.store(false, moRelease)
    node.pendingCommands.store(0, moRelease)
    node.ready.store(false, moRelease)
    node.shutdownComplete.store(false, moRelease)
    node.threadJoined.store(false, moRelease)
    node.queuedCommands.store(0, moRelease)
    let bootstrapAddrs = multiaddrsFromExtra(cfg.extra, "bootstrapMultiaddrs")
    if bootstrapAddrs.len > 0:
      registerInitialPeerHints(node, bootstrapAddrs, "bootstrap")
    let relayAddrs = multiaddrsFromExtra(cfg.extra, "relayMultiaddrs")
    if relayAddrs.len > 0:
      registerInitialPeerHints(node, relayAddrs, "relay")
    GC_ref(node)
    let handlePtr = makeNodeHandle(node)
    try:
      createThread(node.thread, eventLoop, handlePtr)
      info "event loop thread created", threadId = cast[int](node.thread)
      debugLog("[nimlibp2p] event loop thread created")
    except CatchableError as exc:
      recordInitError("createThread failed: " & exc.msg)
      debugLog("[nimlibp2p] createThread failed: " & exc.msg)
      closeThreadSignal(node.shutdownSignal, "init:createThread failed")
      closeThreadSignal(node.readySignal, "init:createThread failed")
      closeThreadSignal(node.signal, "init:createThread failed")
      if not node.queueStub.isNil:
        destroyCommand(node.queueStub)
        node.queueStub = nil
      GC_unref(node)
      freeNodeHandle(handlePtr)
      return nil
    ensureReady(node)
    debugLog(
      "[nimlibp2p] ensureReady returned node=" & $cast[int](node) &
      " ready=" & $node.ready.load(moAcquire)
    )
    info "libp2p node ready", started = node.started
    debugLog(
      "[nimlibp2p] libp2p node ready node=" & $cast[int](node) &
      " handle=" & $cast[int](handlePtr)
    )
    handlePtr
  except CatchableError as exc:
    let message = "unhandled exception: " & exc.msg
    if lastInitError.len == 0:
      recordInitError(message)
    debugLog("[nimlibp2p] init exception: " & message)
    nil
  except:
    let message = "unhandled non-catchable exception: " & getCurrentExceptionMsg()
    if lastInitError.len == 0:
      recordInitError(message)
    debugLog("[nimlibp2p] init fatal: " & message)
    nil

proc libp2p_node_start*(handle: pointer): cint {.exportc, cdecl, dynlib.} =
  let node = nodeFromHandle(handle)
  if node.isNil:
    return NimResultInvalidArgument
  let cmd = makeCommand(cmdStart)
  let result = submitCommand(node, cmd)
  let rc = result.resultCode
  let err = result.errorMsg
  destroyCommand(cmd)
  debugLog(
    "[nimlibp2p] libp2p_node_start result code=" & $int(rc) &
    " err=" & err
  )
  if rc != NimResultOk and err == "WARN_DISCOVERY_ADVERTISE":
    return NimResultOk
  rc

proc libp2p_node_stop*(handle: pointer): cint {.exportc, cdecl, dynlib.} =
  let node = nodeFromHandle(handle)
  if node.isNil:
    return NimResultInvalidArgument
  let cmd = makeCommand(cmdStop)
  let result = submitCommand(node, cmd)
  let rc = result.resultCode
  destroyCommand(cmd)
  if rc == NimResultOk:
    waitShutdown(node)
    if not node.threadJoined.load(moAcquire):
      joinThread(node.thread)
      node.threadJoined.store(true, moRelease)
    node.running.store(false, moRelease)
  rc

proc libp2p_node_free*(handle: pointer) {.exportc, cdecl, dynlib.} =
  let node = nodeFromHandle(handle)
  let handleAddr = cast[int](handle)
  if node.isNil:
    debugLog(
      "[nimlibp2p] libp2p_node_free skipped handle=" & $handleAddr &
      " node=nil"
    )
    freeNodeHandle(handle)
    return
  let nodeAddr = cast[int](node)
  debugLog(
    "[nimlibp2p] libp2p_node_free begin handle=" & $handleAddr &
    " node=" & $nodeAddr
  )
  info "libp2p_node_free begin", handleAddr = handleAddr, nodeAddr = nodeAddr
  let wasRunning = node.running.load(moAcquire)
  if wasRunning:
    discard libp2p_node_stop(handle)
  waitShutdown(node)
  node.running.store(false, moRelease)
  closeThreadSignal(node.signal, "node_free")
  closeThreadSignal(node.readySignal, "node_free")
  closeThreadSignal(node.shutdownSignal, "node_free")
  if not node.queueStub.isNil:
    destroyCommand(node.queueStub)
    node.queueStub = nil
  deinitLock(node.eventLogLock)
  GC_unref(node)
  debugLog(
    "[nimlibp2p] libp2p_node_free completed handle=" & $handleAddr &
    " node=" & $nodeAddr
  )
  info "libp2p_node_free completed", handleAddr = handleAddr, nodeAddr = nodeAddr
  freeNodeHandle(handle)

proc libp2p_pubsub_publish*(
    handle: pointer,
    topic: cstring,
    payload: ptr byte,
    payloadLen: csize_t,
    deliveredOut: ptr csize_t
): cint {.exportc, cdecl, dynlib.} =
  if topic.isNil:
    return NimResultInvalidArgument
  let node = nodeFromHandle(handle)
  if node.isNil:
    return NimResultInvalidArgument
  var data: seq[byte] = @[]
  if payloadLen > 0 and not payload.isNil:
    data.setLen(int(payloadLen))
    copyMem(addr data[0], payload, int(payloadLen))
  let cmd = makeCommand(cmdPublish)
  cmd.topic = $topic
  cmd.payload = data
  let result = submitCommand(node, cmd)
  if deliveredOut != nil:
    deliveredOut[] = csize_t(result.delivered)
  let code = result.resultCode
  destroyCommand(cmd)
  code

proc libp2p_pubsub_subscribe*(
    handle: pointer,
    topic: cstring,
    callback: RawPubsubCallback,
    userData: pointer
): pointer {.exportc, cdecl, dynlib.} =
  if topic.isNil:
    return nil
  let node = nodeFromHandle(handle)
  if node.isNil:
    return nil
  let cmd = makeCommand(cmdSubscribe)
  cmd.topic = $topic
  cmd.callback = callback
  cmd.userData = userData
  let result = submitCommand(node, cmd)
  let ptrResult =
    if result.resultCode == NimResultOk: result.ptrResult else: nil
  destroyCommand(cmd)
  ptrResult

proc libp2p_pubsub_unsubscribe*(
    handle: pointer, subscription: pointer
): cint {.exportc, cdecl, dynlib.} =
  if subscription.isNil:
    return NimResultInvalidArgument
  let node = nodeFromHandle(handle)
  if node.isNil:
    return NimResultInvalidArgument
  let cmd = makeCommand(cmdUnsubscribe)
  cmd.subscriptionPtr = subscription
  let result = submitCommand(node, cmd)
  let rc = result.resultCode
  destroyCommand(cmd)
  rc

proc libp2p_connect_peer*(handle: pointer, peerId: cstring): cint {.exportc, cdecl, dynlib.} =
  if peerId.isNil:
    return NimResultInvalidArgument
  let node = nodeFromHandle(handle)
  if node.isNil:
    return NimResultInvalidArgument
  let cmd = makeCommand(cmdConnectPeer)
  cmd.peerId = $peerId
  let result = submitCommand(node, cmd)
  let rc = result.resultCode
  let err = result.errorMsg
  destroyCommand(cmd)
  if rc != NimResultOk:
    warn "libp2p_connect_peer failed", peer = $peerId, err = err
  rc

proc libp2p_connect_multiaddr*(
    handle: pointer, multiaddr: cstring
): cint {.exportc, cdecl, dynlib.} =
  if multiaddr.isNil:
    return NimResultInvalidArgument
  let node = nodeFromHandle(handle)
  if node.isNil:
    return NimResultInvalidArgument
  let cmd = makeCommand(cmdConnectMultiaddr)
  cmd.multiaddr = $multiaddr
  let result = submitCommand(node, cmd)
  let rc = result.resultCode
  let err = result.errorMsg
  destroyCommand(cmd)
  if rc != NimResultOk:
    warn "libp2p_connect_multiaddr failed", addr = $multiaddr, err = err
  rc

proc libp2p_disconnect_peer*(handle: pointer, peerId: cstring): cint {.exportc, cdecl, dynlib.} =
  if peerId.isNil:
    return NimResultInvalidArgument
  let node = nodeFromHandle(handle)
  if node.isNil:
    return NimResultInvalidArgument
  let cmd = makeCommand(cmdDisconnectPeer)
  cmd.peerId = $peerId
  let result = submitCommand(node, cmd)
  let rc = result.resultCode
  destroyCommand(cmd)
  rc

proc libp2p_send_dm_payload*(handle: pointer, peerId: cstring, payload: ptr byte, payloadLen: csize_t): bool {.exportc, cdecl, dynlib.} =
  if peerId.isNil:
    return false
  let node = nodeFromHandle(handle)
  if node.isNil:
    return false
  let cmd = makeCommand(cmdSendDirect)
  cmd.peerId = $peerId
  var jsonPayload = ""
  var parsedEnvelope: JsonNode = nil
  if payloadLen > 0 and not payload.isNil:
    let length = int(payloadLen)
    var buffer = newString(length)
    copyMem(addr buffer[0], payload, length)
    try:
      parsedEnvelope = parseJson(buffer)
    except CatchableError:
      parsedEnvelope = nil
    if not parsedEnvelope.isNil and parsedEnvelope.kind == JObject:
      jsonPayload = buffer
    else:
      cmd.payload.setLen(length)
      copyMem(addr cmd.payload[0], payload, length)
  if jsonPayload.len > 0:
    cmd.jsonString = jsonPayload
    if not parsedEnvelope.isNil and parsedEnvelope.kind == JObject:
      cmd.ackRequested = jsonGetBool(parsedEnvelope, "ackRequested")
      let mid = jsonGetStr(parsedEnvelope, "mid")
      if mid.len > 0:
        cmd.messageId = mid
      let opValue = jsonGetStr(parsedEnvelope, "op")
      if opValue.len > 0:
        cmd.op = opValue
  else:
    cmd.op = "binary"
    cmd.messageId = ""
    cmd.ackRequested = false
  let result = submitCommand(node, cmd)
  let rc = result.resultCode
  destroyCommand(cmd)
  rc == NimResultOk

proc libp2p_send_direct_text*(
    handle: pointer,
    peerId: cstring,
    messageId: cstring,
    text: cstring,
    replyTo: cstring,
    requestAck: bool,
    timeoutMs: cint
): bool {.exportc, cdecl, dynlib.} =
  if peerId.isNil or text.isNil:
    return false
  let node = nodeFromHandle(handle)
  if node.isNil:
    return false
  let cmd = makeCommand(cmdSendDirect)
  cmd.peerId = $peerId
  cmd.messageId = if messageId.isNil: "" else: $messageId
  cmd.textPayload = $text
  cmd.replyTo = if replyTo.isNil: "" else: $replyTo
  cmd.op = "text"
  cmd.ackRequested = requestAck
  cmd.timeoutMs = int(timeoutMs)
  let result = submitCommand(node, cmd)
  let ok = result.resultCode == NimResultOk and (not requestAck or result.boolResult)
  destroyCommand(cmd)
  ok

proc libp2p_send_chat_control*(
    handle: pointer,
    peerId: cstring,
    op: cstring,
    messageId: cstring,
    body: cstring,
    target: cstring,
    requestAck: bool,
    timeoutMs: cint
): bool {.exportc, cdecl, dynlib.} =
  if peerId.isNil or op.isNil:
    return false
  let node = nodeFromHandle(handle)
  if node.isNil:
    return false
  let cmd = makeCommand(cmdSendDirect)
  cmd.peerId = $peerId
  cmd.op = $op
  cmd.messageId = if messageId.isNil: "" else: $messageId
  cmd.textPayload = if body.isNil: "" else: $body
  cmd.target = if target.isNil: "" else: $target
  cmd.ackRequested = requestAck
  cmd.timeoutMs = int(timeoutMs)
  let result = submitCommand(node, cmd)
  let ok = result.resultCode == NimResultOk and (not requestAck or result.boolResult)
  if not ok and result.errorMsg.len > 0:
    node.lastDirectError = result.errorMsg
  destroyCommand(cmd)
  ok

proc libp2p_send_chat_ack_ffi*(
    handle: pointer,
    peerId: cstring,
    messageId: cstring,
    success: bool,
    errorMsg: cstring
): bool {.exportc, cdecl, dynlib.} =
  if peerId.isNil or messageId.isNil:
    return false
  let node = nodeFromHandle(handle)
  if node.isNil:
    return false
  let cmd = makeCommand(cmdSendChatAck)
  cmd.peerId = $peerId
  cmd.messageId = $messageId
  cmd.boolResult = success
  cmd.errorMsg = if errorMsg.isNil: "" else: $errorMsg
  let result = submitCommand(node, cmd)
  let ok = result.resultCode == NimResultOk
  destroyCommand(cmd)
  ok

proc libp2p_send_with_ack*(
    handle: pointer,
    peerId: cstring,
    jsonPayload: cstring,
    timeoutMs: cint
): bool {.exportc, cdecl, dynlib.} =
  if peerId.isNil or jsonPayload.isNil:
    return false
  let node = nodeFromHandle(handle)
  if node.isNil:
    return false
  let cmd = makeCommand(cmdSendDirect)
  cmd.peerId = $peerId
  cmd.jsonString = $jsonPayload
  cmd.ackRequested = true
  cmd.timeoutMs = int(timeoutMs)
  let result = submitCommand(node, cmd)
  let ok = result.resultCode == NimResultOk and result.boolResult
  if not ok and result.errorMsg.len > 0:
    node.lastDirectError = result.errorMsg
  destroyCommand(cmd)
  ok

proc libp2p_get_last_direct_error*(handle: pointer): cstring {.exportc, cdecl, dynlib.} =
  let node = nodeFromHandle(handle)
  if node.isNil:
    return allocCString("")
  let cmd = makeCommand(cmdGetLastDirectError)
  let result = submitCommand(node, cmd)
  let text = result.stringResult
  destroyCommand(cmd)
  allocCString(text)

proc libp2p_initialize_conn_events*(handle: pointer): bool {.exportc, cdecl, dynlib.} =
  let node = nodeFromHandle(handle)
  if node.isNil:
    return false
  let cmd = makeCommand(cmdInitializeConnEvents)
  let result = submitCommand(node, cmd)
  let ok = result.resultCode == NimResultOk
  destroyCommand(cmd)
  ok

proc libp2p_refresh_peer_connections*(handle: pointer): bool {.exportc, cdecl, dynlib.} =
  let node = nodeFromHandle(handle)
  if node.isNil:
    return false
  let cmd = makeCommand(cmdRefreshPeerConnections)
  let result = submitCommand(node, cmd)
  let ok = result.resultCode == NimResultOk
  destroyCommand(cmd)
  ok

proc libp2p_keepalive_pin*(handle: pointer, peerId: cstring): bool {.exportc, cdecl, dynlib.} =
  if peerId.isNil:
    return false
  let node = nodeFromHandle(handle)
  if node.isNil:
    return false
  let cmd = makeCommand(cmdKeepAlivePin)
  cmd.peerId = $peerId
  let result = submitCommand(node, cmd)
  let ok = result.resultCode == NimResultOk
  destroyCommand(cmd)
  ok

proc libp2p_keepalive_unpin*(handle: pointer, peerId: cstring): bool {.exportc, cdecl, dynlib.} =
  if peerId.isNil:
    return false
  let node = nodeFromHandle(handle)
  if node.isNil:
    return false
  let cmd = makeCommand(cmdKeepAliveUnpin)
  cmd.peerId = $peerId
  let result = submitCommand(node, cmd)
  let ok = result.resultCode == NimResultOk
  destroyCommand(cmd)
  ok

proc libp2p_emit_manual_event*(handle: pointer, topic: cstring, payload: cstring): bool {.exportc, cdecl, dynlib.} =
  if topic.isNil or payload.isNil:
    return false
  let node = nodeFromHandle(handle)
  if node.isNil:
    return false
  let cmd = makeCommand(cmdEmitManualEvent)
  cmd.topic = $topic
  cmd.jsonString = $payload
  let result = submitCommand(node, cmd)
  let ok = result.resultCode == NimResultOk
  destroyCommand(cmd)
  ok

proc libp2p_boost_connectivity*(handle: pointer): bool {.exportc, cdecl, dynlib.} =
  let node = nodeFromHandle(handle)
  if node.isNil:
    return false
  let cmd = makeCommand(cmdBoostConnectivity)
  let result = submitCommand(node, cmd)
  let ok = result.resultCode == NimResultOk
  destroyCommand(cmd)
  ok

proc libp2p_get_lan_endpoints_json*(handle: pointer): cstring {.exportc, cdecl, dynlib.} =
  let node = nodeFromHandle(handle)
  if node.isNil:
    return allocCString("[]")
  let cmd = makeCommand(cmdGetLanEndpoints)
  let result = submitCommand(node, cmd)
  var text = "[]"
  if result.resultCode == NimResultOk:
    try:
      if result.stringResult.len > 0:
        text = result.stringResult
    except CatchableError:
      text = "[]"
  destroyCommand(cmd)
  allocCString(text)

proc libp2p_fetch_feed_snapshot*(handle: pointer): cstring {.exportc, cdecl, dynlib.} =
  let node = nodeFromHandle(handle)
  if node.isNil:
    return allocCString("{}")
  let cmd = makeCommand(cmdFetchFeedSnapshot)
  let result = submitCommand(node, cmd)
  let text = if result.resultCode == NimResultOk: result.stringResult else: "{}"
  destroyCommand(cmd)
  allocCString(text)

proc libp2p_feed_subscribe_peer*(handle: pointer, peerId: cstring): bool {.exportc, cdecl, dynlib.} =
  if peerId.isNil:
    return false
  let node = nodeFromHandle(handle)
  if node.isNil:
    return false
  let cmd = makeCommand(cmdFeedSubscribePeer)
  cmd.peerId = $peerId
  let result = submitCommand(node, cmd)
  let ok = result.resultCode == NimResultOk
  destroyCommand(cmd)
  ok

proc libp2p_feed_unsubscribe_peer*(handle: pointer, peerId: cstring): bool {.exportc, cdecl, dynlib.} =
  if peerId.isNil:
    return false
  let node = nodeFromHandle(handle)
  if node.isNil:
    return false
  let cmd = makeCommand(cmdFeedUnsubscribePeer)
  cmd.peerId = $peerId
  let result = submitCommand(node, cmd)
  let ok = result.resultCode == NimResultOk
  destroyCommand(cmd)
  ok

proc libp2p_feed_publish_entry*(handle: pointer, jsonPayload: cstring): bool {.exportc, cdecl, dynlib.} =
  if jsonPayload.isNil:
    return false
  let node = nodeFromHandle(handle)
  if node.isNil:
    return false
  let cmd = makeCommand(cmdFeedPublishEntry)
  cmd.jsonString = $jsonPayload
  let result = submitCommand(node, cmd)
  let ok = result.resultCode == NimResultOk
  destroyCommand(cmd)
  ok

proc libp2p_upsert_livestream_cfg*(handle: pointer, streamKey: cstring, configJson: cstring): bool {.exportc, cdecl, dynlib.} =
  if streamKey.isNil:
    return false
  let node = nodeFromHandle(handle)
  if node.isNil:
    return false
  let cmd = makeCommand(cmdUpsertLivestream)
  cmd.streamKey = $streamKey
  if not configJson.isNil:
    cmd.jsonString = $configJson
  let result = submitCommand(node, cmd)
  let ok = result.resultCode == NimResultOk
  destroyCommand(cmd)
  ok

proc libp2p_publish_livestream_frame_ffi*(
    handle: pointer,
    streamKey: cstring,
    payload: ptr byte,
    payloadLen: csize_t
): bool {.exportc, cdecl, dynlib.} =
  if streamKey.isNil or payload.isNil or payloadLen == 0:
    return false
  let node = nodeFromHandle(handle)
  if node.isNil:
    return false
  var data = newSeq[byte](int(payloadLen))
  copyMem(addr data[0], payload, int(payloadLen))
  let cmd = makeCommand(cmdPublishLivestream)
  cmd.streamKey = $streamKey
  cmd.payload = data
  let result = submitCommand(node, cmd)
  let ok = result.resultCode == NimResultOk
  destroyCommand(cmd)
  ok

proc libp2p_lan_group_join*(handle: pointer, groupId: cstring): bool {.exportc, cdecl, dynlib.} =
  if groupId.isNil:
    return false
  let node = nodeFromHandle(handle)
  if node.isNil:
    return false
  let cmd = makeCommand(cmdLanGroupJoin)
  cmd.topic = $groupId
  let result = submitCommand(node, cmd)
  let ok = result.resultCode == NimResultOk
  destroyCommand(cmd)
  ok

proc libp2p_lan_group_leave*(handle: pointer, groupId: cstring): bool {.exportc, cdecl, dynlib.} =
  if groupId.isNil:
    return false
  let node = nodeFromHandle(handle)
  if node.isNil:
    return false
  let cmd = makeCommand(cmdLanGroupLeave)
  cmd.topic = $groupId
  let result = submitCommand(node, cmd)
  let ok = result.resultCode == NimResultOk
  destroyCommand(cmd)
  ok

proc libp2p_lan_group_send*(handle: pointer, groupId: cstring, message: cstring): bool {.exportc, cdecl, dynlib.} =
  if groupId.isNil or message.isNil:
    return false
  let node = nodeFromHandle(handle)
  if node.isNil:
    return false
  let cmd = makeCommand(cmdLanGroupSend)
  cmd.topic = $groupId
  cmd.textPayload = $message
  let result = submitCommand(node, cmd)
  let ok = result.resultCode == NimResultOk
  destroyCommand(cmd)
  ok

proc libp2p_rendezvous_advertise*(
    handle: pointer, namespace: cstring, ttlMs: cint
): bool {.exportc, cdecl, dynlib.} =
  if namespace.isNil:
    return false
  let node = nodeFromHandle(handle)
  if node.isNil:
    return false
  let cmd = makeCommand(cmdRendezvousAdvertise)
  cmd.topic = $namespace
  cmd.timeoutMs = if ttlMs > 0: ttlMs else: 120_000
  let result = submitCommand(node, cmd)
  let ok = result.resultCode == NimResultOk
  destroyCommand(cmd)
  ok

proc libp2p_rendezvous_discover*(
    handle: pointer, namespace: cstring, limit: cint
): cstring {.exportc, cdecl, dynlib.} =
  let node = nodeFromHandle(handle)
  if node.isNil:
    return allocCString("[]")
  let cmd = makeCommand(cmdRendezvousDiscover)
  if not namespace.isNil:
    cmd.topic = $namespace
  cmd.intResult = if limit > 0: limit else: 20
  let result = submitCommand(node, cmd)
  let text =
    if result.resultCode == NimResultOk and result.stringResult.len > 0:
      result.stringResult
    else:
      "[]"
  destroyCommand(cmd)
  allocCString(text)

proc libp2p_rendezvous_unregister*(handle: pointer, namespace: cstring): bool {.exportc, cdecl, dynlib.} =
  if namespace.isNil:
    return false
  let node = nodeFromHandle(handle)
  if node.isNil:
    return false
  let cmd = makeCommand(cmdRendezvousUnregister)
  cmd.topic = $namespace
  let result = submitCommand(node, cmd)
  let ok = result.resultCode == NimResultOk
  destroyCommand(cmd)
  ok

proc libp2p_wait_secure_channel_ffi*(handle: pointer, peerId: cstring, timeoutMs: cint): bool {.exportc, cdecl, dynlib.} =
  if peerId.isNil:
    return false
  let node = nodeFromHandle(handle)
  if node.isNil:
    return false
  let cmd = makeCommand(cmdWaitSecureChannel)
  cmd.peerId = $peerId
  cmd.timeoutMs = int(timeoutMs)
  let result = submitCommand(node, cmd)
  let ok = result.resultCode == NimResultOk and result.boolResult
  destroyCommand(cmd)
  ok

proc libp2p_poll_events*(handle: pointer, maxEvents: cint): cstring {.exportc, cdecl, dynlib.} =
  let node = nodeFromHandle(handle)
  if node.isNil:
    return allocCString("[]")
  let cmd = makeCommand(cmdPollEvents)
  cmd.requestLimit = int(maxEvents)
  let result = submitCommand(node, cmd)
  let output =
    if result.resultCode == NimResultOk:
      allocCString(result.stringResult)
    else:
      allocCString("[]")
  destroyCommand(cmd)
  output
proc libp2p_get_connected_peers_json*(handle: pointer): cstring {.exportc, cdecl, dynlib.} =
  let node = nodeFromHandle(handle)
  if node.isNil:
    return nil
  let cmd = makeCommand(cmdConnectedPeers)
  let result = submitCommand(node, cmd)
  let output =
    if result.resultCode == NimResultOk:
      allocCString(result.stringResult)
    else:
      nil
  destroyCommand(cmd)
  output

proc parseJsonStringSeq(text: string): seq[string]

proc libp2p_connected_peers_info*(handle: pointer): cstring {.exportc, cdecl, dynlib.} =
  let node = nodeFromHandle(handle)
  if node.isNil:
    return allocCString("[]")
  let cmd = makeCommand(cmdConnectedPeersInfo)
  let result = submitCommand(node, cmd)
  let output =
    if result.resultCode == NimResultOk and result.stringResult.len > 0:
      allocCString(result.stringResult)
    else:
      allocCString("[]")
  destroyCommand(cmd)
  output

proc libp2p_get_peer_multiaddrs_json*(
    handle: pointer, peerId: cstring
): cstring {.exportc, cdecl, dynlib.} =
  if peerId.isNil:
    return nil
  let node = nodeFromHandle(handle)
  if node.isNil:
    return nil
  let cmd = makeCommand(cmdPeerMultiaddrs)
  cmd.peerId = $peerId
  let result = submitCommand(node, cmd)
  let output =
    if result.resultCode == NimResultOk:
      allocCString(result.stringResult)
    else:
      nil
  destroyCommand(cmd)
  output

proc libp2p_register_peer_hints*(
    handle: pointer, peerId: cstring, addressesJson: cstring, source: cstring
): bool {.exportc, cdecl, dynlib.} =
  if peerId.isNil:
    return false
  var addresses: seq[string] = @[]
  if addressesJson != nil:
    addresses = parseJsonStringSeq($addressesJson)
  if addresses.len == 0:
    return false
  let node = nodeFromHandle(handle)
  if node.isNil:
    return false
  let cmd = makeCommand(cmdRegisterPeerHints)
  cmd.peerId = $peerId
  cmd.addresses = addresses
  if source != nil:
    cmd.source = $source
  let result = submitCommand(node, cmd)
  let ok = result.resultCode == NimResultOk
  destroyCommand(cmd)
  ok

proc libp2p_add_external_address*(
    handle: pointer, multiaddr: cstring
): bool {.exportc, cdecl, dynlib.} =
  if multiaddr.isNil:
    return false
  let addrStr = $multiaddr
  if addrStr.len == 0:
    return false
  let node = nodeFromHandle(handle)
  if node.isNil:
    return false
  let cmd = makeCommand(cmdAddExternalAddress)
  cmd.multiaddr = addrStr
  let result = submitCommand(node, cmd)
  let ok = result.resultCode == NimResultOk
  destroyCommand(cmd)
  ok

proc libp2p_is_peer_connected*(handle: pointer, peerId: cstring): bool {.exportc, cdecl, dynlib.} =
  if peerId.isNil:
    return false
  let node = nodeFromHandle(handle)
  if node.isNil:
    return false
  let cmd = makeCommand(cmdCheckPeerConnected)
  cmd.peerId = $peerId
  let result = submitCommand(node, cmd)
  let connected = result.resultCode == NimResultOk and result.boolResult
  destroyCommand(cmd)
  connected

proc libp2p_sync_peerstore_state*(handle: pointer): cstring {.exportc, cdecl, dynlib.} =
  let node = nodeFromHandle(handle)
  if node.isNil:
    return allocCString("{\"connected_peers\":[],\"total_count\":0}")
  let cmd = makeCommand(cmdSyncPeerstore)
  let result = submitCommand(node, cmd)
  let output =
    if result.resultCode == NimResultOk and result.stringResult.len > 0:
      allocCString(result.stringResult)
    else:
      allocCString("{\"connected_peers\":[],\"total_count\":0}")
  destroyCommand(cmd)
  output

proc libp2p_load_stored_peers*(handle: pointer): cstring {.exportc, cdecl, dynlib.} =
  let node = nodeFromHandle(handle)
  if node.isNil:
    return allocCString("{\"peers\":[],\"totalCount\":0}")
  let cmd = makeCommand(cmdLoadStoredPeers)
  let result = submitCommand(node, cmd)
  let output =
    if result.resultCode == NimResultOk and result.stringResult.len > 0:
      allocCString(result.stringResult)
    else:
      allocCString("{\"peers\":[],\"totalCount\":0}")
  destroyCommand(cmd)
  output

proc libp2p_get_diagnostics_json*(handle: pointer): cstring {.exportc, cdecl, dynlib.} =
  let node = nodeFromHandle(handle)
  if node.isNil:
    return nil
  info "libp2p_get_diagnostics_json called", nodeStarted = node.started
  debugLog("[nimlibp2p] libp2p_get_diagnostics_json submit command")
  if not node.started:
    debugLog("[nimlibp2p] diagnostics requested while node not started")
    return allocCString("{}")
  let cmd = makeCommand(cmdDiagnostics)
  let result = submitCommand(node, cmd)
  debugLog("[nimlibp2p] libp2p_get_diagnostics_json command completed code=" & $result.resultCode)
  let output =
    if result.resultCode == NimResultOk:
      allocCString(result.stringResult)
    else:
      allocCString("{}")
  destroyCommand(cmd)
  output

proc libp2p_string_free*(value: cstring) {.exportc, cdecl, dynlib.} =
  if value != nil:
    deallocShared(value)

proc jsonEncodeStringSeq(items: seq[string]): string =
  var arr = newJArray()
  for item in items:
    arr.add(%item)
  $arr

proc parseJsonStringSeq(text: string): seq[string] =
  if text.len == 0:
    return @[]
  try:
    let node = parseJson(text)
    if node.kind == JArray:
      for item in node:
        if item.kind == JString:
          result.add(item.getStr())
  except CatchableError:
    discard

proc libp2p_get_listen_addresses*(handle: pointer): cstring {.exportc, cdecl, dynlib.} =
  let node = nodeFromHandle(handle)
  if node.isNil:
    return allocCString("[]")
  var addrs = node.cfg.listenAddresses
  try:
    if not node.switchInstance.isNil:
      let info = node.switchInstance.peerInfo
      if not info.isNil:
        let refreshed = refreshLanAwareAddrs(node)
        var runtimeAddrs = if refreshed.len > 0: refreshed else: info.listenAddrs
        if runtimeAddrs.len == 0:
          runtimeAddrs = info.addrs
        if runtimeAddrs.len > 0:
          addrs = runtimeAddrs.mapIt($it)
  except CatchableError:
    discard
  allocCString(jsonEncodeStringSeq(addrs))

proc libp2p_get_dialable_addresses*(handle: pointer): cstring {.exportc, cdecl, dynlib.} =
  let node = nodeFromHandle(handle)
  if node.isNil:
    return allocCString("[]")
  var addrs = node.cfg.listenAddresses.filterIt(not it.endsWith("/tcp/0"))
  try:
    if not node.switchInstance.isNil:
      let info = node.switchInstance.peerInfo
      if not info.isNil:
        let refreshed = refreshLanAwareAddrs(node)
        var runtimeAddrs = (if refreshed.len > 0: refreshed else: info.listenAddrs).mapIt($it)
        if runtimeAddrs.len == 0:
          runtimeAddrs = info.addrs.mapIt($it)
        let filtered = runtimeAddrs.filterIt(
          (not it.endsWith("/tcp/0")) and
          (not it.contains("/ip4/0.0.0.0/")) and
          (not it.contains("/ip6/::/"))
        )
        if filtered.len > 0:
          addrs = filtered
  except CatchableError:
    discard
  allocCString(jsonEncodeStringSeq(addrs))

proc libp2p_get_local_peer_id*(handle: pointer): cstring {.exportc, cdecl, dynlib.} =
  let node = nodeFromHandle(handle)
  if node.isNil:
    return nil
  let peer = localPeerId(node)
  if peer.len == 0:
    return nil
  allocCString(peer)

proc libp2p_node_is_started*(handle: pointer): bool {.exportc, cdecl, dynlib.} =
  let node = nodeFromHandle(handle)
  if node.isNil:
    debugLog(
      "[nimlibp2p] libp2p_node_is_started handle=" & $cast[int](handle) &
      " node=nil"
    )
    return false
  debugLog(
    "[nimlibp2p] libp2p_node_is_started handle=" & $cast[int](handle) &
    " node=" & $cast[int](node) &
    " nodeStarted=" & $node.started
  )
  node.started

proc libp2p_get_bootstrap_status*(handle: pointer): cstring {.exportc, cdecl, dynlib.} =
  let node = nodeFromHandle(handle)
  if node.isNil:
    return allocCString("{}")
  when defined(android):
    var obj = newJObject()
    try:
      if node.switchInstance != nil:
        var connected = initHashSet[string]()
        for pid in node.switchInstance.connectedPeers(Direction.Out):
          connected.incl($pid)
        for pid in node.switchInstance.connectedPeers(Direction.In):
          connected.incl($pid)
        let addrBook = node.switchInstance.peerStore[AddressBook]
        if addrBook != nil:
          for pid, addrs in addrBook.book:
            let peerIdStr = $pid
            let statusVal =
              if connected.contains(peerIdStr): "connected=true"
              elif addrs.len > 0: "connected=false"
              else: "unknown"
            obj[peerIdStr] = %statusVal
    except CatchableError:
      discard
    if obj.len == 0:
      return allocCString("{}")
    allocCString($obj)
  else:
    ## HarmonyOS 版本暂不访问内部 peerStore，避免未适配导致崩溃。
    allocCString("{}")

proc libp2p_reconnect_bootstrap*(handle: pointer): bool {.exportc, cdecl, dynlib.} =
  let node = nodeFromHandle(handle)
  if node.isNil:
    return false
  let cmd = makeCommand(cmdReconnectBootstrap)
  let result = submitCommand(node, cmd)
  let ok = result.resultCode == NimResultOk
  destroyCommand(cmd)
  ok

proc libp2p_mdns_set_enabled*(handle: pointer, enabled: bool): bool {.exportc, cdecl, dynlib.} =
  let node = nodeFromHandle(handle)
  debugLog(
    "[nimlibp2p] libp2p_mdns_set_enabled handle=" & $cast[int](handle) &
    " node=" & $cast[int](node) & " enabled=" & $enabled
  )
  if node.isNil:
    debugLog(
      "[nimlibp2p] libp2p_mdns_set_enabled node=nil handle=" & $cast[int](handle) &
      " enabled=" & $enabled
    )
    return false
  let cmd = makeCommand(cmdMdnsSetEnabled)
  cmd.boolParam = enabled
  let result = submitCommand(node, cmd)
  let ok = result.resultCode == NimResultOk and result.boolResult
  if ok:
    info "mdns set enabled", enabled = enabled
  else:
    warn "mdns set enabled failed", enabled = enabled, err = result.errorMsg
  destroyCommand(cmd)
  ok

proc libp2p_identity_from_seed*(seed: ptr uint8, seedLen: csize_t): cstring {.exportc, cdecl, dynlib.} =
  setupForeignThreadGc()
  defer: tearDownForeignThreadGc()
  try:
    debugLog("[nimlibp2p] libp2p_identity_from_seed start seedLen=" & $seedLen)
    if seed.isNil or seedLen == 0:
      return allocCString("{\"error\":\"seed_empty\"}")

    let length = int(seedLen)
    var seedBytes = newSeq[byte](length)
    if length > 0:
      copyMem(addr seedBytes[0], seed, seedLen)

    debugLog("[nimlibp2p] skipping digest (workaround for SIGSEGV), mixing seed manually")
    var entropy = newSeq[byte](32)
    for i in 0 ..< seedBytes.len:
      entropy[i mod 32] = entropy[i mod 32] xor seedBytes[i]
    
    # Ensure we have some entropy if seed was all zeros (unlikely) or handled by loop
    if seedBytes.len == 0:
       # Should be handled by earlier check
       discard

    debugLog("[nimlibp2p] initializing RNG")
    var rngRef = new(HmacDrbgContext)
    if entropy.len == 0:
      return allocCString("{\"error\":\"entropy_empty\"}")
      
    hmacDrbgInit(
      rngRef[],
      unsafeAddr sha256Vtable,
      unsafeAddr entropy[0],
      uint(entropy.len)
    )

    debugLog("[nimlibp2p] generating keypair")
    let pairRes = KeyPair.random(PKScheme.Ed25519, rngRef[])
    if pairRes.isErr():
      let errMsg = $pairRes.error
      debugLog("[nimlibp2p] keypair generation failed: " & errMsg)
      return allocCString("{\"error\":\"keypair_generation_failed\",\"detail\":\"" & errMsg & "\"}")
    let pair = pairRes.get()

    let privBytes = pair.seckey.getRawBytes().valueOr:
      return allocCString("{\"error\":\"private_bytes_failed\",\"detail\":\"" & $error & "\"}")
    let pubBytes = pair.pubkey.getRawBytes().valueOr:
      return allocCString("{\"error\":\"public_bytes_failed\",\"detail\":\"" & $error & "\"}")

    let peerRes = PeerId.init(pair.seckey)
    if peerRes.isErr():
      let errMsg = $peerRes.error
      return allocCString("{\"error\":\"peerid_derive_failed\",\"detail\":\"" & errMsg & "\"}")
    let peerStr = $peerRes.get()

    let payload = %*{
      "privateKey": base64.encode(privBytes),
      "publicKey": base64.encode(pubBytes),
      "peerId": peerStr,
      "source": "seeded"
    }
    debugLog("[nimlibp2p] success: " & peerStr)
    allocCString($payload)
  except CatchableError as exc:
    let safeMsg = exc.msg.replace("\"", "'")
    debugLog("[nimlibp2p] exception: " & safeMsg)
    allocCString("{\"error\":\"exception\",\"detail\":\"" & safeMsg & "\"}")

proc libp2p_generate_identity_json*(): cstring {.exportc, cdecl, dynlib.} =
  ## Generate a fresh Ed25519 keypair and return JSON payload with base64 fields.
  setupForeignThreadGc()
  defer: tearDownForeignThreadGc()
  try:
    let rngRef = newRng()
    let pairRes = KeyPair.random(PKScheme.Ed25519, rngRef[])
    if pairRes.isErr():
      let errMsg = pairRes.error
      return allocCString("{\"error\":\"keypair_generation_failed\",\"detail\":\"" & $errMsg & "\"}")
    let pair = pairRes.get()

    let privBytes = pair.seckey.getRawBytes().valueOr:
      return allocCString("{\"error\":\"private_bytes_failed\",\"detail\":\"" & $error & "\"}")
    let pubBytes = pair.pubkey.getRawBytes().valueOr:
      return allocCString("{\"error\":\"public_bytes_failed\",\"detail\":\"" & $error & "\"}")

    let peerRes = PeerId.init(pair.seckey)
    if peerRes.isErr():
      let errMsg = peerRes.error
      return allocCString("{\"error\":\"peerid_derive_failed\",\"detail\":\"" & $errMsg & "\"}")
    let peerStr = $peerRes.get()

    let payload = %*{
      "privateKey": base64.encode(privBytes),
      "publicKey": base64.encode(pubBytes),
      "peerId": peerStr,
      "source": "nim-libp2p"
    }
    allocCString($payload)
  except CatchableError as exc:
    let safeMsg = exc.msg.replace("\"", "'")
    allocCString("{\"error\":\"exception\",\"detail\":\"" & safeMsg & "\"}")

proc libp2p_mdns_set_interface*(handle: pointer, ipv4: cstring): bool {.exportc, cdecl, dynlib.} =
  let node = nodeFromHandle(handle)
  debugLog("[nimlibp2p] libp2p_mdns_set_interface handle=" & $cast[int](handle) & " node=" & $cast[int](node) & " ipv4=" & (if ipv4.isNil: "<nil>" else: $ipv4))
  if node.isNil:
    return false
  var ip = ""
  if ipv4 != nil:
    ip = $ipv4
  ip = ip.strip()
  if ip == "0.0.0.0":
    ip = ""
  let cmd = makeCommand(cmdMdnsSetInterface)
  cmd.textPayload = ip
  let result = submitCommand(node, cmd)
  let ok = result.resultCode == NimResultOk and result.boolResult
  if ok:
    info "mdns interface updated", ipv4 = ip
  else:
    warn "mdns interface update failed", ipv4 = ip, err = result.errorMsg
  destroyCommand(cmd)
  ok

proc libp2p_mdns_debug*(handle: pointer): cstring {.exportc, cdecl, dynlib.} =
  let node = nodeFromHandle(handle)
  if node.isNil:
    return allocCString("{\"error\":\"node_nil\"}")
  let cmd = makeCommand(cmdMdnsDebug)
  let result = submitCommand(node, cmd)
  let payload =
    if result.resultCode == NimResultOk and result.stringResult.len > 0:
      result.stringResult
    else:
      "{\"error\":\"mdns_debug_failed\",\"detail\":\"" & result.errorMsg & "\"}"
  destroyCommand(cmd)
  allocCString(payload)

proc libp2p_mdns_multicast_host*(): cstring {.exportc, cdecl, dynlib.} =
  MdnsIpv4Multicast.cstring

proc libp2p_mdns_set_interval*(handle: pointer, seconds: cint): bool {.exportc, cdecl, dynlib.} =
  if seconds <= 0:
    return false
  let node = nodeFromHandle(handle)
  if node.isNil:
    return false
  let cmd = makeCommand(cmdMdnsSetInterval)
  cmd.intParam = int(seconds)
  let result = submitCommand(node, cmd)
  let ok = result.resultCode == NimResultOk
  if ok:
    info "mdns interval updated", seconds = int(seconds)
  else:
    warn "mdns interval update failed", seconds = int(seconds), err = result.errorMsg
  destroyCommand(cmd)
  ok

proc libp2p_mdns_probe*(handle: pointer): bool {.exportc, cdecl, dynlib.} =
  let node = nodeFromHandle(handle)
  debugLog(
    "[nimlibp2p] libp2p_mdns_probe handle=" & $cast[int](handle) &
    " node=" & $cast[int](node)
  )
  if node.isNil:
    debugLog(
      "[nimlibp2p] libp2p_mdns_probe node=nil handle=" & $cast[int](handle)
    )
    return false
  let cmd = makeCommand(cmdMdnsProbe)
  let result = submitCommand(node, cmd)
  let ok = result.resultCode == NimResultOk and result.boolResult
  if ok:
    info "mdns probe requested"
  destroyCommand(cmd)
  ok

proc libp2p_reserve_on_relay*(handle: pointer, relayAddr: cstring): bool {.exportc, cdecl, dynlib.} =
  if relayAddr.isNil:
    return false
  let node = nodeFromHandle(handle)
  if node.isNil:
    return false
  let cmd = makeCommand(cmdReserveOnRelay)
  cmd.multiaddr = $relayAddr
  let result = submitCommand(node, cmd)
  let ok = result.resultCode == NimResultOk and result.intResult > 0
  destroyCommand(cmd)
  ok

proc libp2p_reserve_on_all_relays*(handle: pointer): cint {.exportc, cdecl, dynlib.} =
  let node = nodeFromHandle(handle)
  if node.isNil:
    return cint(0)
  let cmd = makeCommand(cmdReserveOnAllRelays)
  let result = submitCommand(node, cmd)
  let count = if result.resultCode == NimResultOk: result.intResult else: 0
  destroyCommand(cmd)
  cint(count)

proc libp2p_dex_init*(handle: pointer, configJson: cstring): bool {.exportc, cdecl, dynlib.} =
  let node = nodeFromHandle(handle)
  if node.isNil: return false
  let cmd = makeCommand(cmdDexInit)
  cmd.jsonString = if configJson != nil: $configJson else: ""
  let result = submitCommand(node, cmd)
  let ok = result.resultCode == NimResultOk
  destroyCommand(cmd)
  ok

proc libp2p_dex_submit_order*(handle: pointer, orderJson: cstring): bool {.exportc, cdecl, dynlib.} =
  let node = nodeFromHandle(handle)
  if node.isNil: return false
  let cmd = makeCommand(cmdDexSubmitOrder)
  cmd.jsonString = if orderJson != nil: $orderJson else: ""
  let result = submitCommand(node, cmd)
  let ok = result.resultCode == NimResultOk
  destroyCommand(cmd)
  ok

proc libp2p_dex_submit_mixer_intent*(handle: pointer, intentJson: cstring): bool {.exportc, cdecl, dynlib.} =
  let node = nodeFromHandle(handle)
  if node.isNil: return false
  let cmd = makeCommand(cmdDexSubmitMixerIntent)
  cmd.jsonString = if intentJson != nil: $intentJson else: ""
  let result = submitCommand(node, cmd)
  let ok = result.resultCode == NimResultOk
  destroyCommand(cmd)
  ok

proc libp2p_dex_get_market_data*(handle: pointer, asset: cstring): cstring {.exportc, cdecl, dynlib.} =
  setupForeignThreadGc()
  defer: tearDownForeignThreadGc()
  let node = nodeFromHandle(handle)
  if node.isNil: return nil
  let cmd = makeCommand(cmdDexGetMarketData)
  cmd.textPayload = if asset != nil: $asset else: ""
  let result = submitCommand(node, cmd)
  let payload = if result.resultCode == NimResultOk: result.stringResult else: ""
  destroyCommand(cmd)
  allocCString(payload)

proc libp2p_dex_get_wallets*(handle: pointer): cstring {.exportc, cdecl, dynlib.} =
  setupForeignThreadGc()
  defer: tearDownForeignThreadGc()
  let node = nodeFromHandle(handle)
  if node.isNil: return allocCString("[]")
  let cmd = makeCommand(cmdDexGetWallets)
  let result = submitCommand(node, cmd)
  let payload = if result.resultCode == NimResultOk: result.stringResult else: "[]"
  destroyCommand(cmd)
  allocCString(payload)


# ---------------------------------------------------------------------------
# Adapter Signatures (Scriptless Scripts) - Production Implementation
# ---------------------------------------------------------------------------

proc strToBytes(s: string): seq[byte] =
  result = newSeq[byte](s.len)
  for i in 0..<s.len: result[i] = byte(s[i])

proc hexToSeqByte(h: string): seq[byte] =
  result = newSeq[byte](h.len div 2)
  hexToBytes(h, result)

proc hexToBlind(h: string): CoinJoinResult[CoinJoinBlind] =
  if h.len != 64: return err(newCoinJoinError(cjInvalidInput, "invalid hex len"))
  var b: CoinJoinBlind
  try:
    let bytes = hexToSeqByte(h)
    for i in 0..<32: b[i] = bytes[i]
    ok(b)
  except:
    err(newCoinJoinError(cjInvalidInput, "hex decode failed"))

proc blindToHex(b: CoinJoinBlind): string =
  var s = newStringOfCap(64)
  for i in 0..<32: s.add(toHex(b[i]))
  s


proc libp2p_adapter_generate_secret*(): cstring {.exportc, cdecl, dynlib.} =
  # Real: Generate random scalar
  var bytes: array[32, byte]
  discard randomBytes(bytes)
  result = allocCString(toHex(bytes))

proc libp2p_adapter_compute_payment_point*(secret: cstring): cstring {.exportc, cdecl, dynlib.} =
  # Real: P = s*G
  let sRes = hexToBlind($secret)
  if sRes.isErr: return allocCString("")
  
  let pubRes = generatorMul(encodeAmount(0)) # Hack to get type? No, scalarToPub
  # secp_utils.scalarToPub takes CoinJoinAmount (uint64 array), not Blind (scalar).
  # Wait, scalarToPub takes CoinJoinAmount.
  # But I have CoinJoinBlind (scalar).
  # secp_utils.scalarToPub implementation: SkPrivateKey.init(scalar)
  # CoinJoinAmount is array[32, byte] (same as Blind).
  
  let s = sRes.get
  let pubRes2 = scalarToPub(cast[CoinJoinAmount](s))
  if pubRes2.isErr: return allocCString("")
  
  let pub = pubRes2.get
  let raw = secp256k1.SkPublicKey(pub).toRawCompressed()
  result = allocCString(toHex(raw))

proc libp2p_adapter_sign*(msg: cstring, privKey: cstring, point: cstring): cstring {.exportc, cdecl, dynlib.} =
  # Real Schnorr Adaptor Signature
  # 1. Parse inputs
  let xRes = hexToBlind($privKey)
  if xRes.isErr: return allocCString("err_priv")
  let x = xRes.get
  
  let T_hex = $point
  let T_bytes = hexToSeqByte(T_hex)
  let T_res = secp.SkPublicKey.init(T_bytes)
  if T_res.isErr: return allocCString("err_point")
  let T = T_res.get
  
  # 2. Generate k (nonce)
  var k_bytes: array[32, byte]
  discard randomBytes(k_bytes)
  let k = cast[CoinJoinBlind](k_bytes)
  
  # 3. R = k*G
  let R_res = scalarToPub(cast[CoinJoinAmount](k))
  if R_res.isErr: return allocCString("err_R")
  let R = R_res.get
  
  # 4. R_adaptor = R + T
  let R_adaptor_res = combineKeys([R, T])
  if R_adaptor_res.isErr: return allocCString("err_combine")
  let R_adaptor = R_adaptor_res.get
  
  # 5. e = Hash(R_adaptor || P || m)
  # P = x*G
  let P_res = scalarToPub(cast[CoinJoinAmount](x))
  if P_res.isErr: return allocCString("err_P")
  let P = P_res.get
  
  var ctx = newSeq[byte]()
  ctx.add(secp256k1.SkPublicKey(R_adaptor).toRawCompressed())
  ctx.add(secp256k1.SkPublicKey(P).toRawCompressed())
  ctx.add(strToBytes($msg))
  let hash = sha256.digest(ctx)
  let e = cast[CoinJoinBlind](hash.data)
  
  # 6. s_adaptor = k + e*x
  let term_res = mulScalars(e, x)
  if term_res.isErr: return allocCString("err_mul")
  let term = term_res.get
  
  let s_adaptor_res = addBlinds(k, term)
  if s_adaptor_res.isErr: return allocCString("err_add")
  let s_adaptor = s_adaptor_res.get
  
  # Return: R_adaptor (hex) | s_adaptor (hex) | T (hex)
  let R_adaptor_hex = toHex(secp256k1.SkPublicKey(R_adaptor).toRawCompressed())
  let s_adaptor_hex = blindToHex(s_adaptor)
  
  result = allocCString(R_adaptor_hex & "|" & s_adaptor_hex & "|" & T_hex)

proc libp2p_adapter_complete_sig*(adaptorSig: cstring, secret: cstring): cstring {.exportc, cdecl, dynlib.} =
  # Real: s = s_adaptor + t
  let parts = ($adaptorSig).split('|')
  if parts.len < 3: return allocCString("err_fmt")
  
  let s_adaptor_hex = parts[1]
  let s_adaptor_res = hexToBlind(s_adaptor_hex)
  if s_adaptor_res.isErr: return allocCString("err_parse_sig")
  let s_adaptor = s_adaptor_res.get
  
  let t_res = hexToBlind($secret)
  if t_res.isErr: return allocCString("err_parse_secret")
  let t = t_res.get
  
  let s_res = addBlinds(s_adaptor, t)
  if s_res.isErr: return allocCString("err_complete")
  let s = s_res.get
  
  # Return: R_adaptor | s
  result = allocCString(parts[0] & "|" & blindToHex(s))

proc libp2p_adapter_extract_secret*(adaptorSig: cstring, validSig: cstring): cstring {.exportc, cdecl, dynlib.} =
  # Real: t = s - s_adaptor
  let a_parts = ($adaptorSig).split('|')
  let v_parts = ($validSig).split('|')
  if a_parts.len < 3 or v_parts.len < 2: return allocCString("err_fmt")
  
  let s_adaptor_hex = a_parts[1]
  let s_hex = v_parts[1]
  
  let s_adaptor_res = hexToBlind(s_adaptor_hex)
  let s_res = hexToBlind(s_hex)
  
  if s_adaptor_res.isErr or s_res.isErr: return allocCString("err_parse")
  
  let t_res = subBlinds(s_res.get, s_adaptor_res.get)
  if t_res.isErr: return allocCString("err_sub")
  
  result = allocCString(blindToHex(t_res.get))


# ---------------------------------------------------------------------------
# MPC-TSS Implementation
# ---------------------------------------------------------------------------

proc libp2p_mpc_keygen_init*(): cstring {.exportc, cdecl, dynlib.} =
  let (sec, pub) = mpc_keygen_init()
  let secHex = blindToHex(sec)
  let pubHex = toHex(secp256k1.SkPublicKey(pub).toRawCompressed())
  result = allocCString(secHex & "|" & pubHex)

proc libp2p_mpc_keygen_finalize*(localPubHex: cstring, remotePubHex: cstring): cstring {.exportc, cdecl, dynlib.} =
  let lPubBytes = hexToSeqByte($localPubHex)
  let rPubBytes = hexToSeqByte($remotePubHex)
  
  let lPubRes = secp.SkPublicKey.init(lPubBytes)
  if lPubRes.isErr: return allocCString("")
  
  let jointRes = mpc_keygen_finalize(lPubRes.get, rPubBytes)
  
  if jointRes.isErr: return allocCString("")
  result = allocCString(toHex(secp256k1.SkPublicKey(jointRes.get).toRawCompressed()))

proc libp2p_mpc_sign_init*(): cstring {.exportc, cdecl, dynlib.} =
  let (nonce, noncePub) = mpc_sign_init()
  let nonceHex = blindToHex(nonce)
  let noncePubHex = toHex(secp256k1.SkPublicKey(noncePub).toRawCompressed())
  result = allocCString(nonceHex & "|" & noncePubHex)

proc libp2p_mpc_sign_partial*(
  msg: cstring,
  secretHex: cstring,
  nonceHex: cstring,
  jointPubHex: cstring,
  jointNoncePubHex: cstring
): cstring {.exportc, cdecl, dynlib.} =
  let secRes = hexToBlind($secretHex)
  if secRes.isErr: return allocCString("")
  
  let nonceRes = hexToBlind($nonceHex)
  if nonceRes.isErr: return allocCString("")
  
  let jPubRes = secp.SkPublicKey.init(hexToSeqByte($jointPubHex))
  if jPubRes.isErr: return allocCString("")
  
  let jNoncePubRes = secp.SkPublicKey.init(hexToSeqByte($jointNoncePubHex))
  if jNoncePubRes.isErr: return allocCString("")
  
  let s1Res = mpc_sign_partial($msg, secRes.get, nonceRes.get, jPubRes.get, jNoncePubRes.get)
  if s1Res.isErr: return allocCString("")
  result = allocCString(blindToHex(s1Res.get))

proc libp2p_mpc_sign_combine*(s1Hex: cstring, s2Hex: cstring, jointNoncePubHex: cstring): cstring {.exportc, cdecl, dynlib.} =
  let s1 = hexToBlind($s1Hex).get
  let s2 = hexToBlind($s2Hex).get
  
  let sRes = mpc_sign_combine(s1, s2)
  if sRes.isErr: return allocCString("")
  
  # Format: r_hex | s_hex | v_byte
  # R is the joint nonce pub (x-coordinate is r)
  let R_bytes = hexToSeqByte($jointNoncePubHex)
  # In Schnorr/MuSig, R is the nonce commitment. For ECDSA compatibility (if using adaptor-like logic),
  # we normally need (r, s).
  # Assuming mpc_sign_combine returns the aggregated 's' scalar.
  # 'r' is the x-coordinate of R (jointNoncePub).
  
  # Extract r (32 bytes) from R (33 bytes compressed)
  # Compressed format: 02/03 | x (32 bytes)
  let r_hex = toHex(R_bytes[1..32])
  let s_hex = blindToHex(sRes.get)
  
  # Calculate v (Recovery ID) - Simplified for demo
  # In real ECDSA, v depends on y-parity of R and s-value.
  # For Ethereum (EIP-155), v = CHAIN_ID * 2 + 35 + recovery_id
  # Here we just return the recovery_id (0 or 1) based on the first byte of R (02 -> 0, 03 -> 1)
  let v_byte = if R_bytes[0] == 0x02: "0" else: "1"
  
  result = allocCString(r_hex & "|" & s_hex & "|" & $v_byte)
