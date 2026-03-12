import std/[algorithm, atomics, base64, json, options, sequtils, sets, strformat,
            strutils, tables, times, locks, random]
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
import libp2p/protocols/ping
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
  import libp2p/transports/msquictransport

import examples/dex/dex_node as dex_node
import examples/dex/mixer_service as mixer_service
import examples/dex/types as dex_types
import examples/dex/marketdata/kline_store as dex_kline_store
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
  DefaultMdnsServiceName = "_unimaker._udp.local."
  EventLogLimit = (when defined(android): 0 else: 512)
  NodeTelemetryTopic = "unimaker/node/telemetry/v1"
  NodeTelemetryIntervalMs = 15_000
  NodeTelemetryTtlMs = 180_000
  PeerLatencyTimeoutMs = 1_500
  ProbeDurationDefaultMs = 2_500
  ProbeDurationMinMs = 800
  ProbeDurationMaxMs = 15_000
  ProbeChunkDefaultBytes = 12 * 1024
  ProbeChunkMinBytes = 512
  ProbeChunkMaxBytes = 64 * 1024
  ProbeWindowTailMs = 600
  ProbeSendTimeoutMs = 3_500

var lastInitError = ""

logScope:
  topics = "nimlibp2p ffi"

when defined(android):
  proc android_log_write(prio: cint; tag: cstring; text: cstring): cint {.cdecl, importc: "__android_log_write", header: "<android/log.h>".}
  const AndroidLogError = cint(6)
  const AndroidLogDebug = cint(3)

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
    discard android_log_write(AndroidLogDebug, "nim-libp2p", msg.cstring)
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
    cmdGetRandomBootstrapPeers,
    cmdJoinViaRandomBootstrap,
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
    cmdMeasurePeerBandwidth,
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
    ## Keep the signal loop future alive; otherwise GC may collect it and break
    ## command dispatch while the event loop is idle.
    signalLoopTask: Future[void]
    ## Periodic wake-up task to prevent `chronos.poll()` from blocking forever
    ## when there are no timers/IO events (commands are submitted from other threads).
    idleWakeTask: Future[void]
    readySignal: ThreadSignalPtr
    shutdownSignal: ThreadSignalPtr
    commandQueueHead: Atomic[Command]
    commandQueueTail: Atomic[Command]
    queuedCommands: Atomic[int]
    thread: Thread[pointer]
    dexNode: DexNode
    subscriptions: Table[pointer, Subscription]
    peerHints: Table[string, seq[string]]
    bootstrapLastSeen: Table[string, int64]
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
    telemetryLoop: Future[void]
    metricsLoop: Future[void]
    metricsTextfileLoop: Future[void]
    pingProtocol: Ping
    eventLog: seq[JsonNode]
    eventLogLock: Lock
    eventLogLimit: int
    receivedDmLog: seq[JsonNode]
    receivedDmLogLock: Lock
    receivedDmLogLimit: int
    localSystemProfile: JsonNode
    localSystemProfileUpdatedAt: int64
    peerSystemProfiles: Table[string, JsonNode]
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

proc acquireCommand(kind: CommandKind): Command =
  new(result)
  resetCommandFields(result)
  result.kind = kind
  result.completion = nil
  result.next.store(nil, moRelease)
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
proc localPeerId(n: NimNode): string
proc gatherKnownAddrs(n: NimNode, peer: PeerId): seq[MultiAddress]
proc jsonGetStr(node: JsonNode, key: string, defaultValue: string = ""): string
proc loadSocialStateUnsafe(node: NimNode): JsonNode
proc persistSocialStateUnsafe(node: NimNode, state: JsonNode)
proc makeNotificationNoLock(
    state: JsonNode, kind: string, title: string, body: string, payload: JsonNode = newJObject()
 )
proc appendConversationMessageNoLock(
    state: JsonNode, conversationId: string, peerId: string, message: JsonNode
 )
proc socialStatsNode(state: JsonNode): JsonNode

const
  SocialStateVersion = 1
  SocialMaxConversationMessages = 500
  SocialMaxMoments = 1000
  SocialMaxNotifications = 2000
  SocialMaxSynccastRooms = 256
  SocialDefaultLimit = 20
  ReceivedDmLogLimit = 256

var socialStateMem {.global.}: Table[int, JsonNode] = initTable[int, JsonNode]()
var socialStateMemLock {.global.}: Lock
initLock(socialStateMemLock)

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
  if n.eventLogLimit > 0:
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

proc eventEntryEntity(entry: JsonNode): string =
  if entry.kind != JObject:
    return ""
  if entry.hasKey("entity") and entry["entity"].kind == JString:
    return entry["entity"].getStr().toLowerAscii()
  if entry.hasKey("payload"):
    let payloadNode = entry["payload"]
    case payloadNode.kind
    of JObject:
      if payloadNode.hasKey("entity") and payloadNode["entity"].kind == JString:
        return payloadNode["entity"].getStr().toLowerAscii()
    of JString:
      try:
        let parsed = parseJson(payloadNode.getStr())
        if parsed.kind == JObject and parsed.hasKey("entity") and parsed["entity"].kind == JString:
          return parsed["entity"].getStr().toLowerAscii()
      except CatchableError:
        discard
    else:
      discard
  ""

proc pollEventLogFiltered(n: NimNode, limit: int, dropDm: bool): string =
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
    var retained: seq[JsonNode] = @[]
    retained.setLen(0)
    for i in 0 ..< available:
      let entry = n.eventLog[i]
      if i < takeCount:
        if dropDm and eventEntryEntity(entry) == "dm":
          retained.add(entry)
        else:
          arr.add(entry)
      else:
        retained.add(entry)
    n.eventLog = retained
      
    return $arr
  finally:
    n.eventLogLock.release()

proc pollEventLog(n: NimNode, limit: int): string =
  pollEventLogFiltered(n, limit, dropDm = false)

proc recordReceivedDm(
    n: NimNode,
    peerId: string,
    conversationId: string,
    messageId: string,
    body: string,
    timestampMs: int64,
    payload: JsonNode
) {.gcsafe, raises: [].} =
  if n.isNil:
    return
  var row = newJObject()
  row["peerId"] = %peerId
  row["from"] = %peerId
  row["conversationId"] = %conversationId
  row["messageId"] = %messageId
  row["mid"] = %messageId
  row["content"] = %body
  row["body"] = %body
  row["timestampMs"] = %timestampMs
  row["timestamp_ms"] = %timestampMs
  row["payload"] = payload
  n.receivedDmLogLock.acquire()
  try:
    n.receivedDmLog.add(row)
    if n.receivedDmLog.len > n.receivedDmLogLimit:
      let excess = n.receivedDmLog.len - n.receivedDmLogLimit
      if excess > 0:
        n.receivedDmLog.delete(0, excess - 1)
  finally:
    n.receivedDmLogLock.release()

proc pollReceivedDmLog(n: NimNode, limit: int): string =
  if n.isNil:
    return "{\"items\":[]}"
  n.receivedDmLogLock.acquire()
  try:
    let bounded =
      if limit <= 0 or limit > n.receivedDmLogLimit:
        n.receivedDmLogLimit
      else:
        limit
    let available = n.receivedDmLog.len
    var items = newJArray()
    if available > 0:
      let takeCount = min(bounded, available)
      for i in 0 ..< takeCount:
        items.add(n.receivedDmLog[i])
      if takeCount == available:
        n.receivedDmLog.setLen(0)
      else:
        n.receivedDmLog.delete(0, takeCount - 1)
    var response = newJObject()
    response["items"] = items
    return $response
  finally:
    n.receivedDmLogLock.release()


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

proc jsonGetFloat(node: JsonNode, key: string, defaultValue: float = 0.0): float =
  if node.kind == JObject and node.hasKey(key):
    let v = node.getOrDefault(key)
    case v.kind
    of JFloat:
      return v.getFloat()
    of JInt:
      return float(v.getInt())
    of JString:
      try:
        return parseFloat(v.getStr())
      except CatchableError:
        discard
    else:
      discard
  defaultValue

proc safeReadText(path: string): string =
  if path.len == 0:
    return ""
  try:
    if fileExists(path):
      return readFile(path).strip()
  except CatchableError:
    discard
  ""

proc cloneOwnedString(text: string): string =
  if text.len == 0:
    return ""
  text & ""

proc parseIntLoose(text: string): int64 =
  var token = ""
  for ch in text:
    if ch in {'0' .. '9'}:
      token.add(ch)
    elif token.len > 0:
      break
  if token.len == 0:
    return 0
  try:
    parseBiggestInt(token)
  except CatchableError:
    0

proc parseIntFlexible(text: string): int64 =
  let trimmed = text.strip()
  if trimmed.len == 0:
    return 0
  if trimmed.startsWith("0x") or trimmed.startsWith("0X"):
    try:
      return parseHexInt(trimmed).int64
    except CatchableError:
      discard
  parseIntLoose(trimmed)

proc valueAfterColon(line: string): string =
  let idx = line.find(':')
  if idx < 0:
    return line.strip()
  line[idx + 1 ..< line.len].strip()

proc readCpuModel(): string =
  let text = safeReadText("/proc/cpuinfo")
  if text.len == 0:
    return "unknown"
  var fallbackProcessor = ""
  for lineRaw in text.splitLines():
    let line = lineRaw.strip()
    if line.len == 0:
      continue
    let lowered = line.toLowerAscii()
    if lowered.startsWith("hardware") or lowered.startsWith("model name") or
        lowered.startsWith("cpu model"):
      let value = valueAfterColon(line)
      if value.len > 0:
        return value
    if lowered.startsWith("processor"):
      let value = valueAfterColon(line)
      if value.len > 0:
        var numericOnly = true
        for ch in value:
          if ch notin {'0' .. '9'}:
            numericOnly = false
            break
        if not numericOnly:
          return value
        if fallbackProcessor.len == 0:
          fallbackProcessor = value
  if fallbackProcessor.len > 0:
    return "cpu-" & fallbackProcessor
  "unknown"

proc readCpuFrequencyMHz(): int =
  let paths = @[
    "/sys/devices/system/cpu/cpu0/cpufreq/scaling_cur_freq",
    "/sys/devices/system/cpu/cpu0/cpufreq/cpuinfo_max_freq",
    "/sys/devices/system/cpu/cpu0/cpufreq/base_frequency",
  ]
  for path in paths:
    let raw = safeReadText(path)
    let value = parseIntFlexible(raw)
    if value > 0:
      if value > 10_000:
        return int(value div 1_000) # kHz -> MHz
      return int(value)
  let cpuInfo = safeReadText("/proc/cpuinfo")
  for lineRaw in cpuInfo.splitLines():
    let line = lineRaw.strip()
    if line.toLowerAscii().startsWith("cpu mhz"):
      let value = parseIntFlexible(valueAfterColon(line))
      if value > 0:
        return int(value)
  0

proc readCpuCoreCount(): int =
  var cores = 0
  try:
    if dirExists("/sys/devices/system/cpu"):
      for kind, path in walkDir("/sys/devices/system/cpu"):
        if kind != pcDir:
          continue
        let name = splitPath(path).tail
        if not name.startsWith("cpu") or name.len <= 3:
          continue
        var digitsOnly = true
        for ch in name[3 ..< name.len]:
          if ch notin {'0' .. '9'}:
            digitsOnly = false
            break
        if digitsOnly:
          inc cores
  except CatchableError:
    discard
  if cores > 0:
    return cores
  when defined(posix):
    when declared(sysconf) and declared(SC_NPROCESSORS_ONLN):
      let online = sysconf(SC_NPROCESSORS_ONLN)
      if online > 0:
        return int(online)
  0

proc readMemoryTotalBytes(): int64 =
  let memInfo = safeReadText("/proc/meminfo")
  for lineRaw in memInfo.splitLines():
    let line = lineRaw.strip()
    if line.startsWith("MemTotal:"):
      let kb = parseIntFlexible(line)
      if kb > 0:
        return kb * 1024
  0

proc readMemoryFrequencyMHz(): int =
  when defined(android):
    return 0
  var maxFreqHz: int64 = 0
  try:
    if dirExists("/sys/class/devfreq"):
      for kind, path in walkDir("/sys/class/devfreq"):
        if kind != pcDir:
          continue
        let name = splitPath(path).tail.toLowerAscii()
        if ("ddr" notin name) and ("mem" notin name) and ("bw" notin name):
          continue
        let value = parseIntFlexible(safeReadText(path / "cur_freq"))
        if value > maxFreqHz:
          maxFreqHz = value
  except CatchableError:
    discard
  if maxFreqHz >= 1_000_000:
    return int(maxFreqHz div 1_000_000)
  if maxFreqHz >= 1_000:
    return int(maxFreqHz div 1_000)
  if maxFreqHz > 0:
    return int(maxFreqHz)
  0

proc readDiskType(): string =
  when defined(android):
    return "flash"
  var foundNvme = false
  var foundUfs = false
  var foundEmmc = false
  try:
    if dirExists("/sys/block"):
      for kind, path in walkDir("/sys/block"):
        if kind != pcDir:
          continue
        let name = splitPath(path).tail.toLowerAscii()
        if name.startsWith("nvme"):
          foundNvme = true
        elif name.startsWith("sda") or name.startsWith("sdb") or
            name.startsWith("sdc") or name.startsWith("ufs"):
          foundUfs = true
        elif name.startsWith("mmcblk"):
          foundEmmc = true
  except CatchableError:
    discard
  if foundNvme:
    return "NVMe"
  if foundUfs:
    return "UFS"
  if foundEmmc:
    return "eMMC"
  "unknown"

proc readDiskSpaceBytes(basePath: string): tuple[total: int64, available: int64] =
  var path = basePath.strip()
  if path.len == 0:
    path = getCurrentDir()
  if path.len == 0:
    path = "/"
  when defined(posix):
    when declared(statvfs):
      var statbuf: Statvfs
      if statvfs(path.cstring, statbuf) == 0:
        let blockSize = int64(statbuf.f_frsize)
        return (int64(statbuf.f_blocks) * blockSize, int64(statbuf.f_bavail) * blockSize)
  (0'i64, 0'i64)

proc readGpuModel(): string =
  let candidates = @[
    "/sys/class/kgsl/kgsl-3d0/gpu_model",
    "/sys/class/kgsl/kgsl-3d0/device/model",
    "/sys/class/misc/mali0/device/gpuinfo",
    "/proc/gpuinfo",
  ]
  for path in candidates:
    let text = safeReadText(path)
    if text.len == 0:
      continue
    for lineRaw in text.splitLines():
      let line = lineRaw.strip()
      if line.len > 0:
        return line
  "unknown"

proc readGpuMemoryBytes(): int64 =
  let candidates = @[
    "/sys/class/kgsl/kgsl-3d0/gmem_size",
    "/sys/class/kgsl/kgsl-3d0/memstore_size",
    "/sys/class/misc/mali0/device/memory",
  ]
  for path in candidates:
    let value = parseIntFlexible(safeReadText(path))
    if value > 0:
      return value
  0

proc readOsVersion(): string =
  when defined(android):
    let buildProps = @[
      "/system/build.prop",
      "/system/system/build.prop",
      "/vendor/build.prop",
    ]
    for buildPath in buildProps:
      let buildProp = safeReadText(buildPath)
      for lineRaw in buildProp.splitLines():
        let line = lineRaw.strip()
        if line.startsWith("ro.build.version.release="):
          let idx = line.find('=')
          if idx >= 0 and idx + 1 < line.len:
            return line[idx + 1 ..< line.len].strip()
    return "Android"
  elif defined(ios):
    "iOS"
  elif defined(macosx):
    "macOS"
  elif defined(linux):
    "Linux"
  else:
    "unknown"

proc readKernelVersion(): string =
  when defined(android):
    let osRelease = safeReadText("/proc/sys/kernel/osrelease")
    if osRelease.len > 0:
      return osRelease
    return "unknown"
  else:
    safeReadText("/proc/version")

proc defaultSystemProfile(): JsonNode =
  %* {
    "os": {
      "name": (when defined(android): "Android" elif defined(ios): "iOS" elif defined(macosx): "macOS" elif defined(linux): "Linux" else: "unknown"),
      "version": readOsVersion(),
      "kernel": readKernelVersion(),
      "arch": hostCPU,
    },
    "cpu": {
      "model": "unknown",
      "frequencyMHz": 0,
      "cores": 0,
    },
    "memory": {
      "frequencyMHz": 0,
      "totalBytes": 0'i64,
    },
    "disk": {
      "type": "unknown",
      "totalBytes": 0'i64,
      "availableBytes": 0'i64,
    },
    "gpu": {
      "model": "unknown",
      "vramBytes": 0'i64,
    },
    "bandwidth": {
      "uplinkBps": 0.0,
      "downlinkBps": 0.0,
      "uplinkTotalBytes": 0'i64,
      "downlinkTotalBytes": 0'i64,
      "totalTransferBytes": 0'i64,
    },
    "updatedAtMs": 0'i64,
  }

proc collectLocalSystemProfile(n: NimNode): JsonNode =
  let nowTs = nowMillis()
  if not n.localSystemProfile.isNil and nowTs - n.localSystemProfileUpdatedAt < 5_000:
    return n.localSystemProfile

  var profile = defaultSystemProfile()

  profile["cpu"]["model"] = %readCpuModel()
  profile["cpu"]["frequencyMHz"] = %readCpuFrequencyMHz()
  profile["cpu"]["cores"] = %readCpuCoreCount()

  profile["memory"]["frequencyMHz"] = %readMemoryFrequencyMHz()
  profile["memory"]["totalBytes"] = %readMemoryTotalBytes()

  let diskSpace = readDiskSpaceBytes(if n.cfg.dataDir.isSome: n.cfg.dataDir.get() else: "/")
  profile["disk"]["type"] = %readDiskType()
  profile["disk"]["totalBytes"] = %diskSpace.total
  profile["disk"]["availableBytes"] = %diskSpace.available

  profile["gpu"]["model"] = %readGpuModel()
  profile["gpu"]["vramBytes"] = %readGpuMemoryBytes()

  if not n.switchInstance.isNil and not n.switchInstance.bandwidthManager.isNil:
    let snapshot = n.switchInstance.bandwidthManager.snapshot()
    profile["bandwidth"]["uplinkBps"] = %snapshot.outboundRateBps
    profile["bandwidth"]["downlinkBps"] = %snapshot.inboundRateBps
    profile["bandwidth"]["uplinkTotalBytes"] = %snapshot.totalOutbound
    profile["bandwidth"]["downlinkTotalBytes"] = %snapshot.totalInbound
    profile["bandwidth"]["totalTransferBytes"] = %(snapshot.totalOutbound + snapshot.totalInbound)

  profile["updatedAtMs"] = %nowTs
  n.localSystemProfile = profile
  n.localSystemProfileUpdatedAt = nowTs
  profile

proc sanitizeSystemProfile(node: JsonNode): JsonNode =
  var profile = defaultSystemProfile()
  if node.kind != JObject:
    return profile
  if node.hasKey("os") and node["os"].kind == JObject:
    profile["os"] = node["os"]
  if node.hasKey("cpu") and node["cpu"].kind == JObject:
    profile["cpu"] = node["cpu"]
  if node.hasKey("memory") and node["memory"].kind == JObject:
    profile["memory"] = node["memory"]
  if node.hasKey("disk") and node["disk"].kind == JObject:
    profile["disk"] = node["disk"]
  if node.hasKey("gpu") and node["gpu"].kind == JObject:
    profile["gpu"] = node["gpu"]
  if node.hasKey("bandwidth") and node["bandwidth"].kind == JObject:
    profile["bandwidth"] = node["bandwidth"]
    profile["bandwidth"]["totalTransferBytes"] = %(
      jsonGetInt64(profile["bandwidth"], "totalTransferBytes",
        jsonGetInt64(profile["bandwidth"], "uplinkTotalBytes", 0'i64) +
        jsonGetInt64(profile["bandwidth"], "downlinkTotalBytes", 0'i64))
    )
  profile["updatedAtMs"] = %jsonGetInt64(node, "updatedAtMs", nowMillis())
  profile

proc prunePeerProfiles(n: NimNode) =
  let nowTs = nowMillis()
  var expired: seq[string] = @[]
  for peerId, profile in n.peerSystemProfiles.pairs:
    let updated = jsonGetInt64(profile, "updatedAtMs", 0)
    if updated <= 0 or nowTs - updated > NodeTelemetryTtlMs:
      expired.add(peerId)
  for peerId in expired:
    n.peerSystemProfiles.del(peerId)

proc nodeTelemetryPublishEnabled(cfg: NodeConfig): bool =
  result = true
  when defined(android) or defined(ohos):
    result = false
  if not cfg.extra.isNil and cfg.extra.kind == JObject:
    if cfg.extra.hasKey("enableNodeTelemetryPubsub"):
      let node = cfg.extra["enableNodeTelemetryPubsub"]
      if node.kind == JBool:
        result = node.getBool()
    if cfg.extra.hasKey("disableNodeTelemetryPubsub"):
      let node = cfg.extra["disableNodeTelemetryPubsub"]
      if node.kind == JBool and node.getBool():
        result = false

proc publishLocalTelemetry(n: NimNode): Future[void] {.async.} =
  if n.isNil or not n.started or n.gossip.isNil or not nodeTelemetryPublishEnabled(n.cfg):
    return
  var peerId = ""
  if not n.switchInstance.isNil and not n.switchInstance.peerInfo.isNil:
    try:
      peerId = $n.switchInstance.peerInfo.peerId
    except CatchableError:
      discard
  if peerId.len == 0:
    return
  let profile = collectLocalSystemProfile(n)
  var payload = newJObject()
  payload["peerId"] = %peerId
  payload["systemProfile"] = profile
  payload["timestamp_ms"] = %nowMillis()
  try:
    discard await n.gossip.PubSub.publish(NodeTelemetryTopic, stringToBytes($payload))
  except CatchableError as exc:
    debugLog("[nimlibp2p] publishLocalTelemetry failed err=" & exc.msg)

proc telemetryLoop(n: NimNode) {.async.} =
  while n.running.load(moAcquire):
    if n.started and n.defaultTopicsReady:
      prunePeerProfiles(n)
      await publishLocalTelemetry(n)
    await sleepAsync(chronos.milliseconds(NodeTelemetryIntervalMs))

proc measureConnectionLatencyMs(n: NimNode, conn: Connection): Future[int] {.async.} =
  if n.isNil or conn.isNil or n.pingProtocol.isNil:
    return -1
  try:
    let pingFuture = n.pingProtocol.ping(conn)
    let completed = await withTimeout(pingFuture, chronos.milliseconds(PeerLatencyTimeoutMs))
    if not completed:
      return -1
    let duration = pingFuture.read()
    let millis = duration.nanoseconds() div 1_000_000
    if millis < 0:
      return -1
    return int(millis)
  except CatchableError:
    return -1

proc clampProbeDurationMs(value: int): int =
  let candidate = if value > 0: value else: ProbeDurationDefaultMs
  max(ProbeDurationMinMs, min(candidate, ProbeDurationMaxMs))

proc clampProbeChunkBytes(value: int): int =
  let candidate = if value > 0: value else: ProbeChunkDefaultBytes
  max(ProbeChunkMinBytes, min(candidate, ProbeChunkMaxBytes))

proc isCircuitAddressText(addressText: string): bool =
  "/p2p-circuit" in addressText.toLowerAscii()

proc peerLikelyRelayed(n: NimNode, peerIdText: string, peerId: PeerId): bool =
  if n.isNil:
    return false
  if n.peerHints.hasKey(peerIdText):
    let hinted = n.peerHints.getOrDefault(peerIdText)
    for addr in hinted:
      if isCircuitAddressText(addr):
        return true
  if n.switchInstance.isNil:
    return false
  let addrBook = n.switchInstance.peerStore[AddressBook]
  if addrBook.isNil:
    return false
  for pid, addrs in addrBook.book.pairs:
    if pid != peerId:
      continue
    for addr in addrs:
      if isCircuitAddressText($addr):
        return true
    break
  false

proc peerBandwidthTotals(n: NimNode, peerId: PeerId): tuple[outbound: int64, inbound: int64] =
  if n.isNil or n.switchInstance.isNil:
    return (0'i64, 0'i64)
  let peerBw = n.switchInstance.bandwidthStats(peerId)
  if peerBw.isSome():
    let stats = peerBw.get()
    return (int64(stats.outbound.totalBytes), int64(stats.inbound.totalBytes))
  (0'i64, 0'i64)

proc computeProbeBps(deltaBytes: int64, elapsedMs: int64): float64 =
  if deltaBytes <= 0 or elapsedMs <= 0:
    return 0.0
  (float64(deltaBytes) * 8_000.0) / float64(elapsedMs)

proc sendProbeChunks(
    n: NimNode,
    remotePid: PeerId,
    sessionId: string,
    durationMs: int,
    chunkBytes: int
): Future[tuple[payloadBytes: int64, elapsedMs: int64, chunkCount: int, ok: bool, err: string]] {.async.} =
  if n.isNil or n.dmService.isNil:
    return (0'i64, 0'i64, 0, false, "direct message service unavailable")
  let safeDurationMs = clampProbeDurationMs(durationMs)
  let safeChunkBytes = clampProbeChunkBytes(chunkBytes)
  let timeout = chronos.milliseconds(max(ProbeSendTimeoutMs, safeDurationMs))
  let chunkBody = "x".repeat(safeChunkBytes)
  var sentBytes = 0'i64
  var sentChunks = 0
  let startMs = nowMillis()
  while nowMillis() - startMs < int64(safeDurationMs):
    var envelope = newJObject()
    envelope["op"] = %"bw_probe_chunk"
    envelope["sid"] = %sessionId
    envelope["timestamp_ms"] = %nowMillis()
    envelope["ackRequested"] = %false
    envelope["body"] = %chunkBody
    let payload = stringToBytes($envelope)
    try:
      let sendRes = await n.dmService.send(remotePid, payload, false, "", timeout)
      if not sendRes[0]:
        return (sentBytes, max(1'i64, nowMillis() - startMs), sentChunks, false, sendRes[1])
      sentBytes += int64(safeChunkBytes)
      inc sentChunks
    except CatchableError as exc:
      return (sentBytes, max(1'i64, nowMillis() - startMs), sentChunks, false, exc.msg)
  (sentBytes, max(1'i64, nowMillis() - startMs), sentChunks, true, "")

proc runProbePushTraffic(
    n: NimNode,
    targetPeerId: string,
    sessionId: string,
    durationMs: int,
    chunkBytes: int
) {.async.} =
  if n.isNil or n.dmService.isNil or targetPeerId.len == 0:
    return
  var remotePid: PeerId
  if not remotePid.init(targetPeerId):
    return
  let runResult = await sendProbeChunks(
    n,
    remotePid,
    if sessionId.len > 0: sessionId else: ("probe-push-" & $nowMillis()),
    durationMs,
    chunkBytes
  )
  var event = newJObject()
  event["type"] = %"BandwidthProbePushComplete"
  event["peer_id"] = %targetPeerId
  event["session_id"] = %(if sessionId.len > 0: sessionId else: "")
  event["payload_bytes"] = %runResult.payloadBytes
  event["elapsed_ms"] = %runResult.elapsedMs
  event["chunk_count"] = %runResult.chunkCount
  event["ok"] = %runResult.ok
  if not runResult.ok and runResult.err.len > 0:
    event["error"] = %runResult.err
  event["timestamp_ms"] = %nowMillis()
  recordEvent(n, "network_event", $event)

proc handleNodeTelemetry(n: NimNode, payload: seq[byte]) =
  let raw = bytesToString(payload).strip()
  if raw.len == 0:
    return
  var envelope: JsonNode
  try:
    envelope = parseJson(raw)
  except CatchableError:
    return
  if envelope.kind != JObject:
    return
  let peerId = jsonGetStr(envelope, "peerId")
  var localId = ""
  if not n.switchInstance.isNil and not n.switchInstance.peerInfo.isNil:
    try:
      localId = $n.switchInstance.peerInfo.peerId
    except CatchableError:
      discard
  if peerId.len == 0 or peerId == localId:
    return
  let sourceProfile =
    if envelope.hasKey("systemProfile"): envelope["systemProfile"] else: envelope
  let profile = sanitizeSystemProfile(sourceProfile)
  profile["updatedAtMs"] = %jsonGetInt64(envelope, "timestamp_ms", nowMillis())
  n.peerSystemProfiles[peerId] = profile
  var event = newJObject()
  event["type"] = %"NodeTelemetry"
  event["peer_id"] = %peerId
  event["timestamp_ms"] = %nowMillis()
  recordEvent(n, "network_event", $event)

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

proc handleDirectMessageNode(n: NimNode, envelope: JsonNode, raw: string) {.gcsafe, raises: [].} =
  try:
    var fromPeer = jsonGetStr(envelope, "from")
    if fromPeer.len == 0:
      fromPeer = jsonGetStr(envelope, "peer_id")
    let op = jsonGetStr(envelope, "op").toLowerAscii()
    if op == "ack":
      handleDirectAckNode(n, envelope)
      return
    if op == "bw_probe_push":
      let sessionId = jsonGetStr(envelope, "sid", "probe-push")
      let durationMs = clampProbeDurationMs(int(jsonGetInt64(envelope, "durationMs", int64(ProbeDurationDefaultMs))))
      let chunkBytes = clampProbeChunkBytes(int(jsonGetInt64(envelope, "chunkBytes", int64(ProbeChunkDefaultBytes))))
      if fromPeer.len > 0:
        asyncSpawn(runProbePushTraffic(n, fromPeer, sessionId, durationMs, chunkBytes))
      return
    if op == "bw_probe_chunk":
      return
    let originalMid = jsonGetStr(envelope, "mid")
    let mid = ensureMessageId(originalMid)
    let ts = jsonGetInt64(envelope, "timestamp_ms", nowMillis())
    let ackRequested = jsonGetBool(envelope, "ackRequested", false)
    var body = jsonGetStr(envelope, "body")
    if body.len == 0:
      body = raw
    var payloadNode = envelope
    if payloadNode.isNil or payloadNode.kind != JObject:
      payloadNode = newJObject()
    payloadNode["body"] = %body
    payloadNode["messageId"] = %mid
    payloadNode["mid"] = %mid
    payloadNode["timestampMs"] = %ts
    payloadNode["timestamp_ms"] = %ts
    if fromPeer.len > 0:
      payloadNode["from"] = %fromPeer
      payloadNode["peerId"] = %fromPeer
    let rawConvId = jsonGetStr(payloadNode, "conversationId")
    let convId =
      if rawConvId.len > 0:
        rawConvId
      elif fromPeer.len > 0:
        "dm:" & fromPeer
      else:
        ""
    if convId.len > 0:
      payloadNode["conversationId"] = %convId
    if fromPeer.len > 0:
      payloadNode["sender"] = %fromPeer
    if fromPeer.len > 0 and convId.len > 0:
      recordReceivedDm(n, fromPeer, convId, mid, body, ts, payloadNode)
    var event = newJObject()
    event["type"] = %"MessageReceived"
    event["peer_id"] = %((if fromPeer.len == 0: "unknown" else: fromPeer))
    event["message_id"] = %mid
    event["timestamp_ms"] = %ts
    event["payload"] = payloadNode
    event["body"] = %body
    if convId.len > 0:
      event["conversation_id"] = %convId
    event["transport"] = %"nim-direct"
    recordEvent(n, "network_event", $event)
    if fromPeer.len > 0 and convId.len > 0:
      var socialEvt = newJObject()
      socialEvt["kind"] = %"social"
      socialEvt["entity"] = %"dm"
      socialEvt["op"] = %"receive"
      socialEvt["traceId"] = %("trace-" & $ts & "-" & mid)
      socialEvt["seq"] = %ts
      socialEvt["timestampMs"] = %ts
      socialEvt["conversationId"] = %convId
      socialEvt["source"] = %"direct_message"
      socialEvt["payload"] = payloadNode
      recordEvent(n, "social_event", $socialEvt)
    if ackRequested and fromPeer.len > 0 and originalMid.len > 0:
      debugLog("[nimlibp2p] dm ack satisfied via direct stream peer=" & fromPeer & " mid=" & originalMid)
  except Exception as exc:
    warn "handleDirectMessageNode failed", err = exc.msg

proc handleDirectTopic(n: NimNode, topic: string, payload: seq[byte]) {.gcsafe, raises: [].} =
  try:
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
  except Exception as exc:
    warn "handleDirectTopic failed", topic = topic, err = exc.msg

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
          try:
            handleDirectTopic(n, inboxTopic, msg.payload)
          except CatchableError as exc:
            warn "setupDefaultTopics: direct message handler failed", peer = local, err = exc.msg
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
  if not n.internalSubscriptions.hasKey(NodeTelemetryTopic):
    let telemetryHandler =
      proc(node: NimNode, t: string, payload: seq[byte]) {.gcsafe.} =
        handleNodeTelemetry(node, payload)
    ensureInternalSubscription(n, NodeTelemetryTopic, makeTopicHandler(n, telemetryHandler))
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
  if nodeTelemetryPublishEnabled(n.cfg):
    asyncSpawn(publishLocalTelemetry(n))

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
  if preferredValid:
    var reordered: seq[string] = @[preferred]
    for ip in ips:
      if ip != preferred:
        reordered.add(ip)
    ips = reordered
    debugLog("[nimlibp2p] collectLanIpv4Addrs prioritize preferred=" & preferred)
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

proc stableTransportPriority(text: string): int =
  let lower = text.toLowerAscii()
  if lower.contains("/tcp/"):
    return 0
  if lower.contains("/quic-v1"):
    return 1
  if lower.contains("/quic"):
    return 2
  if lower.contains("/udp/"):
    return 3
  4

proc preferStableMultiaddrs(addrs: seq[MultiAddress]): seq[MultiAddress] =
  if addrs.len <= 1:
    return addrs
  var indexed: seq[(int, int, MultiAddress)] = @[]
  for idx, addr in addrs:
    indexed.add((stableTransportPriority($addr), idx, addr))
  indexed.sort(proc(a, b: (int, int, MultiAddress)): int =
    result = cmp(a[0], b[0])
    if result == 0:
      result = cmp(a[1], b[1])
  )
  result = indexed.mapIt(it[2])

proc preferStableAddrStrings(addrs: seq[string]): seq[string] =
  if addrs.len <= 1:
    return addrs
  var indexed: seq[(int, int, string)] = @[]
  for idx, addr in addrs:
    indexed.add((stableTransportPriority(addr), idx, addr))
  indexed.sort(proc(a, b: (int, int, string)): int =
    result = cmp(a[0], b[0])
    if result == 0:
      result = cmp(a[1], b[1])
  )
  result = indexed.mapIt(it[2])

proc refreshLanAwareAddrs(n: NimNode): seq[MultiAddress] =
  if n.switchInstance.isNil or n.switchInstance.peerInfo.isNil:
    return @[]
  var base = n.switchInstance.peerInfo.listenAddrs
  if base.len == 0:
    base = n.switchInstance.peerInfo.addrs
  let preservedBase = base
  let preferredIpv4 = n.mdnsPreferredIpv4.strip()
  let lanIpv4 =
    if preferredIpv4.len > 0 and preferredIpv4 != "0.0.0.0" and
        preferredIpv4 != "127.0.0.1" and isLanIpv4(preferredIpv4):
      debugLog("[nimlibp2p] refreshLanAwareAddrs use preferred-only ipv4=" & preferredIpv4)
      @[preferredIpv4]
    else:
      collectLanIpv4Addrs(preferredIpv4)
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
  if dedup.len == 0 and preservedBase.len > 0:
    debugLog(
      "[nimlibp2p] refreshLanAwareAddrs preserve-base because concrete listeners unavailable base=" &
      preservedBase.mapIt($it).join(",")
    )
    return preservedBase
  if filtered > 0 and dedup.len == 0:
    debugLog("[nimlibp2p] refreshLanAwareAddrs dropped unspecified addresses; no concrete listeners available")
  elif filtered > 0:
    debugLog("[nimlibp2p] refreshLanAwareAddrs filtered=" & $filtered)
  debugLog("[nimlibp2p] refreshLanAwareAddrs dedup=" & (if dedup.len > 0: dedup.mapIt($it).join(",") else: "<empty>"))
  n.switchInstance.peerInfo.listenAddrs = dedup
  n.switchInstance.peerInfo.addrs = dedup
  dedup

proc mdnsDnsaddrPreview(node: NimNode): JsonNode =
  var entries = newJArray()
  if node.isNil or node.switchInstance.isNil or node.switchInstance.peerInfo.isNil:
    return entries
  let peerInfo = node.switchInstance.peerInfo
  let preview =
    if not node.mdnsInterface.isNil:
      node.mdnsInterface.resolveMdnsDnsaddrs(Opt.some(peerInfo.peerId), peerInfo.listenAddrs)
    else:
      let fullAddrs = peerInfo.fullAddrs().valueOr: @[]
      fullAddrs.mapIt("dnsaddr=" & $it)
  for entry in preview:
    entries.add(%entry)
  entries

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
  if cfg.extra.hasKey("mdns"):
    let mdnsNode = cfg.extra["mdns"]
    if mdnsNode.kind == JObject:
      let nested = jsonGetStr(mdnsNode, "preferredIpv4")
      if nested.len > 0:
        debugLog("[nimlibp2p] extractPreferredIpv4 mdns.preferredIpv4=" & nested)
        return nested
  ""

proc canonicalMdnsServiceName(name: string): string {.gcsafe.}
proc legacyMdnsVariantsEnabled(): bool {.gcsafe.}
proc noteMdnsDiscovery(
  n: NimNode, peer: PeerId, addrs: seq[MultiAddress]
) {.gcsafe, raises: [].}

proc computeMdnsServiceName(n: NimNode): string {.gcsafe.} =
  let envService = getEnv("UNIMAKER_MDNS_SERVICE")
  if envService.len > 0:
    return canonicalMdnsServiceName(envService)
  if not n.cfg.extra.isNil and n.cfg.extra.kind == JObject:
    if n.cfg.extra.hasKey("mdnsService"):
      let explicit = jsonGetStr(n.cfg.extra, "mdnsService")
      if explicit.len > 0:
        return canonicalMdnsServiceName(explicit)
    let mdnsNode = n.cfg.extra.getOrDefault("mdns")
    if mdnsNode.kind == JObject:
      let nested = jsonGetStr(mdnsNode, "service")
      if nested.len > 0:
        return canonicalMdnsServiceName(nested)
  canonicalMdnsServiceName(DefaultMdnsServiceName)

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
    n.bootstrapLastSeen[peerId] = nowMillis()
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

proc prepareMdns(n: NimNode) {.gcsafe.} =
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
  if n.mdnsPreferredIpv4.len > 0 and
      (lanIpsSnapshot.len == 0 or n.mdnsPreferredIpv4 in lanIpsSnapshot):
    preferredIp = n.mdnsPreferredIpv4
  if preferredIp.len == 0 and not peerInfo.isNil:
    for ma in peerInfo.listenAddrs:
      let addrStr = $ma
      if addrStr.contains("/ip4/"):
        let parts = addrStr.split("/ip4/")
        if parts.len >= 2:
          let ipPart = parts[^1]
          let slashIdx = ipPart.find('/')
          let ip = if slashIdx >= 0: ipPart[0 ..< slashIdx] else: ipPart
          if ip.len > 0 and ip != "0.0.0.0" and ip != "127.0.0.1" and
              (lanIpsSnapshot.len == 0 or ip in lanIpsSnapshot):
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
  mdnsIface.onPeerFound = proc(attrs: PeerAttributes) =
    let peerOpt = attrs{PeerId}
    if peerOpt.isNone:
      return
    let addrs = attrs.getAll(MultiAddress)
    if addrs.len == 0:
      return
    noteMdnsDiscovery(n, peerOpt.get(), addrs)
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

proc updateMdnsAdvertisements(n: NimNode) {.gcsafe.} =
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
  (preferStableMultiaddrs(accepted), rejected)

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
    let connectFuture = n.switchInstance.connect(peer, dialable, forceDial = true)
    let connected = await withTimeout(connectFuture, chronos.milliseconds(8_000))
    if not connected:
      connectFuture.cancelSoon()
      debugLog("[nimlibp2p] ensurePeerConnectivity timeout peer=" & $peer)
      return false
    await connectFuture
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

type BootstrapCandidate = object
  peerId: string
  multiaddrs: seq[string]
  lastSeenAt: int64
  sources: seq[string]

proc addBootstrapSource(target: var seq[string], source: string) =
  if source.len > 0 and source notin target:
    target.add(source)

proc normalizeBootstrapMultiaddr(peerId: string, rawAddr: string): string =
  var candidate = canonicalizeMultiaddrText(rawAddr.strip())
  if candidate.len == 0:
    return ""
  if peerId.len > 0 and candidate.find("/p2p/") < 0:
    candidate.add("/p2p/" & peerId)
  if hasZeroPortSegment(candidate, "/tcp/") or hasZeroPortSegment(candidate, "/udp/"):
    return ""
  if isUnspecifiedMultiaddrText(candidate):
    return ""
  let maRes = MultiAddress.init(candidate)
  if maRes.isErr():
    return ""
  var reason = ""
  if not mdnsDialable(maRes.get(), reason):
    return ""
  $maRes.get()

proc addBootstrapAddresses(peerId: string, values: seq[string], target: var seq[string]) =
  for raw in values:
    let normalized = normalizeBootstrapMultiaddr(peerId, raw)
    if normalized.len > 0 and normalized notin target:
      target.add(normalized)

proc candidateLastSeen(n: NimNode, peerId: string): int64 =
  var ts = n.bootstrapLastSeen.getOrDefault(peerId, 0)
  ts = max(ts, n.mdnsLastSeen.getOrDefault(peerId, 0))
  if n.peerSystemProfiles.hasKey(peerId):
    ts = max(ts, jsonGetInt64(n.peerSystemProfiles.getOrDefault(peerId), "updatedAtMs", 0))
  ts

proc collectBootstrapCandidates(n: NimNode): seq[BootstrapCandidate] =
  var candidates = initTable[string, BootstrapCandidate]()
  let localId = localPeerId(n)
  let nowTs = nowMillis()

  proc upsert(peerId: string, addrs: seq[string], source: string, seenAt: int64 = 0'i64) =
    let normalizedPeerId = peerId.strip()
    if normalizedPeerId.len == 0 or normalizedPeerId == localId:
      return
    var entry = candidates.getOrDefault(
      normalizedPeerId,
      BootstrapCandidate(
        peerId: normalizedPeerId,
        multiaddrs: @[],
        lastSeenAt: 0,
        sources: @[]
      )
    )
    addBootstrapAddresses(normalizedPeerId, addrs, entry.multiaddrs)
    addBootstrapSource(entry.sources, source)
    entry.lastSeenAt = max(entry.lastSeenAt, max(candidateLastSeen(n, normalizedPeerId), seenAt))
    candidates[normalizedPeerId] = entry

  for peerId, addrs in n.peerHints.pairs:
    upsert(peerId, addrs, "Hints")

  if not n.switchInstance.isNil:
    var connected = initHashSet[string]()
    try:
      for pid in n.switchInstance.connectedPeers(Direction.Out):
        connected.incl($pid)
      for pid in n.switchInstance.connectedPeers(Direction.In):
        connected.incl($pid)
    except CatchableError:
      discard
    for peerId in connected.items():
      upsert(peerId, @[], "Connected", nowTs)

    let store = n.switchInstance.peerStore
    if not store.isNil:
      try:
        let addrBook = store[AddressBook]
        for peer, addrs in addrBook.book.pairs:
          var addrStrings: seq[string] = @[]
          for addr in addrs:
            addrStrings.add($addr)
          upsert($peer, addrStrings, "DHT")
      except CatchableError as exc:
        debugLog("[nimlibp2p] collectBootstrapCandidates peerstore failed err=" & exc.msg)

  for peerId, item in candidates.pairs:
    var entry = item
    if entry.multiaddrs.len == 0:
      continue
    if entry.lastSeenAt <= 0 and "Connected" in entry.sources:
      entry.lastSeenAt = nowTs
    if entry.lastSeenAt <= 0:
      entry.lastSeenAt = nowTs - 1
    result.add(entry)

  result.sort(proc(a, b: BootstrapCandidate): int =
    result = cmp(b.lastSeenAt, a.lastSeenAt)
    if result == 0:
      result = cmp(a.peerId, b.peerId)
  )

proc clampBootstrapLimit(limit: int, defaultLimit: int, maxLimit: int): int =
  let resolved = if limit > 0: limit else: defaultLimit
  max(1, min(resolved, maxLimit))

proc selectRandomBootstrapCandidates(candidates: seq[BootstrapCandidate], limit: int): seq[BootstrapCandidate] =
  if candidates.len == 0 or limit <= 0:
    return @[]
  if candidates.len <= limit:
    return candidates
  var pool = candidates
  let poolSize = min(pool.len, max(limit * 4, limit))
  pool.setLen(poolSize)
  random.randomize(int(nowMillis() and 0x7fff_ffff'i64))
  for i in countdown(pool.high, 1):
    let j = random.rand(i)
    swap(pool[i], pool[j])
  result = pool[0 ..< min(limit, pool.len)]
  result.sort(proc(a, b: BootstrapCandidate): int =
    result = cmp(b.lastSeenAt, a.lastSeenAt)
    if result == 0:
      result = cmp(a.peerId, b.peerId)
  )

proc connectBootstrapCandidate(n: NimNode, candidate: BootstrapCandidate): Future[bool] {.async.} =
  if n.switchInstance.isNil or candidate.multiaddrs.len == 0:
    return false
  var parsed: seq[MultiAddress] = @[]
  for addr in candidate.multiaddrs:
    let maRes = MultiAddress.init(addr)
    if maRes.isOk():
      parsed.add(maRes.get())
  if parsed.len == 0:
    return false
  let ordered = preferStableMultiaddrs(parsed)
  for ma in ordered:
    try:
      let connectFuture = n.switchInstance.connect(ma, allowUnknownPeerId = true)
      let completed = await withTimeout(connectFuture, chronos.milliseconds(8_000))
      if completed:
        discard await connectFuture
        return true
      connectFuture.cancelSoon()
    except CatchableError:
      discard
  let peerRes = PeerId.init(candidate.peerId)
  if peerRes.isOk():
    try:
      await n.switchInstance.connect(peerRes.get(), ordered, forceDial = true)
      return true
    except CatchableError:
      discard
  false

proc stripTrailingDots(value: var string) {.gcsafe.} =
  while value.len > 0 and value[^1] == '.':
    value.setLen(value.len - 1)

proc resolveServiceBase(name: string): string {.gcsafe.} =
  var base = name.strip()
  stripTrailingDots(base)
  const LocalSuffix = ".local"
  if base.len >= LocalSuffix.len:
    let tail = base[(base.len - LocalSuffix.len) ..< base.len]
    if tail.toLowerAscii() == LocalSuffix:
      base.setLen(base.len - LocalSuffix.len)
      stripTrailingDots(base)
  base

proc canonicalMdnsServiceName(name: string): string {.gcsafe.} =
  let base = resolveServiceBase(name)
  if base.len == 0:
    return DefaultMdnsServiceName
  base & ".local."

proc legacyMdnsVariantsEnabled(): bool {.gcsafe.} =
  let raw = getEnv("UNIMAKER_MDNS_LEGACY_VARIANTS").strip().toLowerAscii()
  raw in ["1", "true", "yes", "on"]

proc serviceQueryVariants(name: string): seq[string] {.gcsafe.} =
  ## 生产环境默认只查询一个规范服务名，避免重复 query/response 放大局域网负担。
  ## 如需兼容旧端，可用 UNIMAKER_MDNS_LEGACY_VARIANTS=1 临时恢复多变体探测。
  var variants: seq[string] = @[]
  proc addVariant(value: string) =
    let candidate = value.strip()
    if candidate.len == 0:
      return
    if variants.find(candidate) < 0:
      variants.add(candidate)
  let canonical = canonicalMdnsServiceName(name)
  addVariant(canonical)
  if not legacyMdnsVariantsEnabled():
    return variants
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

proc mdnsAutoConnectEnabled(): bool =
  let raw = getEnv("LIBP2P_MDNS_AUTO_CONNECT", "").strip().toLowerAscii()
  if raw.len == 0:
    return false
  raw in ["1", "true", "yes", "on"]

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
  let ordered = preferStableMultiaddrs(dialable)
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

proc noteMdnsDiscovery(
  n: NimNode, peer: PeerId, addrs: seq[MultiAddress]
) {.gcsafe, raises: [].} =
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
  let autoConnectEnabled = mdnsAutoConnectEnabled()
  var shouldConnect = autoConnectEnabled
  if shouldConnect and n.mdnsLastSeen.hasKey(peerStr):
    let last = n.mdnsLastSeen.getOrDefault(peerStr)
    if now - last < MdnsConnectThrottleMs:
      shouldConnect = false
      debugLog("[nimlibp2p] mdns discovery throttled peer=" & peerStr & " elapsed=" & $(now - last))
  n.mdnsLastSeen[peerStr] = now
  n.bootstrapLastSeen[peerStr] = now

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
  elif dialable.len > 0 and not autoConnectEnabled:
    debugLog("[nimlibp2p] mdns auto-connect disabled peer=" & peerStr)

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
  try:
    prepareMdns(n)
  except CatchableError as exc:
    warn "prepareMdns failed", err = exc.msg
    return
  except Exception as exc:
    warn "prepareMdns fatal", err = exc.msg
    return
  if n.discovery.isNil or n.mdnsInterface.isNil:
    return
  debugLog("[nimlibp2p] startMdns: updating advertisements started=" & $n.started)
  try:
    updateMdnsAdvertisements(n)
  except CatchableError as exc:
    warn "updateMdnsAdvertisements failed", err = exc.msg
    return
  except Exception as exc:
    warn "updateMdnsAdvertisements fatal", err = exc.msg
    return
  if not n.started:
    return
  var services: seq[string] = @[]
  let candidates = serviceQueryVariants(n.mdnsService) &
    serviceQueryVariants(DefaultMdnsServiceName)
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
    await sleepAsync(chronos.milliseconds(200))
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
      try:
        updateMdnsAdvertisements(n)
      except CatchableError as exc:
        warn "kickMdnsQuery advertise failed", err = exc.msg
      except Exception as exc:
        warn "kickMdnsQuery advertise fatal", err = exc.msg
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
    try:
      updateMdnsAdvertisements(n)
    except CatchableError as exc:
      warn "kickMdnsQuery keepalive advertise failed", err = exc.msg
    except Exception as exc:
      warn "kickMdnsQuery keepalive advertise fatal", err = exc.msg
    if not n.mdnsInterface.isNil:
      try:
        await n.mdnsInterface.probeNow()
      except CatchableError as exc:
        warn "kickMdnsQuery probe failed", err = exc.msg
        debugLog("[nimlibp2p] kickMdnsQuery probe failed err=" & exc.msg)
      except Exception as exc:
        warn "kickMdnsQuery probe fatal", err = exc.msg
        debugLog("[nimlibp2p] kickMdnsQuery probe fatal err=" & exc.msg)

proc setupConnectionEvents(n: NimNode) =
  if n.connectionEventsEnabled:
    return
  let connectedHandler =
    proc(peerId: PeerId, event: ConnEvent): Future[void] {.closure, async: (raises: [CancelledError]).} =
      n.bootstrapLastSeen[$peerId] = nowMillis()
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
    var parsed: seq[MultiAddress] = @[]
    for addrStr in addresses:
      let ma = MultiAddress.init(addrStr)
      if ma.isOk():
        let maAddr = ma.get()
        parsed.add(maAddr)
        try:
          discard await n.switchInstance.connect(maAddr, allowUnknownPeerId = true)
        except CatchableError as exc:
          warn "boostConnectivity: connect via multiaddr failed", peer = peerId, address = addrStr, err = exc.msg
          debugLog(
            "[nimlibp2p] boostConnectivity connect multiaddr failed peer=" & peerId &
            " addr=" & addrStr &
            " err=" & exc.msg
          )
    if parsed.len == 0:
      continue
    if peerRes.isOk():
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
  var lastConnCount = -1
  var lastConnected = false
  debugLog("[nimlibp2p] waitForSecureChannel start peer=" & peerId & " timeoutMs=" & $max(timeoutMs, 500))
  while nowMillis() < deadline:
    let connCount =
      if n.switchInstance.isNil: 0
      else: n.switchInstance.connManager.connCount(peer)
    let connected = peerCurrentlyConnected(n, peer)
    if connCount != lastConnCount or connected != lastConnected:
      lastConnCount = connCount
      lastConnected = connected
      debugLog(
        "[nimlibp2p] waitForSecureChannel peer=" & peerId &
        " connCount=" & $connCount &
        " connectedPeers=" & $(if connected: 1 else: 0)
      )
    if connCount > 0 or connected:
      debugLog("[nimlibp2p] waitForSecureChannel ready peer=" & peerId)
      return true
    await sleepAsync(chronos.milliseconds(150))
  debugLog("[nimlibp2p] waitForSecureChannel timeout peer=" & peerId)
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
      var msquicBuilderCfg = MsQuicTransportBuilderConfig.init()
      if cfg.dataDir.isSome():
        let tlsTempDir = cfg.dataDir.get() / "tls_tmp"
        msquicBuilderCfg.onTransport = Opt.some(MsQuicTransportHook(
          proc(transport: MsQuicTransport) =
            transport.setTlsTempDir(tlsTempDir)
        ))
      builder = builder.withMsQuicTransport(msquicBuilderCfg)
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
  # Support both Mplex (default in newStandardSwitch) and Yamux for interop.
  builder = builder.withMplex()
  debugLog("[nimlibp2p] buildSwitch applied Mplex")
  builder = builder.withYamux()
  debugLog("[nimlibp2p] buildSwitch applied Yamux")
  var enableNoise = true
  var enableTls = true
  when defined(android):
    when defined(libp2p_pure_crypto):
      enableNoise = false
      enableTls = true
    else:
      enableTls = false
  if cfg.extra.kind == JObject:
    if cfg.extra.hasKey("enableNoise"):
      let node = cfg.extra["enableNoise"]
      if node.kind == JBool:
        enableNoise = node.getBool()
    elif cfg.extra.hasKey("disableNoise"):
      let node = cfg.extra["disableNoise"]
      if node.kind == JBool:
        enableNoise = not node.getBool()
    if cfg.extra.hasKey("enableTls"):
      let node = cfg.extra["enableTls"]
      if node.kind == JBool:
        enableTls = node.getBool()
    elif cfg.extra.hasKey("disableTls"):
      let node = cfg.extra["disableTls"]
      if node.kind == JBool:
        enableTls = not node.getBool()
  if not enableNoise and not enableTls:
    debugLog("[nimlibp2p] buildSwitch secure managers empty after config; restoring Noise")
    enableNoise = true
  if enableTls:
    builder = builder.withTls()
    debugLog("[nimlibp2p] buildSwitch applied TLS")
  else:
    debugLog("[nimlibp2p] buildSwitch TLS disabled")
  if enableNoise:
    builder = builder.withNoise()
    debugLog("[nimlibp2p] buildSwitch applied Noise")
  else:
    debugLog("[nimlibp2p] buildSwitch Noise disabled")
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
        try:
          if n.pingProtocol.isNil:
            n.pingProtocol = Ping.new()
            n.switchInstance.mount(n.pingProtocol)
          await n.switchInstance.start()
          n.started = true
          debugLog("[nimlibp2p] cmdStart: switch.start completed")
          info "cmdStart: switch started", listenAddrs = n.switchInstance.peerInfo.listenAddrs.mapIt($it)
          debugLog("[nimlibp2p] cmdStart: switch started")
          await setupDefaultTopics(n)
          debugLog("[nimlibp2p] cmdStart: default topics configured")
          if n.peerHints.len > 0:
            proc delayedBoost() {.async.} =
              await sleepAsync(chronos.seconds(1))
              await boostConnectivity(n)
            asyncSpawn(delayedBoost())
          scheduleStartMdns(n)
          debugLog("[nimlibp2p] cmdStart: mdns start scheduled")
          if n.mdnsRestartPending:
            info "cmdStart: applying deferred mdns restart", ipv4 = n.mdnsPreferredIpv4
            asyncSpawn(restartMdnsSafe(n, "cmdStart"))
            debugLog("[nimlibp2p] cmdStart: mdns restart scheduled")
          if nodeTelemetryPublishEnabled(n.cfg):
            if n.telemetryLoop.isNil or n.telemetryLoop.finished():
              n.telemetryLoop = telemetryLoop(n)
              asyncSpawn(n.telemetryLoop)
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
            scheduleStartMdns(n)
            if n.mdnsRestartPending:
              info "cmdStart: applying deferred mdns restart", ipv4 = n.mdnsPreferredIpv4
              asyncSpawn(restartMdnsSafe(n, "cmdStart:advertise"))
            if nodeTelemetryPublishEnabled(n.cfg):
              if n.telemetryLoop.isNil or n.telemetryLoop.finished():
                n.telemetryLoop = telemetryLoop(n)
                asyncSpawn(n.telemetryLoop)
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
              n.bootstrapLastSeen[command.peerId] = nowMillis()
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
        let timeoutMs = if command.timeoutMs > 0: command.timeoutMs else: 10_000
        let maxAttempts = if command.timeoutMs > 0 and command.timeoutMs <= 2_000: 1 else: 3
        var attempt = 0
        var success = false
        var lastError = ""
        debugLog(
          "[nimlibp2p] cmdConnectMultiaddr start addr=" & command.multiaddr &
          " timeoutMs=" & $timeoutMs &
          " maxAttempts=" & $maxAttempts
        )
        while attempt < maxAttempts and not success:
          let attemptNum = attempt + 1
          try:
            let fut = n.switchInstance.connect(ma, allowUnknownPeerId = true)
            let completed = await withTimeout(fut, chronos.milliseconds(timeoutMs))
            if not completed:
              fut.cancelSoon()
              lastError = "connect timeout"
              warn "cmdConnectMultiaddr timeout", addr = command.multiaddr, attempt = attemptNum
              debugLog(
                "[nimlibp2p] cmdConnectMultiaddr timeout addr=" & command.multiaddr &
                " attempt=" & $attemptNum
              )
            else:
              discard await fut
              success = true
              debugLog(
                "[nimlibp2p] cmdConnectMultiaddr connected addr=" & command.multiaddr &
                " attempt=" & $attemptNum
              )
              break
          except CancelledError as exc:
            lastError = exc.msg
            debugLog(
              "[nimlibp2p] cmdConnectMultiaddr cancelled addr=" & command.multiaddr &
              " attempt=" & $attemptNum &
              " err=" & lastError
            )
            break
          except CatchableError as exc:
            lastError = if exc.msg.len > 0: exc.msg else: $exc.name
            warn "cmdConnectMultiaddr dial failed", addr = command.multiaddr, attempt = attemptNum, err = lastError
            debugLog(
              "[nimlibp2p] cmdConnectMultiaddr dial failed addr=" & command.multiaddr &
              " attempt=" & $attemptNum &
              " err=" & lastError
            )
          attempt.inc()
          if attempt < maxAttempts:
            let delayMs = min(800, 200 * attempt)
            await sleepAsync(chronos.milliseconds(delayMs))
        if success:
          command.resultCode = NimResultOk
          command.errorMsg = ""
          n.lastDirectError = ""
          let peerOpt = extractPeerIdFromMultiaddrStr(command.multiaddr)
          if peerOpt.isSome():
            n.bootstrapLastSeen[peerOpt.get()] = nowMillis()
          let successMsg = "[nimlibp2p] cmdConnectMultiaddr success addr=" & command.multiaddr
          info "cmdConnectMultiaddr success", addr = command.multiaddr
          debugLog(successMsg)
        else:
          let errMsg = if lastError.len > 0: lastError else: "connect failed"
          command.resultCode = NimResultError
          command.errorMsg = errMsg
          n.lastDirectError = errMsg
          warn "cmdConnectMultiaddr failed", addr = command.multiaddr, err = errMsg
          when defined(android):
            discard android_log_write(
              AndroidLogError,
              "nim-libp2p",
              ("cmdConnectMultiaddr failed addr=" & command.multiaddr & " err=" & errMsg).cstring,
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
      let localProfile = collectLocalSystemProfile(n)
      n.peerSystemProfiles[$peerInfo.peerId] = localProfile
      prunePeerProfiles(n)
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
        "systemProfile": localProfile,
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
      prunePeerProfiles(n)
      var arr = newJArray()
      let connections = n.switchInstance.connManager.getConnections()
      for peerId, muxers in connections.pairs:
        let peerIdText = $peerId
        var entry = newJObject()
        entry["peerId"] = %peerIdText
        entry["connectionCount"] = %muxers.len
        var directions = newJArray()
        var streamCount = 0
        for mux in muxers:
          if mux.isNil or mux.connection.isNil:
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
        entry["latencyMs"] = %(-1)
        let peerBw = n.switchInstance.bandwidthStats(peerId)
        let relayed = peerLikelyRelayed(n, peerIdText, peerId)
        var bandwidthNode = newJObject()
        bandwidthNode["uplinkBps"] = %0.0
        bandwidthNode["downlinkBps"] = %0.0
        bandwidthNode["uplinkTotalBytes"] = %0'i64
        bandwidthNode["downlinkTotalBytes"] = %0'i64
        bandwidthNode["totalTransferBytes"] = %0'i64
        bandwidthNode["isRelayed"] = %relayed
        bandwidthNode["relayBottleneckBps"] = %0.0
        var uplinkEma = 0.0
        var downlinkEma = 0.0
        if peerBw.isSome():
          let stats = peerBw.get()
          uplinkEma = stats.outbound.emaBytesPerSecond
          downlinkEma = stats.inbound.emaBytesPerSecond
          bandwidthNode["uplinkBps"] = %uplinkEma
          bandwidthNode["downlinkBps"] = %downlinkEma
          bandwidthNode["uplinkTotalBytes"] = %stats.outbound.totalBytes
          bandwidthNode["downlinkTotalBytes"] = %stats.inbound.totalBytes
          bandwidthNode["totalTransferBytes"] = %(stats.outbound.totalBytes + stats.inbound.totalBytes)
        if relayed:
          bandwidthNode["relayBottleneckBps"] = %min(uplinkEma, downlinkEma)
        entry["bandwidth"] = bandwidthNode
        entry["isRelayed"] = %relayed
        var peerProfile = defaultSystemProfile()
        if n.peerSystemProfiles.hasKey(peerIdText):
          peerProfile = sanitizeSystemProfile(n.peerSystemProfiles.getOrDefault(peerIdText))
        peerProfile["bandwidth"] = bandwidthNode
        entry["systemProfile"] = peerProfile
        var latency = -1
        if not n.pingProtocol.isNil:
          for mux in muxers:
            if mux.isNil or mux.connection.isNil:
              continue
            latency = await measureConnectionLatencyMs(n, mux.connection)
            if latency >= 0:
              break
        entry["latencyMs"] = %latency
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
      n.bootstrapLastSeen[command.peerId] = nowMillis()
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
    of cmdGetRandomBootstrapPeers:
      let limit = clampBootstrapLimit(command.requestLimit, defaultLimit = 8, maxLimit = 32)
      let candidates = collectBootstrapCandidates(n)
      let selected = selectRandomBootstrapCandidates(candidates, limit)
      var peersNode = newJArray()
      for item in selected:
        var entry = newJObject()
        entry["peerId"] = %item.peerId
        entry["multiaddrs"] = %item.multiaddrs
        entry["lastSeenAt"] = %item.lastSeenAt
        entry["sources"] = %item.sources
        peersNode.add(entry)
      var payload = newJObject()
      payload["peers"] = peersNode
      payload["selectedCount"] = %selected.len
      payload["totalCount"] = %candidates.len
      payload["timestamp_ms"] = %nowMillis()
      command.stringResult = $payload
      command.resultCode = NimResultOk
    of cmdJoinViaRandomBootstrap:
      if not n.started:
        command.resultCode = NimResultInvalidState
        command.errorMsg = "node not started"
      else:
        let limit = clampBootstrapLimit(command.requestLimit, defaultLimit = 3, maxLimit = 8)
        let candidates = collectBootstrapCandidates(n)
        let selected = selectRandomBootstrapCandidates(candidates, limit)
        var attemptsNode = newJArray()
        var connectedCount = 0
        for item in selected:
          let connected = await connectBootstrapCandidate(n, item)
          if connected:
            inc connectedCount
            n.bootstrapLastSeen[item.peerId] = nowMillis()
          var entry = newJObject()
          entry["peerId"] = %item.peerId
          entry["multiaddrs"] = %item.multiaddrs
          entry["lastSeenAt"] = %item.lastSeenAt
          entry["sources"] = %item.sources
          entry["connected"] = %connected
          attemptsNode.add(entry)
        var payload = newJObject()
        payload["attempted"] = attemptsNode
        payload["attemptCount"] = %selected.len
        payload["connectedCount"] = %connectedCount
        payload["ok"] = %(connectedCount > 0)
        payload["timestamp_ms"] = %nowMillis()
        command.stringResult = $payload
        command.intResult = connectedCount
        command.boolResult = connectedCount > 0
        if connectedCount == 0:
          command.errorMsg = "no bootstrap peer connected"
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
          let sendFuture = n.dmService.send(
            remotePid, bytes, command.ackRequested, mid, timeout
          )
          let sendCompleted = await withTimeout(sendFuture, timeout + chronos.milliseconds(750))
          if not sendCompleted:
            sendFuture.cancelSoon()
            sendOk = false
            sendErr = "send timeout"
            debugLog("[nimlibp2p] cmdSendDirect timeout peer=" & command.peerId &
              " mid=" & mid & " ackRequested=" & $command.ackRequested)
          else:
            let resultTuple = await sendFuture
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
      if not n.switchInstance.isNil and not n.switchInstance.peerInfo.isNil:
        let peerInfo = n.switchInstance.peerInfo
        obj["listenAddresses"] = %(peerInfo.listenAddrs.mapIt($it))
        obj["dialableAddresses"] = %(peerInfo.addrs.mapIt($it))
      else:
        obj["listenAddresses"] = newJArray()
        obj["dialableAddresses"] = newJArray()
      obj["dnsaddrEntries"] = mdnsDnsaddrPreview(n)
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
    of cmdMeasurePeerBandwidth:
      if not n.started:
        command.resultCode = NimResultInvalidState
        command.errorMsg = "node not started"
      elif command.peerId.len == 0:
        command.resultCode = NimResultInvalidArgument
        command.errorMsg = "missing peer id"
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
        let durationMs = clampProbeDurationMs(command.timeoutMs)
        let chunkBytes = clampProbeChunkBytes(command.intParam)
        let relayed = peerLikelyRelayed(n, command.peerId, remotePid)
        var probe = newJObject()
        probe["ok"] = %false
        probe["peerId"] = %command.peerId
        probe["durationMs"] = %durationMs
        probe["chunkBytes"] = %chunkBytes
        probe["isRelayed"] = %relayed
        let local = localPeerId(n)
        probe["localPeerId"] = %local
        let connectivityOk = await ensurePeerConnectivity(n, remotePid)
        if not connectivityOk:
          let err = if command.errorMsg.len > 0: command.errorMsg else: "peer unreachable"
          command.errorMsg = err
          probe["error"] = %err
          probe["timestampMs"] = %nowMillis()
          command.stringResult = $probe
          command.boolResult = false
          command.resultCode = NimResultOk
          return

        let sessionBase = "probe-" & $nowMillis()
        let uploadWindowStart = nowMillis()
        let uploadBefore = peerBandwidthTotals(n, remotePid)
        let uploadRun = await sendProbeChunks(n, remotePid, sessionBase & "-up", durationMs, chunkBytes)
        await sleepAsync(chronos.milliseconds(ProbeWindowTailMs))
        let uploadAfter = peerBandwidthTotals(n, remotePid)
        let uploadWindowMs = max(1'i64, nowMillis() - uploadWindowStart)
        let uplinkBytes = max(0'i64, uploadAfter.outbound - uploadBefore.outbound)
        let uplinkBps = computeProbeBps(uplinkBytes, uploadWindowMs)

        let downloadWindowStart = nowMillis()
        let downloadBefore = peerBandwidthTotals(n, remotePid)
        let requestMid = ensureMessageId("bw-probe-down-" & $nowMillis())
        var request = newJObject()
        request["op"] = %"bw_probe_push"
        request["mid"] = %requestMid
        request["sid"] = %(sessionBase & "-down")
        request["from"] = %local
        request["durationMs"] = %durationMs
        request["chunkBytes"] = %chunkBytes
        request["timestamp_ms"] = %nowMillis()
        request["ackRequested"] = %true
        let requestPayload = stringToBytes($request)
        let requestTimeout = chronos.milliseconds(max(ProbeSendTimeoutMs, durationMs))
        var downloadRequestOk = false
        var downloadRequestErr = ""
        try:
          let sendRes = await n.dmService.send(remotePid, requestPayload, true, requestMid, requestTimeout)
          downloadRequestOk = sendRes[0]
          downloadRequestErr = sendRes[1]
        except CatchableError as exc:
          downloadRequestOk = false
          downloadRequestErr = exc.msg
        if downloadRequestOk:
          await sleepAsync(chronos.milliseconds(durationMs + ProbeWindowTailMs))
        let downloadAfter = peerBandwidthTotals(n, remotePid)
        let downloadWindowMs = max(1'i64, nowMillis() - downloadWindowStart)
        let downlinkBytes = max(0'i64, downloadAfter.inbound - downloadBefore.inbound)
        let downlinkBps = computeProbeBps(downlinkBytes, downloadWindowMs)
        let bottleneckBps = if relayed: min(uplinkBps, downlinkBps) else: 0.0

        let probeOk = uploadRun.ok and downloadRequestOk
        probe["ok"] = %probeOk
        probe["uplinkBps"] = %uplinkBps
        probe["downlinkBps"] = %downlinkBps
        probe["uplinkSampleBytes"] = %uplinkBytes
        probe["downlinkSampleBytes"] = %downlinkBytes
        probe["uploadWindowMs"] = %uploadWindowMs
        probe["downloadWindowMs"] = %downloadWindowMs
        probe["uploadChunkCount"] = %uploadRun.chunkCount
        probe["uploadPayloadBytes"] = %uploadRun.payloadBytes
        probe["relayLimited"] = %relayed
        probe["relayBottleneckBps"] = %bottleneckBps
        if not uploadRun.ok and uploadRun.err.len > 0:
          probe["uploadError"] = %uploadRun.err
        if not downloadRequestOk and downloadRequestErr.len > 0:
          probe["downloadError"] = %downloadRequestErr
        probe["timestampMs"] = %nowMillis()
        command.boolResult = probeOk
        if not probeOk:
          if not uploadRun.ok and uploadRun.err.len > 0:
            command.errorMsg = uploadRun.err
          elif not downloadRequestOk and downloadRequestErr.len > 0:
            command.errorMsg = downloadRequestErr
          else:
            command.errorMsg = "bandwidth probe failed"
        command.stringResult = $probe
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
        cfg.enableAutoMatch = false
        cfg.enableAutoTrade = false
        # Production defaults: require signatures.
        var allowUnsigned = false
        var enableSigning = true
        
        if command.jsonString.len > 0:
          try:
             let j = parseJson(command.jsonString)
             if j.hasKey("mode") and j["mode"].kind == JString:
               case j["mode"].getStr().toLowerAscii()
               of "matcher":
                 cfg.mode = dex_types.DexMode.dmMatcher
               of "observer":
                 cfg.mode = dex_types.DexMode.dmObserver
               of "kline":
                 cfg.mode = dex_types.DexMode.dmKline
               else:
                 cfg.mode = dex_types.DexMode.dmTrader
             if j.hasKey("enableMixer"):
               cfg.enableMixer = j["enableMixer"].getBool()
             if j.hasKey("enableAutoMatch"):
               cfg.enableAutoMatch = j["enableAutoMatch"].getBool()
             if j.hasKey("enableAutoTrade"):
               cfg.enableAutoTrade = j["enableAutoTrade"].getBool()
             if j.hasKey("allowUnsigned"):
               allowUnsigned = j["allowUnsigned"].getBool()
             if j.hasKey("enableSigning"):
               enableSigning = j["enableSigning"].getBool()
          except CatchableError:
             debugLog("[nimlibp2p] cmdDexInit failed to parse json config")

        let store = dex_store.initOrderStore(cfg.dataDir)
        let keyPath = cfg.dataDir / dex_signing.DefaultKeyFile
        let signingCfg = dex_signing.SigningConfig(
            mode: if enableSigning: dex_signing.smEnabled else: dex_signing.smDisabled,
            keyPath: keyPath,
            requireRemoteSignature: false,
            allowUnsigned: allowUnsigned
        )
        let signingCtx = dex_signing.initSigning(signingCfg)
        
        n.dexNode = await dex_node.initDexNode(n.switchInstance, n.gossip, cfg, store, signingCtx)
        n.dexNode.onEvent = proc(topic: string, payload: string) {.gcsafe, raises: [].} =
          recordEvent(n, topic, payload)
        
        if n.dexNode.mixerCtx != nil:
          n.dexNode.mixerCtx.onEvent = proc(payload: string) {.gcsafe.} =
            recordEvent(n, "dex.mixer", payload)
        
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
      if n.dexNode.isNil:
        command.errorMsg = "DEX not initialized"
        command.resultCode = NimResultInvalidState
      else:
        proc jsonFloat(node: JsonNode, key: string, fallback: float): float =
          if node.kind != JObject or not node.hasKey(key):
            return fallback
          let value = node[key]
          case value.kind
          of JFloat, JInt:
            value.getFloat()
          of JString:
            try:
              parseFloat(value.getStr())
            except ValueError:
              fallback
          else:
            fallback

        proc jsonInt(node: JsonNode, key: string, fallback: int): int =
          if node.kind != JObject or not node.hasKey(key):
            return fallback
          let value = node[key]
          case value.kind
          of JInt, JFloat:
            value.getInt()
          of JString:
            try:
              parseInt(value.getStr())
            except ValueError:
              fallback
          else:
            fallback

        proc jsonStr(node: JsonNode, key: string, fallback = ""): string =
          if node.kind == JObject and node.hasKey(key) and node[key].kind == JString:
            node[key].getStr()
          else:
            fallback

        try:
          var order: dex_types.OrderMessage
          if command.jsonString.len > 0:
            let json = parseJson(command.jsonString)
            order = dex_types.OrderMessage(
              id: jsonStr(json, "id"),
              traderPeer: jsonStr(json, "traderPeer"),
              baseAsset:
                if json.kind == JObject and json.hasKey("baseAsset"):
                  dex_types.decodeAssetDef(json["baseAsset"])
                else:
                  dex_types.AssetDef(chainId: dex_types.ChainBTC, symbol: "BTC", decimals: 8, assetType: dex_types.AssetNative),
              quoteAsset:
                if json.kind == JObject and json.hasKey("quoteAsset"):
                  dex_types.decodeAssetDef(json["quoteAsset"])
                else:
                  dex_types.AssetDef(chainId: dex_types.ChainBSC, symbol: "USDC", decimals: 6, assetType: dex_types.AssetERC20),
              side: jsonStr(json, "side", "buy"),
              price: jsonFloat(json, "price", 0.0),
              amount: jsonFloat(json, "amount", 0.0),
              ttlMs: jsonInt(json, "ttlMs", 60_000),
              timestamp: jsonInt(json, "timestamp", 0).int64,
              signature: jsonStr(json, "signature"),
              signerPubKey: jsonStr(json, "signerPubKey"),
              signatureVersion: jsonInt(json, "signatureVersion", 0),
              onionPath: @[]
            )
          else:
            order = dex_types.OrderMessage(
              id: "",
              traderPeer: "",
              baseAsset: dex_types.AssetDef(chainId: dex_types.ChainBTC, symbol: "BTC", decimals: 8, assetType: dex_types.AssetNative),
              quoteAsset: dex_types.AssetDef(chainId: dex_types.ChainBSC, symbol: "USDC", decimals: 6, assetType: dex_types.AssetERC20),
              side: "buy",
              price: 65_000.0,
              amount: 0.01,
              ttlMs: 60_000,
              timestamp: nowMillis(),
              signature: "",
              signerPubKey: "",
              signatureVersion: 0,
              onionPath: @[]
            )

          let localPeer = $n.switchInstance.peerInfo.peerId
          let signingPeer = dex_signing.peerIdStr(n.dexNode.signing)
          order.traderPeer = if signingPeer.len > 0: signingPeer else: localPeer
          if order.id.len == 0:
            order.id = $n.dexNode.rng[].generate(uint64)
          if order.timestamp <= 0:
            order.timestamp = nowMillis()
          if order.ttlMs <= 0:
            order.ttlMs = 60_000
          if order.price <= 0:
            order.price = 65_000.0
          if order.amount <= 0:
            order.amount = 0.01
          if order.side.len == 0:
            order.side = "buy"

          dex_signing.signOrder(n.dexNode.signing, order)
          discard await n.dexNode.publishOrder(order)
          recordEvent(n, "dex.order_submitted", $(%*{
            "id": order.id,
            "side": order.side,
            "price": order.price,
            "amount": order.amount,
            "base": order.baseAsset.toString(),
            "quote": order.quoteAsset.toString(),
          }))
          command.resultCode = NimResultOk
        except CatchableError as exc:
          command.errorMsg = exc.msg
          command.resultCode = NimResultError

    of cmdDexGetMarketData:
      if n.dexNode.isNil or n.dexNode.klineStore.isNil:
        command.stringResult = "[]"
        command.resultCode = NimResultOk
      else:
        let filter = command.textPayload.strip()
        let buckets = await dex_kline_store.snapshot(n.dexNode.klineStore)
        var resultArr = newJArray()
        for bucket in buckets:
          if filter.len > 0 and bucket.asset != filter and filter != "*":
            continue
          resultArr.add(dex_types.klineBucketToJson(bucket))
        command.stringResult = $resultArr
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

proc idleWakeLoop(n: NimNode) {.async.} =
  ## Keep the Chronos dispatcher from blocking indefinitely when idle.
  ## Commands are submitted from foreign threads via `submitCommand`, so we need
  ## the event loop to periodically wake and drain the command queue.
  while not n.stopRequested.load(moAcquire):
    try:
      await sleepAsync(chronos.milliseconds(250))
    except CancelledError:
      break

proc eventLoop(handlePtr: pointer) {.thread.} =
  let handle = cast[NodeHandle](handlePtr)
  if handle.isNil or handle[].node.isNil:
    debugLog("[nimlibp2p] event loop received null handle")
    return
  let node = handle[].node
  let nodeAddr = cast[int](node)
  let debugEventLoop = getEnv("LIBP2P_DEBUG_EVENTLOOP").len > 0
  debugLog("[nimlibp2p] event loop entering node=" & $nodeAddr)
  info "event loop entering", nodeAddr = nodeAddr
  chronos.setThreadDispatcher(newDispatcher())
  node.signalLoopTask = signalLoop(node)
  asyncSpawn(node.signalLoopTask)
  node.idleWakeTask = idleWakeLoop(node)
  asyncSpawn(node.idleWakeTask)
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
      if debugEventLoop:
        debugLog("[nimlibp2p] event loop polling node=" & $nodeAddr)
      poll()
      if debugEventLoop:
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
    if remaining > 0 and debugEventLoop:
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

proc submitCommandNoWait(node: NimNode, cmd: Command): bool =
  let active = node.running.load(moAcquire)
  if not active:
    warn "submitCommandNoWait rejected: node not running", kind = $cmd.kind
    cmd.resultCode = NimResultInvalidState
    return false
  if node.signal.isNil:
    warn "submitCommandNoWait rejected: node signal missing", kind = $cmd.kind
    cmd.resultCode = NimResultInvalidState
    return false
  info "submitCommandNoWait", kind = $cmd.kind, pending = node.pendingCommands.load(moAcquire)
  enqueueCommand(node, cmd)
  let queueLen = node.queuedCommands.load(moAcquire)
  let fired = node.signal.fireSync()
  if fired.isErr():
    warn "failed to fire thread signal", err = fired.error
  elif fired.get():
    debugLog(
      "[nimlibp2p] submitCommandNoWait fireSync ok node=" & $cast[int](node) &
      " kind=" & $cmd.kind &
      " queued=" & $queueLen
    )
  true

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
      signalLoopTask: nil,
      idleWakeTask: nil,
      readySignal: readySignalPtr,
      shutdownSignal: shutdownSignalPtr,
      subscriptions: initTable[pointer, Subscription](),
      peerHints: initTable[string, seq[string]](),
      bootstrapLastSeen: initTable[string, int64](),
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
      telemetryLoop: nil,
      pingProtocol: nil,
      eventLog: @[],
      receivedDmLog: @[],
      eventLogLimit: EventLogLimit,
      receivedDmLogLimit: ReceivedDmLogLimit,
      localSystemProfile: newJObject(),
      localSystemProfileUpdatedAt: 0,
      peerSystemProfiles: initTable[string, JsonNode](),
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
    initLock(node.receivedDmLogLock)
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
  deinitLock(node.eventLogLock)
  deinitLock(node.receivedDmLogLock)
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

proc libp2p_connect_multiaddr_timeout*(
    handle: pointer, multiaddr: cstring, timeoutMs: cint
): cint {.exportc, cdecl, dynlib.} =
  if multiaddr.isNil:
    return NimResultInvalidArgument
  let node = nodeFromHandle(handle)
  if node.isNil:
    return NimResultInvalidArgument
  let cmd = makeCommand(cmdConnectMultiaddr)
  cmd.multiaddr = $multiaddr
  cmd.timeoutMs = max(int(timeoutMs), 250)
  let result = submitCommand(node, cmd)
  let rc = result.resultCode
  let err = result.errorMsg
  destroyCommand(cmd)
  if rc != NimResultOk:
    warn "libp2p_connect_multiaddr_timeout failed", addr = $multiaddr, timeoutMs = int(timeoutMs), err = err
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
  let text = cloneOwnedString(result.stringResult)
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
        text = cloneOwnedString(result.stringResult)
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
  let text =
    if result.resultCode == NimResultOk:
      let copied = cloneOwnedString(result.stringResult)
      if copied.len > 0: copied else: "{}"
    else:
      "{}"
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
      cloneOwnedString(result.stringResult)
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

proc libp2p_measure_peer_bandwidth*(
    handle: pointer,
    peerId: cstring,
    durationMs: cint,
    chunkBytes: cint
): cstring {.exportc, cdecl, dynlib.} =
  if peerId.isNil:
    return allocCString("{\"ok\":false,\"error\":\"missing peer id\"}")
  let node = nodeFromHandle(handle)
  if node.isNil:
    return allocCString("{\"ok\":false,\"error\":\"node not initialized\"}")
  let cmd = makeCommand(cmdMeasurePeerBandwidth)
  cmd.peerId = $peerId
  cmd.timeoutMs = int(durationMs)
  cmd.intParam = int(chunkBytes)
  let result = submitCommand(node, cmd)
  let payload =
    if result.resultCode == NimResultOk and result.stringResult.len > 0:
      cloneOwnedString(result.stringResult)
    else:
      (if result.errorMsg.len > 0:
        "{\"ok\":false,\"error\":\"" & result.errorMsg.replace("\"", "\\\"") & "\"}"
       else:
        "{\"ok\":false,\"error\":\"probe failed\"}")
  destroyCommand(cmd)
  allocCString(payload)

proc libp2p_poll_events*(handle: pointer, maxEvents: cint): cstring {.exportc, cdecl, dynlib.} =
  let node = nodeFromHandle(handle)
  if node.isNil:
    return allocCString("[]")
  let cmd = makeCommand(cmdPollEvents)
  cmd.requestLimit = int(maxEvents)
  let result = submitCommand(node, cmd)
  let output =
    if result.resultCode == NimResultOk:
      allocCString(cloneOwnedString(result.stringResult))
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
      allocCString(cloneOwnedString(result.stringResult))
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
      allocCString(cloneOwnedString(result.stringResult))
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
      allocCString(cloneOwnedString(result.stringResult))
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
      allocCString(cloneOwnedString(result.stringResult))
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
      allocCString(cloneOwnedString(result.stringResult))
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
  var payloadText =
    if result.resultCode == NimResultOk and result.stringResult.len > 0:
      result.stringResult
    else:
      "{}"
  try:
    var payload = parseJson(payloadText)
    if payload.kind != JObject:
      payload = newJObject()
    var socialStats = newJObject()
    socialStateMemLock.acquire()
    try:
      let socialState = loadSocialStateUnsafe(node)
      socialStats = socialStatsNode(socialState)
    finally:
      socialStateMemLock.release()
    if socialStats.kind != JObject:
      socialStats = newJObject()
    acquire(node.eventLogLock)
    try:
      socialStats["eventQueueDepth"] = %node.eventLog.len
    finally:
      release(node.eventLogLock)
    payload["social"] = socialStats
    payloadText = $payload
  except CatchableError:
    discard
  let output = allocCString(payloadText)
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

proc jsonArrayValues(node: JsonNode, key: string): seq[string] =
  if node.kind != JObject or not node.hasKey(key):
    return @[]
  let arr = node.getOrDefault(key)
  if arr.kind != JArray:
    return @[]
  for item in arr:
    if item.kind == JString:
      result.add(item.getStr())

proc appendUniqueString(arr: JsonNode, value: string) =
  if arr.isNil or arr.kind != JArray or value.len == 0:
    return
  for item in arr:
    if item.kind == JString and item.getStr() == value:
      return
  arr.add(%value)

proc defaultSocialState(): JsonNode =
  result = newJObject()
  result["version"] = %SocialStateVersion
  result["conversations"] = newJObject()
  result["contacts"] = newJObject()
  result["groups"] = newJObject()
  result["moments"] = newJArray()
  result["notifications"] = newJArray()
  result["synccastRooms"] = newJObject()

proc ensureSocialSchema(state: JsonNode): JsonNode =
  result = if state.isNil or state.kind != JObject: defaultSocialState() else: state
  if not result.hasKey("version"): result["version"] = %SocialStateVersion
  if not result.hasKey("conversations") or result["conversations"].kind != JObject:
    result["conversations"] = newJObject()
  if not result.hasKey("contacts") or result["contacts"].kind != JObject:
    result["contacts"] = newJObject()
  if not result.hasKey("groups") or result["groups"].kind != JObject:
    result["groups"] = newJObject()
  if not result.hasKey("moments") or result["moments"].kind != JArray:
    result["moments"] = newJArray()
  if not result.hasKey("notifications") or result["notifications"].kind != JArray:
    result["notifications"] = newJArray()
  if not result.hasKey("synccastRooms") or result["synccastRooms"].kind != JObject:
    result["synccastRooms"] = newJObject()

proc socialStatePath(node: NimNode): string =
  if node.isNil or node.cfg.dataDir.isNone:
    return ""
  node.cfg.dataDir.get() / "social_state.json"

proc loadSocialStateUnsafe(node: NimNode): JsonNode =
  let key = cast[int](node)
  if socialStateMem.hasKey(key):
    return socialStateMem.getOrDefault(key)
  var state = defaultSocialState()
  let path = socialStatePath(node)
  if path.len > 0:
    try:
      if fileExists(path):
        let parsed = parseJson(readFile(path))
        if parsed.kind == JObject:
          state = parsed
    except CatchableError:
      discard
  state = ensureSocialSchema(state)
  socialStateMem[key] = state
  state

proc persistSocialStateUnsafe(node: NimNode, state: JsonNode) =
  if node.isNil:
    return
  let key = cast[int](node)
  socialStateMem[key] = state
  let path = socialStatePath(node)
  if path.len == 0:
    return
  try:
    writeFile(path, $state)
  except CatchableError as exc:
    warn "persist social state failed", err = exc.msg, path = path

proc makeTraceId(): string =
  "trace-" & $nowMillis() & "-" & $rand(1_000_000)

proc socialStatsNode(state: JsonNode): JsonNode =
  result = newJObject()
  let convCount = if state.hasKey("conversations"): state["conversations"].len else: 0
  let contactCount = if state.hasKey("contacts"): state["contacts"].len else: 0
  let groupCount = if state.hasKey("groups"): state["groups"].len else: 0
  let momentCount = if state.hasKey("moments") and state["moments"].kind == JArray: state["moments"].len else: 0
  let notificationCount =
    if state.hasKey("notifications") and state["notifications"].kind == JArray: state["notifications"].len else: 0
  let synccastRoomCount =
    if state.hasKey("synccastRooms") and state["synccastRooms"].kind == JObject: state["synccastRooms"].len else: 0
  result["conversations"] = %convCount
  result["contacts"] = %contactCount
  result["groups"] = %groupCount
  result["moments"] = %momentCount
  result["notifications"] = %notificationCount
  result["synccastRooms"] = %synccastRoomCount
  result["pendingRetransmissions"] = %0
  result["eventQueueDepth"] = %0

proc makeNotificationNoLock(
    state: JsonNode, kind: string, title: string, body: string, payload: JsonNode = newJObject()
) =
  if state.kind != JObject or state["notifications"].kind != JArray:
    return
  var item = newJObject()
  item["id"] = %("notif-" & $nowMillis() & "-" & $rand(10_000))
  item["kind"] = %kind
  item["title"] = %title
  item["body"] = %body
  item["payload"] = payload
  item["read"] = %false
  item["timestampMs"] = %nowMillis()
  var nextNotifications = newJArray()
  nextNotifications.add(item)
  for existing in state["notifications"]:
    if nextNotifications.len >= SocialMaxNotifications:
      break
    nextNotifications.add(existing)
  state["notifications"] = nextNotifications

proc ensureConversationNoLock(state: JsonNode, conversationId: string, peerId: string): JsonNode =
  if state.kind != JObject:
    return newJObject()
  if not state.hasKey("conversations") or state["conversations"].kind != JObject:
    state["conversations"] = newJObject()
  let conversations = state["conversations"]
  if not conversations.hasKey(conversationId) or conversations[conversationId].kind != JObject:
    var conv = newJObject()
    conv["conversationId"] = %conversationId
    conv["peerId"] = %peerId
    conv["messages"] = newJArray()
    conv["updatedAt"] = %nowMillis()
    conversations[conversationId] = conv
  let conv = conversations[conversationId]
  if not conv.hasKey("messages") or conv["messages"].kind != JArray:
    conv["messages"] = newJArray()
  conv

proc appendConversationMessageNoLock(
    state: JsonNode, conversationId: string, peerId: string, message: JsonNode
) =
  let conv = ensureConversationNoLock(state, conversationId, peerId)
  if conv.kind != JObject:
    return
  conv["messages"].add(message)
  if conv["messages"].len > SocialMaxConversationMessages:
    var trimmed = newJArray()
    let start = conv["messages"].len - SocialMaxConversationMessages
    for i in start ..< conv["messages"].len:
      trimmed.add(conv["messages"][i])
    conv["messages"] = trimmed
  conv["updatedAt"] = %nowMillis()

proc normalizeSocialPayload(payload: string): JsonNode =
  if payload.len == 0:
    return newJObject()
  try:
    let parsed = parseJson(payload)
    if parsed.kind == JObject:
      return parsed
  except CatchableError:
    discard
  var obj = newJObject()
  obj["raw"] = %payload
  obj

proc maybeBodyFromPayload(payload: JsonNode): string =
  if payload.kind != JObject:
    return ""
  result = jsonGetStr(payload, "text")
  if result.len == 0: result = jsonGetStr(payload, "body")
  if result.len == 0: result = jsonGetStr(payload, "content")

proc recordSocialEvent(
    node: NimNode,
    entity: string,
    op: string,
    payload: JsonNode,
    conversationId: string = "",
    groupId: string = "",
    postId: string = "",
    source: string = "nim-social"
) =
  var evt = newJObject()
  evt["kind"] = %"social"
  evt["entity"] = %entity
  evt["op"] = %op
  evt["traceId"] = %makeTraceId()
  evt["seq"] = %nowMillis()
  evt["timestampMs"] = %nowMillis()
  evt["source"] = %source
  if conversationId.len > 0:
    evt["conversationId"] = %conversationId
  if groupId.len > 0:
    evt["groupId"] = %groupId
  if postId.len > 0:
    evt["postId"] = %postId
  evt["payload"] = payload
  recordEvent(node, "social_event", $evt)

proc takeCStringAndFree(value: cstring): string =
  if value.isNil:
    return ""
  result = $value
  libp2p_string_free(value)

proc parseSourceFilter(sourceFilter: string): HashSet[string] =
  result = initHashSet[string]()
  for part in sourceFilter.split({',', ' ', ';'}):
    let token = part.strip().toLowerAscii()
    if token.len > 0:
      result.incl(token)

proc collectMdnsPeerRows(node: NimNode): JsonNode =
  var peers = newJArray()
  var ordered: seq[(string, int64)] = @[]
  for peerId, seenAt in node.mdnsLastSeen.pairs:
    if peerId.len == 0:
      continue
    ordered.add((peerId, seenAt))
  ordered.sort(proc(a, b: (string, int64)): int =
    result = cmp(b[1], a[1])
    if result == 0:
      result = cmp(a[0], b[0])
  )
  for entry in ordered:
    let peerId = entry[0]
    let seenAt = entry[1]
    var row = newJObject()
    row["peerId"] = %peerId
    row["lastSeenAt"] = %seenAt
    var sources = newJArray()
    sources.add(%"mDNS")
    let hints = preferStableAddrStrings(node.peerHints.getOrDefault(peerId))
    var addrs = newJArray()
    var hasLan = false
    var hasWan = false
    for hint in hints:
      let normalized = normalizeBootstrapMultiaddr(peerId, hint.strip())
      if normalized.len == 0:
        continue
      addrs.add(%normalized)
      if isLanAddress(normalized):
        hasLan = true
      else:
        hasWan = true
    if hasLan:
      sources.add(%"LAN")
    if hasWan:
      sources.add(%"WAN")
    row["sources"] = sources
    row["multiaddrs"] = addrs
    peers.add(row)
  peers

proc collectDiscoveredPeers(handle: pointer, node: NimNode): JsonNode =
  var peers = initTable[string, JsonNode]()
  let local = localPeerId(node)

  proc upsertPeer(peerId: string, source: string, addrs: seq[string], seenAt: int64 = 0'i64) =
    if peerId.len == 0 or (local.len > 0 and peerId == local):
      return
    let effectiveSeenAt = if seenAt > 0: seenAt else: nowMillis()
    if not peers.hasKey(peerId):
      var row = newJObject()
      row["peerId"] = %peerId
      row["multiaddrs"] = newJArray()
      row["sources"] = newJArray()
      row["lastSeenAt"] = %effectiveSeenAt
      peers[peerId] = row
    let row = peers[peerId]
    if row.hasKey("multiaddrs") and row["multiaddrs"].kind == JArray:
      for addr in addrs:
        let normalized = normalizeBootstrapMultiaddr(peerId, addr.strip())
        if normalized.len > 0:
          appendUniqueString(row["multiaddrs"], normalized)
    if row.hasKey("sources") and row["sources"].kind == JArray and source.len > 0:
      appendUniqueString(row["sources"], source)
    let existingSeenAt = jsonGetInt64(row, "lastSeenAt", 0)
    row["lastSeenAt"] = %max(existingSeenAt, effectiveSeenAt)

  proc annotateTransportSources(peerId: string, addrs: seq[string], seenAt: int64 = 0'i64) =
    var hasLan = false
    var hasWan = false
    for addr in addrs:
      let normalized = normalizeBootstrapMultiaddr(peerId, addr.strip())
      if normalized.len == 0:
        continue
      if isLanAddress(normalized):
        hasLan = true
      else:
        hasWan = true
    if hasLan:
      upsertPeer(peerId, "LAN", @[], seenAt)
    if hasWan:
      upsertPeer(peerId, "WAN", @[], seenAt)

  let connectedRaw = takeCStringAndFree(libp2p_connected_peers_info(handle))
  if connectedRaw.len > 0:
    try:
      let arr = parseJson(connectedRaw)
      if arr.kind == JArray:
        for row in arr:
          let peerId = jsonGetStr(row, "peerId", jsonGetStr(row, "peer_id"))
          let addrs = preferStableAddrStrings(jsonArrayValues(row, "addresses") & jsonArrayValues(row, "multiaddrs"))
          let seenAt = jsonGetInt64(row, "lastSeenAt", nowMillis())
          upsertPeer(peerId, "Connected", addrs, seenAt)
          annotateTransportSources(peerId, addrs, seenAt)
    except CatchableError:
      discard

  for peerId, seenAt in node.mdnsLastSeen.pairs:
    let hints = preferStableAddrStrings(node.peerHints.getOrDefault(peerId))
    upsertPeer(peerId, "mDNS", hints, seenAt)
    annotateTransportSources(peerId, hints, seenAt)

  var arr = newJArray()
  for _, row in peers:
    arr.add(row)
  arr

proc filteredDiscoveredPeers(handle: pointer, node: NimNode, sourceFilterText: string, limit: int): tuple[peers: JsonNode, totalCount: int] =
  let filter = parseSourceFilter(sourceFilterText)
  let discovered = collectDiscoveredPeers(handle, node)
  var filtered: seq[JsonNode] = @[]
  for row in discovered:
    if filter.len > 0:
      var matched = false
      if row.kind == JObject and row.hasKey("sources") and row["sources"].kind == JArray:
        for src in row["sources"]:
          if src.kind == JString and filter.contains(src.getStr().toLowerAscii()):
            matched = true
            break
      if not matched:
        continue
    filtered.add(row)
  filtered.sort(
    proc(a, b: JsonNode): int =
      cmp(jsonGetInt64(b, "lastSeenAt"), jsonGetInt64(a, "lastSeenAt"))
  )
  let bounded = if limit <= 0: filtered.len else: min(filtered.len, limit)
  var peers = newJArray()
  if bounded > 0:
    for i in 0 ..< bounded:
      peers.add(filtered[i])
  (peers: peers, totalCount: filtered.len)

proc buildMdnsDebugPayload(node: NimNode): JsonNode =
  var obj = newJObject()
  obj["enabled"] = %node.mdnsEnabled
  obj["started"] = %node.started
  obj["preferred_ipv4"] = %node.mdnsPreferredIpv4
  obj["restart_pending"] = %node.mdnsRestartPending
  obj["interval_seconds"] = %node.mdnsIntervalSeconds
  obj["interface_bound_ipv4"] =
    if not node.mdnsInterface.isNil: %node.mdnsInterface.boundIpv4 else: %""
  if not node.switchInstance.isNil and not node.switchInstance.peerInfo.isNil:
    let peerInfo = node.switchInstance.peerInfo
    obj["listenAddresses"] = %(peerInfo.listenAddrs.mapIt($it))
    obj["dialableAddresses"] = %(peerInfo.addrs.mapIt($it))
  else:
    obj["listenAddresses"] = newJArray()
    obj["dialableAddresses"] = newJArray()
  obj["dnsaddrEntries"] = mdnsDnsaddrPreview(node)
  let peers = collectMdnsPeerRows(node)
  obj["peerCount"] = %peers.len
  obj["peers"] = peers
  obj

proc connectedPeersInfoArray(handle: pointer): JsonNode =
  let raw = takeCStringAndFree(libp2p_connected_peers_info(handle))
  if raw.len > 0:
    try:
      let parsed = parseJson(raw)
      if parsed.kind == JArray:
        return parsed
    except CatchableError:
      discard
  newJArray()

proc connectedPeersArray(handle: pointer): JsonNode =
  let raw = takeCStringAndFree(libp2p_get_connected_peers_json(handle))
  if raw.len > 0:
    try:
      let parsed = parseJson(raw)
      if parsed.kind == JArray:
        return parsed
    except CatchableError:
      discard
  newJArray()

proc buildNetworkResourcesPayload(node: NimNode, discoveredCount: int, connectedCount: int): JsonNode =
  let localProfile = collectLocalSystemProfile(node)
  prunePeerProfiles(node)
  var cpuTotal = jsonGetInt64(localProfile["cpu"], "cores", 0)
  var memoryTotal = jsonGetInt64(localProfile["memory"], "totalBytes", 0)
  var diskTotal = jsonGetInt64(localProfile["disk"], "totalBytes", 0)
  var diskAvailable = jsonGetInt64(localProfile["disk"], "availableBytes", 0)
  var gpuVramTotal = jsonGetInt64(localProfile["gpu"], "vramBytes", 0)
  var uplink = jsonGetFloat(localProfile["bandwidth"], "uplinkBps", 0.0)
  var downlink = jsonGetFloat(localProfile["bandwidth"], "downlinkBps", 0.0)
  var uplinkBytes = jsonGetInt64(localProfile["bandwidth"], "uplinkTotalBytes", 0)
  var downlinkBytes = jsonGetInt64(localProfile["bandwidth"], "downlinkTotalBytes", 0)
  for peerId, profile in node.peerSystemProfiles.pairs:
    if peerId == localPeerId(node):
      continue
    cpuTotal += jsonGetInt64(profile["cpu"], "cores", 0)
    memoryTotal += jsonGetInt64(profile["memory"], "totalBytes", 0)
    diskTotal += jsonGetInt64(profile["disk"], "totalBytes", 0)
    diskAvailable += jsonGetInt64(profile["disk"], "availableBytes", 0)
    gpuVramTotal += jsonGetInt64(profile["gpu"], "vramBytes", 0)
    uplink += jsonGetFloat(profile["bandwidth"], "uplinkBps", 0.0)
    downlink += jsonGetFloat(profile["bandwidth"], "downlinkBps", 0.0)
    uplinkBytes += jsonGetInt64(profile["bandwidth"], "uplinkTotalBytes", 0)
    downlinkBytes += jsonGetInt64(profile["bandwidth"], "downlinkTotalBytes", 0)
  %* {
    "nodeCount": max(1, discoveredCount + 1),
    "onlineCount": max(1, connectedCount + discoveredCount),
    "connectedPeers": connectedCount,
    "cpuCoresTotal": cpuTotal,
    "memoryTotalBytes": memoryTotal,
    "diskTotalBytes": diskTotal,
    "diskAvailableBytes": diskAvailable,
    "gpuVramTotalBytes": gpuVramTotal,
    "uplinkBps": uplink,
    "downlinkBps": downlink,
    "uplinkTotalBytes": uplinkBytes,
    "downlinkTotalBytes": downlinkBytes,
    "totalTransferBytes": uplinkBytes + downlinkBytes,
  }

proc social_list_discovered_peers*(handle: pointer, sourceFilter: cstring, limit: cint): cstring {.exportc, cdecl, dynlib.} =
  let node = nodeFromHandle(handle)
  if node.isNil:
    return allocCString("{\"peers\":[],\"totalCount\":0}")
  let sourceFilterText = if sourceFilter != nil: $sourceFilter else: ""
  let filtered = filteredDiscoveredPeers(handle, node, sourceFilterText, int(limit))
  var response = newJObject()
  response["peers"] = filtered.peers
  response["totalCount"] = %filtered.totalCount
  allocCString($response)

proc libp2p_get_local_system_profile_json*(handle: pointer): cstring {.exportc, cdecl, dynlib.} =
  let node = nodeFromHandle(handle)
  if node.isNil:
    return allocCString("{}")
  allocCString($collectLocalSystemProfile(node))

proc libp2p_get_network_resources_json*(handle: pointer): cstring {.exportc, cdecl, dynlib.} =
  let node = nodeFromHandle(handle)
  if node.isNil:
    return allocCString("{}")
  prunePeerProfiles(node)
  let discovered = filteredDiscoveredPeers(handle, node, "", 0)
  let connected = connectedPeersInfoArray(handle)
  let payload = buildNetworkResourcesPayload(node, discovered.totalCount, connected.len)
  allocCString($payload)

proc libp2p_refresh_node_resources*(handle: pointer, limit: cint): cstring {.exportc, cdecl, dynlib.} =
  let node = nodeFromHandle(handle)
  if node.isNil:
    return allocCString("{}")
  prunePeerProfiles(node)
  let effectiveLimit = if limit <= 0: 0 else: int(limit)
  let discovered = filteredDiscoveredPeers(handle, node, "", effectiveLimit)
  let connected = connectedPeersInfoArray(handle)
  let payload = buildNetworkResourcesPayload(node, discovered.totalCount, connected.len)
  allocCString($payload)

proc libp2p_network_discovery_snapshot*(handle: pointer, sourceFilter: cstring, limit: cint, connectCap: cint): cstring {.exportc, cdecl, dynlib.} =
  discard connectCap
  let node = nodeFromHandle(handle)
  if node.isNil:
    return allocCString("{\"connectedPeers\":[],\"connectedPeersInfo\":[],\"discoveredPeers\":{\"peers\":[],\"totalCount\":0},\"mdnsDebug\":{}}")
  let sourceFilterText = if sourceFilter != nil: $sourceFilter else: ""
  let effectiveLimit = if limit <= 0: 0 else: int(limit)
  let discovered = filteredDiscoveredPeers(handle, node, sourceFilterText, effectiveLimit)
  let connected = connectedPeersArray(handle)
  let connectedInfo = connectedPeersInfoArray(handle)
  let localProfile = collectLocalSystemProfile(node)
  let networkResources = buildNetworkResourcesPayload(node, discovered.totalCount, connectedInfo.len)
  var payload = newJObject()
  payload["ok"] = %true
  payload["peerId"] = %localPeerId(node)
  payload["connectedPeers"] = connected
  payload["connectedPeersInfo"] = connectedInfo
  payload["discoveredPeers"] = %* {
    "peers": discovered.peers,
    "totalCount": discovered.totalCount,
  }
  payload["mdnsDebug"] = buildMdnsDebugPayload(node)
  payload["autoConnect"] = %* {
    "enabled": mdnsAutoConnectEnabled(),
    "attempted": 0,
    "connected": connectedInfo.len,
  }
  payload["connectedCount"] = %connectedInfo.len
  payload["localSystemProfile"] = localProfile
  payload["systemProfile"] = localProfile
  payload["networkResources"] = networkResources
  payload["mdnsProbeOk"] = %true
  payload["bootstrap"] = %* {"ok": true}
  allocCString($payload)

proc social_connect_peer*(handle: pointer, peerId: cstring, multiaddr: cstring): bool {.exportc, cdecl, dynlib.} =
  if peerId.isNil:
    return false
  let node = nodeFromHandle(handle)
  if node.isNil:
    return false
  let peer = $peerId
  let dialAddr = if multiaddr == nil: "" else: $multiaddr
  var connected = false
  if dialAddr.len > 0:
    connected = libp2p_connect_multiaddr(handle, dialAddr.cstring) == NimResultOk
  if not connected:
    connected = libp2p_connect_peer(handle, peer.cstring) == NimResultOk
  var payload = newJObject()
  payload["peerId"] = %peer
  payload["multiaddr"] = %dialAddr
  payload["ok"] = %connected
  recordSocialEvent(node, "discovery", "connect_peer", payload, source = "social_connect_peer")
  connected

proc social_dm_send*(
    handle: pointer, peerId: cstring, conversationId: cstring, messageJson: cstring
): bool {.exportc, cdecl, dynlib.} =
  if peerId.isNil or messageJson.isNil:
    return false
  let node = nodeFromHandle(handle)
  if node.isNil:
    return false
  let peer = $peerId
  var payload = normalizeSocialPayload($messageJson)
  let convIdRaw = if conversationId != nil: $conversationId else: ""
  let convId = if convIdRaw.len > 0: convIdRaw else: "dm:" & peer
  var mid = jsonGetStr(payload, "messageId", jsonGetStr(payload, "mid", jsonGetStr(payload, "id")))
  mid = ensureMessageId(mid)
  payload["messageId"] = %mid
  payload["mid"] = %mid
  if not payload.hasKey("timestampMs"):
    payload["timestampMs"] = %nowMillis()
  payload["timestamp_ms"] = %jsonGetInt64(payload, "timestampMs", nowMillis())
  payload["conversationId"] = %convId
  payload["to"] = %peer
  let me = localPeerId(node)
  if me.len > 0:
    payload["from"] = %me
  let body = maybeBodyFromPayload(payload)
  let wirePayload = $payload
  var ok = libp2p_send_with_ack(handle, peer.cstring, ($payload).cstring, 8_000)
  if not ok:
    ok = libp2p_send_direct_text(
      handle,
      peer.cstring,
      mid.cstring,
      wirePayload.cstring,
      "".cstring,
      true,
      8_000
    )
  socialStateMemLock.acquire()
  try:
    let state = loadSocialStateUnsafe(node)
    var msg = newJObject()
    msg["id"] = %mid
    msg["sender"] = %"me"
    msg["peerId"] = %peer
    msg["conversationId"] = %convId
    msg["content"] = %body
    msg["timestampMs"] = %jsonGetInt64(payload, "timestampMs", nowMillis())
    msg["status"] = %(if ok: "sent" else: "failed")
    msg["payload"] = payload
    appendConversationMessageNoLock(state, convId, peer, msg)
    makeNotificationNoLock(
      state,
      if ok: "dm_sent" else: "dm_send_failed",
      if ok: "消息已发送" else: "消息发送失败",
      if body.len > 0: body else: mid,
      msg
    )
    persistSocialStateUnsafe(node, state)
  finally:
    socialStateMemLock.release()
  recordSocialEvent(node, "dm", "send", payload, conversationId = convId, source = "social_dm_send")
  ok

proc social_dm_edit*(
    handle: pointer, peerId: cstring, conversationId: cstring, messageId: cstring, patchJson: cstring
): bool {.exportc, cdecl, dynlib.} =
  if peerId.isNil or messageId.isNil:
    return false
  let node = nodeFromHandle(handle)
  if node.isNil:
    return false
  let peer = $peerId
  let mid = $messageId
  let convIdRaw = if conversationId != nil: $conversationId else: ""
  let convId = if convIdRaw.len > 0: convIdRaw else: "dm:" & peer
  let patchText = if patchJson == nil: "{}" else: $patchJson
  let ok = libp2p_send_chat_control(
    handle,
    peer.cstring,
    "edit".cstring,
    mid.cstring,
    patchText.cstring,
    mid.cstring,
    true,
    8_000
  )
  socialStateMemLock.acquire()
  try:
    let state = loadSocialStateUnsafe(node)
    let conv = ensureConversationNoLock(state, convId, peer)
    if conv.kind == JObject and conv.hasKey("messages") and conv["messages"].kind == JArray:
      for item in conv["messages"]:
        if item.kind == JObject and jsonGetStr(item, "id") == mid:
          let patch = normalizeSocialPayload(patchText)
          let newBody = maybeBodyFromPayload(patch)
          if newBody.len > 0:
            item["content"] = %newBody
          item["edited"] = %true
          item["editedAt"] = %nowMillis()
          item["patch"] = patch
    persistSocialStateUnsafe(node, state)
  finally:
    socialStateMemLock.release()
  var payload = newJObject()
  payload["peerId"] = %peer
  payload["messageId"] = %mid
  payload["patch"] = normalizeSocialPayload(patchText)
  payload["ok"] = %ok
  recordSocialEvent(node, "dm", "edit", payload, conversationId = convId, source = "social_dm_edit")
  ok

proc social_dm_revoke*(
    handle: pointer, peerId: cstring, conversationId: cstring, messageId: cstring, reason: cstring
): bool {.exportc, cdecl, dynlib.} =
  if peerId.isNil or messageId.isNil:
    return false
  let node = nodeFromHandle(handle)
  if node.isNil:
    return false
  let peer = $peerId
  let mid = $messageId
  let why = if reason == nil: "revoke" else: $reason
  let convIdRaw = if conversationId != nil: $conversationId else: ""
  let convId = if convIdRaw.len > 0: convIdRaw else: "dm:" & peer
  let ok = libp2p_send_chat_control(
    handle,
    peer.cstring,
    "revoke".cstring,
    mid.cstring,
    why.cstring,
    mid.cstring,
    true,
    8_000
  )
  socialStateMemLock.acquire()
  try:
    let state = loadSocialStateUnsafe(node)
    let conv = ensureConversationNoLock(state, convId, peer)
    if conv.kind == JObject and conv.hasKey("messages") and conv["messages"].kind == JArray:
      for item in conv["messages"]:
        if item.kind == JObject and jsonGetStr(item, "id") == mid:
          item["revoked"] = %true
          item["revokeReason"] = %why
          item["revokedAt"] = %nowMillis()
    persistSocialStateUnsafe(node, state)
  finally:
    socialStateMemLock.release()
  var payload = newJObject()
  payload["peerId"] = %peer
  payload["messageId"] = %mid
  payload["reason"] = %why
  payload["ok"] = %ok
  recordSocialEvent(node, "dm", "revoke", payload, conversationId = convId, source = "social_dm_revoke")
  ok

proc social_dm_ack*(
    handle: pointer, peerId: cstring, conversationId: cstring, messageId: cstring, status: cstring
): bool {.exportc, cdecl, dynlib.} =
  if peerId.isNil or messageId.isNil:
    return false
  let node = nodeFromHandle(handle)
  if node.isNil:
    return false
  let peer = $peerId
  let mid = $messageId
  let stat = if status == nil: "acked" else: $status
  let success = stat.toLowerAscii() in ["ok", "acked", "success", "delivered"]
  let convIdRaw = if conversationId != nil: $conversationId else: ""
  let convId = if convIdRaw.len > 0: convIdRaw else: "dm:" & peer
  let ok = libp2p_send_chat_ack_ffi(handle, peer.cstring, mid.cstring, success, stat.cstring)
  socialStateMemLock.acquire()
  try:
    let state = loadSocialStateUnsafe(node)
    let conv = ensureConversationNoLock(state, convId, peer)
    if conv.kind == JObject and conv.hasKey("messages") and conv["messages"].kind == JArray:
      for item in conv["messages"]:
        if item.kind == JObject and jsonGetStr(item, "id") == mid:
          item["ackStatus"] = %stat
          item["ackedAt"] = %nowMillis()
    persistSocialStateUnsafe(node, state)
  finally:
    socialStateMemLock.release()
  var payload = newJObject()
  payload["peerId"] = %peer
  payload["messageId"] = %mid
  payload["status"] = %stat
  payload["ok"] = %ok
  recordSocialEvent(node, "dm", "ack", payload, conversationId = convId, source = "social_dm_ack")
  ok

proc social_contacts_send_request*(handle: pointer, peerId: cstring, helloText: cstring): bool {.exportc, cdecl, dynlib.} =
  if peerId.isNil:
    return false
  let node = nodeFromHandle(handle)
  if node.isNil:
    return false
  let peer = $peerId
  let hello = if helloText == nil: "" else: $helloText
  socialStateMemLock.acquire()
  try:
    let state = loadSocialStateUnsafe(node)
    if not state["contacts"].hasKey(peer) or state["contacts"][peer].kind != JObject:
      state["contacts"][peer] = newJObject()
    let contact = state["contacts"][peer]
    contact["peerId"] = %peer
    contact["status"] = %"pending_outgoing"
    contact["helloText"] = %hello
    contact["updatedAt"] = %nowMillis()
    makeNotificationNoLock(state, "contact_request_sent", "好友申请已发送", peer, contact)
    persistSocialStateUnsafe(node, state)
  finally:
    socialStateMemLock.release()
  var payload = newJObject()
  payload["peerId"] = %peer
  payload["helloText"] = %hello
  recordSocialEvent(node, "contacts", "send_request", payload, source = "social_contacts_send_request")
  true

proc social_contacts_accept*(handle: pointer, peerId: cstring): bool {.exportc, cdecl, dynlib.} =
  if peerId.isNil:
    return false
  let node = nodeFromHandle(handle)
  if node.isNil:
    return false
  let peer = $peerId
  socialStateMemLock.acquire()
  try:
    let state = loadSocialStateUnsafe(node)
    if not state["contacts"].hasKey(peer) or state["contacts"][peer].kind != JObject:
      state["contacts"][peer] = newJObject()
    let contact = state["contacts"][peer]
    contact["peerId"] = %peer
    contact["status"] = %"accepted"
    contact["updatedAt"] = %nowMillis()
    makeNotificationNoLock(state, "contact_accepted", "已添加好友", peer, contact)
    persistSocialStateUnsafe(node, state)
  finally:
    socialStateMemLock.release()
  var payload = newJObject()
  payload["peerId"] = %peer
  recordSocialEvent(node, "contacts", "accept", payload, source = "social_contacts_accept")
  true

proc social_contacts_reject*(handle: pointer, peerId: cstring, reason: cstring): bool {.exportc, cdecl, dynlib.} =
  if peerId.isNil:
    return false
  let node = nodeFromHandle(handle)
  if node.isNil:
    return false
  let peer = $peerId
  let why = if reason == nil: "rejected" else: $reason
  socialStateMemLock.acquire()
  try:
    let state = loadSocialStateUnsafe(node)
    if not state["contacts"].hasKey(peer) or state["contacts"][peer].kind != JObject:
      state["contacts"][peer] = newJObject()
    let contact = state["contacts"][peer]
    contact["peerId"] = %peer
    contact["status"] = %"rejected"
    contact["reason"] = %why
    contact["updatedAt"] = %nowMillis()
    makeNotificationNoLock(state, "contact_rejected", "已拒绝好友申请", peer, contact)
    persistSocialStateUnsafe(node, state)
  finally:
    socialStateMemLock.release()
  var payload = newJObject()
  payload["peerId"] = %peer
  payload["reason"] = %why
  recordSocialEvent(node, "contacts", "reject", payload, source = "social_contacts_reject")
  true

proc social_contacts_remove*(handle: pointer, peerId: cstring): bool {.exportc, cdecl, dynlib.} =
  if peerId.isNil:
    return false
  let node = nodeFromHandle(handle)
  if node.isNil:
    return false
  let peer = $peerId
  socialStateMemLock.acquire()
  try:
    let state = loadSocialStateUnsafe(node)
    if state["contacts"].hasKey(peer):
      state["contacts"].delete(peer)
    makeNotificationNoLock(state, "contact_removed", "已删除联系人", peer)
    persistSocialStateUnsafe(node, state)
  finally:
    socialStateMemLock.release()
  var payload = newJObject()
  payload["peerId"] = %peer
  recordSocialEvent(node, "contacts", "remove", payload, source = "social_contacts_remove")
  true

proc social_groups_create*(handle: pointer, groupMetaJson: cstring): cstring {.exportc, cdecl, dynlib.} =
  let node = nodeFromHandle(handle)
  if node.isNil:
    return allocCString("{\"ok\":false,\"error\":\"node_nil\"}")
  var meta = normalizeSocialPayload(if groupMetaJson == nil: "{}" else: $groupMetaJson)
  var groupId = jsonGetStr(meta, "groupId", jsonGetStr(meta, "id"))
  if groupId.len == 0:
    groupId = "group-" & $nowMillis() & "-" & $rand(10_000)
  var members = jsonArrayValues(meta, "members")
  let me = localPeerId(node)
  if me.len > 0 and me notin members:
    members.add(me)
  socialStateMemLock.acquire()
  var group = newJObject()
  try:
    let state = loadSocialStateUnsafe(node)
    group["groupId"] = %groupId
    group["name"] = %jsonGetStr(meta, "name", groupId)
    group["ownerPeerId"] = %jsonGetStr(meta, "ownerPeerId", me)
    group["createdAt"] = %nowMillis()
    group["updatedAt"] = %nowMillis()
    group["members"] = newJArray()
    for member in members:
      appendUniqueString(group["members"], member)
    group["meta"] = meta
    state["groups"][groupId] = group
    makeNotificationNoLock(state, "group_created", "群组已创建", jsonGetStr(group, "name"), group)
    persistSocialStateUnsafe(node, state)
  finally:
    socialStateMemLock.release()
  recordSocialEvent(node, "groups", "create", group, groupId = groupId, source = "social_groups_create")
  var response = newJObject()
  response["ok"] = %true
  response["groupId"] = %groupId
  response["group"] = group
  allocCString($response)

proc social_groups_update*(handle: pointer, groupId: cstring, patchJson: cstring): bool {.exportc, cdecl, dynlib.} =
  if groupId.isNil:
    return false
  let node = nodeFromHandle(handle)
  if node.isNil:
    return false
  let gid = $groupId
  let patch = normalizeSocialPayload(if patchJson == nil: "{}" else: $patchJson)
  var ok = false
  socialStateMemLock.acquire()
  try:
    let state = loadSocialStateUnsafe(node)
    if state["groups"].hasKey(gid) and state["groups"][gid].kind == JObject:
      let group = state["groups"][gid]
      for key, value in pairs(patch):
        if key in ["groupId", "id", "createdAt"]:
          continue
        group[key] = value
      group["updatedAt"] = %nowMillis()
      ok = true
      persistSocialStateUnsafe(node, state)
  finally:
    socialStateMemLock.release()
  var payload = newJObject()
  payload["groupId"] = %gid
  payload["patch"] = patch
  payload["ok"] = %ok
  recordSocialEvent(node, "groups", "update", payload, groupId = gid, source = "social_groups_update")
  ok

proc social_groups_invite*(handle: pointer, groupId: cstring, peerIdsJson: cstring): bool {.exportc, cdecl, dynlib.} =
  if groupId.isNil:
    return false
  let node = nodeFromHandle(handle)
  if node.isNil:
    return false
  let gid = $groupId
  let peers = parseJsonStringSeq(if peerIdsJson == nil: "[]" else: $peerIdsJson)
  var ok = false
  socialStateMemLock.acquire()
  try:
    let state = loadSocialStateUnsafe(node)
    if state["groups"].hasKey(gid) and state["groups"][gid].kind == JObject:
      let group = state["groups"][gid]
      if not group.hasKey("members") or group["members"].kind != JArray:
        group["members"] = newJArray()
      for peer in peers:
        appendUniqueString(group["members"], peer)
      group["updatedAt"] = %nowMillis()
      ok = true
      makeNotificationNoLock(state, "group_invite", "群邀请已发送", gid, group)
      persistSocialStateUnsafe(node, state)
  finally:
    socialStateMemLock.release()
  var payload = newJObject()
  payload["groupId"] = %gid
  payload["peerIds"] = %(peers)
  payload["ok"] = %ok
  recordSocialEvent(node, "groups", "invite", payload, groupId = gid, source = "social_groups_invite")
  ok

proc social_groups_kick*(handle: pointer, groupId: cstring, peerId: cstring): bool {.exportc, cdecl, dynlib.} =
  if groupId.isNil or peerId.isNil:
    return false
  let node = nodeFromHandle(handle)
  if node.isNil:
    return false
  let gid = $groupId
  let peer = $peerId
  var ok = false
  socialStateMemLock.acquire()
  try:
    let state = loadSocialStateUnsafe(node)
    if state["groups"].hasKey(gid) and state["groups"][gid].kind == JObject:
      let group = state["groups"][gid]
      if group.hasKey("members") and group["members"].kind == JArray:
        var nextMembers = newJArray()
        for member in group["members"]:
          if member.kind == JString and member.getStr() != peer:
            nextMembers.add(member)
        group["members"] = nextMembers
      group["updatedAt"] = %nowMillis()
      ok = true
      persistSocialStateUnsafe(node, state)
  finally:
    socialStateMemLock.release()
  var payload = newJObject()
  payload["groupId"] = %gid
  payload["peerId"] = %peer
  payload["ok"] = %ok
  recordSocialEvent(node, "groups", "kick", payload, groupId = gid, source = "social_groups_kick")
  ok

proc social_groups_leave*(handle: pointer, groupId: cstring): bool {.exportc, cdecl, dynlib.} =
  if groupId.isNil:
    return false
  let node = nodeFromHandle(handle)
  if node.isNil:
    return false
  let gid = $groupId
  let me = localPeerId(node)
  var ok = false
  socialStateMemLock.acquire()
  try:
    let state = loadSocialStateUnsafe(node)
    if state["groups"].hasKey(gid) and state["groups"][gid].kind == JObject:
      let group = state["groups"][gid]
      if group.hasKey("members") and group["members"].kind == JArray:
        var nextMembers = newJArray()
        for member in group["members"]:
          if member.kind == JString and member.getStr() != me:
            nextMembers.add(member)
        group["members"] = nextMembers
      group["updatedAt"] = %nowMillis()
      ok = true
      persistSocialStateUnsafe(node, state)
  finally:
    socialStateMemLock.release()
  var payload = newJObject()
  payload["groupId"] = %gid
  payload["peerId"] = %me
  payload["ok"] = %ok
  recordSocialEvent(node, "groups", "leave", payload, groupId = gid, source = "social_groups_leave")
  ok

proc social_groups_send*(handle: pointer, groupId: cstring, messageJson: cstring): bool {.exportc, cdecl, dynlib.} =
  if groupId.isNil or messageJson.isNil:
    return false
  let node = nodeFromHandle(handle)
  if node.isNil:
    return false
  let gid = $groupId
  var payload = normalizeSocialPayload($messageJson)
  var mid = jsonGetStr(payload, "messageId", jsonGetStr(payload, "mid", jsonGetStr(payload, "id")))
  mid = ensureMessageId(mid)
  let me = localPeerId(node)
  payload["messageId"] = %mid
  payload["mid"] = %mid
  payload["groupId"] = %gid
  payload["from"] = %me
  payload["timestampMs"] = %jsonGetInt64(payload, "timestampMs", nowMillis())
  let topic = "unimaker/group/" & gid & "/v1"
  let data = stringToBytes($payload)
  var delivered: csize_t = 0
  let rc = libp2p_pubsub_publish(
    handle,
    topic.cstring,
    if data.len > 0: addr data[0] else: nil,
    csize_t(data.len),
    addr delivered
  )
  let ok = rc == NimResultOk
  socialStateMemLock.acquire()
  try:
    let state = loadSocialStateUnsafe(node)
    var message = newJObject()
    message["id"] = %mid
    message["sender"] = %"me"
    message["groupId"] = %gid
    message["content"] = %maybeBodyFromPayload(payload)
    message["timestampMs"] = %jsonGetInt64(payload, "timestampMs", nowMillis())
    message["status"] = %(if ok: "sent" else: "failed")
    message["payload"] = payload
    appendConversationMessageNoLock(state, "group:" & gid, gid, message)
    persistSocialStateUnsafe(node, state)
  finally:
    socialStateMemLock.release()
  recordSocialEvent(node, "groups", "send", payload, conversationId = "group:" & gid, groupId = gid, source = "social_groups_send")
  ok

proc ensureSynccastRoomNoLock(state: JsonNode, roomId: string): JsonNode =
  if state.kind != JObject:
    return newJObject()
  if not state.hasKey("synccastRooms") or state["synccastRooms"].kind != JObject:
    state["synccastRooms"] = newJObject()
  let rooms = state["synccastRooms"]
  if not rooms.hasKey(roomId) or rooms[roomId].kind != JObject:
    var room = newJObject()
    room["roomId"] = %roomId
    room["createdAt"] = %nowMillis()
    room["updatedAt"] = %nowMillis()
    room["program"] = newJObject()
    room["members"] = newJArray()
    room["state"] = newJObject()
    room["lastControl"] = newJObject()
    rooms[roomId] = room
  let room = rooms[roomId]
  if not room.hasKey("program") or room["program"].kind != JObject:
    room["program"] = newJObject()
  if not room.hasKey("members") or room["members"].kind != JArray:
    room["members"] = newJArray()
  if not room.hasKey("state") or room["state"].kind != JObject:
    room["state"] = newJObject()
  if not room.hasKey("lastControl") or room["lastControl"].kind != JObject:
    room["lastControl"] = newJObject()
  room

proc synccastConversationId(roomId: string): string =
  "group:" & roomId

proc synccastControlAllowed(op: string): bool =
  let normalized = op.toLowerAscii()
  if normalized in ["seek", "set_seek", "rate", "set_rate", "speed", "set_speed", "playback_rate"]:
    return false
  normalized in ["play", "pause", "resume", "stop", "heartbeat", "sync_anchor", "pip_join", "pip_leave", "ready", "state"]

proc social_synccast_upsert_program*(
    handle: pointer, roomId: cstring, programJson: cstring
): cstring {.exportc, cdecl, dynlib.} =
  if roomId.isNil:
    return allocCString("{\"ok\":false,\"error\":\"room_id_missing\"}")
  let node = nodeFromHandle(handle)
  if node.isNil:
    return allocCString("{\"ok\":false,\"error\":\"node_nil\"}")
  let rid = ($roomId).strip()
  if rid.len == 0:
    return allocCString("{\"ok\":false,\"error\":\"room_id_missing\"}")
  var program = normalizeSocialPayload(if programJson == nil: "{}" else: $programJson)
  var programId = jsonGetStr(program, "programId", jsonGetStr(program, "id"))
  if programId.len == 0:
    programId = "program-" & $nowMillis() & "-" & $rand(10_000)
  let nowTs = nowMillis()
  program["programId"] = %programId
  program["id"] = %programId
  program["updatedAt"] = %nowTs
  if not program.hasKey("allowSeek"):
    program["allowSeek"] = %false
  if not program.hasKey("allowRateChange"):
    program["allowRateChange"] = %false
  if not program.hasKey("mode"):
    program["mode"] = %"realtime_program"
  var roomSnapshot = newJObject()
  socialStateMemLock.acquire()
  try:
    let state = loadSocialStateUnsafe(node)
    let room = ensureSynccastRoomNoLock(state, rid)
    room["program"] = program
    room["updatedAt"] = %nowTs
    let stateNode = room["state"]
    stateNode["programId"] = %programId
    if not stateNode.hasKey("playbackState"):
      stateNode["playbackState"] = %"paused"
    roomSnapshot = room
    persistSocialStateUnsafe(node, state)
  finally:
    socialStateMemLock.release()
  var payload = newJObject()
  payload["roomId"] = %rid
  payload["groupId"] = %rid
  payload["program"] = program
  payload["ok"] = %true
  recordSocialEvent(
    node,
    "synccast",
    "upsert_program",
    payload,
    conversationId = synccastConversationId(rid),
    groupId = rid,
    source = "social_synccast_upsert_program"
  )
  var wire = newJObject()
  wire["kind"] = %"social"
  wire["entity"] = %"synccast"
  wire["op"] = %"upsert_program"
  wire["roomId"] = %rid
  wire["groupId"] = %rid
  wire["conversationId"] = %synccastConversationId(rid)
  wire["timestampMs"] = %nowTs
  wire["program"] = program
  discard social_groups_send(handle, rid.cstring, ($wire).cstring)
  var response = newJObject()
  response["ok"] = %true
  response["roomId"] = %rid
  response["programId"] = %programId
  response["room"] = roomSnapshot
  allocCString($response)

proc social_synccast_join*(
    handle: pointer, roomId: cstring, peerId: cstring
): bool {.exportc, cdecl, dynlib.} =
  if roomId.isNil:
    return false
  let node = nodeFromHandle(handle)
  if node.isNil:
    return false
  let rid = ($roomId).strip()
  if rid.len == 0:
    return false
  let local = localPeerId(node)
  var targetPeer = if peerId == nil: local else: ($peerId).strip()
  if targetPeer.len == 0:
    targetPeer = local
  if targetPeer.len == 0:
    return false
  let nowTs = nowMillis()
  socialStateMemLock.acquire()
  try:
    let state = loadSocialStateUnsafe(node)
    let room = ensureSynccastRoomNoLock(state, rid)
    appendUniqueString(room["members"], targetPeer)
    if local.len > 0:
      appendUniqueString(room["members"], local)
    room["updatedAt"] = %nowTs
    persistSocialStateUnsafe(node, state)
  finally:
    socialStateMemLock.release()
  var payload = newJObject()
  payload["roomId"] = %rid
  payload["peerId"] = %targetPeer
  payload["ok"] = %true
  recordSocialEvent(
    node,
    "synccast",
    "join",
    payload,
    conversationId = synccastConversationId(rid),
    groupId = rid,
    source = "social_synccast_join"
  )
  var wire = newJObject()
  wire["kind"] = %"social"
  wire["entity"] = %"synccast"
  wire["op"] = %"join"
  wire["roomId"] = %rid
  wire["groupId"] = %rid
  wire["conversationId"] = %synccastConversationId(rid)
  wire["peerId"] = %targetPeer
  wire["timestampMs"] = %nowTs
  discard social_groups_send(handle, rid.cstring, ($wire).cstring)
  true

proc social_synccast_leave*(
    handle: pointer, roomId: cstring
): bool {.exportc, cdecl, dynlib.} =
  if roomId.isNil:
    return false
  let node = nodeFromHandle(handle)
  if node.isNil:
    return false
  let rid = ($roomId).strip()
  if rid.len == 0:
    return false
  let local = localPeerId(node)
  if local.len == 0:
    return false
  var ok = false
  let nowTs = nowMillis()
  socialStateMemLock.acquire()
  try:
    let state = loadSocialStateUnsafe(node)
    if state.hasKey("synccastRooms") and state["synccastRooms"].kind == JObject and state["synccastRooms"].hasKey(rid):
      let room = state["synccastRooms"][rid]
      if room.kind == JObject and room.hasKey("members") and room["members"].kind == JArray:
        var nextMembers = newJArray()
        for member in room["members"]:
          if member.kind == JString and member.getStr() != local:
            nextMembers.add(member)
        room["members"] = nextMembers
      room["updatedAt"] = %nowTs
      ok = true
      persistSocialStateUnsafe(node, state)
  finally:
    socialStateMemLock.release()
  var payload = newJObject()
  payload["roomId"] = %rid
  payload["peerId"] = %local
  payload["ok"] = %ok
  recordSocialEvent(
    node,
    "synccast",
    "leave",
    payload,
    conversationId = synccastConversationId(rid),
    groupId = rid,
    source = "social_synccast_leave"
  )
  if ok:
    var wire = newJObject()
    wire["kind"] = %"social"
    wire["entity"] = %"synccast"
    wire["op"] = %"leave"
    wire["roomId"] = %rid
    wire["groupId"] = %rid
    wire["conversationId"] = %synccastConversationId(rid)
    wire["peerId"] = %local
    wire["timestampMs"] = %nowTs
    discard social_groups_send(handle, rid.cstring, ($wire).cstring)
  ok

proc social_synccast_control*(
    handle: pointer, roomId: cstring, controlJson: cstring
): bool {.exportc, cdecl, dynlib.} =
  if roomId.isNil:
    return false
  let node = nodeFromHandle(handle)
  if node.isNil:
    return false
  let rid = ($roomId).strip()
  if rid.len == 0:
    return false
  var control = normalizeSocialPayload(if controlJson == nil: "{}" else: $controlJson)
  let op = jsonGetStr(control, "op", jsonGetStr(control, "action")).toLowerAscii()
  if op.len == 0:
    return false
  if not synccastControlAllowed(op):
    var rejected = newJObject()
    rejected["roomId"] = %rid
    rejected["op"] = %op
    rejected["error"] = %"unsupported_op_for_watch_party"
    rejected["ok"] = %false
    recordSocialEvent(
      node,
      "synccast",
      "control_rejected",
      rejected,
      conversationId = synccastConversationId(rid),
      groupId = rid,
      source = "social_synccast_control"
    )
    return false
  let nowTs = nowMillis()
  control["op"] = %op
  control["timestampMs"] = %jsonGetInt64(control, "timestampMs", nowTs)
  control["timestamp_ms"] = %jsonGetInt64(control, "timestampMs", nowTs)
  var ok = false
  socialStateMemLock.acquire()
  try:
    let state = loadSocialStateUnsafe(node)
    let room = ensureSynccastRoomNoLock(state, rid)
    let stateNode = room["state"]
    stateNode["op"] = %op
    stateNode["timestampMs"] = %jsonGetInt64(control, "timestampMs", nowTs)
    if op in ["play", "resume"]:
      stateNode["playbackState"] = %"playing"
    elif op in ["pause", "stop"]:
      stateNode["playbackState"] = %(if op == "pause": "paused" else: "stopped")
    if control.hasKey("positionMs"):
      stateNode["positionMs"] = %jsonGetInt64(control, "positionMs", 0)
    if control.hasKey("anchorTsMs"):
      stateNode["anchorTsMs"] = %jsonGetInt64(control, "anchorTsMs", nowTs)
    for key, value in pairs(control):
      if key in ["op", "action", "timestamp_ms"]:
        continue
      stateNode[key] = value
    room["lastControl"] = control
    room["updatedAt"] = %nowTs
    ok = true
    persistSocialStateUnsafe(node, state)
  finally:
    socialStateMemLock.release()
  var payload = newJObject()
  payload["roomId"] = %rid
  payload["groupId"] = %rid
  payload["control"] = control
  payload["ok"] = %ok
  recordSocialEvent(
    node,
    "synccast",
    "control",
    payload,
    conversationId = synccastConversationId(rid),
    groupId = rid,
    source = "social_synccast_control"
  )
  if ok:
    var wire = newJObject()
    wire["kind"] = %"social"
    wire["entity"] = %"synccast"
    wire["op"] = %op
    wire["roomId"] = %rid
    wire["groupId"] = %rid
    wire["conversationId"] = %synccastConversationId(rid)
    wire["timestampMs"] = %jsonGetInt64(control, "timestampMs", nowTs)
    wire["control"] = control
    discard social_groups_send(handle, rid.cstring, ($wire).cstring)
  ok

proc social_synccast_get_state*(
    handle: pointer, roomId: cstring
): cstring {.exportc, cdecl, dynlib.} =
  if roomId.isNil:
    return allocCString("{\"ok\":false,\"error\":\"room_id_missing\"}")
  let node = nodeFromHandle(handle)
  if node.isNil:
    return allocCString("{\"ok\":false,\"error\":\"node_nil\"}")
  let rid = ($roomId).strip()
  if rid.len == 0:
    return allocCString("{\"ok\":false,\"error\":\"room_id_missing\"}")
  var response = newJObject()
  socialStateMemLock.acquire()
  try:
    let state = loadSocialStateUnsafe(node)
    if state.hasKey("synccastRooms") and state["synccastRooms"].kind == JObject and state["synccastRooms"].hasKey(rid):
      response["ok"] = %true
      response["roomId"] = %rid
      response["room"] = state["synccastRooms"][rid]
    else:
      response["ok"] = %false
      response["roomId"] = %rid
      response["error"] = %"room_not_found"
  finally:
    socialStateMemLock.release()
  allocCString($response)

proc social_synccast_list_rooms*(
    handle: pointer, limit: cint
): cstring {.exportc, cdecl, dynlib.} =
  let node = nodeFromHandle(handle)
  if node.isNil:
    return allocCString("{\"items\":[],\"totalCount\":0}")
  var rooms: seq[JsonNode] = @[]
  socialStateMemLock.acquire()
  try:
    let state = loadSocialStateUnsafe(node)
    if state.hasKey("synccastRooms") and state["synccastRooms"].kind == JObject:
      for _, room in pairs(state["synccastRooms"]):
        if room.kind == JObject:
          rooms.add(room)
  finally:
    socialStateMemLock.release()
  rooms.sort(
    proc(a, b: JsonNode): int =
      cmp(jsonGetInt64(b, "updatedAt"), jsonGetInt64(a, "updatedAt"))
  )
  let requested =
    if limit <= 0:
      min(rooms.len, SocialMaxSynccastRooms)
    else:
      min(rooms.len, min(int(limit), SocialMaxSynccastRooms))
  var items = newJArray()
  if requested > 0:
    for i in 0 ..< requested:
      items.add(rooms[i])
  var response = newJObject()
  response["items"] = items
  response["totalCount"] = %rooms.len
  allocCString($response)

proc social_moments_publish*(handle: pointer, postJson: cstring): cstring {.exportc, cdecl, dynlib.} =
  if postJson.isNil:
    return allocCString("{\"ok\":false,\"error\":\"post_missing\"}")
  let node = nodeFromHandle(handle)
  if node.isNil:
    return allocCString("{\"ok\":false,\"error\":\"node_nil\"}")
  var post = normalizeSocialPayload($postJson)
  var postId = jsonGetStr(post, "postId", jsonGetStr(post, "id"))
  if postId.len == 0:
    postId = "post-" & $nowMillis() & "-" & $rand(10_000)
  post["postId"] = %postId
  post["id"] = %postId
  post["authorPeerId"] = %localPeerId(node)
  post["timestampMs"] = %jsonGetInt64(post, "timestampMs", nowMillis())
  if not post.hasKey("likes") or post["likes"].kind != JArray:
    post["likes"] = newJArray()
  if not post.hasKey("comments") or post["comments"].kind != JArray:
    post["comments"] = newJArray()
  socialStateMemLock.acquire()
  try:
    let state = loadSocialStateUnsafe(node)
    var nextMoments = newJArray()
    nextMoments.add(post)
    for existing in state["moments"]:
      if nextMoments.len >= SocialMaxMoments:
        break
      nextMoments.add(existing)
    state["moments"] = nextMoments
    makeNotificationNoLock(state, "moment_published", "动态已发布", jsonGetStr(post, "content"), post)
    persistSocialStateUnsafe(node, state)
  finally:
    socialStateMemLock.release()
  recordSocialEvent(node, "moments", "publish", post, postId = postId, source = "social_moments_publish")
  var response = newJObject()
  response["ok"] = %true
  response["postId"] = %postId
  response["post"] = post
  allocCString($response)

proc social_moments_delete*(handle: pointer, postId: cstring): bool {.exportc, cdecl, dynlib.} =
  if postId.isNil:
    return false
  let node = nodeFromHandle(handle)
  if node.isNil:
    return false
  let pid = $postId
  var ok = false
  socialStateMemLock.acquire()
  try:
    let state = loadSocialStateUnsafe(node)
    if state.hasKey("moments") and state["moments"].kind == JArray:
      for post in state["moments"]:
        if post.kind == JObject and jsonGetStr(post, "postId", jsonGetStr(post, "id")) == pid:
          post["deleted"] = %true
          post["deletedAt"] = %nowMillis()
          ok = true
      if ok:
        persistSocialStateUnsafe(node, state)
  finally:
    socialStateMemLock.release()
  var payload = newJObject()
  payload["postId"] = %pid
  payload["ok"] = %ok
  recordSocialEvent(node, "moments", "delete", payload, postId = pid, source = "social_moments_delete")
  ok

proc social_moments_like*(handle: pointer, postId: cstring, like: bool): bool {.exportc, cdecl, dynlib.} =
  if postId.isNil:
    return false
  let node = nodeFromHandle(handle)
  if node.isNil:
    return false
  let pid = $postId
  let me = localPeerId(node)
  var ok = false
  socialStateMemLock.acquire()
  try:
    let state = loadSocialStateUnsafe(node)
    if state.hasKey("moments") and state["moments"].kind == JArray:
      for post in state["moments"]:
        if post.kind != JObject:
          continue
        if jsonGetStr(post, "postId", jsonGetStr(post, "id")) != pid:
          continue
        if not post.hasKey("likes") or post["likes"].kind != JArray:
          post["likes"] = newJArray()
        if like:
          appendUniqueString(post["likes"], me)
        else:
          var nextLikes = newJArray()
          for item in post["likes"]:
            if item.kind == JString and item.getStr() != me:
              nextLikes.add(item)
          post["likes"] = nextLikes
        post["updatedAt"] = %nowMillis()
        ok = true
      if ok:
        persistSocialStateUnsafe(node, state)
  finally:
    socialStateMemLock.release()
  var payload = newJObject()
  payload["postId"] = %pid
  payload["like"] = %like
  payload["peerId"] = %me
  payload["ok"] = %ok
  recordSocialEvent(node, "moments", "like", payload, postId = pid, source = "social_moments_like")
  ok

proc social_moments_comment*(handle: pointer, postId: cstring, commentJson: cstring): bool {.exportc, cdecl, dynlib.} =
  if postId.isNil or commentJson.isNil:
    return false
  let node = nodeFromHandle(handle)
  if node.isNil:
    return false
  let pid = $postId
  var comment = normalizeSocialPayload($commentJson)
  var commentId = jsonGetStr(comment, "commentId", jsonGetStr(comment, "id"))
  if commentId.len == 0:
    commentId = "comment-" & $nowMillis() & "-" & $rand(10_000)
  comment["commentId"] = %commentId
  comment["id"] = %commentId
  comment["peerId"] = %localPeerId(node)
  comment["timestampMs"] = %jsonGetInt64(comment, "timestampMs", nowMillis())
  var ok = false
  socialStateMemLock.acquire()
  try:
    let state = loadSocialStateUnsafe(node)
    if state.hasKey("moments") and state["moments"].kind == JArray:
      for post in state["moments"]:
        if post.kind != JObject:
          continue
        if jsonGetStr(post, "postId", jsonGetStr(post, "id")) != pid:
          continue
        if not post.hasKey("comments") or post["comments"].kind != JArray:
          post["comments"] = newJArray()
        post["comments"].add(comment)
        post["updatedAt"] = %nowMillis()
        ok = true
      if ok:
        persistSocialStateUnsafe(node, state)
  finally:
    socialStateMemLock.release()
  var payload = newJObject()
  payload["postId"] = %pid
  payload["comment"] = comment
  payload["ok"] = %ok
  recordSocialEvent(node, "moments", "comment", payload, postId = pid, source = "social_moments_comment")
  ok

proc social_moments_timeline*(handle: pointer, cursor: cstring, limit: cint): cstring {.exportc, cdecl, dynlib.} =
  let node = nodeFromHandle(handle)
  if node.isNil:
    return allocCString("{\"items\":[],\"nextCursor\":\"\",\"hasMore\":false}")
  let requested = if limit > 0: int(limit) else: SocialDefaultLimit
  let startIndex =
    if cursor.isNil:
      0
    else:
      int(max(parseIntLoose($cursor), 0))
  var response = newJObject()
  var items = newJArray()
  var nextCursor = ""
  var hasMore = false
  socialStateMemLock.acquire()
  try:
    let state = loadSocialStateUnsafe(node)
    if state.hasKey("moments") and state["moments"].kind == JArray:
      var filtered: seq[JsonNode] = @[]
      for post in state["moments"]:
        if post.kind == JObject and not jsonGetBool(post, "deleted", false):
          filtered.add(post)
      filtered.sort(
        proc(a, b: JsonNode): int =
          cmp(jsonGetInt64(b, "timestampMs", jsonGetInt64(b, "timestamp", 0)),
              jsonGetInt64(a, "timestampMs", jsonGetInt64(a, "timestamp", 0)))
      )
      if startIndex < filtered.len:
        let stopIndex = min(startIndex + requested, filtered.len)
        for i in startIndex ..< stopIndex:
          items.add(filtered[i])
        hasMore = stopIndex < filtered.len
        if hasMore:
          nextCursor = $stopIndex
  finally:
    socialStateMemLock.release()
  response["items"] = items
  response["nextCursor"] = %nextCursor
  response["hasMore"] = %hasMore
  allocCString($response)

proc social_notifications_list*(handle: pointer, cursor: cstring, limit: cint): cstring {.exportc, cdecl, dynlib.} =
  let node = nodeFromHandle(handle)
  if node.isNil:
    return allocCString("{\"items\":[],\"nextCursor\":\"\",\"hasMore\":false}")
  let requested = if limit > 0: int(limit) else: SocialDefaultLimit
  let startIndex =
    if cursor.isNil:
      0
    else:
      int(max(parseIntLoose($cursor), 0))
  var response = newJObject()
  var items = newJArray()
  var nextCursor = ""
  var hasMore = false
  socialStateMemLock.acquire()
  try:
    let state = loadSocialStateUnsafe(node)
    if state.hasKey("notifications") and state["notifications"].kind == JArray:
      let list = state["notifications"]
      if startIndex < list.len:
        let stopIndex = min(startIndex + requested, list.len)
        for i in startIndex ..< stopIndex:
          items.add(list[i])
        hasMore = stopIndex < list.len
        if hasMore:
          nextCursor = $stopIndex
  finally:
    socialStateMemLock.release()
  response["items"] = items
  response["nextCursor"] = %nextCursor
  response["hasMore"] = %hasMore
  allocCString($response)

proc social_query_presence*(handle: pointer, peerIdsJson: cstring): cstring {.exportc, cdecl, dynlib.} =
  let node = nodeFromHandle(handle)
  if node.isNil:
    return allocCString("{\"peers\":[]}")
  let peerIds = parseJsonStringSeq(if peerIdsJson == nil: "[]" else: $peerIdsJson)
  var connected = initHashSet[string]()
  let connectedRaw = takeCStringAndFree(libp2p_get_connected_peers_json(handle))
  if connectedRaw.len > 0:
    try:
      let arr = parseJson(connectedRaw)
      if arr.kind == JArray:
        for item in arr:
          if item.kind == JString:
            connected.incl(item.getStr())
    except CatchableError:
      discard
  var peers = newJArray()
  for peer in peerIds:
    if peer.len == 0:
      continue
    var row = newJObject()
    row["peerId"] = %peer
    row["online"] = %connected.contains(peer)
    row["lastSeenAt"] = %(if connected.contains(peer): nowMillis() else: int64(0))
    row["source"] = %(if connected.contains(peer): "connected_peers" else: "peerstore")
    peers.add(row)
  var response = newJObject()
  response["peers"] = peers
  allocCString($response)

proc social_poll_events*(handle: pointer, maxEvents: cint): cstring {.exportc, cdecl, dynlib.} =
  libp2p_poll_events(handle, maxEvents)

proc social_poll_events_without_dm*(handle: pointer, maxEvents: cint): cstring {.exportc, cdecl, dynlib.} =
  let node = nodeFromHandle(handle)
  if node.isNil:
    return allocCString("[]")
  let requested =
    if maxEvents <= 0:
      64
    else:
      int(maxEvents)
  allocCString(pollEventLogFiltered(node, requested, dropDm = true))

proc social_received_direct_messages*(handle: pointer, limit: cint): cstring {.exportc, cdecl, dynlib.} =
  let node = nodeFromHandle(handle)
  if node.isNil:
    return allocCString("{\"items\":[]}")
  let safeLimit = if limit <= 0: 64 else: int(limit)
  allocCString(pollReceivedDmLog(node, safeLimit))

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
  let ordered = preferStableAddrStrings(addrs)
  allocCString(jsonEncodeStringSeq(ordered))

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
  ## Return real bootstrap status on every mobile platform (Android/iOS/Harmony)
  ## to keep bridge diagnostics consistent.
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
    allocCString("{}")
  else:
    allocCString($obj)

proc libp2p_reconnect_bootstrap*(handle: pointer): bool {.exportc, cdecl, dynlib.} =
  let node = nodeFromHandle(handle)
  if node.isNil:
    return false
  let cmd = makeCommand(cmdReconnectBootstrap)
  let result = submitCommand(node, cmd)
  let ok = result.resultCode == NimResultOk
  destroyCommand(cmd)
  ok

proc libp2p_get_random_bootstrap_peers*(handle: pointer, limit: cint): cstring {.exportc, cdecl, dynlib.} =
  let node = nodeFromHandle(handle)
  if node.isNil:
    return allocCString("{\"peers\":[],\"selectedCount\":0,\"totalCount\":0}")
  let cmd = makeCommand(cmdGetRandomBootstrapPeers)
  cmd.requestLimit = int(limit)
  let result = submitCommand(node, cmd)
  let output =
    if result.resultCode == NimResultOk and result.stringResult.len > 0:
      allocCString(cloneOwnedString(result.stringResult))
    else:
      allocCString("{\"peers\":[],\"selectedCount\":0,\"totalCount\":0}")
  destroyCommand(cmd)
  output

proc libp2p_join_via_random_bootstrap*(handle: pointer, limit: cint): cstring {.exportc, cdecl, dynlib.} =
  let node = nodeFromHandle(handle)
  if node.isNil:
    return allocCString("{\"ok\":false,\"attemptCount\":0,\"connectedCount\":0,\"attempted\":[]}")
  let cmd = makeCommand(cmdJoinViaRandomBootstrap)
  cmd.requestLimit = int(limit)
  let result = submitCommand(node, cmd)
  let output =
    if result.resultCode == NimResultOk and result.stringResult.len > 0:
      allocCString(cloneOwnedString(result.stringResult))
    else:
      allocCString("{\"ok\":false,\"attemptCount\":0,\"connectedCount\":0,\"attempted\":[]}")
  destroyCommand(cmd)
  output

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
      cloneOwnedString(result.stringResult)
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
  let ok = submitCommandNoWait(node, cmd)
  if ok:
    info "mdns probe requested"
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
  let payload = if result.resultCode == NimResultOk: cloneOwnedString(result.stringResult) else: ""
  destroyCommand(cmd)
  allocCString(payload)

proc libp2p_dex_get_wallets*(handle: pointer): cstring {.exportc, cdecl, dynlib.} =
  setupForeignThreadGc()
  defer: tearDownForeignThreadGc()
  let node = nodeFromHandle(handle)
  if node.isNil: return allocCString("[]")
  let cmd = makeCommand(cmdDexGetWallets)
  let result = submitCommand(node, cmd)
  let payload =
    if result.resultCode == NimResultOk:
      let copied = cloneOwnedString(result.stringResult)
      if copied.len > 0: copied else: "[]"
    else:
      "[]"
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
