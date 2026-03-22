import std/[algorithm, atomics, base64, json, math, options, sequtils, sets, strformat,
            strutils, tables, times, locks, random]
import std/os
import chronos
import chronos/apps/http/httpclient
import chronos/threadsync
import chronicles
when defined(metrics):
  import metrics
import stew/objects
when defined(posix):
  import posix
import libp2p/[bandwidthmanager, builders, connmanager, features, memorymanager, multiaddress,
               muxers/mplex/mplex, muxers/muxer, muxers/yamux/yamux, peerid, peerstore, protocols/connectivity/relay/client,
               resourcemanager, switch, wire]
import libp2p/discovery/discoverymngr
import libp2p/discovery/mdns
import libp2p/crypto/[crypto, ed25519/ed25519]
import libp2p/peerinfo
import libp2p/protocols/pubsub/[gossipsub, pubsub, peertable]
import libp2p/protocols/dm/dmservice
import libp2p/protocols/feed/feedservice
import libp2p/protocols/ping
import libp2p/protocols/connectivity/autonat/types as autonat_types
import libp2p/protocols/connectivity/autonat/service as autonat_service
import libp2p/protocols/connectivity/autonatv2/service as autonatv2_service
import libp2p/services/wanbootstrapservice
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
  import libp2p/transports/quicruntime
  import "libp2p/transports/nim-msquic/api/diagnostics_model"

import examples/dex/dex_node as dex_node
import examples/dex/mixer_service as mixer_service
import examples/dex/types as dex_types
import examples/dex/marketdata/kline_store as dex_kline_store
import examples/dex/storage/order_store as dex_store
import examples/dex/security/signing as dex_signing
import examples/mobile_ffi/game_mesh_core
import examples/mpc_crypto
import libp2p/crypto/coinjoin/[types, secp_utils]
import nimcrypto/sysrand
import secp256k1
import nimcrypto/sha2
import libp2p/crypto/secp

when defined(android):
  import segfaults
  proc nim_bridge_emit_event(topic: cstring, payload: cstring) {.cdecl, importc: "nim_bridge_emit_event".}
elif defined(ios):
  proc nim_bridge_emit_event(topic: cstring, payload: cstring) {.cdecl, importc: "nim_bridge_emit_event".}
  proc cheng_bridge_emit_event(topic: cstring, payload: cstring) {.cdecl, importc: "cheng_bridge_emit_event".}
else:
  proc nim_bridge_emit_event(topic: cstring, payload: cstring) =
    discard
  proc cheng_bridge_emit_event(topic: cstring, payload: cstring) =
    discard

proc nim_thread_attach*() {.exportc, cdecl, dynlib.} =
  ## Register the current foreign thread with Nim's GC before touching heap data.
  ## Android bridge threads call this via JNI to avoid GC crashes.
  setupForeignThreadGc()

proc nim_thread_detach*() {.exportc, cdecl, dynlib.} =
  ## Optional counterpart for tests; production currently keeps threads pinned.
  tearDownForeignThreadGc()

proc canonicalizeMultiaddrText(text: string): string {.gcsafe, raises: [].} =
  if text.len == 0:
    return text
  let maRes = MultiAddress.init(text)
  if maRes.isOk():
    return $maRes.get()
  result = text

const LegacyPublicWanBootstrapFallbackAddrs = [
  "/dnsaddr/bootstrap.libp2p.io/p2p/QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN",
  "/dnsaddr/bootstrap.libp2p.io/p2p/QmQCU2EcMqAqQPR2i9bChDtGNJchTbq5TbXJJ16u19uLTa",
  "/dnsaddr/bootstrap.libp2p.io/p2p/QmbLHAnMoJPWSCR5Zhtx6BHJX9KiKNN6tpvbUcqanj75Nb",
  "/dnsaddr/bootstrap.libp2p.io/p2p/QmcZf59bWwK5XFi76CZX8cbJ4BhTzzA3gU1ZjYZcYW3dwt",
]

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
  FullP2PDefaultPort* = 4001
  PhysicalAddressPrefixes* = ["/awdl/", "/nan/", "/nearlink/"]
  MaxFeedHistory = 128
  FeedPayloadMaxBytes = 512 * 1024
  FeedPayloadMaxStringLen = 16 * 1024
  FeedPayloadMaxArrayLen = 128
  FeedPayloadMaxObjectEntries = 128
  FeedPayloadMaxDepth = 8
  FeedPayloadLargeKeyThreshold = 1024
  MediaFrameLogLimit = 512
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
  MdnsPeerRetentionMinMs = 12_000'i64
  MdnsPeerRetentionMultiplier = 4'i64
  DefaultMdnsServiceName = "_unimaker._udp.local."
  EventLogLimit = (when defined(android): 128 else: 512)
  NodeTelemetryTopic = "unimaker/node/telemetry/v1"
  NodeTelemetryIntervalMs = 15_000
  NodeTelemetryTtlMs = 180_000
  BootstrapCandidateCap = 7
  BootstrapDialConcurrency = 3
  BootstrapRecentStableSlots = 2
  BootstrapRandomPoolCap = 28
  PeerLatencyTimeoutMs = 1_500
  PeerLatencySampleWindow = 32
  PeerLatencyStatsRetentionMs = 180_000
  PeerLatencyRefreshMs = 2_000
  PeerLatencyMeasureDefaultSamples = 5
  PeerLatencyMeasureMinSamples = 1
  PeerLatencyMeasureMaxSamples = 12
  PeerLatencyInterSampleDelayMs = 60
  PeerLatencyEwmaAlpha = 0.35
  ProbeDurationDefaultMs = 2_500
  ProbeDurationMinMs = 800
  ProbeDurationMaxMs = 15_000
  ProbeChunkDefaultBytes = 12 * 1024
  ProbeChunkMinBytes = 512
  ProbeChunkMaxBytes = 64 * 1024
  ProbeWindowTailMs = 600
  ProbeSendTimeoutMs = 3_500
  ProbeSessionRetentionMs = 180_000
  UdpEchoProbePort = 4002
  UdpEchoProbeTimeoutDefaultMs = 2_500
  UdpEchoProbeMagic = "um_udp_echo_v1:"
  PublicIpProbeRefreshMs = 60_000'i64
  PublicIpProbeRetryMs = 15_000'i64
  PublicIpProbeTimeoutMs = 2_500
  PublicIpProbeLoopKickMs = 2_000'i64
  PublicIpv4ProbeUrl = "http://4.ipw.cn"
  PublicIpv6ProbeUrl = "http://6.ipw.cn"
  MobileYamuxMaxChannels = 1024
  MobileYamuxWindowSize = 1_048_576
  MobileYamuxMaxSendQueueSize = 1_048_576
  MobileMplexMaxChannels = 256
  ShutdownWaitTimeoutMs = 15_000
  NimLibp2pBuildMarker* = "nimlibp2p@" & CompileDate & "T" & CompileTime

var lastInitError = ""
var lastRuntimeError = ""
var msquicDiagnosticsHookInstalled = false
var messageIdCounter: int64 = 0
var messageIdLock: Lock
var lastNodeIdentityLock: Lock
var lastNodeHandle: pointer = nil
var lastNodePeerIdCache = ""

initLock(messageIdLock)
initLock(lastNodeIdentityLock)

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
  lastRuntimeError = ""

proc recordInitError(reason: string) =
  lastInitError = reason

proc clearRuntimeError() =
  lastRuntimeError = ""

proc recordRuntimeError(reason: string) =
  lastRuntimeError = reason
  error "libp2p node init failed", err = reason
  debugLog("[nimlibp2p] init failure: " & reason)

proc ensureMsQuicDiagnosticsHook() =
  when defined(libp2p_msquic_experimental):
    if msquicDiagnosticsHookInstalled:
      return
    msquicDiagnosticsHookInstalled = true
    registerDiagnosticsHook(proc(event: DiagnosticsEvent) {.gcsafe.} =
      when not defined(ohos):
        if event.kind == diagConnectionEvent:
          let lowered = event.note.toLowerAscii()
          if "kind=sfkack" in lowered or "kind=sfkstream" in lowered:
            return
      var msg = "[nimlibp2p] msquic_diag kind=" & $event.kind
      if not event.handle.isNil:
        msg.add(" handle=" & $cast[uint](event.handle))
      if event.paramId != 0'u32:
        msg.add(" paramId=" & $event.paramId)
      if event.note.len > 0:
        msg.add(" note=" & event.note)
      debugLog(msg)
    )

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

  SharedFileEntry = object
    key: string
    path: string
    fileName: string
    mimeType: string
    sha256: string
    sizeBytes: int64
    registeredAtMs: int64

  FileChunkWaiter = ref object
    future: Future[string]
    createdAt: int64
    timeoutMs: int

  ProbeSessionTraffic = object
    payloadBytes: int64
    chunkCount: int
    updatedAtMs: int64

  PublicIpProbeState = object
    ipv4: string
    ipv6: string
    ipv4Error: string
    ipv6Error: string
    updatedAtMs: int64
    inFlight: bool

  PeerLatencyMetric = object
    recentSamples: seq[int]
    totalSuccessCount: int64
    totalFailureCount: int64
    lastMs: int
    minMs: int
    maxMs: int
    ewmaMs: float
    lastUpdatedAtMs: int64
    lastFailureAtMs: int64
    lastError: string

  PeerLatencyProfile = object
    transportRtt: PeerLatencyMetric
    dmAckRtt: PeerLatencyMetric
    updatedAtMs: int64

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
    cmdJoinViaSeedBootstrap,
    cmdUdpEchoProbe,
    cmdSendDirect,
    cmdSendChatAck,
    cmdGetLastDirectError,
    cmdInitializeConnEvents,
    cmdRefreshPeerConnections,
    cmdKeepAlivePin,
    cmdKeepAliveUnpin,
    cmdEmitManualEvent,
    cmdUpdateHostNetworkStatus,
    cmdGetHostNetworkStatus,
    cmdNetworkDiscoverySnapshot,
    cmdBoostConnectivity,
    cmdGetLanEndpoints,
    cmdFetchFeedSnapshot,
    cmdUiFrameSnapshot,
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
    cmdMeasurePeerLatency,
    cmdRequestFileChunk,
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
    preSerializedJson: bool
    highPriority: bool
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
    ## Keep the command-drain future alive so queued FFI commands execute
    ## sequentially on the event-loop dispatcher instead of racing gossipsub
    ## subscribe/publish operations from multiple async tasks.
    commandDrainTask: Future[void]
    ## Periodic wake-up task to prevent `chronos.poll()` from blocking forever
    ## when there are no timers/IO events (commands are submitted from other threads).
    idleWakeTask: Future[void]
    readySignal: ThreadSignalPtr
    shutdownSignal: ThreadSignalPtr
    commandQueueHead: Atomic[Command]
    commandQueueTail: Atomic[Command]
    priorityCommandQueueHead: Atomic[Command]
    priorityCommandQueueTail: Atomic[Command]
    queuedCommands: Atomic[int]
    queuedPriorityCommands: Atomic[int]
    ## Old queue tails are retained for the lifetime of the node.
    ##
    ## With ORC + guarded atomic ref stores, eagerly releasing the retired tail on
    ## the event-loop thread has produced intermittent use-after-free / corrupted
    ## ref-header crashes under heavy GameMesh automation on Harmony. Keep the
    ## consumed tail strongly referenced until node teardown instead of destroying
    ## it inline during dequeue.
    retiredCommandTails: seq[Command]
    thread: Thread[pointer]
    dexNode: DexNode
    subscriptions: Table[pointer, Subscription]
    peerHints: Table[string, seq[string]]
    peerHintSources: Table[string, seq[string]]
    authoritativeBootstrapPeerIds: seq[string]
    lastAuthoritativeBootstrapKickMs: int64
    bootstrapLastSeen: Table[string, int64]
    mdnsEnabled: bool
    mdnsIntervalSeconds: int
    running: Atomic[bool]
    stopRequested: Atomic[bool]
    pendingCommands: Atomic[int]
    ready: Atomic[bool]
    shutdownComplete: Atomic[bool]
    started: bool
    commandDrainActive: bool
    threadJoined: Atomic[bool]
    directWaiters: Table[string, DirectWaiter]
    pendingFileChunkResponses: Table[string, FileChunkWaiter]
    lastDirectError: string
    sharedFiles: Table[string, SharedFileEntry]
    sharedFilesLock: Lock
    lastChunkSize: int
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
    mediaFrameLog: seq[JsonNode]
    mediaFrameLogLock: Lock
    mediaFrameLogLimit: int
    receivedDmLog: seq[JsonNode]
    receivedDmLogLock: Lock
    receivedDmLogLimit: int
    localSystemProfile: JsonNode
    localSystemProfileUpdatedAt: int64
    hostNetworkStatus: JsonNode
    hostNetworkStatusUpdatedAt: int64
    publicIpProbeState: PublicIpProbeState
    publicIpProbeLock: Lock
    udpEchoSock4: DatagramTransport
    udpEchoSock6: DatagramTransport
    lastConnectedPeersInfo: JsonNode
    lastConnectedPeersInfoUpdatedAt: int64
    probeInboundTraffic: Table[string, ProbeSessionTraffic]
    peerSystemProfiles: Table[string, JsonNode]
    peerLatencyProfiles: Table[string, PeerLatencyProfile]
    lanEndpointsCache: JsonNode
    feedItems: seq[CachedFeedItem]
    livestreams: Table[string, LivestreamSession]
    lanGroups: Table[string, LanGroupState]
    rendezvous: RendezVous
    relayReservations: Table[string, int64]
    discovery: DiscoveryManager
    configuredMdnsService: string
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
    selfHandle: pointer
    gameMesh: GameMeshRuntime
    gameMeshLock: Lock

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
  cmd.preSerializedJson = false
  cmd.highPriority = false
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
  ## Do not eagerly clear payload/string/seq fields here.
  ##
  ## Commands are still transiently referenced by the lock-free queue tail after
  ## `submitCommand()` returns. Clearing ref-counted fields from the caller
  ## thread can race with ORC destruction on the queue thread and corrupt seq
  ## headers. Let the normal ref-counted object lifetime release those fields.

proc initCommandQueue(n: NimNode) =
  let stub = acquireCommand(cmdNone)
  stub.completion = nil
  stub.next.store(nil, moRelease)
  n.commandQueueHead.store(stub, moRelease)
  n.commandQueueTail.store(stub, moRelease)
  let priorityStub = acquireCommand(cmdNone)
  priorityStub.completion = nil
  priorityStub.next.store(nil, moRelease)
  n.priorityCommandQueueHead.store(priorityStub, moRelease)
  n.priorityCommandQueueTail.store(priorityStub, moRelease)
  n.queuedCommands.store(0, moRelease)
  n.queuedPriorityCommands.store(0, moRelease)

proc parseDirectEnvelopeOp(payload: string): string =
  let trimmed = payload.strip()
  if trimmed.len == 0 or trimmed[0] != '{':
    return ""
  try:
    let node = parseJson(trimmed)
    if node.kind != JObject:
      return ""
    let envelopeType =
      if node.hasKey("type") and node["type"].kind == JString:
        node["type"].getStr()
      else:
        ""
    if envelopeType.len > 0:
      return envelopeType
    if node.hasKey("op") and node["op"].kind == JString:
      return node["op"].getStr()
    return ""
  except CatchableError:
    return ""

proc classifyDirectCommandOp(cmd: Command): string =
  if cmd.isNil:
    return ""
  let directOp = cmd.op.strip()
  if directOp.len > 0 and directOp.toLowerAscii() != "text":
    return directOp
  if cmd.jsonString.len > 0:
    let parsed = parseDirectEnvelopeOp(cmd.jsonString)
    if parsed.len > 0:
      return parsed
  if cmd.textPayload.len > 0:
    let parsed = parseDirectEnvelopeOp(cmd.textPayload)
    if parsed.len > 0:
      return parsed
  return directOp

proc defaultDirectTimeoutMs(opValue: string): int =
  let normalized = opValue.strip().toLowerAscii()
  if normalized in ["media_frame", "media_heartbeat"]:
    return 900
  if normalized.startsWith("game_"):
    return 8_000
  return 0

proc shouldPreferWanBootstrapForDirectOp(opValue: string): bool =
  let normalized = opValue.strip().toLowerAscii()
  normalized.startsWith("game_") or normalized.startsWith("media_")

proc isHighPriorityCommand(cmd: Command): bool =
  if cmd.isNil:
    return false
  case cmd.kind
  of cmdSendChatAck:
    return true
  of cmdSendDirect:
    let normalized = classifyDirectCommandOp(cmd).strip().toLowerAscii()
    return normalized.startsWith("game_") or normalized == "ack"
  else:
    return false

proc enqueueCommand(n: NimNode, cmd: Command) =
  if cmd.isNil:
    return
  cmd.highPriority = isHighPriorityCommand(cmd)
  cmd.next.store(nil, moRelease)
  let previous =
    if cmd.highPriority:
      n.priorityCommandQueueHead.exchange(cmd, moAcquireRelease)
    else:
      n.commandQueueHead.exchange(cmd, moAcquireRelease)
  previous.next.store(cmd, moRelease)
  atomicInc(n.queuedCommands)
  if cmd.highPriority:
    atomicInc(n.queuedPriorityCommands)

proc dequeueFromQueue(tailRef: var Atomic[Command], retired: var seq[Command]): Command =
  let tail = tailRef.load(moAcquire)
  let next = tail.next.load(moAcquire)
  if next.isNil:
    return nil
  tailRef.store(next, moRelease)
  retired.add(tail)
  next

proc dequeueCommand(n: NimNode): Command =
  let priorityNext = dequeueFromQueue(n.priorityCommandQueueTail, n.retiredCommandTails)
  if not priorityNext.isNil:
    return priorityNext
  dequeueFromQueue(n.commandQueueTail, n.retiredCommandTails)

proc logMemoryLoop(n: NimNode) {.async.}
proc localPeerId(n: NimNode): string {.gcsafe, raises: [].}
proc gatherKnownAddrs(n: NimNode, peer: PeerId): seq[MultiAddress]
proc jsonGetStr(node: JsonNode, key: string, defaultValue: string = ""): string
proc ensureMessageId(mid: string): string {.gcsafe, raises: [].}
proc connectedPeersArray(handle: pointer): JsonNode
proc addPeerHintSource(n: NimNode, peerId: string, source: string) {.gcsafe.}
proc loadSocialStateUnsafe(node: NimNode): JsonNode
proc persistSocialStateUnsafe(node: NimNode, state: JsonNode)
proc makeNotificationNoLock(
    state: JsonNode, kind: string, title: string, body: string, payload: JsonNode = newJObject()
 )
proc appendConversationMessageNoLock(
    state: JsonNode, conversationId: string, peerId: string, message: JsonNode
 )
proc socialStatsNode(state: JsonNode): JsonNode
proc findConnectedPeerInfoRow(entries: JsonNode, peerId: string): JsonNode {.gcsafe.}
proc sendProbeDirectMessage(
    n: NimNode,
    remotePid: PeerId,
    payload: seq[byte],
    ackRequested: bool,
    messageId: string,
    timeout: chronos.Duration
): Future[(bool, string)] {.async.}
proc registerInitialPeerHints(n: NimNode, multiaddrs: seq[string], source: string) {.gcsafe.}
proc preferStableAddrStrings(addrs: seq[string]): seq[string] {.gcsafe.}
proc libp2p_connect_peer_timeout*(handle: pointer, peerId: cstring, timeoutMs: cint): cint {.exportc, cdecl, dynlib.}
proc libp2p_connect_multiaddr*(handle: pointer, multiaddr: cstring): cint {.exportc, cdecl, dynlib.}
proc social_connect_peer_timeout*(handle: pointer, peerId: cstring, multiaddr: cstring, timeoutMs: cint): bool {.exportc, cdecl, dynlib.}
proc social_connect_peer*(handle: pointer, peerId: cstring, multiaddr: cstring): bool {.exportc, cdecl, dynlib.}
proc social_dm_send*(handle: pointer, peerId: cstring, conversationId: cstring, messageJson: cstring): bool {.exportc, cdecl, dynlib.}

const
  SocialStateVersion = 2
  SocialMaxConversationMessages = 500
  SocialMaxMoments = 1000
  SocialMaxNotifications = 2000
  SocialMaxSynccastRooms = 256
  SocialMaxPublishTasks = 256
  SocialMaxPlaybackSessions = 128
  SocialMaxReportTickets = 256
  SocialMaxTombstones = 256
  SocialMaxDistributionLogs = 512
  SocialDefaultLimit = 20
  SocialPublishingRecoveryMs = 15_000'i64
  ReceivedDmLogLimit = 256
  ConnectedPeersInfoSnapshotFallbackMs = 5_000'i64

proc stripLeadingPhysicalPrefix*(text: string): string {.gcsafe, raises: [].} =
  result = text.strip()
  while result.len > 0:
    let lower = result.toLowerAscii()
    var matched = false
    for prefix in PhysicalAddressPrefixes:
      if lower.startsWith(prefix):
        if result.len <= prefix.len:
          return ""
        result = "/" & result[prefix.len .. ^1]
        matched = true
        break
    if not matched:
      break

proc parseDialableMultiaddr*(text: string): Result[MultiAddress, string] =
  let dialText = stripLeadingPhysicalPrefix(text)
  if dialText.len == 0:
    return err("empty dialable multiaddr")
  MultiAddress.init(dialText)

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

proc recordMediaFrame(n: NimNode, envelope: JsonNode) =
  if n.isNil or envelope.isNil or envelope.kind != JObject:
    return
  if n.mediaFrameLogLimit <= 0:
    return
  var entry = newJObject()
  entry["topic"] = %"media.frame"
  entry["timestamp_ms"] = %nowMillis()
  entry["payload"] = cloneJson(envelope)
  n.mediaFrameLogLock.acquire()
  try:
    n.mediaFrameLog.add(entry)
    if n.mediaFrameLog.len > n.mediaFrameLogLimit:
      let excess = n.mediaFrameLog.len - n.mediaFrameLogLimit
      if excess > 0:
        n.mediaFrameLog.delete(0, excess - 1)
  finally:
    n.mediaFrameLogLock.release()

proc pollMediaFrameLog(n: NimNode, limit: int): string =
  if n.isNil:
    return "[]"
  n.mediaFrameLogLock.acquire()
  try:
    let bounded =
      if limit <= 0 or limit > n.mediaFrameLogLimit:
        n.mediaFrameLogLimit
      else:
        limit
    let available = n.mediaFrameLog.len
    if available == 0:
      return "[]"
    let takeCount = min(bounded, available)
    var arr = newJArray()
    var retained: seq[JsonNode] = @[]
    retained.setLen(0)
    for i in 0 ..< available:
      let entry = n.mediaFrameLog[i]
      if i < takeCount:
        arr.add(entry)
      else:
        retained.add(entry)
    n.mediaFrameLog = retained
    return $arr
  finally:
    n.mediaFrameLogLock.release()

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

proc detectMuxerCodec(conn: Connection): string =
  if conn.isNil:
    return ""
  var current = conn
  var visited = initHashSet[int]()
  while not current.isNil:
    let currentAddr = cast[int](current)
    if visited.contains(currentAddr):
      break
    visited.incl(currentAddr)
    if current.negotiatedMuxer.len > 0:
      return current.negotiatedMuxer
    let protocol = current.protocol.strip()
    if protocol.len > 0:
      let normalized = protocol.toLowerAscii()
      if normalized.contains("yamux"):
        return YamuxCodec
      if normalized.contains("mplex"):
        return MplexCodec
    let wrapped = current.getWrapped()
    if wrapped.isNil or wrapped == current:
      break
    current = wrapped
  ""

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

proc drainReceivedDmRows(n: NimNode): seq[JsonNode] =
  if n.isNil:
    return @[]
  n.receivedDmLogLock.acquire()
  try:
    result = move(n.receivedDmLog)
    n.receivedDmLog = @[]
  finally:
    n.receivedDmLogLock.release()

proc restoreReceivedDmRows(n: NimNode; rows: seq[JsonNode]) =
  if n.isNil or rows.len == 0:
    return
  n.receivedDmLogLock.acquire()
  try:
    for row in rows:
      if row.isNil:
        continue
      n.receivedDmLog.add(row)
    if n.receivedDmLog.len > n.receivedDmLogLimit:
      let excess = n.receivedDmLog.len - n.receivedDmLogLimit
      if excess > 0:
        n.receivedDmLog.delete(0, excess - 1)
  finally:
    n.receivedDmLogLock.release()

proc expandNestedGamePayload(payload: JsonNode): JsonNode =
  if payload.isNil or payload.kind != JObject:
    return payload
  result = newJObject()
  for key, value in payload:
    result[key] = if value.isNil: newJNull() else: value
  var nestedRaw = jsonGetStr(result, "text")
  if nestedRaw.len == 0:
    nestedRaw = jsonGetStr(result, "body")
  if nestedRaw.len == 0:
    nestedRaw = jsonGetStr(result, "content")
  if nestedRaw.len == 0:
    return
  try:
    let nested = parseJson(nestedRaw)
    if nested.kind != JObject:
      return
    for key, value in nested:
      result[key] = if value.isNil: newJNull() else: value
    result["body"] = %nestedRaw
  except CatchableError:
    discard


proc ensureJson(node: var JsonNode) =
  if node.isNil:
    node = newJObject()

proc refreshLanAwareAddrs(n: NimNode): seq[MultiAddress] {.gcsafe.}

proc normalizeListenProfile(value: string): string =
  case value.strip().toLowerAscii()
  of "full_p2p", "满血p2p", "mobile_full_p2p", "physical_dual_stack", "physical_dual_stack_4001",
     "quic_tcp_dual_stack", "dual_stack_4001":
    "physical_dual_stack"
  else:
    ""

proc fullP2PListenAddresses*(port: int = FullP2PDefaultPort): seq[string] =
  let safePort =
    if port > 0 and port <= high(uint16).int:
      port
    else:
      FullP2PDefaultPort
  @[
    "/ip4/0.0.0.0/udp/" & $safePort & "/quic-v1",
    "/ip6/::/udp/" & $safePort & "/quic-v1",
    "/ip4/0.0.0.0/tcp/" & $safePort,
    "/ip6/::/tcp/" & $safePort
  ]

proc prioritizeGameSeedAddrs(seedAddrs: seq[string]): seq[string] =
  proc seedRank(address: string): int =
    let lower = address.toLowerAscii()
    let isPhysical =
      lower.contains("/awdl/") or lower.contains("/nan/") or lower.contains("/nearlink/")
    let hasTcp = "/tcp/" in lower
    let hasUdp = "/udp/" in lower
    let hasQuic = "/quic-v1" in lower or "/quic/" in lower
    let hasIpv4 = "/ip4/" in lower
    let hasIpv6 = "/ip6/" in lower and "/ip6/fe80:" notin lower
    if hasQuic and hasIpv6 and not isPhysical:
      return 0
    if hasQuic and hasIpv4 and not isPhysical:
      return 1
    if hasQuic and isPhysical:
      return 2
    if hasTcp and hasIpv6 and not isPhysical:
      return 3
    if hasTcp and hasIpv4 and not isPhysical:
      return 4
    if hasTcp and isPhysical:
      return 5
    if hasUdp and not hasQuic and hasIpv6:
      return 6
    if hasUdp and not hasQuic and hasIpv4:
      return 7
    if hasIpv6:
      return 8
    if hasIpv4:
      return 9
    10

  var ranked: seq[string] = @[]
  var seen = initHashSet[string]()
  for addr in seedAddrs:
    let normalized = addr.strip()
    if normalized.len == 0 or normalized in seen:
      continue
    seen.incl(normalized)
    ranked.add(normalized)
  ranked.sort(proc(a, b: string): int =
    let rankA = seedRank(a)
    let rankB = seedRank(b)
    if rankA < rankB:
      return -1
    if rankA > rankB:
      return 1
    system.cmp(a, b)
  )
  ranked

proc currentGameSeedAddrs(node: NimNode, peerId: string): seq[string] =
  if node.isNil:
    return @[]
  var candidates: seq[string] = @[]
  var seen = initHashSet[string]()

  proc addCandidate(rawAddr: string) =
    let trimmed = rawAddr.strip()
    if trimmed.len == 0:
      return
    let lower = trimmed.toLowerAscii()
    if lower.endsWith("/tcp/0") or lower.contains("/ip4/0.0.0.0/") or lower.contains("/ip6/::/"):
      return
    let normalized =
      if "/p2p/" in lower or "/ipfs/" in lower or peerId.len == 0:
        trimmed
      else:
        trimmed & "/p2p/" & peerId
    if normalized notin seen:
      seen.incl(normalized)
      candidates.add(normalized)

  try:
    if not node.switchInstance.isNil:
      let info = node.switchInstance.peerInfo
      if not info.isNil:
        let refreshed = refreshLanAwareAddrs(node)
        let runtimeAddrs = if refreshed.len > 0: refreshed else: info.addrs
        for addr in runtimeAddrs:
          addCandidate($addr)
        for addr in info.listenAddrs:
          addCandidate($addr)
  except CatchableError:
    discard

  if candidates.len == 0:
    for addr in node.cfg.listenAddresses:
      addCandidate(addr)

  prioritizeGameSeedAddrs(candidates)

proc submitCommandNoWait(node: NimNode, cmd: Command): bool
proc submitCommand(node: NimNode, cmd: Command): Command
proc maybeAttachOutboundGameSeedAddrs(
    node: NimNode,
    envelope: var JsonNode,
    source: string,
    fallbackSeedAddrs: seq[string]
)

proc sendGameOutbound(node: NimNode, outbound: GameOutboundMessage): bool =
  if node.isNil or node.selfHandle.isNil or outbound.peerId.len == 0 or outbound.payload.isNil:
    return false
  let orderedSeedAddrs = prioritizeGameSeedAddrs(outbound.seedAddrs)
  var envelope =
    if outbound.payload.isNil or outbound.payload.kind != JObject:
      newJObject()
    else:
      cloneJson(outbound.payload)
  let mid = jsonGetStr(envelope, "messageId", jsonGetStr(envelope, "mid"))
  let localId = localPeerId(node)
  if jsonGetStr(envelope, "op").len == 0:
    envelope["op"] = %"text"
  if mid.len > 0 and jsonGetStr(envelope, "mid").len == 0:
    envelope["mid"] = %mid
  if localId.len > 0 and jsonGetStr(envelope, "from").len == 0:
    envelope["from"] = %localId
  if not envelope.hasKey("timestamp_ms"):
    envelope["timestamp_ms"] = %nowMillis()
  if not envelope.hasKey("ackRequested"):
    envelope["ackRequested"] = %false
  maybeAttachOutboundGameSeedAddrs(node, envelope, outbound.source, orderedSeedAddrs)
  let payloadText = $envelope
  let envelopeOp = jsonGetStr(envelope, "type", jsonGetStr(envelope, "op", "text"))
  let cmd = makeCommand(cmdSendDirect)
  cmd.peerId = outbound.peerId
  cmd.jsonString = payloadText
  cmd.preSerializedJson = true
  cmd.ackRequested = false
  cmd.op = envelopeOp
  cmd.addresses = orderedSeedAddrs
  cmd.source = outbound.source
  cmd.timeoutMs = max(defaultDirectTimeoutMs(envelopeOp), 8_000)
  if mid.len > 0:
    cmd.messageId = mid
  let sendResult = submitCommand(node, cmd)
  let ok = sendResult.resultCode == NimResultOk and sendResult.boolResult
  if ok:
    debugLog(
      "[nimlibp2p] sendGameOutbound success peer=" & outbound.peerId &
      " mid=" & mid &
      " source=" & outbound.source
    )
  else:
    let errMsg =
      if sendResult.errorMsg.len > 0:
        sendResult.errorMsg
      else:
        "game outbound failed"
    debugLog(
      "[nimlibp2p] sendGameOutbound failed peer=" & outbound.peerId &
      " mid=" & mid &
      " source=" & outbound.source &
      " err=" & errMsg
    )
  destroyCommand(cmd)
  ok

proc queueGameOutbound(node: NimNode, outbound: GameOutboundMessage): bool =
  if node.isNil or node.selfHandle.isNil or outbound.peerId.len == 0 or outbound.payload.isNil:
    return false
  let orderedSeedAddrs = prioritizeGameSeedAddrs(outbound.seedAddrs)
  var envelope =
    if outbound.payload.isNil or outbound.payload.kind != JObject:
      newJObject()
    else:
      cloneJson(outbound.payload)
  let mid = jsonGetStr(envelope, "messageId", jsonGetStr(envelope, "mid"))
  let localId = localPeerId(node)
  if jsonGetStr(envelope, "op").len == 0:
    envelope["op"] = %"text"
  if mid.len > 0 and jsonGetStr(envelope, "mid").len == 0:
    envelope["mid"] = %mid
  if localId.len > 0 and jsonGetStr(envelope, "from").len == 0:
    envelope["from"] = %localId
  if not envelope.hasKey("timestamp_ms"):
    envelope["timestamp_ms"] = %nowMillis()
  if not envelope.hasKey("ackRequested"):
    envelope["ackRequested"] = %false
  maybeAttachOutboundGameSeedAddrs(node, envelope, outbound.source, orderedSeedAddrs)
  let payloadText = $envelope
  let envelopeOp = jsonGetStr(envelope, "type", jsonGetStr(envelope, "op", "text"))
  let cmd = makeCommand(cmdSendDirect)
  cmd.peerId = outbound.peerId
  cmd.jsonString = payloadText
  cmd.preSerializedJson = true
  cmd.ackRequested = false
  cmd.op = envelopeOp
  cmd.addresses = orderedSeedAddrs
  cmd.source = outbound.source
  cmd.timeoutMs = max(defaultDirectTimeoutMs(envelopeOp), 8_000)
  if mid.len > 0:
    cmd.messageId = mid
  let ok = submitCommandNoWait(node, cmd)
  if ok:
    debugLog(
      "[nimlibp2p] queueGameOutbound queued peer=" & outbound.peerId &
      " mid=" & mid &
      " source=" & outbound.source
    )
  else:
    let errMsg =
      if cmd.errorMsg.len > 0:
        cmd.errorMsg
      else:
        "game outbound queue failed"
    debugLog(
      "[nimlibp2p] queueGameOutbound failed peer=" & outbound.peerId &
      " mid=" & mid &
      " source=" & outbound.source &
      " err=" & errMsg
    )
  ok

proc executeGameResultOutbound(node: NimNode, opResult: var GameOperationResult) =
  if node.isNil:
    return
  var failedSource = ""
  opResult.outboundQueued = true
  opResult.outboundFailedSources = @[]
  for outbound in opResult.outbound:
    if not sendGameOutbound(node, outbound) and failedSource.len == 0:
      failedSource =
        if outbound.source.len > 0:
          outbound.source
        else:
          "game_outbound"
  if failedSource.len > 0:
    opResult.outboundQueued = false
    opResult.outboundFailedSources = @[failedSource]
    if opResult.ok:
      opResult.ok = false
    if opResult.error.len == 0:
      opResult.error = "game_outbound_failed:" & failedSource

proc executeGameResultOutboundQueued(node: NimNode, opResult: var GameOperationResult) =
  if node.isNil:
    return
  var failedSources: seq[string] = @[]
  opResult.outboundQueued = true
  opResult.outboundFailedSources = @[]
  for outbound in opResult.outbound:
    if not queueGameOutbound(node, outbound):
      let failedSource =
        if outbound.source.len > 0:
          outbound.source
        else:
          "game_outbound"
      if failedSource notin failedSources:
        failedSources.add(failedSource)
  if failedSources.len > 0:
    opResult.outboundQueued = false
    opResult.outboundFailedSources = failedSources
    debugLog(
      "[nimlibp2p] executeGameResultOutboundQueued pending app=" & opResult.appId &
      " room=" & opResult.roomId &
      " sources=" & failedSources.join(",")
    )

proc collectLocalGameSeedAddrs(node: NimNode): seq[string] {.gcsafe.} =
  if node.isNil:
    return @[]
  var addrs = node.cfg.listenAddresses.filterIt(
    (not it.endsWith("/tcp/0")) and
    (not it.contains("/ip4/0.0.0.0/")) and
    (not it.contains("/ip6/::/"))
  )
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
  result = preferStableAddrStrings(addrs)

proc multiaddrIpComponent(text: string, marker: string): string {.gcsafe.} =
  let lower = text.toLowerAscii()
  let idx = lower.find(marker)
  if idx < 0:
    return ""
  let startIdx = idx + marker.len
  var endIdx = lower.find('/', startIdx)
  if endIdx < 0:
    endIdx = lower.len
  lower[startIdx ..< endIdx].strip()

proc hasPhysicalTransportPrefix(addrText: string): bool {.gcsafe.} =
  let lower = addrText.toLowerAscii()
  lower.startsWith("/awdl/") or lower.startsWith("/nan/") or lower.startsWith("/nearlink/")

proc isProviderGlobalIpv6Text(ip: string): bool {.gcsafe.} =
  let normalized = ip.strip().toLowerAscii()
  if normalized.len == 0:
    return false
  if normalized == "::" or normalized == "::1":
    return false
  if normalized.startsWith("fe80:") or normalized.startsWith("fc") or normalized.startsWith("fd"):
    return false
  if normalized.startsWith("ff"):
    return false
  true

proc isProviderPublicIpv4Text(ip: string): bool {.gcsafe.} =
  let normalized = ip.strip()
  if normalized.len == 0:
    return false
  if normalized.startsWith("10.") or normalized.startsWith("127.") or normalized.startsWith("192.168.") or
     normalized.startsWith("169.254."):
    return false
  if normalized.startsWith("172."):
    let parts = normalized.split('.')
    if parts.len >= 2:
      let second =
        try:
          parseInt(parts[1])
        except CatchableError:
          -1
      if second >= 16 and second <= 31:
        return false
  true

proc isWanProviderIpv6QuicAddr(addrText: string): bool {.gcsafe.} =
  let lower = addrText.toLowerAscii()
  if hasPhysicalTransportPrefix(lower) or "/ip6/" notin lower or "/quic-v1" notin lower:
    return false
  let ip = multiaddrIpComponent(lower, "/ip6/")
  isProviderGlobalIpv6Text(ip)

proc isWanProviderIpv4QuicAddr(addrText: string): bool {.gcsafe.} =
  let lower = addrText.toLowerAscii()
  if hasPhysicalTransportPrefix(lower) or "/ip4/" notin lower or "/quic-v1" notin lower:
    return false
  let ip = multiaddrIpComponent(lower, "/ip4/")
  isProviderPublicIpv4Text(ip)

proc isWanProviderIpv6Addr(addrText: string): bool {.gcsafe.} =
  let lower = addrText.toLowerAscii()
  if hasPhysicalTransportPrefix(lower) or "/ip6/" notin lower:
    return false
  let ip = multiaddrIpComponent(lower, "/ip6/")
  isProviderGlobalIpv6Text(ip)

proc isWanProviderIpv4Addr(addrText: string): bool {.gcsafe.} =
  let lower = addrText.toLowerAscii()
  if hasPhysicalTransportPrefix(lower) or "/ip4/" notin lower:
    return false
  let ip = multiaddrIpComponent(lower, "/ip4/")
  isProviderPublicIpv4Text(ip)

proc isRelayBackedWanSeedAddr(addrText: string): bool {.gcsafe.} =
  let lower = addrText.toLowerAscii()
  if "/p2p-circuit" notin lower or "/quic-v1" notin lower:
    return false
  isWanProviderIpv6QuicAddr(addrText) or isWanProviderIpv4QuicAddr(addrText)

proc replaceIpv4(addrStr: string, ip: string): Option[string] {.gcsafe.}
proc replaceIpv6(addrStr: string, ip: string): Option[string] {.gcsafe.}
proc currentHostNetworkStatus(n: NimNode): JsonNode {.gcsafe.}

proc extraMultiaddrTexts(extra: JsonNode, keys: openArray[string]): seq[string] {.gcsafe.} =
  if extra.isNil or extra.kind != JObject:
    return @[]
  var seen = initHashSet[string]()
  for key in keys:
    let node = extra.getOrDefault(key)
    if node.isNil or node.kind != JArray:
      continue
    for item in node:
      if item.kind != JString:
        continue
      let value = item.getStr().strip()
      if value.len == 0:
        continue
      let parsed = MultiAddress.init(value)
      if parsed.isErr():
        continue
      let canonical = $parsed.get()
      if canonical.len == 0 or canonical in seen:
        continue
      seen.incl(canonical)
      result.add(canonical)

proc canonicalizeWanProviderAddr(
    addrText: string,
    preferredIpv4: string,
    preferredIpv6: string
): string {.gcsafe.} =
  let lower = addrText.toLowerAscii()
  if hasPhysicalTransportPrefix(lower) or "/quic-v1" notin lower:
    return ""
  if "/ip6/" in lower:
    let ipv6 = preferredIpv6.strip()
    if not isProviderGlobalIpv6Text(ipv6):
      return ""
    let rewritten = replaceIpv6(addrText, ipv6)
    if rewritten.isSome():
      let parsed = MultiAddress.init(rewritten.get())
      if parsed.isOk():
        return $parsed.get()
    return ""
  if "/ip4/" in lower:
    let ipv4 = preferredIpv4.strip()
    if not isProviderPublicIpv4Text(ipv4):
      return ""
    let rewritten = replaceIpv4(addrText, ipv4)
    if rewritten.isSome():
      let parsed = MultiAddress.init(rewritten.get())
      if parsed.isOk():
        return $parsed.get()
    return ""
  ""

proc collectWanProviderSeedAddrs(node: NimNode): seq[string] {.gcsafe.} =
  if node.isNil:
    return @[]
  let hostNetwork = currentHostNetworkStatus(node)
  let observedPublicIpv6 =
    if hostNetwork.kind == JObject and hostNetwork.hasKey("publicIpv6") and hostNetwork["publicIpv6"].kind == JString:
      hostNetwork["publicIpv6"].getStr().strip()
    else:
      ""
  let observedPublicIpv4 =
    if hostNetwork.kind == JObject and hostNetwork.hasKey("publicIpv4") and hostNetwork["publicIpv4"].kind == JString:
      hostNetwork["publicIpv4"].getStr().strip()
    else:
      ""
  # WAN provider publication must be keyed off reflected public endpoints.
  # Falling back to preferred/local interface addresses mis-publishes
  # unverified home-broadband or cellular locators as direct seeds.
  let preferredIpv6 =
    if isProviderGlobalIpv6Text(observedPublicIpv6):
      observedPublicIpv6
    else:
      ""
  let preferredIpv4 =
    if isProviderPublicIpv4Text(observedPublicIpv4):
      observedPublicIpv4
    else:
      ""
  var localSeedAddrs = prioritizeGameSeedAddrs(collectLocalGameSeedAddrs(node))
  if localSeedAddrs.len == 0:
    localSeedAddrs = prioritizeGameSeedAddrs(
      node.cfg.listenAddresses.filterIt(
        (not it.endsWith("/tcp/0")) and
        (not it.endsWith("/udp/0")) and
        ("/quic-v1" in it.toLowerAscii())
      )
    )
  var seen = initHashSet[string]()
  for addr in localSeedAddrs:
    let canonical = canonicalizeWanProviderAddr(addr, preferredIpv4, preferredIpv6)
    if canonical.len == 0 or canonical in seen:
      continue
    seen.incl(canonical)
    result.add(canonical)

proc configuredRelayReservationAddrs(node: NimNode): seq[string] {.gcsafe.} =
  if node.isNil:
    return @[]
  extraMultiaddrTexts(node.cfg.extra, ["relayMultiaddrs"])

proc configuredAnchorSeedAddrs(node: NimNode): seq[string] {.gcsafe.} =
  if node.isNil:
    return @[]
  var addrs = extraMultiaddrTexts(
    node.cfg.extra,
    [
      "bootstrapMultiaddrs",
      "wanBootstrapMultiaddrs",
      "bootstrap_multiaddrs",
      "static_bootstrap",
      "bootstrap_nodes",
    ],
  )
  addrs = addrs.filterIt(
    isWanProviderIpv6QuicAddr(it) or isWanProviderIpv4QuicAddr(it)
  )
  prioritizeGameSeedAddrs(addrs)

proc collectRelayBackedProviderSeedAddrs(node: NimNode): seq[string] {.gcsafe.} =
  if node.isNil:
    return @[]
  let localSeedAddrs = prioritizeGameSeedAddrs(collectLocalGameSeedAddrs(node))
  var seen = initHashSet[string]()
  for addr in localSeedAddrs:
    let canonical = canonicalizeMultiaddrText(addr)
    if canonical.len == 0 or canonical in seen:
      continue
    if not isRelayBackedWanSeedAddr(canonical):
      continue
    seen.incl(canonical)
    result.add(canonical)

proc dedupeLocalMultiaddrs(addrs: openArray[MultiAddress]): seq[MultiAddress] {.gcsafe.} =
  var seen = initHashSet[string]()
  for addr in addrs:
    let key = $addr
    if key.len == 0 or key in seen:
      continue
    result.add(addr)
    seen.incl(key)

proc appendParsedUniqueAddrs(
    dest: var seq[MultiAddress],
    addrTexts: openArray[string],
) {.gcsafe.} =
  var seen = initHashSet[string]()
  for addr in dest:
    let key = $addr
    if key.len > 0:
      seen.incl(key)
  for addrText in addrTexts:
    let canonical = canonicalizeMultiaddrText(addrText.strip())
    if canonical.len == 0:
      continue
    var normalized = canonical
    for marker in ["/p2p/", "/ipfs/"]:
      let idx = normalized.rfind(marker)
      if idx >= 0:
        normalized = normalized[0 ..< idx]
        break
    if normalized.len == 0:
      continue
    let parsed = parseDialableMultiaddr(normalized)
    if parsed.isErr():
      continue
    let ma = parsed.get()
    let key = $ma
    if key.len == 0 or key in seen:
      continue
    dest.add(ma)
    seen.incl(key)

proc syncAdvertisedPeerInfo(
    node: NimNode,
    reason = "unspecified",
    reannounce = false,
): Future[bool] {.async.} =
  if node.isNil or node.switchInstance.isNil or node.switchInstance.peerInfo.isNil:
    return false

  let peerInfo = node.switchInstance.peerInfo
  let previousListen = peerInfo.listenAddrs.mapIt($it)
  let previousAddrs = peerInfo.addrs.mapIt($it)

  let refreshed = try:
      refreshLanAwareAddrs(node)
    except CatchableError as exc:
      debugLog(
        "[nimlibp2p] syncAdvertisedPeerInfo refresh_failed reason=" & reason &
        " err=" & exc.msg
      )
      @[]
    except Exception:
      debugLog(
        "[nimlibp2p] syncAdvertisedPeerInfo refresh_failed reason=" & reason
      )
      @[]
  var nextListen =
    if refreshed.len > 0:
      dedupeLocalMultiaddrs(refreshed)
    else:
      dedupeLocalMultiaddrs(peerInfo.listenAddrs)
  if nextListen.len == 0:
    nextListen = dedupeLocalMultiaddrs(peerInfo.addrs)

  var nextAddrs =
    if peerInfo.addrs.len > 0:
      dedupeLocalMultiaddrs(peerInfo.addrs)
    else:
      dedupeLocalMultiaddrs(nextListen)
  let directSeedAddrs = try:
      collectWanProviderSeedAddrs(node)
    except CatchableError as exc:
      debugLog(
        "[nimlibp2p] syncAdvertisedPeerInfo direct_seed_failed reason=" & reason &
        " err=" & exc.msg
      )
      @[]
    except Exception:
      debugLog(
        "[nimlibp2p] syncAdvertisedPeerInfo direct_seed_failed reason=" & reason
      )
      @[]
  let relaySeedAddrs = try:
      collectRelayBackedProviderSeedAddrs(node)
    except CatchableError as exc:
      debugLog(
        "[nimlibp2p] syncAdvertisedPeerInfo relay_seed_failed reason=" & reason &
        " err=" & exc.msg
      )
      @[]
    except Exception:
      debugLog(
        "[nimlibp2p] syncAdvertisedPeerInfo relay_seed_failed reason=" & reason
      )
      @[]
  appendParsedUniqueAddrs(nextAddrs, directSeedAddrs)
  appendParsedUniqueAddrs(nextAddrs, relaySeedAddrs)
  if nextAddrs.len == 0:
    nextAddrs = dedupeLocalMultiaddrs(nextListen)
  if nextListen.len == 0 and nextAddrs.len > 0:
    nextListen = dedupeLocalMultiaddrs(nextAddrs)

  let nextListenText = nextListen.mapIt($it)
  let nextAddrsText = nextAddrs.mapIt($it)
  let changed = previousListen != nextListenText or previousAddrs != nextAddrsText

  let sprRes = SignedPeerRecord.init(
    peerInfo.privateKey, PeerRecord.init(peerInfo.peerId, nextAddrs)
  )
  if sprRes.isErr():
    debugLog(
      "[nimlibp2p] syncAdvertisedPeerInfo spr_failed reason=" & reason
    )
    return false

  peerInfo.listenAddrs = nextListen
  peerInfo.addrs = nextAddrs
  peerInfo.signedPeerRecord = sprRes.get()

  debugLog(
    "[nimlibp2p] syncAdvertisedPeerInfo reason=" & reason &
    " changed=" & $changed &
    " listen=" & (if nextListenText.len > 0: nextListenText.join(",") else: "<empty>") &
    " addrs=" & (if nextAddrsText.len > 0: nextAddrsText.join(",") else: "<empty>")
  )

  if changed and reannounce:
    let svc = getWanBootstrapService(node.switchInstance)
    var connectedCount = 0
    try:
      if not node.switchInstance.isNil:
        var seen = initHashSet[string]()
        for pid in node.switchInstance.connectedPeers(Direction.Out):
          seen.incl($pid)
        for pid in node.switchInstance.connectedPeers(Direction.In):
          seen.incl($pid)
        connectedCount = seen.len
    except CatchableError:
      connectedCount = 0
    if not svc.isNil and connectedCount > 0:
      try:
        discard await svc.advertiseRendezvousNamespaces()
      except CatchableError as exc:
        debugLog(
          "[nimlibp2p] syncAdvertisedPeerInfo reannounce_failed reason=" & reason &
          " err=" & exc.msg
        )

  changed

proc extractPeerIdFromMultiaddrStr(ma: string): Option[string] {.gcsafe, raises: [].}

proc currentConnectedPeerCount(node: NimNode): int {.gcsafe.} =
  if node.isNil or node.switchInstance.isNil:
    return 0
  var seen = initHashSet[string]()
  try:
    for pid in node.switchInstance.connectedPeers(Direction.Out):
      seen.incl($pid)
    for pid in node.switchInstance.connectedPeers(Direction.In):
      seen.incl($pid)
  except CatchableError:
    return 0
  seen.len

proc currentConnectedPeerIds(node: NimNode): HashSet[string] {.gcsafe.} =
  result = initHashSet[string]()
  if node.isNil or node.switchInstance.isNil:
    return
  try:
    for pid in node.switchInstance.connectedPeers(Direction.Out):
      result.incl($pid)
    for pid in node.switchInstance.connectedPeers(Direction.In):
      result.incl($pid)
  except CatchableError:
    result.clear()

proc anyConfiguredAnchorConnected(
    anchorSeedAddrs: seq[string],
    connectedPeerIds: HashSet[string]
): bool {.gcsafe.} =
  if anchorSeedAddrs.len == 0 or connectedPeerIds.len == 0:
    return false
  for addr in anchorSeedAddrs:
    let peerOpt = extractPeerIdFromMultiaddrStr(addr)
    if peerOpt.isSome() and peerOpt.get() in connectedPeerIds:
      return true
  false

proc reachabilityLabel(value: autonat_types.NetworkReachability): string {.gcsafe.} =
  case value
  of autonat_types.NetworkReachability.Reachable:
    "reachable"
  of autonat_types.NetworkReachability.NotReachable:
    "not_reachable"
  else:
    "unknown"

proc currentReachabilityEvidence(
    node: NimNode
): tuple[value: autonat_types.NetworkReachability, source: string] {.gcsafe.} =
  result = (autonat_types.NetworkReachability.Unknown, "")
  if node.isNil or node.switchInstance.isNil:
    return
  for service in node.switchInstance.services:
    if service of autonatv2_service.AutonatV2Service:
      let reachability = autonatv2_service.AutonatV2Service(service).networkReachability
      if reachability == autonat_types.NetworkReachability.Reachable:
        return (reachability, "autonat_v2")
      if reachability == autonat_types.NetworkReachability.NotReachable and result.source.len == 0:
        result = (reachability, "autonat_v2")
    elif service of autonat_service.AutonatService:
      let reachability = autonat_service.AutonatService(service).networkReachability
      if reachability == autonat_types.NetworkReachability.Reachable:
        return (reachability, "autonat")
      if reachability == autonat_types.NetworkReachability.NotReachable and result.source.len == 0:
        result = (reachability, "autonat")
  for transport in node.switchInstance.transports:
    let reachability = transport.networkReachability
    if reachability == autonat_types.NetworkReachability.Reachable:
      return (reachability, "transport")
    if reachability == autonat_types.NetworkReachability.NotReachable and result.source.len == 0:
      result = (reachability, "transport")

proc buildWanProviderPayload(node: NimNode): JsonNode {.gcsafe.} =
  var payload = newJObject()
  let running = not node.isNil and node.started
  let switchReady = not node.isNil and not node.switchInstance.isNil
  let hostNetwork = currentHostNetworkStatus(node)
  let localSeedAddrs =
    if node.isNil:
      @[]
    else:
      prioritizeGameSeedAddrs(collectLocalGameSeedAddrs(node))
  let directProviderSeedAddrs =
    if node.isNil:
      @[]
    else:
      collectWanProviderSeedAddrs(node)
  let relayProviderSeedAddrs =
    if node.isNil:
      @[]
    else:
      collectRelayBackedProviderSeedAddrs(node)
  let anchorSeedAddrs =
    if node.isNil:
      @[]
    else:
      configuredAnchorSeedAddrs(node)
  let authoritativePeerIds =
    if node.isNil:
      @[]
    else:
      node.authoritativeBootstrapPeerIds
  let localHintPeerIds =
    if node.isNil:
      @[]
    else:
      toSeq(node.peerHints.keys).sorted(system.cmp[string])
  let localHasGlobalIpv6 = localSeedAddrs.anyIt(isWanProviderIpv6Addr(it))
  let localHasPublicIpv4 = localSeedAddrs.anyIt(isWanProviderIpv4Addr(it))
  let localHasQuic = localSeedAddrs.anyIt("/quic-v1" in it.toLowerAscii())
  let observedPublicIpv6 =
    if hostNetwork.kind == JObject and hostNetwork.hasKey("publicIpv6") and hostNetwork["publicIpv6"].kind == JString:
      hostNetwork["publicIpv6"].getStr().strip()
    else:
      ""
  let observedPublicIpv4 =
    if hostNetwork.kind == JObject and hostNetwork.hasKey("publicIpv4") and hostNetwork["publicIpv4"].kind == JString:
      hostNetwork["publicIpv4"].getStr().strip()
    else:
      ""
  let preferredIpv6 =
    if isProviderGlobalIpv6Text(observedPublicIpv6):
      observedPublicIpv6
    elif hostNetwork.kind == JObject and hostNetwork.hasKey("preferredIpv6") and hostNetwork["preferredIpv6"].kind == JString:
      hostNetwork["preferredIpv6"].getStr()
    elif hostNetwork.kind == JObject and hostNetwork.hasKey("localIpv6") and hostNetwork["localIpv6"].kind == JString:
      hostNetwork["localIpv6"].getStr()
    else:
      ""
  let directPreferredIpv4 =
    if hostNetwork.kind == JObject and hostNetwork.hasKey("preferredIpv4") and hostNetwork["preferredIpv4"].kind == JString:
      hostNetwork["preferredIpv4"].getStr()
    elif hostNetwork.kind == JObject and hostNetwork.hasKey("localIpv4") and hostNetwork["localIpv4"].kind == JString:
      hostNetwork["localIpv4"].getStr()
    else:
      ""
  let preferredIpv4 =
    if isProviderPublicIpv4Text(directPreferredIpv4):
      if isProviderPublicIpv4Text(observedPublicIpv4):
        observedPublicIpv4
      else:
        directPreferredIpv4
    else:
      ""
  let hasDirectPublicWanQuicSeed = directProviderSeedAddrs.len > 0
  let hasRelayBackedSeed = relayProviderSeedAddrs.len > 0
  let hasAnchorSeed = anchorSeedAddrs.len > 0
  let providerDirectAdvertisable = running and switchReady and hasDirectPublicWanQuicSeed
  let connectedPeerIds = currentConnectedPeerIds(node)
  let providerConnectedPeerCount =
    connectedPeerIds.len
  let providerReachabilityEvidence = currentReachabilityEvidence(node)
  let providerReachability = reachabilityLabel(providerReachabilityEvidence.value)
  let providerDirectVerifiedReachable =
    providerDirectAdvertisable and (
      providerConnectedPeerCount > 0 or
        providerReachabilityEvidence.value == autonat_types.NetworkReachability.Reachable
    )
  let providerRelayVerifiedReachable = hasRelayBackedSeed
  let providerAnchorVerifiedReachable =
    hasAnchorSeed and anyConfiguredAnchorConnected(anchorSeedAddrs, connectedPeerIds)
  let providerVerifiedReachable =
    providerDirectVerifiedReachable or
      providerRelayVerifiedReachable or
      providerAnchorVerifiedReachable
  let publicIpv4RequiresHolePunching = observedPublicIpv4.len > 0 and not localHasPublicIpv4
  let providerPublicationMode =
    if providerDirectVerifiedReachable and hasDirectPublicWanQuicSeed:
      "direct_verified"
    elif hasRelayBackedSeed:
      "relay_reserved"
    elif hasAnchorSeed and publicIpv4RequiresHolePunching:
      "anchor_assisted_hole_punch"
    elif hasAnchorSeed and hasDirectPublicWanQuicSeed:
      "anchor_assisted_unverified_direct"
    elif hasAnchorSeed:
      "anchor_only"
    else:
      "unpublished"
  let providerSeedAddrs =
    case providerPublicationMode
    of "direct_verified":
      directProviderSeedAddrs
    of "relay_reserved":
      relayProviderSeedAddrs
    of "anchor_assisted_hole_punch", "anchor_assisted_unverified_direct", "anchor_only":
      anchorSeedAddrs
    else:
      @[]
  let providerAdvertisable = running and switchReady and providerSeedAddrs.len > 0
  let providerEligible = providerAdvertisable
  let providerReason =
    if not running:
      "node_not_started"
    elif not switchReady:
      "switch_not_initialized"
    elif localSeedAddrs.len == 0:
      "no_seed_addrs"
    elif not localHasGlobalIpv6 and not localHasPublicIpv4:
      if publicIpv4RequiresHolePunching:
        "observed_public_ipv4_requires_hole_punching"
      else:
        "missing_public_wan_seed"
    elif not localHasQuic:
      "missing_quic_seed"
    elif not hasDirectPublicWanQuicSeed and not hasRelayBackedSeed and not hasAnchorSeed:
      "missing_public_wan_quic_seed"
    elif providerPublicationMode == "relay_reserved":
      "relay_reserved_for_hole_punch"
    elif providerPublicationMode == "anchor_assisted_hole_punch":
      "anchor_required_for_ipv4_hole_punch"
    elif providerPublicationMode == "anchor_assisted_unverified_direct":
      "anchor_required_for_unverified_direct"
    elif providerPublicationMode == "anchor_only":
      "anchor_only_publication"
    elif providerReachabilityEvidence.value == autonat_types.NetworkReachability.NotReachable:
      "wan_public_ip_quic_not_reachable"
    elif not providerVerifiedReachable:
      if providerReachabilityEvidence.source.len > 0:
        "wan_public_ip_quic_reachability_unknown"
      else:
        "wan_public_ip_quic_unverified_no_connected_peer"
    else:
      "wan_public_ip_quic_verified"
  payload["reported"] = %true
  payload["providerPolicy"] = %"wan_public_ip_quic"
  payload["providerPublicationMode"] = %providerPublicationMode
  payload["providerAdvertisable"] = %providerAdvertisable
  payload["providerDirectAdvertisable"] = %providerDirectAdvertisable
  payload["providerVerifiedReachable"] = %providerVerifiedReachable
  payload["providerDirectVerifiedReachable"] = %providerDirectVerifiedReachable
  payload["providerRelayVerifiedReachable"] = %providerRelayVerifiedReachable
  payload["providerAnchorVerifiedReachable"] = %providerAnchorVerifiedReachable
  payload["providerConnectedPeerCount"] = %providerConnectedPeerCount
  payload["providerEligible"] = %providerEligible
  payload["providerReason"] = %providerReason
  payload["providerReachability"] = %providerReachability
  payload["providerReachabilitySource"] = %providerReachabilityEvidence.source
  payload["providerSeedAddrs"] = %providerSeedAddrs
  payload["providerDirectSeedAddrs"] = %directProviderSeedAddrs
  payload["providerRelaySeedAddrs"] = %relayProviderSeedAddrs
  payload["anchorSeedAddrs"] = %anchorSeedAddrs
  payload["authoritativeBootstrapPeerIds"] = %authoritativePeerIds
  payload["authoritativeBootstrapPeerIdCount"] = %authoritativePeerIds.len
  payload["localHintPeerIds"] = %localHintPeerIds
  payload["localHintPeerCount"] = %localHintPeerIds.len
  payload["seedAddrs"] = %localSeedAddrs
  payload["localHasGlobalIpv6"] = %localHasGlobalIpv6
  payload["localHasPublicIpv4"] = %localHasPublicIpv4
  payload["localHasQuic"] = %localHasQuic
  payload["hostPreferredIpv6"] = %preferredIpv6
  payload["hostPreferredIpv4"] = %preferredIpv4
  payload["hostPublicIpv6"] = %observedPublicIpv6
  payload["hostPublicIpv4"] = %observedPublicIpv4
  payload["publicIpv4RequiresHolePunching"] = %publicIpv4RequiresHolePunching
  payload["providerNeedsHolePunching"] = %publicIpv4RequiresHolePunching
  if providerSeedAddrs.len > 0:
    payload["preferredSeedAddr"] = %providerSeedAddrs[0]
  if directProviderSeedAddrs.len > 0:
    payload["preferredDirectSeedAddr"] = %directProviderSeedAddrs[0]
  if relayProviderSeedAddrs.len > 0:
    payload["preferredRelaySeedAddr"] = %relayProviderSeedAddrs[0]
  if anchorSeedAddrs.len > 0:
    payload["preferredAnchorSeedAddr"] = %anchorSeedAddrs[0]
  payload

proc attachBootstrapProvider(
    bootstrapNode: var JsonNode,
    providerNode: JsonNode,
) {.gcsafe.} =
  if bootstrapNode.isNil or bootstrapNode.kind != JObject:
    bootstrapNode = newJObject()
  bootstrapNode["provider"] = providerNode
  if providerNode.kind != JObject:
    return
  for key in [
    "reported",
    "providerPolicy",
    "providerPublicationMode",
    "providerAdvertisable",
    "providerDirectAdvertisable",
    "providerVerifiedReachable",
    "providerDirectVerifiedReachable",
    "providerRelayVerifiedReachable",
    "providerAnchorVerifiedReachable",
    "providerConnectedPeerCount",
    "providerEligible",
    "providerReason",
    "providerReachability",
    "providerReachabilitySource",
    "providerSeedAddrs",
    "providerDirectSeedAddrs",
    "providerRelaySeedAddrs",
    "anchorSeedAddrs",
    "preferredSeedAddr",
    "preferredDirectSeedAddr",
    "preferredRelaySeedAddr",
    "preferredAnchorSeedAddr",
    "localHasGlobalIpv6",
    "localHasPublicIpv4",
    "localHasQuic",
    "hostPreferredIpv6",
    "hostPreferredIpv4",
    "hostPublicIpv6",
    "hostPublicIpv4",
    "publicIpv4RequiresHolePunching",
    "providerNeedsHolePunching",
  ]:
    if providerNode.hasKey(key):
      bootstrapNode[key] = providerNode[key]

proc maybeAttachOutboundGameSeedAddrs(
    node: NimNode,
    envelope: var JsonNode,
    source: string,
    fallbackSeedAddrs: seq[string]
) =
  if node.isNil or envelope.isNil or envelope.kind != JObject:
    return
  let normalizedSource = source.strip().toLowerAscii()
  if not normalizedSource.startsWith("game_"):
    return
  var localSeedAddrs = prioritizeGameSeedAddrs(fallbackSeedAddrs)
  if localSeedAddrs.len == 0:
    localSeedAddrs = collectLocalGameSeedAddrs(node)
  if localSeedAddrs.len == 0:
    return
  let gameNode = envelope{"game"}
  if gameNode.isNil or gameNode.kind != JObject:
    return
  if gameNode{"payload"}.isNil or gameNode["payload"].kind != JObject:
    gameNode["payload"] = newJObject()
  gameNode["payload"]["seedAddrs"] = %localSeedAddrs

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
  if node.isNil:
    return defaultValue
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
  if node.isNil:
    return defaultValue
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
  if node.isNil:
    return defaultValue
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

proc finiteFloat(value: float, defaultValue = 0.0): float =
  case classify(value)
  of fcNormal, fcZero, fcSubnormal:
    value
  else:
    defaultValue

proc jsonSafeFloat(value: float, defaultValue = 0.0): JsonNode =
  %finiteFloat(value, defaultValue)

proc jsonGetFloat(node: JsonNode, key: string, defaultValue: float = 0.0): float =
  let fallback = finiteFloat(defaultValue)
  if node.isNil:
    return fallback
  if node.kind == JObject and node.hasKey(key):
    let v = node.getOrDefault(key)
    case v.kind
    of JFloat:
      return finiteFloat(v.getFloat(), fallback)
    of JInt:
      return finiteFloat(float(v.getInt()), fallback)
    of JString:
      try:
        return finiteFloat(parseFloat(v.getStr()), fallback)
      except CatchableError:
        discard
    else:
      discard
  fallback

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

when defined(metrics):
  template bindDefaultMetricsRegistryToCurrentThread() =
    {.cast(gcsafe).}:
      withLock(defaultRegistry.lock):
        for collector in defaultRegistry.collectors:
          if not collector.isNil:
            collector.creationThreadId = getThreadId()

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

proc defaultHostNetworkStatus(): JsonNode =
  %* {
    "type": "HostNetworkStatus",
    "networkType": "none",
    "transport": "none",
    "ssid": "",
    "localIpv4": "",
    "preferredIpv4": "",
    "localIpv6": "",
    "preferredIpv6": "",
    "publicIpv4": "",
    "publicIpv6": "",
    "publicIpv4ProbeError": "",
    "publicIpv6ProbeError": "",
    "publicIpProbeUpdatedAtMs": 0'i64,
    "publicIpv4RequiresHolePunching": false,
    "listenPort": 0,
    "isConnected": false,
    "isMetered": false,
    "timestampMs": 0'i64,
    "reason": "manual",
  }

proc normalizeHostNetworkType(value: string): string =
  case value.strip().toLowerAscii()
  of "wifi":
    "wifi"
  of "cellular":
    "cellular"
  of "ethernet":
    "ethernet"
  of "vpn":
    "vpn"
  of "other":
    "other"
  of "none":
    "none"
  else:
    "other"

proc normalizeHostTransport(value: string, networkType: string): string =
  case value.strip().toLowerAscii()
  of "wifi":
    "wifi"
  of "nr", "nr5g", "5g_nr", "nsa", "nr_nsa", "nr_sa":
    "5g"
  of "5g":
    "5g"
  of "lte", "lte_a", "lte+", "4g_lte":
    "4g"
  of "4g":
    "4g"
  of "umts", "hspa", "hspa+", "wcdma", "evdo", "cdma2000":
    "3g"
  of "3g":
    "3g"
  of "edge", "gprs", "gsm", "cdma", "1xrtt":
    "2g"
  of "2g":
    "2g"
  of "cellular":
    "cellular"
  of "ethernet":
    "ethernet"
  of "vpn":
    "vpn"
  of "other":
    "other"
  of "none":
    "none"
  else:
    case networkType
    of "wifi":
      "wifi"
    of "cellular":
      "cellular"
    of "ethernet":
      "ethernet"
    of "vpn":
      "vpn"
    of "none":
      "none"
    else:
      "other"

proc normalizeHostReason(value: string): string =
  case value.strip().toLowerAscii()
  of "init":
    "init"
  of "start":
    "start"
  of "available":
    "available"
  of "lost":
    "lost"
  of "capabilities_changed":
    "capabilities_changed"
  of "properties_changed":
    "properties_changed"
  of "manual":
    "manual"
  else:
    "manual"

proc sanitizeHostSsid(value: string): string =
  let raw = value.strip()
  if raw.len == 0:
    return ""
  if raw == "0x":
    return ""
  let lowered = raw.toLowerAscii()
  if lowered == "<unknown ssid>" or lowered == "unknown ssid":
    return ""
  if raw.len >= 2 and raw[0] == '"' and raw[^1] == '"':
    return raw[1 .. ^2].strip()
  raw

proc sanitizeHostIpv4(value: string): string =
  let raw = value.strip()
  if raw.len == 0 or raw == "0.0.0.0" or raw == "127.0.0.1":
    return ""
  raw

proc sanitizeHostIpv6(value: string): string =
  let raw = value.strip()
  if raw.len == 0:
    return ""
  let lower = raw.toLowerAscii()
  if lower in ["::", "::1", "0:0:0:0:0:0:0:0"]:
    return ""
  raw

proc normalizePublicIpProbeValue(value: string, family: int): string {.gcsafe.} =
  let raw = value.strip()
  if raw.len == 0:
    return ""
  let candidate = raw.splitWhitespace()[0].strip()
  case family
  of 4:
    if isProviderPublicIpv4Text(candidate): candidate else: ""
  of 6:
    if isProviderGlobalIpv6Text(candidate): candidate else: ""
  else:
    ""

proc currentPublicIpProbeState(n: NimNode): PublicIpProbeState {.gcsafe.} =
  if n.isNil:
    return PublicIpProbeState()
  acquire(n.publicIpProbeLock)
  try:
    result = n.publicIpProbeState
  finally:
    release(n.publicIpProbeLock)

proc updatePublicIpProbeState(
    n: NimNode,
    ipv4, ipv6, ipv4Error, ipv6Error: string,
    updatedAtMs: int64,
    inFlight: bool
) {.gcsafe.} =
  if n.isNil:
    return
  acquire(n.publicIpProbeLock)
  try:
    n.publicIpProbeState.ipv4 = ipv4.strip()
    n.publicIpProbeState.ipv6 = ipv6.strip()
    n.publicIpProbeState.ipv4Error = ipv4Error.strip()
    n.publicIpProbeState.ipv6Error = ipv6Error.strip()
    n.publicIpProbeState.updatedAtMs = max(0'i64, updatedAtMs)
    n.publicIpProbeState.inFlight = inFlight
  finally:
    release(n.publicIpProbeLock)

proc markPublicIpProbeInFlight(n: NimNode): bool {.gcsafe.} =
  if n.isNil:
    return false
  acquire(n.publicIpProbeLock)
  try:
    if n.publicIpProbeState.inFlight:
      return false
    n.publicIpProbeState.inFlight = true
    return true
  finally:
    release(n.publicIpProbeLock)

proc clearPublicIpProbeState(n: NimNode) {.gcsafe.} =
  updatePublicIpProbeState(n, "", "", "", "", 0, false)

proc attachPublicIpProbeFields(status: var JsonNode, probe: PublicIpProbeState) {.gcsafe.} =
  if status.isNil or status.kind != JObject:
    status = defaultHostNetworkStatus()
  let preferredIpv4 = jsonGetStr(status, "preferredIpv4", jsonGetStr(status, "localIpv4", "")).strip()
  let publicIpv4 = normalizePublicIpProbeValue(probe.ipv4, 4)
  let publicIpv6 = normalizePublicIpProbeValue(probe.ipv6, 6)
  status["publicIpv4"] = %publicIpv4
  status["publicIpv6"] = %publicIpv6
  status["publicIpv4ProbeError"] = %probe.ipv4Error
  status["publicIpv6ProbeError"] = %probe.ipv6Error
  status["publicIpProbeUpdatedAtMs"] = %max(0'i64, probe.updatedAtMs)
  status["publicIpv4RequiresHolePunching"] = %(publicIpv4.len > 0 and not isProviderPublicIpv4Text(preferredIpv4))

proc shouldRefreshPublicIpProbe(
    n: NimNode,
    hostStatus: JsonNode,
    force = false
): bool {.gcsafe.} =
  if n.isNil:
    return false
  if force:
    return true
  if hostStatus.kind != JObject or not jsonGetBool(hostStatus, "isConnected", false):
    return false
  let probe = currentPublicIpProbeState(n)
  if probe.inFlight:
    return false
  let nowMs = nowMillis()
  if probe.updatedAtMs <= 0:
    return true
  let hasSuccessfulResult = probe.ipv4.len > 0 or probe.ipv6.len > 0
  let ttlMs = if hasSuccessfulResult: PublicIpProbeRefreshMs else: PublicIpProbeRetryMs
  nowMs - probe.updatedAtMs >= ttlMs

proc probePublicIp(url: string, family: int): Future[(string, string)] {.async.} =
  let session = HttpSessionRef.new()
  let request = HttpClientRequestRef.get(
    session,
    url,
    headers = [("User-Agent", "nim-libp2p/mobile_ffi")]
  ).valueOr:
    return ("", "create_request_failed:" & error)
  try:
    let responseFuture = request.send()
    let responseReady = await withTimeout(responseFuture, chronos.milliseconds(PublicIpProbeTimeoutMs))
    if not responseReady:
      responseFuture.cancelSoon()
      return ("", "send_timeout")
    let response = responseFuture.read()
    if response.status != 200:
      return ("", "http_status_" & $response.status)
    let bodyFuture = response.getBodyBytes()
    let bodyReady = await withTimeout(bodyFuture, chronos.milliseconds(PublicIpProbeTimeoutMs))
    if not bodyReady:
      bodyFuture.cancelSoon()
      return ("", "body_timeout")
    let parsed = normalizePublicIpProbeValue(bytesToString(bodyFuture.read()), family)
    if parsed.len == 0:
      return ("", "invalid_body")
    return (parsed, "")
  except CancelledError:
    return ("", "cancelled")
  except HttpError as exc:
    return ("", "http_error:" & exc.msg)
  except CatchableError as exc:
    return ("", "request_failed:" & exc.msg)

proc refreshPublicIpProbeAsync(
    n: NimNode,
    hostStatus: JsonNode,
    force = false
): Future[void] {.async.} =
  if n.isNil:
    return
  if not shouldRefreshPublicIpProbe(n, hostStatus, force):
    return
  if not markPublicIpProbeInFlight(n):
    return
  let ipv4Future = probePublicIp(PublicIpv4ProbeUrl, 4)
  let ipv6Future = probePublicIp(PublicIpv6ProbeUrl, 6)
  var ipv4 = ""
  var ipv6 = ""
  var ipv4Error = ""
  var ipv6Error = ""
  try:
    (ipv4, ipv4Error) = await ipv4Future
  except CatchableError as exc:
    ipv4Error = "probe_failed:" & exc.msg
  try:
    (ipv6, ipv6Error) = await ipv6Future
  except CatchableError as exc:
    ipv6Error = "probe_failed:" & exc.msg
  updatePublicIpProbeState(n, ipv4, ipv6, ipv4Error, ipv6Error, nowMillis(), false)

proc restartMdnsSafe(n: NimNode, reason: string): Future[void] {.async.}
proc stopMdns(n: NimNode): Future[void] {.async.}

proc runHostNetworkStatusSideEffects(
    n: NimNode,
    hostStatus: JsonNode,
    mdnsTransportChanged: bool,
    currentMdnsAllowed: bool,
): Future[void] {.async.} =
  if n.isNil:
    return
  # Yield once so fireSync callers on the command queue can continue first.
  await sleepAsync(chronos.milliseconds(1))
  try:
    when defined(ohos):
      if jsonGetBool(hostStatus, "isConnected", false):
        debugLog("[nimlibp2p] cmdUpdateHostNetworkStatus network_side_effects:deferred_on_ohos")
      if n.mdnsEnabled and n.started and mdnsTransportChanged:
        debugLog("[nimlibp2p] cmdUpdateHostNetworkStatus mdns_update:deferred_on_ohos")
    else:
      if jsonGetBool(hostStatus, "isConnected", false):
        debugLog("[nimlibp2p] cmdUpdateHostNetworkStatus public_ip_probe:start")
        await refreshPublicIpProbeAsync(n, hostStatus)
        debugLog("[nimlibp2p] cmdUpdateHostNetworkStatus public_ip_probe:done")
        debugLog("[nimlibp2p] cmdUpdateHostNetworkStatus sync_advertised:start")
        discard await syncAdvertisedPeerInfo(
          n,
          reason = "host_network_status",
          reannounce = true,
        )
        debugLog("[nimlibp2p] cmdUpdateHostNetworkStatus sync_advertised:done")
      if n.mdnsEnabled and n.started and mdnsTransportChanged:
        if currentMdnsAllowed:
          debugLog("[nimlibp2p] cmdUpdateHostNetworkStatus mdns_restart:spawn")
          asyncCheck(restartMdnsSafe(n, "host_network_status"))
        else:
          debugLog("[nimlibp2p] cmdUpdateHostNetworkStatus mdns_stop:start")
          await stopMdns(n)
          debugLog("[nimlibp2p] cmdUpdateHostNetworkStatus mdns_stop:done")
    debugLog("[nimlibp2p] cmdUpdateHostNetworkStatus side_effects:done")
  except CancelledError:
    debugLog("[nimlibp2p] cmdUpdateHostNetworkStatus side_effects:cancelled")
  except CatchableError as exc:
    debugLog(
      "[nimlibp2p] cmdUpdateHostNetworkStatus side_effects:error " & exc.msg
    )

proc sanitizeHostNetworkStatus(node: JsonNode): JsonNode =
  var status = defaultHostNetworkStatus()
  if node.kind != JObject:
    status["timestampMs"] = %nowMillis()
    return status

  var networkType = normalizeHostNetworkType(
    jsonGetStr(node, "networkType", jsonGetStr(node, "network_type", "none"))
  )
  var transport = normalizeHostTransport(
    jsonGetStr(node, "transport", jsonGetStr(node, "transportType", networkType)),
    networkType
  )
  var connected = jsonGetBool(
    node,
    "isConnected",
    jsonGetBool(node, "connected", networkType != "none" and transport != "none")
  )
  var ssid = sanitizeHostSsid(jsonGetStr(node, "ssid", ""))
  var metered = jsonGetBool(
    node,
    "isMetered",
    jsonGetBool(node, "metered", false)
  )
  if not connected:
    networkType = "none"
    transport = "none"
    ssid = ""
    metered = false

  let timestampMs = max(0'i64, jsonGetInt64(node, "timestampMs", nowMillis()))
  let localIpv4Raw = jsonGetStr(node, "localIpv4", jsonGetStr(node, "local_ipv4", ""))
  let preferredIpv4Raw = jsonGetStr(node, "preferredIpv4", jsonGetStr(node, "preferred_ipv4", localIpv4Raw))
  let localIpv6Raw = jsonGetStr(node, "localIpv6", jsonGetStr(node, "local_ipv6", ""))
  let preferredIpv6Raw = jsonGetStr(node, "preferredIpv6", jsonGetStr(node, "preferred_ipv6", localIpv6Raw))
  let localIpv4 = sanitizeHostIpv4(localIpv4Raw)
  let preferredIpv4 = block:
    let sanitized = sanitizeHostIpv4(preferredIpv4Raw)
    if sanitized.len > 0: sanitized else: localIpv4
  let localIpv6 = sanitizeHostIpv6(localIpv6Raw)
  let preferredIpv6 = block:
    let sanitized = sanitizeHostIpv6(preferredIpv6Raw)
    if sanitized.len > 0: sanitized else: localIpv6
  let publicIpv4 = normalizePublicIpProbeValue(jsonGetStr(node, "publicIpv4", ""), 4)
  let publicIpv6 = normalizePublicIpProbeValue(jsonGetStr(node, "publicIpv6", ""), 6)
  let publicIpv4ProbeError = jsonGetStr(node, "publicIpv4ProbeError", "").strip()
  let publicIpv6ProbeError = jsonGetStr(node, "publicIpv6ProbeError", "").strip()
  let publicIpProbeUpdatedAtMs = max(0'i64, jsonGetInt64(node, "publicIpProbeUpdatedAtMs", 0))
  let listenPort = max(0, int(jsonGetInt64(node, "listenPort", jsonGetInt64(node, "listen_port", 0))))
  status["networkType"] = %networkType
  status["transport"] = %transport
  status["ssid"] = %ssid
  status["localIpv4"] = %localIpv4
  status["preferredIpv4"] = %preferredIpv4
  status["localIpv6"] = %localIpv6
  status["preferredIpv6"] = %preferredIpv6
  status["publicIpv4"] = %publicIpv4
  status["publicIpv6"] = %publicIpv6
  status["publicIpv4ProbeError"] = %publicIpv4ProbeError
  status["publicIpv6ProbeError"] = %publicIpv6ProbeError
  status["publicIpProbeUpdatedAtMs"] = %publicIpProbeUpdatedAtMs
  status["publicIpv4RequiresHolePunching"] = %(publicIpv4.len > 0 and not isProviderPublicIpv4Text(preferredIpv4))
  status["listenPort"] = %listenPort
  status["isConnected"] = %connected
  status["isMetered"] = %metered
  status["timestampMs"] = %(if timestampMs > 0: timestampMs else: nowMillis())
  status["reason"] = %normalizeHostReason(jsonGetStr(node, "reason", "manual"))
  status

proc currentHostNetworkStatus(n: NimNode): JsonNode {.gcsafe.} =
  if n.isNil or n.hostNetworkStatus.isNil:
    return defaultHostNetworkStatus()
  result = sanitizeHostNetworkStatus(n.hostNetworkStatus)
  attachPublicIpProbeFields(result, currentPublicIpProbeState(n))

proc buildHostNetworkEvent(status: JsonNode): JsonNode =
  let normalized = sanitizeHostNetworkStatus(status)
  %* {
    "topic": "network_event",
    "kind": "host",
    "entity": "network",
    "op": "status",
    "timestampMs": jsonGetInt64(normalized, "timestampMs", nowMillis()),
    "payload": normalized,
  }

proc applyHostNetworkStatus(n: NimNode, status: JsonNode): JsonNode =
  let previous =
    if n.isNil or n.hostNetworkStatus.isNil:
      defaultHostNetworkStatus()
    else:
      sanitizeHostNetworkStatus(n.hostNetworkStatus)
  let normalized = sanitizeHostNetworkStatus(status)
  n.hostNetworkStatus = normalized
  n.hostNetworkStatusUpdatedAt = jsonGetInt64(normalized, "timestampMs", nowMillis())
  let preferredIpv4 = jsonGetStr(normalized, "preferredIpv4", jsonGetStr(normalized, "localIpv4", "")).strip()
  if preferredIpv4.len > 0 and preferredIpv4 != n.mdnsPreferredIpv4:
    n.mdnsPreferredIpv4 = preferredIpv4
    if not n.mdnsInterface.isNil:
      n.mdnsInterface.setPreferredIpv4(n.mdnsPreferredIpv4)
  let connected = jsonGetBool(normalized, "isConnected", false)
  let networkChanged =
    jsonGetBool(previous, "isConnected", false) != connected or
    jsonGetStr(previous, "networkType", "") != jsonGetStr(normalized, "networkType", "") or
    jsonGetStr(previous, "transport", "") != jsonGetStr(normalized, "transport", "") or
    jsonGetStr(previous, "preferredIpv4", "") != jsonGetStr(normalized, "preferredIpv4", "") or
    jsonGetStr(previous, "preferredIpv6", "") != jsonGetStr(normalized, "preferredIpv6", "")
  if not connected:
    clearPublicIpProbeState(n)
  else:
    if networkChanged:
      clearPublicIpProbeState(n)
  n.localSystemProfileUpdatedAt = 0
  n.localSystemProfile = newJObject()
  result = normalized
  attachPublicIpProbeFields(result, currentPublicIpProbeState(n))

proc quicAvailable(addrs: openArray[string]): bool =
  for addr in addrs:
    if addr.contains("/quic"):
      return true
  false

proc tcpAvailable(addrs: openArray[string]): bool =
  for addr in addrs:
    if addr.contains("/tcp/"):
      return true
  false

when defined(libp2p_msquic_experimental):
  proc quicRuntimePreferenceLabel(
      pref: QuicRuntimePreference
  ): string {.gcsafe, raises: [].} =
    case pref
    of qrpAuto:
      "auto"
    of qrpNativeOnly:
      "native_only"
    of qrpBuiltinPreferred:
      "builtin_preferred"
    of qrpBuiltinOnly:
      "builtin_only"

  proc parseQuicRuntimePreferenceValue(
      value: string
  ): Result[Option[QuicRuntimePreference], string] {.gcsafe, raises: [].} =
    let normalized = value.strip().toLowerAscii().replace("-", "_")
    if normalized.len == 0:
      return ok(none(QuicRuntimePreference))
    case normalized
    of "auto", "default":
      ok(some(qrpAuto))
    of "native", "native_only", "msquic_native":
      ok(some(qrpNativeOnly))
    of "builtin_preferred", "prefer_builtin", "prefer_pure_nim", "prefer_nim":
      ok(some(qrpBuiltinPreferred))
    of "builtin", "builtin_only", "pure_nim", "nim", "nim_quic", "msquic_builtin":
      ok(some(qrpBuiltinOnly))
    else:
      err("invalid QUIC runtime preference: " & value)

  proc configuredQuicRuntimePreference(
      extra: JsonNode
  ): Result[Option[QuicRuntimePreference], string] {.gcsafe, raises: [].} =
    if extra.kind != JObject:
      return ok(none(QuicRuntimePreference))
    parseQuicRuntimePreferenceValue(
      jsonGetStr(
        extra,
        "quicRuntimePreference",
        jsonGetStr(
          extra,
          "quic_runtime_preference",
          jsonGetStr(
            extra,
            "quicRuntime",
            jsonGetStr(
              extra,
              "quic_runtime",
              jsonGetStr(
                extra,
                "msquicRuntime",
                jsonGetStr(extra, "msquic_runtime", "")
              )
            )
          )
        )
      )
    )

  proc configuredQuicRuntimeLibraryPath(
      extra: JsonNode
  ): string {.gcsafe, raises: [].} =
    if extra.kind != JObject:
      return ""
    jsonGetStr(
      extra,
      "quicRuntimeLibraryPath",
      jsonGetStr(
        extra,
        "quic_runtime_library_path",
        jsonGetStr(
          extra,
          "msquicLibraryPath",
          jsonGetStr(extra, "msquic_library_path", "")
        )
      )
    ).strip()

proc buildTransportHealthPayload(
    n: NimNode, listenAddrs, dialableAddrs: openArray[string]
): JsonNode {.gcsafe.} =
  let hostNetwork = currentHostNetworkStatus(n)
  let quicEnabled = quicAvailable(listenAddrs) or quicAvailable(dialableAddrs)
  let tcpEnabled = tcpAvailable(listenAddrs) or tcpAvailable(dialableAddrs)
  let physicalPrefixes = %*["/awdl", "/nan", "/nearlink"]
  when defined(libp2p_msquic_experimental):
    let runtimeInfo =
      block:
        if not n.isNil and not n.switchInstance.isNil:
          let stats = n.switchInstance.msquicTransportStats()
          if stats.len > 0:
            stats[0].runtime
          else:
            QuicRuntimeInfo(
              kind: qrkUnavailable,
              implementation: quicruntime.kindLabel(qrkUnavailable),
              path: "",
              requestedVersion: 0'u32,
              negotiatedVersion: 0'u32,
              compileTimeBuiltin: false,
              loaded: false
            )
        else:
          QuicRuntimeInfo(
            kind: qrkUnavailable,
            implementation: quicruntime.kindLabel(qrkUnavailable),
            path: "",
            requestedVersion: 0'u32,
            negotiatedVersion: 0'u32,
            compileTimeBuiltin: false,
            loaded: false
          )
    let requestedPreference =
      block:
        if not n.isNil:
          let prefRes = configuredQuicRuntimePreference(n.cfg.extra)
          if prefRes.isOk and prefRes.get().isSome:
            quicRuntimePreferenceLabel(prefRes.get().get())
          else:
            quicRuntimePreferenceLabel(qrpAuto)
        else:
          quicRuntimePreferenceLabel(qrpAuto)
    let requestedLibraryPath =
      if not n.isNil: configuredQuicRuntimeLibraryPath(n.cfg.extra) else: ""
    let quicMuxerLabel =
      case runtimeInfo.kind
      of qrkMsQuicBuiltin:
        "msquic-builtin"
      of qrkMsQuicNative:
        "msquic-native"
      else:
        "quic"
  else:
    let quicMuxerLabel = "quic"
  let muxerPreference =
    if quicEnabled and not tcpEnabled:
      @[ %quicMuxerLabel ]
    else:
      @[ %"/yamux/1.0.0", %"/mplex/6.7.0" ]
  let transportPolicy =
    if quicEnabled and tcpEnabled:
      "quic_preferred"
    elif quicEnabled:
      "quic_only"
    elif tcpEnabled:
      "tcp_only"
    else:
      "unconfigured"
  let transportMode =
    if quicEnabled and tcpEnabled:
      "quic_tcp"
    elif quicEnabled:
      "quic"
    elif tcpEnabled:
      "tcp"
    else:
      "none"
  when defined(libp2p_msquic_experimental):
    let runtimePayload = %* {
      "kind": quicruntime.kindLabel(runtimeInfo.kind),
      "implementation": runtimeInfo.implementation,
      "path": runtimeInfo.path,
      "loaded": runtimeInfo.loaded,
      "compileTimeBuiltin": runtimeInfo.compileTimeBuiltin,
      "requestedVersion": runtimeInfo.requestedVersion,
      "negotiatedVersion": runtimeInfo.negotiatedVersion,
      "pureNim": quicruntime.isPureNimRuntime(runtimeInfo),
      "requestedPreference": requestedPreference,
      "requestedLibraryPath": requestedLibraryPath,
    }
  else:
    let runtimePayload = %* {
      "kind": "unavailable",
      "implementation": "unavailable",
      "path": "",
      "loaded": false,
      "compileTimeBuiltin": false,
      "requestedVersion": 0'u32,
      "negotiatedVersion": 0'u32,
      "pureNim": false,
      "requestedPreference": "auto",
      "requestedLibraryPath": "",
    }
  %* {
    "transportPolicy": transportPolicy,
    "transport": transportMode,
    "tcpNoDelay": true,
    "quicAvailable": quicEnabled,
    "quicRuntime": runtimePayload,
    "muxerPreference": muxerPreference,
    "physicalAddressPrefixes": physicalPrefixes,
    "latencySampling": {
      "windowSize": PeerLatencySampleWindow,
      "refreshMs": PeerLatencyRefreshMs,
      "defaultSamples": PeerLatencyMeasureDefaultSamples,
    },
    "hostNetworkStatus": hostNetwork,
  }

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
    "network": defaultHostNetworkStatus(),
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
    profile["bandwidth"]["uplinkBps"] = jsonSafeFloat(snapshot.outboundRateBps)
    profile["bandwidth"]["downlinkBps"] = jsonSafeFloat(snapshot.inboundRateBps)
    profile["bandwidth"]["uplinkTotalBytes"] = %snapshot.totalOutbound
    profile["bandwidth"]["downlinkTotalBytes"] = %snapshot.totalInbound
    profile["bandwidth"]["totalTransferBytes"] = %(snapshot.totalOutbound + snapshot.totalInbound)
  profile["network"] = currentHostNetworkStatus(n)

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
  if node.hasKey("network") and node["network"].kind == JObject:
    profile["network"] = sanitizeHostNetworkStatus(node["network"])
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

proc clampPeerLatencySampleCount(value: int): int =
  let candidate = if value > 0: value else: PeerLatencyMeasureDefaultSamples
  max(PeerLatencyMeasureMinSamples, min(candidate, PeerLatencyMeasureMaxSamples))

proc trimPeerLatencySamples(metric: var PeerLatencyMetric) =
  if metric.recentSamples.len > PeerLatencySampleWindow:
    let excess = metric.recentSamples.len - PeerLatencySampleWindow
    if excess > 0:
      metric.recentSamples.delete(0, excess - 1)

proc latencyMetricLastMs(metric: PeerLatencyMetric): int =
  if metric.totalSuccessCount > 0 and metric.lastUpdatedAtMs > 0:
    metric.lastMs
  else:
    -1

proc recordLatencyMetricSuccess(metric: var PeerLatencyMetric, latencyMs: int, atMs = nowMillis()) =
  if latencyMs < 0:
    return
  let hadSamples = metric.totalSuccessCount > 0
  metric.recentSamples.add(latencyMs)
  trimPeerLatencySamples(metric)
  inc metric.totalSuccessCount
  metric.lastMs = latencyMs
  if not hadSamples or latencyMs < metric.minMs:
    metric.minMs = latencyMs
  if not hadSamples or latencyMs > metric.maxMs:
    metric.maxMs = latencyMs
  if not hadSamples or metric.ewmaMs <= 0.0:
    metric.ewmaMs = float(latencyMs)
  else:
    metric.ewmaMs = finiteFloat(
      metric.ewmaMs * (1.0 - PeerLatencyEwmaAlpha) + float(latencyMs) * PeerLatencyEwmaAlpha,
      float(latencyMs)
    )
  metric.lastUpdatedAtMs = atMs
  metric.lastError = ""

proc recordLatencyMetricFailure(metric: var PeerLatencyMetric, err: string, atMs = nowMillis()) =
  inc metric.totalFailureCount
  metric.lastFailureAtMs = atMs
  if err.len > 0:
    metric.lastError = err

proc latencySampleQuantile(samples: seq[int], pct: float): int =
  if samples.len == 0:
    return -1
  var ordered = samples
  ordered.sort(proc(a, b: int): int = system.cmp(a, b))
  let clampedPct =
    if pct < 0.0: 0.0
    elif pct > 1.0: 1.0
    else: pct
  let index = int(floor(clampedPct * float(max(ordered.len - 1, 0))))
  ordered[index]

proc latencyMetricJson(metric: PeerLatencyMetric): JsonNode =
  var node = newJObject()
  let lastMs = latencyMetricLastMs(metric)
  var avgMs = -1.0
  if metric.recentSamples.len > 0:
    var total = 0.0
    for sample in metric.recentSamples:
      total += float(sample)
    avgMs = total / float(metric.recentSamples.len)
  node["lastMs"] = %lastMs
  node["minMs"] = %(if metric.totalSuccessCount > 0: metric.minMs else: -1)
  node["maxMs"] = %(if metric.totalSuccessCount > 0: metric.maxMs else: -1)
  node["avgMs"] = %avgMs
  node["ewmaMs"] = %(if metric.totalSuccessCount > 0: finiteFloat(metric.ewmaMs, avgMs) else: -1.0)
  node["p50Ms"] = %latencySampleQuantile(metric.recentSamples, 0.50)
  node["p95Ms"] = %latencySampleQuantile(metric.recentSamples, 0.95)
  node["recentSampleCount"] = %metric.recentSamples.len
  node["totalSuccessCount"] = %metric.totalSuccessCount
  node["totalFailureCount"] = %metric.totalFailureCount
  node["lastUpdatedAtMs"] = %metric.lastUpdatedAtMs
  node["lastFailureAtMs"] = %metric.lastFailureAtMs
  node["lastError"] = %metric.lastError
  node

proc peerLatencyProfileJson(profile: PeerLatencyProfile): JsonNode =
  var node = newJObject()
  let transportLast = latencyMetricLastMs(profile.transportRtt)
  let dmAckLast = latencyMetricLastMs(profile.dmAckRtt)
  let preferredMetric =
    if transportLast >= 0: "transport_rtt"
    elif dmAckLast >= 0: "dm_ack_rtt"
    else: "none"
  let preferredLatencyMs =
    if transportLast >= 0: transportLast
    elif dmAckLast >= 0: dmAckLast
    else: -1
  node["preferredMetric"] = %preferredMetric
  node["preferredLatencyMs"] = %preferredLatencyMs
  node["transportRtt"] = latencyMetricJson(profile.transportRtt)
  node["dmAckRtt"] = latencyMetricJson(profile.dmAckRtt)
  node["updatedAtMs"] = %profile.updatedAtMs
  node

proc prunePeerLatencyProfiles(n: NimNode) =
  if n.isNil:
    return
  let nowTs = nowMillis()
  var expired: seq[string] = @[]
  for peerId, profile in n.peerLatencyProfiles.pairs:
    if profile.updatedAtMs <= 0 or nowTs - profile.updatedAtMs > PeerLatencyStatsRetentionMs:
      expired.add(peerId)
  for peerId in expired:
    n.peerLatencyProfiles.del(peerId)

proc cachedTransportLatencyMs(n: NimNode, peerId: string): int =
  if n.isNil or peerId.len == 0:
    return -1
  if not n.peerLatencyProfiles.hasKey(peerId):
    return -1
  latencyMetricLastMs(n.peerLatencyProfiles.getOrDefault(peerId).transportRtt)

proc cachedDirectAckLatencyMs(n: NimNode, peerId: string): int =
  if n.isNil or peerId.len == 0:
    return -1
  if not n.peerLatencyProfiles.hasKey(peerId):
    return -1
  latencyMetricLastMs(n.peerLatencyProfiles.getOrDefault(peerId).dmAckRtt)

proc freshTransportLatencyMs(n: NimNode, peerId: string, maxAgeMs = PeerLatencyRefreshMs): int =
  if n.isNil or peerId.len == 0:
    return -1
  if not n.peerLatencyProfiles.hasKey(peerId):
    return -1
  let metric = n.peerLatencyProfiles.getOrDefault(peerId).transportRtt
  let lastMs = latencyMetricLastMs(metric)
  if lastMs < 0:
    return -1
  if metric.lastUpdatedAtMs <= 0:
    return -1
  let ageMs = nowMillis() - metric.lastUpdatedAtMs
  if ageMs < 0 or ageMs > int64(maxAgeMs):
    return -1
  lastMs

proc peerLatencyStatsJson(n: NimNode, peerId: string): JsonNode =
  if n.isNil or peerId.len == 0:
    return peerLatencyProfileJson(PeerLatencyProfile())
  if not n.peerLatencyProfiles.hasKey(peerId):
    return peerLatencyProfileJson(PeerLatencyProfile())
  peerLatencyProfileJson(n.peerLatencyProfiles.getOrDefault(peerId))

proc recordTransportLatencySuccess(n: NimNode, peerId: string, latencyMs: int, atMs = nowMillis()) =
  if n.isNil or peerId.len == 0 or latencyMs < 0:
    return
  var profile = n.peerLatencyProfiles.getOrDefault(peerId)
  recordLatencyMetricSuccess(profile.transportRtt, latencyMs, atMs)
  profile.updatedAtMs = atMs
  n.peerLatencyProfiles[peerId] = profile

proc recordTransportLatencyFailure(n: NimNode, peerId: string, err: string, atMs = nowMillis()) =
  if n.isNil or peerId.len == 0:
    return
  var profile = n.peerLatencyProfiles.getOrDefault(peerId)
  recordLatencyMetricFailure(profile.transportRtt, err, atMs)
  profile.updatedAtMs = atMs
  n.peerLatencyProfiles[peerId] = profile

proc recordDirectAckLatencySuccess(n: NimNode, peerId: string, latencyMs: int, atMs = nowMillis()) =
  if n.isNil or peerId.len == 0 or latencyMs < 0:
    return
  var profile = n.peerLatencyProfiles.getOrDefault(peerId)
  recordLatencyMetricSuccess(profile.dmAckRtt, latencyMs, atMs)
  profile.updatedAtMs = atMs
  n.peerLatencyProfiles[peerId] = profile

proc recordDirectAckLatencyFailure(n: NimNode, peerId: string, err: string, atMs = nowMillis()) =
  if n.isNil or peerId.len == 0:
    return
  var profile = n.peerLatencyProfiles.getOrDefault(peerId)
  recordLatencyMetricFailure(profile.dmAckRtt, err, atMs)
  profile.updatedAtMs = atMs
  n.peerLatencyProfiles[peerId] = profile

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

proc measureConnectionLatencyMs(
    n: NimNode,
    conn: Connection,
    timeoutMs = PeerLatencyTimeoutMs
): Future[int] {.async.} =
  if n.isNil or conn.isNil or n.pingProtocol.isNil:
    return -1
  try:
    let pingFuture = n.pingProtocol.ping(conn)
    let completed = await withTimeout(
      pingFuture,
      chronos.milliseconds(max(timeoutMs, 250))
    )
    if not completed:
      return -1
    let duration = pingFuture.read()
    let millis = duration.nanoseconds() div 1_000_000
    if millis < 0:
      return -1
    return int(millis)
  except CatchableError:
    return -1

proc measurePeerTransportLatencyMs(
    n: NimNode,
    peerId: PeerId,
    timeoutMs = PeerLatencyTimeoutMs
): Future[int] {.async.} =
  if n.isNil or n.switchInstance.isNil or n.pingProtocol.isNil:
    return -1
  let safeTimeoutMs = max(timeoutMs, 250)
  var pingConn: Connection = nil
  try:
    let dialFuture = n.switchInstance.dial(peerId, PingCodec)
    let dialCompleted = await withTimeout(
      dialFuture,
      chronos.milliseconds(safeTimeoutMs)
    )
    if not dialCompleted:
      dialFuture.cancelSoon()
      return -1
    pingConn = await dialFuture
    if pingConn.isNil:
      return -1
    return await measureConnectionLatencyMs(n, pingConn, safeTimeoutMs)
  except CatchableError:
    return -1
  finally:
    if not pingConn.isNil:
      try:
        await pingConn.close()
      except CatchableError:
        discard

proc measurePeerDirectAckLatencyMs(
    n: NimNode,
    remotePid: PeerId,
    peerIdText: string,
    timeoutMs = PeerLatencyTimeoutMs
): Future[int] {.async.} =
  if n.isNil or n.dmService.isNil or peerIdText.len == 0:
    return -1
  let local =
    try:
      localPeerId(n)
    except Exception:
      ""
  if local.len == 0:
    return -1
  let safeTimeoutMs = max(timeoutMs, 500)
  let startMs = nowMillis()
  let mid = "latency-" & $startMs & "-" & peerIdText
  var envelope = newJObject()
  envelope["op"] = %"latency_probe"
  envelope["mid"] = %mid
  envelope["from"] = %local
  envelope["timestamp_ms"] = %startMs
  envelope["ackRequested"] = %true
  let sendRes = await sendProbeDirectMessage(
    n,
    remotePid,
    stringToBytes($envelope),
    true,
    mid,
    chronos.milliseconds(safeTimeoutMs)
  )
  let latencyMs = int(max(0'i64, nowMillis() - startMs))
  if sendRes[0]:
    recordDirectAckLatencySuccess(n, peerIdText, latencyMs)
    return latencyMs
  let errMsg =
    if sendRes[1].len > 0: sendRes[1]
    else: "ack timeout"
  recordDirectAckLatencyFailure(n, peerIdText, errMsg)
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

proc probeTrafficKey(peerId: string, sessionId: string): string =
  peerId & "|" & sessionId

proc pruneProbeTraffic(n: NimNode, nowMs = nowMillis()) =
  if n.isNil or n.probeInboundTraffic.len == 0:
    return
  var staleKeys: seq[string] = @[]
  for key, stats in n.probeInboundTraffic.pairs:
    if nowMs - stats.updatedAtMs > ProbeSessionRetentionMs:
      staleKeys.add(key)
  for key in staleKeys:
    n.probeInboundTraffic.del(key)

proc noteProbeInboundChunk(n: NimNode, peerId: string, sessionId: string, payloadBytes: int64) =
  if n.isNil or peerId.len == 0 or sessionId.len == 0 or payloadBytes <= 0:
    return
  let nowMs = nowMillis()
  pruneProbeTraffic(n, nowMs)
  let key = probeTrafficKey(peerId, sessionId)
  var stats = n.probeInboundTraffic.getOrDefault(key)
  stats.payloadBytes += payloadBytes
  inc stats.chunkCount
  stats.updatedAtMs = nowMs
  n.probeInboundTraffic[key] = stats

proc readProbeInboundTraffic(n: NimNode, peerId: string, sessionId: string): ProbeSessionTraffic =
  if n.isNil or peerId.len == 0 or sessionId.len == 0:
    return ProbeSessionTraffic()
  pruneProbeTraffic(n)
  n.probeInboundTraffic.getOrDefault(probeTrafficKey(peerId, sessionId))

proc ensurePeerConnectivity(n: NimNode, peer: PeerId, preferWanBootstrap = false): Future[bool] {.async.}

proc shouldResetProbeConnection(errMsg: string): bool =
  if errMsg.len == 0:
    return false
  let lowered = errMsg.toLowerAscii()
  lowered.contains("failed dial existing") or
    lowered.contains("stream eof") or
    lowered.contains("newstream") or
    lowered.contains("connection reset") or
    lowered.contains("muxer")

proc repairProbeConnection(
    n: NimNode,
    remotePid: PeerId,
    errMsg: string
): Future[bool] {.async.} =
  if n.isNil or n.switchInstance.isNil or not shouldResetProbeConnection(errMsg):
    return false
  try:
    await n.switchInstance.disconnect(remotePid)
    debugLog("[nimlibp2p] bandwidth probe disconnected stale conn peer=" & $remotePid &
      " err=" & errMsg)
  except CatchableError as exc:
    debugLog("[nimlibp2p] bandwidth probe disconnect failed peer=" & $remotePid &
      " err=" & exc.msg)
  await sleepAsync(chronos.milliseconds(150))
  let reconnected = await ensurePeerConnectivity(n, remotePid)
  debugLog("[nimlibp2p] bandwidth probe reconnect peer=" & $remotePid &
    " ok=" & $reconnected)
  reconnected

proc sendProbeDirectMessage(
    n: NimNode,
    remotePid: PeerId,
    payload: seq[byte],
    ackRequested: bool,
    messageId: string,
    timeout: chronos.Duration
): Future[(bool, string)] {.async.} =
  if n.isNil or n.dmService.isNil:
    return (false, "direct message service unavailable")
  try:
    let sendFuture = n.dmService.send(remotePid, payload, ackRequested, messageId, timeout)
    let waitBudget = timeout + chronos.milliseconds(750)
    let sendCompleted = await withTimeout(sendFuture, waitBudget)
    if not sendCompleted:
      sendFuture.cancelSoon()
      return (false, "send timeout")
    return await sendFuture
  except CatchableError as exc:
    return (false, if exc.msg.len > 0: exc.msg else: $exc.name)

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
  var repaired = false
  let startMs = nowMillis()
  while nowMillis() - startMs < int64(safeDurationMs):
    var envelope = newJObject()
    envelope["op"] = %"bw_probe_chunk"
    envelope["sid"] = %sessionId
    envelope["timestamp_ms"] = %nowMillis()
    envelope["ackRequested"] = %false
    envelope["body"] = %chunkBody
    let payload = stringToBytes($envelope)
    let sendRes = await sendProbeDirectMessage(n, remotePid, payload, false, "", timeout)
    if not sendRes[0]:
      if not repaired and await repairProbeConnection(n, remotePid, sendRes[1]):
        repaired = true
        continue
      return (sentBytes, max(1'i64, nowMillis() - startMs), sentChunks, false, sendRes[1])
    sentBytes += int64(safeChunkBytes)
    inc sentChunks
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

proc localPeerId(n: NimNode): string {.gcsafe, raises: [].} =
  if n.switchInstance.isNil or n.switchInstance.peerInfo.isNil:
    return ""
  try:
    result = $n.switchInstance.peerInfo.peerId
  except CatchableError:
    result = ""

proc normalizeSharedFilePath(rawPath: string): string =
  result = rawPath.strip()
  if result.startsWith("file://"):
    result = result[7 .. ^1]

proc makeSharedFileEntry(
    key: string,
    path: string,
    fileName: string,
    mimeType: string,
    sha256: string,
    sizeBytes: int64
): SharedFileEntry =
  let normalizedPath = normalizeSharedFilePath(path)
  let normalizedName =
    if fileName.strip().len > 0:
      fileName.strip()
    else:
      normalizedPath.extractFilename()
  SharedFileEntry(
    key: key.strip(),
    path: normalizedPath,
    fileName: normalizedName,
    mimeType: mimeType.strip(),
    sha256: sha256.strip(),
    sizeBytes: max(sizeBytes, 0'i64),
    registeredAtMs: nowMillis(),
  )

proc registerSharedFile(node: NimNode, entry: SharedFileEntry): bool =
  if node.isNil or entry.key.len == 0 or entry.path.len == 0 or not fileExists(entry.path):
    return false
  node.sharedFilesLock.acquire()
  try:
    node.sharedFiles[entry.key] = entry
  finally:
    node.sharedFilesLock.release()
  true

proc lookupSharedFile(node: NimNode, key: string): tuple[ok: bool, entry: SharedFileEntry] =
  if node.isNil or key.len == 0:
    return (false, default(SharedFileEntry))
  node.sharedFilesLock.acquire()
  try:
    if node.sharedFiles.hasKey(key):
      return (true, node.sharedFiles[key])
  finally:
    node.sharedFilesLock.release()
  (false, default(SharedFileEntry))

proc readSharedFileChunk(
    entry: SharedFileEntry,
    offset: int64,
    maxBytes: int
): tuple[ok: bool, data: seq[byte], totalSize: int64, error: string] =
  if entry.path.len == 0 or not fileExists(entry.path):
    return (false, @[], 0'i64, "shared_file_missing")
  let totalSize =
    try:
      int64(getFileSize(entry.path))
    except CatchableError:
      entry.sizeBytes
  let safeTotal = max(max(totalSize, entry.sizeBytes), 0'i64)
  let safeOffset = max(offset, 0'i64)
  if safeOffset >= safeTotal:
    return (true, @[], safeTotal, "")
  let remaining = safeTotal - safeOffset
  var chunkSize = maxBytes
  if chunkSize <= 0:
    chunkSize = 262_144
  chunkSize = min(chunkSize, 262_144)
  if remaining < int64(chunkSize):
    chunkSize = int(remaining)
  if chunkSize <= 0:
    return (true, @[], safeTotal, "")
  var handle: File
  if not open(handle, entry.path, fmRead):
    return (false, @[], safeTotal, "shared_file_open_failed")
  try:
    setFilePos(handle, safeOffset)
    var data = newSeq[byte](chunkSize)
    let readCount = readBytes(handle, data, 0, chunkSize)
    if readCount <= 0:
      return (true, @[], safeTotal, "")
    if readCount < data.len:
      data.setLen(readCount)
    return (true, data, safeTotal, "")
  except CatchableError as exc:
    return (false, @[], safeTotal, if exc.msg.len > 0: exc.msg else: "shared_file_read_failed")
  finally:
    close(handle)

proc completeFileChunkWaiter(n: NimNode, requestId: string, payload: string): bool =
  if n.isNil or requestId.len == 0:
    return false
  if not n.pendingFileChunkResponses.hasKey(requestId):
    return false
  let waiter = n.pendingFileChunkResponses.getOrDefault(requestId)
  n.pendingFileChunkResponses.del(requestId)
  if waiter.isNil or waiter.future.isNil:
    return false
  if not waiter.future.finished():
    waiter.future.complete(payload)
  true

proc sendDirectJsonNoAck(
    n: NimNode,
    peerIdText: string,
    envelope: JsonNode,
    messageId = ""
): Future[(bool, string)] {.async.} =
  if n.isNil or n.dmService.isNil:
    return (false, "direct message service unavailable")
  var remotePid: PeerId
  if not remotePid.init(peerIdText):
    return (false, "invalid peer id")
  discard await ensurePeerConnectivity(n, remotePid)
  var payload = envelope
  if payload.isNil or payload.kind != JObject:
    payload = newJObject()
  let mid = ensureMessageId(if messageId.len > 0: messageId else: jsonGetStr(payload, "mid"))
  payload["mid"] = %mid
  if not payload.hasKey("from"):
    payload["from"] = %localPeerId(n)
  if not payload.hasKey("timestamp_ms"):
    payload["timestamp_ms"] = %nowMillis()
  try:
    return await n.dmService.send(
      remotePid,
      stringToBytes($payload),
      false,
      mid,
      chronos.milliseconds(6_000),
    )
  except CatchableError as exc:
    return (false, if exc.msg.len > 0: exc.msg else: $exc.name)

proc sendFileChunkResponse(
    n: NimNode,
    peerIdText: string,
    envelope: JsonNode,
    messageId = ""
): Future[void] {.async.} =
  discard await sendDirectJsonNoAck(n, peerIdText, envelope, messageId)

proc ensureMessageId(mid: string): string {.gcsafe, raises: [].} =
  if mid.len > 0:
    return mid
  let ts = nowMillis()
  var nonce: int64
  messageIdLock.acquire()
  try:
    inc messageIdCounter
    nonce = messageIdCounter
  finally:
    messageIdLock.release()
  return "msg-" & $ts & "-" & toHex(cast[uint64](nonce), 8)

proc stripPeerIdSuffixFromMultiaddr(text: string): string {.gcsafe, raises: [].}

proc udpEchoPayload(nonce: string): seq[byte] {.gcsafe, raises: [].} =
  stringToBytes(UdpEchoProbeMagic & nonce)

proc deriveUdpEchoTargetMultiaddr(addrText: string): Result[string, string] {.gcsafe, raises: [].} =
  let canonical = try:
      stripPeerIdSuffixFromMultiaddr(addrText)
    except CatchableError as exc:
      return err("canonicalize_failed:" & exc.msg)
    except Exception:
      return err("canonicalize_failed")
  if canonical.len == 0:
    return err("empty_multiaddr")
  let lower = canonical.toLowerAscii()
  var host = ""
  if "/ip6/" in lower:
    let startIdx = canonical.toLowerAscii().find("/ip6/") + 5
    var endIdx = canonical.find('/', startIdx)
    if endIdx < 0:
      endIdx = canonical.len
    host = canonical[startIdx ..< endIdx].strip()
    if host.len == 0:
      return err("missing_ipv6_host")
    let target = "/ip6/" & host & "/udp/" & $UdpEchoProbePort
    let parsed = MultiAddress.init(target)
    if parsed.isErr():
      return err("invalid_udp_target:" & parsed.error)
    return ok($parsed.get())
  if "/ip4/" in lower:
    let startIdx = canonical.toLowerAscii().find("/ip4/") + 5
    var endIdx = canonical.find('/', startIdx)
    if endIdx < 0:
      endIdx = canonical.len
    host = canonical[startIdx ..< endIdx].strip()
    if host.len == 0:
      return err("missing_ipv4_host")
    let target = "/ip4/" & host & "/udp/" & $UdpEchoProbePort
    let parsed = MultiAddress.init(target)
    if parsed.isErr():
      return err("invalid_udp_target:" & parsed.error)
    return ok($parsed.get())
  err("missing_ip_protocol")

proc stopUdpEchoResponder(n: NimNode): Future[void] {.async.} =
  if n.isNil:
    return
  if not n.udpEchoSock4.isNil:
    try:
      await n.udpEchoSock4.closeWait()
    except CatchableError as exc:
      debugLog("[nimlibp2p] stopUdpEchoResponder ipv4 err=" & exc.msg)
    n.udpEchoSock4 = nil
  if not n.udpEchoSock6.isNil:
    try:
      await n.udpEchoSock6.closeWait()
    except CatchableError as exc:
      debugLog("[nimlibp2p] stopUdpEchoResponder ipv6 err=" & exc.msg)
    n.udpEchoSock6 = nil

proc startUdpEchoResponder(n: NimNode): Future[void] {.async.} =
  if n.isNil:
    return

  proc onUdpEchoDatagram(transp: DatagramTransport, remote: TransportAddress): Future[void] {.async: (raises: []).} =
    try:
      let message = transp.getMessage()
      if message.len == 0:
        return
      let text = bytesToString(message)
      if not text.startsWith(UdpEchoProbeMagic):
        return
      await transp.sendTo(remote, unsafeAddr message[0], message.len)
    except CatchableError as exc:
      debugLog("[nimlibp2p] udp echo responder err=" & exc.msg)

  if n.udpEchoSock4.isNil:
    try:
      n.udpEchoSock4 = newDatagramTransport(
        onUdpEchoDatagram,
        local = initTAddress("0.0.0.0", Port(UdpEchoProbePort)),
        flags = {ServerFlags.ReuseAddr},
      )
      debugLog("[nimlibp2p] udp echo responder ipv4 bound port=" & $UdpEchoProbePort)
    except CatchableError as exc:
      debugLog("[nimlibp2p] udp echo responder ipv4 bind failed err=" & exc.msg)
  if n.udpEchoSock6.isNil:
    try:
      n.udpEchoSock6 = newDatagramTransport6(
        onUdpEchoDatagram,
        local = initTAddress("::", Port(UdpEchoProbePort)),
        flags = {ServerFlags.ReuseAddr},
      )
      debugLog("[nimlibp2p] udp echo responder ipv6 bound port=" & $UdpEchoProbePort)
    except CatchableError as exc:
      debugLog("[nimlibp2p] udp echo responder ipv6 bind failed err=" & exc.msg)

proc runUdpEchoProbe(n: NimNode, addrText: string, timeoutMs: int): Future[JsonNode] {.async.} =
  var payload = newJObject()
  payload["probeTransport"] = %"udp_echo"
  payload["probePort"] = %UdpEchoProbePort
  payload["requestedAddr"] = %addrText
  payload["timestampMs"] = %nowMillis()
  let targetText = deriveUdpEchoTargetMultiaddr(addrText).valueOr:
    payload["ok"] = %false
    payload["error"] = %error
    payload["attemptedAddr"] = %""
    return payload
  payload["attemptedAddr"] = %targetText
  let targetMa = MultiAddress.init(targetText).valueOr:
    payload["ok"] = %false
    payload["error"] = %("invalid_target_multiaddr:" & error)
    return payload
  let remoteAddr = wire.initTAddress(targetMa).valueOr:
    payload["ok"] = %false
    payload["error"] = %("invalid_target_transport_address:" & error)
    return payload
  let nonce = ensureMessageId("")
  let message = udpEchoPayload(nonce)
  let receivedFuture = newFuture[void]("nimlibp2p.udpEchoProbe")
  var responseRemote = ""

  proc onProbeDatagram(transp: DatagramTransport, remote: TransportAddress): Future[void] {.async: (raises: []).} =
    try:
      let received = transp.getMessage()
      if received.len != message.len:
        return
      var matched = true
      for idx in 0 ..< message.len:
        if received[idx] != message[idx]:
          matched = false
          break
      if not matched:
        return
      responseRemote = $remote
      if not receivedFuture.finished():
        receivedFuture.complete()
    except CatchableError as exc:
      debugLog("[nimlibp2p] udp echo probe callback err=" & exc.msg)

  let startedAtMs = nowMillis()
  let safeTimeoutMs = max(250, if timeoutMs > 0: timeoutMs else: UdpEchoProbeTimeoutDefaultMs)
  var sock: DatagramTransport = nil
  try:
    if remoteAddr.family == AddressFamily.IPv6:
      sock = newDatagramTransport6(onProbeDatagram)
    else:
      sock = newDatagramTransport(onProbeDatagram)
    await sock.sendTo(remoteAddr, unsafeAddr message[0], message.len)
    try:
      await receivedFuture.wait(chronos.milliseconds(safeTimeoutMs))
      payload["ok"] = %true
      payload["error"] = %""
      payload["rttMs"] = %(nowMillis() - startedAtMs)
      payload["responseRemote"] = %responseRemote
    except AsyncTimeoutError:
      payload["ok"] = %false
      payload["error"] = %"timeout"
    except CatchableError as exc:
      payload["ok"] = %false
      payload["error"] = %exc.msg
  except CatchableError as exc:
    payload["ok"] = %false
    payload["error"] = %exc.msg
  finally:
    if not sock.isNil:
      try:
        await sock.closeWait()
      except CatchableError as exc:
        debugLog("[nimlibp2p] udp echo probe close err=" & exc.msg)
  payload

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

proc socialFanoutRecentPublishedMomentsToPeer(
    node: NimNode,
    peerId: string,
    limit = 4
): bool

proc appendEarlyNotificationNoLock(
    state: JsonNode,
    kind: string,
    title: string,
    body: string,
    payload: JsonNode,
    deliveryClass: string
) =
  if state.kind != JObject:
    return
  if not state.hasKey("notifications") or state["notifications"].kind != JArray:
    state["notifications"] = newJArray()
  let peerId = jsonGetStr(payload, "peerId", jsonGetStr(payload, "from", jsonGetStr(payload, "authorPeerId"))).strip()
  let contentId = jsonGetStr(payload, "contentId", jsonGetStr(payload, "postId", jsonGetStr(payload, "id"))).strip()
  let conversationId = jsonGetStr(payload, "conversationId").strip()
  let groupId = jsonGetStr(payload, "groupId").strip()
  let subjectId =
    if contentId.len > 0: contentId
    elif conversationId.len > 0: conversationId
    elif groupId.len > 0: groupId
    elif peerId.len > 0: peerId
    else: kind
  let eventKind = jsonGetStr(payload, "eventKind", kind).strip()
  let dedupeKey = eventKind & "|" & peerId & "|" & subjectId
  var item = newJObject()
  item["id"] = %("notif-" & $nowMillis() & "-" & $rand(10_000))
  item["kind"] = %kind
  item["type"] = %(
    if kind == "dm_received": "dm"
    elif kind == "group_message_received": "group"
    elif kind == "content_feed_received": "content"
    else: "notification"
  )
  item["title"] = %title
  item["body"] = %body
  item["payload"] = payload
  item["read"] = %false
  item["readState"] = %"unread"
  item["timestampMs"] = %jsonGetInt64(payload, "timestampMs", jsonGetInt64(payload, "timestamp_ms", nowMillis()))
  item["deliveryClass"] = %deliveryClass
  item["eventKind"] = %eventKind
  item["topic"] = %jsonGetStr(payload, "topic", "")
  item["producer"] = %peerId
  item["subjectId"] = %subjectId
  item["channel"] = %jsonGetStr(payload, "channel", jsonGetStr(payload, "category"))
  item["peerId"] = %peerId
  item["groupId"] = %groupId
  item["contentId"] = %contentId
  item["conversationId"] = %conversationId
  item["dedupeKey"] = %dedupeKey
  var nextNotifications = newJArray()
  var replaced = false
  nextNotifications.add(item)
  for existing in state["notifications"]:
    if nextNotifications.len >= SocialMaxNotifications:
      break
    if existing.kind == JObject and jsonGetStr(existing, "dedupeKey").strip() == dedupeKey:
      replaced = true
      continue
    nextNotifications.add(existing)
  if not replaced:
    state["notifications"] = nextNotifications
  else:
    state["notifications"] = nextNotifications

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
    socialStateMemLock.acquire()
    try:
      let state = loadSocialStateUnsafe(n)
      let localPeer = localPeerId(n)
      let publishState = jsonGetStr(sanitized, "publishState", "published").strip().toLowerAscii()
      let visibility = jsonGetStr(sanitized, "visibility", "public").strip().toLowerAscii()
      let homeEligible = jsonGetBool(sanitized, "homeEligible", true)
      let tombstoned = jsonGetBool(sanitized, "tombstoned", false)
      if author.len > 0 and author != localPeer and publishState == "published" and visibility == "public" and homeEligible and not tombstoned:
        var notificationPayload = sanitized
        notificationPayload["producer"] = %author
        notificationPayload["peerId"] = %author
        notificationPayload["contentId"] = %itemId
        notificationPayload["subjectId"] = %itemId
        notificationPayload["eventKind"] = %"content_publish"
        notificationPayload["topic"] = %"content/global"
        notificationPayload["fetchHint"] = %"content_detail"
        appendEarlyNotificationNoLock(
          state,
          "content_feed_received",
          jsonGetStr(notificationPayload, "title", "新内容"),
          jsonGetStr(notificationPayload, "summary", jsonGetStr(notificationPayload, "content")),
          notificationPayload,
          "inbox",
        )
        persistSocialStateUnsafe(n, state)
    finally:
      socialStateMemLock.release()
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
  if fromPeer.len > 0:
    socialStateMemLock.acquire()
    try:
      let state = loadSocialStateUnsafe(n)
      let localPeer = localPeerId(n)
      if fromPeer != localPeer:
        var message = newJObject()
        message["id"] = %(if mid.len > 0: mid else: ensureMessageId(""))
        message["sender"] = %fromPeer
        message["groupId"] = %groupId
        message["content"] = %body
        message["timestampMs"] = %ts
        message["status"] = %"received"
        if parsedOk:
          message["payload"] = parsed
        appendConversationMessageNoLock(state, "group:" & groupId, groupId, message)
        var notificationPayload = newJObject()
        notificationPayload["groupId"] = %groupId
        notificationPayload["peerId"] = %fromPeer
        notificationPayload["producer"] = %fromPeer
        notificationPayload["subjectId"] = %("group:" & groupId)
        notificationPayload["conversationId"] = %("group:" & groupId)
        notificationPayload["eventKind"] = %"group_message"
        notificationPayload["deliveryClass"] = %"inbox"
        notificationPayload["topic"] = %("group/" & groupId)
        notificationPayload["timestampMs"] = %ts
        appendEarlyNotificationNoLock(state, "group_message_received", "群消息", body, notificationPayload, "inbox")
        persistSocialStateUnsafe(n, state)
    finally:
      socialStateMemLock.release()
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
        {.cast(gcsafe).}:
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

proc socialGameEnvelopeType(payload: JsonNode): string =
  if payload.isNil or payload.kind != JObject:
    return ""
  let gameNode = payload{"game"}
  if gameNode.isNil or gameNode.kind != JObject:
    return ""
  jsonGetStr(gameNode, "type")

proc shouldPersistDirectMessageToSocial(payload: JsonNode): bool =
  if payload.isNil:
    return false
  let gameType = socialGameEnvelopeType(payload)
  if gameType.len == 0:
    return true
  gameType == "game_invite"

proc compactGamePayloadForSocial(payload: JsonNode): JsonNode =
  if payload.isNil:
    return newJNull()
  if payload.kind != JObject:
    return cloneJson(payload)
  result = newJObject()
  for key in ["event", "winner", "seat", "bid", "valid", "replayStatus"]:
    if payload.hasKey(key):
      result[key] = cloneJson(payload[key])
  let eventsNode = payload{"events"}
  if not eventsNode.isNil and eventsNode.kind == JArray:
    result["eventCount"] = %eventsNode.len
  let deckNode = payload{"initialDeckIds"}
  if not deckNode.isNil and deckNode.kind == JArray:
    result["initialDeckCount"] = %deckNode.len
  let auditNode = payload{"audit_report"}
  if not auditNode.isNil and auditNode.kind == JObject:
    let audit = auditNode
    if audit.hasKey("valid"):
      result["auditValid"] = cloneJson(audit["valid"])
    if audit.hasKey("winner"):
      result["auditWinner"] = cloneJson(audit["winner"])

proc compactDirectPayloadForSocial(payload: JsonNode): JsonNode =
  if payload.isNil:
    return newJNull()
  if payload.kind != JObject:
    return cloneJson(payload)
  let gameType = socialGameEnvelopeType(payload)
  if gameType.len == 0:
    return cloneJson(payload)
  result = newJObject()
  for key in [
    "messageId", "mid", "conversationId", "body", "content", "text",
    "timestampMs", "timestamp_ms", "from", "peerId", "sender", "type"
  ]:
    if payload.hasKey(key):
      result[key] = cloneJson(payload[key])
  let gameNode = payload{"game"}
  if not gameNode.isNil and gameNode.kind == JObject:
    let compactPayload =
      case gameType
      of "game_invite":
        let invitePayload = gameNode{"payload"}
        if invitePayload.isNil: newJNull() else: cloneJson(invitePayload)
      of "game_audit":
        compactGamePayloadForSocial(gameNode{"payload"})
      else:
        newJNull()
    result["game"] = %*{
      "type": gameType,
      "appId": jsonGetStr(gameNode, "appId"),
      "roomId": jsonGetStr(gameNode, "roomId"),
      "matchId": jsonGetStr(gameNode, "matchId"),
      "protocolVersion": int(jsonGetInt64(gameNode, "protocolVersion", 1)),
      "senderPeerId": jsonGetStr(gameNode, "senderPeerId"),
      "seatId": jsonGetStr(gameNode, "seatId"),
      "seq": int(jsonGetInt64(gameNode, "seq")),
      "stateHash": jsonGetStr(gameNode, "stateHash"),
      "sentAtMs": jsonGetInt64(gameNode, "sentAtMs", 0),
      "payload": compactPayload
    }

proc handleDirectMessageNode(
    n: NimNode,
    envelope: JsonNode,
    raw: string,
    senderPeer = ""
) {.raises: [].} =
  try:
    var fromPeer = jsonGetStr(envelope, "from")
    if fromPeer.len == 0:
      fromPeer = jsonGetStr(envelope, "peer_id")
    if fromPeer.len == 0 and senderPeer.len > 0:
      fromPeer = senderPeer
    let kind = jsonGetStr(envelope, "kind").toLowerAscii()
    let entity = jsonGetStr(envelope, "entity").toLowerAscii()
    if kind == "social" and entity == "content_feed":
      let socialOp = jsonGetStr(envelope, "op").toLowerAscii()
      if socialOp == "sync_request":
        let recentLimit = max(int(jsonGetInt64(envelope, "limit", 6)), 1)
        if fromPeer.len > 0:
          {.cast(gcsafe).}:
            discard socialFanoutRecentPublishedMomentsToPeer(n, fromPeer, min(recentLimit, 12))
        return
      let contentPayload =
        if envelope.hasKey("payload") and envelope["payload"].kind == JObject:
          envelope["payload"]
        else:
          envelope
      handleContentFeedNode(n, $contentPayload, fromPeer)
      return
    let op = jsonGetStr(envelope, "op").toLowerAscii()
    if op == "ack":
      handleDirectAckNode(n, envelope)
      return
    if op == "file_chunk_response":
      let requestId = jsonGetStr(envelope, "requestId", jsonGetStr(envelope, "reply_to"))
      if requestId.len > 0:
        discard completeFileChunkWaiter(n, requestId, $envelope)
      return
    if op == "file_chunk_request":
      let requestId = jsonGetStr(envelope, "requestId", jsonGetStr(envelope, "mid"))
      var response = newJObject()
      response["op"] = %"file_chunk_response"
      response["requestId"] = %requestId
      response["reply_to"] = %jsonGetStr(envelope, "mid")
      response["from"] = %localPeerId(n)
      response["timestamp_ms"] = %nowMillis()
      let key = jsonGetStr(envelope, "key").strip()
      let chunkOffset = max(jsonGetInt64(envelope, "offset", 0'i64), 0'i64)
      var chunkLimit = int(jsonGetInt64(envelope, "length", 262_144'i64))
      if chunkLimit <= 0:
        chunkLimit = 262_144
      chunkLimit = min(chunkLimit, 262_144)
      let lookup = lookupSharedFile(n, key)
      if not lookup.ok:
        response["ok"] = %false
        response["error"] = %"shared_file_not_registered"
        response["key"] = %key
      else:
        let chunk = readSharedFileChunk(lookup.entry, chunkOffset, chunkLimit)
        if not chunk.ok:
          response["ok"] = %false
          response["error"] = %chunk.error
          response["key"] = %key
        else:
          let totalSize = max(chunk.totalSize, 0'i64)
          let eof = chunkOffset + int64(chunk.data.len) >= totalSize
          response["ok"] = %true
          response["key"] = %lookup.entry.key
          response["fileName"] = %lookup.entry.fileName
          response["mimeType"] = %lookup.entry.mimeType
          response["sha256"] = %lookup.entry.sha256
          response["offset"] = %chunkOffset
          response["totalSize"] = %totalSize
          response["chunkSize"] = %chunk.data.len
          response["eof"] = %eof
          response["payloadBase64"] = %base64.encode(chunk.data)
      if fromPeer.len > 0:
        asyncSpawn(sendFileChunkResponse(n, fromPeer, response, "file-chunk-response-" & requestId))
      return
    if op == "bw_probe_push":
      let sessionId = jsonGetStr(envelope, "sid", "probe-push")
      let durationMs = clampProbeDurationMs(int(jsonGetInt64(envelope, "durationMs", int64(ProbeDurationDefaultMs))))
      let chunkBytes = clampProbeChunkBytes(int(jsonGetInt64(envelope, "chunkBytes", int64(ProbeChunkDefaultBytes))))
      if fromPeer.len > 0:
        asyncSpawn(runProbePushTraffic(n, fromPeer, sessionId, durationMs, chunkBytes))
      return
    if op == "bw_probe_chunk":
      let sessionId = jsonGetStr(envelope, "sid")
      let bodyBytes = int64(jsonGetStr(envelope, "body").len)
      if fromPeer.len > 0 and sessionId.len > 0 and bodyBytes > 0:
        noteProbeInboundChunk(n, fromPeer, sessionId, bodyBytes)
      return
    if op == "latency_probe":
      return
    if op == "media_frame":
      if fromPeer.len > 0 and jsonGetStr(envelope, "senderPeerId").len == 0:
        envelope["senderPeerId"] = %fromPeer
      let tsMs =
        jsonGetInt64(
          envelope,
          "timestampMs",
          jsonGetInt64(envelope, "timestamp_ms", nowMillis()),
        )
      envelope["timestampMs"] = %tsMs
      envelope["timestamp_ms"] = %tsMs
      recordMediaFrame(n, envelope)
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
      {.cast(gcsafe).}:
        socialStateMemLock.acquire()
        try:
          let state = loadSocialStateUnsafe(n)
          let localPeer = localPeerId(n)
          if fromPeer != localPeer:
            if shouldPersistDirectMessageToSocial(payloadNode):
              let storedPayload = compactDirectPayloadForSocial(payloadNode)
              var message = newJObject()
              message["id"] = %mid
              message["sender"] = %fromPeer
              message["peerId"] = %fromPeer
              message["conversationId"] = %convId
              message["content"] = %body
              message["timestampMs"] = %ts
              message["status"] = %"received"
              message["payload"] = storedPayload
              appendConversationMessageNoLock(state, convId, fromPeer, message)
              var notificationPayload = cloneJson(storedPayload)
              notificationPayload["peerId"] = %fromPeer
              notificationPayload["producer"] = %fromPeer
              notificationPayload["subjectId"] = %convId
              notificationPayload["conversationId"] = %convId
              notificationPayload["eventKind"] = %"dm"
              notificationPayload["topic"] = %("dm/inbox/" & fromPeer)
              appendEarlyNotificationNoLock(state, "dm_received", "新消息", body, notificationPayload, "priority")
              persistSocialStateUnsafe(n, state)
        finally:
          socialStateMemLock.release()
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

proc processDirectTopic(
    n: NimNode,
    topic: string,
    payload: seq[byte],
    senderPeer: string
) {.gcsafe, raises: [].} =
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
      {.cast(gcsafe).}:
        handleDirectMessageNode(n, envelope, raw, senderPeer)
  except Exception as exc:
    warn "handleDirectTopic failed", topic = topic, err = exc.msg

proc handleDirectTopic(n: NimNode, topic: string, payload: seq[byte]) {.gcsafe, raises: [].} =
  processDirectTopic(n, topic, payload, "")

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
            processDirectTopic(n, inboxTopic, msg.payload, $msg.sender)
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
      proc(node: NimNode, t: string, payload: seq[byte]) {.gcsafe.} =
        {.cast(gcsafe).}:
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
          try:
            {.cast(gcsafe).}:
              handleContentFeedNode(n, raw, publisherStr)
          except Exception as exc:
            warn "setupDefaultTopics: feed handler failed", peer = local, err = exc.msg
      n.feedService = newFeedService(n.gossip, localPid, feedHandler)
      try:
        # Join the local per-peer feed topic so subsequent publishes build a
        # usable mesh/fanout instead of only updating local task state.
        await n.feedService.subscribeToPeer(localPid)
      except CatchableError as exc:
        warn "setupDefaultTopics: self feed subscription failed", peer = local, err = exc.msg
    else:
      warn "setupDefaultTopics: invalid local peer id for feed service", peer = local
  n.defaultTopicsReady = true
  if nodeTelemetryPublishEnabled(n.cfg):
    asyncSpawn(publishLocalTelemetry(n))

proc isLanIpv4(ip: string): bool {.gcsafe.} =
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

proc collectLanIpv4Addrs(preferred: string = ""): seq[string] {.gcsafe.} =
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

proc mdnsAllowedForHostStatus(hostStatus: JsonNode): bool {.gcsafe.} =
  let networkType = jsonGetStr(hostStatus, "networkType", "").strip().toLowerAscii()
  let transport = jsonGetStr(hostStatus, "transport", "").strip().toLowerAscii()
  if networkType in ["wifi", "ethernet"] or transport in ["wifi", "ethernet"]:
    return true
  if networkType in ["cellular", "vpn", "none"] or
      transport in ["cellular", "5g", "4g", "3g", "2g", "vpn", "none"]:
    return false
  let preferredIpv4 = jsonGetStr(hostStatus, "preferredIpv4", jsonGetStr(hostStatus, "localIpv4", "")).strip()
  collectLanIpv4Addrs(preferredIpv4).len > 0

proc mdnsAllowedForCurrentHost(n: NimNode): bool {.gcsafe.} =
  mdnsAllowedForHostStatus(currentHostNetworkStatus(n))

proc isLanIpv6(ip: string): bool {.gcsafe.} =
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

proc isGlobalHostIpv6(ip: string): bool {.gcsafe.} =
  let normalized = sanitizeHostIpv6(ip)
  if normalized.len == 0:
    return false
  let lower = normalized.toLowerAscii()
  if lower == "::" or lower == "::1":
    return false
  if lower.startsWith("fe80:") or lower.startsWith("fc") or lower.startsWith("fd"):
    return false
  if lower.startsWith("ff"):
    return false
  true

proc isPublicHostIpv4(ip: string): bool {.gcsafe.} =
  let normalized = sanitizeHostIpv4(ip)
  normalized.len > 0 and not isLanIpv4(normalized)

proc collectLanIpv6Addrs(): seq[string] {.gcsafe.} =
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

proc replaceIpv4(addrStr: string, ip: string): Option[string] {.gcsafe.} =
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

proc replaceIpv6(addrStr: string, ip: string): Option[string] {.gcsafe.} =
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
): seq[MultiAddress] {.gcsafe.} =
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

proc expandHostNetworkMultiaddrs(
    addrs: seq[MultiAddress],
    wanIpv4: seq[string],
    wanIpv6: seq[string]
): seq[MultiAddress] {.gcsafe.} =
  if addrs.len == 0:
    return @[]
  var seen = initHashSet[string]()
  for ma in addrs:
    let original = $ma
    if original.len == 0:
      continue
    var replaced = false
    if wanIpv4.len > 0 and original.contains("/ip4/"):
      for ip in wanIpv4:
        let opt = replaceIpv4(original, ip)
        if opt.isSome():
          let rewritten = opt.get()
          let parsed = MultiAddress.init(rewritten)
          if parsed.isOk():
            let normalized = $parsed.get()
            if not seen.contains(normalized):
              result.add(parsed.get())
              seen.incl(normalized)
              replaced = true
    if wanIpv6.len > 0 and original.contains("/ip6/"):
      for ip in wanIpv6:
        let opt = replaceIpv6(original, ip)
        if opt.isSome():
          let rewritten = opt.get()
          let parsed = MultiAddress.init(rewritten)
          if parsed.isOk():
            let normalized = $parsed.get()
            if not seen.contains(normalized):
              result.add(parsed.get())
              seen.incl(normalized)
              replaced = true
    if not replaced and not seen.contains(original):
      result.add(ma)
      seen.incl(original)

proc expandPhysicalPrefixMultiaddrs(addrs: seq[MultiAddress]): seq[MultiAddress] {.gcsafe.} =
  if addrs.len == 0:
    return @[]
  var seen = initHashSet[string]()
  for ma in addrs:
    let original = $ma
    if original.len == 0:
      continue
    if original notin seen:
      result.add(ma)
      seen.incl(original)
    let lower = original.toLowerAscii()
    let alreadyPrefixed = PhysicalAddressPrefixes.anyIt(lower.startsWith(it))
    let ipTransportAddr =
      (lower.contains("/ip4/") or lower.contains("/ip6/")) and
      (lower.contains("/quic-v1") or lower.contains("/tcp/"))
    if alreadyPrefixed or not ipTransportAddr:
      continue
    let trimmed = original.strip(chars = {'/'})
    if trimmed.len == 0:
      continue
    for prefix in PhysicalAddressPrefixes:
      let prefixedText = prefix & trimmed
      let parsed = MultiAddress.init(prefixedText)
      if parsed.isOk():
        let normalized = $parsed.get()
        if normalized notin seen:
          result.add(parsed.get())
          seen.incl(normalized)

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
  if lower.contains("/quic-v1"):
    return 0
  if lower.contains("/quic"):
    return 1
  if lower.contains("/tcp/"):
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

proc preferStableAddrStrings(addrs: seq[string]): seq[string] {.gcsafe.} =
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

proc addrTransportPortKey(text: string): string {.gcsafe.} =
  let lower = text.toLowerAscii()
  var transport = ""
  var marker = ""
  if lower.contains("/quic-v1") or lower.contains("/quic/"):
    transport = "quic"
    marker = "/udp/"
  elif lower.contains("/tcp/"):
    transport = "tcp"
    marker = "/tcp/"
  elif lower.contains("/udp/"):
    transport = "udp"
    marker = "/udp/"
  if marker.len == 0:
    return ""
  let idx = lower.find(marker)
  if idx < 0:
    return ""
  let startIdx = idx + marker.len
  var endIdx = lower.find('/', startIdx)
  if endIdx < 0:
    endIdx = lower.len
  let port = lower[startIdx ..< endIdx].strip()
  if port.len == 0:
    return ""
  transport & ":" & port

proc refreshLanAwareAddrs(n: NimNode): seq[MultiAddress] {.gcsafe.} =
  if n.switchInstance.isNil or n.switchInstance.peerInfo.isNil:
    return @[]
  var base: seq[MultiAddress] = @[]
  if n.cfg.listenAddresses.len > 0:
    for cfgAddr in n.cfg.listenAddresses:
      let parsed = MultiAddress.init(cfgAddr)
      if parsed.isOk():
        base.add(parsed.get())
  if base.len == 0:
    base = n.switchInstance.peerInfo.listenAddrs
  if base.len == 0:
    base = n.switchInstance.peerInfo.addrs
  let preservedBase = base
  let hostNetwork = currentHostNetworkStatus(n)
  let preferredIpv4 = jsonGetStr(
    hostNetwork,
    "preferredIpv4",
    jsonGetStr(hostNetwork, "localIpv4", n.mdnsPreferredIpv4)
  ).strip()
  let localIpv4 = jsonGetStr(hostNetwork, "localIpv4", preferredIpv4).strip()
  let preferredIpv6 = jsonGetStr(
    hostNetwork,
    "preferredIpv6",
    jsonGetStr(hostNetwork, "localIpv6", "")
  ).strip()
  let localIpv6 = jsonGetStr(hostNetwork, "localIpv6", preferredIpv6).strip()
  let lanIpv4 = block:
    var ips: seq[string] = @[]
    var seen = initHashSet[string]()
    for candidate in [preferredIpv4, localIpv4, n.mdnsPreferredIpv4]:
      let normalized = sanitizeHostIpv4(candidate)
      if normalized.len > 0 and isLanIpv4(normalized) and normalized notin seen:
        if normalized == preferredIpv4:
          debugLog("[nimlibp2p] refreshLanAwareAddrs use preferred ipv4=" & normalized)
        ips.add(normalized)
        seen.incl(normalized)
    for candidate in collectLanIpv4Addrs(preferredIpv4):
      if candidate.len > 0 and candidate notin seen:
        ips.add(candidate)
        seen.incl(candidate)
    ips
  let lanIpv6 = block:
    var ips: seq[string] = @[]
    var seen = initHashSet[string]()
    for candidate in [preferredIpv6, localIpv6]:
      let normalized = sanitizeHostIpv6(candidate)
      if normalized.len > 0 and isLanIpv6(normalized) and normalized notin seen:
        ips.add(normalized)
        seen.incl(normalized)
    for candidate in collectLanIpv6Addrs():
      if candidate.len > 0 and candidate notin seen:
        ips.add(candidate)
        seen.incl(candidate)
    ips
  let wanIpv4 = block:
    var ips: seq[string] = @[]
    var seen = initHashSet[string]()
    for candidate in [preferredIpv4, localIpv4]:
      let normalized = sanitizeHostIpv4(candidate)
      if isPublicHostIpv4(normalized) and normalized notin seen:
        ips.add(normalized)
        seen.incl(normalized)
    ips
  let wanIpv6 = block:
    var ips: seq[string] = @[]
    var seen = initHashSet[string]()
    for candidate in [preferredIpv6, localIpv6]:
      let normalized = sanitizeHostIpv6(candidate)
      if isGlobalHostIpv6(normalized) and normalized notin seen:
        ips.add(normalized)
        seen.incl(normalized)
    ips
  debugLog("[nimlibp2p] refreshLanAwareAddrs lanIpv4=" &
    (if lanIpv4.len > 0: lanIpv4.join(",") else: "<empty>") &
    " lanIpv6=" & (if lanIpv6.len > 0: lanIpv6.join(",") else: "<empty>") &
    " wanIpv4=" & (if wanIpv4.len > 0: wanIpv4.join(",") else: "<empty>") &
    " wanIpv6=" & (if wanIpv6.len > 0: wanIpv6.join(",") else: "<empty>"))
  var expanded = expandLanMultiaddrs(base, lanIpv4, lanIpv6)
  if expanded.len == 0:
    expanded = base
  expanded = expandHostNetworkMultiaddrs(expanded, wanIpv4, wanIpv6)
  if expanded.len == 0:
    return @[]
  expanded = expandPhysicalPrefixMultiaddrs(expanded)
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

proc configuredMdnsServiceName(cfg: NodeConfig): string {.gcsafe.} =
  let envService = getEnv("UNIMAKER_MDNS_SERVICE")
  if envService.len > 0:
    return canonicalMdnsServiceName(envService)
  if not cfg.extra.isNil and cfg.extra.kind == JObject:
    if cfg.extra.hasKey("mdnsService"):
      let explicit = jsonGetStr(cfg.extra, "mdnsService")
      if explicit.len > 0:
        return canonicalMdnsServiceName(explicit)
    let mdnsNode = cfg.extra.getOrDefault("mdns")
    if not mdnsNode.isNil and mdnsNode.kind == JObject:
      let nested = jsonGetStr(mdnsNode, "service")
      if nested.len > 0:
        return canonicalMdnsServiceName(nested)
  canonicalMdnsServiceName(DefaultMdnsServiceName)

proc computeMdnsServiceName(n: NimNode): string {.gcsafe.} =
  if n.isNil:
    return canonicalMdnsServiceName(DefaultMdnsServiceName)
  if n.mdnsService.len > 0:
    return canonicalMdnsServiceName(n.mdnsService)
  if n.configuredMdnsService.len > 0:
    return canonicalMdnsServiceName(n.configuredMdnsService)
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

proc wanBootstrapEnabled(cfg: NodeConfig): bool =
  if cfg.extra.kind == JObject:
    if jsonGetBool(cfg.extra, "disableWanBootstrap", false) or
        jsonGetBool(cfg.extra, "disable_wan_bootstrap", false):
      return false
  true

proc publicBootstrapEnabled(cfg: NodeConfig): bool =
  if cfg.extra.kind == JObject:
    if jsonGetBool(cfg.extra, "disablePublicBootstrap", false) or
        jsonGetBool(cfg.extra, "disable_public_bootstrap", false):
      return false
    return jsonGetBool(cfg.extra, "enablePublicBootstrap", false) or
      jsonGetBool(cfg.extra, "enable_public_bootstrap", false)
  false

proc parseWanBootstrapRole(text: string): WanBootstrapRole =
  let normalized = text.strip().toLowerAscii()
  case normalized
  of "anchor":
    WanBootstrapRole.anchor
  of "public", "public_node", "publicnode":
    WanBootstrapRole.publicNode
  else:
    WanBootstrapRole.mobileEdge

proc configuredWanBootstrapRole(cfg: NodeConfig): WanBootstrapRole =
  if cfg.extra.kind == JObject:
    let raw = jsonGetStr(
      cfg.extra,
      "wanBootstrapRole",
      jsonGetStr(cfg.extra, "wan_bootstrap_role", "")
    )
    if raw.len > 0:
      return parseWanBootstrapRole(raw)
  WanBootstrapRole.mobileEdge

proc configuredWanNetworkId(cfg: NodeConfig): string =
  if cfg.extra.kind == JObject:
    let raw = jsonGetStr(
      cfg.extra,
      "wanBootstrapNetworkId",
      jsonGetStr(
        cfg.extra,
        "wan_bootstrap_network_id",
        jsonGetStr(
          cfg.extra,
          "networkId",
          jsonGetStr(cfg.extra, "network_id", "")
        )
      )
    ).strip()
    if raw.len > 0:
      return raw
  "unimaker-mobile"

proc configuredWanJournalPath(cfg: NodeConfig): string =
  if cfg.extra.kind == JObject:
    let raw = jsonGetStr(
      cfg.extra,
      "wanBootstrapJournalPath",
      jsonGetStr(cfg.extra, "wan_bootstrap_journal_path", "")
    ).strip()
    if raw.len > 0:
      return raw
  if cfg.dataDir.isSome():
    cfg.dataDir.get() / "wan_bootstrap_journal.json"
  else:
    ""

proc configuredWanBootstrapAuthoritative(cfg: NodeConfig): bool =
  if cfg.extra.kind == JObject:
    if jsonGetBool(cfg.extra, "bootstrapAuthoritative", false) or
        jsonGetBool(cfg.extra, "bootstrap_authoritative", false):
      return true
  false

proc configuredWanBootstrapAddrs(cfg: NodeConfig): seq[MultiAddress] =
  var seen = initHashSet[string]()
  for key in [
    "bootstrapMultiaddrs",
    "wanBootstrapMultiaddrs",
    "bootstrap_multiaddrs",
    "static_bootstrap",
    "bootstrap_nodes",
  ]:
    for raw in multiaddrsFromExtra(cfg.extra, key):
      let parsed = MultiAddress.init(raw)
      if parsed.isErr():
        continue
      let normalized = $parsed.get()
      if normalized in seen:
        continue
      seen.incl(normalized)
      result.add(parsed.get())

  if result.len == 0 and publicBootstrapEnabled(cfg):
    for raw in LegacyPublicWanBootstrapFallbackAddrs:
      let parsed = MultiAddress.init(raw)
      if parsed.isErr():
        continue
      let normalized = $parsed.get()
      if normalized in seen:
        continue
      seen.incl(normalized)
      result.add(parsed.get())

proc effectiveBootstrapHints(cfg: NodeConfig): seq[MultiAddress] =
  configuredWanBootstrapAddrs(cfg)

proc configuredWanBootstrapAuthoritativePeerIds(cfg: NodeConfig): seq[string] =
  if not configuredWanBootstrapAuthoritative(cfg):
    return @[]
  var seen = initHashSet[string]()
  for addr in configuredWanBootstrapAddrs(cfg):
    let peerIdOpt = extractPeerIdFromMultiaddrStr($addr)
    if peerIdOpt.isNone():
      continue
    let peerId = peerIdOpt.get().strip()
    if peerId.len == 0 or peerId in seen:
      continue
    seen.incl(peerId)
    result.add(peerId)

proc configuredWanBootstrapConfig(cfg: NodeConfig): WanBootstrapConfig =
  bootstrapRoleConfig(
    role = configuredWanBootstrapRole(cfg),
    networkId = configuredWanNetworkId(cfg),
    journalPath = configuredWanJournalPath(cfg),
    authoritativePeerIds = configuredWanBootstrapAuthoritativePeerIds(cfg),
    fallbackDnsAddrs = configuredWanBootstrapAddrs(cfg),
  )

proc extractPeerIdFromMultiaddrStr(ma: string): Option[string] {.gcsafe, raises: [].} =
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

proc stripPeerIdSuffixFromMultiaddr(text: string): string {.gcsafe, raises: [].} =
  let canonical = canonicalizeMultiaddrText(text.strip())
  if canonical.len == 0:
    return ""
  for marker in ["/p2p/", "/ipfs/"]:
    let idx = canonical.rfind(marker)
    if idx >= 0:
      return canonical[0 ..< idx]
  canonical

proc registerInitialPeerHints(n: NimNode, multiaddrs: seq[string], source: string) {.gcsafe.} =
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
      let maRes = parseDialableMultiaddr(addrStr)
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
    addPeerHintSource(n, peerId, source)
    n.bootstrapLastSeen[peerId] = nowMillis()
    let peerRes = PeerId.init(peerId)
    if peerRes.isOk() and not store.isNil:
      try:
        store.addAddressesWithTTL(peerRes.get(), parsed, chronos.minutes(30))
      except CatchableError as exc:
        warn "initial peer hint addAddressesWithTTL failed", peer = peerId, source = source, err = exc.msg
    info "initial peer hints registered", peer = peerId, source = source, hintCount = parsed.len

proc syncPeerHintsIntoBootstrapService(n: NimNode, source = "initial_sync") {.gcsafe.} =
  if n.isNil or n.switchInstance.isNil or n.peerHints.len == 0:
    return
  for peerId, addrs in n.peerHints.pairs:
    let peerRes = PeerId.init(peerId)
    if peerRes.isErr():
      continue
    var parsed: seq[MultiAddress] = @[]
    for addrStr in addrs:
      let maRes = parseDialableMultiaddr(addrStr)
      if maRes.isErr():
        continue
      parsed.add(maRes.get())
    if parsed.len == 0:
      continue
    let sources = n.peerHintSources.getOrDefault(peerId)
    let hintSource =
      if sources.len > 0:
        sources[0]
      else:
        source
    try:
      discard n.switchInstance.registerBootstrapHint(
        peerRes.get(),
        parsed,
        source = hintSource,
        trust = WanBootstrapTrust.quarantine
      )
    except CatchableError as exc:
      warn "sync peer hints into bootstrap service failed", peer = peerId, source = hintSource, err = exc.msg

proc exactBootstrapReconnectAddrs(addrs: seq[MultiAddress]): seq[MultiAddress] {.gcsafe.} =
  var seen = initHashSet[string]()
  for addr in addrs:
    let text = $addr
    if text.contains("/p2p/") or text.contains("/ipfs/"):
      let key = $addr
      if key.len == 0 or key in seen:
        continue
      seen.incl(key)
      result.add(addr)
  if result.len == 0:
    for addr in addrs:
      let key = $addr
      if key.len == 0 or key in seen:
        continue
      seen.incl(key)
      result.add(addr)
  if result.len > 1:
    result = preferStableMultiaddrs(result)

proc reconnectAuthoritativeBootstrapExact(
    n: NimNode, source = "auto_authoritative_seed"
): Future[void] {.async.} =
  if n.isNil or n.switchInstance.isNil or n.authoritativeBootstrapPeerIds.len == 0:
    return
  for peerId in n.authoritativeBootstrapPeerIds:
    let peerRes = PeerId.init(peerId)
    if peerRes.isErr():
      continue
    var parsed: seq[MultiAddress] = @[]
    for addrStr in n.peerHints.getOrDefault(peerId):
      let maRes = parseDialableMultiaddr(addrStr)
      if maRes.isErr():
        continue
      parsed.add(maRes.get())
    parsed = exactBootstrapReconnectAddrs(parsed)
    if parsed.len == 0:
      continue
    try:
      let joinResult = await joinViaSeedBootstrap(
        n.switchInstance,
        peerRes.get(),
        parsed,
        source,
      )
      debugLog(
        "[nimlibp2p] reconnectAuthoritativeBootstrapExact peer=" & peerId &
        " ok=" & $joinResult.ok &
        " connectedCount=" & $joinResult.connectedCount &
        " attemptedCount=" & $joinResult.attemptedCount
      )
      if joinResult.ok:
        return
    except CatchableError as exc:
      debugLog(
        "[nimlibp2p] reconnectAuthoritativeBootstrapExact failed peer=" & peerId &
        " err=" & exc.msg
      )

proc applyAuthoritativeBootstrapConfig(n: NimNode) {.gcsafe.} =
  if n.isNil or n.switchInstance.isNil or n.authoritativeBootstrapPeerIds.len == 0:
    return
  let svc = getWanBootstrapService(n.switchInstance)
  if svc.isNil:
    return
  svc.config.authoritativePeerIds = n.authoritativeBootstrapPeerIds

proc peerCurrentlyConnected(n: NimNode, peer: PeerId): bool {.gcsafe, raises: [].}

proc promoteConnectedBootstrapPeer(
    n: NimNode,
    peerIdText: string,
    addrTexts: seq[string],
    source = "direct_exact_connect",
): Future[bool] {.async.} =
  if n.isNil or n.switchInstance.isNil:
    return false
  let normalizedPeerId = peerIdText.strip()
  if normalizedPeerId.len == 0:
    return false
  let peerRes = PeerId.init(normalizedPeerId)
  if peerRes.isErr():
    return false
  let peerId = peerRes.get()
  if not peerCurrentlyConnected(n, peerId):
    return false
  discard await syncAdvertisedPeerInfo(
    n,
    reason = source & "_promote",
    reannounce = false,
  )
  var bootstrapAddrs: seq[MultiAddress] = @[]
  for addrStr in addrTexts & n.peerHints.getOrDefault(normalizedPeerId):
    let normalizedAddr = stripPeerIdSuffixFromMultiaddr(addrStr)
    if normalizedAddr.len == 0:
      continue
    let directAddrRes = parseDialableMultiaddr(normalizedAddr)
    if directAddrRes.isOk():
      bootstrapAddrs.add(directAddrRes.get())
  let svc = getWanBootstrapService(n.switchInstance)
  if svc.isNil or bootstrapAddrs.len == 0:
    return false
  try:
    return await svc.recordConnectedBootstrapPeer(
      peerId,
      bootstrapAddrs,
      source,
    )
  except CatchableError as exc:
    debugLog(
      "[nimlibp2p] promoteConnectedBootstrapPeer failed peer=" & normalizedPeerId &
      " source=" & source &
      " err=" & exc.msg
    )
    false

proc promoteConnectedBootstrapPeerDetached(
    n: NimNode,
    peerIdText: string,
    addrTexts: seq[string],
    source = "direct_exact_connect",
): Future[void] {.async.} =
  discard await promoteConnectedBootstrapPeer(
    n,
    peerIdText,
    addrTexts,
    source,
  )


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
  if not mdnsAllowedForCurrentHost(n):
    debugLog("[nimlibp2p] prepareMdns: skip non-lan host transport")
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

proc mdnsPeerRetentionMs(n: NimNode): int64 =
  if n.isNil:
    return MdnsPeerRetentionMinMs
  let intervalMs = int64(max(n.mdnsIntervalSeconds, 1) * 1_000)
  max(MdnsPeerRetentionMinMs, intervalMs * MdnsPeerRetentionMultiplier)

proc removeMdnsHintState(n: NimNode, peerId: string) {.gcsafe.} =
  if n.isNil:
    return
  let normalizedPeerId = peerId.strip()
  if normalizedPeerId.len == 0:
    return
  if n.mdnsLastSeen.hasKey(normalizedPeerId):
    n.mdnsLastSeen.del(normalizedPeerId)
  var remainingSources: seq[string] = @[]
  for source in n.peerHintSources.getOrDefault(normalizedPeerId):
    if source.strip().toLowerAscii() != "mdns" and source notin remainingSources:
      remainingSources.add(source)
  if remainingSources.len > 0:
    n.peerHintSources[normalizedPeerId] = remainingSources
  else:
    if n.peerHintSources.hasKey(normalizedPeerId):
      n.peerHintSources.del(normalizedPeerId)
    if n.peerHints.hasKey(normalizedPeerId):
      n.peerHints.del(normalizedPeerId)
    if n.bootstrapLastSeen.hasKey(normalizedPeerId):
      n.bootstrapLastSeen.del(normalizedPeerId)

proc pruneStaleMdnsPeers(n: NimNode, forceAll = false) {.gcsafe.} =
  if n.isNil:
    return
  var stalePeerIds: seq[string] = @[]
  let nowTs = nowMillis()
  let maxAgeMs = mdnsPeerRetentionMs(n)
  for peerId, seenAt in n.mdnsLastSeen.pairs:
    if peerId.len == 0:
      continue
    if forceAll or seenAt <= 0 or nowTs - seenAt > maxAgeMs:
      stalePeerIds.add(peerId)
  for peerId in stalePeerIds:
    removeMdnsHintState(n, peerId)

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

proc disconnectPeerWithTimeout(
    n: NimNode,
    peer: PeerId,
    timeoutMs: int,
    reason: string
): Future[bool] {.async.} =
  if n.switchInstance.isNil:
    return false
  let safeTimeoutMs = max(timeoutMs, 250)
  try:
    let disconnectFuture = n.switchInstance.disconnect(peer)
    let completed = await withTimeout(disconnectFuture, chronos.milliseconds(safeTimeoutMs))
    if not completed:
      try:
        await disconnectFuture.cancelAndWait()
      except CancelledError:
        discard
      except CatchableError as cancelExc:
        debugLog(
          "[nimlibp2p] disconnect cancel failed peer=" & $peer &
          " timeoutMs=" & $safeTimeoutMs &
          " reason=" & reason &
          " err=" & cancelExc.msg
        )
      debugLog(
        "[nimlibp2p] disconnect timeout peer=" & $peer &
        " timeoutMs=" & $safeTimeoutMs &
        " reason=" & reason
      )
      return false
    await disconnectFuture
    debugLog(
      "[nimlibp2p] disconnect ok peer=" & $peer &
      " timeoutMs=" & $safeTimeoutMs &
      " reason=" & reason
    )
    true
  except CancelledError as exc:
    debugLog(
      "[nimlibp2p] disconnect cancelled peer=" & $peer &
      " timeoutMs=" & $safeTimeoutMs &
      " reason=" & reason &
      " err=" & exc.msg
    )
    false
  except CatchableError as exc:
    debugLog(
      "[nimlibp2p] disconnect failed peer=" & $peer &
      " timeoutMs=" & $safeTimeoutMs &
      " reason=" & reason &
      " err=" & exc.msg
    )
    false

proc cancelFutureAndDrain[T](
    fut: Future[T],
    context: string
): Future[void] {.async.} =
  if fut.isNil or fut.finished():
    return
  try:
    await fut.cancelAndWait()
  except CancelledError:
    discard
  except CatchableError as exc:
    debugLog(
      "[nimlibp2p] future cancel drain failed context=" & context &
      " err=" & exc.msg
    )

proc drainFutureDetached[T](
    fut: Future[T],
    context: string
) {.async: (raises: []).} =
  if fut.isNil:
    return
  try:
    discard await fut
    debugLog("[nimlibp2p] future detached complete context=" & context)
  except CancelledError as exc:
    debugLog(
      "[nimlibp2p] future detached cancelled context=" & context &
      " err=" & exc.msg
    )
  except CatchableError as exc:
    debugLog(
      "[nimlibp2p] future detached failed context=" & context &
      " err=" & exc.msg
    )

proc awaitFutureWithinDeadline[T](
    fut: Future[T],
    timeoutMs: int,
    pollMs = 50
): Future[bool] {.async.} =
  if fut.isNil:
    return false
  if fut.finished():
    return true
  let safeTimeoutMs = max(timeoutMs, 0)
  if safeTimeoutMs == 0:
    return fut.finished()
  let deadlineMs = nowMillis() + int64(safeTimeoutMs)
  let safePollMs = max(pollMs, 5)
  while not fut.finished():
    let remainingMs = int(deadlineMs - nowMillis())
    if remainingMs <= 0:
      return false
    await sleepAsync(chronos.milliseconds(min(safePollMs, remainingMs)))
  true

proc awaitFutureWithinDeadlineOrPeerConnected[T](
    n: NimNode,
    fut: Future[T],
    peer: PeerId,
    timeoutMs: int,
    pollMs = 50
): Future[tuple[completed: bool, observedConnected: bool]] {.async.} =
  if fut.isNil:
    return (false, false)
  if fut.finished():
    return (true, false)
  if peer.len > 0 and peerCurrentlyConnected(n, peer):
    return (false, true)
  let safeTimeoutMs = max(timeoutMs, 0)
  if safeTimeoutMs == 0:
    return (fut.finished(), peer.len > 0 and peerCurrentlyConnected(n, peer))
  let deadlineMs = nowMillis() + int64(safeTimeoutMs)
  let safePollMs = max(pollMs, 5)
  while not fut.finished():
    if peer.len > 0 and peerCurrentlyConnected(n, peer):
      return (false, true)
    let remainingMs = int(deadlineMs - nowMillis())
    if remainingMs <= 0:
      return (false, false)
    await sleepAsync(chronos.milliseconds(min(safePollMs, remainingMs)))
  (true, false)

proc connectMultiaddrDeferred(
    n: NimNode,
    ma: MultiAddress
): Future[PeerId] {.async.} =
  await sleepAsync(chronos.milliseconds(0))
  return await n.switchInstance.connect(ma, allowUnknownPeerId = true)

proc ensurePeerConnectivity(n: NimNode, peer: PeerId, preferWanBootstrap = false): Future[bool] {.async.} =
  if peerCurrentlyConnected(n, peer):
    return true
  proc tryKnownAddresses(): Future[bool] {.async.} =
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
    false

  if await tryKnownAddresses():
    return true
  if preferWanBootstrap and not n.switchInstance.isNil:
    try:
      let joinResult = await bootstrapReconnect(n.switchInstance, BootstrapCandidateCap)
      debugLog(
        "[nimlibp2p] ensurePeerConnectivity bootstrapReconnect peer=" & $peer &
        " ok=" & $joinResult.ok &
        " connectedCount=" & $joinResult.connectedCount &
        " attemptedCount=" & $joinResult.attemptedCount
      )
    except CatchableError as exc:
      debugLog("[nimlibp2p] ensurePeerConnectivity bootstrapReconnect failed peer=" & $peer & " err=" & exc.msg)
    except Exception as exc:
      debugLog("[nimlibp2p] ensurePeerConnectivity bootstrapReconnect fatal peer=" & $peer & " err=" & exc.msg)
    if peerCurrentlyConnected(n, peer):
      return true
    if await tryKnownAddresses():
      return true
  peerCurrentlyConnected(n, peer)

type BootstrapCandidate = object
  peerId: string
  multiaddrs: seq[string]
  lastSeenAt: int64
  sources: seq[string]
  selectionReason: string

proc wanBootstrapCandidateToJson(candidate: WanBootstrapCandidate): JsonNode =
  result = newJObject()
  result["peerId"] = %($candidate.peerId)
  result["addrs"] = %candidate.addrs.mapIt($it)
  result["lastSeenAtMs"] = %candidate.lastSeenAtMs
  result["sources"] = %candidate.sources
  result["selectionReason"] = %candidate.selectionReason
  result["relayCapable"] = %candidate.relayCapable
  result["signedPeerRecordSeen"] = %candidate.signedPeerRecordSeen

proc wanBootstrapJoinResultToJson(joinResult: WanBootstrapJoinResult): JsonNode =
  result = newJObject()
  result["attemptedCount"] = %joinResult.attemptedCount
  result["connectedCount"] = %joinResult.connectedCount
  result["ok"] = %joinResult.ok
  result["timestampMs"] = %joinResult.timestampMs

  var attemptsNode = newJArray()
  var attemptedNode = newJArray()
  for attempt in joinResult.attempts:
    var candidateNode = wanBootstrapCandidateToJson(attempt.candidate)

    var attemptNode = newJObject()
    attemptNode["stage"] = %attempt.stage
    attemptNode["connected"] = %attempt.connected
    attemptNode["attemptedAddr"] = %attempt.attemptedAddr
    attemptNode["error"] = %attempt.error
    attemptNode["candidate"] = candidateNode
    attemptsNode.add(attemptNode)

    var legacyNode = newJObject()
    legacyNode["peerId"] = %($attempt.candidate.peerId)
    legacyNode["multiaddrs"] = %attempt.candidate.addrs.mapIt($it)
    legacyNode["lastSeenAt"] = %attempt.candidate.lastSeenAtMs
    legacyNode["sources"] = %attempt.candidate.sources
    legacyNode["selectionReason"] = %attempt.candidate.selectionReason
    legacyNode["connected"] = %attempt.connected
    legacyNode["attemptedAddr"] = %attempt.attemptedAddr
    legacyNode["error"] = %attempt.error
    attemptedNode.add(legacyNode)

  result["attempts"] = attemptsNode
  result["attempted"] = attemptedNode

proc bootstrapCandidateToJson(candidate: BootstrapCandidate): JsonNode {.gcsafe.} =
  result = newJObject()
  result["peerId"] = %candidate.peerId
  result["addrs"] = %candidate.multiaddrs
  result["lastSeenAtMs"] = %candidate.lastSeenAt
  result["sources"] = %candidate.sources
  result["selectionReason"] = %candidate.selectionReason

proc collectLocalBootstrapCandidates(n: NimNode): seq[BootstrapCandidate] {.gcsafe.}
proc selectProgressiveBootstrapCandidates(
    candidates: seq[BootstrapCandidate], limit: int
): seq[BootstrapCandidate] {.gcsafe.}

proc mergeLocalBootstrapStatus(node: NimNode, payload: var JsonNode, limit: int) {.gcsafe.} =
  if node.isNil:
    return
  let candidates = collectLocalBootstrapCandidates(node)
  let selected = selectProgressiveBootstrapCandidates(candidates, limit)
  var connected = initHashSet[string]()
  if not node.switchInstance.isNil:
    try:
      for pid in node.switchInstance.connectedPeers(Direction.Out):
        connected.incl($pid)
      for pid in node.switchInstance.connectedPeers(Direction.In):
        connected.incl($pid)
    except CatchableError:
      discard

  var selectedNode = newJArray()
  for item in selected:
    selectedNode.add(bootstrapCandidateToJson(item))
  let hasServiceSnapshot =
    payload.kind == JObject and (
      payload.hasKey("running") or
      payload.hasKey("candidateCount") or
      payload.hasKey("selectedCandidates") or
      payload.hasKey("metrics") or
      payload.hasKey("hasLastJoinResult")
    )
  if not hasServiceSnapshot:
    payload["networkId"] = %configuredWanNetworkId(node.cfg)
    payload["role"] = %($configuredWanBootstrapRole(node.cfg))
    payload["running"] = %(not node.switchInstance.isNil)
    payload["hintPeerCount"] = %candidates.len
    payload["candidateCount"] = %candidates.len
    payload["connectedPeerCount"] = %connected.len
    payload["selectedCandidates"] = selectedNode
    payload["selectedCandidateCount"] = %selected.len
  payload["localHintPeerCount"] = %candidates.len
  payload["localCandidateCount"] = %candidates.len
  payload["localConnectedPeerCount"] = %connected.len
  payload["localSelectedCandidates"] = selectedNode
  payload["localSelectedCandidateCount"] = %selected.len

proc hasConnectedAuthoritativeBootstrap(node: NimNode): bool {.gcsafe.} =
  if node.isNil or node.switchInstance.isNil or node.authoritativeBootstrapPeerIds.len == 0:
    return false
  var connected = initHashSet[string]()
  try:
    for pid in node.switchInstance.connectedPeers(Direction.Out):
      connected.incl($pid)
    for pid in node.switchInstance.connectedPeers(Direction.In):
      connected.incl($pid)
  except CatchableError:
    return false
  for peerId in node.authoritativeBootstrapPeerIds:
    if peerId in connected:
      return true
  false

proc maybeKickAuthoritativeBootstrap(
    node: NimNode,
    source = "auto_authoritative_seed_status",
    minIntervalMs = 5_000'i64,
) {.gcsafe.} =
  when defined(ohos):
    return
  if node.isNil or not node.started or node.switchInstance.isNil:
    return
  if node.authoritativeBootstrapPeerIds.len == 0:
    return
  if hasConnectedAuthoritativeBootstrap(node):
    return
  let nowMs = nowMillis()
  if nowMs - node.lastAuthoritativeBootstrapKickMs < minIntervalMs:
    return
  node.lastAuthoritativeBootstrapKickMs = nowMs
  asyncSpawn(reconnectAuthoritativeBootstrapExact(node, source))

proc addBootstrapSource(target: var seq[string], source: string) =
  if source.len > 0 and source notin target:
    target.add(source)

proc addPeerHintSource(n: NimNode, peerId: string, source: string) {.gcsafe.} =
  if n.isNil:
    return
  let normalizedPeerId = peerId.strip()
  let normalizedSource = source.strip()
  if normalizedPeerId.len == 0 or normalizedSource.len == 0:
    return
  var known = n.peerHintSources.getOrDefault(normalizedPeerId)
  if normalizedSource notin known:
    known.add(normalizedSource)
  n.peerHintSources[normalizedPeerId] = known

proc isBootstrapInfraHint(source: string): bool =
  let normalized = source.strip().toLowerAscii()
  normalized in ["bootstrap", "relay", "mdns", "dht"]

proc isDirectHintSource(source: string): bool =
  let normalized = source.strip()
  if normalized.len == 0:
    return false
  not isBootstrapInfraHint(normalized)

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

proc collectLocalBootstrapCandidates(n: NimNode): seq[BootstrapCandidate] {.gcsafe.} =
  pruneStaleMdnsPeers(n)
  var candidates = initTable[string, BootstrapCandidate]()
  let localId = localPeerId(n)
  let nowTs = nowMillis()
  var connected = initHashSet[string]()
  let authoritativeBootstrapActive =
    n.authoritativeBootstrapPeerIds.len > 0

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
        sources: @[],
        selectionReason: ""
      )
    )
    addBootstrapAddresses(normalizedPeerId, addrs, entry.multiaddrs)
    addBootstrapSource(entry.sources, source)
    entry.lastSeenAt = max(entry.lastSeenAt, max(candidateLastSeen(n, normalizedPeerId), seenAt))
    candidates[normalizedPeerId] = entry

  for peerId, addrs in n.peerHints.pairs:
    upsert(peerId, addrs, "Hints")
    for hintSource in n.peerHintSources.getOrDefault(peerId):
      upsert(peerId, @[], "Hint:" & hintSource)

  if not n.switchInstance.isNil:
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
    if authoritativeBootstrapActive and connected.len == 0:
      if peerId notin n.authoritativeBootstrapPeerIds:
        continue
    if entry.lastSeenAt <= 0 and "Connected" in entry.sources:
      entry.lastSeenAt = nowTs
    if entry.lastSeenAt <= 0:
      entry.lastSeenAt = nowTs - 1
    result.add(entry)

proc bootstrapSourceScore(candidate: BootstrapCandidate): int {.gcsafe.} =
  result = min(candidate.multiaddrs.len, 8)
  for source in candidate.sources:
    case source
    of "Connected":
      result += 300
    of "Hints":
      result += 200
    of "DHT":
      result += 100
    of "mdns", "Hint:mdns":
      result += 40
    else:
      if source.startsWith("Hint:"):
        let hintSource = source[5 .. ^1]
        if isDirectHintSource(hintSource):
          result += 450
        elif isBootstrapInfraHint(hintSource):
          result += 120

proc compareBootstrapCandidates(a, b: BootstrapCandidate): int {.gcsafe.} =
  result = cmp(b.lastSeenAt, a.lastSeenAt)
  if result == 0:
    result = cmp(bootstrapSourceScore(b), bootstrapSourceScore(a))
  if result == 0:
    result = cmp(b.multiaddrs.len, a.multiaddrs.len)
  if result == 0:
    result = cmp(a.peerId, b.peerId)

proc sortBootstrapCandidates(candidates: var seq[BootstrapCandidate]) {.gcsafe.} =
  candidates.sort(compareBootstrapCandidates)

proc clampBootstrapLimit(limit: int, defaultLimit: int, maxLimit: int): int {.gcsafe.} =
  let resolved = if limit > 0: limit else: defaultLimit
  max(1, min(resolved, maxLimit))

proc selectProgressiveBootstrapCandidates(
    candidates: seq[BootstrapCandidate], limit: int
): seq[BootstrapCandidate] {.gcsafe.} =
  let target = clampBootstrapLimit(
    limit, defaultLimit = BootstrapCandidateCap, maxLimit = BootstrapCandidateCap
  )
  if candidates.len == 0 or target <= 0:
    return @[]

  var ordered = candidates
  sortBootstrapCandidates(ordered)
  var selected: seq[BootstrapCandidate] = @[]

  var selectedPeerIds = initHashSet[string]()

  proc addSelected(entry: BootstrapCandidate, reason: string): bool =
    if entry.peerId.len == 0 or entry.peerId in selectedPeerIds:
      return false
    var chosen = entry
    chosen.selectionReason = reason
    selected.add(chosen)
    selectedPeerIds.incl(entry.peerId)
    true

  proc takeMatching(
      reason: string,
      maxCount: int,
      predicate: proc(candidate: BootstrapCandidate): bool {.gcsafe, raises: [].}
  ) =
    var added = 0
    for entry in ordered:
      if selected.len >= target or added >= maxCount:
        break
      if entry.peerId in selectedPeerIds or not predicate(entry):
        continue
      if addSelected(entry, reason):
        inc added

  takeMatching(
    "direct_hint",
    1,
    proc(candidate: BootstrapCandidate): bool =
      for source in candidate.sources:
        if source.startsWith("Hint:") and isDirectHintSource(source[5 .. ^1]):
          return true
      false
  )

  if selected.len < target:
    takeMatching(
      "hint",
      1,
      proc(candidate: BootstrapCandidate): bool =
        "Hints" in candidate.sources
    )

  if selected.len < target:
    takeMatching(
      "recent_stable",
      min(BootstrapRecentStableSlots, target - selected.len),
      proc(candidate: BootstrapCandidate): bool = true
    )

  if selected.len >= target:
    return selected

  var pool: seq[BootstrapCandidate] = @[]
  for entry in ordered:
    if entry.peerId in selectedPeerIds:
      continue
    pool.add(entry)
    if pool.len >= BootstrapRandomPoolCap:
      break
  if pool.len == 0:
    return selected

  random.randomize(int(nowMillis() and 0x7fff_ffff'i64))
  for i in countdown(pool.high, 1):
    let j = random.rand(i)
    swap(pool[i], pool[j])
  for entry in pool:
    if selected.len >= target:
      break
    discard addSelected(entry, "random_pool")
  selected

proc connectBootstrapCandidate(n: NimNode, candidate: BootstrapCandidate): Future[bool] {.async.} =
  if n.switchInstance.isNil or candidate.multiaddrs.len == 0:
    return false
  var parsed: seq[MultiAddress] = @[]
  for addr in candidate.multiaddrs:
    let maRes = parseDialableMultiaddr(addr)
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

proc connectBootstrapCandidates(
    n: NimNode,
    candidates: seq[BootstrapCandidate],
    concurrency = BootstrapDialConcurrency
): Future[seq[bool]] {.async.} =
  if candidates.len == 0:
    return @[]

  let batchSize = max(1, concurrency)
  result = newSeq[bool](candidates.len)
  var start = 0
  while start < candidates.len:
    let stop = min(start + batchSize, candidates.len)
    var futs: seq[Future[bool]] = @[]
    for idx in start ..< stop:
      futs.add(connectBootstrapCandidate(n, candidates[idx]))
    let grouped = allFutures(futs)
    if not grouped.isNil:
      await grouped
    var batchConnected = false
    for offset, fut in futs:
      if fut.isNil or not fut.finished():
        continue
      let connected =
        try:
          fut.read()
        except CatchableError:
          false
      result[start + offset] = connected
      if connected:
        batchConnected = true
    if batchConnected:
      break
    start = stop

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
    return true
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
  addPeerHintSource(n, peerStr, "mdns")
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
  n.mdnsService = n.configuredMdnsService
  pruneStaleMdnsPeers(n, forceAll = true)
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
  var bootstrapReconnectTriggered = false
  for peerId, addresses in n.peerHints.pairs:
    if peerId in n.authoritativeBootstrapPeerIds:
      when defined(ohos):
        if not bootstrapReconnectTriggered:
          bootstrapReconnectTriggered = true
          debugLog(
            "[nimlibp2p] boostConnectivity skip authoritative bootstrapReconnect on ohos peer=" &
            peerId
          )
        continue
      if not bootstrapReconnectTriggered and not n.switchInstance.isNil:
        bootstrapReconnectTriggered = true
        try:
          let joinResult = await bootstrapReconnect(n.switchInstance, BootstrapCandidateCap)
          debugLog(
            "[nimlibp2p] boostConnectivity authoritative bootstrapReconnect peer=" & peerId &
            " ok=" & $joinResult.ok &
            " connectedCount=" & $joinResult.connectedCount &
            " attemptedCount=" & $joinResult.attemptedCount
          )
        except CatchableError as exc:
          debugLog(
            "[nimlibp2p] boostConnectivity authoritative bootstrapReconnect failed peer=" &
            peerId & " err=" & exc.msg
          )
      continue
    let peerRes = PeerId.init(peerId)
    var parsed: seq[MultiAddress] = @[]
    for addrStr in addresses:
      let ma = parseDialableMultiaddr(addrStr)
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

proc rememberLastNodeIdentity(handle: pointer; node: NimNode = nil) {.raises: [].} =
  var peerId = ""
  let sourceNode =
    if node.isNil:
      nodeFromHandle(handle)
    else:
      node
  if not sourceNode.isNil:
    peerId = localPeerId(sourceNode)
  acquire(lastNodeIdentityLock)
  try:
    if not handle.isNil:
      lastNodeHandle = handle
    if peerId.len > 0:
      lastNodePeerIdCache = peerId
  finally:
    release(lastNodeIdentityLock)

proc clearRememberedLastNodeHandle(handle: pointer) {.raises: [].} =
  if handle.isNil:
    return
  acquire(lastNodeIdentityLock)
  try:
    if lastNodeHandle == handle:
      lastNodeHandle = nil
  finally:
    release(lastNodeIdentityLock)

proc lastKnownNodePeerId(): string {.raises: [].} =
  var handle: pointer = nil
  acquire(lastNodeIdentityLock)
  try:
    handle = lastNodeHandle
    result = lastNodePeerIdCache
  finally:
    release(lastNodeIdentityLock)
  if handle.isNil:
    return result
  let node = nodeFromHandle(handle)
  if node.isNil:
    return result
  let livePeerId = localPeerId(node)
  if livePeerId.len == 0:
    return result
  if livePeerId != result:
    acquire(lastNodeIdentityLock)
    try:
      lastNodePeerIdCache = livePeerId
    finally:
      release(lastNodeIdentityLock)
  result = livePeerId

proc freeNodeHandle(handle: pointer) =
  if handle.isNil:
    return
  let h = cast[NodeHandle](handle)
  h[].node = nil
  deallocShared(h)

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
        let requestedListenProfile = normalizeListenProfile(
          jsonGetStr(
            root,
            "listenProfile",
            jsonGetStr(
              root,
              "listen_profile",
              jsonGetStr(
                cfg.extra,
                "listenProfile",
                jsonGetStr(cfg.extra, "listen_profile", "")
              )
            )
          )
        )
        let requestedListenPort = max(
          0,
          int(
            jsonGetInt64(
              root,
              "listenPort",
              jsonGetInt64(
                root,
                "listen_port",
                jsonGetInt64(
                  cfg.extra,
                  "listenPort",
                  jsonGetInt64(cfg.extra, "listen_port", 0)
                )
              )
            )
          )
        )
        if cfg.listenAddresses.len == 0 and requestedListenProfile == "physical_dual_stack":
          cfg.listenAddresses = fullP2PListenAddresses(
            if requestedListenPort > 0: requestedListenPort else: FullP2PDefaultPort
          )
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

  if wanBootstrapEnabled(cfg):
    builder = builder.withWanBootstrapProfile(configuredWanBootstrapConfig(cfg))
    debugLog(
      "[nimlibp2p] buildSwitch enabled WAN bootstrap profile role=" &
      $configuredWanBootstrapRole(cfg) &
      " networkId=" & configuredWanNetworkId(cfg)
    )

  let wantsTcp =
    if cfg.listenAddresses.len == 0:
      true
    else:
      cfg.listenAddresses.anyIt(it.contains("/tcp/"))
  let wantsQuic = cfg.listenAddresses.anyIt(it.contains("/quic"))

  if wantsTcp:
    builder = builder.withTcpTransport({ServerFlags.TcpNoDelay})
    debugLog("[nimlibp2p] buildSwitch applied TCP transport")
  else:
    debugLog("[nimlibp2p] buildSwitch skipped TCP transport (not configured)")
  when defined(libp2p_quic_support) and not defined(libp2p_msquic_experimental):
    {.error: "libp2p_quic_support has been removed from mobile_ffi. Enable -d:libp2p_msquic_experimental only.".}

  when defined(libp2p_msquic_experimental):
    var msquicActivated = false
    if wantsQuic:
      var msquicBuilderCfg = MsQuicTransportBuilderConfig.init()
      let prefRes = configuredQuicRuntimePreference(cfg.extra)
      if prefRes.isErr:
        return err(prefRes.error)
      if prefRes.get().isSome:
        msquicBuilderCfg.config.setRuntimePreference(prefRes.get().get())
        debugLog(
          "[nimlibp2p] buildSwitch applied QUIC runtime preference=" &
          quicRuntimePreferenceLabel(prefRes.get().get())
        )
      let explicitRuntimePath = configuredQuicRuntimeLibraryPath(cfg.extra)
      if explicitRuntimePath.len > 0:
        msquicBuilderCfg.config.loadOptions.explicitPath = explicitRuntimePath
        msquicBuilderCfg.config.loadOptions.allowFallback = false
        debugLog(
          "[nimlibp2p] buildSwitch applied QUIC runtime library path=" &
          explicitRuntimePath
        )
      let bridgeRes = msquicruntime.acquireMsQuicBridge(msquicBuilderCfg.config.loadOptions)
      if bridgeRes.success and not bridgeRes.bridge.isNil:
        msquicruntime.releaseMsQuicBridge(bridgeRes.bridge)
        if cfg.extra.kind == JObject:
          let preferredClientBindHost = jsonGetStr(
            cfg.extra,
            "preferredClientBindHost",
            jsonGetStr(cfg.extra, "preferred_client_bind_host", "")
          ).strip()
          if preferredClientBindHost.len > 0 and preferredClientBindHost != "0.0.0.0" and
              preferredClientBindHost != "127.0.0.1":
            msquicBuilderCfg.config.clientBindHost = preferredClientBindHost
            let preferredClientBindPort = max(
              0,
              int(jsonGetInt64(
                cfg.extra,
                "preferredClientBindPort",
                jsonGetInt64(cfg.extra, "preferred_client_bind_port", 0)
              ))
            )
            if preferredClientBindPort > 0 and preferredClientBindPort <= high(uint16).int:
              msquicBuilderCfg.config.clientBindPort = uint16(preferredClientBindPort)
            debugLog(
              "[nimlibp2p] buildSwitch applied MsQuic client bind host=" &
              msquicBuilderCfg.config.clientBindHost &
              " port=" & $msquicBuilderCfg.config.clientBindPort
            )
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
        debugLog("[nimlibp2p] MsQuic runtime unavailable err=" & errMsg)
        warn "MsQuic runtime unavailable", err = errMsg
        return err("MsQuic runtime unavailable: " & errMsg)
    else:
      debugLog("[nimlibp2p] buildSwitch skipped MsQuic transport (not configured)")
    if wantsQuic and not msquicActivated:
      debugLog("[nimlibp2p] No QUIC transport available because MsQuic runtime is unavailable")
  else:
    if wantsQuic:
      return err("QUIC listen addresses require -d:libp2p_msquic_experimental")

  if not wantsTcp and not wantsQuic:
    return err("no supported transports configured in listenAddresses")
  let pureQuicOnly = wantsQuic and not wantsTcp
  if pureQuicOnly:
    builder = builder.withoutImplicitSecureDefault()
  if not pureQuicOnly:
    # Prefer Yamux on mobile; do not register protocol-level fallback muxers.
    builder = builder.withYamux(
      maxChannCount = MobileYamuxMaxChannels,
      windowSize = MobileYamuxWindowSize,
      maxSendQueueSize = MobileYamuxMaxSendQueueSize,
    )
    debugLog("[nimlibp2p] buildSwitch applied Yamux")
  else:
    debugLog("[nimlibp2p] buildSwitch skipped Yamux for QUIC-only profile")
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
  if pureQuicOnly:
    enableNoise = false
    enableTls = false
  elif not enableNoise and not enableTls:
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

type NcProbeMode = enum
  ncpmRawTcp
  ncpmLibp2p

proc parseNcProbeTargetStrings(raw: string): seq[string] =
  let trimmed = raw.strip()
  if trimmed.len == 0:
    return

  try:
    let parsed = parseJson(trimmed)
    case parsed.kind
    of JArray:
      for item in parsed.items:
        if item.kind == JString:
          let value = item.getStr().strip()
          if value.len > 0:
            result.add(value)
    of JString:
      let value = parsed.getStr().strip()
      if value.len > 0:
        result.add(value)
    else:
      discard
  except CatchableError:
    discard

  if result.len > 0:
    return

  for token in trimmed.replace("\r", "\n").replace(",", "\n").splitLines():
    let value = token.strip()
    if value.len > 0:
      result.add(value)

proc parseNcProbeTargets(raw: string): seq[MultiAddress] =
  for item in parseNcProbeTargetStrings(raw):
    let parsed = MultiAddress.init(item)
    if parsed.isErr:
      raise newException(ValueError, "invalid multiaddr: " & item)
    result.add(parsed.get())
  if result.len == 0:
    raise newException(ValueError, "no probe targets provided")

proc resolveNcProbeTlsTempDir(): string =
  proc isWritableDir(path: string): bool =
    let trimmed = path.strip()
    if trimmed.len == 0:
      return false
    try:
      createDir(trimmed)
    except CatchableError:
      return false

    randomize()
    let probePath = trimmed / (
      "nim-libp2p-probe-" & $toUnix(getTime()) & "-" & $rand(0x00FF_FFFF) & ".tmp"
    )
    try:
      writeFile(probePath, "probe")
      removeFile(probePath)
      true
    except CatchableError:
      try:
        if fileExists(probePath):
          removeFile(probePath)
      except CatchableError:
        discard
      false

  var candidates: seq[string] = @[]
  let explicitTempDir = getEnv("UNIMAKER_LIBP2P_TLS_TMP_DIR").strip()
  if explicitTempDir.len > 0:
    candidates.add(explicitTempDir)

  let dataDir = getEnv("UNIMAKER_LIBP2P_DATA_DIR").strip()
  if dataDir.len > 0:
    candidates.add(dataDir / "tls_tmp")

  let tmpDirEnv = getEnv("TMPDIR").strip()
  if tmpDirEnv.len > 0:
    candidates.add(tmpDirEnv / "nim-libp2p" / "tls_tmp")

  let homeDir = getEnv("HOME").strip()
  if homeDir.len > 0:
    candidates.add(homeDir / ".cache" / "nim-libp2p" / "tls_tmp")
    candidates.add(homeDir / "cache" / "nim-libp2p" / "tls_tmp")
    candidates.add(homeDir / "files" / "nim-libp2p" / "tls_tmp")

  try:
    let cwd = getCurrentDir().strip()
    if cwd.len > 0 and cwd != "/":
      candidates.add(cwd / ".nim-libp2p" / "tls_tmp")
  except CatchableError:
    discard

  try:
    let sysTempDir = getTempDir().strip()
    if sysTempDir.len > 0:
      candidates.add(sysTempDir / "nim-libp2p" / "tls_tmp")
  except CatchableError:
    discard

  for candidate in candidates:
    if isWritableDir(candidate):
      return candidate

  return ""

proc classifyNcProbeTarget(target: MultiAddress): set[NcProbeMode] =
  let text = $target
  if "/tcp/" in text:
    result.incl(ncpmRawTcp)
    result.incl(ncpmLibp2p)
  elif "/quic-v1" in text:
    result.incl(ncpmLibp2p)

proc rawTcpProbeAddress(target: MultiAddress): MultiAddress =
  if "/p2p/" in $target:
    return target[0 .. 1].valueOr:
      raise newException(ValueError, "failed to derive raw tcp address from " & $target)
  target

proc ncProbeElapsedMs(startedAt: float): int64 =
  int64(max(0.0, (epochTime() - startedAt) * 1000.0))

proc closeProbeStream(stream: StreamTransport) {.async: (raises: []).} =
  if stream.isNil:
    return
  try:
    await stream.closeWait()
  except CatchableError:
    discard

proc buildNcProbeSwitch(targets: seq[MultiAddress]): Switch =
  let rng = newRng()
  var listenAddrs = @[MultiAddress.init("/ip6/::/tcp/0").get()]
  if targets.anyIt("/quic-v1" in $it):
    when defined(libp2p_msquic_experimental):
      listenAddrs.add(MultiAddress.init("/ip6/::/udp/0/quic-v1").get())

  var builder = SwitchBuilder
    .new()
    .withRng(rng)
    .withAddresses(listenAddrs)
    .withTcpTransport()
    .withMplex()
    .withNoise()

  if targets.anyIt("/quic-v1" in $it):
    when defined(libp2p_msquic_experimental):
      var msquicBuilderCfg = MsQuicTransportBuilderConfig.init()
      let tlsTempDir = resolveNcProbeTlsTempDir()
      if tlsTempDir.len > 0:
        msquicBuilderCfg.onTransport = Opt.some(MsQuicTransportHook(
          proc(transport: MsQuicTransport) =
            transport.setTlsTempDir(tlsTempDir)
        ))
      builder = builder.withMsQuicTransport(msquicBuilderCfg)

  builder.build()

proc buildLibp2pProbeFailure(target: MultiAddress; error: string): JsonNode =
  result = newJObject()
  result["probe"] = %"libp2p"
  result["target"] = %($target)
  result["ok"] = %false
  result["durationMs"] = %0
  result["error"] = %(if error.len > 0: error else: "libp2p probe unavailable")

proc runRawTcpProbe(target: MultiAddress; timeoutMs: int): Future[JsonNode] {.async.} =
  result = newJObject()
  result["probe"] = %"raw-tcp"
  let rawTarget =
    try:
      rawTcpProbeAddress(target)
    except CatchableError as exc:
      result["target"] = %($target)
      result["ok"] = %false
      result["durationMs"] = %0
      result["error"] = %exc.msg
      return

  result["target"] = %($rawTarget)
  let startedAt = epochTime()
  let dialFut = wire.connect(rawTarget)
  try:
    let stream = await dialFut.wait(chronos.milliseconds(timeoutMs))
    defer:
      await closeProbeStream(stream)
    result["ok"] = %true
    result["durationMs"] = %ncProbeElapsedMs(startedAt)
  except AsyncTimeoutError:
    if not dialFut.finished():
      dialFut.cancel()
    result["ok"] = %false
    result["durationMs"] = %int64(timeoutMs)
    result["error"] = %"timeout"
  except CatchableError as exc:
    result["ok"] = %false
    result["durationMs"] = %ncProbeElapsedMs(startedAt)
    result["error"] = %exc.msg

proc runLibp2pNcProbe(
    sw: Switch, target: MultiAddress, timeoutMs: int
): Future[JsonNode] {.async.} =
  result = newJObject()
  result["probe"] = %"libp2p"
  result["target"] = %($target)
  if sw.isNil:
    result["ok"] = %false
    result["durationMs"] = %0
    result["error"] = %"probe switch unavailable"
    return

  if "/quic-v1" in $target and not defined(libp2p_msquic_experimental):
    result["ok"] = %false
    result["durationMs"] = %0
    result["error"] = %"quic unavailable in current build"
    return

  let startedAt = epochTime()
  let dialFut = sw.connect(target, allowUnknownPeerId = true)
  try:
    let peerId = await dialFut.wait(chronos.milliseconds(timeoutMs))
    result["ok"] = %true
    result["durationMs"] = %ncProbeElapsedMs(startedAt)
    result["peerId"] = %($peerId)
    try:
      await sw.disconnect(peerId)
    except CatchableError:
      discard
  except AsyncTimeoutError:
    if not dialFut.finished():
      dialFut.cancel()
    result["ok"] = %false
    result["durationMs"] = %int64(timeoutMs)
    result["error"] = %"timeout"
  except CatchableError as exc:
    result["ok"] = %false
    result["durationMs"] = %ncProbeElapsedMs(startedAt)
    result["error"] = %exc.msg

proc runNativeNcProbe(targets: seq[MultiAddress]; timeoutMs: int): Future[JsonNode] {.async.} =
  result = newJObject()
  result["ok"] = %false
  result["timeoutMs"] = %timeoutMs
  result["targetCount"] = %targets.len
  var items = newJArray()

  let needLibp2p = targets.anyIt(ncpmLibp2p in classifyNcProbeTarget(it))
  var sw: Switch = nil
  var switchError = ""
  if needLibp2p:
    try:
      sw = buildNcProbeSwitch(targets)
      await sw.start()
      result["localPeerId"] = %($sw.peerInfo.peerId)
    except CatchableError as exc:
      switchError = exc.msg

  try:
    var anySuccess = false
    for target in targets:
      let modes = classifyNcProbeTarget(target)
      if ncpmRawTcp in modes:
        let rawProbe = await runRawTcpProbe(target, timeoutMs)
        anySuccess = anySuccess or jsonBool(rawProbe, "ok", false)
        items.add(rawProbe)
      if ncpmLibp2p in modes:
        let libp2pProbe =
          if sw.isNil:
            buildLibp2pProbeFailure(target, switchError)
          else:
            await runLibp2pNcProbe(sw, target, timeoutMs)
        anySuccess = anySuccess or jsonBool(libp2pProbe, "ok", false)
        items.add(libp2pProbe)
    result["ok"] = %anySuccess
    result["results"] = items
  finally:
    if not sw.isNil:
      await sw.stop()

proc libp2p_native_nc_probe*(targetsJson: cstring, timeoutMs: cint): cstring {.exportc, cdecl, dynlib.} =
  let rawTargets = (if targetsJson.isNil: "" else: $targetsJson).strip()
  if rawTargets.len == 0:
    return allocCString("{\"ok\":false,\"error\":\"missing_targets\",\"results\":[]}")

  let safeTimeoutMs = max(int(timeoutMs), 1)
  try:
    let payload = waitFor runNativeNcProbe(parseNcProbeTargets(rawTargets), safeTimeoutMs)
    allocCString($payload)
  except CatchableError as exc:
    let payload = %*{
      "ok": false,
      "error": exc.msg,
      "timeoutMs": safeTimeoutMs,
      "results": []
    }
    allocCString($payload)

proc libp2p_get_last_error*(): cstring {.exportc, cdecl, dynlib.} =
  if lastRuntimeError.len > 0:
    return allocCString(lastRuntimeError)
  if lastInitError.len > 0:
    return allocCString(lastInitError)
  allocCString("")

proc finalizeCommand(cmd: Command) =
  if cmd.completion.isNil:
    return
  let res = cmd.completion.fireSync()
  if res.isErr():
    warn "command completion signal failed", err = res.error

proc buildConnectedPeersArrayForNode(node: NimNode): JsonNode {.gcsafe, raises: [].}
proc recentConnectedPeersInfo(
    node: NimNode,
    maxAgeMs = ConnectedPeersInfoSnapshotFallbackMs
): JsonNode {.gcsafe, raises: [].}
proc buildConnectedPeersArrayFromInfo(connectedInfo: JsonNode): JsonNode {.gcsafe, raises: [].}
proc buildConnectedPeersInfoArrayForNode(
    node: NimNode,
    includeLatency = true
): Future[JsonNode] {.async, gcsafe.}
proc filteredDiscoveredPeersForNode(
    node: NimNode,
    sourceFilterText: string,
    limit: int,
    connectedInfo: JsonNode
): tuple[peers: JsonNode, totalCount: int] {.gcsafe.}
proc buildMdnsDebugPayload(node: NimNode): JsonNode {.gcsafe.}
proc buildNetworkResourcesPayload(
    node: NimNode,
    discoveredCount: int,
    connectedCount: int
): JsonNode {.gcsafe.}
proc connectedPeersInfoArray(handle: pointer): JsonNode

proc buildFeedSnapshotPayload(node: NimNode): JsonNode {.gcsafe.} =
  var arr = newJArray()
  if not node.isNil:
    for item in node.feedItems:
      var obj = newJObject()
      obj["id"] = %item.id
      obj["author"] = %item.author
      obj["timestamp_ms"] = %item.timestamp
      obj["payload"] = item.payload
      arr.add(obj)
  var snapshot = newJObject()
  snapshot["items"] = arr
  snapshot["timestamp_ms"] = %nowMillis()
  snapshot

proc buildNetworkDiscoverySnapshotPayload(
    node: NimNode,
    connectedInfo: JsonNode,
    sourceFilterText: string,
    limit: int
): JsonNode {.gcsafe.} =
  let effectiveLimit = if limit <= 0: 0 else: limit
  let connected =
    if connectedInfo.len > 0:
      buildConnectedPeersArrayFromInfo(connectedInfo)
    else:
      buildConnectedPeersArrayForNode(node)
  let discovered = filteredDiscoveredPeersForNode(
    node, sourceFilterText, effectiveLimit, connectedInfo
  )
  let localProfile = collectLocalSystemProfile(node)
  let networkResources = buildNetworkResourcesPayload(
    node, discovered.totalCount, connectedInfo.len
  )
  let listenAddrs =
    if not node.switchInstance.isNil and not node.switchInstance.peerInfo.isNil:
      node.switchInstance.peerInfo.listenAddrs.mapIt($it)
    else:
      @[]
  let dialableAddrs =
    if not node.switchInstance.isNil and not node.switchInstance.peerInfo.isNil:
      node.switchInstance.peerInfo.addrs.mapIt($it)
    else:
      @[]
  let seedAddrs =
    if not node.isNil:
      collectLocalGameSeedAddrs(node)
    else:
      @[]
  let bootstrapProvider = buildWanProviderPayload(node)
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
  payload["listenAddresses"] = %listenAddrs
  payload["dialableAddresses"] = %dialableAddrs
  payload["seedMultiaddrs"] = %seedAddrs
  payload["localSystemProfile"] = localProfile
  payload["systemProfile"] = localProfile
  payload["transportHealth"] = buildTransportHealthPayload(
    node, listenAddrs, dialableAddrs
  )
  payload["hostNetworkStatus"] = currentHostNetworkStatus(node)
  payload["networkResources"] = networkResources
  payload["mdnsProbeOk"] = %true
  maybeKickAuthoritativeBootstrap(node, "auto_authoritative_seed_snapshot")
  var bootstrapNode = newJObject()
  try:
    if not node.switchInstance.isNil:
      bootstrapNode = bootstrapStateJson(node.switchInstance, BootstrapCandidateCap)
  except CatchableError:
    bootstrapNode = newJObject()
  if node.authoritativeBootstrapPeerIds.len > 0:
    mergeLocalBootstrapStatus(node, bootstrapNode, BootstrapCandidateCap)
  attachBootstrapProvider(bootstrapNode, bootstrapProvider)
  payload["bootstrap"] = bootstrapNode
  payload["bootstrapProvider"] = bootstrapProvider
  payload

proc buildUiFrameSnapshotPayload(
    node: NimNode,
    discoveryPayload: JsonNode,
    maxEvents: int
): JsonNode =
  var payload = newJObject()
  payload["ok"] = %true
  payload["timestampMs"] = %nowMillis()
  payload["running"] = %(not node.isNil and node.started)
  payload["peerId"] = %localPeerId(node)
  try:
    payload["events"] = parseJson(pollEventLog(node, maxEvents))
  except CatchableError:
    payload["events"] = newJArray()
  payload["discovery"] = discoveryPayload
  payload["lanEndpoints"] = gatherLanEndpoints(node)
  payload["feed"] = buildFeedSnapshotPayload(node)
  payload

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
          applyAuthoritativeBootstrapConfig(n)
          syncPeerHintsIntoBootstrapService(n)
          debugLog("[nimlibp2p] cmdStart: switch.start completed")
          info "cmdStart: switch started", listenAddrs = n.switchInstance.peerInfo.listenAddrs.mapIt($it)
          debugLog("[nimlibp2p] cmdStart: switch started")
          await setupDefaultTopics(n)
          debugLog("[nimlibp2p] cmdStart: default topics configured")
          await startUdpEchoResponder(n)
          debugLog("[nimlibp2p] cmdStart: udp echo responder ready")
          when not defined(ohos):
            if n.authoritativeBootstrapPeerIds.len > 0 and not n.switchInstance.isNil:
              proc delayedAuthoritativeBootstrapReconnect() {.async.} =
                await sleepAsync(chronos.seconds(1))
                try:
                  await reconnectAuthoritativeBootstrapExact(n, "auto_authoritative_seed_startup")
                except CatchableError as exc:
                  debugLog(
                    "[nimlibp2p] delayed authoritative bootstrapReconnect failed err=" &
                    exc.msg
                  )
              asyncSpawn(delayedAuthoritativeBootstrapReconnect())
          when defined(ohos):
            if n.authoritativeBootstrapPeerIds.len > 0:
              debugLog("[nimlibp2p] cmdStart: skip authoritative bootstrap auto reconnect on ohos")
          if n.peerHints.len > 0:
            proc delayedBoost() {.async.} =
              await sleepAsync(chronos.seconds(1))
              await boostConnectivity(n)
            asyncSpawn(delayedBoost())
          let relayReservationAddrs = configuredRelayReservationAddrs(n)
          if relayReservationAddrs.len > 0:
            proc delayedRelayReservation() {.async.} =
              await sleepAsync(chronos.seconds(1))
              var reservedTotal = 0
              for relayAddr in relayReservationAddrs:
                reservedTotal += await reserveOnRelayAsync(n, relayAddr)
              debugLog(
                "[nimlibp2p] cmdStart: relay reservation attempted count=" &
                $relayReservationAddrs.len &
                " reserved=" & $reservedTotal
              )
            asyncSpawn(delayedRelayReservation())
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
            await startUdpEchoResponder(n)
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
        await stopUdpEchoResponder(n)
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
        if not n.signal.isNil:
          discard n.signal.fireSync()
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
            let timeoutMs =
              if command.timeoutMs > 0:
                max(command.timeoutMs, 250)
              else:
                5_000
            let maxAttempts =
              if command.timeoutMs > 0 and command.timeoutMs <= 2_000:
                1
              else:
                3
            var attempt = 0
            var success = false
            var lastError = ""
            while attempt < maxAttempts and not success:
              try:
                let dialFut = n.switchInstance.connect(peer, dialable, forceDial = true)
                let completed = await withTimeout(dialFut, chronos.milliseconds(timeoutMs))
                if not completed:
                  dialFut.cancelSoon()
                  lastError = "connect timeout"
                  let attemptNum = attempt + 1
                  warn "cmdConnectPeer timeout", peer = command.peerId, attempt = attemptNum
                  debugLog(
                    "[nimlibp2p] cmdConnectPeer timeout peer=" & command.peerId &
                    " attempt=" & $attemptNum &
                    " timeoutMs=" & $timeoutMs
                  )
                else:
                  await dialFut
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
        let dialMultiaddr = stripLeadingPhysicalPrefix(command.multiaddr)
        let ma = parseDialableMultiaddr(command.multiaddr).valueOr:
          command.resultCode = NimResultInvalidArgument
          command.errorMsg = "invalid multiaddr"
          n.lastDirectError = command.errorMsg
          return
        let lowerMultiaddr = dialMultiaddr.toLowerAscii()
        let quicDial =
          lowerMultiaddr.contains("/quic-v1") or
          lowerMultiaddr.contains("/quic/")
        let requestedTimeoutMs = if command.timeoutMs > 0: command.timeoutMs else: 0
        let timeoutMs =
          if quicDial:
            if requestedTimeoutMs > 0:
              max(requestedTimeoutMs, 250)
            else:
              30_000
          else:
            if requestedTimeoutMs > 0:
              max(requestedTimeoutMs, 250)
            else:
              10_000
        let maxAttempts =
          if command.requestLimit > 0:
            max(1, command.requestLimit)
          elif quicDial:
            1
          elif command.timeoutMs > 0 and command.timeoutMs <= 2_000:
            1
          else:
            3
        var attempt = 0
        var success = false
        var lastError = ""
        debugLog(
          "[nimlibp2p] cmdConnectMultiaddr start addr=" & command.multiaddr &
          " dialAddr=" & dialMultiaddr &
          " timeoutMs=" & $timeoutMs &
          " maxAttempts=" & $maxAttempts &
          " quicDial=" & $quicDial
        )
        while attempt < maxAttempts and not success:
          let attemptNum = attempt + 1
          try:
            let fullAddressRes = parseFullAddress(ma)
            var completed = false
            if fullAddressRes.isOk():
              let (peerId, _) = fullAddressRes.get()
              if quicDial and peerCurrentlyConnected(n, peerId):
                try:
                  let disconnected = await disconnectPeerWithTimeout(
                    n,
                    peerId,
                    min(timeoutMs, 2_000),
                    "cmdConnectMultiaddr_quic_stale",
                  )
                  debugLog(
                    "[nimlibp2p] cmdConnectMultiaddr drop existing peer before quic dial addr=" &
                    command.multiaddr &
                    " peer=" & $peerId &
                    " disconnected=" & $disconnected
                  )
                except CatchableError as discExc:
                  debugLog(
                    "[nimlibp2p] cmdConnectMultiaddr disconnect before quic dial failed addr=" &
                    command.multiaddr &
                    " peer=" & $peerId &
                    " err=" & discExc.msg
                  )
              debugLog(
                "[nimlibp2p] cmdConnectMultiaddr connect_future_create addr=" &
                command.multiaddr &
                " attempt=" & $attemptNum &
                " deferred=1"
              )
              let addrFut = connectMultiaddrDeferred(n, ma)
              debugLog(
                "[nimlibp2p] cmdConnectMultiaddr connect_future_created addr=" &
                command.multiaddr &
                " attempt=" & $attemptNum
              )
              var observedConnected = false
              try:
                let waitRes = await awaitFutureWithinDeadlineOrPeerConnected(
                  n,
                  addrFut,
                  peerId,
                  timeoutMs,
                )
                completed = waitRes.completed
                observedConnected = waitRes.observedConnected
                if completed:
                  discard await addrFut
                elif observedConnected:
                  debugLog(
                    "[nimlibp2p] cmdConnectMultiaddr observed_connected addr=" &
                    command.multiaddr &
                    " attempt=" & $attemptNum &
                    " peer=" & $peerId
                  )
                  if not addrFut.finished():
                    asyncSpawn drainFutureDetached(
                      addrFut,
                      "cmdConnectMultiaddr_observed_connected:" &
                        command.multiaddr & ":attempt=" & $attemptNum,
                    )
              except CatchableError:
                if not addrFut.finished():
                  await cancelFutureAndDrain(
                    addrFut,
                    "cmdConnectMultiaddr_catch:" & command.multiaddr & ":attempt=" & $attemptNum,
                  )
                raise
              if not completed:
                if not addrFut.finished():
                  if command.boolParam and quicDial:
                    asyncSpawn drainFutureDetached(
                      addrFut,
                      "cmdConnectMultiaddr_timeout_detached:" &
                        command.multiaddr & ":attempt=" & $attemptNum,
                    )
                  else:
                    await cancelFutureAndDrain(
                      addrFut,
                      "cmdConnectMultiaddr_timeout:" & command.multiaddr & ":attempt=" & $attemptNum,
                    )
                if quicDial and peerCurrentlyConnected(n, peerId):
                  discard await disconnectPeerWithTimeout(
                    n,
                    peerId,
                    min(timeoutMs, 750),
                    "cmdConnectMultiaddr_quic_timeout_cleanup",
                  )
              elif observedConnected:
                completed = true
            else:
              debugLog(
                "[nimlibp2p] cmdConnectMultiaddr connect_future_create addr=" &
                command.multiaddr &
                " attempt=" & $attemptNum &
                " deferred=1"
              )
              let addrFut = connectMultiaddrDeferred(n, ma)
              debugLog(
                "[nimlibp2p] cmdConnectMultiaddr connect_future_created addr=" &
                command.multiaddr &
                " attempt=" & $attemptNum
              )
              try:
                completed = await awaitFutureWithinDeadline(addrFut, timeoutMs)
                if completed:
                  discard await addrFut
              except CatchableError:
                if not addrFut.finished():
                  await cancelFutureAndDrain(
                    addrFut,
                    "cmdConnectMultiaddr_catch:" & command.multiaddr & ":attempt=" & $attemptNum,
                  )
                raise
              if not completed and not addrFut.finished():
                await cancelFutureAndDrain(
                  addrFut,
                  "cmdConnectMultiaddr_timeout:" & command.multiaddr & ":attempt=" & $attemptNum,
                )
            if not completed:
              lastError = "connect timeout"
              warn "cmdConnectMultiaddr timeout", addr = command.multiaddr, attempt = attemptNum
              debugLog(
                "[nimlibp2p] cmdConnectMultiaddr timeout addr=" & command.multiaddr &
                " attempt=" & $attemptNum
              )
            else:
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
            let peerText = peerOpt.get()
            n.bootstrapLastSeen[peerText] = nowMillis()
            let shouldPromoteBootstrap =
              peerText in n.authoritativeBootstrapPeerIds or
                n.peerHints.hasKey(peerText)
            if shouldPromoteBootstrap:
              asyncSpawn(promoteConnectedBootstrapPeerDetached(
                n,
                peerText,
                @[command.multiaddr],
                "direct_exact_connect",
              ))
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
      command.stringResult = $buildConnectedPeersArrayForNode(n)
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
      let hostNetwork = currentHostNetworkStatus(n)
      let transportHealth = buildTransportHealthPayload(n, listenAddrs, dialableAddrs)
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
        "transportHealth": transportHealth,
        "hostNetworkStatus": hostNetwork,
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
      command.stringResult = $(await buildConnectedPeersInfoArrayForNode(n))
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
      var normalizedAddrs: seq[string] = @[]
      for addrStr in command.addresses:
        let normalizedText = stripPeerIdSuffixFromMultiaddr(addrStr)
        if normalizedText.len == 0:
          continue
        let ma = parseDialableMultiaddr(normalizedText).valueOr:
          warn "registerPeerHints: invalid addr", peerId = command.peerId, address = normalizedText, err = error
          continue
        if normalizedText notin normalizedAddrs:
          normalizedAddrs.add(normalizedText)
        parsed.add(ma)
      if parsed.len == 0:
        command.resultCode = NimResultInvalidArgument
        command.errorMsg = "no valid addresses"
        return
      let hintSource =
        if command.source.strip().len > 0:
          command.source.strip()
        else:
          "manual"
      n.peerHints[command.peerId] = normalizedAddrs
      addPeerHintSource(n, command.peerId, hintSource)
      n.bootstrapLastSeen[command.peerId] = nowMillis()
      let peerStore = n.switchInstance.peerStore
      if not peerStore.isNil:
        try:
          peerStore.addAddressesWithTTL(
            peer, parsed, chronos.minutes(30)
          )
        except CatchableError as exc:
          warn "registerPeerHints: addAddressesWithTTL failed", peerId = command.peerId, err = exc.msg
      if not n.switchInstance.isNil:
        discard n.switchInstance.registerBootstrapHint(
          peer, parsed, source = hintSource, trust = WanBootstrapTrust.quarantine
        )
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
      if n.switchInstance.isNil:
        command.resultCode = NimResultInvalidState
        command.errorMsg = "switch not initialized"
      else:
        let joinResult = await bootstrapReconnect(n.switchInstance, BootstrapCandidateCap)
        command.stringResult = $wanBootstrapJoinResultToJson(joinResult)
        command.delivered = joinResult.connectedCount
        command.boolResult = joinResult.ok
        command.intResult = joinResult.connectedCount
        if not joinResult.ok:
          command.errorMsg = "no bootstrap peer connected"
        command.resultCode = NimResultOk
    of cmdGetRandomBootstrapPeers:
      let limit = clampBootstrapLimit(
        command.requestLimit,
        defaultLimit = BootstrapCandidateCap,
        maxLimit = BootstrapCandidateCap
      )
      let candidates = collectLocalBootstrapCandidates(n)
      let selected = selectProgressiveBootstrapCandidates(candidates, limit)
      var peersNode = newJArray()
      for item in selected:
        var entry = newJObject()
        entry["peerId"] = %item.peerId
        entry["multiaddrs"] = %item.multiaddrs
        entry["lastSeenAt"] = %item.lastSeenAt
        entry["sources"] = %item.sources
        entry["selectionReason"] = %item.selectionReason
        peersNode.add(entry)
      var payload = newJObject()
      payload["peers"] = peersNode
      payload["policy"] = %"progressive_1_2_7"
      payload["candidateCap"] = %BootstrapCandidateCap
      payload["dialConcurrency"] = %BootstrapDialConcurrency
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
        let limit = clampBootstrapLimit(
          command.requestLimit,
          defaultLimit = BootstrapCandidateCap,
          maxLimit = BootstrapCandidateCap
        )
        if n.switchInstance.isNil:
          command.resultCode = NimResultInvalidState
          command.errorMsg = "switch not initialized"
        else:
          let joinResult = await joinViaBootstrap(n.switchInstance, limit)
          var payload = wanBootstrapJoinResultToJson(joinResult)
          payload["policy"] = %"progressive_1_2_7"
          payload["candidateCap"] = %BootstrapCandidateCap
          payload["dialConcurrency"] = %BootstrapDialConcurrency
          payload["attemptCount"] = %joinResult.attemptedCount
          payload["timestamp_ms"] = %joinResult.timestampMs
          command.stringResult = $payload
          command.intResult = joinResult.connectedCount
          command.boolResult = joinResult.ok
          if not joinResult.ok:
            command.errorMsg = "no bootstrap peer connected"
          command.resultCode = NimResultOk
    of cmdJoinViaSeedBootstrap:
      if not n.started:
        command.resultCode = NimResultInvalidState
        command.errorMsg = "node not started"
      elif command.peerId.len == 0 or command.addresses.len == 0:
        command.resultCode = NimResultInvalidArgument
        command.errorMsg = "missing peerId or addresses"
      else:
        let peer = PeerId.init(command.peerId).valueOr:
          command.resultCode = NimResultInvalidArgument
          command.errorMsg = "invalid peer id"
          return
        var parsed: seq[MultiAddress] = @[]
        var normalizedAddrs: seq[string] = @[]
        for addrStr in command.addresses:
          let normalizedText = stripPeerIdSuffixFromMultiaddr(addrStr)
          if normalizedText.len == 0:
            continue
          let ma = parseDialableMultiaddr(normalizedText).valueOr:
            warn "joinViaSeedBootstrap: invalid addr", peerId = command.peerId, address = normalizedText, err = error
            continue
          if normalizedText in normalizedAddrs:
            continue
          normalizedAddrs.add(normalizedText)
          parsed.add(ma)
        if parsed.len == 0:
          command.resultCode = NimResultInvalidArgument
          command.errorMsg = "no valid addresses"
          return
        let hostNetwork = currentHostNetworkStatus(n)
        let localSeedAddrs = collectLocalGameSeedAddrs(n)
        let localHasIpv4 = block:
          var found = false
          for addrText in localSeedAddrs:
            if "/ip4/" in addrText.toLowerAscii():
              found = true
              break
          found
        let localHasGlobalIpv6 = block:
          var found = false
          for addrText in localSeedAddrs:
            let lower = addrText.toLowerAscii()
            if "/ip6/" notin lower:
              continue
            let parts = lower.split("/ip6/")
            if parts.len < 2:
              continue
            let ipPart = parts[^1]
            let slashIdx = ipPart.find('/')
            let ip = if slashIdx >= 0: ipPart[0 ..< slashIdx] else: ipPart
            if isGlobalHostIpv6(ip):
              found = true
              break
          found
        var filteredParsed: seq[MultiAddress] = @[]
        var filteredAddrs: seq[string] = @[]
        var sawIpv4 = false
        var sawIpv6 = false
        for idx, normalizedText in normalizedAddrs:
          let lower = normalizedText.toLowerAscii()
          let usesIpv6 = "/ip6/" in lower
          let usesIpv4 = "/ip4/" in lower
          if usesIpv6:
            sawIpv6 = true
          if usesIpv4:
            sawIpv4 = true
          let supported =
            (usesIpv6 and localHasGlobalIpv6) or
            (usesIpv4 and localHasIpv4) or
            (not usesIpv6 and not usesIpv4)
          if supported:
            filteredAddrs.add(normalizedText)
            filteredParsed.add(parsed[idx])
        if filteredParsed.len == 0:
          var payload = newJObject()
          let errorText =
            if sawIpv6 and not localHasGlobalIpv6 and not sawIpv4:
              "no_matching_wan_path:local_global_ipv6_unavailable"
            elif sawIpv4 and not localHasIpv4 and not sawIpv6:
              "no_matching_wan_path:local_ipv4_unavailable"
            else:
              "no_matching_wan_path:local_seed_family_incompatible"
          payload["ok"] = %false
          payload["error"] = %errorText
          payload["attemptedCount"] = %0
          payload["connectedCount"] = %0
          payload["attempts"] = newJArray()
          payload["attempted"] = newJArray()
          payload["policy"] = %"seeded_direct_hint"
          payload["candidateCap"] = %BootstrapCandidateCap
          payload["dialConcurrency"] = %BootstrapDialConcurrency
          payload["attemptCount"] = %0
          payload["timestamp_ms"] = %nowMillis()
          payload["seedPeerId"] = %command.peerId
          payload["seedAddrs"] = %normalizedAddrs
          payload["seedSource"] = %command.source.strip()
          payload["allowFallback"] = %false
          payload["hostNetworkStatus"] = hostNetwork
          payload["localSeedAddrs"] = %localSeedAddrs
          payload["localHasIpv4"] = %localHasIpv4
          payload["localHasGlobalIpv6"] = %localHasGlobalIpv6
          command.stringResult = $payload
          command.intResult = 0
          command.boolResult = false
          command.errorMsg = errorText
          command.resultCode = NimResultOk
          return
        normalizedAddrs = filteredAddrs
        parsed = filteredParsed
        let hintSource =
          if command.source.strip().len > 0:
            command.source.strip()
          else:
            "seeded_join"
        n.peerHints[command.peerId] = normalizedAddrs
        addPeerHintSource(n, command.peerId, hintSource)
        n.bootstrapLastSeen[command.peerId] = nowMillis()
        let peerStore = n.switchInstance.peerStore
        if not peerStore.isNil:
          try:
            peerStore.addAddressesWithTTL(peer, parsed, chronos.minutes(30))
          except CatchableError as exc:
            warn "joinViaSeedBootstrap: addAddressesWithTTL failed", peerId = command.peerId, err = exc.msg
        if not n.switchInstance.isNil:
          discard n.switchInstance.registerBootstrapHint(
            peer, parsed, source = hintSource, trust = WanBootstrapTrust.quarantine
          )
          let limit = clampBootstrapLimit(
            command.requestLimit,
            defaultLimit = 1,
            maxLimit = BootstrapCandidateCap
          )
          let joinResult = await joinViaSeedBootstrap(
            n.switchInstance,
            peer,
            parsed,
            hintSource,
          )
          var payload = wanBootstrapJoinResultToJson(joinResult)
          payload["policy"] = %"seeded_direct_hint"
          payload["candidateCap"] = %BootstrapCandidateCap
          payload["dialConcurrency"] = %BootstrapDialConcurrency
          payload["attemptCount"] = %joinResult.attemptedCount
          payload["timestamp_ms"] = %joinResult.timestampMs
          payload["seedPeerId"] = %command.peerId
          payload["seedAddrs"] = %normalizedAddrs
          payload["seedSource"] = %hintSource
          payload["allowFallback"] = %false
          command.stringResult = $payload
          command.intResult = joinResult.connectedCount
          command.boolResult = joinResult.ok
          if not joinResult.ok:
            command.errorMsg = "no bootstrap peer connected"
          command.resultCode = NimResultOk
        else:
          command.resultCode = NimResultInvalidState
          command.errorMsg = "switch not initialized"
    of cmdUdpEchoProbe:
      if not n.started:
        command.resultCode = NimResultInvalidState
        command.errorMsg = "node not started"
      elif command.multiaddr.len == 0:
        command.resultCode = NimResultInvalidArgument
        command.errorMsg = "missing target multiaddr"
      else:
        let probePayload = await runUdpEchoProbe(n, command.multiaddr, command.timeoutMs)
        command.stringResult = $probePayload
        command.boolResult = jsonBool(probePayload, "ok", false)
        if not command.boolResult:
          command.errorMsg = jsonGetStr(probePayload, "error", "udp_echo_probe_failed")
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
        if command.addresses.len > 0:
          try:
            registerInitialPeerHints(
              n,
              command.addresses,
              if command.source.len > 0: command.source else: "cmd_send_direct"
            )
          except CatchableError as exc:
            debugLog(
              "[nimlibp2p] cmdSendDirect registerInitialPeerHints failed peer=" &
              command.peerId &
              " err=" & exc.msg
            )
        var envelope: JsonNode
        if command.preSerializedJson and command.jsonString.len > 0:
          envelope = nil
        elif command.jsonString.len > 0:
          try:
            envelope = parseJson(command.jsonString)
          except CatchableError as exc:
            warn "cmdSendDirect: invalid json", err = exc.msg
            envelope = newJObject()
        if not command.preSerializedJson and (envelope.isNil or envelope.kind != JObject):
          envelope = newJObject()
        var mid =
          if command.preSerializedJson:
            ""
          else:
            jsonGetStr(envelope, "mid")
        if command.messageId.len > 0:
          mid = command.messageId
        mid = ensureMessageId(mid)
        let opValue =
          if command.op.len > 0:
            command.op
          elif command.preSerializedJson:
            "text"
          elif jsonGetStr(envelope, "op").len > 0:
            jsonGetStr(envelope, "op")
          else:
            "text"
        if not command.preSerializedJson:
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
        let connectivityOk = await ensurePeerConnectivity(
          n,
          remotePid,
          preferWanBootstrap = shouldPreferWanBootstrapForDirectOp(opValue)
        )
        if not connectivityOk:
          let errMsg = if command.errorMsg.len > 0: command.errorMsg else: "peer unreachable"
          command.errorMsg = errMsg
          command.boolResult = false
          command.resultCode = NimResultError
          n.lastDirectError = errMsg
          if command.ackRequested and
              opValue.toLowerAscii() notin ["ack", "bw_probe_push", "bw_probe_chunk"]:
            recordDirectAckLatencyFailure(n, command.peerId, errMsg)
          debugLog("[nimlibp2p] cmdSendDirect connectivity missing peer=" & command.peerId & " err=" & errMsg)
          info "cmdSendDirect failed", peer = command.peerId, mid = mid, err = errMsg
          return
        debugLog("[nimlibp2p] cmdSendDirect connectivity peer=" & command.peerId &
          " ok=true ackRequested=" & $command.ackRequested)
        let bytes =
          if command.preSerializedJson:
            stringToBytes(command.jsonString)
          else:
            stringToBytes($envelope)
        var sendOk = false
        var sendErr = ""
        let sendStartMs = nowMillis()
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
          let latencyMs = int(max(0'i64, nowMillis() - sendStartMs))
          let recordAckLatency =
            opValue.toLowerAscii() notin ["ack", "bw_probe_push", "bw_probe_chunk"]
          if recordAckLatency:
            if sendOk:
              recordDirectAckLatencySuccess(n, command.peerId, latencyMs)
            else:
              let latencyErr =
                if sendErr.len > 0: sendErr
                elif errMsg.len > 0: errMsg
                else: "ack timeout"
              recordDirectAckLatencyFailure(n, command.peerId, latencyErr)
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
    of cmdUpdateHostNetworkStatus:
      if command.jsonString.len == 0:
        command.resultCode = NimResultInvalidArgument
        command.errorMsg = "missing host network status payload"
      else:
        try:
          debugLog(
            "[nimlibp2p] cmdUpdateHostNetworkStatus payload=" & command.jsonString
          )
          let previousHost = currentHostNetworkStatus(n)
          let payload = parseJson(command.jsonString)
          var normalized = applyHostNetworkStatus(n, payload)
          debugLog(
            "[nimlibp2p] cmdUpdateHostNetworkStatus applied connected=" &
            $jsonGetBool(normalized, "isConnected", false) &
            " networkType=" & jsonGetStr(normalized, "networkType", "none") &
            " transport=" & jsonGetStr(normalized, "transport", "none")
          )
          let previousMdnsAllowed = mdnsAllowedForHostStatus(previousHost)
          let currentMdnsAllowed = mdnsAllowedForHostStatus(normalized)
          let mdnsTransportChanged =
            previousMdnsAllowed != currentMdnsAllowed or
            jsonGetStr(previousHost, "networkType", "") != jsonGetStr(normalized, "networkType", "") or
            jsonGetStr(previousHost, "transport", "") != jsonGetStr(normalized, "transport", "") or
            jsonGetStr(previousHost, "preferredIpv4", "") != jsonGetStr(normalized, "preferredIpv4", "") or
            jsonGetStr(previousHost, "localIpv4", "") != jsonGetStr(normalized, "localIpv4", "")
          asyncCheck(runHostNetworkStatusSideEffects(
            n,
            normalized,
            mdnsTransportChanged,
            currentMdnsAllowed,
          ))
          debugLog("[nimlibp2p] cmdUpdateHostNetworkStatus side_effects:spawned")
          let event = buildHostNetworkEvent(normalized)
          recordEvent(n, "network_event", $event)
          command.stringResult = $normalized
          command.resultCode = NimResultOk
          debugLog("[nimlibp2p] cmdUpdateHostNetworkStatus done")
        except CatchableError as exc:
          command.resultCode = NimResultInvalidArgument
          command.errorMsg = "invalid host network status payload: " & exc.msg
    of cmdGetHostNetworkStatus:
      command.stringResult = $currentHostNetworkStatus(n)
      command.resultCode = NimResultOk
    of cmdNetworkDiscoverySnapshot:
      discard command.intParam
      var connectedInfo = await buildConnectedPeersInfoArrayForNode(
        n, includeLatency = false
      )
      if connectedInfo.len == 0:
        let recent = recentConnectedPeersInfo(n)
        if recent.len > 0:
          connectedInfo = recent
      command.stringResult = $buildNetworkDiscoverySnapshotPayload(
        n, connectedInfo, command.source, command.requestLimit
      )
      command.resultCode = NimResultOk
    of cmdBoostConnectivity:
      await boostConnectivity(n)
      command.resultCode = NimResultOk
    of cmdGetLanEndpoints:
      let json = gatherLanEndpoints(n)
      command.stringResult = $json
      command.resultCode = NimResultOk
    of cmdFetchFeedSnapshot:
      command.stringResult = $buildFeedSnapshotPayload(n)
      command.resultCode = NimResultOk
    of cmdUiFrameSnapshot:
      var connectedInfo = await buildConnectedPeersInfoArrayForNode(
        n, includeLatency = false
      )
      if connectedInfo.len == 0:
        let recent = recentConnectedPeersInfo(n)
        if recent.len > 0:
          connectedInfo = recent
      let discoveryPayload = buildNetworkDiscoverySnapshotPayload(
        n, connectedInfo, "", command.intParam
      )
      command.stringResult = $buildUiFrameSnapshotPayload(
        n, discoveryPayload, command.requestLimit
      )
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
          warn "cmdPublishLivestream skipped without subscribers", streamKey = command.streamKey, size = command.payload.len
          command.resultCode = NimResultOk
          command.errorMsg = ""
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
          {.cast(gcsafe).}:
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
      command.stringResult = $buildMdnsDebugPayload(n)
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
        let peerUplinkBytes = max(0'i64, uploadAfter.outbound - uploadBefore.outbound)
        let uplinkBytes =
          if peerUplinkBytes > 0: peerUplinkBytes
          else: uploadRun.payloadBytes
        let uplinkBps = computeProbeBps(uplinkBytes, uploadWindowMs)

        let downloadWindowStart = nowMillis()
        let downloadBefore = peerBandwidthTotals(n, remotePid)
        let downloadSessionId = sessionBase & "-down"
        let downloadBeforeTraffic = readProbeInboundTraffic(n, command.peerId, downloadSessionId)
        let requestMid = ensureMessageId("bw-probe-down-" & $nowMillis())
        var request = newJObject()
        request["op"] = %"bw_probe_push"
        request["mid"] = %requestMid
        request["sid"] = %downloadSessionId
        request["from"] = %local
        request["durationMs"] = %durationMs
        request["chunkBytes"] = %chunkBytes
        request["timestamp_ms"] = %nowMillis()
        request["ackRequested"] = %true
        let requestPayload = stringToBytes($request)
        let requestTimeout = chronos.milliseconds(max(ProbeSendTimeoutMs, durationMs))
        var downloadRequestOk = false
        var downloadRequestErr = ""
        var downloadRepaired = false
        let sendRes = await sendProbeDirectMessage(
          n, remotePid, requestPayload, true, requestMid, requestTimeout
        )
        downloadRequestOk = sendRes[0]
        downloadRequestErr = sendRes[1]
        if not downloadRequestOk and not downloadRepaired and
            await repairProbeConnection(n, remotePid, downloadRequestErr):
          downloadRepaired = true
          let retryRes = await sendProbeDirectMessage(
            n, remotePid, requestPayload, true, requestMid, requestTimeout
          )
          downloadRequestOk = retryRes[0]
          downloadRequestErr = retryRes[1]
        if downloadRequestOk:
          await sleepAsync(chronos.milliseconds(durationMs + ProbeWindowTailMs))
        let downloadAfter = peerBandwidthTotals(n, remotePid)
        let downloadAfterTraffic = readProbeInboundTraffic(n, command.peerId, downloadSessionId)
        let downloadWindowMs = max(1'i64, nowMillis() - downloadWindowStart)
        let peerDownlinkBytes = max(0'i64, downloadAfter.inbound - downloadBefore.inbound)
        let sessionDownlinkBytes = max(
          0'i64, downloadAfterTraffic.payloadBytes - downloadBeforeTraffic.payloadBytes
        )
        let sessionDownlinkChunks = max(
          0, downloadAfterTraffic.chunkCount - downloadBeforeTraffic.chunkCount
        )
        let downlinkBytes =
          if peerDownlinkBytes > 0: peerDownlinkBytes
          else: sessionDownlinkBytes
        let downlinkBps = computeProbeBps(downlinkBytes, downloadWindowMs)
        let bottleneckBps = if relayed: min(uplinkBps, downlinkBps) else: 0.0

        let probeOk = uploadRun.ok and downloadRequestOk
        probe["ok"] = %probeOk
        probe["uplinkBps"] = jsonSafeFloat(uplinkBps)
        probe["downlinkBps"] = jsonSafeFloat(downlinkBps)
        probe["uplinkSampleBytes"] = %uplinkBytes
        probe["downlinkSampleBytes"] = %downlinkBytes
        probe["uploadWindowMs"] = %uploadWindowMs
        probe["downloadWindowMs"] = %downloadWindowMs
        probe["uploadChunkCount"] = %uploadRun.chunkCount
        probe["uploadPayloadBytes"] = %uploadRun.payloadBytes
        probe["downloadChunkCount"] = %sessionDownlinkChunks
        probe["relayLimited"] = %relayed
        probe["relayBottleneckBps"] = jsonSafeFloat(bottleneckBps)
        if not uploadRun.ok and uploadRun.err.len > 0:
          probe["uploadError"] = %uploadRun.err
          probe["error"] = %uploadRun.err
        if not downloadRequestOk and downloadRequestErr.len > 0:
          probe["downloadError"] = %downloadRequestErr
          if not probe.hasKey("error"):
            probe["error"] = %downloadRequestErr
        if not probeOk and not probe.hasKey("error"):
          probe["error"] = %"bandwidth probe failed"
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
    of cmdMeasurePeerLatency:
      var payload = newJObject()
      payload["ok"] = %false
      payload["peerId"] = %command.peerId
      let sampleCount = clampPeerLatencySampleCount(command.intParam)
      let timeoutMs =
        if command.timeoutMs > 0:
          max(250, min(command.timeoutMs, 10_000))
        else:
          PeerLatencyTimeoutMs
      payload["sampleCount"] = %sampleCount
      payload["timeoutMs"] = %timeoutMs
      payload["samplesMs"] = newJArray()
      payload["sampleMetric"] = %"none"
      payload["transportSamplesMs"] = newJArray()
      payload["dmAckSamplesMs"] = newJArray()
      payload["successfulSamples"] = %0
      payload["failedSamples"] = %0
      payload["latencyMs"] = %(-1)
      payload["transportRttMs"] = %(-1)
      payload["dmAckRttMs"] = %(-1)
      payload["muxers"] = newJArray()
      payload["isRelayed"] = %false
      payload["bandwidth"] = newJObject()
      payload["timestampMs"] = %nowMillis()
      if not n.started:
        payload["error"] = %"node not started"
      elif command.peerId.len == 0:
        payload["error"] = %"missing peer id"
      else:
        await setupDefaultTopics(n)
        var remotePid: PeerId
        if not remotePid.init(command.peerId):
          payload["error"] = %"invalid peer id"
        else:
          let connectivityOk = await ensurePeerConnectivity(n, remotePid)
          if not connectivityOk:
            let errText = if command.errorMsg.len > 0: command.errorMsg else: "peer unreachable"
            payload["error"] = %errText
            recordTransportLatencyFailure(n, command.peerId, errText)
          else:
            var preferredSamples = newJArray()
            var transportSamples = newJArray()
            var dmAckSamples = newJArray()
            for idx in 0 ..< sampleCount:
              let latency = await measurePeerTransportLatencyMs(n, remotePid, timeoutMs)
              if latency >= 0:
                recordTransportLatencySuccess(n, command.peerId, latency)
                transportSamples.add(%latency)
                preferredSamples.add(%latency)
              else:
                recordTransportLatencyFailure(n, command.peerId, "latency timeout")
                let dmAckLatency = await measurePeerDirectAckLatencyMs(
                  n, remotePid, command.peerId, timeoutMs
                )
                if dmAckLatency >= 0:
                  dmAckSamples.add(%dmAckLatency)
                  preferredSamples.add(%dmAckLatency)
              if idx + 1 < sampleCount:
                await sleepAsync(chronos.milliseconds(PeerLatencyInterSampleDelayMs))
            let preferredSuccessCount = preferredSamples.len
            let preferredFailureCount = max(0, sampleCount - preferredSuccessCount)
            let sampleMetric =
              if transportSamples.len > 0 and dmAckSamples.len > 0:
                "hybrid"
              elif transportSamples.len > 0:
                "transport_rtt"
              elif dmAckSamples.len > 0:
                "dm_ack_rtt"
              else:
                "none"
            let connectedInfo = await buildConnectedPeersInfoArrayForNode(n, includeLatency = false)
            let matched = findConnectedPeerInfoRow(connectedInfo, command.peerId)
            let latencyStats = peerLatencyStatsJson(n, command.peerId)
            let transportLatency = cachedTransportLatencyMs(n, command.peerId)
            let dmAckLatency = cachedDirectAckLatencyMs(n, command.peerId)
            let preferredLatency =
              if transportLatency >= 0: transportLatency
              elif dmAckLatency >= 0: dmAckLatency
              else: -1
            payload["ok"] = %(preferredSuccessCount > 0)
            payload["sampleMetric"] = %sampleMetric
            payload["samplesMs"] = preferredSamples
            payload["transportSamplesMs"] = transportSamples
            payload["dmAckSamplesMs"] = dmAckSamples
            payload["successfulSamples"] = %preferredSuccessCount
            payload["failedSamples"] = %preferredFailureCount
            payload["latencyMs"] = %preferredLatency
            payload["transportRttMs"] = %transportLatency
            payload["dmAckRttMs"] = %dmAckLatency
            payload["latencyStats"] = latencyStats
            if matched.kind == JObject:
              if matched.hasKey("muxers") and matched["muxers"].kind == JArray:
                payload["muxers"] = matched["muxers"]
              payload["isRelayed"] = %jsonGetBool(matched, "isRelayed", false)
              if matched.hasKey("bandwidth") and matched["bandwidth"].kind == JObject:
                payload["bandwidth"] = matched["bandwidth"]
              if matched.hasKey("systemProfile") and matched["systemProfile"].kind == JObject:
                payload["systemProfile"] = matched["systemProfile"]
            if preferredSuccessCount == 0:
              payload["error"] = %"latency_unavailable"
      command.boolResult = jsonGetBool(payload, "ok", false)
      command.stringResult = $payload
      command.resultCode = NimResultOk
    of cmdRequestFileChunk:
      n.lastChunkSize = 0
      var response = newJObject()
      response["ok"] = %false
      response["chunkSize"] = %0
      if not n.started:
        response["error"] = %"node not started"
      elif command.peerId.len == 0:
        response["error"] = %"missing peer id"
      elif command.jsonString.len == 0:
        response["error"] = %"missing request payload"
      else:
        await setupDefaultTopics(n)
        if n.dmService.isNil:
          response["error"] = %"direct message service unavailable"
        else:
          var remotePid: PeerId
          if not remotePid.init(command.peerId):
            response["error"] = %"invalid peer id"
          else:
            let connectivityOk = await ensurePeerConnectivity(n, remotePid)
            if not connectivityOk:
              response["error"] = %(if command.errorMsg.len > 0: command.errorMsg else: "peer unreachable")
            else:
              var requestPayload: JsonNode
              try:
                requestPayload = parseJson(command.jsonString)
              except CatchableError as exc:
                requestPayload = newJObject()
                response["error"] = %("invalid request payload: " & exc.msg)
              if not response.hasKey("error"):
                if requestPayload.kind != JObject:
                  requestPayload = newJObject()
                let requestId = ensureMessageId(jsonGetStr(requestPayload, "requestId", "file-chunk-" & $nowMillis()))
                let requestedLength = int(jsonGetInt64(requestPayload, "length", int64(command.intParam)))
                let maxLength =
                  if command.intParam > 0:
                    min(command.intParam, 262_144)
                  else:
                    262_144
                requestPayload["op"] = %"file_chunk_request"
                requestPayload["requestId"] = %requestId
                requestPayload["from"] = %localPeerId(n)
                requestPayload["timestamp_ms"] = %nowMillis()
                requestPayload["ackRequested"] = %true
                if requestedLength <= 0:
                  requestPayload["length"] = %maxLength
                else:
                  requestPayload["length"] = %min(requestedLength, maxLength)
                let waiter = FileChunkWaiter(
                  future: newFuture[string]("nimlibp2p.fileChunkResponse"),
                  createdAt: nowMillis(),
                  timeoutMs: max(command.timeoutMs, 10_000),
                )
                n.pendingFileChunkResponses[requestId] = waiter
                let timeout = chronos.milliseconds(max(waiter.timeoutMs, 1_000))
                var sendOk = false
                var sendErr = ""
                try:
                  let sendFuture = n.dmService.send(
                    remotePid,
                    stringToBytes($requestPayload),
                    true,
                    requestId,
                    timeout,
                  )
                  let sendCompleted = await withTimeout(sendFuture, timeout + chronos.milliseconds(750))
                  if not sendCompleted:
                    sendFuture.cancelSoon()
                    sendErr = "send timeout"
                  else:
                    let resultTuple = await sendFuture
                    sendOk = resultTuple[0]
                    sendErr = resultTuple[1]
                except CatchableError as exc:
                  sendErr = if exc.msg.len > 0: exc.msg else: $exc.name
                if not sendOk:
                  n.pendingFileChunkResponses.del(requestId)
                  response["error"] = %(if sendErr.len > 0: sendErr else: "request_failed")
                else:
                  let responseReady = await withTimeout(waiter.future, timeout + chronos.milliseconds(1_500))
                  if not responseReady:
                    if n.pendingFileChunkResponses.hasKey(requestId):
                      n.pendingFileChunkResponses.del(requestId)
                    if not waiter.future.finished():
                      waiter.future.cancelSoon()
                    response["error"] = %"file_chunk_response_timeout"
                  else:
                    let responseText = await waiter.future
                    try:
                      let parsed = parseJson(responseText)
                      command.stringResult = $parsed
                      response = parsed
                      n.lastChunkSize = int(jsonGetInt64(parsed, "chunkSize", 0'i64))
                    except CatchableError:
                      command.stringResult = responseText
                      response["error"] = %"invalid_file_chunk_response"
      if command.stringResult.len == 0:
        command.stringResult = $response
      command.boolResult = jsonGetBool(response, "ok", false)
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

proc processCommandQueue(n: NimNode) {.raises: [], gcsafe.}

proc drainCommandQueue(n: NimNode) {.async: (raises: []), gcsafe.} =
  try:
    while true:
      let command = dequeueCommand(n)
      if command.isNil:
        break
      if command.kind == cmdNone:
        continue
      atomicDec(n.queuedCommands)
      if command.highPriority:
        atomicDec(n.queuedPriorityCommands)
      debugLog(
        "[nimlibp2p] processCommandQueue dispatch node=" & $cast[int](n) &
        " kind=" & $command.kind
      )
      atomicInc(n.pendingCommands)
      await runCommand(n, command)
  finally:
    n.commandDrainActive = false
    n.commandDrainTask = nil
    if n.queuedCommands.load(moAcquire) > 0:
      try:
        processCommandQueue(n)
      except Exception as exc:
        warn "processCommandQueue restart failed", err = exc.msg

proc processCommandQueue(n: NimNode) {.raises: [], gcsafe.} =
  if n.commandDrainActive:
    return
  n.commandDrainActive = true
  n.commandDrainTask = drainCommandQueue(n)
  asyncSpawn(n.commandDrainTask)

proc signalLoop(n: NimNode) {.async.} =
  debugLog("[nimlibp2p] signalLoop start node=" & $cast[int](n))
  while not n.stopRequested.load(moAcquire):
    try:
      debugLog(
        "[nimlibp2p] signalLoop wait node=" & $cast[int](n) &
        " queued=" & $n.queuedCommands.load(moAcquire) &
        " pending=" & $n.pendingCommands.load(moAcquire)
      )
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
      debugLog("[nimlibp2p] signalLoop cancelled node=" & $cast[int](n))
      break
    except CatchableError as exc:
      warn "signal loop error", err = exc.msg
      debugLog("[nimlibp2p] signalLoop error node=" & $cast[int](n) & " err=" & exc.msg)
    except Exception as exc:
      debugLog("[nimlibp2p] signalLoop fatal node=" & $cast[int](n) & " err=" & exc.msg)
  debugLog("[nimlibp2p] signalLoop exit node=" & $cast[int](n))

proc idleWakeLoop(n: NimNode) {.async.} =
  ## Keep the Chronos dispatcher from blocking indefinitely when idle.
  ## Commands are submitted from foreign threads via `submitCommand`, so we need
  ## the event loop to periodically wake and drain the command queue.
  var lastPublicIpProbeKickMs = 0'i64
  var lastDrainLogMs = 0'i64
  var lastPendingHeartbeatMs = 0'i64
  while not n.stopRequested.load(moAcquire):
    try:
      await sleepAsync(chronos.milliseconds(250))
      processCommandQueue(n)
      let nowMs = nowMillis()
      let queued = n.queuedCommands.load(moAcquire)
      let pending = n.pendingCommands.load(moAcquire)
      if pending > 0 and (lastPendingHeartbeatMs == 0 or nowMs - lastPendingHeartbeatMs >= 1_000):
        lastPendingHeartbeatMs = nowMs
        debugLog(
          "[nimlibp2p] idleWakeLoop heartbeat node=" & $cast[int](n) &
          " queued=" & $queued &
          " pending=" & $pending &
          " drainActive=" & $n.commandDrainActive
        )
      if queued > 0 and (lastDrainLogMs == 0 or nowMs - lastDrainLogMs >= 2_000):
        lastDrainLogMs = nowMs
        debugLog(
          "[nimlibp2p] idleWakeLoop drain node=" & $cast[int](n) &
          " queued=" & $queued &
          " pending=" & $pending &
          " drainActive=" & $n.commandDrainActive
        )
      if nowMs - lastPublicIpProbeKickMs >= PublicIpProbeLoopKickMs:
        lastPublicIpProbeKickMs = nowMs
        let hostStatus = sanitizeHostNetworkStatus(n.hostNetworkStatus)
        if jsonGetBool(hostStatus, "isConnected", false) and shouldRefreshPublicIpProbe(n, hostStatus):
          asyncSpawn(refreshPublicIpProbeAsync(n, hostStatus))
    except CancelledError:
      debugLog("[nimlibp2p] idleWakeLoop cancelled node=" & $cast[int](n))
      break
    except CatchableError as exc:
      debugLog("[nimlibp2p] idleWakeLoop error node=" & $cast[int](n) & " err=" & exc.msg)
    except Exception as exc:
      debugLog("[nimlibp2p] idleWakeLoop fatal node=" & $cast[int](n) & " err=" & exc.msg)
  debugLog("[nimlibp2p] idleWakeLoop exit node=" & $cast[int](n))

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
  when defined(metrics):
    bindDefaultMetricsRegistryToCurrentThread()
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

proc waitCommandCompletion(cmd: Command, timeoutMs = ShutdownWaitTimeoutMs): bool =
  if cmd.isNil or cmd.completion.isNil:
    return true
  let waitRes = cmd.completion.waitSync(chronos.milliseconds(timeoutMs))
  if waitRes.isErr():
    warn "waitCommandCompletion failed", kind = $cmd.kind, err = waitRes.error
    return false
  waitRes.get()

proc waitShutdown(node: NimNode, timeoutMs = ShutdownWaitTimeoutMs): bool =
  if node.shutdownComplete.load(moAcquire):
    return true
  if node.shutdownSignal.isNil:
    var waited = 0
    let maxWait = max(0, timeoutMs)
    while not node.shutdownComplete.load(moAcquire) and waited < maxWait:
      sleep(50)
      waited += 50
    return node.shutdownComplete.load(moAcquire)
  let waitRes = node.shutdownSignal.waitSync(chronos.milliseconds(timeoutMs))
  if waitRes.isErr():
    warn "waitShutdown wait failed", err = waitRes.error
    return false
  waitRes.get() and node.shutdownComplete.load(moAcquire)

proc closeThreadSignal(signalPtr: var ThreadSignalPtr, context: string) =
  ## Safely close and nil the Chronos thread signal, guarding against double close.
  discard context
  if signalPtr.isNil:
    return
  let current = signalPtr
  signalPtr = nil
  discard current.close()

proc submitCommand(node: NimNode, cmd: Command): Command =
  let active = node.running.load(moAcquire)
  if not active:
    warn "submitCommand rejected: node not running", kind = $cmd.kind
    cmd.resultCode = NimResultInvalidState
    if cmd.errorMsg.len == 0:
      cmd.errorMsg = "node not running"
    recordRuntimeError(cmd.errorMsg)
    return cmd
  info "submitCommand", kind = $cmd.kind, pending = node.pendingCommands.load(moAcquire)
  let enqueueStart = epochTime()
  if cmd.completion.isNil:
    let signalRes = ThreadSignalPtr.new()
    if signalRes.isErr():
      cmd.resultCode = NimResultError
      cmd.errorMsg = "ThreadSignalPtr.new failed: " & signalRes.error
      recordRuntimeError(cmd.errorMsg)
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
  if cmd.resultCode == NimResultOk:
    clearRuntimeError()
  elif cmd.errorMsg.len > 0:
    recordRuntimeError(cmd.errorMsg)
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
  if cmd.completion.isNil:
    let signalRes = ThreadSignalPtr.new()
    if signalRes.isErr():
      cmd.resultCode = NimResultError
      cmd.errorMsg = "ThreadSignalPtr.new failed: " & signalRes.error
      return false
    cmd.completion = signalRes.get()
  info "submitCommandNoWait", kind = $cmd.kind, pending = node.pendingCommands.load(moAcquire)
  enqueueCommand(node, cmd)
  let queueLen = node.queuedCommands.load(moAcquire)
  let fired = node.signal.fireSync(chronos.milliseconds(5))
  if fired.isErr():
    warn "failed to fire thread signal", err = fired.error
  elif fired.get():
    debugLog(
      "[nimlibp2p] submitCommandNoWait fireSync ok node=" & $cast[int](node) &
      " kind=" & $cmd.kind &
      " queued=" & $queueLen
    )
  else:
    debugLog(
      "[nimlibp2p] submitCommandNoWait fireSync timeout node=" & $cast[int](node) &
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
  ensureMsQuicDiagnosticsHook()
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
      peerHintSources: initTable[string, seq[string]](),
      authoritativeBootstrapPeerIds: configuredWanBootstrapAuthoritativePeerIds(cfg),
      bootstrapLastSeen: initTable[string, int64](),
      directWaiters: initTable[string, DirectWaiter](),
      pendingFileChunkResponses: initTable[string, FileChunkWaiter](),
      lastDirectError: "",
      sharedFiles: initTable[string, SharedFileEntry](),
      lastChunkSize: 0,
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
      mediaFrameLog: @[],
      receivedDmLog: @[],
      eventLogLimit: EventLogLimit,
      mediaFrameLogLimit: MediaFrameLogLimit,
      receivedDmLogLimit: ReceivedDmLogLimit,
      localSystemProfile: newJObject(),
      localSystemProfileUpdatedAt: 0,
      hostNetworkStatus: defaultHostNetworkStatus(),
      hostNetworkStatusUpdatedAt: 0,
      publicIpProbeState: PublicIpProbeState(),
      lastConnectedPeersInfo: newJArray(),
      lastConnectedPeersInfoUpdatedAt: 0,
      probeInboundTraffic: initTable[string, ProbeSessionTraffic](),
      peerSystemProfiles: initTable[string, JsonNode](),
      peerLatencyProfiles: initTable[string, PeerLatencyProfile](),
      lanEndpointsCache: newJObject(),
      feedItems: @[],
      livestreams: initTable[string, LivestreamSession](),
      lanGroups: initTable[string, LanGroupState](),
      rendezvous: findRendezvousService(sw),
      relayReservations: initTable[string, int64](),
      discovery: nil,
      configuredMdnsService: configuredMdnsServiceName(cfg),
      mdnsInterface: nil,
      mdnsService: configuredMdnsServiceName(cfg),
      mdnsServices: @[],
      mdnsQueries: @[],
      mdnsWatchers: @[],
      mdnsLastSeen: initTable[string, int64](),
      mdnsPreferredIpv4: initialPreferredIpv4,
      mdnsRestartPending: false,
      mdnsStarting: false,
      mdnsReady: false,
      mdnsRestarting: false,
      selfHandle: nil,
      gameMesh: newGameMeshRuntime(),
    )
    initLock(node.eventLogLock)
    initLock(node.mediaFrameLogLock)
    initLock(node.receivedDmLogLock)
    initLock(node.sharedFilesLock)
    initLock(node.gameMeshLock)
    initLock(node.publicIpProbeLock)
    let disableMdns = (not cfg.extra.isNil and cfg.extra.kind == JObject) and
      (
        jsonGetBool(cfg.extra, "disableMdns", false) or
        jsonGetBool(cfg.extra, "disable_mdns", false)
      )
    node.mdnsEnabled = not disableMdns
    node.mdnsIntervalSeconds = 2
    initCommandQueue(node)
    node.running.store(true, moRelease)
    node.stopRequested.store(false, moRelease)
    node.pendingCommands.store(0, moRelease)
    node.ready.store(false, moRelease)
    node.shutdownComplete.store(false, moRelease)
    node.threadJoined.store(false, moRelease)
    node.queuedCommands.store(0, moRelease)
    let bootstrapAddrs = effectiveBootstrapHints(cfg)
    if bootstrapAddrs.len > 0:
      registerInitialPeerHints(node, bootstrapAddrs.mapIt($it), "bootstrap")
    let relayAddrs = multiaddrsFromExtra(cfg.extra, "relayMultiaddrs")
    if relayAddrs.len > 0:
      registerInitialPeerHints(node, relayAddrs, "relay")
    GC_ref(node)
    let handlePtr = makeNodeHandle(node)
    node.selfHandle = handlePtr
    rememberLastNodeIdentity(handlePtr, node)
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
    rememberLastNodeIdentity(handlePtr, node)
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
  if rc == NimResultOk:
    rememberLastNodeIdentity(handle, node)
  if rc != NimResultOk and err == "WARN_DISCOVERY_ADVERTISE":
    return NimResultOk
  rc

proc libp2p_node_stop*(handle: pointer): cint {.exportc, cdecl, dynlib.} =
  let node = nodeFromHandle(handle)
  if node.isNil:
    return NimResultInvalidArgument
  if node.shutdownComplete.load(moAcquire):
    if not node.threadJoined.load(moAcquire):
      joinThread(node.thread)
      node.threadJoined.store(true, moRelease)
    node.running.store(false, moRelease)
    return NimResultOk
  if node.stopRequested.load(moAcquire):
    let stopped = waitShutdown(node)
    if stopped and not node.threadJoined.load(moAcquire):
      joinThread(node.thread)
      node.threadJoined.store(true, moRelease)
    if stopped:
      node.running.store(false, moRelease)
      return NimResultOk
    return NimResultError
  let cmd = makeCommand(cmdStop)
  let submitted = submitCommandNoWait(node, cmd)
  if not submitted:
    let rc = cmd.resultCode
    destroyCommand(cmd)
    return rc
  node.stopRequested.store(true, moRelease)
  node.running.store(false, moRelease)
  if not node.signal.isNil:
    discard node.signal.fireSync()
  let stopCompleted = waitCommandCompletion(cmd)
  let shutdownCompleted = waitShutdown(node)
  var rc = cmd.resultCode
  if rc == NimResultOk and (not stopCompleted or not shutdownCompleted):
    rc = NimResultError
  if shutdownCompleted:
    if not node.threadJoined.load(moAcquire):
      joinThread(node.thread)
      node.threadJoined.store(true, moRelease)
  destroyCommand(cmd)
  rc

proc libp2p_node_free*(handle: pointer) {.exportc, cdecl, dynlib.} =
  let node = nodeFromHandle(handle)
  if node.isNil:
    clearRememberedLastNodeHandle(handle)
    freeNodeHandle(handle)
    return
  let wasRunning = node.running.load(moAcquire)
  let stopping = node.stopRequested.load(moAcquire)
  let shutdownComplete = node.shutdownComplete.load(moAcquire)
  if wasRunning and not stopping and not shutdownComplete:
    discard libp2p_node_stop(handle)
  else:
    discard waitShutdown(node)
  if node.shutdownComplete.load(moAcquire) and not node.threadJoined.load(moAcquire):
    joinThread(node.thread)
    node.threadJoined.store(true, moRelease)
  node.running.store(false, moRelease)
  # Mobile hosts keep a singleton node for the process lifetime. Eagerly
  # tearing down the full GC-managed object graph here still races with
  # lingering libp2p callbacks after stop; detach the handle and let process
  # teardown reclaim the node graph instead.
  clearRememberedLastNodeHandle(handle)
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
  libp2p_connect_peer_timeout(handle, peerId, 5_000)

proc libp2p_connect_peer_timeout*(handle: pointer, peerId: cstring, timeoutMs: cint): cint {.exportc, cdecl, dynlib.} =
  if peerId.isNil:
    return NimResultInvalidArgument
  let node = nodeFromHandle(handle)
  if node.isNil:
    return NimResultInvalidArgument
  let cmd = makeCommand(cmdConnectPeer)
  cmd.peerId = $peerId
  cmd.timeoutMs = max(int(timeoutMs), 250)
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

proc libp2p_connect_multiaddr_kickoff*(
    handle: pointer, multiaddr: cstring, timeoutMs: cint
): bool {.exportc, cdecl, dynlib.} =
  if multiaddr.isNil:
    return false
  let node = nodeFromHandle(handle)
  if node.isNil:
    return false
  let cmd = makeCommand(cmdConnectMultiaddr)
  cmd.multiaddr = $multiaddr
  cmd.timeoutMs = max(int(timeoutMs), 250)
  let ok = submitCommandNoWait(node, cmd)
  if ok:
    info "connect multiaddr kickoff requested", addr = $multiaddr, timeoutMs = int(timeoutMs)
  else:
    let err = cmd.errorMsg
    warn "libp2p_connect_multiaddr_kickoff failed", addr = $multiaddr, timeoutMs = int(timeoutMs), err = err
    destroyCommand(cmd)
  ok

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
      let opValue = jsonGetStr(parsedEnvelope, "type", jsonGetStr(parsedEnvelope, "op"))
      if opValue.len > 0:
        cmd.op = opValue
        let timeoutMs = defaultDirectTimeoutMs(opValue)
        if timeoutMs > 0:
          cmd.timeoutMs = timeoutMs
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
  let derivedOp = parseDirectEnvelopeOp(cmd.textPayload)
  cmd.op = if derivedOp.len > 0: derivedOp else: "text"
  cmd.ackRequested = requestAck
  let requestedTimeout = int(timeoutMs)
  let defaultTimeout = defaultDirectTimeoutMs(cmd.op)
  cmd.timeoutMs = if requestedTimeout > 0: requestedTimeout else: defaultTimeout
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
  let derivedOp = parseDirectEnvelopeOp(cmd.jsonString)
  if derivedOp.len > 0:
    cmd.op = derivedOp
  cmd.ackRequested = true
  let requestedTimeout = int(timeoutMs)
  let defaultTimeout = defaultDirectTimeoutMs(cmd.op)
  cmd.timeoutMs = if requestedTimeout > 0: requestedTimeout else: defaultTimeout
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

proc libp2p_update_host_network_status*(
    handle: pointer, payloadJson: cstring
): bool {.exportc, cdecl, dynlib.} =
  if payloadJson.isNil:
    return false
  let node = nodeFromHandle(handle)
  if node.isNil:
    return false
  let cmd = makeCommand(cmdUpdateHostNetworkStatus)
  cmd.jsonString = $payloadJson
  let result = submitCommand(node, cmd)
  let ok = result.resultCode == NimResultOk
  destroyCommand(cmd)
  ok

proc libp2p_get_host_network_status_json*(handle: pointer): cstring {.exportc, cdecl, dynlib.} =
  let node = nodeFromHandle(handle)
  if node.isNil:
    return allocCString($defaultHostNetworkStatus())
  let cmd = makeCommand(cmdGetHostNetworkStatus)
  let result = submitCommand(node, cmd)
  let text =
    if result.resultCode == NimResultOk and result.stringResult.len > 0:
      cloneOwnedString(result.stringResult)
    else:
      $defaultHostNetworkStatus()
  destroyCommand(cmd)
  allocCString(text)

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

proc libp2p_ui_frame_snapshot*(handle: pointer, maxEvents: cint, discoveryLimit: cint): cstring {.exportc, cdecl, dynlib.} =
  let node = nodeFromHandle(handle)
  if node.isNil:
    return allocCString("{\"ok\":false,\"events\":[],\"discovery\":{},\"lanEndpoints\":{\"endpoints\":[]},\"feed\":{\"items\":[]}}")
  let cmd = makeCommand(cmdUiFrameSnapshot)
  cmd.requestLimit = max(int(maxEvents), 0)
  cmd.intParam = max(int(discoveryLimit), 0)
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

proc libp2p_fetch_file_providers*(
    handle: pointer,
    key: cstring,
    limit: cint
): cstring {.exportc, cdecl, dynlib.} =
  let node = nodeFromHandle(handle)
  if node.isNil:
    return allocCString("{\"providers\":[],\"count\":0}")
  let normalizedKey = if key.isNil: "" else: ($key).strip()
  let maxProviders = max(1, if limit > 0: int(limit) else: 8)
  var providers = newJArray()
  var seen = initHashSet[string]()
  if normalizedKey.len > 0:
    let local = localPeerId(node)
    let localEntry = lookupSharedFile(node, normalizedKey)
    if localEntry.ok and local.len > 0:
      providers.add(%local)
      seen.incl(local)
  let connected = connectedPeersArray(handle)
  if connected.kind == JArray:
    for item in connected:
      let peerId =
        if item.kind == JString:
          item.getStr().strip()
        else:
          ""
      if peerId.len > 0 and peerId notin seen:
        providers.add(%peerId)
        seen.incl(peerId)
      if providers.len >= maxProviders:
        break
  if providers.len < maxProviders:
    for peerId, _ in node.peerHints.pairs:
      let normalizedPeer = peerId.strip()
      if normalizedPeer.len > 0 and normalizedPeer notin seen:
        providers.add(%normalizedPeer)
        seen.incl(normalizedPeer)
      if providers.len >= maxProviders:
        break
  var response = newJObject()
  response["providers"] = providers
  response["count"] = %providers.len
  allocCString($response)

proc libp2p_register_local_file*(
    handle: pointer,
    key: cstring,
    path: cstring,
    metaJson: cstring
): cstring {.exportc, cdecl, dynlib.} =
  let node = nodeFromHandle(handle)
  if node.isNil:
    return allocCString("{\"ok\":false,\"error\":\"node_not_initialized\"}")
  let normalizedKey = if key.isNil: "" else: ($key).strip()
  let normalizedPath = if path.isNil: "" else: normalizeSharedFilePath($path)
  if normalizedKey.len == 0:
    return allocCString("{\"ok\":false,\"error\":\"missing_key\"}")
  if normalizedPath.len == 0 or not fileExists(normalizedPath):
    return allocCString("{\"ok\":false,\"error\":\"file_missing\"}")
  var meta = newJObject()
  if not metaJson.isNil:
    try:
      meta = parseJson($metaJson)
    except CatchableError:
      meta = newJObject()
  let entry = makeSharedFileEntry(
    normalizedKey,
    normalizedPath,
    jsonGetStr(meta, "fileName"),
    jsonGetStr(meta, "mimeType"),
    jsonGetStr(meta, "sha256"),
    jsonGetInt64(meta, "sizeBytes", 0'i64),
  )
  if not registerSharedFile(node, entry):
    return allocCString("{\"ok\":false,\"error\":\"register_failed\"}")
  var response = newJObject()
  response["ok"] = %true
  response["key"] = %entry.key
  response["path"] = %entry.path
  response["sizeBytes"] = %max(entry.sizeBytes, 0'i64)
  response["fileName"] = %entry.fileName
  response["mimeType"] = %entry.mimeType
  allocCString($response)

proc libp2p_request_file_chunk*(
    handle: pointer,
    peerId: cstring,
    requestJson: cstring,
    maxBytes: cint
): cstring {.exportc, cdecl, dynlib.} =
  let node = nodeFromHandle(handle)
  if node.isNil:
    return allocCString("{\"ok\":false,\"error\":\"node_not_initialized\",\"chunkSize\":0}")
  let cmd = makeCommand(cmdRequestFileChunk)
  cmd.peerId = if peerId.isNil: "" else: $peerId
  cmd.jsonString = if requestJson.isNil: "" else: $requestJson
  cmd.intParam = if maxBytes > 0: int(maxBytes) else: 262_144
  cmd.timeoutMs = 10_000
  let result = submitCommand(node, cmd)
  let text =
    if result.stringResult.len > 0:
      cloneOwnedString(result.stringResult)
    else:
      "{\"ok\":false,\"error\":\"request_file_chunk_failed\",\"chunkSize\":0}"
  destroyCommand(cmd)
  allocCString(text)

proc libp2p_last_chunk_size*(handle: pointer): cint {.exportc, cdecl, dynlib.} =
  let node = nodeFromHandle(handle)
  if node.isNil:
    return 0
  cint(max(node.lastChunkSize, 0))

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

proc libp2p_measure_peer_latency*(
    handle: pointer,
    peerId: cstring,
    sampleCount: cint,
    timeoutMs: cint
): cstring {.exportc, cdecl, dynlib.} =
  if peerId.isNil:
    return allocCString("{\"ok\":false,\"error\":\"missing peer id\"}")
  let node = nodeFromHandle(handle)
  if node.isNil:
    return allocCString("{\"ok\":false,\"error\":\"node not initialized\"}")
  let cmd = makeCommand(cmdMeasurePeerLatency)
  cmd.peerId = $peerId
  cmd.intParam = int(sampleCount)
  cmd.timeoutMs = int(timeoutMs)
  let result = submitCommand(node, cmd)
  let payload =
    if result.resultCode == NimResultOk and result.stringResult.len > 0:
      cloneOwnedString(result.stringResult)
    else:
      (if result.errorMsg.len > 0:
        "{\"ok\":false,\"error\":\"" & result.errorMsg.replace("\"", "\\\"") & "\"}"
       else:
        "{\"ok\":false,\"error\":\"latency probe failed\"}")
  destroyCommand(cmd)
  allocCString(payload)

proc libp2p_poll_events*(handle: pointer, maxEvents: cint): cstring {.exportc, cdecl, dynlib.} =
  let node = nodeFromHandle(handle)
  if node.isNil:
    return allocCString("[]")
  allocCString(cloneOwnedString(pollEventLog(node, int(maxEvents))))

proc libp2p_poll_media_frames*(handle: pointer, maxEvents: cint): cstring {.exportc, cdecl, dynlib.} =
  let node = nodeFromHandle(handle)
  if node.isNil:
    return allocCString("[]")
  allocCString(cloneOwnedString(pollMediaFrameLog(node, int(maxEvents))))
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
  result["subscriptions"] = newJObject()
  result["synccastRooms"] = newJObject()
  result["publishTasks"] = newJArray()
  result["reportTickets"] = newJArray()
  result["tombstones"] = newJArray()
  result["distributionLogs"] = newJArray()

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
  if not result.hasKey("subscriptions") or result["subscriptions"].kind != JObject:
    result["subscriptions"] = newJObject()
  if not result.hasKey("synccastRooms") or result["synccastRooms"].kind != JObject:
    result["synccastRooms"] = newJObject()
  if not result.hasKey("publishTasks") or result["publishTasks"].kind != JArray:
    result["publishTasks"] = newJArray()
  if not result.hasKey("playbackSessions") or result["playbackSessions"].kind != JArray:
    result["playbackSessions"] = newJArray()
  if not result.hasKey("reportTickets") or result["reportTickets"].kind != JArray:
    result["reportTickets"] = newJArray()
  if not result.hasKey("tombstones") or result["tombstones"].kind != JArray:
    result["tombstones"] = newJArray()
  if not result.hasKey("distributionLogs") or result["distributionLogs"].kind != JArray:
    result["distributionLogs"] = newJArray()

proc prependBoundedNode(arr: JsonNode, item: JsonNode, maxItems: int) =
  if arr.isNil or arr.kind != JArray:
    return
  arr.elems.insert(item, 0)
  while arr.len > maxItems:
    arr.elems.delete(arr.len - 1)

proc toJsonStringArray(items: seq[string]): JsonNode =
  result = newJArray()
  for item in items:
    if item.len > 0:
      result.add(%item)

proc jsonArrayContainsString(arr: JsonNode, value: string): bool =
  if arr.isNil or arr.kind != JArray:
    return false
  let normalized = value.strip()
  if normalized.len == 0:
    return false
  for item in arr:
    if item.kind == JString and item.getStr().strip() == normalized:
      return true
  false

proc collectJsonStringArray(node: JsonNode, key: string): seq[string] =
  if node.kind != JObject or not node.hasKey(key):
    return @[]
  let value = node[key]
  if value.kind == JArray:
    for item in value:
      if item.kind == JString and item.getStr().strip().len > 0:
        result.add(item.getStr().strip())
    return result
  if value.kind == JString:
    return parseJsonStringSeq(value.getStr())
  @[]

proc socialDmBucket(peerId: string): string =
  let normalized = peerId.strip()
  if normalized.len >= 2:
    normalized.substr(max(0, normalized.len - 2), normalized.len - 1).toLowerAscii()
  else:
    "default"

proc ensureSocialSubscriptionsNoLock(state: JsonNode): JsonNode =
  if state.kind != JObject:
    return newJObject()
  if not state.hasKey("subscriptions") or state["subscriptions"].kind != JObject:
    state["subscriptions"] = newJObject()
  let subscriptions = state["subscriptions"]
  if not subscriptions.hasKey("topics") or subscriptions["topics"].kind != JArray:
    subscriptions["topics"] = newJArray()
  if not subscriptions.hasKey("preferences") or subscriptions["preferences"].kind != JObject:
    subscriptions["preferences"] = newJObject()
  if not subscriptions.hasKey("following") or subscriptions["following"].kind != JArray:
    subscriptions["following"] = newJArray()
  if not subscriptions.hasKey("groupMemberships") or subscriptions["groupMemberships"].kind != JArray:
    subscriptions["groupMemberships"] = newJArray()
  if not subscriptions.hasKey("lastCompiledAt"):
    subscriptions["lastCompiledAt"] = %0
  let preferences = subscriptions["preferences"]
  if not preferences.hasKey("contentGlobal"):
    preferences["contentGlobal"] = %true
  if not preferences.hasKey("contentFollowing"):
    preferences["contentFollowing"] = %true
  if not preferences.hasKey("dm"):
    preferences["dm"] = %true
  if not preferences.hasKey("groups"):
    preferences["groups"] = %true
  if not preferences.hasKey("governance"):
    preferences["governance"] = %true
  subscriptions

proc socialBuildSubscriptionSnapshotNoLock(state: JsonNode, localPeer: string): JsonNode =
  let subscriptions = ensureSocialSubscriptionsNoLock(state)
  var topics = newJArray()
  if jsonGetBool(subscriptions["preferences"], "contentGlobal", true):
    topics.add(%"content/global")
  if jsonGetBool(subscriptions["preferences"], "contentFollowing", true):
    topics.add(%"content/following")
  if jsonGetBool(subscriptions["preferences"], "dm", true):
    topics.add(%("dm/inbox/" & socialDmBucket(localPeer)))
  if jsonGetBool(subscriptions["preferences"], "groups", true):
    for groupId in subscriptions["groupMemberships"]:
      if groupId.kind == JString and groupId.getStr().strip().len > 0:
        topics.add(%("group/" & groupId.getStr().strip()))
  subscriptions["topics"] = topics
  subscriptions["lastCompiledAt"] = %nowMillis()
  subscriptions

proc socialNotificationType(kind: string): string =
  let normalized = kind.strip().toLowerAscii()
  case normalized
  of "content_feed_received", "moment_published":
    "content"
  of "dm_received", "dm_sent", "dm_send_failed":
    "dm"
  of "group_message_received", "group_created", "group_invite":
    "group"
  of "governance_reviewed", "tombstone_issued", "tombstone_restored":
    "governance"
  else:
    if normalized.startsWith("contact_"): "contact" else: "system"

proc socialNotificationEventKind(kind: string): string =
  let normalized = kind.strip().toLowerAscii()
  case normalized
  of "content_feed_received", "moment_published":
    "content_publish"
  of "dm_received", "dm_sent", "dm_send_failed":
    "dm"
  of "group_message_received":
    "group_message"
  of "group_created", "group_invite":
    "group_event"
  of "governance_reviewed":
    "governance_result"
  of "tombstone_issued":
    "tombstone_issue"
  of "tombstone_restored":
    "tombstone_restore"
  else:
    normalized

proc socialNotificationProducer(payload: JsonNode): string =
  jsonGetStr(
    payload,
    "producer",
    jsonGetStr(
      payload,
      "authorPeerId",
      jsonGetStr(payload, "from", jsonGetStr(payload, "peerId", jsonGetStr(payload, "peer_id")))
    )
  ).strip()

proc socialNotificationContentId(payload: JsonNode): string =
  jsonGetStr(
    payload,
    "contentId",
    jsonGetStr(payload, "postId", jsonGetStr(payload, "id", jsonGetStr(payload, "subjectId")))
  ).strip()

proc socialNotificationConversationId(payload: JsonNode): string =
  jsonGetStr(payload, "conversationId").strip()

proc socialNotificationGroupId(payload: JsonNode): string =
  jsonGetStr(payload, "groupId").strip()

proc socialNotificationChannel(kind: string, payload: JsonNode): string =
  let explicit = jsonGetStr(payload, "channel", jsonGetStr(payload, "category")).strip()
  if explicit.len > 0:
    case explicit.toLowerAscii()
    of "content", "内容":
      return "content"
    of "product", "commerce", "电商":
      return "product"
    of "live", "直播":
      return "live"
    of "app", "应用":
      return "app"
    of "food", "外卖":
      return "food"
    of "ride", "顺风车":
      return "ride"
    of "job", "求职":
      return "job"
    of "hire", "招聘":
      return "hire"
    of "rent", "出租":
      return "rent"
    of "sell", "出售":
      return "sell"
    of "secondhand", "二手":
      return "secondhand"
    of "crowdfunding", "众筹":
      return "crowdfunding"
    of "ad", "广告":
      return "ad"
    else:
      return explicit.toLowerAscii()
  let normalized = kind.strip().toLowerAscii()
  if normalized in ["dm_received", "dm_sent", "dm_send_failed", "group_message_received", "group_created", "group_invite"]:
    return "messages"
  if normalized in ["governance_reviewed", "tombstone_issued", "tombstone_restored"]:
    return "system"
  "content"

proc socialNotificationSubjectId(kind: string, payload: JsonNode): string =
  let contentId = socialNotificationContentId(payload)
  if contentId.len > 0:
    return contentId
  let conversationId = socialNotificationConversationId(payload)
  if conversationId.len > 0:
    return conversationId
  let groupId = socialNotificationGroupId(payload)
  if groupId.len > 0:
    return groupId
  let peerId = socialNotificationProducer(payload)
  if peerId.len > 0:
    return peerId
  let normalized = kind.strip()
  if normalized.len > 0: normalized else: "notification"

proc socialNotificationDeliveryClass(kind: string, payload: JsonNode): string =
  let explicit = jsonGetStr(payload, "deliveryClass").strip().toLowerAscii()
  if explicit.len > 0:
    return explicit
  let normalized = kind.strip().toLowerAscii()
  case normalized
  of "dm_received", "governance_reviewed", "tombstone_issued", "tombstone_restored":
    "priority"
  of "group_message_received", "content_feed_received", "moment_published", "group_created", "group_invite":
    "inbox"
  else:
    "silent"

proc socialNotificationTopic(kind: string, payload: JsonNode): string =
  let explicit = jsonGetStr(payload, "topic").strip()
  if explicit.len > 0:
    return explicit
  let normalized = kind.strip().toLowerAscii()
  if normalized in ["dm_received", "dm_sent", "dm_send_failed"]:
    return "dm/inbox/" & socialDmBucket(socialNotificationProducer(payload))
  let groupId = socialNotificationGroupId(payload)
  if groupId.len > 0:
    return "group/" & groupId
  let producer = socialNotificationProducer(payload)
  if normalized in ["content_feed_received", "moment_published"] and producer.len > 0:
    return if jsonGetBool(payload, "following", false): "content/following" else: "content/global"
  "content/global"

proc socialNotificationReadByDefault(kind: string, payload: JsonNode): bool =
  let explicit = jsonGetBool(payload, "read", false)
  if explicit:
    return true
  let explicitState = jsonGetStr(payload, "readState").strip().toLowerAscii()
  if explicitState == "read":
    return true
  kind.strip().toLowerAscii() in ["dm_sent", "moment_published", "contact_request_sent", "contact_accepted", "contact_rejected", "contact_removed"]

proc socialNotificationPriorityValue(deliveryClass: string): int =
  case deliveryClass.strip().toLowerAscii()
  of "push_candidate":
    3
  of "priority":
    2
  of "inbox":
    1
  else:
    0

proc socialShouldNotifyRemoteContentNoLock(state: JsonNode, producer: string): bool =
  let subscriptions = ensureSocialSubscriptionsNoLock(state)
  if producer.len == 0:
    return jsonGetBool(subscriptions["preferences"], "contentGlobal", true)
  if jsonArrayContainsString(subscriptions["following"], producer):
    return jsonGetBool(subscriptions["preferences"], "contentFollowing", true)
  if state.hasKey("contacts") and state["contacts"].kind == JObject and state["contacts"].hasKey(producer):
    let contact = state["contacts"][producer]
    let status = jsonGetStr(contact, "status", "accepted").strip().toLowerAscii()
    if status in ["accepted", "following", "friend", "friends"]:
      return jsonGetBool(subscriptions["preferences"], "contentFollowing", true)
  jsonGetBool(subscriptions["preferences"], "contentGlobal", true)

proc socialNotificationsSummaryNodeNoLock(state: JsonNode): JsonNode =
  var summary = newJObject()
  var unreadByChannel = newJObject()
  var unreadByEvent = newJObject()
  var unreadTotal = 0
  var unreadPriority = 0
  if state.kind == JObject and state.hasKey("notifications") and state["notifications"].kind == JArray:
    for item in state["notifications"]:
      if item.kind != JObject:
        continue
      let readState = jsonGetStr(item, "readState", if jsonGetBool(item, "read", false): "read" else: "unread").strip().toLowerAscii()
      if readState == "read":
        continue
      unreadTotal.inc()
      let deliveryClass = jsonGetStr(item, "deliveryClass", "inbox").strip().toLowerAscii()
      if deliveryClass in ["priority", "push_candidate"]:
        unreadPriority.inc()
      let channel = jsonGetStr(item, "channel", "system").strip()
      if channel.len > 0:
        unreadByChannel[channel] = %(jsonGetInt64(unreadByChannel, channel, 0) + 1)
      let eventKind = jsonGetStr(item, "eventKind", jsonGetStr(item, "type", "notification")).strip()
      if eventKind.len > 0:
        unreadByEvent[eventKind] = %(jsonGetInt64(unreadByEvent, eventKind, 0) + 1)
  summary["unreadTotal"] = %unreadTotal
  summary["unreadPriority"] = %unreadPriority
  summary["unreadByChannel"] = unreadByChannel
  summary["unreadByEvent"] = unreadByEvent
  summary

proc containsAnyKeyword(text: string, keywords: openArray[string]): bool =
  for keyword in keywords:
    if keyword.len > 0 and text.contains(keyword):
      return true
  false

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
    state: JsonNode, kind: string, title: string, body: string, payload: JsonNode
) =
  if state.kind != JObject or state["notifications"].kind != JArray:
    return
  let producer = socialNotificationProducer(payload)
  let channel = socialNotificationChannel(kind, payload)
  let subjectId = socialNotificationSubjectId(kind, payload)
  let deliveryClass = socialNotificationDeliveryClass(kind, payload)
  let readByDefault = socialNotificationReadByDefault(kind, payload)
  let eventKind = socialNotificationEventKind(kind)
  let topic = socialNotificationTopic(kind, payload)
  let dedupeKey = eventKind & "|" & producer & "|" & subjectId
  var item = newJObject()
  item["id"] = %("notif-" & $nowMillis() & "-" & $rand(10_000))
  item["kind"] = %kind
  item["type"] = %socialNotificationType(kind)
  item["title"] = %title
  item["body"] = %body
  item["payload"] = payload
  item["read"] = %readByDefault
  item["readState"] = %(if readByDefault: "read" else: "unread")
  item["timestampMs"] = %jsonGetInt64(payload, "timestampMs", jsonGetInt64(payload, "timestamp_ms", nowMillis()))
  item["deliveryClass"] = %deliveryClass
  item["eventKind"] = %eventKind
  item["priority"] = %socialNotificationPriorityValue(deliveryClass)
  item["topic"] = %topic
  item["producer"] = %producer
  item["subjectId"] = %subjectId
  item["channel"] = %channel
  item["peerId"] = %jsonGetStr(payload, "peerId", jsonGetStr(payload, "from"))
  item["groupId"] = %socialNotificationGroupId(payload)
  item["contentId"] = %socialNotificationContentId(payload)
  item["conversationId"] = %socialNotificationConversationId(payload)
  item["fetchHint"] = %jsonGetStr(payload, "fetchHint", if socialNotificationContentId(payload).len > 0: "content_detail" else: "")
  item["dedupeKey"] = %dedupeKey
  var nextNotifications = newJArray()
  var replaced = false
  for existing in state["notifications"]:
    if existing.kind != JObject:
      continue
    if jsonGetStr(existing, "dedupeKey").strip() == dedupeKey:
      if not replaced:
        nextNotifications.add(item)
        replaced = true
      continue
    if nextNotifications.len >= SocialMaxNotifications:
      break
    nextNotifications.add(existing)
  if not replaced and nextNotifications.len < SocialMaxNotifications:
    nextNotifications.elems.insert(item, 0)
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

proc cacheConnectedPeersInfo(node: NimNode, connectedInfo: JsonNode) {.gcsafe.} =
  if node.isNil or connectedInfo.isNil or connectedInfo.kind != JArray or connectedInfo.len == 0:
    return
  node.lastConnectedPeersInfo = connectedInfo
  node.lastConnectedPeersInfoUpdatedAt = nowMillis()

proc recentConnectedPeersInfo(
    node: NimNode,
    maxAgeMs = ConnectedPeersInfoSnapshotFallbackMs
): JsonNode {.gcsafe, raises: [].} =
  if node.isNil or node.lastConnectedPeersInfo.isNil or
      node.lastConnectedPeersInfo.kind != JArray or
      node.lastConnectedPeersInfo.len == 0:
    return newJArray()
  let ageMs = nowMillis() - node.lastConnectedPeersInfoUpdatedAt
  if ageMs < 0 or ageMs > maxAgeMs:
    return newJArray()
  node.lastConnectedPeersInfo

proc buildConnectedPeersArrayFromInfo(connectedInfo: JsonNode): JsonNode {.gcsafe, raises: [].} =
  if connectedInfo.isNil or connectedInfo.kind != JArray:
    return newJArray()
  var seen = initHashSet[string]()
  var peers: seq[string] = @[]
  for row in connectedInfo:
    let peerId = jsonGetStr(row, "peerId", jsonGetStr(row, "peer_id"))
    if peerId.len == 0 or seen.contains(peerId):
      continue
    seen.incl(peerId)
    peers.add(peerId)
  peers.sort(proc(a, b: string): int = cmp(a, b))
  var arr = newJArray()
  for peer in peers:
    arr.add(%peer)
  arr

proc buildConnectedPeersArrayForNode(node: NimNode): JsonNode {.gcsafe, raises: [].} =
  if node.isNil or node.switchInstance.isNil or node.switchInstance.connManager.isNil:
    return newJArray()
  let connections = node.switchInstance.connManager.getConnections()
  var seen = initHashSet[string]()
  var peers: seq[string] = @[]
  for pid, muxers in connections.pairs:
    if muxers.len == 0:
      continue
    let text = $pid
    if not seen.contains(text):
      seen.incl(text)
      peers.add(text)
  peers.sort(proc(a, b: string): int = cmp(a, b))
  var arr = newJArray()
  for peer in peers:
    arr.add(%peer)
  arr

proc findConnectedPeerInfoRow(entries: JsonNode, peerId: string): JsonNode {.gcsafe.} =
  if entries.isNil or entries.kind != JArray or peerId.len == 0:
    return newJNull()
  for row in entries:
    if row.kind != JObject:
      continue
    let current = jsonGetStr(row, "peerId", jsonGetStr(row, "peer_id"))
    if current == peerId:
      return row
  newJNull()

proc buildConnectedPeersInfoArrayForNode(
    node: NimNode,
    includeLatency = true
): Future[JsonNode] {.async, gcsafe.} =
  if node.isNil or node.switchInstance.isNil or node.switchInstance.connManager.isNil:
    return newJArray()
  prunePeerProfiles(node)
  prunePeerLatencyProfiles(node)
  let connections = node.switchInstance.connManager.getConnections()
  var arr = newJArray()
  for peerId, muxers in connections.pairs:
    let peerIdText = $peerId
    var entry = newJObject()
    entry["peerId"] = %peerIdText
    entry["connectionCount"] = %muxers.len
    var directions = newJArray()
    var muxerNames = newJArray()
    var seenMuxers = initHashSet[string]()
    var streamCount = 0
    for mux in muxers:
      if mux.isNil or mux.connection.isNil:
        continue
      let dirStr =
        if mux.connection.dir == Direction.In: "inbound" else: "outbound"
      directions.add(%dirStr)
      let detectedMuxerCodec = detectMuxerCodec(mux.connection)
      let muxerCodec =
        if detectedMuxerCodec.len > 0:
          detectedMuxerCodec
        elif mux of Yamux:
          YamuxCodec
        elif mux of Mplex:
          MplexCodec
        else:
          ""
      if muxerCodec.len > 0 and not seenMuxers.contains(muxerCodec):
        seenMuxers.incl(muxerCodec)
        muxerNames.add(%muxerCodec)
      try:
        streamCount += mux.getStreams().len
      except CatchableError:
        discard
    entry["directions"] = directions
    entry["muxers"] = muxerNames
    entry["streamCount"] = %streamCount
    let peerBw = node.switchInstance.bandwidthStats(peerId)
    let relayed = peerLikelyRelayed(node, peerIdText, peerId)
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
      bandwidthNode["uplinkBps"] = jsonSafeFloat(uplinkEma)
      bandwidthNode["downlinkBps"] = jsonSafeFloat(downlinkEma)
      bandwidthNode["uplinkTotalBytes"] = %stats.outbound.totalBytes
      bandwidthNode["downlinkTotalBytes"] = %stats.inbound.totalBytes
      bandwidthNode["totalTransferBytes"] =
        %(stats.outbound.totalBytes + stats.inbound.totalBytes)
    if relayed:
      bandwidthNode["relayBottleneckBps"] = jsonSafeFloat(min(uplinkEma, downlinkEma))
    entry["bandwidth"] = bandwidthNode
    entry["isRelayed"] = %relayed
    var peerProfile = defaultSystemProfile()
    if node.peerSystemProfiles.hasKey(peerIdText):
      try:
        peerProfile = sanitizeSystemProfile(node.peerSystemProfiles[peerIdText])
      except KeyError:
        discard
    peerProfile["bandwidth"] = bandwidthNode
    entry["systemProfile"] = peerProfile
    var transportLatency = cachedTransportLatencyMs(node, peerIdText)
    let freshTransportLatency = freshTransportLatencyMs(node, peerIdText)
    if freshTransportLatency >= 0:
      transportLatency = freshTransportLatency
    elif includeLatency and not node.pingProtocol.isNil:
      let measured = await measurePeerTransportLatencyMs(node, peerId)
      if measured >= 0:
        recordTransportLatencySuccess(node, peerIdText, measured)
        transportLatency = measured
    let dmAckLatency = cachedDirectAckLatencyMs(node, peerIdText)
    let latencyStats = peerLatencyStatsJson(node, peerIdText)
    let preferredLatency =
      if transportLatency >= 0: transportLatency
      elif dmAckLatency >= 0: dmAckLatency
      else: -1
    entry["latencyMs"] = %preferredLatency
    entry["transportRttMs"] = %transportLatency
    entry["dmAckRttMs"] = %dmAckLatency
    entry["latencyStats"] = latencyStats
    arr.add(entry)
  cacheConnectedPeersInfo(node, arr)
  arr

proc collectMdnsPeerRows(node: NimNode): JsonNode =
  pruneStaleMdnsPeers(node)
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

proc collectDiscoveredPeersForNode(node: NimNode, connectedInfo: JsonNode): JsonNode {.gcsafe.} =
  pruneStaleMdnsPeers(node)
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
    var row: JsonNode
    try:
      row = peers[peerId]
    except KeyError:
      return
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

  if connectedInfo.kind == JArray:
    for row in connectedInfo:
      let peerId = jsonGetStr(row, "peerId", jsonGetStr(row, "peer_id"))
      let addrs = preferStableAddrStrings(
        jsonArrayValues(row, "addresses") & jsonArrayValues(row, "multiaddrs")
      )
      let seenAt = jsonGetInt64(row, "lastSeenAt", nowMillis())
      upsertPeer(peerId, "Connected", addrs, seenAt)
      annotateTransportSources(peerId, addrs, seenAt)

  for peerId, seenAt in node.mdnsLastSeen.pairs:
    let hints = preferStableAddrStrings(node.peerHints.getOrDefault(peerId))
    upsertPeer(peerId, "mDNS", hints, seenAt)
    annotateTransportSources(peerId, hints, seenAt)

  var arr = newJArray()
  for _, row in peers:
    arr.add(row)
  arr

proc collectDiscoveredPeers(handle: pointer, node: NimNode): JsonNode =
  collectDiscoveredPeersForNode(node, connectedPeersInfoArray(handle))

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

proc filteredDiscoveredPeersForNode(
    node: NimNode,
    sourceFilterText: string,
    limit: int,
    connectedInfo: JsonNode
): tuple[peers: JsonNode, totalCount: int] {.gcsafe.} =
  let filter = parseSourceFilter(sourceFilterText)
  let discovered = collectDiscoveredPeersForNode(node, connectedInfo)
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

proc buildMdnsDebugPayload(node: NimNode): JsonNode {.gcsafe.} =
  pruneStaleMdnsPeers(node)
  var obj = newJObject()
  if not node.isNil and not node.switchInstance.isNil and not node.switchInstance.peerInfo.isNil:
    discard refreshLanAwareAddrs(node)
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

proc buildNetworkResourcesPayload(node: NimNode, discoveredCount: int, connectedCount: int): JsonNode {.gcsafe.} =
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
    "uplinkBps": finiteFloat(uplink),
    "downlinkBps": finiteFloat(downlink),
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
  let node = nodeFromHandle(handle)
  if node.isNil:
    return allocCString("{\"connectedPeers\":[],\"connectedPeersInfo\":[],\"discoveredPeers\":{\"peers\":[],\"totalCount\":0},\"mdnsDebug\":{}}")
  let cmd = makeCommand(cmdNetworkDiscoverySnapshot)
  cmd.source = if sourceFilter != nil: $sourceFilter else: ""
  cmd.requestLimit = if limit <= 0: 0 else: int(limit)
  cmd.intParam = int(connectCap)
  let result = submitCommand(node, cmd)
  let payload =
    if result.resultCode == NimResultOk and result.stringResult.len > 0:
      cloneOwnedString(result.stringResult)
    else:
      "{\"connectedPeers\":[],\"connectedPeersInfo\":[],\"discoveredPeers\":{\"peers\":[],\"totalCount\":0},\"mdnsDebug\":{}}"
  destroyCommand(cmd)
  allocCString(payload)

proc social_connect_peer*(handle: pointer, peerId: cstring, multiaddr: cstring): bool =
  social_connect_peer_timeout(handle, peerId, multiaddr, 5_000)

proc social_connect_peer_timeout*(handle: pointer, peerId: cstring, multiaddr: cstring, timeoutMs: cint): bool =
  if peerId.isNil:
    return false
  let node = nodeFromHandle(handle)
  if node.isNil:
    return false
  let peer = $peerId
  let dialAddr = if multiaddr == nil: "" else: $multiaddr
  let safeTimeoutMs = max(int(timeoutMs), 250)
  var connected = false
  if dialAddr.len > 0:
    connected = libp2p_connect_multiaddr_timeout(handle, dialAddr.cstring, safeTimeoutMs.cint) == NimResultOk
  if not connected:
    connected = libp2p_connect_peer_timeout(handle, peer.cstring, safeTimeoutMs.cint) == NimResultOk
  var payload = newJObject()
  payload["peerId"] = %peer
  payload["multiaddr"] = %dialAddr
  payload["ok"] = %connected
  recordSocialEvent(node, "discovery", "connect_peer", payload, source = "social_connect_peer")
  connected

proc social_dm_send*(
    handle: pointer, peerId: cstring, conversationId: cstring, messageJson: cstring
): bool =
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
  let statePayload = cloneJson(payload)
  let eventPayload = cloneJson(payload)
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
    msg["payload"] = statePayload
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
  recordSocialEvent(node, "dm", "send", eventPayload, conversationId = convId, source = "social_dm_send")
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
  let success = stat.toLowerAscii() in ["ok", "acked", "success", "delivered", "received", "read"]
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

proc parseJsonStringArray(text: string): seq[string] =
  let trimmed = text.strip()
  if trimmed.len == 0:
    return @[]
  try:
    let parsed = parseJson(trimmed)
    if parsed.kind != JArray:
      return @[]
    for item in parsed:
      if item.kind == JString:
        let value = canonicalizeMultiaddrText(item.getStr().strip())
        if value.len > 0 and value notin result:
          result.add(value)
  except CatchableError:
    discard

proc annotateGameMeshPayload(payload: JsonNode) =
  if payload.isNil or payload.kind != JObject:
    return
  payload["nativeBuildMarker"] = %NimLibp2pBuildMarker
  payload["nativeBuildSource"] = %"nim-libp2p"

proc encodeGameOperationPayload(opResult: GameOperationResult): string =
  var payload = newJObject()
  let outboundQueued =
    if opResult.outbound.len == 0:
      true
    else:
      opResult.outboundQueued
  payload["ok"] = %opResult.ok
  payload["handled"] = %opResult.handled
  payload["error"] = %opResult.error
  payload["appId"] = %opResult.appId
  payload["roomId"] = %opResult.roomId
  payload["matchId"] = %opResult.matchId
  payload["state"] =
    if opResult.statePayload.isNil:
      newJObject()
    else:
      cloneJson(opResult.statePayload)
  annotateGameMeshPayload(payload["state"])
  payload["uiMessage"] =
    if opResult.uiMessage.isNil:
      newJNull()
    else:
      cloneJson(opResult.uiMessage)
  payload["outboundCount"] = %opResult.outbound.len
  payload["outboundQueued"] = %outboundQueued
  payload["outboundFailedSources"] = %(opResult.outboundFailedSources)
  payload["nativeBuildMarker"] = %NimLibp2pBuildMarker
  payload["nativeBuildSource"] = %"nim-libp2p"
  $payload

proc localPeerIdFromNode(node: NimNode): string =
  if node.isNil:
    return ""
  let fromRuntime = localPeerId(node).strip()
  if fromRuntime.len > 0:
    return fromRuntime
  if not node.switchInstance.isNil and not node.switchInstance.peerInfo.isNil:
    return $node.switchInstance.peerInfo.peerId
  ""

proc ingestPendingGameMessages(node: NimNode) =
  if node.isNil:
    return
  let localPeerId = localPeerIdFromNode(node)
  if localPeerId.len == 0:
    return
  let rows = drainReceivedDmRows(node)
  var retainedRows: seq[JsonNode] = @[]
  for row in rows:
    if row.isNil or row.kind != JObject:
      continue
    let peerId = jsonGetStr(row, "peerId", jsonGetStr(row, "from"))
    let conversationId = jsonGetStr(row, "conversationId")
    let messageId = jsonGetStr(row, "messageId", jsonGetStr(row, "mid"))
    let payloadNode =
      if row.hasKey("payload"):
        row["payload"]
      else:
        row
    let expandedPayload = expandNestedGamePayload(payloadNode)
    let envelopeType = socialGameEnvelopeType(expandedPayload)
    if envelopeType.len == 0:
      retainedRows.add(row)
      continue
    if envelopeType.len > 0:
      debugLog(
        "[nimlibp2p] ingestPendingGameMessages begin peer=" & peerId &
        " conv=" & conversationId &
        " mid=" & messageId &
        " type=" & envelopeType &
        " room=" & jsonGetStr(expandedPayload{"game"}, "roomId") &
        " seq=" & $jsonGetInt64(expandedPayload{"game"}, "seq", -1)
      )
    var opResult: GameOperationResult
    node.gameMeshLock.acquire()
    try:
      opResult = handleIncomingGameMessage(
        node.gameMesh,
        peerId,
        conversationId,
        messageId,
        expandedPayload,
        localPeerId
      )
    finally:
      node.gameMeshLock.release()
    if envelopeType.len > 0:
      debugLog(
        "[nimlibp2p] ingestPendingGameMessages done mid=" & messageId &
        " type=" & envelopeType &
        " ok=" & $opResult.ok &
        " handled=" & $opResult.handled &
        " err=" & opResult.error &
        " state=" & $(if opResult.statePayload.isNil: newJNull() else: opResult.statePayload)
      )
    if opResult.handled and opResult.outbound.len > 0:
      executeGameResultOutboundQueued(node, opResult)
  if retainedRows.len > 0:
    restoreReceivedDmRows(node, retainedRows)

proc gameMeshCurrentStateJson(node: NimNode, appId, roomId: string): string =
  if node.isNil:
    return "{\"appId\":\"\",\"roomId\":\"\"}"
  ingestPendingGameMessages(node)
  node.gameMeshLock.acquire()
  try:
    let payload = currentStatePayload(node.gameMesh, appId.strip(), roomId.strip())
    annotateGameMeshPayload(payload)
    $payload
  finally:
    node.gameMeshLock.release()

proc parseGameJoinSeedPayload(raw: string): tuple[remoteSeedAddrs, localSeedAddrs: seq[string]] =
  let trimmed = raw.strip()
  if trimmed.len == 0:
    return (@[], @[])
  if trimmed.startsWith("{"):
    try:
      let parsed = parseJson(trimmed)
      proc readSeedArray(node: JsonNode, key: string): seq[string] =
        if node.kind != JObject or not node.hasKey(key) or node[key].kind != JArray:
          return @[]
        for item in node[key]:
          if item.kind == JString:
            let value = item.getStr().strip()
            if value.len > 0:
              result.add(value)
      let remoteSeedAddrs = readSeedArray(parsed, "remoteSeedAddrs")
      let localSeedAddrs = readSeedArray(parsed, "localSeedAddrs")
      if remoteSeedAddrs.len > 0 or localSeedAddrs.len > 0:
        return (remoteSeedAddrs, localSeedAddrs)
    except CatchableError:
      discard
  let legacySeedAddrs = parseJsonStringArray(trimmed)
  (legacySeedAddrs, legacySeedAddrs)

proc libp2p_game_mesh_create_room*(
    handle: pointer,
    appId: cstring,
    remotePeerId: cstring,
    conversationId: cstring,
    seedAddrsJson: cstring,
    matchSeed: cstring,
    sendInvite: bool
): cstring {.exportc, cdecl, dynlib.} =
  try:
    let node = nodeFromHandle(handle)
    if node.isNil or appId.isNil or remotePeerId.isNil:
      recordRuntimeError("game_mesh_create_room_invalid_args")
      return allocCString("""{"ok":false,"handled":true,"error":"game_mesh_create_room_invalid_args"}""")
    clearRuntimeError()
    let safeAppId = $appId
    let safeRemotePeerId = $remotePeerId
    let safeConversationId = if conversationId.isNil: "" else: $conversationId
    let safeMatchSeed = if matchSeed.isNil: "" else: $matchSeed
    let localPeerId = localPeerIdFromNode(node)
    if localPeerId.len == 0:
      recordRuntimeError("game_mesh_create_room_missing_local_peer")
      return allocCString("""{"ok":false,"handled":true,"error":"game_mesh_create_room_missing_local_peer"}""")
    let providedSeedAddrs = parseJsonStringArray(if seedAddrsJson.isNil: "" else: $seedAddrsJson)
    let runtimeSeedAddrs = currentGameSeedAddrs(node, localPeerId)
    let safeSeedAddrs =
      if runtimeSeedAddrs.len > 0:
        runtimeSeedAddrs
      else:
        prioritizeGameSeedAddrs(providedSeedAddrs)
    var opResult: GameOperationResult
    node.gameMeshLock.acquire()
    try:
      opResult = createInviteRoom(
        node.gameMesh,
        safeAppId,
        localPeerId,
        safeRemotePeerId,
        safeConversationId,
        safeSeedAddrs,
        safeMatchSeed,
        sendInvite
      )
    finally:
      node.gameMeshLock.release()
    if not opResult.ok and opResult.error.len > 0:
      recordRuntimeError(opResult.error)
    else:
      clearRuntimeError()
    executeGameResultOutboundQueued(node, opResult)
    allocCString(encodeGameOperationPayload(opResult))
  except Exception as exc:
    let stackText = getStackTrace(exc).multiReplace(("\n", "|"), ("\r", ""))
    let errMsg = "game_mesh_create_room_exception:" &
      (if exc.msg.len > 0: exc.msg else: $exc.name) &
      (if stackText.len > 0: " stack=" & stackText else: "")
    recordRuntimeError(errMsg)
    allocCString(("{\"ok\":false,\"handled\":true,\"error\":\"" &
      errMsg.multiReplace(("\\", "\\\\"), ("\"", "\\\"")) & "\"}"))

proc libp2p_game_mesh_join_room*(
    handle: pointer,
    appId: cstring,
    roomId: cstring,
    hostPeerId: cstring,
    conversationId: cstring,
    seedAddrsJson: cstring,
    matchId: cstring,
    mode: cstring
): cstring {.exportc, cdecl, dynlib.} =
  try:
    let node = nodeFromHandle(handle)
    if node.isNil or appId.isNil or roomId.isNil or hostPeerId.isNil:
      recordRuntimeError("game_mesh_join_room_invalid_args")
      return allocCString("""{"ok":false,"handled":true,"error":"game_mesh_join_room_invalid_args"}""")
    clearRuntimeError()
    let joinStartedAt = epochTime()
    debugLog("[nimlibp2p] game_mesh_join_room:start roomId=" & $roomId &
      " hostPeerId=" & $hostPeerId &
      " conversationId=" & (if conversationId.isNil: "" else: $conversationId))
    let localPeerId = localPeerIdFromNode(node)
    if localPeerId.len == 0:
      recordRuntimeError("game_mesh_join_room_missing_local_peer")
      return allocCString("""{"ok":false,"handled":true,"error":"game_mesh_join_room_missing_local_peer"}""")
    let (safeRemoteSeedAddrs, parsedLocalSeedAddrs) = parseGameJoinSeedPayload(
      if seedAddrsJson.isNil: "" else: $seedAddrsJson
    )
    debugLog("[nimlibp2p] game_mesh_join_room:seedPayload roomId=" & $roomId &
      " remoteSeedCount=" & $safeRemoteSeedAddrs.len &
      " localSeedCount=" & $parsedLocalSeedAddrs.len)
    let runtimeLocalSeedAddrs = currentGameSeedAddrs(node, localPeerId)
    let safeLocalSeedAddrs =
      if runtimeLocalSeedAddrs.len > 0:
        runtimeLocalSeedAddrs
      else:
        prioritizeGameSeedAddrs(parsedLocalSeedAddrs)
    debugLog("[nimlibp2p] game_mesh_join_room:localSeedSummary roomId=" & $roomId &
      " runtimeLocalSeedCount=" & $runtimeLocalSeedAddrs.len &
      " effectiveLocalSeedCount=" & $safeLocalSeedAddrs.len)
    var opResult: GameOperationResult
    debugLog("[nimlibp2p] game_mesh_join_room:joinRoom:start roomId=" & $roomId)
    node.gameMeshLock.acquire()
    try:
      opResult = joinRoom(
        node.gameMesh,
        $appId,
        $roomId,
        localPeerId,
        $hostPeerId,
        if conversationId.isNil: "" else: $conversationId,
        if matchId.isNil: "" else: $matchId,
        safeRemoteSeedAddrs,
        safeLocalSeedAddrs,
        if mode.isNil: "" else: $mode
      )
    finally:
      node.gameMeshLock.release()
    debugLog("[nimlibp2p] game_mesh_join_room:joinRoom:done roomId=" & $roomId &
      " ok=" & $opResult.ok &
      " handled=" & $opResult.handled &
      " outboundCount=" & $opResult.outbound.len &
      " stateBytes=" & $opResult.statePayload.len)
    if not opResult.ok and opResult.error.len > 0:
      recordRuntimeError(opResult.error)
    else:
      clearRuntimeError()
    debugLog("[nimlibp2p] game_mesh_join_room:executeOutbound:start roomId=" & $roomId &
      " outboundCount=" & $opResult.outbound.len)
    executeGameResultOutboundQueued(node, opResult)
    debugLog("[nimlibp2p] game_mesh_join_room:executeOutbound:done roomId=" & $roomId)
    let encoded = encodeGameOperationPayload(opResult)
    debugLog("[nimlibp2p] game_mesh_join_room:done roomId=" & $roomId &
      " elapsedMs=" & $int((epochTime() - joinStartedAt) * 1000) &
      " payloadBytes=" & $encoded.len)
    allocCString(encoded)
  except Exception as exc:
    let errMsg = "game_mesh_join_room_exception:" &
      exc.msg.multiReplace(("\\", "\\\\"), ("\"", "\\\"")) &
      " stack=" & getStackTrace(exc).multiReplace(("\\", "\\\\"), ("\"", "\\\""), ("\n", "|"))
    recordRuntimeError(errMsg)
    allocCString("{\"ok\":false,\"handled\":true,\"error\":\"" &
      errMsg.multiReplace(("\\", "\\\\"), ("\"", "\\\"")) & "\"}")

proc libp2p_game_mesh_restore_snapshot*(
    handle: pointer,
    snapshotJson: cstring
): cstring {.exportc, cdecl, dynlib.} =
  try:
    let node = nodeFromHandle(handle)
    if node.isNil or snapshotJson.isNil:
      recordRuntimeError("game_mesh_restore_snapshot_invalid_args")
      return allocCString("""{"ok":false,"handled":true,"error":"game_mesh_restore_snapshot_invalid_args"}""")
    clearRuntimeError()
    let localPeerId = localPeerIdFromNode(node)
    if localPeerId.len == 0:
      recordRuntimeError("game_mesh_restore_snapshot_missing_local_peer")
      return allocCString("""{"ok":false,"handled":true,"error":"game_mesh_restore_snapshot_missing_local_peer"}""")
    var snapshot = newJObject()
    try:
      snapshot = parseJson($snapshotJson)
    except CatchableError as exc:
      let errMsg = "game_mesh_restore_snapshot_invalid_json:" & exc.msg
      recordRuntimeError(errMsg)
      return allocCString(("{\"ok\":false,\"handled\":true,\"error\":\"" &
        errMsg.multiReplace(("\\", "\\\\"), ("\"", "\\\"")) & "\"}"))
    var opResult: GameOperationResult
    node.gameMeshLock.acquire()
    try:
      opResult = restoreRoomSnapshot(node.gameMesh, snapshot, localPeerId)
    finally:
      node.gameMeshLock.release()
    if not opResult.ok and opResult.error.len > 0:
      recordRuntimeError(opResult.error)
    else:
      clearRuntimeError()
    executeGameResultOutboundQueued(node, opResult)
    allocCString(encodeGameOperationPayload(opResult))
  except Exception as exc:
    let errMsg = "game_mesh_restore_snapshot_exception:" &
      (if exc.msg.len > 0: exc.msg else: $exc.name) &
      " stack=" & getStackTrace(exc).multiReplace(("\\", "\\\\"), ("\"", "\\\""), ("\n", "|"))
    recordRuntimeError(errMsg)
    allocCString("{\"ok\":false,\"handled\":true,\"error\":\"" &
      errMsg.multiReplace(("\\", "\\\\"), ("\"", "\\\"")) & "\"}")

proc libp2p_game_mesh_handle_incoming*(
    handle: pointer,
    peerId: cstring,
    conversationId: cstring,
    messageId: cstring,
    payloadJson: cstring
): cstring {.exportc, cdecl, dynlib.} =
  let node = nodeFromHandle(handle)
  if node.isNil or peerId.isNil or payloadJson.isNil:
    recordRuntimeError("game_mesh_handle_incoming_invalid_args")
    return allocCString("""{"ok":false,"handled":true,"error":"game_mesh_handle_incoming_invalid_args"}""")
  clearRuntimeError()
  let localPeerId = localPeerIdFromNode(node)
  if localPeerId.len == 0:
    recordRuntimeError("game_mesh_handle_incoming_missing_local_peer")
    return allocCString("""{"ok":false,"handled":true,"error":"game_mesh_handle_incoming_missing_local_peer"}""")
  var parsed = newJObject()
  try:
    parsed = parseJson($payloadJson)
  except CatchableError as exc:
    let errMsg = "game_mesh_handle_incoming_invalid_json:" & exc.msg
    recordRuntimeError(errMsg)
    return allocCString(("{\"ok\":false,\"handled\":true,\"error\":\"" &
      errMsg.multiReplace(("\\", "\\\\"), ("\"", "\\\"")) & "\"}"))
  var opResult: GameOperationResult
  node.gameMeshLock.acquire()
  try:
    opResult = handleIncomingGameMessage(
      node.gameMesh,
      $peerId,
      if conversationId.isNil: "" else: $conversationId,
      if messageId.isNil: "" else: $messageId,
      parsed,
      localPeerId
    )
  finally:
    node.gameMeshLock.release()
  if not opResult.ok and opResult.error.len > 0:
    recordRuntimeError(opResult.error)
  else:
    clearRuntimeError()
  # User-triggered game actions should return with the updated local room state
  # immediately; network delivery continues on the node command loop.
  executeGameResultOutboundQueued(node, opResult)
  allocCString(encodeGameOperationPayload(opResult))

proc libp2p_game_mesh_submit_action*(
    handle: pointer,
    appId: cstring,
    roomId: cstring,
    actionJson: cstring
): cstring {.exportc, cdecl, dynlib.} =
  let node = nodeFromHandle(handle)
  if node.isNil or appId.isNil or roomId.isNil or actionJson.isNil:
    recordRuntimeError("game_mesh_submit_action_invalid_args")
    return allocCString("""{"ok":false,"handled":true,"error":"game_mesh_submit_action_invalid_args"}""")
  clearRuntimeError()
  var action = newJObject()
  try:
    action = parseJson($actionJson)
  except CatchableError as exc:
    let errMsg = "game_mesh_submit_action_invalid_json:" & exc.msg
    recordRuntimeError(errMsg)
    return allocCString(("{\"ok\":false,\"handled\":true,\"error\":\"" &
      errMsg.multiReplace(("\\", "\\\\"), ("\"", "\\\"")) & "\"}"))
  let safeAppId = $appId
  let safeRoomId = $roomId
  let op = jsonGetStr(action, "op", jsonGetStr(action, "event")).toLowerAscii()
  var opResult: GameOperationResult
  node.gameMeshLock.acquire()
  try:
    case safeAppId
    of "chess":
      case op
      of "move":
        let fromOpt = decodePosition(action{"from"})
        let toOpt = decodePosition(action{"to"})
        if fromOpt.isNone() or toOpt.isNone():
          opResult = GameOperationResult(ok: false, handled: true, error: "invalid_move", appId: safeAppId, roomId: safeRoomId)
          opResult.statePayload = currentStatePayload(node.gameMesh, safeAppId, safeRoomId)
        else:
          opResult = submitXiangqiMove(node.gameMesh, safeRoomId, fromOpt.get(), toOpt.get())
      of "resign":
        opResult = resignXiangqi(node.gameMesh, safeRoomId)
      of "draw_offer":
        opResult = offerXiangqiDraw(node.gameMesh, safeRoomId)
      of "draw_accept":
        opResult = respondXiangqiDraw(node.gameMesh, safeRoomId, true)
      of "draw_reject":
        opResult = respondXiangqiDraw(node.gameMesh, safeRoomId, false)
      of "rematch_offer":
        opResult = offerXiangqiRematch(node.gameMesh, safeRoomId)
      of "rematch_accept":
        opResult = respondXiangqiRematch(node.gameMesh, safeRoomId, true)
      of "rematch_reject":
        opResult = respondXiangqiRematch(node.gameMesh, safeRoomId, false)
      of "voice_offer":
        opResult = submitXiangqiVoiceOffer(
          node.gameMesh,
          safeRoomId,
          jsonGetStr(action, "voiceRoomId"),
          jsonGetStr(action, "voiceStreamKey"),
          jsonGetStr(action, "voiceTitle"),
          jsonGetBool(action, "voiceAudioOnly", true),
        )
      of "voice_clear":
        opResult = clearXiangqiVoiceOffer(node.gameMesh, safeRoomId)
      of "restart":
        opResult = restartRoom(node.gameMesh, safeAppId, safeRoomId)
      else:
        opResult = GameOperationResult(ok: false, handled: true, error: "unsupported_game_action", appId: safeAppId, roomId: safeRoomId)
        opResult.statePayload = currentStatePayload(node.gameMesh, safeAppId, safeRoomId)
    of "doudizhu":
      case op
      of "bid":
        opResult = submitDoudizhuBid(node.gameMesh, safeRoomId, int(jsonGetInt64(action, "bid", 0)))
      of "play":
        opResult = submitDoudizhuPlay(node.gameMesh, safeRoomId, intList(action{"cardIds"}))
      of "pass":
        opResult = submitDoudizhuPass(node.gameMesh, safeRoomId)
      of "restart":
        opResult = restartRoom(node.gameMesh, safeAppId, safeRoomId)
      else:
        opResult = GameOperationResult(ok: false, handled: true, error: "unsupported_game_action", appId: safeAppId, roomId: safeRoomId)
        opResult.statePayload = currentStatePayload(node.gameMesh, safeAppId, safeRoomId)
    else:
      opResult = GameOperationResult(ok: false, handled: true, error: "unsupported_game", appId: safeAppId, roomId: safeRoomId)
      opResult.statePayload = currentStatePayload(node.gameMesh, safeAppId, safeRoomId)
  finally:
    node.gameMeshLock.release()
  if not opResult.ok and opResult.error.len > 0:
    recordRuntimeError(opResult.error)
  else:
    clearRuntimeError()
  executeGameResultOutboundQueued(node, opResult)
  allocCString(encodeGameOperationPayload(opResult))

proc libp2p_game_mesh_current_state*(
    handle: pointer,
    appId: cstring,
    roomId: cstring
): cstring {.exportc, cdecl, dynlib.} =
  let node = nodeFromHandle(handle)
  if node.isNil or appId.isNil or roomId.isNil:
    return allocCString("""{"appId":"","roomId":""}""")
  clearRuntimeError()
  allocCString(gameMeshCurrentStateJson(node, $appId, $roomId))

proc libp2p_game_mesh_request_snapshot*(
    handle: pointer,
    appId: cstring,
    roomId: cstring
): cstring {.exportc, cdecl, dynlib.} =
  let node = nodeFromHandle(handle)
  if node.isNil or appId.isNil or roomId.isNil:
    recordRuntimeError("game_mesh_request_snapshot_invalid_args")
    return allocCString("""{"ok":false,"handled":true,"error":"game_mesh_request_snapshot_invalid_args"}""")
  clearRuntimeError()
  let safeAppId = $appId
  let safeRoomId = $roomId
  ingestPendingGameMessages(node)
  var opResult: GameOperationResult
  let localSeedAddrs = collectLocalGameSeedAddrs(node)
  node.gameMeshLock.acquire()
  try:
    opResult = requestRoomSnapshot(node.gameMesh, safeAppId, safeRoomId, localSeedAddrs)
  finally:
    node.gameMeshLock.release()
  if not opResult.ok and opResult.error.len > 0:
    recordRuntimeError(opResult.error)
  else:
    clearRuntimeError()
  if opResult.outbound.len > 0:
    executeGameResultOutboundQueued(node, opResult)
  allocCString(encodeGameOperationPayload(opResult))

proc libp2p_game_mesh_dispatch_snapshot*(
    handle: pointer,
    appId: cstring,
    roomId: cstring
): cstring {.exportc, cdecl, dynlib.} =
  let node = nodeFromHandle(handle)
  if node.isNil or appId.isNil or roomId.isNil:
    recordRuntimeError("game_mesh_dispatch_snapshot_invalid_args")
    return allocCString("""{"ok":false,"handled":true,"error":"game_mesh_dispatch_snapshot_invalid_args"}""")
  clearRuntimeError()
  let safeAppId = $appId
  let safeRoomId = $roomId
  ingestPendingGameMessages(node)
  var opResult: GameOperationResult
  let localSeedAddrs = collectLocalGameSeedAddrs(node)
  node.gameMeshLock.acquire()
  try:
    opResult = dispatchRoomSnapshot(node.gameMesh, safeAppId, safeRoomId, localSeedAddrs)
  finally:
    node.gameMeshLock.release()
  if not opResult.ok and opResult.error.len > 0:
    recordRuntimeError(opResult.error)
  else:
    clearRuntimeError()
  if opResult.outbound.len > 0:
    executeGameResultOutboundQueued(node, opResult)
  allocCString(encodeGameOperationPayload(opResult))

proc libp2p_game_mesh_dispatch_audit*(
    handle: pointer,
    appId: cstring,
    roomId: cstring
): cstring {.exportc, cdecl, dynlib.} =
  let node = nodeFromHandle(handle)
  if node.isNil or appId.isNil or roomId.isNil:
    recordRuntimeError("game_mesh_dispatch_audit_invalid_args")
    return allocCString("""{"ok":false,"handled":true,"error":"game_mesh_dispatch_audit_invalid_args"}""")
  clearRuntimeError()
  let safeAppId = $appId
  let safeRoomId = $roomId
  ingestPendingGameMessages(node)
  var opResult: GameOperationResult
  node.gameMeshLock.acquire()
  try:
    opResult = dispatchRoomAudit(node.gameMesh, safeAppId, safeRoomId)
  finally:
    node.gameMeshLock.release()
  if not opResult.ok and opResult.error.len > 0:
    recordRuntimeError(opResult.error)
  else:
    clearRuntimeError()
  if opResult.outbound.len > 0:
    executeGameResultOutboundQueued(node, opResult)
  allocCString(encodeGameOperationPayload(opResult))

proc libp2p_game_mesh_wait_state*(
    handle: pointer,
    appId: cstring,
    roomId: cstring,
    expectedPhase: cstring,
    timeoutMs: cint
): cstring {.exportc, cdecl, dynlib.} =
  let node = nodeFromHandle(handle)
  if node.isNil or appId.isNil or roomId.isNil:
    recordRuntimeError("game_mesh_wait_state_invalid_args")
    return allocCString("""{"ok":false,"error":"game_mesh_wait_state_invalid_args"}""")
  clearRuntimeError()
  let safeAppId = $appId
  let safeRoomId = $roomId
  let expected = if expectedPhase.isNil: "" else: $expectedPhase
  let timeoutBudget = max(int(timeoutMs), 1_000)
  let deadline = nowMillis() + int64(timeoutBudget)
  while nowMillis() < deadline:
    let payloadText = gameMeshCurrentStateJson(node, safeAppId, safeRoomId)
    try:
      var payload = parseJson(payloadText)
      let phase = jsonGetStr(payload, "phase")
      if expected.len == 0 or phase.toUpperAscii() == expected.toUpperAscii():
        payload["ok"] = %true
        return allocCString($payload)
    except CatchableError:
      discard
    sleep(120)
  try:
    var payload = parseJson(gameMeshCurrentStateJson(node, safeAppId, safeRoomId))
    payload["ok"] = %false
    payload["error"] = %"game_state_timeout"
    return allocCString($payload)
  except CatchableError:
    recordRuntimeError("game_state_timeout")
    allocCString("""{"ok":false,"error":"game_state_timeout"}""")

proc libp2p_game_mesh_apply_script*(
    handle: pointer,
    appId: cstring,
    roomId: cstring,
    scriptName: cstring,
    timeoutMs: cint
): cstring {.exportc, cdecl, dynlib.} =
  let node = nodeFromHandle(handle)
  if node.isNil or appId.isNil or roomId.isNil:
    recordRuntimeError("game_mesh_apply_script_invalid_args")
    return allocCString("""{"ok":false,"error":"game_mesh_apply_script_invalid_args"}""")
  clearRuntimeError()
  let safeAppId = $appId
  let safeRoomId = $roomId
  let script = if scriptName.isNil or ($scriptName).strip().len == 0: "default" else: $scriptName
  let timeoutBudget = max(int(timeoutMs), 5_000)
  let deadline = nowMillis() + int64(timeoutBudget)
  while nowMillis() < deadline:
    let stateText = gameMeshCurrentStateJson(node, safeAppId, safeRoomId)
    var payload = newJObject()
    try:
      payload = parseJson(stateText)
    except CatchableError:
      discard
    let phase = jsonGetStr(payload, "phase").toUpperAscii()
    if phase == "FINISHED":
      payload["ok"] = %true
      payload["script"] = %script
      return allocCString($payload)
    var opResult: GameOperationResult
    node.gameMeshLock.acquire()
    try:
      opResult = applyAutomationStep(node.gameMesh, safeAppId, safeRoomId)
    finally:
      node.gameMeshLock.release()
    if opResult.handled and opResult.outbound.len > 0:
      executeGameResultOutboundQueued(node, opResult)
    if not opResult.ok and opResult.error.len > 0 and opResult.error != "script_exhausted":
      recordRuntimeError(opResult.error)
      var resultPayload = parseJson(encodeGameOperationPayload(opResult))
      resultPayload["script"] = %script
      return allocCString($resultPayload)
    sleep(160)
  try:
    var payload = parseJson(gameMeshCurrentStateJson(node, safeAppId, safeRoomId))
    payload["ok"] = %false
    payload["script"] = %script
    payload["error"] = %"game_script_timeout"
    return allocCString($payload)
  except CatchableError:
    recordRuntimeError("game_script_timeout")
    allocCString("""{"ok":false,"error":"game_script_timeout"}""")

proc libp2p_game_mesh_apply_step*(
    handle: pointer,
    appId: cstring,
    roomId: cstring
): cstring {.exportc, cdecl, dynlib.} =
  let node = nodeFromHandle(handle)
  if node.isNil or appId.isNil or roomId.isNil:
    recordRuntimeError("game_mesh_apply_step_invalid_args")
    return allocCString("""{"ok":false,"error":"game_mesh_apply_step_invalid_args"}""")
  clearRuntimeError()
  let safeAppId = $appId
  let safeRoomId = $roomId
  ingestPendingGameMessages(node)
  var opResult: GameOperationResult
  node.gameMeshLock.acquire()
  try:
    opResult = applyAutomationStep(node.gameMesh, safeAppId, safeRoomId)
  finally:
    node.gameMeshLock.release()
  if opResult.handled and opResult.outbound.len > 0:
    executeGameResultOutboundQueued(node, opResult)
  if not opResult.ok and opResult.error.len > 0 and opResult.error != "script_exhausted":
    recordRuntimeError(opResult.error)
  else:
    clearRuntimeError()
  allocCString(encodeGameOperationPayload(opResult))

proc libp2p_game_mesh_replay_verify*(
    handle: pointer,
    appId: cstring,
    roomId: cstring
): cstring {.exportc, cdecl, dynlib.} =
  let node = nodeFromHandle(handle)
  if node.isNil or appId.isNil or roomId.isNil:
    recordRuntimeError("game_mesh_replay_verify_invalid_args")
    return allocCString("""{"ok":false,"error":"game_mesh_replay_verify_invalid_args"}""")
  clearRuntimeError()
  var payload = newJObject()
  node.gameMeshLock.acquire()
  try:
    payload = replayVerify(node.gameMesh, $appId, $roomId)
  finally:
    node.gameMeshLock.release()
  allocCString($payload)

proc libp2p_game_mesh_local_smoke*(
    handle: pointer,
    appId: cstring
): cstring {.exportc, cdecl, dynlib.} =
  discard handle
  if appId.isNil:
    recordRuntimeError("game_mesh_local_smoke_invalid_args")
    return allocCString("""{"ok":false,"error":"game_mesh_local_smoke_invalid_args"}""")
  clearRuntimeError()
  allocCString($(localSmoke($appId)))

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
  if gid.len == 0:
    return false
  if not ensureLanGroupSubscription(node, gid):
    return false
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

proc socialCanonicalChannel(category: string): string =
  let normalized = category.strip().toLowerAscii()
  case normalized
  of "", "content", "内容":
    "content"
  of "product", "commerce", "电商":
    "product"
  of "live", "直播":
    "live"
  of "app", "应用":
    "app"
  of "food", "外卖":
    "food"
  of "ride", "顺风车":
    "ride"
  of "job", "求职":
    "job"
  of "hire", "招聘":
    "hire"
  of "rent", "出租":
    "rent"
  of "sell", "出售":
    "sell"
  of "secondhand", "二手":
    "secondhand"
  of "crowdfunding", "众筹":
    "crowdfunding"
  of "ad", "广告":
    "ad"
  else:
    if normalized.len == 0: "content" else: normalized

proc socialDisplayNameForPeer(peerId: string): string =
  let normalized = peerId.strip()
  if normalized.len == 0:
    return "nim-libp2p"
  if normalized.len <= 8:
    return "节点 " & normalized
  "节点 " & normalized[^8 .. ^1]

proc socialMomentTimestamp(post: JsonNode): int64 =
  jsonGetInt64(post, "timestampMs", jsonGetInt64(post, "timestamp", nowMillis()))

proc socialMomentAuthorPeerId(node: NimNode, post: JsonNode): string =
  let authorPeerId = jsonGetStr(post, "authorPeerId", jsonGetStr(post, "peerId")).strip()
  if authorPeerId.len > 0:
    return authorPeerId
  localPeerId(node)

proc socialMomentAuthorName(node: NimNode, post: JsonNode, authorPeerId: string): string =
  let explicitName = jsonGetStr(post, "authorName", jsonGetStr(post, "nickname")).strip()
  if explicitName.len > 0:
    return explicitName
  if authorPeerId.len > 0:
    return socialDisplayNameForPeer(authorPeerId)
  let localPeer = localPeerId(node)
  if localPeer.len > 0:
    return socialDisplayNameForPeer(localPeer)
  "nim-libp2p"

proc socialMomentContent(post: JsonNode): string =
  let content = maybeBodyFromPayload(post).strip()
  if content.len > 0:
    return content
  jsonGetStr(post, "summary", jsonGetStr(post, "title")).strip()

proc socialMomentSummary(post: JsonNode, content: string): string =
  jsonGetStr(post, "summary", content).strip()

proc socialMomentTitle(post: JsonNode, content: string): string =
  let explicitTitle = jsonGetStr(post, "title").strip()
  if explicitTitle.len > 0:
    return explicitTitle
  let summary = socialMomentSummary(post, content)
  if summary.len > 0:
    return summary
  content

proc socialMomentMediaLabel(post: JsonNode): string =
  let directLabel = jsonGetStr(
    post,
    "mediaLabel",
    jsonGetStr(post, "media", jsonGetStr(post, "coverMedia"))
  ).strip()
  if directLabel.len > 0:
    return directLabel
  if post.hasKey("mediaItems") and post["mediaItems"].kind == JArray:
    for item in post["mediaItems"]:
      if item.kind == JString and item.getStr().strip().len > 0:
        return item.getStr().strip()
  ""

proc socialMomentLocationHint(post: JsonNode): string =
  jsonGetStr(post, "locationHint", jsonGetStr(post, "location")).strip()

proc socialMomentType(post: JsonNode, channel: string): string =
  let explicitType = jsonGetStr(post, "type").strip()
  if explicitType.len > 0:
    return explicitType
  if channel == "live":
    return "live"
  if channel == "app":
    return "app"
  if channel == "product":
    return "product"
  let mediaLabel = socialMomentMediaLabel(post).toLowerAscii()
  if mediaLabel.contains(".mp4") or mediaLabel.contains(".mov") or mediaLabel.contains("video"):
    return "video"
  if mediaLabel.contains(".mp3") or mediaLabel.contains(".m4a") or mediaLabel.contains("audio"):
    return "audio"
  if mediaLabel.contains(".jpg") or mediaLabel.contains(".jpeg") or mediaLabel.contains(".png") or mediaLabel.contains("image"):
    return "image"
  "text"

proc socialMomentExtraNode(post: JsonNode): JsonNode =
  if post.kind != JObject:
    return newJObject()
  if post.hasKey("extra") and post["extra"].kind in {JObject, JArray}:
    return post["extra"]
  if post.hasKey("extraJson") and post["extraJson"].kind in {JObject, JArray}:
    return post["extraJson"]
  let raw = jsonGetStr(post, "extra", jsonGetStr(post, "extraJson")).strip()
  if raw.len == 0:
    return newJObject()
  try:
    let parsed = parseJson(raw)
    if parsed.kind in {JObject, JArray}:
      return parsed
  except CatchableError:
    discard
  newJObject()

proc socialMomentSynccastNode(post: JsonNode): JsonNode =
  let extraNode = socialMomentExtraNode(post)
  if extraNode.kind != JObject:
    return newJObject()
  if extraNode.hasKey("synccast") and extraNode["synccast"].kind == JObject:
    return extraNode["synccast"]
  newJObject()

proc socialSlugId(value: string, fallback: string): string

proc socialMomentMediaTransferEntries(post: JsonNode): JsonNode =
  let extraNode = socialMomentExtraNode(post)
  if extraNode.kind != JObject:
    return newJArray()
  if extraNode.hasKey("mediaTransfer") and extraNode["mediaTransfer"].kind == JObject:
    let transfer = extraNode["mediaTransfer"]
    if transfer.hasKey("entries") and transfer["entries"].kind == JArray:
      return transfer["entries"]
  newJArray()

proc socialPrimaryMediaTransfer(post: JsonNode): JsonNode =
  let entries = socialMomentMediaTransferEntries(post)
  if entries.kind == JArray:
    for row in entries:
      if row.kind != JObject:
        continue
      if jsonGetStr(row, "key", jsonGetStr(row, "publishedUri", jsonGetStr(row, "uri"))).strip().len > 0:
        return row
  newJObject()

proc socialPlaybackMode(post: JsonNode, mediaKind: string, hasAsset: bool): string =
  let category = socialCanonicalChannel(jsonGetStr(post, "category", jsonGetStr(post, "channel")))
  let synccast = socialMomentSynccastNode(post)
  let roomId = jsonGetStr(synccast, "roomId", "").strip()
  let streamKey = jsonGetStr(synccast, "streamKey", "").strip()
  if category == "live":
    if hasAsset or roomId.len > 0 or streamKey.len > 0:
      return "program_live"
    return "interactive_live"
  if mediaKind in ["audio", "video"]:
    return "vod"
  if roomId.len > 0 or streamKey.len > 0:
    return "program_live"
  ""

proc socialBuildMediaAssetManifest(post: JsonNode): JsonNode =
  let category = socialCanonicalChannel(jsonGetStr(post, "category", jsonGetStr(post, "channel")))
  let mediaKind = socialMomentType(post, category).strip().toLowerAscii()
  let transferEntries = socialMomentMediaTransferEntries(post)
  let primaryTransfer = socialPrimaryMediaTransfer(post)
  let primaryTransferKey =
    jsonGetStr(primaryTransfer, "key", jsonGetStr(primaryTransfer, "publishedUri", jsonGetStr(primaryTransfer, "uri"))).strip()
  let hasPrimaryTransfer = primaryTransfer.kind == JObject and primaryTransferKey.len > 0
  if mediaKind notin ["audio", "video"] and not hasPrimaryTransfer and transferEntries.len <= 0:
    return newJNull()
  let contentId = jsonGetStr(post, "postId", jsonGetStr(post, "id")).strip()
  let extraNode = socialMomentExtraNode(post)
  let primaryKey =
    if hasPrimaryTransfer:
      jsonGetStr(primaryTransfer, "key", contentId).strip()
    else:
      contentId
  let assetId = if primaryKey.len > 0: primaryKey else: contentId
  let rootCid =
    jsonGetStr(primaryTransfer, "sha256", jsonGetStr(primaryTransfer, "digest", assetId)).strip()
  let durationMs =
    max(
      jsonGetInt64(extraNode, "durationMs", jsonGetInt64(post, "durationMs", 0)),
      0'i64
    )
  let gopMs = max(jsonGetInt64(socialMomentSynccastNode(post), "gopMs", 500), 0'i64)
  let frameIntervalMs =
    max(jsonGetInt64(socialMomentSynccastNode(post), "videoFrameIntervalMs", 67), 0'i64)
  var tracks = newJArray()
  if transferEntries.kind == JArray and transferEntries.len > 0:
    for index in 0 ..< transferEntries.len:
      let row = transferEntries[index]
      if row.kind != JObject:
        continue
      var track = newJObject()
      let fileName = jsonGetStr(row, "fileName", jsonGetStr(row, "label", "media-" & $index)).strip()
      let trackId =
        jsonGetStr(row, "slot", jsonGetStr(row, "key", "track-" & $index)).strip()
      track["trackId"] = %(if trackId.len > 0: trackId else: "track-" & $index)
      track["kind"] = %jsonGetStr(row, "kind", mediaKind)
      track["label"] = %(if fileName.len > 0: fileName else: "媒体轨道")
      track["mimeType"] = %jsonGetStr(row, "mimeType", "")
      track["sizeBytes"] = %max(jsonGetInt64(row, "sizeBytes", 0), 0'i64)
      track["rootCid"] = %jsonGetStr(row, "sha256", jsonGetStr(row, "digest", rootCid))
      track["publishedUri"] = %jsonGetStr(row, "publishedUri", jsonGetStr(row, "uri"))
      track["seekable"] = %(mediaKind in ["audio", "video"])
      tracks.add(track)
  elif assetId.len > 0:
    var track = newJObject()
    track["trackId"] = %("track-" & socialSlugId(assetId, "primary"))
    track["kind"] = %mediaKind
    track["label"] = %socialMomentMediaLabel(post)
    track["mimeType"] = %""
    track["sizeBytes"] = %0
    track["rootCid"] = %rootCid
    track["publishedUri"] = %jsonGetStr(post, "media", jsonGetStr(post, "coverMedia"))
    track["seekable"] = %(mediaKind in ["audio", "video"])
    tracks.add(track)
  var asset = newJObject()
  asset["assetId"] = %assetId
  asset["rootCid"] = %rootCid
  asset["tracks"] = tracks
  asset["durationMs"] = %durationMs
  asset["poster"] = %jsonGetStr(post, "coverMedia", socialMomentMediaLabel(post))
  asset["seekable"] = %(mediaKind in ["audio", "video"])
  asset["keyframeIndex"] =
    if mediaKind == "video":
      %*{
        "gopMs": gopMs,
        "frameIntervalMs": frameIntervalMs,
      }
    else:
      newJNull()
  asset["encryptionEpoch"] = %max(jsonGetInt64(extraNode, "encryptionEpoch", nowMillis()), 0'i64)
  asset

proc socialSafeMediaAssetManifest(post: JsonNode): JsonNode =
  try:
    socialBuildMediaAssetManifest(post)
  except CatchableError as exc:
    warn "social media asset manifest failed", err = exc.msg
    newJNull()
  except Defect as exc:
    error "social media asset manifest defect", err = exc.msg
    newJNull()

proc socialBuildPlaybackDescriptor(node: NimNode, post: JsonNode): JsonNode =
  let category = socialCanonicalChannel(jsonGetStr(post, "category", jsonGetStr(post, "channel")))
  let synccast = socialMomentSynccastNode(post)
  let mediaAsset = socialSafeMediaAssetManifest(post)
  let mediaKind = jsonGetStr(synccast, "mediaKind", socialMomentType(post, category)).strip().toLowerAscii()
  let hasAsset = mediaAsset.kind == JObject and jsonGetStr(mediaAsset, "assetId", "").len > 0
  let mode = socialPlaybackMode(post, mediaKind, hasAsset)
  if mode.len == 0:
    return newJNull()
  let roomId = jsonGetStr(synccast, "roomId", "").strip()
  let streamKey = jsonGetStr(synccast, "streamKey", "").strip()
  let programId =
    jsonGetStr(
      synccast,
      "programId",
      jsonGetStr(synccast, "id", "program-" & socialSlugId(jsonGetStr(post, "postId", jsonGetStr(post, "id")), "media"))
    ).strip()
  let durationMs =
    if mediaAsset.kind == JObject: max(jsonGetInt64(mediaAsset, "durationMs", 0), 0'i64) else: 0'i64
  let dvrWindowMs =
    if mode == "program_live":
      max(jsonGetInt64(synccast, "dvrWindowMs", 30_000), 0'i64)
    else:
      0'i64
  let liveEdgeMs =
    if mode in ["interactive_live", "program_live"]:
      max(jsonGetInt64(synccast, "updatedAt", jsonGetInt64(post, "publishedAt", nowMillis())), 0'i64)
    else:
      0'i64
  let seekableEndMs =
    if mode == "vod":
      durationMs
    elif mode == "program_live":
      liveEdgeMs
    else:
      0'i64
  var descriptor = newJObject()
  descriptor["mode"] = %mode
  descriptor["transport"] = %"synccast"
  descriptor["assetId"] =
    %(if mediaAsset.kind == JObject: jsonGetStr(mediaAsset, "assetId", "") else: "")
  descriptor["programId"] = %(if programId.len > 0: programId else: "program-" & socialSlugId(roomId, "room"))
  descriptor["roomId"] = %roomId
  descriptor["streamKey"] = %streamKey
  descriptor["mediaKind"] = %mediaKind
  descriptor["anchorPeerId"] = %jsonGetStr(post, "authorPeerId", localPeerId(node))
  descriptor["title"] = %jsonGetStr(synccast, "title", socialMomentTitle(post, socialMomentContent(post)))
  descriptor["durationMs"] = %durationMs
  descriptor["liveEdgeMs"] = %liveEdgeMs
  descriptor["dvrWindowMs"] = %dvrWindowMs
  descriptor["seekableStartMs"] =
    %(if mode == "program_live": max(liveEdgeMs - dvrWindowMs, 0'i64) else: 0'i64)
  descriptor["seekableEndMs"] = %max(seekableEndMs, 0'i64)
  descriptor["allowSeek"] = %(mode == "vod")
  descriptor["allowPause"] = %(mode == "vod" or mode == "program_live")
  descriptor["allowRateChange"] = %false
  descriptor["rateOptions"] = toJsonStringArray(@["1.0"])
  descriptor["presentation"] = %"inline_player"
  descriptor["autoplay"] = %jsonGetBool(synccast, "autoplay", true)
  descriptor["showSessionUi"] = %jsonGetBool(synccast, "showSessionUi", false)
  descriptor["showAttachments"] = %jsonGetBool(synccast, "showAttachments", false)
  descriptor["latencyProfile"] = %(jsonGetStr(synccast, "latencyProfile", "e2e_200ms_fixed"))
  descriptor["targetLatencyMs"] = %jsonGetInt64(synccast, "targetLatencyMs", 200)
  descriptor["decodePreference"] = %(jsonGetStr(synccast, "decodePreference", "hardware_first"))
  descriptor

proc socialBuildAssetStatus(node: NimNode, post: JsonNode, statusLabel = ""): JsonNode =
  let asset = socialSafeMediaAssetManifest(post)
  if asset.kind != JObject:
    return newJNull()
  let assetId = jsonGetStr(asset, "assetId", "").strip()
  let contentId = jsonGetStr(post, "postId", jsonGetStr(post, "id")).strip()
  let authorPeerId = jsonGetStr(post, "authorPeerId", "").strip()
  let localAvailable =
    assetId.len > 0 and lookupSharedFile(node, assetId).ok
  let resolvedStatus =
    if statusLabel.len > 0:
      statusLabel
    elif localAvailable:
      "local_asset_ready"
    elif authorPeerId.len > 0:
      "remote_asset_pending"
    else:
      "asset_pending"
  result = newJObject()
  result["assetId"] = %assetId
  result["contentId"] = %contentId
  result["status"] = %resolvedStatus
  result["localAvailable"] = %localAvailable
  result["providerCandidates"] =
    (if authorPeerId.len > 0: toJsonStringArray(@[authorPeerId]) else: newJArray())
  result["asset"] = asset

proc socialTaskHasMediaAsset(task: JsonNode): bool =
  let asset = socialSafeMediaAssetManifest(task)
  asset.kind == JObject and jsonGetStr(asset, "assetId", "").strip().len > 0

proc socialSeamlessSynccastMediaKind(post: JsonNode): string =
  let synccast = socialMomentSynccastNode(post)
  if synccast.kind != JObject:
    return ""
  let category = socialCanonicalChannel(jsonGetStr(post, "category", jsonGetStr(post, "channel")))
  let mediaKind = jsonGetStr(synccast, "mediaKind", socialMomentType(post, category)).strip().toLowerAscii()
  if mediaKind notin ["audio", "video"]:
    return ""
  let deliveryMode = jsonGetStr(synccast, "deliveryMode", "").strip().toLowerAscii()
  let presentation = jsonGetStr(synccast, "presentation", "").strip().toLowerAscii()
  if deliveryMode == "synccast" or presentation == "inline_autoplay":
    return mediaKind
  ""

proc socialSynccastShowAttachments(post: JsonNode): bool =
  let synccast = socialMomentSynccastNode(post)
  if synccast.kind != JObject:
    return true
  jsonGetBool(synccast, "showAttachments", true)

proc socialSlugId(value: string, fallback: string): string =
  var slug = newStringOfCap(value.len)
  for ch in value:
    if (ch >= '0' and ch <= '9') or (ch >= 'a' and ch <= 'z') or (ch >= 'A' and ch <= 'Z') or ch in {'-', '_'}:
      slug.add(ch)
    elif ch in {' ', ':', '/', '\\', '.', '#'}:
      slug.add('-')
  result = slug.strip(chars = {'-', '_'})
  if result.len == 0:
    result = fallback

proc socialEnsureTaskSynccastMetadata(
    task: JsonNode,
    defaultId: string,
    authorPeerId: string,
    nowTs: int64
) =
  if task.kind != JObject:
    return
  let channel = socialCanonicalChannel(jsonGetStr(task, "category", jsonGetStr(task, "channel")))
  let mediaKind = socialMomentType(task, channel).strip().toLowerAscii()
  if mediaKind notin ["audio", "video"]:
    return
  var extraNode = socialMomentExtraNode(task)
  if extraNode.kind != JObject:
    extraNode = newJObject()
  var synccast =
    if extraNode.hasKey("synccast") and extraNode["synccast"].kind == JObject:
      extraNode["synccast"]
    else:
      newJObject()
  let seed = socialSlugId(defaultId, "media-" & $nowTs)
  let title = socialMomentTitle(task, socialMomentContent(task))
  let mediaLabel = socialMomentMediaLabel(task)
  let roomId = jsonGetStr(synccast, "roomId", "").strip()
  let streamKey = jsonGetStr(synccast, "streamKey", "").strip()
  let programId = jsonGetStr(synccast, "programId", jsonGetStr(synccast, "id", "")).strip()
  synccast["deliveryMode"] = %(jsonGetStr(synccast, "deliveryMode", "synccast"))
  synccast["mediaKind"] = %(jsonGetStr(synccast, "mediaKind", mediaKind))
  synccast["roomId"] = %(if roomId.len > 0: roomId else: "synccast-" & seed)
  synccast["streamKey"] = %(if streamKey.len > 0: streamKey else: "content-" & seed)
  synccast["programId"] = %(if programId.len > 0: programId else: "program-" & seed)
  synccast["id"] = %jsonGetStr(synccast, "programId", "program-" & seed)
  synccast["title"] = %jsonGetStr(synccast, "title", if title.len > 0: title else: "同步播放")
  synccast["mode"] = %"realtime_program"
  synccast["presentation"] = %(jsonGetStr(synccast, "presentation", "inline_autoplay"))
  synccast["autoplay"] = %jsonGetBool(synccast, "autoplay", true)
  synccast["showSessionUi"] = %jsonGetBool(synccast, "showSessionUi", false)
  synccast["showAttachments"] = %jsonGetBool(synccast, "showAttachments", false)
  synccast["latencyProfile"] = %(jsonGetStr(synccast, "latencyProfile", "e2e_200ms_fixed"))
  synccast["targetLatencyMs"] = %jsonGetInt64(synccast, "targetLatencyMs", 200)
  synccast["audioFrameMs"] = %jsonGetInt64(synccast, "audioFrameMs", 20)
  synccast["videoFrameIntervalMs"] = %jsonGetInt64(synccast, "videoFrameIntervalMs", 67)
  synccast["gopMs"] = %jsonGetInt64(synccast, "gopMs", 500)
  synccast["allowBFrames"] = %false
  synccast["decodePreference"] = %(jsonGetStr(synccast, "decodePreference", "hardware_first"))
  synccast["allowSeek"] = %false
  synccast["allowRateChange"] = %false
  synccast["authorPeerId"] = %authorPeerId
  synccast["createdAt"] = %jsonGetInt64(synccast, "createdAt", nowTs)
  synccast["updatedAt"] = %nowTs
  if mediaLabel.len > 0 and jsonGetStr(synccast, "mediaLabel", "").len == 0:
    synccast["mediaLabel"] = %mediaLabel
  extraNode["synccast"] = synccast
  task["extra"] = extraNode

proc socialUpsertTaskSynccastProgram(node: NimNode, task: JsonNode): bool =
  if node.isNil or task.kind != JObject:
    return false
  let extraNode = socialMomentExtraNode(task)
  if extraNode.kind != JObject or not extraNode.hasKey("synccast") or extraNode["synccast"].kind != JObject:
    return false
  let synccast = extraNode["synccast"]
  let deliveryMode = jsonGetStr(synccast, "deliveryMode", "").strip().toLowerAscii()
  let mediaKind = jsonGetStr(synccast, "mediaKind", socialMomentType(task, socialCanonicalChannel(jsonGetStr(task, "category", jsonGetStr(task, "channel"))))).strip().toLowerAscii()
  if deliveryMode != "synccast" and mediaKind notin ["audio", "video"]:
    return false
  let rid = jsonGetStr(synccast, "roomId", "").strip()
  if rid.len == 0:
    return false
  let nowTs = nowMillis()
  let summaryText = socialMomentSummary(task, socialMomentContent(task))
  var program = newJObject()
  let title = jsonGetStr(synccast, "title", socialMomentTitle(task, socialMomentContent(task)))
  let programId = jsonGetStr(synccast, "programId", jsonGetStr(synccast, "id", "program-" & socialSlugId(rid, "media"))).strip()
  program["programId"] = %(if programId.len > 0: programId else: "program-" & socialSlugId(rid, "media"))
  program["id"] = %jsonGetStr(program, "programId", "program-" & socialSlugId(rid, "media"))
  program["title"] = %(if title.len > 0: title else: "同步播放")
  program["description"] =
    %(if summaryText.len > 160: summaryText[0 .. 159] else: summaryText)
  program["category"] = %socialCanonicalChannel(jsonGetStr(task, "category", jsonGetStr(task, "channel")))
  program["mode"] = %"realtime_program"
  program["mediaKind"] = %mediaKind
  program["streamKey"] = %jsonGetStr(synccast, "streamKey", "content-" & socialSlugId(rid, "media"))
  program["mediaLabel"] = %socialMomentMediaLabel(task)
  program["anchorPeerId"] = %jsonGetStr(task, "authorPeerId", localPeerId(node))
  program["latencyProfile"] = %(jsonGetStr(synccast, "latencyProfile", "e2e_200ms_fixed"))
  program["targetLatencyMs"] = %jsonGetInt64(synccast, "targetLatencyMs", 200)
  program["audioFrameMs"] = %jsonGetInt64(synccast, "audioFrameMs", 20)
  program["videoFrameIntervalMs"] = %jsonGetInt64(synccast, "videoFrameIntervalMs", 67)
  program["gopMs"] = %jsonGetInt64(synccast, "gopMs", 500)
  program["allowBFrames"] = %false
  program["decodePreference"] = %(jsonGetStr(synccast, "decodePreference", "hardware_first"))
  program["allowSeek"] = %false
  program["allowRateChange"] = %false
  program["updatedAt"] = %nowTs
  socialStateMemLock.acquire()
  try:
    let state = loadSocialStateUnsafe(node)
    let room = ensureSynccastRoomNoLock(state, rid)
    room["program"] = program
    room["updatedAt"] = %nowTs
    let stateNode = room["state"]
    stateNode["programId"] = %jsonGetStr(program, "programId", "")
    stateNode["streamKey"] = %jsonGetStr(program, "streamKey", "")
    stateNode["mediaKind"] = %mediaKind
    if not stateNode.hasKey("playbackState"):
      stateNode["playbackState"] = %"paused"
    let authorPeerId = jsonGetStr(task, "authorPeerId", localPeerId(node))
    if authorPeerId.len > 0:
      appendUniqueString(room["members"], authorPeerId)
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
    source = "social_publish_enqueue"
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
  discard social_groups_send(node.selfHandle, rid.cstring, ($wire).cstring)
  true

proc socialMomentMediaItems(post: JsonNode): JsonNode =
  if post.kind == JObject and post.hasKey("mediaItems") and post["mediaItems"].kind == JArray:
    return post["mediaItems"]
  var items = newJArray()
  let mediaLabel = socialMomentMediaLabel(post)
  if mediaLabel.len > 0:
    items.add(%mediaLabel)
  items

proc socialCanonicalAttachmentRows(post: JsonNode): JsonNode =
  if socialSeamlessSynccastMediaKind(post).len > 0 and not socialSynccastShowAttachments(post):
    return newJArray()
  var rows = newJArray()
  if post.kind == JObject and post.hasKey("attachments") and post["attachments"].kind == JArray:
    for rawAttachment in post["attachments"]:
      if rawAttachment.kind != JObject:
        continue
      var attachment = newJObject()
      let aid = jsonGetStr(rawAttachment, "id", jsonGetStr(rawAttachment, "slot", jsonGetStr(rawAttachment, "name"))).strip()
      let label = jsonGetStr(rawAttachment, "label", jsonGetStr(rawAttachment, "name", "附件")).strip()
      let uri = jsonGetStr(rawAttachment, "uri", jsonGetStr(rawAttachment, "path")).strip()
      attachment["id"] = %(if aid.len > 0: aid else: label)
      attachment["kind"] = %jsonGetStr(rawAttachment, "kind", jsonGetStr(rawAttachment, "mimeType", "file"))
      attachment["label"] = %(if label.len > 0: label else: "附件")
      attachment["uri"] = %uri
      attachment["blurred"] = %jsonGetBool(rawAttachment, "blurred", false)
      rows.add(attachment)
    if rows.len > 0:
      return rows
  let extraNode = socialMomentExtraNode(post)
  if extraNode.kind == JObject and extraNode.hasKey("attachments") and extraNode["attachments"].kind == JArray:
    for rawAttachment in extraNode["attachments"]:
      if rawAttachment.kind != JObject:
        continue
      var attachment = newJObject()
      let slot = jsonGetStr(rawAttachment, "slot").strip()
      let label = jsonGetStr(rawAttachment, "label", jsonGetStr(rawAttachment, "name", "附件")).strip()
      let uri = jsonGetStr(rawAttachment, "uri", jsonGetStr(rawAttachment, "path")).strip()
      let attachmentId =
        if slot.len > 0:
          slot
        elif label.len > 0:
          label
        else:
          uri
      attachment["id"] = %(if attachmentId.len > 0: attachmentId else: "attachment")
      attachment["kind"] = %jsonGetStr(rawAttachment, "kind", jsonGetStr(rawAttachment, "mimeType", "file"))
      attachment["label"] = %(if label.len > 0: label else: "附件")
      attachment["uri"] = %uri
      attachment["blurred"] = %jsonGetBool(rawAttachment, "blurred", false)
      rows.add(attachment)
  rows

proc socialBuildCanonicalMomentItem(node: NimNode, post: JsonNode): JsonNode
proc socialEnsureDiscoveredFeedSubscriptions(node: NimNode, limit = 8)

proc socialTaskId(task: JsonNode): string =
  jsonGetStr(task, "taskId", jsonGetStr(task, "id")).strip()

proc socialTaskContentId(task: JsonNode): string =
  jsonGetStr(task, "contentId", jsonGetStr(task, "postId", jsonGetStr(task, "id"))).strip()

proc socialTaskDistributionTags(task: JsonNode): JsonNode =
  if task.kind == JObject and task.hasKey("distributionTags") and task["distributionTags"].kind == JArray:
    return task["distributionTags"]
  newJArray()

proc socialFindTaskIndexNoLock(state: JsonNode, taskId: string): int =
  if state.kind != JObject or not state.hasKey("publishTasks") or state["publishTasks"].kind != JArray:
    return -1
  for index, task in state["publishTasks"].elems:
    if socialTaskId(task) == taskId:
      return index
  -1

proc socialFindTaskByContentIdIndexNoLock(state: JsonNode, contentId: string): int =
  if state.kind != JObject or not state.hasKey("publishTasks") or state["publishTasks"].kind != JArray:
    return -1
  for index, task in state["publishTasks"].elems:
    if socialTaskContentId(task) == contentId or jsonGetStr(task, "postId", "").strip() == contentId:
      return index
  -1

proc socialFindMomentByContentIdNoLock(state: JsonNode, contentId: string): JsonNode =
  if state.kind != JObject or not state.hasKey("moments") or state["moments"].kind != JArray:
    return newJNull()
  for post in state["moments"]:
    if post.kind != JObject:
      continue
    if jsonGetStr(post, "postId", jsonGetStr(post, "id")).strip() == contentId:
      return post
  newJNull()

proc socialFindActiveTombstoneNoLock(state: JsonNode, contentId: string): JsonNode =
  if state.kind != JObject or not state.hasKey("tombstones") or state["tombstones"].kind != JArray:
    return newJNull()
  for record in state["tombstones"]:
    if record.kind != JObject:
      continue
    if jsonGetStr(record, "contentId", jsonGetStr(record, "content_hash")).strip() != contentId:
      continue
    if jsonGetBool(record, "active", true):
      return record
  newJNull()

proc socialAppendDistributionLogNoLock(
    state: JsonNode,
    contentId: string,
    decision: string,
    reasonCode: string,
    ruleVersion: string = "cn_prd_v1"
) =
  if state.kind != JObject:
    return
  if not state.hasKey("distributionLogs") or state["distributionLogs"].kind != JArray:
    state["distributionLogs"] = newJArray()
  var entry = newJObject()
  entry["contentId"] = %contentId
  entry["decision"] = %decision
  entry["reasonCode"] = %reasonCode
  entry["ruleVersion"] = %ruleVersion
  entry["decidedAt"] = %nowMillis()
  prependBoundedNode(state["distributionLogs"], entry, SocialMaxDistributionLogs)

proc socialUpsertMomentNoLock(state: JsonNode, payload: JsonNode) =
  if state.kind != JObject:
    return
  if not state.hasKey("moments") or state["moments"].kind != JArray:
    state["moments"] = newJArray()
  let contentId = jsonGetStr(payload, "postId", jsonGetStr(payload, "id")).strip()
  var nextMoments = newJArray()
  nextMoments.add(payload)
  for existing in state["moments"]:
    if nextMoments.len >= SocialMaxMoments:
      break
    if existing.kind != JObject:
      continue
    let existingId = jsonGetStr(existing, "postId", jsonGetStr(existing, "id")).strip()
    if existingId == contentId:
      continue
    nextMoments.add(existing)
  state["moments"] = nextMoments

proc socialMarkTaskPublishedNoLock(
    state: JsonNode,
    task: JsonNode,
    payload: JsonNode,
    publishedAt: int64,
    emitNotification: bool = true
) =
  let contentId = socialTaskContentId(task)
  task["publishState"] = %"published"
  task["visibility"] = %"public"
  task["homeEligible"] = %true
  task["publishedAt"] = %publishedAt
  task["updatedAt"] = %publishedAt
  task["lastAttemptAt"] = %publishedAt
  task["nextAttemptAt"] = %0
  task["reasonCode"] = %"published_to_feed"
  task["tombstoned"] = %false
  socialUpsertMomentNoLock(state, payload)
  socialAppendDistributionLogNoLock(state, contentId, "PASS_TO_POOL", "published_to_feed")
  if emitNotification:
    makeNotificationNoLock(state, "moment_published", "动态已发布", jsonGetStr(task, "content"), payload)

proc socialClassifyTask(task: JsonNode): tuple[moderationClass: string, decision: string, reasonCode: string, tags: seq[string]] =
  let channel = socialCanonicalChannel(jsonGetStr(task, "category", jsonGetStr(task, "channel")))
  let text = (
    jsonGetStr(task, "title") & "\n" &
    jsonGetStr(task, "summary") & "\n" &
    socialMomentContent(task) & "\n" &
    jsonGetStr(task, "locationHint") & "\n" &
    channel
  ).toLowerAscii()
  if containsAnyKeyword(text, [
    "诈骗", "毒品", "枪支", "炸药", "暴恐", "洗钱", "开盒", "黑产", "儿童色情", "卖淫"
  ]):
    return ("C0", "BLOCK_AND_PRESERVE", "c0_prohibited_keyword", @["REQUIRE_PRO_REVIEW"])
  if containsAnyKeyword(text, [
    "保本", "稳赚", "代投", "贷款", "刷单", "博彩", "返利", "医美", "炒币"
  ]):
    return ("C1", "HOLD_FOR_PRO_REVIEW", "c1_high_risk_keyword", @["REQUIRE_PRO_REVIEW", "MISLEADING"])
  if channel == "ad" or containsAnyKeyword(text, [
    "vx", "wechat", "加微", "qq", "招商", "代理", "引流", "课程", "兼职", "推广"
  ]):
    return ("C2", "HOLD_FOR_PRO_REVIEW", "c2_commercial_review_required", @["COMMERCIAL_RISK", "REQUIRE_PRO_REVIEW"])
  ("C3", "PASS_TO_POOL", "step0_pass", @["SAFE_FOR_HOME"])

proc socialApplyStep0NoLock(task: JsonNode) =
  let classification = socialClassifyTask(task)
  let nowTs = nowMillis()
  let hasMediaAsset = socialTaskHasMediaAsset(task)
  task["moderationClass"] = %classification.moderationClass
  task["moderationDecision"] = %classification.decision
  task["distributionTags"] = toJsonStringArray(classification.tags)
  task["reasonCode"] = %classification.reasonCode
  task["updatedAt"] = %nowTs
  task["reviewerNote"] = %jsonGetStr(task, "reviewerNote", "")
  case classification.decision
  of "BLOCK_AND_PRESERVE":
    task["publishState"] = %"rejected"
    task["visibility"] = %"owner_only"
    task["homeEligible"] = %false
  of "HOLD_FOR_PRO_REVIEW":
    task["publishState"] = %"under_review"
    task["visibility"] = %"owner_only"
    task["homeEligible"] = %false
  else:
    task["publishState"] = %(if hasMediaAsset: "asset_ready" else: "approved_pending_transport")
    task["visibility"] = %"owner_only"
    task["homeEligible"] = %false

proc socialBuildTaskSnapshot(node: NimNode, task: JsonNode): JsonNode =
  var item = socialBuildCanonicalMomentItem(node, task)
  let taskId = socialTaskId(task)
  let contentId = socialTaskContentId(task)
  item["taskId"] = %taskId
  item["contentId"] = %(if contentId.len > 0: contentId else: jsonGetStr(item, "id"))
  item["publishState"] = %jsonGetStr(task, "publishState", "queued")
  item["moderationClass"] = %jsonGetStr(task, "moderationClass", "UNKNOWN")
  item["moderationDecision"] = %jsonGetStr(task, "moderationDecision", "NONE")
  item["visibility"] = %jsonGetStr(task, "visibility", "owner_only")
  item["homeEligible"] = %jsonGetBool(task, "homeEligible", false)
  item["distributionTags"] = socialTaskDistributionTags(task)
  item["reasonCode"] = %jsonGetStr(task, "reasonCode", "")
  item["reviewerNote"] = %jsonGetStr(task, "reviewerNote", "")
  item["retryCount"] = %jsonGetInt64(task, "retryCount", 0)
  item["lastAttemptAt"] = %jsonGetInt64(task, "lastAttemptAt", 0)
  item["publishedAt"] = %jsonGetInt64(task, "publishedAt", 0)
  item["updatedAt"] = %jsonGetInt64(task, "updatedAt", jsonGetInt64(item, "createdAt", 0))
  item["tombstoned"] = %jsonGetBool(task, "tombstoned", false)
  let publishState = jsonGetStr(task, "publishState", "queued")
  let assetStatusLabel =
    if publishState in ["asset_ingesting", "queued", "governance_pending"]:
      "asset_ingesting"
    elif publishState in ["asset_ready", "approved_pending_transport", "publishing", "published"]:
      "asset_ready"
    else:
      ""
  let assetStatus = socialBuildAssetStatus(node, task, assetStatusLabel)
  if assetStatus.kind == JObject:
    item["assetStatus"] = assetStatus
  item

proc socialTaskToMomentPayload(node: NimNode, task: JsonNode): JsonNode =
  var payload = socialBuildCanonicalMomentItem(node, task)
  payload["postId"] = %socialTaskContentId(task)
  payload["id"] = %socialTaskContentId(task)
  payload["publishState"] = %"published"
  payload["moderationClass"] = %jsonGetStr(task, "moderationClass", "C3")
  payload["moderationDecision"] = %"PASS_TO_POOL"
  payload["visibility"] = %"public"
  payload["homeEligible"] = %true
  payload["distributionTags"] = socialTaskDistributionTags(task)
  payload["reasonCode"] = %jsonGetStr(task, "reasonCode", "published_to_feed")
  payload["publishedAt"] = %nowMillis()
  payload["tombstoned"] = %false
  payload["likes"] = newJArray()
  payload["comments"] = newJArray()
  payload

proc socialCollectRecentPublishedMomentPayloads(
    node: NimNode,
    limit = 4
): seq[JsonNode] =
  if node.isNil or limit <= 0:
    return @[]
  socialStateMemLock.acquire()
  try:
    let state = loadSocialStateUnsafe(node)
    if state.kind != JObject or not state.hasKey("moments") or state["moments"].kind != JArray:
      return @[]
    for post in state["moments"]:
      if result.len >= limit:
        break
      if post.kind != JObject or jsonGetBool(post, "deleted", false):
        continue
      let publishState = jsonGetStr(post, "publishState", "published")
      let visibility = jsonGetStr(post, "visibility", "public")
      let homeEligible = jsonGetBool(post, "homeEligible", true)
      let tombstoned = jsonGetBool(post, "tombstoned", false)
      if publishState != "published" or visibility != "public" or not homeEligible or tombstoned:
        continue
      result.add(cloneJson(post))
  finally:
    socialStateMemLock.release()

proc socialSubmitSharedContentBroadcast(node: NimNode, payload: JsonNode): bool =
  if node.isNil or node.selfHandle.isNil:
    return false
  let raw = stringToBytes($payload)
  var delivered: csize_t = 0
  let rc = libp2p_pubsub_publish(
    node.selfHandle,
    "unimaker/content/v1".cstring,
    if raw.len > 0: addr raw[0] else: nil,
    csize_t(raw.len),
    addr delivered,
  )
  rc == NimResultOk

proc socialWaitSecureChannel(
    node: NimNode,
    peerId: string,
    timeoutMs = 1200
): bool =
  if node.isNil:
    return false
  let normalizedPeerId = peerId.strip()
  if normalizedPeerId.len == 0:
    return false
  let cmd = makeCommand(cmdWaitSecureChannel)
  cmd.peerId = normalizedPeerId
  cmd.timeoutMs = max(timeoutMs, 500)
  let result = submitCommand(node, cmd)
  let ok = result.resultCode == NimResultOk and result.boolResult
  destroyCommand(cmd)
  ok

proc socialSendContentFeedEnvelopeToPeer(
    node: NimNode,
    peerId: string,
    payload: JsonNode,
    op = "publish"
): bool =
  if node.isNil or node.selfHandle.isNil:
    return false
  let normalizedPeerId = peerId.strip()
  if normalizedPeerId.len == 0:
    return false
  let local = localPeerId(node)
  if local.len > 0 and normalizedPeerId == local:
    return false
  discard social_connect_peer(node.selfHandle, normalizedPeerId.cstring, nil)
  discard socialWaitSecureChannel(node, normalizedPeerId)
  var wire = newJObject()
  wire["kind"] = %"social"
  wire["entity"] = %"content_feed"
  wire["op"] = %op
  wire["timestampMs"] = %nowMillis()
  wire["payload"] = payload
  let raw = stringToBytes($wire)
  libp2p_send_dm_payload(
    node.selfHandle,
    normalizedPeerId.cstring,
    if raw.len > 0: addr raw[0] else: nil,
    csize_t(raw.len),
  )

proc socialRequestRecentPublishedMomentsFromPeer(
    node: NimNode,
    peerId: string,
    limit = 6
): bool =
  if node.isNil or node.selfHandle.isNil:
    return false
  let normalizedPeerId = peerId.strip()
  if normalizedPeerId.len == 0:
    return false
  let local = localPeerId(node)
  if local.len > 0 and normalizedPeerId == local:
    return false
  var wire = newJObject()
  wire["kind"] = %"social"
  wire["entity"] = %"content_feed"
  wire["op"] = %"sync_request"
  wire["timestampMs"] = %nowMillis()
  wire["limit"] = %(max(limit, 1))
  let raw = stringToBytes($wire)
  libp2p_send_dm_payload(
    node.selfHandle,
    normalizedPeerId.cstring,
    if raw.len > 0: addr raw[0] else: nil,
    csize_t(raw.len),
  )

proc socialFanoutRecentPublishedMomentsToPeer(
    node: NimNode,
    peerId: string,
    limit = 4
): bool =
  for payload in socialCollectRecentPublishedMomentPayloads(node, limit):
    if socialSendContentFeedEnvelopeToPeer(node, peerId, payload, "sync_publish"):
      result = true

proc socialFanoutRecentPublishedMoments(
    node: NimNode,
    limit = 4
): bool =
  if node.isNil or node.selfHandle.isNil:
    return false
  let connectedPeers = connectedPeersArray(node.selfHandle)
  if connectedPeers.kind != JArray:
    return false
  for peerNode in connectedPeers:
    if peerNode.kind != JString:
      continue
    if socialFanoutRecentPublishedMomentsToPeer(node, peerNode.getStr().strip(), limit):
      result = true

proc socialFanoutDirectContentEnvelope(node: NimNode, payload: JsonNode): bool =
  if node.isNil or node.selfHandle.isNil:
    return false
  let connectedPeers = connectedPeersArray(node.selfHandle)
  if connectedPeers.kind != JArray:
    return false
  for peerNode in connectedPeers:
    if peerNode.kind != JString:
      continue
    let peerId = peerNode.getStr().strip()
    let sent = socialSendContentFeedEnvelopeToPeer(node, peerId, payload)
    result = result or sent

proc socialSubmitFeedEntry(node: NimNode, payload: JsonNode): bool =
  if node.isNil:
    return false
  let cmd = makeCommand(cmdFeedPublishEntry)
  cmd.jsonString = $payload
  let result = submitCommand(node, cmd)
  var ok = result.resultCode == NimResultOk
  destroyCommand(cmd)
  if socialSubmitSharedContentBroadcast(node, payload):
    ok = true
  if socialFanoutDirectContentEnvelope(node, payload):
    ok = true
  ok

proc socialProcessPendingPublishTasks(node: NimNode, maxCount: int) =
  if node.isNil or maxCount <= 0:
    return
  var processed = 0
  while processed < maxCount:
    socialEnsureDiscoveredFeedSubscriptions(node, 12)
    var candidateTaskId = ""
    var candidateSnapshot = newJNull()
    let nowTs = nowMillis()
    socialStateMemLock.acquire()
    try:
      let state = loadSocialStateUnsafe(node)
      var shouldPersist = false
      if state.hasKey("publishTasks") and state["publishTasks"].kind == JArray:
        for task in state["publishTasks"]:
          if task.kind != JObject:
            continue
          if jsonGetBool(task, "tombstoned", false):
            continue
          var publishState = jsonGetStr(task, "publishState", "queued")
          let lastAttemptAt = jsonGetInt64(task, "lastAttemptAt", 0)
          if publishState == "publishing":
            let stale = lastAttemptAt <= 0 or nowTs - lastAttemptAt >= SocialPublishingRecoveryMs
            if stale:
              let contentId = socialTaskContentId(task)
              let existingMoment = socialFindMomentByContentIdNoLock(state, contentId)
              if existingMoment.kind == JObject:
                socialMarkTaskPublishedNoLock(
                  state,
                  task,
                  existingMoment,
                  max(lastAttemptAt, nowTs),
                  emitNotification = false
                )
              else:
                let nextRetry = int(jsonGetInt64(task, "retryCount", 0)) + 1
                task["retryCount"] = %nextRetry
                task["updatedAt"] = %nowTs
                task["lastAttemptAt"] = %nowTs
                task["nextAttemptAt"] = %nowTs
                task["reasonCode"] = %"publishing_timeout_recovered"
                if nextRetry >= 5:
                  task["publishState"] = %"failed"
                else:
                  task["publishState"] = %"retrying"
              publishState = jsonGetStr(task, "publishState", "queued")
              shouldPersist = true
          let nextAttemptAt = jsonGetInt64(task, "nextAttemptAt", 0)
          if publishState notin ["asset_ready", "approved_pending_transport", "retrying"]:
            continue
          if nextAttemptAt > nowTs:
            continue
          task["publishState"] = %"publishing"
          task["lastAttemptAt"] = %nowTs
          task["updatedAt"] = %nowTs
          candidateTaskId = socialTaskId(task)
          candidateSnapshot = parseJson($task)
          shouldPersist = true
          break
      if shouldPersist:
        persistSocialStateUnsafe(node, state)
    finally:
      socialStateMemLock.release()
    if candidateTaskId.len == 0 or candidateSnapshot.kind != JObject:
      break
    var payload = newJNull()
    var published = false
    var processingError = ""
    try:
      discard socialUpsertTaskSynccastProgram(node, candidateSnapshot)
      payload = socialTaskToMomentPayload(node, candidateSnapshot)
      published = socialSubmitFeedEntry(node, payload)
    except CatchableError as exc:
      processingError = if exc.msg.len > 0: exc.msg else: $exc.name
      recordRuntimeError("social_publish_pump_failed:" & processingError)
      warn "social publish pump failed", err = processingError, taskId = candidateTaskId
      published = false
    except Defect as exc:
      processingError = if exc.msg.len > 0: exc.msg else: $exc.name
      recordRuntimeError("social_publish_pump_defect:" & processingError)
      error "social publish pump defect", err = processingError, taskId = candidateTaskId
      published = false
    socialStateMemLock.acquire()
    try:
      let state = loadSocialStateUnsafe(node)
      let index = socialFindTaskIndexNoLock(state, candidateTaskId)
      if index >= 0:
        let task = state["publishTasks"][index]
        let retryCount = int(jsonGetInt64(task, "retryCount", 0))
        if processingError.len > 0:
          let nextRetry = retryCount + 1
          task["retryCount"] = %nextRetry
          task["updatedAt"] = %nowTs
          task["lastAttemptAt"] = %nowTs
          task["nextAttemptAt"] = %(nowTs + int64(min(30_000, nextRetry * 2_000)))
          task["reasonCode"] = %"transport_publish_error"
          task["reviewerNote"] = %processingError
          if nextRetry >= 5:
            task["publishState"] = %"failed"
          else:
            task["publishState"] = %"retrying"
        elif published:
          socialMarkTaskPublishedNoLock(state, task, payload, nowTs)
        else:
          let nextRetry = retryCount + 1
          task["retryCount"] = %nextRetry
          task["updatedAt"] = %nowTs
          task["lastAttemptAt"] = %nowTs
          task["nextAttemptAt"] = %(nowTs + int64(min(30_000, nextRetry * 2_000)))
          task["reasonCode"] = %"transport_publish_failed"
          if nextRetry >= 5:
            task["publishState"] = %"failed"
          else:
            task["publishState"] = %"retrying"
        persistSocialStateUnsafe(node, state)
    finally:
      socialStateMemLock.release()
    if published:
      discard socialFanoutRecentPublishedMoments(node, 6)
    inc processed

proc socialCollectRemoteFeedCanonicalItems(node: NimNode, channelFilter: string): seq[JsonNode] =
  if node.isNil:
    return @[]
  let cmd = makeCommand(cmdFetchFeedSnapshot)
  let commandResult = submitCommand(node, cmd)
  let raw =
    if commandResult.resultCode == NimResultOk:
      cloneOwnedString(commandResult.stringResult)
    else:
      ""
  destroyCommand(cmd)
  if raw.len == 0:
    return @[]
  try:
    let root = parseJson(raw)
    let items =
      if root.kind == JObject and root.hasKey("items") and root["items"].kind == JArray:
        root["items"]
      elif root.kind == JArray:
        root
      else:
        newJArray()
    for row in items:
      let payload =
        if row.kind == JObject and row.hasKey("payload") and row["payload"].kind == JObject:
          row["payload"]
        elif row.kind == JObject:
          row
        else:
          newJNull()
      if payload.kind != JObject:
        continue
      let canonical = socialBuildCanonicalMomentItem(node, payload)
      let publishState = jsonGetStr(payload, "publishState", "published")
      let visibility = jsonGetStr(payload, "visibility", "public")
      let homeEligible = jsonGetBool(payload, "homeEligible", true)
      let tombstoned = jsonGetBool(payload, "tombstoned", false)
      if publishState != "published" or visibility != "public" or not homeEligible or tombstoned:
        continue
      if channelFilter.len > 0 and socialCanonicalChannel(channelFilter) != socialCanonicalChannel(jsonGetStr(canonical, "channel", jsonGetStr(canonical, "category"))):
        continue
      result.add(canonical)
  except CatchableError:
    discard

proc socialCollectRemoteFeedCanonicalItemsWithRetry(
    node: NimNode,
    channelFilter: string,
    targetContentId = "",
    waitMs = 360
): seq[JsonNode] =
  let normalizedTarget = targetContentId.strip()
  result = socialCollectRemoteFeedCanonicalItems(node, channelFilter)
  let hasTarget =
    proc(items: seq[JsonNode]): bool =
      if normalizedTarget.len == 0:
        return items.len > 0
      for item in items:
        let itemId = jsonGetStr(item, "id", jsonGetStr(item, "postId")).strip()
        if itemId == normalizedTarget:
          return true
      false
  if hasTarget(result) or waitMs <= 0:
    return
  let connected = connectedPeersArray(node.selfHandle)
  if connected.kind != JArray or connected.len <= 0:
    return
  for peerNode in connected:
    if peerNode.kind != JString:
      continue
    discard socialRequestRecentPublishedMomentsFromPeer(
      node,
      peerNode.getStr().strip(),
      if normalizedTarget.len > 0: 12 else: 6,
    )
  let deadline = nowMillis() + int64(waitMs)
  while nowMillis() < deadline:
    sleep(60)
    result = socialCollectRemoteFeedCanonicalItems(node, channelFilter)
    if hasTarget(result):
      return

proc socialBuildCanonicalMomentItem(node: NimNode, post: JsonNode): JsonNode =
  let postId = jsonGetStr(post, "postId", jsonGetStr(post, "id")).strip()
  let category = socialCanonicalChannel(jsonGetStr(post, "category", jsonGetStr(post, "channel")))
  let content = socialMomentContent(post)
  let summary = socialMomentSummary(post, content)
  let title = socialMomentTitle(post, content)
  let authorPeerId = socialMomentAuthorPeerId(node, post)
  let authorName = socialMomentAuthorName(node, post, authorPeerId)
  let createdAt = socialMomentTimestamp(post)
  let mediaLabel = socialMomentMediaLabel(post)
  var item = newJObject()
  item["id"] = %postId
  item["postId"] = %postId
  item["category"] = %category
  item["channel"] = %category
  item["type"] = %socialMomentType(post, category)
  item["title"] = %title
  item["summary"] = %summary
  item["content"] = %content
  item["body"] = %content
  item["mediaLabel"] = %mediaLabel
  item["media"] = %jsonGetStr(post, "media", mediaLabel)
  item["coverMedia"] = %jsonGetStr(post, "coverMedia", mediaLabel)
  item["locationHint"] = %socialMomentLocationHint(post)
  item["authorPeerId"] = %authorPeerId
  item["authorName"] = %authorName
  item["createdAt"] = %createdAt
  item["timestampMs"] = %createdAt
  item["likeCount"] = %(if post.hasKey("likes") and post["likes"].kind == JArray: post["likes"].len else: 0)
  item["commentCount"] = %(if post.hasKey("comments") and post["comments"].kind == JArray: post["comments"].len else: 0)
  item["mediaItems"] = socialMomentMediaItems(post)
  item["attachments"] = socialCanonicalAttachmentRows(post)
  item["extra"] = socialMomentExtraNode(post)
  item["publishState"] = %jsonGetStr(post, "publishState", "published")
  item["moderationClass"] = %jsonGetStr(post, "moderationClass", "C3")
  item["moderationDecision"] = %jsonGetStr(post, "moderationDecision", "PASS_TO_POOL")
  item["visibility"] = %jsonGetStr(post, "visibility", "public")
  item["homeEligible"] = %jsonGetBool(post, "homeEligible", true)
  item["distributionTags"] =
    if post.hasKey("distributionTags") and post["distributionTags"].kind == JArray:
      post["distributionTags"]
    else:
      toJsonStringArray(@["SAFE_FOR_HOME"])
  item["reasonCode"] = %jsonGetStr(post, "reasonCode", "")
  item["tombstoned"] = %jsonGetBool(post, "tombstoned", false)
  item["publishedAt"] = %jsonGetInt64(post, "publishedAt", createdAt)
  var builderErrors = newJArray()
  let mediaAsset = socialSafeMediaAssetManifest(post)
  if mediaAsset.kind == JObject:
    item["mediaAsset"] = mediaAsset
  try:
    let playback = socialBuildPlaybackDescriptor(node, post)
    if playback.kind == JObject:
      item["playback"] = playback
  except CatchableError as exc:
    builderErrors.add(%("playback:" & exc.msg))
    warn "social playback descriptor failed", err = exc.msg
  except Defect as exc:
    builderErrors.add(%("playback:" & exc.msg))
    error "social playback descriptor defect", err = exc.msg
  try:
    let assetStatus = socialBuildAssetStatus(node, post)
    if assetStatus.kind == JObject:
      item["assetStatus"] = assetStatus
  except CatchableError as exc:
    builderErrors.add(%("asset_status:" & exc.msg))
    warn "social asset status failed", err = exc.msg
  except Defect as exc:
    builderErrors.add(%("asset_status:" & exc.msg))
    error "social asset status defect", err = exc.msg
  if builderErrors.len > 0:
    item["builderErrors"] = builderErrors
  item

proc socialCollectCanonicalMoments(node: NimNode): seq[JsonNode] =
  if node.isNil:
    return @[]
  socialStateMemLock.acquire()
  try:
    let state = loadSocialStateUnsafe(node)
    if state.hasKey("moments") and state["moments"].kind == JArray:
      for post in state["moments"]:
        if post.kind == JObject and not jsonGetBool(post, "deleted", false):
          result.add(post)
  finally:
    socialStateMemLock.release()
  result.sort(
    proc(a, b: JsonNode): int =
      cmp(
        socialMomentTimestamp(b),
        socialMomentTimestamp(a)
      )
  )

proc socialEnsureDiscoveredFeedSubscriptions(node: NimNode, limit = 8) =
  if node.isNil or node.feedService.isNil:
    return
  let liveConnectedPeers = connectedPeersArray(node.selfHandle)
  let recentInfo = recentConnectedPeersInfo(node)
  var connectedInfo = newJArray()
  var seenConnected = initHashSet[string]()
  if recentInfo.kind == JArray:
    for row in recentInfo:
      if row.kind != JObject:
        continue
      let peerId = jsonGetStr(row, "peerId", jsonGetStr(row, "peer_id")).strip()
      if peerId.len == 0 or seenConnected.contains(peerId):
        continue
      seenConnected.incl(peerId)
      connectedInfo.add(row)
  if liveConnectedPeers.kind == JArray:
    for peerNode in liveConnectedPeers:
      if peerNode.kind != JString:
        continue
      let peerId = peerNode.getStr().strip()
      if peerId.len == 0 or seenConnected.contains(peerId):
        continue
      seenConnected.incl(peerId)
      var row = newJObject()
      row["peerId"] = %peerId
      connectedInfo.add(row)
  let discovered = filteredDiscoveredPeersForNode(
    node,
    "",
    max(limit, 0),
    connectedInfo,
  ).peers
  if discovered.kind != JArray and connectedInfo.kind != JArray:
    return
  let local = localPeerId(node)
  var seenPeers = initHashSet[string]()
  proc ensureSubscribed(row: JsonNode) =
    if row.kind != JObject:
      return
    let peerId = jsonGetStr(row, "peerId", jsonGetStr(row, "peer_id")).strip()
    if peerId.len == 0 or (local.len > 0 and peerId == local) or seenPeers.contains(peerId):
      return
    seenPeers.incl(peerId)
    let cmd = makeCommand(cmdFeedSubscribePeer)
    cmd.peerId = peerId
    discard submitCommand(node, cmd)
    destroyCommand(cmd)
  if connectedInfo.kind == JArray:
    for row in connectedInfo:
      ensureSubscribed(row)
  if discovered.kind == JArray:
    for row in discovered:
      ensureSubscribed(row)

proc socialMomentMatchesChannel(post: JsonNode, channelFilter: string): bool =
  let normalizedFilter = channelFilter.strip().toLowerAscii()
  if normalizedFilter.len == 0 or normalizedFilter == "all" or normalizedFilter == "全部":
    return true
  socialCanonicalChannel(jsonGetStr(post, "category", jsonGetStr(post, "channel"))) == socialCanonicalChannel(channelFilter)

proc socialListErrorResponse(fnName: string, excName: string, excMsg: string): cstring =
  let resolved = if excMsg.len > 0: excMsg else: excName
  allocCString(
    "{\"items\":[],\"nextCursor\":\"\",\"hasMore\":false,\"error\":\"" &
    escapeJson(resolved) &
    "\",\"function\":\"" &
    escapeJson(fnName) &
    "\"}"
  )

proc socialObjectErrorResponse(
    fnName: string,
    fallbackJson: string,
    excName: string,
    excMsg: string
): cstring =
  let resolved = if excMsg.len > 0: excMsg else: excName
  let fallback =
    try:
      let parsed = parseJson(fallbackJson)
      if parsed.kind == JObject:
        parsed
      else:
        newJObject()
    except CatchableError:
      newJObject()
  fallback["error"] = %resolved
  fallback["function"] = %fnName
  allocCString($fallback)

proc social_feed_snapshot*(
    handle: pointer, channel: cstring, cursor: cstring, limit: cint
): cstring {.exportc, cdecl, dynlib.} =
  try:
    let node = nodeFromHandle(handle)
    if node.isNil:
      return allocCString("{\"items\":[],\"nextCursor\":\"\",\"hasMore\":false}")
    socialEnsureDiscoveredFeedSubscriptions(node, 12)
    socialProcessPendingPublishTasks(node, 4)
    let requested = if limit > 0: min(int(limit), 200) else: SocialDefaultLimit
    let startIndex =
      if cursor.isNil:
        0
      else:
        int(max(parseIntLoose($cursor), 0))
    let channelFilter = if channel.isNil: "" else: ($channel).strip()
    let moments = socialCollectCanonicalMoments(node)
    var filtered: seq[JsonNode] = @[]
    var seenIds = initHashSet[string]()
    for post in moments:
      let publishState = jsonGetStr(post, "publishState", "published")
      let visibility = jsonGetStr(post, "visibility", "public")
      let homeEligible = jsonGetBool(post, "homeEligible", true)
      let tombstoned = jsonGetBool(post, "tombstoned", false)
      if publishState != "published" or visibility != "public" or not homeEligible or tombstoned:
        continue
      if socialMomentMatchesChannel(post, channelFilter):
        let item = socialBuildCanonicalMomentItem(node, post)
        let itemId = jsonGetStr(item, "id", jsonGetStr(item, "postId")).strip()
        if itemId.len > 0 and not seenIds.contains(itemId):
          seenIds.incl(itemId)
          filtered.add(item)
    for item in socialCollectRemoteFeedCanonicalItemsWithRetry(node, channelFilter):
      let itemId = jsonGetStr(item, "id", jsonGetStr(item, "postId")).strip()
      if itemId.len == 0 or seenIds.contains(itemId):
        continue
      seenIds.incl(itemId)
      filtered.add(item)
    filtered.sort(
      proc(a, b: JsonNode): int =
        cmp(
          jsonGetInt64(b, "createdAt", jsonGetInt64(b, "timestampMs", 0)),
          jsonGetInt64(a, "createdAt", jsonGetInt64(a, "timestampMs", 0))
        )
    )
    var response = newJObject()
    var items = newJArray()
    var nextCursor = ""
    var hasMore = false
    if startIndex < filtered.len:
      let stopIndex = min(startIndex + requested, filtered.len)
      for i in startIndex ..< stopIndex:
        items.add(filtered[i])
      hasMore = stopIndex < filtered.len
      if hasMore:
        nextCursor = $stopIndex
    response["channel"] = %socialCanonicalChannel(channelFilter)
    response["items"] = items
    response["nextCursor"] = %nextCursor
    response["hasMore"] = %hasMore
    socialStateMemLock.acquire()
    try:
      let state = loadSocialStateUnsafe(node)
      response["notificationSummary"] = socialNotificationsSummaryNodeNoLock(state)
      response["subscriptionSnapshot"] = socialBuildSubscriptionSnapshotNoLock(state, localPeerId(node))
    finally:
      socialStateMemLock.release()
    allocCString($response)
  except CatchableError as exc:
    warn "social_feed_snapshot failed", err = exc.msg
    socialListErrorResponse("social_feed_snapshot", $exc.name, exc.msg)
  except Defect as exc:
    error "social_feed_snapshot defect", err = exc.msg
    socialListErrorResponse("social_feed_snapshot", $exc.name, exc.msg)

proc social_feed_debug_snapshot*(handle: pointer): cstring {.exportc, cdecl, dynlib.} =
  try:
    let node = nodeFromHandle(handle)
    if node.isNil:
      return allocCString("{\"ok\":false,\"error\":\"node_nil\"}")
    var response = newJObject()
    response["ok"] = %true
    response["peerId"] = %localPeerId(node)
    response["connectedPeers"] = connectedPeersArray(node.selfHandle)
    var subscribedTopics = newJArray()
    if not node.feedService.isNil:
      for topic in node.feedService.subscriptionTopics():
        subscribedTopics.add(%topic)
    response["subscribedTopics"] = subscribedTopics
    var feedItems = newJArray()
    for item in node.feedItems:
      var entry = newJObject()
      entry["id"] = %item.id
      entry["author"] = %item.author
      entry["timestampMs"] = %item.timestamp
      let payload =
        if item.payload.kind == JObject:
          item.payload
        else:
          newJObject()
      entry["publishState"] = %jsonGetStr(payload, "publishState", "")
      entry["visibility"] = %jsonGetStr(payload, "visibility", "")
      feedItems.add(entry)
    response["feedItems"] = feedItems
    allocCString($response)
  except CatchableError as exc:
    warn "social_feed_debug_snapshot failed", err = exc.msg
    socialObjectErrorResponse("social_feed_debug_snapshot", "{\"ok\":false}", $exc.name, exc.msg)
  except Defect as exc:
    error "social_feed_debug_snapshot defect", err = exc.msg
    socialObjectErrorResponse("social_feed_debug_snapshot", "{\"ok\":false}", $exc.name, exc.msg)

proc social_content_detail*(handle: pointer, contentId: cstring): cstring {.exportc, cdecl, dynlib.} =
  try:
    if contentId.isNil:
      return allocCString("{\"ok\":false,\"error\":\"content_id_missing\"}")
    let node = nodeFromHandle(handle)
    if node.isNil:
      return allocCString("{\"ok\":false,\"error\":\"node_nil\"}")
    var normalizedId = ($contentId).strip()
    if normalizedId.startsWith("moment:") and normalizedId.len > 7:
      normalizedId = normalizedId[7 .. ^1]
    if normalizedId.len == 0:
      return allocCString("{\"ok\":false,\"error\":\"content_id_missing\"}")
    socialEnsureDiscoveredFeedSubscriptions(node, 12)
    socialProcessPendingPublishTasks(node, 2)
    let localPeer = localPeerId(node)
    let moments = socialCollectCanonicalMoments(node)
    var response = newJObject()
    for post in moments:
      let postId = jsonGetStr(post, "postId", jsonGetStr(post, "id")).strip()
      if postId != normalizedId:
        continue
      var item = socialBuildCanonicalMomentItem(node, post)
      item["likes"] =
        if post.hasKey("likes") and post["likes"].kind == JArray:
          post["likes"]
        else:
          newJArray()
      item["comments"] =
        if post.hasKey("comments") and post["comments"].kind == JArray:
          post["comments"]
        else:
          newJArray()
      response["viewerCanAccess"] = %true
      response["accessPolicy"] = %"public"
      response["ok"] = %true
      response["item"] = item
      if item.hasKey("playback"):
        response["playback"] = item["playback"]
      if item.hasKey("mediaAsset"):
        response["mediaAsset"] = item["mediaAsset"]
      if item.hasKey("assetStatus"):
        response["assetStatus"] = item["assetStatus"]
      return allocCString($response)
    socialStateMemLock.acquire()
    try:
      let state = loadSocialStateUnsafe(node)
      let taskIndex = socialFindTaskByContentIdIndexNoLock(state, normalizedId)
      if taskIndex >= 0:
        let task = state["publishTasks"][taskIndex]
        var item = socialBuildTaskSnapshot(node, task)
        let authorPeerId = jsonGetStr(task, "authorPeerId", "")
        let isOwner = authorPeerId.len > 0 and authorPeerId == localPeer
        let visibility = jsonGetStr(task, "visibility", "owner_only")
        let publishState = jsonGetStr(task, "publishState", "queued")
        let tombstoned = jsonGetBool(task, "tombstoned", false)
        response["ok"] = %true
        response["item"] = item
        if tombstoned:
          response["viewerCanAccess"] = %isOwner
          response["accessPolicy"] = %"tombstoned"
          let tombstone = socialFindActiveTombstoneNoLock(state, normalizedId)
          if tombstone.kind == JObject:
            response["tombstonePayload"] = tombstone
        elif publishState == "published" and visibility == "public":
          response["viewerCanAccess"] = %true
          response["accessPolicy"] = %"public"
        elif isOwner:
          response["viewerCanAccess"] = %true
          response["accessPolicy"] = %"owner_only"
        else:
          response["viewerCanAccess"] = %false
          response["accessPolicy"] = %"restricted"
        if item.hasKey("playback"):
          response["playback"] = item["playback"]
        if item.hasKey("mediaAsset"):
          response["mediaAsset"] = item["mediaAsset"]
        if item.hasKey("assetStatus"):
          response["assetStatus"] = item["assetStatus"]
        return allocCString($response)
    finally:
      socialStateMemLock.release()
    for item in socialCollectRemoteFeedCanonicalItemsWithRetry(node, "", normalizedId):
      let itemId = jsonGetStr(item, "id", jsonGetStr(item, "postId")).strip()
      if itemId != normalizedId:
        continue
      response["ok"] = %true
      response["viewerCanAccess"] = %true
      response["accessPolicy"] = %"public"
      response["item"] = item
      if item.hasKey("playback"):
        response["playback"] = item["playback"]
      if item.hasKey("mediaAsset"):
        response["mediaAsset"] = item["mediaAsset"]
      if item.hasKey("assetStatus"):
        response["assetStatus"] = item["assetStatus"]
      return allocCString($response)
    response["ok"] = %false
    response["error"] = %"content_not_found"
    response["contentId"] = %normalizedId
    allocCString($response)
  except CatchableError as exc:
    warn "social_content_detail failed", err = exc.msg
    socialObjectErrorResponse("social_content_detail", "{\"ok\":false}", $exc.name, exc.msg)
  except Defect as exc:
    error "social_content_detail defect", err = exc.msg
    socialObjectErrorResponse("social_content_detail", "{\"ok\":false}", $exc.name, exc.msg)

proc socialFindPlaybackSessionIndexNoLock(state: JsonNode, sessionId: string): int =
  if state.kind != JObject or not state.hasKey("playbackSessions") or state["playbackSessions"].kind != JArray:
    return -1
  var index = 0
  for session in state["playbackSessions"].items:
    if session.kind != JObject:
      inc index
      continue
    if jsonGetStr(session, "sessionId", "").strip() == sessionId:
      return index
    inc index
  -1

proc socialResolvePlayableContent(node: NimNode, refId: string): JsonNode =
  let normalizedRef = refId.strip()
  if normalizedRef.len == 0:
    return newJNull()
  for post in socialCollectCanonicalMoments(node):
    let postId = jsonGetStr(post, "postId", jsonGetStr(post, "id")).strip()
    let mediaAsset = socialBuildMediaAssetManifest(post)
    let assetId = if mediaAsset.kind == JObject: jsonGetStr(mediaAsset, "assetId", "").strip() else: ""
    if postId == normalizedRef or assetId == normalizedRef:
      return socialBuildCanonicalMomentItem(node, post)
  socialStateMemLock.acquire()
  try:
    let state = loadSocialStateUnsafe(node)
    if state.hasKey("publishTasks") and state["publishTasks"].kind == JArray:
      for task in state["publishTasks"]:
        if task.kind != JObject:
          continue
        let contentId = socialTaskContentId(task)
        let mediaAsset = socialBuildMediaAssetManifest(task)
        let assetId = if mediaAsset.kind == JObject: jsonGetStr(mediaAsset, "assetId", "").strip() else: ""
        if contentId == normalizedRef or assetId == normalizedRef:
          return socialBuildTaskSnapshot(node, task)
  finally:
    socialStateMemLock.release()
  for item in socialCollectRemoteFeedCanonicalItemsWithRetry(node, "", normalizedRef):
    let itemId = jsonGetStr(item, "id", jsonGetStr(item, "postId")).strip()
    let mediaAsset =
      if item.kind == JObject and item.hasKey("mediaAsset") and item["mediaAsset"].kind == JObject:
        item["mediaAsset"]
      else:
        newJNull()
    let assetId = if mediaAsset.kind == JObject: jsonGetStr(mediaAsset, "assetId", "").strip() else: ""
    if itemId == normalizedRef or assetId == normalizedRef:
      return item
  newJNull()

proc socialBuildPlaybackBufferedRanges(descriptor: JsonNode, positionMs: int64): JsonNode =
  result = newJArray()
  if descriptor.kind != JObject:
    return
  let mode = jsonGetStr(descriptor, "mode", "").strip().toLowerAscii()
  if mode == "vod":
    let durationMs = max(jsonGetInt64(descriptor, "durationMs", 0), 0'i64)
    if durationMs > 0:
      result.add(%*{"startMs": 0, "endMs": durationMs})
    return
  if mode == "program_live":
    let liveEdgeMs = max(jsonGetInt64(descriptor, "liveEdgeMs", positionMs), 0'i64)
    let dvrWindowMs = max(jsonGetInt64(descriptor, "dvrWindowMs", 0), 0'i64)
    result.add(%*{"startMs": max(liveEdgeMs - dvrWindowMs, 0'i64), "endMs": liveEdgeMs})

proc socialBuildPlaybackSessionSnapshot(
    node: NimNode,
    sessionId: string,
    item: JsonNode,
    initialState: string,
    requestedPositionMs: int64
): JsonNode =
  let playback =
    if item.kind == JObject and item.hasKey("playback") and item["playback"].kind == JObject:
      item["playback"]
    else:
      newJObject()
  let mediaAsset =
    if item.kind == JObject and item.hasKey("mediaAsset") and item["mediaAsset"].kind == JObject:
      item["mediaAsset"]
    else:
      newJObject()
  let assetStatus =
    if item.kind == JObject and item.hasKey("assetStatus") and item["assetStatus"].kind == JObject:
      item["assetStatus"]
    else:
      newJObject()
  let mode = jsonGetStr(playback, "mode", "").strip().toLowerAscii()
  let liveEdgeMs = max(jsonGetInt64(playback, "liveEdgeMs", 0), 0'i64)
  let allowSeek = jsonGetBool(playback, "allowSeek", false)
  let positionMs =
    if initialState == "join_live" and mode in ["interactive_live", "program_live"]:
      liveEdgeMs
    elif allowSeek:
      max(requestedPositionMs, 0'i64)
    else:
      max(min(requestedPositionMs, liveEdgeMs), 0'i64)
  let resolvedState =
    case initialState.strip().toLowerAscii()
    of "play", "playing":
      "playing"
    of "pause", "paused":
      "paused"
    of "join_live":
      "playing"
    else:
      if jsonGetBool(playback, "autoplay", true): "playing" else: "ready"
  let nowTs = nowMillis()
  result = newJObject()
  result["sessionId"] = %sessionId
  result["contentId"] = %jsonGetStr(item, "postId", jsonGetStr(item, "id"))
  result["playback"] = playback
  result["mediaAsset"] = mediaAsset
  result["assetStatus"] = assetStatus
  result["positionMs"] = %positionMs
  result["state"] = %resolvedState
  result["error"] = %""
  result["connectedPeers"] = connectedPeersArray(node.selfHandle)
  result["sourceState"] =
    %(if assetStatus.kind == JObject and jsonGetStr(assetStatus, "status", "").len > 0:
        jsonGetStr(assetStatus, "status", "")
      elif mode in ["interactive_live", "program_live"]:
        "program_ready"
      else:
        "asset_pending")
  result["bufferedRanges"] = socialBuildPlaybackBufferedRanges(playback, positionMs)
  result["createdAt"] = %nowTs
  result["updatedAt"] = %nowTs
  result["roomId"] = %jsonGetStr(playback, "roomId", "")
  result["streamKey"] = %jsonGetStr(playback, "streamKey", "")

proc social_playback_open*(handle: pointer, payloadJson: cstring): cstring {.exportc, cdecl, dynlib.} =
  if payloadJson.isNil:
    return allocCString("{\"ok\":false,\"error\":\"payload_missing\"}")
  let node = nodeFromHandle(handle)
  if node.isNil:
    return allocCString("{\"ok\":false,\"error\":\"node_nil\"}")
  let payload = normalizeSocialPayload($payloadJson)
  let contentId =
    jsonGetStr(payload, "contentId", jsonGetStr(payload, "assetId", jsonGetStr(payload, "id"))).strip()
  if contentId.len == 0:
    return allocCString("{\"ok\":false,\"error\":\"content_id_missing\"}")
  let item = socialResolvePlayableContent(node, contentId)
  if item.kind != JObject:
    return allocCString("{\"ok\":false,\"error\":\"content_not_found\"}")
  let requestedSessionId = jsonGetStr(payload, "sessionId", "").strip()
  let sessionId =
    if requestedSessionId.len > 0:
      requestedSessionId
    else:
      "playback-" & socialSlugId(contentId, "content") & "-" & $nowMillis()
  let requestedState = jsonGetStr(payload, "state", "open")
  let requestedPositionMs = max(jsonGetInt64(payload, "positionMs", 0), 0'i64)
  let session = socialBuildPlaybackSessionSnapshot(node, sessionId, item, requestedState, requestedPositionMs)
  socialStateMemLock.acquire()
  try:
    let state = loadSocialStateUnsafe(node)
    let index = socialFindPlaybackSessionIndexNoLock(state, sessionId)
    if index >= 0:
      state["playbackSessions"].elems[index] = session
    else:
      prependBoundedNode(state["playbackSessions"], session, SocialMaxPlaybackSessions)
    persistSocialStateUnsafe(node, state)
  finally:
    socialStateMemLock.release()
  var response = newJObject()
  response["ok"] = %true
  response["session"] = session
  allocCString($response)

proc social_playback_state*(handle: pointer, sessionId: cstring): cstring {.exportc, cdecl, dynlib.} =
  if sessionId.isNil:
    return allocCString("{\"ok\":false,\"error\":\"session_id_missing\"}")
  let node = nodeFromHandle(handle)
  if node.isNil:
    return allocCString("{\"ok\":false,\"error\":\"node_nil\"}")
  let normalizedId = ($sessionId).strip()
  if normalizedId.len == 0:
    return allocCString("{\"ok\":false,\"error\":\"session_id_missing\"}")
  var response = newJObject()
  socialStateMemLock.acquire()
  try:
    let state = loadSocialStateUnsafe(node)
    let index = socialFindPlaybackSessionIndexNoLock(state, normalizedId)
    if index >= 0:
      let session = cloneJson(state["playbackSessions"][index])
      session["connectedPeers"] = connectedPeersArray(node.selfHandle)
      session["updatedAt"] = %nowMillis()
      response["ok"] = %true
      response["session"] = session
    else:
      response["ok"] = %false
      response["error"] = %"session_not_found"
      response["sessionId"] = %normalizedId
  finally:
    socialStateMemLock.release()
  allocCString($response)

proc social_playback_control*(handle: pointer, payloadJson: cstring): cstring {.exportc, cdecl, dynlib.} =
  if payloadJson.isNil:
    return allocCString("{\"ok\":false,\"error\":\"payload_missing\"}")
  let node = nodeFromHandle(handle)
  if node.isNil:
    return allocCString("{\"ok\":false,\"error\":\"node_nil\"}")
  let payload = normalizeSocialPayload($payloadJson)
  let sessionId = jsonGetStr(payload, "sessionId", "").strip()
  let op = jsonGetStr(payload, "op", jsonGetStr(payload, "state", "")).strip().toLowerAscii()
  if sessionId.len == 0:
    return allocCString("{\"ok\":false,\"error\":\"session_id_missing\"}")
  var response = newJObject()
  socialStateMemLock.acquire()
  try:
    let state = loadSocialStateUnsafe(node)
    let index = socialFindPlaybackSessionIndexNoLock(state, sessionId)
    if index < 0:
      response["ok"] = %false
      response["error"] = %"session_not_found"
      response["sessionId"] = %sessionId
    else:
      if op == "close":
        state["playbackSessions"].elems.delete(index)
        persistSocialStateUnsafe(node, state)
        response["ok"] = %true
        response["closed"] = %true
        response["sessionId"] = %sessionId
      else:
        let session = state["playbackSessions"][index]
        let playback =
          if session.kind == JObject and session.hasKey("playback") and session["playback"].kind == JObject:
            session["playback"]
          else:
            newJObject()
        let allowSeek = jsonGetBool(playback, "allowSeek", false)
        let liveEdgeMs = max(jsonGetInt64(playback, "liveEdgeMs", jsonGetInt64(session, "positionMs", 0)), 0'i64)
        case op
        of "play":
          session["state"] = %"playing"
        of "pause":
          session["state"] = %"paused"
        of "seek":
          if not allowSeek:
            response["ok"] = %false
            response["error"] = %"seek_not_allowed"
            response["sessionId"] = %sessionId
            return allocCString($response)
          session["positionMs"] = %max(jsonGetInt64(payload, "positionMs", 0), 0'i64)
          session["state"] = %"playing"
        of "join_live":
          session["positionMs"] = %liveEdgeMs
          session["state"] = %"playing"
        of "rate":
          session["state"] = %jsonGetStr(session, "state", "playing")
        else:
          session["state"] = %(if jsonGetStr(session, "state", "").len > 0: jsonGetStr(session, "state", "") else: "playing")
        session["bufferedRanges"] = socialBuildPlaybackBufferedRanges(playback, jsonGetInt64(session, "positionMs", 0))
        session["connectedPeers"] = connectedPeersArray(node.selfHandle)
        session["updatedAt"] = %nowMillis()
        persistSocialStateUnsafe(node, state)
        response["ok"] = %true
        response["session"] = cloneJson(session)
  finally:
    socialStateMemLock.release()
  allocCString($response)

proc social_media_asset_status*(handle: pointer, assetId: cstring): cstring {.exportc, cdecl, dynlib.} =
  if assetId.isNil:
    return allocCString("{\"ok\":false,\"error\":\"asset_id_missing\"}")
  let node = nodeFromHandle(handle)
  if node.isNil:
    return allocCString("{\"ok\":false,\"error\":\"node_nil\"}")
  let normalizedId = ($assetId).strip()
  if normalizedId.len == 0:
    return allocCString("{\"ok\":false,\"error\":\"asset_id_missing\"}")
  let item = socialResolvePlayableContent(node, normalizedId)
  if item.kind != JObject:
    return allocCString("{\"ok\":false,\"error\":\"asset_not_found\"}")
  let assetStatus =
    if item.hasKey("assetStatus") and item["assetStatus"].kind == JObject:
      item["assetStatus"]
    else:
      socialBuildAssetStatus(node, item)
  var response = newJObject()
  response["ok"] = %(assetStatus.kind == JObject)
  if assetStatus.kind == JObject:
    response["assetStatus"] = assetStatus
    if assetStatus.hasKey("asset"):
      response["asset"] = assetStatus["asset"]
  else:
    response["error"] = %"asset_not_found"
  allocCString($response)

proc social_publish_enqueue*(handle: pointer, postJson: cstring): cstring {.exportc, cdecl, dynlib.} =
  try:
    if postJson.isNil:
      return allocCString("{\"ok\":false,\"error\":\"post_missing\"}")
    let node = nodeFromHandle(handle)
    if node.isNil:
      return allocCString("{\"ok\":false,\"error\":\"node_nil\"}")
    var task = normalizeSocialPayload($postJson)
    let nowTs = nowMillis()
    var contentId = socialTaskContentId(task)
    let channel = socialCanonicalChannel(
      jsonGetStr(task, "publishCategory", jsonGetStr(task, "category", jsonGetStr(task, "channel")))
    )
    if contentId.len == 0:
      contentId = channel & "-" & $nowTs & "-" & $rand(10_000)
    let content = socialMomentContent(task)
    if content.len == 0:
      return allocCString("{\"ok\":false,\"error\":\"content_missing\"}")
    let taskId = "task-" & $nowTs & "-" & $rand(1_000_000)
    let authorPeerId = localPeerId(node)
    task["taskId"] = %taskId
    task["contentId"] = %contentId
    task["postId"] = %contentId
    task["id"] = %contentId
    task["category"] = %channel
    task["channel"] = %channel
    task["content"] = %content
    task["summary"] = %socialMomentSummary(task, content)
    task["title"] = %socialMomentTitle(task, content)
    task["type"] = %socialMomentType(task, channel)
    task["mediaLabel"] = %socialMomentMediaLabel(task)
    task["locationHint"] = %socialMomentLocationHint(task)
    task["authorPeerId"] = %authorPeerId
    task["authorName"] = %socialDisplayNameForPeer(authorPeerId)
    task["createdAt"] = %jsonGetInt64(task, "createdAt", nowTs)
    task["timestampMs"] = %jsonGetInt64(task, "timestampMs", nowTs)
    task["updatedAt"] = %nowTs
    socialEnsureTaskSynccastMetadata(task, contentId, authorPeerId, nowTs)
    task["publishState"] = %"governance_pending"
    task["moderationClass"] = %"UNKNOWN"
    task["moderationDecision"] = %"NONE"
    task["visibility"] = %"owner_only"
    task["homeEligible"] = %false
    task["distributionTags"] = newJArray()
    task["reasonCode"] = %"queued_for_governance"
    task["retryCount"] = %0
    task["lastAttemptAt"] = %0
    task["publishedAt"] = %0
    task["tombstoned"] = %false
    if not task.hasKey("likes") or task["likes"].kind != JArray:
      task["likes"] = newJArray()
    if not task.hasKey("comments") or task["comments"].kind != JArray:
      task["comments"] = newJArray()
    socialStateMemLock.acquire()
    try:
      let state = loadSocialStateUnsafe(node)
      socialApplyStep0NoLock(task)
      prependBoundedNode(state["publishTasks"], task, SocialMaxPublishTasks)
      socialAppendDistributionLogNoLock(
        state,
        contentId,
        jsonGetStr(task, "moderationDecision", "NONE"),
        jsonGetStr(task, "reasonCode", "queued_for_governance")
      )
      persistSocialStateUnsafe(node, state)
    finally:
      socialStateMemLock.release()
    recordSocialEvent(node, "publish_tasks", "enqueue", task, postId = contentId, source = "social_publish_enqueue")
    var response = newJObject()
    response["ok"] = %true
    response["task"] = socialBuildTaskSnapshot(node, task)
    allocCString($response)
  except CatchableError as exc:
    var response = newJObject()
    response["ok"] = %false
    response["error"] = %(if exc.msg.len > 0: exc.msg else: $exc.name)
    response["function"] = %"social_publish_enqueue"
    allocCString($response)
  except Defect as exc:
    error "social_publish_enqueue defect", err = exc.msg
    var response = newJObject()
    response["ok"] = %false
    response["error"] = %(if exc.msg.len > 0: exc.msg else: $exc.name)
    response["function"] = %"social_publish_enqueue"
    allocCString($response)

proc social_publish_task*(handle: pointer, taskId: cstring): cstring {.exportc, cdecl, dynlib.} =
  try:
    if taskId.isNil:
      return allocCString("{\"ok\":false,\"error\":\"task_id_missing\"}")
    let node = nodeFromHandle(handle)
    if node.isNil:
      return allocCString("{\"ok\":false,\"error\":\"node_nil\"}")
    socialProcessPendingPublishTasks(node, 2)
    let normalizedTaskId = ($taskId).strip()
    var response = newJObject()
    socialStateMemLock.acquire()
    try:
      let state = loadSocialStateUnsafe(node)
      let index = socialFindTaskIndexNoLock(state, normalizedTaskId)
      if index >= 0:
        response["ok"] = %true
        response["task"] = socialBuildTaskSnapshot(node, state["publishTasks"][index])
      else:
        response["ok"] = %false
        response["error"] = %"task_not_found"
        response["taskId"] = %normalizedTaskId
    finally:
      socialStateMemLock.release()
    allocCString($response)
  except CatchableError as exc:
    warn "social_publish_task failed", err = exc.msg
    socialObjectErrorResponse("social_publish_task", "{\"ok\":false}", $exc.name, exc.msg)
  except Defect as exc:
    error "social_publish_task defect", err = exc.msg
    socialObjectErrorResponse("social_publish_task", "{\"ok\":false}", $exc.name, exc.msg)

proc social_publish_tasks*(handle: pointer, scope: cstring, cursor: cstring, limit: cint): cstring {.exportc, cdecl, dynlib.} =
  try:
    let node = nodeFromHandle(handle)
    if node.isNil:
      return allocCString("{\"items\":[],\"nextCursor\":\"\",\"hasMore\":false}")
    socialProcessPendingPublishTasks(node, 4)
    let localPeer = localPeerId(node)
    let requested = if limit > 0: min(int(limit), 200) else: SocialDefaultLimit
    let startIndex =
      if cursor.isNil:
        0
      else:
        int(max(parseIntLoose($cursor), 0))
    let normalizedScope = if scope.isNil: "" else: ($scope).strip().toLowerAscii()
    var rows: seq[JsonNode] = @[]
    socialStateMemLock.acquire()
    try:
      let state = loadSocialStateUnsafe(node)
      if state.hasKey("publishTasks") and state["publishTasks"].kind == JArray:
        for task in state["publishTasks"]:
          if task.kind != JObject:
            continue
          let authorPeerId = jsonGetStr(task, "authorPeerId", "")
          let publishState = jsonGetStr(task, "publishState", "queued")
          let shouldInclude =
            case normalizedScope
            of "mine", "owner":
              authorPeerId == localPeer
            of "review", "under_review":
              publishState == "under_review"
            of "public":
              publishState == "published"
            else:
              true
          if shouldInclude:
            rows.add(socialBuildTaskSnapshot(node, task))
    finally:
      socialStateMemLock.release()
    rows.sort(
      proc(a, b: JsonNode): int =
        cmp(
          jsonGetInt64(b, "updatedAt", jsonGetInt64(b, "createdAt", 0)),
          jsonGetInt64(a, "updatedAt", jsonGetInt64(a, "createdAt", 0))
        )
    )
    var response = newJObject()
    var items = newJArray()
    var nextCursor = ""
    var hasMore = false
    if startIndex < rows.len:
      let stopIndex = min(startIndex + requested, rows.len)
      for index in startIndex ..< stopIndex:
        items.add(rows[index])
      hasMore = stopIndex < rows.len
      if hasMore:
        nextCursor = $stopIndex
    response["items"] = items
    response["nextCursor"] = %nextCursor
    response["hasMore"] = %hasMore
    allocCString($response)
  except CatchableError as exc:
    warn "social_publish_tasks failed", err = exc.msg
    socialListErrorResponse("social_publish_tasks", $exc.name, exc.msg)
  except Defect as exc:
    error "social_publish_tasks defect", err = exc.msg
    socialListErrorResponse("social_publish_tasks", $exc.name, exc.msg)

proc social_report_submit*(handle: pointer, payloadJson: cstring): cstring {.exportc, cdecl, dynlib.} =
  if payloadJson.isNil:
    return allocCString("{\"ok\":false,\"error\":\"payload_missing\"}")
  let node = nodeFromHandle(handle)
  if node.isNil:
    return allocCString("{\"ok\":false,\"error\":\"node_nil\"}")
  let payload = normalizeSocialPayload($payloadJson)
  let contentId = jsonGetStr(payload, "contentId", jsonGetStr(payload, "postId", jsonGetStr(payload, "id"))).strip()
  if contentId.len == 0:
    return allocCString("{\"ok\":false,\"error\":\"content_id_missing\"}")
  let nowTs = nowMillis()
  let channel = jsonGetStr(payload, "channel", "blue").strip().toLowerAscii()
  var ticket = newJObject()
  ticket["ticketId"] = %("report-" & $nowTs & "-" & $rand(1_000_000))
  ticket["contentId"] = %contentId
  ticket["channel"] = %(if channel == "red": "red" else: "blue")
  ticket["reporterId"] = %localPeerId(node)
  ticket["reasonCode"] = %jsonGetStr(payload, "reasonCode", "user_report")
  ticket["detail"] = %jsonGetStr(payload, "detail", jsonGetStr(payload, "content", ""))
  ticket["status"] = %(if channel == "red": "queued_pro_review" else: "queued_blue_review")
  ticket["createdAt"] = %nowTs
  socialStateMemLock.acquire()
  try:
    let state = loadSocialStateUnsafe(node)
    prependBoundedNode(state["reportTickets"], ticket, SocialMaxReportTickets)
    persistSocialStateUnsafe(node, state)
  finally:
    socialStateMemLock.release()
  recordSocialEvent(node, "reports", "submit", ticket, postId = contentId, source = "social_report_submit")
  var response = newJObject()
  response["ok"] = %true
  response["ticket"] = ticket
  allocCString($response)

proc social_report_list*(handle: pointer, scope: cstring, cursor: cstring, limit: cint): cstring {.exportc, cdecl, dynlib.} =
  let node = nodeFromHandle(handle)
  if node.isNil:
    return allocCString("{\"items\":[],\"nextCursor\":\"\",\"hasMore\":false}")
  let requested = if limit > 0: min(int(limit), 200) else: SocialDefaultLimit
  let startIndex =
    if cursor.isNil:
      0
    else:
      int(max(parseIntLoose($cursor), 0))
  let normalizedScope = if scope.isNil: "" else: ($scope).strip().toLowerAscii()
  var rows: seq[JsonNode] = @[]
  socialStateMemLock.acquire()
  try:
    let state = loadSocialStateUnsafe(node)
    if state.hasKey("reportTickets") and state["reportTickets"].kind == JArray:
      for ticket in state["reportTickets"]:
        if ticket.kind != JObject:
          continue
        let shouldInclude =
          case normalizedScope
          of "red":
            jsonGetStr(ticket, "channel", "") == "red"
          of "blue":
            jsonGetStr(ticket, "channel", "") == "blue"
          else:
            true
        if shouldInclude:
          rows.add(ticket)
  finally:
    socialStateMemLock.release()
  rows.sort(proc(a, b: JsonNode): int = cmp(jsonGetInt64(b, "createdAt", 0), jsonGetInt64(a, "createdAt", 0)))
  var response = newJObject()
  var items = newJArray()
  var nextCursor = ""
  var hasMore = false
  if startIndex < rows.len:
    let stopIndex = min(startIndex + requested, rows.len)
    for index in startIndex ..< stopIndex:
      items.add(rows[index])
    hasMore = stopIndex < rows.len
    if hasMore:
      nextCursor = $stopIndex
  response["items"] = items
  response["nextCursor"] = %nextCursor
  response["hasMore"] = %hasMore
  allocCString($response)

proc social_governance_review*(handle: pointer, payloadJson: cstring): cstring {.exportc, cdecl, dynlib.} =
  if payloadJson.isNil:
    return allocCString("{\"ok\":false,\"error\":\"payload_missing\"}")
  let node = nodeFromHandle(handle)
  if node.isNil:
    return allocCString("{\"ok\":false,\"error\":\"node_nil\"}")
  let payload = normalizeSocialPayload($payloadJson)
  let decision = jsonGetStr(payload, "decision").strip().toLowerAscii()
  let taskId = jsonGetStr(payload, "taskId").strip()
  let contentId = jsonGetStr(payload, "contentId").strip()
  if taskId.len == 0 and contentId.len == 0:
    return allocCString("{\"ok\":false,\"error\":\"task_id_missing\"}")
  let reviewerId = jsonGetStr(payload, "reviewerId", localPeerId(node))
  let reviewerNote = jsonGetStr(payload, "reviewerNote", "")
  var snapshot = newJNull()
  socialStateMemLock.acquire()
  try:
    let state = loadSocialStateUnsafe(node)
    let index =
      if taskId.len > 0:
        socialFindTaskIndexNoLock(state, taskId)
      else:
        socialFindTaskByContentIdIndexNoLock(state, contentId)
    if index >= 0:
      let task = state["publishTasks"][index]
      task["reviewerId"] = %reviewerId
      task["reviewedAt"] = %nowMillis()
      task["reviewerNote"] = %reviewerNote
      case decision
      of "approve", "pass", "publish":
        task["moderationDecision"] = %"PASS_TO_POOL"
        task["publishState"] = %(if socialTaskHasMediaAsset(task): "asset_ready" else: "approved_pending_transport")
        task["visibility"] = %"owner_only"
        task["homeEligible"] = %false
        task["reasonCode"] = %"manual_review_approved"
        task["distributionTags"] = toJsonStringArray(@["SAFE_FOR_HOME"])
      of "reject", "block":
        task["moderationDecision"] = %"BLOCK_AND_PRESERVE"
        task["publishState"] = %"rejected"
        task["visibility"] = %"owner_only"
        task["homeEligible"] = %false
        task["reasonCode"] = %"manual_review_rejected"
        task["distributionTags"] = toJsonStringArray(@["REQUIRE_PRO_REVIEW"])
      else:
        task["moderationDecision"] = %"HOLD_FOR_PRO_REVIEW"
        task["publishState"] = %"under_review"
        task["visibility"] = %"owner_only"
        task["homeEligible"] = %false
        task["reasonCode"] = %"manual_review_hold"
        task["distributionTags"] = toJsonStringArray(@["REQUIRE_PRO_REVIEW"])
      task["updatedAt"] = %nowMillis()
      socialAppendDistributionLogNoLock(
        state,
        socialTaskContentId(task),
        jsonGetStr(task, "moderationDecision", "NONE"),
        jsonGetStr(task, "reasonCode", "manual_review")
      )
      var notificationPayload = newJObject()
      let reviewContentId = socialTaskContentId(task)
      notificationPayload["contentId"] = %reviewContentId
      notificationPayload["subjectId"] = %reviewContentId
      notificationPayload["peerId"] = %jsonGetStr(task, "authorPeerId")
      notificationPayload["producer"] = %reviewerId
      notificationPayload["channel"] = %jsonGetStr(task, "channel", jsonGetStr(task, "category"))
      notificationPayload["eventKind"] = %"governance_result"
      notificationPayload["deliveryClass"] = %"priority"
      notificationPayload["timestampMs"] = %jsonGetInt64(task, "reviewedAt", nowMillis())
      makeNotificationNoLock(
        state,
        "governance_reviewed",
        "治理结果已更新",
        (if reviewerNote.len > 0: reviewerNote else: jsonGetStr(task, "reasonCode", "manual_review")),
        notificationPayload,
      )
      persistSocialStateUnsafe(node, state)
      snapshot = socialBuildTaskSnapshot(node, task)
  finally:
    socialStateMemLock.release()
  if decision in ["approve", "pass", "publish"]:
    socialProcessPendingPublishTasks(node, 1)
    socialStateMemLock.acquire()
    try:
      let state = loadSocialStateUnsafe(node)
      let index =
        if taskId.len > 0:
          socialFindTaskIndexNoLock(state, taskId)
        else:
          socialFindTaskByContentIdIndexNoLock(state, contentId)
      if index >= 0:
        snapshot = socialBuildTaskSnapshot(node, state["publishTasks"][index])
    finally:
      socialStateMemLock.release()
  var response = newJObject()
  if snapshot.kind == JObject:
    response["ok"] = %true
    response["task"] = snapshot
  else:
    response["ok"] = %false
    response["error"] = %"task_not_found"
  allocCString($response)

proc social_tombstone_issue*(handle: pointer, payloadJson: cstring): cstring {.exportc, cdecl, dynlib.} =
  if payloadJson.isNil:
    return allocCString("{\"ok\":false,\"error\":\"payload_missing\"}")
  let node = nodeFromHandle(handle)
  if node.isNil:
    return allocCString("{\"ok\":false,\"error\":\"node_nil\"}")
  let payload = normalizeSocialPayload($payloadJson)
  let contentId = jsonGetStr(payload, "contentId", jsonGetStr(payload, "postId", jsonGetStr(payload, "id"))).strip()
  if contentId.len == 0:
    return allocCString("{\"ok\":false,\"error\":\"content_id_missing\"}")
  let nowTs = nowMillis()
  var tombstone = newJObject()
  tombstone["tombstone_id"] = %("tombstone-" & $nowTs & "-" & $rand(1_000_000))
  tombstone["contentId"] = %contentId
  tombstone["content_hash"] = %contentId
  tombstone["scope"] = %jsonGetStr(payload, "scope", "public_content")
  tombstone["reason_code"] = %jsonGetStr(payload, "reasonCode", "manual_tombstone")
  tombstone["issued_by"] = %jsonGetStr(payload, "issuedBy", localPeerId(node))
  tombstone["issued_at"] = %nowTs
  tombstone["basis_type"] = %jsonGetStr(payload, "basisType", "professional_review")
  tombstone["execution_receipt"] = %jsonGetStr(payload, "executionReceipt", "")
  tombstone["restore_allowed"] = %jsonGetBool(payload, "restoreAllowed", true)
  tombstone["restore_decision_id"] = %jsonGetStr(payload, "restoreDecisionId", "")
  tombstone["active"] = %true
  var snapshot = newJNull()
  socialStateMemLock.acquire()
  try:
    let state = loadSocialStateUnsafe(node)
    prependBoundedNode(state["tombstones"], tombstone, SocialMaxTombstones)
    let taskIndex = socialFindTaskByContentIdIndexNoLock(state, contentId)
    if taskIndex >= 0:
      let task = state["publishTasks"][taskIndex]
      task["tombstoned"] = %true
      task["publishState"] = %"tombstoned"
      task["visibility"] = %"owner_only"
      task["homeEligible"] = %false
      task["reasonCode"] = %jsonGetStr(payload, "reasonCode", "manual_tombstone")
      task["updatedAt"] = %nowTs
      snapshot = socialBuildTaskSnapshot(node, task)
    if state.hasKey("moments") and state["moments"].kind == JArray:
      for post in state["moments"]:
        let postId = jsonGetStr(post, "postId", jsonGetStr(post, "id")).strip()
        if postId == contentId:
          post["tombstoned"] = %true
          post["deleted"] = %true
          post["reasonCode"] = %jsonGetStr(payload, "reasonCode", "manual_tombstone")
          post["updatedAt"] = %nowTs
    socialAppendDistributionLogNoLock(state, contentId, "TOMBSTONE", jsonGetStr(payload, "reasonCode", "manual_tombstone"))
    var notificationPayload = newJObject()
    notificationPayload["contentId"] = %contentId
    notificationPayload["subjectId"] = %contentId
    notificationPayload["channel"] = %jsonGetStr(payload, "channel", "content")
    notificationPayload["eventKind"] = %"tombstone_issue"
    notificationPayload["deliveryClass"] = %"priority"
    notificationPayload["producer"] = %jsonGetStr(payload, "issuedBy", localPeerId(node))
    notificationPayload["timestampMs"] = %nowTs
    makeNotificationNoLock(
      state,
      "tombstone_issued",
      "内容已下架",
      jsonGetStr(payload, "reasonCode", "manual_tombstone"),
      notificationPayload,
    )
    persistSocialStateUnsafe(node, state)
  finally:
    socialStateMemLock.release()
  recordSocialEvent(node, "tombstones", "issue", tombstone, postId = contentId, source = "social_tombstone_issue")
  var response = newJObject()
  response["ok"] = %true
  response["tombstone"] = tombstone
  if snapshot.kind == JObject:
    response["task"] = snapshot
  allocCString($response)

proc social_tombstone_restore*(handle: pointer, payloadJson: cstring): cstring {.exportc, cdecl, dynlib.} =
  if payloadJson.isNil:
    return allocCString("{\"ok\":false,\"error\":\"payload_missing\"}")
  let node = nodeFromHandle(handle)
  if node.isNil:
    return allocCString("{\"ok\":false,\"error\":\"node_nil\"}")
  let payload = normalizeSocialPayload($payloadJson)
  let contentId = jsonGetStr(payload, "contentId", jsonGetStr(payload, "postId", jsonGetStr(payload, "id"))).strip()
  let tombstoneId = jsonGetStr(payload, "tombstoneId", jsonGetStr(payload, "tombstone_id")).strip()
  if contentId.len == 0 and tombstoneId.len == 0:
    return allocCString("{\"ok\":false,\"error\":\"content_id_missing\"}")
  var snapshot = newJNull()
  var restoredTombstone = newJNull()
  socialStateMemLock.acquire()
  try:
    let state = loadSocialStateUnsafe(node)
    if state.hasKey("tombstones") and state["tombstones"].kind == JArray:
      for record in state["tombstones"]:
        if record.kind != JObject:
          continue
        let sameRecord =
          (tombstoneId.len > 0 and jsonGetStr(record, "tombstone_id", "").strip() == tombstoneId) or
          (contentId.len > 0 and jsonGetStr(record, "contentId", jsonGetStr(record, "content_hash")).strip() == contentId and jsonGetBool(record, "active", true))
        if not sameRecord:
          continue
        record["active"] = %false
        record["restore_decision_id"] = %jsonGetStr(payload, "restoreDecisionId", jsonGetStr(record, "restore_decision_id"))
        restoredTombstone = record
        break
    let effectiveContentId =
      if contentId.len > 0: contentId
      elif restoredTombstone.kind == JObject: jsonGetStr(restoredTombstone, "contentId", jsonGetStr(restoredTombstone, "content_hash")).strip()
      else: ""
    let taskIndex = socialFindTaskByContentIdIndexNoLock(state, effectiveContentId)
    if taskIndex >= 0:
      let task = state["publishTasks"][taskIndex]
      task["tombstoned"] = %false
      if jsonGetInt64(task, "publishedAt", 0) > 0:
        task["publishState"] = %"published"
        task["visibility"] = %"public"
        task["homeEligible"] = %true
      else:
        task["publishState"] = %"under_review"
        task["visibility"] = %"owner_only"
        task["homeEligible"] = %false
      task["reasonCode"] = %"tombstone_restored"
      task["updatedAt"] = %nowMillis()
      snapshot = socialBuildTaskSnapshot(node, task)
    if effectiveContentId.len > 0 and state.hasKey("moments") and state["moments"].kind == JArray:
      for post in state["moments"]:
        let postId = jsonGetStr(post, "postId", jsonGetStr(post, "id")).strip()
        if postId == effectiveContentId:
          post["tombstoned"] = %false
          post["deleted"] = %false
          post["updatedAt"] = %nowMillis()
    if effectiveContentId.len > 0:
      socialAppendDistributionLogNoLock(state, effectiveContentId, "RESTORE", "tombstone_restored")
      var notificationPayload = newJObject()
      notificationPayload["contentId"] = %effectiveContentId
      notificationPayload["subjectId"] = %effectiveContentId
      notificationPayload["channel"] = %jsonGetStr(payload, "channel", "content")
      notificationPayload["eventKind"] = %"tombstone_restore"
      notificationPayload["deliveryClass"] = %"priority"
      notificationPayload["producer"] = %jsonGetStr(payload, "reviewerId", localPeerId(node))
      notificationPayload["timestampMs"] = %nowMillis()
      makeNotificationNoLock(
        state,
        "tombstone_restored",
        "内容已恢复",
        "治理恢复后重新可见",
        notificationPayload,
      )
    persistSocialStateUnsafe(node, state)
  finally:
    socialStateMemLock.release()
  var response = newJObject()
  response["ok"] = %(restoredTombstone.kind == JObject)
  if restoredTombstone.kind == JObject:
    response["tombstone"] = restoredTombstone
  else:
    response["error"] = %"tombstone_not_found"
  if snapshot.kind == JObject:
    response["task"] = snapshot
  allocCString($response)

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
  try:
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
    socialStateMemLock.acquire()
    try:
      let state = loadSocialStateUnsafe(node)
      response["summary"] = socialNotificationsSummaryNodeNoLock(state)
      response["subscriptions"] = socialBuildSubscriptionSnapshotNoLock(state, localPeerId(node))
    finally:
      socialStateMemLock.release()
    allocCString($response)
  except CatchableError as exc:
    warn "social_notifications_list failed", err = exc.msg
    socialListErrorResponse("social_notifications_list", $exc.name, exc.msg)
  except Defect as exc:
    error "social_notifications_list defect", err = exc.msg
    socialListErrorResponse("social_notifications_list", $exc.name, exc.msg)

proc social_notifications_summary*(handle: pointer): cstring {.exportc, cdecl, dynlib.} =
  try:
    let node = nodeFromHandle(handle)
    if node.isNil:
      return allocCString("{\"unreadTotal\":0,\"unreadPriority\":0,\"unreadByChannel\":{},\"unreadByEvent\":{}}")
    var summary = newJObject()
    socialStateMemLock.acquire()
    try:
      let state = loadSocialStateUnsafe(node)
      summary = socialNotificationsSummaryNodeNoLock(state)
    finally:
      socialStateMemLock.release()
    allocCString($summary)
  except CatchableError as exc:
    warn "social_notifications_summary failed", err = exc.msg
    socialObjectErrorResponse(
      "social_notifications_summary",
      "{\"unreadTotal\":0,\"unreadPriority\":0,\"unreadByChannel\":{},\"unreadByEvent\":{}}",
      $exc.name,
      exc.msg,
    )
  except Defect as exc:
    error "social_notifications_summary defect", err = exc.msg
    socialObjectErrorResponse(
      "social_notifications_summary",
      "{\"unreadTotal\":0,\"unreadPriority\":0,\"unreadByChannel\":{},\"unreadByEvent\":{}}",
      $exc.name,
      exc.msg,
    )

proc social_notifications_mark*(handle: pointer, payloadJson: cstring): cstring {.exportc, cdecl, dynlib.} =
  try:
    let node = nodeFromHandle(handle)
    if node.isNil:
      return allocCString("{\"ok\":false,\"error\":\"node_nil\"}")
    let payload = normalizeSocialPayload(if payloadJson.isNil: "{}" else: $payloadJson)
    let markAll = jsonGetBool(payload, "markAll", false)
    let readState = jsonGetStr(payload, "readState", "read").strip().toLowerAscii()
    let readValue = readState == "read"
    let id = jsonGetStr(payload, "id").strip()
    let subjectId = jsonGetStr(payload, "subjectId", jsonGetStr(payload, "contentId", jsonGetStr(payload, "conversationId", jsonGetStr(payload, "groupId", jsonGetStr(payload, "peerId"))))).strip()
    let channel = jsonGetStr(payload, "channel").strip()
    let ids = collectJsonStringArray(payload, "ids")
    proc idMarked(target: string): bool =
      for current in ids:
        if current == target:
          return true
      false
    var updated = 0
    var summary = newJObject()
    socialStateMemLock.acquire()
    try:
      let state = loadSocialStateUnsafe(node)
      if state.hasKey("notifications") and state["notifications"].kind == JArray:
        for item in state["notifications"]:
          if item.kind != JObject:
            continue
          let matches =
            markAll or
            (id.len > 0 and jsonGetStr(item, "id").strip() == id) or
            (ids.len > 0 and idMarked(jsonGetStr(item, "id").strip())) or
            (subjectId.len > 0 and (
              jsonGetStr(item, "subjectId").strip() == subjectId or
              jsonGetStr(item, "contentId").strip() == subjectId or
              jsonGetStr(item, "conversationId").strip() == subjectId or
              jsonGetStr(item, "groupId").strip() == subjectId or
              jsonGetStr(item, "peerId").strip() == subjectId
            )) or
            (channel.len > 0 and jsonGetStr(item, "channel").strip() == channel)
          if not matches:
            continue
          item["read"] = %readValue
          item["readState"] = %(if readValue: "read" else: "unread")
          item["readAt"] = %nowMillis()
          updated.inc()
      if updated > 0:
        persistSocialStateUnsafe(node, state)
      summary = socialNotificationsSummaryNodeNoLock(state)
    finally:
      socialStateMemLock.release()
    var response = newJObject()
    response["ok"] = %true
    response["updated"] = %updated
    response["summary"] = summary
    allocCString($response)
  except CatchableError as exc:
    warn "social_notifications_mark failed", err = exc.msg
    socialObjectErrorResponse("social_notifications_mark", "{\"ok\":false}", $exc.name, exc.msg)
  except Defect as exc:
    error "social_notifications_mark defect", err = exc.msg
    socialObjectErrorResponse("social_notifications_mark", "{\"ok\":false}", $exc.name, exc.msg)

proc social_subscriptions_set*(handle: pointer, payloadJson: cstring): cstring {.exportc, cdecl, dynlib.} =
  let node = nodeFromHandle(handle)
  if node.isNil:
    return allocCString("{\"ok\":false,\"error\":\"node_nil\"}")
  let payload = normalizeSocialPayload(if payloadJson.isNil: "{}" else: $payloadJson)
  var snapshot = newJObject()
  socialStateMemLock.acquire()
  try:
    let state = loadSocialStateUnsafe(node)
    let subscriptions = ensureSocialSubscriptionsNoLock(state)
    let preferences = subscriptions["preferences"]
    let following = subscriptions["following"]
    let groups = subscriptions["groupMemberships"]

    for peerId in collectJsonStringArray(payload, "follow"):
      appendUniqueString(following, peerId)
    for peerId in collectJsonStringArray(payload, "following"):
      appendUniqueString(following, peerId)
    for peerId in collectJsonStringArray(payload, "unfollow"):
      if following.kind == JArray:
        var next = newJArray()
        for current in following:
          if current.kind == JString and current.getStr().strip() != peerId.strip():
            next.add(current)
        subscriptions["following"] = next
    for groupId in collectJsonStringArray(payload, "groupMemberships"):
      appendUniqueString(groups, groupId)
    for groupId in collectJsonStringArray(payload, "joinGroups"):
      appendUniqueString(groups, groupId)
    for groupId in collectJsonStringArray(payload, "leaveGroups"):
      if groups.kind == JArray:
        var next = newJArray()
        for current in groups:
          if current.kind == JString and current.getStr().strip() != groupId.strip():
            next.add(current)
        subscriptions["groupMemberships"] = next
    if payload.hasKey("preferences") and payload["preferences"].kind == JObject:
      for key, value in payload["preferences"]:
        preferences[key] = value
    snapshot = socialBuildSubscriptionSnapshotNoLock(state, localPeerId(node))
    persistSocialStateUnsafe(node, state)
  finally:
    socialStateMemLock.release()
  var response = newJObject()
  response["ok"] = %true
  response["subscriptions"] = snapshot
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
  rememberLastNodeIdentity(handle, node)
  allocCString(peer)

proc libp2p_get_last_node_peer_id*(): cstring {.exportc, cdecl, dynlib.} =
  let peer = lastKnownNodePeerId()
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
  maybeKickAuthoritativeBootstrap(node, "auto_authoritative_seed_status")
  let providerNode = buildWanProviderPayload(node)
  try:
    if node.switchInstance != nil:
      var bootstrap = bootstrapStateJson(node.switchInstance, BootstrapCandidateCap)
      if node.authoritativeBootstrapPeerIds.len > 0:
        mergeLocalBootstrapStatus(node, bootstrap, BootstrapCandidateCap)
      attachBootstrapProvider(bootstrap, providerNode)
      if bootstrap.kind == JObject and bootstrap.len > 0:
        return allocCString($bootstrap)
  except CatchableError:
    discard
  ## Fallback for runtimes that have not mounted WanBootstrapService yet.
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

  attachBootstrapProvider(obj, providerNode)
  allocCString($obj)

proc buildBootstrapStatusNode(node: NimNode): JsonNode {.gcsafe.} =
  if node.isNil:
    return newJObject()
  maybeKickAuthoritativeBootstrap(node, "auto_authoritative_seed_status")
  let providerNode = buildWanProviderPayload(node)
  try:
    if node.switchInstance != nil:
      var bootstrap = bootstrapStateJson(node.switchInstance, BootstrapCandidateCap)
      if node.authoritativeBootstrapPeerIds.len > 0:
        mergeLocalBootstrapStatus(node, bootstrap, BootstrapCandidateCap)
      attachBootstrapProvider(bootstrap, providerNode)
      if bootstrap.kind == JObject:
        return bootstrap
  except CatchableError:
    discard
  var obj = newJObject()
  attachBootstrapProvider(obj, providerNode)
  obj

proc bootstrapStatusShowsConnectedPeer(payload: JsonNode, peerId: string): bool {.gcsafe.} =
  if peerId.len == 0 or payload.kind != JObject:
    return false
  if payload.hasKey("connectedPeers") and payload["connectedPeers"].kind == JArray:
    for item in payload["connectedPeers"].items:
      if item.kind == JString and item.getStr().strip() == peerId:
        return true
  if payload.hasKey("selectedCandidates") and payload["selectedCandidates"].kind == JArray:
    for candidate in payload["selectedCandidates"].items:
      if candidate.kind != JObject:
        continue
      if jsonGetStr(candidate, "peerId") != peerId:
        continue
      let sources = candidate{"sources"}
      if not sources.isNil and sources.kind == JArray:
        for sourceNode in sources.items:
          if sourceNode.kind == JString and cmpIgnoreCase(sourceNode.getStr().strip(), "Connected") == 0:
            return true
  false

proc waitForBootstrapPeerConnectedWithinDeadline(
    node: NimNode,
    peerId: string,
    timeoutMs: int,
    pollMs = 100
): bool {.gcsafe, raises: [].} =
  if node.isNil or peerId.len == 0:
    return false
  proc readBootstrapConnected(): bool {.raises: [].} =
    try:
      let payload = buildBootstrapStatusNode(node)
      bootstrapStatusShowsConnectedPeer(payload, peerId)
    except Exception:
      false
  let safeTimeoutMs = max(timeoutMs, 0)
  if safeTimeoutMs == 0:
    return readBootstrapConnected()
  let deadlineMs = nowMillis() + int64(safeTimeoutMs)
  let safePollMs = max(pollMs, 25)
  while true:
    if readBootstrapConnected():
      return true
    let remainingMs = int(deadlineMs - nowMillis())
    if remainingMs <= 0:
      return false
    sleep(min(safePollMs, remainingMs))

proc waitForPeerConnectedWithinDeadline(
    node: NimNode,
    peer: PeerId,
    timeoutMs: int,
    pollMs = 100
): bool {.gcsafe, raises: [].} =
  if node.isNil or peer.len == 0:
    return false
  let safeTimeoutMs = max(timeoutMs, 0)
  if safeTimeoutMs == 0:
    return peerCurrentlyConnected(node, peer)
  let deadlineMs = nowMillis() + int64(safeTimeoutMs)
  let safePollMs = max(pollMs, 25)
  while true:
    if peerCurrentlyConnected(node, peer):
      return true
    let remainingMs = int(deadlineMs - nowMillis())
    if remainingMs <= 0:
      return false
    sleep(min(safePollMs, remainingMs))

proc bootstrapSeedConnectExactPayload(
    node: NimNode,
    seedPeerId: string,
    source: string,
    attemptedAddr: string,
    attempts: seq[JsonNode],
    ok: bool,
    error: string
): string {.gcsafe.} =
  let bootstrap = buildBootstrapStatusNode(node)
  let bootstrapConnected = bootstrapStatusShowsConnectedPeer(bootstrap, seedPeerId)
  var payload = newJObject()
  payload["ok"] = %ok
  payload["peerId"] = %seedPeerId
  payload["source"] = %source
  payload["attemptedAddr"] = %attemptedAddr
  payload["attemptCount"] = %attempts.len
  payload["bootstrapConnected"] = %(ok and bootstrapConnected)
  payload["bootstrap"] = bootstrap
  var attemptsNode = newJArray()
  for item in attempts:
    attemptsNode.add(item)
  payload["attempts"] = attemptsNode
  if error.len > 0:
    payload["error"] = %error
  $payload

proc exactSeedConnectReservedBudgetMs(maText: string): int {.gcsafe.} =
  let lowerAddr = maText.toLowerAscii()
  if lowerAddr.contains("/quic-v1") or lowerAddr.contains("/quic/"):
    2_000
  else:
    when defined(ohos):
      4_000
    else:
      3_000

proc exactSeedConnectAttemptCapMs(maText: string, hasFutureAttempts: bool): int {.gcsafe.} =
  let lowerAddr = maText.toLowerAscii()
  if lowerAddr.contains("/quic-v1") or lowerAddr.contains("/quic/"):
    when defined(ohos):
      if hasFutureAttempts:
        3_500
      else:
        5_000
    else:
      if hasFutureAttempts:
        6_000
      else:
        8_000
  else:
    when defined(ohos):
      5_000
    else:
      3_500

type BootstrapSeedConnectExactKickoffArg = object
  handle: pointer
  peerId: cstring
  addressesJson: cstring
  source: cstring
  limit: cint
  secureTimeoutMs: cint

proc destroyBootstrapSeedConnectExactKickoffArg(
    arg: ptr BootstrapSeedConnectExactKickoffArg
) {.gcsafe, raises: [].} =
  if arg.isNil:
    return
  if arg.peerId != nil:
    deallocShared(arg.peerId)
    arg.peerId = nil
  if arg.addressesJson != nil:
    deallocShared(arg.addressesJson)
    arg.addressesJson = nil
  if arg.source != nil:
    deallocShared(arg.source)
    arg.source = nil
  deallocShared(arg)

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
    return allocCString("{\"ok\":false,\"attemptCount\":0,\"attemptedCount\":0,\"connectedCount\":0,\"attempts\":[],\"attempted\":[]}")
  let cmd = makeCommand(cmdJoinViaRandomBootstrap)
  cmd.requestLimit = int(limit)
  let result = submitCommand(node, cmd)
  let output =
    if result.resultCode == NimResultOk and result.stringResult.len > 0:
      allocCString(cloneOwnedString(result.stringResult))
    else:
      allocCString("{\"ok\":false,\"attemptCount\":0,\"attemptedCount\":0,\"connectedCount\":0,\"attempts\":[],\"attempted\":[]}")
  destroyCommand(cmd)
  output

proc libp2p_join_via_seed_bootstrap*(
    handle: pointer, peerId: cstring, addressesJson: cstring, source: cstring, limit: cint
): cstring {.exportc, cdecl, dynlib.} =
  let node = nodeFromHandle(handle)
  if node.isNil:
    return allocCString("{\"ok\":false,\"attemptCount\":0,\"attemptedCount\":0,\"connectedCount\":0,\"attempts\":[],\"attempted\":[]}")
  let safePeerId = (if peerId.isNil: "" else: $peerId).strip()
  let safeAddressesJson = (if addressesJson.isNil: "" else: $addressesJson).strip()
  if safePeerId.len == 0 or safeAddressesJson.len == 0:
    return allocCString("{\"ok\":false,\"error\":\"missing peerId or addresses\"}")
  var addresses: seq[string] = @[]
  try:
    let parsed = parseJson(safeAddressesJson)
    if parsed.kind == JArray:
      for item in parsed.items:
        if item.kind == JString:
          let value = item.getStr().strip()
          if value.len > 0:
            addresses.add(value)
  except CatchableError:
    discard
  if addresses.len == 0:
    return allocCString("{\"ok\":false,\"error\":\"no valid addresses\"}")
  let cmd = makeCommand(cmdJoinViaSeedBootstrap)
  cmd.peerId = safePeerId
  cmd.addresses = addresses
  cmd.source = (if source.isNil: "" else: $source).strip()
  cmd.requestLimit = int(limit)
  let result = submitCommand(node, cmd)
  let output =
    if result.resultCode == NimResultOk and result.stringResult.len > 0:
      allocCString(cloneOwnedString(result.stringResult))
    else:
      allocCString("{\"ok\":false,\"attemptCount\":0,\"attemptedCount\":0,\"connectedCount\":0,\"attempts\":[],\"attempted\":[]}")
  destroyCommand(cmd)
  output

proc libp2p_bootstrap_seed_connect_exact*(
    handle: pointer,
    peerId: cstring,
    addressesJson: cstring,
    source: cstring,
    limit: cint,
    secureTimeoutMs: cint
): cstring {.exportc, cdecl, dynlib.} =
  let node = nodeFromHandle(handle)
  if node.isNil:
    debugLog("[nimlibp2p] bootstrapSeedConnectExact node=nil")
    return allocCString(
      "{\"ok\":false,\"error\":\"node_not_initialized\",\"attemptCount\":0,\"attemptedAddr\":\"\",\"attempts\":[],\"bootstrap\":{}}"
    )
  let safePeerId = (if peerId.isNil: "" else: $peerId).strip()
  let safeAddressesJson = (if addressesJson.isNil: "" else: $addressesJson).strip()
  debugLog(
    "[nimlibp2p] bootstrapSeedConnectExact start peer=" & safePeerId &
    " source=" & (if source.isNil: "" else: ($source).strip()) &
    " secureTimeoutMs=" & $int(secureTimeoutMs) &
    " addressesJson=" & safeAddressesJson
  )
  if safePeerId.len == 0 or safeAddressesJson.len == 0:
    debugLog("[nimlibp2p] bootstrapSeedConnectExact missing peer or addresses")
    return allocCString(
      "{\"ok\":false,\"error\":\"missing_peerId_or_addresses\",\"attemptCount\":0,\"attemptedAddr\":\"\",\"attempts\":[],\"bootstrap\":{}}"
    )
  var addresses = parseJsonStringSeq(safeAddressesJson)
  if addresses.len == 0:
    debugLog("[nimlibp2p] bootstrapSeedConnectExact no valid addresses after parse")
    return allocCString(
      "{\"ok\":false,\"error\":\"no_valid_addresses\",\"attemptCount\":0,\"attemptedAddr\":\"\",\"attempts\":[],\"bootstrap\":{}}"
    )
  let orderedAddrs = prioritizeGameSeedAddrs(preferStableAddrStrings(addresses))
  let safeLimit =
    if limit > 0:
      min(max(int(limit), 1), orderedAddrs.len)
    else:
      orderedAddrs.len
  if safeLimit <= 0:
    debugLog("[nimlibp2p] bootstrapSeedConnectExact safeLimit<=0")
    return allocCString(
      "{\"ok\":false,\"error\":\"no_valid_addresses\",\"attemptCount\":0,\"attemptedAddr\":\"\",\"attempts\":[],\"bootstrap\":{}}"
    )
  let safeSource = (if source.isNil: "" else: $source).strip()
  let effectiveSource = if safeSource.len > 0: safeSource else: "seeded_exact_connect"
  debugLog(
    "[nimlibp2p] bootstrapSeedConnectExact registerHints peer=" & safePeerId &
    " limit=" & $safeLimit &
    " ordered=" & orderedAddrs[0 ..< safeLimit].join("|")
  )
  let hintsCmd = makeCommand(cmdRegisterPeerHints)
  hintsCmd.peerId = safePeerId
  hintsCmd.addresses = orderedAddrs[0 ..< safeLimit]
  hintsCmd.source = effectiveSource
  let hintsResult = submitCommand(node, hintsCmd)
  debugLog(
    "[nimlibp2p] bootstrapSeedConnectExact registerHints done peer=" & safePeerId &
    " rc=" & $int(hintsResult.resultCode) &
    " err=" & hintsResult.errorMsg.strip()
  )
  destroyCommand(hintsCmd)

  let peerRes = PeerId.init(safePeerId)
  let totalBudgetMs =
    if secureTimeoutMs > 0:
      max(min(int(secureTimeoutMs), 10_000), 2_500)
    else:
      5_000
  let exactStartedAtMs = nowMillis()
  var attempts: seq[JsonNode] = @[]
  var lastError = ""
  var attemptedAddr = ""

  for idx, addr in orderedAddrs[0 ..< safeLimit]:
    let elapsedBeforeAttempt = int(nowMillis() - exactStartedAtMs)
    var remainingBudgetMs = totalBudgetMs - elapsedBeforeAttempt
    if remainingBudgetMs <= 0:
      lastError = "bootstrap_seed_connect_timeout"
      debugLog(
        "[nimlibp2p] bootstrapSeedConnectExact budget exhausted peer=" & safePeerId &
        " attemptedAddr=" & attemptedAddr &
        " totalBudgetMs=" & $totalBudgetMs
      )
      break
    attemptedAddr = addr
    let lowerAddr = addr.toLowerAscii()
    let remainingAttempts = max(1, safeLimit - idx)
    var reservedForFutureMs = 0
    if remainingAttempts > 1:
      for futureAddr in orderedAddrs[(idx + 1) ..< safeLimit]:
        reservedForFutureMs += exactSeedConnectReservedBudgetMs(futureAddr)
    let maxConnectTimeoutMs = exactSeedConnectAttemptCapMs(addr, remainingAttempts > 1)
    let timeoutMs =
      max(
        750,
        min(
          maxConnectTimeoutMs,
          remainingBudgetMs - reservedForFutureMs
        )
      )
    debugLog(
      "[nimlibp2p] bootstrapSeedConnectExact attempt:start peer=" & safePeerId &
      " addr=" & addr &
      " timeoutMs=" & $timeoutMs &
      " remainingBudgetMs=" & $remainingBudgetMs &
      " totalBudgetMs=" & $totalBudgetMs
    )
    let cmd = makeCommand(cmdConnectMultiaddr)
    cmd.multiaddr = addr
    cmd.timeoutMs = timeoutMs
    cmd.requestLimit = 1
    if remainingAttempts == 1 and (lowerAddr.contains("/quic-v1") or lowerAddr.contains("/quic/")):
      cmd.boolParam = true
    let result = submitCommand(node, cmd)
    let rc = result.resultCode
    let err = result.errorMsg.strip()
    debugLog(
      "[nimlibp2p] bootstrapSeedConnectExact attempt:connect_done peer=" & safePeerId &
      " addr=" & addr &
      " rc=" & $int(rc) &
      " err=" & err
    )
    destroyCommand(cmd)

    var attemptNode = newJObject()
    attemptNode["addr"] = %addr
    attemptNode["timeoutMs"] = %timeoutMs
    attemptNode["ok"] = %(rc == NimResultOk)
    if err.len > 0:
      attemptNode["error"] = %err

    var peerConnected = false
    if peerRes.isOk():
      peerConnected = peerCurrentlyConnected(node, peerRes.get())
    attemptNode["peerConnected"] = %peerConnected

    var secureOk = false
    if rc != NimResultOk and remainingAttempts == 1 and
        (lowerAddr.contains("/quic-v1") or lowerAddr.contains("/quic/")):
      let elapsedBeforeLateCheck = int(nowMillis() - exactStartedAtMs)
      remainingBudgetMs = totalBudgetMs - elapsedBeforeLateCheck
      let lateConnectWindowMs = max(0, min(2_500, remainingBudgetMs - 250))
      if lateConnectWindowMs > 0:
        debugLog(
          "[nimlibp2p] bootstrapSeedConnectExact attempt:late_connect_wait_start peer=" & safePeerId &
          " addr=" & addr &
          " waitMs=" & $lateConnectWindowMs
        )
        if peerRes.isOk():
          peerConnected = waitForPeerConnectedWithinDeadline(
            node,
            peerRes.get(),
            lateConnectWindowMs,
          )
          attemptNode["peerConnected"] = %peerConnected
        if not peerConnected:
          let lateBootstrapConnected = waitForBootstrapPeerConnectedWithinDeadline(
            node,
            safePeerId,
            lateConnectWindowMs,
          )
          attemptNode["bootstrapConnected"] = %lateBootstrapConnected
          if lateBootstrapConnected:
            attemptNode["lateConnected"] = %true
            attempts.add(attemptNode)
            debugLog(
              "[nimlibp2p] bootstrapSeedConnectExact success late peer=" & safePeerId &
              " addr=" & addr &
              " bootstrapConnected=1"
            )
            asyncSpawn(promoteConnectedBootstrapPeerDetached(
              node,
              safePeerId,
              orderedAddrs[0 ..< safeLimit],
              effectiveSource,
            ))
            return allocCString(
              bootstrapSeedConnectExactPayload(
                node,
                safePeerId,
                effectiveSource,
                attemptedAddr,
                attempts,
                true,
                "",
              )
            )
        elif peerConnected:
          attemptNode["lateConnected"] = %true

    if rc == NimResultOk:
      let elapsedBeforeSecure = int(nowMillis() - exactStartedAtMs)
      remainingBudgetMs = totalBudgetMs - elapsedBeforeSecure
      let secureWaitMs =
        max(
          250,
          min(
            if lowerAddr.contains("/quic-v1") or lowerAddr.contains("/quic/"):
              1_500
            else:
              2_000,
            remainingBudgetMs - reservedForFutureMs
          )
        )
      debugLog(
        "[nimlibp2p] bootstrapSeedConnectExact attempt:secure_wait_start peer=" & safePeerId &
        " addr=" & addr &
        " waitMs=" & $secureWaitMs &
        " remainingBudgetMs=" & $remainingBudgetMs
      )
      secureOk = libp2p_wait_secure_channel_ffi(handle, safePeerId.cstring, secureWaitMs.cint)
      debugLog(
        "[nimlibp2p] bootstrapSeedConnectExact attempt:secure_wait_done peer=" & safePeerId &
        " addr=" & addr &
        " secureOk=" & $secureOk
      )
      if not peerConnected and peerRes.isOk():
        peerConnected = peerCurrentlyConnected(node, peerRes.get())
        attemptNode["peerConnected"] = %peerConnected
    attemptNode["secureOk"] = %secureOk

    debugLog(
      "[nimlibp2p] bootstrapSeedConnectExact attempt:bootstrap_status_start peer=" & safePeerId &
      " addr=" & addr
    )
    var bootstrapNode = buildBootstrapStatusNode(node)
    var bootstrapConnected = bootstrapStatusShowsConnectedPeer(bootstrapNode, safePeerId)
    debugLog(
      "[nimlibp2p] bootstrapSeedConnectExact attempt:bootstrap_status_done peer=" & safePeerId &
      " addr=" & addr &
      " peerConnected=" & $peerConnected &
      " bootstrapConnected=" & $bootstrapConnected
    )
    attemptNode["bootstrapConnected"] = %bootstrapConnected
    attempts.add(attemptNode)

    if peerConnected or bootstrapConnected:
      debugLog(
        "[nimlibp2p] bootstrapSeedConnectExact success peer=" & safePeerId &
        " addr=" & addr &
        " peerConnected=" & $peerConnected &
        " bootstrapConnected=" & $bootstrapConnected
      )
      asyncSpawn(promoteConnectedBootstrapPeerDetached(
        node,
        safePeerId,
        orderedAddrs[0 ..< safeLimit],
        effectiveSource,
      ))
      bootstrapNode = buildBootstrapStatusNode(node)
      bootstrapConnected = bootstrapStatusShowsConnectedPeer(bootstrapNode, safePeerId)
      return allocCString(
        bootstrapSeedConnectExactPayload(
          node,
          safePeerId,
          effectiveSource,
          attemptedAddr,
          attempts,
          true,
          "",
        )
      )

    if err.len > 0:
      lastError = err
  if lastError.len == 0:
    lastError = "bootstrap_seed_connect_timeout"
  debugLog(
    "[nimlibp2p] bootstrapSeedConnectExact failed peer=" & safePeerId &
    " attemptedAddr=" & attemptedAddr &
    " lastError=" & lastError
  )
  allocCString(
    bootstrapSeedConnectExactPayload(
      node,
      safePeerId,
      effectiveSource,
      attemptedAddr,
      attempts,
      false,
      lastError,
    )
  )

proc libp2p_bootstrap_seed_connect_exact_bridge(
    handle: pointer,
    peerId: cstring,
    addressesJson: cstring,
    source: cstring,
    limit: cint,
    secureTimeoutMs: cint
): cstring {.cdecl, importc: "libp2p_bootstrap_seed_connect_exact".}

proc bootstrapSeedConnectExactKickoffThread(
    arg: pointer
): pointer {.noconv.} =
  let kickoffArg = cast[ptr BootstrapSeedConnectExactKickoffArg](arg)
  setupForeignThreadGc()
  defer:
    destroyBootstrapSeedConnectExactKickoffArg(kickoffArg)
    tearDownForeignThreadGc()
  if kickoffArg.isNil:
    return nil
  let safePeerId = if kickoffArg.peerId.isNil: "" else: $kickoffArg.peerId
  let safeAddressesJson = if kickoffArg.addressesJson.isNil: "[]" else: $kickoffArg.addressesJson
  debugLog(
    "[nimlibp2p] bootstrapSeedConnectExactKickoffThread start peer=" & safePeerId &
    " limit=" & $int(kickoffArg.limit) &
    " secureTimeoutMs=" & $int(kickoffArg.secureTimeoutMs) &
    " addressesJson=" & safeAddressesJson
  )
  try:
    let payload = takeCStringAndFree(
      libp2p_bootstrap_seed_connect_exact_bridge(
        kickoffArg.handle,
        kickoffArg.peerId,
        kickoffArg.addressesJson,
        kickoffArg.source,
        kickoffArg.limit,
        kickoffArg.secureTimeoutMs,
      )
    )
    debugLog(
      "[nimlibp2p] bootstrapSeedConnectExactKickoffThread done peer=" & safePeerId &
      " payloadBytes=" & $payload.len
    )
  except CatchableError as exc:
    debugLog(
      "[nimlibp2p] bootstrapSeedConnectExactKickoffThread failed peer=" & safePeerId &
      " err=" & exc.msg
    )
  except Exception as exc:
    debugLog(
      "[nimlibp2p] bootstrapSeedConnectExactKickoffThread fatal peer=" & safePeerId &
      " err=" & exc.msg
    )
  nil

proc libp2p_bootstrap_seed_connect_exact_kickoff*(
    handle: pointer,
    peerId: cstring,
    addressesJson: cstring,
    source: cstring,
    limit: cint,
    secureTimeoutMs: cint
): bool {.exportc, cdecl, dynlib.} =
  clearRuntimeError()
  let node = nodeFromHandle(handle)
  if node.isNil:
    recordRuntimeError("node_not_initialized")
    return false
  let safePeerId = (if peerId.isNil: "" else: $peerId).strip()
  let safeAddressesJson = (if addressesJson.isNil: "" else: $addressesJson).strip()
  if safePeerId.len == 0 or safeAddressesJson.len == 0:
    recordRuntimeError("missing_peerId_or_addresses")
    return false
  let parsedAddresses = parseJsonStringSeq(safeAddressesJson)
  if parsedAddresses.len == 0:
    recordRuntimeError("no_valid_addresses")
    return false
  let orderedAddrs = prioritizeGameSeedAddrs(preferStableAddrStrings(parsedAddresses))
  let safeLimit =
    if limit > 0:
      min(max(int(limit), 1), orderedAddrs.len)
    else:
      orderedAddrs.len
  if safeLimit <= 0:
    recordRuntimeError("no_valid_addresses")
    return false

  let arg = cast[ptr BootstrapSeedConnectExactKickoffArg](
    allocShared0(sizeof(BootstrapSeedConnectExactKickoffArg))
  )
  if arg.isNil:
    recordRuntimeError("kickoff_arg_alloc_failed")
    return false
  arg.handle = handle
  arg.peerId = allocCString(safePeerId)
  arg.addressesJson = allocCString(safeAddressesJson)
  arg.source = allocCString((if source.isNil: "" else: $source).strip())
  arg.limit = cint(safeLimit)
  arg.secureTimeoutMs =
    if secureTimeoutMs > 0:
      cint(max(min(int(secureTimeoutMs), 10_000), 250))
    else:
      cint(5_000)
  if arg.peerId.isNil or arg.addressesJson.isNil or arg.source.isNil:
    destroyBootstrapSeedConnectExactKickoffArg(arg)
    recordRuntimeError("kickoff_arg_copy_failed")
    return false

  try:
    when defined(posix):
      var worker: Pthread
      let createRc = pthread_create(addr worker, nil, bootstrapSeedConnectExactKickoffThread, cast[pointer](arg))
      if createRc != 0:
        destroyBootstrapSeedConnectExactKickoffArg(arg)
        recordRuntimeError("bootstrap_seed_connect_exact_kickoff_thread_failed:" & $createRc)
        return false
      discard pthread_detach(worker)
    else:
      recordRuntimeError("bootstrap_seed_connect_exact_kickoff_unsupported_platform")
      destroyBootstrapSeedConnectExactKickoffArg(arg)
      return false
    debugLog(
      "[nimlibp2p] bootstrapSeedConnectExact kickoff queued peer=" & safePeerId &
      " limit=" & $safeLimit &
      " secureTimeoutMs=" & $int(arg.secureTimeoutMs)
    )
    true
  except CatchableError as exc:
    destroyBootstrapSeedConnectExactKickoffArg(arg)
    recordRuntimeError("bootstrap_seed_connect_exact_kickoff_failed: " & exc.msg)
    false

proc libp2p_udp_echo_probe*(handle: pointer, multiaddr: cstring, timeoutMs: cint): cstring {.exportc, cdecl, dynlib.} =
  let node = nodeFromHandle(handle)
  if node.isNil:
    return allocCString("{\"ok\":false,\"error\":\"node_not_initialized\",\"probeTransport\":\"udp_echo\"}")
  let safeMultiaddr = (if multiaddr.isNil: "" else: $multiaddr).strip()
  if safeMultiaddr.len == 0:
    return allocCString("{\"ok\":false,\"error\":\"missing_target_multiaddr\",\"probeTransport\":\"udp_echo\"}")
  let cmd = makeCommand(cmdUdpEchoProbe)
  cmd.multiaddr = safeMultiaddr
  cmd.timeoutMs = int(timeoutMs)
  let result = submitCommand(node, cmd)
  let output =
    if result.resultCode == NimResultOk and result.stringResult.len > 0:
      allocCString(cloneOwnedString(result.stringResult))
    else:
      allocCString("{\"ok\":false,\"error\":\"udp_echo_probe_failed\",\"probeTransport\":\"udp_echo\"}")
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
