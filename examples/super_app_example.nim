## 17. 安卓 DApp 设计蓝图记录（2025-03-13 下午）
# - **总体架构**：采用多层设计——libp2p 节点（Nim 编译为 Android NDK 库，经 JNI 暴露）、数据层（Room/DataStore + GraphSync/Bitswap）、服务层（内容发布、DM、群聊红包、直播等）、UI 层（Jetpack Compose + Kotlin MVVM）。所有网络事件通过 `Libp2pNodeManager` 汇总，利用 Flow 与单向数据流保持 UI 状态一致。
# - **核心功能规划**：
#   1. **首页发现瀑布流**：订阅 `content-feed` gossip topic，记录 `FeedItem` （封面、媒体类型、描述）。大媒体借助 GraphSync/Bitswap 拉取并本地缓存，Compose `LazyVerticalGrid` 显示瀑布流。节点首页的封面信息由节点发布到 `ProfileTopic/peerId`。
#   2. **消息中心（DM + 通知）**：DM 走点对点 stream（Noise/TLS）并实现 ACK/重试保证有序不丢；Room 持久化消息状态。系统通知统一 topic 广播，UI 展示未读角标。
#   3. **节点导航**：关注列表本地存储，在线状态基于 Identify/Rendezvous；展示绑定域名且可跳转到节点详情；详情页提供节点信息、内容列表、DM 入口。
#   4. **局域网群聊与红包**：通过 mDNS/autonat 确定局域网节点，一键建群创建独立 PubSub topic；红包消息附带积分账本，抢红包后同步积分，流程参考微信。
#   5. **内容同步与 QUIC 迁移**：局域网内 GossipSub/Bitswap 同步内容；监听网络变化优先使用局域网 QUIC，并在链路切换时主动发起迁移，保持无缝体验。
#   6. **直播**：关注节点直播时在首页关注区提示；直播 topic `live/<peerId>` 使用 QUIC 低延迟传输，数据 plane 可选 RTP over stream 或自定义 chunk。
# - **运维与资源管理**：在 `SwitchBuilder` 中启用 ResourceManager、BandwidthManager、MemoryManager；配合 `withMetricsExporter` / `withOpenTelemetryExporter` 导出指标和日志；敏感通信可使用 PSK（私有网络）。
# - **开发建议**：以“内容→消息→群聊→直播→监控”顺序迭代，强化 JNI/NDK 封装与测试（网络切换、红包逻辑、直播压力等）；所有 libp2p 操作置于协程线程池，避免 UI 卡顿。
{.used.}

when not compileOption("threads"):
{.fatal: "please compile this example with --threads:on".}

import std/[json, options, os, sequtils, strformat, strutils, tables, times]
import chronos
import stew/byteutils

import libp2p/bandwidthmanager
import libp2p/builders
import libp2p/features
import libp2p/memorymanager
import libp2p/multiaddress
import libp2p/peerid
import libp2p/resourcemanager
import libp2p/stream/lpstream
import libp2p/switch
import libp2p/protocols/pubsub/[gossipsub, pubsub]
import libp2p/discovery/[discoverymngr, mdns]
when libp2pFetchEnabled:
  import libp2p/protocols/fetch/[fetch, protobuf]
when libp2pDataTransferEnabled:
  import libp2p/protocols/datatransfer/[datatransfer, protobuf]
import libp2p/protocols/rendezvous/rendezvous

const
  FeedTopic = "content-feed"
  NotificationsTopic = "/unimaker/system-notify"
  LanGroupPrefix = "lan/chat/"
  DefaultAddr = "/ip4/127.0.0.1/tcp/0"
  DemoProfileName = "Nim UniMaker"
  DirectCodec = "/unimaker/dm/1.0.0"
  DefaultMdnsService = "_unimaker._udp."
  DefaultMdnsQueryMs = 5_000
  DefaultMdnsAnnounceMs = 12_000
  LanDialBackoffMs = 5_000
  LanPeerExpiryMs = 10 * 60 * 1000
  MaxLanPeers = 48
  MaxTotalMemoryBytes = 256 * 1024 * 1024
  MaxPeerMemoryBytes = 64 * 1024 * 1024
  GossipMemoryTotalBytes = 128 * 1024 * 1024
  GossipMemoryPerPeerBytes = 16 * 1024 * 1024
  DirectMemoryTotalBytes = 32 * 1024 * 1024
  DirectMemoryPerPeerBytes = 8 * 1024 * 1024
  MaxConnectionsTotal = 320
  MaxConnectionsPerPeer = 48
  MaxStreamsTotal = 2048
  MaxStreamsPerPeer = 128
  GossipMaxMessageBytes = 2 * 1024 * 1024
  GossipQueueLimit = 512
  GossipBandwidthEstimate = 50_000_000
  GossipProtocolBandwidth = 5_000_000.0

proc bytesToString(payload: seq[byte]): string =
  if payload.len == 0:
    return ""
  result = newString(payload.len)
  copyMem(addr result[0], unsafeAddr payload[0], payload.len)

proc jsonGetStr(node: JsonNode, key: string, defaultValue: string = ""): string =
  if node.kind == JObject and node.hasKey(key):
    let v = node[key]
    case v.kind
    of JString: return v.getStr()
    of JInt: return $v.getInt()
    of JFloat: return $v.getFloat()
    of JBool: return if v.getBool(): "true" else: "false"
    else: discard
  defaultValue

proc jsonGetBool(node: JsonNode, key: string, defaultValue: bool = false): bool =
  if node.kind == JObject and node.hasKey(key):
    let v = node[key]
    case v.kind
    of JBool: return v.getBool()
    of JInt, JFloat: return v.getInt() != 0
    of JString: return v.getStr().toLowerAscii() in ["true", "1", "yes", "y"]
    else: discard
  defaultValue

proc clampSeq[T](s: var seq[T], limit: int) =
  while s.len > limit:
    s.delete(0)

proc nowMillis(): int64 =
  int64(epochTime() * 1000)

proc shortPeerId(peer: string): string =
  if peer.len <= 10:
    return peer
  peer[0 ..< 6] & "..." & peer[peer.len - 4 ..< peer.len]

proc formatElapsed(sinceMs: int64): string =
  let diff = max(nowMillis() - sinceMs, 0)
  let seconds = diff div 1000
  if seconds < 60:
    return $seconds & "s"
  let minutes = seconds div 60
  if minutes < 60:
    return $minutes & "m"
  let hours = minutes div 60
  if hours < 24:
    return $hours & "h"
  let days = hours div 24
  $days & "d"

proc pruneLanPeers(app: MegaApp) =
  let cutoff = nowMillis() - LanPeerExpiryMs
  var stale: seq[string] = @[]
  for peer, info in app.lanPeers.pairs:
    if info.lastSeen <= cutoff:
      stale.add(peer)
  for peer in stale:
    app.lanPeers.del(peer)

  if app.lanPeers.len <= MaxLanPeers:
    return
  var oldestPeer = ""
  var oldestSeen = high(int64)
  for peer, info in app.lanPeers.pairs:
    if info.lastSeen < oldestSeen:
      oldestSeen = info.lastSeen
      oldestPeer = peer
  if oldestPeer.len > 0:
    app.lanPeers.del(oldestPeer)

proc envDurationMs(key: string, fallback: int): Duration =
  let raw = getEnv(key)
  if raw.len == 0:
    return chronos.milliseconds(fallback)
  try:
    let value = parseInt(raw)
    if value > 0:
      return chronos.milliseconds(value)
  except ValueError:
    discard
  chronos.milliseconds(fallback)

proc lanAddrSummary(addrs: seq[MultiAddress]): string =
  if addrs.len == 0:
    return "(no address)"
  let head = $addrs[0]
  if addrs.len == 1:
    return head
  head & " +" & $(addrs.len - 1) & " more"

proc handleMdnsPeer(app: MegaApp, peerId: PeerId, addrs: seq[MultiAddress]) {.async.} =
  if not app.running:
    return
  let key = $peerId
  let nowMs = nowMillis()
  let wasKnown = app.lanPeers.hasKey(key)
  var info =
    if wasKnown:
      app.lanPeers[key]
    else:
      LanPeerInfo()
  let previousSeen = info.lastSeen
  info.addrs = addrs
  info.lastSeen = nowMs
  let shouldLog = (not wasKnown) or (previousSeen == 0) or (nowMs - previousSeen >= 15_000)
  let shouldDial =
    addrs.len > 0 and (
      (not info.connected) or (nowMs - info.lastDialAttempt >= LanDialBackoffMs)
    )
  if shouldDial:
    info.lastDialAttempt = nowMs
  app.lanPeers[key] = info
  app.pruneLanPeers()

  if shouldLog:
    app.logEvent(
      "[mdns] discovered LAN peer " & shortPeerId(key) & " @ " & lanAddrSummary(addrs)
    )

  if not shouldDial or not app.running:
    return

  try:
    await app.switch.connect(peerId, addrs, forceDial = false)
    info = app.lanPeers.getOrDefault(key, info)
    info.connected = true
    app.lanPeers[key] = info
    app.logEvent("[mdns] connected to " & shortPeerId(key))
  except CatchableError as exc:
    info = app.lanPeers.getOrDefault(key, info)
    info.connected = false
    app.lanPeers[key] = info
    app.logEvent("[mdns] failed to connect to " & shortPeerId(key) & ": " & exc.msg)

proc watchMdns(app: MegaApp, query: DiscoveryQuery): Future[void] {.async.} =
  while app.running:
    try:
      let attrs = await query.getPeer()
      let serviceOpt = attrs{DiscoveryService}
      if serviceOpt.isSome:
        let svc = string(serviceOpt.get())
        if app.mdnsService.len > 0 and svc != app.mdnsService:
          continue
      let peerOpt = attrs{PeerId}
      if peerOpt.isNone:
        continue
      let peerId = peerOpt.get()
      if $peerId == app.localPeer:
        continue
      await handleMdnsPeer(app, peerId, attrs.getAll(MultiAddress))
    except DiscoveryFinished:
      break
    except CancelledError:
      break
    except CatchableError as exc:
      if not app.running:
        break
      app.logEvent("[mdns] query failed: " & exc.msg)
      await sleepAsync(chronos.seconds(2))

proc startMdns(app: MegaApp) {.async.} =
  if app.discovery != nil:
    if app.mdnsWatcher != nil and not app.mdnsWatcher.finished():
      app.mdnsWatcher.cancelSoon()
    if app.mdnsQuery != nil:
      app.mdnsQuery.stop()
    app.discovery.stop()

  let serviceName =
    block:
      let envService = getEnv("UNIMAKER_MDNS_SERVICE")
      if envService.len > 0:
        envService
      else:
        DefaultMdnsService

  let queryInterval = envDurationMs("UNIMAKER_MDNS_QUERY_MS", DefaultMdnsQueryMs)
  let announceInterval =
    envDurationMs("UNIMAKER_MDNS_ANNOUNCE_MS", DefaultMdnsAnnounceMs)

  let disc = DiscoveryManager()
  let mdnsIface = MdnsInterface.new(
    peerInfo = app.switch.peerInfo,
    rng = newRng(),
    serviceName = serviceName,
    queryInterval = queryInterval,
    announceInterval = announceInterval
  )
  disc.add(mdnsIface)
  disc.advertise(DiscoveryService(serviceName))
  disc.advertise(app.switch.peerInfo.peerId)
  for addr in app.switch.peerInfo.addrs:
    disc.advertise(addr)

  app.discovery = disc
  app.mdnsInterface = mdnsIface
  app.mdnsService = serviceName
  let query = disc.request(DiscoveryService(serviceName))
  app.mdnsQuery = query

  let watcher = watchMdns(app, query)
  app.mdnsWatcher = watcher
  asyncSpawn watcher
  app.logEvent(
    "[mdns] service enabled " & serviceName & " (query " & $queryInterval & ", announce " & $announceInterval & ")"
  )

type
  LanPeerInfo = object
    addrs: seq[MultiAddress]
    lastSeen: int64
    lastDialAttempt: int64
    connected: bool
  MegaApp = ref object
    switch: Switch
    gossip: GossipSub
    rendezvous: RendezVous
    stdinReader: StreamTransport
    feedItems: seq[JsonNode]
    dmHistory: seq[string]
    notifications: seq[string]
    eventLog: seq[string]
    liveFrames: int
    running: bool
    feedTopic: string
    profileTopic: string
    liveTopic: string
    notificationsTopic: string
    localPeer: string
    directCodec: string
    selfDialAddress: string
    groupHandlers: Table[string, TopicHandler]
    groups: Table[string, int64]
    discovery: DiscoveryManager
    mdnsInterface: MdnsInterface
    mdnsQuery: DiscoveryQuery
    mdnsWatcher: Future[void]
    mdnsService: string
    lanPeers: Table[string, LanPeerInfo]
  DirectProto = ref object of LPProtocol
    app: MegaApp

proc logEvent(app: MegaApp, msg: string) =
  let timePart = now().format("HH:mm:ss")
  let line = "[" & timePart & "] " & msg
  echo line
  app.eventLog.add(line)
  clampSeq(app.eventLog, 40)

proc ensureSelfDialAddress(app: MegaApp): string =
  if app.selfDialAddress.len == 0:
    for address in app.switch.peerInfo.addrs:
      let strAddr = $address
      if strAddr.startsWith("/ip"):
        app.selfDialAddress = strAddr & "/p2p/" & app.localPeer
        break
    if app.selfDialAddress.len == 0 and app.switch.peerInfo.addrs.len > 0:
      app.selfDialAddress = $app.switch.peerInfo.addrs[0] & "/p2p/" & app.localPeer
  app.selfDialAddress

proc recordFeed(app: MegaApp, item: JsonNode) =
  app.feedItems.add(item)
  clampSeq(app.feedItems, 24)
  let author = jsonGetStr(item, "author", "unknown")
  let desc = jsonGetStr(item, "description", jsonGetStr(item, "title", "no description"))
  app.logEvent(&"[feed] {author} published: {desc}")

proc recordDm(app: MegaApp, message: string) =
  app.dmHistory.add(message)
  clampSeq(app.dmHistory, 32)

proc recordNotification(app: MegaApp, message: string) =
  app.notifications.add(message)
  clampSeq(app.notifications, 32)
  app.logEvent("[notify] " & message)

proc handleFeedMessage(app: MegaApp, payload: seq[byte]) {.async.} =
  let raw = bytesToString(payload)
  try:
    recordFeed(app, parseJson(raw))
  except CatchableError:
    recordFeed(app, %*{"description": raw, "author": "unknown"})

proc handleProfileMessage(app: MegaApp, payload: seq[byte]) {.async.} =
  let raw = bytesToString(payload)
  try:
    let data = parseJson(raw)
    let peerId = jsonGetStr(data, "peer_id", "unknown")
    let name = jsonGetStr(data, "display_name", DemoProfileName)
    app.logEvent(&"[profile] {peerId} updated display name: {name}")
  except CatchableError:
    app.logEvent("[profile] received unparseable profile: " & raw)

proc handleNotificationMessage(app: MegaApp, payload: seq[byte]) {.async.} =
  let raw = bytesToString(payload)
  try:
    let data = parseJson(raw)
    recordNotification(app, jsonGetStr(data, "body", raw))
  except CatchableError:
    recordNotification(app, raw)

proc handleLiveFrame(app: MegaApp, payload: seq[byte]) {.async.} =
  inc app.liveFrames
  app.logEvent(&"[live] received frame #{app.liveFrames}, size={payload.len} bytes")

proc handleLanMessage(app: MegaApp, groupId: string, payload: seq[byte]) {.async.} =
  let raw = bytesToString(payload)
  var body = raw
  var fromPeer = "unknown"
  try:
    let data = parseJson(raw)
    body = jsonGetStr(data, "body", raw)
    fromPeer = jsonGetStr(data, "from", fromPeer)
  except CatchableError:
    discard
  app.groups[groupId] = nowMillis()
  app.logEvent(&"[lan:{groupId}] {fromPeer}: {body}")

proc handleIncomingDirect(app: MegaApp, conn: Connection) {.async: (raises: [CancelledError]).} =
  let remote = $conn.peerId
  app.logEvent("[dm] inbound direct stream from " & remote)
  try:
    while true:
      let bytes = await conn.readLp(65536)
      if bytes.len == 0:
        break
      let raw = bytesToString(bytes)
      var envelope: JsonNode
      var ackRequested = false
      try:
        envelope = parseJson(raw)
        ackRequested = jsonGetBool(envelope, "ackRequested", false)
      except CatchableError:
        envelope = newJObject()
      let mid = jsonGetStr(envelope, "mid")
      let body =
        if envelope.kind == JObject and envelope.hasKey("body"):
          jsonGetStr(envelope, "body", raw)
        else:
          raw
      recordDm(app, &"[from {remote}] {body}")
      app.logEvent(&"[dm] {remote}: {body}")
      if ackRequested:
        var ack = newJObject()
        ack["op"] = %"ack"
        ack["mid"] = %mid
        ack["from"] = %app.localPeer
        ack["timestamp_ms"] = %nowMillis()
        ack["ack"] = %true
        try:
          await conn.writeLp(($ack).toBytes())
        except CatchableError as exc:
          app.logEvent("[dm] failed to send ACK: " & exc.msg)
  except LPStreamEOFError:
    discard
  except CatchableError as exc:
    app.logEvent("[dm] failed to process direct message: " & exc.msg)
  finally:
    try:
      await conn.close()
    except CatchableError:
      discard
    app.logEvent("[dm] direct stream closed: " & remote)

proc newDirectProto(app: MegaApp): DirectProto =
  let proto = DirectProto(app: app)
  proto.codec = DirectCodec
  proc handle(conn: Connection, codec: string) {.async: (raises: [CancelledError]), gcsafe.} =
    await handleIncomingDirect(app, conn)
  proto.handler = handle
  proto

proc connectToMultiaddr(app: MegaApp, address: string): Future[Connection] {.async.} =
  let idx = address.rfind("/p2p/")
  if idx < 0:
    raise newException(ValueError, "multiaddr missing /p2p/<peerId> segment")
  let baseAddr = address[0 ..< idx]
  let peerPart = address[(idx + 5) ..< address.len].strip()
  if peerPart.len == 0:
    raise newException(ValueError, "failed to parse PeerId")
  let peer = PeerId.init(peerPart).valueOr:
    raise newException(ValueError, "unable to decode PeerId: " & peerPart)
  let wire = MultiAddress.init(baseAddr).valueOr:
    raise newException(ValueError, "invalid dial address: " & baseAddr)
  await app.switch.dial(peer, @[wire], app.directCodec)

proc waitAck(app: MegaApp, conn: Connection, mid: string, timeoutMs: int): Future[bool] {.async.} =
  let deadline = nowMillis() + max(timeoutMs, 500)
  while nowMillis() <= deadline:
    let readFuture = conn.readLp(65536)
    let completed =
      try:
        await withTimeout(readFuture, chronos.milliseconds(400))
      except CancelledError as exc:
        raise exc
    if not completed:
      continue
    try:
      let bytes = await readFuture
      if bytes.len == 0:
        return false
      let raw = bytesToString(bytes)
      let envelope = parseJson(raw)
      if jsonGetStr(envelope, "op") == "ack" and jsonGetStr(envelope, "mid") == mid:
        return jsonGetBool(envelope, "ack", true)
      app.logEvent("[dm] received non-ACK message: " & raw)
    except LPStreamEOFError:
      break
    except CatchableError as exc:
      app.logEvent("[dm] failed while waiting for ACK: " & exc.msg)
      break
  false

proc sendDirectMessage(app: MegaApp, address, text: string) {.async.} =
  if text.len == 0:
    app.logEvent("[dm] message body must not be empty")
    return
  for attempt in 0 ..< 3:
    var conn: Connection
    try:
      conn = await connectToMultiaddr(app, address)
      let mid = "dm-" & $nowMillis()
      let envelope = %*{
        "op": "text",
        "mid": mid,
        "from": app.localPeer,
        "body": text,
        "timestamp_ms": nowMillis(),
        "ackRequested": true
      }
      await conn.writeLp(($envelope).toBytes())
      if await waitAck(app, conn, mid, 2_000):
        recordDm(app, &"[to {address}] {text}")
        app.logEvent("[dm] delivered and ACK received")
        return
      app.logEvent(&"[dm] attempt {attempt + 1} missing ACK; retrying")
    except CatchableError as exc:
      app.logEvent("[dm] send failed: " & exc.msg)
    finally:
      if conn != nil:
        try:
          await conn.close()
        except CatchableError:
          discard
    await sleepAsync(chronos.milliseconds(200))
  app.logEvent("[dm] maximum retries reached; send failed")

proc sendDirectToSelf(app: MegaApp, text: string) {.async.} =
  let address = ensureSelfDialAddress(app)
  if address.len == 0:
    app.logEvent("[dm] no local listen address; cannot self-test direct messaging")
    return
  await sendDirectMessage(app, address, text)

proc publishFeed(app: MegaApp, description: string) {.async.} =
  let payload = %*{
    "id": "demo-" & $nowMillis(),
    "author": app.localPeer,
    "media_type": "image",
    "description": description,
    "timestamp": nowMillis()
  }
  discard await app.gossip.PubSub.publish(app.feedTopic, ($payload).toBytes())
  app.logEvent("[feed] published waterfall card")

proc publishProfile(app: MegaApp) {.async.} =
  let profile = %*{
    "peer_id": app.localPeer,
    "display_name": DemoProfileName,
    "avatar": "cid://demo-avatar",
    "headline": "Libp2p Android node"
  }
  discard await app.gossip.PubSub.publish(app.profileTopic, ($profile).toBytes())
  app.logEvent("[profile] broadcast profile")

proc publishNotification(app: MegaApp, body: string) {.async.} =
  let notif = %*{
    "body": body,
    "timestamp_ms": nowMillis()
  }
  discard await app.gossip.PubSub.publish(app.notificationsTopic, ($notif).toBytes())
  recordNotification(app, body)

proc publishLiveFrame(app: MegaApp, label: string) {.async.} =
  inc app.liveFrames
  let frameIndex = app.liveFrames
  let frame = &"frame-{frameIndex}-{label}"
  discard await app.gossip.PubSub.publish(app.liveTopic, frame.toBytes())
  app.logEvent(&"[live] streamed frame #{app.liveFrames}")

proc joinLanGroup(app: MegaApp, groupId: string) {.async.} =
  if groupId.len == 0:
    app.logEvent("[lan] group ID must not be empty")
    return
  let topic = LanGroupPrefix & groupId
  if app.groupHandlers.hasKey(topic):
    app.logEvent("[lan] already joined group: " & groupId)
    return
  proc handler(topic: string, payload: seq[byte]) {.async.} =
    await handleLanMessage(app, groupId, payload)
  app.gossip.PubSub.subscribe(topic, handler)
  app.groupHandlers[topic] = handler
  app.groups[groupId] = nowMillis()
  app.logEvent("[lan] joined group: " & groupId)

proc leaveLanGroup(app: MegaApp, groupId: string) {.async.} =
  let topic = LanGroupPrefix & groupId
  if not app.groupHandlers.hasKey(topic):
    app.logEvent("[lan] not joined to that group")
    return
  let handler = app.groupHandlers.getOrDefault(topic)
  try:
    app.gossip.PubSub.unsubscribe(topic, handler)
  except CatchableError as exc:
    app.logEvent("[lan] failed to unsubscribe: " & exc.msg)
  app.groupHandlers.del(topic)
  if app.groups.hasKey(groupId):
    app.groups.del(groupId)
  app.logEvent("[lan] left group: " & groupId)

proc sendLanMessage(app: MegaApp, groupId, text: string) {.async.} =
  if text.len == 0:
    app.logEvent("[lan] message body must not be empty")
    return
  let topic = LanGroupPrefix & groupId
  if not app.groupHandlers.hasKey(topic):
    app.logEvent("[lan] join the group first: " & groupId)
    return
  let payload = %*{
    "mid": "lan-" & $nowMillis(),
    "from": app.localPeer,
    "body": text,
    "timestamp_ms": nowMillis()
  }
  discard await app.gossip.PubSub.publish(topic, ($payload).toBytes())
  app.logEvent(&"[lan:{groupId}] sent: {text}")

proc advertiseRendezvous(app: MegaApp, ns: string) {.async.} =
  if ns.len == 0:
    app.logEvent("[rdv] namespace must not be empty")
    return
  try:
    await app.rendezvous.advertise(ns, chronos.minutes(5))
    let local = app.rendezvous.requestLocally(ns)
    app.logEvent(&"[rdv] registered namespace {ns}, local records={local.len}")
  except CatchableError as exc:
    app.logEvent("[rdv] registration failed: " & exc.msg)

proc printStatus(app: MegaApp) =
  echo "\n------ Node Overview ------"
  echo "PeerId: ", app.localPeer
  echo "Listen addresses:"
  for address in app.switch.peerInfo.addrs:
    echo "  ", $address, "/p2p/", app.localPeer
  echo "Recent feed:"
  for item in app.feedItems[^min(app.feedItems.len, 3) ..< app.feedItems.len]:
    echo "  - ", jsonGetStr(item, "description"), " (author ", jsonGetStr(item, "author"), ")"
  echo "DM total entries: ", app.dmHistory.len
  if app.dmHistory.len > 0:
    echo "  Latest: ", app.dmHistory[^1]
  echo "Live frame count: ", app.liveFrames
  echo "Joined groups: ", toSeq(app.groups.keys)
  echo "LAN peer count: ", app.lanPeers.len
  if app.lanPeers.len > 0:
    let peers = toSeq(app.lanPeers.keys)
    var displayed = 0
    for peer in peers:
      if displayed >= 5:
        break
      let info = app.lanPeers[peer]
      echo "  - ", shortPeerId(peer), " last seen ", formatElapsed(info.lastSeen), " ago @ ", lanAddrSummary(info.addrs)
      inc displayed
  echo "----------------------\n"

proc printEvents(app: MegaApp) =
  echo "\n------ Recent Events ------"
  for line in app.eventLog:
    echo line
  echo "----------------------\n"

proc printHelp() =
  echo """
Commands:
  help                show this help
  status              print node overview
  events              display recent events
  feed [text]         publish a feed card
  profile             broadcast profile info
  notify <text>       broadcast notification
  dmself <text>       send a direct message to self and wait for ACK
  dm <addr> <text>    send a direct message over the given multiaddr
  join <groupId>      join a LAN group
  leave <groupId>     leave a LAN group
  send <groupId> <text>  send a group message
  live [label]        push one live frame
  rendezvous <ns>     advertise in the given rendezvous namespace
  network             simulate network change and prefer QUIC/local path
  exit                shut down the node gracefully
"""

proc handleCommand(app: MegaApp, line: string) {.async.} =
  let trimmed = line.strip()
  if trimmed.len == 0:
    return
  let spaceIdx = trimmed.find(' ')
  let cmd = (if spaceIdx < 0: trimmed else: trimmed[0 ..< spaceIdx]).toLowerAscii()
  let args = if spaceIdx < 0: "" else: trimmed[spaceIdx + 1 ..< trimmed.len].strip()
  case cmd
  of "help", "?":
    printHelp()
  of "status":
    printStatus(app)
  of "events":
    printEvents(app)
  of "feed":
    await publishFeed(app, if args.len == 0: "homepage waterfall demo" else: args)
  of "profile":
    await publishProfile(app)
  of "notify":
    if args.len == 0:
      app.logEvent("[notify] text must not be empty")
    else:
      await publishNotification(app, args)
  of "dmself":
    await sendDirectToSelf(app, if args.len == 0: "Welcome to the UniMaker message center" else: args)
  of "dm":
    let idx = args.find(' ')
    if idx < 0:
      app.logEvent("usage: dm <multiaddr> <text>")
    else:
      let address = args[0 ..< idx]
      let text = args[idx + 1 ..< args.len].strip()
      await sendDirectMessage(app, address, text)
  of "join":
    await joinLanGroup(app, args)
  of "leave":
    await leaveLanGroup(app, args)
  of "send":
    let idx = args.find(' ')
    if idx < 0:
      app.logEvent("usage: send <groupId> <text>")
    else:
      let gid = args[0 ..< idx]
      let text = args[idx + 1 ..< args.len].strip()
      await sendLanMessage(app, gid, text)
  of "live":
    await publishLiveFrame(app, if args.len == 0: "demo" else: args)
  of "rendezvous":
    await advertiseRendezvous(app, args)
  of "network":
    app.logEvent("[network] detected network change; preferring QUIC/local link")
  of "exit", "quit":
    app.running = false
  else:
    app.logEvent("unknown command: " & cmd & "; type help for usage")

proc setupDefaultSubscriptions(app: MegaApp) =
  app.gossip.PubSub.subscribe(
    app.feedTopic,
    proc(topic: string, payload: seq[byte]) {.async.} =
      await handleFeedMessage(app, payload)
  )
  app.gossip.PubSub.subscribe(
    app.profileTopic,
    proc(topic: string, payload: seq[byte]) {.async.} =
      await handleProfileMessage(app, payload)
  )
  app.gossip.PubSub.subscribe(
    app.notificationsTopic,
    proc(topic: string, payload: seq[byte]) {.async.} =
      await handleNotificationMessage(app, payload)
  )
  app.gossip.PubSub.subscribe(
    app.liveTopic,
    proc(topic: string, payload: seq[byte]) {.async.} =
      await handleLiveFrame(app, payload)
  )

proc bootstrapDemo(app: MegaApp) {.async.} =
  await publishProfile(app)
  await sleepAsync(chronos.milliseconds(80))
  await publishFeed(app, "homepage waterfall demo")
  await sleepAsync(chronos.milliseconds(80))
  await publishNotification(app, "System notice: welcome to the UniMaker network node")
  await sleepAsync(chronos.milliseconds(80))
  await sendDirectToSelf(app, "Welcome to the UniMaker message center")
  await sleepAsync(chronos.milliseconds(80))
  await publishLiveFrame(app, "warmup")
  app.logEvent("basic self-check complete; awaiting commands.")

proc printWelcome(app: MegaApp) =
  echo "=========================================="
  echo " UniMaker Mega App Demo (Gossip + Direct) "
  echo "=========================================="
  echo "Local PeerId: ", app.localPeer
  echo "Listen addresses:"
  for address in app.switch.peerInfo.addrs:
    echo "  ", $address, "/p2p/", app.localPeer
  let selfAddr = ensureSelfDialAddress(app)
  if selfAddr.len > 0:
    echo "Local direct-test address: ", selfAddr
  echo "Type help for commands; exit to stop."

proc newMegaApp(stdinReader: StreamTransport): Future[MegaApp] {.async.} =
  let rng = newRng()

  var bandwidthLimits = BandwidthLimitConfig.init()
  var gossipThrottle = ThrottleConfig.init()
  gossipThrottle.inbound = some(DirectionLimit.init(GossipProtocolBandwidth, 2.0))
  gossipThrottle.outbound = some(DirectionLimit.init(GossipProtocolBandwidth, 2.0))
  bandwidthLimits.setProtocolLimit(GossipSubCodec_14, gossipThrottle)

  var memoryLimits = MemoryLimitConfig.init()
  memoryLimits.maxTotal = MaxTotalMemoryBytes
  memoryLimits.perPeer = MaxPeerMemoryBytes
  memoryLimits.setProtocolLimit(
    GossipSubCodec_14,
    MemoryProtocolLimit.init(
      maxTotal = GossipMemoryTotalBytes, perPeer = GossipMemoryPerPeerBytes
    )
  )
  memoryLimits.setProtocolLimit(
    DirectCodec,
    MemoryProtocolLimit.init(
      maxTotal = DirectMemoryTotalBytes, perPeer = DirectMemoryPerPeerBytes
    )
  )

  var connectionLimits = ConnectionLimits.init(
    maxTotal = MaxConnectionsTotal,
    maxInbound = MaxConnectionsTotal div 2,
    maxOutbound = MaxConnectionsTotal div 2,
    perPeerTotal = MaxConnectionsPerPeer,
    perPeerInbound = MaxConnectionsPerPeer div 2,
    perPeerOutbound = MaxConnectionsPerPeer div 2
  )

  var streamLimits = StreamLimits.init(
    maxTotal = MaxStreamsTotal,
    perPeerTotal = MaxStreamsPerPeer,
    perPeerInbound = MaxStreamsPerPeer div 2,
    perPeerOutbound = MaxStreamsPerPeer div 2
  )
  streamLimits.setProtocolLimit(
    GossipSubCodec_14,
    maxTotal = 512,
    maxInbound = 320,
    maxOutbound = 320,
    perPeerTotal = 48,
    perPeerInbound = 32,
    perPeerOutbound = 32
  )
  streamLimits.setProtocolLimit(
    DirectCodec,
    maxTotal = 96,
    maxInbound = 64,
    maxOutbound = 64,
    perPeerTotal = 16,
    perPeerInbound = 12,
    perPeerOutbound = 12
  )
  streamLimits.setProtocolGroup(GossipSubCodec_14, "pubsub")
  streamLimits.setGroupLimit(
    "pubsub",
    maxTotal = 640,
    perPeerTotal = 64,
    perPeerInbound = 48,
    perPeerOutbound = 48
  )

  let resConfig = ResourceManagerConfig.init(connectionLimits, streamLimits)

  var builder = SwitchBuilder.new()
  builder = builder.withRng(rng)
  builder = builder.withAddresses(@[MultiAddress.init(DefaultAddr).tryGet()])
  builder = builder.withTcpTransport()
  when defined(libp2p_msquic_experimental):
    builder = builder.withMsQuicTransport()
  elif defined(libp2p_quic_support):
    builder = builder.withQuicTransport()
  builder = builder.withYamux()
  builder = builder.withNoise()
  builder = builder.withTls()
  builder = builder.withBandwidthLimits(bandwidthLimits)
  builder = builder.withMemoryLimits(memoryLimits)
  builder = builder.withResourceManager(resConfig)
  builder = builder.withAutonat()
  builder = builder.withCircuitRelay()
  builder = builder.withMetricsExporter(port = Port(9310))

  let rendez = RendezVous.new()
  builder = builder.withRendezVous(rendez)

  let otelEndpoint = getEnv("LIBP2P_OTEL_ENDPOINT")
  if otelEndpoint.len > 0:
    try:
      builder = builder.withOpenTelemetryExporter(
        endpoint = otelEndpoint,
        resourceAttributes = [("service.name", "unimaker-demo"), ("service.version", "0.2.0")]
      )
    except ValueError as exc:
      echo "[mega-app] OpenTelemetry exporter not enabled: ", exc.msg

  when libp2pFetchEnabled:
    builder = builder.withFetch(
      proc(key: string): Future[FetchResponse] {.async.} =
        ## 简化 GraphSync/Bitswap：这里只返回 404，实际项目中替换为真实数据层。
        FetchResponse(status: FetchStatus.fsNotFound, data: @[])
    )

  when libp2pDataTransferEnabled:
    builder = builder.withDataTransfer(
      proc(conn: Connection, msg: DataTransferMessage): Future[Option[DataTransferMessage]] {.async.} =
        ## 示例中忽略数据传输请求，保持接口形状便于后续扩展。
        none(DataTransferMessage)
    )

  let psk = getEnv("LIBP2P_PSK")
  if psk.len > 0:
    try:
      builder = builder.withPnetFromString(psk)
    except CatchableError as exc:
      echo "[mega-app] private network PSK configuration failed: ", exc.msg

  let sw = builder.build()
  let gossipParams = GossipSubParams.init(
    maxMessageSize = GossipMaxMessageBytes,
    maxNumElementsInNonPriorityQueue = GossipQueueLimit,
    bandwidthEstimatebps = GossipBandwidthEstimate,
    sendIDontWantOnPublish = true
  )
  let gossip = GossipSub.init(
    switch = sw,
    parameters = gossipParams,
    verifySignature = true,
    sign = true
  )
  sw.mount(gossip.PubSub)
  sw.mount(rendez)

  let app = MegaApp(
    switch: sw,
    gossip: gossip,
    rendezvous: rendez,
    stdinReader: stdinReader,
    feedItems: @[],
    dmHistory: @[],
    notifications: @[],
    eventLog: @[],
    liveFrames: 0,
    running: true,
    feedTopic: FeedTopic,
    profileTopic: "",
    liveTopic: "",
    notificationsTopic: NotificationsTopic,
    localPeer: "",
    directCodec: DirectCodec,
    selfDialAddress: "",
    groupHandlers: initTable[string, TopicHandler](),
    groups: initTable[string, int64](),
    discovery: nil,
    mdnsInterface: nil,
    mdnsQuery: nil,
    mdnsWatcher: nil,
    mdnsService: "",
    lanPeers: initTable[string, LanPeerInfo]()
  )

  let proto = newDirectProto(app)
  sw.mount(proto)

  await sw.start()

  app.localPeer = $sw.peerInfo.peerId
  app.profileTopic = "ProfileTopic/" & app.localPeer
  app.liveTopic = "live/" & app.localPeer

  await startMdns(app)
  setupDefaultSubscriptions(app)
  discard ensureSelfDialAddress(app)
  printWelcome(app)
  await bootstrapDemo(app)

  return app

proc runUi(app: MegaApp) {.async.} =
  printHelp()
  while app.running:
    stdout.write("\ncommand> ")
    stdout.flushFile()
    let line = await app.stdinReader.readLine()
    await handleCommand(app, line)
  app.logEvent("shutting down node...")

proc shutdown(app: MegaApp) {.async.} =
  if app.mdnsWatcher != nil and not app.mdnsWatcher.finished():
    app.mdnsWatcher.cancelSoon()
  if app.mdnsQuery != nil:
    app.mdnsQuery.stop()
  if app.discovery != nil:
    app.discovery.stop()
  if app.mdnsWatcher != nil:
    try:
      await app.mdnsWatcher
    except CancelledError:
      discard
    except CatchableError:
      discard
  try:
    await app.switch.stop()
  except CatchableError as exc:
    echo "[mega-app] failed to stop switch: ", exc.msg

proc readInput(wfd: AsyncFD) {.thread.} =
  let transp = fromPipe(wfd)
  while true:
    var line: string
    try:
      line = stdin.readLine()
    except EOFError:
      break
    discard waitFor transp.write(line & "\n")

proc main() {.async.} =
  let (rfd, wfd) = createAsyncPipe()
  let stdinReader = fromPipe(rfd)
  var inputThread: Thread[AsyncFD]
  try:
    inputThread.createThread(readInput, wfd)
  except Exception as exc:
    quit("failed to create input thread: " & exc.msg)

  let app = await newMegaApp(stdinReader)
  try:
    await runUi(app)
  finally:
    await shutdown(app)
  quit(0)

when isMainModule:
  waitFor main()
