## Android 平台 mDNS 自测入口。
## 移植自 UniMaker 工程的 `mdns_selftest.nim`，仅依赖当前仓库代码。

import std/[os, strutils, options]
import chronos
import libp2p/[builders, multiaddress, peerid, switch]
import libp2p/discovery/[discoverymngr, mdns]

proc parseDuration(envName: string, defaultSeconds: int): Duration =
  ## 从环境变量读取秒级持续时间。
  let raw = getEnv(envName)
  if raw.len == 0:
    return defaultSeconds.seconds
  try:
    return parseInt(raw).seconds
  except ValueError:
    return defaultSeconds.seconds

proc parseMillis(envName: string, defaultMillis: int): Duration =
  ## 从环境变量读取毫秒级持续时间。
  let raw = getEnv(envName)
  if raw.len == 0:
    return defaultMillis.milliseconds
  try:
    return parseInt(raw).milliseconds
  except ValueError:
    return defaultMillis.milliseconds

proc initSwitch(listenAddr: string): Future[Switch] {.async: (raises: [CatchableError, Exception]).} =
  ## 初始化单节点开关并启动监听。
  let addrRes = MultiAddress.init(listenAddr)
  if addrRes.isErr:
    raise newException(ValueError, "invalid listen multiaddr: " & addrRes.error)
  let builder = newStandardSwitchBuilder(addrs = addrRes.get(), transport = TransportType.TCP)
  let sw = builder.build()
  await sw.start()
  return sw

proc runMdnsProbe(
    serviceName: string,
    listenAddr: string,
    duration: Duration,
    queryInterval: Duration,
    announceInterval: Duration
) {.async: (raises: [CatchableError, Exception]).} =
  ## 启动 mDNS 服务并持续监听一定时间。
  let sw = await initSwitch(listenAddr)
  defer:
    try:
      await sw.stop()
    except CatchableError:
      discard

  echo "[mdns-selftest] local peer: ", $sw.peerInfo.peerId
  for addr in sw.peerInfo.addrs:
    echo "[mdns-selftest] listen addr: ", $addr, "/p2p/", $sw.peerInfo.peerId

  let mdns =
    try:
      MdnsInterface.new(
        peerInfo = sw.peerInfo,
        serviceName = serviceName,
        queryInterval = queryInterval,
        announceInterval = announceInterval
      )
    except CatchableError as exc:
      raise newException(ValueError, "mdns init failed: " & exc.msg)
  mdns.advertisementUpdated = newAsyncEvent()
  mdns.toAdvertise.add(DiscoveryService(serviceName))
  mdns.toAdvertise.add(sw.peerInfo.peerId)
  for addr in sw.peerInfo.addrs:
    mdns.toAdvertise.add(addr)

  let discoveryQueue = newAsyncQueue[PeerAttributes]()
  mdns.onPeerFound = proc(pa: PeerAttributes) =
    echo "[mdns-selftest] onPeerFound raw attrs"
    for attr in pa:
      if attr.ofType(PeerId):
        echo "  peerId=", attr.to(PeerId)
      elif attr.ofType(MultiAddress):
        echo "  addr=", attr.to(MultiAddress)
      elif attr.ofType(DiscoveryService):
        echo "  service=", string attr.to(DiscoveryService)
    try:
      discoveryQueue.putNoWait(pa)
    except AsyncQueueFullError:
      discard

  var queryAttr: PeerAttributes
  queryAttr.add(DiscoveryService(serviceName))

  let advertiseLoop = mdns.advertise()
  let requestLoop = mdns.request(queryAttr)
  defer:
    if not advertiseLoop.finished():
      advertiseLoop.cancelSoon()
    if not requestLoop.finished():
      requestLoop.cancelSoon()

  proc watcher() {.async.} =
    while true:
      try:
        let attrs = await discoveryQueue.popFirst()
        let serviceOpt = attrs{DiscoveryService}
        if serviceOpt.isSome and string(serviceOpt.get()) != serviceName:
          continue
        let peerOpt = attrs{PeerId}
        if peerOpt.isNone:
          continue
        let remote = peerOpt.get()
        if remote == sw.peerInfo.peerId:
          echo "[mdns-selftest] observed self announcement (ignored)"
          continue
        echo "[mdns-selftest] discovered peer: ", $remote
        for addr in attrs.getAll(MultiAddress):
          echo "  addr: ", $addr
      except DiscoveryFinished:
        break
      except CancelledError:
        break
      except CatchableError as exc:
        echo "[mdns-selftest] discovery error: ", exc.msg
        await sleepAsync(2.seconds)

  asyncSpawn watcher()
  await sleepAsync(duration)

when isMainModule:
  let serviceName = getEnv("MDNS_SERVICE", "_unimaker._udp.")
  let listenAddr = getEnv("MDNS_LISTEN", "/ip4/0.0.0.0/tcp/0")
  let duration = parseDuration("MDNS_TEST_SECONDS", 30)
  let queryInterval = parseMillis("MDNS_QUERY_MS", 2000)
  let announceInterval = parseMillis("MDNS_ANNOUNCE_MS", 5000)

  try:
    waitFor runMdnsProbe(
      serviceName = serviceName,
      listenAddr = listenAddr,
      duration = duration,
      queryInterval = queryInterval,
      announceInterval = announceInterval
    )
  except CatchableError as exc:
    quit("[mdns-selftest] failed: " & exc.msg, QuitFailure)
