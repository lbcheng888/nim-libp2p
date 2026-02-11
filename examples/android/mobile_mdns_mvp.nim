import std/[os, strutils, options]
import chronos
import libp2p/[builders, multiaddress, switch, peerid]
import libp2p/discovery/[discoverymngr, mdns]

proc parseDuration(envName: string, defaultSeconds: int): Duration =
  let raw = getEnv(envName)
  if raw.len == 0:
    return defaultSeconds.seconds
  try:
    return parseInt(raw).seconds
  except ValueError:
    return defaultSeconds.seconds

proc parseMillis(envName: string, defaultMillis: int): Duration =
  let raw = getEnv(envName)
  if raw.len == 0:
    return defaultMillis.milliseconds
  try:
    return parseInt(raw).milliseconds
  except ValueError:
    return defaultMillis.milliseconds

proc initSwitch(listenAddr: string): Future[Switch] {.async: (raises: [CatchableError, Exception]).} =
  var addrs: seq[MultiAddress]

  proc appendAddr(raw: string) =
    let trimmed = raw.strip()
    if trimmed.len == 0:
      return
    let parsed = MultiAddress.init(trimmed)
    if parsed.isErr:
      echo "[mobile-mdns] invalid listen multiaddr: ", trimmed, " err=", parsed.error
      return
    addrs.add(parsed.get())

  appendAddr(listenAddr)
  appendAddr(getEnv("MOBILE_MDNS_LISTEN_V6", "/ip6/::/tcp/0"))

  if addrs.len == 0:
    raise newException(ValueError, "no valid listen addresses provided")

  let builder = newStandardSwitchBuilder(addrs = addrs, transport = TransportType.TCP)
  let sw = builder.build()
  await sw.start()
  echo "[mobile-mdns] local peer: ", $sw.peerInfo.peerId
  for addr in sw.peerInfo.addrs:
    echo "[mobile-mdns] listen addr: ", $addr, "/p2p/", $sw.peerInfo.peerId
  return sw

proc runMdnsProbe(
    serviceName: string,
    listenAddr: string,
    duration: Duration,
    queryInterval: Duration,
    announceInterval: Duration
) {.async: (raises: [CatchableError, Exception]).} =
  let sw = await initSwitch(listenAddr)
  defer:
    try:
      await sw.stop()
    except CatchableError:
      discard

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
    echo "[mobile-mdns] onPeerFound raw attrs"
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

  var ipv4Hits = 0
  var ipv6Hits = 0

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
          echo "[mobile-mdns] observed self announcement (ignored)"
          continue
        echo "[mobile-mdns] discovered peer: ", $remote
        for addr in attrs.getAll(MultiAddress):
          let addrStr = $addr
          echo "  addr: ", addrStr
          if "/ip4/" in addrStr:
            inc ipv4Hits
          elif "/ip6/" in addrStr:
            inc ipv6Hits
      except DiscoveryFinished:
        break
      except CancelledError:
        break
      except CatchableError as exc:
        echo "[mobile-mdns] discovery error: ", exc.msg
        await sleepAsync(2.seconds)

  let watcherFut = watcher()
  asyncCheck watcherFut
  await sleepAsync(duration)

  if not watcherFut.finished():
    watcherFut.cancelSoon()
    try:
      await watcherFut
    except CancelledError:
      discard

  echo "[mobile-mdns] summary ipv4=", ipv4Hits, " ipv6=", ipv6Hits
  if ipv4Hits == 0:
    echo "[mobile-mdns] WARNING: no IPv4 addresses observed"
  if ipv6Hits == 0:
    echo "[mobile-mdns] WARNING: no IPv6 addresses observed"

when isMainModule:
  let serviceName = getEnv("MOBILE_MDNS_SERVICE", "_unimaker._udp.")
  let listenAddr = getEnv("MOBILE_MDNS_LISTEN", "/ip4/0.0.0.0/tcp/0")
  let duration = parseDuration("MOBILE_MDNS_SECONDS", 15)
  let queryInterval = parseMillis("MOBILE_MDNS_QUERY_MS", 2000)
  let announceInterval = parseMillis("MOBILE_MDNS_ANNOUNCE_MS", 5000)

  try:
    waitFor runMdnsProbe(
      serviceName = serviceName,
      listenAddr = listenAddr,
      duration = duration,
      queryInterval = queryInterval,
      announceInterval = announceInterval
    )
  except CatchableError as exc:
    quit("[mobile-mdns] failed: " & exc.msg, QuitFailure)
