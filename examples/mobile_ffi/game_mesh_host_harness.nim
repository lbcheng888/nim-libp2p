{.used.}

{.emit: """
void nim_bridge_emit_event(const char *topic, const char *payload) {
  (void)topic;
  (void)payload;
}
""".}

import std/[algorithm, base64, json, os, parseopt, sequtils, sets, strformat, strutils]
from std/times import epochTime

import ../../libp2p/crypto/crypto
import ../../libp2p/peerid
import ./libnimlibp2p

when defined(posix):
  proc hardExit(code: cint) {.importc: "_exit", header: "<unistd.h>".}

type
  HostTransportMode* = enum
    htmTcpExplicitSeed
    htmTcpPublicSeed
    htmQuicExplicitSeed
    htmQuicDualStackFixedPort

  HostNode* = object
    label*: string
    handle*: pointer
    dataDir*: string
    peerId*: string
    dialAddr*: string
    dialAddrs*: seq[string]
    transportMode*: HostTransportMode
    listenPort*: int

proc nowMillis*(): int64 =
  int64(epochTime() * 1000)

proc takeCStringAndFree*(value: cstring): string =
  if value.isNil:
    return ""
  result = $value
  libp2p_string_free(value)

proc safeCString*(value: cstring): string {.raises: [].} =
  try:
    takeCStringAndFree(value)
  except CatchableError:
    ""

template safeCStringExpr*(valueExpr: untyped): string =
  block:
    try:
      let value = valueExpr
      safeCString(value)
    except CatchableError:
      ""

proc fallbackJsonNode(fallback: string): JsonNode =
  if fallback.len > 0 and fallback[0] == '[':
    newJArray()
  else:
    newJObject()

proc cloneJson(node: JsonNode): JsonNode =
  if node.isNil:
    return newJNull()
  case node.kind
  of JObject:
    result = newJObject()
    for key, value in node:
      result[key] = cloneJson(value)
  of JArray:
    result = newJArray()
    for value in node:
      result.add(cloneJson(value))
  of JString:
    result = %node.getStr()
  of JInt:
    result = %node.getInt()
  of JFloat:
    result = %node.getFloat()
  of JBool:
    result = %node.getBool()
  of JNull:
    result = newJNull()

proc safeJsonFromCString*(value: cstring, fallback = "{}"): JsonNode {.raises: [].} =
  try:
    let text = safeCString(value)
    if text.len == 0:
      return fallbackJsonNode(fallback)
    if text.len > 1_000_000:
      return fallbackJsonNode(fallback)
    if text[0] notin {'{', '['}:
      return fallbackJsonNode(fallback)
    parseJson(text)
  except CatchableError:
    fallbackJsonNode(fallback)

template safeJsonExpr*(valueExpr: untyped, fallback = "{}"): JsonNode =
  block:
    try:
      let value = valueExpr
      safeJsonFromCString(value, fallback)
    except CatchableError:
      fallbackJsonNode(fallback)

proc intField*(node: JsonNode, key: string, defaultValue = 0): int =
  if node.kind == JObject and node.hasKey(key):
    let value = node[key]
    case value.kind
    of JInt:
      return value.getInt()
    of JFloat:
      return int(value.getFloat())
    of JString:
      try:
        return parseInt(value.getStr())
      except ValueError:
        discard
    else:
      discard
  defaultValue

proc boolField*(node: JsonNode, key: string, defaultValue = false): bool =
  if node.kind == JObject and node.hasKey(key) and node[key].kind == JBool:
    return node[key].getBool()
  defaultValue

proc strField*(node: JsonNode, key: string, defaultValue = ""): string =
  if node.kind == JObject and node.hasKey(key) and node[key].kind == JString:
    return node[key].getStr()
  defaultValue

proc stringArray(values: seq[string]): JsonNode =
  result = newJArray()
  for value in values:
    result.add(%value)

proc hostHarnessDataDir(prefix: string): string =
  let path = getTempDir() / "nim-libp2p-game-mesh-host" / (prefix & "-" & $nowMillis())
  createDir(path)
  path

proc testIdentity(): JsonNode =
  let rng = newRng()
  let pair = KeyPair.random(PKScheme.Ed25519, rng[]).get()
  let privateKey = pair.seckey.getRawBytes().get()
  let publicKey = pair.pubkey.getRawBytes().get()
  let peerId = $PeerId.init(pair.seckey).get()
  %*{
    "privateKey": base64.encode(privateKey),
    "publicKey": base64.encode(publicKey),
    "peerId": peerId,
    "source": "host_harness"
  }

proc seededIdentity(seedText: string; source = "host_harness_seeded"): JsonNode =
  let normalized = seedText.strip()
  if normalized.len == 0:
    return testIdentity()
  var seedBytes = newSeq[byte](normalized.len)
  for idx, ch in normalized:
    seedBytes[idx] = byte(ord(ch) and 0xff)
  let payload =
    if seedBytes.len > 0:
      safeJsonFromCString(
        libp2p_identity_from_seed(
          cast[ptr uint8](unsafeAddr seedBytes[0]),
          csize_t(seedBytes.len)
        )
      )
    else:
      newJObject()
  if payload.kind == JObject and strField(payload, "peerId").len > 0:
    result = payload
    result["source"] = %source
    return result
  testIdentity()

proc stableWanBootstrapIdentity(label, role, networkId: string): JsonNode =
  seededIdentity(
    "wan_bootstrap_host|" & label.strip() & "|" & role.strip().toLowerAscii() & "|" &
      networkId.strip(),
    source = "host_harness_stable_wan_bootstrap"
  )

proc dualStackLoopbackListenAddrs(port: int): JsonNode =
  %*[
    "/ip4/127.0.0.1/udp/" & $port & "/quic-v1",
    "/ip6/::1/udp/" & $port & "/quic-v1",
    "/ip4/127.0.0.1/tcp/" & $port,
    "/ip6/::1/tcp/" & $port
  ]

proc isPhysicalPrefixedAddr(maddr: string): bool =
  let lower = maddr.toLowerAscii()
  lower.startsWith("/awdl/") or lower.startsWith("/nan/") or lower.startsWith("/nearlink/")

proc isConcreteIpv4Multiaddr(maddr: string): bool =
  let lower = maddr.toLowerAscii()
  "/ip4/" in lower and "/ip4/0.0.0.0/" notin lower and "/ip4/127.0.0.1/" notin lower

proc isConcreteIpv6Multiaddr(maddr: string): bool =
  let lower = maddr.toLowerAscii()
  "/ip6/" in lower and "/ip6/::/" notin lower and "/ip6/::1/" notin lower and
    "/ip6/fe80:" notin lower

proc protocolValue(maddr, proto: string): string =
  if maddr.len == 0:
    return ""
  let marker = "/" & proto.toLowerAscii() & "/"
  let lower = maddr.toLowerAscii()
  let idx = lower.find(marker)
  if idx < 0:
    return ""
  let startIdx = idx + marker.len
  var endIdx = maddr.find('/', startIdx)
  if endIdx < 0:
    endIdx = maddr.len
  maddr[startIdx ..< endIdx]

proc candidateDialAddrsFromJson(
    raw, peerId: string, allowLoopbackFallback: bool
): seq[string] =
  if raw.len == 0:
    return @[]
  let parsed = parseJson(raw)
  if parsed.kind != JArray:
    return @[]
  var seen = initHashSet[string]()
  for item in parsed:
    if item.kind != JString:
      continue
    var multiaddr = item.getStr().strip()
    if allowLoopbackFallback:
      multiaddr = multiaddr.replace("/ip4/0.0.0.0/", "/ip4/127.0.0.1/")
      multiaddr = multiaddr.replace("/ip6/::/", "/ip6/::1/")
    if "/p2p/" notin multiaddr and "/ipfs/" notin multiaddr:
      multiaddr &= "/p2p/" & peerId
    if multiaddr.len > 0 and multiaddr notin seen:
      result.add(multiaddr)
      seen.incl(multiaddr)

proc firstDialAddrFromJson(
    raw, peerId: string, mode: HostTransportMode, allowLoopbackFallback: bool
): string =
  var candidates = candidateDialAddrsFromJson(raw, peerId, allowLoopbackFallback)
  sort(candidates, proc(a, b: string): int =
    proc dialRank(multiaddr: string): int =
      let lower = multiaddr.toLowerAscii()
      let hasQuic = "/quic-v1" in lower
      let hasTcp = "/tcp/" in lower
      let hasIpv6 = "/ip6/" in lower and "/ip6/fe80:" notin lower
      let hasIpv4 = "/ip4/" in lower
      let hasPhysical = isPhysicalPrefixedAddr(multiaddr)
      if hasQuic and hasIpv6:
        return (if hasPhysical: 1 else: 0)
      if hasQuic and hasIpv4:
        return (if hasPhysical: 3 else: 2)
      if hasTcp and hasIpv6:
        return (if hasPhysical: 5 else: 4)
      if hasTcp and hasIpv4:
        return (if hasPhysical: 7 else: 6)
      if hasIpv6:
        return (if hasPhysical: 9 else: 8)
      if hasIpv4:
        return (if hasPhysical: 11 else: 10)
      12
    let rankA = dialRank(a)
    let rankB = dialRank(b)
    if rankA < rankB:
      return -1
    if rankA > rankB:
      return 1
    system.cmp(a, b)
  )
  for multiaddr in candidates:
    let lower = multiaddr.toLowerAscii()
    case mode
    of htmTcpExplicitSeed, htmTcpPublicSeed:
      if "/tcp/" notin lower:
        continue
    of htmQuicExplicitSeed:
      if "/quic-v1" notin lower:
        continue
    of htmQuicDualStackFixedPort:
      if "/quic-v1" notin lower:
        continue
    return multiaddr
  ""

proc publishHostNetworkStatus(
    node: HostNode,
    networkType: string,
    transport: string,
    localIpv4 = "",
    preferredIpv4 = "",
    localIpv6 = "",
    preferredIpv6 = "",
    reason = "manual"
)

proc refreshDialState(node: var HostNode) =
  if node.handle.isNil or node.peerId.len == 0:
    return
  let dialable = safeCStringExpr(libp2p_get_dialable_addresses(node.handle))
  node.dialAddrs = candidateDialAddrsFromJson(
    dialable,
    node.peerId,
    allowLoopbackFallback = false
  )
  if node.dialAddrs.len == 0:
    node.dialAddrs = candidateDialAddrsFromJson(
      safeCStringExpr(libp2p_get_listen_addresses(node.handle)),
      node.peerId,
      allowLoopbackFallback = true
    )
  node.dialAddr = firstDialAddrFromJson(
    dialable,
    node.peerId,
    node.transportMode,
    allowLoopbackFallback = false
  )
  if node.dialAddr.len == 0:
    let listenAddrs = safeCStringExpr(libp2p_get_listen_addresses(node.handle))
    node.dialAddr = firstDialAddrFromJson(
      listenAddrs,
      node.peerId,
      node.transportMode,
      allowLoopbackFallback = true
    )

proc waitUntil*(deadlineMs: int64, intervalMs: int, condition: proc(): bool): bool =
  while nowMillis() <= deadlineMs:
    if condition():
      return true
    sleep(intervalMs)
  false

proc listenAddressesForMode(transportMode: HostTransportMode, listenPort: int): JsonNode =
  case transportMode
  of htmTcpExplicitSeed:
    %*["/ip4/127.0.0.1/tcp/0"]
  of htmTcpPublicSeed:
    let port =
      if listenPort > 0:
        listenPort
      else:
        4001
    %*[
      "/ip4/0.0.0.0/tcp/" & $port,
      "/ip6/::/tcp/" & $port
    ]
  of htmQuicExplicitSeed:
    %*["/ip4/127.0.0.1/udp/0/quic-v1"]
  of htmQuicDualStackFixedPort:
    %fullP2PListenAddresses(if listenPort > 0: listenPort else: 4001)

proc initHostNode(
    label: string,
    dataDir: string,
    transportMode: HostTransportMode,
    listenPort: int,
    config: JsonNode
): HostNode =
  let handle = libp2p_node_init(($config).cstring)
  if handle.isNil:
    raise newException(IOError, "libp2p_node_init failed for " & label)
  if not libp2p_mdns_set_enabled(handle, false):
    raise newException(IOError, "libp2p_mdns_set_enabled(false) failed for " & label)
  HostNode(
    label: label,
    handle: handle,
    dataDir: dataDir,
    transportMode: transportMode,
    listenPort: listenPort
  )

proc createNode*(label: string, transportMode = htmTcpExplicitSeed, listenPort = 0): HostNode =
  let dataDir = hostHarnessDataDir(label)
  let config = %*{
    "identity": testIdentity(),
    "dataDir": dataDir,
    "listenAddresses": listenAddressesForMode(transportMode, listenPort)
  }
  result = initHostNode(label, dataDir, transportMode, listenPort, config)

proc createWanBootstrapNode*(
    label: string,
    role = "mobile",
    networkId = "unimaker-mobile",
    transportMode = htmQuicExplicitSeed,
    listenPort = 0,
    enablePublicBootstrap = false,
    staticBootstrap: seq[string] = @[]
): HostNode =
  let dataDir = hostHarnessDataDir(label)
  var config = %*{
    "identity": stableWanBootstrapIdentity(label, role, networkId),
    "dataDir": dataDir,
    "listenAddresses": listenAddressesForMode(transportMode, listenPort),
    "extra": {
      "wanBootstrapRole": role.strip().toLowerAscii(),
      "wanBootstrapNetworkId": networkId.strip(),
      "enable_public_bootstrap": enablePublicBootstrap,
      "disable_public_bootstrap": not enablePublicBootstrap,
      "static_bootstrap": stringArray(staticBootstrap)
    }
  }
  result = initHostNode(label, dataDir, transportMode, listenPort, config)

proc stopNode*(node: var HostNode) =
  if node.handle.isNil:
    return
  discard libp2p_node_stop(node.handle)
  sleep(200)
  libp2p_node_free(node.handle)
  node.handle = nil
  sleep(120)

proc startNode*(node: var HostNode) =
  if libp2p_node_start(node.handle) != 0:
    raise newException(IOError, "libp2p_node_start failed for " & node.label)
  let handle = node.handle
  let started = waitUntil(nowMillis() + 15_000, 150, proc(): bool =
    libp2p_node_is_started(handle) and safeCStringExpr(libp2p_get_local_peer_id(handle)).len > 0
  )
  if not started:
    raise newException(IOError, "node did not start for " & node.label)
  node.peerId = safeCStringExpr(libp2p_get_local_peer_id(handle))
  refreshDialState(node)
  if node.dialAddr.len == 0:
    let transportName =
      case node.transportMode
      of htmTcpExplicitSeed: "tcp"
      of htmTcpPublicSeed: "tcp-public"
      of htmQuicExplicitSeed: "quic"
      of htmQuicDualStackFixedPort: "dual-stack quic"
    raise newException(IOError, "missing " & transportName & " dialable address for " & node.label)
  # The switch reports started slightly before the host listener is consistently
  # ready for immediate external dials. A short settle window removes flaky
  # connection-refused races in local bootstrap tests.
  sleep(1_000)

proc registerBootstrapHint*(
    node: HostNode,
    peerId: string,
    addrs: seq[string],
    source = "manual_direct_hint"
): bool =
  if node.handle.isNil or peerId.len == 0 or addrs.len == 0:
    return false
  libp2p_register_peer_hints(
    node.handle,
    peerId.cstring,
    ($stringArray(addrs)).cstring,
    source.cstring
  )

proc bootstrapStatus*(node: HostNode): JsonNode =
  if node.handle.isNil:
    return newJObject()
  safeJsonExpr(libp2p_get_bootstrap_status(node.handle))

proc joinViaBootstrap*(node: HostNode, limit = 0): JsonNode =
  if node.handle.isNil:
    return %*{
      "ok": false,
      "attemptedCount": 0,
      "attemptCount": 0,
      "connectedCount": 0,
      "attempts": [],
      "attempted": []
    }
  safeJsonExpr(libp2p_join_via_random_bootstrap(node.handle, cint(limit)))

proc reconnectBootstrap*(node: HostNode): bool =
  not node.handle.isNil and libp2p_reconnect_bootstrap(node.handle)

proc discoverySnapshot*(
    node: HostNode,
    sourceFilter = "",
    limit = 64,
    connectCap = 0
): JsonNode =
  if node.handle.isNil:
    return newJObject()
  safeJsonExpr(
    libp2p_network_discovery_snapshot(
      node.handle,
      sourceFilter.cstring,
      cint(limit),
      cint(connectCap)
    )
  )

proc firstMatchingAddr(node: HostNode, required: seq[string], allowPhysical = true): string =
  for addr in node.dialAddrs:
    let lower = addr.toLowerAscii()
    if not allowPhysical and isPhysicalPrefixedAddr(addr):
      continue
    var ok = true
    for token in required:
      if token.toLowerAscii() notin lower:
        ok = false
        break
    if ok:
      return addr
  ""

proc bestQuicAddr(node: HostNode, familyToken: string): string =
  let family = familyToken.toLowerAscii()
  for addr in node.dialAddrs:
    let lower = addr.toLowerAscii()
    if isPhysicalPrefixedAddr(addr):
      continue
    if "/quic-v1" notin lower or family notin lower:
      continue
    if family == "/ip6/" and not isConcreteIpv6Multiaddr(addr):
      continue
    if family == "/ip4/" and not isConcreteIpv4Multiaddr(addr):
      continue
    return addr
  ""

proc bestRoutableAddr(node: HostNode, familyToken: string): string =
  let token = familyToken.toLowerAscii()
  for addr in node.dialAddrs:
    let lower = addr.toLowerAscii()
    if isPhysicalPrefixedAddr(addr):
      continue
    if token notin lower:
      continue
    if token == "/ip6/" and ("/ip6/fe80:" in lower or "/ip6/::/" in lower):
      continue
    if token == "/ip4/" and "/ip4/0.0.0.0/" in lower:
      continue
    return addr
  ""

proc bestRoutableSnapshotAddr(node: HostNode, familyToken: string): string =
  if node.handle.isNil:
    return ""
  let snapshot = safeJsonExpr(
    libp2p_network_discovery_snapshot(node.handle, "".cstring, 64.cint, 0.cint)
  )
  if snapshot.kind != JObject:
    return ""
  let token = familyToken.toLowerAscii()
  for key in ["seedMultiaddrs", "dialableAddresses", "multiaddrs", "listenAddresses"]:
    if not snapshot.hasKey(key) or snapshot[key].kind != JArray:
      continue
    for item in snapshot[key]:
      if item.kind != JString:
        continue
      let maddr = item.getStr().strip()
      let lower = maddr.toLowerAscii()
      if isPhysicalPrefixedAddr(maddr):
        continue
      if token notin lower:
        continue
      if token == "/ip6/" and ("/ip6/fe80:" in lower or "/ip6/::/" in lower):
        continue
      if token == "/ip4/" and "/ip4/0.0.0.0/" in lower:
        continue
      return maddr
  ""

proc publishDetectedNetworkStatus*(
    node: var HostNode,
    networkType: string,
    transport: string,
    reason = "manual",
    preferIpv6 = false
) =
  refreshDialState(node)
  var localIpv4 = protocolValue(
    block:
      let ipv4Preferred = bestQuicAddr(node, "/ip4/")
      let ipv4Fallback = bestRoutableAddr(node, "/ip4/")
      let ipv4SnapshotFallback = bestRoutableSnapshotAddr(node, "/ip4/")
      if ipv4Preferred.len > 0:
        ipv4Preferred
      elif ipv4Fallback.len > 0:
        ipv4Fallback
      else:
        ipv4SnapshotFallback,
    "ip4"
  )
  var localIpv6 = protocolValue(
    block:
      let ipv6Preferred = bestQuicAddr(node, "/ip6/")
      let ipv6Fallback = bestRoutableAddr(node, "/ip6/")
      let ipv6SnapshotFallback = bestRoutableSnapshotAddr(node, "/ip6/")
      if ipv6Preferred.len > 0:
        ipv6Preferred
      elif ipv6Fallback.len > 0:
        ipv6Fallback
      else:
        ipv6SnapshotFallback,
    "ip6"
  )
  if localIpv6.len == 0:
    let deadlineMs = nowMillis() + 3_000
    while localIpv6.len == 0 and nowMillis() <= deadlineMs:
      let snapshotAddr = bestRoutableSnapshotAddr(node, "/ip6/")
      if snapshotAddr.len > 0:
        localIpv6 = protocolValue(snapshotAddr, "ip6")
      if localIpv6.len == 0:
        sleep(150)
  if not preferIpv6 and localIpv4.len == 0:
    let deadlineMs = nowMillis() + 3_000
    while localIpv4.len == 0 and nowMillis() <= deadlineMs:
      let snapshotAddr = bestRoutableSnapshotAddr(node, "/ip4/")
      if snapshotAddr.len > 0:
        localIpv4 = protocolValue(snapshotAddr, "ip4")
      if localIpv4.len == 0:
        sleep(150)
  if node.transportMode == htmQuicDualStackFixedPort:
    if not preferIpv6 and localIpv4.len == 0:
      raise newException(IOError, "missing dual-stack IPv4 QUIC dialable address for " & node.label)
    if localIpv6.len == 0:
      raise newException(IOError, "missing dual-stack IPv6 QUIC dialable address for " & node.label)
  publishHostNetworkStatus(
    node,
    networkType,
    transport,
    localIpv4 = (if preferIpv6: "" else: localIpv4),
    preferredIpv4 = (if preferIpv6: "" else: localIpv4),
    localIpv6 = localIpv6,
    preferredIpv6 = localIpv6,
    reason = reason
  )
  refreshDialState(node)

proc quicSeedAddrs(node: HostNode): seq[string] =
  for addr in node.dialAddrs:
    if "/quic-v1" in addr.toLowerAscii():
      result.add(addr)

proc publishHostNetworkStatus(
    node: HostNode,
    networkType: string,
    transport: string,
    localIpv4 = "",
    preferredIpv4 = "",
    localIpv6 = "",
    preferredIpv6 = "",
    reason = "manual"
) =
  let payload = %*{
    "type": "HostNetworkStatus",
    "networkType": networkType,
    "transport": transport,
    "localIpv4": localIpv4,
    "preferredIpv4": preferredIpv4,
    "localIpv6": localIpv6,
    "preferredIpv6": preferredIpv6,
    "listenPort": node.listenPort,
    "isConnected": true,
    "isMetered": transport notin ["wifi", "ethernet"],
    "reason": reason,
    "timestampMs": nowMillis()
  }
  if not libp2p_update_host_network_status(node.handle, ($payload).cstring):
    raise newException(IOError, "host network status update failed for " & node.label)

proc connectGuestToHost*(guest: HostNode, host: HostNode, dialAddr = "") =
  let targetAddr = if dialAddr.len > 0: dialAddr else: host.dialAddr
  echo "phase:connect:start transport=" &
    (case guest.transportMode
     of htmTcpExplicitSeed: "tcp"
     of htmTcpPublicSeed: "tcp-public"
     of htmQuicExplicitSeed: "quic"
     of htmQuicDualStackFixedPort: "quic-dual")
  let connectRc = libp2p_connect_multiaddr_timeout(guest.handle, targetAddr.cstring, 8_000)
  if connectRc != 0:
    raise newException(IOError, "guest connect_multiaddr failed rc=" & $connectRc)
  echo "phase:connect:connect_multiaddr_ok"
  let guestHandle = guest.handle
  let hostHandle = host.handle
  let hostPeerId = host.peerId
  let guestPeerId = guest.peerId
  let connected = waitUntil(nowMillis() + 15_000, 150, proc(): bool =
    libp2p_is_peer_connected(guestHandle, hostPeerId.cstring) and
    libp2p_is_peer_connected(hostHandle, guestPeerId.cstring)
  )
  if not connected:
    raise newException(IOError, "nodes failed to connect")
  echo "phase:connect:peer_connected"
  if not libp2p_wait_secure_channel_ffi(guestHandle, hostPeerId.cstring, 12_000):
    raise newException(IOError, "guest secure channel did not become ready")
  echo "phase:connect:guest_secure_ready"
  if not libp2p_wait_secure_channel_ffi(hostHandle, guestPeerId.cstring, 12_000):
    raise newException(IOError, "host secure channel did not become ready")
  echo "phase:connect:host_secure_ready"

proc currentState(node: HostNode, appId, roomId: string): JsonNode =
  safeJsonExpr(libp2p_game_mesh_current_state(node.handle, appId.cstring, roomId.cstring))

proc replayVerify(node: HostNode, appId, roomId: string): JsonNode =
  safeJsonExpr(libp2p_game_mesh_replay_verify(node.handle, appId.cstring, roomId.cstring))

proc pollEvents(node: HostNode): JsonNode =
  safeJsonExpr(libp2p_poll_events(node.handle, 64), "[]")

proc stateFromStep(stepPayload: JsonNode): JsonNode =
  if stepPayload.kind == JObject and stepPayload.hasKey("state") and stepPayload["state"].kind == JObject:
    return cloneJson(stepPayload["state"])
  newJObject()

proc stateReady(appId: string, payload: JsonNode): bool =
  let phase = strField(payload, "phase").toUpperAscii()
  if phase == "FINISHED":
    return true
  if payload.kind != JObject or not payload.hasKey("state"):
    return false
  if boolField(payload, "connected"):
    return true
  if intField(payload, "seq", 0) > 0:
    return true
  case appId
  of "chess":
    intField(payload, "moveCount", 0) > 0
  of "doudizhu":
    let events = payload{"events"}
    events.kind == JArray and events.len > 1
  else:
    false

proc bothFinished(hostState, guestState: JsonNode): bool =
  strField(hostState, "phase").toUpperAscii() == "FINISHED" and
    strField(guestState, "phase").toUpperAscii() == "FINISHED"

proc modeForApp(appId: string): string =
  if appId == "doudizhu": "p2p_bot_room" else: "p2p_room"

proc matchSeedForApp(appId: string): string =
  if appId == "doudizhu": "host-harness-ddz-seed" else: ""

proc waitForReady(appId, roomId: string, host, guest: HostNode): tuple[hostState, guestState: JsonNode] =
  let hostRef = host
  let guestRef = guest
  var hostState = newJObject()
  var guestState = newJObject()
  let ready = waitUntil(nowMillis() + 15_000, 150, proc(): bool =
    hostState = currentState(hostRef, appId, roomId)
    guestState = currentState(guestRef, appId, roomId)
    stateReady(appId, hostState) and stateReady(appId, guestState)
  )
  if not ready:
    raise newException(IOError, "game mesh room did not become ready for " & appId)
  result.hostState = hostState
  result.guestState = guestState

proc runLoop(appId, roomId: string, host, guest: HostNode, timeoutMs: int): tuple[hostState, guestState: JsonNode, hostLastStep, guestLastStep: JsonNode, iterations: int] =
  let deadline = nowMillis() + int64(timeoutMs)
  var lastSignature = ""
  var progressAt = nowMillis()
  result.hostState = currentState(host, appId, roomId)
  result.guestState = currentState(guest, appId, roomId)
  while nowMillis() <= deadline:
    inc(result.iterations)
    result.hostLastStep = safeJsonExpr(libp2p_game_mesh_apply_step(host.handle, appId.cstring, roomId.cstring))
    result.guestLastStep = safeJsonExpr(libp2p_game_mesh_apply_step(guest.handle, appId.cstring, roomId.cstring))
    if strField(result.hostLastStep, "error") notin ["", "script_exhausted"] and not boolField(result.hostLastStep, "ok", true):
      raise newException(IOError, "host apply_step failed: " & $result.hostLastStep)
    if strField(result.guestLastStep, "error") notin ["", "script_exhausted"] and not boolField(result.guestLastStep, "ok", true):
      raise newException(IOError, "guest apply_step failed: " & $result.guestLastStep)
    let hostStepState = stateFromStep(result.hostLastStep)
    let guestStepState = stateFromStep(result.guestLastStep)
    if hostStepState.kind == JObject and hostStepState.len > 0:
      result.hostState = hostStepState
    elif result.iterations mod 4 == 0:
      result.hostState = currentState(host, appId, roomId)
    if guestStepState.kind == JObject and guestStepState.len > 0:
      result.guestState = guestStepState
    elif result.iterations mod 4 == 0:
      result.guestState = currentState(guest, appId, roomId)
    let signature =
      strField(result.hostState, "phase") & ":" & strField(result.hostState, "stateHash") & ":" &
      $intField(result.hostState, "seq", 0) & ":" &
      strField(result.guestState, "phase") & ":" & strField(result.guestState, "stateHash") & ":" &
      $intField(result.guestState, "seq", 0)
    if signature != lastSignature:
      lastSignature = signature
      progressAt = nowMillis()
    if bothFinished(result.hostState, result.guestState):
      result.hostState = currentState(host, appId, roomId)
      result.guestState = currentState(guest, appId, roomId)
      return
    if nowMillis() - progressAt > 6_000:
      raise newException(IOError, "game mesh loop stalled for " & appId &
        " host=" & $result.hostState & " guest=" & $result.guestState)
    sleep(160)
  raise newException(IOError, "game mesh loop timed out for " & appId)

proc reachedSwitchPoint(appId: string, hostState, guestState: JsonNode): bool =
  if bothFinished(hostState, guestState):
    return false
  let samePhase = strField(hostState, "phase") == strField(guestState, "phase")
  let sameHash = strField(hostState, "stateHash") == strField(guestState, "stateHash")
  let bothConnected = boolField(hostState, "connected") and boolField(guestState, "connected")
  case appId
  of "chess":
    let hostMoves = intField(hostState, "moveCount", 0)
    let guestMoves = intField(guestState, "moveCount", 0)
    bothConnected and samePhase and sameHash and hostMoves >= 1 and guestMoves >= 1
  of "doudizhu":
    let hostSeq = intField(hostState, "seq", 0)
    let guestSeq = intField(guestState, "seq", 0)
    # Mid-match doudizhu snapshots expose seat-local information and the bot
    # can advance the host one phase ahead of the guest. Once both sides are
    # connected and the guest has materialized the room while the host has
    # progressed past the opening bids, the switch path is safe to exercise.
    bothConnected and hostSeq >= 2 and guestSeq >= 1
  else:
    bothConnected and samePhase and sameHash and
      stateReady(appId, hostState) and stateReady(appId, guestState)

proc advanceUntilSwitchPoint(
    appId, roomId: string,
    host, guest: HostNode,
    timeoutMs: int
): tuple[hostState, guestState: JsonNode, hostLastStep, guestLastStep: JsonNode, iterations: int] =
  if appId == "chess":
    let deadline = nowMillis() + int64(timeoutMs)
    var lastSignature = ""
    var progressAt = nowMillis()
    result.hostState = currentState(host, appId, roomId)
    result.guestState = currentState(guest, appId, roomId)
    while nowMillis() <= deadline:
      inc(result.iterations)
      result.hostLastStep = safeJsonExpr(libp2p_game_mesh_apply_step(host.handle, appId.cstring, roomId.cstring))
      if strField(result.hostLastStep, "error") notin ["", "script_exhausted"] and
          not boolField(result.hostLastStep, "ok", true):
        raise newException(IOError, "host apply_step failed: " & $result.hostLastStep)
      let hostStepState = stateFromStep(result.hostLastStep)
      if hostStepState.kind == JObject and hostStepState.len > 0:
        result.hostState = hostStepState
      else:
        result.hostState = currentState(host, appId, roomId)

      # Pre-switch we only ingest on the guest side; otherwise the guest will
      # immediately auto-play its reply and we lose a stable mid-match checkpoint.
      result.guestLastStep = newJObject()
      result.guestState = currentState(guest, appId, roomId)

      if reachedSwitchPoint(appId, result.hostState, result.guestState):
        return
      let signature =
        strField(result.hostState, "phase") & ":" & strField(result.hostState, "stateHash") & ":" &
        $intField(result.hostState, "seq", 0) & ":" &
        strField(result.guestState, "phase") & ":" & strField(result.guestState, "stateHash") & ":" &
        $intField(result.guestState, "seq", 0)
      if signature != lastSignature:
        lastSignature = signature
        progressAt = nowMillis()
      if bothFinished(result.hostState, result.guestState):
        raise newException(IOError, "game mesh match finished before switch point for " & appId)
      if nowMillis() - progressAt > 6_000:
        raise newException(IOError, "game mesh pre-switch loop stalled for " & appId &
          " host=" & $result.hostState & " guest=" & $result.guestState)
      sleep(160)
    raise newException(IOError, "game mesh pre-switch loop timed out for " & appId)

  let deadline = nowMillis() + int64(timeoutMs)
  var lastSignature = ""
  var progressAt = nowMillis()
  result.hostState = currentState(host, appId, roomId)
  result.guestState = currentState(guest, appId, roomId)
  while nowMillis() <= deadline:
    inc(result.iterations)
    result.hostLastStep = safeJsonExpr(libp2p_game_mesh_apply_step(host.handle, appId.cstring, roomId.cstring))
    result.guestLastStep = safeJsonExpr(libp2p_game_mesh_apply_step(guest.handle, appId.cstring, roomId.cstring))
    if strField(result.hostLastStep, "error") notin ["", "script_exhausted"] and not boolField(result.hostLastStep, "ok", true):
      raise newException(IOError, "host apply_step failed: " & $result.hostLastStep)
    if strField(result.guestLastStep, "error") notin ["", "script_exhausted"] and not boolField(result.guestLastStep, "ok", true):
      raise newException(IOError, "guest apply_step failed: " & $result.guestLastStep)
    let hostStepState = stateFromStep(result.hostLastStep)
    let guestStepState = stateFromStep(result.guestLastStep)
    if hostStepState.kind == JObject and hostStepState.len > 0:
      result.hostState = hostStepState
    elif result.iterations mod 2 == 0:
      result.hostState = currentState(host, appId, roomId)
    if guestStepState.kind == JObject and guestStepState.len > 0:
      result.guestState = guestStepState
    elif result.iterations mod 2 == 0:
      result.guestState = currentState(guest, appId, roomId)
    if reachedSwitchPoint(appId, result.hostState, result.guestState):
      return
    let signature =
      strField(result.hostState, "phase") & ":" & strField(result.hostState, "stateHash") & ":" &
      $intField(result.hostState, "seq", 0) & ":" &
      strField(result.guestState, "phase") & ":" & strField(result.guestState, "stateHash") & ":" &
      $intField(result.guestState, "seq", 0)
    if signature != lastSignature:
      lastSignature = signature
      progressAt = nowMillis()
    if bothFinished(result.hostState, result.guestState):
      raise newException(IOError, "game mesh match finished before switch point for " & appId)
    if nowMillis() - progressAt > 6_000:
      raise newException(IOError, "game mesh pre-switch loop stalled for " & appId &
        " host=" & $result.hostState & " guest=" & $result.guestState)
    sleep(160)
  raise newException(IOError, "game mesh pre-switch loop timed out for " & appId)

proc diagnostics(node: HostNode): JsonNode =
  safeJsonExpr(libp2p_get_diagnostics_json(node.handle))

proc connectedPeersInfo(node: HostNode): JsonNode =
  safeJsonExpr(libp2p_connected_peers_info(node.handle), "[]")

proc writeMatchSummary(outDir, appId: string, summary: JsonNode) =
  if outDir.len == 0:
    return
  createDir(outDir)
  writeFile(outDir / (appId & ".summary.json"), pretty(summary))

proc applyLanToMobileSwitch(host, guest: var HostNode): string =
  # This is a host-side mobility simulation: we refresh advertised addresses,
  # force a disconnect, then re-establish over IPv6 QUIC. True QUIC connection
  # migration must be validated on real devices where the CID survives an actual
  # Wi-Fi <-> cellular path change without this explicit reconnect.
  publishDetectedNetworkStatus(host, "cellular", "5g", reason = "properties_changed", preferIpv6 = true)
  publishDetectedNetworkStatus(guest, "cellular", "5g", reason = "properties_changed", preferIpv6 = true)
  discard libp2p_disconnect_peer(host.handle, guest.peerId.cstring)
  discard libp2p_disconnect_peer(guest.handle, host.peerId.cstring)
  let hostHandle = host.handle
  let guestHandle = guest.handle
  let hostPeerId = host.peerId
  let guestPeerId = guest.peerId
  let disconnected = waitUntil(nowMillis() + 10_000, 150, proc(): bool =
    not libp2p_is_peer_connected(hostHandle, guestPeerId.cstring) and
    not libp2p_is_peer_connected(guestHandle, hostPeerId.cstring)
  )
  if not disconnected:
    raise newException(IOError, "peer disconnect did not complete before switch reconnect")
  discard pollEvents(host)
  discard pollEvents(guest)
  sleep(500)
  refreshDialState(host)
  refreshDialState(guest)
  let hostIpv6Quic = bestQuicAddr(host, "/ip6/")
  if hostIpv6Quic.len == 0:
    raise newException(IOError, "missing IPv6 QUIC dial address for host switch")
  connectGuestToHost(guest, host, dialAddr = hostIpv6Quic)
  result = hostIpv6Quic

proc runLocalGameMeshMatch*(
    appId: string,
    timeoutMs = 45_000,
    outDir = "",
    cleanup = true,
    transportMode = htmTcpExplicitSeed,
    switchMidMatch = false
): JsonNode =
  let fixedPortBase =
    if transportMode == htmQuicDualStackFixedPort:
      4001
    else:
      0
  var host = createNode(appId & "-host", transportMode, listenPort = fixedPortBase)
  var guest = createNode(
    appId & "-guest",
    transportMode,
    listenPort = if fixedPortBase > 0: fixedPortBase + 1 else: 0
  )
  var summary = newJObject()
  var initialConnectAddr = ""
  var switchConnectAddr = ""
  try:
    startNode(host)
    startNode(guest)
    if transportMode == htmQuicDualStackFixedPort:
      publishDetectedNetworkStatus(host, "wifi", "wifi", reason = "init")
      publishDetectedNetworkStatus(guest, "wifi", "wifi", reason = "init")
      initialConnectAddr = bestQuicAddr(host, "/ip6/")
      if initialConnectAddr.len == 0:
        raise newException(IOError, "missing IPv6 QUIC dial address for host initial connect")
      connectGuestToHost(guest, host, dialAddr = initialConnectAddr)
    else:
      connectGuestToHost(guest, host)
      initialConnectAddr = host.dialAddr

    let conversationId = "dm:" & guest.peerId
    let guestSeeds =
      if transportMode == htmQuicDualStackFixedPort:
        quicSeedAddrs(guest)
      else:
        @[guest.dialAddr]
    let createPayload = safeJsonExpr(
      libp2p_game_mesh_create_room(
        host.handle,
        appId.cstring,
        guest.peerId.cstring,
        conversationId.cstring,
        ($stringArray(guestSeeds)).cstring,
        matchSeedForApp(appId).cstring,
        false
      )
    )
    if not boolField(createPayload, "ok", false):
      raise newException(IOError, "create_room failed: " & $createPayload)
    let roomId = strField(createPayload, "roomId")
    let matchId = strField(createPayload, "matchId")
    let seedPayload = %*{
      "remoteSeedAddrs":
        if transportMode == htmQuicDualStackFixedPort:
          quicSeedAddrs(host)
        else:
          @[host.dialAddr],
      "localSeedAddrs": guestSeeds
    }
    let joinPayload = safeJsonExpr(
      libp2p_game_mesh_join_room(
        guest.handle,
        appId.cstring,
        roomId.cstring,
        host.peerId.cstring,
        conversationId.cstring,
        ($seedPayload).cstring,
        matchId.cstring,
        modeForApp(appId).cstring
      )
    )
    if not boolField(joinPayload, "ok", false):
      raise newException(IOError, "join_room failed: " & $joinPayload)

    var readyStates = waitForReady(appId, roomId, host, guest)
    if switchMidMatch:
      discard advanceUntilSwitchPoint(appId, roomId, host, guest, max(timeoutMs div 3, 10_000))
      switchConnectAddr = applyLanToMobileSwitch(host, guest)
    var loopResult = runLoop(appId, roomId, host, guest, timeoutMs)
    let hostReplay = replayVerify(host, appId, roomId)
    let guestReplay = replayVerify(guest, appId, roomId)

    summary = %*{
      "ok": bothFinished(loopResult.hostState, loopResult.guestState) and
        boolField(hostReplay, "ok", false) and boolField(guestReplay, "ok", false) and
        strField(loopResult.hostState, "stateHash") == strField(loopResult.guestState, "stateHash"),
      "appId": appId,
      "roomId": roomId,
      "matchId": matchId,
      "conversationId": conversationId,
      "transport":
        case transportMode
        of htmTcpExplicitSeed: "tcp_explicit_seed"
        of htmTcpPublicSeed: "tcp_public_seed"
        of htmQuicExplicitSeed: "quic_explicit_seed"
        of htmQuicDualStackFixedPort:
          if switchMidMatch: "quic_dual_stack_switch_simulated" else: "quic_dual_stack_fixed_port",
      "switchVerified": switchMidMatch,
      "mobilityValidation": {
        "mode": (if switchMidMatch: "simulated_disconnect_reconnect" else: "none"),
        "realConnectionMigration": false
      },
      "addressSelection": {
        "initialConnectAddr": initialConnectAddr,
        "switchConnectAddr": switchConnectAddr
      },
      "host": {
        "peerId": host.peerId,
        "dialAddr": host.dialAddr,
        "dialAddrs": host.dialAddrs,
        "readyState": readyStates.hostState,
        "finalState": loopResult.hostState,
        "lastStep": loopResult.hostLastStep,
        "replay": hostReplay
      },
      "guest": {
        "peerId": guest.peerId,
        "dialAddr": guest.dialAddr,
        "dialAddrs": guest.dialAddrs,
        "readyState": readyStates.guestState,
        "finalState": loopResult.guestState,
        "lastStep": loopResult.guestLastStep,
        "replay": guestReplay
      },
      "iterations": loopResult.iterations
    }
    writeMatchSummary(outDir, appId, summary)
  except CatchableError as exc:
    summary = %*{
      "ok": false,
      "appId": appId,
      "error": exc.msg,
      "host": {
        "peerId": host.peerId,
        "dialAddr": host.dialAddr,
        "dialAddrs": host.dialAddrs
      },
      "guest": {
        "peerId": guest.peerId,
        "dialAddr": guest.dialAddr,
        "dialAddrs": guest.dialAddrs
      }
    }
    writeMatchSummary(outDir, appId, summary)
    raise
  finally:
    if cleanup:
      stopNode(host)
      stopNode(guest)
  summary

proc runLocalGameMeshSuite*(
    apps: seq[string],
    timeoutMs = 45_000,
    outDir = "",
    transportMode = htmTcpExplicitSeed,
    switchMidMatch = false
): JsonNode =
  result = %*{
    "ok": true,
    "results": newJArray(),
    "timestampMs": nowMillis()
  }
  for index, appId in apps:
    let matchOutDir =
      if outDir.len > 0:
        outDir / appId
      else:
        ""
    # The standalone host harness exits immediately after the final run, so
    # skipping teardown there avoids unrelated shutdown races masking game results.
    let cleanup =
      if apps.len == 1:
        false
      else:
        not (appId == "doudizhu" and index == apps.high)
    try:
      let summary = runLocalGameMeshMatch(
        appId,
        timeoutMs = timeoutMs,
        outDir = matchOutDir,
        cleanup = cleanup,
        transportMode = transportMode,
        switchMidMatch = switchMidMatch
      )
      if not boolField(summary, "ok", false):
        result["ok"] = %false
      result["results"].add(summary)
    except CatchableError as exc:
      result["ok"] = %false
      result["results"].add(%*{
        "ok": false,
        "appId": appId,
        "error": exc.msg
      })
  if outDir.len > 0:
    createDir(outDir)
    writeFile(outDir / "summary.json", pretty(result))

when isMainModule:
  var apps: seq[string] = @["chess", "doudizhu"]
  var outDir = getCurrentDir() / "build" / "game-mesh-host" / $nowMillis()
  var timeoutMs = 45_000
  var transportMode = htmTcpExplicitSeed
  var switchMidMatch = false

  var parser = initOptParser(commandLineParams())
  for kind, key, value in parser.getopt():
    case kind
    of cmdLongOption, cmdShortOption:
      case key
      of "app":
        let lowered = value.strip().toLowerAscii()
        if lowered == "all" or lowered.len == 0:
          apps = @["chess", "doudizhu"]
        else:
          apps = lowered.split(',').mapIt(it.strip()).filterIt(it.len > 0)
      of "out", "out-dir":
        if value.strip().len > 0:
          outDir = value.strip()
      of "timeout-ms":
        if value.strip().len > 0:
          timeoutMs = max(parseInt(value.strip()), 5_000)
      of "transport":
        case value.strip().toLowerAscii()
        of "", "tcp":
          transportMode = htmTcpExplicitSeed
        of "quic":
          transportMode = htmQuicExplicitSeed
        of "quic-dual", "dual":
          transportMode = htmQuicDualStackFixedPort
        else:
          raise newException(ValueError, "unknown transport: " & value)
      of "network-switch":
        let lowered = value.strip().toLowerAscii()
        switchMidMatch = lowered in ["1", "true", "yes", "on", "lan-mobile", "lan_to_mobile"]
      else:
        discard
    else:
      discard

  let summary = runLocalGameMeshSuite(
    apps,
    timeoutMs = timeoutMs,
    outDir = outDir,
    transportMode = transportMode,
    switchMidMatch = switchMidMatch
  )
  echo pretty(summary)
  stdout.flushFile()
  let exitCode = if boolField(summary, "ok", false): 0.cint else: 1.cint
  when defined(posix):
    hardExit(exitCode)
  else:
    quit(int(exitCode))
