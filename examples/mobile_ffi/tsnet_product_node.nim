{.used.}

{.emit: """
void nim_bridge_emit_event(const char *topic, const char *payload) {
  (void)topic;
  (void)payload;
}
""".}

import std/[base64, json, net, options, os, posix, sets, strutils, syncio]
from std/nativesockets import selectRead
from std/times import epochTime
import chronos
import ../../libp2p/crypto/crypto
import ../../libp2p/peerid
import ../../libp2p/transports/tsnet/quicrelay
import ./libnimlibp2p

type
  Ifaddrs {.importc: "struct ifaddrs", header: "<ifaddrs.h>", bycopy.} = object
    ifa_next: ptr Ifaddrs
    ifa_name: cstring
    ifa_flags: cuint
    ifa_addr: ptr SockAddr
    ifa_netmask: ptr SockAddr
    ifa_dstaddr: ptr SockAddr
    ifa_data: pointer

  NodeOptions = object
    label: string
    rpcHost: string
    rpcPort: int
    controlUrl: string
    controlProtocol: string
    controlEndpoint: string
    relayEndpoint: string
    authKey: string
    hostname: string
    stateDir: string
    dataDir: string
    identityPath: string
    bridgeLibraryPath: string
    wireguardPort: int
    listenPort: int
    logLevel: string
    enableDebug: bool
    quicOnly: bool
    waitTimeoutSec: int

  ProductNode = ref object
    handle: pointer
    opts: NodeOptions
    stopRequested: bool

  PendingKind = enum
    pendingWaitStarted
    pendingWaitTailnetReady
    pendingWaitPeerReady
    pendingConnectExact
    pendingSendDm
    pendingWaitDm

  WatchClient = ref object
    socket: Socket
    deadlineMs: int64
    maxEvents: int

  PendingRequest = ref object
    socket: Socket
    deadlineMs: int64
    kind: PendingKind
    requestKey: string
    peerId: string
    bodyContains: string

const
  ProductProtocolVersion = 1
  MaxFrameBytes = 16 * 1024 * 1024

var productRpcNonce: uint64 = 0

when not declared(getifaddrs):
  proc getifaddrs(ifap: ptr ptr Ifaddrs): cint {.importc, header: "<ifaddrs.h>".}
when not declared(freeifaddrs):
  proc freeifaddrs(ifa: ptr Ifaddrs) {.importc, header: "<ifaddrs.h>".}

proc nowMillis(): int64 {.gcsafe.} =
  int64(epochTime() * 1000)

proc normalizeIpv6Text(ip: string): string =
  let trimmed = ip.strip()
  if trimmed.len == 0:
    return ""
  let zoneIdx = trimmed.find('%')
  if zoneIdx >= 0:
    return trimmed[0 ..< zoneIdx].strip()
  trimmed

proc isGlobalIpv6Text(ip: string): bool =
  let normalized = normalizeIpv6Text(ip)
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

proc isVpnInterfaceName(ifName: string): bool =
  let lower = ifName.strip().toLowerAscii()
  lower.startsWith("utun") or
    lower.startsWith("tun") or
    lower.startsWith("tap") or
    lower.startsWith("tailscale")

proc interfacePriority(ifName: string): int =
  let lower = ifName.strip().toLowerAscii()
  if lower.startsWith("en") or lower.startsWith("eth") or lower.startsWith("wlan") or lower.startsWith("wl"):
    return 0
  if lower.startsWith("rmnet") or lower.startsWith("wwan") or lower.startsWith("pdp_ip"):
    return 1
  2

proc detectBestGlobalIpv6(): string =
  var bestScore = high(int)
  var bestIp = ""
  when declared(getifaddrs):
    var ifap: ptr Ifaddrs = nil
    if getifaddrs(addr ifap) == 0:
      defer:
        freeifaddrs(ifap)
      var cursor = ifap
      while cursor != nil:
        let ifName =
          if cursor.ifa_name.isNil:
            ""
          else:
            $cursor.ifa_name
        when declared(cursor.ifa_flags):
          when declared(IFF_LOOPBACK):
            if (cursor.ifa_flags and IFF_LOOPBACK) != 0:
              cursor = cursor.ifa_next
              continue
        if ifName.len == 0 or isVpnInterfaceName(ifName):
          cursor = cursor.ifa_next
          continue
        let addrPtr = cursor.ifa_addr
        when declared(posix.AF_INET6):
          if addrPtr != nil and cint(addrPtr.sa_family) == posix.AF_INET6:
            let sin6 = cast[ptr Sockaddr_in6](addrPtr)
            var buffer: array[64, char]
            when declared(inet_ntop):
              if inet_ntop(posix.AF_INET6, addr sin6.sin6_addr, cast[ptr char](addr buffer[0]), buffer.len.cint) != nil:
                let ip = normalizeIpv6Text($cast[cstring](addr buffer[0]))
                if isGlobalIpv6Text(ip):
                  let score = interfacePriority(ifName)
                  if score < bestScore:
                    bestScore = score
                    bestIp = ip
        cursor = cursor.ifa_next
  bestIp

proc nextProductRpcNonce(): uint64 =
  inc productRpcNonce
  productRpcNonce

proc ensureProductMessageId(messageId: string): string =
  let trimmed = messageId.strip()
  if trimmed.len > 0:
    return trimmed
  "dm-" & $nowMillis() & "-" & $nextProductRpcNonce()

proc makeProductRequestKey(prefix, messageId: string): string =
  prefix & ":" & messageId

proc fail(msg: string) {.noreturn.} =
  raise newException(IOError, msg)

proc takeCStringAndFree(value: cstring): string {.gcsafe.} =
  if value.isNil:
    return ""
  result = $value
  libp2p_string_free(value)

proc safeCString(value: cstring): string {.gcsafe, raises: [].} =
  try:
    takeCStringAndFree(value)
  except CatchableError:
    ""

proc parseJsonFallback(fallback: string): JsonNode {.gcsafe, raises: [].} =
  try:
    parseJson(fallback)
  except CatchableError:
    if fallback == "[]":
      newJArray()
    elif fallback == "null":
      newJNull()
    else:
      newJObject()

proc safeJson(value: cstring; fallback = "{}"): JsonNode {.gcsafe, raises: [].} =
  try:
    let text = takeCStringAndFree(value)
    if text.len == 0:
      parseJsonFallback(fallback)
    else:
      parseJson(text)
  except CatchableError:
    parseJsonFallback(fallback)

proc encodeFrame(payload: string): string {.gcsafe.} =
  result = newStringOfCap(4 + payload.len)
  result.add(char((payload.len shr 24) and 0xFF))
  result.add(char((payload.len shr 16) and 0xFF))
  result.add(char((payload.len shr 8) and 0xFF))
  result.add(char(payload.len and 0xFF))
  result.add(payload)

proc decodeFrameLength(header: string): int {.gcsafe.} =
  if header.len != 4:
    return -1
  (ord(header[0]) shl 24) or
    (ord(header[1]) shl 16) or
    (ord(header[2]) shl 8) or
    ord(header[3])

proc strField(node: JsonNode; key: string; defaultValue = ""): string {.gcsafe, raises: [].} =
  try:
    if node.kind == JObject and node.hasKey(key) and node[key].kind == JString:
      return node[key].getStr()
  except CatchableError:
    discard
  defaultValue

proc boolField(node: JsonNode; key: string; defaultValue = false): bool {.gcsafe, raises: [].} =
  try:
    if node.kind == JObject and node.hasKey(key) and node[key].kind == JBool:
      return node[key].getBool()
  except CatchableError:
    discard
  defaultValue

proc intField(node: JsonNode; key: string; defaultValue = 0): int {.gcsafe, raises: [].} =
  try:
    if node.kind == JObject and node.hasKey(key) and node[key].kind == JInt:
      return node[key].getInt()
  except CatchableError:
    discard
  defaultValue

proc generatedIdentity(): JsonNode =
  let rng = newRng()
  let pair = KeyPair.random(PKScheme.Ed25519, rng[]).get()
  let privateKey = pair.seckey.getRawBytes().get()
  let publicKey = pair.pubkey.getRawBytes().get()
  let peerId = $PeerId.init(pair.seckey).get()
  %*{
    "privateKey": base64.encode(privateKey),
    "publicKey": base64.encode(publicKey),
    "peerId": peerId,
    "source": "tsnet_product_node"
  }

proc validIdentity(node: JsonNode): bool =
  node.kind == JObject and
    strField(node, "peerId").len > 0 and
    strField(node, "privateKey").len > 0 and
    strField(node, "publicKey").len > 0

proc stableIdentity(identityPath: string): JsonNode =
  if identityPath.len > 0 and fileExists(identityPath):
    try:
      let loaded = parseJson(readFile(identityPath))
      if validIdentity(loaded):
        return loaded
    except CatchableError:
      discard
  result = generatedIdentity()
  if identityPath.len > 0:
    createDir(parentDir(identityPath))
    writeFile(identityPath, $result)

proc withPeerIdSuffix(listenAddrs: JsonNode; peerId: string): JsonNode {.gcsafe.} =
  if listenAddrs.kind != JArray or peerId.len == 0:
    return listenAddrs
  result = newJArray()
  for item in listenAddrs.items:
    if item.kind != JString:
      continue
    var maText = item.getStr().strip()
    if maText.len == 0:
      continue
    let lower = maText.toLowerAscii()
    if "/p2p/" notin lower and "/ipfs/" notin lower:
      maText.add("/p2p/" & peerId)
    result.add(%maText)

proc filterConnectableListenAddrs(listenAddrs: JsonNode): JsonNode {.gcsafe.} =
  if listenAddrs.kind != JArray:
    return listenAddrs
  result = newJArray()
  var seen = initHashSet[string]()
  for item in listenAddrs.items:
    if item.kind != JString:
      continue
    let maText = item.getStr().strip()
    if maText.len == 0:
      continue
    let lower = maText.toLowerAscii()
    if lower.startsWith("/awdl/") or lower.startsWith("/nan/") or lower.startsWith("/nearlink/"):
      continue
    if maText in seen:
      continue
    seen.incl(maText)
    result.add(%maText)

proc discoveryListenAddrs(node: ProductNode): JsonNode =
  if node.isNil or node.handle.isNil:
    return newJArray()
  let snapshot =
    safeJson(libp2p_network_discovery_snapshot(node.handle, nil, 0.cint, 0.cint), "{}")
  if snapshot.kind != JObject:
    return newJArray()
  filterConnectableListenAddrs(snapshot.getOrDefault("listenAddresses"))

proc relayPublishedListenAddrs(tailnetPayload: JsonNode): JsonNode {.gcsafe.} =
  if tailnetPayload.isNil or tailnetPayload.kind != JObject:
    return newJArray()
  let listeners = tailnetPayload.getOrDefault("relayListeners")
  if listeners.isNil or listeners.kind != JArray:
    return newJArray()
  result = newJArray()
  var seen = initHashSet[string]()
  for item in listeners.items:
    if item.isNil or item.kind != JObject:
      continue
    let route = item.getOrDefault("route")
    let stage = item.getOrDefault("stage")
    if route.isNil or stage.isNil or route.kind != JString or stage.kind != JString:
      continue
    let routeText = route.getStr().strip()
    let stageText = stage.getStr().strip().toLowerAscii()
    if routeText.len == 0:
      continue
    if stageText in ["failed", "dropped", "stopped"]:
      continue
    if routeText in seen:
      continue
    seen.incl(routeText)
    result.add(%routeText)

proc synthesizedTsnetListenAddrs(
    node: ProductNode;
    peerId: string;
    tailnetPayload: JsonNode
): JsonNode {.gcsafe.} =
  result = newJArray()
  if node.isNil or peerId.len == 0 or node.opts.listenPort <= 0:
    return
  if tailnetPayload.kind != JObject or not boolField(tailnetPayload, "ok"):
    return
  if not boolField(tailnetPayload, "providerReady") or
      not boolField(tailnetPayload, "proxyListenersReady"):
    return
  let ipRows = tailnetPayload.getOrDefault("tailnetIPs")
  if ipRows.kind != JArray:
    return
  var seen = initHashSet[string]()
  for item in ipRows.items:
    if item.kind != JString:
      continue
    let ipText = item.getStr().strip()
    if ipText.len == 0:
      continue
    let prefix =
      if ':' in ipText:
        "/ip6/"
      else:
        "/ip4/"
    let addrText =
      prefix & ipText & "/udp/" & $node.opts.listenPort &
      "/quic-v1/tsnet/p2p/" & peerId
    if addrText in seen:
      continue
    seen.incl(addrText)
    result.add(%addrText)

proc normalizeExactDialAddrs(peerId: string; listenAddrs: JsonNode): seq[string] {.gcsafe.} =
  if listenAddrs.kind != JArray:
    return @[]
  var prepared: seq[string] = @[]
  for item in listenAddrs.items:
    if item.kind != JString:
      continue
    var maText = item.getStr().strip()
    if maText.len == 0:
      continue
    let lower = maText.toLowerAscii()
    if lower.startsWith("/awdl/") or lower.startsWith("/nan/") or lower.startsWith("/nearlink/"):
      continue
    if "/p2p/" notin lower and "/ipfs/" notin lower and peerId.len > 0:
      maText.add("/p2p/" & peerId)
    prepared.add(maText)
  if prepared.len == 0:
    return @[]

  var quicTsnet: seq[string] = @[]
  var tcpTsnet: seq[string] = @[]
  var other: seq[string] = @[]
  for maText in prepared:
    let lower = maText.toLowerAscii()
    if "/tsnet" in lower and "/udp/" in lower and "/quic-v1" in lower:
      quicTsnet.add(maText)
    elif "/tsnet" in lower and "/tcp/" in lower:
      tcpTsnet.add(maText)
    else:
      other.add(maText)

  let selected =
    if quicTsnet.len > 0:
      quicTsnet
    elif tcpTsnet.len > 0:
      tcpTsnet
    else:
      other

  var seen = initHashSet[string]()
  for maText in selected:
    if seen.contains(maText):
      continue
    seen.incl(maText)
    result.add(maText)

proc defaultListenAddrs(opts: NodeOptions): JsonNode =
  let tcpPort = if opts.listenPort > 0: opts.listenPort else: 0
  result = newJArray()
  if not opts.quicOnly:
    result.add(%("/ip4/0.0.0.0/tcp/" & $tcpPort & "/tsnet"))
    result.add(%("/ip6/::/tcp/" & $tcpPort & "/tsnet"))
  when defined(libp2p_msquic_experimental):
    let udpPort = if opts.listenPort > 0: opts.listenPort else: 0
    result.add(%("/ip4/0.0.0.0/udp/" & $udpPort & "/quic-v1/tsnet"))
    result.add(%("/ip6/::/udp/" & $udpPort & "/quic-v1/tsnet"))
  elif opts.quicOnly:
    fail("quicOnly requires libp2p_msquic_experimental")

proc effectiveControlUrl(opts: NodeOptions): string =
  let explicit = opts.controlUrl.strip()
  if explicit.len > 0:
    return explicit
  opts.controlEndpoint.strip()

proc buildConfig(opts: NodeOptions): JsonNode =
  %*{
    "identity": stableIdentity(opts.identityPath),
    "dataDir": opts.dataDir,
    "listenAddresses": defaultListenAddrs(opts),
    "transportPolicy": (if opts.quicOnly: "quic_only" else: "quic_preferred"),
    "automations": {
      "gossipsub": false,
      "autonat": false,
      "circuitRelay": false
    },
    "extra": {
      "underlay": "tsnet",
      "quicRuntimePreference": "builtin_only",
      "disableMdns": true,
      "disable_mdns": true,
      "disableWanBootstrap": true,
      "disable_wan_bootstrap": true,
      "enable_public_bootstrap": false,
      "disable_public_bootstrap": true,
      "disableNodeTelemetryPubsub": true,
      "disableDefaultTsnetListen": false,
      "transportPolicy": (if opts.quicOnly: "quic_only" else: "quic_preferred"),
      "tsnet": {
        "controlUrl": effectiveControlUrl(opts),
        "controlProtocol": opts.controlProtocol,
        "controlEndpoint": opts.controlEndpoint,
        "relayEndpoint": opts.relayEndpoint,
        "authKey": opts.authKey,
        "hostname": opts.hostname,
        "stateDir": opts.stateDir,
        "wireguardPort": opts.wireguardPort,
        "bridgeLibraryPath": opts.bridgeLibraryPath,
        "logLevel": opts.logLevel,
        "enableDebug": opts.enableDebug,
        "disableDefaultListen": false
      }
    }
  }

proc parseOptions(): NodeOptions =
  result = NodeOptions(
    label: "tsnet-product-node",
    rpcHost: "127.0.0.1",
    rpcPort: 19101,
    controlProtocol: "nim_quic",
    wireguardPort: 41641,
    logLevel: "debug",
    enableDebug: true,
    quicOnly: true,
    waitTimeoutSec: 60
  )
  for arg in commandLineParams():
    if not arg.startsWith("--"):
      continue
    let body = arg[2 .. ^1]
    let parts = body.split("=", 1)
    let rawKey = parts[0].strip().toLowerAscii()
    let key = rawKey.replace("-", "").replace("_", "")
    let val = if parts.len > 1: parts[1] else: ""
    case key
    of "label":
      result.label = val.strip()
    of "rpchost":
      result.rpcHost = val.strip()
    of "rpcport":
      result.rpcPort = max(1, parseInt(val))
    of "controlurl":
      result.controlUrl = val.strip()
    of "controlprotocol":
      result.controlProtocol = val.strip().toLowerAscii()
    of "controlendpoint":
      result.controlEndpoint = val.strip()
    of "relayendpoint":
      result.relayEndpoint = val.strip()
    of "authkey":
      result.authKey = val
    of "hostname":
      result.hostname = val.strip()
    of "statedir":
      result.stateDir = val.strip()
    of "datadir":
      result.dataDir = val.strip()
    of "identitypath":
      result.identityPath = val.strip()
    of "bridgelibrarypath":
      result.bridgeLibraryPath = val.strip()
    of "wireguardport":
      result.wireguardPort = max(0, parseInt(val))
    of "listenport":
      result.listenPort = max(0, parseInt(val))
    of "loglevel":
      result.logLevel = val.strip()
    of "enabledebug":
      result.enableDebug = val.strip().toLowerAscii() notin ["0", "false", "no"]
    of "quiconly":
      result.quicOnly = val.strip().toLowerAscii() notin ["0", "false", "no"]
    of "waittimeoutsec":
      result.waitTimeoutSec = max(1, parseInt(val))
    else:
      discard
  if result.hostname.len == 0:
    result.hostname = result.label
  if result.stateDir.len == 0:
    result.stateDir = getCurrentDir() / "build" / result.label / "state"
  if result.dataDir.len == 0:
    result.dataDir = getCurrentDir() / "build" / result.label / "data"
  if result.identityPath.len == 0:
    result.identityPath = result.stateDir / "libp2p-identity.json"

proc recvExact(client: Socket, size: int; timeoutMs: int): string =
  result = ""
  while result.len < size:
    let chunk = client.recv(size - result.len, timeout = timeoutMs)
    if chunk.len == 0:
      raise newException(IOError, "socket closed while reading")
    result.add(chunk)

proc recvFrame(client: Socket; timeoutMs: int): Option[string] =
  var header = ""
  while header.len < 4:
    let chunk = client.recv(4 - header.len, timeout = timeoutMs)
    if chunk.len == 0:
      if header.len == 0:
        return none(string)
      raise newException(IOError, "socket closed while reading frame header")
    header.add(chunk)
  let payloadLen = decodeFrameLength(header)
  if payloadLen < 0 or payloadLen > MaxFrameBytes:
    raise newException(IOError, "invalid request frame length")
  if payloadLen == 0:
    return some("")
  some(recvExact(client, payloadLen, timeoutMs))

proc sendExact(client: Socket, payload: string) =
  if payload.len == 0:
    return
  var written = 0
  while written < payload.len:
    let sent = client.send(unsafeAddr payload[written], payload.len - written)
    if sent <= 0:
      raise newException(IOError, "socket closed while writing")
    written += sent

proc startStatus(node: ProductNode): JsonNode {.raises: [].} =
  safeJson(libp2p_get_start_status_json(node.handle))

proc tailnetStartStatus(node: ProductNode): JsonNode {.raises: [].} =
  safeJson(libp2p_get_tailnet_start_status_json(node.handle))

proc tailnetStatus(node: ProductNode): JsonNode {.raises: [].} =
  safeJson(libp2p_tailnet_status_json(node.handle), """{"ok":false}""")

proc tailnetStatusText(node: ProductNode): string {.raises: [].} =
  let text = safeCString(libp2p_tailnet_status_json(node.handle))
  if text.len > 0:
    text
  else:
    """{"ok":false}"""

proc tailnetDerpMap(node: ProductNode): JsonNode {.raises: [].} =
  safeJson(libp2p_tailnet_derp_map(node.handle), "null")

proc connectedPeersInfo(node: ProductNode): JsonNode {.raises: [].} =
  try:
    safeJson(libp2p_connected_peers_info(node.handle), "[]")
  except Exception:
    newJArray()

proc discoverySnapshot(node: ProductNode): JsonNode {.raises: [].} =
  try:
    safeJson(libp2p_network_discovery_snapshot(node.handle, nil, 0.cint, 0.cint), "{}")
  except Exception:
    newJObject()

proc relayProbeUrl(node: ProductNode; request: JsonNode): string {.raises: [].} =
  let explicitRelayUrl = strField(request, "relayUrl").strip()
  if explicitRelayUrl.len > 0:
    return explicitRelayUrl
  let tailnetPayload = node.tailnetStatus()
  for candidate in [
    strField(tailnetPayload, "controlEndpoint").strip(),
    strField(tailnetPayload, "controlUrl").strip(),
    node.opts.relayEndpoint.strip(),
    node.opts.controlEndpoint.strip(),
    node.opts.controlUrl.strip(),
  ]:
    if candidate.len == 0:
      continue
    let lowered = candidate.toLowerAscii()
    if lowered.startsWith("quic://"):
      return candidate
  ""

proc relayPingPayload(node: ProductNode; request: JsonNode): JsonNode {.raises: [].} =
  let relayUrl = relayProbeUrl(node, request)
  if relayUrl.len == 0:
    return %*{
      "ok": false,
      "target": "relay",
      "error": "relay_url_unavailable"
    }
  let sampleCount = max(1, intField(request, "sampleCount", max(1, intField(request, "samples", 5))))
  let timeoutMs = max(250, intField(request, "timeoutMs", 3_000))
  let startedAtMs = nowMillis()
  var successCount = 0
  var minLatencyMs = high(int64)
  var maxLatencyMs = 0'i64
  var latencyTotalMs = 0'i64
  var firstError = ""
  var runtimePreference = ""
  var runtimeImplementation = ""
  var runtimeKind = ""
  var runtimePath = ""
  var samples = newJArray()
  for sequence in 1 .. sampleCount:
    let sampleStartedAtMs = nowMillis()
    var probe = TsnetQuicRelayProbeResult(
      ok: false,
      relayUrl: relayUrl,
      route: "",
      runtimePreference: "",
      runtimeImplementation: "",
      runtimePath: "",
      runtimeKind: "",
      connected: false,
      acknowledged: false,
      error: ""
    )
    try:
      probe = waitFor probeRelayPing(
        relayUrl,
        timeout = chronos.milliseconds(timeoutMs)
      )
    except CatchableError as exc:
      probe.error = exc.msg
    let latencyMs = max(nowMillis() - sampleStartedAtMs, 0)
    if runtimePreference.len == 0:
      runtimePreference = probe.runtimePreference
    if runtimeImplementation.len == 0:
      runtimeImplementation = probe.runtimeImplementation
    if runtimeKind.len == 0:
      runtimeKind = probe.runtimeKind
    if runtimePath.len == 0:
      runtimePath = probe.runtimePath
    if probe.ok:
      inc successCount
      minLatencyMs = min(minLatencyMs, latencyMs)
      maxLatencyMs = max(maxLatencyMs, latencyMs)
      latencyTotalMs += latencyMs
    elif firstError.len == 0:
      firstError =
        if probe.error.len > 0:
          probe.error
        else:
          "relay_ping_failed"
    var sample = %*{
      "sequence": sequence,
      "latencyMs": latencyMs,
      "ok": probe.ok,
      "connected": probe.connected,
      "acknowledged": probe.acknowledged,
    }
    if probe.runtimePreference.len > 0:
      sample["runtimePreference"] = %probe.runtimePreference
    if probe.runtimeImplementation.len > 0:
      sample["runtimeImplementation"] = %probe.runtimeImplementation
    if probe.runtimeKind.len > 0:
      sample["runtimeKind"] = %probe.runtimeKind
    if probe.runtimePath.len > 0:
      sample["runtimePath"] = %probe.runtimePath
    if probe.error.len > 0:
      sample["error"] = %probe.error
    samples.add(sample)
  let ok = successCount > 0
  %*{
    "ok": ok,
    "target": "relay",
    "relayUrl": relayUrl,
    "tailnetRelay": strField(node.tailnetStatus(), "tailnetRelay"),
    "sampleCount": sampleCount,
    "successCount": successCount,
    "minLatencyMs": (if ok: minLatencyMs else: 0),
    "maxLatencyMs": (if ok: maxLatencyMs else: 0),
    "avgLatencyMs": (if ok: latencyTotalMs div int64(successCount) else: 0),
    "elapsedMs": max(nowMillis() - startedAtMs, 0),
    "timeoutMs": timeoutMs,
    "runtimePreference": runtimePreference,
    "runtimeImplementation": runtimeImplementation,
    "runtimeKind": runtimeKind,
    "runtimePath": runtimePath,
    "samples": samples,
    "error": (if ok: "" else: firstError),
  }

proc tailnetPing(node: ProductNode; request: JsonNode): JsonNode {.raises: [].} =
  let target =
    strField(request, "target", strField(request, "mode", strField(request, "pingTarget"))).
      strip().
      toLowerAscii()
  if target == "relay":
    return relayPingPayload(node, request)
  safeJson(libp2p_tailnet_ping(node.handle, ($request).cstring), """{"ok":false}""")

proc pollEvents(node: ProductNode; maxEvents: int): JsonNode {.raises: [].} =
  try:
    safeJson(libp2p_poll_events(node.handle, cint(max(1, maxEvents))), "[]")
  except CatchableError:
    newJArray()

proc initNode(opts: NodeOptions): ProductNode =
  createDir(opts.stateDir)
  createDir(opts.dataDir)
  let handle = libp2p_node_init(($buildConfig(opts)).cstring)
  if handle.isNil:
    fail("libp2p_node_init failed")
  discard libp2p_mdns_set_enabled(handle, false)
  if not libp2p_node_start_kickoff(handle):
    libp2p_node_free(handle)
    fail("libp2p_node_start_kickoff failed")
  let globalIpv6 = detectBestGlobalIpv6()
  var initialHostNetworkStatus = %*{
    "networkType": "other",
    "transport": "other",
    "listenPort": opts.listenPort,
    "isConnected": true,
    "isMetered": false,
    "timestampMs": nowMillis(),
    "reason": "start"
  }
  if globalIpv6.len > 0:
    initialHostNetworkStatus["localIpv6"] = %globalIpv6
    initialHostNetworkStatus["preferredIpv6"] = %globalIpv6
    initialHostNetworkStatus["publicIpv6"] = %globalIpv6
  discard libp2p_update_host_network_status(handle, ($initialHostNetworkStatus).cstring)
  stderr.writeLine("[tsnet-product-node] node start queued")
  ProductNode(handle: handle, opts: opts, stopRequested: false)

proc stopNode(node: ProductNode) =
  if node.isNil or node.handle.isNil:
    return
  discard libp2p_node_stop(node.handle)
  libp2p_node_free(node.handle)
  node.handle = nil

proc localInfo(node: ProductNode): JsonNode {.raises: [].} =
  let startPayload = node.startStatus()
  let tailnetStartPayload = node.tailnetStartStatus()
  let tailnetPayload = node.tailnetStatus()
  let peerId = safeCString(libp2p_get_local_peer_id(node.handle))
  let relayListenAddrs =
    withPeerIdSuffix(
      filterConnectableListenAddrs(relayPublishedListenAddrs(tailnetPayload)),
      peerId
    )
  var listenAddrs = relayListenAddrs
  var listenAddrSource = "relay_listeners"
  if listenAddrs.kind != JArray or listenAddrs.len == 0:
    listenAddrs =
      withPeerIdSuffix(
        filterConnectableListenAddrs(
          safeJson(libp2p_get_runtime_listen_addresses(node.handle), "[]")
        ),
        peerId
      )
    listenAddrSource = "runtime"
  if listenAddrs.kind != JArray or listenAddrs.len == 0:
    listenAddrs =
      withPeerIdSuffix(
        filterConnectableListenAddrs(
          safeJson(libp2p_get_listen_addresses(node.handle), "[]")
        ),
        peerId
      )
    listenAddrSource = "published"
  if listenAddrs.kind != JArray or listenAddrs.len == 0:
    listenAddrs = synthesizedTsnetListenAddrs(node, peerId, tailnetPayload)
    listenAddrSource = "synthesized"
  %*{
    "label": node.opts.label,
    "hostname": node.opts.hostname,
    "peerId": peerId,
    "listenAddrs": listenAddrs,
    "listenAddrSource": listenAddrSource,
    "startStatus": startPayload,
    "tailnetStartStatus": tailnetStartPayload,
    "updatedAtMs": nowMillis()
  }

proc fullStatus(node: ProductNode): JsonNode {.raises: [].} =
  var result = node.localInfo()
  result["tailnetStatus"] = node.tailnetStatus()
  result["derpMap"] = node.tailnetDerpMap()
  result

proc startedSnapshot(node: ProductNode): JsonNode {.raises: [].} =
  let startPayload = node.startStatus()
  %*{
    "ok": boolField(startPayload, "started") and strField(startPayload, "stage") != "failed",
    "startStatus": startPayload
  }

proc tailnetReadySnapshot(node: ProductNode): JsonNode {.raises: [].} =
  let tailnetStartPayload = node.tailnetStartStatus()
  let tailnetPayload = node.tailnetStatus()
  let peerId = safeCString(libp2p_get_local_peer_id(node.handle))
  %*{
    "ok": strField(tailnetStartPayload, "stage") != "failed" and
      boolField(tailnetPayload, "ok") and
      boolField(tailnetPayload, "providerReady") and
      boolField(tailnetPayload, "proxyListenersReady"),
    "tailnetStartStatus": tailnetStartPayload,
    "tailnetStatus": tailnetPayload,
    "listenAddrs": newJArray(),
    "peerId": %peerId
  }

proc connectExactKickoff(
    node: ProductNode;
    peerId: string;
    listenAddrs: JsonNode;
    timeoutMs: int
): JsonNode {.raises: [].}

proc connectExactStatus(peerId: string): JsonNode {.raises: [].}

proc peerReadySnapshot(node: ProductNode; peerId: string; sliceTimeoutMs: int): JsonNode {.raises: [].} =
  if peerId.len == 0:
    return %*{"ok": false, "error": "peer_id_missing"}
  let safeSliceMs = max(1, min(sliceTimeoutMs, 250))
  let readyOk =
    try:
      libp2p_wait_peer_ready_ffi(node.handle, peerId.cstring, cint(safeSliceMs))
    except CatchableError:
      false
  %*{
    "ok": readyOk,
    "peerId": peerId,
    "secureOk": readyOk,
    "identifiedOk": readyOk,
    "secureBudgetMs": safeSliceMs,
    "identifyBudgetMs": safeSliceMs,
    "error": (if readyOk: "" else: "peer_not_ready")
  }

proc connectExact(node: ProductNode; peerId: string; listenAddrs: JsonNode; timeoutMs: int): JsonNode {.raises: [].} =
  if peerId.len == 0:
    return %*{"ok": false, "error": "peer_id_missing"}
  if listenAddrs.kind != JArray or listenAddrs.len == 0:
    return %*{"ok": false, "error": "listen_addrs_missing"}
  let kickoffPayload = connectExactKickoff(node, peerId, listenAddrs, timeoutMs)
  if not boolField(kickoffPayload, "ok"):
    return kickoffPayload
  let startedAtMs = nowMillis()
  let deadlineMs = startedAtMs + int64(max(1_000, timeoutMs))
  while nowMillis() <= deadlineMs:
    let kickoffState = connectExactStatus(peerId)
    let status = strField(kickoffState, "status")
    if status == "done":
      var payload =
        if kickoffState.kind == JObject and kickoffState.hasKey("payload"):
          kickoffState.getOrDefault("payload")
        else:
          %*{"ok": false, "peerId": peerId, "error": "connect_exact_missing_payload"}
      if boolField(payload, "ok"):
        if not payload.hasKey("secureOk"):
          let elapsedMs = int(max(0'i64, nowMillis() - startedAtMs))
          let remainingMs = max(1_000, timeoutMs - elapsedMs)
          let readyPayload = peerReadySnapshot(node, peerId, remainingMs)
          payload["secureOk"] = readyPayload.getOrDefault("secureOk")
          payload["identifiedOk"] = readyPayload.getOrDefault("identifiedOk")
        elif not payload.hasKey("identifiedOk"):
          payload["identifiedOk"] = %false
      return payload
    sleep(100)
  %*{
    "ok": false,
    "peerId": peerId,
    "error": "connect_exact_timeout",
    "state": connectExactStatus(peerId)
  }

proc connectExactKickoff(
    node: ProductNode;
    peerId: string;
    listenAddrs: JsonNode;
    timeoutMs: int
): JsonNode {.raises: [].} =
  if peerId.len == 0:
    return %*{"ok": false, "error": "peer_id_missing"}
  if listenAddrs.kind != JArray or listenAddrs.len == 0:
    return %*{"ok": false, "error": "listen_addrs_missing"}
  let normalizedAddrs = normalizeExactDialAddrs(peerId, listenAddrs)
  if normalizedAddrs.len == 0:
    return %*{"ok": false, "error": "no_supported_exact_addrs"}
  let addressesJson = $(%normalizedAddrs)
  let started =
    try:
      libp2p_bootstrap_seed_connect_exact_kickoff(
        node.handle,
        peerId.cstring,
        addressesJson.cstring,
        "product_connect_exact".cstring,
        cint(normalizedAddrs.len),
        cint(max(250, min(timeoutMs, 10_000)))
      )
    except Exception:
      false
  if not started:
    let err = safeCString(libp2p_get_last_error())
    return %*{
      "ok": false,
      "error": (if err.len > 0: err else: "connect_exact_kickoff_failed"),
      "peerId": peerId
    }
  %*{
    "ok": true,
    "status": "queued",
    "peerId": peerId,
    "listenAddrs": normalizedAddrs,
    "timeoutMs": max(250, min(timeoutMs, 10_000))
  }

proc connectExactStatus(peerId: string): JsonNode {.raises: [].} =
  if peerId.len == 0:
    return %*{"ok": false, "error": "peer_id_missing"}
  try:
    safeJson(
      libp2p_bootstrap_seed_connect_exact_kickoff_status(peerId.cstring),
      """{"ok":false,"peerId":"","status":"missing"}"""
    )
  except CatchableError as exc:
    %*{"ok": false, "peerId": peerId, "status": "error", "error": exc.msg}

proc relaySessionStatus(node: ProductNode; peerId: string): JsonNode {.raises: [].} =
  let safePeerId = peerId.strip()
  if safePeerId.len == 0:
    return %*{"ok": false, "status": "missing", "error": "peer_id_missing"}
  try:
    safeJson(
      libp2p_relay_session_status(node.handle, safePeerId.cstring),
      """{"ok":false,"status":"missing","error":"relay_session_status_failed"}"""
    )
  except CatchableError as exc:
    %*{"ok": false, "peerId": safePeerId, "status": "error", "error": exc.msg}

proc relaySessionPrepare(
    node: ProductNode;
    peerId: string;
    listenAddrs: JsonNode;
    timeoutMs: int
): JsonNode {.raises: [].} =
  let safePeerId = peerId.strip()
  if safePeerId.len == 0:
    return %*{"ok": false, "error": "peer_id_missing"}
  let normalizedAddrs = normalizeExactDialAddrs(safePeerId, listenAddrs)
  if listenAddrs.kind == JArray and listenAddrs.len > 0 and normalizedAddrs.len == 0:
    return %*{
      "ok": false,
      "peerId": safePeerId,
      "error": "no_supported_exact_addrs"
    }
  if normalizedAddrs.len > 0:
    let hintsOk =
      try:
        libp2p_register_peer_hints(
          node.handle,
          safePeerId.cstring,
          ($(%normalizedAddrs)).cstring,
          "product_relay_session_prepare".cstring
        )
      except Exception:
        false
    if not hintsOk:
      return %*{
        "ok": false,
        "peerId": safePeerId,
        "error": "register_peer_hints_failed",
        "listenAddrs": normalizedAddrs,
      }
  let prepareOk =
    try:
      libp2p_prepare_relay_session(
        node.handle,
        safePeerId.cstring,
        "product_relay_session_prepare".cstring
      )
    except Exception:
      false
  if not prepareOk:
    return %*{
      "ok": false,
      "peerId": safePeerId,
      "error": safeCString(libp2p_get_last_direct_error(node.handle)),
      "listenAddrs": normalizedAddrs,
    }
  let startedAtMs = nowMillis()
  let deadlineAtMs = startedAtMs + int64(max(1_000, timeoutMs))
  var lastSnapshot = relaySessionStatus(node, safePeerId)
  while true:
    let status = strField(lastSnapshot, "status").toLowerAscii()
    let sessionConnKey = intField(lastSnapshot, "sessionConnKey", 0)
    if status == "ready" and sessionConnKey > 0:
      lastSnapshot["prepareLatencyMs"] = %(nowMillis() - startedAtMs)
      lastSnapshot["listenAddrs"] = %normalizedAddrs
      return lastSnapshot
    if status in ["failed", "cleared"] or (status == "missing" and nowMillis() >= deadlineAtMs):
      lastSnapshot["prepareLatencyMs"] = %(nowMillis() - startedAtMs)
      lastSnapshot["listenAddrs"] = %normalizedAddrs
      return lastSnapshot
    if nowMillis() >= deadlineAtMs:
      lastSnapshot["ok"] = %false
      lastSnapshot["error"] = %"relay_session_prepare_timeout"
      lastSnapshot["prepareLatencyMs"] = %(nowMillis() - startedAtMs)
      lastSnapshot["listenAddrs"] = %normalizedAddrs
      return lastSnapshot
    sleep(50)
    lastSnapshot = relaySessionStatus(node, safePeerId)

proc sendDmKickoff(
    node: ProductNode;
    peerId, listenAddrsJson, messageId, text, replyTo: string;
    requestAck: bool;
    timeoutMs: int;
    relayOnly: bool;
    preferredSessionConnKey: int
): JsonNode {.raises: [].} =
  let safePeerId = peerId.strip()
  let safeText = text
  if safePeerId.len == 0:
    return %*{"ok": false, "error": "peer_id_missing"}
  if safeText.len == 0:
    return %*{"ok": false, "error": "text_missing", "peerId": safePeerId}
  let mid = ensureProductMessageId(messageId)
  let requestKey = makeProductRequestKey("send_dm", mid)
  var envelope = newJObject()
  envelope["op"] = %"text"
  envelope["mid"] = %mid
  envelope["messageId"] = %mid
  envelope["body"] = %safeText
  envelope["ackRequested"] = %requestAck
  envelope["timestamp_ms"] = %nowMillis()
  if replyTo.len > 0:
    envelope["reply_to"] = %replyTo
  let addressCount =
    if listenAddrsJson.len == 0:
      0
    else:
      try:
        let rows = parseJson(listenAddrsJson)
        if rows.kind == JArray: rows.len else: 0
      except CatchableError:
        0
  let kickoffOk =
    try:
      libp2p_send_with_ack_seeded_kickoff(
        node.handle,
        requestKey.cstring,
        safePeerId.cstring,
        ($envelope).cstring,
        (if listenAddrsJson.len > 0: listenAddrsJson.cstring else: "[]".cstring),
        "product_send_dm_async".cstring,
        cint(timeoutMs),
        clonglong(preferredSessionConnKey),
        relayOnly
      )
    except CatchableError:
      false
  if not kickoffOk:
    return %*{
      "ok": false,
      "peerId": safePeerId,
      "messageId": mid,
      "requestKey": requestKey,
      "error": safeCString(libp2p_get_last_direct_error(node.handle))
    }
  %*{
    "ok": true,
    "status": "queued",
    "peerId": safePeerId,
    "messageId": mid,
    "requestKey": requestKey,
    "requestAck": requestAck,
    "relayOnly": relayOnly,
    "sessionConnKey": preferredSessionConnKey,
    "addressCount": addressCount,
    "timeoutMs": timeoutMs
  }

proc sendDmStatus(requestKey: string): JsonNode {.raises: [].} =
  if requestKey.strip().len == 0:
    return %*{"ok": false, "status": "missing", "error": "request_key_missing"}
  try:
    safeJson(
      libp2p_send_with_ack_kickoff_status(requestKey.cstring),
      """{"ok":false,"requestKey":"","status":"missing"}"""
    )
  except CatchableError as exc:
    %*{
      "ok": false,
      "requestKey": requestKey,
      "status": "error",
      "error": exc.msg
    }

proc requestListenAddrsNode(request: JsonNode): JsonNode {.gcsafe, raises: [].} =
  if request.isNil or request.kind != JObject or not request.hasKey("listenAddrs"):
    return newJNull()
  let value = request.getOrDefault("listenAddrs")
  if value.isNil:
    return newJNull()
  value

proc dmRows(node: ProductNode): JsonNode {.raises: [].} =
  safeJson(social_received_direct_messages(node.handle, 128), """{"items":[]}""")

proc waitDm(
    node: ProductNode;
    fromPeerId, bodyContains: string;
    timeoutMs: int
): JsonNode {.raises: [].} =
  var rows = newJObject()
  let deadlineMs = nowMillis() + int64(timeoutMs)
  var ok = false
  while nowMillis() <= deadlineMs:
    rows = dmRows(node)
    let items =
      if rows.kind == JObject: rows.getOrDefault("items")
      else: newJNull()
    if items.kind != JArray:
      sleep(250)
      continue
    for item in items.items:
      let fromPeer = strField(item, "peerId", strField(item, "from"))
      let body = strField(item, "body", strField(item, "content"))
      if (fromPeerId.len == 0 or fromPeer == fromPeerId) and
          (bodyContains.len == 0 or body.contains(bodyContains)):
        ok = true
        break
    if ok:
      break
    sleep(250)
  %*{
    "ok": ok,
    "rows": rows
  }

proc dmSnapshot(node: ProductNode; fromPeerId, bodyContains: string): JsonNode {.raises: [].} =
  let rows = dmRows(node)
  var ok = false
  let items =
    if rows.kind == JObject: rows.getOrDefault("items")
    else: newJNull()
  if items.kind == JArray:
    for item in items.items:
      let fromPeer = strField(item, "peerId", strField(item, "from"))
      let body = strField(item, "body", strField(item, "content"))
      if (fromPeerId.len == 0 or fromPeer == fromPeerId) and
          (bodyContains.len == 0 or body.contains(bodyContains)):
        ok = true
        break
  %*{
    "ok": ok,
    "rows": rows
  }

proc rpcResponse(ok: bool; payload: JsonNode): JsonNode =
  %*{
    "version": ProductProtocolVersion,
    "ok": ok,
    "payload": payload
  }

proc rpcResponseText(ok: bool; payloadText: string): string =
  let trimmedPayload = payloadText.strip()
  let safePayload =
    if trimmedPayload.len > 0:
      trimmedPayload
    else:
      "{}"
  "{\"version\":" & $ProductProtocolVersion &
    ",\"ok\":" & (if ok: "true" else: "false") &
    ",\"payload\":" & safePayload & "}"

proc rpcEvent(kind: string; payload: JsonNode): JsonNode =
  %*{
    "version": ProductProtocolVersion,
    "ok": true,
    "kind": kind,
    "payload": payload
  }

proc sendFrame(client: Socket; payload: string)
proc closeSocketQuiet(client: Socket)

proc evaluatePending(node: ProductNode; pending: PendingRequest): tuple[done: bool, response: JsonNode] {.raises: [].} =
  let timedOut = nowMillis() > pending.deadlineMs
  case pending.kind
  of pendingWaitStarted:
    let payload = startedSnapshot(node)
    let stage = strField(payload.getOrDefault("startStatus"), "stage")
    if boolField(payload, "ok") or stage == "failed" or timedOut:
      var finalPayload = payload
      if timedOut and not boolField(finalPayload, "ok"):
        finalPayload["error"] = %"start_timeout"
      return (true, rpcResponse(true, finalPayload))
  of pendingWaitTailnetReady:
    let payload = tailnetReadySnapshot(node)
    let stage = strField(payload.getOrDefault("tailnetStartStatus"), "stage")
    if boolField(payload, "ok") or stage == "failed" or timedOut:
      var finalPayload = payload
      if timedOut and not boolField(finalPayload, "ok"):
        finalPayload["error"] = %"tailnet_ready_timeout"
      return (true, rpcResponse(true, finalPayload))
  of pendingWaitPeerReady:
    let payload = peerReadySnapshot(node, pending.peerId, 50)
    if boolField(payload, "ok") or timedOut:
      var finalPayload = payload
      if timedOut and not boolField(finalPayload, "ok"):
        finalPayload["error"] = %"peer_ready_timeout"
      return (true, rpcResponse(true, finalPayload))
  of pendingConnectExact:
    let kickoffState = connectExactStatus(pending.peerId)
    let status = strField(kickoffState, "status")
    if status == "done":
      var payload =
        if kickoffState.kind == JObject and kickoffState.hasKey("payload"):
          kickoffState.getOrDefault("payload")
        else:
          %*{"ok": false, "peerId": pending.peerId, "error": "connect_exact_missing_payload"}
      if boolField(payload, "ok") and not boolField(payload, "identifiedOk"):
        let remainingMs = max(250, int(pending.deadlineMs - nowMillis()))
        let readyPayload = peerReadySnapshot(node, pending.peerId, remainingMs)
        payload["secureOk"] = readyPayload.getOrDefault("secureOk")
        payload["identifiedOk"] = readyPayload.getOrDefault("identifiedOk")
        if not boolField(readyPayload, "ok"):
          payload["ok"] = %false
          payload["error"] = %"peer_identify_not_ready"
      return (true, rpcResponse(true, payload))
    if timedOut:
      let payload =
        %*{
          "ok": false,
          "peerId": pending.peerId,
          "error": "connect_exact_timeout",
          "state": kickoffState
        }
      return (true, rpcResponse(true, payload))
  of pendingSendDm:
    let kickoffState = sendDmStatus(pending.requestKey)
    let status = strField(kickoffState, "status")
    if status == "done":
      let payload =
        if kickoffState.kind == JObject and kickoffState.hasKey("payload"):
          kickoffState.getOrDefault("payload")
        else:
          %*{
            "ok": false,
            "requestKey": pending.requestKey,
            "peerId": pending.peerId,
            "error": "send_dm_missing_payload"
          }
      return (true, rpcResponse(true, payload))
    if status == "missing" or timedOut:
      let payload =
        if timedOut:
          %*{
            "ok": false,
            "requestKey": pending.requestKey,
            "peerId": pending.peerId,
            "error": "send_dm_timeout",
            "state": kickoffState
          }
        else:
          %*{
            "ok": false,
            "requestKey": pending.requestKey,
            "peerId": pending.peerId,
            "error": "send_dm_missing_state",
            "state": kickoffState
          }
      return (true, rpcResponse(true, payload))
  of pendingWaitDm:
    let payload = dmSnapshot(node, pending.peerId, pending.bodyContains)
    if boolField(payload, "ok") or timedOut:
      var finalPayload = payload
      if timedOut and not boolField(finalPayload, "ok"):
        finalPayload["error"] = %"wait_dm_timeout"
      return (true, rpcResponse(true, finalPayload))
  (false, newJNull())

proc registerPendingRequest(
    node: ProductNode;
    client: Socket;
    request: JsonNode;
    pendingRequests: var seq[PendingRequest]
): bool =
  let op = strField(request, "op").toLowerAscii()
  let timeoutMs = max(1_000, intField(request, "timeoutMs", 30_000))
  let requestListenAddrs = requestListenAddrsNode(request)
  var pendingKind: PendingKind
  var requestKey = ""
  case op
  of "wait_started":
    pendingKind = pendingWaitStarted
  of "wait_tailnet_ready":
    pendingKind = pendingWaitTailnetReady
  of "wait_peer_ready":
    pendingKind = pendingWaitPeerReady
  of "wait_dm":
    pendingKind = pendingWaitDm
  of "send_dm":
    let kickoffPayload = sendDmKickoff(
      node,
      strField(request, "peerId"),
      if requestListenAddrs.kind != JNull:
        $requestListenAddrs
      else:
        "",
      strField(request, "messageId"),
      strField(request, "text"),
      strField(request, "replyTo"),
      boolField(request, "requestAck", true),
      timeoutMs,
      boolField(request, "relayOnly", false),
      max(0, intField(request, "sessionConnKey", 0))
    )
    if not boolField(kickoffPayload, "ok"):
      sendFrame(client, $rpcResponse(true, kickoffPayload))
      closeSocketQuiet(client)
      return true
    pendingKind = pendingSendDm
    requestKey = strField(kickoffPayload, "requestKey")
  of "connect_exact":
    let kickoffPayload = connectExactKickoff(
      node,
      strField(request, "peerId"),
      if requestListenAddrs.kind != JNull: requestListenAddrs else: newJArray(),
      timeoutMs
    )
    if not boolField(kickoffPayload, "ok"):
      sendFrame(client, $rpcResponse(true, kickoffPayload))
      closeSocketQuiet(client)
      return true
    pendingKind = pendingConnectExact
  else:
    return false

  let pending = PendingRequest(
    socket: client,
    deadlineMs: nowMillis() + int64(timeoutMs),
    kind: pendingKind,
    requestKey: requestKey,
    peerId: strField(request, "peerId"),
    bodyContains: strField(request, "bodyContains")
  )
  let evaluation = evaluatePending(node, pending)
  if evaluation.done:
    sendFrame(client, $evaluation.response)
    closeSocketQuiet(client)
    return true
  pendingRequests.add(pending)
  true

proc handleRequest(
    node: ProductNode,
    request: JsonNode
): JsonNode {.raises: [].} =
  let op = strField(request, "op").toLowerAscii()
  let requestListenAddrs = requestListenAddrsNode(request)
  if op == "ping":
    return rpcResponse(true, %*{"pong": true, "label": node.opts.label})
  if op == "status":
    return rpcResponse(true, fullStatus(node))
  if op == "wait_started":
    return rpcResponse(true, startedSnapshot(node))
  if op == "wait_tailnet_ready":
    return rpcResponse(true, tailnetReadySnapshot(node))
  if op == "wait_peer_ready":
    return rpcResponse(
      true,
      peerReadySnapshot(
        node,
        strField(request, "peerId"),
        50
      )
    )
  if op == "local_info":
    return rpcResponse(true, localInfo(node))
  if op == "connect_exact":
    return rpcResponse(
      true,
      connectExact(
        node,
        strField(request, "peerId"),
        if requestListenAddrs.kind != JNull:
          requestListenAddrs
        else:
          newJArray(),
        max(1_000, intField(request, "timeoutMs", 30_000))
      )
    )
  if op == "connect_exact_kickoff":
    return rpcResponse(
      true,
      connectExactKickoff(
        node,
        strField(request, "peerId"),
        if requestListenAddrs.kind != JNull:
          requestListenAddrs
        else:
          newJArray(),
        max(1_000, intField(request, "timeoutMs", 30_000))
      )
    )
  if op == "connect_exact_status":
    return rpcResponse(true, connectExactStatus(strField(request, "peerId")))
  if op == "send_dm_kickoff":
    return rpcResponse(
      true,
      sendDmKickoff(
        node,
        strField(request, "peerId"),
        if requestListenAddrs.kind != JNull:
          $requestListenAddrs
        else:
          "",
        strField(request, "messageId"),
        strField(request, "text"),
        strField(request, "replyTo"),
        boolField(request, "requestAck", true),
        max(1_000, intField(request, "timeoutMs", 30_000)),
        boolField(request, "relayOnly", false),
        max(0, intField(request, "sessionConnKey", 0))
      )
    )
  if op == "send_dm_status":
    return rpcResponse(true, sendDmStatus(strField(request, "requestKey")))
  if op == "send_dm":
    return rpcResponse(false, %*{"error": "send_dm_requires_pending_registration"})
  if op == "dm_rows":
    return rpcResponse(true, dmRows(node))
  if op == "wait_dm":
    return rpcResponse(
      true,
      dmSnapshot(
        node,
        strField(request, "fromPeerId"),
        strField(request, "bodyContains")
      )
    )
  if op == "poll_events":
    return rpcResponse(true, %*{
      "events": node.pollEvents(max(1, intField(request, "maxEvents", 64)))
    })
  if op == "tailnet_ping":
    return rpcResponse(true, node.tailnetPing(request))
  if op == "tailnet_status":
    return rpcResponse(true, node.tailnetStatus())
  if op == "relay_session_prepare":
    return rpcResponse(
      true,
      relaySessionPrepare(
        node,
        strField(request, "peerId"),
        requestListenAddrs,
        max(1_000, intField(request, "timeoutMs", 30_000))
      )
    )
  if op == "relay_session_status":
    return rpcResponse(true, relaySessionStatus(node, strField(request, "peerId")))
  if op == "connected_peers_info":
    return rpcResponse(true, node.connectedPeersInfo())
  if op == "discovery_snapshot":
    return rpcResponse(true, node.discoverySnapshot())
  if op == "stop":
    node.stopRequested = true
    return rpcResponse(true, %*{"stopping": true})
  rpcResponse(false, %*{"error": "unsupported_op", "op": op})

proc sendFrame(client: Socket; payload: string) =
  sendExact(client, encodeFrame(payload))

proc closeSocketQuiet(client: Socket) =
  if client.isNil:
    return
  try:
    client.close()
  except CatchableError:
    discard

proc registerWatchClient(
    node: ProductNode;
    client: Socket;
    request: JsonNode;
    watchers: var seq[WatchClient]
) =
  let timeoutMs = max(1_000, intField(request, "timeoutMs", 300_000))
  let maxEvents = max(1, intField(request, "maxEvents", 64))
  let includeSnapshot = boolField(request, "includeSnapshot", true)
  sendFrame(client, $rpcResponse(true, %*{
    "stream": "events",
    "accepted": true,
    "timeoutMs": timeoutMs,
    "maxEvents": maxEvents
  }))
  if includeSnapshot:
    sendFrame(client, $rpcEvent("snapshot", %*{
      "label": node.opts.label,
      "startStatus": node.startStatus(),
      "tailnetStartStatus": node.tailnetStartStatus(),
      "tailnetStatus": node.tailnetStatus(),
      "localInfo": node.localInfo(),
      "updatedAtMs": nowMillis()
    }))
  watchers.add(WatchClient(
    socket: client,
    deadlineMs: nowMillis() + int64(timeoutMs),
    maxEvents: maxEvents
  ))

proc flushWatchClients(node: ProductNode; watchers: var seq[WatchClient]) =
  var idx = 0
  while idx < watchers.len:
    let watcher = watchers[idx]
    if node.stopRequested or nowMillis() > watcher.deadlineMs:
      try:
        sendFrame(watcher.socket, $rpcEvent("eof", %*{
          "reason": (if node.stopRequested: "node_stopping" else: "timeout"),
          "updatedAtMs": nowMillis()
        }))
      except CatchableError:
        discard
      closeSocketQuiet(watcher.socket)
      watchers.delete(idx)
      continue
    inc idx

  if watchers.len == 0:
    return

  var maxBatch = 1
  for watcher in watchers:
    maxBatch = max(maxBatch, watcher.maxEvents)
  let events = node.pollEvents(maxBatch)
  if events.kind != JArray or events.len == 0:
    return

  idx = 0
  while idx < watchers.len:
    let watcher = watchers[idx]
    var failed = false
    try:
      for entry in events.items:
        sendFrame(watcher.socket, $rpcEvent("event", entry))
    except CatchableError:
      failed = true
    if failed:
      closeSocketQuiet(watcher.socket)
      watchers.delete(idx)
    else:
      inc idx

proc flushPendingRequests(node: ProductNode; pendingRequests: var seq[PendingRequest]) =
  var idx = 0
  while idx < pendingRequests.len:
    let pending = pendingRequests[idx]
    var remove = false
    try:
      let evaluation = evaluatePending(node, pending)
      if evaluation.done:
        sendFrame(pending.socket, $evaluation.response)
        closeSocketQuiet(pending.socket)
        remove = true
    except CatchableError:
      closeSocketQuiet(pending.socket)
      remove = true
    if remove:
      pendingRequests.delete(idx)
    else:
      inc idx

proc serve(node: ProductNode) =
  var server = newSocket()
  var watchers: seq[WatchClient] = @[]
  var pendingRequests: seq[PendingRequest] = @[]
  try:
    server.setSockOpt(OptReuseAddr, true)
    server.bindAddr(Port(node.opts.rpcPort), node.opts.rpcHost)
    server.listen()
    stderr.writeLine("[tsnet-product-node] listening " & node.opts.rpcHost & ":" & $node.opts.rpcPort)
    while not node.stopRequested:
      flushWatchClients(node, watchers)
      flushPendingRequests(node, pendingRequests)
      var fds = @[server.getFd()]
      let ready =
        try:
          selectRead(fds, 250)
        except CatchableError:
          0
      if ready <= 0:
        continue

      var client: Socket = nil
      var clientAddr = ""
      try:
        client = newSocket()
        server.acceptAddr(client, clientAddr)
        let payloadOpt = recvFrame(client, 1_000)
        if payloadOpt.isNone():
          closeSocketQuiet(client)
          continue
        let payload = payloadOpt.get()
        let request =
          if payload.len == 0:
            newJObject()
          else:
            try:
              parseJson(payload)
            except CatchableError as exc:
              sendFrame(client, $(rpcResponse(false, %*{"error": "invalid_json", "detail": exc.msg})))
              closeSocketQuiet(client)
              continue
        if strField(request, "op").toLowerAscii() == "tailnet_status":
          sendFrame(client, rpcResponseText(true, tailnetStatusText(node)))
          closeSocketQuiet(client)
          continue
        if registerPendingRequest(node, client, request, pendingRequests):
          client = nil
          continue
        if strField(request, "op").toLowerAscii() == "watch_events":
          registerWatchClient(node, client, request, watchers)
          client = nil
          continue
        let response = handleRequest(node, request)
        sendFrame(client, $response)
        closeSocketQuiet(client)
      except CatchableError as exc:
        if not client.isNil:
          try:
            sendFrame(client, $(rpcResponse(false, %*{"error": "rpc_io_error", "detail": exc.msg})))
          except CatchableError:
            discard
          closeSocketQuiet(client)
    flushWatchClients(node, watchers)
  finally:
    for watcher in watchers:
      closeSocketQuiet(watcher.socket)
    for pending in pendingRequests:
      closeSocketQuiet(pending.socket)
    closeSocketQuiet(server)

when isMainModule:
  let opts = parseOptions()
  if opts.bridgeLibraryPath.len > 0:
    putEnv("NIM_TSNET_BRIDGE_LIB", opts.bridgeLibraryPath)
  var node: ProductNode = nil
  try:
    node = initNode(opts)
    serve(node)
  finally:
    stopNode(node)
