{.push raises: [].}

import std/[json, nativesockets, options, os, posix, sequtils, sets, strutils, tables, times, uri]
import chronos
import chronos/osdefs

import ../../stream/lpstream
import ../../utility
import ../quicruntime as qrt
import ../msquicdriver as msquicdrv
import ../msquicstream
import "../nim-msquic/api/event_model" as msevents
import "../nim-msquic/tls/common" as mstlstypes
import ./control

template quicSafe(body: untyped) =
  {.cast(gcsafe).}:
    body

const
  NimTsnetQuicPrefix* = "/nim-tsnet-control-quic/v1"
  NimTsnetQuicAlpn* = "nim-tsnet-control-quic"
  NimTsnetQuicDefaultPort* = 9444'u16
  QuicControlConnectEventTimeout = chronos.seconds(1)
  QuicControlConnectMaxEvents = 8
  QuicControlRequestAttempts = 8
  QuicControlRetryBackoff = chronos.milliseconds(250)
  QuicControlWriteDrainDelay = chronos.milliseconds(10)
  QuicGatewayResponseDrainDelay = chronos.seconds(1)
  QuicControlRequestTimeout = chronos.seconds(30)
  OpenNetworkRegionId = 901
  OpenNetworkRegionCode = "sin"
  OpenNetworkRegionName = "Open QUIC Network"
  OpenNetworkDerpPort = 443
  OpenNetworkServerPublicKey = "mkey:open-network-control"
  OpenNetworkServerLegacyPublicKey = "mkey:open-network-control-legacy"
  OpenNetworkLoginName = "open@nim-tsnet.local"
  OpenNetworkDisplayName = "Open Network"

type
  Ifaddrs {.importc: "struct ifaddrs", header: "<ifaddrs.h>", bycopy.} = object
    ifa_next: ptr Ifaddrs
    ifa_name: cstring
    ifa_flags: cuint
    ifa_addr: ptr SockAddr
    ifa_netmask: ptr SockAddr
    ifa_dstaddr: ptr SockAddr
    ifa_data: pointer

  TsnetQuicControlEndpoint* = object
    url*: string
    host*: string
    authority*: string
    port*: uint16
    prefix*: string

  TsnetQuicControlGatewayConfig* = object
    listenHost*: string
    listenPort*: uint16
    controlUrl*: string
    upstreamTransport*: TsnetControlTransport
    certificatePem*: string
    privateKeyPem*: string
    openNetwork*: bool
    publicHost*: string
    publicIpv4*: string
    publicIpv6*: string
    regionId*: int
    regionCode*: string
    regionName*: string
    derpPort*: int

  TsnetQuicControlGateway* = ref object
    cfg*: TsnetQuicControlGatewayConfig
    handle*: msquicdrv.MsQuicTransportHandle
    listener*: pointer
    listenerState*: msquicdrv.MsQuicListenerState
    running*: bool
    acceptLoop*: Future[void]
    boundPort*: uint16
    acceptedConnections*: int
    openState*: TsnetQuicOpenNetworkState

  TsnetQuicRpcClient = ref object
    endpoint: TsnetQuicControlEndpoint
    handle: msquicdrv.MsQuicTransportHandle
    connPtr: pointer
    connState: msquicdrv.MsQuicConnectionState

  TsnetQuicOpenNetworkNode = object
    nodeId: string
    nodeKey: string
    machineKey: string
    discoKey: string
    hostname: string
    libp2pPeerId: string
    libp2pListenAddrs: seq[string]
    tailnetIpv4: string
    tailnetIpv6: string
    mapSessionHandle: string
    seq: int64
    lastSeenUnixMilli: int64

  TsnetQuicOpenNetworkState = ref object
    serverPublicKey: string
    serverLegacyPublicKey: string
    nodes: Table[string, TsnetQuicOpenNetworkNode]

var quicClientCache {.threadvar.}: Table[string, TsnetQuicRpcClient]

proc c_getifaddrs(ifap: ptr ptr Ifaddrs): cint {.importc: "getifaddrs", header: "<ifaddrs.h>".}
proc c_freeifaddrs(ifa: ptr Ifaddrs) {.importc: "freeifaddrs", header: "<ifaddrs.h>".}

proc nowUnixMilli(): int64 =
  getTime().toUnix().int64 * 1000

proc bytesToString(bytes: openArray[byte]): string =
  result = newString(bytes.len)
  for i, value in bytes:
    result[i] = char(value)

proc stringToBytes(text: string): seq[byte] =
  result = newSeqOfCap[byte](text.len)
  for ch in text:
    result.add(byte(ord(ch)))

proc appendUint32BE(target: var seq[byte], value: uint32) =
  target.add(byte((value shr 24) and 0xFF'u32))
  target.add(byte((value shr 16) and 0xFF'u32))
  target.add(byte((value shr 8) and 0xFF'u32))
  target.add(byte(value and 0xFF'u32))

proc readUint32BE(payload: openArray[byte], offset: int): uint32 =
  (uint32(payload[offset]) shl 24) or
  (uint32(payload[offset + 1]) shl 16) or
  (uint32(payload[offset + 2]) shl 8) or
  uint32(payload[offset + 3])

proc defaultQuicPort(parsed: Uri): uint16 =
  if parsed.port.len > 0:
    try:
      return uint16(max(1, parsed.port.parseInt()))
    except CatchableError:
      discard
  NimTsnetQuicDefaultPort

proc bracketAuthorityHost(host: string): string =
  if host.contains(":") and not host.startsWith("["):
    "[" & host & "]"
  else:
    host

proc detectAddressFamily(host: string): uint16 =
  if host.len == 0:
    return 0'u16
  if host.contains(':'):
    return 23'u16
  let parts = host.split('.')
  if parts.len != 4:
    return 0'u16
  for part in parts:
    if part.len == 0 or part.len > 3:
      return 0'u16
    for ch in part:
      if ch < '0' or ch > '9':
        return 0'u16
    try:
      let value = parseInt(part)
      if value < 0 or value > 255:
        return 0'u16
    except CatchableError:
      return 0'u16
  2'u16

proc enumerateLocalInterfaceAddrs(): HashSet[string] {.gcsafe, raises: [].} =
  result = initHashSet[string]()
  var ifap: ptr Ifaddrs = nil
  try:
    if c_getifaddrs(addr ifap) == 0:
      defer:
        c_freeifaddrs(ifap)
      var cursor = ifap
      while cursor != nil:
        let addrPtr = cursor.ifa_addr
        if not addrPtr.isNil:
          if cint(addrPtr.sa_family) == posix.AF_INET:
            let sin = cast[ptr Sockaddr_in](addrPtr)
            let raw = posix.inet_ntoa(sin.sin_addr)
            if raw != nil:
              result.incl($raw)
          elif cint(addrPtr.sa_family) == posix.AF_INET6:
            let sin6 = cast[ptr Sockaddr_in6](addrPtr)
            var buffer: array[64, char]
            when declared(inet_ntop):
              if inet_ntop(
                  posix.AF_INET6,
                  addr sin6.sin6_addr,
                  cast[ptr char](addr buffer[0]),
                  buffer.len.cint
              ) != nil:
                var ip = $cast[cstring](addr buffer[0])
                let zoneIdx = ip.find('%')
                if zoneIdx >= 0:
                  ip = ip[0 ..< zoneIdx]
                result.incl(ip.toLowerAscii())
        cursor = cursor.ifa_next
  except CatchableError:
    discard

proc decodeHexNibble(ch: char): int =
  case ch
  of '0' .. '9':
    ord(ch) - ord('0')
  of 'a' .. 'f':
    ord(ch) - ord('a') + 10
  of 'A' .. 'F':
    ord(ch) - ord('A') + 10
  else:
    -1

proc openNetworkRegionId(cfg: TsnetQuicControlGatewayConfig): int =
  if cfg.regionId > 0: cfg.regionId else: OpenNetworkRegionId

proc openNetworkRegionCode(cfg: TsnetQuicControlGatewayConfig): string =
  let value = cfg.regionCode.strip().toLowerAscii()
  if value.len > 0: value else: OpenNetworkRegionCode

proc openNetworkRegionName(cfg: TsnetQuicControlGatewayConfig): string =
  let value = cfg.regionName.strip()
  if value.len > 0: value else: OpenNetworkRegionName

proc openNetworkDerpPort(cfg: TsnetQuicControlGatewayConfig): int =
  if cfg.derpPort > 0: cfg.derpPort else: OpenNetworkDerpPort

proc defaultAdvertisedHost(cfg: TsnetQuicControlGatewayConfig): string =
  let publicHost = cfg.publicHost.strip()
  if publicHost.len > 0:
    return publicHost
  let publicIpv4 = cfg.publicIpv4.strip()
  if publicIpv4.len > 0:
    return publicIpv4
  let publicIpv6 = cfg.publicIpv6.strip()
  if publicIpv6.len > 0:
    return publicIpv6
  cfg.listenHost.strip()

proc deterministicNodeTailnet(nodeKey: string): tuple[ipv4, ipv6, nodeId: string] =
  let raw =
    if nodeKey.contains(":"): nodeKey.split(":", maxsplit = 1)[1]
    else: nodeKey
  var words = [0'u8, 0'u8, 0'u8, 0'u8]
  var wordIdx = 0
  var nibble = -1
  for ch in raw:
    let value = decodeHexNibble(ch)
    if value < 0:
      continue
    if nibble < 0:
      nibble = value
    else:
      words[wordIdx mod words.len] = byte((nibble shl 4) or value)
      inc wordIdx
      nibble = -1
      if wordIdx >= words.len:
        break
  let octet3 = int(words[0])
  let octet4 = (int(words[1]) mod 253) + 2
  let hextetA = (uint16(words[0]) shl 8) or uint16(words[1])
  let hextetB = (uint16(words[2]) shl 8) or uint16(words[3])
  result.ipv4 = "100.64." & $octet3 & "." & $octet4
  result.ipv6 = "fd7a:115c:a1e0::" & toHex(hextetA, 4) & ":" & toHex(hextetB, 4)
  let suffix =
    if raw.len >= 8: raw[0 .. 7].toLowerAscii()
    elif raw.len > 0: raw.toLowerAscii()
    else: "anonymous"
  result.nodeId = "open-" & suffix

proc initOpenNetworkState(): TsnetQuicOpenNetworkState =
  TsnetQuicOpenNetworkState(
    serverPublicKey: OpenNetworkServerPublicKey,
    serverLegacyPublicKey: OpenNetworkServerLegacyPublicKey,
    nodes: initTable[string, TsnetQuicOpenNetworkNode]()
  )

proc tlsServerName(host: string): Option[string] {.gcsafe, raises: [].} =
  if host.len == 0:
    return none(string)
  let suppressIpSni =
    getEnv("NIM_TSNET_CONTROL_QUIC_NO_SNI", "").strip().toLowerAscii() in
      ["1", "true", "yes"]
  if suppressIpSni and detectAddressFamily(host) != 0'u16:
    return none(string)
  some(host)

proc isLoopbackControlHost(host: string): bool {.gcsafe, raises: [].}

proc localLoopbackDialHost(host: string): Option[string] {.gcsafe, raises: [].} =
  let normalized = host.strip().toLowerAscii()
  if normalized.len == 0:
    return none(string)
  let family = detectAddressFamily(normalized)
  if family == 0'u16 or isLoopbackControlHost(normalized):
    return none(string)
  let localAddrs = enumerateLocalInterfaceAddrs()
  if normalized notin localAddrs:
    return none(string)
  if family == 23'u16:
    some("::1")
  else:
    some("127.0.0.1")

proc isLoopbackControlHost(host: string): bool {.gcsafe, raises: [].} =
  let lowered = host.strip().toLowerAscii()
  lowered in ["127.0.0.1", "::1", "localhost"]

proc parseRuntimePreference(raw: string): Option[qrt.QuicRuntimePreference] =
  case raw.strip().toLowerAscii()
  of "auto":
    some(qrt.qrpAuto)
  of "native", "native_only", "builtin_preferred":
    some(qrt.qrpBuiltinOnly)
  of "", "builtin", "builtin_only", "nim", "nim_quic":
    some(qrt.qrpBuiltinOnly)
  else:
    none(qrt.QuicRuntimePreference)

proc quicControlRuntimePreference(): qrt.QuicRuntimePreference =
  parseRuntimePreference(getEnv("NIM_TSNET_CONTROL_QUIC_RUNTIME", "")).get(qrt.qrpBuiltinOnly)

proc applyRuntimePreference(cfg: var msquicdrv.MsQuicTransportConfig) =
  when compiles(qrt.useBuiltinRuntime(cfg)):
    case quicControlRuntimePreference()
    of qrt.qrpAuto:
      qrt.useAutoRuntime(cfg)
    of qrt.qrpNativeOnly:
      qrt.useBuiltinRuntime(cfg)
    of qrt.qrpBuiltinPreferred:
      qrt.useBuiltinRuntime(cfg)
    of qrt.qrpBuiltinOnly:
      qrt.useBuiltinRuntime(cfg)

proc quicGatewayDebugEnabled(): bool =
  getEnv("NIM_TSNET_QUIC_GATEWAY_DEBUG").strip() in ["1", "true", "TRUE", "yes", "YES"]

proc quicGatewayDebug(message: string) =
  if not quicGatewayDebugEnabled():
    return
  try:
    stderr.writeLine("[nim_quic_gateway] " & message)
  except IOError:
    discard

proc quicClientDebugEnabled(): bool =
  getEnv("NIM_TSNET_QUIC_CLIENT_DEBUG").strip() in ["1", "true", "TRUE", "yes", "YES"]

proc quicClientDebug(message: string) =
  if not quicClientDebugEnabled():
    return
  try:
    stderr.writeLine("[nim_quic_client] " & message)
  except IOError:
    discard

proc resolveDialHost(host: string, port: uint16): Result[string, string] {.gcsafe, raises: [].} =
  if host.len == 0:
    return err("nim_quic control host is empty")
  let literalOverride = localLoopbackDialHost(host)
  if literalOverride.isSome():
    return ok(literalOverride.get())
  if detectAddressFamily(host) != 0'u16:
    return ok(host)
  for domain in [AF_INET, AF_INET6, AF_UNSPEC]:
    try:
      let addrInfo = getAddrInfo(
        host,
        Port(port),
        domain,
        SOCK_DGRAM,
        IPPROTO_UDP
      )
      if addrInfo.isNil:
        continue
      defer:
        freeAddrInfo(addrInfo)
      let resolved = getAddrString(cast[ptr SockAddr](addrInfo.ai_addr))
      if resolved.len > 0:
        let resolvedOverride = localLoopbackDialHost(resolved)
        if resolvedOverride.isSome():
          return ok(resolvedOverride.get())
        return ok(resolved)
    except CatchableError:
      discard
  err("failed to resolve nim_quic control host " & host)

proc quicEndpoint*(controlUrl: string): Result[TsnetQuicControlEndpoint, string] =
  let normalized = normalizeControlUrl(controlUrl)
  if normalized.len == 0:
    return err("tsnet controlUrl is empty")
  let parsed =
    try:
      parseUri(normalized)
    except CatchableError as exc:
      return err("failed to parse tsnet controlUrl " & normalized & ": " & exc.msg)
  if parsed.hostname.len == 0:
    return err("tsnet controlUrl is missing a hostname")
  let port = defaultQuicPort(parsed)
  let authorityHost = bracketAuthorityHost(parsed.hostname)
  let authority = authorityHost & ":" & $port
  ok(TsnetQuicControlEndpoint(
    url: "quic://" & authority & NimTsnetQuicPrefix,
    host: parsed.hostname,
    authority: authority,
    port: port,
    prefix: NimTsnetQuicPrefix
  ))

proc nimQuicBaseUrl*(controlUrl: string): string =
  let endpoint = quicEndpoint(controlUrl).valueOr:
    return ""
  endpoint.url

proc nimQuicHealthUrl*(controlUrl: string): string =
  let base = nimQuicBaseUrl(controlUrl)
  if base.len == 0:
    return ""
  base & "/health"

proc nimQuicKeyUrl*(controlUrl: string, capabilityVersion: int): string =
  let base = nimQuicBaseUrl(controlUrl)
  if base.len == 0:
    return ""
  base & "/key?v=" & $capabilityVersion

proc nimQuicRegisterUrl*(controlUrl: string): string =
  let base = nimQuicBaseUrl(controlUrl)
  if base.len == 0:
    return ""
  base & "/register"

proc nimQuicMapUrl*(controlUrl: string): string =
  let base = nimQuicBaseUrl(controlUrl)
  if base.len == 0:
    return ""
  base & "/map"

proc buildRpcRequest(
    route: string,
    capabilityVersion = 0,
    requestPayload: JsonNode = nil
): JsonNode =
  result = %*{
    "version": 1,
    "route": route
  }
  if capabilityVersion > 0:
    result["capabilityVersion"] = %capabilityVersion
  if not requestPayload.isNil and requestPayload.kind != JNull:
    result["request"] = requestPayload

proc copyStringField(source: JsonNode, target: JsonNode, key: string) =
  if source.isNil or source.kind != JObject or target.isNil or target.kind != JObject:
    return
  if not source.hasKey(key):
    return
  let value = source.getOrDefault(key)
  if value.kind == JString and value.getStr().len > 0:
    target[key] = %value.getStr()

proc copyBoolField(source: JsonNode, target: JsonNode, key: string) =
  if source.isNil or source.kind != JObject or target.isNil or target.kind != JObject:
    return
  if not source.hasKey(key):
    return
  let value = source.getOrDefault(key)
  if value.kind == JBool:
    target[key] = %value.getBool()

proc copyIntField(source: JsonNode, target: JsonNode, key: string) =
  if source.isNil or source.kind != JObject or target.isNil or target.kind != JObject:
    return
  if not source.hasKey(key):
    return
  let value = source.getOrDefault(key)
  case value.kind
  of JInt:
    target[key] = %value.getBiggestInt()
  of JFloat:
    target[key] = %value.getFloat().int64
  else:
    discard

proc copyArrayField(source: JsonNode, target: JsonNode, key: string) =
  if source.isNil or source.kind != JObject or target.isNil or target.kind != JObject:
    return
  if not source.hasKey(key):
    return
  let value = source.getOrDefault(key)
  if value.kind == JArray:
    target[key] = value

proc jsonStringField(source: JsonNode, key: string): string =
  if source.isNil or source.kind != JObject or not source.hasKey(key):
    return ""
  let value = source.getOrDefault(key)
  if value.kind == JString:
    return value.getStr().strip()
  ""

proc jsonStringArrayField(source: JsonNode, key: string): seq[string] =
  if source.isNil or source.kind != JObject or not source.hasKey(key):
    return @[]
  let value = source.getOrDefault(key)
  if value.kind != JArray:
    return @[]
  for item in value.items():
    if item.kind != JString:
      continue
    let text = item.getStr().strip()
    if text.len > 0:
      result.add(text)

proc trimHostinfo(hostinfo: JsonNode): JsonNode =
  result = newJObject()
  copyStringField(hostinfo, result, "Hostname")
  copyStringField(hostinfo, result, "PeerID")
  copyArrayField(hostinfo, result, "ListenAddrs")

proc trimPeerNode(node: JsonNode): JsonNode =
  result = newJObject()
  copyStringField(node, result, "Name")
  copyStringField(node, result, "PeerID")
  copyIntField(node, result, "HomeDERP")
  copyArrayField(node, result, "Addresses")
  copyArrayField(node, result, "ListenAddrs")
  let hostinfo = trimHostinfo(
    if node.isNil or node.kind != JObject: newJNull()
    else: node.getOrDefault("Hostinfo")
  )
  if hostinfo.len > 0:
    result["Hostinfo"] = hostinfo

proc trimDerpNode(node: JsonNode): JsonNode =
  result = newJObject()
  copyStringField(node, result, "Name")
  copyIntField(node, result, "RegionID")
  copyStringField(node, result, "HostName")
  copyStringField(node, result, "CertName")
  copyStringField(node, result, "IPv4")
  copyStringField(node, result, "IPv6")
  copyIntField(node, result, "STUNPort")
  copyBoolField(node, result, "STUNOnly")
  copyIntField(node, result, "DERPPort")
  copyBoolField(node, result, "CanPort80")

proc trimDerpRegion(region: JsonNode): JsonNode =
  result = newJObject()
  copyIntField(region, result, "RegionID")
  copyStringField(region, result, "RegionCode")
  copyStringField(region, result, "RegionName")
  copyBoolField(region, result, "Avoid")
  copyBoolField(region, result, "NoMeasureNoHome")
  let nodes =
    if region.isNil or region.kind != JObject: newJNull()
    else: region.getOrDefault("Nodes")
  if nodes.kind == JArray:
    var trimmedNodes = newJArray()
    for node in nodes.items():
      if node.kind == JObject:
        trimmedNodes.add(trimDerpNode(node))
    result["Nodes"] = trimmedNodes
    result["NodeCount"] = %trimmedNodes.len

proc trimDerpMapPayload(derpMap: JsonNode): JsonNode =
  result = newJObject()
  copyBoolField(derpMap, result, "OmitDefaultRegions")
  let regions =
    if derpMap.isNil or derpMap.kind != JObject: newJNull()
    else: derpMap.getOrDefault("Regions")
  case regions.kind
  of JObject:
    var trimmedRegions = newJObject()
    var count = 0
    for key, value in regions:
      if value.kind == JObject:
        trimmedRegions[key] = trimDerpRegion(value)
        inc count
    result["Regions"] = trimmedRegions
    result["RegionCount"] = %count
  of JArray:
    var trimmedRegions = newJArray()
    for value in regions.items():
      if value.kind == JObject:
        trimmedRegions.add(trimDerpRegion(value))
    result["Regions"] = trimmedRegions
    result["RegionCount"] = %trimmedRegions.len
  else:
    discard

proc trimMapPayload(payload: JsonNode): JsonNode =
  if payload.isNil or payload.kind != JObject:
    return payload
  result = newJObject()
  copyBoolField(payload, result, "KeepAlive")
  copyStringField(payload, result, "Domain")
  copyBoolField(payload, result, "CollectServices")
  copyStringField(payload, result, "MapSessionHandle")
  copyIntField(payload, result, "Seq")
  let node =
    if payload.hasKey("Node"): payload.getOrDefault("Node")
    else: newJNull()
  if node.kind == JObject:
    result["Node"] = trimPeerNode(node)
  let derpMap =
    if payload.hasKey("DERPMap"): payload.getOrDefault("DERPMap")
    else: newJNull()
  if derpMap.kind == JObject:
    result["DERPMap"] = trimDerpMapPayload(derpMap)
  let peers =
    if payload.hasKey("Peers"): payload.getOrDefault("Peers")
    else: newJNull()
  if peers.kind == JArray:
    var trimmedPeers = newJArray()
    for peer in peers.items():
      if peer.kind == JObject:
        trimmedPeers.add(trimPeerNode(peer))
    result["Peers"] = trimmedPeers
    result["PeerCount"] = %peers.len

proc ensureOpenNode(
    gateway: TsnetQuicControlGateway,
    requestNode: JsonNode
): TsnetQuicOpenNetworkNode =
  if gateway.openState.isNil:
    gateway.openState = initOpenNetworkState()
  let hostinfo =
    if not requestNode.isNil and requestNode.kind == JObject and requestNode.hasKey("Hostinfo"):
      requestNode.getOrDefault("Hostinfo")
    else:
      newJNull()
  let nodeKey = jsonStringField(requestNode, "NodeKey")
  let machineKey = jsonStringField(hostinfo, "BackendLogID")
  let discoKey = jsonStringField(requestNode, "DiscoKey")
  let requestedHost = jsonStringField(hostinfo, "Hostname")
  let libp2pPeerId = jsonStringField(hostinfo, "PeerID")
  let libp2pListenAddrs = jsonStringArrayField(hostinfo, "ListenAddrs")
  var existing = gateway.openState.nodes.getOrDefault(nodeKey, TsnetQuicOpenNetworkNode())
  if existing.nodeId.len == 0:
    let tailnet = deterministicNodeTailnet(nodeKey)
    existing = TsnetQuicOpenNetworkNode(
      nodeId: tailnet.nodeId,
      nodeKey: nodeKey,
      machineKey: machineKey,
      discoKey: discoKey,
      hostname: if requestedHost.len > 0: requestedHost else: tailnet.nodeId,
      libp2pPeerId: libp2pPeerId,
      libp2pListenAddrs: libp2pListenAddrs,
      tailnetIpv4: tailnet.ipv4,
      tailnetIpv6: tailnet.ipv6,
      mapSessionHandle: "session-" & tailnet.nodeId,
      seq: 0,
      lastSeenUnixMilli: nowUnixMilli()
    )
  else:
    if requestedHost.len > 0:
      existing.hostname = requestedHost
    if machineKey.len > 0:
      existing.machineKey = machineKey
    if discoKey.len > 0:
      existing.discoKey = discoKey
    if libp2pPeerId.len > 0:
      existing.libp2pPeerId = libp2pPeerId
    if libp2pListenAddrs.len > 0:
      existing.libp2pListenAddrs = libp2pListenAddrs
    existing.lastSeenUnixMilli = nowUnixMilli()
  gateway.openState.nodes[nodeKey] = existing
  existing

proc openNetworkServerKey(
    gateway: TsnetQuicControlGateway,
    capabilityVersion: int
): JsonNode =
  if gateway.openState.isNil:
    gateway.openState = initOpenNetworkState()
  %*{
    "publicKey": gateway.openState.serverPublicKey,
    "legacyPublicKey": gateway.openState.serverLegacyPublicKey,
    "capabilityVersion": capabilityVersion
  }

proc openNetworkRegisterResponse(
    node: TsnetQuicOpenNetworkNode
): JsonNode =
  %*{
    "MachineAuthorized": true,
    "AuthURL": "",
    "NodeKeySignature": "open-network",
    "User": {
      "LoginName": OpenNetworkLoginName,
      "DisplayName": OpenNetworkDisplayName
    },
    "Login": {
      "LoginName": OpenNetworkLoginName
    },
    "Node": {
      "StableID": node.nodeId,
      "Name": node.hostname,
      "PeerID": node.libp2pPeerId,
      "ListenAddrs": node.libp2pListenAddrs,
      "Addresses": [node.tailnetIpv4 & "/32", node.tailnetIpv6 & "/128"],
      "Hostinfo": {
        "Hostname": node.hostname,
        "PeerID": node.libp2pPeerId,
        "ListenAddrs": node.libp2pListenAddrs
      }
    }
  }

proc openNetworkDerpMap(gateway: TsnetQuicControlGateway): JsonNode =
  let host = gateway.cfg.defaultAdvertisedHost()
  %*{
    "OmitDefaultRegions": true,
    "Regions": {
      $gateway.cfg.openNetworkRegionId(): {
        "RegionID": gateway.cfg.openNetworkRegionId(),
        "RegionCode": gateway.cfg.openNetworkRegionCode(),
        "RegionName": gateway.cfg.openNetworkRegionName(),
        "Nodes": [{
          "Name": $gateway.cfg.openNetworkRegionId(),
          "RegionID": gateway.cfg.openNetworkRegionId(),
          "HostName": host,
          "IPv4": gateway.cfg.publicIpv4.strip(),
          "IPv6": gateway.cfg.publicIpv6.strip(),
          "DERPPort": gateway.cfg.openNetworkDerpPort()
        }]
      }
    }
  }

proc openNetworkMapResponse(
    gateway: TsnetQuicControlGateway,
    selfNode: TsnetQuicOpenNetworkNode
): JsonNode =
  if gateway.openState.isNil:
    gateway.openState = initOpenNetworkState()
  var peers = newJArray()
  for _, candidate in gateway.openState.nodes:
    if candidate.nodeKey == selfNode.nodeKey:
      continue
    peers.add(%*{
      "StableID": candidate.nodeId,
      "Name": candidate.hostname,
      "PeerID": candidate.libp2pPeerId,
      "ListenAddrs": candidate.libp2pListenAddrs,
      "HomeDERP": gateway.cfg.openNetworkRegionId(),
      "Addresses": [candidate.tailnetIpv4 & "/32", candidate.tailnetIpv6 & "/128"],
      "Hostinfo": {
        "Hostname": candidate.hostname,
        "PeerID": candidate.libp2pPeerId,
        "ListenAddrs": candidate.libp2pListenAddrs
      }
    })
  var updated = selfNode
  inc updated.seq
  updated.lastSeenUnixMilli = nowUnixMilli()
  gateway.openState.nodes[selfNode.nodeKey] = updated
  %*{
    "KeepAlive": true,
    "MapSessionHandle": updated.mapSessionHandle,
    "Seq": updated.seq,
    "Node": {
      "StableID": updated.nodeId,
      "Name": updated.hostname,
      "PeerID": updated.libp2pPeerId,
      "ListenAddrs": updated.libp2pListenAddrs,
      "HomeDERP": gateway.cfg.openNetworkRegionId(),
      "Addresses": [updated.tailnetIpv4 & "/32", updated.tailnetIpv6 & "/128"],
      "Hostinfo": {
        "Hostname": updated.hostname,
        "PeerID": updated.libp2pPeerId,
        "ListenAddrs": updated.libp2pListenAddrs
      }
    },
    "DERPMap": gateway.openNetworkDerpMap(),
    "Peers": peers
  }

proc responsePayload(payload: JsonNode): Result[JsonNode, string] =
  if payload.isNil or payload.kind != JObject:
    return err("nim_quic control response must be a JSON object")
  let okNode = payload.getOrDefault("ok")
  let okValue =
    if not okNode.isNil and okNode.kind == JBool: okNode.getBool()
    else: false
  if not okValue:
    let errorNode = payload.getOrDefault("error")
    let message =
      if not errorNode.isNil and errorNode.kind == JString and errorNode.getStr().strip().len > 0:
        errorNode.getStr().strip()
      else:
        "nim_quic control request failed"
    return err(message)
  let payloadNode =
    if payload.hasKey("payload"): payload.getOrDefault("payload")
    else: newJObject()
  ok(payloadNode)

proc rpcError(message: string): JsonNode =
  %*{
    "ok": false,
    "error": message
  }

proc rpcSuccess(payload: JsonNode): JsonNode =
  result = newJObject()
  result["ok"] = %true
  result["payload"] =
    if payload.isNil: newJObject()
    else: payload

proc safeCloseStream(
    handle: msquicdrv.MsQuicTransportHandle,
    streamPtr: pointer,
    streamState: msquicdrv.MsQuicStreamState
) {.gcsafe, raises: [].} =
  if handle.isNil or streamPtr.isNil or streamState.isNil:
    return
  try:
    quicSafe:
      msquicdrv.closeStream(handle, streamPtr, streamState)
  except CatchableError:
    discard

proc safeShutdownConnection(
    handle: msquicdrv.MsQuicTransportHandle,
    connPtr: pointer
) {.gcsafe, raises: [].} =
  if handle.isNil or connPtr.isNil:
    return
  try:
    quicSafe:
      discard msquicdrv.shutdownConnection(handle, connPtr)
  except CatchableError:
    discard

proc safeCloseConnection(
    handle: msquicdrv.MsQuicTransportHandle,
    connPtr: pointer,
    connState: msquicdrv.MsQuicConnectionState
) {.gcsafe, raises: [].} =
  if handle.isNil or connPtr.isNil or connState.isNil:
    return
  try:
    quicSafe:
      msquicdrv.closeConnection(handle, connPtr, connState)
  except CatchableError:
    discard

proc safeShutdownRuntime(handle: msquicdrv.MsQuicTransportHandle) {.gcsafe, raises: [].} =
  if handle.isNil:
    return
  try:
    quicSafe:
      msquicdrv.shutdown(handle)
  except CatchableError:
    discard

proc closeQuicRpcClient(client: TsnetQuicRpcClient) =
  if client.isNil:
    return
  safeShutdownConnection(client.handle, client.connPtr)
  safeCloseConnection(client.handle, client.connPtr, client.connState)
  safeShutdownRuntime(client.handle)
  client.handle = nil
  client.connPtr = nil
  client.connState = nil

proc canReuseCachedQuicClient(client: TsnetQuicRpcClient): bool =
  if client.isNil:
    return false
  if client.handle.isNil or client.connPtr.isNil or client.connState.isNil:
    return false
  if client.connState.closed:
    return false
  if not msquicdrv.connectionHandshakeComplete(client.connState):
    return false
  true

proc discardCachedQuicClient(cacheKey: string) =
  if cacheKey.len == 0:
    return
  let cached = quicClientCache.getOrDefault(cacheKey, nil)
  if not cached.isNil:
    closeQuicRpcClient(cached)
    quicClientCache.del(cacheKey)

proc closeAllQuicClients*() =
  if quicClientCache.len == 0:
    return
  let keys = toSeq(quicClientCache.keys)
  for key in keys:
    discardCachedQuicClient(key)

proc safeStreamId(
    streamState: msquicdrv.MsQuicStreamState
): Result[uint64, string] {.gcsafe, raises: [].} =
  if streamState.isNil:
    return err("nil nim_quic stream state")
  try:
    quicSafe:
      result = msquicdrv.streamId(streamState)
  except CatchableError as exc:
    return err("failed to query nim_quic stream id: " & exc.msg)

proc safeAttachIncomingConnection(
    handle: msquicdrv.MsQuicTransportHandle,
    connection: pointer
): tuple[state: Option[msquicdrv.MsQuicConnectionState], error: string]
    {.gcsafe, raises: [].} =
  try:
    quicSafe:
      result = msquicdrv.attachIncomingConnection(handle, connection)
  except CatchableError as exc:
    return (
      none(msquicdrv.MsQuicConnectionState),
      "failed to attach incoming nim_quic connection: " & exc.msg
    )

proc awaitConnected(
    connState: msquicdrv.MsQuicConnectionState
) : Future[(bool, string)] {.async.} =
  if connState.isNil:
    return (false, "nim_quic connection state unavailable")
  var attempt = 0
  var timeoutCount = 0
  while attempt < QuicControlConnectMaxEvents:
    if msquicdrv.connectionHandshakeComplete(connState):
      quicClientDebug("handshake complete via state poll attempt=" & $attempt)
      return (true, "handshake_complete_poll")
    inc attempt
    let eventFuture = connState.nextQuicConnectionEvent()
    let timeoutFuture = sleepAsync(QuicControlConnectEventTimeout)
    let winner = await race(cast[FutureBase](eventFuture), cast[FutureBase](timeoutFuture))
    if winner == cast[FutureBase](timeoutFuture):
      eventFuture.cancel()
      inc timeoutCount
      quicClientDebug(
        "connection event timeout attempt=" & $attempt &
        " timeoutCount=" & $timeoutCount
      )
      continue
    timeoutFuture.cancel()
    let event =
      try:
        await eventFuture
      except qrt.QuicRuntimeEventQueueClosed:
        return (false, "nim_quic connection event queue closed")
    case event.kind
    of qrt.qceConnected:
      quicClientDebug("connection connected attempt=" & $attempt)
      return (true, "connected")
    of qrt.qceShutdownInitiated:
      let closeReason = msquicdrv.connectionCloseReason(connState)
      quicClientDebug("connection shutdown initiated attempt=" & $attempt)
      return (
        false,
        "nim_quic connection shutdown initiated" &
          (if closeReason.len > 0: " reason=" & closeReason else: "")
      )
    of qrt.qceShutdownComplete:
      let closeReason = msquicdrv.connectionCloseReason(connState)
      quicClientDebug("connection shutdown complete attempt=" & $attempt)
      return (
        false,
        "nim_quic connection shutdown complete" &
          (if closeReason.len > 0: " reason=" & closeReason else: "")
      )
    else:
      quicClientDebug("connection event kind=" & $event.kind & " attempt=" & $attempt)
      discard
  if msquicdrv.connectionHandshakeComplete(connState):
    quicClientDebug("handshake complete via final state poll")
    return (true, "handshake_complete_poll")
  if timeoutCount > 0:
    (false, "timeout waiting for nim_quic connection event")
  else:
    (false, "nim_quic dial exceeded event budget")

proc createBidiStream(
    handle: msquicdrv.MsQuicTransportHandle,
    connPtr: pointer,
    connState: msquicdrv.MsQuicConnectionState
): Result[MsQuicStream, string] =
  let created = msquicdrv.createStream(
    handle,
    connPtr,
    flags = 0'u32,
    connectionState = connState
  )
  if created.error.len > 0 or created.state.isNone or created.stream.isNil:
    return err(
      "failed to create nim_quic stream: " &
      (if created.error.len > 0: created.error else: "unknown error")
    )
  let startErr = msquicdrv.startStream(handle, created.stream)
  if startErr.len > 0:
    safeCloseStream(handle, created.stream, created.state.get())
    return err("failed to start nim_quic stream: " & startErr)
  try:
    ok(newMsQuicStream(created.state.get(), handle, Direction.Out))
  except LPStreamError as exc:
    safeCloseStream(handle, created.stream, created.state.get())
    err("failed to wrap nim_quic stream: " & exc.msg)

proc awaitIncomingBidiStream(
    handle: msquicdrv.MsQuicTransportHandle,
    connState: msquicdrv.MsQuicConnectionState
): Future[MsQuicStream] {.async: (raises: [CancelledError, CatchableError]).} =
  while true:
    var pending = msquicdrv.popPendingStreamState(connState)
    if pending.isNone:
      pending = some(await msquicdrv.awaitPendingStreamState(connState))
    if pending.isNone:
      continue
    let streamState = pending.get()
    if streamState.isNil or streamState.stream.isNil:
      continue
    if msquicdrv.isLocalInitiated(streamState):
      continue
    let idRes = safeStreamId(streamState)
    if idRes.isErr():
      safeCloseStream(handle, cast[pointer](streamState.stream), streamState)
      continue
    let isUnidirectional = (idRes.get() and 0x2'u64) != 0'u64
    if isUnidirectional:
      safeCloseStream(handle, cast[pointer](streamState.stream), streamState)
      continue
    try:
      return newMsQuicStream(streamState, handle, Direction.In)
    except LPStreamError:
      safeCloseStream(handle, cast[pointer](streamState.stream), streamState)
      continue

proc readJsonMessage(
    stream: MsQuicStream
): Future[JsonNode] {.async: (raises: [CancelledError, CatchableError]).} =
  var payload: seq[byte] = @[]
  var framedMode = false
  var frameLen = -1
  proc parseLegacyIfComplete(): Option[JsonNode] =
    if payload.len == 0:
      return none(JsonNode)
    try:
      return some(parseJson(bytesToString(payload)))
    except CatchableError:
      return none(JsonNode)
  while true:
    try:
      let chunk = await stream.read()
      if chunk.len == 0:
        break
      payload.add(chunk)
      quicClientDebug("read chunk bytes=" & $chunk.len & " buffered=" & $payload.len)
      quicGatewayDebug("read chunk bytes=" & $chunk.len & " buffered=" & $payload.len)
      if payload.len >= 4 and not framedMode:
        let first = char(payload[0])
        if first notin {'{', '['}:
          framedMode = true
          frameLen = int(readUint32BE(payload, 0))
          quicClientDebug("framed message length=" & $frameLen)
          quicGatewayDebug("framed message length=" & $frameLen)
      if framedMode:
        if frameLen < 0:
          return newJObject()
        if payload.len >= 4 + frameLen:
          return parseJson(bytesToString(payload.toOpenArray(4, 3 + frameLen)))
      else:
        let parsed = parseLegacyIfComplete()
        if parsed.isSome:
          return parsed.get()
    except LPStreamEOFError:
      break
    except CatchableError as exc:
      if payload.len >= 4 and not framedMode:
        let first = char(payload[0])
        if first notin {'{', '['}:
          framedMode = true
          frameLen = int(readUint32BE(payload, 0))
      if framedMode:
        if frameLen >= 0 and payload.len >= 4 + frameLen:
          return parseJson(bytesToString(payload.toOpenArray(4, 3 + frameLen)))
      else:
        let parsed = parseLegacyIfComplete()
        if parsed.isSome:
          return parsed.get()
      raise exc
  if payload.len == 0:
    return newJObject()
  if framedMode:
    if frameLen >= 0 and payload.len >= 4 + frameLen:
      return parseJson(bytesToString(payload.toOpenArray(4, 3 + frameLen)))
  else:
    let parsed = parseLegacyIfComplete()
    if parsed.isSome:
      return parsed.get()
  parseJson(bytesToString(payload))

proc writeJsonMessage(
    stream: MsQuicStream,
    payload: JsonNode
) {.async: (raises: [CancelledError, CatchableError]).} =
  let encoded =
    if payload.isNil: "{}"
    else: $payload
  let bodyBytes = stringToBytes(encoded)
  var encodedBytes = newSeqOfCap[byte](4 + bodyBytes.len)
  appendUint32BE(encodedBytes, uint32(bodyBytes.len))
  encodedBytes.add(bodyBytes)
  quicClientDebug("write framed bytes=" & $encodedBytes.len & " body=" & $bodyBytes.len)
  quicGatewayDebug("write framed bytes=" & $encodedBytes.len & " body=" & $bodyBytes.len)
  await stream.write(encodedBytes)
  if encodedBytes.len > 0:
    await sleepAsync(QuicControlWriteDrainDelay)

proc upstreamTransport(gateway: TsnetQuicControlGateway): TsnetControlTransport =
  if gateway.isNil or gateway.cfg.upstreamTransport.isNil:
    return defaultControlTransport()
  gateway.cfg.upstreamTransport

proc routeKey(
    gateway: TsnetQuicControlGateway,
    request: JsonNode
): JsonNode =
  quicGatewayDebug("route=key begin")
  let capabilityVersion =
    if request.kind == JObject and request.hasKey("capabilityVersion"):
      try:
        request["capabilityVersion"].getInt()
      except CatchableError:
        130
    else:
      130
  if not gateway.isNil and gateway.cfg.openNetwork:
    quicGatewayDebug("route=key open-network ok")
    return rpcSuccess(gateway.openNetworkServerKey(capabilityVersion))
  let fetched =
    if gateway.isNil or gateway.cfg.upstreamTransport.isNil:
      fetchControlServerKey(gateway.cfg.controlUrl, capabilityVersion)
    else:
      gateway.cfg.upstreamTransport.fetchServerKey(gateway.cfg.controlUrl)
  if fetched.isErr():
    quicGatewayDebug("route=key error=" & fetched.error)
    return rpcError(fetched.error)
  quicGatewayDebug("route=key ok")
  rpcSuccess(fetched.get().rawPayload)

proc routeRegister(
    gateway: TsnetQuicControlGateway,
    request: JsonNode
): JsonNode =
  quicGatewayDebug("route=register begin")
  let requestNode =
    if not request.isNil and request.kind == JObject: request.getOrDefault("request")
    else: newJNull()
  if requestNode.isNil or requestNode.kind != JObject:
    quicGatewayDebug("route=register invalid request")
    return rpcError("missing request object")
  if not gateway.isNil and gateway.cfg.openNetwork:
    let node = gateway.ensureOpenNode(requestNode)
    quicGatewayDebug("route=register open-network ok nodeId=" & node.nodeId)
    return rpcSuccess(openNetworkRegisterResponse(node))
  let regRes = gateway.upstreamTransport().registerNode(gateway.cfg.controlUrl, requestNode)
  if regRes.isErr():
    quicGatewayDebug("route=register error=" & regRes.error)
    return rpcError(regRes.error)
  quicGatewayDebug("route=register ok")
  rpcSuccess(regRes.get().rawPayload)

proc routeMap(
    gateway: TsnetQuicControlGateway,
    request: JsonNode
): JsonNode =
  quicGatewayDebug("route=map begin")
  let requestNode =
    if not request.isNil and request.kind == JObject: request.getOrDefault("request")
    else: newJNull()
  if requestNode.isNil or requestNode.kind != JObject:
    quicGatewayDebug("route=map invalid request")
    return rpcError("missing request object")
  if not gateway.isNil and gateway.cfg.openNetwork:
    let node = gateway.ensureOpenNode(requestNode)
    quicGatewayDebug("route=map open-network ok nodeId=" & node.nodeId)
    return rpcSuccess(trimMapPayload(gateway.openNetworkMapResponse(node)))
  let mapRes = gateway.upstreamTransport().mapPoll(gateway.cfg.controlUrl, requestNode)
  if mapRes.isErr():
    quicGatewayDebug("route=map error=" & mapRes.error)
    return rpcError(mapRes.error)
  quicGatewayDebug("route=map ok")
  rpcSuccess(trimMapPayload(mapRes.get().rawPayload))

proc routeHealth(gateway: TsnetQuicControlGateway): JsonNode =
  rpcSuccess(%*{
    "ok": true,
    "controlUrl":
      if gateway.isNil: ""
      else: gateway.cfg.controlUrl,
    "protocol": "nim_quic",
    "upstreamProtocol":
      if gateway.isNil or gateway.cfg.openNetwork:
        "open_network"
      else:
        "ts2021_h2",
    "endpointPrefix": NimTsnetQuicPrefix
  })

proc handleGatewayRequest(
    gateway: TsnetQuicControlGateway,
    request: JsonNode
): JsonNode =
  if request.isNil or request.kind != JObject:
    quicGatewayDebug("route=<invalid> requestKind=" & $(
      if request.isNil: "nil" else: $request.kind
    ))
    return rpcError("nim_quic request must be a JSON object")
  let routeNode = request.getOrDefault("route")
  let route =
    if not routeNode.isNil and routeNode.kind == JString: routeNode.getStr().strip()
    else: ""
  quicGatewayDebug("route=" & (if route.len > 0: route else: "<empty>"))
  case route
  of "health":
    routeHealth(gateway)
  of "key":
    routeKey(gateway, request)
  of "register":
    routeRegister(gateway, request)
  of "map":
    routeMap(gateway, request)
  else:
    rpcError("not_found")

proc new*(
    _: type[TsnetQuicControlGateway],
    cfg: TsnetQuicControlGatewayConfig
): TsnetQuicControlGateway =
  TsnetQuicControlGateway(cfg: cfg, boundPort: cfg.listenPort)

proc boundPort*(gateway: TsnetQuicControlGateway): uint16 =
  if gateway.isNil:
    return 0'u16
  gateway.boundPort

proc serveRequestStream(
    gateway: TsnetQuicControlGateway,
    stream: MsQuicStream
) {.async: (raises: []).} =
  defer:
    if not stream.isNil:
      await noCancel stream.closeImpl()
  try:
    let request = await readJsonMessage(stream)
    quicGatewayDebug("request-json=" & $request)
    let response = handleGatewayRequest(gateway, request)
    quicGatewayDebug("response-json=" & $response)
    await writeJsonMessage(stream, response)
    await stream.closeWrite()
    await sleepAsync(QuicGatewayResponseDrainDelay)
  except CatchableError as exc:
    quicGatewayDebug("serveRequestStream error=" & exc.msg)
    try:
      await writeJsonMessage(stream, rpcError(exc.msg))
      await stream.closeWrite()
      await sleepAsync(QuicGatewayResponseDrainDelay)
    except CatchableError:
      discard

proc serveConnection(
    gateway: TsnetQuicControlGateway,
    connPtr: pointer,
    connState: msquicdrv.MsQuicConnectionState
) {.async: (raises: []).} =
  defer:
    safeShutdownConnection(gateway.handle, connPtr)
    safeCloseConnection(gateway.handle, connPtr, connState)
  while gateway.running:
    let stream =
      try:
        await awaitIncomingBidiStream(gateway.handle, connState)
      except CatchableError:
        break
    asyncSpawn gateway.serveRequestStream(stream)

proc listenerLoop(gateway: TsnetQuicControlGateway) {.async: (raises: []).} =
  while gateway.running and not gateway.listenerState.isNil:
    let event =
      try:
        await msquicdrv.nextListenerEvent(gateway.listenerState)
      except CatchableError:
        break
    quicGatewayDebug("listener event kind=" & $event.kind)
    if event.kind != msevents.leNewConnection or event.connection.isNil:
      continue
    var stateOpt = msquicdrv.takePendingConnection(gateway.listenerState, event.connection)
    if stateOpt.isNone:
      let attachRes = safeAttachIncomingConnection(gateway.handle, event.connection)
      if attachRes.error.len > 0 or attachRes.state.isNone:
        continue
      stateOpt = attachRes.state
    inc gateway.acceptedConnections
    asyncSpawn gateway.serveConnection(event.connection, stateOpt.get())

proc start*(gateway: TsnetQuicControlGateway): Result[void, string] =
  if gateway.isNil:
    return err("nim_quic gateway is nil")
  if gateway.running:
    return ok()
  if gateway.cfg.openNetwork and gateway.openState.isNil:
    gateway.openState = initOpenNetworkState()
  if gateway.cfg.certificatePem.strip().len == 0 or gateway.cfg.privateKeyPem.strip().len == 0:
    return err("nim_quic gateway requires certificatePem and privateKeyPem")
  var cfg = msquicdrv.MsQuicTransportConfig(
    alpns: @[NimTsnetQuicAlpn],
    appName: "nim-tsnet-control-quic"
  )
  applyRuntimePreference(cfg)
  let (handle, initErr) = msquicdrv.initMsQuicTransport(cfg)
  if handle.isNil:
    return err("failed to initialize nim_quic gateway runtime: " & initErr)
  let serverTlsCfg = mstlstypes.TlsConfig(
    role: mstlstypes.tlsServer,
    alpns: @[NimTsnetQuicAlpn],
    transportParameters: @[],
    serverName: none(string),
    certificatePem: some(gateway.cfg.certificatePem),
    privateKeyPem: some(gateway.cfg.privateKeyPem),
    certificateFile: none(string),
    privateKeyFile: none(string),
    privateKeyPassword: none(string),
    pkcs12File: none(string),
    pkcs12Data: none(seq[uint8]),
    pkcs12Password: none(string),
    certificateHash: none(mstlstypes.TlsCertificateHash),
    certificateStore: none(string),
    certificateStoreFlags: 0'u32,
    certificateContext: none(pointer),
    caCertificateFile: none(string),
    resumptionTicket: none(seq[uint8]),
    enableZeroRtt: false,
    useSharedSessionCache: false,
    disableCertificateValidation: false,
    requireClientAuth: false,
    enableOcsp: false,
    indicateCertificateReceived: false,
    deferCertificateValidation: false,
    useBuiltinCertificateValidation: false,
    allowedCipherSuites: none(uint32),
    tempDirectory: none(string)
  )
  let credErr = msquicdrv.loadCredential(handle, serverTlsCfg)
  if credErr.len > 0:
    safeShutdownRuntime(handle)
    return err("failed to load nim_quic gateway credential: " & credErr)
  let created = msquicdrv.createListener(handle)
  if created.error.len > 0 or created.state.isNone or created.listener.isNil:
    safeShutdownRuntime(handle)
    return err("failed to create nim_quic gateway listener: " & created.error)
  var storage: Sockaddr_storage
  var sockLen: SockLen
  try:
    let bindAddr = initTAddress(gateway.cfg.listenHost, Port(gateway.cfg.listenPort))
    toSAddr(bindAddr, storage, sockLen)
  except CatchableError as exc:
    msquicdrv.closeListener(handle, created.listener, created.state.get())
    safeShutdownRuntime(handle)
    return err("failed to bind nim_quic gateway listener: " & exc.msg)
  let startErr = msquicdrv.startListener(
    handle,
    created.listener,
    address = addr storage
  )
  if startErr.len > 0:
    msquicdrv.closeListener(handle, created.listener, created.state.get())
    safeShutdownRuntime(handle)
    return err("failed to start nim_quic gateway listener: " & startErr)
  gateway.handle = handle
  gateway.listener = created.listener
  gateway.listenerState = created.state.get()
  gateway.running = true
  let addressRes = msquicdrv.getListenerAddress(handle, created.listener)
  if addressRes.isOk():
    gateway.boundPort = uint16(addressRes.get().port)
  gateway.acceptLoop = gateway.listenerLoop()
  asyncSpawn gateway.acceptLoop
  ok()

proc stop*(gateway: TsnetQuicControlGateway) =
  if gateway.isNil or not gateway.running:
    return
  gateway.running = false
  if not gateway.handle.isNil and not gateway.listener.isNil:
    discard msquicdrv.stopListener(gateway.handle, gateway.listener)
    msquicdrv.closeListener(gateway.handle, gateway.listener, gateway.listenerState)
  safeShutdownRuntime(gateway.handle)
  gateway.handle = nil
  gateway.listener = nil
  gateway.listenerState = nil
  gateway.acceptLoop = nil

proc cachedQuicClient(
    controlUrl: string
): Result[TsnetQuicRpcClient, string] =
  let endpoint = quicEndpoint(controlUrl).valueOr:
    return err(error)
  let cached = quicClientCache.getOrDefault(endpoint.url, nil)
  if canReuseCachedQuicClient(cached):
    quicClientDebug("reusing cached connection for " & endpoint.url)
    return ok(cached)
  if not cached.isNil:
    let handshakeComplete =
      if cached.connState.isNil: false
      else: msquicdrv.connectionHandshakeComplete(cached.connState)
    quicClientDebug(
      "discarding stale cached connection for " & endpoint.url &
      " closed=" & $(
        if cached.connState.isNil: true
        else: cached.connState.closed
      ) &
      " handshakeComplete=" & $handshakeComplete
    )
    discardCachedQuicClient(endpoint.url)
  let dialHost = resolveDialHost(endpoint.host, endpoint.port).valueOr:
    return err(error)
  let dialFamily = detectAddressFamily(dialHost)

  var cfg = msquicdrv.MsQuicTransportConfig(
    alpns: @[NimTsnetQuicAlpn],
    appName: "nim-tsnet-control-quic"
  )
  applyRuntimePreference(cfg)
  let (handle, initErr) = msquicdrv.initMsQuicTransport(cfg)
  if handle.isNil:
    return err("failed to initialize nim_quic control runtime: " & initErr)

  let clientTlsCfg = mstlstypes.TlsConfig(
    role: mstlstypes.tlsClient,
    alpns: @[NimTsnetQuicAlpn],
    transportParameters: @[],
    serverName: tlsServerName(endpoint.host),
    certificatePem: none(string),
    privateKeyPem: none(string),
    certificateFile: none(string),
    privateKeyFile: none(string),
    privateKeyPassword: none(string),
    pkcs12File: none(string),
    pkcs12Data: none(seq[uint8]),
    pkcs12Password: none(string),
    certificateHash: none(mstlstypes.TlsCertificateHash),
    certificateStore: none(string),
    certificateStoreFlags: 0'u32,
    certificateContext: none(pointer),
    caCertificateFile: none(string),
    resumptionTicket: none(seq[uint8]),
    enableZeroRtt: false,
    useSharedSessionCache: false,
    disableCertificateValidation: true,
    requireClientAuth: false,
    enableOcsp: false,
    indicateCertificateReceived: false,
    deferCertificateValidation: false,
    useBuiltinCertificateValidation: false,
    allowedCipherSuites: none(uint32),
    tempDirectory: none(string)
  )
  let credErr = msquicdrv.loadCredential(handle, clientTlsCfg)
  if credErr.len > 0:
    safeShutdownRuntime(handle)
    return err("failed to load nim_quic client credential: " & credErr)

  let dialed = msquicdrv.dialConnection(
    handle,
    dialHost,
    endpoint.port,
    addressFamily = dialFamily
  )
  quicClientDebug(
    "dial endpoint=" & endpoint.url &
    " dialHost=" & dialHost &
    " family=" & $dialFamily &
    " runtimePreference=" & $quicControlRuntimePreference() &
    " tlsServerName=" &
    (if clientTlsCfg.serverName.isSome: clientTlsCfg.serverName.get() else: "<none>")
  )
  if dialed.error.len > 0 or dialed.connection.isNil or dialed.state.isNone:
    safeShutdownRuntime(handle)
    return err("failed to dial nim_quic control endpoint " & endpoint.url & ": " & dialed.error)
  let connPtr = dialed.connection
  let connState = dialed.state.get()

  var dialState = (false, "unknown")
  try:
    dialState = waitFor awaitConnected(connState)
  except CancelledError:
    dialState = (false, "nim_quic dial cancelled")
  except CatchableError as exc:
    dialState = (false, "nim_quic dial wait raised: " & exc.msg)
  if not dialState[0]:
    if dialState[1].startsWith("timeout waiting for nim_quic connection event"):
      if not msquicdrv.connectionHandshakeComplete(connState):
        safeShutdownConnection(handle, connPtr)
        safeCloseConnection(handle, connPtr, connState)
        safeShutdownRuntime(handle)
        return err(
          dialState[1] & " and handshake did not complete for " & endpoint.url
        )
      quicClientDebug(
        "continuing without connected event for " & endpoint.url &
        " reason=" & dialState[1]
      )
    else:
      safeShutdownConnection(handle, connPtr)
      safeCloseConnection(handle, connPtr, connState)
      safeShutdownRuntime(handle)
      let reason =
        if dialState[1].len > 0: dialState[1]
        else: "nim_quic dial failed before connection"
      return err(reason & " for " & endpoint.url)
  let client = TsnetQuicRpcClient(
    endpoint: endpoint,
    handle: handle,
    connPtr: connPtr,
    connState: connState
  )
  quicClientCache[endpoint.url] = client
  ok(client)

proc executeSingleQuicRequest(
    controlUrl: string,
    request: JsonNode
): Result[JsonNode, string] =
  let client = cachedQuicClient(controlUrl).valueOr:
    return err(error)

  let stream = createBidiStream(client.handle, client.connPtr, client.connState).valueOr:
    return err(error)
  defer:
    try:
      waitFor noCancel stream.closeImpl()
    except CatchableError:
      discard
  try:
    quicClientDebug("stream created; sending request")
    let requestFuture = proc(): Future[JsonNode] {.async.} =
      await writeJsonMessage(stream, request)
      # This RPC is exactly one request per stream; half-close the write side
      # so peers that wait for FIN can start processing immediately.
      await stream.closeWrite()
      quicClientDebug("request sent; awaiting response")
      await readJsonMessage(stream)
    let responseFut = requestFuture()
    let timeoutFut = sleepAsync(QuicControlRequestTimeout)
    let winner = waitFor race(cast[FutureBase](responseFut), cast[FutureBase](timeoutFut))
    if winner == cast[FutureBase](timeoutFut):
      responseFut.cancel()
      discardCachedQuicClient(client.endpoint.url)
      return err(
        "nim_quic request timed out at " & client.endpoint.url &
        " after " & $QuicControlRequestTimeout
      )
    timeoutFut.cancel()
    let responseNode = waitFor responseFut
    quicClientDebug("response received=" & $responseNode)
    let payload = responsePayload(responseNode).valueOr:
      discardCachedQuicClient(client.endpoint.url)
      return err(error)
    ok(payload)
  except CatchableError as exc:
    quicClientDebug("request error=" & exc.msg)
    discardCachedQuicClient(client.endpoint.url)
    err("nim_quic request failed at " & client.endpoint.url & ": " & exc.msg)

proc executeQuicRequest(
    controlUrl: string,
    request: JsonNode
): Result[JsonNode, string] =
  var failures: seq[string] = @[]
  for attempt in 0 ..< QuicControlRequestAttempts:
    let response = executeSingleQuicRequest(controlUrl, request)
    if response.isOk():
      return response
    failures.add(response.error)
    quicClientDebug("attempt " & $(attempt + 1) & " failed: " & response.error)
    let endpoint = quicEndpoint(controlUrl).valueOr:
      return err(error)
    discardCachedQuicClient(endpoint.url)
    if attempt + 1 < QuicControlRequestAttempts:
      try:
        waitFor sleepAsync(QuicControlRetryBackoff)
      except CancelledError:
        discard
  if failures.len == 0:
    return err("nim_quic request failed")
  err(failures.join("; "))

proc fetchControlServerKeyQuic*(
    controlUrl: string,
    capabilityVersion: int
): Result[TsnetControlServerKey, string] =
  let payload = executeQuicRequest(
    controlUrl,
    buildRpcRequest("key", capabilityVersion = capabilityVersion)
  ).valueOr:
    return err(error)
  parseControlServerKey(
    payload,
    capabilityVersion = capabilityVersion
  )

proc fetchControlServerKeyQuicSafe*(
    controlUrl: string
): Result[TsnetControlServerKey, string] =
  var failures: seq[string] = @[]
  for version in TsnetDefaultControlCapabilityVersions:
    let fetched = fetchControlServerKeyQuic(controlUrl, version)
    if fetched.isOk():
      return fetched
    failures.add(fetched.error)
  if failures.len == 0:
    return err("no nim_quic control capability versions were provided")
  err(failures.join("; "))

proc registerNodeQuic*(
    controlUrl: string,
    request: JsonNode
): Result[JsonNode, string] =
  executeQuicRequest(
    controlUrl,
    buildRpcRequest("register", requestPayload = request)
  )

proc mapPollQuic*(
    controlUrl: string,
    request: JsonNode
): Result[JsonNode, string] =
  executeQuicRequest(
    controlUrl,
    buildRpcRequest("map", requestPayload = request)
  )

proc healthQuic*(
    controlUrl: string
): Result[JsonNode, string] =
  executeQuicRequest(
    controlUrl,
    buildRpcRequest("health")
  )

proc quicControlTransport*(): TsnetControlTransport =
  let noiseUrlProc: TsnetControlUrlProc =
    proc(controlUrl: string): string {.closure, gcsafe, raises: [].} =
      nimQuicKeyUrl(controlUrl, TsnetDefaultControlCapabilityVersions[0])
  let keyUrlProc: TsnetControlVersionedUrlProc =
    proc(controlUrl: string, capabilityVersion: int): string {.closure, gcsafe, raises: [].} =
      nimQuicKeyUrl(controlUrl, capabilityVersion)
  let registerUrlProc: TsnetControlUrlProc =
    proc(controlUrl: string): string {.closure, gcsafe, raises: [].} =
      nimQuicRegisterUrl(controlUrl)
  let mapUrlProc: TsnetControlUrlProc =
    proc(controlUrl: string): string {.closure, gcsafe, raises: [].} =
      nimQuicMapUrl(controlUrl)
  let fetchProc: TsnetControlKeyFetchProc =
    proc(controlUrl: string): Result[TsnetControlServerKey, string] {.closure, gcsafe, raises: [].} =
      var res: Result[TsnetControlServerKey, string]
      quicSafe:
        res = fetchControlServerKeyQuicSafe(controlUrl)
      res
  let registerProc: TsnetControlRegisterProc =
    proc(controlUrl: string, request: JsonNode): Result[JsonNode, string] {.closure, gcsafe, raises: [].} =
      var res: Result[JsonNode, string]
      quicSafe:
        res = registerNodeQuic(controlUrl, request)
      res
  let mapProc: TsnetControlMapPollProc =
    proc(controlUrl: string, request: JsonNode): Result[JsonNode, string] {.closure, gcsafe, raises: [].} =
      var res: Result[JsonNode, string]
      quicSafe:
        res = mapPollQuic(controlUrl, request)
      res
  TsnetControlTransport.init(
    protocolLabel = "nim_quic",
    noiseUrlProc = noiseUrlProc,
    keyUrlProc = keyUrlProc,
    registerUrlProc = registerUrlProc,
    mapUrlProc = mapUrlProc,
    fetchServerKeyProc = fetchProc,
    registerNodeProc = registerProc,
    mapPollProc = mapProc
  )
