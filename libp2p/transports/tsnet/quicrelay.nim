{.push raises: [].}

import chronos
import chronos/osdefs
import std/[json, locks, nativesockets, options, os, posix, sequtils, sets, strutils, tables, uri]
from std/times import epochTime

import ../../multiaddress
import ../../stream/lpstream
import ../../utility
import ../../wire
import ../tsnetprovidertypes
import ../quicruntime as qrt
import ../msquicdriver as msquicdrv
import ../msquicstream
import "../nim-msquic/api/event_model" as msevents
import "../nim-msquic/tls/common" as mstlstypes
import ./control
import ./proxy

template quicRelaySafe(body: untyped) =
  {.cast(gcsafe).}:
    body

const
  NimTsnetQuicRelayDefaultPort* = 9446'u16
  NimTsnetQuicRelayPrefix* = "/nim-tsnet-relay-quic/v1"
  NimTsnetQuicRelayAlpn* = "nim-tsnet-relay-quic"
  RelayDialTaskRoutePrefix = "dial:"
  RelayFrameBufferSize = 64 * 1024
  RelayDialRetryWindow = 45.seconds
  RelayDialRetryStep = 100.milliseconds
  RelayListenerStartupStagger = 2.seconds
  QuicRelayConnectEventTimeout = 1.seconds
  QuicRelayConnectMaxEvents = 8
  QuicRelayConnectAttempts = 8
  QuicRelayConnectRetryBackoff = 250.milliseconds
  QuicRelayWriteDrainDelay = 25.milliseconds
  QuicRelayGatewayResponseDrainDelay = 500.milliseconds
  QuicRelayRpcTimeout = 10.seconds
  QuicRelayDialAckTimeout = 35.seconds
  QuicRelayListenerReadyTimeout = 30.seconds
  QuicRelayAcceptAckTimeout = 10.seconds
  QuicRelayAcceptAwaitTimeout = 5.seconds
  QuicRelayIncomingWaitTimeout = 30.seconds
  QuicRelayIncomingRevalidateInterval = 2.seconds
  QuicRelayReadinessPingAttempts = 3
  QuicRelayReadinessRetryDelay = 500.milliseconds
  QuicRelayRoutePublishAttempts = 10
  QuicRelayRoutePublishRetryDelay = 250.milliseconds
  QuicRelayKeepAliveInterval = 5.seconds
  QuicRelayKeepAliveAckTimeout = 5.seconds
  QuicRelayHandshakeIdleTimeoutMs = 30_000'u64
  QuicRelayIdleTimeoutMs = 120_000'u64
  QuicRelayKeepAliveIntervalMs = 5_000'u32
  QuicRelayReadRetryDelay = 50.milliseconds

type
  Ifaddrs {.importc: "struct ifaddrs", header: "<ifaddrs.h>", bycopy.} = object
    ifa_next: ptr Ifaddrs
    ifa_name: cstring
    ifa_flags: cuint
    ifa_addr: ptr SockAddr
    ifa_netmask: ptr SockAddr
    ifa_dstaddr: ptr SockAddr
    ifa_data: pointer

  NimStrPayloadShim = object
    cap: int
    data: UncheckedArray[char]

  NimStringV2Shim = object
    len: int
    p: ptr NimStrPayloadShim

  TsnetQuicRelayEndpoint* = object
    url*: string
    host*: string
    authority*: string
    port*: uint16
    prefix*: string

  TsnetRelayTaskHandle = ref object
    routeId: int
    route: string
    task: Future[void]
    server: StreamServer
    datagram: DatagramTransport

  TsnetUdpDialDiagnostic* = object
    phase*: string
    detail*: string
    attempts*: int
    updatedUnixMilli*: int64

  TsnetRelayListenerDiagnostic* = object
    route*: string
    kind*: string
    stage*: string
    detail*: string
    updatedUnixMilli*: int64

  TsnetRelayListenerState = object
    kind: string
    stage: string
    detail: string
    updatedUnixMilli: int64

  TsnetRelayListenerStageSnapshot* = object
    route*: string
    stage*: string

  TsnetQuicRelayListener = ref object
    connPtr: pointer
    connState: msquicdrv.MsQuicConnectionState
    candidates: seq[string]
    stream: MsQuicStream
    generation: string
    persistentControlStream: bool
    published: bool
    awaiting: bool
    readyFuture: Future[JsonNode]
    busy: bool

  TsnetQuicRelayPendingBridge = ref object
    route: string
    kind: string
    stream: MsQuicStream
    future: Future[MsQuicStream]

  TsnetQuicRelayGateway* = ref object
    handle*: msquicdrv.MsQuicTransportHandle
    listener*: pointer
    listenerState*: msquicdrv.MsQuicListenerState
    running*: bool
    acceptLoop*: Future[void]
    boundPort*: uint16
    listenersLock: Lock
    listenersLockInit: bool
    listeners: Table[string, TsnetQuicRelayListener]
    listenerGenerationCounter: uint64
    pendingBridgeCounter: uint64
    pendingBridges: Table[string, TsnetQuicRelayPendingBridge]

  TsnetQuicRelayClient = ref object
    endpoint: TsnetQuicRelayEndpoint
    handle: msquicdrv.MsQuicTransportHandle
    connPtr: pointer
    connState: msquicdrv.MsQuicConnectionState
    activeBridgeCount: int
    bridgeDrainFuture: Future[void]

  TsnetQuicRelayProbeResult* = object
    ok*: bool
    relayUrl*: string
    route*: string
    runtimePreference*: string
    runtimeImplementation*: string
    runtimePath*: string
    runtimeKind*: string
    connected*: bool
    acknowledged*: bool
    error*: string

  TsnetQuicRelayCandidateExchangeResult* = object
    ok*: bool
    relayUrl*: string
    route*: string
    listenerCandidates*: seq[string]
    dialerCandidates*: seq[string]
    error*: string

var relayRegistryLock {.global.}: Lock
var relayConfigLock {.global.}: Lock
var relayNextRouteId {.global.}: int
var relayOwnerTasks {.global.}: Table[int, Table[int, TsnetRelayTaskHandle]] =
  initTable[int, Table[int, TsnetRelayTaskHandle]]()
var relayOwnerRouteTaskIds {.global.}: Table[int, Table[string, int]] =
  initTable[int, Table[string, int]]()
var relayOwnerClients {.global.}: Table[int, Table[string, TsnetQuicRelayClient]] =
  initTable[int, Table[string, TsnetQuicRelayClient]]()
var relayOwnerEpochs {.global.}: Table[int, int] = initTable[int, int]()
var relayOwnerActiveRoutes {.global.}: Table[int, Table[string, int]] =
  initTable[int, Table[string, int]]()
var relayReadyRoutes {.global.}: HashSet[string] = initHashSet[string]()
var relayLocallyUsableRoutes {.global.}: HashSet[string] = initHashSet[string]()
var relayPendingRoutes {.global.}: HashSet[string] = initHashSet[string]()
var relayAwaitingRoutes {.global.}: HashSet[string] = initHashSet[string]()
var relayIncomingRoutes {.global.}: HashSet[string] = initHashSet[string]()
var relayReadyStageRoutes {.global.}: HashSet[string] = initHashSet[string]()
var relayBridgeAttachedRoutes {.global.}: HashSet[string] = initHashSet[string]()
var relayServingRoutes {.global.}: HashSet[string] = initHashSet[string]()
var relayRepairRequestedRoutes {.global.}: HashSet[string] = initHashSet[string]()
var relayKnownRoutes {.global.}: HashSet[string] = initHashSet[string]()
var relayDroppedRoutes {.global.}: HashSet[string] = initHashSet[string]()
var relayUdpDialStates {.global.}: Table[int, Table[string, TsnetUdpDialDiagnostic]] =
  initTable[int, Table[string, TsnetUdpDialDiagnostic]]()
var relayListenerStates {.global.}: Table[int, Table[string, TsnetRelayListenerState]] =
  initTable[int, Table[string, TsnetRelayListenerState]]()

initLock(relayRegistryLock)
initLock(relayConfigLock)

proc c_getifaddrs(ifap: ptr ptr Ifaddrs): cint {.importc: "getifaddrs", header: "<ifaddrs.h>".}
proc c_freeifaddrs(ifa: ptr Ifaddrs) {.importc: "freeifaddrs", header: "<ifaddrs.h>".}

proc bytesToString(bytes: openArray[byte]): string =
  result = newString(bytes.len)
  for i, value in bytes:
    result[i] = char(value)

proc ownedText(text: string): string {.gcsafe, raises: [].} =
  let textLen =
    when defined(nimSeqsV2):
      let textHeader = cast[ptr NimStringV2Shim](unsafeAddr text)
      if textHeader == nil or textHeader[].p == nil or textHeader[].len == 0:
        return ""
      textHeader[].len
    else:
      if text.len == 0:
        return ""
      text.len
  if textLen == 0:
    return ""
  result = newString(textLen)
  copyMem(addr result[0], unsafeAddr text[0], textLen)

proc ownedTextSeq(items: openArray[string]): seq[string] {.gcsafe, raises: [].} =
  result = newSeqOfCap[string](items.len)
  for item in items:
    result.add(ownedText(item))

proc ownedRelayEndpoint(
    endpoint: TsnetQuicRelayEndpoint
): TsnetQuicRelayEndpoint {.gcsafe, raises: [].} =
  TsnetQuicRelayEndpoint(
    url: ownedText(endpoint.url),
    host: ownedText(endpoint.host),
    authority: ownedText(endpoint.authority),
    port: endpoint.port,
    prefix: ownedText(endpoint.prefix)
  )

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

proc splitCompleteFramedPayload(
    payload: seq[byte]
): Option[tuple[frame: seq[byte], rest: seq[byte]]] {.gcsafe, raises: [].} =
  if payload.len < 4:
    return none(tuple[frame: seq[byte], rest: seq[byte]])
  let frameLen = int(readUint32BE(payload, 0))
  if frameLen < 0:
    return none(tuple[frame: seq[byte], rest: seq[byte]])
  let frameEnd = 4 + frameLen
  if payload.len < frameEnd:
    return none(tuple[frame: seq[byte], rest: seq[byte]])
  let frame =
    if frameLen == 0:
      @[]
    else:
      payload.toOpenArray(4, frameEnd - 1).toSeq()
  let rest =
    if payload.len > frameEnd:
      payload[frameEnd ..< payload.len]
    else:
      @[]
  some((frame: frame, rest: rest))

proc defaultRelayPort(parsed: Uri): uint16 =
  if parsed.port.len > 0:
    try:
      return uint16(max(1, parsed.port.parseInt()))
    except CatchableError:
      discard
  NimTsnetQuicRelayDefaultPort

proc explicitRelayPort(parsed: Uri): uint16 {.gcsafe, raises: [].} =
  let normalizedPath = parsed.path.strip()
  if normalizedPath == NimTsnetQuicRelayPrefix:
    return defaultRelayPort(parsed)
  NimTsnetQuicRelayDefaultPort

proc normalizedRelayKind(kind, route: string): string {.gcsafe, raises: [].} =
  let lowered = kind.strip().toLowerAscii()
  if lowered == "udp":
    return "udp"
  if lowered == "tcp":
    return "tcp"
  let routeLower = route.toLowerAscii()
  if "/udp/" in routeLower and "/quic-v1" in routeLower:
    return "udp"
  "tcp"

proc relayReadyRouteKey(ownerId: int, route: string): string =
  let normalizedRoute = route.strip()
  if ownerId <= 0 or normalizedRoute.len == 0:
    ""
  else:
    $ownerId & "|" & normalizedRoute

proc relayRouteCanRequestRepair(route: string): bool =
  let normalizedRoute = route.strip()
  normalizedRoute.len > 0 and
    not normalizedRoute.startsWith(RelayDialTaskRoutePrefix)

proc clearRelayRouteKeysForOwnerLocked(routes: var HashSet[string], ownerId: int) =
  if ownerId <= 0:
    return
  let prefix = $ownerId & "|"
  var staleKeys: seq[string] = @[]
  for key in routes.items:
    if key.startsWith(prefix):
      staleKeys.add(key)
  for key in staleKeys:
    routes.excl(key)

proc collectRelayOwnerRoutesLocked(ownerId: int): seq[string] =
  if ownerId <= 0:
    return @[]
  let prefix = $ownerId & "|"
  var seen = initHashSet[string]()
  template rememberRoute(routeExpr: untyped) =
    let route = routeExpr
    let normalized = route.strip()
    if normalized.len == 0 or normalized in seen:
      discard
    else:
      seen.incl(normalized)
      result.add(ownedText(normalized))

  template rememberRoutesFromKeys(routesSet: untyped) =
    for key in routesSet.items:
      if not key.startsWith(prefix):
        continue
      let routeStart = prefix.len
      if routeStart >= key.len:
        continue
      rememberRoute(key[routeStart .. ^1])

  relayOwnerRouteTaskIds.withValue(ownerId, routeIds):
    for route in routeIds[].keys:
      rememberRoute(route)

  rememberRoutesFromKeys(relayKnownRoutes)
  rememberRoutesFromKeys(relayPendingRoutes)
  rememberRoutesFromKeys(relayAwaitingRoutes)
  rememberRoutesFromKeys(relayIncomingRoutes)
  rememberRoutesFromKeys(relayReadyStageRoutes)
  rememberRoutesFromKeys(relayBridgeAttachedRoutes)
  rememberRoutesFromKeys(relayServingRoutes)
  rememberRoutesFromKeys(relayReadyRoutes)
  rememberRoutesFromKeys(relayLocallyUsableRoutes)
  rememberRoutesFromKeys(relayDroppedRoutes)

proc relayOwnerEpochMatchesLocked(ownerId: int, ownerEpoch: int): bool =
  ownerId > 0 and relayOwnerEpochs.getOrDefault(ownerId, 0) == ownerEpoch

proc closeOwnerRelayClients(ownerId: int) {.gcsafe, raises: [].}

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

proc tlsServerName(host: string): Option[string] {.gcsafe, raises: [].} =
  if host.len == 0:
    return none(string)
  let suppressIpSni =
    getEnv("NIM_TSNET_RELAY_QUIC_NO_SNI", "").strip().toLowerAscii() in
      ["1", "true", "yes"]
  if suppressIpSni and detectAddressFamily(host) != 0'u16:
    return none(string)
  some(host)

proc isLoopbackRelayHost(host: string): bool {.gcsafe, raises: [].}

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

proc relayRuntimePreference(
    defaultPreference: qrt.QuicRuntimePreference
): qrt.QuicRuntimePreference =
  var cachedReady {.global.}: bool
  var cachedPreference {.global.}: qrt.QuicRuntimePreference
  withLock(relayConfigLock):
    if cachedReady:
      return cachedPreference
  let parsed = parseRuntimePreference(getEnv("NIM_TSNET_RELAY_QUIC_RUNTIME", "")).get(defaultPreference)
  withLock(relayConfigLock):
    if not cachedReady:
      cachedPreference = parsed
      cachedReady = true
    return cachedPreference

proc resolveDialHost(host: string, port: uint16): Result[string, string] {.gcsafe, raises: [].} =
  if host.len == 0:
    return err("nim_quic relay host is empty")
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
        return ok(resolved)
    except CatchableError:
      discard
  err("failed to resolve nim_quic relay host " & host)

proc resolveDialHostForTest*(host: string, port: uint16): Result[string, string] {.gcsafe, raises: [].} =
  resolveDialHost(host, port)

proc quicRelayTrace(message: string) =
  try:
    stderr.writeLine("[tsnet-quicrelay] " & message)
    flushFile(stderr)
  except CatchableError:
    discard

proc currentUnixMilli(): int64 =
  int64(epochTime() * 1000.0)

proc runtimePreferenceLabel(pref: qrt.QuicRuntimePreference): string =
  case pref
  of qrt.qrpAuto:
    "auto"
  of qrt.qrpNativeOnly:
    "builtin_only"
  of qrt.qrpBuiltinPreferred:
    "builtin_only"
  of qrt.qrpBuiltinOnly:
    "builtin_only"

proc quicRelayEndpoint*(controlUrl: string): Result[TsnetQuicRelayEndpoint, string] =
  let normalized = normalizeControlUrl(controlUrl)
  if normalized.len == 0:
    return err("tsnet relay URL is empty")
  let parsed =
    try:
      parseUri(normalized)
    except CatchableError as exc:
      return err("failed to parse tsnet relay URL " & normalized & ": " & exc.msg)
  if parsed.hostname.len == 0:
    return err("tsnet relay URL is missing a hostname")
  let port =
    case parsed.scheme.toLowerAscii()
    of "https", "http", "quic":
      explicitRelayPort(parsed)
    else:
      defaultRelayPort(parsed)
  let authorityHost = bracketAuthorityHost(parsed.hostname)
  let authority = authorityHost & ":" & $port
  ok(TsnetQuicRelayEndpoint(
    url: ownedText("quic://" & authority & NimTsnetQuicRelayPrefix),
    host: ownedText(parsed.hostname),
    authority: ownedText(authority),
    port: port,
    prefix: ownedText(NimTsnetQuicRelayPrefix)
  ))

proc nimQuicRelayBaseUrl*(controlUrl: string): string =
  let endpoint = quicRelayEndpoint(controlUrl).valueOr:
    return ""
  endpoint.url

proc rpcError(message: string): JsonNode =
  %*{
    "ok": false,
    "error": message
  }

proc rpcSuccess(payload: JsonNode): JsonNode =
  %*{
    "ok": true,
    "payload":
      if payload.isNil: newJObject()
      else: payload
  }

proc jsonString(node: JsonNode, key: string): string =
  if node.isNil or node.kind != JObject or not node.hasKey(key):
    return ""
  let value = node.getOrDefault(key)
  if value.kind == JString:
    return value.getStr().strip()
  ""

proc jsonStrings(node: JsonNode, key: string): seq[string] =
  if node.isNil or node.kind != JObject or not node.hasKey(key):
    return @[]
  let value = node.getOrDefault(key)
  if value.kind != JArray:
    return @[]
  var seen = initHashSet[string]()
  for item in value.items():
    if item.kind != JString:
      continue
    let candidate = item.getStr().strip()
    if candidate.len == 0 or candidate in seen:
      continue
    seen.incl(candidate)
    result.add(candidate)

proc rpcPayload(node: JsonNode): JsonNode =
  if node.isNil or node.kind != JObject:
    return newJObject()
  let payload = node.getOrDefault("payload")
  if payload.isNil or payload.kind == JNull:
    return newJObject()
  payload

proc relayListenerGeneration(ack: JsonNode): string =
  if ack.isNil:
    return ""
  jsonString(rpcPayload(ack), "generation")

proc isDirectCandidateAddress(address: MultiAddress): bool =
  var parts: seq[string] = @[]
  for part in ($address).split('/'):
    if part.len > 0:
      parts.add(part)
  if parts.len < 2:
    return false
  let family = parts[0]
  let host = parts[1].strip().toLowerAscii()
  case family
  of "ip4":
    if host.len == 0 or host == "0.0.0.0":
      return false
    if host == "127.0.0.1" or host.startsWith("127."):
      return false
    true
  of "ip6":
    if host.len == 0 or host == "::":
      return false
    if host == "::1" or host == "0:0:0:0:0:0:0:1":
      return false
    true
  else:
    false

proc parseDirectCandidate(text: string): Option[MultiAddress] =
  let candidate = text.strip()
  if candidate.len == 0:
    return
  let parsed = MultiAddress.init(candidate)
  if parsed.isErr():
    return
  if not isDirectCandidateAddress(parsed.get()):
    return
  some(parsed.get())

proc isLoopbackRelayHost(host: string): bool {.gcsafe, raises: [].} =
  let normalized = host.strip().toLowerAscii()
  normalized == "localhost" or
    normalized == "127.0.0.1" or
    normalized.startsWith("127.") or
    normalized == "::1" or
    normalized == "0:0:0:0:0:0:0:1"

proc normalizeCandidateText(text: string): string =
  result = text.strip()
  let p2pIdx = result.find("/p2p/")
  if p2pIdx >= 0:
    result = result[0 ..< p2pIdx]
  if result.endsWith("/tsnet"):
    result.setLen(max(0, result.len - "/tsnet".len))
  result = result.strip()

proc appendRelayCandidate(
    candidates: var seq[string],
    seen: var HashSet[string],
    raw: string
) =
  let normalized = normalizeCandidateText(raw)
  if normalized.len == 0:
    return
  let parsed = parseDirectCandidate(normalized)
  if parsed.isNone():
    return
  let canonical = $parsed.get()
  if canonical.find("/udp/") < 0 or canonical.find("/quic-v1") < 0:
    return
  if canonical in seen:
    return
  seen.incl(canonical)
  candidates.add(canonical)

proc appendJsonRelayCandidates(
    candidates: var seq[string],
    seen: var HashSet[string],
    node: JsonNode
) =
  if node.isNil:
    return
  case node.kind
  of JString:
    appendRelayCandidate(candidates, seen, node.getStr())
  of JArray:
    for item in node.items():
      appendJsonRelayCandidates(candidates, seen, item)
  of JObject:
    if node.hasKey("multiaddr"):
      appendJsonRelayCandidates(candidates, seen, node.getOrDefault("multiaddr"))
    if node.hasKey("multiaddrs"):
      appendJsonRelayCandidates(candidates, seen, node.getOrDefault("multiaddrs"))
    if node.hasKey("endpoints"):
      appendJsonRelayCandidates(candidates, seen, node.getOrDefault("endpoints"))
  else:
    discard

proc relayCandidatesForRaw*(rawLocal: string, bridgeExtraJson = ""): seq[string] =
  var seen = initHashSet[string]()
  appendRelayCandidate(result, seen, rawLocal)

  let normalizedRaw = normalizeCandidateText(rawLocal)
  if normalizedRaw.len == 0 or normalizedRaw.find("/udp/") < 0 or normalizedRaw.find("/quic-v1") < 0:
    return result

  let rawAddress = MultiAddress.init(normalizedRaw).valueOr:
    return result
  let rawPort = portFromAddress(rawAddress)
  if rawPort <= 0:
    return result
  if bridgeExtraJson.strip().len == 0:
    return result

  let extras =
    try:
      parseJson(bridgeExtraJson)
    except CatchableError:
      return result

  let hostNetwork =
    if extras.kind == JObject and extras.hasKey("hostNetwork"):
      extras.getOrDefault("hostNetwork")
    else:
      newJNull()

  proc jsonNodeString(node: JsonNode, key: string): string =
    if node.kind == JObject and node.hasKey(key):
      let value = node.getOrDefault(key)
      if value.kind == JString:
        return value.getStr().strip()
    ""

  let publicIpv4 =
    if jsonNodeString(extras, "publicIpv4").len > 0:
      jsonNodeString(extras, "publicIpv4")
    else:
      jsonNodeString(hostNetwork, "publicIpv4")
  let publicIpv6 =
    if jsonNodeString(extras, "publicIpv6").len > 0:
      jsonNodeString(extras, "publicIpv6")
    else:
      jsonNodeString(hostNetwork, "publicIpv6")
  if publicIpv4.len > 0:
    appendRelayCandidate(
      result,
      seen,
      "/ip4/" & publicIpv4 & "/udp/" & $rawPort & "/quic-v1"
    )
  if publicIpv6.len > 0:
    appendRelayCandidate(
      result,
      seen,
      "/ip6/" & publicIpv6 & "/udp/" & $rawPort & "/quic-v1"
    )

  for key in [
      "directCandidates",
      "direct_candidates",
      "candidates",
      "candidateAddrs",
      "candidate_addrs",
      "lanEndpoints",
      "lan_endpoints"
    ]:
    if extras.kind == JObject and extras.hasKey(key):
      appendJsonRelayCandidates(result, seen, extras.getOrDefault(key))
  appendJsonRelayCandidates(result, seen, hostNetwork)

proc currentRuntimeInfoSafe(): qrt.QuicRuntimeInfo {.gcsafe, raises: [].} =
  {.cast(gcsafe).}:
    qrt.currentQuicRuntimeInfo()

proc safeCloseStream(
    handle: msquicdrv.MsQuicTransportHandle,
    streamPtr: pointer,
    streamState: msquicdrv.MsQuicStreamState
) {.gcsafe, raises: [].} =
  if handle.isNil or streamPtr.isNil or streamState.isNil:
    return
  try:
    quicRelaySafe:
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
    quicRelaySafe:
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
  let rootedState = connState
  GC_ref(rootedState)
  try:
    quicRelaySafe:
      msquicdrv.closeConnection(handle, connPtr, rootedState)
  except CatchableError:
    discard
  finally:
    GC_unref(rootedState)

proc safeShutdownRuntime(handle: msquicdrv.MsQuicTransportHandle) {.gcsafe, raises: [].} =
  if handle.isNil:
    return
  try:
    quicRelaySafe:
      msquicdrv.shutdown(handle)
  except CatchableError:
    discard

proc rawSocketAddress(
    host: string,
    port: int,
    family: string,
    kind: TsnetProxyKind = TsnetProxyKind.Tcp
): Result[MultiAddress, string] =
  if family notin ["ip4", "ip6"]:
    return err("unsupported tsnet relay raw family " & family)
  let normalizedHost = host.strip().toLowerAscii()
  if normalizedHost.len == 0:
    return err("missing tsnet relay raw host")
  if port <= 0:
    return err("missing tsnet relay raw port")
  let text =
    case kind
    of TsnetProxyKind.Tcp:
      "/" & family & "/" & normalizedHost & "/tcp/" & $port
    of TsnetProxyKind.Quic:
      "/" & family & "/" & normalizedHost & "/udp/" & $port & "/quic-v1"
  MultiAddress.init(text)

proc rawSocketAddress(
    localAddress: string,
    family: string,
    kind: TsnetProxyKind = TsnetProxyKind.Tcp
): Result[MultiAddress, string] =
  try:
    let ma = MultiAddress.init(localAddress).valueOr:
      return err("failed to parse tsnet relay local address: " & error)
    let parts = ($ma).split('/')
    if parts.len < 5:
      return err("failed to parse tsnet relay local address " & localAddress)
    rawSocketAddress(parts[2], parseInt(parts[4]), family, kind)
  except CatchableError as exc:
    err("failed to inspect tsnet relay raw socket address: " & exc.msg)

proc rawSocketAddress(
    localAddress: TransportAddress,
    family: string,
    kind: TsnetProxyKind = TsnetProxyKind.Tcp
): Result[MultiAddress, string] =
  try:
    let protocol =
      case kind
      of TsnetProxyKind.Tcp:
        IPPROTO_TCP
      of TsnetProxyKind.Quic:
        IPPROTO_UDP
    var ma = MultiAddress.init(localAddress, protocol).valueOr:
      return err("failed to convert tsnet relay local transport address: " & error)
    if kind == TsnetProxyKind.Quic:
      let quicSuffix = MultiAddress.init("/quic-v1").valueOr:
        return err("failed to build tsnet relay quic suffix: " & error)
      ma = concat(ma, quicSuffix).valueOr:
        return err("failed to append tsnet relay quic suffix: " & error)
    ok(ma)
  except CatchableError as exc:
    err("failed to inspect tsnet relay transport socket address: " & exc.msg)

proc rawSocketFromAddress(address: string): Result[(string, int, string), string] =
  let parsed = MultiAddress.init(address).valueOr:
    return err("invalid tsnet relay raw address " & address & ": " & error)
  let family = familyFromAddress(parsed)
  if family.len == 0:
    return err("unsupported tsnet relay raw family in " & address)
  let port = portFromAddress(parsed)
  if port <= 0:
    return err("unsupported tsnet relay raw port in " & address)
  let parts = ($parsed).split('/')
  if parts.len < 5:
    return err("unsupported tsnet relay raw address " & address)
  ok((parts[2], port, family))

proc relayLocalAddress(host: string, port: int): Result[TransportAddress, string] =
  try:
    ok(initTAddress(host, Port(port)))
  except CatchableError as exc:
    err("failed to build tsnet relay local address: " & exc.msg)

proc buildAdvertisedSource(
    family, ip: string,
    port: int,
    kind: TsnetProxyKind = TsnetProxyKind.Tcp
): string =
  case kind
  of TsnetProxyKind.Tcp:
    "/" & family & "/" & ip.strip().toLowerAscii() & "/tcp/" & $port & "/tsnet"
  of TsnetProxyKind.Quic:
    "/" & family & "/" & ip.strip().toLowerAscii() & "/udp/" & $port & "/quic-v1/tsnet"

proc safeCloseTransport(transp: StreamTransport) {.async: (raises: []).} =
  if transp.isNil:
    return
  try:
    await transp.closeWait()
  except CatchableError:
    discard

proc closeQuicRelayClient(client: TsnetQuicRelayClient) =
  if client.isNil:
    return
  safeShutdownConnection(client.handle, client.connPtr)
  safeCloseConnection(client.handle, client.connPtr, client.connState)
  safeShutdownRuntime(client.handle)
  client.handle = nil
  client.connPtr = nil
  client.connState = nil

proc retainRelayClientBridge(client: TsnetQuicRelayClient) {.gcsafe, raises: [].} =
  if client.isNil:
    return
  if client.activeBridgeCount <= 0:
    client.activeBridgeCount = 0
    if client.bridgeDrainFuture.isNil or client.bridgeDrainFuture.finished():
      client.bridgeDrainFuture = Future[void].init("tsnet.quicrelay.client.bridgeDrain")
  inc client.activeBridgeCount

proc releaseRelayClientBridge(client: TsnetQuicRelayClient) {.gcsafe, raises: [].} =
  if client.isNil or client.activeBridgeCount <= 0:
    return
  dec client.activeBridgeCount
  if client.activeBridgeCount != 0:
    return
  let drained = client.bridgeDrainFuture
  if not drained.isNil and not drained.finished():
    drained.complete()

proc waitRelayClientBridgeDrain(
    client: TsnetQuicRelayClient
) {.async: (raises: []).} =
  if client.isNil or client.activeBridgeCount <= 0:
    return
  let drained = client.bridgeDrainFuture
  if drained.isNil or drained.finished():
    return
  quicRelayTrace(
    "relay client shutdown waiting bridges endpoint=" & client.endpoint.url &
    " active=" & $client.activeBridgeCount
  )
  try:
    await drained
  except CatchableError:
    discard

proc closeQuicRelayClientAfterBridgeDrain(
    client: TsnetQuicRelayClient
) {.async: (raises: []).} =
  if client.isNil:
    return
  let handle = client.handle
  let connPtr = client.connPtr
  let connState = client.connState
  client.handle = nil
  client.connPtr = nil
  client.connState = nil
  safeShutdownConnection(handle, connPtr)
  await client.waitRelayClientBridgeDrain()
  safeCloseConnection(handle, connPtr, connState)
  safeShutdownRuntime(handle)
  client.bridgeDrainFuture = nil

proc relayClientBridgeDrainLifecycleForTests*(): tuple[
    futureCreated: bool,
    pendingAfterRetain: bool,
    pendingAfterFirstRelease: bool,
    finishedAfterSecondRelease: bool
  ] {.gcsafe, raises: [].} =
  let client = TsnetQuicRelayClient()
  retainRelayClientBridge(client)
  retainRelayClientBridge(client)
  let drained = client.bridgeDrainFuture
  result.futureCreated = not drained.isNil
  result.pendingAfterRetain = not drained.isNil and not drained.finished()
  releaseRelayClientBridge(client)
  result.pendingAfterFirstRelease = not drained.isNil and not drained.finished()
  releaseRelayClientBridge(client)
  result.finishedAfterSecondRelease = not drained.isNil and drained.finished()

proc relayHandshakeComplete(
    connState: msquicdrv.MsQuicConnectionState
): bool {.gcsafe, raises: [].}

proc relayClientUsable(client: TsnetQuicRelayClient): bool {.gcsafe, raises: [].} =
  not client.isNil and
    not client.handle.isNil and
    not client.connPtr.isNil and
    not client.connState.isNil and
    not client.connState.closed and
    (relayHandshakeComplete(client.connState) or
      isLoopbackRelayHost(client.endpoint.host))

proc relayHandshakeComplete(
    connState: msquicdrv.MsQuicConnectionState
): bool {.gcsafe, raises: [].} =
  if connState.isNil:
    return false
  quicRelaySafe:
    result = msquicdrv.connectionHandshakeComplete(connState)

proc relayCloseReason(
    connState: msquicdrv.MsQuicConnectionState
): string {.gcsafe, raises: [].} =
  if connState.isNil:
    return ""
  quicRelaySafe:
    result = msquicdrv.connectionCloseReason(connState)

proc safeStreamId(
    streamState: msquicdrv.MsQuicStreamState
): Result[uint64, string] {.gcsafe, raises: [].} =
  if streamState.isNil:
    return err("nil nim_quic relay stream state")
  try:
    quicRelaySafe:
      result = msquicdrv.streamId(streamState)
  except CatchableError as exc:
    return err("failed to query nim_quic relay stream id: " & exc.msg)

proc safeAttachIncomingConnection(
    handle: msquicdrv.MsQuicTransportHandle,
    connection: pointer
): tuple[state: Option[msquicdrv.MsQuicConnectionState], error: string]
    {.gcsafe, raises: [].} =
  try:
    quicRelaySafe:
      result = msquicdrv.attachIncomingConnection(handle, connection)
  except CatchableError as exc:
    return (
      none(msquicdrv.MsQuicConnectionState),
      "failed to attach incoming nim_quic relay connection: " & exc.msg
    )

proc awaitConnected(
    connState: msquicdrv.MsQuicConnectionState
): Future[(bool, string)] {.async.} =
  if connState.isNil:
    return (false, "nim_quic relay connection state unavailable")
  var attempt = 0
  while attempt < QuicRelayConnectMaxEvents:
    if relayHandshakeComplete(connState):
      return (true, "handshake_complete_poll")
    inc attempt
    let eventFuture = connState.nextQuicConnectionEvent()
    let timeoutFuture = sleepAsync(QuicRelayConnectEventTimeout)
    let winner = await race(cast[FutureBase](eventFuture), cast[FutureBase](timeoutFuture))
    if winner == cast[FutureBase](timeoutFuture):
      eventFuture.cancel()
      continue
    timeoutFuture.cancel()
    let event =
      try:
        await eventFuture
      except qrt.QuicRuntimeEventQueueClosed:
        return (false, "nim_quic relay connection event queue closed")
    case event.kind
    of qrt.qceConnected:
      return (true, "connected")
    of qrt.qceShutdownInitiated:
      let reason = relayCloseReason(connState)
      return (
        false,
        "nim_quic relay connection shutdown initiated" &
          (if reason.len > 0: " reason=" & reason else: "")
      )
    of qrt.qceShutdownComplete:
      let reason = relayCloseReason(connState)
      return (
        false,
        "nim_quic relay connection shutdown complete" &
          (if reason.len > 0: " reason=" & reason else: "")
      )
    else:
      discard
  if relayHandshakeComplete(connState):
    return (true, "handshake_complete_poll")
  (false, "timeout waiting for nim_quic relay connection event")

proc createBidiStream(
    handle: msquicdrv.MsQuicTransportHandle,
    connPtr: pointer,
    connState: msquicdrv.MsQuicConnectionState
): Result[MsQuicStream, string] {.gcsafe, raises: [].} =
  var created: tuple[stream: pointer, state: Option[msquicdrv.MsQuicStreamState], error: string]
  quicRelaySafe:
    created = msquicdrv.createStream(
      handle,
      connPtr,
      flags = 0'u32,
      connectionState = connState
    )
  if created.error.len > 0 or created.state.isNone or created.stream.isNil:
    return err(
      "failed to create nim_quic relay stream: " &
      (if created.error.len > 0: created.error else: "unknown error")
    )
  var startErr = ""
  quicRelaySafe:
    startErr = msquicdrv.startStream(handle, created.stream)
  if startErr.len > 0:
    safeCloseStream(handle, created.stream, created.state.get())
    return err("failed to start nim_quic relay stream: " & startErr)
  try:
    quicRelaySafe:
      result = ok(newMsQuicStream(created.state.get(), handle, Direction.Out))
  except LPStreamError as exc:
    safeCloseStream(handle, created.stream, created.state.get())
    result = err("failed to wrap nim_quic relay stream: " & exc.msg)

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
      quicRelayTrace("awaitIncomingBidiStream skipped local-initiated pending stream")
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
  var payload = stream.takeCachedBytes()
  let cachedFrame = splitCompleteFramedPayload(payload)
  if cachedFrame.isSome():
    let framed = cachedFrame.get()
    stream.restoreCachedBytes(framed.rest)
    return parseJson(bytesToString(framed.frame))
  while true:
    try:
      let chunk = await stream.read()
      if chunk.len == 0:
        break
      payload.add(chunk)
      let framed = splitCompleteFramedPayload(payload)
      if framed.isSome():
        let complete = framed.get()
        stream.restoreCachedBytes(complete.rest)
        return parseJson(bytesToString(complete.frame))
    except LPStreamEOFError:
      break
    except CatchableError as exc:
      let framed = splitCompleteFramedPayload(payload)
      if framed.isSome():
        let complete = framed.get()
        stream.restoreCachedBytes(complete.rest)
        return parseJson(bytesToString(complete.frame))
      raise exc
  let framed = splitCompleteFramedPayload(payload)
  if framed.isSome():
    let complete = framed.get()
    stream.restoreCachedBytes(complete.rest)
    return parseJson(bytesToString(complete.frame))
  stream.restoreCachedBytes(@[])
  if payload.len == 0:
    raise newException(
      IOError,
      "nim_quic relay stream closed before a complete JSON frame was received"
    )
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
  await stream.write(encodedBytes)
  if encodedBytes.len > 0:
    await sleepAsync(QuicRelayWriteDrainDelay)

proc writeJsonMessageWithTimeout(
    stream: MsQuicStream,
    payload: JsonNode,
    timeout: Duration,
    label: string
) {.async: (raises: [CancelledError, CatchableError]).} =
  let writeFut = writeJsonMessage(stream, payload)
  let timeoutFut = sleepAsync(timeout)
  let winner = await race(cast[FutureBase](writeFut), cast[FutureBase](timeoutFut))
  if winner == cast[FutureBase](timeoutFut):
    writeFut.cancelSoon()
    raise newException(
      IOError,
      "nim_quic relay timed out writing " & label & " after " & $timeout
    )
  timeoutFut.cancelSoon()
  await writeFut

proc readJsonMessageWithTimeout(
    stream: MsQuicStream,
    timeout: Duration,
    label: string
): Future[JsonNode] {.async: (raises: [CancelledError, CatchableError]).} =
  let readFut = readJsonMessage(stream)
  let timeoutFut = sleepAsync(timeout)
  let winner = await race(cast[FutureBase](readFut), cast[FutureBase](timeoutFut))
  if winner == cast[FutureBase](timeoutFut):
    readFut.cancelSoon()
    raise newException(
      IOError,
      "nim_quic relay timed out waiting for " & label & " after " & $timeout
    )
  timeoutFut.cancelSoon()
  await readFut

proc relayRouteStatus(
    client: TsnetQuicRelayClient,
    route: string,
    timeout: Duration = QuicRelayRpcTimeout
): Future[Result[bool, string]] {.async: (raises: [CancelledError]).}

proc markRelayListenerStage*(
    ownerId: int,
    route: string,
    kind: string,
    stage: string,
    detail = ""
) {.gcsafe, raises: [].}

proc ensureRelayRoutePublishedOnConnection(
    client: TsnetQuicRelayClient,
    route: string,
    attempts = QuicRelayRoutePublishAttempts,
    timeout: Duration = QuicRelayRpcTimeout
): Future[Result[void, string]] {.async: (raises: [CancelledError]).}

proc clearRelayReadyRoute(ownerId: int, route: string) {.gcsafe, raises: [].}
proc relayRouteReady*(ownerId: int, route: string): bool {.gcsafe, raises: [].}
proc markRelayRouteReady(ownerId: int, route: string) {.gcsafe, raises: [].}
proc relayOwnerHasActiveRouteTask*(ownerId: int, route: string): bool {.gcsafe, raises: [].}
proc relayShouldPreserveActiveRouteAfterProbeFailure*(
    preserveServingRoute: bool,
    routeLocallyReady: bool,
    error: string
): bool {.gcsafe, raises: [].}
proc relayRouteKnown(ownerId: int, route: string): bool {.gcsafe, raises: [].}
proc relayRouteServingNow(ownerId: int, route: string): bool {.gcsafe, raises: [].}
proc relayRouteDropped*(ownerId: int, route: string): bool {.gcsafe, raises: [].}
proc relayRouteStateLabel*(ownerId: int, route: string): string {.gcsafe, raises: [].}

proc listenerConnectionUsable(
    listener: TsnetQuicRelayListener
): bool {.gcsafe, raises: [].}

proc relayListenerStage(ownerId: int, route: string): string {.gcsafe, raises: [].} =
  relayRouteStateLabel(ownerId, route)

proc relayListenerHasStage*(ownerId: int, route: string): bool {.gcsafe, raises: [].} =
  relayRouteKnown(ownerId, route)

proc relayListenerStateRecord(
    route: string,
    kind: string,
    stage: string,
    detail = ""
): TsnetRelayListenerState =
  TsnetRelayListenerState(
    kind: ownedText(normalizedRelayKind(kind, route)),
    stage: ownedText(stage),
    detail: ownedText(detail),
    updatedUnixMilli: currentUnixMilli()
  )

proc relayListenerStageCountsAsReady(stage: string): bool =
  let lowered = stage.strip().toLowerAscii()
  lowered in ["awaiting", "incoming", "ready", "bridge_attached", "serving"]

proc relayListenerStageCountsAsPublished(stage: string): bool =
  let lowered = stage.strip().toLowerAscii()
  lowered in ["awaiting", "incoming", "ready", "bridge_attached", "serving"]

proc relayListenerStageLabelLocked(
    ownerId: int,
    route: string,
    fallbackStage = ""
): string =
  relayListenerStates.withValue(ownerId, states):
    let stored = states[].getOrDefault(route)
    let storedStage = stored.stage.strip()
    if storedStage.len > 0:
      return storedStage
  let key = relayReadyRouteKey(ownerId, route)
  if key.len == 0:
    return fallbackStage
  if key in relayDroppedRoutes:
    return "dropped"
  if key in relayBridgeAttachedRoutes:
    return "bridge_attached"
  if key in relayReadyStageRoutes:
    return "ready"
  if key in relayReadyRoutes or key in relayLocallyUsableRoutes:
    return "ready"
  if key in relayIncomingRoutes:
    return "incoming"
  if key in relayAwaitingRoutes:
    return "awaiting"
  if key in relayPendingRoutes:
    return "pending"
  if key in relayServingRoutes:
    if fallbackStage.len > 0:
      return fallbackStage
    return "serving"
  if key in relayKnownRoutes:
    if fallbackStage.len > 0:
      return fallbackStage
    return "known"
  fallbackStage

proc relayRouteCountsAsPublishedLocked(
    ownerId: int,
    route: string,
    fallbackStage = ""
): bool =
  let key = relayReadyRouteKey(ownerId, route)
  if key.len > 0:
    if key in relayReadyRoutes or key in relayLocallyUsableRoutes or
        key in relayAwaitingRoutes or key in relayIncomingRoutes or
        key in relayReadyStageRoutes or key in relayBridgeAttachedRoutes or
        key in relayServingRoutes:
      return true
  relayListenerStageCountsAsPublished(fallbackStage)

proc recordRelayListenerStateLocked(
    ownerId: int,
    route: string,
    kind: string,
    stage: string,
    detail = ""
) =
  if ownerId <= 0 or route.len == 0 or stage.len == 0:
    return
  relayListenerStates.mgetOrPut(
    ownerId,
    initTable[string, TsnetRelayListenerState]()
  )[ownedText(route)] = relayListenerStateRecord(route, kind, stage, detail)

proc relayRouteLocallyReady*(ownerId: int, route: string): bool {.gcsafe, raises: [].} =
  quicRelaySafe:
    let key = relayReadyRouteKey(ownerId, route)
    if key.len == 0:
      return false
    withLock(relayRegistryLock):
      result = key in relayReadyRoutes or key in relayLocallyUsableRoutes

proc relayRouteKnown(ownerId: int, route: string): bool {.gcsafe, raises: [].} =
  quicRelaySafe:
    let key = relayReadyRouteKey(ownerId, route)
    if key.len == 0:
      return false
    withLock(relayRegistryLock):
      result = key in relayKnownRoutes

proc relayRouteServingNow(ownerId: int, route: string): bool {.gcsafe, raises: [].} =
  quicRelaySafe:
    let key = relayReadyRouteKey(ownerId, route)
    if key.len == 0:
      return false
    withLock(relayRegistryLock):
      result = key in relayServingRoutes

proc relayRouteDropped*(ownerId: int, route: string): bool {.gcsafe, raises: [].} =
  quicRelaySafe:
    let key = relayReadyRouteKey(ownerId, route)
    if key.len == 0:
      return false
    withLock(relayRegistryLock):
      result = key in relayDroppedRoutes

proc relayRouteStateLabel*(ownerId: int, route: string): string {.gcsafe, raises: [].} =
  quicRelaySafe:
    let key = relayReadyRouteKey(ownerId, route)
    if key.len == 0:
      return ""
    withLock(relayRegistryLock):
      if key in relayDroppedRoutes:
        return "dropped"
      if key in relayBridgeAttachedRoutes:
        return "bridge_attached"
      if key in relayReadyStageRoutes:
        return "ready"
      if key in relayReadyRoutes or key in relayLocallyUsableRoutes:
        return "ready"
      if key in relayIncomingRoutes:
        return "incoming"
      if key in relayPendingRoutes:
        return "pending"
      if key in relayAwaitingRoutes:
        return "awaiting"
      if key in relayServingRoutes:
        return "serving"
      if key in relayKnownRoutes:
        return "known"
    return ""

proc relayShouldPreserveServingRoute(
    ownerId: int,
    route: string
): bool {.gcsafe, raises: [].} =
  relayRouteLocallyReady(ownerId, route) or
    relayOwnerHasActiveRouteTask(ownerId, route)

proc relayShouldPreserveUnpublishedServingRoute(
    ownerId: int,
    route: string
): bool {.gcsafe, raises: [].} =
  if not relayRouteServingNow(ownerId, route):
    return false
  relayRouteLocallyReady(ownerId, route) or
    relayOwnerHasActiveRouteTask(ownerId, route)

proc relayRouteServingStage*(ownerId: int, route: string): bool {.gcsafe, raises: [].} =
  quicRelaySafe:
    let key = relayReadyRouteKey(ownerId, route)
    if key.len == 0:
      return false
    withLock(relayRegistryLock):
      result =
        key in relayPendingRoutes or
        key in relayServingRoutes or
        key in relayIncomingRoutes or
        key in relayReadyStageRoutes or
        key in relayBridgeAttachedRoutes

proc relayRouteRepairRequested*(ownerId: int, route: string): bool {.gcsafe, raises: [].} =
  quicRelaySafe:
    let key = relayReadyRouteKey(ownerId, route)
    if key.len == 0:
      return false
    withLock(relayRegistryLock):
      result = key in relayRepairRequestedRoutes

proc clearRelayRouteRepairRequest*(
    ownerId: int,
    route: string
) {.gcsafe, raises: [].} =
  quicRelaySafe:
    let key = relayReadyRouteKey(ownerId, route)
    if key.len == 0:
      return
    withLock(relayRegistryLock):
      relayRepairRequestedRoutes.excl(key)

proc requestRelayRouteRepair*(
    ownerId: int,
    route: string
) {.gcsafe, raises: [].} =
  quicRelaySafe:
    let normalizedRoute = route.strip()
    let key = relayReadyRouteKey(ownerId, normalizedRoute)
    if key.len == 0 or not relayRouteCanRequestRepair(normalizedRoute):
      return
    withLock(relayRegistryLock):
      relayRepairRequestedRoutes.incl(key)

proc ensureRelayRoutePublished(
    endpoint: TsnetQuicRelayEndpoint,
    route: string,
    runtimePreference: qrt.QuicRuntimePreference = qrt.qrpBuiltinOnly,
    attempts = QuicRelayRoutePublishAttempts,
    timeout: Duration = QuicRelayRpcTimeout
): Result[void, string] {.gcsafe, raises: [].}

proc probeRelayRoutePublished(
    endpoint: TsnetQuicRelayEndpoint,
    route: string,
    runtimePreference: qrt.QuicRuntimePreference = qrt.qrpBuiltinOnly,
    timeout: Duration = QuicRelayRpcTimeout
): Result[bool, string] {.gcsafe, raises: [].}

proc connectRelayAsync(
    endpoint: TsnetQuicRelayEndpoint,
    runtimePreference: qrt.QuicRuntimePreference = qrt.qrpBuiltinOnly,
    allowWithoutConnected = true
): Future[Result[TsnetQuicRelayClient, string]] {.async: (raises: [CancelledError]).}

proc probeRelayRoutePublishedAsync(
    endpoint: TsnetQuicRelayEndpoint,
    route: string,
    runtimePreference: qrt.QuicRuntimePreference = qrt.qrpBuiltinOnly,
    timeout: Duration = QuicRelayRpcTimeout
): Future[Result[bool, string]] {.async: (raises: [CancelledError]).}

proc keepRelayConnectionAlive(
    client: TsnetQuicRelayClient,
    label: string
): Future[void] {.async: (raises: [CancelledError, CatchableError]).}

proc awaitPersistentIncomingWithRouteValidation(
    client: TsnetQuicRelayClient,
    stream: MsQuicStream,
    ownerId: int,
    routeId: int,
    route: string,
    interval: Duration = QuicRelayIncomingRevalidateInterval
): Future[JsonNode] {.async: (raises: [CancelledError, CatchableError]).} =
  let validationRoute = ownedText(route)
  let validationEndpoint =
    if client.isNil:
      TsnetQuicRelayEndpoint()
    else:
      ownedRelayEndpoint(client.endpoint)
  if client.isNil:
    raise newException(IOError, "nim_quic relay client is nil while awaiting incoming")
  if stream.isNil:
    raise newException(IOError, "nim_quic relay stream is nil while awaiting incoming")
  if validationRoute.len == 0:
    raise newException(IOError, "nim_quic relay route is empty while awaiting incoming")
  var validationLoop: Future[void] = nil
  var keepAliveLoop: Future[void] = nil
  var validationClient: TsnetQuicRelayClient = nil

  proc closeValidationClient() =
    if validationClient.isNil:
      return
    closeQuicRelayClient(validationClient)
    validationClient = nil

  proc probeRoutePublishedOnValidationClient(): Future[Result[bool, string]]
      {.async: (raises: [CancelledError]).} =
    if validationEndpoint.url.len == 0:
      return err("nim_quic relay validation endpoint is empty")
    var lastError = ""
    for attempt in 0 .. 1:
      if not relayClientUsable(validationClient):
        closeValidationClient()
        validationClient = (await connectRelayAsync(validationEndpoint)).valueOr:
          lastError = "nim_quic relay route_status connect failed: " & error
          if attempt == 1:
            return err(lastError)
          continue
      try:
        let published = await relayRouteStatus(
          validationClient,
          validationRoute,
          QuicRelayRpcTimeout
        )
        if published.isOk():
          return published
        lastError = published.error
      except CancelledError as exc:
        raise exc
      except CatchableError as exc:
        lastError = "nim_quic relay route_status raised: " & exc.msg
      closeValidationClient()
      if attempt == 1:
        return err(lastError)
    return err(lastError)

  try:
    let incomingFut = readJsonMessage(stream)
    let revalidateFut = proc(): Future[void] {.async: (raises: [CancelledError, CatchableError]).} =
      while true:
        await sleepAsync(interval)
        if incomingFut.finished():
          return
        # Keep route_status on a dedicated client, but reuse that client
        # across validation ticks so the event loop doesn't keep creating
        # fresh relay runtimes after the DM path has already succeeded.
        let published = await probeRoutePublishedOnValidationClient()
        let previousStage = relayRouteStateLabel(ownerId, validationRoute)
        let hadLocalReady = relayRouteLocallyReady(ownerId, validationRoute)
        let keepServingRoute =
          relayShouldPreserveServingRoute(ownerId, validationRoute)
        let keepServingUnpublishedRoute =
          relayShouldPreserveUnpublishedServingRoute(ownerId, validationRoute)
        if published.isErr():
          if relayShouldPreserveActiveRouteAfterProbeFailure(
              keepServingRoute,
              hadLocalReady,
              published.error
            ):
            quicRelayTrace(
              "nim_quic relay incoming validation preserved active routeId=" & $routeId &
              " route=" & validationRoute &
              " stage=" & previousStage &
              " err=" & published.error
            )
            markRelayRouteReady(ownerId, validationRoute)
            continue
          clearRelayReadyRoute(ownerId, validationRoute)
          markRelayListenerStage(
            ownerId,
            validationRoute,
            normalizedRelayKind("", validationRoute),
            "dropped",
            published.error
          )
          raise newException(
            IOError,
            "nim_quic relay route validation failed routeId=" & $routeId &
              " route=" & validationRoute &
              " err=" & published.error
          )
        if not published.get():
          if keepServingUnpublishedRoute:
            quicRelayTrace(
              "nim_quic relay incoming validation preserved locally serving unpublished routeId=" &
              $routeId &
              " route=" & validationRoute &
              " stage=" & previousStage
            )
            markRelayRouteReady(ownerId, validationRoute)
            continue
          clearRelayReadyRoute(ownerId, validationRoute)
          markRelayListenerStage(
            ownerId,
            validationRoute,
            normalizedRelayKind("", validationRoute),
            "dropped",
            "route_not_published"
          )
          raise newException(
            IOError,
            "nim_quic relay route lost routeId=" & $routeId &
              " route=" & validationRoute
          )
    validationLoop = revalidateFut()
    keepAliveLoop = keepRelayConnectionAlive(
      client,
      "listener routeId=" & $routeId & " route=" & validationRoute
    )
    let winner = await race(
      cast[FutureBase](incomingFut),
      cast[FutureBase](validationLoop),
      cast[FutureBase](keepAliveLoop)
    )
    if winner == cast[FutureBase](incomingFut):
      if not validationLoop.isNil and not validationLoop.finished():
        validationLoop.cancelSoon()
      if not keepAliveLoop.isNil and not keepAliveLoop.finished():
        keepAliveLoop.cancelSoon()
      return await incomingFut
    if winner == cast[FutureBase](keepAliveLoop):
      incomingFut.cancelSoon()
      if not validationLoop.isNil and not validationLoop.finished():
        validationLoop.cancelSoon()
      try:
        await keepAliveLoop
      except CatchableError as exc:
        raise newException(
          IOError,
          "nim_quic relay keepalive failed routeId=" & $routeId &
            " route=" & validationRoute &
            " err=" & exc.msg
        )
      raise newException(
        IOError,
        "nim_quic relay keepalive stopped routeId=" & $routeId &
          " route=" & validationRoute
      )
    incomingFut.cancelSoon()
    if not keepAliveLoop.isNil and not keepAliveLoop.finished():
      keepAliveLoop.cancelSoon()
    await validationLoop
    raise newException(
      IOError,
      "nim_quic relay incoming validation loop exited without payload routeId=" &
        $routeId & " route=" & validationRoute
    )
  except CatchableError as exc:
    clearRelayReadyRoute(ownerId, validationRoute)
    raise newException(
      IOError,
      "nim_quic relay incoming wait failed routeId=" & $routeId &
        " route=" & validationRoute &
        " err=" & exc.msg
    )
  finally:
    if not validationLoop.isNil and not validationLoop.finished():
      validationLoop.cancelSoon()
    if not keepAliveLoop.isNil and not keepAliveLoop.finished():
      keepAliveLoop.cancelSoon()
    closeValidationClient()

proc awaitJsonFutureWithTimeout(
    fut: Future[JsonNode],
    timeout: Duration,
    label: string
): Future[JsonNode] {.async: (raises: [CancelledError, CatchableError]).} =
  if fut.isNil:
    raise newException(IOError, "nim_quic relay future unavailable for " & label)
  let timeoutFut = sleepAsync(timeout)
  let winner = await race(cast[FutureBase](fut), cast[FutureBase](timeoutFut))
  if winner == cast[FutureBase](timeoutFut):
    fut.cancelSoon()
    raise newException(
      IOError,
      "nim_quic relay timed out waiting for " & label & " after " & $timeout
    )
  timeoutFut.cancelSoon()
  await fut

proc awaitBridgeAttachWithTimeout(
    fut: Future[MsQuicStream],
    timeout: Duration,
    label: string
): Future[MsQuicStream] {.async: (raises: [CancelledError, CatchableError]).} =
  if fut.isNil:
    raise newException(IOError, "nim_quic relay bridge future unavailable for " & label)
  let timeoutFut = sleepAsync(timeout)
  let winner = await race(cast[FutureBase](fut), cast[FutureBase](timeoutFut))
  if winner == cast[FutureBase](timeoutFut):
    fut.cancelSoon()
    raise newException(
      IOError,
      "nim_quic relay timed out waiting for " & label & " after " & $timeout
    )
  timeoutFut.cancelSoon()
  await fut

proc readBinaryFrame(
    stream: MsQuicStream
): Future[seq[byte]] {.async: (raises: [CancelledError, CatchableError]).} =
  var payload = stream.takeCachedBytes()
  let cachedFrame = splitCompleteFramedPayload(payload)
  if cachedFrame.isSome():
    let framed = cachedFrame.get()
    stream.restoreCachedBytes(framed.rest)
    return framed.frame
  while true:
    try:
      let chunk = await stream.read()
      if chunk.len == 0:
        break
      payload.add(chunk)
      let framed = splitCompleteFramedPayload(payload)
      if framed.isSome():
        let complete = framed.get()
        stream.restoreCachedBytes(complete.rest)
        return complete.frame
    except LPStreamEOFError:
      break
  let framed = splitCompleteFramedPayload(payload)
  if framed.isSome():
    let complete = framed.get()
    stream.restoreCachedBytes(complete.rest)
    return complete.frame
  stream.restoreCachedBytes(@[])
  @[]

proc readRelayJsonMessageFromCachedForTests*(
    payload: seq[byte]
): Future[JsonNode] {.async: (raises: [CancelledError, CatchableError]).} =
  let stream = newCachedMsQuicStreamForTests(payload)
  await readJsonMessage(stream)

proc readRelayBinaryFrameFromCachedForTests*(
    payload: seq[byte]
): Future[seq[byte]] {.async: (raises: [CancelledError, CatchableError]).} =
  let stream = newCachedMsQuicStreamForTests(payload)
  await readBinaryFrame(stream)

proc readRelayDialAckThenBinaryFromCachedForTests*(
    payload: seq[byte]
): Future[tuple[ack: JsonNode, frame: seq[byte]]] {.async: (raises: [CancelledError, CatchableError]).} =
  let stream = newCachedMsQuicStreamForTests(payload)
  result.ack = await readJsonMessage(stream)
  result.frame = await readBinaryFrame(stream)

proc writeBinaryFrame(
    stream: MsQuicStream,
    payload: seq[byte]
) {.async: (raises: [CancelledError, CatchableError]).} =
  var encoded = newSeqOfCap[byte](4 + payload.len)
  appendUint32BE(encoded, uint32(payload.len))
  encoded.add(payload)
  await stream.write(encoded)
  if encoded.len > 0:
    await sleepAsync(QuicRelayWriteDrainDelay)

proc readFrameString(
    stream: MsQuicStream
): Future[string] {.async: (raises: [CancelledError, CatchableError]).} =
  let node = await readJsonMessage(stream)
  $node

proc closeQuicStream(stream: MsQuicStream) {.async: (raises: []).} =
  if stream.isNil:
    return
  try:
    stream.closeNow()
  except CatchableError:
    discard

proc finishQuicStream(
    stream: MsQuicStream,
    drainDelay: Duration = QuicRelayGatewayResponseDrainDelay
) {.async: (raises: []).} =
  if stream.isNil:
    return
  let liveStream = stream
  liveStream.restoreCachedBytes(@[])
  try:
    await liveStream.sendFin()
  except CatchableError:
    discard
  if drainDelay > ZeroDuration:
    try:
      await sleepAsync(drainDelay)
    except CatchableError:
      discard
  try:
    liveStream.closeNow()
  except CatchableError:
    discard

proc pingRelayConnection(
    client: TsnetQuicRelayClient,
    timeout: Duration = QuicRelayRpcTimeout
): Future[Result[void, string]] {.async: (raises: [CancelledError]).} =
  if client.isNil:
    return err("nim_quic relay client is nil")
  var stream: MsQuicStream = nil
  try:
    stream = createBidiStream(client.handle, client.connPtr, client.connState).valueOr:
      return err("failed to create nim_quic relay stream: " & error)
    await writeJsonMessage(stream, %*{
      "version": 1,
      "mode": "ping"
    })
    let pong = await readJsonMessageWithTimeout(
      stream,
      timeout,
      "nim_quic relay ping ack"
    )
    if pong.isNil or not pong{"ok"}.getBool():
      return err("nim_quic relay ping rejected")
    ok()
  except CancelledError as exc:
    raise exc
  except CatchableError as exc:
    err("nim_quic relay ping failed: " & exc.msg)
  finally:
    if not stream.isNil:
      await noCancel stream.finishQuicStream(QuicRelayWriteDrainDelay)

proc connectRelay(
    endpoint: TsnetQuicRelayEndpoint,
    runtimePreference: qrt.QuicRuntimePreference = qrt.qrpBuiltinOnly,
    allowWithoutConnected = true
): Result[TsnetQuicRelayClient, string] {.gcsafe, raises: [].}

proc relayRouteStatus(
    client: TsnetQuicRelayClient,
    route: string,
    timeout: Duration = QuicRelayRpcTimeout
): Future[Result[bool, string]] {.async: (raises: [CancelledError]).} =
  if client.isNil:
    return err("nim_quic relay client is nil")
  if route.len == 0:
    return err("nim_quic relay route is empty")
  var stream: MsQuicStream = nil
  try:
    stream = createBidiStream(client.handle, client.connPtr, client.connState).valueOr:
      return err("failed to create nim_quic relay stream: " & error)
    await writeJsonMessage(stream, %*{
      "version": 1,
      "mode": "route_status",
      "route": route
    })
    let response = await readJsonMessageWithTimeout(
      stream,
      timeout,
      "nim_quic relay route_status ack"
    )
    if response.isNil or not response{"ok"}.getBool():
      return err("nim_quic relay route_status rejected")
    ok(rpcPayload(response){"published"}.getBool(false))
  except CancelledError as exc:
    raise exc
  except CatchableError as exc:
    err("nim_quic relay route_status failed: " & exc.msg)
  finally:
    if not stream.isNil:
      await noCancel stream.finishQuicStream(QuicRelayWriteDrainDelay)

proc ensureRelayReady(
    client: TsnetQuicRelayClient,
    endpoint: TsnetQuicRelayEndpoint,
    attempts = QuicRelayReadinessPingAttempts,
    timeout: Duration = QuicRelayRpcTimeout
): Result[void, string] {.gcsafe, raises: [].} =
  var lastError = ""
  for attempt in 0 ..< max(1, attempts):
    if attempt > 0:
      try:
        waitFor sleepAsync(QuicRelayReadinessRetryDelay)
      except CatchableError:
        discard
    let pinged =
      try:
        waitFor pingRelayConnection(client, timeout)
      except CatchableError as exc:
        Result[void, string].err(
          "nim_quic relay readiness ping failed for " & endpoint.url & ": " & exc.msg
        )
    if pinged.isOk():
      return ok()
    lastError = pinged.error
    quicRelayTrace(
        "relay readiness ping failed endpoint=" & endpoint.url &
        " attempt=" & $(attempt + 1) &
        " err=" & lastError
      )
  err(lastError)

proc ensureRelayRoutePublishedOnConnection(
    client: TsnetQuicRelayClient,
    route: string,
    attempts = QuicRelayRoutePublishAttempts,
    timeout: Duration = QuicRelayRpcTimeout
): Future[Result[void, string]] {.async: (raises: [CancelledError]).} =
  if client.isNil:
    return err("nim_quic relay client is nil")
  if route.len == 0:
    return err("nim_quic relay route is empty")
  var lastError = ""
  for attempt in 0 ..< max(1, attempts):
    if attempt > 0:
      await sleepAsync(QuicRelayRoutePublishRetryDelay)
    let published = await relayRouteStatus(client, route, timeout)
    if published.isOk():
      if published.get():
        quicRelayTrace(
          "relay route published route=" & route &
          " attempt=" & $(attempt + 1)
        )
        return ok()
      lastError = "route_not_published"
    else:
      lastError = published.error
    quicRelayTrace(
      "relay route publication pending route=" & route &
      " attempt=" & $(attempt + 1) &
      " err=" & lastError
    )
  err(lastError)

proc ensureRelayRoutePublished(
    endpoint: TsnetQuicRelayEndpoint,
    route: string,
    runtimePreference: qrt.QuicRuntimePreference = qrt.qrpBuiltinOnly,
    attempts = QuicRelayRoutePublishAttempts,
    timeout: Duration = QuicRelayRpcTimeout
): Result[void, string] {.gcsafe, raises: [].} =
  if route.len == 0:
    return err("nim_quic relay route is empty")
  var lastError = ""
  for attempt in 0 ..< max(1, attempts):
    if attempt > 0:
      try:
        waitFor sleepAsync(QuicRelayRoutePublishRetryDelay)
      except CatchableError:
        discard
    let probeClient = connectRelay(endpoint, runtimePreference).valueOr:
      lastError = "nim_quic relay route_status connect failed: " & error
      quicRelayTrace(
        "relay route_status connect failed endpoint=" & endpoint.url &
        " route=" & route &
        " attempt=" & $(attempt + 1) &
        " err=" & lastError
      )
      continue
    let published =
      try:
        waitFor relayRouteStatus(probeClient, route, timeout)
      except CatchableError as exc:
        Result[bool, string].err("nim_quic relay route_status raised: " & exc.msg)
      finally:
        closeQuicRelayClient(probeClient)
    if published.isOk():
      if published.get():
        quicRelayTrace(
          "relay route published endpoint=" & endpoint.url &
          " route=" & route &
          " attempt=" & $(attempt + 1)
        )
        return ok()
      lastError = "route_not_published"
    else:
      lastError = published.error
      quicRelayTrace(
        "relay route_status failed endpoint=" & endpoint.url &
        " route=" & route &
        " attempt=" & $(attempt + 1) &
        " err=" & lastError
      )
  err(lastError)

proc probeRelayRoutePublished(
    endpoint: TsnetQuicRelayEndpoint,
    route: string,
    runtimePreference: qrt.QuicRuntimePreference = qrt.qrpBuiltinOnly,
    timeout: Duration = QuicRelayRpcTimeout
): Result[bool, string] {.gcsafe, raises: [].} =
  if route.len == 0:
    return err("nim_quic relay route is empty")
  let probeClient = connectRelay(endpoint, runtimePreference).valueOr:
    return err("nim_quic relay route_status connect failed: " & error)
  try:
    waitFor relayRouteStatus(probeClient, route, timeout)
  except CatchableError as exc:
    err("nim_quic relay route_status raised: " & exc.msg)
  finally:
    closeQuicRelayClient(probeClient)

proc probeRelayRoutePublishedAsync(
    endpoint: TsnetQuicRelayEndpoint,
    route: string,
    runtimePreference: qrt.QuicRuntimePreference = qrt.qrpBuiltinOnly,
    timeout: Duration = QuicRelayRpcTimeout
): Future[Result[bool, string]] {.async: (raises: [CancelledError]).} =
  if route.len == 0:
    return err("nim_quic relay route is empty")
  let probeClient = (await connectRelayAsync(endpoint, runtimePreference)).valueOr:
    return err("nim_quic relay route_status connect failed: " & error)
  try:
    return await relayRouteStatus(probeClient, route, timeout)
  except CancelledError as exc:
    raise exc
  except CatchableError as exc:
    return err("nim_quic relay route_status raised: " & exc.msg)
  finally:
    closeQuicRelayClient(probeClient)

proc confirmRelayListenRegistration(
    regStream: MsQuicStream,
    endpoint: TsnetQuicRelayEndpoint,
    route: string,
    timeout: Duration = QuicRelayRpcTimeout
): Future[Result[void, string]] {.async: (raises: [CancelledError]).} =
  if regStream.isNil:
    return err("nim_quic relay listen stream is nil")
  if route.len == 0:
    return err("nim_quic relay route is empty")
  try:
    let response = await readJsonMessageWithTimeout(
      regStream,
      timeout,
      "nim_quic relay listen ack"
    )
    if response.isNil or not response{"ok"}.getBool():
      return err("nim_quic relay listen rejected")
    let payload = rpcPayload(response)
    let ackRoute = jsonString(payload, "route")
    if ackRoute.len > 0 and ackRoute != route:
      return err(
        "nim_quic relay listen route mismatch for " & endpoint.url &
        ": expected " & route & " got " & ackRoute
      )
    ok()
  except CancelledError as exc:
    raise exc
  except CatchableError as exc:
    err("nim_quic relay listen ack failed: " & exc.msg)

proc bridgeLocalToQuic(local: StreamTransport, relay: MsQuicStream): Future[void]
    {.async: (raises: [CancelledError]).} =
  var buf = newSeq[byte](RelayFrameBufferSize)
  while true:
    let read =
      try:
        await local.readOnce(addr buf[0], buf.len)
      except CatchableError:
        break
    if read <= 0:
      break
    try:
      await relay.write(buf[0 ..< read])
    except CatchableError:
      break
  try:
    await relay.sendFin()
  except CatchableError:
    discard

proc bridgeQuicToLocal(relay: MsQuicStream, local: StreamTransport): Future[void]
    {.async: (raises: [CancelledError]).} =
  while true:
    let chunk =
      try:
        await relay.read()
      except LPStreamEOFError:
        break
      except CatchableError as exc:
        if relay.shouldRetryReadFailure(exc.msg):
          await sleepAsync(QuicRelayReadRetryDelay)
          continue
        break
    if chunk.len == 0:
      break
    try:
      discard await local.write(chunk)
    except CatchableError:
      break

proc bridgeQuicToQuic(src, dst: MsQuicStream): Future[void]
    {.async: (raises: [CancelledError]).} =
  while true:
    let chunk =
      try:
        await src.read()
      except LPStreamEOFError:
        break
      except CatchableError as exc:
        if src.shouldRetryReadFailure(exc.msg):
          await sleepAsync(QuicRelayReadRetryDelay)
          continue
        break
    if chunk.len == 0:
      break
    try:
      await dst.write(chunk)
    except CatchableError:
      break
  try:
    await dst.sendFin()
  except CatchableError:
    discard

proc bridgeLocalAndQuic(
    local: StreamTransport,
    relay: MsQuicStream
): Future[void] {.async: (raises: []).} =
  let forward = bridgeLocalToQuic(local, relay)
  let backward = bridgeQuicToLocal(relay, local)
  try:
    discard await one(@[forward, backward])
  except CatchableError:
    discard
  var cancels: seq[Future[void]] = @[]
  if not forward.finished():
    cancels.add(forward.cancelAndWait())
  if not backward.finished():
    cancels.add(backward.cancelAndWait())
  if cancels.len > 0:
    await noCancel allFutures(cancels)
  await noCancel local.safeCloseTransport()
  await noCancel relay.closeQuicStream()

type
  TsnetUdpRelayBridge = ref object
    ownerId: int
    family: string
    target: TransportAddress
    stream: MsQuicStream
    bridgeTask: Future[void]
    rawKey: string
    clientRemote: Option[TransportAddress]
    pendingFrames: seq[seq[byte]]
    pendingClientFrames: seq[seq[byte]]
    writeGate: AsyncLock
    clientFrameGate: AsyncLock
    loggedProxyIngress: bool
    loggedTargetEgress: bool
    loggedClientEgress: bool
    proxyIngressPackets: int
    proxyIngressBytes: int64
    targetEgressPackets: int
    targetEgressBytes: int64
    clientEgressPackets: int
    clientEgressBytes: int64

proc closeDatagramTransport(transp: DatagramTransport) {.async: (raises: []).} =
  if transp.isNil:
    return
  try:
    transp.close()
  except CatchableError:
    discard
  try:
    await transp.closeWait()
  except CatchableError:
    discard

proc udpBridgeSummary(state: TsnetUdpRelayBridge): string =
  if state.isNil:
    return "udpBridge=<nil>"
  "owner=" & $state.ownerId &
    " rawKey=" & state.rawKey &
    " target=" & $state.target &
    " clientRemote=" &
      (if state.clientRemote.isSome(): $state.clientRemote.get() else: "<none>") &
    " proxyIngressPackets=" & $state.proxyIngressPackets &
    " proxyIngressBytes=" & $state.proxyIngressBytes &
    " targetEgressPackets=" & $state.targetEgressPackets &
    " targetEgressBytes=" & $state.targetEgressBytes &
    " clientEgressPackets=" & $state.clientEgressPackets &
    " clientEgressBytes=" & $state.clientEgressBytes &
    " pendingFrames=" & $state.pendingFrames.len &
    " pendingClientFrames=" & $state.pendingClientFrames.len

proc udpBridgeAcceptsSource*(
    target: TransportAddress,
    clientRemote: Option[TransportAddress],
    remote: TransportAddress,
    restrictSource: bool
): tuple[accept: bool, learnClient: bool] {.gcsafe, raises: [].} =
  if restrictSource:
    return ($remote == $target, false)
  if clientRemote.isNone():
    return (true, true)
  ($remote == $clientRemote.get(), false)

proc writeUdpFrameLocked(
    state: TsnetUdpRelayBridge,
    payload: seq[byte]
) {.async: (raises: [CatchableError]).} =
  if state.isNil:
    return
  if state.writeGate.isNil:
    state.writeGate = newAsyncLock()
  try:
    await state.writeGate.acquire()
  except CancelledError as exc:
    raise newException(IOError, exc.msg)
  try:
    if state.stream.isNil:
      if state.pendingFrames.len < 128:
        state.pendingFrames.add(payload)
      return
    try:
      await writeBinaryFrame(state.stream, payload)
    except CancelledError as exc:
      raise newException(IOError, exc.msg)
  finally:
    try:
      state.writeGate.release()
    except AsyncLockError:
      discard

proc flushPendingUdpFrames(state: TsnetUdpRelayBridge) {.async: (raises: []).} =
  if state.isNil or state.pendingFrames.len == 0:
    return
  if state.writeGate.isNil:
    state.writeGate = newAsyncLock()
  try:
    await state.writeGate.acquire()
  except CancelledError:
    return
  try:
    if state.stream.isNil or state.pendingFrames.len == 0:
      return
    let pending = state.pendingFrames
    state.pendingFrames = @[]
    for frame in pending:
      try:
        await writeBinaryFrame(state.stream, frame)
      except CatchableError:
        break
  finally:
    try:
      state.writeGate.release()
    except AsyncLockError:
      discard

proc takePendingClientFrames(
    state: TsnetUdpRelayBridge,
    payload: seq[byte] = @[]
): seq[seq[byte]] {.gcsafe, raises: [].} =
  if state.isNil:
    return @[]
  if state.clientRemote.isNone():
    if payload.len > 0 and state.pendingClientFrames.len < 128:
      state.pendingClientFrames.add(payload)
      quicRelayTrace(
        "udp relay client frame queued owner=" & $state.ownerId &
        " rawKey=" & state.rawKey &
        " frameLen=" & $payload.len &
        " pendingClientFrames=" & $state.pendingClientFrames.len
      )
    elif payload.len > 0:
      quicRelayTrace(
        "udp relay client frame dropped owner=" & $state.ownerId &
        " rawKey=" & state.rawKey &
        " frameLen=" & $payload.len &
        " pendingClientFrames=" & $state.pendingClientFrames.len &
        " reason=queue_full"
      )
    return @[]
  if state.pendingClientFrames.len == 0 and payload.len == 0:
    return @[]
  swap(result, state.pendingClientFrames)
  if payload.len > 0:
    result.add(payload)

proc flushPendingClientFramesToClient(
    transp: DatagramTransport,
    state: TsnetUdpRelayBridge,
    payload: seq[byte] = @[]
) {.async: (raises: [CatchableError]).} =
  if transp.isNil or state.isNil:
    return
  if state.clientFrameGate.isNil:
    state.clientFrameGate = newAsyncLock()
  try:
    await state.clientFrameGate.acquire()
  except CancelledError as exc:
    raise newException(IOError, exc.msg)
  try:
    let outgoing = state.takePendingClientFrames(payload)
    if outgoing.len == 0:
      return
    let destination = state.clientRemote.get()
    quicRelayTrace(
      "udp relay client flush begin owner=" & $state.ownerId &
      " rawKey=" & state.rawKey &
      " dest=" & $destination &
      " frames=" & $outgoing.len &
      " pendingAfterTake=" & $state.pendingClientFrames.len
    )
    var totalBytes = 0
    for frame in outgoing:
      if frame.len == 0:
        continue
      totalBytes += frame.len
      inc state.clientEgressPackets
      state.clientEgressBytes += frame.len.int64
      if not state.loggedClientEgress:
        state.loggedClientEgress = true
        quicRelayTrace(
          "udp relay egress->client owner=" & $state.ownerId &
          " rawKey=" & state.rawKey &
          " dest=" & $destination &
          " len=" & $frame.len
        )
      try:
        await transp.sendTo(destination, frame)
      except CancelledError as exc:
        raise newException(IOError, exc.msg)
    quicRelayTrace(
      "udp relay client flush done owner=" & $state.ownerId &
      " rawKey=" & state.rawKey &
      " dest=" & $destination &
      " frames=" & $outgoing.len &
      " bytes=" & $totalBytes
    )
  finally:
    try:
      state.clientFrameGate.release()
    except AsyncLockError:
      discard

proc pendingClientFramesAfterRemoteLearnedForTests*(
    queuedFrames: seq[seq[byte]],
    payloadAfterLearn: seq[byte]
): tuple[queuedBeforeLearn: int, outgoing: seq[seq[byte]]] {.gcsafe, raises: [].} =
  let state = TsnetUdpRelayBridge(
    ownerId: 1,
    family: "ip4",
    target: TransportAddress(),
    stream: nil,
    rawKey: "test",
    clientRemote: none(TransportAddress),
    pendingFrames: @[],
    pendingClientFrames: @[],
    loggedProxyIngress: false,
    loggedTargetEgress: false,
    loggedClientEgress: false
  )
  for frame in queuedFrames:
    discard state.takePendingClientFrames(frame)
  result.queuedBeforeLearn = state.pendingClientFrames.len
  state.clientRemote = some(TransportAddress())
  result.outgoing = state.takePendingClientFrames(payloadAfterLearn)

proc bridgeUdpProxyToQuic(
    transp: DatagramTransport,
    state: TsnetUdpRelayBridge,
    remote: TransportAddress,
    restrictSource: bool
) {.async: (raises: []).} =
  if transp.isNil or state.isNil:
    return
  try:
    let sourceDecision = udpBridgeAcceptsSource(
      state.target,
      state.clientRemote,
      remote,
      restrictSource
    )
    if not sourceDecision.accept:
      quicRelayTrace(
        "udp proxy ingress dropped owner=" & $state.ownerId &
        " rawKey=" & state.rawKey &
        " remote=" & $remote &
        " target=" & $state.target &
        " clientRemote=" &
          (if state.clientRemote.isSome(): $state.clientRemote.get() else: "<none>") &
        " restrictSource=" & $restrictSource
      )
      return
    let message = transp.getMessage()
    if message.len == 0:
      return
    inc state.proxyIngressPackets
    state.proxyIngressBytes += message.len.int64
    if not state.loggedProxyIngress:
      state.loggedProxyIngress = true
      quicRelayTrace(
        "udp proxy ingress owner=" & $state.ownerId &
        " rawKey=" & state.rawKey &
        " remote=" & $remote &
        " len=" & $message.len &
        " restrictSource=" & $restrictSource
      )
    if sourceDecision.learnClient:
      quicRelayTrace(
        "udp relay learned clientRemote owner=" & $state.ownerId &
        " rawKey=" & state.rawKey &
        " clientRemote=" & $remote &
        " pendingClientFrames=" & $state.pendingClientFrames.len
      )
      state.clientRemote = some(remote)
      await flushPendingClientFramesToClient(transp, state)
    await state.writeUdpFrameLocked(message)
  except CatchableError:
    discard

proc bridgeQuicToUdpTarget(
    stream: MsQuicStream,
    transp: DatagramTransport,
    state: TsnetUdpRelayBridge,
    target: TransportAddress,
    toClient: bool
) {.async: (raises: [CancelledError]).} =
  while true:
    let payload =
      try:
        await readBinaryFrame(stream)
      except LPStreamEOFError:
        break
      except CatchableError as exc:
        if stream.shouldRetryReadFailure(exc.msg):
          await sleepAsync(QuicRelayReadRetryDelay)
          continue
        break
    if payload.len == 0:
      break
    if toClient:
      if state.clientRemote.isNone():
        quicRelayTrace(
          "udp relay egress->client waiting owner=" & $state.ownerId &
          " rawKey=" & state.rawKey &
          " frameLen=" & $payload.len &
          " pendingClientFrames=" & $state.pendingClientFrames.len
        )
      try:
        await flushPendingClientFramesToClient(transp, state, payload)
      except CatchableError:
        break
    else:
      let destination = target
      inc state.targetEgressPackets
      state.targetEgressBytes += payload.len.int64
      if not state.loggedTargetEgress:
        state.loggedTargetEgress = true
        quicRelayTrace(
          "udp relay egress->target owner=" & $state.ownerId &
          " rawKey=" & state.rawKey &
          " dest=" & $destination &
          " len=" & $payload.len
        )
      try:
        await transp.sendTo(destination, payload)
      except CatchableError:
        break

proc bridgeUdpAndQuic(
    transp: DatagramTransport,
    state: TsnetUdpRelayBridge,
    target: TransportAddress,
    toClient: bool
) {.async: (raises: []).} =
  try:
    await bridgeQuicToUdpTarget(state.stream, transp, state, target, toClient)
  except CatchableError:
    discard
  quicRelayTrace(
    "udp bridge summary toClient=" & $toClient & " " & state.udpBridgeSummary()
  )
  await noCancel transp.closeDatagramTransport()
  await noCancel state.stream.closeQuicStream()

proc bridgeUdpAndQuicWithRelayClient(
    transp: DatagramTransport,
    state: TsnetUdpRelayBridge,
    target: TransportAddress,
    toClient: bool,
    client: TsnetQuicRelayClient
) {.async: (raises: []).} =
  retainRelayClientBridge(client)
  try:
    await bridgeUdpAndQuic(transp, state, target, toClient)
  finally:
    state.bridgeTask = nil
    releaseRelayClientBridge(client)

proc bridgeQuicStreams(
    a, b: MsQuicStream
): Future[void] {.async: (raises: []).} =
  let forward = bridgeQuicToQuic(a, b)
  let backward = bridgeQuicToQuic(b, a)
  try:
    await allFutures(@[forward, backward])
  except CatchableError:
    discard
  await noCancel a.closeQuicStream()
  await noCancel b.closeQuicStream()

proc relayAllocateRouteId(ownerId: int): int =
  if ownerId <= 0:
    return 0
  withLock(relayRegistryLock):
    inc relayNextRouteId
    result = relayNextRouteId

proc relayStoreTask(
    ownerId: int,
    routeId: int,
    route: string,
    server: StreamServer,
    datagram: DatagramTransport,
    task: Future[void]
) =
  if ownerId <= 0:
    return
  withLock(relayRegistryLock):
    let handle = TsnetRelayTaskHandle(
      routeId: routeId,
      route: ownedText(route),
      server: server,
      datagram: datagram,
      task: task
    )
    relayOwnerTasks.mgetOrPut(
      ownerId,
      initTable[int, TsnetRelayTaskHandle]()
    )[routeId] = handle
    if route.len > 0:
      relayOwnerRouteTaskIds.mgetOrPut(
        ownerId,
        initTable[string, int]()
      )[ownedText(route)] = routeId

proc relayOwnerEpoch(ownerId: int): int {.gcsafe, raises: [].} =
  quicRelaySafe:
    if ownerId <= 0:
      return 0
    withLock(relayRegistryLock):
      result = relayOwnerEpochs.getOrDefault(ownerId, 0)
      if result <= 0:
        result = 1
        relayOwnerEpochs[ownerId] = result

proc markRelayRouteTaskActive(
    ownerId: int,
    route: string,
    ownerEpoch: int
) {.gcsafe, raises: [].} =
  quicRelaySafe:
    if ownerId <= 0 or route.len == 0:
      return
    withLock(relayRegistryLock):
      relayOwnerActiveRoutes.mgetOrPut(
        ownerId,
        initTable[string, int]()
      )[ownedText(route)] = ownerEpoch

proc clearRelayRouteTaskActive(
    ownerId: int,
    route: string,
    ownerEpoch: int
) {.gcsafe, raises: [].} =
  quicRelaySafe:
    if ownerId <= 0 or route.len == 0:
      return
    withLock(relayRegistryLock):
      var clearRouteId = false
      var deleteRoutes = false
      relayOwnerActiveRoutes.withValue(ownerId, routes):
        if routes[].getOrDefault(route, 0) != ownerEpoch:
          return
        routes[].del(route)
        clearRouteId = true
        if routes[].len == 0:
          deleteRoutes = true
      if not clearRouteId:
        return
      if deleteRoutes:
        relayOwnerActiveRoutes.del(ownerId)
      var deleteRouteIds = false
      relayOwnerRouteTaskIds.withValue(ownerId, routeIds):
        routeIds[].del(route)
        if routeIds[].len == 0:
          deleteRouteIds = true
      if deleteRouteIds:
        relayOwnerRouteTaskIds.del(ownerId)

proc relayFinishRouteTask(
    ownerId: int,
    routeId: int,
    route: string,
    ownerEpoch: int
) {.gcsafe, raises: [].} =
  quicRelaySafe:
    if ownerId <= 0 or routeId <= 0:
      return
    withLock(relayRegistryLock):
      var deleteRoutes = false
      relayOwnerActiveRoutes.withValue(ownerId, routes):
        if route.len > 0 and routes[].getOrDefault(route, 0) == ownerEpoch:
          routes[].del(route)
        if routes[].len == 0:
          deleteRoutes = true
      if deleteRoutes:
        relayOwnerActiveRoutes.del(ownerId)

      var deleteRouteIds = false
      relayOwnerRouteTaskIds.withValue(ownerId, routeIds):
        if route.len > 0 and routeIds[].getOrDefault(route, 0) == routeId:
          routeIds[].del(route)
        if routeIds[].len == 0:
          deleteRouteIds = true
      if deleteRouteIds:
        relayOwnerRouteTaskIds.del(ownerId)

      var deleteOwnerTasks = false
      relayOwnerTasks.withValue(ownerId, ownerTasks):
        ownerTasks[].del(routeId)
        if ownerTasks[].len == 0:
          deleteOwnerTasks = true
      if deleteOwnerTasks:
        relayOwnerTasks.del(ownerId)
      let routeKey = relayReadyRouteKey(ownerId, route)
      if routeKey.len > 0 and
          relayRouteCanRequestRepair(route) and
          relayOwnerEpochMatchesLocked(ownerId, ownerEpoch):
        relayRepairRequestedRoutes.incl(routeKey)

proc relayOwnerTaskRevoked(
    ownerId: int,
    ownerEpoch: int
): bool {.gcsafe, raises: [].} =
  relayOwnerEpoch(ownerId) != ownerEpoch

proc relayDialTaskRoute(route: string): string =
  if route.len == 0:
    return ""
  RelayDialTaskRoutePrefix & route

proc relayTakeOwnerTasks(ownerId: int): seq[TsnetRelayTaskHandle]

proc relayStopTaskHandle(handle: TsnetRelayTaskHandle) =
  if handle.isNil:
    return
  let task = handle.task
  if task.isNil or task.finished():
    return
  task.cancelSoon()

proc takeAndRevokeRelayListeners(
    ownerId: int
): seq[TsnetRelayTaskHandle] {.gcsafe, raises: [].} =
  quicRelaySafe:
    if ownerId <= 0:
      return @[]
    result = relayTakeOwnerTasks(ownerId)
    withLock(relayRegistryLock):
      relayOwnerEpochs[ownerId] = relayOwnerEpochs.getOrDefault(ownerId, 0) + 1
      relayOwnerActiveRoutes.del(ownerId)
      clearRelayRouteKeysForOwnerLocked(relayReadyRoutes, ownerId)
      clearRelayRouteKeysForOwnerLocked(relayLocallyUsableRoutes, ownerId)
      clearRelayRouteKeysForOwnerLocked(relayPendingRoutes, ownerId)
      clearRelayRouteKeysForOwnerLocked(relayAwaitingRoutes, ownerId)
      clearRelayRouteKeysForOwnerLocked(relayIncomingRoutes, ownerId)
      clearRelayRouteKeysForOwnerLocked(relayReadyStageRoutes, ownerId)
      clearRelayRouteKeysForOwnerLocked(relayBridgeAttachedRoutes, ownerId)
      clearRelayRouteKeysForOwnerLocked(relayServingRoutes, ownerId)
      clearRelayRouteKeysForOwnerLocked(relayRepairRequestedRoutes, ownerId)
      clearRelayRouteKeysForOwnerLocked(relayKnownRoutes, ownerId)
      clearRelayRouteKeysForOwnerLocked(relayDroppedRoutes, ownerId)
      relayUdpDialStates.del(ownerId)
      relayListenerStates.del(ownerId)
      relayOwnerRouteTaskIds.del(ownerId)

proc stopRelayListenersAsync*(
    ownerId: int
): Future[void] {.async: (raises: [CancelledError]).} =
  if ownerId <= 0:
    return
  let stoppedTasks = takeAndRevokeRelayListeners(ownerId)
  var pendingCancels: seq[Future[void]] = @[]
  for handle in stoppedTasks:
    relayStopTaskHandle(handle)
    if not handle.task.isNil and not handle.task.finished():
      pendingCancels.add(handle.task.cancelAndWait())
  closeOwnerRelayClients(ownerId)
  if pendingCancels.len > 0:
    try:
      await noCancel allFutures(pendingCancels)
    except CatchableError:
      discard

proc relayCancelRouteTasks(ownerId: int, route: string) =
  if ownerId <= 0 or route.len == 0:
    return
  var cancelled: seq[TsnetRelayTaskHandle] = @[]
  withLock(relayRegistryLock):
    var routeId = 0
    var deleteRouteIds = false
    relayOwnerRouteTaskIds.withValue(ownerId, routeIds):
      routeId = routeIds[].getOrDefault(route, 0)
      if routeId > 0:
        routeIds[].del(route)
        if routeIds[].len == 0:
          deleteRouteIds = true
    if deleteRouteIds:
      relayOwnerRouteTaskIds.del(ownerId)
    if routeId > 0:
      var deleteOwnerTasks = false
      relayOwnerTasks.withValue(ownerId, ownerTasks):
        let handle = ownerTasks[].getOrDefault(routeId, nil)
        if not handle.isNil:
          cancelled.add(handle)
          ownerTasks[].del(routeId)
          if ownerTasks[].len == 0:
            deleteOwnerTasks = true
      if deleteOwnerTasks:
        relayOwnerTasks.del(ownerId)
  if cancelled.len > 0:
    quicRelayTrace(
      "relay cancelling route tasks owner=" & $ownerId &
      " route=" & route &
      " count=" & $cancelled.len
    )
  for handle in cancelled:
    relayStopTaskHandle(handle)

proc relayTakeOwnerTasks(ownerId: int): seq[TsnetRelayTaskHandle] =
  if ownerId <= 0:
    return @[]
  withLock(relayRegistryLock):
    relayOwnerTasks.withValue(ownerId, ownerTasks):
      result = newSeqOfCap[TsnetRelayTaskHandle](ownerTasks[].len)
      for _, handle in ownerTasks[].pairs:
        result.add(handle)
    if result.len > 0:
      relayOwnerTasks.del(ownerId)
      relayOwnerRouteTaskIds.del(ownerId)

proc relayOwnerTaskCount(ownerId: int): int =
  if ownerId <= 0:
    return 0
  withLock(relayRegistryLock):
    let ownerEpoch = relayOwnerEpochs.getOrDefault(ownerId, 0)
    relayOwnerActiveRoutes.withValue(ownerId, routes):
      for entryEpoch in routes[].values:
        if entryEpoch == ownerEpoch:
          inc result

proc relayOwnerHasActiveRouteTask*(ownerId: int, route: string): bool {.gcsafe, raises: [].} =
  quicRelaySafe:
    if ownerId <= 0 or route.len == 0:
      return false
    withLock(relayRegistryLock):
      let ownerEpoch = relayOwnerEpochs.getOrDefault(ownerId, 0)
      if ownerEpoch <= 0:
        return false
      relayOwnerActiveRoutes.withValue(ownerId, routes):
        result = routes[].getOrDefault(route, 0) == ownerEpoch

proc relayOwnerHasRunningRouteTask*(ownerId: int, route: string): bool {.gcsafe, raises: [].} =
  quicRelaySafe:
    if ownerId <= 0 or route.len == 0:
      return false
    withLock(relayRegistryLock):
      let ownerEpoch = relayOwnerEpochs.getOrDefault(ownerId, 0)
      if ownerEpoch <= 0:
        return false
      var routeActive = false
      relayOwnerActiveRoutes.withValue(ownerId, routes):
        routeActive = routes[].getOrDefault(route, 0) == ownerEpoch
      if not routeActive:
        return false
      var routeId = 0
      relayOwnerRouteTaskIds.withValue(ownerId, routeIds):
        routeId = routeIds[].getOrDefault(route, 0)
      if routeId <= 0:
        return false
      relayOwnerTasks.withValue(ownerId, ownerTasks):
        result = ownerTasks[].hasKey(routeId)

proc relayRouteTaskCurrentLocked(
    ownerId: int,
    route: string,
    routeId: int,
    ownerEpoch: int
): bool =
  if ownerId <= 0 or route.len == 0 or routeId <= 0:
    return false
  if relayOwnerEpochs.getOrDefault(ownerId, 0) != ownerEpoch:
    return false
  var currentRouteId = 0
  relayOwnerRouteTaskIds.withValue(ownerId, routeIds):
    currentRouteId = routeIds[].getOrDefault(route, 0)
  currentRouteId == routeId

proc relayOwnerTaskCountForTests*(ownerId: int): int =
  if ownerId <= 0:
    return 0
  withLock(relayRegistryLock):
    let ownerEpoch = relayOwnerEpochs.getOrDefault(ownerId, 0)
    relayOwnerActiveRoutes.withValue(ownerId, routes):
      for entryEpoch in routes[].values:
        if entryEpoch == ownerEpoch:
          inc result
    relayOwnerTasks.withValue(ownerId, ownerTasks):
      if ownerTasks[].len > result:
        result = ownerTasks[].len

proc markRelayRouteReady(ownerId: int, route: string) {.gcsafe, raises: [].} =
  quicRelaySafe:
    let key = relayReadyRouteKey(ownerId, route)
    if key.len == 0:
      return
    withLock(relayRegistryLock):
      relayReadyRoutes.incl(key)
      recordRelayListenerStateLocked(ownerId, route, "", "ready")

proc clearRelayReadyRoute(ownerId: int, route: string) {.gcsafe, raises: [].} =
  quicRelaySafe:
    let key = relayReadyRouteKey(ownerId, route)
    if key.len == 0:
      return
    withLock(relayRegistryLock):
      relayReadyRoutes.excl(key)

proc markRelayRouteReadyIfCurrent*(
    ownerId: int,
    route: string,
    ownerEpoch: int
) {.gcsafe, raises: [].} =
  quicRelaySafe:
    let key = relayReadyRouteKey(ownerId, route)
    if key.len == 0:
      return
    withLock(relayRegistryLock):
      if not relayOwnerEpochMatchesLocked(ownerId, ownerEpoch):
        return
      relayReadyRoutes.incl(key)
      recordRelayListenerStateLocked(ownerId, route, "", "ready")

proc markRelayRouteReadyIfTaskCurrent*(
    ownerId: int,
    route: string,
    routeId: int,
    ownerEpoch: int
) {.gcsafe, raises: [].} =
  quicRelaySafe:
    let key = relayReadyRouteKey(ownerId, route)
    if key.len == 0:
      return
    withLock(relayRegistryLock):
      if not relayRouteTaskCurrentLocked(ownerId, route, routeId, ownerEpoch):
        return
      relayReadyRoutes.incl(key)
      recordRelayListenerStateLocked(ownerId, route, "", "ready")

proc clearRelayReadyRouteIfCurrent*(
    ownerId: int,
    route: string,
    ownerEpoch: int
) {.gcsafe, raises: [].} =
  quicRelaySafe:
    let key = relayReadyRouteKey(ownerId, route)
    if key.len == 0:
      return
    withLock(relayRegistryLock):
      if not relayOwnerEpochMatchesLocked(ownerId, ownerEpoch):
        return
      relayReadyRoutes.excl(key)

proc clearRelayReadyRouteIfTaskCurrent*(
    ownerId: int,
    route: string,
    routeId: int,
    ownerEpoch: int
) {.gcsafe, raises: [].} =
  quicRelaySafe:
    let key = relayReadyRouteKey(ownerId, route)
    if key.len == 0:
      return
    withLock(relayRegistryLock):
      if not relayRouteTaskCurrentLocked(ownerId, route, routeId, ownerEpoch):
        return
      relayReadyRoutes.excl(key)

proc clearRelayReadyRoutes(ownerId: int) {.gcsafe, raises: [].} =
  quicRelaySafe:
    if ownerId <= 0:
      return
    withLock(relayRegistryLock):
      clearRelayRouteKeysForOwnerLocked(relayReadyRoutes, ownerId)

proc markRelayListenerStage*(
    ownerId: int,
    route: string,
    kind: string,
    stage: string,
    detail = ""
) {.gcsafe, raises: [].} =
  quicRelaySafe:
    if ownerId <= 0 or route.len == 0 or stage.len == 0:
      return
    let routeKey = relayReadyRouteKey(ownerId, route)
    withLock(relayRegistryLock):
      if routeKey.len > 0:
        relayKnownRoutes.incl(routeKey)
        if relayListenerStageCountsAsReady(stage):
          relayLocallyUsableRoutes.incl(routeKey)
        else:
          relayLocallyUsableRoutes.excl(routeKey)
        let loweredStage = stage.strip().toLowerAscii()
        if loweredStage == "awaiting":
          relayAwaitingRoutes.incl(routeKey)
        else:
          relayAwaitingRoutes.excl(routeKey)
        if loweredStage in ["scheduled", "restarting"]:
          relayPendingRoutes.incl(routeKey)
        else:
          relayPendingRoutes.excl(routeKey)
        if loweredStage == "incoming":
          relayIncomingRoutes.incl(routeKey)
        else:
          relayIncomingRoutes.excl(routeKey)
        if loweredStage == "ready":
          relayReadyStageRoutes.incl(routeKey)
        else:
          relayReadyStageRoutes.excl(routeKey)
        if loweredStage == "bridge_attached":
          relayBridgeAttachedRoutes.incl(routeKey)
        else:
          relayBridgeAttachedRoutes.excl(routeKey)
        if loweredStage in ["incoming", "ready", "bridge_attached"]:
          relayServingRoutes.incl(routeKey)
        else:
          relayServingRoutes.excl(routeKey)
        if loweredStage == "dropped":
          relayDroppedRoutes.incl(routeKey)
        else:
          relayDroppedRoutes.excl(routeKey)
        if loweredStage in [
            "scheduled",
            "connecting",
            "registering",
            "awaiting",
            "incoming",
            "ready",
            "bridge_attached",
            "restarting"
          ]:
          relayRepairRequestedRoutes.excl(routeKey)
        recordRelayListenerStateLocked(ownerId, route, kind, stage, detail)

proc markRelayListenerStageIfCurrent*(
    ownerId: int,
    route: string,
    ownerEpoch: int,
    kind: string,
    stage: string,
    detail = ""
) {.gcsafe, raises: [].} =
  quicRelaySafe:
    if ownerId <= 0 or route.len == 0 or stage.len == 0:
      return
    let routeKey = relayReadyRouteKey(ownerId, route)
    withLock(relayRegistryLock):
      if not relayOwnerEpochMatchesLocked(ownerId, ownerEpoch):
        return
      if routeKey.len > 0:
        relayKnownRoutes.incl(routeKey)
        if relayListenerStageCountsAsReady(stage):
          relayLocallyUsableRoutes.incl(routeKey)
        else:
          relayLocallyUsableRoutes.excl(routeKey)
        let loweredStage = stage.strip().toLowerAscii()
        if loweredStage == "awaiting":
          relayAwaitingRoutes.incl(routeKey)
        else:
          relayAwaitingRoutes.excl(routeKey)
        if loweredStage in ["scheduled", "restarting"]:
          relayPendingRoutes.incl(routeKey)
        else:
          relayPendingRoutes.excl(routeKey)
        if loweredStage == "incoming":
          relayIncomingRoutes.incl(routeKey)
        else:
          relayIncomingRoutes.excl(routeKey)
        if loweredStage == "ready":
          relayReadyStageRoutes.incl(routeKey)
        else:
          relayReadyStageRoutes.excl(routeKey)
        if loweredStage == "bridge_attached":
          relayBridgeAttachedRoutes.incl(routeKey)
        else:
          relayBridgeAttachedRoutes.excl(routeKey)
        if loweredStage in ["incoming", "ready", "bridge_attached"]:
          relayServingRoutes.incl(routeKey)
        else:
          relayServingRoutes.excl(routeKey)
        if loweredStage == "dropped":
          relayDroppedRoutes.incl(routeKey)
        else:
          relayDroppedRoutes.excl(routeKey)
        if loweredStage in [
            "scheduled",
            "connecting",
            "registering",
            "awaiting",
            "incoming",
            "ready",
            "bridge_attached",
            "restarting"
          ]:
          relayRepairRequestedRoutes.excl(routeKey)
        recordRelayListenerStateLocked(ownerId, route, kind, stage, detail)

proc markRelayListenerStageIfTaskCurrent*(
    ownerId: int,
    route: string,
    routeId: int,
    ownerEpoch: int,
    kind: string,
    stage: string,
    detail = ""
) {.gcsafe, raises: [].} =
  quicRelaySafe:
    if ownerId <= 0 or route.len == 0 or routeId <= 0 or stage.len == 0:
      return
    let routeKey = relayReadyRouteKey(ownerId, route)
    withLock(relayRegistryLock):
      if not relayRouteTaskCurrentLocked(ownerId, route, routeId, ownerEpoch):
        return
      if routeKey.len > 0:
        relayKnownRoutes.incl(routeKey)
        if relayListenerStageCountsAsReady(stage):
          relayLocallyUsableRoutes.incl(routeKey)
        else:
          relayLocallyUsableRoutes.excl(routeKey)
        let loweredStage = stage.strip().toLowerAscii()
        if loweredStage == "awaiting":
          relayAwaitingRoutes.incl(routeKey)
        else:
          relayAwaitingRoutes.excl(routeKey)
        if loweredStage in ["scheduled", "restarting"]:
          relayPendingRoutes.incl(routeKey)
        else:
          relayPendingRoutes.excl(routeKey)
        if loweredStage == "incoming":
          relayIncomingRoutes.incl(routeKey)
        else:
          relayIncomingRoutes.excl(routeKey)
        if loweredStage == "ready":
          relayReadyStageRoutes.incl(routeKey)
        else:
          relayReadyStageRoutes.excl(routeKey)
        if loweredStage == "bridge_attached":
          relayBridgeAttachedRoutes.incl(routeKey)
        else:
          relayBridgeAttachedRoutes.excl(routeKey)
        if loweredStage in ["incoming", "ready", "bridge_attached"]:
          relayServingRoutes.incl(routeKey)
        else:
          relayServingRoutes.excl(routeKey)
        if loweredStage == "dropped":
          relayDroppedRoutes.incl(routeKey)
        else:
          relayDroppedRoutes.excl(routeKey)
        if loweredStage in [
            "scheduled",
            "connecting",
            "registering",
            "awaiting",
            "incoming",
            "ready",
            "bridge_attached",
            "restarting"
          ]:
          relayRepairRequestedRoutes.excl(routeKey)
        recordRelayListenerStateLocked(ownerId, route, kind, stage, detail)

proc relayRememberRouteTaskForTests*(
    ownerId: int,
    route: string,
    routeId: int,
    ownerEpoch: int
) {.gcsafe, raises: [].} =
  quicRelaySafe:
    if ownerId <= 0 or route.len == 0 or routeId <= 0 or ownerEpoch <= 0:
      return
    withLock(relayRegistryLock):
      relayOwnerEpochs[ownerId] = ownerEpoch
      relayOwnerActiveRoutes.mgetOrPut(
        ownerId,
        initTable[string, int]()
      )[ownedText(route)] = ownerEpoch
      relayOwnerRouteTaskIds.mgetOrPut(
        ownerId,
        initTable[string, int]()
      )[ownedText(route)] = routeId

proc relayListenerStateSnapshots*(
    ownerId: int,
    expectedRoutes: openArray[string]
): seq[TsnetRelayListenerDiagnostic] {.gcsafe, raises: [].} =
  quicRelaySafe:
    if ownerId <= 0:
      return
    withLock(relayRegistryLock):
      if expectedRoutes.len > 0:
        var seen = initHashSet[string]()
        for route in expectedRoutes:
          if route.len == 0 or route in seen:
            continue
          seen.incl(route)
          let stage = relayListenerStageLabelLocked(ownerId, route)
          if stage.len == 0:
            continue
          result.add(TsnetRelayListenerDiagnostic(
            route: ownedText(route),
            kind: normalizedRelayKind("", route),
            stage: ownedText(stage),
            detail: "",
            updatedUnixMilli: currentUnixMilli()
          ))
      else:
        for route in collectRelayOwnerRoutesLocked(ownerId):
          let stage = relayListenerStageLabelLocked(ownerId, route)
          if stage.len == 0:
            continue
          result.add(TsnetRelayListenerDiagnostic(
            route: ownedText(route),
            kind: normalizedRelayKind("", route),
            stage: ownedText(stage),
            detail: "",
            updatedUnixMilli: currentUnixMilli()
          ))

proc relayListenerStateSnapshots*(
    ownerId: int
): seq[TsnetRelayListenerDiagnostic] {.gcsafe, raises: [].} =
  relayListenerStateSnapshots(ownerId, [])

proc relayListenerStageSnapshots*(
    ownerId: int
): seq[TsnetRelayListenerStageSnapshot] {.gcsafe, raises: [].} =
  quicRelaySafe:
    if ownerId <= 0:
      return
    for state in relayListenerStateSnapshots(ownerId):
      result.add(TsnetRelayListenerStageSnapshot(
        route: state.route,
        stage: state.stage
      ))

proc relayListenerStatesPayload*(
    ownerId: int,
    expectedRoutes: openArray[string]
): JsonNode {.gcsafe, raises: [].} =
  result = newJArray()
  for state in relayListenerStateSnapshots(ownerId, expectedRoutes):
    result.add(%*{
      "route": state.route,
      "kind": state.kind,
      "stage": state.stage,
      "detail": state.detail,
      "updatedUnixMilli": state.updatedUnixMilli,
    })

proc relayListenerStatesPayload*(ownerId: int): JsonNode {.gcsafe, raises: [].} =
  relayListenerStatesPayload(ownerId, [])

proc relayPublishedRouteTexts*(
    ownerId: int,
    expectedRoutes: openArray[string]
): seq[string] {.gcsafe, raises: [].} =
  quicRelaySafe:
    if ownerId <= 0:
      return
    withLock(relayRegistryLock):
      var seen = initHashSet[string]()
      for route in expectedRoutes:
        if route.len == 0 or route in seen:
          continue
        seen.incl(route)
        let stage = relayListenerStageLabelLocked(ownerId, route)
        if stage.len == 0:
          continue
        if relayRouteCountsAsPublishedLocked(ownerId, route, stage):
          result.add(ownedText(route))

proc markUdpDialProgress(
    ownerId: int,
    rawKey: string,
    phase: string,
    detail = "",
    attempts = 0
) {.gcsafe, raises: [].} =
  quicRelaySafe:
    if ownerId <= 0 or rawKey.len == 0:
      return
    let dialKey = ownedText(rawKey)
    quicRelayTrace(
      "udp dial state update owner=" & $ownerId &
      " rawKey=" & rawKey &
      " phase=" & phase &
      " detail=" & detail &
      " attempts=" & $attempts
    )
    withLock(relayRegistryLock):
      relayUdpDialStates.mgetOrPut(
        ownerId,
        initTable[string, TsnetUdpDialDiagnostic]()
      )[dialKey] = TsnetUdpDialDiagnostic(
        phase: ownedText(phase),
        detail: ownedText(detail),
        attempts: max(0, attempts),
        updatedUnixMilli: currentUnixMilli()
      )

proc markUdpDialPending(ownerId: int, rawKey: string) {.gcsafe, raises: [].} =
  markUdpDialProgress(ownerId, rawKey, "pending")

proc markUdpDialReady(ownerId: int, rawKey: string) {.gcsafe, raises: [].} =
  markUdpDialProgress(ownerId, rawKey, "ready")

proc markUdpDialFailed(ownerId: int, rawKey: string, error: string) {.gcsafe, raises: [].} =
  markUdpDialProgress(
    ownerId,
    rawKey,
    "failed",
    if error.len > 0: error else: "udp_dial_failed"
  )

proc udpDialState*(
    ownerId: int,
    rawKey: string
): tuple[
    known, ready: bool,
    error, phase, detail: string,
    attempts: int,
    updatedUnixMilli: int64
  ] {.gcsafe, raises: [].} =
  quicRelaySafe:
    if ownerId <= 0 or rawKey.len == 0:
      return
    withLock(relayRegistryLock):
      if relayUdpDialStates.hasKey(ownerId):
        let states =
          relayUdpDialStates.mgetOrPut(
            ownerId,
            initTable[string, TsnetUdpDialDiagnostic]()
          )
        var found = false
        for existingKey, value in states.pairs():
          if existingKey != rawKey:
            continue
          found = true
          result.known = true
          result.phase = value.phase
          result.detail = value.detail
          result.attempts = value.attempts
          result.updatedUnixMilli = value.updatedUnixMilli
          if value.phase == "ready" or value.phase == "active":
            result.ready = true
          elif value.phase == "failed":
            result.error =
              if value.detail.len > 0: value.detail
              else: "udp_dial_failed"
          break
        if not found:
          quicRelayTrace(
            "udp dial state lookup miss owner=" & $ownerId &
            " rawKey=" & rawKey &
            " knownStates=" & $states.len
          )

proc udpDialStatesPayload*(ownerId: int): JsonNode {.gcsafe, raises: [].} =
  result = newJArray()
  quicRelaySafe:
    if ownerId <= 0:
      return
    withLock(relayRegistryLock):
      if relayUdpDialStates.hasKey(ownerId):
        let states =
          relayUdpDialStates.mgetOrPut(
            ownerId,
            initTable[string, TsnetUdpDialDiagnostic]()
          )
        for rawKey, state in states.pairs():
          result.add(%*{
            "rawKey": rawKey,
            "phase": state.phase,
            "detail": state.detail,
            "attempts": state.attempts,
            "updatedUnixMilli": state.updatedUnixMilli,
            "ready": state.phase == "ready" or state.phase == "active",
          })

proc udpRelayRouteReady*(ownerId: int, route: string): bool {.gcsafe, raises: [].} =
  quicRelaySafe:
    let key = relayReadyRouteKey(ownerId, route)
    if key.len == 0:
      return false
    withLock(relayRegistryLock):
      result = key in relayReadyRoutes

proc relayRouteReady*(ownerId: int, route: string): bool {.gcsafe, raises: [].} =
  udpRelayRouteReady(ownerId, route)

proc relayShouldPreserveActiveRouteAfterProbeFailure*(
    preserveServingRoute: bool,
    routeLocallyReady: bool,
    error: string
): bool {.gcsafe, raises: [].} =
  if not preserveServingRoute or not routeLocallyReady:
    return false
  let lowered = error.strip().toLowerAscii()
  lowered.startsWith("nim_quic relay route_status connect failed:") or
    lowered.startsWith("nim_quic relay route_status raised:")

proc relayRouteReadyOrPublished*(
    ownerId: int,
    endpointUrl: string,
    route: string,
    runtimePreference: qrt.QuicRuntimePreference = qrt.qrpBuiltinOnly
): bool {.gcsafe, raises: [].} =
  let previousStage = relayRouteStateLabel(ownerId, route).toLowerAscii()
  let preserveServingRoute =
    relayShouldPreserveServingRoute(ownerId, route)
  let preserveUnpublishedServingRoute =
    relayShouldPreserveUnpublishedServingRoute(ownerId, route)
  let hadLocalReady = relayRouteLocallyReady(ownerId, route)
  if route.len == 0 or endpointUrl.strip().len == 0:
    if preserveServingRoute:
      markRelayRouteReady(ownerId, route)
      return true
    if hadLocalReady or previousStage.len > 0:
      clearRelayReadyRoute(ownerId, route)
      markRelayListenerStage(
        ownerId,
        route,
        normalizedRelayKind("", route),
        "dropped",
        "route_status_unavailable"
      )
    return false
  let endpointRes = quicRelayEndpoint(endpointUrl)
  if endpointRes.isErr():
    if preserveServingRoute:
      markRelayRouteReady(ownerId, route)
      return true
    if hadLocalReady or previousStage.len > 0:
      clearRelayReadyRoute(ownerId, route)
      markRelayListenerStage(
        ownerId,
        route,
        normalizedRelayKind("", route),
        "dropped",
        endpointRes.error
      )
    return false
  let published = probeRelayRoutePublished(
    endpointRes.get(),
    route,
    runtimePreference
  )
  quicRelayTrace(
    "relay route ready check owner=" & $ownerId &
    " route=" & route &
    " hadLocalReady=" & $hadLocalReady &
    " previousStage=" & previousStage &
    " publishedOk=" & $published.isOk() &
    " published=" & $(if published.isOk(): published.get() else: false) &
    " err=" & (if published.isErr(): published.error else: "")
  )
  if published.isOk() and published.get():
    markRelayRouteReady(ownerId, route)
    return true
  if published.isOk() and not published.get() and preserveUnpublishedServingRoute:
    quicRelayTrace(
      "relay route ready check preserving locally serving unpublished route owner=" &
      $ownerId &
      " route=" & route &
      " stage=" & previousStage
    )
    markRelayRouteReady(ownerId, route)
    return true
  if published.isErr() and relayShouldPreserveActiveRouteAfterProbeFailure(
      preserveServingRoute,
      hadLocalReady,
      published.error
    ):
    quicRelayTrace(
      "relay route ready check preserving active route after probe transport failure owner=" &
      $ownerId &
      " route=" & route &
      " stage=" & previousStage &
      " err=" & published.error
    )
    markRelayRouteReady(ownerId, route)
    return true
  clearRelayReadyRoute(ownerId, route)
  if hadLocalReady or previousStage.len > 0:
    markRelayListenerStage(
      ownerId,
      route,
      normalizedRelayKind("", route),
      "dropped",
      if published.isOk(): "route_not_published" else: published.error
    )
  false

proc stopRelayListeners*(ownerId: int) =
  if ownerId <= 0:
    return
  let stoppedTasks = relayTakeOwnerTasks(ownerId)
  withLock(relayRegistryLock):
    relayOwnerEpochs[ownerId] = relayOwnerEpochs.getOrDefault(ownerId, 0) + 1
    relayOwnerActiveRoutes.del(ownerId)
    clearRelayRouteKeysForOwnerLocked(relayReadyRoutes, ownerId)
    clearRelayRouteKeysForOwnerLocked(relayLocallyUsableRoutes, ownerId)
    clearRelayRouteKeysForOwnerLocked(relayPendingRoutes, ownerId)
    clearRelayRouteKeysForOwnerLocked(relayAwaitingRoutes, ownerId)
    clearRelayRouteKeysForOwnerLocked(relayIncomingRoutes, ownerId)
    clearRelayRouteKeysForOwnerLocked(relayReadyStageRoutes, ownerId)
    clearRelayRouteKeysForOwnerLocked(relayBridgeAttachedRoutes, ownerId)
    clearRelayRouteKeysForOwnerLocked(relayServingRoutes, ownerId)
    clearRelayRouteKeysForOwnerLocked(relayRepairRequestedRoutes, ownerId)
    clearRelayRouteKeysForOwnerLocked(relayKnownRoutes, ownerId)
    clearRelayRouteKeysForOwnerLocked(relayDroppedRoutes, ownerId)
    relayUdpDialStates.del(ownerId)
    relayListenerStates.del(ownerId)
  for handle in stoppedTasks:
    relayStopTaskHandle(handle)
  closeOwnerRelayClients(ownerId)

proc new*(_: type[TsnetQuicRelayGateway]): TsnetQuicRelayGateway =
  result = TsnetQuicRelayGateway(
    boundPort: NimTsnetQuicRelayDefaultPort,
    listeners: initTable[string, TsnetQuicRelayListener](),
    pendingBridges: initTable[string, TsnetQuicRelayPendingBridge]()
  )
  initLock(result.listenersLock)
  result.listenersLockInit = true

proc nextRelayListenerGeneration(gateway: TsnetQuicRelayGateway): string =
  if gateway.isNil:
    return ""
  inc gateway.listenerGenerationCounter
  "listener-" & $gateway.listenerGenerationCounter

proc storeListener(
    gateway: TsnetQuicRelayGateway,
    route: string,
    connPtr: pointer,
    connState: msquicdrv.MsQuicConnectionState,
    candidates: seq[string] = @[],
    stream: MsQuicStream = nil
) : string =
  if gateway.isNil:
    return ""
  var staleReady: Future[JsonNode] = nil
  var replacedGeneration = false
  acquire(gateway.listenersLock)
  try:
    result = gateway.nextRelayListenerGeneration()
    let listener = gateway.listeners.getOrDefault(route, nil)
    if listener.isNil:
      gateway.listeners[ownedText(route)] = TsnetQuicRelayListener(
        connPtr: connPtr,
        connState: connState,
        candidates: candidates,
        stream: stream,
        generation: result,
        persistentControlStream: false,
        published: false,
        awaiting: false,
        readyFuture: nil,
        busy: false
      )
    else:
      staleReady = listener.readyFuture
      replacedGeneration = listener.generation.len > 0
      listener.connPtr = connPtr
      listener.connState = connState
      listener.candidates = candidates
      listener.stream = stream
      listener.generation = result
      listener.persistentControlStream = false
      listener.published = false
      listener.awaiting = false
      listener.readyFuture = nil
      listener.busy = false
  finally:
    release(gateway.listenersLock)
  if replacedGeneration:
    quicRelayTrace(
      "gateway replacing stale listener route=" & route &
      " reason=storeListener_generation"
    )
  if not staleReady.isNil and not staleReady.finished():
    staleReady.fail(
      newException(
        IOError,
        "nim_quic relay listener generation replaced during listen registration"
      )
    )

proc attachListenerStream(
    gateway: TsnetQuicRelayGateway,
    route: string,
    generation: string,
    connPtr: pointer,
    connState: msquicdrv.MsQuicConnectionState,
    stream: MsQuicStream
): string =
  if gateway.isNil or route.len == 0 or generation.len == 0 or stream.isNil:
    return "route_not_registered"
  var staleReady: Future[JsonNode] = nil
  var replacedStalePersistent = false
  acquire(gateway.listenersLock)
  try:
    let listener = gateway.listeners.getOrDefault(route, nil)
    if listener.isNil:
      return "route_not_registered"
    if listener.generation != generation:
      return "stale_listener_generation"
    let stalePersistent =
      listener.persistentControlStream and
      listener.stream != stream and
      not listenerConnectionUsable(listener)
    if listener.persistentControlStream and
        not listener.stream.isNil and
        listener.stream != stream and
        (listener.busy or
          (not listener.readyFuture.isNil and not listener.readyFuture.finished())) and
        not stalePersistent:
      return "listener_busy"
    staleReady = listener.readyFuture
    replacedStalePersistent = stalePersistent
    listener.connPtr = connPtr
    listener.connState = connState
    listener.stream = stream
    listener.generation = generation
    listener.persistentControlStream = true
    listener.published = false
    listener.awaiting = false
    listener.readyFuture = nil
    listener.busy = false
  finally:
    release(gateway.listenersLock)
  if replacedStalePersistent:
    quicRelayTrace(
      "gateway replacing stale listener route=" & route &
      " reason=attachListenerStream"
    )
  if not staleReady.isNil and not staleReady.finished():
    staleReady.fail(
      newException(
        IOError,
        "nim_quic relay listener control stream was replaced before ready"
      )
    )
  ""

proc detachListenerStream(
    gateway: TsnetQuicRelayGateway,
    route: string,
    stream: MsQuicStream,
    err: string
) =
  if gateway.isNil or route.len == 0 or stream.isNil:
    return
  var staleReady: Future[JsonNode] = nil
  var unpublishRoute = false
  acquire(gateway.listenersLock)
  try:
    let listener = gateway.listeners.getOrDefault(route, nil)
    if listener.isNil or listener.stream != stream:
      return
    quicRelayTrace(
      "gateway listener stream detach route=" & route &
      " persistent=" & $listener.persistentControlStream &
      " awaiting=" & $listener.awaiting &
      " busy=" & $listener.busy &
      " err=" & (if err.len > 0: err else: "<empty>")
    )
    staleReady = listener.readyFuture
    if listener.persistentControlStream:
      unpublishRoute = true
      gateway.listeners.del(route)
    else:
      listener.stream = nil
      listener.persistentControlStream = false
      listener.published = false
      listener.awaiting = false
      listener.readyFuture = nil
  finally:
    release(gateway.listenersLock)
  if not staleReady.isNil and not staleReady.finished():
    staleReady.fail(
      newException(
        IOError,
        if err.len > 0: err else: "nim_quic relay listener control stream detached"
      )
    )
  if unpublishRoute:
    quicRelayTrace("gateway listener unpublished route=" & route)

proc markListenerAwaiting(
    gateway: TsnetQuicRelayGateway,
    route: string,
    generation: string,
    stream: MsQuicStream
): bool =
  if gateway.isNil or route.len == 0 or generation.len == 0 or stream.isNil:
    return false
  acquire(gateway.listenersLock)
  try:
    let listener = gateway.listeners.getOrDefault(route, nil)
    if listener.isNil or listener.stream != stream or listener.generation != generation:
      return false
    listener.published = true
    listener.awaiting = true
    return true
  finally:
    release(gateway.listenersLock)

proc beginPersistentListenerIncoming(
    gateway: TsnetQuicRelayGateway,
    route: string
): Result[tuple[stream: MsQuicStream, readyFuture: Future[JsonNode]], string] =
  if gateway.isNil or route.len == 0:
    return err("listener_not_ready")
  acquire(gateway.listenersLock)
  try:
    let listener = gateway.listeners.getOrDefault(route, nil)
    if listener.isNil:
      quicRelayTrace("gateway listener incoming begin route=" & route & " err=missing_listener")
      return err("listener_not_ready")
    if listener.stream.isNil:
      quicRelayTrace("gateway listener incoming begin route=" & route & " err=missing_stream")
      return err("listener_not_ready")
    if not listener.persistentControlStream:
      quicRelayTrace("gateway listener incoming begin route=" & route & " err=not_persistent")
      return err("listener_not_ready")
    if not listenerConnectionUsable(listener):
      quicRelayTrace("gateway listener incoming begin route=" & route & " err=unusable_listener")
      return err("listener_not_ready")
    if not listener.awaiting:
      quicRelayTrace("gateway listener incoming begin route=" & route & " err=not_awaiting")
      return err("listener_not_ready")
    if not listener.readyFuture.isNil and not listener.readyFuture.finished():
      quicRelayTrace("gateway listener incoming begin route=" & route & " err=busy")
      return err("listener_busy")
    listener.awaiting = false
    listener.readyFuture = Future[JsonNode].init("tsnet.quicrelay.listener.ready")
    quicRelayTrace("gateway listener incoming begin route=" & route & " ok=true")
    return ok((listener.stream, listener.readyFuture))
  finally:
    release(gateway.listenersLock)

proc completePersistentListenerReady(
    gateway: TsnetQuicRelayGateway,
    route: string,
    generation: string,
    stream: MsQuicStream,
    payload: JsonNode
): bool =
  if gateway.isNil or route.len == 0 or generation.len == 0 or stream.isNil:
    return false
  var readyFuture: Future[JsonNode] = nil
  acquire(gateway.listenersLock)
  try:
    let listener = gateway.listeners.getOrDefault(route, nil)
    if listener.isNil or listener.stream != stream or listener.generation != generation:
      return false
    readyFuture = listener.readyFuture
    listener.readyFuture = nil
  finally:
    release(gateway.listenersLock)
  if readyFuture.isNil or readyFuture.finished():
    return false
  readyFuture.complete(payload)
  true

proc allocatePendingBridgeSession(
    gateway: TsnetQuicRelayGateway,
    route: string,
    kind: string
): tuple[sessionId: string, future: Future[MsQuicStream]] =
  if gateway.isNil or route.len == 0:
    return ("", nil)
  acquire(gateway.listenersLock)
  try:
    inc gateway.pendingBridgeCounter
    let sessionId =
      route & "#bridge-" & $gateway.pendingBridgeCounter &
      "-" & $currentUnixMilli()
    let future = Future[MsQuicStream].init("tsnet.quicrelay.bridge.attach")
    gateway.pendingBridges[sessionId] = TsnetQuicRelayPendingBridge(
      route: route,
      kind: kind,
      stream: nil,
      future: future
    )
    (sessionId, future)
  finally:
    release(gateway.listenersLock)

proc clearPendingBridgeSession(
    gateway: TsnetQuicRelayGateway,
    sessionId: string
) =
  if gateway.isNil or sessionId.len == 0:
    return
  acquire(gateway.listenersLock)
  try:
    let pending = gateway.pendingBridges.getOrDefault(sessionId, nil)
    if not pending.isNil and not pending.future.isNil and not pending.future.finished():
      pending.future.fail(
        newException(
          IOError,
          "nim_quic relay pending bridge session cleared before attachment"
        )
      )
    gateway.pendingBridges.del(sessionId)
  finally:
    release(gateway.listenersLock)

proc attachPendingBridgeSession(
    gateway: TsnetQuicRelayGateway,
    sessionId: string,
    route: string,
    kind: string,
    stream: MsQuicStream
): string =
  if gateway.isNil:
    return "gateway_unavailable"
  if sessionId.len == 0:
    return "missing_bridge_session_id"
  if stream.isNil:
    return "bridge_stream_unavailable"
  acquire(gateway.listenersLock)
  try:
    let pending = gateway.pendingBridges.getOrDefault(sessionId, nil)
    if pending.isNil:
      return "bridge_session_not_found"
    if pending.route != route:
      return "bridge_route_mismatch"
    if pending.kind != kind:
      return "bridge_kind_mismatch"
    if not pending.stream.isNil:
      return "bridge_session_already_attached"
    pending.stream = stream
    ""
  finally:
    release(gateway.listenersLock)

proc confirmPendingBridgeSession(
    gateway: TsnetQuicRelayGateway,
    sessionId: string,
    route: string,
    kind: string,
    stream: MsQuicStream
): string =
  if gateway.isNil:
    return "gateway_unavailable"
  if sessionId.len == 0:
    return "missing_bridge_session_id"
  if stream.isNil:
    return "bridge_stream_unavailable"
  acquire(gateway.listenersLock)
  try:
    let pending = gateway.pendingBridges.getOrDefault(sessionId, nil)
    if pending.isNil:
      return "bridge_session_not_found"
    if pending.route != route:
      return "bridge_route_mismatch"
    if pending.kind != kind:
      return "bridge_kind_mismatch"
    if pending.future.isNil:
      return "bridge_session_future_unavailable"
    if pending.stream != stream:
      return "bridge_stream_mismatch"
    if pending.future.finished():
      return "bridge_session_already_confirmed"
    pending.future.complete(stream)
    ""
  finally:
    release(gateway.listenersLock)

proc acquireListenerRoute(
    gateway: TsnetQuicRelayGateway,
    route: string
): tuple[listener: TsnetQuicRelayListener, acquired: bool] =
  if gateway.isNil or route.len == 0:
    return (nil, false)
  acquire(gateway.listenersLock)
  try:
    let listener = gateway.listeners.getOrDefault(route, nil)
    if listener.isNil:
      return (nil, false)
    if not listenerConnectionUsable(listener):
      return (listener, false)
    if listener.busy:
      return (listener, false)
    listener.busy = true
    (listener, true)
  finally:
    release(gateway.listenersLock)

proc releaseListenerRoute(
    gateway: TsnetQuicRelayGateway,
    route: string
) =
  if gateway.isNil or route.len == 0:
    return
  acquire(gateway.listenersLock)
  try:
    let listener = gateway.listeners.getOrDefault(route, nil)
    if not listener.isNil:
      listener.busy = false
  finally:
    release(gateway.listenersLock)

proc getListener(
    gateway: TsnetQuicRelayGateway,
    route: string
): TsnetQuicRelayListener =
  if gateway.isNil:
    return nil
  acquire(gateway.listenersLock)
  try:
    result = gateway.listeners.getOrDefault(route, nil)
  finally:
    release(gateway.listenersLock)

proc listenerRoutePublished(listener: TsnetQuicRelayListener): bool =
  if listener.isNil or
      not listener.persistentControlStream or
      listener.stream.isNil or
      not listenerConnectionUsable(listener):
    return false
  listener.published

proc listenerConnectionUsable(
    listener: TsnetQuicRelayListener
): bool {.gcsafe, raises: [].} =
  if listener.isNil:
    return false
  if listener.connState.isNil or listener.connState.closed:
    return false
  if not relayHandshakeComplete(listener.connState):
    return false
  let reason = relayCloseReason(listener.connState)
  if reason.len > 0:
    return false
  if listener.persistentControlStream:
    if listener.stream.isNil:
      return false
    return listener.stream.connectionUsable()
  true

proc listenerRouteStatusDetail(listener: TsnetQuicRelayListener): string =
  if listener.isNil:
    return "listener=nil"
  let readyPending =
    not listener.readyFuture.isNil and
    not listener.readyFuture.finished()
  let connectionUsable = listenerConnectionUsable(listener)
  "listener=nil=false" &
    " persistent=" & $listener.persistentControlStream &
    " streamNil=" & $listener.stream.isNil &
    " connUsable=" & $connectionUsable &
    " published=" & $listener.published &
    " awaiting=" & $listener.awaiting &
    " busy=" & $listener.busy &
    " readyPending=" & $readyPending

proc clearListenerRoutesForConnection(
    gateway: TsnetQuicRelayGateway,
    connPtr: pointer
) =
  if gateway.isNil or connPtr.isNil:
    return
  var removed: seq[string] = @[]
  acquire(gateway.listenersLock)
  try:
    var kept = initTable[string, TsnetQuicRelayListener]()
    for route, listener in gateway.listeners.pairs():
      if listener.isNil or listener.connPtr != connPtr:
        kept[ownedText(route)] = listener
      else:
        removed.add(route)
    gateway.listeners = kept
  finally:
    release(gateway.listenersLock)
  if removed.len > 0:
    quicRelayTrace(
      "gateway cleared listener routes connPtr=" & $cast[uint](connPtr) &
      " routes=" & removed.join(",")
    )

proc clearListeners(gateway: TsnetQuicRelayGateway) =
  if gateway.isNil:
    return
  acquire(gateway.listenersLock)
  try:
    gateway.listeners.clear()
  finally:
    release(gateway.listenersLock)

proc clearListenerRoute(
    gateway: TsnetQuicRelayGateway,
    route: string
) =
  if gateway.isNil or route.len == 0:
    return
  acquire(gateway.listenersLock)
  try:
    gateway.listeners.del(route)
  finally:
    release(gateway.listenersLock)

proc dropListenerRouteForTests*(
    gateway: TsnetQuicRelayGateway,
    route: string
) =
  gateway.clearListenerRoute(route)

proc injectStalePersistentListenerForTests*(
    gateway: TsnetQuicRelayGateway,
    route: string,
    busy = true,
    readyPending = true
) =
  if gateway.isNil or route.len == 0:
    return
  var readyFuture: Future[JsonNode] = nil
  if readyPending:
    readyFuture = Future[JsonNode].init("tsnet.quicrelay.listener.ready.stale")
  acquire(gateway.listenersLock)
  try:
    gateway.listeners[ownedText(route)] = TsnetQuicRelayListener(
      connPtr: cast[pointer](1),
      connState: nil,
      candidates: @[],
      stream: newCachedMsQuicStreamForTests(@[]),
      generation: "test-generation",
      persistentControlStream: true,
      published: true,
      awaiting: true,
      readyFuture: readyFuture,
      busy: busy
    )
  finally:
    release(gateway.listenersLock)

proc serveRelayStream(
    gateway: TsnetQuicRelayGateway,
    connPtr: pointer,
    connState: msquicdrv.MsQuicConnectionState,
    stream: MsQuicStream
) {.async: (raises: []).} =
  var keepOpen = false
  var pendingBridgeSessionId = ""
  try:
    let request = await readJsonMessage(stream)
    let mode = jsonString(request, "mode")
    let route = jsonString(request, "route")
    let candidates = jsonStrings(request, "candidates")
    let kind = normalizedRelayKind(jsonString(request, "kind"), route)
    quicRelayTrace("gateway request mode=" & mode & " route=" & route)
    case mode
    of "listen":
      let generation = gateway.storeListener(
        route,
        connPtr,
        connState,
        candidates,
        nil
      )
      quicRelayTrace("gateway stored listener route=" & route)
      await writeJsonMessageWithTimeout(stream, rpcSuccess(%*{
          "mode": "listen",
          "route": route,
          "candidates": candidates,
          "generation": generation
      }), QuicRelayRpcTimeout, "gateway listen ack")
    of "accept":
      let generation = jsonString(request, "generation")
      let attachError = gateway.attachListenerStream(route, generation, connPtr, connState, stream)
      if attachError.len > 0:
        quicRelayTrace(
          "gateway accept rejected route=" & route &
          " err=" & attachError
        )
        await writeJsonMessage(stream, rpcError(attachError))
        return
      # Once the accept stream is attached, ownership belongs to the listener
      # route. Do not leave it in the short-lived request/response lifecycle;
      # otherwise a cancellation during the initial ack write can wrongly fall
      # through `finally` and close the long-lived stream.
      keepOpen = true
      quicRelayTrace("gateway accept stream attached route=" & route)
      await writeJsonMessageWithTimeout(stream, rpcSuccess(%*{
        "mode": "accept",
        "route": route
      }), QuicRelayAcceptAckTimeout, "gateway accept ack")
      quicRelayTrace("gateway accept ack sent route=" & route)
      try:
        while gateway.running:
          let controlMsg = await readJsonMessage(stream)
          if controlMsg.isNil or controlMsg.kind != JObject:
            raise newException(
              IOError,
              "nim_quic relay accept stream received invalid control payload"
            )
          let op = jsonString(controlMsg, "op")
          case op
          of "await":
            let generation = jsonString(controlMsg, "generation")
            if not gateway.markListenerAwaiting(route, generation, stream):
              raise newException(
                IOError,
                "nim_quic relay accept stream lost attached listener route"
              )
            await writeJsonMessageWithTimeout(stream, rpcSuccess(%*{
              "op": "await",
              "route": route,
              "published": true,
              "generation": generation
            }), QuicRelayAcceptAckTimeout, "gateway await ack")
            quicRelayTrace("gateway listener await registered route=" & route)
          of "ready":
            let generation = jsonString(controlMsg, "generation")
            if gateway.completePersistentListenerReady(route, generation, stream, controlMsg):
              quicRelayTrace("gateway listener ready signaled route=" & route)
            else:
              quicRelayTrace("gateway listener ready ignored route=" & route)
          else:
            quicRelayTrace(
              "gateway accept stream ignored route=" & route &
              " op=" & op
            )
      finally:
        gateway.detachListenerStream(
          route,
          stream,
          "nim_quic relay accept stream closed before next control message"
        )
        await stream.closeQuicStream()
      return
    of "route_status":
      let listener = gateway.getListener(route)
      let published = listenerRoutePublished(listener)
      if not published:
        quicRelayTrace(
          "gateway route_status false route=" & route &
          " " & listenerRouteStatusDetail(listener)
        )
      await writeJsonMessage(stream, rpcSuccess(%*{
        "mode": "route_status",
        "route": route,
        "published": published
      }))
    of "bridge_attach":
      let sessionId = jsonString(request, "sessionId")
      let attachError = gateway.attachPendingBridgeSession(
        sessionId,
        route,
        kind,
        stream
      )
      if attachError.len > 0:
        quicRelayTrace(
          "gateway bridge attach rejected route=" & route &
          " sessionId=" & sessionId &
          " err=" & attachError
        )
        await writeJsonMessage(stream, rpcError(attachError))
        return
      keepOpen = true
      pendingBridgeSessionId = sessionId
      try:
        await writeJsonMessageWithTimeout(stream, rpcSuccess(%*{
          "mode": "bridge_attach",
          "route": route,
          "sessionId": sessionId
        }), QuicRelayRpcTimeout, "gateway bridge attach ack")
      except CatchableError as exc:
        gateway.clearPendingBridgeSession(sessionId)
        quicRelayTrace(
          "gateway bridge attach ack failed route=" & route &
          " sessionId=" & sessionId &
          " err=" & exc.msg
        )
        return
      let confirmError = gateway.confirmPendingBridgeSession(
        sessionId,
        route,
        kind,
        stream
      )
      if confirmError.len > 0:
        gateway.clearPendingBridgeSession(sessionId)
        quicRelayTrace(
          "gateway bridge attach confirm failed route=" & route &
          " sessionId=" & sessionId &
          " err=" & confirmError
        )
        return
      quicRelayTrace(
        "gateway bridge attach accepted route=" & route &
        " sessionId=" & sessionId
      )
    of "ping":
      await writeJsonMessage(stream, rpcSuccess(%*{
        "mode": "ping"
      }))
    of "dial":
      let (listener, acquired) = gateway.acquireListenerRoute(route)
      if listener.isNil:
        quicRelayTrace("gateway missing listener route=" & route)
        await writeJsonMessage(stream, rpcError("route_not_registered"))
        return
      if not acquired:
        quicRelayTrace("gateway listener busy route=" & route)
        await writeJsonMessage(stream, rpcError("listener_busy"))
        return
      let source = jsonString(request, "source")
      quicRelayTrace("gateway dial matched route=" & route & " source=" & source)
      var listenerControlStream: MsQuicStream = nil
      var listenerDataStream: MsQuicStream = nil
      var routeLockHeld = true
      var bridgeAttachFuture: Future[MsQuicStream] = nil
      var listenerReadyFuture: Future[JsonNode] = nil
      try:
        let bridgeSession = gateway.allocatePendingBridgeSession(route, kind)
        pendingBridgeSessionId = bridgeSession.sessionId
        bridgeAttachFuture = bridgeSession.future
        if pendingBridgeSessionId.len == 0 or bridgeAttachFuture.isNil:
          await writeJsonMessage(stream, rpcError("bridge_session_unavailable"))
          return
        let persistentIncoming = gateway.beginPersistentListenerIncoming(route).valueOr:
          quicRelayTrace("gateway listener not awaiting route=" & route & " err=" & error)
          await writeJsonMessage(stream, rpcError(error))
          return
        listenerControlStream = persistentIncoming.stream
        listenerReadyFuture = persistentIncoming.readyFuture
        quicRelayTrace("gateway notifying listener route=" & route & " controlStream=persistent")
        var ready: JsonNode = nil
        try:
          quicRelayTrace("gateway listener incoming_write_begin route=" & route)
          await writeJsonMessageWithTimeout(listenerControlStream, %*{
            "op": "incoming",
            "route": route,
            "source": source,
            "candidates": candidates,
            "sessionId": pendingBridgeSessionId
          }, QuicRelayAcceptAckTimeout, "gateway incoming")
          quicRelayTrace("gateway listener incoming_write_done route=" & route)
          quicRelayTrace("gateway listener ready_wait_begin route=" & route & " persistent=true")
          ready = await awaitJsonFutureWithTimeout(
            listenerReadyFuture,
            QuicRelayListenerReadyTimeout,
            "gateway listener ready"
          )
          quicRelayTrace("gateway listener ready_wait_done route=" & route)
        except CatchableError as exc:
          quicRelayTrace(
            "gateway listener notify/ready failed route=" & route &
            " err=" & exc.msg
          )
          try:
            await writeJsonMessage(stream, rpcError("listener_not_ready"))
          except CatchableError:
            discard
          gateway.detachListenerStream(route, listenerControlStream, exc.msg)
          await listenerControlStream.closeQuicStream()
          return
        if not ready{"ok"}.getBool():
          quicRelayTrace("gateway listener not ready route=" & route)
          await writeJsonMessage(stream, rpcError("listener_not_ready"))
          gateway.detachListenerStream(
            route,
            listenerControlStream,
            "nim_quic relay listener rejected ready"
          )
          await listenerControlStream.closeQuicStream()
          return
        let readyCandidates = jsonStrings(ready, "candidates")
        let listenerCandidates =
          if readyCandidates.len > 0: readyCandidates else: listener.candidates
        quicRelayTrace("gateway listener ready route=" & route)
        listenerDataStream =
          try:
            await awaitBridgeAttachWithTimeout(
              bridgeAttachFuture,
              QuicRelayListenerReadyTimeout,
              kind & " bridge attach"
            )
          except CatchableError as exc:
            quicRelayTrace(
              "gateway bridge attach wait failed route=" & route &
              " sessionId=" & pendingBridgeSessionId &
              " err=" & exc.msg
            )
            await writeJsonMessage(stream, rpcError("listener_not_ready"))
            return
        await writeJsonMessageWithTimeout(stream, rpcSuccess(%*{
          "mode": "dial",
          "route": route,
          "candidates": listenerCandidates,
          "sessionId": pendingBridgeSessionId
        }), QuicRelayRpcTimeout, "gateway dial ack")
        quicRelayTrace("gateway bridging route=" & route)
        gateway.releaseListenerRoute(route)
        routeLockHeld = false
        await bridgeQuicStreams(listenerDataStream, stream)
      finally:
        if routeLockHeld:
          gateway.releaseListenerRoute(route)
        if pendingBridgeSessionId.len > 0:
          gateway.clearPendingBridgeSession(pendingBridgeSessionId)
      keepOpen = true
    else:
      quicRelayTrace("gateway unsupported mode=" & mode)
      await writeJsonMessage(stream, rpcError("unsupported_mode"))
  except CatchableError as exc:
    quicRelayTrace("serveRelayStream failed: " & exc.msg)
  finally:
    if not keepOpen:
      await stream.finishQuicStream()

proc serveConnection(
    gateway: TsnetQuicRelayGateway,
    connPtr: pointer,
    connState: msquicdrv.MsQuicConnectionState
) {.async: (raises: []).} =
  quicRelayTrace("gateway connection accepted")
  defer:
    gateway.clearListenerRoutesForConnection(connPtr)
    safeShutdownConnection(gateway.handle, connPtr)
    safeCloseConnection(gateway.handle, connPtr, connState)
  while gateway.running:
    let stream =
      try:
        await awaitIncomingBidiStream(gateway.handle, connState)
      except CatchableError:
        quicRelayTrace("gateway connection stream loop exit")
        break
    quicRelayTrace("gateway bidi stream accepted")
    asyncSpawn gateway.serveRelayStream(connPtr, connState, stream)

proc listenerLoop(gateway: TsnetQuicRelayGateway) {.async: (raises: []).} =
  while gateway.running and not gateway.listenerState.isNil:
    let event =
      try:
        await msquicdrv.nextListenerEvent(gateway.listenerState)
      except CatchableError:
        break
    if event.kind != msevents.leNewConnection or event.connection.isNil:
      continue
    var stateOpt = msquicdrv.takePendingConnection(gateway.listenerState, event.connection)
    if stateOpt.isNone:
      let attachRes = safeAttachIncomingConnection(gateway.handle, event.connection)
      if attachRes.error.len > 0 or attachRes.state.isNone:
        continue
      stateOpt = attachRes.state
    asyncSpawn gateway.serveConnection(event.connection, stateOpt.get())

proc start*(
    gateway: TsnetQuicRelayGateway,
    listenHost: string,
    listenPort: uint16,
    certificatePem: string,
    privateKeyPem: string
): Result[void, string] =
  if gateway.isNil:
    return err("nim_quic relay gateway is nil")
  if gateway.running:
    return ok()
  if certificatePem.strip().len == 0 or privateKeyPem.strip().len == 0:
    return err("nim_quic relay gateway requires certificatePem and privateKeyPem")
  var cfg = msquicdrv.MsQuicTransportConfig(
    alpns: @[NimTsnetQuicRelayAlpn],
    appName: "nim-tsnet-relay-quic",
    handshakeIdleTimeoutMs: QuicRelayHandshakeIdleTimeoutMs,
    idleTimeoutMs: QuicRelayIdleTimeoutMs,
    keepAliveIntervalMs: QuicRelayKeepAliveIntervalMs
  )
  when compiles(qrt.useBuiltinRuntime(cfg)):
    qrt.useBuiltinRuntime(cfg)
  let (handle, initErr) = msquicdrv.initMsQuicTransport(cfg)
  if handle.isNil:
    return err("failed to initialize nim_quic relay runtime: " & initErr)
  let serverTlsCfg = mstlstypes.TlsConfig(
    role: mstlstypes.tlsServer,
    alpns: @[NimTsnetQuicRelayAlpn],
    transportParameters: @[],
    serverName: none(string),
    certificatePem: some(certificatePem),
    privateKeyPem: some(privateKeyPem),
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
    return err("failed to load nim_quic relay credential: " & credErr)
  let created = msquicdrv.createListener(handle)
  if created.error.len > 0 or created.state.isNone or created.listener.isNil:
    safeShutdownRuntime(handle)
    return err("failed to create nim_quic relay listener: " & created.error)
  var storage: Sockaddr_storage
  var sockLen: SockLen
  try:
    let bindAddr = initTAddress(listenHost, Port(listenPort))
    toSAddr(bindAddr, storage, sockLen)
  except CatchableError as exc:
    msquicdrv.closeListener(handle, created.listener, created.state.get())
    safeShutdownRuntime(handle)
    return err("failed to bind nim_quic relay listener: " & exc.msg)
  let startErr = msquicdrv.startListener(handle, created.listener, address = addr storage)
  if startErr.len > 0:
    msquicdrv.closeListener(handle, created.listener, created.state.get())
    safeShutdownRuntime(handle)
    return err("failed to start nim_quic relay listener: " & startErr)
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

proc stop*(gateway: TsnetQuicRelayGateway) =
  if gateway.isNil or not gateway.running:
    return
  gateway.running = false
  gateway.clearListeners()
  if not gateway.handle.isNil and not gateway.listener.isNil:
    discard msquicdrv.stopListener(gateway.handle, gateway.listener)
    msquicdrv.closeListener(gateway.handle, gateway.listener, gateway.listenerState)
  safeShutdownRuntime(gateway.handle)
  gateway.handle = nil
  gateway.listener = nil
  gateway.listenerState = nil
  gateway.acceptLoop = nil

proc connectRelayAttempt(
    endpoint: TsnetQuicRelayEndpoint,
    runtimePreference: qrt.QuicRuntimePreference = qrt.qrpBuiltinOnly,
    allowWithoutConnected = true
): Result[TsnetQuicRelayClient, string] {.gcsafe, raises: [].} =
  let dialHost = resolveDialHost(endpoint.host, endpoint.port).valueOr:
    return err(error)
  let dialFamily = detectAddressFamily(dialHost)
  var cfg = msquicdrv.MsQuicTransportConfig(
    alpns: @[NimTsnetQuicRelayAlpn],
    appName: "nim-tsnet-relay-quic",
    handshakeIdleTimeoutMs: QuicRelayHandshakeIdleTimeoutMs,
    idleTimeoutMs: QuicRelayIdleTimeoutMs,
    keepAliveIntervalMs: QuicRelayKeepAliveIntervalMs
  )
  let effectiveRuntimePreference = relayRuntimePreference(runtimePreference)
  quicRelaySafe:
    case effectiveRuntimePreference
    of qrt.qrpAuto:
      qrt.useAutoRuntime(cfg)
    of qrt.qrpNativeOnly:
      qrt.useBuiltinRuntime(cfg)
    of qrt.qrpBuiltinPreferred:
      qrt.useBuiltinRuntime(cfg)
    of qrt.qrpBuiltinOnly:
      qrt.useBuiltinRuntime(cfg)
  var handle: msquicdrv.MsQuicTransportHandle
  var initErr = ""
  quicRelaySafe:
    (handle, initErr) = msquicdrv.initMsQuicTransport(cfg)
  if handle.isNil:
    return err("failed to initialize nim_quic relay runtime: " & initErr)
  let clientTlsCfg = mstlstypes.TlsConfig(
    role: mstlstypes.tlsClient,
    alpns: @[NimTsnetQuicRelayAlpn],
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
  var credErr = ""
  quicRelaySafe:
    credErr = msquicdrv.loadCredential(handle, clientTlsCfg)
  if credErr.len > 0:
    safeShutdownRuntime(handle)
    return err("failed to load nim_quic relay client credential: " & credErr)
  var dialed: tuple[connection: pointer, state: Option[msquicdrv.MsQuicConnectionState], error: string]
  quicRelaySafe:
    dialed = msquicdrv.dialConnection(
      handle,
      dialHost,
      endpoint.port,
      addressFamily = dialFamily
    )
  quicRelayTrace(
    "dial endpoint=" & endpoint.url &
    " dialHost=" & dialHost &
    " family=" & $dialFamily &
    " runtimePreference=" & $effectiveRuntimePreference &
    " tlsServerName=" &
    (if clientTlsCfg.serverName.isSome: clientTlsCfg.serverName.get() else: "<none>")
  )
  if dialed.error.len > 0 or dialed.connection.isNil or dialed.state.isNone:
    safeShutdownRuntime(handle)
    return err("failed to dial nim_quic relay endpoint " & endpoint.url & ": " & dialed.error)
  let connPtr = dialed.connection
  let connState = dialed.state.get()
  let dialState =
    try:
      waitFor awaitConnected(connState)
    except CatchableError as exc:
      (false, "nim_quic relay dial wait raised: " & exc.msg)
  if not dialState[0]:
    if allowWithoutConnected and
        dialState[1].startsWith("timeout waiting for nim_quic relay connection event"):
      if isLoopbackRelayHost(endpoint.host):
        quicRelayTrace(
          "proceeding without CONNECTED event for loopback endpoint=" & endpoint.url &
          " reason=" & dialState[1]
        )
      elif not relayHandshakeComplete(connState):
        safeShutdownConnection(handle, connPtr)
        safeCloseConnection(handle, connPtr, connState)
        safeShutdownRuntime(handle)
        return err(
          dialState[1] & " and handshake did not complete for " & endpoint.url
        )
      else:
        quicRelayTrace(
          "proceeding without CONNECTED event endpoint=" & endpoint.url &
          " reason=" & dialState[1]
        )
    else:
      let reason =
        if dialState[1].len > 0: dialState[1]
        else: "nim_quic relay dial failed before connection"
      safeShutdownConnection(handle, connPtr)
      safeCloseConnection(handle, connPtr, connState)
      safeShutdownRuntime(handle)
      return err(reason & " for " & endpoint.url)
  result = ok(TsnetQuicRelayClient(
    endpoint: ownedRelayEndpoint(endpoint),
    handle: handle,
    connPtr: connPtr,
    connState: connState
  ))

proc connectRelayAttemptAsync(
    endpoint: TsnetQuicRelayEndpoint,
    runtimePreference: qrt.QuicRuntimePreference = qrt.qrpBuiltinOnly,
    allowWithoutConnected = true
): Future[Result[TsnetQuicRelayClient, string]] {.async: (raises: [CancelledError]).} =
  let dialHost = resolveDialHost(endpoint.host, endpoint.port).valueOr:
    return err(error)
  let dialFamily = detectAddressFamily(dialHost)
  var cfg = msquicdrv.MsQuicTransportConfig(
    alpns: @[NimTsnetQuicRelayAlpn],
    appName: "nim-tsnet-relay-quic",
    handshakeIdleTimeoutMs: QuicRelayHandshakeIdleTimeoutMs,
    idleTimeoutMs: QuicRelayIdleTimeoutMs,
    keepAliveIntervalMs: QuicRelayKeepAliveIntervalMs
  )
  let effectiveRuntimePreference = relayRuntimePreference(runtimePreference)
  quicRelaySafe:
    case effectiveRuntimePreference
    of qrt.qrpAuto:
      qrt.useAutoRuntime(cfg)
    of qrt.qrpNativeOnly:
      qrt.useBuiltinRuntime(cfg)
    of qrt.qrpBuiltinPreferred:
      qrt.useBuiltinRuntime(cfg)
    of qrt.qrpBuiltinOnly:
      qrt.useBuiltinRuntime(cfg)
  var handle: msquicdrv.MsQuicTransportHandle
  var initErr = ""
  quicRelaySafe:
    (handle, initErr) = msquicdrv.initMsQuicTransport(cfg)
  if handle.isNil:
    return err("failed to initialize nim_quic relay runtime: " & initErr)
  let clientTlsCfg = mstlstypes.TlsConfig(
    role: mstlstypes.tlsClient,
    alpns: @[NimTsnetQuicRelayAlpn],
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
  var credErr = ""
  quicRelaySafe:
    credErr = msquicdrv.loadCredential(handle, clientTlsCfg)
  if credErr.len > 0:
    safeShutdownRuntime(handle)
    return err("failed to load nim_quic relay client credential: " & credErr)
  var dialed: tuple[connection: pointer, state: Option[msquicdrv.MsQuicConnectionState], error: string]
  quicRelaySafe:
    dialed = msquicdrv.dialConnection(
      handle,
      dialHost,
      endpoint.port,
      addressFamily = dialFamily
    )
  quicRelayTrace(
    "dial endpoint=" & endpoint.url &
    " dialHost=" & dialHost &
    " family=" & $dialFamily &
    " runtimePreference=" & $effectiveRuntimePreference &
    " tlsServerName=" &
    (if clientTlsCfg.serverName.isSome: clientTlsCfg.serverName.get() else: "<none>")
  )
  if dialed.error.len > 0 or dialed.connection.isNil or dialed.state.isNone:
    safeShutdownRuntime(handle)
    return err("failed to dial nim_quic relay endpoint " & endpoint.url & ": " & dialed.error)
  let connPtr = dialed.connection
  let connState = dialed.state.get()
  let dialState =
    try:
      await awaitConnected(connState)
    except CancelledError as exc:
      safeShutdownConnection(handle, connPtr)
      safeCloseConnection(handle, connPtr, connState)
      safeShutdownRuntime(handle)
      raise exc
    except CatchableError as exc:
      (false, "nim_quic relay dial wait raised: " & exc.msg)
  if not dialState[0]:
    if allowWithoutConnected and
        dialState[1].startsWith("timeout waiting for nim_quic relay connection event"):
      if isLoopbackRelayHost(endpoint.host):
        quicRelayTrace(
          "proceeding without CONNECTED event for loopback endpoint=" & endpoint.url &
          " reason=" & dialState[1]
        )
      elif not relayHandshakeComplete(connState):
        safeShutdownConnection(handle, connPtr)
        safeCloseConnection(handle, connPtr, connState)
        safeShutdownRuntime(handle)
        return err(
          dialState[1] & " and handshake did not complete for " & endpoint.url
        )
      else:
        quicRelayTrace(
          "proceeding without CONNECTED event endpoint=" & endpoint.url &
          " reason=" & dialState[1]
        )
    else:
      let reason =
        if dialState[1].len > 0: dialState[1]
        else: "nim_quic relay dial failed before connection"
      safeShutdownConnection(handle, connPtr)
      safeCloseConnection(handle, connPtr, connState)
      safeShutdownRuntime(handle)
      return err(reason & " for " & endpoint.url)
  return ok(TsnetQuicRelayClient(
    endpoint: ownedRelayEndpoint(endpoint),
    handle: handle,
    connPtr: connPtr,
    connState: connState
  ))

proc connectRelay(
    endpoint: TsnetQuicRelayEndpoint,
    runtimePreference: qrt.QuicRuntimePreference = qrt.qrpBuiltinOnly,
    allowWithoutConnected = true
): Result[TsnetQuicRelayClient, string] {.gcsafe, raises: [].} =
  var failures: seq[string] = @[]
  for attempt in 0 ..< QuicRelayConnectAttempts:
    let client = connectRelayAttempt(endpoint, runtimePreference, allowWithoutConnected).valueOr:
      failures.add(error)
      quicRelayTrace(
        "relay connect failed endpoint=" & endpoint.url &
        " attempt=" & $(attempt + 1) &
        " err=" & error
      )
      if attempt + 1 < QuicRelayConnectAttempts:
        try:
          waitFor sleepAsync(QuicRelayConnectRetryBackoff)
        except CatchableError:
          discard
      continue
    return ok(client)
  if failures.len == 0:
    return err("nim_quic relay connect failed")
  err(failures.join("; "))

proc connectRelayAsync(
    endpoint: TsnetQuicRelayEndpoint,
    runtimePreference: qrt.QuicRuntimePreference = qrt.qrpBuiltinOnly,
    allowWithoutConnected = true
): Future[Result[TsnetQuicRelayClient, string]] {.async: (raises: [CancelledError]).} =
  var failures: seq[string] = @[]
  for attempt in 0 ..< QuicRelayConnectAttempts:
    let client = await connectRelayAttemptAsync(endpoint, runtimePreference, allowWithoutConnected)
    if client.isOk():
      return client
    failures.add(client.error)
    quicRelayTrace(
      "relay connect failed endpoint=" & endpoint.url &
      " attempt=" & $(attempt + 1) &
      " err=" & client.error
    )
    if attempt + 1 < QuicRelayConnectAttempts:
      await sleepAsync(QuicRelayConnectRetryBackoff)
  if failures.len == 0:
    return err("nim_quic relay connect failed")
  return err(failures.join("; "))

proc sharedRelayClient(
    ownerId: int,
    endpoint: TsnetQuicRelayEndpoint,
    runtimePreference: qrt.QuicRuntimePreference = qrt.qrpBuiltinOnly,
    allowWithoutConnected = true
): Result[TsnetQuicRelayClient, string] {.gcsafe, raises: [].} =
  if ownerId <= 0:
    return connectRelay(endpoint, runtimePreference, allowWithoutConnected)
  var cached: TsnetQuicRelayClient = nil
  var stale: TsnetQuicRelayClient = nil
  quicRelaySafe:
    withLock(relayRegistryLock):
      if relayOwnerClients.hasKey(ownerId):
        let ownerClients = relayOwnerClients.getOrDefault(ownerId)
        if ownerClients.hasKey(endpoint.url):
          cached = ownerClients.getOrDefault(endpoint.url)
  if not relayClientUsable(cached):
    stale = cached
    cached = nil
    quicRelaySafe:
      withLock(relayRegistryLock):
        if relayOwnerClients.hasKey(ownerId):
          var perOwner = relayOwnerClients.getOrDefault(ownerId)
          if perOwner.getOrDefault(endpoint.url) == stale:
            perOwner.del(endpoint.url)
            relayOwnerClients[ownerId] = perOwner
          if perOwner.len == 0:
            relayOwnerClients.del(ownerId)
  if relayClientUsable(cached):
    return ok(cached)
  if not stale.isNil:
    closeQuicRelayClient(stale)
  let fresh = connectRelay(endpoint, runtimePreference, allowWithoutConnected).valueOr:
    return err(error)
  quicRelaySafe:
    withLock(relayRegistryLock):
      var ownerClients =
        if relayOwnerClients.hasKey(ownerId):
          relayOwnerClients.getOrDefault(ownerId)
        else:
          initTable[string, TsnetQuicRelayClient]()
      ownerClients[endpoint.url] = fresh
      relayOwnerClients[ownerId] = ownerClients
  ok(fresh)

proc sharedRelayClientAsync(
    ownerId: int,
    endpoint: TsnetQuicRelayEndpoint,
    runtimePreference: qrt.QuicRuntimePreference = qrt.qrpBuiltinOnly,
    allowWithoutConnected = true
): Future[Result[TsnetQuicRelayClient, string]] {.async: (raises: [CancelledError]).} =
  if ownerId <= 0:
    return await connectRelayAsync(endpoint, runtimePreference, allowWithoutConnected)
  var cached: TsnetQuicRelayClient = nil
  var stale: TsnetQuicRelayClient = nil
  quicRelaySafe:
    withLock(relayRegistryLock):
      if relayOwnerClients.hasKey(ownerId):
        let ownerClients = relayOwnerClients.getOrDefault(ownerId)
        if ownerClients.hasKey(endpoint.url):
          cached = ownerClients.getOrDefault(endpoint.url)
  if not relayClientUsable(cached):
    stale = cached
    cached = nil
    quicRelaySafe:
      withLock(relayRegistryLock):
        if relayOwnerClients.hasKey(ownerId):
          var perOwner = relayOwnerClients.getOrDefault(ownerId)
          if perOwner.getOrDefault(endpoint.url) == stale:
            perOwner.del(endpoint.url)
            relayOwnerClients[ownerId] = perOwner
          if perOwner.len == 0:
            relayOwnerClients.del(ownerId)
  if relayClientUsable(cached):
    return ok(cached)
  if not stale.isNil:
    closeQuicRelayClient(stale)
  let fresh = (await connectRelayAsync(
    endpoint,
    runtimePreference,
    allowWithoutConnected
  )).valueOr:
    return err(error)
  quicRelaySafe:
    withLock(relayRegistryLock):
      var ownerClients =
        if relayOwnerClients.hasKey(ownerId):
          relayOwnerClients.getOrDefault(ownerId)
        else:
          initTable[string, TsnetQuicRelayClient]()
      ownerClients[endpoint.url] = fresh
      relayOwnerClients[ownerId] = ownerClients
  return ok(fresh)

proc closeOwnerRelayClients(ownerId: int) {.gcsafe, raises: [].} =
  if ownerId <= 0:
    return
  var stale: seq[TsnetQuicRelayClient] = @[]
  quicRelaySafe:
    withLock(relayRegistryLock):
      if relayOwnerClients.hasKey(ownerId):
        let ownerClients = relayOwnerClients.getOrDefault(ownerId)
        if ownerClients.len > 0:
          for client in ownerClients.values:
            stale.add(client)
        relayOwnerClients.del(ownerId)
  for client in stale:
    closeQuicRelayClient(client)

proc invalidateSharedRelayClient(
    ownerId: int,
    endpoint: TsnetQuicRelayEndpoint,
    client: TsnetQuicRelayClient
) {.gcsafe, raises: [].} =
  if client.isNil:
    return
  if ownerId <= 0:
    closeQuicRelayClient(client)
    return

  var stale: TsnetQuicRelayClient = nil
  quicRelaySafe:
    withLock(relayRegistryLock):
      if relayOwnerClients.hasKey(ownerId):
        var ownerClients = relayOwnerClients.getOrDefault(ownerId)
        if ownerClients.getOrDefault(endpoint.url) == client:
          stale = client
          ownerClients.del(endpoint.url)
          if ownerClients.len > 0:
            relayOwnerClients[ownerId] = ownerClients
          else:
            relayOwnerClients.del(ownerId)

  if stale.isNil:
    stale = client
  closeQuicRelayClient(stale)

proc keepRelayConnectionAlive(
    client: TsnetQuicRelayClient,
    label: string
): Future[void] {.async: (raises: [CancelledError, CatchableError]).} =
  while true:
    await sleepAsync(QuicRelayKeepAliveInterval)
    var stream: MsQuicStream = nil
    try:
      stream = createBidiStream(client.handle, client.connPtr, client.connState).valueOr:
        raise newException(CatchableError, error)
      await writeJsonMessage(stream, %*{
        "version": 1,
        "mode": "ping"
      })
      let pong = await readJsonMessageWithTimeout(
        stream,
        QuicRelayKeepAliveAckTimeout,
        "nim_quic relay keepalive ack"
      )
      if pong.isNil or not pong{"ok"}.getBool():
        raise newException(CatchableError, "nim_quic relay keepalive rejected")
    except CancelledError:
      raise
    except CatchableError as exc:
      quicRelayTrace("relay keepalive failed label=" & label & " err=" & exc.msg)
      raise
    finally:
      if not stream.isNil:
        await stream.closeQuicStream()

proc probeListenerRegister*(
    relayUrl: string,
    route: string,
    runtimePreference: qrt.QuicRuntimePreference = qrt.qrpBuiltinOnly
): Future[TsnetQuicRelayProbeResult] {.async: (raises: [CancelledError]).} =
  result = TsnetQuicRelayProbeResult(
    ok: false,
    relayUrl: relayUrl,
    route: route,
    runtimePreference: runtimePreferenceLabel(runtimePreference),
    runtimeImplementation: "",
    runtimePath: "",
    runtimeKind: "",
    connected: false,
    acknowledged: false,
    error: ""
  )
  let endpoint = quicRelayEndpoint(relayUrl).valueOr:
    result.error = error
    return
  let client = connectRelay(endpoint, runtimePreference).valueOr:
    result.error = error
    return
  result.connected = true
  let runtimeInfo = currentRuntimeInfoSafe()
  result.runtimeImplementation = runtimeInfo.implementation
  result.runtimePath = runtimeInfo.path
  result.runtimeKind = qrt.kindLabel(runtimeInfo.kind)
  var stream: MsQuicStream = nil
  try:
    stream = createBidiStream(client.handle, client.connPtr, client.connState).valueOr:
      result.error = error
      return
    await writeJsonMessage(stream, %*{
      "version": 1,
      "mode": "listen",
      "kind": "udp",
      "route": route
    })
    let ack = await readJsonMessageWithTimeout(stream, QuicRelayRpcTimeout, "probe ack")
    result.acknowledged = ack.kind == JObject and ack{"ok"}.getBool()
    if result.acknowledged:
      result.ok = true
    else:
      result.error =
        if ack.kind == JObject:
          jsonString(ack, "error")
        else:
          "invalid_ack_payload"
  except CancelledError as exc:
    result.error = exc.msg
    raise exc
  except CatchableError as exc:
    result.error = exc.msg
  finally:
    if not stream.isNil:
      await noCancel stream.closeQuicStream()
    closeQuicRelayClient(client)

proc probeRelayPing*(
    relayUrl: string,
    runtimePreference: qrt.QuicRuntimePreference = qrt.qrpBuiltinOnly
): Future[TsnetQuicRelayProbeResult] {.async: (raises: [CancelledError]).} =
  result = TsnetQuicRelayProbeResult(
    ok: false,
    relayUrl: relayUrl,
    route: "",
    runtimePreference: runtimePreferenceLabel(runtimePreference),
    runtimeImplementation: "",
    runtimePath: "",
    runtimeKind: "",
    connected: false,
    acknowledged: false,
    error: ""
  )
  let endpoint = quicRelayEndpoint(relayUrl).valueOr:
    result.error = error
    return
  let client = connectRelay(endpoint, runtimePreference).valueOr:
    result.error = error
    return
  result.connected = true
  let runtimeInfo = currentRuntimeInfoSafe()
  result.runtimeImplementation = runtimeInfo.implementation
  result.runtimePath = runtimeInfo.path
  result.runtimeKind = qrt.kindLabel(runtimeInfo.kind)
  let pinged =
    try:
      await pingRelayConnection(client)
    except CancelledError as exc:
      result.error = exc.msg
      raise exc
    except CatchableError as exc:
      Result[void, string].err(exc.msg)
  if pinged.isOk():
    result.ok = true
    result.acknowledged = true
  else:
    result.error = pinged.error
  closeQuicRelayClient(client)

proc probeDialCandidateExchange*(
    relayUrl: string,
    route: string,
    listenerCandidates: seq[string],
    dialerCandidates: seq[string],
    runtimePreference: qrt.QuicRuntimePreference = qrt.qrpBuiltinOnly
): Future[TsnetQuicRelayCandidateExchangeResult] {.async: (raises: [CancelledError]).} =
  result = TsnetQuicRelayCandidateExchangeResult(
    ok: false,
    relayUrl: relayUrl,
    route: route,
    listenerCandidates: @[],
    dialerCandidates: @[],
    error: ""
  )
  let endpoint = quicRelayEndpoint(relayUrl).valueOr:
    result.error = error
    return
  let listenerClient = connectRelay(endpoint, runtimePreference).valueOr:
    result.error = error
    return
  let dialerClient = connectRelay(endpoint, runtimePreference).valueOr:
    result.error = error
    closeQuicRelayClient(listenerClient)
    return
  var listenerRegStream: MsQuicStream = nil
  var listenerAcceptStream: MsQuicStream = nil
  var listenerDataStream: MsQuicStream = nil
  var dialerStream: MsQuicStream = nil
  try:
    listenerRegStream = createBidiStream(listenerClient.handle, listenerClient.connPtr, listenerClient.connState).valueOr:
      result.error = error
      return
    await writeJsonMessage(listenerRegStream, %*{
      "version": 1,
      "mode": "listen",
      "kind": "udp",
      "route": route,
      "candidates": listenerCandidates
    })
    let listenerAck = await readJsonMessageWithTimeout(
      listenerRegStream,
      QuicRelayRpcTimeout,
      "listener register ack"
    )
    if not listenerAck{"ok"}.getBool():
      result.error = jsonString(listenerAck, "error")
      return
    let listenerGeneration = relayListenerGeneration(listenerAck)
    if listenerGeneration.len == 0:
      result.error = "listener_generation_missing"
      return
    await listenerRegStream.closeQuicStream()
    listenerRegStream = nil

    listenerAcceptStream = createBidiStream(listenerClient.handle, listenerClient.connPtr, listenerClient.connState).valueOr:
      result.error = error
      return
    await writeJsonMessage(listenerAcceptStream, %*{
      "version": 1,
      "mode": "accept",
      "kind": "udp",
      "route": route,
      "generation": listenerGeneration
    })
    let acceptAck = await readJsonMessageWithTimeout(
      listenerAcceptStream,
      QuicRelayAcceptAckTimeout,
      "listener accept ack"
    )
    if not acceptAck{"ok"}.getBool():
      result.error = jsonString(acceptAck, "error")
      return
    await writeJsonMessage(listenerAcceptStream, %*{
      "op": "await",
      "route": route,
      "generation": listenerGeneration
    })
    let awaitAck = await readJsonMessageWithTimeout(
      listenerAcceptStream,
      QuicRelayAcceptAckTimeout,
      "listener await ack"
    )
    if not awaitAck{"ok"}.getBool() or jsonString(rpcPayload(awaitAck), "op") != "await":
      result.error = jsonString(awaitAck, "error")
      return

    dialerStream = createBidiStream(dialerClient.handle, dialerClient.connPtr, dialerClient.connState).valueOr:
      result.error = error
      return
    await writeJsonMessage(dialerStream, %*{
      "version": 1,
      "mode": "dial",
      "kind": "udp",
      "route": route,
      "source": "/ip4/100.64.0.99/udp/4001/quic-v1/tsnet",
      "candidates": dialerCandidates
    })
    let incoming = await readJsonMessageWithTimeout(
      listenerAcceptStream,
      QuicRelayRpcTimeout,
      "listener incoming payload"
    )
    let sessionId = jsonString(incoming, "sessionId")
    result.dialerCandidates = jsonStrings(incoming, "candidates")
    listenerDataStream = createBidiStream(listenerClient.handle, listenerClient.connPtr, listenerClient.connState).valueOr:
      result.error = error
      return
    await writeJsonMessage(listenerDataStream, %*{
      "version": 1,
      "mode": "bridge_attach",
      "kind": "udp",
      "route": route,
      "sessionId": sessionId
    })
    let attachAck = await readJsonMessageWithTimeout(
      listenerDataStream,
      QuicRelayDialAckTimeout,
      "listener bridge attach ack"
    )
    if not attachAck{"ok"}.getBool():
      result.error = jsonString(attachAck, "error")
      return
    await writeJsonMessage(listenerAcceptStream, %*{
      "ok": true,
      "op": "ready",
      "candidates": listenerCandidates,
      "sessionId": sessionId,
      "generation": listenerGeneration
    })

    let dialAck = await readJsonMessageWithTimeout(dialerStream, QuicRelayRpcTimeout, "dial ack")
    if not dialAck{"ok"}.getBool():
      result.error = jsonString(dialAck, "error")
      return
    result.listenerCandidates = jsonStrings(rpcPayload(dialAck), "candidates")
    await dialerStream.closeQuicStream()
    dialerStream = nil
    result.ok = result.listenerCandidates == listenerCandidates and
      result.dialerCandidates == dialerCandidates
  except CancelledError as exc:
    result.error = exc.msg
    raise exc
  except CatchableError as exc:
    result.error = exc.msg
  finally:
    if not dialerStream.isNil:
      await noCancel dialerStream.closeQuicStream()
    if not listenerDataStream.isNil:
      await noCancel listenerDataStream.closeQuicStream()
    if not listenerAcceptStream.isNil:
      await noCancel listenerAcceptStream.closeQuicStream()
    if not listenerRegStream.isNil:
      await noCancel listenerRegStream.closeQuicStream()
    closeQuicRelayClient(dialerClient)
    closeQuicRelayClient(listenerClient)

proc probeAcceptStreamReuse*(
    relayUrl: string,
    route: string,
    listenerCandidates: seq[string],
    dialerCandidates: seq[string],
    runtimePreference: qrt.QuicRuntimePreference = qrt.qrpBuiltinOnly
): Future[TsnetQuicRelayCandidateExchangeResult] {.async: (raises: [CancelledError]).} =
  result = TsnetQuicRelayCandidateExchangeResult(
    ok: false,
    relayUrl: relayUrl,
    route: route,
    listenerCandidates: @[],
    dialerCandidates: @[],
    error: ""
  )
  let endpoint = quicRelayEndpoint(relayUrl).valueOr:
    result.error = error
    return
  let listenerClient = connectRelay(endpoint, runtimePreference).valueOr:
    result.error = error
    return
  let dialerClient = connectRelay(endpoint, runtimePreference).valueOr:
    result.error = error
    closeQuicRelayClient(listenerClient)
    return
  var listenerRegStream: MsQuicStream = nil
  var listenerAcceptStream: MsQuicStream = nil
  var listenerDataStream: MsQuicStream = nil
  var dialerStream: MsQuicStream = nil
  try:
    listenerRegStream = createBidiStream(listenerClient.handle, listenerClient.connPtr, listenerClient.connState).valueOr:
      result.error = error
      return
    await writeJsonMessage(listenerRegStream, %*{
      "version": 1,
      "mode": "listen",
      "kind": "udp",
      "route": route,
      "candidates": listenerCandidates
    })
    let listenerAck = await readJsonMessageWithTimeout(
      listenerRegStream,
      QuicRelayRpcTimeout,
      "listener register ack"
    )
    if not listenerAck{"ok"}.getBool():
      result.error = jsonString(listenerAck, "error")
      return
    let listenerGeneration = relayListenerGeneration(listenerAck)
    if listenerGeneration.len == 0:
      result.error = "listener_generation_missing"
      return
    await listenerRegStream.closeQuicStream()
    listenerRegStream = nil

    listenerAcceptStream = createBidiStream(listenerClient.handle, listenerClient.connPtr, listenerClient.connState).valueOr:
      result.error = error
      return
    await writeJsonMessage(listenerAcceptStream, %*{
      "version": 1,
      "mode": "accept",
      "kind": "udp",
      "route": route,
      "generation": listenerGeneration
    })
    let acceptAck = await readJsonMessageWithTimeout(
      listenerAcceptStream,
      QuicRelayAcceptAckTimeout,
      "listener accept ack"
    )
    if not acceptAck{"ok"}.getBool():
      result.error = jsonString(acceptAck, "error")
      return
    await writeJsonMessage(listenerAcceptStream, %*{
      "op": "await",
      "route": route,
      "generation": listenerGeneration
    })
    let awaitAck = await readJsonMessageWithTimeout(
      listenerAcceptStream,
      QuicRelayAcceptAckTimeout,
      "listener await ack"
    )
    if not awaitAck{"ok"}.getBool() or jsonString(rpcPayload(awaitAck), "op") != "await":
      result.error = jsonString(awaitAck, "error")
      return

    dialerStream = createBidiStream(dialerClient.handle, dialerClient.connPtr, dialerClient.connState).valueOr:
      result.error = error
      return
    await writeJsonMessage(dialerStream, %*{
      "version": 1,
      "mode": "dial",
      "kind": "udp",
      "route": route,
      "source": "/ip4/100.64.0.99/udp/4001/quic-v1/tsnet",
      "candidates": dialerCandidates
    })
    let incoming = await readJsonMessageWithTimeout(
      listenerAcceptStream,
      QuicRelayDialAckTimeout,
      "listener incoming payload"
    )
    let sessionId = jsonString(incoming, "sessionId")
    result.dialerCandidates = jsonStrings(incoming, "candidates")
    await writeJsonMessage(listenerAcceptStream, %*{
      "ok": true,
      "op": "ready",
      "candidates": listenerCandidates,
      "sessionId": sessionId,
      "generation": listenerGeneration
    })

    listenerDataStream = createBidiStream(listenerClient.handle, listenerClient.connPtr, listenerClient.connState).valueOr:
      result.error = error
      return
    await writeJsonMessage(listenerDataStream, %*{
      "version": 1,
      "mode": "bridge_attach",
      "kind": "udp",
      "route": route,
      "sessionId": sessionId
    })
    let attachAck = await readJsonMessageWithTimeout(
      listenerDataStream,
      QuicRelayDialAckTimeout,
      "listener bridge attach ack"
    )
    if not attachAck{"ok"}.getBool():
      result.error = jsonString(attachAck, "error")
      return

    let dialAck = await readJsonMessageWithTimeout(
      dialerStream,
      QuicRelayDialAckTimeout,
      "dial ack"
    )
    if not dialAck{"ok"}.getBool():
      result.error = jsonString(dialAck, "error")
      return
    result.listenerCandidates = jsonStrings(rpcPayload(dialAck), "candidates")
    result.ok = result.listenerCandidates == listenerCandidates and
      result.dialerCandidates == dialerCandidates

    await dialerStream.closeQuicStream()
    dialerStream = nil
    await listenerDataStream.closeQuicStream()
    listenerDataStream = nil

    await sleepAsync(100.milliseconds)
    await writeJsonMessage(listenerAcceptStream, %*{
      "op": "await",
      "route": route,
      "generation": listenerGeneration
    })
    let secondAwaitAck = await readJsonMessageWithTimeout(
      listenerAcceptStream,
      QuicRelayAcceptAckTimeout,
      "listener second await ack"
    )
    if not secondAwaitAck{"ok"}.getBool() or
        jsonString(rpcPayload(secondAwaitAck), "op") != "await":
      result.error = jsonString(secondAwaitAck, "error")
      return

    dialerStream = createBidiStream(dialerClient.handle, dialerClient.connPtr, dialerClient.connState).valueOr:
      result.error = error
      return
    await writeJsonMessage(dialerStream, %*{
      "version": 1,
      "mode": "dial",
      "kind": "udp",
      "route": route,
      "source": "/ip4/100.64.0.98/udp/4002/quic-v1/tsnet",
      "candidates": dialerCandidates
    })
    let secondIncoming = await readJsonMessageWithTimeout(
      listenerAcceptStream,
      QuicRelayDialAckTimeout,
      "listener second incoming payload"
    )
    let secondSessionId = jsonString(secondIncoming, "sessionId")
    let secondDialerCandidates = jsonStrings(secondIncoming, "candidates")
    await writeJsonMessage(listenerAcceptStream, %*{
      "ok": true,
      "op": "ready",
      "candidates": listenerCandidates,
      "sessionId": secondSessionId,
      "generation": listenerGeneration
    })

    listenerDataStream = createBidiStream(listenerClient.handle, listenerClient.connPtr, listenerClient.connState).valueOr:
      result.error = error
      return
    await writeJsonMessage(listenerDataStream, %*{
      "version": 1,
      "mode": "bridge_attach",
      "kind": "udp",
      "route": route,
      "sessionId": secondSessionId
    })
    let secondAttachAck = await readJsonMessageWithTimeout(
      listenerDataStream,
      QuicRelayDialAckTimeout,
      "listener second bridge attach ack"
    )
    if not secondAttachAck{"ok"}.getBool():
      result.error = jsonString(secondAttachAck, "error")
      return

    let secondDialAck = await readJsonMessageWithTimeout(
      dialerStream,
      QuicRelayDialAckTimeout,
      "second dial ack"
    )
    if not secondDialAck{"ok"}.getBool():
      result.error = jsonString(secondDialAck, "error")
      return
    let secondListenerCandidates = jsonStrings(rpcPayload(secondDialAck), "candidates")
    result.ok = result.ok and
      secondListenerCandidates == listenerCandidates and
      secondDialerCandidates == dialerCandidates
  except CancelledError as exc:
    result.error = exc.msg
    raise exc
  except CatchableError as exc:
    result.error = exc.msg
  finally:
    if not dialerStream.isNil:
      await noCancel dialerStream.closeQuicStream()
    if not listenerDataStream.isNil:
      await noCancel listenerDataStream.closeQuicStream()
    if not listenerAcceptStream.isNil:
      await noCancel listenerAcceptStream.closeQuicStream()
    if not listenerRegStream.isNil:
      await noCancel listenerRegStream.closeQuicStream()
    closeQuicRelayClient(dialerClient)
    closeQuicRelayClient(listenerClient)

proc probeAcceptStreamReuseRouteStatusGap*(
    relayUrl: string,
    route: string,
    listenerCandidates: seq[string],
    dialerCandidates: seq[string],
    runtimePreference: qrt.QuicRuntimePreference = qrt.qrpBuiltinOnly
): Future[tuple[ok: bool, published: bool, error: string]] {.async: (raises: [CancelledError]).} =
  result = (false, false, "")
  let endpoint = quicRelayEndpoint(relayUrl).valueOr:
    result.error = error
    return
  let listenerClient = connectRelay(endpoint, runtimePreference).valueOr:
    result.error = error
    return
  let dialerClient = connectRelay(endpoint, runtimePreference).valueOr:
    result.error = error
    closeQuicRelayClient(listenerClient)
    return
  var listenerRegStream: MsQuicStream = nil
  var listenerAcceptStream: MsQuicStream = nil
  var listenerDataStream: MsQuicStream = nil
  var dialerStream: MsQuicStream = nil
  var statusClient: TsnetQuicRelayClient = nil
  try:
    listenerRegStream = createBidiStream(listenerClient.handle, listenerClient.connPtr, listenerClient.connState).valueOr:
      result.error = error
      return
    await writeJsonMessage(listenerRegStream, %*{
      "version": 1,
      "mode": "listen",
      "kind": "udp",
      "route": route,
      "candidates": listenerCandidates
    })
    let listenerAck = await readJsonMessageWithTimeout(
      listenerRegStream,
      QuicRelayRpcTimeout,
      "listener register ack"
    )
    if not listenerAck{"ok"}.getBool():
      result.error = jsonString(listenerAck, "error")
      return
    let listenerGeneration = relayListenerGeneration(listenerAck)
    if listenerGeneration.len == 0:
      result.error = "listener_generation_missing"
      return
    await listenerRegStream.closeQuicStream()
    listenerRegStream = nil

    listenerAcceptStream = createBidiStream(listenerClient.handle, listenerClient.connPtr, listenerClient.connState).valueOr:
      result.error = error
      return
    await writeJsonMessage(listenerAcceptStream, %*{
      "version": 1,
      "mode": "accept",
      "kind": "udp",
      "route": route,
      "generation": listenerGeneration
    })
    let acceptAck = await readJsonMessageWithTimeout(
      listenerAcceptStream,
      QuicRelayAcceptAckTimeout,
      "listener accept ack"
    )
    if not acceptAck{"ok"}.getBool():
      result.error = jsonString(acceptAck, "error")
      return
    await writeJsonMessage(listenerAcceptStream, %*{
      "op": "await",
      "route": route,
      "generation": listenerGeneration
    })
    let awaitAck = await readJsonMessageWithTimeout(
      listenerAcceptStream,
      QuicRelayAcceptAckTimeout,
      "listener await ack"
    )
    if not awaitAck{"ok"}.getBool() or jsonString(rpcPayload(awaitAck), "op") != "await":
      result.error = jsonString(awaitAck, "error")
      return

    dialerStream = createBidiStream(dialerClient.handle, dialerClient.connPtr, dialerClient.connState).valueOr:
      result.error = error
      return
    await writeJsonMessage(dialerStream, %*{
      "version": 1,
      "mode": "dial",
      "kind": "udp",
      "route": route,
      "source": "/ip4/100.64.0.99/udp/4001/quic-v1/tsnet",
      "candidates": dialerCandidates
    })
    let incoming = await readJsonMessageWithTimeout(
      listenerAcceptStream,
      QuicRelayDialAckTimeout,
      "listener incoming payload"
    )
    let sessionId = jsonString(incoming, "sessionId")
    await writeJsonMessage(listenerAcceptStream, %*{
      "ok": true,
      "op": "ready",
      "candidates": listenerCandidates,
      "sessionId": sessionId,
      "generation": listenerGeneration
    })

    listenerDataStream = createBidiStream(listenerClient.handle, listenerClient.connPtr, listenerClient.connState).valueOr:
      result.error = error
      return
    await writeJsonMessage(listenerDataStream, %*{
      "version": 1,
      "mode": "bridge_attach",
      "kind": "udp",
      "route": route,
      "sessionId": sessionId
    })
    let attachAck = await readJsonMessageWithTimeout(
      listenerDataStream,
      QuicRelayDialAckTimeout,
      "listener bridge attach ack"
    )
    if not attachAck{"ok"}.getBool():
      result.error = jsonString(attachAck, "error")
      return

    let dialAck = await readJsonMessageWithTimeout(
      dialerStream,
      QuicRelayDialAckTimeout,
      "dial ack"
    )
    if not dialAck{"ok"}.getBool():
      result.error = jsonString(dialAck, "error")
      return

    await dialerStream.closeQuicStream()
    dialerStream = nil
    await listenerDataStream.closeQuicStream()
    listenerDataStream = nil

    statusClient = connectRelay(endpoint, runtimePreference).valueOr:
      result.error = error
      return
    let published = await relayRouteStatus(statusClient, route, QuicRelayRpcTimeout)
    if published.isErr():
      result.error = published.error
      return
    result.ok = true
    result.published = published.get()
  except CancelledError as exc:
    result.error = exc.msg
    raise exc
  except CatchableError as exc:
    result.error = exc.msg
  finally:
    if not statusClient.isNil:
      closeQuicRelayClient(statusClient)
    if not dialerStream.isNil:
      await noCancel dialerStream.closeQuicStream()
    if not listenerDataStream.isNil:
      await noCancel listenerDataStream.closeQuicStream()
    if not listenerAcceptStream.isNil:
      await noCancel listenerAcceptStream.closeQuicStream()
    if not listenerRegStream.isNil:
      await noCancel listenerRegStream.closeQuicStream()
    closeQuicRelayClient(dialerClient)
    closeQuicRelayClient(listenerClient)

proc probeIdlePersistentListenerRouteStatusStability*(
    relayUrl: string,
    route: string,
    listenerCandidates: seq[string],
    runtimePreference: qrt.QuicRuntimePreference = qrt.qrpBuiltinOnly,
    holdFor: Duration = 12.seconds,
    probeEvery: Duration = 1.seconds
): Future[tuple[ok: bool, probes: int, error: string]] {.async: (raises: [CancelledError]).} =
  result = (false, 0, "")
  let endpoint = quicRelayEndpoint(relayUrl).valueOr:
    result.error = error
    return
  let listenerClient = connectRelay(endpoint, runtimePreference).valueOr:
    result.error = error
    return
  var listenerRegStream: MsQuicStream = nil
  var listenerAcceptStream: MsQuicStream = nil
  try:
    listenerRegStream = createBidiStream(listenerClient.handle, listenerClient.connPtr, listenerClient.connState).valueOr:
      result.error = error
      return
    await writeJsonMessage(listenerRegStream, %*{
      "version": 1,
      "mode": "listen",
      "kind": "udp",
      "route": route,
      "candidates": listenerCandidates
    })
    let listenerAck = await readJsonMessageWithTimeout(
      listenerRegStream,
      QuicRelayRpcTimeout,
      "listener register ack"
    )
    if not listenerAck{"ok"}.getBool():
      result.error = jsonString(listenerAck, "error")
      return
    let listenerGeneration = relayListenerGeneration(listenerAck)
    if listenerGeneration.len == 0:
      result.error = "listener_generation_missing"
      return
    await listenerRegStream.closeQuicStream()
    listenerRegStream = nil

    listenerAcceptStream = createBidiStream(listenerClient.handle, listenerClient.connPtr, listenerClient.connState).valueOr:
      result.error = error
      return
    await writeJsonMessage(listenerAcceptStream, %*{
      "version": 1,
      "mode": "accept",
      "kind": "udp",
      "route": route,
      "generation": listenerGeneration
    })
    let acceptAck = await readJsonMessageWithTimeout(
      listenerAcceptStream,
      QuicRelayAcceptAckTimeout,
      "listener accept ack"
    )
    if not acceptAck{"ok"}.getBool():
      result.error = jsonString(acceptAck, "error")
      return
    await writeJsonMessage(listenerAcceptStream, %*{
      "op": "await",
      "route": route,
      "generation": listenerGeneration
    })
    let awaitAck = await readJsonMessageWithTimeout(
      listenerAcceptStream,
      QuicRelayAcceptAckTimeout,
      "listener await ack"
    )
    if not awaitAck{"ok"}.getBool() or jsonString(rpcPayload(awaitAck), "op") != "await":
      result.error = jsonString(awaitAck, "error")
      return
    if not rpcPayload(awaitAck){"published"}.getBool(false):
      result.error = "route_not_published"
      return

    let deadlineUnixMilli = currentUnixMilli() + holdFor.milliseconds.int64
    while currentUnixMilli() < deadlineUnixMilli:
      let statusClient = connectRelay(endpoint, runtimePreference).valueOr:
        result.error = error
        return
      try:
        let published = await relayRouteStatus(statusClient, route, QuicRelayRpcTimeout)
        if published.isErr():
          result.error = published.error
          return
        if not published.get():
          result.error = "route_not_published_during_idle_probe"
          return
        inc result.probes
      finally:
        closeQuicRelayClient(statusClient)
      await sleepAsync(probeEvery)
    result.ok = result.probes > 0
  except CancelledError as exc:
    result.error = exc.msg
    raise exc
  except CatchableError as exc:
    result.error = exc.msg
  finally:
    if not listenerAcceptStream.isNil:
      await noCancel listenerAcceptStream.closeQuicStream()
    if not listenerRegStream.isNil:
      await noCancel listenerRegStream.closeQuicStream()
    closeQuicRelayClient(listenerClient)

proc probeAcceptStreamPendingReadyRouteStatus*(
    relayUrl: string,
    route: string,
    listenerCandidates: seq[string],
    dialerCandidates: seq[string],
    runtimePreference: qrt.QuicRuntimePreference = qrt.qrpBuiltinOnly
): Future[TsnetQuicRelayCandidateExchangeResult] {.async: (raises: [CancelledError]).} =
  result = TsnetQuicRelayCandidateExchangeResult(
    ok: false,
    relayUrl: relayUrl,
    route: route,
    listenerCandidates: @[],
    dialerCandidates: @[],
    error: ""
  )
  let endpoint = quicRelayEndpoint(relayUrl).valueOr:
    result.error = error
    return
  let listenerClient = connectRelay(endpoint, runtimePreference).valueOr:
    result.error = error
    return
  let dialerClient = connectRelay(endpoint, runtimePreference).valueOr:
    result.error = error
    closeQuicRelayClient(listenerClient)
    return
  var listenerRegStream: MsQuicStream = nil
  var listenerAcceptStream: MsQuicStream = nil
  var listenerDataStream: MsQuicStream = nil
  var dialerStream: MsQuicStream = nil
  var statusClient: TsnetQuicRelayClient = nil
  try:
    listenerRegStream = createBidiStream(listenerClient.handle, listenerClient.connPtr, listenerClient.connState).valueOr:
      result.error = error
      return
    await writeJsonMessage(listenerRegStream, %*{
      "version": 1,
      "mode": "listen",
      "kind": "udp",
      "route": route,
      "candidates": listenerCandidates
    })
    let listenerAck = await readJsonMessageWithTimeout(
      listenerRegStream,
      QuicRelayRpcTimeout,
      "listener register ack"
    )
    if not listenerAck{"ok"}.getBool():
      result.error = jsonString(listenerAck, "error")
      return
    let listenerGeneration = relayListenerGeneration(listenerAck)
    if listenerGeneration.len == 0:
      result.error = "listener_generation_missing"
      return
    await listenerRegStream.closeQuicStream()
    listenerRegStream = nil

    listenerAcceptStream = createBidiStream(listenerClient.handle, listenerClient.connPtr, listenerClient.connState).valueOr:
      result.error = error
      return
    await writeJsonMessage(listenerAcceptStream, %*{
      "version": 1,
      "mode": "accept",
      "kind": "udp",
      "route": route,
      "generation": listenerGeneration
    })
    let acceptAck = await readJsonMessageWithTimeout(
      listenerAcceptStream,
      QuicRelayAcceptAckTimeout,
      "listener accept ack"
    )
    if not acceptAck{"ok"}.getBool():
      result.error = jsonString(acceptAck, "error")
      return
    await writeJsonMessage(listenerAcceptStream, %*{
      "op": "await",
      "route": route,
      "generation": listenerGeneration
    })
    let awaitAck = await readJsonMessageWithTimeout(
      listenerAcceptStream,
      QuicRelayAcceptAckTimeout,
      "listener await ack"
    )
    if not awaitAck{"ok"}.getBool() or jsonString(rpcPayload(awaitAck), "op") != "await":
      result.error = jsonString(awaitAck, "error")
      return

    dialerStream = createBidiStream(dialerClient.handle, dialerClient.connPtr, dialerClient.connState).valueOr:
      result.error = error
      return
    await writeJsonMessage(dialerStream, %*{
      "version": 1,
      "mode": "dial",
      "kind": "udp",
      "route": route,
      "source": "/ip4/100.64.0.97/udp/4003/quic-v1/tsnet",
      "candidates": dialerCandidates
    })
    let incoming = await readJsonMessageWithTimeout(
      listenerAcceptStream,
      QuicRelayDialAckTimeout,
      "listener incoming payload while ready pending"
    )
    statusClient = connectRelay(endpoint, runtimePreference).valueOr:
      result.error = error
      return
    let published = await relayRouteStatus(statusClient, route, QuicRelayRpcTimeout)
    if published.isErr():
      result.error = published.error
      return
    if not published.get():
      result.error = "route_not_published_while_ready_pending"
      return
    closeQuicRelayClient(statusClient)
    statusClient = nil
    let sessionId = jsonString(incoming, "sessionId")
    result.dialerCandidates = jsonStrings(incoming, "candidates")
    await writeJsonMessage(listenerAcceptStream, %*{
      "ok": true,
      "op": "ready",
      "candidates": listenerCandidates,
      "sessionId": sessionId,
      "generation": listenerGeneration
    })

    listenerDataStream = createBidiStream(listenerClient.handle, listenerClient.connPtr, listenerClient.connState).valueOr:
      result.error = error
      return
    await writeJsonMessage(listenerDataStream, %*{
      "version": 1,
      "mode": "bridge_attach",
      "kind": "udp",
      "route": route,
      "sessionId": sessionId
    })
    let attachAck = await readJsonMessageWithTimeout(
      listenerDataStream,
      QuicRelayDialAckTimeout,
      "listener bridge attach ack after route_status"
    )
    if not attachAck{"ok"}.getBool():
      result.error = jsonString(attachAck, "error")
      return

    let dialAck = await readJsonMessageWithTimeout(
      dialerStream,
      QuicRelayDialAckTimeout,
      "dial ack after route_status"
    )
    if not dialAck{"ok"}.getBool():
      result.error = jsonString(dialAck, "error")
      return
    result.listenerCandidates = jsonStrings(rpcPayload(dialAck), "candidates")
    result.ok = result.listenerCandidates == listenerCandidates and
      result.dialerCandidates == dialerCandidates
  except CancelledError as exc:
    result.error = exc.msg
    raise exc
  except CatchableError as exc:
    result.error = exc.msg
  finally:
    closeQuicRelayClient(statusClient)
    if not dialerStream.isNil:
      await noCancel dialerStream.closeQuicStream()
    if not listenerDataStream.isNil:
      await noCancel listenerDataStream.closeQuicStream()
    if not listenerAcceptStream.isNil:
      await noCancel listenerAcceptStream.closeQuicStream()
    if not listenerRegStream.isNil:
      await noCancel listenerRegStream.closeQuicStream()
    closeQuicRelayClient(dialerClient)
    closeQuicRelayClient(listenerClient)

proc listenerLoop(
    ownerId: int,
    ownerEpoch: int,
    routeId: int,
    startupDelay: Duration,
    relayEndpoint: TsnetQuicRelayEndpoint,
    route: string,
    rawLocal: string
): Future[void] {.async: (raises: [CancelledError]).} =
  quicRelayTrace("listener task start routeId=" & $routeId)
  var regStream: MsQuicStream = nil
  var acceptStream: MsQuicStream = nil
  var listenerGeneration = ""
  proc clearReady() =
    clearRelayReadyRouteIfTaskCurrent(ownerId, route, routeId, ownerEpoch)
  proc markReady() =
    markRelayRouteReadyIfTaskCurrent(ownerId, route, routeId, ownerEpoch)
  proc markStage(stage: string, detail = "") =
    markRelayListenerStageIfTaskCurrent(
      ownerId,
      route,
      routeId,
      ownerEpoch,
      "tcp",
      stage,
      detail
    )
  try:
    if startupDelay > ZeroDuration:
      quicRelayTrace(
        "listener startup delay routeId=" & $routeId &
        " delayMs=" & $startupDelay.milliseconds
      )
      await sleepAsync(startupDelay)
    while true:
      if relayOwnerTaskRevoked(ownerId, ownerEpoch):
        break
      clearReady()
      markStage("connecting")
      let client = (await sharedRelayClientAsync(ownerId, relayEndpoint)).valueOr:
        markStage("failed", error)
        quicRelayTrace("listener relay connect failed routeId=" & $routeId & " err=" & error)
        await sleepAsync(1.seconds)
        continue
      block setup:
        regStream = createBidiStream(client.handle, client.connPtr, client.connState).valueOr:
          markStage("failed", error)
          quicRelayTrace("listener stream create failed routeId=" & $routeId & " err=" & error)
          break setup
        let hello = %*{
          "version": 1,
          "mode": "listen",
          "route": route,
          "candidates": []
        }
        try:
          await writeJsonMessageWithTimeout(
            regStream,
            hello,
            QuicRelayRpcTimeout,
            "listener register"
          )
          quicRelayTrace(
            "listener registration sent routeId=" & $routeId &
            " route=" & route &
            " mode=ack_first"
          )
          let listenerAck = await readJsonMessageWithTimeout(
            regStream,
            QuicRelayRpcTimeout,
            "listener register ack"
          )
          if listenerAck.isNil or not listenerAck{"ok"}.getBool():
            markStage(
              "failed",
              if listenerAck.isNil: "register_ack_nil" else: jsonString(listenerAck, "error")
            )
            quicRelayTrace(
              "listener registration rejected routeId=" & $routeId &
              " route=" & route &
              " err=" & (if listenerAck.isNil: "<nil>" else: jsonString(listenerAck, "error"))
            )
            break setup
          listenerGeneration = relayListenerGeneration(listenerAck)
          if listenerGeneration.len == 0:
            markStage("failed", "listener_generation_missing")
            quicRelayTrace(
              "listener registration missing generation routeId=" & $routeId &
              " route=" & route
            )
            break setup
        except CatchableError as exc:
          quicRelayTrace(
            "listener registration failed routeId=" & $routeId &
            " route=" & route &
            " err=" & exc.msg
          )
          break setup
        await regStream.closeQuicStream()
        regStream = nil

        var restart = false
        while true:
          if relayOwnerTaskRevoked(ownerId, ownerEpoch):
            restart = false
            break
          if not acceptStream.isNil and not acceptStream.connectionUsable():
            quicRelayTrace(
              "listener dropping stale accept stream routeId=" & $routeId &
              " route=" & route
            )
            await acceptStream.closeQuicStream()
            acceptStream = nil
          if acceptStream.isNil:
            markStage("registering")
            acceptStream = createBidiStream(client.handle, client.connPtr, client.connState).valueOr:
              invalidateSharedRelayClient(ownerId, relayEndpoint, client)
              markStage("failed", error)
              quicRelayTrace(
                "listener accept stream create failed routeId=" & $routeId &
                " route=" & route &
                " err=" & error
              )
              restart = true
              break
            try:
              await writeJsonMessageWithTimeout(acceptStream, %*{
                "version": 1,
                "mode": "accept",
                "kind": "tcp",
                "route": route,
                "generation": listenerGeneration
              }, QuicRelayAcceptAckTimeout, "listener accept")
              let acceptAck = await readJsonMessageWithTimeout(
                acceptStream,
                QuicRelayAcceptAckTimeout,
                "listener accept ack"
              )
              if acceptAck.isNil or not acceptAck{"ok"}.getBool():
                invalidateSharedRelayClient(ownerId, relayEndpoint, client)
                quicRelayTrace(
                  "listener accept stream ack rejected routeId=" & $routeId &
                  " route=" & route &
                  " err=" & (if acceptAck.isNil: "<nil>" else: jsonString(acceptAck, "error"))
                )
                await acceptStream.closeQuicStream()
                acceptStream = nil
                restart = true
                break
            except CatchableError as exc:
              invalidateSharedRelayClient(ownerId, relayEndpoint, client)
              quicRelayTrace(
                "listener accept stream register failed routeId=" & $routeId &
                " route=" & route &
                " err=" & exc.msg
              )
              await acceptStream.closeQuicStream()
              acceptStream = nil
              restart = true
              break
          try:
            await writeJsonMessageWithTimeout(acceptStream, %*{
              "op": "await",
              "route": route,
              "generation": listenerGeneration
            }, QuicRelayAcceptAckTimeout, "listener await")
            let awaitAck = await readJsonMessageWithTimeout(
              acceptStream,
              QuicRelayAcceptAckTimeout,
              "listener await ack"
            )
            if awaitAck.isNil or not awaitAck{"ok"}.getBool() or
                jsonString(rpcPayload(awaitAck), "op") != "await":
              invalidateSharedRelayClient(ownerId, relayEndpoint, client)
              quicRelayTrace(
                "listener await rejected routeId=" & $routeId &
                " route=" & route &
                " err=" & (if awaitAck.isNil: "<nil>" else: jsonString(awaitAck, "error"))
              )
              await acceptStream.closeQuicStream()
              acceptStream = nil
              restart = true
              break
            let awaitPayload = rpcPayload(awaitAck)
            if not awaitPayload{"published"}.getBool(false):
              invalidateSharedRelayClient(ownerId, relayEndpoint, client)
              markStage("failed", "route_not_published")
              quicRelayTrace(
                "listener await missing published routeId=" & $routeId &
                " route=" & route
              )
              await acceptStream.closeQuicStream()
              acceptStream = nil
              restart = true
              break
            markReady()
            markStage("awaiting")
            quicRelayTrace(
              "listener published routeId=" & $routeId &
              " route=" & route
            )
          except CatchableError as exc:
            invalidateSharedRelayClient(ownerId, relayEndpoint, client)
            quicRelayTrace(
              "listener await failed routeId=" & $routeId &
              " route=" & route &
              " err=" & exc.msg
            )
            await acceptStream.closeQuicStream()
            acceptStream = nil
            restart = true
            break
          let controlStream =
            try:
              # The attached accept stream is the authoritative long-lived
              # rendezvous channel. After the gateway has acknowledged `await`,
              # keep this stream single-purpose and wait only for `incoming`.
              # Route publication validation must stay inside this wait path,
              # not inside read-only status APIs.
              await awaitPersistentIncomingWithRouteValidation(
                client,
                acceptStream,
                ownerId,
                routeId,
                route
              )
            except CatchableError as exc:
              invalidateSharedRelayClient(ownerId, relayEndpoint, client)
              quicRelayTrace(
                "listener incoming payload failed routeId=" & $routeId &
                " route=" & route &
                " err=" & exc.msg
              )
              markStage("dropped", exc.msg)
              await acceptStream.closeQuicStream()
              acceptStream = nil
              restart = true
              break
          let incoming =
            controlStream
          quicRelayTrace(
            "listener incoming stream accepted routeId=" & $routeId &
            " route=" & route
          )
          if incoming.isNil or incoming.kind != JObject:
            await acceptStream.closeQuicStream()
            acceptStream = nil
            restart = true
            break
          if jsonString(incoming, "op") != "incoming":
            continue
          markStage("incoming")
          quicRelayTrace("listener incoming routeId=" & $routeId & " route=" & route)
          let sourceAdvertised = jsonString(incoming, "source")
          let bridgeSessionId = jsonString(incoming, "sessionId")
          let rawTarget = rawSocketFromAddress(rawLocal).valueOr:
            discard
            restart = true
            break
          let localTarget = relayLocalAddress(rawTarget[0], rawTarget[1]).valueOr:
            discard
            restart = true
            break
          var localTransport: StreamTransport = nil
          try:
            localTransport = await connect(localTarget)
            quicRelayTrace(
              "listener connected local routeId=" & $routeId &
              " localTarget=" & $localTarget
            )
          except CatchableError:
            markStage("failed", "local_connect_failed")
            quicRelayTrace("listener local connect failed routeId=" & $routeId)
            restart = true
            break
          if sourceAdvertised.len > 0:
            let sourceAddr = MultiAddress.init(sourceAdvertised)
            if sourceAddr.isOk():
              try:
                let localObserved = rawSocketAddress($localTransport.localAddress, rawTarget[2])
                if localObserved.isOk():
                  registerResolvedRemote(ownerId, localObserved.get(), sourceAddr.get())
              except CatchableError:
                discard
          let bridgeStream = createBidiStream(client.handle, client.connPtr, client.connState).valueOr:
            await localTransport.safeCloseTransport()
            await acceptStream.closeQuicStream()
            acceptStream = nil
            quicRelayTrace(
              "listener bridge stream create failed routeId=" & $routeId &
              " route=" & route &
              " err=" & error
            )
            restart = true
            break
          try:
            await writeJsonMessage(bridgeStream, %*{
              "version": 1,
              "mode": "bridge_attach",
              "kind": "tcp",
              "route": route,
              "sessionId": bridgeSessionId
            })
            let attachAck = await readJsonMessageWithTimeout(
              bridgeStream,
              QuicRelayDialAckTimeout,
              "listener bridge attach ack"
            )
            if attachAck.isNil or not attachAck{"ok"}.getBool():
              markStage("failed",
                if attachAck.isNil: "bridge_attach_ack_nil" else: jsonString(attachAck, "error"))
              quicRelayTrace(
                "listener bridge attach rejected routeId=" & $routeId &
                " route=" & route &
                " err=" & (if attachAck.isNil: "<nil>" else: jsonString(attachAck, "error"))
              )
              await localTransport.safeCloseTransport()
              await bridgeStream.closeQuicStream()
              await acceptStream.closeQuicStream()
              acceptStream = nil
              restart = true
              break
            quicRelayTrace(
              "listener bridge attached routeId=" & $routeId &
              " route=" & route &
              " sessionId=" & bridgeSessionId
            )
            await writeJsonMessageWithTimeout(acceptStream, %*{
              "ok": true,
              "op": "ready",
              "candidates": [],
              "sessionId": bridgeSessionId,
              "generation": listenerGeneration
            }, QuicRelayAcceptAckTimeout, "listener ready")
            markStage("ready")
            quicRelayTrace("listener ready routeId=" & $routeId & " route=" & route)
            markStage("bridge_attached")
          except CatchableError as exc:
            markStage("failed", exc.msg)
            quicRelayTrace(
              "listener bridge attach failed routeId=" & $routeId &
              " route=" & route &
              " err=" & exc.msg
            )
            await localTransport.safeCloseTransport()
            await bridgeStream.closeQuicStream()
            await acceptStream.closeQuicStream()
            acceptStream = nil
            restart = true
            break
          quicRelayTrace("listener bridge start routeId=" & $routeId & " route=" & route)
          await bridgeLocalAndQuic(localTransport, bridgeStream)
          quicRelayTrace("listener bridge end routeId=" & $routeId & " route=" & route)
          continue
        if not restart:
          break
        if not relayRouteDropped(ownerId, route):
          markStage("restarting")
        if not acceptStream.isNil:
          await acceptStream.closeQuicStream()
          acceptStream = nil
      await regStream.closeQuicStream()
      regStream = nil
      await sleepAsync(200.milliseconds)
  except CancelledError:
    discard
  finally:
    if not acceptStream.isNil:
      await noCancel acceptStream.closeQuicStream()
    if not regStream.isNil:
      await noCancel regStream.closeQuicStream()
    clearReady()
    markStage("stopped")
    relayFinishRouteTask(ownerId, routeId, route, ownerEpoch)
    quicRelayTrace("listener task stop routeId=" & $routeId)

proc startRelayListener*(
    ownerId: int,
    relayUrl: string,
    advertisedText: string,
    rawLocalText: string
): Result[void, string] {.gcsafe, raises: [].} =
  quicRelaySafe:
    if ownerId <= 0:
      return err("invalid tsnet relay owner id")
    let relayEndpoint = quicRelayEndpoint(relayUrl).valueOr:
      return err(error)
    let route = ownedText(advertisedText)
    let ownerEpoch = relayOwnerEpoch(ownerId)
    if relayRouteLocallyReady(ownerId, route):
      quicRelayTrace("listener start skipped owner=" & $ownerId & " route=" & route & " reason=locally_ready")
      return ok()
    if relayOwnerHasRunningRouteTask(ownerId, route):
      quicRelayTrace("listener start skipped owner=" & $ownerId & " route=" & route & " reason=active_task")
      return ok()
    if relayOwnerHasActiveRouteTask(ownerId, route):
      quicRelayTrace("listener start clearing stale active task owner=" & $ownerId & " route=" & route)
      relayCancelRouteTasks(ownerId, route)
      clearRelayRouteTaskActive(ownerId, route, ownerEpoch)
    markRelayListenerStage(ownerId, route, "tcp", "scheduled")
    let startupDelay = RelayListenerStartupStagger * relayOwnerTaskCount(ownerId)
    markRelayRouteTaskActive(ownerId, route, ownerEpoch)
    let routeId = relayAllocateRouteId(ownerId)
    let task = listenerLoop(
      ownerId,
      ownerEpoch,
      routeId,
      startupDelay,
      ownedRelayEndpoint(relayEndpoint),
      route,
      ownedText(rawLocalText)
    )
    relayStoreTask(ownerId, routeId, route, nil, nil, task)
    asyncSpawn task
    quicRelayTrace("listener scheduled routeId=" & $routeId & " route=" & route)
    return ok()

proc startRelayListener*(
    ownerId: int,
    relayUrl: string,
    advertised: MultiAddress,
    rawLocal: MultiAddress
): Result[void, string] {.gcsafe, raises: [].} =
  startRelayListener(ownerId, relayUrl, $advertised, $rawLocal)

proc udpListenerLoop(
    ownerId: int,
    ownerEpoch: int,
    routeId: int,
    startupDelay: Duration,
    relayEndpoint: TsnetQuicRelayEndpoint,
    route: string,
    listenerCandidates: seq[string],
    rawHost: string,
    rawPort: int,
    rawFamily: string
): Future[void] {.async: (raises: [CancelledError]).} =
  quicRelayTrace("udp listener task start routeId=" & $routeId)
  var client: TsnetQuicRelayClient = nil
  var regStream: MsQuicStream = nil
  var acceptStream: MsQuicStream = nil
  var listenerGeneration = ""
  proc clearReady() =
    clearRelayReadyRouteIfTaskCurrent(ownerId, route, routeId, ownerEpoch)
  proc markReady() =
    markRelayRouteReadyIfTaskCurrent(ownerId, route, routeId, ownerEpoch)
  proc markStage(stage: string, detail = "") =
    markRelayListenerStageIfTaskCurrent(
      ownerId,
      route,
      routeId,
      ownerEpoch,
      "udp",
      stage,
      detail
    )
  proc dropClient() {.async: (raises: []).} =
    if not client.isNil:
      let stale = client
      client = nil
      await closeQuicRelayClientAfterBridgeDrain(stale)
  try:
    if startupDelay > ZeroDuration:
      quicRelayTrace(
        "udp listener startup delay routeId=" & $routeId &
        " delayMs=" & $startupDelay.milliseconds
      )
      await sleepAsync(startupDelay)
    while true:
      if relayOwnerTaskRevoked(ownerId, ownerEpoch):
        break
      clearReady()
      markStage("connecting")
      # Each long-lived UDP listener owns its relay client. A route-specific
      # registration failure must not invalidate a sibling listener on the same owner.
      client = (await connectRelayAsync(relayEndpoint)).valueOr:
        markStage("failed", error)
        quicRelayTrace("udp listener relay connect failed routeId=" & $routeId & " err=" & error)
        await sleepAsync(1.seconds)
        continue
      block setup:
        regStream = createBidiStream(client.handle, client.connPtr, client.connState).valueOr:
          await dropClient()
          markStage("failed", error)
          quicRelayTrace("udp listener stream create failed routeId=" & $routeId & " err=" & error)
          break setup
        quicRelayTrace("udp listener stream created routeId=" & $routeId & " route=" & route)
        let hello = %*{
          "version": 1,
          "mode": "listen",
          "kind": "udp",
          "route": route,
          "candidates": listenerCandidates
        }
        try:
          await writeJsonMessageWithTimeout(
            regStream,
            hello,
            QuicRelayRpcTimeout,
            "udp listener register"
          )
          quicRelayTrace("udp listener hello sent routeId=" & $routeId & " route=" & route)
          quicRelayTrace(
            "udp listener registration sent routeId=" & $routeId &
            " route=" & route &
            " mode=ack_first"
          )
          let listenerAck = await readJsonMessageWithTimeout(
            regStream,
            QuicRelayRpcTimeout,
            "udp listener register ack"
          )
          if listenerAck.isNil or not listenerAck{"ok"}.getBool():
            let rejectErr =
              if listenerAck.isNil:
                "register_ack_nil"
              else:
                jsonString(listenerAck, "error")
            await dropClient()
            markStage("failed", rejectErr)
            quicRelayTrace(
              "udp listener registration rejected routeId=" & $routeId &
              " route=" & route &
              " err=" & (if listenerAck.isNil: "<nil>" else: rejectErr)
            )
            if rejectErr == "not_found":
              quicRelayTrace(
                "udp listener fatal registration rejection routeId=" & $routeId &
                " route=" & route &
                " err=" & rejectErr
              )
              return
            break setup
          listenerGeneration = relayListenerGeneration(listenerAck)
          if listenerGeneration.len == 0:
            await dropClient()
            markStage("failed", "listener_generation_missing")
            quicRelayTrace(
              "udp listener registration missing generation routeId=" & $routeId &
              " route=" & route
            )
            break setup
        except CatchableError as exc:
          await dropClient()
          quicRelayTrace(
            "udp listener hello exchange failed routeId=" & $routeId &
            " route=" & route &
            " err=" & exc.msg
          )
          break setup
        var restart = false
        if not regStream.isNil:
          await regStream.closeQuicStream()
          regStream = nil
        while true:
          if relayOwnerTaskRevoked(ownerId, ownerEpoch):
            restart = false
            break
          if not acceptStream.isNil and not acceptStream.connectionUsable():
            quicRelayTrace(
              "udp listener dropping stale accept stream routeId=" & $routeId &
              " route=" & route
            )
            await acceptStream.closeQuicStream()
            acceptStream = nil
          if acceptStream.isNil:
            markStage("registering")
            acceptStream = createBidiStream(client.handle, client.connPtr, client.connState).valueOr:
              await dropClient()
              markStage("failed", error)
              quicRelayTrace(
                "udp listener accept stream create failed routeId=" & $routeId &
                " route=" & route &
                " err=" & error
              )
              restart = true
              break
            try:
              await writeJsonMessageWithTimeout(acceptStream, %*{
                "version": 1,
                "mode": "accept",
                "kind": "udp",
                "route": route,
                "generation": listenerGeneration
              }, QuicRelayAcceptAckTimeout, "udp listener accept")
              quicRelayTrace(
                "udp listener accept stream registered routeId=" & $routeId &
                " route=" & route
              )
              let acceptAck = await readJsonMessageWithTimeout(
                acceptStream,
                QuicRelayAcceptAckTimeout,
                "udp listener accept ack"
              )
              if not acceptAck{"ok"}.getBool():
                await dropClient()
                quicRelayTrace(
                  "udp listener accept stream ack rejected routeId=" & $routeId &
                  " route=" & route &
                  " err=" & jsonString(acceptAck, "error")
                )
                await acceptStream.closeQuicStream()
                acceptStream = nil
                restart = true
                break
              quicRelayTrace(
                "udp listener accept stream acked routeId=" & $routeId &
                " route=" & route
              )
            except CatchableError as exc:
              await dropClient()
              quicRelayTrace(
                "udp listener accept stream register failed routeId=" & $routeId &
                " route=" & route &
                " err=" & exc.msg
              )
              await acceptStream.closeQuicStream()
              acceptStream = nil
              restart = true
              break
          try:
            await writeJsonMessageWithTimeout(acceptStream, %*{
              "op": "await",
              "route": route,
              "generation": listenerGeneration
            }, QuicRelayAcceptAckTimeout, "udp listener await")
            let awaitAck = await readJsonMessageWithTimeout(
              acceptStream,
              QuicRelayAcceptAckTimeout,
              "udp listener await ack"
            )
            if awaitAck.isNil or not awaitAck{"ok"}.getBool() or
                jsonString(rpcPayload(awaitAck), "op") != "await":
              await dropClient()
              quicRelayTrace(
                "udp listener accept stream await rejected routeId=" & $routeId &
                " route=" & route &
                " err=" & (if awaitAck.isNil: "<nil>" else: jsonString(awaitAck, "error"))
              )
              await acceptStream.closeQuicStream()
              acceptStream = nil
              restart = true
              break
            quicRelayTrace(
              "udp listener accept stream awaiting routeId=" & $routeId &
              " route=" & route
            )
            let awaitPayload = rpcPayload(awaitAck)
            if not awaitPayload{"published"}.getBool(false):
              await dropClient()
              markStage("failed", "route_not_published")
              quicRelayTrace(
                "udp listener accept stream await missing published routeId=" & $routeId &
                " route=" & route
              )
              await acceptStream.closeQuicStream()
              acceptStream = nil
              restart = true
              break
            markReady()
            markStage("awaiting")
            quicRelayTrace(
              "udp listener published routeId=" & $routeId &
              " route=" & route
            )
          except CatchableError as exc:
            await dropClient()
            quicRelayTrace(
              "udp listener accept stream await failed routeId=" & $routeId &
              " route=" & route &
              " err=" & exc.msg
            )
            await acceptStream.closeQuicStream()
            acceptStream = nil
            await dropClient()
            restart = true
            break
          quicRelayTrace(
            "udp listener awaiting incoming stream routeId=" & $routeId &
            " route=" & route &
            " handshakeComplete=" & $relayHandshakeComplete(client.connState) &
            (let reason = relayCloseReason(client.connState);
             if reason.len > 0: " closeReason=" & reason else: "")
          )
          let controlStream =
            try:
              if not acceptStream.isNil:
                acceptStream
              else:
                await awaitIncomingBidiStream(client.handle, client.connState)
            except CatchableError as exc:
              quicRelayTrace(
                "udp listener incoming stream accept failed routeId=" & $routeId &
                " route=" & route &
                " err=" & exc.msg
              )
              restart = true
              break
          let incoming =
            try:
              quicRelayTrace(
                "udp listener incoming_payload_wait_begin routeId=" & $routeId &
                " route=" & route &
                " persistent=" & $(not acceptStream.isNil) &
                (let sid = controlStream.streamId();
                 if sid.isOk(): " streamId=" & $sid.get() else: "")
              )
              if not acceptStream.isNil:
                # Once the long-lived accept stream starts waiting for an
                # incoming payload, this route is already actively serving
                # traffic and must not be mistaken for an idle `awaiting`
                # listener by the repair loop.
                markStage("incoming")
              if not acceptStream.isNil:
                # Once `await` is acknowledged, the attached accept stream is
                # the only control lane for this listener. Keep route
                # validation inside this long-lived wait path instead of
                # re-probing from read-only status APIs.
                let payload = await awaitPersistentIncomingWithRouteValidation(
                  client,
                  controlStream,
                  ownerId,
                  routeId,
                  route
                )
                quicRelayTrace(
                  "udp listener incoming_payload_wait_done routeId=" & $routeId &
                  " route=" & route &
                  (let sid = controlStream.streamId();
                   if sid.isOk(): " streamId=" & $sid.get() else: "")
                )
                payload
              else:
                await readJsonMessageWithTimeout(
                  controlStream,
                  QuicRelayIncomingWaitTimeout,
                  "udp listener incoming payload"
                )
            except CatchableError as exc:
              await dropClient()
              quicRelayTrace(
                "udp listener incoming payload failed routeId=" & $routeId &
                " route=" & route &
                " err=" & exc.msg
              )
              markStage("dropped", exc.msg)
              await controlStream.closeQuicStream()
              if controlStream == acceptStream:
                acceptStream = nil
              restart = true
              break
          quicRelayTrace(
            "udp listener incoming stream accepted routeId=" & $routeId &
            " route=" & route
          )
          if incoming.isNil or incoming.kind != JObject:
            await controlStream.closeQuicStream()
            if controlStream == acceptStream:
              acceptStream = nil
            restart = true
            break
          if incoming.hasKey("ok") and not incoming{"ok"}.getBool(true):
            quicRelayTrace(
              "udp listener incoming rejected routeId=" & $routeId &
              " route=" & route &
              " err=" & jsonString(incoming, "error")
            )
            if controlStream != acceptStream:
              await controlStream.closeQuicStream()
            restart = true
            break
          if jsonString(incoming, "op") != "incoming":
            if controlStream != acceptStream:
              await controlStream.closeQuicStream()
            continue
          markStage("incoming")
          quicRelayTrace("udp listener incoming routeId=" & $routeId & " route=" & route)
          let sourceAdvertised = jsonString(incoming, "source")
          let bridgeSessionId = jsonString(incoming, "sessionId")
          let dialerCandidates = jsonStrings(incoming, "candidates")
          let localTarget = relayLocalAddress(rawHost, rawPort).valueOr:
            restart = true
            break
          let state = TsnetUdpRelayBridge(
            ownerId: ownerId,
            family: rawFamily,
            target: localTarget,
            stream: nil,
            rawKey: "",
            clientRemote: none(TransportAddress),
            pendingFrames: @[],
            pendingClientFrames: @[],
            loggedProxyIngress: false,
            loggedTargetEgress: false,
            loggedClientEgress: false
          )
          var localUdp: DatagramTransport = nil
          try:
            proc onLocalDatagram(
                transp: DatagramTransport,
                remote: TransportAddress
            ): Future[void] {.async: (raises: []).} =
              await bridgeUdpProxyToQuic(transp, state, remote, true)

            if rawFamily == "ip6":
              localUdp = newDatagramTransport6(
                onLocalDatagram,
                local = initTAddress("::1", Port(0)),
                flags = {ServerFlags.ReuseAddr}
              )
            else:
              localUdp = newDatagramTransport(
                onLocalDatagram,
                local = initTAddress("127.0.0.1", Port(0)),
                flags = {ServerFlags.ReuseAddr}
              )
          except CatchableError:
            restart = true
            break

          if sourceAdvertised.len > 0:
            let sourceAddr = MultiAddress.init(sourceAdvertised)
            if sourceAddr.isOk():
              try:
                let localObserved =
                  rawSocketAddress(localUdp.localAddress(), rawFamily, TsnetProxyKind.Quic)
                if localObserved.isOk():
                  registerResolvedRemote(ownerId, localObserved.get(), sourceAddr.get())
              except CatchableError:
                discard
              var promotedCount = 0
              for candidate in dialerCandidates:
                let candidateAddr = parseDirectCandidate(candidate)
                if candidateAddr.isNone():
                  continue
                discard registerDirectRoute(
                  ownerId,
                  sourceAddr.get(),
                  candidateAddr.get(),
                  TsnetPathPunchedDirect
                )
                inc promotedCount
              if promotedCount > 0:
                quicRelayTrace(
                  "udp listener learned punched_direct routeId=" & $routeId &
                  " route=" & route &
                  " source=" & sourceAdvertised &
                  " promoted=" & $promotedCount
                )
          let dataStream = createBidiStream(client.handle, client.connPtr, client.connState).valueOr:
            await localUdp.closeDatagramTransport()
            await acceptStream.closeQuicStream()
            acceptStream = nil
            quicRelayTrace(
              "udp listener data stream create failed routeId=" & $routeId &
              " route=" & route &
              " err=" & error
            )
            restart = true
            break
          try:
            await writeJsonMessage(dataStream, %*{
              "version": 1,
              "mode": "bridge_attach",
              "kind": "udp",
              "route": route,
              "sessionId": bridgeSessionId
            })
            let attachAck = await readJsonMessageWithTimeout(
              dataStream,
              QuicRelayDialAckTimeout,
              "udp listener bridge attach ack"
            )
            if not attachAck{"ok"}.getBool():
              markStage("failed", jsonString(attachAck, "error"))
              quicRelayTrace(
                "udp listener bridge attach rejected routeId=" & $routeId &
                " route=" & route &
                " err=" & jsonString(attachAck, "error")
              )
              await localUdp.closeDatagramTransport()
              await dataStream.closeQuicStream()
              await acceptStream.closeQuicStream()
              acceptStream = nil
              restart = true
              break
            quicRelayTrace(
              "udp listener bridge attached routeId=" & $routeId &
              " route=" & route &
              " sessionId=" & bridgeSessionId
            )
            await writeJsonMessageWithTimeout(controlStream, %*{
              "ok": true,
              "op": "ready",
              "candidates": listenerCandidates,
              "sessionId": bridgeSessionId,
              "generation": listenerGeneration
            }, QuicRelayAcceptAckTimeout, "udp listener ready")
            markStage("ready")
            quicRelayTrace("udp listener ready routeId=" & $routeId & " route=" & route)
            state.stream = dataStream
            markStage("bridge_attached")
          except CatchableError as exc:
            markStage("failed", exc.msg)
            quicRelayTrace(
              "udp listener bridge attach failed routeId=" & $routeId &
              " route=" & route &
              " err=" & exc.msg
            )
            await localUdp.closeDatagramTransport()
            await dataStream.closeQuicStream()
            await acceptStream.closeQuicStream()
            acceptStream = nil
            restart = true
            break

          await state.flushPendingUdpFrames()
          let listenerClient = client
          let bridgeTask =
            bridgeUdpAndQuicWithRelayClient(
              localUdp,
              state,
              localTarget,
              false,
              listenerClient
            )
          state.bridgeTask = bridgeTask
          # This per-session bridge is rooted by `state.bridgeTask` for the
          # lifetime of the session. We deliberately keep it out of the global
          # owner task table, which is reserved for long-lived listener/dial
          # routes and has previously been a crash source for transient bridges.
          asyncSpawn bridgeTask
          quicRelayTrace(
            "udp listener bridge task scheduled advertisedRoute=" & route &
            " sessionId=" & bridgeSessionId
          )
          continue
        if not restart:
          break
        if not relayRouteDropped(ownerId, route):
          markStage("restarting")
        if not acceptStream.isNil:
          await acceptStream.closeQuicStream()
          acceptStream = nil
      if not regStream.isNil:
        await regStream.closeQuicStream()
        regStream = nil
      await dropClient()
      await sleepAsync(200.milliseconds)
  except CancelledError:
    discard
  finally:
    if not acceptStream.isNil:
      await noCancel acceptStream.closeQuicStream()
    if not regStream.isNil:
      await noCancel regStream.closeQuicStream()
    await noCancel dropClient()
    clearReady()
    markStage("stopped")
    relayFinishRouteTask(ownerId, routeId, route, ownerEpoch)
    quicRelayTrace("udp listener task stop routeId=" & $routeId)

proc startUdpRelayListener*(
    ownerId: int,
    relayUrl: string,
    advertisedText: string,
    rawLocalText: string,
    bridgeExtraJson = ""
): Result[void, string] {.gcsafe, raises: [].} =
  quicRelaySafe:
    if ownerId <= 0:
      return err("invalid tsnet relay owner id")
    let relayEndpoint = quicRelayEndpoint(relayUrl).valueOr:
      return err(error)
    let route = ownedText(advertisedText)
    let rawTarget = rawSocketFromAddress(rawLocalText).valueOr:
      return err(error)
    let listenerCandidates = ownedTextSeq(relayCandidatesForRaw(rawLocalText, bridgeExtraJson))
    let ownerEpoch = relayOwnerEpoch(ownerId)
    if relayRouteLocallyReady(ownerId, route):
      quicRelayTrace("udp listener start skipped owner=" & $ownerId & " route=" & route & " reason=locally_ready")
      return ok()
    if relayOwnerHasRunningRouteTask(ownerId, route):
      quicRelayTrace("udp listener start skipped owner=" & $ownerId & " route=" & route & " reason=active_task")
      return ok()
    if relayOwnerHasActiveRouteTask(ownerId, route):
      quicRelayTrace("udp listener start clearing stale active task owner=" & $ownerId & " route=" & route)
      relayCancelRouteTasks(ownerId, route)
      clearRelayRouteTaskActive(ownerId, route, ownerEpoch)
    markRelayListenerStage(ownerId, route, "udp", "scheduled")
    let startupDelay = RelayListenerStartupStagger * relayOwnerTaskCount(ownerId)
    markRelayRouteTaskActive(ownerId, route, ownerEpoch)
    let routeId = relayAllocateRouteId(ownerId)
    let task = udpListenerLoop(
      ownerId,
      ownerEpoch,
      routeId,
      startupDelay,
      ownedRelayEndpoint(relayEndpoint),
      route,
      listenerCandidates,
      ownedText(rawTarget[0]),
      rawTarget[1],
      ownedText(rawTarget[2])
    )
    relayStoreTask(ownerId, routeId, route, nil, nil, task)
    asyncSpawn task
    quicRelayTrace("udp listener scheduled routeId=" & $routeId & " route=" & route)
    return ok()

proc startUdpRelayListener*(
    ownerId: int,
    relayUrl: string,
    advertised: MultiAddress,
    rawLocal: MultiAddress,
    bridgeExtraJson = ""
): Result[void, string] {.gcsafe, raises: [].} =
  startUdpRelayListener(ownerId, relayUrl, $advertised, $rawLocal, bridgeExtraJson)

proc dialRelayBridge(
    ownerId: int,
    routeId: int,
    relayEndpoint: TsnetQuicRelayEndpoint,
    route: string,
    source: string,
    localClient: StreamTransport
): Future[void] {.async: (raises: [CancelledError]).} =
  try:
    quicRelayTrace("dial bridge start routeId=" & $routeId & " route=" & route)
    let startedAt = Moment.now()
    while true:
      let client = (await sharedRelayClientAsync(ownerId, relayEndpoint)).valueOr:
        if Moment.now() >= startedAt + RelayDialRetryWindow:
          await localClient.safeCloseTransport()
          return
        await sleepAsync(RelayDialRetryStep)
        continue
      var stream: MsQuicStream = nil
      var dataStream: MsQuicStream = nil
      var retry = false
      block dialAttempt:
        stream = createBidiStream(client.handle, client.connPtr, client.connState).valueOr:
          quicRelayTrace("dial stream create failed routeId=" & $routeId)
          retry = true
          break dialAttempt
        try:
          await writeJsonMessageWithTimeout(stream, %*{
            "version": 1,
            "mode": "dial",
            "route": route,
            "source": source,
            "candidates": []
          }, QuicRelayRpcTimeout, "dial request")
          quicRelayTrace("dial request sent routeId=" & $routeId & " route=" & route)
          let dialAck = await readJsonMessageWithTimeout(
            stream,
            QuicRelayDialAckTimeout,
            "dial ack"
          )
          if not dialAck{"ok"}.getBool():
            let dialError = jsonString(dialAck, "error")
            quicRelayTrace("dial ack error routeId=" & $routeId & " err=" & dialError)
            if dialError in ["route_not_registered", "listener_not_ready", "listener_busy"] and
                Moment.now() < startedAt + RelayDialRetryWindow:
              retry = true
              break dialAttempt
            await localClient.safeCloseTransport()
            return
          quicRelayTrace("dial ack ok routeId=" & $routeId & " route=" & route)
          dataStream = stream
          stream = nil
          quicRelayTrace("dial bridge start routeId=" & $routeId & " route=" & route)
          await bridgeLocalAndQuic(localClient, dataStream)
          quicRelayTrace("dial bridge end routeId=" & $routeId & " route=" & route)
          return
        except CatchableError:
          quicRelayTrace("dial bridge exception routeId=" & $routeId)
          retry = Moment.now() < startedAt + RelayDialRetryWindow
      await stream.closeQuicStream()
      await dataStream.closeQuicStream()
      if not retry:
        await localClient.safeCloseTransport()
        return
      await sleepAsync(RelayDialRetryStep)
  except CancelledError:
    quicRelayTrace("udp dial bridge cancelled routeId=" & $routeId)
    discard
  except CatchableError:
    discard
  finally:
    await localClient.safeCloseTransport()

proc openDialProxy*(
    ownerId: int,
    relayUrl: string,
    family: string,
    localTailnetIp: string,
    remoteAdvertised: MultiAddress
): Result[MultiAddress, string] =
  if ownerId <= 0:
    return err("invalid tsnet relay owner id")
  let relayEndpoint = quicRelayEndpoint(relayUrl).valueOr:
    return err(error)
  if family notin ["ip4", "ip6"]:
    return err("unsupported tsnet relay family " & family)
  if localTailnetIp.strip().len == 0:
    return err("missing local tailnet IP for tsnet relay dial proxy")
  let loopbackHost = if family == "ip6": "::1" else: "127.0.0.1"
  let bindAddress =
    try:
      initTAddress(loopbackHost, Port(0))
    except CatchableError as exc:
      return err("failed to bind tsnet relay dial server: " & exc.msg)
  let completion = newFuture[void]("tsnet.quicrelay.dialserver")
  let handleRouteId = relayAllocateRouteId(ownerId)
  let route = $remoteAdvertised
  let source = buildAdvertisedSource(family, localTailnetIp, 0)
  var server: StreamServer = nil
  try:
    proc serveDialClient(
        srv: StreamServer,
        localClient: StreamTransport
    ) {.async: (raises: []).} =
      try:
        srv.stop()
      except CatchableError:
        discard
      try:
        let localPort =
          try:
            let localAddr = MultiAddress.init(localClient.localAddress).valueOr:
              raise newException(CatchableError, error)
            portFromAddress(localAddr)
          except CatchableError:
            0
        let adjustedSource =
          if localPort > 0:
            buildAdvertisedSource(family, localTailnetIp, localPort)
          else:
            source
        await dialRelayBridge(
          ownerId,
          handleRouteId,
          ownedRelayEndpoint(relayEndpoint),
          route,
          adjustedSource,
          localClient
        )
      except CancelledError:
        discard
      finally:
        if not completion.finished():
          completion.complete()

    server = createStreamServer(bindAddress, serveDialClient, flags = {ReuseAddr})
    server.start()
  except CatchableError as exc:
    return err("failed to start tsnet relay dial server: " & exc.msg)
  proc ownDialServer() {.async: (raises: []).} =
    try:
      await completion
    except CancelledError:
      discard
    except CatchableError:
      discard
    finally:
      if not server.isNil:
        try:
          server.stop()
        except CatchableError:
          discard
        try:
          server.close()
        except CatchableError:
          discard
  let serverTask = ownDialServer()
  relayStoreTask(ownerId, handleRouteId, "", nil, nil, serverTask)
  asyncSpawn serverTask
  let rawAddress = MultiAddress.init(server.sock.getLocalAddress()).valueOr:
    try:
      server.stop()
      server.close()
    except CatchableError:
      discard
    return err("failed to build tsnet relay dial proxy address: " & error)
  quicRelayTrace("dial server scheduled routeId=" & $handleRouteId & " raw=" & $rawAddress)
  ok(rawAddress)

proc dialUdpBridge(
    routeId: int,
    relayEndpoint: TsnetQuicRelayEndpoint,
    route: string,
    source: string,
    proxy: DatagramTransport,
    state: TsnetUdpRelayBridge,
    dialerCandidates: seq[string]
): Future[void] {.async: (raises: []).} =
  var lastError = ""
  var attempt = 0
  try:
    quicRelayTrace(
      "udp dial bridge start routeId=" & $routeId &
      " route=" & route &
      " source=" & source
    )
    let startedAt = Moment.now()
    while true:
      inc attempt
      markUdpDialProgress(
        state.ownerId,
        state.rawKey,
        "connect",
        relayEndpoint.url,
        attempt
      )
      let client = (await sharedRelayClientAsync(state.ownerId, relayEndpoint)).valueOr:
        quicRelayTrace("udp dial bridge connect failed routeId=" & $routeId & " err=" & error)
        lastError = "udp dial bridge connect failed: " & error
        markUdpDialProgress(state.ownerId, state.rawKey, "retrying", lastError, attempt)
        if Moment.now() >= startedAt + RelayDialRetryWindow:
          markUdpDialFailed(state.ownerId, state.rawKey, lastError)
          await proxy.closeDatagramTransport()
          return
        await sleepAsync(RelayDialRetryStep)
        continue
      var stream: MsQuicStream = nil
      var dataStream: MsQuicStream = nil
      var retry = false
      block dialAttempt:
        markUdpDialProgress(state.ownerId, state.rawKey, "stream_open", route, attempt)
        stream = createBidiStream(client.handle, client.connPtr, client.connState).valueOr:
          quicRelayTrace("udp dial bridge stream create failed routeId=" & $routeId & " err=" & error)
          lastError = "udp dial bridge stream create failed: " & error
          markUdpDialProgress(state.ownerId, state.rawKey, "retrying", lastError, attempt)
          retry = true
          break dialAttempt
        try:
          await writeJsonMessageWithTimeout(stream, %*{
            "version": 1,
            "mode": "dial",
            "kind": "udp",
            "route": route,
            "source": source,
            "candidates": dialerCandidates
          }, QuicRelayRpcTimeout, "udp dial request")
          markUdpDialProgress(state.ownerId, state.rawKey, "ack_wait", route, attempt)
          quicRelayTrace("udp dial bridge request sent routeId=" & $routeId & " route=" & route)
          let dialAck = await readJsonMessageWithTimeout(
            stream,
            QuicRelayDialAckTimeout,
            "udp dial ack"
          )
          if not dialAck{"ok"}.getBool():
            let dialError = jsonString(dialAck, "error")
            quicRelayTrace("udp dial bridge ack error routeId=" & $routeId & " err=" & dialError)
            lastError = "udp dial bridge ack error: " & dialError
            markUdpDialProgress(
              state.ownerId,
              state.rawKey,
              "retrying",
              if dialError.len > 0: dialError else: lastError,
              attempt
            )
            if dialError in ["route_not_registered", "listener_not_ready", "listener_busy"] and
                Moment.now() < startedAt + RelayDialRetryWindow:
              retry = true
              break dialAttempt
            markUdpDialFailed(state.ownerId, state.rawKey, lastError)
            await proxy.closeDatagramTransport()
            return
          quicRelayTrace("udp dial bridge ack ok routeId=" & $routeId & " route=" & route)
          markUdpDialProgress(state.ownerId, state.rawKey, "ack_ok", route, attempt)
          let advertisedRoute = MultiAddress.init(route)
          if advertisedRoute.isOk():
            var promotedCount = 0
            for candidate in jsonStrings(rpcPayload(dialAck), "candidates"):
              let candidateAddr = parseDirectCandidate(candidate)
              if candidateAddr.isNone():
                quicRelayTrace(
                  "udp dial bridge skipped unusable candidate routeId=" & $routeId &
                  " route=" & route &
                  " candidate=" & candidate
                )
                continue
              discard registerDirectRoute(
                state.ownerId,
                advertisedRoute.get(),
                candidateAddr.get(),
                TsnetPathPunchedDirect
              )
              registerResolvedRemote(state.ownerId, candidateAddr.get(), advertisedRoute.get())
              inc promotedCount
              quicRelayTrace(
                "udp dial bridge promoted punched_direct routeId=" & $routeId &
                " route=" & route &
                " candidate=" & candidate
              )
            if promotedCount == 0:
              quicRelayTrace(
                "udp dial bridge had no usable direct candidates routeId=" & $routeId &
                " route=" & route
              )
          dataStream = stream
          stream = nil
          state.stream = dataStream
          markUdpDialReady(state.ownerId, state.rawKey)
          markUdpDialProgress(state.ownerId, state.rawKey, "active", route, attempt)
          await state.flushPendingUdpFrames()
          quicRelayTrace("udp dial bridge stream active routeId=" & $routeId & " route=" & route)
          await bridgeUdpAndQuic(proxy, state, state.target, true)
          quicRelayTrace("udp dial bridge completed routeId=" & $routeId & " route=" & route)
          return
        except CatchableError as exc:
          lastError = "udp dial bridge exception: " & exc.msg
          quicRelayTrace("udp dial bridge exception routeId=" & $routeId & " err=" & exc.msg)
          markUdpDialProgress(state.ownerId, state.rawKey, "retrying", lastError, attempt)
          retry = Moment.now() < startedAt + RelayDialRetryWindow
      await stream.closeQuicStream()
      await dataStream.closeQuicStream()
      if not retry:
        markUdpDialFailed(state.ownerId, state.rawKey, lastError)
        await proxy.closeDatagramTransport()
        return
      await sleepAsync(RelayDialRetryStep)
  except CancelledError:
    quicRelayTrace("udp dial bridge cancelled routeId=" & $routeId)
    markUdpDialFailed(state.ownerId, state.rawKey, "udp dial bridge cancelled")
    discard
  except CatchableError as exc:
    lastError = "udp dial bridge fatal: " & exc.msg
    quicRelayTrace("udp dial bridge fatal routeId=" & $routeId & " err=" & exc.msg)
    markUdpDialFailed(state.ownerId, state.rawKey, lastError)
    discard
  finally:
    await proxy.closeDatagramTransport()

proc openDialUdpProxy*(
    ownerId: int,
    relayUrl: string,
    family: string,
    localTailnetIp: string,
    remoteAdvertised: MultiAddress,
    bridgeExtraJson = ""
): Result[MultiAddress, string] =
  if ownerId <= 0:
    return err("invalid tsnet relay owner id")
  let relayEndpoint = quicRelayEndpoint(relayUrl).valueOr:
    return err(error)
  if family notin ["ip4", "ip6"]:
    return err("unsupported tsnet relay family " & family)
  if localTailnetIp.strip().len == 0:
    return err("missing local tailnet IP for tsnet relay UDP dial proxy")
  let route = $remoteAdvertised
  let taskRoute = relayDialTaskRoute(route)
  relayCancelRouteTasks(ownerId, taskRoute)
  var proxy: DatagramTransport = nil
  let placeholderTarget =
    try:
      initTAddress(if family == "ip6": "::1" else: "127.0.0.1", Port(0))
    except CatchableError as exc:
      return err("failed to initialize tsnet relay UDP placeholder address: " & exc.msg)
  let state = TsnetUdpRelayBridge(
    ownerId: ownerId,
    family: family,
    target: placeholderTarget,
    stream: nil,
    rawKey: "",
    clientRemote: none(TransportAddress),
    pendingFrames: @[],
    pendingClientFrames: @[],
    loggedProxyIngress: false,
    loggedTargetEgress: false,
    loggedClientEgress: false
  )
  try:
    proc onProxyDatagram(
        transp: DatagramTransport,
        remote: TransportAddress
    ): Future[void] {.async: (raises: []).} =
      await bridgeUdpProxyToQuic(transp, state, remote, false)

    if family == "ip6":
      proxy = newDatagramTransport6(
        onProxyDatagram,
        local = initTAddress("::1", Port(0)),
        flags = {ServerFlags.ReuseAddr}
      )
    else:
      proxy = newDatagramTransport(
        onProxyDatagram,
        local = initTAddress("127.0.0.1", Port(0)),
        flags = {ServerFlags.ReuseAddr}
      )
  except CatchableError as exc:
    return err("failed to start tsnet relay UDP proxy: " & exc.msg)

  let proxyLocalAddress =
    try:
      proxy.localAddress()
    except CatchableError as exc:
      try:
        proxy.close()
      except CatchableError:
        discard
      return err("failed to inspect tsnet relay UDP proxy local address: " & exc.msg)
  let rawAddress = rawSocketAddress(proxyLocalAddress, family, TsnetProxyKind.Quic).valueOr:
    try:
      proxy.close()
    except CatchableError:
      discard
    return err(error)
  let source = buildAdvertisedSource(family, localTailnetIp, portFromAddress(rawAddress), TsnetProxyKind.Quic)
  let rawKey = $rawAddress
  let dialerCandidates = relayCandidatesForRaw($rawAddress, bridgeExtraJson)
  state.rawKey = rawKey
  # The loopback UDP proxy is bound, but the relay stream is not usable until
  # the dial bridge has received an ACK and attached the QUIC stream.
  markUdpDialPending(ownerId, rawKey)
  var handleRouteId = 0
  quicRelaySafe:
    handleRouteId = relayAllocateRouteId(ownerId)
  let bridgeTask = dialUdpBridge(
    handleRouteId,
    ownedRelayEndpoint(relayEndpoint),
    route,
    source,
    proxy,
    state,
    dialerCandidates
  )
  quicRelaySafe:
    # The dial bridge task owns and closes `proxy` in its own finally path.
    # Keeping only the task handle avoids storing the live datagram transport
    # in the owner task table.
    relayStoreTask(ownerId, handleRouteId, taskRoute, nil, nil, bridgeTask)
  asyncSpawn bridgeTask
  quicRelayTrace(
    "udp dial proxy scheduled routeId=" & $handleRouteId &
    " route=" & route &
    " raw=" & $rawAddress
  )
  ok(rawAddress)
