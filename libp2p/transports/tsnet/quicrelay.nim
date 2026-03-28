{.push raises: [].}

import chronos
import chronos/osdefs
import std/[json, locks, nativesockets, options, os, sequtils, sets, strutils, tables, uri]
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
  QuicRelayAcceptRefreshTimeout = 10.seconds
  QuicRelayIncomingWaitTimeout = 30.seconds
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
  TsnetQuicRelayEndpoint* = object
    url*: string
    host*: string
    authority*: string
    port*: uint16
    prefix*: string

  TsnetRelayTaskHandle = ref object
    routeId: int
    task: Future[void]
    server: StreamServer
    datagram: DatagramTransport

  TsnetUdpDialDiagnostic* = object
    phase*: string
    detail*: string
    attempts*: int
    updatedUnixMilli*: int64

  TsnetQuicRelayListener = ref object
    connPtr: pointer
    connState: msquicdrv.MsQuicConnectionState
    candidates: seq[string]
    stream: MsQuicStream
    reuseStream: bool
    busy: bool

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

  TsnetQuicRelayClient = ref object
    endpoint: TsnetQuicRelayEndpoint
    handle: msquicdrv.MsQuicTransportHandle
    connPtr: pointer
    connState: msquicdrv.MsQuicConnectionState

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
var relayNextRouteId {.global.}: int
var relayOwnerTasks {.global.}: Table[int, seq[TsnetRelayTaskHandle]] = initTable[int, seq[TsnetRelayTaskHandle]]()
var relayReadyRoutes {.global.}: Table[int, HashSet[string]] = initTable[int, HashSet[string]]()
var relayUdpDialStates {.global.}: Table[int, Table[string, TsnetUdpDialDiagnostic]] =
  initTable[int, Table[string, TsnetUdpDialDiagnostic]]()

initLock(relayRegistryLock)

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

proc defaultRelayPort(parsed: Uri): uint16 =
  if parsed.port.len > 0:
    try:
      return uint16(max(1, parsed.port.parseInt()))
    except CatchableError:
      discard
  NimTsnetQuicRelayDefaultPort

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

proc tlsServerName(host: string): Option[string] =
  if host.len == 0:
    return none(string)
  let suppressIpSni =
    getEnv("NIM_TSNET_RELAY_QUIC_NO_SNI", "").strip().toLowerAscii() in
      ["1", "true", "yes"]
  if suppressIpSni and detectAddressFamily(host) != 0'u16:
    return none(string)
  some(host)

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
  parseRuntimePreference(getEnv("NIM_TSNET_RELAY_QUIC_RUNTIME", "")).get(defaultPreference)

proc resolveDialHost(host: string, port: uint16): Result[string, string] =
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
      if parsed.port.len > 0:
        defaultRelayPort(parsed)
      else:
        NimTsnetQuicRelayDefaultPort
    else:
      defaultRelayPort(parsed)
  let authorityHost = bracketAuthorityHost(parsed.hostname)
  let authority = authorityHost & ":" & $port
  ok(TsnetQuicRelayEndpoint(
    url: "quic://" & authority & NimTsnetQuicRelayPrefix,
    host: parsed.hostname,
    authority: authority,
    port: port,
    prefix: NimTsnetQuicRelayPrefix
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

proc isLoopbackRelayHost(host: string): bool =
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
  try:
    quicRelaySafe:
      msquicdrv.closeConnection(handle, connPtr, connState)
  except CatchableError:
    discard

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
  var frameLen = -1
  if payload.len >= 4:
    frameLen = int(readUint32BE(payload, 0))
  while true:
    try:
      let chunk = await stream.read()
      if chunk.len == 0:
        break
      payload.add(chunk)
      if frameLen < 0 and payload.len >= 4:
        frameLen = int(readUint32BE(payload, 0))
      if frameLen >= 0 and payload.len >= 4 + frameLen:
        let frameEnd = 4 + frameLen
        let node = parseJson(bytesToString(payload.toOpenArray(4, frameEnd - 1)))
        if payload.len > frameEnd:
          stream.restoreCachedBytes(payload[frameEnd ..< payload.len])
        else:
          stream.restoreCachedBytes(@[])
        return node
    except LPStreamEOFError:
      break
    except CatchableError as exc:
      if frameLen < 0 and payload.len >= 4:
        frameLen = int(readUint32BE(payload, 0))
      if frameLen >= 0 and payload.len >= 4 + frameLen:
        let frameEnd = 4 + frameLen
        let node = parseJson(bytesToString(payload.toOpenArray(4, frameEnd - 1)))
        if payload.len > frameEnd:
          stream.restoreCachedBytes(payload[frameEnd ..< payload.len])
        else:
          stream.restoreCachedBytes(@[])
        return node
      raise exc
  if payload.len == 0:
    stream.restoreCachedBytes(@[])
    raise newException(
      IOError,
      "nim_quic relay stream closed before a complete JSON frame was received"
    )
  if frameLen >= 0 and payload.len >= 4 + frameLen:
    let frameEnd = 4 + frameLen
    let node = parseJson(bytesToString(payload.toOpenArray(4, frameEnd - 1)))
    if payload.len > frameEnd:
      stream.restoreCachedBytes(payload[frameEnd ..< payload.len])
    else:
      stream.restoreCachedBytes(@[])
    return node
  stream.restoreCachedBytes(@[])
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

proc readJsonMessageWithTimeout(
    stream: MsQuicStream,
    timeout: Duration,
    label: string
): Future[JsonNode] {.async: (raises: [CancelledError, CatchableError]).} =
  let readFut = readJsonMessage(stream)
  let timeoutFut = sleepAsync(timeout)
  let winner = await race(cast[FutureBase](readFut), cast[FutureBase](timeoutFut))
  if winner == cast[FutureBase](timeoutFut):
    raise newException(
      IOError,
      "nim_quic relay timed out waiting for " & label & " after " & $timeout
    )
  timeoutFut.cancel()
  await readFut

proc readBinaryFrame(
    stream: MsQuicStream
): Future[seq[byte]] {.async: (raises: [CancelledError, CatchableError]).} =
  var payload = stream.takeCachedBytes()
  var frameLen = -1
  if payload.len >= 4:
    frameLen = int(readUint32BE(payload, 0))
  while true:
    try:
      let chunk = await stream.read()
      if chunk.len == 0:
        break
      payload.add(chunk)
      if frameLen < 0 and payload.len >= 4:
        frameLen = int(readUint32BE(payload, 0))
      if frameLen >= 0 and payload.len >= 4 + frameLen:
        let frameEnd = 4 + frameLen
        if payload.len > frameEnd:
          stream.restoreCachedBytes(payload[frameEnd ..< payload.len])
        else:
          stream.restoreCachedBytes(@[])
        if frameLen == 0:
          return @[]
        return payload.toOpenArray(4, frameEnd - 1).toSeq()
    except LPStreamEOFError:
      break
  if frameLen >= 0 and payload.len >= 4 + frameLen:
    let frameEnd = 4 + frameLen
    if payload.len > frameEnd:
      stream.restoreCachedBytes(payload[frameEnd ..< payload.len])
    else:
      stream.restoreCachedBytes(@[])
    if frameLen == 0:
      return @[]
    return payload.toOpenArray(4, frameEnd - 1).toSeq()
  stream.restoreCachedBytes(@[])
  @[]

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
    await stream.closeImpl()
  except CatchableError:
    discard

proc finishQuicStream(
    stream: MsQuicStream,
    drainDelay: Duration = QuicRelayGatewayResponseDrainDelay
) {.async: (raises: []).} =
  if stream.isNil:
    return
  stream.restoreCachedBytes(@[])
  try:
    await stream.sendFin()
  except CatchableError:
    discard
  if drainDelay > ZeroDuration:
    try:
      await sleepAsync(drainDelay)
    except CatchableError:
      discard
  await stream.closeQuicStream()

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
    rawKey: string
    clientRemote: Option[TransportAddress]
    pendingFrames: seq[seq[byte]]
    writeGate: AsyncLock
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
    " pendingFrames=" & $state.pendingFrames.len

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

proc bridgeUdpProxyToQuic(
    transp: DatagramTransport,
    state: TsnetUdpRelayBridge,
    remote: TransportAddress,
    restrictSource: bool
) {.async: (raises: []).} =
  if transp.isNil or state.isNil:
    return
  try:
    if restrictSource and $remote != $state.target:
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
    if not restrictSource and state.clientRemote.isNone():
      state.clientRemote = some(remote)
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
    let destination =
      if toClient:
        if state.clientRemote.isNone():
          continue
        state.clientRemote.get()
      else:
        target
    if toClient:
      inc state.clientEgressPackets
      state.clientEgressBytes += payload.len.int64
      if not state.loggedClientEgress:
        state.loggedClientEgress = true
        quicRelayTrace(
          "udp relay egress->client owner=" & $state.ownerId &
          " rawKey=" & state.rawKey &
          " dest=" & $destination &
          " len=" & $payload.len
        )
    else:
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

proc relayRegisterTask(
    ownerId: int,
    server: StreamServer,
    datagram: DatagramTransport,
    task: Future[void]
): TsnetRelayTaskHandle =
  if ownerId <= 0:
    return TsnetRelayTaskHandle(routeId: 0, server: server, datagram: datagram, task: task)
  withLock(relayRegistryLock):
    inc relayNextRouteId
    result = TsnetRelayTaskHandle(
      routeId: relayNextRouteId,
      server: server,
      datagram: datagram,
      task: task
    )
    var handles = relayOwnerTasks.getOrDefault(ownerId)
    handles.add(result)
    relayOwnerTasks[ownerId] = handles

proc relayOwnerTaskCount(ownerId: int): int =
  if ownerId <= 0:
    return 0
  withLock(relayRegistryLock):
    result = relayOwnerTasks.getOrDefault(ownerId).len

proc markRelayRouteReady(ownerId: int, route: string) {.gcsafe, raises: [].} =
  quicRelaySafe:
    if ownerId <= 0 or route.len == 0:
      return
    withLock(relayRegistryLock):
      var routes = relayReadyRoutes.getOrDefault(ownerId)
      routes.incl(route)
      relayReadyRoutes[ownerId] = routes

proc clearRelayReadyRoutes(ownerId: int) {.gcsafe, raises: [].} =
  quicRelaySafe:
    if ownerId <= 0:
      return
    withLock(relayRegistryLock):
      relayReadyRoutes.del(ownerId)

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
    withLock(relayRegistryLock):
      var states = relayUdpDialStates.getOrDefault(ownerId)
      states[rawKey] = TsnetUdpDialDiagnostic(
        phase: phase,
        detail: detail,
        attempts: max(0, attempts),
        updatedUnixMilli: currentUnixMilli()
      )
      relayUdpDialStates[ownerId] = states

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
      let states = relayUdpDialStates.getOrDefault(ownerId)
      if not states.hasKey(rawKey):
        return
      result.known = true
      let value = states.getOrDefault(rawKey)
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

proc udpDialStatesPayload*(ownerId: int): JsonNode {.gcsafe, raises: [].} =
  result = newJArray()
  quicRelaySafe:
    if ownerId <= 0:
      return
    withLock(relayRegistryLock):
      let states = relayUdpDialStates.getOrDefault(ownerId)
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
    if ownerId <= 0 or route.len == 0:
      return false
    withLock(relayRegistryLock):
      let routes = relayReadyRoutes.getOrDefault(ownerId)
      result = route in routes

proc relayRouteReady*(ownerId: int, route: string): bool {.gcsafe, raises: [].} =
  udpRelayRouteReady(ownerId, route)

proc stopRelayListeners*(ownerId: int) =
  if ownerId <= 0:
    return
  var handles: seq[TsnetRelayTaskHandle] = @[]
  withLock(relayRegistryLock):
    handles = relayOwnerTasks.getOrDefault(ownerId)
    relayOwnerTasks.del(ownerId)
    relayReadyRoutes.del(ownerId)
    relayUdpDialStates.del(ownerId)
  for handle in handles:
    if not handle.server.isNil:
      try:
        handle.server.stop()
        handle.server.close()
      except CatchableError:
        discard
    if not handle.datagram.isNil:
      try:
        handle.datagram.close()
      except CatchableError:
        discard
    if not handle.task.isNil and not handle.task.finished():
      handle.task.cancelSoon()

proc new*(_: type[TsnetQuicRelayGateway]): TsnetQuicRelayGateway =
  result = TsnetQuicRelayGateway(
    boundPort: NimTsnetQuicRelayDefaultPort,
    listeners: initTable[string, TsnetQuicRelayListener]()
  )
  initLock(result.listenersLock)
  result.listenersLockInit = true

proc storeListener(
    gateway: TsnetQuicRelayGateway,
    route: string,
    connPtr: pointer,
    connState: msquicdrv.MsQuicConnectionState,
    candidates: seq[string] = @[],
    stream: MsQuicStream = nil,
    reuseStream = false
) =
  if gateway.isNil:
    return
  acquire(gateway.listenersLock)
  try:
    gateway.listeners[route] = TsnetQuicRelayListener(
      connPtr: connPtr,
      connState: connState,
      candidates: candidates,
      stream: stream,
      reuseStream: reuseStream
    )
  finally:
    release(gateway.listenersLock)

proc attachListenerStream(
    gateway: TsnetQuicRelayGateway,
    route: string,
    connPtr: pointer,
    connState: msquicdrv.MsQuicConnectionState,
    stream: MsQuicStream
): bool =
  if gateway.isNil or route.len == 0 or stream.isNil:
    return false
  acquire(gateway.listenersLock)
  try:
    let listener = gateway.listeners.getOrDefault(route, nil)
    if listener.isNil:
      return false
    listener.connPtr = connPtr
    listener.connState = connState
    listener.stream = stream
    listener.busy = false
    return true
  finally:
    release(gateway.listenersLock)

proc consumeListenerStream(
    gateway: TsnetQuicRelayGateway,
    route: string
): MsQuicStream =
  if gateway.isNil or route.len == 0:
    return nil
  acquire(gateway.listenersLock)
  try:
    let listener = gateway.listeners.getOrDefault(route, nil)
    if listener.isNil:
      return nil
    result = listener.stream
    listener.stream = nil
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

proc clearListenerRoutesForConnection(
    gateway: TsnetQuicRelayGateway,
    connPtr: pointer
) =
  if gateway.isNil or connPtr.isNil:
    return
  acquire(gateway.listenersLock)
  try:
    var kept = initTable[string, TsnetQuicRelayListener]()
    for route, listener in gateway.listeners.pairs():
      if listener.isNil or listener.connPtr != connPtr:
        kept[route] = listener
    gateway.listeners = kept
  finally:
    release(gateway.listenersLock)

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

proc serveRelayStream(
    gateway: TsnetQuicRelayGateway,
    connPtr: pointer,
    connState: msquicdrv.MsQuicConnectionState,
    stream: MsQuicStream
) {.async: (raises: []).} =
  var keepOpen = false
  try:
    let request = await readJsonMessage(stream)
    let mode = jsonString(request, "mode")
    let route = jsonString(request, "route")
    let candidates = jsonStrings(request, "candidates")
    let kind = jsonString(request, "kind").toLowerAscii()
    quicRelayTrace("gateway request mode=" & mode & " route=" & route)
    case mode
    of "listen":
      let reuseStream = request{"reuseStream"}.getBool(false)
      gateway.storeListener(
        route,
        connPtr,
        connState,
        candidates,
        nil,
        reuseStream
      )
      quicRelayTrace("gateway stored listener route=" & route)
      await writeJsonMessage(stream, rpcSuccess(%*{
          "mode": "listen",
          "route": route,
          "candidates": candidates
      }))
    of "accept":
      if not gateway.attachListenerStream(route, connPtr, connState, stream):
        quicRelayTrace("gateway missing accept route=" & route)
        await writeJsonMessage(stream, rpcError("route_not_registered"))
        return
      # Once the accept stream is attached, ownership belongs to the listener
      # route. Do not leave it in the short-lived request/response lifecycle;
      # otherwise a cancellation during the initial ack write can wrongly fall
      # through `finally` and close the long-lived stream.
      keepOpen = true
      quicRelayTrace("gateway accept stream attached route=" & route)
      await writeJsonMessage(stream, rpcSuccess(%*{
        "mode": "accept",
        "route": route
      }))
      quicRelayTrace("gateway accept ack sent route=" & route)
    of "route_status":
      let published = not gateway.getListener(route).isNil
      await writeJsonMessage(stream, rpcSuccess(%*{
        "mode": "route_status",
        "route": route,
        "published": published
      }))
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
      let attachedStream = not listener.stream.isNil
      let reuseStream = listener.reuseStream and attachedStream
      let splitDialDataStream = kind.toLowerAscii() == "udp"
      var listenerControlStream: MsQuicStream = nil
      try:
        if attachedStream:
          listenerControlStream = gateway.consumeListenerStream(route)
        if listenerControlStream.isNil:
          listenerControlStream =
            createBidiStream(gateway.handle, listener.connPtr, listener.connState).valueOr:
              gateway.clearListenerRoutesForConnection(listener.connPtr)
              quicRelayTrace("gateway listener stream create failed route=" & route & " err=" & error)
              await writeJsonMessage(stream, rpcError("listener_not_ready"))
              return
        quicRelayTrace(
          "gateway notifying listener route=" & route &
          " reuseStream=" & $reuseStream &
          " attachedStream=" & $attachedStream
        )
        if attachedStream and reuseStream:
          let awaitMsg =
            try:
              await readJsonMessageWithTimeout(
                listenerControlStream,
                QuicRelayAcceptAwaitTimeout,
                "gateway listener await"
              )
            except CatchableError as exc:
              quicRelayTrace(
                "gateway listener await failed route=" & route &
                " err=" & exc.msg
              )
              await writeJsonMessage(stream, rpcError("listener_not_ready"))
              await listenerControlStream.closeQuicStream()
              return
          if awaitMsg.isNil or awaitMsg.kind != JObject or
              jsonString(awaitMsg, "op") != "await":
            quicRelayTrace(
              "gateway listener await rejected route=" & route &
              " payload=" & (if awaitMsg.isNil: "<nil>" else: $awaitMsg)
            )
            await writeJsonMessage(stream, rpcError("listener_not_ready"))
            await listenerControlStream.closeQuicStream()
            return
        await writeJsonMessage(listenerControlStream, %*{
          "op": "incoming",
          "route": route,
          "source": source,
          "candidates": candidates
        })
        let ready =
          try:
            await readJsonMessageWithTimeout(
              listenerControlStream,
              QuicRelayListenerReadyTimeout,
              "gateway listener ready"
            )
          except CatchableError as exc:
            quicRelayTrace(
              "gateway listener ready wait failed route=" & route &
              " err=" & exc.msg
            )
            await writeJsonMessage(stream, rpcError("listener_not_ready"))
            await listenerControlStream.closeQuicStream()
            return
        if not ready{"ok"}.getBool():
          quicRelayTrace("gateway listener not ready route=" & route)
          if not splitDialDataStream:
            await writeJsonMessage(stream, rpcError("listener_not_ready"))
          await listenerControlStream.closeQuicStream()
          return
        let readyCandidates = jsonStrings(ready, "candidates")
        let listenerCandidates =
          if readyCandidates.len > 0: readyCandidates else: listener.candidates
        quicRelayTrace("gateway listener ready route=" & route)
        await writeJsonMessage(stream, rpcSuccess(%*{
          "mode": "dial",
          "route": route,
          "candidates": listenerCandidates
        }))
        quicRelayTrace("gateway bridging route=" & route)
        if attachedStream or splitDialDataStream:
          # For UDP, reuse the existing dial/incoming control streams as the
          # bridged data streams after the JSON handshake completes. Prefer the
          # listener's pre-attached accept stream whenever available so WAN
          # live traffic does not depend on a server-initiated incoming stream.
          await bridgeQuicStreams(listenerControlStream, stream)
        else:
          await listenerControlStream.closeQuicStream()
          let listenerDataStream =
            createBidiStream(gateway.handle, listener.connPtr, listener.connState).valueOr:
              gateway.clearListenerRoutesForConnection(listener.connPtr)
              quicRelayTrace("gateway listener data stream create failed route=" & route & " err=" & error)
              await writeJsonMessage(stream, rpcError("listener_not_ready"))
              return
          await writeJsonMessage(listenerDataStream, %*{
            "op": "bridge",
            "kind": kind,
            "route": route
          })
          await bridgeQuicStreams(listenerDataStream, stream)
      finally:
        gateway.releaseListenerRoute(route)
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
    endpoint: endpoint,
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
    if isLoopbackRelayHost(endpoint.host):
      return ok(client)
    let pinged = ensureRelayReady(client, endpoint)
    if pinged.isOk():
      return ok(client)
    let readyErr = pinged.error
    failures.add(readyErr)
    quicRelayTrace(
      "relay readiness failed endpoint=" & endpoint.url &
      " attempt=" & $(attempt + 1) &
      " err=" & readyErr
    )
    closeQuicRelayClient(client)
    if attempt + 1 < QuicRelayConnectAttempts:
      try:
        waitFor sleepAsync(QuicRelayConnectRetryBackoff)
      except CatchableError:
        discard
  if failures.len == 0:
    return err("nim_quic relay connect failed")
  err(failures.join("; "))

proc keepRelayConnectionAlive(
    client: TsnetQuicRelayClient,
    label: string
): Future[void] {.async: (raises: [CancelledError]).} =
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
      return
    except CatchableError as exc:
      quicRelayTrace("relay keepalive failed label=" & label & " err=" & exc.msg)
      return
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
  var listenerIncomingStream: MsQuicStream = nil
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

    await listenerRegStream.closeQuicStream()
    listenerRegStream = nil
    listenerIncomingStream =
      try:
        await awaitIncomingBidiStream(listenerClient.handle, listenerClient.connState)
      except CatchableError as exc:
        result.error = "listener incoming stream accept failed: " & exc.msg
        return
    let incoming = await readJsonMessageWithTimeout(
      listenerIncomingStream,
      QuicRelayRpcTimeout,
      "listener incoming payload"
    )
    result.dialerCandidates = jsonStrings(incoming, "candidates")
    await writeJsonMessage(listenerIncomingStream, %*{
      "ok": true,
      "op": "ready",
      "candidates": listenerCandidates
    })

    let dialAck = await readJsonMessageWithTimeout(dialerStream, QuicRelayRpcTimeout, "dial ack")
    if not dialAck{"ok"}.getBool():
      result.error = jsonString(dialAck, "error")
      return
    result.listenerCandidates = jsonStrings(rpcPayload(dialAck), "candidates")
    await dialerStream.closeQuicStream()
    dialerStream = nil
    await listenerIncomingStream.closeQuicStream()
    listenerIncomingStream = nil
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
    if not listenerIncomingStream.isNil:
      await noCancel listenerIncomingStream.closeQuicStream()
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
      "candidates": listenerCandidates,
      "reuseStream": true
    })
    let listenerAck = await readJsonMessageWithTimeout(
      listenerRegStream,
      QuicRelayRpcTimeout,
      "listener register ack"
    )
    if not listenerAck{"ok"}.getBool():
      result.error = jsonString(listenerAck, "error")
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
      "route": route
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
      "route": route
    })

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
    result.dialerCandidates = jsonStrings(incoming, "candidates")
    await writeJsonMessage(listenerAcceptStream, %*{
      "ok": true,
      "op": "ready",
      "candidates": listenerCandidates
    })

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
  except CancelledError as exc:
    result.error = exc.msg
    raise exc
  except CatchableError as exc:
    result.error = exc.msg
  finally:
    if not dialerStream.isNil:
      await noCancel dialerStream.closeQuicStream()
    if not listenerAcceptStream.isNil:
      await noCancel listenerAcceptStream.closeQuicStream()
    if not listenerRegStream.isNil:
      await noCancel listenerRegStream.closeQuicStream()
    closeQuicRelayClient(dialerClient)
    closeQuicRelayClient(listenerClient)

proc listenerLoop(
    ownerId: int,
    routeId: int,
    startupDelay: Duration,
    relayUrl: string,
    route: string,
    rawLocal: string
): Future[void] {.async: (raises: [CancelledError]).} =
  quicRelayTrace("listener task start routeId=" & $routeId)
  try:
    if startupDelay > ZeroDuration:
      quicRelayTrace(
        "listener startup delay routeId=" & $routeId &
        " delayMs=" & $startupDelay.milliseconds
      )
      await sleepAsync(startupDelay)
    while true:
      let endpoint = quicRelayEndpoint(relayUrl).valueOr:
        quicRelayTrace("listener endpoint parse failed routeId=" & $routeId & " err=" & error)
        await sleepAsync(1.seconds)
        continue
      let client = connectRelay(endpoint).valueOr:
        quicRelayTrace("listener relay connect failed routeId=" & $routeId & " err=" & error)
        await sleepAsync(1.seconds)
        continue
      var regStream: MsQuicStream = nil
      block setup:
        regStream = createBidiStream(client.handle, client.connPtr, client.connState).valueOr:
          quicRelayTrace("listener stream create failed routeId=" & $routeId & " err=" & error)
          break setup
        let hello = %*{
          "version": 1,
          "mode": "listen",
          "route": route,
          "candidates": []
        }
        try:
          await writeJsonMessage(regStream, hello)
          quicRelayTrace(
            "listener registration sent routeId=" & $routeId &
            " route=" & route &
            " mode=ack_first"
          )
        except CatchableError:
          break setup
        let confirmed =
          try:
            await confirmRelayListenRegistration(regStream, endpoint, route)
          except CatchableError as exc:
            Result[void, string].err("nim_quic relay listen ack raised: " & exc.msg)
        if confirmed.isOk():
          quicRelayTrace(
            "listener registration acked routeId=" & $routeId &
            " route=" & route
          )
        else:
          quicRelayTrace(
            "listener registration ack failed routeId=" & $routeId &
            " route=" & route &
            " err=" & confirmed.error &
            " fallback=route_status"
          )
        if confirmed.isErr():
          let published = ensureRelayRoutePublished(endpoint, route)
          if published.isErr():
            quicRelayTrace(
              "listener publication confirm failed routeId=" & $routeId &
              " route=" & route &
              " err=" & published.error
            )
            break setup
        markRelayRouteReady(ownerId, route)
        quicRelayTrace(
          "listener published routeId=" & $routeId &
          " route=" & route
        )
        await regStream.closeQuicStream()
        regStream = nil

        var restart = false
        while true:
          let controlStream =
            try:
              await awaitIncomingBidiStream(client.handle, client.connState)
            except CatchableError as exc:
              quicRelayTrace(
                "listener incoming stream accept failed routeId=" & $routeId &
                " route=" & route &
                " err=" & exc.msg
              )
              restart = true
              break
          let incoming =
            try:
              await readJsonMessage(controlStream)
            except CatchableError as exc:
              quicRelayTrace(
                "listener incoming payload failed routeId=" & $routeId &
                " route=" & route &
                " err=" & exc.msg
              )
              await controlStream.closeQuicStream()
              restart = true
              break
          quicRelayTrace(
            "listener incoming stream accepted routeId=" & $routeId &
            " route=" & route
          )
          if incoming.isNil or incoming.kind != JObject:
            await controlStream.closeQuicStream()
            restart = true
            break
          if jsonString(incoming, "op") != "incoming":
            await controlStream.closeQuicStream()
            continue
          quicRelayTrace("listener incoming routeId=" & $routeId & " route=" & route)
          let sourceAdvertised = jsonString(incoming, "source")
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
          try:
            await writeJsonMessage(controlStream, %*{
              "ok": true,
              "op": "ready",
              "candidates": []
            })
            quicRelayTrace("listener ready routeId=" & $routeId & " route=" & route)
          except CatchableError:
            await localTransport.safeCloseTransport()
            await controlStream.closeQuicStream()
            restart = true
            break
          controlStream.restoreCachedBytes(@[])
          let bridgeStream =
            try:
              await awaitIncomingBidiStream(client.handle, client.connState)
            except CatchableError as exc:
              quicRelayTrace(
                "listener bridge stream accept failed routeId=" & $routeId &
                " route=" & route &
                " err=" & exc.msg
              )
              await localTransport.safeCloseTransport()
              restart = true
              break
          let bridgeOp =
            try:
              await readJsonMessage(bridgeStream)
            except CatchableError as exc:
              quicRelayTrace(
                "listener bridge intro failed routeId=" & $routeId &
                " route=" & route &
                " err=" & exc.msg
              )
              await localTransport.safeCloseTransport()
              await bridgeStream.closeQuicStream()
              restart = true
              break
          if bridgeOp.isNil or bridgeOp.kind != JObject or jsonString(bridgeOp, "op") != "bridge":
            quicRelayTrace(
              "listener bridge intro rejected routeId=" & $routeId &
              " route=" & route
            )
            await localTransport.safeCloseTransport()
            await bridgeStream.closeQuicStream()
            restart = true
            break
          bridgeStream.restoreCachedBytes(@[])
          await controlStream.finishQuicStream(QuicRelayGatewayResponseDrainDelay)
          quicRelayTrace("listener bridge start routeId=" & $routeId & " route=" & route)
          await bridgeLocalAndQuic(localTransport, bridgeStream)
          quicRelayTrace("listener bridge end routeId=" & $routeId & " route=" & route)
          restart = true
          break
        if not restart:
          break
      await regStream.closeQuicStream()
      closeQuicRelayClient(client)
      await sleepAsync(200.milliseconds)
  except CancelledError:
    discard
  finally:
    quicRelayTrace("listener task stop routeId=" & $routeId)

proc startRelayListener*(
    ownerId: int,
    relayUrl: string,
    advertised: MultiAddress,
    rawLocal: MultiAddress
): Result[void, string] =
  if ownerId <= 0:
    return err("invalid tsnet relay owner id")
  discard quicRelayEndpoint(relayUrl).valueOr:
    return err(error)
  let route = $advertised
  let startupDelay = RelayListenerStartupStagger * relayOwnerTaskCount(ownerId)
  let task = listenerLoop(
    ownerId,
    relayNextRouteId + 1,
    startupDelay,
    relayUrl,
    route,
    $rawLocal
  )
  let handle = relayRegisterTask(ownerId, nil, nil, task)
  asyncSpawn task
  quicRelayTrace("listener scheduled routeId=" & $handle.routeId & " route=" & route)
  ok()

proc udpListenerLoop(
    ownerId: int,
    routeId: int,
    startupDelay: Duration,
    relayUrl: string,
    route: string,
    rawLocal: string,
    bridgeExtraJson: string
): Future[void] {.async: (raises: [CancelledError]).} =
  quicRelayTrace("udp listener task start routeId=" & $routeId)
  let listenerCandidates = relayCandidatesForRaw(rawLocal, bridgeExtraJson)
  try:
    if startupDelay > ZeroDuration:
      quicRelayTrace(
        "udp listener startup delay routeId=" & $routeId &
        " delayMs=" & $startupDelay.milliseconds
      )
      await sleepAsync(startupDelay)
    while true:
      let endpoint = quicRelayEndpoint(relayUrl).valueOr:
        quicRelayTrace("udp listener endpoint parse failed routeId=" & $routeId & " err=" & error)
        await sleepAsync(1.seconds)
        continue
      let client = connectRelay(endpoint).valueOr:
        quicRelayTrace("udp listener relay connect failed routeId=" & $routeId & " err=" & error)
        await sleepAsync(1.seconds)
        continue
      # On WAN we prefer a client-initiated accept stream. The listener keeps
      # that bidi stream attached, explicitly sends `await`, and only then
      # does the gateway answer with `incoming` on the same stream. This
      # avoids the least stable path we have seen in practice: a delayed
      # server-push onto a previously attached stream without a fresh
      # request/response turn.
      let preferReuseStream = not isLoopbackRelayHost(endpoint.host)
      var regStream: MsQuicStream = nil
      block setup:
        regStream = createBidiStream(client.handle, client.connPtr, client.connState).valueOr:
          quicRelayTrace("udp listener stream create failed routeId=" & $routeId & " err=" & error)
          break setup
        quicRelayTrace("udp listener stream created routeId=" & $routeId & " route=" & route)
        let hello = %*{
          "version": 1,
          "mode": "listen",
          "kind": "udp",
          "route": route,
          "candidates": listenerCandidates,
          "reuseStream": preferReuseStream
        }
        try:
          await writeJsonMessage(regStream, hello)
          quicRelayTrace("udp listener hello sent routeId=" & $routeId & " route=" & route)
          quicRelayTrace(
            "udp listener registration sent routeId=" & $routeId &
            " route=" & route &
            " mode=ack_first"
          )
        except CatchableError as exc:
          quicRelayTrace(
            "udp listener hello exchange failed routeId=" & $routeId &
            " route=" & route &
            " err=" & exc.msg
          )
          break setup
        let confirmed =
          try:
            await confirmRelayListenRegistration(regStream, endpoint, route)
          except CatchableError as exc:
            Result[void, string].err("nim_quic relay listen ack raised: " & exc.msg)
        if confirmed.isOk():
          quicRelayTrace(
            "udp listener registration acked routeId=" & $routeId &
            " route=" & route
          )
        else:
          quicRelayTrace(
            "udp listener registration ack failed routeId=" & $routeId &
            " route=" & route &
            " err=" & confirmed.error &
            " fallback=route_status"
          )
        if confirmed.isErr():
          let published = ensureRelayRoutePublished(endpoint, route)
          if published.isErr():
            quicRelayTrace(
              "udp listener publication confirm failed routeId=" & $routeId &
              " route=" & route &
              " err=" & published.error
            )
            break setup
        markRelayRouteReady(ownerId, route)
        quicRelayTrace(
          "udp listener published routeId=" & $routeId &
          " route=" & route
        )
        var restart = false
        if not regStream.isNil:
          await regStream.closeQuicStream()
          regStream = nil
        while true:
          var acceptStream: MsQuicStream = nil
          if preferReuseStream:
            acceptStream = createBidiStream(client.handle, client.connPtr, client.connState).valueOr:
              quicRelayTrace(
                "udp listener accept stream create failed routeId=" & $routeId &
                " route=" & route &
                " err=" & error
              )
              restart = true
              break
            try:
              await writeJsonMessage(acceptStream, %*{
                "version": 1,
                "mode": "accept",
                "kind": "udp",
                "route": route
              })
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
                quicRelayTrace(
                  "udp listener accept stream ack rejected routeId=" & $routeId &
                  " route=" & route &
                  " err=" & jsonString(acceptAck, "error")
                )
                await acceptStream.closeQuicStream()
                restart = true
                break
              quicRelayTrace(
                "udp listener accept stream acked routeId=" & $routeId &
                " route=" & route
              )
              await writeJsonMessage(acceptStream, %*{
                "op": "await",
                "route": route
              })
              quicRelayTrace(
                "udp listener accept stream awaiting routeId=" & $routeId &
                " route=" & route
              )
            except CatchableError as exc:
              quicRelayTrace(
                "udp listener accept stream register failed routeId=" & $routeId &
                " route=" & route &
                " err=" & exc.msg
              )
              await acceptStream.closeQuicStream()
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
              if not acceptStream.isNil:
                # This stream is a long-lived accept poll. On slower peers,
                # especially Android, there can be a long warm-up gap between
                # the listener attaching and the first dial arriving. Refresh
                # the accept stream periodically so we don't sit forever on an
                # idle attached stream that has silently gone stale.
                await readJsonMessageWithTimeout(
                  controlStream,
                  QuicRelayAcceptRefreshTimeout,
                  "udp listener incoming reuse"
                )
              else:
                await readJsonMessageWithTimeout(
                  controlStream,
                  QuicRelayIncomingWaitTimeout,
                  "udp listener incoming payload"
                )
            except CatchableError as exc:
              if not acceptStream.isNil and
                  exc.msg.contains("timed out waiting for udp listener incoming reuse"):
                quicRelayTrace(
                  "udp listener accept stream refresh routeId=" & $routeId &
                  " route=" & route
                )
                await controlStream.closeQuicStream()
                continue
              quicRelayTrace(
                "udp listener incoming payload failed routeId=" & $routeId &
                " route=" & route &
                " err=" & exc.msg
              )
              await controlStream.closeQuicStream()
              restart = true
              break
          quicRelayTrace(
            "udp listener incoming stream accepted routeId=" & $routeId &
            " route=" & route
          )
          if incoming.isNil or incoming.kind != JObject:
            await controlStream.closeQuicStream()
            restart = true
            break
          if incoming.hasKey("ok") and not incoming{"ok"}.getBool(true):
            quicRelayTrace(
              "udp listener incoming rejected routeId=" & $routeId &
              " route=" & route &
              " err=" & jsonString(incoming, "error")
            )
            await controlStream.closeQuicStream()
            restart = true
            break
          if jsonString(incoming, "op") != "incoming":
            await controlStream.closeQuicStream()
            continue
          quicRelayTrace("udp listener incoming routeId=" & $routeId & " route=" & route)
          let sourceAdvertised = jsonString(incoming, "source")
          let dialerCandidates = jsonStrings(incoming, "candidates")
          let rawTarget = rawSocketFromAddress(rawLocal).valueOr:
            restart = true
            break
          let localTarget = relayLocalAddress(rawTarget[0], rawTarget[1]).valueOr:
            restart = true
            break
          let state = TsnetUdpRelayBridge(
            ownerId: ownerId,
            family: rawTarget[2],
            target: localTarget,
            stream: nil,
            rawKey: "",
            clientRemote: none(TransportAddress),
            pendingFrames: @[],
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

            if rawTarget[2] == "ip6":
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
                  rawSocketAddress(localUdp.localAddress(), rawTarget[2], TsnetProxyKind.Quic)
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

          try:
            await writeJsonMessage(controlStream, %*{
              "ok": true,
              "op": "ready",
              "candidates": listenerCandidates
            })
            quicRelayTrace("udp listener ready routeId=" & $routeId & " route=" & route)
          except CatchableError:
            await localUdp.closeDatagramTransport()
            await controlStream.closeQuicStream()
            restart = true
            break
          state.stream = controlStream

          quicRelayTrace("udp listener bridge start routeId=" & $routeId & " route=" & route)
          await bridgeUdpAndQuic(localUdp, state, localTarget, false)
          quicRelayTrace("udp listener bridge end routeId=" & $routeId & " route=" & route)
          restart = true
          break
        if not restart:
          break
      if not regStream.isNil:
        await regStream.closeQuicStream()
      closeQuicRelayClient(client)
      await sleepAsync(200.milliseconds)
  except CancelledError:
    discard
  finally:
    quicRelayTrace("udp listener task stop routeId=" & $routeId)

proc startUdpRelayListener*(
    ownerId: int,
    relayUrl: string,
    advertised: MultiAddress,
    rawLocal: MultiAddress,
    bridgeExtraJson = ""
): Result[void, string] =
  if ownerId <= 0:
    return err("invalid tsnet relay owner id")
  discard quicRelayEndpoint(relayUrl).valueOr:
    return err(error)
  let route = $advertised
  let startupDelay = RelayListenerStartupStagger * relayOwnerTaskCount(ownerId)
  let task = udpListenerLoop(
    ownerId,
    relayNextRouteId + 1,
    startupDelay,
    relayUrl,
    route,
    $rawLocal,
    bridgeExtraJson
  )
  let handle = relayRegisterTask(ownerId, nil, nil, task)
  asyncSpawn task
  quicRelayTrace("udp listener scheduled routeId=" & $handle.routeId & " route=" & route)
  ok()

proc dialRelayBridge(
    routeId: int,
    relayUrl: string,
    route: string,
    source: string,
    localClient: StreamTransport
): Future[void] {.async: (raises: [CancelledError]).} =
  try:
    quicRelayTrace("dial bridge start routeId=" & $routeId & " route=" & route)
    let endpoint = quicRelayEndpoint(relayUrl).valueOr:
      await localClient.safeCloseTransport()
      return
    let startedAt = Moment.now()
    while true:
      let client = connectRelay(endpoint).valueOr:
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
          await writeJsonMessage(stream, %*{
            "version": 1,
            "mode": "dial",
            "route": route,
            "source": source,
            "candidates": []
          })
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
      closeQuicRelayClient(client)
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
  discard quicRelayEndpoint(relayUrl).valueOr:
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
  let handle = relayRegisterTask(ownerId, nil, nil, completion)
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
        await dialRelayBridge(handle.routeId, relayUrl, route, adjustedSource, localClient)
      except CancelledError:
        discard
      finally:
        if not completion.finished():
          completion.complete()

    server = createStreamServer(bindAddress, serveDialClient, flags = {ReuseAddr})
    server.start()
  except CatchableError as exc:
    return err("failed to start tsnet relay dial server: " & exc.msg)
  handle.server = server
  let rawAddress = MultiAddress.init(server.sock.getLocalAddress()).valueOr:
    try:
      server.stop()
      server.close()
    except CatchableError:
      discard
    return err("failed to build tsnet relay dial proxy address: " & error)
  quicRelayTrace("dial server scheduled routeId=" & $handle.routeId & " raw=" & $rawAddress)
  ok(rawAddress)

proc dialUdpBridge(
    routeId: int,
    relayUrl: string,
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
    let endpoint = quicRelayEndpoint(relayUrl).valueOr:
      quicRelayTrace("udp dial bridge endpoint parse failed routeId=" & $routeId & " err=" & error)
      markUdpDialFailed(state.ownerId, state.rawKey, "udp dial bridge endpoint parse failed: " & error)
      await proxy.closeDatagramTransport()
      return
    let startedAt = Moment.now()
    while true:
      inc attempt
      markUdpDialProgress(
        state.ownerId,
        state.rawKey,
        "connect",
        endpoint.url,
        attempt
      )
      let client = connectRelay(endpoint).valueOr:
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
          await writeJsonMessage(stream, %*{
            "version": 1,
            "mode": "dial",
            "kind": "udp",
            "route": route,
            "source": source,
            "candidates": dialerCandidates
          })
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
          stream.restoreCachedBytes(@[])
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
      closeQuicRelayClient(client)
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
  discard quicRelayEndpoint(relayUrl).valueOr:
    return err(error)
  if family notin ["ip4", "ip6"]:
    return err("unsupported tsnet relay family " & family)
  if localTailnetIp.strip().len == 0:
    return err("missing local tailnet IP for tsnet relay UDP dial proxy")
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
  let route = $remoteAdvertised
  let rawKey = $rawAddress
  let dialerCandidates = relayCandidatesForRaw($rawAddress, bridgeExtraJson)
  state.rawKey = rawKey
  # The loopback UDP proxy is bound, but the relay stream is not usable until
  # the dial bridge has received an ACK and attached the QUIC stream.
  markUdpDialPending(ownerId, rawKey)
  let bridgeTask = dialUdpBridge(
    relayNextRouteId + 1,
    relayUrl,
    route,
    source,
    proxy,
    state,
    dialerCandidates
  )
  let handle = relayRegisterTask(ownerId, nil, proxy, bridgeTask)
  asyncSpawn bridgeTask
  quicRelayTrace("udp dial proxy scheduled routeId=" & $handle.routeId & " raw=" & $rawAddress)
  ok(rawAddress)
