{.push raises: [].}

import std/[json, options, os, strutils]

import ../../multiaddress
import ../../utility
import ../tsnetprovidertypes
import ./control
when defined(libp2p_msquic_experimental):
  import ./h3control
  import ./quiccontrol
  import ./quicrelay as qrelay
import ./oracle
import ./proxy
import ./session
import ./state
import ./tcprelay as trelay
import ./tcpcontrol

type
  TsnetInAppRuntimeStatus* {.pure.} = enum
    Stopped
    Bootstrapping
    Running
    Failed

  TsnetBoundControlContext = ref object
    transport: TsnetControlTransport
    endpoint: string

  TsnetInAppRuntime* = ref object
    cfg*: TsnetProviderConfig
    status*: TsnetInAppRuntimeStatus
    state*: TsnetStoredState
    control*: TsnetControlSnapshot
    session*: TsnetSessionSnapshot
    lastError*: string
    runtimeId*: int
    controlTransport*: TsnetControlTransport
    controlProtocolRequested*: string
    controlProtocolSelected*: string
    controlEndpoint*: string
    controlFallback*: string
    tcpListenerRoutes*: seq[string]
    udpListenerRoutes*: seq[string]

const
  TsnetInAppRuntimeNotImplementedError* =
    TsnetMissingInAppProviderError

var runtimeIdCounter {.global.} = 0

proc safeTrace(message: string) =
  try:
    stderr.writeLine(message)
  except IOError:
    discard

proc nextRuntimeId(): int =
  inc runtimeIdCounter
  runtimeIdCounter

proc oracleEnabled(runtime: TsnetInAppRuntime): bool {.gcsafe.} =
  not runtime.isNil and runtime.cfg.enableDebug

proc baseControlUrl(runtime: TsnetInAppRuntime): string {.gcsafe.}
proc selectedOrRequestedControlProtocol(runtime: TsnetInAppRuntime): string {.gcsafe.}
proc resolvedControlDialUrl(runtime: TsnetInAppRuntime, protocol: string): string {.gcsafe.}

proc liveControlRequested(runtime: TsnetInAppRuntime): bool {.gcsafe.} =
  not runtime.isNil and runtime.baseControlUrl().len > 0

proc runtimeUnavailableError*(runtime: TsnetInAppRuntime): string {.gcsafe.} =
  if runtime.isNil:
    return TsnetInAppRuntimeNotImplementedError
  if runtime.lastError.strip().len > 0:
    return runtime.lastError.strip()
  let controlUrl = runtime.baseControlUrl()
  let controlKey = runtime.state.controlPublicKey.strip()
  if controlUrl.len == 0:
    return TsnetInAppRuntimeNotImplementedError
  if controlKey.len == 0:
    return
      "self-hosted tsnet control was requested at " & controlUrl &
      ", but the selected Nim in-app control client has not completed register/map"
  "self-hosted tsnet control was detected at " & controlUrl &
    " (" & controlKey & "), but the selected Nim in-app control client has not completed register/map"

proc requestedControlProtocol*(runtime: TsnetInAppRuntime): string {.gcsafe.} =
  if runtime.isNil:
    return TsnetControlProtocolAuto
  normalizeControlProtocol(runtime.cfg.controlProtocol)

proc selectedControlProtocol*(runtime: TsnetInAppRuntime): string {.gcsafe.} =
  if runtime.isNil:
    return ""
  runtime.controlProtocolSelected

proc configuredControlEndpoint(runtime: TsnetInAppRuntime): string {.gcsafe.} =
  if runtime.isNil:
    return ""
  runtime.cfg.controlEndpoint.strip()

proc configuredRelayEndpoint(runtime: TsnetInAppRuntime): string {.gcsafe.} =
  if runtime.isNil:
    return ""
  runtime.cfg.relayEndpoint.strip()

proc effectiveControlUrl(runtime: TsnetInAppRuntime): string {.gcsafe.} =
  if runtime.isNil:
    return ""
  let persisted = runtime.state.controlUrl.strip()
  if persisted.len > 0:
    return persisted
  let configured = runtime.cfg.controlUrl.strip()
  if configured.len > 0:
    return configured
  let selected = runtime.selectedOrRequestedControlProtocol()
  runtime.resolvedControlDialUrl(selected).strip()

proc baseControlUrl(runtime: TsnetInAppRuntime): string {.gcsafe.} =
  runtime.effectiveControlUrl()

proc selectedOrRequestedControlProtocol(runtime: TsnetInAppRuntime): string {.gcsafe.} =
  if runtime.isNil:
    return TsnetControlProtocolAuto
  let selected = runtime.controlProtocolSelected.strip()
  if selected.len > 0:
    return normalizeControlProtocol(selected)
  runtime.requestedControlProtocol()

proc clearListenerRoutes(runtime: TsnetInAppRuntime) {.gcsafe.} =
  if runtime.isNil:
    return
  runtime.tcpListenerRoutes.setLen(0)
  runtime.udpListenerRoutes.setLen(0)

proc rememberListenerRoute(routes: var seq[string], route: string) {.gcsafe.} =
  if route.len == 0:
    return
  if route notin routes:
    routes.add(route)

proc requestString(node: JsonNode, key: string): string
proc liveQuicRelayMode(runtime: TsnetInAppRuntime): bool {.gcsafe.}
proc resolvedRelayDialUrl(runtime: TsnetInAppRuntime): string {.gcsafe.}

proc proxyListenerReadiness(runtime: TsnetInAppRuntime): tuple[expected, ready: int] {.gcsafe.} =
  if runtime.isNil:
    return (0, 0)
  result.expected = runtime.tcpListenerRoutes.len + runtime.udpListenerRoutes.len
  when defined(libp2p_msquic_experimental):
    if runtime.liveQuicRelayMode():
      for route in runtime.tcpListenerRoutes:
        if qrelay.relayRouteReady(runtime.runtimeId, route):
          inc result.ready
      for route in runtime.udpListenerRoutes:
        if qrelay.relayRouteReady(runtime.runtimeId, route):
          inc result.ready
      return
  result.ready = runtime.tcpListenerRoutes.len + runtime.udpListenerRoutes.len

proc listenerNeedsRepair*(runtime: TsnetInAppRuntime): bool {.gcsafe.} =
  proc fieldString(node: JsonNode; key: string): string {.gcsafe.} =
    if node.isNil or node.kind != JObject or not node.hasKey(key):
      return ""
    let value = node.getOrDefault(key)
    if value.kind == JString:
      return value.getStr()
    ""

  if runtime.isNil or runtime.status != TsnetInAppRuntimeStatus.Running:
    return false
  let readiness = runtime.proxyListenerReadiness()
  if readiness.expected == 0 or readiness.ready >= readiness.expected:
    return false
  when defined(libp2p_msquic_experimental):
    if runtime.liveQuicRelayMode():
      let relayEndpoint = runtime.resolvedRelayDialUrl()
      let listeners = qrelay.relayListenerStatesPayload(runtime.runtimeId)
      if listeners.kind != JArray:
        return false
      for item in listeners.items:
        if item.kind != JObject:
          continue
        let route = fieldString(item, "route")
        let stage = fieldString(item, "stage").toLowerAscii()
        if stage in ["failed", "dropped", "stopped"]:
          return true
        if route.len > 0 and stage in ["awaiting", "incoming", "ready"] and
            not qrelay.relayRouteReadyOrPublished(
              runtime.runtimeId,
              relayEndpoint,
              route
            ):
          return true
  false

proc resolvedControlDialUrl(runtime: TsnetInAppRuntime, protocol: string): string {.gcsafe.} =
  if runtime.isNil:
    return ""
  let normalized = normalizeControlProtocol(protocol)
  let override = runtime.configuredControlEndpoint()
  if override.len > 0 and normalized in [
    TsnetControlProtocolNimQuic,
    TsnetControlProtocolNimTcp,
    TsnetControlProtocolNimH3
  ]:
    return override
  runtime.state.controlUrl.strip()

proc resolvedRelayDialUrl(runtime: TsnetInAppRuntime): string {.gcsafe.} =
  if runtime.isNil:
    return ""
  let relayOverride = runtime.configuredRelayEndpoint()
  if relayOverride.len > 0:
    return relayOverride
  let baseControl = runtime.baseControlUrl()
  when defined(libp2p_msquic_experimental):
    let derived = qrelay.nimQuicRelayBaseUrl(baseControl)
    if derived.len > 0:
      return derived
  ""

proc boundNoiseUrl(context: TsnetBoundControlContext): string {.gcsafe, raises: [].} =
  {.cast(gcsafe).}:
    if context.transport.isNil:
      ""
    elif not context.transport.noiseUrlProc.isNil:
      context.transport.noiseUrlProc(context.endpoint)
    else:
      controlNoiseUrl(context.endpoint)

proc boundKeyUrl(
    context: TsnetBoundControlContext,
    capabilityVersion: int
): string {.gcsafe, raises: [].} =
  {.cast(gcsafe).}:
    if context.transport.isNil:
      ""
    elif not context.transport.keyUrlProc.isNil:
      context.transport.keyUrlProc(context.endpoint, capabilityVersion)
    else:
      controlKeyUrl(context.endpoint, capabilityVersion)

proc boundRegisterUrl(context: TsnetBoundControlContext): string {.gcsafe, raises: [].} =
  {.cast(gcsafe).}:
    if context.transport.isNil:
      ""
    elif not context.transport.registerUrlProc.isNil:
      context.transport.registerUrlProc(context.endpoint)
    else:
      controlRegisterUrl(context.endpoint)

proc boundMapUrl(context: TsnetBoundControlContext): string {.gcsafe, raises: [].} =
  {.cast(gcsafe).}:
    if context.transport.isNil:
      ""
    elif not context.transport.mapUrlProc.isNil:
      context.transport.mapUrlProc(context.endpoint)
    else:
      controlMapUrl(context.endpoint)

proc boundFetchServerKey(
    context: TsnetBoundControlContext
): Result[TsnetControlServerKey, string] {.gcsafe, raises: [].} =
  {.cast(gcsafe).}:
    safeTrace(
      "[tsnet-runtime] boundFetchServerKey endpoint=" & context.endpoint &
      " transportNil=" & $context.transport.isNil &
      " fetchNil=" &
      $(if context.transport.isNil: true else: context.transport.fetchServerKeyProc.isNil)
    )
    if context.transport.isNil or context.transport.fetchServerKeyProc.isNil:
      return err("bound tsnet control transport is missing fetchServerKey")
    context.transport.fetchServerKeyProc(context.endpoint)

proc boundRegisterNode(
    context: TsnetBoundControlContext,
    request: JsonNode
): Result[JsonNode, string] {.gcsafe, raises: [].} =
  {.cast(gcsafe).}:
    if context.transport.isNil or context.transport.registerNodeProc.isNil:
      return err("bound tsnet control transport is missing registerNode")
    let registered = context.transport.registerNodeProc(context.endpoint, request).valueOr:
      return err(error)
    ok(registered)

proc boundMapPoll(
    context: TsnetBoundControlContext,
    request: JsonNode
): Result[JsonNode, string] {.gcsafe, raises: [].} =
  {.cast(gcsafe).}:
    if context.transport.isNil or context.transport.mapPollProc.isNil:
      return err("bound tsnet control transport is missing mapPoll")
    let mapped = context.transport.mapPollProc(context.endpoint, request).valueOr:
      return err(error)
    ok(mapped)

proc boundControlTransport(
    transport: TsnetControlTransport,
    endpointOverride: string
): TsnetControlTransport =
  if transport.isNil:
    return nil
  let normalized = endpointOverride.strip()
  if normalized.len == 0:
    return transport
  let context = TsnetBoundControlContext(
    transport: transport,
    endpoint: normalized
  )
  TsnetControlTransport.init(
    protocolLabel = transport.protocolLabel(),
    noiseUrlProc =
      proc(_: string): string {.closure, gcsafe, raises: [].} =
        boundNoiseUrl(context),
    keyUrlProc =
      proc(_: string, capabilityVersion: int): string {.closure, gcsafe, raises: [].} =
        boundKeyUrl(context, capabilityVersion),
    registerUrlProc =
      proc(_: string): string {.closure, gcsafe, raises: [].} =
        boundRegisterUrl(context),
    mapUrlProc =
      proc(_: string): string {.closure, gcsafe, raises: [].} =
        boundMapUrl(context),
    fetchServerKeyProc =
      proc(_: string): Result[TsnetControlServerKey, string] {.closure, gcsafe, raises: [].} =
        boundFetchServerKey(context),
    registerNodeProc =
      proc(_: string, request: JsonNode): Result[JsonNode, string] {.closure, gcsafe, raises: [].} =
        boundRegisterNode(context, request),
    mapPollProc =
      proc(_: string, request: JsonNode): Result[JsonNode, string] {.closure, gcsafe, raises: [].} =
        boundMapPoll(context, request)
  )

proc new*(_: type[TsnetInAppRuntime], cfg: TsnetProviderConfig): TsnetInAppRuntime =
  TsnetInAppRuntime(
    cfg: cfg,
    status: TsnetInAppRuntimeStatus.Stopped,
    state: TsnetStoredState.init(hostname = cfg.hostname, controlUrl = cfg.controlUrl),
    control: TsnetControlSnapshot.init(),
    session: TsnetSessionSnapshot.init(),
    lastError: "",
    runtimeId: nextRuntimeId(),
    controlTransport: nil,
    controlProtocolRequested: normalizeControlProtocol(cfg.controlProtocol),
    controlProtocolSelected: "",
    controlEndpoint: "",
    controlFallback: "",
    tcpListenerRoutes: @[],
    udpListenerRoutes: @[]
  )

proc new*(
    _: type[TsnetInAppRuntime],
    cfg: TsnetProviderConfig,
    controlTransport: TsnetControlTransport
): TsnetInAppRuntime =
  result = TsnetInAppRuntime.new(cfg)
  result.controlTransport = controlTransport

proc ready*(runtime: TsnetInAppRuntime): bool {.gcsafe.} =
  not runtime.isNil and runtime.status == TsnetInAppRuntimeStatus.Running

proc localProxyRegistryMode(runtime: TsnetInAppRuntime): bool {.gcsafe.} =
  not runtime.isNil and (
    (runtime.oracleEnabled() and oracle.oraclePath(runtime.cfg.stateDir).len > 0 and fileExists(oracle.oraclePath(runtime.cfg.stateDir))) or
    not runtime.controlTransport.isNil
  )

proc liveTcpRelayMode(runtime: TsnetInAppRuntime): bool {.gcsafe.} =
  false

proc liveQuicRelayMode(runtime: TsnetInAppRuntime): bool {.gcsafe.} =
  when defined(libp2p_msquic_experimental):
    not runtime.isNil and
      runtime.liveControlRequested() and
      not runtime.localProxyRegistryMode() and
      runtime.resolvedRelayDialUrl().toLowerAscii().startsWith("quic://")
  else:
    false

proc capabilities*(runtime: TsnetInAppRuntime): TsnetProviderCapabilities {.gcsafe.} =
  let ready = runtime.ready()
  let surfacesAvailable =
    ready or runtime.control.available() or runtime.session.status.backendState.len > 0
  let tcpProxyBacked =
    ready and runtime.session.supportsProxyRouting() and
      (runtime.localProxyRegistryMode() or runtime.liveQuicRelayMode())
  let udpProxyBacked =
    ready and runtime.session.supportsProxyRouting() and
      (runtime.localProxyRegistryMode() or runtime.liveQuicRelayMode())
  let proxyBacked = tcpProxyBacked or udpProxyBacked
  TsnetProviderCapabilities(
    inApp: true,
    realTailnet: ready,
    proxyBacked: proxyBacked,
    tcpProxy: tcpProxyBacked,
    udpProxy: udpProxyBacked,
    remoteResolution: tcpProxyBacked or udpProxyBacked,
    statusApi: surfacesAvailable,
    pingApi: surfacesAvailable,
    derpMapApi: surfacesAvailable
  )

proc statePath*(runtime: TsnetInAppRuntime): string =
  if runtime.isNil:
    return ""
  state.statePath(runtime.cfg.stateDir)

proc oraclePath*(runtime: TsnetInAppRuntime): string =
  if runtime.isNil:
    return ""
  oracle.oraclePath(runtime.cfg.stateDir)

proc loadPersistedState*(runtime: TsnetInAppRuntime): Result[TsnetStoredState, string] =
  if runtime.isNil:
    return err("tsnet in-app runtime is nil")
  let loaded =
    state.loadStoredState(
      runtime.cfg.stateDir,
      hostname = runtime.cfg.hostname,
      controlUrl = runtime.cfg.controlUrl
    ).valueOr:
      runtime.status = TsnetInAppRuntimeStatus.Failed
      runtime.lastError = error
      return err(error)
  runtime.state = loaded
  ok(runtime.state)

proc ensureStateMaterial*(runtime: TsnetInAppRuntime): Result[bool, string] =
  if runtime.isNil:
    return err("tsnet in-app runtime is nil")
  var changed = false
  let generated = state.ensureIdentityKeys(runtime.state).valueOr:
    runtime.status = TsnetInAppRuntimeStatus.Failed
    runtime.lastError = error
    return err(error)
  changed = changed or generated
  let requestedHostname = runtime.cfg.hostname.strip()
  if requestedHostname.len > 0 and runtime.state.hostname != requestedHostname:
    runtime.state.hostname = requestedHostname
    changed = true
  let requestedControlUrl = runtime.cfg.controlUrl.strip()
  if requestedControlUrl.len > 0 and runtime.state.controlUrl != requestedControlUrl:
    runtime.state.controlUrl = requestedControlUrl
    changed = true
  if changed and runtime.statePath().len > 0:
    discard state.storeStoredState(runtime.cfg.stateDir, runtime.state).valueOr:
      runtime.status = TsnetInAppRuntimeStatus.Failed
      runtime.lastError = error
      return err(error)
  ok(changed)

proc persistState*(runtime: TsnetInAppRuntime): Result[string, string] =
  if runtime.isNil:
    return err("tsnet in-app runtime is nil")
  let written = state.storeStoredState(runtime.cfg.stateDir, runtime.state).valueOr:
    runtime.status = TsnetInAppRuntimeStatus.Failed
    runtime.lastError = error
    return err(error)
  ok(written)

proc mergeFixtureSession(
    runtime: TsnetInAppRuntime,
    fixture: TsnetOracleFixture
): Result[void, string] =
  let status = parseStatusSnapshot(fixture.status).valueOr:
    return err(error)
  let derpMap = parseDerpMapSnapshot(fixture.derpMap).valueOr:
    return err(error)
  let ping = parsePingSnapshot(fixture.ping).valueOr:
    return err(error)
  runtime.control = TsnetControlSnapshot.init(
    source = fixture.source,
    capturedAtUnixMilli = fixture.capturedAtUnixMilli,
    registerPayload = fixture.controlRegister,
    mapPollPayload = fixture.controlMapPoll
  )
  runtime.session = TsnetSessionSnapshot.init(
    source = fixture.source,
    capturedAtUnixMilli = fixture.capturedAtUnixMilli,
    status = status,
    derpMap = derpMap,
    ping = ping
  )
  if status.hostname.len > 0:
    runtime.state.hostname = status.hostname
  if status.controlUrl.len > 0:
    runtime.state.controlUrl = status.controlUrl
  runtime.state.tailnetIPs = status.tailnetIPs
  runtime.state.homeDerp = runtime.session.homeDerpLabel()
  runtime.state.peerCache = runtime.session.peerCache()
  runtime.state.lastControlBootstrapError = ""
  discard runtime.persistState().valueOr:
    return err(error)
  ok()

proc refreshBootstrapMaterial(runtime: TsnetInAppRuntime) =
  if runtime.isNil:
    return
  discard runtime.ensureStateMaterial()

proc bridgeExtraString(node: JsonNode, keys: openArray[string]): string =
  if node.isNil or node.kind != JObject:
    return ""
  for key in keys:
    if node.hasKey(key):
      let value = node.getOrDefault(key)
      if value.kind == JString:
        let text = value.getStr().strip()
        if text.len > 0:
          return text
  ""

proc bridgeExtraStrings(node: JsonNode, keys: openArray[string]): seq[string] =
  if node.isNil or node.kind != JObject:
    return @[]
  for key in keys:
    if not node.hasKey(key):
      continue
    let value = node.getOrDefault(key)
    if value.kind != JArray:
      continue
    for item in value.items():
      if item.kind == JString:
        let text = item.getStr().strip()
        if text.len > 0 and text notin result:
          result.add(text)
    if result.len > 0:
      return result

proc bridgeLocalMetadata(runtime: TsnetInAppRuntime): tuple[peerId: string, listenAddrs: seq[string]] =
  if runtime.isNil:
    return ("", @[])
  let raw = runtime.cfg.bridgeExtraJson.strip()
  if raw.len == 0:
    return ("", @[])
  let payload = try:
      parseJson(raw)
    except CatchableError:
      return ("", @[])
  (
    bridgeExtraString(payload, ["libp2pPeerId", "localPeerId", "peerId"]),
    bridgeExtraStrings(payload, ["libp2pListenAddrs", "listenAddresses", "listenAddrs"])
  )

proc bootstrapInput(runtime: TsnetInAppRuntime): TsnetControlBootstrapInput =
  runtime.refreshBootstrapMaterial()
  let metadata = runtime.bridgeLocalMetadata()
  TsnetControlBootstrapInput(
    controlUrl: runtime.baseControlUrl(),
    hostname: runtime.state.hostname,
    authKey: runtime.cfg.authKey,
    wireguardPort: runtime.cfg.wireguardPort,
    machinePrivateKey: runtime.state.machineKey,
    machineKeyPresent: runtime.state.machineKey.strip().len > 0,
    machinePublicKey: runtime.state.machinePublicKey,
    nodeKeyPresent: runtime.state.nodeKey.strip().len > 0,
    nodePublicKey: runtime.state.nodePublicKey,
    wgKeyPresent: runtime.state.wgKey.strip().len > 0,
    discoPublicKey: runtime.state.discoPublicKey,
    persistedNodeIdPresent: runtime.state.nodeId.strip().len > 0,
    libp2pPeerId: metadata.peerId,
    libp2pListenAddrs: metadata.listenAddrs
  )

proc buildControlProbePayload(
    runtime: TsnetInAppRuntime,
    probed: TsnetControlServerKey
): JsonNode =
  control.buildControlProbePayload(runtime.bootstrapInput(), probed)

proc buildRegisterBootstrapPayload*(
    runtime: TsnetInAppRuntime,
    probed: TsnetControlServerKey
): JsonNode =
  control.buildRegisterBootstrapPayload(runtime.bootstrapInput(), probed)

proc buildMapBootstrapPayload*(
    runtime: TsnetInAppRuntime,
    probed: TsnetControlServerKey
): JsonNode =
  control.buildMapBootstrapPayload(runtime.bootstrapInput(), probed)

proc buildUnavailableSession*(
    runtime: TsnetInAppRuntime,
    backendState: string,
    errorCode: string
): TsnetSessionSnapshot =
  let statusError =
    if runtime.lastError.strip().len > 0:
      runtime.lastError.strip()
    else:
      errorCode
  let authUrl =
    if errorCode == "auth_required":
      runtime.state.controlUrl
    else:
      ""
  let derpSummary = runtime.state.homeDerp
  let tailnetRelay =
    if derpSummary.contains("/"):
      derpSummary.split("/", maxsplit = 1)[1]
    else:
      derpSummary
  TsnetSessionSnapshot.init(
    source = "nim-tsnet-inapp-live",
    capturedAtUnixMilli = runtime.state.lastControlSuccessUnixMilli,
    status = TsnetStatusSnapshot(
      ok: false,
      error: statusError,
      started: false,
      backendState: backendState,
      authUrl: authUrl,
      hostname: runtime.state.hostname,
      controlUrl: runtime.state.controlUrl,
      wireguardPort: runtime.cfg.wireguardPort,
      tailnetIPs: runtime.state.tailnetIPs,
      tailnetPath: "",
      tailnetRelay: tailnetRelay,
      tailnetDerpMapSummary: derpSummary,
      tailnetPeers: @[]
    ),
    derpMap = TsnetDerpMapSnapshot(
      ok: false,
      error: statusError,
      omitDefaultRegions: false,
      regionCount: 0,
      regions: @[]
    ),
    ping = TsnetPingSnapshot(
      ok: false,
      error: statusError,
      peerIP: "",
      pingType: "TSMP",
      samples: @[],
      minLatencyMs: 0,
      maxLatencyMs: 0,
      avgLatencyMs: 0,
      elapsedMs: 0
    )
  )

proc setTailnetPathState(
    runtime: TsnetInAppRuntime,
    path: string,
    relayLabel = ""
) =
  if runtime.isNil:
    return
  let normalized = normalizeTailnetPath(path)
  runtime.session.status.tailnetPath = normalized
  if tailnetPathUsesRelay(normalized):
    runtime.session.status.tailnetRelay = relayLabel
  else:
    runtime.session.status.tailnetRelay = ""

proc relayLabelForPath(runtime: TsnetInAppRuntime): string =
  if runtime.isNil:
    return ""
  let current = runtime.session.status.tailnetRelay.strip()
  if current.len > 0:
    return current
  let summary = runtime.session.homeDerpLabel()
  if summary.contains("/"):
    return summary.split("/", maxsplit = 1)[1]
  summary

proc promoteDirectPath*(runtime: TsnetInAppRuntime, punched = false) =
  if runtime.isNil:
    return
  runtime.setTailnetPathState(
    if punched: TsnetPathPunchedDirect else: TsnetPathDirect
  )

proc demoteRelayPath*(runtime: TsnetInAppRuntime) =
  if runtime.isNil:
    return
  runtime.setTailnetPathState(TsnetPathRelay, runtime.relayLabelForPath())

proc preferredDirectSnapshot(runtime: TsnetInAppRuntime): Option[TsnetDirectRouteSnapshot] =
  if runtime.isNil:
    return
  let snapshots = directRouteSnapshots(runtime.runtimeId)
  if snapshots.len == 0:
    return
  var selected = 0
  for idx in 1..<snapshots.len:
    let candidatePath = normalizeTailnetPath(snapshots[idx].pathKind)
    let incumbentPath = normalizeTailnetPath(snapshots[selected].pathKind)
    if candidatePath != incumbentPath:
      if candidatePath == TsnetPathDirect:
        selected = idx
        continue
      if incumbentPath == TsnetPathDirect:
        continue
      if candidatePath == TsnetPathPunchedDirect and incumbentPath != TsnetPathPunchedDirect:
        selected = idx
        continue
    if snapshots[idx].hitCount > snapshots[selected].hitCount:
      selected = idx
      continue
    if snapshots[idx].lastSelectedUnixMilli > snapshots[selected].lastSelectedUnixMilli:
      selected = idx
  some(snapshots[selected])

proc appendTailnetPathDiagnostics(payload: JsonNode, runtime: TsnetInAppRuntime): JsonNode =
  result =
    if payload.isNil or payload.kind != JObject:
      newJObject()
    else:
      payload
  if runtime.isNil:
    return
  let normalizedPath = normalizeTailnetPath(requestString(result, "tailnetPath"))
  let relayLabel = runtime.relayLabelForPath()
  let directSnapshots = directRouteSnapshots(runtime.runtimeId)
  let preferredDirect = runtime.preferredDirectSnapshot()
  var pathEntries = newJArray()

  if normalizedPath == TsnetPathRelay:
    var relayEntry = newJObject()
    relayEntry["path"] = %TsnetPathRelay
    relayEntry["active"] = %true
    relayEntry["backup"] = %false
    if relayLabel.len > 0:
      relayEntry["relay"] = %relayLabel
    pathEntries.add(relayEntry)

  for snapshot in directSnapshots:
    var entry = newJObject()
    let snapshotPath = normalizeTailnetPath(snapshot.pathKind)
    let isPreferred =
      preferredDirect.isSome() and
      preferredDirect.get().raw == snapshot.raw and
      preferredDirect.get().advertised == snapshot.advertised
    let isActive =
      normalizedPath != TsnetPathRelay and
      snapshotPath == normalizedPath and
      isPreferred
    entry["path"] = %snapshotPath
    entry["active"] = %isActive
    entry["backup"] = %(not isActive)
    entry["advertised"] = %($snapshot.advertised)
    entry["raw"] = %($snapshot.raw)
    entry["hitCount"] = %snapshot.hitCount
    entry["failureCount"] = %snapshot.failureCount
    if snapshot.lastSelectedUnixMilli > 0:
      entry["lastSelectedUnixMilli"] = %snapshot.lastSelectedUnixMilli
    if snapshot.suspendedUntilUnixMilli > 0:
      entry["suspendedUntilUnixMilli"] = %snapshot.suspendedUntilUnixMilli
    pathEntries.add(entry)

  if normalizedPath in [TsnetPathDirect, TsnetPathPunchedDirect]:
    var relayEntry = newJObject()
    relayEntry["path"] = %TsnetPathRelay
    relayEntry["active"] = %false
    relayEntry["backup"] = %true
    if relayLabel.len > 0:
      relayEntry["relay"] = %relayLabel
    pathEntries.add(relayEntry)

  let pathCounts = directRoutePathCounts(runtime.runtimeId)
  result["tailnetPrimaryPath"] = %normalizedPath
  result["tailnetPaths"] = pathEntries
  result["tailnetMultipath"] = %(pathEntries.len > 1)
  result["tailnetDirectRouteCount"] = %pathCounts.directCount
  result["tailnetPunchedRouteCount"] = %pathCounts.punchedCount

proc appendControlDiagnostics(payload: JsonNode, runtime: TsnetInAppRuntime): JsonNode =
  result =
    if payload.isNil or payload.kind != JObject:
      newJObject()
    else:
      payload
  result["controlProtocolRequested"] = %runtime.requestedControlProtocol()
  result["controlProtocolSelected"] = %runtime.selectedControlProtocol()
  result["controlEndpoint"] = %runtime.controlEndpoint
  result["controlFallback"] = %runtime.controlFallback

proc protocolNeedsStdlibSsl(protocol: string): bool =
  normalizeControlProtocol(protocol) == TsnetControlProtocolTs2021H2

proc controlProtocolCandidates(requestedProtocol: string): seq[string] =
  if normalizeControlProtocol(requestedProtocol) != TsnetControlProtocolAuto:
    return @[normalizeControlProtocol(requestedProtocol)]
  @[TsnetControlProtocolNimQuic]

proc unsupportedControlTransport(
    protocolLabel: string,
    message: string
): TsnetControlTransport =
  TsnetControlTransport.init(
    protocolLabel = protocolLabel,
    fetchServerKeyProc =
      proc(controlUrl: string): Result[TsnetControlServerKey, string] {.closure, gcsafe, raises: [].} =
        err(message),
    registerNodeProc =
      proc(controlUrl: string, request: JsonNode): Result[JsonNode, string] {.closure, gcsafe, raises: [].} =
        err(message),
    mapPollProc =
      proc(controlUrl: string, request: JsonNode): Result[JsonNode, string] {.closure, gcsafe, raises: [].} =
        err(message)
  )

proc liveControlTransport(
    runtime: TsnetInAppRuntime,
    protocol: string
): tuple[transport: TsnetControlTransport, endpoint: string] =
  let dialUrl = runtime.resolvedControlDialUrl(protocol)
  case normalizeControlProtocol(protocol)
  of TsnetControlProtocolNimQuic:
    when defined(libp2p_msquic_experimental):
      result.transport = boundControlTransport(quicControlTransport(), dialUrl)
      result.endpoint = nimQuicBaseUrl(dialUrl)
    else:
      result.transport = unsupportedControlTransport(
        "nim_quic",
        "nim_quic control transport requires -d:libp2p_msquic_experimental"
      )
      result.endpoint = ""
  of TsnetControlProtocolNimTcp:
    result.transport = boundControlTransport(tcpControlTransport(), dialUrl)
    result.endpoint = nimTcpBaseUrl(dialUrl)
  of TsnetControlProtocolNimH3:
    when defined(libp2p_msquic_experimental):
      result.transport = boundControlTransport(TsnetControlTransport.init(
        protocolLabel = "nim_h3",
        noiseUrlProc =
          proc(controlUrl: string): string {.closure, gcsafe, raises: [].} =
            nimH3KeyUrl(controlUrl, TsnetDefaultControlCapabilityVersions[0]),
        keyUrlProc =
          proc(controlUrl: string, capabilityVersion: int): string {.closure, gcsafe, raises: [].} =
            nimH3KeyUrl(controlUrl, capabilityVersion),
        registerUrlProc =
          proc(controlUrl: string): string {.closure, gcsafe, raises: [].} =
            let base = nimH3BaseUrl(controlUrl)
            if base.len == 0: "" else: base & "/register",
        mapUrlProc =
          proc(controlUrl: string): string {.closure, gcsafe, raises: [].} =
            let base = nimH3BaseUrl(controlUrl)
            if base.len == 0: "" else: base & "/map",
        fetchServerKeyProc =
          proc(controlUrl: string): Result[TsnetControlServerKey, string] {.closure, gcsafe, raises: [].} =
            fetchControlServerKeyH3Safe(controlUrl),
        registerNodeProc = registerNodeH3Safe,
        mapPollProc = mapPollH3Safe
      ), dialUrl)
      result.endpoint = nimH3BaseUrl(dialUrl)
    else:
      result.transport = unsupportedControlTransport(
        "nim_h3",
        "nim_h3 control transport requires -d:libp2p_msquic_experimental"
      )
      result.endpoint = ""
  else:
    result.transport = defaultControlTransport()
    result.endpoint = normalizeControlUrl(dialUrl)

proc probeControlServer(runtime: TsnetInAppRuntime): Result[TsnetControlBootstrapResult, string] =
  if runtime.isNil:
    return err("tsnet in-app runtime is nil")
  if not runtime.liveControlRequested():
    return err("tsnet controlUrl is empty")
  when defined(libp2p_msquic_experimental):
    defer:
      closeAllQuicClients()
  let requestedProtocol = runtime.requestedControlProtocol()
  let effectiveControlUrl = runtime.baseControlUrl()
  if runtime.state.controlUrl.strip().len == 0 and effectiveControlUrl.len > 0:
    runtime.state.controlUrl = effectiveControlUrl
  runtime.controlProtocolRequested = requestedProtocol
  runtime.controlFallback = ""
  var failures: seq[string] = @[]
  var selectedProtocol = requestedProtocol
  var boot = TsnetControlBootstrapResult.init()
  var bootReady = false
  let candidates =
    if not runtime.controlTransport.isNil:
      @[requestedProtocol]
    else:
      controlProtocolCandidates(requestedProtocol)
  for candidate in candidates:
    let normalized = normalizeControlProtocol(candidate)
    runtime.controlProtocolSelected = normalized
    if runtime.controlTransport.isNil:
      let resolved = runtime.liveControlTransport(normalized)
      runtime.controlEndpoint = resolved.endpoint
      when not defined(ssl):
        if runtime.resolvedControlDialUrl(normalized).toLowerAscii().startsWith("https://") and
            protocolNeedsStdlibSsl(normalized):
          failures.add("ssl_required_for_https_control")
          continue
      let bootstrap = control.bootstrapControl(
        resolved.transport,
        runtime.bootstrapInput()
      )
      if bootstrap.isOk():
        selectedProtocol = normalized
        boot = bootstrap.get()
        bootReady = true
        break
      failures.add(normalized & ": " & bootstrap.error)
      continue

    let bootstrap = control.bootstrapControl(
      runtime.controlTransport,
      runtime.bootstrapInput()
    )
    if bootstrap.isOk():
      selectedProtocol = normalized
      runtime.controlEndpoint = normalizeControlUrl(runtime.state.controlUrl)
      boot = bootstrap.get()
      bootReady = true
      break
    failures.add(normalized & ": " & bootstrap.error)
    break

  if not bootReady:
    if failures.len > 0:
      runtime.controlFallback = failures.join("; ")
      return err(runtime.controlFallback)
    return err("failed to bootstrap tsnet control")
  runtime.controlProtocolSelected = selectedProtocol
  if requestedProtocol == TsnetControlProtocolAuto and failures.len > 0:
    runtime.controlFallback = failures.join("; ")
  let probed = boot.serverKey
  runtime.state.controlPublicKey = probed.publicKey
  runtime.state.controlLegacyPublicKey = probed.legacyPublicKey
  runtime.state.lastControlSuccessUnixMilli = probed.fetchedAtUnixMilli
  runtime.state.lastRegisterAttemptUnixMilli = probed.fetchedAtUnixMilli
  runtime.state.lastMapPollAttemptUnixMilli = probed.fetchedAtUnixMilli
  runtime.state.lastControlBootstrapError = ""
  runtime.control = boot.snapshot
  if boot.registerResult.capturedAtUnixMilli > 0:
    runtime.state.lastRegisterAttemptUnixMilli = boot.registerResult.capturedAtUnixMilli
  if boot.mapPollResult.capturedAtUnixMilli > 0:
    runtime.state.lastMapPollAttemptUnixMilli = boot.mapPollResult.capturedAtUnixMilli
  discard runtime.persistState().valueOr:
    return err(error)
  ok(boot)

proc persistMappedRuntime(
    runtime: TsnetInAppRuntime,
    bootstrap: TsnetControlBootstrapResult
): Result[void, string] =
  if runtime.isNil:
    return err("tsnet in-app runtime is nil")
  if bootstrap.mapPollResult.rawPayload.isNil or bootstrap.mapPollResult.rawPayload.kind == JNull:
    return err("tsnet map poll result is missing a response payload")
  runtime.session = sessionFromControlMap(
    source = "nim-tsnet-inapp-live",
    capturedAtUnixMilli = bootstrap.mapPollResult.capturedAtUnixMilli,
    rawPayload = bootstrap.mapPollResult.rawPayload,
    hostname = runtime.state.hostname,
    controlUrl = runtime.state.controlUrl,
    wireguardPort = runtime.cfg.wireguardPort
  ).valueOr:
    return err(error)
  runtime.state.tailnetIPs = runtime.session.status.tailnetIPs
  runtime.state.homeDerp = runtime.session.homeDerpLabel()
  runtime.state.peerCache = runtime.session.peerCache()
  runtime.state.lastControlBootstrapError = ""
  runtime.state.lastControlSuccessUnixMilli =
    max(runtime.state.lastControlSuccessUnixMilli, bootstrap.mapPollResult.capturedAtUnixMilli)
  runtime.state.mapSessionHandle = bootstrap.mapPollResult.summary.mapSessionHandle
  runtime.state.mapSessionSeq = bootstrap.mapPollResult.summary.seq
  discard runtime.persistState().valueOr:
    return err(error)
  ok()

proc start*(runtime: TsnetInAppRuntime): Result[void, string] =
  if runtime.isNil:
    return err("tsnet in-app runtime is nil")
  runtime.status = TsnetInAppRuntimeStatus.Bootstrapping
  runtime.clearListenerRoutes()
  discard runtime.loadPersistedState().valueOr:
    return err(error)
  discard runtime.ensureStateMaterial().valueOr:
    return err(error)
  let oracleFixturePath = runtime.oraclePath()
  if runtime.oracleEnabled() and oracleFixturePath.len > 0 and fileExists(oracleFixturePath):
    let fixture = loadOracleFixture(oracleFixturePath).valueOr:
      runtime.status = TsnetInAppRuntimeStatus.Failed
      runtime.lastError = error
      return err(error)
    let merged = runtime.mergeFixtureSession(fixture)
    if merged.isErr():
      runtime.status = TsnetInAppRuntimeStatus.Failed
      runtime.lastError = merged.error
      return err(merged.error)
    runtime.status = TsnetInAppRuntimeStatus.Running
    runtime.lastError = ""
    return ok()
  if runtime.liveControlRequested():
    let bootstrap = runtime.probeControlServer().valueOr:
      runtime.status = TsnetInAppRuntimeStatus.Failed
      runtime.lastError = error
      return err(error)
    if bootstrap.error.len > 0:
      runtime.lastError = bootstrap.error
    case bootstrap.stage
    of TsnetControlBootstrapStage.AuthRequired:
      runtime.session = runtime.buildUnavailableSession("AuthRequired", "auth_required")
    of TsnetControlBootstrapStage.Registered:
      runtime.session = runtime.buildUnavailableSession("Registered", "map_poll_required")
    of TsnetControlBootstrapStage.ControlReachable:
      runtime.session = runtime.buildUnavailableSession(
        "ControlReachable",
        "control_reachable_but_register_map_unimplemented"
      )
    of TsnetControlBootstrapStage.Mapped:
      let persisted = runtime.persistMappedRuntime(bootstrap)
      if persisted.isErr():
        runtime.status = TsnetInAppRuntimeStatus.Failed
        runtime.lastError = persisted.error
        return err(persisted.error)
      runtime.demoteRelayPath()
      runtime.status = TsnetInAppRuntimeStatus.Running
      runtime.lastError = ""
      return ok()
    of TsnetControlBootstrapStage.ProbeFailed:
      runtime.session = TsnetSessionSnapshot.init()
    runtime.state.lastControlBootstrapError =
      if bootstrap.error.len > 0:
        bootstrap.error
      else:
        runtime.runtimeUnavailableError()
    discard runtime.persistState()
  runtime.status = TsnetInAppRuntimeStatus.Failed
  runtime.lastError = runtime.runtimeUnavailableError()
  err(runtime.lastError)

proc reset*(runtime: TsnetInAppRuntime): Result[void, string] =
  if runtime.isNil:
    return err("tsnet in-app runtime is nil")
  let path = runtime.statePath()
  if path.len > 0 and fileExists(path):
    try:
      removeFile(path)
    except CatchableError as exc:
      return err("failed to remove tsnet state " & path & ": " & exc.msg)
  runtime.state = TsnetStoredState.init(
    hostname = runtime.cfg.hostname,
    controlUrl = runtime.cfg.controlUrl
  )
  runtime.control = TsnetControlSnapshot.init()
  runtime.session = TsnetSessionSnapshot.init()
  runtime.status = TsnetInAppRuntimeStatus.Stopped
  runtime.lastError = ""
  runtime.clearListenerRoutes()
  runtime.controlProtocolRequested = normalizeControlProtocol(runtime.cfg.controlProtocol)
  runtime.controlProtocolSelected = ""
  runtime.controlEndpoint = ""
  runtime.controlFallback = ""
  unregisterProxyRoutes(runtime.runtimeId)
  trelay.stopRelayListeners(runtime.runtimeId)
  when defined(libp2p_msquic_experimental):
    qrelay.stopRelayListeners(runtime.runtimeId)
  ok()

proc refreshControlMetadata*(runtime: TsnetInAppRuntime): Result[void, string] =
  if runtime.isNil:
    return err("tsnet in-app runtime is nil")
  if not runtime.liveControlRequested():
    return err("tsnet controlUrl is empty")
  let bootstrap = runtime.probeControlServer().valueOr:
    runtime.lastError = error
    runtime.state.lastControlBootstrapError = error
    discard runtime.persistState()
    return err(error)
  if bootstrap.stage != TsnetControlBootstrapStage.Mapped:
    let errText =
      if bootstrap.error.len > 0:
        bootstrap.error
      else:
        "tsnet control metadata refresh did not reach mapped"
    runtime.lastError = errText
    runtime.state.lastControlBootstrapError = errText
    discard runtime.persistState()
    return err(errText)
  let persisted = runtime.persistMappedRuntime(bootstrap)
  if persisted.isErr():
    let error = persisted.error
    runtime.lastError = error
    runtime.state.lastControlBootstrapError = error
    discard runtime.persistState()
    return err(error)
  runtime.demoteRelayPath()
  runtime.status = TsnetInAppRuntimeStatus.Running
  runtime.lastError = ""
  discard runtime.persistState()
  ok()

proc stop*(runtime: TsnetInAppRuntime) =
  if runtime.isNil:
    return
  unregisterProxyRoutes(runtime.runtimeId)
  runtime.status = TsnetInAppRuntimeStatus.Stopped
  runtime.lastError = ""
  runtime.clearListenerRoutes()
  runtime.controlProtocolSelected = ""
  runtime.controlEndpoint = ""
  runtime.controlFallback = ""
  trelay.stopRelayListeners(runtime.runtimeId)
  when defined(libp2p_msquic_experimental):
    qrelay.stopRelayListeners(runtime.runtimeId)

proc unavailablePayloadError(): string

proc reconcileProxyListeners*(
    runtime: TsnetInAppRuntime
): Result[void, string] =
  if runtime.isNil:
    return err("tsnet in-app runtime is nil")
  if not runtime.ready() or not runtime.session.supportsProxyRouting():
    return err(unavailablePayloadError())
  when defined(libp2p_msquic_experimental):
    if not runtime.liveQuicRelayMode():
      return ok()
    let routes = proxyRouteSnapshots(runtime.runtimeId)
    if routes.len == 0:
      return ok()
    qrelay.stopRelayListeners(runtime.runtimeId)
    for route in routes:
      let started =
        case route.kind
        of TsnetProxyKind.Tcp:
          qrelay.startRelayListener(
            runtime.runtimeId,
            runtime.resolvedRelayDialUrl(),
            route.advertised,
            route.raw
          )
        of TsnetProxyKind.Quic:
          qrelay.startUdpRelayListener(
            runtime.runtimeId,
            runtime.resolvedRelayDialUrl(),
            route.advertised,
            route.raw,
            runtime.cfg.bridgeExtraJson
          )
      if started.isErr():
        return err(started.error)
    return ok()
  ok()

proc unavailablePayloadError(): string =
  TsnetMissingInAppProviderError

proc statusPayload*(runtime: TsnetInAppRuntime): Result[JsonNode, string] =
  if runtime.isNil:
    return err("tsnet in-app runtime is nil")
  if not runtime.ready() and runtime.session.status.backendState.len == 0:
    return err(unavailablePayloadError())
  var payload = appendControlDiagnostics(runtime.session.status.toJson(), runtime)
  let normalizedTailnetPath = normalizeTailnetPath(requestString(payload, "tailnetPath"))
  if normalizedTailnetPath.len > 0:
    payload["tailnetPath"] = %normalizedTailnetPath
    if not tailnetPathUsesRelay(normalizedTailnetPath):
      payload["tailnetRelay"] = %""
  payload = appendTailnetPathDiagnostics(payload, runtime)
  let readiness = runtime.proxyListenerReadiness()
  payload["proxyListenerExpected"] = %readiness.expected
  payload["proxyListenerReady"] = %readiness.ready
  payload["proxyListenersReady"] = %(readiness.expected == 0 or readiness.ready >= readiness.expected)
  when defined(libp2p_msquic_experimental):
    if runtime.liveQuicRelayMode():
      payload["udpDialStates"] = qrelay.udpDialStatesPayload(runtime.runtimeId)
      payload["relayListeners"] = qrelay.relayListenerStatesPayload(runtime.runtimeId)
  ok(payload)

proc requestString(node: JsonNode, key: string): string =
  if node.isNil or node.kind != JObject or not node.hasKey(key):
    return ""
  let value = node.getOrDefault(key)
  if value.kind == JString:
    return value.getStr().strip()
  ""

proc pingPayload*(runtime: TsnetInAppRuntime, request: JsonNode): Result[JsonNode, string] =
  if runtime.isNil:
    return err("tsnet in-app runtime is nil")
  if not runtime.ready() and runtime.session.status.backendState.len == 0:
    return err(unavailablePayloadError())
  let requestedType = requestString(request, "pingType").toUpperAscii()
  let requestedPeer = requestString(request, "peerIP")
  if requestedType.len > 0 and requestedType != "TSMP":
    var unsupported = TsnetPingSnapshot(
      ok: false,
      error: "unsupported_ping_type",
      peerIP:
        if requestedPeer.len > 0: requestedPeer
        else: runtime.session.ping.peerIP,
      pingType: requestedType,
      samples: @[],
      minLatencyMs: 0,
      maxLatencyMs: 0,
      avgLatencyMs: 0,
      elapsedMs: 0
    )
    return ok(unsupported.toJson())
  var payload = runtime.session.ping
  if payload.pingType.len == 0:
    payload.pingType = "TSMP"
  if requestedPeer.len > 0:
    payload.peerIP = requestedPeer
  if requestedType == "TSMP":
    payload.pingType = "TSMP"
  ok(payload.toJson())

proc derpMapPayload*(runtime: TsnetInAppRuntime): Result[JsonNode, string] =
  if runtime.isNil:
    return err("tsnet in-app runtime is nil")
  if not runtime.ready() and runtime.session.status.backendState.len == 0:
    return err(unavailablePayloadError())
  ok(runtime.session.derpMap.toJson())

proc listenTcpProxy*(
    runtime: TsnetInAppRuntime,
    family: string,
    port: int,
    localAddress: MultiAddress
): Result[MultiAddress, string] =
  if runtime.isNil:
    return err("tsnet in-app runtime is nil")
  if not runtime.ready() or not runtime.session.supportsProxyRouting():
    return err(unavailablePayloadError())
  let useFamily =
    if family.len > 0: family
    else: familyFromAddress(localAddress)
  let tailnetIp = chooseTailnetIp(runtime.state.tailnetIPs, useFamily)
  let usePort =
    if port > 0: port
    else: portFromAddress(localAddress)
  let advertised = buildAdvertisedAddress(useFamily, tailnetIp, usePort, TsnetProxyKind.Tcp).valueOr:
    return err(error)
  let registered = registerProxyRoute(runtime.runtimeId, advertised, localAddress)
  if registered.isErr():
    return err(registered.error)
  runtime.tcpListenerRoutes.rememberListenerRoute($advertised)
  if runtime.liveQuicRelayMode():
    when defined(libp2p_msquic_experimental):
      let relayStarted = qrelay.startRelayListener(
        runtime.runtimeId,
        runtime.resolvedRelayDialUrl(),
        advertised,
        localAddress
      )
      if relayStarted.isErr():
        return err(relayStarted.error)
  ok(advertised)

proc listenUdpProxy*(
    runtime: TsnetInAppRuntime,
    family: string,
    port: int,
    localAddress: MultiAddress
): Result[MultiAddress, string] =
  if runtime.isNil:
    return err("tsnet in-app runtime is nil")
  if not runtime.ready() or not runtime.session.supportsProxyRouting():
    return err(unavailablePayloadError())
  let useFamily =
    if family.len > 0: family
    else: familyFromAddress(localAddress)
  let tailnetIp = chooseTailnetIp(runtime.state.tailnetIPs, useFamily)
  let usePort =
    if port > 0: port
    else: portFromAddress(localAddress)
  let advertised = buildAdvertisedAddress(useFamily, tailnetIp, usePort, TsnetProxyKind.Quic).valueOr:
    return err(error)
  let registered = registerProxyRoute(runtime.runtimeId, advertised, localAddress)
  if registered.isErr():
    return err(registered.error)
  runtime.udpListenerRoutes.rememberListenerRoute($advertised)
  if runtime.liveQuicRelayMode():
    when defined(libp2p_msquic_experimental):
      let relayStarted = qrelay.startUdpRelayListener(
        runtime.runtimeId,
        runtime.resolvedRelayDialUrl(),
        advertised,
        localAddress,
        runtime.cfg.bridgeExtraJson
      )
      if relayStarted.isErr():
        return err(relayStarted.error)
  ok(advertised)

proc registerDirectProxyRoute*(
    runtime: TsnetInAppRuntime,
    advertised: MultiAddress,
    rawAddress: MultiAddress,
    punched = false
): Result[void, string] =
  if runtime.isNil:
    return err("tsnet in-app runtime is nil")
  if not runtime.ready() or not runtime.session.supportsProxyRouting():
    return err(unavailablePayloadError())
  let pathKind =
    if punched: TsnetPathPunchedDirect
    else: TsnetPathDirect
  registerDirectRoute(runtime.runtimeId, advertised, rawAddress, pathKind)

proc markFailedDirectProxyRoute*(
    runtime: TsnetInAppRuntime,
    advertised: MultiAddress,
    rawAddress: MultiAddress
): Result[void, string] =
  if runtime.isNil:
    return err("tsnet in-app runtime is nil")
  markDirectRouteFailure(runtime.runtimeId, advertised, rawAddress)

proc dialTcpProxy*(
    runtime: TsnetInAppRuntime,
    family, ip: string,
    port: int
): Result[MultiAddress, string] =
  if runtime.isNil:
    return err("tsnet in-app runtime is nil")
  if not runtime.ready() or not runtime.session.supportsProxyRouting():
    return err(unavailablePayloadError())
  let localRoute = lookupRawTarget(family, ip, port, TsnetProxyKind.Tcp)
  if localRoute.isOk():
    return localRoute
  let directRoute = lookupDirectTarget(runtime.runtimeId, family, ip, port, TsnetProxyKind.Tcp)
  if directRoute.isOk():
    runtime.setTailnetPathState(directRoute.get().pathKind)
    return ok(directRoute.get().raw)
  if runtime.liveQuicRelayMode():
    when defined(libp2p_msquic_experimental):
      runtime.demoteRelayPath()
      let remoteAdvertised = buildAdvertisedAddress(family, ip, port, TsnetProxyKind.Tcp).valueOr:
        return err(error)
      let localTailnetIp = chooseTailnetIp(runtime.state.tailnetIPs, family)
      return qrelay.openDialProxy(
        runtime.runtimeId,
        runtime.resolvedRelayDialUrl(),
        family,
        localTailnetIp,
        remoteAdvertised
      )
  err(localRoute.error)

proc dialUdpProxy*(
    runtime: TsnetInAppRuntime,
    family, ip: string,
    port: int
): Result[MultiAddress, string] =
  if runtime.isNil:
    return err("tsnet in-app runtime is nil")
  if not runtime.ready() or not runtime.session.supportsProxyRouting():
    return err(unavailablePayloadError())
  let localRoute = lookupRawTarget(family, ip, port, TsnetProxyKind.Quic)
  if localRoute.isOk():
    return localRoute
  let directRoute = lookupDirectTarget(runtime.runtimeId, family, ip, port, TsnetProxyKind.Quic)
  if directRoute.isOk():
    runtime.setTailnetPathState(directRoute.get().pathKind)
    return ok(directRoute.get().raw)
  if runtime.liveQuicRelayMode():
    when defined(libp2p_msquic_experimental):
      runtime.demoteRelayPath()
      let remoteAdvertised = buildAdvertisedAddress(family, ip, port, TsnetProxyKind.Quic).valueOr:
        return err(error)
      let localTailnetIp = chooseTailnetIp(runtime.state.tailnetIPs, family)
      return qrelay.openDialUdpProxy(
        runtime.runtimeId,
        runtime.resolvedRelayDialUrl(),
        family,
        localTailnetIp,
        remoteAdvertised,
        runtime.cfg.bridgeExtraJson
      )
  err(localRoute.error)

proc dialUdpProxyRelayFallback*(
    runtime: TsnetInAppRuntime,
    family, ip: string,
    port: int
): Result[MultiAddress, string] =
  if runtime.isNil:
    return err("tsnet in-app runtime is nil")
  if not runtime.ready() or not runtime.session.supportsProxyRouting():
    return err(unavailablePayloadError())
  if runtime.liveQuicRelayMode():
    when defined(libp2p_msquic_experimental):
      runtime.demoteRelayPath()
      let remoteAdvertised = buildAdvertisedAddress(family, ip, port, TsnetProxyKind.Quic).valueOr:
        return err(error)
      let localTailnetIp = chooseTailnetIp(runtime.state.tailnetIPs, family)
      return qrelay.openDialUdpProxy(
        runtime.runtimeId,
        runtime.resolvedRelayDialUrl(),
        family,
        localTailnetIp,
        remoteAdvertised,
        runtime.cfg.bridgeExtraJson
      )
  dialUdpProxy(runtime, family, ip, port)

proc udpDialState*(
    runtime: TsnetInAppRuntime,
    rawAddress: MultiAddress
): tuple[
    known, ready: bool,
    error, phase, detail: string,
    attempts: int,
    updatedUnixMilli: int64
  ] =
  if runtime.isNil:
    return
  when defined(libp2p_msquic_experimental):
    if runtime.liveQuicRelayMode():
      return qrelay.udpDialState(runtime.runtimeId, $rawAddress)

proc resolveRemote*(
    runtime: TsnetInAppRuntime,
    rawAddress: MultiAddress
): Result[MultiAddress, string] =
  if runtime.isNil:
    return err("tsnet in-app runtime is nil")
  if not runtime.ready() or not runtime.session.supportsProxyRouting():
    return err(unavailablePayloadError())
  resolveAdvertisedRemote(rawAddress)
