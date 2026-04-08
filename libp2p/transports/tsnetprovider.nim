{.push raises: [].}

import std/[json, locks, strutils]
import chronos
import chronicles

import ../multiaddress
import ../utility
import ./tsnetproviderbuiltin
import ./tsnetproviderinapp
import ./tsnetproviderlegacy as legacy
import ./tsnetprovidertypes

export tsnetprovidertypes

proc ownedText(text: string): string {.gcsafe, raises: [].} =
  if text.len == 0:
    return ""
  result = newStringOfCap(text.len)
  result.add(text)

proc ownedTextSeq(items: openArray[string]): seq[string] {.gcsafe, raises: [].} =
  result = newSeqOfCap[string](items.len)
  for item in items:
    result.add(ownedText(item))

type
  TsnetProvider* = ref object
    cfg*: TsnetProviderConfig
    kind*: TsnetProviderKind
    started*: bool
    lastError*: string
    stateLock: Lock
    runtime: tsnetproviderinapp.TsnetInAppRuntime
    bridge: legacy.TsnetLegacyBridgeHandle

  TsnetProviderReadSnapshot = object
    cfg: TsnetProviderConfig
    kind: TsnetProviderKind
    started: bool
    lastError: string
    runtime: tsnetproviderinapp.TsnetInAppRuntime
    bridge: legacy.TsnetLegacyBridgeHandle

proc new*(_: type[TsnetProvider], cfg: TsnetProviderConfig): TsnetProvider =
  result = TsnetProvider(
    cfg: block:
      var copy = cfg
      copy.bridgeExtraJson = ownedText(cfg.bridgeExtraJson)
      copy,
    kind: TsnetProviderKind.BuiltinSynthetic,
    started: false,
    lastError: ""
  )
  initLock(result.stateLock)

proc providerReadSnapshot(provider: TsnetProvider): TsnetProviderReadSnapshot {.gcsafe.} =
  if provider.isNil:
    return
  withLock(provider.stateLock):
    result.cfg = provider.cfg
    result.kind = provider.kind
    result.started = provider.started
    result.lastError = provider.lastError
    result.runtime = provider.runtime
    result.bridge = provider.bridge

proc snapshotLegacyBridgeRequested(snapshot: TsnetProviderReadSnapshot): bool {.gcsafe.} =
  legacy.legacyBridgeRequested(snapshot.cfg)

proc snapshotRuntimeRequested(snapshot: TsnetProviderReadSnapshot): bool {.gcsafe.} =
  snapshot.snapshotLegacyBridgeRequested() or
    tsnetproviderinapp.realRuntimeRequested(snapshot.cfg)

proc snapshotRequestedBackendLabel(snapshot: TsnetProviderReadSnapshot): string {.gcsafe.} =
  if snapshot.snapshotLegacyBridgeRequested():
    return "legacy-go-bridge"
  if tsnetproviderinapp.realRuntimeRequested(snapshot.cfg):
    return "nim-inapp"
  "builtin-synthetic"

proc kindLabel*(provider: TsnetProvider): string {.gcsafe.} =
  if provider.isNil:
    return kindLabel(TsnetProviderKind.BuiltinSynthetic)
  kindLabel(provider.kind)

proc capabilities*(kind: TsnetProviderKind): TsnetProviderCapabilities {.gcsafe.} =
  case kind
  of TsnetProviderKind.BuiltinSynthetic:
    builtinSyntheticCapabilities()
  of TsnetProviderKind.InAppUnavailable:
    unavailableCapabilities()
  of TsnetProviderKind.InAppReal:
    tsnetproviderinapp.inAppRuntimeCapabilities()
  of TsnetProviderKind.LegacyBridge:
    legacy.legacyBridgeCapabilities()

proc snapshotCapabilities(snapshot: TsnetProviderReadSnapshot): TsnetProviderCapabilities {.gcsafe.} =
  if snapshot.kind == TsnetProviderKind.BuiltinSynthetic and
      snapshot.runtime.isNil and snapshot.bridge.isNil and
      not snapshot.started and snapshot.lastError.len == 0:
    return builtinSyntheticCapabilities()
  case snapshot.kind
  of TsnetProviderKind.InAppUnavailable:
    if tsnetproviderinapp.runtimePresent(snapshot.runtime):
      return tsnetproviderinapp.runtimeCapabilities(snapshot.runtime)
    unavailableCapabilities()
  of TsnetProviderKind.InAppReal:
    if tsnetproviderinapp.runtimeActive(snapshot.runtime):
      return tsnetproviderinapp.runtimeCapabilities(snapshot.runtime)
    unavailableCapabilities()
  of TsnetProviderKind.LegacyBridge:
    if legacy.bridgeActive(snapshot.bridge):
      return legacy.legacyBridgeCapabilities()
    unavailableCapabilities()
  of TsnetProviderKind.BuiltinSynthetic:
    builtinSyntheticCapabilities()

proc capabilities*(provider: TsnetProvider): TsnetProviderCapabilities {.gcsafe.} =
  if provider.isNil:
    return builtinSyntheticCapabilities()
  result = snapshotCapabilities(provider.providerReadSnapshot())

proc isProxyBacked*(provider: TsnetProvider): bool {.gcsafe.} =
  if provider.isNil:
    return false
  let snapshot = provider.providerReadSnapshot()
  result =
    case snapshot.kind
    of TsnetProviderKind.InAppReal:
      tsnetproviderinapp.runtimeCapabilities(snapshot.runtime).proxyBacked
    of TsnetProviderKind.LegacyBridge:
      legacy.bridgeActive(snapshot.bridge)
    else:
      false

proc supportsExactRouting*(provider: TsnetProvider): bool {.gcsafe.} =
  if provider.isNil:
    return false
  result = provider.providerReadSnapshot().kind in {
    TsnetProviderKind.InAppReal,
    TsnetProviderKind.LegacyBridge
  }

proc legacyBridgeRequested*(provider: TsnetProvider): bool {.gcsafe.} =
  if provider.isNil:
    return false
  legacy.legacyBridgeRequested(provider.cfg)

proc runtimeRequested*(provider: TsnetProvider): bool {.gcsafe.} =
  if provider.isNil:
    return false
  provider.legacyBridgeRequested() or tsnetproviderinapp.realRuntimeRequested(provider.cfg)

proc requestedBackendLabel*(provider: TsnetProvider): string {.gcsafe.} =
  if provider.isNil:
    return "builtin-synthetic"
  if provider.legacyBridgeRequested():
    return "legacy-go-bridge"
  if tsnetproviderinapp.realRuntimeRequested(provider.cfg):
    return "nim-inapp"
  "builtin-synthetic"

proc ready*(provider: TsnetProvider): bool {.gcsafe.} =
  if provider.isNil:
    return false
  result = provider.providerReadSnapshot().started

proc providerRuntimeHandle(
    provider: TsnetProvider
): tuple[kind: TsnetProviderKind, runtime: tsnetproviderinapp.TsnetInAppRuntime] {.gcsafe.} =
  if provider.isNil:
    return (TsnetProviderKind.BuiltinSynthetic, nil)
  withLock(provider.stateLock):
    result.kind = provider.kind
    result.runtime = provider.runtime

proc listenerNeedsRepair*(provider: TsnetProvider): bool {.gcsafe.} =
  if provider.isNil:
    return false
  let handle = provider.providerRuntimeHandle()
  result =
    case handle.kind
    of TsnetProviderKind.InAppUnavailable, TsnetProviderKind.InAppReal:
      tsnetproviderinapp.listenerNeedsRepair(handle.runtime)
    else:
      false

proc proxyListenersReady*(provider: TsnetProvider): bool {.gcsafe.} =
  if provider.isNil:
    return false
  let handle = provider.providerRuntimeHandle()
  result =
    case handle.kind
    of TsnetProviderKind.InAppUnavailable, TsnetProviderKind.InAppReal:
      tsnetproviderinapp.proxyListenersReady(handle.runtime)
    else:
      false

proc proxyRouteCount*(provider: TsnetProvider): int {.gcsafe.} =
  if provider.isNil:
    return 0
  let handle = provider.providerRuntimeHandle()
  result =
    case handle.kind
    of TsnetProviderKind.InAppUnavailable, TsnetProviderKind.InAppReal:
      tsnetproviderinapp.proxyRouteCount(handle.runtime)
    else:
      0

proc publishedAddrTexts*(provider: TsnetProvider): seq[string] {.gcsafe.} =
  if provider.isNil:
    return @[]
  let handle = provider.providerRuntimeHandle()
  let published =
    case handle.kind
    of TsnetProviderKind.InAppUnavailable, TsnetProviderKind.InAppReal:
      tsnetproviderinapp.publishedAddrTexts(handle.runtime)
    else:
      @[]
  result = ownedTextSeq(published)

proc tailnetIpTexts*(provider: TsnetProvider): seq[string] {.gcsafe.} =
  if provider.isNil:
    return @[]
  let snapshot = provider.providerReadSnapshot()
  result =
    case snapshot.kind
    of TsnetProviderKind.InAppUnavailable, TsnetProviderKind.InAppReal:
      tsnetproviderinapp.tailnetIpTexts(snapshot.runtime)
    else:
      @[]

proc failure*(provider: TsnetProvider): string {.gcsafe.} =
  if provider.isNil:
    return ""
  result = provider.providerReadSnapshot().lastError

proc updateBridgeExtraJson*(provider: TsnetProvider, bridgeExtraJson: string) {.gcsafe.} =
  if provider.isNil:
    return
  let ownedJson = ownedText(bridgeExtraJson)
  withLock(provider.stateLock):
    provider.cfg.bridgeExtraJson = ownedJson
    if tsnetproviderinapp.runtimePresent(provider.runtime):
      provider.runtime.cfg.bridgeExtraJson = ownedJson

proc jsonField(node: JsonNode, key: string): JsonNode {.gcsafe.} =
  if node.isNil or node.kind != JObject or not node.hasKey(key):
    return newJNull()
  node.getOrDefault(key)

proc annotatePayloadSnapshot(
    snapshot: TsnetProviderReadSnapshot,
    payload: JsonNode
): JsonNode {.gcsafe.} =
  result =
    if payload.isNil or payload.kind != JObject:
      newJObject()
    else:
      payload
  let normalizedTailnetPath =
    if result.hasKey("tailnetPath"):
      let tailnetPathNode = result.getOrDefault("tailnetPath")
      if tailnetPathNode.kind == JString:
        normalizeTailnetPath(tailnetPathNode.getStr())
      else:
        ""
    else:
      ""
  if normalizedTailnetPath.len > 0:
    result["tailnetPath"] = %normalizedTailnetPath
    if not tailnetPathUsesRelay(normalizedTailnetPath):
      result["tailnetRelay"] = %""
  result["providerKind"] = %kindLabel(snapshot.kind)
  result["providerRequestedBackend"] = %snapshot.snapshotRequestedBackendLabel()
  result["providerRuntimeRequested"] = %snapshot.snapshotRuntimeRequested()
  result["providerReady"] = %snapshot.started
  let caps = snapshotCapabilities(snapshot)
  result["providerInApp"] = %caps.inApp
  result["legacyBridgeDeprecated"] = %(snapshot.kind == TsnetProviderKind.LegacyBridge)
  result["providerCapabilities"] = caps.toJson()
  if snapshot.lastError.len > 0:
    result["providerError"] = %snapshot.lastError
  if snapshot.kind == TsnetProviderKind.LegacyBridge:
    result["providerWarning"] = %TsnetLegacyBridgeWarning

proc annotatePayload*(provider: TsnetProvider, payload: JsonNode): JsonNode {.gcsafe.} =
  result = annotatePayloadSnapshot(provider.providerReadSnapshot(), payload)

proc start*(provider: TsnetProvider): Result[TsnetProviderKind, string] =
  if provider.isNil:
    return err("tsnet provider is nil")
  withLock(provider.stateLock):
    warn "tsnet provider start begin",
      started = provider.started,
      runtimeRequested = provider.runtimeRequested(),
      legacyRequested = provider.legacyBridgeRequested(),
      currentKind = kindLabel(provider.kind)
    if provider.started:
      warn "tsnet provider start reuse",
        kind = kindLabel(provider.kind)
      return ok(provider.kind)
    provider.lastError = ""
    if legacy.bridgeActive(provider.bridge):
      provider.started = true
      provider.kind = TsnetProviderKind.LegacyBridge
      warn "tsnet provider start legacy active"
      return ok(provider.kind)
    if not provider.runtimeRequested():
      provider.started = true
      provider.kind = TsnetProviderKind.BuiltinSynthetic
      warn "tsnet provider start builtin synthetic"
      return ok(provider.kind)
    if not provider.legacyBridgeRequested():
      warn "tsnet provider start in-app runtime create begin"
      provider.runtime = tsnetproviderinapp.newInAppRuntime(provider.cfg)
      warn "tsnet provider start in-app runtime create done"
      warn "tsnet provider start in-app runtime start begin"
      let started = tsnetproviderinapp.startInAppRuntime(provider.runtime)
      if started.isErr():
        provider.started = false
        provider.kind = TsnetProviderKind.InAppUnavailable
        provider.lastError = started.error
        warn "tsnet provider start in-app runtime start failed",
          err = provider.lastError
        return err(provider.lastError)
      provider.started = true
      provider.lastError = ""
      provider.kind = TsnetProviderKind.InAppReal
      warn "tsnet provider start in-app runtime start done",
        kind = kindLabel(provider.kind)
      return ok(provider.kind)
    warn "tsnet provider start legacy bridge open begin"
    let opened = legacy.openLegacyBridge(provider.cfg).valueOr:
      provider.started = false
      provider.kind = TsnetProviderKind.LegacyBridge
      provider.lastError =
        "no Nim in-app tsnet provider is available yet; legacy bridge load failed: " &
        error
      warn "tsnet provider start legacy bridge open failed",
        err = provider.lastError
      return err(provider.lastError)
    provider.bridge = opened
    provider.started = true
    provider.lastError = ""
    provider.kind = TsnetProviderKind.LegacyBridge
    warn "tsnet provider start legacy bridge open done"
    result = ok(provider.kind)

proc warmConfigJson*(provider: TsnetProvider): Result[string, string] {.gcsafe.} =
  if provider.isNil:
    return err("tsnet provider is nil")
  withLock(provider.stateLock):
    if provider.legacyBridgeRequested():
      return err("tsnet warm snapshot does not support legacy bridge")
    if not provider.runtimeRequested():
      return err("tsnet warm snapshot requires a real in-app runtime")
    result = ok($(provider.cfg.toJson()))

proc applyWarmSnapshot*(
    provider: TsnetProvider,
    payloadText: string
): Result[TsnetProviderKind, string] {.gcsafe.} =
  if provider.isNil:
    return err("tsnet provider is nil")
  withLock(provider.stateLock):
    if provider.legacyBridgeRequested():
      return err("tsnet warm snapshot does not support legacy bridge")
    if not provider.runtimeRequested():
      provider.started = true
      provider.kind = TsnetProviderKind.BuiltinSynthetic
      provider.lastError = ""
      return ok(provider.kind)
    provider.runtime = tsnetproviderinapp.newInAppRuntime(provider.cfg)
    let applied = tsnetproviderinapp.applyWarmSnapshotJson(provider.runtime, payloadText)
    if applied.isErr():
      provider.started = false
      provider.kind = TsnetProviderKind.InAppUnavailable
      provider.lastError = applied.error
      return err(applied.error)
    provider.started = true
    provider.kind = TsnetProviderKind.InAppReal
    provider.lastError = provider.runtime.lastError
    result = ok(provider.kind)

proc reset*(provider: TsnetProvider): Result[void, string] =
  if provider.isNil:
    return err("tsnet provider is nil")
  withLock(provider.stateLock):
    if tsnetproviderinapp.runtimePresent(provider.runtime):
      return tsnetproviderinapp.resetInAppRuntime(provider.runtime)
    if not legacy.bridgeActive(provider.bridge):
      return ok()
    result = legacy.resetLegacyBridge(provider.bridge)

proc refreshControlMetadata*(provider: TsnetProvider): Result[void, string] {.gcsafe.} =
  if provider.isNil:
    return err("tsnet provider is nil")
  var runtime: tsnetproviderinapp.TsnetInAppRuntime = nil
  provider.stateLock.acquire()
  try:
    if provider.kind != TsnetProviderKind.InAppReal or not tsnetproviderinapp.runtimePresent(provider.runtime):
      return ok()
    runtime = provider.runtime
  finally:
    provider.stateLock.release()
  let refreshed = tsnetproviderinapp.refreshControlMetadata(runtime)
  if refreshed.isErr():
    let error = refreshed.error
    withLock(provider.stateLock):
      provider.lastError = error
    return err(error)
  withLock(provider.stateLock):
    provider.lastError = ""
  ok()

proc reconcileProxyListeners*(
    provider: TsnetProvider
): Future[Result[void, string]] {.async: (raises: [CancelledError]).} =
  if provider.isNil:
    return err("tsnet provider is nil")
  var runtime: tsnetproviderinapp.TsnetInAppRuntime = nil
  provider.stateLock.acquire()
  try:
    if provider.kind != TsnetProviderKind.InAppReal or not tsnetproviderinapp.runtimePresent(provider.runtime):
      return ok()
    runtime = provider.runtime
  finally:
    provider.stateLock.release()
  let repaired = await tsnetproviderinapp.reconcileProxyListeners(runtime)
  if repaired.isErr():
    withLock(provider.stateLock):
      provider.lastError = repaired.error
    return err(repaired.error)
  withLock(provider.stateLock):
    provider.lastError = ""
  ok()

proc stop*(provider: TsnetProvider) =
  if provider.isNil:
    return
  withLock(provider.stateLock):
    provider.started = false
    provider.lastError = ""
    tsnetproviderinapp.closeInAppRuntime(provider.runtime)
    if legacy.bridgeActive(provider.bridge):
      legacy.closeLegacyBridge(provider.bridge)
      provider.bridge = nil
    provider.kind = TsnetProviderKind.BuiltinSynthetic

proc statusPayload*(provider: TsnetProvider): Result[JsonNode, string] =
  if provider.isNil:
    return err("tsnet provider is nil")
  let snapshot = provider.providerReadSnapshot()
  if not snapshotCapabilities(snapshot).statusApi:
    return err("tsnet provider is not running a real tailnet backend")
  if snapshot.kind in {TsnetProviderKind.InAppReal, TsnetProviderKind.InAppUnavailable}:
    let payload = tsnetproviderinapp.statusPayload(snapshot.runtime).valueOr:
      return err(error)
    return ok(annotatePayloadSnapshot(snapshot, payload))
  let payload = legacy.statusPayload(snapshot.bridge).valueOr:
    return err(error)
  result = ok(annotatePayloadSnapshot(snapshot, payload))

proc pingPayload*(provider: TsnetProvider, request: JsonNode): Result[JsonNode, string] =
  if provider.isNil:
    return err("tsnet provider is nil")
  let snapshot = provider.providerReadSnapshot()
  if not snapshotCapabilities(snapshot).pingApi:
    return err("tailnet ping requires a real tsnet provider")
  if snapshot.kind in {TsnetProviderKind.InAppReal, TsnetProviderKind.InAppUnavailable}:
    let payload = tsnetproviderinapp.pingPayload(snapshot.runtime, request).valueOr:
      return err(error)
    return ok(annotatePayloadSnapshot(snapshot, payload))
  let payload = legacy.pingPayload(snapshot.bridge, request).valueOr:
    return err(error)
  result = ok(annotatePayloadSnapshot(snapshot, payload))

proc derpMapPayload*(provider: TsnetProvider): Result[JsonNode, string] =
  if provider.isNil:
    return err("tsnet provider is nil")
  let snapshot = provider.providerReadSnapshot()
  if snapshot.kind in {TsnetProviderKind.InAppReal, TsnetProviderKind.InAppUnavailable}:
    let payload = tsnetproviderinapp.derpMapPayload(snapshot.runtime).valueOr:
      return err(error)
    return ok(annotatePayloadSnapshot(snapshot, payload))
  if not snapshotCapabilities(snapshot).realTailnet:
    return ok(annotatePayloadSnapshot(snapshot, builtinSyntheticDerpMapPayload()))
  let payload = legacy.derpMapPayload(snapshot.bridge).valueOr:
    return err(error)
  result = ok(annotatePayloadSnapshot(snapshot, payload))

proc listenTcpProxy*(
    provider: TsnetProvider,
    family: string,
    port: int,
    localAddress: MultiAddress
): Result[MultiAddress, string] =
  if provider.isNil:
    return err("tsnet provider is nil")
  withLock(provider.stateLock):
    case provider.kind
    of TsnetProviderKind.InAppReal:
      result = tsnetproviderinapp.listenTcpProxy(provider.runtime, family, port, localAddress)
    of TsnetProviderKind.LegacyBridge:
      result = legacy.listenTcpProxy(provider.bridge, family, port, localAddress)
    else:
      result = err("tsnet provider does not expose tcp proxy routing")

proc listenUdpProxy*(
    provider: TsnetProvider,
    family: string,
    port: int,
    localAddress: MultiAddress
): Result[MultiAddress, string] =
  if provider.isNil:
    return err("tsnet provider is nil")
  withLock(provider.stateLock):
    case provider.kind
    of TsnetProviderKind.InAppReal:
      result = tsnetproviderinapp.listenUdpProxy(provider.runtime, family, port, localAddress)
    of TsnetProviderKind.LegacyBridge:
      result = legacy.listenUdpProxy(provider.bridge, family, port, localAddress)
    else:
      result = err("tsnet provider does not expose udp proxy routing")

proc dialTcpProxy*(
    provider: TsnetProvider,
    family, ip: string,
    port: int
): Result[MultiAddress, string] =
  if provider.isNil:
    return err("tsnet provider is nil")
  withLock(provider.stateLock):
    case provider.kind
    of TsnetProviderKind.InAppReal:
      result = tsnetproviderinapp.dialTcpProxy(provider.runtime, family, ip, port)
    of TsnetProviderKind.LegacyBridge:
      result = legacy.dialTcpProxy(provider.bridge, family, ip, port)
    else:
      result = err("tsnet provider does not expose tcp proxy routing")

proc dialTcpProxyExact*(
    provider: TsnetProvider,
    family, ip: string,
    port: int
): Result[MultiAddress, string] =
  if provider.isNil:
    return err("tsnet provider is nil")
  withLock(provider.stateLock):
    case provider.kind
    of TsnetProviderKind.InAppReal:
      result = tsnetproviderinapp.dialTcpProxyExact(provider.runtime, family, ip, port)
    of TsnetProviderKind.LegacyBridge:
      result = legacy.dialTcpProxyExact(provider.bridge, family, ip, port)
    else:
      result = err("tsnet provider does not expose tcp proxy routing")

proc dialUdpProxy*(
    provider: TsnetProvider,
    family, ip: string,
    port: int
): Result[MultiAddress, string] =
  if provider.isNil:
    return err("tsnet provider is nil")
  withLock(provider.stateLock):
    case provider.kind
    of TsnetProviderKind.InAppReal:
      result = tsnetproviderinapp.dialUdpProxy(provider.runtime, family, ip, port)
    of TsnetProviderKind.LegacyBridge:
      result = legacy.dialUdpProxy(provider.bridge, family, ip, port)
    else:
      result = err("tsnet provider does not expose udp proxy routing")

proc dialUdpProxyExact*(
    provider: TsnetProvider,
    family, ip: string,
    port: int
): Result[MultiAddress, string] =
  if provider.isNil:
    return err("tsnet provider is nil")
  withLock(provider.stateLock):
    case provider.kind
    of TsnetProviderKind.InAppReal:
      result = tsnetproviderinapp.dialUdpProxyExact(provider.runtime, family, ip, port)
    of TsnetProviderKind.LegacyBridge:
      result = legacy.dialUdpProxyExact(provider.bridge, family, ip, port)
    else:
      result = err("tsnet provider does not expose udp proxy routing")

proc dialUdpProxyExactTarget*(
    provider: TsnetProvider,
    family, ip: string,
    port: int
): Result[TsnetProxyDialTarget, string] =
  if provider.isNil:
    return err("tsnet provider is nil")
  withLock(provider.stateLock):
    case provider.kind
    of TsnetProviderKind.InAppReal:
      result = tsnetproviderinapp.dialUdpProxyExactTarget(provider.runtime, family, ip, port)
    of TsnetProviderKind.LegacyBridge:
      result = legacy.dialUdpProxyExactTarget(provider.bridge, family, ip, port)
    else:
      result = err("tsnet provider does not expose udp proxy routing")

proc planUdpExactDialTarget*(
    provider: TsnetProvider,
    family, ip: string,
    port: int,
    relayAllowed: bool
): TsnetUdpExactDialPlan =
  if provider.isNil:
    return TsnetUdpExactDialPlan(
      mode:
        if relayAllowed:
          TsnetProxyDialMode.RelayBridge
        else:
          TsnetProxyDialMode.DirectRoute,
      pathKind: "",
      rawAddress: MultiAddress(),
      ready: false,
      stage: TsnetUdpExactDialStage.Unavailable,
      error: "tsnet provider is nil",
      updatedUnixMilli: 0,
    )
  withLock(provider.stateLock):
    case provider.kind
    of TsnetProviderKind.InAppReal:
      result = tsnetproviderinapp.planUdpExactDialTarget(
        provider.runtime,
        family,
        ip,
        port,
        relayAllowed,
      )
    of TsnetProviderKind.LegacyBridge:
      result = legacy.planUdpExactDialTarget(
        provider.bridge,
        family,
        ip,
        port,
        relayAllowed,
      )
    else:
      result = TsnetUdpExactDialPlan(
        mode:
          if relayAllowed:
            TsnetProxyDialMode.RelayBridge
          else:
            TsnetProxyDialMode.DirectRoute,
        pathKind: "",
        rawAddress: MultiAddress(),
        ready: false,
        stage: TsnetUdpExactDialStage.Unavailable,
        error: "tsnet provider does not expose udp proxy routing",
        updatedUnixMilli: 0,
      )

proc lookupUdpDirectRouteTarget*(
    provider: TsnetProvider,
    family, ip: string,
    port: int
): Result[TsnetDirectRouteTarget, string] =
  if provider.isNil:
    return err("tsnet provider does not expose direct route lookup")
  withLock(provider.stateLock):
    if provider.kind == TsnetProviderKind.InAppReal:
      return tsnetproviderinapp.lookupUdpDirectRouteTarget(provider.runtime, family, ip, port)
    return err("tsnet provider does not expose direct route lookup")

proc dialUdpProxyRelayFallback*(
    provider: TsnetProvider,
    family, ip: string,
    port: int
): Result[MultiAddress, string] =
  if provider.isNil:
    return err("tsnet provider is nil")
  withLock(provider.stateLock):
    case provider.kind
    of TsnetProviderKind.InAppReal:
      result = tsnetproviderinapp.dialUdpProxyRelayFallback(provider.runtime, family, ip, port)
    of TsnetProviderKind.LegacyBridge:
      result = legacy.dialUdpProxy(provider.bridge, family, ip, port)
    else:
      result = err("tsnet provider does not expose udp proxy routing")

proc dialUdpProxyRelayFallbackTarget*(
    provider: TsnetProvider,
    family, ip: string,
    port: int
): Result[TsnetProxyDialTarget, string] =
  if provider.isNil:
    return err("tsnet provider is nil")
  withLock(provider.stateLock):
    case provider.kind
    of TsnetProviderKind.InAppReal:
      result = tsnetproviderinapp.dialUdpProxyRelayFallbackTarget(provider.runtime, family, ip, port)
    of TsnetProviderKind.LegacyBridge:
      result = legacy.dialUdpProxyRelayFallbackTarget(provider.bridge, family, ip, port)
    else:
      result = err("tsnet provider does not expose udp proxy routing")

proc markFailedDirectProxyRoute*(
    provider: TsnetProvider,
    advertised: MultiAddress,
    rawAddress: MultiAddress
): Result[void, string] =
  if provider.isNil:
    return err("tsnet provider does not support direct route failure tracking")
  withLock(provider.stateLock):
    if provider.kind != TsnetProviderKind.InAppReal:
      return err("tsnet provider does not support direct route failure tracking")
    result = tsnetproviderinapp.markFailedDirectProxyRoute(provider.runtime, advertised, rawAddress)

proc udpDialState*(
    provider: TsnetProvider,
    rawKey: string,
    routeKey = ""
): tuple[
    known, ready: bool,
    error, phase, detail: string,
    attempts: int,
    updatedUnixMilli: int64
  ] =
  if provider.isNil:
    return
  let snapshot = provider.providerReadSnapshot()
  if snapshot.kind == TsnetProviderKind.InAppReal:
    return tsnetproviderinapp.udpDialState(snapshot.runtime, rawKey, routeKey)

proc resolveRemote*(provider: TsnetProvider, rawAddress: MultiAddress): Result[MultiAddress, string] =
  if provider.isNil:
    return err("tsnet provider does not expose remote resolution")
  let snapshot = provider.providerReadSnapshot()
  case snapshot.kind
  of TsnetProviderKind.InAppReal:
    result = tsnetproviderinapp.resolveRemote(snapshot.runtime, rawAddress)
  of TsnetProviderKind.LegacyBridge:
    result = legacy.resolveRemote(snapshot.bridge, rawAddress)
  else:
    result = err("tsnet provider does not expose remote resolution")
