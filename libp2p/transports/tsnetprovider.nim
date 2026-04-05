{.push raises: [].}

import std/[json, locks, strutils]
import chronos

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

type
  TsnetProvider* = ref object
    cfg*: TsnetProviderConfig
    kind*: TsnetProviderKind
    started*: bool
    lastError*: string
    stateLock: Lock
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

proc capabilitiesUnlocked(provider: TsnetProvider): TsnetProviderCapabilities {.gcsafe.} =
  if provider.isNil:
    return builtinSyntheticCapabilities()
  case provider.kind
  of TsnetProviderKind.InAppUnavailable:
    if tsnetproviderinapp.runtimePresent(provider.runtime):
      return tsnetproviderinapp.runtimeCapabilities(provider.runtime)
    unavailableCapabilities()
  of TsnetProviderKind.InAppReal:
    if tsnetproviderinapp.runtimeActive(provider.runtime):
      return tsnetproviderinapp.runtimeCapabilities(provider.runtime)
    unavailableCapabilities()
  of TsnetProviderKind.LegacyBridge:
    if legacy.bridgeActive(provider.bridge):
      return legacy.legacyBridgeCapabilities()
    unavailableCapabilities()
  of TsnetProviderKind.BuiltinSynthetic:
    builtinSyntheticCapabilities()

proc capabilities*(provider: TsnetProvider): TsnetProviderCapabilities {.gcsafe.} =
  if provider.isNil:
    return builtinSyntheticCapabilities()
  withLock(provider.stateLock):
    result = capabilitiesUnlocked(provider)

proc isProxyBacked*(provider: TsnetProvider): bool {.gcsafe.} =
  if provider.isNil:
    return false
  withLock(provider.stateLock):
    result = provider.kind == TsnetProviderKind.LegacyBridge

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
  withLock(provider.stateLock):
    result = provider.started

proc listenerNeedsRepair*(provider: TsnetProvider): bool {.gcsafe.} =
  if provider.isNil:
    return false
  withLock(provider.stateLock):
    result =
      case provider.kind
      of TsnetProviderKind.InAppUnavailable, TsnetProviderKind.InAppReal:
        tsnetproviderinapp.listenerNeedsRepair(provider.runtime)
      else:
        false

proc proxyListenersReady*(provider: TsnetProvider): bool {.gcsafe.} =
  if provider.isNil:
    return false
  withLock(provider.stateLock):
    result =
      case provider.kind
      of TsnetProviderKind.InAppUnavailable, TsnetProviderKind.InAppReal:
        tsnetproviderinapp.proxyListenersReady(provider.runtime)
      else:
        false

proc proxyRouteCount*(provider: TsnetProvider): int {.gcsafe.} =
  if provider.isNil:
    return 0
  withLock(provider.stateLock):
    result =
      case provider.kind
      of TsnetProviderKind.InAppUnavailable, TsnetProviderKind.InAppReal:
        tsnetproviderinapp.proxyRouteCount(provider.runtime)
      else:
        0

proc publishedAddrTexts*(provider: TsnetProvider): seq[string] {.gcsafe.} =
  if provider.isNil:
    return @[]
  withLock(provider.stateLock):
    result =
      case provider.kind
      of TsnetProviderKind.InAppUnavailable, TsnetProviderKind.InAppReal:
        tsnetproviderinapp.publishedAddrTexts(provider.runtime)
      else:
        @[]

proc failure*(provider: TsnetProvider): string {.gcsafe.} =
  if provider.isNil:
    return ""
  withLock(provider.stateLock):
    result = provider.lastError

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

proc annotatePayloadUnlocked(provider: TsnetProvider, payload: JsonNode): JsonNode {.gcsafe.} =
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
  result["providerKind"] = %kindLabel(provider.kind)
  result["providerRequestedBackend"] = %provider.requestedBackendLabel()
  result["providerRuntimeRequested"] = %provider.runtimeRequested()
  result["providerReady"] = %provider.started
  let caps = capabilitiesUnlocked(provider)
  result["providerInApp"] = %caps.inApp
  result["legacyBridgeDeprecated"] = %(provider.kind == TsnetProviderKind.LegacyBridge)
  result["providerCapabilities"] = caps.toJson()
  if provider.lastError.len > 0:
    result["providerError"] = %provider.lastError
  if provider.kind == TsnetProviderKind.LegacyBridge:
    result["providerWarning"] = %TsnetLegacyBridgeWarning

proc annotatePayload*(provider: TsnetProvider, payload: JsonNode): JsonNode {.gcsafe.} =
  if provider.isNil:
    return annotatePayloadUnlocked(provider, payload)
  withLock(provider.stateLock):
    result = annotatePayloadUnlocked(provider, payload)

proc start*(provider: TsnetProvider): Result[TsnetProviderKind, string] =
  if provider.isNil:
    return err("tsnet provider is nil")
  withLock(provider.stateLock):
    if provider.started:
      return ok(provider.kind)
    provider.lastError = ""
    if legacy.bridgeActive(provider.bridge):
      provider.started = true
      provider.kind = TsnetProviderKind.LegacyBridge
      return ok(provider.kind)
    if not provider.runtimeRequested():
      provider.started = true
      provider.kind = TsnetProviderKind.BuiltinSynthetic
      return ok(provider.kind)
    if not provider.legacyBridgeRequested():
      provider.runtime = tsnetproviderinapp.newInAppRuntime(provider.cfg)
      let started = tsnetproviderinapp.startInAppRuntime(provider.runtime)
      if started.isErr():
        provider.started = false
        provider.kind = TsnetProviderKind.InAppUnavailable
        provider.lastError = started.error
        return err(provider.lastError)
      provider.started = true
      provider.lastError = ""
      provider.kind = TsnetProviderKind.InAppReal
      return ok(provider.kind)
    let opened = legacy.openLegacyBridge(provider.cfg).valueOr:
      provider.started = false
      provider.kind = TsnetProviderKind.LegacyBridge
      provider.lastError =
        "no Nim in-app tsnet provider is available yet; legacy bridge load failed: " &
        error
      return err(provider.lastError)
    provider.bridge = opened
    provider.started = true
    provider.lastError = ""
    provider.kind = TsnetProviderKind.LegacyBridge
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
  withLock(provider.stateLock):
    if not capabilitiesUnlocked(provider).statusApi:
      return err("tsnet provider is not running a real tailnet backend")
    if provider.kind in {TsnetProviderKind.InAppReal, TsnetProviderKind.InAppUnavailable}:
      let payload = tsnetproviderinapp.statusPayload(provider.runtime).valueOr:
        return err(error)
      return ok(annotatePayloadUnlocked(provider, payload))
    let payload = legacy.statusPayload(provider.bridge).valueOr:
      return err(error)
    result = ok(annotatePayloadUnlocked(provider, payload))

proc pingPayload*(provider: TsnetProvider, request: JsonNode): Result[JsonNode, string] =
  if provider.isNil:
    return err("tsnet provider is nil")
  withLock(provider.stateLock):
    if not capabilitiesUnlocked(provider).pingApi:
      return err("tailnet ping requires a real tsnet provider")
    if provider.kind in {TsnetProviderKind.InAppReal, TsnetProviderKind.InAppUnavailable}:
      let payload = tsnetproviderinapp.pingPayload(provider.runtime, request).valueOr:
        return err(error)
      return ok(annotatePayloadUnlocked(provider, payload))
    let payload = legacy.pingPayload(provider.bridge, request).valueOr:
      return err(error)
    result = ok(annotatePayloadUnlocked(provider, payload))

proc derpMapPayload*(provider: TsnetProvider): Result[JsonNode, string] =
  if provider.isNil:
    return err("tsnet provider is nil")
  withLock(provider.stateLock):
    if provider.kind in {TsnetProviderKind.InAppReal, TsnetProviderKind.InAppUnavailable}:
      let payload = tsnetproviderinapp.derpMapPayload(provider.runtime).valueOr:
        return err(error)
      return ok(annotatePayloadUnlocked(provider, payload))
    if not capabilitiesUnlocked(provider).realTailnet:
      return ok(annotatePayloadUnlocked(provider, builtinSyntheticDerpMapPayload()))
    let payload = legacy.derpMapPayload(provider.bridge).valueOr:
      return err(error)
    result = ok(annotatePayloadUnlocked(provider, payload))

proc listenTcpProxy*(
    provider: TsnetProvider,
    family: string,
    port: int,
    localAddress: MultiAddress
): Result[MultiAddress, string] =
  if provider.isNil:
    return err("tsnet provider is nil")
  withLock(provider.stateLock):
    if provider.kind != TsnetProviderKind.LegacyBridge:
      return err("tsnet provider does not expose tcp proxy routing")
    result = legacy.listenTcpProxy(provider.bridge, family, port, localAddress)

proc listenUdpProxy*(
    provider: TsnetProvider,
    family: string,
    port: int,
    localAddress: MultiAddress
): Result[MultiAddress, string] =
  if provider.isNil:
    return err("tsnet provider is nil")
  withLock(provider.stateLock):
    if provider.kind != TsnetProviderKind.LegacyBridge:
      return err("tsnet provider does not expose udp proxy routing")
    result = legacy.listenUdpProxy(provider.bridge, family, port, localAddress)

proc dialTcpProxy*(
    provider: TsnetProvider,
    family, ip: string,
    port: int
): Result[MultiAddress, string] =
  if provider.isNil:
    return err("tsnet provider is nil")
  withLock(provider.stateLock):
    if provider.kind != TsnetProviderKind.LegacyBridge:
      return err("tsnet provider does not expose tcp proxy routing")
    result = legacy.dialTcpProxy(provider.bridge, family, ip, port)

proc dialTcpProxyExact*(
    provider: TsnetProvider,
    family, ip: string,
    port: int
): Result[MultiAddress, string] =
  if provider.isNil:
    return err("tsnet provider is nil")
  withLock(provider.stateLock):
    if provider.kind != TsnetProviderKind.LegacyBridge:
      return err("tsnet provider does not expose tcp proxy routing")
    result = legacy.dialTcpProxyExact(provider.bridge, family, ip, port)

proc dialUdpProxy*(
    provider: TsnetProvider,
    family, ip: string,
    port: int
): Result[MultiAddress, string] =
  if provider.isNil:
    return err("tsnet provider is nil")
  withLock(provider.stateLock):
    if provider.kind != TsnetProviderKind.LegacyBridge:
      return err("tsnet provider does not expose udp proxy routing")
    result = legacy.dialUdpProxy(provider.bridge, family, ip, port)

proc dialUdpProxyExact*(
    provider: TsnetProvider,
    family, ip: string,
    port: int
): Result[MultiAddress, string] =
  if provider.isNil:
    return err("tsnet provider is nil")
  withLock(provider.stateLock):
    if provider.kind != TsnetProviderKind.LegacyBridge:
      return err("tsnet provider does not expose udp proxy routing")
    result = legacy.dialUdpProxyExact(provider.bridge, family, ip, port)

proc dialUdpProxyExactTarget*(
    provider: TsnetProvider,
    family, ip: string,
    port: int
): Result[TsnetProxyDialTarget, string] =
  if provider.isNil:
    return err("tsnet provider is nil")
  withLock(provider.stateLock):
    if provider.kind != TsnetProviderKind.LegacyBridge:
      return err("tsnet provider does not expose udp proxy routing")
    result = legacy.dialUdpProxyExactTarget(provider.bridge, family, ip, port)

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
    if provider.kind != TsnetProviderKind.LegacyBridge:
      return err("tsnet provider does not expose udp proxy routing")
    result = legacy.dialUdpProxy(provider.bridge, family, ip, port)

proc dialUdpProxyRelayFallbackTarget*(
    provider: TsnetProvider,
    family, ip: string,
    port: int
): Result[TsnetProxyDialTarget, string] =
  if provider.isNil:
    return err("tsnet provider is nil")
  withLock(provider.stateLock):
    if provider.kind != TsnetProviderKind.LegacyBridge:
      return err("tsnet provider does not expose udp proxy routing")
    result = legacy.dialUdpProxyRelayFallbackTarget(provider.bridge, family, ip, port)

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
    rawAddress: MultiAddress
): tuple[
    known, ready: bool,
    error, phase, detail: string,
    attempts: int,
    updatedUnixMilli: int64
  ] =
  if provider.isNil:
    return
  withLock(provider.stateLock):
    if provider.kind == TsnetProviderKind.InAppReal:
      return tsnetproviderinapp.udpDialState(provider.runtime, rawAddress)

proc resolveRemote*(provider: TsnetProvider, rawAddress: MultiAddress): Result[MultiAddress, string] =
  if provider.isNil:
    return err("tsnet provider does not expose remote resolution")
  withLock(provider.stateLock):
    if provider.kind != TsnetProviderKind.LegacyBridge:
      return err("tsnet provider does not expose remote resolution")
    result = legacy.resolveRemote(provider.bridge, rawAddress)
