{.push raises: [].}

import std/[json, strutils]

import ../multiaddress
import ../utility
import ./tsnetproviderbuiltin
import ./tsnetproviderinapp
import ./tsnetproviderlegacy as legacy
import ./tsnetprovidertypes

export tsnetprovidertypes

type
  TsnetProvider* = ref object
    cfg*: TsnetProviderConfig
    kind*: TsnetProviderKind
    started*: bool
    lastError*: string
    runtime: tsnetproviderinapp.TsnetInAppRuntime
    bridge: legacy.TsnetLegacyBridgeHandle

proc new*(_: type[TsnetProvider], cfg: TsnetProviderConfig): TsnetProvider =
  TsnetProvider(
    cfg: cfg,
    kind: TsnetProviderKind.BuiltinSynthetic,
    started: false,
    lastError: ""
  )

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

proc capabilities*(provider: TsnetProvider): TsnetProviderCapabilities {.gcsafe.} =
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

proc isProxyBacked*(provider: TsnetProvider): bool {.gcsafe.} =
  provider.capabilities().proxyBacked

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
  not provider.isNil and provider.started

proc failure*(provider: TsnetProvider): string {.gcsafe.} =
  if provider.isNil:
    return ""
  provider.lastError

proc updateBridgeExtraJson*(provider: TsnetProvider, bridgeExtraJson: string) {.gcsafe.} =
  if provider.isNil:
    return
  provider.cfg.bridgeExtraJson = bridgeExtraJson
  if tsnetproviderinapp.runtimePresent(provider.runtime):
    provider.runtime.cfg.bridgeExtraJson = bridgeExtraJson

proc jsonField(node: JsonNode, key: string): JsonNode {.gcsafe.} =
  if node.isNil or node.kind != JObject or not node.hasKey(key):
    return newJNull()
  node.getOrDefault(key)

proc annotatePayload*(provider: TsnetProvider, payload: JsonNode): JsonNode {.gcsafe.} =
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
  result["providerKind"] = %provider.kindLabel()
  result["providerRequestedBackend"] = %provider.requestedBackendLabel()
  result["providerRuntimeRequested"] = %provider.runtimeRequested()
  result["providerReady"] = %provider.ready()
  let caps = provider.capabilities()
  result["providerInApp"] = %caps.inApp
  result["legacyBridgeDeprecated"] = %(provider.kind == TsnetProviderKind.LegacyBridge)
  result["providerCapabilities"] = caps.toJson()
  if provider.failure().len > 0:
    result["providerError"] = %provider.failure()
  if provider.kind == TsnetProviderKind.LegacyBridge:
    result["providerWarning"] = %TsnetLegacyBridgeWarning

proc start*(provider: TsnetProvider): Result[TsnetProviderKind, string] =
  if provider.isNil:
    return err("tsnet provider is nil")
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
  ok(provider.kind)

proc reset*(provider: TsnetProvider): Result[void, string] =
  if provider.isNil:
    return err("tsnet provider is nil")
  if tsnetproviderinapp.runtimePresent(provider.runtime):
    return tsnetproviderinapp.resetInAppRuntime(provider.runtime)
  if not legacy.bridgeActive(provider.bridge):
    return ok()
  legacy.resetLegacyBridge(provider.bridge)

proc refreshControlMetadata*(provider: TsnetProvider): Result[void, string] {.gcsafe.} =
  if provider.isNil:
    return err("tsnet provider is nil")
  if provider.kind != TsnetProviderKind.InAppReal or not tsnetproviderinapp.runtimePresent(provider.runtime):
    return ok()
  let refreshed = tsnetproviderinapp.refreshControlMetadata(provider.runtime)
  if refreshed.isErr():
    let error = refreshed.error
    provider.lastError = error
    return err(error)
  provider.lastError = ""
  ok()

proc reconcileProxyListeners*(provider: TsnetProvider): Result[void, string] {.gcsafe.} =
  if provider.isNil:
    return err("tsnet provider is nil")
  if provider.kind != TsnetProviderKind.InAppReal or not tsnetproviderinapp.runtimePresent(provider.runtime):
    return ok()
  let repaired = tsnetproviderinapp.reconcileProxyListeners(provider.runtime)
  if repaired.isErr():
    provider.lastError = repaired.error
    return err(repaired.error)
  provider.lastError = ""
  ok()

proc stop*(provider: TsnetProvider) =
  if provider.isNil:
    return
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
  if not provider.capabilities().statusApi:
    return err("tsnet provider is not running a real tailnet backend")
  if provider.kind in {TsnetProviderKind.InAppReal, TsnetProviderKind.InAppUnavailable}:
    let payload = tsnetproviderinapp.statusPayload(provider.runtime).valueOr:
      return err(error)
    return ok(provider.annotatePayload(payload))
  let payload = legacy.statusPayload(provider.bridge).valueOr:
    return err(error)
  ok(provider.annotatePayload(payload))

proc pingPayload*(provider: TsnetProvider, request: JsonNode): Result[JsonNode, string] =
  if provider.isNil:
    return err("tsnet provider is nil")
  if not provider.capabilities().pingApi:
    return err("tailnet ping requires a real tsnet provider")
  if provider.kind in {TsnetProviderKind.InAppReal, TsnetProviderKind.InAppUnavailable}:
    let payload = tsnetproviderinapp.pingPayload(provider.runtime, request).valueOr:
      return err(error)
    return ok(provider.annotatePayload(payload))
  let payload = legacy.pingPayload(provider.bridge, request).valueOr:
    return err(error)
  ok(provider.annotatePayload(payload))

proc derpMapPayload*(provider: TsnetProvider): Result[JsonNode, string] =
  if provider.isNil:
    return err("tsnet provider is nil")
  if provider.kind in {TsnetProviderKind.InAppReal, TsnetProviderKind.InAppUnavailable}:
    let payload = tsnetproviderinapp.derpMapPayload(provider.runtime).valueOr:
      return err(error)
    return ok(provider.annotatePayload(payload))
  if not provider.capabilities().realTailnet:
    return ok(provider.annotatePayload(builtinSyntheticDerpMapPayload()))
  let payload = legacy.derpMapPayload(provider.bridge).valueOr:
    return err(error)
  ok(provider.annotatePayload(payload))

proc listenTcpProxy*(
    provider: TsnetProvider,
    family: string,
    port: int,
    localAddress: MultiAddress
): Result[MultiAddress, string] =
  if not provider.isProxyBacked():
    return err("tsnet provider does not expose tcp proxy routing")
  if provider.kind == TsnetProviderKind.InAppReal:
    return tsnetproviderinapp.listenTcpProxy(provider.runtime, family, port, localAddress)
  legacy.listenTcpProxy(provider.bridge, family, port, localAddress)

proc listenUdpProxy*(
    provider: TsnetProvider,
    family: string,
    port: int,
    localAddress: MultiAddress
): Result[MultiAddress, string] =
  if not provider.isProxyBacked():
    return err("tsnet provider does not expose udp proxy routing")
  if provider.kind == TsnetProviderKind.InAppReal:
    return tsnetproviderinapp.listenUdpProxy(provider.runtime, family, port, localAddress)
  legacy.listenUdpProxy(provider.bridge, family, port, localAddress)

proc dialTcpProxy*(
    provider: TsnetProvider,
    family, ip: string,
    port: int
): Result[MultiAddress, string] =
  if not provider.isProxyBacked():
    return err("tsnet provider does not expose tcp proxy routing")
  if provider.kind == TsnetProviderKind.InAppReal:
    return tsnetproviderinapp.dialTcpProxy(provider.runtime, family, ip, port)
  legacy.dialTcpProxy(provider.bridge, family, ip, port)

proc dialUdpProxy*(
    provider: TsnetProvider,
    family, ip: string,
    port: int
): Result[MultiAddress, string] =
  if not provider.isProxyBacked():
    return err("tsnet provider does not expose udp proxy routing")
  if provider.kind == TsnetProviderKind.InAppReal:
    return tsnetproviderinapp.dialUdpProxy(provider.runtime, family, ip, port)
  legacy.dialUdpProxy(provider.bridge, family, ip, port)

proc dialUdpProxyRelayFallback*(
    provider: TsnetProvider,
    family, ip: string,
    port: int
): Result[MultiAddress, string] =
  if not provider.isProxyBacked():
    return err("tsnet provider does not expose udp proxy routing")
  if provider.kind == TsnetProviderKind.InAppReal:
    return tsnetproviderinapp.dialUdpProxyRelayFallback(provider.runtime, family, ip, port)
  legacy.dialUdpProxy(provider.bridge, family, ip, port)

proc markFailedDirectProxyRoute*(
    provider: TsnetProvider,
    advertised: MultiAddress,
    rawAddress: MultiAddress
): Result[void, string] =
  if provider.isNil or provider.kind != TsnetProviderKind.InAppReal:
    return err("tsnet provider does not support direct route failure tracking")
  tsnetproviderinapp.markFailedDirectProxyRoute(provider.runtime, advertised, rawAddress)

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
  if provider.kind == TsnetProviderKind.InAppReal:
    return tsnetproviderinapp.udpDialState(provider.runtime, rawAddress)

proc resolveRemote*(provider: TsnetProvider, rawAddress: MultiAddress): Result[MultiAddress, string] =
  if not provider.isProxyBacked():
    return err("tsnet provider does not expose remote resolution")
  if provider.kind == TsnetProviderKind.InAppReal:
    return tsnetproviderinapp.resolveRemote(provider.runtime, rawAddress)
  legacy.resolveRemote(provider.bridge, rawAddress)
