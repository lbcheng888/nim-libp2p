# Nim-LibP2P
# Copyright (c) 2023-2024 Status Research & Development GmbH
# Licensed under either of
#  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
#  * MIT license ([LICENSE-MIT](LICENSE-MIT))
# at your option.
# This file may not be copied, modified, or distributed except according to
# those terms.

{.push raises: [].}

import std/[json, locks, strutils, tables]
from std/times import epochTime
import chronos, chronicles

import ../crypto/crypto
import ../multiaddress
import ../multicodec
import ../muxers/muxer
import ../peerid
import ../stream/connection
import ../upgrademngrs/upgrade
import ../utility
import ./transport
import ./tcptransport
import ./tsnetprovider

when defined(libp2p_msquic_experimental):
  import ./msquictransport

logScope:
  topics = "libp2p tsnettransport"

template tsnetSafe(body: untyped) =
  {.cast(gcsafe).}:
    body

proc ownedText(text: string): string {.gcsafe, raises: [].} =
  if text.len == 0:
    return ""
  result = newStringOfCap(text.len)
  result.add(text)

proc currentUnixMilli(): int64 =
  int64(epochTime() * 1000.0)

type
  TsnetPathKind* {.pure.} = enum
    Tcp
    Quic

  TsnetTransportError* = object of transport.TransportError
  TsnetTransportDialError* = object of transport.TransportDialError

  TsnetStatusSnapshot* = object
    tailnetIPs*: seq[string]
    tailnetPath*: string
    tailnetRelay*: string
    tailnetDerpMapSummary*: string
    tailnetPeers*: seq[string]

  TsnetTransportBuilderConfig* = object
    controlUrl*: string
    controlProtocol*: string
    controlEndpoint*: string
    relayEndpoint*: string
    authKey*: string
    hostname*: string
    stateDir*: string
    wireguardPort*: int
    bridgeLibraryPath*: string
    logLevel*: string
    enableDebug*: bool
    bridgeExtraJson*: string
    when defined(libp2p_msquic_experimental):
      quicConfig*: MsQuicTransportConfig
      quicTlsTempDir*: string

  TsnetParsedAddress = object
    family: string
    ip: string
    port: int
    kind: TsnetPathKind
    explicitIP: bool
    raw: MultiAddress
    full: MultiAddress

  TsnetListenerMapping = object
    published: bool
    raw: MultiAddress
    advertised: MultiAddress
    rawText: string
    advertisedText: string

  TsnetTransport* = ref object of Transport
    cfg*: TsnetTransportBuilderConfig
    tcpTransport: TcpTransport
    when defined(libp2p_msquic_experimental):
      quicTransport: MsQuicTransport
    provider: TsnetProvider
    providerListenersActivated: bool
    acceptFuts: seq[Future[Connection]]
    acceptKinds: seq[TsnetPathKind]
    listenerMappings: seq[TsnetListenerMapping]
    syntheticIPv4: string
    syntheticIPv6: string

var tsnetRegistryLock {.global.}: Lock
var tsnetSyntheticNodeCounter {.global.} = 0
var tsnetAdvertisedToRaw {.global.}: Table[string, string] = initTable[string, string]()

initLock(tsnetRegistryLock)

proc listenerMappingSnapshot(
    mappings: seq[TsnetListenerMapping]
): seq[TsnetListenerMapping] {.gcsafe.} =
  result = newSeqOfCap[TsnetListenerMapping](mappings.len)
  for mapping in mappings.items():
    result.add(mapping)

proc listenerMappingCount(self: TsnetTransport): int {.gcsafe.} =
  if self.isNil:
    return 0
  self.listenerMappings.len

proc init*(_: type[TsnetTransportBuilderConfig]): TsnetTransportBuilderConfig =
  result = TsnetTransportBuilderConfig(
    controlUrl: "",
    controlProtocol: TsnetControlProtocolAuto,
    controlEndpoint: "",
    relayEndpoint: "",
    authKey: "",
    hostname: "",
    stateDir: "",
    wireguardPort: 0,
    bridgeLibraryPath: "",
    logLevel: "",
    enableDebug: false,
    bridgeExtraJson: ""
  )
  when defined(libp2p_msquic_experimental):
    result.quicConfig = MsQuicTransportConfig()
    result.quicTlsTempDir = ""

proc providerConfig(cfg: TsnetTransportBuilderConfig): TsnetProviderConfig =
  TsnetProviderConfig(
    controlUrl: cfg.controlUrl,
    controlProtocol: cfg.controlProtocol,
    controlEndpoint: cfg.controlEndpoint,
    relayEndpoint: cfg.relayEndpoint,
    authKey: cfg.authKey,
    hostname: cfg.hostname,
    stateDir: cfg.stateDir,
    wireguardPort: cfg.wireguardPort,
    bridgeLibraryPath: cfg.bridgeLibraryPath,
    logLevel: cfg.logLevel,
    enableDebug: cfg.enableDebug,
    bridgeExtraJson: ownedText(cfg.bridgeExtraJson)
  )

proc mergeBridgeExtraJson(
    current: string,
    peerId: string,
    listenAddrs: openArray[string]
): string =
  var payload =
    if current.strip().len > 0:
      try:
        parseJson(current)
      except CatchableError:
        newJObject()
    else:
      newJObject()
  if payload.kind != JObject:
    payload = newJObject()
  let normalizedPeerId = peerId.strip()
  if normalizedPeerId.len > 0:
    payload["libp2pPeerId"] = %normalizedPeerId
    payload["localPeerId"] = %normalizedPeerId
  var normalizedListenAddrs: seq[string] = @[]
  for addr in listenAddrs:
    let text = addr.strip()
    if text.len > 0 and text notin normalizedListenAddrs:
      normalizedListenAddrs.add(text)
  if normalizedListenAddrs.len > 0:
    payload["libp2pListenAddrs"] = %normalizedListenAddrs
    payload["listenAddresses"] = %normalizedListenAddrs
  $payload

proc parseKnownAddress(text: string): MultiAddress {.gcsafe.} =
  MultiAddress.init(text).expect("tsnet transport constructed a valid multiaddr")

proc jsonField(node: JsonNode, key: string): JsonNode =
  if node.isNil or node.kind != JObject or not node.hasKey(key):
    return newJNull()
  node.getOrDefault(key)

proc jsonFieldStr(node: JsonNode, key, defaultValue: string = ""): string =
  let value = jsonField(node, key)
  if value.isNil or value.kind == JNull:
    return defaultValue
  if value.kind == JString:
    return value.getStr()
  $value

proc jsonFieldBool(node: JsonNode, key: string, defaultValue = false): bool =
  let value = jsonField(node, key)
  if value.isNil:
    return defaultValue
  case value.kind
  of JBool:
    value.getBool()
  of JString:
    value.getStr().strip().toLowerAscii() in ["1", "true", "yes", "y"]
  else:
    defaultValue

proc jsonFieldStrings(node: JsonNode, key: string): seq[string] =
  let value = jsonField(node, key)
  if value.isNil or value.kind != JArray:
    return @[]
  for item in value:
    if item.kind == JString and item.getStr().strip().len > 0:
      result.add(item.getStr().strip())

proc nextSyntheticNodeId(): int {.gcsafe.} =
  tsnetSafe:
    withLock(tsnetRegistryLock):
      inc tsnetSyntheticNodeCounter
      result = tsnetSyntheticNodeCounter

proc syntheticTailnetIPv4(id: int): string {.gcsafe.} =
  let hi = (id shr 8) and 0xFF
  let lo = id and 0xFF
  "100.64." & $hi & "." & $lo

proc syntheticTailnetIPv6(id: int): string {.gcsafe.} =
  "fd7a:115c:a1e0::" & toHex(id and 0xFFFF, 4).toLowerAscii()

proc providerUsesProxyRouting(self: TsnetTransport): bool {.gcsafe.} =
  tsnetSafe:
    result = not self.provider.isNil and self.provider.isProxyBacked()

proc providerCapabilitiesSafe(self: TsnetTransport): TsnetProviderCapabilities {.gcsafe.} =
  tsnetSafe:
    result =
      if self.provider.isNil:
        capabilities(TsnetProviderKind.BuiltinSynthetic)
      else:
        self.provider.capabilities()

proc providerListenerNeedsRepairSafe(self: TsnetTransport): bool {.gcsafe.} =
  tsnetSafe:
    result = not self.provider.isNil and self.provider.listenerNeedsRepair()

proc providerProxyRouteCountSafe(self: TsnetTransport): int {.gcsafe.} =
  tsnetSafe:
    result =
      if self.provider.isNil:
        0
      else:
        self.provider.proxyRouteCount()

proc providerPublishedAddrTextsSafe(self: TsnetTransport): seq[string] {.gcsafe.} =
  tsnetSafe:
    result =
      if self.provider.isNil:
        @[]
      else:
        self.provider.publishedAddrTexts()

proc parseTsnetAddress(
    address: MultiAddress, listen: bool
): Result[TsnetParsedAddress, string] {.gcsafe.}
proc providerListenTcpSafe(
    self: TsnetTransport,
    family: string,
    port: int,
    localAddress: MultiAddress
): Result[MultiAddress, string] {.gcsafe.}
proc providerListenUdpSafe(
    self: TsnetTransport,
    family: string,
    port: int,
    localAddress: MultiAddress
): Result[MultiAddress, string] {.gcsafe.}

proc refreshListenerMapping(
    transport: TsnetTransport,
    index: int,
    advertised: MultiAddress
): Result[void, string] {.gcsafe.} =
  tsnetSafe:
    var nextMappings = listenerMappingSnapshot(transport.listenerMappings)
    if index < 0 or index >= nextMappings.len:
      return err("tsnet listener mapping index out of range")
    let raw = nextMappings[index].raw
    let advertisedKey = $advertised
    let rawKey =
      if nextMappings[index].rawText.len > 0:
        nextMappings[index].rawText
      else:
        $raw
    let oldAdvertisedKey =
      if nextMappings[index].advertisedText.len > 0:
        nextMappings[index].advertisedText
      else:
        $nextMappings[index].advertised
    withLock(tsnetRegistryLock):
      let existing = tsnetAdvertisedToRaw.getOrDefault(advertisedKey)
      if existing.len > 0 and existing != rawKey:
        return err("duplicate /tsnet listener registration: " & advertisedKey)
      if nextMappings[index].published and oldAdvertisedKey != advertisedKey:
        if tsnetAdvertisedToRaw.getOrDefault(oldAdvertisedKey) == rawKey:
          tsnetAdvertisedToRaw.del(oldAdvertisedKey)
      tsnetAdvertisedToRaw[advertisedKey] = rawKey
    nextMappings[index].published = true
    nextMappings[index].advertised = advertised
    nextMappings[index].advertisedText = ownedText(advertisedKey)
    nextMappings[index].rawText = ownedText(rawKey)
    transport.listenerMappings = nextMappings
  ok()

proc activateProviderListeners(
    self: TsnetTransport
): Result[void, string] {.gcsafe.} =
  tsnetSafe:
    if self.isNil:
      return err("tsnet transport is nil")
    if self.providerListenersActivated or not self.providerUsesProxyRouting():
      return ok()
    var activated: seq[MultiAddress] = @[]
    let mappings = listenerMappingSnapshot(self.listenerMappings)
    for idx, mapping in mappings:
      let parsed = parseTsnetAddress(mapping.advertised, listen = true).valueOr:
        return err(error)
      let upgraded =
        case parsed.kind
        of TsnetPathKind.Tcp:
          self.providerListenTcpSafe(parsed.family, parsed.port, mapping.raw)
        of TsnetPathKind.Quic:
          self.providerListenUdpSafe(parsed.family, parsed.port, mapping.raw)
      let advertised = upgraded.valueOr:
        return err(error)
      let refreshed = self.refreshListenerMapping(idx, advertised)
      if refreshed.isErr():
        return err(refreshed.error)
      activated.add(advertised)
    if self.running:
      self.addrs = activated
    self.providerListenersActivated = true
  ok()

proc prewarmProvider*(self: TsnetTransport): Result[TsnetProviderKind, string] {.gcsafe.} =
  tsnetSafe:
    result = self.provider.start()

proc providerStartSafe(self: TsnetTransport): Result[TsnetProviderKind, string] {.gcsafe.} =
  tsnetSafe:
    let started = self.provider.start().valueOr:
      return err(error)
    if self.providerUsesProxyRouting():
      let activated = self.activateProviderListeners()
      if activated.isErr():
        return err(activated.error)
    result = ok(started)

proc warmProvider*(self: TsnetTransport): Result[TsnetProviderKind, string] {.gcsafe.} =
  self.providerStartSafe()

proc publishLocalPeerInfo*(
    self: TsnetTransport,
    peerId: string,
    listenAddrs: openArray[string]
) {.gcsafe.} =
  if self.isNil:
    return
  let nextJson = mergeBridgeExtraJson(self.cfg.bridgeExtraJson, peerId, listenAddrs)
  let ownedJson = ownedText(nextJson)
  self.cfg.bridgeExtraJson = ownedJson
  self.provider.updateBridgeExtraJson(ownedJson)

proc refreshProviderControlMetadata*(self: TsnetTransport): Result[void, string] {.gcsafe.} =
  tsnetSafe:
    if self.isNil:
      return err("tsnet transport is nil")
    result = self.provider.refreshControlMetadata()

proc reconcileProviderListeners*(
    self: TsnetTransport
): Future[Result[void, string]] {.async: (raises: [CancelledError]).} =
  tsnetSafe:
    if self.isNil:
      return err("tsnet transport is nil")
    if not self.providerUsesProxyRouting():
      return ok()
    let repaired = await self.provider.reconcileProxyListeners()
    if repaired.isErr():
      return err(repaired.error)
    let routeRegistryMissing =
      self.listenerMappingCount() > 0 and self.providerProxyRouteCountSafe() == 0
    if routeRegistryMissing:
      # Only republish from the transport-side authoritative mapping when the
      # provider-side proxy-route registry is truly empty. Inner relay listeners
      # can legitimately self-heal while readiness is temporarily below expected.
      self.providerListenersActivated = false
      let reactivated = self.activateProviderListeners()
      if reactivated.isErr():
        return err(reactivated.error)
    self.providerListenersActivated = true
    result = ok()

proc publishedAddrs*(self: TsnetTransport): seq[MultiAddress] {.gcsafe.} =
  if self.isNil:
    return @[]
  if self.providerUsesProxyRouting():
    let providerPublished = self.providerPublishedAddrTextsSafe()
    if providerPublished.len > 0:
      result = newSeqOfCap[MultiAddress](providerPublished.len)
      for route in providerPublished:
        if route.len > 0:
          result.add(parseKnownAddress(route))
      if result.len > 0:
        return
  let mappings = listenerMappingSnapshot(self.listenerMappings)
  for mapping in mappings:
    if mapping.published:
      result.add(mapping.advertised)
  if result.len == 0:
    result = self.addrs

proc publishedAddrTexts*(self: TsnetTransport): seq[string] {.gcsafe.} =
  if self.isNil:
    return @[]
  if self.providerUsesProxyRouting():
    let providerPublished = self.providerPublishedAddrTextsSafe()
    if providerPublished.len > 0:
      return providerPublished
  let mappings = listenerMappingSnapshot(self.listenerMappings)
  for mapping in mappings:
    if mapping.published and mapping.advertisedText.len > 0:
      result.add(ownedText(mapping.advertisedText))

proc providerResetSafe(self: TsnetTransport) {.gcsafe.} =
  tsnetSafe:
    discard self.provider.reset()

proc providerStopSafe(self: TsnetTransport) {.gcsafe.} =
  tsnetSafe:
    self.provider.stop()

proc deferredStatusPayload(
    self: TsnetTransport,
    reason: string
): JsonNode {.gcsafe.} =
  self.provider.annotatePayload(%*{
    "ok": false,
    "deferred": true,
    "providerStage": "deferred",
    "reason": reason,
    "started": self.running,
    "tailnetIPs": newJArray(),
    "tailnetPath": "",
    "tailnetRelay": "",
    "tailnetDerpMapSummary": "",
    "tailnetPeers": newJArray(),
  })

proc providerListenTcpSafe(
    self: TsnetTransport,
    family: string,
    port: int,
    localAddress: MultiAddress
): Result[MultiAddress, string] {.gcsafe.} =
  tsnetSafe:
    result = self.provider.listenTcpProxy(family, port, localAddress)

proc providerListenUdpSafe(
    self: TsnetTransport,
    family: string,
    port: int,
    localAddress: MultiAddress
): Result[MultiAddress, string] {.gcsafe.} =
  tsnetSafe:
    result = self.provider.listenUdpProxy(family, port, localAddress)

proc providerDialTcpSafe(
    self: TsnetTransport,
    family, ip: string,
    port: int
): Result[MultiAddress, string] {.gcsafe.} =
  tsnetSafe:
    result = self.provider.dialTcpProxy(family, ip, port)

proc providerDialTcpExactSafe(
    self: TsnetTransport,
    family, ip: string,
    port: int
): Result[MultiAddress, string] {.gcsafe.} =
  tsnetSafe:
    result = self.provider.dialTcpProxyExact(family, ip, port)

proc providerDialUdpSafe(
    self: TsnetTransport,
    family, ip: string,
    port: int
): Result[MultiAddress, string] {.gcsafe.} =
  tsnetSafe:
    result = self.provider.dialUdpProxy(family, ip, port)

proc providerDialUdpExactSafe(
    self: TsnetTransport,
    family, ip: string,
    port: int
): Result[MultiAddress, string] {.gcsafe.} =
  tsnetSafe:
    result = self.provider.dialUdpProxyExact(family, ip, port)

proc providerDialUdpExactTargetSafe(
    self: TsnetTransport,
    family, ip: string,
    port: int
): Result[TsnetProxyDialTarget, string] {.gcsafe.} =
  tsnetSafe:
    result = self.provider.dialUdpProxyExactTarget(family, ip, port)

proc providerDialUdpFallbackSafe(
    self: TsnetTransport,
    family, ip: string,
    port: int
): Result[MultiAddress, string] {.gcsafe.} =
  tsnetSafe:
    result = self.provider.dialUdpProxyRelayFallback(family, ip, port)

proc providerDialUdpFallbackTargetSafe(
    self: TsnetTransport,
    family, ip: string,
    port: int
): Result[TsnetProxyDialTarget, string] {.gcsafe.} =
  tsnetSafe:
    result = self.provider.dialUdpProxyRelayFallbackTarget(family, ip, port)

proc providerMarkFailedDirectUdpSafe(
    self: TsnetTransport,
    advertised: MultiAddress,
    rawAddress: MultiAddress
): Result[void, string] {.gcsafe.} =
  tsnetSafe:
    result = self.provider.markFailedDirectProxyRoute(advertised, rawAddress)

proc providerUdpDialStateSafe(
    self: TsnetTransport,
    rawAddress: MultiAddress
): tuple[
    known, ready: bool,
    error, phase, detail: string,
    attempts: int,
    updatedUnixMilli: int64
  ] {.gcsafe.} =
  tsnetSafe:
    result = self.provider.udpDialState(rawAddress)

proc providerResolveRemoteSafe(
    self: TsnetTransport,
    rawAddress: MultiAddress
): Result[MultiAddress, string] {.gcsafe.} =
  tsnetSafe:
    result = self.provider.resolveRemote(rawAddress)

proc providerStatusPayloadSafe(self: TsnetTransport): Result[JsonNode, string] {.gcsafe.} =
  tsnetSafe:
    result = self.provider.statusPayload()

proc providerReadySafe*(self: TsnetTransport): bool {.gcsafe.} =
  tsnetSafe:
    result = not self.isNil and not self.provider.isNil and self.provider.ready()

proc proxyListenersReadySafe*(self: TsnetTransport): bool {.gcsafe.} =
  tsnetSafe:
    result = not self.isNil and not self.provider.isNil and
      self.provider.proxyListenersReady()

proc providerPingPayloadSafe(
    self: TsnetTransport,
    payload: JsonNode
): Result[JsonNode, string] {.gcsafe.} =
  tsnetSafe:
    result = self.provider.pingPayload(payload)

proc providerDerpMapPayloadSafe(self: TsnetTransport): Result[JsonNode, string] {.gcsafe.} =
  tsnetSafe:
    result = self.provider.derpMapPayload()

proc waitForProviderUdpDialReady(
    self: TsnetTransport,
    dialTarget: TsnetProxyDialTarget
) {.async: (raises: [CancelledError, transport.TransportError]).} =
  const
    pollStep = 25.milliseconds
    dialReadyTimeout = 30.seconds
    dialProgressStallNoticeMs = 3_000'i64

  if dialTarget.mode != TsnetProxyDialMode.RelayBridge:
    return

  let rawAddress = dialTarget.rawAddress
  let deadline = Moment.now() + dialReadyTimeout
  var lastPhase = ""
  var lastDetail = ""
  var lastAttempts = -1
  var lastUpdate = int64(0)
  var lastStallNoticeUpdate = int64(-1)
  while true:
    let state = self.providerUdpDialStateSafe(rawAddress)
    if not state.known:
      if Moment.now() >= deadline:
        raise newException(
          TsnetTransportDialError,
          "timeout waiting for tsnet udp relay dial state for " & $rawAddress &
            " mode=relay_bridge"
        )
      await sleepAsync(pollStep)
      continue
    if state.ready:
      return
    if state.phase.len > 0 and (
        state.phase != lastPhase or
        state.detail != lastDetail or
        state.attempts != lastAttempts or
        state.updatedUnixMilli != lastUpdate
      ):
      trace "tsnet udp dial progress",
        rawAddress = $rawAddress,
        phase = state.phase,
        detail = state.detail,
        attempts = state.attempts
      lastPhase = state.phase
      lastDetail = state.detail
      lastAttempts = state.attempts
      lastUpdate = state.updatedUnixMilli
      lastStallNoticeUpdate = int64(-1)
    if state.error.len > 0:
      raise newException(
        TsnetTransportDialError,
        "tsnet udp relay dial failed for " & $rawAddress & ": " & state.error &
          (if state.phase.len > 0: " (phase=" & state.phase & ")" else: "") &
          (if state.attempts > 0: " attempts=" & $state.attempts else: "")
      )
    if state.updatedUnixMilli > 0 and state.phase.len > 0:
      let ageMs = currentUnixMilli() - state.updatedUnixMilli
      if ageMs >= dialProgressStallNoticeMs and
          state.updatedUnixMilli != lastStallNoticeUpdate:
        trace "tsnet udp dial stalled",
          rawAddress = $rawAddress,
          phase = state.phase,
          detail = state.detail,
          attempts = state.attempts,
          stallMs = ageMs
        lastStallNoticeUpdate = state.updatedUnixMilli
    if Moment.now() >= deadline:
      let extra =
        if state.phase.len == 0:
          ""
        else:
          " lastPhase=" & state.phase &
            (if state.detail.len > 0: " detail=" & state.detail else: "") &
            (if state.attempts > 0: " attempts=" & $state.attempts else: "") &
            (if state.updatedUnixMilli > 0:
              " ageMs=" & $(max(int64(0), currentUnixMilli() - state.updatedUnixMilli))
             else: "")
      raise newException(
        TsnetTransportDialError,
        "timeout waiting for tsnet udp relay readiness for " & $rawAddress & extra
      )
    await sleepAsync(pollStep)

proc hasTsnetSuffix(address: MultiAddress): bool {.gcsafe.} =
  let text = $address
  let lower = text.toLowerAscii()
  if lower.len <= 6:
    return false
  if lower.endsWith("/tsnet"):
    return true
  for marker in ["/p2p/", "/ipfs/"]:
    let idx = lower.rfind(marker)
    if idx >= 0 and idx > 6:
      return lower[0 ..< idx].endsWith("/tsnet")
  false

proc stripTsnetSuffix(address: MultiAddress): Result[MultiAddress, string] {.gcsafe.} =
  let text = $address
  var baseText = text
  let lower = text.toLowerAscii()
  for marker in ["/p2p/", "/ipfs/"]:
    let idx = lower.rfind(marker)
    if idx >= 0:
      baseText = text[0 ..< idx]
      break
  if not baseText.toLowerAscii().endsWith("/tsnet"):
    return err("address is missing /tsnet: " & text)
  if baseText.len <= 6:
    return err("invalid /tsnet address: " & text)
  let rawText = baseText[0 ..< baseText.len - 6]
  MultiAddress.init(rawText)

proc splitAddressParts(address: MultiAddress): seq[string] {.gcsafe.} =
  for part in ($address).split('/'):
    if part.len > 0:
      result.add(part)

proc tsnetAddressBaseIndex(parts: seq[string]): int {.gcsafe.} =
  if parts.len > 0 and parts[0].toLowerAscii() in ["awdl", "nan", "nearlink"]:
    return 1
  0

proc portFromAddress(address: MultiAddress): int {.gcsafe.} =
  let parts = splitAddressParts(address)
  var idx = 0
  while idx + 1 < parts.len:
    if parts[idx] == "tcp" or parts[idx] == "udp":
      try:
        return parseInt(parts[idx + 1])
      except ValueError:
        return 0
    inc idx
  0

proc familyFromAddress(address: MultiAddress): string {.gcsafe.} =
  let parts = splitAddressParts(address)
  let baseIdx = tsnetAddressBaseIndex(parts)
  if parts.len >= baseIdx + 2 and (parts[baseIdx] == "ip4" or parts[baseIdx] == "ip6"):
    return parts[baseIdx]
  ""

proc hostFromAddress(address: MultiAddress): string {.gcsafe.} =
  let parts = splitAddressParts(address)
  let baseIdx = tsnetAddressBaseIndex(parts)
  if parts.len >= baseIdx + 2 and (parts[baseIdx] == "ip4" or parts[baseIdx] == "ip6"):
    return parts[baseIdx + 1]
  ""

proc quicDialHostname*(
    self: TsnetTransport,
    requestedHostname: string,
    rawTarget: MultiAddress
): string =
  if self.providerUsesProxyRouting():
    let rawHost = hostFromAddress(rawTarget)
    if rawHost.len > 0:
      return rawHost
    return ""
  requestedHostname

proc buildTsnetAddress(
    family, ip: string, port: int, kind: TsnetPathKind
): MultiAddress {.gcsafe.} =
  case kind
  of TsnetPathKind.Tcp:
    parseKnownAddress("/" & family & "/" & ip & "/tcp/" & $port & "/tsnet")
  of TsnetPathKind.Quic:
    parseKnownAddress("/" & family & "/" & ip & "/udp/" & $port & "/quic-v1/tsnet")

proc buildRawLoopbackAddress(parsed: TsnetParsedAddress): MultiAddress {.gcsafe.} =
  let host =
    if parsed.family == "ip6":
      "::1"
    else:
      "127.0.0.1"
  case parsed.kind
  of TsnetPathKind.Tcp:
    parseKnownAddress("/" & parsed.family & "/" & host & "/tcp/0")
  of TsnetPathKind.Quic:
    parseKnownAddress("/" & parsed.family & "/" & host & "/udp/0/quic-v1")

proc preferSingleQuicRelayFamily(transport: TsnetTransport): bool =
  transport.cfg.relayEndpoint.strip().toLowerAscii().startsWith("quic://")

proc primaryQuicRelayFamily(requested: seq[TsnetParsedAddress]): string =
  for item in requested:
    if item.family == "ip4":
      return "ip4"
  if requested.len > 0:
    return requested[0].family
  ""

proc parseTsnetAddress(
    address: MultiAddress, listen: bool
): Result[TsnetParsedAddress, string] {.gcsafe.} =
  let raw = stripTsnetSuffix(address).valueOr:
    return err(error)
  let parts = splitAddressParts(raw)
  let baseIdx = tsnetAddressBaseIndex(parts)
  if parts.len < baseIdx + 4:
    return err("unsupported /tsnet address: " & $address)
  if parts[baseIdx] != "ip4" and parts[baseIdx] != "ip6":
    return err("unsupported /tsnet address family: " & $address)

  var parsed = TsnetParsedAddress(
    family: parts[baseIdx],
    ip: parts[baseIdx + 1].toLowerAscii(),
    port: 0,
    explicitIP: true,
    raw: raw,
    full: address
  )
  parsed.explicitIP = not (
    (parsed.family == "ip4" and parsed.ip == "0.0.0.0") or
    (parsed.family == "ip6" and parsed.ip == "::")
  )

  try:
    if parts.len == baseIdx + 4 and parts[baseIdx + 2] == "tcp":
      parsed.kind = TsnetPathKind.Tcp
      parsed.port = parseInt(parts[baseIdx + 3])
    elif parts.len == baseIdx + 5 and parts[baseIdx + 2] == "udp" and parts[baseIdx + 4] == "quic-v1":
      parsed.kind = TsnetPathKind.Quic
      parsed.port = parseInt(parts[baseIdx + 3])
    else:
      return err("unsupported /tsnet address: " & $address)
  except ValueError as exc:
    return err("invalid /tsnet port in " & $address & ": " & exc.msg)

  if not listen and not parsed.explicitIP:
    return err("dial address must use a concrete tailnet IP: " & $address)
  ok(parsed)

proc advertisedAddress(
    transport: TsnetTransport,
    parsed: TsnetParsedAddress,
    rawActual: MultiAddress
): MultiAddress {.gcsafe.} =
  let ip =
    if parsed.explicitIP:
      parsed.ip
    elif parsed.family == "ip6":
      transport.syntheticIPv6
    else:
      transport.syntheticIPv4
  let port =
    if parsed.port > 0:
      parsed.port
    else:
      portFromAddress(rawActual)
  buildTsnetAddress(parsed.family, ip, port, parsed.kind)

proc providerTailnetIp(self: TsnetTransport, family: string): string =
  let payload = self.providerStatusPayloadSafe().valueOr:
    return if family == "ip6": self.syntheticIPv6 else: self.syntheticIPv4
  for candidate in jsonFieldStrings(payload, "tailnetIPs"):
    let lowered = candidate.toLowerAscii()
    if family == "ip6":
      if lowered.contains(":"):
        return lowered
    else:
      if lowered.contains("."):
        return lowered
  if family == "ip6":
    self.syntheticIPv6
  else:
    self.syntheticIPv4

proc synthesizedEndpoint(
    transport: TsnetTransport, rawAddr: MultiAddress, kind: TsnetPathKind
): MultiAddress {.gcsafe.} =
  let family = familyFromAddress(rawAddr)
  let ip =
    if family == "ip6":
      transport.syntheticIPv6
    else:
      transport.syntheticIPv4
  buildTsnetAddress(family, ip, portFromAddress(rawAddr), kind)

proc providerSynthesizedEndpoint(
    transport: TsnetTransport, rawAddr: MultiAddress, kind: TsnetPathKind
): MultiAddress =
  let family = familyFromAddress(rawAddr)
  let ip = transport.providerTailnetIp(family)
  buildTsnetAddress(family, ip, portFromAddress(rawAddr), kind)

proc providerSynthesizedEndpointSafe(
    transport: TsnetTransport, rawAddr: MultiAddress, kind: TsnetPathKind
): MultiAddress {.gcsafe.} =
  tsnetSafe:
    result = transport.providerSynthesizedEndpoint(rawAddr, kind)

proc fallbackDialEndpoint(
    transport: TsnetTransport,
    family: string,
    kind: TsnetPathKind
): MultiAddress =
  let ip =
    if transport.providerUsesProxyRouting():
      transport.providerTailnetIp(family)
    elif family == "ip6":
      transport.syntheticIPv6
    else:
      transport.syntheticIPv4
  buildTsnetAddress(family, ip, 0, kind)

proc registerListenerMapping(
    transport: TsnetTransport, advertised: MultiAddress, raw: MultiAddress
): Result[void, string] {.gcsafe.} =
  tsnetSafe:
    let advertisedKey = $advertised
    let rawKey = $raw
    withLock(tsnetRegistryLock):
      if tsnetAdvertisedToRaw.hasKey(advertisedKey):
        return err("duplicate /tsnet listener registration: " & advertisedKey)
      tsnetAdvertisedToRaw[advertisedKey] = rawKey
  var nextMappings = listenerMappingSnapshot(transport.listenerMappings)
  nextMappings.add(
    TsnetListenerMapping(
      published: true,
      raw: raw,
      advertised: advertised,
      rawText: ownedText(rawKey),
      advertisedText: ownedText(advertisedKey)
    )
  )
  transport.listenerMappings = nextMappings
  ok()

proc rememberListenerMapping(
    transport: TsnetTransport, advertised: MultiAddress, raw: MultiAddress
): Result[void, string] {.gcsafe.} =
  let advertisedKey = $advertised
  let rawKey = $raw
  var nextMappings = listenerMappingSnapshot(transport.listenerMappings)
  nextMappings.add(
    TsnetListenerMapping(
      published: false,
      raw: raw,
      advertised: advertised,
      rawText: ownedText(rawKey),
      advertisedText: ownedText(advertisedKey)
    )
  )
  transport.listenerMappings = nextMappings
  ok()

proc clearListenerMappings(transport: TsnetTransport) {.gcsafe.} =
  let snapshot = listenerMappingSnapshot(transport.listenerMappings)
  transport.listenerMappings = @[]
  tsnetSafe:
    withLock(tsnetRegistryLock):
      for mapping in snapshot:
        if not mapping.published:
          continue
        let advertisedKey = mapping.advertisedText
        let rawKey = mapping.rawText
        if advertisedKey.len == 0 or rawKey.len == 0:
          continue
        if tsnetAdvertisedToRaw.getOrDefault(advertisedKey) == rawKey:
          tsnetAdvertisedToRaw.del(advertisedKey)

proc lookupRawListener(address: MultiAddress): Opt[MultiAddress] {.gcsafe.} =
  tsnetSafe:
    let key = $address
    withLock(tsnetRegistryLock):
      let rawValue = tsnetAdvertisedToRaw.getOrDefault(key)
      if rawValue.len > 0:
        return Opt.some(parseKnownAddress(rawValue))
  Opt.none(MultiAddress)

proc lookupAdvertisedListenerForRaw(
    transport: TsnetTransport, rawAddress: MultiAddress
): Opt[MultiAddress] {.gcsafe.} =
  tsnetSafe:
    if transport.isNil:
      return Opt.none(MultiAddress)
    let key = $rawAddress
    let mappings = listenerMappingSnapshot(transport.listenerMappings)
    for mapping in mappings:
      if mapping.rawText != key:
        continue
      if mapping.advertisedText.len == 0:
        continue
      return Opt.some(parseKnownAddress(mapping.advertisedText))
  Opt.none(MultiAddress)

proc rewriteAcceptedConnection(
    transport: TsnetTransport, conn: Connection, kind: TsnetPathKind
) =
  if conn.isNil:
    return
  if conn.localAddr.isSome:
    let localOpt = transport.lookupAdvertisedListenerForRaw(conn.localAddr.get())
    if localOpt.isSome:
      conn.localAddr = Opt.some(localOpt.get())
  if conn.observedAddr.isSome:
    let observedOpt = transport.lookupAdvertisedListenerForRaw(conn.observedAddr.get())
    if observedOpt.isSome:
      conn.observedAddr = Opt.some(observedOpt.get())
    elif transport.providerUsesProxyRouting():
      let remoteRes = transport.providerResolveRemoteSafe(conn.observedAddr.get())
      if remoteRes.isOk:
        conn.observedAddr = Opt.some(remoteRes.get())
      else:
        conn.observedAddr = Opt.none(MultiAddress)
    else:
      conn.observedAddr = Opt.none(MultiAddress)
  else:
    conn.observedAddr = Opt.none(MultiAddress)
  discard kind

proc new*(
    _: type[TsnetTransport],
    upgrader: Upgrade,
    privateKey: PrivateKey,
    cfg: TsnetTransportBuilderConfig = TsnetTransportBuilderConfig.init(),
): TsnetTransport =
  let syntheticId = nextSyntheticNodeId()
  result = TsnetTransport(
    cfg: cfg,
    upgrader: upgrader,
    networkReachability: NetworkReachability.Unknown,
    providerListenersActivated: false,
    acceptFuts: @[],
    acceptKinds: @[],
    listenerMappings: @[],
    syntheticIPv4: syntheticTailnetIPv4(syntheticId),
    syntheticIPv6: syntheticTailnetIPv6(syntheticId),
    provider: TsnetProvider.new(providerConfig(cfg))
  )
  result.cfg.bridgeExtraJson = ownedText(result.cfg.bridgeExtraJson)
  procCall Transport(result).initialize()
  result.tcpTransport = TcpTransport.new(flags = {ServerFlags.TcpNoDelay}, upgrade = upgrader)
  when defined(libp2p_msquic_experimental):
    result.quicTransport = newMsQuicTransport(upgrader, privateKey, cfg.quicConfig)
    if cfg.quicTlsTempDir.strip().len > 0:
      result.quicTransport.setTlsTempDir(cfg.quicTlsTempDir)

method handles*(
    self: TsnetTransport, address: MultiAddress
): bool {.gcsafe, raises: [].} =
  if not procCall Transport(self).handles(address):
    return false
  if not hasTsnetSuffix(address):
    return false
  parseTsnetAddress(address, listen = true).isOk()

method start*(
    self: TsnetTransport, addrs: seq[MultiAddress]
) {.async: (raises: [LPError, transport.TransportError, CancelledError]).} =
  if self.running:
    return

  var tcpRequested: seq[TsnetParsedAddress] = @[]
  var tcpRaw: seq[MultiAddress] = @[]
  var quicRequested: seq[TsnetParsedAddress] = @[]
  var quicRaw: seq[MultiAddress] = @[]
  self.providerListenersActivated = false
  let runtimeRequested = not self.provider.isNil and self.provider.runtimeRequested()
  let useProxyProvider = self.providerUsesProxyRouting()

  for address in addrs:
    let parsed = parseTsnetAddress(address, listen = true).valueOr:
      raise newException(TsnetTransportError, error)
    case parsed.kind
    of TsnetPathKind.Tcp:
      tcpRequested.add(parsed)
      tcpRaw.add(buildRawLoopbackAddress(parsed))
    of TsnetPathKind.Quic:
      when defined(libp2p_msquic_experimental):
        quicRequested.add(parsed)
        quicRaw.add(buildRawLoopbackAddress(parsed))
      else:
        raise newException(
          TsnetTransportError,
          "QUIC /tsnet listeners require -d:libp2p_msquic_experimental"
        )

  if self.preferSingleQuicRelayFamily() and quicRequested.len > 1:
    let selectedFamily = primaryQuicRelayFamily(quicRequested)
    if selectedFamily.len > 0:
      var filteredRequested: seq[TsnetParsedAddress] = @[]
      var filteredRaw: seq[MultiAddress] = @[]
      for idx, parsed in quicRequested:
        if parsed.family == selectedFamily:
          filteredRequested.add(parsed)
          filteredRaw.add(quicRaw[idx])
      if filteredRequested.len > 0:
        quicRequested = filteredRequested
        quicRaw = filteredRaw

  try:
    if tcpRaw.len > 0:
      await self.tcpTransport.start(tcpRaw)
      for idx, rawActual in self.tcpTransport.addrs:
        let registerRes =
          if useProxyProvider:
            let advertised = self.providerListenTcpSafe(
              tcpRequested[idx].family,
              tcpRequested[idx].port,
              rawActual
            ).valueOr:
              raise newException(TsnetTransportError, error)
            self.registerListenerMapping(advertised, rawActual)
          elif runtimeRequested:
            self.rememberListenerMapping(tcpRequested[idx].full, rawActual)
          else:
            let advertised = self.advertisedAddress(tcpRequested[idx], rawActual)
            self.registerListenerMapping(advertised, rawActual)
        if registerRes.isErr:
          raise newException(TsnetTransportError, registerRes.error)
    when defined(libp2p_msquic_experimental):
      if quicRaw.len > 0:
        await self.quicTransport.start(quicRaw)
        for idx, rawActual in self.quicTransport.addrs:
          let registerRes =
            if useProxyProvider:
              let advertised = self.providerListenUdpSafe(
                quicRequested[idx].family,
                quicRequested[idx].port,
                rawActual
              ).valueOr:
                raise newException(TsnetTransportError, error)
              self.registerListenerMapping(advertised, rawActual)
            elif runtimeRequested:
              self.rememberListenerMapping(quicRequested[idx].full, rawActual)
            else:
              let advertised = self.advertisedAddress(quicRequested[idx], rawActual)
              self.registerListenerMapping(advertised, rawActual)
          if registerRes.isErr:
            raise newException(TsnetTransportError, registerRes.error)
  except CancelledError as exc:
    self.clearListenerMappings()
    self.providerResetSafe()
    await noCancel self.tcpTransport.stop()
    when defined(libp2p_msquic_experimental):
      await noCancel self.quicTransport.stop()
    raise exc
  except LPError as exc:
    self.clearListenerMappings()
    self.providerResetSafe()
    await noCancel self.tcpTransport.stop()
    when defined(libp2p_msquic_experimental):
      await noCancel self.quicTransport.stop()
    raise exc
  except transport.TransportError as exc:
    self.clearListenerMappings()
    self.providerResetSafe()
    await noCancel self.tcpTransport.stop()
    when defined(libp2p_msquic_experimental):
      await noCancel self.quicTransport.stop()
    raise exc
  except CatchableError as exc:
    self.clearListenerMappings()
    self.providerResetSafe()
    await noCancel self.tcpTransport.stop()
    when defined(libp2p_msquic_experimental):
      await noCancel self.quicTransport.stop()
    raise newException(TsnetTransportError, exc.msg, exc)

  var advertised: seq[MultiAddress] = @[]
  for mapping in listenerMappingSnapshot(self.listenerMappings):
    if mapping.published:
      advertised.add(mapping.advertised)
  await procCall Transport(self).start(advertised)

method stop*(self: TsnetTransport) {.async: (raises: []).} =
  if self.running:
    await noCancel procCall Transport(self).stop()

  for fut in self.acceptFuts:
    if not fut.finished():
      fut.cancelSoon()
  self.acceptFuts.setLen(0)
  self.acceptKinds.setLen(0)

  self.clearListenerMappings()
  self.providerListenersActivated = false
  self.providerResetSafe()
  self.providerStopSafe()
  await noCancel self.tcpTransport.stop()
  when defined(libp2p_msquic_experimental):
    await noCancel self.quicTransport.stop()

method accept*(
    self: TsnetTransport
): Future[Connection] {.async: (raises: [transport.TransportError, CancelledError]).} =
  if not self.running:
    raise newTransportClosedError()

  if self.acceptFuts.len == 0:
    if self.tcpTransport.running:
      self.acceptFuts.add(self.tcpTransport.accept())
      self.acceptKinds.add(TsnetPathKind.Tcp)
    when defined(libp2p_msquic_experimental):
      if self.quicTransport.running:
        self.acceptFuts.add(self.quicTransport.accept())
        self.acceptKinds.add(TsnetPathKind.Quic)
  if self.acceptFuts.len == 0:
    raise newException(TsnetTransportError, "No /tsnet listeners configured")

  let finished =
    try:
      await one(self.acceptFuts)
    except ValueError as exc:
      raise newException(TsnetTransportError, exc.msg)

  let idx = self.acceptFuts.find(finished)
  if idx < 0:
    raise newException(TsnetTransportError, "tsnet accept future not tracked")

  let kind = self.acceptKinds[idx]
  case kind
  of TsnetPathKind.Tcp:
    self.acceptFuts[idx] = self.tcpTransport.accept()
  of TsnetPathKind.Quic:
    when defined(libp2p_msquic_experimental):
      self.acceptFuts[idx] = self.quicTransport.accept()
    else:
      raise newException(TsnetTransportError, "QUIC /tsnet not available")

  let conn =
    try:
      await finished
    except CancelledError as exc:
      raise exc
    except transport.TransportError as exc:
      raise exc
    except CatchableError as exc:
      raise newException(TsnetTransportError, exc.msg, exc)
  self.rewriteAcceptedConnection(conn, kind)
  conn

method dial*(
    self: TsnetTransport,
    hostname: string,
    address: MultiAddress,
    peerId: Opt[PeerId] = Opt.none(PeerId),
): Future[Connection] {.async: (raises: [transport.TransportError, CancelledError]).} =
  let parsed = parseTsnetAddress(address, listen = false).valueOr:
    raise newException(TsnetTransportDialError, error)
  if not self.providerUsesProxyRouting() and not self.provider.isNil and self.provider.runtimeRequested():
    discard self.providerStartSafe().valueOr:
      raise newException(TsnetTransportDialError, error)
  var udpDialTarget = TsnetProxyDialTarget(
    rawAddress: buildRawLoopbackAddress(parsed),
    mode: TsnetProxyDialMode.Local,
  )
  let rawTarget =
    if self.providerUsesProxyRouting():
      case parsed.kind
      of TsnetPathKind.Tcp:
        self.providerDialTcpExactSafe(parsed.family, parsed.ip, parsed.port).valueOr:
          raise newException(TsnetTransportDialError, error)
      of TsnetPathKind.Quic:
        when defined(libp2p_msquic_experimental):
          udpDialTarget = self.providerDialUdpExactTargetSafe(parsed.family, parsed.ip, parsed.port).valueOr:
            raise newException(TsnetTransportDialError, error)
          udpDialTarget.rawAddress
        else:
          raise newException(
            TsnetTransportDialError,
            "QUIC /tsnet dialing requires -d:libp2p_msquic_experimental"
          )
    else:
      let rawTargetOpt = lookupRawListener(address)
      if rawTargetOpt.isNone:
        raise newException(
          TsnetTransportDialError,
          "no local /tsnet route is registered for " & $address
        )
      rawTargetOpt.get()
  let conn =
    case parsed.kind
    of TsnetPathKind.Tcp:
      await self.tcpTransport.dial(hostname, rawTarget, peerId)
    of TsnetPathKind.Quic:
      when defined(libp2p_msquic_experimental):
        if not self.quicTransport.running:
          try:
            # QUIC clients may dial without registering a local /tsnet listener first.
            await self.quicTransport.start(@[])
          except CancelledError as exc:
            raise exc
          except transport.TransportError as exc:
            raise newException(TsnetTransportDialError, exc.msg, exc)
          except LPError as exc:
            raise newException(TsnetTransportDialError, exc.msg, exc)
          except CatchableError as exc:
            raise newException(TsnetTransportDialError, exc.msg, exc)
        if self.providerUsesProxyRouting():
          await self.waitForProviderUdpDialReady(udpDialTarget)
        let dialHostname = self.quicDialHostname(hostname, rawTarget)
        try:
          await self.quicTransport.dial(dialHostname, rawTarget, peerId)
        except CancelledError as exc:
          raise exc
        except CatchableError as exc:
          if self.providerUsesProxyRouting():
            let status = self.providerStatusPayloadSafe()
            let path =
              if status.isOk():
                normalizeTailnetPath(jsonFieldStr(status.get(), "tailnetPath", ""))
              else:
                ""
            if path in [TsnetPathDirect, TsnetPathPunchedDirect]:
              discard self.providerMarkFailedDirectUdpSafe(parsed.full, rawTarget)
              let fallbackTarget = self.providerDialUdpFallbackTargetSafe(parsed.family, parsed.ip, parsed.port).valueOr:
                raise newException(TsnetTransportDialError, exc.msg, exc)
              if $fallbackTarget.rawAddress != $rawTarget:
                await self.waitForProviderUdpDialReady(fallbackTarget)
                let fallbackHostname = self.quicDialHostname(hostname, fallbackTarget.rawAddress)
                try:
                  return await self.quicTransport.dial(fallbackHostname, fallbackTarget.rawAddress, peerId)
                except CancelledError as retryExc:
                  raise retryExc
                except CatchableError:
                  discard
          raise newException(TsnetTransportDialError, exc.msg, exc)
      else:
        raise newException(
          TsnetTransportDialError,
          "QUIC /tsnet dialing requires -d:libp2p_msquic_experimental"
        )

  if conn.localAddr.isSome:
    let rawLocal = conn.localAddr.get()
    let advertisedLocal =
      if self.providerUsesProxyRouting():
        self.providerSynthesizedEndpointSafe(rawLocal, parsed.kind)
      else:
        self.synthesizedEndpoint(rawLocal, parsed.kind)
    conn.localAddr = Opt.some(advertisedLocal)
  else:
    conn.localAddr = Opt.some(self.fallbackDialEndpoint(parsed.family, parsed.kind))
  conn.observedAddr = Opt.some(address)
  conn

method upgrade*(
    self: TsnetTransport, conn: Connection, peerId: Opt[PeerId]
): Future[Muxer] {.async: (raises: [CancelledError, LPError], raw: true).} =
  when defined(libp2p_msquic_experimental):
    if conn of MsQuicConnection:
      return self.quicTransport.upgrade(conn, peerId)
  return procCall Transport(self).upgrade(conn, peerId)

proc tailnetStatus*(self: TsnetTransport): TsnetStatusSnapshot {.public, gcsafe.} =
  let providerCapabilities = self.providerCapabilitiesSafe()
  if providerCapabilities.statusApi:
    let payload = self.providerStatusPayloadSafe().valueOr:
      return TsnetStatusSnapshot(
        tailnetIPs: @[self.syntheticIPv4, self.syntheticIPv6],
        tailnetPath: if self.listenerMappingCount() > 0: "direct" else: "idle",
        tailnetRelay: "",
        tailnetDerpMapSummary: "",
        tailnetPeers: @[]
      )
    var peers: seq[string] = @[]
    let peersNode = jsonField(payload, "tailnetPeers")
    if not peersNode.isNil and peersNode.kind == JArray:
      for item in peersNode:
        let dnsName = jsonFieldStr(item, "dnsName")
        if dnsName.len > 0:
          peers.add(dnsName)
          continue
        let hostName = jsonFieldStr(item, "hostName")
        if hostName.len > 0:
          peers.add(hostName)
    return TsnetStatusSnapshot(
      tailnetIPs: jsonFieldStrings(payload, "tailnetIPs"),
      tailnetPath: jsonFieldStr(payload, "tailnetPath", "idle"),
      tailnetRelay: jsonFieldStr(payload, "tailnetRelay"),
      tailnetDerpMapSummary: jsonFieldStr(payload, "tailnetDerpMapSummary"),
      tailnetPeers: peers
    )
  TsnetStatusSnapshot(
    tailnetIPs: @[self.syntheticIPv4, self.syntheticIPv6],
    tailnetPath: if self.listenerMappingCount() > 0: "direct" else: "idle",
    tailnetRelay: "",
    tailnetDerpMapSummary: "",
    tailnetPeers: @[]
  )

proc tailnetStatusPayload*(self: TsnetTransport): Result[JsonNode, string] {.gcsafe.} =
  if not self.provider.isNil and self.provider.runtimeRequested() and
      not self.providerCapabilitiesSafe().statusApi:
    return ok(self.deferredStatusPayload("provider_start_deferred"))
  if self.providerCapabilitiesSafe().statusApi:
    return self.providerStatusPayloadSafe()
  ok(self.provider.annotatePayload(%*{
    "ok": true,
    "started": self.running,
    "tailnetIPs": @[self.syntheticIPv4, self.syntheticIPv6],
    "tailnetPath": (if self.listenerMappingCount() > 0: "direct" else: "idle"),
    "tailnetRelay": "",
    "tailnetDerpMapSummary": "",
    "tailnetPeers": newJArray(),
  }))

proc listenerNeedsRepair*(self: TsnetTransport): bool {.gcsafe.} =
  if self.isNil:
    return false
  self.providerListenerNeedsRepairSafe()

proc tailnetPingPayload*(
    self: TsnetTransport,
    peerIp: string,
    pingType = "",
    sampleCount = 0,
    timeoutMs = 0
): Result[JsonNode, string] {.gcsafe.} =
  if not self.providerCapabilitiesSafe().pingApi and not self.provider.isNil and
      self.provider.runtimeRequested():
    discard self.providerStartSafe()
  if not self.providerCapabilitiesSafe().pingApi:
    return err("tailnet ping requires a real tsnet provider")
  let payload = %*{
    "peerIP": peerIp,
    "pingType": pingType,
    "sampleCount": sampleCount,
    "timeoutMs": timeoutMs,
  }
  self.providerPingPayloadSafe(payload)

proc tailnetDerpMapPayload*(self: TsnetTransport): Result[JsonNode, string] {.gcsafe.} =
  if not self.providerCapabilitiesSafe().derpMapApi and not self.provider.isNil and
      self.provider.runtimeRequested():
    discard self.providerStartSafe()
  self.providerDerpMapPayloadSafe()
