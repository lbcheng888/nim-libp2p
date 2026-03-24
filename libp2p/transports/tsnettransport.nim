# Nim-LibP2P
# Copyright (c) 2023-2024 Status Research & Development GmbH
# Licensed under either of
#  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
#  * MIT license ([LICENSE-MIT](LICENSE-MIT))
# at your option.
# This file may not be copied, modified, or distributed except according to
# those terms.

{.push raises: [].}

import std/[json, locks, os, strutils, tables]
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
import ./tsnetbridge as tsbridge

when defined(libp2p_msquic_experimental):
  import ./msquictransport

logScope:
  topics = "libp2p tsnettransport"

template tsnetSafe(body: untyped) =
  {.cast(gcsafe).}:
    body

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
    raw: MultiAddress
    advertised: MultiAddress

  TsnetTransport* = ref object of Transport
    cfg*: TsnetTransportBuilderConfig
    tcpTransport: TcpTransport
    when defined(libp2p_msquic_experimental):
      quicTransport: MsQuicTransport
    bridge: tsbridge.TsnetBridge
    acceptFuts: seq[Future[Connection]]
    acceptKinds: seq[TsnetPathKind]
    listenerMappings: seq[TsnetListenerMapping]
    syntheticIPv4: string
    syntheticIPv6: string

var tsnetRegistryLock {.global.}: Lock
var tsnetSyntheticNodeCounter {.global.} = 0
var tsnetAdvertisedToRaw {.global.}: Table[string, string] = initTable[string, string]()
var tsnetRawListenerToAdvertised {.global.}: Table[string, string] =
  initTable[string, string]()
var tsnetRawEndpointToAdvertised {.global.}: Table[string, string] =
  initTable[string, string]()

initLock(tsnetRegistryLock)

proc init*(_: type[TsnetTransportBuilderConfig]): TsnetTransportBuilderConfig =
  result = TsnetTransportBuilderConfig(
    controlUrl: "",
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

proc bridgeRequested(self: TsnetTransport): bool =
  self.cfg.bridgeLibraryPath.strip().len > 0 or
  self.cfg.controlUrl.strip().len > 0 or
  self.cfg.authKey.strip().len > 0 or
  self.cfg.hostname.strip().len > 0 or
  self.cfg.stateDir.strip().len > 0 or
  getEnv(tsbridge.DefaultTsnetBridgeEnvVar).strip().len > 0

proc bridgeConfigJson(self: TsnetTransport): string =
  var payload =
    block:
      let raw = self.cfg.bridgeExtraJson.strip()
      if raw.len == 0:
        newJObject()
      else:
        try:
          let parsed = parseJson(raw)
          if parsed.kind == JObject:
            parsed
          else:
            newJObject()
        except CatchableError:
          newJObject()
  payload["controlUrl"] = %self.cfg.controlUrl
  payload["authKey"] = %self.cfg.authKey
  payload["hostname"] = %self.cfg.hostname
  payload["stateDir"] = %self.cfg.stateDir
  payload["wireguardPort"] = %self.cfg.wireguardPort
  payload["logLevel"] = %self.cfg.logLevel
  payload["enableDebug"] = %self.cfg.enableDebug
  $payload

proc bridgeActive(self: TsnetTransport): bool =
  not self.bridge.isNil and self.bridge.handle > 0

proc ensureBridge(self: TsnetTransport): Result[bool, string] =
  if self.bridgeActive():
    return ok(true)
  let preferred = self.bridgeRequested()
  let opened = tsbridge.openTsnetBridge(self.bridgeConfigJson(), self.cfg.bridgeLibraryPath).valueOr:
    if preferred:
      return err(error)
    return ok(false)
  self.bridge = opened
  ok(true)

proc bridgeEnsureSafe(self: TsnetTransport): Result[bool, string] {.gcsafe.} =
  tsnetSafe:
    result = self.ensureBridge()

proc bridgeResetSafe(self: TsnetTransport) {.gcsafe.} =
  tsnetSafe:
    if self.bridgeActive():
      discard tsbridge.reset(self.bridge)

proc bridgeCloseSafe(self: TsnetTransport) {.gcsafe.} =
  tsnetSafe:
    if self.bridgeActive():
      self.bridge.close()
      self.bridge = nil

proc bridgeListenTcpSafe(
    self: TsnetTransport,
    family: string,
    port: int,
    localAddress: MultiAddress
): Result[MultiAddress, string] {.gcsafe.} =
  tsnetSafe:
    result = tsbridge.listenTcpProxy(self.bridge, family, port, localAddress)

proc bridgeListenUdpSafe(
    self: TsnetTransport,
    family: string,
    port: int,
    localAddress: MultiAddress
): Result[MultiAddress, string] {.gcsafe.} =
  tsnetSafe:
    result = tsbridge.listenUdpProxy(self.bridge, family, port, localAddress)

proc bridgeDialTcpSafe(
    self: TsnetTransport,
    family, ip: string,
    port: int
): Result[MultiAddress, string] {.gcsafe.} =
  tsnetSafe:
    result = tsbridge.dialTcpProxy(self.bridge, family, ip, port)

proc bridgeDialUdpSafe(
    self: TsnetTransport,
    family, ip: string,
    port: int
): Result[MultiAddress, string] {.gcsafe.} =
  tsnetSafe:
    result = tsbridge.dialUdpProxy(self.bridge, family, ip, port)

proc bridgeResolveRemoteSafe(
    self: TsnetTransport,
    rawAddress: MultiAddress
): Result[MultiAddress, string] {.gcsafe.} =
  tsnetSafe:
    result = tsbridge.resolveRemote(self.bridge, rawAddress)

proc bridgeStatusPayloadSafe(self: TsnetTransport): Result[JsonNode, string] {.gcsafe.} =
  tsnetSafe:
    if not self.bridgeActive():
      result = err("tsnet bridge is not active")
    else:
      result = tsbridge.statusPayload(self.bridge)

proc bridgePingPayloadSafe(
    self: TsnetTransport,
    payload: JsonNode
): Result[JsonNode, string] {.gcsafe.} =
  tsnetSafe:
    result = tsbridge.pingPayload(self.bridge, payload)

proc bridgeDerpMapPayloadSafe(self: TsnetTransport): Result[JsonNode, string] {.gcsafe.} =
  tsnetSafe:
    result = tsbridge.derpMapPayload(self.bridge)

proc hasTsnetSuffix(address: MultiAddress): bool {.gcsafe.} =
  let text = $address
  text.len > 6 and text.toLowerAscii().endsWith("/tsnet")

proc stripTsnetSuffix(address: MultiAddress): Result[MultiAddress, string] {.gcsafe.} =
  let text = $address
  if not text.toLowerAscii().endsWith("/tsnet"):
    return err("address is missing /tsnet: " & text)
  if text.len <= 6:
    return err("invalid /tsnet address: " & text)
  let rawText = text[0 ..< text.len - 6]
  MultiAddress.init(rawText)

proc splitAddressParts(address: MultiAddress): seq[string] {.gcsafe.} =
  for part in ($address).split('/'):
    if part.len > 0:
      result.add(part)

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
  if parts.len >= 2 and (parts[0] == "ip4" or parts[0] == "ip6"):
    return parts[0]
  ""

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

proc parseTsnetAddress(
    address: MultiAddress, listen: bool
): Result[TsnetParsedAddress, string] {.gcsafe.} =
  let raw = stripTsnetSuffix(address).valueOr:
    return err(error)
  let parts = splitAddressParts(raw)
  if parts.len < 4:
    return err("unsupported /tsnet address: " & $address)
  if parts[0] != "ip4" and parts[0] != "ip6":
    return err("unsupported /tsnet address family: " & $address)

  var parsed = TsnetParsedAddress(
    family: parts[0],
    ip: parts[1].toLowerAscii(),
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
    if parts.len == 4 and parts[2] == "tcp":
      parsed.kind = TsnetPathKind.Tcp
      parsed.port = parseInt(parts[3])
    elif parts.len == 5 and parts[2] == "udp" and parts[4] == "quic-v1":
      parsed.kind = TsnetPathKind.Quic
      parsed.port = parseInt(parts[3])
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

proc bridgeStatusPayload(self: TsnetTransport): Result[JsonNode, string] =
  if not self.bridgeActive():
    return err("tsnet bridge is not active")
  tsbridge.statusPayload(self.bridge)

proc bridgeTailnetIp(self: TsnetTransport, family: string): string =
  let payload = self.bridgeStatusPayloadSafe().valueOr:
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

proc bridgeSynthesizedEndpoint(
    transport: TsnetTransport, rawAddr: MultiAddress, kind: TsnetPathKind
): MultiAddress =
  let family = familyFromAddress(rawAddr)
  let ip = transport.bridgeTailnetIp(family)
  buildTsnetAddress(family, ip, portFromAddress(rawAddr), kind)

proc bridgeSynthesizedEndpointSafe(
    transport: TsnetTransport, rawAddr: MultiAddress, kind: TsnetPathKind
): MultiAddress {.gcsafe.} =
  tsnetSafe:
    result = transport.bridgeSynthesizedEndpoint(rawAddr, kind)

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
      tsnetRawListenerToAdvertised[rawKey] = advertisedKey
  transport.listenerMappings.add(TsnetListenerMapping(raw: raw, advertised: advertised))
  ok()

proc clearListenerMappings(transport: TsnetTransport) {.gcsafe.} =
  tsnetSafe:
    withLock(tsnetRegistryLock):
      for mapping in transport.listenerMappings:
        let advertisedKey = $mapping.advertised
        let rawKey = $mapping.raw
        if tsnetAdvertisedToRaw.getOrDefault(advertisedKey) == rawKey:
          tsnetAdvertisedToRaw.del(advertisedKey)
        if tsnetRawListenerToAdvertised.getOrDefault(rawKey) == advertisedKey:
          tsnetRawListenerToAdvertised.del(rawKey)
  transport.listenerMappings.setLen(0)

proc lookupRawListener(address: MultiAddress): Opt[MultiAddress] {.gcsafe.} =
  tsnetSafe:
    let key = $address
    withLock(tsnetRegistryLock):
      let rawValue = tsnetAdvertisedToRaw.getOrDefault(key)
      if rawValue.len > 0:
        return Opt.some(parseKnownAddress(rawValue))
  Opt.none(MultiAddress)

proc lookupAdvertisedForRaw(
    rawAddress: MultiAddress
): Opt[MultiAddress] {.gcsafe.} =
  tsnetSafe:
    let key = $rawAddress
    withLock(tsnetRegistryLock):
      let listenerValue = tsnetRawListenerToAdvertised.getOrDefault(key)
      if listenerValue.len > 0:
        return Opt.some(parseKnownAddress(listenerValue))
      let endpointValue = tsnetRawEndpointToAdvertised.getOrDefault(key)
      if endpointValue.len > 0:
        return Opt.some(parseKnownAddress(endpointValue))
  Opt.none(MultiAddress)

proc registerEndpoint(rawAddress: MultiAddress, advertised: MultiAddress) {.gcsafe.} =
  tsnetSafe:
    withLock(tsnetRegistryLock):
      tsnetRawEndpointToAdvertised[$rawAddress] = $advertised

proc rewriteAcceptedConnection(
    transport: TsnetTransport, conn: Connection, kind: TsnetPathKind
) =
  if conn.isNil:
    return
  if conn.localAddr.isSome:
    let localOpt = lookupAdvertisedForRaw(conn.localAddr.get())
    if localOpt.isSome:
      conn.localAddr = Opt.some(localOpt.get())
  if conn.observedAddr.isSome:
    let observedOpt = lookupAdvertisedForRaw(conn.observedAddr.get())
    if observedOpt.isSome:
      conn.observedAddr = Opt.some(observedOpt.get())
    elif transport.bridgeActive():
      let remoteRes = transport.bridgeResolveRemoteSafe(conn.observedAddr.get())
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
    acceptFuts: @[],
    acceptKinds: @[],
    listenerMappings: @[],
    syntheticIPv4: syntheticTailnetIPv4(syntheticId),
    syntheticIPv6: syntheticTailnetIPv6(syntheticId)
  )
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
  let useBridge = self.bridgeEnsureSafe().valueOr:
    raise newException(TsnetTransportError, error)

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

  try:
    if tcpRaw.len > 0:
      await self.tcpTransport.start(tcpRaw)
      for idx, rawActual in self.tcpTransport.addrs:
        let advertised =
          if useBridge:
            self.bridgeListenTcpSafe(
              tcpRequested[idx].family,
              tcpRequested[idx].port,
              rawActual
            ).valueOr:
              raise newException(TsnetTransportError, error)
          else:
            self.advertisedAddress(tcpRequested[idx], rawActual)
        let registerRes = self.registerListenerMapping(
          advertised,
          rawActual
        )
        if registerRes.isErr:
          raise newException(TsnetTransportError, registerRes.error)
    when defined(libp2p_msquic_experimental):
      if quicRaw.len > 0:
        await self.quicTransport.start(quicRaw)
        for idx, rawActual in self.quicTransport.addrs:
          let advertised =
            if useBridge:
              self.bridgeListenUdpSafe(
                quicRequested[idx].family,
                quicRequested[idx].port,
                rawActual
              ).valueOr:
                raise newException(TsnetTransportError, error)
            else:
              self.advertisedAddress(quicRequested[idx], rawActual)
          let registerRes = self.registerListenerMapping(
            advertised,
            rawActual
          )
          if registerRes.isErr:
            raise newException(TsnetTransportError, registerRes.error)
  except CancelledError as exc:
    self.clearListenerMappings()
    self.bridgeResetSafe()
    await noCancel self.tcpTransport.stop()
    when defined(libp2p_msquic_experimental):
      await noCancel self.quicTransport.stop()
    raise exc
  except LPError as exc:
    self.clearListenerMappings()
    self.bridgeResetSafe()
    await noCancel self.tcpTransport.stop()
    when defined(libp2p_msquic_experimental):
      await noCancel self.quicTransport.stop()
    raise exc
  except transport.TransportError as exc:
    self.clearListenerMappings()
    self.bridgeResetSafe()
    await noCancel self.tcpTransport.stop()
    when defined(libp2p_msquic_experimental):
      await noCancel self.quicTransport.stop()
    raise exc
  except CatchableError as exc:
    self.clearListenerMappings()
    self.bridgeResetSafe()
    await noCancel self.tcpTransport.stop()
    when defined(libp2p_msquic_experimental):
      await noCancel self.quicTransport.stop()
    raise newException(TsnetTransportError, exc.msg, exc)

  var advertised: seq[MultiAddress] = @[]
  for mapping in self.listenerMappings:
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
  self.bridgeResetSafe()
  self.bridgeCloseSafe()
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
  let rawTarget =
    if self.bridgeActive():
      case parsed.kind
      of TsnetPathKind.Tcp:
        self.bridgeDialTcpSafe(parsed.family, parsed.ip, parsed.port).valueOr:
          raise newException(TsnetTransportDialError, error)
      of TsnetPathKind.Quic:
        when defined(libp2p_msquic_experimental):
          self.bridgeDialUdpSafe(parsed.family, parsed.ip, parsed.port).valueOr:
            raise newException(TsnetTransportDialError, error)
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
          raise newException(
            TsnetTransportDialError,
            "tsnet QUIC path is not running"
          )
        await self.quicTransport.dial(hostname, rawTarget, peerId)
      else:
        raise newException(
          TsnetTransportDialError,
          "QUIC /tsnet dialing requires -d:libp2p_msquic_experimental"
        )

  if conn.localAddr.isSome:
    let rawLocal = conn.localAddr.get()
    let advertisedLocal =
      if self.bridgeActive():
        self.bridgeSynthesizedEndpointSafe(rawLocal, parsed.kind)
      else:
        self.synthesizedEndpoint(rawLocal, parsed.kind)
    registerEndpoint(rawLocal, advertisedLocal)
    conn.localAddr = Opt.some(advertisedLocal)
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
  if self.bridgeActive():
    let payload = self.bridgeStatusPayloadSafe().valueOr:
      return TsnetStatusSnapshot(
        tailnetIPs: @[self.syntheticIPv4, self.syntheticIPv6],
        tailnetPath: if self.listenerMappings.len > 0: "direct" else: "idle",
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
    tailnetPath: if self.listenerMappings.len > 0: "direct" else: "idle",
    tailnetRelay: "",
    tailnetDerpMapSummary: "",
    tailnetPeers: @[]
  )

proc tailnetStatusPayload*(self: TsnetTransport): Result[JsonNode, string] =
  if self.bridgeActive():
    return self.bridgeStatusPayloadSafe()
  ok(%*{
    "ok": true,
    "started": self.running,
    "tailnetIPs": @[self.syntheticIPv4, self.syntheticIPv6],
    "tailnetPath": (if self.listenerMappings.len > 0: "direct" else: "idle"),
    "tailnetRelay": "",
    "tailnetDerpMapSummary": "",
    "tailnetPeers": newJArray(),
  })

proc tailnetPingPayload*(
    self: TsnetTransport,
    peerIp: string,
    pingType = "",
    sampleCount = 0,
    timeoutMs = 0
): Result[JsonNode, string] =
  if not self.bridgeActive():
    return err("tailnet ping requires a real tsnet bridge")
  let payload = %*{
    "peerIP": peerIp,
    "pingType": pingType,
    "sampleCount": sampleCount,
    "timeoutMs": timeoutMs,
  }
  self.bridgePingPayloadSafe(payload)

proc tailnetDerpMapPayload*(self: TsnetTransport): Result[JsonNode, string] =
  if not self.bridgeActive():
    return ok(%*{
      "ok": true,
      "omitDefaultRegions": false,
      "regionCount": 0,
      "regions": newJArray(),
    })
  self.bridgeDerpMapPayloadSafe()
