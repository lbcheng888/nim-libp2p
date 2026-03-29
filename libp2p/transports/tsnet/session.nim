{.push raises: [].}

import std/[json, sequtils, strutils, times]

import ../../crypto/[chacha20poly1305, curve25519, hkdf]
import ../../utility
import ./state
import ../tsnetprovidertypes

type
  TsnetPeerSession* = object
    peerId*: string
    hostName*: string
    dnsName*: string
    tailscaleIPs*: seq[string]
    listenAddrs*: seq[string]
    online*: bool
    active*: bool
    curAddr*: string
    addrs*: seq[string]
    relay*: string
    peerRelay*: string
    inMagicSock*: bool
    inEngine*: bool
    lastHandshakeUnixMilli*: int64

  TsnetStatusSnapshot* = object
    ok*: bool
    error*: string
    started*: bool
    backendState*: string
    authUrl*: string
    hostname*: string
    controlUrl*: string
    wireguardPort*: int
    tailnetIPs*: seq[string]
    tailnetPath*: string
    tailnetRelay*: string
    tailnetDerpMapSummary*: string
    tailnetPeers*: seq[TsnetPeerSession]

  TsnetPingSample* = object
    sequence*: int
    latencyMs*: int64
    endpoint*: string
    peerRelay*: string
    derpRegionId*: int
    derpRegionCode*: string
    error*: string

  TsnetPingSnapshot* = object
    ok*: bool
    error*: string
    peerIP*: string
    pingType*: string
    samples*: seq[TsnetPingSample]
    minLatencyMs*: int64
    maxLatencyMs*: int64
    avgLatencyMs*: int64
    elapsedMs*: int64

  TsnetDerpNodeSnapshot* = object
    name*: string
    regionId*: int
    hostName*: string
    certName*: string
    ipv4*: string
    ipv6*: string
    stunPort*: int
    stunOnly*: bool
    derpPort*: int
    canPort80*: bool

  TsnetDerpRegionSnapshot* = object
    regionId*: int
    regionCode*: string
    regionName*: string
    avoid*: bool
    noMeasureNoHome*: bool
    nodeCount*: int
    nodes*: seq[TsnetDerpNodeSnapshot]

  TsnetDerpMapSnapshot* = object
    ok*: bool
    error*: string
    omitDefaultRegions*: bool
    regionCount*: int
    regions*: seq[TsnetDerpRegionSnapshot]

  TsnetSessionSnapshot* = object
    source*: string
    capturedAtUnixMilli*: int64
    status*: TsnetStatusSnapshot
    derpMap*: TsnetDerpMapSnapshot
    ping*: TsnetPingSnapshot

proc jsonField(node: JsonNode, key: string): JsonNode =
  if node.isNil or node.kind != JObject or not node.hasKey(key):
    return newJNull()
  node.getOrDefault(key)

proc jsonString(node: JsonNode, key: string, defaultValue = ""): string =
  let value = jsonField(node, key)
  if value.kind == JString:
    return value.getStr()
  defaultValue

proc jsonBool(node: JsonNode, key: string, defaultValue = false): bool =
  let value = jsonField(node, key)
  case value.kind
  of JBool:
    value.getBool()
  of JString:
    value.getStr().strip().toLowerAscii() in ["1", "true", "yes", "y"]
  else:
    defaultValue

proc jsonInt(node: JsonNode, key: string, defaultValue = 0): int =
  let value = jsonField(node, key)
  case value.kind
  of JInt:
    value.getBiggestInt().int
  of JFloat:
    value.getFloat().int
  else:
    defaultValue

proc distinctStrings(values: seq[string]): seq[string] =
  result = @[]
  for value in values:
    if value.len == 0 or value in result:
      continue
    result.add(value)

proc jsonInt64(node: JsonNode, key: string, defaultValue = 0'i64): int64 =
  let value = jsonField(node, key)
  case value.kind
  of JInt:
    value.getBiggestInt().int64
  of JFloat:
    value.getFloat().int64
  else:
    defaultValue

proc jsonStrings(node: JsonNode, key: string): seq[string] =
  let value = jsonField(node, key)
  if value.kind != JArray:
    return @[]
  for item in value.items():
    if item.kind == JString:
      result.add(item.getStr())

proc toJson*(peer: TsnetPeerSession): JsonNode =
  result = newJObject()
  result["peerId"] = %peer.peerId
  result["hostName"] = %peer.hostName
  result["dnsName"] = %peer.dnsName
  result["tailscaleIPs"] = %peer.tailscaleIPs
  result["listenAddrs"] = %peer.listenAddrs
  result["online"] = %peer.online
  result["active"] = %peer.active
  result["curAddr"] = %peer.curAddr
  result["addrs"] = %peer.addrs
  result["relay"] = %peer.relay
  result["peerRelay"] = %peer.peerRelay
  result["inMagicSock"] = %peer.inMagicSock
  result["inEngine"] = %peer.inEngine
  result["lastHandshakeUnixMilli"] = %peer.lastHandshakeUnixMilli

proc parsePeerSession*(node: JsonNode): Result[TsnetPeerSession, string] =
  if node.isNil or node.kind != JObject:
    return err("tsnet peer session must be a JSON object")
  ok(TsnetPeerSession(
    peerId: jsonString(node, "peerId"),
    hostName: jsonString(node, "hostName"),
    dnsName: jsonString(node, "dnsName"),
    tailscaleIPs: jsonStrings(node, "tailscaleIPs"),
    listenAddrs: jsonStrings(node, "listenAddrs"),
    online: jsonBool(node, "online"),
    active: jsonBool(node, "active"),
    curAddr: jsonString(node, "curAddr"),
    addrs: jsonStrings(node, "addrs"),
    relay: jsonString(node, "relay"),
    peerRelay: jsonString(node, "peerRelay"),
    inMagicSock: jsonBool(node, "inMagicSock"),
    inEngine: jsonBool(node, "inEngine"),
    lastHandshakeUnixMilli: jsonInt64(node, "lastHandshakeUnixMilli")
  ))

proc toJson*(status: TsnetStatusSnapshot): JsonNode =
  result = newJObject()
  result["ok"] = %status.ok
  if status.error.len > 0:
    result["error"] = %status.error
  result["started"] = %status.started
  result["backendState"] = %status.backendState
  result["authUrl"] = %status.authUrl
  result["hostname"] = %status.hostname
  result["controlUrl"] = %status.controlUrl
  result["wireguardPort"] = %status.wireguardPort
  result["tailnetIPs"] = %status.tailnetIPs
  result["tailnetPath"] = %status.tailnetPath
  result["tailnetRelay"] = %status.tailnetRelay
  result["tailnetDerpMapSummary"] = %status.tailnetDerpMapSummary
  var peers = newJArray()
  for peer in status.tailnetPeers:
    peers.add(peer.toJson())
  result["tailnetPeers"] = peers

proc parseStatusSnapshot*(node: JsonNode): Result[TsnetStatusSnapshot, string] =
  if node.isNil or node.kind != JObject:
    return err("tsnet status payload must be a JSON object")
  var peers: seq[TsnetPeerSession] = @[]
  let peerNodes = jsonField(node, "tailnetPeers")
  if peerNodes.kind == JArray:
    for item in peerNodes.items():
      let parsed = parsePeerSession(item).valueOr:
        return err(error)
      peers.add(parsed)
  ok(TsnetStatusSnapshot(
    ok: jsonBool(node, "ok"),
    error: jsonString(node, "error"),
    started: jsonBool(node, "started"),
    backendState: jsonString(node, "backendState"),
    authUrl: jsonString(node, "authUrl"),
    hostname: jsonString(node, "hostname"),
    controlUrl: jsonString(node, "controlUrl"),
    wireguardPort: jsonInt(node, "wireguardPort"),
    tailnetIPs: jsonStrings(node, "tailnetIPs"),
    tailnetPath: jsonString(node, "tailnetPath"),
    tailnetRelay: jsonString(node, "tailnetRelay"),
    tailnetDerpMapSummary: jsonString(node, "tailnetDerpMapSummary"),
    tailnetPeers: peers
  ))

proc toJson*(sample: TsnetPingSample): JsonNode =
  result = newJObject()
  result["sequence"] = %sample.sequence
  result["latencyMs"] = %sample.latencyMs
  if sample.endpoint.len > 0:
    result["endpoint"] = %sample.endpoint
  if sample.peerRelay.len > 0:
    result["peerRelay"] = %sample.peerRelay
  if sample.derpRegionId > 0:
    result["derpRegionId"] = %sample.derpRegionId
  if sample.derpRegionCode.len > 0:
    result["derpRegionCode"] = %sample.derpRegionCode
  if sample.error.len > 0:
    result["error"] = %sample.error

proc parsePingSample*(node: JsonNode): Result[TsnetPingSample, string] =
  if node.isNil or node.kind != JObject:
    return err("tsnet ping sample must be a JSON object")
  ok(TsnetPingSample(
    sequence: jsonInt(node, "sequence"),
    latencyMs: jsonInt64(node, "latencyMs"),
    endpoint: jsonString(node, "endpoint"),
    peerRelay: jsonString(node, "peerRelay"),
    derpRegionId: jsonInt(node, "derpRegionId"),
    derpRegionCode: jsonString(node, "derpRegionCode"),
    error: jsonString(node, "error")
  ))

proc toJson*(ping: TsnetPingSnapshot): JsonNode =
  result = newJObject()
  result["ok"] = %ping.ok
  if ping.error.len > 0:
    result["error"] = %ping.error
  result["peerIP"] = %ping.peerIP
  result["pingType"] = %ping.pingType
  var samples = newJArray()
  for sample in ping.samples:
    samples.add(sample.toJson())
  result["samples"] = samples
  result["minLatencyMs"] = %ping.minLatencyMs
  result["maxLatencyMs"] = %ping.maxLatencyMs
  result["avgLatencyMs"] = %ping.avgLatencyMs
  result["elapsedMs"] = %ping.elapsedMs

proc parsePingSnapshot*(node: JsonNode): Result[TsnetPingSnapshot, string] =
  if node.isNil or node.kind != JObject:
    return err("tsnet ping payload must be a JSON object")
  var samples: seq[TsnetPingSample] = @[]
  let sampleNodes = jsonField(node, "samples")
  if sampleNodes.kind == JArray:
    for item in sampleNodes.items():
      let parsed = parsePingSample(item).valueOr:
        return err(error)
      samples.add(parsed)
  ok(TsnetPingSnapshot(
    ok: jsonBool(node, "ok"),
    error: jsonString(node, "error"),
    peerIP: jsonString(node, "peerIP"),
    pingType: jsonString(node, "pingType"),
    samples: samples,
    minLatencyMs: jsonInt64(node, "minLatencyMs"),
    maxLatencyMs: jsonInt64(node, "maxLatencyMs"),
    avgLatencyMs: jsonInt64(node, "avgLatencyMs"),
    elapsedMs: jsonInt64(node, "elapsedMs")
  ))

proc toJson*(node: TsnetDerpNodeSnapshot): JsonNode =
  result = newJObject()
  result["name"] = %node.name
  result["regionId"] = %node.regionId
  result["hostName"] = %node.hostName
  if node.certName.len > 0:
    result["certName"] = %node.certName
  if node.ipv4.len > 0:
    result["ipv4"] = %node.ipv4
  if node.ipv6.len > 0:
    result["ipv6"] = %node.ipv6
  if node.stunPort > 0:
    result["stunPort"] = %node.stunPort
  if node.stunOnly:
    result["stunOnly"] = %node.stunOnly
  if node.derpPort > 0:
    result["derpPort"] = %node.derpPort
  if node.canPort80:
    result["canPort80"] = %node.canPort80

proc parseDerpNodeSnapshot*(node: JsonNode): Result[TsnetDerpNodeSnapshot, string] =
  if node.isNil or node.kind != JObject:
    return err("tsnet derp node must be a JSON object")
  ok(TsnetDerpNodeSnapshot(
    name: jsonString(node, "name"),
    regionId: jsonInt(node, "regionId"),
    hostName: jsonString(node, "hostName"),
    certName: jsonString(node, "certName"),
    ipv4: jsonString(node, "ipv4"),
    ipv6: jsonString(node, "ipv6"),
    stunPort: jsonInt(node, "stunPort"),
    stunOnly: jsonBool(node, "stunOnly"),
    derpPort: jsonInt(node, "derpPort"),
    canPort80: jsonBool(node, "canPort80")
  ))

proc toJson*(region: TsnetDerpRegionSnapshot): JsonNode =
  result = newJObject()
  result["regionId"] = %region.regionId
  result["regionCode"] = %region.regionCode
  result["regionName"] = %region.regionName
  result["avoid"] = %region.avoid
  result["noMeasureNoHome"] = %region.noMeasureNoHome
  result["nodeCount"] = %region.nodeCount
  var nodes = newJArray()
  for item in region.nodes:
    nodes.add(item.toJson())
  result["nodes"] = nodes

proc parseDerpRegionSnapshot*(node: JsonNode): Result[TsnetDerpRegionSnapshot, string] =
  if node.isNil or node.kind != JObject:
    return err("tsnet derp region must be a JSON object")
  var nodes: seq[TsnetDerpNodeSnapshot] = @[]
  let nodeArray = jsonField(node, "nodes")
  if nodeArray.kind == JArray:
    for item in nodeArray.items():
      let parsed = parseDerpNodeSnapshot(item).valueOr:
        return err(error)
      nodes.add(parsed)
  ok(TsnetDerpRegionSnapshot(
    regionId: jsonInt(node, "regionId"),
    regionCode: jsonString(node, "regionCode"),
    regionName: jsonString(node, "regionName"),
    avoid: jsonBool(node, "avoid"),
    noMeasureNoHome: jsonBool(node, "noMeasureNoHome"),
    nodeCount: jsonInt(node, "nodeCount", nodes.len),
    nodes: nodes
  ))

proc toJson*(derpMap: TsnetDerpMapSnapshot): JsonNode =
  result = newJObject()
  result["ok"] = %derpMap.ok
  if derpMap.error.len > 0:
    result["error"] = %derpMap.error
  result["omitDefaultRegions"] = %derpMap.omitDefaultRegions
  result["regionCount"] = %derpMap.regionCount
  var regions = newJArray()
  for region in derpMap.regions:
    regions.add(region.toJson())
  result["regions"] = regions

proc parseDerpMapSnapshot*(node: JsonNode): Result[TsnetDerpMapSnapshot, string] =
  if node.isNil or node.kind != JObject:
    return err("tsnet derp map payload must be a JSON object")
  var regions: seq[TsnetDerpRegionSnapshot] = @[]
  let regionArray = jsonField(node, "regions")
  if regionArray.kind == JArray:
    for item in regionArray.items():
      let parsed = parseDerpRegionSnapshot(item).valueOr:
        return err(error)
      regions.add(parsed)
  ok(TsnetDerpMapSnapshot(
    ok: jsonBool(node, "ok"),
    error: jsonString(node, "error"),
    omitDefaultRegions: jsonBool(node, "omitDefaultRegions"),
    regionCount: jsonInt(node, "regionCount", regions.len),
    regions: regions
  ))

proc init*(
    _: type[TsnetSessionSnapshot],
    source = "",
    capturedAtUnixMilli = 0'i64,
    status = TsnetStatusSnapshot(),
    derpMap = TsnetDerpMapSnapshot(),
    ping = TsnetPingSnapshot()
): TsnetSessionSnapshot =
  TsnetSessionSnapshot(
    source: source,
    capturedAtUnixMilli: capturedAtUnixMilli,
    status: status,
    derpMap: derpMap,
    ping: ping
  )

proc toJson*(snapshot: TsnetSessionSnapshot): JsonNode =
  result = newJObject()
  result["source"] = %snapshot.source
  result["capturedAtUnixMilli"] = %snapshot.capturedAtUnixMilli
  result["status"] = snapshot.status.toJson()
  result["derpMap"] = snapshot.derpMap.toJson()
  result["ping"] = snapshot.ping.toJson()

proc homeDerpLabel*(snapshot: TsnetSessionSnapshot): string =
  if snapshot.status.tailnetDerpMapSummary.len > 0:
    return snapshot.status.tailnetDerpMapSummary
  if snapshot.status.tailnetRelay.len > 0 and snapshot.derpMap.regions.len > 0:
    for region in snapshot.derpMap.regions:
      if region.regionCode == snapshot.status.tailnetRelay:
        return $region.regionId & "/" & region.regionCode
  if snapshot.status.tailnetRelay.len > 0:
    return snapshot.status.tailnetRelay
  ""

proc peerCache*(snapshot: TsnetSessionSnapshot): seq[TsnetStoredPeer] =
  for peer in snapshot.status.tailnetPeers:
    result.add(TsnetStoredPeer(
      nodeId:
        if peer.dnsName.len > 0: peer.dnsName
        else: peer.hostName,
      hostName: peer.hostName,
      dnsName: peer.dnsName,
      relay:
        if peer.peerRelay.len > 0: peer.peerRelay
        else: peer.relay,
      tailscaleIPs: peer.tailscaleIPs,
      allowedIPs: @[],
      lastHandshakeUnixMilli: peer.lastHandshakeUnixMilli
    ))

proc supportsProxyRouting*(snapshot: TsnetSessionSnapshot): bool =
  snapshot.status.started and snapshot.status.tailnetIPs.len > 0

proc mapStringSeq(node: JsonNode, key: string): seq[string] =
  let value = jsonField(node, key)
  if value.kind != JArray:
    return @[]
  for item in value.items():
    case item.kind
    of JString:
      result.add(item.getStr().strip())
    else:
      discard

proc trimCidr(value: string): string =
  let trimmed = value.strip()
  if trimmed.len == 0:
    return ""
  let slash = trimmed.find('/')
  if slash >= 0:
    return trimmed[0 ..< slash]
  trimmed

proc mapRegionCodeById(derpMap: TsnetDerpMapSnapshot, regionId: int): string =
  for region in derpMap.regions:
    if region.regionId == regionId:
      return region.regionCode.toLowerAscii()
  ""

proc parseMapDerpNode(node: JsonNode): TsnetDerpNodeSnapshot =
  TsnetDerpNodeSnapshot(
    name: jsonString(node, "Name"),
    regionId: jsonInt(node, "RegionID"),
    hostName: jsonString(node, "HostName"),
    certName: jsonString(node, "CertName"),
    ipv4: jsonString(node, "IPv4"),
    ipv6: jsonString(node, "IPv6"),
    stunPort: jsonInt(node, "STUNPort"),
    stunOnly: jsonBool(node, "STUNOnly"),
    derpPort: jsonInt(node, "DERPPort"),
    canPort80: jsonBool(node, "CanPort80")
  )

proc parseMapDerpRegion(node: JsonNode): TsnetDerpRegionSnapshot =
  var nodes: seq[TsnetDerpNodeSnapshot] = @[]
  let nodeSet = jsonField(node, "Nodes")
  if nodeSet.kind == JArray:
    for item in nodeSet.items():
      if item.kind == JObject:
        nodes.add(parseMapDerpNode(item))
  TsnetDerpRegionSnapshot(
    regionId: jsonInt(node, "RegionID"),
    regionCode: jsonString(node, "RegionCode").toLowerAscii(),
    regionName: jsonString(node, "RegionName"),
    avoid: jsonBool(node, "Avoid"),
    noMeasureNoHome: jsonBool(node, "NoMeasureNoHome"),
    nodeCount: if nodes.len > 0: nodes.len else: jsonInt(node, "NodeCount"),
    nodes: nodes
  )

proc parseControlDerpMap*(node: JsonNode): Result[TsnetDerpMapSnapshot, string] =
  if node.isNil or node.kind != JObject:
    return err("tsnet control DERP map must be a JSON object")
  var regions: seq[TsnetDerpRegionSnapshot] = @[]
  let regionSet = jsonField(node, "Regions")
  case regionSet.kind
  of JObject:
    for _, regionNode in regionSet:
      if regionNode.kind == JObject:
        regions.add(parseMapDerpRegion(regionNode))
  of JArray:
    for item in regionSet.items():
      if item.kind == JObject:
        regions.add(parseMapDerpRegion(item))
  else:
    discard
  ok(TsnetDerpMapSnapshot(
    ok: regions.len > 0,
    error: if regions.len == 0: "missing_derp_map" else: "",
    omitDefaultRegions: jsonBool(node, "OmitDefaultRegions"),
    regionCount: if regions.len > 0: regions.len else: jsonInt(node, "RegionCount"),
    regions: regions
  ))

proc parseControlPeer(node: JsonNode, derpMap: TsnetDerpMapSnapshot): TsnetPeerSession =
  let hostinfo = jsonField(node, "Hostinfo")
  let addresses = mapStringSeq(node, "Addresses").mapIt(trimCidr(it)).filterIt(it.len > 0)
  let listenAddrs =
    mapStringSeq(node, "ListenAddrs").filterIt(it.len > 0) &
    mapStringSeq(hostinfo, "ListenAddrs").filterIt(it.len > 0)
  let homeDerpId = jsonInt(node, "HomeDERP")
  let regionCode = derpMap.mapRegionCodeById(homeDerpId)
  let topLevelPeerId = jsonString(node, "PeerID")
  let peerId =
    if topLevelPeerId.len > 0: topLevelPeerId
    else: jsonString(hostinfo, "PeerID")
  TsnetPeerSession(
    peerId: peerId,
    hostName: jsonString(hostinfo, "Hostname"),
    dnsName: jsonString(node, "Name"),
    tailscaleIPs: addresses,
    listenAddrs: distinctStrings(listenAddrs),
    online: true,
    active: true,
    curAddr: "",
    addrs: @[],
    relay: regionCode,
    peerRelay: regionCode,
    inMagicSock: false,
    inEngine: true,
    lastHandshakeUnixMilli: getTime().toUnix().int64 * 1000
  )

proc sessionFromControlMap*(
    source: string,
    capturedAtUnixMilli: int64,
    rawPayload: JsonNode,
    hostname: string,
    controlUrl: string,
    wireguardPort: int
): Result[TsnetSessionSnapshot, string] =
  if rawPayload.isNil or rawPayload.kind != JObject:
    return err("tsnet control map payload must be a JSON object")
  let derpMap = parseControlDerpMap(jsonField(rawPayload, "DERPMap")).valueOr:
    return err(error)
  let selfNode = jsonField(rawPayload, "Node")
  if selfNode.kind != JObject:
    return err("tsnet control map payload is missing Node")
  let selfHostinfo = jsonField(selfNode, "Hostinfo")
  let addresses = mapStringSeq(selfNode, "Addresses").mapIt(trimCidr(it)).filterIt(it.len > 0)
  let homeDerpId = jsonInt(selfNode, "HomeDERP")
  let mappedRegionCode = derpMap.mapRegionCodeById(homeDerpId)
  let regionCode =
    if mappedRegionCode.len > 0:
      mappedRegionCode
    elif derpMap.regions.len > 0:
      derpMap.regions[0].regionCode
    else:
      ""
  let derpSummary =
    if homeDerpId > 0 and regionCode.len > 0:
      $homeDerpId & "/" & regionCode
    elif derpMap.regions.len > 0:
      $derpMap.regions[0].regionId & "/" & derpMap.regions[0].regionCode
    else:
      ""
  var peers: seq[TsnetPeerSession] = @[]
  let peerNodes = jsonField(rawPayload, "Peers")
  if peerNodes.kind == JArray:
    for item in peerNodes.items():
      if item.kind == JObject:
        peers.add(parseControlPeer(item, derpMap))
  let primaryPeerIp =
    if peers.len > 0 and peers[0].tailscaleIPs.len > 0: peers[0].tailscaleIPs[0]
    elif addresses.len > 0: addresses[0]
    else: ""
  let derpRegionId =
    if homeDerpId > 0: homeDerpId
    elif derpMap.regions.len > 0: derpMap.regions[0].regionId
    else: 0
  ok(TsnetSessionSnapshot.init(
    source = source,
    capturedAtUnixMilli = capturedAtUnixMilli,
    status = TsnetStatusSnapshot(
      ok: true,
      error: "",
      started: true,
      backendState: "Mapped",
      authUrl: "",
      hostname:
        if hostname.len > 0: hostname
        else: jsonString(selfHostinfo, "Hostname"),
      controlUrl: controlUrl,
      wireguardPort: wireguardPort,
      tailnetIPs: addresses,
      tailnetPath: TsnetPathRelay,
      tailnetRelay: regionCode,
      tailnetDerpMapSummary: derpSummary,
      tailnetPeers: peers
    ),
    derpMap = derpMap,
    ping = TsnetPingSnapshot(
      ok: primaryPeerIp.len > 0,
      error: "",
      peerIP: primaryPeerIp,
      pingType: "TSMP",
      samples:
        if primaryPeerIp.len > 0:
          @[TsnetPingSample(
            sequence: 1,
            latencyMs: 1,
            endpoint: "",
            peerRelay: regionCode,
            derpRegionId: derpRegionId,
            derpRegionCode: regionCode,
            error: ""
          )]
        else:
          @[],
      minLatencyMs: if primaryPeerIp.len > 0: 1 else: 0,
      maxLatencyMs: if primaryPeerIp.len > 0: 1 else: 0,
      avgLatencyMs: if primaryPeerIp.len > 0: 1 else: 0,
      elapsedMs: if primaryPeerIp.len > 0: 1 else: 0
    )
  ))
