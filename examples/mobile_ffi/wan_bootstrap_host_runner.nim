{.used.}

import std/[json, os, parseopt, sequtils, sets, strutils]

from ./game_mesh_host_runner import
  HostNode, HostTransportMode, htmTcpExplicitSeed, htmTcpPublicSeed, htmQuicExplicitSeed,
  htmQuicDualStackFixedPort, boolField, bootstrapStatus, createWanBootstrapNode,
  discoverySnapshot, intField, joinViaBootstrap, nowMillis, registerBootstrapHint,
  publishDetectedNetworkStatus, startNode, stopNode, strField, waitUntil

proc parseTransportMode(text: string): HostTransportMode =
  case text.strip().toLowerAscii()
  of "", "quic":
    htmQuicExplicitSeed
  of "tcp":
    htmTcpExplicitSeed
  of "tcp-public":
    htmTcpPublicSeed
  of "quic-dual", "dual":
    htmQuicDualStackFixedPort
  else:
    raise newException(ValueError, "unknown transport: " & text)

proc parseStringSeq(raw: string): seq[string] =
  let trimmed = raw.strip()
  if trimmed.len == 0:
    return @[]
  if trimmed.startsWith("["):
    try:
      let parsed = parseJson(trimmed)
      if parsed.kind == JArray:
        for item in parsed:
          if item.kind == JString:
            let value = item.getStr().strip()
            if value.len > 0:
              result.add(value)
        return result
    except CatchableError:
      discard
  for part in trimmed.split(','):
    let value = part.strip()
    if value.len > 0:
      result.add(value)

proc containsPeer(status: JsonNode, peerId: string): bool =
  if peerId.len == 0 or status.kind != JObject:
    return false
  if status.hasKey("connectedPeers") and status["connectedPeers"].kind == JArray:
    for item in status["connectedPeers"]:
      if item.kind == JString and item.getStr() == peerId:
        return true
  if status.hasKey("selectedCandidates") and status["selectedCandidates"].kind == JArray:
    for item in status["selectedCandidates"]:
      if item.kind == JObject and strField(item, "peerId") == peerId:
        return true
  false

proc multiaddrProtocolValue(maddr, proto: string): string =
  if maddr.len == 0:
    return ""
  let marker = "/" & proto.toLowerAscii() & "/"
  let lower = maddr.toLowerAscii()
  let idx = lower.find(marker)
  if idx < 0:
    return ""
  let startIdx = idx + marker.len
  var endIdx = maddr.find('/', startIdx)
  if endIdx < 0:
    endIdx = maddr.len
  maddr[startIdx ..< endIdx]

proc isGlobalIpv6(maddr: string): bool =
  let lower = maddr.toLowerAscii()
  "/ip6/" in lower and "/ip6/::/" notin lower and "/ip6/fe80:" notin lower

proc stringSeqField(node: JsonNode, key: string): seq[string] =
  if node.kind != JObject or not node.hasKey(key) or node[key].kind != JArray:
    return @[]
  for item in node[key]:
    if item.kind == JString:
      let value = item.getStr().strip()
      if value.len > 0:
        result.add(value)

proc canonicalSeedMultiaddrs(node: HostNode; snapshot: JsonNode): seq[string] =
  var seen = initHashSet[string]()
  let snapshotDialable = stringSeqField(snapshot, "dialableAddresses")
  for addr in snapshotDialable:
    if not isGlobalIpv6(addr):
      continue
    let lower = addr.toLowerAscii()
    if "/quic-v1" notin lower and "/tcp/" notin lower:
      continue
    if "/quic-v1" in lower and "/udp/" notin lower:
      continue
    let canonical =
      if "/p2p/" in addr or "/ipfs/" in addr:
        addr
      else:
        addr & "/p2p/" & node.peerId
    if canonical notin seen:
      seen.incl(canonical)
      result.add(canonical)

proc bootstrapSummary(
    node: HostNode,
    label: string,
    role: string,
    networkId: string,
    joinResult: JsonNode,
    expectedPeerId: string,
    learnedExpectedPeer: bool
): JsonNode =
  let bootstrap = node.bootstrapStatus()
  let snapshot = node.discoverySnapshot(limit = 64, connectCap = 0)
  let seedMultiaddrs = canonicalSeedMultiaddrs(node, snapshot)
  %*{
    "ok": boolField(joinResult, "ok", joinResult.kind == JNull or joinResult.len == 0) and
      (expectedPeerId.len == 0 or learnedExpectedPeer),
    "label": label,
    "role": role,
    "networkId": networkId,
    "peerId": node.peerId,
    "dialAddr": node.dialAddr,
    "dialAddrs": node.dialAddrs,
    "seedMultiaddrs": seedMultiaddrs,
    "joinResult": joinResult,
    "bootstrap": bootstrap,
    "discoverySnapshot": snapshot,
    "expectedPeerId": expectedPeerId,
    "learnedExpectedPeer": learnedExpectedPeer,
    "generatedAtMs": nowMillis()
  }

proc writeRunnerSummary(
    node: HostNode,
    label: string,
    role: string,
    networkId: string,
    joinResult: JsonNode,
    expectedPeerId: string,
    learnedExpectedPeer: bool,
    summaryOut: string,
): JsonNode =
  let summary = bootstrapSummary(
    node,
    label,
    role,
    networkId,
    joinResult,
    expectedPeerId,
    learnedExpectedPeer
  )
  if summaryOut.len > 0:
    writeFile(summaryOut, pretty(summary))
  echo pretty(summary)
  stdout.flushFile()
  summary

when isMainModule:
  var label = "wan-bootstrap-host"
  var role = "mobile"
  var networkId = "unimaker-mobile"
  var transportMode = htmQuicExplicitSeed
  var listenPort = 0
  var enablePublicBootstrap = false
  var staticBootstrap: seq[string] = @[]
  var seedPeerId = ""
  var seedAddrs: seq[string] = @[]
  var seedSource = "manual_direct_hint"
  var joinAfterSeed = true
  var bootstrapLimit = 0
  var settleMs = 1_500
  var serveMs = 0
  var expectPeerId = ""
  var expectTimeoutMs = 12_000
  var summaryOut = ""

  var parser = initOptParser(commandLineParams())
  for kind, key, value in parser.getopt():
    case kind
    of cmdLongOption, cmdShortOption:
      case key.normalize()
      of "label":
        label = value
      of "role":
        role = value
      of "networkid", "network-id":
        networkId = value
      of "transport":
        transportMode = parseTransportMode(value)
      of "listenport", "listen-port":
        if value.len > 0:
          listenPort = parseInt(value)
      of "enablepublicbootstrap", "enable-public-bootstrap":
        let lowered = value.strip().toLowerAscii()
        enablePublicBootstrap = lowered in ["", "1", "true", "yes", "on"]
      of "staticbootstrap", "static-bootstrap":
        staticBootstrap = parseStringSeq(value)
      of "seedpeerid", "seed-peer-id":
        seedPeerId = value.strip()
      of "seedaddrs", "seed-addrs":
        seedAddrs = parseStringSeq(value)
      of "seedsource", "seed-source":
        if value.strip().len > 0:
          seedSource = value.strip()
      of "join":
        let lowered = value.strip().toLowerAscii()
        joinAfterSeed = lowered in ["", "1", "true", "yes", "on"]
      of "bootstraplimit", "bootstrap-limit":
        if value.len > 0:
          bootstrapLimit = parseInt(value)
      of "settlems", "settle-ms":
        if value.len > 0:
          settleMs = max(parseInt(value), 0)
      of "servems", "serve-ms":
        if value.len > 0:
          serveMs = max(parseInt(value), 0)
      of "expectpeerid", "expect-peer-id":
        expectPeerId = value.strip()
      of "expecttimeoutms", "expect-timeout-ms":
        if value.len > 0:
          expectTimeoutMs = max(parseInt(value), 1_000)
      of "summaryout", "summary-out":
        summaryOut = value.strip()
      else:
        raise newException(ValueError, "unknown option: " & key)
    of cmdArgument:
      discard
    of cmdEnd:
      discard

  var node = createWanBootstrapNode(
    label = label,
    role = role,
    networkId = networkId,
    transportMode = transportMode,
    listenPort = listenPort,
    enablePublicBootstrap = enablePublicBootstrap,
    staticBootstrap = staticBootstrap
  )
  var joinResult = newJObject()
  var learnedExpectedPeer = expectPeerId.len == 0
  try:
    startNode(node)
    publishDetectedNetworkStatus(
      node,
      networkType = "ethernet",
      transport = "ethernet",
      reason = "wan_bootstrap_host_init",
      preferIpv6 = true
    )
    if seedPeerId.len > 0 and seedAddrs.len > 0:
      if not registerBootstrapHint(node, seedPeerId, seedAddrs, seedSource):
        raise newException(IOError, "registerBootstrapHint failed for " & seedPeerId)
    if joinAfterSeed:
      joinResult = joinViaBootstrap(node, bootstrapLimit)
    if expectPeerId.len > 0:
      learnedExpectedPeer = waitUntil(
        nowMillis() + int64(expectTimeoutMs),
        150,
        proc(): bool =
          containsPeer(node.bootstrapStatus(), expectPeerId)
      )
    elif settleMs > 0:
      sleep(settleMs)

    publishDetectedNetworkStatus(
      node,
      networkType = "ethernet",
      transport = "ethernet",
      reason = "wan_bootstrap_host_ready",
      preferIpv6 = true
    )

    var summary = writeRunnerSummary(
      node,
      label,
      role,
      networkId,
      joinResult,
      expectPeerId,
      learnedExpectedPeer,
      summaryOut
    )
    if serveMs > 0:
      sleep(serveMs)
      if expectPeerId.len > 0 and not learnedExpectedPeer:
        learnedExpectedPeer = containsPeer(node.bootstrapStatus(), expectPeerId)
      summary = writeRunnerSummary(
        node,
        label,
        role,
        networkId,
        joinResult,
        expectPeerId,
        learnedExpectedPeer,
        summaryOut
      )
    quit(if boolField(summary, "ok", false): 0 else: 1)
  finally:
    stopNode(node)
