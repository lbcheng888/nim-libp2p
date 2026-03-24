{.used.}

{.emit: """
void nim_bridge_emit_event(const char *topic, const char *payload) {
  (void)topic;
  (void)payload;
}
""".}

import std/[base64, json, os, sequtils, strutils]
from std/times import epochTime

import unittest2

import ./helpers
import ../libp2p/crypto/crypto
import ../libp2p/peerid
import ../examples/mobile_ffi/libnimlibp2p

proc nowMillis(): int64 =
  int64(epochTime() * 1000)

template waitUntilSatisfied(timeoutMs: int, intervalMs: int, body: untyped) =
  block:
    let deadline = nowMillis() + int64(timeoutMs)
    var satisfied = false
    while nowMillis() <= deadline:
      let ok = block:
        body
      if ok:
        satisfied = true
        break
      sleep(intervalMs)
    check satisfied

proc takeCStringAndFree(value: cstring): string =
  if value.isNil:
    return ""
  result = $value
  libp2p_string_free(value)

proc safeCString(value: cstring): string {.raises: [].} =
  try:
    takeCStringAndFree(value)
  except CatchableError:
    ""

template safeCStringExpr(valueExpr: untyped): string =
  block:
    try:
      let value = valueExpr
      safeCString(value)
    except CatchableError:
      ""

proc fallbackJsonNode(fallback: string): JsonNode {.raises: [].} =
  if fallback.len > 0 and fallback[0] == '[':
    newJArray()
  else:
    newJObject()

proc safeJsonFromCString(value: cstring, fallback: string = "{}"): JsonNode {.raises: [].} =
  try:
    let text = safeCString(value)
    if text.len == 0:
      return fallbackJsonNode(fallback)
    parseJson(text)
  except CatchableError:
    fallbackJsonNode(fallback)

template safeJsonExpr(valueExpr: untyped, fallback: string = "{}"): JsonNode =
  block:
    try:
      let value = valueExpr
      safeJsonFromCString(value, fallback)
    except CatchableError:
      fallbackJsonNode(fallback)

proc testDataDir(prefix: string): string =
  let path = getTempDir() / "nim-libp2p-mobile-ffi-tests" / (prefix & "-" & $nowMillis())
  createDir(path)
  path

proc testIdentity(): JsonNode =
  let pair = KeyPair.random(PKScheme.Ed25519, rng[]).get()
  let privateKey = pair.seckey.getRawBytes().get()
  let publicKey = pair.pubkey.getRawBytes().get()
  let peerId = $PeerId.init(pair.seckey).get()
  %* {
    "privateKey": base64.encode(privateKey),
    "publicKey": base64.encode(publicKey),
    "peerId": peerId,
    "source": "test",
  }

proc dualStackListenAddrs(port: int): JsonNode =
  %*[
    "/ip4/127.0.0.1/udp/" & $port & "/quic-v1",
    "/ip6/::1/udp/" & $port & "/quic-v1",
    "/ip4/127.0.0.1/tcp/" & $port,
    "/ip6/::1/tcp/" & $port
  ]

proc newTestNode(prefix: string, listenAddrs: JsonNode = newJNull()): pointer =
  let identity = testIdentity()
  var config = %* {
    "identity": identity,
    "dataDir": testDataDir(prefix),
  }
  if listenAddrs.kind == JArray and listenAddrs.len > 0:
    config["listenAddresses"] = listenAddrs
  let handle = libp2p_node_init(($config).cstring)
  check not handle.isNil
  if handle.isNil:
    raise newException(IOError, "libp2p_node_init failed for " & prefix)
  let mdnsDisabled = libp2p_mdns_set_enabled(handle, false)
  check mdnsDisabled
  if not mdnsDisabled:
    raise newException(IOError, "libp2p_mdns_set_enabled(false) failed for " & prefix)
  handle

proc stopAndFreeNode(handle: pointer) =
  if handle.isNil:
    return
  discard libp2p_node_stop(handle)
  sleep(250)
  libp2p_node_free(handle)
  sleep(150)

proc startNode(handle: pointer) =
  check libp2p_node_start(handle) == 0
  waitUntilSatisfied(15_000, 150):
    libp2p_node_is_started(handle) and
    safeCStringExpr(libp2p_get_local_peer_id(handle)).len > 0

proc firstTcpAddrFromJson(raw: string, peerId: string, allowLoopbackFallback: bool): string =
  if raw.len == 0:
    return ""
  let parsed = parseJson(raw)
  if parsed.kind != JArray:
    return ""
  for item in parsed:
    if item.kind != JString:
      continue
    var multiaddr = item.getStr().strip()
    if not multiaddr.contains("/tcp/"):
      continue
    if allowLoopbackFallback:
      multiaddr = multiaddr.replace("/ip4/0.0.0.0/", "/ip4/127.0.0.1/")
      multiaddr = multiaddr.replace("/ip6/::/", "/ip6/::1/")
    if not multiaddr.contains("/p2p/") and not multiaddr.contains("/ipfs/"):
      multiaddr &= "/p2p/" & peerId
    return multiaddr
  ""

proc firstTcpDialableAddr(handle: pointer, peerId: string): string =
  result = firstTcpAddrFromJson(
    safeCStringExpr(libp2p_get_dialable_addresses(handle)),
    peerId,
    allowLoopbackFallback = false
  )
  if result.len > 0:
    return result
  result = firstTcpAddrFromJson(
    safeCStringExpr(libp2p_get_listen_addresses(handle)),
    peerId,
    allowLoopbackFallback = true
  )

proc findPeerInfo(entries: JsonNode, peerId: string): JsonNode =
  if entries.kind != JArray:
    return newJNull()
  for item in entries:
    if item.kind != JObject:
      continue
    let current =
      if item.hasKey("peerId") and item["peerId"].kind == JString:
        item["peerId"].getStr()
      elif item.hasKey("peer_id") and item["peer_id"].kind == JString:
        item["peer_id"].getStr()
      else:
        ""
    if current == peerId:
      return item
  newJNull()

suite "Mobile FFI host network status and muxer preference":
  test "host network status is normalized and exposed via events diagnostics and snapshots":
    let handle = newTestNode(
      "host-network",
      when defined(libp2p_msquic_experimental):
        %fullP2PListenAddresses(4011)
      else:
        %*["/ip4/127.0.0.1/tcp/4011"]
    )
    defer:
      stopAndFreeNode(handle)

    startNode(handle)

    let statusPayload = %* {
      "type": "HostNetworkStatus",
      "networkType": "WiFi",
      "transport": "NR",
      "ssid": "\"lab-ssid\"",
      "localIpv4": "192.168.3.23",
      "preferredIpv4": "192.168.3.23",
      "localIpv6": "2408:4001::23",
      "preferredIpv6": "2408:4001::23",
      "isConnected": true,
      "isMetered": true,
      "timestampMs": 1_735_689_600_123'i64,
      "reason": "capabilities_changed",
    }
    check libp2p_update_host_network_status(handle, ($statusPayload).cstring)

    let currentStatus = safeJsonExpr(libp2p_get_host_network_status_json(handle))
    check currentStatus["networkType"].getStr() == "wifi"
    check currentStatus["transport"].getStr() == "5g"
    check currentStatus["ssid"].getStr() == "lab-ssid"
    check currentStatus["preferredIpv4"].getStr() == "192.168.3.23"
    check currentStatus["preferredIpv6"].getStr() == "2408:4001::23"
    check currentStatus["isConnected"].getBool()
    check currentStatus["isMetered"].getBool()
    check currentStatus["reason"].getStr() == "capabilities_changed"

    var seenNetworkEvent = false
    var eventPayload = newJNull()
    waitUntilSatisfied(8_000, 150):
      let rawEvents = safeCStringExpr(libp2p_poll_events(handle, 32))
      if rawEvents.len > 0:
        let events = parseJson(rawEvents)
        if events.kind == JArray:
          for item in events:
            if item.kind != JObject:
              continue
            if item{"topic"}.getStr() != "network_event":
              continue
            let payloadNode =
              if item.hasKey("payload") and item["payload"].kind == JString:
                try:
                  parseJson(item["payload"].getStr())
                except CatchableError:
                  newJNull()
              else:
                item{"payload"}
            if payloadNode.kind == JObject and
               payloadNode{"kind"}.getStr() == "host" and
               payloadNode{"entity"}.getStr() == "network" and
               payloadNode{"op"}.getStr() == "status":
              seenNetworkEvent = true
              eventPayload = payloadNode{"payload"}
      seenNetworkEvent

    check eventPayload.kind == JObject
    check eventPayload["transport"].getStr() == "5g"

    let diagnostics = safeJsonExpr(libp2p_get_diagnostics_json(handle))
    check diagnostics["transportHealth"]["transportPolicy"].getStr() ==
      (when defined(libp2p_msquic_experimental): "quic_preferred" else: "tcp_only")
    check diagnostics["transportHealth"]["tcpNoDelay"].getBool()
    check diagnostics["transportHealth"]["physicalAddressPrefixes"].kind == JArray
    check diagnostics["transportHealth"]["hostNetworkStatus"]["transport"].getStr() == "5g"
    check diagnostics["systemProfile"]["network"]["networkType"].getStr() == "wifi"
    check diagnostics["systemProfile"]["network"]["preferredIpv6"].getStr() == "2408:4001::23"
    check diagnostics["hostNetworkStatus"]["ssid"].getStr() == "lab-ssid"

    let dialableAddrs = safeJsonExpr(libp2p_get_dialable_addresses(handle), "[]")
    when defined(libp2p_msquic_experimental):
      check dialableAddrs.getElems().anyIt(
        it.kind == JString and it.getStr().contains("/quic-v1")
      )
      check dialableAddrs.getElems().anyIt(
        it.kind == JString and it.getStr().contains("/awdl/")
      )

    let snapshot = safeJsonExpr(
      libp2p_network_discovery_snapshot(handle, "".cstring, 16, 4)
    )
    check snapshot["transportHealth"]["hostNetworkStatus"]["reason"].getStr() == "capabilities_changed"
    check snapshot["hostNetworkStatus"]["ssid"].getStr() == "lab-ssid"

  test "tcp peers negotiate yamux first and connectedPeersInfo exposes muxers":
    let nodeA = newTestNode("muxer-a")
    let nodeB = newTestNode("muxer-b")
    defer:
      stopAndFreeNode(nodeA)
      stopAndFreeNode(nodeB)

    startNode(nodeA)
    startNode(nodeB)

    let peerB = safeCStringExpr(libp2p_get_local_peer_id(nodeB))
    check peerB.len > 0

    var dialAddr = ""
    waitUntilSatisfied(10_000, 150):
      dialAddr = firstTcpDialableAddr(nodeB, peerB)
      dialAddr.len > 0

    check libp2p_connect_multiaddr(nodeA, dialAddr.cstring) == 0
    waitUntilSatisfied(15_000, 200):
      libp2p_is_peer_connected(nodeA, peerB.cstring)

    var peerInfo = newJNull()
    waitUntilSatisfied(15_000, 200):
      let connectedInfo = safeJsonExpr(libp2p_connected_peers_info(nodeA), "[]")
      peerInfo = findPeerInfo(connectedInfo, peerB)
      peerInfo.kind == JObject and peerInfo{"muxers"}.kind == JArray and peerInfo["muxers"].len > 0

    let muxers = peerInfo["muxers"].getElems().mapIt(it.getStr())
    check muxers.anyIt(it.contains("yamux"))
    check not muxers.anyIt(it.contains("mplex"))

    var snapshotMuxers: seq[string] = @[]
    waitUntilSatisfied(15_000, 200):
      let rawSnapshot = safeCStringExpr(
        libp2p_network_discovery_snapshot(nodeA, "".cstring, 8, 4)
      )
      snapshotMuxers = @[]
      if rawSnapshot.len == 0:
        false
      else:
        try:
          let snapshot = parseJson(rawSnapshot)
          let snapshotPeerInfo = findPeerInfo(snapshot["connectedPeersInfo"], peerB)
          if snapshotPeerInfo.kind == JObject and snapshotPeerInfo{"muxers"}.kind == JArray:
            snapshotMuxers = snapshotPeerInfo["muxers"].getElems().mapIt(it.getStr())
        except CatchableError:
          snapshotMuxers = @[]
        snapshotMuxers.anyIt(it.contains("yamux"))
    check snapshotMuxers.anyIt(it.contains("yamux"))
    check not snapshotMuxers.anyIt(it.contains("mplex"))
