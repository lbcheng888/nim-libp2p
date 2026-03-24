{.used.}

{.emit: """
void nim_bridge_emit_event(const char *topic, const char *payload) {
  (void)topic;
  (void)payload;
}
""".}

import std/[base64, json, os, strutils]
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

template safeCStringExpr(valueExpr: untyped): string =
  block:
    try:
      let value = valueExpr
      takeCStringAndFree(value)
    except CatchableError:
      ""

template safeJsonExpr(valueExpr: untyped, fallback: string = "{}"): JsonNode =
  block:
    try:
      let value = valueExpr
      let text = takeCStringAndFree(value)
      if text.len == 0:
        parseJson(fallback)
      else:
        parseJson(text)
    except CatchableError:
      parseJson(fallback)

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

proc newTestNode(prefix: string): pointer =
  let identity = testIdentity()
  let config = %* {
    "identity": identity,
    "dataDir": testDataDir(prefix),
  }
  let handle = libp2p_node_init(($config).cstring)
  check not handle.isNil
  check libp2p_mdns_set_enabled(handle, false)
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
    if item{"peerId"}.getStr() == peerId or item{"peer_id"}.getStr() == peerId:
      return item
  newJNull()

suite "Mobile FFI latency probe":
  test "runtime latency probe updates transport and dm ack stats":
    let nodeA = newTestNode("latency-a")
    let nodeB = newTestNode("latency-b")
    defer:
      stopAndFreeNode(nodeA)
      stopAndFreeNode(nodeB)

    startNode(nodeA)
    startNode(nodeB)

    let peerA = safeCStringExpr(libp2p_get_local_peer_id(nodeA))
    let peerB = safeCStringExpr(libp2p_get_local_peer_id(nodeB))
    check peerA.len > 0
    check peerB.len > 0

    var dialAddr = ""
    waitUntilSatisfied(10_000, 150):
      dialAddr = firstTcpDialableAddr(nodeB, peerB)
      dialAddr.len > 0

    check libp2p_connect_multiaddr(nodeA, dialAddr.cstring) == 0
    waitUntilSatisfied(15_000, 200):
      libp2p_is_peer_connected(nodeA, peerB.cstring) and
        libp2p_is_peer_connected(nodeB, peerA.cstring)

    let latencyProbe = safeJsonExpr(
      libp2p_measure_peer_latency(nodeA, peerB.cstring, 3, 1_500)
    )
    check latencyProbe["ok"].getBool()
    check latencyProbe["peerId"].getStr() == peerB
    check latencyProbe["successfulSamples"].getInt() >= 1
    check latencyProbe["samplesMs"].kind == JArray
    check latencyProbe["samplesMs"].len >= 1
    check latencyProbe["latencyMs"].getInt() >= 0
    check latencyProbe["latencyStats"]["transportRtt"]["totalSuccessCount"].getInt() >= 1

    let dmPayload = %* {"text": "latency-runtime"}
    check social_dm_send(
      nodeA,
      peerB.cstring,
      "dm:latency-runtime".cstring,
      ($dmPayload).cstring
    )

    var dmLatencyStats = newJNull()
    waitUntilSatisfied(10_000, 150):
      let connectedInfo = safeJsonExpr(libp2p_connected_peers_info(nodeA), "[]")
      let row = findPeerInfo(connectedInfo, peerB)
      if row.kind == JObject and row{"latencyStats"}.kind == JObject:
        dmLatencyStats = row["latencyStats"]
      dmLatencyStats.kind == JObject and
        dmLatencyStats["transportRtt"]["totalSuccessCount"].getInt() >= 1 and
        dmLatencyStats["dmAckRtt"]["totalSuccessCount"].getInt() >= 1

    check dmLatencyStats["preferredMetric"].getStr() in ["transport_rtt", "dm_ack_rtt"]
    check dmLatencyStats["dmAckRtt"]["lastMs"].getInt() >= 0
