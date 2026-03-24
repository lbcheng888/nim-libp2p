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

suite "Mobile FFI bandwidth probe fallback":
  test "download samples are counted from direct-message probe chunks":
    let nodeA = newTestNode("probe-a")
    let nodeB = newTestNode("probe-b")
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

    check libp2p_wait_secure_channel_ffi(nodeA, peerB.cstring, 10_000)

    let probe = safeJsonExpr(libp2p_measure_peer_bandwidth(nodeA, peerB.cstring, 1_200, 4_096))
    check probe["ok"].getBool()
    check probe["uploadChunkCount"].getInt() > 0
    check probe["uploadPayloadBytes"].getInt() > 0
    check probe["uplinkSampleBytes"].getInt() > 0
    check probe["downloadChunkCount"].getInt() > 0
    check probe["downlinkSampleBytes"].getInt() > 0
    check probe["uplinkBps"].getFloat() > 0
    check probe["downlinkBps"].getFloat() > 0
