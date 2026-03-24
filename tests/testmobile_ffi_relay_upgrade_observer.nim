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
  %*{
    "privateKey": base64.encode(privateKey),
    "publicKey": base64.encode(publicKey),
    "peerId": peerId,
    "source": "test",
  }

proc newTestNode(prefix: string, listenAddr: string): pointer =
  let identity = testIdentity()
  let config = %*{
    "identity": identity,
    "dataDir": testDataDir(prefix),
    "listenAddresses": [listenAddr],
  }
  let handle = libp2p_node_init(($config).cstring)
  check not handle.isNil
  if handle.isNil:
    raise newException(IOError, "libp2p_node_init failed for " & prefix)
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

proc directAddr(peerId: string, port: int): string =
  "/ip4/127.0.0.1/tcp/" & $port & "/p2p/" & peerId

proc fakeRelayRouteForDirectAddr(directAddr: string, relayPeerId: string, targetPeerId: string): string =
  let suffix = "/p2p/" & targetPeerId
  if directAddr.endsWith(suffix):
    return directAddr[0 ..< directAddr.len - suffix.len] &
      "/p2p/" & relayPeerId &
      "/p2p-circuit/p2p/" & targetPeerId
  directAddr & "/p2p/" & relayPeerId & "/p2p-circuit/p2p/" & targetPeerId

suite "Mobile FFI relay-first upgrade observer":
  test "reports already_direct when target is already directly connected":
    let nodeA = newTestNode("relay-upgrade-observer-a", "/ip4/127.0.0.1/tcp/4111")
    let nodeB = newTestNode("relay-upgrade-observer-b", "/ip4/127.0.0.1/tcp/4112")
    defer:
      stopAndFreeNode(nodeA)
      stopAndFreeNode(nodeB)

    startNode(nodeA)
    startNode(nodeB)

    let peerA = safeCStringExpr(libp2p_get_local_peer_id(nodeA))
    let peerB = safeCStringExpr(libp2p_get_local_peer_id(nodeB))
    check peerA.len > 0
    check peerB.len > 0

    let addrB = directAddr(peerB, 4112)
    check libp2p_connect_multiaddr(nodeA, addrB.cstring) == 0
    waitUntilSatisfied(15_000, 200):
      libp2p_is_peer_connected(nodeA, peerB.cstring)

    let relayAddr = fakeRelayRouteForDirectAddr(addrB, peerA, peerB)
    let addrsJson = $(%*[relayAddr, addrB])
    let payload = safeJsonExpr(
      libp2p_bootstrap_seed_observe_direct_upgrade(
        nodeA,
        peerB.cstring,
        addrsJson.cstring,
        "ffi_test".cstring,
        2.cint,
        2_000.cint,
        2_000.cint,
      )
    )

    check payload["ok"].getBool()
    check payload["stage"].getStr() == "already_direct"
    check payload["connection"]["selectedPath"].getStr() == "direct"
    check payload["connection"]["directConnected"].getBool()
    check payload["relayCandidateCount"].getInt() == 1
    check payload["directCandidateCount"].getInt() == 1

  test "rejects seed sets without relay candidates":
    let nodeA = newTestNode("relay-upgrade-missing-relay-a", "/ip4/127.0.0.1/tcp/4121")
    let nodeB = newTestNode("relay-upgrade-missing-relay-b", "/ip4/127.0.0.1/tcp/4122")
    defer:
      stopAndFreeNode(nodeA)
      stopAndFreeNode(nodeB)

    startNode(nodeA)
    startNode(nodeB)

    let peerB = safeCStringExpr(libp2p_get_local_peer_id(nodeB))
    check peerB.len > 0

    let addrsJson = $(%*[directAddr(peerB, 4122)])
    let payload = safeJsonExpr(
      libp2p_bootstrap_seed_observe_direct_upgrade(
        nodeA,
        peerB.cstring,
        addrsJson.cstring,
        "ffi_test".cstring,
        1.cint,
        1_500.cint,
        1_500.cint,
      )
    )

    check not payload["ok"].getBool()
    check payload["stage"].getStr() == "missing_relay_addresses"
    check payload["error"].getStr() == "missing_relay_addresses"
    check payload["relayCandidateCount"].getInt() == 0
    check payload["directCandidateCount"].getInt() == 1

  test "reports relay_connect_failed when relay path cannot be established":
    let nodeA = newTestNode("relay-upgrade-connect-failed-a", "/ip4/127.0.0.1/tcp/4131")
    let nodeB = newTestNode("relay-upgrade-connect-failed-b", "/ip4/127.0.0.1/tcp/4132")
    defer:
      stopAndFreeNode(nodeA)
      stopAndFreeNode(nodeB)

    startNode(nodeA)
    startNode(nodeB)

    let peerA = safeCStringExpr(libp2p_get_local_peer_id(nodeA))
    let peerB = safeCStringExpr(libp2p_get_local_peer_id(nodeB))
    check peerA.len > 0
    check peerB.len > 0

    let unreachableRelayAddr =
      "/ip4/127.0.0.1/tcp/45999/p2p/" & peerA &
        "/p2p-circuit/p2p/" & peerB
    let addrsJson = $(%*[unreachableRelayAddr])
    let payload = safeJsonExpr(
      libp2p_bootstrap_seed_observe_direct_upgrade(
        nodeA,
        peerB.cstring,
        addrsJson.cstring,
        "ffi_test".cstring,
        1.cint,
        1_500.cint,
        1_500.cint,
      )
    )

    check not payload["ok"].getBool()
    check payload["stage"].getStr() == "relay_connect_failed"
    check payload["relayCandidateCount"].getInt() == 1
    check payload["directCandidateCount"].getInt() == 0
    check not payload["relayObserved"].getBool()
    check not payload["directObserved"].getBool()
    check payload["attemptCount"].getInt() == 1
    check payload["attempts"][0]["path"].getStr() == "relay"
