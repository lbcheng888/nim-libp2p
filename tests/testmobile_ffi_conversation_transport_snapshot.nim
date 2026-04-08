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

proc takeCStringAndFree(value: cstring): string =
  if value.isNil:
    return ""
  result = $value
  libp2p_string_free(value)

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

template safeCStringExpr(valueExpr: untyped): string =
  block:
    try:
      let value = valueExpr
      takeCStringAndFree(value)
    except CatchableError:
      ""

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

proc testPeerId(): string =
  let pair = KeyPair.random(PKScheme.Ed25519, rng[]).get()
  $PeerId.init(pair.seckey).get()

proc newTestNodeWithDir(
    dataDir: string,
    listenAddrs: seq[string] = @["/ip4/127.0.0.1/tcp/0"],
): pointer =
  let config = %* {
    "identity": testIdentity(),
    "dataDir": dataDir,
    "listenAddresses": %*listenAddrs,
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
  let deadline = epochTime() + 15.0
  while epochTime() <= deadline:
    if libp2p_node_is_started(handle):
      return
    sleep(150)
  check false

proc routeAddrsJson(addrs: seq[string]): string =
  $(%*addrs)

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
    if multiaddr.contains("/tcp/0/") or multiaddr.endsWith("/tcp/0"):
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

proc upsertRouteIntent(
    handle: pointer,
    peerId: string,
    addrs: seq[string],
    source: string,
    policy: string,
    limit = 0,
): bool =
  libp2p_path_intent_upsert(
    handle,
    peerId.cstring,
    routeAddrsJson(addrs).cstring,
    source.cstring,
    policy.cstring,
    cint(limit),
  )

proc clearRouteIntent(
    handle: pointer,
    peerId: string,
    addrs: seq[string],
    limit = 0,
): bool =
  libp2p_path_intent_clear(
    handle,
    peerId.cstring,
    routeAddrsJson(addrs).cstring,
    cint(limit),
  )

proc routeSnapshot(
    handle: pointer,
    peerId: string,
    addrs: seq[string],
    limit = 0,
): JsonNode =
  safeJsonExpr(
    libp2p_path_snapshot_for_addresses(
      handle,
      peerId.cstring,
      routeAddrsJson(addrs).cstring,
      cint(limit),
    )
  )

proc directSessionTargetSet(
    handle: pointer,
    peerId: string,
    addrs: seq[string],
    source: string,
    limit = 0,
): bool =
  libp2p_direct_session_target_set(
    handle,
    peerId.cstring,
    routeAddrsJson(addrs).cstring,
    source.cstring,
    cint(limit),
  )

proc directSessionTargetStatus(
    handle: pointer,
    peerId: string,
    addrs: seq[string],
    limit = 0,
): JsonNode =
  safeJsonExpr(
    libp2p_direct_session_target_status_for_addresses(
      handle,
      peerId.cstring,
      routeAddrsJson(addrs).cstring,
      cint(limit),
    ),
    """{"status":"missing","payload":{"status":"missing"}}"""
  )

proc relaySessionStatus(
    handle: pointer,
    peerId: string,
): JsonNode =
  safeJsonExpr(
    libp2p_relay_session_status(
      handle,
      peerId.cstring,
    ),
    """{"ok":false,"status":"missing","error":"missing_peer_id"}"""
  )

proc prepareRelaySession(
    handle: pointer,
    peerId: string,
    source = "ffi_test_prepare",
): bool =
  libp2p_prepare_relay_session(
    handle,
    peerId.cstring,
    source.cstring,
  )

proc sendWithAckKickoff(
    handle: pointer,
    requestKey: string,
    peerId: string,
    payload: string,
    timeoutMs: int,
    preferredSessionConnKey: int,
): bool =
  libp2p_send_with_ack_kickoff(
    handle,
    requestKey.cstring,
    peerId.cstring,
    payload.cstring,
    cint(timeoutMs),
    clonglong(preferredSessionConnKey),
  )

proc sendWithAckSeededKickoff(
    handle: pointer,
    requestKey: string,
    peerId: string,
    payload: string,
    addrs: seq[string],
    source: string,
    timeoutMs: int,
    preferredSessionConnKey: int,
    relayOnly: bool,
): bool =
  libp2p_send_with_ack_seeded_kickoff(
    handle,
    requestKey.cstring,
    peerId.cstring,
    payload.cstring,
    routeAddrsJson(addrs).cstring,
    source.cstring,
    cint(timeoutMs),
    clonglong(preferredSessionConnKey),
    relayOnly,
  )

proc sendWithAckKickoffStatus(
    requestKey: string
): JsonNode =
  safeJsonExpr(
    libp2p_send_with_ack_kickoff_status(requestKey.cstring),
    """{"status":"missing","payload":{}}"""
  )

suite "Mobile FFI conversation transport snapshot":
  test "path intent upsert updates policy on the same route":
    let handle = newTestNodeWithDir(testDataDir("conversation-transport-policy"))
    defer:
      stopAndFreeNode(handle)

    startNode(handle)

    let peerId = "12D3KooWTransportSnapshotPeer"
    let addrs = @[
      "/ip4/100.64.0.10/udp/40001/quic-v1/tsnet/p2p/12D3KooWTransportSnapshotPeer"
    ]

    check upsertRouteIntent(handle, peerId, addrs, "ffi_test", "direct_then_relay", 1)
    let snapshot1 = routeSnapshot(handle, peerId, addrs, 1)
    check snapshot1["peerId"].getStr() == peerId
    check snapshot1["policy"].getStr() == "direct_then_relay"
    check snapshot1["routeKey"].getStr().len > 0
    check snapshot1["selectedAddrs"].len == 1
    check snapshot1["selectedAddrs"][0].getStr() == addrs[0]
    check snapshot1["status"].getStr().len > 0

    check upsertRouteIntent(handle, peerId, addrs, "ffi_test", "relay_only", 1)
    let snapshot2 = routeSnapshot(handle, peerId, addrs, 1)
    check snapshot2["routeKey"].getStr() == snapshot1["routeKey"].getStr()
    check snapshot2["policy"].getStr() == "relay_only"
    check snapshot2["selectedAddrs"][0].getStr() == addrs[0]

  test "relay_only intent uses seeded route instead of failing route_missing":
    let handle = newTestNodeWithDir(testDataDir("conversation-transport-relay-seed"))
    defer:
      stopAndFreeNode(handle)

    startNode(handle)

    let peerId = testPeerId()
    let addrs = @[
      "/ip4/100.64.0.40/udp/43001/quic-v1/tsnet/p2p/" & peerId
    ]

    check upsertRouteIntent(handle, peerId, addrs, "ffi_test_relay_seed", "relay_only", 1)

    let deadline = epochTime() + 2.0
    var snapshot = routeSnapshot(handle, peerId, addrs, 1)
    while epochTime() <= deadline:
      snapshot = routeSnapshot(handle, peerId, addrs, 1)
      if snapshot["relayStage"].getStr() != "missing" or
          snapshot["relayError"].getStr().len > 0 or
          snapshot["relayLastDialStage"].getStr().len > 0:
        break
      sleep(100)

    check snapshot["policy"].getStr() == "relay_only"
    check snapshot["selectedAddrs"].len == 1
    check snapshot["selectedAddrs"][0].getStr() == addrs[0]
    check snapshot["relayError"].getStr() != "route_missing"
    check snapshot["relayLastDialStage"].getStr() != "route_missing"

  test "clearing one route does not clear a sibling route for the same peer":
    let handle = newTestNodeWithDir(testDataDir("conversation-transport-clear"))
    defer:
      stopAndFreeNode(handle)

    startNode(handle)

    let peerId = "12D3KooWTransportSnapshotSibling"
    let addrsA = @[
      "/ip4/100.64.0.20/udp/41001/quic-v1/tsnet/p2p/12D3KooWTransportSnapshotSibling"
    ]
    let addrsB = @[
      "/ip4/100.64.0.20/udp/41002/quic-v1/tsnet/p2p/12D3KooWTransportSnapshotSibling"
    ]

    check upsertRouteIntent(handle, peerId, addrsA, "ffi_test_route_a", "direct_then_relay", 1)
    check upsertRouteIntent(handle, peerId, addrsB, "ffi_test_route_b", "relay_only", 1)

    let snapshotA = routeSnapshot(handle, peerId, addrsA, 1)
    let snapshotB = routeSnapshot(handle, peerId, addrsB, 1)
    check snapshotA["routeKey"].getStr().len > 0
    check snapshotB["routeKey"].getStr().len > 0
    check snapshotA["routeKey"].getStr() != snapshotB["routeKey"].getStr()
    check snapshotB["policy"].getStr() == "relay_only"

    check clearRouteIntent(handle, peerId, addrsA, 1)

    let clearedA = routeSnapshot(handle, peerId, addrsA, 1)
    let stillLiveB = routeSnapshot(handle, peerId, addrsB, 1)
    check clearedA["status"].getStr() == "missing"
    check clearedA["lastError"].getStr() == "missing_route_intent"
    check stillLiveB["routeKey"].getStr() == snapshotB["routeKey"].getStr()
    check stillLiveB["policy"].getStr() == "relay_only"
    check stillLiveB["status"].getStr() != "missing"

  test "path snapshot stays readable across repeated queries":
    let handle = newTestNodeWithDir(testDataDir("conversation-transport-repeat"))
    defer:
      stopAndFreeNode(handle)

    startNode(handle)

    let peerId = "12D3KooWTransportSnapshotRepeat"
    let addrs = @[
      "/ip4/100.64.0.30/udp/42001/quic-v1/tsnet/p2p/12D3KooWTransportSnapshotRepeat"
    ]

    check upsertRouteIntent(handle, peerId, addrs, "ffi_test_repeat", "direct_then_relay", 1)
    let first = routeSnapshot(handle, peerId, addrs, 1)
    let expectedRouteKey = first["routeKey"].getStr()
    check expectedRouteKey.len > 0

    for _ in 0 ..< 24:
      let snapshot = routeSnapshot(handle, peerId, addrs, 1)
      check snapshot["routeKey"].getStr() == expectedRouteKey
      check snapshot["peerId"].getStr() == peerId
      check snapshot["policy"].getStr() == "direct_then_relay"
      check snapshot.hasKey("directAttemptCount")
      check snapshot.hasKey("directSuccessCount")
      check snapshot.hasKey("directSuccessRatePct")
      check snapshot.hasKey("directDmLatencyMs")
      check snapshot.hasKey("relayDmLatencyMs")

  test "clearing the last relay route keeps sticky peer relay heat":
    let handle = newTestNodeWithDir(testDataDir("conversation-transport-relay-clear"))
    defer:
      stopAndFreeNode(handle)

    startNode(handle)

    let peerId = testPeerId()
    let addrs = @[
      "/ip4/100.64.0.50/udp/44001/quic-v1/tsnet/p2p/" & peerId
    ]

    check upsertRouteIntent(handle, peerId, addrs, "ffi_test_relay_clear", "relay_only", 1)
    check prepareRelaySession(handle, peerId, "ffi_test_relay_clear_prepare")

    let warmDeadline = epochTime() + 1.0
    var activeStatus = relaySessionStatus(handle, peerId)
    while epochTime() <= warmDeadline:
      activeStatus = relaySessionStatus(handle, peerId)
      if activeStatus["status"].getStr() != "missing":
        break
      sleep(25)
    check activeStatus["status"].getStr() != "missing"

    check clearRouteIntent(handle, peerId, addrs, 1)

    let clearedRoute = routeSnapshot(handle, peerId, addrs, 1)
    check clearedRoute["status"].getStr() == "missing"
    check clearedRoute["lastError"].getStr() == "missing_route_intent"

    let retainDeadline = epochTime() + 0.35
    var retainedStatus = relaySessionStatus(handle, peerId)
    while epochTime() <= retainDeadline:
      retainedStatus = relaySessionStatus(handle, peerId)
      if retainedStatus["status"].getStr() != "missing":
        break
      sleep(25)
    check retainedStatus["status"].getStr() != "missing"

  test "clearing a failed relay route still retains peer relay heat":
    let handle = newTestNodeWithDir(testDataDir("conversation-transport-relay-retain"))
    defer:
      stopAndFreeNode(handle)

    startNode(handle)

    let peerId = "12D3KooWTransportRetainInvalid"
    let addrs = @[
      "/ip4/100.64.0.55/udp/44005/quic-v1/tsnet/p2p/" & peerId
    ]

    check upsertRouteIntent(handle, peerId, addrs, "ffi_test_relay_retain", "relay_only", 1)

    var failedStatus = relaySessionStatus(handle, peerId)
    waitUntilSatisfied(2_000, 25):
      failedStatus = relaySessionStatus(handle, peerId)
      failedStatus["status"].getStr() != "running"

    check clearRouteIntent(handle, peerId, addrs, 1)

    var retainedStatus = relaySessionStatus(handle, peerId)
    waitUntilSatisfied(1_500, 50):
      retainedStatus = relaySessionStatus(handle, peerId)
      retainedStatus["status"].getStr() != "missing"

    check retainedStatus["status"].getStr() != "missing"

  test "seeded kickoff never reintroduces fake ack reserve":
    let nodeA = newTestNodeWithDir(testDataDir("conversation-transport-budget"))
    defer:
      stopAndFreeNode(nodeA)

    startNode(nodeA)

    let peerB = testPeerId()
    check peerB.len > 0

    let requestKey = "conversation-transport-budget-" & $nowMillis()
    let addrs = @[
      "/ip4/100.64.0.61/udp/45001/quic-v1/tsnet/p2p/" & peerB
    ]
    let payload = %*{
      "mid": requestKey,
      "messageId": requestKey,
      "op": "text",
      "type": "text",
      "text": "budget-regression",
      "body": "budget-regression",
      "content": "budget-regression",
      "conversationId": "dm:" & requestKey,
      "timestampMs": nowMillis(),
      "ackRequested": true,
    }

    check sendWithAckSeededKickoff(
      nodeA,
      requestKey,
      peerB,
      $payload,
      addrs,
      "ffi_test_seeded_budget",
      2_800,
      0,
      true,
    )

    var kickoffStatus = newJNull()
    waitUntilSatisfied(12_000, 50):
      kickoffStatus = sendWithAckKickoffStatus(requestKey)
      kickoffStatus.kind == JObject and kickoffStatus["status"].getStr() == "done"

    check kickoffStatus["status"].getStr() == "done"
    check kickoffStatus["payload"]["stage"].getStr().len > 0
    check kickoffStatus["payload"].hasKey("reservedAckBudgetMs")
    check kickoffStatus["payload"]["reservedAckBudgetMs"].getInt() == 0
