{.used.}

{.emit: """
void nim_bridge_emit_event(const char *topic, const char *payload) {
  (void)topic;
  (void)payload;
}
""".}

import std/[base64, json, os]
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
    "listenAddresses": %fullP2PListenAddresses(4001),
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
  let deadline = nowMillis() + 15_000
  var started = false
  while nowMillis() <= deadline:
    if libp2p_node_is_started(handle):
      started = true
      break
    sleep(150)
  check started

suite "Mobile FFI bootstrap exact relay-backed seeds":
  test "relay-backed bootstrap seed survives seeded kickoff normalization":
    let handle = newTestNode("bootstrap-exact-relay-seed")
    defer:
      stopAndFreeNode(handle)

    startNode(handle)

    let relayPeerId = testIdentity()["peerId"].getStr()
    let targetPeerId = testIdentity()["peerId"].getStr()
    let relayedBootstrapAddr =
      "/ip6/2001:db8::44/udp/4001/quic-v1/p2p/" & relayPeerId &
      "/p2p-circuit/p2p/" & targetPeerId
    let requestKey = "relay_bootstrap_seeded_kickoff"
    let kickoffOk =
      libp2p_send_with_ack_seeded_kickoff(
        handle,
        requestKey.cstring,
        targetPeerId.cstring,
        """{"op":"text","body":"hello","ackRequested":true}""".cstring,
        ($(%[relayedBootstrapAddr])).cstring,
        "test_seeded_kickoff".cstring,
        1_000,
        0,
        true,
      )
    check kickoffOk

    var statusNode = newJObject()
    var payload = newJObject()
    let deadline = nowMillis() + 3_000
    while nowMillis() <= deadline:
      statusNode = parseJson(
        takeCStringAndFree(
          libp2p_send_with_ack_kickoff_status(requestKey.cstring)
        )
      )
      if statusNode.kind == JObject and statusNode.hasKey("payload"):
        let payloadNode = statusNode["payload"]
        if payloadNode.kind == JObject:
          payload = payloadNode
      if payload.kind == JObject and payload.hasKey("addressCount"):
        break
      sleep(25)

    check statusNode.kind == JObject
    check payload.kind == JObject
    check payload.hasKey("addressCount")
    check payload["addressCount"].getInt() == 1
