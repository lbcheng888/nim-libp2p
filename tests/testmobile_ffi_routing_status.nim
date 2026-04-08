{.used.}

{.emit: """
void nim_bridge_emit_event(const char *topic, const char *payload) {
  (void)topic;
  (void)payload;
}
""".}

import std/[base64, json, os, strutils, unittest]
from std/times import epochTime

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

proc takeCString(value: cstring): string =
  if value.isNil:
    return ""
  result = $value
  libp2p_string_free(value)

proc testDataDir(prefix: string): string =
  let path = getTempDir() / "nim-libp2p-mobile-ffi-routing-status" / (prefix & "-" & $nowMillis())
  createDir(path)
  path

proc testIdentity(): JsonNode =
  let localRng = newRng()
  let pair = KeyPair.random(PKScheme.Ed25519, localRng[]).get()
  let privateKey = pair.seckey.getRawBytes().get()
  let publicKey = pair.pubkey.getRawBytes().get()
  let peerId = $PeerId.init(pair.seckey).get()
  %*{
    "privateKey": base64.encode(privateKey),
    "publicKey": base64.encode(publicKey),
    "peerId": peerId,
    "source": "test",
  }

proc newTestNodeWithIdentity(prefix: string, identity: JsonNode, extra: JsonNode): pointer =
  let cfg = %*{
    "identity": identity,
    "dataDir": testDataDir(prefix),
    "extra": extra,
  }
  let handle = libp2p_node_init(($cfg).cstring)
  check not handle.isNil
  if handle.isNil:
    raise newException(IOError, "libp2p_node_init failed for " & prefix)
  check libp2p_mdns_set_enabled(handle, false)
  handle

proc newTestNode(prefix: string, extra: JsonNode): pointer =
  newTestNodeWithIdentity(prefix, testIdentity(), extra)

proc deterministicPath(peerId: string, serveDepth: int): seq[int] =
  let normalized = peerId.strip()
  let suffixLen = max(0, min(8, max(1, serveDepth) - 1))
  if normalized.len == 0 or suffixLen == 0:
    return @[]
  var hash = 0'i64
  for ch in normalized:
    hash = (hash * 131'i64 + ord(ch).int64) mod 2147483647'i64
  for idx in 0 ..< suffixLen:
    hash = (hash * 131'i64 + (idx + 1).int64) mod 2147483647'i64
    result.add(int((hash mod 9'i64) + 1'i64))

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
    libp2p_node_is_started(handle)

suite "mobile ffi routing status":
  test "routing status getter reports configured routing planes":
    let cases = @[
      (
        "legacy",
        %*{
          "disablePublicBootstrap": true
        },
        "legacyOnly",
        "legacy",
        false
      ),
      (
        "dual-legacy",
        %*{
          "disablePublicBootstrap": true,
          "routingMode": "dualStack",
          "primaryRoutingPlane": "legacy",
          "lsmrNetworkId": "mobile-ffi-routing-dual-legacy",
          "lsmrPath": [5]
        },
        "dualStack",
        "legacy",
        true
      ),
      (
        "dual-lsmr",
        %*{
          "disablePublicBootstrap": true,
          "routingMode": "dualStack",
          "primaryRoutingPlane": "lsmr",
          "lsmrNetworkId": "mobile-ffi-routing-dual-lsmr",
          "lsmrPath": [5]
        },
        "dualStack",
        "lsmr",
        true
      ),
      (
        "lsmr-only",
        %*{
          "disablePublicBootstrap": true,
          "routingMode": "lsmrOnly",
          "primaryRoutingPlane": "lsmr",
          "lsmrNetworkId": "mobile-ffi-routing-lsmr-only",
          "lsmrPath": [5]
        },
        "lsmrOnly",
        "lsmr",
        false
      ),
    ]

    for (label, extra, expectedMode, expectedPrimary, expectedShadow) in cases:
      let handle = newTestNode(label, extra)
      defer:
        stopAndFreeNode(handle)

      let status = parseJson(takeCString(libp2p_get_routing_status_json(handle)))
      check status["mode"].getStr() == expectedMode
      check status["primary"].getStr() == expectedPrimary
      check status["shadowMode"].getBool() == expectedShadow
      check status.hasKey("legacyCandidates")
      check status.hasKey("legacyTrusted")
      check status.hasKey("lsmrActiveCertificates")
      check status.hasKey("lsmrMigrations")
      check status.hasKey("lsmrIsolations")
      check status.hasKey("lsmrNetworkId")
      check status.hasKey("lsmrPath")
      check status.hasKey("lsmrPathDerived")
      check status.hasKey("lsmrServeDepth")
      check status.hasKey("lsmrAnchorCount")

  test "routing status derives lsmr path from stable identity and exposes lsmr metadata":
    let identity = testIdentity()
    let peerId = identity["peerId"].getStr()
    let handle = newTestNodeWithIdentity(
      "dual-lsmr-derived",
      identity,
      %*{
        "disablePublicBootstrap": true,
        "routingMode": "dualStack",
        "primaryRoutingPlane": "lsmr",
        "lsmrNetworkId": "mobile-ffi-routing-derived",
        "lsmrServeDepth": 2,
        "lsmrAnchors": [
          {
            "peerId": "12D3KooWMJmRGksDpUMeaDSsJikJNgBs9M5fsJihHf8kPptSaUun",
            "addrs": [
              "/ip6/2001:19f0:4400:7c67:5400:6ff:fe04:54f8/udp/4001/quic-v1/p2p/12D3KooWMJmRGksDpUMeaDSsJikJNgBs9M5fsJihHf8kPptSaUun"
            ],
            "operatorId": "unimaker-root",
            "regionDigit": 5,
            "attestedPrefix": [5],
            "serveDepth": 2,
            "directionMask": 511,
            "canIssueRootCert": true
          }
        ]
      }
    )
    defer:
      stopAndFreeNode(handle)

    let status = parseJson(takeCString(libp2p_get_routing_status_json(handle)))
    check status["mode"].getStr() == "dualStack"
    check status["primary"].getStr() == "lsmr"
    check status["lsmrNetworkId"].getStr() == "mobile-ffi-routing-derived"
    check status["lsmrServeDepth"].getInt() == 2
    check status["lsmrAnchorCount"].getInt() == 1
    check status["lsmrPathDerived"].getBool()
    check status["lsmrPath"].kind == JArray
    let expectedPath = deterministicPath(peerId, 2)
    check status["lsmrPath"].len == expectedPath.len
    for idx, digit in expectedPath:
      check status["lsmrPath"][idx].getInt() == digit

  test "bootstrap status exposes routing status after node start":
    let handle = newTestNode(
      "bootstrap-status",
      %*{
        "disablePublicBootstrap": true,
        "routingMode": "dualStack",
        "primaryRoutingPlane": "lsmr",
        "lsmrNetworkId": "mobile-ffi-routing-bootstrap",
        "lsmrServeDepth": 2,
        "lsmrAnchors": [
          {
            "peerId": "12D3KooWMJmRGksDpUMeaDSsJikJNgBs9M5fsJihHf8kPptSaUun",
            "addrs": [
              "/ip6/2001:19f0:4400:7c67:5400:6ff:fe04:54f8/udp/4001/quic-v1/p2p/12D3KooWMJmRGksDpUMeaDSsJikJNgBs9M5fsJihHf8kPptSaUun"
            ],
            "operatorId": "unimaker-root",
            "regionDigit": 5,
            "attestedPrefix": [5],
            "serveDepth": 2,
            "directionMask": 511,
            "canIssueRootCert": true
          }
        ]
      }
    )
    defer:
      stopAndFreeNode(handle)

    startNode(handle)

    let bootstrap = parseJson(takeCString(libp2p_get_bootstrap_status(handle)))
    check bootstrap["routingMode"].getStr() == "dualStack"
    check bootstrap["primaryPlane"].getStr() == "lsmr"
    check bootstrap["shadowMode"].getBool()
    check bootstrap.hasKey("routingStatus")
    check bootstrap["routingStatus"]["mode"].getStr() == "dualStack"
    check bootstrap["routingStatus"]["primary"].getStr() == "lsmr"
    check bootstrap["routingStatus"]["shadowMode"].getBool()
    check bootstrap["routingStatus"]["lsmrNetworkId"].getStr() == "mobile-ffi-routing-bootstrap"
    check bootstrap["routingStatus"]["lsmrServeDepth"].getInt() == 2
    check bootstrap["routingStatus"]["lsmrAnchorCount"].getInt() == 1
    check bootstrap["routingStatus"]["lsmrPath"].kind == JArray
    check bootstrap["routingStatus"]["lsmrPath"].len > 0
    check bootstrap["routingStatus"]["lsmrPathDerived"].getBool()
