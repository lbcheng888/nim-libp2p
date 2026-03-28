{.used.}

{.emit: """
void nim_bridge_emit_event(const char *topic, const char *payload) {
  (void)topic;
  (void)payload;
}
""".}

import std/[base64, json, os, unittest]
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

proc newTestNode(prefix: string, extra: JsonNode): pointer =
  let cfg = %*{
    "identity": testIdentity(),
    "dataDir": testDataDir(prefix),
    "extra": extra,
  }
  let handle = libp2p_node_init(($cfg).cstring)
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

  test "bootstrap status exposes routing status after node start":
    let handle = newTestNode(
      "bootstrap-status",
      %*{
        "disablePublicBootstrap": true,
        "routingMode": "dualStack",
        "primaryRoutingPlane": "lsmr",
        "lsmrNetworkId": "mobile-ffi-routing-bootstrap",
        "lsmrPath": [5]
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
