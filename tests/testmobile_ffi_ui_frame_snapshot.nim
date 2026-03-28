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
  let config = %* {
    "identity": testIdentity(),
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

suite "Mobile FFI UI frame snapshot":
  test "frame snapshot combines events discovery lan endpoints and feed":
    let handle = newTestNode("ui-frame")
    defer:
      stopAndFreeNode(handle)

    startNode(handle)

    let statusPayload = %* {
      "type": "HostNetworkStatus",
      "networkType": "wifi",
      "transport": "wifi",
      "ssid": "lab",
      "isConnected": true,
      "isMetered": false,
      "timestampMs": nowMillis(),
      "reason": "manual",
    }
    check libp2p_update_host_network_status(handle, ($statusPayload).cstring)

    var snapshot = newJNull()
    waitUntilSatisfied(10_000, 150):
      snapshot = safeJsonExpr(
        libp2p_ui_frame_snapshot(handle, 32, 32),
        """{"ok":false,"events":[],"discovery":{},"lanEndpoints":{"endpoints":[]},"feed":{"items":[]}}"""
      )
      snapshot.kind == JObject and
        snapshot["ok"].getBool() and
        snapshot["events"].kind == JArray and
        snapshot["discovery"].kind == JObject and
        snapshot["discovery"]["listenAddresses"].kind == JArray and
        snapshot["lanEndpoints"].kind == JObject and
        snapshot["lanEndpoints"]["endpoints"].kind == JArray and
        snapshot["feed"].kind == JObject and
        snapshot["feed"]["items"].kind == JArray

    check snapshot["running"].getBool()
    check snapshot["peerId"].getStr().len > 0
    check snapshot["discovery"]["transportHealth"]["muxerPreference"].kind == JArray
    check snapshot["discovery"]["transportHealth"]["quicRuntime"].kind == JObject
    check snapshot["discovery"]["transportHealth"]["quicRuntime"].hasKey("kind")
    check snapshot["discovery"]["transportHealth"]["quicRuntime"].hasKey("implementation")
    check snapshot["discovery"]["transportHealth"]["quicRuntime"].hasKey("path")
    check snapshot["discovery"]["transportHealth"]["quicRuntime"].hasKey("loaded")
    check snapshot["discovery"]["transportHealth"]["quicRuntime"].hasKey("compileTimeBuiltin")
    check snapshot["discovery"]["transportHealth"]["quicRuntime"].hasKey("pureNim")
    check snapshot["discovery"]["transportHealth"]["quicRuntime"].hasKey("requestedPreference")
    when defined(libp2p_msquic_experimental) and defined(libp2p_msquic_builtin):
      check snapshot["discovery"]["transportHealth"]["quicRuntime"]["requestedPreference"].getStr() == "builtin_only"
    check snapshot["discovery"]["transportHealth"]["quicRuntime"].hasKey("requestedLibraryPath")
    check snapshot["discovery"]["hostNetworkStatus"]["networkType"].getStr() == "wifi"
    check snapshot["discovery"]["hostNetworkStatus"].hasKey("publicIpv4")
    check snapshot["discovery"]["hostNetworkStatus"].hasKey("publicIpv6")
    check snapshot["discovery"]["hostNetworkStatus"].hasKey("publicIpv4RequiresHolePunching")
    check snapshot["discovery"]["bootstrap"].kind == JObject
    check not snapshot["discovery"]["bootstrap"].hasKey("ok")
    check snapshot["discovery"]["bootstrapProvider"].kind == JObject
    check snapshot["discovery"]["bootstrap"].hasKey("providerEligible")
    check snapshot["discovery"]["bootstrap"].hasKey("providerReason")
    check snapshot["discovery"]["bootstrap"].hasKey("providerPublicationMode")
    check snapshot["discovery"]["bootstrap"].hasKey("providerSeedAddrs")
    check snapshot["discovery"]["bootstrap"].hasKey("providerDirectSeedAddrs")
    check snapshot["discovery"]["bootstrap"].hasKey("providerRelaySeedAddrs")
    check snapshot["discovery"]["bootstrap"].hasKey("anchorSeedAddrs")
    check snapshot["discovery"]["bootstrap"].hasKey("hostPublicIpv4")
    check snapshot["discovery"]["bootstrap"].hasKey("hostPublicIpv6")
    check snapshot["discovery"]["bootstrap"].hasKey("publicIpv4RequiresHolePunching")
    check snapshot["discovery"]["bootstrap"].hasKey("providerNeedsHolePunching")
    check snapshot["discovery"]["bootstrap"].hasKey("exactSeedDialPolicy")
    check snapshot["discovery"]["bootstrap"]["exactSeedDialPolicy"].kind == JString
    check snapshot["discovery"]["bootstrap"]["exactSeedDialPolicy"].getStr().len > 0
    check snapshot["discovery"]["bootstrap"].hasKey("exactSeedAddrFallbackAllowed")
    check snapshot["discovery"]["bootstrap"]["exactSeedAddrFallbackAllowed"].kind == JBool
    check snapshot["discovery"]["bootstrap"].hasKey("exactSeedParallelDial")
    check snapshot["discovery"]["bootstrap"]["exactSeedParallelDial"].kind == JBool

    var sawNetworkEvent = false
    for item in snapshot["events"]:
      if item.kind != JObject:
        continue
      if item{"topic"}.getStr() == "network_event":
        sawNetworkEvent = true
        break
    check sawNetworkEvent
