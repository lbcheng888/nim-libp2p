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

proc fixturePath(name: string): string =
  currentSourcePath.parentDir() / "fixtures" / "tsnet" / name

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

proc newTestNodeWithConfig(prefix: string, extraConfig: JsonNode): pointer =
  let identity = testIdentity()
  var config = %* {
    "identity": identity,
    "dataDir": testDataDir(prefix),
    "listenAddresses": %fullP2PListenAddresses(4001),
  }
  if extraConfig.kind == JObject:
    config["extra"] = extraConfig
    for key, value in extraConfig:
      config[key] = value
  let handle = libp2p_node_init(($config).cstring)
  check not handle.isNil
  if handle.isNil:
    raise newException(IOError, "libp2p_node_init failed for " & prefix)
  check libp2p_mdns_set_enabled(handle, false)
  handle

proc newTsnetTestNodeWithConfig(
    prefix: string,
    extraConfig: JsonNode,
    warmFixture = false,
): pointer =
  let dataDir = testDataDir(prefix)
  if warmFixture:
    copyFile(fixturePath("self_hosted_901_sin.json"), dataDir / "nim-tsnet-oracle.json")
  let identity = testIdentity()
  var config = %* {
    "identity": identity,
    "dataDir": dataDir,
    "listenAddresses": %*[
      "/ip4/0.0.0.0/tcp/0/tsnet",
      "/ip4/0.0.0.0/udp/0/quic-v1/tsnet",
    ],
  }
  if extraConfig.kind == JObject:
    config["extra"] = extraConfig
    for key, value in extraConfig:
      config[key] = value
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
    libp2p_node_is_started(handle)

suite "Mobile FFI WAN provider seed synthesis":
  test "bootstrap status synthesizes public ipv6 quic seeds from host status listenPort":
    let handle = newTestNode("wan-provider-seeds")
    defer:
      stopAndFreeNode(handle)

    startNode(handle)

    let hostStatus = %* {
      "type": "HostNetworkStatus",
      "networkType": "cellular",
      "transport": "cellular",
      "localIpv4": "10.67.35.137",
      "preferredIpv4": "10.67.35.137",
      "localIpv6": "240e:43d:682c:222:a0b5:23ff:fe8e:a9d3",
      "preferredIpv6": "240e:43d:682c:222:a0b5:23ff:fe8e:a9d3",
      "publicIpv4": "106.34.150.71",
      "publicIpv6": "240e:43d:682c:222:a0b5:23ff:fe8e:a9d3",
      "listenPort": 4001,
      "isConnected": true,
      "isMetered": true,
      "timestampMs": 1_775_547_616_637'i64,
      "reason": "manual",
    }
    check libp2p_update_host_network_status(handle, ($hostStatus).cstring)

    let peerId = takeCStringAndFree(libp2p_get_local_peer_id(handle))
    check peerId.len > 0

    let expectedProviderSeed = "/ip6/240e:43d:682c:222:a0b5:23ff:fe8e:a9d3/udp/4001/quic-v1/p2p/" & peerId
    let expectedDiscoverySeed = "/ip6/240e:43d:682c:222:a0b5:23ff:fe8e:a9d3/udp/4001/quic-v1"
    let bootstrapStatus = safeJsonExpr(libp2p_get_bootstrap_status(handle))
    let providerDirectSeeds = bootstrapStatus["providerDirectSeedAddrs"].getElems().mapIt(it.getStr())
    check bootstrapStatus["localHasGlobalIpv6"].getBool()
    check bootstrapStatus["localHasQuic"].getBool()
    check bootstrapStatus["providerReason"].getStr() != "no_seed_addrs"
    check bootstrapStatus["providerDirectSeedAddrs"].kind == JArray
    check expectedProviderSeed in providerDirectSeeds
    check providerDirectSeeds.allIt("/ip4/0.0.0.0/" notin it.toLowerAscii())
    check providerDirectSeeds.allIt("/tsnet" notin it.toLowerAscii())

    let discovery = safeJsonExpr(libp2p_network_discovery_snapshot(handle, "".cstring, 8, 4))
    check discovery["seedMultiaddrs"].kind == JArray
    check expectedDiscoverySeed in discovery["seedMultiaddrs"].getElems().mapIt(it.getStr())

  test "bootstrap status synthesizes direct ipv6 provider seeds from global interface ipv6 without reflection":
    let handle = newTestNode("wan-provider-ipv6-no-reflection")
    defer:
      stopAndFreeNode(handle)

    startNode(handle)

    let hostStatus = %* {
      "type": "HostNetworkStatus",
      "networkType": "cellular",
      "transport": "cellular",
      "localIpv4": "10.67.35.137",
      "preferredIpv4": "10.67.35.137",
      "localIpv6": "240e:43d:682c:222:a0b5:23ff:fe8e:a9d3",
      "preferredIpv6": "240e:43d:682c:222:a0b5:23ff:fe8e:a9d3",
      "publicIpv4": "",
      "publicIpv6": "",
      "listenPort": 4001,
      "isConnected": true,
      "isMetered": true,
      "timestampMs": 1_775_547_616_638'i64,
      "reason": "manual",
    }
    check libp2p_update_host_network_status(handle, ($hostStatus).cstring)

    let peerId = takeCStringAndFree(libp2p_get_local_peer_id(handle))
    check peerId.len > 0

    let expectedProviderSeed = "/ip6/240e:43d:682c:222:a0b5:23ff:fe8e:a9d3/udp/4001/quic-v1/p2p/" & peerId
    let bootstrapStatus = safeJsonExpr(libp2p_get_bootstrap_status(handle))
    let providerDirectSeeds = bootstrapStatus["providerDirectSeedAddrs"].getElems().mapIt(it.getStr())
    check bootstrapStatus["providerDirectSeedAddrs"].kind == JArray
    check expectedProviderSeed in providerDirectSeeds
    check providerDirectSeeds.allIt("/ip4/0.0.0.0/" notin it.toLowerAscii())
    check providerDirectSeeds.allIt("/tsnet" notin it.toLowerAscii())

  test "tsnet underlay still advertises direct ipv6 provider seeds and dialable addrs":
    let handle = newTsnetTestNodeWithConfig(
      "wan-tsnet-direct-ipv6",
      %* {},
    )
    defer:
      stopAndFreeNode(handle)

    startNode(handle)

    let hostStatus = %* {
      "type": "HostNetworkStatus",
      "networkType": "cellular",
      "transport": "cellular",
      "localIpv4": "10.67.35.137",
      "preferredIpv4": "10.67.35.137",
      "localIpv6": "240e:43d:682c:222:a0b5:23ff:fe8e:a9d3",
      "preferredIpv6": "240e:43d:682c:222:a0b5:23ff:fe8e:a9d3",
      "publicIpv4": "",
      "publicIpv6": "",
      "listenPort": 4001,
      "isConnected": true,
      "isMetered": true,
      "timestampMs": 1_775_547_616_639'i64,
      "reason": "manual",
    }
    check libp2p_update_host_network_status(handle, ($hostStatus).cstring)

    let peerId = takeCStringAndFree(libp2p_get_local_peer_id(handle))
    check peerId.len > 0

    let expectedProviderSeed = "/ip6/240e:43d:682c:222:a0b5:23ff:fe8e:a9d3/udp/4001/quic-v1/p2p/" & peerId
    let expectedDialableAddr = "/ip6/240e:43d:682c:222:a0b5:23ff:fe8e:a9d3/udp/4001/quic-v1"

    waitUntilSatisfied(12_000, 200):
      let bootstrapStatus = safeJsonExpr(libp2p_get_bootstrap_status(handle))
      let providerStatus =
        if bootstrapStatus.hasKey("provider") and bootstrapStatus["provider"].kind == JObject:
          bootstrapStatus["provider"]
        else:
          bootstrapStatus
      let providerSeeds =
        if providerStatus.hasKey("providerDirectSeedAddrs") and
            providerStatus["providerDirectSeedAddrs"].kind == JArray:
          providerStatus["providerDirectSeedAddrs"].getElems().mapIt(it.getStr())
        else:
          @[]
      let discovery = safeJsonExpr(libp2p_network_discovery_snapshot(handle, "".cstring, 8, 4))
      let dialable =
        if discovery.hasKey("dialableAddresses") and discovery["dialableAddresses"].kind == JArray:
          discovery["dialableAddresses"].getElems().mapIt(it.getStr())
        else:
          @[]
      expectedProviderSeed in providerSeeds and
        expectedDialableAddr in dialable and
        providerSeeds.allIt("/ip4/0.0.0.0/" notin it.toLowerAscii()) and
        providerSeeds.allIt("/tsnet" notin it.toLowerAscii())

  test "relay bootstrap multiaddrs enter authoritative bootstrap without reusing relay reservation field":
    let relayPeerId = testIdentity()["peerId"].getStr()
    let bootstrapPeerId = testIdentity()["peerId"].getStr()
    let relayedBootstrapAddr =
      "/ip6/2001:db8::44/udp/4001/quic-v1/p2p/" & relayPeerId &
      "/p2p-circuit/p2p/" & bootstrapPeerId
    let handle = newTestNodeWithConfig(
      "wan-relay-bootstrap-seeds",
      %* {
        "bootstrap_authoritative": true,
        "disable_public_bootstrap": true,
        "enable_public_bootstrap": false,
        "relayBootstrapMultiaddrs": [relayedBootstrapAddr],
      },
    )
    defer:
      stopAndFreeNode(handle)

    startNode(handle)

    var lastBootstrapStatus = newJObject()
    let deadline = nowMillis() + 8_000
    var satisfied = false
    while nowMillis() <= deadline:
      lastBootstrapStatus = safeJsonExpr(libp2p_get_bootstrap_status(handle))
      let providerStatus =
        if lastBootstrapStatus.hasKey("provider") and lastBootstrapStatus["provider"].kind == JObject:
          lastBootstrapStatus["provider"]
        else:
          newJObject()
      let authoritative =
        if lastBootstrapStatus.hasKey("authoritativeBootstrapPeerIds") and
            lastBootstrapStatus["authoritativeBootstrapPeerIds"].kind == JArray:
          lastBootstrapStatus["authoritativeBootstrapPeerIds"].getElems().mapIt(it.getStr())
        elif providerStatus.hasKey("authoritativeBootstrapPeerIds") and
            providerStatus["authoritativeBootstrapPeerIds"].kind == JArray:
          providerStatus["authoritativeBootstrapPeerIds"].getElems().mapIt(it.getStr())
        else:
          @[]
      let anchorSeeds =
        if lastBootstrapStatus.hasKey("anchorSeedAddrs") and
            lastBootstrapStatus["anchorSeedAddrs"].kind == JArray:
          lastBootstrapStatus["anchorSeedAddrs"].getElems().mapIt(it.getStr())
        else:
          @[]
      let selectedCandidates =
        if lastBootstrapStatus.hasKey("selectedCandidates") and
            lastBootstrapStatus["selectedCandidates"].kind == JArray:
          lastBootstrapStatus["selectedCandidates"].getElems()
        else:
          @[]
      satisfied =
        authoritative == @[bootstrapPeerId] and
          relayedBootstrapAddr in anchorSeeds and
          selectedCandidates.anyIt(
            it.kind == JObject and
              it{"peerId"}.kind == JString and
              it{"peerId"}.getStr() == bootstrapPeerId and
              it.hasKey("addrs") and
              it["addrs"].kind == JArray and
              relayedBootstrapAddr in it["addrs"].getElems().mapIt(it.getStr())
          )
      if satisfied:
        break
      sleep(150)

    if not satisfied:
      echo "relay bootstrap status=", $lastBootstrapStatus
    check satisfied

  test "tsnet anchor publication does not regress to fake tsnet pending":
    let relayPeerId = testIdentity()["peerId"].getStr()
    let bootstrapPeerId = testIdentity()["peerId"].getStr()
    let relayedBootstrapAddr =
      "/ip6/2001:db8::44/udp/4001/quic-v1/p2p/" & relayPeerId &
      "/p2p-circuit/p2p/" & bootstrapPeerId
    let handle = newTsnetTestNodeWithConfig(
      "wan-tsnet-anchor-status",
      %* {
        "bootstrap_authoritative": true,
        "disable_public_bootstrap": true,
        "enable_public_bootstrap": false,
        "relayBootstrapMultiaddrs": [relayedBootstrapAddr],
      },
    )
    defer:
      stopAndFreeNode(handle)

    startNode(handle)

    let bootstrapStatus = safeJsonExpr(libp2p_get_bootstrap_status(handle))
    let providerStatus =
      if bootstrapStatus.hasKey("provider") and bootstrapStatus["provider"].kind == JObject:
        bootstrapStatus["provider"]
      else:
        bootstrapStatus
    check providerStatus["providerPublicationMode"].getStr() != "tsnet_published"
    check providerStatus["providerReason"].getStr() != "tsnet_publication_not_ready"
