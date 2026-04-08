{.used.}

import std/[json, os, sequtils, strutils]
import unittest2

import
  ../libp2p/[
    builders,
    crypto/crypto,
    multiaddress,
    transports/transport,
    transports/tsnetprovider,
    transports/tsnettransport,
    upgrademngrs/upgrade,
  ]

import ./helpers, ./commontransport

proc fixturePath(name: string): string =
  currentSourcePath.parentDir() / "fixtures" / "tsnet" / name

var tempDirCounter = 0

proc tempStateDir(tag: string): string =
  inc tempDirCounter
  let path = getTempDir() / ("nim-libp2p-tsnet-transport-" & tag & "-" & $tempDirCounter)
  createDir(path)
  path

suite "Tsnet transport":
  teardown:
    checkTrackers()

  proc makeTransport(): Transport =
    let privKey = PrivateKey.random(rng[]).expect("private key")
    TsnetTransport.new(Upgrade(), privKey)

  test "withTsnetTransport accepts custom config":
    var builder = SwitchBuilder.new()
    var cfg = TsnetTransportBuilderConfig.init()
    cfg.hostname = "tsnet-test"
    cfg.controlUrl = "https://headscale.example"
    builder = builder.withTsnetTransport(cfg)
    check builder != nil

  test "tsnet quic transport defaults keep resident session alive":
    var cfg = TsnetTransportBuilderConfig.init()
    when defined(libp2p_msquic_experimental):
      check cfg.quicConfig.handshakeIdleTimeoutMs == 30_000'u64
      check cfg.quicConfig.idleTimeoutMs == 120_000'u64
      check cfg.quicConfig.keepAliveIntervalMs == 5_000'u32

  test "publishLocalPeerInfo keeps static tsnet config and merges bridge direct candidates":
    let privKey = PrivateKey.random(rng[]).expect("private key")
    var cfg = TsnetTransportBuilderConfig.init()
    cfg.bridgeExtraJson = $(%*{
      "hostname": "android-node",
      "androidInterfaces": [
        {"name": "wlan0"}
      ]
    })
    let transport = TsnetTransport.new(Upgrade(), privKey, cfg)
    transport.publishLocalPeerInfo(
      "12D3KooWTestBridgeMeta",
      ["/ip4/100.64.0.10/udp/52488/quic-v1/tsnet/p2p/12D3KooWTestBridgeMeta"],
      TsnetBridgeExtraPayload(
        updatedAtMs: 123456789,
        publicIpv6: "2001:db8::52",
        directCandidates: @[
          "/ip6/2001:db8::52/udp/52488/quic-v1/p2p/12D3KooWTestBridgeMeta"
        ]
      )
    )
    let payload = parseJson(transport.cfg.bridgeExtraJson)
    check payload["hostname"].getStr() == "android-node"
    check payload["androidInterfaces"][0]["name"].getStr() == "wlan0"
    check payload["libp2pPeerId"].getStr() == "12D3KooWTestBridgeMeta"
    check not payload.hasKey("publicIpv4")
    check payload["publicIpv6"].getStr() == "2001:db8::52"
    check payload["directCandidates"][0].getStr() ==
      "/ip6/2001:db8::52/udp/52488/quic-v1/p2p/12D3KooWTestBridgeMeta"

  asyncTest "listener advertises /tsnet addresses":
    let transport = TsnetTransport(makeTransport())
    await transport.start(@[MultiAddress.init("/ip4/0.0.0.0/tcp/0/tsnet").tryGet()])
    check transport.addrs.len == 1
    check $transport.addrs[0] != "/ip4/0.0.0.0/tcp/0/tsnet"
    check $transport.addrs[0] == ($transport.addrs[0]).toLowerAscii()
    check ($transport.addrs[0]).endsWith("/tsnet")
    check transport.handles(transport.addrs[0])
    let status = transport.tailnetStatus()
    check status.tailnetIPs.len == 2
    check status.tailnetIPs.anyIt(it.startsWith("100.64."))
    let statusPayload = transport.tailnetStatusPayload().tryGet()
    check statusPayload["providerKind"].getStr() == "builtin-synthetic"
    check statusPayload["providerRequestedBackend"].getStr() == "builtin-synthetic"
    check not statusPayload["providerReady"].getBool()
    check statusPayload["providerInApp"].getBool()
    check statusPayload["providerCapabilities"]["inApp"].getBool()
    check not statusPayload["providerCapabilities"]["proxyBacked"].getBool()
    await transport.stop()

  asyncTest "self-hosted config does not block cold-start before first tailnet call":
    let privKey = PrivateKey.random(rng[]).expect("private key")
    var cfg = TsnetTransportBuilderConfig.init()
    cfg.controlUrl = "https://headscale.invalid"
    cfg.authKey = "tskey-auth-placeholder"
    let transport = TsnetTransport.new(Upgrade(), privKey, cfg)
    let listenAddr = MultiAddress.init("/ip4/0.0.0.0/tcp/0/tsnet").tryGet()
    await transport.start(@[listenAddr])
    check transport.addrs.len == 0
    let statusPayload = transport.tailnetStatusPayload().tryGet()
    check statusPayload["providerRequestedBackend"].getStr() == "nim-inapp"
    check not statusPayload["providerReady"].getBool()
    check statusPayload["deferred"].getBool()
    check statusPayload["reason"].getStr() == "provider_start_deferred"
    check not statusPayload["ok"].getBool()
    await transport.stop()

  asyncTest "self-hosted config publishes /tsnet addresses only after warm":
    let dir = tempStateDir("oracle-warm")
    copyFile(fixturePath("self_hosted_901_sin.json"), dir / "nim-tsnet-oracle.json")

    let privKey = PrivateKey.random(rng[]).expect("private key")
    var cfg = TsnetTransportBuilderConfig.init()
    cfg.controlUrl = "https://64-176-84-12.sslip.io"
    cfg.stateDir = dir
    cfg.hostname = "nim-transport-deferred-warm"
    cfg.enableDebug = true
    let transport = TsnetTransport.new(Upgrade(), privKey, cfg)

    await transport.start(@[MultiAddress.init("/ip4/0.0.0.0/tcp/0/tsnet").tryGet()])
    check transport.addrs.len == 0
    let deferredPayload = transport.tailnetStatusPayload().tryGet()
    check deferredPayload["deferred"].getBool()
    check transport.warmProvider().isOk()
    check transport.addrs.len == 1
    check ($transport.addrs[0]).endsWith("/tsnet")
    let readyPayload = transport.tailnetStatusPayload().tryGet()
    check readyPayload["providerReady"].getBool()
    await transport.stop()

  asyncTest "self-hosted oracle fixture starts nim in-app transport status path":
    let dir = tempStateDir("oracle")
    copyFile(fixturePath("self_hosted_901_sin.json"), dir / "nim-tsnet-oracle.json")

    let privKey = PrivateKey.random(rng[]).expect("private key")
    var cfg = TsnetTransportBuilderConfig.init()
    cfg.controlUrl = "https://64-176-84-12.sslip.io"
    cfg.relayEndpoint = "quic://64.176.84.12:9446/nim-tsnet-relay-quic/v1"
    cfg.stateDir = dir
    cfg.hostname = "nim-transport-test"
    cfg.enableDebug = true
    let transport = TsnetTransport.new(Upgrade(), privKey, cfg)

    await transport.start(@[MultiAddress.init("/ip4/0.0.0.0/tcp/0/tsnet").tryGet()])
    check transport.warmProvider().isOk()
    let payload = transport.tailnetStatusPayload().tryGet()
    let payloadAgain = transport.tailnetStatusPayload().tryGet()
    check payload["providerKind"].getStr() == "nim-inapp"
    check payload["tailnetDerpMapSummary"].getStr() == "901/sin"
    check payload["providerCapabilities"]["proxyBacked"].getBool()
    check payloadAgain["providerCapabilities"]["proxyBacked"].getBool()
    check transport.addrs.len == 1
    check ($transport.addrs[0]).endsWith("/tsnet")
    await transport.stop()

  test "handles rejects malformed /tsnet shapes":
    let transport = makeTransport()
    check not transport.handles(MultiAddress.init("/ip4/127.0.0.1/tsnet").tryGet())
    check not transport.handles(MultiAddress.init("/tsnet").tryGet())

  commonTransportTest(makeTransport, "/ip4/0.0.0.0/tcp/0/tsnet")
