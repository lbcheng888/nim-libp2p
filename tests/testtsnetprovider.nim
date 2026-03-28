{.used.}

import std/[json, os, strutils, times]
import unittest2

import
  ../libp2p/[
    transports/tsnetprovider,
    utility,
  ]

proc fixturePath(name: string): string =
  currentSourcePath.parentDir() / "fixtures" / "tsnet" / name

proc tempStateDir(tag: string): string =
  let path = getTempDir() / ("nim-libp2p-tsnet-provider-" & tag & "-" & $epochTime())
  createDir(path)
  path

suite "Tsnet provider":
  test "default provider starts in builtin synthetic mode":
    let provider = TsnetProvider.new(TsnetProviderConfig.init())
    check not provider.runtimeRequested()

    let startRes = provider.start()
    check startRes.isOk()
    check startRes.get() == TsnetProviderKind.BuiltinSynthetic
    check provider.kind == TsnetProviderKind.BuiltinSynthetic
    check provider.ready()
    check provider.requestedBackendLabel() == "builtin-synthetic"
    check provider.failure() == ""
    let caps = provider.capabilities()
    check caps.inApp
    check not caps.realTailnet
    check not caps.proxyBacked
    check caps.derpMapApi
    check not caps.statusApi
    check not caps.pingApi

    let derpRes = provider.derpMapPayload()
    check derpRes.isOk()
    let derpPayload = derpRes.get()
    check derpPayload["providerKind"].getStr() == "builtin-synthetic"
    check derpPayload["providerRequestedBackend"].getStr() == "builtin-synthetic"
    check derpPayload["providerReady"].getBool()
    check derpPayload["providerInApp"].getBool()
    check derpPayload["providerCapabilities"]["inApp"].getBool()
    check not derpPayload["providerCapabilities"]["realTailnet"].getBool()

    provider.stop()

  test "explicit real tailnet request fails fast without a provider":
    var cfg = TsnetProviderConfig.init()
    cfg.controlUrl = "https://headscale.invalid"

    let provider = TsnetProvider.new(cfg)
    let startRes = provider.start()
    check startRes.isErr()
    check startRes.error == "nim_quic: nim_quic control transport requires -d:libp2p_msquic_experimental"
    check provider.kind == TsnetProviderKind.InAppUnavailable
    check provider.requestedBackendLabel() == "nim-inapp"
    check not provider.ready()
    check provider.failure() == "nim_quic: nim_quic control transport requires -d:libp2p_msquic_experimental"

  test "explicit legacy bridge path keeps deprecated compatibility path opt-in":
    var cfg = TsnetProviderConfig.init()
    cfg.controlUrl = "https://headscale.invalid"
    cfg.bridgeLibraryPath = "/definitely/missing/libtsnetbridge.dylib"

    let provider = TsnetProvider.new(cfg)
    check provider.runtimeRequested()
    check provider.legacyBridgeRequested()

    let startRes = provider.start()
    check startRes.isErr()
    check startRes.error.contains("legacy bridge load failed")
    check provider.kind == TsnetProviderKind.LegacyBridge
    check provider.requestedBackendLabel() == "legacy-go-bridge"
    check not provider.ready()
    check provider.failure().contains("legacy bridge load failed")

  test "oracle fixture enables nim in-app status surfaces":
    let dir = tempStateDir("oracle")
    copyFile(fixturePath("self_hosted_901_sin.json"), dir / "nim-tsnet-oracle.json")

    var cfg = TsnetProviderConfig.init()
    cfg.controlUrl = "https://64-176-84-12.sslip.io"
    cfg.stateDir = dir
    cfg.hostname = "nim-provider-test"
    cfg.enableDebug = true

    let provider = TsnetProvider.new(cfg)
    let startRes = provider.start()
    check startRes.isOk()
    check startRes.get() == TsnetProviderKind.InAppReal
    check provider.kind == TsnetProviderKind.InAppReal
    check provider.ready()
    check provider.requestedBackendLabel() == "nim-inapp"
    check provider.failure() == ""

    let caps = provider.capabilities()
    check caps.inApp
    check caps.realTailnet
    check caps.proxyBacked
    check caps.statusApi
    check caps.pingApi
    check caps.derpMapApi

    let status = provider.statusPayload()
    check status.isOk()
    check status.get()["providerKind"].getStr() == "nim-inapp"
    check status.get()["tailnetPath"].getStr() == "relay"
    check status.get()["tailnetDerpMapSummary"].getStr() == "901/sin"

    let ping = provider.pingPayload(%* {})
    check ping.isOk()
    check ping.get()["pingType"].getStr() == "TSMP"

    let derpMap = provider.derpMapPayload()
    check derpMap.isOk()
    check derpMap.get()["regions"][0]["regionCode"].getStr() == "sin"

    provider.stop()
