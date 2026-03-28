{.used.}

import std/[json, os, strutils, times]
import unittest2

import ../libp2p/transports/[tsnet/runtime, tsnet/state, tsnet/control, tsnetprovidertypes]
import ../libp2p/utility

proc fixturePath(name: string): string =
  currentSourcePath.parentDir() / "fixtures" / "tsnet" / name

proc tempRuntimeDir(tag: string): string =
  let path = getTempDir() / ("nim-libp2p-tsnet-runtime-" & tag & "-" & $epochTime())
  createDir(path)
  path

suite "Tsnet in-app runtime":
  test "persisted state is reused and start fails fast honestly":
    let dir = tempRuntimeDir("bootstrap")
    let cfg = TsnetProviderConfig(
      controlUrl: "https://headscale.invalid",
      authKey: "tskey-test",
      hostname: "nim-runtime-test",
      stateDir: dir,
      wireguardPort: 41641,
      bridgeLibraryPath: "",
      logLevel: "debug",
      enableDebug: true,
      bridgeExtraJson: ""
    )

    let runtime = TsnetInAppRuntime.new(cfg)

    let persisted = runtime.persistState()
    check persisted.isOk()

    let loaded = runtime.loadPersistedState()
    check loaded.isOk()
    check loaded.get().hostname == "nim-runtime-test"
    check loaded.get().controlUrl == "https://headscale.invalid"
    check loaded.get().machineKey.len == 0

    let materialized = runtime.ensureStateMaterial()
    check materialized.isOk()
    check materialized.get()
    check runtime.state.machineKey.startsWith(TsnetMachineKeyPrefix)
    check runtime.state.nodeKey.startsWith(TsnetNodeKeyPrefix)
    check runtime.state.wgKey.startsWith(TsnetWireGuardKeyPrefix)
    check runtime.state.machinePublicKey.startsWith(TsnetMachinePublicKeyPrefix)
    check runtime.state.nodePublicKey.startsWith(TsnetNodePublicKeyPrefix)
    check runtime.state.discoPublicKey.startsWith(TsnetDiscoPublicKeyPrefix)

    let machineKey = runtime.state.machineKey
    let nodeKey = runtime.state.nodeKey
    let wgKey = runtime.state.wgKey
    let machinePublicKey = runtime.state.machinePublicKey
    let nodePublicKey = runtime.state.nodePublicKey
    let discoPublicKey = runtime.state.discoPublicKey

    let started = runtime.start()
    check started.isErr()
    when defined(libp2p_msquic_experimental):
      check started.error.startsWith("nim_quic: ")
      check started.error.contains("headscale.invalid")
    else:
      check started.error == "nim_quic: nim_quic control transport requires -d:libp2p_msquic_experimental"
    check runtime.status == TsnetInAppRuntimeStatus.Failed
    when defined(libp2p_msquic_experimental):
      check runtime.lastError.startsWith("nim_quic: ")
      check runtime.lastError.contains("headscale.invalid")
    else:
      check runtime.lastError == "nim_quic: nim_quic control transport requires -d:libp2p_msquic_experimental"

    let persistedAfterStart = loadStoredState(dir)
    check persistedAfterStart.isOk()
    check persistedAfterStart.get().machineKey == machineKey
    check persistedAfterStart.get().nodeKey == nodeKey
    check persistedAfterStart.get().wgKey == wgKey
    check persistedAfterStart.get().machinePublicKey == machinePublicKey
    check persistedAfterStart.get().nodePublicKey == nodePublicKey
    check persistedAfterStart.get().discoPublicKey == discoPublicKey

    let reset = runtime.reset()
    check reset.isOk()
    check not fileExists(dir / "nim-tsnet-state.json")

  test "oracle fixture enables runtime status derp and ping surfaces":
    let dir = tempRuntimeDir("oracle")
    copyFile(fixturePath("self_hosted_901_sin.json"), dir / "nim-tsnet-oracle.json")
    let cfg = TsnetProviderConfig(
      controlUrl: "https://64-176-84-12.sslip.io",
      authKey: "tskey-test",
      hostname: "nim-runtime-test",
      stateDir: dir,
      wireguardPort: 41641,
      bridgeLibraryPath: "",
      logLevel: "debug",
      enableDebug: true,
      bridgeExtraJson: ""
    )

    let runtime = TsnetInAppRuntime.new(cfg)
    let started = runtime.start()
    check started.isOk()
    check runtime.ready()
    check runtime.status == TsnetInAppRuntimeStatus.Running
    check runtime.capabilities().proxyBacked

    let status = runtime.statusPayload()
    check status.isOk()
    check status.get()["tailnetPath"].getStr() == "relay"
    check status.get()["tailnetDerpMapSummary"].getStr() == "901/sin"
    check status.get()["tailnetIPs"][0].getStr() == "100.64.0.25"

    let derpMap = runtime.derpMapPayload()
    check derpMap.isOk()
    check derpMap.get()["regions"][0]["regionCode"].getStr() == "sin"

    let ping = runtime.pingPayload(newJObject())
    check ping.isOk()
    check ping.get()["ok"].getBool()
    check ping.get()["pingType"].getStr() == "TSMP"

    let unsupported = runtime.pingPayload(%* {"peerIP": "100.64.0.25", "pingType": "ICMP"})
    check unsupported.isOk()
    check not unsupported.get()["ok"].getBool()
    check unsupported.get()["error"].getStr() == "unsupported_ping_type"

    let persisted = loadStoredState(dir)
    check persisted.isOk()
    check persisted.get().homeDerp == "901/sin"
    check persisted.get().peerCache.len == 1

    runtime.promoteDirectPath(punched = true)
    let punchedStatus = runtime.statusPayload()
    check punchedStatus.isOk()
    check punchedStatus.get()["tailnetPath"].getStr() == "punched_direct"
    check punchedStatus.get()["tailnetRelay"].getStr() == ""

    runtime.promoteDirectPath()
    let directStatus = runtime.statusPayload()
    check directStatus.isOk()
    check directStatus.get()["tailnetPath"].getStr() == "direct"
    check directStatus.get()["tailnetRelay"].getStr() == ""

    runtime.demoteRelayPath()
    let relayStatus = runtime.statusPayload()
    check relayStatus.isOk()
    check relayStatus.get()["tailnetPath"].getStr() == "relay"
    check relayStatus.get()["tailnetRelay"].getStr() == "sin"

  test "oracle fixture is ignored when debug mode is disabled":
    let dir = tempRuntimeDir("oracle-prod")
    copyFile(fixturePath("self_hosted_901_sin.json"), dir / "nim-tsnet-oracle.json")
    let cfg = TsnetProviderConfig(
      controlUrl: "",
      authKey: "",
      hostname: "nim-runtime-prod",
      stateDir: dir,
      wireguardPort: 41641,
      bridgeLibraryPath: "",
      logLevel: "",
      enableDebug: false,
      bridgeExtraJson: ""
    )

    let runtime = TsnetInAppRuntime.new(cfg)
    let started = runtime.start()
    check started.isErr()
    check started.error == TsnetMissingInAppProviderError
    check runtime.status == TsnetInAppRuntimeStatus.Failed

  test "control probe without auth key no longer pre-fails bootstrap state":
    let dir = tempRuntimeDir("auth-required")
    let cfg = TsnetProviderConfig(
      controlUrl: "https://64-176-84-12.sslip.io",
      authKey: "",
      hostname: "nim-runtime-auth",
      stateDir: dir,
      wireguardPort: 41641,
      bridgeLibraryPath: "",
      logLevel: "",
      enableDebug: false,
      bridgeExtraJson: ""
    )

    let runtime = TsnetInAppRuntime.new(cfg)
    runtime.state.controlPublicKey = "mkey:control"
    runtime.state.lastControlSuccessUnixMilli = 1774333653153
    runtime.control = TsnetControlSnapshot.init(
      source = "test",
      capturedAtUnixMilli = 1774333653153,
      probePayload = %*{
        "stage": "control_key_probe",
        "noiseUrl": controlNoiseUrl(cfg.controlUrl)
      },
      registerPayload = runtime.buildRegisterBootstrapPayload(
        TsnetControlServerKey.init(
          capabilityVersion = 130,
          publicKey = "mkey:control",
          legacyPublicKey = "mkey:legacy",
          fetchedAtUnixMilli = 1774333653153
        )
      ),
      mapPollPayload = runtime.buildMapBootstrapPayload(
        TsnetControlServerKey.init(
          capabilityVersion = 130,
          publicKey = "mkey:control",
          legacyPublicKey = "mkey:legacy",
          fetchedAtUnixMilli = 1774333653153
        )
      )
    )
    runtime.session = runtime.buildUnavailableSession("ControlReachable", "register_or_map_incomplete")
    runtime.status = TsnetInAppRuntimeStatus.Failed
    runtime.lastError = runtime.runtimeUnavailableError()

    check not runtime.lastError.startsWith("auth_required:")
    check runtime.lastError.contains("has not completed register/map")
    check runtime.control.available()
    check runtime.control.toJson()["probe"]["stage"].getStr() == "control_key_probe"
    check runtime.control.toJson()["probe"]["noiseUrl"].getStr().endsWith("/ts2021")
    check runtime.control.toJson()["register"]["registerUrl"].getStr().endsWith("/machine/register")
    check runtime.control.toJson()["register"]["request"]["NodeKey"].getStr().startsWith("nodekey:")
    check runtime.control.toJson()["mapPoll"]["mapUrl"].getStr().endsWith("/machine/map")
    check runtime.control.toJson()["mapPoll"]["request"]["DiscoKey"].getStr().startsWith("discokey:")
    check runtime.session.status.error == "register_or_map_incomplete"
    check runtime.session.status.backendState == "ControlReachable"
    check runtime.session.status.authUrl == ""
    let statusPayload = runtime.statusPayload()
    check statusPayload.isOk()
    check statusPayload.get()["backendState"].getStr() == "ControlReachable"
    let pingPayload = runtime.pingPayload(%* {})
    check pingPayload.isOk()
    check pingPayload.get()["error"].getStr() == "register_or_map_incomplete"
    let derpMapPayload = runtime.derpMapPayload()
    check derpMapPayload.isOk()
    check derpMapPayload.get()["error"].getStr() == "register_or_map_incomplete"

  test "control probe with auth key surfaces control reachable bootstrap state":
    let dir = tempRuntimeDir("control-reachable")
    let cfg = TsnetProviderConfig(
      controlUrl: "https://64-176-84-12.sslip.io",
      authKey: "tskey-test",
      hostname: "nim-runtime-live",
      stateDir: dir,
      wireguardPort: 41641,
      bridgeLibraryPath: "",
      logLevel: "",
      enableDebug: false,
      bridgeExtraJson: ""
    )

    let runtime = TsnetInAppRuntime.new(cfg)
    runtime.state.controlPublicKey = "mkey:control"
    runtime.state.lastControlSuccessUnixMilli = 1774333653153
    runtime.control = TsnetControlSnapshot.init(
      source = "test",
      capturedAtUnixMilli = 1774333653153,
      probePayload = %*{
        "stage": "control_key_probe",
        "authKeyProvided": true,
        "noiseUrl": controlNoiseUrl(cfg.controlUrl)
      },
      registerPayload = runtime.buildRegisterBootstrapPayload(
        TsnetControlServerKey.init(
          capabilityVersion = 130,
          publicKey = "mkey:control",
          legacyPublicKey = "mkey:legacy",
          fetchedAtUnixMilli = 1774333653153
        )
      ),
      mapPollPayload = runtime.buildMapBootstrapPayload(
        TsnetControlServerKey.init(
          capabilityVersion = 130,
          publicKey = "mkey:control",
          legacyPublicKey = "mkey:legacy",
          fetchedAtUnixMilli = 1774333653153
        )
      )
    )
    runtime.session = runtime.buildUnavailableSession(
      "ControlReachable",
      "control_reachable_but_register_map_unimplemented"
    )
    runtime.status = TsnetInAppRuntimeStatus.Failed
    runtime.lastError = runtime.runtimeUnavailableError()

    check runtime.lastError.contains("selected Nim in-app control client has not completed register/map")
    check runtime.control.available()
    check runtime.control.toJson()["probe"]["authKeyProvided"].getBool()
    check runtime.control.toJson()["register"]["protocol"].getStr() == "ts2021/http2"
    check runtime.control.toJson()["register"]["request"]["Auth"]["AuthKey"].getStr() == "tskey-test"
    check runtime.control.toJson()["mapPoll"]["compress"].getStr() == "zstd"
    check runtime.session.status.error == "control_reachable_but_register_map_unimplemented"
    check runtime.session.status.backendState == "ControlReachable"
    check runtime.session.status.authUrl.len == 0
    let statusPayload = runtime.statusPayload()
    check statusPayload.isOk()
    check statusPayload.get()["backendState"].getStr() == "ControlReachable"
