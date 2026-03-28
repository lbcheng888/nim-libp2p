{.used.}

when defined(ssl):
  import std/[json, os, strutils, times]
  import unittest2

  import ../libp2p/transports/[tsnet/runtime, tsnet/state, tsnet/control, tsnetprovidertypes]
  import ../libp2p/utility

  proc defaultHeadscaleJsonPath(): string =
    currentSourcePath.parentDir().parentDir() / "build" / "headscale-setup.json"

  proc headscaleJsonPath(): string =
    let envPath = getEnv("NIM_TSNET_LIVE_HEADSCALE_JSON").strip()
    if envPath.len > 0:
      return envPath
    defaultHeadscaleJsonPath()

  proc loadHeadscaleConfig(path: string): Result[JsonNode, string] =
    try:
      ok(parseFile(path))
    except CatchableError as exc:
      err("failed to parse live headscale config " & path & ": " & exc.msg)

  proc jsonString(node: JsonNode, keys: openArray[string]): string =
    if node.isNil or node.kind != JObject:
      return ""
    for key in keys:
      if node.hasKey(key):
        let value = node.getOrDefault(key)
        if value.kind == JString:
          return value.getStr().strip()
    ""

  proc tempRuntimeDir(tag: string): string =
    let path = getTempDir() / ("nim-libp2p-tsnet-live-" & tag & "-" & $epochTime())
    createDir(path)
    path

  proc makeLiveCfg(
      controlUrl, authKey, hostname, dir, controlProtocol: string
  ): TsnetProviderConfig =
    TsnetProviderConfig(
      controlUrl: controlUrl,
      authKey: authKey,
      hostname: hostname,
      stateDir: dir,
      wireguardPort: 41641,
      controlProtocol: controlProtocol,
      bridgeLibraryPath: "",
      logLevel: "",
      enableDebug: false,
      bridgeExtraJson: ""
    )

  proc checkLiveRuntime(
      runtime: TsnetInAppRuntime,
      controlUrl, expectedSelectedProtocol: string
  ) =
    check runtime.status == TsnetInAppRuntimeStatus.Running
    check runtime.control.available()
    check runtime.state.controlPublicKey.startsWith("mkey:")
    check runtime.state.machinePublicKey.startsWith(TsnetMachinePublicKeyPrefix)
    check runtime.state.nodePublicKey.startsWith(TsnetNodePublicKeyPrefix)
    check runtime.state.discoPublicKey.startsWith(TsnetDiscoPublicKeyPrefix)
    check runtime.state.lastControlSuccessUnixMilli > 0
    check runtime.state.lastRegisterAttemptUnixMilli > 0
    check runtime.state.lastMapPollAttemptUnixMilli > 0
    check runtime.state.lastControlBootstrapError.len == 0
    check runtime.state.homeDerp == "901/sin"
    check runtime.state.tailnetIPs.len > 0
    check runtime.ready()

    let controlPayload = runtime.control.toJson()
    check controlPayload["register"]["stage"].getStr() == "registered"
    check controlPayload["mapPoll"]["stage"].getStr() == "mapped"

    let statusPayload = runtime.statusPayload()
    check statusPayload.isOk()
    check statusPayload.get()["backendState"].getStr() == "Mapped"
    check statusPayload.get()["controlUrl"].getStr() == controlUrl
    check statusPayload.get()["tailnetPath"].getStr() == "relay"
    check statusPayload.get()["tailnetRelay"].getStr() == "sin"
    check statusPayload.get()["controlProtocolSelected"].getStr() == expectedSelectedProtocol
    check statusPayload.get()["controlEndpoint"].getStr().len > 0

    let pingPayload = runtime.pingPayload(newJObject())
    check pingPayload.isOk()
    check pingPayload.get()["ok"].getBool()
    check pingPayload.get()["samples"][0]["derpRegionId"].getInt() == 901

    let derpMapPayload = runtime.derpMapPayload()
    check derpMapPayload.isOk()
    check derpMapPayload.get()["ok"].getBool()
    check derpMapPayload.get()["regions"][0]["regionId"].getInt() == 901
    check derpMapPayload.get()["regions"][0]["regionCode"].getStr() == "sin"

  suite "Tsnet in-app runtime live":
    test "self-hosted control probe reaches running runtime state over h2":
      let cfgPath = headscaleJsonPath()
      if not fileExists(cfgPath):
        skip()

      let liveResult = loadHeadscaleConfig(cfgPath)
      if liveResult.isErr():
        skip()
      let live = liveResult.get()
      let controlUrl = jsonString(live, ["controlUrl", "controlURL"])
      let authKey = jsonString(live, ["authKey"])
      if controlUrl.len == 0 or authKey.len == 0:
        skip()

      let dir = tempRuntimeDir("probe")
      let cfg = makeLiveCfg(
        controlUrl, authKey, "nim-live-probe", dir, TsnetControlProtocolTs2021H2
      )

      let runtime = TsnetInAppRuntime.new(cfg)
      let started = runtime.start()
      check started.isOk()
      checkLiveRuntime(runtime, controlUrl, TsnetControlProtocolTs2021H2)

      let controlPayload = runtime.control.toJson()
      check controlPayload["probe"]["protocol"].getStr() == "ts2021/http2"
      check controlPayload["probe"]["noiseUrl"].getStr().endsWith("/ts2021")
      check controlPayload["register"]["registerUrl"].getStr().endsWith("/machine/register")
      check controlPayload["register"]["request"]["NodeKey"].getStr().startsWith("nodekey:")
      check controlPayload["register"]["stage"].getStr() == "registered"
      check controlPayload["mapPoll"]["mapUrl"].getStr().endsWith("/machine/map")
      check controlPayload["mapPoll"]["request"]["DiscoKey"].getStr().startsWith("discokey:")
      check controlPayload["mapPoll"]["stage"].getStr() == "mapped"

      let persisted = loadStoredState(dir)
      check persisted.isOk()
      check persisted.get().controlUrl == controlUrl
      check persisted.get().controlPublicKey == runtime.state.controlPublicKey

    test "self-hosted control probe reaches running runtime state over nim h3":
      if getEnv("NIM_TSNET_ENABLE_H3_LIVE_TEST").strip() != "1":
        skip()
        return

      let cfgPath = headscaleJsonPath()
      if not fileExists(cfgPath):
        skip()
        return

      let liveResult = loadHeadscaleConfig(cfgPath)
      if liveResult.isErr():
        skip()
        return
      let live = liveResult.get()
      let controlUrl = jsonString(live, ["controlUrl", "controlURL"])
      let authKey = jsonString(live, ["authKey"])
      if controlUrl.len == 0 or authKey.len == 0:
        skip()
        return

      let dir = tempRuntimeDir("probe-h3")
      let cfg = makeLiveCfg(
        controlUrl, authKey, "nim-live-probe-h3", dir, TsnetControlProtocolNimH3
      )

      let runtime = TsnetInAppRuntime.new(cfg)
      let started = runtime.start()
      check started.isOk()
      checkLiveRuntime(runtime, controlUrl, TsnetControlProtocolNimH3)

      let controlPayload = runtime.control.toJson()
      check controlPayload["probe"]["protocol"].getStr() == "nim_h3"
      check controlPayload["probe"]["noiseUrl"].getStr().contains("/nim-tsnet-control/v1/key")
      check controlPayload["register"]["registerUrl"].getStr().contains("/nim-tsnet-control/v1/register")
      check controlPayload["mapPoll"]["mapUrl"].getStr().contains("/nim-tsnet-control/v1/map")
else:
  import unittest2

  suite "Tsnet in-app runtime live":
    test "ssl disabled":
      skip()
