{.used.}

import std/[json, os, times]
import unittest2

import ../libp2p/multiaddress
import ../libp2p/transports/[tsnet/control, tsnet/proxy, tsnet/runtime, tsnet/state]
import ../libp2p/transports/tsnetprovidertypes
import ../libp2p/utility

proc tempRuntimeDir(tag: string): string =
  let path = getTempDir() / ("nim-libp2p-tsnet-proxy-" & tag & "-" & $epochTime())
  createDir(path)
  path

proc mockMappedTransport(
    hostname: string,
    nodeId: int,
    ip4: string,
    ip6: string,
    peerHostname: string,
    peerId: int,
    peerIp4: string,
): TsnetControlTransport =
  let fetchProc: TsnetControlKeyFetchProc =
    proc(controlUrl: string): Result[TsnetControlServerKey, string] {.closure, gcsafe, raises: [].} =
      ok(TsnetControlServerKey.init(
        capabilityVersion = 130,
        publicKey = "mkey:control",
        legacyPublicKey = "mkey:legacy",
        fetchedAtUnixMilli = 1774333653153
      ))
  let registerProc: TsnetControlRegisterProc =
    proc(controlUrl: string, request: JsonNode): Result[JsonNode, string] {.closure, gcsafe, raises: [].} =
      let hostinfo = request{"Hostinfo"}
      if hostinfo.isNil:
        return err("missing Hostinfo")
      let hostnameNode = hostinfo{"Hostname"}
      if hostnameNode.isNil:
        return err("missing Hostname")
      if hostnameNode.getStr() != hostname:
        return err("unexpected hostname")
      ok(%*{
        "MachineAuthorized": true,
        "User": {"LoginName": hostname, "DisplayName": hostname},
        "Login": {"LoginName": hostname & "@example.com"}
      })
  let mapProc: TsnetControlMapPollProc =
    proc(controlUrl: string, request: JsonNode): Result[JsonNode, string] {.closure, gcsafe, raises: [].} =
      ok(%*{
        "MapSessionHandle": hostname & "-session",
        "Seq": 1,
        "Node": {
          "ID": nodeId,
          "StableID": hostname & "-stable",
          "Name": hostname,
          "HomeDERP": 901,
          "Addresses": [ip4 & "/32", ip6 & "/128"],
          "Hostinfo": {"Hostname": hostname}
        },
        "DERPMap": {
          "OmitDefaultRegions": true,
          "Regions": {
            "901": {
              "RegionID": 901,
              "RegionCode": "sin",
              "RegionName": "Vultr Singapore",
              "Nodes": [{
                "Name": "901a",
                "RegionID": 901,
                "HostName": "64-176-84-12.sslip.io",
                "DERPPort": 443
              }]
            }
          }
        },
        "Peers": [{
          "ID": peerId,
          "StableID": peerHostname & "-stable",
          "Name": peerHostname,
          "HomeDERP": 901,
          "Addresses": [peerIp4 & "/32"],
          "Hostinfo": {"Hostname": peerHostname}
        }]
      })
  TsnetControlTransport.init(
    fetchServerKeyProc = fetchProc,
    registerNodeProc = registerProc,
    mapPollProc = mapProc
  )

suite "Tsnet proxy runtime":
  test "invalid proxy registry rows are ignored during lookup":
    unregisterProxyRoutes(7001)
    unregisterProxyRoutes(7002)
    let advertised = MultiAddress.init("/ip4/100.64.0.10/udp/4001/quic-v1/tsnet").tryGet()
    let raw = MultiAddress.init("/ip4/127.0.0.1/udp/41002/quic-v1").tryGet()
    check registerProxyRoute(7001, advertised, raw).isOk()
    injectInvalidProxyRegistrationForTest(7002, "/ip4/127.0.0.1/udp/49999/quic-v1")

    let looked = lookupRawTarget("ip4", "100.64.0.10", 4001, TsnetProxyKind.Quic)
    check looked.isOk()
    check looked.get() == raw

    unregisterProxyRoutes(7001)
    unregisterProxyRoutes(7002)

  test "proxy route snapshots ignore invalid proxy rows without crashing":
    unregisterProxyRoutes(7006)
    let advertised = MultiAddress.init("/ip4/100.64.0.16/udp/4006/quic-v1/tsnet").tryGet()
    let raw = MultiAddress.init("/ip4/127.0.0.1/udp/41016/quic-v1").tryGet()
    check registerProxyRoute(7006, advertised, raw).isOk()
    injectInvalidProxyRegistrationForTest(7006, "/ip4/127.0.0.1/udp/49992/quic-v1")

    let snapshots = proxyRouteSnapshots(7006)
    check snapshots.len == 1
    check snapshots[0].advertised == advertised
    check snapshots[0].raw == raw
    check snapshots[0].kind == TsnetProxyKind.Quic

    unregisterProxyRoutes(7006)

  test "proxy route snapshots stay stable across repeated sanitize cycles":
    unregisterProxyRoutes(7007)
    unregisterProxyRoutes(7008)
    let advertised = MultiAddress.init("/ip4/100.64.0.17/udp/4007/quic-v1/tsnet").tryGet()
    let raw = MultiAddress.init("/ip4/127.0.0.1/udp/41017/quic-v1").tryGet()
    check registerProxyRoute(7007, advertised, raw).isOk()
    injectInvalidProxyRegistrationForTest(7008, "/ip4/127.0.0.1/udp/49991/quic-v1")

    for _ in 0 .. 255:
      let snapshots = proxyRouteSnapshots(7007)
      check snapshots.len == 1
      check snapshots[0].advertised == advertised
      check snapshots[0].raw == raw
      let looked = lookupRawTarget("ip4", "100.64.0.17", 4007, TsnetProxyKind.Quic)
      check looked.isOk()
      check looked.get() == raw

    unregisterProxyRoutes(7007)
    unregisterProxyRoutes(7008)

  test "proxy route snapshots ignore malformed multiaddr rows":
    unregisterProxyRoutes(7011)
    unregisterProxyRoutes(7012)
    let advertised = MultiAddress.init("/ip4/100.64.0.19/udp/4009/quic-v1/tsnet").tryGet()
    let raw = MultiAddress.init("/ip4/127.0.0.1/udp/41019/quic-v1").tryGet()
    check registerProxyRoute(7011, advertised, raw).isOk()
    injectMalformedProxyRegistrationForTest(
      7012,
      "/ip4/100.64.0.19/udp/4009/quic-v1/tsnet",
      "invalid-raw-multiaddr"
    )

    let snapshots = proxyRouteSnapshots(7011)
    check snapshots.len == 1
    check snapshots[0].advertised == advertised
    check snapshots[0].raw == raw
    let looked = lookupRawTarget("ip4", "100.64.0.19", 4009, TsnetProxyKind.Quic)
    check looked.isOk()
    check looked.get() == raw

    unregisterProxyRoutes(7011)
    unregisterProxyRoutes(7012)

  test "proxy route snapshots ignore corrupted shared key lengths":
    unregisterProxyRoutes(7015)
    unregisterProxyRoutes(7016)
    let advertised = MultiAddress.init("/ip4/100.64.0.21/udp/4011/quic-v1/tsnet").tryGet()
    let raw = MultiAddress.init("/ip4/127.0.0.1/udp/41021/quic-v1").tryGet()
    check registerProxyRoute(7015, advertised, raw).isOk()
    injectCorruptProxyRegistrationLenForTest(
      7016,
      "/ip4/100.64.0.21/udp/4011/quic-v1/tsnet",
      "/ip4/127.0.0.1/udp/49989/quic-v1",
      advertisedLen = 1537
    )

    for _ in 0 .. 255:
      let snapshots = proxyRouteSnapshots(7015)
      check snapshots.len == 1
      check snapshots[0].advertised == advertised
      check snapshots[0].raw == raw

    unregisterProxyRoutes(7015)
    unregisterProxyRoutes(7016)

  test "resolve advertised remote survives invalid proxy rows across repeated sanitize cycles":
    unregisterProxyRoutes(7009)
    unregisterProxyRoutes(7010)
    let advertised = MultiAddress.init("/ip4/100.64.0.18/udp/4008/quic-v1/tsnet").tryGet()
    let raw = MultiAddress.init("/ip4/127.0.0.1/udp/41018/quic-v1").tryGet()
    check registerProxyRoute(7009, advertised, raw).isOk()
    injectInvalidProxyRegistrationForTest(7010, "/ip4/127.0.0.1/udp/49990/quic-v1")

    for _ in 0 .. 255:
      let looked = resolveAdvertisedRemote(raw)
      check looked.isOk()
      check looked.get() == advertised

    unregisterProxyRoutes(7009)
    unregisterProxyRoutes(7010)

  test "resolve advertised remote ignores corrupted shared key lengths":
    unregisterProxyRoutes(7017)
    let raw = MultiAddress.init("/ip4/127.0.0.1/udp/49988/quic-v1").tryGet()
    let advertised = MultiAddress.init("/ip4/100.64.0.22/udp/4012/quic-v1/tsnet").tryGet()
    registerResolvedRemote(7017, raw, advertised)
    injectCorruptResolvedRemoteRegistrationLenForTest(
      7017,
      "/ip4/127.0.0.1/udp/49987/quic-v1",
      "/ip4/100.64.0.23/udp/4013/quic-v1/tsnet",
      rawLen = 1537
    )

    for _ in 0 .. 255:
      let looked = resolveAdvertisedRemote(raw)
      check looked.isOk()
      check looked.get() == advertised

    unregisterProxyRoutes(7017)

  test "direct route snapshots ignore invalid resolved remote rows":
    unregisterProxyRoutes(7003)
    let advertised = MultiAddress.init("/ip4/100.64.0.12/udp/4002/quic-v1/tsnet").tryGet()
    let raw = MultiAddress.init("/ip4/203.0.113.12/udp/41012/quic-v1").tryGet()
    check registerDirectRoute(7003, advertised, raw, TsnetPathDirect).isOk()
    injectInvalidResolvedRemoteRegistrationForTest(7003, "/ip4/127.0.0.1/udp/49998/quic-v1")

    let snapshots = directRouteSnapshots(7003)
    check snapshots.len == 1
    check snapshots[0].advertised == advertised
    check snapshots[0].raw == raw
    check normalizeTailnetPath(snapshots[0].pathKind) == TsnetPathDirect

    unregisterProxyRoutes(7003)

  test "direct route snapshots ignore malformed multiaddr rows":
    unregisterProxyRoutes(7013)
    unregisterProxyRoutes(7014)
    let advertised = MultiAddress.init("/ip4/100.64.0.20/udp/4010/quic-v1/tsnet").tryGet()
    let raw = MultiAddress.init("/ip4/203.0.113.20/udp/41020/quic-v1").tryGet()
    check registerDirectRoute(7013, advertised, raw, TsnetPathDirect).isOk()
    injectMalformedDirectRouteRegistrationForTest(
      7014,
      "/ip4/100.64.0.20/udp/4010/quic-v1/tsnet",
      "invalid-direct-raw",
      TsnetPathDirect
    )

    let snapshots = directRouteSnapshots(7013)
    check snapshots.len == 1
    check snapshots[0].advertised == advertised
    check snapshots[0].raw == raw
    check normalizeTailnetPath(snapshots[0].pathKind) == TsnetPathDirect

    unregisterProxyRoutes(7013)
    unregisterProxyRoutes(7014)

  test "direct route snapshots ignore corrupted shared key lengths":
    unregisterProxyRoutes(7018)
    unregisterProxyRoutes(7019)
    let advertised = MultiAddress.init("/ip4/100.64.0.24/udp/4014/quic-v1/tsnet").tryGet()
    let raw = MultiAddress.init("/ip4/203.0.113.24/udp/41024/quic-v1").tryGet()
    check registerDirectRoute(7018, advertised, raw, TsnetPathDirect).isOk()
    injectCorruptDirectRouteRegistrationLenForTest(
      7019,
      "/ip4/100.64.0.24/udp/4014/quic-v1/tsnet",
      "/ip4/203.0.113.25/udp/41025/quic-v1",
      TsnetPathDirect,
      rawLen = 1537
    )

    for _ in 0 .. 255:
      let snapshots = directRouteSnapshots(7018)
      check snapshots.len == 1
      check snapshots[0].advertised == advertised
      check snapshots[0].raw == raw
      check normalizeTailnetPath(snapshots[0].pathKind) == TsnetPathDirect

    unregisterProxyRoutes(7018)
    unregisterProxyRoutes(7019)

  test "resolved remote cleanup survives unregister without crashing":
    unregisterProxyRoutes(7004)
    let raw = MultiAddress.init("/ip4/127.0.0.1/udp/49997/quic-v1").tryGet()
    let advertised = MultiAddress.init("/ip4/100.64.0.13/udp/4003/quic-v1/tsnet").tryGet()
    registerResolvedRemote(7004, raw, advertised)
    injectInvalidResolvedRemoteRegistrationForTest(7004, "/ip4/127.0.0.1/udp/49996/quic-v1")

    let looked = resolveAdvertisedRemote(raw)
    check looked.isOk()
    check looked.get() == advertised

    unregisterProxyRoutes(7004)
    check resolveAdvertisedRemote(raw).isErr()

  test "resolved remote register survives invalid rows during sanitize":
    unregisterProxyRoutes(7005)
    let rawA = MultiAddress.init("/ip4/127.0.0.1/udp/49995/quic-v1").tryGet()
    let advertisedA = MultiAddress.init("/ip4/100.64.0.14/udp/4004/quic-v1/tsnet").tryGet()
    let rawB = MultiAddress.init("/ip4/127.0.0.1/udp/49994/quic-v1").tryGet()
    let advertisedB = MultiAddress.init("/ip4/100.64.0.15/udp/4005/quic-v1/tsnet").tryGet()
    injectInvalidResolvedRemoteRegistrationForTest(7005, "/ip4/127.0.0.1/udp/49993/quic-v1")

    registerResolvedRemote(7005, rawA, advertisedA)
    registerResolvedRemote(7005, rawB, advertisedB)

    let lookedA = resolveAdvertisedRemote(rawA)
    check lookedA.isOk()
    check lookedA.get() == advertisedA
    let lookedB = resolveAdvertisedRemote(rawB)
    check lookedB.isOk()
    check lookedB.get() == advertisedB

    unregisterProxyRoutes(7005)

  test "resolved remote register replaces previous advertised for the same raw":
    unregisterProxyRoutes(7020)
    let raw = MultiAddress.init("/ip4/127.0.0.1/udp/49992/quic-v1").tryGet()
    let advertisedA = MultiAddress.init("/ip4/100.64.0.25/udp/4015/quic-v1/tsnet").tryGet()
    let advertisedB = MultiAddress.init("/ip4/100.64.0.26/udp/4016/quic-v1/tsnet").tryGet()

    registerResolvedRemote(7020, raw, advertisedA)
    check resolveAdvertisedRemote(raw).get() == advertisedA

    registerResolvedRemote(7020, raw, advertisedB)
    let looked = resolveAdvertisedRemote(raw)
    check looked.isOk()
    check looked.get() == advertisedB

    unregisterProxyRoutes(7020)

  test "resolved remote lookup ignores detached gc text fields":
    unregisterProxyRoutes(7021)
    let raw = MultiAddress.init("/ip4/127.0.0.1/udp/49991/quic-v1").tryGet()
    let advertised = MultiAddress.init("/ip4/100.64.0.27/udp/4017/quic-v1/tsnet").tryGet()

    injectResolvedRemoteWithDetachedTextsForTest(
      7021,
      $raw,
      $advertised,
      rawText = "",
      advertisedText = ""
    )

    for _ in 0 .. 255:
      let looked = resolveAdvertisedRemote(raw)
      check looked.isOk()
      check looked.get() == advertised

    unregisterProxyRoutes(7021)

  test "status payload ignores invalid direct route rows":
    let runtimeDir = tempRuntimeDir("invalid-direct-route-status")
    let runtimeCfg = TsnetProviderConfig(
      controlUrl: "http://headscale.local",
      authKey: "tskey-invalid-direct-route-status",
      hostname: "invalid-direct-route-status-node",
      stateDir: runtimeDir,
      wireguardPort: 41649,
      bridgeLibraryPath: "",
      logLevel: "",
      enableDebug: false,
      bridgeExtraJson: ""
    )
    let runtime = TsnetInAppRuntime.new(
      runtimeCfg,
      mockMappedTransport(
        "invalid-direct-route-status-node",
        11,
        "100.64.0.21",
        "fd7a:115c:a1e0::21",
        "peer-node",
        12,
        "100.64.0.22"
      )
    )

    check runtime.start().isOk()
    check runtime.ready()
    injectInvalidDirectRouteRegistrationForTest(
      runtime.runtimeId,
      "/ip4/203.0.113.21/udp/42021/quic-v1"
    )

    let statusPayload = runtime.statusPayload()
    check statusPayload.isOk()
    check directRouteSnapshots(runtime.runtimeId).len == 0

    runtime.stop()

  test "mapped runtime exposes proxy-backed TCP and UDP routes with lifecycle cleanup":
    let serverDir = tempRuntimeDir("server")
    let clientDir = tempRuntimeDir("client")
    let serverCfg = TsnetProviderConfig(
      controlUrl: "http://headscale.local",
      authKey: "tskey-server",
      hostname: "server-node",
      stateDir: serverDir,
      wireguardPort: 41641,
      bridgeLibraryPath: "",
      logLevel: "",
      enableDebug: false,
      bridgeExtraJson: ""
    )
    let clientCfg = TsnetProviderConfig(
      controlUrl: "http://headscale.local",
      authKey: "tskey-client",
      hostname: "client-node",
      stateDir: clientDir,
      wireguardPort: 41642,
      bridgeLibraryPath: "",
      logLevel: "",
      enableDebug: false,
      bridgeExtraJson: ""
    )
    let server = TsnetInAppRuntime.new(
      serverCfg,
      mockMappedTransport(
        "server-node",
        1,
        "100.64.0.10",
        "fd7a:115c:a1e0::10",
        "client-node",
        2,
        "100.64.0.11"
      )
    )
    let client = TsnetInAppRuntime.new(
      clientCfg,
      mockMappedTransport(
        "client-node",
        2,
        "100.64.0.11",
        "fd7a:115c:a1e0::11",
        "server-node",
        1,
        "100.64.0.10"
      )
    )

    check server.start().isOk()
    check client.start().isOk()
    check server.ready()
    check client.ready()
    check server.capabilities().proxyBacked
    check client.capabilities().proxyBacked

    let serverTcpRaw = MultiAddress.init("/ip4/127.0.0.1/tcp/41001").tryGet()
    let serverUdpRaw = MultiAddress.init("/ip4/127.0.0.1/udp/41002/quic-v1").tryGet()

    let serverTcp = server.listenTcpProxy("ip4", 4001, serverTcpRaw)
    check serverTcp.isOk()
    check $serverTcp.get() == "/ip4/100.64.0.10/tcp/4001/tsnet"

    let serverUdp = server.listenUdpProxy("ip4", 4001, serverUdpRaw)
    check serverUdp.isOk()
    check $serverUdp.get() == "/ip4/100.64.0.10/udp/4001/quic-v1/tsnet"

    let clientTcpDial = client.dialTcpProxy("ip4", "100.64.0.10", 4001)
    check clientTcpDial.isOk()
    check clientTcpDial.get() == serverTcpRaw

    let clientUdpDial = client.dialUdpProxy("ip4", "100.64.0.10", 4001)
    check clientUdpDial.isOk()
    check clientUdpDial.get() == serverUdpRaw

    let resolvedTcp = server.resolveRemote(serverTcpRaw)
    check resolvedTcp.isOk()
    check resolvedTcp.get() == serverTcp.get()

    let resolvedUdp = server.resolveRemote(serverUdpRaw)
    check resolvedUdp.isOk()
    check resolvedUdp.get() == serverUdp.get()

    server.stop()
    let dialAfterStop = client.dialTcpProxy("ip4", "100.64.0.10", 4001)
    check dialAfterStop.isErr()

    discard client.reset()

  test "direct and punched_direct routes override relay lookup and reset path labels":
    let dir = tempRuntimeDir("direct-paths")
    let cfg = TsnetProviderConfig(
      controlUrl: "http://headscale.local",
      authKey: "tskey-client",
      hostname: "client-node",
      stateDir: dir,
      wireguardPort: 41642,
      bridgeLibraryPath: "",
      logLevel: "",
      enableDebug: false,
      bridgeExtraJson: ""
    )
    let runtime = TsnetInAppRuntime.new(
      cfg,
      mockMappedTransport(
        "client-node",
        2,
        "100.64.0.11",
        "fd7a:115c:a1e0::11",
        "server-node",
        1,
        "100.64.0.10"
      )
    )

    check runtime.start().isOk()
    check runtime.ready()
    check runtime.statusPayload().get()["tailnetPath"].getStr() == "relay"

    let directAdvertised = MultiAddress.init("/ip4/100.64.0.10/tcp/4001/tsnet").tryGet()
    let directRaw = MultiAddress.init("/ip4/198.51.100.10/tcp/41001").tryGet()
    check runtime.registerDirectProxyRoute(directAdvertised, directRaw).isOk()

    let directDial = runtime.dialTcpProxy("ip4", "100.64.0.10", 4001)
    check directDial.isOk()
    check directDial.get() == directRaw
    let directStatus = runtime.statusPayload()
    check directStatus.isOk()
    check directStatus.get()["tailnetPath"].getStr() == "direct"
    check directStatus.get()["tailnetRelay"].getStr() == ""

    let punchedAdvertised = MultiAddress.init("/ip4/100.64.0.10/udp/4001/quic-v1/tsnet").tryGet()
    let punchedRawPrimary = MultiAddress.init("/ip4/192.168.1.10/udp/42002/quic-v1").tryGet()
    let punchedRawBackup = MultiAddress.init("/ip4/203.0.113.10/udp/42003/quic-v1").tryGet()
    check runtime.registerDirectProxyRoute(
      punchedAdvertised,
      punchedRawPrimary,
      punched = true
    ).isOk()
    check runtime.registerDirectProxyRoute(
      punchedAdvertised,
      punchedRawBackup,
      punched = true
    ).isOk()

    let punchedDial = runtime.dialUdpProxy("ip4", "100.64.0.10", 4001)
    check punchedDial.isOk()
    check punchedDial.get() == punchedRawBackup
    let punchedStatus = runtime.statusPayload()
    check punchedStatus.isOk()
    check punchedStatus.get()["tailnetPath"].getStr() == "punched_direct"
    check punchedStatus.get()["tailnetRelay"].getStr() == ""
    check punchedStatus.get()["tailnetPrimaryPath"].getStr() == "punched_direct"
    check punchedStatus.get()["tailnetMultipath"].getBool()
    check punchedStatus.get()["tailnetDirectRouteCount"].getInt() == 2
    check punchedStatus.get()["tailnetPunchedRouteCount"].getInt() == 1
    check punchedStatus.get()["tailnetPaths"].len == 4

    let directAfterPunch = runtime.dialUdpProxy("ip4", "100.64.0.10", 4001)
    check directAfterPunch.isOk()
    check directAfterPunch.get() == punchedRawBackup
    let settledDirectStatus = runtime.statusPayload()
    check settledDirectStatus.isOk()
    check settledDirectStatus.get()["tailnetPath"].getStr() == "direct"
    check settledDirectStatus.get()["tailnetRelay"].getStr() == ""
    check settledDirectStatus.get()["tailnetPrimaryPath"].getStr() == "direct"
    check settledDirectStatus.get()["tailnetMultipath"].getBool()
    check settledDirectStatus.get()["tailnetDirectRouteCount"].getInt() == 2
    check settledDirectStatus.get()["tailnetPunchedRouteCount"].getInt() == 1
    check settledDirectStatus.get()["tailnetPaths"].len == 4

    runtime.demoteRelayPath()
    let relayStatus = runtime.statusPayload()
    check relayStatus.isOk()
    check relayStatus.get()["tailnetPath"].getStr() == "relay"
    check relayStatus.get()["tailnetRelay"].getStr() == "sin"
    check relayStatus.get()["tailnetPrimaryPath"].getStr() == "relay"
    check relayStatus.get()["tailnetMultipath"].getBool()
    check relayStatus.get()["tailnetDirectRouteCount"].getInt() == 2
    check relayStatus.get()["tailnetPunchedRouteCount"].getInt() == 1

    runtime.stop()
    let afterStop = runtime.dialTcpProxy("ip4", "100.64.0.10", 4001)
    check afterStop.isErr()

  test "exact udp dial can use direct route when exact target is direct-ready":
    let dir = tempRuntimeDir("exact-udp-dial")
    let cfg = TsnetProviderConfig(
      controlUrl: "http://headscale.local",
      authKey: "tskey-client",
      hostname: "client-node",
      stateDir: dir,
      wireguardPort: 41642,
      bridgeLibraryPath: "",
      logLevel: "",
      enableDebug: false,
      bridgeExtraJson: ""
    )
    let runtime = TsnetInAppRuntime.new(
      cfg,
      mockMappedTransport(
        "client-node",
        2,
        "100.64.0.11",
        "fd7a:115c:a1e0::11",
        "server-node",
        1,
        "100.64.0.10"
      )
    )

    check runtime.start().isOk()
    check runtime.ready()

    let advertised = MultiAddress.init("/ip4/100.64.0.10/udp/4001/quic-v1/tsnet").tryGet()
    let directRaw = MultiAddress.init("/ip4/198.51.100.10/udp/42012/quic-v1").tryGet()

    check runtime.registerDirectProxyRoute(advertised, directRaw, punched = true).isOk()

    let routeSelectingDial = runtime.dialUdpProxy("ip4", "100.64.0.10", 4001)
    check routeSelectingDial.isOk()
    check routeSelectingDial.get() == directRaw

    let exactDial = runtime.dialUdpProxyExact("ip4", "100.64.0.10", 4001)
    check exactDial.isOk()
    check exactDial.get() == directRaw

    runtime.stop()

  test "exact udp dial target carries explicit readiness mode":
    let dir = tempRuntimeDir("exact-udp-dial-target")
    let cfg = TsnetProviderConfig(
      controlUrl: "http://headscale.local",
      authKey: "tskey-client",
      hostname: "client-node",
      stateDir: dir,
      wireguardPort: 41642,
      bridgeLibraryPath: "",
      logLevel: "",
      enableDebug: false,
      bridgeExtraJson: ""
    )
    let runtime = TsnetInAppRuntime.new(
      cfg,
      mockMappedTransport(
        "client-node",
        2,
        "100.64.0.11",
        "fd7a:115c:a1e0::11",
        "server-node",
        1,
        "100.64.0.10"
      )
    )

    check runtime.start().isOk()
    check runtime.ready()

    let advertised = MultiAddress.init("/ip4/100.64.0.10/udp/4001/quic-v1/tsnet").tryGet()
    let localRaw = MultiAddress.init("/ip4/127.0.0.1/udp/42013/quic-v1").tryGet()
    check registerProxyRoute(runtime.runtimeId, advertised, localRaw).isOk()

    let exactTarget = runtime.dialUdpProxyExactTarget("ip4", "100.64.0.10", 4001)
    check exactTarget.isOk()
    check exactTarget.get().rawAddress == localRaw
    check exactTarget.get().mode == TsnetProxyDialMode.Local

    runtime.stop()

  test "exact udp dial target prefers direct route before relay":
    let dir = tempRuntimeDir("exact-udp-direct-target")
    let cfg = TsnetProviderConfig(
      controlUrl: "http://headscale.local",
      authKey: "tskey-client",
      hostname: "client-node",
      stateDir: dir,
      wireguardPort: 41642,
      bridgeLibraryPath: "",
      logLevel: "",
      enableDebug: false,
      bridgeExtraJson: ""
    )
    let runtime = TsnetInAppRuntime.new(
      cfg,
      mockMappedTransport(
        "client-node",
        2,
        "100.64.0.11",
        "fd7a:115c:a1e0::11",
        "server-node",
        1,
        "100.64.0.10"
      )
    )

    check runtime.start().isOk()
    check runtime.ready()

    let advertised = MultiAddress.init("/ip4/100.64.0.10/udp/4001/quic-v1/tsnet").tryGet()
    let directRaw = MultiAddress.init("/ip4/198.51.100.10/udp/42002/quic-v1").tryGet()
    check runtime.registerDirectProxyRoute(advertised, directRaw, punched = true).isOk()

    let exactTarget = runtime.dialUdpProxyExactTarget("ip4", "100.64.0.10", 4001)
    check exactTarget.isOk()
    check exactTarget.get().rawAddress == directRaw
    check exactTarget.get().mode == TsnetProxyDialMode.DirectRoute

    let status = runtime.statusPayload()
    check status.isOk()
    check status.get()["tailnetPath"].getStr() in ["punched_direct", "direct"]
    check status.get()["tailnetDirectRouteCount"].getInt() == 1

    runtime.stop()

  test "failed direct route is temporarily suspended and backup route is selected":
    let dir = tempRuntimeDir("direct-fallback")
    let cfg = TsnetProviderConfig(
      controlUrl: "http://headscale.local",
      authKey: "tskey-client",
      hostname: "client-node",
      stateDir: dir,
      wireguardPort: 41642,
      bridgeLibraryPath: "",
      logLevel: "",
      enableDebug: false,
      bridgeExtraJson: ""
    )
    let runtime = TsnetInAppRuntime.new(
      cfg,
      mockMappedTransport(
        "client-node",
        2,
        "100.64.0.11",
        "fd7a:115c:a1e0::11",
        "server-node",
        1,
        "100.64.0.10"
      )
    )

    check runtime.start().isOk()
    check runtime.ready()

    let advertised = MultiAddress.init("/ip4/100.64.0.10/udp/4001/quic-v1/tsnet").tryGet()
    let preferredRaw = MultiAddress.init("/ip4/198.51.100.10/udp/42002/quic-v1").tryGet()
    let backupRaw = MultiAddress.init("/ip4/192.168.1.10/udp/42003/quic-v1").tryGet()
    check runtime.registerDirectProxyRoute(advertised, preferredRaw, punched = true).isOk()
    check runtime.registerDirectProxyRoute(advertised, backupRaw, punched = true).isOk()

    let firstDial = runtime.dialUdpProxy("ip4", "100.64.0.10", 4001)
    check firstDial.isOk()
    check firstDial.get() == preferredRaw

    check runtime.markFailedDirectProxyRoute(advertised, preferredRaw).isOk()

    let fallbackDial = runtime.dialUdpProxy("ip4", "100.64.0.10", 4001)
    check fallbackDial.isOk()
    check fallbackDial.get() == backupRaw

    let snapshots = directRouteSnapshots(runtime.runtimeId)
    check snapshots.len == 2
    var preferredSnapshotFound = false
    for snapshot in snapshots:
      if $snapshot.raw == $preferredRaw:
        preferredSnapshotFound = true
        check snapshot.failureCount == 1
        check snapshot.suspendedUntilUnixMilli > 0
    check preferredSnapshotFound

    runtime.stop()
