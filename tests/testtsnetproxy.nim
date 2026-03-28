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
