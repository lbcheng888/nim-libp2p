{.used.}

when defined(libp2p_msquic_experimental):
  import std/[json, os, strutils, times]
  import unittest2
  import chronos

  import ../libp2p/transports/[tsnet/control, tsnet/quiccontrol, tsnet/runtime, tsnetprovidertypes]
  import ../libp2p/utility

  const
    TestCertificate = """-----BEGIN CERTIFICATE-----
MIIDCTCCAfGgAwIBAgIUFlk8X0WMC1uH+ONhqdMOH/xpTwIwDQYJKoZIhvcNAQEL
BQAwFDESMBAGA1UEAwwJbG9jYWxob3N0MB4XDTI1MTEwNzEzMTUzNloXDTI2MTEw
NzEzMTUzNlowFDESMBAGA1UEAwwJbG9jYWxob3N0MIIBIjANBgkqhkiG9w0BAQEF
AAOCAQ8AMIIBCgKCAQEAxzZ1uprFN6z0AutrOtzHXyAAVtodC708bVWSr/cj3Nbm
Bk1jAobnZVQD/tZhkqhx+LMpXaJ62PrLqfOHH6Lr1npNTgRToLEqtuXF7tnuXiWs
iBTyA3zbP5OmzPy2eZmmUojtpFvb2t7IMViVOTtsgUlM6vNkqk6cLAeIRBNy0Uti
HKn06z6Do4NNvm/bylQsHOg1xmBsrj5VCb+IBw2aBJm1sq8I5fn106ocoF+aXAnF
i/PemsNj9RyIpYAF8/1M3hcXfRjcfYU6q/QtQLlEeZBHUozKjyGTlu3snU9ssLyz
qdDft7tTJ82g+N+FNbGUqKRKykc6q5mguFR993f0PQIDAQABo1MwUTAdBgNVHQ4E
FgQUsc2QNJqJyhA9hoxW+BCalOL28jkwHwYDVR0jBBgwFoAUsc2QNJqJyhA9hoxW
+BCalOL28jkwDwYDVR0TAQH/BAUwAwEB/zANBgkqhkiG9w0BAQsFAAOCAQEAtImN
3+maaA/fWAb3k0slLirqV51w7hYaza0g/8xB9gqAJQL2rLfCWXZOlOB/fLCghIPt
lBcIokfj7qJq0vRAzs7aHO77wGeWnjt2a5dD0PpMlJGqWZzt3Pp/d8CGNGtm9HGS
zJNY7BHiLQXfbThvx6k/Bdwwu5Wsiv5XbZbhGh1q5wusy09i8M+NYuvjDAPuLPOJ
tlz1zTO7a2YWG1dbMxemzbz90ZJoqrsiHw3AAtiO7k15YqmvLVpu+GLS7wIOIviP
/+R8wdJd6+/oL8XADgbPMwkytgghBDWD1zhbuUuO28ELevgV/N9Wy1WvYhhZiPZ2
figoosWp8NMAFoiMwA==
-----END CERTIFICATE-----"""

    TestPrivateKey = """-----BEGIN PRIVATE KEY-----
MIIEugIBADANBgkqhkiG9w0BAQEFAASCBKQwggSgAgEAAoIBAQDHNnW6msU3rPQC
62s63MdfIABW2h0LvTxtVZKv9yPc1uYGTWMChudlVAP+1mGSqHH4syldonrY+sup
84cfouvWek1OBFOgsSq25cXu2e5eJayIFPIDfNs/k6bM/LZ5maZSiO2kW9va3sgx
WJU5O2yBSUzq82SqTpwsB4hEE3LRS2IcqfTrPoOjg02+b9vKVCwc6DXGYGyuPlUJ
v4gHDZoEmbWyrwjl+fXTqhygX5pcCcWL896aw2P1HIilgAXz/UzeFxd9GNx9hTqr
9C1AuUR5kEdSjMqPIZOW7eydT2ywvLOp0N+3u1MnzaD434U1sZSopErKRzqrmaC4
VH33d/Q9AgMBAAECggEAHzufm6EWWJNKMob8ad8hdv2KcBOESEnkBnRLKkGCIuai
a8yIQGYsM0vH0JWF+LtmGwrj6mVGA2zWr4+Z2NDvTtIf+qJdBi2gt8owjTEn2STo
9vDpvLg/m6knlq3sYgY/+GK1d3ZbcuZI1su/oZh6e46le5SrbLQkchbAO4QdFUkI
h88McLhZakWCWbCSd7xQg42JTauhuHeo+Y916ktpABO9RPOUzTSd8YvByK9HAX5t
ax174FOZVWAu57mVIZ/3pD3RKMextny+3IucGR9r4Qv2Dndo9e4FZzjl+KTtpT4t
1Stumr9V7knjFbsd2UdRe+jiGe3yAmd1QsXvCXANQQKBgQDlO6XK54uKMjWkVc5B
DpEE7n6lvef5t3BAnVEemIs3vE4KIYcXOkYgwAsXtdzZvYjtMdZL0MEGXX0rJd/D
DwC6Ig4hzmLBGN3rOt+UTvnEXncyU22ZNLur31A+5jlxb+TuXR8wGDhjwVp1Y/aw
1tyn4Aid6dT1qS3jdMTQzu/3UQKBgQDeeW5LjjsbD+nDPY4xHJyag/GZ60v9wx9E
J6fNxRSNTFaZYwrZgjV15dwtSsFYoVuTIAQ8NvmUQY3EsrbWdb1IaIiwJ7n03syy
5P9anTL0q7UrwhGg424eZjYtcKdztlh6mKC/41TGphyhtmPYOO6eOLDaRrh7KmXE
ogiRZNkLLQJ/ZEm0PxEN+2f8D+l6UvwMdhvhTKHI23dlpN8unjQetEOt4MDKWV8l
Ty61q6nk9V32ic9D8edii2ZbXIU1YCEwMD618BRbIB/A9yjKqBflLgQmId5eFKj9
cjRA50PR3c8WWTJkcqYmBX6SFMmnI7bc0pUxL+UdRly9tsVfVfszAQKBgGT+bRKB
m9VaMP1/2Sf0XCdM1IXSKiolxPDUq7meyQin6fwx2QAKuygtU/l/oSwR/BdbBnEr
Z7tk0u3DT3sl8eqIAd0t+53s8rIXgNBq4nHt7Q3TSNtnw1qrfda8+FdwJNRqqzbR
BXA0gnTq7oJ+vdw30hkU17SZ957/C7KtPFZ1AoGATM42Hj6UCOwN28kI8H97g3hB
8qL4HyummV+S0xTkmpqaaeTlig6Nn3NO/XlIeM4JYcyMjCTkwDXKgthB5rgJGAka
9MEr/XFzQFG5qMgXwDriPkD9VdyAZAatsNHKyAQo3Cmq5DXJl59OZ15W9jlqKaQZ
/z8swJdkQ7mkgfDuX14=
-----END PRIVATE KEY-----"""

  proc tempRuntimeDir(tag: string): string =
    let path = getTempDir() / ("nim-libp2p-tsnet-quic-" & tag & "-" & $epochTime())
    createDir(path)
    path

  proc mockControlTransport(): TsnetControlTransport =
    let fetchProc: TsnetControlKeyFetchProc =
      proc(controlUrl: string): Result[TsnetControlServerKey, string] {.closure, gcsafe, raises: [].} =
        ok(TsnetControlServerKey.init(
          capabilityVersion = 130,
          publicKey = "mkey:control",
          legacyPublicKey = "mkey:legacy",
          fetchedAtUnixMilli = 1774333653153,
          rawPayload = %*{
            "publicKey": "mkey:control",
            "legacyPublicKey": "mkey:legacy"
          }
        ))
    let registerProc: TsnetControlRegisterProc =
      proc(controlUrl: string, request: JsonNode): Result[JsonNode, string] {.closure, gcsafe, raises: [].} =
        if not request{"NodeKey"}.getStr().startsWith("nodekey:"):
          return err("unexpected NodeKey")
        ok(%*{
          "MachineAuthorized": true,
          "User": {"LoginName": "lbcheng", "DisplayName": "LB Cheng"},
          "Login": {"LoginName": "lbcheng@example.com"}
        })
    let mapProc: TsnetControlMapPollProc =
      proc(controlUrl: string, request: JsonNode): Result[JsonNode, string] {.closure, gcsafe, raises: [].} =
        if not request{"DiscoKey"}.getStr().startsWith("discokey:"):
          return err("unexpected DiscoKey")
        ok(%*{
          "MapSessionHandle": "session-1",
          "Seq": 7,
          "Node": {
            "ID": 1,
            "StableID": "node-1",
            "Name": "nim-runtime",
            "HomeDERP": 901,
            "Addresses": ["100.64.0.10/32", "fd7a:115c:a1e0::10/128"],
            "Hostinfo": {"Hostname": "nim-runtime"}
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
            "ID": 2,
            "StableID": "node-2",
            "Name": "peer-runtime",
            "HomeDERP": 901,
            "Addresses": ["100.64.0.11/32"],
            "Hostinfo": {"Hostname": "peer-runtime"}
          }]
        })
    TsnetControlTransport.init(
      protocolLabel = "ts2021/http2",
      fetchServerKeyProc = fetchProc,
      registerNodeProc = registerProc,
      mapPollProc = mapProc
    )

  proc startGateway(openNetwork = false): tuple[gateway: TsnetQuicControlGateway, controlUrl: string] =
    let gateway = TsnetQuicControlGateway.new(TsnetQuicControlGatewayConfig(
      listenHost: "127.0.0.1",
      listenPort: 0'u16,
      controlUrl: if openNetwork: "" else: "https://headscale.invalid",
      upstreamTransport: if openNetwork: nil else: mockControlTransport(),
      certificatePem: TestCertificate,
      privateKeyPem: TestPrivateKey,
      openNetwork: openNetwork,
      publicHost: "127.0.0.1",
      publicIpv4: "127.0.0.1",
      publicIpv6: "::1",
      regionId: 901,
      regionCode: "sin",
      regionName: "Open QUIC Network",
      derpPort: 9446
    ))
    let started = gateway.start()
    check started.isOk()
    check gateway.boundPort() > 0'u16
    waitFor sleepAsync(100)
    (gateway, "https://localhost:" & $gateway.boundPort())

  suite "Tsnet control-over-QUIC":
    test "custom nim_quic gateway proxies key register and map":
      let (gateway, controlUrl) = startGateway()
      defer:
        gateway.stop()

      let fetched = fetchControlServerKeyQuicSafe(controlUrl)
      check fetched.isOk()
      check fetched.get().publicKey == "mkey:control"

      let registerReq = buildRegisterRequestPayload(
        capabilityVersion = 130,
        nodePublicKey = "nodekey:node",
        machinePublicKey = "mkey:machine",
        hostname = "nim-runtime",
        authKey = "tskey-test"
      )
      let registerRes = registerNodeQuic(controlUrl, registerReq)
      check registerRes.isOk()
      check registerRes.get()["MachineAuthorized"].getBool()

      let mapReq = buildMapRequestPayload(
        capabilityVersion = 130,
        nodePublicKey = "nodekey:node",
        discoPublicKey = "discokey:wg",
        hostname = "nim-runtime"
      )
      let mapRes = mapPollQuic(controlUrl, mapReq)
      check mapRes.isOk()
      check mapRes.get()["DERPMap"]["Regions"]["901"]["RegionCode"].getStr() == "sin"

      let healthRes = healthQuic(controlUrl)
      check healthRes.isOk()
      check healthRes.get()["protocol"].getStr() == "nim_quic"
      check gateway.acceptedConnections == 1

    test "open network gateway authorizes register and map without auth key":
      let (gateway, controlUrl) = startGateway(openNetwork = true)
      defer:
        gateway.stop()

      let fetched = fetchControlServerKeyQuicSafe(controlUrl)
      check fetched.isOk()
      check fetched.get().publicKey == "mkey:open-network-control"

      let registerReq = buildRegisterRequestPayload(
        capabilityVersion = 130,
        nodePublicKey = "nodekey:abcdef0123456789",
        machinePublicKey = "mkey:machine",
        libp2pPeerId = "12D3KooWAndroidPeer",
        libp2pListenAddrs = @["/ip4/100.64.160.122/udp/4001/quic-v1/tsnet/p2p/12D3KooWAndroidPeer"],
        hostname = "nim-open-runtime",
        authKey = ""
      )
      let registerRes = registerNodeQuic(controlUrl, registerReq)
      check registerRes.isOk()
      check registerRes.get()["MachineAuthorized"].getBool()
      check registerRes.get()["Node"]["Addresses"][0].getStr().startsWith("100.64.")

      let mapReq = buildMapRequestPayload(
        capabilityVersion = 130,
        nodePublicKey = "nodekey:abcdef0123456789",
        discoPublicKey = "discokey:wg",
        libp2pPeerId = "12D3KooWAndroidPeer",
        libp2pListenAddrs = @["/ip4/100.64.160.122/udp/4001/quic-v1/tsnet/p2p/12D3KooWAndroidPeer"],
        hostname = "nim-open-runtime"
      )
      let mapRes = mapPollQuic(controlUrl, mapReq)
      check mapRes.isOk()
      check mapRes.get()["DERPMap"]["Regions"]["901"]["RegionCode"].getStr() == "sin"
      check mapRes.get()["Node"]["Addresses"][0].getStr().startsWith("100.64.")
      check mapRes.get()["Node"]["PeerID"].getStr() == "12D3KooWAndroidPeer"
      check mapRes.get()["Node"]["ListenAddrs"][0].getStr().contains("/tsnet/p2p/12D3KooWAndroidPeer")

      let registerReq2 = buildRegisterRequestPayload(
        capabilityVersion = 130,
        nodePublicKey = "nodekey:feedface01234567",
        machinePublicKey = "mkey:machine-2",
        libp2pPeerId = "12D3KooWMacPeer",
        libp2pListenAddrs = @["/ip4/100.64.203.158/udp/4001/quic-v1/tsnet/p2p/12D3KooWMacPeer"],
        hostname = "nim-open-runtime-2",
        authKey = ""
      )
      let registerRes2 = registerNodeQuic(controlUrl, registerReq2)
      check registerRes2.isOk()

      let mapReq2 = buildMapRequestPayload(
        capabilityVersion = 130,
        nodePublicKey = "nodekey:feedface01234567",
        discoPublicKey = "discokey:wg-2",
        libp2pPeerId = "12D3KooWMacPeer",
        libp2pListenAddrs = @["/ip4/100.64.203.158/udp/4001/quic-v1/tsnet/p2p/12D3KooWMacPeer"],
        hostname = "nim-open-runtime-2"
      )
      let mapRes2 = mapPollQuic(controlUrl, mapReq2)
      check mapRes2.isOk()
      check mapRes2.get()["Peers"].len == 1
      check mapRes2.get()["Peers"][0]["PeerID"].getStr() == "12D3KooWAndroidPeer"
      check mapRes2.get()["Peers"][0]["ListenAddrs"][0].getStr().contains("/tsnet/p2p/12D3KooWAndroidPeer")

    test "runtime reaches running state over nim_quic without ssl":
      let (gateway, controlUrl) = startGateway()
      defer:
        gateway.stop()

      let dir = tempRuntimeDir("runtime")
      let cfg = TsnetProviderConfig(
        controlUrl: controlUrl,
        controlProtocol: TsnetControlProtocolNimQuic,
        authKey: "tskey-test",
        hostname: "nim-runtime",
        stateDir: dir,
        wireguardPort: 41641,
        bridgeLibraryPath: "",
        logLevel: "",
        enableDebug: false,
        bridgeExtraJson: ""
      )

      let liveRuntime = TsnetInAppRuntime.new(cfg)
      let started = liveRuntime.start()
      check started.isOk()
      check liveRuntime.ready()
      check liveRuntime.status == TsnetInAppRuntimeStatus.Running
      check liveRuntime.selectedControlProtocol() == TsnetControlProtocolNimQuic

      let statusPayload = liveRuntime.statusPayload()
      check statusPayload.isOk()
      check statusPayload.get()["backendState"].getStr() == "Mapped"
      check statusPayload.get()["tailnetPath"].getStr() == "relay"
      check statusPayload.get()["controlProtocolSelected"].getStr() == TsnetControlProtocolNimQuic
      check statusPayload.get()["controlEndpoint"].getStr().startsWith("quic://")
      check statusPayload.get()["tailnetRelay"].getStr() == "sin"

      let pingPayload = liveRuntime.pingPayload(newJObject())
      check pingPayload.isOk()
      check pingPayload.get()["ok"].getBool()

      let derpMapPayload = liveRuntime.derpMapPayload()
      check derpMapPayload.isOk()
      check derpMapPayload.get()["regions"][0]["regionCode"].getStr() == "sin"

    test "runtime reaches running state over open nim_quic without auth key":
      let (gateway, controlUrl) = startGateway(openNetwork = true)
      defer:
        gateway.stop()

      let dir = tempRuntimeDir("runtime-open")
      let cfg = TsnetProviderConfig(
        controlUrl: controlUrl,
        controlProtocol: TsnetControlProtocolNimQuic,
        authKey: "",
        hostname: "nim-runtime-open",
        stateDir: dir,
        wireguardPort: 41641,
        bridgeLibraryPath: "",
        logLevel: "",
        enableDebug: false,
        bridgeExtraJson: ""
      )

      let liveRuntime = TsnetInAppRuntime.new(cfg)
      let started = liveRuntime.start()
      check started.isOk()
      check liveRuntime.ready()
      let statusPayload = liveRuntime.statusPayload()
      check statusPayload.isOk()
      check statusPayload.get()["backendState"].getStr() == "Mapped"
      check statusPayload.get()["tailnetIPs"][0].getStr().startsWith("100.64.")
      check statusPayload.get()["controlProtocolSelected"].getStr() == TsnetControlProtocolNimQuic

    test "runtime auto selects nim_quic without protocol fallback":
      let (gateway, controlUrl) = startGateway(openNetwork = true)
      defer:
        gateway.stop()

      let dir = tempRuntimeDir("runtime-auto")
      let cfg = TsnetProviderConfig(
        controlUrl: controlUrl,
        controlProtocol: TsnetControlProtocolAuto,
        authKey: "",
        hostname: "nim-runtime-auto",
        stateDir: dir,
        wireguardPort: 41641,
        bridgeLibraryPath: "",
        logLevel: "",
        enableDebug: false,
        bridgeExtraJson: ""
      )

      let liveRuntime = TsnetInAppRuntime.new(cfg)
      let started = liveRuntime.start()
      check started.isOk()
      check liveRuntime.ready()
      check liveRuntime.selectedControlProtocol() == TsnetControlProtocolNimQuic
      let statusPayload = liveRuntime.statusPayload()
      check statusPayload.isOk()
      check statusPayload.get()["controlFallback"].getStr() == ""

    test "runtime honors explicit nim_quic control endpoint override":
      let (gateway, controlUrl) = startGateway()
      defer:
        gateway.stop()

      let dir = tempRuntimeDir("runtime-override")
      let cfg = TsnetProviderConfig(
        controlUrl: "https://headscale.invalid",
        controlProtocol: TsnetControlProtocolNimQuic,
        controlEndpoint: controlUrl,
        authKey: "tskey-test",
        hostname: "nim-runtime-override",
        stateDir: dir,
        wireguardPort: 41641,
        bridgeLibraryPath: "",
        logLevel: "",
        enableDebug: false,
        bridgeExtraJson: ""
      )

      let liveRuntime = TsnetInAppRuntime.new(cfg)
      let started = liveRuntime.start()
      check started.isOk()
      check liveRuntime.ready()
      let statusPayload = liveRuntime.statusPayload()
      check statusPayload.isOk()
      check statusPayload.get()["controlProtocolSelected"].getStr() == TsnetControlProtocolNimQuic
      check statusPayload.get()["controlEndpoint"].getStr().startsWith("quic://localhost:")
      check statusPayload.get()["controlFallback"].getStr() == ""

    test "runtime reaches running state over nim_quic with explicit IP endpoint":
      let (gateway, controlUrl) = startGateway()
      defer:
        gateway.stop()

      let ipControlUrl = controlUrl.replace("localhost", "127.0.0.1")
      let dir = tempRuntimeDir("runtime-ip-endpoint")
      let cfg = TsnetProviderConfig(
        controlUrl: "https://headscale.invalid",
        controlProtocol: TsnetControlProtocolNimQuic,
        controlEndpoint: ipControlUrl,
        authKey: "tskey-test",
        hostname: "nim-runtime-ip-endpoint",
        stateDir: dir,
        wireguardPort: 41641,
        bridgeLibraryPath: "",
        logLevel: "",
        enableDebug: false,
        bridgeExtraJson: ""
      )

      let liveRuntime = TsnetInAppRuntime.new(cfg)
      let started = liveRuntime.start()
      check started.isOk()
      check liveRuntime.ready()
      let statusPayload = liveRuntime.statusPayload()
      check statusPayload.isOk()
      check statusPayload.get()["controlProtocolSelected"].getStr() == TsnetControlProtocolNimQuic
      check statusPayload.get()["controlEndpoint"].getStr().startsWith("quic://127.0.0.1:")
      check statusPayload.get()["controlFallback"].getStr() == ""

else:
  import unittest2

  suite "Tsnet control-over-QUIC":
    test "msquic runtime unavailable":
      skip()
