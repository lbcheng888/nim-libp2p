{.used.}

when defined(libp2p_msquic_experimental):
  import std/[json, nativesockets, options, os, sequtils, strutils, times]
  import chronos
  import unittest2

  import ../libp2p/multiaddress
  import ../libp2p/transports/[
    tsnet/control,
    tsnet/proxy,
    tsnet/quiccontrol,
    tsnet/quicrelay,
    tsnet/runtime,
    tsnetprovidertypes,
  ]
  import ../libp2p/utility
  import ../libp2p/wire
  import ./utils/async_tests

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
    let path = getTempDir() / ("nim-libp2p-tsnet-quicrelay-" & tag & "-" & $epochTime())
    createDir(path)
    path

  proc mockPairTransport(): TsnetControlTransport =
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
        let hostinfo = request{"Hostinfo"}
        let hostname =
          if hostinfo.isNil: ""
          else: hostinfo{"Hostname"}.getStr().strip()
        if hostname notin ["relay-server", "relay-client"]:
          return err("unexpected hostname")
        ok(%*{
          "MachineAuthorized": true,
          "User": {"LoginName": hostname, "DisplayName": hostname},
          "Login": {"LoginName": hostname & "@example.com"}
        })
    let mapProc: TsnetControlMapPollProc =
      proc(controlUrl: string, request: JsonNode): Result[JsonNode, string] {.closure, gcsafe, raises: [].} =
        let hostinfo = request{"Hostinfo"}
        let hostname =
          if hostinfo.isNil: ""
          else: hostinfo{"Hostname"}.getStr().strip()
        let (nodeId, ip4, peerHostname, peerId, peerIp4) =
          if hostname == "relay-client":
            (2, "100.64.0.11", "relay-server", 1, "100.64.0.10")
          else:
            (1, "100.64.0.10", "relay-client", 2, "100.64.0.11")
        ok(%*{
          "MapSessionHandle": hostname & "-session",
          "Seq": 1,
          "Node": {
            "ID": nodeId,
            "StableID": hostname & "-stable",
            "Name": hostname,
            "HomeDERP": 901,
            "Addresses": [ip4 & "/32"],
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
                  "HostName": "127.0.0.1",
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

  proc startControlGateway(): tuple[gateway: TsnetQuicControlGateway, controlUrl: string] =
    let gateway = TsnetQuicControlGateway.new(TsnetQuicControlGatewayConfig(
      listenHost: "127.0.0.1",
      listenPort: 0'u16,
      controlUrl: "https://headscale.invalid",
      upstreamTransport: mockPairTransport(),
      certificatePem: TestCertificate,
      privateKeyPem: TestPrivateKey
    ))
    let started = gateway.start()
    check started.isOk()
    check gateway.boundPort() > 0'u16
    waitFor sleepAsync(100)
    (gateway, "https://localhost:" & $gateway.boundPort())

  proc startRelayGateway(): tuple[gateway: TsnetQuicRelayGateway, relayUrl: string] =
    let gateway = TsnetQuicRelayGateway.new()
    let started = gateway.start("127.0.0.1", 0'u16, TestCertificate, TestPrivateKey)
    check started.isOk()
    check gateway.boundPort > 0'u16
    waitFor sleepAsync(100)
    (gateway, "quic://127.0.0.1:" & $gateway.boundPort)

  proc buildRuntime(
      controlUrl: string,
      relayUrl: string,
      hostname: string,
      stateDir: string,
      bridgeExtraJson = ""
  ): TsnetInAppRuntime =
    TsnetInAppRuntime.new(TsnetProviderConfig(
      controlUrl: controlUrl,
      controlProtocol: TsnetControlProtocolNimQuic,
      relayEndpoint: relayUrl,
      authKey: "tskey-test",
      hostname: hostname,
      stateDir: stateDir,
      wireguardPort: 41641,
      bridgeLibraryPath: "",
      logLevel: "",
      enableDebug: false,
      bridgeExtraJson: bridgeExtraJson
    ))

  proc runtimeReadySafe(runtime: TsnetInAppRuntime): bool {.gcsafe, raises: [].} =
    {.cast(gcsafe).}:
      runtime.ready()

  proc startRuntimeSafe(runtime: TsnetInAppRuntime): Result[void, string] {.gcsafe, raises: [].} =
    {.cast(gcsafe).}:
      runtime.start()

  proc stopRuntimeSafe(runtime: TsnetInAppRuntime) {.gcsafe, raises: [].} =
    {.cast(gcsafe).}:
      runtime.stop()

  proc listenTcpProxySafe(
      runtime: TsnetInAppRuntime,
      family: string,
      port: int,
      raw: MultiAddress
  ): Result[MultiAddress, string] {.gcsafe, raises: [].} =
    {.cast(gcsafe).}:
      runtime.listenTcpProxy(family, port, raw)

  proc dialTcpProxySafe(
      runtime: TsnetInAppRuntime,
      family: string,
      ip: string,
      port: int
  ): Result[MultiAddress, string] {.gcsafe, raises: [].} =
    {.cast(gcsafe).}:
      runtime.dialTcpProxy(family, ip, port)

  proc unregisterProxyRoutesSafe(ownerId: int) {.gcsafe, raises: [].} =
    {.cast(gcsafe).}:
      unregisterProxyRoutes(ownerId)

  proc closeTransportSafe(transp: StreamTransport) {.async: (raises: []).} =
    if transp.isNil:
      return
    try:
      await transp.closeWait()
    except CatchableError:
      discard

  proc readChunk(transp: StreamTransport, maxBytes: int): Future[seq[byte]] {.async: (raises: []).} =
    var buf = newSeq[byte](maxBytes)
    let read =
      try:
        await transp.readOnce(addr buf[0], buf.len)
      except CatchableError as exc:
        raiseAssert "test relay readChunk failed: " & exc.msg
    if read <= 0:
      return @[]
    buf.setLen(read)
    buf

  proc bytesToString(data: seq[byte]): string =
    result = newString(data.len)
    if data.len > 0:
      copyMem(addr result[0], unsafeAddr data[0], data.len)

  proc probeDialViaAcceptStream(
      relayUrl: string,
      route: string,
      listenerCandidates: seq[string],
      dialerCandidates: seq[string]
  ): Future[TsnetQuicRelayCandidateExchangeResult] {.async: (raises: [CancelledError]).} =
    return await probeAcceptStreamReuse(
      relayUrl,
      route,
      listenerCandidates,
      dialerCandidates
    )

  proc startEchoServer(host: string): Future[(StreamServer, MultiAddress)] {.async: (raises: []).} =
    let bindAddr =
      try:
        initTAddress(host, Port(0))
      except CatchableError as exc:
        raiseAssert "test relay initTAddress failed: " & exc.msg

    proc handleClient(server: StreamServer, client: StreamTransport) {.async: (raises: []).} =
      try:
        let body = await readChunk(client, 4096)
        discard await client.write("echo:" & bytesToString(body))
      except CatchableError:
        discard
      finally:
        await closeTransportSafe(client)
        try:
          server.stop()
        except CatchableError:
          discard
        try:
          server.close()
        except CatchableError:
          discard

    let server =
      try:
        createStreamServer(bindAddr, handleClient, {ReuseAddr})
      except CatchableError as exc:
        raiseAssert "test relay createStreamServer failed: " & exc.msg
    try:
      server.start()
      let ma = MultiAddress.init(server.sock.getLocalAddress()).tryGet()
      (server, ma)
    except CatchableError as exc:
      try:
        server.stop()
      except CatchableError:
        discard
      try:
        server.close()
      except CatchableError:
        discard
      raiseAssert "test relay start server failed: " & exc.msg

  proc startUdpEchoServer(host: string): Future[(DatagramTransport, MultiAddress)] {.async: (raises: []).} =
    let bindAddr =
      try:
        initTAddress(host, Port(0))
      except CatchableError as exc:
        raiseAssert "test relay init udp bind failed: " & exc.msg

    proc handlePacket(
        transp: DatagramTransport,
        remote: TransportAddress
    ): Future[void] {.async: (raises: []).} =
      try:
        let body = transp.getMessage()
        await transp.sendTo(remote, "echo:" & bytesToString(body))
      except CatchableError:
        discard

    let server =
      try:
        newDatagramTransport(
          handlePacket,
          local = bindAddr,
          flags = {ServerFlags.ReuseAddr}
        )
      except CatchableError as exc:
        raiseAssert "test relay create udp echo failed: " & exc.msg

    try:
      var ma = MultiAddress.init(server.localAddress(), IPPROTO_UDP).tryGet()
      let quicSuffix = MultiAddress.init("/quic-v1").tryGet()
      ma = concat(ma, quicSuffix).tryGet()
      (server, ma)
    except CatchableError as exc:
      await server.closeWait()
      raiseAssert "test relay start udp echo failed: " & exc.msg

  proc stopDatagram(transp: DatagramTransport): Future[void] {.async: (raises: []).} =
    if transp.isNil:
      return
    try:
      transp.close()
    except CatchableError:
      discard
    try:
      await transp.closeWait()
    except CatchableError:
      discard

  proc stopServer(server: StreamServer): Future[void] {.async: (raises: []).} =
    if server.isNil:
      return
    try:
      server.stop()
      server.close()
      await server.join()
    except CatchableError:
      discard

  proc waitForProxyListenersReady(runtime: TsnetInAppRuntime, timeoutMs = 5000): bool =
    let maxPolls = max(1, timeoutMs div 100)
    for _ in 0..<maxPolls:
      let status = runtime.statusPayload()
      if status.isOk() and status.get().hasKey("proxyListenersReady") and
          status.get()["proxyListenersReady"].getBool():
        return true
      waitFor sleepAsync(100)
    false

  proc waitForPunchedDirectRoute(
      runtime: TsnetInAppRuntime,
      family, ip: string,
      port: int,
      timeoutMs = 5000
  ): Result[MultiAddress, string] =
    let maxPolls = max(1, timeoutMs div 100)
    var lastError = "punched_direct route not ready"
    for _ in 0..<maxPolls:
      let dial = runtime.dialUdpProxy(family, ip, port)
      if dial.isOk():
        let status = runtime.statusPayload()
        if status.isOk() and status.get()["tailnetPath"].getStr() == "punched_direct":
          return dial
      else:
        lastError = dial.error
      waitFor sleepAsync(100)
    err(lastError)

  proc waitForUdpDialReady(
      runtime: TsnetInAppRuntime,
      raw: MultiAddress,
      timeoutMs = 5000
  ): Result[void, string] =
    let maxPolls = max(1, timeoutMs div 100)
    var lastError = "udp relay dial not ready"
    for _ in 0..<maxPolls:
      let state = runtime.udpDialState(raw)
      if state.known:
        if state.ready:
          return ok()
        if state.error.len > 0:
          lastError = state.error
      waitFor sleepAsync(100)
    err(lastError)

  suite "Tsnet QUIC relay":
    test "minimal listener registration probe succeeds":
      let (gateway, relayUrl) = startRelayGateway()
      defer:
        gateway.stop()

      let probe =
        waitFor probeListenerRegister(
          relayUrl,
          "/ip4/100.64.0.10/udp/4001/quic-v1/tsnet"
        )
      if not probe.ok:
        checkpoint("listener probe error: " & probe.error)
      check probe.ok
      check probe.connected
      check probe.acknowledged

    test "minimal relay ping probe succeeds":
      let (gateway, relayUrl) = startRelayGateway()
      defer:
        gateway.stop()

      let probe = waitFor probeRelayPing(relayUrl)
      if not probe.ok:
        checkpoint("relay ping error: " & probe.error)
      check probe.ok
      check probe.connected
      check probe.acknowledged

    test "gateway exchanges listener and dialer candidates":
      let (gateway, relayUrl) = startRelayGateway()
      defer:
        gateway.stop()

      let listenerCandidates = @[
        "/ip4/198.51.100.10/udp/4001/quic-v1",
        "/ip6/2001:db8::10/udp/4001/quic-v1"
      ]
      let dialerCandidates = @[
        "/ip4/203.0.113.20/udp/4555/quic-v1"
      ]
      let exchange =
        waitFor probeDialCandidateExchange(
          relayUrl,
          "/ip4/100.64.0.10/udp/4001/quic-v1/tsnet",
          listenerCandidates,
          dialerCandidates
        )
      check exchange.ok
      check exchange.listenerCandidates == listenerCandidates
      check exchange.dialerCandidates == dialerCandidates

    test "accept stream can receive incoming over the same client initiated stream":
      let (gateway, relayUrl) = startRelayGateway()
      defer:
        gateway.stop()

      let listenerCandidates = @[
        "/ip4/198.51.100.10/udp/4001/quic-v1"
      ]
      let dialerCandidates = @[
        "/ip4/203.0.113.20/udp/4555/quic-v1"
      ]
      let exchange =
        waitFor probeDialViaAcceptStream(
          relayUrl,
          "/ip4/100.64.0.10/udp/4001/quic-v1/tsnet",
          listenerCandidates,
          dialerCandidates
        )
      check exchange.ok
      check exchange.listenerCandidates == listenerCandidates
      check exchange.dialerCandidates == dialerCandidates

    test "accept stream stays healthy while same connection route_status runs concurrently":
      let (gateway, relayUrl) = startRelayGateway()
      defer:
        gateway.stop()

      let listenerCandidates = @[
        "/ip4/198.51.100.10/udp/4001/quic-v1"
      ]
      let dialerCandidates = @[
        "/ip4/203.0.113.30/udp/4556/quic-v1"
      ]
      let exchange =
        waitFor probeAcceptStreamRouteStatusReuse(
          relayUrl,
          "/ip4/100.64.0.20/udp/4001/quic-v1/tsnet",
          listenerCandidates,
          dialerCandidates
        )
      if not exchange.ok:
        checkpoint("accept+route_status probe error: " & exchange.error)
      check exchange.ok
      check exchange.listenerCandidates == listenerCandidates
      check exchange.dialerCandidates == dialerCandidates

    test "udp bridge session binds exactly one client source":
      let target = initTAddress("127.0.0.1", Port(40111))
      let firstClient = initTAddress("127.0.0.1", Port(50199))
      let secondClient = initTAddress("127.0.0.1", Port(50200))

      let listenerDecision = udpBridgeAcceptsSource(
        target,
        none(TransportAddress),
        target,
        true
      )
      check listenerDecision.accept
      check not listenerDecision.learnClient

      let firstDialDecision = udpBridgeAcceptsSource(
        target,
        none(TransportAddress),
        firstClient,
        false
      )
      check firstDialDecision.accept
      check firstDialDecision.learnClient

      let repeatDialDecision = udpBridgeAcceptsSource(
        target,
        some(firstClient),
        firstClient,
        false
      )
      check repeatDialDecision.accept
      check not repeatDialDecision.learnClient

      let wrongDialDecision = udpBridgeAcceptsSource(
        target,
        some(firstClient),
        secondClient,
        false
      )
      check not wrongDialDecision.accept
      check not wrongDialDecision.learnClient

    test "relay candidates include public and lan hints from bridgeExtraJson":
      let candidates = relayCandidatesForRaw(
        "/ip4/127.0.0.1/udp/4001/quic-v1",
        $ %*{
          "publicIpv4": "198.51.100.50",
          "publicIpv6": "2001:db8::50",
          "directCandidates": [
            "/ip4/203.0.113.77/udp/4100/quic-v1",
            "/ip4/203.0.113.77/udp/4100/quic-v1/p2p/12D3KooWTest"
          ],
          "lanEndpoints": {
            "endpoints": [
              {
                "multiaddrs": [
                  "/ip4/192.168.1.10/udp/4001/quic-v1",
                  "/ip4/127.0.0.1/udp/4001/quic-v1"
                ]
              }
            ]
          }
        }
      )
      check candidates == @[
        "/ip4/198.51.100.50/udp/4001/quic-v1",
        "/ip6/2001:db8::50/udp/4001/quic-v1",
        "/ip4/203.0.113.77/udp/4100/quic-v1",
        "/ip4/192.168.1.10/udp/4001/quic-v1"
      ]

    test "candidate exchange can promote punched_direct udp path":
      let (gateway, relayUrl) = startRelayGateway()
      defer:
        gateway.stop()

      let listenerCandidates = @[
        "/ip4/198.51.100.10/udp/4001/quic-v1",
        "/ip4/203.0.113.10/udp/4002/quic-v1"
      ]
      let dialerCandidates = @[
        "/ip4/203.0.113.20/udp/4555/quic-v1"
      ]
      let exchange =
        waitFor probeDialCandidateExchange(
          relayUrl,
          "/ip4/100.64.0.10/udp/4001/quic-v1/tsnet",
          listenerCandidates,
          dialerCandidates
        )
      check exchange.ok

      let runtime = TsnetInAppRuntime.new(
        TsnetProviderConfig(
          controlUrl: "http://headscale.local",
          authKey: "tskey-client",
          hostname: "relay-client",
          stateDir: tempRuntimeDir("punched-direct"),
          wireguardPort: 41642,
          bridgeLibraryPath: "",
          logLevel: "",
          enableDebug: false,
          bridgeExtraJson: ""
        ),
        mockPairTransport()
      )
      check runtime.start().isOk()
      let advertised = MultiAddress.init("/ip4/100.64.0.10/udp/4001/quic-v1/tsnet").tryGet()
      let directCandidate = MultiAddress.init(exchange.listenerCandidates[0]).tryGet()
      let backupCandidate = MultiAddress.init(exchange.listenerCandidates[1]).tryGet()
      check runtime.registerDirectProxyRoute(advertised, directCandidate, punched = true).isOk()
      check runtime.registerDirectProxyRoute(advertised, backupCandidate, punched = true).isOk()

      let udpDial = runtime.dialUdpProxy("ip4", "100.64.0.10", 4001)
      check udpDial.isOk()
      check udpDial.get() == directCandidate

      let status = runtime.statusPayload()
      check status.isOk()
      check status.get()["tailnetPath"].getStr() == "punched_direct"
      check status.get()["tailnetRelay"].getStr() == ""
      check status.get()["tailnetPrimaryPath"].getStr() == "punched_direct"
      check status.get()["tailnetMultipath"].getBool()
      check status.get()["tailnetPunchedRouteCount"].getInt() == 1
      check status.get()["tailnetDirectRouteCount"].getInt() == 1

      let udpDialSettled = runtime.dialUdpProxy("ip4", "100.64.0.10", 4001)
      check udpDialSettled.isOk()
      check udpDialSettled.get() == directCandidate

      let settledStatus = runtime.statusPayload()
      check settledStatus.isOk()
      check settledStatus.get()["tailnetPath"].getStr() == "direct"
      check settledStatus.get()["tailnetMultipath"].getBool()
      check settledStatus.get()["tailnetDirectRouteCount"].getInt() == 1
      check settledStatus.get()["tailnetPunchedRouteCount"].getInt() == 1

      runtime.stop()

    test "live runtimes keep relay path when listener only advertises loopback candidate":
      let (controlGateway, controlUrl) = startControlGateway()
      let (relayGateway, relayUrl) = startRelayGateway()
      let server = buildRuntime(controlUrl, relayUrl, "relay-server", tempRuntimeDir("udp-server"))
      let client = buildRuntime(controlUrl, relayUrl, "relay-client", tempRuntimeDir("udp-client"))

      try:
        check startRuntimeSafe(server).isOk()
        check startRuntimeSafe(client).isOk()
        check runtimeReadySafe(server)
        check runtimeReadySafe(client)

        let rawServer = MultiAddress.init("/ip4/127.0.0.1/udp/41002/quic-v1").tryGet()
        let advertised = server.listenUdpProxy("ip4", 4001, rawServer)
        check advertised.isOk()
        check waitForProxyListenersReady(server)

        unregisterProxyRoutesSafe(server.runtimeId)

        let firstDial = client.dialUdpProxy("ip4", "100.64.0.10", 4001)
        check firstDial.isOk()
        check $firstDial.get() != $rawServer
        let firstDialState = client.udpDialState(firstDial.get())
        check firstDialState.known
        check not firstDialState.ready
        check firstDialState.error.len == 0
        check firstDialState.phase.len > 0
        let relayStatus = client.statusPayload()
        check relayStatus.isOk()
        check relayStatus.get()["tailnetPath"].getStr() == "relay"
        check relayStatus.get().hasKey("udpDialStates")
        check relayStatus.get()["udpDialStates"].kind == JArray
        check relayStatus.get()["udpDialStates"].len >= 1
        let followupDial = client.dialUdpProxy("ip4", "100.64.0.10", 4001)
        check followupDial.isOk()
        check $followupDial.get() != $rawServer

        let settledRelayStatus = client.statusPayload()
        check settledRelayStatus.isOk()
        check settledRelayStatus.get()["tailnetPath"].getStr() == "relay"
        check settledRelayStatus.get()["tailnetRelay"].getStr() == "sin"
      finally:
        stopRuntimeSafe(client)
        stopRuntimeSafe(server)
        relayGateway.stop()
        controlGateway.stop()

    test "live runtimes exchange public candidates and listener learns punched_direct backup":
      let (controlGateway, controlUrl) = startControlGateway()
      let (relayGateway, relayUrl) = startRelayGateway()
      let server = buildRuntime(
        controlUrl,
        relayUrl,
        "relay-server",
        tempRuntimeDir("udp-public-server"),
        $ %*{"publicIpv4": "198.51.100.10"}
      )
      let client = buildRuntime(
        controlUrl,
        relayUrl,
        "relay-client",
        tempRuntimeDir("udp-public-client"),
        $ %*{"publicIpv4": "203.0.113.20"}
      )

      proc waitForDirectRouteCount(runtime: TsnetInAppRuntime, expected: int, timeoutMs = 5000): bool =
        let maxPolls = max(1, timeoutMs div 100)
        for _ in 0..<maxPolls:
          if directRouteCount(runtime.runtimeId) >= expected:
            return true
          waitFor sleepAsync(100)
        false

      try:
        check startRuntimeSafe(server).isOk()
        check startRuntimeSafe(client).isOk()
        check runtimeReadySafe(server)
        check runtimeReadySafe(client)

        let rawServer = MultiAddress.init("/ip4/127.0.0.1/udp/41012/quic-v1").tryGet()
        let advertised = server.listenUdpProxy("ip4", 4001, rawServer)
        check advertised.isOk()
        check waitForProxyListenersReady(server)

        unregisterProxyRoutesSafe(server.runtimeId)

        let firstDial = client.dialUdpProxy("ip4", "100.64.0.10", 4001)
        check firstDial.isOk()
        let promotedDial = waitForPunchedDirectRoute(client, "ip4", "100.64.0.10", 4001)
        check promotedDial.isOk()
        check $promotedDial.get() == "/ip4/198.51.100.10/udp/41012/quic-v1"
        check waitForDirectRouteCount(client, 1)
        check waitForDirectRouteCount(server, 1)

        let clientStatus = client.statusPayload()
        check clientStatus.isOk()
        check clientStatus.get()["tailnetPath"].getStr() == "punched_direct"
        check clientStatus.get()["tailnetDirectRouteCount"].getInt() == 1
        check clientStatus.get()["tailnetPunchedRouteCount"].getInt() == 0

        let serverStatus = server.statusPayload()
        check serverStatus.isOk()
        check serverStatus.get()["tailnetPath"].getStr() == "relay"
        check serverStatus.get()["tailnetDirectRouteCount"].getInt() == 0
        check serverStatus.get()["tailnetPunchedRouteCount"].getInt() == 1
        check serverStatus.get()["tailnetMultipath"].getBool()
      finally:
        stopRuntimeSafe(client)
        stopRuntimeSafe(server)
        relayGateway.stop()
        controlGateway.stop()

    test "live runtimes bridge a tcp proxy route over quic relay":
      let (controlGateway, controlUrl) = startControlGateway()
      let (relayGateway, relayUrl) = startRelayGateway()
      let server = buildRuntime(controlUrl, relayUrl, "relay-server", tempRuntimeDir("server"))
      let client = buildRuntime(controlUrl, relayUrl, "relay-client", tempRuntimeDir("client"))

      try:
        let startedServer = startRuntimeSafe(server)
        if startedServer.isErr():
          checkpoint("server start failed: " & startedServer.error)
        check startedServer.isOk()
        let startedClient = startRuntimeSafe(client)
        if startedClient.isErr():
          checkpoint("client start failed: " & startedClient.error)
        check startedClient.isOk()
        check runtimeReadySafe(server)
        check runtimeReadySafe(client)

        let serverStatus = server.statusPayload()
        if serverStatus.isErr():
          checkpoint("server status failed: " & serverStatus.error)
        check serverStatus.isOk()
        if serverStatus.isOk():
          check serverStatus.get()["tailnetPath"].getStr() == "relay"
        let clientStatus = client.statusPayload()
        if clientStatus.isErr():
          checkpoint("client status failed: " & clientStatus.error)
        check clientStatus.isOk()
        if clientStatus.isOk():
          check clientStatus.get()["tailnetPath"].getStr() == "relay"

        let (echoServer, rawServer) = waitFor startEchoServer("127.0.0.1")
        try:
          let advertised = listenTcpProxySafe(server, "ip4", 4001, rawServer)
          check advertised.isOk()
          check $advertised.get() == "/ip4/100.64.0.10/tcp/4001/tsnet"
          check waitForProxyListenersReady(server)

          unregisterProxyRoutesSafe(server.runtimeId)

          let rawDial = dialTcpProxySafe(client, "ip4", "100.64.0.10", 4001)
          check rawDial.isOk()
          check $rawDial.get() != $rawServer

          let conn = waitFor connect(rawDial.get())
          discard waitFor conn.write("hello-quic-relay")
          let echoed = waitFor readChunk(conn, 64)
          check bytesToString(echoed) == "echo:hello-quic-relay"
          waitFor closeTransportSafe(conn)
        finally:
          waitFor stopServer(echoServer)
      finally:
        stopRuntimeSafe(client)
        stopRuntimeSafe(server)
        relayGateway.stop()
        controlGateway.stop()

    test "live runtimes bridge repeated udp datagrams over quic relay":
      let (controlGateway, controlUrl) = startControlGateway()
      let (relayGateway, relayUrl) = startRelayGateway()
      let server = buildRuntime(controlUrl, relayUrl, "relay-server", tempRuntimeDir("udp-burst-server"))
      let client = buildRuntime(controlUrl, relayUrl, "relay-client", tempRuntimeDir("udp-burst-client"))

      try:
        check startRuntimeSafe(server).isOk()
        check startRuntimeSafe(client).isOk()
        check runtimeReadySafe(server)
        check runtimeReadySafe(client)

        let (udpEcho, rawServer) = waitFor startUdpEchoServer("127.0.0.1")
        defer:
          waitFor stopDatagram(udpEcho)

        let advertised = server.listenUdpProxy("ip4", 4001, rawServer)
        check advertised.isOk()
        check waitForProxyListenersReady(server)

        unregisterProxyRoutesSafe(server.runtimeId)

        let rawDial = client.dialUdpProxy("ip4", "100.64.0.10", 4001)
        check rawDial.isOk()
        check $rawDial.get() != $rawServer
        check waitForUdpDialReady(client, rawDial.get()).isOk()

        var received: seq[string] = @[]
        let expected = @["echo:ping-1", "echo:ping-2", "echo:ping-3", "echo:ping-4"]
        let remote = initTAddress(rawDial.get()).tryGet()

        proc handleClientPacket(
            transp: DatagramTransport,
            peer: TransportAddress
        ): Future[void] {.async: (raises: []).} =
          try:
            let payload = transp.getMessage()
            received.add(bytesToString(payload))
          except CatchableError:
            discard

        let udpClient =
          try:
            newDatagramTransport(
              handleClientPacket,
              local = initTAddress("127.0.0.1", Port(0)),
              flags = {ServerFlags.ReuseAddr}
            )
          except CatchableError as exc:
            raiseAssert "test relay create udp client failed: " & exc.msg
        defer:
          waitFor stopDatagram(udpClient)

        for payload in ["ping-1", "ping-2", "ping-3", "ping-4"]:
          waitFor udpClient.sendTo(remote, payload)

        for _ in 0..<50:
          if received.len >= expected.len:
            break
          waitFor sleepAsync(100)

        check received == expected
      finally:
        stopRuntimeSafe(client)
        stopRuntimeSafe(server)
        relayGateway.stop()
        controlGateway.stop()

else:
  import unittest2

  suite "Tsnet QUIC relay":
    test "msquic runtime unavailable":
      skip()
