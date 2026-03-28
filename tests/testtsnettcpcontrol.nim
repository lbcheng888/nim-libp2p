{.used.}

when compileOption("threads"):
  import std/[json, net, os, strutils, times]
  import unittest2

  import ../libp2p/transports/[tsnet/control, tsnet/runtime, tsnet/tcpcontrol, tsnetprovidertypes]
  import ../libp2p/utility

  type
    GatewayThreadArgs = object
      host: string
      port: int
      expectedConnections: int

  proc tempRuntimeDir(tag: string): string =
    let path = getTempDir() / ("nim-libp2p-tsnet-tcp-" & tag & "-" & $epochTime())
    createDir(path)
    path

  proc rpcError(message: string): JsonNode =
    %*{
      "ok": false,
      "error": message
    }

  proc rpcSuccess(payload: JsonNode): JsonNode =
    %*{
      "ok": true,
      "payload":
        if payload.isNil: newJObject()
        else: payload
    }

  proc encodeFrame(payload: string): string =
    let size = payload.len
    result = newString(4 + size)
    result[0] = char((size shr 24) and 0xFF)
    result[1] = char((size shr 16) and 0xFF)
    result[2] = char((size shr 8) and 0xFF)
    result[3] = char(size and 0xFF)
    for i, ch in payload:
      result[4 + i] = ch

  proc decodeFrameSize(header: string): int =
    if header.len != 4:
      return -1
    (ord(header[0]) shl 24) or
    (ord(header[1]) shl 16) or
    (ord(header[2]) shl 8) or
    ord(header[3])

  proc readExact(socket: Socket, size: int): string =
    result = newStringOfCap(size)
    while result.len < size:
      let chunk = socket.recv(size - result.len)
      if chunk.len == 0:
        raise newException(IOError, "socket closed while reading nim_tcp test frame")
      result.add(chunk)

  proc readFrame(socket: Socket): string =
    let header = readExact(socket, 4)
    let size = decodeFrameSize(header)
    if size < 0:
      raise newException(IOError, "invalid nim_tcp test frame header")
    if size == 0:
      return ""
    readExact(socket, size)

  proc writeFrame(socket: Socket, payload: string) =
    socket.send(encodeFrame(payload))

  proc handleRequest(request: JsonNode): JsonNode =
    if request.kind != JObject:
      return rpcError("nim_tcp request must be a JSON object")
    let route =
      if request.hasKey("route"): request["route"].getStr().strip()
      else: ""
    case route
    of "health":
      rpcSuccess(%*{"protocol": "nim_tcp"})
    of "key":
      rpcSuccess(%*{
        "publicKey": "mkey:control",
        "legacyPublicKey": "mkey:legacy"
      })
    of "register":
      let reqNode = request.getOrDefault("request")
      if not reqNode{"NodeKey"}.getStr().startsWith("nodekey:"):
        return rpcError("unexpected NodeKey")
      rpcSuccess(%*{
        "MachineAuthorized": true,
        "User": {"LoginName": "lbcheng", "DisplayName": "LB Cheng"},
        "Login": {"LoginName": "lbcheng@example.com"}
      })
    of "map":
      let reqNode = request.getOrDefault("request")
      if not reqNode{"DiscoKey"}.getStr().startsWith("discokey:"):
        return rpcError("unexpected DiscoKey")
      rpcSuccess(%*{
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
    else:
      rpcError("not_found")

  proc gatewayThreadMain(args: GatewayThreadArgs) {.thread.} =
    let domain =
      if args.host.contains(":"): Domain.AF_INET6
      else: Domain.AF_INET
    let server = newSocket(domain = domain, sockType = SockType.SOCK_STREAM, protocol = Protocol.IPPROTO_TCP)
    server.setSockOpt(OptReuseAddr, true)
    server.bindAddr(Port(args.port), args.host)
    server.listen()
    for _ in 0 ..< args.expectedConnections:
      var client: owned(Socket)
      server.accept(client)
      try:
        let rawRequest = readFrame(client)
        let request = parseJson(rawRequest)
        writeFrame(client, $(handleRequest(request)))
      finally:
        if not client.isNil:
          client.close()
    server.close()

  proc startGateway(expectedConnections: int): tuple[thread: Thread[GatewayThreadArgs], controlUrl: string] =
    let port = 20000 + int(epochTime()) mod 20000
    var thread: Thread[GatewayThreadArgs]
    createThread(thread, gatewayThreadMain, GatewayThreadArgs(
      host: "127.0.0.1",
      port: port,
      expectedConnections: expectedConnections
    ))
    sleep(100)
    (thread, "tcp://127.0.0.1:" & $port)

  suite "Tsnet control-over-TCP":
    test "custom nim_tcp gateway proxies key register and map":
      var gateway = startGateway(4)
      defer:
        joinThread(gateway.thread)

      let fetched = fetchControlServerKeyTcpSafe(gateway.controlUrl)
      check fetched.isOk()
      check fetched.get().publicKey == "mkey:control"

      let registerReq = buildRegisterRequestPayload(
        capabilityVersion = 130,
        nodePublicKey = "nodekey:node",
        machinePublicKey = "mkey:machine",
        hostname = "nim-runtime",
        authKey = "tskey-test"
      )
      let registerRes = registerNodeTcp(gateway.controlUrl, registerReq)
      check registerRes.isOk()
      check registerRes.get()["MachineAuthorized"].getBool()

      let mapReq = buildMapRequestPayload(
        capabilityVersion = 130,
        nodePublicKey = "nodekey:node",
        discoPublicKey = "discokey:wg",
        hostname = "nim-runtime"
      )
      let mapRes = mapPollTcp(gateway.controlUrl, mapReq)
      check mapRes.isOk()
      check mapRes.get()["DERPMap"]["Regions"]["901"]["RegionCode"].getStr() == "sin"

      let healthRes = healthTcp(gateway.controlUrl)
      check healthRes.isOk()
      check healthRes.get()["protocol"].getStr() == "nim_tcp"

    test "runtime reaches running state over nim_tcp":
      var gateway = startGateway(3)
      defer:
        joinThread(gateway.thread)

      let dir = tempRuntimeDir("runtime")
      let cfg = TsnetProviderConfig(
        controlUrl: gateway.controlUrl,
        controlProtocol: TsnetControlProtocolNimTcp,
        authKey: "tskey-test",
        hostname: "nim-runtime-tcp",
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
      check liveRuntime.selectedControlProtocol() == TsnetControlProtocolNimTcp

      let statusPayload = liveRuntime.statusPayload()
      check statusPayload.isOk()
      check statusPayload.get()["backendState"].getStr() == "Mapped"
      check statusPayload.get()["controlProtocolSelected"].getStr() == TsnetControlProtocolNimTcp
      check statusPayload.get()["controlEndpoint"].getStr().startsWith("tcp://127.0.0.1:")
      check statusPayload.get()["tailnetRelay"].getStr() == "sin"

      let pingPayload = liveRuntime.pingPayload(newJObject())
      check pingPayload.isOk()
      check pingPayload.get()["ok"].getBool()

      let derpMapPayload = liveRuntime.derpMapPayload()
      check derpMapPayload.isOk()
      check derpMapPayload.get()["regions"][0]["regionCode"].getStr() == "sin"

    test "runtime honors explicit nim_tcp control endpoint override":
      var gateway = startGateway(3)
      defer:
        joinThread(gateway.thread)

      let dir = tempRuntimeDir("runtime-override")
      let cfg = TsnetProviderConfig(
        controlUrl: "https://headscale.invalid",
        controlProtocol: TsnetControlProtocolNimTcp,
        controlEndpoint: gateway.controlUrl,
        authKey: "tskey-test",
        hostname: "nim-runtime-tcp-override",
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
      check statusPayload.get()["controlProtocolSelected"].getStr() == TsnetControlProtocolNimTcp
      check statusPayload.get()["controlEndpoint"].getStr().startsWith("tcp://127.0.0.1:")
else:
  import unittest2

  suite "Tsnet control-over-TCP":
    test "thread support unavailable":
      skip()
