{.used.}

import std/[json, os, sequtils, strutils, tables, times]

import chronos
import stew/byteutils

import ../libp2p/multiaddress
import ../libp2p/transports/[tsnet/control, tsnet/runtime, tsnet/proxy, tsnetprovidertypes]
import ../libp2p/utility
import ../libp2p/wire
import ./helpers

type
  RelayGateway = ref object
    server: StreamServer
    listeners: Table[string, StreamTransport]

proc tempRuntimeDir(tag: string): string =
  let path = getTempDir() / ("nim-libp2p-tsnet-relay-" & tag & "-" & $epochTime())
  createDir(path)
  path

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

proc bytesToString(bytes: openArray[byte]): string =
  result = newString(bytes.len)
  for i, value in bytes:
    result[i] = char(value)

proc readExact(transp: StreamTransport, size: int): Future[string] {.async: (raises: []).} =
  var buf = newSeq[byte](size)
  try:
    if size > 0:
      await transp.readExactly(addr buf[0], size)
  except CatchableError as exc:
    raiseAssert "test relay readExact failed: " & exc.msg
  bytesToString(buf)

proc readFrame(transp: StreamTransport): Future[string] {.async: (raises: []).} =
  let header = await readExact(transp, 4)
  let size = decodeFrameSize(header)
  doAssert size >= 0
  if size == 0:
    return ""
  await readExact(transp, size)

proc writeFrame(transp: StreamTransport, payload: string): Future[void] {.async: (raises: []).} =
  try:
    discard await transp.write(encodeFrame(payload))
  except CatchableError as exc:
    raiseAssert "test relay writeFrame failed: " & exc.msg

proc closeTransport(transp: StreamTransport) {.async: (raises: []).} =
  if transp.isNil:
    return
  try:
    await transp.closeWait()
  except CatchableError:
    discard

proc bridgePump(src, dst: StreamTransport): Future[void] {.async: (raises: [CancelledError]).} =
  var buf = newSeq[byte](64 * 1024)
  while true:
    let read =
      try:
        await src.readOnce(addr buf[0], buf.len)
      except CatchableError:
        break
    if read <= 0:
      break
    try:
      discard await dst.write(buf[0 ..< read])
    except CatchableError:
      break

proc readChunk(transp: StreamTransport, maxBytes: int): Future[seq[byte]] {.async: (raises: []).} =
  var buf = newSeq[byte](maxBytes)
  try:
    let read = await transp.readOnce(addr buf[0], buf.len)
    if read <= 0:
      return @[]
    buf.setLen(read)
    return buf
  except CatchableError as exc:
    raiseAssert "test relay readChunk failed: " & exc.msg

proc bridgeTransports(a, b: StreamTransport): Future[void] {.async: (raises: []).} =
  let f1 = bridgePump(a, b)
  let f2 = bridgePump(b, a)
  try:
    discard await one(@[f1, f2])
  except CatchableError:
    discard
  var cancels: seq[Future[void]] = @[]
  if not f1.finished():
    cancels.add(f1.cancelAndWait())
  if not f2.finished():
    cancels.add(f2.cancelAndWait())
  if cancels.len > 0:
    await noCancel allFutures(cancels)
  await noCancel closeTransport(a)
  await noCancel closeTransport(b)

proc rpcError(message: string): JsonNode =
  %*{"ok": false, "error": message}

proc rpcSuccess(payload: JsonNode): JsonNode =
  %*{"ok": true, "payload": payload}

proc startRelayGateway(host: string, port: int): RelayGateway =
  let gateway = RelayGateway(listeners: initTable[string, StreamTransport]())
  let bindAddr = initTAddress(host, Port(port))

  proc handleClient(server: StreamServer, client: StreamTransport) {.async: (raises: []).} =
    try:
      let request = parseJson(await readFrame(client))
      let mode = request{"mode"}.getStr().strip()
      let route = request{"route"}.getStr().strip()
      case mode
      of "listen":
        let existing = gateway.listeners.getOrDefault(route)
        if not existing.isNil:
          await closeTransport(existing)
        gateway.listeners[route] = client
        await writeFrame(client, $(rpcSuccess(%*{"mode": "listen", "route": route})))
      of "dial":
        let listener = gateway.listeners.getOrDefault(route)
        if listener.isNil:
          await writeFrame(client, $(rpcError("route_not_registered")))
          await closeTransport(client)
          return
        gateway.listeners.del(route)
        let source = request{"source"}.getStr().strip()
        await writeFrame(listener, $(%*{
          "op": "incoming",
          "route": route,
          "source": source
        }))
        let ready = parseJson(await readFrame(listener))
        if not ready{"ok"}.getBool():
          await writeFrame(client, $(rpcError("listener_not_ready")))
          await closeTransport(listener)
          await closeTransport(client)
          return
        await writeFrame(client, $(rpcSuccess(%*{"mode": "dial", "route": route})))
        await bridgeTransports(listener, client)
      else:
        await writeFrame(client, $(rpcError("unsupported_mode")))
        await closeTransport(client)
    except CatchableError:
      await closeTransport(client)

  gateway.server = createStreamServer(bindAddr, handleClient, {ReuseAddr})
  gateway.server.start()
  result = gateway

proc stopRelayGateway(gateway: RelayGateway): Future[void] {.async: (raises: []).} =
  if gateway.isNil:
    return
  for listener in gateway.listeners.values.toSeq():
    await closeTransport(listener)
  gateway.listeners.clear()
  if not gateway.server.isNil:
    try:
      gateway.server.stop()
      gateway.server.close()
      await gateway.server.join()
    except CatchableError:
      discard

proc startEchoServer(host: string): Future[(StreamServer, MultiAddress)] {.async: (raises: []).} =
  let bindAddr =
    try:
      initTAddress(host, Port(0))
    except CatchableError as exc:
      raiseAssert "failed to bind echo server: " & exc.msg

  proc handleClient(server: StreamServer, client: StreamTransport) {.async: (raises: []).} =
    try:
      let body = await readChunk(client, 4096)
      discard await client.write("echo:" & string.fromBytes(body))
      await closeTransport(client)
      server.stop()
      server.close()
    except CatchableError:
      await closeTransport(client)

  let server =
    try:
      let created = createStreamServer(bindAddr, handleClient, {ReuseAddr})
      created.start()
      created
    except CatchableError as exc:
      raiseAssert "failed to start echo server: " & exc.msg
  let ma =
    try:
      MultiAddress.init(server.sock.getLocalAddress()).tryGet()
    except CatchableError as exc:
      raiseAssert "failed to inspect echo server address: " & exc.msg
  (server, ma)

proc stopServer(server: StreamServer): Future[void] {.async: (raises: []).} =
  if server.isNil:
    return
  try:
    server.stop()
    server.close()
    await server.join()
  except CatchableError:
    discard

proc mockMappedTransport(
    hostname: string,
    nodeId: int,
    ip4: string,
    peerHostname: string,
    peerId: int,
    peerIp4: string
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
      if hostinfo{"Hostname"}.getStr() != hostname:
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

proc buildRuntime(
    controlUrl: string,
    hostname: string,
    stateDir: string,
    nodeId: int,
    ip4: string,
    peerHostname: string,
    peerId: int,
    peerIp4: string
): TsnetInAppRuntime =
  TsnetInAppRuntime.new(
    TsnetProviderConfig(
      controlUrl: controlUrl,
      controlProtocol: TsnetControlProtocolNimTcp,
      authKey: "tskey-test",
      hostname: hostname,
      stateDir: stateDir,
      wireguardPort: 41641,
      bridgeLibraryPath: "",
      logLevel: "",
      enableDebug: false,
      bridgeExtraJson: ""
    ),
    mockMappedTransport(hostname, nodeId, ip4, peerHostname, peerId, peerIp4)
  )

proc startRuntimeSafe(runtime: TsnetInAppRuntime): Result[void, string] {.gcsafe, raises: [].} =
  {.cast(gcsafe).}:
    runtime.start()

proc runtimeReadySafe(runtime: TsnetInAppRuntime): bool {.gcsafe, raises: [].} =
  {.cast(gcsafe).}:
    runtime.ready()

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

proc stopRuntimeSafe(runtime: TsnetInAppRuntime) {.gcsafe, raises: [].} =
  {.cast(gcsafe).}:
    runtime.stop()

proc disableLocalControlTransportSafe(runtime: TsnetInAppRuntime) {.gcsafe, raises: [].} =
  {.cast(gcsafe).}:
    runtime.controlTransport = nil

proc unregisterProxyRoutesSafe(ownerId: int) {.gcsafe, raises: [].} =
  {.cast(gcsafe).}:
    unregisterProxyRoutes(ownerId)

suite "Tsnet TCP relay":
  asyncTest "live runtimes bridge a tcp proxy route over custom relay":
    let relayPort = 22080 + int(epochTime()) mod 1000
    let controlUrl = "tcp://127.0.0.1:" & $(relayPort - 1)
    let gateway = startRelayGateway("127.0.0.1", relayPort)

    let server = buildRuntime(
      controlUrl,
      "relay-server",
      tempRuntimeDir("server"),
      1,
      "100.64.0.10",
      "relay-client",
      2,
      "100.64.0.11"
    )
    let client = buildRuntime(
      controlUrl,
      "relay-client",
      tempRuntimeDir("client"),
      2,
      "100.64.0.11",
      "relay-server",
      1,
      "100.64.0.10"
    )

    check startRuntimeSafe(server).isOk()
    check startRuntimeSafe(client).isOk()
    check runtimeReadySafe(server)
    check runtimeReadySafe(client)
    disableLocalControlTransportSafe(server)
    disableLocalControlTransportSafe(client)

    let (echoServer, rawServer) = await startEchoServer("127.0.0.1")
    let advertised = listenTcpProxySafe(server, "ip4", 4001, rawServer)
    check advertised.isOk()
    check $advertised.get() == "/ip4/100.64.0.10/tcp/4001/tsnet"

    unregisterProxyRoutesSafe(server.runtimeId)

    let rawDial = dialTcpProxySafe(client, "ip4", "100.64.0.10", 4001)
    check rawDial.isOk()
    check $rawDial.get() != $rawServer

    let conn = await connect(rawDial.get())
    discard await conn.write("hello")
    let echoed = await readChunk(conn, 64)
    check string.fromBytes(echoed) == "echo:hello"
    await closeTransport(conn)

    stopRuntimeSafe(client)
    stopRuntimeSafe(server)
    await stopServer(echoServer)
    await stopRelayGateway(gateway)

  asyncTest "live runtimes fall back to relay dial when local proxy registry is absent":
    let relayPort = 23080 + int(epochTime()) mod 1000
    let controlUrl = "tcp://127.0.0.1:" & $(relayPort - 1)
    let gateway = startRelayGateway("127.0.0.1", relayPort)

    let server = buildRuntime(
      controlUrl,
      "relay-server-live",
      tempRuntimeDir("server-live"),
      1,
      "100.64.0.10",
      "relay-client-live",
      2,
      "100.64.0.11"
    )
    let client = buildRuntime(
      controlUrl,
      "relay-client-live",
      tempRuntimeDir("client-live"),
      2,
      "100.64.0.11",
      "relay-server-live",
      1,
      "100.64.0.10"
    )

    check startRuntimeSafe(server).isOk()
    check startRuntimeSafe(client).isOk()
    check runtimeReadySafe(server)
    check runtimeReadySafe(client)
    disableLocalControlTransportSafe(server)
    disableLocalControlTransportSafe(client)

    let (echoServer, rawServer) = await startEchoServer("127.0.0.1")
    let advertised = listenTcpProxySafe(server, "ip4", 4001, rawServer)
    check advertised.isOk()
    check $advertised.get() == "/ip4/100.64.0.10/tcp/4001/tsnet"

    unregisterProxyRoutesSafe(server.runtimeId)

    let rawDial = dialTcpProxySafe(client, "ip4", "100.64.0.10", 4001)
    check rawDial.isOk()
    check $rawDial.get() != $rawServer

    let conn = await connect(rawDial.get())
    discard await conn.write("hello-relay")
    let echoed = await readChunk(conn, 64)
    check string.fromBytes(echoed) == "echo:hello-relay"
    await closeTransport(conn)

    stopRuntimeSafe(client)
    stopRuntimeSafe(server)
    await stopServer(echoServer)
    await stopRelayGateway(gateway)
