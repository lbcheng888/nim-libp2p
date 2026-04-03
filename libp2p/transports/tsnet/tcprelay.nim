{.push raises: [].}

import chronos
import std/[json, locks, strutils, tables, uri]

import ../../multiaddress
import ../../utility
import ../../wire
import ./control
import ./proxy

const
  NimTsnetTcpRelayDefaultPort* = 9446'u16
  NimTsnetTcpRelayPrefix* = "/nim-tsnet-relay-tcp/v1"
  RelayFrameBufferSize = 64 * 1024
  RelayDialRetryWindow = 2.seconds
  RelayDialRetryStep = 100.milliseconds

type
  TsnetTcpRelayEndpoint* = object
    url*: string
    host*: string
    authority*: string
    port*: uint16
    prefix*: string

  TsnetRelayTaskHandle = ref object
    routeId: int
    task: Future[void]
    server: StreamServer

var relayRegistryLock {.global.}: Lock
var relayNextRouteId {.global.}: int
var relayOwnerTasks {.global.}: Table[int, Table[int, TsnetRelayTaskHandle]] =
  initTable[int, Table[int, TsnetRelayTaskHandle]]()

initLock(relayRegistryLock)

proc bytesToString(bytes: openArray[byte]): string =
  result = newString(bytes.len)
  for i, value in bytes:
    result[i] = char(value)

proc defaultRelayPort(parsed: Uri): uint16 =
  case parsed.scheme.toLowerAscii()
  of "tcp":
    if parsed.port.len > 0:
      try:
        return uint16(max(1, parsed.port.parseInt() + 1))
      except CatchableError:
        discard
    return uint16(NimTsnetTcpRelayDefaultPort)
  else:
    if parsed.port.len > 0 and parsed.scheme.toLowerAscii() in ["relay", "nimtcp"]:
      try:
        return uint16(max(1, parsed.port.parseInt()))
      except CatchableError:
        discard
    return NimTsnetTcpRelayDefaultPort

proc bracketAuthorityHost(host: string): string =
  if host.contains(":") and not host.startsWith("["):
    "[" & host & "]"
  else:
    host

proc tcpRelayEndpoint*(controlUrl: string): Result[TsnetTcpRelayEndpoint, string] =
  let normalized =
    if controlUrl.strip().len == 0: ""
    else: normalizeControlUrl(controlUrl)
  if normalized.len == 0:
    return err("tsnet controlUrl is empty")
  let parsed =
    try:
      parseUri(normalized)
    except CatchableError as exc:
      return err("failed to parse tsnet controlUrl " & normalized & ": " & exc.msg)
  if parsed.hostname.len == 0:
    return err("tsnet controlUrl is missing a hostname")
  let port = defaultRelayPort(parsed)
  let authorityHost = bracketAuthorityHost(parsed.hostname)
  let authority = authorityHost & ":" & $port
  ok(TsnetTcpRelayEndpoint(
    url: "tcp://" & authority & NimTsnetTcpRelayPrefix,
    host: parsed.hostname,
    authority: authority,
    port: port,
    prefix: NimTsnetTcpRelayPrefix
  ))

proc nimTcpRelayBaseUrl*(controlUrl: string): string =
  let endpoint = tcpRelayEndpoint(controlUrl).valueOr:
    return ""
  endpoint.url

proc jsonString(node: JsonNode, key: string): string =
  if node.isNil or node.kind != JObject or not node.hasKey(key):
    return ""
  let value = node.getOrDefault(key)
  if value.kind == JString:
    return value.getStr().strip()
  ""

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

proc safeCloseTransport(transp: StreamTransport) {.async: (raises: []).} =
  if transp.isNil:
    return
  try:
    await transp.closeWait()
  except CatchableError:
    discard

proc safeCloseServer(server: StreamServer) =
  if server.isNil:
    return
  try:
    server.stop()
    server.close()
  except CatchableError:
    discard

proc readExactAsync(
    transp: StreamTransport,
    size: int
): Future[Result[string, string]] {.async: (raises: []).} =
  var buf = newSeq[byte](size)
  try:
    if size > 0:
      await transp.readExactly(addr buf[0], size)
    return ok(bytesToString(buf))
  except CancelledError:
    return err("tsnet relay read cancelled")
  except CatchableError as exc:
    return err("failed to read tsnet relay frame: " & exc.msg)

proc readFrameAsync(
    transp: StreamTransport
): Future[Result[string, string]] {.async: (raises: []).} =
  let header = await readExactAsync(transp, 4)
  if header.isErr():
    return err(header.error)
  let size = decodeFrameSize(header.get())
  if size < 0:
    return err("invalid tsnet relay frame header")
  if size == 0:
    return ok("")
  await readExactAsync(transp, size)

proc writeFrameAsync(
    transp: StreamTransport,
    payload: string
): Future[Result[void, string]] {.async: (raises: []).} =
  try:
    discard await transp.write(encodeFrame(payload))
    ok()
  except CancelledError:
    err("tsnet relay write cancelled")
  except CatchableError as exc:
    err("failed to write tsnet relay frame: " & exc.msg)

proc rawSocketFromAddress(address: string): Result[(string, int, string), string] =
  let parsed = MultiAddress.init(address).valueOr:
    return err("invalid tsnet relay raw address " & address & ": " & error)
  let family = familyFromAddress(parsed)
  if family.len == 0:
    return err("unsupported tsnet relay raw family in " & address)
  let port = portFromAddress(parsed)
  if port <= 0:
    return err("unsupported tsnet relay raw port in " & address)
  let parts = ($parsed).split('/')
  if parts.len < 5:
    return err("unsupported tsnet relay raw address " & address)
  ok((parts[2], port, family))

proc rawSocketAddress(localAddress: string, family: string): Result[MultiAddress, string] =
  try:
    let ma = MultiAddress.init(localAddress).valueOr:
      return err("failed to parse tsnet relay local address: " & error)
    let parts = ($ma).split('/')
    if parts.len < 5:
      return err("failed to parse tsnet relay local address " & localAddress)
    let host = parts[2].toLowerAscii()
    let port = parts[4]
    let kind =
      if family == "ip6":
        "/ip6/" & host
      else:
        "/ip4/" & host
    ok(MultiAddress.init(kind & "/tcp/" & port).expect("tsnet relay built a valid raw tcp multiaddr"))
  except CatchableError as exc:
    err("failed to inspect tsnet relay raw socket address: " & exc.msg)

proc buildAdvertisedSource(
    family, ip: string,
    port: int
): string =
  "/" & family & "/" & ip.strip().toLowerAscii() & "/tcp/" & $port & "/tsnet"

proc relayTrace(message: string) =
  try:
    stderr.writeLine("[tsnet-tcprelay] " & message)
    flushFile(stderr)
  except CatchableError:
    discard

proc relayRegisterTask(
    ownerId: int,
    server: StreamServer,
    task: Future[void]
): TsnetRelayTaskHandle =
  if ownerId <= 0:
    return TsnetRelayTaskHandle(routeId: 0, server: server, task: task)
  withLock(relayRegistryLock):
    inc relayNextRouteId
    result = TsnetRelayTaskHandle(
      routeId: relayNextRouteId,
      server: server,
      task: task
    )
    var handles = relayOwnerTasks.getOrDefault(ownerId, initTable[int, TsnetRelayTaskHandle]())
    handles[result.routeId] = result
    relayOwnerTasks[ownerId] = move(handles)

proc stopRelayListeners*(ownerId: int) =
  if ownerId <= 0:
    return
  var handles: seq[TsnetRelayTaskHandle] = @[]
  withLock(relayRegistryLock):
    if relayOwnerTasks.hasKey(ownerId):
      let ownerHandles = relayOwnerTasks.getOrDefault(ownerId, initTable[int, TsnetRelayTaskHandle]())
      handles = newSeqOfCap[TsnetRelayTaskHandle](ownerHandles.len)
      for _, handle in ownerHandles.pairs:
        handles.add(handle)
      relayOwnerTasks.del(ownerId)
  for handle in handles:
    if not handle.server.isNil:
      safeCloseServer(handle.server)
    if not handle.task.isNil and not handle.task.finished():
      handle.task.cancelSoon()

proc bridgePump(
    src, dst: StreamTransport
): Future[void] {.async: (raises: [CancelledError]).} =
  var buf = newSeq[byte](RelayFrameBufferSize)
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

proc bridgeTransports(
    a, b: StreamTransport
): Future[void] {.async: (raises: []).} =
  let forward = bridgePump(a, b)
  let backward = bridgePump(b, a)
  try:
    discard await one(@[forward, backward])
  except CatchableError:
    discard
  var cancels: seq[Future[void]] = @[]
  if not forward.finished():
    cancels.add(forward.cancelAndWait())
  if not backward.finished():
    cancels.add(backward.cancelAndWait())
  if cancels.len > 0:
    await noCancel allFutures(cancels)
  await noCancel a.safeCloseTransport()
  await noCancel b.safeCloseTransport()

proc relayConnectAsync(
    endpoint: TsnetTcpRelayEndpoint
): Future[Result[StreamTransport, string]] {.async: (raises: []).} =
  try:
    let address = initTAddress(endpoint.host, Port(endpoint.port))
    ok(await connect(address))
  except CancelledError:
    err("tsnet tcp relay connect cancelled")
  except CatchableError as exc:
    err("tsnet tcp relay connect failed at " & endpoint.url & ": " & exc.msg)

proc relayLocalAddress(host: string, port: int): Result[MultiAddress, string] =
  let family =
    if host.contains(":"): "ip6"
    else: "ip4"
  MultiAddress.init("/" & family & "/" & host & "/tcp/" & $port)

proc listenerLoop(
    ownerId: int,
    routeId: int,
    route: string,
    rawLocal: string,
    controlUrl: string
): Future[void] {.async: (raises: [CancelledError]).} =
  relayTrace("listener task start routeId=" & $routeId)
  try:
    while true:
      let endpoint = tcpRelayEndpoint(controlUrl).valueOr:
        relayTrace("listener endpoint parse failed routeId=" & $routeId & " err=" & error)
        await sleepAsync(1.seconds)
        continue
      relayTrace("listener connecting routeId=" & $routeId & " endpoint=" & endpoint.url)
      var relayTransport: StreamTransport = nil
      relayTransport = (await relayConnectAsync(endpoint)).valueOr:
        relayTrace("listener relay connect failed routeId=" & $routeId & " err=" & error)
        await sleepAsync(1.seconds)
        continue

      let hello = %*{
        "version": 1,
        "mode": "listen",
        "route": route
      }
      if (await writeFrameAsync(relayTransport, $hello)).isErr():
        relayTrace("listener hello write failed routeId=" & $routeId)
        await relayTransport.safeCloseTransport()
        await sleepAsync(1.seconds)
        continue
      let helloAck = await readFrameAsync(relayTransport)
      if helloAck.isErr():
        relayTrace("listener hello ack read failed routeId=" & $routeId & " err=" & helloAck.error)
        await relayTransport.safeCloseTransport()
        await sleepAsync(1.seconds)
        continue
      let helloNode =
        try:
          parseJson(helloAck.get())
        except CatchableError:
          await relayTransport.safeCloseTransport()
          await sleepAsync(1.seconds)
          continue
      if not helloNode{"ok"}.getBool():
        relayTrace("listener hello ack not ok routeId=" & $routeId)
        await relayTransport.safeCloseTransport()
        await sleepAsync(1.seconds)
        continue

      var restart = false
      while true:
        let incoming = await readFrameAsync(relayTransport)
        if incoming.isErr():
          relayTrace("listener incoming read failed routeId=" & $routeId & " err=" & incoming.error)
          restart = true
          break
        if incoming.get().len == 0:
          continue
        let incomingNode =
          try:
            parseJson(incoming.get())
          except CatchableError:
            restart = true
            break
        if jsonString(incomingNode, "op") != "incoming":
          continue

        relayTrace("listener incoming routeId=" & $routeId)
        let sourceAdvertised = jsonString(incomingNode, "source")
        let rawTarget = rawSocketFromAddress(rawLocal).valueOr:
          relayTrace("listener raw target parse failed routeId=" & $routeId & " err=" & error)
          discard await writeFrameAsync(relayTransport, $(rpcError(error)))
          restart = true
          break
        let localTarget = relayLocalAddress(rawTarget[0], rawTarget[1]).valueOr:
          relayTrace("listener raw target build failed routeId=" & $routeId & " err=" & error)
          discard await writeFrameAsync(relayTransport, $(rpcError(error)))
          restart = true
          break
        var localTransport: StreamTransport = nil
        try:
          localTransport = await connect(localTarget)
        except CatchableError as exc:
          relayTrace("listener local connect failed routeId=" & $routeId & " err=" & exc.msg)
          discard await writeFrameAsync(relayTransport, $(rpcError("failed to connect raw listener: " & exc.msg)))
          await localTransport.safeCloseTransport()
          restart = true
          break

        if sourceAdvertised.len > 0:
          let sourceAddr = MultiAddress.init(sourceAdvertised)
          if sourceAddr.isOk():
            try:
              let localObserved = rawSocketAddress($localTransport.localAddress, rawTarget[2])
              if localObserved.isOk():
                registerResolvedRemote(ownerId, localObserved.get(), sourceAddr.get())
            except CatchableError:
              discard

        let readyPayload = %*{
          "ok": true,
          "op": "ready"
        }
        if (await writeFrameAsync(relayTransport, $readyPayload)).isErr():
          relayTrace("listener ready write failed routeId=" & $routeId)
          await localTransport.safeCloseTransport()
          restart = true
          break

        relayTrace("listener bridge start routeId=" & $routeId)
        await bridgeTransports(relayTransport, localTransport)
        relayTrace("listener bridge done routeId=" & $routeId)
        restart = true
        break

      await relayTransport.safeCloseTransport()
      if not restart:
        break
      await sleepAsync(200.milliseconds)
  except CancelledError:
    discard
  finally:
    relayTrace("listener task stop routeId=" & $routeId)

proc startRelayListener*(
    ownerId: int,
    controlUrl: string,
    advertised: MultiAddress,
    rawLocal: MultiAddress
): Result[void, string] =
  if ownerId <= 0:
    return err("invalid tsnet relay owner id")
  discard tcpRelayEndpoint(controlUrl).valueOr:
    return err(error)
  let route = $advertised
  let task = listenerLoop(ownerId, relayNextRouteId + 1, route, $rawLocal, controlUrl)
  let handle = relayRegisterTask(ownerId, nil, task)
  asyncSpawn task
  relayTrace("listener scheduled routeId=" & $handle.routeId & " route=" & route)
  ok()

proc dialRelayBridge(
    routeId: int,
    controlUrl: string,
    route: string,
    source: string,
    localClient: StreamTransport
): Future[void] {.async: (raises: [CancelledError]).} =
  try:
    let endpoint = tcpRelayEndpoint(controlUrl).valueOr:
      relayTrace("dial endpoint parse failed routeId=" & $routeId & " err=" & error)
      await localClient.safeCloseTransport()
      return

    let startedAt = Moment.now()
    var relayTransport: StreamTransport = nil
    while true:
      relayTransport = (await relayConnectAsync(endpoint)).valueOr:
        if Moment.now() >= startedAt + RelayDialRetryWindow:
          relayTrace("dial relay connect failed routeId=" & $routeId & " err=" & error)
          await localClient.safeCloseTransport()
          return
        await sleepAsync(RelayDialRetryStep)
        continue
      let dialReq = %*{
        "version": 1,
        "mode": "dial",
        "route": route,
        "source": source
      }
      let writeRes = await writeFrameAsync(relayTransport, $dialReq)
      if writeRes.isErr():
        await relayTransport.safeCloseTransport()
        if Moment.now() >= startedAt + RelayDialRetryWindow:
          relayTrace("dial request write failed routeId=" & $routeId)
          await localClient.safeCloseTransport()
          return
        await sleepAsync(RelayDialRetryStep)
        continue
      let dialAck = await readFrameAsync(relayTransport)
      if dialAck.isErr():
        await relayTransport.safeCloseTransport()
        if Moment.now() >= startedAt + RelayDialRetryWindow:
          relayTrace("dial ack read failed routeId=" & $routeId & " err=" & dialAck.error)
          await localClient.safeCloseTransport()
          return
        await sleepAsync(RelayDialRetryStep)
        continue
      let dialNode =
        try:
          parseJson(dialAck.get())
        except CatchableError:
          await relayTransport.safeCloseTransport()
          if Moment.now() >= startedAt + RelayDialRetryWindow:
            relayTrace("dial ack parse failed routeId=" & $routeId)
            await localClient.safeCloseTransport()
            return
          await sleepAsync(RelayDialRetryStep)
          continue
      if not dialNode{"ok"}.getBool():
        let dialError = jsonString(dialNode, "error")
        await relayTransport.safeCloseTransport()
        if dialError != "route_not_registered" or Moment.now() >= startedAt + RelayDialRetryWindow:
          relayTrace("dial ack not ok routeId=" & $routeId & " err=" & dialError)
          await localClient.safeCloseTransport()
          return
        await sleepAsync(RelayDialRetryStep)
        continue

      await bridgeTransports(relayTransport, localClient)
      return
  except CancelledError as exc:
    raise exc
  except CatchableError as exc:
    relayTrace("dial loop failed routeId=" & $routeId & " err=" & exc.msg)
  finally:
    await localClient.safeCloseTransport()

proc openDialProxy*(
    ownerId: int,
    controlUrl: string,
    family: string,
    localTailnetIp: string,
    remoteAdvertised: MultiAddress
): Result[MultiAddress, string] =
  if ownerId <= 0:
    return err("invalid tsnet relay owner id")
  discard tcpRelayEndpoint(controlUrl).valueOr:
    return err(error)
  if family notin ["ip4", "ip6"]:
    return err("unsupported tsnet relay family " & family)
  if localTailnetIp.strip().len == 0:
    return err("missing local tailnet IP for tsnet relay dial proxy")

  let loopbackHost = if family == "ip6": "::1" else: "127.0.0.1"
  let bindAddress =
    try:
      initTAddress(loopbackHost, Port(0))
    except CatchableError as exc:
      return err("failed to bind tsnet relay dial server: " & exc.msg)
  let completion = newFuture[void]("tsnet.tcprelay.dialserver")
  let handle = relayRegisterTask(ownerId, nil, completion)
  let route = $remoteAdvertised
  let source = buildAdvertisedSource(family, localTailnetIp, 0)
  var server: StreamServer = nil
  try:
    proc serveDialClient(
        srv: StreamServer,
        localClient: StreamTransport
    ) {.async: (raises: []).} =
      safeCloseServer(srv)
      try:
        let localPort =
          try:
            let localAddr = MultiAddress.init(localClient.localAddress).valueOr:
              raise newException(CatchableError, error)
            portFromAddress(localAddr)
          except CatchableError:
            0
        let adjustedSource =
          if localPort > 0:
            buildAdvertisedSource(family, localTailnetIp, localPort)
          else:
            source
        await dialRelayBridge(handle.routeId, controlUrl, route, adjustedSource, localClient)
      except CancelledError:
        discard
      finally:
        if not completion.finished():
          completion.complete()

    server = createStreamServer(bindAddress, serveDialClient, flags = {ReuseAddr})
    server.start()
  except CatchableError as exc:
    return err("failed to start tsnet relay dial server: " & exc.msg)
  handle.server = server

  let rawAddress = MultiAddress.init(server.sock.getLocalAddress()).valueOr:
    safeCloseServer(server)
    return err("failed to build tsnet relay dial proxy address: " & error)
  relayTrace("dial server scheduled routeId=" & $handle.routeId & " raw=" & $rawAddress)
  ok(rawAddress)
