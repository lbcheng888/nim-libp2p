import chronos
import std/[json, os, parseopt, strutils]

import ../libp2p/transports/tsnet/[control, tcpcontrol]
import ../libp2p/utility

type
  GatewayArgs = object
    listenHost: string
    listenPort: int
    controlUrl: string

proc parseArgs(): Result[GatewayArgs, string] =
  var args = GatewayArgs(
    listenHost: "0.0.0.0",
    listenPort: int(NimTsnetTcpDefaultPort),
    controlUrl: ""
  )
  var parser = initOptParser(commandLineParams())
  while true:
    parser.next()
    case parser.kind
    of cmdEnd:
      break
    of cmdLongOption, cmdShortOption:
      case parser.key
      of "listen-host":
        args.listenHost = parser.val.strip()
      of "listen-port":
        try:
          args.listenPort = max(1, parser.val.parseInt())
        except CatchableError:
          return err("invalid --listen-port: " & parser.val)
      of "control-url":
        args.controlUrl = parser.val.strip()
      else:
        return err("unknown option: --" & parser.key)
    of cmdArgument:
      return err("unexpected argument: " & parser.key)
  if args.controlUrl.len == 0:
    return err("missing --control-url")
  ok(args)

proc tcpGatewayTrace(message: string) =
  try:
    stderr.writeLine("[nim_tcp_gateway] " & message)
    flushFile(stderr)
  except CatchableError:
    discard

proc closeTransport(transp: StreamTransport) {.async: (raises: []).} =
  if transp.isNil:
    return
  try:
    await transp.closeWait()
  except CatchableError:
    discard

proc bytesToString(bytes: openArray[byte]): string =
  result = newString(bytes.len)
  for i, value in bytes:
    result[i] = char(value)

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

proc readExactAsync(
    transp: StreamTransport,
    size: int
): Future[Result[string, string]] {.async: (raises: []).} =
  var buf = newSeq[byte](size)
  try:
    if size > 0:
      await transp.readExactly(addr buf[0], size)
    result = ok(bytesToString(buf))
  except CancelledError:
    result = err("nim_tcp gateway read cancelled")
  except CatchableError as exc:
    result = err("failed to read nim_tcp gateway frame: " & exc.msg)

proc readFrameAsync(
    transp: StreamTransport
): Future[Result[string, string]] {.async: (raises: []).} =
  let header = await readExactAsync(transp, 4)
  if header.isErr():
    return err(header.error)
  let size = decodeFrameSize(header.get())
  if size < 0:
    return err("invalid nim_tcp gateway frame header")
  if size == 0:
    return ok("")
  await readExactAsync(transp, size)

proc writeFrameAsync(
    transp: StreamTransport,
    payload: string
): Future[Result[void, string]] {.async: (raises: []).} =
  try:
    discard await transp.write(encodeFrame(payload))
    result = ok()
  except CancelledError:
    result = err("nim_tcp gateway write cancelled")
  except CatchableError as exc:
    result = err("failed to write nim_tcp gateway frame: " & exc.msg)

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

proc requestNode(raw: string): Result[JsonNode, string] =
  try:
    ok(parseJson(raw))
  except CatchableError as exc:
    err("invalid JSON request body: " & exc.msg)

proc routeKey(args: GatewayArgs, request: JsonNode): JsonNode =
  let capabilityVersion =
    if request.kind == JObject and request.hasKey("capabilityVersion"):
      try:
        request["capabilityVersion"].getInt()
      except CatchableError:
        130
    else:
      130
  let fetched = fetchControlServerKey(args.controlUrl, capabilityVersion)
  if fetched.isErr():
    return rpcError(fetched.error)
  rpcSuccess(fetched.get().rawPayload)

proc routeRegister(args: GatewayArgs, request: JsonNode): JsonNode =
  let requestPayload =
    if request.kind == JObject: request.getOrDefault("request")
    else: newJNull()
  if requestPayload.kind != JObject:
    return rpcError("missing request object")
  let regRes = defaultControlTransport().registerNode(args.controlUrl, requestPayload)
  if regRes.isErr():
    return rpcError(regRes.error)
  rpcSuccess(regRes.get().rawPayload)

proc routeMap(args: GatewayArgs, request: JsonNode): JsonNode =
  let requestPayload =
    if request.kind == JObject: request.getOrDefault("request")
    else: newJNull()
  if requestPayload.kind != JObject:
    return rpcError("missing request object")
  let mapRes = defaultControlTransport().mapPoll(args.controlUrl, requestPayload)
  if mapRes.isErr():
    return rpcError(mapRes.error)
  rpcSuccess(mapRes.get().rawPayload)

proc routeHealth(args: GatewayArgs): JsonNode =
  rpcSuccess(%*{
    "controlUrl": args.controlUrl,
    "protocol": "nim_tcp",
    "upstreamProtocol": "ts2021_h2",
    "endpointPrefix": NimTsnetTcpPrefix
  })

proc handleRequest(args: GatewayArgs, request: JsonNode): JsonNode =
  let route =
    if request.kind == JObject and request.hasKey("route"):
      request["route"].getStr().strip()
    else:
      ""
  tcpGatewayTrace("route=" & (if route.len > 0: route else: "<empty>"))
  case route
  of "health":
    routeHealth(args)
  of "key":
    routeKey(args, request)
  of "register":
    routeRegister(args, request)
  of "map":
    routeMap(args, request)
  else:
    rpcError("not_found")

proc handleClient(
    args: GatewayArgs,
    server: StreamServer,
    client: StreamTransport
) {.async: (raises: []).} =
  discard server
  tcpGatewayTrace("client accepted")
  try:
    let rawRequest = await readFrameAsync(client)
    if rawRequest.isErr():
      tcpGatewayTrace("read failed err=" & rawRequest.error)
      discard await writeFrameAsync(client, $(rpcError(rawRequest.error)))
      return
    let request = requestNode(rawRequest.get()).valueOr:
      tcpGatewayTrace("parse failed err=" & error)
      discard await writeFrameAsync(client, $(rpcError(error)))
      return
    let response = handleRequest(args, request)
    let writeRes = await writeFrameAsync(client, $response)
    if writeRes.isErr():
      tcpGatewayTrace("write failed err=" & writeRes.error)
    else:
      tcpGatewayTrace("response sent")
  except CancelledError:
    discard
  except CatchableError as exc:
    tcpGatewayTrace("client handler error=" & exc.msg)
  finally:
    await closeTransport(client)

proc main() {.async: (raises: [CatchableError]).} =
  let args = parseArgs().valueOr:
    stderr.writeLine(error)
    quit(2)

  let bindAddr = initTAddress(args.listenHost, Port(args.listenPort))
  proc onClient(srv: StreamServer, client: StreamTransport) {.async: (raises: []).} =
    await handleClient(args, srv, client)
  let server = createStreamServer(
    bindAddr,
    onClient,
    flags = {ReuseAddr}
  )
  server.start()

  echo $(
    %*{
      "ok": true,
      "listenHost": args.listenHost,
      "listenPort": args.listenPort,
      "controlUrl": args.controlUrl,
      "protocol": "nim_tcp",
      "healthUrl": nimTcpHealthUrl("tcp://" & args.listenHost & ":" & $args.listenPort)
    }
  )

  while true:
    await sleepAsync(1.hours)

waitFor main()
