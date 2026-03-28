import std/[asynchttpserver, asyncdispatch, json, options, os, parseopt, strutils, uri]

import ../libp2p/transports/tsnet/[control, h3control]
import ../libp2p/utility

type
  GatewayArgs = object
    listenHost: string
    listenPort: int
    controlUrl: string

proc jsonString(node: JsonNode, key: string, defaultValue = ""): string =
  if node.isNil or node.kind != JObject or not node.hasKey(key):
    return defaultValue
  let value = node.getOrDefault(key)
  if value.kind == JString:
    return value.getStr().strip()
  defaultValue

proc parseArgs(): Result[GatewayArgs, string] =
  var args = GatewayArgs(
    listenHost: "127.0.0.1",
    listenPort: 9443,
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

proc jsonResponse(
    req: Request,
    status: HttpCode,
    payload: JsonNode
): Future[void] =
  var headers = newHttpHeaders()
  headers["content-type"] = "application/json"
  req.respond(status, $payload, headers)

proc errorPayload(message: string): JsonNode =
  %*{
    "ok": false,
    "error": message
  }

proc requestJson(req: Request): Result[JsonNode, string] =
  try:
    if req.body.len == 0:
      return ok(newJObject())
    ok(parseJson(req.body))
  except CatchableError as exc:
    err("invalid JSON request body: " & exc.msg)

proc firstQueryParam(query: string, key: string): string =
  if query.len == 0:
    return ""
  for pair in query.split("&"):
    if pair.len == 0:
      continue
    let eqPos = pair.find('=')
    let pairKey =
      if eqPos < 0: pair
      else: pair[0 ..< eqPos]
    if pairKey == key:
      if eqPos < 0:
        return ""
      try:
        return decodeUrl(pair[eqPos + 1 .. ^1], decodePlus = false)
      except CatchableError:
        return pair[eqPos + 1 .. ^1]
  ""

proc routeKey(args: GatewayArgs, req: Request): Future[void]
    {.async.} =
  let rawUrl = req.url
  let version =
    try:
      max(1, firstQueryParam(rawUrl.query, "v").parseInt())
    except CatchableError:
      130
  let fetched = control.fetchControlServerKey(args.controlUrl, version)
  if fetched.isErr():
    await req.jsonResponse(Http502, errorPayload(fetched.error))
    return
  var payload = fetched.get().rawPayload
  if payload.isNil or payload.kind == JNull:
    payload = newJObject()
  payload["ok"] = %true
  await req.jsonResponse(Http200, payload)

proc routeRegister(args: GatewayArgs, req: Request): Future[void]
    {.async.} =
  let payload = requestJson(req).valueOr:
    await req.jsonResponse(Http400, errorPayload(error))
    return
  let requestNode = payload{"request"}
  if requestNode.isNil or requestNode.kind != JObject:
    await req.jsonResponse(Http400, errorPayload("missing request object"))
    return
  let regRes = defaultControlTransport().registerNode(args.controlUrl, requestNode)
  if regRes.isErr():
    await req.jsonResponse(Http502, errorPayload(regRes.error))
    return
  await req.jsonResponse(Http200, regRes.get().rawPayload)

proc routeMap(args: GatewayArgs, req: Request): Future[void]
    {.async.} =
  let payload = requestJson(req).valueOr:
    await req.jsonResponse(Http400, errorPayload(error))
    return
  let requestNode = payload{"request"}
  if requestNode.isNil or requestNode.kind != JObject:
    await req.jsonResponse(Http400, errorPayload("missing request object"))
    return
  let mapRes = defaultControlTransport().mapPoll(args.controlUrl, requestNode)
  if mapRes.isErr():
    await req.jsonResponse(Http502, errorPayload(mapRes.error))
    return
  await req.jsonResponse(Http200, mapRes.get().rawPayload)

proc routeHealth(args: GatewayArgs, req: Request): Future[void]
    {.async.} =
  await req.jsonResponse(Http200, %*{
    "ok": true,
    "controlUrl": args.controlUrl,
    "protocol": "nim_h3",
    "upstreamProtocol": "ts2021_h2",
    "endpointPrefix": NimTsnetH3Prefix
  })

proc handler(args: GatewayArgs, req: Request): Future[void]
    {.async.} =
  let rawPath = req.url.path.normalizePathEnd(false)
  if req.reqMethod == HttpGet and rawPath == NimTsnetH3Prefix & "/health":
    await routeHealth(args, req)
  elif req.reqMethod == HttpGet and rawPath == NimTsnetH3Prefix & "/key":
    await routeKey(args, req)
  elif req.reqMethod == HttpPost and rawPath == NimTsnetH3Prefix & "/register":
    await routeRegister(args, req)
  elif req.reqMethod == HttpPost and rawPath == NimTsnetH3Prefix & "/map":
    await routeMap(args, req)
  else:
    await req.jsonResponse(Http404, errorPayload("not_found"))

proc main() =
  let args = parseArgs().valueOr:
    stderr.writeLine(error)
    quit(2)
  let server = newAsyncHttpServer()
  proc onRequest(req: Request): Future[void] {.async.} =
    await handler(args, req)
  echo $(
    %*{
      "ok": true,
      "listenHost": args.listenHost,
      "listenPort": args.listenPort,
      "controlUrl": args.controlUrl,
      "healthUrl": "http://" & args.listenHost & ":" & $args.listenPort & NimTsnetH3Prefix & "/health"
    }
  )
  waitFor server.serve(Port(args.listenPort), onRequest, address = args.listenHost)

main()
