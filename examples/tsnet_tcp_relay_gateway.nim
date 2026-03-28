import chronos
import std/[json, os, parseopt, sequtils, strutils, tables]

import ../libp2p/utility

type
  GatewayArgs = object
    listenHost: string
    listenPort: int

  RelayGateway = ref object
    server: StreamServer
    listeners: Table[string, StreamTransport]

proc parseArgs(): Result[GatewayArgs, string] =
  var args = GatewayArgs(
    listenHost: "0.0.0.0",
    listenPort: 9446
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
      else:
        return err("unknown option: --" & parser.key)
    of cmdArgument:
      return err("unexpected argument: " & parser.key)
  ok(args)

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
    raiseAssert "relay gateway readExact failed: " & exc.msg
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
    raiseAssert "relay gateway writeFrame failed: " & exc.msg

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

proc startGateway(host: string, port: int): RelayGateway =
  let gateway = RelayGateway(listeners: initTable[string, StreamTransport]())
  let bindAddr =
    try:
      initTAddress(host, Port(port))
    except CatchableError as exc:
      stderr.writeLine("failed to bind relay gateway: " & exc.msg)
      quit(2)

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
        await writeFrame(client, $(rpcSuccess(%*{
          "mode": "listen",
          "route": route
        })))
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
        await writeFrame(client, $(rpcSuccess(%*{
          "mode": "dial",
          "route": route
        })))
        await bridgeTransports(listener, client)
      else:
        await writeFrame(client, $(rpcError("unsupported_mode")))
        await closeTransport(client)
    except CatchableError:
      await closeTransport(client)

  gateway.server = createStreamServer(bindAddr, handleClient, {ReuseAddr})
  gateway.server.start()
  gateway

proc main() =
  let args = parseArgs().valueOr:
    stderr.writeLine(error)
    quit(2)

  let gateway = startGateway(args.listenHost, args.listenPort)

  echo $(
    %*{
      "ok": true,
      "listenHost": args.listenHost,
      "listenPort": args.listenPort,
      "protocol": "nim_tcp_relay_async"
    }
  )

  waitFor gateway.server.join()

main()
