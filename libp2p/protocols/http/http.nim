# Nim-LibP2P
# Copyright (c)
# Licensed under either of
#  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
#  * MIT license ([LICENSE-MIT](LICENSE-MIT))
# at your option.

{.push raises: [].}

import std/[strutils, tables, options, strformat]
import stew/byteutils
import chronos, chronicles

import ../../switch
import ../../peerid
import ../../errors
import ../../stream/connection
import ../../protocols/protocol
import ../../utility
import ../../multiaddress

const
  HttpCodec* = "/http/1.1"
  MaxRequestLine = 8 * 1024
  MaxHeaderLine = 16 * 1024
  MaxHeaders = 100
  DefaultReasonPhrase = "OK"
  DefaultHttpBodyLimit* = 1_048_576

logScope:
  topics = "libp2p http"

type
  HttpHeaders* = Table[string, string]

  HttpRequest* = object
    verb*: string
    path*: string
    headers*: HttpHeaders
    body*: seq[byte]
    remotePeerId*: Option[PeerId]
    remoteAddr*: Option[MultiAddress]

  HttpResponse* = object
    status*: int
    reason*: string
    headers*: HttpHeaders
    body*: seq[byte]

  HttpHandler* = proc(req: HttpRequest): Future[HttpResponse] {.
    gcsafe, raises: []
  .}

  HttpRoute = object
    path: string
    handler: HttpHandler

  HttpConfig* = object
    maxRequestBytes*: int
    maxResponseBytes*: int
    handlerTimeout*: Duration

  HttpService* = ref object of LPProtocol
    routes: seq[HttpRoute]
    defaultHandler: HttpHandler
    config: HttpConfig

proc init*(
    _: type HttpConfig,
    maxRequestBytes: int = DefaultHttpBodyLimit,
    maxResponseBytes: int = DefaultHttpBodyLimit,
    handlerTimeout: Duration = 10.seconds,
): HttpConfig =
  HttpConfig(
    maxRequestBytes: maxRequestBytes,
    maxResponseBytes: maxResponseBytes,
    handlerTimeout: handlerTimeout,
  )

proc validate(config: HttpConfig) =
  if config.maxRequestBytes <= 0:
    raise newException(Defect, "http maxRequestBytes must be positive")
  if config.maxResponseBytes <= 0:
    raise newException(Defect, "http maxResponseBytes must be positive")
  if config.handlerTimeout < 0.seconds:
    raise newException(Defect, "http handlerTimeout cannot be negative")

proc normalizeHeader(name: string): string =
  result = name.toLowerAscii()

proc parseStartLine(line: string): tuple[verb, path, version: string] {.raises: [ValueError].} =
  let parts = line.splitWhitespace()
  if parts.len != 3:
    raise (ref ValueError)(msg: "invalid HTTP request line: " & line)
  (parts[0], parts[1], parts[2])

proc stripCr(text: string): string =
  if text.endsWith("\r"):
    text[0 ..< text.high]
  else:
    text

proc readHeaderLines(
    conn: Connection
): Future[seq[string]] {.async: (raises: [CancelledError, LPStreamError, ValueError]).} =
  var lines: seq[string] = @[]
  while true:
    let line = await conn.readLine(limit = MaxHeaderLine)
    let stripped = stripCr(line)
    if stripped.len == 0:
      break
    lines.add(stripped)
    if lines.len > MaxHeaders:
      raise (ref ValueError)(msg: "too many headers")
  lines

proc parseHeaders(lines: seq[string]): HttpHeaders {.raises: [ValueError].} =
  var headers = initTable[string, string](lines.len)
  for line in lines:
    let idx = line.find(':')
    if idx <= 0:
      raise (ref ValueError)(msg: "invalid header line: " & line)
    let name = normalizeHeader(line[0 ..< idx].strip())
    let value = line[idx + 1 .. ^1].strip()
    headers[name] = value
  headers

proc contentLength(headers: HttpHeaders): int {.raises: [ValueError].} =
  if headers.contains("content-length"):
    try:
      parseInt(headers["content-length"])
    except ValueError:
      raise (ref ValueError)(msg: "invalid content-length")
  else:
    0

proc readBody(
    conn: Connection, len: int
): Future[seq[byte]] {.async: (raises: [CancelledError, LPStreamError]).} =
  if len <= 0:
    return @[]
  var buf = newSeqUninit[byte](len)
  await conn.readExactly(addr buf[0], len)
  buf

proc readRequest(
    conn: Connection, maxBody: int = -1
): Future[HttpRequest] {.async: (raises: [CancelledError, LPStreamError, ValueError]).} =
  let startLine = stripCr(await conn.readLine(limit = MaxRequestLine))
  if startLine.len == 0:
    raise (ref ValueError)(msg: "empty request line")
  let (verb, path, _) = parseStartLine(startLine)
  let headerLines = await readHeaderLines(conn)
  var headers = parseHeaders(headerLines)
  if not headers.contains("connection"):
    headers["connection"] = "close"
  let len =
    try:
      contentLength(headers)
    except ValueError as exc:
      raise (ref ValueError)(msg: "invalid content-length: " & exc.msg)
  if maxBody >= 0 and len > maxBody:
    raise (ref ValueError)(msg: "request body exceeds configured limit")
  let body = await conn.readBody(len)
  HttpRequest(verb: verb, path: path, headers: headers, body: body)

proc statusReason(status: int): string =
  case status
  of 200: "OK"
  of 201: "Created"
  of 202: "Accepted"
  of 204: "No Content"
  of 301: "Moved Permanently"
  of 302: "Found"
  of 304: "Not Modified"
  of 400: "Bad Request"
  of 401: "Unauthorized"
  of 403: "Forbidden"
  of 404: "Not Found"
  of 405: "Method Not Allowed"
  of 413: "Payload Too Large"
  of 500: "Internal Server Error"
  of 502: "Bad Gateway"
  of 504: "Gateway Timeout"
  of 503: "Service Unavailable"
  else:
    DefaultReasonPhrase

proc serializeResponse(resp: HttpResponse): seq[byte] =
  var buf = newSeq[byte]()
  let reason = if resp.reason.len == 0: statusReason(resp.status) else: resp.reason
  let statusLine = &"HTTP/1.1 {resp.status} {reason}\r\n"
  buf.add(statusLine.toBytes())

  var headers = resp.headers
  if headers.len == 0:
    headers = initTable[string, string]()
  if not headers.contains("content-length"):
    headers["content-length"] = $resp.body.len
  if not headers.contains("connection"):
    headers["connection"] = "close"

  for name, value in headers.pairs():
    let headerLine = &"{name}: {value}\r\n"
    buf.add(headerLine.toBytes())
  buf.add(@[byte '\r', byte '\n'])
  if resp.body.len > 0:
    buf.add(resp.body)
  buf

proc serializeRequest(req: HttpRequest): seq[byte] =
  var buf = newSeq[byte]()
  let line = &"{req.verb} {req.path} HTTP/1.1\r\n"
  buf.add(line.toBytes())
  var headers = req.headers
  if headers.len == 0:
    headers = initTable[string, string]()
  if not headers.contains("host"):
    headers["host"] = ""
  if not headers.contains("connection"):
    headers["connection"] = "close"
  if not headers.contains("content-length"):
    headers["content-length"] = $req.body.len
  for name, value in headers.pairs():
    let headerLine = &"{name}: {value}\r\n"
    buf.add(headerLine.toBytes())
  buf.add(@[byte '\r', byte '\n'])
  if req.body.len > 0:
    buf.add(req.body)
  buf

proc defaultResponse(status: int, message: string): HttpResponse =
  HttpResponse(
    status: status,
    reason: statusReason(status),
    headers: {"content-type": "text/plain; charset=utf-8"}.toTable(),
    body: message.toBytes(),
  )

proc pickHandler(svc: HttpService, path: string): HttpHandler =
  if svc.routes.len == 0:
    return svc.defaultHandler
  var selected: Option[HttpHandler]
  var selectedLen = -1
  for route in svc.routes:
    if path.startsWith(route.path) and route.path.len > selectedLen:
      selected = some(route.handler)
      selectedLen = route.path.len
  selected.get(svc.defaultHandler)

proc new*(
    T: type HttpService,
    defaultHandler: HttpHandler,
    config: HttpConfig = HttpConfig.init(),
): T {.public.} =
  validate(config)
  let service = HttpService(
    routes: @[], defaultHandler: defaultHandler, config: config
  )
  service.init()
  service.codec = HttpCodec
  service

proc registerRoute*(svc: HttpService, path: string, handler: HttpHandler) {.public.} =
  svc.routes.add(HttpRoute(path: path, handler: handler))

proc setDefaultHandler*(svc: HttpService, handler: HttpHandler) {.public.} =
  svc.defaultHandler = handler

method init*(svc: HttpService) =
  proc handle(conn: Connection, proto: string) {.async: (raises: [CancelledError]).} =
    trace "handling http request", conn
    try:
      var req = await readRequest(conn, svc.config.maxRequestBytes)
      req.remotePeerId = some(conn.peerId)
      if conn.observedAddr.isSome():
        req.remoteAddr = some(conn.observedAddr.get())
      trace "received http request", httpMethod = req.verb, path = req.path
      let handler = svc.pickHandler(req.path)
      var resp: HttpResponse
      if handler.isNil:
        resp = defaultResponse(404, "Not Found")
      else:
        if svc.config.handlerTimeout > 0.seconds:
          let handlerFuture = handler(req)
          let completed = await withTimeout(handlerFuture, svc.config.handlerTimeout)
          if not completed:
            trace "http handler timeout", conn, path = req.path
            resp = defaultResponse(504, "Gateway Timeout")
          else:
            resp = await handlerFuture
        else:
          resp = await handler(req)
      if resp.status == 0:
        resp.status = 200
      if resp.body.len > svc.config.maxResponseBytes:
        trace "http response exceeds limit", conn,
          size = resp.body.len, limit = svc.config.maxResponseBytes
        let limited = defaultResponse(500, "Response Too Large")
        try:
          await conn.write(serializeResponse(limited))
        except LPStreamError as sendErr:
          trace "failed sending http error response", conn, err = sendErr.msg
        return
      let payload = serializeResponse(resp)
      try:
        await conn.write(payload)
      except LPStreamError as sendErr:
        trace "failed sending http response", conn, err = sendErr.msg
    except CancelledError as exc:
      trace "http handler cancelled", conn
      raise exc
    except ValueError as exc:
      trace "invalid http request", err = exc.msg
      let status =
        if exc.msg == "request body exceeds configured limit": 413 else: 400
      let resp = defaultResponse(status, if status == 413: "Payload Too Large" else: "Bad Request")
      try:
        await conn.write(serializeResponse(resp))
      except LPStreamError as sendErr:
        trace "failed sending http bad-request response", conn, err = sendErr.msg
    except CatchableError as exc:
      trace "exception while handling http request", err = exc.msg
      let resp = defaultResponse(500, "Internal Server Error")
      try:
        await conn.write(serializeResponse(resp))
      except LPStreamError as sendErr:
        trace "failed sending http internal error response", conn, err = sendErr.msg
    finally:
      await conn.close()

  svc.handler = handle

proc httpRequest*(
    sw: Switch,
    peerId: PeerId,
    request: HttpRequest,
    timeout: Duration = 10.seconds,
    maxResponseBytes = DefaultHttpBodyLimit,
): Future[HttpResponse] {.async: (raises: [CancelledError, LPError]).} =
  if maxResponseBytes <= 0:
    raise (ref LPError)(msg: "maxResponseBytes must be positive")
  let conn =
    try:
      await sw.dial(peerId, @[HttpCodec])
    except CancelledError as exc:
      raise exc
    except CatchableError as exc:
      raise (ref LPError)(msg: "http dial failed: " & exc.msg, parent: exc)

  let serialized = serializeRequest(request)
  try:
    await conn.write(serialized)
  except LPStreamError as exc:
    await conn.close()
    raise (ref LPError)(msg: "http write failed: " & exc.msg, parent: exc)

  let startLine =
    try:
      stripCr(await conn.readLine(limit = MaxRequestLine))
    except LPStreamError as exc:
      await conn.close()
      raise (ref LPError)(msg: "http read status failed: " & exc.msg, parent: exc)
  let parts = startLine.splitWhitespace()
  if parts.len < 2:
    await conn.close()
    raise (ref LPError)(msg: "invalid http response status line")
  let status =
    try:
      parseInt(parts[1])
    except ValueError:
      await conn.close()
      raise (ref LPError)(msg: "invalid http status code")
  let headerLines =
    try:
      await readHeaderLines(conn)
    except ValueError as exc:
      await conn.close()
      raise (ref LPError)(msg: "invalid http response headers: " & exc.msg)
    except LPStreamError as exc:
      await conn.close()
      raise (ref LPError)(msg: "http read headers failed: " & exc.msg, parent: exc)
  let headers =
    try:
      parseHeaders(headerLines)
    except ValueError as exc:
      await conn.close()
      raise (ref LPError)(msg: "invalid http header line: " & exc.msg)
  let len =
    try:
      contentLength(headers)
    except ValueError as exc:
      await conn.close()
      raise (ref LPError)(msg: "invalid http content-length: " & exc.msg)
  if len > maxResponseBytes:
    await conn.close()
    raise (ref LPError)(
      msg: "http response exceeds maxResponseBytes (" & $len & " > " & $maxResponseBytes & ")"
    )
  let body =
    try:
      await conn.readBody(len)
    except LPStreamError as exc:
      await conn.close()
      raise (ref LPError)(msg: "http read body failed: " & exc.msg, parent: exc)
  if body.len > maxResponseBytes:
    await conn.close()
    raise (ref LPError)(
      msg: "http response exceeds maxResponseBytes (" & $body.len & " > " & $maxResponseBytes & ")"
    )
  await conn.close()
  HttpResponse(
    status: status,
    reason: if parts.len >= 3: parts[2 .. ^1].join(" ") else: statusReason(status),
    headers: headers,
    body: body,
  )
