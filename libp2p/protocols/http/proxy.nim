# Nim-LibP2P
# Copyright (c)
# Licensed under either of
#  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
#  * MIT license ([LICENSE-MIT](LICENSE-MIT))
# at your option.

{.push raises: [].}

import std/[strutils, tables, options]
import std/uri
import chronos, chronicles
import chronos/apps/http/httpclient
import stew/byteutils

import ./http
import ../../utility
import ../../multiaddress

logScope:
  topics = "libp2p http proxy"

const
  Libp2pForwardedProto = "libp2p"
  Libp2pPeerIdHeader = "x-libp2p-peer-id"
  Libp2pRemoteAddrHeader = "x-libp2p-remote-addr"
  DefaultForwardedForHeader = "x-forwarded-for"

type
  HttpProxyConfig* = object
    ## Configuration for reverse proxying HTTP requests over libp2p streams.
    target*: Uri
    stripPath*: string
    preserveHost*: bool
    addForwardedProto*: bool
    extraRequestHeaders*: Table[string, string]
    addForwardedFor*: bool
    forwardedForHeader*: string
    addPeerIdHeader*: bool
    peerIdHeader*: string
    addRemoteAddrHeader*: bool
    remoteAddrHeader*: string

  HttpReverseProxy* = ref object
    config*: HttpProxyConfig
    session*: HttpSessionRef
    hostHeader: string

proc normalizeStripPath(path: string): string =
  if path.len == 0:
    return ""
  var res = if path.startsWith("/"): path else: "/" & path
  if res == "/":
    return ""
  while res.len > 1 and res.endsWith("/"):
    res.setLen(res.len - 1)
  res

proc defaultPortForScheme(scheme: string): int =
  case scheme.toLowerAscii()
  of "http": 80
  of "https": 443
  else:
    0

proc parsePort(port: string): int =
  if port.len == 0:
    return 0
  try:
    parseInt(port)
  except ValueError:
    0

proc computeHostHeader(target: Uri): string =
  var host = target.hostname
  let targetPort = parsePort(target.port)
  let defaultPort = defaultPortForScheme(target.scheme)
  if targetPort != 0 and targetPort != defaultPort:
    host.add(":" & $targetPort)
  host

proc toHeaderTable(
    entries: openArray[(string, string)]
): Table[string, string] =
  var headers = initTable[string, string](entries.len)
  for (k, v) in entries:
    headers[k.toLowerAscii()] = v
  headers

proc init*(
    _: type HttpProxyConfig,
    target: Uri,
    stripPath: string = "",
    preserveHost: bool = false,
    addForwardedProto: bool = true,
    extraHeaders: openArray[(string, string)] = [],
): HttpProxyConfig {.raises: [ValueError].} =
  if target.scheme.len == 0 or target.hostname.len == 0:
    raise newException(ValueError, "proxy target must include scheme and hostname")
  let scheme = target.scheme.toLowerAscii()
  if scheme notin ["http", "https"]:
    raise newException(ValueError, "proxy target scheme must be http or https")
  HttpProxyConfig(
    target: target,
    stripPath: normalizeStripPath(stripPath),
    preserveHost: preserveHost,
    addForwardedProto: addForwardedProto,
    extraRequestHeaders: toHeaderTable(extraHeaders),
    addForwardedFor: true,
    forwardedForHeader: DefaultForwardedForHeader,
    addPeerIdHeader: true,
    peerIdHeader: Libp2pPeerIdHeader,
    addRemoteAddrHeader: true,
    remoteAddrHeader: Libp2pRemoteAddrHeader,
  )

proc init*(
    _: type HttpProxyConfig,
    target: string,
    stripPath: string = "",
    preserveHost: bool = false,
    addForwardedProto: bool = true,
    extraHeaders: openArray[(string, string)] = [],
): HttpProxyConfig {.raises: [ValueError].} =
  let uri =
    try:
      parseUri(target)
    except ValueError as exc:
      raise newException(ValueError, "failed to parse proxy target URI: " & exc.msg)
  HttpProxyConfig.init(
    uri, stripPath = stripPath, preserveHost = preserveHost,
    addForwardedProto = addForwardedProto, extraHeaders = extraHeaders
  )

proc new*(
    T: type HttpReverseProxy,
    config: HttpProxyConfig,
    session: HttpSessionRef = HttpSessionRef.new(),
): T {.public.} =
  doAssert(not session.isNil(), "HttpReverseProxy requires a valid HttpSessionRef")
  HttpReverseProxy(config: config, session: session, hostHeader: computeHostHeader(config.target))

proc close*(proxy: HttpReverseProxy) {.async: (raises: [CancelledError]).} =
  if proxy.isNil():
    return
  if not proxy.session.isNil():
    await proxy.session.closeWait()

proc plainResponse(status: int, message: string): HttpResponse =
  HttpResponse(
    status: status,
    headers: {"content-type": "text/plain; charset=utf-8"}.toTable(),
    body: message.toBytes(),
  )

proc combinePaths(basePath, rel: string): string =
  var base = basePath
  var relative = rel
  if relative.len == 0:
    if base.len == 0:
      return "/"
    if base.startsWith("/"):
      return base
    return "/" & base
  if not relative.startsWith("/"):
    relative = "/" & relative
  if base.len == 0 or base == "/":
    return relative
  if not base.startsWith("/"):
    base = "/" & base
  if base.endsWith("/"):
    if relative.len == 1:
      return base
    return base & relative[1 .. ^1]
  if relative.len == 1:
    return base
  base & relative

proc buildUpstreamUri(proxy: HttpReverseProxy, requestPath: string): Uri =
  var dest = proxy.config.target
  var path = requestPath
  var query = ""
  let idx = path.find('?')
  if idx >= 0:
    query = if idx + 1 < path.len: path[idx + 1 .. ^1] else: ""
    path = if idx > 0: path[0 ..< idx] else: ""
  if path.len == 0:
    path = "/"
  let strip = proxy.config.stripPath
  if strip.len > 0 and path.startsWith(strip):
    let next = strip.len
    if path.len == strip.len or (next < path.len and path[next] == '/'):
      path = path[strip.len .. ^1]
      if path.len == 0:
        path = "/"
      elif path[0] != '/':
        path = "/" & path
  dest.path = combinePaths(dest.path, path)
  dest.query = query
  dest.anchor = ""
  dest

proc parseMethod(verb: string): HttpMethod {.raises: [ValueError].} =
  case verb.toUpperAscii()
  of "GET": MethodGet
  of "POST": MethodPost
  of "PUT": MethodPut
  of "DELETE": MethodDelete
  of "PATCH": MethodPatch
  of "HEAD": MethodHead
  of "OPTIONS": MethodOptions
  of "TRACE": MethodTrace
  of "CONNECT": MethodConnect
  else:
    raise newException(ValueError, "unsupported verb: " & verb)

proc extractForwardedForTargets(req: HttpRequest): seq[string] =
  if req.remoteAddr.isSome():
    let remoteAddr = req.remoteAddr.get()
    let stringResult = remoteAddr.toString()
    if stringResult.isOk():
      let text = stringResult.get()
      let parts = text.split('/')
      var idx = 0
      while idx < parts.len:
        let part = parts[idx]
        if part.len == 0:
          inc idx
          continue
        if part in ["ip4", "ip6", "dns", "dns4", "dns6", "dnsaddr"]:
          if idx + 1 < parts.len:
            let value = parts[idx + 1]
            if value.len > 0:
              result.add(value)
              break
        inc idx
  if result.len == 0 and req.remotePeerId.isSome():
    result.add($req.remotePeerId.get())

proc remoteMultiaddrText(req: HttpRequest): Option[string] =
  if req.remoteAddr.isSome():
    let remoteAddr = req.remoteAddr.get()
    let strRes = remoteAddr.toString()
    if strRes.isOk():
      return some(strRes.get())
  none(string)

proc prepareHeaders(
    proxy: HttpReverseProxy, request: HttpRequest
): seq[(string, string)] =
  var prepared = initTable[string, string]()
  for name, value in request.headers.pairs():
    let lower = name.toLowerAscii()
    case lower
    of "connection", "content-length", "transfer-encoding", "te":
      continue
    of "host":
      if proxy.config.preserveHost:
        prepared[lower] = value
    else:
      prepared[lower] = value
  if not proxy.config.preserveHost:
    prepared["host"] = proxy.hostHeader
  prepared["connection"] = "close"
  if proxy.config.addForwardedProto:
    prepared["x-forwarded-proto"] = Libp2pForwardedProto
  if proxy.config.addForwardedFor:
    let headerName = proxy.config.forwardedForHeader.toLowerAscii()
    var value = prepared.getOrDefault(headerName, "")
    let targets = extractForwardedForTargets(request)
    if targets.len > 0:
      let addition = targets.join(", ")
      if value.len == 0:
        prepared[headerName] = addition
      else:
        prepared[headerName] = value & ", " & addition
  if proxy.config.addPeerIdHeader and request.remotePeerId.isSome():
    let headerName = proxy.config.peerIdHeader.toLowerAscii()
    prepared[headerName] = $request.remotePeerId.get()
  if proxy.config.addRemoteAddrHeader:
    let headerName = proxy.config.remoteAddrHeader.toLowerAscii()
    var existing = prepared.getOrDefault(headerName, "")
    let addrText = remoteMultiaddrText(request)
    if addrText.isSome():
      if existing.len == 0:
        prepared[headerName] = addrText.get()
      else:
        prepared[headerName] = existing & ", " & addrText.get()
  for key, value in proxy.config.extraRequestHeaders.pairs():
    prepared[key.toLowerAscii()] = value
  var headers: seq[(string, string)] = @[]
  for key, value in prepared.pairs():
    headers.add((key, value))
  headers

proc prepareResponseHeaders(resp: HttpClientResponseRef): HttpHeaders =
  var headers = initTable[string, string]()
  for (name, value) in resp.headers.stringItems():
    let lower = name.toLowerAscii()
    case lower
    of "connection", "transfer-encoding", "content-length":
      continue
    else:
      headers[lower] = value
  headers

proc forward(
    proxy: HttpReverseProxy, req: HttpRequest
): Future[HttpResponse] {.async: (raises: [CancelledError]).} =
  if proxy.isNil():
    return plainResponse(500, "HttpReverseProxy not initialized")

  var httpMethod: HttpMethod
  try:
    httpMethod = parseMethod(req.verb)
  except ValueError:
    return plainResponse(405, "Method Not Allowed")

  if httpMethod == MethodConnect:
    return plainResponse(501, "CONNECT tunneling is not supported")

  let upstream = proxy.buildUpstreamUri(req.path)
  let headers = proxy.prepareHeaders(req)
  trace "proxy forwarding request", verb = req.verb, path = req.path, upstream = $upstream

  let reqResult = HttpClientRequestRef.new(
    proxy.session,
    $upstream,
    httpMethod,
    flags = {HttpClientRequestFlag.CloseConnection},
    headers = headers,
    body = req.body,
  )

  if reqResult.isErr():
    trace "proxy failed to build upstream request", upstream = $upstream
    return plainResponse(502, "Bad Gateway: invalid upstream URL")

  var requestRef: HttpClientRequestRef = nil
  var responseRef: HttpClientResponseRef = nil
  try:
    requestRef = reqResult.get()
    responseRef = await requestRef.send()
    let body =
      try:
        await responseRef.getBodyBytes()
      except HttpError as exc:
        trace "proxy failed to read upstream body", err = exc.msg
        return plainResponse(502, "Bad Gateway: upstream read failed")

    let respHeaders = prepareResponseHeaders(responseRef)
    return HttpResponse(
      status: responseRef.status,
      reason: responseRef.reason,
      headers: respHeaders,
      body: body,
    )
  except CancelledError as exc:
    trace "proxy request cancelled", upstream = $upstream
    raise exc
  except HttpError as exc:
    trace "proxy upstream error", err = exc.msg
    return plainResponse(502, "Bad Gateway: upstream request failed")
  finally:
    if not responseRef.isNil():
      await responseRef.closeWait()
    if not requestRef.isNil():
      await requestRef.closeWait()

proc handler*(proxy: HttpReverseProxy): HttpHandler =
  proc proxyHandler(req: HttpRequest): Future[HttpResponse] {.async.} =
    await proxy.forward(req)
  proxyHandler

proc route*(proxy: HttpReverseProxy, path: string): (string, HttpHandler) =
  (path, proxy.handler())
