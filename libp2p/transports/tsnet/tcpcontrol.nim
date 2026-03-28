{.push raises: [].}

import std/[json, net, strutils, uri]

import ../../utility
import ./control

const
  NimTsnetTcpPrefix* = "/nim-tsnet-control-tcp/v1"
  NimTsnetTcpDefaultPort* = 9445'u16

type
  TsnetTcpControlEndpoint* = object
    url*: string
    host*: string
    authority*: string
    port*: uint16
    prefix*: string

proc safeTrace(message: string) =
  try:
    stderr.writeLine(message)
  except IOError:
    discard

proc bytesToString(bytes: openArray[byte]): string =
  result = newString(bytes.len)
  for i, value in bytes:
    result[i] = char(value)

proc defaultTcpPort(parsed: Uri): uint16 =
  if parsed.port.len > 0:
    try:
      return uint16(max(1, parsed.port.parseInt()))
    except CatchableError:
      discard
  NimTsnetTcpDefaultPort

proc bracketAuthorityHost(host: string): string =
  if host.contains(":") and not host.startsWith("["):
    "[" & host & "]"
  else:
    host

proc tcpEndpoint*(controlUrl: string): Result[TsnetTcpControlEndpoint, string] =
  let normalized = normalizeControlUrl(controlUrl)
  if normalized.len == 0:
    return err("tsnet controlUrl is empty")
  let parsed =
    try:
      parseUri(normalized)
    except CatchableError as exc:
      return err("failed to parse tsnet controlUrl " & normalized & ": " & exc.msg)
  if parsed.hostname.len == 0:
    return err("tsnet controlUrl is missing a hostname")
  let port = defaultTcpPort(parsed)
  let authorityHost = bracketAuthorityHost(parsed.hostname)
  let authority = authorityHost & ":" & $port
  ok(TsnetTcpControlEndpoint(
    url: "tcp://" & authority & NimTsnetTcpPrefix,
    host: parsed.hostname,
    authority: authority,
    port: port,
    prefix: NimTsnetTcpPrefix
  ))

proc nimTcpBaseUrl*(controlUrl: string): string =
  let endpoint = tcpEndpoint(controlUrl).valueOr:
    return ""
  endpoint.url

proc nimTcpHealthUrl*(controlUrl: string): string =
  let base = nimTcpBaseUrl(controlUrl)
  if base.len == 0:
    return ""
  base & "/health"

proc nimTcpKeyUrl*(controlUrl: string, capabilityVersion: int): string =
  let base = nimTcpBaseUrl(controlUrl)
  if base.len == 0:
    return ""
  base & "/key?v=" & $capabilityVersion

proc nimTcpRegisterUrl*(controlUrl: string): string =
  let base = nimTcpBaseUrl(controlUrl)
  if base.len == 0:
    return ""
  base & "/register"

proc nimTcpMapUrl*(controlUrl: string): string =
  let base = nimTcpBaseUrl(controlUrl)
  if base.len == 0:
    return ""
  base & "/map"

proc buildRpcRequest(
    route: string,
    capabilityVersion = 0,
    requestPayload: JsonNode = nil
): JsonNode =
  result = %*{
    "version": 1,
    "route": route
  }
  if capabilityVersion > 0:
    result["capabilityVersion"] = %capabilityVersion
  if not requestPayload.isNil and requestPayload.kind != JNull:
    result["request"] = requestPayload

proc responsePayload(payload: JsonNode): Result[JsonNode, string] =
  if payload.isNil or payload.kind != JObject:
    return err("nim_tcp control response must be a JSON object")
  let okNode = payload.getOrDefault("ok")
  let okValue =
    if not okNode.isNil and okNode.kind == JBool: okNode.getBool()
    else: false
  if not okValue:
    let errorNode = payload.getOrDefault("error")
    let message =
      if not errorNode.isNil and errorNode.kind == JString and errorNode.getStr().strip().len > 0:
        errorNode.getStr().strip()
      else:
        "nim_tcp control request failed"
    return err(message)
  let payloadNode =
    if payload.hasKey("payload"): payload.getOrDefault("payload")
    else: newJObject()
  ok(payloadNode)

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

proc readExact(socket: Socket, size: int): Result[string, string] =
  var data = newStringOfCap(size)
  while data.len < size:
    let chunk =
      try:
        socket.recv(size - data.len)
      except CatchableError as exc:
        return err("failed to read nim_tcp frame: " & exc.msg)
    if chunk.len == 0:
      return err("socket closed while reading nim_tcp frame")
    data.add(chunk)
  ok(data)

proc writeFrame(socket: Socket, payload: string): Result[void, string] =
  try:
    socket.send(encodeFrame(payload))
    ok()
  except CatchableError as exc:
    err("failed to write nim_tcp frame: " & exc.msg)

proc readFrame(socket: Socket): Result[string, string] =
  let header = readExact(socket, 4).valueOr:
    return err(error)
  let size = decodeFrameSize(header)
  if size < 0:
    return err("invalid nim_tcp frame header")
  if size == 0:
    return ok("")
  readExact(socket, size)

proc safeClose(socket: Socket) {.gcsafe, raises: [].} =
  if socket.isNil:
    return
  try:
    socket.close()
  except Exception:
    discard

proc executeTcpRequest*(
    controlUrl: string,
    request: JsonNode
): Result[JsonNode, string] {.gcsafe.} =
  let endpoint = tcpEndpoint(controlUrl).valueOr:
    return err(error)

  var socket: Socket = nil
  try:
    let domain =
      if endpoint.host.contains(":"): Domain.AF_INET6
      else: Domain.AF_INET
    socket = newSocket(domain = domain, sockType = SockType.SOCK_STREAM, protocol = Protocol.IPPROTO_TCP)
    socket.connect(endpoint.host, Port(endpoint.port))
    let written = socket.writeFrame($request)
    if written.isErr():
      return err(written.error)
    let rawResponse = socket.readFrame().valueOr:
      return err(error)
    let responseNode =
      try:
        parseJson(rawResponse)
      except CatchableError as exc:
        return err("nim_tcp control response is not valid JSON: " & exc.msg)
    responsePayload(responseNode)
  except CatchableError as exc:
    err("nim_tcp request failed at " & endpoint.url & ": " & exc.msg)
  finally:
    socket.safeClose()

proc fetchControlServerKeyTcp*(
    controlUrl: string,
    capabilityVersion: int
): Result[TsnetControlServerKey, string] {.gcsafe.} =
  let payload = executeTcpRequest(
    controlUrl,
    buildRpcRequest("key", capabilityVersion = capabilityVersion)
  ).valueOr:
    return err(error)
  parseControlServerKey(payload, capabilityVersion = capabilityVersion)

proc fetchControlServerKeyTcpSafe*(
    controlUrl: string
): Result[TsnetControlServerKey, string] {.gcsafe.} =
  safeTrace("[nim_tcp_control] fetch controlUrl=" & controlUrl)
  var failures: seq[string] = @[]
  for version in TsnetDefaultControlCapabilityVersions:
    let fetched = fetchControlServerKeyTcp(controlUrl, version)
    if fetched.isOk():
      return fetched
    failures.add(fetched.error)
  if failures.len == 0:
    return err("no nim_tcp control capability versions were provided")
  err(failures.join("; "))

proc registerNodeTcp*(
    controlUrl: string,
    request: JsonNode
): Result[JsonNode, string] {.gcsafe.} =
  executeTcpRequest(
    controlUrl,
    buildRpcRequest("register", requestPayload = request)
  )

proc mapPollTcp*(
    controlUrl: string,
    request: JsonNode
): Result[JsonNode, string] {.gcsafe.} =
  executeTcpRequest(
    controlUrl,
    buildRpcRequest("map", requestPayload = request)
  )

proc healthTcp*(
    controlUrl: string
): Result[JsonNode, string] {.gcsafe.} =
  executeTcpRequest(
    controlUrl,
    buildRpcRequest("health")
  )

proc tcpControlTransport*(): TsnetControlTransport =
  let noiseUrlProc: TsnetControlUrlProc =
    proc(controlUrl: string): string {.closure, gcsafe, raises: [].} =
      nimTcpKeyUrl(controlUrl, TsnetDefaultControlCapabilityVersions[0])
  let keyUrlProc: TsnetControlVersionedUrlProc =
    proc(controlUrl: string, capabilityVersion: int): string {.closure, gcsafe, raises: [].} =
      nimTcpKeyUrl(controlUrl, capabilityVersion)
  let registerUrlProc: TsnetControlUrlProc =
    proc(controlUrl: string): string {.closure, gcsafe, raises: [].} =
      nimTcpRegisterUrl(controlUrl)
  let mapUrlProc: TsnetControlUrlProc =
    proc(controlUrl: string): string {.closure, gcsafe, raises: [].} =
      nimTcpMapUrl(controlUrl)
  let fetchProc: TsnetControlKeyFetchProc =
    proc(controlUrl: string): Result[TsnetControlServerKey, string] {.closure, gcsafe, raises: [].} =
      fetchControlServerKeyTcpSafe(controlUrl)
  let registerProc: TsnetControlRegisterProc =
    proc(controlUrl: string, request: JsonNode): Result[JsonNode, string] {.closure, gcsafe, raises: [].} =
      registerNodeTcp(controlUrl, request)
  let mapProc: TsnetControlMapPollProc =
    proc(controlUrl: string, request: JsonNode): Result[JsonNode, string] {.closure, gcsafe, raises: [].} =
      mapPollTcp(controlUrl, request)
  TsnetControlTransport.init(
    protocolLabel = "nim_tcp",
    noiseUrlProc = noiseUrlProc,
    keyUrlProc = keyUrlProc,
    registerUrlProc = registerUrlProc,
    mapUrlProc = mapUrlProc,
    fetchServerKeyProc = fetchProc,
    registerNodeProc = registerProc,
    mapPollProc = mapProc
  )
