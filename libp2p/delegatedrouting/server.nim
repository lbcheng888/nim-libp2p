# Nim-LibP2P
# Licensed under either of
#  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
#  * MIT license ([LICENSE-MIT](LICENSE-MIT))
# at your option.

{.push raises: [].}

import std/[base64, json, math, options, strutils, tables, uri]
import chronos, chronicles
import stew/byteutils

import ../cid
import ../multiaddress
import ../peerid
import ../protocols/http/http
import ../delegatedrouting
import ./store

logScope:
  topics = "libp2p delegatedrouting server"

const
  ProvidersPrefix = "/routing/v1/providers/"
  RecordsPrefix = "/routing/v1/records/"
  NdjsonMediaType = "application/x-ndjson"
  JsonMediaType = "application/json"

type
  DelegatedRoutingServer* = ref object
    store*: DelegatedRoutingStore

proc new*(
    _: type DelegatedRoutingServer, store: DelegatedRoutingStore
): DelegatedRoutingServer =
  DelegatedRoutingServer(store: store)

proc errorResponse(status: int, message: string): HttpResponse =
  HttpResponse(
    status: status,
    headers: {"content-type": "text/plain; charset=utf-8"}.toTable(),
    body: message.toBytes(),
  )

proc preferNdjson(req: HttpRequest): bool =
  let accept = req.headers.getOrDefault("accept", "")
  accept.contains("application/x-ndjson") or accept.contains("application/ndjson")

proc encodeProvider(record: DelegatedProviderRecord): JsonNode =
  result = newJObject()
  result["Schema"] = %record.schema
  result["ID"] = %($record.peerId)
  if record.addresses.len > 0:
    var addrs = newJArray()
    for addr in record.addresses:
      addrs.add(%($addr))
    result["Addrs"] = addrs
  if record.protocols.len > 0:
    var protocols = newJArray()
    for proto in record.protocols:
      protocols.add(%proto)
    result["Protocols"] = protocols

proc encodeRecord(record: DelegatedRoutingRecord): JsonNode =
  result = newJObject()
  result["Key"] = %base64.encode(record.key)
  result["Value"] = %base64.encode(record.value)
  if record.timeReceived.isSome:
    result["TimeReceived"] = %record.timeReceived.get()

proc contentType(req: HttpRequest): string =
  let raw = req.headers.getOrDefault("content-type", "")
  for part in raw.split(';'):
    let trimmed = part.strip()
    if trimmed.len > 0:
      return trimmed.toLowerAscii()
  ""

proc isNdjsonType(mediaType: string): bool =
  mediaType.contains("application/x-ndjson") or mediaType.contains("application/ndjson")

proc bytesToString(data: seq[byte]): string =
  result = newString(data.len)
  for i, b in data:
    result[i] = char(b)

proc fieldOrNil(node: JsonNode, field: string): JsonNode =
  if node.kind != JObject:
    return nil
  for key, value in node.pairs():
    if key == field:
      return value
  nil

proc decodeBase64ToBytes(payload: string): Result[seq[byte], string] =
  try:
    let decoded = base64.decode(payload)
    var bytes = newSeq[byte](decoded.len)
    for i, ch in decoded:
      bytes[i] = byte(ch)
    ok(bytes)
  except ValueError as exc:
    err("invalid base64 payload: " & exc.msg)

proc parseDurationString(text: string): Result[Duration, string] =
  let trimmed = text.strip().toLowerAscii()
  if trimmed.len == 0:
    return err("ttl string is empty")

  const units = [
    ("ns", 1.0),
    ("us", 1_000.0),
    ("ms", 1_000_000.0),
    ("s", 1_000_000_000.0),
    ("m", 60.0 * 1_000_000_000.0),
    ("h", 3600.0 * 1_000_000_000.0),
  ]

  var numberPart = trimmed
  var multiplier = 1_000_000_000.0
  for unit in units:
    if trimmed.endsWith(unit[0]):
      numberPart = trimmed[0 ..< trimmed.len - unit[0].len].strip()
      multiplier = unit[1]
      break

  if numberPart.len == 0:
    return err("ttl numeric portion is empty")

  let value =
    try:
      parseFloat(numberPart)
    except ValueError:
      return err("invalid ttl numeric portion: " & numberPart)

  if value <= 0.0:
    return ok(chronos.ZeroDuration)

  let nanosFloat = value * multiplier
  if nanosFloat > float64(high(int64)):
    return err("ttl exceeds supported range")

  let nanos = int64(round(nanosFloat))
  if nanos <= 0:
    return ok(chronos.ZeroDuration)
  ok(chronos.nanoseconds(nanos))

proc parseTtlField(
    node: JsonNode, field: string, defaultTtl: Duration
): Result[Duration, string] =
  let ttlNode = fieldOrNil(node, field)
  if ttlNode.isNil or ttlNode.kind == JNull:
    return ok(defaultTtl)

  case ttlNode.kind
  of JInt:
    let secs = ttlNode.getInt()
    if secs <= 0:
      ok(defaultTtl)
    else:
      if int64(secs) > high(int64) div 1_000_000_000'i64:
        err("ttl value overflow")
      else:
        ok(chronos.nanoseconds(int64(secs) * 1_000_000_000'i64))
  of JFloat:
    let secs = ttlNode.getFloat()
    if secs <= 0.0:
      ok(defaultTtl)
    else:
      let nanosFloat = secs * 1_000_000_000.0
      if nanosFloat > float64(high(int64)):
        err("ttl value overflow")
      else:
        let nanos = int64(round(nanosFloat))
        if nanos <= 0:
          ok(defaultTtl)
        else:
          ok(chronos.nanoseconds(nanos))
  of JString:
    let parsed = parseDurationString(ttlNode.getStr()).valueOr:
      return err(error)
    if parsed <= chronos.ZeroDuration:
      ok(defaultTtl)
    else:
      ok(parsed)
  else:
    err("unsupported ttl field type")

proc sanitizeTtl(value, default: Duration): Duration =
  if value <= chronos.ZeroDuration:
    default
  else:
    value

proc parsePeerIdField(node: JsonNode): Result[PeerId, string] =
  let idNode = fieldOrNil(node, "ID")
  if idNode.isNil or idNode.kind != JString:
    return err("missing ID field")
  var pid: PeerId
  if pid.init(idNode.getStr()):
    ok(pid)
  else:
    err("invalid peer id: " & idNode.getStr())

proc parseStringArrayField(node: JsonNode, field: string): Result[seq[string], string] =
  let fieldNode = fieldOrNil(node, field)
  if fieldNode.isNil:
    return ok(newSeq[string]())
  if fieldNode.kind != JArray:
    return err(field & " field must be an array of strings")
  var values: seq[string] = @[]
  for item in fieldNode:
    if item.kind != JString:
      return err(field & " field must be an array of strings")
    let text = item.getStr().strip()
    if text.len > 0:
      values.add(text)
  ok(values)

proc parseMultiaddrArrayField(
    node: JsonNode, field: string
): Result[seq[MultiAddress], string] =
  let fieldNode = fieldOrNil(node, field)
  if fieldNode.isNil:
    return ok(newSeq[MultiAddress]())
  if fieldNode.kind != JArray:
    return err(field & " field must be an array of multiaddresses")
  var values: seq[MultiAddress] = @[]
  for item in fieldNode:
    if item.kind != JString:
      return err(field & " field must be an array of multiaddresses")
    let maRes = MultiAddress.init(item.getStr())
    if maRes.isErr:
      return err("invalid multiaddress: " & maRes.error())
    values.add(maRes.get())
  ok(values)

proc parseProviderEntry(
    node: JsonNode, defaultTtl: Duration
): Result[(DelegatedProviderRecord, Duration), string] =
  if node.isNil or node.kind != JObject:
    return err("provider entry must be an object")

  var schema = "peer"
  let schemaNode = fieldOrNil(node, "Schema")
  if not schemaNode.isNil and schemaNode.kind == JString:
    let trimmed = schemaNode.getStr().strip()
    if trimmed.len > 0:
      schema = trimmed
  let normalized = schema.toLowerAscii()

  let ttlValue = parseTtlField(node, "TTL", defaultTtl).valueOr:
    return err(error)
  let ttl = sanitizeTtl(ttlValue, defaultTtl)

  let peerId = parsePeerIdField(node).valueOr:
    return err(error)
  let addresses = parseMultiaddrArrayField(node, "Addrs").valueOr:
    return err(error)
  var protocols = parseStringArrayField(node, "Protocols").valueOr:
    return err(error)

  if normalized == "bitswap" and protocols.len == 0:
    let protoNode = fieldOrNil(node, "Protocol")
    if not protoNode.isNil and protoNode.kind == JString:
      let text = protoNode.getStr().strip()
      if text.len > 0:
        protocols.add(text)

  if normalized != "peer" and normalized != "bitswap":
    return err("unsupported provider schema: " & schema)

  let record = DelegatedProviderRecord(
    schema: schema,
    peerId: peerId,
    addresses: addresses,
    protocols: protocols,
  )
  ok((record, ttl))

proc parseRecordEntry(
    node: JsonNode, key: seq[byte], defaultTtl: Duration
): Result[(DelegatedRoutingRecord, Duration), string] =
  if node.isNil or node.kind != JObject:
    return err("record entry must be an object")

  let valueNode = fieldOrNil(node, "Value")
  if valueNode.isNil or valueNode.kind != JString:
    return err("missing Value field")
  let valueBytes = decodeBase64ToBytes(valueNode.getStr()).valueOr:
    return err(error)

  var timeOpt = none(string)
  let timeNode = fieldOrNil(node, "TimeReceived")
  if not timeNode.isNil:
    if timeNode.kind == JString:
      let text = timeNode.getStr().strip()
      if text.len > 0:
        timeOpt = some(text)
    elif timeNode.kind != JNull:
      return err("TimeReceived field must be string or null")

  let ttlValue = parseTtlField(node, "TTL", defaultTtl).valueOr:
    return err(error)
  let ttl = sanitizeTtl(ttlValue, defaultTtl)

  let record = DelegatedRoutingRecord(key: key, value: valueBytes, timeReceived: timeOpt)
  ok((record, ttl))

proc parseNdjsonObjects(payload: string): Result[seq[JsonNode], string] =
  var items: seq[JsonNode] = @[]
  for rawLine in payload.splitLines():
    let line = rawLine.strip()
    if line.len == 0:
      continue
    try:
      items.add(parseJson(line))
    except CatchableError as exc:
      return err("failed to parse ndjson line: " & exc.msg)
  if items.len == 0:
    return err("ndjson payload is empty")
  ok(items)

proc parseJsonCollection(
    payload: string, fieldName: string
): Result[seq[JsonNode], string] =
  var root: JsonNode
  try:
    root = parseJson(payload)
  except CatchableError as exc:
    return err("failed to parse json payload: " & exc.msg)

  case root.kind
  of JArray:
    var items: seq[JsonNode] = @[]
    for entry in root:
      items.add(entry)
    ok(items)
  of JObject:
    let container = fieldOrNil(root, fieldName)
    if container.isNil:
      return err("missing `" & fieldName & "` field in json payload")
    if container.kind != JArray:
      return err("`" & fieldName & "` field must be an array")
    var items: seq[JsonNode] = @[]
    for entry in container:
      items.add(entry)
    ok(items)
  else:
    err("json payload must be an array or object with `" & fieldName & "` array")

proc ingestProviders(
    server: DelegatedRoutingServer, key: string, req: HttpRequest
): Future[HttpResponse] {.async: (raises: [CancelledError]).} =
  if req.body.len == 0:
    return errorResponse(400, "empty provider payload")

  let mediaType = req.contentType()
  let payload = bytesToString(req.body)
  let nodes = block:
    if isNdjsonType(mediaType):
      parseNdjsonObjects(payload)
    else:
      parseJsonCollection(payload, "Providers")
  let objects = nodes.valueOr:
    return errorResponse(400, error)

  var entries: seq[(DelegatedProviderRecord, Duration)] = @[]
  for node in objects:
    let parsed = parseProviderEntry(node, server.store.providerTtl).valueOr:
      return errorResponse(400, error)
    entries.add(parsed)

  if entries.len == 0:
    return errorResponse(400, "no provider entries found")

  for (record, ttl) in entries:
    await server.store.addProvider(key, record, ttl)

  info "ingested delegated providers", cid = key, count = entries.len
  HttpResponse(
    status: 202,
    headers: {"content-type": "text/plain; charset=utf-8"}.toTable(),
    body: "accepted".toBytes(),
  )

proc ingestRecords(
    server: DelegatedRoutingServer, key: seq[byte], req: HttpRequest
): Future[HttpResponse] {.async: (raises: [CancelledError]).} =
  if req.body.len == 0:
    return errorResponse(400, "empty record payload")

  let mediaType = req.contentType()
  let payload = bytesToString(req.body)
  let nodes = block:
    if isNdjsonType(mediaType):
      parseNdjsonObjects(payload)
    else:
      parseJsonCollection(payload, "Records")
  let objects = nodes.valueOr:
    return errorResponse(400, error)

  var entries: seq[(DelegatedRoutingRecord, Duration)] = @[]
  for node in objects:
    let parsed = parseRecordEntry(node, key, server.store.recordTtl).valueOr:
      return errorResponse(400, error)
    entries.add(parsed)

  if entries.len == 0:
    return errorResponse(400, "no record entries found")

  for (record, ttl) in entries:
    await server.store.addRecord(record, ttl)

  info "ingested delegated records", count = entries.len
  HttpResponse(
    status: 202,
    headers: {"content-type": "text/plain; charset=utf-8"}.toTable(),
    body: "accepted".toBytes(),
  )

proc handleProviders(
    server: DelegatedRoutingServer, req: HttpRequest
): Future[HttpResponse] {.async: (raises: [CancelledError]).} =
  if server.isNil or server.store.isNil:
    return errorResponse(503, "delegated routing not configured")

  if not req.path.startsWith(ProvidersPrefix):
    return errorResponse(404, "not found")

  var suffix = req.path[ProvidersPrefix.len .. ^1]
  let queryPos = suffix.find('?')
  if queryPos >= 0:
    suffix = suffix[0 ..< queryPos]
  if suffix.len == 0:
    return errorResponse(400, "missing provider identifier")

  discard Cid.init(suffix).valueOr:
    return errorResponse(400, "invalid cid: " & $error)

  let verb = req.verb.toUpperAscii()
  if verb == "POST":
    return await server.ingestProviders(suffix, req)
  if verb != "GET":
    return errorResponse(405, "method not allowed")

  let providers = await server.store.getProviders(suffix)
  if providers.len == 0:
    return errorResponse(404, "no providers found")

  if req.preferNdjson():
    var lines: seq[string] = @[]
    for provider in providers:
      lines.add($provider.encodeProvider())
    let payload = lines.join("\n").toBytes()
    return HttpResponse(
      status: 200,
      headers: {"content-type": NdjsonMediaType}.toTable(),
      body: payload,
    )

  var providerArray = newJArray()
  for provider in providers:
    providerArray.add(provider.encodeProvider())
  let root = %*{"Providers": providerArray}
  return HttpResponse(
    status: 200,
    headers: {"content-type": JsonMediaType}.toTable(),
    body: ($root).toBytes(),
  )

proc decodeRecordKey(path: string): Result[seq[byte], string] =
  var suffix = path[RecordsPrefix.len .. ^1]
  let queryPos = suffix.find('?')
  if queryPos >= 0:
    suffix = suffix[0 ..< queryPos]
  if suffix.len == 0:
    return err("missing key")
  try:
    let decoded = decodeUrl(suffix, decodePlus = false)
    var bytes = newSeq[byte](decoded.len)
    for i, ch in decoded:
      bytes[i] = byte(ch)
    ok(bytes)
  except ValueError as exc:
    err("failed to decode key: " & exc.msg)

proc handleRecords(
    server: DelegatedRoutingServer, req: HttpRequest
): Future[HttpResponse] {.async: (raises: [CancelledError]).} =
  if server.isNil or server.store.isNil:
    return errorResponse(503, "delegated routing not configured")

  if not req.path.startsWith(RecordsPrefix):
    return errorResponse(404, "not found")

  let keyBytes = decodeRecordKey(req.path).valueOr:
    return errorResponse(400, error)

  let verb = req.verb.toUpperAscii()
  if verb == "POST":
    return await server.ingestRecords(keyBytes, req)
  if verb != "GET":
    return errorResponse(405, "method not allowed")

  let records = await server.store.getRecords(keyBytes)
  if records.len == 0:
    return errorResponse(404, "no records found")

  if req.preferNdjson():
    var lines: seq[string] = @[]
    for record in records:
      lines.add($record.encodeRecord())
    let payload = lines.join("\n").toBytes()
    return HttpResponse(
      status: 200,
      headers: {"content-type": NdjsonMediaType}.toTable(),
      body: payload,
    )

  var recordArray = newJArray()
  for record in records:
    recordArray.add(record.encodeRecord())
  let root = %*{"Records": recordArray}
  HttpResponse(
    status: 200,
    headers: {"content-type": JsonMediaType}.toTable(),
    body: ($root).toBytes(),
  )

proc registerRoutes*(
    server: DelegatedRoutingServer, svc: HttpService
) {.raises: [Defect].} =
  if server.isNil or svc.isNil:
    raise newException(Defect, "delegated routing server requires http service")
  svc.registerRoute(ProvidersPrefix, proc(req: HttpRequest): Future[HttpResponse] {.async.} =
    return await server.handleProviders(req)
  )
  svc.registerRoute(RecordsPrefix, proc(req: HttpRequest): Future[HttpResponse] {.async.} =
    return await server.handleRecords(req)
  )

{.pop.}
