# Nim-LibP2P
# Copyright (c) 2025
# Licensed under either of
#  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
#  * MIT license ([LICENSE-MIT](LICENSE-MIT))
# at your option.
# This file may not be copied, modified, or distributed except according to
# those terms.

{.push raises: [].}

import std/[algorithm, base64, json, options, strutils, strformat, uri]
import chronos, chronos/apps/http/httpclient, chronicles
import pkg/results

import ./cid
import ./errors
import ./multiaddress
import ./peerid

logScope:
  topics = "libp2p delegatedrouting"

const
  DefaultUserAgent* = "nim-libp2p"
  NdjsonMediaType = "application/x-ndjson"
  JsonMediaType = "application/json"

type
  DelegatedRoutingError* = object of LPError
    status*: int

  DelegatedRoutingClient* = ref object of RootObj
    session: HttpSessionRef
    baseUrl: string
    userAgent: string
    acceptNdjson: bool
    protocolFilter: seq[string]
    addrFilter: seq[string]

  DelegatedProviderRecord* = object
    schema*: string
    peerId*: PeerId
    addresses*: seq[MultiAddress]
    protocols*: seq[string]

  DelegatedRoutingRecord* = object
    key*: seq[byte]
    value*: seq[byte]
    timeReceived*: Option[string]

proc newDelegatedRoutingError(message: string, status: int = 0): ref DelegatedRoutingError =
  (ref DelegatedRoutingError)(msg: message, status: status)

proc normalizeBaseUrl(rawUrl: string): Result[string, string] =
  let trimmed = rawUrl.strip()
  if trimmed.len == 0:
    return err("delegated routing base URL cannot be empty")

  var base = trimmed
  while base.len > 0 and base[^1] == '/':
    base.setLen(base.len - 1)
  if base.len == 0:
    return err("delegated routing base URL cannot be root only")

  if base.endsWith("/routing/v1"):
    base.setLen(base.len - "/routing/v1".len)
    while base.len > 0 and base[^1] == '/':
      base.setLen(base.len - 1)
    if base.len == 0:
      return err("delegated routing base URL cannot resolve to root only")

  let schemePos = base.find("://")
  if schemePos <= 0 or schemePos + 3 >= base.len:
    return err("delegated routing base URL must include scheme")

  if base.find('?', schemePos + 3) >= 0 or base.find('#', schemePos + 3) >= 0:
    return err("delegated routing base URL cannot include query or fragment")

  let pathPos = base.find('/', schemePos + 3)
  if pathPos >= 0:
    return err(
      "delegated routing base URL must not include path segments; pass `/routing/v1` explicitly instead"
    )

  ok(base)

proc normalizeFilters(values: seq[string], lowerAll: bool): seq[string] =
  var items: seq[string] = @[]
  for value in values:
    var v = value.strip()
    if v.len == 0:
      continue
    if lowerAll:
      if v[0] == '!':
        v = "!" & v[1 .. ^1].toLowerAscii()
      else:
        v = v.toLowerAscii()
    items.add(v)
  items.sort()
  result = newSeq[string]()
  for v in items:
    if result.len == 0 or result[result.high] != v:
      result.add(v)

proc buildProvidersEndpoint(client: DelegatedRoutingClient, key: string): string =
  var url = client.baseUrl & "/routing/v1/providers/" & key
  var params: seq[string] = @[]
  if client.protocolFilter.len > 0:
    params.add("filter-protocols=" & client.protocolFilter.join(","))
  if client.addrFilter.len > 0:
    params.add("filter-addrs=" & client.addrFilter.join(","))
  if params.len > 0:
    url.add('?')
    url.add(params.join("&"))
  url

proc buildRecordsEndpoint(
    client: DelegatedRoutingClient, key: seq[byte]
): string =
  let raw = bytesToString(key)
  let encoded = encodeUrl(raw, usePlus = false)
  client.baseUrl & "/routing/v1/records/" & encoded

proc acceptHeader(client: DelegatedRoutingClient): string =
  if client.acceptNdjson:
    NdjsonMediaType & ", " & JsonMediaType
  else:
    JsonMediaType

proc bytesToString(data: seq[byte]): string =
  result = newString(data.len)
  for i, b in data:
    result[i] = char(b)

proc stringToBytes(text: string): seq[byte] =
  result = newSeq[byte](text.len)
  for i, c in text:
    result[i] = byte(c)

proc fieldOrNil(node: JsonNode, field: string): JsonNode =
  if node.kind != JObject:
    return nil
  for key, value in node.pairs():
    if key == field:
      return value
  nil

proc parseProtocols(node: JsonNode, field: string): seq[string] =
  let container = fieldOrNil(node, field)
  if not container.isNil and container.kind == JArray:
    for item in container:
      if item.kind == JString:
        result.add(item.getStr())

proc parseAddresses(node: JsonNode, field: string): seq[MultiAddress] =
  let container = fieldOrNil(node, field)
  if not container.isNil and container.kind == JArray:
    for addrNode in container:
      if addrNode.kind != JString:
        continue
      let addrStr = addrNode.getStr()
      let parsed = MultiAddress.init(addrStr)
      if parsed.isOk:
        result.add(parsed.get())
      else:
        warn "delegated routing provider contains invalid multiaddress",
          address = addrStr, error = parsed.error()

proc parsePeerId(node: JsonNode, field: string): Result[PeerId, string] =
  let fieldNode = fieldOrNil(node, field)
  if fieldNode.isNil or fieldNode.kind != JString:
    return err(&"missing {field} field")
  let text = fieldNode.getStr()
  var pid: PeerId
  if pid.init(text):
    ok(pid)
  else:
    err("invalid peer id: " & text)

proc toProviderRecord(
    schema: string, pid: PeerId, addrs: seq[MultiAddress], protocols: seq[string]
): DelegatedProviderRecord =
  DelegatedProviderRecord(schema: schema, peerId: pid, addresses: addrs, protocols: protocols)

proc parsePeerRecord(node: JsonNode): Result[DelegatedProviderRecord, string] =
  let pid = ?parsePeerId(node, "ID")
  let addrs = parseAddresses(node, "Addrs")
  let protocols = parseProtocols(node, "Protocols")
  ok(toProviderRecord("peer", pid, addrs, protocols))

proc parseBitswapRecord(node: JsonNode): Result[DelegatedProviderRecord, string] =
  let pid = ?parsePeerId(node, "ID")
  var protocols = parseProtocols(node, "Protocols")
  let protocolNode = fieldOrNil(node, "Protocol")
  if protocols.len == 0 and not protocolNode.isNil and protocolNode.kind == JString:
    protocols.add(protocolNode.getStr())
  let addrs = parseAddresses(node, "Addrs")
  ok(toProviderRecord("bitswap", pid, addrs, protocols))

proc parseProviderNode(node: JsonNode): Result[DelegatedProviderRecord, string] =
  if node.kind != JObject:
    return err("provider entry must be an object")
  let schemaNode = fieldOrNil(node, "Schema")
  if schemaNode.isNil or schemaNode.kind != JString:
    return err("provider entry missing Schema")
  let schema = schemaNode.getStr()
  case schema
  of "peer":
    parsePeerRecord(node)
  of "bitswap":
    parseBitswapRecord(node)
  else:
    err("unsupported provider schema: " & schema)

proc parseProvidersFromJson(body: seq[byte]): Result[seq[DelegatedProviderRecord], string] =
  var providers: seq[DelegatedProviderRecord] = @[]
  try:
    let root = parseJson(bytesToString(body))
    if root.kind != JObject:
      return err("response JSON must be an object")
    let list = fieldOrNil(root, "Providers")
    if list.isNil or list.kind != JArray:
      return err("response JSON missing Providers array")
    for item in list:
      let parsed = parseProviderNode(item)
      if parsed.isOk:
        providers.add(parsed.get())
      else:
        warn "failed to decode delegated routing provider entry", error = parsed.error()
    ok(providers)
  except CatchableError as exc:
    err("invalid JSON payload: " & exc.msg)

proc parseProvidersFromNdjson(body: seq[byte]): Result[seq[DelegatedProviderRecord], string] =
  var providers: seq[DelegatedProviderRecord] = @[]
  let text = bytesToString(body)
  for line in text.splitLines():
    let trimmed = line.strip()
    if trimmed.len == 0:
      continue
    try:
      let node = parseJson(trimmed)
      let parsed = parseProviderNode(node)
      if parsed.isOk:
        providers.add(parsed.get())
      else:
        warn "failed to decode delegated routing provider entry", error = parsed.error()
    except CatchableError as exc:
      warn "failed to parse delegated routing NDJSON line", line = trimmed, error = exc.msg
  ok(providers)

proc decodeBase64(value: string, field: string): Result[seq[byte], string] =
  try:
    let decoded = base64.decode(value)
    ok(stringToBytes(decoded))
  except CatchableError as exc:
    err("invalid base64 " & field & ": " & exc.msg)

proc parseRecordPayload(node: JsonNode): Result[DelegatedRoutingRecord, string] =
  if node.isNil:
    return err("record entry missing payload")
  var recordNode = node
  if node.kind == JObject:
    let nested = fieldOrNil(node, "Record")
    if not nested.isNil:
      recordNode = nested
  if recordNode.kind != JObject:
    return err("record entry must be an object")

  let keyNode = fieldOrNil(recordNode, "Key")
  if keyNode.isNil or keyNode.kind != JString:
    return err("record entry missing Key string")
  let valueNode = fieldOrNil(recordNode, "Value")
  if valueNode.isNil or valueNode.kind != JString:
    return err("record entry missing Value string")

  let keyBytes = ?decodeBase64(keyNode.getStr(), "Key")
  let valueBytes = ?decodeBase64(valueNode.getStr(), "Value")

  var timeOpt: Option[string] = none(string)
  let timeNode = fieldOrNil(recordNode, "TimeReceived")
  if not timeNode.isNil and timeNode.kind == JString:
    timeOpt = some(timeNode.getStr())

  ok(DelegatedRoutingRecord(key: keyBytes, value: valueBytes, timeReceived: timeOpt))

proc parseRecordsFromJson(body: seq[byte]): Result[seq[DelegatedRoutingRecord], string] =
  var records: seq[DelegatedRoutingRecord] = @[]
  try:
    let root = parseJson(bytesToString(body))
    case root.kind
    of JObject:
      let listNode = fieldOrNil(root, "Records")
      if not listNode.isNil and listNode.kind == JArray:
        for item in listNode:
          let parsed = parseRecordPayload(item)
          if parsed.isOk:
            records.add(parsed.get())
          else:
            warn "failed to decode delegated routing record entry",
              error = parsed.error()
      else:
        let parsed = parseRecordPayload(root)
        if parsed.isOk:
          records.add(parsed.get())
        else:
          return err(parsed.error())
    of JArray:
      for item in root:
        let parsed = parseRecordPayload(item)
        if parsed.isOk:
          records.add(parsed.get())
        else:
          warn "failed to decode delegated routing record entry",
            error = parsed.error()
    else:
      return err("records JSON payload must be an object or array")
    ok(records)
  except CatchableError as exc:
    err("invalid JSON payload: " & exc.msg)

proc parseRecordsFromNdjson(body: seq[byte]): Result[seq[DelegatedRoutingRecord], string] =
  var records: seq[DelegatedRoutingRecord] = @[]
  let text = bytesToString(body)
  for line in text.splitLines():
    let trimmed = line.strip()
    if trimmed.len == 0:
      continue
    try:
      let node = parseJson(trimmed)
      let parsed = parseRecordPayload(node)
      if parsed.isOk:
        records.add(parsed.get())
      else:
        warn "failed to decode delegated routing record entry",
          error = parsed.error()
    except CatchableError as exc:
      warn "failed to parse delegated routing NDJSON record",
        line = trimmed, error = exc.msg
  ok(records)

proc new*(
    T: typedesc[DelegatedRoutingClient],
    baseUrl: string,
    userAgent: string = DefaultUserAgent,
    acceptNdjson: bool = true,
    protocolFilter: seq[string] = @[],
    addrFilter: seq[string] = @[]
): DelegatedRoutingClient {.raises: [LPError], public.} =
  let normalized = normalizeBaseUrl(baseUrl).valueOr:
    raise newException(LPError, "invalid delegated routing base URL: " & error)
  var ua = userAgent.strip()
  if ua.len == 0:
    ua = DefaultUserAgent

  DelegatedRoutingClient(
    session: HttpSessionRef.new(),
    baseUrl: normalized,
    userAgent: ua,
    acceptNdjson: acceptNdjson,
    protocolFilter: normalizeFilters(protocolFilter, true),
    addrFilter: normalizeFilters(addrFilter, true),
  )

proc isNilOrUnconfigured(client: DelegatedRoutingClient): bool =
  client.isNil or client.session.isNil

proc findProviders*(
    client: DelegatedRoutingClient, key: Cid
): Future[seq[DelegatedProviderRecord]] {.
    async: (raises: [CancelledError, DelegatedRoutingError])
.} =
  if client.isNilOrUnconfigured():
    raise newDelegatedRoutingError("delegated routing client not initialized")

  let endpoint = buildProvidersEndpoint(client, $key)
  let headers = @[
    ("Accept", client.acceptHeader()),
    ("User-Agent", client.userAgent),
  ]

  let request = HttpClientRequestRef.get(client.session, endpoint, headers = headers).valueOr:
    raise newDelegatedRoutingError("failed to construct delegated routing request")

  let response =
    try:
      await request.send()
    except HttpError as exc:
      raise newDelegatedRoutingError("delegated routing request failed: " & exc.msg)

  let status = response.status
  let contentType = response.headers.getString("Content-Type").toLowerAscii()
  let body =
    try:
      await response.getBodyBytes()
    except HttpError as exc:
      raise newDelegatedRoutingError("delegated routing body read failed: " & exc.msg)

  if status == 404:
    trace "delegated routing returned no providers", cid = $key
    return @[]

  if status != 200:
    let message =
      if body.len > 0: bytesToString(body) else: "unexpected status " & $status
    raise newDelegatedRoutingError(message, status)

  let providers =
    if client.acceptNdjson and contentType.contains(NdjsonMediaType):
      parseProvidersFromNdjson(body).valueOr:
        raise newDelegatedRoutingError(error, status)
    elif contentType.contains(JsonMediaType) or contentType.len == 0:
      parseProvidersFromJson(body).valueOr:
        raise newDelegatedRoutingError(error, status)
    else:
      raise newDelegatedRoutingError(
        "unsupported delegated routing content type: " & contentType, status
      )

  providers

proc getRecords*(
    client: DelegatedRoutingClient, key: seq[byte]
): Future[seq[DelegatedRoutingRecord]] {.
    async: (raises: [CancelledError, DelegatedRoutingError])
.} =
  if client.isNilOrUnconfigured():
    raise newDelegatedRoutingError("delegated routing client not initialized")

  let endpoint = buildRecordsEndpoint(client, key)
  let headers = @[
    ("Accept", client.acceptHeader()),
    ("User-Agent", client.userAgent),
  ]

  let request = HttpClientRequestRef.get(client.session, endpoint, headers = headers).valueOr:
    raise newDelegatedRoutingError("failed to construct delegated routing request")

  let response =
    try:
      await request.send()
    except HttpError as exc:
      raise newDelegatedRoutingError("delegated routing request failed: " & exc.msg)

  let status = response.status
  let contentType = response.headers.getString("Content-Type").toLowerAscii()
  let body =
    try:
      await response.getBodyBytes()
    except HttpError as exc:
      raise newDelegatedRoutingError("delegated routing body read failed: " & exc.msg)

  if status == 404:
    trace "delegated routing returned no records"
    return @[]

  if status != 200:
    let message =
      if body.len > 0: bytesToString(body) else: "unexpected status " & $status
    raise newDelegatedRoutingError(message, status)

  let records =
    if client.acceptNdjson and contentType.contains(NdjsonMediaType):
      parseRecordsFromNdjson(body).valueOr:
        raise newDelegatedRoutingError(error, status)
    elif contentType.contains(JsonMediaType) or contentType.len == 0:
      parseRecordsFromJson(body).valueOr:
        raise newDelegatedRoutingError(error, status)
    else:
      raise newDelegatedRoutingError(
        "unsupported delegated routing content type: " & contentType, status
      )

  records
