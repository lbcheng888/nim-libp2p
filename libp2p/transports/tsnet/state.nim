{.push raises: [].}

import std/[json, os, parsejson, streams, strutils, times, sequtils]
import nimcrypto/sysrand

import ../../crypto/curve25519
import ../../utility

type
  TsnetStoredPeer* = object
    nodeId*: string
    hostName*: string
    dnsName*: string
    relay*: string
    tailscaleIPs*: seq[string]
    allowedIPs*: seq[string]
    lastHandshakeUnixMilli*: int64

  TsnetStoredState* = object
    schemaVersion*: int
    machineKey*: string
    nodeKey*: string
    wgKey*: string
    machinePublicKey*: string
    nodePublicKey*: string
    discoPublicKey*: string
    controlPublicKey*: string
    controlLegacyPublicKey*: string
    hostname*: string
    controlUrl*: string
    nodeId*: string
    userLogin*: string
    homeDerp*: string
    tailnetIPs*: seq[string]
    peerCache*: seq[TsnetStoredPeer]
    mapSessionHandle*: string
    mapSessionSeq*: int64
    lastControlSuccessUnixMilli*: int64
    lastRegisterAttemptUnixMilli*: int64
    lastMapPollAttemptUnixMilli*: int64
    lastControlBootstrapError*: string
    createdAtUnixMilli*: int64
    updatedAtUnixMilli*: int64

const
  TsnetStoredStateFilename* = "nim-tsnet-state.json"
  CurrentTsnetStoredStateSchemaVersion* = 1
  TsnetCurvePrivateKeyPrefix* = "privkey:"
  TsnetMachineKeyPrefix* = TsnetCurvePrivateKeyPrefix
  TsnetNodeKeyPrefix* = TsnetCurvePrivateKeyPrefix
  TsnetWireGuardKeyPrefix* = TsnetCurvePrivateKeyPrefix
  TsnetMachinePublicKeyPrefix* = "mkey:"
  TsnetNodePublicKeyPrefix* = "nodekey:"
  TsnetDiscoPublicKeyPrefix* = "discokey:"

proc normalizeState(state: var TsnetStoredState)
proc syncDerivedPublicKeys*(state: var TsnetStoredState): Result[bool, string]

proc nowUnixMilli(): int64 =
  getTime().toUnix().int64 * 1000

proc jsonString(node: JsonNode, key: string, defaultValue = ""): string =
  if node.isNil or node.kind != JObject or not node.hasKey(key):
    return defaultValue
  let value = node.getOrDefault(key)
  if value.kind == JString:
    return value.getStr()
  defaultValue

proc jsonInt64(node: JsonNode, key: string, defaultValue = 0'i64): int64 =
  if node.isNil or node.kind != JObject or not node.hasKey(key):
    return defaultValue
  let value = node.getOrDefault(key)
  case value.kind
  of JInt:
    value.getBiggestInt().int64
  of JFloat:
    value.getFloat().int64
  else:
    defaultValue

proc jsonStringSeq(node: JsonNode, key: string): seq[string] =
  if node.isNil or node.kind != JObject or not node.hasKey(key):
    return @[]
  let value = node.getOrDefault(key)
  if value.kind != JArray:
    return @[]
  for item in value.items():
    if item.kind == JString:
      result.add(item.getStr())

proc decodeHexNibble(ch: char): int =
  case ch
  of '0' .. '9':
    ord(ch) - ord('0')
  of 'a' .. 'f':
    ord(ch) - ord('a') + 10
  of 'A' .. 'F':
    ord(ch) - ord('A') + 10
  else:
    -1

proc decodeHexBytes(text: string, expectedBytes: int): Result[seq[byte], string] =
  let raw = text.strip()
  if raw.len != expectedBytes * 2:
    return err(
      "tsnet key material has invalid hex length " & $raw.len &
      ", expected " & $(expectedBytes * 2)
    )
  result = ok(newSeq[byte](expectedBytes))
  for i in 0 ..< expectedBytes:
    let hi = decodeHexNibble(raw[i * 2])
    let lo = decodeHexNibble(raw[i * 2 + 1])
    if hi < 0 or lo < 0:
      return err("tsnet key material contains invalid hex")
    result.get()[i] = byte((hi shl 4) or lo)

proc encodeHexBytes(bytes: openArray[byte]): string =
  result = newStringOfCap(bytes.len * 2)
  for value in bytes:
    result.add(toHex(value, 2))

proc clampCurve25519Private(raw: var Curve25519Key) =
  raw[0] = raw[0] and 0xF8'u8
  raw[31] = raw[31] and 0x7F'u8
  raw[31] = raw[31] or 0x40'u8

proc prefixedCurve25519Private(raw: Curve25519Key): string =
  TsnetCurvePrivateKeyPrefix & encodeHexBytes(raw)

proc prefixedCurve25519Public(raw: Curve25519Key, publicPrefix: string): string =
  publicPrefix & encodeHexBytes(raw)

proc parseCurve25519Private(text: string): Result[Curve25519Key, string] =
  let value = text.strip()
  if not value.startsWith(TsnetCurvePrivateKeyPrefix):
    return err("tsnet key material is not a typed Curve25519 private key")
  let decoded = decodeHexBytes(
    value.substr(TsnetCurvePrivateKeyPrefix.len),
    Curve25519KeySize
  ).valueOr:
    return err(error)
  ok(intoCurve25519Key(decoded))

proc deriveCurve25519Public(
    privateText: string,
    publicPrefix: string
): Result[string, string] =
  let privateKey = parseCurve25519Private(privateText).valueOr:
    return err(error)
  ok(prefixedCurve25519Public(curve25519.public(privateKey), publicPrefix))

proc toJson*(peer: TsnetStoredPeer): JsonNode =
  result = newJObject()
  result["nodeId"] = %peer.nodeId
  result["hostName"] = %peer.hostName
  result["dnsName"] = %peer.dnsName
  result["relay"] = %peer.relay
  result["tailscaleIPs"] = %peer.tailscaleIPs
  result["allowedIPs"] = %peer.allowedIPs
  result["lastHandshakeUnixMilli"] = %peer.lastHandshakeUnixMilli

proc parseStoredPeer(node: JsonNode): TsnetStoredPeer =
  TsnetStoredPeer(
    nodeId: jsonString(node, "nodeId"),
    hostName: jsonString(node, "hostName"),
    dnsName: jsonString(node, "dnsName"),
    relay: jsonString(node, "relay"),
    tailscaleIPs: jsonStringSeq(node, "tailscaleIPs"),
    allowedIPs: jsonStringSeq(node, "allowedIPs"),
    lastHandshakeUnixMilli: jsonInt64(node, "lastHandshakeUnixMilli")
  )

proc parserLocation(parser: JsonParser): string =
  parser.getFilename() & "(" & $parser.getLine() & ", " & $parser.getColumn() & ")"

proc parserFailure(parser: JsonParser): string =
  try:
    parser.errorMsg()
  except CatchableError:
    parserLocation(parser) & " Error: invalid JSON"

template valueOr[E](self: Result[void, E], body: untyped): bool =
  if self.isErr():
    let error {.inject.} = self.error
    body
    false
  else:
    true

proc parserNext(parser: var JsonParser): Result[void, string] =
  try:
    parser.next()
    ok()
  except CatchableError as exc:
    err(parserLocation(parser) & " Error: failed to read JSON: " & exc.msg)

proc parserStringValue(parser: var JsonParser): Result[string, string] =
  try:
    ok(parser.str())
  except CatchableError as exc:
    err(parserLocation(parser) & " Error: failed to read JSON string: " & exc.msg)

template requireParserNext(parser: var JsonParser): untyped =
  block:
    let parserNextResult = parserNext(parser)
    if parserNextResult.isErr():
      return err(parserNextResult.error)

template requireOk(step: untyped): untyped =
  let stepResult {.inject.} = step
  if stepResult.isErr():
    return err(stepResult.error)

proc parserError(parser: JsonParser, msg: string): string =
  if parser.kind == jsonError:
    return parserFailure(parser)
  parserLocation(parser) & " Error: " & msg

proc skipCurrentJsonValue(parser: var JsonParser): Result[void, string] =
  case parser.kind
  of jsonString, jsonInt, jsonFloat, jsonTrue, jsonFalse, jsonNull:
    ok()
  of jsonObjectStart, jsonArrayStart:
    var depth = 1
    while depth > 0:
      requireParserNext(parser)
      case parser.kind
      of jsonError:
        return err(parserFailure(parser))
      of jsonEof:
        return err(parserError(parser, "unexpected EOF while skipping JSON value"))
      of jsonObjectStart, jsonArrayStart:
        inc depth
      of jsonObjectEnd, jsonArrayEnd:
        dec depth
      else:
        discard
    ok()
  else:
    err(parserError(parser, "expected JSON value"))

proc assignJsonStringField(
    parser: var JsonParser,
    target: var string
): Result[void, string] =
  case parser.kind
  of jsonString:
    target = parser.parserStringValue().valueOr:
      return err(error)
    ok()
  of jsonObjectStart:
    skipCurrentJsonValue(parser)
  else:
    ok()

proc assignJsonInt64Field(
    parser: var JsonParser,
    target: var int64
): Result[void, string] =
  case parser.kind
  of jsonInt:
    try:
      target = parser.getInt().int64
      ok()
    except CatchableError:
      err(parserError(parser, "invalid integer value"))
  of jsonFloat:
    try:
      target = parser.getFloat().int64
      ok()
    except CatchableError:
      err(parserError(parser, "invalid float value"))
  of jsonObjectStart:
    skipCurrentJsonValue(parser)
  else:
    ok()

proc parseJsonStringArray(
    parser: var JsonParser,
    target: var seq[string]
): Result[void, string] =
  target = @[]
  case parser.kind
  of jsonArrayStart:
    requireParserNext(parser)
    while parser.kind != jsonArrayEnd:
      case parser.kind
      of jsonError:
        return err(parserFailure(parser))
      of jsonEof:
        return err(parserError(parser, "unexpected EOF while parsing string array"))
      of jsonString:
        let item = parser.parserStringValue().valueOr:
          return err(error)
        target.add(item)
      of jsonObjectStart, jsonArrayStart:
        requireOk(skipCurrentJsonValue(parser))
      else:
        discard
      requireParserNext(parser)
    ok()
  of jsonObjectStart:
    skipCurrentJsonValue(parser)
  else:
    ok()

proc parseStoredPeerFromParser(parser: var JsonParser): Result[TsnetStoredPeer, string] =
  if parser.kind != jsonObjectStart:
    return err(parserError(parser, "tsnet state peerCache entries must be objects"))
  var peer: TsnetStoredPeer
  requireParserNext(parser)
  while parser.kind != jsonObjectEnd:
    case parser.kind
    of jsonError:
      return err(parserFailure(parser))
    of jsonEof:
      return err(parserError(parser, "unexpected EOF while parsing peerCache entry"))
    of jsonString:
      let key = parser.parserStringValue().valueOr:
        return err(error)
      requireParserNext(parser)
      case key
      of "nodeId":
        requireOk(assignJsonStringField(parser, peer.nodeId))
      of "hostName":
        requireOk(assignJsonStringField(parser, peer.hostName))
      of "dnsName":
        requireOk(assignJsonStringField(parser, peer.dnsName))
      of "relay":
        requireOk(assignJsonStringField(parser, peer.relay))
      of "tailscaleIPs":
        requireOk(parseJsonStringArray(parser, peer.tailscaleIPs))
      of "allowedIPs":
        requireOk(parseJsonStringArray(parser, peer.allowedIPs))
      of "lastHandshakeUnixMilli":
        requireOk(assignJsonInt64Field(parser, peer.lastHandshakeUnixMilli))
      else:
        requireOk(skipCurrentJsonValue(parser))
      requireParserNext(parser)
    else:
      return err(parserError(parser, "tsnet state peerCache entry field name must be a string"))
  ok(peer)

proc parseStoredPeerCache(
    parser: var JsonParser,
    target: var seq[TsnetStoredPeer]
): Result[void, string] =
  if parser.kind != jsonArrayStart:
    return err(parserError(parser, "tsnet state peerCache must be an array"))
  target = @[]
  requireParserNext(parser)
  while parser.kind != jsonArrayEnd:
    case parser.kind
    of jsonError:
      return err(parserFailure(parser))
    of jsonEof:
      return err(parserError(parser, "unexpected EOF while parsing peerCache"))
    of jsonObjectStart:
      let peer = parseStoredPeerFromParser(parser).valueOr:
        return err(error)
      target.add(peer)
      requireParserNext(parser)
    else:
      return err(parserError(parser, "tsnet state peerCache entries must be objects"))
  ok()

proc parseStoredState*(
    payloadText: string,
    source = "input"
): Result[TsnetStoredState, string] =
  var input = newStringStream(payloadText)
  var parser: JsonParser
  try:
    parser.open(input, source)
  except CatchableError as exc:
    return err(source & " Error: failed to open JSON parser: " & exc.msg)
  defer:
    try:
      parser.close()
    except CatchableError:
      discard

  requireParserNext(parser)
  if parser.kind == jsonError:
    return err(parserFailure(parser))
  if parser.kind != jsonObjectStart:
    return err(parserError(parser, "tsnet state file must contain a JSON object"))

  var state: TsnetStoredState
  requireParserNext(parser)
  while parser.kind != jsonObjectEnd:
    case parser.kind
    of jsonError:
      return err(parserFailure(parser))
    of jsonEof:
      return err(parserError(parser, "unexpected EOF while parsing tsnet state"))
    of jsonString:
      let key = parser.parserStringValue().valueOr:
        return err(error)
      requireParserNext(parser)
      case key
      of "schemaVersion":
        var schemaVersion = state.schemaVersion.int64
        requireOk(assignJsonInt64Field(parser, schemaVersion))
        state.schemaVersion = schemaVersion.int
      of "machineKey":
        requireOk(assignJsonStringField(parser, state.machineKey))
      of "nodeKey":
        requireOk(assignJsonStringField(parser, state.nodeKey))
      of "wgKey":
        requireOk(assignJsonStringField(parser, state.wgKey))
      of "machinePublicKey":
        requireOk(assignJsonStringField(parser, state.machinePublicKey))
      of "nodePublicKey":
        requireOk(assignJsonStringField(parser, state.nodePublicKey))
      of "discoPublicKey":
        requireOk(assignJsonStringField(parser, state.discoPublicKey))
      of "controlPublicKey":
        requireOk(assignJsonStringField(parser, state.controlPublicKey))
      of "controlLegacyPublicKey":
        requireOk(assignJsonStringField(parser, state.controlLegacyPublicKey))
      of "hostname":
        requireOk(assignJsonStringField(parser, state.hostname))
      of "controlUrl":
        requireOk(assignJsonStringField(parser, state.controlUrl))
      of "nodeId":
        requireOk(assignJsonStringField(parser, state.nodeId))
      of "userLogin":
        requireOk(assignJsonStringField(parser, state.userLogin))
      of "homeDerp":
        requireOk(assignJsonStringField(parser, state.homeDerp))
      of "tailnetIPs":
        requireOk(parseJsonStringArray(parser, state.tailnetIPs))
      of "peerCache":
        requireOk(parseStoredPeerCache(parser, state.peerCache))
      of "mapSessionHandle":
        requireOk(assignJsonStringField(parser, state.mapSessionHandle))
      of "mapSessionSeq":
        requireOk(assignJsonInt64Field(parser, state.mapSessionSeq))
      of "lastControlSuccessUnixMilli":
        requireOk(assignJsonInt64Field(parser, state.lastControlSuccessUnixMilli))
      of "lastRegisterAttemptUnixMilli":
        requireOk(assignJsonInt64Field(parser, state.lastRegisterAttemptUnixMilli))
      of "lastMapPollAttemptUnixMilli":
        requireOk(assignJsonInt64Field(parser, state.lastMapPollAttemptUnixMilli))
      of "lastControlBootstrapError":
        requireOk(assignJsonStringField(parser, state.lastControlBootstrapError))
      of "createdAtUnixMilli":
        requireOk(assignJsonInt64Field(parser, state.createdAtUnixMilli))
      of "updatedAtUnixMilli":
        requireOk(assignJsonInt64Field(parser, state.updatedAtUnixMilli))
      else:
        requireOk(skipCurrentJsonValue(parser))
      requireParserNext(parser)
    else:
      return err(parserError(parser, "tsnet state field name must be a string"))

  requireParserNext(parser)
  if parser.kind == jsonError:
    return err(parserFailure(parser))
  if parser.kind != jsonEof:
    return err(parserError(parser, "unexpected trailing JSON content"))

  normalizeState(state)
  discard syncDerivedPublicKeys(state)
  ok(state)

proc init*(
    _: type[TsnetStoredState],
    hostname = "",
    controlUrl = ""
): TsnetStoredState =
  let now = nowUnixMilli()
  TsnetStoredState(
    schemaVersion: CurrentTsnetStoredStateSchemaVersion,
    machineKey: "",
    nodeKey: "",
    wgKey: "",
    machinePublicKey: "",
    nodePublicKey: "",
    discoPublicKey: "",
    controlPublicKey: "",
    controlLegacyPublicKey: "",
    hostname: hostname,
    controlUrl: controlUrl,
    nodeId: "",
    userLogin: "",
    homeDerp: "",
    tailnetIPs: @[],
    peerCache: @[],
    mapSessionHandle: "",
    mapSessionSeq: 0,
    lastControlSuccessUnixMilli: 0,
    lastRegisterAttemptUnixMilli: 0,
    lastMapPollAttemptUnixMilli: 0,
    lastControlBootstrapError: "",
    createdAtUnixMilli: now,
    updatedAtUnixMilli: now
  )

proc normalizeState(state: var TsnetStoredState) =
  let now = nowUnixMilli()
  if state.schemaVersion <= 0:
    state.schemaVersion = CurrentTsnetStoredStateSchemaVersion
  if state.createdAtUnixMilli <= 0:
    if state.updatedAtUnixMilli > 0:
      state.createdAtUnixMilli = state.updatedAtUnixMilli
    else:
      state.createdAtUnixMilli = now
  if state.updatedAtUnixMilli <= 0:
    state.updatedAtUnixMilli = state.createdAtUnixMilli

proc generateCurve25519Private(): Result[string, string] =
  var raw: Curve25519Key
  if randomBytes(raw) != raw.len:
    return err("failed to generate tsnet key material from system RNG")
  clampCurve25519Private(raw)
  ok(prefixedCurve25519Private(raw))

proc syncDerivedPublicKeys*(state: var TsnetStoredState): Result[bool, string] =
  normalizeState(state)
  var changed = false
  if state.machineKey.strip().len > 0:
    let machinePublic = deriveCurve25519Public(
      state.machineKey,
      TsnetMachinePublicKeyPrefix
    )
    if machinePublic.isOk() and state.machinePublicKey != machinePublic.get():
      state.machinePublicKey = machinePublic.get()
      changed = true
  if state.nodeKey.strip().len > 0:
    let nodePublic = deriveCurve25519Public(
      state.nodeKey,
      TsnetNodePublicKeyPrefix
    )
    if nodePublic.isOk() and state.nodePublicKey != nodePublic.get():
      state.nodePublicKey = nodePublic.get()
      changed = true
  if state.wgKey.strip().len > 0:
    let discoPublic = deriveCurve25519Public(
      state.wgKey,
      TsnetDiscoPublicKeyPrefix
    )
    if discoPublic.isOk() and state.discoPublicKey != discoPublic.get():
      state.discoPublicKey = discoPublic.get()
      changed = true
  ok(changed)

proc ensureIdentityKeys*(state: var TsnetStoredState): Result[bool, string] =
  normalizeState(state)
  var changed = false
  if state.machineKey.strip().len == 0:
    state.machineKey = generateCurve25519Private().valueOr:
      return err(error)
    changed = true
  if state.nodeKey.strip().len == 0:
    state.nodeKey = generateCurve25519Private().valueOr:
      return err(error)
    changed = true
  if state.wgKey.strip().len == 0:
    state.wgKey = generateCurve25519Private().valueOr:
      return err(error)
    changed = true
  let derived = syncDerivedPublicKeys(state).valueOr:
    return err(error)
  changed = changed or derived
  ok(changed)

proc toJson*(state: var TsnetStoredState): JsonNode =
  result = newJObject()
  result["schemaVersion"] = %state.schemaVersion
  result["machineKey"] = %state.machineKey
  result["nodeKey"] = %state.nodeKey
  result["wgKey"] = %state.wgKey
  result["machinePublicKey"] = %state.machinePublicKey
  result["nodePublicKey"] = %state.nodePublicKey
  result["discoPublicKey"] = %state.discoPublicKey
  result["controlPublicKey"] = %state.controlPublicKey
  result["controlLegacyPublicKey"] = %state.controlLegacyPublicKey
  result["hostname"] = %state.hostname
  result["controlUrl"] = %state.controlUrl
  result["nodeId"] = %state.nodeId
  result["userLogin"] = %state.userLogin
  result["homeDerp"] = %state.homeDerp
  result["tailnetIPs"] = %state.tailnetIPs
  result["mapSessionHandle"] = %state.mapSessionHandle
  result["mapSessionSeq"] = %state.mapSessionSeq
  result["lastControlSuccessUnixMilli"] = %state.lastControlSuccessUnixMilli
  result["lastRegisterAttemptUnixMilli"] = %state.lastRegisterAttemptUnixMilli
  result["lastMapPollAttemptUnixMilli"] = %state.lastMapPollAttemptUnixMilli
  result["lastControlBootstrapError"] = %state.lastControlBootstrapError
  result["createdAtUnixMilli"] = %state.createdAtUnixMilli
  result["updatedAtUnixMilli"] = %state.updatedAtUnixMilli
  var peers = newJArray()
  for peer in state.peerCache:
    peers.add(peer.toJson())
  result["peerCache"] = peers

proc parseStoredState*(node: JsonNode): Result[TsnetStoredState, string] =
  if node.isNil or node.kind != JObject:
    return err("tsnet state file must contain a JSON object")
  var state = TsnetStoredState(
    schemaVersion: jsonInt64(node, "schemaVersion").int,
    machineKey: jsonString(node, "machineKey"),
    nodeKey: jsonString(node, "nodeKey"),
    wgKey: jsonString(node, "wgKey"),
    machinePublicKey: jsonString(node, "machinePublicKey"),
    nodePublicKey: jsonString(node, "nodePublicKey"),
    discoPublicKey: jsonString(node, "discoPublicKey"),
    controlPublicKey: jsonString(node, "controlPublicKey"),
    controlLegacyPublicKey: jsonString(node, "controlLegacyPublicKey"),
    hostname: jsonString(node, "hostname"),
    controlUrl: jsonString(node, "controlUrl"),
    nodeId: jsonString(node, "nodeId"),
    userLogin: jsonString(node, "userLogin"),
    homeDerp: jsonString(node, "homeDerp"),
    tailnetIPs: jsonStringSeq(node, "tailnetIPs"),
    peerCache: @[],
    mapSessionHandle: jsonString(node, "mapSessionHandle"),
    mapSessionSeq: jsonInt64(node, "mapSessionSeq"),
    lastControlSuccessUnixMilli: jsonInt64(node, "lastControlSuccessUnixMilli"),
    lastRegisterAttemptUnixMilli: jsonInt64(node, "lastRegisterAttemptUnixMilli"),
    lastMapPollAttemptUnixMilli: jsonInt64(node, "lastMapPollAttemptUnixMilli"),
    lastControlBootstrapError: jsonString(node, "lastControlBootstrapError"),
    createdAtUnixMilli: jsonInt64(node, "createdAtUnixMilli"),
    updatedAtUnixMilli: jsonInt64(node, "updatedAtUnixMilli")
  )
  if node.hasKey("peerCache"):
    let peers = node.getOrDefault("peerCache")
    if peers.kind != JArray:
      return err("tsnet state peerCache must be an array")
    for item in peers.items():
      if item.kind != JObject:
        return err("tsnet state peerCache entries must be objects")
      state.peerCache.add(parseStoredPeer(item))
  normalizeState(state)
  discard syncDerivedPublicKeys(state)
  ok(state)

proc statePath*(stateDir: string): string =
  let clean = stateDir.strip()
  if clean.len == 0 or not clean.isAbsolute:
    return ""
  clean / TsnetStoredStateFilename

proc validateStateDir*(stateDir: string): Result[string, string] =
  let clean = stateDir.strip()
  if clean.len == 0:
    return err("tsnet stateDir is empty")
  if not clean.isAbsolute:
    return err("tsnet stateDir must be an absolute filesystem path")
  ok(clean)

proc loadStoredState*(
    stateDir: string,
    hostname = "",
    controlUrl = ""
): Result[TsnetStoredState, string] =
  let clean = validateStateDir(stateDir).valueOr:
    return err(error)
  let path = clean / TsnetStoredStateFilename
  if not fileExists(path):
    return ok(TsnetStoredState.init(hostname = hostname, controlUrl = controlUrl))
  try:
    let loaded = parseStoredState(readFile(path), path).valueOr:
      return err("failed to load tsnet state from " & path & ": " & error)
    var resultState = loaded
    if resultState.hostname.len == 0 and hostname.len > 0:
      resultState.hostname = hostname
    if resultState.controlUrl.len == 0 and controlUrl.len > 0:
      resultState.controlUrl = controlUrl
    ok(resultState)
  except CatchableError as exc:
    err("failed to load tsnet state from " & path & ": " & exc.msg)

proc storeStoredState*(stateDir: string, state: var TsnetStoredState): Result[string, string] =
  let clean = validateStateDir(stateDir).valueOr:
    return err(error)
  let path = clean / TsnetStoredStateFilename
  normalizeState(state)
  state.updatedAtUnixMilli = nowUnixMilli()
  try:
    createDir(clean)
    writeFile(path, $state.toJson())
    ok(path)
  except CatchableError as exc:
    err("failed to store tsnet state to " & path & ": " & exc.msg)
