{.push raises: [].}

import std/[httpclient, json, net, sequtils, strutils, times, uri]

import ../../utility
import ./ts2021client

proc safeTrace(message: string) =
  try:
    stderr.writeLine(message)
  except IOError:
    discard

type
  TsnetControlSnapshot* = object
    source*: string
    capturedAtUnixMilli*: int64
    probePayload*: JsonNode
    registerPayload*: JsonNode
    mapPollPayload*: JsonNode

  TsnetControlServerKey* = object
    capabilityVersion*: int
    publicKey*: string
    legacyPublicKey*: string
    fetchedAtUnixMilli*: int64
    rawPayload*: JsonNode

  TsnetRegisterBootstrapPlan* = object
    protocol*: string
    controlUrl*: string
    noiseUrl*: string
    registerUrl*: string
    capabilityVersion*: int
    controlPublicKey*: string
    controlLegacyPublicKey*: string
    machinePublicKey*: string
    nodePublicKey*: string
    hostname*: string
    authKeyProvided*: bool
    persistedNodeIdPresent*: bool

  TsnetMapBootstrapPlan* = object
    protocol*: string
    controlUrl*: string
    noiseUrl*: string
    mapUrl*: string
    capabilityVersion*: int
    nodePublicKey*: string
    discoPublicKey*: string
    hostname*: string
    keepAlive*: bool
    stream*: bool
    compress*: string

  TsnetRegisterResponseSnapshot* = object
    machineAuthorized*: bool
    nodeKeyExpired*: bool
    authUrl*: string
    error*: string
    userLoginName*: string
    userDisplayName*: string
    loginName*: string
    nodeKeySignaturePresent*: bool

  TsnetMapResponseSummary* = object
    keepAlive*: bool
    nodePresent*: bool
    derpMapPresent*: bool
    peerCount*: int
    domain*: string
    collectServices*: bool
    mapSessionHandle*: string
    seq*: int64

  TsnetControlRegisterResult* = object
    snapshot*: TsnetRegisterResponseSnapshot
    rawPayload*: JsonNode
    capturedAtUnixMilli*: int64

  TsnetControlMapPollResult* = object
    summary*: TsnetMapResponseSummary
    rawPayload*: JsonNode
    capturedAtUnixMilli*: int64

  TsnetControlBootstrapInput* = object
    controlUrl*: string
    hostname*: string
    authKey*: string
    wireguardPort*: int
    machinePrivateKey*: string
    machineKeyPresent*: bool
    machinePublicKey*: string
    nodeKeyPresent*: bool
    nodePublicKey*: string
    wgKeyPresent*: bool
    discoPublicKey*: string
    persistedNodeIdPresent*: bool
    libp2pPeerId*: string
    libp2pListenAddrs*: seq[string]

  TsnetControlBootstrapStage* {.pure.} = enum
    ProbeFailed
    AuthRequired
    ControlReachable
    Registered
    Mapped

  TsnetControlBootstrapResult* = object
    stage*: TsnetControlBootstrapStage
    error*: string
    serverKey*: TsnetControlServerKey
    snapshot*: TsnetControlSnapshot
    registerResult*: TsnetControlRegisterResult
    mapPollResult*: TsnetControlMapPollResult

  TsnetControlKeyFetchProc* =
    proc(controlUrl: string): Result[TsnetControlServerKey, string] {.closure, gcsafe, raises: [].}
  TsnetControlRegisterProc* =
    proc(controlUrl: string, request: JsonNode): Result[JsonNode, string] {.closure, gcsafe, raises: [].}
  TsnetControlMapPollProc* =
    proc(controlUrl: string, request: JsonNode): Result[JsonNode, string] {.closure, gcsafe, raises: [].}
  TsnetControlUrlProc* =
    proc(controlUrl: string): string {.closure, gcsafe, raises: [].}
  TsnetControlVersionedUrlProc* =
    proc(controlUrl: string, capabilityVersion: int): string {.closure, gcsafe, raises: [].}

  TsnetControlTransport* = ref object
    protocolLabel*: string
    noiseUrlProc*: TsnetControlUrlProc
    keyUrlProc*: TsnetControlVersionedUrlProc
    registerUrlProc*: TsnetControlUrlProc
    mapUrlProc*: TsnetControlUrlProc
    fetchServerKeyProc*: TsnetControlKeyFetchProc
    registerNodeProc*: TsnetControlRegisterProc
    mapPollProc*: TsnetControlMapPollProc

const
  TsnetDefaultControlCapabilityVersions* = [130, 109]
  TsnetControlNoiseUpgradePath* = "/ts2021"
  TsnetControlRegisterPath* = "/machine/register"
  TsnetControlMapPath* = "/machine/map"

proc nowUnixMilli(): int64 =
  getTime().toUnix().int64 * 1000

proc init*(
    _: type[TsnetControlSnapshot],
    source = "",
    capturedAtUnixMilli = 0'i64,
    probePayload: JsonNode = nil,
    registerPayload: JsonNode = nil,
    mapPollPayload: JsonNode = nil
): TsnetControlSnapshot =
  TsnetControlSnapshot(
    source: source,
    capturedAtUnixMilli: capturedAtUnixMilli,
    probePayload:
      if probePayload.isNil: newJNull()
      else: probePayload,
    registerPayload:
      if registerPayload.isNil: newJNull()
      else: registerPayload,
    mapPollPayload:
      if mapPollPayload.isNil: newJNull()
      else: mapPollPayload
  )

proc init*(
    _: type[TsnetControlServerKey],
    capabilityVersion = 0,
    publicKey = "",
    legacyPublicKey = "",
    fetchedAtUnixMilli = 0'i64,
    rawPayload: JsonNode = nil
): TsnetControlServerKey =
  TsnetControlServerKey(
    capabilityVersion: capabilityVersion,
    publicKey: publicKey,
    legacyPublicKey: legacyPublicKey,
    fetchedAtUnixMilli: fetchedAtUnixMilli,
    rawPayload:
      if rawPayload.isNil: newJNull()
      else: rawPayload
  )

proc init*(
    _: type[TsnetControlBootstrapResult],
    stage = TsnetControlBootstrapStage.ProbeFailed,
    error = "",
    serverKey = TsnetControlServerKey.init(),
    snapshot = TsnetControlSnapshot.init(),
    registerResult = TsnetControlRegisterResult(),
    mapPollResult = TsnetControlMapPollResult()
): TsnetControlBootstrapResult =
  TsnetControlBootstrapResult(
    stage: stage,
    error: error,
    serverKey: serverKey,
    snapshot: snapshot,
    registerResult: registerResult,
    mapPollResult: mapPollResult
  )

proc init*(
    _: type[TsnetControlTransport],
    protocolLabel = "",
    noiseUrlProc: TsnetControlUrlProc = nil,
    keyUrlProc: TsnetControlVersionedUrlProc = nil,
    registerUrlProc: TsnetControlUrlProc = nil,
    mapUrlProc: TsnetControlUrlProc = nil,
    fetchServerKeyProc: TsnetControlKeyFetchProc = nil,
    registerNodeProc: TsnetControlRegisterProc = nil,
    mapPollProc: TsnetControlMapPollProc = nil
): TsnetControlTransport =
  TsnetControlTransport(
    protocolLabel: protocolLabel,
    noiseUrlProc: noiseUrlProc,
    keyUrlProc: keyUrlProc,
    registerUrlProc: registerUrlProc,
    mapUrlProc: mapUrlProc,
    fetchServerKeyProc: fetchServerKeyProc,
    registerNodeProc: registerNodeProc,
    mapPollProc: mapPollProc
  )

proc available*(snapshot: TsnetControlSnapshot): bool =
  snapshot.probePayload.kind != JNull or
    snapshot.registerPayload.kind != JNull or
    snapshot.mapPollPayload.kind != JNull

proc available*(keys: TsnetControlServerKey): bool =
  keys.publicKey.strip().len > 0

proc available*(bootstrap: TsnetControlBootstrapResult): bool =
  bootstrap.snapshot.available() or bootstrap.serverKey.available()

proc stageLabel*(stage: TsnetControlBootstrapStage): string =
  case stage
  of TsnetControlBootstrapStage.ProbeFailed:
    "probe_failed"
  of TsnetControlBootstrapStage.AuthRequired:
    "auth_required"
  of TsnetControlBootstrapStage.ControlReachable:
    "control_reachable"
  of TsnetControlBootstrapStage.Registered:
    "registered"
  of TsnetControlBootstrapStage.Mapped:
    "mapped"

proc controlKeyUrl*(controlUrl: string, capabilityVersion: int): string
proc controlNoiseUrl*(controlUrl: string): string
proc controlRegisterUrl*(controlUrl: string): string
proc controlMapUrl*(controlUrl: string): string

proc protocolLabel*(transport: TsnetControlTransport): string =
  if transport.isNil or transport.protocolLabel.strip().len == 0:
    return "ts2021/http2"
  transport.protocolLabel.strip()

proc noiseUrl*(transport: TsnetControlTransport, controlUrl: string): string =
  if not transport.isNil and not transport.noiseUrlProc.isNil:
    let resolved = transport.noiseUrlProc(controlUrl).strip()
    if resolved.len > 0:
      return resolved
  controlNoiseUrl(controlUrl)

proc keyUrl*(
    transport: TsnetControlTransport,
    controlUrl: string,
    capabilityVersion: int
): string =
  if not transport.isNil and not transport.keyUrlProc.isNil:
    let resolved = transport.keyUrlProc(controlUrl, capabilityVersion).strip()
    if resolved.len > 0:
      return resolved
  controlKeyUrl(controlUrl, capabilityVersion)

proc registerUrl*(transport: TsnetControlTransport, controlUrl: string): string =
  if not transport.isNil and not transport.registerUrlProc.isNil:
    let resolved = transport.registerUrlProc(controlUrl).strip()
    if resolved.len > 0:
      return resolved
  controlRegisterUrl(controlUrl)

proc mapUrl*(transport: TsnetControlTransport, controlUrl: string): string =
  if not transport.isNil and not transport.mapUrlProc.isNil:
    let resolved = transport.mapUrlProc(controlUrl).strip()
    if resolved.len > 0:
      return resolved
  controlMapUrl(controlUrl)

proc toJson*(snapshot: TsnetControlSnapshot): JsonNode =
  result = newJObject()
  result["source"] = %snapshot.source
  result["capturedAtUnixMilli"] = %snapshot.capturedAtUnixMilli
  result["probe"] = snapshot.probePayload
  result["register"] = snapshot.registerPayload
  result["mapPoll"] = snapshot.mapPollPayload

proc toJson*(keys: TsnetControlServerKey): JsonNode =
  result = newJObject()
  result["capabilityVersion"] = %keys.capabilityVersion
  result["publicKey"] = %keys.publicKey
  result["legacyPublicKey"] = %keys.legacyPublicKey
  result["fetchedAtUnixMilli"] = %keys.fetchedAtUnixMilli
  result["raw"] = keys.rawPayload

proc toJson*(input: TsnetControlBootstrapInput): JsonNode =
  %*{
    "controlUrl": input.controlUrl,
    "hostname": input.hostname,
    "authKeyProvided": input.authKey.strip().len > 0,
    "wireguardPort": input.wireguardPort,
    "machineKeyPresent": input.machineKeyPresent,
    "machinePublicKey": input.machinePublicKey,
    "nodeKeyPresent": input.nodeKeyPresent,
    "nodePublicKey": input.nodePublicKey,
    "wgKeyPresent": input.wgKeyPresent,
    "discoPublicKey": input.discoPublicKey,
    "persistedNodeIdPresent": input.persistedNodeIdPresent,
    "libp2pPeerId": input.libp2pPeerId,
    "libp2pListenAddrs": input.libp2pListenAddrs,
  }

proc cloneJson(node: JsonNode): JsonNode =
  if node.isNil:
    return newJNull()
  try:
    parseJson($node)
  except CatchableError:
    node

proc transportRequestMeta(
    request: JsonNode,
    input: TsnetControlBootstrapInput,
    serverKey: TsnetControlServerKey
): JsonNode =
  result = cloneJson(request)
  if result.kind == JObject:
    result["_tsnetMachinePrivateKey"] = %input.machinePrivateKey
    result["_tsnetControlPublicKey"] = %serverKey.publicKey

proc toJson*(registerResult: TsnetControlRegisterResult): JsonNode
proc toJson*(mapPollResult: TsnetControlMapPollResult): JsonNode

proc toJson*(bootstrap: TsnetControlBootstrapResult): JsonNode =
  result = %*{
    "stage": bootstrap.stage.stageLabel(),
    "error": bootstrap.error,
  }
  result["serverKey"] = bootstrap.serverKey.toJson()
  result["snapshot"] = bootstrap.snapshot.toJson()
  result["registerResult"] = bootstrap.registerResult.toJson()
  result["mapPollResult"] = bootstrap.mapPollResult.toJson()

proc toJson*(plan: TsnetRegisterBootstrapPlan): JsonNode =
  %*{
    "protocol": plan.protocol,
    "controlUrl": plan.controlUrl,
    "noiseUrl": plan.noiseUrl,
    "registerUrl": plan.registerUrl,
    "capabilityVersion": plan.capabilityVersion,
    "controlPublicKey": plan.controlPublicKey,
    "controlLegacyPublicKey": plan.controlLegacyPublicKey,
    "machinePublicKey": plan.machinePublicKey,
    "nodePublicKey": plan.nodePublicKey,
    "hostname": plan.hostname,
    "authKeyProvided": plan.authKeyProvided,
    "persistedNodeIdPresent": plan.persistedNodeIdPresent,
  }

proc toJson*(plan: TsnetMapBootstrapPlan): JsonNode =
  %*{
    "protocol": plan.protocol,
    "controlUrl": plan.controlUrl,
    "noiseUrl": plan.noiseUrl,
    "mapUrl": plan.mapUrl,
    "capabilityVersion": plan.capabilityVersion,
    "nodePublicKey": plan.nodePublicKey,
    "discoPublicKey": plan.discoPublicKey,
    "hostname": plan.hostname,
    "keepAlive": plan.keepAlive,
    "stream": plan.stream,
    "compress": plan.compress,
  }

proc jsonField(node: JsonNode, key: string): JsonNode =
  if node.isNil or node.kind != JObject or not node.hasKey(key):
    return newJNull()
  node.getOrDefault(key)

proc jsonString(node: JsonNode, key: string): string =
  let value = jsonField(node, key)
  if value.kind == JString:
    return value.getStr().strip()
  ""

proc jsonFieldInsensitive(node: JsonNode, keys: openArray[string]): JsonNode =
  if node.isNil or node.kind != JObject:
    return newJNull()
  for key in keys:
    if node.hasKey(key):
      return node.getOrDefault(key)
  newJNull()

proc jsonStringInsensitive(node: JsonNode, keys: openArray[string]): string =
  let value = jsonFieldInsensitive(node, keys)
  if value.kind == JString:
    return value.getStr().strip()
  ""

proc jsonBoolInsensitive(node: JsonNode, keys: openArray[string]): bool =
  let value = jsonFieldInsensitive(node, keys)
  case value.kind
  of JBool:
    value.getBool()
  of JString:
    value.getStr().strip().toLowerAscii() in ["1", "true", "yes", "y"]
  else:
    false

proc jsonInt64Insensitive(node: JsonNode, keys: openArray[string]): int64 =
  let value = jsonFieldInsensitive(node, keys)
  case value.kind
  of JInt:
    value.getBiggestInt().int64
  of JFloat:
    value.getFloat().int64
  else:
    0'i64

proc jsonBoolNode(value: JsonNode): bool =
  case value.kind
  of JBool:
    value.getBool()
  of JString:
    value.getStr().strip().toLowerAscii() in ["1", "true", "yes", "y"]
  else:
    false

proc parseControlServerKey*(
    payload: JsonNode,
    capabilityVersion = 0,
    fetchedAtUnixMilli = 0'i64
): Result[TsnetControlServerKey, string] =
  if payload.isNil or payload.kind != JObject:
    return err("tsnet control key payload must be a JSON object")
  let publicKey = jsonString(payload, "publicKey")
  if publicKey.len == 0:
    return err("tsnet control key payload is missing publicKey")
  ok(TsnetControlServerKey.init(
    capabilityVersion = capabilityVersion,
    publicKey = publicKey,
    legacyPublicKey = jsonString(payload, "legacyPublicKey"),
    fetchedAtUnixMilli =
      if fetchedAtUnixMilli > 0: fetchedAtUnixMilli
      else: nowUnixMilli(),
    rawPayload = payload
  ))

proc normalizeControlUrl*(controlUrl: string): string =
  result = controlUrl.strip()
  while result.len > 0 and result[^1] == '/':
    result.setLen(result.len - 1)

proc controlKeyUrl*(controlUrl: string, capabilityVersion: int): string =
  let base = normalizeControlUrl(controlUrl)
  if base.len == 0:
    return ""
  let separator =
    if base.contains("?"):
      "&"
    else:
      "?"
  base & "/key" & separator & "v=" & $capabilityVersion

proc controlNoiseUrl*(controlUrl: string): string =
  let base = normalizeControlUrl(controlUrl)
  if base.len == 0:
    return ""
  base & TsnetControlNoiseUpgradePath

proc controlRegisterUrl*(controlUrl: string): string =
  let base = normalizeControlUrl(controlUrl)
  if base.len == 0:
    return ""
  base & TsnetControlRegisterPath

proc controlMapUrl*(controlUrl: string): string =
  let base = normalizeControlUrl(controlUrl)
  if base.len == 0:
    return ""
  base & TsnetControlMapPath

proc buildRegisterRequestPayload*(
    capabilityVersion: int,
    nodePublicKey: string,
    machinePublicKey: string,
    hostname: string,
    libp2pPeerId = "",
    libp2pListenAddrs: seq[string] = @[],
    authKey = "",
    oldNodeKey = "",
    followup = "",
    nlPublicKey = "",
    ephemeral = false
): JsonNode

proc buildMapRequestPayload*(
    capabilityVersion: int,
    nodePublicKey: string,
    discoPublicKey: string,
    hostname: string,
    libp2pPeerId = "",
    libp2pListenAddrs: seq[string] = @[],
    stream = true,
    keepAlive = true,
    compress = "zstd",
    omitPeers = false
): JsonNode

proc buildControlProbePayload*(
    input: TsnetControlBootstrapInput,
    probed: TsnetControlServerKey,
    protocolLabel = "ts2021/http2",
    noiseUrl = "",
    registerUrl = "",
    mapUrl = ""
): JsonNode =
  let resolvedNoiseUrl =
    if noiseUrl.strip().len > 0: noiseUrl.strip()
    else: controlNoiseUrl(input.controlUrl)
  let resolvedRegisterUrl =
    if registerUrl.strip().len > 0: registerUrl.strip()
    else: controlRegisterUrl(input.controlUrl)
  let resolvedMapUrl =
    if mapUrl.strip().len > 0: mapUrl.strip()
    else: controlMapUrl(input.controlUrl)
  %*{
    "stage": "control_key_probe",
    "protocol": protocolLabel,
    "controlUrl": input.controlUrl,
    "noiseUrl": resolvedNoiseUrl,
    "registerUrl": resolvedRegisterUrl,
    "mapUrl": resolvedMapUrl,
    "hostname": input.hostname,
    "wireguardPort": input.wireguardPort,
    "capabilityVersion": probed.capabilityVersion,
    "publicKey": probed.publicKey,
    "legacyPublicKey": probed.legacyPublicKey,
    "fetchedAtUnixMilli": probed.fetchedAtUnixMilli,
    "authKeyProvided": input.authKey.strip().len > 0,
    "machineKeyPresent": input.machineKeyPresent,
    "machinePublicKey": input.machinePublicKey,
    "nodeKeyPresent": input.nodeKeyPresent,
    "nodePublicKey": input.nodePublicKey,
    "wgKeyPresent": input.wgKeyPresent,
    "discoPublicKey": input.discoPublicKey,
    "persistedNodeIdPresent": input.persistedNodeIdPresent,
  }

proc buildRegisterBootstrapPayload*(
    input: TsnetControlBootstrapInput,
    probed: TsnetControlServerKey,
    protocolLabel = "ts2021/http2",
    noiseUrl = "",
    registerUrl = ""
): JsonNode =
  let resolvedNoiseUrl =
    if noiseUrl.strip().len > 0: noiseUrl.strip()
    else: controlNoiseUrl(input.controlUrl)
  let resolvedRegisterUrl =
    if registerUrl.strip().len > 0: registerUrl.strip()
    else: controlRegisterUrl(input.controlUrl)
  let plan = TsnetRegisterBootstrapPlan(
    protocol: protocolLabel,
    controlUrl: input.controlUrl,
    noiseUrl: resolvedNoiseUrl,
    registerUrl: resolvedRegisterUrl,
    capabilityVersion: probed.capabilityVersion,
    controlPublicKey: probed.publicKey,
    controlLegacyPublicKey: probed.legacyPublicKey,
    machinePublicKey: input.machinePublicKey,
    nodePublicKey: input.nodePublicKey,
    hostname: input.hostname,
    authKeyProvided: input.authKey.strip().len > 0,
    persistedNodeIdPresent: input.persistedNodeIdPresent
  )
  result = plan.toJson()
  result["request"] =
    buildRegisterRequestPayload(
      capabilityVersion = plan.capabilityVersion,
      nodePublicKey = plan.nodePublicKey,
      machinePublicKey = plan.machinePublicKey,
      hostname = plan.hostname,
      libp2pPeerId = input.libp2pPeerId,
      libp2pListenAddrs = input.libp2pListenAddrs,
      authKey = input.authKey
    )
  result["response"] = newJNull()
  result["stage"] = %"register_pending"

proc buildMapBootstrapPayload*(
    input: TsnetControlBootstrapInput,
    probed: TsnetControlServerKey,
    protocolLabel = "ts2021/http2",
    noiseUrl = "",
    mapUrl = ""
): JsonNode =
  let resolvedNoiseUrl =
    if noiseUrl.strip().len > 0: noiseUrl.strip()
    else: controlNoiseUrl(input.controlUrl)
  let resolvedMapUrl =
    if mapUrl.strip().len > 0: mapUrl.strip()
    else: controlMapUrl(input.controlUrl)
  let plan = TsnetMapBootstrapPlan(
    protocol: protocolLabel,
    controlUrl: input.controlUrl,
    noiseUrl: resolvedNoiseUrl,
    mapUrl: resolvedMapUrl,
    capabilityVersion: probed.capabilityVersion,
    nodePublicKey: input.nodePublicKey,
    discoPublicKey: input.discoPublicKey,
    hostname: input.hostname,
    keepAlive: protocolLabel notin ["nim_quic", "nim_tcp"],
    stream: protocolLabel notin ["nim_quic", "nim_tcp"],
    compress: "zstd"
  )
  result = plan.toJson()
  result["request"] =
    buildMapRequestPayload(
      capabilityVersion = plan.capabilityVersion,
      nodePublicKey = plan.nodePublicKey,
      discoPublicKey = plan.discoPublicKey,
      hostname = plan.hostname,
      libp2pPeerId = input.libp2pPeerId,
      libp2pListenAddrs = input.libp2pListenAddrs,
      stream = plan.stream,
      keepAlive = plan.keepAlive,
      compress = plan.compress
    )
  result["response"] = newJNull()
  result["stage"] = %"map_pending"

proc buildRegisterRequestPayload*(
    capabilityVersion: int,
    nodePublicKey: string,
    machinePublicKey: string,
    hostname: string,
    libp2pPeerId = "",
    libp2pListenAddrs: seq[string] = @[],
    authKey = "",
    oldNodeKey = "",
    followup = "",
    nlPublicKey = "",
    ephemeral = false
): JsonNode =
  result = newJObject()
  result["Version"] = %capabilityVersion
  result["NodeKey"] = %nodePublicKey
  if oldNodeKey.strip().len > 0:
    result["OldNodeKey"] = %oldNodeKey
  if nlPublicKey.strip().len > 0:
    result["NLKey"] = %nlPublicKey
  if authKey.strip().len > 0:
    result["Auth"] = %*{"AuthKey": authKey}
  if followup.strip().len > 0:
    result["Followup"] = %followup
  if ephemeral:
    result["Ephemeral"] = %true
  var hostinfo = %*{
    "Hostname": hostname,
    "BackendLogID": machinePublicKey,
  }
  if libp2pPeerId.strip().len > 0:
    hostinfo["PeerID"] = %libp2pPeerId.strip()
  let normalizedListenAddrs =
    libp2pListenAddrs
      .mapIt(it.strip())
      .filterIt(it.len > 0)
  if normalizedListenAddrs.len > 0:
    hostinfo["ListenAddrs"] = %normalizedListenAddrs
  result["Hostinfo"] = hostinfo

proc buildMapRequestPayload*(
    capabilityVersion: int,
    nodePublicKey: string,
    discoPublicKey: string,
    hostname: string,
    libp2pPeerId = "",
    libp2pListenAddrs: seq[string] = @[],
    stream = true,
    keepAlive = true,
    compress = "zstd",
    omitPeers = false
): JsonNode =
  result = newJObject()
  result["Version"] = %capabilityVersion
  result["Compress"] = %compress
  result["KeepAlive"] = %keepAlive
  result["NodeKey"] = %nodePublicKey
  result["DiscoKey"] = %discoPublicKey
  result["Stream"] = %stream
  result["OmitPeers"] = %omitPeers
  var hostinfo = %*{
    "Hostname": hostname
  }
  if libp2pPeerId.strip().len > 0:
    hostinfo["PeerID"] = %libp2pPeerId.strip()
  let normalizedListenAddrs =
    libp2pListenAddrs
      .mapIt(it.strip())
      .filterIt(it.len > 0)
  if normalizedListenAddrs.len > 0:
    hostinfo["ListenAddrs"] = %normalizedListenAddrs
  result["Hostinfo"] = hostinfo

proc parseRegisterResponse*(
    payload: JsonNode
): Result[TsnetRegisterResponseSnapshot, string] =
  if payload.isNil or payload.kind != JObject:
    return err("tsnet register response payload must be a JSON object")
  let userNode = jsonFieldInsensitive(payload, ["User", "user"])
  let loginNode = jsonFieldInsensitive(payload, ["Login", "login"])
  ok(TsnetRegisterResponseSnapshot(
    machineAuthorized: jsonBoolInsensitive(payload, ["MachineAuthorized", "machineAuthorized"]),
    nodeKeyExpired: jsonBoolInsensitive(payload, ["NodeKeyExpired", "nodeKeyExpired"]),
    authUrl: jsonStringInsensitive(payload, ["AuthURL", "authUrl"]),
    error: jsonStringInsensitive(payload, ["Error", "error"]),
    userLoginName: jsonStringInsensitive(userNode, ["LoginName", "loginName"]),
    userDisplayName: jsonStringInsensitive(userNode, ["DisplayName", "displayName"]),
    loginName: jsonStringInsensitive(loginNode, ["LoginName", "loginName"]),
    nodeKeySignaturePresent:
      jsonFieldInsensitive(payload, ["NodeKeySignature", "nodeKeySignature"]).kind != JNull
  ))

proc toJson*(snapshot: TsnetRegisterResponseSnapshot): JsonNode =
  %*{
    "machineAuthorized": snapshot.machineAuthorized,
    "nodeKeyExpired": snapshot.nodeKeyExpired,
    "authUrl": snapshot.authUrl,
    "error": snapshot.error,
    "userLoginName": snapshot.userLoginName,
    "userDisplayName": snapshot.userDisplayName,
    "loginName": snapshot.loginName,
    "nodeKeySignaturePresent": snapshot.nodeKeySignaturePresent,
  }

proc parseMapResponseSummary*(
    payload: JsonNode
): Result[TsnetMapResponseSummary, string] =
  if payload.isNil or payload.kind != JObject:
    return err("tsnet map response payload must be a JSON object")
  let peersNode = jsonFieldInsensitive(payload, ["Peers", "peers"])
  let collectServicesNode = jsonFieldInsensitive(payload, ["CollectServices", "collectServices"])
  ok(TsnetMapResponseSummary(
    keepAlive: jsonBoolInsensitive(payload, ["KeepAlive", "keepAlive"]),
    nodePresent: jsonFieldInsensitive(payload, ["Node", "node"]).kind == JObject,
    derpMapPresent: jsonFieldInsensitive(payload, ["DERPMap", "derpMap"]).kind == JObject,
    peerCount:
      if peersNode.kind == JArray: peersNode.len
      else: 0,
    domain: jsonStringInsensitive(payload, ["Domain", "domain"]),
    collectServices: jsonBoolNode(collectServicesNode),
    mapSessionHandle: jsonStringInsensitive(payload, ["MapSessionHandle", "mapSessionHandle"]),
    seq: jsonInt64Insensitive(payload, ["Seq", "seq"])
  ))

proc toJson*(summary: TsnetMapResponseSummary): JsonNode =
  %*{
    "keepAlive": summary.keepAlive,
    "nodePresent": summary.nodePresent,
    "derpMapPresent": summary.derpMapPresent,
    "peerCount": summary.peerCount,
    "domain": summary.domain,
    "collectServices": summary.collectServices,
    "mapSessionHandle": summary.mapSessionHandle,
    "seq": summary.seq,
  }

proc init*(
    _: type[TsnetControlRegisterResult],
    snapshot = TsnetRegisterResponseSnapshot(),
    rawPayload: JsonNode = nil,
    capturedAtUnixMilli = 0'i64
): TsnetControlRegisterResult =
  TsnetControlRegisterResult(
    snapshot: snapshot,
    rawPayload:
      if rawPayload.isNil: newJNull()
      else: rawPayload,
    capturedAtUnixMilli: capturedAtUnixMilli
  )

proc init*(
    _: type[TsnetControlMapPollResult],
    summary = TsnetMapResponseSummary(),
    rawPayload: JsonNode = nil,
    capturedAtUnixMilli = 0'i64
): TsnetControlMapPollResult =
  TsnetControlMapPollResult(
    summary: summary,
    rawPayload:
      if rawPayload.isNil: newJNull()
      else: rawPayload,
    capturedAtUnixMilli: capturedAtUnixMilli
  )

proc toJson*(registerResult: TsnetControlRegisterResult): JsonNode =
  let rawPayload =
    if registerResult.rawPayload.isNil: newJNull()
    else: registerResult.rawPayload
  result = newJObject()
  result["capturedAtUnixMilli"] = %registerResult.capturedAtUnixMilli
  result["snapshot"] = registerResult.snapshot.toJson()
  result["raw"] = rawPayload

proc toJson*(mapPollResult: TsnetControlMapPollResult): JsonNode =
  let rawPayload =
    if mapPollResult.rawPayload.isNil: newJNull()
    else: mapPollResult.rawPayload
  result = newJObject()
  result["capturedAtUnixMilli"] = %mapPollResult.capturedAtUnixMilli
  result["summary"] = mapPollResult.summary.toJson()
  result["raw"] = rawPayload

proc fetchControlServerKey*(
    controlUrl: string,
    capabilityVersion: int
): Result[TsnetControlServerKey, string] =
  let target = controlKeyUrl(controlUrl, capabilityVersion)
  safeTrace("[tsnet-control] default fetch target=" & target)
  if target.len == 0:
    return err("tsnet controlUrl is empty")
  var client: HttpClient
  try:
    when defined(ssl):
      let sslContext = newContext(verifyMode = CVerifyPeer)
      client = newHttpClient(sslContext = sslContext)
    else:
      client = newHttpClient()
    let body = client.getContent(target)
    let payload = parseJson(body)
    parseControlServerKey(
      payload,
      capabilityVersion = capabilityVersion,
      fetchedAtUnixMilli = nowUnixMilli()
    )
  except Exception as exc:
    err("failed to probe tsnet control key from " & target & ": " & exc.msg)
  finally:
    if not client.isNil:
      try:
        client.close()
      except Exception:
        discard

proc fetchControlServerKey*(
    controlUrl: string,
    capabilityVersions: openArray[int]
): Result[TsnetControlServerKey, string] =
  var failures: seq[string] = @[]
  for version in capabilityVersions:
    let fetched = fetchControlServerKey(controlUrl, version)
    if fetched.isOk():
      return fetched
    failures.add(fetched.error)
  if failures.len == 0:
    return err("no tsnet control capability versions were provided")
  err(failures.join("; "))

proc fetchControlServerKey*(controlUrl: string): Result[TsnetControlServerKey, string] =
  fetchControlServerKey(controlUrl, TsnetDefaultControlCapabilityVersions)

proc defaultControlTransport*(): TsnetControlTransport =
  TsnetControlTransport.init(
    protocolLabel = "ts2021/http2",
    fetchServerKeyProc = fetchControlServerKey,
    registerNodeProc = registerNodeTs2021,
    mapPollProc = mapPollTs2021
  )

proc fetchServerKey*(
    transport: TsnetControlTransport,
    controlUrl: string
): Result[TsnetControlServerKey, string] =
  if transport.isNil:
    return err("tsnet control transport is nil")
  if transport.fetchServerKeyProc.isNil:
    return fetchControlServerKey(controlUrl)
  transport.fetchServerKeyProc(controlUrl)

proc supportsNodeRegistration*(transport: TsnetControlTransport): bool =
  not transport.isNil and not transport.registerNodeProc.isNil

proc supportsMapPoll*(transport: TsnetControlTransport): bool =
  not transport.isNil and not transport.mapPollProc.isNil

proc registerNode*(
    transport: TsnetControlTransport,
    controlUrl: string,
    request: JsonNode
): Result[TsnetControlRegisterResult, string] =
  if not transport.supportsNodeRegistration():
    return err("tsnet control transport does not implement registerNode")
  let rawPayload = transport.registerNodeProc(controlUrl, request).valueOr:
    return err(error)
  let parsed = parseRegisterResponse(rawPayload).valueOr:
    return err(error)
  ok(TsnetControlRegisterResult.init(
    snapshot = parsed,
    rawPayload = rawPayload,
    capturedAtUnixMilli = nowUnixMilli()
  ))

proc mapPoll*(
    transport: TsnetControlTransport,
    controlUrl: string,
    request: JsonNode
): Result[TsnetControlMapPollResult, string] =
  if not transport.supportsMapPoll():
    return err("tsnet control transport does not implement mapPoll")
  let rawPayload = transport.mapPollProc(controlUrl, request).valueOr:
    return err(error)
  let parsed = parseMapResponseSummary(rawPayload).valueOr:
    return err(error)
  ok(TsnetControlMapPollResult.init(
    summary = parsed,
    rawPayload = rawPayload,
    capturedAtUnixMilli = nowUnixMilli()
  ))

proc classifyBootstrapStage*(input: TsnetControlBootstrapInput): TsnetControlBootstrapStage =
  TsnetControlBootstrapStage.ControlReachable

proc bootstrapControl*(
    transport: TsnetControlTransport,
    input: TsnetControlBootstrapInput,
    source = "nim-tsnet-inapp-live"
): Result[TsnetControlBootstrapResult, string] =
  if transport.isNil:
    return err("tsnet control transport is nil")
  let probed = transport.fetchServerKey(input.controlUrl).valueOr:
    return err(error)
  let snapshot = TsnetControlSnapshot.init(
    source = source,
    capturedAtUnixMilli = probed.fetchedAtUnixMilli,
    probePayload = buildControlProbePayload(
      input,
      probed,
      protocolLabel = transport.protocolLabel(),
      noiseUrl = transport.noiseUrl(input.controlUrl),
      registerUrl = transport.registerUrl(input.controlUrl),
      mapUrl = transport.mapUrl(input.controlUrl)
    ),
    registerPayload = buildRegisterBootstrapPayload(
      input,
      probed,
      protocolLabel = transport.protocolLabel(),
      noiseUrl = transport.noiseUrl(input.controlUrl),
      registerUrl = transport.registerUrl(input.controlUrl)
    ),
    mapPollPayload = buildMapBootstrapPayload(
      input,
      probed,
      protocolLabel = transport.protocolLabel(),
      noiseUrl = transport.noiseUrl(input.controlUrl),
      mapUrl = transport.mapUrl(input.controlUrl)
    )
  )
  let initialStage = classifyBootstrapStage(input)
  var bootstrap = TsnetControlBootstrapResult.init(
    stage = initialStage,
    error = "",
    serverKey = probed,
    snapshot = snapshot
  )

  if initialStage != TsnetControlBootstrapStage.ControlReachable:
    return ok(bootstrap)

  if transport.supportsNodeRegistration():
    let registerRequest = transportRequestMeta(
      jsonField(snapshot.registerPayload, "request"),
      input,
      probed
    )
    let registerResult = transport.registerNode(input.controlUrl, registerRequest).valueOr:
      bootstrap.error = error
      return ok(bootstrap)
    bootstrap.registerResult = registerResult
    bootstrap.snapshot.registerPayload["response"] = registerResult.rawPayload
    if registerResult.snapshot.error.len > 0:
      bootstrap.error = registerResult.snapshot.error
      return ok(bootstrap)
    if registerResult.snapshot.authUrl.len > 0 and not registerResult.snapshot.machineAuthorized:
      bootstrap.stage = TsnetControlBootstrapStage.AuthRequired
      return ok(bootstrap)
    bootstrap.stage = TsnetControlBootstrapStage.Registered
    bootstrap.snapshot.registerPayload["stage"] = %bootstrap.stage.stageLabel()

    if transport.supportsMapPoll():
      let mapRequest = transportRequestMeta(
        jsonField(snapshot.mapPollPayload, "request"),
        input,
        probed
      )
      let mapPollResult = transport.mapPoll(input.controlUrl, mapRequest).valueOr:
        bootstrap.error = error
        return ok(bootstrap)
      bootstrap.mapPollResult = mapPollResult
      bootstrap.snapshot.mapPollPayload["response"] = mapPollResult.rawPayload
      bootstrap.stage = TsnetControlBootstrapStage.Mapped
      bootstrap.snapshot.mapPollPayload["stage"] = %bootstrap.stage.stageLabel()
  ok(bootstrap)
