{.push raises: [].}

import std/[dynlib, json, os, strutils]
import results

import ../multiaddress

const
  DefaultTsnetBridgeEnvVar* = "NIM_TSNET_BRIDGE_LIB"

when defined(windows):
  const DefaultTsnetBridgeCandidates = ["tsnetbridge.dll", "libtsnetbridge.dll"]
elif defined(macosx):
  const DefaultTsnetBridgeCandidates = ["libtsnetbridge.dylib"]
else:
  const DefaultTsnetBridgeCandidates = ["libtsnetbridge.so", "libtsnetbridge.dylib"]

type
  TsnetBridgeCreateProc = proc(configJson: cstring): int64 {.cdecl.}
  TsnetBridgeReleaseProc = proc(handle: int64) {.cdecl.}
  TsnetBridgeResetProc = proc(handle: int64): cint {.cdecl.}
  TsnetBridgeJsonProc = proc(handle: int64): cstring {.cdecl.}
  TsnetBridgeJsonArgProc = proc(handle: int64, requestJson: cstring): cstring {.cdecl.}
  TsnetBridgeStringFreeProc = proc(value: cstring) {.cdecl.}

  TsnetBridgeLibrary = ref object
    handle: LibHandle
    path*: string
    createProc: TsnetBridgeCreateProc
    releaseProc: TsnetBridgeReleaseProc
    resetProc: TsnetBridgeResetProc
    statusProc: TsnetBridgeJsonProc
    pingProc: TsnetBridgeJsonArgProc
    derpMapProc: TsnetBridgeJsonProc
    listenTcpProc: TsnetBridgeJsonArgProc
    listenUdpProc: TsnetBridgeJsonArgProc
    dialTcpProc: TsnetBridgeJsonArgProc
    dialUdpProc: TsnetBridgeJsonArgProc
    resolveRemoteProc: TsnetBridgeJsonArgProc
    stringFreeProc: TsnetBridgeStringFreeProc

  TsnetBridge* = ref object
    lib: TsnetBridgeLibrary
    handle*: int64

proc loadSymbol[T](handle: LibHandle, name: string): Result[T, string] =
  let symbol = handle.symAddr(name)
  if symbol.isNil:
    return err("missing symbol: " & name)
  ok(cast[T](symbol))

proc candidatePaths(explicitPath: string): seq[string] =
  let trimmed = explicitPath.strip()
  if trimmed.len > 0:
    result.add(trimmed)
  let envPath = getEnv(DefaultTsnetBridgeEnvVar).strip()
  if envPath.len > 0 and envPath notin result:
    result.add(envPath)
  for name in DefaultTsnetBridgeCandidates:
    if name notin result:
      result.add(name)

proc loadLibrary(explicitPath: string): Result[TsnetBridgeLibrary, string] =
  var errors: seq[string] = @[]
  for path in candidatePaths(explicitPath):
    let handle = loadLib(path)
    if handle.isNil:
      errors.add("load failed: " & path)
      continue

    let createProc = loadSymbol[TsnetBridgeCreateProc](handle, "TsnetBridgeCreate").valueOr:
      errors.add(path & ": " & error)
      unloadLib(handle)
      continue
    let releaseProc = loadSymbol[TsnetBridgeReleaseProc](handle, "TsnetBridgeRelease").valueOr:
      errors.add(path & ": " & error)
      unloadLib(handle)
      continue
    let resetProc = loadSymbol[TsnetBridgeResetProc](handle, "TsnetBridgeReset").valueOr:
      errors.add(path & ": " & error)
      unloadLib(handle)
      continue
    let statusProc = loadSymbol[TsnetBridgeJsonProc](handle, "TsnetBridgeStatusJSON").valueOr:
      errors.add(path & ": " & error)
      unloadLib(handle)
      continue
    let pingProc = loadSymbol[TsnetBridgeJsonArgProc](handle, "TsnetBridgePingJSON").valueOr:
      errors.add(path & ": " & error)
      unloadLib(handle)
      continue
    let derpMapProc = loadSymbol[TsnetBridgeJsonProc](handle, "TsnetBridgeDERPMapJSON").valueOr:
      errors.add(path & ": " & error)
      unloadLib(handle)
      continue
    let listenTcpProc = loadSymbol[TsnetBridgeJsonArgProc](handle, "TsnetBridgeListenTCPProxyJSON").valueOr:
      errors.add(path & ": " & error)
      unloadLib(handle)
      continue
    let listenUdpProc = loadSymbol[TsnetBridgeJsonArgProc](handle, "TsnetBridgeListenUDPProxyJSON").valueOr:
      errors.add(path & ": " & error)
      unloadLib(handle)
      continue
    let dialTcpProc = loadSymbol[TsnetBridgeJsonArgProc](handle, "TsnetBridgeDialTCPProxyJSON").valueOr:
      errors.add(path & ": " & error)
      unloadLib(handle)
      continue
    let dialUdpProc = loadSymbol[TsnetBridgeJsonArgProc](handle, "TsnetBridgeDialUDPProxyJSON").valueOr:
      errors.add(path & ": " & error)
      unloadLib(handle)
      continue
    let resolveRemoteProc = loadSymbol[TsnetBridgeJsonArgProc](handle, "TsnetBridgeResolveRemoteJSON").valueOr:
      errors.add(path & ": " & error)
      unloadLib(handle)
      continue
    let stringFreeProc = loadSymbol[TsnetBridgeStringFreeProc](handle, "TsnetBridgeStringFree").valueOr:
      errors.add(path & ": " & error)
      unloadLib(handle)
      continue

    return ok(TsnetBridgeLibrary(
      handle: handle,
      path: path,
      createProc: createProc,
      releaseProc: releaseProc,
      resetProc: resetProc,
      statusProc: statusProc,
      pingProc: pingProc,
      derpMapProc: derpMapProc,
      listenTcpProc: listenTcpProc,
      listenUdpProc: listenUdpProc,
      dialTcpProc: dialTcpProc,
      dialUdpProc: dialUdpProc,
      resolveRemoteProc: resolveRemoteProc,
      stringFreeProc: stringFreeProc
    ))

  err("unable to load tsnet bridge library: " & errors.join("; "))

proc jsonField(node: JsonNode, key: string): JsonNode =
  if node.isNil or node.kind != JObject or not node.hasKey(key):
    return newJNull()
  node.getOrDefault(key)

proc jsonFieldStr(node: JsonNode, key, defaultValue: string = ""): string =
  let field = jsonField(node, key)
  if field.isNil:
    return defaultValue
  if field.kind == JString:
    return field.getStr()
  if field.kind == JNull:
    return defaultValue
  $field

proc callJson(lib: TsnetBridgeLibrary, procRef: TsnetBridgeJsonProc, handle: int64): Result[JsonNode, string] =
  if lib.isNil:
    return err("tsnet bridge library is nil")
  let raw =
    try:
      procRef(handle)
    except Exception as exc:
      return err("tsnet bridge call failed: " & exc.msg)
  if raw.isNil:
    return err("tsnet bridge returned nil response")
  let payload = $raw
  try:
    lib.stringFreeProc(raw)
  except Exception:
    discard
  try:
    let node = parseJson(payload)
    let okField = jsonField(node, "ok")
    if node.kind == JObject and okField.kind == JBool and not okField.getBool():
      return err(jsonFieldStr(node, "error", "tsnet bridge request failed"))
    ok(node)
  except CatchableError as exc:
    err("invalid tsnet bridge json: " & exc.msg)

proc callJson(
    lib: TsnetBridgeLibrary,
    procRef: TsnetBridgeJsonArgProc,
    handle: int64,
    request: JsonNode
): Result[JsonNode, string] =
  if lib.isNil:
    return err("tsnet bridge library is nil")
  let raw =
    try:
      procRef(handle, $request)
    except Exception as exc:
      return err("tsnet bridge call failed: " & exc.msg)
  if raw.isNil:
    return err("tsnet bridge returned nil response")
  let payload = $raw
  try:
    lib.stringFreeProc(raw)
  except Exception:
    discard
  try:
    let node = parseJson(payload)
    let okField = jsonField(node, "ok")
    if node.kind == JObject and okField.kind == JBool and not okField.getBool():
      return err(jsonFieldStr(node, "error", "tsnet bridge request failed"))
    ok(node)
  except CatchableError as exc:
    err("invalid tsnet bridge json: " & exc.msg)

proc openTsnetBridge*(configJson, explicitPath: string): Result[TsnetBridge, string] =
  let lib = loadLibrary(explicitPath).valueOr:
    return err(error)
  let handle =
    try:
      lib.createProc(configJson.cstring)
    except Exception as exc:
      return err("tsnet bridge create failed: " & exc.msg)
  if handle <= 0:
    return err("tsnet bridge create failed")
  ok(TsnetBridge(lib: lib, handle: handle))

proc close*(bridge: TsnetBridge) =
  if bridge.isNil or bridge.handle <= 0 or bridge.lib.isNil:
    return
  try:
    bridge.lib.releaseProc(bridge.handle)
  except Exception:
    discard
  bridge.handle = 0

proc reset*(bridge: TsnetBridge): Result[void, string] =
  if bridge.isNil or bridge.handle <= 0 or bridge.lib.isNil:
    return err("tsnet bridge is not open")
  let rc =
    try:
      bridge.lib.resetProc(bridge.handle)
    except Exception as exc:
      return err("tsnet bridge reset failed: " & exc.msg)
  if rc == 0:
    return err("tsnet bridge reset failed")
  ok()

proc statusPayload*(bridge: TsnetBridge): Result[JsonNode, string] =
  if bridge.isNil or bridge.handle <= 0 or bridge.lib.isNil:
    return err("tsnet bridge is not open")
  callJson(bridge.lib, bridge.lib.statusProc, bridge.handle)

proc pingPayload*(bridge: TsnetBridge, request: JsonNode): Result[JsonNode, string] =
  if bridge.isNil or bridge.handle <= 0 or bridge.lib.isNil:
    return err("tsnet bridge is not open")
  callJson(bridge.lib, bridge.lib.pingProc, bridge.handle, request)

proc derpMapPayload*(bridge: TsnetBridge): Result[JsonNode, string] =
  if bridge.isNil or bridge.handle <= 0 or bridge.lib.isNil:
    return err("tsnet bridge is not open")
  callJson(bridge.lib, bridge.lib.derpMapProc, bridge.handle)

proc listenTcpProxy*(
    bridge: TsnetBridge,
    family: string,
    port: int,
    localAddress: MultiAddress
): Result[MultiAddress, string] =
  let payload = %*{
    "family": family,
    "port": port,
    "localAddress": $localAddress,
  }
  let response = callJson(bridge.lib, bridge.lib.listenTcpProc, bridge.handle, payload).valueOr:
    return err(error)
  let listening = jsonFieldStr(response, "listeningAddress")
  if listening.len == 0:
    return err("tsnet bridge tcp listener missing listeningAddress")
  MultiAddress.init(listening)

proc listenUdpProxy*(
    bridge: TsnetBridge,
    family: string,
    port: int,
    localAddress: MultiAddress
): Result[MultiAddress, string] =
  let payload = %*{
    "family": family,
    "port": port,
    "localAddress": $localAddress,
  }
  let response = callJson(bridge.lib, bridge.lib.listenUdpProc, bridge.handle, payload).valueOr:
    return err(error)
  let listening = jsonFieldStr(response, "listeningAddress")
  if listening.len == 0:
    return err("tsnet bridge udp listener missing listeningAddress")
  MultiAddress.init(listening)

proc dialTcpProxy*(
    bridge: TsnetBridge,
    family, ip: string,
    port: int
): Result[MultiAddress, string] =
  let payload = %*{
    "family": family,
    "ip": ip,
    "port": port,
  }
  let response = callJson(bridge.lib, bridge.lib.dialTcpProc, bridge.handle, payload).valueOr:
    return err(error)
  let proxyAddress = jsonFieldStr(response, "proxyAddress")
  if proxyAddress.len == 0:
    return err("tsnet bridge tcp dial missing proxyAddress")
  MultiAddress.init(proxyAddress)

proc dialUdpProxy*(
    bridge: TsnetBridge,
    family, ip: string,
    port: int
): Result[MultiAddress, string] =
  let payload = %*{
    "family": family,
    "ip": ip,
    "port": port,
  }
  let response = callJson(bridge.lib, bridge.lib.dialUdpProc, bridge.handle, payload).valueOr:
    return err(error)
  let proxyAddress = jsonFieldStr(response, "proxyAddress")
  if proxyAddress.len == 0:
    return err("tsnet bridge udp dial missing proxyAddress")
  MultiAddress.init(proxyAddress)

proc resolveRemote*(bridge: TsnetBridge, rawAddress: MultiAddress): Result[MultiAddress, string] =
  let payload = %*{
    "rawAddress": $rawAddress
  }
  let response = callJson(bridge.lib, bridge.lib.resolveRemoteProc, bridge.handle, payload).valueOr:
    return err(error)
  let remoteAddress = jsonFieldStr(response, "remoteAddress")
  if remoteAddress.len == 0:
    return err("tsnet bridge resolve missing remoteAddress")
  MultiAddress.init(remoteAddress)
