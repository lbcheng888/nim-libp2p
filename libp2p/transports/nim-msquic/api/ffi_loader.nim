## MsQuic FFI 加载器：负责动态解析官方 MsQuic C 库并获取 `QUIC_API_TABLE`。

import std/[dynlib, os, sequtils, strformat, strutils]

import ./api_impl as builtin_api

type
  MsQuicStatus* = int32
  MsQuicVersion* = uint32
  QuicApiTablePtr* = pointer

const
  DefaultMsQuicEnvVar* = "NIM_MSQUIC_LIB"
  DefaultMsQuicVersion* = MsQuicVersion(0x00000002)

type
  MsQuicOpenProc = proc(apiTable: ptr QuicApiTablePtr): MsQuicStatus {.cdecl.}
  MsQuicOpenVersionProc = proc(version: MsQuicVersion;
                               apiTable: ptr QuicApiTablePtr): MsQuicStatus {.cdecl.}
  MsQuicCloseProc = proc(apiTable: QuicApiTablePtr): void {.cdecl.}

  MsQuicRuntime* = ref object
    handle*: LibHandle
    path*: string
    apiTable*: QuicApiTablePtr
    closeProc*: MsQuicCloseProc
    status*: MsQuicStatus
    openSymbol*: string
    versionRequested*: MsQuicVersion
    versionNegotiated*: MsQuicVersion

  MsQuicLoadOptions* = object
    explicitPath*: string
    allowFallback*: bool = true
    requestedVersion*: MsQuicVersion = DefaultMsQuicVersion
    preferVersionedOpen*: bool = true
    allowOpenFallback*: bool = true

  MsQuicLoadResult* = object
    success*: bool
    runtime*: MsQuicRuntime
    status*: MsQuicStatus
    attemptedPaths*: seq[string]
    openSymbol*: string
    error*: string

when defined(windows):
  const MsQuicLibraryCandidates = [
    "msquic.dll",
    "msquic.lib"
  ]
else:
  const MsQuicLibraryCandidates = [
    "libmsquic.so",
    "libmsquic.dylib"
  ]

when defined(windows):
  type
    MsQuicOpenProc = proc(apiTable: ptr QuicApiTablePtr): MsQuicStatus {.stdcall.}
    MsQuicOpenVersionProc = proc(version: MsQuicVersion;
                                 apiTable: ptr QuicApiTablePtr): MsQuicStatus {.stdcall.}
    MsQuicCloseProc = proc(apiTable: QuicApiTablePtr): void {.stdcall.}

proc normalizePath(path: string): string =
  if path.len == 0:
    return path
  if dirExists(path) or fileExists(path):
    return absolutePath(path)
  path

proc candidatePaths(options: MsQuicLoadOptions): seq[string] =
  var candidates: seq[string] = @[]
  if options.explicitPath.len > 0:
    candidates.add(options.explicitPath)
  let envPath = getEnv(DefaultMsQuicEnvVar)
  if envPath.len > 0:
    candidates.add(envPath)
  if options.allowFallback:
    for name in MsQuicLibraryCandidates:
      candidates.add(name)
  for cand in candidates:
    if cand.len > 0 and cand notin result:
      result.add cand

proc loadSymbol[T](handle: LibHandle; name: string): T =
  let symbolPtr = handle.symAddr(name)
  if symbolPtr.isNil:
    raise newException(OSError, fmt"无法找到符号 {name}")
  cast[T](symbolPtr)

proc openSymbolOrder(options: MsQuicLoadOptions): seq[string] =
  if options.preferVersionedOpen:
    result.add "MsQuicOpenVersion"
    if options.allowOpenFallback:
      result.add "MsQuicOpen"
  else:
    result.add "MsQuicOpen"
    if options.allowOpenFallback:
      result.add "MsQuicOpenVersion"

proc describeErrors(errors: seq[string]; status: MsQuicStatus): string =
  var parts = errors.filterIt(it.len > 0)
  if status != 0:
    parts.add(fmt"最后状态 {status}")
  if parts.len == 0:
    return "未能解析 MsQuicOpen/MsQuicOpenVersion"
  parts.join("; ")

proc tryLoadFromPath(path: string; options: MsQuicLoadOptions): MsQuicLoadResult =
  let normalized = normalizePath(path)
  var handle = loadLib(path)
  if handle.isNil:
    return MsQuicLoadResult(
      success: false,
      attemptedPaths: @[normalized],
      error: "无法加载库"
    )
  var closeProc: MsQuicCloseProc
  try:
    closeProc = loadSymbol[MsQuicCloseProc](handle, "MsQuicClose")
  except CatchableError as exc:
    unloadLib(handle)
    return MsQuicLoadResult(
      success: false,
      attemptedPaths: @[normalized],
      error: fmt"加载 MsQuicClose 失败: {exc.msg}"
    )

  var errors: seq[string] = @[]
  var tablePtr: QuicApiTablePtr = nil
  var status: MsQuicStatus = 0
  var openSymbol = ""
  var versionNegotiated = MsQuicVersion(0)

  for symbol in openSymbolOrder(options):
    try:
      case symbol
      of "MsQuicOpenVersion":
        let openVersion = loadSymbol[MsQuicOpenVersionProc](handle, symbol)
        status = openVersion(options.requestedVersion, addr tablePtr)
        if status == 0 and not tablePtr.isNil:
          openSymbol = symbol
          versionNegotiated = options.requestedVersion
          break
        if status != 0:
          errors.add fmt"{symbol}: 状态 {status}"
        else:
          errors.add fmt"{symbol}: 未返回有效 QUIC_API_TABLE"
        tablePtr = nil
      of "MsQuicOpen":
        let openProc = loadSymbol[MsQuicOpenProc](handle, symbol)
        status = openProc(addr tablePtr)
        if status == 0 and not tablePtr.isNil:
          openSymbol = symbol
          versionNegotiated = MsQuicVersion(0)
          break
        if status != 0:
          errors.add fmt"{symbol}: 状态 {status}"
        else:
          errors.add fmt"{symbol}: 未返回有效 QUIC_API_TABLE"
        tablePtr = nil
    except CatchableError as exc:
      errors.add fmt"{symbol}: {exc.msg}"
      status = MsQuicStatus(-1)

  if openSymbol.len == 0 or tablePtr.isNil:
    unloadLib(handle)
    return MsQuicLoadResult(
      success: false,
      status: status,
      attemptedPaths: @[normalized],
      error: describeErrors(errors, status)
    )

  MsQuicLoadResult(
    success: true,
    runtime: MsQuicRuntime(
      handle: handle,
      path: normalized,
      apiTable: tablePtr,
      closeProc: closeProc,
      status: status,
      openSymbol: openSymbol,
      versionRequested: options.requestedVersion,
      versionNegotiated: versionNegotiated
    ),
    status: status,
    attemptedPaths: @[normalized],
    openSymbol: openSymbol
  )

proc loadMsQuic*(options: MsQuicLoadOptions = MsQuicLoadOptions()): MsQuicLoadResult =
  # UniMaker Pure Nim Skeleton Mode:
  # Bypass all dynamic library loading. Directly use the internal Nim API stubs.
  var tablePtr: QuicApiTablePtr = nil
  let rawStatus = builtin_api.MsQuicOpenVersion(options.requestedVersion, addr tablePtr)
  let status = MsQuicStatus(int32(rawStatus))
  
  if rawStatus == builtin_api.QUIC_STATUS_SUCCESS and not tablePtr.isNil:
    return MsQuicLoadResult(
      success: true,
      runtime: MsQuicRuntime(
        handle: nil,
        path: "builtin-nim-pure-skeleton",
        apiTable: tablePtr,
        closeProc: builtin_api.MsQuicClose,
        status: status,
        openSymbol: "MsQuicOpenVersion",
        versionRequested: options.requestedVersion,
        versionNegotiated: options.requestedVersion
      ),
      status: status,
      attemptedPaths: @["builtin-nim-pure-skeleton"],
      openSymbol: "MsQuicOpenVersion"
    )
  
  MsQuicLoadResult(
    success: false,
    status: status,
    attemptedPaths: @["builtin-nim-pure-skeleton"],
    error: "Failed to initialize Pure Nim Skeleton: " & $rawStatus
  )

proc unloadMsQuic*(runtime: var MsQuicRuntime) =
  if runtime.isNil:
    return
  if not runtime.apiTable.isNil and runtime.closeProc != nil:
    runtime.closeProc(runtime.apiTable)
  if not runtime.handle.isNil:
    unloadLib(runtime.handle)
  runtime.apiTable = nil
  runtime.handle = nil
  runtime.closeProc = nil
  runtime.openSymbol = ""
  runtime.versionNegotiated = MsQuicVersion(0)
  runtime.versionRequested = MsQuicVersion(0)
  runtime.status = 0
  runtime.path = ""
  runtime = nil

proc quicApiTablePtr*(runtime: MsQuicRuntime): QuicApiTablePtr =
  if runtime.isNil:
    return nil
  runtime.apiTable

template asQuicApiTable*[T](runtime: MsQuicRuntime): ptr T =
  ## 将 FFI 表指针转换为调用方自定义的 `ptr T` 类型。
  let raw = quicApiTablePtr(runtime)
  if raw.isNil:
    nil
  else:
    cast[ptr T](raw)
