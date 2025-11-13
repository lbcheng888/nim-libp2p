import std/[os, unittest]

import ../api/ffi_loader

type
  DummyApiTable = object

suite "MsQuic FFI loader (F1)":
  test "加载不存在的 MsQuic 库返回失败并记录路径":
    let bogusPath =
      when defined(windows):
        r"C:\not_exists\msquic.dll"
      else:
        "/tmp/not_exists/libmsquic.so"
    let result = loadMsQuic(MsQuicLoadOptions(explicitPath: bogusPath, allowFallback: false))
    check result.success == false
    check result.runtime.isNil
    check bogusPath in result.attemptedPaths
    check result.error.len > 0
    check result.openSymbol.len == 0

  test "当环境变量指向错误路径时保持可恢复":
    let original = getEnv(DefaultMsQuicEnvVar)
    let bogus =
      when defined(windows):
        r"C:\invalid\msquic.dll"
      else:
        "/invalid/libmsquic.so"
    putEnv(DefaultMsQuicEnvVar, bogus)
    defer:
      if original.len == 0:
        delEnv(DefaultMsQuicEnvVar)
      else:
        putEnv(DefaultMsQuicEnvVar, original)
    let result = loadMsQuic()
    check result.success == false
    check bogus in result.attemptedPaths
    check result.runtime.isNil
    check result.openSymbol.len == 0

  test "重复卸载安全且幂等":
    var runtime: MsQuicRuntime = nil
    unloadMsQuic(runtime) # no-op
    check runtime.isNil

  test "quicApiTablePtr/asQuicApiTable 在空 runtime 上返回 nil":
    check quicApiTablePtr(nil).isNil
    let runtime = MsQuicRuntime(
      handle: nil,
      path: "",
      apiTable: nil,
      closeProc: nil,
      status: 0,
      openSymbol: "",
      versionRequested: DefaultMsQuicVersion,
      versionNegotiated: MsQuicVersion(0)
    )
    check quicApiTablePtr(runtime).isNil
    check asQuicApiTable[DummyApiTable](runtime).isNil

  test "asQuicApiTable 返回强类型 API 表指针":
    var table: DummyApiTable
    let runtime = MsQuicRuntime(
      handle: nil,
      path: "",
      apiTable: cast[QuicApiTablePtr](addr table),
      closeProc: nil,
      status: 0,
      openSymbol: "MsQuicOpen",
      versionRequested: DefaultMsQuicVersion,
      versionNegotiated: DefaultMsQuicVersion
    )
    let tablePtr = asQuicApiTable[DummyApiTable](runtime)
    check tablePtr == addr table
