import std/unittest

import ../api/ffi_loader

suite "MsQuic FFI loader builtin":
  test "编译期开关强制走 builtin runtime":
    check CompileTimeBuiltinMsQuic
    let result = loadMsQuic(
      MsQuicLoadOptions(
        explicitPath: "/definitely/not/used/libmsquic.so",
        allowFallback: false
      )
    )
    check result.success
    check result.error.len == 0
    check result.openSymbol == "MsQuicOpenVersion"
    check result.runtime.path == "builtin-nim-quic-native"
    check result.attemptedPaths == @["builtin-nim-quic-native"]
