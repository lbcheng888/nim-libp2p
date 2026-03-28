import std/[json, os, strutils, unittest]

import ../examples/libnimlibp2p

proc takeCString(value: cstring): string =
  if value.isNil:
    return ""
  result = $value
  libp2p_string_free(value)

suite "libnimlibp2p QUIC runtime config":
  test "init accepts valid QUIC runtime preference":
    let dataDir = getTempDir() / "nim-libp2p-libffi-quic-runtime-valid"
    createDir(dataDir)
    let cfg = %*{
      "dataDir": dataDir,
      "extra": {
        "quicRuntimePreference": "builtin_only"
      }
    }
    let handle = libp2p_node_init(($cfg).cstring)
    check not handle.isNil
    if not handle.isNil:
      libp2p_node_free(handle)

  test "init rejects invalid QUIC runtime preference":
    let dataDir = getTempDir() / "nim-libp2p-libffi-quic-runtime-invalid"
    createDir(dataDir)
    let cfg = %*{
      "dataDir": dataDir,
      "extra": {
        "quicRuntimePreference": "definitely_invalid"
      }
    }
    let handle = libp2p_node_init(($cfg).cstring)
    check handle.isNil
    let err = takeCString(libp2p_get_last_error())
    check err.contains("invalid QUIC runtime preference")

  test "diagnostics default to builtin_only when builtin runtime is compiled in":
    when defined(libp2p_msquic_experimental) and defined(libp2p_msquic_builtin):
      let dataDir = getTempDir() / "nim-libp2p-libffi-quic-runtime-default"
      createDir(dataDir)
      let cfg = %*{
        "dataDir": dataDir,
        "extra": {}
      }
      let handle = libp2p_node_init(($cfg).cstring)
      check not handle.isNil
      if not handle.isNil:
        check libp2p_node_start(handle) == 0
        let raw = takeCString(libp2p_get_diagnostics_json(handle))
        let diagnostics = parseJson(raw)
        check diagnostics.kind == JObject
        check diagnostics.hasKey("quicRuntime")
        check diagnostics["quicRuntime"].kind == JObject
        check diagnostics["quicRuntime"]["requestedPreference"].getStr() == "builtin_only"
        libp2p_node_free(handle)
    else:
      skip()
