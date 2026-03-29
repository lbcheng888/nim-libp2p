import std/[os, unittest]

import "../libp2p/transports/quicruntime" as quicrt
when defined(libp2p_msquic_experimental):
  import "../libp2p/transports/msquicruntime" as msquicrt

suite "QuicRuntime kind metadata":
  test "runtime kind labels only expose in-repo implementations":
    check quicrt.kindLabel(quicrt.qrkUnavailable) == "unavailable"
    check quicrt.kindLabel(quicrt.qrkMsQuicBuiltin) == "msquic_builtin"
    check quicrt.kindLabel(quicrt.qrkMsQuicNative) == "msquic_native"

  test "pure nim runtime detection only matches builtin runtime":
    let builtin = quicrt.QuicRuntimeInfo(kind: quicrt.qrkMsQuicBuiltin)
    let native = quicrt.QuicRuntimeInfo(kind: quicrt.qrkMsQuicNative)
    let unavailable = quicrt.QuicRuntimeInfo(kind: quicrt.qrkUnavailable)

    check quicrt.isPureNimRuntime(builtin)
    check not quicrt.isPureNimRuntime(native)
    check not quicrt.isPureNimRuntime(unavailable)

  test "builtin runtime bridge is self-hosted when compiled in":
    when defined(libp2p_msquic_experimental) and defined(libp2p_msquic_builtin):
      let bridgeRes = msquicrt.acquireMsQuicBridge()
      check bridgeRes.success
      check not bridgeRes.bridge.isNil
      let info = quicrt.currentQuicRuntimeInfo()
      check info.loaded
      check info.kind == quicrt.qrkMsQuicBuiltin
      check info.compileTimeBuiltin
      msquicrt.releaseMsQuicBridge(bridgeRes.bridge)
    else:
      skip()

  test "native runtime path is disabled in favor of builtin runtime":
    when defined(libp2p_msquic_experimental) and defined(libp2p_msquic_builtin):
      var cfg = quicrt.QuicRuntimeConfig()
      cfg.useNativeRuntime()
      let bridgeRes = msquicrt.acquireMsQuicBridge(cfg.loadOptions)
      check bridgeRes.success
      check not bridgeRes.bridge.isNil
      let info = quicrt.currentQuicRuntimeInfo()
      check info.loaded
      check info.kind == quicrt.qrkMsQuicBuiltin
      check info.compileTimeBuiltin
      msquicrt.releaseMsQuicBridge(bridgeRes.bridge)
    else:
      skip()

  test "builtin runtime bridge can be reopened after full release":
    when defined(libp2p_msquic_experimental) and defined(libp2p_msquic_builtin):
      for _ in 0 ..< 3:
        let first = msquicrt.acquireMsQuicBridge()
        check first.success
        check not first.bridge.isNil
        msquicrt.releaseMsQuicBridge(first.bridge)

        let second = msquicrt.acquireMsQuicBridge()
        check second.success
        check not second.bridge.isNil
        msquicrt.releaseMsQuicBridge(second.bridge)
    else:
      skip()
