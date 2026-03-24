import std/unittest

import "../libp2p/transports/quicruntime" as quicrt

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
