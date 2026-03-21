import std/[strutils, unittest]

const WrapperSource = staticRead("../libp2p/transports/msquicwrapper.nim")

suite "MsQuic wrapper compatibility":
  test "legacy wrapper module is intentionally removed":
    check WrapperSource.contains("compatibility layer has been removed")
    check WrapperSource.contains("msquictransport only")
