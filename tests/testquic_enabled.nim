{.used.}

import std/[strutils, unittest]

const QuicTransportSource = staticRead("../libp2p/transports/quictransport.nim")

suite "Quic transport":
  test "legacy OpenSSL QUIC transport path is intentionally removed":
    check QuicTransportSource.contains("OpenSSL QUIC transport path has been removed")
    check QuicTransportSource.contains("msquictransport only")
