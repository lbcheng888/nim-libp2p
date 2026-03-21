import std/unittest

suite "Quic handshake":
  test "legacy OpenSSL QUIC handshake test disabled":
    checkpoint("quictransport has been removed; use builtin/native MsQuic transport tests instead")
    skip()
