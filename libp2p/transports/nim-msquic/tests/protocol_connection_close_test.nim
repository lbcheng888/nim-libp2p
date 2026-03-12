import unittest

import ../protocol/protocol_core as proto

suite "QUIC CONNECTION_CLOSE protocol":
  test "application close round-trips":
    let encoded = proto.encodeConnectionCloseFrame(
      42'u64,
      reasonPhrase = "app-close",
      applicationClose = true
    )
    var pos = 0
    let parsed = proto.parseConnectionCloseFrame(encoded, pos)
    check parsed.applicationClose
    check not parsed.hasFrameType
    check parsed.errorCode == 42'u64
    check parsed.reasonPhrase == "app-close"
    check pos == encoded.len

  test "transport close round-trips frame type":
    let encoded = proto.encodeConnectionCloseFrame(
      7'u64,
      reasonPhrase = "transport-close",
      frameType = 0x06'u64,
      applicationClose = false
    )
    var pos = 0
    let parsed = proto.parseConnectionCloseFrame(encoded, pos)
    check not parsed.applicationClose
    check parsed.hasFrameType
    check parsed.frameType == 0x06'u64
    check parsed.errorCode == 7'u64
    check parsed.reasonPhrase == "transport-close"
    check pos == encoded.len
