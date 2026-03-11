import std/unittest

import ../protocol/protocol_core

suite "QUIC stream control frames":
  test "RESET_STREAM round-trips":
    let encoded = encodeResetStreamFrame(7'u64, 99'u64, 1234'u64)
    var pos = 0
    let parsed = parseResetStreamFrame(encoded, pos)
    check parsed.streamId == 7'u64
    check parsed.applicationErrorCode == 99'u64
    check parsed.finalSize == 1234'u64
    check pos == encoded.len

  test "STOP_SENDING round-trips":
    let encoded = encodeStopSendingFrame(11'u64, 55'u64)
    var pos = 0
    let parsed = parseStopSendingFrame(encoded, pos)
    check parsed.streamId == 11'u64
    check parsed.applicationErrorCode == 55'u64
    check pos == encoded.len
