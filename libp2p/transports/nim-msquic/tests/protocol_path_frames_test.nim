import unittest

import ../protocol/protocol_core

proc challengeBytes(seed: uint8): array[8, byte] =
  for i in 0 .. 7:
    result[i] = seed + byte(i)

suite "QUIC path frames":
  test "PATH_CHALLENGE round-trips":
    let challenge = challengeBytes(0x10)
    let encoded = encodePathChallengeFrame(challenge)
    var pos = 0
    let parsed = parsePathChallengeFrame(encoded, pos)
    check pos == encoded.len
    check parsed.data == challenge

  test "PATH_RESPONSE round-trips":
    let response = challengeBytes(0x20)
    let encoded = encodePathResponseFrame(response)
    var pos = 0
    let parsed = parsePathResponseFrame(encoded, pos)
    check pos == encoded.len
    check parsed.data == response

  test "NEW_CONNECTION_ID round-trips":
    let cid = @[0xAA'u8, 0xBB'u8, 0xCC'u8, 0xDD'u8]
    var token: array[16, byte]
    for i in 0 ..< 16:
      token[i] = byte(0x30 + i)
    let encoded = encodeNewConnectionIdFrame(7'u64, 3'u64, cid, token)
    var pos = 0
    let parsed = parseNewConnectionIdFrame(encoded, pos)
    check pos == encoded.len
    check parsed.sequence == 7'u64
    check parsed.retirePriorTo == 3'u64
    check parsed.connectionId == cid
    check parsed.statelessResetToken == token

  test "RETIRE_CONNECTION_ID round-trips":
    let encoded = encodeRetireConnectionIdFrame(5'u64)
    var pos = 0
    let parsed = parseRetireConnectionIdFrame(encoded, pos)
    check pos == encoded.len
    check parsed.sequence == 5'u64
