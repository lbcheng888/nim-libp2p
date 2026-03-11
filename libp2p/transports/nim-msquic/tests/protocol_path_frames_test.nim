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
