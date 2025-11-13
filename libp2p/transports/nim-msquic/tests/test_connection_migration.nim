## 验证连接迁移与 Stateless Reset 蓝图状态（C1）。

import unittest
import std/sequtils
import ../core/common
import ../core/connection_model

proc token16(value: uint8): array[16, uint8] =
  for i in 0 ..< 16:
    result[i] = value + uint8(i)

proc challenge8(value: uint8): array[8, uint8] =
  for i in 0 ..< 8:
    result[i] = value + uint8(i)

suite "Connection migration state":
  test "path challenge lifecycle":
    var conn = newConnectionModel(
      initConnectionId(@[1'u8,2'u8,3'u8,4'u8]),
      initConnectionId(@[5'u8,6'u8,7'u8,8'u8]),
      attempted = 0x1,
      negotiated = 0x1)
    conn.initiatePathChallenge(1, challenge8(10))
    check conn.migration.pendingChallenges.len == 1
    conn.completePathValidation(1, true)
    check conn.migration.activePathId == 1
    check conn.migration.validatedPaths.contains(1)

  test "stateless reset tokens are tracked":
    var conn = newConnectionModel(
      initConnectionId(@[1'u8]),
      initConnectionId(@[2'u8]),
      attempted = 0x1,
      negotiated = 0x1)
    conn.registerStatelessReset(token16(0x10))
    check conn.migration.statelessResetTokens.len == 1
    conn.ensurePath(0).recordStatelessResetToken(token16(0x20))
    check conn.paths[0].resetToken == token16(0x20)

  test "preferred address configuration":
    var conn = newConnectionModel(
      initConnectionId(@[11'u8]),
      initConnectionId(@[12'u8]),
      attempted = 0x1,
      negotiated = 0x1)
    let preferred = PreferredAddressState(
      ipv4Address: "10.0.0.1",
      ipv4Port: 4443,
      ipv6Address: "::1",
      ipv6Port: 4443,
      hasPreferred: true,
      cid: initConnectionId(@[9'u8,9'u8,9'u8,9'u8]),
      statelessResetToken: token16(0xAA))
    conn.configurePreferredAddress(preferred)
    check conn.migration.preferredAddress.ipv4Address == "10.0.0.1"
    check conn.migration.preferredAddress.hasPreferred
