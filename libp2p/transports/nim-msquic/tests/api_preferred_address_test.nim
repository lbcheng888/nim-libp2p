import unittest

import ../api/api_impl

proc token16(seed: uint8): array[16, uint8] =
  for i in 0 ..< 16:
    result[i] = seed + uint8(i)

suite "MsQuic preferred address path manager":
  test "preferred address probe creates candidate path and can become active":
    var apiPtr: pointer
    check MsQuicOpenVersion(2'u32, addr apiPtr) == QUIC_STATUS_SUCCESS
    defer:
      MsQuicClose(apiPtr)
    let api = cast[ptr QuicApiTable](apiPtr)

    var registration: HQUIC
    check api.RegistrationOpen(nil, addr registration) == QUIC_STATUS_SUCCESS
    defer:
      api.RegistrationClose(registration)

    var connection: HQUIC
    check api.ConnectionOpen(registration, nil, nil, addr connection) == QUIC_STATUS_SUCCESS
    defer:
      api.ConnectionClose(connection)

    check seedConnectionIdsForTest(
      connection,
      @[0x01'u8, 0x02'u8, 0x03'u8, 0x04'u8],
      @[0x11'u8, 0x12'u8, 0x13'u8, 0x14'u8]
    )
    var basePathId = high(uint8)
    check triggerConnectionMigrationProbe(connection, "10.0.0.10", 4442'u16, basePathId)
    check basePathId < high(uint8)
    check confirmConnectionValidatedPath(connection, basePathId)

    let preferredCid = @[0x21'u8, 0x22'u8, 0x23'u8, 0x24'u8]
    var token = token16(0x50)
    check configureConnectionPreferredAddress(
      connection,
      "10.0.0.42",
      4443'u16,
      unsafeAddr preferredCid[0],
      preferredCid.len.uint32,
      unsafeAddr token[0]
    )

    var pathId = high(uint8)
    check triggerPreferredAddressMigration(connection, pathId)
    check pathId > 0'u8

    var activePathId = high(uint8)
    var pathCount = 0'u8
    var isActive = true
    var isValidated = true
    var challengeOutstanding = false
    var responsePending = false
    var activePeerSeq = 0'u64
    var resetTokenCount = 0'u32
    var retiredPeerCidCount = 0'u32

    check getConnectionActivePathId(connection, activePathId)
    check activePathId == basePathId
    check getConnectionKnownPathCount(connection, pathCount)
    check pathCount >= 1'u8
    check getConnectionPathState(
      connection,
      pathId,
      isActive,
      isValidated,
      challengeOutstanding,
      responsePending
    )
    check not isActive
    check not isValidated
    check challengeOutstanding
    check responsePending

    check confirmConnectionValidatedPath(connection, pathId)
    check getConnectionActivePathId(connection, activePathId)
    check activePathId == pathId
    check getConnectionPathState(
      connection,
      pathId,
      isActive,
      isValidated,
      challengeOutstanding,
      responsePending
    )
    check isActive
    check isValidated
    check not challengeOutstanding
    check not responsePending
    check getConnectionPeerCidBytesForTest(connection) == preferredCid
    check getConnectionActivePeerCidSequenceForTest(connection, activePeerSeq)
    check activePeerSeq > 0'u64
    check getConnectionStatelessResetTokenCountForTest(connection, resetTokenCount)
    check resetTokenCount == 1'u32
    check getConnectionRetiredPeerCidCountForTest(connection, retiredPeerCidCount)
    check retiredPeerCidCount == 1'u32
    check getConnectionPathState(
      connection,
      basePathId,
      isActive,
      isValidated,
      challengeOutstanding,
      responsePending
    )
    check not isActive
    check isValidated

  test "repeated preferred probe does not duplicate pending challenges and path response activates preferred path":
    var apiPtr: pointer
    check MsQuicOpenVersion(2'u32, addr apiPtr) == QUIC_STATUS_SUCCESS
    defer:
      MsQuicClose(apiPtr)
    let api = cast[ptr QuicApiTable](apiPtr)

    var registration: HQUIC
    check api.RegistrationOpen(nil, addr registration) == QUIC_STATUS_SUCCESS
    defer:
      api.RegistrationClose(registration)

    var connection: HQUIC
    check api.ConnectionOpen(registration, nil, nil, addr connection) == QUIC_STATUS_SUCCESS
    defer:
      api.ConnectionClose(connection)

    check seedConnectionIdsForTest(
      connection,
      @[0x31'u8, 0x32'u8, 0x33'u8, 0x34'u8],
      @[0x41'u8, 0x42'u8, 0x43'u8, 0x44'u8]
    )

    var basePathId = high(uint8)
    check triggerConnectionMigrationProbe(connection, "10.0.0.11", 4450'u16, basePathId)
    check confirmConnectionValidatedPath(connection, basePathId)

    let preferredCid = @[0x51'u8, 0x52'u8, 0x53'u8, 0x54'u8]
    var token = token16(0x70)
    check configureConnectionPreferredAddress(
      connection,
      "10.0.0.99",
      4451'u16,
      unsafeAddr preferredCid[0],
      preferredCid.len.uint32,
      unsafeAddr token[0]
    )

    var preferredPathId = high(uint8)
    check triggerPreferredAddressMigration(connection, preferredPathId)
    check triggerPreferredAddressMigration(connection, preferredPathId)

    var pendingCount = 0'u32
    var activePathId = high(uint8)
    var challengeData: array[8, uint8]
    var isActive = false
    var isValidated = false
    var challengeOutstanding = false
    var responsePending = false
    var activePeerSeq = 0'u64
    var retiredPeerCidCount = 0'u32

    check getConnectionPendingChallengeCountForTest(connection, pendingCount)
    check pendingCount == 1'u32
    check getConnectionActivePathId(connection, activePathId)
    check activePathId == basePathId
    check getConnectionPathChallengeDataForTest(connection, preferredPathId, challengeData)

    var bogusResponse = challengeData
    bogusResponse[0] = bogusResponse[0] xor 0xFF'u8
    check applyPathResponseForTest(connection, bogusResponse)
    check getConnectionActivePathId(connection, activePathId)
    check activePathId == basePathId

    check applyPathResponseForTest(connection, challengeData)
    check getConnectionActivePathId(connection, activePathId)
    check activePathId == preferredPathId
    check getConnectionPeerCidBytesForTest(connection) == preferredCid
    check getConnectionActivePeerCidSequenceForTest(connection, activePeerSeq)
    check activePeerSeq > 0'u64
    check getConnectionRetiredPeerCidCountForTest(connection, retiredPeerCidCount)
    check retiredPeerCidCount == 1'u32
    check getConnectionPendingChallengeCountForTest(connection, pendingCount)
    check pendingCount == 0'u32
    check getConnectionPathState(
      connection,
      preferredPathId,
      isActive,
      isValidated,
      challengeOutstanding,
      responsePending
    )
    check isActive
    check isValidated
    check not challengeOutstanding
    check not responsePending
    check getConnectionPathState(
      connection,
      basePathId,
      isActive,
      isValidated,
      challengeOutstanding,
      responsePending
    )
    check not isActive
    check isValidated

  test "preferred address activation registers preferred stateless reset token":
    var apiPtr: pointer
    check MsQuicOpenVersion(2'u32, addr apiPtr) == QUIC_STATUS_SUCCESS
    defer:
      MsQuicClose(apiPtr)
    let api = cast[ptr QuicApiTable](apiPtr)

    var registration: HQUIC
    check api.RegistrationOpen(nil, addr registration) == QUIC_STATUS_SUCCESS
    defer:
      api.RegistrationClose(registration)

    var connection: HQUIC
    check api.ConnectionOpen(registration, nil, nil, addr connection) == QUIC_STATUS_SUCCESS
    defer:
      api.ConnectionClose(connection)

    check seedConnectionIdsForTest(
      connection,
      @[0x61'u8, 0x62'u8, 0x63'u8, 0x64'u8],
      @[0x71'u8, 0x72'u8, 0x73'u8, 0x74'u8]
    )
    let prePreferredCid = @[0x75'u8, 0x76'u8, 0x77'u8, 0x78'u8]
    let prePreferredToken = token16(0x88)
    check applyPeerNewConnectionIdForTest(connection, 1'u64, 0'u64, prePreferredCid, prePreferredToken)
    check matchesStatelessResetTokenForTest(connection, prePreferredToken)

    var basePathId = high(uint8)
    check triggerConnectionMigrationProbe(connection, "10.0.1.10", 4460'u16, basePathId)
    check confirmConnectionValidatedPath(connection, basePathId)

    let preferredCid = @[0x81'u8, 0x82'u8, 0x83'u8, 0x84'u8]
    var preferredToken = token16(0x90)
    check configureConnectionPreferredAddress(
      connection,
      "10.0.1.42",
      4461'u16,
      unsafeAddr preferredCid[0],
      preferredCid.len.uint32,
      unsafeAddr preferredToken[0]
    )

    var preferredPathId = high(uint8)
    check triggerPreferredAddressMigration(connection, preferredPathId)
    check confirmConnectionValidatedPath(connection, preferredPathId)

    check matchesStatelessResetTokenForTest(connection, preferredToken)
    check not matchesStatelessResetTokenForTest(connection, prePreferredToken)
    var resetTokenCount = 0'u32
    check getConnectionStatelessResetTokenCountForTest(connection, resetTokenCount)
    check resetTokenCount == 1'u32
