import unittest

import ../api/api_impl

proc token16(seed: uint8): array[16, uint8] =
  for i in 0 ..< 16:
    result[i] = seed + uint8(i)

suite "MsQuic builtin connection-id frames":
  test "peer NEW_CONNECTION_ID updates peer CID and records reset tokens":
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

    check seedConnectionIdsForTest(connection, @[0x01'u8, 0x02'u8, 0x03'u8, 0x04'u8], @[0x05'u8, 0x06'u8, 0x07'u8, 0x08'u8])
    check getConnectionPeerCidBytesForTest(connection) == @[0x05'u8, 0x06'u8, 0x07'u8, 0x08'u8]
    var activePeerSeq = high(uint64)
    var peerCidCatalogCount = 0'u32
    var retiredPeerCidCount = 0'u32
    check getConnectionActivePeerCidSequenceForTest(connection, activePeerSeq)
    check activePeerSeq == 0'u64
    check getConnectionPeerCidCatalogCountForTest(connection, peerCidCatalogCount)
    check peerCidCatalogCount == 1'u32
    check getConnectionRetiredPeerCidCountForTest(connection, retiredPeerCidCount)
    check retiredPeerCidCount == 0'u32

    let firstCid = @[0x20'u8, 0x21'u8, 0x22'u8, 0x23'u8]
    let firstToken = token16(0x10)
    check applyPeerNewConnectionIdForTest(connection, 1'u64, 0'u64, firstCid, firstToken)
    check getConnectionPeerCidBytesForTest(connection) == firstCid
    check getConnectionActivePeerCidSequenceForTest(connection, activePeerSeq)
    check activePeerSeq == 1'u64
    check getConnectionRetiredPeerCidCountForTest(connection, retiredPeerCidCount)
    check retiredPeerCidCount == 1'u32
    check matchesStatelessResetTokenForTest(connection, firstToken)

    var resetTokenCount = 0'u32
    check getConnectionStatelessResetTokenCountForTest(connection, resetTokenCount)
    check resetTokenCount == 1'u32

    let olderCid = @[0x30'u8, 0x31'u8, 0x32'u8, 0x33'u8]
    check applyPeerNewConnectionIdForTest(connection, 0'u64, 0'u64, olderCid, token16(0x20))
    check getConnectionPeerCidBytesForTest(connection) == firstCid
    check getConnectionActivePeerCidSequenceForTest(connection, activePeerSeq)
    check activePeerSeq == 1'u64
    check getConnectionPeerCidCatalogCountForTest(connection, peerCidCatalogCount)
    check peerCidCatalogCount == 2'u32
    check getConnectionRetiredPeerCidCountForTest(connection, retiredPeerCidCount)
    check retiredPeerCidCount == 1'u32
    check getConnectionStatelessResetTokenCountForTest(connection, resetTokenCount)
    check resetTokenCount == 1'u32

    let newerCid = @[0x40'u8, 0x41'u8, 0x42'u8, 0x43'u8]
    let newerToken = token16(0x30)
    check applyPeerNewConnectionIdForTest(connection, 2'u64, 2'u64, newerCid, newerToken)
    check getConnectionPeerCidBytesForTest(connection) == newerCid
    check getConnectionActivePeerCidSequenceForTest(connection, activePeerSeq)
    check activePeerSeq == 2'u64
    check getConnectionPeerCidCatalogCountForTest(connection, peerCidCatalogCount)
    check peerCidCatalogCount == 3'u32
    check getConnectionRetiredPeerCidCountForTest(connection, retiredPeerCidCount)
    check retiredPeerCidCount == 2'u32
    check getConnectionStatelessResetTokenCountForTest(connection, resetTokenCount)
    check resetTokenCount == 1'u32
    check not matchesStatelessResetTokenForTest(connection, firstToken)
    check matchesStatelessResetTokenForTest(connection, newerToken)

  test "peer RETIRE_CONNECTION_ID rotates local CID":
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

    check seedConnectionIdsForTest(connection, @[0xA1'u8, 0xA2'u8, 0xA3'u8, 0xA4'u8], @[0xB1'u8, 0xB2'u8, 0xB3'u8, 0xB4'u8])
    let originalLocalCid = getConnectionLocalCidBytesForTest(connection)
    check originalLocalCid == @[0xA1'u8, 0xA2'u8, 0xA3'u8, 0xA4'u8]

    check advertiseLocalConnectionIdForTest(connection)
    check applyPeerRetireConnectionIdForTest(connection, 0'u64)
    let rotatedLocalCid = getConnectionLocalCidBytesForTest(connection)
    check rotatedLocalCid.len > 0
    check rotatedLocalCid != originalLocalCid

    var resetTokenCount = 0'u32
    check getConnectionStatelessResetTokenCountForTest(connection, resetTokenCount)
    check resetTokenCount == 1'u32

    check not applyPeerRetireConnectionIdForTest(connection, 99'u64)
