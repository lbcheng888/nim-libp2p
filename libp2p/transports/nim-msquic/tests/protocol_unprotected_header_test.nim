import unittest

import ../protocol/protocol_core as proto

suite "QUIC unprotected header parsing":
  test "parseUnprotectedHeader accepts zero-length connection ids":
    let packet = @[
      0xE0'u8,
      0x00'u8, 0x00'u8, 0x00'u8, 0x01'u8,
      0x00'u8,
      0x00'u8,
      0x00'u8
    ]

    let header = proto.parseUnprotectedHeader(packet)
    check header.firstByte == 0xE0'u8
    check header.version == 1'u32
    check header.destConnectionId.len == 0
    check header.srcConnectionId.len == 0
    check header.payloadOffset == packet.len
    check header.payloadLength == 0'u64
    check header.packetLength == packet.len

  test "decodePacket accepts zero-length connection ids":
    let packet = @[
      0xE0'u8,
      0x00'u8, 0x00'u8, 0x00'u8, 0x01'u8,
      0x00'u8,
      0x00'u8,
      0x00'u8
    ]

    let decoded = proto.decodePacket(packet)
    check decoded.isLongHeader
    check decoded.longHeader.version == 1'u32
    check decoded.longHeader.destConnectionId.len == 0
    check decoded.longHeader.srcConnectionId.len == 0
    check decoded.payload.len == 0
