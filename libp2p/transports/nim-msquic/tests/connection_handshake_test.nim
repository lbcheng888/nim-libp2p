import std/unittest

import "../core/mod"

when isMainModule:
  suite "Nim MsQuic Initial Handshake":
    test "client builds Initial packet with Crypto payload":
      let clientCid = initConnectionId(@[0xAA'u8, 0xBB'u8])
      let serverCid = initConnectionId(@[0x01'u8, 0x02'u8, 0x03'u8, 0x04'u8])
      let version = QuicVersion(0x1)

      var conn = newConnection(crClient, clientCid, serverCid, version)
      check conn.model.phase == qcpIdle

      let cryptoPayload = @[0x01'u8, 0x00'u8, 0xFD'u8, 0xE0'u8]
      conn.queueInitialCrypto(cryptoPayload)
      check conn.hasInitialCryptoPending()

      let packetHandle = conn.buildInitialPacket()
      check packetHandle.header.form == pfLongHeader
      check packetHandle.header.longHdr.packetType == lhtInitial
      check packetHandle.header.longHdr.packetNumber == 1'u32
      check packetHandle.frameCount == 1
      let frame = packetHandle.frameAt(0)
      check frame.frameType == qftCrypto
      check frame.cryptoOffset == 0
      check cryptoSpanToSeq(frame.cryptoData) == cryptoPayload
      check packetHandle.payloadLength == uint16(cryptoPayload.len)
      check conn.model.phase == qcpHandshake

      conn.onHandshakeAck(ceInitial)
      check conn.handshakeSucceeded()
      check conn.model.phase == qcpEstablished
      check qcfHandshakeConfirmed in conn.model.stateFlags
      check conn.lastBuildNs > 0
      check conn.totalInitialBuilds == 1
      let stats = conn.packetPoolStats()
      check stats.builtPackets == 1
      check stats.releases == 1
      check stats.maxInUse >= 1
