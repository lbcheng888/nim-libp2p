## Nim 版 packet_builder：覆盖最小握手路径所需的长包头与 Crypto 帧封装。

import ./common
import ./packet_model
import ./connection_model
import ./stream_send_impl
import ./perf_tuning

when compiles((proc () {.noGc.} = discard)):
  {.pragma: quicHotPath, noGc, raises: [ValueError].}
else:
  {.pragma: quicHotPath, gcsafe, raises: [ValueError].}

proc makeInvariantHeader*(conn: QuicConnectionModel): PacketHeaderInvariant {.quicHotPath.} =
  PacketHeaderInvariant(
    isLongHeader: true,
    version: conn.attemptedVersion,
    destinationCid: conn.peerConnectionId,
    sourceCid: conn.connectionId)

proc buildLongHeader*(conn: QuicConnectionModel, packetType: LongHeaderType,
    packetNumber: uint32): QuicPacketHeader {.quicHotPath.} =
  QuicPacketHeader(
    form: pfLongHeader,
    longHdr: PacketHeaderLong(
      invariant: makeInvariantHeader(conn),
      packetType: packetType,
      tokenLength: QuicVarInt(0),
      packetNumber: packetNumber))

proc nextPacketNumber*(space: var PacketNumberSpaceState): uint32 {.quicHotPath.} =
  let nextValue = space.largestSent + 1
  assert nextValue <= uint64(high(uint32)), "packet number overflow"
  space.recordSentPacket(nextValue)
  uint32(nextValue)

proc populateInitialPacket*(conn: var QuicConnectionModel,
    packet: PreparedPacketHandle, chunk: CryptoChunk) {.quicHotPath.} =
  ## 填充 Initial 包内容，供内存池重复使用。
  assert chunk.epoch == ceInitial, "Initial 包只允许携带 Initial Crypto 数据"
  let packetNumber = conn.packetSpaces[ceInitial].nextPacketNumber()
  let header = conn.buildLongHeader(lhtInitial, packetNumber)
  packet.setHeader(header)
  packet.setMetadata(chunk.epoch, packetNumber)
  discard packet.appendCryptoFrame(chunk.offset, chunk.data)
  conn.stateFlags.incl(qcfHandshakeStarted)
  conn.stateFlags.excl(qcfHandshakeComplete)
  conn.stateFlags.excl(qcfHandshakeConfirmed)
  conn.handshake.retryUsed = false
  conn.handshake.confirmed = false
  conn.handshake.peerTransportParamsValid = false
  conn.handshake.zeroRttAccepted = false
  conn.setPhaseFromFlags()

proc recordAckForEpoch*(conn: var QuicConnectionModel, epoch: CryptoEpoch) =
  ## 在最小实现中，直接将该 epoch 的最大包号视为已确认。
  let largest = conn.packetSpaces[epoch].largestSent
  if largest > 0:
    conn.packetSpaces[epoch].recordAckedPacket(largest)

proc buildInitialPacket*(conn: var QuicConnectionModel,
    pool: PreparedPacketPool, chunk: CryptoChunk): PreparedPacketHandle =
  ## 从内存池获取句柄并构建首个 Initial 包。
  let packet = pool.acquirePacket()
  conn.populateInitialPacket(packet, chunk)
  packet
