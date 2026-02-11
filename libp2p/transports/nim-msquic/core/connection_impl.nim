## Nim 版连接最小握手实现：复刻 `connection.c` 中 Initial 握手路径的关键步骤，并加入性能池化。

import std/[monotimes, options, times]

import ./common
import ./connection_model
import ./packet_model
import ./packet_builder_impl
import ./stream_send_impl
import ./perf_tuning

type
  ConnectionRole* = enum
    crClient
    crServer

  QuicConnection* = ref object
    role*: ConnectionRole
    model*: QuicConnectionModel
    initialSender*: CryptoStreamSender
    handshakeSender*: CryptoStreamSender
    packetPool*: PreparedPacketPool
    sentPackets*: seq[PreparedPacketHandle]
    handshakeComplete*: bool
    lastBuildNs*: uint64
    totalInitialBuilds*: uint64

proc newConnection*(role: ConnectionRole, cid, peerCid: ConnectionId,
    version: QuicVersion): QuicConnection =
  ## 构建 Nim 端连接对象，并激活默认路径。
  var negotiated = version
  var baseModel = newConnectionModel(cid, peerCid, version, negotiated)
  baseModel.activatePath(0'u8)
  new(result)
  result.role = role
  result.model = baseModel
  result.initialSender = initCryptoStreamSender(ceInitial)
  result.handshakeSender = initCryptoStreamSender(ceHandshake)
  result.packetPool = initPreparedPacketPool()
  result.sentPackets = @[]
  result.handshakeComplete = false
  result.lastBuildNs = 0
  result.totalInitialBuilds = 0

proc queueInitialCrypto*(conn: QuicConnection, data: openArray[uint8]) =
  ## 将 TLS ClientHello 等初始握手数据放入初始 Crypto 发送器。
  conn.initialSender.enqueueCryptoData(data)

proc hasInitialCryptoPending*(conn: QuicConnection): bool =
  conn.initialSender.hasPendingData()

proc buildInitialPacket*(conn: QuicConnection, maxFrameSize: Natural = 1200): PreparedPacketHandle =
  ## 从待发送数据构建首个 Initial 包。
  var chunk: CryptoChunk
  if not conn.initialSender.emitChunk(maxFrameSize, chunk):
    raise newException(ValueError, "无可发送的初始 Crypto 数据")
  let start = getMonoTime()
  result = conn.model.buildInitialPacket(conn.packetPool, chunk)
  let elapsed = getMonoTime() - start
  conn.packetPool.recordBuild(result, elapsed)
  conn.sentPackets.add(result)
  conn.lastBuildNs = uint64(elapsed.inNanoseconds())
  conn.totalInitialBuilds.inc

proc onHandshakeAck*(conn: QuicConnection, epoch: CryptoEpoch) =
  ## 收到 ACK 后更新握手状态并推进到 Established。
  conn.model.recordAckForEpoch(epoch)
  if conn.sentPackets.len > 0:
    var remaining: seq[PreparedPacketHandle] = @[]
    for packet in conn.sentPackets:
      if packet.epoch == epoch:
        conn.packetPool.releasePacket(packet)
      else:
        remaining.add(packet)
    conn.sentPackets = remaining
  if conn.handshakeComplete:
    return
  conn.handshakeComplete = true
  conn.model.handshake.confirmed = true
  conn.model.handshake.peerTransportParamsValid = true
  conn.model.stateFlags.incl(qcfHandshakeComplete)
  conn.model.stateFlags.incl(qcfHandshakeConfirmed)
  conn.model.setPhaseFromFlags()

proc handshakeSucceeded*(conn: QuicConnection): bool =
  conn.handshakeComplete

proc lastSentPacket*(conn: QuicConnection): Option[PreparedPacketHandle] =
  if conn.sentPackets.len == 0:
    return none(PreparedPacketHandle)
  some(conn.sentPackets[^1])

proc packetPoolStats*(conn: QuicConnection): PacketPoolStats =
  conn.packetPool.snapshot()
