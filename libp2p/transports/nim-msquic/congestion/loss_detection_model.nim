## 丢包检测状态蓝图，对齐 `src/core/loss_detection.c` / `loss_detection.h`。

import ./common
import ../core/packet_model

type
  PacketRecord* = object
    packetNumber*: uint64
    encryptEpoch*: CryptoEpoch
    sentTimeUs*: uint64
    ackEliciting*: bool
    packetLength*: uint16

  LossDetectionModel* = object
    packetsInFlight*: uint32
    largestAck*: uint64
    largestAckEncryptLevel*: CryptoEpoch
    timeOfLastPacketSent*: uint64
    timeOfLastPacketAcked*: uint64
    timeOfLastAckedPacketSent*: uint64
    adjustedLastAckedTime*: uint64
    totalBytesSent*: uint64
    totalBytesAcked*: uint64
    totalBytesSentAtLastAck*: uint64
    largestSentPacketNumber*: uint64
    sentQueue*: seq[PacketRecord]
    lostQueue*: seq[PacketRecord]
    probeCount*: uint16

proc initLossDetectionModel*(): LossDetectionModel =
  LossDetectionModel(
    packetsInFlight: 0,
    largestAck: 0,
    largestAckEncryptLevel: ceInitial,
    timeOfLastPacketSent: 0,
    timeOfLastPacketAcked: 0,
    timeOfLastAckedPacketSent: 0,
    adjustedLastAckedTime: 0,
    totalBytesSent: 0,
    totalBytesAcked: 0,
    totalBytesSentAtLastAck: 0,
    largestSentPacketNumber: 0,
    sentQueue: @[],
    lostQueue: @[],
    probeCount: 0)

proc onPacketSent*(model: var LossDetectionModel, packet: PacketRecord) =
  ## 对应 `QuicLossDetectionOnPacketSent` 的最小抽象。
  model.largestSentPacketNumber = max(model.largestSentPacketNumber, packet.packetNumber)
  model.timeOfLastPacketSent = packet.sentTimeUs
  model.totalBytesSent += uint64(packet.packetLength)
  model.sentQueue.add(packet)
  if packet.ackEliciting:
    inc(model.packetsInFlight)

proc onAckReceived*(model: var LossDetectionModel, ack: AckEventSnapshot) =
  ## 抽象 `QuicLossDetectionProcessAckFrame` 的核心状态更新。
  model.largestAck = max(model.largestAck, ack.largestAck)
  model.totalBytesAcked = ack.totalAckedRetransmittableBytes
  model.timeOfLastPacketAcked = ack.timeNow
  model.timeOfLastAckedPacketSent = model.timeOfLastPacketSent
  model.adjustedLastAckedTime = ack.adjustedAckTime
  model.totalBytesSentAtLastAck = model.totalBytesSent
  model.resetProbeTimer()
  if model.packetsInFlight > 0 and ack.hasLoss:
    dec(model.packetsInFlight)

proc markPacketLost*(model: var LossDetectionModel, packetNumber: uint64) =
  ## 在 Nim 蓝图内移动丢失包，方便后续恢复逻辑。
  for i, pkt in model.sentQueue:
    if pkt.packetNumber == packetNumber:
      model.lostQueue.add(pkt)
      model.sentQueue.delete(i)
      if pkt.ackEliciting and model.packetsInFlight > 0:
        dec(model.packetsInFlight)
      break

proc computeProbeTimeout*(
    model: LossDetectionModel,
    smoothedRttUs: uint64,
    rttVarianceUs: uint64
  ): uint64 =
  ## 对应 `QuicLossDetectionComputeProbeTimeout` 的近似。
  let base = smoothedRttUs + (4 * rttVarianceUs)
  base shl int(model.probeCount)

proc onProbeTimeoutFired*(model: var LossDetectionModel) =
  ## 触发 PTO 时递增退避计数（上限与 QUIC_CLOSE_PTO_COUNT 一致）。
  if model.probeCount < ClosePtoCount.uint16:
    inc(model.probeCount)

proc resetProbeTimer*(model: var LossDetectionModel) =
  ## 收到有效 ACK 或成功发送后，重置 PTO 退避。
  model.probeCount = 0
