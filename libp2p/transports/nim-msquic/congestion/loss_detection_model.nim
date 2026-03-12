## 丢包检测状态蓝图，对齐 `src/core/loss_detection.c` / `loss_detection.h`。

import ./common
import ../core/packet_model

type
  PacketRecord* = object
    packetNumber*: uint64
    encryptEpoch*: CryptoEpoch
    sentTimeUs*: uint64
    totalBytesSentAtSend*: uint64
    ackEliciting*: bool
    packetLength*: uint16

  LossDetectionModel* = object
    packetsInFlight*: uint32
    packetsInFlightByEpoch*: array[CryptoEpoch, uint32]
    bytesInFlightByEpoch*: array[CryptoEpoch, uint64]
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
    probeCountByEpoch*: array[CryptoEpoch, uint16]

proc initLossDetectionModel*(): LossDetectionModel =
  LossDetectionModel(
    packetsInFlight: 0,
    packetsInFlightByEpoch: default(array[CryptoEpoch, uint32]),
    bytesInFlightByEpoch: default(array[CryptoEpoch, uint64]),
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
    probeCount: 0,
    probeCountByEpoch: default(array[CryptoEpoch, uint16]))

proc recomputeAggregateProbeCount(model: var LossDetectionModel) {.inline.} =
  model.probeCount = 0'u16
  for epoch in CryptoEpoch:
    model.probeCount = max(model.probeCount, model.probeCountByEpoch[epoch])

proc resetProbeTimer*(model: var LossDetectionModel) {.gcsafe.}
proc resetProbeTimerForEpoch*(model: var LossDetectionModel;
    epoch: CryptoEpoch) {.gcsafe.}

proc onPacketSent*(model: var LossDetectionModel, packet: PacketRecord) {.gcsafe.} =
  ## 对应 `QuicLossDetectionOnPacketSent` 的最小抽象。
  model.largestSentPacketNumber = max(model.largestSentPacketNumber, packet.packetNumber)
  model.timeOfLastPacketSent = packet.sentTimeUs
  model.totalBytesSent += uint64(packet.packetLength)
  model.sentQueue.add(packet)
  if packet.ackEliciting:
    inc(model.packetsInFlight)
    inc(model.packetsInFlightByEpoch[packet.encryptEpoch])
    model.bytesInFlightByEpoch[packet.encryptEpoch] += uint64(packet.packetLength)

proc markPacketAcked*(model: var LossDetectionModel; packetNumber: uint64;
    epoch: CryptoEpoch; ackedBytes: var uint32): bool {.gcsafe.} =
  for i, pkt in model.sentQueue:
    if pkt.packetNumber == packetNumber and pkt.encryptEpoch == epoch:
      if pkt.ackEliciting:
        if model.packetsInFlight > 0:
          dec(model.packetsInFlight)
        if model.packetsInFlightByEpoch[epoch] > 0:
          dec(model.packetsInFlightByEpoch[epoch])
        let packetBytes = uint64(pkt.packetLength)
        if model.bytesInFlightByEpoch[epoch] > packetBytes:
          model.bytesInFlightByEpoch[epoch] -= packetBytes
        else:
          model.bytesInFlightByEpoch[epoch] = 0'u64
        ackedBytes += pkt.packetLength.uint32
      model.sentQueue.delete(i)
      return true
  false

proc onPacketsLost*(model: var LossDetectionModel; epoch: CryptoEpoch;
    lostBytes: uint32; lostPackets: uint32) {.gcsafe.} =
  if lostPackets >= model.packetsInFlightByEpoch[epoch]:
    model.packetsInFlightByEpoch[epoch] = 0'u32
  else:
    model.packetsInFlightByEpoch[epoch] -= lostPackets
  if lostPackets >= model.packetsInFlight:
    model.packetsInFlight = 0'u32
  else:
    model.packetsInFlight -= lostPackets
  if model.bytesInFlightByEpoch[epoch] > uint64(lostBytes):
    model.bytesInFlightByEpoch[epoch] -= uint64(lostBytes)
  else:
    model.bytesInFlightByEpoch[epoch] = 0'u64

proc onAckReceived*(model: var LossDetectionModel, ack: AckEventSnapshot) {.gcsafe.} =
  ## 抽象 `QuicLossDetectionProcessAckFrame` 的核心状态更新。
  model.largestAck = max(model.largestAck, ack.largestAck)
  model.largestAckEncryptLevel = ack.ackEpoch
  if ack.ackedRetransmittableBytes > 0'u32:
    model.totalBytesAcked = ack.totalAckedRetransmittableBytes
    model.timeOfLastPacketAcked = ack.timeNow
    if ack.timeOfLargestAckedPacketSent > 0'u64:
      model.timeOfLastAckedPacketSent = ack.timeOfLargestAckedPacketSent
    else:
      model.timeOfLastAckedPacketSent = model.timeOfLastPacketSent
    model.adjustedLastAckedTime = ack.adjustedAckTime
    if ack.totalBytesSentAtLargestAck > 0'u64:
      model.totalBytesSentAtLastAck = ack.totalBytesSentAtLargestAck
    else:
      model.totalBytesSentAtLastAck = model.totalBytesSent
    model.resetProbeTimerForEpoch(ack.ackEpoch)

proc markPacketLost*(model: var LossDetectionModel; packetNumber: uint64;
    epoch: CryptoEpoch) {.gcsafe.} =
  ## 在 Nim 蓝图内移动丢失包，方便后续恢复逻辑。
  for i, pkt in model.sentQueue:
    if pkt.packetNumber == packetNumber and pkt.encryptEpoch == epoch:
      model.lostQueue.add(pkt)
      model.sentQueue.delete(i)
      if pkt.ackEliciting and model.packetsInFlight > 0:
        dec(model.packetsInFlight)
        if model.packetsInFlightByEpoch[pkt.encryptEpoch] > 0:
          dec(model.packetsInFlightByEpoch[pkt.encryptEpoch])
        let packetBytes = uint64(pkt.packetLength)
        if model.bytesInFlightByEpoch[pkt.encryptEpoch] > packetBytes:
          model.bytesInFlightByEpoch[pkt.encryptEpoch] -= packetBytes
        else:
          model.bytesInFlightByEpoch[pkt.encryptEpoch] = 0'u64
      break

proc computeProbeTimeout*(
    model: LossDetectionModel,
    smoothedRttUs: uint64,
    rttVarianceUs: uint64;
    epoch: CryptoEpoch = ceOneRtt
  ): uint64 =
  ## 对应 `QuicLossDetectionComputeProbeTimeout` 的近似。
  let base = smoothedRttUs + (4 * rttVarianceUs)
  base shl int(model.probeCountByEpoch[epoch])

proc onProbeTimeoutFired*(model: var LossDetectionModel; epoch: CryptoEpoch) {.gcsafe.} =
  ## 触发 PTO 时递增退避计数（上限与 QUIC_CLOSE_PTO_COUNT 一致）。
  if model.probeCountByEpoch[epoch] < ClosePtoCount.uint16:
    inc(model.probeCountByEpoch[epoch])
  model.recomputeAggregateProbeCount()

proc resetProbeTimer*(model: var LossDetectionModel) {.gcsafe.} =
  ## 收到有效 ACK 或成功发送后，重置 PTO 退避。
  for epoch in CryptoEpoch:
    model.probeCountByEpoch[epoch] = 0'u16
  model.probeCount = 0'u16

proc resetProbeTimerForEpoch*(model: var LossDetectionModel;
    epoch: CryptoEpoch) {.gcsafe.} =
  ## 仅重置对应 packet space 的 PTO 退避。
  model.probeCountByEpoch[epoch] = 0'u16
  model.recomputeAggregateProbeCount()
