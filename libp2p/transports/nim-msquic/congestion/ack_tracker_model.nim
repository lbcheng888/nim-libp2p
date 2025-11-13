## ACK 跟踪器蓝图，对齐 `src/core/ack_tracker.c` / `ack_tracker.h`。

import ./common

type
  PacketRange* = object
    first*: uint64
    last*: uint64

  AckTrackerModel* = object
    packetNumbersReceived*: seq[PacketRange]
    packetNumbersToAck*: seq[PacketRange]
    receivedEcn*: AckEcnSummary
    largestPacketNumberAcked*: uint64
    largestPacketNumberRecvTime*: uint64
    counters*: AckTrackerCounters

proc initAckTracker*(): AckTrackerModel =
  AckTrackerModel(
    packetNumbersReceived: @[],
    packetNumbersToAck: @[],
    receivedEcn: AckEcnSummary(ect0Count: 0, ect1Count: 0, ceCount: 0),
    largestPacketNumberAcked: 0,
    largestPacketNumberRecvTime: 0,
    counters: AckTrackerCounters(
      ackElicitingPacketsToAck: 0,
      alreadyWrittenAckFrame: false,
      nonZeroRecvEcn: false))

proc trackIncomingPacket*(tracker: var AckTrackerModel, packetNumber: uint64) =
  ## 追加一个已经接收的包号区间，用于重复包检测。
  if tracker.packetNumbersReceived.len == 0 or
      packetNumber > tracker.packetNumbersReceived[^1].last + 1:
    tracker.packetNumbersReceived.add(PacketRange(first: packetNumber, last: packetNumber))
  else:
    tracker.packetNumbersReceived[^1].last = max(tracker.packetNumbersReceived[^1].last, packetNumber)

proc markForAck*(tracker: var AckTrackerModel, packetNumber: uint64, recvTimeUs: uint64,
    ackType: AckType, ackDelayMs: uint32 = MaxAckDelayDefaultMs) =
  ## 抽象 ACK 聚合逻辑，便于 Nim 端推演 ACK 触发条件。
  if tracker.packetNumbersToAck.len == 0 or
      packetNumber > tracker.packetNumbersToAck[^1].last + 1:
    tracker.packetNumbersToAck.add(PacketRange(first: packetNumber, last: packetNumber))
  else:
    tracker.packetNumbersToAck[^1].last = max(tracker.packetNumbersToAck[^1].last, packetNumber)
  tracker.counters.ackElicitingPacketsToAck.inc
  tracker.largestPacketNumberAcked = max(tracker.largestPacketNumberAcked, packetNumber)
  tracker.largestPacketNumberRecvTime = recvTimeUs
  if ackType == ackTypeAckImmediate or ackDelayMs == 0:
    tracker.counters.alreadyWrittenAckFrame = false

proc shouldSendAck*(tracker: AckTrackerModel, packetTolerance: uint8,
    ackType: AckType, reordered: bool): bool =
  ## 对应 `QuicAckTrackerAckPacket` 中的触发阈值。
  if ackType == ackTypeAckImmediate:
    return true
  if tracker.counters.ackElicitingPacketsToAck >= uint16(packetTolerance):
    return true
  if reordered:
    return true
  false
