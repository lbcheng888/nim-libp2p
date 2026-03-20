## ACK 跟踪器蓝图，对齐 `src/core/ack_tracker.c` / `ack_tracker.h`。

import ./common
import ../core/packet_model

type
  PacketRange* = object
    first*: uint64
    last*: uint64

  AckEpochState* = object
    packetNumbersReceived*: seq[PacketRange]
    packetNumbersToAck*: seq[PacketRange]
    largestPacketNumberAcked*: uint64
    largestPacketNumberRecvTime*: uint64
    ackDeadlineUs*: uint64
    counters*: AckTrackerCounters

  AckTrackerModel* = object
    epochs*: array[CryptoEpoch, AckEpochState]
    receivedEcn*: AckEcnSummary

proc initAckTracker*(): AckTrackerModel =
  result.receivedEcn = AckEcnSummary(ect0Count: 0, ect1Count: 0, ceCount: 0)
  for epoch in CryptoEpoch:
    result.epochs[epoch] = AckEpochState(
      packetNumbersReceived: @[],
      packetNumbersToAck: @[],
      largestPacketNumberAcked: 0,
      largestPacketNumberRecvTime: 0,
      ackDeadlineUs: 0,
      counters: AckTrackerCounters(
        ackElicitingPacketsToAck: 0,
        alreadyWrittenAckFrame: false,
        nonZeroRecvEcn: false))

proc ackSpaceEpoch(epoch: CryptoEpoch): CryptoEpoch {.gcsafe.} =
  case epoch
  of ceZeroRtt, ceOneRtt:
    ceOneRtt
  else:
    epoch

proc stateForEpoch(tracker: var AckTrackerModel; epoch: CryptoEpoch): var AckEpochState {.gcsafe.} =
  tracker.epochs[ackSpaceEpoch(epoch)]

proc insertPacketRange(ranges: var seq[PacketRange]; packetNumber: uint64): bool {.gcsafe.} =
  ## Insert one packet number into a sorted non-overlapping range list.
  if ranges.len == 0:
    ranges.add(PacketRange(first: packetNumber, last: packetNumber))
    return true

  var idx = 0
  while idx < ranges.len and ranges[idx].last < packetNumber:
    if ranges[idx].last + 1'u64 < packetNumber:
      inc idx
    else:
      break

  if idx >= ranges.len:
    ranges.add(PacketRange(first: packetNumber, last: packetNumber))
    return true

  if packetNumber >= ranges[idx].first and packetNumber <= ranges[idx].last:
    return false

  if packetNumber + 1'u64 < ranges[idx].first:
    ranges.insert(PacketRange(first: packetNumber, last: packetNumber), idx)
    return true

  if ranges[idx].first > 0'u64 and packetNumber + 1'u64 == ranges[idx].first:
    ranges[idx].first = packetNumber
  elif packetNumber == ranges[idx].last + 1'u64:
    ranges[idx].last = packetNumber
  else:
    ranges.insert(PacketRange(first: packetNumber, last: packetNumber), idx)
    return true

  while idx + 1 < ranges.len and ranges[idx].last + 1'u64 >= ranges[idx + 1].first:
    ranges[idx].last = max(ranges[idx].last, ranges[idx + 1].last)
    ranges.delete(idx + 1)
  if idx > 0 and ranges[idx - 1].last + 1'u64 >= ranges[idx].first:
    ranges[idx - 1].last = max(ranges[idx - 1].last, ranges[idx].last)
    ranges.delete(idx)
  true

proc trackIncomingPacket*(tracker: var AckTrackerModel; epoch: CryptoEpoch;
    packetNumber: uint64) {.gcsafe.} =
  ## 追加一个已经接收的包号区间，用于重复包检测。
  let ackEpoch = ackSpaceEpoch(epoch)
  discard insertPacketRange(tracker.epochs[ackEpoch].packetNumbersReceived, packetNumber)

proc wasPacketReceived*(tracker: var AckTrackerModel; epoch: CryptoEpoch;
    packetNumber: uint64): bool {.gcsafe.} =
  for packetRange in tracker.stateForEpoch(epoch).packetNumbersReceived:
    if packetNumber < packetRange.first:
      return false
    if packetNumber <= packetRange.last:
      return true
  false

proc markForAck*(tracker: var AckTrackerModel; epoch: CryptoEpoch;
    packetNumber: uint64; recvTimeUs: uint64;
    ackType: AckType; ackDelayMs: uint32 = MaxAckDelayDefaultMs) {.gcsafe.} =
  ## 抽象 ACK 聚合逻辑，便于 Nim 端推演 ACK 触发条件。
  let ackEpoch = ackSpaceEpoch(epoch)
  let inserted = insertPacketRange(tracker.epochs[ackEpoch].packetNumbersToAck, packetNumber)
  if not inserted:
    return
  tracker.epochs[ackEpoch].counters.ackElicitingPacketsToAck.inc
  if tracker.epochs[ackEpoch].packetNumbersToAck.len == 1 and
      tracker.epochs[ackEpoch].packetNumbersToAck[0].first == packetNumber and
      tracker.epochs[ackEpoch].packetNumbersToAck[0].last == packetNumber and
      tracker.epochs[ackEpoch].largestPacketNumberAcked == 0'u64:
    tracker.epochs[ackEpoch].largestPacketNumberAcked = packetNumber
    tracker.epochs[ackEpoch].largestPacketNumberRecvTime = recvTimeUs
  elif packetNumber > tracker.epochs[ackEpoch].largestPacketNumberAcked:
    tracker.epochs[ackEpoch].largestPacketNumberAcked = packetNumber
    tracker.epochs[ackEpoch].largestPacketNumberRecvTime = recvTimeUs
  if ackType == ackTypeAckEliciting and tracker.epochs[ackEpoch].ackDeadlineUs == 0'u64:
    tracker.epochs[ackEpoch].ackDeadlineUs =
      recvTimeUs + uint64(ackDelayMs) * 1_000'u64
  elif ackType == ackTypeAckImmediate:
    tracker.epochs[ackEpoch].ackDeadlineUs = recvTimeUs
  tracker.epochs[ackEpoch].counters.alreadyWrittenAckFrame = false

proc consumePendingAck*(tracker: var AckTrackerModel; epoch: CryptoEpoch) {.gcsafe.} =
  ## 发送 ACK 后消费当前待确认区间，避免重复刷同一份 ACK。
  let ackEpoch = ackSpaceEpoch(epoch)
  tracker.epochs[ackEpoch].packetNumbersToAck.setLen(0)
  tracker.epochs[ackEpoch].ackDeadlineUs = 0'u64
  tracker.epochs[ackEpoch].counters.ackElicitingPacketsToAck = 0'u16
  tracker.epochs[ackEpoch].counters.alreadyWrittenAckFrame = true

proc shouldSendAck*(tracker: var AckTrackerModel; epoch: CryptoEpoch;
    packetTolerance: uint8; ackType: AckType; reordered: bool;
    nowUs: uint64 = 0'u64): bool {.gcsafe.} =
  ## 对应 `QuicAckTrackerAckPacket` 中的触发阈值。
  if ackType == ackTypeAckImmediate:
    return true
  if tracker.stateForEpoch(epoch).counters.ackElicitingPacketsToAck >= uint16(packetTolerance):
    return true
  if reordered:
    return true
  if tracker.stateForEpoch(epoch).ackDeadlineUs > 0'u64 and
      nowUs >= tracker.stateForEpoch(epoch).ackDeadlineUs:
    return true
  false

proc packetNumbersToAck*(tracker: var AckTrackerModel; epoch: CryptoEpoch): seq[PacketRange] {.gcsafe.} =
  tracker.stateForEpoch(epoch).packetNumbersToAck

proc largestPacketNumberAcked*(tracker: var AckTrackerModel; epoch: CryptoEpoch): uint64 {.gcsafe.} =
  tracker.stateForEpoch(epoch).largestPacketNumberAcked

proc largestPacketNumberRecvTime*(tracker: var AckTrackerModel; epoch: CryptoEpoch): uint64 {.gcsafe.} =
  tracker.stateForEpoch(epoch).largestPacketNumberRecvTime

proc ackElicitingPacketsToAck*(tracker: var AckTrackerModel; epoch: CryptoEpoch): uint16 {.gcsafe.} =
  tracker.stateForEpoch(epoch).counters.ackElicitingPacketsToAck

proc alreadyWrittenAckFrame*(tracker: var AckTrackerModel; epoch: CryptoEpoch): bool {.gcsafe.} =
  tracker.stateForEpoch(epoch).counters.alreadyWrittenAckFrame

proc setAlreadyWrittenAckFrame*(tracker: var AckTrackerModel; epoch: CryptoEpoch;
    written: bool) {.gcsafe.} =
  tracker.epochs[ackSpaceEpoch(epoch)].counters.alreadyWrittenAckFrame = written

proc ackDeadlineUs*(tracker: var AckTrackerModel; epoch: CryptoEpoch): uint64 {.gcsafe.} =
  tracker.stateForEpoch(epoch).ackDeadlineUs

proc setLargestPacketNumberRecvTime*(tracker: var AckTrackerModel; epoch: CryptoEpoch;
    recvTimeUs: uint64) {.gcsafe.} =
  tracker.epochs[ackSpaceEpoch(epoch)].largestPacketNumberRecvTime = recvTimeUs
