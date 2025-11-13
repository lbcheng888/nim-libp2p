## MsQuic 拥塞控制与恢复通用常量与类型蓝图。
## 参考 `src/core/bbr.c`, `src/core/cubic.c`, `src/core/congestion_control.h`,
## `src/core/loss_detection.h`, `src/core/ack_tracker.h`, `src/core/quicdef.h`。

import ../core/common

type
  SlidingWindowMode* = enum
    ## 单调队列模式：最大值或最小值。
    swMax,
    swMin

const
  BwUnit* = 8'u64                         ## `BW_UNIT`
  GainUnit* = 256'u32                     ## `GAIN_UNIT`
  GainCycleLength* = 8                    ## `GAIN_CYCLE_LENGTH`
  QuantaFactor* = 3'u64                   ## `kQuantaFactor`
  MinCwndInMss* = 4'u32                  ## `kMinCwndInMss`
  DefaultRecoveryCwndInMss* = 2000'u32   ## `kDefaultRecoveryCwndInMss`
  MicroSecsInSec* = 1_000_000'u64        ## `kMicroSecsInSec`
  MilliSecsInSec* = 1_000'u64            ## `kMilliSecsInSec`
  LowPacingRateThresholdBps* = 1_200_000'u64 ## `kLowPacingRateThresholdBytesPerSecond`
  HighPacingRateThresholdBps* = 24_000_000'u64 ## `kHighPacingRateThresholdBytesPerSecond`
  BbrHighGain* = uint32(256 * 2885 div 1000 + 1) ## `kHighGain`
  BbrDrainGain* = uint32(256 * 1000 div 2885)    ## `kDrainGain`
  BbrCwndGain* = 512'u32                    ## `kCwndGain`
  BbrStartupGrowthTarget* = uint32(256 * 5 div 4) ## `kStartupGrowthTarget`
  BbrStartupSlowGrowRoundLimit* = 3'u8      ## `kStartupSlowGrowRoundLimit`
  ProbeRttTimeUs* = 200_000'u32            ## `kProbeRttTimeInUs`
  BbrMinRttExpirationUs* = 10'u64 * MicroSecsInSec ## `kBbrMinRttExpirationInMicroSecs`
  BbrMaxBandwidthFilterLen* = 10'u8
  BbrMaxAckHeightFilterLen* = 10'u8
  BbrDefaultFilterCapacity* = 3'u8      ## `kBbrDefaultFilterCapacity`
  QuicInitialWindowPackets* = 10'u32       ## `QUIC_INITIAL_WINDOW_PACKETS`
  MinAckSendNumber* = 2'u8                 ## `QUIC_MIN_ACK_SEND_NUMBER`
  PacketReorderThreshold* = 3'u32          ## `QUIC_PACKET_REORDER_THRESHOLD`
  MinReorderingThreshold* = 1'u8           ## `QUIC_MIN_REORDERING_THRESHOLD`
  MaxRangeDuplicatePackets* = 0x1000'u32   ## `QUIC_MAX_RANGE_DUPLICATE_PACKETS`
  MaxRangeAckPackets* = 0x800'u32          ## `QUIC_MAX_RANGE_ACK_PACKETS`
  MaxRangeDecodeAcks* = 0x1000'u32         ## `QUIC_MAX_RANGE_DECODE_ACKS`
  MaxAckDelayDefaultMs* = 25'u32           ## `QUIC_TP_MAX_ACK_DELAY_DEFAULT`
  PersistentCongestionThreshold* = 2'u8    ## `QUIC_PERSISTENT_CONGESTION_THRESHOLD`
  ClosePtoCount* = 3'u8                    ## `QUIC_CLOSE_PTO_COUNT`
  PersistentCongestionWindowPackets* = 2'u32 ## `QUIC_PERSISTENT_CONGESTION_WINDOW_PACKETS`

  BbrPacingGainCycle*: array[GainCycleLength, uint32] = [
    uint32(256 * 5 div 4),
    uint32(256 * 3 div 4),
    GainUnit, GainUnit, GainUnit,
    GainUnit, GainUnit, GainUnit
  ]

type
  BbrState* = enum                      ## `BBR_STATE`
    bbrStartup,
    bbrDrain,
    bbrProbeBandwidth,
    bbrProbeRtt

  BbrRecoveryState* = enum              ## `RECOVERY_STATE`
    recoveryNone,
    recoveryConservative,
    recoveryGrowth

  HyStartState* = enum                  ## `QUIC_CUBIC_HYSTART_STATE`
    hyStartNotStarted,
    hyStartActive,
    hyStartDone

  CongestionAlgorithm* = enum
    caCubic,
    caBbr

  SlidingWindowExtremumEntry* = object
    value*: uint64
    time*: uint64                       ## Rtt counter或时间戳（微秒）

  SlidingWindowExtremumModel* = object
    capacity*: uint8
    lifetime*: uint64
    mode*: SlidingWindowMode
    entries*: seq[SlidingWindowExtremumEntry]

  AckDelayInfo* = object
    ackDelayUs*: uint64
    maxAckDelayMs*: uint32
    ackDelayExponent*: uint8

  AckEcnSummary* = object               ## 对应 `QUIC_ACK_ECN_EX`
    ect0Count*: QuicVarInt
    ect1Count*: QuicVarInt
    ceCount*: QuicVarInt

  AckEventSnapshot* = object            ## 对应 `QUIC_ACK_EVENT`
    timeNow*: uint64
    largestAck*: uint64
    largestSentPacketNumber*: uint64
    totalAckedRetransmittableBytes*: uint64
    ackedRetransmittableBytes*: uint32
    smoothedRtt*: uint64
    minRtt*: uint64
    oneWayDelay*: uint64
    adjustedAckTime*: uint64
    implicitAck*: bool
    hasLoss*: bool
    largestAckAppLimited*: bool
    minRttValid*: bool

  LossEventSnapshot* = object           ## 对应 `QUIC_LOSS_EVENT`
    largestPacketNumberLost*: uint64
    largestSentPacketNumber*: uint64
    retransmittableBytesLost*: uint32
    persistentCongestion*: bool

  EcnEventSnapshot* = object            ## 对应 `QUIC_ECN_EVENT`
    largestPacketNumberAcked*: uint64
    largestSentPacketNumber*: uint64

  AckType* = enum                       ## 对应 `QUIC_ACK_TYPE`
    ackTypeNonAckEliciting,
    ackTypeAckEliciting,
    ackTypeAckImmediate

  AckTrackerCounters* = object
    ackElicitingPacketsToAck*: uint16
    alreadyWrittenAckFrame*: bool
    nonZeroRecvEcn*: bool

proc timeReorderThreshold*(rttUs: uint64): uint64 =
  ## `QUIC_TIME_REORDER_THRESHOLD(rtt)` 的 Nim 版本。
  rttUs + (rttUs shr 3)

proc initSlidingWindowExtremum*(
    lifetime: uint64,
    capacity: uint8,
    mode: SlidingWindowMode = swMax
  ): SlidingWindowExtremumModel =
  ## 初始化滑动窗口极值过滤器，实现最值单调队列。
  SlidingWindowExtremumModel(
    capacity: capacity,
    lifetime: lifetime,
    mode: mode,
    entries: @[])

proc reset*(model: var SlidingWindowExtremumModel) =
  ## 清空窗口。
  model.entries.setLen(0)

proc isEmpty*(model: SlidingWindowExtremumModel): bool =
  ## 判断窗口是否为空。
  model.entries.len == 0

proc expireEntries(model: var SlidingWindowExtremumModel, now: uint64) =
  ## 基于时间戳淘汰过期元素。
  while model.entries.len > 0:
    let head = model.entries[0]
    if now >= head.time and now - head.time > model.lifetime:
      model.entries.delete(0)
    else:
      break

proc update*(
    model: var SlidingWindowExtremumModel,
    value: uint64,
    timestamp: uint64
  ) =
  ## 写入一个新的观测值，维护单调性与容量约束。
  if model.capacity == 0:
    return
  if model.entries.len > 0 and timestamp < model.entries[^1].time:
    ## 忽略过期的时间顺序输入。
    return

  model.expireEntries(timestamp)

  while model.entries.len > 0:
    let tail = model.entries[^1]
    if timestamp >= tail.time and timestamp - tail.time > model.lifetime:
      model.entries.setLen(model.entries.len - 1)
      continue

    let dominated =
      if model.mode == swMax:
        value >= tail.value
      else:
        value <= tail.value
    if dominated:
      model.entries.setLen(model.entries.len - 1)
    else:
      break

  if model.entries.len >= model.capacity.int:
    model.entries.delete(0)

  model.entries.add(SlidingWindowExtremumEntry(value: value, time: timestamp))

proc peek*(
    model: SlidingWindowExtremumModel,
    entry: var SlidingWindowExtremumEntry
  ): bool =
  ## 读取当前极值，若窗口为空返回 false。
  if model.entries.len == 0:
    return false
  entry = model.entries[0]
  true

proc currentValue*(
    model: SlidingWindowExtremumModel,
    defaultValue: uint64 = 0'u64
  ): uint64 =
  ## 返回当前极值数值，若为空返回默认值。
  if model.entries.len == 0:
    defaultValue
  else:
    model.entries[0].value
