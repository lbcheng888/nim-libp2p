## BBR 拥塞控制状态蓝图，对齐 `src/core/bbr.c` / `bbr.h`。

import ./common

type
  BbrBandwidthFilter* = object
    ## 对应 `BBR_BANDWIDTH_FILTER`，记录带宽采样的滑动窗口。
    appLimited*: bool
    appLimitedExitTarget*: uint64
    lastSampleBandwidth*: uint64
    window*: SlidingWindowExtremumModel

  BbrModel* = object
    ## 对应 `QUIC_CONGESTION_CONTROL_BBR`。
    btlbwFound*: bool
    exitingQuiescence*: bool
    endOfRecoveryValid*: bool
    endOfRoundTripValid*: bool
    ackAggregationStartTimeValid*: bool
    probeRttRoundValid*: bool
    probeRttEndTimeValid*: bool
    rttSampleExpired*: bool
    minRttTimestampValid*: bool

    initialCongestionWindowPackets*: uint32
    congestionWindow*: uint32
    initialCongestionWindow*: uint32
    recoveryWindow*: uint32

    bytesInFlight*: uint32
    bytesInFlightMax*: uint32
    exemptions*: uint8

    roundTripCounter*: uint64
    cwndGain*: uint32
    pacingGain*: uint32
    sendQuantum*: uint64
    slowStartupRoundCounter*: uint8
    pacingCycleIndex*: uint32

    ackAggregationStartTime*: uint64
    aggregatedAckBytes*: uint64

    recoveryState*: BbrRecoveryState
    state*: BbrState
    cycleStart*: uint64
    endOfRoundTrip*: uint64
    endOfRecovery*: uint64
    lastStartupBandwidth*: uint64
    probeRttRound*: uint64
    probeRttEndTime*: uint64

    maxAckHeightFilter*: SlidingWindowExtremumModel
    minRtt*: uint64
    minRttTimestamp*: uint64
    bandwidthFilter*: BbrBandwidthFilter
    bandwidthEstimate*: uint64
    pacingRateBps*: uint64
    targetCongestionWindow*: uint32
    minCongestionWindow*: uint32
    datagramPayloadBytes*: uint32

proc initBbrBandwidthFilter*(): BbrBandwidthFilter =
  BbrBandwidthFilter(
    appLimited: false,
    appLimitedExitTarget: 0,
    lastSampleBandwidth: 0,
    window: initSlidingWindowExtremum(
      uint64(BbrMaxBandwidthFilterLen),
      BbrDefaultFilterCapacity,
      swMax))

proc initBbrModel*(
    initialWindowPackets: uint32 = QuicInitialWindowPackets,
    datagramPayloadBytes: uint32,
    algorithmGain: tuple[cwndGain, pacingGain: uint32] = (BbrCwndGain, BbrHighGain)
  ): BbrModel =
  ## 初始化 Nim 层 BBR 状态，便于后续阶段复用。
  let initialWindowBytes = datagramPayloadBytes * initialWindowPackets
  result = BbrModel(
    btlbwFound: false,
    exitingQuiescence: true,
    endOfRecoveryValid: false,
    endOfRoundTripValid: false,
    ackAggregationStartTimeValid: false,
    probeRttRoundValid: false,
    probeRttEndTimeValid: false,
    rttSampleExpired: true,
    minRttTimestampValid: false,
    initialCongestionWindowPackets: initialWindowPackets,
    congestionWindow: initialWindowBytes,
    initialCongestionWindow: initialWindowBytes,
    recoveryWindow: initialWindowBytes,
    bytesInFlight: 0,
    bytesInFlightMax: initialWindowBytes div 2,
    exemptions: 0,
    roundTripCounter: 0,
    cwndGain: algorithmGain.cwndGain,
    pacingGain: algorithmGain.pacingGain,
    sendQuantum: uint64(datagramPayloadBytes),
    slowStartupRoundCounter: 0,
    pacingCycleIndex: 0,
    ackAggregationStartTime: 0,
    aggregatedAckBytes: 0,
    recoveryState: recoveryNone,
    state: bbrStartup,
    cycleStart: 0,
    endOfRoundTrip: 0,
    endOfRecovery: 0,
    lastStartupBandwidth: 0,
    probeRttRound: 0,
    probeRttEndTime: 0,
    maxAckHeightFilter: initSlidingWindowExtremum(
      uint64(BbrMaxAckHeightFilterLen),
      BbrDefaultFilterCapacity,
      swMax),
    minRtt: high(uint64),
    minRttTimestamp: 0,
    bandwidthFilter: initBbrBandwidthFilter(),
    bandwidthEstimate: 0,
    pacingRateBps: 0,
    targetCongestionWindow: initialWindowBytes,
    minCongestionWindow: uint32(
      max(uint64(datagramPayloadBytes) * uint64(MinCwndInMss),
          uint64(datagramPayloadBytes) * 2'u64)),
    datagramPayloadBytes: datagramPayloadBytes)

proc resetAckAggregation*(model: var BbrModel) =
  ## 清空 ACK 聚合状态，通常在进入恢复或探测阶段时调用。
  model.ackAggregationStartTimeValid = false
  model.ackAggregationStartTime = 0
  model.aggregatedAckBytes = 0
  model.maxAckHeightFilter.reset()

proc currentBandwidth(filter: BbrBandwidthFilter): uint64 =
  filter.window.currentValue()

proc currentBandwidth(model: BbrModel): uint64 =
  model.bandwidthFilter.currentBandwidth()

proc bytesPerSecondFromBandwidth(rate: uint64): uint64 =
  ## MsQuic 带宽单位是 (bytes/BwUnit)/s，这里换算成字节/s。
  if rate == 0:
    return 0
  rate div BwUnit

proc updateSendQuantum(model: var BbrModel) =
  ## 根据当前 pacing 速率估算每次允许批量发送的字节数。
  let payload = uint64(model.datagramPayloadBytes)
  var quantum = payload

  let pacingGain = uint64(model.pacingGain)
  let pacingRate =
    (model.bandwidthEstimate * pacingGain) div uint64(GainUnit)
  let pacingBytesPerSec = bytesPerSecondFromBandwidth(pacingRate)

  if pacingBytesPerSec == 0:
    quantum = payload
  elif pacingBytesPerSec < LowPacingRateThresholdBps:
    quantum = payload
  elif pacingBytesPerSec < HighPacingRateThresholdBps:
    quantum = payload * 2
  else:
    let burst = pacingBytesPerSec div MilliSecsInSec
    quantum = min(burst, 64'u64 * 1024'u64)
  if quantum < payload:
    quantum = payload
  model.sendQuantum = quantum

proc updateAckAggregation*(model: var BbrModel, ack: AckEventSnapshot): uint64 =
  ## 参考 `BbrCongestionControlUpdateAckAggregation` 估算 ACK 高度补偿。
  if ack.ackedRetransmittableBytes == 0:
    return 0

  if not model.ackAggregationStartTimeValid:
    model.ackAggregationStartTime = ack.timeNow
    model.ackAggregationStartTimeValid = true
    model.aggregatedAckBytes = ack.ackedRetransmittableBytes
    return 0

  if ack.timeNow < model.ackAggregationStartTime:
    model.ackAggregationStartTime = ack.timeNow
    model.aggregatedAckBytes = ack.ackedRetransmittableBytes
    return 0

  let elapsedUs = ack.timeNow - model.ackAggregationStartTime
  let bandwidth = model.currentBandwidth()
  if bandwidth == 0:
    model.ackAggregationStartTime = ack.timeNow
    model.aggregatedAckBytes = ack.ackedRetransmittableBytes
    return 0

  let bandwidthBytesPerSec = bytesPerSecondFromBandwidth(bandwidth)
  let expected =
    (bandwidthBytesPerSec * elapsedUs) div MicroSecsInSec

  if model.aggregatedAckBytes <= expected:
    model.aggregatedAckBytes = ack.ackedRetransmittableBytes
    model.ackAggregationStartTime = ack.timeNow
    return 0

  model.aggregatedAckBytes += ack.ackedRetransmittableBytes
  if model.aggregatedAckBytes < ack.ackedRetransmittableBytes:
    model.aggregatedAckBytes = high(uint64)

  let ackHeight = model.aggregatedAckBytes - expected
  model.maxAckHeightFilter.update(ackHeight, model.roundTripCounter)
  ackHeight

proc pacingIntervalUs*(model: BbrModel): uint64 =
  ## 返回按照当前 pacing 速率发送一个量子所需的时间（微秒）。
  if model.pacingRateBps == 0 or model.sendQuantum == 0:
    return 0
  let interval =
    (model.sendQuantum * MicroSecsInSec) div model.pacingRateBps
  if interval == 0:
    1'u64
  else:
    interval

proc advancePacingCycle*(model: var BbrModel) =
  ## 模拟 `kPacingGain` 周期推进，方便 Nim 端做状态推演。
  let nextIndex = (model.pacingCycleIndex.int + 1) mod GainCycleLength
  model.pacingCycleIndex = uint32(nextIndex)
  model.pacingGain = BbrPacingGainCycle[nextIndex]

proc clampToUint32(value: uint64): uint32 =
  if value > high(uint32).uint64:
    high(uint32)
  else:
    uint32(value)

proc recordBandwidthSample(
    filter: var BbrBandwidthFilter,
    rate: uint64,
    timestamp: uint64,
    appLimitedSample: bool
  ) =
  if rate == 0:
    return
  if appLimitedSample and rate < filter.window.currentValue():
    return
  filter.lastSampleBandwidth = rate
  filter.window.update(rate, timestamp)
  if not appLimitedSample:
    filter.appLimited = false

proc recordBandwidthSample(
    model: var BbrModel,
    rate: uint64,
    timestamp: uint64,
    appLimitedSample: bool
  ) =
  model.bandwidthFilter.recordBandwidthSample(rate, timestamp, appLimitedSample)

proc updateTargetCongestionWindow(
    model: var BbrModel,
    rttUs: uint64
  ) =
  if rttUs == 0:
    return
  let bandwidth = model.currentBandwidth()
  if bandwidth == 0:
    return
  let bdpBytes =
    (bandwidth * rttUs) div (BwUnit * MicroSecsInSec)
  let scaled = (bdpBytes * uint64(model.cwndGain)) div uint64(GainUnit)
  let minCwnd = uint64(model.minCongestionWindow)
  let target = max(minCwnd, scaled)
  model.targetCongestionWindow = clampToUint32(target)

proc canSend*(model: BbrModel): bool =
  model.bytesInFlight < model.congestionWindow or model.exemptions > 0

proc sendAllowance*(model: BbrModel): uint64 =
  if model.congestionWindow > model.bytesInFlight:
    uint64(model.congestionWindow - model.bytesInFlight)
  else:
    0

proc onPacketSent*(
    model: var BbrModel,
    bytes: uint32,
    appLimited: bool = false
  ) =
  ## 记录发送到网络的数据，配合 app-limited 状态。
  if model.bytesInFlight <= high(uint32) - bytes:
    model.bytesInFlight += bytes
  else:
    model.bytesInFlight = high(uint32)
  model.bytesInFlightMax = max(model.bytesInFlightMax, model.bytesInFlight)
  if appLimited:
    model.bandwidthFilter.appLimited = true

proc onDataAcked*(model: var BbrModel, ack: AckEventSnapshot): uint64 =
  ## BBR 主状态机：采样带宽、更新窗口与 pacing。
  let acked = ack.ackedRetransmittableBytes
  if acked == 0:
    return 0

  if model.bytesInFlight > acked:
    model.bytesInFlight -= acked
  else:
    model.bytesInFlight = 0

  var sampleRtt = ack.smoothedRtt
  if sampleRtt == 0:
    sampleRtt = ack.minRtt
  let effectiveMinRtt =
    if model.minRtt != high(uint64):
      model.minRtt
    else:
      0'u64
  if sampleRtt == 0 and model.minRttTimestampValid and effectiveMinRtt != 0:
    sampleRtt = model.minRtt

  if ack.minRttValid:
    if (not model.minRttTimestampValid) or
       (ack.minRtt < model.minRtt) or
       (ack.timeNow > model.minRttTimestamp and
        ack.timeNow - model.minRttTimestamp > BbrMinRttExpirationUs):
      model.minRtt = ack.minRtt
      model.minRttTimestamp = ack.timeNow
      model.minRttTimestampValid = true
      model.rttSampleExpired = false

  if sampleRtt == 0 and model.minRttTimestampValid:
    sampleRtt = model.minRtt

  let deliveryRate =
    if sampleRtt == 0:
      0'u64
    else:
      (uint64(acked) * MicroSecsInSec * BwUnit) div sampleRtt

  model.recordBandwidthSample(deliveryRate, ack.timeNow, ack.largestAckAppLimited)
  model.bandwidthEstimate = max(model.bandwidthEstimate, model.currentBandwidth())

  let newRound =
    if not model.endOfRoundTripValid or ack.largestAck >= model.endOfRoundTrip:
      model.endOfRoundTripValid = true
      model.endOfRoundTrip = ack.largestSentPacketNumber
      inc(model.roundTripCounter)
      true
    else:
      false

  let ackHeight = model.updateAckAggregation(ack)

  if model.state == bbrStartup:
    if newRound:
      if model.lastStartupBandwidth == 0 or
         model.bandwidthEstimate >
         (model.lastStartupBandwidth * uint64(BbrStartupGrowthTarget)) div uint64(GainUnit):
        model.lastStartupBandwidth = model.bandwidthEstimate
        model.slowStartupRoundCounter = 0
      else:
        if model.slowStartupRoundCounter < high(uint8):
          inc(model.slowStartupRoundCounter)
      if model.slowStartupRoundCounter >= BbrStartupSlowGrowRoundLimit:
        model.state = bbrDrain
        model.pacingGain = BbrDrainGain
        model.cycleStart = ack.timeNow
    model.btlbwFound = model.bandwidthEstimate > 0

  if model.state == bbrDrain:
    if model.bytesInFlight <= model.targetCongestionWindow:
      model.state = bbrProbeBandwidth
      model.pacingGain = BbrPacingGainCycle[model.pacingCycleIndex.int]
      model.cycleStart = ack.timeNow

  if model.state == bbrProbeBandwidth and newRound:
    model.advancePacingCycle()
    model.cycleStart = ack.timeNow

  if model.minRttTimestampValid and
     ack.timeNow > model.minRttTimestamp and
     ack.timeNow - model.minRttTimestamp > BbrMinRttExpirationUs and
     model.state != bbrProbeRtt:
    model.state = bbrProbeRtt
    model.pacingGain = GainUnit
    model.cwndGain = GainUnit
    model.probeRttEndTime = ack.timeNow + ProbeRttTimeUs
    model.probeRttEndTimeValid = true
    model.targetCongestionWindow = max(model.minCongestionWindow, uint32(model.datagramPayloadBytes * 2))

  if model.state == bbrProbeRtt:
    if model.probeRttEndTimeValid and ack.timeNow >= model.probeRttEndTime and
       model.bytesInFlight <= model.targetCongestionWindow:
      model.state = bbrProbeBandwidth
      model.probeRttEndTimeValid = false
      model.cwndGain = BbrCwndGain
      model.pacingGain = BbrPacingGainCycle[model.pacingCycleIndex.int]
      model.minRttTimestamp = ack.timeNow
      model.minRttTimestampValid = true

  let targetRtt =
    if sampleRtt != 0: sampleRtt
    elif model.minRttTimestampValid and effectiveMinRtt != 0: effectiveMinRtt
    else: 0'u64

  if targetRtt != 0:
    model.updateTargetCongestionWindow(targetRtt)

  model.updateSendQuantum()

  let pacingBytesPerSec =
    (bytesPerSecondFromBandwidth(model.bandwidthEstimate) * uint64(model.pacingGain)) div uint64(GainUnit)
  if pacingBytesPerSec > 0:
    model.pacingRateBps = pacingBytesPerSec

  var desired = max(uint64(model.targetCongestionWindow), uint64(model.minCongestionWindow))
  let quantaPadding = QuantaFactor * model.sendQuantum
  if quantaPadding > 0 and desired <= high(uint64) - quantaPadding:
    desired += quantaPadding
  else:
    desired = max(desired, uint64(model.minCongestionWindow))

  if model.btlbwFound:
    var entry: SlidingWindowExtremumEntry
    if model.maxAckHeightFilter.peek(entry):
      if desired <= high(uint64) - entry.value:
        desired += entry.value
      else:
        desired = high(uint64)

  model.congestionWindow = clampToUint32(desired)
  model.recoveryWindow = model.congestionWindow

  if model.congestionWindow < model.minCongestionWindow:
    model.congestionWindow = model.minCongestionWindow

  if model.bytesInFlight > model.congestionWindow:
    model.bytesInFlight = model.congestionWindow
  ackHeight

proc onDataLost*(model: var BbrModel, loss: LossEventSnapshot) =
  ## 丢包触发时执行 BBR 恢复逻辑。
  let lostBytes = loss.retransmittableBytesLost
  if model.bytesInFlight > lostBytes:
    model.bytesInFlight -= lostBytes
  else:
    model.bytesInFlight = 0

  model.resetAckAggregation()

  model.recoveryState = recoveryConservative
  model.state = bbrProbeBandwidth
  model.endOfRecoveryValid = true
  model.endOfRecovery = loss.largestSentPacketNumber

  let halved = model.congestionWindow shr 1
  model.congestionWindow = max(halved, model.minCongestionWindow)
  model.targetCongestionWindow = model.congestionWindow
  model.recoveryWindow = model.congestionWindow
  model.bytesInFlight = min(model.bytesInFlight, model.congestionWindow)
