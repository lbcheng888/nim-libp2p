## CUBIC 拥塞控制状态蓝图，对齐 `src/core/cubic.c` / `cubic.h`。

import std/math

import ./common

type
  CubicModel* = object
    hasHadCongestionEvent*: bool
    isInRecovery*: bool
    isInPersistentCongestion*: bool
    timeOfLastAckValid*: bool

    initialWindowPackets*: uint32
    sendIdleTimeoutMs*: uint32

    congestionWindow*: uint32
    prevCongestionWindow*: uint32
    slowStartThreshold*: uint32
    prevSlowStartThreshold*: uint32
    aimdWindow*: uint32
    prevAimdWindow*: uint32
    aimdAccumulator*: uint32

    bytesInFlight*: uint32
    bytesInFlightMax*: uint32
    lastSendAllowance*: uint32
    exemptions*: uint8

    timeOfLastAck*: uint64
    timeOfCongAvoidStart*: uint64
    kCubic*: uint32
    prevKCubic*: uint32
    windowPrior*: uint32
    prevWindowPrior*: uint32
    windowMax*: uint32
    prevWindowMax*: uint32
    windowLastMax*: uint32
    prevWindowLastMax*: uint32

    hyStartState*: HyStartState
    hyStartAckCount*: uint32
    minRttInLastRound*: uint64
    minRttInCurrentRound*: uint64
    cssBaselineMinRtt*: uint64
    hyStartRoundEnd*: uint64
    cWndSlowStartGrowthDivisor*: uint32
    conservativeSlowStartRounds*: uint32

    recoverySentPacketNumber*: uint64
    datagramPayloadBytes*: uint32
    beta*: float64
    cParam*: float64
    kCubicSeconds*: float64

proc initCubicModel*(
    initialWindowPackets: uint32 = QuicInitialWindowPackets,
    datagramPayloadBytes: uint32,
    sendIdleTimeoutMs: uint32 = 0'u32
  ): CubicModel =
  ## 初始化 Nim 层 CUBIC 状态骨架。
  let initialWindowBytes = datagramPayloadBytes * initialWindowPackets
  result = CubicModel(
    hasHadCongestionEvent: false,
    isInRecovery: false,
    isInPersistentCongestion: false,
    timeOfLastAckValid: false,
    initialWindowPackets: initialWindowPackets,
    sendIdleTimeoutMs: sendIdleTimeoutMs,
    congestionWindow: initialWindowBytes,
    prevCongestionWindow: initialWindowBytes,
    slowStartThreshold: high(uint32),
    prevSlowStartThreshold: high(uint32),
    aimdWindow: initialWindowBytes,
    prevAimdWindow: initialWindowBytes,
    aimdAccumulator: 0,
    bytesInFlight: 0,
    bytesInFlightMax: initialWindowBytes div 2,
    lastSendAllowance: 0,
    exemptions: 0,
    timeOfLastAck: 0,
    timeOfCongAvoidStart: 0,
    kCubic: 0,
    prevKCubic: 0,
    windowPrior: initialWindowBytes,
    prevWindowPrior: initialWindowBytes,
    windowMax: initialWindowBytes,
    prevWindowMax: initialWindowBytes,
    windowLastMax: initialWindowBytes,
    prevWindowLastMax: initialWindowBytes,
    hyStartState: hyStartNotStarted,
    hyStartAckCount: 0,
    minRttInLastRound: 0,
    minRttInCurrentRound: high(uint64),
    cssBaselineMinRtt: 0,
    hyStartRoundEnd: 0,
    cWndSlowStartGrowthDivisor: 1,
    conservativeSlowStartRounds: 0,
    recoverySentPacketNumber: 0,
    datagramPayloadBytes: datagramPayloadBytes,
    beta: 0.7,
    cParam: 0.4,
    kCubicSeconds: 0.0)

proc enterRecovery*(model: var CubicModel, lostBytes: uint32) =
  ## 为 Nim 蓝图提供一个恢复入口，便于实验性状态机模拟。
  model.isInRecovery = true
  model.hasHadCongestionEvent = true
  if model.congestionWindow > lostBytes:
    model.congestionWindow = model.congestionWindow - lostBytes
  else:
    model.congestionWindow = MinCwndInMss

proc computeMinCwnd(model: CubicModel): uint32 =
  let twoPackets = uint64(model.datagramPayloadBytes) * 2'u64
  let minPackets = uint64(model.datagramPayloadBytes) * uint64(MinCwndInMss)
  uint32(max(twoPackets, minPackets))

proc saturatingAddUint32(a, b: uint32): uint32 =
  let sum = uint64(a) + uint64(b)
  if sum > high(uint32).uint64:
    high(uint32)
  else:
    uint32(sum)

proc clampFloatToUint32(value: float64): uint32 =
  if value <= 0.0:
    0'u32
  elif value >= float(high(uint32)):
    high(uint32)
  else:
    uint32(value)

proc updateKCubic(model: var CubicModel) =
  if model.cParam <= 0.0 or model.windowMax == 0:
    model.kCubicSeconds = 0.0
    return
  let numerator = float64(model.windowMax) * (1.0 - model.beta)
  if numerator <= 0.0:
    model.kCubicSeconds = 0.0
  else:
    model.kCubicSeconds = cbrt(numerator / model.cParam)

proc canSend*(model: CubicModel): bool =
  model.bytesInFlight < model.congestionWindow or model.exemptions > 0

proc sendAllowance*(model: CubicModel): uint64 =
  if model.congestionWindow > model.bytesInFlight:
    uint64(model.congestionWindow - model.bytesInFlight)
  else:
    0

proc onPacketSent*(
    model: var CubicModel,
    bytes: uint32,
    ackEliciting: bool = true
  ) =
  ## 记录发送事件，更新飞行字节数。
  if ackEliciting:
    if model.bytesInFlight <= high(uint32) - bytes:
      model.bytesInFlight += bytes
    else:
      model.bytesInFlight = high(uint32)
  model.bytesInFlightMax = max(model.bytesInFlightMax, model.bytesInFlight)

proc onDataAcked*(model: var CubicModel, ack: AckEventSnapshot) =
  ## 依据 CUBIC 函数更新拥塞窗口。
  let acked = ack.ackedRetransmittableBytes
  if acked == 0:
    return

  if model.bytesInFlight > acked:
    model.bytesInFlight -= acked
  else:
    model.bytesInFlight = 0

  model.timeOfLastAck = ack.timeNow
  model.timeOfLastAckValid = true

  if model.isInRecovery and ack.largestAck > model.recoverySentPacketNumber:
    model.isInRecovery = false

  let minCwnd = model.computeMinCwnd()

  if model.congestionWindow < model.slowStartThreshold:
    model.congestionWindow = saturatingAddUint32(model.congestionWindow, acked)
    if model.congestionWindow >= model.slowStartThreshold:
      model.timeOfCongAvoidStart = ack.timeNow
      model.windowPrior = model.congestionWindow
      model.windowMax = max(model.windowMax, model.congestionWindow)
      model.updateKCubic()
  else:
    if model.timeOfCongAvoidStart == 0:
      model.timeOfCongAvoidStart = ack.timeNow
    let elapsedUs =
      if ack.timeNow > model.timeOfCongAvoidStart:
        ack.timeNow - model.timeOfCongAvoidStart
      else:
        0'u64
    let t = float64(elapsedUs) / float64(MicroSecsInSec)
    let diff = t - model.kCubicSeconds
    let cubicTarget = model.windowMax.float64 + model.cParam * diff * diff * diff
    let renoInc =
      if model.congestionWindow == 0'u32:
        float64(model.datagramPayloadBytes)
      else:
        float64(model.datagramPayloadBytes) * float64(acked) /
        float64(model.congestionWindow)
    let renoTarget = model.congestionWindow.float64 + renoInc
    let desired = max(cubicTarget, renoTarget)
    let bounded = clamp(desired, float64(minCwnd), float64(high(uint32)))
    model.congestionWindow = clampFloatToUint32(bounded)

  if model.congestionWindow < minCwnd:
    model.congestionWindow = minCwnd

  model.bytesInFlight = min(model.bytesInFlight, model.congestionWindow)
  model.bytesInFlightMax = max(model.bytesInFlightMax, model.bytesInFlight)

proc onDataLost*(model: var CubicModel, loss: LossEventSnapshot) =
  ## 丢包回退时执行 CUBIC 的乘性减小。
  let lostBytes = loss.retransmittableBytesLost
  if model.bytesInFlight > lostBytes:
    model.bytesInFlight -= lostBytes
  else:
    model.bytesInFlight = 0

  model.isInRecovery = true
  model.hasHadCongestionEvent = true
  model.recoverySentPacketNumber = loss.largestSentPacketNumber

  model.windowLastMax = model.windowMax
  model.windowPrior = model.congestionWindow
  model.windowMax = max(model.congestionWindow, model.computeMinCwnd())

  let minCwnd = model.computeMinCwnd()
  model.slowStartThreshold = max(model.congestionWindow shr 1, minCwnd)
  model.congestionWindow = max(model.slowStartThreshold, minCwnd)

  model.updateKCubic()
  model.bytesInFlight = min(model.bytesInFlight, model.congestionWindow)

proc onProbeTimeout*(model: var CubicModel) =
  ## PTO 回退至最小窗口。
  let minCwnd = model.computeMinCwnd()
  model.slowStartThreshold = max(model.congestionWindow shr 1, minCwnd)
  model.congestionWindow = minCwnd
  model.bytesInFlight = min(model.bytesInFlight, model.congestionWindow)
  model.isInRecovery = false
  model.updateKCubic()
