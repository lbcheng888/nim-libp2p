## NewReno 拥塞控制最小实现，支撑 B2 阶段的 Nim 骨架。
## 参考 RFC 9002 以及 MsQuic `congestion_control.c` 对慢启动与 AIMD 的处理。

import ./common

type
  NewRenoPhase* = enum
    nrSlowStart
    nrCongestionAvoidance
    nrRecovery

  NewRenoModel* = object
    congestionWindow*: uint64          ## 当前拥塞窗口（字节）
    ssthresh*: uint64                  ## 慢启动阈值
    bytesInFlight*: uint64             ## 已发送且未确认数据
    minCongestionWindow*: uint64       ## 下限窗口（通常为 2-4 个 MSS）
    maxDatagramSize*: uint32
    recoveryStartPacket*: uint64
    phase*: NewRenoPhase

proc initNewReno*(
    maxDatagramSize: uint32,
    initialWindowPackets: uint32 = QuicInitialWindowPackets,
    minWindowPackets: uint32 = 2'u32
  ): NewRenoModel =
  ## 初始化 NewReno 状态。
  let initialWindow = uint64(maxDatagramSize) * uint64(initialWindowPackets)
  let minPackets = max(max(minWindowPackets, 2'u32), MinCwndInMss)
  let minWindow = uint64(maxDatagramSize) * uint64(minPackets)
  NewRenoModel(
    congestionWindow: initialWindow,
    ssthresh: high(uint64),
    bytesInFlight: 0,
    minCongestionWindow: minWindow,
    maxDatagramSize: maxDatagramSize,
    recoveryStartPacket: 0,
    phase: nrSlowStart)

proc canSend*(model: NewRenoModel): bool =
  ## 是否允许继续发送 ACK 诱发数据。
  model.bytesInFlight < model.congestionWindow

proc sendAllowance*(model: NewRenoModel): uint64 =
  ## 返回当前可用窗口字节数。
  if model.congestionWindow > model.bytesInFlight:
    model.congestionWindow - model.bytesInFlight
  else:
    0

proc onPacketSent*(model: var NewRenoModel, bytes: uint32) =
  ## 记录新的传输。
  model.bytesInFlight += uint64(bytes)

proc updatePhase(model: var NewRenoModel) =
  ## 根据窗口与阈值刷新阶段状态。
  if model.phase == nrRecovery:
    return
  if model.congestionWindow < model.ssthresh:
    model.phase = nrSlowStart
  else:
    model.phase = nrCongestionAvoidance

proc exitRecovery(model: var NewRenoModel) =
  if model.phase == nrRecovery:
    if model.congestionWindow < model.ssthresh:
      model.phase = nrSlowStart
    else:
      model.phase = nrCongestionAvoidance

proc onDataAcked*(model: var NewRenoModel, ack: AckEventSnapshot) =
  ## 最小化的 ACK 回调，依据 RFC 9002 11.2 进行 cwnd 更新。
  let acked = uint64(ack.ackedRetransmittableBytes)
  if acked == 0:
    return

  if model.bytesInFlight > acked:
    model.bytesInFlight -= acked
  else:
    model.bytesInFlight = 0

  if model.phase == nrRecovery and ack.largestAck <= model.recoveryStartPacket:
    ## 仍处于恢复阶段，忽略 cwnd 增长。
    return

  if model.phase == nrRecovery and ack.largestAck > model.recoveryStartPacket:
    model.exitRecovery()

  case model.phase
  of nrSlowStart:
    model.congestionWindow += acked
  of nrCongestionAvoidance:
    let increment = (uint64(model.maxDatagramSize) * acked) div model.congestionWindow
    if increment == 0:
      model.congestionWindow += uint64(model.maxDatagramSize)
    else:
      model.congestionWindow += increment
  of nrRecovery:
    discard
  model.updatePhase()

proc onDataLost*(model: var NewRenoModel, loss: LossEventSnapshot) =
  ## 丢包时降低窗口并进入恢复阶段。
  let lostBytes = uint64(loss.retransmittableBytesLost)
  if model.bytesInFlight > lostBytes:
    model.bytesInFlight -= lostBytes
  else:
    model.bytesInFlight = 0

  model.ssthresh = max(model.congestionWindow shr 1, model.minCongestionWindow)
  model.congestionWindow = max(model.ssthresh, model.minCongestionWindow)
  model.phase = nrRecovery
  model.recoveryStartPacket = loss.largestSentPacketNumber

proc onProbeTimeout*(model: var NewRenoModel) =
  ## PTO 触发时退回最小窗口，重新慢启动。
  model.ssthresh = max(model.congestionWindow shr 1, model.minCongestionWindow)
  model.congestionWindow = model.minCongestionWindow
  model.bytesInFlight = min(model.bytesInFlight, model.congestionWindow)
  model.phase = nrSlowStart
  model.recoveryStartPacket = 0
