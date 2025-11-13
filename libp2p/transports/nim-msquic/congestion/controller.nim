## 拥塞控制策略切换接口，实现 BBR/CUBIC 在 Nim 侧的统一封装。

import ./common
import ./bbr_model
import ./cubic_model
import ./pacing_scheduler

type
  CongestionController* = object
    algorithm*: CongestionAlgorithm
    datagramPayloadBytes*: uint32
    initialWindowPackets*: uint32
    cubic*: CubicModel
    bbr*: BbrModel
    pacing*: PacingState

proc initCongestionController*(
    algorithm: CongestionAlgorithm,
    datagramPayloadBytes: uint32,
    initialWindowPackets: uint32 = QuicInitialWindowPackets
  ): CongestionController =
  ## 初始化统一的拥塞控制器。
  result.datagramPayloadBytes = datagramPayloadBytes
  result.initialWindowPackets = initialWindowPackets
  result.cubic = initCubicModel(initialWindowPackets, datagramPayloadBytes)
  result.bbr = initBbrModel(initialWindowPackets, datagramPayloadBytes)
  result.algorithm = algorithm
  result.pacing = initPacingState()

proc switchAlgorithm*(
    controller: var CongestionController,
    algorithm: CongestionAlgorithm
  ) =
  ## 切换拥塞控制策略，并重置对应状态。
  if controller.algorithm == algorithm:
    return
  controller.algorithm = algorithm
  case algorithm
  of caCubic:
    controller.cubic = initCubicModel(
      controller.initialWindowPackets,
      controller.datagramPayloadBytes)
  of caBbr:
    controller.bbr = initBbrModel(
      controller.initialWindowPackets,
      controller.datagramPayloadBytes)
  controller.pacing.reset()

proc onPacketSent*(
    controller: var CongestionController,
    bytes: uint32,
    ackEliciting: bool = true,
    appLimited: bool = false,
    sendTimeUs: uint64 = 0
  ) =
  ## 根据当前策略记录发送事件。
  case controller.algorithm
  of caCubic:
    controller.cubic.onPacketSent(bytes, ackEliciting)
  of caBbr:
    controller.bbr.onPacketSent(bytes, appLimited)
    if sendTimeUs != 0 and ackEliciting:
      controller.pacing.commitSend(sendTimeUs, bytes)

proc onAcked*(
    controller: var CongestionController,
    ack: AckEventSnapshot
  ) =
  ## ACK 到达时更新对应算法状态。
  case controller.algorithm
  of caCubic:
    controller.cubic.onDataAcked(ack)
  of caBbr:
    let ackHeight = controller.bbr.onDataAcked(ack)
    if ackHeight > 0:
      controller.pacing.recordAckHeight(ackHeight)

proc onLost*(
    controller: var CongestionController,
    loss: LossEventSnapshot
  ) =
  ## 丢包或超时回退。
  case controller.algorithm
  of caCubic:
    controller.cubic.onDataLost(loss)
  of caBbr:
    controller.bbr.onDataLost(loss)
    controller.pacing.onLoss()

proc canSend*(controller: CongestionController): bool =
  ## 是否允许继续发包。
  case controller.algorithm
  of caCubic:
    controller.cubic.canSend()
  of caBbr:
    controller.bbr.canSend()

proc sendAllowance*(controller: var CongestionController, nowUs: uint64 = 0): uint64 =
  ## 返回剩余可用窗口，BBR 会结合 pacing 状态给出实际可发送字节数。
  case controller.algorithm
  of caCubic:
    controller.cubic.sendAllowance()
  of caBbr:
    controller.pacing.allowance(controller.bbr, nowUs)

proc congestionWindowBytes*(controller: CongestionController): uint64 =
  ## 获取当前拥塞窗口大小（字节）。
  case controller.algorithm
  of caCubic:
    controller.cubic.congestionWindow.uint64
  of caBbr:
    controller.bbr.congestionWindow.uint64
