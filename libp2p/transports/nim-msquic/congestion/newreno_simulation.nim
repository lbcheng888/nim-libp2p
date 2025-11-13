## 简易仿真脚本，用于观察 NewReno cwnd 演进（B2 产出之一）。

import std/sequtils
import ./newreno_model
import ./common

type
  SimulationStep* = object
    ackedBytes*: uint32
    lostBytes*: uint32

proc simulateNewReno*(
    datagramSize: uint32,
    pattern: seq[SimulationStep]
  ): seq[uint64] =
  ## 根据给定 ACK/丢包模式返回每一步的拥塞窗口。
  var model = initNewReno(datagramSize)
  var totalAcked: uint64 = 0
  var largestPacket: uint64 = 0
  for step in pattern:
    if step.ackedBytes > 0:
      model.onPacketSent(step.ackedBytes)
      inc(largestPacket)
      totalAcked += step.ackedBytes.uint64
      let ack = AckEventSnapshot(
        timeNow: largestPacket * 1000,
        largestAck: largestPacket,
        largestSentPacketNumber: largestPacket,
        totalAckedRetransmittableBytes: totalAcked,
        ackedRetransmittableBytes: step.ackedBytes,
        smoothedRtt: 1000,
        minRtt: 1000,
        oneWayDelay: 0,
        adjustedAckTime: largestPacket * 1000,
        implicitAck: false,
        hasLoss: false,
        largestAckAppLimited: false,
        minRttValid: true)
      model.onDataAcked(ack)
    if step.lostBytes > 0:
      model.onPacketSent(step.lostBytes)
      let loss = LossEventSnapshot(
        largestPacketNumberLost: largestPacket,
        largestSentPacketNumber: largestPacket,
        retransmittableBytesLost: step.lostBytes,
        persistentCongestion: false)
      model.onDataLost(loss)
    result.add(model.congestionWindow)

when isMainModule:
  ## 默认场景：前 5 次 ACK 正常，随后一次丢包并恢复。
  let steps = @[
    SimulationStep(ackedBytes: 1200, lostBytes: 0),
    SimulationStep(ackedBytes: 1200, lostBytes: 0),
    SimulationStep(ackedBytes: 1200, lostBytes: 0),
    SimulationStep(ackedBytes: 1200, lostBytes: 0),
    SimulationStep(ackedBytes: 1200, lostBytes: 0),
    SimulationStep(ackedBytes: 0, lostBytes: 2400),
    SimulationStep(ackedBytes: 1200, lostBytes: 0),
    SimulationStep(ackedBytes: 1200, lostBytes: 0)
  ]
  let cwnds = simulateNewReno(1200, steps)
  for idx, cwnd in cwnds:
    echo "step=", idx, " cwnd=", cwnd
