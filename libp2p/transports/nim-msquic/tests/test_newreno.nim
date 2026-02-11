## 基础单测：验证 NewReno 慢启动与丢包恢复逻辑（B6）。

import unittest
import ../congestion/newreno_model
import ../congestion/common

proc makeAck(ackedBytes: uint32, largestAck: uint64): AckEventSnapshot =
  AckEventSnapshot(
    timeNow: largestAck * 1000,
    largestAck: largestAck,
    largestSentPacketNumber: largestAck,
    totalAckedRetransmittableBytes: ackedBytes,
    ackedRetransmittableBytes: ackedBytes,
    smoothedRtt: 1_000,
    minRtt: 1_000,
    oneWayDelay: 0,
    adjustedAckTime: largestAck * 1000,
    implicitAck: false,
    hasLoss: false,
    largestAckAppLimited: false,
    minRttValid: true)

proc makeLoss(lostBytes: uint32, pn: uint64): LossEventSnapshot =
  LossEventSnapshot(
    largestPacketNumberLost: pn,
    largestSentPacketNumber: pn,
    retransmittableBytesLost: lostBytes,
    persistentCongestion: false)

suite "NewReno basic behaviour":
  test "slow start increases cwnd by acked bytes":
    var model = initNewReno(1200)
    let initialCwnd = model.congestionWindow
    model.onPacketSent(1200)
    model.onDataAcked(makeAck(1200, 1))
    check model.congestionWindow == initialCwnd + 1200
    check model.phase == nrSlowStart

  test "loss halves congestion window and enters recovery":
    var model = initNewReno(1200)
    model.onPacketSent(2400)
    model.onDataAcked(makeAck(1200, 1))
    let cwndBeforeLoss = model.congestionWindow
    model.onDataLost(makeLoss(1200, 2))
    check model.congestionWindow == max(cwndBeforeLoss shr 1, model.minCongestionWindow)
    check model.phase == nrRecovery

  test "recovery exits after acknowledging higher packet":
    var model = initNewReno(1200)
    model.onPacketSent(2400)
    model.onDataAcked(makeAck(1200, 1))
    model.onDataLost(makeLoss(1200, 2))
    check model.phase == nrRecovery
    model.onPacketSent(1200)
    model.onDataAcked(makeAck(1200, 3))
    check model.phase != nrRecovery
