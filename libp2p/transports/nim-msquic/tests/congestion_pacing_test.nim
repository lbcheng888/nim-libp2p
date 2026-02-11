## 验证 D2 阶段引入的 pacing、ACK 聚合与时间轮调度逻辑。

import unittest
import std/options

import ../congestion/bbr_model
import ../congestion/common
import ../congestion/pacing_scheduler
import ../congestion/timer_wheel_model

proc makeAck(
    ackedBytes: uint32,
    totalAcked: uint64,
    nowUs: uint64,
    largestAck: uint64,
    largestSent: uint64
  ): AckEventSnapshot =
  AckEventSnapshot(
    timeNow: nowUs,
    largestAck: largestAck,
    largestSentPacketNumber: largestSent,
    totalAckedRetransmittableBytes: totalAcked,
    ackedRetransmittableBytes: ackedBytes,
    smoothedRtt: 200_000,
    minRtt: 200_000,
    oneWayDelay: 0,
    adjustedAckTime: nowUs,
    implicitAck: false,
    hasLoss: false,
    largestAckAppLimited: false,
    minRttValid: true)

suite "BBR pacing enhancements":
  test "ack aggregation increases congestion window":
    var model = initBbrModel(initialWindowPackets = 10, datagramPayloadBytes = 1200)
    model.bytesInFlight = 2400

    var ack1 = makeAck(1200, 1200, 1_000_000, 10, 20)
    discard model.onDataAcked(ack1)
    let cwndAfterFirst = model.congestionWindow

    var ack2 = makeAck(2400, 3600, 1_050_000, 12, 24)
    let ackHeight = model.onDataAcked(ack2)
    check ackHeight > 0
    check model.congestionWindow > cwndAfterFirst

suite "Pacing scheduler":
  test "burst allowance boosts pacing":
    var model = initBbrModel(initialWindowPackets = 10, datagramPayloadBytes = 1200)
    model.congestionWindow = 48_000
    model.bytesInFlight = 24_000
    model.pacingRateBps = 12_000_000
    model.sendQuantum = 1200
    model.minRtt = 200_000

    var pacing = initPacingState()
    pacing.lastSendTimeUs = 1_000_000

    let baseAllowance = pacing.allowance(model, 1_000_500)
    check baseAllowance >= model.sendQuantum

    pacing.commitSend(1_000_500, baseAllowance)
    pacing.recordAckHeight(5_000)
    let burstAllowance = pacing.allowance(model, 1_001_000)
    check burstAllowance > baseAllowance

    pacing.commitSend(1_001_000, min(burstAllowance, 6_000))
    check pacing.burstAllowanceBytes < 5_000

suite "Timer wheel scheduler":
  test "expired entries pop in order":
    var wheel = initTimerWheel()
    wheel.updateConnection(1, 1_000_000)
    wheel.updateConnection(2, 900_000)
    wheel.updateConnection(3, 1_500_000)

    let first = wheel.peekNext()
    check isSome(first)
    check get(first).key == 2

    var expired = wheel.popExpired(950_000)
    check expired.len == 1
    check expired[0].key == 2

    expired = wheel.popExpired(1_600_000)
    check expired.len == 2
    check expired[0].key == 1
    check expired[1].key == 3

    check isNone(wheel.peekNext())
