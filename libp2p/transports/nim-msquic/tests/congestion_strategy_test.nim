## 针对 C2 阶段新增的拥塞控制策略实现进行验证。

import std/unittest

import ../congestion/controller
import ../congestion/common
import ../congestion/bbr_model

suite "Congestion controller strategy":

  test "BBR grows congestion window under steady ACKs":
    var controller = initCongestionController(caBbr, 1200'u32)
    const packetsPerRound = 10
    for i in 0..<6:
      for _ in 0..<packetsPerRound:
        controller.onPacketSent(1200, appLimited = false)
      var ack = AckEventSnapshot(
        timeNow: uint64(i + 1) * 100_000,
        largestAck: uint64((i + 1) * packetsPerRound),
        largestSentPacketNumber: uint64((i + 1) * packetsPerRound),
        totalAckedRetransmittableBytes: uint64((i + 1) * packetsPerRound * 1200),
        ackedRetransmittableBytes: uint32(packetsPerRound * 1200),
        smoothedRtt: 100_000,
        minRtt: 95_000,
        oneWayDelay: 0,
        adjustedAckTime: uint64(i + 1) * 100_000,
        implicitAck: false,
        hasLoss: false,
        largestAckAppLimited: false,
        minRttValid: true
      )
      controller.onAcked(ack)

    let initialWindow = uint64(controller.initialWindowPackets) * uint64(controller.datagramPayloadBytes)
    check controller.congestionWindowBytes() > initialWindow
    check controller.canSend()
    check controller.bbr.state in {bbrStartup, bbrProbeBandwidth, bbrDrain}

  test "CUBIC reduces congestion window after loss":
    var controller = initCongestionController(caCubic, 1200'u32)
    for i in 0..<6:
      controller.onPacketSent(1200)
      var ack = AckEventSnapshot(
        timeNow: uint64(i + 1) * 120_000,
        largestAck: uint64(i + 1),
        largestSentPacketNumber: uint64(i + 1),
        totalAckedRetransmittableBytes: uint64((i + 1) * 1200),
        ackedRetransmittableBytes: 1200,
        smoothedRtt: 120_000,
        minRtt: 110_000,
        oneWayDelay: 0,
        adjustedAckTime: uint64(i + 1) * 120_000,
        implicitAck: false,
        hasLoss: false,
        largestAckAppLimited: false,
        minRttValid: true
      )
      controller.onAcked(ack)

    let windowBeforeLoss = controller.congestionWindowBytes()
    controller.onLost(LossEventSnapshot(
      largestPacketNumberLost: 6,
      largestSentPacketNumber: 6,
      retransmittableBytesLost: 1200,
      persistentCongestion: false))
    check controller.congestionWindowBytes() < windowBeforeLoss

  test "Persistent congestion clamps CUBIC to minimum window":
    var controller = initCongestionController(caCubic, 1200'u32)
    for _ in 0..<6:
      controller.onPacketSent(1200)
    controller.onLost(LossEventSnapshot(
      largestPacketNumberLost: 6,
      largestSentPacketNumber: 6,
      retransmittableBytesLost: 7_200,
      persistentCongestion: true))
    check controller.congestionWindowBytes() == 4'u64 * 1200'u64

  test "Persistent congestion clamps BBR to minimum window":
    var controller = initCongestionController(caBbr, 1200'u32)
    for _ in 0..<6:
      controller.onPacketSent(1200, appLimited = false)
    controller.onLost(LossEventSnapshot(
      largestPacketNumberLost: 6,
      largestSentPacketNumber: 6,
      retransmittableBytesLost: 7_200,
      persistentCongestion: true))
    check controller.congestionWindowBytes() == 4'u64 * 1200'u64

  test "Probe timeout clamps CUBIC and BBR to minimum window":
    var cubic = initCongestionController(caCubic, 1200'u32)
    for i in 0..<6:
      cubic.onPacketSent(1200)
      var ack = AckEventSnapshot(
        timeNow: uint64(i + 1) * 120_000,
        largestAck: uint64(i + 1),
        largestSentPacketNumber: uint64(i + 1),
        totalAckedRetransmittableBytes: uint64((i + 1) * 1200),
        ackedRetransmittableBytes: 1200,
        smoothedRtt: 120_000,
        minRtt: 110_000,
        oneWayDelay: 0,
        adjustedAckTime: uint64(i + 1) * 120_000,
        implicitAck: false,
        hasLoss: false,
        largestAckAppLimited: false,
        minRttValid: true
      )
      cubic.onAcked(ack)
    check cubic.congestionWindowBytes() > 4'u64 * 1200'u64
    cubic.onProbeTimeout()
    check cubic.congestionWindowBytes() == 4'u64 * 1200'u64

    var bbr = initCongestionController(caBbr, 1200'u32)
    for i in 0..<6:
      bbr.onPacketSent(1200, appLimited = false)
      var ack = AckEventSnapshot(
        timeNow: uint64(i + 1) * 100_000,
        largestAck: uint64(i + 1),
        largestSentPacketNumber: uint64(i + 1),
        totalAckedRetransmittableBytes: uint64((i + 1) * 1200),
        ackedRetransmittableBytes: 1200,
        smoothedRtt: 100_000,
        minRtt: 95_000,
        oneWayDelay: 0,
        adjustedAckTime: uint64(i + 1) * 100_000,
        implicitAck: false,
        hasLoss: false,
        largestAckAppLimited: false,
        minRttValid: true
      )
      bbr.onAcked(ack)
    check bbr.congestionWindowBytes() >= 4'u64 * 1200'u64
    bbr.onProbeTimeout()
    check bbr.congestionWindowBytes() == 4'u64 * 1200'u64

  test "Strategy switch resets state to target algorithm":
    var controller = initCongestionController(caBbr, 1200'u32)
    controller.onPacketSent(1200)
    controller.switchAlgorithm(caCubic)
    check controller.algorithm == caCubic
    let expectedWindow = uint64(controller.initialWindowPackets) * uint64(controller.datagramPayloadBytes)
    check controller.congestionWindowBytes() == expectedWindow
    controller.switchAlgorithm(caBbr)
    check controller.algorithm == caBbr

  test "ACK-only sends do not consume BBR send allowance":
    var controller = initCongestionController(caBbr, 1200'u32)
    let initialAllowance = controller.sendAllowance()
    controller.onPacketSent(1200, ackEliciting = false, appLimited = false)
    check controller.sendAllowance() == initialAllowance
