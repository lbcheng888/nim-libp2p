## 验证 E2 阶段新增的拥塞恢复回归套件。

import unittest
import std/sequtils

import ../congestion/validation_suite

proc makeReferenceSnapshot(): PerfSnapshot =
  PerfSnapshot(
    name: "msquic-original",
    measurements: @[
      PerfDatum(
        metric: pmThroughputMbps,
        statistic: psP95,
        source: pdsLola,
        value: 8_500.0,
        note: "baseline throughput"),
      PerfDatum(
        metric: pmLatencyP99Ms,
        statistic: psP99,
        source: pdsNetperf,
        value: 3.0,
        note: "baseline latency"),
      PerfDatum(
        metric: pmPacketLossPercent,
        statistic: psMax,
        source: pdsNetperf,
        value: 0.3,
        note: "baseline loss"),
      PerfDatum(
        metric: pmHandshakeDurationMs,
        statistic: psMedian,
        source: pdsQlog,
        value: 25.0,
        note: "baseline handshake time")])

proc makeCandidateSnapshot(
    throughput: float;
    latency: float;
    loss: float;
    handshake: float
  ): PerfSnapshot =
  PerfSnapshot(
    name: "nim-msquic",
    measurements: @[
      PerfDatum(
        metric: pmThroughputMbps,
        statistic: psP95,
        source: pdsLola,
        value: throughput,
        note: "nim throughput"),
      PerfDatum(
        metric: pmLatencyP99Ms,
        statistic: psP99,
        source: pdsNetperf,
        value: latency,
        note: "nim latency"),
      PerfDatum(
        metric: pmPacketLossPercent,
        statistic: psMax,
        source: pdsNetperf,
        value: loss,
        note: "nim loss"),
      PerfDatum(
        metric: pmHandshakeDurationMs,
        statistic: psMedian,
        source: pdsQlog,
        value: handshake,
        note: "nim handshake time")])

suite "congestion validation suite (E2)":
  test "Nim 指标落在容差以内时通过":
    let validationSuite = defaultE2ValidationSuite()
    let reference = makeReferenceSnapshot()
    let candidate = makeCandidateSnapshot(
      throughput = 8_320.0,
      latency = 3.2,
      loss = 0.35,
      handshake = 26.5)
    let report = runCongestionValidation(
      validationSuite,
      candidate,
      reference)
    check report.success
    check report.issues.len == 0
    check report.perfResults.len == validationSuite.perfComparisons.len
    for sampleReport in report.sampleResults:
      check sampleReport.success
    for interopReport in report.interopResults:
      check interopReport.success

  test "吞吐下降超出阈值会被标记为回归":
    let validationSuite = defaultE2ValidationSuite()
    let reference = makeReferenceSnapshot()
    let candidate = makeCandidateSnapshot(
      throughput = 7_000.0,
      latency = 3.2,
      loss = 0.35,
      handshake = 26.5)
    let report = runCongestionValidation(
      validationSuite,
      candidate,
      reference)
    check report.success == false
    check report.issues.len > 0
    let perfFailures = report.perfResults.filterIt(not it.passed)
    check perfFailures.len >= 1
