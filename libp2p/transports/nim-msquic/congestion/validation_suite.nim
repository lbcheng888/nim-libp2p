## 验证发布（E2）：聚合 sample、interop 与性能采样，生成拥塞控制回归报告。

import std/math
import std/options
import std/strformat
import std/strutils

import ../tooling/sample_tool
import ../tooling/interop_runner
import ../tooling/perf_ci_pipeline

type
  SamplePair* = object
    ## 定义一次端到端 sample 验证，覆盖 client/server 两端。
    name*: string
    client*: SampleConfig
    server*: SampleConfig
    requireDatagram*: bool

  SamplePairReport* = object
    name*: string
    clientResult*: SampleResult
    serverResult*: SampleResult
    success*: bool
    issues*: seq[string]
    traceLines*: seq[string]

  PerfDatum* = object
    metric*: PerfMetricKind
    statistic*: PerfStatistic
    source*: PerfDataSource
    value*: float
    note*: string

  PerfSnapshot* = object
    name*: string
    measurements*: seq[PerfDatum]

  PerfComparisonSpec* = object
    metric*: PerfMetricKind
    statistic*: PerfStatistic
    source*: PerfDataSource
    comparator*: PerfComparator
    tolerancePct*: float
    description*: string

  PerfComparisonResult* = object
    spec*: PerfComparisonSpec
    baseline*: float
    candidate*: float
    deltaPct*: float
    passed*: bool
    note*: string

  NetworkTestRecord* = object
    name*: string
    channel*: string
    lines*: seq[string]

  CongestionValidationSuite* = object
    name*: string
    samplePairs*: seq[SamplePair]
    interopScenarios*: seq[InteropScenario]
    perfComparisons*: seq[PerfComparisonSpec]
    notes*: seq[string]

  CongestionValidationReport* = object
    suiteName*: string
    sampleResults*: seq[SamplePairReport]
    interopResults*: seq[InteropResult]
    perfResults*: seq[PerfComparisonResult]
    records*: seq[NetworkTestRecord]
    issues*: seq[string]
    success*: bool

proc buildTraceLines(sample: SampleResult; prefix: string): seq[string] =
  result = @[]
  for entry in sample.trace:
    result.add &"{prefix}.{entry.stage}: {entry.detail}"
  for diag in sample.diagnostics:
    result.add &"{prefix}.diagnostics: {diag}"
  if sample.handshakePacket.isSome:
    result.add &"{prefix}.handshakePacket: present"

proc mergeTrace(pairReport: var SamplePairReport) =
  pairReport.traceLines = @[]
  pairReport.traceLines.add &"{pairReport.name}: client/server handshake log"
  let clientLines = buildTraceLines(pairReport.clientResult, "client")
  for line in clientLines:
    pairReport.traceLines.add(line)
  let serverLines = buildTraceLines(pairReport.serverResult, "server")
  for line in serverLines:
    pairReport.traceLines.add(line)

proc runSamplePair(pair: SamplePair): SamplePairReport =
  let clientRes = runSample(pair.client)
  let serverRes = runSample(pair.server)
  var issues: seq[string] = @[]
  if not clientRes.success:
    issues.add("client sample handshake failed")
  if not serverRes.success:
    issues.add("server sample handshake failed")
  if pair.requireDatagram and (not clientRes.datagramEnabled or not serverRes.datagramEnabled):
    issues.add("datagram path not enabled in sample validation")
  var report = SamplePairReport(
    name: pair.name,
    clientResult: clientRes,
    serverResult: serverRes,
    success: issues.len == 0,
    issues: issues,
    traceLines: @[])
  report.mergeTrace()
  report

proc findDatum(snapshot: PerfSnapshot; spec: PerfComparisonSpec): Option[PerfDatum] =
  for datum in snapshot.measurements:
    if datum.metric == spec.metric and
       datum.statistic == spec.statistic and
       datum.source == spec.source:
      return some(datum)
  none(PerfDatum)

proc passes(spec: PerfComparisonSpec; baseline, candidate: float): bool =
  let tolerance = spec.tolerancePct / 100.0
  case spec.comparator
  of pcHigherIsBetter:
    if baseline == 0.0:
      candidate >= 0.0
    else:
      candidate >= baseline * (1.0 - tolerance)
  of pcLowerIsBetter:
    if baseline == 0.0:
      candidate <= spec.tolerancePct
    else:
      candidate <= baseline * (1.0 + tolerance)
  of pcWithinPercent:
    if baseline == 0.0:
      abs(candidate) <= spec.tolerancePct
    else:
      abs(candidate - baseline) <= abs(baseline) * tolerance

proc deltaPercent(baseline, candidate: float): float =
  if baseline == 0.0:
    if candidate == 0.0:
      0.0
    elif candidate > 0.0:
      100.0
    else:
      -100.0
  else:
    (candidate - baseline) / baseline * 100.0

proc evaluatePerf(
    spec: PerfComparisonSpec;
    reference: PerfSnapshot;
    candidate: PerfSnapshot
  ): PerfComparisonResult =
  let baseOpt = reference.findDatum(spec)
  let candOpt = candidate.findDatum(spec)
  if baseOpt.isNone or candOpt.isNone:
    return PerfComparisonResult(
      spec: spec,
      baseline: (if baseOpt.isSome: baseOpt.get.value else: NaN),
      candidate: (if candOpt.isSome: candOpt.get.value else: NaN),
      deltaPct: NaN,
      passed: false,
      note: "missing measurement in snapshot")
  let base = baseOpt.get.value
  let current = candOpt.get.value
  let delta = deltaPercent(base, current)
  let ok = passes(spec, base, current)
  let note = fmt"{spec.description} baseline={base:.2f} candidate={current:.2f} Δ={delta:.2f}% tolerance=±{spec.tolerancePct:.1f}%"
  PerfComparisonResult(
    spec: spec,
    baseline: base,
    candidate: current,
    deltaPct: delta,
    passed: ok,
    note: note)

proc collectTrace(records: var seq[NetworkTestRecord]; report: SamplePairReport) =
  records.add NetworkTestRecord(
    name: report.name,
    channel: "sample",
    lines: report.traceLines)

proc collectInteropTrace(records: var seq[NetworkTestRecord]; result: InteropResult) =
  var lines = @["interop.trace"]
  for entry in result.trace:
    lines.add(entry)
  for issue in result.issues:
    lines.add &"issue: {issue}"
  records.add NetworkTestRecord(
    name: result.scenarioName,
    channel: "interop",
    lines: lines)

proc runCongestionValidation*(
    suite: CongestionValidationSuite;
    candidatePerf: PerfSnapshot;
    referencePerf: PerfSnapshot
  ): CongestionValidationReport =
  var report = CongestionValidationReport(
    suiteName: suite.name,
    sampleResults: @[],
    interopResults: @[],
    perfResults: @[],
    records: @[],
    issues: @[],
    success: true)

  for note in suite.notes:
    report.records.add NetworkTestRecord(
      name: suite.name,
      channel: "note",
      lines: @[note])

  for pair in suite.samplePairs:
    let sampleReport = runSamplePair(pair)
    report.sampleResults.add(sampleReport)
    collectTrace(report.records, sampleReport)
    if not sampleReport.success:
      report.success = false
      report.issues.add &"{pair.name}: " & sampleReport.issues.join("; ")

  for scenario in suite.interopScenarios:
    let interopRes = runInteropScenario(scenario)
    report.interopResults.add(interopRes)
    collectInteropTrace(report.records, interopRes)
    if not interopRes.success:
      report.success = false
      report.issues.add &"{scenario.name}: " & interopRes.issues.join("; ")

  for spec in suite.perfComparisons:
    let perfRes = evaluatePerf(spec, referencePerf, candidatePerf)
    report.perfResults.add(perfRes)
    report.records.add NetworkTestRecord(
      name: spec.description,
      channel: "perf",
      lines: @[perfRes.note])
    if not perfRes.passed:
      report.success = false
      report.issues.add &"{spec.description}: {perfRes.note}"

  report

proc defaultE2PerfComparisons*(): seq[PerfComparisonSpec] =
  @[
    PerfComparisonSpec(
      metric: pmThroughputMbps,
      statistic: psP95,
      source: pdsLola,
      comparator: pcHigherIsBetter,
      tolerancePct: 5.0,
      description: "LOLA 吞吐率 (P95)"),
    PerfComparisonSpec(
      metric: pmLatencyP99Ms,
      statistic: psP99,
      source: pdsNetperf,
      comparator: pcLowerIsBetter,
      tolerancePct: 10.0,
      description: "netperf 延迟 (P99)"),
    PerfComparisonSpec(
      metric: pmPacketLossPercent,
      statistic: psMax,
      source: pdsNetperf,
      comparator: pcLowerIsBetter,
      tolerancePct: 20.0,
      description: "netperf 丢包率 (Max)"),
    PerfComparisonSpec(
      metric: pmHandshakeDurationMs,
      statistic: psMedian,
      source: pdsQlog,
      comparator: pcLowerIsBetter,
      tolerancePct: 15.0,
      description: "qlog 握手时延 (Median)")]

proc defaultE2ValidationSuite*(): CongestionValidationSuite =
  let clientBaseline = initSampleConfig(srClient, peer = "nim-msquic-server")
  let serverBaseline = initSampleConfig(srServer, peer = "nim-msquic-client")
  var datagramClient = clientBaseline
  datagramClient.enableDatagram = true
  var datagramServer = serverBaseline
  datagramServer.enableDatagram = true

  CongestionValidationSuite(
    name: "E2 Congestion Validation",
    samplePairs: @[
      SamplePair(
        name: "sample-baseline",
        client: clientBaseline,
        server: serverBaseline,
        requireDatagram: false),
      SamplePair(
        name: "sample-datagram",
        client: datagramClient,
        server: datagramServer,
        requireDatagram: true)],
    interopScenarios: @[
      InteropScenario(
        name: "interop.ps1-hq-interop",
        client: clientBaseline,
        server: serverBaseline,
        requireDatagram: false),
      InteropScenario(
        name: "interop.ps1-datagram",
        client: datagramClient,
        server: datagramServer,
        requireDatagram: true)],
    perfComparisons: defaultE2PerfComparisons(),
    notes: @[
      "参考 scripts/interop.ps1 场景定义，复用 Nim sample 配置",
      "结合 src/perf 与 src/tools/sample 收集 Nim 与原始 MsQuic 指标"])

proc renderReport*(
    report: CongestionValidationReport;
    includeTraces: bool = false
  ): string =
  var lines: seq[string] = @[
    fmt"[{report.suiteName}] success={report.success} issues={report.issues.len}",
    "sample results:"]
  for entry in report.sampleResults:
    lines.add fmt"  - {entry.name}: success={entry.success}"
    if entry.issues.len > 0:
      lines.add fmt"    issues => {entry.issues.join(\"; \")}"
  lines.add("interop results:")
  for entry in report.interopResults:
    lines.add fmt"  - {entry.scenarioName}: success={entry.success}"
    if entry.issues.len > 0:
      lines.add fmt"    issues => {entry.issues.join(\"; \")}"
  lines.add("perf results:")
  for entry in report.perfResults:
    let status = if entry.passed: "pass" else: "fail"
    lines.add fmt"  - {entry.spec.description}: {status} ({entry.note})"
  if includeTraces:
    lines.add("records:")
    for rec in report.records:
      lines.add fmt"  [{rec.channel}] {rec.name}"
      for line in rec.lines:
        lines.add "    " & line
  if report.issues.len > 0:
    lines.add("issues summary:")
    for issue in report.issues:
      lines.add "  - " & issue
  lines.join("\n")
