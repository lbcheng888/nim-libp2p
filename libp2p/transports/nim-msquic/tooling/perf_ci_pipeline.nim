## D6 性能回归 CI 蓝图：整合性能基准、qlog 采集与报警策略。

import std/options
import std/sequtils
import std/strformat
import std/strutils

import ./common

type
  PerfDataSource* = enum
    pdsLola
    pdsNetperf
    pdsQlog
    pdsFlamegraph

  PerfMetricKind* = enum
    pmThroughputMbps
    pmLatencyP99Ms
    pmCpuPercent
    pmHandshakeDurationMs
    pmPacketLossPercent

  PerfStatistic* = enum
    psAverage
    psMedian
    psP95
    psP99
    psMax

  PerfComparator* = enum
    pcHigherIsBetter
    pcLowerIsBetter
    pcWithinPercent

  PerfMetricSpec* = object
    metric*: PerfMetricKind
    statistic*: PerfStatistic
    source*: PerfDataSource
    summary*: string

  PerfCollector* = object
    name*: string
    source*: PerfDataSource
    scripts*: seq[string]
    artifacts*: seq[string]
    produces*: set[ArtifactKind]
    summary*: string

  PerfThreshold* = object
    metric*: PerfMetricKind
    statistic*: PerfStatistic
    comparator*: PerfComparator
    baseline*: float
    tolerancePct*: float
    windowMinutes*: int

  PerfAlertPolicy* = object
    name*: string
    threshold*: PerfThreshold
    severity*: string
    notify*: seq[string]
    cooldownMinutes*: int
    autoCreateBug*: bool

  PerfReportChannel* = object
    name*: string
    medium*: string
    endpoint*: string
    includeFlamegraph*: bool

let
  throughputThreshold = PerfThreshold(
    metric: pmThroughputMbps,
    statistic: psP95,
    comparator: pcHigherIsBetter,
    baseline: 8_500.0,
    tolerancePct: 5.0,
    windowMinutes: 60)
  latencyThreshold = PerfThreshold(
    metric: pmLatencyP99Ms,
    statistic: psP99,
    comparator: pcLowerIsBetter,
    baseline: 3.0,
    tolerancePct: 10.0,
    windowMinutes: 60)
  cpuThreshold = PerfThreshold(
    metric: pmCpuPercent,
    statistic: psAverage,
    comparator: pcLowerIsBetter,
    baseline: 65.0,
    tolerancePct: 7.5,
    windowMinutes: 120)
  handshakeThreshold = PerfThreshold(
    metric: pmHandshakeDurationMs,
    statistic: psMedian,
    comparator: pcLowerIsBetter,
    baseline: 25.0,
    tolerancePct: 15.0,
    windowMinutes: 60)
  lossThreshold = PerfThreshold(
    metric: pmPacketLossPercent,
    statistic: psMax,
    comparator: pcLowerIsBetter,
    baseline: 0.5,
    tolerancePct: 20.0,
    windowMinutes: 60)

let
  PerfCollectors*: seq[PerfCollector] = @[
    PerfCollector(
      name: "lola-regression",
      source: pdsLola,
      scripts: @["scripts/lola-perf.ps1", "scripts/emulated-performance.ps1"],
      artifacts: @["artifacts/perf/lola/*.json", "artifacts/perf/lola/*.csv"],
      produces: {akTests, akDocs},
      summary: "基于 lola-perf.ps1 的吞吐与握手性能对比，兼容历史 perf.ps1 输出格式。"),
    PerfCollector(
      name: "netperf-baseline",
      source: pdsNetperf,
      scripts: @["scripts/secnetperf.ps1", "scripts/secnetperf-helpers.psm1"],
      artifacts: @["artifacts/perf/netperf/*.csv"],
      produces: {akTests},
      summary: "使用 secnetperf 场景记录吞吐、延迟、丢包指标，供 CI 回归分析。"),
    PerfCollector(
      name: "qlog-harvest",
      source: pdsQlog,
      scripts: @["nim/tooling/trace_pipeline.nim"],
      artifacts: @["artifacts/qlog/*.sqlog", "artifacts/qlog/*.json"],
      produces: {akDocs, akTools},
      summary: "驱动 Nim trace pipeline 汇集 qlog 轨迹，标注握手与拥塞事件。"),
    PerfCollector(
      name: "flamegraph-profile",
      source: pdsFlamegraph,
      scripts: @["scripts/emulated-performance.ps1"],
      artifacts: @["artifacts/perf/flamegraph/*.svg", "artifacts/perf/flamegraph/*.folded"],
      produces: {akDocs},
      summary: "在性能节点生成 CPU flamegraph，跟踪 Nim 热路径回归。")]

let
  PerfMetrics*: seq[PerfMetricSpec] = @[
    PerfMetricSpec(
      metric: pmThroughputMbps,
      statistic: psP95,
      source: pdsLola,
      summary: "LOLA 场景 95 百分位吞吐率 (Mbps)，验证总体回归。"),
    PerfMetricSpec(
      metric: pmLatencyP99Ms,
      statistic: psP99,
      source: pdsNetperf,
      summary: "netperf RTT/延迟 P99 指标 (ms)，评估尾延迟。"),
    PerfMetricSpec(
      metric: pmCpuPercent,
      statistic: psAverage,
      source: pdsFlamegraph,
      summary: "CPU 平均占用率，取自 flamegraph perf script。"),
    PerfMetricSpec(
      metric: pmHandshakeDurationMs,
      statistic: psMedian,
      source: pdsQlog,
      summary: "通过 qlog 事件计算握手完成耗时中位数 (ms)。"),
    PerfMetricSpec(
      metric: pmPacketLossPercent,
      statistic: psMax,
      source: pdsNetperf,
      summary: "netperf 报告的最大丢包率 (%).")]

let
  PerfThresholds*: seq[PerfThreshold] = @[
    throughputThreshold,
    latencyThreshold,
    cpuThreshold,
    handshakeThreshold,
    lossThreshold]

let
  PerfAlerts*: seq[PerfAlertPolicy] = @[
    PerfAlertPolicy(
      name: "throughput-regression-critical",
      threshold: throughputThreshold,
      severity: "critical",
      notify: @["perf-oncall@msquic.dev", "nim-msquic@contoso.example"],
      cooldownMinutes: 30,
      autoCreateBug: true),
    PerfAlertPolicy(
      name: "latency-tail-warning",
      threshold: latencyThreshold,
      severity: "warning",
      notify: @["perf-oncall@msquic.dev"],
      cooldownMinutes: 120,
      autoCreateBug: false),
    PerfAlertPolicy(
      name: "cpu-usage-watch",
      threshold: cpuThreshold,
      severity: "warning",
      notify: @["perf-oncall@msquic.dev"],
      cooldownMinutes: 180,
      autoCreateBug: false),
    PerfAlertPolicy(
      name: "handshake-regression",
      threshold: handshakeThreshold,
      severity: "critical",
      notify: @["nim-msquic-core@contoso.example"],
      cooldownMinutes: 60,
      autoCreateBug: true)]

let
  PerfReportChannels*: seq[PerfReportChannel] = @[
    PerfReportChannel(
      name: "teams-perf-room",
      medium: "teams-webhook",
      endpoint: "https://alerts.contoso.example/hooks/msquic/perf",
      includeFlamegraph: true),
    PerfReportChannel(
      name: "email-weekly-digest",
      medium: "smtp",
      endpoint: "perf-reports@msquic.dev",
      includeFlamegraph: false)]

proc evaluateThreshold*(threshold: PerfThreshold, candidate: float): bool =
  let factor = threshold.tolerancePct / 100.0
  case threshold.comparator
  of pcHigherIsBetter:
    if threshold.baseline == 0.0:
      return candidate >= 0.0
    let floorValue = threshold.baseline * (1.0 - factor)
    candidate >= floorValue
  of pcLowerIsBetter:
    if threshold.baseline == 0.0:
      return candidate <= threshold.tolerancePct
    let ceilingValue = threshold.baseline * (1.0 + factor)
    candidate <= ceilingValue
  of pcWithinPercent:
    if threshold.baseline == 0.0:
      return abs(candidate) <= threshold.tolerancePct
    abs(candidate - threshold.baseline) <= abs(threshold.baseline) * factor

proc metricsBySource*(source: PerfDataSource): seq[PerfMetricSpec] =
  PerfMetrics.filterIt(it.source == source)

proc collectorBySource*(source: PerfDataSource): Option[PerfCollector] =
  for collector in PerfCollectors:
    if collector.source == source:
      return some(collector)
  none(PerfCollector)

proc describeAlert*(policy: PerfAlertPolicy): string =
  let threshold = policy.threshold
  let direction =
    case threshold.comparator
    of pcHigherIsBetter: "下降超过"
    of pcLowerIsBetter: "上升超过"
    of pcWithinPercent: "偏离"
  let notifyList = policy.notify.join(", ")
  &"{policy.name}: {policy.severity} {direction} ±{threshold.tolerancePct:.1f}% (baseline {threshold.baseline:.2f})，通知 {notifyList}"

proc renderReportTargets*(): string =
  PerfReportChannels.mapIt(&"{it.name} => {it.medium} ({it.endpoint})").join("\n")

proc pipelineSummary*(): string =
  var lines: seq[string]
  for collector in PerfCollectors:
    lines.add(&"[collector] {collector.name}: {collector.summary}")
  for metric in PerfMetrics:
    lines.add(&"[metric] {metric.metric} {metric.statistic} <- {metric.source}")
  for alert in PerfAlerts:
    lines.add(&"[alert] {alert.describeAlert()}")
  lines.add("[reports] " & renderReportTargets())
  lines.join("\n")
