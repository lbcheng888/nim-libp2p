# 观测与运维子系统的核心类型定义。

import std/[json, tables, times]

type
  MetricKind* = enum
    mkCounter,
    mkGauge,
    mkHistogram

  LabelPairs* = OrderedTable[string, string]

  MetricDescriptor* = object
    name*: string
    help*: string
    kind*: MetricKind
    labelNames*: seq[string]
    unit*: string

  HistogramState* = object
    boundaries*: seq[float]
    counts*: seq[int64]
    sum*: float

  MetricPoint* = object
    labels*: LabelPairs
    value*: float
    histogram*: HistogramState
    timestamp*: DateTime

  MetricSnapshot* = object
    descriptor*: MetricDescriptor
    points*: seq[MetricPoint]

  ObsEventKind* = enum
    obsConfigReloaded,
    obsRuntimeTuning,
    obsProfilingSample,
    obsMetricAnomaly,
    obsCustom

  ObsEvent* = object
    kind*: ObsEventKind
    timestamp*: DateTime
    message*: string
    tags*: LabelPairs
    payload*: JsonNode

  EventListener* = proc (event: ObsEvent) {.gcsafe.}

  RuntimeCommand* = proc (args: seq[string]): string

  HotSwapScenario* = object
    name*: string
    description*: string
    steps*: seq[string]

  HotSwapResult* = object
    scenario*: HotSwapScenario
    success*: bool
    details*: string

  TelemetryField* = object
    name*: string
    description*: string
    exportKey*: string
    required*: bool

  DashboardPanel* = object
    title*: string
    promQuery*: string
    unit*: string
    description*: string

  DashboardTemplate* = object
    name*: string
    panels*: seq[DashboardPanel]

proc newLabelPairs*(pairs: openArray[(string, string)]): LabelPairs =
  ## 由键值对构造标签表，后续导出时可保持稳定顺序。
  result = initOrderedTable[string, string]()
  for (k, v) in pairs:
    result[k] = v
