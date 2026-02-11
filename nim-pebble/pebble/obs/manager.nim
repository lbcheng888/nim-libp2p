# 观测与运维中枢：整合指标、事件、运维操作与 Profiling。

import std/[json, strformat, strutils, tables, times]

import pebble/config/loader
import pebble/obs/events
import pebble/obs/metrics
import pebble/obs/ops
import pebble/obs/profiling
import pebble/obs/types

type
  ObservabilityOptions* = object
    histogramBuckets*: seq[float]
    eventBufferSize*: int
    enableProfiling*: bool

  Observability* = ref object
    metrics*: MetricsRegistry
    events*: EventBus
    ops*: OpsCenter
    profiler*: Profiler

proc defaultObservabilityOptions*(): ObservabilityOptions =
  ObservabilityOptions(
    histogramBuckets: DefaultHistogramBuckets,
    eventBufferSize: 512,
    enableProfiling: true
  )

proc publishEvent*(obs: Observability; kind: ObsEventKind;
                   message: string;
                   tags: openArray[(string, string)] = [];
                   payload: JsonNode = nil) {.gcsafe.}

proc newObservability*(options: ObservabilityOptions = defaultObservabilityOptions()): Observability =
  let opts = options
  let metrics = newMetricsRegistry(opts.histogramBuckets)
  let events = newEventBus(opts.eventBufferSize)
  let opsCenter = newOpsCenter()
  let profiler = (if opts.enableProfiling: newProfiler() else: nil)
  metrics.declareMetric(MetricDescriptor(
    name: "pebble_events_total",
    help: "事件计数",
    kind: mkCounter,
    labelNames: @["kind"],
    unit: ""
  ))
  metrics.declareMetric(MetricDescriptor(
    name: "pebble_config_reload_total",
    help: "配置热更新结果统计",
    kind: mkCounter,
    labelNames: @["name", "status"],
    unit: ""
  ))
  metrics.declareMetric(MetricDescriptor(
    name: "pebble_metrics_anomaly_total",
    help: "指标自检发现的异常",
    kind: mkCounter,
    labelNames: @["metric"],
    unit: ""
  ))
  metrics.declareMetric(MetricDescriptor(
    name: "pebble_hot_swap_result_total",
    help: "运行期参数热切换结果",
    kind: mkCounter,
    labelNames: @["scenario", "result"],
    unit: ""
  ))
  result = Observability(metrics: metrics,
                         events: events,
                         ops: opsCenter,
                         profiler: profiler)

  let obsRef = result

  obsRef.ops.registerCommand("metrics/prometheus", "导出 Prometheus 文本",
    proc (_: seq[string]): string {.gcsafe.} =
      if obsRef.metrics.isNil():
        ""
      else:
        obsRef.metrics.renderPrometheus()
  )
  obsRef.ops.registerCommand("metrics/statsd", "导出 StatsD 文本",
    proc (_: seq[string]): string {.gcsafe.} =
      if obsRef.metrics.isNil():
        ""
      else:
        obsRef.metrics.renderStatsd().join("\n")
  )
  obsRef.ops.registerCommand("events/recent", "查看最近事件",
    proc (args: seq[string]): string {.gcsafe.} =
      var limit = 20
      if args.len > 0:
        try:
          limit = args[0].parseInt()
        except ValueError:
          limit = 20
      if limit <= 0:
        limit = 20
      let recent = obsRef.events.recent(limit)
      var arr = newJArray()
      for ev in recent:
        var obj = newJObject()
        obj["kind"] = newJString($ev.kind)
        obj["timestamp"] = newJString(ev.timestamp.format("yyyy-MM-dd'T'HH:mm:ss'.'fff"))
        obj["message"] = newJString(ev.message)
        var tagNode = newJObject()
        for k, v in ev.tags:
          tagNode[k] = newJString(v)
        obj["tags"] = tagNode
        obj["payload"] = (if ev.payload.isNil(): newJNull() else: ev.payload)
        arr.add(obj)
      $arr
  )
  obsRef.ops.registerCommand("config/reload", "立即轮询热更新入口",
    proc (_: seq[string]): string {.gcsafe.} =
      obsRef.ops.pollReloaders()
      obsRef.publishEvent(obsRuntimeTuning,
                          "manual config reload triggered",
                          @[("source", "command")])
      "reload triggered"
  )

proc exportPrometheus*(obs: Observability): string =
  if obs.isNil() or obs.metrics.isNil():
    return ""
  obs.metrics.renderPrometheus()

proc exportStatsd*(obs: Observability; prefix: string = ""): seq[string] =
  if obs.isNil() or obs.metrics.isNil():
    return @[]
  obs.metrics.renderStatsd(prefix)

proc publishEvent*(obs: Observability; kind: ObsEventKind;
                   message: string;
                   tags: openArray[(string, string)] = [];
                   payload: JsonNode = nil) {.gcsafe.} =
  if obs.isNil() or obs.events.isNil():
    return
  let tagTable = newLabelPairs(tags)
  let event = ObsEvent(kind: kind,
                       timestamp: now(),
                       message: message,
                       tags: tagTable,
                       payload: if payload.isNil(): newJNull() else: payload)
  obs.events.publish(event)
  if not obs.metrics.isNil():
    obs.metrics.incCounter("pebble_events_total", "事件计数",
                           @[("kind", $kind)])

proc attachConfigHotReload*(obs: Observability; name, path: string;
                            intervalMs = 1000;
                            callback: ConfigReloadCallback) =
  if obs.isNil():
    return
  let wrapped = proc (cfg: FlatConfig) {.gcsafe.} =
    try:
      callback(cfg)
      obs.metrics.incCounter("pebble_config_reload_total",
                             "配置热更新结果统计",
                             @[("name", name), ("status", "success")])
      var payload = newJObject()
      for k, v in cfg:
        payload[k] = newJString(v)
      obs.publishEvent(obsConfigReloaded,
                       &"config '{name}' reloaded",
                       @[("name", name)],
                       payload)
    except CatchableError as err:
      obs.metrics.incCounter("pebble_config_reload_total",
                             "配置热更新结果统计",
                             @[("name", name), ("status", "failure")])
      obs.publishEvent(obsConfigReloaded,
                       &"config '{name}' reload failed",
                       @[("name", name)],
                       %*{"error": err.msg})
      raise
  let reloader = newConfigHotReloader(path, wrapped, intervalMs)
  obs.ops.registerReloader(name, reloader)

proc runMetricSelfChecks*(obs: Observability): seq[string] =
  if obs.isNil() or obs.metrics.isNil():
    return @[]
  var issues: seq[string] = @[]
  for snap in obs.metrics.snapshot():
    case snap.descriptor.kind
    of mkCounter:
      for point in snap.points:
        if point.value < 0.0:
          let msg = &"counter {snap.descriptor.name} negative: {point.value}"
          issues.add(msg)
          obs.metrics.incCounter("pebble_metrics_anomaly_total",
                                 "指标自检发现的异常",
                                 @[("metric", snap.descriptor.name)])
          obs.publishEvent(obsMetricAnomaly, msg,
                           @[("metric", snap.descriptor.name)])
    of mkGauge:
      discard
    of mkHistogram:
      for point in snap.points:
        var total: int64 = 0
        for count in point.histogram.counts:
          if count < 0:
            let msg = &"histogram {snap.descriptor.name} bucket negative"
            issues.add(msg)
            obs.metrics.incCounter("pebble_metrics_anomaly_total",
                                   "指标自检发现的异常",
                                   @[("metric", snap.descriptor.name)])
            obs.publishEvent(obsMetricAnomaly, msg,
                             @[("metric", snap.descriptor.name)])
          total += count
        if total == 0 and snap.descriptor.name.endsWith("_seconds"):
          issues.add(&"histogram {snap.descriptor.name} has no samples")
  if issues.len > 0:
    obs.publishEvent(obsMetricAnomaly,
                     "metric self-check detected anomalies",
                     @[("count", $issues.len)],
                     %*{"issues": issues})
  issues

proc simulateRuntimeTuning*(obs: Observability; scenario: HotSwapScenario;
                            apply: proc (): bool {.gcsafe.}): HotSwapResult =
  if obs.isNil():
    return HotSwapResult(scenario: scenario, success: false,
                         details: "observability not initialized")
  let simResult = obs.ops.simulateHotSwap(scenario, apply)
  obs.metrics.incCounter("pebble_hot_swap_result_total",
                         "运行期参数热切换结果",
                         @[("scenario", scenario.name),
                           ("result", if simResult.success: "success" else: "failure")])
  obs.publishEvent(obsRuntimeTuning,
                   &"hot swap scenario '{scenario.name}' completed",
                   @[("scenario", scenario.name)],
                   %*{"success": simResult.success, "details": simResult.details})
  simResult

proc profile*(obs: Observability; name: string;
              labels: openArray[(string, string)] = [];
              body: proc () {.gcsafe.}) =
  if obs.isNil() or obs.profiler.isNil():
    body()
    return
  obs.profiler.profileBlock(obs.metrics, obs.events, name, labels, body)

proc recentEvents*(obs: Observability; limit = 20): seq[ObsEvent] =
  if obs.isNil() or obs.events.isNil():
    return @[]
  obs.events.recent(limit)

proc availableCommands*(obs: Observability): seq[(string, string)] =
  if obs.isNil() or obs.ops.isNil():
    return @[]
  obs.ops.listCommands()

proc telemetryCatalog*(obs: Observability): seq[TelemetryField] =
  if obs.isNil() or obs.ops.isNil():
    return @[]
  obs.ops.telemetry()

proc dashboardTemplates*(obs: Observability): seq[DashboardTemplate] =
  if obs.isNil() or obs.ops.isNil():
    return @[]
  obs.ops.dashboards()
