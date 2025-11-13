# 观测指标注册、聚合与导出逻辑。

import std/[locks, sequtils, strformat, strutils, tables, times]

import pebble/obs/types

type
  MetricDatum = object
    value: float
    histogram: HistogramState

  MetricEntry = ref object
    desc: MetricDescriptor
    lock: Lock
    data: Table[string, MetricDatum]

  MetricsRegistry* = ref object
    lock: Lock
    metrics: Table[string, MetricEntry]
    defaultBuckets*: seq[float]

const
  DefaultHistogramBuckets* = @[0.001, 0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]

proc newMetricsRegistry*(buckets: seq[float] = DefaultHistogramBuckets): MetricsRegistry =
  result = MetricsRegistry(metrics: initTable[string, MetricEntry](),
                           defaultBuckets: buckets)
  initLock(result.lock)

proc labelsKey(desc: MetricDescriptor; labels: LabelPairs): string =
  if desc.labelNames.len == 0:
    return ""
  var parts: seq[string] = @[]
  for name in desc.labelNames:
    let value = if labels.hasKey(name): labels[name] else: ""
    parts.add(name & "=" & value)
  parts.join("|")

proc ensureHistogram(desc: MetricDescriptor; datum: var MetricDatum; buckets: seq[float]) =
  if datum.histogram.boundaries.len == 0:
    datum.histogram.boundaries = buckets
    datum.histogram.counts = newSeq[int64](buckets.len + 1)
    datum.histogram.sum = 0.0

proc ensureMetric(registry: MetricsRegistry; descriptor: MetricDescriptor): MetricEntry =
  acquire(registry.lock)
  if registry.metrics.hasKey(descriptor.name):
    let entry = registry.metrics[descriptor.name]
    release(registry.lock)
    return entry
  var entry = MetricEntry(desc: descriptor,
                          data: initTable[string, MetricDatum]())
  initLock(entry.lock)
  registry.metrics[descriptor.name] = entry
  release(registry.lock)
  entry

proc declareMetric*(registry: MetricsRegistry; descriptor: MetricDescriptor) =
  discard ensureMetric(registry, descriptor)

proc incCounter*(registry: MetricsRegistry; name: string; help: string = "";
                 labels: openArray[(string, string)] = [];
                 amount = 1.0) =
  let descriptor = MetricDescriptor(name: name, help: help,
                                    kind: mkCounter,
                                    labelNames: @labels.mapIt(it[0]),
                                    unit: "")
  let entry = ensureMetric(registry, descriptor)
  let labelPairs = newLabelPairs(labels)
  let key = labelsKey(entry.desc, labelPairs)
  acquire(entry.lock)
  var datum = entry.data.getOrDefault(key)
  datum.value += amount
  datum.histogram.boundaries.setLen(0)
  entry.data[key] = datum
  release(entry.lock)

proc setGauge*(registry: MetricsRegistry; name: string; help: string = "";
               labels: openArray[(string, string)] = [];
               value: float) =
  let descriptor = MetricDescriptor(name: name, help: help,
                                    kind: mkGauge,
                                    labelNames: @labels.mapIt(it[0]),
                                    unit: "")
  let entry = ensureMetric(registry, descriptor)
  let labelPairs = newLabelPairs(labels)
  let key = labelsKey(entry.desc, labelPairs)
  acquire(entry.lock)
  var datum = entry.data.getOrDefault(key)
  datum.value = value
  datum.histogram.boundaries.setLen(0)
  entry.data[key] = datum
  release(entry.lock)

proc observeHistogram*(registry: MetricsRegistry; name: string; help: string = "";
                       labels: openArray[(string, string)] = [];
                       value: float; buckets: seq[float] = @[]) =
  let effectiveBuckets = if buckets.len > 0: buckets else: registry.defaultBuckets
  var labelNames: seq[string] = @labels.mapIt(it[0])
  if labelNames.len == 0:
    labelNames = @[]
  let descriptor = MetricDescriptor(name: name, help: help,
                                    kind: mkHistogram,
                                    labelNames: labelNames,
                                    unit: "seconds")
  let entry = ensureMetric(registry, descriptor)
  let labelPairs = newLabelPairs(labels)
  let key = labelsKey(entry.desc, labelPairs)
  acquire(entry.lock)
  var datum = entry.data.getOrDefault(key)
  ensureHistogram(entry.desc, datum, effectiveBuckets)
  datum.histogram.sum += value
  block bucketLoop:
    for idx, boundary in datum.histogram.boundaries:
      if value <= boundary:
        datum.histogram.counts[idx] += 1
        break bucketLoop
    datum.histogram.counts[^1] += 1
  entry.data[key] = datum
  release(entry.lock)

proc snapshot*(registry: MetricsRegistry): seq[MetricSnapshot] =
  acquire(registry.lock)
  let entries = toSeq(registry.metrics.values)
  release(registry.lock)
  for entry in entries:
    acquire(entry.lock)
    var snap = MetricSnapshot(descriptor: entry.desc, points: @[])
    for key, datum in entry.data:
      var labels = initOrderedTable[string, string]()
      if entry.desc.labelNames.len > 0 and key.len > 0:
        for pair in key.split('|'):
          let idx = pair.find('=')
          if idx >= 0:
            labels[pair[0 ..< idx]] = pair[idx + 1 .. ^1]
      var point = MetricPoint(labels: labels,
                              value: datum.value,
                              histogram: datum.histogram,
                              timestamp: now())
      snap.points.add(point)
    release(entry.lock)
    result.add(snap)

proc escapeLabel(value: string): string =
  result = value.replace("\\", "\\\\").replace("\n", "\\n").replace("\"", "\\\"")

proc renderPrometheus*(registry: MetricsRegistry): string =
  var lines: seq[string] = @[]
  for snap in registry.snapshot():
    lines.add(&"# HELP {snap.descriptor.name} {snap.descriptor.help}")
    let metricType = case snap.descriptor.kind
      of mkCounter: "counter"
      of mkGauge: "gauge"
      of mkHistogram: "histogram"
    lines.add(&"# TYPE {snap.descriptor.name} {metricType}")
    case snap.descriptor.kind
    of mkCounter, mkGauge:
      for point in snap.points:
        var labelParts: seq[string] = @[]
        for k, v in point.labels.pairs:
          labelParts.add(&"{k}=\"{escapeLabel(v)}\"")
        let labelSuffix = if labelParts.len > 0: "{" & labelParts.join(",") & "}" else: ""
        lines.add(&"{snap.descriptor.name}{labelSuffix} {point.value:.6f}")
    of mkHistogram:
      for point in snap.points:
        var cumulativeCount: int64 = 0
        for idx, boundary in point.histogram.boundaries:
          cumulativeCount += point.histogram.counts[idx]
          var labelParts: seq[string] = @[]
          for k, v in point.labels.pairs:
            labelParts.add(&"{k}=\"{escapeLabel(v)}\"")
          labelParts.add(&"le=\"{boundary:.6f}\"")
          lines.add(&"{snap.descriptor.name}_bucket{{{labelParts.join(\",\")}}} {cumulativeCount}")
        cumulativeCount += point.histogram.counts[^1]
        var baseLabels: seq[string] = @[]
        for k, v in point.labels.pairs:
          baseLabels.add(&"{k}=\"{escapeLabel(v)}\"")
        var bucketLabels = baseLabels & @["le=\"+Inf\""]
        lines.add(&"{snap.descriptor.name}_bucket{{{bucketLabels.join(\",\")}}} {cumulativeCount}")
        let baseSuffix = if baseLabels.len > 0: "{" & baseLabels.join(",") & "}" else: ""
        lines.add(&"{snap.descriptor.name}_sum{baseSuffix} {point.histogram.sum:.6f}")
        lines.add(&"{snap.descriptor.name}_count{baseSuffix} {cumulativeCount}")
  lines.join("\n")

proc renderStatsd*(registry: MetricsRegistry; prefix: string = ""): seq[string] =
  for snap in registry.snapshot():
    let metricName = if prefix.len > 0: prefix & "." & snap.descriptor.name
                     else: snap.descriptor.name
    case snap.descriptor.kind
    of mkCounter:
      for point in snap.points:
        result.add(&"{metricName}:{point.value:.0f}|c")
    of mkGauge:
      for point in snap.points:
        result.add(&"{metricName}:{point.value:.6f}|g")
    of mkHistogram:
      var totalCount: int64 = 0
      for point in snap.points:
        totalCount = 0
        for idx in 0 ..< point.histogram.counts.len:
          totalCount += point.histogram.counts[idx]
        result.add(&"{metricName}.count:{totalCount}|c")
        if totalCount > 0:
          let avg = point.histogram.sum / totalCount.float
          result.add(&"{metricName}.avg:{avg:.6f}|g")
          result.add(&"{metricName}.sum:{point.histogram.sum:.6f}|g")
