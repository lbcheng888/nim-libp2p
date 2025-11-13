# 轻量级 profiling hook，支持指标与事件双写。

import std/[json, monotimes, strformat, times]

import pebble/obs/events
import pebble/obs/metrics
import pebble/obs/types

type
  ProfilingSink* = proc (name: string; durationSeconds: float) {.gcsafe.}

  Profiler* = ref object
    enabled*: bool
    sinks: seq[ProfilingSink]

proc newProfiler*(): Profiler =
  Profiler(enabled: true, sinks: @[])

proc registerSink*(profiler: Profiler; sink: ProfilingSink) =
  if profiler.isNil() or sink.isNil():
    return
  profiler.sinks.add(sink)

proc notifySinks(profiler: Profiler; name: string; duration: float) =
  if profiler.isNil():
    return
  for sink in profiler.sinks:
    try:
      if not sink.isNil():
        sink(name, duration)
    except CatchableError:
      discard

proc profileBlock*(profiler: Profiler; metrics: MetricsRegistry;
                   bus: EventBus; name: string;
                   labels: openArray[(string, string)] = [];
                   body: proc () {.gcsafe.}) =
  if profiler.isNil() or not profiler.enabled:
    body()
    return
  let start = getMonoTime()
  try:
    body()
  finally:
    let elapsed = (getMonoTime() - start).inNanoseconds.float / 1_000_000_000.0
    if not metrics.isNil():
      metrics.observeHistogram("pebble_profile_seconds",
                               "Profiling duration",
                               labels,
                               elapsed)
    if not bus.isNil():
      let event = ObsEvent(
        kind: obsProfilingSample,
        timestamp: now(),
        message: &"profiling block '{name}' completed",
        tags: newLabelPairs(labels),
        payload: %*{"duration_seconds": elapsed}
      )
      bus.publish(event)
    profiler.notifySinks(name, elapsed)
