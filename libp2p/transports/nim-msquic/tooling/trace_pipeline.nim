## 诊断/trace 管道：将 Nim 版 MsQuic 事件整理为可读序列。

import std/sequtils
import std/monotimes

import "../api/diagnostics_model"
import ./sample_tool

type
  TraceSource* = enum
    tsDiagnostics
    tsSample
    tsInterop

  TraceRecord* = object
    tick*: int64
    source*: TraceSource
    message*: string

  TraceCollector* = ref object
    name*: string
    records*: seq[TraceRecord]
    active*: bool

proc diagKindName(kind: DiagnosticsEventKind): string =
  case kind
  of diagRegistrationOpened: "diagRegistrationOpened"
  of diagRegistrationShutdown: "diagRegistrationShutdown"
  of diagConfigurationLoaded: "diagConfigurationLoaded"
  of diagConnectionParamSet: "diagConnectionParamSet"
  of diagConnectionStarted: "diagConnectionStarted"
  of diagConnectionEvent: "diagConnectionEvent"

proc traceSourceName(source: TraceSource): string =
  case source
  of tsDiagnostics: "diagnostics"
  of tsSample: "sample"
  of tsInterop: "interop"

proc monotonicTick(): int64 =
  ## 使用单调时钟 ticks （纳秒级）作为排序依据。
  getMonoTime().ticks

proc startTraceCollector*(name: string): TraceCollector =
  let collector = TraceCollector(name: name, records: @[], active: true)
  clearDiagnosticsHooks()
  let self = collector
  registerDiagnosticsHook(proc (event: DiagnosticsEvent) {.gcsafe.} =
    if self.isNil or not self.active:
      return
    let message =
      if event.note.len == 0:
        diagKindName(event.kind)
      else:
        diagKindName(event.kind) & ": " & event.note
    self.records.add TraceRecord(
      tick: monotonicTick(),
      source: tsDiagnostics,
      message: message))
  collector

proc newTraceCollector*(name: string): TraceCollector =
  TraceCollector(name: name, records: @[], active: false)

proc stop*(collector: TraceCollector) =
  if collector.isNil or not collector.active:
    return
  collector.active = false
  clearDiagnosticsHooks()

proc ingestSampleResult*(collector: TraceCollector; result: SampleResult) =
  if collector.isNil:
    return
  for entry in result.trace:
    collector.records.add TraceRecord(
      tick: monotonicTick(),
      source: tsSample,
      message: entry.stage & ": " & entry.detail)
  for diag in result.diagnostics:
    collector.records.add TraceRecord(
      tick: monotonicTick(),
      source: tsDiagnostics,
      message: diag)

proc records*(collector: TraceCollector): seq[TraceRecord] =
  if collector.isNil:
    @[]
  else:
    collector.records.sortedByIt(it.tick)

proc summarize*(collector: TraceCollector): seq[string] =
  collector.records().mapIt(
    "[" & collector.name & "] " & $it.tick & " " & traceSourceName(it.source) &
      ": " & it.message)
