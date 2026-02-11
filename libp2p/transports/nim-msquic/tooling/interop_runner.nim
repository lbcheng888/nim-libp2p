## 互操作场景编排：基于 Nim sample 工具验证握手匹配。

import std/sequtils
import std/strformat

import ./sample_tool
import ./trace_pipeline

type
  InteropScenario* = object
    name*: string
    client*: SampleConfig
    server*: SampleConfig
    requireDatagram*: bool

  InteropResult* = object
    scenarioName*: string
    clientResult*: SampleResult
    serverResult*: SampleResult
    trace*: seq[string]
    success*: bool
    issues*: seq[string]

proc runInteropScenario*(scenario: InteropScenario): InteropResult =
  var collector = newTraceCollector(scenario.name)
  let clientRes = runSample(scenario.client)
  let serverRes = runSample(scenario.server)
  collector.ingestSampleResult(clientRes)
  collector.ingestSampleResult(serverRes)

  var issues: seq[string] = @[]
  if not clientRes.success:
    issues.add("client handshake failed")
  if not serverRes.success:
    issues.add("server handshake failed")
  if scenario.requireDatagram and (not clientRes.datagramEnabled or not serverRes.datagramEnabled):
    issues.add("datagram path not enabled")
  if scenario.client.alpn.len > 0 and scenario.client.alpn != serverRes.negotiatedAlpn:
    issues.add fmt"ALPN mismatch: {scenario.client.alpn} vs {serverRes.negotiatedAlpn}"

  InteropResult(
    scenarioName: scenario.name,
    clientResult: clientRes,
    serverResult: serverRes,
    trace: collector.summarize(),
    success: issues.len == 0,
    issues: issues)

