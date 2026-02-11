## 工具组件目录：列出 Nim 版 sample/trace/fuzz/interop 等实现。

import ./common

const
  ToolComponents*: seq[ToolingComponent] = @[
    ToolingComponent(
      name: "nim-sample-cli",
      category: tcCliSample,
      sources: @["nim/tooling/sample_tool.nim"],
      driver: "runSample",
      notes: "MsQuic sample CLI 的 Nim 版本，支持握手演示与诊断输出。"),
    ToolingComponent(
      name: "nim-trace-pipeline",
      category: tcDiagnostics,
      sources: @["nim/tooling/trace_pipeline.nim"],
      driver: "startTraceCollector",
      notes: "捕获 `DiagnosticsEvent` 并汇总 sample/interop 记录。"),
    ToolingComponent(
      name: "nim-fuzz-harness",
      category: tcDiagnostics,
      sources: @["nim/tooling/fuzz_harness.nim"],
      driver: "fuzzInitialFlight",
      notes: "覆盖初始握手 Crypto 帧的模糊测试入口。"),
    ToolingComponent(
      name: "nim-interop-runner",
      category: tcInterop,
      sources: @["nim/tooling/interop_runner.nim"],
      driver: "runInteropScenario",
      notes: "编排 client/server sample 握手，模拟 scripts/interop.ps1 场景。")]

proc componentsByCategory*(category: ToolCategory): seq[ToolingComponent] =
  ToolComponents.filterIt(it.category == category)

