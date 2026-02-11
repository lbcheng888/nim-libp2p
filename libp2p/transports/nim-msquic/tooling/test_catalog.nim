## 测试套件总览，对齐 `src/test`, `scripts/test.ps1` 等入口。

import ./common

const
  TestDriverScripts*: seq[ScriptDescriptor] = @[
    ScriptDescriptor(
      path: "scripts/test.ps1",
      language: slPowerShell,
      summary: "顶层测试入口，调度 GTest、Interop 与压力测试。",
      stage: psTest),
    ScriptDescriptor(
      path: "scripts/run-gtest.ps1",
      language: slPowerShell,
      summary: "运行 `src/test/bin` 下生成的 gtest 可执行文件。",
      stage: psTest),
    ScriptDescriptor(
      path: "scripts/interop.ps1",
      language: slPowerShell,
      summary: "与 QUIC.NET/QNS 等外部实现进行互操作测试。",
      stage: psInterop),
    ScriptDescriptor(
      path: "scripts/recvfuzz.ps1",
      language: slPowerShell,
      summary: "启动 Fuzzing 场景，使用 `src/fuzzing` 定义。",
      stage: psTest),
    ScriptDescriptor(
      path: "scripts/lola-perf.ps1",
      language: slPowerShell,
      summary: "调用内置性能测试 (LOLA)，结合 `src/perf`。",
      stage: psPerf)]

  TestSuites*: seq[TestSuiteDescriptor] = @[
    TestSuiteDescriptor(
      name: "lib-gtests",
      kind: tsUnit,
      sources: @["src/test/lib", "src/test/MsQuicTests.h"],
      driverScripts: @["scripts/run-gtest.ps1"],
      notes: "基于 GTest 的核心协议与 API 单测，涵盖连接、流、MTU、握手等。"),
    TestSuiteDescriptor(
      name: "bin-integration",
      kind: tsIntegration,
      sources: @["src/test/bin", "src/tools/sample", "nim/tooling/sample_tool.nim"],
      driverScripts: @["scripts/test.ps1"],
      notes: "调用 sample 工具进行端到端握手、数据传输与配置验证。"),
    TestSuiteDescriptor(
      name: "interop-suite",
      kind: tsInterop,
      sources: @["scripts/interop.ps1", "src/tools/interop", "nim/tooling/interop_runner.nim"],
      driverScripts: @["scripts/interop.ps1"],
      notes: "与 msquic 生态及 QNS 的互通测试，依赖 docker/qns.Dockerfile。"),
    TestSuiteDescriptor(
      name: "performance",
      kind: tsPerf,
      sources: @["src/perf", "scripts/lola-perf.ps1", "scripts/rps.ps1"],
      driverScripts: @["scripts/lola-perf.ps1", "scripts/emulated-performance.ps1"],
      notes: "吞吐、延迟、CPU 利用率等性能基准，支持自动化对比。"),
    TestSuiteDescriptor(
      name: "fuzzing",
      kind: tsFuzz,
      sources: @["src/fuzzing", "scripts/recvfuzz.ps1", "scripts/secnetperf.ps1", "nim/tooling/fuzz_harness.nim"],
      driverScripts: @["scripts/recvfuzz.ps1"],
      notes: "覆盖数据包接收路径与安全场景的模糊测试。")]
