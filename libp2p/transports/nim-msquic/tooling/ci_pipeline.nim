## CI 流水线蓝图，汇总 `scripts/*.ps1` 与 `scripts/*.sh` 的关键环节。

import ./common

const
  CiSupportScripts*: seq[ScriptDescriptor] = @[
    ScriptDescriptor(
      path: "scripts/prepare-machine.ps1",
      language: slPowerShell,
      summary: "安装构建依赖、证书与工具链，CI 环境初始化。",
      stage: psConfigure),
    ScriptDescriptor(
      path: "scripts/install-build-artifacts.ps1",
      language: slPowerShell,
      summary: "将生成的二进制与调试符号部署到测试机。",
      stage: psBuild),
    ScriptDescriptor(
      path: "scripts/merge-coverage.ps1",
      language: slPowerShell,
      summary: "聚合测试覆盖率，生成上传报告。",
      stage: psTest),
    ScriptDescriptor(
      path: "scripts/validate-package-commits.ps1",
      language: slPowerShell,
      summary: "验证发行包对应的 Git 提交和元数据一致性。",
      stage: psPackage),
    ScriptDescriptor(
      path: "scripts/update-version.ps1",
      language: slPowerShell,
      summary: "根据版本策略更新 `version.json` 等文件。",
      stage: psPublish),
    ScriptDescriptor(
      path: "scripts/update-docfx-site.ps1",
      language: slPowerShell,
      summary: "刷新 DocFX 文档站点，发布 API 文档。",
      stage: psDoc),
    ScriptDescriptor(
      path: "scripts/qns.Dockerfile",
      language: slShell,
      summary: "构建 QNS 互操作 Docker 镜像。",
      stage: psInterop),
    ScriptDescriptor(
      path: "scripts/emulated-performance.ps1",
      language: slPowerShell,
      summary: "在 CI 性能节点模拟高并发表现，生成 CPU profile 与 qlog。",
      stage: psPerf),
    ScriptDescriptor(
      path: "scripts/secnetperf.ps1",
      language: slPowerShell,
      summary: "调用 netperf/SECNET 场景收集吞吐与延迟基线。",
      stage: psPerf),
    ScriptDescriptor(
      path: "nim/tooling/perf_ci_pipeline.nim",
      language: slNim,
      summary: "评估性能阈值、聚合 qlog 与 flamegraph 报告，触发报警策略。",
      stage: psPerf),
    ScriptDescriptor(
      path: "project/nimsquic.nimble",
      language: slNim,
      summary: "Nim blueprint 任务（build/test/lint），覆盖 sample/trace/fuzz 工具。",
      stage: psTest)]

  CiJobs*: seq[CiJob] = @[
    CiJob(
      name: "configure-and-build",
      stage: psBuild,
      scripts: @["scripts/prepare-machine.ps1", "scripts/build.ps1"],
      produces: {akDynamicLib, akStaticLib, akTools},
      dependsOn: @[],
      notes: "CI 首阶段完成依赖安装与矩阵构建，对应 CMakePresets.json 中的平台。"),
    CiJob(
      name: "unit-and-integration-tests",
      stage: psTest,
      scripts: @["scripts/test.ps1", "scripts/run-gtest.ps1"],
      produces: {akTests},
      dependsOn: @["configure-and-build"],
      notes: "运行 gtest、sample 集成与 sanity 测试，生成覆盖率输入。"),
    CiJob(
      name: "nim-tooling-validation",
      stage: psTest,
      scripts: @["project/nimsquic.nimble (build)", "project/nimsquic.nimble (test)"],
      produces: {akTools, akTests},
      dependsOn: @["configure-and-build"],
      notes: "执行 Nim 版 sample/trace/fuzz/interop 验证，补齐 C6 迁移任务。"),
    CiJob(
      name: "interop-and-fuzz",
      stage: psInterop,
      scripts: @["scripts/interop.ps1", "scripts/recvfuzz.ps1", "scripts/qns.Dockerfile"],
      produces: {akTests},
      dependsOn: @["configure-and-build"],
      notes: "执行互操作与模糊测试，依赖 docker 化环境。"),
    CiJob(
      name: "perf-benchmark",
      stage: psPerf,
      scripts: @["scripts/lola-perf.ps1", "scripts/emulated-performance.ps1"],
      produces: {akTests},
      dependsOn: @["configure-and-build"],
      notes: "在专用硬件节点运行性能基线，采集 lola 指标并生成 qlog/flamegraph 原始数据。"),
    CiJob(
      name: "perf-regression-watch",
      stage: psPerf,
      scripts: @["nim/tooling/perf_ci_pipeline.nim", "scripts/secnetperf.ps1"],
      produces: {akTests, akDocs},
      dependsOn: @["perf-benchmark"],
      notes: "聚合 netperf/qlog/flamegraph 数据，执行阈值对比并推送性能报告与报警。"),
    CiJob(
      name: "coverage-and-packaging",
      stage: psPackage,
      scripts: @["scripts/merge-coverage.ps1", "scripts/make-packages.sh", "scripts/package-build.sh"],
      produces: {akPackages, akDocs},
      dependsOn: @["unit-and-integration-tests"],
      notes: "合并覆盖率报告并触发发行包打包。"),
    CiJob(
      name: "release-and-docs",
      stage: psPublish,
      scripts: @["scripts/create-release.ps1", "scripts/update-docfx-site.ps1", "scripts/update-version.ps1"],
      produces: {akPackages, akDocs},
      dependsOn: @["coverage-and-packaging"],
      notes: "推送 release、更新版本与文档站点，输出最终制品。")]
