## 跨平台发布验证矩阵，对应 E4 阶段的兼容性测试与打包清单。

import std/strformat
import std/strutils

import ./platform_matrix
import ../tooling/common
import ../tooling/build_matrix
import ../tooling/ci_pipeline

type
  PlatformArch* = enum
    paX64
    paArm64

  ReleaseChannel* = enum
    rcNightly
    rcPreview
    rcStable

  GateExecution* = object
    jobName*: string
    stage*: PipelineStage
    scripts*: seq[string]
    expectedArtifacts*: set[ArtifactKind]
    required*: bool

  PackagingRecipe* = object
    name*: string
    scripts*: seq[string]
    artifacts*: set[ArtifactKind]
    installNotes*: seq[string]

  PlatformReleaseScenario* = object
    capability*: PlatformCapability
    arch*: PlatformArch
    buildProfile*: string
    azureQueue*: string
    channel*: ReleaseChannel
    gates*: seq[GateExecution]
    packaging*: PackagingRecipe
    notes*: seq[string]

  PlatformReleaseMatrix* = object
    pipelineFile*: string
    scenarios*: seq[PlatformReleaseScenario]
    documentation*: seq[string]

proc lookupCiJob(jobName: string): CiJob =
  for job in CiJobs:
    if job.name == jobName:
      return job
  raise newException(ValueError, "未找到 CI 作业: " & jobName)

proc gateFromCi(jobName: string; required: bool = true): GateExecution =
  let job = lookupCiJob(jobName)
  GateExecution(
    jobName: job.name,
    stage: job.stage,
    scripts: job.scripts,
    expectedArtifacts: job.produces,
    required: required)

proc fetchCapability(os: PlatformOs): PlatformCapability =
  PlatformMatrix[os]

proc findBuildProfile(name: string; profile: var BuildProfile): bool =
  for item in BuildProfiles:
    if item.name == name:
      profile = item
      return true
  false

proc scenarioIsConsistent*(scenario: PlatformReleaseScenario): bool =
  var profile: BuildProfile
  if not findBuildProfile(scenario.buildProfile, profile):
    return false
  if scenario.packaging.name.len == 0 or scenario.packaging.scripts.len == 0:
    return false
  if scenario.packaging.installNotes.len == 0:
    return false
  for note in scenario.packaging.installNotes:
    if note.len == 0:
      return false
  if scenario.gates.len == 0:
    return false
  let artifactIntersection = scenario.packaging.artifacts * profile.artifacts
  if artifactIntersection == {}:
    return false
  for gate in scenario.gates:
    if gate.required and gate.scripts.len == 0:
      return false
  scenario.azureQueue.len > 0

proc defaultPlatformReleaseMatrix*(): PlatformReleaseMatrix =
  let windowsScenario = PlatformReleaseScenario(
    capability: fetchCapability(posWindowsUser),
    arch: paX64,
    buildProfile: "windows-debug-schannel",
    azureQueue: "windows-x64",
    channel: rcStable,
    gates: @[
      gateFromCi("configure-and-build"),
      gateFromCi("unit-and-integration-tests"),
      gateFromCi("nim-tooling-validation"),
      gateFromCi("coverage-and-packaging"),
      gateFromCi("release-and-docs")
    ],
    packaging: PackagingRecipe(
      name: "msquic-win64-msi",
      scripts: @[
        "scripts/package-nuget.ps1",
        "scripts/create-release.ps1",
        "scripts/validate-package-commits.ps1"
      ],
      artifacts: {akPackages, akDynamicLib, akHeaders},
      installNotes: @[
        "使用 `msiexec /i msquic-x64.msi /qn` 静默安装。",
        "NuGet 包 `Microsoft.Quic` 可通过 `dotnet add package Microsoft.Quic --version <tag>` 更新。"
      ]
    ),
    notes: @[
      "覆盖 WinUser datapath 与 Schannel TLS 适配。",
      "复用 `.azure/OneBranch.Official.yml` 的 Windows 资源池。"
    ])

  let linuxScenario = PlatformReleaseScenario(
    capability: fetchCapability(posLinux),
    arch: paX64,
    buildProfile: "linux-debug-quictls",
    azureQueue: "linux-x64",
    channel: rcStable,
    gates: @[
      gateFromCi("configure-and-build"),
      gateFromCi("unit-and-integration-tests"),
      gateFromCi("interop-and-fuzz"),
      gateFromCi("perf-regression-watch"),
      gateFromCi("coverage-and-packaging")
    ],
    packaging: PackagingRecipe(
      name: "msquic-linux-x64",
      scripts: @[
        "scripts/make-packages.sh",
        "scripts/package-build.sh",
        "scripts/upload-linux-packages.sh"
      ],
      artifacts: {akPackages, akDynamicLib, akStaticLib},
      installNotes: @[
        "安装 `deb` 包示例：`sudo dpkg -i msquic_x64.deb`。",
        "`libmsquic.so` 附带 `pkg-config` 描述，执行 `pkg-config --libs msquic` 校验。"
      ]
    ),
    notes: @[
      "验证 epoll datapath 与 quictls 集成。",
      "互操作任务复用 `scripts/qns.Dockerfile`。"
    ])

  let freebsdScenario = PlatformReleaseScenario(
    capability: fetchCapability(posFreeBsd),
    arch: paX64,
    buildProfile: "freebsd-kqueue-release",
    azureQueue: "freebsd-x64",
    channel: rcPreview,
    gates: @[
      gateFromCi("configure-and-build"),
      gateFromCi("nim-tooling-validation", required = false),
      gateFromCi("coverage-and-packaging")
    ],
    packaging: PackagingRecipe(
      name: "msquic-freebsd",
      scripts: @[
        "scripts/make-packages.sh",
        "scripts/package-distribution.ps1"
      ],
      artifacts: {akPackages, akDynamicLib},
      installNotes: @[
        "使用 `sudo pkg add msquic-freebsd.txz` 安装，并执行 `service msquicd restart` 验证。",
        "包含 `/usr/local/lib/libmsquic.so` 与 `/usr/local/include/quic`。"
      ]
    ),
    notes: @[
      "kqueue datapath 覆盖基础互操作场景，性能基线后续补齐。",
      "预览渠道通过手动审批发布。"
    ])

  let macScenario = PlatformReleaseScenario(
    capability: fetchCapability(posMacos),
    arch: paArm64,
    buildProfile: "darwin-xcframework",
    azureQueue: "macos-arm64",
    channel: rcPreview,
    gates: @[
      gateFromCi("configure-and-build"),
      gateFromCi("nim-tooling-validation"),
      gateFromCi("coverage-and-packaging")
    ],
    packaging: PackagingRecipe(
      name: "msquic-darwin-xcframework",
      scripts: @[
        "scripts/package-darwin-framework.ps1",
        "scripts/package-darwin-xcframework.ps1",
        "scripts/package-distribution.ps1"
      ],
      artifacts: {akPackages, akHeaders},
      installNotes: @[
        "执行 `pwsh -File scripts/package-darwin-framework.ps1` 生成 `.framework`，随后聚合为 `msquic.xcframework`。",
        "SwiftPM 集成示例：在 `Package.swift` 中添加 `.binaryTarget(name: \"MsQuic\", path: \"Frameworks/msquic.xcframework\")`。"
      ]
    ),
    notes: @[
      "覆盖 macOS kqueue datapath 与 Apple 平台证书链。",
      "arm64/x64 双架构通过 XCFramework 聚合。"
    ])

  PlatformReleaseMatrix(
    pipelineFile: ".azure/OneBranch.Official.yml",
    scenarios: @[
      windowsScenario,
      linuxScenario,
      freebsdScenario,
      macScenario
    ],
    documentation: @[
      "docs/Deployment.md",
      "docs/BUILD.md",
      "docs/TEST.md"
    ])

proc releaseChecklist*(matrix: PlatformReleaseMatrix): seq[string] =
  result = @[]
  for scenario in matrix.scenarios:
    var gateNames: seq[string] = @[]
    for gate in scenario.gates:
      gateNames.add(gate.jobName)
    let gateList = gateNames.join(", ")
    result.add fmt"{scenario.capability.os}/{scenario.arch} -> {scenario.azureQueue} ({gateList})"
