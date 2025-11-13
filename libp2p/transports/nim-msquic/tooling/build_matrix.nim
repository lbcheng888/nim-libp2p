## 构建与产物矩阵，映射 `CMakePresets.json` 与 `scripts/*`。

import ./common

const
  BuildScripts*: seq[ScriptDescriptor] = @[
    ScriptDescriptor(
      path: "scripts/build.ps1",
      language: slPowerShell,
      summary: "统一入口，调用 CMake/Ninja 构建 MsQuic 二进制与示例。",
      stage: psBuild),
    ScriptDescriptor(
      path: "scripts/build.rs",
      language: slRust,
      summary: "Rust 绑定构建脚本，生成 `msquic-sys` 所需定义。",
      stage: psBuild),
    ScriptDescriptor(
      path: "scripts/package-build.sh",
      language: slBash,
      summary: "Linux 包装构建产物并调用 `cmake/package` 目标。",
      stage: psPackage),
    ScriptDescriptor(
      path: "scripts/package-darwin-framework.ps1",
      language: slPowerShell,
      summary: "在 macOS 上生成框架与 XCFramework 包。",
      stage: psPackage),
    ScriptDescriptor(
      path: "scripts/make-packages.sh",
      language: slBash,
      summary: "调用 CPack/自定义脚本聚合发行包。",
      stage: psPackage),
    ScriptDescriptor(
      path: "scripts/create-release.ps1",
      language: slPowerShell,
      summary: "封装 release 流水线步骤，上传制品。",
      stage: psPublish)]

  BuildProfiles*: seq[BuildProfile] = @[
    BuildProfile(
      name: "windows-debug-schannel",
      description: "Windows x64 Debug 构建，使用 Schannel TLS 后端。",
      cmakePreset: "windows-schannel-debug",
      toolchains: {btCmake, btNinja, btMsvc},
      primaryScripts: @["scripts/build.ps1"],
      artifacts: {akDynamicLib, akStaticLib, akTools, akTests}),
    BuildProfile(
      name: "linux-debug-quictls",
      description: "Linux x64 Debug 构建，使用 quictls 后端。",
      cmakePreset: "linux-quictls-debug",
      toolchains: {btCmake, btNinja, btClang},
      primaryScripts: @["scripts/build.ps1", "scripts/make-packages.sh"],
      artifacts: {akDynamicLib, akStaticLib, akTools, akTests}),
    BuildProfile(
      name: "freebsd-kqueue-release",
      description: "FreeBSD x64 Release 构建，使用 kqueue datapath。",
      cmakePreset: "freebsd-release",
      toolchains: {btCmake, btNinja, btClang},
      primaryScripts: @["scripts/build.ps1", "scripts/make-packages.sh"],
      artifacts: {akDynamicLib, akPackages, akHeaders}),
    BuildProfile(
      name: "rust-msquic-sys",
      description: "Rust 绑定生成，使用 Cargo + build.rs 生成 FFI 头。",
      cmakePreset: "windows-schannel-debug",
      toolchains: {btCargo, btCmake},
      primaryScripts: @["scripts/build.rs"],
      artifacts: {akStaticLib, akHeaders}),
    BuildProfile(
      name: "darwin-xcframework",
      description: "macOS/iOS 框架打包，供 Apple 平台消费。",
      cmakePreset: "linux-quictls-debug",
      toolchains: {btCmake, btNinja},
      primaryScripts: @["scripts/package-darwin-framework.ps1"],
      artifacts: {akPackages, akDynamicLib, akHeaders})]
