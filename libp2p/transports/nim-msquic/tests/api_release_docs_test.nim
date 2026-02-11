## 验证 E5 阶段 API 文档与迁移指南生成逻辑。

import unittest
import std/sequtils
import std/strutils

import ../api/release_docs

suite "api release docs (E5)":
  test "API 文档包含关键函数与事件表":
    let doc = buildApiReference()
    check doc.contains("msquicOpenVersion")
    check doc.contains("connectionStart")
    check doc.contains("连接事件")
    check doc.contains("QUIC_PARAM_CONN_DATAGRAM_SEND_ENABLED")

  test "迁移指南覆盖六大步骤":
    let guide = buildMigrationGuide()
    for idx in 1..6:
      check guide.contains("步骤 " & $idx)
    check guide.contains("sample_echo")

  test "示例资产提供 README 与主程序":
    let assets = generateSampleProjectAssets()
    check assets.len == 2
    let readme = assets.filterIt(it.path.endsWith("README.md"))
    check readme.len == 1
    check readme[0].contents.contains("E5 原子任务")
    let main = assets.filterIt(it.path.endsWith("main.nim"))
    check main.len == 1
    check main[0].contents.contains("runScenario")

  test "兼容性检查默认通过":
    let report = runSampleCompatibilityCheck()
    check report.success
    check report.clientSuccess
    check report.serverSuccess
    check report.datagramConsistent
