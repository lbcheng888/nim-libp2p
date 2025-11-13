## 验证 E6 发布流水线蓝图的完整性。

import unittest
import std/sets
import std/sequtils
import std/strutils

import ../tooling/[release_pipeline, common]

suite "release pipeline blueprint (E6)":
  test "工作流配置满足一致性约束":
    let workflow = releaseWorkflowBlueprint()
    check workflowIsConsistent(workflow)
    check workflow.gates.len == 2
    check workflow.gates[0].job.stage == psPackage
    check workflow.gates[1].job.stage == psPublish
    check workflow.gates[1].approvals.len >= 2

  test "标签渲染支持正式与预览版本":
    let policy = releaseWorkflowBlueprint().versioning
    check renderTag(policy, 2, 6, 0) == "v2.6.0"
    check renderTag(policy, 2, 6, 0, preview = true) == "v2.6.0-preview"

  test "许可证审查覆盖第三方通知":
    let notices = releaseWorkflowBlueprint().licenseNoticePaths().toHashSet()
    check notices.contains("THIRD-PARTY-NOTICES")
    check notices.contains("LICENSE")

  test "发布清单列出审批与工件":
    let lines = renderReleaseChecklist(releaseWorkflowBlueprint())
    check lines.len > 0
    check lines.anyIt(it.contains("release-and-docs"))
    check lines.anyIt(it.contains("version: scripts/update-version.ps1"))
