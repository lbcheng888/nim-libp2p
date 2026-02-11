## E6 发布流水线蓝本：覆盖版本标记、许可证审查与合规材料。

import std/sequtils
import std/strutils

import ./common
import ./ci_pipeline

type
  ApprovalKind* = enum
    ## 发布前需要的审批类型。
    akReleaseManagement
    akSecurity
    akLegal

  ManualApproval* = object
    ## 描述一次人工审批及动作提示。
    name*: string
    owners*: seq[string]
    instructions*: string
    kind*: ApprovalKind

  VersionBumpAction* = object
    ## 版本更新脚本及作用范围。
    script*: string
    targets*: seq[string]
    description*: string

  VersioningPolicy* = object
    tagFormat*: string
    releaseBranchPattern*: string
    previewBranchPattern*: string
    actions*: seq[VersionBumpAction]
    verificationScripts*: seq[string]
    notes*: seq[string]

  LicenseCheck* = object
    component*: string
    license*: string
    notice*: string
    scanScripts*: seq[string]
    approvedBy*: seq[string]
    requiresArchive*: bool

  ComplianceArtifact* = object
    name*: string
    path*: string
    stage*: PipelineStage
    owner*: string
    generatedBy*: string
    description*: string

  ReleaseGate* = object
    job*: CiJob
    mandatory*: bool
    approvals*: seq[ManualApproval]
    description*: string

  ReleaseWorkflow* = object
    pipelineFile*: string
    gates*: seq[ReleaseGate]
    versioning*: VersioningPolicy
    licenseChecks*: seq[LicenseCheck]
    complianceArtifacts*: seq[ComplianceArtifact]
    auditTrail*: seq[string]

proc findCiJob(name: string): CiJob =
  for job in CiJobs:
    if job.name == name:
      return job
  raise newException(ValueError, "未找到 CI 作业: " & name)

proc manualApprovalIsValid*(approval: ManualApproval): bool =
  approval.name.len > 0 and
  approval.owners.len > 0 and
  approval.instructions.len > 0

proc gateIsValid*(gate: ReleaseGate): bool =
  gate.job.name.len > 0 and
  gate.description.len > 0 and
  (not gate.mandatory or gate.job.scripts.len > 0) and
  (gate.approvals.allIt(manualApprovalIsValid(it)) or gate.approvals.len == 0)

proc renderTag*(policy: VersioningPolicy; major, minor, patch: int; preview: bool = false): string =
  var tag = policy.tagFormat
  tag = tag.replace("{major}", $major)
  tag = tag.replace("{minor}", $minor)
  tag = tag.replace("{patch}", $patch)
  if preview:
    tag.add("-preview")
  tag

proc workflowIsConsistent*(workflow: ReleaseWorkflow): bool =
  if workflow.pipelineFile.len == 0 or not workflow.pipelineFile.endsWith(".yml"):
    return false
  if workflow.gates.len == 0 or workflow.versioning.actions.len == 0:
    return false
  var lastStage = psConfigure
  for gate in workflow.gates:
    if not gateIsValid(gate):
      return false
    if gate.job.stage < lastStage:
      return false
    lastStage = gate.job.stage
    if gate.job.stage == psPublish and gate.approvals.len == 0:
      return false
  if workflow.licenseChecks.len == 0 or workflow.complianceArtifacts.len == 0:
    return false
  for check in workflow.licenseChecks:
    if check.notice.len == 0 or check.component.len == 0:
      return false
    if check.requiresArchive and not check.notice.endsWith("NOTICES"):
      return false
  for artifact in workflow.complianceArtifacts:
    if artifact.path.len == 0 or artifact.name.len == 0 or artifact.generatedBy.len == 0:
      return false
  workflow.auditTrail.len > 0

proc licenseNoticePaths*(workflow: ReleaseWorkflow): seq[string] =
  workflow.licenseChecks.mapIt(it.notice)

proc renderReleaseChecklist*(workflow: ReleaseWorkflow): seq[string] =
  var lines: seq[string]
  for gate in workflow.gates:
    lines.add(
      gate.job.name & " -> " & $gate.job.stage & " (" &
      gate.job.scripts.join(", ") & ")")
    if gate.approvals.len > 0:
      let owners = gate.approvals.mapIt(it.owners.join("/")).join(", ")
      lines.add("  approvals: " & owners)
  for action in workflow.versioning.actions:
    lines.add("version: " & action.script & " => " & action.targets.join(", "))
  for artifact in workflow.complianceArtifacts:
    lines.add("artifact: " & artifact.name & " @ " & artifact.path)
  lines

proc releaseWorkflowBlueprint*(): ReleaseWorkflow =
  let releaseManagers = ManualApproval(
    name: "Release 管理复核",
    owners: @["MsQuic Release Managers"],
    instructions: "核对 docs/Release.md 与 docs/Versions.md 的版本信息，同时确认 OneBranch.Publish.yml 中的发布分支。",
    kind: akReleaseManagement)
  let securityReview = ManualApproval(
    name: "安全基线确认",
    owners: @["Security PM", "CredScan Owners"],
    instructions: "查看 docs/CredScan.md 和 scripts/create-release.ps1 输出的 CredScan 报告，确保无阻断项。",
    kind: akSecurity)
  let legalApproval = ManualApproval(
    name: "许可证合规确认",
    owners: @["MsQuic Legal"],
    instructions: "审阅 THIRD-PARTY-NOTICES 与 docs/Release.md 的许可证章节，确保第三方组件列表完整。",
    kind: akLegal)

  let packagingGate = ReleaseGate(
    job: findCiJob("coverage-and-packaging"),
    mandatory: true,
    approvals: @[],
    description: "聚合覆盖率并输出多平台包，作为发布候选输入。")
  let releaseGate = ReleaseGate(
    job: findCiJob("release-and-docs"),
    mandatory: true,
    approvals: @[releaseManagers, securityReview, legalApproval],
    description: "执行版本更新、生成 release notes，并触发文档站点刷新。")

  let versionPolicy = VersioningPolicy(
    tagFormat: "v{major}.{minor}.{patch}",
    releaseBranchPattern: "release/*",
    previewBranchPattern: "main",
    actions: @[
      VersionBumpAction(
        script: "scripts/update-version.ps1",
        targets: @["version.json", "project/nimsquic.nimble"],
        description: "同步核心版本号与 Nim 包版本。"),
      VersionBumpAction(
        script: "scripts/create-release.ps1",
        targets: @["docs/Release.md", "docs/Versions.md"],
        description: "生成 release notes 并推送 Git tag。"),
      VersionBumpAction(
        script: "scripts/write-versions.ps1",
        targets: @["docs/Deployment.md", "docs/Release.md"],
        description: "更新部署矩阵与支持窗口描述。")],
    verificationScripts: @[
      "scripts/validate-package-commits.ps1",
      "scripts/sign.ps1",
      "scripts/update-docfx-site.ps1"],
    notes: @[
      "预发布分支继承 main，Tag 添加 -preview 后缀。",
      "正式发布要求 OneBranch.Publish.yml 完成全矩阵通过。"])

  let licenseChecks = @[
    LicenseCheck(
      component: "OpenSSL / quictls",
      license: "Apache-2.0 WITH OpenSSL-exception",
      notice: "THIRD-PARTY-NOTICES",
      scanScripts: @["scripts/create-release.ps1", "scripts/package-distribution.ps1"],
      approvedBy: @["MsQuic Legal"],
      requiresArchive: true),
    LicenseCheck(
      component: "Nim 工具链补充脚本",
      license: "MIT",
      notice: "LICENSE",
      scanScripts: @["scripts/create-release.ps1"],
      approvedBy: @["MsQuic Release Managers"],
      requiresArchive: false)]

  let complianceArtifacts = @[
    ComplianceArtifact(
      name: "CredScan 报告",
      path: "docs/CredScan.md",
      stage: psTest,
      owner: "Security PM",
      generatedBy: "scripts/create-release.ps1",
      description: "列出凭据扫描结果与豁免记录。"),
    ComplianceArtifact(
      name: "FIPS 状态",
      path: "docs/FIPS.md",
      stage: psPublish,
      owner: "Security PM",
      generatedBy: "scripts/create-release.ps1",
      description: "记录 FIPS 兼容性与验证步骤。"),
    ComplianceArtifact(
      name: "发布指南",
      path: "docs/Deployment.md",
      stage: psDoc,
      owner: "Release Docs",
      generatedBy: "scripts/update-docfx-site.ps1",
      description: "包含安装流程、平台矩阵与回滚策略。"),
    ComplianceArtifact(
      name: "版本支持矩阵",
      path: "docs/Versions.md",
      stage: psPublish,
      owner: "Release Docs",
      generatedBy: "scripts/write-versions.ps1",
      description: "维护支持周期、分支与渠道信息。")]

  ReleaseWorkflow(
    pipelineFile: ".azure/OneBranch.Publish.yml",
    gates: @[packagingGate, releaseGate],
    versioning: versionPolicy,
    licenseChecks: licenseChecks,
    complianceArtifacts: complianceArtifacts,
    auditTrail: @[
      "生成 Git tag 并附带 release notes。",
      "归档 THIRD-PARTY-NOTICES 与 CredScan 报告。",
      "更新 DocFX 输出并刷新公开文档站点。"])
