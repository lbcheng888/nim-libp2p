## 验证 E3 阶段的 TLS 安全审计聚合结果。

import unittest
import std/sequtils
import std/strutils

import ../tls/security_audit

suite "tls security audit (E3)":
  test "默认审计包含密钥生命周期告警与依赖策略":
    let report = runDefaultSecurityAudit()
    check report.overallStatus == astWarning
    let secretChecks = report.checks.filterIt(it.id == "TLS-SEC-002")
    check secretChecks.len == 1
    check secretChecks[0].status == astWarning
    check report.remediationPlan.anyIt("TLS-SEC-002" in it.relatedChecks)
    check report.dependencyPlan.anyIt(contains(it.component, "quictls"))

  test "渲染结果暴露剩余风险摘要":
    let report = runDefaultSecurityAudit()
    let rendered = renderAuditReport(report)
    check contains(rendered, "residual risks")
    check contains(rendered, "TLS-SEC-003")
