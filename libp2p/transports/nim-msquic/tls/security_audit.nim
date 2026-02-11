## 验证发布（E3）：TLS 安全审计、内存/线程检查与依赖维护策略。

import std/sets
import std/strformat
import std/strutils

type
  AuditSeverity* = enum
    aseInfo
    aseLow
    aseMedium
    aseHigh
    aseCritical

  AuditStatus* = enum
    astPassed
    astWarning
    astFailed

  SecurityCheck* = object
    id*: string
    area*: string
    title*: string
    severity*: AuditSeverity
    status*: AuditStatus
    evidence*: seq[string]
    recommendation*: string

  MemorySafetyInsight* = object
    symbol*: string
    safe*: bool
    note*: string

  ThreadSafetyInsight* = object
    resource*: string
    safe*: bool
    detail*: string

  RemediationItem* = object
    id*: string
    relatedChecks*: seq[string]
    severity*: AuditSeverity
    summary*: string
    fixWindowDays*: int
    owner*: string
    tasks*: seq[string]

  DependencyUpgradeAction* = object
    component*: string
    currentVersion*: string
    targetVersion*: string
    cadenceWeeks*: int
    leadTimeDays*: int
    steps*: seq[string]
    notes*: seq[string]

  SecurityAuditReport* = object
    scope*: string
    checks*: seq[SecurityCheck]
    memoryInsights*: seq[MemorySafetyInsight]
    threadInsights*: seq[ThreadSafetyInsight]
    remediationPlan*: seq[RemediationItem]
    dependencyPlan*: seq[DependencyUpgradeAction]
    residualRisks*: seq[string]
    overallStatus*: AuditStatus

proc statusLabel(status: AuditStatus): string =
  case status
  of astPassed: "passed"
  of astWarning: "warning"
  of astFailed: "failed"

proc severityLabel(severity: AuditSeverity): string =
  case severity
  of aseInfo: "info"
  of aseLow: "low"
  of aseMedium: "medium"
  of aseHigh: "high"
  of aseCritical: "critical"

proc aggregateStatus(checks: openArray[SecurityCheck]): AuditStatus =
  var result = astPassed
  for check in checks:
    case check.status
    of astFailed:
      return astFailed
    of astWarning:
      if result == astPassed:
        result = astWarning
    of astPassed:
      discard
  result

proc collectResidualRisks(checks: openArray[SecurityCheck];
    remediation: openArray[RemediationItem]): seq[string] =
  var mapped = initHashSet[string]()
  for item in remediation:
    for id in item.relatedChecks:
      mapped.incl id
  for check in checks:
    if check.status != astPassed:
      var line = fmt"{check.id} {check.title}: {check.recommendation}"
      if mapped.contains(check.id):
        line &= " (remediation scheduled)"
      result.add(line)

proc defaultSecurityChecks*(): seq[SecurityCheck] =
  @[
    SecurityCheck(
      id: "TLS-SEC-001",
      area: "协议面",
      title: "限定 TLS 1.3 握手",
      severity: aseLow,
      status: astPassed,
      evidence: @[
        "nim/tls/openssl_adapter.nim:608-610 强制最小/最大协议版本为 TLS1.3",
        "nim/tls/openssl_adapter.nim:611 关闭历史票据以避免遗留配置"
      ],
      recommendation: "保持 TLS1.3-only 策略，并在 OpenSSL 官方 QUIC 支持可用时复核配置"),
    SecurityCheck(
      id: "TLS-SEC-002",
      area: "密钥生命周期",
      title: "握手密钥显式清理",
      severity: aseHigh,
      status: astWarning,
      evidence: @[
        "nim/tls/openssl_adapter.nim:700-708 将 QUIC 密钥复制到 pendingSecrets",
        "nim/tls/openssl_adapter.nim:735-736 重置序列但未执行显式内存擦除"
      ],
      recommendation: "为 pendingSecrets/pendingSecretViews 引入安全擦除流程并增加单元测试覆盖"),
    SecurityCheck(
      id: "TLS-SEC-003",
      area: "会话管理",
      title: "共享 Session Cache 并发控制",
      severity: aseMedium,
      status: astWarning,
      evidence: @[
        "nim/tls/openssl_adapter.nim:104-133 使用全局 Table 管理 quictls 会话票据",
        "缺少互斥机制会在多线程握手/销毁时留下竞争窗口"
      ],
      recommendation: "为 gSessionCache 引入细粒度锁或线程局部替代方案"),
    SecurityCheck(
      id: "TLS-SEC-004",
      area: "证书与密钥轮换",
      title: "运行期证书更新",
      severity: aseMedium,
      status: astPassed,
      evidence: @[
        "nim/tls/openssl_adapter.nim:842-849 允许服务器上下文热更新证书/私钥",
        "nim/tls/openssl_adapter.nim:820-825 支持 QUIC Key Update，确保长连安全"
      ],
      recommendation: "在 E4 阶段补充证书轮换的端到端演练")
  ]

proc defaultMemoryInsights*(): seq[MemorySafetyInsight] =
  @[
    MemorySafetyInsight(
      symbol: "TlsPendingSecret.material",
      safe: false,
      note: "握手密钥通过 seq[uint8] 存放（nim/tls/openssl_adapter.nim:370-381），释放时仅缩短长度，需显式清零"),
    MemorySafetyInsight(
      symbol: "TlsSecretView.dataPtr",
      safe: true,
      note: "采用只读视图暴露 quictls buffer（nim/tls/openssl_adapter.nim:411-419），避免 Nim 端的额外拷贝")
  ]

proc defaultThreadInsights*(): seq[ThreadSafetyInsight] =
  @[
    ThreadSafetyInsight(
      resource: "OpenSSL 句柄加载",
      safe: true,
      detail: "libSsl/libCrypto/sslInitialized 使用 threadvar 隔离（nim/tls/openssl_adapter.nim:97-100），避免跨线程重入"),
    ThreadSafetyInsight(
      resource: "共享 Session Cache",
      safe: false,
      detail: "gSessionCache 为无锁 Table，fetch/store/clear 操作可能被多个握手线程并发访问（nim/tls/openssl_adapter.nim:104-133）")
  ]

proc defaultRemediationPlan(checks: seq[SecurityCheck]): seq[RemediationItem] =
  var items: seq[RemediationItem] = @[]
  for check in checks:
    if check.status == astPassed:
      continue
    case check.id
    of "TLS-SEC-002":
      items.add RemediationItem(
        id: "TLS-R1",
        relatedChecks: @[check.id],
        severity: check.severity,
        summary: "握手密钥显式擦除",
        fixWindowDays: 7,
        owner: "tls-core",
        tasks: @[
          "为 pendingSecrets/pendingSecretViews 引入 secureZeroMem 助手",
          "在 processHandshake/destroy 结束前调用擦除逻辑，并覆盖所有错误分支",
          "扩展 tls_handshake_test 验证密钥擦除，确保无残留数据"
        ])
    of "TLS-SEC-003":
      items.add RemediationItem(
        id: "TLS-R2",
        relatedChecks: @[check.id],
        severity: check.severity,
        summary: "Session Cache 并发防护",
        fixWindowDays: 10,
        owner: "tls-runtime",
        tasks: @[
          "以 AsyncFence 或读写锁保护 gSessionCache 操作",
          "评估线程局部缓存方案并在高并发压力测试中验证",
          "补充竞态回归测试，覆盖 resumption 票据过期与清理场景"
        ])
    else:
      items.add RemediationItem(
        id: fmt"{check.id}-followup",
        relatedChecks: @[check.id],
        severity: check.severity,
        summary: check.recommendation,
        fixWindowDays: 14,
        owner: "tls-core",
        tasks: @["跟踪审计建议并在下一阶段复核"])
  items

proc defaultDependencyPlan*(): seq[DependencyUpgradeAction] =
  @[
    DependencyUpgradeAction(
      component: "quictls (OpenSSL QUIC fork)",
      currentVersion: "submodules/quictls @ msquic baseline",
      targetVersion: "对齐上游 quictls 最新安全标签并准备切换至 OpenSSL 官方 QUIC 支持",
      cadenceWeeks: 6,
      leadTimeDays: 3,
      steps: @[
        "每两周跟进 quictls 安全公告并同步至 internal security alias",
        "在候选更新上运行 tls_handshake_test、interop 与 fuzz 三套回归",
        "冻结发布前生成 SBOM 并记录 OpenSSL/CVE 覆盖情况"
      ],
      notes: @[
        "重点关注 TLS CVE（如证书链验证、HKDF 漏洞），确保 72 小时内补丁合入",
        "跟进 OpenSSL#22643 官方 QUIC 支持进展，准备最晚在 E5 评审时给出切换窗口"
      ]),
    DependencyUpgradeAction(
      component: "系统 OpenSSL 后备路径",
      currentVersion: "依赖平台预装 OpenSSL 3.0.x/3.1.x",
      targetVersion: "统一到 3.2 LTS 以上版本并关闭 QUIC 禁用补丁",
      cadenceWeeks: 12,
      leadTimeDays: 7,
      steps: @[
        "为 linux/macOS 包构建添加 openssl --version 采集与最小版本校验",
        "在 CI 中引入 docker 镜像扫描，防止老版本 OpenSSL 混入发布产物",
        "准备配置切换脚本，便于回退到 quictls 方案"
      ],
      notes: @[
        "当平台提供 QUIC-ready OpenSSL 时，需要验证 tls_handshake_test 与 interop 结果一致",
        "维护发布说明，指导使用者在无法升级系统 OpenSSL 时选择 quictls 包"
      ])
  ]

proc runDefaultSecurityAudit*(): SecurityAuditReport =
  let checks = defaultSecurityChecks()
  let memory = defaultMemoryInsights()
  let threads = defaultThreadInsights()
  let remediation = defaultRemediationPlan(checks)
  let dependencies = defaultDependencyPlan()
  SecurityAuditReport(
    scope: "E3 TLS 安全审计",
    checks: checks,
    memoryInsights: memory,
    threadInsights: threads,
    remediationPlan: remediation,
    dependencyPlan: dependencies,
    residualRisks: collectResidualRisks(checks, remediation),
    overallStatus: aggregateStatus(checks)
  )

proc renderAuditReport*(report: SecurityAuditReport;
    includePlans: bool = true): string =
  var lines: seq[string] = @[
    fmt"{report.scope}: status={statusLabel(report.overallStatus)}",
    "checks:"]
  for check in report.checks:
    lines.add fmt"  - {check.id} ({check.area}) {check.title}: {statusLabel(check.status)} severity={severityLabel(check.severity)}"
    if check.evidence.len > 0:
      lines.add "    evidence => " & check.evidence.join(" | ")
    if check.status != astPassed:
      lines.add "    recommendation => " & check.recommendation
  lines.add("memory insights:")
  for insight in report.memoryInsights:
    let state = if insight.safe: "safe" else: "risk"
    lines.add fmt"  - {insight.symbol}: {state} ({insight.note})"
  lines.add("thread insights:")
  for insight in report.threadInsights:
    let state = if insight.safe: "safe" else: "risk"
    lines.add fmt"  - {insight.resource}: {state} ({insight.detail})"
  if includePlans:
    lines.add("remediation plan:")
    for item in report.remediationPlan:
      lines.add fmt"  - {item.id} owner={item.owner} window={item.fixWindowDays}d severity={severityLabel(item.severity)}"
      lines.add "    tasks => " & item.tasks.join(" | ")
    lines.add("dependency plan:")
    for action in report.dependencyPlan:
      lines.add fmt"  - {action.component}: cadence={action.cadenceWeeks}w lead={action.leadTimeDays}d"
      lines.add "    current => " & action.currentVersion
      lines.add "    target => " & action.targetVersion
      lines.add "    steps => " & action.steps.join(" | ")
      if action.notes.len > 0:
        lines.add "    notes => " & action.notes.join(" | ")
  if report.residualRisks.len > 0:
    lines.add("residual risks:")
    for risk in report.residualRisks:
      lines.add "  - " & risk
  lines.join("\n")
