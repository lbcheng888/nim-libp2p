## 验证发布阶段（E5）：生成 Nim API 文档、迁移指南与示例项目资产。

import std/sequtils
import std/strformat
import std/strutils

import ./common
import ./api_surface_model
import ./event_model
import ../tooling/sample_tool

type
  ApiDocSection* = object
    title*: string
    lines*: seq[string]

  ApiDocBundle* = object
    reference*: string
    migrationGuide*: string
    sampleReadme*: string
    sampleSource*: string
    compatibility*: string

  SampleProjectAsset* = object
    path*: string
    contents*: string
    description*: string

  SampleCompatibilityReport* = object
    success*: bool
    clientSuccess*: bool
    serverSuccess*: bool
    datagramConsistent*: bool
    notes*: seq[string]

proc categoryName(cat: ApiFunctionCategory): string =
  case cat
  of afInfrastructure: "基础设施"
  of afUtility: "通用工具"
  of afParameters: "参数访问"
  of afRegistration: "注册管理"
  of afConfiguration: "配置管理"
  of afListener: "监听控制"
  of afConnection: "连接操作"
  of afStream: "数据流"
  of afDatagram: "数据报"
  of afPreviewOnly: "预览特性"

proc handleName(kind: QuicHandleKind): string =
  case kind
  of qhkApiTable: "apiTable"
  of qhkRegistration: "registration"
  of qhkConfiguration: "configuration"
  of qhkListener: "listener"
  of qhkConnection: "connection"
  of qhkStream: "stream"
  of qhkDatagram: "datagram"
  of qhkExecution: "execution"
  of qhkConnectionPool: "connectionPool"
  of qhkAny: "any"

proc escapeBars(text: string): string =
  text.replace("|", "\\|")

proc buildCategorySection(cat: ApiFunctionCategory;
                          funcs: seq[ApiFunctionSpec]): ApiDocSection =
  var lines: seq[string] = @[]
  if funcs.len == 0:
    return ApiDocSection(title: categoryName(cat), lines: @["（该类别暂无 Nim 暴露的公开函数。）"])
  lines.add "| Nim 符号 | C 符号 | 句柄 | 摘要 | 引入版本 |"
  lines.add "| --- | --- | --- | --- | --- |"
  for spec in funcs:
    lines.add fmt"| `{spec.nimSymbol}` | `{spec.cSymbol}` | {handleName(spec.primaryHandle)} | {escapeBars(spec.summary)} | {spec.availability.introduced} |"
  ApiDocSection(title: categoryName(cat), lines: lines)

proc buildEventSection(): ApiDocSection =
  var lines: seq[string] = @[]
  lines.add "| 事件 | 说明 |"
  lines.add "| --- | --- |"
  for kind in ConnectionEventKind:
    let description =
      case kind
      of ceConnected: "握手完成并协商 ALPN"
      of ceShutdownInitiated: "应用或对端发起关闭"
      of ceShutdownComplete: "连接资源已释放"
      of ceSettingsApplied: "MsQuic 设置已生效"
      of ceDatagramStateChanged: "数据报能力或配额变化"
      of ceParameterUpdated: "参数更新事件"
    lines.add fmt"| `{kind}` | {description} |"
  ApiDocSection(title: "连接事件", lines: lines)

proc buildParamSection(): ApiDocSection =
  let rows = @[
    ("QUIC_PARAM_CONN_SETTINGS", "连接级别参数包"),
    ("QUIC_PARAM_CONN_CLOSE_REASON_PHRASE", "关闭原因短语"),
    ("QUIC_PARAM_CONN_STREAM_SCHEDULING_SCHEME", "流调度策略"),
    ("QUIC_PARAM_CONN_DATAGRAM_RECEIVE_ENABLED", "是否允许接收数据报"),
    ("QUIC_PARAM_CONN_DATAGRAM_SEND_ENABLED", "是否允许发送数据报"),
    ("QUIC_PARAM_CONN_DISABLE_1RTT_ENCRYPTION", "禁用 1-RTT 加密（调试用途）")
  ]
  var lines: seq[string] = @[
    "| 参数 ID | 描述 |",
    "| --- | --- |"
  ]
  for row in rows:
    lines.add fmt"| `{row[0]}` | {row[1]} |"
  ApiDocSection(title: "常用 QUIC_PARAM", lines: lines)

proc buildApiReference*(): string =
  var sections: seq[ApiDocSection] = @[]
  for cat in ApiFunctionCategory:
    let funcs = MsQuicApiFunctionsV2.filterIt(it.category == cat)
    if funcs.len > 0:
      sections.add(buildCategorySection(cat, funcs))
  sections.add(buildEventSection())
  sections.add(buildParamSection())

  var lines: seq[string] = @[
    "# Nim MsQuic API 文档（E5）",
    "",
    "该文档由 `nim/api/release_docs.nim` 生成，覆盖 MsQuic 2.x 级别的 Nim API 表与事件模型。"
  ]
  for section in sections:
    lines.add ""
    lines.add "## " & section.title
    for line in section.lines:
      lines.add line
  lines.add ""
  lines.add "最后更新：验证发布阶段（E5），用于发布前 API 语义核查。"
  lines.join("\n")

proc buildMigrationGuide*(): string =
  let steps = @[
    ("步骤 1：加载 MsQuic API 表", "调用 `msquicOpenVersion` 并保存返回的函数表，确保与目标二进制版本匹配。"),
    ("步骤 2：初始化注册与配置", "使用 `registrationOpen`、`configurationOpen` 创建 Nim 侧资源，并保持与 C API 一致的关闭顺序。"),
    ("步骤 3：桥接回调与上下文", "通过 `setCallbackHandler` 将 Nim 回调函数包装为 C ABI；必要时搭配 `setHandleContext` 维护对象映射。"),
    ("步骤 4：参数与设置校验", "使用 `setParam` 与 `getParam` 映射 `QUIC_PARAM_*`，对关键配置（如数据报开关）进行互通测试。"),
    ("步骤 5：流与数据报语义验证", "对照 `stream*`、`datagramSend` 等 API，复用 `nim/tooling/sample_tool.nim` 检查握手、流量与数据报逻辑。"),
    ("步骤 6：关闭与资源回收", "调用 `connectionShutdown`、`streamClose`、`registrationClose`，确保 Nim 层析构顺序与 C 实现一致。")
  ]
  var lines: seq[string] = @[
    "# Nim MsQuic 迁移指南（E5）",
    "",
    "该指南帮助现有 C/C++ MsQuic 集成平滑迁移至 Nim 版本，覆盖主要资源生命周期与语义对齐要点。",
    ""
  ]
  for step in steps:
    lines.add fmt"- {step[0]}：{step[1]}"
  lines.add ""
  lines.add "配套示例位于 `nim/project/sample_echo`，展示握手与数据报场景的端到端迁移结果。"
  lines.join("\n")

const
  SampleReadme = """# Nim MsQuic 示例项目（E5）

该示例实现 E5 原子任务中要求的 API 演示与语义校验，聚焦以下目标：

- 调用 `nim/tooling/sample_tool.nim` 提供的最小握手流程，演示 Nim 版 API 的典型用法。
- 同时覆盖基础握手与启用数据报通道的场景，验证与 C 版 `src/tools/sample` 的语义一致性。
- 作为迁移指南的配套示例，展示如何在 Nim 工程中引入 `nim/api` 与 `nim/tooling` 模块。

## 运行方式

```bash
nim r --path:../../ nim/project/sample_echo/src/main.nim
```

运行结果会打印客户端/服务端握手是否成功，以及数据报配置是否同步；若任一检查失败将以非零返回码退出，便于在 CI 场景中集成。
"""

  SampleMain = """## Nim MsQuic E5 示例：验证 API 语义兼容与数据报能力。

import std/strformat

import ../../tooling/sample_tool

type
  SampleOutcome = object
    name: string
    success: bool
    datagramParity: bool

proc runScenario(enableDatagram: bool; peer: string): SampleOutcome =
  var clientConf = initSampleConfig(srClient, peer = peer & "-server")
  var serverConf = initSampleConfig(srServer, peer = peer & "-client")
  clientConf.enableDatagram = enableDatagram
  serverConf.enableDatagram = enableDatagram
  let clientRes = runSample(clientConf)
  let serverRes = runSample(serverConf)
  SampleOutcome(
    name: if enableDatagram: "datagram-enabled" else: "baseline",
    success: clientRes.success and serverRes.success,
    datagramParity: clientRes.datagramEnabled == serverRes.datagramEnabled)

proc report(outcome: SampleOutcome) =
  echo fmt"[{outcome.name}] success={outcome.success} datagramParity={outcome.datagramParity}"

proc verifySample(): bool =
  let baseline = runScenario(false, "nim-e5")
  report(baseline)
  if not baseline.success or not baseline.datagramParity:
    return false
  let datagram = runScenario(true, "nim-e5")
  report(datagram)
  baseline.success and datagram.success and datagram.datagramParity

when isMainModule:
  if verifySample():
    echo "E5 sample verification succeeded."
  else:
    echo "E5 sample verification failed."
    quit 1
"""

proc generateSampleProjectAssets*(): seq[SampleProjectAsset] =
  @[
    SampleProjectAsset(
      path: "nim/project/sample_echo/README.md",
      contents: SampleReadme,
      description: "运行说明与迁移要点"),
    SampleProjectAsset(
      path: "nim/project/sample_echo/src/main.nim",
      contents: SampleMain,
      description: "握手与数据报兼容性示例")
  ]

proc runSampleCompatibilityCheck*(enableDatagram: bool = true): SampleCompatibilityReport =
  var clientConf = initSampleConfig(srClient, peer = "nim-release-docs-server")
  var serverConf = initSampleConfig(srServer, peer = "nim-release-docs-client")
  clientConf.enableDatagram = enableDatagram
  serverConf.enableDatagram = enableDatagram
  let clientResult = runSample(clientConf)
  let serverResult = runSample(serverConf)
  SampleCompatibilityReport(
    success: clientResult.success and serverResult.success and
             (clientResult.datagramEnabled == serverResult.datagramEnabled),
    clientSuccess: clientResult.success,
    serverSuccess: serverResult.success,
    datagramConsistent: clientResult.datagramEnabled == serverResult.datagramEnabled,
    notes: @[
      fmt"client-success={clientResult.success}",
      fmt"server-success={serverResult.success}",
      fmt"client-datagram={clientResult.datagramEnabled}",
      fmt"server-datagram={serverResult.datagramEnabled}"])

proc buildReleaseBundle*(): ApiDocBundle =
  let reference = buildApiReference()
  let guide = buildMigrationGuide()
  let assets = generateSampleProjectAssets()
  let sampleReadme = assets[0].contents
  let sampleSource = assets[1].contents
  let compatibility = runSampleCompatibilityCheck().notes.join("\n")
  ApiDocBundle(
    reference: reference,
    migrationGuide: guide,
    sampleReadme: sampleReadme,
    sampleSource: sampleSource,
    compatibility: compatibility)
