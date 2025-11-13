# 资源与带宽指标导出

`libp2p/metrics_exporter.nim` 为 Resource Manager 与 BandwidthManager 补齐了面向 Prometheus / OpenTelemetry 的通用指标。模块提供以下核心能力：

- `initSwitchMetricsState` / `updateResourceMetrics` / `updateBandwidthMetrics` / `updateSwitchMetrics`：使用 `SwitchMetricsState` 缓存资源、共享池与带宽标签，分别处理一次性快照与周期刷新；
- `updateWebtransportMetrics`：在启用 QUIC/WebTransport 时汇总当前及历史 certhash，记录会话并发上限，输出证书轮换间隔与保留历史窗口，并追加最近一次成功轮换的时间戳，同时统计握手拒绝原因（`libp2p_webtransport_rejections_total`）；
- `initBitswapMetricsState` / `updateBitswapMetrics`：使用 `BitswapMetricsState` 缓存历史标签，异步聚合 `BitswapLedger`，统计流量、HAVE / DONT_HAVE、债务比例，并额外导出前 5 名债务节点的债务比与累计字节；若启用了 `AutoProviderBlockStore`，会同步导出 provider 广告尝试、成功 / 失败次数与最近成功时间戳；
- `updateGraphSyncMetrics`：汇总 `GraphSyncService` 请求响应统计，输出请求完成情况、区块/字节累计与缺失链路计数；
- `updateDataTransferMetrics`：异步统计 `DataTransferChannelManager` 中的通道状态、方向及暂停情况，便于观测 go-data-transfer 互操作时的通道生命周期；
- `updateIpnsMetrics`：汇总一个或多个 `KadDHT` 的 IPNS 记录刷新情况，输出当前追踪 key 数、回退窗口与成功 / 失败次数；
- `spawnMetricsLoop`：按设定的 `Duration` 周期采集，并返回可取消的 `Future[void]`，可选地接入 IPNS 追踪回调。
- `OtelMetricsService.registerIpnsTracker`：在 OTLP/HTTP 推送模式下注册 `KadDHT`，自动将 IPNS 刷新指标封装进 OpenTelemetry 数据点。

## 快速集成

```nim
import chronos
import libp2p

let sw = SwitchBuilder.new()
  .withMetricsExporter("0.0.0.0", Port(8000), 5.seconds)
  .build()

await sw.start()

# ...应用逻辑...

await sw.stop()
```

`withMetricsExporter` 会自动挂载 `MetricsService`：服务启动时立即采集一次资源 / 带宽快照、后台周期性刷新，并在 `http://<address>:<port>/metrics` 与 `/health` 暴露 Prometheus 友好的指标。停机时服务会自动停止刷新任务并关闭 HTTP server，无需额外清理。

如果节点启用了 KadDHT，可选择将实例注册到 `MetricsService.registerIpnsTracker`，便于同步导出 IPNS 记录刷新指标。例如：

```nim
let metricsSvc = sw.services
  .filterIt(it of MetricsService)
  .mapIt(MetricsService(it))[0]
metricsSvc.registerIpnsTracker(kad)
```

注册后，服务会在后台循环中调用 `updateIpnsMetrics`，实时反映各命名空间的刷新成功率、退避窗口与下一次刷新时间。

## OpenTelemetry 推送导出

如果部署环境无法通过 Prometheus pull 指标，可以改用 `withOpenTelemetryExporter` 直接向 OTLP/HTTP 采集端推送 JSON 负载。该服务会基于 `Switch.resourceSnapshot()`、`BandwidthManager.snapshot()`、`Switch.bitswapLedgerStats()`、`Switch.dataTransferStats()`（如启用 DataTransfer）、`Switch.webtransportCerthashHistory()` / `Switch.webtransportMaxSessions()` / `Switch.webtransportRotationInterval()` / `Switch.webtransportRotationKeepHistory()` / `Switch.webtransportLastRotation()` / `Switch.webtransportSessions()` 以及（可选）通过 `registerIpnsTracker` 注册的 `KadDHT` 自动构造 Gauge / Sum 数据点，并按照 `interval` 周期触发推送。

```nim
import chronos
import libp2p
import libp2p/services/otelmetricsservice

let sw = SwitchBuilder.new()
  .withOpenTelemetryExporter(
    "https://otel-collector.example.com/v1/metrics",
    headers = [("Authorization", "Bearer <token>")],
    resourceAttributes = [("service.instance.id", "node-a")],
    interval = 10.seconds,
  )
  .build()

await sw.start()
# 如果节点启用了 KadDHT，可选地将其注册到 OTLP exporter：
let otelSvc = sw.services
  .filterIt(it of OtelMetricsService)
  .mapIt(OtelMetricsService(it))[0]
otelSvc.registerIpnsTracker(kad)

# ...
await sw.stop()
```

默认会补齐 `service.name=nim-libp2p`、`telemetry.sdk.language=nim` 与 `telemetry.sdk.name=nim-libp2p` 等资源属性，可通过 `resourceAttributes` 覆盖或追加；`scopeName` / `scopeVersion` 参数用于标识 instrumentation scope，`timeout` 控制单次推送等待时间（默认 3s）。失败时服务会记录日志并在下一周期重试，无需额外处理。

## OpenTelemetry 日志导出

若需要将 DataTransfer 通道状态流转同步到日志平台，可启用 `withOpenTelemetryLogsExporter` 将事件以 OTLP/HTTP `resourceLogs` 形式推送：

```nim
import chronos
import libp2p
import libp2p/services/otellogsservice

let sw = SwitchBuilder.new()
  .withGraphSyncDataTransfer() # 构建时创建 DataTransferChannelManager
  .withOpenTelemetryLogsExporter(
    "https://otel-collector.example.com/v1/logs",
    headers = [("Authorization", "Bearer <token>")],
    resourceAttributes = [("deployment.environment", "staging")],
    globalAttributes = [("service.instance.id", "node-a")],
    flushInterval = 2.seconds,
    maxBatchSize = 128,
  )
  .build()

await sw.start()
# ...
await sw.stop()
```

`OtelLogsService` 会自动订阅 `DataTransferChannelManager` 事件并生成包含事件类型、方向、状态、重试策略、最近错误/状态等属性的日志记录，默认以 2 秒间隔或批量达到 `maxBatchSize` 时发送。可通过 `globalAttributes` 为所有日志追加统一标签，通过 `severityOverrides`（`seq[DataTransferSeverityOverride]`，定义于 `libp2p/services/otellogsservice.nim`）重映射不同事件的严重级别；若需即时推送或解绑某个管理器，可调用 `flushNow`、`registerDataTransferManager` 与 `unregisterDataTransferManager`。`DataTransferChannelManager` 现支持多播事件处理，GraphSync 适配器与日志导出可同时挂载，无需额外接线。

## OpenTelemetry 链路追踪导出

若需要在观测层重建 DataTransfer 通道的生命周期，可启用 `withOpenTelemetryTracesExporter` 将状态流转映射为 OTLP/HTTP `resourceSpans`：

```nim
import chronos
import libp2p
import libp2p/services/oteltracesservice

let sw = SwitchBuilder.new()
  .withGraphSyncDataTransfer()
  .withOpenTelemetryTracesExporter(
    "https://otel-collector.example.com/v1/traces",
    headers = [("Authorization", "Bearer <token>")],
    globalAttributes = [("service.instance.id", "node-a")],
    spanKind = "SPAN_KIND_INTERNAL",
    flushInterval = 2.seconds,
    maxBatchSize = 64,
  )
  .build()

await sw.start()
# ...
await sw.stop()
```

`OtelTracesService` 会自动向 `DataTransferChannelManager` 注册事件处理器：每个通道在首次事件触发时创建一个 span，后续事件将作为 span event 记录，同时更新暂停状态、最近错误与 voucher 信息。完成、取消、失败或 voucher 被拒绝时会标记 span 结束并立即加入批次；当批量达到 `maxBatchSize` 或到达 `flushInterval` 时，会向 OTLP 采集端推送 `resourceSpans`。服务提供 `globalAttributes` 用于统一打标签，`spanKind` 可调整为 `SPAN_KIND_CLIENT` / `SPAN_KIND_SERVER` 等枚举值；如需即时推送或手动解绑，可调用 `flushNow`、`registerDataTransferManager` 与 `unregisterDataTransferManager`。

## CoinJoin 监控指标

在构建阶段启用 `-d:libp2p_coinjoin` 并注册 CoinJoin metrics loop（D2.3.f 里程碑）后，可通过 `withMetricsExporter`/`withOpenTelemetryExporter` 额外导出以下指标：

| 指标 | 类型 | 说明 |
| --- | --- | --- |
| `libp2p_coinjoin_session_active` | Gauge | 当前各阶段（Discovery/Commit/Shuffle/Signature/Finalize）的会话数量 |
| `libp2p_coinjoin_nonce_reuse_total` | Counter | 检测到的 nonce 重用次数（Musig2/FROST） |
| `libp2p_coinjoin_partial_fail_total` | Counter | partial signature 验证失败次数（缺失/格式错误/重复提交） |
| `libp2p_coinjoin_session_abort_total` | Counter | 由于超时、Blame、链上失败导致的会话终止次数 |
| `libp2p_coinjoin_collateral_locked` | Gauge | `MixerCollateralVault` 中已锁定的押金总额（单位按配置，可为 BTC/USDC） |
| `libp2p_coinjoin_collateral_slashed_total` | Counter | 押金罚没累计次数或金额 |

采集流程示例：
1. CoinJoin 会话状态机在 `stepSession` 中调用 `recordSessionPhase(phase)`、`recordSessionAbort(reason)` 更新 Gauge/Counter；
2. Musig2/FROST 聚合在 `processPartialSignature` 返回 `cjInvalidInput` 前调用 `incPartialFail(participantId)`，并在检测 nonce 重用时递增 `nonce_reuse_total`；
3. 押金模块监听 `CollateralLocked/Slashed/Released` 事件并调用 `updateCollateralMetrics(lockAmount, slashed)`；
4. Ops 可通过 `metricsExporter.updateCoinJoinMetrics(state)` 周期性导出上述指标，或在 OTLP 推送中追加 `coinjoin.*` 数据点。

> 该功能与 D2 子任务紧密关联，提交实现时请同步更新 `docs/dev_plan.md` 与 `docs/dex_bridge_design.md` 的相应章节。

### 手动控制

如果希望与现有的 HTTP server 集成或自定义更新周期，可以直接使用 `metrics_exporter` 中的 API：

```nim
import chronos
import metrics/chronos_httpserver
import libp2p
import libp2p/metrics_exporter

let sw = newStandardSwitch()
await sw.start()

var switchState = initSwitchMetricsState()
updateSwitchMetrics(sw, switchState)  # 可选：启动时采集一次
var bitswapState = initBitswapMetricsState()
await updateBitswapMetrics(sw, bitswapState)
let metricsLoop = spawnMetricsLoop(sw, 5.seconds)
let httpServer = MetricsHttpServerRef.new("0.0.0.0", Port(8000)).get()
await httpServer.start()

# 停止时确保关闭任务与 HTTP server
await metricsLoop.cancelAndWait()
await httpServer.stop()
await httpServer.close()
await sw.stop()
```

若需在自定义循环中同步导出 IPNS 指标，可将 `KadDHT` 列表封装为回调传入：`let metricsLoop = spawnMetricsLoop(sw, 5.seconds, proc (): seq[KadDHT] = @[kad])`。

> 提示：如果完全依赖 `spawnMetricsLoop`，可以省略手动调用 `updateBitswapMetrics`。但在自定义调度场景（例如集成现有定时器）时，应复用同一个 `BitswapMetricsState` 实例以便正确清零失效的债务指标。

在手动释放资源的场景，可以创建 `switchState` / `bitswapState` / `ipnsState` 并重复复用，通过 `updateResourceMetrics(sw.resourceSnapshot(), switchState)`、`updateBandwidthMetrics(sw.bandwidthManager, switchState)` 与 `updateIpnsMetrics(@[kad], ipnsState)`（其中 `kad` 为需要监控的 `KadDHT` 实例）将快照写入指标，然后交由外部任务 —— 如调度器或已有的 metrics loop —— 定时调用。

### 指标命名约定

- `libp2p_resource_connections{direction="inbound|outbound"}`：活动连接数量；
- `libp2p_resource_streams{direction=...}`：活动流数量；
- `libp2p_resource_shared_pool_{connections|streams}{pool=...,direction=...}`：共享池利用率；
- `libp2p_bandwidth_total_bytes{direction=...}` 与 `libp2p_bandwidth_rate_bps{direction=...}`：全局带宽累积与速率；
- `libp2p_bandwidth_protocol_{bytes|rate_bps}{protocol=...,direction=...}`：单协议统计；
- `libp2p_bandwidth_group_{bytes|rate_bps}{group=...,direction=...}`：`BandwidthManager.groupStats` 聚合后的协议组指标。
- `libp2p_bitswap_{bytes|blocks|have|dont_have|wants}_total{direction=...}`：Bitswap Ledger 跟踪的累计字节 / 区块 / HAVE / DONT_HAVE / Wants 次数；
- `libp2p_bitswap_exchanges_total`：Ledger 记录的交换次数；
- `libp2p_bitswap_peers`：当前活跃的 Bitswap 对等节点数；
- `libp2p_bitswap_max_debt_ratio`：Ledger 计算的最大债务比，用于识别拖欠节点；
- `libp2p_bitswap_top_debtor_ratio{peer=...,rank=...}`：当前债务比最高的前 5 个对等节点及其债务比，`rank` 从 1 起排序；
- `libp2p_bitswap_top_debtor_bytes{peer=...,rank=...,direction="sent|received"}`：对应前 5 个债务节点的累计发送 / 接收字节，便于评估欠债规模；
- `libp2p_bitswap_advertisements_total{target="dht|delegated|all",result="success|failure|skipped"}`：`AutoProviderBlockStore` 记录的 Provider 广告累计次数；`all/skipped` 代表因最小间隔而跳过的请求；
- `libp2p_bitswap_advertisement_last_success_timestamp{target="dht|delegated"}`：最近一次 DHT / Delegated Provider 广告成功的 Unix 时间戳；
- `libp2p_bitswap_advertisement_last_attempt_timestamp`：最近一次尝试发送 Provider 广告的 Unix 时间戳；
- `libp2p_graphsync_requests_total{direction=...,outcome="total|completed|partial|failed"}`：GraphSync 服务端与客户端累计处理的请求次数及结果分布；
- `libp2p_graphsync_blocks_total{direction=...}` / `libp2p_graphsync_bytes_total{direction=...}`：GraphSync 传输的区块数量与字节总量；
- `libp2p_graphsync_missing_links_total{direction=...}`：GraphSync 报告的缺失链路次数，便于定位不完整的同步树；
- `libp2p_datatransfer_channels_total`：当前 `DataTransferChannelManager` 管理的通道总数（包含入站与出站）；
- `libp2p_datatransfer_channels{direction="inbound|outbound",status="requested|accepted|ongoing|paused|completed|cancelled|failed"}`：按方向与状态细分的通道数量，用于观测活跃与终止通道的分布；
- `libp2p_datatransfer_paused{direction=...,kind="local|remote"}`：本地或远端处于暂停状态的通道个数；
- `libp2p_webtransport_cert_hash{state="current|history",hash="..."}`：当前可用的 WebTransport 证书指纹（`state=current`）与保留的历史指纹（`state=history`），便于监控证书轮换窗口；
- `libp2p_webtransport_max_sessions`：当前配置的 WebTransport 会话并发上限；
- `libp2p_webtransport_sessions{state="ready|pending"}`：当前处于握手完成（`state=ready`）或仍在初始化（`state=pending`）阶段的 WebTransport 会话数量；
- `libp2p_webtransport_handshake_seconds{state="ready|pending"}`：WebTransport 握手平均耗时（已完成）与当前等待中的握手平均排队时长（秒），便于识别握手瓶颈；
- `libp2p_webtransport_rejections_total{reason="session_limit|missing_connect_protocol|missing_datagram|missing_session_accept|connect_status_rejected|invalid_request"}`：按原因统计的 WebTransport 握手拒绝累计次数，可用于观测远端协议协商缺失或本地并发限制命中情况；
- `libp2p_webtransport_rotation_interval_seconds`：当前配置的 WebTransport 证书轮换间隔（秒）；
- `libp2p_webtransport_rotation_keep_history`：轮换时保留的历史 certhash 数量；
- `libp2p_webtransport_last_rotation_timestamp`：最近一次成功轮换 WebTransport 证书的 Unix 时间戳；
- `libp2p_ipns_records_total{state="tracked|overdue"}`：当前缓存的 IPNS 记录数量以及已到期待刷新数量；
- `libp2p_ipns_namespace_records{namespace=...,state="active|backoff"}`：按命名空间统计的活跃 / 处于退避窗口的记录数；
- `libp2p_ipns_refresh_total{namespace=...,result="success|failure"}`：自启动以来按命名空间累积的刷新成功 / 失败次数；
- `libp2p_ipns_next_attempt_seconds{namespace=...}`：距离该命名空间下一次计划刷新所需的最短秒数；
- `libp2p_ipns_last_event_unix{namespace=...,event="success|failure"}`：最近一次刷新成功 / 失败事件的 Unix 时间戳，可用于观测长时间无进展的命名空间。

借助这些关键指标，可以在 Grafana 或 Prometheus 中监控共享池的配额命中情况、发现带宽热点协议，并据此调整 `Switch.setResourceSharedPoolLimit`、`BandwidthManager.updateLimits` 等运行时配置。
