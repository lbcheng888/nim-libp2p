# 生产级 nim-libp2p DEX 与 K 线系统设计

## 1. 目标与范围
- **交易核心**：在 nim-libp2p 覆盖网络上构建高吞吐、低延迟、可扩展的订单撮合与结算体系，覆盖挂单、撤单、撮合、账户及风险控制。
- **行情体系**：为终端提供实时行情、逐笔成交、K 线（OHLCV）等市场数据，具备毫秒级延迟与分钟级回溯能力。
- **生产约束**：遵循金融合规（KYC/AML 可选实现）、高可用部署（99.9% SLA）、数据一致性（弱一致 / 最终一致）与可观察性需求。
- **技术基座**：全面复用 nim-libp2p 模块（SwitchBuilder、GossipSub、DataTransfer、Kademlia、pnet、ResourceManager 等）以及仓库现有示例经验。

## 2. 系统总体架构
```
┌────────────────────────────────────────────────────────────┐
│                    Client & Partner APIs                   │
│ REST / WebSocket / FIX  ─────────┐                         │
└──────────────────────────────────┴─────────────────────────┘
                 │                                       ↑
                 ▼                                       │
┌────────────────────────────────────────────────────────────┐
│                  Front Layer (Ingress Gateways)            │
│ - libp2p HTTP 服务网关 (身份校验、节流、签名验证)          │
│ - WebSocket 广播节点 (报价、成交、K 线推送)               │
│ - FIX/交易 API 适配器 (可选)                              │
└────────────────────────────────────────────────────────────┘
                 │  gossipsub / request-response / http-over-libp2p
                 ▼
┌────────────────────────────────────────────────────────────┐
│       Core Matching & Risk Layer (libp2p overlay)          │
│ - Order Intake Service (订单验证、归档、重放)               │
│ - Matching Engine (撮合 + AMM/限价双模式)                  │
│ - Position / Risk Engine (账户、保证金、限额)              │
│ - Settlement Executor (链上/链下结算)                      │
│ - Liquidity Routing (做市商、跨市场套利接入)               │
└────────────────────────────────────────────────────────────┘
                 │
                 ▼
┌────────────────────────────────────────────────────────────┐
│            Market Data & K-Line Layer                      │
│ - Trade Stream Processor (逐笔成交流)                      │
│ - K 线 Aggregator (多时间粒度 OHLCV 计算)                  │
│ - Order Book Snapshotter (定频快照)                        │
│ - Time-Series Storage (TSDB/列式存储 + 冷热分层)          │
│ - 数据分发：GossipSub topics + HTTP/FIX Market Feed        │
└────────────────────────────────────────────────────────────┘
                 │
                 ▼
┌────────────────────────────────────────────────────────────┐
│      Platform Services                                     │
│ - Identity & Access (DID/VC、ACL)                          │
│ - Key Management (HSM/TEE、pnet)                           │
│ - Observability (Prometheus/Otel、日志、审计)              │
│ - Deployment (Kubernetes、Ansible、遗留 bare-metal)        │
└────────────────────────────────────────────────────────────┘

## 3. 模块设计
### 3.1 网络与节点层
- **SwitchBuilder 配置**：启用 Noise+TLS、Yamux、Mplex，结合 AutoNAT v2、AutoRelay 保证跨 NAT 可用性；在生产环境默认开启 pnet 与 Tor/QUIC 组合，防止网络探测。
- **资源治理**：利用 `ResourceManager`、`BandwidthManager` 对撮合流、行情流、K 线流实施协议级与节点级限额；通过 `Switch.setResourceSharedPoolLimit` 动态调节高峰期资源。
- **身份管理**：基于 `peerstore.MetadataBook` 维护节点角色（matcher、kline、settlement）；关键节点要求通过 VC/TEE 证明加入。

> 参考实现：`examples/dex/network_identity.nim` 演示了 SwitchBuilder 构建、私网密钥、角色注册以及身份握手协议的代码骨架，可作为生产实现的起点。

### 3.2 订单撮合子系统
- **订单入口**：客户端签名订单后经 HTTP-over-libp2p 或 GossipSub `dex.orders` 主题进入；Order Intake 节点校验签名、账户状态、冷却期，并写入 Append-Only 日志（WAL）。
- **撮合核心**：
  - **限价撮合**：内存级 OrderBook（红黑树/跳表），通过 actor 模式或 lock-free 技术实现；使用 Nim `runtime_actor`（见 `docs/runtime_actor.md`）构建单线程事件循环，保证严格顺序。
  - **AMM/混合模式**：将 AMM 池状态与限价盘整合，提供不同产品线。
- **流动性管理**：做市节点通过 `dex.liquidity` 主题发布报价；撮合核心可使用 request-response 协商大额交易或 RFQ。
- **结算接口**：撮合结果写入 Settlement Queue，并通过 DataTransfer/GraphSync 将证明或链上 calldata 发送给 Settlement Executor。

### 3.3 风险与资金管理
- **账户模型**：支持全仓/逐仓，维护余额、保证金、风险比率。
- **风控**：实时监测大额订单、价格偏离、账户限额；通过 GossipSub `dex.risk.alerts` 主题推送告警。
- **清算与保险基金**：当风险指标触发，自动撮合清算订单；保险基金节点通过 libp2p 流获取数据并执行策略。

## 4. K 线系统设计
### 4.1 数据来源
- 逐笔成交事件由 Matching Engine 以 GossipSub `dex.trades` 主题广播（包含时间戳、价格、数量、方向、成交 ID）。
- Order Book 快照定频发布到 `dex.book.snapshot` 主题，供重算与校验。
- 外部市场/预言机数据通过 Oracle 节点以 `dex.oracle.prices` 主题注入。

### 4.2 聚合流程
1. **流式摄取**：K 线节点订阅 `dex.trades`，将事件写入高速缓存与本地持久队列（例如 Kafka/NATS/Chronicle DB）。
2. **窗口聚合**：按 1s → 1m → 5m → 1h → 1d 等多级时间窗口递归聚合：  
   - open = 窗口内第一笔成交价；close = 最后一笔；high/low = max/min；volume = 数量合计；turnover = 价格×数量累积。  
   - 对齐基于 UTC 微秒时间戳，使用事件到达与生成时间双重校验，必要时回补缺失成交。
3. **校验与重算**：若接收到延迟成交或行情修正，则针对受影响的窗口重新聚合，并生成版本号（`version`, `seq`)。
4. **存储策略**：  
   - 热数据（≤7 天）写入内存列存或高速 TSDB（InfluxDB、QuestDB）；  
   - 冷数据（>7 天）写入对象存储（Parquet）并通过 Bitswap/GraphSync 提供分发。  
   - 同步写入 Checkpoint，方便新节点快速恢复。

### 4.3 分发与消费
- **实时推送**：  
  - GossipSub 主题：`dex.kline.<symbol>.<interval>`，消息体为压缩 Protobuf/CBOR。  
  - WebSocket：HTTP 服务节点订阅 GossipSub 后向终端推送。  
  - FIX/REST：提供 `GET /kline?symbol=ETH_USDT&interval=1m&limit=1000` 查询接口，内部直接读取热存储或 LRU 缓存。
- **一致性保障**：引入 Sequence + Checkpoint（`lastTradeId`、`windowStart`）。消费端若发现缺口可通过 request-response 向 K 线节点请求补偿窗口。
- **监控指标**：  
  - `dex_kline_delay_seconds`：成交到 K 线发布的延迟。  
  - `dex_kline_rebuild_total`：重算次数。  
  - `dex_trade_ingest_lag`：摄取延迟。  
  - `dex_marketdata_pub_rate`：各主题每秒消息数。

## 5. 数据与存储策略
- **交易日志**：WAL + 区块压缩（Parquet/ORC）。支持增量快照，利用 Bitswap 发布备份块，Kad DHT 广告 provider。
- **账户/风险数据**：采用分布式 KV（TiKV/ScyllaDB）或 PostgreSQL 集群；通过 `libp2p/protocols/fetch` 提供读取服务。
- **市场数据缓存**：  
  - Redis/Dragonfly 作为热缓存。  
  - RocksDB/Badger 作冷存索引。  
  - 使用 GossipSub 的状态通告同步缓存失效。

## 6. 安全、合规与审计
- **网络安全**：全局启用 pnet；关键节点使用多区域 relay；外部查询通过 API Gateway 作访问控制与节流。
- **交易安全**：订单签名 + 双重验证；可选硬件签名（YubiHSM）。  
- **权限与审计**：记录所有管理操作，存储于 append-only log，结合 Merkle Proof 防篡改。  
- **合规接口**：集成链上/链下黑名单、AML 分析，必要时在订单入口拒绝交易。
- **客户端防护同步（落地示例）**：
  - `examples/dex/security/signing.nim` 使用 ed25519 自动生成/加载密钥，`DexNode` 在广播前签名、在订阅端验签，`--allow-unsigned/--disable-signature` 控制兼容与调试。
  - 本地存储通过 nim-pebble WAL (`examples/dex/storage/order_store.nim`) 记录全部订单/撮合，重启由 WAL 重放恢复，并暴露 `exposureFor` 与敞口快照供风控使用。`dex_node --metrics-file` 输出 Prometheus 文本格式指标，移动端可交由 textfile collector、node-exporter 或端内 nim-pebble 任务读取并上报（例如 `dex_mobile_metrics.prom`）。
  - 节点级敞口可用 `--limit=<asset>:<amount>` 实时丢弃超额订单，`DexNode.riskDrops` 计数并输出在 `logStoreStats`，便于接入上层告警。

## 7. 运维与部署
- **拓扑建议**：  
  - 匹配、风险、K 线服务运行在独立节点池，通过 libp2p overlay 互联；  
  - 不同区域部署多个 Gateway，使用 Rendezvous + Kademlia 进行发现；  
  - K 线节点至少部署 3+ 副本，采用 leader/follower 模式保障一致性。
- **CI/CD**：  
  - Nim 编译产物通过 Docker 镜像发布。  
  - 集成测试含撮合 + K 线回测场景。  
  - 使用 `tests/pubsub` 系列作为 GossipSub 回归基础，新增 K 线聚合单元测试。
- **监控**：Prometheus + Grafana 观测网络、撮合延迟、K 线延迟；日志接入 Loki/ELK。  
  - 关键告警：订单处理超时、撮合节点失联、K 线延迟过阈值、数据重算频率异常。

## 7.1 移动端优化与适配
- **轻量网络栈**：移动端 SDK 以 `libp2p` QUIC / WebTransport 作为首选传输，降级到 WebSocket；通过 `withTcpTransport` + `memorytransport` 的组合在弱网测试环境模拟。
- **断线重连**：客户端维护 PeerStore 快照与身份缓存；失去连接后优先通过最近成功的 Relay / Rendezvous 重新注册；配合 `HPService` 或本地 NAT 穿透（如 Android + Wi-Fi Direct）。
- **有限算力优化**：  
  - 行情订阅默认采用增量推送（逐笔 + 最新 K 线），历史数据由 REST/FETCH 拉取；  
  - 启用消息压缩与裁剪（仅保留必要字段）。  
  - 客户端维护 LRU K 线缓存，后台使用延迟写入减少电量消耗。
- **安全与密钥管理**：在移动端引入安全模块（如 Android Keystore、iOS Secure Enclave）存储私钥；身份握手流程复用 `DexIdentityProtocol` 的角色标签，确保终端在 ACL 范围内。
- **灰度发布**：移动端 SDK 通过 feature flag 控制协议升级，CI/CD 需提供真实设备（Android/iOS）自动化覆盖，针对弱网（2G/3G）和漫游场景做基准测试。
- **可观测性**：移动端通过轻量遥测（例如 gRPC-Web 或 HTTPS）上报关键指标（连接成功率、重连次数、CPU/内存占用、耗电），接入 Ops 监控平台。
- **落地提示**：当前示例 (`examples/dex/dex_node.nim`) 已启用 ed25519 签名与 nim-pebble WAL；通过 `--limit=<asset>:<amount>` 可在端上实时拒绝超额订单，并将 `riskDrops/invalidOrders` 暴露到日志，便于复用至移动 SDK。

### 7.2 nim-pebble 本地数据层
- **选择动因**：nim-pebble 提供的 MemTable、WAL、资源管理与 VFS 装饰器能在设备端构建低功耗、可控的嵌入式 LSM，适合缓存订单、撮合回执与 K 线快照。`nim-pebble/pebble/mem/memtable.nim:1` 展示了内置的流量控制与 `ResourceManager` 结合方式，可在移动端限制内存占用并在达到软阈值时触发 flush。
- **资源治理**：`nim-pebble/pebble/runtime/resource_manager.nim:1` 将内存、磁盘、文件句柄与“Compaction token”抽象成配额；结合 `admitWrite`（MemTable）即可在峰值期调节写入延迟，避免抢占 UI 线程。
- **持久化管道**：`nim-pebble/pebble/wal/writer.nim:1` 提供多段 WAL、自动刷盘/回收逻辑，可按 `autoSyncBytes` 调整写放，确保订单与成交在弱网/闪退场景下可恢复。WAL 后端可以挂接 `pebble/runtime/executor.nim:1` 的线程池，在系统空闲时批量刷盘或压缩。
- **VFS 适配**：面向闪存的读缓存/缓冲写入可直接复用 `nim-pebble/pebble/vfs/cache_fs.nim:1` 与 `nim-pebble/pebble/vfs/buffered_fs.nim:1`，结合移动端的沙盒路径挂载，既降低随机 IO，又能按需丢弃缓存。
- **接入示例**：`examples/bridge/bridge/storage.nim:1` 已演示如何在事件存储中引入 `pebble/batch/db`、`pebble/wal/writer` 与 `pebble/runtime/executor`。移动端 DEX 可使用相同模式：1) 将订单/成交/风险快照序列化为 Pebble KV；2) 以 WAL + MemTable 记录最新状态；3) 在后台 compaction 线程内把已确认成交压缩成冷数据，必要时再通过 Bitswap 广播。
- **实践建议**：
  1. **数据分层**：热数据（最近 N 分钟撮合、K 线缓存）驻留在 MemTable，冷数据落地到 Pebble SSTable，K 线回放直接查询本地 LSM 后再与节点比对。
  2. **批处理与同步**：利用 `pebble/runtime/executor` 的 TaskHandle 绑定移动端调度器（WorkManager / BackgroundTaskScheduler），在充电或 Wi-Fi 时批量做 compaction。
  3. **额度映射**：把 nim-pebble 的 `ResourceKind` 与移动端的配额（前台/后台内存、磁盘）映射起来，写入 `ResourceManager.setQuota`，并将超限事件上报至客户端遥测。
  4. **共享缓存**：若终端需要离线展示历史 K 线，可把 Bitswap 拉取的区块落在 Pebble，再通过 VFS 缓存暴露给 UI，避免重复解析。
  5. **安全性**：结合平台加密文件系统或在 WAL 段落写入前做对称加密，密钥托管在 Secure Enclave / Keystore，防止订单被本地窃取。

## 8. 灾备与扩展
- **快速恢复**：  
  - K 线节点启动时拉取最新 Checkpoint + WAL 重放；  
  - 撮合节点使用快照 + 订单回放重建 OrderBook。  
  - Settlement Executor 维护待结算队列，失败自动重试。
- **水平扩展**：  
  - 通过一致性哈希划分交易对（Symbol 分片），每个分片拥有独立撮合与 K 线节点；  
  - 使用 libp2p Relay 连接分片内节点，跨分片通过 GossipSub 汇聚。
- **未来演进**：加入链下隐私（FHE/TEE）、高频行情（纳秒级时间戳）、多链结算以及基于 libp2p QUIC/WebTransport 的移动端优化。

## 9. 下一步落地建议
1. 基于当前 `examples/dex/dex_node.nim` 抽象网络层库，搭建多服务框架（命令/事件总线、配置中心）。  
2. 实现逐笔成交流 → K 线聚合 pipeline 的最小化产品，优先支持 1s/1m 两档。  
3. 增加 e2e 测试：模拟 10k TPS 订单，验证撮合延迟与 K 线同步。  
4. 建立运维手册：节点扩容、密钥轮换、指标阈值、故障演练。  
5. 规划安全审计与合规检查（渗透测试、密钥管理评估、金融监管接口）。

## 附录：原子任务正交矩阵

| 模块 \\ 生命周期 | 需求澄清 / 设计 | 核心实现 | 测试验证 | 部署上线 | 监控 & 运维 |
| ---------------- | --------------- | -------- | -------- | -------- | ----------- |
| **网络与身份** | 定义节点角色、拓扑、pnet 策略；梳理 ACL/VC 权限模型 | SwitchBuilder 配置、AutoNAT/Relay 集成、连接门控（✅ 已完成，见 `examples/dex/network_identity.nim`） | 单元测试连接建立、NAT 打洞；互操作测试（go-libp2p/rust-libp2p） | 生成节点配置、证书及 pnet key；发布部署脚本 | 监控连接数、带宽、失败拨号；密钥轮换流程 |
| **订单入口 & 撮合** | 订单类型、撮合规则、账户模型；WAL 结构 | Intake 校验、WAL 写入、限价/AMM 引擎、风险检查钩子 | 回放测试、压力测试（≥10k TPS）、一致性检查 | 滚动升级策略、分片部署方案 | 撮合延迟、队列深度、失败率、回放工具 |
| **风险与资金** | 保证金公式、风控阈值、清算规则 | 风控引擎、限额服务、保险基金逻辑 | 模拟不同市场情景、极端价格测试 | 风险参数管理接口、分布式配置 | 指标：风险系数、清算事件、限额触发；审计日志 |
| **结算与桥接** | 支持链清单、结算流程、证明格式 | Settlement Queue、DataTransfer/GraphSync 交互、外部链适配器 | 单元测试 + 沙箱链集成测试 | 多链节点部署、密钥管理、回滚流程 | 结算成功率、延迟、重试；链上/链下对账 |
| **行情 & K 线** | 数据粒度、窗口策略、发布协议 | Trade Stream、K 线 Aggregator、快照服务、TSDB 接口 | 回放聚合、延迟测试、重算验证 | K 线节点多副本部署、缓存策略 | 指标：聚合延迟、重算次数、订阅速率；告警阈值 |
| **存储 & 数据湖** | 热/冷数据策略、备份策略、合规要求 | 热存缓存、冷存落地、Bitswap Provider、快照工具 | 数据一致性校验、恢复演练 | 备份调度、对象存储配置、访问控制 | 存储容量、复制延迟、恢复演习记录 |
| **安全与合规** | KYC/AML 需求、权限矩阵、审计范围 | 签名验证、风控拦截、审计日志、合规 API | 安全测试、渗透测试、合规检查脚本 | 密钥托管、HSM/TEE 部署、合规接口对接 | 审计追踪、告警、合规报告生成 |
| **可观测性 & 运维** | 指标/日志/追踪需求清单 | Prometheus/Otel 集成、日志标准化、事件通知 | 混沌演练、告警测试、容量规划 | CI/CD、蓝绿/金丝雀发布、配置管理 | SLA 监控、故障响应流程、事后复盘 |
| **移动端与接入层** | 客户端场景梳理（平台、网络、功耗）、SDK 能力清单 | 移动 SDK 网络适配（QUIC/WS）、断线重连、身份缓存、K 线轻量模式（✅ `DexIdentityProtocol` 可复用，待补充终端封装） | 真机弱网/高延迟测试、功耗与内存剖析、端到端用户体验测试 | 灰度发布策略、应用商店合规、配置中心/特性开关 | 客户端遥测指标、崩溃分析、用户反馈闭环、版本回滚流程 |

## 附录：比特币转账与买卖集成原子任务正交矩阵

| 模块 \ 生命周期 | 需求澄清 / 设计 | 核心实现 | 测试验证 | 部署上线 | 监控 & 运维 |
| ---------------- | --------------- | -------- | -------- | -------- | ----------- |
| **BTC 网络与同步** | 选型全节点 RPC / Neutrino / 第三方 API；定义区块/UTXO 同步策略与信任域 | 封装 `btc_sync` 服务：管理区块头、过滤器、UTXO 集合；与 libp2p 节点共享状态缓存 | 主网/测试网回放、分叉与重组模拟、断网/弱网恢复测试 | 独立 BTC 同步节点部署、证书管理、访问控制 | 区块滞后、同步延迟、重组次数、RPC 错误、带宽/CPU 指标 |
| **钱包与密钥** | 需求多签/阈值签名还是单签；冷/热钱包切分；密钥审计策略 | 集成 BIP32/BIP39/BIP86 推导、PSBT 组装、硬件签名接口；实现出入金地址池管理 | 单元测试（签名、序列化）、集成测试（PSBT->Tx）、硬件签名回归 | HSM/硬件钱包部署、密钥轮换流程、权限隔离 | 钱包余额、地址消耗率、签名失败率、密钥审计日志 |
| **资金托管与清算** | 明确托管模式（多签、HTLC、桥合约）；定义链上/链下结算流程 | 托管协调器：维护托管状态机、发起/确认 BTC 转账、生成凭证写入 nim-pebble WAL；映射到 DEX 敞口 | 端到端清算演练、异常回滚、重放攻击、签名门限容错测试 | 托管节点部署、阈值签名服务上线、跨环境灰度策略 | 托管余额、签名参与率、清算成功率、回滚事件告警 |
| **订单与撮合扩展** | 设计 BTC 现货/OTC/闪兑产品；定义 `asset=BTC/*` 的撮合/撮合后转账流程 | 扩展 `OrderMessage`、`MatchMessage` 含 BTC 转账引用；撮合成功后触发链上交易或托管更新；将 TxID 写入 WAL | 撮合回放、双花/延迟演练、撮合-转账一致性检查 | 在撮合集群灰度开启 BTC 资产、配置风控阈值、联动托管节点 | `BTC` 敞口、撮合->链上落地时延、失败订单、双花检测告警 |
| **风险与合规** | KYC/AML、链上监控、限额、黑名单策略 | 风控引擎读取链上余额/UTXO、同步 OFAC/Travel Rule 黑名单、实时限额；审计日志落地 | 压测（限额/拉黑）、链上风险场景仿真、审计日志恢复 | 合规接口对接、黑名单同步、定期审计计划 | 风险限额触发率、黑名单命中、审计日志完整性、自检作业 |
| **可观测性与运营** | 定义 BTC 特有指标、日志、追踪需求 | 扩展 metrics：区块高度、重组次数、Tx 成功率、UTXO 缓存；日志标记 TxID 与订单 ID；Tracing 关联合约/托管事件 | 混沌测试：节点宕机、RPC 超时、交易卡块；指标告警验证 | CI/CD 针对 BTC 服务独立发布、蓝绿/金丝雀策略、故障预案 | Prometheus/Otel 监控 BTC 服务、链上确认延迟、Tx fee 追踪、运维 runbook |

## 附录：BTC ↔ USDC 兑换方案（含 RPC 149.28.194.117:8332）

1. **整体架构**
   - DEX 继续承担订单撮合与风险控制，新增 “BTC ↔ USDC 兑换协调器” 子模块。协调器监听 `dex.matches`，对 `asset` 为 `BTC/USDC` 的撮合结果发起链上结算。
   - **比特币侧**：通过环境变量注入的 RPC (`RPC_USER`, `RPC_PASSWORD`, 端点 `149.28.194.117:8332`) 调用全节点，读取区块状态 (`getblockchaininfo`) 并生成/广播转账 (`walletcreatefundedpsbt`, `sendrawtransaction`)。协调器维护 nim-pebble WAL，将订单 ID ↔ TxID 关联记录。
   - **USDC 侧**：默认以以太坊主网为基线（可扩展至 Polygon/Arbitrum）。兑换协调器通过现有 bridge 模块（`examples/bridge`) 的存储与服务模式，封装一个 `usdc_transfer` 服务：与外部 ERC-20 托管合约交互（调用 `transferFrom` / `transfer`），并在 nim-pebble 中记账。若其他链（Solana、Polygon）需要支持，则在该服务中添加多链适配层。

2. **资金托管**
   - **BTC**：采用多签钱包或 PSBT 工作流，由撮合结果触发的协调器负责构建交易，签名由密钥服务/硬件 HSM 完成；当 Tx 广播成功并达到确认阈值后，将状态写入 WAL 并通知 DEX 风控降低敞口。
   - **USDC**：使用托管合约地址，兑换成功后由合约向用户地址或流动性池转账。协调器负责在链上确认交易成功，并将 `TxHash`、gas 费用记录在 nim-pebble。

3. **流程概述**
   1. Trader 发布 `BTC/USDC` 订单 → Matcher 撮合成功 → `MatchMessage` 附带兑换需求。
   2. 兑换协调器捕获匹配，先检查风控（额度、OFAC 列表、链上余额）。
   3. 调用 BTC RPC：构建 PSBT、签名、`sendrawtransaction`，并将 TxID 关联订单。
   4. 调用 USDC 服务：向托管合约发起对侧转账（或锁定 USDC）。
   5. 链上确认达到阈值后，协调器在 nim-pebble 中更新结算状态，通过 GossipSub/HTTP 回执发送给撮合双方。

4. **安全与配置**
   - 将 RPC 凭证存于 `.env` (`RPC_USER`, `RPC_PASSWORD`)，由 DEX 进程读取后注入连接池，避免硬编码；RPC 连接使用 Basic Auth + TLS（若节点支持）。
   - USDC 侧 RPC/私钥同样通过环境变量/密钥管理系统提供，协调器只持有短期会话密钥或调用外部签名服务。
   - 全流程的关键事件（请求 TxID、确认高度、签名失败）写入 `dex_node` 的 metrics 与日志中，以 `dex_btc_*` / `dex_usdc_*` 指标形式暴露。

5. **分阶段落地建议**
   - **阶段 1**：实现读取 BTC 区块信息与 USDC 链上余额，并将 `BTC/USDC` 撮合结果持久化到 nim-pebble（不做真实转账）。
   - **阶段 2**：打通 BTC RPC 生成/广播测试网交易，USDC 侧通过模拟合约或测试链转账；验证 WAL 与指标。
   - **阶段 3**：引入正式托管方案（多签/HTLC）以及 KYC/AML 流程，完成主网部署与监控告警。

### 跨链 USDC ↔ BTC 方案（ETH / BSC / Solana / Tron）

| 模块 | 原子任务 | 产出 | 负责人 | 备注 |
| --- | --- | --- | --- | --- |
| **BTC-Testnet** | 1. RPC 连通性检测 (`getblockchaininfo`, `estimatesmartfee`) | CLI/自动化报告 + 连接池配置 | 网络 | 端点 `http://127.0.0.1:18332`，需 Basic Auth |
| | 2. PSBT 构建与签名回放 (`walletcreatefundedpsbt` → HSM stub → `finalizepsbt`) | `btc_psbt_playbook.md` + 单元测试 | 结算 | 测试钱包保留 10 BTCt，用于回归 |
| | 3. 结算状态写入 Nim-pebble (`settlement_record wal`) | `examples/dex/storage/settlement.wal` | 存储 | 与 Android FFI 对齐 JSON Schema |
| | 4. JSON-RPC 客户端 (`chronos/apps/http/httpclient` + Basic Auth) | `btc_rpc_client.nim` + 单元测试 | 结算 | 支持 `getblockchaininfo`/`walletcreatefundedpsbt`/`sendrawtransaction` |
| | 5. PSBT 构建器 (`examples/dex/settlement/btc_psbt.nim`) | 纯 Nim 序列化 & `signRaw` stub | 安全 | 先集成 `.env` 的 `PRIVATE_KEY`，后续接 HSM |
| | 6. Coordinator 落地：`submitBtcSwap` 替换 `simulateBtcSwap` | `coordinator.nim` 调用 RPC + WAL 记录 TxID | 结算 | 错误重试、指数退避 |
| | 7. FFI & Command：`libp2p_submit_btc_swap` + Android `NimBridge.submitBtcSwap` | 新增 command + JNI + Compose 控件 | FFI/客户端 | 触发参数：`orderId`, `amount`, `targetAddr`, `RPC_USER/PASSWORD` |
| | 8. 监控：`dex_btc_tx_latency`, `dex_btc_rpc_errors_total` | Prometheus Rule + Alert | 可观测性 | 阈值：RPC 错误 5 次/5min 告警 |
| **BSC-Testnet** | 1. EVM JSON-RPC 客户端（`eth_call`, `eth_sendRawTransaction`） | `UsdcSettlementClientBsc` | 跨链 | RPC `https://bsc-testnet-dataseed.bnbchain.org` |
| | 2. Keystore/私钥管理（临时 HSM stub） | `.env` (`BSC_TESTNET_PK`) + key vault 指南 | 安全 | 安卓侧走 Keystore，服务端走 Hashicorp Vault |
| | 3. tUSDC 合约交互 (`transfer`, `balanceOf`) | ABI + `dex_bsc_transfer.nim` | 智能合约 | 合约 `0xE4140d73e9F09C5f783eC2BD8976cd8256A69AD0` |
| **K 线节点** | 1. Trades → Kline pipeline (`dex.trades`/`dex.kline.*`) | `kline_store.nim` + Prom 指标 | 市场数据 | 先支持 1s/1m，后续扩展 |
| | 2. Checkpoint 与 WAL 重放 | `kline_checkpoint.md` | 存储 | 与移动端 pebble 方案一致 |
| **Android 集成** | 1. FFI 扩展：提交 BTC/USDC 结算、监听 K 线 | JNI + Kotlin API (`submitBtcSwap`, `submitBscUsdcTransfer`) | 移动 | 复用 `NimBridge` | 
| | 2. UI：BSC/BTC 控制台 + K 线可视化 | Compose 视图 + ViewModel 状态机 | 客户端 | 与 QA 对齐验收脚本 |
| **Observability** | 1. Prom 指标：`dex_btc_tx_confirmations`, `dex_usdc_tx_latency_ms`, `dex_kline_delay_seconds` | metrics 规格文档 | 可观测性 | 延迟 SLA：< 3s |
| | 2. 告警矩阵（RPC 超时、余额不足、PSBT 失败） | Alertmanager 规则 | SRE | 分级：P1/ P2 |

> 原子任务定义：可在一周内独立完成、验收标准明确、上下游依赖清晰。矩阵行可按团队细分，列可扩展测试/风控/文档子任务。

1. **逻辑分层**
   - **DEX 撮合层**：延续现有 `dex_node` 的 GossipSub 撮合，扩展 `asset` 列表支持 `BTC/USDC`、`USDC/BTC`（区分链）。
   - **跨链协调层**：新增 `CrossChainSwapCoordinator` 服务。它订阅 `dex.matches`，根据 `asset` 信息选择对应链的 `UsdcSettlementClient` 与 `BtcSettlementClient`。协调器负责：
     1. 校验链上余额/授权。
     2. 调用链节点生成/签名交易。
     3. 将链上 Tx hash、状态写入 nim-pebble WAL，并在 `dex.matches` 或 REST API 中回执。
   - **链适配层**：为每条链提供统一接口：
     ```
     type UsdcSettlementClient = concept client
       proc prepareTransfer(client; to: string; amount: uint256; memo: string): Future[ChainTx]
       proc submit(client; tx: ChainTx): Future[TxReceipt]
       proc pollStatus(client; txId: string): Future[TxStatus]
     ```
     具体实现因链而异（ETH/BSC: EVM；Solana: RPC + ed25519；Tron: TRC20 + gRPC）。

2. **BTC 侧**
   - 使用 `.env` 中的 `RPC_USER`、`RPC_PASSWORD` 对 `149.28.194.117:8332` 进行 Basic Auth 调用。
   - `BtcSettlementClient` 支持：
     - `getblockchaininfo`、`getbalance`、`listunspent` 监控资金。
     - PSBT 流程：`walletcreatefundedpsbt` → HSM/外部签名 → `finalizepsbt`/`sendrawtransaction`。
   - 交易成功后将 TxID 写入 WAL、metrics（`dex_btc_tx_confirmations`）。

3. **USDC 侧 (多链)**
   - **以太坊**：
     - RPC：`https://rpc.ankr.com/eth`（可替换自建/Infura）。
     - USDC 合约：`0xA0b86991c6218b36c1d19d4a2e9eb0ce3606eb48`。
     - `UsdcSettlementClientEth`：基于 JSON-RPC 发送 `eth_call`/`eth_sendRawTransaction`，通过 `bridge` 模块里的 `TransactionBuilder` 撰写 `transfer`/`approve`，签名交由 HSM/KMS。
   - **BSC**：
     - RPC：`https://bsc-dataseed.binance.org/`。
     - USDC 合约：`0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d`。
     - 逻辑同 EVM，注意 gas 价格与 nonce 独立管理。
   - **Solana**：
     - RPC：`https://api.mainnet-beta.solana.com`，USDC mint `EPjFWdd5AufqSSqeM2qDQ3Nf5JknLm4K71y9w4G5DNs`。
     - `UsdcSettlementClientSol`: 使用 `solana-web3` 风格接口生成指令（`Token Program` transfer），签名依赖 ed25519 密钥或外部签名器；需维护 `recentBlockhash` 缓存。
   - **Tron**：
     - RPC：`https://api.trongrid.io` (TRC20 USDC: `TEkxiTehnzSmSe2XqrBj4w32RUN966rdz8`)。
     - 客户端生成 TRC20 `TransferContract`，通过 Tron gRPC/HTTP 进行签名与广播；注意 API key 配额限制。

4. **托管与风控**
   - 每条链的热钱包地址配置在 `.env` (`ETH_USDC_HOT_WALLET`, `BSC_USDC_HOT_WALLET`, `SOL_USDC_AUTHORITY`, `TRON_USDC_OWNER`)；余额低于阈值时告警。
   - BTC/USDC 双边均需限额与黑名单检查：
     - `dex_risk_usdc_limit{chain}`、`dex_risk_btc_limit` 指标用于监控敞口。
     - OFAC/KYC 列表在协调器中缓存，撮合前校验出入金地址。
   - WAL 记录：`orderId`, `chain`, `txId`, `direction`, `amount`, `status`，便于审计/回放。

5. **监控与回执**
   - 新增 GossipSub 主题 `dex.settlements`，协调器在链上确认完成后广播回执。
   - Metrics 示例：
     - `dex_usdc_tx_submitted_total{chain}`
     - `dex_usdc_tx_confirmations{chain}`
     - `dex_btc_psbt_pending`
     - `dex_crosschain_failures_total{phase}`（准备/提交/确认）。
   - 日志与 tracing：每个跨链操作附带 `orderId`, `matchId`, `chain`, `txId`，利于故障定位。

6. **阶段推进**
   - **阶段 A**：实现 `CrossChainSwapCoordinator` + `BtcSettlementClient`（仅查询 + mock 提交），验证 WAL/metrics。
   - **阶段 B**：落地 EVM (ETH/BSC) USDC 转账；Solana/Tron 保留 stub。
   - **阶段 C**：补齐 Solana/Tron 客户端、HTLC/多签托管、合规审计流程，并上线主网。

### 附录：BSC 测试网 & BTC 测试网 USDC/BTC 兑换原子任务矩阵

| 模块 \ 生命周期 | 需求澄清 / 设计 | 核心实现 | 测试验证 | 部署上线 | 监控 & 运维 |
| ---------------- | --------------- | -------- | -------- | -------- | ----------- |
| **BSC Testnet 接入** | 选定 RPC（`https://bsc-testnet-dataseed.bnbchain.org`）、账户体系、tUSDC 合约（`0xE4140d73e9F09C5f783eC2BD8976cd8256A69AD0`）、gas 策略 | 实现 EVM JSON-RPC 客户端、Keccak/RLP、secp256k1 签名；`UsdcSettlementClientBsc` 负责 nonce/gas 管理、`transfer` 交易构造；从 `.env` 读取 `PRIVATE_KEY` | 使用 BSC testnet faucet + Etherscan 验证转账，编写集成测试（撮合 → tUSDC transfer）；模拟 gas price 波动、nonce 冲突 | 在测试环境部署协调器 + BSC 节点，配置定时补充 test BNB，灰度开放 tUSDC 兑换 | 指标：`dex_settlement_events_total{chain="bsc-test"}`、gas 余额、nonce backlog；日志保留 Tx hash；失败重试/告警流程 |
| **BTC Testnet 接入** | 确定 Testnet3 RPC (`BTC_TEST_RPC_URL`)、PSBT 工作流、UTXO 管理、签名密钥来源 | `BtcSettlementClientTestnet`: 调用 `walletcreatefundedpsbt` + PSBT 签名（secp256k1 或 HSM），`sendrawtransaction`，并写入 Tx hash；支持 `getblockchaininfo`/`listunspent` 监控 | 在 Testnet3 上回放从撮合 → PSBT → Broadcast → 6 确认流程；模拟双花/重组；加入单元测试验证 PSBT 序列化 | 在测试环境部署 bitcoind testnet 节点（或公共 RPC），配置 watcher 监控确认；灰度开放 BTC/testUSDC 兑换 | 指标：`dex_btc_psbt_pending`、`dex_btc_confirmations`、`dex_crosschain_failures_total{phase}`；日志记录 TxID、PSBT 摘要；告警：同步滞后、PSBT 失败等 |

### 公开 RPC 与 USDC 合约地址（ETH / BSC / Solana / 波场）

| 链 | 官方/公共 RPC 入口（可直接用于 PoC） | 主网 USDC 合约地址 | 备注 |
| --- | --- | --- | --- |
| Ethereum Mainnet | `https://rpc.ankr.com/eth` *(可替换 Infura/Alchemy，自行配置 API key)* | `0xA0b86991c6218b36c1d19d4a2e9eb0ce3606eb48` | 18 decimals；建议通过 `.env` 注入 `ETH_RPC_URL`、`ETH_USDC_ADDRESS`；gas 费用、nonce 处理需复用现有 `bridge` 模块逻辑。 |
| BNB Smart Chain (BSC) | `https://bsc-dataseed.binance.org/` | `0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d` | 18 decimals；可配置 `BSC_RPC_URL`；注意 BSC 使用 BNB 作为 gas，需单独监控 gas 余额。 |
| Solana Mainnet-Beta | `https://api.mainnet-beta.solana.com` | `EPjFWdd5AufqSSqeM2qDQ3Nf5JknLm4K71y9w4G5DNs` | USDC mint；协调器需要调用 JSON-RPC + `solana-web3` 风格签名，可通过 FFI/bridge 进程管理；需维护 `recentBlockhash` 缓存。 |
| Tron Mainnet | `https://api.trongrid.io` *(需申请 API key)* | `TEkxiTehnzSmSe2XqrBj4w32RUN966rdz8` | 波场 USDC (TRC20)；建议将 `TRON_RPC_URL`、`TRON_USDC_ADDRESS` 写入 `.env`，通过 gRPC/HTTP API 构建/签名交易。 |

> **配置建议**：
> - 在 `dex_node` 或兑换协调器启动时读取 `.env` 中的 `ETH_RPC_URL`、`BSC_RPC_URL`、`SOL_RPC_URL`、`TRON_RPC_URL` 以及对应的 `*_USDC_ADDRESS`。默认值可以指向上表中的公共 RPC/地址，但生产环境必须替换为自建节点或合规服务。
> - 每条链的签名私钥/多签参与者信息不要放入配置文件，统一由密钥管理服务（HSM / KMS）提供临时签名接口；DEX 只保存授权 token。
> - 监控侧新增 `dex_usdc_chain_height{chain="eth"}`、`dex_usdc_tx_confirmations{chain}` 等指标，快速定位跨链资金卡顿。
