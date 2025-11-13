# 使用 nim-libp2p 的移动端全网一致性后台更新方案

## 背景与目标
- **目标**：确保任意一个发布节点产出新版本后，Android、鸿蒙、苹果生态的应用都能在后台自动收敛到同一版本。
- **挑战**：终端网络条件差异巨大、节点可能离线、需要端到端可信校验，同时各移动系统对后台任务与安装权限有不同限制。
- **依赖**：nim-libp2p 提供的 GossipSub、KadDHT、Bitswap/GraphSync、RecordStore/IPNS、ResourceManager 等模块。

## 网络与节点拓扑设计
- **统一更新网络**：所有发布及下载节点运行同一套 Nim 服务。通过 `SwitchBuilder` 启用 `withTcpTransport`、`withMsQuicTransport`、`withNoise`、`withGossipsub`、`withBitswap`、`withGraphSync`，形成去中心化拓扑。
- **事件广播 (GossipSub)**：使用专用主题（示例：`unimaker/mobile/version`）发布带签名的 manifest，利用 GossipSub 的评分、PX 与心跳机制保证消息最终一致达到所有在线节点。
- **内容寻址分发**：安装包与差分片以 CID 存放到分布式存储，`AutoProviderBlockStore` 自动向 KadDHT 宣告 provider，确保缺块节点可从任意在线 peers 拉取。
- **权威指针 (IPNS)**：将每个渠道的最新 manifest CID 写入 IPNS 记录，并通过 KadDHT 刷新、缓存 TTL。迟到节点上线后解析 `ipns://<name>` 即可定位最新版本。

## Manifest 与一致性控制
- **Manifest 结构**：包含渠道、语义化版本、单调递增 `sequence`、发布时间、支持平台、差分基线、包哈希列表及可选元数据。使用 `SignedEnvelope` 由发布者私钥签名。
- **冲突处理**：`sequence` 必须递增；`IpnsNamespacePolicy` 可配置“拒绝回滚”，同时 Local RecordStore 会比较缓存有效期，避免旧记录覆盖新记录。
- **发布流程**：
  1. 构建各平台包及差分，计算哈希与 CID；
  2. 生成 manifest，并由多重签名或门限签名确认；
  3. 通过 GossipSub 发布 manifest；
  4. 更新 IPNS 指针，同时可写入 DHT 自定义 key 作为 fallback。
- **节点同步流程**：节点订阅 GossipSub 主题并验证签名 → 状态机转入下载流程 → Bitswap/GraphSync 拉取内容 → 验证哈希 → 将状态持久化，交给平台安装接口。

## 各终端后台更新策略
- **Android**：使用 `WorkManager` 周期唤醒代理服务；下载内容写入共享缓存并结合 `BandwidthManager` 节流；企业分发场景可调用 `PackageInstaller.Session` 静默安装，官方渠道集成 Play Core In-App Update。
- **鸿蒙 (HarmonyOS)**：在 `StageAbility` + `BackgroundTaskScheduler` 中轮询 manifest；调用 `SystemInstaller.installPackage` 安装 `.hap` 或 `.hpkg`；差分资源可同步到 `DistributedData` 目录以支持多设备协同。
- **苹果 (iOS/iPadOS)**：遵循苹果热更新限制（仅更新脚本/资源）；可借助企业证书或 App Store 官方流转；通过 `BGAppRefreshTask` 或静默推送触发代理，下载后替换热更新包或提示用户进入 App Store。

## 安全与运营治理
- **身份与加密**：启用 Noise 握手，结合 `peeridauth` 验证对端身份；manifest 引入多重签名与内容哈希链。
- **撤销与黑名单**：发布撤销列表 CID，客户端在处理 manifest 之前校验；`ConnManager` 与 GossipSub 黑名单剔除异常节点。
- **资源治理**：开启 `ResourceManager`、`BandwidthManager`，结合 `metrics_exporter`、`MetricsService`、`OtelMetricsService` 输出 IPNS、带宽、传输指标，纳入告警体系。

## 最佳实践：IPNS + 自定义 KadDHT Key
- **首选 IPNS**：通过 `_dnslink.<domain>` 绑定 IPNS Key，实现可读域名 → 最新 manifest 的映射；RecordStore 自动验证序列号与有效期，确保迟到节点不会回滚。
- **回退机制**：在 DHT 存储自定义 key（示例 `/unimaker/version/stable`）时，直接写入签名 manifest 作为备援。客户端解析失败或网络不支持 IPNS 时，可回退到该 key；同样依赖 `SignedEnvelope` 校验。
- **双轨策略**：应用启动时先解析 IPNS，失败再请求 fallback key；二者均获取成功时比较 `sequence`，择优采用高版本。

## 推进步骤
1. **PoC 阶段**：搭建最小网络（2 个发布节点 + 3 个消费节点 + Android 测试 App），验证 GossipSub 通知 + IPNS 指针 + 差分下载链路。
2. **跨平台适配**：扩展鸿蒙、iOS 代理，适配后台唤醒策略与安装 API，补齐签名与证书管理。
3. **生产部署**：建设 CI/CD，自动生成 manifest 与签名；搭建监控告警、密钥轮换、撤销流程。
4. **持续运营**：定期演练回滚与证书轮换，优化 GossipSub 参数、带宽调度和节点覆盖率。

## 跨平台一致性说明
- Manifest 在构建阶段应同时携带 Android、鸿蒙、iOS 三端产物（或差分），并写入同一渠道的记录。
- 任一平台的发布节点只要发布新的 manifest，其他平台的 Updater 守护都会订阅到同一条消息，通过 `platform` 字段筛选出自己的 artifact。
- 因此只要 CI/CD 保证三端包体同步产出，即便只有其中一个平台节点触发发布，三端也会自动收敛到最新序号，确保没有版本碎片。

## 代码资源
- `updater/` 目录提供 Nim 版本的更新代理示例，包括：
  - `config.nim`：JSON 配置解析；
  - `manifest.nim`：manifest 数据结构与 SignedEnvelope 支持；
  - `updater.nim`：GossipSub 订阅、DHT 拉取与处理回调（默认假设 IPNS/DHT key 直接存储签名 manifest，可在 `resolveIpns` 中扩展真实指针解析）；
  - `main.nim`：命令行入口，支持读取配置并输出 manifest 更新日志。
