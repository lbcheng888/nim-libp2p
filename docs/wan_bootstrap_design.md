# nim-libp2p 广域网递进引导方案

## 目标

`nim-libp2p` 在广域网环境下的引导目标，已经收敛为默认不依赖公共静态引导节点，而是构建一个：

- 主路径尽量动态化
- 兜底入口尽量少
- 对节点失活、NAT、弱网和移动端更稳
- 不把少数前节点打成热点

的引导体系。

本文结合 `cheng-libp2p/docs/引导方案.md` 的思路，将其映射到 `nim-libp2p` 当前已有能力上：

- `DOD 记忆喷洒` -> 本地历史成功节点缓存
- `社交图谱注入` -> peer hints / 深链接 / 邀请地址
- `物理近场裂变` -> `MobileMeshService`
- `协议寄生` -> 显式配置时才启用 `dnsaddr + Rendezvous + Relay` 兜底

`BT Mainline DHT` 寄生不在 v1 落地范围内。

## 总体结构

广域网引导采用分层策略：

1. `DirectHint`
2. `BootstrapJournal`
3. `BootstrapSnapshot`
4. `Rendezvous`
5. `DnsaddrFallback (optional)`

其中：

- `DirectHint`：用户邀请、二维码、深链接、业务消息中附带的 `peerId + multiaddr` 或 `SignedPeerRecord`
- `BootstrapJournal`：本地历史成功节点缓存
- `BootstrapSnapshot`：已连接节点返回的候选快照
- `Rendezvous`：动态发现活节点
- `DnsaddrFallback`：仅在调用方显式配置时启用的少量锚点域名保险丝

`KadDHT` 不参与第一跳冷启动，只在成功接入后用于扩散和长期维持。当前实现已经支持 `enableKadAfterJoin`，在 `joinViaBootstrap()` 成功后自动对已挂载的 `KadDHT` 实例执行 `bootstrap(peers)`。

默认移动端 profile 现已进入 `seedless first-three` 模式：

- 不再默认注入 `bootstrap.libp2p.io`
- 第二个节点通过第一个节点的 `DirectHint` 接入
- 第三个节点通过第二个节点的 `DirectHint` 接入
- 第三个节点接入后，立即从第二个节点拉取 `BootstrapSnapshot`，补齐第一个节点
- 第四个及以后节点继续使用前节点池的 `1-2-7 / 3` 递进扩散

## 递进拓扑：1-2-7 / 3

引导拓扑采用递进扩散：

- 第二个节点优先通过第一个节点引导
- 第三个节点优先通过第二个节点，引导失败回退第一个节点
- 第四个及以后节点，从已知前节点池中随机选择最多 `7` 个候选

为了避免网络风暴，实际拨号并发固定为最多 `3` 个。

因此核心参数固定为：

- `maxBootstrapCandidates = 7`
- `maxConcurrentBootstrapDials = 3`

### 为什么不是 3 / 3

仅保留 `3` 个候选会让节点失活后的恢复能力太弱。一个候选池里只要有 2 个地址过期，就会显著拉低冷启动成功率。

### 为什么不是 7 / 7

候选池上限 `7` 是为了冗余；如果并发也直接放到 `7`，在移动端和小规模网络里容易造成前节点热点，甚至放大成瞬时连接洪峰。

### 设计结论

- `7` 用作抗失活候选池
- `3` 用作实际并发拨号上限

## 候选选择规则

第四个及以后节点按配额构造候选：

- `1` 个直接 hint 节点
- `1` 个普通 hint 节点
- `2` 个最近成功的稳定节点
- 其余名额从前 `28` 个高分候选中随机抽样补齐

候选优先级综合考虑：

- 最近成功时间
- hint 来源类型
- 是否为已连接 / 历史成功节点
- 地址数量

当前实现中，hint 来源分两类：

- 直接 hint：如 `manual`、`game_mesh_join_room`
- 基础设施 hint：如 `bootstrap`、`relay`、`mdns`

这意味着 v1 并不维护一个严格的“全网第 N 个节点”成员视图，而是通过“直接邀请优先 + 最近成功优先 + 随机补齐”来近似实现 1-2-7 递进扩散。

## 冷启动流程

1. 优先使用 `DirectHint` 和 `BootstrapJournal`
2. 使用 `1-2-7` 规则选择最多 `7` 个候选
3. 采用分波次拨号，每波最多 `3` 个
4. 任意建立 `1` 条稳定连接后停止前台 bootstrap
5. 连上后拉取上游 `BootstrapSnapshot`
6. 若配置了 `attestPeer`，在连接建立后先做业务侧信任校验；校验拒绝时立即断开并把该 peer 逐出当前候选集
7. 若调用方显式配置了 fallback 且本地候选不足或全部失败，则回退到 `dnsaddr + Rendezvous + Relay`

## 稳定连接判定

前台 bootstrap 的成功条件不是“所有候选都连上”，而是：

- 成功建立至少 `1` 条稳定连接

稳定连接需满足：

- 安全握手完成
- identify 成功
- 地址信息可回灌本地缓存

## 锚点职责

锚点不是长期唯一 bootnode，而是广域网保险丝。其职责固定为：

- 提供 `dnsaddr` 解析入口
- 挂载 `Rendezvous`
- 提供 `Relay Hop`
- 提供 `AutoNATv2 Server`

最小部署建议：

- 至少 `6` 个锚点
- 至少 `3` 个独立运维主体
- 至少 `2` 个不同 ASN / 云厂商

## 防热点 / 防风暴约束

本方案不会天然制造“全网风暴”，但如果不做限制，会把少数前节点打成热点。因此实现必须遵守：

- 候选上限固定 `7`
- 启动并发固定 `3`
- 任意建立 `1` 条稳定连接后立即停止前台 bootstrap
- 候选不全取头部节点，必须引入随机池抽样
- bootstrap 结果必须回写本地缓存，减少反复命中相同兜底入口
- 新接入节点不应立刻成为高优先 bootstrap source

## 当前仓库落地范围

当前已经落地两层实现：

- `examples/mobile_ffi/libnimlibp2p.nim`
  - 现有移动 FFI 引导路径已切到 `progressive_1_2_7`
  - 默认不再注入公共 `bootstrap.libp2p.io`
  - 仅在显式开启 `enablePublicBootstrap` 或显式传入 `bootstrapMultiaddrs/static_bootstrap/bootstrap_nodes` 时才启用静态 fallback
  - 启动拨号改为最多 `3` 并发的分波次拨号
  - 输出 JSON 返回 `selectionReason`、`candidateCap`、`dialConcurrency`
  - 记录 hint 来源，区分直接 hint 与基础设施 hint

- `libp2p/services/wanbootstrapservice.nim`
  - 提供通用 `WanBootstrapService`
  - 实现 `BootstrapJournal` 文件持久化
  - 实现 `BootstrapSnapshot` 自定义协议
  - 实现 `dnsaddr` fallback 解析与候选注入
  - 实现 `Rendezvous stable/daily namespace` 的广告与刷新
  - 提供 `attestPeer` 回调扩展点和 `rejected / quarantine / trusted` 信任分层
  - 支持 `enableKadAfterJoin`，在第一跳成功后自动触发已挂载 `KadDHT` 的 bootstrap
  - 提供 `bootstrapRoleConfig`、`joinViaBootstrap`、`bootstrapReconnect`
  - 提供 `bootstrapStateSnapshot()` / `bootstrapStateJson()` 状态接口
  - 记录 `refresh/join` 的内存统计与 metrics 计数

- `libp2p/builders.nim`
  - 提供 `withWanBootstrapService(...)`
  - 提供 `withWanBootstrapProfile(...)`
  - `anchor` profile 默认附加 `SignedPeerRecord`、`Relay Hop`、`AutoNATv2 Server`
  - `publicNode` profile 默认附加 `SignedPeerRecord`、`AutoNATv2 Client/Service`
  - `mobileEdge` profile 默认附加 `SignedPeerRecord`、`Relay Client`、`AutoNATv2 Client/Service`

## Role Profile 默认装配

`withWanBootstrapProfile(...)` 的目标不是替代业务层完整拓扑配置，而是把 WAN bootstrap 所需的最低公共件一次性接好：

- `anchor`
  - 开启 `WanBootstrapService`
  - 开启 `SignedPeerRecord`
  - 默认挂载 `Relay Hop`
  - 默认挂载 `AutoNATv2 Server`
  - `Rendezvous` 由 `WanBootstrapService` 根据 `mountRendezvousProtocol` 管理并应用 `stable/daily` namespace policy

- `publicNode`
  - 开启 `WanBootstrapService`
  - 开启 `SignedPeerRecord`
  - 默认挂载 `AutoNATv2 Client/Service`
  - 不默认挂载 relay server/client，避免普通节点无约束放大中继面

- `mobileEdge`
  - 开启 `WanBootstrapService`
  - 开启 `SignedPeerRecord`
  - 默认挂载 `AutoNATv2 Client/Service`
  - 默认挂载 `Relay Client`

如果调用方已经显式配置了 `Relay` 或 `AutoNATv2` 组件，profile 不会强行覆盖已有设置。

## 仍然保留为扩展点的部分

当前版本仍刻意保留为业务层或后续增强项的部分：

- `attestPeer` 的具体业务规则，例如质押证明 / 最终性证明校验
- 更细粒度的 `quarantine -> trusted` 升级插件和策略组合
- 更强的 journal 存储格式和大规模节点压测

## 可观测性

当前实现已经补上最小可用的 WAN bootstrap 可观测面：

- 状态接口
  - `bootstrapStateSnapshot()`：返回当前候选数、已连接 peer、最近一次 refresh 报告、最近一次 join 结果、累计 join/refresh 统计
  - `bootstrapStateJson()`：将同样的信息序列化成 JSON，方便直接暴露给调试接口或移动端 FFI

- metrics
  - `refresh_runs` / `imported_candidates`：按 `dnsaddr`、`rendezvous`、`snapshot`、`rendezvous_advertise` 分类计数
  - `join_runs` / `join_attempts`：按 stage 和结果区分统计
  - `attest_runs`：按 `trusted / quarantine / rejected / error` 分类计数
  - `kad_bootstrap_runs`：按 `success / partial / failure` 分类计数，并记录触发了多少个已挂载的 `KadDHT` 协议实例
  - `candidates` / `connected_peers` / `hint_peers`：当前 gauge

- 统计语义修正
  - `stopAfterConnected` 提前停止后，不再把未实际拨号的候选计入 `attemptedCount`
  - `selectedCandidates` 只表示“当前仍可用于引导的候选”；被 `attestPeer` 判为 `rejected` 的节点会从该集合中剔除，但仍会保留在 `lastJoinResult` 里用于排障
  - 因此当前 join 统计可以直接用来评估引导热点和连接风暴风险

## 测试覆盖

当前测试除了内存传输路径，还增加了更贴近真实环境的 TCP 集成场景：

- `stopAfterConnected = 1` 时，验证只记录真实拨号尝试
- `attestPeer` 拒绝已连接 bootstrap peer 时，验证立即断开且不再进入当前候选集
- `attestPeer` 提升节点为 `trusted` 时，验证信任状态和指标更新
- `enableKadAfterJoin` 打开时，验证第一跳成功后会触发已挂载 `KadDHT` 的 bootstrap
- `bootstrapStateSnapshot` / `bootstrapStateJson` 验证 join 与 refresh 统计回填
- TCP 多节点场景下，通过 `dnsaddr + Rendezvous + Snapshot` 让多个客户端并发接入并发现公共节点
- Memory 三节点场景下，验证“第 2 个走第 1 个、第 3 个走第 2 个，并从第 2 个的 `BootstrapSnapshot` 学到第 1 个”，全程不依赖静态 fallback
