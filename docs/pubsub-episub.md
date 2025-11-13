# Episub (Proximity Aware GossipSub)

- 协议目标：通过 HyParView 式主动/被动视图管理，为 GossipSub 构建低跳数、带宽友好的多播树。
- 控制通道：在订阅主题 `topic` 时，同时订阅 `topic & "/_episub"` 控制流，交换如下消息：
  * `JOIN`：请求加入主动视图，带 `ttl` 控制随机游走深度；
  * `NEIGHBOR`：接受请求的新邻居；
  * `FORWARDJOIN`：将新节点写入被动视图；
  * `GETNODES/NODES`：拉取最新被动视图。

## 使用方式
```nim
import libp2p/[switch, builders, protocols/pubsub/episub/episub]

let (sw, router) = SwitchBuilder
  .new()
  .buildWithEpisub(
    params = GossipSubParams.init(enableShardEncoding = true),
    epiParams = defaultParams()
  )

router.subscribe("/live/sports", handler)
```

- `EpisubParams` 可调整主动/被动视图大小、随机邻居数、GETNODES 返回数量、JOIN/ForwardJoin TTL、维护间隔以及 `inactiveTimeout`（自动剔除超时未响应的主动邻居）；`passiveEntryTtl` 控制被动邻居的寿命，超过该时长未收到更新会被自动清理；`passiveAgeHalfLife` 为被动邻居的指数衰减半衰期，越久未被选中的节点权重越低；`randomWalkLength` / `randomWalkFanout` / `randomWalkCooldown` 控制被动视图的按距离加权随机游走频率与扇出，用于持续刷新候选集合；
- 默认控制后缀 `/_episub`，可在参数中覆盖。
- `maintenanceInterval` 控制后台视图修复周期（默认 15s），周期内会轮询被动视图以补齐主动邻居；若主动邻居超过 `inactiveTimeout` 未刷新心跳，将触发 `DISCONNECT` 广播并把对端降级到被动视图。

## 当前能力
- 基于 `HashSet` 维护主动/被动邻居，记录每个候选与本地 `PeerId` 的 XOR 距离，容量溢出时淘汰最远节点，`lastHeard` 记录心跳时间；
- 被动视图抽样改为按距离加权采样，并结合 `passiveAgeHalfLife` 对长期未更新的候选做指数衰减，维护循环更倾向挑选最近、最新鲜的邻居补齐主动视图，使拓扑更贴近“近距离优先”的设计；
- 周期性维护时按 XOR 距离执行加权随机游走，请求多跳 `GETNODES` 以补齐低水位被动视图，默认每 30s 最多进行 3 步、每步 2 个邻居扇出，避免拓扑停滞；
- 订阅时自动发送 JOIN 与 GETNODES 请求，接收方按 TTL 随机转发或接受；接受 `NEIGHBOR` 后会把对端提升为 GossipSub mesh 节点；
- 通过 `promoteToMesh`/`removePeer` 将 HyParView 视图和 GossipSub mesh 同步，使发布路径可直接复用主动视图；
- 取消订阅时解绑控制处理器并清理局部视图，控制消息仍允许被动补偿。
- 后台维护循环会在 `passiveEntryTtl` 到期时移除被动邻居，并在响应 GETNODES 请求时按 XOR 距离加权采样且至多返回 `nodesResponseCount` 个候选，减少过期或海量节点导致的负载。
- 主动视图满载时会根据最近一次 `JOIN/NEIGHBOR` 或 `GETNODES/NODES` 往返时间与心跳滞后度量新旧邻居质量，只在候选明显优于当前最差邻居时替换，并向被驱逐节点发送 `DISCONNECT` 后降级至被动视图，保持连通性的同时优先保留低延迟、高活跃度的节点。

## 待完成项
- 与 go-libp2p episub 的互操作测试。
