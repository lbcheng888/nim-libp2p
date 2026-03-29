# LSMR 证书收敛算法

## 目标

`LSMR` 的证书收敛只做一件事：让所有节点对同一组原始证书材料，经过同一套确定性步骤后，得到同一份 `ActiveLsmrBook`。

这里不允许：

- `sync` 当成 `publish`
- 收到一条证书就立刻 active
- 缺父链、缺 migration 时靠后续重试碰运气补齐
- 靠周期全表扫描修正启动期乱序

## 核心状态

| 层次 | 作用 |
|---|---|
| `peerAddresses` | 只解决“能连谁” |
| `LsmrChainBook` | 原始坐标证书链，只存不判 active |
| `LsmrMigrationBook` | 迁移链，只存不 promote |
| `LsmrIsolationBook` | 隔离证据，只存不 promote |
| `ActiveLsmrBook` | 经过完整父链、迁移链、见证链校验后的最终 active 视图 |

## 正确语义

| 操作 | 正确语义 |
|---|---|
| `coordinate_sync` | `pull`。只表示“我把当前原始材料快照给了你”。绝不计入该 peer 的 publish 去重账本。 |
| `coordinate_publish` | `push`。只表示“我向你增量推送了一批原始材料”。只有这一步才允许记 publish 去重账本。 |
| `store raw` | 把坐标证书放入 `LsmrChainBook/LsmrBook`，但不直接写 `ActiveLsmrBook`。 |
| `promote active` | 只在“父链已 active、migration 已安装、隔离证据已处理、签名和覆盖通过”后，才写 `ActiveLsmrBook`。 |

## 收敛状态机

### 1. 接收阶段

收到 `coordinate_sync` 或 `coordinate_publish` 后，只做三件事：

1. 安装 `peerAddresses`
2. 原样安装全部 `coordinateRecords` 到原始链
3. 原样安装全部 `migrationRecords` 和 `isolationEvidence`

这一步结束后，`raw` 和 `active` 还是分开的。

### 2. Promote 阶段

接收阶段结束后，进入统一 promote：

1. 把当前所有候选 peer 按 `certifiedDepth 升序 -> epoch 升序 -> peerId` 排序
2. 按顺序调用 `promoteValidatedCoordinateRecord`
3. 只要某次 promote 让 `ActiveLsmrBook` 发生变化，就继续下一轮
4. 直到一整轮没有任何 active 变化，收敛结束

这保证了：

- 根层先 active
- 父前缀先 active
- migration 先安装，再允许深层 active
- 深层证书不会因为先到而被错误当成最终 active

### 3. 重校验阶段

后台重校验必须和安装阶段走**同一套闭包 promote**，不能退回成“只拿 `LsmrBook` 的 latest 直接验一遍”。

否则会出现这种错误：

- 安装阶段先把同一 peer 的 `root -> deep` 链正确推进成 active
- 后台重校验却只拿 latest `deep` 去验
- 结果把刚收敛好的 active 又删掉

所以重校验的唯一正确做法是：

1. 从 `LsmrChainBook` 收集 peer
2. 对每个 peer 的整条证书链按同样的依赖顺序 promote
3. 收敛到固定点后结束

## 发布规则

| 触发条件 | 动作 |
|---|---|
| 本地 `ActiveLsmrBook` 真正发生变化 | 对当前全部已知 `sync peer` 排一次增量 `publish` |
| 仅收到 `sync` 快照 | 不记 publish 去重账本 |
| 仅原始链变化，但 active 没变 | 不触发级联发布 |

所以真正级联的是 **active delta**，不是原始快照；而且级联范围必须是**全部已知同步邻居**，不能只限“当前已经有坐标的那一小撮 peer”，否则新证书会卡在局部岛上。

## 失败原则

发现以下任一情况，必须立即暴露，不允许兜底：

- 深层证书缺父链
- 深层证书缺 migration
- 同 epoch 双位置
- 逆序 epoch
- 无效签名
- 覆盖不足

## 实现要求

| 文件 | 要求 |
|---|---|
| `libp2p/services/lsmrservice.nim` | `apply` 改成 `raw first, promote later` |
| `libp2p/services/lsmrservice.nim` | `coordinate_sync` 不得污染 publish 去重账本 |
| `libp2p/services/lsmrservice.nim` | topology 变化后只排增量 publish，不再把 sync 快照当作已发布 |
| `libp2p/services/lsmrservice.nim` | 重校验必须复用同一套 promote closure，不能 latest-only |
| `libp2p/services/lsmrservice.nim` | active delta 必须向全部已知 sync peer 级联 |
| `tests/testlsmr.nim` | 必须覆盖“迁移链在同一轮 sync 内到达时，深层证书能直接 active” |
| `tests/testlsmr.nim` | 必须覆盖“foreign rooted chain 在 sync/publish 两条路径都能直接收敛成 active” |
