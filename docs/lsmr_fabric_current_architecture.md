# LSMR/Fabric 当前实现快照

## 目标

这份文档只回答一件事：

**现在仓库里的 `LSMR + Fabric` 到底是怎么跑的。**

它不是设计幻想，也不是未来路线图，只记录当前已经落进代码的主路径。

## 总览

| 层 | 当前实现 |
|---|---|
| 控制面 | `LSMR` 证书原始安装 + 闭包提升，最后收敛成 `ActiveLsmrBook` |
| 数据面 | `Fabric` 通过 `DeliveryLedger + submit session` 做一跳可靠提交 |
| 业务推进 | 接收端先入 inbox，再由 `inbound drain + maintenance` 推进本地状态机 |
| 测试口 | `overlay / slow-connect / slow-ack / maintenance / inbound` 都是 fail-fast |

## 控制面

控制面算法以 [lsmr_certificate_convergence_algorithm.md](/Users/lbcheng/nim-libp2p/docs/lsmr_certificate_convergence_algorithm.md) 为准。

当前实现的硬规则：

| 规则 | 当前实现 |
|---|---|
| 原始证书与 active 视图分离 | 是 |
| `coordinate_sync` 只做 raw 快照安装 | 是 |
| `coordinate_publish` 只做原始材料增量推送 | 是 |
| active 提升走闭包 promote | 是 |
| 后台 revalidation 复用同一套 promote closure | 是 |
| active 变化后向同步邻居级联发布 | 是 |

当前这部分已经收口，启动期 `overlay` 可以收敛到 `4/4/4/4`。

## 数据面

数据面算法以 [fabric_lsmr_long_chain_algorithm.md](/Users/lbcheng/nim-libp2p/docs/fabric_lsmr_long_chain_algorithm.md) 为准。

### 1. DeliveryLedger

`FabricNode` 现在给三类消息都维护独立账本：

| 消息 | key | target key |
|---|---|
| `event` | `eventId` | `peerId|scopePath` |
| `attestation` | `eventId:role:signer` | `peerId` |
| `eventCertificate` | `eventId` | `peerId` |

每个 key 都有：

| 字段 | 含义 |
|---|---|
| `remainingTargets` | 还没送的一跳目标 |
| `inflightTargets` | 已经发出、正在等 ACK |
| `deliveredTargets` | 已收到一跳 ACK |

当前实现不再允许“本地入队即完成”。

### 2. submit 会话

`Fabric submit` 现在不是“每个 peer 一条串行流”，而是：

| 维度 | 当前实现 |
|---|---|
| peer 维度 | 每个 peer 独立维护 submit 会话 |
| lane 维度 | `event / attestation / other` 三条固定 lane |
| 会话状态 | `cold / warming / ready / broken` |
| 会话粒度 | `peerId|lane` |

当前热路径规则：

| 规则 | 当前实现 |
|---|---|
| 只有 `ready` 会话允许业务写入 | 是 |
| 非 `ready` 会话直接返回失败并保留 pending | 是 |
| repair 只能后台执行 | 是 |
| 热路径不做整链 fresh redial | 是 |

### 3. submit ACK 语义

当前 `/fabric/submit/1` 已经改成：

| 阶段 | 当前实现 |
|---|---|
| 收到 frame | decode 成 `FabricSubmitEnvelope` |
| ACK 前 | 先放进 submit inbox |
| ACK | 按 item 回 `FabricSubmitAckFrame` |
| 本地 apply | 由 inbox worker 后续推进，不阻塞 ACK |

也就是说：

**现在 ACK 只表示“远端 inbox 已接收”，不表示“远端业务处理完”。**

### 4. 每条 lane 的发送方式

每条 lane 当前是：

| 项目 | 当前实现 |
|---|---|
| 子流数量 | 1 条 submit 子流 |
| 并发窗口 | 有界 pipeline |
| ACK 对账 | 按 itemKey 对账 |
| 失败回滚 | 当前 target 从 `inflight` 回到 `remaining` |

这比早期的 stop-and-wait 好很多，但仍然是当前剩余瓶颈。

## 业务状态切换

当前 `FabricNode` 的关键规则：

| 场景 | 当前实现 |
|---|---|
| 新 event 构造 | 父前沿只从 `certifiedEvents()` 取 |
| 本地 `submitTx()` | 直接走真实入站推进，不再只注册本地 event |
| `registerAttestation()` | 已 certified 的 event 不再重灌 att pending |
| `refreshLsmrPending()` | 不再把全量历史 event/att/cert 重新灌回 pending |
| `registerEventCertificate()` 成功后 | 立即清该 event 的 event delivery 和 att delivery，只剩 cert 传播 |

## 测试口

当前主集成测试是 [fabric_network_lsmr.nim](/Users/lbcheng/nim-libp2p/tests/fabric_network_lsmr.nim)。

测试现在不是“傻等总超时”，而是主动盯病灶：

| 指标 | 当前阈值 |
|---|---|
| overlay 收敛 | `10s` 内必须 `4/4/4/4` |
| submit 慢连接 | fail-fast |
| submit 慢 ACK | `>= 2000ms` 直接 fail-fast |
| inbound 慢处理 | fail-fast |
| maintenance 慢处理 | fail-fast |

## 当前已经确认收掉的问题

| 已收掉的问题 | 当前状态 |
|---|---|
| 证书 latest-only 重校验把 active 链打坏 | 已修 |
| `Connected` 后盲目全表同步 | 已修 |
| relay scope 错传成 nextHop 自己路径 | 已修 |
| submit ACK 等远端业务处理 | 已修 |
| `event / attestation / cert` 共用一条 stop-and-wait runner | 已修成三条 lane |
| cert 后 event/att 还长期占住 backlog | 已修 |

## 当前还没收口的点

| 问题 | 当前状态 |
|---|---|
| 长链 `61 tx` 仍未完全收口 | 未修完 |
| `submit` 后半程 ACK 在高负载下仍会拉长到秒级 | 未修完 |
| 每条 lane 现在仍只有一条 submit 子流 | 未修完 |

## 下一刀应该砍哪里

| 方向 | 原因 |
|---|---|
| 每条 lane 改成固定大小的多子流池 | 一个慢 ACK 不能继续卡住整条 lane |
| 保持 ACK 只等 inbox 接收 | 这条语义不能回退 |
| 继续保留 fail-fast 测试口 | 不允许再回到“等 90 秒才知道挂了” |
