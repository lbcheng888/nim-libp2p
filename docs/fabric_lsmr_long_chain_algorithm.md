# Fabric over LSMR 长链收口算法

## 结论

当前长链卡死的根因，不是 `overlay` 没收敛，也不是 `submit` 连接没建好，而是 **`/fabric/submit/1` 的 ACK 语义错了**。

现在的一跳 ACK 实际上被绑在远端本地业务处理完成上。这样一来，发送端虽然只想确认“下一跳已经收到了”，却被迫等待远端把 `event / attestation / certificate` 真正跑完本地状态机。长链里这会把第一批 batch 的 ACK 拖到秒级甚至十几秒，整条链自然被串行卡死。

理论正确的算法只有一条：

| 层级 | 正确语义 |
|---|---|
| 网络层 ACK | 只确认“该 item 已进入远端有界 inbox” |
| 业务层收敛 | 由远端自己的 `inbound drain + maintenance` 继续推进 |

也就是说：

**一跳交付确认** 和 **远端本地处理完成** 必须是两件事，绝不能混成一个 ACK。

## 目标

| 目标 | 定义 |
|---|---|
| 打通 `lsmrOnly` Fabric 长链 | `61 tx` 后 4 节点都拿到 `61 event / 61 cert`，observer 能查询到 61 条内容 |
| 去掉错误语义 | 禁止“本地入队即完成”，禁止“等待远端业务处理后才 ACK” |
| 关键路径固定化 | 不靠超时、不靠重试运气、不靠局部补丁 |

## 当前错误

| 当前做法 | 为什么错 |
|---|---|
| sender 把一跳 ACK 当成“远端已经处理完” | 这会把远端 CPU、队列、证书推进时延全部压回 sender |
| `/fabric/submit/1` handler 在 ACK 前 `await eventHandler / attestationHandler / eventCertificateHandler` | 这让 submit stream 退化成“远端本地状态机 RPC” |
| sender 看到 ACK 慢，就把长链推进速度绑定到第一批 batch | 第一批 batch 不回 ACK，后面就全堵住 |

## 核心原则

| 原则 | 规则 |
|---|---|
| 单一职责 | submit stream 只做可靠移交，不做远端业务收敛 |
| 严格确认 | `accepted=true` 只表示“进入远端 inbox”，不表示“远端已应用” |
| 有界背压 | inbox 满了就 `accepted=false`，不做静默吞包 |
| 无降级 | 不允许 fallback、兜底广播、启发式补发 |
| 单调推进 | 新 event 只挂 certified frontier；cert 成立后立即清理旧 event/att pending |

## 状态机

### 1. 发送端 DeliveryLedger

每类消息都维护独立账本。

| 类型 | key |
|---|---|
| event | `eventId` |
| attestation | `eventId:role:signer` |
| eventCertificate | `eventId` |

每个 key 都有一份 `DeliveryLedger`：

| 字段 | 含义 |
|---|---|
| `remainingTargets` | 还没送到的一跳目标 |
| `inflightTargets` | 已经发出、正在等 ACK 的目标 |
| `deliveredTargets` | 已收到一跳 ACK 的目标 |

目标 key 规则：

| 消息 | target key |
|---|---|
| event | `peerId|scopePath` |
| attestation | `peerId` |
| eventCertificate | `peerId` |

说明：

| 项目 | 规则 |
|---|---|
| event 需要保留 `scope` | 因为 event 是几何 relay，`peer + scope` 才是稳定目标 |
| att/cert 不带 `scope` | 它们是 validator fanout，一跳目标只按 peer 计 |

### 2. submit 会话机

每个 peer 一个 submit actor。

| 状态 | 含义 |
|---|---|
| `cold` | 没有 submit stream |
| `warming` | 正在建 `/fabric/submit/1` stream |
| `ready` | 允许业务写入 |
| `broken` | 当前 stream 坏了，必须 repair |

硬规则：

| 行为 | 规则 |
|---|---|
| 热路径写入 | 只允许写 `ready` 会话 |
| 非 `ready` | 立即返回失败，保留 pending，并触发后台 warm |
| repair | 只能由后台 warm runner 执行，不能在业务热路径里整链 redial |

### 3. 接收端 submit inbox

每个节点新增有界 `SubmitInbox`。

| 字段 | 含义 |
|---|---|
| `items` | 已收到但还没进业务状态机的 envelope |
| `capacity` | 固定上限 |

接收端 submit handler 的唯一职责：

1. 读 frame
2. decode envelope
3. 尝试 append 到 inbox
4. 立刻回 ACK

ACK 规则：

| 条件 | `accepted` |
|---|---|
| decode 成功且 inbox 有空间 | `true` |
| decode 失败 | `false` |
| inbox 满 | `false` |

**禁止在 ACK 前等待任何业务 handler。**

## 提交流程

### 1. event

发送端：

1. 用 certified frontier 构造新 event
2. 通过 LSMR 几何规则计算 `peer|scope`
3. 对每个目标执行：
   - `remaining -> inflight`
   - 推入该 peer 的 outbound actor
4. 收到 ACK 后：
   - `inflight -> delivered`
5. 所有目标都 `delivered` 后，才清该 event 的 pending

接收端：

1. submit handler 只把 envelope 放进 inbox
2. inbound worker 从 inbox 取出，转成 `InboundMessage(imEvent, relayScope=scopePrefix)`
3. `drainInbound + maintenance` 再走现有 `registerEvent / disseminateEvent`

### 2. attestation

发送端：

1. 只对 validator peers 计算目标
2. 每个目标只按 peer 做一跳确认
3. `delivered` 的定义仍然是“对方 inbox 已接收”

接收端：

1. submit handler 只收件入 inbox
2. inbound worker 再调用 `registerAttestation`
3. 如果 att 导致 cert 满足条件，证书推进由本地 maintenance 处理

### 3. eventCertificate

发送端：

1. cert 一旦本地成立，立即：
   - 清本 event 的 `pendingEvents`
   - 清本 event 的全部 `pendingAttestations`
2. 之后只传播 `eventCertificate`

接收端：

1. submit handler 只收件入 inbox
2. inbound worker 再调用 `registerEventCertificate`

## 公平性算法

每个 peer 的 outbound actor 使用固定轮转：

| 顺序 | 说明 |
|---|---|
| `event` | 第一优先级 |
| `attestation` | 第二优先级 |
| `eventCertificate` | 第三优先级 |

规则：

| 规则 | 说明 |
|---|---|
| 每轮只发一个类型的一个 batch | 避免某一类消息长期霸占 |
| batch 大小固定 | 不用动态启发式 |
| 轮次结束后切到下一类 | 保证 cert 不会永远被 event 压住 |

## 拓扑变化

`topologyChanged` 只做两件事：

| 动作 | 规则 |
|---|---|
| submit 预热 | 对新的 certified neighbors 建 warm |
| 重新计算 remainingTargets | 只重算还没 `delivered` 的目标 |

禁止：

| 禁止项 | 原因 |
|---|---|
| 重发已经 `delivered` 的一跳目标 | 会污染严格 ACK 语义 |
| 因拓扑变化清空整份 pending | 会丢掉严格账本 |

## 父前沿规则

新 event 的父节点只能来自 `certifiedEvents()` 的 frontier。

| 错误做法 | 正确做法 |
|---|---|
| 从 `knownEvents()` 混入未认证父事件 | 只从 certified frontier 选父 |

这样能保证：

| 性质 | 结果 |
|---|---|
| cert 链单调 | 后继 event 不会挂在未认证父上 |
| long chain 可验证 | 证书推进不会被历史脏前沿污染 |

## ACK 帧

统一使用逐 item ACK。

```text
FabricSubmitAckFrame {
  items: [
    { itemKey, accepted },
    ...
  ]
}
```

规则：

| 规则 | 说明 |
|---|---|
| batch 内逐 item ACK | 不能用单个 `"ok"` 代表整批成功 |
| sender 只认自己的 `itemKey` | 不允许按顺序猜 |
| 某个 item `accepted=false` | 只回滚该 item 的 target，不影响同批其它 item |

## 失败语义

| 失败点 | 处理 |
|---|---|
| stream 不 `ready` | 不写；保留 pending；触发 warm |
| write 异常 | 当前 target 从 `inflight` 回到 `remaining`；会话置 `broken`；后台 repair |
| ACK decode 失败 | 当前 batch 全部算失败；会话置 `broken`；后台 repair |
| inbox 满 | 接收端明确 `accepted=false`，sender 保留该 target |

说明：

**失败只能退回 `remaining`，不能假装成功。**

## 实现落点

| 文件 | 必改内容 |
|---|---|
| `libp2p/fabric/net/service.nim` | submit handler 改成“decode -> append inbox -> immediate ack”；新增有界 inbox；`submitFramePayloadAcked` 继续只认逐 item ACK |
| `libp2p/fabric/node.nim` | 新增从 submit inbox 到 `InboundMessage` 的桥；保留 `DeliveryLedger`；所有完成判定只由 ACK 或更高状态触发 |
| `tests/fabric_network_lsmr.nim` | 增加主动异常：batch ACK 超阈值、overlay 不健康、长链停在固定台阶时立即报错 |

## 验收标准

| 项目 | 标准 |
|---|---|
| overlay | `4/4/4/4` 在 10 秒内完成 |
| submit ACK | 单 batch ACK 必须回到毫秒级，不能再等远端业务处理 |
| 61 tx 长链 | 4 节点都达到 `61 event / 61 cert` |
| observer 查询 | 能看到 61 条内容 |
| backlog | 不允许长期卡在 `61/61/0` 或 `45/45/16` 这种固定台阶 |

## 一句话版本

**把 `/fabric/submit/1` 从“远端业务 RPC”改回“下一跳可靠移交协议”：ACK 只确认 inbox 接收，业务处理异步继续跑。这样长链才能真正并行推进，而不是被第一批 batch 串行拖死。**
