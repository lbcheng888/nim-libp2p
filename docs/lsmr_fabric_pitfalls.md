# LSMR/Fabric 踩过的坑

## 用法

这份文档只记录两类东西：

| 类别 | 定义 |
|---|---|
| 已经踩过的错路 | 以后不能再回去 |
| 当时为什么错 | 以后评审时直接对照这一列 |

## 控制面

| 错路 | 现象 | 正确做法 |
|---|---|---|
| 把 `coordinate_sync` 当成 `coordinate_publish` | publish 去重账本被污染，启动期证书卡在局部岛上 | `sync` 只拉 raw 快照，`publish` 才算 push |
| 收到一条证书就直接写 active | 深层证书先到时会错 active，后面父链到了又打架 | 先存 raw，再统一做闭包 promote |
| 后台 revalidation 走 latest-only | 安装阶段刚收好的 active 链被后台重校验删掉 | revalidation 必须复用同一套 promote closure |
| active 变化后只发给一小撮“已知有坐标”的 peer | 某些节点长期停在 `3/4` | active delta 必须向全部同步邻居级联 |
| 继续靠周期全表扫描修正启动期乱序 | 看起来偶尔能好，实际完全不稳定 | 事件驱动 + 单次 `sync -> publish` |

## 数据面 ACK 语义

| 错路 | 现象 | 正确做法 |
|---|---|---|
| submit ACK 等远端 `registerEvent/registerAttestation/registerEventCertificate` 跑完 | 第一批 batch 的 ACK 被拖到秒级甚至十几秒 | ACK 只确认“远端 inbox 已接收” |
| 一跳 ACK 和远端业务完成混成一个确认 | sender 被远端 CPU 和 maintenance 拖死 | 网络交付确认和业务收敛必须分层 |
| batch 只回一个总 `ok` | 批内部分成功/失败完全对不上账 | 必须逐 item ACK |
| 不校验 ACK 主键，只按顺序猜 | 长链时很容易串账 | 只按 `itemKey` 对账 |

## submit 会话机

| 错路 | 现象 | 正确做法 |
|---|---|---|
| broken submit stream 就当成 broken peer connection | 热路径一失败就整链 fresh dial，时延飙升 | stream 坏了只修 stream，不默认拆 peer 连接 |
| 热路径里做 repair/redial | submit 被锁死在秒级 | repair 只能后台做 |
| 会话没 ready 也硬写 | 失败语义变脏，pending 被提前清掉 | 只有 `ready` 才能写 |
| 只预热底层 peer connectivity，不预热 `/fabric/submit/1` | 第一笔真业务还要现开子流 | 必须预热真实 submit stream |

## 出站调度

| 错路 | 现象 | 正确做法 |
|---|---|---|
| `event / attestation / cert` 共用一条 stop-and-wait runner | event 会把 att 和 cert 压死 | 至少拆成固定 lane |
| 出站 runner 在 `not-ready` 后只等 maintenance 偶然再碰到 | 某条 lane 会长时间“停车” | submit ready 后必须主动重拉被卡住的 lane |
| 每轮按字符串重排 pending | 打乱原本的时钟顺序，后续推进异常 | 保持稳定顺序，不额外乱排 |
| cert 成立后还继续发旧 event/att | backlog 永远降不下来 | cert 成立后立即清 event/att delivery |

## 账本语义

| 错路 | 现象 | 正确做法 |
|---|---|---|
| 本地入队就算完成 | 网络还没送出去，pending 却被清掉 | 只有一跳 ACK 或更高状态才能清 |
| event 的 target key 只按 `eventId` 或只按 `peerId` | 不同 `scope` 会串在一起 | event 必须按 `peerId|scopePath` 对账 |
| topology 变化就整份 pending 重算重灌 | 会重复发已成功的一跳，账本失真 | 只重算没 `delivered` 的目标 |

## 业务状态机

| 错路 | 现象 | 正确做法 |
|---|---|---|
| 新 event 从 `knownEvents()` 混父前沿 | 会挂到未认证父事件上，证书链不单调 | 只从 `certifiedEvents()` 取父前沿 |
| `submitTx()` 只本地注册 event，靠后台维护慢慢补传播 | root 看起来有进展，其他节点长期没消息 | 本地提交必须直接走真实传播路径 |
| 已 certified 的 event 还继续接收并排 attestation | 长链后半段全是旧 att 噪音 | certified 后禁止再灌旧 att pending |

## 测试

| 错路 | 现象 | 正确做法 |
|---|---|---|
| 只盯总超时 | 90 秒后才知道挂，完全不知道第一现场 | 对 overlay、slow-connect、slow-ack、maintenance、inbound 都 fail-fast |
| 看到慢连接就认定是连接问题 | 经常误诊 | 先分清是 `connect` 慢还是 `ACK` 慢 |
| 只看 root 的进度 | 会误以为系统在推进 | 必须同时看 4 个节点的 `event/cert/content` |

## 一句话记忆

| 句子 | 含义 |
|---|---|
| ACK 只认下一跳收件，不认对方处理完 | 这是 Fabric 长链能跑起来的底线 |
| raw 和 active 必须分离 | 这是 LSMR 证书收敛能稳定的底线 |
| 账本只能被 ACK 或更高状态清理 | 这是“不乱试”的底线 |
