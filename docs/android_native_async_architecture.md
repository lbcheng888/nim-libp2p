# Android Native 异步架构约束

本文档只定义一条产品级正确路径。以后 Android、Mac、远端产品节点都按这条路径实现，不再临时发明新流程。

## 1. 总目标

| 目标 | 说明 |
| --- | --- |
| 单机单常驻进程 | Android App 内只有一个常驻 native 节点，不再起短命请求进程 |
| 单拥有线程 | native 节点只允许一个 Kotlin 线程拥有，所有调用都排队进入 |
| 短 RPC | UI 发的是短请求，不直接卡在长握手、长等待、长读写上 |
| 事件流 | 状态变化和收到的消息通过事件流推给 UI，不靠文件、不靠轮询硬顶 |
| 发送优先 | 用户点击发送后，状态查询、列表刷新、诊断轮询必须让路 |

## 2. 组件边界

| 组件 | 职责 | 禁止事项 |
| --- | --- | --- |
| UI / ViewModel | 发短请求、显示状态、订阅事件 | 直接并发碰 native 节点 |
| BridgeFacade | 把 UI 请求翻译成短 RPC | 在这里做长阻塞轮询 |
| Native 节点 | 拥有真正的 libp2p/tsnet 状态机 | 被多个 Kotlin 线程同时调用 |
| tsnet / QUIC | 跑自己的控制面、中继、DM | 为了 UI 方便暴露文件交换或临时入口 |

## 3. 唯一允许的调用模型

| 步骤 | 规则 |
| --- | --- |
| 1 | UI 发起短 RPC，例如 `connect_exact_kickoff`、`send_with_ack_kickoff` |
| 2 | BridgeFacade 把请求投递到 NativeNodeDispatcher |
| 3 | native 节点立即返回 `accepted / rejected / busy` |
| 4 | 后续结果通过 `status` 快照和 `event` 事件流返回 |
| 5 | UI 只读快照和事件，不再直接持锁等结果 |
| 6 | 如果 UI 已拿到 seed addrs，必须随 `send_with_ack_kickoff` 一起下发，不能再先同步 `prepare_route` |

## 4. 线程约束

| 约束 | 说明 |
| --- | --- |
| NativeNodeDispatcher 必须单线程 | 这是 native 节点唯一拥有线程 |
| 只允许短任务进入 Dispatcher | 长耗时任务必须拆成 `kickoff + status/event` |
| 不允许多个 Kotlin 线程并发操作同一个 native handle | 不再依赖“全局大锁兜住一切” |
| UI 刷新、事件泵、状态查询不能抢发送关键路径 | 发送中的关键窗口必须优先 |

## 5. 状态模型

| 类别 | 正确做法 |
| --- | --- |
| `runtimeHealth` | 读快照 |
| `tailnetStatus` | 读快照 |
| `listenAddrs/dialableAddrs` | 读快照 |
| `connect_exact` | `kickoff + status` |
| `send_with_ack` | `kickoff + status` |
| `send_with_ack` + seed addrs | 直接把 `addresses/source` 带进 kickoff，由 native 异步任务完成 connectivity |
| DM 收件 | 走事件流 |

## 6. 发送关键窗口

| 阶段 | 要求 |
| --- | --- |
| `prepare_route` | 不允许再做同步 `connect_exact/wait_secure_channel` |
| `connect_exact_kickoff` | 只投递请求，不同步等连接完成 |
| `wait_secure_channel` | 只允许 native 异步任务内部做，不允许 Kotlin 发送链长等 |
| `send_with_ack_kickoff` | 只投递请求，不同步等 ACK |
| `wait_send_ack` | 后台刷新、社交同步、诊断轮询全部让路 |

## 7. 事件流约束

| 事件 | 含义 |
| --- | --- |
| `runtime.*` | 节点启动、停止、错误 |
| `tailnet.*` | `queued/starting/started/failed` |
| `dm.recv` | 收到 DM |
| `dm.ack` | ACK 到达 |
| `transport.path` | `relay/punched_direct/direct` 变化 |

要求：

| 约束 | 说明 |
| --- | --- |
| 事件流必须是网络/内存通道 | 不允许写文件通知 UI |
| 事件必须可重放快照 | 新订阅者先拿当前状态，再接增量事件 |
| UI 收到 `dm.recv` 后直接更新会话 | 不再靠“定时扫收件箱”当主路径 |

## 8. 当前产品主线

| 项 | 当前要求 |
| --- | --- |
| 控制端点 | `quic://64.176.84.12:9444/nim-tsnet-control-quic/v1` |
| 中继端点 | `quic://64.176.84.12:9446/nim-tsnet-relay-quic/v1` |
| Android 发送路径 | 生产 UI -> BridgeFacade -> native 节点 -> tsnet relay/direct -> Mac |
| 状态通知 | 产品节点事件流 + 快照 |

`443/udp` 以后可以单独做抗封锁入口，但不影响当前主线。

## 9. 禁止事项

| 禁止 | 原因 |
| --- | --- |
| 再引入文件交换 | 会制造额外状态漂移 |
| 再引入短命请求进程 | 会制造生命周期和端口竞争 |
| 在 UI 线程或 ViewModel 里直接做长 JNI 调用 | 会拖垮发送体验 |
| 用全局大锁包住长耗时连接和发送 | 会把状态查询和发送互相拖死 |
| 在 Kotlin 里先同步 `prepare_route/connect_exact/wait_secure_channel` 再发送 | 会把发送主路径重新拖回长阻塞 |
| 为绕开问题再开第二套协议路径 | 会让问题长期留在主线里 |

## 10. 迁移要求

| 阶段 | 必须完成的事 |
| --- | --- |
| 第一阶段 | 单拥有线程、发送关键窗口、只读缓存让路 |
| 第二阶段 | 统一 `kickoff + status + event`，去掉同步长 JNI 调用，`send_with_ack` 直接带 seed addrs |
| 第三阶段 | ViewModel 不再直接散落调用 `ChengLibp2pNative.*`，统一走 BridgeFacade |
| 第四阶段 | Android 生产 UI 与 Mac 双向 DM 全部只走这套路径 |
