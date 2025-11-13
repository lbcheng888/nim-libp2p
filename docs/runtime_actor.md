# libp2p Mobile Runtime Actor 设计说明

## 线程模型概览

- **Runtime Actor 线程**：由 `libp2p_node_init` 创建的专属线程，运行 Chronos dispatcher，串行处理所有命令与回调。只有该线程可以直接读写 `NimNode` 状态。
- **JNI Handler 线程**：Android/Harmony 侧在 `HandlerThread` 或 hvigor 线程中封装命令，通过 FFI 投递给 Runtime Actor，自身只负责排队和等待结果。
- **MsQuic / mDNS 后台线程**：外部库可能在自身线程触发事件，这些事件必须通过命令或 `asyncSpawn` 方式重新排队，让 Runtime Actor 执行状态变更。
- **其它入口线程**：调用 FFI 前必须执行 `nim_thread_attach`，只允许读取只读数据或提交命令，禁止跨线程直接访问 `NimNode`。

Runtime Actor 通过单线程事件循环和无锁命令队列运行，彻底移除互斥和条件变量。

## 命令队列与执行

1. JNI 侧创建 `Command`，填充参数后调用 `submitCommand`。
2. `submitCommand` 将命令插入基于 `Atomic` 的无锁链表，并通过 `ThreadSignal` 唤醒 Runtime Actor。
3. Runtime Actor 在事件循环中调用 `processCommandQueue` 逐个取出命令，并在 `runCommand` 中串行执行。
4. 命令完成后通过命令自带的 `ThreadSignal` 通知等待方，JNI 将结果回传上层。

命令队列只有 Runtime Actor 消费，任何状态修改都要通过命令进行。

## 命令分类

`CommandKind` 中定义了完整的命令集合，常见分类如下：

- **生命周期**：`cmdStart`、`cmdStop`、`cmdWaitSecureChannel`。
- **发布/订阅**：`cmdPublish`、`cmdSubscribe`、`cmdUnsubscribe`。
- **连接管理**：`cmdConnectPeer`、`cmdConnectMultiaddr`、`cmdDisconnectPeer`。
- **诊断/维护**：`cmdDiagnostics`、`cmdEmitManualEvent`、`cmdGetLanEndpoints`。
- **发现相关**：`cmdMdns*`、`cmdRendezvous*`、`cmdBoostConnectivity`。
- **传输扩展**：`cmdReserveOnRelay`、`cmdReserveOnAllRelays`（MsQuic）。
- **业务模块**：Feed、Livestream、LAN 组网等命令。

新增功能时必须新增命令并在 `runCommand` 中实现处理逻辑。

## JNI 交互约定

- FFI 导出函数只做参数转换与 `submitCommand` 调用，不得直接操作 `NimNode`。
- 需要异步返回的场景（例如 `poll_events`）在命令完成后返回 JSON/字节串。
- 所有 JNI 线程在调用 FFI 前需执行 `nim_thread_attach`；完成后视情况 `nim_thread_detach`。
- 错误通过 `resultCode`/`errorMsg` 回传，严重错误可通过 `libp2p_get_last_error` 读取。

## 回调回投

- MsQuic 监听/连接事件在 `watchMdns`、`reserveOnRelayAsync` 等流程中，使用命令或 `asyncSpawn` 将事件重新排队。
- mDNS 发现事件通过 `noteMdnsDiscovery` 记录并进一步触发命令。
- 所有需要上报 UI 的事件使用 `recordEvent` + `nim_bridge_emit_event` 机制，避免直接跨线程触发 Kotlin 回调。

## 生命周期

1. `libp2p_node_init`：解析配置 -> 构建 `Switch` -> 初始化命令队列和线程信号 -> 启动 Actor 线程。
2. `libp2p_node_start`：提交 `cmdStart`，启动传输、挂载服务、初始化 mDNS/MsQuic 等模块。
3. `libp2p_node_stop`：提交 `cmdStop`，关闭传输/服务并等待命令队列清空。
4. `libp2p_node_free`：确保停止后释放线程信号、队列和资源。

## 日志与监控

- Actor 线程日志以 `[nimlibp2p]` 开头，记录命令执行及 MsQuic/mDNS 状态。
- `cmdStart` 等关键路径会记录子步骤（传输启动、默认主题、mDNS 等），便于排查卡顿。
- JNI 侧通过 `NimNativeInterface` 打印调用链路并可将事件透传到 UI。

## 关闭与错误处理

- 每个命令完成后都会调用 `finalizeCommand` 通知等待方。
- `submitCommand` 在节点未运行时直接返回 `NimResultInvalidState`。
- `waitShutdown` 在等待超时时输出警告但不会强制终止线程。
- `runCommand` 捕获异常并将 `resultCode` 设为 `NimResultError`，同时记录日志。

## 编译时开关

- `-d:libp2p_actor_minimal`：启用最小运行时，仅支持 init/start/stop。
- `-d:libp2p_actor_no_msquic`：禁用 MsQuic 相关命令/传输，命令返回 `NimResultInvalidState`。
- `-d:libp2p_actor_no_mdns`：禁用 mDNS，命令/FFI 统一返回“未启用”状态。

通过这些开关可按模块逐步接入并验证，在较小范围内确认稳定后再扩展完整功能。
