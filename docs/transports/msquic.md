# MsQuic 传输栈运维备忘

本文档说明如何在 `nim-libp2p` 中启用并验证 MsQuic 数据面，包括构建选项、运行时依赖、常见测试与运维提示。

## 1. 启用方式

- 编译开关：`-d:libp2p_msquic_experimental`。  
  未开启时仅编译 OpenSSL QUIC 驱动，MsQuic 代码保持占位状态。
- 禁用旧驱动：启用 MsQuic 时请移除 `-d:libp2p_quic_support`，避免编译遗留的 OpenSSL QUIC 代码路径。
- 入口文件：`libp2p/transports/msquictransport.nim` 挂载 MsQuic-only 传输，`SwitchBuilder.withMsQuicTransport` / `newStandardSwitchBuilder` 默认走该实现。

## 2. 运行时依赖

### 2.1 原生 MsQuic 库

- 推荐按平台安装微软官方 `msquic` 库，或设置环境变量 `NIM_MSQUIC_LIB` 指向自定义路径。  
- 运行时会调用 `MsQuicOpenVersion`/`MsQuicClose` 获取 `QUIC_API_TABLE`，加载失败时会记录错误并回退。
- 仓库内的构建脚本（`scripts/build_nim_android.sh`、`scripts/build_nim_ohos.sh`、`scripts/build_nim_libp2p.sh`、`nim-libp2p/examples/build_*.sh`、`nim-libp2p/tests/build_*_mobile_tests.sh` 等）会自动执行 `nim-libp2p/scripts/nim/bootstrap_msquic.sh env`。该脚本会：
  - 检查 `NIM_MSQUIC_LIB` 是否已指向现有库；
  - 在常见系统路径（`/usr/lib`、Homebrew 等）搜索 `libmsquic`；
  - 若未找到且设置了 `MSQUIC_BOOTSTRAP_URL`，自动下载解压并设定 `NIM_MSQUIC_LIB`。
  使用者也可以直接独立运行 `scripts/nim/bootstrap_msquic.sh env`，然后 `eval` 其输出以持久化环境变量。

### 2.2 纯 Nim Fallback（开发 / CI 场景）

- 若未找到原生库，`libp2p/transports/nim-msquic/api/ffi_loader.nim` 会自动启用内置纯 Nim 实现（`api_impl.nim`）。  
- 该模式模拟 MsQuic 行为，主要用于单元测试和逻辑回归，不提供真实 QUIC 数据面性能。
- 切换回真实 MsQuic 时，无需额外配置，只需确保库存在即可。

## 3. 验证流程

在纯 Nim fallback 环境下，默认执行以下编译检查：

```bash
# 拨号生命周期（P4）
nim check -d:threads -d:libp2p_msquic_experimental tests/test_msquic_dial.nim

# 流与 Datagram（P3）
nim check -d:threads -d:libp2p_msquic_experimental tests/test_msquic_stream.nim

# 监听生命周期（P2）
nim check -d:threads -d:libp2p_msquic_experimental tests/test_msquic_listener.nim
```

若部署了原生 MsQuic，可将上面的 `nim check` 替换为 `nim c -r`，以执行实际事件流程。

## 4. 运维提示

1. **监控驱动切换**：拨号/监听失败时 `msquicdriver` 会自动落回 OpenSSL；可通过 `MsQuicDriverEnvVar` (`NIM_LIBP2P_QUIC_DRIVER`) 强制选择驱动并观察日志。
2. **调试事件队列**：连接、流、监听分别暴露 `nextConnectionEvent`、`nextStreamEvent`、`nextListenerEvent`，在 Fallback 模式下便于模拟与测试。
3. **Datagram 追踪**：`sendDatagram` 会触发 `ceParameterUpdated` 事件，可在日志中确认 MsQuic/回退模式是否正确执行。
4. **驱动指标**：启用 `metrics` 编译开关后，`libp2p_quic_active_driver`、`libp2p_quic_driver_switch_total` 与 `libp2p_quic_msquic_failures_total` 会分别标识当前使用的驱动、切换原因以及 MsQuic 失败阶段。

如需将 MsQuic 纳入 CI，请在 Runner 上预先填充原生库或设定 `NIM_MSQUIC_LIB` 路径，并执行 `nim c -r ...` 以覆盖真实场景。

## 5. OpenSSL → MsQuic 迁移任务矩阵

为了彻底移除 OpenSSL QUIC 数据面、仅保留 `nim-msquic`，需要一组正交原子任务。以下矩阵同步到仓库，用于跟踪重构状态：

| 编号 | 原子任务 | 状态 | 备注 |
| --- | --- | --- | --- |
| **A1** | 构建脚本默认启用 `-d:libp2p_msquic_experimental` 并下沉 MsQuic stub | ✅ | 所有主/示例/测试脚本均调用 `scripts/nim/bootstrap_msquic.sh env`，`scripts/build_nim_android.sh release` 已实机验证 |
| **A2** | 清理 `libssl/libcrypto` 链接、引入 MsQuic-only runtime loader | ⏳ | 需要结合 C1/W1 设计新的 loader 注入点与平台证书配置 |
| **B1.a** | MsQuic read-path 将 `StreamEvent` payload 交给 driver | ✅ | `StreamEvent` 已携带 MsQuic 缓冲并在 driver 中入队 |
| **B1.b** | 实现 `MsQuicStream`/`MsQuicConnection`（LPStream 接口、WebTransport/Datagram 集成） | ▶ | `msquicstream.nim`/`msquicconnection.nim` 初版就绪，待补 WebTransport/多路复用语义 |
| **B1.c** | `SwitchBuilder` 中暴露 MsQuic-only 入口，默认替换 OpenSSL | ✅ | `withMsQuicTransport` + `newStandardSwitchBuilder` 默认挂载 MsQuic |
| **B2** | 删除 `openssl_quic.nim` 在生产路径的引用 | ✅ | MsQuic 构建路径停用 `quictransport/openssl_quic`，`withQuicTransport` 仅作兼容包装 |
| **W1** | HTTP/3 SETTINGS 校验与 CONNECT 握手迁移到 MsQuic | ✅ | `msquictransport.nim` / `msquicconnection.nim` 已实现 MsQuic 控制流、SETTINGS 解析与 CONNECT 响应，拨号/监听路径默认执行 MsQuic WebTransport 握手 |
| **W2** | WebTransport 证书/certhash/轮换与 MsQuic 集成 | ✅ | `msquictransport.nim` 复用 libp2p X.509 生成器，`msquicdriver.loadCredential` 注入 MsQuic TLS 凭据，历史 certhash 与 `rotateCertificate` 均已与 MsQuic 对齐 |
| **W3** | HTTP/3 控制流/路径/Datagram 行为在 MsQuic 中实现 | ✅ | MsQuic 拨号/监听已完成 WebTransport 控制流、会话槽位与 Datagram 状态同步，`MsQuicConnection` 暴露会话快照 |
| **C1** | 对接 MsQuic TLS 抽象（Schannel/openssl_adapter/自带证书） | ✅ | `MsQuicTransport` 暴露 TLS override API，支持 Schannel hash/store、PKCS#12、现有 PEM 文件及运行时 Credential Flags |
| **C2** | 保持现有证书缓存与 WebTransport certhash 逻辑 | ✅ | 新增 `MsQuicTransport.loadWebtransportCerthashHistory`、`Switch.restoreWebtransportCerthashHistory` 与 `SwitchBuilder.withMsQuicWebtransportCerthashHistory`，`libnimlibp2p` 配置 JSON 支持 `msQuicCerthashHistory`，启动时自动回填 `/certhash/...` 广播并在拨号侧复用历史指纹，实现证书轮换后跨重启保留 |
| **D1** | DirectMessage/Feed/WebTransport 测试覆盖 MsQuic 连接 | ✅ | DirectMessage / Feed / WebTransport 三类集成测试已扩展至 MsQuic 路径 |
| **D2** | Diagnostics（`NetworkEventService`、`msquicwrapper`）输出 MsQuic 指标 | ✅ | MsQuic 连接/会话统计通过 `MsQuicStats` 网络事件与 blueprint session JSON 暴露，移动端 Diagnostics 面板可直接消费 |
| **E1** | Android/Harmony E2E 使用 MsQuic host 互通 | ✅ | 新增 `automated-tests/android-e2e-test/src/androidTest/java/com/example/unimaker/e2e/QuicDirectMessageSmokeTest.kt`，仪器化流程自动启动 MsQuic 节点、拨号 `quic_dm_host` 并完成 `PING/PONG` 往返，校验 `getNetworkDiagnostics()` 中的 MsQuic 摘要 |
| **E2** | QUIC smoke/CI pipeline 移除 OpenSSL host，提供回滚文档 | ✅ | `run_quic_dm_smoke.sh` 默认 MsQuic-only 构建/运行，`QUIC_DM_SMOKE_USE_OPENSSL=1` 可按需回退 |

> 执行说明：每完成一个原子任务，更新本表并在同一 PR 中附带验证日志（例如 `quic_dm_smoke`、`./gradlew connectedDebugAndroidTest`）。当 B1 + C1 + D1 就绪后即可切换默认驱动为 MsQuic-only，并删除 OpenSSL 相关代码。

### 5.1 B1 运行时驱动拆解

| 子任务 | 说明 | 状态 |
| --- | --- | --- |
| **B1.b1** MsQuic 数据面接口设计 | 设计 `MsQuicConnection`/`MsQuicStream` 与 `LPStream`/ResourceManager/WebTransport 的映射；定义错误语义与生命周期 | ✅（见“5.2 设计概述”） |
| **B1.b2** MsQuic 流实现 | 基于 `msquicdriver` 提供的 payload/事件，完成 `MsQuicStream` 读写、Datagram、Bandwidth 集成 | ▶（`libp2p/transports/msquicstream.nim` 新增 LPStream 实现，支持 read/write/close 与带宽统计，等待与 MsQuicConnection 对接） |
| **B1.b3** MsQuic 连接/监听实现 | 实现 `MsQuicConnection`、WebTransport/HTTP3 握手、Listener 接入；更新 `MsQuicTransport` 骨架 | ▶（设计完成，见“5.3 MsQuicConnection 设计”小节；正拆分实现步骤） |
| **B1.c** SwitchBuilder 接入 | 在 `SwitchBuilder`/`Switch` 中提供 MsQuic-only 构建路径；开放配置项 | ✅（`SwitchBuilder.withMsQuicTransport`/`newStandardSwitchBuilder` 已落地） |
| **W1** HTTP/3 / WebTransport 握手迁移 | 构建 MsQuic HTTP/3 control stream、注入 CONNECT 请求、验证 SETTINGS/DATAGRAM | ✅（MsQuic 控制流/CONNECT 握手已落地，后续任务聚焦 Datagram 与 Session 管理） |
| **W2** MsQuic 证书/certhash 集成 | 将 certhash 轮换、历史窗口、证书生成器迁移到 MsQuic | ✅ |
| **W3** MsQuic HTTP/3 数据通道 | WebTransport 控制流、Datagram、错误语义与 MsQuic 一致 | ✅ |

#### 当前进展

- 完成 `MsQuicStream`/`MsQuicConnection` 设计稿（B1.b1）：涵盖状态机、事件/错误映射、Bandwidth/ResourceManager 钩子、WebTransport certhash 更新等，见下节设计概述。
- 产出 `libp2p/transports/msquicstream.nim`（B1.b2）：实现 LPStream 接口、读写路径与带宽计费，等待 MsQuicConnection 调用。
- 新增 `libp2p/transports/msquicconnection.nim` + `msquictransport.nim` 拨号路径（B1.b3.1）：利用 msquicdriver 的事件队列构建 `MsQuicConnection`，`MsQuicTransport.dial` 默认尝试 MsQuic-only 连接，不再仅作为 OpenSSL 预热步骤。
- WebTransport 握手（W1）：`msquictransport.nim`/`msquicconnection.nim` 已接入 MsQuic HTTP/3 控制流逻辑，拨号与监听路径默认执行 SETTINGS 校验与 CONNECT 响应组装。
- WebTransport 证书与 certhash（W2）：`msquictransport.nim` 复用 libp2p X.509 生成器并通过 `msquicdriver.loadCredential` 注入 MsQuic TLS 凭据，`rotateCertificate` 支持历史窗口与 `/certhash` 列表同步。
- WebTransport 数据路径（W3）：`msquictransport.nim` 补齐会话槽位预留/释放、HTTP/3 控制流与 CONNECT 响应元数据、Datagram 队列管理，`msquicconnection.nim` 保存握手起止时间与会话 ID，监听/拨号快照均可反映 WebTransport 状态。
- TLS 重构（C1）：`msquictransport.nim` 新增 `configureTls`/`clearTlsOverrides` 等接口，并提供 `setTlsCertificatePem`、`setTlsCertificateFiles`、`setTlsCertificateHash`、`setTlsPkcs12File/Data`、`setTlsFlags`、`setTlsTempDir`、`setWebtransportCerthashOverride`，可在运行时选择 MsQuic/SChannel 证书来源或切换验证策略；`msquicdriver.loadCredential` 支持配置级临时目录；`api/tls_bridge.nim` 扩展 `TlsConfig` 与 `TlsCredentialBinding`，覆盖 HASH/HASH_STORE/PKCS12/CONTEXT 等 Credential 类型，测试 `api_tls_bridge_test.nim` 覆盖新场景。
- 诊断事件（D2）：`msquictransport.nim` 输出 `MsQuicTransportStats`/`MsQuicConnectionSnapshot`，
  `Switch.msquicTransportStats()` 汇总多监听实例，`libnimlibp2p.nim` 在启动/握手/诊断命令时
  发布 `MsQuicStats` 网络事件（含会话、Datagram、certhash、拒绝原因等），`msquicwrapper.nim`
  额外提供 blueprint session JSON，NetworkEventService 可直接用于 UI 展示 MsQuic 健康状态。
- QUIC smoke（E2）：`scripts/run_quic_dm_smoke.sh` 默认调用 `bootstrap_msquic` 并以 MsQuic-only
  构建主机程序；如需临时回滚，可设置 `QUIC_DM_SMOKE_USE_OPENSSL=1` 恢复旧版 OpenSSL +
  `libp2p_quic_support` 路径。
- 协议覆盖测试（D1）：`generateNodes` 支持自定义 `TransportType`/监听地址，`testdirectdm.nim` 与 `testfeed_service.nim` 新增 MsQuic 版本的 DirectMessage/Feed 用例，`test_msquic_webtransport.nim` 验证 MsQuic WebTransport 握手与会话快照，确保核心应用协议均可在 MsQuic-only 环境运行。
- 证书缓存（C2）：`MsQuicTransport.loadWebtransportCerthashHistory`、`Switch.restoreWebtransportCerthashHistory` 与 `SwitchBuilder.withMsQuicWebtransportCerthashHistory` 支持在节点重启时恢复历史 `/certhash` 指纹；`libnimlibp2p` JSON 配置新增 `msQuicCerthashHistory` 字段，启动阶段会自动写回广播地址并在拨号时复用提示。
- 移动端诊断（E1）：Android 仪器化测试 `QuicDirectMessageSmokeTest` 负责启动 MsQuic 节点、拨号 `quic_dm_host` 并完成 `PING/PONG` 往返，同时通过 `NimNativeInterface.getNetworkDiagnostics()` 校验 MsQuic 摘要（certhash 历史、transport 数量、会话指标）；Harmony `Libp2pService` 同步解析统计数据以供 UI 展示。
- `SwitchBuilder.withQuicTransport` 在启用 MsQuic 时退化为 `withMsQuicTransport`，移除对 `openssl_quic.nim` 的运行时依赖；`quictransport.nim` 在 MsQuic 模式下直接阻止编译旧驱动（B2）。
- `SwitchBuilder.withMsQuicTransport`（B1.c）提供 MsQuic-only 接入点；`newStandardSwitchBuilder` 在编译期检测 `-d:libp2p_msquic_experimental` 后默认加载 MsQuic 传输。

### 5.2 MsQuicStream / MsQuicConnection 设计概述

1. **连接生命周期**
   - `MsQuicConnection` 将 `msquicdriver` 的 `MsQuicConnectionState` 与 `HQUIC` 句柄封装到 libp2p `Connection` 中，统一管理：
     - `ceConnected/ceShutdown*` 事件对应的 `Connection` 状态转换；
     - WebTransport HTTP/3 SETTINGS 验证、certhash 历史维护、Datagram enable/disable；
     - 与 `BandwidthManager`、`ResourceManager` 的协作，确保新建流遵守全局配额。
   - 连接关闭需触发 `msruntime.shutdownConnection` 并回收所有流，避免 MsQuic handle 泄漏。

2. **流模型**
   - `MsQuicStream` 继承 `LPStream`，对接 `MsQuicStreamState` 队列：
     - `seReceive`：使用 `StreamEvent.payload`（已在 B1.a 实现）推入 `readQueue`，供 `readOnce/readExactly` 消费；
     - `seSendComplete`、`sePeerSendAbort` 等映射为 `LPStreamError`，保留 MsQuic 的 errorCode；
     - `seIdealSendBufferSize`、`seReceiveBufferNeeded` 针对 `BandwidthManager` 做反馈。
   - 写路径通过 `msruntime.sendStream`，并在 `MsQuicStreamState.pendingSends` 中追踪 outstanding buffer；完成或失败后更新 ResourceManager 计数。
   - Datagram 支持：连接层提供 `sendDatagram`/`incomingDatagram`，内部利用 `ceDatagram*` 事件维护状态和错误。

3. **WebTransport / HTTP/3**
   - MsQuic 监听端读取 HTTP/3 SETTINGS（`QUIC_HTTP3_SETTINGS`）并调用现有 `ensureSettingsCompat`；客户端将 `WebtransportHandshakeInfo` 注入新的控制流。
   - certhash/证书管理沿用 `QuicTransport` 逻辑，只是证书获取来自 MsQuic（支持 `setCertificateProvider` hook）。

4. **错误语义**
   - 统一将 MsQuic 的 `QUIC_STATUS`、`QUIC_STREAM_EVENT_*` 映射到 `LPStreamError`/`TransportError`，保留 `connectionErrorCode`、`streamId`、`appCloseInProgress` 等信息，方便 diagnostics。
   - 连接级别错误（例如 handshake failure、HTTP/3 拒绝）通过 `TransportDialError` 传递，并包含 MsQuic 状态码。

5. **SwitchBuilder 集成**
   - 新增 `SwitchBuilder.withMsQuicTransport(cfg)`，可定制 ALPN、ExecutionProfile 等 MsQuicTransportConfig 字段，并通过 `onTransport` hook 做额外注入；
   - 在 `Switch.start()` 时根据配置决定是否仅加载 MsQuic 驱动（禁用 OpenSSL），为 B1.c/B2 铺路。

### 5.3 MsQuicConnection 设计宏观方案（B1.b3）

| 子任务 | 说明 | 备注 |
| --- | --- | --- |
| **B1.b3.1** 拨号入口 | 在 `MsQuicTransport.dial` 内创建 MsQuic 连接句柄、注入 HTTP/3 / WebTransport 请求头、等待 `ceConnected`/`ceShutdown*` 事件；成功后创建 `MsQuicConnection` 对象 | ✅（`libp2p/transports/msquictransport.nim`/`msquicconnection.nim` 提供实验性单流拨号，后续可再叠加 WebTransport / HTTP3 语义） |
| **B1.b3.2** 监听入口 | `MsQuicTransport.start` 创建 MsQuic listener，监听 `leNewConnection` 事件，构建 `MsQuicConnection` 并交给 upgrader；支持多地址、WebTransport certhash | 替换 `openssl_quic.nim` listener 行为 |
| **B1.b3.3** WebTransport/Datagram 集成 | 将 HTTP/3 SETTINGS 验证、certhash、Datagram enable/disable 映射到 MsQuic 事件；与现有 `QuicTransport` 逻辑保持兼容 | 依赖 B1.b1 的错误语义与 certhash 设计 |
| **B1.b3.4** Resource/Bandwidth 管理 | 在连接/流生命周期内应用 `BandwidthManager`、`ResourceManager` 限额；支持关停时释放配额 | 与 `MsQuicStream` 共享接口 |

> 当前处于设计阶段（B1.b3 ▶）：上表给出了四个正交子任务，后续可以逐项实现并在矩阵中更新状态。

### 5.4 与 `nim-quic` 的融合方案

`nim-libp2p/libp2p/nim-quic/` 维护了一套独立的 QUIC 实现（协议栈、配置、测试齐备），若想与当前 MsQuic 路线融合，可按以下步骤推进：

| 子任务 | 说明 | 依赖 |
| --- | --- | --- |
| **Q1** 能力盘点与差异分析 | 罗列 `nim-quic` 已实现的协议（QUIC Crypto、Streams、HTTP/3）与 libp2p 需求（WebTransport、Noise/TLS、ResourceManager）；输出差异文档 | ✅（见“5.5 nim-quic vs MsQuic 差异总结”） |
| **Q2** API 对齐层设计 | 设计一个统一的 `QuicRuntime` 接口，抽象出连接/流、握手、证书、Datagram、metrics，供 MsQuic 与 nim-quic 双实现复用 | ✅（`docs/transports/quic_runtime.md` 设计稿，见下节摘要） |
| **Q3** 迁移/封装 nim-quic 模块 | 将 `nim-quic` 的 `quic.nim`、配置、事件模型以模块方式引入 `libp2p/transports`，与新 `QuicRuntime` 对齐；保留原项目的测试 | Q2 |
| **Q4** 互操作验证 | 使用 nim-quic 驱动跑通 libp2p WebTransport/DirectMessage/Feed 测试，与 MsQuic/OpenSSL 结果比对，确认可作为第二实现 | Q3、W1/W2 |

> 该方案与 MsQuic 路线不是互斥关系：MsQuic 侧重官方 QUIC 栈，而 nim-quic 可作为备用实现或实验场。推进 Q1~Q4 需要专门的设计评审与资源投入，建议在 B1/W1 形成稳定基础后启动。

### 5.5 nim-quic vs MsQuic 能力差异（Q1）

| 模块 | nim-quic 现状 | libp2p 需求 | 差异/备注 |
| --- | --- | --- | --- |
| 基础 QUIC 栈 | 自研 QUIC 协议实现（握手、crypto、streams）；可编译为纯 Nim 库 | libp2p 目前通过 OpenSSL QUIC 或 MsQuic 获取传输能力 | nim-quic 已有完整 QUIC core，可作为替代 runtime |
| HTTP/3 / H3 Datagrams | nim-quic 提供 HTTP/3 控制流与数据面（`quic/h3` 模块） | libp2p 依赖 HTTP/3 CONNECT + DATAGRAM 用于 WebTransport | MsQuic 已完成 W1/W3，对照 nim-quic 仍缺 WebTransport 握手/certhash |
| WebTransport | 无直接实现（需在 HTTP/3 基础上扩展） | libp2p `quictransport` 中已有 WebTransport 握手、certhash、session 限流 | 缺：WebTransport-specific handshake、certhash、会话管理 |
| Noise/TLS 升级 | nim-quic 以 QUIC crypto 自身握手，未与 libp2p Noise/TLS 交互 | libp2p 使用 Noise/TLS 作为安全层，QUIC 传输只负责底层 bytes | 需定义 `QuicRuntime` 以供 Noise/TLS 复用（Q2） |
| Resource/Bandwidth 管理 | 无 libp2p ResourceManager/BandwidthManager 钩子 | libp2p 需要 per-conn/stream 计费与限额 | 需在封装层增补 |
| Diagnostics/metrics | nim-quic 提供自身日志/统计 | libp2p 需要统一 metrics（WebTransport、driver switch 等） | 需在桥接层映射 |

> 结论：nim-quic 已具备 QUIC/H3 核心，可作为候选 runtime。但要用于 libp2p，需要在外层补齐 WebTransport、Noise/TLS 集成、ResourceManager/metrics 等功能。设计与实现应按 Q2–Q4 的顺序推进。

## 6. 移动端互通排障笔记（2024-11）

- `collectLanIpv4Addrs` 在 Android 上会回落到 `setMdnsInterface` 注入的首选 IPv4，日志标记为 `collectLanIpv4Addrs fallback<-preferred`，避免 `enumerateLanIpv4` 在受限环境下返回空列表导致 mDNS 广告缺失本地地址。
- `gatherLanEndpoints` 输出的 mDNS JSON 已统一为扁平 `multiaddr` 结构，Kotlin `LanEndpointDto` 不再因为缺失字段触发解码失败及后续的 `pthread_mutex_lock` FORTIFY 崩溃。
- MsQuic 拨号路径会根据多地址解析出的 host 选择 `QUIC_ADDRESS_FAMILY_INET/INET6`，日志可见 `MsQuic dial` 相关事件，修复 IP 字面量拨号因 DNS 解析失败触发的 `internalConnect` 错误。
- 结合 `adb logcat -s nim-libp2p,NimNativeInterface` 与 Harmony `NetworkEventService` 日志，可确认 Android/Harmony 互发现时会携带真实 `/ip4` 候选，MsQuic 拨号不再触发即时 `TransportDialError`。
- Android 侧若检测到华为/Harmony 设备的 Keystore 会触发 `pthread_mutex_lock` FORTIFY 终止，`Libp2pKeyManager` 会日志提示 `Keystore disabled; removing encrypted keypair` 并自动回落至 `PLAIN:` 存储，防止启动阶段的 SIGABRT。
