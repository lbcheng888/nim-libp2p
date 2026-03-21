# Native + builtin QUIC 长期路线图 / 里程碑

本文档是 `nim-libp2p` QUIC 长期策略的唯一源文档。

当前仓库里的“纯 Nim QUIC”指的是 `libp2p/transports/nim-msquic` 的 builtin runtime，
也就是编译期内置的 MsQuic-compatible 纯 Nim 实现；仓库内并没有单独接入的
`libp2p/nim-quic` 第二实现。

文件名保留 `nim_quic_roadmap.md` 只是为了兼容现有引用。路线本身已经收敛为一套
`QuicRuntime` 契约下的两种 backend 形态：native MsQuic 与 builtin 纯 Nim runtime。

## 目标与边界

- 长期目标：把 builtin 纯 Nim runtime 提升为默认 QUIC backend。
- 保留目标：native MsQuic 持续保留为行为基线、性能基线、兼容路径和显式回退路径。
- 架构目标：上层 transport、builder、diagnostics、业务协议只依赖统一 `QuicRuntime`
  契约，不感知 backend 差异。
- 范围：传输层、握手、拥塞恢复、流/数据报、HTTP/3、WebTransport、诊断与回滚。
- 非目标：本路线不规划仓库外独立 `nim-quic` 接线，也不规划 quiche/quic-go/ngtcp2
  外部互操作矩阵。

## 长期运行策略

- `builtin` 是长期默认生产目标，但不是立刻切默认。
- `native` 在整个路线期内都必须继续可选、可测、可回退，不因为 builtin 进展而删除。
- 所有新增能力都优先落在 `QuicRuntime` 契约和 transport-neutral 测试矩阵上，避免为
  builtin 单独分叉上层接口。
- 行为与性能对照只以 native MsQuic 为唯一基线。

## 现状摘要

- 已有两种运行形态：
  - native MsQuic runtime
  - builtin 纯 Nim MsQuic-compatible runtime
- 已具备：
  - `QuicRuntime` 统一契约
  - runtime 选择策略与诊断字段
  - transport 接线、事件 façade、handler façade
- 主要缺口：
  - builtin runtime 的协议完整性
  - builtin 与 native 的业务语义对齐
  - builtin 的性能追平与生产硬化
  - builtin 成为默认 backend 的客观切换门槛

## 路线分期

### P0 - 契约冻结与基线统一

- 冻结 `QuicRuntime` 的 `connect/listen/stream/datagram/event/error` 语义。
- 冻结 runtime 选择、诊断字段、错误分类和 builder 配置含义。
- 建立 builtin/native 共用的 transport-neutral 测试与 smoke 基线。

验收：
- transport 层不再依赖 backend-specific 事件枚举或回调签名。
- builtin 与 native 共用同一组 builder/runtime preference 语义。
- 文档明确 builtin 是长期默认目标，native 是基线与回退。

### P1 - 协议核心与握手闭环

- builtin 补齐 QUIC 连接/流状态机、ACK/Loss、transport parameters。
- 补齐 TLS 1.3、ALPN、证书链、certhash、0/1-RTT 语义。
- 对齐 builtin 与 native 的错误语义、关闭语义和诊断字段。

验收：
- builtin 跑通 `connect/listen/stream/datagram` 最小闭环。
- QUIC version、transport parameter、握手错误、关闭错误的行为与 native 对齐。
- 现有 runtime-neutral 事件与 handler 测试在 builtin/native 语义上成立。

### P2 - HTTP/3 / WebTransport 业务语义对齐

- builtin 补齐 HTTP/3 控制流、SETTINGS、QPACK 基础。
- 补齐 WebTransport 生命周期、session 限额、datagram 语义。
- 保持与现有 `msquictransport` 业务接口兼容，不新增第二套上层接口。

验收：
- DirectMessage、Feed、WebTransport 现有测试在 builtin 上通过。
- certhash 复用/轮换、CONNECT/SETTINGS、datagram 开关与 native 行为一致。
- transport 层无需为 builtin 添加业务分支。

### P3 - 传输与性能追平

- builtin 补齐丢包恢复、重传、拥塞控制、Datagram 热路径和 0-RTT 基线。
- 建立 builtin 对 native 的固定性能对照项。
- 输出 CPU、内存、RTT、吞吐的基线报告。

验收：
- 内部 QUIC smoke、DM、Feed、WebTransport 基线稳定运行。
- builtin 的性能达到默认切换门槛。
- 性能回归可以通过指标或基准测试稳定发现。

### P4 - 生产硬化与默认切换

- 补齐 metrics、diagnostics、fuzz、长压测、多平台 soak。
- 定义灰度、回滚、事故诊断和强制回退文档。
- 在满足默认切换门槛后，将 builder/runtime 默认偏好切到 builtin。

验收：
- builtin 满足默认切换标准。
- native 仍保留显式选择入口和回退文档。
- 默认切换不会改变上层 API 和业务接线。

## 默认切换标准

- 功能门槛：
  - builtin 通过现有 QUIC transport、DirectMessage、Feed、WebTransport 测试矩阵。
  - builtin 与 native 共用同一套 `QuicRuntime` 契约，不引入上层特判。
- 稳定性门槛：
  - 长压测与 soak 中无系统性崩溃、死锁、资源泄漏、事件错序。
  - 关键错误场景都有 diagnostics 字段和回退手段。
- 性能门槛：
  - 握手成功率不低于 native。
  - p95 RTT 不高于 native 的 1.5x。
  - 吞吐不低于 native 的 70%。
  - CPU 与内存放大不高于 native 的 1.5x。
- 切换策略：
  - 仅切换默认偏好，不删除 native 路径。
  - native 继续作为行为/性能基线和显式回退选项存在。

## 近期推进项

1. 先完成 P1，把 builtin 的 QUIC core、TLS 1.3 和错误语义补成闭环。
2. 再完成 P2，把 HTTP/3 / WebTransport 业务语义拉齐到现有 transport 入口。
3. 然后进入 P3/P4，用 native 做唯一对照，推动 builtin 达到默认切换门槛。
