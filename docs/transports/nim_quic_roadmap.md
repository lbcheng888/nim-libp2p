# 纯 Nim QUIC 长期路线图 / 里程碑

本文档面向 `nim-libp2p` 的长期演进：在已集成真实 MsQuic 的前提下，逐步补齐纯 Nim QUIC 数据面，作为跨平台可控、可裁剪、可审计的长期路线。

## 目标与边界

- 目标：在不牺牲安全性与互操作性的前提下，逐步构建纯 Nim QUIC + HTTP/3 + WebTransport 栈。
- 范围：传输层、拥塞控制、握手、流/数据报、WebTransport 会话层与联调工具链。
- 非目标：短期内取代 MsQuic 作为主生产路径；纯 Nim 作为长期选项与应急 fallback。

## 现状摘要

- 生产路径：MsQuic 为主，纯 Nim QUIC 仅作为逻辑回归/CI 辅助。
- 已具备：MsQuic transport + WebTransport 集成、证书与 certhash 轮换框架、DirectMessage/Feed 测试扩展。
- 缺口：纯 Nim QUIC 的完整传输栈、性能与互操作性验证、协议栈稳定性。

## 路线分期（阶段化交付）

### P0 - 规范与基线（基础设施期）

- 建立纯 Nim QUIC 设计蓝图与模块边界（协议核心 / TLS / 传输 / 事件）。
- 定义互操作性目标：与 MsQuic/Quiche/Quic-go/NgTcp2 的最小互联矩阵。
- 补齐基础测试工装：握手、流、数据报、连接关闭、重试。  

交付物：架构说明、接口契约、基础测试模板。

### P1 - 协议核心与握手（可连接期）

- QUIC 协议核心状态机（Conn/Stream/Packet/ACK/Loss）。
- TLS 1.3 集成（密钥派生、证书验证、0-RTT/1-RTT 切换）。
- ALPN/证书链/certhash 兼容。  

验收：
- 本地 loopback 与 MsQuic 互通。
- QUIC version/transport parameter 校验通过。

### P2 - 传输与拥塞控制（可用期）

- 拥塞控制：CUBIC / BBR（与 MsQuic 行为对齐）。
- 可靠性：丢包恢复、拥塞窗口收敛、重传策略。
- Datagram/0-RTT 基线支持。  

验收：
- P2P DirectMessage/Feed 基线跑通。
- RTT/吞吐与 MsQuic 在同量级。

### P3 - HTTP/3 / WebTransport（可业务化期）

- HTTP/3 控制流、SETTINGS、QPACK 基础。
- WebTransport 连接生命周期、会话限额与 datagram 语义。
- 与现有 `msquictransport` 接口一致的适配层。  

验收：
- WebTransport 握手、certhash 复用/轮换。
- 端到端 DM/直播数据通道可用。

### P4 - 生产级与观测（可落地期）

- 性能剖析：CPU/内存/IO 占用与峰值模型。
- 安全与稳定：fuzz/兼容性测试、协议异常处理。
- 可观测：连接质量指标、拥塞/丢包指标、端到端 Trace。  

验收：
- 多平台连续压测通过。
- 线上灰度与回滚机制完备。

## 互操作性矩阵（建议）

- MsQuic: QUIC + WebTransport + Datagram
- Quiche: QUIC + H3
- Quic-go: QUIC + H3
- ngtcp2: QUIC core

每一阶段必须通过至少两种实现互通验证。

## 近期推进项（落地建议）

1. 优先补齐 QUIC core + TLS 1.3 handshake 最小链路（P1）。
2. 抽象传输事件层，保证与 MsQuic 事件模型 1:1 映射。
3. 将现有 DirectMessage/Feed 测试基于纯 Nim QUIC 路径复用。

