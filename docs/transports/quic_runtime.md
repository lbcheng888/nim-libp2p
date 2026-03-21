# QuicRuntime 统一抽象（builtin/native 收敛版）

本文档记录 `nim-libp2p` 当前真正存在的 QUIC runtime 抽象。

当前仓库里的“纯 Nim QUIC”就是 builtin 的 `nim-msquic` runtime，也就是编译期内置的
MsQuic-compatible 实现；仓库内并没有单独接入的 `libp2p/nim-quic` 第二实现。

## 目标

- 给当前库内的两种运行形态提供统一运行时视图：
  - 编译期 builtin 的纯 Nim MsQuic-compatible runtime
  - 动态加载的原生 MsQuic runtime
- 让 transport / diagnostics / builder 配置共享同一层 runtime 契约，而不是各自散落在
  `msquictransport`、`msquicdriver`、`ffi_loader` 与 FFI 配置里。
- 为长期目标铺路：把 builtin 纯 Nim runtime 从“可诊断、可切换”继续推进到默认 backend，
  同时保留 native MsQuic 作为行为基线、性能基线和显式回退路径。

## 当前接口

这一版 `QuicRuntime` 已经收口了三层 façade：

- 执行 façade：统一暴露 `connect/listen/stream/datagram/close/shutdown`。
- 事件 façade：把 `nim-msquic/api/event_model` 映射成 runtime-neutral 的 `Quic*Event`。
- handler façade：把 `dial/listen/attach/create/adopt` 的回调统一成
  `QuicRuntime*Handler`，并保留调用方原始 `userContext`。

对应代码：

- [quicruntime.nim](/Users/lbcheng/nim-libp2p/libp2p/transports/quicruntime.nim)

导出的核心结构：

- `QuicRuntimeKind`
  - `qrkUnavailable`
  - `qrkMsQuicBuiltin`
  - `qrkMsQuicNative`
- `QuicRuntimePreference`
  - `qrpAuto`
  - `qrpNativeOnly`
  - `qrpBuiltinPreferred`
  - `qrpBuiltinOnly`
- `QuicRuntimeInfo`
  - `kind`
  - `implementation`
  - `path`
  - `requestedVersion`
  - `negotiatedVersion`
  - `compileTimeBuiltin`
  - `loaded`
- `QuicConnectionEventKind` / `QuicConnectionEvent`
- `QuicStreamEventKind` / `QuicStreamEvent`
- `QuicListenerEventKind` / `QuicListenerEvent`
- `QuicRuntimeConnectionHandler`
- `QuicRuntimeStreamHandler`
- `QuicRuntimeListenerHandler`
- `nextQuicConnectionEvent`
- `nextQuicStreamEvent`
- `nextQuicListenerEvent`
- `dialConnection`
- `createListener`
- `attachIncomingConnection`
- `attachIncomingConnectionAdopted`
- `createStream`
- `adoptStream`

## 长期策略

- `builtin` 是长期默认目标，但当前不是默认切换完成态。
- `native` 是长期保留的支持形态，职责包括：
  - 行为基线
  - 性能基线
  - 平台兼容路径
  - 显式回退路径
- `QuicRuntime` 的职责不是暴露两套上层接口，而是确保 transport、builder、diagnostics
  和测试入口对 backend 无感知。
- 本仓库不再把“未来第二实现”作为 `QuicRuntime` 的规划前提；长期路线只围绕
  builtin 与 native 两种现有形态展开。

## 当前接线点

- [msquictransport.nim](/Users/lbcheng/nim-libp2p/libp2p/transports/msquictransport.nim)
  运行时调用和监听/连接事件消费都通过 `quicruntime` façade 接线。
- [msquicconnection.nim](/Users/lbcheng/nim-libp2p/libp2p/transports/msquicconnection.nim)
  连接监控逻辑消费 `QuicConnectionEvent`，不再直接绑定 `msevents.ce*`。
- [test_quic_runtime_events.nim](/Users/lbcheng/nim-libp2p/tests/test_quic_runtime_events.nim)
  覆盖原始事件到 runtime-neutral 事件的映射。
- [test_quic_runtime_handlers.nim](/Users/lbcheng/nim-libp2p/tests/test_quic_runtime_handlers.nim)
  覆盖 runtime-neutral handler 的编译与最小 smoke。
- [test_quic_runtime_info.nim](/Users/lbcheng/nim-libp2p/tests/test_quic_runtime_info.nim)
  覆盖 runtime kind 元信息与 `isPureNimRuntime` 语义。

## 长期目标如何定义

长期目标不再定义成“再接入一个仓库里并不存在的独立 `nim-quic` runtime”，而是：

1. 继续把 builtin 纯 Nim runtime 补齐到完整 QUIC/HTTP3/WebTransport 数据面。
2. 在不改变上层 API 的前提下，把 builtin 推到默认 backend。
3. 让 native MsQuic 和 builtin 纯 Nim runtime 共享同一套 transport、事件、诊断、
   builder 和测试入口。
4. 让 native 始终作为行为/性能基线和显式回退路径保留。

## 下一步应推进什么

后续推进点聚焦 builtin 纯 Nim runtime 本身，而不是第二实现接线：

1. 协议核心完备化
   - 连接状态机、TLS 1.3、transport parameters、0/1-RTT、错误语义
2. 传输与性能
   - 丢包恢复、重传、拥塞控制、Datagram、吞吐/RTT 基线
3. 业务语义
   - HTTP/3 SETTINGS、QPACK 基础、WebTransport 生命周期与 session/datagram 语义
4. 生产硬化
   - metrics、diagnostics、fuzz、长压测、多平台 soak、灰度/回滚

默认切换标准见 [nim_quic_roadmap.md](/Users/lbcheng/nim-libp2p/docs/transports/nim_quic_roadmap.md)。

## 非目标

这一版不是把 builtin 纯 Nim runtime 直接宣告为生产完成。  
它完成的是 runtime 契约收敛，确保后续长期目标能沿现有 builtin/backend + native baseline
这条路线连续推进。
