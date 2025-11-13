# OpenSSL QUIC 适配推进记录

## 当前状态
- `openssl_quic.nim` 统一通过 `pollForQuicEvents` + `SSL_poll` 驱动监听器、连接、流事件；`nim_ssl_poll_item_*` 用于封装 `SSL_POLL_ITEM` 初始化/复用。
- 入站/出站流均基于事件掩码调度：
  - `Listener.accept`、`Connection.incomingStream` 依赖 `SSL_POLL_EVENT_IC/IS*`；
  - `Connection.openStream` 引入 `SSL_POLL_EVENT_OS*`，可在流控允许时非阻塞创建新流。
- `Stream.read/write/closeWrite` 使用统一的 `SSL_poll` 轮询逻辑（基于连接 `SSL` 驱动事件、针对具体流 `SSL` 轮询），`Stream.close` 以 `SSL_stream_conclude` 做优雅收尾。
- 针对 `SSL_ERROR_SSL` 增补 `SSL_get_stream_*_state/error_code` 映射，可辨别远端 RESET、方向错误等场景，并将 QUIC 应用错误码写入日志与 `LPStreamError`。
- 单测 `tests/testquic_handshake.nim` 通过，验证基本握手与双向流的事件链。

## 后续关注
1. 在 RPC/消息层消费新的 `LPStreamError` 信息（可通过 `quicDetails` 获取 state/app_error/stream_id 字段），为 UI 与日志提供更精准的提示。
2. 在 CI 环境执行完整 `nimble test`，重点关注 `tests/testall` 里的长耗时用例（`jitter`、`Autonat` 等）在新事件循环下是否稳定。
3. 对接移动端、WebTransport 等多流场景回归测试，确认在真实业务流量下无隐式阻塞或事件漏报。***
