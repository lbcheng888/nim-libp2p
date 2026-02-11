## MsQuic API 热路径优化策略（D5）

### 优化目标
- 将 `MsQuic` 高频 API（`SetContext`/`GetContext`/`SetCallbackHandler`/Datagram 参数族）在 Nim 端降到最小调用开销，确保 `{.noGc.}` 与 `{.inline.}` 的使用场景安全。
- 暴露可直接从 `src/bin`、`src/plugins` 访问的 C Shim，减少二次封装层参与。
- 提供可复现的热路径基准脚本与兼容性校验样例。

### 主要改动
- 引入 `quicApiHot` pragma 及 `getHandleFast/connectionFromHandleFast` 等快速句柄转换，`SetContext`/`GetContext`/`SetCallbackHandler` 走零分配路径。
- 将 Datagram 参数的事件与诊断拼接改为常量字面值，避免热路径字符串拼装。
- 新增 `MsQuic*Shim`（上下文与 Datagram 开关/查询）以面向 C 消费者暴露稳定符号。
- 拓展 `nim/tests/api_hotpath_test.nim` 代替 C 客户端验证：确认 Shim 与函数表互通、Datagram 更新事件与诊断钩子保持一致。
- 编写 `nim/tests/api_hotpath_bench.nim`，针对 `SetContext` 与 Datagram 快路径/通用路径建立计时循环。

### 基准脚本说明
- 路径：`nim/tests/api_hotpath_bench.nim`
- 运行方式：`nim c -r nim/tests/api_hotpath_bench.nim`
- 当前阻碍：由于 Nim 2 默认关闭指针算术且 `packet_builder_impl` 仍使用 `quicHotPath` 的 `raises: []` 约束，脚本在编译阶段会提示未列出的异常及指针偏移支持，请在合入 `ptrOffset` 适配并放宽对应调用栈后运行获取 `ns/op` 结果。

### 兼容性验证
- 测试样例：`nim/tests/api_hotpath_test.nim`
  - 校验 `MsQuicSetContextShim` 与函数表互通。
  - 切换 Datagram 收发开关并验证连接事件队列与诊断钩子均产生 `receive=true/false`、`send=true/false`。
  - 确认 `MsQuicGetDatagram*Shim` 返回 1 字节规范化标志，满足 C 侧结构体布局。

### 后续跟进
- 在 `perf_hotpaths` 脚本可运行后补充基准结果记录，并将 `MsQuicEnableDatagram*Shim` 接入 `src/bin` 插件演示。
- 继续排查 `quicHotPath` 与异常声明的冲突，确保 Nim 2 工具链下的热路径实现可全量编译。
