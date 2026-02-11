# Nim MsQuic 示例项目（E5）

该示例实现 E5 原子任务中要求的 API 演示与语义校验，聚焦以下目标：

- 调用 `nim/tooling/sample_tool.nim` 提供的最小握手流程，演示 Nim 版 API 的典型用法。
- 同时覆盖基础握手与启用数据报通道的场景，验证与 C 版 `src/tools/sample` 的语义一致性。
- 作为迁移指南的配套示例，展示如何在 Nim 工程中引入 `nim/api` 与 `nim/tooling` 模块。

## 运行方式

```bash
nim r --path:../../ nim/project/sample_echo/src/main.nim
```

运行结果会打印客户端/服务端握手是否成功，以及数据报配置是否同步；若任一检查失败将以非零返回码退出，便于在 CI 场景中集成。
