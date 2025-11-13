# Nim MsQuic Project Blueprint（B6）

该目录提供 Nim 版 MsQuic 的基础工程脚手架，覆盖 B6 原子任务要求：

- `nimsquic.nimble`：定义 Nim 2.2.6 依赖、源码路径（`../nim`）、以及 `build` / `test` / `lint` 三个 Nimble 任务。
  - `nimble build`：对核心模块（协议、平台、工具链）执行 `nim check`。
  - `nimble test`：默认运行 `nim/tests/test_newreno.nim`, `test_flow_control.nim`, `test_connection_migration.nim`, `test_frame_catalog.nim`，验证拥塞、流量控制、迁移与帧分类逻辑。API 行为相关的 `test_api_settings.nim`、`test_api_events.nim` 可通过 `nim r --path:../nim ../tests/<test>.nim` 单独执行。
  - `nimble lint`：运行 `nim check --styleCheck:hint` 做静态分析示例。
- 建议通过 `scripts/nim/*.sh` 在 CI 中调用 Nimble 任务，保持与现有 `scripts/` 目录一致的自动化风格。

执行示例：

```bash
cd nim/project
nimble build
nimble test
nimble lint
```

上述命令将编译校验 Nim 蓝图模块，运行单测并完成基础静态检查，为后续 B 阶段逐步补全实现提供最小流水线。
