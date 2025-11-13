# Nim MsQuic Blueprint

该目录存放基于 MsQuic 现有 C 代码推导出的 Nim 端蓝图，目前覆盖 A1（协议核心建模）、A2（拥塞与恢复分析）、A3（TLS 与安全）、A4（平台抽象）、A5（API/FFI 分析）与 A6（工具与 CI）等原子任务的产出。

## 协议核心（A1）

- `core/connection_model.nim`：对齐 `src/core/connection.c` / `connection.h` 的连接状态与关键结构。
- `core/packet_model.nim`：映射 `src/core/packet.c` / `packet.h` 的数据结构。
- `core/frame_model.nim`：复刻 `src/core/frame.c` / `frame.h` 的帧类型与基本载荷结构。
- `core/stream_model.nim`：对应 `src/core/stream*.c` / `stream.h` 的流状态管理骨架。
- `core/flow_control_model.nim`：建模连接/流级流量控制窗口与允许运算。

## 协议核心增强（C1）

- `core/frame_model.nim`：补齐所有帧类型的载荷定义（STREAM、ACK、PATH_CHALLENGE、CONNECTION_CLOSE、DATAGRAM 等），提供 `framePayloadKind` 映射。
- `core/connection_model.nim`：扩展路径迁移、首选地址与 stateless reset token 状态，新增迁移控制 API。
- `core/flow_control_model.nim`：连接与流级流控初始化、允许运算及收发回调。
- `tests/test_flow_control.nim`、`tests/test_connection_migration.nim`、`tests/test_frame_catalog.nim`：验证流控、迁移与帧分类逻辑。

## 拥塞与恢复（A2）

- `congestion/common.nim`：汇总 BBR/CUBIC/丢包检测的常量、枚举与事件快照。
- `congestion/bbr_model.nim`：建模 `QUIC_CONGESTION_CONTROL_BBR` 及带宽滤波状态机。
- `congestion/cubic_model.nim`：建模 `QUIC_CONGESTION_CONTROL_CUBIC` 与 HyStart 状态。
- `congestion/loss_detection_model.nim`：抽象 `QUIC_LOSS_DETECTION` 的飞行队列与 PTO 参数。
- `congestion/ack_tracker_model.nim`：复刻 ACK 聚合与 ECN 统计逻辑。
- `congestion/README.md`：沉淀算法说明、性能基线数据与调参项清单。

## 平台抽象（A4）

- `platform/common.nim`：对齐 `src/inc/quic_datapath.h` 的枚举与常量。
- `platform/datapath_model.nim`：建模 `src/platform/platform_internal.h` 的数据路径公共结构。
- `platform/platform_matrix.nim`：梳理 `src/platform/datapath_*.c` 的平台能力、FFI 依赖与默认配置。

## TLS 与安全（A3）

- `tls/blueprint.nim`：总结 `src/core/crypto_tls.c`, `src/platform/tls_*.c`, `src/platform/crypt_*.c` 的 TLS 接入分层与平台差异。

## TLS 性能优化（D3）

- `tls/common.nim`：新增 `TlsSecretView` 与 `useSharedSessionCache` 配置入口。
- `tls/openssl_adapter.nim`：实现共享会话缓存、批量握手 flight 聚合、密钥零拷贝视图及可清空缓存的 `clearSharedSessionCache`。
- `tests/tls_handshake_test.nim`：补充密钥视图与共享缓存验证用例。

## API 与 FFI（A5）

- `api/common.nim`：梳理 `HQUIC` 句柄、API 函数分类、标志位及 Nim 侧 Draft 配置结构。
- `api/api_surface_model.nim`：给出 `QUIC_API_TABLE` 到 Nim 的映射表、C++ RAII 包装对应关系、兼容性边界以及 Nim 对外接口草案。

## API 与 FFI 扩展（C5）

- `api/event_model.nim`：统一连接事件枚举与载荷，支持 Nim 侧回调处理。
- `api/settings_model.nim`：提供 `QUIC_SETTINGS` 的 Nim 版 Overlay 及性能画像（低延迟/高吞吐/Scavenger）。
- `api/diagnostics_model.nim`：注册与触发 MsQuic API 诊断 hook。
- `api/param_catalog.nim`：常用 `QUIC_PARAM_*` ID 清单。
- `api/api_impl.nim`：扩展 `SetParam`/`GetParam` 支持流量调度、Datagram 开关、1-RTT 加密控制与设置应用，同时分发 Nim 事件与诊断条目。
- `tests/test_api_settings.nim`、`tests/test_api_events.nim`：验证设置应用、参数读写及连接事件派发。

## 工具链与 CI（A6）

- `tooling/common.nim`：定义脚本语言、构建工具链、制品类型等基础枚举。
- `tooling/build_matrix.nim`：整理 `CMakePresets.json` 与 `scripts/build*.ps1|.sh` 的构建流程及产物映射。
- `tooling/test_catalog.nim`：汇总 `src/test`, `scripts/test.ps1`、`scripts/run-gtest.ps1` 等测试资产。
- `tooling/ci_pipeline.nim`：描述 CI 发布、互操作、性能与文档流水线使用的关键脚本。
- `tooling/perf_ci_pipeline.nim`：聚合性能采集、阈值评估与报警配置。

## 发布验证（E1）

- `validation/protocol_validation.nim`：编排握手、互操作与模糊场景，生成发布前协议验证摘要。
- `validation/mod.nim`：导出验证套件供脚本或 CI 调用。
- `tests/protocol_validation_test.nim`：在 Nim 测试集中校验验证套件是否通过。
- 环境变量 `NIM_MSQUIC_SKIP_TLS` 默认为 `auto`，在缺少 OpenSSL 库时自动跳过 TLS 验证；设置为 `0` 可强制执行 TLS 测试。

## 发布验证（E4）

- `platform/release_validation.nim`：定义 Windows/Linux/FreeBSD/macOS 发布矩阵、Azure 作业映射与打包配方。
- `tests/platform_release_validation_test.nim`：确保平台场景覆盖核心操作系统并通过一致性检查。
- `tooling/build_matrix.nim`：扩展构建画像，补齐 FreeBSD Kqueue 发布配置与制品交叉验证。

## 运行集成（F 阶段）

- F1（FFI 底座）：`api/ffi_loader.nim` 负责动态加载官方 MsQuic C 库，解析 `QUIC_API_TABLE` 并提供 Nim 端访问入口。
- F2（拥塞桥接）：`api/congestion_bridge.nim` 将 Nim 侧拥塞策略映射到 `SetParam`/`GetParam`。
- F3（TLS 桥接）：`api/tls_bridge.nim` 把 Nim TLS 配置转换为 MsQuic `QUIC_CREDENTIAL_CONFIG`。
- F4（平台桥接）：`api/platform_bridge.nim` 处理 datapath、线程模型与绑定参数的下发。
- F5（API 运行时）：`api/runtime_bridge.nim` 接线事件回调与资源生命周期，驱动实际连接/流。
- F6（工具链支持）：`scripts/nim/bootstrap_msquic.sh` 等辅助脚本在本地/CI 检测并配置 MsQuic 运行时依赖。

## 运行 MsQuic FFI 底座

- 运行时需预先安装官方 MsQuic 动态库。可通过设置环境变量 `NIM_MSQUIC_LIB` 指向库的绝对路径，或将库放入系统默认搜索路径（Windows: `msquic.dll` / `msquic.lib`，Linux: `libmsquic.so`，macOS: `libmsquic.dylib`）。
- `api/ffi_loader.nim` 会优先尝试 `MsQuicOpenVersion`（默认请求版本 `0x00000002`），若符号缺失则回退到 `MsQuicOpen`，并在卸载时调用 `MsQuicClose` 释放 `QUIC_API_TABLE`；模板 `asQuicApiTable[T]` 可帮助将底层指针转换为目标结构类型。
- 脚本 `scripts/nim/bootstrap_msquic.sh env` 会在本地/CI 环境自动探测 MsQuic 库（必要时下载官方发布包），并输出可 `eval` 的 `export NIM_MSQUIC_LIB=...` 语句；`scripts/nim/validate.sh` 已默认调用该脚本，可通过 `NIM_MSQUIC_BOOTSTRAP_SKIP=1` 跳过。

## 最小可用实现 —— 工具链（B6）

- `project/nimsquic.nimble`：定义 Nim 项目的构建、测试、Lint 任务。
- `project/README.md`：记录 Nimble 任务与执行示例。
- `tests/test_newreno.nim`：使用 Nim `unittest` 框架验证 NewReno 拥塞控制。
- `../scripts/nim/{build,test,lint}.sh`：在 MsQuic 自动化中调用 Nimble 的脚本入口。

这些模块仅包含 Nim 语言层的类型与状态建模，为后续阶段的功能实现提供正交的基础模块划分。
