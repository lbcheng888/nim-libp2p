# Nim 拥塞与恢复实现（C2）

本目录对应 `docs/nim_msquic_rewrite_matrix.md` 中 C2 原子任务：在保留既有蓝图的基础上，完成 BBR/CUBIC 等高级拥塞算法的 Nim 版本，并提供统一的策略切换与测试验证。

## 目录概览
- `common.nim`：整合常量、枚举、事件快照，并实现单调队列版 `SlidingWindowExtremum`。
- `bbr_model.nim`：落地 BBR 状态机、带宽滤波与 pacing/窗口更新逻辑。
- `cubic_model.nim`：实现 CUBIC 三次函数窗口演进、乘性减小与 PTO 回退。
- `controller.nim`：统一的策略切换入口，封装 BBR/CUBIC 的发送、ACK、丢包接口。
- `loss_detection_model.nim`：`QUIC_LOSS_DETECTION` 状态建模。
- `ack_tracker_model.nim`：ACK 聚合与 ECN 统计。
- `newreno_model.nim`：RFC 9002 NewReno 基线实现（B2）。
- `newreno_simulation.nim`：NewReno 仿真脚本（B2）。
- `mod.nim`：模块导出聚合。
- `tests/congestion_strategy_test.nim`：策略切换与窗口演进验证用例。

## C2 主要实现
- **滑动窗口极值**：`common.nim` 引入容量/生命周期控制的单调队列，支撑 BBR 带宽滤波与 ACK 聚合。
- **BBR**：`bbr_model.nim` 覆盖 STARTUP/DRAIN/PROBE_BW/PROBE_RTT 状态流转，基于采样带宽计算 BDP 及 pacing，保持最小窗口约束。
- **CUBIC**：`cubic_model.nim` 使用三次函数和 Reno 退火对照更新窗口，支持丢包乘性减小与 PTO 处理。
- **策略切换**：`controller.nim` 对外暴露统一接口，便于核心代码按配置在 BBR/CUBIC 间切换。
- **自动化验证**：`nimsquic.nimble` 将 `congestion_strategy_test.nim` 纳入 `nimble test`；用例覆盖 BBR 窗口增长、CUBIC 丢包回退与策略切换复位。

## 性能基线数据
| 项目 | 数值 | 来源 |
|------|------|------|
| 初始拥塞窗口 (`InitialWindowPackets`) | 10 包 | `quicdef.h` `QUIC_INITIAL_WINDOW_PACKETS` |
| BBR 高/低速判定 | 1.2 Mbps / 24 Mbps | `kLowPacingRateThresholdBytesPerSecond`, `kHighPacingRateThresholdBytesPerSecond` |
| BBR 增益参数 | `High=739/256`, `Drain=88/256`, `CwndGain=2` | `kHighGain`, `kDrainGain`, `kCwndGain` |
| STARTUP 退火 | 启发式增益 1.25，慢增回合上限 3 | `kStartupGrowthTarget`, `kStartupSlowGrowRoundLimit` |
| Probe RTT 限制 | 200 ms 最低拥塞窗口维持时间 | `kProbeRttTimeInUs` |
| Min RTT 过期时间 | 10 s | `kBbrMinRttExpirationInMicroSecs` |
| ACK 延迟默认上限 | 25 ms | `QUIC_TP_MAX_ACK_DELAY_DEFAULT` |
| 丢包重排序阈值 | 3 包 / RTT + RTT/8 | `QUIC_PACKET_REORDER_THRESHOLD`, `QUIC_TIME_REORDER_THRESHOLD` |
| 持续拥塞窗口 | 2 包 | `QUIC_PERSISTENT_CONGESTION_WINDOW_PACKETS` |

以上数值已在 `common.nim` 以常量形式暴露，可直接在 Nim 仿真或单测中引用，作为与 C 实现对比的性能基线。

## 调参项清单
1. `InitialWindowPackets`, `SendIdleTimeoutMs`、`PacingEnabled` 等全局设置，对应 `QUIC_SETTINGS`。
2. BBR 专属：`CwndGain`, `PacingGain`, `SendQuantum`, `ProbeRttTimeUs`，可通过 `{.pragma.}` 或配置文件注入。
3. CUBIC 专属：`HyStartEnabled`, `CWndSlowStartGrowthDivisor`, `ConservativeSlowStartRounds`，与 RFC 8312bis 对齐。
4. 丢包检测：`ProbeCount`、`PacketReorderThreshold`, `MinAckSendNumber`，影响 PTO 退避与 ACK 策略。
5. ACK 管理：`PacketTolerance`, `MaxAckDelayMs`, `AckDelayExponent` (Transport Parameter)，决定延迟 ACK 行为。

调参项均在蓝图中暴露为字段，结合 `docs/Settings.md`、`src/core/settings.c` 可进一步对接 Nim 侧配置入口。
