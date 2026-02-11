# CoinJoin + Onion D1 评审纪要

> 本模板用于记录 D1 评审会议过程与结论；会议结束后请立即填写，并将整改项同步至 `docs/D1_review.md` 与 `docs/dev_plan.md`。

## 1. 会议信息
- 日期：2025-03-18
- 时间：10:00-11:15 UTC+8
- 参会：Privacy WG（Alice）、DEX Core（Bob）、Bridge（Carol）、Security（Dave）、Ops（Eve）、Program Owner（Frank）
- 会议链接 / 录制：Meet #coinjoin-d1-review / 链接已存储在 Confluence（internal链接）

## 2. 议程回顾
1. 规范回顾（D1.1~D1.4）
2. 威胁模型与匿名性指标讨论
3. 测试矩阵与 CI 计划
4. D2 资源与时间线确认
5. 签字 / 待跟进事项

## 3. 关键结论
- 结论 1：D1 规范覆盖完整（角色/状态机/消息/Blame），可进入 D2；匿名性指标保持 N_min=5、N_target=10，并允许业务根据流动性动态上调。
- 结论 2：测试矩阵可落地，但要求在 D2.2 前准备 `tests/coinjoin/test_shuffle.nim`、`tests/coinjoin/test_signature.nim` 框架；Ops 将在 D2.5 引入 `coinjoin-fuzz`。
- 结论 3：安全团队要求在 D2.1 完成后提供经济惩罚与押金策略（补充到 `docs/dex_bridge_design.md`），作为 D1 整改项。

## 4. Rectify Items（按优先级）

| # | 描述 | Owner | 优先级 | ETA | 备注 |
| - | --- | --- | --- | --- | --- |
| 1 | 在 `docs/dex_bridge_design.md` 补充经济惩罚/押金策略，涵盖押金金额、罚没条件 | Program Owner + Privacy WG | High | 2025-03-25 | ✅ 2025-03-18 已添加“经济惩罚与押金策略”小节 |
| 2 | 准备 `tests/coinjoin/test_shuffle.nim`/`test_signature.nim` 骨架 | Crypto Core | Medium | 2025-03-24 | ✅ 2025-03-18 已提交占位测试文件 |
| 3 | Ops 在 `docs/metrics.md` 记录 coinjoin 指标需求（nonce reuse、partial fail、session abort） | Ops | Medium | 2025-03-26 | ✅ `docs/metrics.md` 新增 “CoinJoin 监控指标” |

> 完成后请在 `docs/D1_review.md` 第 10 节“R3 整改闭环”中引用本表格行号，并在 `docs/dev_plan.md` 更新状态。

## 5. Sign-off

| 角色 | 姓名 | 签字/时间 | 备注 |
| --- | --- | --- | --- |
| Privacy WG 代表 | Alice | 2025-03-18 11:10 | ✅ |
| DEX Core 代表 | Bob | 2025-03-18 11:11 | ✅ |
| Bridge 代表 | Carol | 2025-03-18 11:11 | ✅ |
| Security 代表 | Dave | 2025-03-18 11:12 | ✅ |
| Ops 代表 | Eve | 2025-03-18 11:12 | ✅ |
| Program Owner | Frank | 2025-03-18 11:13 | ✅ |

## 6. 附件
- Slides / 文档链接：Confluence `CoinJoin-D1-Review` (internal link)
- 录屏 / Chat 记录：Meet recording ID `coinjoin-d1-20250318`
