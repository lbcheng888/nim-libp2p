# Bridge 生产级扩展方案

## 1. 已落地内容
- **模块化重构**：`bridge/config`, `model`, `services/coordinator|watcher|signer|executor`, `storage`；`bridge_node` 仅负责编排。
- **配置系统**：CLI/JSON 文件/环境变量三层合并，支持 `--config` 与 `BRIDGE_*` 覆盖，可通过 `--sig-threshold`、`--executor-interval`、`--executor-batch`、`--task-visibility` 等开关调优 Executor 与 Pebble 任务队列；StorageBackend 可切换。
- **事件模型**：schemaVersion + eventHash + 高度字段，Watcher/Signer/Executor 共享。
- **基础持久化**：内存与文件存储实现，统一 `EventStore` API。
- **服务层**：WatcherService 生成/广播事件；SignerService 验证并发布签名；ExecutorService 以 Pebble 任务队列为中心（enqueue/dequeue/ack/retry），按签名阈值驱动 finalize Hook。
- **Pebble KV 资源/Compaction**：`nim-pebble/pebble/kv.nim` 中暴露资源配额、WAL 同步与 Compaction Scheduler 接口，`nim-pebble/tests/test_kv_resources.nim` 验证限流与调度观测。（待验证）
- **Pebble KV 事务日志**：`examples/bridge/bridge/storage.nim` 提供事件/签名/任务 `WithLog` 变体与 `replayWrites`，结合 `withWriteBatch` 捕获原子写入日志并跨节点幂等重放；`nim-pebble/pebble/kv.nim` 新增 `encodeWrite/encodeWritesToJson|String` 与对应解码（Base64）便于日志分发，`tests/test_bridge_storage.nim` + `nim-pebble/tests/test_kv_basic.nim` 校验回放与序列化一致性。（✅ 已验证）

## 2. 待办与下一步
1. **配置系统扩展 (M0)**
   - [ ] YAML/JSON Schema 校验与嵌套结构；
   - [ ] 密钥/Signer/HSM 地址配置；
   - [ ] 多监听地址（TCP/QUIC）配置。
2. **WatcherService (M1)**
   - [ ] 链适配器（Mock/真实）输入；
   - [ ] Merkle/轻客户端验证与去重缓存；
   - [ ] 事件重放/断点恢复。
3. **SignerService (M2)**
   - [ ] 阈值策略与聚合上下文；
   - [ ] PeerScore/ACL 验证；
   - [ ] 远程签名器/HSM 集成。
4. **ExecutorService (M3)**
   - [x] 任务队列（执行、回执、重试）：基于 `EventStore.enqueue/dequeue/ack/retry` + `ExecutorService` 批量消费，实现幂等 finalize；
   - [ ] Gas/Nonce 策略 & 多链执行器接口；
   - [ ] 最终性确认与冲突处理。
5. **存储增强 (M1/M2)**
   - [ ] 集成 pebble/RocksDB，支持列族与批处理；
   - [ ] 异步任务用于签名聚合、垃圾回收。
6. **可观测/运维 (M3)**
   - [ ] 结构化日志（chronicles）、metrics、Admin RPC；
   - [ ] 运行状态查询与黑名单管理。
7. **测试矩阵 (M4)**
   - [ ] 单元、集成、仿真测试脚本；
   - [ ] CI/CD 流程与容器化部署。

## 3. Pebble KV 支撑能力（正交原子矩阵）

| 维度 \\ 阶段 | 设计 | 实现 | 验证 |
| --- | --- | --- | --- |
| **KV 基础读写** | 定义键空间（`event:*`, `sig:*`, `task:*`）与幂等策略；明确错误处理规范 | ✅ `examples/bridge/bridge/storage.nim` 接入 PebbleKV `get/put/applyBatch` 并落地事件与任务索引 `ksIndex` 读写（`EventStore` 持久化），完成基础 CRUD | CRUD 单元测试、并发写入、崩溃恢复 |
| **迭代与前缀扫描** | 规划前缀排序及快照语义；确定 Watcher/Executor 所需的遍历接口 | ✅ `nim-pebble/pebble/kv.nim` 提供 `scanPrefix`/`seek`/快照迭代器并支撑 `EventStore.scanPending` 前缀分页；`examples/bridge/bridge/storage.nim` 维护 `pending:` 索引与游标接口 | ✅ `nim-pebble/tests/test_kv_scan.nim` 覆盖 10K+ 前缀遍历与快照一致性；`tests/test_bridge_storage.nim` 验证分页游标（性能基准待补） |
| **批处理与事务** | ✅ 梳理 Watcher/Signer/Executor 原子写入需求（事件+索引、签名+计数、任务+索引）与幂等回放策略 | ✅ `examples/bridge/bridge/storage.nim` 增补 `putWithLog/appendSignatureWithLog/updateStatusWithLog`、任务队列 `enqueue/dequeue/ack/retry WithLog` 与 `replayWrites`，并在 `nim-pebble/pebble/kv.nim` 提供 `encodeWrite/encodeWritesToJson|String` & `decodeWritesFromJson|String`（Base64）以便跨节点传输 `KVWrite` 日志 | ✅ `nim-pebble/tests/test_kv_basic.nim` + `tests/test_bridge_storage.nim` 新增事件/任务日志重放与写日志 JSON roundtrip 用例，校验重复提交跳过、跨节点序列化一致性（混沌/WAL 压测待追加） |
| **任务队列能力** | ✅ 设计任务状态键（ready/running/done/failed）与 `TasksIndexKey` 顺序索引，定义可见性超时 `visibleAtMs`、重试计数 `attempts/maxAttempts` 及游标格式 `timestamp:id` | ✅ `examples/bridge/bridge/storage.nim` 扩展状态索引写入与 `scanTasks` 前缀扫描，`enqueue/dequeue/ack/retry` 事务统一维护 Pebble `ksTask` 与 `task:*` 前缀 | ✅ `tests/test_bridge_storage.nim` 新增“task status index tracks lifecycle transitions” 用例，验证 ready→running→done/failed 转换及索引回放一致性 |
| **资源与 Compaction** | ✅ 梳理 memtable/WAL/compaction 策略与监控指标，默认配置联动软/硬内存阈值、WAL `autoSync` 策略和 compaction scheduler 入口，运维侧可从 `stats` 拉取 mem/wal/compaction 观测数据 | ✅ 在 `nim-pebble/pebble/kv.nim` 实装资源配额、mem flush/compaction 触发与统计接口，`defaultPebbleKVConfig` 支持软硬限额、WAL 同步节奏以及 `setCompactionScheduler` 注入执行器 | ✅ `nim-pebble/tests/test_kv_resources.nim` 集成测试覆盖资源限额快照、WAL 路径与 compaction scheduler 监控字段，结合 `resource_manager` 验证限流与调度可观测性 |
| **运维工具链** | ✅ 梳理 dump/inspect/repair/backup 运维流程与 CLI 使用指引 | ✅ `pebble_cli` 新增 `kv dump/inspect/backup/repair/health` 子命令落地运维工具链 | ✅ `nim-pebble/tests/test_cli_ops.nim` 覆盖 dump/inspect/backup/repair/health 全流程 |

## 3. 当前风险
- FileStore 仅用于演示，不具备高并发/崩溃恢复能力；需尽快接入 pebble。
- Watcher/Signer/Executor 仍使用随机数据，未对接真实链适配器与阈值策略。
- Telemetry、Admin、测试体系仍缺失。
