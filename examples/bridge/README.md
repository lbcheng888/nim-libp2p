# nim-libp2p Bridge 示例

该示例展示了 Watcher/Signer/Executor 拆分后的跨链桥节点骨架：

- `bridge/services/coordinator.nim`：封装 libp2p Switch、GossipSub、Peer 管理；
- `bridge/services/watcherservice.nim`：Watcher 生成事件、持久化并广播；
- `bridge/services/signerservice.nim`：Signer 验证事件并发布签名；
- `bridge/storage.nim`：提供内存与文件存储实现；
- `bridge/config.nim`：统一 CLI/默认配置，支持 `--storage-backend=file`/`pebble`，并暴露签名阈值、Executor 周期、任务重试等生产级开关。

## 编译与运行

```bash
nim c --path:. examples/bridge/bridge_node.nim
```

启动 Watcher/Signer/Executor：

```bash
./examples/bridge/bridge_node --mode=watcher --storage-backend=file --storage-path=/tmp/bridge
./examples/bridge/bridge_node --mode=signer --peer=/ip4/127.0.0.1/tcp/4001/p2p/<WatcherPeerID>
./examples/bridge/bridge_node --mode=executor --peer=/ip4/127.0.0.1/tcp/4001/p2p/<WatcherPeerID>
```

Watcher 会周期性生成带 schemaVersion/eventHash 的事件并存入 `storage_path/events/*.json`；Signer 通过 `SignerHooks` 验证事件并广播签名；Executor 会轮询 pending 事件并调用 hooks 完成 finalize。启用 `--enable-onion-demo` 后，Executor 还会基于当前 peers/配置构造 `libp2p/protocols/onionpay` 多跳路径并输出日志，验证 Onion packet 的封装/剥离是否成功。

### 运行时配置

常用 CLI / 环境变量：

| 功能 | CLI 参数 | 环境变量 | 说明 |
| --- | --- | --- | --- |
| 签名阈值 | `--sig-threshold=<n>` | `BRIDGE_SIG_THRESHOLD` | Executor 触发任务前所需最小签名数 |
| Executor 轮询间隔 | `--executor-interval=<ms>` | `BRIDGE_EXECUTOR_INTERVAL_MS` | 任务扫描/执行周期 |
| Executor 批量 | `--executor-batch=<n>` | `BRIDGE_EXECUTOR_BATCH` | 单次 `dequeueTasks` 数量 |
| 任务可见性超时 | `--task-visibility=<ms>` | `BRIDGE_TASK_VISIBILITY_MS` | 任务 lease 超时后重新入队 |
| 任务最大重试 | `--task-max-attempts=<n>` | `BRIDGE_TASK_MAX_ATTEMPTS` | 超出后标记失败 |
| 洋葱路径演示 | `--enable-onion-demo` / `--onion-hops=<n>` | `BRIDGE_ONION_DEMO` / `BRIDGE_ONION_HOPS` | Executor finalize 后构建 `onionpay` 包并记录 hop 路径，便于调试匿名转发 |

## BSC ⇆ BTC 测试网桥接

桥计划 (`examples/bridge/bridge_plan.md`) 中的跨链桥已按以下流程落地：

1. **BSC Watcher**  
   - `bridge/services/bscwatcher.nim` 通过 `eth_getLogs` 订阅 BSC 测试网 USDC (`0xE4140...AD0`) 的 `Transfer` 事件；
   - 目标地址（Vault）与地址映射在配置中声明。Watcher 将 BSC 发起地址映射为目标 BTC 地址，生成 `LockEventMessage` 并写入 `EventStore`；
  - `proofBlob` 携带 `bscTxHash`、`amountUsdc`、`btcAddress` 等信息，Executor 可直接用于兑付；同时生成 `proofDigest`（payload 的 SHA-256），供签名节点与 Executor 校验。

2. **Signer / Gossip**  
   - Watcher 广播事件后，Signer 仍按原逻辑验证并发布签名；
   - 事件达到 `signatureThreshold` 后，Executor 将事件写入任务队列。

3. **BTC Executor**  
   - `bridge/executor/btcsettlement.nim` implements `ExecutorHooks`，使用 BTC JSON-RPC (`sendtoaddress`) 将等值测试网 BTC 打到目标地址；
   - 兑换比例由 `btcUsdPrice`（默认 68,000）控制，可在配置或环境变量中覆盖；
   - 执行成功后 `EventStore` 状态置为 `esExecuted`。

### 配置示例

参见 `examples/bridge/bridge_config_bsc_btc.json`，关键字段：

| 字段 | 说明 |
| --- | --- |
| `bscRpcUrl` | BSC 测试网 RPC 入口 |
| `bscUsdcContract` | ERC-20 USDC 合约地址 |
| `bscVaultAddress` | 监听的 USDC 接收地址（桥保管地址） |
| `addressMappings` | BSC 地址与 BTC 地址映射，Watcher 会用来推导兑付地址 |
| `btcRpcUrl` / `btcRpcUser` / `btcRpcPassword` | BTC Testnet JSON-RPC 访问凭据 |
| `btcUsdPrice` | 1 BTC = ? USD，用于 USDC → BTC 换算 |
| `minBridgeAmount` | 过滤小额请求（单位 USDC） |

可以为三类节点分别加载该配置并指定模式：

```bash
# Watcher：监听 BSC，生成事件
./examples/bridge/bridge_node --config=examples/bridge/bridge_config_bsc_btc.json --mode=watcher

# Signer：连接 Watcher，签名事件
./examples/bridge/bridge_node --config=examples/bridge/bridge_config_bsc_btc.json --mode=signer --peer=<watcher-multiaddr>

# Executor：读取签名，兑付 BTC
./examples/bridge/bridge_node --config=examples/bridge/bridge_config_bsc_btc.json --mode=executor --peer=<watcher-multiaddr>
```

> 注意：当前 Executor 需使用 `file`/`memory` 存储，Pebble 后端将另行适配。

### 运行指引

1. 为需要桥接的 BSC 地址准备目标 BTC-t 测试地址，在 `addressMappings` 中登记；
2. 将 USDC（BSC testnet）转入 Vault 地址（配置中的 `bscVaultAddress`）；
3. Watcher 发现 `Transfer` 日志后在 libp2p 网络中广播事件；
4. Signer 达到阈值后，Executor 读取事件并调用 BTC RPC `sendtoaddress`，完成兑付；
5. `node.executor` 会在新的事件或签名到达时触发 `processOnce()`，因此无需额外的轮询线程。

## TODO
- YAML/JSON 配置加载、远程 signer/HSM；
- 链适配器轻客户端验证与事件缓存；
- 阈值签名、PeerScore/ACL、安全策略；
- Pebble/RocksDB 持久化、异步任务；
- Telemetry/metrics/Admin 接口；
- 单元/集成/仿真测试与 CI。
