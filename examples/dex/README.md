# nim-libp2p DEX 示例

`dex_node.nim` 展示了如何使用 nim-libp2p 构建一个轻量级的订单广播、撮合与行情聚合流程。节点通过 GossipSub 主题交换订单、成交以及逐笔成交（`dex.orders` / `dex.matches` / `dex.trades`），新增的 K 线角色可基于逐笔数据生成多档 OHLCV 桶并写入 nim-pebble WAL。可选的混币演示会启用 `dex.mixers` 主题与 `/dex/mixer` HTTP route，在网络内周期性分发「CoinJoin 请求 → HTTP 参数交换 → Mixer 计划」日志。

## 编译

```bash
nim c --path:. examples/dex/dex_node.nim
```

## 运行角色

- `--mode=trader`：周期性生成随机订单并广播，同时监听成交回执。
- `--mode=matcher`：订阅订单消息，模拟做市商给出成交并广播匹配结果，同时将撮合结果转换为 `dex.trades` 逐笔行情流。
- `--mode=observer`：仅监听订单与成交，便于调试消息流。
- `--mode=kline`：订阅 `dex.trades` 主题，聚合 1s / 1m 等时间粒度的 K 线窗口并写入 `data-dir/kline/kline.wal`。

默认监听地址为 `/ip4/0.0.0.0/tcp/0`。可通过 `--listen` 指定多个地址：

```bash
./examples/dex/dex_node --mode=matcher --listen=/ip4/0.0.0.0/tcp/9001
```

节点之间可以用 `--peer` 传入完整的 multiaddress（包含 `/p2p/<PeerID>`）完成手动连接：

```bash
./examples/dex/dex_node --mode=trader \
  --peer=/ip4/127.0.0.1/tcp/9001/p2p/12D3KooWR... \
  --interval=3000 --asset=ETH/USDT --asset=BTC/USDT
```

更多参数：

- `--asset=<symbol>`：向资产列表追加条目，用于生成随机订单。
- `--interval=<ms>`：交易员模式下的订单间隔（毫秒）。
- `--data-dir=<path>`：指定本地持久化目录，默认写入 `dex_state/orders.wal`。示例使用 nim-pebble 的 WAL 记录所有订单/成交，并周期性输出资产敞口统计。
- `--key-path=<file>`：签名模式下的私钥文件（默认 `data-dir/node_ed25519.key`）。节点会自动生成 ed25519 密钥，并用其为所有订单/成交签名。
- `--allow-unsigned`：放行未签名的数据包，便于与旧版本互操作；默认强制校验签名并拒绝伪造消息。
- `--disable-signature`：完全关闭本地签名/验签逻辑，仅用于调试。
- `--limit=<asset>:<amount>`：为指定资产设置敞口上限，超过后直接丢弃新订单（可配置多次）。
- `--metrics-file=<path>` / `--metrics-interval=<n>`：将 `dex_invalid_*`、`dex_risk_drops_total`、`dex_expired_orders_total`、`dex_asset_exposure{asset,side}` 等指标按 n 次事件写入 Prometheus 文本，便于 node-exporter textfile collector、移动端遥测或 nim-pebble 旁路任务读取并上报。
- `--enable-trades[=true|false]`：控制 matcher 是否广播逐笔成交 (`dex.trades`)，默认 `true`。
- `--kline-scale=<seconds>`：为 K 线节点追加时间粒度（默认 1s、60s），可多次指定，例如 `--kline-scale=300`。
- `--kline-prune-minutes=<n>`：内存窗口保留时长，默认 60 分钟。
- `--enable-mixer[=true|false]`：开放混币 demo，节点会广播 `dex.mixers` 请求并在本地 `/dex/mixer` 路径上返回调度计划，默认关闭。
- `--mixer-interval=<ms>`：混币请求发送间隔，默认 15000ms。
- `--mixer-slots=<n>`：单次混币调度的 slot 数量，默认 3。

## 安全与风控提示

- 所有订单与撮合默认使用 ed25519 签名，`--allow-unsigned` 仅用于兼容旧节点，`--disable-signature` 只在调试环境启用。
- `--limit=<asset>:<amount>` 配置端上敞口上限，并结合 `OrderStore.purgeExpired()` 自动清理 TTL 超时订单，避免移动设备持续累积风险。
- 本地持久化通过 nim-pebble WAL 记录订单/撮合，重启后自动重放。建议放在移动端沙盒目录，并结合平台加密存储或文件权限防止被窃取。
- 指标文件中会包含 `dex_invalid_*`、`dex_risk_drops_total`、`dex_expired_orders_total` 与 `dex_asset_exposure{asset,side}` 等度量，可由 node-exporter textfile collector、移动端遥测 SDK 或自定义 nim-pebble 任务读取后推送到 Prometheus/Otel 管道。

### 快速演示（本地三角色）

```bash
# matcher：撮合 + 逐笔发布
./examples/dex/dex_node --mode=matcher --listen=/ip4/0.0.0.0/tcp/9001 --metrics-file=/tmp/matcher.prom

# trader：生成订单并连接 matcher
./examples/dex/dex_node --mode=trader --peer=/ip4/127.0.0.1/tcp/9001/p2p/<matcherPeerId>

# kline：订阅 dex.trades，聚合 1s/1m
./examples/dex/dex_node --mode=kline --peer=/ip4/127.0.0.1/tcp/9001/p2p/<matcherPeerId> \
  --kline-scale=1 --kline-scale=60 --metrics-file=/tmp/kline.prom
```

日志中可看到 matcher 输出 `[trade] publish ...`、kline 节点输出 `[kline/stats] buckets=...` 并在 `dex_state/kline/` 生成 WAL。`--metrics-file` 可直接被 Prometheus textfile collector 抓取。

启动多个实例后即可看到 GossipSub 主题上流转的订单、撮合以及 K 线聚合事件。示例聚焦网络交互与行情聚合逻辑，链上结算仍由 `examples/dex/settlement/coordinator.nim` 中的模拟器处理。

## 移动端运行提示

- 编译：iOS/Android 需使用 Nim 1.6+ 的交叉编译链，确保 `chronos`、`libp2p` 可针对目标平台构建。移动端通常通过 QUIC/WebTransport/Relay 出站拨号，记得启用相应 CLI 开关（见 `dex_node --help`）。
- 网络：弱网场景建议关闭 flood publish（`--gossipFlood=false`）并缩小 `--interval`；可通过 `--relay` 指定中继 multiaddr，或使用 `--transport=msquic`/`--transport=ws`。
- 日志：使用 `-d:release --verbosity:1` 等参数减少 I/O，避免拖慢移动端 CPU/电量。
- 身份缓存：配合 `network_identity.nim` 中的 `DexIdentityProtocol`，可在首次连接后缓存角色标签，断线重连时减少握手往返。
