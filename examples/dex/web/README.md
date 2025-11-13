# TradingView K 线面板

`tradingview_dashboard.html` 是一个零依赖的前端示例，展示如何用 TradingView Lightweight Charts 构建专业 K 线与交易 UI。核心特性：

- 首次加载使用 Binance REST API 回填历史数据，随后自动接入 Binance WebSocket（Ticker、Kline、Depth、Trades）实现低延迟推送。
- TradingView Lightweight Charts 蜡烛 + 量能图，支持秒级刷新、多个周期（1m/5m/15m/30m/1h/4h/1d）切换。
- 交易票据、盘口列表、实时成交与“模拟撮合订单”面板，覆盖典型交易终端组件。
- 买卖表单通过内置 Demo Matching API 模拟真实撮合，返回订单编号、成交状态与均价，方便端到端联调。

## 使用方式

1. 直接在本地用浏览器打开 `tradingview_dashboard.html`（无需编译或启动后端）。
2. 确保浏览器可访问 `https://api.binance.com` 与 `wss://stream.binance.com:9443`，页面会自动启动 REST 回填 + WebSocket 推送。
3. 若需调整交易对或刷新策略，可在文件底部脚本中修改 `SYMBOL`、`BASE_URL`、`WS_BASE` 或订阅的时间粒度。

## 模拟撮合 / 下单流程

页面内置 `DemoMatchingApi`：前端提交订单后会异步返回撮合结果（支持全部成交 / 部分成交 / 排队）。成功回调会刷新“模拟撮合订单”列表，方便测试端到端下单逻辑或与真实后端对接前的演示。

> Demo API 仅做演示用途，如需串联真实撮合服务，可在脚本末尾替换 `matchingApi` 的实现。
