# Fetch 协议

- 协议 ID：`/libp2p/fetch/0.0.1`
- 功能：按键查询对端数据（`Fetch(key) -> (status, value)`），适合作为 PubSub 持久化或其它请求-响应基础模块。

## 使用示例
```nim
import std/options
import libp2p/[builders, protocols/fetch/fetch, peerid]

# 服务端
proc fetchHandler(key: string): Future[FetchResponse] {.async.} =
  if key == "latest" :
    FetchResponse(status: FetchStatus.fsOk, data: "hello".toBytes())
  else:
    FetchResponse(status: FetchStatus.fsNotFound, data: @[])

let sw = newStandardSwitch(
  fetchHandler = Opt.some(fetchHandler),
  fetchConfig = FetchConfig.init(maxResponseBytes = 256 * 1024),
)

# 客户端
let peer: PeerId = ... # 对端 PeerId
let resp = await fetch(sw, peer, "latest", maxResponseBytes = 256 * 1024)
if resp.status == FetchStatus.fsOk:
  echo resp.data
```

## 错误处理
- 超时：`fetch(...)` 支持自定义 `timeout`（默认 10s），超时将抛出 `FetchError`，并携带 `kind = feTimeout`。
- 编码错误或对端异常：返回 `FetchStatus.fsError`，客户端需自行判断。
- `FetchStatus` 值：
  * `fsOk`：成功返回数据；
  * `fsNotFound`：未找到；
  * `fsError`：内部错误或协议异常；
  * `fsTooLarge`：返回体超过服务端允许的最大尺寸。
- `FetchError.kind` 分类：
  * `feDialFailure`：拨号阶段失败；
  * `feTimeout`：读取响应超时；
  * `feWriteFailure`/`feReadFailure`：读取或写入流失败；
  * `feInvalidResponse`：解析响应失败；
  * `feConfig`：本地配置非法（如 `maxAttempts <= 0`）。
- 默认请求/响应上限为 `DefaultFetchMessageSize`（512 KiB），可通过 `FetchConfig` / `fetch(..., maxResponseBytes=...)` 调整
- 每次调用默认会在失败时重试 2 次（共 3 次尝试），可通过 `fetch(..., maxAttempts=..., retryDelay=...)` 调整次数与重试间隔；`FetchError.kind` 可判断失败原因。

## 配置能力
- `FetchConfig` 支持设置 `maxRequestBytes`、`maxResponseBytes` 与 `handlerTimeout`，默认均为 512 KiB/512 KiB/10 秒；
- 服务端在响应体超出 `maxResponseBytes` 时会主动返回 `fsTooLarge`，客户端可通过 `fetch(..., maxResponseBytes = ...)` 与之匹配；
- 若 `handlerTimeout` > 0，则会在超时后返回 `fsError`，防止长时间占用连接。
- 客户端侧新增 `maxAttempts` 与 `retryDelay` 参数，分别限制重试次数与相邻重试的等待时长，默认值为 3 次、200ms；
- 也可在 `newStandardSwitch(...)` 时直接传入 `fetchHandler` / `fetchConfig` 以便快速开启该服务
