# libp2p HTTP 协议支持

- 协议 ID：`/http/1.1`
- 作用：在任意 libp2p 连接上复用 HTTP/1.1 语义，实现请求-响应式交互；兼容浏览器、边缘节点等仅支持 HTTP 的组件。

## 服务端
```nim
import std/options
import libp2p/[builders, protocols/http/http]

proc rootHandler(req: HttpRequest): Future[HttpResponse] {.async.} =
  HttpResponse(
    status: 200,
    headers: {"content-type": "text/plain"}.toTable(),
    body: "hello from nim-libp2p".toBytes(),
  )

let switch = newStandardSwitch(
  httpDefaultHandler = Opt.some(rootHandler),
  httpConfig = HttpConfig.init(maxRequestBytes = 256 * 1024, handlerTimeout = 5.seconds),
)
```

`withHttpService` 支持注册多条路径前缀（最长匹配）。除 `Connection: close` 与 `Content-Length` 外，可通过 `response.headers` 设置自定义头部。

## 客户端
```nim
import libp2p/[switch, protocols/http/http, peerid]

let resp = await httpRequest(
  sw,
  peerId,
  HttpRequest(verb: "GET", path: "/"),
  maxResponseBytes = 256 * 1024,
)
echo resp.status       # => 200
echo resp.body         # => ...
```

目前支持：
- 简单 GET/POST（`Content-Length`，无分块编码）
- 最多 100 个头部
- `Connection: close` 语义；请求处理完成后主动关闭流
- 可通过 `HttpConfig` 设置请求/响应体上限与处理超时，超出限制时返回 `413 Payload Too Large` 或 `504 Gateway Timeout`
- 客户端 `httpRequest` 也支持 `maxResponseBytes`，超过阈值将抛出 `LPError`
- 默认 `maxRequestBytes` / `maxResponseBytes` 均为 `DefaultHttpBodyLimit`（1 MiB），可按需覆盖

## 反向代理

`HttpReverseProxy` 可以直接复用已有 HTTP(S) 服务：它会在转发前处理路径前缀、重写 Host/Connection 头，并默认注入以下头部，方便与 go-libp2p-http 等实现互通。

- `X-Forwarded-Proto: libp2p`
- `X-Forwarded-For`：如果连接观测地址包含 `/ip4`/`/ip6`/`/dns*` 段，则追加对应主机；否则回退到远端 PeerId。
- `X-Libp2p-Peer-Id`：远端 PeerId 的 base58 表示。
- `X-Libp2p-Remote-Addr`：远端观测到的 multiaddr，便于上游服务记录完整路由信息。

```nim
import std/[options, uri]
import stew/byteutils
import libp2p/[builders, protocols/http/http, protocols/http/proxy]

proc notFound(req: HttpRequest): Future[HttpResponse] {.async.} =
  HttpResponse(
    status: 404,
    headers: {"content-type": "text/plain; charset=utf-8"}.toTable(),
    body: "no route".toBytes(),
  )

let proxyCfg = HttpProxyConfig.init(
  target = parseUri("http://127.0.0.1:8080"),
  stripPath = "/gateway",
)
let gatewayProxy = HttpReverseProxy.new(proxyCfg)

let sw = newStandardSwitch(
  httpDefaultHandler = Opt.some(notFound),
  httpRoutes = @[gatewayProxy.route("/gateway")],
)
```

`stripPath` 用于移除请求路径的前缀，匹配成功后将余下部分附加在目标服务的基路径后。也可以通过 `extraHeaders` 在构造配置时注入固定头部，例如携带认证信息。

如需关闭上述自动头部，可修改 `HttpProxyConfig`：

```nim
var proxyCfg = HttpProxyConfig.init(target = parseUri("https://api.example"))
proxyCfg.addForwardedFor = false
proxyCfg.addPeerIdHeader = false
proxyCfg.addRemoteAddrHeader = false
```

未来可扩展：
- HTTP/2/3 映射
- 分块编码、升级头部、长连接复用等高级特性
- 亦可直接在 `newStandardSwitch(...)` 传入 `httpDefaultHandler` / `httpConfig` 以初始化带 HTTP 服务的节点
