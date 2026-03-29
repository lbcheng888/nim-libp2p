# tsnet QUIC Relay 协议

本文档只定义一条正确路径：

- 节点到网关只有一条长期 QUIC 连接
- 连接里只有一条长期控制流
- 每个 bridge 会话单独开数据流
- 节点只有在网关确认 `route_registered + await` 后，才能把 `/tsnet` 地址对外发布

本文档不讨论旧的临时流、文件交换、额外控制连接，也不保留“先试试再说”的时序。

## 1. 角色

| 角色 | 职责 |
| --- | --- |
| listener 节点 | 注册 `/tsnet` 路由，长期等待入站连接 |
| dialer 节点 | 请求拨号某个 `/tsnet` 路由 |
| relay gateway | 保存路由表，通知 listener，有序建立 bridge |

## 2. 连接与流

| 层级 | 数量 | 用途 |
| --- | --- | --- |
| 节点 ↔ gateway QUIC 连接 | 1 | 长期会话 |
| 控制流 | 1 | `accept/await/incoming/ready`、候选地址、路由状态 |
| bridge 数据流 | 每个 `bridgeId` 1 条 | 真正转发业务数据 |

约束：

| 约束 | 说明 |
| --- | --- |
| 控制消息只能走控制流 | 不能混进数据流 |
| 数据流只能承载 bridge 数据 | 不能顺手塞 `ready` 或 `route_status` |
| 一个 `bridgeId` 只能绑定一条数据流 | 防止串线 |
| 一个流关闭不能影响同连接其他流 | 这是 builtin QUIC 的基础要求 |

## 3. 控制消息

### 3.1 listener 注册

1. listener 开一条普通短流，发送：

```json
{
  "version": 1,
  "mode": "listen",
  "kind": "tcp|udp",
  "route": "/.../tsnet",
  "candidates": ["..."]
}
```

2. gateway 返回：

```json
{
  "ok": true,
  "payload": {
    "mode": "listen",
    "route": "/.../tsnet",
    "candidates": ["..."]
  }
}
```

3. listener 关闭这条短流。

### 3.2 listener 建立长期控制流

1. listener 再开一条长期双向流，发送：

```json
{
  "version": 1,
  "mode": "accept",
  "kind": "tcp|udp",
  "route": "/.../tsnet"
}
```

2. gateway 返回 `accept` 确认。

3. 之后 listener 必须在这条长期控制流上反复发送：

```json
{
  "op": "await",
  "route": "/.../tsnet"
}
```

4. gateway 只有在路由表里确实保存了这条 route 后，才返回：

```json
{
  "ok": true,
  "payload": {
    "op": "await",
    "route": "/.../tsnet",
    "published": true
  }
}
```

只有收到这条 `published=true` 的 `await` 确认，listener 才能把 `/tsnet` 地址对外发布。

## 4. 拨号

### 4.1 dialer 请求

dialer 开一条短流，发送：

```json
{
  "version": 1,
  "mode": "dial",
  "kind": "tcp|udp",
  "route": "/.../tsnet",
  "source": "/.../tsnet",
  "candidates": ["..."]
}
```

### 4.2 gateway 通知 listener

gateway 必须满足下面两个条件才允许继续：

| 条件 | 说明 |
| --- | --- |
| 路由存在 | `route_registered` 已完成 |
| listener 正在 `await` | 长期控制流已经进入等待态 |

满足后，gateway 在 listener 的长期控制流上发送：

```json
{
  "op": "incoming",
  "route": "/.../tsnet",
  "source": "/.../tsnet",
  "candidates": ["..."],
  "sessionId": "<bridgeId>"
}
```

### 4.3 listener 确认 ready

listener 准备好本地目标后，在**同一条长期控制流**返回：

```json
{
  "ok": true,
  "op": "ready",
  "candidates": ["..."],
  "sessionId": "<bridgeId>"
}
```

### 4.4 listener 附着数据流

listener 另开一条新的 bridge 数据流，发送：

```json
{
  "version": 1,
  "mode": "bridge_attach",
  "kind": "tcp|udp",
  "route": "/.../tsnet",
  "sessionId": "<bridgeId>"
}
```

gateway 确认后，才能把这条数据流和 dialer 的短流桥起来。

### 4.5 gateway 返回拨号成功

只有在 `ready + bridge_attach` 都完成之后，gateway 才能给 dialer 返回：

```json
{
  "ok": true,
  "payload": {
    "mode": "dial",
    "route": "/.../tsnet",
    "candidates": ["..."],
    "sessionId": "<bridgeId>"
  }
}
```

## 5. 状态机

### 5.1 listener

| 状态 | 进入条件 | 离开条件 |
| --- | --- | --- |
| `registered` | `listen` 成功 | 建立 `accept` 长期控制流 |
| `attached` | `accept` 成功 | 发送 `await` |
| `awaiting` | 收到 `await + published=true` | 收到 `incoming` |
| `bridging` | 发送 `ready` 且 `bridge_attach` 成功 | bridge 结束，回到 `awaiting` |

### 5.2 gateway 路由可见性

| 条件 | 路由是否可拨 |
| --- | --- |
| 只 `listen` 成功 | 否 |
| `listen + accept` 成功 | 否 |
| `listen + accept + await(published=true)` | 是 |
| 长期控制流断开 | 否，立即撤销 |

## 6. 数据流规则

| 规则 | 说明 |
| --- | --- |
| 一个 `bridgeId` 一条数据流 | 不允许复用控制流带数据 |
| UDP 桥只绑定一个客户端源地址 | 防止一个桥吃到多个客户端的数据 |
| route 状态变化不走数据流 | 避免数据面和控制面互相污染 |
| bridge 失败时只影响这个 `bridgeId` | 不能拖死长期控制流 |

## 7. builtin QUIC 需要满足的实现语义

| 语义 | 说明 |
| --- | --- |
| `streamId -> streamState` 注册表 | 连接层只做 demux |
| 入站流独立队列 | 新流只进入 `pendingInboundStreams` |
| 每条流独立 `readQueue/readWaiters/EOF` | 一个流关闭不能误伤其他流 |
| 已缓存 payload 必须先交付再关闭 | 不允许把最后一帧吞掉 |

## 8. 为什么旧路子不对

| 旧做法 | 问题 |
| --- | --- |
| listener 在 `listen` 成功后就自认 ready | 网关还可能没真正发布 route |
| 把 `incoming/ready` 和数据桥混在同一条临时流里 | 多流语义不清，容易串线 |
| 为不同场景保留不同控制路径 | 同一个协议被拆碎，无法证明正确性 |

## 9. 当前仓库实现对应

| 文件 | 职责 |
| --- | --- |
| [quicrelay.nim](/Users/lbcheng/nim-libp2p/libp2p/transports/tsnet/quicrelay.nim) | relay 协议实现 |
| [msquicdriver_experimental.nim](/Users/lbcheng/nim-libp2p/libp2p/transports/msquicdriver_experimental.nim) | builtin QUIC 连接/流注册表与读队列 |
| [msquicstream.nim](/Users/lbcheng/nim-libp2p/libp2p/transports/msquicstream.nim) | LPStream 适配层 |
| [tsnet_product_node.nim](/Users/lbcheng/nim-libp2p/examples/mobile_ffi/tsnet_product_node.nim) | 产品节点 RPC 入口 |

## 10. 下一步只允许做什么

| 允许 | 不允许 |
| --- | --- |
| 修 builtin QUIC 的同连接多流 | 再造第二套 relay 协议 |
| 修长期控制流与 bridge 数据流的边界 | 用文件交换或外部脚本补洞 |
| 用产品节点 RPC 做端到端验证 | 回到临时 request 进程/额外控制连接 |
