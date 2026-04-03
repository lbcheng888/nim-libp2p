# tsnet_remote_admin

以后远端产品节点更新只用这个 Nim 工具，不再手写 `ssh/scp/systemctl`。

二进制位置：

- `/Users/lbcheng/nim-libp2p/build/tsnet_remote_admin`

## 常用命令

| 用途 | 命令 |
| --- | --- |
| 同步产品节点源码 | `/Users/lbcheng/nim-libp2p/build/tsnet_remote_admin --ssh-host 64.176.84.12 --ssh-user root --op sync-product` |
| 远端重编产品节点 | `/Users/lbcheng/nim-libp2p/build/tsnet_remote_admin --ssh-host 64.176.84.12 --ssh-user root --op build-product` |
| 重启远端产品服务 | `/Users/lbcheng/nim-libp2p/build/tsnet_remote_admin --ssh-host 64.176.84.12 --ssh-user root --op restart-product` |
| 查看远端产品服务状态 | `/Users/lbcheng/nim-libp2p/build/tsnet_remote_admin --ssh-host 64.176.84.12 --ssh-user root --op status-product` |
| 一次完成同步+重编+重启+状态 | `/Users/lbcheng/nim-libp2p/build/tsnet_remote_admin --ssh-host 64.176.84.12 --ssh-user root --op deploy-product` |
| 同步 gateway 源码 | `/Users/lbcheng/nim-libp2p/build/tsnet_remote_admin --ssh-host 64.176.84.12 --ssh-user root --op sync-gateway` |
| 远端重编 gateway | `/Users/lbcheng/nim-libp2p/build/tsnet_remote_admin --ssh-host 64.176.84.12 --ssh-user root --op build-gateway` |
| 重启远端 gateway 服务 | `/Users/lbcheng/nim-libp2p/build/tsnet_remote_admin --ssh-host 64.176.84.12 --ssh-user root --op restart-gateway` |
| 查看远端 gateway 服务状态 | `/Users/lbcheng/nim-libp2p/build/tsnet_remote_admin --ssh-host 64.176.84.12 --ssh-user root --op status-gateway` |
| 查看远端 gateway 最近日志 | `/Users/lbcheng/nim-libp2p/build/tsnet_remote_admin --ssh-host 64.176.84.12 --ssh-user root --op journal-gateway --lines 200` |
| 一次完成 gateway 同步+重编+重启+状态 | `/Users/lbcheng/nim-libp2p/build/tsnet_remote_admin --ssh-host 64.176.84.12 --ssh-user root --op deploy-gateway` |

## 默认同步文件

| 文件 |
| --- |
| `examples/mobile_ffi/libnimlibp2p.nim` |
| `examples/mobile_ffi/tsnet_product_node.nim` |

如果只想同步指定文件，可以重复传 `--file`。

## gateway 默认同步文件

| 文件 |
| --- |
| `examples/tsnet_quic_gateway_service.nim` |
| `libp2p/transports/msquicstream.nim` |
| `libp2p/transports/tsnet/control.nim` |
| `libp2p/transports/tsnet/quiccontrol.nim` |
| `libp2p/transports/tsnet/quicrelay.nim` |
