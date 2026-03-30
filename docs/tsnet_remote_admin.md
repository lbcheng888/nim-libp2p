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

## 默认同步文件

| 文件 |
| --- |
| `examples/mobile_ffi/libnimlibp2p.nim` |
| `examples/mobile_ffi/tsnet_product_node.nim` |

如果只想同步指定文件，可以重复传 `--file`。
