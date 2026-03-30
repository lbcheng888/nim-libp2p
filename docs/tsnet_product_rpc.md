# tsnet_product_rpc

远端产品节点操作以后只用一个二进制：

- 本地二进制：`/Users/lbcheng/nim-libp2p/build/tsnet_product_rpc`
- 远端二进制：`/root/nim-libp2p/build/tsnet_product_rpc`

## 规则

| 规则 | 说明 |
| --- | --- |
| 不再手写 `ssh ... '{\"op\":...}'` | 容易写错、转义容易坏 |
| 状态、连接、发消息都走 `tsnet_product_rpc` | 本地和远端只认这一套入口 |
| 远端编译固定用 `/root/Nim/bin/nim` | 远端 `PATH` 里没有 `nim` |

## 常用命令

| 用途 | 命令 |
| --- | --- |
| 本地 tailnet 状态 | `/Users/lbcheng/nim-libp2p/build/tsnet_product_rpc --pretty --port 19125 --op tailnet_status` |
| 远端 tailnet 状态 | `/Users/lbcheng/nim-libp2p/build/tsnet_product_rpc --pretty --ssh-host 64.176.84.12 --ssh-user root --port 19111 --op tailnet_status` |
| 远端本地信息 | `/Users/lbcheng/nim-libp2p/build/tsnet_product_rpc --pretty --ssh-host 64.176.84.12 --ssh-user root --port 19111 --op local_info` |
| 等待远端 tailnet ready | `/Users/lbcheng/nim-libp2p/build/tsnet_product_rpc --pretty --ssh-host 64.176.84.12 --ssh-user root --port 19111 --op wait_tailnet_ready --timeoutMs 120000` |

## 复杂操作

`connect_exact` 不再手写 JSON。直接重复传 `--listen-addr`：

```bash
/Users/lbcheng/nim-libp2p/build/tsnet_product_rpc \
  --pretty \
  --ssh-host 64.176.84.12 \
  --ssh-user root \
  --port 19111 \
  --op connect_exact \
  --peer-id 12D3KooWD9hvzzGh7XyrPaBP1DivMjjrEfbKF12yhM8eGx74vxB5 \
  --listen-addr /ip4/100.64.252.94/udp/40125/quic-v1/tsnet/p2p/12D3KooWD9hvzzGh7XyrPaBP1DivMjjrEfbKF12yhM8eGx74vxB5 \
  --listen-addr /awdl/ip4/100.64.252.94/udp/40125/quic-v1/tsnet/p2p/12D3KooWD9hvzzGh7XyrPaBP1DivMjjrEfbKF12yhM8eGx74vxB5 \
  --listen-addr /nan/ip4/100.64.252.94/udp/40125/quic-v1/tsnet/p2p/12D3KooWD9hvzzGh7XyrPaBP1DivMjjrEfbKF12yhM8eGx74vxB5 \
  --listen-addr /nearlink/ip4/100.64.252.94/udp/40125/quic-v1/tsnet/p2p/12D3KooWD9hvzzGh7XyrPaBP1DivMjjrEfbKF12yhM8eGx74vxB5
```

发消息同样直接结构化参数：

```bash
/Users/lbcheng/nim-libp2p/build/tsnet_product_rpc \
  --pretty \
  --ssh-host 64.176.84.12 \
  --ssh-user root \
  --port 19111 \
  --op send_dm \
  --peer-id 12D3KooWD9hvzzGh7XyrPaBP1DivMjjrEfbKF12yhM8eGx74vxB5 \
  --text hello
```

## 远端更新

| 步骤 | 命令 |
| --- | --- |
| 同步源码 | `scp /Users/lbcheng/nim-libp2p/examples/mobile_ffi/tsnet_product_rpc.nim root@64.176.84.12:/root/nim-libp2p/examples/mobile_ffi/tsnet_product_rpc.nim` |
| 远端编译 | `ssh root@64.176.84.12 'cd /root/nim-libp2p && /root/Nim/bin/nim c --hints:off --warnings:off -o:/root/nim-libp2p/build/tsnet_product_rpc /root/nim-libp2p/examples/mobile_ffi/tsnet_product_rpc.nim'` |

