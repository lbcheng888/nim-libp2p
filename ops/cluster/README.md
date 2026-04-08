# Fabric/LSMR 三节点部署

固定节点：

- `64.176.84.12`
  - gateway: `9444/9446`
  - product rpc: `19111`
  - product listen: `40111`
  - service: `nim-fabric-product.service`
- `154.26.191.226`
  - product rpc: `19112`
  - product listen: `40112`
  - service: `nim-fabric-product.service`
- `local-macbook`
  - product rpc: `19113`
  - product listen: `40113`
  - service: `com.lbcheng.nim-fabric-product.mac`

统一资产：

- 节点清单：`ops/cluster/cluster_nodes.json`
- genesis：`ops/cluster/fabric_cluster_genesis.json`
- 运行脚本：`ops/cluster/run_fabric_product_node.sh`
- 安装脚本：`ops/cluster/install_fabric_three_node.sh`

构建：

```bash
nim c -d:ssl -o:build/rwadd examples/rwadd.nim
```

安装：

```bash
ops/cluster/install_fabric_three_node.sh 64
ops/cluster/install_fabric_three_node.sh dmit
ops/cluster/install_fabric_three_node.sh mac
```

健康检查：

```bash
curl -sf http://127.0.0.1:19111/healthz
curl -sf http://127.0.0.1:19112/healthz
curl -sf http://127.0.0.1:19113/healthz
```

JSON-RPC：

```bash
curl -sf http://127.0.0.1:19111 \
  -H 'Content-Type: application/json' \
  --data-binary '{"jsonrpc":"2.0","id":"status","method":"fabric.status","params":{}}'

curl -sf http://127.0.0.1:19111 \
  -H 'Content-Type: application/json' \
  --data-binary '{"jsonrpc":"2.0","id":"quiesce","method":"fabric.quiesce","params":{}}'
```

移动端默认集群配置：

- `gatewayHost = 64.176.84.12`
- `controlEndpoint = quic://64.176.84.12:9444/nim-tsnet-control-quic/v1`
- `relayEndpoint = quic://64.176.84.12:9446/nim-tsnet-relay-quic/v1`
- `productRpcBaseUrl` 默认由 native bridge 推导为 `http://64.176.84.12:19111`
