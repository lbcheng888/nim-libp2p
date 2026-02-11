# 私有网络（pnet）集成设计

## 1. 目标
- 在 nim-libp2p 中实现与 [Private Networks PSK 规范](https://github.com/libp2p/specs/blob/master/pnet/Private-Networks-PSK-V1.md) 一致的功能。
- 为入站与出站连接提供预共享密钥（PSK）级别的流量加密，确保只有持有同一 PSK 的节点才能进入网络。
- 与现有安全通道（Noise、TLS、Plaintext）和传输栈保持兼容，不破坏既有 API。

## 2. 背景与参考
- 规范：`/key/swarm/psk/1.0.0/` 多段多编码文件格式、24 字节随机 nonce、XSalsa20 流加密、强制密钥长度 32 字节。
- Go 实现参考：
  - `github.com/libp2p/go-libp2p/p2p/net/pnet/psk_conn.go`：在原始 `net.Conn` 上写入随机 nonce，并用 XSalsa20 对读写流做异或。
  - `github.com/libp2p/go-libp2p/core/pnet/codec.go`：解析 `swarm.key` 文件（支持 `/bin/`、`/base16/`、`/base64/`）。
  - `github.com/libp2p/go-libp2p/core/pnet/env.go`：`LIBP2P_FORCE_PNET` 环境变量强制启用。
- Nim 现状：
  - `libp2p/builders.nim:366` 构建 `MuxedUpgrade` 用于安全/复用阶段，缺少保护层。
  - `libp2p/transports/transport.nim:83` 的 `upgrade()` 负责调用 `Upgrade.upgrade`。

## 3. 范围与非目标
### 3.1 范围
- 支持从文件与内存加载 32 字节 PSK。
- 在连接升级前增加“保护器”阶段，出入站都必须通过。
- 提供 `SwitchBuilder.withPnet(psk: PrivateNetworkKey)`、`SwitchBuilder.withPnetFromFile(path)` 等构造器。
- 为 `LIBP2P_FORCE_PNET=1` 时，若 `SwitchBuilder` 未配置 PSK 则抛错。
- 提供基础测试（单元、集成）与 go-libp2p 互操作测试脚本。

### 3.2 非目标
- 不实现 PKI 模式（未来版本）。
- 不改变 `muxed`、`autoTLS` 等既有组件的行为。
- 不在本迭代内提供配置热更新。

## 4. 架构设计
### 4.1 新增组件
| 组件 | 功能 | 关键接口 |
| ---- | ---- | -------- |
| `libp2p/pnet.nim` | PSK 加载、连接保护器实现 | `loadPrivateNetworkKey*`、`loadPrivateNetworkKeyFromString*`、`newConnectionProtector*`、`protect*` |
| `libp2p/crypto/xsalsa20.nim` | XSalsa20 流封装 | `initXSalsa20Stream*` |
| `libp2p/upgrademngrs/muxedupgrade.nim` | 在 upgrade 流水线注入保护器 | `MuxedUpgrade.new(..., protector)` |
| `libp2p/builders.nim` | `SwitchBuilder` 配置私有网络 | `withPnet*`、`withPnetFromFile*`、`withPnetFromString*` |

> XSalsa20 实现方案：首选复用 `libsodium` 绑定（若仓库已有），否则移植 go-libp2p 的 `xsalsa20` 逻辑到 Nim，封装成 `libp2p/crypto/xsalsa20.nim`。

### 4.2 连接流程调整
1. 传输层建立 Raw `Connection`。
2. 若 Switch 启用了 `Protector`：
   - 出站：生成随机 24 字节 nonce，先写入，再对后续流量进行 `XORKeyStream`，返回包装后的 `Connection`。
   - 入站：读取 24 字节 nonce，初始化解密流，对随后数据解密。
3. 将经过保护的 `Connection` 交给 `Upgrade.secure()`（Noise/TLS）。
4. 复用、Identify 等流程保持不变。

### 4.3 API 与配置
- `SwitchBuilder.withPnet(psk: PrivateNetworkKey)`：直接注入预加载的 PSK。
- `SwitchBuilder.withPnetFromFile(path: string)`：从 `swarm.key` 文件加载 PSK。
- `SwitchBuilder.withPnetFromString(content: string)`：解析内嵌的 `swarm.key` 文本，便于示例与动态加载。
- 示例 `examples/pnet_private_network.nim` 展示了两节点共用 PSK 完成私有网络握手。
- `SwitchBuilder.build` 中：
  - 若 `LIBP2P_FORCE_PNET=1` 且未调用 `withPnet` ➜ 抛出 `LPError`.
  - 若配置了 pnet，构造 `MuxedUpgrade.new(muxers, secureManagers, ms, protector)`。
- `MuxedUpgrade.upgrade`：
  ```nim
  proc upgrade(self: MuxedUpgrade, conn: Connection, peerId: Opt[PeerId]): Future[Muxer] =
    let protectedConn =
      if self.protector.isNil: conn else: self.protector.protect(conn)
    let securedConn = await self.secure(protectedConn, peerId)
    let muxer = (await self.mux(securedConn)).valueOr:
      raise newException(UpgradeFailedError, "a muxer is required")
    result = muxer
  ```
  - `Transport.upgrade` 调用保持不变，注入的是带有保护器的 `MuxedUpgrade`.

### 4.4 错误处理 & 日志
- `PnetError` 枚举：`InvalidKeyLength`, `NonceRead`, `NonceWrite`, `Decrypt`, `ForceEnabled`.
- 入站若 nonce 读取失败 ➜ 关闭连接，返回 `AuthedFailed`，计入指标。
- 加载文件失败时返回 `Result` 对象并记录路径。
- 保护器启动时在日志打印 `pnet enabled` + `pskId`（psk hash 前 4 字节）以方便排查。

### 4.5 性能考虑
- 采用 `buffer` 池（类似 go 的 `go-buffer-pool`）减少分配。
- `XORKeyStream` 中尽量使用 in-place。
- 由于 PSK 层先于 Noise/TLS，瓶颈以 `xsalsa20` 实现效率为主，后续压测对比原始 TCP。

## 5. 兼容性与特性开关
- 默认关闭，只有显式配置时启用。
- 新增编译 flag `libp2p_pnet_disable` 用于在不支持 xsalsa20 的平台下跳过编译（测试、CI 场景）。
- `LIBP2P_FORCE_PNET` 仅在 build 阶段检查，不在运行时重复读取。

## 6. 测试计划
### 6.1 单元测试
- `pskloader`：覆盖 `/bin/`、`/base16/`、`/base64/` 格式、错误路径、长度不符等情况。
- `protector`：
  - 出站写入 nonce、入站读取 nonce 的 happy path。
  - 读取失败、写入失败、PSK 长度错误的异常。
- `ProtectedUpgrade`：在模拟连接上验证调用顺序（保护器→secure→mux）。

### 6.2 集成测试
- 基于 `tests/testswitch.nim` 新增场景：两个节点共享 PSK 成功握手；不同 PSK 拒绝并确保无 Identify 消息。
- 发布 fixture `tests/scripts/pnet_go_interop.sh`，通过 go-libp2p 启动受保护 listener，验证 Nim client。

### 6.3 回归与性能
- 启用 pnet 与禁用 pnet 的 RTT、吞吐对比（可用 `performance` 目录现有脚本）。
- 确保 `SwitchBuilder` 其他功能（autonat、relay）与 pnet 同时启用时不会冲突。

## 7. 交付里程碑
1. **M1-1**：完成 `pskloader` 与 `protector` 基础实现 + 单元测试。
2. **M1-2**：扩展 `SwitchBuilder`/`Upgrade`，跑通简单连接示例。
3. **M1-3**：互操作测试、文档更新、在 `README` Modules 表增加 pnet。

## 8. 风险与缓解
- **依赖风险**：Nim 缺少成熟 XSalsa20 实现 ➜ 优先评估 `libsodium` 绑定；若不可用，移植经过验证的实现并配套测试向量。
- **资源占用**：双层加密可能影响吞吐 ➜ 基准测试并记录；必要时允许禁用 buffer pool。
- **使用错误**：用户忘记配置 PSK ➜ 在构建时立即报错，并在文档中强调。

## 9. 未决问题
- 是否需要支持多个 PSK（滚动密钥）？暂定不支持，但在 API 预留可能性（例如传递回调）。
- 是否需要 CLI 或工具生成 `swarm.key`？建议后续提供 `nimble task`。
- 与 QUIC/Relay 叠加时是否需额外兼容逻辑？预计不需要，但需在集成测试中覆盖。
