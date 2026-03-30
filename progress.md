# progress

| 时间 | 进展 |
| --- | --- |
| 2026-03-30 | 已把 Android/native 异步架构约束写成文档 |
| 2026-03-30 | 已把 `NativeNodeDispatcher` 从 2 线程改成 1 线程 |
| 2026-03-30 | 已把发送窗口的只读状态改成优先走缓存 |
| 2026-03-30 | 已重新打包并安装 Android Debug 包 |
| 2026-03-30 | 已把 Android 发送主链改成 `register_hints + send_with_ack_kickoff`，移除 Kotlin 同步 `prepare_route/connect_exact/waitSecure` |
| 2026-03-30 | 已把 `seedAddrs/source` 接进 `send_with_ack` native kickoff，发送任务改为自己处理 connectivity |
| 2026-03-30 | 已去掉消息页发送前等待后台 preconnect job 的 2.5 秒阻塞 |
| 2026-03-30 | 已把 `refreshNodeResources`、`connectedPeers`、`connectedPeersInfo` 收成发送优先的低优先级缓存快照 |
| 2026-03-30 | 已把 UI 帧轮询和系统状态刷新在发送关键窗口内改成直接让路或使用快照 |
| 2026-03-30 | 已给 `getLastDirectError` 增加缓存快照，发送失败后不再因为读错误详情再次卡全局锁 |
| 2026-03-30 | 已确认远端到本地失败的单一主因是本地 `proxyListenersReady` 吃旧缓存，gateway 已无该 route 仍显示 ready |
| 2026-03-30 | 已把远端产品节点操作收成结构化 Nim 工具 `tsnet_product_rpc`，支持 `--op/--peer-id/--listen-addr` 和 `--ssh-host`，不再手写远端 JSON |
| 2026-03-30 | 已同步并在远端用 `/root/Nim/bin/nim` 编译新的 `tsnet_product_rpc`，本地和远端 `tailnet_status/local_info` 已跑通 |
| 2026-03-30 | 已确认 `connect_exact` 真正失败点已收敛到 identify：`peerConnected=true`、`secureOk=true`、`identifiedOk=false` |
| 2026-03-30 | 已修掉 `tsnet_product_node` 把 kickoff 初期短暂 `missing` 误报成 `connect_exact_missing_state` 的竞态 |
| 2026-03-30 | 已给 `ensurePeerIdentified` 和 `peerStore.identify` 补窄日志，能区分 `beginProtocolNegotiation`、`multistream select` 和最终等待超时 |
| 2026-03-30 | `test_msquic_concurrent_streams` 继续通过，说明当前主线不是 QUIC 多流基础件整体失效，而是产品 identify 路径 |
| 2026-03-30 | 已验证新版 `tsnet_product_node` 下，同步 `connect_exact` 不再秒回 `connect_exact_missing_state` |
| 2026-03-30 | 已把 `idleWakeLoop` 的 listener 自修从“构整份 tailnetStatus JSON”改成轻量布尔检查，节点在 `connect_exact` 期间不再因后台状态轮询崩溃 |
| 2026-03-30 | 已修掉 `quicrelay` 里 owner 级状态写成局部副本的问题：`relayOwnerTasks`、`relayListenerStates`、`relayUdpDialStates`、`relayOwnerClients` 现在都是真正写回全局表 |
| 2026-03-30 | `testtsnetquicrelay.nim` 已恢复为 `16 OK`，说明 relay listener/dial 状态机这一层已经收住 |
| 2026-03-30 | 本地 fresh 产品节点重新验证后，新的真实 blocker 已经前移到 `9444` 控制握手：`wait_tailnet_ready` 失败，错误是 `nim_quic connection event and handshake did not complete` |
| 2026-03-30 | 已确认本地产品节点 `local_info` 只返回 `relay_listeners` 的真实 IPv4 route，`listenAddrSource=relay_listeners`，不再按 `tailnetIPs` 伪造双栈 `/tsnet` 地址 |
| 2026-03-30 | 已把远端产品节点更新到同版源码并重编恢复服务，远端 `local_info` 现在也只返回 `relay_listeners` 的真实 IPv4 route，`listenAddrSource=relay_listeners` |
| 2026-03-30 | 已用真实 `relay_listeners` route 实跑 Mac→远端产品节点：`connect_exact` 成功，`identifiedOk=true`，说明地址问题已排除 |
| 2026-03-30 | 同一轮实跑里 `send_dm` 仍然返回 `send_dm_timeout`，远端 `wait_dm` 未收到 token；当前主线已收窄到 `connect_exact` 之后的 identify/DM 发送后半段 |
| 2026-03-30 | 已给 `quicrelay` 长期控制流所有关键写操作加超时，避免 `incoming/ready/await/ack` 卡死后还长期占住 route |
| 2026-03-30 | 已把这条长期控制流约束写进 `docs/transports/tsnet_quic_relay_protocol.md`，以后控制流写超时必须立刻失效重建 |
| 2026-03-30 | 已定位 `send_dm_kickoff` 的预序列化直发包缺少 `from` 等字段；发送端即使拿到 LP ack，接收端仍可能因为 sender 身份为空而不入 `dm_rows` |
| 2026-03-30 | 已修 `libnimlibp2p.nim`：预序列化直发包现在统一补齐 `op/mid/messageId/from/timestamp_ms/timestampMs/ackRequested/reply_to/target` 后再发送 |
| 2026-03-30 | 已强制远端用更新后的 `libnimlibp2p.nim + dmservice.nim` 全量重编 `tsnet_product_node` 并重启 `nim-tsnet-product.service` |
| 2026-03-30 | 已把 `runtime.proxyListenerReadiness` 改成按 gateway 真实发布态校验 route，不再只信本地 `relayReadyRoutes` 缓存 |
| 2026-03-30 | 修完后第一轮 live 已证明先前的 `route_not_registered` 分叉被压下去了：`connect_exact` 一度重新恢复成功 |
| 2026-03-30 | 已定位 `send_dm_kickoff` 的另一处错层：外层 `connect_exact` 成功后，内部 `executeSendDirectCommand` 还会因为带显式地址再次走 `ensurePeerReadyViaExplicitAddresses` 重拨 |
| 2026-03-30 | 已修 `executeSendDirectCommand`：若 peer 已连上，则直接 `waitForPeerReady`，不再对同一 peer 做二次 exact dial |
| 2026-03-30 | 已确认远端 route 不是“从未注册”，而是 18:03:21 因 `incoming` 写 10 秒超时被 gateway 主动下架，之后所有拨号都变成 `missing listener` |
| 2026-03-30 | 已定位节点侧自动补 listener 失效的直接原因：`runtime.listenerNeedsRepair` 和 `tailnetListenerNeedsRepair` 都漏掉了“本地仍 awaiting/ready，但 gateway 已经无 route”的情况 |
| 2026-03-30 | 已把 `tailnetControlRefreshLoop` 改成只有地址真变化才触发 `syncAdvertisedPeerInfo`，并给 `spawnTailnetWarmAdvertisedPeerInfo` 加了单任务并发门，避免产品节点自己每秒叠加发布同步 |
| 2026-03-30 | 本地新二进制验证已通过：`wait_tailnet_ready` 恢复秒回，`local_info` 再次只发布 `relay_listeners` 的真实 route，本地不再出现 99% CPU 的自刷状态 |
| 2026-03-30 | 新一轮本地→远端 `connect_exact` 失败重新收敛为 `route_not_registered`，同时远端 journal 明确显示它仍在每秒刷 `syncAdvertisedPeerInfo reason=tailnet_warm`，说明远端当前还在跑旧产品二进制，尚未吃到这轮本地修复 |
| 2026-03-30 | 已把 `tsnet_product_node.localInfo` 改成惰性优先 `relay_listeners -> runtime -> published -> synthesized`，不再在热路径里 eager 跑 discovery/network snapshot；本地和远端 `local_info` 现在都秒回 |
| 2026-03-30 | 已用 `tsnet_remote_admin --op deploy-product` 把远端产品节点切到新二进制；远端 `nim-tsnet-product.service` 已重启到最新版本 |
| 2026-03-30 | 已再次实跑本地→远端产品路径：`connect_exact` 成功，`send_dm` 成功，远端 `wait_dm` 已收到并入库 token `dm_token_20260330_193650` |
| 2026-03-30 | 已实跑远端→本地反向产品路径：`connect_exact` 成功，`send_dm` 成功，本地 `wait_dm` 已收到并入库 token `dm_token_20260330_193745` |
| 2026-03-30 | 已修 `runSendDirectKickoffAsync` 没把 `requestKey` 传进内层 `cmdSendDirect` 的真 bug；之前 Android `send_with_ack` 虽然真的在跑，但状态表永远只停在 `kickoff` |
| 2026-03-30 | 已给 `send_with_ack` 和 `multistream` 加窄日志，并重新打包安装 Android Debug 包；设备 `com.unimaker.native.debug` 已更新到 20:35:57 这版 |
| 2026-03-30 | 已把 `dmservice.send` 的内层阶段 `reuse_live/fresh_dial/write/ack_wait/ack_done` 接进 `send_with_ack` kickoff 状态机，Android 下一轮不再只看到 `send_begin` 这个黑盒阶段 |
| 2026-03-30 | 已用正确的 `-d:libp2p_msquic_experimental` 编译门验证新的 `libnimlibp2p.nim + dmservice.nim` 通过，本地产品节点构建未被这轮日志改动打坏 |
| 2026-03-30 | 新日志已证实 Android 发送失败里那条 `kickoff` 不是网络阶段，而是状态传播 bug；本地 Mac 仍能看到 identify 流和后续第二条新流 |
| 2026-03-30 | 重装后的生产 UI 自动点击使用了旧坐标，没有真正触发新一轮发送；当前剩余的是 UI 自动动作对齐问题，不再是 `requestKey` 状态 bug |
| 2026-03-30 | 已修 Android tsnet 地址发布污染；`libnimlibp2p.nim` 现在在 tsnet 模式下只发布 `/tsnet` 地址，不再混入宿主机 `10.x/192.168.x` host-network 地址 |
| 2026-03-30 | 已把 `NativeAppViewModel.configureNativeConnectivity` 收成单飞 + 2.5 秒冷却窗口；高频重复调用现在会直接 `skip=cooldown/in_flight` |
| 2026-03-30 | 已修 `MainActivity` deep link 入口：truth route 现在从 `intent.data` 解析，不再只认 `route` extra |
| 2026-03-30 | 已实测验证新 deep link：冷启动后可直接落到目标节点消息页，`seedAddrsJson` 和 `draftText` 都能进页面 |
| 2026-03-30 | 已用生产 UI 真正发出 token `ui2329cfg2`；本地 Mac `wait_dm` 超时，说明这次失败不是假点击 |
| 2026-03-30 | Android 这轮失败已从旧的 `peer_ready_failed` 收敛成 `send_with_ack_timeout`，状态卡在 `send_begin`；页面顶部显示 `libp2p RELAY · tailnet RELAY · 发送失败 send_begin 10062ms send_with_ack_timeout` |
| 2026-03-30 | 这轮 logcat 已证明后台连通性刷新不再是主因：发送后只有 `configureNativeConnectivity skip=cooldown`，没有再次把主链打回 `peer_ready_failed` |
| 2026-03-31 | 新一轮 Android 生产 UI 发送里，顶部状态已从粗粒度 `send_begin` 前推到 `wait_send_ack`，说明 `dmservice.send` 的内层阶段已经开始回写到 kickoff 状态机 |
| 2026-03-31 | 已抓到 Android 真崩点：进程不是正常超时退出，而是在后台线程调用 `libp2p_get_connected_peers_json/social_list_discovered_peers` 时触发 `FORTIFY: FD_SET: file descriptor 1024 >= FD_SETSIZE 1024` 并被系统杀回桌面 |
| 2026-03-31 | 已把 Android interactive native priority 窗口上限从 30 秒提到 120 秒，并把消息发送入口的窗口时长从 12 秒提到 90 秒，避免 tsnet 发送尚未结束时后台轮询恢复直打 native |
| 2026-03-31 | 已删除 `BridgeFacades.sendDirectText` 里的同步 `prepareDirectRoutePeer`；消息页现在只做 `register_hints + send_with_ack_kickoff(seedAddrs)`，不再先走一条错层的 `connectPeer/socialConnectPeer/connectMultiaddr` 预连接 |
| 2026-03-31 | 已把 tsnet 发送 budget 调整为由 `send_with_ack` 自己覆盖完整 `connect + ack`，并重新安装 Android Debug 包；设备 `com.unimaker.native.debug` 更新时间为 `2026-03-31 00:32:43` |
