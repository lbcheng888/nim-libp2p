# task_plan

| files | action | verify | done |
| --- | --- | --- | --- |
| `libp2p/connmanager.nim` `tests/testconnmngr.nim` | 修每 peer 连接上限的 off-by-one，并在名额已满时优先替换同方向旧 live 连接，避免新入站被历史残留卡死 | `nim c -r tests/testconnmngr.nim` 通过，`25 OK` | yes |
| `libp2p/transports/tsnet/quicrelay.nim` | listener 持久 accept 流的 `route_status` 校验改走独立 fresh probe，避免校验复用当前 control client 把正在跑的 bridge 连坐打死 | 编译通过，生产节点实跑不再出现 `Too many connections for peer`，继续收 relay listener 真状态 | in_progress |
| `libp2p/transports/tsnet/quiccontrol.nim` `tests/testtsnetquiccontrol_fin.nim` | 把 control 单请求流收成“写完即 half-close”，并补一条“服务端等 EOF 才解码”回归，先把 builtin QUIC 底层请求结束语义钉死 | `nim c -r -d:ssl -d:libp2p_msquic_experimental -d:libp2p_msquic_builtin tests/testtsnetquiccontrol.nim` 为 `7 OK`，`nim c -r -d:ssl -d:libp2p_msquic_experimental -d:libp2p_msquic_builtin tests/testtsnetquiccontrol_fin.nim` 为 `1 OK` | yes |
| `libp2p/transports/tsnet/proxy.nim` `tests/testtsnetproxy.nim` | 修 `proxyRouteSnapshots/sanitizeProxyRoutesUnsafe` 上的半残 key 崩溃，禁止产品节点在 listener route 丢失后因为 proxy registry 脏行直接 SIGSEGV | `nim c -r -d:ssl -d:libp2p_msquic_experimental -d:libp2p_msquic_builtin tests/testtsnetproxy.nim` 为 `9 OK`，本地产品节点重启后 `19125` 不再因这条崩溃掉线 | yes |
| `libp2p/transports/tsnet/quicrelay.nim` | gateway 同一路由重复 `listen/accept` 时，不得覆盖正在 `ready_wait/busy` 的 active listener；先收掉 `accept stream lost attached listener route` | 远端 journal 不再出现 `lost attached listener route`，本地产品节点重启后 Android 再发时 gateway 可继续进入真实 `dial/bridge` 或给出新的准确失败 | in_progress |
| `libp2p/transports/tsnet/quicrelay.nim` `libp2p/transports/msquicstream.nim` `tests/testtsnetquicrelay.nim` | 修 framed reader 对 cached 完整 frame 的漏消费，并清掉 listener restart 后悬挂的 `acceptStream/regStream`，禁止 persistent accept lane 因“缓存里已有完整 incoming 但还继续 read”卡死 | `nim c tests/testtsnetquicrelay.nim` 通过；`cached complete json/binary frame...`、`accept stream can receive incoming...`、`route_status stays published while listener ready is pending` 全绿；本地产品节点重启后 `proxyListenersReady=true`、`listenAddrSource=relay_listeners` | yes |
| `examples/mobile_ffi/tsnet_product_node.nim` | `relayPublishedListenAddrs` 做 nil 安全，`local_info` 不能再把产品节点打崩 | 本地重启后 `local_info` 不再 SIGSEGV | yes |
| `docs/android_native_async_architecture.md` | 固化 Android/native 异步架构约束 | 文档存在且约束清楚 | yes |
| `BridgeFacades.kt` | 发送链改成 hints + native kickoff，不再同步 preconnect；消息页删除 `prepareDirectRoutePeer` 这条错层同步预连接 | Kotlin 编译、安装 APK、生产 UI 发送 | in_progress |
| `ChengLibp2pNative.kt` | 增加 seeded kickoff，把 seed addrs 直接带进 native 异步任务 | Kotlin 编译、安装 APK、生产 UI 发送 | in_progress |
| `libp2p_bridge.cpp` | JNI 暴露 seeded kickoff | Native 编译、安装 APK | in_progress |
| `libnimlibp2p.nim` | send_with_ack kickoff 接收 addresses/source 并传进 async 发送任务 | Android so 编译、安装 APK、生产 UI 发送 | in_progress |
| `NativeAppViewModel.kt` | 发送时不再等待后台 preconnect job | Kotlin 编译、安装 APK、生产 UI 发送 | in_progress |
| `quicrelay.nim` `testtsnetquicrelay.nim` | 去掉 `proxyListenersReady` 的旧缓存假绿，gateway 丢 route 后立即掉回未就绪并触发修复 | 单测通过，本地/远端产品节点反向 DM 恢复 | in_progress |
| `tsnet_product_node.nim` `libnimlibp2p.nim` | 本地和远端 `local_info` 都只发布 `relay_listeners` 里的真实 route，不再按 `tailnetIPs` 合成 `/tsnet` 地址 | 本地与远端 `local_info.listenAddrSource=relay_listeners`，且只返回真实 IPv4 route | yes |
| `tsnet_product_rpc.nim` `docs/tsnet_product_rpc.md` | 远端产品节点操作收成结构化 Nim 工具，禁止再手写 `ssh + JSON` | 本地和远端 `tailnet_status/local_info/connect_exact` 用新工具跑通 | yes |
| `api_impl.nim` `api_loss_recovery_test.nim` | 修复 1-RTT PTO 在 probe 发不出去时的过去时间忙循环；远端被动 close/reset 时立即停 maintenance 并清 recovery 状态 | `api_loss_recovery_test` 通过，gateway CPU 恢复正常，`9444` fresh probe 再次 `Running/Mapped` | yes |
| `tsnet_product_node.nim` | `connect_exact` 同步包装不再把短暂 `missing` 当最终失败 | 产品 RPC 同步 `connect_exact` 不再秒回 `connect_exact_missing_state` | yes |
| `libnimlibp2p.nim` `peerstore.nim` | 把 identify 失败点钉到 `ensurePeerIdentified -> beginProtocolNegotiation/select` | 本地产品节点日志能区分是缺 muxer、协商卡住还是最终 identify 超时 | yes |
| `libnimlibp2p.nim` `tsnet/runtime.nim` `tsnetprovider*.nim` `tsnettransport.nim` | 去掉 `idleWakeLoop` 每秒构整份 `tailnetStatus` 的重路径，改成轻量 listener repair 判定 | 本地产品节点跑 `connect_exact` 时不再在后台状态轮询里崩溃 | yes |
| `quicrelay.nim` | 修 owner 级 relay 状态写回错误，恢复 listener/dial task、listener stage、udp dial state 的真实持久化 | `testtsnetquicrelay.nim` 全绿 | yes |
| `quicrelay.nim` `docs/transports/tsnet_quic_relay_protocol.md` | 给长期控制流写操作加硬超时，超时立即判定控制流失效并重建，禁止再出现“流卡死但 route 还显示 busy/awaiting” | `testtsnetquicrelay.nim` 全绿，长期控制流不再无限挂住 | yes |
| `runtime.nim` | `proxyListenerReadiness` 不再只信本地 ready 集合，而是按 gateway 真实发布态校验 route；发布态一丢就立刻掉回未就绪并触发修复 | `route_not_registered` 不再在远端仍报 `awaiting/ready` 时持续复现 | in_progress |
| `runtime.nim` `libnimlibp2p.nim` `testtsnetquicrelay.nim` | listener 自修判定覆盖“本地仍 await/ready，但 gateway 已经下架 route”的情况，避免 route 被下架后永远不重注册 | 本地回归通过，远端 route 被网关下架后能自动补回 | in_progress |
| `libnimlibp2p.nim` | `tailnetControlRefreshLoop` 只在地址真变化时才触发 `syncAdvertisedPeerInfo`，并给 `spawnTailnetWarmAdvertisedPeerInfo` 加单任务并发门，避免产品节点自己把 listener 发布链刷爆 | 本地单节点 `wait_tailnet_ready/local_info` 秒回且不再 99% CPU；远端同步后不再周期性丢 route | yes |
| `libnimlibp2p.nim` | 修 `send_dm_kickoff` 预序列化直发包缺 `from/mid/timestamp/ackRequested`，保证接收端即使 `conn.peerId` 未就绪也能正确记账入库 | 新一轮 `send_dm_status` 完成后，远端 `dm_rows` 能看到 token | in_progress |
| `libnimlibp2p.nim` | `send_dm_kickoff` 外层若已 `connect_exact` 成功，就直接复用现有连接做 `waitForPeerReady`，不再因为带显式地址而重复走一次 exact dial | 外层 `connect_exact` 成功后，`send_dm_kickoff` 不再报内部 `connect timeout` | in_progress |
| `tsnet_product_node.nim` `libnimlibp2p.nim` `peerstore.nim` | 继续收 Mac→远端产品节点在真实 `relay_listeners` route 上的 `send_dm_timeout`；当前 `connect_exact` 已成功，卡在 identify/DM 发送后半段 | `connect_exact` 成功后，`send_dm` 不再超时，远端 `wait_dm` 能收到 token | yes |
| `libnimlibp2p.nim` | `send_with_ack_kickoff` 内层异步发送命令必须继承 `requestKey`，否则 Android 只能看到永远不前进的 `kickoff` 假状态 | Android `timeout_status.payload.stage` 不再固定卡在 `kickoff` | yes |
| `libnimlibp2p.nim` `dmservice.nim` | 把 `send_with_ack` 内层 DM 发送过程接进 kickoff 状态机，明确暴露 `fresh_dial/write/ack_wait/ack_done` | Android 再发一次时，状态不再只停在 `send_begin`，而能看到具体卡点 | in_progress |
| `NativeAppViewModel.kt` | `configureNativeConnectivity` 收成单飞 + 冷却窗口，发送期不再被同签名高频刷新反复抢 native | Android logcat 出现 `skip=cooldown/in_flight`，生产 UI 失败类型不再回到 `peer_ready_failed` | yes |
| `MainActivity.kt` | deep link 必须从 `intent.data` 解析 truth route，不能只认 `route` extra | 冷启动可直接代码切到目标节点消息页，并带上 `seedAddrsJson/draftText` | yes |
| `BridgeFacades.kt` `libnimlibp2p.nim` | 继续收 Android 生产 UI 当前 `send_with_ack_timeout@send_begin`；现在页面正确、draft 正确、消息真正发出，但 Mac 仍未收到 | Android 生产 UI 单发 token 后，本地 Mac `wait_dm` 能收到 | in_progress |
| `BridgeFacades.kt` `NativeAppViewModel.kt` | Android 生产 UI 自动发送动作必须跟随当前页面真实输入框和发送按钮位置，不能复用旧坐标 | 重装 APK 后自动输入/发送仍能稳定触发一次真实发送 | in_progress |
| `NativeAppViewModel.kt` `ChengLibp2pNative.kt` | 发送生命周期必须全程保持 interactive native priority，不能 12 秒后让后台 `getConnectedPeers/socialListDiscoveredPeers` 恢复直打 native 并触发 `FD_SET>=1024` 崩溃 | Android 发送期间不再被系统拉回桌面，crash buffer 不再出现 `FD_SET` 越界 | in_progress |
| `libp2p/transports/tsnet/quicrelay.nim` `libnimlibp2p.nim` `examples/mobile_ffi/tsnet_product_node.nim` | 继续收 Android 生产 UI 这轮新的真实 `peer_ready_failed 10s connect timeout`；当前本地 listener 稳态已恢复，但发送还没走到 `incoming/ready`，说明 blocker 已前移回 connect 阶段 | Android 生产 UI 再发时，不再是 `peer_ready_failed connect timeout`，且本地 `wait_dm` 能收到 token | in_progress |
