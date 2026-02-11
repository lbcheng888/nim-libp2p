# Unimaker 移动端集成 nim-libp2p Updater 指南

本文档说明如何在 Android、鸿蒙 (HarmonyOS) 与 iOS/iPadOS 应用内集成 `nim-libp2p` 提供的更新代理（`updater/` 目录），实现终端自动拉取并安装最新版本，避免版本碎片。

---

## 1. 组件概览
- **Updater 守护进程**：基于 `updater/main.nim`，负责订阅 GossipSub 主题、解析 IPNS/DHT 指针、拉取 Manifest 与安装包，并向应用层暴露当前版本状态。
- **Manifest**：`updater/manifest.nim` 定义的结构，包含渠道、`sequence`、版本号、包哈希、差分信息等，并通过 SignedEnvelope 签名保障可信。
- **配置文件**：参考 `updater/example_config.json`，指定监听地址、引导节点、主题名称、IPNS/Fallback key 及轮询周期等。
- **内容分发**：实际 APK/HAP/差分包通过 Bitswap/GraphSync 分发，Manifest 记录对应 CID，确保全网节点能一致获取。

集成流程即：在终端部署守护 → 与应用建立 IPC → 在 CI/CD 发版时生成 Manifest → 守护根据 Manifest 下载并驱动安装。

---

## 2. 通用集成步骤
1. **编译守护**
   - 在工程中添加 `updater/` 源文件；对 Android 使用 NDK + nimble 编译成本地可执行或静态库；iOS/鸿蒙可编译成 CLI 工具或后台守护。
   - 示例命令：`nim c -d:release updater/main.nim` 生成 `updater` 可执行。

2. **部署守护**
   - 将二进制与配置文件打包进 App 安装包；配置文件可在安装时根据环境（渠道/节点列表）覆盖。
   - 确保守护存储位置应用可读写，并允许其持久运行（Android 推荐放入 `app_data/`；iOS 可放入共享容器/Document；鸿蒙放入应用 sandbox）。

3. **启动与监控**
   - App 启动时检测守护是否运行（例如检查本地 Unix Domain Socket / TCP 端口）；未运行则拉起守护。
   - 建立 IPC（可选 HTTP、本地 socket、AIDL、Ability Connect），以查询版本状态、安装进度与错误信息。

4. **处理 Manifest**
   - 守护监听 `unimaker/mobile/version` 主题，解析 Manifest；如序号高于本地记录则下载对应包，写入缓存并更新状态。
   - 应用层读取状态后触发平台安装 API（见下节各平台流程），安装成功后通知守护更新本地 `sequence`。

5. **安全与校验**
   - 守护验证 Manifest 签名、校验哈希，拒绝低序号或未签名记录。
   - 在应用层对安装包进行二次哈希校验，可选搭配可信执行环境/签名校验。

### 2.1 UI 与用户交互
- Updater 属于后台能力，但建议在应用中提供最少化的可视反馈：
  - **升级提示/更新说明**：遵循应用商店规范，提示版本变化与权限请求。即便企业场景静默安装，也应保留“当前版本/最新版本”页面便于排查。
  - **进度与错误**：通过 IPC 获取守护状态，在 UI 中展示“下载中/安装中/失败重试”等信息。
  - **回滚/延后**：如需允许用户暂缓升级，可提供操作入口通知守护暂停或恢复安装。
- 对于完全静默升级（企业/MDM），可隐藏实时弹窗，仅在设置页展示只读状态。

---

## 3. Android 集成
### 3.1 守护运行
- 使用 `WorkManager`、`JobScheduler` 或常驻 Service 持续运行守护。守护可通过 `ProcessBuilder` 拉起本地可执行，或直接编译成 JNI 动态库在独立线程中执行。
- 配置文件 (`updater/example_config.json`) 随安装写入 `files/updater/config.json`，启动时传入路径 `nim r updater/main.nim files/updater/config.json`。

### 3.2 IPC 与状态管理
- 建议提供一个 AIDL 或绑定 Service，守护将当前 Manifest、下载进度、安装要求写入共享存储（如 Room/SQLite/SharedPreferences）。
- 应用层界面读取状态，提示用户升级；企业场景可自动完成静默升级。

### 3.3 安装流程
- **企业/系统签名**：使用 `PackageInstaller.Session` 实现无人值守安装。守护下载完毕后将 APK 写入临时文件，调用 Service 通知应用执行安装。
- **Play 商店**：集成 Play Core In-App Update，守护下载包体后提供 `FileProvider` URI 给 Play Core，触发柔性/立即更新流程。
- 安装完成后更新本地 `sequence` 并清理旧包，避免碎片。

---

## 4. 鸿蒙 (HarmonyOS) 集成
### 4.1 守护运行
- 在 `StageAbility` 中注册 `BackgroundTaskScheduler` 或使用系统保活能力，确保守护获取执行时间。
- 守护可作为 `ServiceAbility` 或独立 native 进程运行，配置写于 ETS 或 native 配置目录。

### 4.2 IPC 与状态
- 使用 `DataAbilityHelper` 或 `DistributedDataService` 存储 Manifest 状态，方便多设备同步。
- 应用层 Ability 定期查询守护状态，若检测到新版本则调度 UI/后台安装。

### 4.3 安装流程
- 调用 `SystemInstaller.installPackage` 完成 `.hap`/`.hpkg` 安装。若为差分包，守护应在安装前执行合并。
- 安装成功后通知守护更新序号，并记录最新 Manifest，其他设备即可复用缓存文件，避免重复下载。

---

## 5. iOS / iPadOS 集成
### 5.1 守护运行
- iOS 不允许长期常驻进程，可采用以下策略之一：
  - **使用 App Extensions + BGAppRefreshTask**：将 Updater 编译成命令行工具，配合后台刷新任务定期唤醒；
  - **企业分发/MDM 场景**：配合企业证书，守护通过自建 MDM 代理或签名脚本触发安装；
  - **前台保活**：在 App 前台时执行更新检查，后台则使用静默推送或短时后台任务完成拉取。
- 配置文件放入共享容器 (App Group) 以便主 App / 扩展共享。

### 5.2 Manifest 处理
- 守护下载 Manifest 后，将所需资源写入 App Group 目录，并通过 `NSFileCoordinator` 通知主应用。
- 对于热更新资源（JS/CSS/Lua 等），主应用可直接解压覆盖；二进制更新需遵守苹果政策，只能提示用户通过 App Store 升级或使用企业签名通道。

### 5.3 安装策略
- **热更新**：将资源包解压到本地，更新后标记当前 `sequence`；若失败可回退至旧版本。
- **企业分发**：守护下载 `ipa` 后可调用自研安装器或借助 MDM API (`itms-services`) 下发安装链接。
- 无论哪种方式，始终以 Manifest 序号为准，旧序号立即拒绝，防止回退造成碎片。

---

## 6. CI/CD 与运维建议
1. **版本生成**
   - 构建三端产物；为每个渠道生成差分/全量包；通过多重签名写入 Manifest（调用 `SignedManifest.init`）。
   - 将 Manifest 发布到 GossipSub、IPNS/DHT；更新 `unimaker/mobile/version` 主题。

2. **密钥管理**
   - 将签名私钥存放在 HSM/软件保险箱；Manifest 需要多方审批后发布。
   - 建立撤销列表（RevocationList），守护在处理 Manifest 前校验。

3. **监控与告警**
   - 启用 `metrics_exporter` 输出 IPNS 刷新、下载成功率、安装状态，接入 Prometheus/Grafana。
   - 守护日志建议写入本地循环缓冲并上报后端，便于追溯。

4. **回滚机制**
   - Manifest 中保留 `previousCid`，守护在检测到重大故障时可回退到上一版本。
   - CI/CD 保留至少两版差分，便于快速恢复。

---

## 7. 常见问题与排查
- **跨平台是否同步更新**：Manifest 同时携带三端产物，只要任一发布节点发布新 Manifest，其余平台守护会在订阅到消息后下载对应平台 artifact，实现序号统一。若某平台产物缺失，请检查 CI/CD 是否在 Manifest 中写入该平台的 CID。
- **守护未获取最新版本**：检查 GossipSub 连接、IPNS 解析是否成功（可通过 metrics 查看）；确保配置的引导节点在线。
- **安装失败**：确认包体哈希与 Manifest 一致；Android 检查签名权限，iOS 验证是否触犯苹果政策，鸿蒙确认权限。
- **版本碎片**：主要因逻辑未校验 Manifest 序号或存在多守护实例，可通过唯一锁/状态机避免重复安装。

---

## 8. 参考文件
- `updater/config.nim`：守护配置结构
- `updater/manifest.nim`：Manifest 编码/解析
- `updater/updater.nim`：核心逻辑实现，可扩展 IPNS 解析或自定义回退
- `updater/main.nim`：命令行入口与信号处理
- `docs/mobile_update_consistency.md`：总体方案设计
- `docs/mobile_updater_integration.md`（本文）

---

集成完成后，移动端只需保持守护长期运行，即可在任意节点发布新版本时自动拉取、校验并触发安装，确保所有设备始终同步最新版本，无版本碎片。