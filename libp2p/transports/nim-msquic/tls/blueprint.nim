## 覆盖 A3 原子任务的 TLS 与安全接入蓝图。

import std/sequtils

type
  TlsLayer* = enum
    tlInterfaceGlue ## `crypto_tls.c` / `crypto.c` 与 Nim 之间的握手编排。
    tlSecretSchedule ## QUIC 密钥派生与密钥材料生命周期管理。
    tlBackendAdapter ## 跨平台 `CxPlatTls*` 具体实现的绑定层。
    tlCredentialManagement ## 证书、密钥与信任根加载的抽象。
    tlResumptionSupport ## 0-RTT 与会话票据缓存的生命周期。

  TlsComponent* = object
    name*: string
    layer*: TlsLayer
    cSources*: seq[string]
    responsibilities*: seq[string]
    nimFacade*: string
    ffiTouchpoints*: seq[string]
    remarks*: string

  TlsBackendKind* = enum
    tbSchannel
    tbOpenSsl
    tbQuicTls

  PlatformKind* = enum
    pkWindows
    pkLinux
    pkFreeBsd
    pkMacOs

  PlatformBackendMapping* = object
    platform*: PlatformKind
    defaultBackend*: TlsBackendKind
    supportedBackends*: seq[TlsBackendKind]
    provider*: string
    cSources*: seq[string]
    transports*: seq[string]
    notes*: seq[string]

  RiskSeverity* = enum
    riskLow
    riskMedium
    riskHigh

  SecurityRisk* = object
    id*: string
    area*: string
    severity*: RiskSeverity
    description*: string
    triggerPoints*: seq[string]
    mitigation*: seq[string]

const
  tlsIntegrationDesign*: seq[TlsComponent] = @[
    TlsComponent(
      name: "QuicTlsDriver",
      layer: tlInterfaceGlue,
      cSources: @["src/core/crypto_tls.c", "src/core/crypto.c"],
      responsibilities: @[
        "管理 ClientHello/Transport Parameter 的编码、解析与校验",
        "协调 QUIC-TLS 消息流与连接状态机，负责握手状态推进",
        "暴露 `QuicTls` 事件回调给上层，引导密钥更新、会话票据与 0-RTT 路径"
      ],
      nimFacade: "提供 `proc driveHandshake(ctx: TlsContextRef, input: HandshakePayload): HandshakeResult` 封装，配合 Nim Future 适配 MsQuic 事件循环。",
      ffiTouchpoints: @[
        "CxPlatTlsProcessData",
        "CxPlatTlsGenerateSelfSignedCert",
        "QuicCryptoInitialize"
      ],
      remarks: "需要保持与 MsQuic 预期的 PASSIVE_LEVEL 回调语义一致，避免 Nim GC 在 DISPATCH_LEVEL 上运行。"
    ),
    TlsComponent(
      name: "KeyScheduleManager",
      layer: tlSecretSchedule,
      cSources: @["src/core/crypto.c", "src/core/packet_builder.c"],
      responsibilities: @[
        "按 epoch 管理握手/1-RTT 密钥，覆盖 Secret Derive、Key Update、AEAD 密钥缓存",
        "协调 `QUIC_PACKET_SPACE` 与 TLS Secret 的生命周期，确保重传路径同步",
        "提供密钥擦除与审计钩子，满足合规要求"
      ],
      nimFacade: "定义 `type SecretDerivationHook* = proc (labels: QuicHkdfLabel): DerivedSecret` 与 `proc rotateKeys*` 等操作入口。",
      ffiTouchpoints: @[
        "QuicCryptoCombineIvAndPacketNumber",
        "QuicEncrypt",
        "QuicCryptoKeyWrite"
      ],
      remarks: "Nim 侧需要使用 `{.noExec.}`/`{.noGc.}` 保障密钥缓冲区不被移动，必要时引入自定义内存池。"
    ),
    TlsComponent(
      name: "PlatformTlsAdapter",
      layer: tlBackendAdapter,
      cSources: @["src/platform/tls_openssl.c", "src/platform/tls_quictls.c", "src/platform/tls_schannel.c"],
      responsibilities: @[
        "统一 `CxPlatTls*` 函数族到 Nim FFI 的调用约定和线程模型",
        "描述后端特性差异（ALPN 长度上限、KeyUpdate 支持、Early Data 能力）",
        "处理平台证书回调线程切换，确保 Nim 运行时安全"
      ],
      nimFacade: "宣告 `proc cxPlatTlsProcess*(ctx: pointer, params: var TlsProcessParams): TlsProcessResult {.cdecl, importc.}` 等符号映射与 Nim 包装。",
      ffiTouchpoints: @[
        "CxPlatTlsClientProcessData",
        "CxPlatTlsServerProcessData",
        "CxPlatTlsSetCallbacks"
      ],
      remarks: "需要在 Nim 初始化阶段将回调导出给各后端，避免在握手路径重复设置。"
    ),
    TlsComponent(
      name: "CredentialAuthority",
      layer: tlCredentialManagement,
      cSources: @["src/platform/crypt_bcrypt.c", "src/platform/crypt_openssl.c", "src/platform/cert.c"],
      responsibilities: @[
        "加载用户或系统证书，支持 PEM/PFX/系统证书存储三种入口",
        "管理信任根、证书链缓存与动态更新通知",
        "与应用层 API (`MsQuicCredentialLoad`) 对齐错误语义与日志"
      ],
      nimFacade: "定义 `proc loadCredential*(descriptor: CredentialDescriptor): CredentialHandle`，结合 Nim 异步文件 IO/配置读取。",
      ffiTouchpoints: @[
        "CxPlatSecConfigCreate",
        "CxPlatGetSelfSignedCert",
        "MsQuicSetParam(MsQuicParameter.CredentialReset)"
      ],
      remarks: "文件句柄与敏感数据需要在 Nim 侧落盘策略中及时销毁，避免 GC 延迟释放。"
    ),
    TlsComponent(
      name: "ResumptionTicketStore",
      layer: tlResumptionSupport,
      cSources: @["src/core/crypto_tls.c", "src/core/connection.c"],
      responsibilities: @[
        "跟踪 TLS 会话票据回调，维护 0-RTT 票据与应用层缓存接口",
        "为客户端/服务器提供自定义票据持久化与失效策略",
        "确保票据与路径验证（PATH_VALIDATED）状态保持一致"
      ],
      nimFacade: "暴露 `proc enqueueTicket*(connId: ConnectionId, ticket: TicketBlob)` 与 `proc consumeTicket*`，可挂接至应用层缓存。",
      ffiTouchpoints: @[
        "CxPlatTlsReadTicket",
        "CxPlatTlsWriteTicket",
        "CxPlatTlsSecConfigDelete"
      ],
      remarks: "需要明确多线程访问策略，避免 Nim channel 与 MsQuic worker 线程之间的竞态。"
    )
  ]

  platformBackendMatrix*: seq[PlatformBackendMapping] = @[
    PlatformBackendMapping(
      platform: pkWindows,
      defaultBackend: tbSchannel,
      supportedBackends: @[tbSchannel],
      provider: "SChannel / CNG",
      cSources: @["src/platform/tls_schannel.c", "src/platform/crypt_bcrypt.c"],
      transports: @[
        "CxPlatTlsClient/ServerProcessData",
        "CxPlatTlsGenerateSelfSignedCert"
      ],
      notes: @[
        "系统证书存储 + CNG 密钥，满足 FIPS 验证",
        "支持 ALPN、0-RTT，会话票据存储在注册表缓存中",
        "受限于 Windows 线程模型，回调不得在 DISPATCH_LEVEL 阻塞"
      ]
    ),
    PlatformBackendMapping(
      platform: pkLinux,
      defaultBackend: tbQuicTls,
      supportedBackends: @[tbQuicTls, tbOpenSsl],
      provider: "quictls (OpenSSL 分支)",
      cSources: @["src/platform/tls_quictls.c", "src/platform/crypt_openssl.c"],
      transports: @[
        "CxPlatTls* 与 quictls EVP 接口",
        "MsQuicOpenSslSessionCache"
      ],
      notes: @[
        "默认启用 quictls 以获得 QUIC 专用 API，兼容 OpenSSL 1.1.1 接口",
        "需要与 Nim 封装共享 `BIO` 内存管理，避免双重释放",
        "支持自定义 trust store，需要实现文件监控刷新逻辑"
      ]
    ),
    PlatformBackendMapping(
      platform: pkFreeBsd,
      defaultBackend: tbOpenSsl,
      supportedBackends: @[tbOpenSsl],
      provider: "OpenSSL 1.1.x/3.x",
      cSources: @["src/platform/tls_openssl.c", "src/platform/crypt_openssl.c"],
      transports: @[
        "CxPlatTls*",
        "CxPlatGetSelfSignedCert(OpenSsl)"
      ],
      notes: @[
        "依赖系统包管理的 OpenSSL，需检测 QUIC 扩展是否启用",
        "0-RTT ticket 功能需要显式开启 `ssl_conf`",
        "与 epoll/kqueue 线程模型解耦，回调需下派到 Nim scheduler"
      ]
    ),
    PlatformBackendMapping(
      platform: pkMacOs,
      defaultBackend: tbQuicTls,
      supportedBackends: @[tbQuicTls],
      provider: "quictls + SecureTransport 证书访问",
      cSources: @["src/platform/tls_quictls.c", "src/platform/crypt_openssl.c"],
      transports: @[
        "CxPlatTls*",
        "自定义 Keychain 加载逻辑（需 Nim FFI 实现）"
      ],
      notes: @[
        "系统 SecureTransport 对 QUIC 支持不足，统一采用 quictls",
        "Keychain 访问需要在主线程发起，Nim 层需提供代理任务",
        "Apple Silicon 目标需确保 quictls 使用 ARM64 汇编路径"
      ]
    )
  ]

  securityRiskRegister*: seq[SecurityRisk] = @[
    SecurityRisk(
      id: "TLS-R001",
      area: "回调线程安全",
      severity: riskHigh,
      description: "CxPlatTls 回调在 PASSIVE_LEVEL，但平台实现可能在 worker 线程触发，Nim 运行时若在回调中触发 GC 或 await 会造成死锁或栈破坏。",
      triggerPoints: @[
        "CxPlatTlsProcessData -> CertificateReceived 回调",
        "CxPlatTlsProcessData -> ReceiveTicket 回调"
      ],
      mitigation: @[
        "所有 Nim 回调使用 `{.gcsafe.}` + 手工管理的固定缓冲区",
        "将回调数据复制到 lock-free queue，交给 Nim 主调度线程处理",
        "在调试版本开启线程亲和日志，确保未跨线程触发 GC"
      ]
    ),
    SecurityRisk(
      id: "TLS-R002",
      area: "密钥材料生命周期",
      severity: riskHigh,
      description: "派生出的秘密（handshake secret / 1-RTT secret）若未及时擦除，可能在 Nim 堆或复制缓冲中残留。",
      triggerPoints: @[
        "QuicCryptoInitializeSecrets",
        "QuicCryptoKeyWrite",
        "CxPlatTlsWriteTicket"
      ],
      mitigation: @[
        "采用固定大小的 `SecureBuffer` 对象，释放时调用 `volatile memset`",
        "在 Nim 层包装密钥时使用 `{.noGc.}` 并显式 `zeroMem`",
        "对调试构建加入内存扫描，验证密钥不会出现在 heap dump 中"
      ]
    ),
    SecurityRisk(
      id: "TLS-R003",
      area: "跨语言结构封装",
      severity: riskMedium,
      description: "Transport Parameter 与 ALPN 解析依赖严格的长度校验，若 Nim FFI 封装未验证 `Length`，可能导致缓冲区越界或 DoS。",
      triggerPoints: @[
        "QuicCryptoTlsReadTransportParameters",
        "CxPlatTlsProcessData -> ClientHello Inspect"
      ],
      mitigation: @[
        "在 Nim 层二次校验 varint，拒绝超过 `CXPLAT_TP_MAX_LENGTH` 的输入",
        "统一使用 `checkedSlice` 提供的安全指针",
        "对 fuzz 测试暴露 Nim 包装，使用现有 TLS fuzz corpus 复用"
      ]
    ),
    SecurityRisk(
      id: "TLS-R004",
      area: "依赖版本老化",
      severity: riskMedium,
      description: "quictls/OpenSSL 更新节奏与 MsQuic 不一致，旧版本可能缺少必要的安全修复或 QUIC 扩展。",
      triggerPoints: @[
        "构建阶段链接 quictls 旧版",
        "运行时发现 TLS 1.3 关键扩展缺失"
      ],
      mitigation: @[
        "建立 Nim `dependency_checks.nim`，在初始化阶段验证 `SSL_get_version` 与 QUIC 扩展开关",
        "CI 中加入 `openssl version -v` 与 CVE 数据库比对",
        "提供后备配置，允许回退到系统 OpenSSL 并降级到禁用 0-RTT"
      ]
    ),
    SecurityRisk(
      id: "TLS-R005",
      area: "平台差异与证书访问",
      severity: riskLow,
      description: "Windows 使用系统证书存储，类 UNIX 走文件证书，差异可能导致部署脚本错配。",
      triggerPoints: @[
        "Credential 加载时选择错误的证书来源",
        "自动化脚本未配置 Keychain 权限"
      ],
      mitigation: @[
        "封装统一的 `CredentialDescriptor`，显式声明来源类型",
        "将部署脚本与 Nim API 的默认值对齐，提供示例配置",
        "在启动日志中输出证书提供者与路径"
      ]
    )
  ]

proc recommendedBackend*(platform: PlatformKind): TlsBackendKind =
  ## 返回给定平台默认推荐的 TLS 后端。
  for mapping in platformBackendMatrix:
    if mapping.platform == platform:
      return mapping.defaultBackend
  raise newException(ValueError, "unknown platform mapping: " & $platform)

proc criticalRisks*(): seq[SecurityRisk] =
  ## 返回需要优先缓解的高风险项列表。
  securityRiskRegister.filterIt(it.severity == riskHigh)

proc backendByKind*(backend: TlsBackendKind): PlatformBackendMapping =
  ## 查找第一个使用指定 TLS 后端的映射，用于生成工件或文档示例。
  for mapping in platformBackendMatrix:
    if backend in mapping.supportedBackends:
      return mapping
  raise newException(ValueError, "unsupported backend: " & $backend)
