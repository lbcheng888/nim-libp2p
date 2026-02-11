## MsQuic API/FFI 通用模型定义，覆盖 `msquic.h` 与 `msquic.hpp` 的关键概念。

type
  QuicHandleKind* = enum
    qhkApiTable              ## `QUIC_API_TABLE` 函数集本身。
    qhkRegistration          ## 注册级别句柄（`HQUIC` with registration semantics）。
    qhkConfiguration         ## 配置句柄。
    qhkListener              ## 监听句柄。
    qhkConnection            ## 连接句柄。
    qhkStream                ## 流句柄。
    qhkDatagram              ## 数据报通道，依附连接。
    qhkExecution             ## 预览中的执行上下文。
    qhkConnectionPool        ## 预览中的连接池。
    qhkAny                   ## 通用 `HQUIC` 句柄。

  ApiFunctionCategory* = enum
    afInfrastructure         ## 打开/关闭 MsQuic 或元数据查询。
    afUtility                ## 通用上下文或参数访问。
    afParameters             ## `SetParam` / `GetParam` 族。
    afRegistration           ## 注册生命周期管理。
    afConfiguration          ## 配置与凭据处理。
    afListener               ## 监听器控制。
    afConnection             ## 连接操作。
    afStream                 ## 流相关操作。
    afDatagram               ## 数据报接口。
    afPreviewOnly            ## 仅在预览特性打开时可用。

  ApiAvailability* = object
    introduced*: string
    deprecated*: string
    requiresPreview*: bool
    commentary*: string

  ApiFunctionSpec* = object
    cSymbol*: string
    nimSymbol*: string
    category*: ApiFunctionCategory
    primaryHandle*: QuicHandleKind
    availability*: ApiAvailability
    summary*: string

  CppWrapperSpec* = object
    typeName*: string
    wraps*: seq[QuicHandleKind]
    summary*: string
    lifecycle*: string
    notes*: string

  NimApiFunctionDraft* = object
    name*: string
    params*: seq[string]
    returnType*: string
    boundC*: string
    stage*: string
    semantics*: string

  MsQuicApiSurface* = object
    version*: string
    functions*: seq[ApiFunctionSpec]
    wrappers*: seq[CppWrapperSpec]
    compatibilityNotes*: seq[string]
    nimDraft*: seq[NimApiFunctionDraft]

  QuicExecutionProfile* = enum
    qepLowLatency            ## `QUIC_EXECUTION_PROFILE_LOW_LATENCY`
    qepMaxThroughput         ## `TYPE_MAX_THROUGHPUT`
    qepScavenger             ## `TYPE_SCAVENGER`
    qepRealTime              ## `TYPE_REAL_TIME`

  QuicRegistrationConfig* = object
    appName*: string
    profile*: QuicExecutionProfile
    autoCloseConnections*: bool
    enableExecutionPolling*: bool

  QuicConfigurationDraft* = object
    alpnList*: seq[string]
    credentialSource*: string
    settingsProfile*: string

  QuicListenerDraft* = object
    localAddress*: string
    serverName*: string
    alpn*: seq[string]
    workerHint*: string

  QuicConnectionDraft* = object
    target*: string
    alpn*: seq[string]
    resumptionLevel*: string
    enableEarlyData*: bool

  QuicStreamDraft* = object
    isUnidirectional*: bool
    openFlags*: uint32
    startFlags*: uint32
    shutdownFlags*: uint32

  QuicSendFlags* = distinct uint32
  QuicStreamOpenFlags* = distinct uint32
  QuicStreamStartFlags* = distinct uint32
  QuicStreamShutdownFlags* = distinct uint32
  QuicConnectionShutdownFlags* = distinct uint32
  RegistrationHandle* = distinct pointer
  ConfigurationHandle* = distinct pointer
  ListenerHandle* = distinct pointer
  ConnectionHandle* = distinct pointer
  StreamHandle* = distinct pointer
  ExecutionHandle* = distinct pointer
  ConnectionPoolHandle* = distinct pointer

const
  SendAllowZeroRtt* = QuicSendFlags(0x0001)
  SendStartOnQueue* = QuicSendFlags(0x0002)
  SendFin* = QuicSendFlags(0x0004)
  SendDatagramPriority* = QuicSendFlags(0x0008)
  SendDelay* = QuicSendFlags(0x0010)

  StreamOpenUnidirectional* = QuicStreamOpenFlags(0x0001)
  StreamOpenZeroRtt* = QuicStreamOpenFlags(0x0002)
  StreamOpenDelayIdFcUpdates* = QuicStreamOpenFlags(0x0004)
  StreamOpenAppOwnedBuffers* = QuicStreamOpenFlags(0x0008)

  StreamStartImmediate* = QuicStreamStartFlags(0x0001)
  StreamStartFailBlocked* = QuicStreamStartFlags(0x0002)
  StreamStartShutdownOnFail* = QuicStreamStartFlags(0x0004)
  StreamStartIndicatePeerAccept* = QuicStreamStartFlags(0x0008)
  StreamStartPriorityWork* = QuicStreamStartFlags(0x0010)

  StreamShutdownGraceful* = QuicStreamShutdownFlags(0x0001)
  StreamShutdownAbortSend* = QuicStreamShutdownFlags(0x0002)
  StreamShutdownAbortReceive* = QuicStreamShutdownFlags(0x0004)
  StreamShutdownImmediate* = QuicStreamShutdownFlags(0x0008)
  StreamShutdownInline* = QuicStreamShutdownFlags(0x0010)

  ConnectionShutdownSilent* = QuicConnectionShutdownFlags(0x0001)

proc hasFlag*(flags, flag: QuicSendFlags): bool {.inline.} =
  (cast[uint32](flags) and cast[uint32](flag)) != 0

proc hasFlag*(flags, flag: QuicStreamOpenFlags): bool {.inline.} =
  (cast[uint32](flags) and cast[uint32](flag)) != 0

proc hasFlag*(flags, flag: QuicStreamStartFlags): bool {.inline.} =
  (cast[uint32](flags) and cast[uint32](flag)) != 0

proc hasFlag*(flags, flag: QuicStreamShutdownFlags): bool {.inline.} =
  (cast[uint32](flags) and cast[uint32](flag)) != 0

proc hasFlag*(flags, flag: QuicConnectionShutdownFlags): bool {.inline.} =
  (cast[uint32](flags) and cast[uint32](flag)) != 0
