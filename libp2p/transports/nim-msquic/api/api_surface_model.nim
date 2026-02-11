## MsQuic API/FFI 蓝图，聚焦 `QUIC_API_TABLE` 与 `msquic.hpp` 的 Nim 端映射。

import ./common

func availability(introduced: string; deprecated = ""; preview = false;
                  commentary = ""): ApiAvailability =
  ApiAvailability(
    introduced: introduced,
    deprecated: deprecated,
    requiresPreview: preview,
    commentary: commentary)

let MsQuicApiFunctionsV2*: seq[ApiFunctionSpec] = @[
  ApiFunctionSpec(
    cSymbol: "MsQuicOpenVersion",
    nimSymbol: "msquicOpenVersion",
    category: afInfrastructure,
    primaryHandle: qhkApiTable,
    availability: availability("2.0", commentary = "QUIC_STATUS QUIC_API MsQuicOpenVersion(uint32_t, const void**)"),
    summary: "按请求版本装载 `QUIC_API_TABLE`，提供 Nim 端后续调用的函数指针入口。"),
  ApiFunctionSpec(
    cSymbol: "MsQuicClose",
    nimSymbol: "msquicClose",
    category: afInfrastructure,
    primaryHandle: qhkApiTable,
    availability: availability("2.0", commentary = "void QUIC_API MsQuicClose(const void*)"),
    summary: "释放 `MsQuicOpenVersion` 获取的函数表引用，遵循显式引用计数语义。"),
  ApiFunctionSpec(
    cSymbol: "SetContext",
    nimSymbol: "setHandleContext",
    category: afUtility,
    primaryHandle: qhkAny,
    availability: availability("2.0", commentary = "QUIC_SET_CONTEXT_FN"),
    summary: "为任意 `HQUIC` 句柄绑定自定义上下文指针，回调中可取回 Nim 对象。"),
  ApiFunctionSpec(
    cSymbol: "GetContext",
    nimSymbol: "getHandleContext",
    category: afUtility,
    primaryHandle: qhkAny,
    availability: availability("2.0", commentary = "QUIC_GET_CONTEXT_FN"),
    summary: "读取句柄当前关联的上下文指针，用于回调层对象映射。"),
  ApiFunctionSpec(
    cSymbol: "SetCallbackHandler",
    nimSymbol: "setCallbackHandler",
    category: afUtility,
    primaryHandle: qhkAny,
    availability: availability("2.0", commentary = "QUIC_SET_CALLBACK_HANDLER_FN"),
    summary: "注册 MsQuic 事件回调；Nim 必须提供 C ABI 兼容的桥接函数。"),
  ApiFunctionSpec(
    cSymbol: "SetParam",
    nimSymbol: "setParam",
    category: afParameters,
    primaryHandle: qhkAny,
    availability: availability("2.0", commentary = "QUIC_SET_PARAM_FN"),
    summary: "通过 ID 写入 `QUIC_PARAM_*` 结构体，覆盖全局、注册、连接等级参数。"),
  ApiFunctionSpec(
    cSymbol: "GetParam",
    nimSymbol: "getParam",
    category: afParameters,
    primaryHandle: qhkAny,
    availability: availability("2.0", commentary = "QUIC_GET_PARAM_FN"),
    summary: "查询 `QUIC_PARAM_*` 参数值，需预留缓冲区并遵循 MsQuic 长度协定。"),
  ApiFunctionSpec(
    cSymbol: "RegistrationOpen",
    nimSymbol: "registrationOpen",
    category: afRegistration,
    primaryHandle: qhkRegistration,
    availability: availability("2.0", commentary = "QUIC_REGISTRATION_OPEN_FN"),
    summary: "创建注册句柄并绑定执行画像，是所有连接/监听的根作用域。"),
  ApiFunctionSpec(
    cSymbol: "RegistrationClose",
    nimSymbol: "registrationClose",
    category: afRegistration,
    primaryHandle: qhkRegistration,
    availability: availability("2.0", commentary = "QUIC_REGISTRATION_CLOSE_FN"),
    summary: "释放注册句柄；在 Nim 层需要确保相关连接已清理。"),
  ApiFunctionSpec(
    cSymbol: "RegistrationShutdown",
    nimSymbol: "registrationShutdown",
    category: afRegistration,
    primaryHandle: qhkRegistration,
    availability: availability("2.0", commentary = "QUIC_REGISTRATION_SHUTDOWN_FN"),
    summary: "通知注册下所有连接进入关闭流程，可用于崩溃退出或服务降级。"),
  ApiFunctionSpec(
    cSymbol: "ConfigurationOpen",
    nimSymbol: "configurationOpen",
    category: afConfiguration,
    primaryHandle: qhkConfiguration,
    availability: availability("2.0", commentary = "QUIC_CONFIGURATION_OPEN_FN"),
    summary: "根据 ALPN、设置及 TLS 信息创建配置对象，为连接握手提供上下文。"),
  ApiFunctionSpec(
    cSymbol: "ConfigurationClose",
    nimSymbol: "configurationClose",
    category: afConfiguration,
    primaryHandle: qhkConfiguration,
    availability: availability("2.0", commentary = "QUIC_CONFIGURATION_CLOSE_FN"),
    summary: "释放配置句柄；可在最后一个引用断开后回收 TLS 资源。"),
  ApiFunctionSpec(
    cSymbol: "ConfigurationLoadCredential",
    nimSymbol: "configurationLoadCredential",
    category: afConfiguration,
    primaryHandle: qhkConfiguration,
    availability: availability("2.0", commentary = "QUIC_CONFIGURATION_LOAD_CREDENTIAL_FN"),
    summary: "加载服务端或客户端凭据结构，触发 TLS 堆栈初始化。"),
  ApiFunctionSpec(
    cSymbol: "ListenerOpen",
    nimSymbol: "listenerOpen",
    category: afListener,
    primaryHandle: qhkListener,
    availability: availability("2.0", commentary = "QUIC_LISTENER_OPEN_FN"),
    summary: "在注册作用域内创建监听器，登记回调并准备绑定。"),
  ApiFunctionSpec(
    cSymbol: "ListenerClose",
    nimSymbol: "listenerClose",
    category: afListener,
    primaryHandle: qhkListener,
    availability: availability("2.0", commentary = "QUIC_LISTENER_CLOSE_FN"),
    summary: "销毁监听器句柄，终止后续连接接受。"),
  ApiFunctionSpec(
    cSymbol: "ListenerStart",
    nimSymbol: "listenerStart",
    category: afListener,
    primaryHandle: qhkListener,
    availability: availability("2.0", commentary = "QUIC_LISTENER_START_FN"),
    summary: "指定 ALPN 与端点启动监听，进入被动接受状态。"),
  ApiFunctionSpec(
    cSymbol: "ListenerStop",
    nimSymbol: "listenerStop",
    category: afListener,
    primaryHandle: qhkListener,
    availability: availability("2.0", commentary = "QUIC_LISTENER_STOP_FN"),
    summary: "停止监听端点接受新连接，保持现有连接不变。"),
  ApiFunctionSpec(
    cSymbol: "ConnectionOpen",
    nimSymbol: "connectionOpen",
    category: afConnection,
    primaryHandle: qhkConnection,
    availability: availability("2.0", commentary = "QUIC_CONNECTION_OPEN_FN"),
    summary: "创建连接句柄并注册事件回调，尚未发起握手。"),
  ApiFunctionSpec(
    cSymbol: "ConnectionClose",
    nimSymbol: "connectionClose",
    category: afConnection,
    primaryHandle: qhkConnection,
    availability: availability("2.0", commentary = "QUIC_CONNECTION_CLOSE_FN"),
    summary: "释放连接句柄引用，若仍在使用则等待 MsQuic 主动关闭。"),
  ApiFunctionSpec(
    cSymbol: "ConnectionShutdown",
    nimSymbol: "connectionShutdown",
    category: afConnection,
    primaryHandle: qhkConnection,
    availability: availability("2.0", commentary = "QUIC_CONNECTION_SHUTDOWN_FN"),
    summary: "向对端发起关闭或静默终止，用于应用层主动断连。"),
  ApiFunctionSpec(
    cSymbol: "ConnectionStart",
    nimSymbol: "connectionStart",
    category: afConnection,
    primaryHandle: qhkConnection,
    availability: availability("2.0", commentary = "QUIC_CONNECTION_START_FN"),
    summary: "使用配置与目标端点启动握手流程，触发连接建立。"),
  ApiFunctionSpec(
    cSymbol: "ConnectionSetConfiguration",
    nimSymbol: "connectionSetConfiguration",
    category: afConnection,
    primaryHandle: qhkConnection,
    availability: availability("2.0", commentary = "QUIC_CONNECTION_SET_CONFIGURATION_FN"),
    summary: "为现有连接绑定或更新配置，支持服务器端 SNI 路由。"),
  ApiFunctionSpec(
    cSymbol: "ConnectionSendResumptionTicket",
    nimSymbol: "connectionSendResumptionTicket",
    category: afConnection,
    primaryHandle: qhkConnection,
    availability: availability("2.0", commentary = "QUIC_CONNECTION_SEND_RESUMPTION_FN"),
    summary: "服务端向客户端发送恢复票据，可携带应用数据。"),
  ApiFunctionSpec(
    cSymbol: "StreamOpen",
    nimSymbol: "streamOpen",
    category: afStream,
    primaryHandle: qhkStream,
    availability: availability("2.0", commentary = "QUIC_STREAM_OPEN_FN"),
    summary: "在连接内创建流句柄，支持 0-RTT 与 ID 延迟更新选项。"),
  ApiFunctionSpec(
    cSymbol: "StreamClose",
    nimSymbol: "streamClose",
    category: afStream,
    primaryHandle: qhkStream,
    availability: availability("2.0", commentary = "QUIC_STREAM_CLOSE_FN"),
    summary: "释放流句柄引用，遵循引用计数模型。"),
  ApiFunctionSpec(
    cSymbol: "StreamStart",
    nimSymbol: "streamStart",
    category: afStream,
    primaryHandle: qhkStream,
    availability: availability("2.0", commentary = "QUIC_STREAM_START_FN"),
    summary: "以指定标志启动流，并可触发立即通知或失败策略。"),
  ApiFunctionSpec(
    cSymbol: "StreamShutdown",
    nimSymbol: "streamShutdown",
    category: afStream,
    primaryHandle: qhkStream,
    availability: availability("2.0", commentary = "QUIC_STREAM_SHUTDOWN_FN"),
    summary: "控制流的收/发半关闭或异常终止行为。"),
  ApiFunctionSpec(
    cSymbol: "StreamSend",
    nimSymbol: "streamSend",
    category: afStream,
    primaryHandle: qhkStream,
    availability: availability("2.0", commentary = "QUIC_STREAM_SEND_FN"),
    summary: "发送数据块并附加标志（FIN、延迟、优先级等）。"),
  ApiFunctionSpec(
    cSymbol: "StreamReceiveComplete",
    nimSymbol: "streamReceiveComplete",
    category: afStream,
    primaryHandle: qhkStream,
    availability: availability("2.0", commentary = "QUIC_STREAM_RECEIVE_COMPLETE_FN"),
    summary: "通知 MsQuic 应用已消费指定字节，驱动背压释放。"),
  ApiFunctionSpec(
    cSymbol: "StreamReceiveSetEnabled",
    nimSymbol: "streamReceiveSetEnabled",
    category: afStream,
    primaryHandle: qhkStream,
    availability: availability("2.0", commentary = "QUIC_STREAM_RECEIVE_SET_ENABLED_FN"),
    summary: "启用或暂停更多数据接收，用于应用级流量控制。"),
  ApiFunctionSpec(
    cSymbol: "DatagramSend",
    nimSymbol: "datagramSend",
    category: afDatagram,
    primaryHandle: qhkConnection,
    availability: availability("2.0", commentary = "QUIC_DATAGRAM_SEND_FN"),
    summary: "发送不可靠数据报片段，复用同一连接上下文。"),
  ApiFunctionSpec(
    cSymbol: "ConnectionResumptionTicketValidationComplete",
    nimSymbol: "connectionResumptionTicketValidationComplete",
    category: afConnection,
    primaryHandle: qhkConnection,
    availability: availability("2.2", commentary = "可选回调完成 API"),
    summary: "应用在异步票据验证后调用，继续服务端握手流程。"),
  ApiFunctionSpec(
    cSymbol: "ConnectionCertificateValidationComplete",
    nimSymbol: "connectionCertificateValidationComplete",
    category: afConnection,
    primaryHandle: qhkConnection,
    availability: availability("2.2", commentary = "TLS 证书验证完成通知"),
    summary: "用于异步证书验证，反馈验证结果给 MsQuic。"),
  ApiFunctionSpec(
    cSymbol: "ConnectionOpenInPartition",
    nimSymbol: "connectionOpenInPartition",
    category: afConnection,
    primaryHandle: qhkConnection,
    availability: availability("2.5", commentary = "跨分区创建连接句柄"),
    summary: "在指定 CPU 分区创建连接，提升亲和性控制。"),
  ApiFunctionSpec(
    cSymbol: "StreamProvideReceiveBuffers",
    nimSymbol: "streamProvideReceiveBuffers",
    category: afPreviewOnly,
    primaryHandle: qhkStream,
    availability: availability("2.5", preview = true, commentary = "预览：应用自管接收缓冲区"),
    summary: "启用应用自填充的流接收缓冲，实现零拷贝策略。"),
  ApiFunctionSpec(
    cSymbol: "ConnectionPoolCreate",
    nimSymbol: "connectionPoolCreate",
    category: afPreviewOnly,
    primaryHandle: qhkConnectionPool,
    availability: availability("2.5", preview = true, commentary = "预览：批量连接池 API"),
    summary: "按配置批量创建连接，均衡分布到多个工作线程。"),
  ApiFunctionSpec(
    cSymbol: "ExecutionCreate",
    nimSymbol: "executionCreate",
    category: afPreviewOnly,
    primaryHandle: qhkExecution,
    availability: availability("2.5", preview = true, commentary = "预览：执行上下文"),
    summary: "创建执行上下文集合，与事件循环/线程绑定。"),
  ApiFunctionSpec(
    cSymbol: "ExecutionDelete",
    nimSymbol: "executionDelete",
    category: afPreviewOnly,
    primaryHandle: qhkExecution,
    availability: availability("2.5", preview = true, commentary = "预览：执行上下文销毁"),
    summary: "释放通过 `ExecutionCreate` 创建的执行上下文数组。"),
  ApiFunctionSpec(
    cSymbol: "ExecutionPoll",
    nimSymbol: "executionPoll",
    category: afPreviewOnly,
    primaryHandle: qhkExecution,
    availability: availability("2.5", preview = true, commentary = "预览：事件轮询"),
    summary: "主动轮询执行上下文，允许应用自控调度。"),
  ApiFunctionSpec(
    cSymbol: "RegistrationClose2",
    nimSymbol: "registrationClose2",
    category: afPreviewOnly,
    primaryHandle: qhkRegistration,
    availability: availability("2.6", preview = true, commentary = "预览：改进关闭，能返回活动计数"),
    summary: "改进的注册关闭接口，可在释放前等待活动连接耗尽。")
]

let MsQuicCppWrappers*: seq[CppWrapperSpec] = @[
  CppWrapperSpec(
    typeName: "MsQuicApi",
    wraps: @[qhkApiTable],
    summary: "C++ RAII 封装的函数表载入器。",
    lifecycle: "构造时调用 `MsQuicOpenVersion`，析构时调用 `MsQuicClose`。",
    notes: "Nim 端需要等价的对象以保证异常退出时仍能关闭 API 表。"),
  CppWrapperSpec(
    typeName: "MsQuicRegistration",
    wraps: @[qhkRegistration],
    summary: "注册句柄的自动关闭封装。",
    lifecycle: "析构触发 `RegistrationClose`，可选自动关闭所有连接。",
    notes: "Nim 层应提供 with-resource 辅助函数管理生命周期。"),
  CppWrapperSpec(
    typeName: "MsQuicConfiguration",
    wraps: @[qhkConfiguration],
    summary: "配置句柄 RAII，内含 `LoadCredential` 便捷方法。",
    lifecycle: "析构调用 `ConfigurationClose`。",
    notes: "Nim 计划提供延迟加载凭据与设置序列化 helper。"),
  CppWrapperSpec(
    typeName: "MsQuicListener",
    wraps: @[qhkListener],
    summary: "监听器封装，构造时需提供注册句柄与回调。",
    lifecycle: "析构调用 `ListenerClose`。",
    notes: "Nim API 需要暴露 start/stop，保持回调栈兼容。"),
  CppWrapperSpec(
    typeName: "MsQuicConnection",
    wraps: @[qhkConnection],
    summary: "连接句柄 RAII，提供 start/shutdown 等成员函数。",
    lifecycle: "析构调用 `ConnectionClose`。",
    notes: "Nim 需确保回调线程安全，匹配 RAII 行为。"),
  CppWrapperSpec(
    typeName: "MsQuicStream",
    wraps: @[qhkStream],
    summary: "流句柄封装，提供 send/receive 操作。",
    lifecycle: "析构调用 `StreamClose`。",
    notes: "Nim 应包装 send/receive 以管理缓冲区生存期。"),
  CppWrapperSpec(
    typeName: "MsQuicExecution",
    wraps: @[qhkExecution],
    summary: "预览执行上下文集合，对应 `Execution*` APIs。",
    lifecycle: "析构调用 `ExecutionDelete`。",
    notes: "仅在启用预览特性后暴露，默认 Nim 不导出。")
]

let CompatibilityNotes*: seq[string] = @[
  "Nim 层沿用 MsQuic 手动资源释放约定，避免依赖 GC 自动回收句柄。",
  "所有回调仍在 MsQuic worker 线程触发，需通过 Nim 的 `asyncdispatch` 或自定义队列转发。",
  "参数结构体保持与 C ABI 二进制对齐；Nim 仅作为轻量包装层，不修改布局。",
  "预览接口默认关闭，通过编译开关控制启用，防止破坏稳定 ABI。"
]

let NimInterfaceDraft*: seq[NimApiFunctionDraft] = @[
  NimApiFunctionDraft(
    name: "openMsQuic",
    params: @["version: uint32 = 2"],
    returnType: "MsQuicApiSurface",
    boundC: "MsQuicOpenVersion",
    stage: "A5",
    semantics: "包装 `MsQuicOpenVersion` 并捕获函数表、关闭回调；失败时返回异常或错误码。"),
  NimApiFunctionDraft(
    name: "closeMsQuic",
    params: @["surface: var MsQuicApiSurface"],
    returnType: "void",
    boundC: "MsQuicClose",
    stage: "A5",
    semantics: "调用 `MsQuicClose` 并清空缓存的函数指针，保证幂等。"),
  NimApiFunctionDraft(
    name: "withRegistration",
    params: @["surface: MsQuicApiSurface", "config: QuicRegistrationConfig",
              "body: proc(reg: RegistrationHandle) {.closure.}"],
    returnType: "void",
    boundC: "RegistrationOpen/Close",
    stage: "A5",
    semantics: "提供作用域管理：打开注册后执行 `body`，最后确保 `RegistrationClose`。"),
  NimApiFunctionDraft(
    name: "openConfiguration",
    params: @["surface: MsQuicApiSurface", "reg: RegistrationHandle",
              "draft: QuicConfigurationDraft"],
    returnType: "ConfigurationHandle",
    boundC: "ConfigurationOpen/LoadCredential",
    stage: "A5",
    semantics: "构造配置并按需加载凭据；失败时抛出 Nim 异常。"),
  NimApiFunctionDraft(
    name: "startListener",
    params: @["surface: MsQuicApiSurface", "reg: RegistrationHandle",
              "config: QuicListenerDraft"],
    returnType: "ListenerHandle",
    boundC: "ListenerOpen/Start",
    stage: "A5",
    semantics: "组合打开与启动监听的常用路径，内部注册回调桥。"),
  NimApiFunctionDraft(
    name: "connect",
    params: @["surface: MsQuicApiSurface", "reg: RegistrationHandle",
              "draft: QuicConnectionDraft", "cfg: ConfigurationHandle"],
    returnType: "ConnectionHandle",
    boundC: "ConnectionOpen/Start",
    stage: "A5",
    semantics: "客户端发起连接并绑定配置，回调由上层注入。"),
  NimApiFunctionDraft(
    name: "openStream",
    params: @["surface: MsQuicApiSurface", "conn: ConnectionHandle",
              "streamDraft: QuicStreamDraft"],
    returnType: "StreamHandle",
    boundC: "StreamOpen/StreamStart",
    stage: "A5",
    semantics: "按照草案配置流属性，并自动调用 `StreamStart`。"),
  NimApiFunctionDraft(
    name: "sendStreamData",
    params: @["surface: MsQuicApiSurface", "stream: StreamHandle",
              "buffers: openArray[seq[uint8]]", "flags: QuicSendFlags"],
    returnType: "void",
    boundC: "StreamSend",
    stage: "A5",
    semantics: "提供 Nim 数组到 C 缓冲转换，保证 FIN/延迟标志正确转换。")
]

let MsQuicApiSurfaceV2* = MsQuicApiSurface(
  version: "2.x",
  functions: MsQuicApiFunctionsV2,
  wrappers: MsQuicCppWrappers,
  compatibilityNotes: CompatibilityNotes,
  nimDraft: NimInterfaceDraft)

proc functionsByCategory*(surface: MsQuicApiSurface;
                          category: ApiFunctionCategory): seq[ApiFunctionSpec] =
  for fn in surface.functions:
    if fn.category == category:
      result.add fn

proc functionByNimSymbol*(surface: MsQuicApiSurface;
                          symbol: string): ApiFunctionSpec =
  ## 快速查找指定 Nim 草案函数对应的 C 符号。
  for fn in surface.functions:
    if fn.nimSymbol == symbol:
      return fn
  raise newException(KeyError, "未找到符号: " & symbol)
