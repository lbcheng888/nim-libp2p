## Nim MsQuic 平台桥接：负责将平台蓝图中的线程模型、亲和力与绑定参数
## 转换为 MsQuic 可消费的配置。

import std/[options, strutils]

from ./api_impl import QuicApiTable, QUIC_STATUS, QUIC_STATUS_INVALID_PARAMETER,
    QuicRegistrationConfigC, GlobalExecutionConfigHeader, HQUIC
from ./ffi_loader import MsQuicRuntime
from ./param_catalog import QUIC_PARAM_GLOBAL_EXECUTION_CONFIG,
    QUIC_GLOBAL_EXECUTION_CONFIG_MIN_SIZE
from ../platform/common import WorkerPoolStats, DatapathFeature, SocketFlags,
    DatapathType, dtRaw, dtXdp
from ../platform/datapath_model import DatapathCommon, SocketCommon
from ./common import QuicExecutionProfile, QuicRegistrationConfig,
    qepLowLatency, qepMaxThroughput, qepScavenger, qepRealTime

type
  PlatformBridge* = object
    api*: ptr QuicApiTable

  ExecutionFlag* = enum
    efNoIdealProcessor
    efHighPriority
    efAffinitize

  ExecutionPlan* = object
    flags*: set[ExecutionFlag]
    pollingIdleTimeoutUs*: uint32
    processors*: seq[uint16]
    workerCount*: uint16

  BindingPlan* = object
    address*: string
    port*: uint16
    datapathType*: DatapathType
    shareBinding*: bool
    rawMode*: bool
    isIpv6*: bool

const
  FlagNoIdealProcessor = 0x0008'u32
  FlagHighPriority = 0x0010'u32
  FlagAffinitize = 0x0020'u32

proc initPlatformBridge*(api: ptr QuicApiTable): PlatformBridge =
  ## 通过 MsQuic API 表初始化桥接器，校验核心函数存在。
  if api.isNil:
    raise newException(ValueError, "MsQuic API 表指针为空")
  if api.SetParam.isNil:
    raise newException(ValueError, "MsQuic API 缺少 SetParam 实现")
  if api.RegistrationOpen.isNil:
    raise newException(ValueError, "MsQuic API 缺少 RegistrationOpen 实现")
  PlatformBridge(api: api)

proc initPlatformBridge*(runtime: MsQuicRuntime): PlatformBridge =
  ## 允许直接基于已加载的 MsQuic 运行时构造桥接器。
  if runtime.isNil or runtime.apiTable.isNil:
    raise newException(ValueError, "MsQuic 运行时未初始化或缺少 API 表")
  initPlatformBridge(cast[ptr QuicApiTable](runtime.apiTable))

proc toNativeFlags(flags: set[ExecutionFlag]): uint32 {.inline.} =
  var mask = 0'u32
  if efNoIdealProcessor in flags:
    mask = mask or FlagNoIdealProcessor
  if efHighPriority in flags:
    mask = mask or FlagHighPriority
  if efAffinitize in flags:
    mask = mask or FlagAffinitize
  mask

proc toNativeProfile(profile: QuicExecutionProfile): uint32 {.inline.} =
  case profile
  of qepLowLatency:
    0'u32
  of qepMaxThroughput:
    1'u32
  of qepScavenger:
    2'u32
  of qepRealTime:
    3'u32

proc appendProcessor(plan: var ExecutionPlan; core: int) =
  if core < 0 or core > high(uint16).int:
    return
  let value = uint16(core)
  if value notin plan.processors:
    plan.processors.add(value)

proc deriveExecutionPlan*(stats: WorkerPoolStats; features: set[DatapathFeature];
    workerAffinity: seq[seq[int]] = @[];
    pollingIdleTimeoutOverride: Option[uint32] = none(uint32)): ExecutionPlan =
  ## 根据工作线程画像与 datapath 特性推导 MsQuic 全局执行配置。
  var plan = ExecutionPlan(
    flags: {},
    pollingIdleTimeoutUs: 0'u32,
    processors: @[],
    workerCount: uint16(stats.workerCount))

  if pollingIdleTimeoutOverride.isSome:
    plan.pollingIdleTimeoutUs = pollingIdleTimeoutOverride.get()
  else:
    if stats.workerCount >= 4 or DatapathFeature.dfRecvSideScaling in features:
      plan.pollingIdleTimeoutUs = 100'u32
    elif stats.usesCoreMasks:
      plan.pollingIdleTimeoutUs = 50'u32

  if stats.usesCoreMasks or workerAffinity.len > 0:
    plan.flags.incl efAffinitize
    plan.flags.incl efNoIdealProcessor

  let normalizedPolicy = stats.affinityPolicy.toLowerAscii()
  if normalizedPolicy.len > 0:
    if normalizedPolicy.contains("affinit"):
      plan.flags.incl efAffinitize
    if normalizedPolicy.contains("no-ideal") or normalizedPolicy.contains("manual"):
      plan.flags.incl efNoIdealProcessor
    if normalizedPolicy.contains("high") or normalizedPolicy.contains("prio"):
      plan.flags.incl efHighPriority
    if normalizedPolicy.contains("scavenge"):
      plan.flags.excl efHighPriority

  if DatapathFeature.dfSendSegmentation in features or
      DatapathFeature.dfRawSocket in features or
      stats.workerCount >= 8:
    plan.flags.incl efHighPriority
  elif DatapathFeature.dfRecvSideScaling in features or stats.workerCount >= 4:
    plan.flags.incl efHighPriority

  for group in workerAffinity:
    for core in group:
      plan.appendProcessor(core)

  if plan.processors.len == 0 and stats.usesCoreMasks:
    let workerTotal = int(stats.workerCount)
    for core in 0 ..< workerTotal:
      plan.appendProcessor(core)

  if plan.processors.len > 256:
    raise newException(ValueError, "处理器亲和列表最多支持 256 个条目")

  plan

proc buildExecutionBuffer(plan: ExecutionPlan): seq[uint8] =
  let mask = toNativeFlags(plan.flags)
  var header = GlobalExecutionConfigHeader(
    Flags: mask,
    PollingIdleTimeoutUs: plan.pollingIdleTimeoutUs,
    ProcessorCount: uint32(plan.processors.len))
  let total = QUIC_GLOBAL_EXECUTION_CONFIG_MIN_SIZE.int +
    plan.processors.len * sizeof(uint16)
  result = newSeq[uint8](total)
  copyMem(addr result[0], addr header, sizeof(header))
  if plan.processors.len > 0:
    copyMem(addr result[QUIC_GLOBAL_EXECUTION_CONFIG_MIN_SIZE.int],
      unsafeAddr plan.processors[0],
      plan.processors.len * sizeof(uint16))

proc applyExecutionPlan*(bridge: PlatformBridge; plan: ExecutionPlan): QUIC_STATUS =
  ## 将执行计划写入 MsQuic 全局配置。
  if bridge.api.isNil or bridge.api.SetParam.isNil:
    return QUIC_STATUS_INVALID_PARAMETER
  let buffer = buildExecutionBuffer(plan)
  bridge.api.SetParam(
    nil,
    QUIC_PARAM_GLOBAL_EXECUTION_CONFIG,
    uint32(buffer.len),
    if buffer.len == 0: nil else: addr buffer[0])

proc configureWorkerLayout*(bridge: PlatformBridge; stats: WorkerPoolStats;
    features: set[DatapathFeature]; workerAffinity: seq[seq[int]] = @[];
    pollingIdleTimeoutOverride: Option[uint32] = none(uint32);
    plan: var ExecutionPlan): QUIC_STATUS =
  ## 组合推导与下发逻辑，返回最终计划。
  plan = deriveExecutionPlan(stats, features, workerAffinity,
    pollingIdleTimeoutOverride)
  bridge.applyExecutionPlan(plan)

proc recommendedExecutionProfile*(stats: WorkerPoolStats;
    features: set[DatapathFeature];
    config: QuicRegistrationConfig): QuicExecutionProfile =
  ## 基于平台画像选择合适的 Registration ExecutionProfile。
  if config.profile != qepLowLatency:
    return config.profile
  let normalizedPolicy = stats.affinityPolicy.toLowerAscii()
  if normalizedPolicy.contains("scavenge"):
    return qepScavenger
  if normalizedPolicy.contains("real-") or normalizedPolicy.contains("rt"):
    return qepRealTime
  if DatapathFeature.dfRecvSideScaling in features or
      DatapathFeature.dfSendSegmentation in features or
      stats.workerCount >= 4:
    return qepMaxThroughput
  if config.enableExecutionPolling:
    return qepRealTime
  qepLowLatency

proc openRegistrationWithPlatform*(bridge: PlatformBridge;
    config: QuicRegistrationConfig; stats: WorkerPoolStats;
    datapath: DatapathCommon; registration: ptr HQUIC;
    appliedProfile: var QuicExecutionProfile): QUIC_STATUS =
  ## 使用平台画像辅助选择执行策略并打开 Registration。
  if registration.isNil:
    return QUIC_STATUS_INVALID_PARAMETER
  let derived = recommendedExecutionProfile(stats, datapath.features, config)
  appliedProfile = derived
  var appNameStorage = config.appName
  if appNameStorage.len == 0:
    appNameStorage = datapath.workerStats.affinityPolicy
  if appNameStorage.len == 0:
    appNameStorage = "nim-msquic"
  var cConfig = QuicRegistrationConfigC(
    AppName: (if appNameStorage.len == 0: nil else: appNameStorage.cstring),
    ExecutionProfile: toNativeProfile(derived))
  bridge.api.RegistrationOpen(addr cConfig, registration)

proc deriveBindingPlan*(socket: SocketCommon; bindPort: uint16): BindingPlan =
  ## 从 SocketCommon 推演绑定参数，供后续 Listener/Connection Start 使用。
  var address = socket.localAddress
  let isIpv6 = address.contains(':')
  if address.len == 0:
    address = if isIpv6: "::" else: "0.0.0.0"
  BindingPlan(
    address: address,
    port: bindPort,
    datapathType: socket.datapathType,
    shareBinding: SocketFlags.sfShare in socket.flags,
    rawMode: socket.datapathType in {dtRaw, dtXdp} or SocketFlags.sfXdp in socket.flags,
    isIpv6: isIpv6)

proc bindingSummary*(plan: BindingPlan): string =
  ## 生成可读化的绑定摘要，便于日志或诊断。
  let mode = if plan.rawMode: "raw" else: "udp"
  let share = if plan.shareBinding: "shared" else: "exclusive"
  let family = if plan.isIpv6: "ipv6" else: "ipv4"
  plan.address & ":" & $plan.port & " [" & family & "," & mode & "," & share & "]"
