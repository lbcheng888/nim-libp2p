## Nim 版的数据路径与套接字结构建模，参考 `platform_internal.h`.

import ../core/common
import ./common

type
  DatapathCommon* = object
    udpHandlersPresent*: bool
    tcpHandlersPresent*: bool
    workerStats*: WorkerPoolStats
    features*: set[DatapathFeature]
    hasRawPath*: bool

  SocketCommon* = object
    localAddress*: string
    remoteAddress*: string
    datapathType*: DatapathType
    socketType*: SocketType
    flags*: set[SocketFlags]
    mtu*: uint16
    clientContextName*: string

  SendDataCommon* = object
    datapathType*: DatapathType
    ecn*: EcnType
    dscp*: DscpType
    totalSize*: uint32
    segmentSize*: uint16

  CompletionEventModel* = object
    usesSharedCompletionQueue*: bool
    supportsBatching*: bool
    maxBatchSize*: uint16

  RawSocketSupport* = object
    isAvailable*: bool
    requiresXdpDriver*: bool
    supportsRioFallback*: bool

  IoBackendDescriptor* = object
    name*: string                      ## 对应 `datapath_*.c` 实现名称。
    sourceFiles*: seq[string]          ## 关键 C 文件引用。
    supportedFeatures*: set[DatapathFeature]
    defaultFlags*: set[SocketFlags]
    rawSupport*: RawSocketSupport
    completionModel*: CompletionEventModel

proc describeDatapath*(features: set[DatapathFeature], hasRaw: bool,
                       callbacks: DatapathCallbacks,
                       pool: WorkerPoolStats): DatapathCommon =
  DatapathCommon(
    udpHandlersPresent: callbacks.hasUdpCallbacks,
    tcpHandlersPresent: callbacks.hasTcpCallbacks,
    workerStats: pool,
    features: features,
    hasRawPath: hasRaw)

proc initSocketCommon*(localAddr, remoteAddr: string,
                       datapathType: DatapathType,
                       socketType: SocketType,
                       flags: set[SocketFlags],
                       mtu: uint16,
                       clientContext: string): SocketCommon =
  SocketCommon(
    localAddress: localAddr,
    remoteAddress: remoteAddr,
    datapathType: datapathType,
    socketType: socketType,
    flags: flags,
    mtu: mtu,
    clientContextName: clientContext)
