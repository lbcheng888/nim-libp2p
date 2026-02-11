## 对应 `src/inc/quic_datapath.h` 与 `src/platform/platform_internal.h` 的公共定义。

type
  DscpType* = enum
    dscpCs0 = 0
    dscpLe = 1
    dscpCs1 = 8
    dscpCs2 = 16
    dscpCs3 = 24
    dscpCs4 = 32
    dscpCs5 = 40
    dscpEf = 46

  EcnType* = enum
    ecnNonEct = 0
    ecnEct1 = 1
    ecnEct0 = 2
    ecnCe = 3

  DatapathFeature* = enum
    dfRecvSideScaling
    dfRecvCoalescing
    dfSendSegmentation
    dfLocalPortSharing
    dfPortReservations
    dfTcpSupport
    dfRawSocket
    dfTtl
    dfSendDscp
    dfRecvDscp

  DatapathType* = enum
    dtUnknown
    dtNormal
    dtRaw
    dtIoUring
    dtXdp

  SocketType* = enum
    stUdp
    stTcpListener
    stTcp
    stTcpServer

  SocketFlags* = enum
    sfNone
    sfPcp
    sfShare
    sfXdp
    sfQtip
    sfPartitioned

  WorkerPoolStats* = object
    workerCount*: uint16
    usesCoreMasks*: bool
    affinityPolicy*: string

  DatapathCallbacks* = object
    udpRecvContextLength*: uint32
    hasUdpCallbacks*: bool
    hasTcpCallbacks*: bool

const
  MaxMtu* = 1500               ## `CXPLAT_MAX_MTU`
  MinIpv4HeaderSize* = 20      ## `CXPLAT_MIN_IPV4_HEADER_SIZE`
  MinIpv6HeaderSize* = 40      ## `CXPLAT_MIN_IPV6_HEADER_SIZE`
  UdpHeaderSize* = 8           ## `CXPLAT_UDP_HEADER_SIZE`

proc computeUdpPayloadLimit*(mtu: uint16, isIpv6: bool): uint16 =
  ## 对应 `MaxUdpPayloadSizeForFamily` 的 Nim 版。
  if isIpv6:
    return uint16(mtu - MinIpv6HeaderSize - UdpHeaderSize)
  uint16(mtu - MinIpv4HeaderSize - UdpHeaderSize)
