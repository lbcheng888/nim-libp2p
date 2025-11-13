## 平台特性矩阵，梳理不同操作系统的数据路径实现与 FFI 依赖。

import ./common
import ./datapath_model

type
  PlatformOs* = enum
    posWindowsUser
    posWindowsKernel
    posLinux
    posLinuxIoUring
    posLinuxXdp
    posFreeBsd
    posMacos
    posGenericPosix

  FfiRequirement* = enum
    ffiWinsock
    ffiIoUring
    ffiEpoll
    ffiKqueue
    ffiBsdSockets
    ffiXdpLinux
    ffiDpdk
    ffiSchannel
    ffiOpenSsl
    ffiQuicTls
    ffiCng

  PlatformCapability* = object
    os*: PlatformOs
    datapath*: IoBackendDescriptor
    requiredFfi*: set[FfiRequirement]
    notes*: string

const
  WindowsUserBackend = IoBackendDescriptor(
    name: "winuser",
    sourceFiles: @[
      "src/platform/datapath_winuser.c",
      "src/platform/platform_winuser.c"],
    supportedFeatures: {dfRecvSideScaling, dfRecvCoalescing, dfSendSegmentation,
                        dfLocalPortSharing, dfTcpSupport, dfTtl, dfSendDscp, dfRecvDscp},
    defaultFlags: {sfShare},
    rawSupport: RawSocketSupport(isAvailable: true, requiresXdpDriver: false, supportsRioFallback: true),
    completionModel: CompletionEventModel(
      usesSharedCompletionQueue: true,
      supportsBatching: true,
      maxBatchSize: 64))

  WindowsKernelBackend = IoBackendDescriptor(
    name: "winkernel",
    sourceFiles: @[
      "src/platform/datapath_winkernel.c",
      "src/platform/platform_winkernel.c"],
    supportedFeatures: {dfRecvSideScaling, dfRecvCoalescing, dfSendSegmentation,
                        dfLocalPortSharing, dfPortReservations, dfTcpSupport, dfTtl},
    defaultFlags: {sfShare},
    rawSupport: RawSocketSupport(isAvailable: false, requiresXdpDriver: false, supportsRioFallback: false),
    completionModel: CompletionEventModel(
      usesSharedCompletionQueue: false,
      supportsBatching: false,
      maxBatchSize: 1))

  LinuxEpollBackend = IoBackendDescriptor(
    name: "linux-epoll",
    sourceFiles: @[
      "src/platform/datapath_linux.c",
      "src/platform/datapath_epoll.c",
      "src/platform/platform_posix.c"],
    supportedFeatures: {dfRecvSideScaling, dfRecvCoalescing, dfSendSegmentation,
                        dfRawSocket},
    defaultFlags: {},
    rawSupport: RawSocketSupport(isAvailable: true, requiresXdpDriver: false, supportsRioFallback: false),
    completionModel: CompletionEventModel(
      usesSharedCompletionQueue: true,
      supportsBatching: true,
      maxBatchSize: 32))

  LinuxIoUringBackend = IoBackendDescriptor(
    name: "linux-iouring",
    sourceFiles: @[
      "src/platform/datapath_linux.c",
      "src/platform/datapath_iouring.c"],
    supportedFeatures: {dfRecvSideScaling, dfRecvCoalescing, dfSendSegmentation,
                        dfRawSocket},
    defaultFlags: {},
    rawSupport: RawSocketSupport(isAvailable: true, requiresXdpDriver: false, supportsRioFallback: false),
    completionModel: CompletionEventModel(
      usesSharedCompletionQueue: true,
      supportsBatching: true,
      maxBatchSize: 128))

  LinuxXdpBackend = IoBackendDescriptor(
    name: "linux-xdp",
    sourceFiles: @[
      "src/platform/datapath_raw_xdp_linux.c",
      "src/platform/datapath_raw_xdp_linux_kern.c"],
    supportedFeatures: {dfRecvSideScaling, dfRawSocket},
    defaultFlags: {sfXdp},
    rawSupport: RawSocketSupport(isAvailable: true, requiresXdpDriver: true, supportsRioFallback: false),
    completionModel: CompletionEventModel(
      usesSharedCompletionQueue: true,
      supportsBatching: true,
      maxBatchSize: 256))

  FreeBsdBackend = IoBackendDescriptor(
    name: "bsd-kqueue",
    sourceFiles: @[
      "src/platform/datapath_kqueue.c",
      "src/platform/platform_posix.c"],
    supportedFeatures: {dfRecvSideScaling},
    defaultFlags: {},
    rawSupport: RawSocketSupport(isAvailable: false, requiresXdpDriver: false, supportsRioFallback: false),
    completionModel: CompletionEventModel(
      usesSharedCompletionQueue: true,
      supportsBatching: false,
      maxBatchSize: 16))

  MacosBackend = IoBackendDescriptor(
    name: "macos-kqueue",
    sourceFiles: @[
      "src/platform/datapath_kqueue.c",
      "src/platform/datapath_darwin.c"],
    supportedFeatures: {dfRecvSideScaling},
    defaultFlags: {},
    rawSupport: RawSocketSupport(isAvailable: false, requiresXdpDriver: false, supportsRioFallback: false),
    completionModel: CompletionEventModel(
      usesSharedCompletionQueue: true,
      supportsBatching: false,
      maxBatchSize: 8))

  GenericPosixBackend = IoBackendDescriptor(
    name: "generic-posix",
    sourceFiles: @[
      "src/platform/datapath_unix.c",
      "src/platform/platform_posix.c"],
    supportedFeatures: {},
    defaultFlags: {},
    rawSupport: RawSocketSupport(isAvailable: false, requiresXdpDriver: false, supportsRioFallback: false),
    completionModel: CompletionEventModel(
      usesSharedCompletionQueue: true,
      supportsBatching: false,
      maxBatchSize: 8))

const
  PlatformMatrix*: array[PlatformOs, PlatformCapability] = [
    PlatformCapability(
      os: posWindowsUser,
      datapath: WindowsUserBackend,
      requiredFfi: {ffiWinsock, ffiSchannel, ffiCng},
      notes: "用户态 Winsock datapath，支持 RSS/Coalescing/RIO。"),
    PlatformCapability(
      os: posWindowsKernel,
      datapath: WindowsKernelBackend,
      requiredFfi: {ffiWinsock, ffiSchannel},
      notes: "Windows 内核模式 WSK datapath，与 MsQuic 内核驱动配合使用。"),
    PlatformCapability(
      os: posLinux,
      datapath: LinuxEpollBackend,
      requiredFfi: {ffiEpoll, ffiBsdSockets, ffiOpenSsl, ffiQuicTls},
      notes: "基于 epoll 的通用 Linux datapath，支持 Raw socket 和基本特性。"),
    PlatformCapability(
      os: posLinuxIoUring,
      datapath: LinuxIoUringBackend,
      requiredFfi: {ffiIoUring, ffiBsdSockets, ffiOpenSsl, ffiQuicTls},
      notes: "利用 io_uring 获得更低延迟。"),
    PlatformCapability(
      os: posLinuxXdp,
      datapath: LinuxXdpBackend,
      requiredFfi: {ffiXdpLinux, ffiBsdSockets},
      notes: "基于 XDP/BPF 的原始路径，实现内核旁路。"),
    PlatformCapability(
      os: posFreeBsd,
      datapath: FreeBsdBackend,
      requiredFfi: {ffiKqueue, ffiBsdSockets, ffiOpenSsl},
      notes: "kqueue 驱动的 FreeBSD 支持，特性集相对精简。"),
    PlatformCapability(
      os: posMacos,
      datapath: MacosBackend,
      requiredFfi: {ffiKqueue, ffiBsdSockets, ffiOpenSsl},
      notes: "macOS 上使用 kqueue，与 Darwin 证书 API 相集成。"),
    PlatformCapability(
      os: posGenericPosix,
      datapath: GenericPosixBackend,
      requiredFfi: {ffiBsdSockets, ffiOpenSsl},
      notes: "通用 POSIX fallback，实现最小功能集。")]
