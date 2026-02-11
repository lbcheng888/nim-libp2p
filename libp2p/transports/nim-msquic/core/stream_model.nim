## 映射 `src/core/stream*.c/h` 的流状态结构。

import ./common

type
  StreamType* = enum
    stClientBi
    stServerBi
    stClientUni
    stServerUni

  QuicStreamFlag* = enum
    qsfAllocated
    qsfInitialized
    qsfStarted
    qsfStartedIndicated
    qsfUnidirectional
    qsfSendOpen
    qsfSendOpenAcked
    qsfLocalCloseFin
    qsfLocalCloseReset
    qsfRemoteCloseFin
    qsfRemoteCloseReset
    qsfReceivedStopSending
    qsfSendEnabled
    qsfReceiveEnabled
    qsfReceiveFlushQueued
    qsfReceiveDataPending
    qsfInRecovery
    qsfHandleSendShutdown
    qsfHandleShutdown
    qsfHandleClosed
    qsfShutdownComplete

  QuicStreamSendState* = enum
    qsssDisabled
    qsssStarted
    qsssReset
    qsssResetAcked
    qsssFin
    qsssFinAcked
    qsssReliableReset
    qsssReliableResetAcked

  QuicStreamRecvState* = enum
    qsrsDisabled
    qsrsStarted
    qsrsPaused
    qsrsStopped
    qsrsReset
    qsrsFin
    qsrsReliableReset

  StreamFlowControlState* = object
    maxStreamData*: uint64
    sendWindow*: uint64
    recvWindow*: uint64
    highestReceivedOffset*: uint64
    bytesSent*: uint64
    bytesAcked*: uint64
    bytesReceived*: uint64
    bytesConsumed*: uint64

  StreamReliabilityState* = object
    reliableSendOffset*: uint64
    reliableRecvLimit*: uint64
    pendingReliableReset*: bool

  QuicStreamModel* = object
    id*: uint64
    streamType*: StreamType
    priority*: uint16
    flags*: set[QuicStreamFlag]
    sendState*: QuicStreamSendState
    recvState*: QuicStreamRecvState
    flow*: StreamFlowControlState
    reliability*: StreamReliabilityState

  StreamSchedulingSnapshot* = object
    totalStreams*: array[StreamType, uint32]
    activeSendOrder*: seq[uint64]
    useRoundRobin*: bool

proc streamTypeFromId*(streamId: uint64): StreamType =
  ## 根据 QUIC 规范的低两位推导流类型。
  let owner = streamId and 0b1
  let direction = (streamId shr 1) and 0b1
  case (owner shl 1) or direction
  of 0: stClientBi
  of 1: stServerBi
  of 2: stClientUni
  else: stServerUni

proc setFlag*(stream: var QuicStreamModel, flag: QuicStreamFlag) =
  stream.flags.incl(flag)

proc clearFlag*(stream: var QuicStreamModel, flag: QuicStreamFlag) =
  stream.flags.excl(flag)

proc hasFlag*(stream: QuicStreamModel, flag: QuicStreamFlag): bool =
  flag in stream.flags
