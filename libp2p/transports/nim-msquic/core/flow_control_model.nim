## 流量控制状态蓝图，对齐 `src/core/stream.c`, `src/core/connection.c` 中的发送/接收窗口管理。

import ./common
import ./stream_model

type
  FlowControlLevel* = enum
    fclConnection
    fclStream

  FlowAllowance* = object
    allowedBytes*: uint64
    blocked*: bool

  ConnectionFlowControlState* = object
    maxData*: uint64
    bytesSent*: uint64
    bytesConsumed*: uint64
    windowSize*: uint64
    blocked*: bool

  StreamFlowControlUpdate* = object
    streamId*: uint64
    newMaxData*: uint64
    isFinalUpdate*: bool

proc initConnectionFlowControl*(
    initialMaxData: uint64,
    windowSize: uint64
  ): ConnectionFlowControlState =
  ConnectionFlowControlState(
    maxData: initialMaxData,
    bytesSent: 0,
    bytesConsumed: 0,
    windowSize: windowSize,
    blocked: false)

proc registerSend*(state: var ConnectionFlowControlState, bytes: uint64): bool =
  ## 尝试发送数据，返回是否成功。
  if state.bytesSent + bytes > state.maxData:
    state.blocked = true
    return false
  state.bytesSent += bytes
  true

proc registerConsumption*(state: var ConnectionFlowControlState, bytes: uint64) =
  ## 接收路径消费数据后更新窗口。
  if state.bytesConsumed + bytes <= state.bytesSent:
    state.bytesConsumed += bytes
  if state.bytesSent - state.bytesConsumed <= state.windowSize div 2:
    state.blocked = false

proc updateMaxData*(state: var ConnectionFlowControlState, newLimit: uint64) =
  if newLimit > state.maxData:
    state.maxData = newLimit
    state.blocked = false

proc computeAllowance*(state: ConnectionFlowControlState): FlowAllowance =
  if state.bytesSent >= state.maxData:
    FlowAllowance(allowedBytes: 0, blocked: true)
  else:
    FlowAllowance(
      allowedBytes: state.maxData - state.bytesSent,
      blocked: state.blocked)

proc streamSendAllowance*(stream: QuicStreamModel): FlowAllowance =
  let available =
    if stream.flow.maxStreamData > stream.flow.bytesSent:
      stream.flow.maxStreamData - stream.flow.bytesSent
    else:
      0
  FlowAllowance(
    allowedBytes: available,
    blocked: available == 0 or not stream.hasFlag(qsfSendEnabled))

proc initializeStreamFlow*(
    stream: var QuicStreamModel,
    maxData: uint64,
    recvWindow: uint64
  ) =
  stream.flow.maxStreamData = maxData
  stream.flow.sendWindow = maxData
  stream.flow.recvWindow = recvWindow
  stream.flow.bytesSent = 0
  stream.flow.bytesAcked = 0
  stream.flow.bytesReceived = 0
  stream.flow.bytesConsumed = 0
  stream.flow.highestReceivedOffset = 0

proc registerStreamSend*(stream: var QuicStreamModel, bytes: uint64): bool =
  if stream.flow.bytesSent + bytes > stream.flow.maxStreamData:
    stream.clearFlag(qsfSendEnabled)
    return false
  stream.flow.bytesSent += bytes
  true

proc registerStreamAck*(stream: var QuicStreamModel, bytes: uint64) =
  stream.flow.bytesAcked += bytes
  if not stream.hasFlag(qsfSendEnabled):
    stream.setFlag(qsfSendEnabled)

proc onStreamDataReceived*(stream: var QuicStreamModel, offset: uint64, bytes: uint64) =
  let newEnd = offset + bytes
  if newEnd > stream.flow.highestReceivedOffset:
    stream.flow.highestReceivedOffset = newEnd
  stream.flow.bytesReceived += bytes
  if stream.flow.recvWindow > bytes:
    stream.flow.recvWindow -= bytes
  else:
    stream.flow.recvWindow = 0

proc onStreamDataConsumed*(stream: var QuicStreamModel, bytes: uint64) =
  stream.flow.bytesConsumed += bytes
  if stream.flow.recvWindow + bytes <= stream.flow.maxStreamData:
    stream.flow.recvWindow += bytes
  if stream.flow.bytesReceived > stream.flow.bytesConsumed:
    stream.setFlag(qsfReceiveDataPending)
  else:
    stream.clearFlag(qsfReceiveDataPending)

proc queueStreamUpdate*(
    stream: QuicStreamModel,
    increment: uint64,
    finalUpdate: bool = false
  ): StreamFlowControlUpdate =
  StreamFlowControlUpdate(
    streamId: stream.id,
    newMaxData: stream.flow.maxStreamData + increment,
    isFinalUpdate: finalUpdate)
