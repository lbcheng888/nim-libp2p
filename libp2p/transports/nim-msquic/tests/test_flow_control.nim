## 验证连接与流级流量控制状态机（C1）。

import unittest
import ../core/flow_control_model
import ../core/stream_model

suite "Flow control basics":
  test "connection allowance reduces when sending":
    var fc = initConnectionFlowControl(initialMaxData = 12000, windowSize = 6000)
    check fc.registerSend(4000)
    var allowance = computeAllowance(fc)
    check allowance.allowedBytes == 8000
    check not allowance.blocked

  test "connection blocks when exceeding max data":
    var fc = initConnectionFlowControl(6000, 6000)
    discard fc.registerSend(6000)
    var allowance = computeAllowance(fc)
    check allowance.allowedBytes == 0
    check allowance.blocked
    fc.updateMaxData(9000)
    allowance = computeAllowance(fc)
    check allowance.allowedBytes == 3000
    check not allowance.blocked

  test "stream send allowance respects limit":
    var stream = QuicStreamModel(id: 0)
    stream.setFlag(qsfSendEnabled)
    initializeStreamFlow(stream, maxData = 4096, recvWindow = 4096)
    var allowance = streamSendAllowance(stream)
    check allowance.allowedBytes == 4096
    check stream.registerStreamSend(2048)
    allowance = streamSendAllowance(stream)
    check allowance.allowedBytes == 2048
    check stream.registerStreamSend(2048)
    allowance = streamSendAllowance(stream)
    check allowance.allowedBytes == 0
    check allowance.blocked
    stream.registerStreamAck(2048)
    allowance = streamSendAllowance(stream)
    check allowance.allowedBytes == 0  ## Flow control counts total bytes sent.
