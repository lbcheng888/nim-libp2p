## 验证帧类型与载荷分类映射（C1）。

import unittest
import ../core/frame_model

suite "Frame payload classification":
  test "stream frame recognized":
    check framePayloadKind(qftStream) == fpStream
    check framePayloadKind(qftStream7) == fpStream
  test "ack frame recognized":
    check framePayloadKind(qftAck) == fpAck
    check framePayloadKind(qftAck1) == fpAck
  test "max streams variants":
    check framePayloadKind(qftMaxStreams) == fpMaxStreams
    check framePayloadKind(qftMaxStreams1) == fpMaxStreams
  test "connection close variants":
    check framePayloadKind(qftConnectionClose) == fpConnectionClose
    check framePayloadKind(qftConnectionClose1) == fpConnectionClose
  test "unsupported frame fallback":
    check framePayloadKind(qftPing) == fpNone
