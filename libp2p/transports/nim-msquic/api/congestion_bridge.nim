## MsQuic 拥塞控制参数桥接模块，连接 Nim 拥塞蓝图与实际 MsQuic API。

import std/[strutils]

from ./param_catalog import QUIC_PARAM_CONN_CONGESTION_CONTROL_ALGORITHM
from ./ffi_loader import MsQuicRuntime
from ./api_impl import QuicApiTable, QUIC_STATUS, QUIC_STATUS_SUCCESS,
    QUIC_STATUS_INVALID_PARAMETER, QUIC_STATUS_INVALID_STATE, HQUIC
from ../congestion/common import CongestionAlgorithm, caCubic, caBbr

type
  CongestionBridge* = object
    api*: ptr QuicApiTable

proc initCongestionBridge*(api: ptr QuicApiTable): CongestionBridge =
  ## 基于已经获取的 `QUIC_API_TABLE` 指针初始化桥接器。
  if api.isNil:
    raise newException(ValueError, "MsQuic API 表指针为空")
  if api.SetParam.isNil or api.GetParam.isNil:
    raise newException(ValueError, "MsQuic API 缺少 SetParam/GetParam 实现")
  CongestionBridge(api: api)

proc initCongestionBridge*(runtime: MsQuicRuntime): CongestionBridge =
  ## 从动态加载的 MsQuic 运行时构建桥接器。
  if runtime.isNil or runtime.apiTable.isNil:
    raise newException(ValueError, "MsQuic 运行时未初始化或缺少 API 表")
  initCongestionBridge(cast[ptr QuicApiTable](runtime.apiTable))

proc algorithmToRaw(algorithm: CongestionAlgorithm): uint16 {.inline.} =
  case algorithm
  of caCubic:
    0'u16
  of caBbr:
    1'u16

proc rawToAlgorithm(raw: uint16; algorithm: var CongestionAlgorithm): bool {.inline.} =
  case raw
  of 0'u16:
    algorithm = caCubic
    true
  of 1'u16:
    algorithm = caBbr
    true
  else:
    false

proc algorithmName*(algorithm: CongestionAlgorithm): string {.inline.} =
  ## 将 Nim 枚举转换为 MsQuic 常见策略名称。
  case algorithm
  of caCubic:
    "CUBIC"
  of caBbr:
    "BBR"

proc parseAlgorithmName*(name: string; algorithm: var CongestionAlgorithm): bool {.inline.} =
  ## 从字符串解析拥塞控制策略，支持 CUBIC/BBR（大小写不敏感）。
  let normalized = name.strip().toUpperAscii()
  case normalized
  of "CUBIC":
    algorithm = caCubic
    true
  of "BBR":
    algorithm = caBbr
    true
  else:
    false

proc setConnectionAlgorithm*(bridge: CongestionBridge; connection: HQUIC;
    algorithm: CongestionAlgorithm): QUIC_STATUS =
  ## 调用 MsQuic SetParam 以切换连接的拥塞控制算法。
  if connection.isNil:
    return QUIC_STATUS_INVALID_PARAMETER
  var raw = algorithmToRaw(algorithm)
  bridge.api.SetParam(
    connection,
    QUIC_PARAM_CONN_CONGESTION_CONTROL_ALGORITHM,
    sizeof(raw).uint32,
    addr raw)

proc setConnectionAlgorithmByName*(bridge: CongestionBridge; connection: HQUIC;
    name: string): QUIC_STATUS =
  ## 通过字符串名称选择拥塞控制算法，便于脚本化配置。
  var algorithm: CongestionAlgorithm
  if not parseAlgorithmName(name, algorithm):
    return QUIC_STATUS_INVALID_PARAMETER
  bridge.setConnectionAlgorithm(connection, algorithm)

proc getConnectionAlgorithm*(bridge: CongestionBridge; connection: HQUIC;
    algorithm: var CongestionAlgorithm): QUIC_STATUS =
  ## 读取 MsQuic 当前的拥塞控制算法。
  if connection.isNil:
    return QUIC_STATUS_INVALID_PARAMETER
  var length = sizeof(uint16).uint32
  var raw: uint16 = 0
  let status = bridge.api.GetParam(
    connection,
    QUIC_PARAM_CONN_CONGESTION_CONTROL_ALGORITHM,
    addr length,
    addr raw)
  if status != QUIC_STATUS_SUCCESS:
    return status
  if length != sizeof(uint16).uint32:
    return QUIC_STATUS_INVALID_STATE
  if not rawToAlgorithm(raw, algorithm):
    return QUIC_STATUS_INVALID_PARAMETER
  QUIC_STATUS_SUCCESS
