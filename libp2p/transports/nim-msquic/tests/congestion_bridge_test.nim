import unittest

import ../api/api_impl
import ../api/congestion_bridge
import ../congestion/common

proc withConnection(body: proc (api: ptr QuicApiTable; bridge: CongestionBridge;
    registration: HQUIC; connection: HQUIC)) =
  var apiPtr: pointer
  check MsQuicOpenVersion(2, addr apiPtr) == QUIC_STATUS_SUCCESS
  let api = cast[ptr QuicApiTable](apiPtr)
  let bridge = initCongestionBridge(api)
  var registration: HQUIC = nil
  check api.RegistrationOpen(nil, addr registration) == QUIC_STATUS_SUCCESS
  var connection: HQUIC = nil
  check api.ConnectionOpen(registration, nil, nil, addr connection) == QUIC_STATUS_SUCCESS
  try:
    body(api, bridge, registration, connection)
  finally:
    if not connection.isNil:
      api.ConnectionClose(connection)
    if not registration.isNil:
      api.RegistrationClose(registration)
    MsQuicClose(apiPtr)

suite "MsQuic 拥塞控制桥 (F2)":

  test "默认读取 CUBIC 策略":
    withConnection(proc (api: ptr QuicApiTable; bridge: CongestionBridge;
        registration: HQUIC; connection: HQUIC) =
      discard api
      discard registration
      var algorithm: CongestionAlgorithm
      check bridge.getConnectionAlgorithm(connection, algorithm) == QUIC_STATUS_SUCCESS
      check algorithm == caCubic
    )

  test "切换到 BBR 并支持字符串选择":
    withConnection(proc (api: ptr QuicApiTable; bridge: CongestionBridge;
        registration: HQUIC; connection: HQUIC) =
      discard api
      discard registration
      check bridge.setConnectionAlgorithm(connection, caBbr) == QUIC_STATUS_SUCCESS
      var algorithm: CongestionAlgorithm
      check bridge.getConnectionAlgorithm(connection, algorithm) == QUIC_STATUS_SUCCESS
      check algorithm == caBbr

      check bridge.setConnectionAlgorithmByName(connection, "cubic") == QUIC_STATUS_SUCCESS
      check bridge.getConnectionAlgorithm(connection, algorithm) == QUIC_STATUS_SUCCESS
      check algorithm == caCubic

      check bridge.setConnectionAlgorithmByName(connection, "invalid") == QUIC_STATUS_INVALID_PARAMETER
    )
