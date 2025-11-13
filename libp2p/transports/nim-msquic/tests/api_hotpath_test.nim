import std/unittest
import std/sequtils

import "../api/api_impl"
import "../api/event_model"
import "../api/param_catalog"
import "../api/diagnostics_model"

suite "API 热路径优化":
  setup:
    clearDiagnosticsHooks()

  teardown:
    clearDiagnosticsHooks()

  test "Datagram 快速路径与 C Shim 保持一致":
    var diagNotes: seq[string] = @[]
    registerDiagnosticsHook(proc (event: DiagnosticsEvent) {.gcsafe.} =
      if event.kind == diagConnectionParamSet and
          (event.paramId == QUIC_PARAM_CONN_DATAGRAM_RECEIVE_ENABLED or
          event.paramId == QUIC_PARAM_CONN_DATAGRAM_SEND_ENABLED):
        diagNotes.add(event.note)
    )

    var apiPtr: pointer
    check MsQuicOpenVersion(2'u32, addr apiPtr) == QUIC_STATUS_SUCCESS
    let table = cast[ptr QuicApiTable](apiPtr)

    var registration: HQUIC
    var regConfig = QuicRegistrationConfigC(
      AppName: cstring("nim-msquic-hotpath"),
      ExecutionProfile: QUIC_EXECUTION_PROFILE(0)
    )
    check table.RegistrationOpen(addr regConfig, addr registration) == QUIC_STATUS_SUCCESS

    let alpnString = "hq-hotpath"
    var alpnBuffer = QuicBuffer(
      Length: uint32(alpnString.len),
      Buffer: cast[ptr uint8](alpnString.cstring)
    )
    var configuration: HQUIC
    check table.ConfigurationOpen(registration, addr alpnBuffer, 1'u32,
      nil, 0'u32, nil, addr configuration) == QUIC_STATUS_SUCCESS

    var connection: HQUIC
    proc noopConnectionCallback(connectionHandle: HQUIC; context: pointer;
        event: pointer): QUIC_STATUS {.cdecl.} =
      discard connectionHandle
      discard context
      discard event
      QUIC_STATUS_SUCCESS

    check table.ConnectionOpen(registration, noopConnectionCallback, nil,
      addr connection) == QUIC_STATUS_SUCCESS

    var connectionEvents: seq[(uint32, bool, string)] = @[]
    registerConnectionEventHandler(connection, proc (event: ConnectionEvent) {.gcsafe.} =
      if event.kind == ceDatagramStateChanged:
        connectionEvents.add((event.paramId, event.boolValue, event.note))
    )

    let contextValue = cast[pointer](0xDEADBEEF'i64)
    MsQuicSetContextShim(connection, contextValue)
    check MsQuicGetContextShim(connection) == contextValue

    var receiveEnable = BOOLEAN(1)
    check table.SetParam(connection, QUIC_PARAM_CONN_DATAGRAM_RECEIVE_ENABLED,
      1'u32, addr receiveEnable) == QUIC_STATUS_SUCCESS

    var receiveState = BOOLEAN(0)
    check MsQuicGetDatagramReceiveShim(connection, addr receiveState) == QUIC_STATUS_SUCCESS
    check receiveState == BOOLEAN(1)

    check MsQuicEnableDatagramReceiveShim(connection, BOOLEAN(0)) == QUIC_STATUS_SUCCESS
    check MsQuicGetDatagramReceiveShim(connection, addr receiveState) == QUIC_STATUS_SUCCESS
    check receiveState == BOOLEAN(0)

    check MsQuicEnableDatagramSendShim(connection, BOOLEAN(1)) == QUIC_STATUS_SUCCESS
    var sendState = BOOLEAN(0)
    check MsQuicGetDatagramSendShim(connection, addr sendState) == QUIC_STATUS_SUCCESS
    check sendState == BOOLEAN(1)

    check MsQuicEnableDatagramSendShim(connection, BOOLEAN(0)) == QUIC_STATUS_SUCCESS
    check MsQuicGetDatagramSendShim(connection, addr sendState) == QUIC_STATUS_SUCCESS
    check sendState == BOOLEAN(0)

    table.ConnectionClose(connection)
    table.ConfigurationClose(configuration)
    table.RegistrationClose(registration)
    MsQuicClose(apiPtr)

    check diagNotes == @["receive=true", "receive=false", "send=true", "send=false"]
    check connectionEvents == @[
      (QUIC_PARAM_CONN_DATAGRAM_RECEIVE_ENABLED, true, "receive=true"),
      (QUIC_PARAM_CONN_DATAGRAM_RECEIVE_ENABLED, false, "receive=false"),
      (QUIC_PARAM_CONN_DATAGRAM_SEND_ENABLED, true, "send=true"),
      (QUIC_PARAM_CONN_DATAGRAM_SEND_ENABLED, false, "send=false")
    ]
