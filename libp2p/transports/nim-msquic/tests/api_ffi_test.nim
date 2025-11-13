import std/unittest

import "../api/api_impl"

suite "MsQuic Nim API 最小表面":
  test "MsQuicOpenVersion returns table and drives client start":
    var apiPtr: pointer
    let status = MsQuicOpenVersion(2'u32, addr apiPtr)
    check status == QUIC_STATUS_SUCCESS

    let table = cast[ptr QuicApiTable](apiPtr)
    check table != nil
    check not table.SetContext.isNil
    check not table.ConnectionOpen.isNil

    var registration: HQUIC
    var regConfig = QuicRegistrationConfigC(
      AppName: cstring("nim-msquic"),
      ExecutionProfile: QUIC_EXECUTION_PROFILE(0)
    )
    check table.RegistrationOpen(addr regConfig, addr registration) == QUIC_STATUS_SUCCESS
    check registration != nil

    let alpnString = "hq-interop"
    var alpnBuffer = QuicBuffer(
      Length: uint32(alpnString.len),
      Buffer: cast[ptr uint8](alpnString.cstring)
    )
    var configuration: HQUIC
    check table.ConfigurationOpen(registration, addr alpnBuffer, 1'u32, nil, 0'u32,
      nil, addr configuration) == QUIC_STATUS_SUCCESS
    check configuration != nil

    var credential = 1'u32
    check table.ConfigurationLoadCredential(configuration,
      cast[pointer](addr credential)) == QUIC_STATUS_SUCCESS

    var connection: HQUIC
    var callbackInvoked = false
    var negotiatedLen: uint8 = 0
    var negotiatedAlpn = ""
    var contextValue: uint64 = 0xDEADBEEFu64

    proc connectionCallback(connectionHandle: HQUIC; context: pointer;
        event: pointer): QUIC_STATUS {.cdecl.} =
      doAssert context == cast[pointer](addr contextValue)
      let ev = cast[ptr QuicConnectionEvent](event)
      doAssert ev.Type == QUIC_CONNECTION_EVENT_CONNECTED
      let connected = cast[ptr QuicConnectionEventConnectedPayload](addr ev.Data[0])
      negotiatedLen = connected.NegotiatedAlpnLength
      if connected.NegotiatedAlpn.isNil or negotiatedLen == 0:
        negotiatedAlpn = ""
      else:
        negotiatedAlpn = newString(int(negotiatedLen))
        system.copyMem(addr negotiatedAlpn[0], connected.NegotiatedAlpn, negotiatedAlpn.len)
      callbackInvoked = true
      QUIC_STATUS_SUCCESS

    check table.ConnectionOpen(registration, connectionCallback,
      cast[pointer](addr contextValue), addr connection) == QUIC_STATUS_SUCCESS
    check connection != nil

    check table.ConnectionSetConfiguration(connection, configuration) == QUIC_STATUS_SUCCESS

    table.SetContext(connection, cast[pointer](addr contextValue))
    check table.GetContext(connection) == cast[pointer](addr contextValue)

    check table.ConnectionStart(connection, configuration, QUIC_ADDRESS_FAMILY(2),
      cstring("example.com"), uint16(443)) == QUIC_STATUS_SUCCESS

    check callbackInvoked
    check int(negotiatedLen) == alpnString.len
    check negotiatedAlpn == alpnString

    table.ConnectionShutdown(connection, QUIC_CONNECTION_SHUTDOWN_FLAGS(0), 0)
    table.ConnectionClose(connection)
    table.ConfigurationClose(configuration)
    table.RegistrationClose(registration)
    MsQuicClose(apiPtr)
