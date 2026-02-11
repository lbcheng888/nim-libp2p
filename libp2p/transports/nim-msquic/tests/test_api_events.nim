import unittest
import std/sequtils
import ../api/api_impl
import ../api/event_model

suite "MsQuic connection events":
  test "connected event dispatched to Nim handler":
    var apiPtr: pointer
    check MsQuicOpenVersion(2, addr apiPtr) == QUIC_STATUS_SUCCESS
    let api = cast[ptr QuicApiTable](apiPtr)

    var registration: HQUIC
    check api.RegistrationOpen(nil, addr registration) == QUIC_STATUS_SUCCESS

    var alpn = "hq-interop"
    var alpnBuffer = QuicBuffer(
      Length: uint32(alpn.len),
      Buffer: cast[ptr uint8](alpn.cstring))

    var configuration: HQUIC
    check api.ConfigurationOpen(
      registration,
      addr alpnBuffer,
      1,
      nil,
      0,
      nil,
      addr configuration) == QUIC_STATUS_SUCCESS

    var credentialDummy: uint8
    check api.ConfigurationLoadCredential(configuration, addr credentialDummy) == QUIC_STATUS_SUCCESS

    var connection: HQUIC
    check api.ConnectionOpen(registration, nil, nil, addr connection) == QUIC_STATUS_SUCCESS
    check api.ConnectionSetConfiguration(connection, configuration) == QUIC_STATUS_SUCCESS

    var observed: seq[ConnectionEventKind] = @[]
    registerConnectionEventHandler(connection, proc (event: ConnectionEvent) =
      observed.add(event.kind)
      if event.kind == ceConnected:
        check event.negotiatedAlpn == "hq-interop"
    )

    check api.ConnectionStart(
      connection,
      configuration,
      QUIC_ADDRESS_FAMILY(0),
      "example.com",
      443) == QUIC_STATUS_SUCCESS

    check observed.contains(ceConnected)

    api.ConnectionClose(connection)
    api.ConfigurationClose(configuration)
    api.RegistrationClose(registration)
    MsQuicClose(apiPtr)
