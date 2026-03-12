import unittest
import ../api/api_impl
import ../api/event_model
import ../api/param_catalog
import ../core/packet_model

suite "MsQuic connection events":
  test "parameter updated event dispatched to Nim handler":
    var apiPtr: pointer
    check MsQuicOpenVersion(2, addr apiPtr) == QUIC_STATUS_SUCCESS
    let api = cast[ptr QuicApiTable](apiPtr)

    var registration: HQUIC
    check api.RegistrationOpen(nil, addr registration) == QUIC_STATUS_SUCCESS

    var connection: HQUIC
    check api.ConnectionOpen(registration, nil, nil, addr connection) == QUIC_STATUS_SUCCESS

    var paramSeen = false
    registerConnectionEventHandler(connection, proc (event: ConnectionEvent) {.closure, gcsafe.} =
      if event.kind == ceParameterUpdated:
        paramSeen = true
        check event.paramId == QUIC_PARAM_CONN_DISABLE_1RTT_ENCRYPTION
        check event.boolValue
    )

    var enabled = 1'u8
    check api.SetParam(
      connection,
      QUIC_PARAM_CONN_DISABLE_1RTT_ENCRYPTION,
      1'u32,
      addr enabled
    ) == QUIC_STATUS_SUCCESS

    check paramSeen

    api.ConnectionClose(connection)
    api.RegistrationClose(registration)
    MsQuicClose(apiPtr)

  test "connection shutdown sends close frame and peer observes shutdown events":
    var apiPtr: pointer
    check MsQuicOpenVersion(2, addr apiPtr) == QUIC_STATUS_SUCCESS
    defer:
      MsQuicClose(apiPtr)
    let api = cast[ptr QuicApiTable](apiPtr)

    var registration: HQUIC
    check api.RegistrationOpen(nil, addr registration) == QUIC_STATUS_SUCCESS
    defer:
      api.RegistrationClose(registration)

    var sender: HQUIC
    var receiver: HQUIC
    check api.ConnectionOpen(registration, nil, nil, addr sender) == QUIC_STATUS_SUCCESS
    check api.ConnectionOpen(registration, nil, nil, addr receiver) == QUIC_STATUS_SUCCESS
    defer:
      api.ConnectionClose(sender)
      api.ConnectionClose(receiver)

    check seedConnectionIdsForTest(sender, @[0x01'u8, 0x02'u8], @[0x03'u8, 0x04'u8], true)
    check seedConnectionIdsForTest(receiver, @[0x03'u8, 0x04'u8], @[0x01'u8, 0x02'u8], false)
    check prepareConnectionPacketSendForTest(sender, packet_model.ceOneRtt)
    check prepareConnectionPacketSendForTest(receiver, packet_model.ceOneRtt)

    var initiated = false
    var completed = false
    var observedError = 0'u64
    registerConnectionEventHandler(receiver, proc (event: ConnectionEvent) {.closure, gcsafe.} =
      if event.kind == ceShutdownInitiated:
        initiated = true
        observedError = event.errorCode
      elif event.kind == ceShutdownComplete:
        completed = true
        observedError = event.errorCode
    )

    api.ConnectionShutdown(sender, QUIC_CONNECTION_SHUTDOWN_FLAGS(0), 42'u64)

    var frameKind = sfkStream
    var payload: seq[byte] = @[]
    check getConnectionLastSentFrameKindForTest(sender, frameKind)
    check frameKind == sfkConnectionClose
    check getConnectionLastSentFramePayloadForTest(sender, payload)
    check receiveOneRttPayloadForTest(receiver, 1'u64, payload, nil, 0'u16)

    check initiated
    check completed
    check observedError == 42'u64
