import unittest

import ../api/api_impl
import ../api/event_model

proc token16(seed: uint8): array[16, uint8] =
  for i in 0 ..< 16:
    result[i] = seed + uint8(i)

suite "MsQuic stateless reset":
  test "registered reset token triggers shutdown events":
    var apiPtr: pointer
    check MsQuicOpenVersion(2'u32, addr apiPtr) == QUIC_STATUS_SUCCESS
    defer:
      MsQuicClose(apiPtr)
    let api = cast[ptr QuicApiTable](apiPtr)

    var registration: HQUIC
    check api.RegistrationOpen(nil, addr registration) == QUIC_STATUS_SUCCESS
    defer:
      api.RegistrationClose(registration)

    var connection: HQUIC
    check api.ConnectionOpen(registration, nil, nil, addr connection) == QUIC_STATUS_SUCCESS
    defer:
      api.ConnectionClose(connection)

    check seedConnectionIdsForTest(connection, @[0x01'u8, 0x02'u8], @[0x03'u8, 0x04'u8])
    let resetToken = token16(0x70)
    check applyPeerNewConnectionIdForTest(connection, 1'u64, 0'u64, @[0x11'u8, 0x12'u8], resetToken)

    var initiated = false
    var completed = false
    registerConnectionEventHandler(connection, proc(ev: ConnectionEvent) =
      if ev.kind == ceShutdownInitiated and ev.note == "stateless reset":
        initiated = true
      elif ev.kind == ceShutdownComplete and ev.note == "stateless reset":
        completed = true
    )

    check processStatelessResetForTest(connection, resetToken)
    check initiated
    check completed
