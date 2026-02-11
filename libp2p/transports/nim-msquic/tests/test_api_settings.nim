import unittest
import ../api/api_impl
import ../api/settings_model
import ../api/param_catalog

suite "MsQuic API settings":
  test "apply settings overlay and datagram flags":
    var apiPtr: pointer
    check MsQuicOpenVersion(2, addr apiPtr) == QUIC_STATUS_SUCCESS
    let api = cast[ptr QuicApiTable](apiPtr)

    var registration: HQUIC
    check api.RegistrationOpen(nil, addr registration) == QUIC_STATUS_SUCCESS

    var alpn = "hq-29"
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

    var overlay = overlayForProfile(profileMaxThroughput)
    overlay.idleTimeoutMs = 45_000
    check applySettingsOverlay(connection, overlay) == QUIC_STATUS_SUCCESS

    var readOverlay: QuicSettingsOverlay
    check getConnectionSettingsOverlay(connection, readOverlay)
    check readOverlay.idleTimeoutMs == 45_000
    check readOverlay.datagramReceiveEnabled

    var bufferLen: uint32 = 1
    var boolValue: uint8
    check api.GetParam(
      connection,
      QUIC_PARAM_CONN_DATAGRAM_RECEIVE_ENABLED,
      addr bufferLen,
      addr boolValue) == QUIC_STATUS_SUCCESS
    check boolValue == 1

    boolValue = 1
    check api.SetParam(
      connection,
      QUIC_PARAM_CONN_DATAGRAM_SEND_ENABLED,
      1,
      addr boolValue) == QUIC_STATUS_SUCCESS

    var recvEnabled, sendEnabled: bool
    check getConnectionDatagramState(connection, recvEnabled, sendEnabled)
    check recvEnabled
    check sendEnabled

    api.ConnectionClose(connection)
    api.ConfigurationClose(configuration)
    api.RegistrationClose(registration)
    MsQuicClose(apiPtr)
