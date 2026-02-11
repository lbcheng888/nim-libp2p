import std/unittest

import ../api/api_impl
import ../api/runtime_bridge
import ../api/event_model
import ../api/ffi_loader

suite "MsQuic runtime bridge (F5)":
  test "connection connected event flows through runtime bridge":
    var apiPtr: pointer
    check MsQuicOpenVersion(2'u32, addr apiPtr) == QUIC_STATUS_SUCCESS
    defer:
      MsQuicClose(apiPtr)

    let api = cast[ptr QuicApiTable](apiPtr)
    let runtime = MsQuicRuntime(
      handle: nil,
      path: "nim-msquic",
      apiTable: cast[QuicApiTablePtr](api),
      closeProc: nil,
      status: 0,
      openSymbol: "MsQuicOpenVersion",
      versionRequested: DefaultMsQuicVersion,
      versionNegotiated: DefaultMsQuicVersion
    )

    let bridge = initRuntimeBridge(runtime)
    defer:
      var closable = bridge
      close(closable)

    var registration: HQUIC
    var regConfig = QuicRegistrationConfigC(
      AppName: cstring("runtime-bridge"),
      ExecutionProfile: QUIC_EXECUTION_PROFILE(0)
    )
    check bridge.openRegistration(addr regConfig, registration) == QUIC_STATUS_SUCCESS
    defer:
      bridge.closeRegistration(registration)

    const BridgeAlpn = "nim-bridge"
    var alpnBuffer = QuicBuffer(
      Length: uint32(BridgeAlpn.len),
      Buffer: cast[ptr uint8](BridgeAlpn.cstring)
    )
    var configuration: HQUIC
    check bridge.openConfiguration(
      registration,
      addr alpnBuffer,
      1'u32,
      nil,
      0'u32,
      nil,
      configuration) == QUIC_STATUS_SUCCESS
    defer:
      bridge.closeConfiguration(configuration)

    var credentialDummy: uint8 = 0
    check not bridge.getApiTable().ConfigurationLoadCredential.isNil
    check bridge.getApiTable().ConfigurationLoadCredential(
      configuration,
      addr credentialDummy) == QUIC_STATUS_SUCCESS

    var connection: HQUIC
    var handlerInvoked = 0
    var sawExpectedConnected = false
    let handler = proc(event: ConnectionEvent) {.gcsafe.} =
      inc handlerInvoked
      if event.kind == ceConnected and
          event.negotiatedAlpn == BridgeAlpn and
          event.sessionResumed == false:
        sawExpectedConnected = true

    check bridge.openConnection(registration, handler, connection) == QUIC_STATUS_SUCCESS
    defer:
      bridge.closeConnection(connection)

    check bridge.getApiTable().ConnectionSetConfiguration(connection, configuration) == QUIC_STATUS_SUCCESS
    check bridge.getApiTable().ConnectionStart(
      connection,
      configuration,
      QUIC_ADDRESS_FAMILY(0),
      cstring("example.com"),
      uint16(443)) == QUIC_STATUS_SUCCESS

    check handlerInvoked == 1
    check sawExpectedConnected

  test "adoptConnection swaps handler before start":
    var apiPtr: pointer
    check MsQuicOpenVersion(2'u32, addr apiPtr) == QUIC_STATUS_SUCCESS
    defer:
      MsQuicClose(apiPtr)

    let api = cast[ptr QuicApiTable](apiPtr)
    let runtime = MsQuicRuntime(
      handle: nil,
      path: "nim-msquic",
      apiTable: cast[QuicApiTablePtr](api),
      closeProc: nil,
      status: 0,
      openSymbol: "MsQuicOpenVersion",
      versionRequested: DefaultMsQuicVersion,
      versionNegotiated: DefaultMsQuicVersion
    )

    let bridge = initRuntimeBridge(runtime)
    defer:
      var closable = bridge
      close(closable)

    var registration: HQUIC
    check bridge.openRegistration(nil, registration) == QUIC_STATUS_SUCCESS
    defer:
      bridge.closeRegistration(registration)

    var configuration: HQUIC
    check bridge.openConfiguration(
      registration,
      nil,
      0,
      nil,
      0,
      nil,
      configuration) == QUIC_STATUS_SUCCESS
    defer:
      bridge.closeConfiguration(configuration)

    var credentialDummy: uint8 = 0
    check bridge.getApiTable().ConfigurationLoadCredential(configuration, addr credentialDummy) == QUIC_STATUS_SUCCESS

    var connection: HQUIC
    var aCount = 0
    var bCount = 0
    let handlerA = proc(event: ConnectionEvent) {.gcsafe.} =
      inc aCount
    let handlerB = proc(event: ConnectionEvent) {.gcsafe.} =
      inc bCount

    check bridge.openConnection(registration, handlerA, connection) == QUIC_STATUS_SUCCESS
    defer:
      bridge.closeConnection(connection)

    check bridge.adoptConnection(connection, handlerB) == QUIC_STATUS_SUCCESS

    check bridge.getApiTable().ConnectionSetConfiguration(connection, configuration) == QUIC_STATUS_SUCCESS
    check bridge.getApiTable().ConnectionStart(
      connection,
      configuration,
      QUIC_ADDRESS_FAMILY(0),
      cstring("example.com"),
      uint16(443)) == QUIC_STATUS_SUCCESS

    check aCount == 0
    check bCount == 1
