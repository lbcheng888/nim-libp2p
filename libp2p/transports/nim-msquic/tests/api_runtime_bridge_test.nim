import std/[posix, unittest]

import chronos

import ../api/api_impl
import ../api/runtime_bridge
import ../api/event_model
import ../api/ffi_loader

var gAdoptBridgePtr: pointer

proc waitForPredicate(predicate: proc(): bool; rounds: int = 40): bool =
  for _ in 0 ..< rounds:
    if predicate():
      return true
    waitFor sleepAsync(50.milliseconds)
  predicate()

suite "MsQuic runtime bridge (F5)":
  test "listener new connection event flows through runtime bridge":
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
    gAdoptBridgePtr = cast[pointer](bridge)
    defer:
      var closable = bridge
      close(closable)
      gAdoptBridgePtr = nil

    var registration: HQUIC
    check bridge.openRegistration(nil, registration) == QUIC_STATUS_SUCCESS
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
    check bridge.getApiTable().ConfigurationLoadCredential(configuration, addr credentialDummy) ==
      QUIC_STATUS_SUCCESS

    var listenerNewConnection = false
    var acceptedConnection: HQUIC = nil
    let listenerHandler = proc(event: ListenerEvent) {.gcsafe.} =
      if event.kind == leNewConnection:
        listenerNewConnection = true
        acceptedConnection = event.connection

    var listener: HQUIC
    check bridge.openListener(registration, listenerHandler, listener) == QUIC_STATUS_SUCCESS
    defer:
      bridge.closeListener(listener)

    var bindAddress: Sockaddr_in
    zeroMem(addr bindAddress, sizeof(bindAddress))
    when declared(bindAddress.sin_len):
      bindAddress.sin_len = uint8(sizeof(Sockaddr_in))
    bindAddress.sin_family = uint8(posix.AF_INET)
    bindAddress.sin_port = htons(41031'u16)
    discard inet_pton(posix.AF_INET, "0.0.0.0".cstring, addr bindAddress.sin_addr)
    check bridge.startListener(listener, addr alpnBuffer, 1'u32, addr bindAddress) == QUIC_STATUS_SUCCESS

    var clientConnection: HQUIC
    check api.ConnectionOpen(registration, nil, nil, addr clientConnection) == QUIC_STATUS_SUCCESS
    defer:
      api.ConnectionClose(clientConnection)
    check api.ConnectionSetConfiguration(clientConnection, configuration) == QUIC_STATUS_SUCCESS
    check api.ConnectionStart(
      clientConnection,
      configuration,
      QUIC_ADDRESS_FAMILY(2),
      cstring("127.0.0.1"),
      uint16(41031)) == QUIC_STATUS_SUCCESS

    check waitForPredicate(proc(): bool = listenerNewConnection and not acceptedConnection.isNil)

  test "adoptConnection accepts accepted-connection handle":
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
    gAdoptBridgePtr = cast[pointer](bridge)
    defer:
      var closable = bridge
      close(closable)
      gAdoptBridgePtr = nil

    var registration: HQUIC
    check bridge.openRegistration(nil, registration) == QUIC_STATUS_SUCCESS
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
    check bridge.getApiTable().ConfigurationLoadCredential(configuration, addr credentialDummy) ==
      QUIC_STATUS_SUCCESS

    var aCount = 0
    var adopted = false
    let handlerA = proc(event: ConnectionEvent) {.gcsafe.} =
      if event.kind == ceConnected:
        inc aCount
    let handlerB = proc(event: ConnectionEvent) {.gcsafe.} =
      discard event

    var listener: HQUIC
    let listenerHandler = proc(event: ListenerEvent) {.gcsafe.} =
      if event.kind == leNewConnection and not event.connection.isNil:
        let bridgeRef = cast[RuntimeBridge](gAdoptBridgePtr)
        {.cast(gcsafe).}:
          adopted = bridgeRef.adoptConnection(event.connection, handlerB) == QUIC_STATUS_SUCCESS
    check bridge.openListener(registration, listenerHandler, listener) == QUIC_STATUS_SUCCESS
    defer:
      bridge.closeListener(listener)

    var bindAddress: Sockaddr_in
    zeroMem(addr bindAddress, sizeof(bindAddress))
    when declared(bindAddress.sin_len):
      bindAddress.sin_len = uint8(sizeof(Sockaddr_in))
    bindAddress.sin_family = uint8(posix.AF_INET)
    bindAddress.sin_port = htons(41032'u16)
    discard inet_pton(posix.AF_INET, "0.0.0.0".cstring, addr bindAddress.sin_addr)
    check bridge.startListener(listener, addr alpnBuffer, 1'u32, addr bindAddress) == QUIC_STATUS_SUCCESS

    var clientConnection: HQUIC
    check bridge.openConnection(registration, handlerA, clientConnection) == QUIC_STATUS_SUCCESS
    defer:
      bridge.closeConnection(clientConnection)
    check bridge.getApiTable().ConnectionSetConfiguration(clientConnection, configuration) == QUIC_STATUS_SUCCESS
    check bridge.getApiTable().ConnectionStart(
      clientConnection,
      configuration,
      QUIC_ADDRESS_FAMILY(2),
      cstring("127.0.0.1"),
      uint16(41032)) == QUIC_STATUS_SUCCESS

    check waitForPredicate(proc(): bool = aCount == 1)
    check waitForPredicate(proc(): bool = adopted)
    check aCount == 1
