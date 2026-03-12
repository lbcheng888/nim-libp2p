import std/[posix, unittest]
import ../api/api_impl
import ../api/settings_model
import ../api/param_catalog
import ../core/packet_model

type
  GlobalExecutionConfigHeaderLite = object
    Flags: uint32
    PollingIdleTimeoutUs: uint32
    ProcessorCount: uint32

  SockAddrInLite = object
    family: uint16
    port: uint16
    rest: array[12, uint8]

proc initSockAddrV4(host: string; port: uint16): Sockaddr_in =
  zeroMem(addr result, sizeof(result))
  when declared(result.sin_len):
    result.sin_len = uint8(sizeof(Sockaddr_in))
  result.sin_family = uint8(posix.AF_INET)
  result.sin_port = htons(port)
  discard inet_pton(posix.AF_INET, host.cstring, addr result.sin_addr)

proc sockAddrPort(addrValue: Sockaddr_in): uint16 =
  ntohs(addrValue.sin_port)

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

  test "partition open validates configured processors and receive buffers are accepted":
    var apiPtr: pointer
    check MsQuicOpenVersion(2, addr apiPtr) == QUIC_STATUS_SUCCESS
    let api = cast[ptr QuicApiTable](apiPtr)

    var processors = [11'u16, 13'u16]
    var payload = newSeq[byte](sizeof(GlobalExecutionConfigHeaderLite) + processors.len * sizeof(uint16))
    var header = GlobalExecutionConfigHeaderLite(
      Flags: 1'u32,
      PollingIdleTimeoutUs: 500'u32,
      ProcessorCount: processors.len.uint32
    )
    copyMem(addr payload[0], addr header, sizeof(header))
    copyMem(addr payload[sizeof(header)], addr processors[0], processors.len * sizeof(uint16))
    check api.SetParam(nil, QUIC_PARAM_GLOBAL_EXECUTION_CONFIG, payload.len.uint32, addr payload[0]) ==
      QUIC_STATUS_SUCCESS

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
    check api.ConnectionOpenInPartition(registration, 1'u16, nil, nil, addr connection) ==
      QUIC_STATUS_SUCCESS

    var stream: HQUIC
    check api.StreamOpen(connection, 0'u32, nil, nil, addr stream) == QUIC_STATUS_SUCCESS
    var recvPayload = "recv-buf"
    var recvBuffer = QuicBuffer(
      Length: uint32(recvPayload.len),
      Buffer: cast[ptr uint8](recvPayload.cstring))
    check api.StreamProvideReceiveBuffers(stream, 1'u32, addr recvBuffer) == QUIC_STATUS_SUCCESS

    var invalidConnection: HQUIC
    check api.ConnectionOpenInPartition(registration, 2'u16, nil, nil, addr invalidConnection) ==
      QUIC_STATUS_INVALID_PARAMETER

    api.StreamClose(stream)
    api.ConnectionClose(connection)
    api.ConfigurationClose(configuration)
    api.RegistrationClose(registration)
    MsQuicClose(apiPtr)

  test "zero-rtt anti-replay cache rejects duplicate binder":
    clearBuiltinZeroRttReplayCacheForTest()
    let ticket = @[0xAA'u8, 0xBB'u8, 0xCC'u8]
    let binder = @[0x10'u8, 0x20'u8, 0x30'u8]
    check claimBuiltinZeroRttReplayForTest(ticket, binder, 1_000_000'u64)
    check not claimBuiltinZeroRttReplayForTest(ticket, binder, 1_000_000'u64)
    check not claimBuiltinZeroRttReplayForTest(ticket, @[0x99'u8, 0x88'u8, 0x77'u8], 1_000_000'u64)
    clearBuiltinZeroRttReplayCacheForTest()

  test "ConnectionSendResumptionTicket honors runtime maxEarlyData semantics":
    clearOverrideBuiltinNewSessionTicketLifetimeSecForTest()
    setOverrideBuiltinNewSessionTicketMaxEarlyDataForTest(0'u32)
    defer:
      clearOverrideBuiltinNewSessionTicketMaxEarlyDataForTest()

    let targetHost = "127.0.0.1"
    let targetPort = 41061'u16
    let targetAlpn = "hq-29"

    var apiPtr: pointer
    check MsQuicOpenVersion(2, addr apiPtr) == QUIC_STATUS_SUCCESS
    let api = cast[ptr QuicApiTable](apiPtr)

    var registration: HQUIC
    check api.RegistrationOpen(nil, addr registration) == QUIC_STATUS_SUCCESS

    var alpnBuffer = QuicBuffer(
      Length: uint32(targetAlpn.len),
      Buffer: cast[ptr uint8](targetAlpn.cstring))

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

    var firstConnection: HQUIC
    check api.ConnectionOpen(registration, nil, nil, addr firstConnection) == QUIC_STATUS_SUCCESS
    check api.ConnectionSetConfiguration(firstConnection, configuration) == QUIC_STATUS_SUCCESS
    check api.ConnectionStart(
      firstConnection,
      configuration,
      QUIC_ADDRESS_FAMILY(2),
      cstring(targetHost),
      targetPort) == QUIC_STATUS_SUCCESS

    let customData = "api-ticket"
    check api.ConnectionSendResumptionTicket(
      firstConnection,
      QUIC_SEND_RESUMPTION_FLAGS(0),
      uint16(customData.len),
      cast[ptr uint8](customData.cstring)) == QUIC_STATUS_SUCCESS

    var cachedTicket: seq[byte] = @[]
    check getBuiltinResumptionTicketForTest(cstring(targetHost), targetPort, cstring(targetAlpn), cachedTicket)
    check cachedTicket.len > 0

    var resumedConnection: HQUIC
    check api.ConnectionOpen(registration, nil, nil, addr resumedConnection) == QUIC_STATUS_SUCCESS
    check api.ConnectionSetConfiguration(resumedConnection, configuration) == QUIC_STATUS_SUCCESS
    check api.ConnectionStart(
      resumedConnection,
      configuration,
      QUIC_ADDRESS_FAMILY(2),
      cstring(targetHost),
      targetPort) == QUIC_STATUS_SUCCESS

    var maxData = 1'u32
    var sentBytes = 99'u64
    var deadlineUs = 99'u64
    check getConnectionZeroRttBudgetForTest(resumedConnection, maxData, sentBytes, deadlineUs)
    check maxData == 0'u32

    var requestedEarlyData = true
    check getConnectionClientHelloRequestsEarlyDataForTest(resumedConnection, requestedEarlyData)
    check not requestedEarlyData

    api.ConnectionClose(resumedConnection)
    api.ConnectionClose(firstConnection)
    api.ConfigurationClose(configuration)
    api.RegistrationClose(registration)
    MsQuicClose(apiPtr)

  test "ConnectionSendResumptionTicket honors runtime lifetime semantics":
    setOverrideBuiltinNewSessionTicketLifetimeSecForTest(0'u32)
    clearOverrideBuiltinNewSessionTicketMaxEarlyDataForTest()
    defer:
      clearOverrideBuiltinNewSessionTicketLifetimeSecForTest()

    let targetHost = "127.0.0.1"
    let targetPort = 41062'u16
    let targetAlpn = "hq-29"

    var apiPtr: pointer
    check MsQuicOpenVersion(2, addr apiPtr) == QUIC_STATUS_SUCCESS
    let api = cast[ptr QuicApiTable](apiPtr)

    var registration: HQUIC
    check api.RegistrationOpen(nil, addr registration) == QUIC_STATUS_SUCCESS

    var alpnBuffer = QuicBuffer(
      Length: uint32(targetAlpn.len),
      Buffer: cast[ptr uint8](targetAlpn.cstring))

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

    var firstConnection: HQUIC
    check api.ConnectionOpen(registration, nil, nil, addr firstConnection) == QUIC_STATUS_SUCCESS
    check api.ConnectionSetConfiguration(firstConnection, configuration) == QUIC_STATUS_SUCCESS
    check api.ConnectionStart(
      firstConnection,
      configuration,
      QUIC_ADDRESS_FAMILY(2),
      cstring(targetHost),
      targetPort) == QUIC_STATUS_SUCCESS

    check api.ConnectionSendResumptionTicket(
      firstConnection,
      QUIC_SEND_RESUMPTION_FLAGS(0),
      0'u16,
      nil) == QUIC_STATUS_SUCCESS

    var cachedTicket: seq[byte] = @[]
    check getBuiltinResumptionTicketForTest(cstring(targetHost), targetPort, cstring(targetAlpn), cachedTicket)
    check cachedTicket.len > 0

    var nextConnection: HQUIC
    check api.ConnectionOpen(registration, nil, nil, addr nextConnection) == QUIC_STATUS_SUCCESS
    check api.ConnectionSetConfiguration(nextConnection, configuration) == QUIC_STATUS_SUCCESS
    check api.ConnectionStart(
      nextConnection,
      configuration,
      QUIC_ADDRESS_FAMILY(2),
      cstring(targetHost),
      targetPort) == QUIC_STATUS_SUCCESS

    var requestedEarlyData = true
    check getConnectionClientHelloRequestsEarlyDataForTest(nextConnection, requestedEarlyData)
    check not requestedEarlyData

    cachedTicket = @[]
    check not getBuiltinResumptionTicketForTest(cstring(targetHost), targetPort, cstring(targetAlpn), cachedTicket)

    api.ConnectionClose(nextConnection)
    api.ConnectionClose(firstConnection)
    api.ConfigurationClose(configuration)
    api.RegistrationClose(registration)
    MsQuicClose(apiPtr)

  test "ConnectionSendResumptionTicket requires started configured connection":
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

    var connection: HQUIC
    check api.ConnectionOpen(registration, nil, nil, addr connection) == QUIC_STATUS_SUCCESS
    check api.ConnectionSendResumptionTicket(
      connection,
      QUIC_SEND_RESUMPTION_FLAGS(0),
      0'u16,
      nil) == QUIC_STATUS_INVALID_STATE

    var credentialDummy: uint8
    check api.ConfigurationLoadCredential(configuration, addr credentialDummy) == QUIC_STATUS_SUCCESS
    check api.ConnectionSetConfiguration(connection, configuration) == QUIC_STATUS_SUCCESS
    check api.ConnectionSendResumptionTicket(
      connection,
      QUIC_SEND_RESUMPTION_FLAGS(0),
      0'u16,
      nil) == QUIC_STATUS_INVALID_STATE

    api.ConnectionClose(connection)
    api.ConfigurationClose(configuration)
    api.RegistrationClose(registration)
    MsQuicClose(apiPtr)

  test "ConnectionSendResumptionTicket rejects unsupported flags":
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
    check api.ConnectionStart(
      connection,
      configuration,
      QUIC_ADDRESS_FAMILY(2),
      cstring("127.0.0.1"),
      uint16(41063)) == QUIC_STATUS_SUCCESS

    check api.ConnectionSendResumptionTicket(
      connection,
      QUIC_SEND_RESUMPTION_FLAGS(1),
      0'u16,
      nil) == QUIC_STATUS_NOT_SUPPORTED

    api.ConnectionClose(connection)
    api.ConfigurationClose(configuration)
    api.RegistrationClose(registration)
    MsQuicClose(apiPtr)

  test "listener and connection address params have real runtime effect":
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

    var listener: HQUIC
    check api.ListenerOpen(registration, nil, nil, addr listener) == QUIC_STATUS_SUCCESS

    var configuredListenerAddr = initSockAddrV4("0.0.0.0", 41041'u16)
    check api.SetParam(
      listener,
      QUIC_PARAM_LISTENER_LOCAL_ADDRESS,
      sizeof(Sockaddr_in).uint32,
      addr configuredListenerAddr) == QUIC_STATUS_SUCCESS
    check api.ListenerStart(listener, addr alpnBuffer, 1'u32, nil) == QUIC_STATUS_SUCCESS

    var listenerAddr: Sockaddr_in
    var listenerAddrLen = sizeof(listenerAddr).uint32
    check api.GetParam(
      listener,
      QUIC_PARAM_LISTENER_LOCAL_ADDRESS,
      addr listenerAddrLen,
      addr listenerAddr) == QUIC_STATUS_SUCCESS
    check sockAddrPort(listenerAddr) == 41041'u16

    var connection: HQUIC
    check api.ConnectionOpen(registration, nil, nil, addr connection) == QUIC_STATUS_SUCCESS
    check api.ConnectionSetConfiguration(connection, configuration) == QUIC_STATUS_SUCCESS

    var preStartRemoteAddr = initSockAddrV4("127.0.0.1", 41043'u16)
    check api.SetParam(
      connection,
      QUIC_PARAM_CONN_REMOTE_ADDRESS,
      sizeof(Sockaddr_in).uint32,
      addr preStartRemoteAddr) == QUIC_STATUS_SUCCESS
    var remoteAddr: Sockaddr_in
    var remoteAddrLen = sizeof(remoteAddr).uint32
    check api.GetParam(
      connection,
      QUIC_PARAM_CONN_REMOTE_ADDRESS,
      addr remoteAddrLen,
      addr remoteAddr) == QUIC_STATUS_SUCCESS
    check sockAddrPort(remoteAddr) == 41043'u16

    var configuredLocalAddr = initSockAddrV4("0.0.0.0", 41042'u16)
    check api.SetParam(
      connection,
      QUIC_PARAM_CONN_LOCAL_ADDRESS,
      sizeof(Sockaddr_in).uint32,
      addr configuredLocalAddr) == QUIC_STATUS_SUCCESS
    check api.ConnectionStart(
      connection,
      configuration,
      QUIC_ADDRESS_FAMILY(2),
      cstring("127.0.0.1"),
      uint16(41041)) == QUIC_STATUS_SUCCESS

    var localAddr: Sockaddr_in
    var localAddrLen = sizeof(localAddr).uint32
    check api.GetParam(
      connection,
      QUIC_PARAM_CONN_LOCAL_ADDRESS,
      addr localAddrLen,
      addr localAddr) == QUIC_STATUS_SUCCESS
    check sockAddrPort(localAddr) == 41042'u16

    check prepareConnectionPacketSendForTest(connection, packet_model.ceOneRtt)

    var updatedRemoteAddr = initSockAddrV4("127.0.0.1", 41044'u16)
    check api.SetParam(
      connection,
      QUIC_PARAM_CONN_REMOTE_ADDRESS,
      sizeof(Sockaddr_in).uint32,
      addr updatedRemoteAddr) == QUIC_STATUS_SUCCESS

    remoteAddrLen = sizeof(remoteAddr).uint32
    check api.GetParam(
      connection,
      QUIC_PARAM_CONN_REMOTE_ADDRESS,
      addr remoteAddrLen,
      addr remoteAddr) == QUIC_STATUS_SUCCESS
    check sockAddrPort(remoteAddr) == 41041'u16

    var activePathId = 255'u8
    var pathCount = 0'u8
    var pendingChallenges = 0'u32
    var isActive = false
    var isValidated = false
    var challengeOutstanding = false
    var responsePending = false
    check getConnectionActivePathId(connection, activePathId)
    check activePathId == 0'u8
    check getConnectionKnownPathCount(connection, pathCount)
    check pathCount == 2'u8
    check getConnectionPendingChallengeCountForTest(connection, pendingChallenges)
    check pendingChallenges == 1'u32
    check getConnectionPathState(
      connection,
      1'u8,
      isActive,
      isValidated,
      challengeOutstanding,
      responsePending)
    check not isActive
    check not isValidated
    check challengeOutstanding
    check responsePending

    var challengeData: array[8, uint8]
    check getConnectionPathChallengeDataForTest(connection, 1'u8, challengeData)
    check applyPathResponseForTest(connection, challengeData)

    check getConnectionActivePathId(connection, activePathId)
    check activePathId == 1'u8
    check getConnectionPendingChallengeCountForTest(connection, pendingChallenges)
    check pendingChallenges == 0'u32
    check getConnectionPathState(
      connection,
      1'u8,
      isActive,
      isValidated,
      challengeOutstanding,
      responsePending)
    check isActive
    check isValidated
    check not challengeOutstanding
    check not responsePending

    remoteAddrLen = sizeof(remoteAddr).uint32
    check api.GetParam(
      connection,
      QUIC_PARAM_CONN_REMOTE_ADDRESS,
      addr remoteAddrLen,
      addr remoteAddr) == QUIC_STATUS_SUCCESS
    check sockAddrPort(remoteAddr) == 41044'u16

    api.ConnectionClose(connection)
    api.ListenerStop(listener)
    api.ListenerClose(listener)
    api.ConfigurationClose(configuration)
    api.RegistrationClose(registration)
    MsQuicClose(apiPtr)
