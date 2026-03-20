import std/[endians, posix, sequtils, strutils, unittest]
from std/times import epochTime

import chronos

import ../api/api_impl
import ../api/diagnostics_model
import ../api/event_model
import ../api/param_catalog
import ../congestion/common

const
  NativeListenerNewConnection = 0'u32
  NativeStreamEventReceive = 1'u32
  TestMessage = "ping-from-client"
  TestDatagram = "datagram-from-client"

var
  gAcceptedConn: HQUIC
  gAcceptedStream: HQUIC
  gHandshakeDerived: bool
  gClientFinishedVerified: bool
  gReceivedMessageOk: bool
  gReceivedDatagramOk: bool
  gAckObserved: bool
  gSessionTicketReceived: bool
  gSessionResumedObserved: bool
  gServerSessionResumedObserved: bool
  gHandshakeDoneObserved: bool
  gClientFinishedHandshakeVerifyFailed: bool
  gExpectZeroRttWindow: bool
  gZeroRttPreHandshakeObserved: bool
  gColdStartConnected: bool
  gColdStartResumed: bool
  gPeerSendAbortedObserved: bool
  gPeerSendAbortCode: uint64

proc extractStreamPayload(event: ptr QuicStreamEvent): seq[byte] =
  if event.isNil or event.Type != NativeStreamEventReceive:
    return @[]
  let payload = cast[ptr QuicStreamEventReceivePayload](addr event.Data[0])
  if payload.isNil or payload.TotalBufferLength == 0'u64 or payload.BufferCount == 0'u32 or
      payload.Buffers.isNil:
    return @[]
  let buffers = cast[ptr UncheckedArray[QuicBuffer]](payload.Buffers)
  var remaining = int(payload.TotalBufferLength)
  for idx in 0 ..< int(payload.BufferCount):
    let buf = buffers[idx]
    if buf.Buffer.isNil or buf.Length == 0:
      continue
    let chunkLen = min(remaining, int(buf.Length))
    let start = result.len
    result.setLen(start + chunkLen)
    copyMem(addr result[start], buf.Buffer, chunkLen)
    remaining -= chunkLen
    if remaining <= 0:
      break

proc waitForPredicate(predicate: proc(): bool; rounds: int = 40): bool =
  for _ in 0 ..< rounds:
    if predicate():
      return true
    waitFor sleepAsync(50.milliseconds)
  predicate()

suite "MsQuic builtin pure-Nim loopback":
  test "listener replies with ServerHello and receives 1-RTT STREAM payload":
    gAcceptedConn = nil
    gAcceptedStream = nil
    gHandshakeDerived = false
    gClientFinishedVerified = false
    gReceivedMessageOk = false
    gReceivedDatagramOk = false
    gAckObserved = false
    gSessionTicketReceived = false
    gSessionResumedObserved = false
    gServerSessionResumedObserved = false
    gHandshakeDoneObserved = false
    gClientFinishedHandshakeVerifyFailed = false
    gExpectZeroRttWindow = false
    gZeroRttPreHandshakeObserved = false
    gColdStartConnected = false
    gColdStartResumed = false
    gPeerSendAbortedObserved = false
    gPeerSendAbortCode = 0'u64
    clearBuiltinZeroRttReplayCacheForTest()

    clearDiagnosticsHooks()
    registerDiagnosticsHook(proc(event: DiagnosticsEvent) {.gcsafe.} =
      when defined(msquic_diag_test):
        stderr.writeLine(event.note)
      if event.note.startsWith("Server Finished verified via Handshake packet"):
        gHandshakeDerived = true
      elif event.note.startsWith("Client Finished verified via Handshake packet"):
        gClientFinishedVerified = true
      elif event.note.contains("Session ticket received len="):
        gSessionTicketReceived = true
      elif event.note.contains("HANDSHAKE_DONE received."):
        gHandshakeDoneObserved = true
      elif event.note.contains("Client Finished verify failed via Handshake packet."):
        gClientFinishedHandshakeVerifyFailed = true
      elif event.note.contains("RX 0-RTT pn="):
        gZeroRttPreHandshakeObserved = true
      if event.note.contains("RX ACK largest="):
        gAckObserved = true
    )
    defer:
      clearDiagnosticsHooks()

    var apiPtr: pointer
    check MsQuicOpenVersion(2'u32, addr apiPtr) == QUIC_STATUS_SUCCESS
    defer:
      MsQuicClose(apiPtr)
    let api = cast[ptr QuicApiTable](apiPtr)

    var registration: HQUIC
    check api.RegistrationOpen(nil, addr registration) == QUIC_STATUS_SUCCESS
    defer:
      api.RegistrationClose(registration)

    const TestAlpn = "nim-builtinq"
    var alpnBuffer = QuicBuffer(
      Length: uint32(TestAlpn.len),
      Buffer: cast[ptr uint8](TestAlpn.cstring)
    )

    var configuration: HQUIC
    check api.ConfigurationOpen(registration, addr alpnBuffer, 1'u32, nil, 0'u32, nil, addr configuration) ==
      QUIC_STATUS_SUCCESS
    defer:
      api.ConfigurationClose(configuration)

    var credentialDummy: uint8 = 0
    check api.ConfigurationLoadCredential(configuration, addr credentialDummy) == QUIC_STATUS_SUCCESS

    let serverStreamHandler = proc(event: StreamEvent) {.gcsafe.} =
      let payload = event.payload
      if event.kind == seReceive and payload.len == TestMessage.len:
        var matched = true
        for idx, ch in TestMessage:
          if payload[idx] != uint8(ord(ch) and 0xFF):
            matched = false
            break
        if matched:
          gReceivedMessageOk = true
      elif event.kind == sePeerSendAborted:
        gPeerSendAbortedObserved = true
        gPeerSendAbortCode = event.errorCode

    let listenerCallback = proc(listener: HQUIC; context: pointer; event: pointer): QUIC_STATUS {.cdecl.} =
      discard listener
      discard context
      let native = cast[ptr QuicListenerEvent](event)
      if native.isNil or native.Type != NativeListenerNewConnection:
        return QUIC_STATUS_SUCCESS
      let payload = cast[ptr QuicListenerEventNewConnectionPayload](addr native.Data[0])
      if payload.isNil or payload.Connection.isNil:
        return QUIC_STATUS_SUCCESS
      gAcceptedConn = payload.Connection
      registerConnectionEventHandler(gAcceptedConn, proc(ev: ConnectionEvent) {.gcsafe.} =
        if ev.kind == cePeerStreamStarted and not ev.stream.isNil and gAcceptedStream.isNil:
          gAcceptedStream = ev.stream
          {.cast(gcsafe).}:
            registerStreamEventHandler(gAcceptedStream, serverStreamHandler)
        elif ev.kind == ceDatagramReceived and ev.datagramPayload.len == TestDatagram.len:
          var matched = true
          for idx, ch in TestDatagram:
            if ev.datagramPayload[idx] != uint8(ord(ch) and 0xFF):
              matched = false
              break
          if matched:
            gReceivedDatagramOk = true
        elif ev.kind == ceConnected and ev.sessionResumed:
          gServerSessionResumedObserved = true
        elif ev.kind == ceConnected:
          when defined(msquic_diag_test):
            stderr.writeLine("server ceConnected resumed=" & $ev.sessionResumed)
      )
      QUIC_STATUS_SUCCESS

    var listener: HQUIC
    check api.ListenerOpen(registration, cast[pointer](listenerCallback), nil, addr listener) == QUIC_STATUS_SUCCESS
    defer:
      api.ListenerClose(listener)

    var bindAddress: Sockaddr_in
    zeroMem(addr bindAddress, sizeof(bindAddress))
    when declared(bindAddress.sin_len):
      bindAddress.sin_len = uint8(sizeof(Sockaddr_in))
    bindAddress.sin_family = uint8(posix.AF_INET)
    bindAddress.sin_port = htons(41001'u16)
    discard inet_pton(posix.AF_INET, "0.0.0.0".cstring, addr bindAddress.sin_addr)
    check api.ListenerStart(listener, addr alpnBuffer, 1'u32, addr bindAddress) == QUIC_STATUS_SUCCESS

    var listenerAddrLen = uint32(sizeof(Sockaddr_storage))
    var listenerAddr: Sockaddr_storage
    check api.GetParam(listener, QUIC_PARAM_LISTENER_LOCAL_ADDRESS, addr listenerAddrLen, addr listenerAddr) ==
      QUIC_STATUS_SUCCESS
    var listenerTa = TransportAddress(family: AddressFamily.None)
    var listenerSockLen = SockLen(listenerAddrLen)
    fromSAddr(addr listenerAddr, listenerSockLen, listenerTa)
    check listenerTa.family == AddressFamily.IPv4
    check listenerTa.port == Port(41001)

    var clientConnection: HQUIC
    check api.ConnectionOpen(registration, nil, nil, addr clientConnection) == QUIC_STATUS_SUCCESS
    defer:
      api.ConnectionClose(clientConnection)
    check api.ConnectionSetConfiguration(clientConnection, configuration) == QUIC_STATUS_SUCCESS
    registerConnectionEventHandler(clientConnection, proc(ev: ConnectionEvent) =
      discard ev
    )
    check api.ConnectionStart(
      clientConnection,
      configuration,
      QUIC_ADDRESS_FAMILY(2),
      cstring("127.0.0.1"),
      uint16(41001)
    ) == QUIC_STATUS_SUCCESS

    var algorithmRaw = uint16(caBbr)
    check api.SetParam(
      clientConnection,
      QUIC_PARAM_CONN_CONGESTION_CONTROL_ALGORITHM,
      uint32(sizeof(uint16)),
      addr algorithmRaw
    ) == QUIC_STATUS_SUCCESS

    let initialHandshakeReady = waitForPredicate(proc(): bool =
      gAcceptedConn != nil and gHandshakeDerived and gClientFinishedVerified and
        gSessionTicketReceived and gHandshakeDoneObserved
    , rounds = 120)
    when defined(msquic_diag_test):
      if not initialHandshakeReady:
        stderr.writeLine(
          "initialHandshakeReady=false accepted=" & $(gAcceptedConn != nil) &
          " handshakeDerived=" & $gHandshakeDerived &
          " clientFinished=" & $gClientFinishedVerified &
          " sessionTicket=" & $gSessionTicketReceived &
          " handshakeDone=" & $gHandshakeDoneObserved
        )
    check initialHandshakeReady

    var initialTicketAgeAdd = 0'u32
    check getConnectionResumptionTicketAgeAddForTest(clientConnection, initialTicketAgeAdd)
    check initialTicketAgeAdd != 0'u32

    var clientRemoteLen = uint32(sizeof(Sockaddr_storage))
    var clientRemote: Sockaddr_storage
    check api.GetParam(clientConnection, QUIC_PARAM_CONN_REMOTE_ADDRESS, addr clientRemoteLen, addr clientRemote) ==
      QUIC_STATUS_SUCCESS
    var clientRemoteTa = TransportAddress(family: AddressFamily.None)
    var clientRemoteSockLen = SockLen(clientRemoteLen)
    fromSAddr(addr clientRemote, clientRemoteSockLen, clientRemoteTa)
    check clientRemoteTa.family == AddressFamily.IPv4
    check clientRemoteTa.port == Port(41001)

    var clientLocalLen = uint32(sizeof(Sockaddr_storage))
    var clientLocal: Sockaddr_storage
    check api.GetParam(clientConnection, QUIC_PARAM_CONN_LOCAL_ADDRESS, addr clientLocalLen, addr clientLocal) ==
      QUIC_STATUS_SUCCESS
    var clientLocalTa = TransportAddress(family: AddressFamily.None)
    var clientLocalSockLen = SockLen(clientLocalLen)
    fromSAddr(addr clientLocal, clientLocalSockLen, clientLocalTa)
    check clientLocalTa.family == AddressFamily.IPv4

    var activePathId = 255'u8
    check getConnectionActivePathId(clientConnection, activePathId)
    check activePathId == 0'u8

    var enabled = BOOLEAN(1)
    check api.SetParam(clientConnection, QUIC_PARAM_CONN_DATAGRAM_RECEIVE_ENABLED, 1'u32, addr enabled) ==
      QUIC_STATUS_SUCCESS
    check api.SetParam(clientConnection, QUIC_PARAM_CONN_DATAGRAM_SEND_ENABLED, 1'u32, addr enabled) ==
      QUIC_STATUS_SUCCESS
    check api.SetParam(gAcceptedConn, QUIC_PARAM_CONN_DATAGRAM_RECEIVE_ENABLED, 1'u32, addr enabled) ==
      QUIC_STATUS_SUCCESS
    check api.SetParam(gAcceptedConn, QUIC_PARAM_CONN_DATAGRAM_SEND_ENABLED, 1'u32, addr enabled) ==
      QUIC_STATUS_SUCCESS

    var clientStream: HQUIC
    check api.StreamOpen(clientConnection, QUIC_STREAM_OPEN_FLAGS(0), nil, nil, addr clientStream) ==
      QUIC_STATUS_SUCCESS
    defer:
      api.StreamClose(clientStream)
    check api.StreamStart(clientStream, QUIC_STREAM_START_FLAGS(0)) == QUIC_STATUS_SUCCESS

    var streamIdLen = uint32(sizeof(uint64))
    var streamId = 0'u64
    check api.GetParam(clientStream, QUIC_PARAM_STREAM_ID, addr streamIdLen, addr streamId) ==
      QUIC_STATUS_SUCCESS
    check streamId == 0'u64

    var clientUniStream: HQUIC
    check api.StreamOpen(clientConnection, QUIC_STREAM_OPEN_FLAGS(1), nil, nil, addr clientUniStream) ==
      QUIC_STATUS_SUCCESS
    defer:
      api.StreamClose(clientUniStream)
    check api.StreamStart(clientUniStream, QUIC_STREAM_START_FLAGS(0)) == QUIC_STATUS_SUCCESS
    streamIdLen = uint32(sizeof(uint64))
    streamId = 0'u64
    check api.GetParam(clientUniStream, QUIC_PARAM_STREAM_ID, addr streamIdLen, addr streamId) ==
      QUIC_STATUS_SUCCESS
    check streamId == 2'u64

    var raw = newSeq[uint8](TestMessage.len)
    for idx, ch in TestMessage:
      raw[idx] = uint8(ord(ch) and 0xFF)
    var sendBuffer = QuicBuffer(
      Length: uint32(raw.len),
      Buffer: addr raw[0]
    )
    check api.StreamSend(clientStream, addr sendBuffer, 1'u32, QUIC_SEND_FLAGS(0), nil) == QUIC_STATUS_SUCCESS

    check waitForPredicate(proc(): bool =
      gReceivedMessageOk
    , rounds = 120)

    var datagramRaw = newSeq[uint8](TestDatagram.len)
    for idx, ch in TestDatagram:
      datagramRaw[idx] = uint8(ord(ch) and 0xFF)
    var datagramBuffer = QuicBuffer(
      Length: uint32(datagramRaw.len),
      Buffer: addr datagramRaw[0]
    )
    check api.DatagramSend(clientConnection, addr datagramBuffer, 1'u32, QUIC_SEND_FLAGS(0), nil) ==
      QUIC_STATUS_SUCCESS

    check waitForPredicate(proc(): bool =
      gReceivedDatagramOk and gAckObserved
    , rounds = 120)

    var disable1Rtt = BOOLEAN(1)
    check api.SetParam(
      clientConnection,
      QUIC_PARAM_CONN_DISABLE_1RTT_ENCRYPTION,
      1'u32,
      addr disable1Rtt
    ) == QUIC_STATUS_SUCCESS
    check api.SetParam(
      gAcceptedConn,
      QUIC_PARAM_CONN_DISABLE_1RTT_ENCRYPTION,
      1'u32,
      addr disable1Rtt
    ) == QUIC_STATUS_SUCCESS

    var disableValue = uint8(0)
    var disableLen = 1'u32
    check api.GetParam(
      clientConnection,
      QUIC_PARAM_CONN_DISABLE_1RTT_ENCRYPTION,
      addr disableLen,
      addr disableValue
    ) == QUIC_STATUS_SUCCESS
    check disableValue == 1'u8
    disableLen = 1'u32
    disableValue = 0'u8
    check api.GetParam(
      gAcceptedConn,
      QUIC_PARAM_CONN_DISABLE_1RTT_ENCRYPTION,
      addr disableLen,
      addr disableValue
    ) == QUIC_STATUS_SUCCESS
    check disableValue == 1'u8

    gReceivedMessageOk = false
    check api.StreamSend(clientStream, addr sendBuffer, 1'u32, QUIC_SEND_FLAGS(0), nil) ==
      QUIC_STATUS_SUCCESS
    check waitForPredicate(proc(): bool =
      gReceivedMessageOk
    , rounds = 120)

    var congestionWindow = 0'u64
    check getConnectionCongestionWindow(clientConnection, congestionWindow)
    check congestionWindow > 0'u64

    var latestRttUs = 0'u64
    var smoothedRttUs = 0'u64
    var minRttUs = 0'u64
    var rttVarianceUs = 0'u64
    check getConnectionRttStats(
      clientConnection,
      latestRttUs,
      smoothedRttUs,
      minRttUs,
      rttVarianceUs
    )
    check latestRttUs > 0'u64
    check smoothedRttUs > 0'u64
    check minRttUs > 0'u64

    var probeCount = high(uint16)
    check getConnectionProbeCount(clientConnection, probeCount)
    check probeCount == 0'u16

    check api.StreamShutdown(
      clientStream,
      QUIC_STREAM_SHUTDOWN_FLAGS(QUIC_STREAM_SHUTDOWN_FLAG_ABORT_SEND),
      QUIC_UINT62(77)
    ) == QUIC_STATUS_SUCCESS
    check waitForPredicate(proc(): bool =
      gPeerSendAbortedObserved
    , rounds = 120)
    check gPeerSendAbortCode == 77'u64

    var knownPaths = 0'u8
    check getConnectionKnownPathCount(clientConnection, knownPaths)
    check knownPaths >= 1'u8

    var pendingPathId = 255'u8
    check triggerConnectionMigrationProbe(
      clientConnection,
      cstring("127.0.0.1"),
      uint16(41002),
      pendingPathId
    )
    check pendingPathId == 1'u8

    var pathActive = false
    var pathValidated = false
    var challengeOutstanding = false
    var responsePending = false
    check getConnectionPathState(
      clientConnection,
      pendingPathId,
      pathActive,
      pathValidated,
      challengeOutstanding,
      responsePending
    )
    check not pathActive
    check not pathValidated
    check challengeOutstanding
    check responsePending

    activePathId = 255'u8
    check getConnectionActivePathId(clientConnection, activePathId)
    check activePathId == 0'u8

    check confirmConnectionValidatedPath(clientConnection, pendingPathId)
    check getConnectionPathState(
      clientConnection,
      pendingPathId,
      pathActive,
      pathValidated,
      challengeOutstanding,
      responsePending
    )
    check pathActive
    check pathValidated
    check not challengeOutstanding
    check not responsePending

    activePathId = 255'u8
    check getConnectionActivePathId(clientConnection, activePathId)
    check activePathId == 1'u8

    var timeoutPathId = 255'u8
    check triggerConnectionMigrationProbe(
      clientConnection,
      cstring("127.0.0.1"),
      uint16(41003),
      timeoutPathId
    )
    waitFor sleepAsync(375.milliseconds)
    check runConnectionLossRecoveryTick(clientConnection)
    check getConnectionProbeCount(clientConnection, probeCount)
    check probeCount > 0'u16

    var replayedZeroRttTicket: seq[byte]
    check getBuiltinResumptionTicketForTest(
      cstring("127.0.0.1"),
      uint16(41001),
      cstring(TestAlpn),
      replayedZeroRttTicket
    )
    check replayedZeroRttTicket.len > 0

    gAcceptedConn = nil
    gAcceptedStream = nil
    gReceivedMessageOk = false
    gHandshakeDerived = false
    gClientFinishedVerified = false
    gSessionTicketReceived = false
    gSessionResumedObserved = false
    gServerSessionResumedObserved = false
    gExpectZeroRttWindow = true
    gZeroRttPreHandshakeObserved = false

    var resumedConnection: HQUIC
    check api.ConnectionOpen(registration, nil, nil, addr resumedConnection) == QUIC_STATUS_SUCCESS
    defer:
      api.ConnectionClose(resumedConnection)
    check api.ConnectionSetConfiguration(resumedConnection, configuration) == QUIC_STATUS_SUCCESS
    registerConnectionEventHandler(resumedConnection, proc(ev: ConnectionEvent) =
      if ev.kind == ceConnected and ev.sessionResumed:
        gSessionResumedObserved = true
    )
    check api.ConnectionStart(
      resumedConnection,
      configuration,
      QUIC_ADDRESS_FAMILY(2),
      cstring("127.0.0.1"),
      uint16(41001)
    ) == QUIC_STATUS_SUCCESS

    var resumedStream: HQUIC
    check api.StreamOpen(resumedConnection, QUIC_STREAM_OPEN_FLAGS(0), nil, nil, addr resumedStream) ==
      QUIC_STATUS_SUCCESS
    defer:
      api.StreamClose(resumedStream)
    check api.StreamStart(resumedStream, QUIC_STREAM_START_FLAGS(0)) == QUIC_STATUS_SUCCESS
    check api.StreamSend(
      resumedStream,
      addr sendBuffer,
      1'u32,
      QUIC_SEND_FLAGS(0x0001'u32),
      nil
    ) == QUIC_STATUS_SUCCESS

    check waitForPredicate(proc(): bool = gSessionResumedObserved, rounds = 120)
    var serverResumedState = false
    check waitForPredicate(proc(): bool =
      gClientFinishedHandshakeVerifyFailed or
      (not gAcceptedConn.isNil and
        getConnectionSessionResumedForTest(gAcceptedConn, serverResumedState) and
        serverResumedState)
    , rounds = 120)
    check not gClientFinishedHandshakeVerifyFailed
    check getConnectionSessionResumedForTest(gAcceptedConn, serverResumedState)
    check serverResumedState
    check waitForPredicate(proc(): bool = gReceivedMessageOk, rounds = 120)
    check waitForPredicate(proc(): bool = gZeroRttPreHandshakeObserved, rounds = 120)
    check waitForPredicate(proc(): bool = gSessionTicketReceived, rounds = 120)

    var refreshedTicketAgeAdd = 0'u32
    check getConnectionResumptionTicketAgeAddForTest(resumedConnection, refreshedTicketAgeAdd)
    check refreshedTicketAgeAdd != 0'u32
    check refreshedTicketAgeAdd != initialTicketAgeAdd

    check setBuiltinResumptionTicketForTest(
      cstring("127.0.0.1"),
      uint16(41001),
      cstring(TestAlpn),
      replayedZeroRttTicket
    )

    gAcceptedConn = nil
    gAcceptedStream = nil
    gReceivedMessageOk = false
    gSessionResumedObserved = false
    gServerSessionResumedObserved = false
    gExpectZeroRttWindow = true
    gZeroRttPreHandshakeObserved = false

    var replayRejectedConnection: HQUIC
    check api.ConnectionOpen(registration, nil, nil, addr replayRejectedConnection) == QUIC_STATUS_SUCCESS
    defer:
      api.ConnectionClose(replayRejectedConnection)
    check api.ConnectionSetConfiguration(replayRejectedConnection, configuration) == QUIC_STATUS_SUCCESS
    registerConnectionEventHandler(replayRejectedConnection, proc(ev: ConnectionEvent) =
      if ev.kind == ceConnected and ev.sessionResumed:
        gSessionResumedObserved = true
    )
    check api.ConnectionStart(
      replayRejectedConnection,
      configuration,
      QUIC_ADDRESS_FAMILY(2),
      cstring("127.0.0.1"),
      uint16(41001)
    ) == QUIC_STATUS_SUCCESS

    var replayRejectedStream: HQUIC
    check api.StreamOpen(
      replayRejectedConnection,
      QUIC_STREAM_OPEN_FLAGS(0),
      nil,
      nil,
      addr replayRejectedStream
    ) == QUIC_STATUS_SUCCESS
    defer:
      api.StreamClose(replayRejectedStream)
    check api.StreamStart(replayRejectedStream, QUIC_STREAM_START_FLAGS(0)) == QUIC_STATUS_SUCCESS
    check api.StreamSend(
      replayRejectedStream,
      addr sendBuffer,
      1'u32,
      QUIC_SEND_FLAGS(0x0001'u32),
      nil
    ) == QUIC_STATUS_SUCCESS

    check waitForPredicate(proc(): bool = gSessionResumedObserved, rounds = 120)
    check waitForPredicate(proc(): bool =
      not gAcceptedConn.isNil and
        getConnectionSessionResumedForTest(gAcceptedConn, serverResumedState) and
        serverResumedState
    , rounds = 120)
    check waitForPredicate(proc(): bool = gReceivedMessageOk, rounds = 120)
    check not gZeroRttPreHandshakeObserved

    check setBuiltinResumptionZeroRttProfileForTest(
      cstring("127.0.0.1"),
      uint16(41001),
      cstring(TestAlpn),
      0'u64,
      0'u32
    )

    gAcceptedConn = nil
    gAcceptedStream = nil
    gReceivedMessageOk = false
    gSessionResumedObserved = false
    gServerSessionResumedObserved = false
    gZeroRttPreHandshakeObserved = false

    var budgetRejectedConnection: HQUIC
    check api.ConnectionOpen(registration, nil, nil, addr budgetRejectedConnection) == QUIC_STATUS_SUCCESS
    defer:
      api.ConnectionClose(budgetRejectedConnection)
    check api.ConnectionSetConfiguration(budgetRejectedConnection, configuration) == QUIC_STATUS_SUCCESS
    registerConnectionEventHandler(budgetRejectedConnection, proc(ev: ConnectionEvent) =
      if ev.kind == ceConnected and ev.sessionResumed:
        gSessionResumedObserved = true
    )
    check api.ConnectionStart(
      budgetRejectedConnection,
      configuration,
      QUIC_ADDRESS_FAMILY(2),
      cstring("127.0.0.1"),
      uint16(41001)
    ) == QUIC_STATUS_SUCCESS

    var budgetRejectedStream: HQUIC
    check api.StreamOpen(
      budgetRejectedConnection,
      QUIC_STREAM_OPEN_FLAGS(0),
      nil,
      nil,
      addr budgetRejectedStream
    ) == QUIC_STATUS_SUCCESS
    defer:
      api.StreamClose(budgetRejectedStream)
    check api.StreamStart(budgetRejectedStream, QUIC_STREAM_START_FLAGS(0)) == QUIC_STATUS_SUCCESS

    var zeroRttMaxData = high(uint32)
    var zeroRttSentBytes = high(uint64)
    var zeroRttDeadlineUs = high(uint64)
    var clientHelloRequestedEarlyData = true
    check getConnectionZeroRttBudgetForTest(
      budgetRejectedConnection,
      zeroRttMaxData,
      zeroRttSentBytes,
      zeroRttDeadlineUs
    )
    check zeroRttMaxData == 0'u32
    check getConnectionClientHelloRequestsEarlyDataForTest(
      budgetRejectedConnection,
      clientHelloRequestedEarlyData
    )
    check not clientHelloRequestedEarlyData
    check api.StreamSend(
      budgetRejectedStream,
      addr sendBuffer,
      1'u32,
      QUIC_SEND_FLAGS(0x0001'u32),
      nil
    ) == QUIC_STATUS_SUCCESS

    check waitForPredicate(proc(): bool = gSessionResumedObserved, rounds = 120)
    check waitForPredicate(proc(): bool =
      not gAcceptedConn.isNil and
        getConnectionSessionResumedForTest(gAcceptedConn, serverResumedState) and
        serverResumedState
    , rounds = 120)
    check waitForPredicate(proc(): bool = gReceivedMessageOk, rounds = 120)
    check not gZeroRttPreHandshakeObserved
    check getConnectionZeroRttBudgetForTest(
      budgetRejectedConnection,
      zeroRttMaxData,
      zeroRttSentBytes,
      zeroRttDeadlineUs
    )
    check zeroRttSentBytes == 0'u64

    check setBuiltinResumptionZeroRttProfileForTest(
      cstring("127.0.0.1"),
      uint16(41001),
      cstring(TestAlpn),
      uint64((epochTime() * 1_000_000.0) - 120_000_000.0),
      16_384'u32
    )

    gAcceptedConn = nil
    gAcceptedStream = nil
    gReceivedMessageOk = false
    gSessionResumedObserved = false
    gServerSessionResumedObserved = false
    gZeroRttPreHandshakeObserved = false

    var staleWindowConnection: HQUIC
    check api.ConnectionOpen(registration, nil, nil, addr staleWindowConnection) == QUIC_STATUS_SUCCESS
    defer:
      api.ConnectionClose(staleWindowConnection)
    check api.ConnectionSetConfiguration(staleWindowConnection, configuration) == QUIC_STATUS_SUCCESS
    registerConnectionEventHandler(staleWindowConnection, proc(ev: ConnectionEvent) =
      if ev.kind == ceConnected and ev.sessionResumed:
        gSessionResumedObserved = true
    )
    check api.ConnectionStart(
      staleWindowConnection,
      configuration,
      QUIC_ADDRESS_FAMILY(2),
      cstring("127.0.0.1"),
      uint16(41001)
    ) == QUIC_STATUS_SUCCESS
    var staleWindowRequestedEarlyData = true
    check getConnectionClientHelloRequestsEarlyDataForTest(
      staleWindowConnection,
      staleWindowRequestedEarlyData
    )
    check not staleWindowRequestedEarlyData

    var staleWindowStream: HQUIC
    check api.StreamOpen(
      staleWindowConnection,
      QUIC_STREAM_OPEN_FLAGS(0),
      nil,
      nil,
      addr staleWindowStream
    ) == QUIC_STATUS_SUCCESS
    defer:
      api.StreamClose(staleWindowStream)
    check api.StreamStart(staleWindowStream, QUIC_STREAM_START_FLAGS(0)) == QUIC_STATUS_SUCCESS
    check api.StreamSend(
      staleWindowStream,
      addr sendBuffer,
      1'u32,
      QUIC_SEND_FLAGS(0x0001'u32),
      nil
    ) == QUIC_STATUS_SUCCESS

    check waitForPredicate(proc(): bool = gSessionResumedObserved, rounds = 120)
    check waitForPredicate(proc(): bool =
      not gAcceptedConn.isNil and
        getConnectionSessionResumedForTest(gAcceptedConn, serverResumedState) and
        serverResumedState
    , rounds = 120)
    check waitForPredicate(proc(): bool = gReceivedMessageOk, rounds = 120)
    check not gZeroRttPreHandshakeObserved

    gAcceptedConn = nil
    gAcceptedStream = nil
    gReceivedMessageOk = false
    gSessionResumedObserved = false
    gServerSessionResumedObserved = false
    gZeroRttPreHandshakeObserved = false
    setForceRejectBuiltinZeroRttForTest(true)
    defer:
      setForceRejectBuiltinZeroRttForTest(false)

    var rejectedZeroRttConnection: HQUIC
    check api.ConnectionOpen(registration, nil, nil, addr rejectedZeroRttConnection) == QUIC_STATUS_SUCCESS
    defer:
      api.ConnectionClose(rejectedZeroRttConnection)
    check api.ConnectionSetConfiguration(rejectedZeroRttConnection, configuration) == QUIC_STATUS_SUCCESS
    registerConnectionEventHandler(rejectedZeroRttConnection, proc(ev: ConnectionEvent) =
      if ev.kind == ceConnected and ev.sessionResumed:
        gSessionResumedObserved = true
    )
    check api.ConnectionStart(
      rejectedZeroRttConnection,
      configuration,
      QUIC_ADDRESS_FAMILY(2),
      cstring("127.0.0.1"),
      uint16(41001)
    ) == QUIC_STATUS_SUCCESS

    var rejectedZeroRttStream: HQUIC
    check api.StreamOpen(
      rejectedZeroRttConnection,
      QUIC_STREAM_OPEN_FLAGS(0),
      nil,
      nil,
      addr rejectedZeroRttStream
    ) == QUIC_STATUS_SUCCESS
    defer:
      api.StreamClose(rejectedZeroRttStream)
    check api.StreamStart(rejectedZeroRttStream, QUIC_STREAM_START_FLAGS(0)) == QUIC_STATUS_SUCCESS
    check api.StreamSend(
      rejectedZeroRttStream,
      addr sendBuffer,
      1'u32,
      QUIC_SEND_FLAGS(0x0001'u32),
      nil
    ) == QUIC_STATUS_SUCCESS

    check waitForPredicate(proc(): bool = gSessionResumedObserved, rounds = 120)
    check waitForPredicate(proc(): bool =
      not gAcceptedConn.isNil and
        getConnectionSessionResumedForTest(gAcceptedConn, serverResumedState) and
        serverResumedState
    , rounds = 120)
    check waitForPredicate(proc(): bool = gReceivedMessageOk, rounds = 120)
    check not gZeroRttPreHandshakeObserved

    block:
      setDropBuiltinEarlyDataRequestForTest(true)
      defer:
        setDropBuiltinEarlyDataRequestForTest(false)

      gAcceptedConn = nil
      gAcceptedStream = nil
      gReceivedMessageOk = false
      gSessionResumedObserved = false
      gServerSessionResumedObserved = false
      gExpectZeroRttWindow = false
      gZeroRttPreHandshakeObserved = false

      var noEarlyDataConnection: HQUIC
      check api.ConnectionOpen(registration, nil, nil, addr noEarlyDataConnection) == QUIC_STATUS_SUCCESS
      defer:
        api.ConnectionClose(noEarlyDataConnection)
      check api.ConnectionSetConfiguration(noEarlyDataConnection, configuration) == QUIC_STATUS_SUCCESS
      registerConnectionEventHandler(noEarlyDataConnection, proc(ev: ConnectionEvent) =
        if ev.kind == ceConnected and ev.sessionResumed:
          gSessionResumedObserved = true
      )
      check api.ConnectionStart(
        noEarlyDataConnection,
        configuration,
        QUIC_ADDRESS_FAMILY(2),
        cstring("127.0.0.1"),
        uint16(41001)
      ) == QUIC_STATUS_SUCCESS

      var noEarlyDataStream: HQUIC
      check api.StreamOpen(
        noEarlyDataConnection,
        QUIC_STREAM_OPEN_FLAGS(0),
        nil,
        nil,
        addr noEarlyDataStream
      ) == QUIC_STATUS_SUCCESS
      defer:
        api.StreamClose(noEarlyDataStream)
      check api.StreamStart(noEarlyDataStream, QUIC_STREAM_START_FLAGS(0)) == QUIC_STATUS_SUCCESS
      check api.StreamSend(
        noEarlyDataStream,
        addr sendBuffer,
        1'u32,
        QUIC_SEND_FLAGS(0x0001'u32),
        nil
      ) == QUIC_STATUS_SUCCESS

      check waitForPredicate(proc(): bool = gSessionResumedObserved, rounds = 120)
      check waitForPredicate(proc(): bool =
        not gAcceptedConn.isNil and
          getConnectionSessionResumedForTest(gAcceptedConn, serverResumedState) and
          serverResumedState
      , rounds = 120)
      check waitForPredicate(proc(): bool = gReceivedMessageOk, rounds = 120)
      check not gZeroRttPreHandshakeObserved

    block:
      setDropBuiltinTicketAgeForTest(true)
      defer:
        setDropBuiltinTicketAgeForTest(false)

      gAcceptedConn = nil
      gAcceptedStream = nil
      gReceivedMessageOk = false
      gSessionResumedObserved = false
      gServerSessionResumedObserved = false
      gExpectZeroRttWindow = false
      gZeroRttPreHandshakeObserved = false
      gColdStartConnected = false
      gColdStartResumed = true
      gClientFinishedHandshakeVerifyFailed = false

      var droppedTicketAgeConnection: HQUIC
      check api.ConnectionOpen(registration, nil, nil, addr droppedTicketAgeConnection) == QUIC_STATUS_SUCCESS
      defer:
        api.ConnectionClose(droppedTicketAgeConnection)
      check api.ConnectionSetConfiguration(droppedTicketAgeConnection, configuration) == QUIC_STATUS_SUCCESS
      registerConnectionEventHandler(droppedTicketAgeConnection, proc(ev: ConnectionEvent) =
        if ev.kind == ceConnected:
          gColdStartConnected = true
          gColdStartResumed = ev.sessionResumed
      )
      check api.ConnectionStart(
        droppedTicketAgeConnection,
        configuration,
        QUIC_ADDRESS_FAMILY(2),
        cstring("127.0.0.1"),
        uint16(41001)
      ) == QUIC_STATUS_SUCCESS

      var droppedTicketAgeStream: HQUIC
      check api.StreamOpen(
        droppedTicketAgeConnection,
        QUIC_STREAM_OPEN_FLAGS(0),
        nil,
        nil,
        addr droppedTicketAgeStream
      ) == QUIC_STATUS_SUCCESS
      defer:
        api.StreamClose(droppedTicketAgeStream)
      check api.StreamStart(droppedTicketAgeStream, QUIC_STREAM_START_FLAGS(0)) == QUIC_STATUS_SUCCESS
      check api.StreamSend(
        droppedTicketAgeStream,
        addr sendBuffer,
        1'u32,
        QUIC_SEND_FLAGS(0x0001'u32),
        nil
      ) == QUIC_STATUS_SUCCESS

      check waitForPredicate(proc(): bool = gColdStartConnected, rounds = 120)
      check not gColdStartResumed
      check waitForPredicate(proc(): bool = gReceivedMessageOk, rounds = 120)
      check not gClientFinishedHandshakeVerifyFailed
      check not gServerSessionResumedObserved
      check not gAcceptedConn.isNil
      check getConnectionSessionResumedForTest(gAcceptedConn, serverResumedState)
      check not serverResumedState

    block:
      setDropBuiltinResumptionBinderForTest(true)
      defer:
        setDropBuiltinResumptionBinderForTest(false)

      gAcceptedConn = nil
      gAcceptedStream = nil
      gReceivedMessageOk = false
      gSessionResumedObserved = false
      gServerSessionResumedObserved = false
      gExpectZeroRttWindow = true
      gZeroRttPreHandshakeObserved = false
      gColdStartConnected = false
      gColdStartResumed = false
      gClientFinishedHandshakeVerifyFailed = false

      var droppedBinderConnection: HQUIC
      check api.ConnectionOpen(registration, nil, nil, addr droppedBinderConnection) == QUIC_STATUS_SUCCESS
      defer:
        api.ConnectionClose(droppedBinderConnection)
      check api.ConnectionSetConfiguration(droppedBinderConnection, configuration) == QUIC_STATUS_SUCCESS
      registerConnectionEventHandler(droppedBinderConnection, proc(ev: ConnectionEvent) =
        if ev.kind == ceConnected:
          gColdStartConnected = true
          gColdStartResumed = ev.sessionResumed
      )
      check api.ConnectionStart(
        droppedBinderConnection,
        configuration,
        QUIC_ADDRESS_FAMILY(2),
        cstring("127.0.0.1"),
        uint16(41001)
      ) == QUIC_STATUS_SUCCESS

      var droppedBinderStream: HQUIC
      check api.StreamOpen(
        droppedBinderConnection,
        QUIC_STREAM_OPEN_FLAGS(0),
        nil,
        nil,
        addr droppedBinderStream
      ) == QUIC_STATUS_SUCCESS
      defer:
        api.StreamClose(droppedBinderStream)
      check api.StreamStart(droppedBinderStream, QUIC_STREAM_START_FLAGS(0)) == QUIC_STATUS_SUCCESS
      check api.StreamSend(
        droppedBinderStream,
        addr sendBuffer,
        1'u32,
        QUIC_SEND_FLAGS(0x0001'u32),
        nil
      ) == QUIC_STATUS_SUCCESS

      check waitForPredicate(proc(): bool = gColdStartConnected, rounds = 120)
      check not gColdStartResumed
      check waitForPredicate(proc(): bool = gReceivedMessageOk, rounds = 120)
      check not gClientFinishedHandshakeVerifyFailed
      check not gServerSessionResumedObserved
      check not gAcceptedConn.isNil
      check getConnectionSessionResumedForTest(gAcceptedConn, serverResumedState)
      check not serverResumedState

    block:
      setTamperBuiltinResumptionBinderForTest(true)
      defer:
        setTamperBuiltinResumptionBinderForTest(false)

      gAcceptedConn = nil
      gAcceptedStream = nil
      gReceivedMessageOk = false
      gSessionResumedObserved = false
      gServerSessionResumedObserved = false
      gExpectZeroRttWindow = false
      gZeroRttPreHandshakeObserved = false
      gColdStartConnected = false
      gColdStartResumed = true
      gClientFinishedHandshakeVerifyFailed = false

      var tamperedBinderConnection: HQUIC
      check api.ConnectionOpen(registration, nil, nil, addr tamperedBinderConnection) == QUIC_STATUS_SUCCESS
      defer:
        api.ConnectionClose(tamperedBinderConnection)
      check api.ConnectionSetConfiguration(tamperedBinderConnection, configuration) == QUIC_STATUS_SUCCESS
      registerConnectionEventHandler(tamperedBinderConnection, proc(ev: ConnectionEvent) =
        if ev.kind == ceConnected:
          gColdStartConnected = true
          gColdStartResumed = ev.sessionResumed
      )
      check api.ConnectionStart(
        tamperedBinderConnection,
        configuration,
        QUIC_ADDRESS_FAMILY(2),
        cstring("127.0.0.1"),
        uint16(41001)
      ) == QUIC_STATUS_SUCCESS

      var tamperedBinderStream: HQUIC
      check api.StreamOpen(
        tamperedBinderConnection,
        QUIC_STREAM_OPEN_FLAGS(0),
        nil,
        nil,
        addr tamperedBinderStream
      ) == QUIC_STATUS_SUCCESS
      defer:
        api.StreamClose(tamperedBinderStream)
      check api.StreamStart(tamperedBinderStream, QUIC_STREAM_START_FLAGS(0)) == QUIC_STATUS_SUCCESS
      check api.StreamSend(
        tamperedBinderStream,
        addr sendBuffer,
        1'u32,
        QUIC_SEND_FLAGS(0x0001'u32),
        nil
      ) == QUIC_STATUS_SUCCESS

      check waitForPredicate(proc(): bool = gColdStartConnected, rounds = 120)
      check not gColdStartResumed
      check waitForPredicate(proc(): bool = gReceivedMessageOk, rounds = 120)
      check not gClientFinishedHandshakeVerifyFailed
      check not gServerSessionResumedObserved
      check not gAcceptedConn.isNil
      check getConnectionSessionResumedForTest(gAcceptedConn, serverResumedState)
      check not serverResumedState

    block:
      setTamperBuiltinResumptionTicketForTest(true)
      defer:
        setTamperBuiltinResumptionTicketForTest(false)

      gAcceptedConn = nil
      gAcceptedStream = nil
      gReceivedMessageOk = false
      gSessionResumedObserved = false
      gServerSessionResumedObserved = false
      gExpectZeroRttWindow = false
      gZeroRttPreHandshakeObserved = false
      gColdStartConnected = false
      gColdStartResumed = true
      gClientFinishedHandshakeVerifyFailed = false

      var tamperedTicketConnection: HQUIC
      check api.ConnectionOpen(registration, nil, nil, addr tamperedTicketConnection) == QUIC_STATUS_SUCCESS
      defer:
        api.ConnectionClose(tamperedTicketConnection)
      check api.ConnectionSetConfiguration(tamperedTicketConnection, configuration) == QUIC_STATUS_SUCCESS
      registerConnectionEventHandler(tamperedTicketConnection, proc(ev: ConnectionEvent) =
        if ev.kind == ceConnected:
          gColdStartConnected = true
          gColdStartResumed = ev.sessionResumed
      )
      check api.ConnectionStart(
        tamperedTicketConnection,
        configuration,
        QUIC_ADDRESS_FAMILY(2),
        cstring("127.0.0.1"),
        uint16(41001)
      ) == QUIC_STATUS_SUCCESS

      var tamperedTicketStream: HQUIC
      check api.StreamOpen(
        tamperedTicketConnection,
        QUIC_STREAM_OPEN_FLAGS(0),
        nil,
        nil,
        addr tamperedTicketStream
      ) == QUIC_STATUS_SUCCESS
      defer:
        api.StreamClose(tamperedTicketStream)
      check api.StreamStart(tamperedTicketStream, QUIC_STREAM_START_FLAGS(0)) == QUIC_STATUS_SUCCESS
      check api.StreamSend(
        tamperedTicketStream,
        addr sendBuffer,
        1'u32,
        QUIC_SEND_FLAGS(0x0001'u32),
        nil
      ) == QUIC_STATUS_SUCCESS

      check waitForPredicate(proc(): bool = gColdStartConnected, rounds = 120)
      check not gColdStartResumed
      check waitForPredicate(proc(): bool = gReceivedMessageOk, rounds = 120)
      check not gClientFinishedHandshakeVerifyFailed
      check not gServerSessionResumedObserved
      check not gAcceptedConn.isNil
      check getConnectionSessionResumedForTest(gAcceptedConn, serverResumedState)
      check not serverResumedState

    block:
      setOverrideBuiltinTicketServerNameForTest(cstring("example.invalid"))
      defer:
        clearOverrideBuiltinTicketServerNameForTest()

      gAcceptedConn = nil
      gAcceptedStream = nil
      gReceivedMessageOk = false
      gSessionResumedObserved = false
      gServerSessionResumedObserved = false
      gExpectZeroRttWindow = false
      gZeroRttPreHandshakeObserved = false
      gColdStartConnected = false
      gColdStartResumed = true
      gClientFinishedHandshakeVerifyFailed = false

      var wrongServerNameConnection: HQUIC
      check api.ConnectionOpen(registration, nil, nil, addr wrongServerNameConnection) == QUIC_STATUS_SUCCESS
      defer:
        api.ConnectionClose(wrongServerNameConnection)
      check api.ConnectionSetConfiguration(wrongServerNameConnection, configuration) == QUIC_STATUS_SUCCESS
      registerConnectionEventHandler(wrongServerNameConnection, proc(ev: ConnectionEvent) =
        if ev.kind == ceConnected:
          gColdStartConnected = true
          gColdStartResumed = ev.sessionResumed
      )
      check api.ConnectionStart(
        wrongServerNameConnection,
        configuration,
        QUIC_ADDRESS_FAMILY(2),
        cstring("127.0.0.1"),
        uint16(41001)
      ) == QUIC_STATUS_SUCCESS

      var wrongServerNameStream: HQUIC
      check api.StreamOpen(
        wrongServerNameConnection,
        QUIC_STREAM_OPEN_FLAGS(0),
        nil,
        nil,
        addr wrongServerNameStream
      ) == QUIC_STATUS_SUCCESS
      defer:
        api.StreamClose(wrongServerNameStream)
      check api.StreamStart(wrongServerNameStream, QUIC_STREAM_START_FLAGS(0)) == QUIC_STATUS_SUCCESS
      check api.StreamSend(
        wrongServerNameStream,
        addr sendBuffer,
        1'u32,
        QUIC_SEND_FLAGS(0x0001'u32),
        nil
      ) == QUIC_STATUS_SUCCESS

      check waitForPredicate(proc(): bool = gColdStartConnected, rounds = 120)
      check gColdStartResumed
      check waitForPredicate(proc(): bool = gReceivedMessageOk, rounds = 120)
      check not gClientFinishedHandshakeVerifyFailed
      check waitForPredicate(proc(): bool = gServerSessionResumedObserved, rounds = 120)
      check not gAcceptedConn.isNil
      check getConnectionSessionResumedForTest(gAcceptedConn, serverResumedState)
      check serverResumedState

    block:
      setOverrideBuiltinTicketServerPortForTest(uint16(41002))
      defer:
        clearOverrideBuiltinTicketServerPortForTest()

      gAcceptedConn = nil
      gAcceptedStream = nil
      gReceivedMessageOk = false
      gSessionResumedObserved = false
      gServerSessionResumedObserved = false
      gExpectZeroRttWindow = false
      gZeroRttPreHandshakeObserved = false
      gColdStartConnected = false
      gColdStartResumed = true
      gClientFinishedHandshakeVerifyFailed = false

      var wrongServerPortConnection: HQUIC
      check api.ConnectionOpen(registration, nil, nil, addr wrongServerPortConnection) == QUIC_STATUS_SUCCESS
      defer:
        api.ConnectionClose(wrongServerPortConnection)
      check api.ConnectionSetConfiguration(wrongServerPortConnection, configuration) == QUIC_STATUS_SUCCESS
      registerConnectionEventHandler(wrongServerPortConnection, proc(ev: ConnectionEvent) =
        if ev.kind == ceConnected:
          gColdStartConnected = true
          gColdStartResumed = ev.sessionResumed
      )
      check api.ConnectionStart(
        wrongServerPortConnection,
        configuration,
        QUIC_ADDRESS_FAMILY(2),
        cstring("127.0.0.1"),
        uint16(41001)
      ) == QUIC_STATUS_SUCCESS

      var wrongServerPortStream: HQUIC
      check api.StreamOpen(
        wrongServerPortConnection,
        QUIC_STREAM_OPEN_FLAGS(0),
        nil,
        nil,
        addr wrongServerPortStream
      ) == QUIC_STATUS_SUCCESS
      defer:
        api.StreamClose(wrongServerPortStream)
      check api.StreamStart(wrongServerPortStream, QUIC_STREAM_START_FLAGS(0)) == QUIC_STATUS_SUCCESS
      check api.StreamSend(
        wrongServerPortStream,
        addr sendBuffer,
        1'u32,
        QUIC_SEND_FLAGS(0),
        nil
      ) == QUIC_STATUS_SUCCESS

      check waitForPredicate(proc(): bool = gColdStartConnected, rounds = 120)
      check not gColdStartResumed
      check waitForPredicate(proc(): bool = gReceivedMessageOk, rounds = 120)
      check not gZeroRttPreHandshakeObserved
      check not gClientFinishedHandshakeVerifyFailed
      check not gServerSessionResumedObserved
      check not gAcceptedConn.isNil
      check getConnectionSessionResumedForTest(gAcceptedConn, serverResumedState)
      check not serverResumedState

    block:
      setOverrideBuiltinTicketAgeDeltaMsForTest(300_000'i64)
      defer:
        clearOverrideBuiltinTicketAgeDeltaMsForTest()

      gAcceptedConn = nil
      gAcceptedStream = nil
      gReceivedMessageOk = false
      gSessionResumedObserved = false
      gServerSessionResumedObserved = false
      gExpectZeroRttWindow = false
      gZeroRttPreHandshakeObserved = false
      gColdStartConnected = false
      gColdStartResumed = true
      gClientFinishedHandshakeVerifyFailed = false

      var wrongTicketAgeConnection: HQUIC
      check api.ConnectionOpen(registration, nil, nil, addr wrongTicketAgeConnection) == QUIC_STATUS_SUCCESS
      defer:
        api.ConnectionClose(wrongTicketAgeConnection)
      check api.ConnectionSetConfiguration(wrongTicketAgeConnection, configuration) == QUIC_STATUS_SUCCESS
      registerConnectionEventHandler(wrongTicketAgeConnection, proc(ev: ConnectionEvent) =
        if ev.kind == ceConnected:
          gColdStartConnected = true
          gColdStartResumed = ev.sessionResumed
      )
      check api.ConnectionStart(
        wrongTicketAgeConnection,
        configuration,
        QUIC_ADDRESS_FAMILY(2),
        cstring("127.0.0.1"),
        uint16(41001)
      ) == QUIC_STATUS_SUCCESS

      var wrongTicketAgeStream: HQUIC
      check api.StreamOpen(
        wrongTicketAgeConnection,
        QUIC_STREAM_OPEN_FLAGS(0),
        nil,
        nil,
        addr wrongTicketAgeStream
      ) == QUIC_STATUS_SUCCESS
      defer:
        api.StreamClose(wrongTicketAgeStream)
      check api.StreamStart(wrongTicketAgeStream, QUIC_STREAM_START_FLAGS(0)) == QUIC_STATUS_SUCCESS
      check api.StreamSend(
        wrongTicketAgeStream,
        addr sendBuffer,
        1'u32,
        QUIC_SEND_FLAGS(0),
        nil
      ) == QUIC_STATUS_SUCCESS

      check waitForPredicate(proc(): bool = gColdStartConnected, rounds = 120)
      check not gColdStartResumed
      check waitForPredicate(proc(): bool = gReceivedMessageOk, rounds = 120)
      check not gZeroRttPreHandshakeObserved
      check not gClientFinishedHandshakeVerifyFailed
      check not gServerSessionResumedObserved
      check not gAcceptedConn.isNil
      check getConnectionSessionResumedForTest(gAcceptedConn, serverResumedState)
      check not serverResumedState

    block:
      setDropBuiltinTicketAgeForTest(true)
      defer:
        setDropBuiltinTicketAgeForTest(false)

      gAcceptedConn = nil
      gAcceptedStream = nil
      gReceivedMessageOk = false
      gSessionResumedObserved = false
      gServerSessionResumedObserved = false
      gExpectZeroRttWindow = false
      gZeroRttPreHandshakeObserved = false
      gColdStartConnected = false
      gColdStartResumed = true
      gClientFinishedHandshakeVerifyFailed = false

      var missingTicketAgeConnection: HQUIC
      check api.ConnectionOpen(registration, nil, nil, addr missingTicketAgeConnection) == QUIC_STATUS_SUCCESS
      defer:
        api.ConnectionClose(missingTicketAgeConnection)
      check api.ConnectionSetConfiguration(missingTicketAgeConnection, configuration) == QUIC_STATUS_SUCCESS
      registerConnectionEventHandler(missingTicketAgeConnection, proc(ev: ConnectionEvent) =
        if ev.kind == ceConnected:
          gColdStartConnected = true
          gColdStartResumed = ev.sessionResumed
      )
      check api.ConnectionStart(
        missingTicketAgeConnection,
        configuration,
        QUIC_ADDRESS_FAMILY(2),
        cstring("127.0.0.1"),
        uint16(41001)
      ) == QUIC_STATUS_SUCCESS

      var missingTicketAgeStream: HQUIC
      check api.StreamOpen(
        missingTicketAgeConnection,
        QUIC_STREAM_OPEN_FLAGS(0),
        nil,
        nil,
        addr missingTicketAgeStream
      ) == QUIC_STATUS_SUCCESS
      defer:
        api.StreamClose(missingTicketAgeStream)
      check api.StreamStart(missingTicketAgeStream, QUIC_STREAM_START_FLAGS(0)) == QUIC_STATUS_SUCCESS
      check api.StreamSend(
        missingTicketAgeStream,
        addr sendBuffer,
        1'u32,
        QUIC_SEND_FLAGS(0),
        nil
      ) == QUIC_STATUS_SUCCESS

      check waitForPredicate(proc(): bool = gColdStartConnected, rounds = 120)
      check not gColdStartResumed
      check waitForPredicate(proc(): bool = gReceivedMessageOk, rounds = 120)
      check not gZeroRttPreHandshakeObserved
      check not gClientFinishedHandshakeVerifyFailed
      check not gServerSessionResumedObserved
      check not gAcceptedConn.isNil
      check getConnectionSessionResumedForTest(gAcceptedConn, serverResumedState)
      check not serverResumedState

    block:
      setOverrideBuiltinTicketAlpnForTest(cstring("wrong-alpn"))
      defer:
        clearOverrideBuiltinTicketAlpnForTest()

      gAcceptedConn = nil
      gAcceptedStream = nil
      gReceivedMessageOk = false
      gSessionResumedObserved = false
      gServerSessionResumedObserved = false
      gExpectZeroRttWindow = false
      gZeroRttPreHandshakeObserved = false
      gColdStartConnected = false
      gColdStartResumed = true
      gClientFinishedHandshakeVerifyFailed = false

      var wrongAlpnConnection: HQUIC
      check api.ConnectionOpen(registration, nil, nil, addr wrongAlpnConnection) == QUIC_STATUS_SUCCESS
      defer:
        api.ConnectionClose(wrongAlpnConnection)
      check api.ConnectionSetConfiguration(wrongAlpnConnection, configuration) == QUIC_STATUS_SUCCESS
      registerConnectionEventHandler(wrongAlpnConnection, proc(ev: ConnectionEvent) =
        if ev.kind == ceConnected:
          gColdStartConnected = true
          gColdStartResumed = ev.sessionResumed
      )
      check api.ConnectionStart(
        wrongAlpnConnection,
        configuration,
        QUIC_ADDRESS_FAMILY(2),
        cstring("127.0.0.1"),
        uint16(41001)
      ) == QUIC_STATUS_SUCCESS

      var wrongAlpnStream: HQUIC
      check api.StreamOpen(
        wrongAlpnConnection,
        QUIC_STREAM_OPEN_FLAGS(0),
        nil,
        nil,
        addr wrongAlpnStream
      ) == QUIC_STATUS_SUCCESS
      defer:
        api.StreamClose(wrongAlpnStream)
      check api.StreamStart(wrongAlpnStream, QUIC_STREAM_START_FLAGS(0)) == QUIC_STATUS_SUCCESS
      check api.StreamSend(
        wrongAlpnStream,
        addr sendBuffer,
        1'u32,
        QUIC_SEND_FLAGS(0),
        nil
      ) == QUIC_STATUS_SUCCESS

      check waitForPredicate(proc(): bool = gColdStartConnected, rounds = 120)
      check not gColdStartResumed
      check waitForPredicate(proc(): bool = gReceivedMessageOk, rounds = 120)
      check not gZeroRttPreHandshakeObserved
      check not gClientFinishedHandshakeVerifyFailed
      check not gServerSessionResumedObserved
      check not gAcceptedConn.isNil
      check getConnectionSessionResumedForTest(gAcceptedConn, serverResumedState)
      check not serverResumedState

    block:
      setOverrideBuiltinTicketServerPortForTest(uint16(0))
      defer:
        clearOverrideBuiltinTicketServerPortForTest()

      gAcceptedConn = nil
      gAcceptedStream = nil
      gReceivedMessageOk = false
      gSessionResumedObserved = false
      gServerSessionResumedObserved = false
      gExpectZeroRttWindow = true
      gZeroRttPreHandshakeObserved = false
      gColdStartConnected = false
      gColdStartResumed = false
      gClientFinishedHandshakeVerifyFailed = false

      var wildcardServerPortConnection: HQUIC
      check api.ConnectionOpen(registration, nil, nil, addr wildcardServerPortConnection) == QUIC_STATUS_SUCCESS
      defer:
        api.ConnectionClose(wildcardServerPortConnection)
      check api.ConnectionSetConfiguration(wildcardServerPortConnection, configuration) == QUIC_STATUS_SUCCESS
      registerConnectionEventHandler(wildcardServerPortConnection, proc(ev: ConnectionEvent) =
        if ev.kind == ceConnected:
          gColdStartConnected = true
          gColdStartResumed = ev.sessionResumed
      )
      check api.ConnectionStart(
        wildcardServerPortConnection,
        configuration,
        QUIC_ADDRESS_FAMILY(2),
        cstring("127.0.0.1"),
        uint16(41001)
      ) == QUIC_STATUS_SUCCESS

      var wildcardServerPortStream: HQUIC
      check api.StreamOpen(
        wildcardServerPortConnection,
        QUIC_STREAM_OPEN_FLAGS(0),
        nil,
        nil,
        addr wildcardServerPortStream
      ) == QUIC_STATUS_SUCCESS
      defer:
        api.StreamClose(wildcardServerPortStream)
      check api.StreamStart(wildcardServerPortStream, QUIC_STREAM_START_FLAGS(0)) == QUIC_STATUS_SUCCESS
      check api.StreamSend(
        wildcardServerPortStream,
        addr sendBuffer,
        1'u32,
        QUIC_SEND_FLAGS(0x0001'u32),
        nil
      ) == QUIC_STATUS_SUCCESS

      check waitForPredicate(proc(): bool = gColdStartConnected, rounds = 120)
      check gColdStartResumed
      check waitForPredicate(proc(): bool = gReceivedMessageOk, rounds = 120)
      check not gClientFinishedHandshakeVerifyFailed
      check waitForPredicate(proc(): bool = gServerSessionResumedObserved, rounds = 120)
      check not gAcceptedConn.isNil
      check getConnectionSessionResumedForTest(gAcceptedConn, serverResumedState)
      check serverResumedState

    block:
      setOverrideBuiltinTicketAlpnForTest(cstring(""))
      defer:
        clearOverrideBuiltinTicketAlpnForTest()

      gAcceptedConn = nil
      gAcceptedStream = nil
      gReceivedMessageOk = false
      gSessionResumedObserved = false
      gServerSessionResumedObserved = false
      gExpectZeroRttWindow = true
      gZeroRttPreHandshakeObserved = false
      gColdStartConnected = false
      gColdStartResumed = false
      gClientFinishedHandshakeVerifyFailed = false

      var wildcardAlpnConnection: HQUIC
      check api.ConnectionOpen(registration, nil, nil, addr wildcardAlpnConnection) == QUIC_STATUS_SUCCESS
      defer:
        api.ConnectionClose(wildcardAlpnConnection)
      check api.ConnectionSetConfiguration(wildcardAlpnConnection, configuration) == QUIC_STATUS_SUCCESS
      registerConnectionEventHandler(wildcardAlpnConnection, proc(ev: ConnectionEvent) =
        if ev.kind == ceConnected:
          gColdStartConnected = true
          gColdStartResumed = ev.sessionResumed
      )
      check api.ConnectionStart(
        wildcardAlpnConnection,
        configuration,
        QUIC_ADDRESS_FAMILY(2),
        cstring("127.0.0.1"),
        uint16(41001)
      ) == QUIC_STATUS_SUCCESS

      var wildcardAlpnStream: HQUIC
      check api.StreamOpen(
        wildcardAlpnConnection,
        QUIC_STREAM_OPEN_FLAGS(0),
        nil,
        nil,
        addr wildcardAlpnStream
      ) == QUIC_STATUS_SUCCESS
      defer:
        api.StreamClose(wildcardAlpnStream)
      check api.StreamStart(wildcardAlpnStream, QUIC_STREAM_START_FLAGS(0)) == QUIC_STATUS_SUCCESS
      check api.StreamSend(
        wildcardAlpnStream,
        addr sendBuffer,
        1'u32,
        QUIC_SEND_FLAGS(0x0001'u32),
        nil
      ) == QUIC_STATUS_SUCCESS

      check waitForPredicate(proc(): bool = gColdStartConnected, rounds = 120)
      check gColdStartResumed
      check waitForPredicate(proc(): bool = gReceivedMessageOk, rounds = 120)
      check not gClientFinishedHandshakeVerifyFailed
      check waitForPredicate(proc(): bool = gServerSessionResumedObserved, rounds = 120)
      check not gAcceptedConn.isNil
      check getConnectionSessionResumedForTest(gAcceptedConn, serverResumedState)
      check serverResumedState

    check setBuiltinResumptionZeroRttProfileForTest(
      cstring("127.0.0.1"),
      uint16(41001),
      cstring(TestAlpn),
      uint64((epochTime() * 1_000_000.0) + 120_000_000.0),
      16_384'u32
    )

    gAcceptedConn = nil
    gAcceptedStream = nil
    gColdStartConnected = false
    gColdStartResumed = true

    var futureIssuedConnection: HQUIC
    check api.ConnectionOpen(registration, nil, nil, addr futureIssuedConnection) == QUIC_STATUS_SUCCESS
    defer:
      api.ConnectionClose(futureIssuedConnection)
    check api.ConnectionSetConfiguration(futureIssuedConnection, configuration) == QUIC_STATUS_SUCCESS
    registerConnectionEventHandler(futureIssuedConnection, proc(ev: ConnectionEvent) =
      if ev.kind == ceConnected:
        gColdStartConnected = true
        gColdStartResumed = ev.sessionResumed
    )
    check api.ConnectionStart(
      futureIssuedConnection,
      configuration,
      QUIC_ADDRESS_FAMILY(2),
      cstring("127.0.0.1"),
      uint16(41001)
    ) == QUIC_STATUS_SUCCESS
    var futureIssuedRequestedEarlyData = true
    check getConnectionClientHelloRequestsEarlyDataForTest(
      futureIssuedConnection,
      futureIssuedRequestedEarlyData
    )
    check not futureIssuedRequestedEarlyData

    check waitForPredicate(proc(): bool =
      gColdStartConnected
    , rounds = 120)
    check not gColdStartResumed

    check expireBuiltinResumptionEntryForTest(cstring("127.0.0.1"), uint16(41001), cstring(TestAlpn))

    gAcceptedConn = nil
    gAcceptedStream = nil
    var coldConnection: HQUIC
    check api.ConnectionOpen(registration, nil, nil, addr coldConnection) == QUIC_STATUS_SUCCESS
    defer:
      api.ConnectionClose(coldConnection)
    check api.ConnectionSetConfiguration(coldConnection, configuration) == QUIC_STATUS_SUCCESS
    registerConnectionEventHandler(coldConnection, proc(ev: ConnectionEvent) =
      if ev.kind == ceConnected:
        gColdStartConnected = true
        gColdStartResumed = ev.sessionResumed
    )
    check api.ConnectionStart(
      coldConnection,
      configuration,
      QUIC_ADDRESS_FAMILY(2),
      cstring("127.0.0.1"),
      uint16(41001)
    ) == QUIC_STATUS_SUCCESS

    check waitForPredicate(proc(): bool =
      gColdStartConnected
    , rounds = 120)
    check not gColdStartResumed

    gReceivedMessageOk = false
    var coldStream: HQUIC
    check api.StreamOpen(coldConnection, QUIC_STREAM_OPEN_FLAGS(0), nil, nil, addr coldStream) ==
      QUIC_STATUS_SUCCESS
    defer:
      api.StreamClose(coldStream)
    check api.StreamStart(coldStream, QUIC_STREAM_START_FLAGS(0)) == QUIC_STATUS_SUCCESS
    check api.StreamSend(coldStream, addr sendBuffer, 1'u32, QUIC_SEND_FLAGS(0), nil) == QUIC_STATUS_SUCCESS
    check waitForPredicate(proc(): bool =
      gReceivedMessageOk
    , rounds = 120)

    block:
      setOverrideBuiltinNewSessionTicketLifetimeSecForTest(0'u32)
      defer:
        clearOverrideBuiltinNewSessionTicketLifetimeSecForTest()

      gAcceptedConn = nil
      gAcceptedStream = nil
      gReceivedMessageOk = false
      gSessionTicketReceived = false
      gColdStartConnected = false
      gColdStartResumed = true

      var shortLifetimeTicketConnection: HQUIC
      check api.ConnectionOpen(registration, nil, nil, addr shortLifetimeTicketConnection) == QUIC_STATUS_SUCCESS
      defer:
        api.ConnectionClose(shortLifetimeTicketConnection)
      check api.ConnectionSetConfiguration(shortLifetimeTicketConnection, configuration) == QUIC_STATUS_SUCCESS
      registerConnectionEventHandler(shortLifetimeTicketConnection, proc(ev: ConnectionEvent) =
        if ev.kind == ceConnected:
          gColdStartConnected = true
          gColdStartResumed = ev.sessionResumed
      )
      check api.ConnectionStart(
        shortLifetimeTicketConnection,
        configuration,
        QUIC_ADDRESS_FAMILY(2),
        cstring("127.0.0.1"),
        uint16(41001)
      ) == QUIC_STATUS_SUCCESS
      check waitForPredicate(proc(): bool = gColdStartConnected, rounds = 120)

      var shortLifetimeTicketStream: HQUIC
      check api.StreamOpen(
        shortLifetimeTicketConnection,
        QUIC_STREAM_OPEN_FLAGS(0),
        nil,
        nil,
        addr shortLifetimeTicketStream
      ) == QUIC_STATUS_SUCCESS
      defer:
        api.StreamClose(shortLifetimeTicketStream)
      check api.StreamStart(shortLifetimeTicketStream, QUIC_STREAM_START_FLAGS(0)) == QUIC_STATUS_SUCCESS
      check api.StreamSend(
        shortLifetimeTicketStream,
        addr sendBuffer,
        1'u32,
        QUIC_SEND_FLAGS(0x0001'u32),
        nil
      ) == QUIC_STATUS_SUCCESS

      check waitForPredicate(proc(): bool = gReceivedMessageOk, rounds = 120)
      check waitForPredicate(proc(): bool = gSessionTicketReceived, rounds = 120)
      check waitForPredicate(proc(): bool =
        var issuedAtUs = 0'u64
        var expiresAtUs = 0'u64
        var maxEarlyData = 0'u32
        getConnectionResumptionTicketMetaForTest(
          shortLifetimeTicketConnection,
          issuedAtUs,
          expiresAtUs,
          maxEarlyData
        ) and expiresAtUs > 0'u64 and expiresAtUs <= issuedAtUs
      , rounds = 120)

      gAcceptedConn = nil
      gAcceptedStream = nil
      gReceivedMessageOk = false
      gColdStartConnected = false
      gColdStartResumed = true

      var expiredLifetimeTicketConnection: HQUIC
      check api.ConnectionOpen(registration, nil, nil, addr expiredLifetimeTicketConnection) == QUIC_STATUS_SUCCESS
      defer:
        api.ConnectionClose(expiredLifetimeTicketConnection)
      check api.ConnectionSetConfiguration(expiredLifetimeTicketConnection, configuration) == QUIC_STATUS_SUCCESS
      registerConnectionEventHandler(expiredLifetimeTicketConnection, proc(ev: ConnectionEvent) =
        if ev.kind == ceConnected:
          gColdStartConnected = true
          gColdStartResumed = ev.sessionResumed
      )
      check api.ConnectionStart(
        expiredLifetimeTicketConnection,
        configuration,
        QUIC_ADDRESS_FAMILY(2),
        cstring("127.0.0.1"),
        uint16(41001)
      ) == QUIC_STATUS_SUCCESS

      check waitForPredicate(proc(): bool = gColdStartConnected, rounds = 120)
      check not gColdStartResumed

      var expiredLifetimeRequestedEarlyData = true
      check getConnectionClientHelloRequestsEarlyDataForTest(
        expiredLifetimeTicketConnection,
        expiredLifetimeRequestedEarlyData
      )
      check not expiredLifetimeRequestedEarlyData

      var expiredLifetimeTicketStream: HQUIC
      check api.StreamOpen(
        expiredLifetimeTicketConnection,
        QUIC_STREAM_OPEN_FLAGS(0),
        nil,
        nil,
        addr expiredLifetimeTicketStream
      ) == QUIC_STATUS_SUCCESS
      defer:
        api.StreamClose(expiredLifetimeTicketStream)
      check api.StreamStart(expiredLifetimeTicketStream, QUIC_STREAM_START_FLAGS(0)) == QUIC_STATUS_SUCCESS
      check api.StreamSend(
        expiredLifetimeTicketStream,
        addr sendBuffer,
        1'u32,
        QUIC_SEND_FLAGS(0x0001'u32),
        nil
      ) == QUIC_STATUS_SUCCESS
      check waitForPredicate(proc(): bool = gReceivedMessageOk, rounds = 120)

    block:
      discard expireBuiltinResumptionEntryForTest(cstring("127.0.0.1"), uint16(41001), cstring(TestAlpn))
      setOverrideBuiltinNewSessionTicketMaxEarlyDataForTest(0'u32)
      defer:
        clearOverrideBuiltinNewSessionTicketMaxEarlyDataForTest()

      gAcceptedConn = nil
      gAcceptedStream = nil
      gReceivedMessageOk = false
      gColdStartConnected = false
      gColdStartResumed = true

      var zeroWireTicketConnection: HQUIC
      check api.ConnectionOpen(registration, nil, nil, addr zeroWireTicketConnection) == QUIC_STATUS_SUCCESS
      defer:
        api.ConnectionClose(zeroWireTicketConnection)
      check api.ConnectionSetConfiguration(zeroWireTicketConnection, configuration) == QUIC_STATUS_SUCCESS
      registerConnectionEventHandler(zeroWireTicketConnection, proc(ev: ConnectionEvent) =
        if ev.kind == ceConnected:
          gColdStartConnected = true
          gColdStartResumed = ev.sessionResumed
      )
      check api.ConnectionStart(
        zeroWireTicketConnection,
        configuration,
        QUIC_ADDRESS_FAMILY(2),
        cstring("127.0.0.1"),
        uint16(41001)
      ) == QUIC_STATUS_SUCCESS
      check waitForPredicate(proc(): bool = gColdStartConnected, rounds = 120)
      check not gColdStartResumed

      var zeroWireTicketStream: HQUIC
      check api.StreamOpen(
        zeroWireTicketConnection,
        QUIC_STREAM_OPEN_FLAGS(0),
        nil,
        nil,
        addr zeroWireTicketStream
      ) == QUIC_STATUS_SUCCESS
      defer:
        api.StreamClose(zeroWireTicketStream)
      check api.StreamStart(zeroWireTicketStream, QUIC_STREAM_START_FLAGS(0)) == QUIC_STATUS_SUCCESS
      check api.StreamSend(
        zeroWireTicketStream,
        addr sendBuffer,
        1'u32,
        QUIC_SEND_FLAGS(0x0001'u32),
        nil
      ) == QUIC_STATUS_SUCCESS
      check waitForPredicate(proc(): bool = gReceivedMessageOk, rounds = 120)
      check waitForPredicate(proc(): bool =
        var issuedAtUs = 0'u64
        var expiresAtUs = 0'u64
        var maxEarlyData = 1'u32
        getConnectionResumptionTicketMetaForTest(
          zeroWireTicketConnection,
          issuedAtUs,
          expiresAtUs,
          maxEarlyData
        ) and maxEarlyData == 0'u32
      , rounds = 120)

      gAcceptedConn = nil
      gAcceptedStream = nil
      gReceivedMessageOk = false
      gSessionResumedObserved = false
      gServerSessionResumedObserved = false
      gZeroRttPreHandshakeObserved = false

      var zeroWireResumedConnection: HQUIC
      check api.ConnectionOpen(registration, nil, nil, addr zeroWireResumedConnection) == QUIC_STATUS_SUCCESS
      defer:
        api.ConnectionClose(zeroWireResumedConnection)
      check api.ConnectionSetConfiguration(zeroWireResumedConnection, configuration) == QUIC_STATUS_SUCCESS
      registerConnectionEventHandler(zeroWireResumedConnection, proc(ev: ConnectionEvent) =
        if ev.kind == ceConnected and ev.sessionResumed:
          gSessionResumedObserved = true
      )
      check api.ConnectionStart(
        zeroWireResumedConnection,
        configuration,
        QUIC_ADDRESS_FAMILY(2),
        cstring("127.0.0.1"),
        uint16(41001)
      ) == QUIC_STATUS_SUCCESS

      var zeroWireRequestedEarlyData = true
      check getConnectionClientHelloRequestsEarlyDataForTest(
        zeroWireResumedConnection,
        zeroWireRequestedEarlyData
      )
      check not zeroWireRequestedEarlyData

      var zeroWireResumedStream: HQUIC
      check api.StreamOpen(
        zeroWireResumedConnection,
        QUIC_STREAM_OPEN_FLAGS(0),
        nil,
        nil,
        addr zeroWireResumedStream
      ) == QUIC_STATUS_SUCCESS
      defer:
        api.StreamClose(zeroWireResumedStream)
      check api.StreamStart(zeroWireResumedStream, QUIC_STREAM_START_FLAGS(0)) == QUIC_STATUS_SUCCESS
      check api.StreamSend(
        zeroWireResumedStream,
        addr sendBuffer,
        1'u32,
        QUIC_SEND_FLAGS(0x0001'u32),
        nil
      ) == QUIC_STATUS_SUCCESS

      check waitForPredicate(proc(): bool = gSessionResumedObserved, rounds = 120)
      check waitForPredicate(proc(): bool =
        not gAcceptedConn.isNil and
          getConnectionSessionResumedForTest(gAcceptedConn, serverResumedState) and
          serverResumedState
      , rounds = 120)
      check waitForPredicate(proc(): bool = gReceivedMessageOk, rounds = 120)
      check not gZeroRttPreHandshakeObserved
