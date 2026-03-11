import std/[endians, sequtils, strutils, unittest]

import chronos

import ../api/api_impl
import ../api/diagnostics_model
import ../api/event_model
import ../api/param_catalog
import ../congestion/common

type
  SockAddrInLite = object
    family: uint16
    port: uint16
    rest: array[12, uint8]

const
  NativeListenerNewConnection = 0'u32
  NativeStreamEventReceive = 1'u32
  TestMessage = "ping-from-client"
  TestDatagram = "datagram-from-client"

var
  gAcceptedConn: HQUIC
  gAcceptedStream: HQUIC
  gHandshakeDerived: bool
  gReceivedMessageOk: bool
  gReceivedDatagramOk: bool
  gAckObserved: bool
  gSessionResumedObserved: bool
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
    gReceivedMessageOk = false
    gReceivedDatagramOk = false
    gAckObserved = false
    gSessionResumedObserved = false
    gColdStartConnected = false
    gColdStartResumed = false
    gPeerSendAbortedObserved = false
    gPeerSendAbortCode = 0'u64

    clearDiagnosticsHooks()
    registerDiagnosticsHook(proc(event: DiagnosticsEvent) {.gcsafe.} =
      if event.note.contains("Handshake Keys Derived. 1-RTT Enabled."):
        gHandshakeDerived = true
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
      )
      QUIC_STATUS_SUCCESS

    var listener: HQUIC
    check api.ListenerOpen(registration, cast[pointer](listenerCallback), nil, addr listener) == QUIC_STATUS_SUCCESS
    defer:
      api.ListenerClose(listener)

    var bindAddress = SockAddrInLite()
    bindAddress.family = 2'u16
    var portHost = 41001'u16
    bigEndian16(addr bindAddress.port, addr portHost)
    check api.ListenerStart(listener, addr alpnBuffer, 1'u32, addr bindAddress) == QUIC_STATUS_SUCCESS

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

    check waitForPredicate(proc(): bool =
      gAcceptedConn != nil and gHandshakeDerived
    )

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
    )

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
    )

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
    )
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

    gAcceptedConn = nil
    gAcceptedStream = nil
    gReceivedMessageOk = false
    gHandshakeDerived = false
    gSessionResumedObserved = false

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

    check waitForPredicate(proc(): bool =
      gSessionResumedObserved and gReceivedMessageOk
    )

    check expireBuiltinResumptionEntryForTest(cstring("127.0.0.1"), uint16(41001), cstring(TestAlpn))

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
    )
    check not gColdStartResumed
