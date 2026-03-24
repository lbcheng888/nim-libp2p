import std/unittest

import "../libp2p/transports/quicruntime" as quicrt
import "../libp2p/transports/nim-msquic/api/event_model" as msevents

when defined(libp2p_msquic_experimental):
  suite "QuicRuntime event adapters":
    test "connection event maps to runtime-neutral shape":
      let raw = msevents.ConnectionEvent(
        connection: cast[pointer](0x11),
        kind: msevents.ceDatagramReceived,
        sessionResumed: true,
        negotiatedAlpn: "libp2p",
        errorCode: 7'u64,
        status: 9'u32,
        paramId: 13'u32,
        boolValue: true,
        maxSendLength: 1200'u16,
        handshakeCompleted: true,
        peerAcknowledgedShutdown: true,
        appCloseInProgress: false,
        userContext: cast[pointer](0x22),
        uintValue: 33'u64,
        note: "datagram",
        stream: cast[pointer](0x33),
        streamFlags: 5'u32,
        streamIsUnidirectional: true,
        datagramPayload: @[byte 1, 2, 3],
        datagramFlags: 17'u32
      )
      let event = quicrt.toQuicEvent(raw)
      check event.kind == quicrt.qceDatagramReceived
      check quicrt.kindLabel(event.kind) == "datagram_received"
      check event.negotiatedAlpn == "libp2p"
      check event.streamIsUnidirectional
      check event.datagramPayload == @[byte 1, 2, 3]

    test "stream event maps to runtime-neutral shape":
      let raw = msevents.StreamEvent(
        stream: cast[pointer](0x44),
        kind: msevents.seReceiveBufferNeeded,
        status: 3'u32,
        id: 8'u64,
        absoluteOffset: 21'u64,
        totalBufferLength: 34'u64,
        bufferCount: 2'u32,
        flags: 6'u32,
        cancelled: false,
        peerAccepted: true,
        graceful: true,
        connectionShutdown: false,
        appCloseInProgress: false,
        connectionShutdownByApp: false,
        connectionClosedRemotely: false,
        connectionErrorCode: 55'u64,
        connectionCloseStatus: 89'u32,
        errorCode: 144'u64,
        clientContext: cast[pointer](0x55),
        byteCount: 233'u64,
        bufferLengthNeeded: 377'u64,
        userContext: cast[pointer](0x66),
        note: "need buffer",
        payload: @[byte 9, 8]
      )
      let event = quicrt.toQuicEvent(raw)
      check event.kind == quicrt.qseReceiveBufferNeeded
      check quicrt.kindLabel(event.kind) == "receive_buffer_needed"
      check event.bufferLengthNeeded == 377'u64
      check event.payload == @[byte 9, 8]

    test "listener event maps to runtime-neutral shape":
      let raw = msevents.ListenerEvent(
        listener: cast[pointer](0x77),
        kind: msevents.leDosModeChanged,
        connection: cast[pointer](0x88),
        info: cast[pointer](0x99),
        dosModeEnabled: true,
        appCloseInProgress: true,
        userContext: cast[pointer](0xAA),
        note: "dos"
      )
      let event = quicrt.toQuicEvent(raw)
      check event.kind == quicrt.qleDosModeChanged
      check quicrt.kindLabel(event.kind) == "dos_mode_changed"
      check event.dosModeEnabled
      check event.note == "dos"
else:
  suite "QuicRuntime event adapters":
    test "experimental features disabled":
      skip("libp2p_msquic_experimental not enabled")
