import std/[options, sequtils, unittest]

import chronos

import "../libp2p/transports/msquicdriver" as msdriver
import "../libp2p/transports/nim-msquic/api/event_model" as msevents
import "../libp2p/transports/nim-msquic/api/param_catalog" as msparam

when defined(libp2p_msquic_experimental):
  suite "MsQuic stream & datagram":
    test "stream receive events propagate":
      let (handle, initErr) = msdriver.initMsQuicTransport()
      if initErr.len > 0 or handle.isNil:
        echo "MsQuic runtime unavailable: ", initErr
        skip()
      else:
        defer:
          if not handle.isNil:
            handle.shutdown()

        let (connPtr, connStateOpt, dialErr) = handle.dialConnection("stream.example", 3443'u16)
        if dialErr.len > 0 or connStateOpt.isNone:
          echo "MsQuic dial unavailable: ", dialErr
          skip()
        else:
          let connState = connStateOpt.get()
          discard waitFor connState.nextConnectionEvent() # ceConnected

          let (streamPtr, streamStateOpt, streamErr) = handle.createStream(
            connPtr,
            connectionState = connState
          )
          if streamErr.len > 0 or streamStateOpt.isNone:
            echo "MsQuic stream unavailable: ", streamErr
            skip()
          else:
            let streamState = streamStateOpt.get()
            check handle.startStream(streamPtr).len == 0

            let startEvent = waitFor streamState.nextStreamEvent()
            check startEvent.kind == msevents.seStartComplete

            let writeErr = msdriver.writeStream(streamState, @[byte 0xAA, 0xBB], clientContext = nil)
            check writeErr.len == 0
            let sendEvent = waitFor streamState.nextStreamEvent()
            check sendEvent.kind == msevents.seSendComplete

            let payload = @[byte 1, 2, 3, 4]
            msdriver.pushStreamReceive(streamState, payload)

            let recvEvent = waitFor streamState.nextStreamEvent()
            check recvEvent.kind == msevents.seReceive
            check recvEvent.totalBufferLength == uint64(payload.len)
            let buf = waitFor msdriver.readStream(streamState)
            check buf.len == payload.len

            handle.closeStream(streamPtr, streamState)
            discard handle.shutdownConnection(connPtr)
            handle.closeConnection(connPtr, connState)

    test "datagram send surfaces connection event":
      let (handle, initErr) = msdriver.initMsQuicTransport()
      if initErr.len > 0 or handle.isNil:
        echo "MsQuic runtime unavailable: ", initErr
        skip()
      else:
        defer:
          if not handle.isNil:
            handle.shutdown()

        let (connPtr, connStateOpt, dialErr) = handle.dialConnection("datagram.example", 4545'u16)
        if dialErr.len > 0 or connStateOpt.isNone:
          echo "MsQuic dial unavailable: ", dialErr
          skip()
        else:
          let connState = connStateOpt.get()
          discard waitFor connState.nextConnectionEvent()

          let err = handle.sendDatagram(connPtr, @[byte 0xAA])
          check err.len == 0
          let evt = waitFor connState.nextConnectionEvent()
          check evt.kind == msevents.ceParameterUpdated
          check evt.paramId == msparam.QUIC_PARAM_CONN_DATAGRAM_SEND_ENABLED

          discard handle.shutdownConnection(connPtr)
          handle.closeConnection(connPtr, connState)
else:
  suite "MsQuic stream & datagram":
    test "experimental features disabled":
      skip("libp2p_msquic_experimental not enabled")
