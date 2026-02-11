import std/[options, unittest]

import chronos

import "../libp2p/transports/msquicdriver" as msdriver
import "../libp2p/transports/nim-msquic/api/event_model" as msevents

when defined(libp2p_msquic_experimental):
  suite "MsQuic experimental driver":
    test "dial lifecycle emits connection events":
      let (handle, initErr) = msdriver.initMsQuicTransport()
      if initErr.len > 0 or handle.isNil:
        echo "MsQuic runtime unavailable: ", initErr
        skip()
      else:
        defer:
          if not handle.isNil:
            handle.shutdown()

        let (connPtr, stateOpt, dialErr) = handle.dialConnection("example.com", 1443'u16)
        if dialErr.len > 0 or stateOpt.isNone:
          echo "MsQuic dial unavailable: ", dialErr
          skip()
        else:
          let connState = stateOpt.get()
          let connected = waitFor connState.nextConnectionEvent()
          check connected.kind == msevents.ceConnected

          discard handle.shutdownConnection(connPtr, errorCode = 7'u64)
          let shutdownEvent = waitFor connState.nextConnectionEvent()
          check shutdownEvent.kind == msevents.ceShutdownInitiated

          handle.closeConnection(connPtr, connState)
          expect msdriver.MsQuicEventQueueClosed:
            discard waitFor connState.nextConnectionEvent()

    test "dial fails after transport shutdown":
      let (handle, initErr) = msdriver.initMsQuicTransport()
      if initErr.len > 0 or handle.isNil:
        echo "MsQuic runtime unavailable: ", initErr
        skip()
      handle.shutdown()

      let (_, stateOpt, dialErr) = handle.dialConnection("example.com", 1'u16)
      check dialErr.len > 0
      check stateOpt.isNone

    test "stream start event and datagram send succeed":
      let (handle, initErr) = msdriver.initMsQuicTransport()
      if initErr.len > 0 or handle.isNil:
        echo "MsQuic runtime unavailable: ", initErr
        skip()
      else:
        defer:
          if not handle.isNil:
            handle.shutdown()

        let (connPtr, connStateOpt, dialErr) = handle.dialConnection("stream.test", 9443'u16)
        if dialErr.len > 0 or connStateOpt.isNone:
          echo "MsQuic dial unavailable: ", dialErr
          skip()
        else:
          let connState = connStateOpt.get()
          discard waitFor connState.nextConnectionEvent()

          let (streamPtr, streamStateOpt, streamErr) = handle.createStream(
            connPtr,
            connectionState = connState
          )
          if streamErr.len > 0 or streamStateOpt.isNone:
            echo "MsQuic stream unavailable: ", streamErr
            skip()
          else:
            let streamState = streamStateOpt.get()

            let startErr = handle.startStream(streamPtr)
            check startErr.len == 0
            let startEvent = waitFor streamState.nextStreamEvent()
            check startEvent.kind == msevents.seStartComplete

            let datagramErr = handle.sendDatagram(connPtr, @[byte 0x1, 0x2, 0x3])
            check datagramErr.len == 0

            handle.closeStream(streamPtr, streamState)
            expect msdriver.MsQuicEventQueueClosed:
              discard waitFor streamState.nextStreamEvent()

            discard handle.shutdownConnection(connPtr)
            handle.closeConnection(connPtr, connState)
else:
  suite "MsQuic experimental driver":
    test "experimental features disabled":
      skip("libp2p_msquic_experimental not enabled")
