import std/[options, unittest]

import chronos

import ./msquic_test_helpers
import ../libp2p/transports/msquicdriver as msdriver
import ../libp2p/transports/quicruntime as quicrt

when defined(libp2p_msquic_experimental):
  suite "MsQuic buffered close":
    test "closed stream still drains buffered payload and eof markers":
      let (handle, initErr) = msdriver.initMsQuicTransport()
      if initErr.len > 0 or handle.isNil:
        echo "MsQuic runtime unavailable: ", initErr
        skip()
      else:
        defer:
          if not handle.isNil:
            msdriver.shutdown(handle)

        let (listenerOpt, listenerErr) = startLoopbackListener(handle)
        if listenerErr.len > 0 or listenerOpt.isNone:
          echo "MsQuic listener unavailable: ", listenerErr
          skip()
        else:
          let listener = listenerOpt.get()
          defer:
            discard msdriver.stopListener(handle, listener.listener)
            msdriver.closeListener(handle, listener.listener, listener.state)

          let (connPtr, connStateOpt, dialErr) =
            msdriver.dialConnection(handle, LoopbackDialHost, listener.port)
          if dialErr.len > 0 or connStateOpt.isNone:
            echo "MsQuic dial unavailable: ", dialErr
            skip()
          else:
            let connState = connStateOpt.get()
            let (serverConnPtr, serverStateOpt, acceptErr) =
              acceptPendingConnection(listener.state)
            if acceptErr.len > 0 or serverStateOpt.isNone:
              echo "MsQuic accept unavailable: ", acceptErr
              skip()
            else:
              let serverState = serverStateOpt.get()
              defer:
                discard msdriver.shutdownConnection(handle, serverConnPtr)
                msdriver.closeConnection(handle, serverConnPtr, serverState)
                discard msdriver.shutdownConnection(handle, connPtr)
                msdriver.closeConnection(handle, connPtr, connState)

              discard nextConnectionEventOfKind(connState, quicrt.qceConnected)
              discard nextConnectionEventOfKind(serverState, quicrt.qceConnected)

              let (streamPtr, streamStateOpt, streamErr) = msdriver.createStream(
                handle,
                connPtr,
                connectionState = connState
              )
              if streamErr.len > 0 or streamStateOpt.isNone:
                echo "MsQuic stream unavailable: ", streamErr
                skip()
              else:
                let streamState = streamStateOpt.get()
                check msdriver.startStream(handle, streamPtr).len == 0
                discard nextStreamEventOfKind(streamState, quicrt.qseStartComplete)

                let payload = @[byte 0xCA, 0xFE, 0xBA, 0xBE]
                msdriver.pushStreamReceive(streamState, payload)
                msdriver.pushStreamReceive(streamState, @[])
                msdriver.closeStream(handle, streamPtr, streamState)

                let first = waitFor msdriver.readStream(streamState)
                check first == payload

                let eofChunk = waitFor msdriver.readStream(streamState)
                check eofChunk.len == 0

    test "close still fulfills pending read waiters with queued payload and eof":
      let (handle, initErr) = msdriver.initMsQuicTransport()
      if initErr.len > 0 or handle.isNil:
        echo "MsQuic runtime unavailable: ", initErr
        skip()
      else:
        defer:
          if not handle.isNil:
            msdriver.shutdown(handle)

        let (listenerOpt, listenerErr) = startLoopbackListener(handle)
        if listenerErr.len > 0 or listenerOpt.isNone:
          echo "MsQuic listener unavailable: ", listenerErr
          skip()
        else:
          let listener = listenerOpt.get()
          defer:
            discard msdriver.stopListener(handle, listener.listener)
            msdriver.closeListener(handle, listener.listener, listener.state)

          let (connPtr, connStateOpt, dialErr) =
            msdriver.dialConnection(handle, LoopbackDialHost, listener.port)
          if dialErr.len > 0 or connStateOpt.isNone:
            echo "MsQuic dial unavailable: ", dialErr
            skip()
          else:
            let connState = connStateOpt.get()
            let (serverConnPtr, serverStateOpt, acceptErr) =
              acceptPendingConnection(listener.state)
            if acceptErr.len > 0 or serverStateOpt.isNone:
              echo "MsQuic accept unavailable: ", acceptErr
              skip()
            else:
              let serverState = serverStateOpt.get()
              defer:
                discard msdriver.shutdownConnection(handle, serverConnPtr)
                msdriver.closeConnection(handle, serverConnPtr, serverState)
                discard msdriver.shutdownConnection(handle, connPtr)
                msdriver.closeConnection(handle, connPtr, connState)

              discard nextConnectionEventOfKind(connState, quicrt.qceConnected)
              discard nextConnectionEventOfKind(serverState, quicrt.qceConnected)

              let (streamPtr, streamStateOpt, streamErr) = msdriver.createStream(
                handle,
                connPtr,
                connectionState = connState
              )
              if streamErr.len > 0 or streamStateOpt.isNone:
                echo "MsQuic stream unavailable: ", streamErr
                skip()
              else:
                let streamState = streamStateOpt.get()
                check msdriver.startStream(handle, streamPtr).len == 0
                discard nextStreamEventOfKind(streamState, quicrt.qseStartComplete)

                let firstRead = msdriver.readStream(streamState)
                let secondRead = msdriver.readStream(streamState)
                let payload = @[byte 0xDE, 0xAD, 0xBE, 0xEF]
                msdriver.pushStreamReceive(streamState, payload)
                msdriver.pushStreamReceive(streamState, @[])
                msdriver.closeStream(handle, streamPtr, streamState)

                check waitFor(firstRead) == payload
                check waitFor(secondRead).len == 0

    test "cancelled read waiter does not consume later payload":
      let (handle, initErr) = msdriver.initMsQuicTransport()
      if initErr.len > 0 or handle.isNil:
        echo "MsQuic runtime unavailable: ", initErr
        skip()
      else:
        defer:
          if not handle.isNil:
            msdriver.shutdown(handle)

        let (listenerOpt, listenerErr) = startLoopbackListener(handle)
        if listenerErr.len > 0 or listenerOpt.isNone:
          echo "MsQuic listener unavailable: ", listenerErr
          skip()
        else:
          let listener = listenerOpt.get()
          defer:
            discard msdriver.stopListener(handle, listener.listener)
            msdriver.closeListener(handle, listener.listener, listener.state)

          let (connPtr, connStateOpt, dialErr) =
            msdriver.dialConnection(handle, LoopbackDialHost, listener.port)
          if dialErr.len > 0 or connStateOpt.isNone:
            echo "MsQuic dial unavailable: ", dialErr
            skip()
          else:
            let connState = connStateOpt.get()
            let (serverConnPtr, serverStateOpt, acceptErr) =
              acceptPendingConnection(listener.state)
            if acceptErr.len > 0 or serverStateOpt.isNone:
              echo "MsQuic accept unavailable: ", acceptErr
              skip()
            else:
              let serverState = serverStateOpt.get()
              defer:
                discard msdriver.shutdownConnection(handle, serverConnPtr)
                msdriver.closeConnection(handle, serverConnPtr, serverState)
                discard msdriver.shutdownConnection(handle, connPtr)
                msdriver.closeConnection(handle, connPtr, connState)

              discard nextConnectionEventOfKind(connState, quicrt.qceConnected)
              discard nextConnectionEventOfKind(serverState, quicrt.qceConnected)

              let (streamPtr, streamStateOpt, streamErr) = msdriver.createStream(
                handle,
                connPtr,
                connectionState = connState
              )
              if streamErr.len > 0 or streamStateOpt.isNone:
                echo "MsQuic stream unavailable: ", streamErr
                skip()
              else:
                let streamState = streamStateOpt.get()
                check msdriver.startStream(handle, streamPtr).len == 0
                discard nextStreamEventOfKind(streamState, quicrt.qseStartComplete)

                let cancelledRead = msdriver.readStream(streamState)
                cancelledRead.cancel()
                check cancelledRead.cancelled()

                let nextRead = msdriver.readStream(streamState)
                let payload = @[byte 0xAA, 0xBB, 0xCC]
                msdriver.pushStreamReceive(streamState, payload)

                check waitFor(nextRead) == payload
else:
  suite "MsQuic buffered close":
    test "experimental features disabled":
      skip("libp2p_msquic_experimental not enabled")
