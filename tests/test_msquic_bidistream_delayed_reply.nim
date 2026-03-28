import std/[options, unittest]

import chronos

import ./msquic_test_helpers
import ../libp2p/transports/msquicdriver as msdriver
import ../libp2p/transports/quicruntime as quicrt

when defined(libp2p_msquic_experimental):
  suite "MsQuic delayed bidi reply":
    test "local initiated bidi stream can receive delayed server reply on same stream":
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
                let clientStream = streamStateOpt.get()
                defer:
                  msdriver.closeStream(handle, streamPtr, clientStream)

                check msdriver.startStream(handle, streamPtr).len == 0
                let (startEventOpt, startEventErr) =
                  nextStreamEventOfKind(clientStream, quicrt.qseStartComplete)
                check startEventErr.len == 0
                check startEventOpt.isSome

                let request = @[byte 0x10, 0x20, 0x30]
                check (waitFor msdriver.writeStreamAndWait(clientStream, request)) == ""

                let serverInboundFut = msdriver.awaitPendingStreamState(serverState)
                check waitFor serverInboundFut.withTimeout(3.seconds)
                let serverInbound = serverInboundFut.read()
                check serverInbound != nil

                let serverRead = msdriver.readStream(serverInbound)
                check waitFor serverRead.withTimeout(3.seconds)
                check serverRead.read() == request

                let clientRead = msdriver.readStream(clientStream)
                waitFor sleepAsync(200.milliseconds)
                check (waitFor msdriver.writeStreamAndWait(
                  serverInbound,
                  @[byte 0x41, 0x42, 0x43]
                )) == ""
                check waitFor clientRead.withTimeout(3.seconds)
                check clientRead.read() == @[byte 0x41, 0x42, 0x43]

else:
  suite "MsQuic delayed bidi reply":
    test "experimental features disabled":
      skip("libp2p_msquic_experimental not enabled")
