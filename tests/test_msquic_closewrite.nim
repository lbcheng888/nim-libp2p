import std/[options, unittest]

import chronos

import ./msquic_test_helpers
import ../libp2p/transports/msquicdriver as msdriver
import ../libp2p/transports/msquicstream
import ../libp2p/transports/quicruntime as quicrt
import ../libp2p/stream/lpstream

when defined(libp2p_msquic_experimental):
  proc bytesToString(data: seq[byte]): string =
    if data.len == 0:
      return ""
    result = newString(data.len)
    copyMem(addr result[0], unsafeAddr data[0], data.len)

  suite "MsQuic closeWrite":
    test "closeWrite flushes payload and EOF":
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
                let clientState = streamStateOpt.get()
                defer:
                  msdriver.closeStream(handle, streamPtr, clientState)

                check msdriver.startStream(handle, streamPtr).len == 0
                let (startEventOpt, startEventErr) =
                  nextStreamEventOfKind(clientState, quicrt.qseStartComplete)
                check startEventErr.len == 0
                check startEventOpt.isSome

                let serverInboundFut = msdriver.awaitPendingStreamState(serverState)
                check waitFor serverInboundFut.withTimeout(3.seconds)
                let serverInbound = serverInboundFut.read()
                check serverInbound != nil

                let clientStream = newMsQuicStream(clientState, handle, Direction.Out)
                let serverStream = newMsQuicStream(serverInbound, handle, Direction.In)
                let payload = "hello-msquic-closewrite"

                let writeFuture = clientStream.writeLp(payload)
                check waitFor writeFuture.withTimeout(3.seconds)

                let closeWriteFuture = clientStream.closeWrite()
                check waitFor closeWriteFuture.withTimeout(3.seconds)

                let readLpFuture = serverStream.readLp(1024)
                check waitFor readLpFuture.withTimeout(3.seconds)
                check bytesToString(readLpFuture.read()) == payload

                let eofFuture = serverStream.read()
                check waitFor eofFuture.withTimeout(3.seconds)
                expect LPStreamEOFError:
                  discard eofFuture.read()
else:
  suite "MsQuic closeWrite":
    test "experimental features disabled":
      skip("libp2p_msquic_experimental not enabled")
