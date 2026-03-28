import std/[options, unittest]

import chronos

import ./msquic_test_helpers
import ../libp2p/transports/msquicdriver as msdriver
import ../libp2p/transports/quicruntime as quicrt

when defined(libp2p_msquic_experimental):
  suite "MsQuic multi roundtrip":
    test "local initiated bidi stream handles repeated request reply turns":
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
              check streamErr.len == 0
              check streamStateOpt.isSome
              if streamErr.len == 0 and streamStateOpt.isSome:
                let clientStream = streamStateOpt.get()
                defer:
                  msdriver.closeStream(handle, streamPtr, clientStream)

                check msdriver.startStream(handle, streamPtr).len == 0
                let (startEventOpt, startEventErr) =
                  nextStreamEventOfKind(clientStream, quicrt.qseStartComplete)
                check startEventErr.len == 0
                check startEventOpt.isSome

                if startEventErr.len == 0 and startEventOpt.isSome:
                  let request1 = @[byte 0x11, 0x22]
                  let serverInboundFut = msdriver.awaitPendingStreamState(serverState)
                  echo "roundtrip stage client->server #1"
                  check (waitFor msdriver.writeStreamAndWait(clientStream, request1)) == ""
                  let inboundOk = waitFor serverInboundFut.withTimeout(3.seconds)
                  check inboundOk
                  if inboundOk:
                    let serverInbound = serverInboundFut.read()
                    check serverInbound != nil
                    if not serverInbound.isNil:
                      let read1 = msdriver.readStream(serverInbound)
                      let read1Ok = waitFor read1.withTimeout(3.seconds)
                      check read1Ok
                      if read1Ok:
                        check read1.read() == request1

                        let reply1 = @[byte 0x31, 0x32]
                        echo "roundtrip stage server->client #1"
                        let clientRead1 = msdriver.readStream(clientStream)
                        waitFor sleepAsync(150.milliseconds)
                        check (waitFor msdriver.writeStreamAndWait(serverInbound, reply1)) == ""
                        let clientRead1Ok = waitFor clientRead1.withTimeout(3.seconds)
                        check clientRead1Ok
                        if clientRead1Ok:
                          check clientRead1.read() == reply1

                          let request2 = @[byte 0x41, 0x42, 0x43]
                          echo "roundtrip stage client->server #2"
                          check (waitFor msdriver.writeStreamAndWait(clientStream, request2)) == ""
                          let read2 = msdriver.readStream(serverInbound)
                          let read2Ok = waitFor read2.withTimeout(3.seconds)
                          check read2Ok
                          if read2Ok:
                            check read2.read() == request2

                            let reply2 = @[byte 0x51, 0x52, 0x53, 0x54]
                            echo "roundtrip stage server->client #2"
                            let clientRead2 = msdriver.readStream(clientStream)
                            waitFor sleepAsync(150.milliseconds)
                            check (waitFor msdriver.writeStreamAndWait(serverInbound, reply2)) == ""
                            let clientRead2Ok = waitFor clientRead2.withTimeout(3.seconds)
                            check clientRead2Ok
                            if clientRead2Ok:
                              check clientRead2.read() == reply2

else:
  suite "MsQuic multi roundtrip":
    test "experimental features disabled":
      skip("libp2p_msquic_experimental not enabled")
