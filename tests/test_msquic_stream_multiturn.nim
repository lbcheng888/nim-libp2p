import std/[options, sequtils, unittest]

import chronos

import ./msquic_test_helpers
import ../libp2p/transports/msquicdriver as msdriver
import ../libp2p/transports/quicruntime as quicrt

when defined(libp2p_msquic_experimental):
  proc waitStep[T](fut: Future[T]; label: string): T =
    check waitFor fut.withTimeout(3.seconds), label
    fut.read()

  suite "MsQuic multi-turn bidi stream":
    test "single bidi stream preserves multiple request and reply turns":
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

                let request1 = @[byte 0x10, 0x20, 0x30]
                let reply1 = @[byte 0x41, 0x42]
                let request2 = @[byte 0x31, 0x32, 0x33, 0x34]
                let reply2 = @[byte 0x51, 0x52, 0x53]
                let request3 = @[byte 0x61]
                let reply3 = @[byte 0x71, 0x72, 0x73, 0x74]

                check waitStep(msdriver.writeStreamAndWait(clientStream, request1), "client write request1") == ""
                let serverInboundFut = msdriver.awaitPendingStreamState(serverState)
                check waitFor serverInboundFut.withTimeout(3.seconds)
                let serverInbound = serverInboundFut.read()
                check serverInbound != nil
                check waitStep(msdriver.readStream(serverInbound), "server read request1") == request1

                check waitStep(msdriver.writeStreamAndWait(serverInbound, reply1), "server write reply1") == ""
                check waitStep(msdriver.readStream(clientStream), "client read reply1") == reply1

                check waitStep(msdriver.writeStreamAndWait(clientStream, request2), "client write request2") == ""
                check waitStep(msdriver.readStream(serverInbound), "server read request2") == request2

                check waitStep(msdriver.writeStreamAndWait(serverInbound, reply2), "server write reply2") == ""
                check waitStep(msdriver.readStream(clientStream), "client read reply2") == reply2

                check waitStep(msdriver.writeStreamAndWait(clientStream, request3), "client write request3") == ""
                check waitStep(msdriver.readStream(serverInbound), "server read request3") == request3

                waitFor sleepAsync(300.milliseconds)
                check waitStep(msdriver.writeStreamAndWait(serverInbound, reply3), "server write reply3") == ""
                check waitStep(msdriver.readStream(clientStream), "client read reply3") == reply3

    test "waiting reply on one bidi stream survives concurrent stream traffic on same connection":
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

              let (stream1Ptr, stream1Opt, stream1Err) = msdriver.createStream(
                handle,
                connPtr,
                connectionState = connState
              )
              if stream1Err.len > 0 or stream1Opt.isNone:
                echo "MsQuic stream unavailable: ", stream1Err
                skip()
              else:
                let clientControl = stream1Opt.get()
                defer:
                  msdriver.closeStream(handle, stream1Ptr, clientControl)
                check msdriver.startStream(handle, stream1Ptr).len == 0
                discard nextStreamEventOfKind(clientControl, quicrt.qseStartComplete)

                let request1 = @[byte 0xA1]
                check waitStep(msdriver.writeStreamAndWait(clientControl, request1), "control request1") == ""
                let serverControlFut = msdriver.awaitPendingStreamState(serverState)
                check waitFor serverControlFut.withTimeout(3.seconds)
                let serverControl = serverControlFut.read()
                check serverControl != nil
                check waitStep(msdriver.readStream(serverControl), "server control read request1") == request1
                check waitStep(msdriver.writeStreamAndWait(serverControl, @[byte 0xB1]), "server control write reply1") == ""
                check waitStep(msdriver.readStream(clientControl), "client control read reply1") == @[byte 0xB1]

                let request2 = @[byte 0xA2, 0xA3]
                check waitStep(msdriver.writeStreamAndWait(clientControl, request2), "control request2") == ""
                check waitStep(msdriver.readStream(serverControl), "server control read request2") == request2

                let pendingReply = msdriver.readStream(clientControl)

                let (stream2Ptr, stream2Opt, stream2Err) = msdriver.createStream(
                  handle,
                  connPtr,
                  connectionState = connState
                )
                check stream2Err.len == 0
                check stream2Opt.isSome
                let clientPing = stream2Opt.get()
                defer:
                  msdriver.closeStream(handle, stream2Ptr, clientPing)
                check msdriver.startStream(handle, stream2Ptr).len == 0
                discard nextStreamEventOfKind(clientPing, quicrt.qseStartComplete)
                check waitStep(msdriver.writeStreamAndWait(clientPing, @[byte 0xC1]), "client ping write") == ""

                let serverPingFut = msdriver.awaitPendingStreamState(serverState)
                check waitFor serverPingFut.withTimeout(3.seconds)
                let serverPing = serverPingFut.read()
                check serverPing != nil
                check waitStep(msdriver.readStream(serverPing), "server ping read") == @[byte 0xC1]
                check waitStep(msdriver.writeStreamAndWait(serverPing, @[byte 0xD1]), "server ping write") == ""
                check waitStep(msdriver.readStream(clientPing), "client ping read") == @[byte 0xD1]

                waitFor sleepAsync(300.milliseconds)
                check waitStep(msdriver.writeStreamAndWait(serverControl, @[byte 0xB2, 0xB3, 0xB4]), "server control write reply2") == ""
                check waitStep(pendingReply, "client control read reply2") == @[byte 0xB2, 0xB3, 0xB4]

    test "long-lived bidi stream still receives delayed reply after idle wait and concurrent side streams":
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

              let (controlPtr, controlOpt, controlErr) = msdriver.createStream(
                handle,
                connPtr,
                connectionState = connState
              )
              check controlErr.len == 0
              check controlOpt.isSome
              let clientControl = controlOpt.get()
              defer:
                msdriver.closeStream(handle, controlPtr, clientControl)
              check msdriver.startStream(handle, controlPtr).len == 0
              discard nextStreamEventOfKind(clientControl, quicrt.qseStartComplete)

              check waitStep(msdriver.writeStreamAndWait(clientControl, @[byte 0x11]), "control write first") == ""
              let serverControlFut = msdriver.awaitPendingStreamState(serverState)
              check waitFor serverControlFut.withTimeout(3.seconds)
              let serverControl = serverControlFut.read()
              check serverControl != nil
              check waitStep(msdriver.readStream(serverControl), "server control read first") == @[byte 0x11]
              check waitStep(msdriver.writeStreamAndWait(serverControl, @[byte 0x21]), "server control ack first") == ""
              check waitStep(msdriver.readStream(clientControl), "client control ack first") == @[byte 0x21]

              check waitStep(msdriver.writeStreamAndWait(clientControl, @[byte 0x12, 0x13]), "control write second") == ""
              check waitStep(msdriver.readStream(serverControl), "server control read second") == @[byte 0x12, 0x13]

              let pendingReply = msdriver.readStream(clientControl)

              for idx in 0 ..< 3:
                let (sidePtr, sideOpt, sideErr) = msdriver.createStream(
                  handle,
                  connPtr,
                  connectionState = connState
                )
                check sideErr.len == 0
                check sideOpt.isSome
                let clientSide = sideOpt.get()
                defer:
                  msdriver.closeStream(handle, sidePtr, clientSide)
                check msdriver.startStream(handle, sidePtr).len == 0
                discard nextStreamEventOfKind(clientSide, quicrt.qseStartComplete)
                check waitStep(msdriver.writeStreamAndWait(clientSide, @[byte(0x30 + idx)]), "side write " & $idx) == ""
                let serverSideFut = msdriver.awaitPendingStreamState(serverState)
                check waitFor serverSideFut.withTimeout(3.seconds)
                let serverSide = serverSideFut.read()
                check serverSide != nil
                check waitStep(msdriver.readStream(serverSide), "side read " & $idx) == @[byte(0x30 + idx)]
                check waitStep(msdriver.writeStreamAndWait(serverSide, @[byte(0x40 + idx)]), "side reply " & $idx) == ""
                check waitStep(msdriver.readStream(clientSide), "side client read " & $idx) == @[byte(0x40 + idx)]
                waitFor sleepAsync(2.seconds)

              check waitStep(msdriver.writeStreamAndWait(serverControl, @[byte 0x22, 0x23, 0x24]), "server control delayed reply") == ""
              check waitStep(pendingReply, "client control delayed reply") == @[byte 0x22, 0x23, 0x24]

    test "long-lived bidi stream reassembles delayed large reply on same stream":
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

              let (controlPtr, controlOpt, controlErr) = msdriver.createStream(
                handle,
                connPtr,
                connectionState = connState
              )
              check controlErr.len == 0
              check controlOpt.isSome
              let clientControl = controlOpt.get()
              defer:
                msdriver.closeStream(handle, controlPtr, clientControl)
              check msdriver.startStream(handle, controlPtr).len == 0
              discard nextStreamEventOfKind(clientControl, quicrt.qseStartComplete)

              check waitStep(msdriver.writeStreamAndWait(clientControl, @[byte 0x91, 0x92]), "large control write first") == ""
              let serverControlFut = msdriver.awaitPendingStreamState(serverState)
              check waitFor serverControlFut.withTimeout(3.seconds)
              let serverControl = serverControlFut.read()
              check serverControl != nil
              check waitStep(msdriver.readStream(serverControl), "large control read first") == @[byte 0x91, 0x92]
              check waitStep(msdriver.writeStreamAndWait(serverControl, @[byte 0xA1]), "large control write ack") == ""
              check waitStep(msdriver.readStream(clientControl), "large control read ack") == @[byte 0xA1]

              check waitStep(msdriver.writeStreamAndWait(clientControl, @[byte 0x93, 0x94, 0x95]), "large control write second") == ""
              check waitStep(msdriver.readStream(serverControl), "large control read second") == @[byte 0x93, 0x94, 0x95]

              let pendingReply = msdriver.readStream(clientControl)
              let largeReply = newSeqWith(8192, byte(0x5A))
              waitFor sleepAsync(2.seconds)
              check waitStep(msdriver.writeStreamAndWait(serverControl, largeReply), "large control delayed reply") == ""
              check waitStep(pendingReply, "large control delayed read") == largeReply

else:
  suite "MsQuic multi-turn bidi stream":
    test "experimental features disabled":
      skip("libp2p_msquic_experimental not enabled")
