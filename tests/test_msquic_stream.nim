import std/[options, unittest]

import chronos
import stew/byteutils

import ./msquic_test_helpers
import "../libp2p/transports/msquicdriver" as msdriver
import "../libp2p/transports/msquicstream"
import "../libp2p/transports/msquicconnection"
import "../libp2p/transports/quicruntime" as quicrt
import "../libp2p/transports/nim-msquic/api/api_impl" as msapi
import "../libp2p/transports/nim-msquic/api/param_catalog" as msparam
import "../libp2p/transports/nim-msquic/protocol/protocol_core" as proto
import "../libp2p/stream/lpstream"

when defined(libp2p_msquic_experimental):
  proc bytesToString(data: seq[byte]): string =
    if data.len == 0:
      return ""
    result = newString(data.len)
    copyMem(addr result[0], unsafeAddr data[0], data.len)

  suite "MsQuic stream & datagram":
    proc waitStep[T](fut: Future[T]; label: string): T =
      check waitFor fut.withTimeout(3.seconds), label
      fut.read()

    test "stream receive events propagate":
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

                let (startEventOpt, startEventErr) =
                  nextStreamEventOfKind(streamState, quicrt.qseStartComplete)
                check startEventErr.len == 0
                check startEventOpt.isSome

                let writeErr = msdriver.writeStream(streamState, @[byte 0xAA, 0xBB], clientContext = nil)
                check writeErr.len == 0
                let (sendEventOpt, sendEventErr) =
                  nextStreamEventOfKind(streamState, quicrt.qseSendComplete)
                check sendEventErr.len == 0
                check sendEventOpt.isSome

                let payload = @[byte 1, 2, 3, 4]
                msdriver.pushStreamReceive(streamState, payload)

                let (recvEventOpt, recvEventErr) =
                  nextStreamEventOfKind(streamState, quicrt.qseReceive)
                check recvEventErr.len == 0
                check recvEventOpt.isSome
                let recvEvent = recvEventOpt.get()
                check recvEvent.totalBufferLength == uint64(payload.len)
                let buf = waitFor msdriver.readStream(streamState)
                check buf.len == payload.len

                msdriver.closeStream(handle, streamPtr, streamState)
                discard msdriver.shutdownConnection(handle, connPtr)
                msdriver.closeConnection(handle, connPtr, connState)

    test "restored pending inbound stream remains deliverable":
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
                let (startEventOpt, startEventErr) =
                  nextStreamEventOfKind(streamState, quicrt.qseStartComplete)
                check startEventErr.len == 0
                check startEventOpt.isSome

                let pending = waitFor msdriver.awaitPendingStreamState(serverState)
                check pending != nil
                let pendingId = quicrt.streamId(pending)
                check pendingId.isOk

                let popped = quicrt.popPendingStreamState(serverState)
                check popped.isSome
                check quicrt.peekPendingStreamState(serverState).isNone

                quicrt.restorePendingStreamState(serverState, popped.get())
                let restored = quicrt.popPendingStreamState(serverState)
                check restored.isSome
                let restoredId = quicrt.streamId(restored.get())
                check restoredId.isOk
                check restoredId.get() == pendingId.get()

                msdriver.closeStream(handle, streamPtr, streamState)

    test "pending inbound stream queue preserves FIFO and sibling close independence":
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

              proc openStarted(): tuple[streamPtr: pointer, state: msdriver.MsQuicStreamState, id: uint64] =
                let (streamPtr, streamStateOpt, streamErr) = msdriver.createStream(
                  handle,
                  connPtr,
                  connectionState = connState
                )
                check streamErr.len == 0
                check streamStateOpt.isSome
                if streamErr.len > 0 or streamStateOpt.isNone:
                  return (nil, nil, 0'u64)
                let streamState = streamStateOpt.get()
                check msdriver.startStream(handle, streamPtr).len == 0
                let (startEventOpt, startEventErr) =
                  nextStreamEventOfKind(streamState, quicrt.qseStartComplete)
                check startEventErr.len == 0
                check startEventOpt.isSome
                let idRes = quicrt.streamId(streamState)
                check idRes.isOk
                (streamPtr, streamState, if idRes.isOk: idRes.get() else: 0'u64)

              let openedA = openStarted()
              let openedB = openStarted()
              let openedC = openStarted()
              var serverInboundA: msdriver.MsQuicStreamState = nil
              var serverInboundB: msdriver.MsQuicStreamState = nil
              var serverInboundC: msdriver.MsQuicStreamState = nil
              defer:
                if not openedA.state.isNil:
                  msdriver.closeStream(handle, openedA.streamPtr, openedA.state)
                if not openedB.state.isNil:
                  msdriver.closeStream(handle, openedB.streamPtr, openedB.state)
                if not openedC.state.isNil:
                  msdriver.closeStream(handle, openedC.streamPtr, openedC.state)
                if not serverInboundA.isNil:
                  msdriver.closeStream(handle, cast[pointer](serverInboundA.stream), serverInboundA)
                if not serverInboundB.isNil:
                  msdriver.closeStream(handle, cast[pointer](serverInboundB.stream), serverInboundB)
                if not serverInboundC.isNil:
                  msdriver.closeStream(handle, cast[pointer](serverInboundC.stream), serverInboundC)

              let inboundAFut = msdriver.awaitPendingStreamState(serverState)
              let inboundBFut = msdriver.awaitPendingStreamState(serverState)
              let inboundCFut = msdriver.awaitPendingStreamState(serverState)
              check waitFor inboundAFut.withTimeout(3.seconds)
              check waitFor inboundBFut.withTimeout(3.seconds)
              check waitFor inboundCFut.withTimeout(3.seconds)
              serverInboundA = inboundAFut.read()
              serverInboundB = inboundBFut.read()
              serverInboundC = inboundCFut.read()
              check serverInboundA != nil
              check serverInboundB != nil
              check serverInboundC != nil

              let inboundAId = quicrt.streamId(serverInboundA)
              let inboundBId = quicrt.streamId(serverInboundB)
              let inboundCId = quicrt.streamId(serverInboundC)
              check inboundAId.isOk
              check inboundBId.isOk
              check inboundCId.isOk
              check @[inboundAId.get(), inboundBId.get(), inboundCId.get()] ==
                @[openedA.id, openedB.id, openedC.id]

              let payloadA = @[byte 0x11, 0x12]
              let payloadC = @[byte 0x31, 0x32, 0x33]
              check waitFor msdriver.writeStreamAndWait(openedA.state, payloadA).withTimeout(3.seconds)
              let serverReadA = msdriver.readStream(serverInboundA)
              check waitFor serverReadA.withTimeout(3.seconds)
              check serverReadA.read() == payloadA

              msdriver.closeStream(handle, cast[pointer](serverInboundB.stream), serverInboundB)
              serverInboundB = nil

              let replyA = @[byte 0x21]
              let replyC = @[byte 0x41, 0x42]
              check waitFor msdriver.writeStreamAndWait(serverInboundA, replyA).withTimeout(3.seconds)
              let clientReadA = msdriver.readStream(openedA.state)
              check waitFor clientReadA.withTimeout(3.seconds)
              check clientReadA.read() == replyA

              check waitFor msdriver.writeStreamAndWait(openedC.state, payloadC).withTimeout(3.seconds)
              let serverReadC = msdriver.readStream(serverInboundC)
              check waitFor serverReadC.withTimeout(3.seconds)
              check serverReadC.read() == payloadC
              check waitFor msdriver.writeStreamAndWait(serverInboundC, replyC).withTimeout(3.seconds)
              let clientReadC = msdriver.readStream(openedC.state)
              check waitFor clientReadC.withTimeout(3.seconds)
              check clientReadC.read() == replyC

    test "locally initiated bidi stream receives delayed server reply on same stream":
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

                let serverInboundFut = msdriver.awaitPendingStreamState(serverState)
                check waitFor serverInboundFut.withTimeout(3.seconds)
                let serverInbound = serverInboundFut.read()
                check serverInbound != nil

                let request = @[byte 0x10, 0x20, 0x30]
                check (waitFor msdriver.writeStreamAndWait(clientStream, request)) == ""

                let serverRead = msdriver.readStream(serverInbound)
                check waitFor serverRead.withTimeout(DefaultMsQuicEventTimeout)
                check serverRead.read() == request

                let clientRead = msdriver.readStream(clientStream)
                waitFor sleepAsync(200.milliseconds)
                discard waitFor msdriver.writeStreamAndWait(serverInbound, @[byte 0x41, 0x42, 0x43])
                check waitFor clientRead.withTimeout(3.seconds)
                check clientRead.read() == @[byte 0x41, 0x42, 0x43]

    test "locally initiated bidi stream supports multi-turn request and reply on same stream":
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

                let serverInboundFut = msdriver.awaitPendingStreamState(serverState)
                check waitFor serverInboundFut.withTimeout(3.seconds)
                let serverInbound = serverInboundFut.read()
                check serverInbound != nil

                let request1 = @[byte 0x10, 0x20, 0x30]
                check waitStep(msdriver.writeStreamAndWait(clientStream, request1), "client write request1") == ""

                let serverRead1 = msdriver.readStream(serverInbound)
                check waitStep(serverRead1, "server read request1") == request1

                let clientRead1 = msdriver.readStream(clientStream)
                waitFor sleepAsync(100.milliseconds)
                check waitStep(msdriver.writeStreamAndWait(serverInbound, @[byte 0x41, 0x42]), "server write reply1") == ""
                check waitStep(clientRead1, "client read reply1") == @[byte 0x41, 0x42]

                let request2 = @[byte 0x31, 0x32, 0x33, 0x34]
                check waitStep(msdriver.writeStreamAndWait(clientStream, request2), "client write request2") == ""

                let serverRead2 = msdriver.readStream(serverInbound)
                check waitStep(serverRead2, "server read request2") == request2

                let clientRead2 = msdriver.readStream(clientStream)
                waitFor sleepAsync(300.milliseconds)
                check waitStep(msdriver.writeStreamAndWait(serverInbound, @[byte 0x51, 0x52, 0x53]), "server write reply2") == ""
                check waitStep(clientRead2, "client read reply2") == @[byte 0x51, 0x52, 0x53]

                let request3 = @[byte 0x61]
                check waitStep(msdriver.writeStreamAndWait(clientStream, request3), "client write request3") == ""

                let serverRead3 = msdriver.readStream(serverInbound)
                check waitStep(serverRead3, "server read request3") == request3

    test "same stream sequential server writes each wait for their own completion":
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
                discard nextStreamEventOfKind(clientStream, quicrt.qseStartComplete)

                let serverInboundFut = msdriver.awaitPendingStreamState(serverState)
                check waitFor serverInboundFut.withTimeout(3.seconds)
                let serverInbound = serverInboundFut.read()
                check serverInbound != nil

                let clientRead1 = msdriver.readStream(clientStream)
                let clientRead2 = msdriver.readStream(clientStream)
                let clientRead3 = msdriver.readStream(clientStream)

                check waitStep(msdriver.writeStreamAndWait(serverInbound, @[byte 0x41]), "server write 1") == ""
                check waitStep(msdriver.writeStreamAndWait(serverInbound, @[byte 0x42, 0x43]), "server write 2") == ""
                check waitStep(msdriver.writeStreamAndWait(serverInbound, @[byte 0x44, 0x45, 0x46]), "server write 3") == ""

                check waitStep(clientRead1, "client read 1") == @[byte 0x41]
                check waitStep(clientRead2, "client read 2") == @[byte 0x42, 0x43]
                check waitStep(clientRead3, "client read 3") == @[byte 0x44, 0x45, 0x46]

    test "same inbound stream second payload does not reappear as new pending stream":
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
                discard nextStreamEventOfKind(clientStream, quicrt.qseStartComplete)

                let serverInboundFut = msdriver.awaitPendingStreamState(serverState)
                check waitFor serverInboundFut.withTimeout(3.seconds)
                let serverInbound = serverInboundFut.read()
                check serverInbound != nil

                let msg1 = @[byte 0x11, 0x22]
                let msg2 = @[byte 0x33, 0x44, 0x55]

                check (waitFor msdriver.writeStreamAndWait(clientStream, msg1)) == ""
                let serverRead1 = msdriver.readStream(serverInbound)
                check waitFor serverRead1.withTimeout(3.seconds)
                check serverRead1.read() == msg1

                check (waitFor msdriver.writeStreamAndWait(clientStream, msg2)) == ""
                let duplicateFut = msdriver.awaitPendingStreamState(serverState)
                check not waitFor duplicateFut.withTimeout(300.milliseconds)
                duplicateFut.cancelSoon()

                let serverRead2 = msdriver.readStream(serverInbound)
                check waitFor serverRead2.withTimeout(3.seconds)
                check serverRead2.read() == msg2

    test "primary connection stream preserves sequential LP exchange on same stream":
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

          let (clientConnPtr, clientConnStateOpt, dialErr) =
            msdriver.dialConnection(handle, LoopbackDialHost, listener.port)
          if dialErr.len > 0 or clientConnStateOpt.isNone:
            echo "MsQuic dial unavailable: ", dialErr
            skip()
          else:
            let clientConnState = clientConnStateOpt.get()
            let (serverConnPtr, serverConnStateOpt, acceptErr) =
              acceptPendingConnection(listener.state)
            if acceptErr.len > 0 or serverConnStateOpt.isNone:
              echo "MsQuic accept unavailable: ", acceptErr
              skip()
            else:
              let serverConnState = serverConnStateOpt.get()
              defer:
                discard msdriver.shutdownConnection(handle, serverConnPtr)
                msdriver.closeConnection(handle, serverConnPtr, serverConnState)
                discard msdriver.shutdownConnection(handle, clientConnPtr)
                msdriver.closeConnection(handle, clientConnPtr, clientConnState)

              discard nextConnectionEventOfKind(clientConnState, quicrt.qceConnected)
              discard nextConnectionEventOfKind(serverConnState, quicrt.qceConnected)

              let clientConn = newMsQuicConnection(
                handle,
                clientConnPtr,
                clientConnState,
                createPrimaryStream = true
              )
              defer:
                if not clientConn.isNil:
                  waitFor clientConn.closeImpl()

              let serverInboundFut = msdriver.awaitPendingStreamState(serverConnState)
              check waitFor serverInboundFut.withTimeout(3.seconds)
              let serverInbound = serverInboundFut.read()
              check serverInbound != nil

              let serverConn = newMsQuicConnection(
                handle,
                serverConnPtr,
                serverConnState,
                cast[pointer](serverInbound.stream),
                primaryStreamState = serverInbound,
                createPrimaryStream = true
              )
              defer:
                if not serverConn.isNil:
                  waitFor serverConn.closeImpl()

              check waitFor clientConn.writeLp("/multistream/1.0.0\n").withTimeout(3.seconds)
              let serverHandshake1 = serverConn.readLp(1024)
              check waitFor serverHandshake1.withTimeout(3.seconds)
              check bytesToString(serverHandshake1.read()) == "/multistream/1.0.0\n"

              check waitFor serverConn.writeLp("/multistream/1.0.0\n").withTimeout(3.seconds)
              let clientHandshake1 = clientConn.readLp(1024)
              check waitFor clientHandshake1.withTimeout(3.seconds)
              check bytesToString(clientHandshake1.read()) == "/multistream/1.0.0\n"

              check waitFor clientConn.writeLp("/test/proto/1.0.0\n").withTimeout(3.seconds)
              let serverProto = serverConn.readLp(1024)
              check waitFor serverProto.withTimeout(3.seconds)
              check bytesToString(serverProto.read()) == "/test/proto/1.0.0\n"

              check waitFor serverConn.writeLp("/test/proto/1.0.0\n").withTimeout(3.seconds)
              let clientProto = clientConn.readLp(1024)
              check waitFor clientProto.withTimeout(3.seconds)
              check bytesToString(clientProto.read()) == "/test/proto/1.0.0\n"

              check waitFor clientConn.writeLp("ping").withTimeout(3.seconds)
              let serverPayloadFut = serverConn.readLp(1024)
              check waitFor serverPayloadFut.withTimeout(3.seconds)
              check bytesToString(serverPayloadFut.read()) == "ping"
              check waitFor serverConn.writeLp("pong").withTimeout(3.seconds)
              let responseFut = clientConn.readLp(1024)
              check waitFor responseFut.withTimeout(3.seconds)
              check bytesToString(responseFut.read()) == "pong"

    test "locally initiated bidi stream survives multi-roundtrip control exchange":
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

                let serverInboundFut = msdriver.awaitPendingStreamState(serverState)
                check waitFor serverInboundFut.withTimeout(3.seconds)
                let serverInbound = serverInboundFut.read()
                check serverInbound != nil

                let clientSend1 = @[byte 0x11, 0x22]
                echo "multi-roundtrip: client send1"
                check (waitFor msdriver.writeStreamAndWait(clientStream, clientSend1)) == ""
                let serverRead1 = msdriver.readStream(serverInbound)
                echo "multi-roundtrip: server read1 wait"
                let serverRead1Ok = waitFor serverRead1.withTimeout(3.seconds)
                check serverRead1Ok
                if serverRead1Ok:
                  check serverRead1.read() == clientSend1

                  let serverSend1 = @[byte 0x31, 0x32]
                  let clientRead1 = msdriver.readStream(clientStream)
                  waitFor sleepAsync(150.milliseconds)
                  echo "multi-roundtrip: server send1"
                  check (waitFor msdriver.writeStreamAndWait(serverInbound, serverSend1)) == ""
                  echo "multi-roundtrip: client read1 wait"
                  let clientRead1Ok = waitFor clientRead1.withTimeout(3.seconds)
                  check clientRead1Ok
                  if clientRead1Ok:
                    check clientRead1.read() == serverSend1

                    let clientSend2 = @[byte 0x41, 0x42, 0x43]
                    echo "multi-roundtrip: client send2"
                    check (waitFor msdriver.writeStreamAndWait(clientStream, clientSend2)) == ""
                    let serverRead2 = msdriver.readStream(serverInbound)
                    echo "multi-roundtrip: server read2 wait"
                    let serverRead2Ok = waitFor serverRead2.withTimeout(3.seconds)
                    check serverRead2Ok
                    if serverRead2Ok:
                      check serverRead2.read() == clientSend2

                      let serverSend2 = @[byte 0x51, 0x52, 0x53, 0x54]
                      let clientRead2 = msdriver.readStream(clientStream)
                      waitFor sleepAsync(150.milliseconds)
                      echo "multi-roundtrip: server send2"
                      check (waitFor msdriver.writeStreamAndWait(serverInbound, serverSend2)) == ""
                      echo "multi-roundtrip: client read2 wait"
                      let clientRead2Ok = waitFor clientRead2.withTimeout(3.seconds)
                      check clientRead2Ok
                      if clientRead2Ok:
                        check clientRead2.read() == serverSend2

    test "out-of-order inbound stream frames are reassembled before delivery":
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

                let streamIdRes = msdriver.streamId(clientStream)
                check streamIdRes.isOk
                let streamId = streamIdRes.get()

                let trailing = proto.encodeStreamFrame(
                  streamId,
                  @[byte 0x43, 0x44],
                  2'u64,
                  false
                )
                check msapi.receiveOneRttPayloadForTest(
                  serverConnPtr,
                  100'u64,
                  trailing,
                  nil,
                  0'u16
                )

                let serverInboundFut = msdriver.awaitPendingStreamState(serverState)
                check waitFor serverInboundFut.withTimeout(3.seconds)
                let serverInbound = serverInboundFut.read()
                check serverInbound != nil

                let serverRead = msdriver.readStream(serverInbound)
                check not waitFor serverRead.withTimeout(200.milliseconds)

                let leading = proto.encodeStreamFrame(
                  streamId,
                  @[byte 0x41, 0x42],
                  0'u64,
                  false
                )
                check msapi.receiveOneRttPayloadForTest(
                  serverConnPtr,
                  101'u64,
                  leading,
                  nil,
                  0'u16
                )

                check waitFor serverRead.withTimeout(3.seconds)
                check serverRead.read() == @[byte 0x41, 0x42, 0x43, 0x44]

    test "MsQuicStream closeWrite flushes payload and EOF":
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

    test "large writeLp payload is segmented and reassembled end-to-end":
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
                var payload = newString(4096)
                for idx in 0 ..< payload.len:
                  payload[idx] = char(ord('a') + (idx mod 26))

                let writeFuture = clientStream.writeLp(payload)
                check waitFor writeFuture.withTimeout(3.seconds)

                let readLpFuture = serverStream.readLp(payload.len + 64)
                check waitFor readLpFuture.withTimeout(3.seconds)
                check bytesToString(readLpFuture.read()) == payload

    test "datagram send surfaces connection event":
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

              discard nextConnectionEventOfKind(connState, quicrt.qceConnected)
              discard nextConnectionEventOfKind(serverState, quicrt.qceConnected)

              check msapi.MsQuicEnableDatagramReceiveShim(
                cast[msapi.HQUIC](serverConnPtr), msapi.BOOLEAN(1)
              ) == msapi.QUIC_STATUS_SUCCESS
              check msapi.MsQuicEnableDatagramReceiveShim(
                cast[msapi.HQUIC](connPtr), msapi.BOOLEAN(1)
              ) == msapi.QUIC_STATUS_SUCCESS
              check msapi.MsQuicEnableDatagramSendShim(
                cast[msapi.HQUIC](connPtr), msapi.BOOLEAN(1)
              ) == msapi.QUIC_STATUS_SUCCESS

              var receiveEnabled = false
              var sendEnabled = false
              check msapi.getConnectionDatagramState(
                cast[msapi.HQUIC](connPtr), receiveEnabled, sendEnabled
              )
              check receiveEnabled
              check sendEnabled

              let err = msdriver.sendDatagram(handle, connPtr, @[byte 0xAA])
              check err.len == 0
              let (sendEvtOpt, sendEvtErr) =
                nextConnectionEventOfKind(connState, quicrt.qceParameterUpdated)
              check sendEvtErr.len == 0
              check sendEvtOpt.isSome
              if sendEvtOpt.isSome:
                check sendEvtOpt.get().paramId == msparam.QUIC_PARAM_CONN_DATAGRAM_SEND_ENABLED

              discard msdriver.shutdownConnection(handle, connPtr)
              msdriver.closeConnection(handle, connPtr, connState)

else:
  suite "MsQuic stream & datagram":
    test "experimental features disabled":
      skip("libp2p_msquic_experimental not enabled")
