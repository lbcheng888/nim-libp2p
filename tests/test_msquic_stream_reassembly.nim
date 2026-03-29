import std/[options, unittest]

import chronos

import ./msquic_test_helpers
import "../libp2p/transports/msquicdriver" as msdriver
import "../libp2p/transports/quicruntime" as quicrt
import "../libp2p/transports/nim-msquic/api/api_impl" as msapi
import "../libp2p/transports/nim-msquic/protocol/protocol_core" as proto

when defined(libp2p_msquic_experimental):
  suite "MsQuic stream reassembly":
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
                waitFor sleepAsync(200.milliseconds)
                check not serverRead.finished()

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

    test "delayed same-stream reply reassembles non-zero offsets after prior reply delivery":
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
                let request = @[byte 0x10]
                check (waitFor msdriver.writeStreamAndWait(clientStream, request)) == ""
                check waitFor serverInboundFut.withTimeout(3.seconds)
                let serverInbound = serverInboundFut.read()
                check serverInbound != nil
                let serverRead = msdriver.readStream(serverInbound)
                check waitFor serverRead.withTimeout(3.seconds)
                check serverRead.read() == request

                let reply1 = @[byte 0x21]
                check (waitFor msdriver.writeStreamAndWait(serverInbound, reply1)) == ""
                let clientRead1 = msdriver.readStream(clientStream)
                check waitFor clientRead1.withTimeout(3.seconds)
                check clientRead1.read() == reply1

                let streamIdRes = msdriver.streamId(clientStream)
                check streamIdRes.isOk
                let streamId = streamIdRes.get()

                let clientRead2 = msdriver.readStream(clientStream)
                waitFor sleepAsync(200.milliseconds)
                check not clientRead2.finished()

                let trailing = proto.encodeStreamFrame(
                  streamId,
                  @[byte 0x43, 0x44],
                  3'u64,
                  false
                )
                check msapi.receiveOneRttPayloadForTest(
                  connPtr,
                  200'u64,
                  trailing,
                  nil,
                  0'u16
                )

                waitFor sleepAsync(200.milliseconds)
                check not clientRead2.finished()

                let leading = proto.encodeStreamFrame(
                  streamId,
                  @[byte 0x41, 0x42],
                  1'u64,
                  false
                )
                check msapi.receiveOneRttPayloadForTest(
                  connPtr,
                  201'u64,
                  leading,
                  nil,
                  0'u16
                )

                check waitFor clientRead2.withTimeout(3.seconds)
                check clientRead2.read() == @[byte 0x41, 0x42, 0x43, 0x44]
else:
  suite "MsQuic stream reassembly":
    test "experimental features disabled":
      skip("libp2p_msquic_experimental not enabled")
