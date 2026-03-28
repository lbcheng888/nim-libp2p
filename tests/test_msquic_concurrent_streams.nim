import std/[options, unittest]

import chronos

import ./msquic_test_helpers
import ../libp2p/transports/msquicdriver as msdriver
import ../libp2p/transports/msquicconnection
import ../libp2p/transports/msquicstream
import ../libp2p/transports/quicruntime as quicrt
import ../libp2p/stream/lpstream

when defined(libp2p_msquic_experimental):
  proc bytesToString(data: seq[byte]): string =
    if data.len == 0:
      return ""
    result = newString(data.len)
    copyMem(addr result[0], unsafeAddr data[0], data.len)

  proc openStartedStream(
      handle: msdriver.MsQuicTransportHandle,
      connPtr: pointer,
      connState: msdriver.MsQuicConnectionState
  ): tuple[streamPtr: pointer, state: msdriver.MsQuicStreamState, err: string] =
    let (streamPtr, streamStateOpt, streamErr) = msdriver.createStream(
      handle,
      connPtr,
      connectionState = connState
    )
    if streamErr.len > 0 or streamStateOpt.isNone or streamPtr.isNil:
      return (nil, nil, if streamErr.len > 0: streamErr else: "stream unavailable")
    let state = streamStateOpt.get()
    let startErr = msdriver.startStream(handle, streamPtr)
    if startErr.len > 0:
      return (nil, nil, startErr)
    let (startEventOpt, startEventErr) =
      nextStreamEventOfKind(state, quicrt.qseStartComplete)
    if startEventErr.len > 0 or startEventOpt.isNone:
      return (nil, nil, if startEventErr.len > 0: startEventErr else: "start event missing")
    (streamPtr, state, "")

  proc wrapStream(
      handle: msdriver.MsQuicTransportHandle,
      state: msdriver.MsQuicStreamState,
      dir: Direction
  ): MsQuicStream =
    newMsQuicStream(state, handle, dir)

  suite "MsQuic concurrent streams":
    test "protocol negotiation lock is per stream, not per connection":
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
                nil,
                true
              )
              defer:
                if not clientConn.isNil:
                  waitFor clientConn.closeImpl()

              let stream1 = clientConn.openMsQuicStream(false, Direction.Out)
              let stream2 = clientConn.openMsQuicStream(false, Direction.Out)
              defer:
                waitFor stream2.closeImpl()
                waitFor stream1.closeImpl()

              check waitFor stream1.beginProtocolNegotiation().withTimeout(1.seconds)
              let secondAcquire = stream2.beginProtocolNegotiation()
              check waitFor secondAcquire.withTimeout(250.milliseconds)
              stream2.endProtocolNegotiation()
              stream1.endProtocolNegotiation()

    test "concurrent identify and dm payload survive on same connection":
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

              let (clientIdentifyPtr, clientIdentifyState, clientIdentifyErr) =
                openStartedStream(handle, clientConnPtr, clientConnState)
              check clientIdentifyErr.len == 0
              if clientIdentifyErr.len > 0:
                skip()

              let (serverReverseIdentifyPtr, serverReverseIdentifyState, serverReverseIdentifyErr) =
                openStartedStream(handle, serverConnPtr, serverConnState)
              check serverReverseIdentifyErr.len == 0
              if serverReverseIdentifyErr.len > 0:
                skip()

              let (clientDmPtr, clientDmState, clientDmErr) =
                openStartedStream(handle, clientConnPtr, clientConnState)
              check clientDmErr.len == 0
              if clientDmErr.len > 0:
                skip()

              var serverIdentifyInState: msdriver.MsQuicStreamState = nil
              var clientReverseIdentifyInState: msdriver.MsQuicStreamState = nil
              var serverDmInState: msdriver.MsQuicStreamState = nil

              defer:
                if not clientIdentifyState.isNil:
                  msdriver.closeStream(handle, clientIdentifyPtr, clientIdentifyState)
                if not serverIdentifyInState.isNil:
                  msdriver.closeStream(handle, cast[pointer](serverIdentifyInState.stream), serverIdentifyInState)
                if not serverReverseIdentifyState.isNil:
                  msdriver.closeStream(handle, serverReverseIdentifyPtr, serverReverseIdentifyState)
                if not clientReverseIdentifyInState.isNil:
                  msdriver.closeStream(handle, cast[pointer](clientReverseIdentifyInState.stream), clientReverseIdentifyInState)
                if not clientDmState.isNil:
                  msdriver.closeStream(handle, clientDmPtr, clientDmState)
                if not serverDmInState.isNil:
                  msdriver.closeStream(handle, cast[pointer](serverDmInState.stream), serverDmInState)

              let clientIdentify = wrapStream(handle, clientIdentifyState, Direction.Out)
              let serverReverseIdentify = wrapStream(handle, serverReverseIdentifyState, Direction.Out)
              let clientDm = wrapStream(handle, clientDmState, Direction.Out)

              let identifyCodec = "/ipfs/id/1.0.0\n"
              let dmCodec = "/unimaker/dm/1.0.0\n"
              let dmPayload = "hello-concurrent-dm"

              let clientIdentifyWrite = clientIdentify.writeLp(identifyCodec)
              echo "concurrent-streams: client identify send"
              check waitFor clientIdentifyWrite.withTimeout(3.seconds)
              let serverIdentifyInFut = msdriver.awaitPendingStreamState(serverConnState)
              echo "concurrent-streams: wait server identify inbound"
              check waitFor serverIdentifyInFut.withTimeout(3.seconds)
              serverIdentifyInState = serverIdentifyInFut.read()
              check serverIdentifyInState != nil

              let serverReverseIdentifyWrite = serverReverseIdentify.writeLp(identifyCodec)
              echo "concurrent-streams: server reverse identify send"
              check waitFor serverReverseIdentifyWrite.withTimeout(3.seconds)
              let clientReverseIdentifyInFut = msdriver.awaitPendingStreamState(clientConnState)
              echo "concurrent-streams: wait client reverse identify inbound"
              check waitFor clientReverseIdentifyInFut.withTimeout(3.seconds)
              clientReverseIdentifyInState = clientReverseIdentifyInFut.read()
              check clientReverseIdentifyInState != nil

              let clientDmSelectWrite = clientDm.writeLp(dmCodec)
              echo "concurrent-streams: client dm select send"
              check waitFor clientDmSelectWrite.withTimeout(3.seconds)
              let serverDmInFut = msdriver.awaitPendingStreamState(serverConnState)
              echo "concurrent-streams: wait server dm inbound"
              check waitFor serverDmInFut.withTimeout(3.seconds)
              serverDmInState = serverDmInFut.read()
              check serverDmInState != nil

              let serverIdentifyIn = wrapStream(handle, serverIdentifyInState, Direction.In)
              let clientReverseIdentifyIn = wrapStream(handle, clientReverseIdentifyInState, Direction.In)
              let serverDmIn = wrapStream(handle, serverDmInState, Direction.In)

              let serverIdentifyReq = serverIdentifyIn.readLp(1024)
              let clientIdentifyReq = clientReverseIdentifyIn.readLp(1024)
              let serverDmSelect = serverDmIn.readLp(1024)
              echo "concurrent-streams: wait server identify read"
              check waitFor serverIdentifyReq.withTimeout(3.seconds)
              echo "concurrent-streams: wait client reverse identify read"
              check waitFor clientIdentifyReq.withTimeout(3.seconds)
              echo "concurrent-streams: wait server dm select read"
              check waitFor serverDmSelect.withTimeout(3.seconds)
              check bytesToString(serverIdentifyReq.read()) == identifyCodec
              check bytesToString(clientIdentifyReq.read()) == identifyCodec
              check bytesToString(serverDmSelect.read()) == dmCodec

              echo "concurrent-streams: send identify/dm select replies"
              check waitFor serverIdentifyIn.writeLp(identifyCodec).withTimeout(3.seconds)
              check waitFor clientReverseIdentifyIn.writeLp(identifyCodec).withTimeout(3.seconds)
              check waitFor serverDmIn.writeLp(dmCodec).withTimeout(3.seconds)

              let clientIdentifyAck = clientIdentify.readLp(1024)
              let serverReverseIdentifyAck = serverReverseIdentify.readLp(1024)
              let clientDmAck = clientDm.readLp(1024)
              echo "concurrent-streams: wait identify/dm select acks"
              check waitFor clientIdentifyAck.withTimeout(3.seconds)
              check waitFor serverReverseIdentifyAck.withTimeout(3.seconds)
              check waitFor clientDmAck.withTimeout(3.seconds)
              check bytesToString(clientIdentifyAck.read()) == identifyCodec
              check bytesToString(serverReverseIdentifyAck.read()) == identifyCodec
              check bytesToString(clientDmAck.read()) == dmCodec

              echo "concurrent-streams: client dm payload send"
              check waitFor clientDm.writeLp(dmPayload).withTimeout(3.seconds)
              echo "concurrent-streams: client dm closeWrite"
              check waitFor clientDm.closeWrite().withTimeout(3.seconds)

              let serverDmPayloadRead = serverDmIn.readLp(1024)
              echo "concurrent-streams: wait server dm payload"
              check waitFor serverDmPayloadRead.withTimeout(3.seconds)
              check bytesToString(serverDmPayloadRead.read()) == dmPayload

              let serverDmEof = serverDmIn.read()
              echo "concurrent-streams: wait server dm eof"
              check waitFor serverDmEof.withTimeout(3.seconds)
              expect LPStreamEOFError:
                discard serverDmEof.read()
else:
  suite "MsQuic concurrent streams":
    test "experimental features disabled":
      skip("libp2p_msquic_experimental not enabled")
