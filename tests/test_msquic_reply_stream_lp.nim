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

  proc expectLp(stream: MsQuicStream; expected: string; label: string) =
    let fut = stream.readLp(max(expected.len + 64, 1024))
    checkpoint(label)
    check waitFor fut.withTimeout(3.seconds)
    check bytesToString(fut.read()) == expected

  proc expectEof(stream: MsQuicStream; label: string) =
    let fut = stream.read()
    checkpoint(label)
    check waitFor fut.withTimeout(3.seconds)
    expect LPStreamEOFError:
      discard fut.read()

  suite "MsQuic reply inbound LP stream":
    test "server reply stream still delivers LP payload after prior inbound stream fully completes":
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

              let (clientReqPtr, clientReqState, clientReqErr) =
                openStartedStream(handle, clientConnPtr, clientConnState)
              check clientReqErr.len == 0
              if clientReqErr.len > 0:
                skip()
              else:
                let clientReq = wrapStream(handle, clientReqState, Direction.Out)
                defer:
                  waitFor clientReq.closeImpl()

                let msCodec = "/multistream/1.0.0\n"
                let dmCodec = "/unimaker/dm/1.0.0\n"

                checkpoint("client req write multistream")
                check waitFor clientReq.writeLp(msCodec).withTimeout(3.seconds)
                let serverReqFut = msdriver.awaitPendingStreamState(serverConnState)
                checkpoint("wait server request stream")
                check waitFor serverReqFut.withTimeout(3.seconds)
                let serverReqState = serverReqFut.read()
                check serverReqState != nil
                let serverReq = wrapStream(handle, serverReqState, Direction.In)
                defer:
                  waitFor serverReq.closeImpl()
                serverReq.expectLp(msCodec, "server req read multistream")
                checkpoint("server req ack multistream")
                check waitFor serverReq.writeLp(msCodec).withTimeout(3.seconds)
                clientReq.expectLp(msCodec, "client req read multistream ack")

                checkpoint("client req write dm codec")
                check waitFor clientReq.writeLp(dmCodec).withTimeout(3.seconds)
                serverReq.expectLp(dmCodec, "server req read dm codec")
                checkpoint("server req ack dm codec")
                check waitFor serverReq.writeLp(dmCodec).withTimeout(3.seconds)
                clientReq.expectLp(dmCodec, "client req read dm codec ack")

                let requestPayload = "dm-request-on-first-stream"
                checkpoint("client req write payload")
                check waitFor clientReq.writeLp(requestPayload).withTimeout(3.seconds)
                checkpoint("client req closeWrite")
                check waitFor clientReq.closeWrite().withTimeout(3.seconds)
                serverReq.expectLp(requestPayload, "server req read payload")
                serverReq.expectEof("server req eof after client closeWrite")

                checkpoint("server req closeWrite")
                check waitFor serverReq.closeWrite().withTimeout(3.seconds)
                clientReq.expectEof("client req eof after server closeWrite")

                let (serverReplyPtr, serverReplyState, serverReplyErr) =
                  openStartedStream(handle, serverConnPtr, serverConnState)
                check serverReplyErr.len == 0
                if serverReplyErr.len > 0:
                  skip()
                else:
                  let serverReply = wrapStream(handle, serverReplyState, Direction.Out)
                  defer:
                    waitFor serverReply.closeImpl()

                  checkpoint("server reply write multistream")
                  check waitFor serverReply.writeLp(msCodec).withTimeout(3.seconds)
                  let clientReplyFut = msdriver.awaitPendingStreamState(clientConnState)
                  checkpoint("wait client reply stream")
                  check waitFor clientReplyFut.withTimeout(3.seconds)
                  let clientReplyState = clientReplyFut.read()
                  check clientReplyState != nil
                  let clientReply = wrapStream(handle, clientReplyState, Direction.In)
                  defer:
                    waitFor clientReply.closeImpl()
                  clientReply.expectLp(msCodec, "client reply read multistream")
                  checkpoint("client reply ack multistream")
                  check waitFor clientReply.writeLp(msCodec).withTimeout(3.seconds)
                  serverReply.expectLp(msCodec, "server reply read multistream ack")

                  checkpoint("server reply write dm codec")
                  check waitFor serverReply.writeLp(dmCodec).withTimeout(3.seconds)
                  clientReply.expectLp(dmCodec, "client reply read dm codec")
                  checkpoint("client reply ack dm codec")
                  check waitFor clientReply.writeLp(dmCodec).withTimeout(3.seconds)
                  serverReply.expectLp(dmCodec, "server reply read dm codec ack")

                  waitFor sleepAsync(300.milliseconds)

                  let replyPayload = "dm-reply-on-second-stream"
                  checkpoint("server reply write payload")
                  check waitFor serverReply.writeLp(replyPayload).withTimeout(3.seconds)
                  checkpoint("server reply closeWrite")
                  check waitFor serverReply.closeWrite().withTimeout(3.seconds)
                  clientReply.expectLp(replyPayload, "client reply read payload")
                  clientReply.expectEof("client reply eof after server closeWrite")
else:
  suite "MsQuic reply inbound LP stream":
    test "experimental features disabled":
      skip("libp2p_msquic_experimental not enabled")
