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

  suite "MsQuic primary connection LP exchange":
    test "primary stream preserves sequential LP handshake and payload":
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

              echo "stage: client write handshake1"
              check waitFor clientConn.writeLp("/multistream/1.0.0\n").withTimeout(3.seconds)
              discard nextConnectionEventOfKind(
                serverConnState,
                quicrt.qcePeerStreamStarted,
                timeout = 3.seconds
              )
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

              echo "stage: server read handshake1"
              let serverHandshake1 = serverConn.readLp(1024)
              check waitFor serverHandshake1.withTimeout(3.seconds)
              check bytesToString(serverHandshake1.read()) == "/multistream/1.0.0\n"

              echo "stage: server write handshake1"
              check waitFor serverConn.writeLp("/multistream/1.0.0\n").withTimeout(3.seconds)
              echo "stage: client read handshake1"
              let clientHandshake1 = clientConn.readLp(1024)
              check waitFor clientHandshake1.withTimeout(3.seconds)
              check bytesToString(clientHandshake1.read()) == "/multistream/1.0.0\n"

              echo "stage: client write proto"
              check waitFor clientConn.writeLp("/test/proto/1.0.0\n").withTimeout(3.seconds)
              echo "stage: server read proto"
              let serverProto = serverConn.readLp(1024)
              check waitFor serverProto.withTimeout(3.seconds)
              check bytesToString(serverProto.read()) == "/test/proto/1.0.0\n"

              echo "stage: server write proto"
              check waitFor serverConn.writeLp("/test/proto/1.0.0\n").withTimeout(3.seconds)
              echo "stage: client read proto"
              let clientProto = clientConn.readLp(1024)
              check waitFor clientProto.withTimeout(3.seconds)
              check bytesToString(clientProto.read()) == "/test/proto/1.0.0\n"

              echo "stage: client write ping"
              check waitFor clientConn.writeLp("ping").withTimeout(3.seconds)
              echo "stage: server read ping"
              let serverPayload = serverConn.readLp(1024)
              check waitFor serverPayload.withTimeout(3.seconds)
              check bytesToString(serverPayload.read()) == "ping"

              echo "stage: server write pong"
              check waitFor serverConn.writeLp("pong").withTimeout(3.seconds)
              echo "stage: client read pong"
              let clientPayload = clientConn.readLp(1024)
              check waitFor clientPayload.withTimeout(3.seconds)
              check bytesToString(clientPayload.read()) == "pong"

    test "new inbound peer stream does not replace primary connection stream":
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

              check waitFor clientConn.writeLp("ping").withTimeout(3.seconds)
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

              let serverPing = serverConn.readLp(1024)
              check waitFor serverPing.withTimeout(3.seconds)
              check bytesToString(serverPing.read()) == "ping"

              let reverseStream = serverConn.openMsQuicStream(false, Direction.Out)
              defer:
                if not reverseStream.isNil:
                  waitFor reverseStream.closeImpl()
              check waitFor reverseStream.writeLp("reverse-stream").withTimeout(3.seconds)

              check waitFor serverConn.writeLp("pong").withTimeout(3.seconds)

              let clientPrimaryPayload = clientConn.readLp(1024)
              check waitFor clientPrimaryPayload.withTimeout(3.seconds)
              check bytesToString(clientPrimaryPayload.read()) == "pong"

              let clientPendingFut = msdriver.awaitPendingStreamState(clientConnState)
              check waitFor clientPendingFut.withTimeout(3.seconds)
              let clientReverseState = clientPendingFut.read()
              check clientReverseState != nil

              let clientReverse = newMsQuicStream(clientReverseState, handle, Direction.In)
              defer:
                if not clientReverse.isNil:
                  waitFor clientReverse.closeImpl()
              let clientReversePayload = clientReverse.readLp(1024)
              check waitFor clientReversePayload.withTimeout(3.seconds)
              check bytesToString(clientReversePayload.read()) == "reverse-stream"
else:
  import ./helpers
  suite "MsQuic primary connection LP exchange":
    test "experimental features disabled":
      skip("libp2p_msquic_experimental not enabled")
