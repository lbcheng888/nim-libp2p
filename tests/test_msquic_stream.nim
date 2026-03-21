import std/[options, unittest]

import chronos

import ./msquic_test_helpers
import "../libp2p/transports/msquicdriver" as msdriver
import "../libp2p/transports/quicruntime" as quicrt
import "../libp2p/transports/nim-msquic/api/api_impl" as msapi
import "../libp2p/transports/nim-msquic/api/param_catalog" as msparam

when defined(libp2p_msquic_experimental):
  suite "MsQuic stream & datagram":
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
