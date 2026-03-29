import std/[options, strutils, unittest]
import results

import chronos

import ./msquic_test_helpers
import "../libp2p/transports/msquicdriver" as msdriver
import "../libp2p/transports/quicruntime" as quicrt
import "../libp2p/transports/nim-msquic/api/api_impl" as msapi
import "../libp2p/transports/nim-msquic/api/param_catalog" as msparam

when defined(libp2p_msquic_experimental):
  suite "MsQuic experimental driver":
    test "dial lifecycle emits connection events":
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

          let (connPtr, stateOpt, dialErr) =
            msdriver.dialConnection(handle, LoopbackDialHost, listener.port)
          if dialErr.len > 0 or stateOpt.isNone:
            echo "MsQuic dial unavailable: ", dialErr
            skip()
          else:
            let connState = stateOpt.get()
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

              let (connectedOpt, connectedErr) =
                nextConnectionEventOfKind(connState, quicrt.qceConnected)
              check connectedErr.len == 0
              check connectedOpt.isSome

              let (serverConnectedOpt, serverConnectedErr) =
                nextConnectionEventOfKind(serverState, quicrt.qceConnected)
              check serverConnectedErr.len == 0
              check serverConnectedOpt.isSome

              discard msdriver.shutdownConnection(handle, connPtr, errorCode = 7'u64)
              msdriver.closeConnection(handle, connPtr, connState)
              expect quicrt.QuicRuntimeEventQueueClosed:
                discard waitFor connState.nextQuicConnectionEvent()

    test "dial fails after transport shutdown":
      let (handle, initErr) = msdriver.initMsQuicTransport()
      if initErr.len > 0 or handle.isNil:
        echo "MsQuic runtime unavailable: ", initErr
        skip()
      msdriver.shutdown(handle)

      let (_, stateOpt, dialErr) = msdriver.dialConnection(handle, "example.com", 1'u16)
      check dialErr.len > 0
      check stateOpt.isNone

    test "stream start event and datagram send succeed":
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

                let startErr = msdriver.startStream(handle, streamPtr)
                check startErr.len == 0
                let (startEventOpt, startEventErr) =
                  nextStreamEventOfKind(streamState, quicrt.qseStartComplete)
                check startEventErr.len == 0
                check startEventOpt.isSome

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

                let datagramErr = msdriver.sendDatagram(handle, connPtr, @[byte 0x1, 0x2, 0x3])
                check datagramErr.len == 0
                let (datagramEventOpt, datagramEventErr) =
                  nextConnectionEventOfKind(connState, quicrt.qceParameterUpdated)
                check datagramEventErr.len == 0
                check datagramEventOpt.isSome
                if datagramEventOpt.isSome:
                  check datagramEventOpt.get().paramId == msparam.QUIC_PARAM_CONN_DATAGRAM_SEND_ENABLED

                msdriver.closeStream(handle, streamPtr, streamState)

                discard msdriver.shutdownConnection(handle, connPtr)
                msdriver.closeConnection(handle, connPtr, connState)

    test "dial separates transport host from TLS server name":
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

          let (connPtr, stateOpt, dialErr) =
            msdriver.dialConnection(
              handle,
              "100.64.185.9",
              listener.port,
              transportHost = LoopbackDialHost
            )
          if dialErr.len > 0 or stateOpt.isNone:
            echo "MsQuic dial unavailable: ", dialErr
            skip()
          else:
            let connState = stateOpt.get()
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

              let remoteRes = msdriver.getConnectionRemoteAddress(handle, connPtr)
              check remoteRes.isOk()
              if remoteRes.isOk():
                let remote = remoteRes.get()
                check ($remote).startsWith(LoopbackDialHost & ":")
                check uint16(remote.port) == listener.port

              let (connectedOpt, connectedErr) =
                nextConnectionEventOfKind(connState, quicrt.qceConnected)
              check connectedErr.len == 0
              check connectedOpt.isSome

              let (serverConnectedOpt, serverConnectedErr) =
                nextConnectionEventOfKind(serverState, quicrt.qceConnected)
              check serverConnectedErr.len == 0
              check serverConnectedOpt.isSome
else:
  suite "MsQuic experimental driver":
    test "experimental features disabled":
      skip("libp2p_msquic_experimental not enabled")
