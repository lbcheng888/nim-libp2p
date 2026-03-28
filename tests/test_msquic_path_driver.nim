{.used.}

when defined(libp2p_msquic_experimental):
  import std/options
  import unittest2
  import results

  import ./msquic_test_helpers
  import ../libp2p/transports/msquicdriver as msdriver
  import ../libp2p/transports/quicruntime as quicrt

  suite "builtin QUIC path driver wrappers":
    test "path migration wrappers reject nil connection state":
      let nilState: msdriver.MsQuicConnectionState = nil

      check not msdriver.activeConnectionPathId(nilState).isOk()
      check not msdriver.knownConnectionPathCount(nilState).isOk()
      check not msdriver.connectionPathState(nilState, 0'u8).isOk()
      check not msdriver.triggerConnectionMigrationProbe(nilState, "127.0.0.1", 9446'u16).isOk()
      check not msdriver.confirmConnectionValidatedPath(nilState, 0'u8).isOk()

    test "path migration wrappers expose live loopback connection state":
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

              let activePathRes = msdriver.activeConnectionPathId(connState)
              check activePathRes.isOk()
              if activePathRes.isOk():
                check activePathRes.get() == 0'u8

              let pathCountRes = msdriver.knownConnectionPathCount(connState)
              check pathCountRes.isOk()
              if pathCountRes.isOk():
                check pathCountRes.get() >= 1'u8

              let activePathStateRes = msdriver.connectionPathState(connState, 0'u8)
              check activePathStateRes.isOk()
              if activePathStateRes.isOk():
                check activePathStateRes.get().active
                check activePathStateRes.get().validated

              let probeRes =
                msdriver.triggerConnectionMigrationProbe(connState, "127.0.0.1", listener.port + 1'u16)
              check probeRes.isOk()
              if probeRes.isOk():
                let candidatePathId = probeRes.get()
                check candidatePathId > 0'u8
                let candidatePathStateRes =
                  msdriver.connectionPathState(connState, candidatePathId)
                check candidatePathStateRes.isOk()
                if candidatePathStateRes.isOk():
                  check not candidatePathStateRes.get().active
                  check not candidatePathStateRes.get().validated
                  check candidatePathStateRes.get().challengeOutstanding
                  check candidatePathStateRes.get().responsePending

                let confirmRes =
                  msdriver.confirmConnectionValidatedPath(connState, candidatePathId)
                check confirmRes.isOk()

                let validatedPathStateRes =
                  msdriver.connectionPathState(connState, candidatePathId)
                check validatedPathStateRes.isOk()
                if validatedPathStateRes.isOk():
                  check validatedPathStateRes.get().active
                  check validatedPathStateRes.get().validated
                  check not validatedPathStateRes.get().challengeOutstanding
                  check not validatedPathStateRes.get().responsePending

else:
  discard
