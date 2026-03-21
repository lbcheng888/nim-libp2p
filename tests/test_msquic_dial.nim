import std/[options, unittest]

import chronos

import ./msquic_test_helpers
import "../libp2p/transports/msquicdriver" as msdriver
import "../libp2p/transports/quicruntime" as quicrt

when defined(libp2p_msquic_experimental):
  suite "MsQuic dial integration":
    test "successful dial emits connection events":
      let (handle, initErr) = msdriver.initMsQuicTransport()
      check initErr.len == 0
      check not handle.isNil
      defer:
        if not handle.isNil:
          msdriver.shutdown(handle)

      let (listenerOpt, listenerErr) = startLoopbackListener(handle)
      check listenerErr.len == 0
      check listenerOpt.isSome
      let listener = listenerOpt.get()
      defer:
        discard msdriver.stopListener(handle, listener.listener)
        msdriver.closeListener(handle, listener.listener, listener.state)

      let (connPtr, stateOpt, dialErr) =
        msdriver.dialConnection(handle, LoopbackDialHost, listener.port)
      check dialErr.len == 0
      check stateOpt.isSome
      let connState = stateOpt.get()

      let (serverConnPtr, serverStateOpt, acceptErr) =
        acceptPendingConnection(listener.state)
      check acceptErr.len == 0
      check serverStateOpt.isSome
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

      check msdriver.shutdownConnection(handle, connPtr, errorCode = 42'u64).len == 0
      msdriver.closeConnection(handle, connPtr, connState)
      expect quicrt.QuicRuntimeEventQueueClosed:
        discard waitFor connState.nextQuicConnectionEvent()

    test "dial fails after transport shutdown":
      let (handle, initErr) = msdriver.initMsQuicTransport()
      check initErr.len == 0
      check not handle.isNil
      msdriver.shutdown(handle)

      let (_, stateOpt, dialErr) = msdriver.dialConnection(handle, "after.shutdown", 1'u16)
      check dialErr.len > 0
      check stateOpt.isNone
else:
  suite "MsQuic dial integration":
    test "experimental features disabled":
      skip("libp2p_msquic_experimental not enabled")
