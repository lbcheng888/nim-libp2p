import std/[options, unittest]

import chronos

import ./msquic_test_helpers
import ../libp2p/transports/msquicdriver as msdriver
import ../libp2p/transports/quicruntime as quicrt

when defined(libp2p_msquic_experimental):
  suite "MsQuic multi listener":
    test "same process can serve two builtin QUIC listeners with different ALPNs":
      let controlCfg = msdriver.MsQuicTransportConfig(
        alpns: @["nim-test-control"],
        appName: "nim-test-control"
      )
      let relayCfg = msdriver.MsQuicTransportConfig(
        alpns: @["nim-test-relay"],
        appName: "nim-test-relay"
      )

      let (controlHandle, controlErr) = msdriver.initMsQuicTransport(controlCfg)
      check controlErr.len == 0
      check not controlHandle.isNil
      if controlErr.len > 0 or controlHandle.isNil:
        skip()
      defer:
        msdriver.shutdown(controlHandle)

      let (relayHandle, relayErr) = msdriver.initMsQuicTransport(relayCfg)
      check relayErr.len == 0
      check not relayHandle.isNil
      if relayErr.len > 0 or relayHandle.isNil:
        skip()
      defer:
        msdriver.shutdown(relayHandle)

      let (controlListenerOpt, controlListenerErr) = startLoopbackListener(controlHandle)
      check controlListenerErr.len == 0
      check controlListenerOpt.isSome
      if controlListenerErr.len > 0 or controlListenerOpt.isNone:
        skip()
      let controlListener = controlListenerOpt.get()
      defer:
        discard msdriver.stopListener(controlHandle, controlListener.listener)
        msdriver.closeListener(controlHandle, controlListener.listener, controlListener.state)

      let (relayListenerOpt, relayListenerErr) = startLoopbackListener(relayHandle)
      check relayListenerErr.len == 0
      check relayListenerOpt.isSome
      if relayListenerErr.len > 0 or relayListenerOpt.isNone:
        skip()
      let relayListener = relayListenerOpt.get()
      defer:
        discard msdriver.stopListener(relayHandle, relayListener.listener)
        msdriver.closeListener(relayHandle, relayListener.listener, relayListener.state)

      let (controlConnPtr, controlConnStateOpt, controlDialErr) =
        msdriver.dialConnection(controlHandle, LoopbackDialHost, controlListener.port)
      check controlDialErr.len == 0
      check controlConnStateOpt.isSome
      if controlDialErr.len > 0 or controlConnStateOpt.isNone:
        skip()
      let controlConnState = controlConnStateOpt.get()
      defer:
        discard msdriver.shutdownConnection(controlHandle, controlConnPtr)
        msdriver.closeConnection(controlHandle, controlConnPtr, controlConnState)

      let (controlServerConnPtr, controlServerStateOpt, controlAcceptErr) =
        acceptPendingConnection(controlListener.state)
      check controlAcceptErr.len == 0
      check controlServerStateOpt.isSome
      if controlAcceptErr.len > 0 or controlServerStateOpt.isNone:
        skip()
      let controlServerState = controlServerStateOpt.get()
      defer:
        discard msdriver.shutdownConnection(controlHandle, controlServerConnPtr)
        msdriver.closeConnection(controlHandle, controlServerConnPtr, controlServerState)

      let (controlConnectedEvt, controlConnectedErr) =
        nextConnectionEventOfKind(controlConnState, quicrt.qceConnected)
      check controlConnectedErr.len == 0
      check controlConnectedEvt.isSome

      let (relayConnPtr, relayConnStateOpt, relayDialErr) =
        msdriver.dialConnection(relayHandle, LoopbackDialHost, relayListener.port)
      check relayDialErr.len == 0
      check relayConnStateOpt.isSome
      if relayDialErr.len > 0 or relayConnStateOpt.isNone:
        skip()
      let relayConnState = relayConnStateOpt.get()
      defer:
        discard msdriver.shutdownConnection(relayHandle, relayConnPtr)
        msdriver.closeConnection(relayHandle, relayConnPtr, relayConnState)

      let (relayServerConnPtr, relayServerStateOpt, relayAcceptErr) =
        acceptPendingConnection(relayListener.state)
      check relayAcceptErr.len == 0
      check relayServerStateOpt.isSome
      if relayAcceptErr.len > 0 or relayServerStateOpt.isNone:
        skip()
      let relayServerState = relayServerStateOpt.get()
      defer:
        discard msdriver.shutdownConnection(relayHandle, relayServerConnPtr)
        msdriver.closeConnection(relayHandle, relayServerConnPtr, relayServerState)

      let (relayConnectedEvt, relayConnectedErr) =
        nextConnectionEventOfKind(relayConnState, quicrt.qceConnected)
      check relayConnectedErr.len == 0
      check relayConnectedEvt.isSome
