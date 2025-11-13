import std/[options, unittest]

import chronos

import "../libp2p/transports/msquicdriver" as msdriver
import "../libp2p/transports/nim-msquic/api/event_model" as msevents

when defined(libp2p_msquic_experimental):
  suite "MsQuic dial integration":
    test "successful dial emits connection events":
      let (handle, initErr) = msdriver.initMsQuicTransport()
      check initErr.len == 0
      check not handle.isNil
      defer:
        if not handle.isNil:
          handle.shutdown()

      let (connPtr, stateOpt, dialErr) = handle.dialConnection("dial.example", 2443'u16)
      check dialErr.len == 0
      check stateOpt.isSome
      let connState = stateOpt.get()

      let connected = waitFor connState.nextConnectionEvent()
      check connected.kind == msevents.ceConnected

      check handle.shutdownConnection(connPtr, errorCode = 42'u64).len == 0
      let shutdownInitiated = waitFor connState.nextConnectionEvent()
      check shutdownInitiated.kind == msevents.ceShutdownInitiated

      handle.closeConnection(connPtr, connState)
      let shutdownComplete = waitFor connState.nextConnectionEvent()
      check shutdownComplete.kind == msevents.ceShutdownComplete

      expect msdriver.MsQuicEventQueueClosed:
        discard waitFor connState.nextConnectionEvent()

    test "dial fails after transport shutdown":
      let (handle, initErr) = msdriver.initMsQuicTransport()
      check initErr.len == 0
      check not handle.isNil
      handle.shutdown()

      let (_, stateOpt, dialErr) = handle.dialConnection("after.shutdown", 1'u16)
      check dialErr.len > 0
      check stateOpt.isNone
else:
  suite "MsQuic dial integration":
    test "experimental features disabled":
      skip("libp2p_msquic_experimental not enabled")
