import std/[options, unittest]

import chronos

import "../libp2p/transports/msquicdriver" as msdriver
import "../libp2p/transports/nim-msquic/api/event_model" as msevents

when defined(libp2p_msquic_experimental):
  suite "MsQuic listener lifecycle":
    test "listener start/stop emits stop event":
      let (handle, initErr) = msdriver.initMsQuicTransport()
      if initErr.len > 0 or handle.isNil:
        echo "MsQuic runtime unavailable: ", initErr
        skip()
      else:
        defer:
          if not handle.isNil:
            handle.shutdown()

        let (listenerPtr, listenerStateOpt, createErr) = handle.createListener()
        if createErr.len > 0 or listenerStateOpt.isNone:
          echo "MsQuic listener unavailable: ", createErr
          skip()
        else:
          let listenerState = listenerStateOpt.get()
          let startErr = handle.startListener(listenerPtr)
          check startErr.len == 0

          let stopErr = handle.stopListener(listenerPtr)
          check stopErr.len == 0
          let stopEvent = waitFor listenerState.nextListenerEvent()
          check stopEvent.kind == msevents.leStopComplete

          handle.closeListener(listenerPtr, listenerState)
          expect msdriver.MsQuicEventQueueClosed:
            discard waitFor listenerState.nextListenerEvent()
else:
  suite "MsQuic listener lifecycle":
    test "experimental features disabled":
      skip("libp2p_msquic_experimental not enabled")
