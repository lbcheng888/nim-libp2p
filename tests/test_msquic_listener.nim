import std/[options, unittest]

import chronos

import "../libp2p/transports/msquicdriver" as msdriver
import "../libp2p/transports/quicruntime" as quicrt

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
            msdriver.shutdown(handle)

        let (listenerPtr, listenerStateOpt, createErr) = msdriver.createListener(handle)
        if createErr.len > 0 or listenerStateOpt.isNone:
          echo "MsQuic listener unavailable: ", createErr
          skip()
        else:
          let listenerState = listenerStateOpt.get()
          let startErr = msdriver.startListener(handle, listenerPtr)
          check startErr.len == 0

          let stopErr = msdriver.stopListener(handle, listenerPtr)
          check stopErr.len == 0
          let stopEvent = waitFor listenerState.nextQuicListenerEvent()
          check stopEvent.kind == quicrt.qleStopComplete

          msdriver.closeListener(handle, listenerPtr, listenerState)
          expect quicrt.QuicRuntimeEventQueueClosed:
            discard waitFor listenerState.nextQuicListenerEvent()
else:
  suite "MsQuic listener lifecycle":
    test "experimental features disabled":
      skip("libp2p_msquic_experimental not enabled")
