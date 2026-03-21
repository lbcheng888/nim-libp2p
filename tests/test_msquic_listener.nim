import std/[options, unittest]

import chronos
import chronos/osdefs

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

    when defined(libp2p_msquic_builtin):
      test "builtin listener keeps separate IPv4 and IPv6 wildcard UDP listeners on one port":
        let (handle, initErr) = msdriver.initMsQuicTransport()
        if initErr.len > 0 or handle.isNil:
          echo "MsQuic runtime unavailable: ", initErr
          skip()
        else:
          defer:
            if not handle.isNil:
              msdriver.shutdown(handle)

          let (listener4Ptr, listener4StateOpt, create4Err) =
            msdriver.createListener(handle)
          if create4Err.len > 0 or listener4StateOpt.isNone:
            echo "MsQuic IPv4 listener unavailable: ", create4Err
            skip()
          else:
            let listener4State = listener4StateOpt.get()
            defer:
              discard msdriver.stopListener(handle, listener4Ptr)
              msdriver.closeListener(handle, listener4Ptr, listener4State)

            var storage4: Sockaddr_storage
            var len4: SockLen
            let bind4 = initTAddress("0.0.0.0", Port(0))
            toSAddr(bind4, storage4, len4)
            let start4Err = msdriver.startListener(
              handle,
              listener4Ptr,
              address = addr storage4
            )
            check start4Err.len == 0

            let addr4Res = msdriver.getListenerAddress(handle, listener4Ptr)
            check addr4Res.isOk()
            if addr4Res.isErr():
              skip()
            let addr4 = addr4Res.get()
            check addr4.family == AddressFamily.IPv4
            check int(addr4.port) > 0

            let (listener6Ptr, listener6StateOpt, create6Err) =
              msdriver.createListener(handle)
            check create6Err.len == 0
            check listener6StateOpt.isSome()
            if create6Err.len > 0 or listener6StateOpt.isNone:
              skip()
            let listener6State = listener6StateOpt.get()
            defer:
              discard msdriver.stopListener(handle, listener6Ptr)
              msdriver.closeListener(handle, listener6Ptr, listener6State)

            var storage6: Sockaddr_storage
            var len6: SockLen
            let bind6 = initTAddress("::", addr4.port)
            toSAddr(bind6, storage6, len6)
            let start6Err = msdriver.startListener(
              handle,
              listener6Ptr,
              address = addr storage6
            )
            check start6Err.len == 0

            let addr6Res = msdriver.getListenerAddress(handle, listener6Ptr)
            check addr6Res.isOk()
            if addr6Res.isErr():
              skip()
            let addr6 = addr6Res.get()
            check addr6.family == AddressFamily.IPv6
            check addr6.port == addr4.port
else:
  suite "MsQuic listener lifecycle":
    test "experimental features disabled":
      skip("libp2p_msquic_experimental not enabled")
