import std/[options, unittest]

import chronos

import ./msquic_test_helpers
import "../libp2p/transports/msquicdriver" as msdriver
import "../libp2p/transports/quicruntime" as quicrt

when defined(libp2p_msquic_experimental):
  var
    connectionHandlerInvoked = false
    streamHandlerInvoked = false
    listenerHandlerInvoked = false
    lastConnectionEvent = quicrt.qceUnknown
    lastStreamEvent = quicrt.qseUnknown
    lastListenerEvent = quicrt.qleUnknown
    seenConnectionUserContext: pointer = nil
    seenStreamUserContext: pointer = nil
    seenListenerUserContext: pointer = nil

  proc onConnectionEvent(event: quicrt.QuicConnectionEvent) {.gcsafe.} =
    connectionHandlerInvoked = true
    lastConnectionEvent = event.kind
    seenConnectionUserContext = event.userContext

  proc onStreamEvent(event: quicrt.QuicStreamEvent) {.gcsafe.} =
    streamHandlerInvoked = true
    lastStreamEvent = event.kind
    seenStreamUserContext = event.userContext

  proc onListenerEvent(event: quicrt.QuicListenerEvent) {.gcsafe.} =
    listenerHandlerInvoked = true
    lastListenerEvent = event.kind
    seenListenerUserContext = event.userContext

  suite "QuicRuntime handler adapters":
    test "runtime-neutral handlers compile through dial/listen/stream wrappers":
      connectionHandlerInvoked = false
      streamHandlerInvoked = false
      listenerHandlerInvoked = false
      lastConnectionEvent = quicrt.qceUnknown
      lastStreamEvent = quicrt.qseUnknown
      lastListenerEvent = quicrt.qleUnknown
      seenConnectionUserContext = nil
      seenStreamUserContext = nil
      seenListenerUserContext = nil

      let (handle, initErr) = quicrt.initQuicRuntime()
      if initErr.len > 0 or handle.isNil:
        echo "QuicRuntime unavailable: ", initErr
        skip()
      else:
        defer:
          if not handle.isNil:
            quicrt.shutdown(handle)

        let listenerCtx = cast[pointer](0x1010)
        let connCtx = cast[pointer](0x2020)
        let streamCtx = cast[pointer](0x3030)
        let (listenerPtr, listenerStateOpt, listenerErr) = quicrt.createListener(
          handle,
          onListenerEvent,
          listenerCtx
        )
        if listenerErr.len > 0 or listenerStateOpt.isNone:
          echo "QuicRuntime listener unavailable: ", listenerErr
          skip()
        else:
          let listenerState = listenerStateOpt.get()
          check handle.startListener(listenerPtr).len == 0
          let addressRes = msdriver.getListenerAddress(handle, listenerPtr)
          if addressRes.isErr():
            echo "QuicRuntime listener address unavailable: ", addressRes.error
            skip()
          else:
            let (connPtr, connStateOpt, dialErr) = quicrt.dialConnection(
              handle,
              LoopbackDialHost,
              uint16(addressRes.get().port),
              onConnectionEvent,
              connCtx
            )
            if dialErr.len > 0 or connStateOpt.isNone:
              echo "QuicRuntime dial unavailable: ", dialErr
              skip()
            else:
              let connState = connStateOpt.get()
              let (serverConnPtr, serverStateOpt, acceptErr) =
                acceptPendingConnection(listenerState)
              if acceptErr.len > 0 or serverStateOpt.isNone:
                echo "QuicRuntime accept unavailable: ", acceptErr
                skip()
              else:
                let serverState = serverStateOpt.get()
                defer:
                  discard handle.shutdownConnection(serverConnPtr)
                  quicrt.closeConnection(handle, serverConnPtr, serverState)

                let (connectedOpt, connectedErr) =
                  nextConnectionEventOfKind(connState, quicrt.qceConnected)
                check connectedErr.len == 0
                check connectedOpt.isSome
                if connectedOpt.isSome:
                  check connectedOpt.get().userContext == connCtx

                let (streamPtr, streamStateOpt, streamErr) = quicrt.createStream(
                  handle,
                  connPtr,
                  onStreamEvent,
                  userContext = streamCtx,
                  connectionState = connState
                )
                if streamErr.len > 0 or streamStateOpt.isNone:
                  echo "QuicRuntime stream unavailable: ", streamErr
                  skip()
                else:
                  let streamState = streamStateOpt.get()
                  check handle.startStream(streamPtr).len == 0
                  let (startEventOpt, startEventErr) =
                    nextStreamEventOfKind(streamState, quicrt.qseStartComplete)
                  check startEventErr.len == 0
                  check startEventOpt.isSome
                  if startEventOpt.isSome:
                    check startEventOpt.get().userContext == streamCtx
                  quicrt.closeStream(handle, streamPtr, streamState)

                discard handle.shutdownConnection(connPtr)
                quicrt.closeConnection(handle, connPtr, connState)

            check handle.stopListener(listenerPtr).len == 0
            let (stopEventOpt, stopEventErr) =
              nextListenerEventOfKind(listenerState, quicrt.qleStopComplete)
            check stopEventErr.len == 0
            check stopEventOpt.isSome
            if stopEventOpt.isSome:
              check stopEventOpt.get().userContext == listenerCtx
            quicrt.closeListener(handle, listenerPtr, listenerState)

        check seenConnectionUserContext == connCtx or seenConnectionUserContext.isNil
        check seenStreamUserContext == cast[pointer](0x3030) or seenStreamUserContext.isNil
        check seenListenerUserContext == listenerCtx or seenListenerUserContext.isNil
        check lastConnectionEvent in {quicrt.qceUnknown, quicrt.qceConnected}
        check lastStreamEvent in {quicrt.qseUnknown, quicrt.qsePeerAccepted, quicrt.qseStartComplete}
        check lastListenerEvent in {quicrt.qleUnknown, quicrt.qleNewConnection, quicrt.qleStopComplete}
        discard connectionHandlerInvoked
        discard streamHandlerInvoked
        discard listenerHandlerInvoked
else:
  suite "QuicRuntime handler adapters":
    test "experimental features disabled":
      skip("libp2p_msquic_experimental not enabled")
