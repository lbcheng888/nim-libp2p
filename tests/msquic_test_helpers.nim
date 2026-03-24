import std/[options]

import chronos
import results

import ../libp2p/transports/msquicdriver as msdriver
import ../libp2p/transports/quicruntime as quicrt

const LoopbackDialHost* = "127.0.0.1"
const DefaultMsQuicEventTimeout* = 2.seconds

type MsQuicLoopbackListener* = object
  listener*: pointer
  state*: msdriver.MsQuicListenerState
  port*: uint16

proc startLoopbackListener*(
    handle: msdriver.MsQuicTransportHandle
): tuple[listener: Option[MsQuicLoopbackListener], error: string] =
  let (listenerPtr, listenerStateOpt, createErr) = msdriver.createListener(handle)
  if createErr.len > 0 or listenerStateOpt.isNone:
    return (none(MsQuicLoopbackListener), createErr)

  let listenerState = listenerStateOpt.get()
  let startErr = msdriver.startListener(handle, listenerPtr)
  if startErr.len > 0:
    msdriver.closeListener(handle, listenerPtr, listenerState)
    return (none(MsQuicLoopbackListener), startErr)

  let addressRes = msdriver.getListenerAddress(handle, listenerPtr)
  if addressRes.isErr():
    discard msdriver.stopListener(handle, listenerPtr)
    msdriver.closeListener(handle, listenerPtr, listenerState)
    return (none(MsQuicLoopbackListener), addressRes.error)

  let listenAddr = addressRes.get()
  return (
    some(MsQuicLoopbackListener(
      listener: listenerPtr,
      state: listenerState,
      port: uint16(listenAddr.port)
    )),
    ""
  )

proc acceptPendingConnection*(
    listenerState: msdriver.MsQuicListenerState
): tuple[connection: pointer, state: Option[msdriver.MsQuicConnectionState], error: string] =
  let evtFuture = listenerState.nextQuicListenerEvent()
  if not waitFor evtFuture.withTimeout(DefaultMsQuicEventTimeout):
    evtFuture.cancelSoon()
    return (nil, none(msdriver.MsQuicConnectionState),
      "timed out waiting for incoming connection event")
  let evt = evtFuture.read()
  if evt.kind != quicrt.qleNewConnection or evt.connection.isNil:
    return (nil, none(msdriver.MsQuicConnectionState),
      "expected incoming connection event")

  let pending = msdriver.takePendingConnection(listenerState, evt.connection)
  if pending.isNone:
    return (nil, none(msdriver.MsQuicConnectionState),
      "missing pending connection state")

  (evt.connection, pending, "")

proc nextConnectionEventOfKind*(
    state: msdriver.MsQuicConnectionState,
    kind: quicrt.QuicConnectionEventKind,
    maxSkip: int = 8,
    timeout: Duration = DefaultMsQuicEventTimeout
): tuple[event: Option[quicrt.QuicConnectionEvent], error: string] =
  for _ in 0 ..< maxSkip:
    let evtFuture = state.nextQuicConnectionEvent()
    if not waitFor evtFuture.withTimeout(timeout):
      evtFuture.cancelSoon()
      return (none(quicrt.QuicConnectionEvent),
        "timed out waiting for connection event kind " & $kind)
    let evt = evtFuture.read()
    if evt.kind == kind:
      return (some(evt), "")
  (none(quicrt.QuicConnectionEvent), "expected connection event kind " & $kind)

proc nextStreamEventOfKind*(
    state: msdriver.MsQuicStreamState,
    kind: quicrt.QuicStreamEventKind,
    maxSkip: int = 8,
    timeout: Duration = DefaultMsQuicEventTimeout
): tuple[event: Option[quicrt.QuicStreamEvent], error: string] =
  for _ in 0 ..< maxSkip:
    let evtFuture = state.nextQuicStreamEvent()
    if not waitFor evtFuture.withTimeout(timeout):
      evtFuture.cancelSoon()
      return (none(quicrt.QuicStreamEvent),
        "timed out waiting for stream event kind " & $kind)
    let evt = evtFuture.read()
    if evt.kind == kind:
      return (some(evt), "")
  (none(quicrt.QuicStreamEvent), "expected stream event kind " & $kind)

proc nextListenerEventOfKind*(
    state: msdriver.MsQuicListenerState,
    kind: quicrt.QuicListenerEventKind,
    maxSkip: int = 8,
    timeout: Duration = DefaultMsQuicEventTimeout
): tuple[event: Option[quicrt.QuicListenerEvent], error: string] =
  for _ in 0 ..< maxSkip:
    let evtFuture = state.nextQuicListenerEvent()
    if not waitFor evtFuture.withTimeout(timeout):
      evtFuture.cancelSoon()
      return (none(quicrt.QuicListenerEvent),
        "timed out waiting for listener event kind " & $kind)
    let evt = evtFuture.read()
    if evt.kind == kind:
      return (some(evt), "")
  (none(quicrt.QuicListenerEvent), "expected listener event kind " & $kind)
