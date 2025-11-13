when not defined(libp2p_msquic_experimental):
  import std/[options]
  import chronos
  import results

  import ./msquicruntime
  import "nim-msquic/api/event_model" as msevents
  import "nim-msquic/api/tls_bridge" as mstls
  import "nim-msquic/tls/common" as mstlstypes

  type
    MsQuicLoadOptions* = msquicruntime.MsQuicLoadOptions
    MsQuicTransportConfig* = object
      loadOptions*: MsQuicLoadOptions
      alpns*: seq[string] = @["libp2p"]
      eventQueueLimit*: int = 0
      eventPollInterval*: Duration = 5.milliseconds
      appName*: string
      executionProfile*: uint32
    MsQuicTransportHandle* = ref object
      config*: MsQuicTransportConfig
      tlsBinding*: mstls.TlsCredentialBinding
    MsQuicConnectionHandler* = proc(event: msevents.ConnectionEvent) {.gcsafe.}
    MsQuicStreamHandler* = proc(event: msevents.StreamEvent) {.gcsafe.}
    MsQuicListenerHandler* = proc(event: msevents.ListenerEvent) {.gcsafe.}
    MsQuicConnectionState* = ref object
    MsQuicListenerState* = ref object
    MsQuicStreamState* = ref object
    MsQuicEventQueueClosed* = object of CatchableError

  const
    DefaultEventQueueLimit* = 0
    DefaultEventPollInterval* = 5.milliseconds

  proc initMsQuicTransport*(cfg: MsQuicTransportConfig = MsQuicTransportConfig()):
      tuple[handle: MsQuicTransportHandle, error: string] =
    (MsQuicTransportHandle(config: cfg), "MsQuic experimental runtime disabled")

  proc adoptConnection*(handle: MsQuicTransportHandle; connection: pointer;
      handler: MsQuicConnectionHandler; userContext: pointer = nil): string =
    "MsQuic experimental runtime disabled"

  proc createListener*(handle: MsQuicTransportHandle;
      handler: MsQuicListenerHandler = nil; userContext: pointer = nil;
      address: pointer = nil; queueLimit: int = 0;
      pollInterval: Duration = DefaultEventPollInterval):
      tuple[listener: pointer, state: Option[MsQuicListenerState], error: string] =
    (nil, none(MsQuicListenerState), "MsQuic experimental runtime disabled")

  proc dialConnection*(handle: MsQuicTransportHandle; serverName: string; port: uint16;
      handler: MsQuicConnectionHandler = nil; userContext: pointer = nil;
      addressFamily: uint16 = 0; queueLimit: int = 0;
      pollInterval: Duration = DefaultEventPollInterval):
      tuple[connection: pointer, state: Option[MsQuicConnectionState], error: string] =
    (nil, none(MsQuicConnectionState), "MsQuic experimental runtime disabled")

  proc createStream*(handle: MsQuicTransportHandle; connection: pointer;
      handler: MsQuicStreamHandler = nil;
      flags: uint32 = 0'u32; userContext: pointer = nil;
      connectionState: MsQuicConnectionState = nil;
      queueLimit: int = 0; pollInterval: Duration = DefaultEventPollInterval):
      tuple[stream: pointer, state: Option[MsQuicStreamState], error: string] =
    (nil, none(MsQuicStreamState), "MsQuic experimental runtime disabled")
  proc adoptStream*(handle: MsQuicTransportHandle; stream: pointer;
      connectionState: MsQuicConnectionState;
      handler: MsQuicStreamHandler = nil; userContext: pointer = nil;
      queueLimit: int = 0; pollInterval: Duration = DefaultEventPollInterval):
      tuple[state: Option[MsQuicStreamState], error: string] =
    (none(MsQuicStreamState), "MsQuic experimental runtime disabled")
  proc loadCredential*(handle: MsQuicTransportHandle; cfg: mstlstypes.TlsConfig;
      tempDir: string = ""): string =
    "MsQuic experimental runtime disabled"

  proc shutdownConnection*(handle: MsQuicTransportHandle; connection: pointer;
      flags: uint32 = 0'u32; errorCode: uint64 = 0'u64): string =
    "MsQuic experimental runtime disabled"

  proc sendDatagram*(handle: MsQuicTransportHandle; connection: pointer;
      payload: openArray[byte]; flags: uint32 = 0'u32;
      clientContext: pointer = nil): string =
    "MsQuic experimental runtime disabled"

  proc startStream*(handle: MsQuicTransportHandle; stream: pointer;
      flags: uint32 = 0'u32): string =
    "MsQuic experimental runtime disabled"

  proc closeStream*(handle: MsQuicTransportHandle; stream: pointer;
      state: MsQuicStreamState = nil) = discard

  proc startListener*(handle: MsQuicTransportHandle; listener: pointer;
      alpns: ptr pointer = nil; alpnCount: uint32 = 0;
      address: pointer = nil): string =
    "MsQuic experimental runtime disabled"

  proc stopListener*(handle: MsQuicTransportHandle; listener: pointer): string =
    "MsQuic experimental runtime disabled"

  proc nextListenerEvent*(state: MsQuicListenerState): Future[msevents.ListenerEvent] =
    let fut = Future[msevents.ListenerEvent].Raising(
      [CancelledError, MsQuicEventQueueClosed]
    ).init("msquic.listener.disabled")
    fut.fail(newException(MsQuicEventQueueClosed, "MsQuic experimental runtime disabled"))
    fut

  proc nextStreamEvent*(state: MsQuicStreamState): Future[msevents.StreamEvent] =
    let fut = Future[msevents.StreamEvent].Raising(
      [CancelledError, MsQuicEventQueueClosed]
    ).init(
      "msquic.stream.disabled"
    )
    fut.fail(newException(MsQuicEventQueueClosed, "MsQuic experimental runtime disabled"))
    fut

  proc pushStreamReceive*(state: MsQuicStreamState; payload: seq[byte]) =
    discard

  proc readStream*(state: MsQuicStreamState): Future[seq[byte]] =
    let fut = Future[seq[byte]].init("msquic.stream.read.disabled")
    fut.fail(newException(MsQuicEventQueueClosed, "MsQuic experimental runtime disabled"))
    fut

  proc writeStream*(state: MsQuicStreamState; data: seq[byte];
      flags: uint32 = 0'u32; clientContext: pointer = nil): string =
    "MsQuic experimental runtime disabled"
  proc streamId*(state: MsQuicStreamState): Result[uint64, string] =
    err("MsQuic experimental runtime disabled")

  proc closeConnection*(handle: MsQuicTransportHandle; connection: pointer;
      state: MsQuicConnectionState = nil) = discard

  proc closeListener*(handle: MsQuicTransportHandle; listener: pointer;
      state: MsQuicListenerState = nil) = discard

  proc attachIncomingConnection*(handle: MsQuicTransportHandle; connection: pointer;
      queueLimit: int = 0; pollInterval: Duration = DefaultEventPollInterval):
      tuple[state: Option[MsQuicConnectionState], error: string] =
    (none(MsQuicConnectionState), "MsQuic experimental runtime disabled")

  proc shutdown*(handle: MsQuicTransportHandle) = discard
else:
  include "msquicdriver_experimental.nim"
