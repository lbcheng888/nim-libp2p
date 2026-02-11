## Nim 端 MsQuic 事件模型，覆盖连接与流层常见回调。

type
  ConnectionEventKind* = enum
    ceConnected
    ceShutdownInitiated
    ceShutdownComplete
    cePeerStreamStarted
    ceDatagramStateChanged
    ceDatagramReceived
    ceSettingsApplied
    ceParameterUpdated
    ceUnknown

  ConnectionEvent* = object
    connection*: pointer
    kind*: ConnectionEventKind
    sessionResumed*: bool
    negotiatedAlpn*: string
    errorCode*: uint64
    status*: uint32
    paramId*: uint32
    boolValue*: bool
    maxSendLength*: uint16
    handshakeCompleted*: bool
    peerAcknowledgedShutdown*: bool
    appCloseInProgress*: bool
    userContext*: pointer
    uintValue*: uint64
    note*: string
    stream*: pointer
    streamFlags*: uint32
    streamIsUnidirectional*: bool
    datagramPayload*: seq[byte]
    datagramFlags*: uint32

  ConnectionEventHandler* = proc (event: ConnectionEvent) {.gcsafe.}

  StreamEventKind* = enum
    seStartComplete
    seReceive
    seSendComplete
    sePeerSendShutdown
    sePeerSendAborted
    sePeerReceiveAborted
    seSendShutdownComplete
    seShutdownComplete
    seIdealSendBufferSize
    seCancelOnLoss
    sePeerAccepted
    seReceiveBufferNeeded
    seUnknown

  StreamEvent* = object
    stream*: pointer
    kind*: StreamEventKind
    status*: uint32
    id*: uint64
    absoluteOffset*: uint64
    totalBufferLength*: uint64
    bufferCount*: uint32
    flags*: uint32
    cancelled*: bool
    peerAccepted*: bool
    graceful*: bool
    connectionShutdown*: bool
    appCloseInProgress*: bool
    connectionShutdownByApp*: bool
    connectionClosedRemotely*: bool
    connectionErrorCode*: uint64
    connectionCloseStatus*: uint32
    errorCode*: uint64
    clientContext*: pointer
    byteCount*: uint64
    bufferLengthNeeded*: uint64
    userContext*: pointer
    note*: string
    payload*: seq[byte]

  StreamEventHandler* = proc (event: StreamEvent) {.gcsafe.}

  ListenerEventKind* = enum
    leNewConnection
    leStopComplete
    leDosModeChanged
    leUnknown

  ListenerEvent* = object
    listener*: pointer
    kind*: ListenerEventKind
    connection*: pointer
    info*: pointer
    dosModeEnabled*: bool
    appCloseInProgress*: bool
    userContext*: pointer
    note*: string

  ListenerEventHandler* = proc (event: ListenerEvent) {.gcsafe.}
