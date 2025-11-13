## MsQuic 适配包装层
##
## 该模块负责在现有 OpenSSL QUIC 适配层之上初始化 nim-msquic 的 API
## 蓝图，并对外暴露原有接口。后续若逐步将底层实现替换为 MsQuic，
## 可在此模块集中调整而无需影响上层调用者。

import std/[json, options, sequtils, strutils]

import chronicles
import chronicles/log_output as chronolog
import chronos/transports/common as chronoCommon
import results

import ./openssl_quic as legacy
import ./msquicruntime

when (NimMajor, NimMinor) >= (2, 0):
  import "nim-msquic/api/api_impl"
  import "nim-msquic/api/common"
  import "nim-msquic/api/event_model"
  import "nim-msquic/api/param_catalog"
else:
  {.error: "MsQuic wrapper requires Nim 2.0 or newer.".}

export legacy

logScope:
  topics = "libp2p msquicwrapper"

const
  msquicReadme* = staticRead("nim-msquic/README.md")
  msquicBanner* = block:
    let lines = msquicReadme.splitLines()
    if lines.len > 0: lines[0] else: "Nim MsQuic blueprint"

when not declared(defaultChroniclesStream):
  chronolog.logStream defaultChroniclesStream[
    textlines[stdout]
  ]

type
  MsQuicBlueprintState* = ref object
    registration*: HQUIC
    configuration*: HQUIC
    connection*: HQUIC
    alpns*: seq[string]
    alpnBuffers*: seq[QuicBuffer]
    serverName*: string
    serverPort*: uint16
    negotiatedAlpn*: string
    handshakeConnected*: bool
    datagramReceiveEnabled*: bool
    datagramSendEnabled*: bool
    events*: seq[ConnectionEventKind]
    bridge*: RuntimeBridge

  MsQuicSessionInfo* = object
    connected*: bool
    negotiatedAlpn*: string
    datagramReceiveEnabled*: bool
    datagramSendEnabled*: bool
    serverName*: string
    serverPort*: uint16
    events*: seq[ConnectionEventKind]

  MsQuicStateHook* = proc(state: MsQuicBlueprintState) {.raises: [Exception].}

var
  msquicApiTable: pointer
  msquicReady: bool
  msquicInitAttempted: bool

proc msquicTable(): ptr QuicApiTable =
  cast[ptr QuicApiTable](msquicApiTable)

proc shutdownMsQuic() {.noconv.} =
  ## 释放 MsQuic API 表引用。
  if msquicReady and not msquicApiTable.isNil:
    MsQuicClose(msquicApiTable)
    msquicApiTable = nil
    msquicReady = false

proc ensureMsQuicInitialized*(): bool {.gcsafe.} =
  ## 确保 nim-msquic 蓝图在启动阶段被引入。
  if not msquicInitAttempted:
    msquicInitAttempted = true
    var table: pointer
    let status = MsQuicOpenVersion(2'u32, addr table)
    if status == QUIC_STATUS_SUCCESS:
      msquicApiTable = table
      msquicReady = true
      addQuitProc(shutdownMsQuic)
      try:
        notice "MsQuic blueprint hooked", banner = msquicBanner
      except Exception:
        discard
    else:
      try:
        warn "MsQuic blueprint unavailable", status = cast[uint32](status)
      except Exception:
        discard
  result = msquicReady

discard ensureMsQuicInitialized()

proc blueprintEventRelay(event: ConnectionEvent) {.gcsafe.}

proc blueprintConnectionCallback(connection: HQUIC; context: pointer;
    ev: pointer): QUIC_STATUS {.cdecl.} =
  if context.isNil or ev.isNil:
    return QUIC_STATUS_INVALID_PARAMETER
  let state = cast[MsQuicBlueprintState](context)
  if state.isNil:
    return QUIC_STATUS_INVALID_PARAMETER
  let payload = cast[ptr QuicConnectionEvent](ev)
  if payload.Type == QUIC_CONNECTION_EVENT_CONNECTED:
    state.handshakeConnected = true
    let connected = cast[ptr QuicConnectionEventConnectedPayload](addr payload.Data[0])
    if connected.NegotiatedAlpnLength > 0'u8 and not connected.NegotiatedAlpn.isNil:
      state.negotiatedAlpn = newString(int(connected.NegotiatedAlpnLength))
      copyMem(addr state.negotiatedAlpn[0], connected.NegotiatedAlpn,
              state.negotiatedAlpn.len)
    else:
      state.negotiatedAlpn = ""
  QUIC_STATUS_SUCCESS

proc blueprintEventRelay(event: ConnectionEvent) {.gcsafe.} =
  let handle = cast[HQUIC](event.connection)
  if handle.isNil:
    return
  let ctx = MsQuicGetContextShim(handle)
  if ctx.isNil:
    return
  let state = cast[MsQuicBlueprintState](ctx)
  if state.isNil:
    return
  state.events.add(event.kind)
  case event.kind
  of ceConnected:
    state.handshakeConnected = true
    state.negotiatedAlpn = event.negotiatedAlpn
  of ceDatagramStateChanged:
    if event.paramId == QUIC_PARAM_CONN_DATAGRAM_RECEIVE_ENABLED:
      state.datagramReceiveEnabled = event.boolValue
    elif event.paramId == QUIC_PARAM_CONN_DATAGRAM_SEND_ENABLED:
      state.datagramSendEnabled = event.boolValue
  else:
    discard

proc releaseBlueprint(state: MsQuicBlueprintState) {.raises: [].} =
  if state.isNil:
    return
  let table = msquicTable()
  try:
    if not table.isNil:
      if not state.connection.isNil:
        try:
          clearConnectionEventHandlers(state.connection)
        except Exception:
          discard
        MsQuicSetContextShim(state.connection, nil)
        table.ConnectionShutdown(state.connection, 0'u32, 0'u64)
        table.ConnectionClose(state.connection)
        state.connection = nil
      if not state.configuration.isNil:
        table.ConfigurationClose(state.configuration)
        state.configuration = nil
      if not state.registration.isNil:
        table.RegistrationClose(state.registration)
        state.registration = nil
  except Exception:
    discard
  finally:
    if not state.bridge.isNil:
      releaseMsQuicBridge(state.bridge)
      state.bridge = nil

proc spawnBlueprintConnection(alpns: seq[string]; serverName: string;
    serverPort: uint16): Option[MsQuicBlueprintState] {.raises: [].} =
  if not ensureMsQuicInitialized():
    return none(MsQuicBlueprintState)
  let table = msquicTable()
  if table.isNil:
    return none(MsQuicBlueprintState)
  var state = MsQuicBlueprintState(
    registration: nil,
    configuration: nil,
    connection: nil,
    alpns: alpns,
    alpnBuffers: @[],
    serverName: serverName,
    serverPort: serverPort,
    negotiatedAlpn: "",
    handshakeConnected: false,
    datagramReceiveEnabled: false,
    datagramSendEnabled: false,
    events: @[],
    bridge: nil
  )
  let bridgeRes = acquireMsQuicBridge()
  if bridgeRes.success:
    state.bridge = bridgeRes.bridge
  else:
    state.bridge = nil
    if bridgeRes.error.len > 0:
      trace "MsQuic runtime unavailable", error = bridgeRes.error
  try:
    var registration: HQUIC
    var config = QuicRegistrationConfigC(
      AppName: cstring("nim-libp2p"),
      ExecutionProfile: 0'u32
    )
    let regStatus = table.RegistrationOpen(addr config, addr registration)
    if regStatus != QUIC_STATUS_SUCCESS or registration.isNil:
      try:
        warn "MsQuic registration open failed", status = regStatus
      except Exception:
        discard
      return none(MsQuicBlueprintState)
    state.registration = registration

    if state.alpns.len > 0:
      state.alpnBuffers = newSeq[QuicBuffer](state.alpns.len)
      for i, value in state.alpns:
        if value.len == 0:
          state.alpnBuffers[i] = QuicBuffer(Length: 0'u32, Buffer: nil)
        else:
          state.alpnBuffers[i] = QuicBuffer(
            Length: uint32(value.len),
            Buffer: cast[ptr uint8](state.alpns[i].cstring)
          )

    var configuration: HQUIC
    let alpnPtr =
      if state.alpnBuffers.len == 0: nil
      else: addr state.alpnBuffers[0]
    let cfgStatus = table.ConfigurationOpen(
      registration,
      alpnPtr,
      uint32(state.alpnBuffers.len),
      nil,
      0'u32,
      nil,
      addr configuration)
    if cfgStatus != QUIC_STATUS_SUCCESS or configuration.isNil:
      try:
        warn "MsQuic configuration open failed", status = cfgStatus
      except Exception:
        discard
      table.RegistrationClose(registration)
      return none(MsQuicBlueprintState)
    state.configuration = configuration
    var dummyCredential = 1'u32
    discard table.ConfigurationLoadCredential(configuration,
      cast[pointer](addr dummyCredential))

    var connection: HQUIC
    let connStatus = table.ConnectionOpen(
      registration,
      blueprintConnectionCallback,
      cast[pointer](state),
      addr connection)
    if connStatus != QUIC_STATUS_SUCCESS or connection.isNil:
      try:
        warn "MsQuic connection open failed", status = connStatus
      except Exception:
        discard
      table.ConfigurationClose(configuration)
      table.RegistrationClose(registration)
      return none(MsQuicBlueprintState)
    state.connection = connection
    MsQuicSetContextShim(connection, cast[pointer](state))
    try:
      registerConnectionEventHandler(connection, blueprintEventRelay)
    except Exception:
      discard

    let setCfgStatus = table.ConnectionSetConfiguration(connection, configuration)
    if setCfgStatus != QUIC_STATUS_SUCCESS:
      try:
        warn "MsQuic connection set configuration failed", status = setCfgStatus
      except Exception:
        discard
      releaseBlueprint(state)
      return none(MsQuicBlueprintState)

    let namePtr = if state.serverName.len == 0:
        nil
      else:
        state.serverName.cstring
    let startStatus = table.ConnectionStart(
      connection,
      configuration,
      QUIC_ADDRESS_FAMILY(2),
      namePtr,
      state.serverPort)
    if startStatus != QUIC_STATUS_SUCCESS:
      try:
        warn "MsQuic connection start failed", status = startStatus
      except Exception:
        discard
      releaseBlueprint(state)
      return none(MsQuicBlueprintState)
    some(state)
  except Exception:
    releaseBlueprint(state)
    none(MsQuicBlueprintState)

proc buildSessionInfo(state: MsQuicBlueprintState): MsQuicSessionInfo {.raises: [], gcsafe.} =
  MsQuicSessionInfo(
    connected: state.handshakeConnected,
    negotiatedAlpn: state.negotiatedAlpn,
    datagramReceiveEnabled: state.datagramReceiveEnabled,
    datagramSendEnabled: state.datagramSendEnabled,
    serverName: state.serverName,
    serverPort: state.serverPort,
    events: state.events
  )

proc registerMsQuicSession*(conn: legacy.Connection; alpns: seq[string]) {.raises: [].} =
  if conn.isNil or alpns.len == 0:
    return
  if not ensureMsQuicInitialized():
    return
  if not conn.msquicState.isNil:
    return
  let remote = conn.remoteAddress()
  let host = chronoCommon.host(remote)
  let rawPort = int(remote.port)
  let port = uint16(if rawPort < 0: 0 else: rawPort and 0xFFFF)
  let stateOpt = spawnBlueprintConnection(alpns, host, port)
  if stateOpt.isNone:
    try:
      warn "Failed to establish MsQuic blueprint session",
           host = host, port = port
    except Exception:
      discard
    return
  let state = stateOpt.get()
  conn.msquicState = cast[pointer](state)

proc unregisterMsQuicSession*(conn: legacy.Connection) {.raises: [].} =
  if conn.isNil:
    return
  let state = cast[MsQuicBlueprintState](conn.msquicState)
  if not state.isNil:
    releaseBlueprint(state)
  conn.msquicState = nil

proc msquicSessionInfo*(conn: legacy.Connection): Option[MsQuicSessionInfo] {.raises: [].} =
  if conn.isNil:
    return none(MsQuicSessionInfo)
  let state = cast[MsQuicBlueprintState](conn.msquicState)
  if state.isNil:
    return none(MsQuicSessionInfo)
  some(buildSessionInfo(state))

proc msquicSessionInfoToJson*(info: MsQuicSessionInfo): JsonNode =
  result = newJObject()
  result["connected"] = %info.connected
  if info.negotiatedAlpn.len > 0:
    result["negotiated_alpn"] = %info.negotiatedAlpn
  result["datagram_receive_enabled"] = %info.datagramReceiveEnabled
  result["datagram_send_enabled"] = %info.datagramSendEnabled
  if info.serverName.len > 0:
    result["server_name"] = %info.serverName
  result["server_port"] = %info.serverPort.int
  var eventsArr = newJArray()
  for kind in info.events:
    eventsArr.add(%($kind))
  result["events"] = eventsArr

proc msquicSessionInfoJson*(conn: legacy.Connection): JsonNode =
  let infoOpt = conn.msquicSessionInfo()
  if infoOpt.isSome:
    msquicSessionInfoToJson(infoOpt.get())
  else:
    newJNull()

proc msquicSessionConnected*(conn: legacy.Connection): bool {.raises: [].} =
  if conn.isNil:
    return false
  let state = cast[MsQuicBlueprintState](conn.msquicState)
  not state.isNil and state.handshakeConnected

proc newMsQuicTestSession*(alpns: seq[string]; serverName: string;
    serverPort: uint16; hook: MsQuicStateHook = nil): Result[MsQuicSessionInfo, string] {.raises: [].} =
  let stateOpt = spawnBlueprintConnection(alpns, serverName, serverPort)
  if stateOpt.isNone:
    return err("failed to start MsQuic blueprint session")
  let state = stateOpt.get()
  if not hook.isNil:
    try:
      hook(state)
    except Exception:
      discard
  let info = buildSessionInfo(state)
  releaseBlueprint(state)
  ok(info)
