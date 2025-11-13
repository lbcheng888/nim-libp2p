# Nim-LibP2P
# Licensed under either of
#  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
#  * MIT license ([LICENSE-MIT](LICENSE-MIT))
# at your option.

import std/[options, tables, strutils]
import std/base64
import chronos, chronicles
import pkg/results

import ../../cid
import ../../peerid
import ../../stream/connection
import ./datatransfer as dt
import ./protobuf
import ./persistence

logScope:
  topics = "libp2p datatransfer channel"

type
  DataTransferChannelDirection* {.pure.} = enum
    dtcdInbound
    dtcdOutbound

  DataTransferChannelStatus* {.pure.} = enum
    dtcsRequested
    dtcsAccepted
    dtcsOngoing
    dtcsPaused
    dtcsCompleted
    dtcsCancelled
    dtcsFailed

  DataTransferEventKind* {.pure.} = enum
    dtekOpened
    dtekRequested
    dtekAccepted
    dtekOngoing
    dtekPaused
    dtekResumed
    dtekCompleted
    dtekCancelled
    dtekFailed
    dtekVoucherRejected

  DataTransferRetryPolicy* = object
    maxAttempts*: int
    retryDelay*: Duration

  DataTransferChannel* = object
    peerId*: PeerId
    transferId*: uint64
    direction*: DataTransferChannelDirection
    request*: DataTransferRequest
    extensions*: seq[DataTransferExtension]
    status*: DataTransferChannelStatus
    paused*: bool
    remotePaused*: bool
    retryPolicy*: DataTransferRetryPolicy
    lastStatus*: Option[string]
    lastError*: Option[string]
    createdAt*: Moment
    updatedAt*: Moment

  DataTransferChannelRef = ref DataTransferChannel

  DataTransferEvent* = object
    kind*: DataTransferEventKind
    channel*: DataTransferChannel
    message*: Option[string]

  DataTransferEventHandler* = proc(event: DataTransferEvent) {.gcsafe, raises: [].}

  DataTransferVoucherValidator* = proc(
      channel: DataTransferChannel, request: DataTransferRequest
  ): Future[bool] {.gcsafe, raises: [].}

  DataTransferResponseDecision* = object
    accept*: bool
    pause*: bool
    status*: Option[string]
    extensions*: seq[DataTransferExtension]

  DataTransferRequestHandler* = proc(
      channel: DataTransferChannel, request: DataTransferRequest
  ): Future[DataTransferResponseDecision] {.gcsafe, raises: [].}

  DataTransferChannelManager* = ref object
    dialer: DataTransferDialer
    channels: Table[string, DataTransferChannelRef]
    validators: Table[string, DataTransferVoucherValidator]
    requestHandler: DataTransferRequestHandler
    eventHandlers: seq[DataTransferEventHandler]
    lock: AsyncLock
    nextTransferId: uint64
    persistence: DataTransferChannelPersistence

  DataTransferStats* = object
    ## Aggregated view of the active data-transfer channels tracked by the manager.
    total*: int
    counts*: array[DataTransferChannelDirection, array[DataTransferChannelStatus, int]]
    pausedLocal*: array[DataTransferChannelDirection, int]
    pausedRemote*: array[DataTransferChannelDirection, int]

const
  DefaultMaxAttempts = 3
  DefaultRetryDelay = 500.milliseconds

proc releaseSafe(lock: AsyncLock) =
  try:
    lock.release()
  except AsyncLockError:
    discard

proc sendOutbound(
    manager: DataTransferChannelManager, channelRef: DataTransferChannelRef
): Future[void] {.async: (raises: [CancelledError, dt.DataTransferError]).}

proc init*(
    _: type DataTransferRetryPolicy,
    maxAttempts: int = DefaultMaxAttempts,
    retryDelay: Duration = DefaultRetryDelay,
): DataTransferRetryPolicy =
  let attempts = if maxAttempts < 1: 1 else: maxAttempts
  DataTransferRetryPolicy(maxAttempts: attempts, retryDelay: retryDelay)

proc accept*(
    _: type DataTransferResponseDecision,
    pause = false,
    status: Option[string] = none(string),
    extensions: seq[DataTransferExtension] = @[],
): DataTransferResponseDecision =
  DataTransferResponseDecision(
    accept: true, pause: pause, status: status, extensions: extensions
  )

proc reject*(
    _: type DataTransferResponseDecision,
    status: string,
    extensions: seq[DataTransferExtension] = @[],
): DataTransferResponseDecision =
  DataTransferResponseDecision(
    accept: false, pause: false, status: some(status), extensions: extensions
  )

proc channelKey(peerId: PeerId, transferId: uint64): string =
  $peerId & ":" & $transferId

proc snapshot(channel: DataTransferChannelRef): DataTransferChannel =
  channel[]

proc newChannelRef(channel: DataTransferChannel): DataTransferChannelRef =
  new(result)
  result[] = channel

proc bytesToBase64(data: seq[byte]): string =
  if data.len == 0:
    return ""
  var buf = newString(data.len)
  for i, b in data:
    buf[i] = char(b)
  base64.encode(buf)

proc base64ToBytes(data: string): Result[seq[byte], string] =
  if data.len == 0:
    return ok(newSeq[byte](0))
  try:
    let decoded = base64.decode(data)
    var bytes = newSeq[byte](decoded.len)
    for i, ch in decoded:
      bytes[i] = byte(ch)
    ok(bytes)
  except ValueError as exc:
    err("base64 decode error: " & exc.msg)

proc requestToRecord(req: DataTransferRequest): DataTransferRequestRecord =
  var baseCidOpt = none(string)
  req.baseCid.withValue(cid):
    baseCidOpt = some($cid)
  DataTransferRequestRecord(
    isPull: req.isPull,
    voucherType: req.voucherType,
    voucher: bytesToBase64(req.voucher),
    selector: bytesToBase64(req.selector),
    baseCid: baseCidOpt,
  )

proc extensionToRecord(ext: DataTransferExtension): DataTransferExtensionRecord =
  DataTransferExtensionRecord(name: ext.name, data: bytesToBase64(ext.data))

proc channelToRecord(channel: DataTransferChannel): DataTransferChannelRecord =
  var result = DataTransferChannelRecord(
    peerId: $channel.peerId,
    transferId: channel.transferId,
    direction: $channel.direction,
    status: $channel.status,
    paused: channel.paused,
    remotePaused: channel.remotePaused,
    retryMaxAttempts: channel.retryPolicy.maxAttempts,
    retryDelayNs: channel.retryPolicy.retryDelay.nanoseconds(),
    lastStatus: channel.lastStatus,
    lastError: channel.lastError,
    createdAtNs: channel.createdAt.epochNanoSeconds(),
    updatedAtNs: channel.updatedAt.epochNanoSeconds(),
    request: some(requestToRecord(channel.request)),
  )
  for ext in channel.extensions:
    result.extensions.add(extensionToRecord(ext))
  result

proc recordToRequest(record: DataTransferRequestRecord): Result[DataTransferRequest, string] =
  let voucherRes = base64ToBytes(record.voucher)
  if voucherRes.isErr:
    return err("invalid voucher data: " & voucherRes.error)
  let selectorRes = base64ToBytes(record.selector)
  if selectorRes.isErr:
    return err("invalid selector data: " & selectorRes.error)

  var baseCidOpt = none(Cid)
  record.baseCid.withValue(value):
    if value.len > 0:
      let cidRes = Cid.init(value)
      if cidRes.isErr:
        return err("invalid base cid: " & $cidRes.error)
      baseCidOpt = some(cidRes.get())

  ok(
    DataTransferRequest(
      isPull: record.isPull,
      voucherType: record.voucherType,
      voucher: voucherRes.get(),
      selector: selectorRes.get(),
      baseCid: baseCidOpt,
    )
  )

proc recordToChannel(
    record: DataTransferChannelRecord
): Result[DataTransferChannel, string] =
  if record.peerId.len == 0:
    return err("missing peer id")
  let peerRes = PeerId.init(record.peerId)
  if peerRes.isErr:
    return err("invalid peer id: " & $peerRes.error)

  if record.direction.len == 0:
    return err("missing direction")
  var direction: DataTransferChannelDirection
  try:
    direction = parseEnum[DataTransferChannelDirection](record.direction)
  except ValueError:
    return err("invalid direction: " & record.direction)

  if record.status.len == 0:
    return err("missing status")
  var status: DataTransferChannelStatus
  try:
    status = parseEnum[DataTransferChannelStatus](record.status)
  except ValueError:
    return err("invalid status: " & record.status)

  if record.request.isNone:
    return err("missing data transfer request")
  let requestRes = recordToRequest(record.request.get())
  if requestRes.isErr:
    return err(requestRes.error)

  var extensions: seq[DataTransferExtension] = @[]
  for ext in record.extensions:
    let dataRes = base64ToBytes(ext.data)
    if dataRes.isErr:
      return err("invalid extension data: " & dataRes.error)
    extensions.add(DataTransferExtension(name: ext.name, data: dataRes.get()))

  let attempts =
    if record.retryMaxAttempts < 1: DefaultMaxAttempts else: record.retryMaxAttempts
  let delayNs =
    if record.retryDelayNs <= 0: DefaultRetryDelay.nanoseconds() else: record.retryDelayNs
  let retryPolicy = DataTransferRetryPolicy(
    maxAttempts: attempts,
    retryDelay: chronos.nanoseconds(delayNs),
  )

  let createdAt =
    if record.createdAtNs != 0:
      Moment.low + chronos.nanoseconds(record.createdAtNs)
    else:
      Moment.now()
  let updatedAt =
    if record.updatedAtNs != 0:
      Moment.low + chronos.nanoseconds(record.updatedAtNs)
    else:
      createdAt

  ok(
    DataTransferChannel(
      peerId: peerRes.get(),
      transferId: record.transferId,
      direction: direction,
      request: requestRes.get(),
      extensions: extensions,
      status: status,
      paused: record.paused,
      remotePaused: record.remotePaused,
      retryPolicy: retryPolicy,
      lastStatus: record.lastStatus,
      lastError: record.lastError,
      createdAt: createdAt,
      updatedAt: updatedAt,
    )
  )

proc persistLocked(manager: DataTransferChannelManager) =
  if manager == nil or manager.persistence.isNil:
    return
  var records: seq[DataTransferChannelRecord] = @[]
  for channelRef in manager.channels.values:
    records.add(channelToRecord(channelRef[]))
  let state = DataTransferChannelState(
    nextTransferId: manager.nextTransferId,
    channels: records,
  )
  let res = manager.persistence.save(state)
  if res.isErr:
    warn "failed to persist data transfer channels", error = res.error

proc restoreFromPersistence(manager: DataTransferChannelManager) =
  if manager == nil or manager.persistence.isNil:
    return

  let stateRes = manager.persistence.load()
  if stateRes.isErr:
    warn "failed to load data transfer persistence", error = stateRes.error
    return

  manager.channels = initTable[string, DataTransferChannelRef]()
  var maxId: uint64 = 0

  let state = stateRes.get()
  for record in state.channels:
    let channelRes = recordToChannel(record)
    if channelRes.isErr:
      warn "skipping persisted data transfer channel", error = channelRes.error
      continue
    let channel = channelRes.get()
    let key = channelKey(channel.peerId, channel.transferId)
    manager.channels[key] = newChannelRef(channel)
    if channel.transferId > maxId:
      maxId = channel.transferId

  let candidate =
    if state.nextTransferId > 0: state.nextTransferId else: 1
  if candidate <= maxId:
    manager.nextTransferId = maxId + 1
  else:
    manager.nextTransferId = candidate

proc emitEvent(
    manager: DataTransferChannelManager,
    kind: DataTransferEventKind,
    channel: DataTransferChannel,
    message: Option[string],
) =
  if manager == nil or manager.eventHandlers.len == 0:
    return
  let handlers = manager.eventHandlers
  for handler in handlers:
    if handler == nil:
      continue
    try:
      handler(DataTransferEvent(kind: kind, channel: channel, message: message))
    except CatchableError as exc:
      debug "data transfer event handler raised", err = exc.msg,
        peerId = channel.peerId, transferId = channel.transferId, kind = kind

proc new*(
    _: type DataTransferChannelManager,
    dialer: DataTransferDialer,
    persistence: DataTransferChannelPersistence = nil,
): DataTransferChannelManager =
  if dialer == nil:
    raise newException(Defect, "data transfer channel manager requires a dialer")
  let manager = DataTransferChannelManager(
    dialer: dialer,
    channels: initTable[string, DataTransferChannelRef](),
    validators: initTable[string, DataTransferVoucherValidator](),
    requestHandler: nil,
    eventHandlers: @[],
    lock: newAsyncLock(),
    nextTransferId: 1,
    persistence: persistence,
  )
  manager.restoreFromPersistence()
  manager

proc setEventHandler*(
    manager: DataTransferChannelManager, handler: DataTransferEventHandler
) =
  if manager == nil:
    return
  manager.eventHandlers = @[]
  if handler != nil:
    manager.eventHandlers.add(handler)

proc addEventHandler*(
    manager: DataTransferChannelManager, handler: DataTransferEventHandler
) {.public.} =
  if manager == nil or handler == nil:
    return
  if manager.eventHandlers.len == 0:
    manager.eventHandlers = @[]
  for existing in manager.eventHandlers:
    if existing == handler:
      return
  manager.eventHandlers.add(handler)

proc removeEventHandler*(
    manager: DataTransferChannelManager, handler: DataTransferEventHandler
) {.public.} =
  if manager == nil or handler == nil or manager.eventHandlers.len == 0:
    return
  var idx = manager.eventHandlers.high
  while idx >= 0:
    if manager.eventHandlers[idx] == handler:
      manager.eventHandlers.delete(idx)
      break
    dec idx

proc setRequestHandler*(
    manager: DataTransferChannelManager, handler: DataTransferRequestHandler
) =
  if manager == nil:
    return
  manager.requestHandler = handler

proc registerVoucherValidator*(
    manager: DataTransferChannelManager, voucherType: string, validator: DataTransferVoucherValidator
) =
  if manager == nil or voucherType.len == 0 or validator == nil:
    return
  manager.validators[voucherType] = validator

proc unregisterVoucherValidator*(
    manager: DataTransferChannelManager, voucherType: string
) =
  if manager == nil or voucherType.len == 0:
    return
  manager.validators.del(voucherType)

proc nextId(manager: DataTransferChannelManager): uint64 =
  let id = manager.nextTransferId
  inc manager.nextTransferId
  if manager.nextTransferId == 0:
    manager.nextTransferId = 1
  id

proc markFailure(
    manager: DataTransferChannelManager,
    channelRef: DataTransferChannelRef,
    message: string,
) {.async: (raises: [CancelledError]).} =
  await manager.lock.acquire()
  defer:
    try:
      releaseSafe(manager.lock)
    except AsyncLockError:
      discard
  channelRef.status = DataTransferChannelStatus.dtcsFailed
  channelRef.lastError = some(message)
  channelRef.updatedAt = Moment.now()
  manager.persistLocked()
  let snap = snapshot(channelRef)
  emitEvent(manager, DataTransferEventKind.dtekFailed, snap, some(message))

proc markOngoing(
    manager: DataTransferChannelManager,
    channelRef: DataTransferChannelRef,
    message: Option[string],
) {.async: (raises: [CancelledError]).} =
  await manager.lock.acquire()
  defer:
    try:
      releaseSafe(manager.lock)
    except AsyncLockError:
      discard
  channelRef.status = DataTransferChannelStatus.dtcsOngoing
  channelRef.paused = false
  channelRef.remotePaused = false
  channelRef.updatedAt = Moment.now()
  manager.persistLocked()
  let snap = snapshot(channelRef)
  emitEvent(manager, DataTransferEventKind.dtekOngoing, snap, message)

proc handleResponse(
    manager: DataTransferChannelManager, peerId: PeerId, message: DataTransferMessage
) {.async: (raises: [CancelledError]).} =
  if message.response.isNone():
    return
  let response = message.response.get()

  await manager.lock.acquire()
  var released = false
  try:
    let key = channelKey(peerId, message.transferId)
    let channelRef = manager.channels.getOrDefault(key, nil)
    if channelRef.isNil:
      trace "data transfer response for unknown channel", peerId, transferId = message.transferId
      return
    channelRef.lastStatus = response.status
    channelRef.remotePaused = message.paused or response.pause
    channelRef.paused = channelRef.remotePaused
    channelRef.updatedAt = Moment.now()
    if not response.accepted:
      channelRef.status = DataTransferChannelStatus.dtcsFailed
      channelRef.lastError = response.status
      manager.persistLocked()
      let snap = snapshot(channelRef)
      try:
        releaseSafe(manager.lock)
      except AsyncLockError:
        discard
      released = true
      emitEvent(manager, DataTransferEventKind.dtekFailed, snap, response.status)
      return
    channelRef.lastError = none(string)
    if channelRef.remotePaused:
      channelRef.status = DataTransferChannelStatus.dtcsPaused
      manager.persistLocked()
      let snap = snapshot(channelRef)
      try:
        releaseSafe(manager.lock)
      except AsyncLockError:
        discard
      released = true
      emitEvent(manager, DataTransferEventKind.dtekPaused, snap, response.status)
      return
    channelRef.status =
      if channelRef.direction == DataTransferChannelDirection.dtcdOutbound:
        DataTransferChannelStatus.dtcsOngoing
      else:
        DataTransferChannelStatus.dtcsAccepted
    manager.persistLocked()
    let snap = snapshot(channelRef)
    releaseSafe(manager.lock)
    released = true
    let kind =
      if snap.status == DataTransferChannelStatus.dtcsOngoing:
        DataTransferEventKind.dtekOngoing
      else:
        DataTransferEventKind.dtekAccepted
    emitEvent(manager, kind, snap, response.status)
  finally:
    if not released:
      try:
        releaseSafe(manager.lock)
      except AsyncLockError:
        discard

proc handleControl(
    manager: DataTransferChannelManager, peerId: PeerId, message: DataTransferMessage
) {.async: (raises: [CancelledError]).} =
  if message.control.isNone():
    return
  let control = message.control.get()

  await manager.lock.acquire()
  var released = false
  try:
    let key = channelKey(peerId, message.transferId)
    let channelRef = manager.channels.getOrDefault(key, nil)
    if channelRef.isNil:
      trace "data transfer control for unknown channel", peerId, transferId = message.transferId
      return
    channelRef.lastStatus = control.status
    channelRef.updatedAt = Moment.now()
    var eventKind: DataTransferEventKind
    let isOutbound = channelRef.direction == DataTransferChannelDirection.dtcdOutbound
    var triggerRestart = false
    case control.controlType
    of DataTransferControlType.dtctPause:
      channelRef.status = DataTransferChannelStatus.dtcsPaused
      channelRef.paused = true
      channelRef.remotePaused = true
      eventKind = DataTransferEventKind.dtekPaused
    of DataTransferControlType.dtctResume:
      channelRef.status = DataTransferChannelStatus.dtcsOngoing
      channelRef.paused = false
      channelRef.remotePaused = false
      eventKind = DataTransferEventKind.dtekResumed
    of DataTransferControlType.dtctCancel:
      channelRef.status = DataTransferChannelStatus.dtcsCancelled
      channelRef.paused = false
      channelRef.remotePaused = false
      eventKind = DataTransferEventKind.dtekCancelled
    of DataTransferControlType.dtctComplete:
      channelRef.status = DataTransferChannelStatus.dtcsCompleted
      channelRef.paused = false
      channelRef.remotePaused = false
      eventKind = DataTransferEventKind.dtekCompleted
    of DataTransferControlType.dtctRestart, DataTransferControlType.dtctOpen:
      channelRef.status = DataTransferChannelStatus.dtcsRequested
      eventKind = DataTransferEventKind.dtekRequested
      triggerRestart = isOutbound
    else:
      return
    manager.persistLocked()
    let snap = snapshot(channelRef)
    releaseSafe(manager.lock)
    released = true
    emitEvent(manager, eventKind, snap, control.status)
    if triggerRestart:
      asyncSpawn sendOutbound(manager, channelRef)
  finally:
    if not released:
      try:
        releaseSafe(manager.lock)
      except AsyncLockError:
        discard

proc handleError(
    manager: DataTransferChannelManager, peerId: PeerId, message: DataTransferMessage
) {.async: (raises: [CancelledError]).} =
  if message.error.isNone():
    return
  let err = message.error.get()
  let description =
    if err.message.len > 0:
      $(err.code) & ": " & err.message
    else:
      $(err.code)
  await manager.lock.acquire()
  var released = false
  try:
    let key = channelKey(peerId, message.transferId)
    let channelRef = manager.channels.getOrDefault(key, nil)
    if channelRef.isNil:
      trace "data transfer error for unknown channel", peerId, transferId = message.transferId
      return
    channelRef.status = DataTransferChannelStatus.dtcsFailed
    channelRef.lastError = some(description)
    channelRef.updatedAt = Moment.now()
    manager.persistLocked()
    let snap = snapshot(channelRef)
    releaseSafe(manager.lock)
    released = true
    emitEvent(manager, DataTransferEventKind.dtekFailed, snap, some(description))
  finally:
    if not released:
      try:
        releaseSafe(manager.lock)
      except AsyncLockError:
        discard

proc evaluateInbound(
    manager: DataTransferChannelManager,
    channel: DataTransferChannel,
    request: DataTransferRequest,
): Future[DataTransferResponseDecision] {.
    async: (raises: [CancelledError, dt.DataTransferError])
.} =
  let validator = manager.validators.getOrDefault(request.voucherType, nil)
  if validator != nil:
    var valid = false
    try:
      valid = await validator(channel, request)
    except CancelledError as exc:
      raise exc
    except CatchableError as exc:
      debug "data transfer voucher validator failed",
        peerId = channel.peerId, transferId = channel.transferId, err = exc.msg
      valid = false
    if not valid:
      return DataTransferResponseDecision(
        accept: false,
        pause: false,
        status: some("voucher rejected"),
        extensions: @[]
      )
  if manager.requestHandler != nil:
    try:
      return await manager.requestHandler(channel, request)
    except CatchableError as exc:
      raise dt.newDataTransferError(
        "data transfer request handler failed: " & exc.msg,
        DataTransferErrorKind.dtekConfig,
        exc,
      )
  return DataTransferResponseDecision(
    accept: true,
    pause: false,
    status: none(string),
    extensions: @[]
  )

proc handleIncomingRequest(
    manager: DataTransferChannelManager, peerId: PeerId, message: DataTransferMessage
): Future[Option[DataTransferMessage]] {.
    async: (raises: [CancelledError, dt.DataTransferError])
.} =
  if message.request.isNone():
    trace "data transfer request missing payload", peerId, transferId = message.transferId
    return none(DataTransferMessage)
  let request = message.request.get()
  let now = Moment.now()

  await manager.lock.acquire()
  var released = false
  var isNew = false
  var channelRef: DataTransferChannelRef
  try:
    let key = channelKey(peerId, message.transferId)
    channelRef = manager.channels.getOrDefault(key, nil)
    if not channelRef.isNil:
      channelRef.request = request
      channelRef.extensions = message.extensions
      channelRef.status = DataTransferChannelStatus.dtcsRequested
      channelRef.paused = message.paused
      channelRef.remotePaused = message.paused
      channelRef.direction = DataTransferChannelDirection.dtcdInbound
      channelRef.updatedAt = now
    else:
      isNew = true
      channelRef = DataTransferChannelRef(
        peerId: peerId,
        transferId: message.transferId,
        direction: DataTransferChannelDirection.dtcdInbound,
        request: request,
        extensions: message.extensions,
        status: DataTransferChannelStatus.dtcsRequested,
        paused: message.paused,
        remotePaused: message.paused,
        retryPolicy: DataTransferRetryPolicy.init(),
        lastStatus: none(string),
        lastError: none(string),
        createdAt: now,
        updatedAt: now,
      )
      manager.channels[key] = channelRef
    manager.persistLocked()
    let snap = snapshot(channelRef)
    releaseSafe(manager.lock)
    released = true
    if isNew:
      emitEvent(manager, DataTransferEventKind.dtekOpened, snap, none(string))
    let decision = await manager.evaluateInbound(snap, request)

    await manager.lock.acquire()
    released = false
    channelRef.lastStatus = decision.status
    channelRef.paused = decision.pause
    channelRef.remotePaused = decision.pause
    channelRef.updatedAt = Moment.now()
    if not decision.accept:
      channelRef.status = DataTransferChannelStatus.dtcsFailed
      channelRef.lastError = decision.status
    else:
      channelRef.status =
        if decision.pause:
          DataTransferChannelStatus.dtcsPaused
        else:
          DataTransferChannelStatus.dtcsAccepted
      channelRef.lastError = none(string)
    manager.persistLocked()
    let finalSnap = snapshot(channelRef)
    releaseSafe(manager.lock)
    released = true

    let eventKind =
      if not decision.accept:
        DataTransferEventKind.dtekVoucherRejected
      elif decision.pause:
        DataTransferEventKind.dtekPaused
      else:
        DataTransferEventKind.dtekAccepted
    emitEvent(manager, eventKind, finalSnap, decision.status)

    let response = DataTransferResponse(
      accepted: decision.accept, status: decision.status, pause: decision.pause
    )
    let responseMsg = DataTransferMessage(
      transferId: message.transferId,
      paused: decision.pause,
      kind: DataTransferMessageKind.dtmkResponse,
      response: some(response),
      extensions: decision.extensions,
    )
    return some(responseMsg)
  finally:
    if not released:
      try:
        releaseSafe(manager.lock)
      except AsyncLockError:
        discard

proc sendOutbound(
    manager: DataTransferChannelManager, channelRef: DataTransferChannelRef
) {.async: (raises: [CancelledError, dt.DataTransferError]).} =
  await manager.lock.acquire()
  var released = false
  try:
    channelRef.status = DataTransferChannelStatus.dtcsRequested
    channelRef.paused = false
    channelRef.remotePaused = false
    channelRef.lastError = none(string)
    channelRef.lastStatus = none(string)
    channelRef.updatedAt = Moment.now()
    manager.persistLocked()
    let snap = snapshot(channelRef)
    releaseSafe(manager.lock)
    released = true
    emitEvent(manager, DataTransferEventKind.dtekRequested, snap, none(string))
  finally:
    if not released:
      try:
        releaseSafe(manager.lock)
      except AsyncLockError:
        discard

  let attempts = max(channelRef.retryPolicy.maxAttempts, 1)
  let delay = channelRef.retryPolicy.retryDelay
  let message = DataTransferMessage(
    transferId: channelRef.transferId,
    paused: false,
    kind: DataTransferMessageKind.dtmkRequest,
    request: some(channelRef.request),
    extensions: channelRef.extensions,
  )

  try:
    let responseOpt = await sendDataTransfer(
      manager.dialer,
      channelRef.peerId,
      message,
      maxAttempts = attempts,
      retryDelay = delay,
    )
    if responseOpt.isSome():
      await manager.handleResponse(channelRef.peerId, responseOpt.get())
    else:
      await manager.markOngoing(channelRef, none(string))
  except dt.DataTransferError as exc:
    await manager.markFailure(channelRef, exc.msg)
    raise exc

proc sendControlMessage(
    manager: DataTransferChannelManager,
    channelRef: DataTransferChannelRef,
    controlType: DataTransferControlType,
    status: Option[string],
    extensions: seq[DataTransferExtension],
    pausedFlag: bool,
): Future[void] {.async: (raises: [CancelledError, dt.DataTransferError]).} =
  let attempts = max(channelRef.retryPolicy.maxAttempts, 1)
  let delay = channelRef.retryPolicy.retryDelay
  let control = DataTransferControl(controlType: controlType, status: status)
  let message = DataTransferMessage(
    transferId: channelRef.transferId,
    paused: pausedFlag,
    kind: DataTransferMessageKind.dtmkControl,
    control: some(control),
    extensions: extensions,
  )
  try:
    discard await sendDataTransfer(
      manager.dialer,
      channelRef.peerId,
      message,
      expectResponse = false,
      maxAttempts = attempts,
      retryDelay = delay,
    )
  except dt.DataTransferError as exc:
    trace "failed to send control message",
      peerId = channelRef.peerId,
      transferId = channelRef.transferId,
      controlType = $controlType,
      err = exc.msg
    raise exc

proc performLocalControl(
    manager: DataTransferChannelManager,
    peerId: PeerId,
    transferId: uint64,
    controlType: DataTransferControlType,
    nextStatus: DataTransferChannelStatus,
    pausedValue: bool,
    remotePausedUpdate: Option[bool],
    status: Option[string],
    extensions: seq[DataTransferExtension],
    pausedFlag: bool,
    eventKind: DataTransferEventKind,
): Future[(Option[DataTransferChannel], DataTransferChannelRef)] {.
    async: (raises: [CancelledError, dt.DataTransferError])
.} =
  if manager == nil:
    return (none(DataTransferChannel), nil)

  await manager.lock.acquire()
  var released = false
  var channelRef: DataTransferChannelRef
  var previous: DataTransferChannel
  try:
    let key = channelKey(peerId, transferId)
    let channelRefOpt = manager.channels.getOrDefault(key, nil)
    if channelRefOpt.isNil:
      releaseSafe(manager.lock)
      return (none(DataTransferChannel), nil)
    channelRef = channelRefOpt
    previous = snapshot(channelRef)
    channelRef.status = nextStatus
    channelRef.paused = pausedValue
    remotePausedUpdate.withValue(flag):
      channelRef.remotePaused = flag
    channelRef.lastStatus = status
    channelRef.lastError = status
    channelRef.updatedAt = Moment.now()
    manager.persistLocked()
    releaseSafe(manager.lock)
    released = true
  finally:
    if not released:
      try:
        releaseSafe(manager.lock)
      except AsyncLockError:
        discard

  try:
    await manager.sendControlMessage(
      channelRef,
      controlType,
      status,
      extensions,
      pausedFlag = pausedFlag,
    )
  except dt.DataTransferError as exc:
    await manager.lock.acquire()
    var rollbackReleased = false
    try:
      channelRef.status = previous.status
      channelRef.paused = previous.paused
      channelRef.remotePaused = previous.remotePaused
      channelRef.lastStatus = previous.lastStatus
      channelRef.lastError = previous.lastError
      channelRef.updatedAt = Moment.now()
      manager.persistLocked()
      releaseSafe(manager.lock)
      rollbackReleased = true
    finally:
      if not rollbackReleased:
        try:
          releaseSafe(manager.lock)
        except AsyncLockError:
          discard
    raise exc

  await manager.lock.acquire()
  var snapshotReleased = false
  var result: DataTransferChannel
  try:
    channelRef.updatedAt = Moment.now()
    manager.persistLocked()
    result = snapshot(channelRef)
    releaseSafe(manager.lock)
    snapshotReleased = true
  finally:
    if not snapshotReleased:
      try:
        releaseSafe(manager.lock)
      except AsyncLockError:
        discard

  emitEvent(manager, eventKind, result, status)
  (some(result), channelRef)

proc pauseChannel*(
    manager: DataTransferChannelManager,
    peerId: PeerId,
    transferId: uint64,
    status: Option[string] = none(string),
    extensions: seq[DataTransferExtension] = @[],
): Future[Option[DataTransferChannel]] {.async: (raises: [CancelledError, dt.DataTransferError]).} =
  let (result, _) = await performLocalControl(
    manager,
    peerId,
    transferId,
    DataTransferControlType.dtctPause,
    DataTransferChannelStatus.dtcsPaused,
    pausedValue = true,
    remotePausedUpdate = none(bool),
    status = status,
    extensions = extensions,
    pausedFlag = true,
    eventKind = DataTransferEventKind.dtekPaused,
  )
  result

proc resumeChannel*(
    manager: DataTransferChannelManager,
    peerId: PeerId,
    transferId: uint64,
    status: Option[string] = none(string),
    extensions: seq[DataTransferExtension] = @[],
): Future[Option[DataTransferChannel]] {.async: (raises: [CancelledError, dt.DataTransferError]).} =
  let (result, _) = await performLocalControl(
    manager,
    peerId,
    transferId,
    DataTransferControlType.dtctResume,
    DataTransferChannelStatus.dtcsOngoing,
    pausedValue = false,
    remotePausedUpdate = none(bool),
    status = status,
    extensions = extensions,
    pausedFlag = false,
    eventKind = DataTransferEventKind.dtekResumed,
  )
  result

proc cancelChannel*(
    manager: DataTransferChannelManager,
    peerId: PeerId,
    transferId: uint64,
    status: Option[string] = none(string),
    extensions: seq[DataTransferExtension] = @[],
): Future[Option[DataTransferChannel]] {.async: (raises: [CancelledError, dt.DataTransferError]).} =
  let (result, _) = await performLocalControl(
    manager,
    peerId,
    transferId,
    DataTransferControlType.dtctCancel,
    DataTransferChannelStatus.dtcsCancelled,
    pausedValue = false,
    remotePausedUpdate = some(false),
    status = status,
    extensions = extensions,
    pausedFlag = false,
    eventKind = DataTransferEventKind.dtekCancelled,
  )
  result

proc completeChannel*(
    manager: DataTransferChannelManager,
    peerId: PeerId,
    transferId: uint64,
    status: Option[string] = none(string),
    extensions: seq[DataTransferExtension] = @[],
): Future[Option[DataTransferChannel]] {.async: (raises: [CancelledError, dt.DataTransferError]).} =
  let (result, _) = await performLocalControl(
    manager,
    peerId,
    transferId,
    DataTransferControlType.dtctComplete,
    DataTransferChannelStatus.dtcsCompleted,
    pausedValue = false,
    remotePausedUpdate = some(false),
    status = status,
    extensions = extensions,
    pausedFlag = false,
    eventKind = DataTransferEventKind.dtekCompleted,
  )
  result

proc restartChannel*(
    manager: DataTransferChannelManager,
    peerId: PeerId,
    transferId: uint64,
    status: Option[string] = none(string),
    extensions: seq[DataTransferExtension] = @[],
): Future[Option[DataTransferChannel]] {.async: (raises: [CancelledError, dt.DataTransferError]).} =
  let (result, channelRef) = await performLocalControl(
    manager,
    peerId,
    transferId,
    DataTransferControlType.dtctRestart,
    DataTransferChannelStatus.dtcsRequested,
    pausedValue = false,
    remotePausedUpdate = some(false),
    status = status,
    extensions = extensions,
    pausedFlag = false,
    eventKind = DataTransferEventKind.dtekRequested,
  )
  if channelRef.isNil or result.isNone:
    return result
  if channelRef.direction == DataTransferChannelDirection.dtcdOutbound:
    asyncSpawn sendOutbound(manager, channelRef)
  result

proc openChannel*(
    manager: DataTransferChannelManager,
    peerId: PeerId,
    request: DataTransferRequest,
    extensions: seq[DataTransferExtension] = @[],
    retryPolicy: DataTransferRetryPolicy = DataTransferRetryPolicy.init(),
    transferId: uint64 = 0,
): Future[DataTransferChannel] {.async: (raises: [CancelledError, dt.DataTransferError]).} =
  if manager == nil:
    raise newException(Defect, "data transfer channel manager is nil")

  await manager.lock.acquire()
  var released = false
  var id = transferId
  var channelRef: DataTransferChannelRef
  try:
    if id == 0:
      id = manager.nextId()
    else:
      let key = channelKey(peerId, id)
      if key in manager.channels:
        raise newException(Defect, "duplicate data transfer channel id")

    let now = Moment.now()
    channelRef = DataTransferChannelRef(
      peerId: peerId,
      transferId: id,
      direction: DataTransferChannelDirection.dtcdOutbound,
      request: request,
      extensions: extensions,
      status: DataTransferChannelStatus.dtcsRequested,
      paused: false,
      remotePaused: false,
      retryPolicy: retryPolicy,
      lastStatus: none(string),
      lastError: none(string),
      createdAt: now,
      updatedAt: now,
    )
    manager.channels[channelKey(peerId, id)] = channelRef
    manager.persistLocked()
    let snap = snapshot(channelRef)
    releaseSafe(manager.lock)
    released = true
    emitEvent(manager, DataTransferEventKind.dtekOpened, snap, none(string))
  finally:
    if not released:
      try:
        releaseSafe(manager.lock)
      except AsyncLockError:
        discard

  try:
    await sendOutbound(manager, channelRef)
  except dt.DataTransferError as exc:
    raise exc

  await manager.lock.acquire()
  defer:
    try:
      releaseSafe(manager.lock)
    except AsyncLockError:
      discard
  snapshot(channelRef)

proc handleMessage*(
    manager: DataTransferChannelManager,
    conn: Connection,
    message: DataTransferMessage,
): Future[Option[DataTransferMessage]] {.
    async: (raises: [CancelledError, dt.DataTransferError])
.} =
  if manager == nil:
    return none(DataTransferMessage)
  let peerId = conn.peerId
  case message.kind
  of DataTransferMessageKind.dtmkRequest:
    return await manager.handleIncomingRequest(peerId, message)
  of DataTransferMessageKind.dtmkResponse:
    await manager.handleResponse(peerId, message)
  of DataTransferMessageKind.dtmkControl:
    await manager.handleControl(peerId, message)
  of DataTransferMessageKind.dtmkError:
    await manager.handleError(peerId, message)
  none(DataTransferMessage)

proc getChannel*(
    manager: DataTransferChannelManager, peerId: PeerId, transferId: uint64
): Future[Option[DataTransferChannel]] {.async.} =
  if manager == nil:
    return none(DataTransferChannel)
  await manager.lock.acquire()
  defer:
    try:
      releaseSafe(manager.lock)
    except AsyncLockError:
      discard
  let key = channelKey(peerId, transferId)
  let channelRef = manager.channels.getOrDefault(key, nil)
  if channelRef.isNil:
    return none(DataTransferChannel)
  some(snapshot(channelRef))

proc removeChannel*(
    manager: DataTransferChannelManager, peerId: PeerId, transferId: uint64
): Future[bool] {.async.} =
  if manager == nil:
    return false
  await manager.lock.acquire()
  defer:
    try:
      releaseSafe(manager.lock)
    except AsyncLockError:
      discard
  let key = channelKey(peerId, transferId)
  if key in manager.channels:
    manager.channels.del(key)
    manager.persistLocked()
    return true
  false

proc stats*(
    manager: DataTransferChannelManager
): Future[DataTransferStats] {.async.} =
  ## Aggregate the currently tracked channels, grouping them by direction and status
  ## and counting the number of locally or remotely paused sessions.
  var result: DataTransferStats
  if manager == nil:
    return result

  await manager.lock.acquire()
  var released = false
  try:
    for channelRef in manager.channels.values:
      let channel = channelRef[]
      inc result.total
      inc result.counts[channel.direction][channel.status]
      if channel.paused:
        inc result.pausedLocal[channel.direction]
      if channel.remotePaused:
        inc result.pausedRemote[channel.direction]
    releaseSafe(manager.lock)
    released = true
  finally:
    if not released:
      try:
        releaseSafe(manager.lock)
      except AsyncLockError:
        discard
  result
