# Nim-LibP2P
# Licensed under either of
#  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
#  * MIT license ([LICENSE-MIT](LICENSE-MIT))
# at your option.

{.push raises: [].}

import std/[json, options, sha1, strutils, tables]
import chronos, chronicles
import chronos/apps/http/httpclient
import stew/byteutils

import ../switch
when not defined(libp2p_disable_datatransfer):
  import ../protocols/datatransfer/channelmanager
import ./otelutils

when defined(libp2p_disable_datatransfer):
  {.fatal: "OpenTelemetry traces exporter requires data-transfer support.".}

logScope:
  topics = "libp2p oteltraces"

const
  DefaultTracesInterval = 2.seconds
  DefaultTracesTimeout = 3.seconds
  DefaultTracesBatchSize = 32
  DefaultSpanKind = "SPAN_KIND_INTERNAL"

type
  OtelSpanEvent = object
    timeUnixNano: uint64
    name: string
    attributes: seq[(string, string)]

  DataTransferSpan = ref object
    traceId: string
    spanId: string
    name: string
    startTimeUnixNano: uint64
    endTimeUnixNano: uint64
    attributes: seq[(string, string)]
    events: seq[OtelSpanEvent]
    statusCode: string
    statusMessage: string
    ended: bool

  DataTransferSubscription = object
    manager: DataTransferChannelManager
    handler: DataTransferEventHandler

  OpenTelemetryTracesConfig* = object
    endpoint*: string
    headers*: seq[(string, string)]
    resourceAttributes*: seq[(string, string)]
    scopeName*: string
    scopeVersion*: string
    flushInterval*: chronos.Duration
    timeout*: chronos.Duration
    maxBatchSize*: int
    globalAttributes*: seq[(string, string)]
    spanKind*: string

  OtelTracesService* = ref object of Service
    config*: OpenTelemetryTracesConfig
    session: HttpSessionRef
    loopFuture: Future[void]
    lock: AsyncLock
    activeSpans: Table[string, DataTransferSpan]
    completed: seq[DataTransferSpan]
    subscriptions: seq[DataTransferSubscription]

proc boolAttr(value: bool): string =
  if value:
    "true"
  else:
    "false"

proc directionLabel(direction: DataTransferChannelDirection): string =
  case direction
  of DataTransferChannelDirection.dtcdInbound:
    "inbound"
  of DataTransferChannelDirection.dtcdOutbound:
    "outbound"

proc statusLabel(status: DataTransferChannelStatus): string =
  case status
  of DataTransferChannelStatus.dtcsRequested:
    "requested"
  of DataTransferChannelStatus.dtcsAccepted:
    "accepted"
  of DataTransferChannelStatus.dtcsOngoing:
    "ongoing"
  of DataTransferChannelStatus.dtcsPaused:
    "paused"
  of DataTransferChannelStatus.dtcsCompleted:
    "completed"
  of DataTransferChannelStatus.dtcsCancelled:
    "cancelled"
  of DataTransferChannelStatus.dtcsFailed:
    "failed"

proc eventLabel(kind: DataTransferEventKind): string =
  case kind
  of DataTransferEventKind.dtekOpened:
    "opened"
  of DataTransferEventKind.dtekRequested:
    "requested"
  of DataTransferEventKind.dtekAccepted:
    "accepted"
  of DataTransferEventKind.dtekOngoing:
    "ongoing"
  of DataTransferEventKind.dtekPaused:
    "paused"
  of DataTransferEventKind.dtekResumed:
    "resumed"
  of DataTransferEventKind.dtekCompleted:
    "completed"
  of DataTransferEventKind.dtekCancelled:
    "cancelled"
  of DataTransferEventKind.dtekFailed:
    "failed"
  of DataTransferEventKind.dtekVoucherRejected:
    "voucher_rejected"

proc spanKey(channel: DataTransferChannel): string =
  $channel.peerId & ":" & $channel.transferId

proc upsertAttribute(
    attrs: var seq[(string, string)], key: string, value: string
) =
  let trimmedKey = key.strip()
  if trimmedKey.len == 0:
    return
  let trimmedValue = value.strip()
  for idx, (existingKey, _) in attrs.pairs():
    if existingKey == trimmedKey:
      attrs[idx] = (trimmedKey, trimmedValue)
      return
  attrs.add((trimmedKey, trimmedValue))

proc deriveIds(
    channel: DataTransferChannel, timestamp: uint64
): (string, string) =
  let base = $channel.peerId & ":" & $channel.transferId & ":" & $timestamp
  let traceHex = ($secureHash(base)).toLowerAscii()
  let traceId =
    if traceHex.len >= 32: traceHex[0 ..< 32] else: traceHex
  let spanHex = ($secureHash(base & ":span")).toLowerAscii()
  let spanId =
    if spanHex.len >= 16: spanHex[0 ..< 16] else: spanHex
  (traceId, spanId)

proc buildBaseAttributes(
    self: OtelTracesService, channel: DataTransferChannel
): seq[(string, string)] =
  result = @[]
  for (key, value) in self.config.globalAttributes:
    result.add((key, value))
  result.add(("libp2p.peer_id", $channel.peerId))
  result.add(("libp2p.data_transfer.transfer_id", $channel.transferId))
  result.add(
    ("libp2p.data_transfer.direction", directionLabel(channel.direction))
  )
  result.add(
    ("libp2p.data_transfer.is_pull", boolAttr(channel.request.isPull))
  )
  result.add(
    (
      "libp2p.data_transfer.retry.max_attempts",
      $channel.retryPolicy.maxAttempts
    )
  )
  result.add(
    (
      "libp2p.data_transfer.retry.delay_ms",
      $channel.retryPolicy.retryDelay.milliseconds()
    )
  )
  if channel.request.voucherType.len > 0:
    result.add(
      ("libp2p.data_transfer.voucher_type", channel.request.voucherType)
    )
  if channel.request.selector.len > 0:
    result.add(
      (
        "libp2p.data_transfer.selector_bytes",
        $channel.request.selector.len
      )
    )
  if channel.extensions.len > 0:
    result.add(
      ("libp2p.data_transfer.extensions", $channel.extensions.len)
    )
  channel.request.baseCid.withValue(cid):
    result.add(("libp2p.data_transfer.base_cid", $cid))
  result.add(("libp2p.data_transfer.status", statusLabel(channel.status)))
  result.add(("libp2p.data_transfer.paused", boolAttr(channel.paused)))
  result.add(
    ("libp2p.data_transfer.remote_paused", boolAttr(channel.remotePaused))
  )
  channel.lastStatus.withValue(lastStatus):
    if lastStatus.len > 0:
      result.add(("libp2p.data_transfer.last_status", lastStatus))
  channel.lastError.withValue(lastError):
    if lastError.len > 0:
      result.add(("libp2p.data_transfer.last_error", lastError))

proc buildEventAttributes(
    self: OtelTracesService, event: DataTransferEvent
): seq[(string, string)] =
  result = @[
    ("libp2p.data_transfer.status", statusLabel(event.channel.status)),
    ("libp2p.data_transfer.paused", boolAttr(event.channel.paused)),
    (
      "libp2p.data_transfer.remote_paused",
      boolAttr(event.channel.remotePaused)
    ),
  ]
  event.message.withValue(msg):
    if msg.len > 0:
      result.add(("libp2p.data_transfer.message", msg))
  event.channel.lastStatus.withValue(lastStatus):
    if lastStatus.len > 0:
      result.add(("libp2p.data_transfer.last_status", lastStatus))
  event.channel.lastError.withValue(lastError):
    if lastError.len > 0:
      result.add(("libp2p.data_transfer.last_error", lastError))

proc newSpan(
    self: OtelTracesService, channel: DataTransferChannel, timestamp: uint64
): DataTransferSpan =
  let (traceId, spanId) = deriveIds(channel, timestamp)
  let name = "libp2p.data_transfer." & directionLabel(channel.direction)
  DataTransferSpan(
    traceId: traceId,
    spanId: spanId,
    name: name,
    startTimeUnixNano: timestamp,
    endTimeUnixNano: timestamp,
    attributes: self.buildBaseAttributes(channel),
    events: @[],
    statusCode: "STATUS_CODE_UNSET",
    statusMessage: "",
    ended: false,
  )

proc recordEvent(
    self: OtelTracesService,
    span: DataTransferSpan,
    event: DataTransferEvent,
    timestamp: uint64,
) =
  let attributes = self.buildEventAttributes(event)
  span.events.add(
    OtelSpanEvent(
      timeUnixNano: timestamp,
      name: "libp2p.data_transfer." & eventLabel(event.kind),
      attributes: attributes,
    )
  )
  span.endTimeUnixNano = timestamp
  for (key, value) in attributes:
    upsertAttribute(span.attributes, key, value)
  upsertAttribute(
    span.attributes, "libp2p.data_transfer.status", statusLabel(event.channel.status)
  )
  case event.kind
  of DataTransferEventKind.dtekCompleted:
    let message = event.message.get("")
    span.statusCode = "STATUS_CODE_OK"
    span.statusMessage = message
    if message.len > 0:
      upsertAttribute(
        span.attributes, "libp2p.data_transfer.completion_message", message
      )
    span.ended = true
  of DataTransferEventKind.dtekCancelled:
    let message =
      if event.message.isSome():
        event.message.get()
      elif event.channel.lastStatus.isSome():
        event.channel.lastStatus.get()
      else:
        ""
    span.statusCode = "STATUS_CODE_ERROR"
    span.statusMessage = message
    if message.len > 0:
      upsertAttribute(
        span.attributes, "libp2p.data_transfer.cancellation_message", message
      )
    span.ended = true
  of DataTransferEventKind.dtekFailed, DataTransferEventKind.dtekVoucherRejected:
    let message =
      if event.message.isSome():
        event.message.get()
      elif event.channel.lastError.isSome():
        event.channel.lastError.get()
      else:
        ""
    span.statusCode = "STATUS_CODE_ERROR"
    span.statusMessage = message
    if message.len > 0:
      upsertAttribute(span.attributes, "libp2p.data_transfer.error", message)
    span.ended = true
  of DataTransferEventKind.dtekPaused:
    upsertAttribute(span.attributes, "libp2p.data_transfer.paused", "true")
  of DataTransferEventKind.dtekResumed:
    upsertAttribute(span.attributes, "libp2p.data_transfer.paused", "false")
  else:
    discard

proc buildTracesPayload(
    self: OtelTracesService, spans: seq[DataTransferSpan]
): string =
  if spans.len == 0:
    return ""

  var spanNodes = newJArray()
  for span in spans:
    if span.isNil():
      continue
    if not span.ended:
      continue
    var node = newJObject()
    node["traceId"] = %span.traceId
    node["spanId"] = %span.spanId
    node["name"] = %span.name
    node["kind"] = %self.config.spanKind
    node["startTimeUnixNano"] = %span.startTimeUnixNano
    node["endTimeUnixNano"] = %span.endTimeUnixNano
    node["attributes"] = makeAttributes(span.attributes)
    if span.events.len > 0:
      var eventsArray = newJArray()
      for event in span.events:
        var eventNode = newJObject()
        eventNode["timeUnixNano"] = %event.timeUnixNano
        eventNode["name"] = %event.name
        if event.attributes.len > 0:
          eventNode["attributes"] = makeAttributes(event.attributes)
        eventsArray.add(eventNode)
      node["events"] = eventsArray
    var statusNode = newJObject()
    statusNode["code"] = %span.statusCode
    if span.statusMessage.len > 0:
      statusNode["message"] = %span.statusMessage
    node["status"] = statusNode
    spanNodes.add(node)

  if spanNodes.len == 0:
    return ""

  var scope = newJObject()
  scope["name"] = %self.config.scopeName
  if self.config.scopeVersion.len > 0:
    scope["version"] = %self.config.scopeVersion

  var scopeSpans = newJObject()
  scopeSpans["scope"] = scope
  scopeSpans["spans"] = spanNodes

  var scopeArray = newJArray()
  scopeArray.add(scopeSpans)

  var resource = newJObject()
  resource["attributes"] = makeAttributes(
    normalizedResourceAttributes(
      self.config.resourceAttributes, self.config.scopeVersion
    )
  )

  var resourceSpans = newJObject()
  resourceSpans["resource"] = resource
  resourceSpans["scopeSpans"] = scopeArray

  var resourceArray = newJArray()
  resourceArray.add(resourceSpans)

  var payload = newJObject()
  payload["resourceSpans"] = resourceArray
  $payload

proc sendPayload(
    self: OtelTracesService, payload: string
): Future[bool] {.async: (raises: [CancelledError]).} =
  if payload.len == 0:
    return true

  if self.session.isNil:
    self.session = HttpSessionRef.new()

  try:
    let headers = buildHeaders(self.config.headers)
    let requestResult = HttpClientRequestRef.post(
      self.session,
      self.config.endpoint,
      body = payload,
      headers = headers,
    )

    if requestResult.isErr:
      warn "OpenTelemetry traces exporter failed to create request",
        error = $requestResult.error
      return false

    let request = requestResult.unsafeGet()
    let responseFuture = request.send()

    if not await responseFuture.withTimeout(self.config.timeout):
      warn "OpenTelemetry traces exporter request timed out",
        timeout = $self.config.timeout
      return false

    let response = await responseFuture
    defer:
      await response.closeWait()

    if response.status < 200 or response.status >= 300:
      let bodyBytes = await response.getBodyBytes()
      var snippet = ""
      let limit = min(bodyBytes.len, 512)
      for i in 0 ..< limit:
        snippet.add(char(bodyBytes[i]))
      warn "OpenTelemetry traces exporter received non-success response",
        status = response.status, body = snippet
      return false

    true
  except CancelledError as exc:
    raise exc
  except AsyncTimeoutError:
    warn "OpenTelemetry traces exporter request timed out", timeout = $self.config.timeout
    return false
  except HttpError as exc:
    warn "OpenTelemetry traces exporter request failed", error = exc.msg
    return false
  except CatchableError as exc:
    warn "OpenTelemetry traces exporter unexpected error", error = exc.msg
    return false

proc flushCompleted(
    self: OtelTracesService
): Future[void] {.async: (raises: [CancelledError]).} =
  if self.isNil:
    return
  var batch: seq[DataTransferSpan] = @[]
  await self.lock.acquire()
  try:
    if self.completed.len > 0:
      let limit =
        if self.config.maxBatchSize > 0 and self.completed.len > self.config.maxBatchSize:
          self.config.maxBatchSize
        else:
          self.completed.len
      batch = self.completed[0 ..< limit]
      if limit >= self.completed.len:
        self.completed.setLen(0)
      else:
        self.completed = self.completed[limit ..< self.completed.len]
  finally:
    try:
      self.lock.release()
    except AsyncLockError:
      discard
  if batch.len > 0:
    let payload = self.buildTracesPayload(batch)
    if payload.len > 0:
      try:
        let sendOk = await self.sendPayload(payload)
        if not sendOk:
          discard
      except CatchableError as exc:
        warn "OpenTelemetry traces exporter flush failed", error = exc.msg
        discard

proc handleDataTransferEvent(
    self: OtelTracesService, event: DataTransferEvent
) {.async.} =
  try:
    let timestamp = unixNowNs()
    var batch: seq[DataTransferSpan] = @[]
    await self.lock.acquire()
    try:
      let key = spanKey(event.channel)
      var span: DataTransferSpan
      if self.activeSpans.hasKey(key):
        span = self.activeSpans[key]
      else:
        span = self.newSpan(event.channel, timestamp)
        self.activeSpans[key] = span
      self.recordEvent(span, event, timestamp)
      if span.ended:
        self.completed.add(span)
        self.activeSpans.del(key)
      let shouldFlush =
        self.config.maxBatchSize > 0 and self.completed.len >= self.config.maxBatchSize
      if shouldFlush:
        let count =
          if self.config.maxBatchSize > 0 and self.completed.len > self.config.maxBatchSize:
            self.config.maxBatchSize
          else:
            self.completed.len
        batch = self.completed[0 ..< count]
        if count >= self.completed.len:
          self.completed.setLen(0)
        else:
          self.completed = self.completed[count ..< self.completed.len]
    finally:
      try:
        self.lock.release()
      except AsyncLockError:
        discard
    if batch.len > 0:
      let payload = self.buildTracesPayload(batch)
      if payload.len > 0:
        try:
          let sendOk = await self.sendPayload(payload)
          if not sendOk:
            discard
        except CatchableError as exc:
          warn "OpenTelemetry traces exporter event flush failed", error = exc.msg
          discard
  except CancelledError as exc:
    raise exc
  except CatchableError as exc:
    debug "OpenTelemetry traces exporter failed to record event", err = exc.msg,
      peerId = event.channel.peerId, transferId = event.channel.transferId,
      event = eventLabel(event.kind)

proc loop(self: OtelTracesService) {.async.} =
  try:
    while true:
      await sleepAsync(self.config.flushInterval)
      await self.flushCompleted()
  except CancelledError as exc:
    discard

proc new*(
    T: typedesc[OtelTracesService],
    endpoint: string,
    headers: seq[(string, string)] = @[],
    resourceAttributes: seq[(string, string)] = @[],
    scopeName: string = "nim-libp2p",
    scopeVersion: string = "",
    flushInterval: chronos.Duration = DefaultTracesInterval,
    timeout: chronos.Duration = DefaultTracesTimeout,
    maxBatchSize: int = DefaultTracesBatchSize,
    globalAttributes: seq[(string, string)] = @[],
    spanKind: string = DefaultSpanKind,
): T {.raises: [ValueError].} =
  let trimmedEndpoint = endpoint.strip()
  if trimmedEndpoint.len == 0:
    raise newException(ValueError, "OpenTelemetry traces endpoint must not be empty")

  var cfg = OpenTelemetryTracesConfig(
    endpoint: trimmedEndpoint,
    headers: headers,
    resourceAttributes: resourceAttributes,
    scopeName: scopeName.strip(),
    scopeVersion: scopeVersion.strip(),
    flushInterval: flushInterval,
    timeout: timeout,
    maxBatchSize: maxBatchSize,
    globalAttributes: @[],
    spanKind: spanKind.strip(),
  )

  for (key, value) in globalAttributes:
    let trimmedKey = key.strip()
    if trimmedKey.len == 0:
      continue
    cfg.globalAttributes.add((trimmedKey, value.strip()))

  if cfg.scopeName.len == 0:
    cfg.scopeName = "nim-libp2p"
  if cfg.flushInterval <= chronos.ZeroDuration:
    cfg.flushInterval = DefaultTracesInterval
  if cfg.timeout <= chronos.ZeroDuration:
    cfg.timeout = DefaultTracesTimeout
  if cfg.maxBatchSize < 1:
    cfg.maxBatchSize = DefaultTracesBatchSize
  if cfg.spanKind.len == 0:
    cfg.spanKind = DefaultSpanKind

  T(
    config: cfg,
    lock: newAsyncLock(),
    activeSpans: initTable[string, DataTransferSpan](),
    completed: @[],
    subscriptions: @[],
  )

proc registerDataTransferManager*(
    self: OtelTracesService, manager: DataTransferChannelManager
) {.public.} =
  if self.isNil or manager.isNil:
    return
  for subscription in self.subscriptions:
    if subscription.manager == manager:
      return
  let handler = proc(event: DataTransferEvent) =
    asyncSpawn(self.handleDataTransferEvent(event))
  manager.addEventHandler(handler)
  self.subscriptions.add(
    DataTransferSubscription(manager: manager, handler: handler)
  )

proc unregisterDataTransferManager*(
    self: OtelTracesService, manager: DataTransferChannelManager
) {.public.} =
  if self.isNil or manager.isNil or self.subscriptions.len == 0:
    return
  var idx = self.subscriptions.high
  while idx >= 0:
    let subscription = self.subscriptions[idx]
    if subscription.manager == manager:
      subscription.manager.removeEventHandler(subscription.handler)
      self.subscriptions.delete(idx)
    dec idx

proc flushNow*(self: OtelTracesService) {.async: (raises: [CancelledError]).} =
  await self.flushCompleted()

method setup*(
    self: OtelTracesService, switch: Switch
): Future[bool] {.async: (raises: [CancelledError]).} =
  let hasBeenSetup = await procCall Service(self).setup(switch)
  if hasBeenSetup and self.session.isNil:
    self.session = HttpSessionRef.new()
  hasBeenSetup

method run*(
    self: OtelTracesService, switch: Switch
) {.async: (raises: [CancelledError]).} =
  if self.session.isNil:
    self.session = HttpSessionRef.new()
  if self.loopFuture.isNil:
    self.loopFuture = self.loop()
    asyncSpawn(self.loopFuture)

method stop*(
    self: OtelTracesService, switch: Switch
): Future[bool] {.async: (raises: [CancelledError]).} =
  let hasBeenStopped = await procCall Service(self).stop(switch)
  if hasBeenStopped:
    if not self.loopFuture.isNil:
      await self.loopFuture.cancelAndWait()
      self.loopFuture = nil
    await self.flushCompleted()
    for subscription in self.subscriptions:
      subscription.manager.removeEventHandler(subscription.handler)
    self.subscriptions.setLen(0)
    if not self.session.isNil:
      await self.session.closeWait()
      self.session = nil
  hasBeenStopped

{.pop.}
