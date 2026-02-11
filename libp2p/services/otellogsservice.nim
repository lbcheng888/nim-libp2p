# Nim-LibP2P
# Licensed under either of
#  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
#  * MIT license ([LICENSE-MIT](LICENSE-MIT))
# at your option.

{.push raises: [].}

import std/[json, options, strutils, tables]
import chronos, chronicles
import chronos/apps/http/httpclient

import ../switch
when not defined(libp2p_disable_datatransfer):
  import ../protocols/datatransfer/channelmanager
import ./otelutils

when defined(libp2p_disable_datatransfer):
  {.fatal: "OpenTelemetry logs exporter requires data-transfer support.".}

logScope:
  topics = "libp2p otellogs"

const
  DefaultLogsInterval = 2.seconds
  DefaultLogsTimeout = 3.seconds
  DefaultLogsBatchSize = 64

type
  OtelLogRecord = object
    timeUnixNano: uint64
    severityNumber: int
    severityText: string
    body: string
    attributes: seq[(string, string)]

  DataTransferSubscription = object
    manager: DataTransferChannelManager
    handler: DataTransferEventHandler

  DataTransferSeverityOverride* = tuple[
    kind: DataTransferEventKind,
    severityNumber: int,
    severityText: string
  ]

  OpenTelemetryLogsConfig* = object
    endpoint*: string
    headers*: seq[(string, string)]
    resourceAttributes*: seq[(string, string)]
    scopeName*: string
    scopeVersion*: string
    flushInterval*: chronos.Duration
    timeout*: chronos.Duration
    maxBatchSize*: int
    globalAttributes*: seq[(string, string)]
    severityOverrides*: Table[DataTransferEventKind, (int, string)]

  OtelLogsService* = ref object of Service
    config*: OpenTelemetryLogsConfig
    session: HttpSessionRef
    loopFuture: Future[void]
    lock: AsyncLock
    buffer: seq[OtelLogRecord]
    subscriptions: seq[DataTransferSubscription]

proc toJsonArray(nodes: seq[JsonNode]): JsonNode =
  result = newJArray()
  for node in nodes:
    result.add(node)

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

proc severityForEvent(
  kind: DataTransferEventKind,
  overrides: Table[DataTransferEventKind, (int, string)]
): (int, string) =
  if overrides.len > 0:
    overrides.withValue(kind, overrideValue):
      return overrideValue

  case kind
  of DataTransferEventKind.dtekFailed:
    (17, "ERROR")
  of DataTransferEventKind.dtekCancelled, DataTransferEventKind.dtekVoucherRejected:
    (13, "WARN")
  of DataTransferEventKind.dtekPaused:
    (9, "INFO")
  of DataTransferEventKind.dtekCompleted, DataTransferEventKind.dtekAccepted,
      DataTransferEventKind.dtekResumed:
    (9, "INFO")
  of DataTransferEventKind.dtekOngoing:
    (9, "INFO")
  else:
    (5, "DEBUG")

proc boolAttr(value: bool): string =
  if value:
    "true"
  else:
    "false"

proc buildBody(event: DataTransferEvent): string =
  var parts: seq[string] = @[
    "data-transfer " & eventLabel(event.kind),
    "peer=" & $event.channel.peerId,
    "transfer=" & $event.channel.transferId,
    "direction=" & directionLabel(event.channel.direction),
  ]
  if event.channel.request.baseCid.isSome():
    parts.add("root=" & $event.channel.request.baseCid.get())
  if event.message.isSome():
    parts.add("message=" & event.message.get())
  parts.join("; ")

proc buildAttributes(
    self: OtelLogsService, event: DataTransferEvent
): seq[(string, string)] =
  result = @[]
  for (key, value) in self.config.globalAttributes:
    result.add((key, value))
  result.add(("libp2p.data_transfer.event", eventLabel(event.kind)))
  result.add(
    ("libp2p.data_transfer.direction", directionLabel(event.channel.direction))
  )
  result.add(("libp2p.data_transfer.status", statusLabel(event.channel.status)))
  result.add(("libp2p.data_transfer.paused", boolAttr(event.channel.paused)))
  result.add(
    ("libp2p.data_transfer.remote_paused", boolAttr(event.channel.remotePaused))
  )
  result.add(("libp2p.data_transfer.pull", boolAttr(event.channel.request.isPull)))
  result.add(("libp2p.peer_id", $event.channel.peerId))
  result.add(("libp2p.data_transfer.transfer_id", $event.channel.transferId))
  result.add(
    (
      "libp2p.data_transfer.retry.max_attempts",
      $event.channel.retryPolicy.maxAttempts
    )
  )
  result.add(
    (
      "libp2p.data_transfer.retry.delay_ms",
      $event.channel.retryPolicy.retryDelay.milliseconds()
    )
  )
  if event.channel.request.voucherType.len > 0:
    result.add(("libp2p.data_transfer.voucher_type", event.channel.request.voucherType))
  event.channel.request.baseCid.withValue(cid):
    result.add(("libp2p.data_transfer.base_cid", $cid))
  if event.channel.request.selector.len > 0:
    result.add(
      ("libp2p.data_transfer.selector_bytes", $event.channel.request.selector.len)
    )
  if event.channel.extensions.len > 0:
    result.add(("libp2p.data_transfer.extensions", $event.channel.extensions.len))
  event.message.withValue(msg):
    result.add(("libp2p.data_transfer.message", msg))
  event.channel.lastStatus.withValue(status):
    result.add(("libp2p.data_transfer.last_status", status))
  event.channel.lastError.withValue(err):
    result.add(("libp2p.data_transfer.last_error", err))

proc toRecord(self: OtelLogsService, event: DataTransferEvent): OtelLogRecord =
  let (severityNumber, severityText) = severityForEvent(
    event.kind, self.config.severityOverrides
  )
  OtelLogRecord(
    timeUnixNano: unixNowNs(),
    severityNumber: severityNumber,
    severityText: severityText,
    body: buildBody(event),
    attributes: self.buildAttributes(event),
  )

proc makeLogNode(record: OtelLogRecord, observedTime: uint64): JsonNode =
  var node = newJObject()
  node["timeUnixNano"] = %int64(record.timeUnixNano)
  node["observedTimeUnixNano"] = %int64(observedTime)
  node["severityNumber"] = %record.severityNumber
  node["severityText"] = %record.severityText
  var body = newJObject()
  body["stringValue"] = %record.body
  node["body"] = body
  if record.attributes.len > 0:
    node["attributes"] = makeAttributes(record.attributes)
  node

proc buildLogsPayload(
    self: OtelLogsService, records: seq[OtelLogRecord]
): string =
  if records.len == 0:
    return ""

  let observed = unixNowNs()
  var logRecords = newJArray()
  for record in records:
    logRecords.add(makeLogNode(record, observed))

  var scope = newJObject()
  if self.config.scopeName.len > 0:
    scope["name"] = %self.config.scopeName
  if self.config.scopeVersion.len > 0:
    scope["version"] = %self.config.scopeVersion

  var scopeLogs = newJObject()
  scopeLogs["scope"] = scope
  scopeLogs["logRecords"] = logRecords

  var scopeLogsArray = newJArray()
  scopeLogsArray.add(scopeLogs)

  let resourceAttrs = normalizedResourceAttributes(
    self.config.resourceAttributes, self.config.scopeVersion
  )
  var resource = newJObject()
  resource["attributes"] = makeAttributes(resourceAttrs)

  var resourceLog = newJObject()
  resourceLog["resource"] = resource
  resourceLog["scopeLogs"] = scopeLogsArray

  var resourceLogs = newJArray()
  resourceLogs.add(resourceLog)

  var payload = newJObject()
  payload["resourceLogs"] = resourceLogs
  $payload

proc buildHeaders(config: OpenTelemetryLogsConfig): seq[(string, string)] =
  var headers = config.headers
  headers.add(("Accept", "application/json"))
  otelutils.buildHeaders(headers, "application/json")

proc sendPayload(
    self: OtelLogsService, payload: string
): Future[bool] {.async: (raises: [CancelledError, CatchableError]).} =
  if payload.len == 0:
    return true

  if self.session.isNil:
    self.session = HttpSessionRef.new()

  try:
    let headers = buildHeaders(self.config)
    let requestResult = HttpClientRequestRef.post(
      self.session,
      self.config.endpoint,
      body = payload,
      headers = headers,
    )

    if requestResult.isErr:
      warn "OpenTelemetry logs exporter failed to create request",
        error = $requestResult.error
      return false

    let request = requestResult.unsafeGet()
    let responseFuture = request.send()

    if not await responseFuture.withTimeout(self.config.timeout):
      warn "OpenTelemetry logs exporter request timed out",
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
      warn "OpenTelemetry logs exporter received non-success response",
        status = response.status, body = snippet
      return false

    true
  except CancelledError as exc:
    raise exc
  except AsyncTimeoutError:
    warn "OpenTelemetry logs exporter request timed out", timeout = $self.config.timeout
    return false
  except HttpError as exc:
    warn "OpenTelemetry logs exporter request failed", error = exc.msg
    return false
  except CatchableError as exc:
    warn "OpenTelemetry logs exporter unexpected error", error = exc.msg
    return false

proc flushBuffer(self: OtelLogsService): Future[void] {.async.} =
  await self.lock.acquire()
  var batch: seq[OtelLogRecord] = @[]
  if self.buffer.len > 0:
    batch = self.buffer
    self.buffer = @[]
  try:
    self.lock.release()
  except AsyncLockError:
    discard
  if batch.len > 0:
    let payload = self.buildLogsPayload(batch)
    if payload.len > 0:
      try:
        let sendOk = await self.sendPayload(payload)
        if not sendOk:
          discard
      except CatchableError as exc:
        warn "OpenTelemetry logs exporter flush failed", error = exc.msg
        discard

proc enqueue(self: OtelLogsService, record: OtelLogRecord) {.async.} =
  await self.lock.acquire()
  var batch: seq[OtelLogRecord] = @[]
  self.buffer.add(record)
  let shouldFlush =
    self.config.maxBatchSize > 0 and self.buffer.len >= self.config.maxBatchSize
  if shouldFlush:
    batch = self.buffer
    self.buffer = @[]
  try:
    self.lock.release()
  except AsyncLockError:
    discard
  if batch.len > 0:
    let payload = self.buildLogsPayload(batch)
    if payload.len > 0:
      try:
        let sendOk = await self.sendPayload(payload)
        if not sendOk:
          discard
      except CatchableError as exc:
        warn "OpenTelemetry logs exporter enqueue flush failed", error = exc.msg
        discard

proc handleDataTransferEvent(
    self: OtelLogsService, event: DataTransferEvent
) {.async.} =
  try:
    let record = self.toRecord(event)
    await self.enqueue(record)
  except CancelledError as exc:
    raise exc
  except CatchableError as exc:
    debug "OpenTelemetry logs exporter failed to enqueue event", err = exc.msg,
      peerId = event.channel.peerId, transferId = event.channel.transferId,
      event = eventLabel(event.kind)

proc loop(self: OtelLogsService) {.async.} =
  try:
    while true:
      await sleepAsync(self.config.flushInterval)
      await self.flushBuffer()
  except CancelledError as exc:
    discard

proc new*(
    T: typedesc[OtelLogsService],
    endpoint: string,
    headers: seq[(string, string)] = @[],
    resourceAttributes: seq[(string, string)] = @[],
    scopeName: string = "nim-libp2p",
    scopeVersion: string = "",
    flushInterval: chronos.Duration = DefaultLogsInterval,
    timeout: chronos.Duration = DefaultLogsTimeout,
    maxBatchSize: int = DefaultLogsBatchSize,
    globalAttributes: seq[(string, string)] = @[],
    severityOverrides: seq[DataTransferSeverityOverride] = @[],
): T {.raises: [ValueError].} =
  let trimmedEndpoint = endpoint.strip()
  if trimmedEndpoint.len == 0:
    raise newException(ValueError, "OpenTelemetry logs endpoint must not be empty")

  var cfg = OpenTelemetryLogsConfig(
    endpoint: trimmedEndpoint,
    headers: headers,
    resourceAttributes: resourceAttributes,
    scopeName: scopeName.strip(),
    scopeVersion: scopeVersion.strip(),
    flushInterval: flushInterval,
    timeout: timeout,
    maxBatchSize: maxBatchSize,
  )
  cfg.globalAttributes = @[]
  for (key, value) in globalAttributes:
    let trimmedKey = key.strip()
    if trimmedKey.len == 0:
      continue
    cfg.globalAttributes.add((trimmedKey, value.strip()))
  cfg.severityOverrides = initTable[DataTransferEventKind, (int, string)]()
  for override in severityOverrides:
    var severityNumber = override.severityNumber
    if severityNumber < 1:
      severityNumber = 1
    elif severityNumber > 24:
      severityNumber = 24
    let trimmedText = override.severityText.strip()
    if trimmedText.len == 0:
      continue
    cfg.severityOverrides[override.kind] = (
      severityNumber, trimmedText.toUpperAscii()
    )

  if cfg.scopeName.len == 0:
    cfg.scopeName = "nim-libp2p"
  if cfg.flushInterval <= chronos.ZeroDuration:
    cfg.flushInterval = DefaultLogsInterval
  if cfg.timeout <= chronos.ZeroDuration:
    cfg.timeout = DefaultLogsTimeout
  if cfg.maxBatchSize < 1:
    cfg.maxBatchSize = DefaultLogsBatchSize

  T(
    config: cfg,
    lock: newAsyncLock(),
    buffer: @[],
    subscriptions: @[],
  )

proc registerDataTransferManager*(
    self: OtelLogsService, manager: DataTransferChannelManager
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
    self: OtelLogsService, manager: DataTransferChannelManager
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

proc flushNow*(self: OtelLogsService) {.async: (raises: [CancelledError]).} =
  if self.isNil:
    return
  try:
    await self.flushBuffer()
  except CatchableError as exc:
    warn "OpenTelemetry logs exporter flushNow failed", error = exc.msg

method setup*(
    self: OtelLogsService, switch: Switch
): Future[bool] {.async: (raises: [CancelledError]).} =
  let hasBeenSetup = await procCall Service(self).setup(switch)
  if hasBeenSetup and self.session.isNil:
    self.session = HttpSessionRef.new()
  hasBeenSetup

method run*(
    self: OtelLogsService, switch: Switch
) {.async: (raises: [CancelledError]).} =
  if self.session.isNil:
    self.session = HttpSessionRef.new()
  if self.loopFuture.isNil:
    self.loopFuture = self.loop()
    asyncSpawn(self.loopFuture)

method stop*(
    self: OtelLogsService, switch: Switch
): Future[bool] {.async: (raises: [CancelledError]).} =
  let hasBeenStopped = await procCall Service(self).stop(switch)
  if hasBeenStopped:
    if not self.loopFuture.isNil:
      await self.loopFuture.cancelAndWait()
      self.loopFuture = nil
    try:
      await self.flushBuffer()
    except CatchableError as exc:
      warn "OpenTelemetry logs exporter stop flush failed", error = exc.msg
    for subscription in self.subscriptions:
      subscription.manager.removeEventHandler(subscription.handler)
    self.subscriptions.setLen(0)
    if not self.session.isNil:
      await self.session.closeWait()
      self.session = nil
  hasBeenStopped

{.pop.}
