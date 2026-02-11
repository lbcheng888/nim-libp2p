# Nim-LibP2P
# Copyright (c)
# Licensed under either of
#  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
#  * MIT license ([LICENSE-MIT](LICENSE-MIT))
# at your option.

{.push raises: [].}

import std/options
import chronos, chronicles

import ../../errors
import ../../dialer
import ../../peerid
import ../../stream/connection
import ../../utils/semaphore
import ../../protocols/protocol
import ./protobuf

const
  DataTransferCodec* = "/p2p/datatransfer/1.2.0"
  DefaultMaxDataTransferMessageBytes* = 2 * 1024 * 1024

logScope:
  topics = "libp2p datatransfer"

type
  DataTransferErrorKind* {.pure.} = enum
    dtekDialFailure,
    dtekTimeout,
    dtekWriteFailure,
    dtekReadFailure,
    dtekDecodeFailure,
    dtekConfig

  DataTransferError* = object of LPError
    kind*: DataTransferErrorKind

  DataTransferHandler* = proc(
      conn: Connection, message: DataTransferMessage
  ): Future[Option[DataTransferMessage]] {.gcsafe, raises: [].}

  DataTransferConfig* = object
    maxMessageBytes*: int
    closeAfterResponse*: bool
    maxConcurrentStreams*: int

  DataTransferService* = ref object of LPProtocol
    messageHandler*: DataTransferHandler
    config*: DataTransferConfig
    streamLimiter: AsyncSemaphore

  DataTransferDialer* = proc(
      peerId: PeerId, protos: seq[string]
  ): Future[Connection] {.gcsafe, raises: [DialFailedError, CancelledError].}

proc init*(
    _: type DataTransferConfig,
    maxMessageBytes: int = DefaultMaxDataTransferMessageBytes,
    closeAfterResponse: bool = true,
    maxConcurrentStreams: int = 32,
): DataTransferConfig =
  DataTransferConfig(
    maxMessageBytes: maxMessageBytes,
    closeAfterResponse: closeAfterResponse,
    maxConcurrentStreams: maxConcurrentStreams,
  )

proc validate(config: DataTransferConfig) =
  if config.maxMessageBytes <= 0:
    raise newException(Defect, "data transfer maxMessageBytes must be positive")
  if config.maxConcurrentStreams < 0:
    raise newException(
      Defect, "data transfer maxConcurrentStreams cannot be negative"
    )

proc new*(
    T: type DataTransferService,
    handler: DataTransferHandler,
    config: DataTransferConfig = DataTransferConfig.init(),
): T {.public.} =
  if handler.isNil():
    raise newException(Defect, "data transfer service requires a handler")
  validate(config)
  let limiter =
    if config.maxConcurrentStreams > 0:
      newAsyncSemaphore(config.maxConcurrentStreams)
    else:
      nil
  let svc = DataTransferService(
    messageHandler: handler, config: config, streamLimiter: limiter
  )
  svc.init()
  svc

method init*(svc: DataTransferService) =
  proc handle(conn: Connection, proto: string) {.async: (raises: [CancelledError]).} =
    trace "handling data transfer stream", conn
    var streamSlotAcquired = false
    try:
      if not svc.streamLimiter.isNil():
        await svc.streamLimiter.acquire()
        streamSlotAcquired = true

      try:
        let payload = await conn.readLp(svc.config.maxMessageBytes)
        let msgOpt = decodeDataTransferMessage(payload)
        if msgOpt.isNone():
          trace "failed decoding data transfer message", conn
          return

        if svc.messageHandler.isNil():
          trace "data transfer handler missing, dropping message", conn
          return

        let responseOpt = await svc.messageHandler(conn, msgOpt.get())
        responseOpt.withValue(resp):
          let encoded = encode(resp)
          try:
            await conn.writeLp(encoded)
          except LPStreamError as exc:
            trace "failed sending data transfer response", conn, err = exc.msg
      finally:
        if streamSlotAcquired:
          svc.streamLimiter.release()
    except CancelledError as exc:
      trace "data transfer handler cancelled", conn
      raise exc
    except LPStreamLimitError as exc:
      trace "data transfer read limit exceeded", conn, err = exc.msg
    except LPStreamError as exc:
      trace "data transfer stream error", conn, err = exc.msg
    except CatchableError as exc:
      trace "exception in data transfer handler", conn, err = exc.msg
    finally:
      if svc.config.closeAfterResponse:
        try:
          await conn.close()
        except CatchableError as exc:
          trace "failed to close data transfer connection", conn, err = exc.msg

  svc.handler = handle
  svc.codec = DataTransferCodec

proc newDataTransferError*(
    message: string,
    kind: DataTransferErrorKind,
    parent: ref CatchableError = nil,
): ref DataTransferError =
  (ref DataTransferError)(msg: message, kind: kind, parent: parent)

func isRetryable(kind: DataTransferErrorKind): bool =
  kind in {
    DataTransferErrorKind.dtekDialFailure,
    DataTransferErrorKind.dtekTimeout,
    DataTransferErrorKind.dtekWriteFailure,
    DataTransferErrorKind.dtekReadFailure,
  }

proc sendDataTransfer*(
    dial: DataTransferDialer,
    peerId: PeerId,
    message: DataTransferMessage,
    timeout: Duration = 10.seconds,
    maxResponseBytes: int = DefaultMaxDataTransferMessageBytes,
    expectResponse: bool = true,
    maxAttempts: int = 1,
    retryDelay: Duration = 200.milliseconds,
): Future[Option[DataTransferMessage]] {.async: (raises: [CancelledError, DataTransferError]).}

proc sendDataTransferOnce(
    dial: DataTransferDialer,
    peerId: PeerId,
    message: DataTransferMessage,
    timeout: Duration = 10.seconds,
    maxResponseBytes: int = DefaultMaxDataTransferMessageBytes,
    expectResponse: bool = true,
): Future[Option[DataTransferMessage]] {.async: (raises: [CancelledError, DataTransferError]).} =
  if maxResponseBytes <= 0:
    raise newDataTransferError(
      "maxResponseBytes must be positive", DataTransferErrorKind.dtekConfig
    )

  trace "sending data transfer message", peerId, transferId = message.transferId

  let conn =
    try:
      await dial(peerId, @[DataTransferCodec])
    except CancelledError as exc:
      raise exc
    except CatchableError as exc:
      trace "data transfer dial failure", peerId, err = exc.msg
      raise newDataTransferError(
        "data transfer dial failed: " & exc.msg,
        DataTransferErrorKind.dtekDialFailure,
        exc,
      )

  defer:
    await conn.close()

  let encoded = encode(message)
  try:
    await conn.writeLp(encoded)
  except CancelledError as exc:
    raise exc
  except LPStreamError as exc:
    raise newDataTransferError(
      "data transfer write failed: " & exc.msg,
      DataTransferErrorKind.dtekWriteFailure,
      exc,
    )

  if not expectResponse:
    return none(DataTransferMessage)

  let readFuture = conn.readLp(maxResponseBytes)
  let completed =
    try:
      await withTimeout(readFuture, timeout)
    except CancelledError as exc:
      raise exc

  if not completed:
    readFuture.cancelSoon()
    raise newDataTransferError(
      "data transfer response timeout", DataTransferErrorKind.dtekTimeout
    )

  let payload =
    try:
      await readFuture
    except CancelledError as exc:
      raise exc
    except LPStreamError as exc:
      raise newDataTransferError(
        "data transfer read failed: " & exc.msg,
        DataTransferErrorKind.dtekReadFailure,
        exc,
      )

  let responseOpt = decodeDataTransferMessage(payload)
  if responseOpt.isNone():
    raise newDataTransferError(
      "data transfer invalid response payload",
      DataTransferErrorKind.dtekDecodeFailure,
    )

  trace "received data transfer response", peerId, transferId = message.transferId
  some(responseOpt.get())

proc sendDataTransfer*(
    dial: DataTransferDialer,
    peerId: PeerId,
    message: DataTransferMessage,
    timeout: Duration = 10.seconds,
    maxResponseBytes: int = DefaultMaxDataTransferMessageBytes,
    expectResponse: bool = true,
    maxAttempts: int = 1,
    retryDelay: Duration = 200.milliseconds,
): Future[Option[DataTransferMessage]] {.async: (raises: [CancelledError, DataTransferError]).} =
  if maxAttempts <= 0:
    raise newDataTransferError(
      "maxAttempts must be positive", DataTransferErrorKind.dtekConfig
    )
  if retryDelay < 0.seconds:
    raise newDataTransferError(
      "retryDelay cannot be negative", DataTransferErrorKind.dtekConfig
    )

  var attempt = 0
  var lastErr: ref DataTransferError
  while attempt < maxAttempts:
    inc attempt
    try:
      return await sendDataTransferOnce(
        dial,
        peerId,
        message,
        timeout = timeout,
        maxResponseBytes = maxResponseBytes,
        expectResponse = expectResponse,
      )
    except CancelledError as exc:
      raise exc
    except DataTransferError as err:
      lastErr = err
      if (not err.kind.isRetryable()) or attempt >= maxAttempts:
        raise err
      trace "data transfer attempt failed, retrying",
        attempt,
        maxAttempts,
        peerId,
        transferId = message.transferId,
        err = err.msg
      if retryDelay > 0.seconds:
        await sleepAsync(retryDelay)

  if lastErr.isNil:
    raise newDataTransferError(
      "data transfer failed without capturing error", DataTransferErrorKind.dtekConfig
    )
  raise lastErr

{.pop.}
