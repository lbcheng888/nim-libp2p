# Nim-LibP2P
# Copyright (c)
# Licensed under either of
#  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
#  * MIT license ([LICENSE-MIT](LICENSE-MIT))
# at your option.

{.push raises: [].}

import std/[options]
import chronos, chronicles

import ../../switch
import ../../peerid
import ../../stream/connection
import ../../protocols/protocol
import ../../errors
import ../../utility
import ./protobuf

const
  FetchCodec* = "/libp2p/fetch/0.0.1"
  DefaultFetchMessageSize* = 1024 * 512

logScope:
  topics = "libp2p fetch"

type
  FetchErrorKind* {.pure.} = enum
    feDialFailure,
    feTimeout,
    feWriteFailure,
    feReadFailure,
    feInvalidResponse,
    feConfig

  FetchError* = object of LPError
    kind*: FetchErrorKind

  FetchHandler* {.public.} = proc(key: string): Future[FetchResponse] {.
    gcsafe, raises: []
  .}

  FetchConfig* = object
    maxRequestBytes*: int
    maxResponseBytes*: int
    handlerTimeout*: Duration

  FetchService* = ref object of LPProtocol
    fetchHandler*: FetchHandler
    config*: FetchConfig

proc init*(
    _: type FetchConfig,
    maxRequestBytes: int = DefaultFetchMessageSize,
    maxResponseBytes: int = DefaultFetchMessageSize,
    handlerTimeout: Duration = 10.seconds,
): FetchConfig =
  FetchConfig(
    maxRequestBytes: maxRequestBytes,
    maxResponseBytes: maxResponseBytes,
    handlerTimeout: handlerTimeout,
  )

proc validate(config: FetchConfig) =
  if config.maxRequestBytes <= 0:
    raise newException(Defect, "fetch maxRequestBytes must be positive")
  if config.maxResponseBytes <= 0:
    raise newException(Defect, "fetch maxResponseBytes must be positive")
  if config.handlerTimeout < 0.seconds:
    raise newException(Defect, "fetch handlerTimeout cannot be negative")

proc new*(
    T: type FetchService, handler: FetchHandler, config: FetchConfig = FetchConfig.init()
): T {.public.} =
  validate(config)
  let svc = FetchService(fetchHandler: handler, config: config)
  svc.init()
  svc

proc sendError(conn: Connection, status: FetchStatus) {.async.} =
  var resp = FetchResponse(status: status, data: @[])
  await conn.writeLp(resp.encode())

method init*(svc: FetchService) =
  proc handle(conn: Connection, proto: string) {.async: (raises: [CancelledError]).} =
    trace "handling fetch request", conn
    try:
      let payload = await conn.readLp(svc.config.maxRequestBytes)
      let reqOpt = decodeFetchRequest(payload)
      if reqOpt.isNone():
        debug "failed decoding fetch request", conn
        try:
          await conn.sendError(FetchStatus.fsError)
        except CatchableError as sendErr:
          trace "failed sending fetch error", conn, err = sendErr.msg
        return
      let request = reqOpt.get()
      var response: FetchResponse
      if svc.fetchHandler.isNil:
        response = FetchResponse(status: FetchStatus.fsNotFound, data: @[])
      else:
        if svc.config.handlerTimeout > 0.seconds:
          let handlerFuture = svc.fetchHandler(request.identifier)
          let completed = await withTimeout(handlerFuture, svc.config.handlerTimeout)
          if not completed:
            trace "fetch handler timeout", conn, key = request.identifier
            response = FetchResponse(status: FetchStatus.fsError, data: @[])
          else:
            response = await handlerFuture
        else:
          response = await svc.fetchHandler(request.identifier)
      if response.data.len > svc.config.maxResponseBytes:
        trace "fetch response exceeded maxResponseBytes",
          size = response.data.len, limit = svc.config.maxResponseBytes
        try:
          await conn.sendError(FetchStatus.fsTooLarge)
        except CatchableError as sendErr:
          trace "failed sending fetch too-large error", conn, err = sendErr.msg
        return
      await conn.writeLp(response.encode())
    except CancelledError as exc:
      trace "fetch handler cancelled", conn
      raise exc
    except LPStreamError as exc:
      trace "stream error during fetch", conn, err = exc.msg
    except CatchableError as exc:
      trace "exception in fetch handler", conn, err = exc.msg
      try:
        await conn.sendError(FetchStatus.fsError)
      except CatchableError as sendErr:
        trace "failed sending fetch handler error", conn, err = sendErr.msg

  svc.handler = handle
  svc.codec = FetchCodec

proc fetchOnce(
    sw: Switch,
    peerId: PeerId,
    key: string,
    timeout: Duration,
    maxResponseBytes: int,
): Future[FetchResponse] {.async: (raises: [CancelledError, FetchError]).} =
  logScope:
    peerId
    key

  trace "starting fetch request attempt"
  let conn =
    try:
      await sw.dial(peerId, @[FetchCodec])
    except CancelledError as exc:
      raise exc
    except CatchableError as exc:
      raise (ref FetchError)(
        msg: "fetch dial failed: " & exc.msg,
        parent: exc,
        kind: FetchErrorKind.feDialFailure,
      )

  defer:
    await conn.close()

  if maxResponseBytes <= 0:
    raise (ref FetchError)(
      msg: "maxResponseBytes must be positive", kind: FetchErrorKind.feConfig
    )

  let req = FetchRequest(identifier: key)
  try:
    await conn.writeLp(req.encode())
  except LPStreamError as exc:
    raise (ref FetchError)(
      msg: "fetch write failed: " & exc.msg,
      parent: exc,
      kind: FetchErrorKind.feWriteFailure,
    )

  let readFuture = conn.readLp(maxResponseBytes)
  let completed =
    try:
      await withTimeout(readFuture, timeout)
    except CancelledError as exc:
      raise exc
  if not completed:
    raise (ref FetchError)(
      msg: "fetch response timeout", kind: FetchErrorKind.feTimeout
    )

  let payload =
    try:
      await readFuture
    except CancelledError as exc:
      raise exc
    except LPStreamError as exc:
      raise (ref FetchError)(
        msg: "fetch read failed: " & exc.msg,
        parent: exc,
        kind: FetchErrorKind.feReadFailure,
      )

  let respOpt = decodeFetchResponse(payload)
  if respOpt.isNone():
    raise (ref FetchError)(
      msg: "invalid fetch response payload", kind: FetchErrorKind.feInvalidResponse
    )
  respOpt.get()

proc fetch*(
    sw: Switch,
    peerId: PeerId,
    key: string,
    timeout: Duration = 10.seconds,
    maxResponseBytes = DefaultFetchMessageSize,
    maxAttempts: int = 3,
    retryDelay: Duration = 200.milliseconds,
): Future[FetchResponse] {.async: (raises: [CancelledError, FetchError]).} =
  if maxAttempts <= 0:
    raise (ref FetchError)(
      msg: "maxAttempts must be positive", kind: FetchErrorKind.feConfig
    )
  if retryDelay < 0.seconds:
    raise (ref FetchError)(
      msg: "retryDelay cannot be negative", kind: FetchErrorKind.feConfig
    )

  var attempt = 0
  var lastErr: ref FetchError
  while attempt < maxAttempts:
    inc attempt
    try:
      return await fetchOnce(sw, peerId, key, timeout, maxResponseBytes)
    except CancelledError as exc:
      raise exc
    except FetchError as err:
      lastErr = err
      if attempt >= maxAttempts:
        raise err
      trace "fetch attempt failed, retrying",
        attempt, maxAttempts, peerId, key, err = err.msg
      if retryDelay > 0.seconds:
        await sleepAsync(retryDelay)
  if lastErr.isNil:
    raise (ref FetchError)(
      msg: "fetch failed without capturing error", kind: FetchErrorKind.feConfig
    )
  raise lastErr
