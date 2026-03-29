# Nim-LibP2P
# Copyright (c)
# Licensed under either of
#  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
#  * MIT license ([LICENSE-MIT](LICENSE-MIT))
# at your option.

{.push raises: [].}

import std/options
from std/times import epochTime
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

proc diagKey(key: string): string =
  let idx = key.find(':')
  if idx >= 0:
    return key[0 ..< idx]
  key

proc diagNowMs(): int64 =
  int64(epochTime() * 1000)

method init*(svc: FetchService) =
  proc handle(conn: Connection, proto: string) {.async: (raises: [CancelledError]).} =
    trace "handling fetch request", conn
    when defined(fetch_diag) or defined(fabric_lsmr_diag):
      echo "t=", diagNowMs(),
        " fetch-server enter peer=", $conn.peerId,
        " proto=", proto
    try:
      when defined(fetch_diag) or defined(fabric_lsmr_diag):
        echo "t=", diagNowMs(),
          " fetch-server read-begin peer=", $conn.peerId
      let payload = await conn.readLp(svc.config.maxRequestBytes)
      when defined(fetch_diag) or defined(fabric_lsmr_diag):
        echo "t=", diagNowMs(),
          " fetch-server read-ok peer=", $conn.peerId,
          " bytes=", payload.len
      let reqOpt = decodeFetchRequest(payload)
      if reqOpt.isNone():
        when defined(fetch_diag) or defined(fabric_lsmr_diag):
          echo "t=", diagNowMs(),
            " fetch-server decode-invalid peer=", $conn.peerId
        debug "failed decoding fetch request", conn
        try:
          await conn.sendError(FetchStatus.fsError)
        except CatchableError as sendErr:
          trace "failed sending fetch error", conn, err = sendErr.msg
        return
      let request = reqOpt.get()
      when defined(fetch_diag) or defined(fabric_lsmr_diag):
        echo "t=", diagNowMs(),
          " fetch-server decode-ok peer=", $conn.peerId,
          " key=", diagKey(request.identifier)
      var response: FetchResponse
      if svc.fetchHandler.isNil:
        response = FetchResponse(status: FetchStatus.fsNotFound, data: @[])
      else:
        when defined(fetch_diag) or defined(fabric_lsmr_diag):
          echo "t=", diagNowMs(),
            " fetch-server handler-begin peer=", $conn.peerId,
            " key=", diagKey(request.identifier)
        if svc.config.handlerTimeout > 0.seconds:
          let handlerFuture = svc.fetchHandler(request.identifier)
          let completed = await withTimeout(handlerFuture, svc.config.handlerTimeout)
          if not completed:
            trace "fetch handler timeout", conn, key = request.identifier
            when defined(fetch_diag) or defined(fabric_lsmr_diag):
              echo "t=", diagNowMs(),
                " fetch-server handler-timeout peer=", $conn.peerId,
                " key=", diagKey(request.identifier)
            response = FetchResponse(status: FetchStatus.fsError, data: @[])
          else:
            response = await handlerFuture
        else:
          response = await svc.fetchHandler(request.identifier)
        when defined(fetch_diag) or defined(fabric_lsmr_diag):
          echo "t=", diagNowMs(),
            " fetch-server handler-done peer=", $conn.peerId,
            " key=", diagKey(request.identifier),
            " status=", ord(response.status)
      if response.data.len > svc.config.maxResponseBytes:
        trace "fetch response exceeded maxResponseBytes",
          size = response.data.len, limit = svc.config.maxResponseBytes
        try:
          await conn.sendError(FetchStatus.fsTooLarge)
        except CatchableError as sendErr:
          trace "failed sending fetch too-large error", conn, err = sendErr.msg
        return
      when defined(fetch_diag) or defined(fabric_lsmr_diag):
        echo "t=", diagNowMs(),
          " fetch-server write-begin peer=", $conn.peerId,
          " key=", diagKey(request.identifier)
      await conn.writeLp(response.encode())
      when defined(fetch_diag) or defined(fabric_lsmr_diag):
        echo "t=", diagNowMs(),
          " fetch-server write-done peer=", $conn.peerId,
          " key=", diagKey(request.identifier)
    except CancelledError as exc:
      trace "fetch handler cancelled", conn
      when defined(fetch_diag) or defined(fabric_lsmr_diag):
        echo "t=", diagNowMs(),
          " fetch-server cancelled peer=", $conn.peerId
      raise exc
    except LPStreamError as exc:
      trace "stream error during fetch", conn, err = exc.msg
      when defined(fetch_diag) or defined(fabric_lsmr_diag):
        echo "t=", diagNowMs(),
          " fetch-server stream-exc peer=", $conn.peerId,
          " err=", exc.msg
    except CatchableError as exc:
      trace "exception in fetch handler", conn, err = exc.msg
      when defined(fetch_diag) or defined(fabric_lsmr_diag):
        echo "t=", diagNowMs(),
          " fetch-server exc peer=", $conn.peerId,
          " err=", exc.msg
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
  when defined(fetch_diag) or defined(fabric_lsmr_diag):
    echo "t=", diagNowMs(),
      " fetch-stage dial-begin peer=", $peerId,
      " key=", diagKey(key)
  let conn =
    try:
      let opened = await sw.dial(peerId, @[FetchCodec])
      when defined(fetch_diag) or defined(fabric_lsmr_diag):
        echo "t=", diagNowMs(),
          " fetch-stage dial-ok peer=", $peerId,
          " key=", diagKey(key)
      opened
    except CancelledError as exc:
      raise exc
    except CatchableError as exc:
      when defined(fetch_diag) or defined(fabric_lsmr_diag):
        echo "t=", diagNowMs(),
          " fetch-stage dial-exc peer=", $peerId,
          " key=", diagKey(key),
          " err=", exc.msg
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
    when defined(fetch_diag) or defined(fabric_lsmr_diag):
      echo "t=", diagNowMs(),
        " fetch-stage write-begin peer=", $peerId,
        " key=", diagKey(key)
    await conn.writeLp(req.encode())
    when defined(fetch_diag) or defined(fabric_lsmr_diag):
      echo "t=", diagNowMs(),
        " fetch-stage write-ok peer=", $peerId,
        " key=", diagKey(key)
  except LPStreamError as exc:
    when defined(fetch_diag) or defined(fabric_lsmr_diag):
      echo "t=", diagNowMs(),
        " fetch-stage write-exc peer=", $peerId,
        " key=", diagKey(key),
        " err=", exc.msg
    raise (ref FetchError)(
      msg: "fetch write failed: " & exc.msg,
      parent: exc,
      kind: FetchErrorKind.feWriteFailure,
    )

  let readFuture = conn.readLp(maxResponseBytes)
  when defined(fetch_diag) or defined(fabric_lsmr_diag):
    echo "t=", diagNowMs(),
      " fetch-stage read-begin peer=", $peerId,
      " key=", diagKey(key),
      " timeoutMs=", timeout.milliseconds
  let completed =
    try:
      await withTimeout(readFuture, timeout)
    except CancelledError as exc:
      raise exc
  if not completed:
    when defined(fetch_diag) or defined(fabric_lsmr_diag):
      echo "t=", diagNowMs(),
        " fetch-stage read-timeout peer=", $peerId,
        " key=", diagKey(key)
    raise (ref FetchError)(
      msg: "fetch response timeout", kind: FetchErrorKind.feTimeout
    )

  let payload =
    try:
      await readFuture
    except CancelledError as exc:
      raise exc
    except LPStreamError as exc:
      when defined(fetch_diag) or defined(fabric_lsmr_diag):
        echo "t=", diagNowMs(),
          " fetch-stage read-exc peer=", $peerId,
          " key=", diagKey(key),
          " err=", exc.msg
      raise (ref FetchError)(
        msg: "fetch read failed: " & exc.msg,
        parent: exc,
        kind: FetchErrorKind.feReadFailure,
      )
  when defined(fetch_diag) or defined(fabric_lsmr_diag):
    echo "t=", diagNowMs(),
      " fetch-stage read-ok peer=", $peerId,
      " key=", diagKey(key),
      " bytes=", payload.len

  let respOpt = decodeFetchResponse(payload)
  if respOpt.isNone():
    when defined(fetch_diag) or defined(fabric_lsmr_diag):
      echo "t=", diagNowMs(),
        " fetch-stage decode-invalid peer=", $peerId,
        " key=", diagKey(key)
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
      if err.kind in {FetchErrorKind.feWriteFailure, FetchErrorKind.feReadFailure}:
        when defined(fetch_diag) or defined(fabric_lsmr_diag):
          echo "fetch-stage retry-drop-peer peer=", $peerId,
            " key=", diagKey(key),
            " kind=", ord(err.kind)
        try:
          await sw.connManager.dropPeer(peerId)
        except CancelledError as exc:
          raise exc
        except CatchableError:
          discard
      trace "fetch attempt failed, retrying",
        attempt, maxAttempts, peerId, key, err = err.msg
      if retryDelay > 0.seconds:
        await sleepAsync(retryDelay)
  if lastErr.isNil:
    raise (ref FetchError)(
      msg: "fetch failed without capturing error", kind: FetchErrorKind.feConfig
    )
  raise lastErr
