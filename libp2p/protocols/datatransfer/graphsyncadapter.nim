# Nim-LibP2P
# Licensed under either of
#  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
#  * MIT license ([LICENSE-MIT](LICENSE-MIT))
# at your option.

import std/[options, tables, strutils, sequtils]
import chronos, chronicles

import ../../cid
import ../../stream/connection
import ../../switch as switchmod
import ../bitswap/store
import ../graphsync/graphsync
import ../graphsync/protobuf
import ./channelmanager
import ./datatransfer
from ./protobuf import DataTransferMessage

logScope:
  topics = "libp2p datatransfer graphsync"

type
  GraphSyncFetchHook* = proc(
      channel: DataTransferChannel, result: GraphSyncFetchResult
  ): Future[void] {.gcsafe, raises: [].}

  GraphSyncTransferConfig* = object
    timeout*: Duration
    storeBlocks*: bool
    autoComplete*: bool
    maxRetries*: int
    retryDelay*: Duration

  GraphSyncDataTransfer* = ref object
    sw: switchmod.Switch
    manager: DataTransferChannelManager
    graphsync: GraphSyncService
    blockStore: BitswapBlockStore
    config: GraphSyncTransferConfig
    hook: GraphSyncFetchHook
    downstream: DataTransferEventHandler
    pending: Table[string, Future[void]]
    lock: AsyncLock

proc init*(
    _: type GraphSyncTransferConfig,
    timeout: Duration = 30.seconds,
    storeBlocks: bool = true,
    autoComplete: bool = true,
    maxRetries: int = 2,
    retryDelay: Duration = 500.milliseconds,
): GraphSyncTransferConfig =
  if timeout <= 0.seconds:
    raise newException(Defect, "graphsync transfer timeout must be positive")
  if maxRetries < 0:
    raise newException(Defect, "graphsync transfer maxRetries cannot be negative")
  if retryDelay < 0.seconds:
    raise newException(Defect, "graphsync transfer retryDelay cannot be negative")
  GraphSyncTransferConfig(
    timeout: timeout,
    storeBlocks: storeBlocks,
    autoComplete: autoComplete,
    maxRetries: maxRetries,
    retryDelay: retryDelay,
  )

proc channelKey(channel: DataTransferChannel): string =
  $channel.peerId & ":" & $channel.transferId

proc resolveStore(adapter: GraphSyncDataTransfer): BitswapBlockStore =
  if not adapter.blockStore.isNil():
    return adapter.blockStore
  adapter.graphsync.store

func needsGraphsyncFetch(channel: DataTransferChannel): bool =
  (channel.direction == DataTransferChannelDirection.dtcdOutbound and channel.request.isPull) or
  (channel.direction == DataTransferChannelDirection.dtcdInbound and not channel.request.isPull)

func isSuccess(status: GraphSyncResponseCode): bool =
  status in {
    GraphSyncResponseCode.gsrsRequestCompletedFull,
    GraphSyncResponseCode.gsrsRequestCompletedPartial,
  }

func toHuman(status: GraphSyncResponseCode): string =
  case status
  of GraphSyncResponseCode.gsrsRequestAcknowledged:
    "acknowledged"
  of GraphSyncResponseCode.gsrsAdditionalPeers:
    "additional peers advertised"
  of GraphSyncResponseCode.gsrsNotEnoughGas:
    "not enough gas"
  of GraphSyncResponseCode.gsrsOtherProtocol:
    "other protocol required"
  of GraphSyncResponseCode.gsrsPartialResponse:
    "partial response"
  of GraphSyncResponseCode.gsrsRequestPaused:
    "request paused"
  of GraphSyncResponseCode.gsrsRequestCompletedFull:
    "completed"
  of GraphSyncResponseCode.gsrsRequestCompletedPartial:
    "completed (partial)"
  of GraphSyncResponseCode.gsrsRequestRejected:
    "rejected"
  of GraphSyncResponseCode.gsrsRequestFailedBusy:
    "busy"
  of GraphSyncResponseCode.gsrsRequestFailedUnknown:
    "unknown failure"
  of GraphSyncResponseCode.gsrsRequestFailedLegal:
    "legal failure"
  of GraphSyncResponseCode.gsrsRequestFailedContentNotFound:
    "content not found"
  of GraphSyncResponseCode.gsrsRequestCancelled:
    "cancelled"

proc extensionMessage(fetchResult: GraphSyncFetchResult): Option[string] =
  if fetchResult.extensions.hasKey("graphsync/message"):
    let data = fetchResult.extensions["graphsync/message"]
    if data.len > 0:
      var msg = newString(data.len)
      for i, b in data:
        msg[i] = char(b)
      return some(msg)
  none(string)

proc statusMessage(fetchResult: GraphSyncFetchResult): Option[string] =
  var parts: seq[string] = @["graphsync " & toHuman(fetchResult.status)]
  extensionMessage(fetchResult).withValue(message):
    parts.add(message)
  if fetchResult.metadata.len > 0:
    let missing = fetchResult.metadata.countIt(
      it[1] == GraphSyncLinkAction.gslaMissing
    )
    if missing > 0:
      parts.add($(missing) & " missing links")
  some(parts.join("; "))

proc toGraphSyncExtensions(channel: DataTransferChannel): seq[(string, seq[byte])] =
  result = @[]
  for ext in channel.extensions:
    result.add((ext.name, ext.data))

proc processEvent(adapter: GraphSyncDataTransfer, event: DataTransferEvent) {.async.}

proc handleEvent(adapter: GraphSyncDataTransfer, event: DataTransferEvent) =
  if adapter.isNil():
    return
  asyncSpawn adapter.processEvent(event)
  if not adapter.downstream.isNil():
    try:
      adapter.downstream(event)
    except CatchableError as exc:
      debug "graphsync downstream event handler raised", peer = event.channel.peerId,
        transferId = event.channel.transferId, kind = event.kind, err = exc.msg

proc newGraphSyncDataTransfer*(
    sw: switchmod.Switch,
    manager: DataTransferChannelManager,
    graphsync: GraphSyncService,
    blockStore: BitswapBlockStore = nil,
    config: GraphSyncTransferConfig = GraphSyncTransferConfig.init(),
    eventHandler: DataTransferEventHandler = nil,
    fetchHook: GraphSyncFetchHook = nil,
): GraphSyncDataTransfer =
  if sw.isNil():
    raise newException(Defect, "graphsync data transfer requires a switch")
  if manager.isNil():
    raise newException(Defect, "graphsync data transfer requires a channel manager")
  if graphsync.isNil():
    raise newException(Defect, "graphsync data transfer requires a graphsync service")

  let adapter = GraphSyncDataTransfer(
    sw: sw,
    manager: manager,
    graphsync: graphsync,
    blockStore: blockStore,
    config: config,
    hook: fetchHook,
    downstream: eventHandler,
    pending: initTable[string, Future[void]](),
    lock: newAsyncLock(),
  )
  manager.addEventHandler(proc(event: DataTransferEvent) =
    adapter.handleEvent(event)
  )
  adapter

proc cleanupTask(
    adapter: GraphSyncDataTransfer, channel: DataTransferChannel
) {.async: (raises: []).} =
  var lockHeld = false
  try:
    await adapter.lock.acquire()
    lockHeld = true
    let key = channel.channelKey()
    if key in adapter.pending:
      adapter.pending.del(key)
  except CancelledError:
    discard
  finally:
    if lockHeld:
      try:
        adapter.lock.release()
      except AsyncLockError:
        discard

proc runFetch(
    adapter: GraphSyncDataTransfer, channel: DataTransferChannel
) {.async: (raises: [CancelledError]).} =
  let key = channel.channelKey()
  try:
    if channel.request.baseCid.isNone():
      warn "graphsync data transfer missing base CID",
        peer = channel.peerId, transferId = channel.transferId
      return
    let root = channel.request.baseCid.get()
    let extensions = toGraphSyncExtensions(channel)

    trace "graphsync data transfer starting fetch",
      peer = channel.peerId,
      transferId = channel.transferId,
      root = $root,
      direction = $channel.direction,
      isPull = channel.request.isPull

    let maxAttempts = max(0, adapter.config.maxRetries) + 1
    var attempt = 0
    var lastStatus = GraphSyncResponseCode.gsrsRequestFailedUnknown
    var lastExtensions = initTable[string, seq[byte]]()
    var blocksMap = initTable[string, (Cid, seq[byte])]()
    var metadataMap = initTable[string, (Cid, GraphSyncLinkAction)]()

    while attempt < maxAttempts:
      inc attempt
      trace "graphsync data transfer fetch attempt",
        peer = channel.peerId,
        transferId = channel.transferId,
        attempt = attempt,
        maxAttempts = maxAttempts,
        direction = $channel.direction,
        isPull = channel.request.isPull

      let result = await adapter.graphsync.request(
        adapter.sw,
        channel.peerId,
        root,
        selector = channel.request.selector,
        extensions = extensions,
        timeout = adapter.config.timeout,
      )

      lastStatus = result.status
      lastExtensions = result.extensions

      for (cid, data) in result.blocks:
        let blockKey = $cid
        if not blocksMap.hasKey(blockKey):
          blocksMap[blockKey] = (cid, data)

      var missingThisAttempt = 0
      for (cid, action) in result.metadata:
        let metaKey = $cid
        metadataMap[metaKey] = (cid, action)
        if action == GraphSyncLinkAction.gslaMissing:
          inc missingThisAttempt

      if missingThisAttempt == 0 or not result.status.isSuccess() or attempt >= maxAttempts:
        if missingThisAttempt > 0 and result.status.isSuccess() and attempt >= maxAttempts:
          warn "graphsync data transfer retries exhausted with missing blocks",
            peer = channel.peerId, transferId = channel.transferId, missing = missingThisAttempt
        break

      trace "graphsync data transfer retrying missing blocks",
        peer = channel.peerId, transferId = channel.transferId, missing = missingThisAttempt,
        nextAttempt = attempt + 1
      if adapter.config.retryDelay > 0.seconds:
        await sleepAsync(adapter.config.retryDelay)

    var finalBlocks: seq[(Cid, seq[byte])] = @[]
    for _, entry in blocksMap:
      finalBlocks.add(entry)

    var finalMetadata: seq[(Cid, GraphSyncLinkAction)] = @[]
    var missingRemaining = 0
    for _, entry in metadataMap:
      let (cidEntry, action) = entry
      finalMetadata.add((cidEntry, action))
      if action == GraphSyncLinkAction.gslaMissing:
        inc missingRemaining

    var finalStatus = lastStatus
    if missingRemaining > 0 and finalStatus.isSuccess():
      finalStatus = GraphSyncResponseCode.gsrsRequestFailedContentNotFound

    let finalResult = GraphSyncFetchResult(
      status: finalStatus,
      blocks: finalBlocks,
      metadata: finalMetadata,
      extensions: lastExtensions,
    )

    if adapter.config.storeBlocks:
      let store = adapter.resolveStore()
      if store.isNil():
        debug "graphsync data transfer received blocks without store",
          peer = channel.peerId, transferId = channel.transferId, blocks = finalResult.blocks.len
      else:
        for (cid, data) in finalResult.blocks:
          try:
            await store.putBlock(cid, data)
          except CancelledError as exc:
            raise exc
          except CatchableError as exc:
            warn "graphsync data transfer failed storing block",
              peer = channel.peerId, transferId = channel.transferId, cid = $cid, err = exc.msg

    if not adapter.hook.isNil():
      try:
        await adapter.hook(channel, finalResult)
      except CancelledError as exc:
        raise exc
      except CatchableError as exc:
        warn "graphsync data transfer hook raised",
          peer = channel.peerId, transferId = channel.transferId, err = exc.msg

    if not finalResult.status.isSuccess():
      let message = statusMessage(finalResult)
      warn "graphsync data transfer fetch failed",
        peer = channel.peerId, transferId = channel.transferId, status = finalResult.status
      try:
        discard await adapter.manager.cancelChannel(
          channel.peerId, channel.transferId, status = message
        )
      except CancelledError as exc:
        raise exc
      except DataTransferError as exc:
        warn "graphsync data transfer cancel failed",
          peer = channel.peerId, transferId = channel.transferId, err = exc.msg
      return

    if adapter.config.autoComplete:
      let message = statusMessage(finalResult)
      try:
        discard await adapter.manager.completeChannel(
          channel.peerId, channel.transferId, status = message
        )
      except CancelledError as exc:
        raise exc
      except DataTransferError as exc:
        warn "graphsync data transfer complete failed",
          peer = channel.peerId, transferId = channel.transferId, err = exc.msg
  except CancelledError:
    trace "graphsync data transfer fetch cancelled",
      peer = channel.peerId, transferId = channel.transferId
  except GraphSyncError as exc:
    let message = some("graphsync error (" & $exc.kind & "): " & exc.msg)
    warn "graphsync data transfer errored",
      peer = channel.peerId, transferId = channel.transferId, err = exc.msg
    try:
      discard await adapter.manager.cancelChannel(
        channel.peerId, channel.transferId, status = message
      )
    except CancelledError as exc:
      raise exc
    except DataTransferError as cancelExc:
      warn "graphsync data transfer cancel failed after error",
        peer = channel.peerId, transferId = channel.transferId, err = cancelExc.msg
  except CatchableError as exc:
    let message = some("graphsync adapter failure: " & exc.msg)
    warn "graphsync data transfer unexpected exception",
      peer = channel.peerId, transferId = channel.transferId, err = exc.msg
    try:
      discard await adapter.manager.cancelChannel(
        channel.peerId, channel.transferId, status = message
      )
    except CancelledError as exc:
      raise exc
    except DataTransferError as cancelExc:
      warn "graphsync data transfer cancel failed after exception",
        peer = channel.peerId, transferId = channel.transferId, err = cancelExc.msg
  finally:
    await adapter.cleanupTask(channel)

proc startPull(adapter: GraphSyncDataTransfer, channel: DataTransferChannel) {.async.} =
  if not channel.needsGraphsyncFetch():
    return
  await adapter.lock.acquire()
  var alreadyRunning = false
  try:
    let key = channel.channelKey()
    if key in adapter.pending:
      let fut = adapter.pending[key]
      if fut.isNil() or fut.finished():
        adapter.pending.del(key)
      else:
        alreadyRunning = true
    if not alreadyRunning:
      let fut = adapter.runFetch(channel)
      adapter.pending[key] = fut
      asyncSpawn fut
  finally:
    try:
      adapter.lock.release()
    except AsyncLockError:
      discard

proc cancelPull(
    adapter: GraphSyncDataTransfer, channel: DataTransferChannel
) {.async.} =
  await adapter.lock.acquire()
  defer:
    try:
      adapter.lock.release()
    except AsyncLockError:
      discard
  let key = channel.channelKey()
  if key in adapter.pending:
    let fut = adapter.pending[key]
    if not fut.isNil() and not fut.finished():
      fut.cancelSoon()

proc processEvent(adapter: GraphSyncDataTransfer, event: DataTransferEvent) {.async.} =
  try:
    case event.kind
    of DataTransferEventKind.dtekOpened:
      if event.channel.needsGraphsyncFetch():
        await adapter.startPull(event.channel)
    of DataTransferEventKind.dtekAccepted, DataTransferEventKind.dtekOngoing, DataTransferEventKind.dtekResumed:
      await adapter.startPull(event.channel)
    of DataTransferEventKind.dtekPaused:
      await adapter.cancelPull(event.channel)
    of DataTransferEventKind.dtekCancelled, DataTransferEventKind.dtekFailed,
        DataTransferEventKind.dtekCompleted, DataTransferEventKind.dtekVoucherRejected:
      await adapter.cancelPull(event.channel)
      await adapter.cleanupTask(event.channel)
    else:
      discard
  except CancelledError:
    trace "graphsync data transfer event cancelled",
      peer = event.channel.peerId, transferId = event.channel.transferId, kind = event.kind
  except CatchableError as exc:
    warn "graphsync data transfer event handler error",
      peer = event.channel.peerId, transferId = event.channel.transferId, kind = event.kind,
      err = exc.msg

proc handler*(adapter: GraphSyncDataTransfer): DataTransferHandler =
  if adapter.isNil():
    return nil
  result =
    proc(
        conn: Connection, message: DataTransferMessage
    ): Future[Option[DataTransferMessage]] {.gcsafe, raises: [].} =
      adapter.manager.handleMessage(conn, message)
