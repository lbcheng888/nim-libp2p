# Nim-LibP2P
# Copyright (c)
# Licensed under either of
#  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
#  * MIT license ([LICENSE-MIT](LICENSE-MIT))
# at your option.
# This file may not be copied, modified, or distributed except according to
# those terms.

{.push raises: [].}

import std/[options, tables, deques]
import chronos, chronicles

import ../../cid
import ../../errors
import ../../multicodec
import ../../multihash
import ../../peerid
import ../../protocols/protocol
import ../../stream/connection
import ../../switch
import ../../vbuffer
import ../../protobuf/minprotobuf
import ../bitswap/store
import ./protobuf
import ./selector
import stew/byteutils

const
  GraphSyncCodec* = "/ipfs/graphsync/1.0.0"
  DefaultMaxGraphSyncMessageBytes* = 4 * 1024 * 1024
  DefaultMaxTraversalBlocks* = 2048
  DefaultGraphSyncAttempts* = 3
  DefaultGraphSyncRetryDelay* = 400.milliseconds

logScope:
  topics = "libp2p graphsync"

type
  GraphSyncErrorKind* {.pure.} = enum
    gsekDialFailure
    gsekTimeout
    gsekWriteFailure
    gsekReadFailure
    gsekDecodeFailure
    gsekValidationFailure
    gsekNotFound
    gsekUnsupported
    gsekConfig

  GraphSyncError* = object of LPError
    kind*: GraphSyncErrorKind

  GraphSyncResponseCode* {.pure.} = enum
    gsrsRequestAcknowledged = 10
    gsrsAdditionalPeers = 11
    gsrsNotEnoughGas = 12
    gsrsOtherProtocol = 13
    gsrsPartialResponse = 14
    gsrsRequestPaused = 15
    gsrsRequestCompletedFull = 20
    gsrsRequestCompletedPartial = 21
    gsrsRequestRejected = 30
    gsrsRequestFailedBusy = 31
    gsrsRequestFailedUnknown = 32
    gsrsRequestFailedLegal = 33
    gsrsRequestFailedContentNotFound = 34
    gsrsRequestCancelled = 35

  GraphSyncBlockProvider* = proc(cid: Cid): Future[Option[seq[byte]]] {.gcsafe, raises: [].}

  GraphSyncConfig* = object
    maxMessageBytes*: int
    maxTraversalBlocks*: int
    maxAttempts*: int
    retryDelay*: Duration
  GraphSyncStats* = object
    inboundRequests*: uint64
    inboundCompleted*: uint64
    inboundPartial*: uint64
    inboundFailed*: uint64
    inboundBlocks*: uint64
    inboundBytes*: uint64
    inboundMissing*: uint64
    outboundRequests*: uint64
    outboundCompleted*: uint64
    outboundPartial*: uint64
    outboundFailed*: uint64
    outboundBlocks*: uint64
    outboundBytes*: uint64
    outboundMissing*: uint64

  GraphSyncTraversalState* = ref object
    root*: Cid
    selector*: seq[byte]
    traversal*: GraphSyncTraversal
    visited*: Table[string, bool]
    enqueued*: Table[string, bool]
    queue*: Deque[(Cid, int)]
    completed*: bool

  GraphSyncService* = ref object of LPProtocol
    provider*: GraphSyncBlockProvider
    store*: BitswapBlockStore
    config*: GraphSyncConfig
    requestLock: AsyncLock
    nextRequestId: int32
    stats: GraphSyncStats
    stateLock: AsyncLock
    inboundStates: Table[string, GraphSyncTraversalState]

  GraphSyncFetchResult* = object
    status*: GraphSyncResponseCode
    blocks*: seq[(Cid, seq[byte])]
    metadata*: seq[(Cid, GraphSyncLinkAction)]
    extensions*: Table[string, seq[byte]]

  GraphSyncRequestResult = object
    response: GraphSyncResponse
    blocks: seq[GraphSyncBlock]

proc shouldExploreChildren(
    traversal: GraphSyncTraversal, depth: int
): bool =
  if traversal.mode != GraphSyncTraversalMode.gstTraverseAll:
    return false
  if traversal.depthLimit >= 0 and depth >= traversal.depthLimit:
    return false
  true

proc init*(
    _: type GraphSyncConfig,
    maxMessageBytes: int = DefaultMaxGraphSyncMessageBytes,
    maxTraversalBlocks: int = DefaultMaxTraversalBlocks,
    maxAttempts: int = DefaultGraphSyncAttempts,
    retryDelay: Duration = DefaultGraphSyncRetryDelay,
): GraphSyncConfig =
  GraphSyncConfig(
    maxMessageBytes: maxMessageBytes,
    maxTraversalBlocks: maxTraversalBlocks,
    maxAttempts: maxAttempts,
    retryDelay: retryDelay,
  )

proc validate(config: GraphSyncConfig) =
  if config.maxMessageBytes <= 0:
    raise newException(Defect, "graphsync maxMessageBytes must be positive")
  if config.maxTraversalBlocks < 0:
    raise newException(Defect, "graphsync maxTraversalBlocks cannot be negative")
  if config.maxAttempts <= 0:
    raise newException(Defect, "graphsync maxAttempts must be positive")
  if config.retryDelay < 0.seconds:
    raise newException(Defect, "graphsync retryDelay cannot be negative")

proc newGraphSyncError(
    message: string,
    kind: GraphSyncErrorKind,
    parent: ref CatchableError = nil,
): ref GraphSyncError =
  (ref GraphSyncError)(msg: message, kind: kind, parent: parent)

func isRetryable(kind: GraphSyncErrorKind): bool =
  kind in {
    GraphSyncErrorKind.gsekDialFailure,
    GraphSyncErrorKind.gsekTimeout,
    GraphSyncErrorKind.gsekWriteFailure,
    GraphSyncErrorKind.gsekReadFailure,
  }

proc recordInboundOutcome(
    svc: GraphSyncService,
    status: GraphSyncResponseCode,
    blocks: seq[GraphSyncBlock],
    metadata: seq[GraphSyncMetadataEntry]
) =
  case status
  of GraphSyncResponseCode.gsrsRequestCompletedFull:
    inc svc.stats.inboundCompleted
  of GraphSyncResponseCode.gsrsRequestCompletedPartial:
    inc svc.stats.inboundPartial
  else:
    inc svc.stats.inboundFailed

  svc.stats.inboundBlocks += uint64(blocks.len)
  var totalBytes: uint64 = 0
  for blk in blocks:
    totalBytes += uint64(blk.data.len)
  svc.stats.inboundBytes += totalBytes

  var missing: uint64 = 0
  for entry in metadata:
    if entry.action == GraphSyncLinkAction.gslaMissing:
      inc missing
  svc.stats.inboundMissing += missing

proc recordOutboundOutcome(
    svc: GraphSyncService,
    status: GraphSyncResponseCode,
    blocks: seq[(Cid, seq[byte])],
    metadata: seq[(Cid, GraphSyncLinkAction)]
) =
  case status
  of GraphSyncResponseCode.gsrsRequestCompletedFull:
    inc svc.stats.outboundCompleted
  of GraphSyncResponseCode.gsrsRequestCompletedPartial:
    inc svc.stats.outboundPartial
  else:
    inc svc.stats.outboundFailed

  svc.stats.outboundBlocks += uint64(blocks.len)
  var totalBytes: uint64 = 0
  for (_, data) in blocks:
    totalBytes += uint64(data.len)
  svc.stats.outboundBytes += totalBytes

  var missing: uint64 = 0
  for (_, action) in metadata:
    if action == GraphSyncLinkAction.gslaMissing:
      inc missing
  svc.stats.outboundMissing += missing

proc recordOutboundFailure(svc: GraphSyncService) =
  inc svc.stats.outboundFailed

proc releaseLockSafe(lock: AsyncLock): bool =
  try:
    lock.release()
    true
  except AsyncLockError:
    false

proc stateKey(peerId: PeerId, requestId: int32): string =
  $peerId & ":" & $requestId

proc cidPrefix(cid: Cid): Option[seq[byte]] =
  let mhRes = cid.mhash()
  if mhRes.isErr():
    return none(seq[byte])
  let mh = mhRes.get()
  var prefixBuf = initVBuffer()

  if cid.version() == CIDv1:
    prefixBuf.writeVarint(1'u64)
    prefixBuf.writeVarint(uint64(int(cid.mcodec)))
  elif cid.version() == CIDv0:
    prefixBuf.writeVarint(0'u64)
  else:
    return none(seq[byte])

  if mh.dpos > 0:
    prefixBuf.writeArray(mh.data.buffer.toOpenArray(0, mh.dpos - 1))
  prefixBuf.finish()
  some(prefixBuf.buffer)

proc decodeCidFromPrefix(prefix: seq[byte], data: seq[byte]): Result[Cid, string] =
  if prefix.len == 0:
    return err("empty prefix")

  var vb = initVBuffer(prefix)
  var peekValue: uint64 = 0
  if vb.peekVarint(peekValue) == -1:
    return err("invalid prefix header")

  var version = CIDv0
  var codecVal = uint64(multiCodec("dag-pb"))
  var mhCode: uint64 = 0
  var mhLen: uint64 = 0

  if peekValue <= 1:
    var rawVersion: uint64 = 0
    if vb.readVarint(rawVersion) == -1:
      return err("failed to read cid version")
    case rawVersion
    of 0'u64:
      version = CIDv0
      codecVal = uint64(multiCodec("dag-pb"))
    of 1'u64:
      version = CIDv1
      var codecRaw: uint64 = 0
      if vb.readVarint(codecRaw) == -1:
        return err("failed to read cid codec")
      codecVal = codecRaw
    else:
      return err("unsupported cid version")
    if vb.readVarint(mhCode) == -1:
      return err("failed to read multihash code")
    if vb.readVarint(mhLen) == -1:
      return err("failed to read multihash length")
  else:
    version = CIDv0
    codecVal = uint64(multiCodec("dag-pb"))
    if vb.readVarint(mhCode) == -1:
      return err("failed to read legacy multihash code")
    if vb.readVarint(mhLen) == -1:
      return err("failed to read legacy multihash length")

  let mhRes = MultiHash.digest(int(mhCode), data)
  if mhRes.isErr():
    return err("unable to digest block payload")
  let mh = mhRes.get()
  if mhLen != uint64(mh.size):
    return err("multihash length mismatch")

  let cidRes =
    if version == CIDv0:
      Cid.init(CIDv0, multiCodec("dag-pb"), mh)
    else:
      Cid.init(CIDv1, MultiCodec(codecVal), mh)
  if cidRes.isErr():
    return err("failed constructing cid from prefix")
  ok(cidRes.get())

proc decodeDagPbLink(data: seq[byte], parent: Cid): Option[Cid] =
  var pb = initProtoBuffer(data)
  var hash: seq[byte] = @[]

  while true:
    let field = pb.readFieldNumber()
    if field == 0'u:
      break
    case field
    of 1'u:
      hash = pb.readBytes()
    else:
      pb.skipValue()

  if hash.len == 0:
    return none(Cid)

  var mh: MultiHash
  let decoded = MultiHash.decode(hash, mh)
  if decoded.isErr():
    return none(Cid)

  let parentCodecRes = parent.contentType()
  if parentCodecRes.isErr():
    return none(Cid)
  let parentCodec = parentCodecRes.get()
  let version =
    if parent.version() == CIDv0: CIDv0 else: CIDv1
  let codec =
    if version == CIDv0: multiCodec("dag-pb") else: parentCodec
  let childRes = Cid.init(version, codec, mh)
  if childRes.isErr():
    return none(Cid)
  some(childRes.get())

proc dagPbChildCids(data: seq[byte], parent: Cid): seq[Cid] =
  var pb = initProtoBuffer(data)
  var links: seq[Cid] = @[]

  while true:
    let field = pb.readFieldNumber()
    if field == 0'u:
      break
    case field
    of 2'u:
      let linkBytes = pb.readBytes()
      let linkOpt = decodeDagPbLink(linkBytes, parent)
      linkOpt.withValue(link):
        links.add(link)
    else:
      pb.skipValue()

  links

proc getBlock(
    svc: GraphSyncService, cid: Cid
): Future[Option[seq[byte]]] {.async: (raises: [CancelledError]).} =
  if not svc.provider.isNil():
    try:
      return await svc.provider(cid)
    except CancelledError as exc:
      raise exc
    except CatchableError as exc:
      trace "graphsync provider raised", cid = $cid, err = exc.msg
      return none(seq[byte])
  elif not svc.store.isNil():
    return await svc.store.getBlock(cid)
  none(seq[byte])

proc collectBlocks(
    svc: GraphSyncService, state: GraphSyncTraversalState
): Future[
    (
      seq[GraphSyncBlock],
      seq[GraphSyncMetadataEntry],
      GraphSyncResponseCode,
      Option[string]
    )
  ] {.async: (raises: [CancelledError]).} =
  var blocks: seq[GraphSyncBlock] = @[]
  var metadata: seq[GraphSyncMetadataEntry] = @[]
  var status = GraphSyncResponseCode.gsrsRequestCompletedFull
  var message = none(string)
  var processed = 0

  while state.queue.len > 0:
    if svc.config.maxTraversalBlocks > 0 and
        processed >= svc.config.maxTraversalBlocks:
      status = GraphSyncResponseCode.gsrsRequestCompletedPartial
      message = some("traversal block limit reached")
      break

    let (current, depth) = state.queue.popFirst()
    let currentKey = $current
    if state.enqueued.hasKey(currentKey):
      state.enqueued.del(currentKey)
    if state.visited.hasKey(currentKey):
      metadata.add(
        GraphSyncMetadataEntry(
          link: current.data.buffer, action: GraphSyncLinkAction.gslaDuplicateNotSent
        )
      )
      continue
    state.visited[currentKey] = true

    var markMissing = false
    let dataOpt = await svc.getBlock(current)
    if dataOpt.isNone():
      status = GraphSyncResponseCode.gsrsRequestFailedContentNotFound
      message = some("missing block " & $current)
      markMissing = true
    else:
      let data = dataOpt.get()

      let prefixOpt = cidPrefix(current)
      if prefixOpt.isNone():
        status = GraphSyncResponseCode.gsrsRequestFailedUnknown
        message = some("unable to build prefix for " & $current)
        markMissing = true
      else:
        let blk = GraphSyncBlock(prefix: prefixOpt.get(), data: data)

        let cidRes = decodeCidFromPrefix(blk.prefix, blk.data)
        if cidRes.isErr():
          status = GraphSyncResponseCode.gsrsRequestFailedUnknown
          message = some("cid validation failed for " & $current)
          markMissing = true
        else:
          let derived = cidRes.get()
          if derived != current:
            status = GraphSyncResponseCode.gsrsRequestFailedUnknown
            message = some("cid mismatch for " & $current)
            markMissing = true
          else:
            metadata.add(
              GraphSyncMetadataEntry(
                link: current.data.buffer, action: GraphSyncLinkAction.gslaPresent
              )
            )
            blocks.add(blk)
            inc processed

            let codecRes = current.contentType()
            if codecRes.isErr():
              continue
            let codec = codecRes.get()
            if codec == multiCodec("dag-pb") and
                shouldExploreChildren(state.traversal, depth):
              let children = dagPbChildCids(data, current)
              for child in children:
                let childKey = $child
                if state.visited.hasKey(childKey):
                  metadata.add(
                    GraphSyncMetadataEntry(
                      link: child.data.buffer,
                      action: GraphSyncLinkAction.gslaDuplicateDagSkipped,
                    )
                  )
                  continue
                if state.enqueued.hasKey(childKey):
                  metadata.add(
                    GraphSyncMetadataEntry(
                      link: child.data.buffer,
                      action: GraphSyncLinkAction.gslaDuplicateNotSent,
                    )
                  )
                  continue
                state.queue.addLast((child, depth + 1))
                state.enqueued[childKey] = true
            else:
              trace "graphsync skipping non dag-pb child expansion", cid = $current,
                codec = codec.int
            continue

    if markMissing:
      metadata.add(
        GraphSyncMetadataEntry(
          link: current.data.buffer, action: GraphSyncLinkAction.gslaMissing
        )
      )
      state.visited.del(currentKey)
      if not state.enqueued.hasKey(currentKey):
        state.queue.addFirst((current, depth))
        state.enqueued[currentKey] = true
      break

  if state.queue.len == 0 and status == GraphSyncResponseCode.gsrsRequestCompletedFull:
    state.completed = true
  if status != GraphSyncResponseCode.gsrsRequestCompletedPartial and
      status != GraphSyncResponseCode.gsrsRequestCompletedFull:
    state.completed = true

  (blocks, metadata, status, message)

proc processRequest(
    svc: GraphSyncService, peerId: PeerId, request: GraphSyncRequest
): Future[GraphSyncRequestResult] {.async: (raises: [CancelledError]).} =
  trace "graphsync processing request",
    peerId,
    requestId = request.id,
    cancel = request.cancel,
    update = request.update
  inc svc.stats.inboundRequests

  let key = stateKey(peerId, request.id)

  if request.cancel:
    await svc.stateLock.acquire()
    var released = false
    try:
      if svc.inboundStates.hasKey(key):
        svc.inboundStates.del(key)
      released = releaseLockSafe(svc.stateLock)
    finally:
      if not released:
        discard releaseLockSafe(svc.stateLock)
    let response = GraphSyncResponse(
      id: request.id,
      status: int32(GraphSyncResponseCode.gsrsRequestCancelled),
      metadata: @[],
      extensions: initTable[string, seq[byte]](),
    )
    svc.recordInboundOutcome(
      GraphSyncResponseCode.gsrsRequestCancelled, @[], @[]
    )
    return GraphSyncRequestResult(response: response, blocks: @[])

  var state: GraphSyncTraversalState
  var accessReleased = false
  if request.update:
    await svc.stateLock.acquire()
    try:
      if svc.inboundStates.hasKey(key):
        state = svc.inboundStates.getOrDefault(key, GraphSyncTraversalState(nil))
        svc.inboundStates.del(key)
      accessReleased = releaseLockSafe(svc.stateLock)
    finally:
      if not accessReleased:
        discard releaseLockSafe(svc.stateLock)
    if state.isNil():
      let response = GraphSyncResponse(
        id: request.id,
        status: int32(GraphSyncResponseCode.gsrsRequestFailedUnknown),
        metadata: @[],
        extensions: initTable[string, seq[byte]](),
      )
      svc.recordInboundOutcome(
        GraphSyncResponseCode.gsrsRequestFailedUnknown, @[], @[]
      )
      return GraphSyncRequestResult(response: response, blocks: @[])

    if request.root.len > 0:
      let cidRes = Cid.init(request.root)
      if cidRes.isErr() or cidRes.get() != state.root:
        await svc.stateLock.acquire()
        var lockReleased = false
        try:
          svc.inboundStates[key] = state
          lockReleased = releaseLockSafe(svc.stateLock)
        finally:
          if not lockReleased:
            discard releaseLockSafe(svc.stateLock)
        let response = GraphSyncResponse(
          id: request.id,
          status: int32(GraphSyncResponseCode.gsrsRequestRejected),
          metadata: @[],
          extensions: initTable[string, seq[byte]](),
        )
        svc.recordInboundOutcome(
          GraphSyncResponseCode.gsrsRequestRejected, @[], @[]
        )
        return GraphSyncRequestResult(response: response, blocks: @[])
    if request.selector.len > 0 and request.selector != state.selector:
      await svc.stateLock.acquire()
      var lockReleased = false
      try:
        svc.inboundStates[key] = state
        lockReleased = releaseLockSafe(svc.stateLock)
      finally:
        if not lockReleased:
          discard releaseLockSafe(svc.stateLock)
      let response = GraphSyncResponse(
        id: request.id,
        status: int32(GraphSyncResponseCode.gsrsRequestRejected),
        metadata: @[],
        extensions: initTable[string, seq[byte]](),
      )
      svc.recordInboundOutcome(
        GraphSyncResponseCode.gsrsRequestRejected, @[], @[]
      )
      return GraphSyncRequestResult(response: response, blocks: @[])
  else:
    if request.root.len == 0:
      trace "graphsync missing root cid", peerId, requestId = request.id
      let response = GraphSyncResponse(
        id: request.id,
        status: int32(GraphSyncResponseCode.gsrsRequestFailedUnknown),
        metadata: @[],
        extensions: initTable[string, seq[byte]](),
      )
      svc.recordInboundOutcome(
        GraphSyncResponseCode.gsrsRequestFailedUnknown, @[], @[]
      )
      return GraphSyncRequestResult(response: response, blocks: @[])
    let cidRes = Cid.init(request.root)
    if cidRes.isErr():
      trace "graphsync invalid root cid", peerId, requestId = request.id
      let response = GraphSyncResponse(
        id: request.id,
        status: int32(GraphSyncResponseCode.gsrsRequestFailedUnknown),
        metadata: @[],
        extensions: initTable[string, seq[byte]](),
      )
      svc.recordInboundOutcome(
        GraphSyncResponseCode.gsrsRequestFailedUnknown, @[], @[]
      )
      return GraphSyncRequestResult(response: response, blocks: @[])
    let traversalRes = parseSelector(request.selector)
    if traversalRes.isErr():
      trace "graphsync invalid selector", peerId, requestId = request.id,
        err = traversalRes.error
      let response = GraphSyncResponse(
        id: request.id,
        status: int32(GraphSyncResponseCode.gsrsRequestFailedUnknown),
        metadata: @[],
        extensions: initTable[string, seq[byte]](),
      )
      svc.recordInboundOutcome(
        GraphSyncResponseCode.gsrsRequestFailedUnknown, @[], @[]
      )
      return GraphSyncRequestResult(response: response, blocks: @[])

    let rootCid = cidRes.get()
    state = GraphSyncTraversalState(
      root: rootCid,
      selector: request.selector,
      traversal: traversalRes.get(),
      visited: initTable[string, bool](),
      enqueued: initTable[string, bool](),
      queue: initDeque[(Cid, int)](),
      completed: false,
    )
    state.queue.addLast((rootCid, 0))
    state.enqueued[$rootCid] = true

  let (blocks, metadata, status, message) = await svc.collectBlocks(state)
  var extensions = initTable[string, seq[byte]]()
  message.withValue(msg):
    extensions["graphsync/message"] = msg.toBytes()

  if (not state.completed) and state.queue.len > 0:
    await svc.stateLock.acquire()
    var lockReleased = false
    try:
      svc.inboundStates[key] = state
      lockReleased = releaseLockSafe(svc.stateLock)
      if not lockReleased:
        trace "failed to release graphsync state lock"
    finally:
      if not lockReleased:
        discard releaseLockSafe(svc.stateLock)

  let response = GraphSyncResponse(
    id: request.id,
    status: int32(status),
    metadata: metadata,
    extensions: extensions,
    )
  svc.recordInboundOutcome(status, blocks, metadata)
  GraphSyncRequestResult(response: response, blocks: blocks)

method init*(svc: GraphSyncService) =
  proc handle(conn: Connection, proto: string) {.async: (raises: [CancelledError]).} =
    trace "graphsync incoming stream", peerId = conn.peerId
    try:
      let payload = await conn.readLp(svc.config.maxMessageBytes)
      let msgOpt = decodeGraphSyncMessage(payload)
      if msgOpt.isNone():
        trace "graphsync failed decoding message", peerId = conn.peerId
        return
      let msg = msgOpt.get()
      if msg.requests.len == 0:
        trace "graphsync message without requests", peerId = conn.peerId
        return

      var responses: seq[GraphSyncResponse] = @[]
      var blocks: seq[GraphSyncBlock] = @[]

      for req in msg.requests:
        let result = await svc.processRequest(conn.peerId, req)
        responses.add(result.response)
        blocks.add(result.blocks)

      let responseMsg = GraphSyncMessage(
        completeRequestList: false,
        requests: @[],
        responses: responses,
        blocks: blocks,
      )
      if responseMsg.responses.len == 0 and responseMsg.blocks.len == 0:
        return
      let encoded = encode(responseMsg)
      await conn.writeLp(encoded)
    except CancelledError as exc:
      raise exc
    except LPStreamLimitError as exc:
      trace "graphsync read limit exceeded", peerId = conn.peerId, err = exc.msg
    except LPStreamError as exc:
      trace "graphsync stream error", peerId = conn.peerId, err = exc.msg
    except CatchableError as exc:
      trace "graphsync handler exception", peerId = conn.peerId, err = exc.msg
    finally:
      try:
        await conn.close()
      except CatchableError as exc:
        trace "graphsync close failed", peerId = conn.peerId, err = exc.msg

  svc.handler = handle
  svc.codec = GraphSyncCodec

proc new*(
    T: type GraphSyncService,
    provider: GraphSyncBlockProvider = nil,
    store: BitswapBlockStore = nil,
    config: GraphSyncConfig = GraphSyncConfig.init(),
): T {.public.} =
  if provider.isNil() and store.isNil():
    raise newException(Defect, "graphsync requires provider or store")
  validate(config)
  let svc = GraphSyncService(
    provider: provider,
    store: store,
    config: config,
    requestLock: newAsyncLock(),
    nextRequestId: 1,
    stats: GraphSyncStats(),
    stateLock: newAsyncLock(),
    inboundStates: initTable[string, GraphSyncTraversalState](),
  )
  svc.init()
  svc

proc nextRequestId(
    svc: GraphSyncService
): Future[int32] {.async: (raises: [CancelledError]).} =
  await svc.requestLock.acquire()
  var released = false
  try:
    let id = svc.nextRequestId
    inc svc.nextRequestId
    if svc.nextRequestId == 0:
      svc.nextRequestId = 1
    released = releaseLockSafe(svc.requestLock)
    return id
  finally:
    if not released:
      discard releaseLockSafe(svc.requestLock)

proc sendGraphSyncOnce(
    sw: Switch,
    peerId: PeerId,
    message: GraphSyncMessage,
    timeout: Duration,
    maxResponseBytes: int,
): Future[GraphSyncMessage] {.async: (raises: [CancelledError, GraphSyncError]).} =
  if maxResponseBytes <= 0:
    raise newGraphSyncError("maxResponseBytes must be positive", GraphSyncErrorKind.gsekConfig)

  trace "sending graphsync message", peerId

  let conn =
    try:
      await sw.dial(peerId, @[GraphSyncCodec])
    except CancelledError as exc:
      raise exc
    except CatchableError as exc:
      raise newGraphSyncError(
        "graphsync dial failed: " & exc.msg, GraphSyncErrorKind.gsekDialFailure, exc
      )

  defer:
    await conn.close()

  let encoded = encode(message)
  try:
    await conn.writeLp(encoded)
  except CancelledError as exc:
    raise exc
  except LPStreamError as exc:
    raise newGraphSyncError(
      "graphsync write failed: " & exc.msg, GraphSyncErrorKind.gsekWriteFailure, exc
    )

  let readFuture = conn.readLp(maxResponseBytes)
  let completed =
    try:
      await withTimeout(readFuture, timeout)
    except CancelledError as exc:
      raise exc

  if not completed:
    readFuture.cancelSoon()
    raise newGraphSyncError(
      "graphsync response timeout", GraphSyncErrorKind.gsekTimeout
    )

  let payload =
    try:
      await readFuture
    except CancelledError as exc:
      raise exc
    except LPStreamError as exc:
      raise newGraphSyncError(
        "graphsync read failed: " & exc.msg, GraphSyncErrorKind.gsekReadFailure, exc
      )

  let responseOpt = decodeGraphSyncMessage(payload)
  if responseOpt.isNone():
    raise newGraphSyncError(
      "graphsync invalid response", GraphSyncErrorKind.gsekDecodeFailure
    )

  trace "received graphsync response", peerId
  responseOpt.get()

proc sendGraphSync*(
    sw: Switch,
    peerId: PeerId,
    message: GraphSyncMessage,
    timeout: Duration,
    maxResponseBytes: int,
    maxAttempts: int,
    retryDelay: Duration,
): Future[GraphSyncMessage] {.async: (raises: [CancelledError, GraphSyncError]).} =
  if maxAttempts <= 0:
    raise newGraphSyncError("maxAttempts must be positive", GraphSyncErrorKind.gsekConfig)
  if retryDelay < 0.seconds:
    raise newGraphSyncError("retryDelay cannot be negative", GraphSyncErrorKind.gsekConfig)

  var attempt = 0
  var lastErr: ref GraphSyncError
  while attempt < maxAttempts:
    inc attempt
    try:
      return await sendGraphSyncOnce(
        sw, peerId, message, timeout = timeout, maxResponseBytes = maxResponseBytes
      )
    except CancelledError as exc:
      raise exc
    except GraphSyncError as err:
      lastErr = err
      if (not err.kind.isRetryable()) or attempt >= maxAttempts:
        raise err
      trace "graphsync attempt failed, retrying",
        attempt, maxAttempts, peerId, err = err.msg
      if retryDelay > 0.seconds:
        await sleepAsync(retryDelay)

  if lastErr.isNil:
    raise newGraphSyncError(
      "graphsync failed without error detail", GraphSyncErrorKind.gsekConfig
    )
  raise lastErr

proc request*(
    svc: GraphSyncService,
    sw: Switch,
    peerId: PeerId,
    root: Cid,
    selector: seq[byte] = @[],
    priority: int32 = 1,
    extensions: seq[(string, seq[byte])] = @[],
    timeout: Duration = 10.seconds,
): Future[GraphSyncFetchResult] {.async: (raises: [CancelledError, GraphSyncError]).} =
  if svc.isNil():
    raise newGraphSyncError("graphsync service is nil", GraphSyncErrorKind.gsekConfig)

  let id = await svc.nextRequestId()
  inc svc.stats.outboundRequests

  var extTable = initTable[string, seq[byte]]()
  for (key, value) in extensions:
    extTable[key] = value

  let request = GraphSyncRequest(
    id: id,
    root: root.data.buffer,
    selector: selector,
    extensions: extTable,
    priority: priority,
    cancel: false,
    update: false,
  )
  let message = GraphSyncMessage(
    completeRequestList: true, requests: @[request], responses: @[], blocks: @[]
  )

  let responseMsg =
    try:
      await sendGraphSync(
        sw,
        peerId,
        message,
        timeout = timeout,
        maxResponseBytes = svc.config.maxMessageBytes,
        maxAttempts = svc.config.maxAttempts,
        retryDelay = svc.config.retryDelay,
      )
    except GraphSyncError as exc:
      svc.recordOutboundFailure()
      raise exc

  var status = GraphSyncResponseCode.gsrsRequestFailedUnknown
  var responseExtensions = initTable[string, seq[byte]]()
  var responseMetadata: seq[(Cid, GraphSyncLinkAction)] = @[]
  var blocks: seq[(Cid, seq[byte])] = @[]
  try:
    for resp in responseMsg.responses:
      if resp.id == id:
        let raw = int(resp.status)
        let lowVal = ord(low(GraphSyncResponseCode))
        let highVal = ord(high(GraphSyncResponseCode))
        if raw >= lowVal and raw <= highVal:
          status = GraphSyncResponseCode(raw)
        else:
          status = GraphSyncResponseCode.gsrsRequestFailedUnknown
        responseExtensions = resp.extensions
        for entry in resp.metadata:
          let cidRes = Cid.init(entry.link)
          if cidRes.isErr():
            trace "graphsync response metadata cid decode failed",
              peerId, requestId = id, err = cidRes.error
            continue
          responseMetadata.add((cidRes.get(), entry.action))
        break

    var seen = initTable[string, bool]()
    for blk in responseMsg.blocks:
      let cidRes = decodeCidFromPrefix(blk.prefix, blk.data)
      if cidRes.isErr():
        raise newGraphSyncError(
          "graphsync block validation failed", GraphSyncErrorKind.gsekValidationFailure
        )
      let cid = cidRes.get()
      let key = $cid
      if not seen.hasKey(key):
        seen[key] = true
        blocks.add((cid, blk.data))
    let rootKey = $root
    if status in {
      GraphSyncResponseCode.gsrsRequestCompletedFull,
      GraphSyncResponseCode.gsrsRequestCompletedPartial,
    } and not seen.hasKey(rootKey):
      raise newGraphSyncError(
        "graphsync response missing root block", GraphSyncErrorKind.gsekValidationFailure
      )
  except GraphSyncError as exc:
    svc.recordOutboundFailure()
    raise exc

  svc.recordOutboundOutcome(status, blocks, responseMetadata)

  GraphSyncFetchResult(
    status: status, blocks: blocks, metadata: responseMetadata, extensions: responseExtensions
  )

proc stats*(svc: GraphSyncService): GraphSyncStats {.public.} =
  svc.stats

proc graphSyncStats*(s: Switch): Option[GraphSyncStats] {.public.} =
  if s.isNil:
    return none(GraphSyncStats)
  for service in s.services:
    if service of GraphSyncService:
      let gs = cast[GraphSyncService](service)
      return some(gs.stats)
  none(GraphSyncStats)

{.pop.}
