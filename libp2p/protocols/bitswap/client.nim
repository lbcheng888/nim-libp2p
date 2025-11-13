# Nim-LibP2P
# Copyright (c)
# Licensed under either of
#  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
#  * MIT license ([LICENSE-MIT](LICENSE-MIT))
# at your option.

{.push raises: [].}

import std/[options, tables, sets, sequtils]
import chronos, chronicles

import ../../switch
import ../../peerid
import ../../errors
import ../../stream/connection
import ../../utility
import ../../varint
import ../../multihash
import ../../multicodec
import ../../cid

import ./bitswap
import ./protobuf
import ./store
import ./ledger

const
  DefaultBitswapRequestTimeout = 10.seconds

logScope:
  topics = "libp2p bitswap"

type
  BitswapClientErrorKind* {.pure.} = enum
    bceDialFailure,
    bceTimeout,
    bceWriteFailure,
    bceReadFailure,
    bceDecodeFailure,
    bceInvalidResponse,
    bceClosed

  BitswapClientError* = object of LPError
    kind*: BitswapClientErrorKind

  BitswapClientConfig* = object
    maxMessageBytes*: int
    requestTimeout*: Duration
    maxAttempts*: int
    retryDelay*: Duration
    enableLedger*: bool
    maxMissingRetries*: int
    missingRetryDelay*: Duration

  BitswapSession* = ref object
    sw*: Switch
    peerId*: PeerId
    config*: BitswapClientConfig
    store*: BitswapBlockStore
    ledger*: BitswapLedger
    pending*: Table[Cid, Future[Option[seq[byte]]]]
    queue*: seq[Cid]
    queueSet*: HashSet[Cid]
    retryCounts*: Table[Cid, int]
    event*: AsyncEvent
    lock*: AsyncLock
    pump*: Future[void]
    stopped*: bool

proc init*(
    _: type BitswapClientConfig,
    maxMessageBytes: int = DefaultMaxMessageBytes,
    requestTimeout: Duration = DefaultBitswapRequestTimeout,
    maxAttempts: int = 1,
    retryDelay: Duration = 200.milliseconds,
    enableLedger: bool = true,
    maxMissingRetries: int = 2,
    missingRetryDelay: Duration = 500.milliseconds,
): BitswapClientConfig =
  BitswapClientConfig(
    maxMessageBytes: maxMessageBytes,
    requestTimeout: requestTimeout,
    maxAttempts: maxAttempts,
    retryDelay: retryDelay,
    enableLedger: enableLedger,
    maxMissingRetries: maxMissingRetries,
    missingRetryDelay: missingRetryDelay,
  )

proc newError(
    msg: string,
    kind: BitswapClientErrorKind,
    parent: ref Exception = nil,
): ref BitswapClientError =
  (ref BitswapClientError)(msg: msg, kind: kind, parent: parent)

proc validate(config: BitswapClientConfig) =
  if config.maxMessageBytes <= 0:
    raise newException(Defect, "bitswap client maxMessageBytes must be positive")
  if config.requestTimeout <= 0.seconds:
    raise newException(Defect, "bitswap client requestTimeout must be positive")
  if config.maxAttempts <= 0:
    raise newException(Defect, "bitswap client maxAttempts must be positive")
  if config.retryDelay < 0.seconds:
    raise newException(Defect, "bitswap client retryDelay cannot be negative")
  if config.maxMissingRetries < 0:
    raise newException(Defect, "bitswap client maxMissingRetries cannot be negative")
  if config.missingRetryDelay < 0.seconds:
    raise newException(Defect, "bitswap client missingRetryDelay cannot be negative")

proc cidFromPayload(prefix: seq[byte], data: seq[byte]): Option[Cid] =
  if prefix.len == 0:
    return none(Cid)

  var offset = 0
  var consumed = 0
  var first: uint64
  if LP.getUVarint(prefix, consumed, first).isErr:
    return none(Cid)
  offset = consumed

  var cidVer = CIDv1
  var codecVal = uint64(0)
  var hashCode = first

  if first == 1'u64 and offset < prefix.len:
    var codecRaw: uint64
    if LP.getUVarint(prefix.toOpenArray(offset, prefix.high), consumed, codecRaw).isErr:
      return none(Cid)
    offset += consumed
    codecVal = codecRaw
    if LP.getUVarint(prefix.toOpenArray(offset, prefix.high), consumed, hashCode).isErr:
      return none(Cid)
    offset += consumed
  else:
    cidVer = CIDv0
    codecVal = uint64(multiCodec("dag-pb"))

  var digestLen: uint64
  if LP.getUVarint(prefix.toOpenArray(offset, prefix.high), consumed, digestLen).isErr:
    return none(Cid)

  let hashCodec = MultiCodec(int(hashCode))
  let digestRes = MultiHash.digest(int(hashCodec), data)
  if digestRes.isErr:
    return none(Cid)
  let digest = digestRes.get()
  if digest.data.buffer.len - digest.dpos != int(digestLen):
    return none(Cid)

  let contentCodec = MultiCodec(int(codecVal))
  let cidRes = Cid.init(if cidVer == CIDv1: CIDv1 else: CIDv0, contentCodec, digest)
  if cidRes.isErr:
    return none(Cid)
  some(cidRes.get())

proc blockMatchesCid(cid: Cid, data: seq[byte]): bool =
  let mhRes = cid.mhash()
  if mhRes.isErr:
    return false
  let expected = mhRes.get()
  let digestRes = MultiHash.digest(int(expected.mcodec), data)
  if digestRes.isErr:
    return false
  let computed = digestRes.get()
  expected.data.buffer == computed.data.buffer

proc takePending(
    session: BitswapSession, cids: openArray[Cid]
): seq[(Cid, Future[Option[seq[byte]]])] {.async: (raises: [CancelledError]).} =
  await session.lock.acquire()
  defer: session.lock.release()
  for cid in cids:
    session.pending.withValue(cid, fut):
      result.add((cid, fut))
      session.pending.del(cid)
      if session.retryCounts.hasKey(cid):
        session.retryCounts.del(cid)

proc failWants(
    session: BitswapSession, wants: seq[Cid], err: ref BitswapClientError
) {.async: (raises: [CancelledError]).} =
  let futures = await session.takePending(wants)
  for (_, fut) in futures:
    if not fut.finished():
      fut.fail(err)

proc scheduleMissingRetry(
    session: BitswapSession, missing: HashSet[Cid]
) {.async: (raises: [CancelledError]).} =
  if missing.len == 0:
    return

  var toRequeue: seq[Cid] = @[]
  var toFail: seq[Cid] = @[]

  await session.lock.acquire()
  if session.stopped:
    toFail = toSeq(missing)
  else:
    for cid in missing:
      let attempts = session.retryCounts.getOrDefault(cid)
      if attempts >= session.config.maxMissingRetries:
        toFail.add(cid)
        session.retryCounts.del(cid)
      else:
        session.retryCounts[cid] = attempts + 1
        toRequeue.add(cid)
  session.lock.release()

  if toFail.len > 0:
    let err = newError(
      "bitswap response missing payload after retries",
      BitswapClientErrorKind.bceInvalidResponse,
    )
    await session.failWants(toFail, err)

  if toRequeue.len == 0:
    return

  let delay = session.config.missingRetryDelay
  if delay > 0.seconds:
    await sleepAsync(delay)

  await session.lock.acquire()
  var trigger = false
  var lateFail: seq[Cid] = @[]
  if session.stopped:
    lateFail = toRequeue
    toRequeue = @[]
  else:
    for cid in toRequeue:
      if cid notin session.queueSet:
        session.queue.add(cid)
        session.queueSet.incl(cid)
        trigger = true
  session.lock.release()

  if trigger:
    trace "bitswap scheduling retry for missing blocks",
      peerId = session.peerId, retryCount = toRequeue.len
    session.event.fire()

  if lateFail.len > 0:
    let err = newError(
      "bitswap retry aborted: session stopped",
      BitswapClientErrorKind.bceClosed,
    )
    await session.failWants(lateFail, err)

proc completeWithData(
    session: BitswapSession,
    results: seq[(Cid, seq[byte])],
) {.async: (raises: [CancelledError, BitswapClientError]).} =
  let futures = await session.takePending(results.mapIt(it[0]))
  var dataTable = initTable[Cid, seq[byte]]()
  for (cid, data) in results:
    dataTable[cid] = data
  for (cid, fut) in futures:
    if fut.finished():
      continue
    let blk = dataTable.getOrDefault(cid, @[])
    if blk.len == 0:
      fut.fail(
        newError(
          "missing bitswap block data for " & $cid, BitswapClientErrorKind.bceInvalidResponse
        )
      )
      continue
    var stored = false
    if not session.store.isNil():
      try:
        await session.store.putBlock(cid, blk)
        stored = true
      except CancelledError as exc:
        raise exc
      except CatchableError as exc:
        trace "bitswap store put failed", cid = $cid, err = exc.msg
    if stored and not session.sw.isNil():
      try:
        await session.sw.publishDelegatedProvider(
          cid, schema = "bitswap", protocols = @[BitswapCodec]
        )
      except CancelledError as exc:
        raise exc
      except CatchableError as exc:
        trace "bitswap delegated provider publish failed", cid = $cid, err = exc.msg
    fut.complete(some(blk))

proc completeDontHave(
    session: BitswapSession, cids: HashSet[Cid]
) {.async: (raises: [CancelledError]).} =
  let futures = await session.takePending(toSeq(cids))
  for (_, fut) in futures:
    if not fut.finished():
      fut.complete(none(seq[byte]))

proc drainQueue(session: BitswapSession): Future[seq[Cid]] {.async.} =
  await session.lock.acquire()
  let wants = session.queue
  session.queue = @[]
  session.queueSet.clear()
  session.lock.release()
  wants

proc mapBlocks(
    session: BitswapSession,
    wants: seq[Cid],
    message: BitswapMessage,
): tuple[found: seq[(Cid, seq[byte])], dontHave: HashSet[Cid], missing: HashSet[Cid]] =
  var dontHave = initHashSet[Cid]()
  var expected = initHashSet[Cid]()
  let wantedSet = wants.toHashSet()

  for presence in message.blockPresences:
    if presence.cid notin wantedSet:
      continue
    case presence.presenceType
    of BlockPresenceType.bpHave:
      expected.incl(presence.cid)
    of BlockPresenceType.bpDontHave:
      dontHave.incl(presence.cid)

  var found = newSeqOfCap[(Cid, seq[byte])](message.payloads.len)

  for payload in message.payloads:
    let cidOpt = cidFromPayload(payload.prefix, payload.data)
    cidOpt.withValue(cid):
      if cid notin wantedSet:
        continue
      found.add((cid, payload.data))
      expected.excl(cid)

  var blockIdx = 0
  for presence in message.blockPresences:
    if presence.presenceType != BlockPresenceType.bpHave:
      continue
    if presence.cid notin wantedSet:
      continue
    if found.anyIt(it[0] == presence.cid):
      continue
    if blockIdx < message.blocks.len:
      let blk = message.blocks[blockIdx]
      if blockMatchesCid(presence.cid, blk):
        found.add((presence.cid, blk))
        expected.excl(presence.cid)
      inc blockIdx

  if found.len == 0 and message.blocks.len > 0 and message.payloads.len == 0:
    let limit = min(message.blocks.len, wants.len)
    for i in 0 ..< limit:
      let cid = wants[i]
      if cid in dontHave or found.anyIt(it[0] == cid):
        continue
      let blk = message.blocks[i]
      if blockMatchesCid(cid, blk):
        found.add((cid, blk))
        expected.excl(cid)

  (found, dontHave, expected)

proc sendWant(
    session: BitswapSession, wants: seq[Cid]
) {.async: (raises: [CancelledError, BitswapClientError]).} =
  let attempts = session.config.maxAttempts
  var attempt = 0
  var lastErr: ref BitswapClientError
  while attempt < attempts:
    inc attempt
    var conn: Connection
    try:
      conn = await session.sw.dial(session.peerId, @[BitswapCodec])
    except CancelledError as exc:
      raise exc
    except CatchableError as exc:
      lastErr = newError(
        "bitswap dial failed: " & exc.msg,
        BitswapClientErrorKind.bceDialFailure,
        exc,
      )
      if attempt >= attempts:
        await session.failWants(wants, lastErr)
        return
      if session.config.retryDelay > 0.seconds:
        await sleepAsync(session.config.retryDelay)
      continue

    defer:
      if not conn.isNil():
        await conn.close()

    var entries: seq[WantEntry] = @[]
    for (idx, cid) in wants.pairs():
      entries.add(
        WantEntry(
          cid: cid,
          priority: int32(high(int32) - idx),
          cancel: false,
          wantType: WantType.wtBlock,
          sendDontHave: true,
        )
      )
    let wantlist = Wantlist(entries: entries, full: false)
    let message = BitswapMessage(
      wantlist: some(wantlist),
      blocks: @[],
      payloads: @[],
      blockPresences: @[],
      pendingBytes: none(int32),
    )
    let payload = encode(message)

    try:
      await conn.writeLp(payload)
      if not session.ledger.isNil and wants.len > 0 and session.peerId.len > 0:
        await session.ledger.recordWants(
          session.peerId, wants.len, BitswapLedgerDirection.bldSent
        )
    except LPStreamError as exc:
      lastErr = newError(
        "bitswap write failed: " & exc.msg,
        BitswapClientErrorKind.bceWriteFailure,
        exc,
      )
      if attempt >= attempts:
        await session.failWants(wants, lastErr)
        return
      if session.config.retryDelay > 0.seconds:
        await sleepAsync(session.config.retryDelay)
      continue

    let readFuture = conn.readLp(session.config.maxMessageBytes)
    let completed =
      try:
        await withTimeout(readFuture, session.config.requestTimeout)
      except CancelledError as exc:
        raise exc

    if not completed:
      lastErr = newError(
        "bitswap request timeout",
        BitswapClientErrorKind.bceTimeout,
      )
      if attempt >= attempts:
        await session.failWants(wants, lastErr)
        return
      if session.config.retryDelay > 0.seconds:
        await sleepAsync(session.config.retryDelay)
      continue

    let response =
      try:
        await readFuture
      except CancelledError as exc:
        raise exc
      except LPStreamError as exc:
        lastErr = newError(
          "bitswap read failed: " & exc.msg,
          BitswapClientErrorKind.bceReadFailure,
          exc,
        )
        if attempt >= attempts:
          await session.failWants(wants, lastErr)
          return
        if session.config.retryDelay > 0.seconds:
          await sleepAsync(session.config.retryDelay)
        continue

    let messageOpt = decodeBitswapMessage(response)
    if messageOpt.isNone():
      lastErr = newError(
        "bitswap response decode failed",
        BitswapClientErrorKind.bceDecodeFailure,
      )
      if attempt >= attempts:
        await session.failWants(wants, lastErr)
        return
      if session.config.retryDelay > 0.seconds:
        await sleepAsync(session.config.retryDelay)
      continue

    let (found, dontHave, missing) = session.mapBlocks(wants, messageOpt.get())
    var delta = BitswapLedgerDelta()
    if found.len > 0:
      delta.blocks = uint64(found.len)
      delta.haveCount += uint64(found.len)
      for (_, blk) in found:
        delta.bytes += uint64(blk.len)
    if dontHave.len > 0:
      delta.dontHaveCount += uint64(dontHave.len)
    if missing.len > 0:
      for cid in missing:
        trace "bitswap response missing block data", cid
    if found.len > 0:
      await session.completeWithData(found)
    if dontHave.len > 0:
      await session.completeDontHave(dontHave)
    if not session.ledger.isNil and session.peerId.len > 0:
      if delta.bytes > 0 or delta.haveCount > 0 or delta.dontHaveCount > 0:
        await session.ledger.recordDelta(
          session.peerId, delta, BitswapLedgerDirection.bldReceived
        )
    if missing.len > 0:
      await session.scheduleMissingRetry(missing)
    return

proc runPump(session: BitswapSession) {.async: (raises: [CancelledError]).} =
  while true:
    if session.stopped:
      break
    session.event.clear()
    await session.event.wait()
    if session.stopped:
      break
    let wants = await session.drainQueue()
    if wants.len == 0:
      continue
    try:
      await session.sendWant(wants)
    except CancelledError as exc:
      raise exc
    except BitswapClientError as err:
      trace "bitswap send failed", peerId = session.peerId, err = err.msg
      await session.failWants(wants, err)
    except CatchableError as exc:
      trace "unexpected bitswap error", peerId = session.peerId, err = exc.msg
      let errorRef = newError(
        "bitswap unexpected error: " & exc.msg,
        BitswapClientErrorKind.bceInvalidResponse,
        exc,
      )
      await session.failWants(wants, errorRef)

proc new*(
    _: type BitswapSession,
    sw: Switch,
    peerId: PeerId,
    store: BitswapBlockStore = nil,
    config: BitswapClientConfig = BitswapClientConfig.init(),
    ledger: BitswapLedger = nil,
): BitswapSession =
  if sw.isNil():
    raise newException(Defect, "BitswapSession requires a valid switch")
  validate(config)
  let ledgerInst =
    if not config.enableLedger:
      nil
    elif not ledger.isNil:
      ledger
    else:
      BitswapLedger.new()
  let session = BitswapSession(
    sw: sw,
    peerId: peerId,
    config: config,
    store: store,
    ledger: ledgerInst,
    pending: initTable[Cid, Future[Option[seq[byte]]]](),
    queue: @[],
    queueSet: initHashSet[Cid](),
    retryCounts: initTable[Cid, int](),
    event: newAsyncEvent(),
    lock: newAsyncLock(),
    pump: nil,
    stopped: false,
  )
  session.pump = asyncSpawn session.runPump()
  session

proc requestBlock*(
    session: BitswapSession, cid: Cid
): Future[Option[seq[byte]]] {.async: (raises: [CancelledError, BitswapClientError]).} =
  if session.isNil() or session.stopped:
    raise newError("bitswap session closed", BitswapClientErrorKind.bceClosed)
  if not session.store.isNil():
    try:
      let cached = await session.store.getBlock(cid)
      if cached.isSome():
        return cached
    except CancelledError as exc:
      raise exc
    except CatchableError as exc:
      trace "bitswap store get failed", cid = $cid, err = exc.msg
  var fut: Future[Option[seq[byte]]]
  var trigger = false
  await session.lock.acquire()
  if session.stopped:
    session.lock.release()
    raise newError("bitswap session closed", BitswapClientErrorKind.bceClosed)
  if session.pending.hasKey(cid):
    fut = session.pending[cid]
  else:
    fut = newFuture[Option[seq[byte]]]("bitswap.session.request")
    session.pending[cid] = fut
    if cid notin session.queueSet:
      session.queue.add(cid)
      session.queueSet.incl(cid)
      trigger = true
  session.lock.release()
  if trigger:
    session.event.fire()
  result = await fut

proc requestBlocks*(
    session: BitswapSession, cids: openArray[Cid]
): Future[Table[Cid, Option[seq[byte]]]] {.
    async: (raises: [CancelledError, BitswapClientError]).
  .} =
  var tasks: seq[Future[Option[seq[byte]]]] = @[]
  for cid in cids:
    tasks.add(session.requestBlock(cid))
  let results = await all(tasks)
  var table = initTable[Cid, Option[seq[byte]]](cids.len)
  for i, cid in cids.pairs():
    table[cid] = results[i]
  table

proc close*(session: BitswapSession) {.async: (raises: [CancelledError]).} =
  if session.isNil() or session.stopped:
    return
  session.stopped = true
  session.event.fire()
  if not session.pump.isNil():
    try:
      await session.pump.cancelAndWait()
    except CatchableError:
      discard
  await session.lock.acquire()
  let futures = toSeq(session.pending.values())
  session.pending.clear()
  session.lock.release()
  let err = newError("bitswap session closed", BitswapClientErrorKind.bceClosed)
  for fut in futures:
    if not fut.finished():
      fut.fail(err)

{.pop.}
