## 简化版 K 线聚合与持久化模块。
##
## 目标：
## 1. 订阅 dex.trades 主题后的逐笔成交流，聚合为多档 K 线窗口；
## 2. 利用 nim-pebble WAL 记录最新 bucket，重启可恢复；
## 3. 对外暴露 snapshot/metrics，供 dex_node 或 REST 层查询。

import std/[json, options, os, sequtils, strformat, tables, times]
import chronos
import stew/byteutils

import pebble/wal/[writer as pebble_wal_writer, reader as pebble_wal_reader, types as pebble_wal_types]

import ../types

const
  DefaultKlineWal* = "kline.wal"
  KlineWalVersion = 1

type
  TimeBucketKey = string

  KlineWalEventKind = enum
    kweTrade
    kweBucket

  KlineWalEvent = object
    kind: KlineWalEventKind
    payload: JsonNode
    timestamp: int64

  KlineStoreStats* = object
    buckets*: int
    published*: int
    droppedTrades*: int
    reopenWindows*: int

  KlineStore* = ref object
    wal: pebble_wal_writer.WalWriter
    walPath: string
    lock: AsyncLock
    buckets: Table[TimeBucketKey, KlineBucket]
    latestSeq: int64
    stats*: KlineStoreStats

proc nowMs(): int64 =
  int64(epochTime() * 1000.0)

proc ensureDir(path: string) =
  let (dir, _) = splitPath(path)
  if dir.len > 0 and not dirExists(dir):
    createDir(dir)

proc walEventToJson(event: KlineWalEvent): JsonNode =
  %*{
    "version": KlineWalVersion,
    "timestamp": event.timestamp,
    "kind":
      case event.kind
      of kweTrade:
        "trade"
      of kweBucket:
        "bucket",
    "payload": event.payload,
  }

proc walKindFromStr(value: string): Option[KlineWalEventKind] =
  case value
  of "trade":
    some(kweTrade)
  of "bucket":
    some(kweBucket)
  else:
    none(KlineWalEventKind)

proc appendWal(store: KlineStore; kind: KlineWalEventKind; payload: JsonNode) {.raises: [CatchableError].} =
  if store.isNil() or store.wal.isNil():
    return
  let node = walEventToJson(KlineWalEvent(kind: kind, payload: payload, timestamp: nowMs()))
  try:
    discard store.wal.append(($node).toBytes())
  except CatchableError as exc:
    raise exc
  except Exception as exc:
    raise newException(CatchableError, exc.msg)

proc bucketToJson(bucket: KlineBucket): JsonNode =
  %*{
    "asset": bucket.asset,
    "scale": bucket.scale.int,
    "windowStartMs": bucket.windowStartMs,
    "open": bucket.open,
    "high": bucket.high,
    "low": bucket.low,
    "close": bucket.close,
    "volume": bucket.volume,
    "trades": bucket.trades,
    "closed": bucket.closed,
    "publishSeq": bucket.publishSeq,
  }

proc tradeToJson(trade: TradeEvent): JsonNode =
  %*{
    "matchId": trade.matchId,
    "orderId": trade.orderId,
    "takerSide": trade.takerSide,
    "price": trade.price,
    "amount": trade.amount,
    "asset": trade.asset,
    "matcherPeer": trade.matcherPeer,
    "makerPeer": trade.makerPeer,
    "createdAt": trade.createdAt,
    "sequence": trade.sequence,
  }

proc applyTrade(store: KlineStore; trade: TradeEvent; scales: seq[Timescale]) =
  if trade.asset.len == 0 or trade.price <= 0 or trade.amount <= 0:
    inc store.stats.droppedTrades
    return
  if trade.sequence > store.latestSeq:
    store.latestSeq = trade.sequence
  for scale in scales:
    let windowStart = alignWindow(trade.createdAt, scale)
    let key = bucketKey(trade.asset, scale, windowStart)
    if not store.buckets.hasKey(key):
      store.buckets[key] = initBucket(trade.asset, scale, trade.createdAt, trade)
    else:
      var bucket = store.buckets[key]
      if trade.createdAt < bucket.windowStartMs or trade.createdAt >= bucket.windowStartMs + scale.int.int64 * 1000:
        ## 延迟成交落入旧窗口，重新对齐。
        inc store.stats.reopenWindows
        let newKey = bucketKey(trade.asset, scale, alignWindow(trade.createdAt, scale))
        var delayed = initBucket(trade.asset, scale, trade.createdAt, trade)
        store.buckets[newKey] = delayed
        continue
      bucket.updateBucket(trade)
      store.buckets[key] = bucket
    store.stats.published = max(store.stats.published, store.buckets[key].publishSeq)
  store.stats.buckets = store.buckets.len

proc pruneClosed(store: KlineStore; horizonMs: int64) =
  let cutoff = nowMs() - horizonMs
  var toRemove: seq[TimeBucketKey] = @[]
  for key, bucket in store.buckets.pairs:
    if bucket.windowStartMs + (bucket.scale.int.int64 * 1000) < cutoff:
      toRemove.add(key)
  for key in toRemove:
    store.buckets.del(key)
  if toRemove.len > 0:
    store.stats.buckets = store.buckets.len

proc replayWal(store: KlineStore) {.gcsafe, raises: [CatchableError].}

proc initKlineStore*(dataDir: string, walFile = DefaultKlineWal): KlineStore {.gcsafe, raises: [CatchableError].} =
  var dir = dataDir
  if dir.len == 0:
    dir = "dex_state"
  ensureDir(dir / "kline")
  let walPath = dir / "kline" / walFile
  let wal =
    try:
      pebble_wal_writer.newWalWriter(
        path = walPath,
        format = pebble_wal_types.wireRecyclable,
        maxSegmentBytes = 4 * 1024 * 1024,
        autoSyncBytes = 64 * 1024,
        maxPendingAutoSyncs = 2,
      )
    except CatchableError as exc:
      raise exc
    except Exception as exc:
      raise newException(CatchableError, exc.msg)
  new(result)
  result.wal = wal
  result.walPath = walPath
  result.lock = newAsyncLock()
  result.buckets = initTable[TimeBucketKey, KlineBucket]()
  result.stats = KlineStoreStats()
  try:
    result.replayWal()
  except CatchableError as exc:
    raise exc
  except Exception as exc:
    raise newException(CatchableError, exc.msg)

proc replayWal(store: KlineStore) {.gcsafe, raises: [CatchableError].} =
  if store.isNil() or store.walPath.len == 0 or not fileExists(store.walPath):
    return
  var reader = pebble_wal_reader.newWalReader(store.walPath)
  defer:
    reader.close()
  while true:
    let entry =
      try:
        reader.readNext()
      except CatchableError as exc:
        raise exc
      except Exception as exc:
        raise newException(CatchableError, exc.msg)
    if entry.isNone():
      break
    let raw = entry.get().payload
    try:
      let node = parseJson(string.fromBytes(raw))
      if node.kind != JObject or not node.hasKey("kind"):
        continue
      let kindOpt = walKindFromStr(node["kind"].getStr())
      if kindOpt.isNone():
        continue
      let payload = if node.hasKey("payload"): node["payload"] else: newJNull()
      case kindOpt.get()
      of kweBucket:
        let bucketOpt = decodeBucket(($payload).toBytes())
        if bucketOpt.isSome():
          let bucket = bucketOpt.get()
          store.buckets[bucketKey(bucket.asset, bucket.scale, bucket.windowStartMs)] = bucket
      of kweTrade:
        let tradeOpt = decodeTrade(($payload).toBytes())
        if tradeOpt.isSome():
          store.latestSeq = max(store.latestSeq, tradeOpt.get().sequence)
    except CatchableError:
      continue
  store.stats.buckets = store.buckets.len

proc recordTrade*(store: KlineStore; trade: TradeEvent; scales: seq[Timescale]) {.async: (raises: [CatchableError]).} =
  if store.isNil():
    return
  await store.lock.acquire()
  try:
    store.appendWal(kweTrade, tradeToJson(trade))
    store.applyTrade(trade, scales)
  finally:
    store.lock.release()

proc closeBucket*(store: KlineStore; bucket: KlineBucket) {.async: (raises: [CatchableError]).} =
  if store.isNil():
    return
  await store.lock.acquire()
  try:
    var copy = bucket
    copy.closed = true
    let key = bucketKey(copy.asset, copy.scale, copy.windowStartMs)
    store.buckets[key] = copy
    store.appendWal(kweBucket, bucketToJson(copy))
  finally:
    store.lock.release()

proc snapshot*(store: KlineStore): Future[seq[KlineBucket]] {.async.} =
  if store.isNil():
    return @[]
  await store.lock.acquire()
  try:
    toSeq(store.buckets.values)
  finally:
    store.lock.release()

proc statsSummary*(store: KlineStore): string =
  if store.isNil():
    return "disabled"
  &"buckets={store.stats.buckets} latestSeq={store.latestSeq} droppedTrades={store.stats.droppedTrades} reopenWindows={store.stats.reopenWindows}"

proc prune*(store: KlineStore; horizonMs: int64) {.async.} =
  if store.isNil():
    return
  await store.lock.acquire()
  try:
    store.pruneClosed(horizonMs)
  finally:
    store.lock.release()

proc collectReady*(store: KlineStore; nowMs: int64): Future[seq[KlineBucket]] {.async.} =
  if store.isNil():
    return @[]
  await store.lock.acquire()
  try:
    var ready: seq[KlineBucket] = @[]
    for key, bucket in store.buckets.mpairs:
      if bucket.closed:
        continue
      let windowEnd = bucket.windowStartMs + bucket.scale.int.int64 * 1000
      if nowMs >= windowEnd:
        bucket.closed = true
        store.appendWal(kweBucket, bucketToJson(bucket))
        ready.add(bucket)
    ready
  finally:
    store.lock.release()
