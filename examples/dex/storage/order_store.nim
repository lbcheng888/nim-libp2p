## Lightweight order persistence built on nim-pebble WAL primitives.

import std/[algorithm, json, options, os, sequtils, strformat, strutils, tables, times]
import chronos
import stew/byteutils

import pebble/runtime/resource_manager as pebble_rm
import pebble/wal/[reader as pebble_wal_reader, writer as pebble_wal_writer, types as pebble_wal_types]

import ../types

const
  DefaultWalFile = "orders.wal"
  WalVersion = 1

type
  OrderStatus* = enum
    osPending, osMatched

  AssetExposure* = object
    buyVolume*: float
    sellVolume*: float
    matchedVolume*: float

  OrderRecord = object
    order: OrderMessage
    status: OrderStatus
    createdMs: int64
    updatedMs: int64
    expiresMs: int64
    lastMatch: Option[MatchMessage]

  ExposureTuple = tuple[
    asset: string,
    buyVolume: float,
    sellVolume: float,
    matchedVolume: float
  ]

  OrderStoreStats* = object
    totalOrders*: int
    pendingOrders*: int
    matchedOrders*: int
    exposure*: seq[ExposureTuple]
    storeExposure*: Table[string, AssetExposure]
    settlementEvents*: int

  WalEventKind = enum
    wekOrder
    wekMatch

  WalEvent = object
    kind: WalEventKind
    payload: JsonNode
    timestampMs: int64

  OrderStore* = ref object
    wal: pebble_wal_writer.WalWriter
    walPath: string
    orders: Table[string, OrderRecord]
    matches: Table[string, MatchMessage]
    exposure: Table[string, AssetExposure]
    lock: AsyncLock
    resources: pebble_rm.ResourceManager
    settlements: seq[SettlementRecord]

proc nowMs(): int64 =
  int64(epochTime() * 1000.0)

proc ensureDir(path: string) =
  if path.len == 0:
    return
  if not dirExists(path):
    createDir(path)

proc orderToJson(order: OrderMessage): JsonNode =
  %*{
    "id": order.id,
    "traderPeer": order.traderPeer,
    "baseAsset": encodeAssetDef(order.baseAsset),
    "quoteAsset": encodeAssetDef(order.quoteAsset),
    "side": order.side,
    "price": order.price,
    "amount": order.amount,
    "ttlMs": order.ttlMs,
  }

proc matchToJson(match: MatchMessage): JsonNode =
  %*{
    "orderId": match.orderId,
    "matcherPeer": match.matcherPeer,
    "price": match.price,
    "amount": match.amount,
    "note": match.note,
  }

proc jsonToOrder(node: JsonNode): Option[OrderMessage] =
  if node.kind != JObject or not node.hasKey("id"):
    return none(OrderMessage)
  some(
    OrderMessage(
      id: node["id"].getStr(),
      traderPeer: node["traderPeer"].getStr(),
      baseAsset: decodeAssetDef(node["baseAsset"]),
      quoteAsset: decodeAssetDef(node["quoteAsset"]),
      side: node["side"].getStr(),
      price: node["price"].getFloat(),
      amount: node["amount"].getFloat(),
      ttlMs: node["ttlMs"].getInt(),
    )
  )

proc jsonToMatch(node: JsonNode): Option[MatchMessage] =
  if node.kind != JObject or not node.hasKey("orderId"):
    return none(MatchMessage)
  some(
    MatchMessage(
      orderId: node["orderId"].getStr(),
      matcherPeer: node["matcherPeer"].getStr(),
      price: node["price"].getFloat(),
      amount: node["amount"].getFloat(),
      note: if node.hasKey("note"): node["note"].getStr() else: "",
    )
  )

proc walEventToJson(event: WalEvent): JsonNode =
  %*{
    "version": WalVersion,
    "timestampMs": event.timestampMs,
    "kind":
      case event.kind
      of wekOrder:
        "order"
      of wekMatch:
        "match",
    "payload": event.payload,
  }

proc walKindFromString(value: string): Option[WalEventKind] =
  case value
  of "order":
    some(wekOrder)
  of "match":
    some(wekMatch)
  else:
    none(WalEventKind)

proc appendWal(store: OrderStore; event: WalEvent) =
  if store.isNil() or store.wal.isNil():
    return
  let node = walEventToJson(event)
  discard store.wal.append(($node).toBytes())

proc updateExposure(store: OrderStore; order: OrderMessage; delta: float; matchedDelta: float = 0.0) =
    let assetKey = order.baseAsset.symbol # Use symbol as key for simplicity
    var exposure = store.exposure.getOrDefault(assetKey, AssetExposure())
    if order.side == "buy":
      exposure.buyVolume = max(0.0, exposure.buyVolume + delta)
    else:
      exposure.sellVolume = max(0.0, exposure.sellVolume + delta)
    if matchedDelta != 0.0:
      exposure.matchedVolume = max(0.0, exposure.matchedVolume + matchedDelta)
    store.exposure[assetKey] = exposure

proc applyOrder(store: OrderStore; order: OrderMessage; tsMs: int64; persist = true) =
    if store.orders.hasKey(order.id):
      return
    let expires = tsMs + int64(order.ttlMs)
    let record = OrderRecord(
      order: order,
      status: osPending,
      createdMs: tsMs,
      updatedMs: tsMs,
      expiresMs: expires,
      lastMatch: none(MatchMessage),
    )
    store.orders[order.id] = record
    let delta = order.amount
    store.updateExposure(order, delta)
    if persist:
      let payload = orderToJson(order)
      store.appendWal(WalEvent(kind: wekOrder, payload: payload, timestampMs: tsMs))

proc applyMatch(store: OrderStore; match: MatchMessage; tsMs: int64; persist = true) =
  let orderOpt = store.orders.getOrDefault(match.orderId, OrderRecord())
  if not store.orders.hasKey(match.orderId):
    # 缺失的订单仍然记录匹配信息，方便调试。
    store.matches[match.orderId] = match
    if persist:
      store.appendWal(WalEvent(kind: wekMatch, payload: matchToJson(match), timestampMs: tsMs))
    return
  var record = orderOpt
  if record.status == osMatched:
    return
  record.status = osMatched
  record.updatedMs = tsMs
  record.lastMatch = some(match)
  store.orders[match.orderId] = record
  store.matches[match.orderId] = match
  store.updateExposure(record.order, -match.amount, match.amount)
  if persist:
    store.appendWal(WalEvent(kind: wekMatch, payload: matchToJson(match), timestampMs: tsMs))

proc replayWal(store: OrderStore) =
  if store.walPath.len == 0 or not fileExists(store.walPath):
    return
  var reader = pebble_wal_reader.newWalReader(store.walPath)
  defer:
    reader.close()
  while true:
    let entry = reader.readNext()
    if entry.isNone():
      break
    let payload = entry.get().payload
    try:
      let node = parseJson(string.fromBytes(payload))
      if node.kind != JObject or not node.hasKey("kind"):
        continue
      let kindOpt = walKindFromString(node["kind"].getStr())
      if kindOpt.isNone():
        continue
      let timestamp = if node.hasKey("timestampMs"): node["timestampMs"].getInt().int64 else: nowMs()
      case kindOpt.get()
      of wekOrder:
        if node.hasKey("payload"):
          let orderOpt = jsonToOrder(node["payload"])
          if orderOpt.isSome():
            store.applyOrder(orderOpt.get(), timestamp, persist = false)
      of wekMatch:
        if node.hasKey("payload"):
          let matchOpt = jsonToMatch(node["payload"])
          if matchOpt.isSome():
            store.applyMatch(matchOpt.get(), timestamp, persist = false)
    except CatchableError:
      continue

proc initOrderStore*(dataDir: string): OrderStore =
  var dir = dataDir
  if dir.len == 0:
    dir = "dex_state"
  ensureDir(dir)
  let walPath = dir / DefaultWalFile
  let wal = pebble_wal_writer.newWalWriter(
    path = walPath,
    format = pebble_wal_types.wireRecyclable,
    maxSegmentBytes = 4 * 1024 * 1024,
    autoSyncBytes = 64 * 1024,
    maxPendingAutoSyncs = 2,
  )
  new(result)
  result.wal = wal
  result.walPath = walPath
  result.orders = initTable[string, OrderRecord]()
  result.matches = initTable[string, MatchMessage]()
  result.exposure = initTable[string, AssetExposure]()
  result.lock = newAsyncLock()
  result.resources = pebble_rm.newResourceManager()
  result.resources.setQuota(pebble_rm.resMemoryBytes, 2 * 1024 * 1024, 4 * 1024 * 1024)
  result.replayWal()
  result.settlements = @[]

proc close*(store: OrderStore) =
  if store.isNil():
    return
  if not store.wal.isNil():
    store.wal.close()
    store.wal = nil

proc recordInboundOrder*(store: OrderStore; order: OrderMessage): Future[void] {.async: (raises: [CatchableError]).} =
  if store.isNil():
    return
  await store.lock.acquire()
  try:
    if store.orders.hasKey(order.id):
      return
    try:
      store.applyOrder(order, nowMs())
    except CatchableError as exc:
      raise exc
    except Exception as exc:
      raise newException(CatchableError, exc.msg)
  finally:
    store.lock.release()

proc recordMatch*(store: OrderStore; match: MatchMessage): Future[void] {.async: (raises: [CatchableError]).} =
  if store.isNil():
    return
  await store.lock.acquire()
  try:
    try:
      store.applyMatch(match, nowMs())
    except CatchableError as exc:
      raise exc
    except Exception as exc:
      raise newException(CatchableError, exc.msg)
  finally:
    store.lock.release()

proc getOrder*(store: OrderStore; orderId: string): Future[Option[OrderMessage]] {.async: (raises: [CatchableError]).} =
  if store.isNil():
    return none(OrderMessage)
  await store.lock.acquire()
  try:
    if store.orders.hasKey(orderId):
      some(store.orders[orderId].order)
    else:
      none(OrderMessage)
  finally:
    store.lock.release()

proc snapshotStats*(store: OrderStore): Future[OrderStoreStats] {.async: (raises: [CatchableError]).} =
  if store.isNil():
    return OrderStoreStats()
  await store.lock.acquire()
  try:
    var stats = OrderStoreStats(storeExposure: initTable[string, AssetExposure]())
    stats.totalOrders = store.orders.len
    for _, record in store.orders.pairs:
      case record.status
      of osPending:
        inc stats.pendingOrders
      of osMatched:
        inc stats.matchedOrders
    var exposuresCopy = initTable[string, AssetExposure]()
    for asset, exposure in store.exposure.pairs:
      stats.exposure.add(
        (asset: asset, buyVolume: exposure.buyVolume, sellVolume: exposure.sellVolume, matchedVolume: exposure.matchedVolume)
      )
      exposuresCopy[asset] = exposure
    stats.storeExposure = exposuresCopy
    stats.exposure.sort(proc (a, b: ExposureTuple): int = cmp(a.asset, b.asset))
    stats.settlementEvents = store.settlements.len
    result = stats
  finally:
    store.lock.release()

proc exposureFor*(store: OrderStore; asset: string): Future[AssetExposure] {.async: (raises: [CatchableError]).} =
  if store.isNil():
    return AssetExposure()
  await store.lock.acquire()
  try:
    store.exposure.getOrDefault(asset, AssetExposure())
  finally:
    store.lock.release()

proc purgeExpired*(store: OrderStore; nowMs: int64): Future[int] {.async: (raises: [CatchableError]).} =
  if store.isNil():
    return 0
  await store.lock.acquire()
  try:
    var expired: seq[string] = @[]
    for key, record in store.orders.pairs:
      if record.status == osPending and record.expiresMs > 0 and record.expiresMs <= nowMs:
        expired.add(key)
    if expired.len == 0:
      return 0
    for key in expired:
      let record = store.orders.getOrDefault(key, OrderRecord())
      if record.order.baseAsset.symbol.len > 0:
        let assetKey = record.order.baseAsset.symbol
        var exposure = store.exposure.getOrDefault(assetKey, AssetExposure())
        if record.order.side.toLowerAscii() == "buy":
          exposure.buyVolume = max(0.0, exposure.buyVolume - record.order.amount)
        else:
          exposure.sellVolume = max(0.0, exposure.sellVolume - record.order.amount)
        store.exposure[assetKey] = exposure
      store.orders.del(key)
    expired.len
  finally:
    store.lock.release()

proc recordSettlement*(store: OrderStore; record: SettlementRecord) {.async: (raises: [CatchableError]).} =
  if store.isNil():
    return
  await store.lock.acquire()
  try:
    store.settlements.add(record)
    if store.settlements.len > 512:
      store.settlements.delete(0, store.settlements.len - 513)
  finally:
    store.lock.release()
