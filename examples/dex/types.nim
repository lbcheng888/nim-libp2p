## Shared types and helpers for the nim-libp2p DEX examples.

import std/[json, options, strformat, strutils]
import stew/byteutils

const
  OrdersTopic* = "dex.orders"
  MatchesTopic* = "dex.matches"
  TradesTopic* = "dex.trades"
  MixersTopic* = "dex.mixers"
  KlineTopicPrefix* = "dex.kline." ## complete topic: prefix & bucket key

type
  DexMode* = enum
    dmTrader, dmMatcher, dmObserver, dmKline

  OrderMessage* = object
    id*: string
    traderPeer*: string
    asset*: string
    side*: string
    price*: float
    amount*: float
    ttlMs*: int
    signature*: string
    signerPubKey*: string
    signatureVersion*: int

  MatchMessage* = object
    orderId*: string
    matcherPeer*: string
    price*: float
    amount*: float
    asset*: string
    note*: string
    signature*: string
    signerPubKey*: string
    signatureVersion*: int

  TradeEvent* = object
    matchId*: string
    orderId*: string
    takerSide*: string ## buy/sell
    price*: float
    amount*: float
    asset*: string
    matcherPeer*: string
    makerPeer*: string
    createdAt*: int64 ## unix ms
    sequence*: int64

  Timescale* = enum
    ts1s = 1
    ts1m = 60
    ts5m = 300
    ts15m = 900
    ts1h = 3600

  KlineBucket* = object
    asset*: string
    scale*: Timescale
    windowStartMs*: int64
    open*: float
    high*: float
    low*: float
    close*: float
    volume*: float
    trades*: int
    closed*: bool
    publishSeq*: int64

  SettlementStatus* = enum
    ssSubmitted, ssCompleted, ssFailed

  SettlementRecord* = object
    orderId*: string
    asset*: string
    chain*: string
    direction*: string
    amount*: float
    txId*: string
    status*: SettlementStatus
    note*: string

  MixerMessage* = object
    kind*: string
    requestId*: string
    initiator*: string
    asset*: string
    amount*: float
    slots*: int
    expiresAt*: int64
    schedule*: seq[string]
    note*: string

proc encodeOrder*(order: OrderMessage): seq[byte] =
  let node = %*{
    "id": order.id,
    "traderPeer": order.traderPeer,
    "asset": order.asset,
    "side": order.side,
    "price": order.price,
    "amount": order.amount,
    "ttlMs": order.ttlMs,
    "signature": order.signature,
    "signerPubKey": order.signerPubKey,
    "signatureVersion": order.signatureVersion,
  }
  ($node).toBytes()

proc decodeOrder*(data: seq[byte]): Option[OrderMessage] =
  try:
    let parsed = parseJson(string.fromBytes(data))
    if parsed.kind != JObject or not parsed.hasKey("id"):
      return none(OrderMessage)
    let ttl = if parsed.hasKey("ttlMs"): parsed["ttlMs"].getInt() else: 0
    result = some(
      OrderMessage(
        id: parsed["id"].getStr(),
        traderPeer: parsed["traderPeer"].getStr(),
        asset: parsed["asset"].getStr(),
        side: parsed["side"].getStr(),
        price: parsed["price"].getFloat(),
        amount: parsed["amount"].getFloat(),
        ttlMs: ttl,
        signature:
          if parsed.hasKey("signature"): parsed["signature"].getStr() else: "",
        signerPubKey:
          if parsed.hasKey("signerPubKey"): parsed["signerPubKey"].getStr()
          else:
            "",
        signatureVersion:
          if parsed.hasKey("signatureVersion"): parsed["signatureVersion"].getInt()
          else:
            0,
      )
    )
  except CatchableError:
    return none(OrderMessage)

proc encodeMatch*(match: MatchMessage): seq[byte] =
  let node = %*{
    "orderId": match.orderId,
    "matcherPeer": match.matcherPeer,
    "price": match.price,
    "amount": match.amount,
    "asset": match.asset,
    "note": match.note,
    "signature": match.signature,
    "signerPubKey": match.signerPubKey,
    "signatureVersion": match.signatureVersion,
  }
  ($node).toBytes()

proc decodeMatch*(data: seq[byte]): Option[MatchMessage] =
  try:
    let parsed = parseJson(string.fromBytes(data))
    if parsed.kind != JObject or not parsed.hasKey("orderId"):
      return none(MatchMessage)
    let note = if parsed.hasKey("note"): parsed["note"].getStr() else: ""
    result = some(
      MatchMessage(
        orderId: parsed["orderId"].getStr(),
        matcherPeer: parsed["matcherPeer"].getStr(),
        price: parsed["price"].getFloat(),
        amount: parsed["amount"].getFloat(),
        asset: if parsed.hasKey("asset"): parsed["asset"].getStr() else: "",
        note: note,
        signature:
          if parsed.hasKey("signature"): parsed["signature"].getStr() else: "",
        signerPubKey:
          if parsed.hasKey("signerPubKey"): parsed["signerPubKey"].getStr()
          else:
            "",
        signatureVersion:
          if parsed.hasKey("signatureVersion"): parsed["signatureVersion"].getInt()
          else:
            0,
      )
      )
  except CatchableError:
    return none(MatchMessage)

proc encodeTrade*(trade: TradeEvent): seq[byte] =
  let node = %*{
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
  ($node).toBytes()

proc decodeTrade*(data: seq[byte]): Option[TradeEvent] =
  try:
    let parsed = parseJson(string.fromBytes(data))
    if parsed.kind != JObject or not parsed.hasKey("matchId"):
      return none(TradeEvent)
    let getStrOr = proc(key, defaultVal: string): string =
      if parsed.hasKey(key): parsed[key].getStr() else: defaultVal
    let getFloatOr = proc(key: string; defaultVal: float): float =
      if parsed.hasKey(key): parsed[key].getFloat() else: defaultVal
    let getIntOr = proc(key: string; defaultVal: int64): int64 =
      if parsed.hasKey(key): parsed[key].getInt().int64 else: defaultVal
    result = some(
      TradeEvent(
        matchId: parsed["matchId"].getStr(),
        orderId: getStrOr("orderId", ""),
        takerSide: getStrOr("takerSide", ""),
        price: getFloatOr("price", 0.0),
        amount: getFloatOr("amount", 0.0),
        asset: getStrOr("asset", ""),
        matcherPeer: getStrOr("matcherPeer", ""),
        makerPeer: getStrOr("makerPeer", ""),
        createdAt: getIntOr("createdAt", 0),
        sequence: getIntOr("sequence", 0),
      )
    )
  except CatchableError:
    return none(TradeEvent)

proc encodeMixer*(msg: MixerMessage): seq[byte] =
  let node = %*{
    "kind": msg.kind,
    "requestId": msg.requestId,
    "initiator": msg.initiator,
    "asset": msg.asset,
    "amount": msg.amount,
    "slots": msg.slots,
    "expiresAt": msg.expiresAt,
    "schedule": msg.schedule,
    "note": msg.note,
  }
  ($node).toBytes()

proc decodeMixer*(data: seq[byte]): Option[MixerMessage] =
  try:
    let parsed = parseJson(string.fromBytes(data))
    if parsed.kind != JObject or not parsed.hasKey("requestId"):
      return none(MixerMessage)
    var schedule: seq[string] = @[]
    if parsed.hasKey("schedule") and parsed["schedule"].kind == JArray:
      for item in parsed["schedule"]:
        schedule.add(item.getStr())
    some(
      MixerMessage(
        kind: if parsed.hasKey("kind"): parsed["kind"].getStr() else: "request",
        requestId: parsed["requestId"].getStr(),
        initiator: if parsed.hasKey("initiator"): parsed["initiator"].getStr() else: "",
        asset: if parsed.hasKey("asset"): parsed["asset"].getStr() else: "",
        amount: if parsed.hasKey("amount"): parsed["amount"].getFloat() else: 0.0,
        slots: if parsed.hasKey("slots"): parsed["slots"].getInt() else: 0,
        expiresAt: if parsed.hasKey("expiresAt"): parsed["expiresAt"].getInt().int64 else: 0,
        schedule: schedule,
        note: if parsed.hasKey("note"): parsed["note"].getStr() else: "",
      )
    )
  except CatchableError:
    none(MixerMessage)

proc canonicalOrderPayload*(order: OrderMessage): string =
  let price = formatFloat(order.price, ffDecimal, 8)
  let amount = formatFloat(order.amount, ffDecimal, 8)
  &"order|{order.id}|{order.traderPeer}|{order.asset}|{order.side}|{price}|{amount}|{order.ttlMs}"

proc canonicalMatchPayload*(match: MatchMessage): string =
  let price = formatFloat(match.price, ffDecimal, 8)
  let amount = formatFloat(match.amount, ffDecimal, 8)
  &"match|{match.orderId}|{match.matcherPeer}|{match.asset}|{price}|{amount}|{match.note}"

proc settlementStatusToStr*(status: SettlementStatus): string =
  case status
  of ssSubmitted:
    "submitted"
  of ssCompleted:
    "completed"
  of ssFailed:
    "failed"

proc settlementStatusFromStr*(value: string): SettlementStatus =
  let lowered = value.toLowerAscii()
  case lowered
  of "completed":
    ssCompleted
  of "failed":
    ssFailed
  else:
    ssSubmitted

proc bucketTopic*(bucket: KlineBucket): string =
  ## Assemble gossip topic for the bucket: dex.kline.<symbol>.<scale>
  let assetKey = bucket.asset.replace("/", "_").toLowerAscii()
  KlineTopicPrefix & assetKey & "." & $bucket.scale.int

proc bucketKey*(asset: string; scale: Timescale; windowStartMs: int64): string =
  asset.toLowerAscii() & "|" & $scale.int & "|" & $windowStartMs

proc alignWindow*(timestampMs: int64; scale: Timescale): int64 =
  let seconds = timestampMs div 1000
  let bucketStart = (seconds div scale.int) * scale.int
  bucketStart * 1000

proc initBucket*(asset: string; scale: Timescale; timestampMs: int64; firstTrade: TradeEvent): KlineBucket =
  let windowStart = alignWindow(timestampMs, scale)
  KlineBucket(
    asset: asset,
    scale: scale,
    windowStartMs: windowStart,
    open: firstTrade.price,
    high: firstTrade.price,
    low: firstTrade.price,
    close: firstTrade.price,
    volume: firstTrade.amount,
    trades: 1,
    closed: false,
    publishSeq: firstTrade.sequence,
  )

proc updateBucket*(bucket: var KlineBucket; trade: TradeEvent) =
  if bucket.trades == 0:
    bucket.open = trade.price
    bucket.low = trade.price
  bucket.high = max(bucket.high, trade.price)
  bucket.low = min(bucket.low, trade.price)
  bucket.close = trade.price
  bucket.volume += trade.amount
  inc bucket.trades
  bucket.publishSeq = max(bucket.publishSeq, trade.sequence)

proc encodeBucket*(bucket: KlineBucket): seq[byte] =
  let node = %*{
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
  ($node).toBytes()

proc decodeBucket*(payload: seq[byte]): Option[KlineBucket] =
  try:
    let node = parseJson(string.fromBytes(payload))
    if node.kind != JObject or not node.hasKey("asset"):
      return none(KlineBucket)
    let scaleVal = if node.hasKey("scale"): node["scale"].getInt() else: ts1m.int
    result = some(
      KlineBucket(
        asset: node["asset"].getStr(),
        scale: Timescale(scaleVal),
        windowStartMs: if node.hasKey("windowStartMs"): node["windowStartMs"].getInt().int64 else: 0'i64,
        open: if node.hasKey("open"): node["open"].getFloat() else: 0.0,
        high: if node.hasKey("high"): node["high"].getFloat() else: 0.0,
        low: if node.hasKey("low"): node["low"].getFloat() else: 0.0,
        close: if node.hasKey("close"): node["close"].getFloat() else: 0.0,
        volume: if node.hasKey("volume"): node["volume"].getFloat() else: 0.0,
        trades: if node.hasKey("trades"): node["trades"].getInt() else: 0,
        closed: if node.hasKey("closed"): node["closed"].getBool() else: false,
        publishSeq: if node.hasKey("publishSeq"): node["publishSeq"].getInt().int64 else: 0'i64,
      )
    )
  except CatchableError:
    return none(KlineBucket)
