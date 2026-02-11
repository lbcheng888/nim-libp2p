## Shared types and helpers for the nim-libp2p DEX examples.

import std/[json, options, strformat, strutils, sequtils, tables, hashes]
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

  ChainId* = enum
    ChainBTC = "BTC"       # Bitcoin (Mainnet/Testnet)
    ChainETH = "ETH"       # Ethereum
    ChainBSC = "BSC"       # Binance Smart Chain
    ChainSOL = "SOL"       # Solana
    ChainTRX = "TRX"       # Tron
    ChainPOL = "POL"       # Polygon
    ChainARB = "ARB"       # Arbitrum

  AssetType* = enum
    AssetNative,    # Native currency (BTC, ETH, SOL)
    AssetERC20,     # EVM Token
    AssetSPL,       # Solana Token
    AssetTRC20,     # Tron Token
    AssetOrdinal    # Bitcoin Ordinal/Rune

  # Robust Asset Definition
  AssetDef* = object
    chainId*: ChainId
    symbol*: string        # e.g. "USDC"
    name*: string          # e.g. "USD Coin"
    decimals*: int         # e.g. 6 or 18
    contractAddress*: string # Hex for EVM, Base58 for SOL, Base58Check for TRX
    assetType*: AssetType
    logoUrl*: string

  # Standardized Multi-Chain Address
  MultiChainAddress* = object
    chain*: ChainId
    raw*: string           # Human readable string (0x... or bc1q... or T...)
    pubKey*: seq[byte]     # Raw public key bytes (33 bytes for BTC/EVM compressed, 32 for Ed25519)

  OrderMessage* = object
    id*: string
    traderPeer*: string
    baseAsset*: AssetDef   # Replaces string asset
    quoteAsset*: AssetDef  # Usually USDC/USDT
    side*: string          # "buy" / "sell"
    price*: float
    amount*: float
    ttlMs*: int
    timestamp*: int64
    signature*: string     # Hex encoded signature of the order hash
    signerPubKey*: string  # Hex encoded public key
    signatureVersion*: int # 0=ECDSA(secp256k1), 1=Ed25519, 2=Schnorr(MuSig2)
    onionPath*: seq[string] # For privacy routing (hops)

  MatchMessage* = object
    orderId*: string
    matcherPeer*: string
    price*: float
    amount*: float
    baseAsset*: AssetDef
    quoteAsset*: AssetDef
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
    baseAsset*: AssetDef
    quoteAsset*: AssetDef
    matcherPeer*: string
    makerPeer*: string
    createdAt*: int64 ## unix ms
    sequence*: int64
    txHash*: string ## Settlement Transaction Hash

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
    ssSubmitted, ssPending, ssConfirming, ssCompleted, ssFailed, ssExpired

  SettlementRecord* = object
    orderId*: string
    baseAsset*: AssetDef
    quoteAsset*: AssetDef
    chain*: ChainId
    txHash*: string
    status*: SettlementStatus
    amount*: float
    fee*: float
    timestamp*: int64

  # --- CoinJoin / Mixer Types (Privacy Network) ---

  MixerIntent* = object
    sessionHint*: uint64
    targetAsset*: AssetDef
    amount*: float
    maxDelayMs*: uint32
    proofDigest*: string
    hopCount*: uint8
    initiatorPk*: string # Ephemeral Public Key for this intent
    signature*: string

  SessionStart* = object
    sessionId*: string
    participants*: seq[string]
    participantPubKeys*: seq[string] # hex encoded X-Only pubkeys
    slotSize*: float
    feeRate*: uint32
    onionSeeds*: seq[string]
    signature*: string

  CommitMsg* = object
    sessionId*: string
    participantId*: string
    commitment*: string # hex encoded
    nonce*: string
    onionProof*: string

  ShuffleMsg* = object
    sessionId*: string
    round*: int
    shuffledList*: seq[string] # list of commitments
    proof*: string

  SignatureMsg* = object
    sessionId*: string
    txDigest*: string
    partialSig*: string

  SettlementProof* = object
    sessionId*: string
    chain*: ChainId
    txId*: string
    proofDigest*: string
    timestamp*: int64

  MixerMessage* = object
    kind*: string
    requestId*: string
    initiator*: string
    targetChain*: ChainId
    amount*: float
    slots*: int
    expiresAt*: int64
    schedule*: seq[string]
    note*: string
    onionPacket*: seq[byte]
    intent*: Option[MixerIntent]
    sessionStart*: Option[SessionStart]
    commit*: Option[CommitMsg]
    shuffle*: Option[ShuffleMsg]
    signature*: Option[SignatureMsg]
    settlement*: Option[SettlementProof]

# --- Helpers ---

proc toString*(a: AssetDef): string =
  $a.chainId & "/" & a.symbol

proc encodeAssetDef*(a: AssetDef): JsonNode =
  %* {
    "chainId": $a.chainId,
    "symbol": a.symbol,
    "name": a.name,
    "decimals": a.decimals,
    "contract": a.contractAddress,
    "type": $a.assetType
  }

proc decodeAssetDef*(n: JsonNode): AssetDef =
  try:
    AssetDef(
      chainId: parseEnum[ChainId](n["chainId"].getStr()),
      symbol: n["symbol"].getStr(),
      name: n.getOrDefault("name").getStr(n["symbol"].getStr()),
      decimals: n["decimals"].getInt(),
      contractAddress: n.getOrDefault("contract").getStr(),
      assetType: parseEnum[AssetType](n.getOrDefault("type").getStr("AssetNative"))
    )
  except:
    # Fallback for empty or legacy
    AssetDef(chainId: ChainBTC, symbol: "UNKNOWN", decimals: 8, assetType: AssetNative)

proc encodeOrder*(order: OrderMessage): seq[byte] =
  let node = %*{
    "id": order.id,
    "traderPeer": order.traderPeer,
    "baseAsset": encodeAssetDef(order.baseAsset),
    "quoteAsset": encodeAssetDef(order.quoteAsset),
    "side": order.side,
    "price": order.price,
    "amount": order.amount,
    "ttlMs": order.ttlMs,
    "signature": order.signature,
    "signerPubKey": order.signerPubKey,
    "signatureVersion": order.signatureVersion,
    "timestamp": order.timestamp
  }
  if order.onionPath.len > 0:
    node["onionPath"] = %order.onionPath
  ($node).toBytes()

proc decodeOrder*(data: seq[byte]): Option[OrderMessage] =
  try:
    let parsed = parseJson(string.fromBytes(data))
    result = some(OrderMessage(
      id: parsed["id"].getStr(),
      traderPeer: parsed["traderPeer"].getStr(),
      baseAsset: decodeAssetDef(parsed["baseAsset"]),
      quoteAsset: decodeAssetDef(parsed["quoteAsset"]),
      side: parsed["side"].getStr(),
      price: parsed["price"].getFloat(),
      amount: parsed["amount"].getFloat(),
      ttlMs: parsed["ttlMs"].getInt(),
      timestamp: parsed.getOrDefault("timestamp").getBiggestInt(),
      signature: parsed["signature"].getStr(),
      signerPubKey: parsed["signerPubKey"].getStr(),
      signatureVersion: parsed["signatureVersion"].getInt()
    ))
    if parsed.hasKey("onionPath"):
      result.get().onionPath = parsed["onionPath"].getElems().mapIt(it.getStr())
  except: return none(OrderMessage)

proc matchMessageToJson*(match: MatchMessage): JsonNode =
  result = %*{
    "orderId": match.orderId,
    "matcherPeer": match.matcherPeer,
    "price": match.price,
    "amount": match.amount,
    "baseAsset": encodeAssetDef(match.baseAsset),
    "quoteAsset": encodeAssetDef(match.quoteAsset),
    "note": match.note,
    "signature": match.signature,
    "signerPubKey": match.signerPubKey,
    "signatureVersion": match.signatureVersion,
  }

proc encodeMatch*(match: MatchMessage): seq[byte] =
  ($matchMessageToJson(match)).toBytes()

proc decodeMatch*(data: seq[byte]): Option[MatchMessage] =
  try:
    let parsed = parseJson(string.fromBytes(data))
    if parsed.kind != JObject:
      return none(MatchMessage)
    var match = MatchMessage(
      orderId: parsed.getOrDefault("orderId").getStr(),
      matcherPeer: parsed.getOrDefault("matcherPeer").getStr(),
      price: parsed.getOrDefault("price").getFloat(),
      amount: parsed.getOrDefault("amount").getFloat(),
      baseAsset:
        if parsed.hasKey("baseAsset"):
          decodeAssetDef(parsed["baseAsset"])
        else:
          AssetDef(chainId: ChainBTC, symbol: "BTC", decimals: 8, assetType: AssetNative),
      quoteAsset:
        if parsed.hasKey("quoteAsset"):
          decodeAssetDef(parsed["quoteAsset"])
        else:
          AssetDef(chainId: ChainETH, symbol: "USDC", decimals: 6, assetType: AssetERC20),
      note:
        if parsed.hasKey("note") and parsed["note"].kind == JString:
          parsed["note"].getStr()
        else:
          "",
      signature:
        if parsed.hasKey("signature") and parsed["signature"].kind == JString:
          parsed["signature"].getStr()
        else:
          "",
      signerPubKey:
        if parsed.hasKey("signerPubKey") and parsed["signerPubKey"].kind == JString:
          parsed["signerPubKey"].getStr()
        else:
          "",
      signatureVersion:
        if parsed.hasKey("signatureVersion") and parsed["signatureVersion"].kind in {JInt, JFloat}:
          parsed["signatureVersion"].getInt()
        else:
          0,
    )
    result = some(match)
  except:
    result = none(MatchMessage)

proc tradeEventToJson*(trade: TradeEvent): JsonNode =
  result = %*{
    "matchId": trade.matchId,
    "orderId": trade.orderId,
    "takerSide": trade.takerSide,
    "price": trade.price,
    "amount": trade.amount,
    "baseAsset": encodeAssetDef(trade.baseAsset),
    "quoteAsset": encodeAssetDef(trade.quoteAsset),
    "matcherPeer": trade.matcherPeer,
    "makerPeer": trade.makerPeer,
    "createdAt": trade.createdAt,
    "sequence": trade.sequence,
    "txHash": trade.txHash,
  }

proc encodeTrade*(trade: TradeEvent): seq[byte] =
  ($tradeEventToJson(trade)).toBytes()

proc decodeTrade*(data: seq[byte]): Option[TradeEvent] =
  try:
    let node = parseJson(string.fromBytes(data))
    if node.kind != JObject:
      return none(TradeEvent)
    result = some(TradeEvent(
      matchId: node.getOrDefault("matchId").getStr(),
      orderId: node.getOrDefault("orderId").getStr(),
      takerSide: node.getOrDefault("takerSide").getStr(),
      price: node.getOrDefault("price").getFloat(),
      amount: node.getOrDefault("amount").getFloat(),
      baseAsset: decodeAssetDef(node.getOrDefault("baseAsset")),
      quoteAsset: decodeAssetDef(node.getOrDefault("quoteAsset")),
      matcherPeer: node.getOrDefault("matcherPeer").getStr(),
      makerPeer: node.getOrDefault("makerPeer").getStr(),
      createdAt:
        if node.hasKey("createdAt") and node["createdAt"].kind in {JInt, JFloat}:
          node["createdAt"].getBiggestInt()
        else:
          0'i64,
      sequence:
        if node.hasKey("sequence") and node["sequence"].kind in {JInt, JFloat}:
          node["sequence"].getBiggestInt()
        else:
          0'i64,
      txHash:
        if node.hasKey("txHash") and node["txHash"].kind == JString:
          node["txHash"].getStr()
        else:
          "",
    ))
  except:
    result = none(TradeEvent)

proc klineBucketToJson*(bucket: KlineBucket): JsonNode =
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

proc settlementStatusToStr*(status: SettlementStatus): string =
  $status

proc settlementStatusFromStr*(value: string): SettlementStatus =
  try:
    parseEnum[SettlementStatus](value)
  except:
    ssSubmitted

# --- Mixer Encoding Helpers ---

proc encodeMixer*(msg: MixerMessage): seq[byte] =
  let node = %*{
    "kind": msg.kind,
    "requestId": msg.requestId,
    "initiator": msg.initiator
  }
  
  if msg.onionPacket.len > 0:
    node["onionPacket"] = %byteutils.toHex(msg.onionPacket)
    
  if msg.sessionStart.isSome:
    let s = msg.sessionStart.get()
    node["sessionStart"] = %*{
      "sessionId": s.sessionId,
      "participants": s.participants,
      "participantPubKeys": s.participantPubKeys,
      "slotSize": s.slotSize,
      "feeRate": s.feeRate,
      "onionSeeds": s.onionSeeds
    }
    
  if msg.commit.isSome:
    let c = msg.commit.get()
    node["commit"] = %*{
      "sessionId": c.sessionId,
      "participantId": c.participantId,
      "commitment": c.commitment,
      "nonce": c.nonce
    }
    
  if msg.shuffle.isSome:
    let s = msg.shuffle.get()
    node["shuffle"] = %*{
      "sessionId": s.sessionId,
      "round": s.round,
      "shuffledList": s.shuffledList,
      "proof": s.proof
    }
    
  if msg.signature.isSome:
    let s = msg.signature.get()
    node["signature"] = %*{
      "sessionId": s.sessionId,
      "txDigest": s.txDigest,
      "partialSig": s.partialSig
    }
    
  if msg.settlement.isSome:
    let s = msg.settlement.get()
    node["settlement"] = %*{
      "sessionId": s.sessionId,
      "chain": $s.chain,
      "txId": s.txId,
      "timestamp": s.timestamp
    }
    
  ($node).toBytes()

proc decodeMixer*(data: seq[byte]): Option[MixerMessage] =
  try:
    let node = parseJson(string.fromBytes(data))
    var msg = MixerMessage(
      kind: node["kind"].getStr(),
      requestId: node["requestId"].getStr(),
      initiator: node["initiator"].getStr()
    )
    
    if node.hasKey("onionPacket"):
      msg.onionPacket = byteutils.hexToSeqByte(node["onionPacket"].getStr())
      
    if node.hasKey("sessionStart"):
      let n = node["sessionStart"]
      msg.sessionStart = some(SessionStart(
        sessionId: n["sessionId"].getStr(),
        participants: n["participants"].getElems().mapIt(it.getStr()),
        participantPubKeys: n["participantPubKeys"].getElems().mapIt(it.getStr()),
        slotSize: n["slotSize"].getFloat(),
        feeRate: n["feeRate"].getBiggestInt().uint32,
        onionSeeds: n["onionSeeds"].getElems().mapIt(it.getStr())
      ))
      
    if node.hasKey("commit"):
      let n = node["commit"]
      msg.commit = some(CommitMsg(
        sessionId: n["sessionId"].getStr(),
        participantId: n["participantId"].getStr(),
        commitment: n["commitment"].getStr(),
        nonce: n["nonce"].getStr()
      ))
      
    if node.hasKey("shuffle"):
      let n = node["shuffle"]
      msg.shuffle = some(ShuffleMsg(
        sessionId: n["sessionId"].getStr(),
        round: n["round"].getInt(),
        shuffledList: n["shuffledList"].getElems().mapIt(it.getStr()),
        proof: n["proof"].getStr()
      ))
      
    if node.hasKey("signature"):
      let n = node["signature"]
      msg.signature = some(SignatureMsg(
        sessionId: n["sessionId"].getStr(),
        txDigest: n["txDigest"].getStr(),
        partialSig: n["partialSig"].getStr()
      ))
      
    if node.hasKey("settlement"):
      let n = node["settlement"]
      msg.settlement = some(SettlementProof(
        sessionId: n["sessionId"].getStr(),
        chain: parseEnum[ChainId](n["chain"].getStr()),
        txId: n["txId"].getStr(),
        timestamp: n["timestamp"].getBiggestInt()
      ))
      
    result = some(msg)
  except:
    result = none(MixerMessage)
