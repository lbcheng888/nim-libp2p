import std/[json, math, options, sequtils, sets, strformat, strutils]
import chronos
import chronos/apps/http/httpclient
import bridge/[config, model, storage]
import bridge/services/coordinator
import bridge/services/zkproof

const
  TransferTopic = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

type
  BscWatcher* = ref object
    coord: Coordinator
    store: EventStore
    cfg: BridgeConfig
    client: HttpClientRef
    lastBlock: int64
    processed: HashSet[string]
    pollInterval: Duration
    addressBook: Table[string, string]
    vaultTopic: string

proc normalizeAddress(addr: string): string =
  if addr.len == 0:
    return ""
  var trimmed = addr.strip().toLowerAscii()
  if trimmed.startsWith("0x"):
    result = trimmed
  else:
    result = "0x" & trimmed

proc topicForAddress(addr: string): string =
  let normalized = normalizeAddress(addr)
  if normalized.len < 2:
    return ""
  var suffix = normalized[2 .. ^1]
  if suffix.len > 64:
    suffix = suffix[^64 .. ^1]
  let padding = max(64 - suffix.len, 0)
  "0x" & repeat('0', padding) & suffix

proc hexToInt64(value: string): int64 =
  let trimmed = value.strip()
  if trimmed.len == 0:
    return 0
  let normalized =
    if trimmed.startsWith("0x") or trimmed.startsWith("0X"):
      trimmed[2 .. ^1]
    else:
      trimmed
  parseHexInt(normalized).int64

proc toBlockHex(value: int64): string =
  var target = value
  if target < 0:
    target = 0
  "0x" & toHex(target, 0).toLowerAscii()

proc amountFromHex(data: string; decimals: int): float =
  var normalized = data.strip()
  if normalized.startsWith("0x") or normalized.startsWith("0X"):
    normalized = normalized[2 .. ^1]
  if normalized.len == 0:
    return 0.0
  var accumulator = 0.0
  for ch in normalized:
    var digit = 0
    if ch in {'0' .. '9'}:
      digit = ord(ch) - ord('0')
    elif ch in {'a' .. 'f'}:
      digit = 10 + ord(ch) - ord('a')
    elif ch in {'A' .. 'F'}:
      digit = 10 + ord(ch) - ord('A')
    accumulator = accumulator * 16.0 + float(digit)
  let scale = pow(10.0, float(decimals))
  if scale <= 0.0:
    return accumulator
  accumulator / scale

proc rpcCall(client: HttpClientRef; url, method: string; params: JsonNode): Future[JsonNode] {.async.} =
  var payload = newJObject()
  payload["jsonrpc"] = %"2.0"
  payload["id"] = %1
  payload["method"] = %method
  payload["params"] = params
  let body = $payload
  var headers = HttpHeaders.new()
  headers.add("Content-Type", "application/json")
  let response = await client.post(url, body = body, headers = headers)
  if response.code != Http200:
    raise newException(IOError, "BSC RPC HTTP " & $response.code & " for method " & method)
  let text = await response.body
  let node = parseJson(text)
  if node.hasKey("error") and node["error"].kind == JObject and node["error"].hasKey("message"):
    let errMsg = node["error"]["message"].getStr()
    raise newException(IOError, "BSC RPC error " & errMsg)
  node["result"]

proc newBscWatcher*(coord: Coordinator; store: EventStore; cfg: BridgeConfig): Option[BscWatcher] =
  if cfg.bscRpcUrl.len == 0 or cfg.bscUsdcContract.len == 0 or cfg.bscVaultAddress.len == 0:
    return none(BscWatcher)
  let client = newHttpClient()
  var mappings = initTable[string, string]()
  for entry in cfg.addressMappings:
    let key = normalizeAddress(entry.bsc)
    let value = entry.btc.strip()
    if key.len > 0 and value.len > 0:
      mappings[key] = value
  let watcher = BscWatcher(
    coord: coord,
    store: store,
    cfg: cfg,
    client: client,
    lastBlock: cfg.bscStartBlock,
    processed: initHashSet[string](),
    pollInterval: chronos.milliseconds(max(cfg.bscPollIntervalMs, 1000)),
    addressBook: mappings,
    vaultTopic: topicForAddress(cfg.bscVaultAddress),
  )
  some(watcher)

proc resolveBtcAddress(watcher: BscWatcher; bscAddress: string): string =
  let normalized = normalizeAddress(bscAddress)
  if watcher.addressBook.hasKey(normalized):
    return watcher.addressBook[normalized]
  watcher.cfg.defaultBtcAddress

proc persistEvent(watcher: BscWatcher; ev: LockEventMessage) =
  try:
    watcher.store.put(
      EventRecord(
        event: ev,
        status: esPending,
        signatures: initOrderedTable[string, SignatureMessage](),
        updatedAt: nowEpoch(),
      )
    )
  except CatchableError as exc:
    echo fmt"[bsc-watcher] persist event {ev.eventId} failed: {exc.msg}"

proc publishEvent(watcher: BscWatcher; ev: LockEventMessage) {.async.} =
  watcher.persistEvent(ev)
  let sent = await watcher.coord.publishEvent(ev)
  echo fmt"[bsc-watcher] event {ev.eventId} sent={sent} amount={ev.amount:.6f} {ev.sourceChain}->{ev.targetChain}"

proc decodeAddress(topicValue: string): string =
  if topicValue.len <= 26:
    return "0x0"
  let suffix = topicValue[^40 .. ^1]
  "0x" & suffix

proc handleLogs(watcher: BscWatcher; logs: JsonNode) {.async.} =
  if logs.isNil() or logs.kind != JArray:
    return
  var maxBlock = watcher.lastBlock
  for entry in logs:
    if entry.kind != JObject or not entry.hasKey("transactionHash"):
      continue
    let txHash = entry["transactionHash"].getStr()
    let blockNumber = hexToInt64(entry.getOrDefault("blockNumber", %"0x0").getStr())
    if blockNumber > maxBlock:
      maxBlock = blockNumber
    let logIndex = hexToInt64(entry.getOrDefault("logIndex", %"0x0").getStr())
    let eventId = "bsc-" & txHash & "-" & $logIndex
    if watcher.processed.containsOrIncl(eventId):
      continue
    if not entry.hasKey("topics") or entry["topics"].kind != JArray or entry["topics"].len < 3:
      continue
    if entry["topics"][0].getStr().toLowerAscii() != TransferTopic:
      continue
    let toTopic = entry["topics"][2].getStr().toLowerAscii()
    if toTopic != watcher.vaultTopic.toLowerAscii():
      continue
    let fromAddr = decodeAddress(entry["topics"][1].getStr())
    let btcAddress = watcher.resolveBtcAddress(fromAddr)
    if btcAddress.len == 0:
      echo fmt"[bsc-watcher] missing BTC映射, 跳过 from={fromAddr}"
      continue
    let amount = amountFromHex(entry.getOrDefault("data", %"0x0").getStr(), 18)
    if amount <= watcher.cfg.minBridgeAmount:
      continue
    var payload = newJObject()
    payload["bscTxHash"] = %txHash
    payload["logIndex"] = %logIndex
    payload["from"] = %fromAddr
    payload["to"] = %watcher.cfg.bscVaultAddress
    payload["amountUsdc"] = %amount
    payload["btcAddress"] = %btcAddress
    payload["asset"] = %"USDC"
    payload["sourceChain"] = %"bsc-testnet"
    payload["targetChain"] = %"btc-testnet"
    var ev = LockEventMessage(
      schemaVersion: EventSchemaVersion,
      eventId: eventId,
      watcherPeer: watcher.coord.localPeerId(),
      asset: "USDC",
      amount: amount,
      sourceChain: "bsc-testnet",
      targetChain: "btc-testnet",
      sourceHeight: blockNumber,
      targetHeight: 0,
      proofKey: txHash,
      proofBlob: "",
      proofDigest: "",
      eventHash: "",
    )
    discard attachProof(ev, payload)
    ev.eventHash = computeEventHash(ev)
    await watcher.publishEvent(ev)
  watcher.lastBlock = maxBlock

proc pollOnce(watcher: BscWatcher) {.async.} =
  let latestBlockNode = await watcher.client.rpcCall(
    watcher.cfg.bscRpcUrl,
    "eth_blockNumber",
    newJArray()
  )
  let latestBlock = hexToInt64(latestBlockNode.getStr())
  if watcher.lastBlock == 0:
    watcher.lastBlock = latestBlock
    return
  if watcher.lastBlock >= latestBlock:
    return
  let fromBlock = watcher.lastBlock + 1
  let filter = newJObject()
  filter["fromBlock"] = %toBlockHex(fromBlock)
  filter["toBlock"] = %toBlockHex(latestBlock)
  filter["address"] = %normalizeAddress(watcher.cfg.bscUsdcContract)
  var topics = newJArray()
  topics.add(%TransferTopic)
  topics.add(%newJNull())
  topics.add(%watcher.vaultTopic)
  filter["topics"] = topics
  let params = newJArray()
  params.add(filter)
  let logResult = await watcher.client.rpcCall(watcher.cfg.bscRpcUrl, "eth_getLogs", params)
  await watcher.handleLogs(logResult)

proc start*(watcher: BscWatcher) {.async.} =
  echo "[bsc-watcher] 启动，监听 vault=", watcher.cfg.bscVaultAddress
  while true:
    try:
      await watcher.pollOnce()
    except CatchableError as exc:
      echo "[bsc-watcher] poll 失败: ", exc.msg
    await sleepAsync(watcher.pollInterval)
