import std/[json, math, strformat, strutils]
import bridge/[config, model]
import bridge/chains/btc_client
import bridge/services/executorservice
import bridge/services/zkproof

proc usdcToSats(amountUsdc, priceUsd: float): int64 =
  if amountUsdc <= 0.0 or priceUsd <= 0.0:
    return 0
  let btcAmount = amountUsdc / priceUsd
  let sats = int64(round(btcAmount * 100_000_000.0))
  if sats <= 0: 1 else: sats

proc satsToBtc(sats: int64): float =
  float(sats) / 100_000_000.0

proc parseBridgePayload(proofBlob: string): Option[JsonNode] =
  let proofOpt = proofPayload(proofBlob)
  if proofOpt.isSome():
    return proofOpt
  if proofBlob.len == 0:
    return none(JsonNode)
  try:
    some(parseJson(proofBlob))
  except CatchableError:
    none(JsonNode)

proc newBridgeExecutorHooks*(cfg: BridgeConfig): ExecutorHooks =
  if cfg.btcRpcUrl.len == 0:
    return defaultHooks()
  var client = newBtcRpcClient(cfg.btcRpcUrl, cfg.btcRpcUser, cfg.btcRpcPassword)
  result.finalize =
    proc(ev: LockEventMessage; sigs: seq[SignatureMessage]) {.closure, raises: [].} =
      if ev.asset.toLowerAscii() != "usdc":
        return
      let payloadOpt = parseBridgePayload(ev.proofBlob)
      if payloadOpt.isNone():
        echo "[btc-exec] 无法解析 proofBlob, event=", ev.eventId
        return
      let payload = payloadOpt.get()
      let btcAddress =
        if payload.hasKey("btcAddress"): payload["btcAddress"].getStr()
        else: cfg.defaultBtcAddress
      if btcAddress.len == 0:
        echo "[btc-exec] 缺少 BTC 地址, event=", ev.eventId
        return
      let usdcAmount =
        if payload.hasKey("amountUsdc"): payload["amountUsdc"].getFloat()
        else: ev.amount
      let sats = usdcToSats(usdcAmount, cfg.btcUsdPrice)
      if sats <= 0:
        echo "[btc-exec] 兑付金额过小, event=", ev.eventId
        return
      let btcAmount = satsToBtc(sats)
      try:
        let txId = client.sendToAddress(btcAddress, btcAmount)
        echo fmt"[btc-exec] 已兑付 BTC tx={txId} amount={btcAmount:.8f} btcAddr={btcAddress} srcTx={payload.getOrDefault(\"bscTxHash\", %\"\").getStr()}"
      except CatchableError as exc:
        echo "[btc-exec] 执行失败: ", exc.msg
