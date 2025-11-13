## Cross-chain settlement coordinator (BTC ↔ USDC stubs).
##
## This module loads RPC configuration from environment variables and
## simulates settlement flows so the DEX can evolve without yet touching
## real wallets. Replace the stub implementations as on-chain workflows
## roll out.

import std/[os, options, sequtils, strformat, strutils]
import chronos

import ../types
import ../storage/order_store

const
  DefaultBtcRpc = "http://149.28.194.117:8332"
  DefaultEthRpc = "https://rpc.ankr.com/eth"
  DefaultBscRpc = "https://bsc-dataseed.binance.org/"
  DefaultSolRpc = "https://api.mainnet-beta.solana.com"
  DefaultTronRpc = "https://api.trongrid.io"

  EthUsdcAddress = "0xA0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
  BscUsdcAddress = "0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d"
  SolUsdcMint = "EPjFWdd5AufqSSqeM2qDQ3Nf5JknLm4K71y9w4G5DNs"
  TronUsdcContract = "TEkxiTehnzSmSe2XqrBj4w32RUN966rdz8"

type
  ChainClient = object
    name: string
    rpcUrl: string
    tokenAddress: string

  CrossChainStats* = object
    submitted*: int
    completed*: int
    failed*: int

  CrossChainCoordinator* = ref object
    btcRpcUrl: string
    btcRpcUser: string
    btcRpcPassword: string
    usdcClients: seq[ChainClient]
    stats*: CrossChainStats
    enabled: bool
    store: OrderStore

proc envOr(name: string, defaultValue: string): string =
  let value = getEnv(name)
  if value.len == 0:
    return defaultValue
  value

proc envBool(name: string, defaultValue = false): bool =
  let value = getEnv(name)
  if value.len == 0:
    return defaultValue
  let lowered = value.toLowerAscii()
  lowered in ["1", "true", "yes", "on"]

proc requiresCrossChain(asset: string): bool =
  let upper = asset.toUpperAscii()
  "BTC" in upper and "USDC" in upper

proc inferChain(asset: string): string =
  ## Basic heuristic: support notation like BTC/USDC@ETH.
  let normalized = asset.toLowerAscii()
  if "@" in normalized:
    let parts = normalized.split("@")
    if parts.len >= 2:
      return parts[^1]
  if "tron" in normalized:
    return "tron"
  if "sol" in normalized:
    return "sol"
  if "bsc" in normalized or "binance" in normalized:
    return "bsc"
  "eth"

proc newCrossChainCoordinator*(store: OrderStore): CrossChainCoordinator =
  if envBool("DEX_DISABLE_CROSSCHAIN"):
    return nil
  new(result)
  result.enabled = true
  result.store = store
  result.btcRpcUrl = envOr("BTC_RPC_URL", DefaultBtcRpc)
  result.btcRpcUser = getEnv("RPC_USER")
  result.btcRpcPassword = getEnv("RPC_PASSWORD")
  result.usdcClients = @[
    ChainClient(
      name: "eth",
      rpcUrl: envOr("ETH_RPC_URL", DefaultEthRpc),
      tokenAddress: envOr("ETH_USDC_ADDRESS", EthUsdcAddress),
    ),
    ChainClient(
      name: "bsc",
      rpcUrl: envOr("BSC_RPC_URL", DefaultBscRpc),
      tokenAddress: envOr("BSC_USDC_ADDRESS", BscUsdcAddress),
    ),
    ChainClient(
      name: "sol",
      rpcUrl: envOr("SOL_RPC_URL", DefaultSolRpc),
      tokenAddress: envOr("SOL_USDC_MINT", SolUsdcMint),
    ),
    ChainClient(
      name: "tron",
      rpcUrl: envOr("TRON_RPC_URL", DefaultTronRpc),
      tokenAddress: envOr("TRON_USDC_ADDRESS", TronUsdcContract),
    ),
  ]

proc selectClient(coord: CrossChainCoordinator; chainName: string): Option[ChainClient] =
  for client in coord.usdcClients:
    if client.name.toLowerAscii() == chainName.toLowerAscii():
      return some(client)
  none(ChainClient)

proc statsSummary*(coord: CrossChainCoordinator): string =
  if coord.isNil() or not coord.enabled:
    return "disabled"
  &"submitted={coord.stats.submitted} completed={coord.stats.completed} failed={coord.stats.failed}"

proc logBtcInfo(coord: CrossChainCoordinator) =
  var authState = "missing-credentials"
  if coord.btcRpcUser.len > 0 or coord.btcRpcPassword.len > 0:
    authState = "credentials-present"
  echo &"[btc/rpc] url={coord.btcRpcUrl} auth={authState}"

proc simulateBtcSwap(coord: CrossChainCoordinator; match: MatchMessage) {.async.} =
  if coord.btcRpcUrl.len == 0:
    echo "[settle/btc] rpc url missing, skip settlement for ", match.orderId
    return
  await sleepAsync(chronos.milliseconds(25))
  let txId = "btc-sim-" & match.orderId
  echo &"[settle/btc] simulated PSBT flow order={match.orderId} amount={match.amount:.6f} asset={match.asset} tx={txId}"
  if not coord.store.isNil():
    await coord.store.recordSettlement(
      SettlementRecord(
        orderId: match.orderId,
        asset: match.asset,
        chain: "btc",
        direction: "btc",
        amount: match.amount,
        txId: txId,
        status: ssCompleted,
        note: "simulated-btc",
      )
    )

proc simulateUsdcTransfer(
    coord: CrossChainCoordinator; client: ChainClient; match: MatchMessage
) {.async.} =
  await sleepAsync(chronos.milliseconds(20))
  echo &"[settle/usdc] chain={client.name} rpc={client.rpcUrl} token={client.tokenAddress} amount={match.amount:.6f} order={match.orderId}"
  if not coord.store.isNil():
    await coord.store.recordSettlement(
      SettlementRecord(
        orderId: match.orderId,
        asset: match.asset,
        chain: client.name,
        direction: "usdc",
        amount: match.amount,
        txId: client.name & "-sim-" & match.orderId,
        status: ssCompleted,
        note: "simulated-usdc",
      )
    )

proc handleMatch*(coord: CrossChainCoordinator; match: MatchMessage) {.async.} =
  if coord.isNil() or not coord.enabled or not requiresCrossChain(match.asset):
    return
  inc coord.stats.submitted
  try:
    coord.logBtcInfo()
    let chainName = inferChain(match.asset)
    let clientOpt = coord.selectClient(chainName)
    await coord.simulateBtcSwap(match)
    if clientOpt.isSome():
      await coord.simulateUsdcTransfer(clientOpt.get(), match)
    else:
      echo &"[settle/usdc] no client for chain={chainName}, order={match.orderId}"
    inc coord.stats.completed
  except CatchableError as exc:
    inc coord.stats.failed
    echo &"[settle] cross-chain failure order={match.orderId} err={exc.msg}"
