## Solana Client
## Implements generic JSON-RPC interactions for Solana.

import std/[json, strformat, strutils, asyncdispatch, httpclient, base64]
import chronicles
import ./chains
import ../types

type
  SolanaClient* = ref object of ChainClient
    httpClient: AsyncHttpClient

proc newSolanaClient*(url: string): SolanaClient =
  new(result)
  result.rpcUrl = url
  result.chainId = ChainSOL
  result.httpClient = newAsyncHttpClient()

# --- Internal RPC Helper ---

proc rpcCall(self: SolanaClient, methodStr: string, params: JsonNode): Future[JsonNode] {.async.} =
  let payload = %*{
    "jsonrpc": "2.0",
    "method": methodStr,
    "params": params,
    "id": 1
  }
  try:
    let resp = await self.httpClient.postContent(self.rpcUrl, $payload)
    let jsonResp = parseJson(resp)
    if jsonResp.hasKey("error"):
      raise newException(NetworkError, "Solana RPC Error: " & jsonResp["error"]["message"].getStr())
    return jsonResp["result"]
  except Exception as e:
    raise newException(NetworkError, "HTTP Request failed: " & e.msg)

# --- Implementation ---

method getBalance*(self: SolanaClient, address: string, asset: AssetId): Future[float] {.async.} =
  if asset.contract.len == 0:
    # Native SOL
    let res = await self.rpcCall("getBalance", %*[address])
    let lamports = res["value"].getBiggestInt().float
    return lamports / 1e9 # 9 decimals for SOL
  else:
    # SPL Token
    # getTokenAccountsByOwner
    let filter = %*{"mint": asset.contract}
    let config = %*{"encoding": "jsonParsed"}
    let res = await self.rpcCall("getTokenAccountsByOwner", %*[address, filter, config])
    let accounts = res["value"]
    if accounts.len == 0: return 0.0
    
    # Sum up all accounts for this mint (usually just one)
    var total = 0.0
    for acc in accounts:
      let info = acc["account"]["data"]["parsed"]["info"]["tokenAmount"]
      total += info["uiAmount"].getFloat()
    return total

method sendTransaction*(self: SolanaClient, signedTx: string): Future[string] {.async.} =
  # signedTx is base64 encoded serialized transaction
  let config = %*{"encoding": "base64"}
  let res = await self.rpcCall("sendTransaction", %*[signedTx, config])
  return res.getStr()

method getTransactionStatus*(self: SolanaClient, txHash: string): Future[SettlementStatus] {.async.} =
  let res = await self.rpcCall("getSignatureStatuses", %*[%*[txHash]])
  let statuses = res["value"]
  if statuses.kind == JNull or statuses.len == 0 or statuses[0].kind == JNull:
    return ssSubmitted # Not found or pending
  
  let status = statuses[0]
  if status.hasKey("err") and status["err"].kind != JNull:
    return ssFailed
  
  let confirmationStatus = status["confirmationStatus"].getStr()
  if confirmationStatus == "finalized":
    return ssCompleted
  else:
    return ssSubmitted # 'processed' or 'confirmed' -> wait for finalized for safety

method estimateGas*(self: SolanaClient, fromAddr, toAddr: string, amount: float, data: string = ""): Future[float] {.async.} =
  # Solana uses fixed fees mostly (5000 lamports per signature), generic estimation involves querying blockhash fee
  # For MVP, return standard fee
  return 0.000005

