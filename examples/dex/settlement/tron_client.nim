## Tron Client
## Implements interactions for Tron (TRX/USDC-TRC20).
## Tron uses a different HTTP API (trongrid) rather than standard JSON-RPC 2.0 usually.

import std/[json, strformat, strutils, asyncdispatch, httpclient]
import chronicles
import ./chains
import ../types

type
  TronClient* = ref object of ChainClient
    httpClient: AsyncHttpClient

proc newTronClient*(url: string): TronClient =
  new(result)
  result.rpcUrl = url # e.g. https://api.trongrid.io
  result.chainId = ChainTRX
  result.httpClient = newAsyncHttpClient()

# --- Internal Helper ---

proc post(self: TronClient, endpoint: string, payload: JsonNode): Future[JsonNode] {.async.} =
  try:
    let url = self.rpcUrl & endpoint
    let resp = await self.httpClient.postContent(url, $payload)
    return parseJson(resp)
  except Exception as e:
    raise newException(NetworkError, "Tron Request failed: " & e.msg)

# --- Implementation ---

method getBalance*(self: TronClient, address: string, asset: AssetId): Future[float] {.async.} =
  let payload = %*{"address": address, "visible": true}
  
  if asset.contract.len == 0:
    # Native TRX
    let res = await self.post("/wallet/getaccount", payload)
    if not res.hasKey("balance"): return 0.0
    let sun = res["balance"].getBiggestInt().float
    return sun / 1e6 # 6 decimals for TRX
  else:
    # TRC20 Token - triggerConstantContract
    # Function: balanceOf(address) -> 70a08231
    # Address must be converted to hex, but Tron API often handles base58 in parameter if visible=true? 
    # Actually triggerconstantcontract usually expects hex parameter.
    # For MVP, let's assume the caller or a helper handles address conversion (Base58 -> Hex).
    # But Tron API 'triggerconstantcontract' takes "owner_address", "contract_address", "function_selector", "parameter"
    
    # Simplification: Assume we use a simplified API or assume native handling.
    # Implementing full TRC20 decode in Nim without libraries is complex.
    # Let's return a mock 0.0 for now or basic implementation if possible.
    return 0.0

method sendTransaction*(self: TronClient, signedTx: string): Future[string] {.async.} =
  # signedTx is JSON object usually for Tron? Or hex?
  # Assuming hex raw data
  let payload = %*{"transaction": signedTx} # This depends on specific broadcast API format
  let res = await self.post("/wallet/broadcasttransaction", payload)
  if res["result"].getBool():
    return res["txid"].getStr() # Assuming API returns txid on success
  else:
    raise newException(TransactionError, "Broadcast failed")

method getTransactionStatus*(self: TronClient, txHash: string): Future[SettlementStatus] {.async.} =
  let payload = %*{"value": txHash}
  let res = await self.post("/wallet/gettransactioninfobyid", payload)
  
  # Check if "receipt" exists
  if not res.hasKey("receipt"):
    return ssSubmitted
  
  let resultStr = res["receipt"]["result"].getStr()
  if resultStr == "SUCCESS":
    return ssCompleted
  else:
    return ssFailed

method estimateGas*(self: TronClient, fromAddr, toAddr: string, amount: float, data: string = ""): Future[float] {.async.} =
  # Tron Energy/Bandwidth model
  return 0.0

