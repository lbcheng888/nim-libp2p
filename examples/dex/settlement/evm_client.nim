## EVM Client (Ethereum, BSC, Polygon, etc.)
## Implements generic JSON-RPC interactions for EVM chains.

import std/[json, strformat, strutils, asyncdispatch, httpclient]
import chronicles
import ./chains
import ../types

type
  EvmClient* = ref object of ChainClient
    httpClient: AsyncHttpClient

proc newEvmClient*(url: string, chain: ChainId): EvmClient =
  new(result)
  result.rpcUrl = url
  result.chainId = chain
  result.httpClient = newAsyncHttpClient()

# --- Internal RPC Helper ---

proc rpcCall(self: EvmClient, methodStr: string, params: JsonNode): Future[JsonNode] {.async.} =
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
      raise newException(NetworkError, "RPC Error: " & jsonResp["error"]["message"].getStr())
    return jsonResp["result"]
  except Exception as e:
    raise newException(NetworkError, "HTTP Request failed: " & e.msg)

# --- Implementation ---

method getBalance*(self: EvmClient, address: string, asset: AssetId): Future[float] {.async.} =
  if asset.contract.len == 0:
    # Native ETH/BNB
    let res = await self.rpcCall("eth_getBalance", %*[address, "latest"])
    let hexStr = res.getStr()
    # Parse hex to float (simple implementation, in production use BigInt)
    # hexStr is likely "0x..."
    if hexStr == "0x": return 0.0
    let wei = parseHexInt(hexStr).float
    return wei / 1e18 # 18 decimals for native assets usually
  else:
    # ERC-20 Token (USDC, etc.)
    # Function signature for balanceOf(address): 70a08231
    # Pad address to 32 bytes
    let strippedAddr = if address.startsWith("0x"): address[2..^1] else: address
    let paddedAddr = repeat("0", 24) & strippedAddr
    let data = "0x70a08231" & paddedAddr
    
    let callParam = %*{
      "to": asset.contract,
      "data": data
    }
    let res = await self.rpcCall("eth_call", %*[callParam, "latest"])
    let hexVal = res.getStr()
    if hexVal == "0x": return 0.0
    # Handle BigInt via string parsing if needed, for MVP float is okay for display
    # But strictly, we should handle large integers.
    # Assuming standard 18 decimals or asset.decimals
    try:
      let rawVal = parseHexInt(hexVal).float
      let divisor = pow(10.0, asset.decimals.float)
      return rawVal / divisor
    except:
      return 0.0

method sendTransaction*(self: EvmClient, signedTx: string): Future[string] {.async.} =
  let res = await self.rpcCall("eth_sendRawTransaction", %*[signedTx])
  return res.getStr()

method getTransactionStatus*(self: EvmClient, txHash: string): Future[SettlementStatus] {.async.} =
  let res = await self.rpcCall("eth_getTransactionReceipt", %*[txHash])
  if res.kind == JNull:
    return ssSubmitted # Not mined yet
  
  let statusHex = res["status"].getStr() # 0x1 (success) or 0x0 (fail)
  if statusHex == "0x1":
    return ssCompleted
  else:
    return ssFailed

method estimateGas*(self: EvmClient, fromAddr, toAddr: string, amount: float, data: string = ""): Future[float] {.async.} =
  # Basic estimation
  let params = %*{
    "from": fromAddr,
    "to": toAddr,
    "data": if data.len > 0: data else: "0x"
  }
  # If sending value, add it (hex wei)
  if amount > 0:
    # Conversion needed... skipping for MVP simplicity, usually gas est doesn't strictly need value for simple transfers
    discard

  let res = await self.rpcCall("eth_estimateGas", %*[params])
  let hexGas = res.getStr()
  return parseHexInt(hexGas).float

