import chronos, json, strutils, strformat
import stew/byteutils
import chronos/apps/http/httpclient
import ./types

type
  EvmClient* = ref object of ChainClient
    rpcUrl*: string
    chainId*: int

proc newEvmClient*(chain: ChainType, rpcUrl: string, chainId: int): EvmClient =
  new(result)
  result.chainType = chain
  result.rpcUrl = rpcUrl
  result.chainId = chainId
  result.httpSession = HttpSessionRef.new()

proc getBalance*(client: EvmClient, address: string): Future[string] {.async.} =
  var params = newJArray()
  params.add(%address)
  params.add(%"latest")
  
  let res = await client.rpcCall(client.rpcUrl, "eth_getBalance", params)
  # hex string
  return res.getStr()

# ERC20 support
const ERC20_TRANSFER_ID = "a9059cbb"
const ERC20_BALANCE_OF_ID = "70a08231"

proc padAddress(addrStr: string): string =
  let clean = if addrStr.startsWith("0x"): addrStr[2..^1] else: addrStr
  "0".repeat(64 - clean.len) & clean

proc getTokenBalance*(client: EvmClient, tokenAddr, ownerAddr: string): Future[string] {.async.} =
  var callObj = newJObject()
  callObj["to"] = %tokenAddr
  callObj["data"] = %("0x" & ERC20_BALANCE_OF_ID & padAddress(ownerAddr))
  
  var params = newJArray()
  params.add(callObj)
  params.add(%"latest")
  
  let res = await client.rpcCall(client.rpcUrl, "eth_call", params)
  return res.getStr()

proc sendTransaction*(client: EvmClient, signedTx: string): Future[string] {.async.} =
  var params = newJArray()
  params.add(%signedTx)
  
  let res = await client.rpcCall(client.rpcUrl, "eth_sendRawTransaction", params)
  return res.getStr()

proc verifyTransaction*(client: EvmClient, txId: string): Future[bool] {.async.} =
  var params = newJArray()
  params.add(%txId)
  
  try:
    let res = await client.rpcCall(client.rpcUrl, "eth_getTransactionReceipt", params)
    if res.kind == JNull: return false
    
    if res.hasKey("status"):
      let statusHex = res["status"].getStr()
      return statusHex == "0x1"
    return false
  except:
    return false
