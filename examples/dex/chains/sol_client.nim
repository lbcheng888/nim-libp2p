import chronos, json, strutils
import stew/byteutils
import chronos/apps/http/httpclient
import ./types

type
  SolClient* = ref object of ChainClient
    rpcUrl*: string

proc newSolClient*(rpcUrl: string): SolClient =
  new(result)
  result.chainType = ctSol
  result.rpcUrl = rpcUrl
  result.httpSession = HttpSessionRef.new()

proc getBalance*(client: SolClient, address: string): Future[string] {.async.} =
  var params = newJArray()
  params.add(%address)
  
  let res = await client.rpcCall(client.rpcUrl, "getBalance", params)
  let val = res["value"].getBiggestInt()
  return $val

# SPL Token Support
# Requires tokenProgramId (usually TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA)
proc getTokenBalance*(client: SolClient, tokenMint, ownerAddr: string): Future[string] {.async.} =
  var params = newJArray()
  params.add(%ownerAddr)
  
  var filter = newJObject()
  filter["mint"] = %tokenMint
  params.add(filter)
  
  var config = newJObject()
  config["encoding"] = %"jsonParsed"
  params.add(config)
  
  let res = await client.rpcCall(client.rpcUrl, "getTokenAccountsByOwner", params)
  let value = res["value"]
  if value.kind == JArray and value.len > 0:
    # Sum up all accounts for this mint
    # Simplified: return first
    let info = value[0]["account"]["data"]["parsed"]["info"]["tokenAmount"]
    return info["amount"].getStr()
  
  return "0"

proc sendTransaction*(client: SolClient, signedTxBase64: string): Future[string] {.async.} =
  var params = newJArray()
  params.add(%signedTxBase64)
  var config = newJObject()
  config["encoding"] = %"base64"
  params.add(config)
  
  let res = await client.rpcCall(client.rpcUrl, "sendTransaction", params)
  return res.getStr()

proc verifyTransaction*(client: SolClient, signature: string): Future[bool] {.async.} =
  var params = newJArray()
  params.add(%signature)
  
  try:
    let res = await client.rpcCall(client.rpcUrl, "getSignatureStatuses", params)
    let value = res["value"]
    if value.kind == JArray and value.len > 0:
       let status = value[0]
       if status.kind != JNull and status.hasKey("confirmationStatus"):
          let s = status["confirmationStatus"].getStr()
          return s == "confirmed" or s == "finalized"
    return false
  except:
    return false
