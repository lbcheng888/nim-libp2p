import chronos, json, strutils, strformat
import stew/byteutils
import chronos/apps/http/httpclient
import ./types

type
  TronClient* = ref object of ChainClient
    apiUrl*: string # e.g. https://api.trongrid.io

proc newTronClient*(apiUrl: string): TronClient =
  new(result)
  result.chainType = ctTron
  result.apiUrl = apiUrl
  result.httpSession = HttpSessionRef.new()

# Helper for POST since Tron uses REST mostly
proc postCall(client: TronClient, path: string, payload: JsonNode): Future[JsonNode] {.async.} =
  let url = client.apiUrl & path
  let body = $payload
  var headers = @[("Content-Type", "application/json")]
  
  # Simple retry wrapper
  var attempts = 0
  while attempts < 3:
    attempts.inc()
    try:
      let reqRes = HttpClientRequestRef.post(client.httpSession, url, body = body.toBytes(), headers = headers)
      if reqRes.isErr:
         if attempts == 3: raise newException(RpcError, "Req Error: " & reqRes.error)
         await sleepAsync(100.millis)
         continue
         
      let req = reqRes.get()
      let response = await req.send()
      
      let respBytes = await response.getBodyBytes()
      let respText = string.fromBytes(respBytes)
      return parseJson(respText)
    except CatchableError as e:
      if attempts == 3: raise e
      await sleepAsync(500.millis)

proc getBalance*(client: TronClient, address: string): Future[string] {.async.} =
  var payload = newJObject()
  payload["address"] = %address
  payload["visible"] = %true
  
  let res = await client.postCall("/wallet/getaccount", payload)
  var val = 0.int64
  if res.hasKey("balance"):
    val = res["balance"].getBiggestInt()
  return $val

# TRC20 Support
proc padAddress(addrStr: string): string =
  let clean = if addrStr.startsWith("41"): addrStr[2..^1] else: addrStr # 41 is Mainnet prefix
  "0".repeat(64 - clean.len) & clean

proc getTokenBalance*(client: TronClient, contractAddr, ownerAddrHex: string): Future[string] {.async.} =
  var payload = newJObject()
  payload["owner_address"] = %ownerAddrHex
  payload["contract_address"] = %contractAddr
  payload["function_selector"] = %"balanceOf(address)"
  # Parameter is address
  payload["parameter"] = %padAddress(ownerAddrHex)
  payload["visible"] = %false
  
  let res = await client.postCall("/wallet/triggerconstantcontract", payload)
  if res.hasKey("constant_result") and res["constant_result"].len > 0:
     return res["constant_result"][0].getStr()
  return "0"

proc broadcastTransaction*(client: TronClient, signedTx: JsonNode): Future[string] {.async.} =
  let res = await client.postCall("/wallet/broadcasttransaction", signedTx)
  if res.hasKey("result") and res["result"].getBool():
     return res["txid"].getStr() 
  if res.hasKey("txid"): return res["txid"].getStr()
  raise newException(RpcError, "Broadcast failed: " & $res)

proc verifyTransaction*(client: TronClient, txId: string): Future[bool] {.async.} =
  var payload = newJObject()
  payload["value"] = %txId
  
  try:
    let res = await client.postCall("/wallet/gettransactioninfobyid", payload)
    if res.hasKey("receipt") and res["receipt"].hasKey("result"):
       return res["receipt"]["result"].getStr() == "SUCCESS"
    return false
  except:
    return false
