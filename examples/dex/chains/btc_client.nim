import chronos, json, strutils, base64
import stint, stew/byteutils
import chronos/apps/http/httpclient
import ./types

type
  BtcClient* = ref object of ChainClient
    rpcUrl*: string
    auth*: string

proc basicAuth(user, password: string): string =
  if user.len == 0 and password.len == 0:
    return ""
  "Basic " & base64.encode(user & ":" & password)

proc newBtcClient*(rpcUrl, user, password: string): BtcClient =
  new(result)
  result.chainType = ctBtc
  result.rpcUrl = rpcUrl
  result.auth = basicAuth(user, password)
  result.httpSession = HttpSessionRef.new()

proc getBalance*(client: BtcClient, address: string = "*"): Future[string] {.async.} =
  # If address is *, get wallet balance
  let res = await client.rpcCall(client.rpcUrl, "getbalance", newJArray())
  let btcFloat = res.getFloat()
  # 1 BTC = 10^8 Sats
  let sats = uint64(btcFloat * 1e8)
  return $sats

proc sendTransaction*(client: BtcClient, signedTxHex: string): Future[string] {.async.} =
  var params = newJArray()
  params.add(%signedTxHex)
  let res = await client.rpcCall(client.rpcUrl, "sendrawtransaction", params)
  return res.getStr()

proc verifyTransaction*(client: BtcClient, txId: string): Future[bool] {.async.} =
  var params = newJArray()
  params.add(%txId)
  params.add(%1) # verbose
  
  try:
    let res = await client.rpcCall(client.rpcUrl, "getrawtransaction", params)
    if res.hasKey("confirmations"):
       return res["confirmations"].getInt() > 0
    return false
  except:
    return false
