## Simple BTC Testnet RPC connectivity check.
## Usage:
##   nim r examples/dex/tools/btc_rpc_check.nim
##   BTC_RPC_URL=http://149.28.194.117:18332 RPC_USER=xxx RPC_PASSWORD=yyy nim r ...

import std/[json, os, base64]
import chronos
import chronos/apps/http/httpclient

proc envOr(name, default: string): string =
  let value = getEnv(name)
  if value.len == 0: default else: value

proc basicAuthHeader(user, password: string): string =
  if user.len == 0 and password.len == 0:
    return ""
  "Basic " & base64.encode(user & ":" & password)

type RpcError = object of CatchableError
proc rpcCall(client: HttpClientRef; url, auth, methodName: string; params: JsonNode = newJArray()): Future[JsonNode] {.async.}
proc runChecks() {.async.}

proc rpcCall(client: HttpClientRef; url, auth, methodName: string; params: JsonNode): Future[JsonNode] {.async.} =
  var payload = newJObject()
  payload["jsonrpc"] = %"2.0"
  payload["id"] = %1
  payload["method"] = %methodName
  payload["params"] = params
  let body = $payload
  var headers = HttpHeaders.new()
  headers.add("Content-Type", "application/json")
  if auth.len > 0:
    headers.add("Authorization", auth)
  let response = await client.post(url, body = body, headers = headers)
  if response.code != Http200:
    raise newException(RpcError, "HTTP " & $response.code & " -> " & await response.body)
  let respText = await response.body
  let node = parseJson(respText)
  if node.hasKey("error") and node["error"].kind == JObject and node["error"].contains("message"):
    let errMsg = node["error"]["message"].getStr()
    if errMsg.len > 0:
      raise newException(RpcError, errMsg)
  result = node["result"]

proc runChecks() {.async.} =
  let rpcUrl = envOr("BTC_RPC_URL", "http://149.28.194.117:18332")
  let rpcUser = envOr("RPC_USER", "")
  let rpcPass = envOr("RPC_PASSWORD", "")
  if rpcUrl.len == 0:
    echo "[rpc] BTC_RPC_URL missing"
    return
  let auth = basicAuthHeader(rpcUser, rpcPass)
  var client = newHttpClient()
  defer: client.close()
  echo "[rpc] dialing ", rpcUrl
  let info = await rpcCall(client, rpcUrl, auth, "getblockchaininfo", newJArray())
  let blocks = info["blocks"].getInt()
  let headers = info["headers"].getInt()
  let chain = info["chain"].getStr()
  echo &"[rpc] chain={chain} blocks={blocks} headers={headers}"
  let feeParam = newJArray()
  feeParam.add(%6)
  let feeRes = await rpcCall(client, rpcUrl, auth, "estimatesmartfee", feeParam)
  if not feeRes.isNil():
    if feeRes.hasKey("feerate"):
      echo &"[rpc] feerate={feeRes[\"feerate\"].getFloat():.8f}"
    else:
      echo "[rpc] fee estimate result: ", $feeRes
  else:
    echo "[rpc] no fee estimate"
  echo "[rpc] connectivity check succeeded"

when isMainModule:
  try:
    waitFor(runChecks())
  except CatchableError as exc:
    echo "[rpc] connectivity check failed: ", exc.msg
