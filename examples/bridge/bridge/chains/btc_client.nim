import std/[httpclient, json, strformat]
import stew/base64

type
  BtcRpcClient* = object
    url*: string
    auth*: string
    http*: HttpClient

proc basicAuth(user, password: string): string =
  if user.len == 0 and password.len == 0:
    return ""
  "Basic " & base64.encode(user & ":" & password)

proc newBtcRpcClient*(url, user, password: string): BtcRpcClient =
  result.url = url
  result.auth = basicAuth(user, password)
  result.http = newHttpClient()

proc rpcCall(client: var BtcRpcClient; method: string; params: JsonNode): JsonNode =
  var payload = newJObject()
  payload["jsonrpc"] = %"1.0"
  payload["id"] = %1
  payload["method"] = %method
  payload["params"] = params
  var headers = newHttpHeaders({"Content-Type": "application/json"})
  if client.auth.len > 0:
    headers.add("Authorization", client.auth)
  let response = client.http.request(
    client.url,
    httpMethod = HttpPost,
    headers = headers,
    body = $payload
  )
  if response.code != Http200:
    raise newException(IOError, "[btc-rpc] HTTP " & $response.code & ": " & response.body)
  let respText = response.body
  let node = parseJson(respText)
  if node.hasKey("error") and node["error"].kind == JObject and node["error"].hasKey("message"):
    let msg = node["error"]["message"].getStr()
    if msg.len > 0:
      raise newException(IOError, "[btc-rpc] " & msg)
  node["result"]

proc sendToAddress*(client: var BtcRpcClient; address: string; amountBtc: float): string =
  var params = newJArray()
  params.add(%address)
  params.add(%amountBtc)
  let result = client.rpcCall("sendtoaddress", params)
  result.getStr()
