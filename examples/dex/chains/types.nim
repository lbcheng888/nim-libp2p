import chronos, json, options
import chronos/apps/http/httpclient
import stint

type
  ChainType* = enum
    ctBtc, ctEth, ctBsc, ctSol, ctTron

  ChainClient* = ref object of RootObj
    chainType*: ChainType
    httpSession*: HttpSessionRef
    
  # Abstract methods to be overridden
  
  # Fetch balance - returns string (BigInt representation in smallest unit)
  
  RpcError* = object of CatchableError

# Helper for RPC - Generic robust implementation
proc stringToBytes(s: string): seq[byte] =
  result = newSeq[byte](s.len)
  for i in 0..<s.len:
    result[i] = s[i].byte

proc bytesToString(bytes: seq[byte]): string =
  result = newString(bytes.len)
  for i in 0..<bytes.len:
    result[i] = bytes[i].char

proc rpcCall*(client: ChainClient, url: string, methodName: string, params: JsonNode, headers: seq[(string, string)] = @[]): Future[JsonNode] {.async.} =
  try:
    var payload = newJObject()
    payload["jsonrpc"] = %"2.0"
    payload["id"] = %1
    payload["method"] = %methodName
    payload["params"] = params
    
    let body = $payload
    var finalHeaders = @[("Content-Type", "application/json")]
    for h in headers:
      finalHeaders.add(h)
    
    # Add retry logic for production grade
    var attempts = 0
    while attempts < 3:
      attempts.inc()
      try:
        let reqRes = HttpClientRequestRef.post(client.httpSession, url, body = stringToBytes(body), headers = finalHeaders)
        if reqRes.isErr:
           if attempts == 3: raise newException(RpcError, "Req Error: " & reqRes.error)
           await sleepAsync(100.millis)
           continue
           
        let req = reqRes.get()
        let response = await req.send()
        
        if response.status >= 400:
           let body = bytesToString(await response.getBodyBytes())
           raise newException(RpcError, "HTTP " & $response.status & ": " & body)
        
        let respBytes = await response.getBodyBytes()
        let respText = bytesToString(respBytes)
        let node = parseJson(respText)
        
        if node.hasKey("error"):
          raise newException(RpcError, $node["error"])
          
        return node["result"]
      except RpcError as e:
        raise e
      except CatchableError as e:
        if attempts == 3: raise newException(RpcError, "RPC Failed after retries: " & e.msg)
        await sleepAsync(500.millis)
        
  except Exception as e:
    raise newException(RpcError, "RPC Call Fatal: " & e.msg)
