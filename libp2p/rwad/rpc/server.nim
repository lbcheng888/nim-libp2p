import std/[base64, json, jsonutils, options, strutils]

import chronos/apps/http/httpserver

import ../codec
import ../identity
import ../node
import ../types

type
  RpcServer* = ref object
    node*: RwadNode
    host*: string
    port*: int
    http*: HttpServerRef

proc stringOf(data: openArray[byte]): string =
  result = newString(data.len)
  if data.len > 0:
    copyMem(addr result[0], unsafeAddr data[0], data.len)

proc rpcResult(id: JsonNode, payload: JsonNode): JsonNode =
  %*{
    "jsonrpc": "2.0",
    "id": id,
    "result": payload,
  }

proc rpcError(id: JsonNode, code: int, message: string): JsonNode =
  %*{
    "jsonrpc": "2.0",
    "id": id,
    "error": {
      "code": code,
      "message": message,
    },
  }

proc jStr(payload: JsonNode, key: string, default = ""): string =
  if payload.kind == JObject and payload.hasKey(key):
    payload[key].getStr(default)
  else:
    default

proc jInt(payload: JsonNode, key: string, default = 0): int =
  if payload.kind == JObject and payload.hasKey(key):
    payload[key].getInt(default).int
  else:
    default

proc jI64(payload: JsonNode, key: string, default = 0'i64): int64 =
  if payload.kind == JObject and payload.hasKey(key):
    payload[key].getInt(default).int64
  else:
    default

proc jStringSeq(payload: JsonNode, key: string): seq[string] =
  if payload.kind == JObject and payload.hasKey(key) and payload[key].kind == JArray:
    for item in payload[key].items:
      result.add(item.getStr())

proc jTxKind(payload: JsonNode, key: string): TxKind =
  if payload.kind != JObject or not payload.hasKey(key):
    raise newException(ValueError, "missing tx kind")
  let node = payload[key]
  case node.kind
  of JInt:
    let value = node.getInt()
    if value < ord(low(TxKind)) or value > ord(high(TxKind)):
      raise newException(ValueError, "unknown tx kind")
    TxKind(value)
  of JString:
    let raw = node.getStr()
    let kindOpt = parseTxKind(raw)
    if kindOpt.isSome():
      return kindOpt.get()
    try:
      parseEnum[TxKind](raw)
    except ValueError:
      raise newException(ValueError, "unknown tx kind")
  else:
    raise newException(ValueError, "invalid tx kind")

proc txFromJson(payload: JsonNode): Tx =
  result = Tx(
    txId: jStr(payload, "txId"),
    kind: jTxKind(payload, "kind"),
    sender: jStr(payload, "sender"),
    senderPublicKey: jStr(payload, "senderPublicKey"),
    nonce: uint64(jInt(payload, "nonce")),
    epoch: uint64(jInt(payload, "epoch")),
    timestamp: jI64(payload, "timestamp"),
    payload: if payload.kind == JObject and payload.hasKey("payload"): payload["payload"] else: newJObject(),
    signature: jStr(payload, "signature"),
  )
  if result.txId.len == 0:
    result.txId = computeTxId(result)

proc dispatch(server: RpcServer, methodName: string, params: JsonNode): JsonNode =
  case methodName
  of "chain.status":
    toJson(server.node.chainStatus())
  of "chain.submit_tx":
    toJson(server.node.submitTx(txFromJson(params["tx"])))
  of "chain.get_block":
    let blockOpt = server.node.getBlock(uint64(jInt(params, "height")))
    if blockOpt.isSome(): toJson(blockOpt.get()) else: newJNull()
  of "consensus.status":
    toJson(server.node.consensusStatus())
  of "consensus.get_batch":
    let batchOpt = server.node.getBatch(jStr(params, "batchId"))
    if batchOpt.isSome(): toJson(batchOpt.get()) else: newJNull()
  of "consensus.get_certificate":
    let certOpt = server.node.getCertificate(jStr(params, "certificateId"))
    if certOpt.isSome(): toJson(certOpt.get()) else: newJNull()
  of "social.publish_tasks":
    toJson(server.node.publishTasks())
  of "social.feed_snapshot":
    toJson(server.node.feedSnapshot(jInt(params, "limit", 50)))
  of "social.content_detail":
    toJson(server.node.contentDetail(jStr(params, "contentId")))
  of "social.store_blob":
    let manifest = jsonTo(params["manifest"], ContentManifest)
    server.node.storeBlob(manifest, jStringSeq(params, "chunksBase64"))
    %*{"ok": true, "manifestCid": manifest.manifestCid}
  of "social.fetch_blob_head":
    let manifest = server.node.blobHead(
      manifestCid = jStr(params, "manifestCid"),
      reader = jStr(params, "reader"),
      ticketId = jStr(params, "ticketId"),
      nowTs = jI64(params, "nowTs"),
    )
    if manifest.isSome(): toJson(manifest.get()) else: newJNull()
  of "social.fetch_chunk":
    let data = server.node.blobChunkBase64(
      manifestCid = jStr(params, "manifestCid"),
      index = jInt(params, "index"),
      reader = jStr(params, "reader"),
      ticketId = jStr(params, "ticketId"),
      nowTs = jI64(params, "nowTs"),
    )
    if data.isSome(): %*{"dataBase64": data.get()} else: newJNull()
  of "social.fetch_range":
    let data = server.node.blobRangeBase64(
      manifestCid = jStr(params, "manifestCid"),
      startPos = jInt(params, "start"),
      endPos = jInt(params, "end"),
      reader = jStr(params, "reader"),
      ticketId = jStr(params, "ticketId"),
      nowTs = jI64(params, "nowTs"),
    )
    if data.isSome(): %*{"dataBase64": data.get()} else: newJNull()
  of "governance.report_list":
    toJson(server.node.reportList(jStr(params, "contentId")))
  of "credit.balance":
    toJson(server.node.creditBalance(jStr(params, "owner")))
  of "treasury.intent_status":
    toJson(server.node.mintIntentStatus(jStr(params, "mintIntentId")))
  of "name.resolve":
    let item = server.node.resolveName(jStr(params, "name"), jI64(params, "nowTs"))
    if item.isSome(): toJson(item.get()) else: newJNull()
  else:
    raise newException(ValueError, "unknown method: " & methodName)

proc newRpcServer*(node: RwadNode, host: string, port: int): RpcServer =
  RpcServer(node: node, host: host, port: port, http: nil)

proc start*(server: RpcServer) {.async.} =
  proc reply(req: HttpRequestRef, body: string, code = Http200): Future[HttpResponseRef] {.async: (raises: [CancelledError]).} =
    try:
      return await req.respond(code, body)
    except HttpWriteError as exc:
      return defaultResponse(exc)

  proc handler(reqFence: RequestFence): Future[HttpResponseRef] {.async: (raises: [CancelledError]).} =
    if reqFence.isErr():
      return defaultResponse(reqFence.error())
    let req = reqFence.get()
    if req.meth == MethodGet and req.uri.path == "/healthz":
      return await reply(req, $toJson(server.node.chainStatus()))
    if req.meth != MethodPost:
      return await reply(req, $rpcError(newJNull(), -32600, "method not allowed"), Http405)
    try:
      let body = stringOf(await req.getBody())
      let rpcReq = parseJson(body)
      let id = if rpcReq.kind == JObject and rpcReq.hasKey("id"): rpcReq["id"] else: newJNull()
      let params =
        if rpcReq.kind == JObject and rpcReq.hasKey("params"): rpcReq["params"]
        else: newJObject()
      var payload: JsonNode
      {.cast(gcsafe).}:
        payload = server.dispatch(jStr(rpcReq, "method"), params)
      return await reply(req, $rpcResult(id, payload))
    except Exception as exc:
      return await reply(req, $rpcError(newJNull(), -32000, exc.msg))

  let address = initTAddress(server.host, Port(server.port))
  let httpRes = HttpServerRef.new(address, handler)
  if httpRes.isErr():
    raise newException(ValueError, $httpRes.error)
  server.http = httpRes.get()
  server.http.start()

proc stop*(server: RpcServer) {.async.} =
  if not server.http.isNil:
    await server.http.closeWait()
