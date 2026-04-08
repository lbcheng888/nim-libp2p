import std/[json, jsonutils, options, strutils]

import chronos/apps/http/httpserver

import ../../rwad/codec
import ../../rwad/types
import ../adapter
import ../node

type
  FabricRpcServer* = ref object
    node*: FabricNode
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

proc txPayloadNode(payload: JsonNode): JsonNode =
  if payload.kind == JObject and payload.hasKey("payload"):
    payload["payload"]
  else:
    newJObject()

proc rpcSubmitResult(
    server: FabricRpcServer, payload: JsonNode
): JsonNode =
  if server.isNil or server.node.isNil:
    raise newException(ValueError, "fabric node missing")
  let localTx = server.node.prepareLocalTx(
    kind = jTxKind(payload, "kind"),
    payload = txPayloadNode(payload),
    epoch = uint64(jInt(payload, "epoch")),
    timestamp = jI64(payload, "timestamp"),
  )
  let offeredNonce = jInt(payload, "nonce", -1)
  if offeredNonce >= 0 and uint64(offeredNonce) != localTx.nonce:
    raise newException(
      ValueError,
      "submit nonce mismatch: expected " & $localTx.nonce & " got " & $offeredNonce,
    )
  let offeredSender = jStr(payload, "sender")
  if offeredSender.len > 0 and offeredSender != localTx.sender:
    raise newException(
      ValueError,
      "submit sender mismatch: expected " & localTx.sender & " got " & offeredSender,
    )
  let offeredPub = jStr(payload, "senderPublicKey")
  if offeredPub.len > 0 and offeredPub != localTx.senderPublicKey:
    raise newException(ValueError, "submit senderPublicKey mismatch")
  let offeredSig = jStr(payload, "signature")
  if offeredSig.len > 0 and offeredSig != localTx.signature:
    raise newException(ValueError, "submit signature mismatch")
  let offeredTxId = jStr(payload, "txId")
  if offeredTxId.len > 0 and offeredTxId != localTx.txId:
    raise newException(
      ValueError,
      "submit txId mismatch: expected " & localTx.txId & " got " & offeredTxId,
    )
  toJson(server.node.submitTx(localTx))

proc dispatch(server: FabricRpcServer, methodName: string, params: JsonNode): JsonNode =
  case methodName
  of "fabric.status":
    toJson(server.node.fabricStatus())
  of "fabric.quiescence_snapshot":
    toJson(server.node.quiescenceSnapshot())
  of "fabric.quiesce":
    toJson(server.node.submitFence())
  of "fabric.submit_event":
    server.rpcSubmitResult(params["tx"])
  of "fabric.get_event":
    let eventOpt = server.node.getEvent(jStr(params, "eventId"))
    if eventOpt.isSome(): toJson(eventOpt.get()) else: newJNull()
  of "fabric.get_event_certificate":
    let certOpt = server.node.getEventCertificate(jStr(params, "eventId"))
    if certOpt.isSome(): toJson(certOpt.get()) else: newJNull()
  of "fabric.get_checkpoint":
    let checkpointOpt = server.node.getCheckpoint(jStr(params, "checkpointId"))
    if checkpointOpt.isSome(): toJson(checkpointOpt.get()) else: newJNull()
  of "fabric.query_projection":
    server.node.queryProjection(jStr(params, "projection", "state"), jStr(params, "checkpointId"))
  of "fabric.get_contract_root":
    let kind = jTxKind(params, "kind")
    let item = getContractRoot(kind)
    %*{"contractRoot": item.contractRoot, "entrypoint": item.entrypoint}
  of "fabric.list_avo_proposals":
    toJson(server.node.listAvoProposals())
  of "social.publish_tasks":
    toJson(server.node.publishTasks(jStr(params, "checkpointId")))
  of "social.feed_snapshot":
    toJson(server.node.feedSnapshot(jInt(params, "limit", 50), jStr(params, "checkpointId")))
  of "social.content_detail":
    let item = server.node.contentDetail(jStr(params, "contentId"), jStr(params, "checkpointId"))
    if item.isSome(): toJson(item.get()) else: newJNull()
  of "governance.report_list":
    toJson(server.node.reportList(jStr(params, "contentId"), jStr(params, "checkpointId")))
  of "credit.balance":
    let item = server.node.creditBalance(jStr(params, "owner"), jStr(params, "checkpointId"))
    if item.isSome(): toJson(item.get()) else: newJNull()
  of "treasury.intent_status":
    let item = server.node.mintIntentStatus(jStr(params, "mintIntentId"), jStr(params, "checkpointId"))
    if item.isSome(): toJson(item.get()) else: newJNull()
  of "name.resolve":
    let item = server.node.resolveName(jStr(params, "name"), jI64(params, "nowTs"), jStr(params, "checkpointId"))
    if item.isSome(): toJson(item.get()) else: newJNull()
  else:
    raise newException(ValueError, "unknown method: " & methodName)

proc newFabricRpcServer*(node: FabricNode, host: string, port: int): FabricRpcServer =
  FabricRpcServer(node: node, host: host, port: port, http: nil)

proc start*(server: FabricRpcServer) {.async.} =
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
      try:
        var payload: JsonNode
        {.cast(gcsafe).}:
          payload = toJson(server.node.fabricStatus())
        return await reply(req, $payload)
      except CatchableError as exc:
        return await reply(req, $rpcError(newJNull(), -32000, exc.msg), Http500)
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

proc stop*(server: FabricRpcServer) {.async.} =
  if not server.http.isNil:
    await server.http.closeWait()
