## Minimal HTTP signaling stub for WebRTC Direct integration tests.
##
## This utility avoids external dependencies and allows the libp2p WebRTC
## transport to exercise the offer/answer flow against a deterministic
## responder. It does **not** establish a real data channel — instead it
## echoes back a canned SDP answer so that higher level logic (HTTP wiring,
## state transitions, metrics) can be validated without libdatachannel.

import std/[json, strformat, strutils, tables]
import std/asynchttpserver
import std/asyncdispatch

const
  DefaultAnswer = """{"type":"answer","sdp":"v=0\no=- 0 0 IN IP4 127.0.0.1\ns=libp2p-webrtc-direct-test\n"}"""

type
  OfferRegistry = Table[string, string]

var offers: OfferRegistry

proc respond(req: Request, code: HttpCode, body: string) {.async.} =
  await req.respond(code, body, newHttpHeaders({"Content-Type": "application/json"}))

proc handleOffer(req: Request) {.async.} =
  let payload = await req.body
  let id = $(offers.len + 1)
  offers[id] = payload
  let response = %*{
    "offerId": id,
    "answer": DefaultAnswer
  }
  await respond(req, Http200, $response)

proc handleAnswer(req: Request) {.async.} =
  let payload = await req.body
  try:
    let parsed = parseJson($payload)
    let offerId = parsed["offerId"].getStr()
    if offerId notin offers:
      await respond(req, Http400, """{"error":"unknown offerId"}""")
      return
    offers[offerId] = payload
    await respond(req, Http200, """{"status":"accepted"}""")
  except CatchableError:
    await respond(req, Http400, """{"error":"invalid JSON"}""")

proc handler(req: Request) {.async.} =
  try:
    if req.reqMethod in {HttpPost}:
      case req.url.path
      of "/offer":
        await handleOffer(req)
      of "/answer":
        await handleAnswer(req)
      else:
        await respond(req, Http404, """{"error":"not found"}""")
    else:
      await respond(req, Http405, """{"error":"method not allowed"}""")
  except CatchableError as exc:
    await respond(
      req,
      Http500,
      fmt"""{{"error":"internal","message":"{exc.msg.escapeJson()}"}""",
    )

when isMainModule:
  let server = newAsyncHttpServer()
  let port = Port(9777)
  echo &"WebRTC Direct signaling stub listening on http://127.0.0.1:{port}/offer"
  waitFor server.serve(Port(port), handler)
