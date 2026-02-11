{.used.}

# Nim-LibP2P

import unittest2
import std/[tables, strutils, sequtils, assertions]
import chronos

import ../libp2p/protocols/http/http
import ../libp2p/stream/bridgestream
import ../libp2p/protocols/protocol
import ./utils/async_tests

proc strToBytes(s: string): seq[byte] =
  s.toSeq().mapIt(byte(it))

suite "http config":
  test "default http config values":
    let cfg = HttpConfig.init()
    check cfg.maxRequestBytes == DefaultHttpBodyLimit
    check cfg.maxResponseBytes == DefaultHttpBodyLimit
    check cfg.handlerTimeout == 10.seconds

  test "custom http config overrides":
    let cfg = HttpConfig.init(
      maxRequestBytes = 4096, maxResponseBytes = 8192, handlerTimeout = 5.seconds
    )
    check cfg.maxRequestBytes == 4096
    check cfg.maxResponseBytes == 8192
    check cfg.handlerTimeout == 5.seconds

  test "http config validation":
    proc okHandler(req: HttpRequest): Future[HttpResponse] {.async.} =
      HttpResponse(status: 200, headers: initTable[string, string](), body: @[])

    let badCfg = HttpConfig(maxRequestBytes: -1, maxResponseBytes: DefaultHttpBodyLimit, handlerTimeout: 10.seconds)
    doAssertRaises(Defect):
      discard HttpService.new(okHandler, badCfg)

  asyncTest "http request payload limit returns 413":
    proc okHandler(req: HttpRequest): Future[HttpResponse] {.async.} =
      HttpResponse(status: 200, headers: initTable[string, string](), body: @[])

    let cfg = HttpConfig.init(maxRequestBytes = 8, maxResponseBytes = 64)
    let svc = HttpService.new(okHandler, cfg)
    let (client, server) = bridgedConnections(closeTogether = false, dirA = Direction.Out, dirB = Direction.In)
    let serviceHandler = svc.handler
    let serverFut = serviceHandler(server, HttpCodec)
    asyncSpawn serverFut

    let payload = "POST / HTTP/1.1\r\ncontent-length: 12\r\nconnection: close\r\n\r\nHello World!"
    await client.write(payload.strToBytes())

    let statusLine = strip(await client.readLine(limit = 8 * 1024))
    check statusLine.splitWhitespace()[1] == "413"

    await client.close()
    await serverFut

  asyncTest "http response payload limit returns 500":
    proc oversizedHandler(req: HttpRequest): Future[HttpResponse] {.async.} =
      HttpResponse(
        status: 200,
        headers: initTable[string, string](),
        body: newSeq[byte](32),
      )

    let cfg = HttpConfig.init(maxRequestBytes = 64, maxResponseBytes = 16)
    let svc = HttpService.new(oversizedHandler, cfg)
    let (client, server) = bridgedConnections(closeTogether = false, dirA = Direction.Out, dirB = Direction.In)
    let serviceHandler = svc.handler
    let serverFut = serviceHandler(server, HttpCodec)
    asyncSpawn serverFut

    let request = "GET / HTTP/1.1\r\ncontent-length: 0\r\nconnection: close\r\n\r\n"
    await client.write(request.strToBytes())

    let statusLine = strip(await client.readLine(limit = 8 * 1024))
    check statusLine.splitWhitespace()[1] == "500"

    await client.close()
    await serverFut

  asyncTest "http handler timeout returns 504":
    proc slowHandler(req: HttpRequest): Future[HttpResponse] {.async.} =
      await sleepAsync(200.milliseconds)
      HttpResponse(status: 200, headers: initTable[string, string](), body: @[])

    let cfg = HttpConfig.init(
      maxRequestBytes = 64, maxResponseBytes = 64, handlerTimeout = 50.milliseconds
    )
    let svc = HttpService.new(slowHandler, cfg)
    let (client, server) = bridgedConnections(closeTogether = false, dirA = Direction.Out, dirB = Direction.In)
    let serviceHandler = svc.handler
    let serverFut = serviceHandler(server, HttpCodec)
    asyncSpawn serverFut

    let request = "GET /slow HTTP/1.1\r\ncontent-length: 0\r\nconnection: close\r\n\r\n"
    await client.write(request.strToBytes())

    let statusLine = strip(await client.readLine(limit = 8 * 1024))
    check statusLine.splitWhitespace()[1] == "504"

    await client.close()
    await serverFut
