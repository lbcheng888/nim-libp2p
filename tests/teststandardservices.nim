{.used.}

# Nim-LibP2P

import unittest2
import std/[tables, sequtils]
import chronos

import ../libp2p/[builders, switch]
import ../libp2p/protocols/fetch/fetch
import ../libp2p/protocols/fetch/protobuf
import ../libp2p/protocols/http/http
import ./utils/async_tests

proc sbytes(s: string): seq[byte] =
  s.toSeq().mapIt(byte(it))

when not defined(libp2p_run_standard_service_tests):
  suite "standard switch helper services":
    test "standard switch helper service tests disabled":
      skip()
else:
  suite "standard switch helper services":
    asyncTest "newStandardSwitch with fetch handler responds":
      proc fetchHandler(key: string): Future[FetchResponse] {.async.} =
        if key == "hello":
          FetchResponse(status: FetchStatus.fsOk, data: sbytes("world"))
        else:
          FetchResponse(status: FetchStatus.fsNotFound, data: @[])

      let fetchHandlerCb: FetchHandler = fetchHandler
      let server = newStandardSwitch(
        fetchHandler = Opt.some(fetchHandlerCb),
        fetchConfig = FetchConfig.init(maxResponseBytes = 1024),
      )
      let client = newStandardSwitch()
      await server.start()
      await client.start()
      defer:
        await client.stop()
        await server.stop()

      await client.connect(server.peerInfo.peerId, server.peerInfo.addrs)

      let resp = await fetch(client, server.peerInfo.peerId, "hello")
      check resp.status == FetchStatus.fsOk
      check resp.data == sbytes("world")

    asyncTest "newStandardSwitch with http handler responds":
      proc defaultHandler(req: HttpRequest): Future[HttpResponse] {.async.} =
        HttpResponse(
          status: 200,
          headers: {"content-type": "text/plain"}.toTable(),
          body: sbytes("nim-libp2p"),
        )

      let httpHandlerCb: HttpHandler = defaultHandler
      let server = newStandardSwitch(
        httpDefaultHandler = Opt.some(httpHandlerCb),
        httpConfig = HttpConfig.init(maxRequestBytes = 1024, maxResponseBytes = 1024),
      )
      let client = newStandardSwitch()
      await server.start()
      await client.start()
      defer:
        await client.stop()
        await server.stop()

      await client.connect(server.peerInfo.peerId, server.peerInfo.addrs)

      let request = HttpRequest(
        verb: "GET", path: "/", headers: initTable[string, string](), body: @[]
      )
      let resp =
        await httpRequest(client, server.peerInfo.peerId, request, maxResponseBytes = 1024)
      check resp.status == 200
      check resp.body == sbytes("nim-libp2p")
