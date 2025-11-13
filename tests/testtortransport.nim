{.used.}

# Nim-Libp2p
# Copyright (c) 2023-2024 Status Research & Development GmbH
# Licensed under either of
#  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
#  * MIT license ([LICENSE-MIT](LICENSE-MIT))
# at your option.
# This file may not be copied, modified, or distributed except according to
# those terms.

{.push raises: [].}

import std/[net, strutils]
import tables
import chronos, stew/[byteutils]
import
  ../libp2p/[
    stream/connection,
    errors,
    transports/tortransport,
    transports/tcptransport,
    upgrademngrs/upgrade,
    multiaddress,
    builders,
  ]

import ./helpers, ./stubs/torstub, ./commontransport

const torServer = initTAddress("127.0.0.1", 9050.Port)
var stub: TorServerStub
var startFut: Future[void]

test "socks5 request parts encode onion3 port":
  let ma = MultiAddress
    .init("/onion3/vww6ybal4bd7szmgncyruucpgfkqahzddi37ktceo3ah7ngmcopnpyyd:1234")
    .tryGet()
  let (_, _, dstPort) = socks5RequestParts(ma)
  check dstPort == @[byte(0x04), byte(0xD2)]

test "dns multiaddr exceeding 255 bytes is rejected":
  let longHost = repeat('a', 256)
  let ma = MultiAddress.init("/dns/" & longHost & "/tcp/80").tryGet()
  expect(LPError):
    discard socks5RequestParts(ma)

proc pickPort(host: string = "127.0.0.1"): Port =
  var sock: Socket
  var created = false
  try:
    let isIpv6 = host.contains(":")
    sock = newSocket(
      domain = if isIpv6: Domain.AF_INET6 else: Domain.AF_INET,
      sockType = SockType.SOCK_STREAM,
      protocol = Protocol.IPPROTO_TCP,
      buffered = true,
      inheritable = false,
    )
    created = true
    sock.setSockOpt(OptReuseAddr, true)
    sock.bindAddr(Port(0), host)
    sock.listen()
    let (_, port) = sock.getLocalAddr()
    return port
  except CatchableError as exc:
    raiseAssert("failed to allocate ephemeral port: " & exc.msg)
  finally:
    if created:
      close(sock)

var
  ipv4Port: Port
  ipv6Port: Port
  onionLocalPort1: Port
  onionLocalPort2: Port
suite "Tor transport":
  setup:
    ipv4Port = pickPort()
    ipv6Port = pickPort("::1")
    onionLocalPort1 = pickPort()
    onionLocalPort2 = pickPort()
    stub = TorServerStub.new()
    stub.registerAddr(
      "127.0.0.1:" & $ipv4Port, "/ip4/127.0.0.1/tcp/" & $ipv4Port
    )
    stub.registerAddr(
      "libp2p.nim:" & $ipv4Port, "/ip4/127.0.0.1/tcp/" & $ipv4Port
    )
    stub.registerAddr("::1:" & $ipv6Port, "/ip6/::1/tcp/" & $ipv6Port)
    stub.registerAddr(
      "a2mncbqsbullu7thgm4e6zxda2xccmcgzmaq44oayhdtm6rav5vovcad.onion:80",
      "/ip4/127.0.0.1/tcp/" & $onionLocalPort1,
    )
    stub.registerAddr(
      "a2mncbqsbullu7thgm4e6zxda2xccmcgzmaq44oayhdtm6rav5vovcae.onion:81",
      "/ip4/127.0.0.1/tcp/" & $onionLocalPort2,
    )
    startFut = stub.start(torServer)
  teardown:
    waitFor startFut.cancelAndWait()
    waitFor stub.stop()
    checkTrackers()

  proc test(listenAddr: string, dialAddr: string) {.async.} =
    let server = TcpTransport.new({ReuseAddr}, Upgrade())
    let ma2 = @[MultiAddress.init(listenAddr).tryGet()]
    await server.start(ma2)

    proc runClient() {.async.} =
      let client = TorTransport.new(transportAddress = torServer, upgrade = Upgrade())
      let conn = await client.dial("", MultiAddress.init(dialAddr).tryGet())

      await conn.write("client")
      var resp: array[6, byte]
      await conn.readExactly(addr resp, 6)
      await conn.close()

      check string.fromBytes(resp) == "server"
      await client.stop()

    proc serverAcceptHandler() {.async.} =
      let conn = await server.accept()
      var resp: array[6, byte]
      await conn.readExactly(addr resp, 6)
      check string.fromBytes(resp) == "client"

      await conn.write("server")
      await conn.close()
      await server.stop()

    asyncSpawn serverAcceptHandler()
    await runClient()

  asyncTest "test start and dial using ipv4":
    let listen = "/ip4/127.0.0.1/tcp/" & $ipv4Port
    await test(listen, listen)

  asyncTest "test start and dial using ipv6":
    let listen = "/ip6/::1/tcp/" & $ipv6Port
    await test(listen, listen)

  asyncTest "test start and dial using dns":
    let listenAddr = "/ip4/127.0.0.1/tcp/" & $ipv4Port
    let dialAddr = "/dns/libp2p.nim/tcp/" & $ipv4Port
    await test(listenAddr, dialAddr)

  asyncTest "test start and dial usion onion3 and builder":
    const TestCodec = "/test/proto/1.0.0" # custom protocol string identifier

    type TestProto = ref object of LPProtocol # declare a custom protocol

    proc new(T: typedesc[TestProto]): T =
      # every incoming connections will be in handled in this closure
      proc handle(
          conn: Connection, proto: string
      ) {.async: (raises: [CancelledError]).} =
        try:
          var resp: array[6, byte]
          await conn.readExactly(addr resp, 6)
          check string.fromBytes(resp) == "client"
          await conn.write("server")
        except LPStreamError:
          check false # should not be here
        finally:
          await conn.close()

      return T.new(codecs = @[TestCodec], handler = handle)

    let rng = newRng()

    let ma = MultiAddress
      .init(
        "/ip4/127.0.0.1/tcp/" & $onionLocalPort1 &
        "/onion3/a2mncbqsbullu7thgm4e6zxda2xccmcgzmaq44oayhdtm6rav5vovcad:80"
      )
      .tryGet()

    let serverSwitch = TorSwitch.new(torServer, rng, @[ma], {ReuseAddr})

    # setup the custom proto
    let testProto = TestProto.new()

    serverSwitch.mount(testProto)
    await serverSwitch.start()

    let serverPeerId = serverSwitch.peerInfo.peerId
    let serverAddress = serverSwitch.peerInfo.addrs

    proc startClient() {.async.} =
      let clientSwitch =
        TorSwitch.new(torServer = torServer, rng = rng, flags = {ReuseAddr})

      let conn = await clientSwitch.dial(serverPeerId, serverAddress, TestCodec)

      await conn.write("client")

      var resp: array[6, byte]
      await conn.readExactly(addr resp, 6)
      check string.fromBytes(resp) == "server"
      await conn.close()
      await clientSwitch.stop()

    await startClient()

    await serverSwitch.stop()

  test "It's not possible to add another transport in TorSwitch":
    let torSwitch = TorSwitch.new(torServer = torServer, rng = rng, flags = {ReuseAddr})
    expect(AssertionDefect):
      torSwitch.addTransport(TcpTransport.new(upgrade = Upgrade()))
    waitFor torSwitch.stop()

  proc transProvider(): Transport =
    TorTransport.new(torServer, {ReuseAddr}, Upgrade())

  commonTransportTest(
    transProvider,
    "/ip4/127.0.0.1/tcp/" & $onionLocalPort1 &
      "/onion3/a2mncbqsbullu7thgm4e6zxda2xccmcgzmaq44oayhdtm6rav5vovcad:80",
    "/ip4/127.0.0.1/tcp/" & $onionLocalPort2 &
      "/onion3/a2mncbqsbullu7thgm4e6zxda2xccmcgzmaq44oayhdtm6rav5vovcae:81",
  )
