{.used.}

when defined(libp2p_msquic_experimental):
  import std/[os, strutils]
  import chronos
  import pkg/results
  import unittest2
  import stew/byteutils

  import
    ../libp2p/[
      crypto/crypto,
      multiaddress,
      muxers/muxer,
      transports/tsnettransport,
      upgrademngrs/upgrade,
    ]

  import ./helpers

  proc fixturePath(name: string): string =
    currentSourcePath.parentDir() / "fixtures" / "tsnet" / name

  var tempDirCounter = 0

  proc tempStateDir(tag: string): string =
    inc tempDirCounter
    let path = getTempDir() / ("nim-libp2p-tsnet-transport-quic-" & tag & "-" & $tempDirCounter)
    createDir(path)
    path

  suite "Tsnet QUIC transport":
    teardown:
      checkTrackers()

    proc makeTransport(): TsnetTransport =
      let privKey = PrivateKey.random(rng[]).expect("private key")
      TsnetTransport.new(Upgrade(), privKey)

    asyncTest "dial and accept keep /tsnet suffix":
      let server = makeTransport()
      let client = makeTransport()

      await server.start(@[MultiAddress.init("/ip4/0.0.0.0/udp/0/quic-v1/tsnet").tryGet()])
      check server.addrs.len == 1
      check ($server.addrs[0]).endsWith("/quic-v1/tsnet")

      let accepted = server.accept()
      let conn = await client.dial("", server.addrs[0])
      let serverConn = await accepted

      check conn.localAddr.isSome()
      check conn.observedAddr.isSome()
      check ($conn.localAddr.get()).endsWith("/quic-v1/tsnet")
      check ($conn.observedAddr.get()).endsWith("/quic-v1/tsnet")
      check serverConn.localAddr.isSome()
      check ($serverConn.localAddr.get()).endsWith("/quic-v1/tsnet")

      let clientMux = await client.upgrade(conn, Opt.none(PeerId))
      let serverMux = await server.upgrade(serverConn, Opt.none(PeerId))
      let inboundReady = Future[string].init("tsnet.transport.quic.inbound")
      serverMux.streamHandler = proc(ch: Connection) {.async: (raises: []).} =
        try:
          var msg = newSeq[byte](4)
          await ch.readExactly(addr msg[0], 4)
          if not inboundReady.finished():
            inboundReady.complete(string.fromBytes(msg))
        except CancelledError:
          discard
        except CatchableError:
          discard
        finally:
          await ch.closeWithEOF()
      let stream = await clientMux.newStream()
      await stream.write("ping")
      check await inboundReady.withTimeout(5.seconds)
      check inboundReady.read() == "ping"
      await stream.closeWithEOF()

      await clientMux.close()
      await serverMux.close()
      await server.stop()
      await client.stop()

    asyncTest "proxy-backed quic dials use raw proxy host instead of remote tsnet host":
      let dir = tempStateDir("dial-host")
      copyFile(fixturePath("self_hosted_901_sin.json"), dir / "nim-tsnet-oracle.json")

      let privKey = PrivateKey.random(rng[]).expect("private key")
      var cfg = TsnetTransportBuilderConfig.init()
      cfg.controlUrl = "https://64-176-84-12.sslip.io"
      cfg.stateDir = dir
      cfg.hostname = "nim-transport-quic-host-test"
      cfg.enableDebug = true
      let transport = TsnetTransport.new(Upgrade(), privKey, cfg)

      await transport.start(@[MultiAddress.init("/ip4/0.0.0.0/udp/0/quic-v1/tsnet").tryGet()])
      check transport.warmProvider().isOk
      let rawTarget = MultiAddress.init("/ip4/127.0.0.1/udp/12345/quic-v1").tryGet()
      check transport.quicDialHostname("100.64.0.77", rawTarget) == "127.0.0.1"
      await transport.stop()
else:
  import unittest2

  suite "Tsnet QUIC transport":
    test "experimental feature disabled":
      skip()
