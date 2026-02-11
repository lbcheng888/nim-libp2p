{.used.}

# Nim-Libp2p
# Copyright (c) 2024 Status Research & Development GmbH
# Licensed under either of
#  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
#  * MIT license ([LICENSE-MIT](LICENSE-MIT))
# at your option.
# This file may not be copied, modified, or distributed except according to
# those terms.

import chronos, stew/byteutils
import ./helpers
import
  ../libp2p/[
    multiaddress,
    peerid,
    crypto/crypto,
    protocols/secure/tls,
    protocols/secure/secure,
    stream/connection,
    transports/transport,
    transports/tcptransport,
    upgrademngrs/muxedupgrade,
  ]

when not defined(libp2p_run_tls_tests):
  suite "TLS":
    test "tls tests disabled":
      skip()
else:
  suite "TLS":
    teardown:
      checkTrackers()

    asyncTest "e2e - tls handshake":
      let serverAddresses = @[MultiAddress.init("/ip4/0.0.0.0/tcp/0").tryGet()]
      let
        serverPrivKey = PrivateKey.random(ECDSA, rng[]).get()
        serverPeerId = PeerId.init(serverPrivKey).get()
        serverTls = TLS.new(rng, serverPrivKey)

      let transportServer: TcpTransport = TcpTransport.new(upgrade = Upgrade())
      asyncSpawn transportServer.start(serverAddresses)

      proc acceptHandler() {.async.} =
        let conn = await transportServer.accept()
        var sconn: Connection
        try:
          sconn = await serverTls.secure(conn, Opt.none(PeerId))
          let incoming = string.fromBytes(await sconn.readLp(1024))
          check incoming == "ping"
          await sconn.writeLp("pong")
        finally:
          if not sconn.isNil:
            await sconn.close()
          await conn.close()

      let acceptFut = acceptHandler()

      let
        clientPrivKey = PrivateKey.random(ECDSA, rng[]).get()
        clientTls = TLS.new(rng, clientPrivKey)
        transportClient: TcpTransport = TcpTransport.new(upgrade = Upgrade())
        conn = await transportClient.dial(transportServer.addrs[0])

      let tlsConn = await clientTls.secure(conn, Opt.some(serverPeerId))
      await tlsConn.writeLp("ping")
      let response = string.fromBytes(await tlsConn.readLp(1024))
      check response == "pong"

      await tlsConn.close()
      await conn.close()
      await acceptFut
      await transportClient.stop()
      await transportServer.stop()

    asyncTest "tls handshake fails on peer mismatch":
      let serverAddresses = @[MultiAddress.init("/ip4/0.0.0.0/tcp/0").tryGet()]
      let
        serverPrivKey = PrivateKey.random(ECDSA, rng[]).get()
        serverPeerId = PeerId.init(serverPrivKey).get()
        serverTls = TLS.new(rng, serverPrivKey)
        transportServer: TcpTransport = TcpTransport.new(upgrade = Upgrade())

      asyncSpawn transportServer.start(serverAddresses)

      proc acceptHandler() {.async.} =
        let conn = await transportServer.accept()
        var sconn: Connection
        try:
          sconn = await serverTls.secure(conn, Opt.none(PeerId))
        except LPStreamError:
          discard
        finally:
          if not sconn.isNil:
            await sconn.close()
          await conn.close()

      let acceptFut = acceptHandler()

      let
        clientPrivKey = PrivateKey.random(ECDSA, rng[]).get()
        clientTls = TLS.new(rng, clientPrivKey)
        transportClient: TcpTransport = TcpTransport.new(upgrade = Upgrade())
        conn = await transportClient.dial(transportServer.addrs[0])
        wrongPeerId = PeerId.init(PrivateKey.random(ECDSA, rng[]).get()).get()

      expect TLSPeerMismatchError:
        discard await clientTls.secure(conn, Opt.some(wrongPeerId))

      await conn.close()
      await acceptFut
      await transportClient.stop()
      await transportServer.stop()

    asyncTest "tls handshake fails on alpn mismatch":
      let serverAddresses = @[MultiAddress.init("/ip4/0.0.0.0/tcp/0").tryGet()]
      let
        serverPrivKey = PrivateKey.random(ECDSA, rng[]).get()
        serverPeerId = PeerId.init(serverPrivKey).get()
        serverTls = TLS.new(rng, serverPrivKey)
        transportServer: TcpTransport = TcpTransport.new(upgrade = Upgrade())

      asyncSpawn transportServer.start(serverAddresses)

      proc acceptHandler() {.async.} =
        let conn = await transportServer.accept()
        var sconn: Connection
        try:
          sconn = await serverTls.secure(conn, Opt.none(PeerId))
          checkpoint "Server unexpectedly completed handshake with mismatched ALPN"
          fail()
        except LPStreamError:
          discard
        finally:
          if not sconn.isNil:
            await sconn.close()
          await conn.close()

      let acceptFut = acceptHandler()

      let
        clientPrivKey = PrivateKey.random(ECDSA, rng[]).get()
        clientTls = TLS.new(rng, clientPrivKey)

      clientTls.setAlpn(@["not-libp2p"])

      let
        transportClient: TcpTransport = TcpTransport.new(upgrade = Upgrade())
        conn = await transportClient.dial(transportServer.addrs[0])

      expect TLSHandshakeError:
        discard await clientTls.secure(conn, Opt.some(serverPeerId))

      await conn.close()
      await acceptFut
      await transportClient.stop()
      await transportServer.stop()
