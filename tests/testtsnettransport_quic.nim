{.used.}

when defined(libp2p_msquic_experimental):
  import std/strutils
  import unittest2
  import stew/byteutils

  import
    ../libp2p/[
      crypto/crypto,
      multiaddress,
      transports/tsnettransport,
      upgrademngrs/upgrade,
    ]

  import ./helpers

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
      let conn = await client.dial(server.addrs[0])
      let serverConn = await accepted

      check conn.localAddr.isSome()
      check conn.observedAddr.isSome()
      check ($conn.localAddr.get()).endsWith("/quic-v1/tsnet")
      check ($conn.observedAddr.get()).endsWith("/quic-v1/tsnet")
      check serverConn.localAddr.isSome()
      check ($serverConn.localAddr.get()).endsWith("/quic-v1/tsnet")

      await conn.write("ping")
      var msg = newSeq[byte](4)
      await serverConn.readExactly(addr msg[0], 4)
      check string.fromBytes(msg) == "ping"

      await conn.close()
      await serverConn.close()
      await server.stop()
      await client.stop()
else:
  import unittest2

  suite "Tsnet QUIC transport":
    test "experimental feature disabled":
      skip()
