{.used.}

# Nim-Libp2p
# Copyright (c) 2024 Status Research & Development GmbH
# Licensed under either of
#  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
#  * MIT license ([LICENSE-MIT](LICENSE-MIT))
# at your option.
# This file may not be copied, modified, or distributed except according to
# those terms.

{.push raises: [].}

import chronos
import
  ../libp2p/[builders, connectiongater, multiaddress],
  ./helpers

suite "Connection gater":
  teardown:
    checkTrackers()

  asyncTest "deny dial blocks outgoing attempt":
    let server =
      SwitchBuilder
        .new()
        .withRng(newRng())
        .withTcpTransport()
        .withNoise()
        .withTls()
        .build()
    defer:
      await server.stop()
    await server.start()

    let gater = ConnectionGater.new()
    gater.blockPeer(server.peerInfo.peerId)

    let client =
      SwitchBuilder
        .new()
        .withRng(newRng())
        .withTcpTransport()
        .withNoise()
        .withTls()
        .withConnectionGater(gater)
        .build()
    defer:
      await client.stop()
    await client.start()

    expect DialFailedError:
      await client.connect(server.peerInfo.peerId, server.peerInfo.addrs)

  asyncTest "deny accept blocks incoming connection":
    let serverGater = ConnectionGater.new()

    let server =
      SwitchBuilder
        .new()
        .withRng(newRng())
        .withTcpTransport()
        .withNoise()
        .withTls()
        .withConnectionGater(serverGater)
        .build()
    defer:
      await server.stop()
    await server.start()

    # Block the listening address to prevent incoming connections.
    server.connectionGater.blockAddress(server.peerInfo.listenAddrs[0])

    let client =
      SwitchBuilder
        .new()
        .withRng(newRng())
        .withTcpTransport()
        .withNoise()
        .withTls()
        .build()
    defer:
      await client.stop()
    await client.start()

    expect DialFailedError:
      await client.connect(server.peerInfo.peerId, server.peerInfo.addrs)

  asyncTest "deny secured connection after upgrade":
    let serverGater = ConnectionGater.new()

    let server =
      SwitchBuilder
        .new()
        .withRng(newRng())
        .withTcpTransport()
        .withNoise()
        .withTls()
        .withConnectionGater(serverGater)
        .build()
    defer:
      await server.stop()
    await server.start()

    let client =
      SwitchBuilder
        .new()
        .withRng(newRng())
        .withTcpTransport()
        .withNoise()
        .withTls()
        .build()
    defer:
      await client.stop()
    await client.start()

    server.connectionGater.blockPeer(client.peerInfo.peerId)

    expect DialFailedError:
      await client.connect(server.peerInfo.peerId, server.peerInfo.addrs)

  asyncTest "deny reuse of existing connection after blocking peer":
    let gater = ConnectionGater.new()

    let server =
      SwitchBuilder
        .new()
        .withRng(newRng())
        .withTcpTransport()
        .withNoise()
        .withTls()
        .build()
    defer:
      await server.stop()
    await server.start()

    let client =
      SwitchBuilder
        .new()
        .withRng(newRng())
        .withTcpTransport()
        .withNoise()
        .withTls()
        .withConnectionGater(gater)
        .build()
    defer:
      await client.stop()
    await client.start()

    await client.connect(server.peerInfo.peerId, server.peerInfo.addrs)
    check client.connManager.connCount(server.peerInfo.peerId) == 1

    gater.blockPeer(server.peerInfo.peerId)

    expect DialFailedError:
      await client.connect(server.peerInfo.peerId, server.peerInfo.addrs)

    checkUntilTimeout:
      client.connManager.connCount(server.peerInfo.peerId) == 0
