{.used.}

# Nim-LibP2P
# Licensed under either of
#  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
#  * MIT license ([LICENSE-MIT](LICENSE-MIT))
# at your option.

import unittest2
import pkg/results

import ../libp2p/[resourcemanager, peerid, stream/connection]

suite "Resource manager shared pools":
  test "setSharedPoolLimit enforces limits and snapshot reflects usage":
    var manager = ResourceManager.new()
    let limit = SharedPoolLimit.init(
      connections = ProtocolLimit.init(
        maxTotal = 1, maxInbound = 1, perPeerTotal = 1, perPeerInbound = 1
      ),
      streams = ProtocolLimit.init(
        maxTotal = 2, maxInbound = 2, perPeerTotal = 2, perPeerInbound = 2
      ),
    )
    manager.setSharedPoolLimit("poolA", limit)

    var peer: PeerId
    require peer.init("QmcgpsyWgH8Y8ajJz1Cu72KnS5uo2Aa2LpzU7kinSupNKC")

    let first = manager.acquireConnection(peer, Direction.In, "poolA")
    require first.isOk()
    let firstPermit = first.get()

    let second = manager.acquireConnection(peer, Direction.In, "poolA")
    check second.isErr()

    let metricsActive = manager.snapshot()
    check metricsActive.sharedPools.len == 1
    let activePool = metricsActive.sharedPools[0]
    check activePool.pool == "poolA"
    check activePool.connInbound == 1
    check activePool.connOutbound == 0

    firstPermit.release()

    let third = manager.acquireConnection(peer, Direction.In, "poolA")
    require third.isOk()
    third.get().release()

    let metricsCleared = manager.snapshot()
    check metricsCleared.sharedPools.len == 0
