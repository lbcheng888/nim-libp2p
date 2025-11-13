{.used.}

# Nim-LibP2P

import unittest2
import chronos
import ../libp2p/protocols/pubsub/gossipsub

import ./utils/async_tests
import ./pubsub/utils

suite "gossipsub max message size":
  asyncTest "publish rejects oversize when sharding disabled":
    let (g, conns, peers) = setupGossipSubWithPeers(
      1, "/max", populateGossipsub = true, populateMesh = true
    )
    defer:
      await teardownGossipSub(g, conns)

    g.parameters.enableShardEncoding = false
    g.parameters.maxMessageSize = 32
    g.maxMessageSize = g.parameters.maxMessageSize

    let largePayload = newSeq[byte](64)
    let sent = await g.publish("/max", largePayload)
    check sent == 0

  asyncTest "publish shards oversize payload when enabled":
    let (g, conns, peers) = setupGossipSubWithPeers(
      1, "/shard", populateGossipsub = true, populateMesh = true
    )
    defer:
      await teardownGossipSub(g, conns)

    g.parameters.enableShardEncoding = true
    g.parameters.shardChunkSize = 16
    g.parameters.shardMinMessageSize = 16
    g.parameters.shardRedundancy = 1
    g.parameters.maxMessageSize = 32
    g.maxMessageSize = g.parameters.maxMessageSize

    let payload = newSeq[byte](64)
    let sent = await g.publish("/shard", payload)
    check sent == peers.len
