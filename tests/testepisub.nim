{.used.}

# Nim-LibP2P

import unittest2
import chronos

import ../libp2p/protocols/pubsub/episub/[episub, protobuf]
import ../libp2p/peerid

suite "episub control":
  test "default params sanity":
    let params = defaultParams()
    check params.activeViewSize > 0
    check params.passiveViewSize > params.activeViewSize
    check params.randomPeers <= params.passiveViewSize
    check params.controlSuffix.len > 0
    check params.maintenanceInterval > 0.seconds
    check params.inactiveTimeout > params.maintenanceInterval

  test "encode decode control":
    let peer = PeerId.init("12D3KooWJzYQwXk1YgnSUZXoqBYwygJyBEtCQpTUMpKyDJZH3LzV").get()
    let msg = EpisubControl(
      kind: estJoin,
      peer: peer,
      ttl: 3,
      peers: @[peer],
    )
    let encoded = encode(msg)
    let decoded = decode(encoded)
    check decoded.isSome()
    let ctrl = decoded.get()
    check ctrl.kind == msg.kind
    check ctrl.peer == msg.peer
    check ctrl.ttl == msg.ttl
    check ctrl.peers.len == 1
