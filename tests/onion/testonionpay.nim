# Nim-LibP2P
# Copyright (c) 2025
# Licensed under MIT or Apache 2.0 at your option.

{.used.}

import unittest
import ../../libp2p/protocols/onionpay
import ../../libp2p/peerid
import ../../libp2p/crypto/crypto

proc makeSecret(value: byte): array[32, byte] =
  for i in 0 ..< result.len:
    result[i] = value xor byte(i)

proc randomPeer(rng: ref HmacDrbgContext): PeerId =
  PeerId.random(rng).tryGet()

suite "Onion builder":
  test "build and peel multi-hop route":
    var rng = newRng()
    let routeId = randomOnionRouteId(rng)
    var hops: seq[OnionRouteHop]
    for i in 0 ..< 3:
      hops.add(
        OnionRouteHop(
          peer: randomPeer(rng),
          secret: makeSecret(byte(0x10 + i)),
          ttl: uint16(5 + i),
        )
      )
    let route = OnionRoute(id: routeId, hops: hops)
    let payload = @[byte 0xAA, 0xBB, 0xCC, 0xDD]
    var packet = buildOnionPacket(route, payload, rng).tryGet()

    for i in 0 ..< hops.len:
      let res = peelOnionLayer(packet, hops[i].secret).tryGet()
      if i < hops.high:
        check res.nextPeer.isSome
        check res.nextPeer.get() == hops[i + 1].peer
        check not res.isFinal
      else:
        check res.isFinal
        check res.nextPeer.isNone
        check res.payload == payload
      check res.ttl == hops[i].ttl

  test "invalid key fails to peel":
    var rng = newRng()
    let routeId = randomOnionRouteId(rng)
    let hopPeer = randomPeer(rng)
    let hop = OnionRouteHop(peer: hopPeer, secret: makeSecret(0x01), ttl: 10)
    let route = OnionRoute(id: routeId, hops: @[hop])
    var packet = buildOnionPacket(route, @[byte 0x01, 0x02], rng).tryGet()
    let badSecret = makeSecret(0xFF)
    check peelOnionLayer(packet, badSecret).isErr
