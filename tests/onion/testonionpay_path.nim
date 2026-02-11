# Nim-LibP2P Onion path integration tests

{.used.}

import unittest

import ../../libp2p/protocols/onionpay
import ../../libp2p/peerid
import ../../libp2p/crypto/crypto

proc makeSecret(seed: byte): array[32, byte] =
  for i in 0 ..< result.len:
    result[i] = byte(seed xor byte(i))

proc randomPeerId(rng: ref HmacDrbgContext): PeerId =
  PeerId.random(rng).tryGet()

suite "Onion path":
  test "peel path preserves ordering":
    var rng = newRng()
    let rid = randomOnionRouteId(rng)
    var hops: seq[OnionRouteHop]
    for i in 0 ..< 4:
      hops.add(
        OnionRouteHop(
          peer: randomPeerId(rng),
          secret: makeSecret(byte(0x30 + i)),
          ttl: uint16(8 + i),
        )
      )
    let route = OnionRoute(id: rid, hops: hops)
    let payload = @[byte 0xCA, 0xFE, 0xBE, 0xEF]
    var packet = buildOnionPacket(route, payload, rng).tryGet()

    for hopIdx, hop in hops:
      let layer = peelOnionLayer(packet, hop.secret).tryGet()
      if hopIdx < hops.high:
        check layer.nextPeer.isSome()
        check layer.nextPeer.get() == hops[hopIdx + 1].peer
        check not layer.isFinal
      else:
        check layer.nextPeer.isNone()
        check layer.isFinal
        check layer.payload == payload
      check layer.ttl == hop.ttl

  test "route tamper detection":
    var rng = newRng()
    let rid = randomOnionRouteId(rng)
    let hop = OnionRouteHop(peer: randomPeerId(rng), secret: makeSecret(0x01), ttl: 5)
    let route = OnionRoute(id: rid, hops: @[hop])
    var packet = buildOnionPacket(route, @[byte 0x01, 0x02], rng).tryGet()
    packet.routeId[0] = byte(packet.routeId[0] xor 0xFF)
    check peelOnionLayer(packet, hop.secret).isErr()
