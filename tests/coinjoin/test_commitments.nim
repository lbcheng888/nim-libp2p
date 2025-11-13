# Nim-Libp2p CoinJoin commitments & onion tests

import unittest2, results
import libp2p/crypto/coinjoin
import libp2p/protocols/onionpay
import libp2p/peerid as lpPeerId

proc makeSeed(tag: byte): OnionSeed =
  var seed: OnionSeed
  for i in 0 ..< seed.len:
    seed[i] = byte(tag xor byte(i))
  seed

proc asciiBytes(s: string): seq[byte] =
  result = newSeq[byte](s.len)
  for i, ch in s:
    result[i] = byte(ch)

when defined(libp2p_coinjoin):
  proc buildRoute(hops: Natural): OnionRoute =
    var route = OnionRoute(id: randomOnionRouteId(), hops: @[])
    for i in 0 ..< hops:
      let peerRes = lpPeerId.PeerId.random()
      let peer =
        if peerRes.isOk:
          peerRes.get()
        else:
          raise newException(Exception, "failed to generate peer id")
      route.hops.add(OnionRouteHop(peer: peer, secret: default(array[32, byte]), ttl: uint16(5 + i)))
    route

  proc routeIdToUint64(routeId: OnionRouteId): uint64 =
    var value = 0'u64
    for b in routeId:
      value = (value shl 8) or uint64(b)
    value

suite "CoinJoin commitments & onion":
  when defined(libp2p_coinjoin):
    test "pedersen commitment roundtrip and mismatch detection":
      let amount = encodeAmount(42)
      let blind = hkdfBlind(@[byte 0x01, 0x02], 9, 1).valueOr:
        fail $error
        return
      let commitment = pedersenCommit(amount, blind).valueOr:
        fail $error
        return
      check verifyCommitment(commitment, amount, blind).valueOr(false)

      let otherBlind = hkdfBlind(@[byte 0x01, 0x02], 9, 2).valueOr:
        fail $error
        return
      check not verifyCommitment(commitment, amount, otherBlind).valueOr(true)

    test "commitment add/subtract closes under group operation":
      let amountIn = encodeAmount(11)
      let amountOut = encodeAmount(5)
      let blindIn = hkdfBlind(@[byte 0x0A], 2, 1).valueOr:
        fail $error
        return
      let blindOut = hkdfBlind(@[byte 0x0B], 2, 2).valueOr:
        fail $error
        return
      let commitIn = pedersenCommit(amountIn, blindIn).valueOr:
        fail $error
        return
      let commitOut = pedersenCommit(amountOut, blindOut).valueOr:
        fail $error
        return
      let diff = subtractCommitments(commitIn, commitOut).valueOr:
        fail $error
        return
      let amountDelta = encodeAmount(11 - 5)
      let blindDelta = subBlinds(blindIn, blindOut).valueOr:
        fail $error
        return
      check verifyCommitment(diff, amountDelta, blindDelta).valueOr(false)

    test "deriveHopSecrets deterministic per session and participant":
      let seed = makeSeed(0x33)
      let secretsA = deriveHopSecrets(seed, 7, asciiBytes("alice"), 3).valueOr:
        fail $error
        return
      check secretsA.len == 3
      check secretsA[0] != secretsA[1]
      let secretsB = deriveHopSecrets(seed, 7, asciiBytes("alice"), 3).valueOr:
        fail $error
        return
      check secretsA == secretsB
      let secretsOtherSession = deriveHopSecrets(seed, 8, asciiBytes("alice"), 3).valueOr:
        fail $error
        return
      check secretsOtherSession != secretsA
      let secretsOtherParticipant = deriveHopSecrets(seed, 7, asciiBytes("bob"), 3).valueOr:
        fail $error
        return
      check secretsOtherParticipant != secretsA

    test "deriveHopSecrets rejects zero hop count":
      let seed = makeSeed(0x21)
      let res = deriveHopSecrets(seed, 1, asciiBytes("alice"), 0)
      check res.isErr and res.error.kind == cjInvalidInput

    test "buildOnionPacket layers can be peeled back":
      let seed = makeSeed(0x55)
      let secrets = deriveHopSecrets(seed, 12, asciiBytes("route-1"), 3).valueOr:
        fail $error
        return
      let payload = @[byte 0x01, 0x02, 0x03, 0x04]
      var packet = buildOnionPacket(0xDEADBEEF'u64, payload, secrets).valueOr:
        fail $error
        return
      for secret in secrets:
        packet = peelOnionLayer(packet, secret).valueOr:
          fail $error
          return
      check packet.payload == payload
      check packet.mac == default(OnionMac)

    test "peelOnionLayer detects tampering":
      let seed = makeSeed(0x77)
      let secrets = deriveHopSecrets(seed, 99, asciiBytes("route-2"), 2).valueOr:
        fail $error
        return
      let payload = @[byte 0x10, 0x20]
      let packet = buildOnionPacket(123'u64, payload, secrets).valueOr:
        fail $error
        return
      var badSecret = secrets[0]
      badSecret[0] = badSecret[0] xor 0xFF
      let res = peelOnionLayer(packet, badSecret)
      check res.isErr and res.error.kind == cjInvalidInput

    test "initRouteCtx stores payload and hop secrets":
      let seed = makeSeed(0x44)
      let payload = @[byte 0xAB, 0xCD]
      let ctx = initRouteCtx(seed, 5, asciiBytes("ctx-user"), 2, payload).valueOr:
        fail $error
        return
      check ctx.hopSecrets.len == 2
      check ctx.payload == payload

    test "onionProof roundtrip and tamper detection":
      let seed = makeSeed(0x88)
      let hops = deriveHopSecrets(seed, 77, asciiBytes("proof-user"), 3).valueOr:
        fail $error
        return
      let transcript = @[byte 0x01, 0x02, 0x03]
      let proof = onionProof(hops, transcript).valueOr:
        fail $error
        return
      check verifyOnionProof(hops, transcript, proof).valueOr(false)
      var tampered = proof
      tampered[0] = tampered[0] xor 0xFF
      check not verifyOnionProof(hops, transcript, tampered).valueOr(true)

    test "bridge to onionpay packet preserves payload":
      let route = buildRoute(3)
      let payload = @[byte 0x01, 0xFE, 0xAA]
      let ctx = initRouteCtx(makeSeed(0x91), 55, asciiBytes("bridge"), 3, payload).valueOr:
        fail $error
        return
      let networkPacket = toOnionPacket(ctx, route).valueOr:
        fail $error
        return
      let cjPacket = fromOnionPacket(networkPacket).valueOr:
        fail $error
        return
      check cjPacket.payload == networkPacket.payload
      check cjPacket.routeId == routeIdToUint64(route.id)

    test "toOnionPacket detects hop mismatch":
      let route = buildRoute(2)
      let ctx = initRouteCtx(makeSeed(0xA1), 77, asciiBytes("mismatch"), 3, @[byte 0x00]).valueOr:
        fail $error
        return
      let res = toOnionPacket(ctx, route)
      check res.isErr and res.error.kind == cjInvalidInput
  else:
    test "coinjoin feature flag disabled":
      let amount = default(CoinJoinAmount)
      let blind = default(CoinJoinBlind)
      let res = pedersenCommit(amount, blind)
      check res.isErr and res.error.kind == cjDisabled
