# Nim-Libp2p
# Copyright (c) 2025 Status Research & Development GmbH
# Licensed under either of
#  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
#  * MIT license ([LICENSE-MIT](LICENSE-MIT))
# at your option.
# This file may not be copied, modified, or distributed except according to
# those terms.

{.push raises: [].}

import results
import nimcrypto/[sha2, hkdf, hmac]

import ../../protocols/onionpay as lpOnion
import ./types

const
  onionHopLabel = "coinjoin-onion-hop"
  onionLayerLabel = "coinjoin-onion-layer"
  onionProofLabel = "coinjoin-onion-proof"

let emptySalt: array[0, byte] = []

proc appendUint64(info: var seq[byte], value: uint64) =
  for i in countdown(7, 0):
    info.add byte((value shr (i * 8)) and 0xff)

proc appendUint32(info: var seq[byte], value: uint32) =
  for i in countdown(3, 0):
    info.add byte((value shr (i * 8)) and 0xff)

proc appendString(info: var seq[byte], s: string) =
  for ch in s:
    info.add byte(ch)

proc appendBytes(info: var seq[byte], data: openArray[byte]) =
  if data.len == 0:
    return
  let start = info.len
  info.setLen(info.len + data.len)
  copyMem(addr info[start], unsafeAddr data[0], data.len)

proc toProtocolRouteId(routeId: uint64): lpOnion.OnionRouteId =
  var out: lpOnion.OnionRouteId
  for i in 0 ..< out.len:
    let shift = (out.len - 1 - i) * 8
    out[i] = byte((routeId shr shift) and 0xff)
  out

proc fromProtocolRouteId(routeId: lpOnion.OnionRouteId): uint64 =
  var value = 0'u64
  for b in routeId:
    value = (value shl 8) or uint64(b)
  value

proc onionErrorToCoinJoin(err: lpOnion.OnionError, feature: string): CoinJoinError =
  let kind =
    case err
    of lpOnion.OnionRouteEmpty, lpOnion.OnionLayerTooShort, lpOnion.OnionPeerDecodeFailed,
        lpOnion.OnionRouteMismatch:
      cjInvalidInput
    else:
      cjInternal
  newCoinJoinError(kind, feature & ": " & $err)

proc encodeLength(dataLen: int): CoinJoinResult[array[4, byte]] =
  if dataLen < 0 or dataLen > high(uint32).int:
    return err(newCoinJoinError(cjInvalidInput, "onion payload too large"))
  var encoded: array[4, byte]
  let value = uint32(dataLen)
  for i in countdown(3, 0):
    encoded[3 - i] = byte((value shr (i * 8)) and 0xff)
  ok(encoded)

proc decodeLength(prefix: openArray[byte]): CoinJoinResult[uint32] =
  if prefix.len < 4:
    return err(newCoinJoinError(cjInvalidInput, "onion body truncated"))
  var value = 0'u32
  for i in 0 ..< 4:
    value = (value shl 8) or uint32(prefix[i])
  ok(value)

proc wrapEnvelope(envelope: seq[byte]): CoinJoinResult[seq[byte]] =
  let lenPrefix = ?encodeLength(envelope.len)
  var wrapped = newSeq[byte](lenPrefix.len + envelope.len)
  copyMem(addr wrapped[0], unsafeAddr lenPrefix[0], lenPrefix.len)
  if envelope.len > 0:
    copyMem(addr wrapped[lenPrefix.len], unsafeAddr envelope[0], envelope.len)
  ok(wrapped)

proc unwrapEnvelope(body: seq[byte]): CoinJoinResult[seq[byte]] =
  if body.len < 4:
    return err(newCoinJoinError(cjInvalidInput, "onion body truncated"))
  let declared = ?decodeLength(body.toOpenArray(0, 3))
  let remaining = body.len - 4
  if declared.int != remaining:
    return err(newCoinJoinError(cjInvalidInput, "onion body length mismatch"))
  if remaining == 0:
    return ok(newSeq[byte]())
  ok(body[4 ..< body.len])

proc isZero(data: OnionSecret): bool =
  for b in data:
    if b != 0:
      return false
  true

proc deriveProofKey(hopSecrets: openArray[OnionSecret]): CoinJoinResult[array[32, byte]] =
  if hopSecrets.len == 0:
    return err(newCoinJoinError(cjInvalidInput, "hop secrets required"))
  var ctx: sha256
  ctx.init()
  ctx.update(onionProofLabel)
  for secret in hopSecrets:
    ctx.update(secret)
  let digest = ctx.finish()
  ok(digest.data)

proc computeLayerMac(
    secret: OnionSecret, routeId: uint64, downstream: openArray[byte]
): OnionMac =
  var ctx = newSeq[byte]()
  ctx.appendString(onionLayerLabel)
  ctx.appendUint64(routeId)
  ctx.appendBytes(downstream)
  let digest = sha256.hmac(secret, ctx)
  digest.data

proc deriveHopSecrets*(
    seed: OnionSeed,
    sessionId: uint64,
    participantId: openArray[byte],
    hopCount: uint8
): CoinJoinResult[seq[OnionSecret]] =
  if not coinJoinSupportEnabled():
    return errDisabled[seq[OnionSecret]]("deriveHopSecrets")
  if hopCount == 0:
    return err(newCoinJoinError(cjInvalidInput, "hopCount must be > 0"))
  if participantId.len == 0:
    return err(newCoinJoinError(cjInvalidInput, "participantId required"))
  if participantId.len > high(uint32).int:
    return err(newCoinJoinError(cjInvalidInput, "participantId too long"))

  var secrets = newSeq[OnionSecret](hopCount.int)
  for idx in 0 ..< hopCount.int:
    var info = newSeq[byte]()
    info.appendString(onionHopLabel)
    info.appendUint64(sessionId)
    info.appendUint32(uint32(participantId.len))
    info.appendBytes(participantId)
    info.add(byte(idx))

    var out: array[1, OnionSecret]
    sha256.hkdf(emptySalt, seed, info, out)
    if out[0].isZero():
      return err(newCoinJoinError(cjInternal, "hkdf produced zero hop secret"))
    secrets[idx] = out[0]
  ok(secrets)

proc initRouteCtx*(
    seed: OnionSeed,
    sessionId: uint64,
    participantId: openArray[byte],
    hopCount: uint8,
    payload: seq[byte]
): CoinJoinResult[OnionRouteCtx] =
  if not coinJoinSupportEnabled():
    return errDisabled[OnionRouteCtx]("initRouteCtx")
  let secrets = ?deriveHopSecrets(seed, sessionId, participantId, hopCount)
  ok(
    OnionRouteCtx(
      seed: seed,
      hopSecrets: secrets,
      payload: payload,
      mac: default(OnionMac)
    )
  )

proc onionProof*(
    hopSecrets: openArray[OnionSecret], transcript: openArray[byte]
): CoinJoinResult[OnionProof] =
  if not coinJoinSupportEnabled():
    return errDisabled[OnionProof]("onionProof")
  let key = ?deriveProofKey(hopSecrets)
  let mac = sha256.hmac(key, transcript)
  ok(mac.data)

proc verifyOnionProof*(
    hopSecrets: openArray[OnionSecret],
    transcript: openArray[byte],
    proof: OnionProof
): CoinJoinResult[bool] =
  if not coinJoinSupportEnabled():
    return errDisabled[bool]("verifyOnionProof")
  let expected = ?onionProof(hopSecrets, transcript)
  ok(expected == proof)

proc prepareRoute(
    route: lpOnion.OnionRoute, hopSecrets: openArray[OnionSecret]
): CoinJoinResult[lpOnion.OnionRoute] =
  if hopSecrets.len == 0:
    return err(newCoinJoinError(cjInvalidInput, "missing hop secrets"))
  if hopSecrets.len != route.hops.len:
    return err(newCoinJoinError(
      cjInvalidInput, "hop secrets and route hops length mismatch"
    ))
  var copy = route
  for i in 0 ..< copy.hops.len:
    copy.hops[i].secret = hopSecrets[i]
  ok(copy)

proc buildOnionPacket*(
    routeId: uint64, payload: seq[byte], hopSecrets: seq[OnionSecret]
): CoinJoinResult[OnionPacket] =
  if not coinJoinSupportEnabled():
    return errDisabled[OnionPacket]("buildOnionPacket")
  if hopSecrets.len == 0:
    return err(newCoinJoinError(cjInvalidInput, "missing hop secrets"))

  var envelope = payload
  var mac = default(OnionMac)
  for idx in countdown(hopSecrets.high, 0):
    let secret = hopSecrets[idx]
    let body = ?wrapEnvelope(envelope)
    mac = computeLayerMac(secret, routeId, body)
    var layer = newSeq[byte](OnionMac.len + body.len)
    if OnionMac.len > 0:
      copyMem(addr layer[0], unsafeAddr mac[0], OnionMac.len)
    if body.len > 0:
      copyMem(addr layer[OnionMac.len], unsafeAddr body[0], body.len)
    envelope = layer
  ok(OnionPacket(routeId: routeId, payload: envelope, mac: mac))

proc peelOnionLayer*(
    packet: OnionPacket, hopSecret: OnionSecret
): CoinJoinResult[OnionPacket] =
  if not coinJoinSupportEnabled():
    return errDisabled[OnionPacket]("peelOnionLayer")
  if packet.payload.len < OnionMac.len + 4:
    return err(newCoinJoinError(cjInvalidInput, "onion payload truncated"))

  var outerMac: OnionMac
  copyMem(addr outerMac[0], unsafeAddr packet.payload[0], OnionMac.len)
  let body = packet.payload[OnionMac.len ..< packet.payload.len]
  let expected = computeLayerMac(hopSecret, packet.routeId, body)
  if expected != outerMac:
    return err(newCoinJoinError(cjInvalidInput, "onion layer mac mismatch"))

  let envelope = ?unwrapEnvelope(body)
  ok(OnionPacket(routeId: packet.routeId, payload: envelope, mac: default(OnionMac)))

proc toOnionPacket*(
    routeCtx: OnionRouteCtx,
    route: lpOnion.OnionRoute,
    rng: ref HmacDrbgContext = nil
): CoinJoinResult[lpOnion.OnionPacket] =
  if not coinJoinSupportEnabled():
    return errDisabled[lpOnion.OnionPacket]("toOnionPacket")
  var prepared = ?prepareRoute(route, routeCtx.hopSecrets)
  let built = lpOnion.buildOnionPacket(prepared, routeCtx.payload, rng)
  if built.isErr:
    return err(onionErrorToCoinJoin(built.error, "onionpay.buildOnionPacket"))
  ok(built.get)

proc fromOnionPacket*(packet: lpOnion.OnionPacket): CoinJoinResult[OnionPacket] =
  if not coinJoinSupportEnabled():
    return errDisabled[OnionPacket]("fromOnionPacket")
  if packet.payload.len < OnionMac.len + 4:
    return err(newCoinJoinError(cjInvalidInput, "onionpay packet truncated"))
  var mac = default(OnionMac)
  copyMem(addr mac[0], unsafeAddr packet.payload[0], OnionMac.len)
  ok(
    OnionPacket(
      routeId: fromProtocolRouteId(packet.routeId),
      payload: packet.payload,
      mac: mac
    )
  )
