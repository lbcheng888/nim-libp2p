# Nim-LibP2P
# Copyright (c) 2025 Status Research & Development GmbH
# Licensed under either of
#  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
#  * MIT license ([LICENSE-MIT](LICENSE-MIT))
# at your option.
# This file may not be copied, modified, or distributed except according to
# those terms.

{.push raises: [].}

import std/options
import nimcrypto/sha2
import chronicles
import results
import ../peerid
import ../crypto/crypto

export options

logScope:
  topics = "libp2p onion"

const
  OnionCodec* = "/onion/1.0.0"
  OnionFlagFinal = 0x01'u8
  NonceSize = 12

type
  OnionRouteId* = array[8, byte]

  OnionRouteHop* = object
    ## Single hop definition used when building an onion packet.
    peer*: PeerId
    secret*: array[32, byte]
    ttl*: uint16

  OnionRoute* = object
    ## Ordered list of hops. `hops[0]` is the entry hop, `hops[^1]` is the exit.
    id*: OnionRouteId
    hops*: seq[OnionRouteHop]

  OnionPacket* = object
    ## Encapsulated payload routed through the network.
    routeId*: OnionRouteId
    payload*: seq[byte]

  OnionLayerResult* = object
    ## Result produced after peeling a layer.
    nextPeer*: Opt[PeerId]
    ttl*: uint16
    payload*: seq[byte]
    isFinal*: bool

  OnionError* = enum
    OnionRouteEmpty
    OnionEntropyUnavailable
    OnionLayerTooShort
    OnionPeerDecodeFailed
    OnionRouteMismatch

  OnionResult*[T] = Result[T, OnionError]

proc randomOnionRouteId*(rng: ref HmacDrbgContext = nil): OnionRouteId =
  ## Generate a random, non-zero identifier for onion routes.
  var rngRef = rng
  if rngRef.isNil:
    rngRef = newRng()
  if rngRef.isNil:
    raiseAssert "Unable to seed RNG for onion route id"

  var res: OnionRouteId
  hmacDrbgGenerate(rngRef[], res.toOpenArray(0, res.high))
  if res == default(OnionRouteId):
    res[0] = 0xAA'u8 # ensure non-zero identifier
  res

proc ensureRng(rng: var ref HmacDrbgContext): bool =
  if rng.isNil:
    rng = newRng()
  rng.isNil.not

proc deriveKeystream(secret, nonce: openArray[byte], length: int): seq[byte] =
  ## Derive a keystream using SHA256(secret || nonce || counter).
  var produced = 0
  var counter = 0'u32
  result = newSeq[byte](length)
  while produced < length:
    var ctx: sha256
    ctx.init()
    ctx.update(secret)
    ctx.update(nonce)
    var counterBuf: array[4, byte]
    counterBuf[0] = byte(counter and 0xFF'u32)
    counterBuf[1] = byte((counter shr 8) and 0xFF)
    counterBuf[2] = byte((counter shr 16) and 0xFF)
    counterBuf[3] = byte((counter shr 24) and 0xFF)
    ctx.update(counterBuf)
    let digest = ctx.finish()
    let chunk = min(digest.data.len, length - produced)
    copyMem(addr result[produced], unsafeAddr digest.data[0], chunk)
    inc(counter)
    inc(produced, chunk)

proc sealLayer(
    secret: array[32, byte],
    plaintext: openArray[byte],
    rng: ref HmacDrbgContext
): seq[byte] =
  var nonce: array[NonceSize, byte]
  hmacDrbgGenerate(rng[], nonce.toOpenArray(0, nonce.high))

  let keystream = deriveKeystream(secret, nonce, plaintext.len)
  result = newSeq[byte](NonceSize + plaintext.len)
  copyMem(addr result[0], unsafeAddr nonce[0], NonceSize)
  for i in 0 ..< plaintext.len:
    result[NonceSize + i] = plaintext[i] xor keystream[i]

proc openLayer(
    secret: array[32, byte],
    ciphertext: openArray[byte]
): OnionResult[seq[byte]] =
  if ciphertext.len <= NonceSize:
    return err(OnionLayerTooShort)

  var nonce: array[NonceSize, byte]
  copyMem(addr nonce[0], unsafeAddr ciphertext[0], NonceSize)
  let dataLen = ciphertext.len - NonceSize
  let keystream = deriveKeystream(secret, nonce, dataLen)
  var plain = newSeq[byte](dataLen)
  for i in 0 ..< dataLen:
    plain[i] = ciphertext[NonceSize + i] xor keystream[i]
  ok(plain)

proc addUint16BE(buf: var seq[byte], value: uint16) =
  buf.add(byte((value shr 8) and 0xFF'u16))
  buf.add(byte(value and 0xFF'u16))

proc readUint16BE(data: openArray[byte], offset: var int): OnionResult[uint16] =
  if offset + 1 >= data.len:
    return err(OnionLayerTooShort)
  let high = uint16(data[offset])
  let low = uint16(data[offset + 1])
  offset += 2
  ok((high shl 8) or low)

proc encodeHeader(
    routeId: OnionRouteId,
    nextPeer: Opt[PeerId],
    ttl: uint16,
    isFinal: bool
): seq[byte] =
  result = newSeq[byte]()
  var flags = 0'u8
  if isFinal:
    flags = flags or OnionFlagFinal
  result.add(flags)
  result.addUint16BE(ttl)
  result.add(routeId)

  if nextPeer.isSome:
    let peerBytes = nextPeer.get().data
    result.addUint16BE(uint16(peerBytes.len))
    result.add(peerBytes)
  else:
    result.addUint16BE(0'u16)

proc decodeHeader(
    routeId: OnionRouteId,
    plain: seq[byte]
): OnionResult[(Opt[PeerId], uint16, bool, seq[byte])] =
  if plain.len < 1 + 2 + len(routeId) + 2:
    return err(OnionLayerTooShort)

  var offset = 0
  let flags = plain[offset]
  inc(offset)
  let ttl = ?readUint16BE(plain, offset)
  for i in 0 ..< len(routeId):
    if offset + i >= plain.len:
      return err(OnionLayerTooShort)
    if plain[offset + i] != routeId[i]:
      return err(OnionRouteMismatch)
  inc(offset, len(routeId))

  let peerLen = ?readUint16BE(plain, offset)
  if offset + peerLen.int > plain.len:
    return err(OnionLayerTooShort)

  var nextPeer: Opt[PeerId]
  if peerLen > 0:
    let res = PeerId.init(plain.toOpenArray(offset, offset + peerLen.int - 1))
    if res.isErr:
      return err(OnionPeerDecodeFailed)
    nextPeer = Opt.some(res.get())
  inc(offset, peerLen.int)

  let payload = plain[offset ..< plain.len]
  ok((nextPeer, ttl, (flags and OnionFlagFinal) != 0, payload))

proc buildOnionPacket*(
    route: OnionRoute,
    payload: seq[byte],
    rng: ref HmacDrbgContext = nil
): OnionResult[OnionPacket] =
  if route.hops.len == 0:
    return err(OnionRouteEmpty)

  var rngRef = rng
  if not ensureRng(rngRef):
    return err(OnionEntropyUnavailable)

  var envelope = payload
  var downstream = Opt.none(PeerId)

  for idx in countdown(route.hops.high, 0):
    let hop = route.hops[idx]
    let header = encodeHeader(route.id, downstream, hop.ttl, idx == route.hops.high)
    var layer = newSeq[byte](header.len + envelope.len)
    copyMem(addr layer[0], unsafeAddr header[0], header.len)
    if envelope.len > 0:
      copyMem(addr layer[header.len], unsafeAddr envelope[0], envelope.len)
    envelope = sealLayer(hop.secret, layer, rngRef)
    downstream = Opt.some(hop.peer)

  ok(OnionPacket(routeId: route.id, payload: envelope))

proc peelOnionLayer*(
    packet: var OnionPacket,
    secret: array[32, byte]
): OnionResult[OnionLayerResult] =
  let plain = ?openLayer(secret, packet.payload)
  let (nextPeer, ttl, isFinal, remaining) = ?decodeHeader(packet.routeId, plain)
  packet.payload = remaining
  ok(
    OnionLayerResult(
      nextPeer: nextPeer,
      ttl: ttl,
      payload: remaining,
      isFinal: isFinal,
    )
  )

proc isComplete*(packet: OnionPacket): bool =
  packet.payload.len == 0
