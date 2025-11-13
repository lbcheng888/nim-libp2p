# Nim-LibP2P
# Copyright (c) 2025
# Licensed under either of
#  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
#  * MIT license ([LICENSE-MIT](LICENSE-MIT))
# at your option.
# This file may not be copied, modified, or distributed except according to
# those terms.

{.push raises: [ValueError].}

import std/[sequtils, hashes, options, sets]
import results
import chronos
import stew/byteutils
import ../../../crypto/[gf256, crypto]

const
  ShardMagic* = "OPSH"
  ShardVersion* = 1u8

type
  SessionId* = uint32
  ShardKey* = tuple[topic: string, session: SessionId]

  ShardConfig* = object
    chunkSize*: int
    redundancy*: int

  RLNCShard* = object
    sessionId*: SessionId
    originalSize*: int
    chunkSize*: int
    shardCount*: int
    redundancy*: int
    shardIndex*: int
    coefficients*: seq[byte]
    payload*: seq[byte]

  ShardSession* = ref object
    sessionId*: SessionId
    originalSize*: int
    chunkSize*: int
    shardCount*: int
    redundancy*: int
    coefficients*: seq[seq[byte]]
    encoded*: seq[seq[byte]]
    seenIndices*: HashSet[int]
    coefficientHashes*: HashSet[string]
    completed*: bool
    createdAt*: Moment

proc appendUint16(dest: var seq[byte], value: int) {.inline, raises: [].} =
  dest.add(byte((value shr 8) and 0xFF))
  dest.add(byte(value and 0xFF))

proc appendUint32(dest: var seq[byte], value: int) {.inline, raises: [].} =
  dest.add(byte((value shr 24) and 0xFF))
  dest.add(byte((value shr 16) and 0xFF))
  dest.add(byte((value shr 8) and 0xFF))
  dest.add(byte(value and 0xFF))

proc readUint16(data: seq[byte], pos: var int): uint16 {.raises: [ValueError].} =
  if pos + 2 > data.len:
    raise (ref ValueError)(msg: "Unexpected end of data")
  result =
    (uint16(data[pos]) shl 8) or
    uint16(data[pos + 1])
  inc pos, 2

proc readUint32(data: seq[byte], pos: var int): uint32 {.raises: [ValueError].} =
  if pos + 4 > data.len:
    raise (ref ValueError)(msg: "Unexpected end of data")
  result =
    (uint32(data[pos]) shl 24) or
    (uint32(data[pos + 1]) shl 16) or
    (uint32(data[pos + 2]) shl 8) or
    uint32(data[pos + 3])
  inc pos, 4

proc isShardPayload*(data: seq[byte]): bool {.raises: [].} =
  if data.len <= ShardMagic.len:
    return false
  for i in 0 ..< ShardMagic.len:
    if data[i] != byte(ShardMagic[i]):
      return false
  true

proc encodeShardPayload*(shard: RLNCShard): Opt[seq[byte]] {.raises: [].} =
  if shard.chunkSize < 0 or shard.chunkSize > uint16.high.int:
    return Opt.none(seq[byte])
  if shard.shardCount <= 0 or shard.shardCount > uint16.high.int:
    return Opt.none(seq[byte])
  if shard.redundancy < 0 or shard.redundancy > uint16.high.int:
    return Opt.none(seq[byte])
  if shard.shardIndex < 0 or shard.shardIndex > uint16.high.int:
    return Opt.none(seq[byte])
  if shard.coefficients.len != shard.shardCount or shard.coefficients.len > uint16.high.int:
    return Opt.none(seq[byte])
  if shard.originalSize < 0 or shard.originalSize > uint32.high.int:
    return Opt.none(seq[byte])

  var encoded: seq[byte] = @[]
  encoded.add(ShardMagic.toBytes())
  encoded.add(byte(ShardVersion))
  encoded.add(byte(0)) # reserved flags
  appendUint32(encoded, int(shard.sessionId))
  appendUint32(encoded, shard.originalSize)
  appendUint16(encoded, shard.chunkSize)
  appendUint16(encoded, shard.shardCount)
  appendUint16(encoded, shard.redundancy)
  appendUint16(encoded, shard.shardIndex)
  appendUint16(encoded, shard.coefficients.len)
  encoded.add(shard.coefficients)
  encoded.add(shard.payload)
  Opt.some(encoded)

proc decodeShardPayload*(data: seq[byte]): Opt[RLNCShard] {.raises: [].} =
  if not data.isShardPayload():
    return Opt.none(RLNCShard)
  try:
    var idx = ShardMagic.len
    let version = data[idx]
    inc idx
    if version != ShardVersion:
      return Opt.none(RLNCShard)
    inc idx # flags
    let sessionId = readUint32(data, idx)
    let originalSize = readUint32(data, idx)
    let chunkSize = int(readUint16(data, idx))
    let shardCount = int(readUint16(data, idx))
    let redundancy = int(readUint16(data, idx))
    let shardIndex = int(readUint16(data, idx))
    let coeffLen = int(readUint16(data, idx))
    if coeffLen != shardCount or shardCount <= 0 or chunkSize <= 0:
      return Opt.none(RLNCShard)
    if idx + coeffLen > data.len:
      return Opt.none(RLNCShard)
    let coeffs = data[idx ..< idx + coeffLen]
    idx += coeffLen
    let bytesLeft = data.len - idx
    if bytesLeft != chunkSize:
      return Opt.none(RLNCShard)
    let payload = data[idx ..< data.len]
    Opt.some(
      RLNCShard(
        sessionId: SessionId(sessionId),
        originalSize: int(originalSize),
        chunkSize: chunkSize,
        shardCount: shardCount,
        redundancy: redundancy,
        shardIndex: shardIndex,
        coefficients: @coeffs,
        payload: @payload,
      )
    )
  except CatchableError:
    Opt.none(RLNCShard)

proc shardKey*(topic: string, sessionId: SessionId): ShardKey {.raises: [].} =
  (topic: topic, session: sessionId)

proc padChunk(data: seq[byte], chunkSize: int, offset: int): seq[byte] {.raises: [].} =
  result = newSeq[byte](chunkSize)
  let remaining = min(chunkSize, data.len - offset)
  if remaining > 0:
    copyMem(addr result[0], unsafeAddr data[offset], remaining)

proc randomCoefficients(count: int, rng: ref HmacDrbgContext): seq[byte] {.raises: [].} =
  result = newSeq[byte](count)
  var buf = newSeqUninit[byte](count)
  while true:
    hmacDrbgGenerate(rng[], buf)
    var allZero = true
    for i in 0 ..< count:
      result[i] = buf[i]
      if result[i] != 0:
        allZero = false
    if not allZero:
      break

proc encodePayload(
    original: seq[seq[byte]], coeffs: seq[byte], chunkSize: int
): seq[byte] {.raises: [].} =
  result = newSeq[byte](chunkSize)
  for shardIdx in 0 ..< coeffs.len:
    let coeff = coeffs[shardIdx]
    if coeff == 0:
      continue
    let shard = original[shardIdx]
    for i in 0 ..< chunkSize:
      result[i] = gfAdd(result[i], gfMul(coeff, shard[i]))

proc encodeShards*(
    data: seq[byte], cfg: ShardConfig, rng: ref HmacDrbgContext, sessionId: SessionId
): seq[seq[byte]] {.raises: [].} =
  if cfg.chunkSize <= 0:
    return @[]
  let shardCount = (data.len + cfg.chunkSize - 1) div cfg.chunkSize
  if shardCount == 0:
    let shardOpt = encodeShardPayload(
      RLNCShard(
        sessionId: sessionId,
        originalSize: 0,
        chunkSize: cfg.chunkSize,
        shardCount: 1,
        redundancy: cfg.redundancy,
        shardIndex: 0,
        coefficients: @[byte 1],
        payload: newSeq[byte](cfg.chunkSize),
      )
    )
    if shardOpt.isNone():
      return @[]
    return @[shardOpt.get()]

  var original = newSeq[seq[byte]](shardCount)
  for i in 0 ..< shardCount:
    original[i] = padChunk(data, cfg.chunkSize, i * cfg.chunkSize)

  let redundancy = max(cfg.redundancy, 0)
  let totalShards = shardCount + redundancy
  var shards = newSeq[seq[byte]](totalShards)

  for i in 0 ..< shardCount:
    var coeffs = newSeq[byte](shardCount)
    coeffs[i] = 1
    let shard = RLNCShard(
      sessionId: sessionId,
      originalSize: data.len,
      chunkSize: cfg.chunkSize,
      shardCount: shardCount,
      redundancy: redundancy,
      shardIndex: i,
      coefficients: coeffs,
      payload: original[i],
    )
    let payloadOpt = encodeShardPayload(shard)
    if payloadOpt.isNone():
      return @[]
    shards[i] = payloadOpt.get()

  for j in 0 ..< redundancy:
    let coeffs = randomCoefficients(shardCount, rng)
    let payload = encodePayload(original, coeffs, cfg.chunkSize)
    let shard = RLNCShard(
      sessionId: sessionId,
      originalSize: data.len,
      chunkSize: cfg.chunkSize,
      shardCount: shardCount,
      redundancy: redundancy,
      shardIndex: shardCount + j,
      coefficients: coeffs,
      payload: payload,
    )
    let payloadOpt = encodeShardPayload(shard)
    if payloadOpt.isNone():
      return @[]
    shards[shardCount + j] = payloadOpt.get()

  shards

proc newShardSession*(shard: RLNCShard): ShardSession {.raises: [].} =
  ShardSession(
    sessionId: shard.sessionId,
    originalSize: shard.originalSize,
    chunkSize: shard.chunkSize,
    shardCount: shard.shardCount,
    redundancy: shard.redundancy,
    coefficients: @[],
    encoded: @[],
    seenIndices: initHashSet[int](),
    coefficientHashes: initHashSet[string](),
    completed: false,
    createdAt: Moment.now(),
  )

proc coefficientHash(coeffs: seq[byte]): string {.raises: [].} =
  byteutils.toHex(coeffs)

proc acceptShard*(session: ShardSession, shard: RLNCShard): bool {.raises: [].} =
  if session.completed:
    return false
  if shard.chunkSize != session.chunkSize or shard.shardCount != session.shardCount:
    return false

  if shard.shardIndex < session.shardCount and shard.coefficients.countIt(it != 0) == 1:
    if shard.shardIndex in session.seenIndices:
      return false
    session.seenIndices.incl(shard.shardIndex)
  else:
    let hashVal = coefficientHash(shard.coefficients)
    if hashVal in session.coefficientHashes:
      return false
    session.coefficientHashes.incl(hashVal)

  session.coefficients.add(shard.coefficients)
  session.encoded.add(shard.payload)
  true

proc decodeShards(
    coefficients: seq[seq[byte]], encoded: seq[seq[byte]], shardCount: int, chunkSize: int
): Opt[seq[byte]] {.raises: [].} =
  if coefficients.len < shardCount:
    return Opt.none(seq[byte])

  var coeff = coefficients.mapIt(deepCopy(it))
  var data = encoded.mapIt(deepCopy(it))
  let rowCount = coeff.len
  var pivotRow = 0

  for pivotCol in 0 ..< shardCount:
    var row = pivotRow
    while row < rowCount and coeff[row][pivotCol] == 0:
      inc row
    if row == rowCount:
      continue
    if row != pivotRow:
      swap(coeff[row], coeff[pivotRow])
      swap(data[row], data[pivotRow])

    let pivotVal = coeff[pivotRow][pivotCol]
    if pivotVal == 0:
      continue
    let invPivot =
      try:
        gfInv(pivotVal)
      except ValueError:
        return Opt.none(seq[byte])
    for col in pivotCol ..< shardCount:
      coeff[pivotRow][col] = gfMul(coeff[pivotRow][col], invPivot)
    for j in 0 ..< data[pivotRow].len:
      data[pivotRow][j] = gfMul(data[pivotRow][j], invPivot)

    for r in 0 ..< rowCount:
      if r == pivotRow:
        continue
      let factor = coeff[r][pivotCol]
      if factor == 0:
        continue
      for c in pivotCol ..< shardCount:
        coeff[r][c] = gfAdd(coeff[r][c], gfMul(factor, coeff[pivotRow][c]))
      for j in 0 ..< data[r].len:
        data[r][j] = gfAdd(data[r][j], gfMul(factor, data[pivotRow][j]))
    inc pivotRow
    if pivotRow == shardCount:
      break

  if pivotRow < shardCount:
    return Opt.none(seq[byte])

  for i in 0 ..< shardCount:
    if coeff[i][i] != 1:
      return Opt.none(seq[byte])
    for j in 0 ..< shardCount:
      if j == i:
        continue
      if coeff[i][j] != 0:
        return Opt.none(seq[byte])

  var assembled = newSeq[byte](shardCount * chunkSize)
  for row in 0 ..< shardCount:
    let chunkStart = row * chunkSize
    copyMem(addr assembled[chunkStart], unsafeAddr data[row][0], chunkSize)

  Opt.some(assembled)

proc tryAssemble*(session: ShardSession): Opt[seq[byte]] {.raises: [].} =
  if session.completed:
    return Opt.none(seq[byte])
  let decoded = decodeShards(session.coefficients, session.encoded, session.shardCount, session.chunkSize)
  if decoded.isNone():
    return Opt.none(seq[byte])
  session.completed = true
  var assembled = decoded.get()
  if assembled.len > session.originalSize:
    assembled.setLen(session.originalSize)
  Opt.some(assembled)
