# Nim-LibP2P
# Copyright (c) 2025 Status Research & Development GmbH
# Licensed under either of
#  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
#  * MIT license ([LICENSE-MIT](LICENSE-MIT))
# at your option.
# This file may not be copied, modified, or distributed except according to
# those terms.

{.push raises: [].}

import std/bitops

const
  BlockSize* = 64
  rounds = 20
  Sigma* = [byte('e'), byte('x'), byte('p'), byte('a'), byte('n'), byte('d'), byte(' '),
            byte('3'), byte('2'), byte('-'), byte('b'), byte('y'), byte('t'), byte('e'),
            byte(' '), byte('k')]

type
  XSalsa20Stream* = object
    key: array[32, byte]
    nonce: array[8, byte]
    buffer: array[BlockSize, byte]
    bufferIndex: int
    counter: uint64

proc load32(data: openArray[byte], offset: int): uint32 {.inline.} =
  uint32(data[offset]) or (uint32(data[offset + 1]) shl 8) or
    (uint32(data[offset + 2]) shl 16) or (uint32(data[offset + 3]) shl 24)

proc store32(data: var openArray[byte], offset: int, value: uint32) {.inline.} =
  data[offset] = byte(value and 0xff'u32)
  data[offset + 1] = byte((value shr 8) and 0xff'u32)
  data[offset + 2] = byte((value shr 16) and 0xff'u32)
  data[offset + 3] = byte((value shr 24) and 0xff'u32)

proc coreBlock(input: array[16, byte], key: array[32, byte], constant: array[16, byte]): array[BlockSize, byte] =
  var
    j0 = load32(constant, 0)
    j1 = load32(key, 0)
    j2 = load32(key, 4)
    j3 = load32(key, 8)
    j4 = load32(key, 12)
    j5 = load32(constant, 4)
    j6 = load32(input, 0)
    j7 = load32(input, 4)
    j8 = load32(input, 8)
    j9 = load32(input, 12)
    j10 = load32(constant, 8)
    j11 = load32(key, 16)
    j12 = load32(key, 20)
    j13 = load32(key, 24)
    j14 = load32(key, 28)
    j15 = load32(constant, 12)

  var
    x0 = j0
    x1 = j1
    x2 = j2
    x3 = j3
    x4 = j4
    x5 = j5
    x6 = j6
    x7 = j7
    x8 = j8
    x9 = j9
    x10 = j10
    x11 = j11
    x12 = j12
    x13 = j13
    x14 = j14
    x15 = j15

  for _ in countup(0, rounds - 1, 2):
    var u = x0 + x12
    x4 = x4 xor rotateLeftBits(u, 7)
    u = x4 + x0
    x8 = x8 xor rotateLeftBits(u, 9)
    u = x8 + x4
    x12 = x12 xor rotateLeftBits(u, 13)
    u = x12 + x8
    x0 = x0 xor rotateLeftBits(u, 18)

    u = x5 + x1
    x9 = x9 xor rotateLeftBits(u, 7)
    u = x9 + x5
    x13 = x13 xor rotateLeftBits(u, 9)
    u = x13 + x9
    x1 = x1 xor rotateLeftBits(u, 13)
    u = x1 + x13
    x5 = x5 xor rotateLeftBits(u, 18)

    u = x10 + x6
    x14 = x14 xor rotateLeftBits(u, 7)
    u = x14 + x10
    x2 = x2 xor rotateLeftBits(u, 9)
    u = x2 + x14
    x6 = x6 xor rotateLeftBits(u, 13)
    u = x6 + x2
    x10 = x10 xor rotateLeftBits(u, 18)

    u = x15 + x11
    x3 = x3 xor rotateLeftBits(u, 7)
    u = x3 + x15
    x7 = x7 xor rotateLeftBits(u, 9)
    u = x7 + x3
    x11 = x11 xor rotateLeftBits(u, 13)
    u = x11 + x7
    x15 = x15 xor rotateLeftBits(u, 18)

    u = x0 + x3
    x1 = x1 xor rotateLeftBits(u, 7)
    u = x1 + x0
    x2 = x2 xor rotateLeftBits(u, 9)
    u = x2 + x1
    x3 = x3 xor rotateLeftBits(u, 13)
    u = x3 + x2
    x0 = x0 xor rotateLeftBits(u, 18)

    u = x5 + x4
    x6 = x6 xor rotateLeftBits(u, 7)
    u = x6 + x5
    x7 = x7 xor rotateLeftBits(u, 9)
    u = x7 + x6
    x4 = x4 xor rotateLeftBits(u, 13)
    u = x4 + x7
    x5 = x5 xor rotateLeftBits(u, 18)

    u = x10 + x9
    x11 = x11 xor rotateLeftBits(u, 7)
    u = x11 + x10
    x8 = x8 xor rotateLeftBits(u, 9)
    u = x8 + x11
    x9 = x9 xor rotateLeftBits(u, 13)
    u = x9 + x8
    x10 = x10 xor rotateLeftBits(u, 18)

    u = x15 + x14
    x12 = x12 xor rotateLeftBits(u, 7)
    u = x12 + x15
    x13 = x13 xor rotateLeftBits(u, 9)
    u = x13 + x12
    x14 = x14 xor rotateLeftBits(u, 13)
    u = x14 + x13
    x15 = x15 xor rotateLeftBits(u, 18)
  x0 += j0
  x1 += j1
  x2 += j2
  x3 += j3
  x4 += j4
  x5 += j5
  x6 += j6
  x7 += j7
  x8 += j8
  x9 += j9
  x10 += j10
  x11 += j11
  x12 += j12
  x13 += j13
  x14 += j14
  x15 += j15

  store32(result, 0, x0)
  store32(result, 4, x1)
  store32(result, 8, x2)
  store32(result, 12, x3)
  store32(result, 16, x4)
  store32(result, 20, x5)
  store32(result, 24, x6)
  store32(result, 28, x7)
  store32(result, 32, x8)
  store32(result, 36, x9)
  store32(result, 40, x10)
  store32(result, 44, x11)
  store32(result, 48, x12)
  store32(result, 52, x13)
  store32(result, 56, x14)
  store32(result, 60, x15)

proc hsalsa20(input: array[16, byte], key: array[32, byte], constant: array[16, byte]): array[32, byte] =
  var
    x0 = load32(constant, 0)
    x1 = load32(key, 0)
    x2 = load32(key, 4)
    x3 = load32(key, 8)
    x4 = load32(key, 12)
    x5 = load32(constant, 4)
    x6 = load32(input, 0)
    x7 = load32(input, 4)
    x8 = load32(input, 8)
    x9 = load32(input, 12)
    x10 = load32(constant, 8)
    x11 = load32(key, 16)
    x12 = load32(key, 20)
    x13 = load32(key, 24)
    x14 = load32(key, 28)
    x15 = load32(constant, 12)

  for _ in countup(0, rounds - 1, 2):
    var u = x0 + x12
    x4 = x4 xor rotateLeftBits(u, 7)
    u = x4 + x0
    x8 = x8 xor rotateLeftBits(u, 9)
    u = x8 + x4
    x12 = x12 xor rotateLeftBits(u, 13)
    u = x12 + x8
    x0 = x0 xor rotateLeftBits(u, 18)

    u = x5 + x1
    x9 = x9 xor rotateLeftBits(u, 7)
    u = x9 + x5
    x13 = x13 xor rotateLeftBits(u, 9)
    u = x13 + x9
    x1 = x1 xor rotateLeftBits(u, 13)
    u = x1 + x13
    x5 = x5 xor rotateLeftBits(u, 18)

    u = x10 + x6
    x14 = x14 xor rotateLeftBits(u, 7)
    u = x14 + x10
    x2 = x2 xor rotateLeftBits(u, 9)
    u = x2 + x14
    x6 = x6 xor rotateLeftBits(u, 13)
    u = x6 + x2
    x10 = x10 xor rotateLeftBits(u, 18)

    u = x15 + x11
    x3 = x3 xor rotateLeftBits(u, 7)
    u = x3 + x15
    x7 = x7 xor rotateLeftBits(u, 9)
    u = x7 + x3
    x11 = x11 xor rotateLeftBits(u, 13)
    u = x11 + x7
    x15 = x15 xor rotateLeftBits(u, 18)

    u = x0 + x3
    x1 = x1 xor rotateLeftBits(u, 7)
    u = x1 + x0
    x2 = x2 xor rotateLeftBits(u, 9)
    u = x2 + x1
    x3 = x3 xor rotateLeftBits(u, 13)
    u = x3 + x2
    x0 = x0 xor rotateLeftBits(u, 18)

    u = x5 + x4
    x6 = x6 xor rotateLeftBits(u, 7)
    u = x6 + x5
    x7 = x7 xor rotateLeftBits(u, 9)
    u = x7 + x6
    x4 = x4 xor rotateLeftBits(u, 13)
    u = x4 + x7
    x5 = x5 xor rotateLeftBits(u, 18)

    u = x10 + x9
    x11 = x11 xor rotateLeftBits(u, 7)
    u = x11 + x10
    x8 = x8 xor rotateLeftBits(u, 9)
    u = x8 + x11
    x9 = x9 xor rotateLeftBits(u, 13)
    u = x9 + x8
    x10 = x10 xor rotateLeftBits(u, 18)

    u = x15 + x14
    x12 = x12 xor rotateLeftBits(u, 7)
    u = x12 + x15
    x13 = x13 xor rotateLeftBits(u, 9)
    u = x13 + x12
    x14 = x14 xor rotateLeftBits(u, 13)
    u = x14 + x13
    x15 = x15 xor rotateLeftBits(u, 18)

  store32(result, 0, x0)
  store32(result, 4, x5)
  store32(result, 8, x10)
  store32(result, 12, x15)
  store32(result, 16, x6)
  store32(result, 20, x7)
  store32(result, 24, x8)
  store32(result, 28, x9)

proc initStream(
    stream: var XSalsa20Stream, key: array[32, byte], nonce: array[24, byte]
) {.raises: [].} =
  stream.bufferIndex = BlockSize
  stream.counter = 0
  for i in 0 ..< BlockSize:
    stream.buffer[i] = 0

  var hNonce: array[16, byte]
  for i in 0 ..< 16:
    hNonce[i] = nonce[i]
  let subKey = hsalsa20(hNonce, key, Sigma)
  stream.key = subKey
  for i in 0 ..< 8:
    stream.nonce[i] = nonce[16 + i]

proc initStream(
    stream: var XSalsa20Stream, key: array[32, byte], nonce: array[8, byte]
) {.raises: [].} =
  stream.bufferIndex = BlockSize
  stream.counter = 0
  for i in 0 ..< BlockSize:
    stream.buffer[i] = 0

  stream.key = key
  for i in 0 ..< 8:
    stream.nonce[i] = nonce[i]

proc initStream(
    stream: var XSalsa20Stream, key: array[32, byte], nonce: openArray[byte]
) {.raises: [ValueError].} =
  case nonce.len
  of 24:
    var tmp: array[24, byte]
    for i in 0 ..< 24:
      tmp[i] = nonce[i]
    stream.initStream(key, tmp)
  of 8:
    var tmp: array[8, byte]
    for i in 0 ..< 8:
      tmp[i] = nonce[i]
    stream.initStream(key, tmp)
  else:
    raise (ref ValueError)(msg: "nonce must be 8 or 24 bytes long for XSalsa20")

proc refill(stream: var XSalsa20Stream) =
  var input: array[16, byte]
  for i in 0 ..< 8:
    input[i] = stream.nonce[i]
  var tmp = stream.counter
  for i in 8 ..< 16:
    input[i] = byte(tmp and 0xff'u64)
    tmp = tmp shr 8
  stream.buffer = coreBlock(input, stream.key, Sigma)
  stream.bufferIndex = 0
  inc stream.counter

proc nextKeystreamByte(stream: var XSalsa20Stream): byte =
  if stream.bufferIndex >= BlockSize:
    stream.refill()
  result = stream.buffer[stream.bufferIndex]
  inc stream.bufferIndex
  if stream.bufferIndex >= BlockSize:
    stream.bufferIndex = BlockSize

proc xorKeyStream*(
    stream: var XSalsa20Stream, dst: var openArray[byte], src: openArray[byte]
) =
  doAssert dst.len >= src.len, "destination buffer too short"

  if src.len == 0:
    return

  if stream.bufferIndex >= BlockSize:
    stream.refill()

  for i in 0 ..< src.len:
    let keyByte = stream.nextKeystreamByte()
    dst[i] = src[i] xor keyByte

proc initXSalsa20Stream*(key: array[32, byte], nonce: array[24, byte]): XSalsa20Stream =
  var stream: XSalsa20Stream
  stream.initStream(key, nonce)
  stream

proc xorKeyStream*(stream: var XSalsa20Stream, data: var openArray[byte]) =
  stream.xorKeyStream(data, data)
