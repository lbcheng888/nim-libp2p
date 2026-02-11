# Nim-LibP2P
# Copyright (c) 2025 Status Research &
# Development GmbH
# Licensed under either of
#  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
#  * MIT license ([LICENSE-MIT](LICENSE-MIT))
# at your option.
# This file may not be copied, modified, or distributed except according to
# those terms.

{.push raises: [].}

import std/[locks]

const Primitive* = 0x11d

var tablesInitialized = false
var tablesLock: Lock

var expTable*: array[512, byte]
var logTable*: array[256, byte]

proc initTables() =
  var x = 1
  for i in 0 ..< 255:
    expTable[i] = byte(x)
    logTable[x] = byte(i)
    x = x shl 1
    if x > 0xFF:
      x = x xor Primitive
  for i in 255 ..< 512:
    expTable[i] = expTable[i - 255]

proc ensureTables() =
  if not tablesInitialized:
    acquire(tablesLock)
    if not tablesInitialized:
      initTables()
      tablesInitialized = true
    release(tablesLock)

proc gfAdd*(a, b: byte): byte {.inline.} =
  a xor b

proc gfSub*(a, b: byte): byte {.inline.} =
  a xor b

proc gfMul*(a, b: byte): byte {.inline.} =
  ensureTables()
  if a == 0 or b == 0:
    return 0
  let idx = int(logTable[a]) + int(logTable[b])
  expTable[idx]

proc gfInv*(a: byte): byte {.inline, raises: [ValueError].} =
  ensureTables()
  if a == 0:
    raise (ref ValueError)(msg: "Cannot invert zero in GF(256)")
  expTable[255 - int(logTable[a])]

proc gfDiv*(a, b: byte): byte {.inline, raises: [ValueError].} =
  if b == 0:
    raise (ref ValueError)(msg: "Division by zero in GF(256)")
  if a == 0:
    return 0
  ensureTables()
  let idx = int(logTable[a]) - int(logTable[b]) + 255
  expTable[idx]

proc gfMulScalar*(coeff: byte, data: var seq[byte]) =
  if coeff == 0:
    for i in 0 ..< data.len:
      data[i] = 0
    return
  if coeff == 1:
    return
  for i in 0 ..< data.len:
    data[i] = gfMul(coeff, data[i])

proc gfAddScaled*(dest: var seq[byte], src: seq[byte], coeff: byte) =
  if coeff == 0:
    return
  for i in 0 ..< dest.len:
    dest[i] = gfAdd(dest[i], gfMul(coeff, src[i]))

proc gfNormalizeRow*(row: var seq[byte], factor: byte) {.raises: [ValueError].} =
  let inv = gfInv(factor)
  for i in 0 ..< row.len:
    row[i] = gfMul(row[i], inv)

proc gfScaleRow*(row: var seq[byte], factor: byte) =
  for i in 0 ..< row.len:
    row[i] = gfMul(row[i], factor)

proc gfRowCombine*(target: var seq[byte], source: seq[byte], factor: byte) =
  if factor == 0:
    return
  for i in 0 ..< target.len:
    target[i] = gfAdd(target[i], gfMul(factor, source[i]))

once initLock(tablesLock)
