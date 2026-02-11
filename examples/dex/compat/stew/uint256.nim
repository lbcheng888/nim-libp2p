import std/[strutils]
import stew/byteutils

type UInt256* = array[32, byte]

proc fromHex*(T: type UInt256, s: string): UInt256 =
  try:
    var res: UInt256
    var hexStr = s
    if hexStr.startsWith("0x"):
      hexStr = hexStr[2..^1]
    
    let bytes = hexToSeqByte(hexStr)
    # Fill from the end (Big Endian)
    let len = min(bytes.len, 32)
    for i in 0 ..< len:
      res[31 - i] = bytes[bytes.len - 1 - i]
    res
  except:
    default(UInt256)

proc toHex*(val: UInt256): string =
  byteutils.toHex(val)

proc `+`*(a, b: UInt256): UInt256 =
  var carry = 0u16
  var res: UInt256
  for i in countdown(31, 0):
    let sum = a[i].uint16 + b[i].uint16 + carry
    res[i] = (sum and 0xFF).byte
    carry = sum shr 8
  res

proc `-`*(a, b: UInt256): UInt256 =
  var borrow = 0
  var res: UInt256
  for i in countdown(31, 0):
    var valA = a[i].int
    var valB = b[i].int
    var diff = valA - valB - borrow
    if diff < 0:
      diff += 256
      borrow = 1
    else:
      borrow = 0
    res[i] = diff.byte
  res

proc `u256`*(n: SomeInteger): UInt256 =
  var res: UInt256
  var v = n.uint64
  for i in 0..7:
    res[31-i] = (v and 0xFF).byte
    v = v shr 8
  res

# Mock big endian helpers used by some clients
proc fromDecimal*(T: type UInt256, s: string): UInt256 =
  # Very basic decimal parsing (ignoring overflow)
  # In real prod this needs full bigint logic
  try:
    let v = parseBiggestUInt(s)
    return v.u256
  except:
    return 0.u256

