# Utility routines for SSTable binary encoding.

import pebble/sstable/types

proc encodeVarint*(value: uint64): string =
  var v = value
  var buffer = newStringOfCap(10)
  while v >= 0x80'u64:
    buffer.add(chr(int((v and 0x7f'u64) or 0x80'u64)))
    v = v shr 7
  buffer.add(chr(int(v)))
  buffer

proc appendVarint*(buffer: var string; value: uint64) =
  var v = value
  while v >= 0x80'u64:
    buffer.add(chr(int((v and 0x7f'u64) or 0x80'u64)))
    v = v shr 7
  buffer.add(chr(int(v)))

proc decodeVarint*(data: string; pos: var int): uint64 =
  var shift = 0
  var result: uint64 = 0
  while true:
    if pos >= data.len:
      raise newException(SSTError, "varint truncated")
    let byte = uint8(data[pos])
    inc pos
    result = result or (uint64(byte and 0x7f) shl shift)
    if (byte and 0x80) == 0:
      break
    shift += 7
    if shift > 63:
      raise newException(SSTError, "varint overflow")
  result

proc appendBytes*(buffer: var string; payload: string) =
  appendVarint(buffer, uint64(payload.len))
  buffer.add(payload)

proc readBytes*(data: string; pos: var int): string =
  let length = int(decodeVarint(data, pos))
  if pos + length > data.len:
    raise newException(SSTError, "payload truncated")
  result = data[pos ..< pos + length]
  pos += length
