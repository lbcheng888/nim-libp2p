# Bloom filter builder and reader for SSTables.

import std/[math, options, sequtils]
import pebble/core/types
import pebble/sstable/types
import pebble/sstable/encoding

type
  BloomFilterBuilder* = object
    bitsPerKey*: int
    keys: seq[string]

proc optimalProbes(bitsPerKey: int): int =
  let probes = int(round(float(bitsPerKey) * ln(2.0)))
  max(1, min(16, probes))

proc hashMix(key: string; salt: uint32): uint32 =
  var h = uint32(2166136261'u32)
  for ch in key:
    h = (h xor uint32(ord(ch))) * 16777619'u32
  h = h xor salt
  h = h * 16777619'u32
  h xor (h shr 16)

proc initBloomFilterBuilder*(bitsPerKey: int): BloomFilterBuilder =
  if bitsPerKey <= 0:
    raise newException(SSTError, "bitsPerKey must be positive")
  BloomFilterBuilder(bitsPerKey: bitsPerKey, keys: @[])

proc addKey*(builder: var BloomFilterBuilder; key: Key) =
  builder.keys.add(key.toBytes())

proc buildFilter(keys: openArray[string]; bitsPerKey: int): BloomFilter =
  if keys.len == 0:
    return BloomFilter(bits: @[], numBits: 0, numProbes: optimalProbes(bitsPerKey))
  let numBits = max(64, keys.len * bitsPerKey)
  let numBytes = (numBits + 7) shr 3
  var bits = newSeq[uint8](numBytes)
  let numProbes = optimalProbes(bitsPerKey)
  let salt1 = 0x47c6a7e5'u32
  let salt2 = 0x9e3779b1'u32
  for key in keys:
    let h1 = hashMix(key, salt1)
    let h2 = hashMix(key, salt2)
    for n in 0 ..< numProbes:
      let combined = uint32(h1 + uint32(n) * h2)
      let bitPos = int(combined mod uint32(numBits))
      let byteIdx = bitPos shr 3
      let mask = uint8(1 shl (bitPos and 7))
      bits[byteIdx] = bits[byteIdx] or mask
  BloomFilter(bits: bits, numBits: numBits, numProbes: numProbes)

proc finish*(builder: var BloomFilterBuilder): tuple[data: string, filter: BloomFilter] =
  let filter = buildFilter(builder.keys, builder.bitsPerKey)
  var encoded = newStringOfCap(filter.bits.len + 16)
  appendVarint(encoded, uint64(filter.numBits))
  appendVarint(encoded, uint64(filter.numProbes))
  for byte in filter.bits:
    encoded.add(chr(int(byte)))
  builder.keys.setLen(0)
  (data: encoded, filter: filter)

proc decodeBloomFilter*(data: string): BloomFilter =
  var pos = 0
  if data.len == 0:
    return BloomFilter(bits: @[], numBits: 0, numProbes: 0)
  let numBits = int(decodeVarint(data, pos))
  let numProbes = int(decodeVarint(data, pos))
  if pos > data.len:
    raise newException(SSTError, "invalid bloom filter encoding")
  let remaining = data.len - pos
  var bits = newSeq[uint8](remaining)
  for idx in 0 ..< remaining:
    bits[idx] = uint8(ord(data[pos + idx]))
  BloomFilter(bits: bits, numBits: numBits, numProbes: numProbes)

proc maybeContains*(filter: BloomFilter; key: Key): bool =
  if filter.numBits == 0 or filter.bits.len == 0:
    return true
  let salt1 = 0x47c6a7e5'u32
  let salt2 = 0x9e3779b1'u32
  let keyBytes = key.toBytes()
  let h1 = hashMix(keyBytes, salt1)
  let h2 = hashMix(keyBytes, salt2)
  for n in 0 ..< filter.numProbes:
    let combined = uint32(h1 + uint32(n) * h2)
    let bitPos = int(combined mod uint32(filter.numBits))
    let byteIdx = bitPos shr 3
    let mask = uint8(1 shl (bitPos and 7))
    if (filter.bits[byteIdx] and mask) == 0:
      return false
  true
