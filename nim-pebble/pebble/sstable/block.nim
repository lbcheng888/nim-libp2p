# Data block construction and decoding helpers.

import std/[options, sequtils, strutils]
import pebble/core/types
import pebble/sstable/types
import pebble/sstable/encoding

const prefixCompressedMagic = "\xffPC"

type
  BlockEntry = object
    key: Key
    value: string

  BlockBuildResult* = object
    payload*: string
    firstKey*: Key
    lastKey*: Key
    numEntries*: int
    rawKeySize*: int
    rawValueSize*: int
    encoding*: BlockEncoding

  BlockBuilder* = object
    entries: seq[BlockEntry]
    approxSize: int
    prefixCompression: bool

  BlockIterator* = object
    data: string
    pos: int
    encoding: BlockEncoding
    remaining: int
    lastKey: string

proc initBlockBuilder*(prefixCompression = false): BlockBuilder =
  BlockBuilder(entries: @[], approxSize: 0, prefixCompression: prefixCompression)

proc decodeLegacy(it: var BlockIterator): Option[tuple[key: Key, value: string]] =
  if it.pos >= it.data.len:
    return none(tuple[key: Key, value: string])
  let keyBytes = readBytes(it.data, it.pos)
  let valueBytes = readBytes(it.data, it.pos)
  some((key: keyBytes.toKey(), value: valueBytes))

proc decodePrefixCompressed(it: var BlockIterator): Option[tuple[key: Key, value: string]] =
  if it.remaining == 0:
    return none(tuple[key: Key, value: string])
  dec(it.remaining)
  let shared = int(decodeVarint(it.data, it.pos))
  if shared < 0 or shared > it.lastKey.len:
    raise newException(SSTError, "前缀压缩共享长度非法: " & $shared)
  let suffix = readBytes(it.data, it.pos)
  let valueBytes = readBytes(it.data, it.pos)
  var keyBytes = if shared == 0: suffix else: it.lastKey[0 ..< shared] & suffix
  it.lastKey = keyBytes
  some((key: keyBytes.toKey(), value: valueBytes))

proc isEmpty*(builder: BlockBuilder): bool =
  builder.entries.len == 0

proc estimatedSize*(builder: BlockBuilder): int =
  ## Rough estimate including varint metadata overhead.
  builder.approxSize + builder.entries.len * 5

proc add*(builder: var BlockBuilder; key: Key; value: string) =
  builder.entries.add(BlockEntry(key: key, value: value))
  builder.approxSize += key.toBytes().len + value.len

proc encodePrefixCompressed(entries: seq[BlockEntry]): string =
  var buffer = newStringOfCap(entries.len * 8)
  appendVarint(buffer, uint64(entries.len))
  var lastKey = ""
  for entry in entries:
    let keyBytes = entry.key.toBytes()
    var shared = 0
    while shared < lastKey.len and shared < keyBytes.len and lastKey[shared] == keyBytes[shared]:
      inc shared
    appendVarint(buffer, uint64(shared))
    let suffix = keyBytes[shared ..< keyBytes.len]
    appendBytes(buffer, suffix)
    appendBytes(buffer, entry.value)
    lastKey = keyBytes
  result = prefixCompressedMagic & buffer

proc finish*(builder: var BlockBuilder): BlockBuildResult =
  if builder.entries.len == 0:
    raise newException(SSTError, "cannot finish empty block")
  if builder.prefixCompression:
    result.payload = encodePrefixCompressed(builder.entries)
    result.encoding = blockEncodingPrefixCompressed
  else:
    var buffer = newStringOfCap(builder.approxSize + builder.entries.len * 8)
    for entry in builder.entries:
      appendBytes(buffer, entry.key.toBytes())
      appendBytes(buffer, entry.value)
    result.payload = buffer
    result.encoding = blockEncodingLegacy
  result.firstKey = builder.entries[0].key
  result.lastKey = builder.entries[^1].key
  result.numEntries = builder.entries.len
  result.rawKeySize = 0
  result.rawValueSize = 0
  for entry in builder.entries:
    result.rawKeySize += entry.key.toBytes().len
    result.rawValueSize += entry.value.len
  builder.entries.setLen(0)
  builder.approxSize = 0

proc initBlockIterator*(data: string): BlockIterator =
  if data.len >= prefixCompressedMagic.len and data.startsWith(prefixCompressedMagic):
    var pos = prefixCompressedMagic.len
    let count = int(decodeVarint(data, pos))
    BlockIterator(data: data, pos: pos, encoding: blockEncodingPrefixCompressed,
                  remaining: count, lastKey: "")
  else:
    BlockIterator(data: data, pos: 0, encoding: blockEncodingLegacy, remaining: -1, lastKey: "")

proc next*(it: var BlockIterator): Option[tuple[key: Key, value: string]] =
  case it.encoding
  of blockEncodingLegacy:
    decodeLegacy(it)
  of blockEncodingPrefixCompressed:
    decodePrefixCompressed(it)
