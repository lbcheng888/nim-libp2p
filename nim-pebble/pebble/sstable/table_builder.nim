# SSTable builder composing data blocks, index, filter, and metadata.

import std/[base64, math, options, sequtils]
import pebble/core/types
import "pebble/sstable/block"
import pebble/sstable/encoding
import pebble/sstable/filter
import pebble/sstable/types

const footerMagic = "SSTN"

type
  TableBuilder* = object
    comparator: Comparator
    blockSize: int
    buffer: string
    currentBlock: BlockBuilder
    prefixCompression: bool
    lastKey: Option[Key]
    sections: seq[TableSection]
    indexEntries: seq[IndexEntry]
    filterEnabled: bool
    filterBuilder: BloomFilterBuilder
    properties: TableProperties
    filterBitsPerKey: int
    pendingHandle: Option[BlockHandle]
    pendingLastKey: Option[Key]

proc initTableBuilder*(comparator: Comparator = compareBytewise;
                       blockSize = 32 * 1024;
                       bloomBitsPerKey = 10;
                       enablePrefixCompression = true): TableBuilder =
  if blockSize <= 0:
    raise newException(SSTError, "blockSize must be positive")
  result.comparator = comparator
  result.blockSize = blockSize
  result.buffer = ""
  result.prefixCompression = enablePrefixCompression
  result.currentBlock = initBlockBuilder(enablePrefixCompression)
  result.lastKey = none(Key)
  result.sections = @[]
  result.indexEntries = @[]
  result.properties = TableProperties()
  result.filterEnabled = bloomBitsPerKey > 0
  if result.filterEnabled:
    result.filterBuilder = initBloomFilterBuilder(bloomBitsPerKey)
    result.filterBitsPerKey = bloomBitsPerKey
    result.properties.bloomBitsPerKey = some(bloomBitsPerKey)
  result.pendingHandle = none(BlockHandle)
  result.pendingLastKey = none(Key)

proc updateProperties(builder: var TableBuilder; key: Key; value: string) =
  builder.properties.numEntries.inc()
  builder.properties.rawKeySize += key.toBytes().len
  builder.properties.rawValueSize += value.len
  if builder.properties.smallestKey.isNone():
    builder.properties.smallestKey = some(key)
  builder.properties.largestKey = some(key)

proc encodeIndex(entries: seq[IndexEntry]): string =
  var buffer = newStringOfCap(entries.len * 24)
  appendVarint(buffer, uint64(entries.len))
  for entry in entries:
    appendBytes(buffer, entry.separator.toBytes())
    appendVarint(buffer, uint64(entry.handle.offset))
    appendVarint(buffer, uint64(entry.handle.length))
  buffer

proc encodeProperties(props: TableProperties): string =
  var records: seq[tuple[key: string, value: string]] = @[]
  records.add(("num_entries", $props.numEntries))
  records.add(("raw_key_size", $props.rawKeySize))
  records.add(("raw_value_size", $props.rawValueSize))
  if props.smallestKey.isSome():
    records.add(("smallest_key", props.smallestKey.get().toBytes().encode()))
  if props.largestKey.isSome():
    records.add(("largest_key", props.largestKey.get().toBytes().encode()))
  if props.bloomBitsPerKey.isSome():
    records.add(("bloom_bits_per_key", $props.bloomBitsPerKey.get()))
  var buffer = newStringOfCap(records.len * 32)
  appendVarint(buffer, uint64(records.len))
  for record in records:
    appendBytes(buffer, record.key)
    appendBytes(buffer, record.value)
  buffer

proc encodeFooter(indexHandle: BlockHandle; filterHandle: Option[BlockHandle];
                  metaHandle: BlockHandle): string =
  var payload = newStringOfCap(32)
  appendVarint(payload, uint64(indexHandle.offset))
  appendVarint(payload, uint64(indexHandle.length))
  if filterHandle.isSome():
    appendVarint(payload, 1)
    appendVarint(payload, uint64(filterHandle.get().offset))
    appendVarint(payload, uint64(filterHandle.get().length))
  else:
    appendVarint(payload, 0)
  appendVarint(payload, uint64(metaHandle.offset))
  appendVarint(payload, uint64(metaHandle.length))
  var buffer = payload
  let length = uint32(payload.len)
  for shift in 0 ..< 4:
    buffer.add(chr(int((length shr (shift * 8)) and 0xff)))
  buffer.add(footerMagic)
  buffer

proc shortestSeparator(a, b: Key): Key =
  let left = a.toBytes()
  let right = b.toBytes()
  let limit = min(left.len, right.len)
  var idx = 0
  while idx < limit and left[idx] == right[idx]:
    inc idx
  if idx < limit:
    let current = uint8(left[idx])
    let target = uint8(right[idx])
    if current < 0xff'u8 and current + 1 < target:
      var candidate = left[0 .. idx]
      candidate[candidate.high] = chr(int(current + 1))
      return candidate.toKey()
  a

proc shortestSuccessor(a: Key): Key =
  var bytes = a.toBytes()
  for idx in countdown(bytes.len - 1, 0):
    let value = uint8(bytes[idx])
    if value < 0xff'u8:
      bytes[idx] = chr(int(value + 1))
      bytes.setLen(idx + 1)
      return bytes.toKey()
  bytes.add(chr(0))
  bytes.toKey()

proc emitPendingIndex(builder: var TableBuilder; nextKey: Key) =
  if builder.pendingHandle.isNone() or builder.pendingLastKey.isNone():
    return
  let separator = shortestSeparator(builder.pendingLastKey.get(), nextKey)
  builder.indexEntries.add(IndexEntry(separator: separator,
                                      handle: builder.pendingHandle.get()))
  builder.pendingHandle = none(BlockHandle)
  builder.pendingLastKey = none(Key)

proc emitFinalPendingIndex(builder: var TableBuilder) =
  if builder.pendingHandle.isNone() or builder.pendingLastKey.isNone():
    return
  let successor = shortestSuccessor(builder.pendingLastKey.get())
  builder.indexEntries.add(IndexEntry(separator: successor,
                                      handle: builder.pendingHandle.get()))
  builder.pendingHandle = none(BlockHandle)
  builder.pendingLastKey = none(Key)

proc flushCurrentBlock(builder: var TableBuilder) =
  if builder.currentBlock.isEmpty():
    return
  let result = builder.currentBlock.finish()
  let offset = builder.buffer.len
  builder.buffer.add(result.payload)
  let handle = initBlockHandle(offset, result.payload.len)
  builder.sections.add(TableSection(kind: blockData, handle: handle))
  builder.pendingHandle = some(handle)
  builder.pendingLastKey = some(result.lastKey)

proc add*(builder: var TableBuilder; key: Key; value: string) =
  if builder.pendingHandle.isSome():
    builder.emitPendingIndex(key)
  if builder.lastKey.isSome():
    if builder.comparator(builder.lastKey.get(), key) >= 0:
      raise newException(SSTError, "keys must be added in strictly increasing order")
  builder.currentBlock.add(key, value)
  builder.lastKey = some(key)
  builder.updateProperties(key, value)
  if builder.filterEnabled:
    builder.filterBuilder.addKey(key)
  if builder.currentBlock.estimatedSize() >= builder.blockSize:
    builder.flushCurrentBlock()

proc finish*(builder: var TableBuilder): TableBuildArtifacts =
  builder.flushCurrentBlock()
  builder.emitFinalPendingIndex()
  if builder.indexEntries.len == 0:
    raise newException(SSTError, "table has no data blocks")
  let indexOffset = builder.buffer.len
  let indexBlock = encodeIndex(builder.indexEntries)
  builder.buffer.add(indexBlock)
  let indexHandle = initBlockHandle(indexOffset, indexBlock.len)
  builder.sections.add(TableSection(kind: blockIndex, handle: indexHandle))
  var bloomData = ""
  var bloomFilter: Option[BloomFilter] = none(BloomFilter)
  if builder.filterEnabled:
    let bloomResult = builder.filterBuilder.finish()
    bloomData = bloomResult.data
    bloomFilter = some(bloomResult.filter)
    let filterOffset = builder.buffer.len
    builder.buffer.add(bloomData)
    let filterHandle = initBlockHandle(filterOffset, bloomData.len)
    builder.sections.add(TableSection(kind: blockFilter, handle: filterHandle))
  let metaOffset = builder.buffer.len
  let metaBlock = encodeProperties(builder.properties)
  builder.buffer.add(metaBlock)
  let metaHandle = initBlockHandle(metaOffset, metaBlock.len)
  builder.sections.add(TableSection(kind: blockMeta, handle: metaHandle))
  var footerFilterHandle: Option[BlockHandle] = none(BlockHandle)
  if builder.filterEnabled:
    let filterSection = builder.sections.filterIt(it.kind == blockFilter)
    if filterSection.len > 0:
      footerFilterHandle = some(filterSection[0].handle)
  let footer = encodeFooter(indexHandle, footerFilterHandle, metaHandle)
  builder.buffer.add(footer)
  result.image = SSTableImage(buffer: builder.buffer, sections: builder.sections)
  result.footer = TableFooter(indexHandle: indexHandle,
                              filterHandle: footerFilterHandle,
                              metaHandle: metaHandle)
  result.properties = builder.properties
  result.bloom = bloomFilter
  result.indexEntries = builder.indexEntries
