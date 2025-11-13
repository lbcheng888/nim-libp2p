# SSTable reader capable of decoding blocks, metadata, and Bloom filters.

import std/[base64, options, strutils]
import pebble/core/types
import "pebble/sstable/block"
import pebble/sstable/encoding
import pebble/sstable/filter
import pebble/sstable/types
import pebble/sstable/cache

const footerMagic = "SSTN"

type
  TableReader* = ref object
    comparator: Comparator
    image: SSTableImage
    footer: TableFooter
    indexEntries: seq[IndexEntry]
    bloom: Option[BloomFilter]
    props: TableProperties
    cache: BlockCache
    cacheNamespace: string

var readerIdCounter = 0

proc nextReaderNamespace(): string =
  inc(readerIdCounter)
  "sst-reader-" & $readerIdCounter

proc cacheKey(reader: TableReader; handle: BlockHandle): string =
  reader.cacheNamespace & ":" & $handle.offset & ":" & $handle.length

proc validateHandle(totalLen: int; handle: BlockHandle; label: string) =
  if handle.offset < 0 or handle.length < 0:
    raise newException(SSTError,
      label & " 区块句柄出现负偏移或负长度")
  if handle.offset > totalLen or handle.length > totalLen:
    raise newException(SSTError,
      label & " 区块句柄超出表范围")
  let endPos = handle.offset + handle.length
  if endPos > totalLen:
    raise newException(SSTError,
      label & " 区块句柄长度越界")

proc sliceForHandle(data: string; handle: BlockHandle; label: string): string =
  validateHandle(data.len, handle, label)
  if handle.length == 0:
    return ""
  data[handle.offset ..< handle.offset + handle.length]

proc loadBlock(reader: TableReader; handle: BlockHandle; label: string): string =
  if not reader.cache.isNil():
    let key = reader.cacheKey(handle)
    let cached = reader.cache.get(key)
    if cached.isSome():
      return cached.get()
    let slice = sliceForHandle(reader.image.buffer, handle, label)
    reader.cache.put(key, slice)
    return slice
  sliceForHandle(reader.image.buffer, handle, label)

proc decodeFooter(data: string): TableFooter =
  if data.len < footerMagic.len + 4:
    raise newException(SSTError, "sstable too small for footer")
  let magicOffset = data.len - footerMagic.len
  if data[magicOffset ..< data.len] != footerMagic:
    raise newException(SSTError, "footer magic mismatch")
  let lengthOffset = magicOffset - 4
  if lengthOffset < 0:
    raise newException(SSTError, "footer corrupted")
  var payloadLen = 0
  for i in 0 ..< 4:
    payloadLen = payloadLen or (ord(data[lengthOffset + i]) shl (i * 8))
  let payloadStart = lengthOffset - payloadLen
  if payloadStart < 0:
    raise newException(SSTError, "footer length out of bounds")
  if payloadStart + payloadLen != lengthOffset:
    raise newException(SSTError, "footer payload length mismatch")
  var pos = payloadStart
  let indexOffset = int(decodeVarint(data, pos))
  let indexLength = int(decodeVarint(data, pos))
  let hasFilter = decodeVarint(data, pos)
  var filterHandle: Option[BlockHandle] = none(BlockHandle)
  if hasFilter == 1:
    let filterOffset = int(decodeVarint(data, pos))
    let filterLength = int(decodeVarint(data, pos))
    filterHandle = some(initBlockHandle(filterOffset, filterLength))
  let metaOffset = int(decodeVarint(data, pos))
  let metaLength = int(decodeVarint(data, pos))
  TableFooter(indexHandle: initBlockHandle(indexOffset, indexLength),
              filterHandle: filterHandle,
              metaHandle: initBlockHandle(metaOffset, metaLength))

proc decodeIndex(data: string): seq[IndexEntry] =
  var pos = 0
  let count = int(decodeVarint(data, pos))
  for _ in 0 ..< count:
    let separatorBytes = readBytes(data, pos)
    let offset = int(decodeVarint(data, pos))
    let length = int(decodeVarint(data, pos))
    result.add(IndexEntry(separator: separatorBytes.toKey(),
                          handle: initBlockHandle(offset, length)))

proc decodeProperties(data: string): TableProperties =
  var pos = 0
  let count = int(decodeVarint(data, pos))
  for _ in 0 ..< count:
    let key = readBytes(data, pos)
    let value = readBytes(data, pos)
    case key
    of "num_entries":
      result.numEntries = value.parseInt()
    of "raw_key_size":
      result.rawKeySize = value.parseInt()
    of "raw_value_size":
      result.rawValueSize = value.parseInt()
    of "smallest_key":
      result.smallestKey = some(value.decode().toKey())
    of "largest_key":
      result.largestKey = some(value.decode().toKey())
    of "bloom_bits_per_key":
      result.bloomBitsPerKey = some(value.parseInt())
    else:
      discard

proc newTableReader*(data: string; comparator: Comparator = compareBytewise;
                     cache: BlockCache = nil): TableReader =
  let footer = decodeFooter(data)
  validateHandle(data.len, footer.indexHandle, "index")
  let indexSlice = sliceForHandle(data, footer.indexHandle, "index")
  let indexEntries = decodeIndex(indexSlice)
  var sections: seq[TableSection] = @[]
  for entry in indexEntries:
    validateHandle(data.len, entry.handle, "data")
    sections.add(TableSection(kind: blockData, handle: entry.handle))
  sections.add(TableSection(kind: blockIndex, handle: footer.indexHandle))
  var bloom: Option[BloomFilter] = none(BloomFilter)
  if footer.filterHandle.isSome():
    let handle = footer.filterHandle.get()
    let filterSlice = sliceForHandle(data, handle, "filter")
    bloom = some(decodeBloomFilter(filterSlice))
    sections.add(TableSection(kind: blockFilter, handle: handle))
  validateHandle(data.len, footer.metaHandle, "meta")
  let metaSlice = sliceForHandle(data, footer.metaHandle, "meta")
  sections.add(TableSection(kind: blockMeta, handle: footer.metaHandle))
  let props = decodeProperties(metaSlice)
  TableReader(
    comparator: comparator,
    image: SSTableImage(buffer: data, sections: sections),
    footer: footer,
    indexEntries: indexEntries,
    bloom: bloom,
    props: props,
    cache: cache,
    cacheNamespace: nextReaderNamespace()
  )

proc properties*(reader: TableReader): TableProperties =
  reader.props

proc maybeContains*(reader: TableReader; key: Key): bool =
  if reader.bloom.isSome():
    return reader.bloom.get().maybeContains(key)
  true

proc locateBlock(reader: TableReader; key: Key): Option[BlockHandle] =
  if reader.indexEntries.len == 0:
    return none(BlockHandle)
  var lo = 0
  var hi = reader.indexEntries.len - 1
  var candidate: Option[BlockHandle] = none(BlockHandle)
  while lo <= hi:
    let mid = (lo + hi) shr 1
    let entry = reader.indexEntries[mid]
    let cmp = reader.comparator(key, entry.separator)
    if cmp <= 0:
      candidate = some(entry.handle)
      if mid == 0:
        break
      hi = mid - 1
    else:
      lo = mid + 1
  candidate

proc get*(reader: TableReader; key: Key): Option[string] =
  if not reader.maybeContains(key):
    return none(string)
  let blockHandleOpt = reader.locateBlock(key)
  if blockHandleOpt.isNone():
    return none(string)
  let handle = blockHandleOpt.get()
  let slice = reader.loadBlock(handle, "data")
  var blockIter = initBlockIterator(slice)
  while true:
    let nextEntry = blockIter.next()
    if nextEntry.isNone():
      break
    let entry = nextEntry.get()
    let cmp = reader.comparator(entry.key, key)
    if cmp == 0:
      return some(entry.value)
    if cmp > 0:
      break
  none(string)

iterator scan*(reader: TableReader): tuple[key: Key, value: string] =
  ## 顺序扫过数据块，返回键值对序列。
  for entry in reader.indexEntries:
    let handle = entry.handle
    let slice = reader.loadBlock(handle, "data")
    var blockIter = initBlockIterator(slice)
    while true:
      let item = blockIter.next()
      if item.isNone():
        break
      let kv = item.get()
      yield (key: kv.key, value: kv.value)

proc tableSize*(reader: TableReader): int =
  ## 返回 SSTable 字节总大小。
  reader.image.buffer.len

proc sections*(reader: TableReader): seq[TableSection] =
  ## 返回表内区块描述。调用方如需修改请自行拷贝。
  reader.image.sections
