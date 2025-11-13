# WAL 读取器：解析 chunk、校验 CRC，并提供顺序记录遍历能力。

import std/[options, os, sequtils, strformat]

import pebble/wal/[checksum, types]

type
  WalChunk = object
    chunkType: ChunkType
    data: seq[byte]
    metadata: WalRecordMetadata

  WalReader* = ref object
    path*: string
    file: File
    blockOffset: int
    tailTolerance: bool
    eof: bool
    offset: int64

proc readFully(file: File; buffer: var seq[byte]): int =
  var total = 0
  while total < buffer.len:
    let readNow = file.readBuffer(unsafeAddr buffer[total], buffer.len - total)
    if readNow <= 0:
      break
    total += readNow
  total

proc skipBytes(reader: WalReader; count: int): bool =
  if count <= 0:
    return true
  var tmp = newSeq[byte](count)
  let readNow = readFully(reader.file, tmp)
  reader.offset += readNow
  reader.blockOffset = 0
  if readNow < count:
    reader.eof = true
    return false
  true

proc parseUint32(buf: seq[byte]; idx: int): uint32 =
  uint32(buf[idx]) or (uint32(buf[idx + 1]) shl 8) or
    (uint32(buf[idx + 2]) shl 16) or (uint32(buf[idx + 3]) shl 24)

proc parseUint64(buf: seq[byte]; idx: int): uint64 =
  var result = uint64(0)
  var i = 0
  while i < 8:
    result = result or (uint64(buf[idx + i]) shl (8 * i))
    inc i
  result

proc nextChunk(reader: WalReader; chunk: var WalChunk): bool =
  if reader.eof:
    return false
  let minHeader = legacyHeaderSize
  while true:
    let remaining = blockSize - reader.blockOffset
    if remaining < minHeader:
      if not reader.skipBytes(remaining):
        return false
      reader.blockOffset = 0
      continue
    var header = newSeq[byte](legacyHeaderSize)
    let readHeader = readFully(reader.file, header)
    if readHeader == 0:
      reader.eof = true
      return false
    if readHeader < legacyHeaderSize:
      if reader.tailTolerance:
        reader.eof = true
        return false
      raise newException(WalError,
        fmt"WAL truncated header at offset {reader.offset}")
    reader.blockOffset += legacyHeaderSize
    reader.offset += legacyHeaderSize
    let chunkTypeVal = header[6]
    let chunkType = ChunkType(chunkTypeVal)
    let formatInfo = chunkFormatTable[chunkType]
    if formatInfo.headerSize == 0:
      # 遇到填充或未知类型，尝试跳过当前块剩余字节。
      if reader.tailTolerance:
        discard reader.skipBytes(blockSize - reader.blockOffset)
        continue
      raise newException(WalError,
        fmt"unknown WAL chunk type {chunkTypeVal} at offset {reader.offset}")
    var fullHeader = header
    if formatInfo.headerSize > legacyHeaderSize:
      let extraSize = formatInfo.headerSize - legacyHeaderSize
      var extra = newSeq[byte](extraSize)
      let readExtra = readFully(reader.file, extra)
      reader.blockOffset += readExtra
      reader.offset += readExtra
      if readExtra < extraSize:
        if reader.tailTolerance:
          reader.eof = true
          return false
        raise newException(WalError,
          fmt"WAL truncated extended header at offset {reader.offset}")
      fullHeader.add(extra)
    let payloadLen = int(fullHeader[4]) or (int(fullHeader[5]) shl 8)
    if payloadLen < 0 or payloadLen > blockSize - formatInfo.headerSize:
      if reader.tailTolerance:
        reader.eof = true
        return false
      raise newException(WalError,
        fmt"invalid chunk length {payloadLen} at offset {reader.offset}")
    var payload = newSeq[byte](payloadLen)
    if payloadLen > 0:
      let readPayload = readFully(reader.file, payload)
      reader.blockOffset += readPayload
      reader.offset += readPayload
      if readPayload < payloadLen:
        if reader.tailTolerance:
          reader.eof = true
          return false
        raise newException(WalError,
          fmt"WAL truncated payload at offset {reader.offset}")
    var metadata = WalRecordMetadata()
    var idx = 7
    if formatInfo.format.isReusable():
      metadata.logNumber = parseUint32(fullHeader, idx)
      idx += 4
    if formatInfo.format.hasSyncOffset():
      metadata.syncOffset = parseUint64(fullHeader, idx)
      idx += 8
    let storedChecksum = parseUint32(fullHeader, 0)
    var crc = extend(0'u32, fromByte(chunkTypeVal))
    if formatInfo.format.isReusable():
      crc = extend(crc, fromUInt32(metadata.logNumber))
    if formatInfo.format.hasSyncOffset():
      crc = extend(crc, fromUInt64(metadata.syncOffset))
    if payloadLen > 0:
      crc = extend(crc, payload)
    let masked = maskChecksum(crc)
    if masked != storedChecksum:
      raise newException(WalError,
        fmt"checksum mismatch at offset {reader.offset - int64(formatInfo.headerSize + payloadLen)}")
    chunk.chunkType = chunkType
    chunk.data = payload
    chunk.metadata = metadata
    if reader.blockOffset >= blockSize:
      reader.blockOffset = reader.blockOffset mod blockSize
    return true

proc newWalReader*(path: string; tailTolerance = true): WalReader =
  var f: File
  if not open(f, path, fmRead):
    raise newException(WalError, "unable to open WAL for reading: " & path)
  new(result)
  result.path = path
  result.file = f
  result.blockOffset = 0
  result.tailTolerance = tailTolerance
  result.eof = false
  result.offset = 0

proc close*(reader: WalReader) =
  if reader.isNil() or reader.file == nil:
    return
  close(reader.file)
  reader.file = nil

proc readNext*(reader: WalReader): Option[WalReadResult] =
  if reader.eof:
    return none(WalReadResult)
  var aggregate = newSeq[byte]()
  var recordMeta = WalRecordMetadata()
  var started = false
  while true:
    var chunk: WalChunk
    if not reader.nextChunk(chunk):
      if started:
        # 遇到截断尾部，丢弃半条记录。
        return none(WalReadResult)
      return none(WalReadResult)
    let pos = chunkFormatTable[chunk.chunkType].position
    case pos
    of chunkPosFull:
      if started and not reader.tailTolerance:
        raise newException(WalError, "unexpected full chunk while record is in progress")
      if started and reader.tailTolerance:
        aggregate.setLen(0)
        started = false
      return some(WalReadResult(payload: chunk.data, metadata: chunk.metadata))
    of chunkPosFirst:
      if started and not reader.tailTolerance:
        raise newException(WalError, "unexpected first chunk before completing previous record")
      if started and reader.tailTolerance:
        aggregate.setLen(0)
      aggregate = chunk.data
      recordMeta = chunk.metadata
      started = true
    of chunkPosMiddle:
      if not started:
        if reader.tailTolerance:
          continue
        raise newException(WalError, "middle chunk without record start")
      aggregate.add(chunk.data)
    of chunkPosLast:
      if not started:
        if reader.tailTolerance:
          continue
        raise newException(WalError, "last chunk without record start")
      aggregate.add(chunk.data)
      let payloadCopy = aggregate
      aggregate = @[]
      started = false
      return some(WalReadResult(payload: payloadCopy, metadata: recordMeta))
    else:
      if reader.tailTolerance:
        continue
      raise newException(WalError, "invalid chunk position encountered")

proc readAll*(reader: WalReader): seq[WalReadResult] =
  result = @[]
  var next = reader.readNext()
  while next.isSome():
    result.add(next.get())
    next = reader.readNext()
