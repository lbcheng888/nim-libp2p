# 常量与类型定义，复用 Go 版 Pebble record/wal 包的线格式。

import std/[options]

const
  blockSize* = 32 * 1024
  blockSizeMask* = blockSize - 1
  legacyHeaderSize* = 7
  recyclableHeaderSize* = legacyHeaderSize + 4
  walSyncHeaderSize* = recyclableHeaderSize + 8

type
  WalError* = object of CatchableError

  ChunkType* = enum
    chunkInvalid = 0
    chunkFull = 1
    chunkFirst = 2
    chunkMiddle = 3
    chunkLast = 4
    chunkRecyclableFull = 5
    chunkRecyclableFirst = 6
    chunkRecyclableMiddle = 7
    chunkRecyclableLast = 8
    chunkWalSyncFull = 9
    chunkWalSyncFirst = 10
    chunkWalSyncMiddle = 11
    chunkWalSyncLast = 12

  ChunkPosition* = enum
    chunkPosInvalid
    chunkPosFull
    chunkPosFirst
    chunkPosMiddle
    chunkPosLast

  WireFormat* = enum
    wireInvalid
    wireLegacy
    wireRecyclable
    wireWalSync

  HeaderFormat* = object
    position*: ChunkPosition
    format*: WireFormat
    headerSize*: int

const
  chunkFormatTable*: array[ChunkType, HeaderFormat] = [
    HeaderFormat(position: chunkPosInvalid, format: wireInvalid, headerSize: 0),      # chunkInvalid
    HeaderFormat(position: chunkPosFull, format: wireLegacy, headerSize: legacyHeaderSize),
    HeaderFormat(position: chunkPosFirst, format: wireLegacy, headerSize: legacyHeaderSize),
    HeaderFormat(position: chunkPosMiddle, format: wireLegacy, headerSize: legacyHeaderSize),
    HeaderFormat(position: chunkPosLast, format: wireLegacy, headerSize: legacyHeaderSize),
    HeaderFormat(position: chunkPosFull, format: wireRecyclable, headerSize: recyclableHeaderSize),
    HeaderFormat(position: chunkPosFirst, format: wireRecyclable, headerSize: recyclableHeaderSize),
    HeaderFormat(position: chunkPosMiddle, format: wireRecyclable, headerSize: recyclableHeaderSize),
    HeaderFormat(position: chunkPosLast, format: wireRecyclable, headerSize: recyclableHeaderSize),
    HeaderFormat(position: chunkPosFull, format: wireWalSync, headerSize: walSyncHeaderSize),
    HeaderFormat(position: chunkPosFirst, format: wireWalSync, headerSize: walSyncHeaderSize),
    HeaderFormat(position: chunkPosMiddle, format: wireWalSync, headerSize: walSyncHeaderSize),
    HeaderFormat(position: chunkPosLast, format: wireWalSync, headerSize: walSyncHeaderSize)
  ]

proc headerSize*(typ: ChunkType): int =
  chunkFormatTable[typ].headerSize

proc chunkPosition*(typ: ChunkType): ChunkPosition =
  chunkFormatTable[typ].position

proc wireFormat*(typ: ChunkType): WireFormat =
  chunkFormatTable[typ].format

proc chunkTypeFor*(pos: ChunkPosition; format: WireFormat): ChunkType =
  case format
  of wireLegacy:
    case pos
    of chunkPosFull: chunkFull
    of chunkPosFirst: chunkFirst
    of chunkPosMiddle: chunkMiddle
    of chunkPosLast: chunkLast
    else: chunkInvalid
  of wireRecyclable:
    case pos
    of chunkPosFull: chunkRecyclableFull
    of chunkPosFirst: chunkRecyclableFirst
    of chunkPosMiddle: chunkRecyclableMiddle
    of chunkPosLast: chunkRecyclableLast
    else: chunkInvalid
  of wireWalSync:
    case pos
    of chunkPosFull: chunkWalSyncFull
    of chunkPosFirst: chunkWalSyncFirst
    of chunkPosMiddle: chunkWalSyncMiddle
    of chunkPosLast: chunkWalSyncLast
    else: chunkInvalid
  else:
    chunkInvalid

proc isReusable*(format: WireFormat): bool =
  ## 是否需要写入 log number 字段。
  format in {wireRecyclable, wireWalSync}

proc hasSyncOffset*(format: WireFormat): bool =
  ## 是否携带 WAL sync offset。
  format == wireWalSync

type
  WalRecordMetadata* = object
    logNumber*: uint32
    syncOffset*: uint64

  WalReadResult* = object
    payload*: seq[byte]
    metadata*: WalRecordMetadata
