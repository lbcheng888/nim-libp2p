# Shared type definitions for the SSTable subsystem.

import std/[options, strformat, strutils]
import pebble/core/types

type
  SSTError* = object of CatchableError

  BlockHandle* = object
    ## Offset and length into the table byte buffer.
    offset*: int
    length*: int

  BlockKind* = enum
    blockData
    blockIndex
    blockFilter
    blockMeta

  BlockEncoding* = enum
    blockEncodingLegacy,
    blockEncodingPrefixCompressed

  TableProperties* = object
    ## Lightweight summary mirroring Pebble's user properties.
    numEntries*: int
    rawKeySize*: int
    rawValueSize*: int
    smallestKey*: Option[Key]
    largestKey*: Option[Key]
    bloomBitsPerKey*: Option[int]

  IndexEntry* = object
    separator*: Key
    handle*: BlockHandle

  TableSection* = object
    kind*: BlockKind
    handle*: BlockHandle

  SSTableImage* = object
    ## Contiguous byte buffer containing all table sections and footer.
    buffer*: string
    sections*: seq[TableSection]

  TableFooter* = object
    indexHandle*: BlockHandle
    filterHandle*: Option[BlockHandle]
    metaHandle*: BlockHandle

  BloomFilter* = object
    bits*: seq[uint8]
    numBits*: int
    numProbes*: int

  TableBuildArtifacts* = object
    image*: SSTableImage
    footer*: TableFooter
    properties*: TableProperties
    bloom*: Option[BloomFilter]
    indexEntries*: seq[IndexEntry]


proc initBlockHandle*(offset, length: int): BlockHandle =
  BlockHandle(offset: offset, length: length)

proc describe*(handle: BlockHandle): string =
  fmt"offset={handle.offset},length={handle.length}"

proc mergeProperties*(into: var TableProperties; other: TableProperties) =
  into.numEntries += other.numEntries
  into.rawKeySize += other.rawKeySize
  into.rawValueSize += other.rawValueSize
  if other.smallestKey.isSome():
    let otherBytes = other.smallestKey.get().toBytes()
    if into.smallestKey.isNone():
      into.smallestKey = other.smallestKey
    else:
      let currentBytes = into.smallestKey.get().toBytes()
      if currentBytes > otherBytes:
        into.smallestKey = other.smallestKey
  if other.largestKey.isSome():
    let otherBytes = other.largestKey.get().toBytes()
    if into.largestKey.isNone():
      into.largestKey = other.largestKey
    else:
      let currentBytes = into.largestKey.get().toBytes()
      if currentBytes < otherBytes:
        into.largestKey = other.largestKey
  if other.bloomBitsPerKey.isSome():
    into.bloomBitsPerKey = other.bloomBitsPerKey
