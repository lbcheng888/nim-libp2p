## 协议核心性能调优：PreparedPacket 内存池与构建统计。

import std/monotimes
import std/times

import ./packet_model

when compiles((proc () {.noGc.} = discard)):
  {.pragma: quicHotPath, noGc.}
else:
  {.pragma: quicHotPath, gcsafe.}

const
  MaxFramesPerPacket* = 8
  DefaultPacketCryptoCapacity* = 1350

type
  PacketPoolStats* = object
    builtPackets*: uint64
    reusedPackets*: uint64
    releases*: uint64
    totalBuildNs*: uint64
    maxBuildNs*: uint64
    maxInUse*: int
    capacity*: int

  PreparedPacketHandle* = ref object
    header*: QuicPacketHeader
    frameCount*: uint8
    frames*: array[MaxFramesPerPacket, FramePayload]
    frameLimit*: int
    payloadLength*: uint16
    cryptoStorage: ptr uint8
    cryptoCapacity*: int
    cryptoUsed*: int
    epoch*: CryptoEpoch
    packetNumber*: uint32
    reused*: bool

  PreparedPacketPool* = ref object
    freeList: seq[PreparedPacketHandle]
    cryptoCapacity: int
    frameCapacity: int
    inUse: int
    stats: PacketPoolStats

proc ptrOffset(base: ptr uint8; offset: int): ptr uint8 {.inline.} =
  if base.isNil or offset == 0:
    return base
  cast[ptr uint8](cast[uint](base) + cast[uint](offset))

proc finalize(handle: PreparedPacketHandle) =
  if handle.cryptoStorage != nil:
    deallocShared(handle.cryptoStorage)
    handle.cryptoStorage = nil
    handle.cryptoCapacity = 0

proc initPreparedPacketHandle(frameCapacity, cryptoCapacity: int): PreparedPacketHandle =
  new(result)
  result.frameLimit = if frameCapacity <= MaxFramesPerPacket: frameCapacity else: MaxFramesPerPacket
  if result.frameLimit <= 0:
    result.frameLimit = MaxFramesPerPacket
  result.cryptoCapacity = if cryptoCapacity > 0: cryptoCapacity else: DefaultPacketCryptoCapacity
  result.cryptoStorage = cast[ptr uint8](allocShared0(result.cryptoCapacity))
  result.frameCount = 0
  result.payloadLength = 0
  result.cryptoUsed = 0
  result.epoch = ceInitial
  result.packetNumber = 0
  result.reused = false

proc resetHandle(handle: PreparedPacketHandle) {.quicHotPath.} =
  handle.frameCount = 0
  handle.payloadLength = 0
  handle.cryptoUsed = 0
  handle.epoch = ceInitial
  handle.packetNumber = 0
  handle.reused = false

proc initPreparedPacketPool*(initialCapacity: Positive = 16,
    cryptoCapacity: int = DefaultPacketCryptoCapacity,
    frameCapacity: int = MaxFramesPerPacket): PreparedPacketPool =
  new(result)
  result.cryptoCapacity = cryptoCapacity
  result.frameCapacity = frameCapacity
  result.freeList = newSeqOfCap[PreparedPacketHandle](initialCapacity)
  for _ in 0 ..< initialCapacity:
    let handle = initPreparedPacketHandle(frameCapacity, cryptoCapacity)
    result.freeList.add(handle)
  result.stats.capacity = initialCapacity
  result.inUse = 0

proc acquirePacket*(pool: PreparedPacketPool): PreparedPacketHandle =
  if pool.freeList.len == 0:
    let handle = initPreparedPacketHandle(pool.frameCapacity, pool.cryptoCapacity)
    pool.stats.capacity.inc
    pool.inUse.inc
    if pool.inUse > pool.stats.maxInUse:
      pool.stats.maxInUse = pool.inUse
    return handle
  let idx = pool.freeList.high
  result = pool.freeList[idx]
  pool.freeList.setLen(idx)
  result.resetHandle()
  result.reused = true
  pool.inUse.inc
  if pool.inUse > pool.stats.maxInUse:
    pool.stats.maxInUse = pool.inUse

proc releasePacket*(pool: PreparedPacketPool, packet: PreparedPacketHandle) =
  if packet.isNil:
    return
  packet.resetHandle()
  pool.freeList.add(packet)
  if pool.inUse > 0:
    pool.inUse.dec
  pool.stats.releases.inc

proc appendCryptoFrame*(packet: PreparedPacketHandle, offset: uint64,
    span: CryptoSpan): FramePayload {.quicHotPath.} =
  if packet.frameCount.int >= packet.frameLimit:
    raise newException(IndexDefect, "frame limit exceeded")
  let length = span.length.int
  if length > packet.cryptoCapacity - packet.cryptoUsed:
    raise newException(ValueError, "packet crypto buffer exhausted")
  var destPtr = ptrOffset(packet.cryptoStorage, packet.cryptoUsed)
  if length > 0 and not span.data.isNil:
    copyMem(destPtr, span.data, length)
  elif length > 0 and span.data.isNil:
    raise newException(ValueError, "crypto span data must not be nil when length > 0")
  let copiedSpan = makeCryptoSpan(destPtr, length)
  let frame = cryptoFrame(offset, copiedSpan)
  packet.frames[packet.frameCount.int] = frame
  packet.frameCount.inc
  packet.cryptoUsed += length
  packet.payloadLength = packet.payloadLength + uint16(length)
  frame

proc setHeader*(packet: PreparedPacketHandle, header: QuicPacketHeader) {.quicHotPath.} =
  packet.header = header

proc setMetadata*(packet: PreparedPacketHandle, epoch: CryptoEpoch,
    packetNumber: uint32) {.quicHotPath.} =
  packet.epoch = epoch
  packet.packetNumber = packetNumber

proc framesLen*(packet: PreparedPacketHandle): int {.inline.} =
  packet.frameCount.int

proc frameAt*(packet: PreparedPacketHandle, idx: Natural): FramePayload =
  if idx >= packet.frameCount.int:
    raise newException(IndexDefect, "frame index out of range")
  packet.frames[idx]

proc toSnapshot*(packet: PreparedPacketHandle): PreparedPacket =
  var framesSeq = newSeq[FramePayload](packet.frameCount.int)
  for i in 0 ..< packet.frameCount.int:
    framesSeq[i] = packet.frames[i]
  PreparedPacket(
    header: packet.header,
    frames: framesSeq,
    payloadLength: packet.payloadLength)

proc recordBuild*(pool: PreparedPacketPool, packet: PreparedPacketHandle,
    duration: Duration) =
  pool.stats.builtPackets.inc
  if packet.reused:
    pool.stats.reusedPackets.inc
  let ns = duration.inNanoseconds()
  pool.stats.totalBuildNs += uint64(ns)
  if uint64(ns) > pool.stats.maxBuildNs:
    pool.stats.maxBuildNs = uint64(ns)

proc snapshot*(pool: PreparedPacketPool): PacketPoolStats =
  result = pool.stats
  if pool.stats.capacity < pool.freeList.len + pool.inUse:
    result.capacity = pool.freeList.len + pool.inUse
