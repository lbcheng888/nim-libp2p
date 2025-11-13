## 最小握手阶段的 Crypto Stream 发送实现，支持内存池与零拷贝 Span。

import ./packet_model

const
  DefaultCryptoBufferCapacity = 4096

type
  CryptoSendBuffer = object
    storage: ptr uint8
    capacity: int
    readPos: int
    writePos: int

  CryptoChunk* = object
    ## 代表一次可被打包发送的 Crypto 帧载荷。
    epoch*: CryptoEpoch
    offset*: uint64
    data*: CryptoSpan

  CryptoStreamSender* = object
    ## 追踪某个加密阶段的待发送数据与偏移。
    epoch*: CryptoEpoch
    nextOffset*: uint64
    buffer*: CryptoSendBuffer

proc ptrOffset(base: ptr uint8; offset: int): ptr uint8 {.inline.} =
  ## Nim 2 默认关闭指针算术，提供显式偏移辅助。
  if base.isNil or offset == 0:
    return base
  cast[ptr uint8](cast[uint](base) + cast[uint](offset))

proc `=destroy`(buf: var CryptoSendBuffer) =
  if buf.storage != nil:
    deallocShared(buf.storage)
    buf.storage = nil
    buf.capacity = 0
    buf.readPos = 0
    buf.writePos = 0

proc initCryptoSendBuffer(capacity: Natural = DefaultCryptoBufferCapacity): CryptoSendBuffer =
  var cap = int(capacity)
  if cap <= 0:
    cap = DefaultCryptoBufferCapacity
  result.capacity = cap
  result.storage = cast[ptr uint8](allocShared0(cap))
  result.readPos = 0
  result.writePos = 0

proc available(buf: CryptoSendBuffer): int {.inline.} =
  buf.writePos - buf.readPos

proc compact(buf: var CryptoSendBuffer) =
  let avail = buf.available()
  if avail == 0:
    buf.readPos = 0
    buf.writePos = 0
    return
  if buf.readPos == 0:
    return
  moveMem(buf.storage, ptrOffset(buf.storage, buf.readPos), avail)
  buf.readPos = 0
  buf.writePos = avail

proc ensureCapacity(buf: var CryptoSendBuffer, additional: int) =
  if additional <= 0:
    return
  let needed = buf.available() + additional
  if needed <= buf.capacity:
    if buf.capacity - buf.writePos < additional:
      buf.compact()
    return
  var newCapacity = buf.capacity
  if newCapacity == 0:
    newCapacity = DefaultCryptoBufferCapacity
  while newCapacity < needed:
    newCapacity = newCapacity * 2
    if newCapacity < needed:
      newCapacity = needed
  let newStorage = cast[ptr uint8](allocShared0(newCapacity))
  let avail = buf.available()
  if avail > 0:
    copyMem(newStorage, ptrOffset(buf.storage, buf.readPos), avail)
  if buf.storage != nil:
    deallocShared(buf.storage)
  buf.storage = newStorage
  buf.capacity = newCapacity
  buf.readPos = 0
  buf.writePos = avail

proc append(buf: var CryptoSendBuffer, data: openArray[uint8]) =
  if data.len == 0:
    return
  buf.ensureCapacity(data.len)
  copyMem(ptrOffset(buf.storage, buf.writePos), unsafeAddr data[0], data.len)
  buf.writePos += data.len

proc peek(buf: CryptoSendBuffer, count: int): CryptoSpan =
  if count <= 0:
    return emptyCryptoSpan()
  makeCryptoSpan(ptrOffset(buf.storage, buf.readPos), count)

proc consume(buf: var CryptoSendBuffer, count: int) =
  if count <= 0:
    return
  buf.readPos += count
  if buf.readPos >= buf.writePos:
    buf.readPos = 0
    buf.writePos = 0

proc initCryptoStreamSender*(epoch: CryptoEpoch, bufferCapacity: Natural = DefaultCryptoBufferCapacity): CryptoStreamSender =
  CryptoStreamSender(epoch: epoch, nextOffset: 0, buffer: initCryptoSendBuffer(bufferCapacity))

proc enqueueCryptoData*(sender: var CryptoStreamSender, data: openArray[uint8]) =
  ## 将 TLS 产生的 Crypto 数据追加到发送缓冲。
  sender.buffer.append(data)

proc hasPendingData*(sender: CryptoStreamSender): bool =
  sender.buffer.available() > 0

proc pendingBytes*(sender: CryptoStreamSender): int =
  sender.buffer.available()

proc emitChunk*(sender: var CryptoStreamSender, maxFrameSize: Natural,
    chunk: var CryptoChunk): bool =
  ## 从缓冲中提取一段数据用于构建 Crypto 帧。
  if sender.buffer.available() == 0:
    return false
  assert maxFrameSize > 0, "maxFrameSize 必须为正数"
  let takeCount = min(sender.buffer.available(), int(maxFrameSize))
  let span = sender.buffer.peek(takeCount)
  chunk = CryptoChunk(
    epoch: sender.epoch,
    offset: sender.nextOffset,
    data: span)
  sender.nextOffset += uint64(takeCount)
  sender.buffer.consume(takeCount)
  true
