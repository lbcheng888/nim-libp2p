# WAL 写入器：负责日志编码、分段轮转以及刷盘调度。

import std/[locks, math, options, os, sequtils, strformat]

when defined(posix):
  import std/posix

import pebble/runtime/executor
import pebble/wal/[checksum, recycler, types]

type
  SegmentAllocator* = proc (index: int): string {.gcsafe, raises: [].}

  WalWriterConfig* = object
    allocator*: SegmentAllocator
    recycler*: LogRecycler
    format*: WireFormat
    logNumber*: uint32
    maxSegmentBytes*: int64
    executor*: Executor
    autoSyncOffsets*: bool
    autoSyncBytes*: int64
    maxPendingAutoSyncs*: int

  WalAppendOptions* = object
    requireSync*: bool
    metadata*: WalRecordMetadata
    forceRotation*: bool

  WalAppendResult* = object
    offset*: int64
    bytesWritten*: int
    syncHandle*: TaskHandle
    segmentPath*: string
  WalWriterStats* = object
    segmentIndex*: int
    segmentPath*: string
    segmentBytes*: int64
    totalBytes*: int64
    pendingAutoSyncs*: int
    bytesSinceSync*: int64

  WalWriter* = ref object
    cfg: WalWriterConfig
    segmentIndex: int
    segmentPath: string
    file: File
    blockOffset: int
    segmentBytes: int64
    totalBytes: int64
    pending: seq[TaskHandle]
    bytesSinceSync: int64
    mutex: Lock
    closed: bool

template ensureAllocator(cfg: WalWriterConfig) =
  if cfg.allocator.isNil():
    raise newException(WalError, "WAL allocator must not be nil")

proc defaultAllocator(path: string): SegmentAllocator =
  result = proc (index: int): string =
    if index == 0:
      return path
    let (dir, name, ext) = splitFile(path)
    return os.joinPath(dir, fmt"{name}.{index}{ext}")

proc ensureParentDir(path: string) =
  let dir = os.splitFile(path).dir
  if dir.len > 0 and not dir.dirExists():
    createDir(dir)

proc initFile(path: string) =
  ensureParentDir(path)
  var f: File
  if not open(f, path, fmWrite):
    raise newException(WalError, "unable to open WAL file: " & path)
  close(f)

proc truncateSegment(path: string; size: int64) =
  when defined(posix):
    if posix.truncate(path.cstring, Off(size)) != 0:
      raise newException(WalError, "failed to truncate segment: " & path)
  else:
    discard

proc writeBytes(file: File; data: openArray[byte]) =
  if data.len == 0:
    return
  var remaining = data.len
  var offset = 0
  let base = data.low
  while remaining > 0:
    let written = file.writeBuffer(unsafeAddr data[base + offset], remaining)
    if written <= 0:
      raise newException(WalError, "failed to write WAL bytes")
    offset += written
    remaining -= written

proc cleanupPending(writer: WalWriter) =
  ## 移除已经完成的刷盘任务，避免 pending 队列无限增长。
  if writer.pending.len == 0:
    return
  var active: seq[TaskHandle] = @[]
  for handle in writer.pending:
    if handle.isNil():
      continue
    if handle.isPending():
      active.add(handle)
    else:
      handle.wait()
  writer.pending = active

proc trackPending(writer: WalWriter; handle: TaskHandle) =
  ## 管理内部自动刷盘任务的并发度。
  if handle.isNil():
    return
  writer.cleanupPending()
  if writer.cfg.maxPendingAutoSyncs > 0:
    while writer.pending.len >= writer.cfg.maxPendingAutoSyncs:
      let waitHandle = writer.pending[0]
      writer.pending.delete(0)
      waitHandle.wait()
      writer.cleanupPending()
  writer.pending.add(handle)

proc padBlock(writer: WalWriter) =
  let remaining = blockSize - writer.blockOffset
  if remaining <= 0:
    return
  var zeros = newSeq[byte](remaining)
  writeBytes(writer.file, zeros)
  writer.blockOffset = 0
  writer.segmentBytes += remaining
  writer.totalBytes += remaining

proc closeCurrentSegment(writer: WalWriter; recycle = true) =
  if writer.file != nil:
    for handle in writer.pending.mitems():
      handle.wait()
    writer.pending.setLen(0)
    writer.file.flushFile()
    close(writer.file)
    writer.file = nil
    if recycle and not writer.cfg.recycler.isNil() and writer.segmentPath.len > 0:
      writer.cfg.recycler.recycle(RecyclableFile(
        path: writer.segmentPath,
        size: writer.segmentBytes
      ))
  writer.blockOffset = 0
  writer.segmentBytes = 0
  writer.bytesSinceSync = 0

proc openSegment(writer: WalWriter; index: int; recyclePrev: bool) =
  writer.closeCurrentSegment(recyclePrev)
  let path = writer.cfg.allocator(index)
  var reused: Option[RecyclableFile]
  if not writer.cfg.recycler.isNil():
    reused = writer.cfg.recycler.acquire()
  if reused.isSome():
    let file = reused.get()
    if file.path != path:
      if path.fileExists():
        removeFile(path)
      moveFile(file.path, path)
    else:
      # 旧文件路径和新路径相同，确保被截断。
      discard
  initFile(path)
  if not open(writer.file, path, fmWrite):
    raise newException(WalError, "unable to open WAL segment: " & path)
  writer.segmentPath = path
  writer.blockOffset = 0
  writer.segmentBytes = 0

proc headerSize(format: WireFormat): int =
  case format
  of wireLegacy: legacyHeaderSize
  of wireRecyclable: recyclableHeaderSize
  of wireWalSync: walSyncHeaderSize
  else: 0

proc flushNow(writer: WalWriter): TaskHandle =
  if writer.cfg.executor.isNil():
    writer.file.flushFile()
    return nil
  let f = writer.file
  let task = proc () {.gcsafe.} =
    f.flushFile()
  writer.cfg.executor.submit(task)

proc maybeAutoSync(writer: WalWriter; bytesWritten: int) =
  if bytesWritten <= 0 or writer.cfg.autoSyncBytes <= 0:
    return
  writer.bytesSinceSync += int64(bytesWritten)
  if writer.bytesSinceSync >= writer.cfg.autoSyncBytes:
    let handle = writer.flushNow()
    if not handle.isNil():
      writer.trackPending(handle)
    writer.bytesSinceSync = 0

proc writeChunk(writer: WalWriter;
                chunkType: ChunkType;
                payload: seq[byte];
                metadata: WalRecordMetadata) =
  var header = newSeq[byte](headerSize(writer.cfg.format))
  let payloadLen = uint16(payload.len)
  var crc = extend(0'u32, fromByte(byte(chunkType.ord)))
  if writer.cfg.format.isReusable():
    crc = extend(crc, fromUInt32(metadata.logNumber))
  if writer.cfg.format.hasSyncOffset():
    crc = extend(crc, fromUInt64(metadata.syncOffset))
  if payload.len > 0:
    crc = extend(crc, payload)
  let checksumMasked = maskChecksum(crc)
  header[0] = byte(checksumMasked and 0xFF)
  header[1] = byte((checksumMasked shr 8) and 0xFF)
  header[2] = byte((checksumMasked shr 16) and 0xFF)
  header[3] = byte((checksumMasked shr 24) and 0xFF)
  header[4] = byte(payloadLen and 0xFF)
  header[5] = byte((payloadLen shr 8) and 0xFF)
  header[6] = byte(chunkType.ord)
  var idx = 7
  if writer.cfg.format.isReusable():
    let logBytes = fromUInt32(metadata.logNumber)
    for b in logBytes:
      header[idx] = b
      inc idx
  if writer.cfg.format.hasSyncOffset():
    let syncBytes = fromUInt64(metadata.syncOffset)
    for b in syncBytes:
      header[idx] = b
      inc idx
  writeBytes(writer.file, header)
  writeBytes(writer.file, payload)
  writer.blockOffset += header.len + payload.len
  writer.segmentBytes += int64(header.len + payload.len)
  writer.totalBytes += int64(header.len + payload.len)

proc rotateIfNeeded(writer: WalWriter; force = false) =
  if force or (writer.cfg.maxSegmentBytes > 0 and
      writer.segmentBytes >= writer.cfg.maxSegmentBytes):
    inc writer.segmentIndex
    writer.openSegment(writer.segmentIndex, true)

proc newWalWriter*(cfg: WalWriterConfig): WalWriter =
  ensureAllocator(cfg)
  var normalized = cfg
  if normalized.autoSyncBytes < 0:
    normalized.autoSyncBytes = 0
  if normalized.maxPendingAutoSyncs < 0:
    normalized.maxPendingAutoSyncs = 0
  new(result)
  result.cfg = normalized
  initLock(result.mutex)
  result.segmentIndex = 0
  result.pending = @[]
  result.totalBytes = 0
  result.bytesSinceSync = 0
  result.closed = false
  result.openSegment(result.segmentIndex, false)

proc stats*(writer: WalWriter): WalWriterStats =
  if writer.isNil():
    return WalWriterStats()
  writer.mutex.acquire()
  defer: writer.mutex.release()
  WalWriterStats(
    segmentIndex: writer.segmentIndex,
    segmentPath: writer.segmentPath,
    segmentBytes: writer.segmentBytes,
    totalBytes: writer.totalBytes,
    pendingAutoSyncs: writer.pending.len,
    bytesSinceSync: writer.bytesSinceSync
  )

proc newWalWriter*(path: string;
                   format: WireFormat = wireRecyclable;
                   logNumber: uint32 = 0;
                   maxSegmentBytes: int64 = 0;
                   recycler: LogRecycler = nil;
                   executor: Executor = nil;
                   autoSyncOffsets = true;
                   autoSyncBytes: int64 = 0;
                   maxPendingAutoSyncs: int = 0): WalWriter =
  let cfg = WalWriterConfig(
    allocator: defaultAllocator(path),
    recycler: recycler,
    format: format,
    logNumber: logNumber,
    maxSegmentBytes: maxSegmentBytes,
    executor: executor,
    autoSyncOffsets: autoSyncOffsets,
    autoSyncBytes: autoSyncBytes,
    maxPendingAutoSyncs: maxPendingAutoSyncs
  )
  newWalWriter(cfg)

proc append*(writer: WalWriter;
             payload: seq[byte];
             options: WalAppendOptions = WalAppendOptions()): WalAppendResult =
  if writer.closed:
    raise newException(WalError, "wal writer already closed")
  writer.mutex.acquire()
  defer: writer.mutex.release()
  if options.forceRotation:
    writer.rotateIfNeeded(true)
  var metadata = options.metadata
  if metadata.logNumber == 0'u32:
    metadata.logNumber = writer.cfg.logNumber
  var recordOffset = -1'i64
  var recordSegmentPath = writer.segmentPath
  let headerBytes = headerSize(writer.cfg.format)
  var writtenBytes = 0
  let totalLen = payload.len
  if totalLen == 0:
    if writer.cfg.maxSegmentBytes > 0 and
        writer.segmentBytes + int64(headerBytes) > writer.cfg.maxSegmentBytes and
        writer.segmentBytes > 0:
      writer.rotateIfNeeded(true)
    if blockSize - writer.blockOffset < headerBytes:
      writer.padBlock()
    if recordOffset < 0:
      recordOffset = writer.segmentBytes
      recordSegmentPath = writer.segmentPath
    var chunkMeta = metadata
    let chunkStart = writer.segmentBytes
    if writer.cfg.format.hasSyncOffset():
      if chunkMeta.syncOffset == 0 and writer.cfg.autoSyncOffsets:
        chunkMeta.syncOffset = uint64(chunkStart + int64(headerBytes))
      elif chunkMeta.syncOffset < uint64(chunkStart):
        raise newException(WalError, "sync offset regression detected")
    writer.writeChunk(chunkTypeFor(chunkPosFull, writer.cfg.format), @[], chunkMeta)
    writtenBytes = headerBytes
  else:
    var remaining = totalLen
    var cursor = 0
    while remaining > 0:
      if blockSize - writer.blockOffset < headerBytes:
        writer.padBlock()
        continue
      let usable = max(0, blockSize - writer.blockOffset - headerBytes)
      if usable == 0:
        writer.padBlock()
        continue
      let chunkLen = min(remaining, usable)
      if writer.cfg.maxSegmentBytes > 0 and
          writer.segmentBytes + (int64(headerBytes) + int64(chunkLen)) > writer.cfg.maxSegmentBytes and
          writer.segmentBytes > 0:
        writer.rotateIfNeeded(true)
        continue
      let position =
        if cursor == 0 and chunkLen == remaining: chunkPosFull
        elif cursor == 0: chunkPosFirst
        elif chunkLen == remaining: chunkPosLast
        else: chunkPosMiddle
      let chunkType = chunkTypeFor(position, writer.cfg.format)
      if chunkType == chunkInvalid:
        raise newException(WalError, "invalid chunk type encoding")
      var chunkData = newSeq[byte](chunkLen)
      for i in 0 ..< chunkLen:
        chunkData[i] = payload[cursor + i]
      var chunkMeta = metadata
      let chunkStart = writer.segmentBytes
      let chunkAdvance = int64(headerBytes) + int64(chunkLen)
      if writer.cfg.format.hasSyncOffset():
        if chunkMeta.syncOffset == 0 and writer.cfg.autoSyncOffsets:
          chunkMeta.syncOffset = uint64(chunkStart + chunkAdvance)
        elif chunkMeta.syncOffset < uint64(chunkStart):
          raise newException(WalError, "sync offset regression detected")
      if recordOffset < 0:
        recordOffset = chunkStart
        recordSegmentPath = writer.segmentPath
      writer.writeChunk(chunkType, chunkData, chunkMeta)
      remaining -= chunkLen
      cursor += chunkLen
      writtenBytes += headerBytes + chunkLen
  if recordOffset < 0:
    recordOffset = writer.segmentBytes
    recordSegmentPath = writer.segmentPath
  result.offset = recordOffset
  result.bytesWritten = writtenBytes
  result.segmentPath = recordSegmentPath
  if options.requireSync:
    if writer.cfg.autoSyncBytes > 0:
      writer.bytesSinceSync += int64(writtenBytes)
    let handle = writer.flushNow()
    writer.bytesSinceSync = 0
    result.syncHandle = handle
  else:
    writer.maybeAutoSync(writtenBytes)
    result.syncHandle = nil

proc append*(writer: WalWriter;
             payload: string;
             options: WalAppendOptions = WalAppendOptions()): WalAppendResult =
  var data = newSeq[byte](payload.len)
  for i, ch in payload:
    data[i] = byte(ord(ch))
  writer.append(data, options)

proc close*(writer: WalWriter) =
  if writer.closed:
    return
  writer.mutex.acquire()
  defer: writer.mutex.release()
  writer.closeCurrentSegment(true)
  writer.closed = true

proc trimTail*(writer: WalWriter) =
  ## 将当前段尾部裁剪到最近一次写入处。
  writer.mutex.acquire()
  defer: writer.mutex.release()
  if writer.file == nil:
    return
  for handle in writer.pending.mitems():
    handle.wait()
  writer.pending.setLen(0)
  writer.bytesSinceSync = 0
  writer.file.flushFile()
  truncateSegment(writer.segmentPath, writer.segmentBytes)

proc sync*(writer: WalWriter) =
  ## 同步等待所有挂起的刷盘任务。
  writer.mutex.acquire()
  defer: writer.mutex.release()
  for handle in writer.pending.mitems():
    handle.wait()
  writer.pending.setLen(0)
  writer.bytesSinceSync = 0
  writer.file.flushFile()
