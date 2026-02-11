# 带缓冲的文件包装与文件系统装饰器，为 POSIX 后端提供通用的读写缓冲能力。

import std/math

import pebble/vfs/types as vtypes

const
  defaultBufferSize = 64 * 1024

type
  ByteSlice = ptr UncheckedArray[byte]

  BufferedFile* = ref object of vtypes.File
    inner: vtypes.File
    readBuf: seq[byte]
    readPos: int
    readLen: int
    writeBuf: seq[byte]
    writeLen: int
    bufferSize: int

  BufferedFileSystem* = ref object of vtypes.FileSystem
    base: vtypes.FileSystem
    bufferSize: int

proc newBufferedFile*(inner: vtypes.File;
                      bufferSize: int = defaultBufferSize): BufferedFile =
  if inner.isNil:
    return nil
  let size = max(bufferSize, 4 * 1024)
  BufferedFile(
    inner: inner,
    readBuf: newSeq[byte](size),
    readPos: 0,
    readLen: 0,
    writeBuf: newSeq[byte](size),
    writeLen: 0,
    bufferSize: size
  )

proc flushWriteBuffer(self: BufferedFile) =
  if self.writeLen == 0:
    return
  var total = 0
  while total < self.writeLen:
    let written = self.inner.write(self.writeBuf[total].addr, self.writeLen - total)
    if written <= 0:
      raise newException(IOError, "缓冲区写入失败")
    total += written
  self.writeLen = 0

proc fillReadBuffer(self: BufferedFile) =
  self.readPos = 0
  let readBytes = self.inner.read(self.readBuf[0].addr, self.bufferSize)
  if readBytes <= 0:
    self.readLen = 0
  else:
    self.readLen = readBytes

method close*(self: BufferedFile) =
  if self.inner.isNil:
    return
  self.flushWriteBuffer()
  self.inner.close()
  self.inner = nil

method read*(self: BufferedFile; dest: pointer; size: int): int =
  if size <= 0:
    return 0
  var total = 0
  let dst = cast[ByteSlice](dest)
  while total < size:
    let available = self.readLen - self.readPos
    if available == 0:
      self.fillReadBuffer()
      if self.readLen == 0:
        break
      continue
    let toCopy = min(size - total, available)
    copyMem(addr dst[total], self.readBuf[self.readPos].addr, toCopy)
    self.readPos += toCopy
    total += toCopy
  total

method readAt*(self: BufferedFile; dest: pointer; size: int; offset: int64): int =
  self.flushWriteBuffer()
  self.inner.readAt(dest, size, offset)

method write*(self: BufferedFile; src: pointer; size: int): int =
  if size <= 0:
    return 0
  var remaining = size
  var position = 0
  let bytes = cast[ByteSlice](src)
  while remaining > 0:
    let space = self.bufferSize - self.writeLen
    if space == 0:
      self.flushWriteBuffer()
      continue
    let toCopy = min(space, remaining)
    copyMem(self.writeBuf[self.writeLen].addr, addr bytes[position], toCopy)
    self.writeLen += toCopy
    remaining -= toCopy
    position += toCopy
    if self.writeLen == self.bufferSize:
      self.flushWriteBuffer()
  size

method writeAt*(self: BufferedFile; src: pointer; size: int; offset: int64): int =
  if size <= 0:
    return 0
  self.flushWriteBuffer()
  self.inner.writeAt(src, size, offset)

method preallocate*(self: BufferedFile; offset, length: int64) =
  self.flushWriteBuffer()
  self.inner.preallocate(offset, length)

method stat*(self: BufferedFile): vtypes.FileInfo =
  self.flushWriteBuffer()
  self.inner.stat()

method sync*(self: BufferedFile) =
  self.flushWriteBuffer()
  self.inner.sync()

method syncData*(self: BufferedFile) =
  self.flushWriteBuffer()
  self.inner.syncData()

method syncTo*(self: BufferedFile; length: int64): bool =
  self.flushWriteBuffer()
  self.inner.syncTo(length)

method prefetch*(self: BufferedFile; offset, length: int64) =
  self.inner.prefetch(offset, length)

method fd*(self: BufferedFile): uint =
  self.inner.fd()

method map*(self: BufferedFile; offset: int64; length: int;
            access: vtypes.MmapAccess): vtypes.MmapRegion =
  self.flushWriteBuffer()
  self.inner.map(offset, length, access)

proc passThrough(self: BufferedFileSystem; file: vtypes.File): vtypes.File =
  if file.isNil:
    return nil
  newBufferedFile(file, self.bufferSize)

proc newBufferedFileSystem*(base: vtypes.FileSystem;
                            bufferSize: int = defaultBufferSize): BufferedFileSystem =
  if base.isNil:
    raise newException(ValueError, "base FileSystem 不可为 nil")
  BufferedFileSystem(
    base: base,
    bufferSize: max(bufferSize, 4 * 1024)
  )

method create*(self: BufferedFileSystem; path: string;
               category: vtypes.DiskWriteCategory): vtypes.File =
  self.passThrough(self.base.create(path, category))

method link*(self: BufferedFileSystem; oldname, newname: string) {.gcsafe, raises: [IOError].} =
  self.base.link(oldname, newname)

method open*(self: BufferedFileSystem; path: string;
             opts: openArray[vtypes.OpenOption]): vtypes.File =
  self.passThrough(self.base.open(path, opts))

method openReadWrite*(self: BufferedFileSystem; path: string;
                      category: vtypes.DiskWriteCategory;
                      opts: openArray[vtypes.OpenOption]): vtypes.File =
  self.passThrough(self.base.openReadWrite(path, category, opts))

method openDir*(self: BufferedFileSystem; path: string): vtypes.File =
  self.base.openDir(path)

method remove*(self: BufferedFileSystem; path: string) =
  self.base.remove(path)

method removeAll*(self: BufferedFileSystem; path: string) =
  self.base.removeAll(path)

method rename*(self: BufferedFileSystem; oldname, newname: string) {.gcsafe, raises: [IOError].} =
  self.base.rename(oldname, newname)

method reuseForWrite*(self: BufferedFileSystem; oldname, newname: string;
                      category: vtypes.DiskWriteCategory): vtypes.File =
  self.passThrough(self.base.reuseForWrite(oldname, newname, category))

method mkdirAll*(self: BufferedFileSystem; path: string; perm: uint32) =
  self.base.mkdirAll(path, perm)

method lock*(self: BufferedFileSystem; path: string): vtypes.File =
  self.base.lock(path)

method list*(self: BufferedFileSystem; path: string): seq[string] =
  self.base.list(path)

method stat*(self: BufferedFileSystem; path: string): vtypes.FileInfo =
  self.base.stat(path)

method pathBase*(self: BufferedFileSystem; path: string): string =
  self.base.pathBase(path)

method pathJoin*(self: BufferedFileSystem; parts: openArray[string]): string =
  self.base.pathJoin(parts)

method pathDir*(self: BufferedFileSystem; path: string): string =
  self.base.pathDir(path)

method diskUsage*(self: BufferedFileSystem; path: string): DiskUsage =
  self.base.diskUsage(path)

method unwrap*(self: BufferedFileSystem): FileSystem =
  self.base
