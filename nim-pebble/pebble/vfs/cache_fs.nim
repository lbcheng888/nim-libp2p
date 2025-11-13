# 只读缓存层，实现基于内存的 VFS 读缓存与 LRU 淘汰策略。

import std/[locks, math, monotimes, os, sequtils, strutils, tables]

import pebble/vfs/types as vtypes

type
  CacheEntry = ref object
    data: seq[byte]
    info: vtypes.FileInfo
    path: string
    lastAccess: MonoTime

  ReadCache* = ref object
    entries: Table[string, CacheEntry]
    order: seq[string]
    capacityBytes: int64
    maxEntries: int
    currentBytes: int64
    lock: Lock

  CachedReadFile* = ref object of vtypes.File
    cache: ReadCache
    entry: CacheEntry
    offset: int

  CachingFileSystem* = ref object of vtypes.FileSystem
    base: vtypes.FileSystem
    cache: ReadCache
    maxCachedFileSize: int64

template withCacheLock(cache: ReadCache; body: untyped) =
  cache.lock.acquire()
  try:
    body
  finally:
    cache.lock.release()

proc newReadCache*(capacityBytes: int64; maxEntries: int = 256): ReadCache =
  if capacityBytes <= 0:
    raise newException(ValueError, "capacityBytes 必须大于 0")
  if maxEntries <= 0:
    raise newException(ValueError, "maxEntries 必须大于 0")
  result = ReadCache(
    entries: initTable[string, CacheEntry](),
    order: @[],
    capacityBytes: capacityBytes,
    maxEntries: maxEntries,
    currentBytes: 0
  )
  initLock(result.lock)

proc get*(cache: ReadCache; path: string): CacheEntry =
  if cache.isNil:
    return nil
  cache.withCacheLock:
    if cache.entries.contains(path):
      let entry = if cache.entries.contains(path): cache.entries.getOrDefault(path, nil) else: nil
      if entry != nil:
        entry.lastAccess = getMonoTime()
        let idx = cache.order.find(path)
        if idx >= 0:
          cache.order.delete(idx)
        cache.order.add(path)
      return entry
  nil

proc removeIndex(cache: ReadCache; idx: int) =
  let key = cache.order[idx]
  cache.order.delete(idx)
  if cache.entries.contains(key):
    cache.currentBytes -= int64(((cache.entries.getOrDefault(key, nil)).data.len))
    cache.entries.del(key)

proc evictIfNeeded(cache: ReadCache) =
  while (cache.currentBytes > cache.capacityBytes or cache.entries.len > cache.maxEntries) and cache.order.len > 0:
    cache.removeIndex(0)

proc store*(cache: ReadCache; entry: CacheEntry) =
  if cache.isNil or entry.isNil:
    return
  cache.withCacheLock:
    if cache.entries.contains(entry.path):
      let existing = cache.entries[entry.path]
      cache.currentBytes -= int64(existing.data.len)
      let idx = cache.order.find(entry.path)
      if idx >= 0:
        cache.order.delete(idx)
    cache.entries[entry.path] = entry
    cache.order.add(entry.path)
    cache.currentBytes += int64(entry.data.len)
    cache.evictIfNeeded()

proc invalidate*(cache: ReadCache; path: string) {.raises: [IOError].} =
  if cache.isNil:
    return
  cache.withCacheLock:
    if cache.entries.contains(path):
      let entry = cache.entries.getOrDefault(path, nil)
      cache.currentBytes -= int64(entry.data.len)
      cache.entries.del(path)
      let idx = cache.order.find(path)
      if idx >= 0:
        cache.order.delete(idx)

proc invalidatePrefix*(cache: ReadCache; prefix: string) {.raises: [IOError].} =
  if cache.isNil or prefix.len == 0:
    return
  cache.withCacheLock:
    var toRemove: seq[string] = @[]
    for key in cache.order:
      if key.startsWith(prefix):
        toRemove.add(key)
    for key in toRemove:
      if cache.entries.contains(key):
        let entry = cache.entries.getOrDefault(key, nil)
        if entry != nil:
          cache.currentBytes -= int64(entry.data.len)
        cache.entries.del(key)
    cache.order = cache.order.filterIt(not it.startsWith(prefix))

proc newCachedReadFile(cache: ReadCache; entry: CacheEntry): CachedReadFile =
  CachedReadFile(cache: cache, entry: entry, offset: 0)

method close*(self: CachedReadFile) =
  self.cache = nil
  self.entry = nil

proc remaining(self: CachedReadFile): int =
  if self.entry.isNil:
    return 0
  self.entry.data.len - self.offset

method read*(self: CachedReadFile; dest: pointer; size: int): int =
  if self.entry.isNil or size <= 0:
    return 0
  let available = self.remaining()
  if available <= 0:
    return 0
  let toCopy = min(size, available)
  copyMem(dest, unsafeAddr self.entry.data[self.offset], toCopy)
  self.offset += toCopy
  if not self.cache.isNil:
    self.entry.lastAccess = getMonoTime()
  toCopy

method readAt*(self: CachedReadFile; dest: pointer; size: int; offset: int64): int =
  if self.entry.isNil or size <= 0:
    return 0
  let off = int(offset)
  if off < 0 or off >= self.entry.data.len:
    return 0
  let available = self.entry.data.len - off
  let toCopy = min(size, available)
  copyMem(dest, unsafeAddr self.entry.data[off], toCopy)
  if not self.cache.isNil:
    self.entry.lastAccess = getMonoTime()
  toCopy

method write*(self: CachedReadFile; src: pointer; size: int): int =
  discard src
  discard size
  raise newException(IOError, "缓存文件为只读，禁止写入")

method writeAt*(self: CachedReadFile; src: pointer; size: int; offset: int64): int =
  discard src
  discard size
  discard offset
  raise newException(IOError, "缓存文件为只读，禁止写入")

method preallocate*(self: CachedReadFile; offset, length: int64) =
  discard offset
  discard length
  raise newException(IOError, "缓存文件不支持预分配")

method stat*(self: CachedReadFile): vtypes.FileInfo =
  if self.entry.isNil:
    raise newException(IOError, "缓存条目已释放")
  self.entry.info

method sync*(self: CachedReadFile) =
  discard

method syncData*(self: CachedReadFile) =
  discard

method syncTo*(self: CachedReadFile; length: int64): bool =
  discard length
  true

method prefetch*(self: CachedReadFile; offset, length: int64) =
  discard offset
  discard length

method fd*(self: CachedReadFile): uint =
  vtypes.InvalidFileDescriptor

method map*(self: CachedReadFile; offset: int64; length: int; access: vtypes.MmapAccess): vtypes.MmapRegion =
  if self.entry.isNil:
    raise newException(IOError, "缓存条目已释放")
  if access == mapReadWrite:
    raise newException(IOError, "缓存映射只读")
  let off = int(offset)
  if off < 0 or off >= self.entry.data.len:
    raise newException(IOError, "超出缓存范围")
  let maxLen = self.entry.data.len - off
  let mapped = if length <= 0 or length > maxLen: maxLen else: length
  var region = vtypes.MmapRegion(
    base: unsafeAddr self.entry.data[off],
    length: mapped,
    access: mapReadOnly
  )
  region.setReleaser(proc (base: pointer; length: int) {.gcsafe.} =
    discard base
    discard length
  )
  region

proc newCachingFileSystem*(base: vtypes.FileSystem; capacityBytes: int64;
                           maxEntries: int = 256;
                           maxCachedFileSize: int64 = -1): CachingFileSystem =
  if base.isNil:
    raise newException(ValueError, "base vtypes.FileSystem 不可为 nil")
  let cache = newReadCache(capacityBytes, maxEntries)
  let fileSizeLimit = if maxCachedFileSize <= 0:
    max(capacityBytes div 4, 128 * 1024)
  else:
    maxCachedFileSize
  CachingFileSystem(
    base: base,
    cache: cache,
    maxCachedFileSize: fileSizeLimit
  )

proc newCachingFileSystem*(base: vtypes.FileSystem; cache: ReadCache;
                           maxCachedFileSize: int64 = -1): CachingFileSystem =
  if base.isNil:
    raise newException(ValueError, "base vtypes.FileSystem 不可为 nil")
  if cache.isNil:
    raise newException(ValueError, "cache 不可为 nil")
  let limit = if maxCachedFileSize <= 0:
    max(cache.capacityBytes div 4, 128 * 1024)
  else:
    maxCachedFileSize
  CachingFileSystem(
    base: base,
    cache: cache,
    maxCachedFileSize: limit
  )

proc cacheFile(self: CachingFileSystem; path: string;
               opts: openArray[vtypes.OpenOption]): vtypes.File {.raises: [IOError].} =
  let source = self.base.open(path, opts)
  if source.isNil:
    return nil
  try:
    let info = source.stat()
    if info.isDir:
      return source
    if info.size < 0 or int64(info.size) > self.maxCachedFileSize:
      return source
    var data = newSeq[byte](int(info.size))
    var offset = 0
    while offset < data.len:
      let readBytes = source.read(data[offset].addr, data.len - offset)
      if readBytes <= 0:
        break
      offset += readBytes
    if offset != data.len:
      source.close()
      return self.base.open(path, opts)
    let entry = CacheEntry(
      data: move(data),
      info: info,
      path: path,
      lastAccess: getMonoTime()
    )
    self.cache.store(entry)
    source.close()
    newCachedReadFile(self.cache, entry)
  except IOError:
    source.close()
    raise newException(IOError, "缓存读取失败")
  except CatchableError as err:
    source.close()
    raise newException(IOError, err.msg)

proc invalidatePath(self: CachingFileSystem; path: string) {.raises: [IOError].} =
  if path.len == 0:
    return
  self.cache.invalidate(path)

proc invalidateTree(self: CachingFileSystem; path: string) {.raises: [IOError].} =
  if path.len == 0:
    return
  self.cache.invalidatePrefix(path & DirSep)
  self.cache.invalidate(path)

method create*(self: CachingFileSystem; path: string;
               category: vtypes.DiskWriteCategory): vtypes.File =
  self.invalidatePath(path)
  self.base.create(path, category)

method link*(self: CachingFileSystem; oldname, newname: string) =
  self.base.link(oldname, newname)
  self.invalidatePath(newname)

method open*(self: CachingFileSystem; path: string;
             opts: openArray[vtypes.OpenOption]): vtypes.File =
  let cached = self.cache.get(path)
  if cached != nil:
    return newCachedReadFile(self.cache, cached)
  result = self.cacheFile(path, opts)
  if result.isNil:
    result = self.base.open(path, opts)

method openReadWrite*(self: CachingFileSystem; path: string;
                      category: vtypes.DiskWriteCategory;
                      opts: openArray[vtypes.OpenOption]): vtypes.File =
  self.invalidatePath(path)
  self.base.openReadWrite(path, category, opts)

method openDir*(self: CachingFileSystem; path: string): vtypes.File =
  self.base.openDir(path)

method remove*(self: CachingFileSystem; path: string) =
  self.invalidatePath(path)
  self.base.remove(path)

method removeAll*(self: CachingFileSystem; path: string) =
  self.invalidateTree(path)
  self.base.removeAll(path)

method rename*(self: CachingFileSystem; oldname, newname: string) =
  self.invalidatePath(oldname)
  self.invalidatePath(newname)
  self.base.rename(oldname, newname)

method reuseForWrite*(self: CachingFileSystem; oldname, newname: string;
                      category: vtypes.DiskWriteCategory): vtypes.File =
  self.invalidatePath(oldname)
  self.invalidatePath(newname)
  self.base.reuseForWrite(oldname, newname, category)

method mkdirAll*(self: CachingFileSystem; path: string; perm: uint32) =
  self.invalidateTree(path)
  self.base.mkdirAll(path, perm)

method lock*(self: CachingFileSystem; path: string): vtypes.File =
  self.base.lock(path)

method list*(self: CachingFileSystem; path: string): seq[string] =
  self.base.list(path)

method stat*(self: CachingFileSystem; path: string): vtypes.FileInfo =
  let cached = self.cache.get(path)
  if cached != nil:
    return cached.info
  self.base.stat(path)

method pathBase*(self: CachingFileSystem; path: string): string =
  self.base.pathBase(path)

method pathJoin*(self: CachingFileSystem; parts: openArray[string]): string =
  self.base.pathJoin(parts)

method pathDir*(self: CachingFileSystem; path: string): string =
  self.base.pathDir(path)

method diskUsage*(self: CachingFileSystem; path: string): DiskUsage =
  self.base.diskUsage(path)

method unwrap*(self: CachingFileSystem): vtypes.FileSystem =
  self.base

proc getCache*(self: CachingFileSystem): ReadCache =
  self.cache
