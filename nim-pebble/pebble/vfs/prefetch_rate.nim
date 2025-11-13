# 预取与速率限制层，为 Pebble VFS 提供异步预取与令牌桶限速能力。

import std/[atomics, math, monotimes, threadpool, times, os]

import pebble/vfs/types as vtypes

type
  RateLimiter* = ref object
    capacity: float
    tokens: float
    refillPerNs: float
    lastRefill: MonoTime

  PrefetchExecutor* = ref object
    enabled: bool
    maxInFlight: int
    inFlight: Atomic[int]

  RateLimitedFile* = ref object of vtypes.File
    inner: vtypes.File
    readLimiter: RateLimiter
    writeLimiter: RateLimiter
    prefetcher: PrefetchExecutor
    asyncPrefetch: bool
    path: string

  RateLimitedFileSystem* = ref object of vtypes.FileSystem
    base: vtypes.FileSystem
    readLimiter: RateLimiter
    writeLimiter: RateLimiter
    prefetcher: PrefetchExecutor
    asyncPrefetch: bool

proc newRateLimiter*(bytesPerSecond: float; burst: float = 0): RateLimiter =
  if bytesPerSecond <= 0:
    return nil
  let burstSize = if burst <= 0: bytesPerSecond else: burst
  RateLimiter(
    capacity: burstSize,
    tokens: burstSize,
    refillPerNs: bytesPerSecond / 1_000_000_000.0,
    lastRefill: getMonoTime()
  )

proc refill(limiter: RateLimiter) =
  if limiter.isNil:
    return
  let now = getMonoTime()
  let delta = now - limiter.lastRefill
  let nanos = float(delta.inNanoseconds)
  if nanos <= 0:
    return
  let added = nanos * limiter.refillPerNs
  limiter.tokens = min(limiter.capacity, limiter.tokens + added)
  limiter.lastRefill = now

proc acquire*(limiter: RateLimiter; amount: int) =
  if limiter.isNil or amount <= 0:
    return
  var need = float(amount)
  while true:
    limiter.refill()
    if limiter.tokens >= need:
      limiter.tokens -= need
      break
    let deficit = need - limiter.tokens
    let waitNs = max(deficit / limiter.refillPerNs, 1.0)
    let waitMs = max(1, int(ceil(waitNs / 1_000_000.0)))
    sleep(waitMs)

proc newPrefetchExecutor*(maxInFlight: int = 8; enabled: bool = true): PrefetchExecutor =
  if not enabled:
    return PrefetchExecutor(enabled: false, maxInFlight: 0)
  var counter: Atomic[int]
  store(counter, 0, moRelaxed)
  let limit = if maxInFlight <= 0: high(int) else: maxInFlight
  PrefetchExecutor(enabled: true, maxInFlight: limit, inFlight: counter)

proc schedule(executor: PrefetchExecutor; job: proc () {.gcsafe, raises: [IOError].}) {.raises: [IOError].} =
  if executor.isNil or not executor.enabled:
    job()
    return
  job()

proc newRateLimitedFile*(inner: vtypes.File; path: string;
                         readLimiter, writeLimiter: RateLimiter;
                         prefetcher: PrefetchExecutor; asyncPrefetch: bool): RateLimitedFile =
  RateLimitedFile(
    inner: inner,
    readLimiter: readLimiter,
    writeLimiter: writeLimiter,
    prefetcher: prefetcher,
    asyncPrefetch: asyncPrefetch,
    path: path
  )

method close*(self: RateLimitedFile) =
  if not self.inner.isNil:
    self.inner.close()
    self.inner = nil

method read*(self: RateLimitedFile; dest: pointer; size: int): int =
  if not self.readLimiter.isNil and size > 0:
    self.readLimiter.acquire(size)
  self.inner.read(dest, size)

method readAt*(self: RateLimitedFile; dest: pointer; size: int; offset: int64): int =
  if not self.readLimiter.isNil and size > 0:
    self.readLimiter.acquire(size)
  self.inner.readAt(dest, size, offset)

method write*(self: RateLimitedFile; src: pointer; size: int): int =
  if not self.writeLimiter.isNil and size > 0:
    self.writeLimiter.acquire(size)
  self.inner.write(src, size)

method writeAt*(self: RateLimitedFile; src: pointer; size: int; offset: int64): int =
  if not self.writeLimiter.isNil and size > 0:
    self.writeLimiter.acquire(size)
  self.inner.writeAt(src, size, offset)

method preallocate*(self: RateLimitedFile; offset, length: int64) =
  self.inner.preallocate(offset, length)

method stat*(self: RateLimitedFile): vtypes.FileInfo =
  self.inner.stat()

method sync*(self: RateLimitedFile) =
  self.inner.sync()

method syncData*(self: RateLimitedFile) =
  self.inner.syncData()

method syncTo*(self: RateLimitedFile; length: int64): bool =
  self.inner.syncTo(length)

method prefetch*(self: RateLimitedFile; offset, length: int64) =
  if length <= 0:
    return
  if not self.asyncPrefetch or self.prefetcher.isNil:
    if not self.readLimiter.isNil:
      self.readLimiter.acquire(int(length))
    self.inner.prefetch(offset, length)
    return
  let innerFile = self.inner
  let limiter = self.readLimiter
  let requestLen = length
  self.prefetcher.schedule(proc () {.gcsafe, raises: [IOError].} =
    try:
      if not limiter.isNil:
        limiter.acquire(int(requestLen))
      innerFile.prefetch(offset, requestLen)
    except IOError:
      discard
  )

method fd*(self: RateLimitedFile): uint =
  self.inner.fd()

method map*(self: RateLimitedFile; offset: int64; length: int;
            access: vtypes.MmapAccess): vtypes.MmapRegion =
  self.inner.map(offset, length, access)

proc wrap(self: RateLimitedFileSystem; file: vtypes.File; path: string): vtypes.File =
  if file.isNil:
    return nil
  if self.readLimiter.isNil and self.writeLimiter.isNil and (self.prefetcher.isNil or not self.asyncPrefetch):
    return file
  newRateLimitedFile(file, path, self.readLimiter, self.writeLimiter, self.prefetcher, self.asyncPrefetch)

proc newRateLimitedFileSystem*(base: vtypes.FileSystem; readBytesPerSec: float = 0;
                               writeBytesPerSec: float = 0; asyncPrefetch: bool = true;
                               maxPrefetchInFlight: int = 8): RateLimitedFileSystem =
  if base.isNil:
    raise newException(ValueError, "base FileSystem 不可为 nil")
  RateLimitedFileSystem(
    base: base,
    readLimiter: newRateLimiter(readBytesPerSec),
    writeLimiter: newRateLimiter(writeBytesPerSec),
    prefetcher: newPrefetchExecutor(maxPrefetchInFlight, asyncPrefetch),
    asyncPrefetch: asyncPrefetch
  )

method create*(self: RateLimitedFileSystem; path: string;
               category: vtypes.DiskWriteCategory): vtypes.File =
  self.wrap(self.base.create(path, category), path)

method link*(self: RateLimitedFileSystem; oldname, newname: string) =
  self.base.link(oldname, newname)

method open*(self: RateLimitedFileSystem; path: string;
             opts: openArray[vtypes.OpenOption]): vtypes.File =
  self.wrap(self.base.open(path, opts), path)

method openReadWrite*(self: RateLimitedFileSystem; path: string;
                      category: vtypes.DiskWriteCategory;
                      opts: openArray[vtypes.OpenOption]): vtypes.File =
  self.wrap(self.base.openReadWrite(path, category, opts), path)

method openDir*(self: RateLimitedFileSystem; path: string): vtypes.File =
  self.base.openDir(path)

method remove*(self: RateLimitedFileSystem; path: string) =
  self.base.remove(path)

method removeAll*(self: RateLimitedFileSystem; path: string) =
  self.base.removeAll(path)

method rename*(self: RateLimitedFileSystem; oldname, newname: string) =
  self.base.rename(oldname, newname)

method reuseForWrite*(self: RateLimitedFileSystem; oldname, newname: string;
                      category: vtypes.DiskWriteCategory): vtypes.File =
  self.wrap(self.base.reuseForWrite(oldname, newname, category), newname)

method mkdirAll*(self: RateLimitedFileSystem; path: string; perm: uint32) =
  self.base.mkdirAll(path, perm)

method lock*(self: RateLimitedFileSystem; path: string): vtypes.File =
  self.base.lock(path)

method list*(self: RateLimitedFileSystem; path: string): seq[string] =
  self.base.list(path)

method stat*(self: RateLimitedFileSystem; path: string): vtypes.FileInfo =
  self.base.stat(path)

method pathBase*(self: RateLimitedFileSystem; path: string): string =
  self.base.pathBase(path)

method pathJoin*(self: RateLimitedFileSystem; parts: openArray[string]): string =
  self.base.pathJoin(parts)

method pathDir*(self: RateLimitedFileSystem; path: string): string =
  self.base.pathDir(path)

method diskUsage*(self: RateLimitedFileSystem; path: string): vtypes.DiskUsage =
  self.base.diskUsage(path)

method unwrap*(self: RateLimitedFileSystem): vtypes.FileSystem =
  self.base
