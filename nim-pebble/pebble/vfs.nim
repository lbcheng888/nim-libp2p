# VFS 子系统入口，向外暴露类型、故障注入、增强层与默认 POSIX 文件系统实现。

import pebble/vfs/types as vfs_types
import pebble/vfs/faults as vfs_faults
import pebble/vfs/posix_fs as vfs_posix
import pebble/vfs/buffered_fs as vfs_buffered
import pebble/vfs/cache_fs as vfs_cache
import pebble/vfs/prefetch_rate as vfs_prefetch
import pebble/vfs/mount_fs as vfs_mount
import pebble/vfs/compat_report as vfs_compat

export vfs_types
export vfs_faults
export vfs_posix
export vfs_buffered
export vfs_cache
export vfs_prefetch
export vfs_mount
export vfs_compat

proc newDefaultPebbleVfs*(root: string = ""; injector: FaultInjector = nil;
                          bufferSize: int = 64 * 1024; cacheCapacityBytes: int64 = 128 * 1024 * 1024;
                          cacheEntries: int = 512; readBytesPerSec: float = 0;
                          writeBytesPerSec: float = 0; asyncPrefetch: bool = true;
                          maxPrefetchInFlight: int = 8): FileSystem =
  ## 构建默认的 VFS 组合：POSIX 后端 → 缓冲层 → 只读缓存 → 限速/异步预取。
  var fs: FileSystem = newPosixFileSystem(root, injector)
  fs = newBufferedFileSystem(fs, bufferSize)
  fs = newCachingFileSystem(fs, cacheCapacityBytes, cacheEntries)
  fs = newRateLimitedFileSystem(fs, readBytesPerSec, writeBytesPerSec, asyncPrefetch, maxPrefetchInFlight)
  fs

proc newMountedPebbleVfs*(mounts: openArray[MountPoint]; root: string = "";
                          injector: FaultInjector = nil; bufferSize: int = 64 * 1024;
                          cacheCapacityBytes: int64 = 128 * 1024 * 1024; cacheEntries: int = 512;
                          readBytesPerSec: float = 0; writeBytesPerSec: float = 0;
                          asyncPrefetch: bool = true; maxPrefetchInFlight: int = 8;
                          passThroughUnmatched: bool = false): FileSystem =
  ## 在默认栈外层叠加挂载层，便于直接映射到 Go 测试数据目录。
  var baseFs = newDefaultPebbleVfs(root, injector, bufferSize, cacheCapacityBytes,
                                   cacheEntries, readBytesPerSec, writeBytesPerSec,
                                   asyncPrefetch, maxPrefetchInFlight)
  if mounts.len == 0:
    return baseFs
  newMountFileSystem(baseFs, mounts, passThroughUnmatched)
