# 基于 POSIX 的 VFS 实现，覆盖 Pebble 所需的核心文件原语。

import std/os except File
import std/[oserrors, strutils, times]
import std/posix

import pebble/vfs/types as vtypes
import pebble/vfs/faults

proc cRename(oldname, newname: cstring): cint {.importc: "rename", header: "<stdio.h>".}

type
  PosixFile* = ref object of vtypes.File
    fd: cint
    path: string
    category: vtypes.DiskWriteCategory
    injector: FaultInjector

  PosixFileSystem* = ref object of vtypes.FileSystem
    root: string
    injector: FaultInjector

const
  defaultFileMode = 0o666

proc osErrorAsIoError(path, op: string) {.noReturn.} =
  let code = osLastError()
  raise newException(IOError, op & " '" & path & "': " & osErrorMsg(code))

proc checkNotClosed(file: PosixFile) =
  if file.fd < 0:
    raise newException(IOError, "文件已关闭: " & file.path)

proc applyInjector(injector: FaultInjector; op: FaultOp; path: string) {.raises: [IOError].} =
  if injector != nil:
    injector.shouldFail(op, path)

proc newPosixFile*(fd: cint; path: string; category: vtypes.DiskWriteCategory;
                   injector: FaultInjector): PosixFile =
  PosixFile(fd: fd, path: path, category: category, injector: injector)

proc resolvePath(fs: PosixFileSystem; path: string): string =
  if fs.root.len == 0:
    result = normalizedPath(path)
  else:
    let joined = joinPath(fs.root, path)
    result = normalizedPath(joined)

proc toFileInfo(statBuf: Stat): vtypes.FileInfo =
  let modeVal = cint(statBuf.st_mode)
  let isDirFlag = (modeVal and S_IFMT) == S_IFDIR
  {.push warning[HoleEnumConv]: off.}
  let secs = int64(statBuf.st_mtime)
  {.pop.}
  vtypes.FileInfo(
    size: int64(statBuf.st_size),
    isDir: isDirFlag,
    permissions: uint32(modeVal and 0o7777),
    modTime: fromUnix(secs)
  )

proc setCloseOnExec(fd: cint) =
  when declared(F_SETFD):
    discard fcntl(fd, F_SETFD, FD_CLOEXEC)

proc wrapFd(fs: PosixFileSystem; fd: cint; path: string; category: vtypes.DiskWriteCategory): PosixFile =
  if fd < 0:
    osErrorAsIoError(path, "打开文件失败")
  setCloseOnExec(fd)
  newPosixFile(fd, path, category, fs.injector)

proc newPosixFileSystem*(root: string = ""; injector: FaultInjector = nil): PosixFileSystem =
  PosixFileSystem(
    root: if root.len == 0: "" else: normalizedPath(root),
    injector: injector
  )

proc filePermFromMode(mode: uint32): set[FilePermission] =
  var result: set[FilePermission] = {}
  if (mode and 0o400) != 0: result.incl(fpUserRead)
  if (mode and 0o200) != 0: result.incl(fpUserWrite)
  if (mode and 0o100) != 0: result.incl(fpUserExec)
  if (mode and 0o040) != 0: result.incl(fpGroupRead)
  if (mode and 0o020) != 0: result.incl(fpGroupWrite)
  if (mode and 0o010) != 0: result.incl(fpGroupExec)
  if (mode and 0o004) != 0: result.incl(fpOthersRead)
  if (mode and 0o002) != 0: result.incl(fpOthersWrite)
  if (mode and 0o001) != 0: result.incl(fpOthersExec)
  result

method close*(self: PosixFile) =
  if self.fd < 0:
    return
  if posix.close(self.fd) != 0:
    osErrorAsIoError(self.path, "关闭文件失败")
  self.fd = -1

method read*(self: PosixFile; dest: pointer; size: int): int =
  checkNotClosed(self)
  applyInjector(self.injector, faultFileRead, self.path)
  let readBytes = posix.read(self.fd, dest, size)
  if readBytes < 0:
    osErrorAsIoError(self.path, "读取失败")
  int(readBytes)

method readAt*(self: PosixFile; dest: pointer; size: int; offset: int64): int =
  checkNotClosed(self)
  applyInjector(self.injector, faultFileReadAt, self.path)
  when declared(pread):
    let readBytes = posix.pread(self.fd, dest, size, posix.Off(offset))
    if readBytes < 0:
      osErrorAsIoError(self.path, "pread 失败")
    int(readBytes)
  else:
    raise newException(IOError, "当前平台不支持 pread")

method write*(self: PosixFile; src: pointer; size: int): int =
  checkNotClosed(self)
  applyInjector(self.injector, faultFileWrite, self.path)
  let written = posix.write(self.fd, src, size)
  if written < 0:
    osErrorAsIoError(self.path, "写入失败")
  int(written)

method writeAt*(self: PosixFile; src: pointer; size: int; offset: int64): int =
  checkNotClosed(self)
  applyInjector(self.injector, faultFileWriteAt, self.path)
  when declared(pwrite):
    let written = posix.pwrite(self.fd, src, size, posix.Off(offset))
    if written < 0:
      osErrorAsIoError(self.path, "pwrite 失败")
    int(written)
  else:
    raise newException(IOError, "当前平台不支持 pwrite")

method preallocate*(self: PosixFile; offset, length: int64) =
  checkNotClosed(self)
  applyInjector(self.injector, faultFilePreallocate, self.path)
  when declared(posix_fallocate):
    let rc = posix.posix_fallocate(self.fd, posix.Off(offset), posix.Off(length))
    if rc != 0:
      let message = $posix.strerror(rc)
      raise newException(IOError, "posix_fallocate 失败: " & message & " (" & $rc & ")")
  else:
    let endPos = posix.Off(offset + length)
    if posix.ftruncate(self.fd, endPos) != 0:
      osErrorAsIoError(self.path, "ftruncate 失败")

method stat*(self: PosixFile): vtypes.FileInfo =
  checkNotClosed(self)
  var buf: Stat
  if posix.fstat(self.fd, buf) != 0:
    osErrorAsIoError(self.path, "fstat 失败")
  toFileInfo(buf)

method sync*(self: PosixFile) =
  checkNotClosed(self)
  applyInjector(self.injector, faultFileSync, self.path)
  if posix.fsync(self.fd) != 0:
    osErrorAsIoError(self.path, "fsync 失败")

method syncData*(self: PosixFile) =
  checkNotClosed(self)
  applyInjector(self.injector, faultFileSyncData, self.path)
  when declared(fdatasync):
    if posix.fdatasync(self.fd) != 0:
      osErrorAsIoError(self.path, "fdatasync 失败")
  else:
    if posix.fsync(self.fd) != 0:
      osErrorAsIoError(self.path, "fsync 失败")

method syncTo*(self: PosixFile; length: int64): bool =
  checkNotClosed(self)
  applyInjector(self.injector, faultFileSyncTo, self.path)
  discard length
  if posix.fsync(self.fd) != 0:
    osErrorAsIoError(self.path, "fsync 失败")
  true

method prefetch*(self: PosixFile; offset, length: int64) =
  checkNotClosed(self)
  applyInjector(self.injector, faultFilePrefetch, self.path)
  when defined(linux):
    discard posix.posix_fadvise(self.fd, posix.Off(offset), posix.Off(length), POSIX_FADV_WILLNEED)
  else:
    discard

method fd*(self: PosixFile): uint =
  if self.fd < 0:
    InvalidFileDescriptor
  else:
    uint(self.fd)

method map*(self: PosixFile; offset: int64; length: int;
            access: vtypes.MmapAccess): vtypes.MmapRegion =
  checkNotClosed(self)
  applyInjector(self.injector, faultFileMap, self.path)
  let prot = case access
    of mapReadOnly: PROT_READ
    of mapReadWrite: PROT_READ or PROT_WRITE
  var flags = MAP_SHARED
  when defined(linux):
    discard
  let mapped = posix.mmap(nil, length, prot, cint(flags), self.fd, posix.Off(offset))
  if mapped == MAP_FAILED:
    osErrorAsIoError(self.path, "mmap 失败")
  var region = vtypes.MmapRegion(base: mapped, length: length, access: access)
  region.setReleaser(proc (base: pointer; size: int) {.gcsafe.} =
    if base != nil:
      discard posix.munmap(base, size)
  )
  region

proc applyOptions(file: vtypes.File; opts: openArray[vtypes.OpenOption]) =
  discard

proc openFile(fs: PosixFileSystem; path: string; flags: cint; perm: uint32;
              category: vtypes.DiskWriteCategory;
              opts: openArray[vtypes.OpenOption]): vtypes.File {.raises: [IOError].} =
  var localFlags = flags
  when declared(O_CLOEXEC):
    localFlags = localFlags or O_CLOEXEC
  let fd = posix.open(path.cstring, localFlags, Mode(perm))
  if fd < 0:
    osErrorAsIoError(path, "打开文件失败")
  let wrapped = wrapFd(fs, fd, path, category)
  applyOptions(wrapped, opts)
  wrapped

proc ensureFileRemoved(path: string) =
  if posix.unlink(path.cstring) != 0:
    let code = osLastError()
    if code != OSErrorCode(ENOENT):
      osErrorAsIoError(path, "删除旧文件失败")

method create*(self: PosixFileSystem; path: string;
               category: vtypes.DiskWriteCategory): vtypes.File {.gcsafe, raises: [IOError].} =
  let resolved = resolvePath(self, path)
  applyInjector(self.injector, faultCreate, resolved)
  let flags = O_RDWR or O_CREAT or O_EXCL
  var fd = posix.open(resolved.cstring, cint(flags), Mode(defaultFileMode))
  while fd < 0 and osLastError() == OSErrorCode(EEXIST):
    ensureFileRemoved(resolved)
    fd = posix.open(resolved.cstring, cint(flags), Mode(defaultFileMode))
  if fd < 0:
    osErrorAsIoError(resolved, "创建文件失败")
  wrapFd(self, fd, resolved, category)

method link*(self: PosixFileSystem; oldname, newname: string) {.gcsafe, raises: [IOError].} =
  let oldPath = resolvePath(self, oldname)
  let newPath = resolvePath(self, newname)
  applyInjector(self.injector, faultLink, newPath)
  try:
    createHardlink(oldPath, newPath)
  except OSError as err:
    raise newException(IOError, err.msg)

method open*(self: PosixFileSystem; path: string;
             opts: openArray[vtypes.OpenOption]): vtypes.File {.gcsafe, raises: [IOError].} =
  let resolved = resolvePath(self, path)
  applyInjector(self.injector, faultOpen, resolved)
  result = openFile(self, resolved, O_RDONLY, uint32(defaultFileMode), vtypes.WriteCategoryUnspecified, opts)

method openReadWrite*(self: PosixFileSystem; path: string;
                      category: vtypes.DiskWriteCategory;
                      opts: openArray[vtypes.OpenOption]): vtypes.File {.gcsafe, raises: [IOError].} =
  let resolved = resolvePath(self, path)
  applyInjector(self.injector, faultOpenReadWrite, resolved)
  result = openFile(self, resolved, O_RDWR or O_CREAT, uint32(defaultFileMode), category, opts)

method openDir*(self: PosixFileSystem; path: string): vtypes.File {.gcsafe, raises: [IOError].} =
  let resolved = resolvePath(self, path)
  applyInjector(self.injector, faultOpenDir, resolved)
  var flags = O_RDONLY
  when declared(O_DIRECTORY):
    flags = flags or O_DIRECTORY
  result = openFile(self, resolved, flags, uint32(defaultFileMode), vtypes.WriteCategoryUnspecified, [])

method remove*(self: PosixFileSystem; path: string) {.gcsafe, raises: [IOError].} =
  let resolved = resolvePath(self, path)
  applyInjector(self.injector, faultRemove, resolved)
  if posix.unlink(resolved.cstring) != 0:
    let code = osLastError()
    if code == OSErrorCode(EISDIR):
      if posix.rmdir(resolved.cstring) != 0:
        osErrorAsIoError(resolved, "rmdir 失败")
    elif code != OSErrorCode(ENOENT):
      osErrorAsIoError(resolved, "unlink 失败")

proc removeTree(path: string) {.raises: [IOError].} =
  if not dirExists(path) and not fileExists(path):
    return
  if dirExists(path):
    try:
      for entry in walkDir(path):
        let child = entry.path
        case entry.kind
        of pcDir, pcLinkToDir:
          removeTree(child)
        else:
          if posix.unlink(child.cstring) != 0 and osLastError() != OSErrorCode(ENOENT):
            osErrorAsIoError(child, "unlink 失败")
    except OSError as err:
      raise newException(IOError, err.msg)
    if posix.rmdir(path.cstring) != 0 and osLastError() != OSErrorCode(ENOENT):
      osErrorAsIoError(path, "rmdir 失败")
  else:
    if posix.unlink(path.cstring) != 0 and osLastError() != OSErrorCode(ENOENT):
      osErrorAsIoError(path, "unlink 失败")

method removeAll*(self: PosixFileSystem; path: string) {.gcsafe, raises: [IOError].} =
  let resolved = resolvePath(self, path)
  applyInjector(self.injector, faultRemoveAll, resolved)
  removeTree(resolved)

method rename*(self: PosixFileSystem; oldname, newname: string) {.gcsafe, raises: [IOError].} =
  let oldPath = resolvePath(self, oldname)
  let newPath = resolvePath(self, newname)
  applyInjector(self.injector, faultRename, newPath)
  if cRename(oldPath.cstring, newPath.cstring) != 0:
    osErrorAsIoError(newPath, "rename 失败")

method reuseForWrite*(self: PosixFileSystem; oldname, newname: string;
                      category: vtypes.DiskWriteCategory): vtypes.File {.gcsafe, raises: [IOError].} =
  let oldPath = resolvePath(self, oldname)
  let newPath = resolvePath(self, newname)
  applyInjector(self.injector, faultReuseForWrite, newPath)
  if cRename(oldPath.cstring, newPath.cstring) != 0:
    if osLastError() == OSErrorCode(ENOENT):
      return openFile(self, newPath, O_RDWR or O_CREAT, uint32(defaultFileMode), category, [])
    osErrorAsIoError(newPath, "reuseForWrite 重命名失败")
  openFile(self, newPath, O_RDWR, uint32(defaultFileMode), category, [])

proc ensureDir(path: string; perm: uint32) {.raises: [IOError].} =
  if dirExists(path):
    return
  if posix.mkdir(path.cstring, Mode(perm)) != 0 and osLastError() != OSErrorCode(EEXIST):
    osErrorAsIoError(path, "mkdir 失败")

method mkdirAll*(self: PosixFileSystem; path: string; perm: uint32) {.gcsafe, raises: [IOError].} =
  let resolved = resolvePath(self, path)
  applyInjector(self.injector, faultMkdirAll, resolved)
  var current = if resolved.len > 0 and resolved[0] == DirSep: $DirSep else: ""
  for part in resolved.split({DirSep, AltSep}):
    if part.len == 0:
      continue
    if current.len > 0 and current[^1] == DirSep:
      current &= part
    elif current.len == 0:
      current = part
    else:
      current = current & DirSep & part
    ensureDir(current, perm)

type
  PosixLock* = ref object of vtypes.File
    fd: cint
    path: string

method close*(self: PosixLock) =
  if self.fd < 0:
    return
  discard posix.close(self.fd)
  self.fd = -1

method lock*(self: PosixFileSystem; path: string): vtypes.File =
  let resolved = resolvePath(self, path)
  applyInjector(self.injector, faultLock, resolved)
  let fd = posix.open(resolved.cstring, O_RDWR or O_CREAT, Mode(0o600))
  if fd < 0:
    osErrorAsIoError(resolved, "打开锁文件失败")
  if posix.ftruncate(fd, 0) != 0:
    let err = osLastError()
    discard posix.close(fd)
    raise newException(IOError, "截断锁文件失败: " & osErrorMsg(err))
  var fl: Tflock
  fl.l_type = cshort(F_WRLCK)
  fl.l_whence = cshort(SEEK_SET)
  fl.l_start = posix.Off(0)
  fl.l_len = posix.Off(0)
  if fcntl(fd, F_SETLK, addr(fl)) != 0:
    let err = osLastError()
    discard posix.close(fd)
    raise newException(IOError, "获取文件锁失败: " & osErrorMsg(err))
  setCloseOnExec(fd)
  PosixLock(fd: fd, path: resolved)

method list*(self: PosixFileSystem; path: string): seq[string] {.gcsafe, raises: [IOError].} =
  let resolved = resolvePath(self, path)
  applyInjector(self.injector, faultList, resolved)
  if not dirExists(resolved):
    osErrorAsIoError(resolved, "目录不存在")
  var entries: seq[string] = @[]
  try:
    for entry in walkDir(resolved):
      entries.add(splitPath(entry.path).tail)
  except OSError as err:
    raise newException(IOError, err.msg)
  entries

method stat*(self: PosixFileSystem; path: string): vtypes.FileInfo {.gcsafe, raises: [IOError].} =
  let resolved = resolvePath(self, path)
  applyInjector(self.injector, faultStat, resolved)
  var st: Stat
  if posix.stat(resolved.cstring, st) != 0:
    osErrorAsIoError(resolved, "stat 失败")
  toFileInfo(st)

method pathBase*(self: PosixFileSystem; path: string): string =
  splitPath(path).tail

method pathJoin*(self: PosixFileSystem; parts: openArray[string]): string =
  if parts.len == 0:
    return "."
  var acc = parts[0]
  for idx in 1 ..< parts.len:
    acc = joinPath(acc, parts[idx])
  acc

method pathDir*(self: PosixFileSystem; path: string): string =
  splitPath(path).head

method diskUsage*(self: PosixFileSystem; path: string): vtypes.DiskUsage =
  let resolved = resolvePath(self, path)
  applyInjector(self.injector, faultDiskUsage, resolved)
  var info: Statvfs
  if posix.statvfs(resolved.cstring, info) != 0:
    osErrorAsIoError(resolved, "statvfs 失败")
  let total = uint64(info.f_frsize) * uint64(info.f_blocks)
  let avail = uint64(info.f_frsize) * uint64(info.f_bavail)
  vtypes.DiskUsage(
    availBytes: avail,
    totalBytes: total,
    usedBytes: total - (uint64(info.f_frsize) * uint64(info.f_bfree))
  )

method unwrap*(self: PosixFileSystem): vtypes.FileSystem =
  nil
