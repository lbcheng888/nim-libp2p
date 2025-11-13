# 提供挂载文件系统包装器，将逻辑前缀映射到任意物理目录，便于复用 Pebble
# 现有测试数据或将 Nim 侧请求路由到不同介质。

import std/algorithm
import std/strutils
import std/os except File, FileInfo

import pebble/vfs/types as vtypes

type
  MountPoint* = object
    ## 逻辑路径前缀与物理路径前缀的绑定。
    logicalPrefix*: string
    physicalPrefix*: string
    readOnly*: bool

  MountBinding = object
    logical: string
    physical: string
    readOnly: bool

  MountResolution = object
    path: string
    readOnly: bool
    matched: bool

  MountFileSystem* = ref object of vtypes.FileSystem
    base: vtypes.FileSystem
    bindings: seq[MountBinding]
    passThrough: bool

proc mountPoint*(logicalPrefix, physicalPrefix: string; readOnly: bool = true): MountPoint =
  ## 构造挂载点描述，默认以只读方式暴露物理目录，避免误修改测试数据。
  MountPoint(logicalPrefix: logicalPrefix, physicalPrefix: physicalPrefix, readOnly: readOnly)

proc readOnlyMount*(logicalPrefix, physicalPrefix: string): MountPoint =
  mountPoint(logicalPrefix, physicalPrefix, true)

proc readWriteMount*(logicalPrefix, physicalPrefix: string): MountPoint =
  mountPoint(logicalPrefix, physicalPrefix, false)

proc isDriveRoot(path: string): bool =
  result = path.len == 3 and path[1] == ':' and path[0].isAlphaAscii() and
           (path[2] == DirSep or (AltSep != '\0' and path[2] == AltSep))

proc stripTrailingPathSep(path: string): string =
  result = path
  if result.len == 0:
    return
  if isDriveRoot(result):
    if result[^1] != DirSep:
      result[^1] = DirSep
    return
  while result.len > 1 and (result[^1] == DirSep or (AltSep != '\0' and result[^1] == AltSep)):
    result.setLen(result.len - 1)

proc canonicalLogical(path: string): string =
  if path.len == 0:
    return ""
  var normalized = normalizedPath(path)
  if AltSep != '\0':
    normalized = normalized.replace(AltSep, DirSep)
  normalized = stripTrailingPathSep(normalized)
  if normalized == ".":
    ""
  else:
    normalized

proc canonicalPhysical(path: string): string =
  if path.len == 0:
    return ""
  var expanded = expandTilde(path)
  var normalized = normalizedPath(expanded)
  if AltSep != '\0':
    normalized = normalized.replace(AltSep, DirSep)
  stripTrailingPathSep(normalized)

proc prefixWithSep(binding: MountBinding): string =
  if binding.logical.len == 0:
    ""
  elif binding.logical[^1] == DirSep:
    binding.logical
  else:
    binding.logical & DirSep

proc stripLeadingSep(path: string): string =
  var idx = 0
  while idx < path.len and (path[idx] == DirSep or (AltSep != '\0' and path[idx] == AltSep)):
    inc idx
  if idx >= path.len:
    ""
  else:
    path[idx .. ^1]

proc resolve(self: MountFileSystem; path: string): MountResolution =
  let logical = canonicalLogical(path)
  for binding in self.bindings:
    if binding.logical.len == 0 or logical == binding.logical or
       (logical.len > binding.logical.len and logical.startsWith(prefixWithSep(binding))):
      let suffix =
        if binding.logical.len == 0:
          logical
        elif logical.len == binding.logical.len:
          ""
        else:
          stripLeadingSep(logical[binding.logical.len .. ^1])
      let physical =
        if suffix.len == 0:
          binding.physical
        elif binding.physical.len == 0:
          suffix
        else:
          joinPath(binding.physical, suffix)
      return MountResolution(path: physical, readOnly: binding.readOnly, matched: true)
  if self.passThrough:
    MountResolution(path: logical, readOnly: false, matched: false)
  else:
    raise newException(IOError, "路径未挂载: " & path)

proc ensureWritable(self: MountFileSystem; res: MountResolution; op, original: string) =
  if res.readOnly:
    raise newException(IOError, op & " 命中只读挂载点: " & original)

proc newMountFileSystem*(base: vtypes.FileSystem; mounts: openArray[MountPoint];
                         passThroughUnmatched: bool = false): MountFileSystem =
  ## 使用给定挂载点包装文件系统。可选 passThroughUnmatched 允许落在未匹配前缀的路径直接透传。
  if base.isNil:
    raise newException(ValueError, "base FileSystem 不可为 nil")
  var bindings: seq[MountBinding] = @[]
  for mount in mounts:
    let logical = canonicalLogical(mount.logicalPrefix)
    let physical = canonicalPhysical(mount.physicalPrefix)
    bindings.add(MountBinding(logical: logical, physical: physical, readOnly: mount.readOnly))
  bindings.sort(proc (a, b: MountBinding): int =
    cmp(b.logical.len, a.logical.len)
  )
  MountFileSystem(base: base, bindings: bindings, passThrough: passThroughUnmatched)

proc addMount*(self: MountFileSystem; mount: MountPoint) =
  ## 运行期新增挂载映射。
  let logical = canonicalLogical(mount.logicalPrefix)
  let physical = canonicalPhysical(mount.physicalPrefix)
  self.bindings.add(MountBinding(logical: logical, physical: physical, readOnly: mount.readOnly))
  self.bindings.sort(proc (a, b: MountBinding): int =
    cmp(b.logical.len, a.logical.len)
  )

proc mountPoints*(self: MountFileSystem): seq[MountPoint] =
  for binding in self.bindings:
    result.add(MountPoint(logicalPrefix: binding.logical,
                          physicalPrefix: binding.physical,
                          readOnly: binding.readOnly))

method create*(self: MountFileSystem; path: string; category: vtypes.DiskWriteCategory): vtypes.File {.gcsafe, raises: [IOError].} =
  let res = self.resolve(path)
  self.ensureWritable(res, "create", path)
  self.base.create(res.path, category)

method link*(self: MountFileSystem; oldname, newname: string) {.gcsafe, raises: [IOError].} =
  let src = self.resolve(oldname)
  let dst = self.resolve(newname)
  self.ensureWritable(src, "link", oldname)
  self.ensureWritable(dst, "link", newname)
  self.base.link(src.path, dst.path)

method open*(self: MountFileSystem; path: string; opts: openArray[vtypes.OpenOption]): vtypes.File {.gcsafe, raises: [IOError].} =
  let res = self.resolve(path)
  self.base.open(res.path, opts)

method openReadWrite*(self: MountFileSystem; path: string; category: vtypes.DiskWriteCategory;
                      opts: openArray[vtypes.OpenOption]): vtypes.File {.gcsafe, raises: [IOError].} =
  let res = self.resolve(path)
  self.ensureWritable(res, "openReadWrite", path)
  self.base.openReadWrite(res.path, category, opts)

method openDir*(self: MountFileSystem; path: string): vtypes.File {.gcsafe, raises: [IOError].} =
  let res = self.resolve(path)
  self.base.openDir(res.path)

method remove*(self: MountFileSystem; path: string) {.gcsafe, raises: [IOError].} =
  let res = self.resolve(path)
  self.ensureWritable(res, "remove", path)
  self.base.remove(res.path)

method removeAll*(self: MountFileSystem; path: string) {.gcsafe, raises: [IOError].} =
  let res = self.resolve(path)
  self.ensureWritable(res, "removeAll", path)
  self.base.removeAll(res.path)

method rename*(self: MountFileSystem; oldname, newname: string) {.gcsafe, raises: [IOError].} =
  let src = self.resolve(oldname)
  let dst = self.resolve(newname)
  self.ensureWritable(src, "rename", oldname)
  self.ensureWritable(dst, "rename", newname)
  self.base.rename(src.path, dst.path)

method reuseForWrite*(self: MountFileSystem; oldname, newname: string;
                      category: vtypes.DiskWriteCategory): vtypes.File {.gcsafe, raises: [IOError].} =
  let src = self.resolve(oldname)
  let dst = self.resolve(newname)
  self.ensureWritable(src, "reuseForWrite", oldname)
  self.ensureWritable(dst, "reuseForWrite", newname)
  self.base.reuseForWrite(src.path, dst.path, category)

method mkdirAll*(self: MountFileSystem; path: string; perm: uint32) {.gcsafe, raises: [IOError].} =
  let res = self.resolve(path)
  self.ensureWritable(res, "mkdirAll", path)
  self.base.mkdirAll(res.path, perm)

method lock*(self: MountFileSystem; path: string): vtypes.File {.gcsafe, raises: [IOError].} =
  let res = self.resolve(path)
  self.ensureWritable(res, "lock", path)
  self.base.lock(res.path)

method list*(self: MountFileSystem; path: string): seq[string] {.gcsafe, raises: [IOError].} =
  let res = self.resolve(path)
  self.base.list(res.path)

method stat*(self: MountFileSystem; path: string): vtypes.FileInfo {.gcsafe, raises: [IOError].} =
  let res = self.resolve(path)
  self.base.stat(res.path)

method pathBase*(self: MountFileSystem; path: string): string {.gcsafe, raises: [IOError].} =
  self.base.pathBase(path)

method pathJoin*(self: MountFileSystem; parts: openArray[string]): string {.gcsafe, raises: [IOError].} =
  self.base.pathJoin(parts)

method pathDir*(self: MountFileSystem; path: string): string {.gcsafe, raises: [IOError].} =
  self.base.pathDir(path)

method diskUsage*(self: MountFileSystem; path: string): vtypes.DiskUsage {.gcsafe, raises: [IOError].} =
  let res = self.resolve(path)
  self.base.diskUsage(res.path)

method unwrap*(self: MountFileSystem): vtypes.FileSystem {.gcsafe, raises: [IOError].} =
  self.base
