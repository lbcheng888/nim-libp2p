# VFS 公共类型与抽象接口定义。
# 提供文件句柄、文件系统、内存映射等抽象，便于后续扩展不同后端实现。

import std/[options, times]

type
  DiskWriteCategory* = distinct string

const
  WriteCategoryUnspecified* = DiskWriteCategory("unspecified")

proc diskCategory*(value: string): DiskWriteCategory =
  ## 创建磁盘写入类别标签。
  DiskWriteCategory(value)

proc toString*(category: DiskWriteCategory): string =
  string(category)

type
  FileInfo* = object
    size*: int64
    isDir*: bool
    permissions*: uint32
    modTime*: Time

  MmapAccess* = enum
    mapReadOnly,
    mapReadWrite

  ReleaseHook* = proc (base: pointer; length: int) {.gcsafe.}

  MmapRegion* = object
    base*: pointer
    length*: int
    access*: MmapAccess
    releaser: ReleaseHook

  File* = ref object of RootObj
  FileSystem* = ref object of RootObj
  OpenOption* = ref object of RootObj

const
  InvalidFileDescriptor* = high(uint)

template abstractMethod(name: string) =
  raise newException(IOError, "调用了抽象方法: " & name)

method close*(self: File) {.base, gcsafe, raises: [IOError].} =
  abstractMethod("File.close")

method read*(self: File; dest: pointer; size: int): int {.base, gcsafe, raises: [IOError].} =
  abstractMethod("File.read")

method readAt*(self: File; dest: pointer; size: int; offset: int64): int {.base, gcsafe, raises: [IOError].} =
  abstractMethod("File.readAt")

method write*(self: File; src: pointer; size: int): int {.base, gcsafe, raises: [IOError].} =
  abstractMethod("File.write")

method writeAt*(self: File; src: pointer; size: int; offset: int64): int {.base, gcsafe, raises: [IOError].} =
  abstractMethod("File.writeAt")

method preallocate*(self: File; offset, length: int64) {.base, gcsafe, raises: [IOError].} =
  abstractMethod("File.preallocate")

method stat*(self: File): FileInfo {.base, gcsafe, raises: [IOError].} =
  abstractMethod("File.stat")

method sync*(self: File) {.base, gcsafe, raises: [IOError].} =
  abstractMethod("File.sync")

method syncData*(self: File) {.base, gcsafe, raises: [IOError].} =
  abstractMethod("File.syncData")

method syncTo*(self: File; length: int64): bool {.base, gcsafe, raises: [IOError].} =
  abstractMethod("File.syncTo")

method prefetch*(self: File; offset, length: int64) {.base, gcsafe, raises: [IOError].} =
  abstractMethod("File.prefetch")

method fd*(self: File): uint {.base, gcsafe, raises: [IOError].} =
  abstractMethod("File.fd")

method map*(self: File; offset: int64; length: int; access: MmapAccess): MmapRegion {.base, gcsafe, raises: [IOError].} =
  abstractMethod("File.map")

method apply*(self: OpenOption; file: File) {.base, gcsafe.} =
  abstractMethod("OpenOption.apply")

type
  DiskUsage* = object
    availBytes*: uint64
    totalBytes*: uint64
    usedBytes*: uint64

method create*(self: FileSystem; path: string; category: DiskWriteCategory): File {.base, gcsafe, raises: [IOError].} =
  abstractMethod("FileSystem.create")

method link*(self: FileSystem; oldname, newname: string) {.base, gcsafe, raises: [IOError].} =
  abstractMethod("FileSystem.link")

method open*(self: FileSystem; path: string; opts: openArray[OpenOption]): File {.base, gcsafe, raises: [IOError].} =
  abstractMethod("FileSystem.open")

method openReadWrite*(self: FileSystem; path: string; category: DiskWriteCategory;
                      opts: openArray[OpenOption]): File {.base, gcsafe, raises: [IOError].} =
  abstractMethod("FileSystem.openReadWrite")

method openDir*(self: FileSystem; path: string): File {.base, gcsafe, raises: [IOError].} =
  abstractMethod("FileSystem.openDir")

method remove*(self: FileSystem; path: string) {.base, gcsafe, raises: [IOError].} =
  abstractMethod("FileSystem.remove")

method removeAll*(self: FileSystem; path: string) {.base, gcsafe, raises: [IOError].} =
  abstractMethod("FileSystem.removeAll")

method rename*(self: FileSystem; oldname, newname: string) {.base, gcsafe, raises: [IOError].} =
  abstractMethod("FileSystem.rename")

method reuseForWrite*(self: FileSystem; oldname, newname: string; category: DiskWriteCategory): File {.base, gcsafe, raises: [IOError].} =
  abstractMethod("FileSystem.reuseForWrite")

method mkdirAll*(self: FileSystem; path: string; perm: uint32) {.base, gcsafe, raises: [IOError].} =
  abstractMethod("FileSystem.mkdirAll")

method lock*(self: FileSystem; path: string): File {.base, gcsafe, raises: [IOError].} =
  abstractMethod("FileSystem.lock")

method list*(self: FileSystem; path: string): seq[string] {.base, gcsafe, raises: [IOError].} =
  abstractMethod("FileSystem.list")

method stat*(self: FileSystem; path: string): FileInfo {.base, gcsafe, raises: [IOError].} =
  abstractMethod("FileSystem.stat")

method pathBase*(self: FileSystem; path: string): string {.base, gcsafe, raises: [IOError].} =
  abstractMethod("FileSystem.pathBase")

method pathJoin*(self: FileSystem; parts: openArray[string]): string {.base, gcsafe, raises: [IOError].} =
  abstractMethod("FileSystem.pathJoin")

method pathDir*(self: FileSystem; path: string): string {.base, gcsafe, raises: [IOError].} =
  abstractMethod("FileSystem.pathDir")

method diskUsage*(self: FileSystem; path: string): DiskUsage {.base, gcsafe, raises: [IOError].} =
  abstractMethod("FileSystem.diskUsage")

method unwrap*(self: FileSystem): FileSystem {.base, gcsafe, raises: [IOError].} =
  abstractMethod("FileSystem.unwrap")

proc isValid*(region: MmapRegion): bool =
  region.base != nil and region.length > 0

proc setReleaser*(region: var MmapRegion; hook: ReleaseHook) =
  region.releaser = hook

proc unmap*(region: var MmapRegion) =
  if region.releaser != nil and region.base != nil and region.length > 0:
    region.releaser(region.base, region.length)
  region.base = nil
  region.length = 0
  region.releaser = nil
