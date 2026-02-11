# VersionSet 管理当前生效的 Version，并负责与 manifest 文件交互。

import std/[locks, options, sequtils]

import pebble/manifest/types
import pebble/manifest/version
import pebble/manifest/version_edit
import pebble/manifest/store
import pebble/runtime/executor

type
  VersionSet* = ref object
    manifestPath*: string
    store*: ManifestStore
    current*: Version
    executor*: Executor
    ownsExecutor: bool
    handles: seq[TaskHandle]
    lock: Lock
    closed: bool
    versionCounter: uint64
    lastErrorMsg: Option[string]

proc newVersionSet*(manifestPath: string;
                    baseVersion: Version = nil;
                    executor: Executor = nil;
                    createIfMissing = true;
                    formatVersion: ManifestFormatVersion = manifestFormatV1): VersionSet =
  var currentVersion: Version = nil
  var counter: uint64 = 0
  var store = initManifestStore(manifestPath, formatVersion)
  let existingRecords = readAllRecords(store)
  if existingRecords.len > 0:
    for record in existingRecords:
      case record.kind
      of recordSnapshot:
        currentVersion = record.version.clone()
        if currentVersion.isNil():
          raise newException(ManifestError, "manifest snapshot 为空")
        currentVersion.id = toVersionId(1)
        counter = currentVersion.id.toUint64()
      of recordEdit:
        if currentVersion.isNil():
          raise newException(ManifestError, "缺少 manifest snapshot")
        currentVersion = currentVersion.applyEdit(record.edit)
        counter += 1
        currentVersion.id = toVersionId(counter)
  else:
    if not createIfMissing and not store.manifestExists():
      raise newException(ManifestError, "manifest 文件不存在: " & manifestPath)
    if baseVersion.isNil():
      currentVersion = initVersion()
    else:
      currentVersion = baseVersion.clone()
    counter = 1
    currentVersion.id = toVersionId(counter)
  if existingRecords.len == 0:
    store.writeSnapshot(currentVersion)
  var exec = executor
  var ownsExec = false
  if exec.isNil():
    exec = initExecutor(mode = execThreadPool, maxWorkers = 1)
    ownsExec = true
  let vs = VersionSet(
    manifestPath: manifestPath,
    store: store,
    current: currentVersion,
    executor: exec,
    ownsExecutor: ownsExec,
    handles: @[],
    closed: false,
    versionCounter: counter,
    lastErrorMsg: none(string)
  )
  initLock(vs.lock)
  vs

proc raiseIfFault(vs: VersionSet) =
  acquire(vs.lock)
  try:
    if vs.closed:
      raise newException(ManifestError, "VersionSet 已关闭")
    if vs.lastErrorMsg.isSome():
      raise newException(ManifestError, vs.lastErrorMsg.get())
  finally:
    release(vs.lock)

proc pruneHandles(vs: VersionSet) =
  vs.handles = vs.handles.filterIt(it.isPending())

proc applyEditLocked(vs: VersionSet; edit: VersionEdit) =
  if edit.isEmpty():
    return
  let nextVersion = vs.current.applyEdit(edit)
  vs.store.appendEdit(edit)
  nextVersion.id = toVersionId(vs.versionCounter + 1)
  vs.versionCounter = nextVersion.id.toUint64()
  vs.current = nextVersion

proc applyEditSync(vs: VersionSet; edit: VersionEdit) =
  acquire(vs.lock)
  try:
    if vs.closed:
      raise newException(ManifestError, "VersionSet 已关闭")
    if vs.lastErrorMsg.isSome():
      raise newException(ManifestError, vs.lastErrorMsg.get())
    vs.applyEditLocked(edit)
  finally:
    release(vs.lock)

proc recordFailure(vs: VersionSet; err: ref Exception) =
  acquire(vs.lock)
  try:
    if vs.lastErrorMsg.isNone():
      vs.lastErrorMsg = some(err.msg)
  finally:
    release(vs.lock)

proc submitEdit*(vs: VersionSet; edit: VersionEdit): TaskHandle =
  vs.raiseIfFault()
  let captured = edit
  let handle = vs.executor.submit(proc () {.gcsafe.} =
    try:
      vs.applyEditSync(captured)
    except CatchableError as err:
      vs.recordFailure(err)
  )
  acquire(vs.lock)
  try:
    vs.pruneHandles()
    if handle.isPending():
      vs.handles.add(handle)
  finally:
    release(vs.lock)
  handle

proc applyEdit*(vs: VersionSet; edit: VersionEdit) =
  vs.applyEditSync(edit)

proc currentVersion*(vs: VersionSet): Version =
  acquire(vs.lock)
  try:
    vs.current.clone()
  finally:
    release(vs.lock)

proc snapshot*(vs: VersionSet): Version =
  vs.currentVersion()

proc sync*(vs: VersionSet) =
  acquire(vs.lock)
  try:
    vs.store.sync()
  finally:
    release(vs.lock)

proc waitPending*(vs: VersionSet) =
  var handles: seq[TaskHandle] = @[]
  acquire(vs.lock)
  try:
    handles = vs.handles
    vs.handles = @[]
  finally:
    release(vs.lock)
  for handle in handles:
    handle.wait()

proc close*(vs: VersionSet) =
  acquire(vs.lock)
  let alreadyClosed = vs.closed
  vs.closed = true
  release(vs.lock)
  if alreadyClosed:
    return
  vs.waitPending()
  if vs.ownsExecutor and vs.executor != nil:
    var empty: seq[TaskHandle] = @[]
    vs.executor.shutdown(empty)
  vs.store.sync()
  vs.store.close()

proc errorMessage*(vs: VersionSet): Option[string] =
  acquire(vs.lock)
  try:
    vs.lastErrorMsg
  finally:
    release(vs.lock)
