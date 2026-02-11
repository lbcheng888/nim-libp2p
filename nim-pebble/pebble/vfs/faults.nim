# VFS 故障注入框架，用于在集成测试或压力测试时模拟磁盘错误。

import std/options

type
  FaultOp* = enum
    faultCreate,
    faultOpen,
    faultOpenReadWrite,
    faultOpenDir,
    faultLink,
    faultRemove,
    faultRemoveAll,
    faultRename,
    faultReuseForWrite,
    faultMkdirAll,
    faultLock,
    faultList,
    faultStat,
    faultDiskUsage,
    faultFileRead,
    faultFileReadAt,
    faultFileWrite,
    faultFileWriteAt,
    faultFilePreallocate,
    faultFileSync,
    faultFileSyncData,
    faultFileSyncTo,
    faultFilePrefetch,
    faultFileMap

  FaultHook* = proc (op: FaultOp; path: string): Option[string] {.gcsafe, raises: [].}

  FaultInjector* = ref object
    hooks: seq[FaultHook]

proc newFaultInjector*(): FaultInjector =
  FaultInjector(hooks: @[])

proc addHook*(injector: FaultInjector; hook: FaultHook) =
  if injector.isNil:
    raise newException(ValueError, "FaultInjector 不可为 nil")
  injector.hooks.add(hook)

proc clearHooks*(injector: FaultInjector) =
  if injector.isNil:
    return
  injector.hooks.setLen(0)

proc shouldFail*(injector: FaultInjector; op: FaultOp; path: string) {.raises: [IOError].} =
  if injector.isNil:
    return
  for hook in injector.hooks:
    let verdict = hook(op, path)
    if verdict.isSome:
      raise newException(IOError, verdict.get)

proc hasHooks*(injector: FaultInjector): bool =
  not injector.isNil and injector.hooks.len > 0
