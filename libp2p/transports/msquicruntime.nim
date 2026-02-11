## MsQuic 运行时管理：集中加载 / 释放 RuntimeBridge，供传输层复用。

import std/[locks, options, os, strformat, strutils, atomics]

import "nim-msquic/api/runtime_bridge" as msruntime
import "nim-msquic/api/ffi_loader" as msffi

export msruntime.RuntimeBridge
export msffi.MsQuicLoadOptions

type
  MsQuicBridgeResult* = object
    success*: bool
    bridge*: RuntimeBridge
    error*: string

var
  gBridgeLock: Lock
  gBridgeLockState: Atomic[int32] ## 0 = uninitialised, 1 = initialising, 2 = ready
  gBridge: RuntimeBridge
  gBridgeRefCount = 0
  gBridgeOptions: MsQuicLoadOptions

proc ensureLock() =
  if gBridgeLockState.load() == 2'i32:
    return
  var expected = 0'i32
  if gBridgeLockState.compareExchange(expected, 1'i32):
    initLock(gBridgeLock)
    gBridgeLockState.store(2'i32)
  else:
    while gBridgeLockState.load() != 2'i32:
      os.sleep(1)

proc mergeOptions(options: MsQuicLoadOptions): MsQuicLoadOptions =
  result = options
  if result.requestedVersion == 0'u32:
    result.requestedVersion = msffi.DefaultMsQuicVersion
  if result.explicitPath.len == 0:
    let envPath = getEnv(msffi.DefaultMsQuicEnvVar)
    if envPath.len > 0:
      result.explicitPath = envPath

proc acquireMsQuicBridge*(options: MsQuicLoadOptions = MsQuicLoadOptions()):
    MsQuicBridgeResult {.raises: [].} =
  ## 获取（并按需加载）全局 MsQuic RuntimeBridge。
  ensureLock()
  var bridgeToReturn: RuntimeBridge = nil
  var errMsg = ""
  var ok = false
  let merged = mergeOptions(options)
  withLock gBridgeLock:
    if not gBridge.isNil:
      inc gBridgeRefCount
      bridgeToReturn = gBridge
      ok = true
    else:
      try:
        let bridge = msruntime.loadRuntimeBridge(merged)
        gBridge = bridge
        gBridgeRefCount = 1
        gBridgeOptions = merged
        bridgeToReturn = bridge
        ok = true
      except Exception as exc:
        errMsg = fmt"loadRuntimeBridge failed: {exc.msg}"
        ok = false
  if ok:
    MsQuicBridgeResult(success: true, bridge: bridgeToReturn)
  else:
    if errMsg.len == 0:
      errMsg = "failed to load MsQuic runtime bridge"
    MsQuicBridgeResult(success: false, bridge: nil, error: errMsg)

proc releaseMsQuicBridge*(bridge: RuntimeBridge) {.raises: [].} =
  ## 释放 RuntimeBridge 引用。引用计数归零时卸载 MsQuic 库。
  if bridge.isNil:
    return
  ensureLock()
  var bridgeToClose: RuntimeBridge = nil
  withLock gBridgeLock:
    if gBridge.isNil or gBridge != bridge:
      return
    dec gBridgeRefCount
    if gBridgeRefCount <= 0:
      bridgeToClose = gBridge
      gBridge = nil
      gBridgeRefCount = 0
  if not bridgeToClose.isNil:
    try:
      var tmp = bridgeToClose
      tmp.close()
    except Exception:
      discard

proc currentMsQuicBridge*(): RuntimeBridge {.raises: [].} =
  ensureLock()
  withLock gBridgeLock:
    result = gBridge

proc msQuicBridgeLoaded*(): bool {.raises: [].} =
  not currentMsQuicBridge().isNil

proc lastMsQuicOptions*(): MsQuicLoadOptions {.raises: [].} =
  ensureLock()
  withLock gBridgeLock:
    result = gBridgeOptions
