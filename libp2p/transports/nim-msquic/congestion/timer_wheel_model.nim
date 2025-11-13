## 时间轮调度模型，对齐 `src/core/timer_wheel.c` 的核心逻辑。

import std/options
import std/tables
import std/algorithm
import std/sequtils

type
  TimerKey* = uint64

  TimerEntry* = object
    key*: TimerKey
    expirationUs*: uint64

  TimerWheel* = object
    slotCount*: uint32
    maxLoadFactor*: uint32
    slots*: seq[seq[TimerEntry]]
    indexMap*: Table[TimerKey, uint32]
    connectionCount*: uint32
    nextExpirationUs*: uint64
    nextKey*: Option[TimerKey]

const
  DefaultSlotCount = 32'u32
  DefaultLoadFactor = 32'u32

proc initTimerWheel*(
    slotCount: uint32 = DefaultSlotCount,
    maxLoadFactor: uint32 = DefaultLoadFactor
  ): TimerWheel =
  assert slotCount > 0
  let emptySlot: seq[TimerEntry] = @[]
  TimerWheel(
    slotCount: slotCount,
    maxLoadFactor: maxLoadFactor,
    slots: newSeqWith(slotCount.int, emptySlot),
    indexMap: initTable[TimerKey, uint32](),
    connectionCount: 0,
    nextExpirationUs: high(uint64),
    nextKey: none(TimerKey))

proc slotIndex(wheel: TimerWheel, expirationUs: uint64): uint32 =
  if wheel.slotCount == 0:
    return 0
  let bucket = (expirationUs div 1000'u64) mod uint64(wheel.slotCount)
  uint32(bucket)

proc recalcNext(wheel: var TimerWheel) =
  wheel.nextExpirationUs = high(uint64)
  wheel.nextKey = none(TimerKey)
  for slot in wheel.slots:
    if slot.len == 0:
      continue
    let entry = slot[0]
    if wheel.nextKey.isNone or entry.expirationUs < wheel.nextExpirationUs:
      wheel.nextExpirationUs = entry.expirationUs
      wheel.nextKey = some(entry.key)

proc insertEntry(wheel: var TimerWheel, entry: TimerEntry) =
  let idx = wheel.slotIndex(entry.expirationUs)
  var slotSeq = wheel.slots[idx.int]
  var pos = slotSeq.len
  while pos > 0 and slotSeq[pos - 1].expirationUs > entry.expirationUs:
    dec pos
  slotSeq.insert(entry, Natural(pos))
  wheel.slots[idx.int] = slotSeq
  wheel.indexMap[entry.key] = idx
  if wheel.nextKey.isNone or entry.expirationUs < wheel.nextExpirationUs:
    wheel.nextExpirationUs = entry.expirationUs
    wheel.nextKey = some(entry.key)

proc resize(wheel: var TimerWheel) =
  let newSlotCount = wheel.slotCount shl 1
  if newSlotCount <= wheel.slotCount:
    return
  var allEntries: seq[TimerEntry] = @[]
  for slot in wheel.slots:
    for entry in slot:
      allEntries.add(entry)
  wheel.slotCount = newSlotCount
  let emptySlot: seq[TimerEntry] = @[]
  wheel.slots = newSeqWith(newSlotCount.int, emptySlot)
  wheel.indexMap = initTable[TimerKey, uint32]()
  wheel.connectionCount = 0
  wheel.nextExpirationUs = high(uint64)
  wheel.nextKey = none(TimerKey)
  for entry in allEntries:
    wheel.insertEntry(entry)
    inc wheel.connectionCount

proc removeConnection*(wheel: var TimerWheel, key: TimerKey) =
  if key notin wheel.indexMap:
    return
  let slotIdx = wheel.indexMap[key]
  wheel.indexMap.del(key)
  var slotSeq = wheel.slots[slotIdx.int]
  var removed = false
  for i in 0 ..< slotSeq.len:
    if slotSeq[i].key == key:
      slotSeq.delete(i)
      removed = true
      break
  if removed:
    if wheel.connectionCount > 0:
      dec wheel.connectionCount
    wheel.slots[slotIdx.int] = slotSeq
    if wheel.nextKey.isSome and wheel.nextKey.get == key:
      wheel.recalcNext()

proc updateConnection*(
    wheel: var TimerWheel,
    key: TimerKey,
    expirationUs: uint64
  ) =
  ## 插入或更新连接的最早定时器。
  if expirationUs == 0:
    wheel.removeConnection(key)
    return
  if key in wheel.indexMap:
    wheel.removeConnection(key)
  wheel.insertEntry(TimerEntry(key: key, expirationUs: expirationUs))
  inc wheel.connectionCount
  if wheel.connectionCount >
      wheel.slotCount * wheel.maxLoadFactor:
    wheel.resize()

proc popExpired*(wheel: var TimerWheel, nowUs: uint64): seq[TimerEntry] =
  ## 返回并移除所有过期的条目。
  result = @[]
  if wheel.connectionCount == 0:
    return
  for idx in 0 ..< wheel.slotCount.int:
    var slotSeq = wheel.slots[idx]
    var removeCount = 0
    for entry in slotSeq:
      if entry.expirationUs <= nowUs:
        result.add(entry)
        wheel.indexMap.del(entry.key)
        inc removeCount
      else:
        break
    if removeCount > 0:
      let lastIdx = removeCount - 1
      if lastIdx >= 0:
        slotSeq.delete(0 .. lastIdx)
      wheel.slots[idx] = slotSeq
      if wheel.connectionCount >= uint32(removeCount):
        wheel.connectionCount -= uint32(removeCount)
      else:
        wheel.connectionCount = 0
  if result.len > 0:
    result.sort(proc (a, b: TimerEntry): int =
      cmp(a.expirationUs, b.expirationUs))
    wheel.recalcNext()

proc peekNext*(wheel: TimerWheel): Option[TimerEntry] =
  ## 返回最早的定时器信息（不移除）。
  if wheel.nextKey.isNone:
    return none(TimerEntry)
  some(TimerEntry(
    key: wheel.nextKey.get,
    expirationUs: wheel.nextExpirationUs))
