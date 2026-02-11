import std/[algorithm, json, locks, options, os, random, sequtils, strformat, strutils, tables, times]

import bridge/model
import pebble/batch/types as pebble_batch_types
import pebble/core/types as pebble_core
import pebble/kv

template requirePebbleSupport(body: untyped) =
  when defined(bridge_disable_pebble):
    raise newException(IOError, "Pebble backend disabled in async bridge mode")
  else:
    body

const
  EventsDir = "events"
  EventsIndexFile = "index.json"
  EventsIndexKey = "events"
  PendingIndexPrefix = "pending:"
  TasksDir = "tasks"
  TasksIndexFile = "tasks_index.json"
  TasksIndexKey = "tasks"
  TaskReadyIndexPrefix = "task:ready:"
  TaskRunningIndexPrefix = "task:running:"
  TaskDoneIndexPrefix = "task:done:"
  TaskFailedIndexPrefix = "task:failed:"
  TaskCursorSeparator = ":"
  TimestampKeyWidth = 20
  DefaultVisibilityMs* = 30_000
  DefaultMaxAttempts* = 5

type
  StorageBackend* = enum
    sbInMemory, sbFile, sbPebble

  TaskStatus* = enum
    tsReady, tsRunning, tsDone, tsFailed

  TaskRecord* = object
    taskId*: string
    payload*: JsonNode
    status*: TaskStatus
    attempts*: int
    maxAttempts*: int
    visibleAtMs*: int64
    receipt*: string
    createdAtMs*: int64
    updatedAtMs*: int64
    lastError*: string

  TaskLease* = object
    taskId*: string
    payload*: JsonNode
    attempts*: int
    maxAttempts*: int
    receipt*: string
    visibleUntilMs*: int64
    createdAtMs*: int64
    updatedAtMs*: int64

  MemEventStore* = object
    events: OrderedTable[string, EventRecord]

  FileEventStore* = object
    basePath: string

  PebbleEventStore* = object
    kv: PebbleKV

  PendingScanPage* = object
    events*: seq[EventRecord]
    hasMore*: bool
    nextCursor*: Option[string]
    snapshot*: Option[Snapshot]

  TaskScanPage* = object
    status*: TaskStatus
    tasks*: seq[TaskRecord]
    hasMore*: bool
    nextCursor*: Option[string]
    snapshot*: Option[Snapshot]

  EventStore* = ref object
    eventLock: Lock
    taskLock: Lock
    tasks: OrderedTable[string, TaskRecord]
    taskIds: seq[string]
    taskSeq: int64
    taskIndexDirty: bool
    case backend*: StorageBackend
    of sbInMemory:
      mem*: MemEventStore
    of sbFile:
      file*: FileEventStore
    of sbPebble:
      pebble*: PebbleEventStore

proc nowMs(): int64 =
  (epochTime() * 1000.0).int64

proc sanitizeId(id: string): string =
  if id.len == 0:
    return "object"
  var buf = newSeq[char]()
  for ch in id:
    if ch.isAlphaNumeric() or ch in {'-', '_', '.'}:
      buf.add(ch)
    else:
      buf.add('_')
  buf.join("")

proc initMemStore(): MemEventStore =
  MemEventStore(events: initOrderedTable[string, EventRecord]())

proc eventsPath(store: FileEventStore): string =
  store.basePath / EventsDir

proc indexPath(store: FileEventStore): string =
  store.basePath / EventsIndexFile

proc tasksPath(store: FileEventStore): string =
  store.basePath / TasksDir

proc tasksIndexPath(store: FileEventStore): string =
  store.basePath / TasksIndexFile

proc ensureFileStore(basePath: string) =
  createDir(basePath)
  createDir(basePath / EventsDir)
  createDir(basePath / TasksDir)

proc eventToFileName(eventId: string): string =
  sanitizeId(eventId) & ".json"

proc taskToFileName(taskId: string): string =
  sanitizeId(taskId) & ".json"

proc zeroPadTimestamp(ts: int64): string =
  ## 生成固定宽度的时间戳字符串，保证字典序与时间排序一致。
  let normalized = if ts < 0: 0 else: ts
  result = $normalized
  if result.len < TimestampKeyWidth:
    result = repeat('0', TimestampKeyWidth - result.len) & result

proc taskStatusPrefix(status: TaskStatus): string =
  case status
  of tsReady:
    TaskReadyIndexPrefix
  of tsRunning:
    TaskRunningIndexPrefix
  of tsDone:
    TaskDoneIndexPrefix
  of tsFailed:
    TaskFailedIndexPrefix

proc statusTimestamp(rec: TaskRecord): int64 =
  case rec.status
  of tsReady, tsRunning:
    rec.visibleAtMs
  of tsDone, tsFailed:
    rec.updatedAtMs

proc taskIndexKey(status: TaskStatus; timestamp: int64; taskId: string): string =
  let prefix = taskStatusPrefix(status)
  if prefix.len == 0:
    return ""
  prefix & zeroPadTimestamp(timestamp) & TaskCursorSeparator & sanitizeId(taskId)

proc taskIndexKey(rec: TaskRecord): string =
  taskIndexKey(rec.status, statusTimestamp(rec), rec.taskId)

proc stripTaskPrefix(rawKey: string; prefix: string): string =
  if rawKey.len >= prefix.len and rawKey.startsWith(prefix):
    rawKey[prefix.len ..< rawKey.len]
  else:
    rawKey

proc taskCursorSuffix(status: TaskStatus; rawKey: string): string =
  let prefix = taskStatusPrefix(status)
  stripTaskPrefix(rawKey, prefix)

proc taskIdFromSuffix(suffix: string): string =
  let sepIdx = suffix.rfind(TaskCursorSeparator)
  if sepIdx == -1 or sepIdx + 1 >= suffix.len:
    suffix
  else:
    suffix[sepIdx + 1 ..< suffix.len]

proc taskCursor(rec: TaskRecord): string =
  let prefix = taskStatusPrefix(rec.status)
  if prefix.len == 0:
    return ""
  stripTaskPrefix(taskIndexKey(rec), prefix)

proc taskIdFromEntry(entry: KVPair; status: TaskStatus): string =
  if entry.value.len > 0:
    entry.value
  else:
    let suffix = taskCursorSuffix(status, entry.id)
    taskIdFromSuffix(suffix)

proc ensurePebbleTaskStatusIndex(store: EventStore; rec: TaskRecord) =
  if store.backend != sbPebble or store.pebble.kv.isNil():
    return
  let key = taskIndexKey(rec)
  if key.len == 0:
    return
  if store.pebble.kv.get(ksIndex, key).isSome():
    return
  store.pebble.kv.applyBatch(@[withPut(ksIndex, key, rec.taskId)])

proc encodeRecord(rec: EventRecord): string =
  $recordToJson(rec)

proc decodeRecord(data: string): Option[EventRecord] =
  try:
    let node = parseJson(data)
    recordFromJson(node)
  except CatchableError:
    none(EventRecord)

proc readFileRecord(store: FileEventStore; eventId: string): Option[EventRecord] =
  let path = store.eventsPath() / eventToFileName(eventId)
  if not fileExists(path):
    return none(EventRecord)
  try:
    decodeRecord(readFile(path))
  except CatchableError:
    none(EventRecord)

proc writeFileRecord(store: FileEventStore; rec: EventRecord) =
  let path = store.eventsPath() / eventToFileName(rec.event.eventId)
  writeFile(path, encodeRecord(rec))

proc loadIndex(store: FileEventStore): seq[string] =
  let path = store.indexPath()
  if not fileExists(path):
    return @[]
  try:
    let node = parseJson(readFile(path))
    if node.kind == JArray:
      return node.elems.mapIt(it.getStr())
    @[]
  except CatchableError:
    @[]

proc storeIndex(store: FileEventStore; events: seq[string]) =
  let path = store.indexPath()
  writeFile(path, $(%events))

proc taskToJson(rec: TaskRecord): JsonNode =
  %*{
    "taskId": rec.taskId,
    "payload": rec.payload,
    "status": ord(rec.status),
    "attempts": rec.attempts,
    "maxAttempts": rec.maxAttempts,
    "visibleAtMs": rec.visibleAtMs,
    "receipt": rec.receipt,
    "createdAtMs": rec.createdAtMs,
    "updatedAtMs": rec.updatedAtMs,
    "lastError": rec.lastError,
  }

proc encodeTask(rec: TaskRecord): string =
  $taskToJson(rec)

proc intField(node: JsonNode; key: string; defaultVal: int): int =
  if node.hasKey(key):
    try:
      node[key].getInt()
    except CatchableError:
      defaultVal
  else:
    defaultVal

proc int64Field(node: JsonNode; key: string; defaultVal: int64): int64 =
  if node.hasKey(key):
    try:
      node[key].getInt().int64
    except CatchableError:
      defaultVal
  else:
    defaultVal

proc stringField(node: JsonNode; key: string; defaultVal: string): string =
  if node.hasKey(key):
    try:
      node[key].getStr()
    except CatchableError:
      defaultVal
  else:
    defaultVal

proc taskFromJson(node: JsonNode): Option[TaskRecord] =
  if node.kind != JObject or not node.hasKey("taskId"):
    return none(TaskRecord)
  var payload = newJNull()
  if node.hasKey("payload"):
    payload = node["payload"]
  let statusVal = intField(node, "status", ord(tsReady))
  if statusVal < ord(low(TaskStatus)) or statusVal > ord(high(TaskStatus)):
    return none(TaskRecord)
  some(
    TaskRecord(
      taskId: node["taskId"].getStr(),
      payload: payload,
      status: TaskStatus(statusVal),
      attempts: intField(node, "attempts", 0),
      maxAttempts: max(1, intField(node, "maxAttempts", DefaultMaxAttempts)),
      visibleAtMs: int64Field(node, "visibleAtMs", 0),
      receipt: stringField(node, "receipt", ""),
      createdAtMs: int64Field(node, "createdAtMs", nowMs()),
      updatedAtMs: int64Field(node, "updatedAtMs", nowMs()),
      lastError: stringField(node, "lastError", "")
    )
  )

proc decodeTask(data: string): Option[TaskRecord] =
  try:
    let node = parseJson(data)
    taskFromJson(node)
  except CatchableError:
    none(TaskRecord)

proc readFileTask(store: FileEventStore; taskId: string): Option[TaskRecord] =
  let path = store.tasksPath() / taskToFileName(taskId)
  if not fileExists(path):
    return none(TaskRecord)
  try:
    decodeTask(readFile(path))
  except CatchableError:
    none(TaskRecord)

proc writeFileTask(store: FileEventStore; rec: TaskRecord) =
  let path = store.tasksPath() / taskToFileName(rec.taskId)
  writeFile(path, encodeTask(rec))

proc loadTaskIndex(store: FileEventStore): seq[string] =
  let path = store.tasksIndexPath()
  if not fileExists(path):
    return @[]
  try:
    let node = parseJson(readFile(path))
    if node.kind == JArray:
      return node.elems.mapIt(it.getStr())
    @[]
  except CatchableError:
    @[]

proc storeTaskIndex(store: FileEventStore; ids: seq[string]) =
  let path = store.tasksIndexPath()
  writeFile(path, $(%ids))

proc getPebbleRecord(store: PebbleEventStore; eventId: string): Option[EventRecord]
proc getPebbleTask(store: PebbleEventStore; taskId: string): Option[TaskRecord]

proc pendingIndexKey(eventId: string): string =
  PendingIndexPrefix & sanitizeId(eventId)

proc decodePendingKey(rawKey: string): string =
  if rawKey.len > PendingIndexPrefix.len:
    rawKey[PendingIndexPrefix.len ..< rawKey.len]
  else:
    rawKey

proc pendingEventId(entry: KVPair): string =
  if entry.value.len > 0:
    entry.value
  else:
    decodePendingKey(entry.id)

proc scanPebblePending(store: PebbleEventStore;
                       startAfter = "";
                       limit = 0;
                       snapshot: Option[Snapshot] = none(Snapshot)): KVScanPage =
  if snapshot.isSome():
    store.kv.scanPrefix(ksIndex, PendingIndexPrefix, startAfter, limit, snapshot.get())
  else:
    store.kv.scanPrefix(ksIndex, PendingIndexPrefix, startAfter, limit)

proc scanPebbleTasks(store: PebbleEventStore;
                     status: TaskStatus;
                     startAfter = "";
                     limit = 0;
                     snapshot: Option[Snapshot] = none(Snapshot)): KVScanPage =
  let prefix = taskStatusPrefix(status)
  if prefix.len == 0:
    return KVScanPage()
  let cursorKey =
    if startAfter.len > 0:
      prefix & startAfter
    else:
      ""
  if snapshot.isSome():
    store.kv.scanPrefix(ksIndex, prefix, cursorKey, limit, snapshot.get())
  else:
    store.kv.scanPrefix(ksIndex, prefix, cursorKey, limit)

proc loadPebblePendingIds(store: PebbleEventStore): seq[string] =
  let page = store.scanPebblePending()
  for entry in page.entries:
    result.add(pendingEventId(entry))

proc scanPending*(store: EventStore;
                  startAfter = "";
                  limit = 0;
                  snapshot: Option[Snapshot] = none(Snapshot)): PendingScanPage =
  let effectiveLimit = if limit < 0: 0 else: limit
  if store.isNil():
    return result
  if store.backend == sbPebble:
    let cursorKey =
      if startAfter.len > 0:
        pendingIndexKey(startAfter)
      else:
        ""
    let page =
      if snapshot.isSome():
        store.pebble.scanPebblePending(cursorKey, effectiveLimit, snapshot)
      else:
        store.pebble.scanPebblePending(cursorKey, effectiveLimit)
    var events: seq[EventRecord] = @[]
    var lastEventId = ""
    for entry in page.entries:
      let eventId = pendingEventId(entry)
      let rec = store.pebble.getPebbleRecord(eventId)
      if rec.isSome() and rec.get().status == esPending:
        let value = rec.get()
        events.add(value)
        lastEventId = value.event.eventId
    result.events = events
    result.hasMore = page.hasMore
    if page.hasMore:
      if lastEventId.len > 0:
        result.nextCursor = some(lastEventId)
      elif page.nextCursor.isSome():
        result.nextCursor = some(decodePendingKey(page.nextCursor.get()))
      else:
        result.nextCursor = none(string)
    else:
      result.nextCursor = none(string)
    result.snapshot = some(page.snapshot)
    return result

  var pending: seq[EventRecord] = @[]
  case store.backend
  of sbInMemory:
    for value in store.mem.events.values:
      if value.status == esPending:
        pending.add(value)
  of sbFile:
    let ids = store.file.loadIndex()
    for id in ids:
      let rec = store.file.readFileRecord(id)
      if rec.isSome() and rec.get().status == esPending:
        pending.add(rec.get())
  of sbPebble:
    discard
  pending.sort(proc (a, b: EventRecord): int =
    cmp(a.event.eventId, b.event.eventId))
  var startIndex = 0
  if startAfter.len > 0 and pending.len > 0:
    while startIndex < pending.len and pending[startIndex].event.eventId <= startAfter:
      inc startIndex
  let remaining = max(pending.len - startIndex, 0)
  let takeCount =
    if effectiveLimit == 0:
      remaining
    else:
      min(effectiveLimit, remaining)
  if takeCount > 0:
    result.events = pending[startIndex ..< startIndex + takeCount]
  else:
    result.events = @[]
  result.hasMore = (startIndex + takeCount) < pending.len
  if result.hasMore and result.events.len > 0:
    result.nextCursor = some(result.events[^1].event.eventId)
  else:
    result.nextCursor = none(string)
  result.snapshot = none(Snapshot)

proc scanTasks*(store: EventStore;
                status: TaskStatus;
                startAfter = "";
                limit = 0;
                snapshot: Option[Snapshot] = none(Snapshot)): TaskScanPage =
  result.status = status
  let effectiveLimit = if limit < 0: 0 else: limit
  if store.isNil():
    return result
  if store.backend != sbPebble:
    return result
  let page =
    if snapshot.isSome():
      store.pebble.scanPebbleTasks(status, startAfter, effectiveLimit, snapshot)
    else:
      store.pebble.scanPebbleTasks(status, startAfter, effectiveLimit)
  var collected: seq[TaskRecord] = @[]
  for entry in page.entries:
    let taskId = taskIdFromEntry(entry, status)
    let recOpt = store.pebble.getPebbleTask(taskId)
    if recOpt.isSome():
      collected.add(recOpt.get())
  result.tasks = collected
  result.hasMore = page.hasMore
  if page.hasMore:
    if page.nextCursor.isSome():
      result.nextCursor = some(taskCursorSuffix(status, page.nextCursor.get()))
    elif page.entries.len > 0:
      result.nextCursor = some(taskCursorSuffix(status, page.entries[^1].id))
    else:
      result.nextCursor = none(string)
  else:
    result.nextCursor = none(string)
  result.snapshot = some(page.snapshot)

proc getPebbleRecord(store: PebbleEventStore; eventId: string): Option[EventRecord] =
  let raw = store.kv.get(ksEvent, eventId)
  if raw.isNone():
    return none(EventRecord)
  decodeRecord(raw.get())

proc putPebbleRecord(store: PebbleEventStore; rec: EventRecord) =
  store.kv.applyBatch(@[withPut(ksEvent, rec.event.eventId, encodeRecord(rec))])

proc getPebbleTask(store: PebbleEventStore; taskId: string): Option[TaskRecord] =
  let raw = store.kv.get(ksTask, taskId)
  if raw.isNone():
    return none(TaskRecord)
  decodeTask(raw.get())

proc putPebbleTask(store: PebbleEventStore; rec: TaskRecord) =
  store.kv.applyBatch(@[withPut(ksTask, rec.taskId, encodeTask(rec))])

proc decodeStringArray(raw: string): seq[string] =
  try:
    let parsed = parseJson(raw)
    if parsed.kind != JArray:
      return @[]
    for item in parsed.elems:
      if item.kind == JString:
        result.add(item.getStr())
  except CatchableError:
    return @[]

proc loadPebbleIndex(store: PebbleEventStore): seq[string] =
  if store.kv.isNil():
    return @[]
  let raw = store.kv.get(ksIndex, EventsIndexKey)
  if raw.isNone():
    return @[]
  decodeStringArray(raw.get())

proc writePebbleIndex(store: PebbleEventStore; ids: seq[string]) =
  if store.kv.isNil():
    return
  store.kv.applyBatch(@[withPut(ksIndex, EventsIndexKey, $(%ids))])

proc loadPebbleTaskIndex(store: PebbleEventStore): seq[string] =
  if store.kv.isNil():
    return @[]
  let raw = store.kv.get(ksIndex, TasksIndexKey)
  if raw.isNone():
    return @[]
  decodeStringArray(raw.get())

proc newPebbleStore(path: string): PebbleEventStore =
  PebbleEventStore(kv: newPebbleKV(path))

proc bumpTaskSeqFromId(store: EventStore; taskId: string) =
  var digits = newSeq[char]()
  for ch in taskId:
    if ch.isDigit():
      digits.add(ch)
  if digits.len == 0:
    return
  try:
    let value = parseBiggestInt(digits.join("")).int64
    if value > store.taskSeq:
      store.taskSeq = value
  except ValueError:
    discard

proc loadTaskCache(store: EventStore) =
  store.tasks = initOrderedTable[string, TaskRecord]()
  store.taskIds = @[]
  store.taskSeq = 0
  store.taskIndexDirty = false
  case store.backend
  of sbInMemory:
    discard
  of sbFile:
    let ids = store.file.loadTaskIndex()
    for id in ids:
      let rec = store.file.readFileTask(id)
      if rec.isSome():
        let task = rec.get()
        store.tasks[task.taskId] = task
        store.taskIds.add(task.taskId)
        store.bumpTaskSeqFromId(task.taskId)
  of sbPebble:
    requirePebbleSupport:
      let ids = store.pebble.loadPebbleTaskIndex()
      for id in ids:
        let rec = store.pebble.getPebbleTask(id)
        if rec.isSome():
          let task = rec.get()
          store.tasks[task.taskId] = task
          store.taskIds.add(task.taskId)
          store.bumpTaskSeqFromId(task.taskId)
          store.ensurePebbleTaskStatusIndex(task)

proc persistTaskIndexUnlocked(store: EventStore) =
  case store.backend
  of sbInMemory:
    discard
  of sbFile:
    store.file.storeTaskIndex(store.taskIds)
  of sbPebble:
    requirePebbleSupport:
      store.taskIndexDirty = true

proc ensureTaskIndexUnlocked(store: EventStore; taskId: string) =
  if store.taskIds.find(taskId) == -1:
    store.taskIds.add(taskId)
    store.persistTaskIndexUnlocked()

proc nextTaskId(store: EventStore; explicit: string): string =
  if explicit.len > 0:
    store.bumpTaskSeqFromId(explicit)
    return explicit
  inc store.taskSeq
  fmt"task-{store.taskSeq:020d}"

proc generateReceipt(taskId: string; attempts: int; timestamp: int64): string =
  let nonce = rand(0xFFFFFFFF)
  fmt"{sanitizeId(taskId)}-{attempts:04d}-{timestamp:013d}-{nonce:08x}"

proc initStore*(backend: StorageBackend; path: string): EventStore =
  randomize()
  var store: EventStore
  case backend
  of sbInMemory:
    store = EventStore(
      backend: sbInMemory,
      mem: initMemStore()
    )
  of sbFile:
    let base = if path.len == 0: "bridge-data" else: path
    ensureFileStore(base)
    store = EventStore(
      backend: sbFile,
      file: FileEventStore(basePath: base)
    )
  of sbPebble:
    requirePebbleSupport:
      let base = if path.len == 0: "pebble-data" else: path
      createDir(base)
      store = EventStore(
        backend: sbPebble,
        pebble: newPebbleStore(base)
      )
  initLock(store.eventLock)
  initLock(store.taskLock)
  store.loadTaskCache()
  store

proc getRecord*(store: EventStore; eventId: string): Option[EventRecord] =
  case store.backend
  of sbInMemory:
    if store.mem.events.hasKey(eventId):
      some(store.mem.events[eventId])
    else:
      none(EventRecord)
  of sbFile:
    store.file.readFileRecord(eventId)
  of sbPebble:
    store.pebble.getPebbleRecord(eventId)

proc upsertIndex(store: EventStore; eventId: string) =
  case store.backend
  of sbInMemory:
    discard
  of sbFile:
    var ids = store.file.loadIndex()
    if ids.find(eventId) == -1:
      ids.add(eventId)
      store.file.storeIndex(ids)
  of sbPebble:
    var ids = store.pebble.loadPebbleIndex()
    if ids.find(eventId) == -1:
      ids.add(eventId)
      store.pebble.writePebbleIndex(ids)

proc removePendingIndex(store: EventStore; eventId: string) =
  case store.backend
  of sbInMemory:
    discard
  of sbFile:
    var ids = store.file.loadIndex()
    if ids.find(eventId) != -1:
      store.file.storeIndex(ids.filterIt(it != eventId))
  of sbPebble:
    store.pebble.kv.applyBatch(@[withDelete(ksIndex, pendingIndexKey(eventId))])
    var ids = store.pebble.loadPebbleIndex()
    let filtered = ids.filterIt(it != eventId)
    if filtered.len != ids.len:
      store.pebble.writePebbleIndex(filtered)

proc commitPebbleBatch(store: EventStore;
                       captureWrites: bool;
                       body: proc (wb: PebbleWriteBatch) {.closure.}): seq[KVWrite] =
  if store.backend != sbPebble or store.pebble.kv.isNil():
    raise newException(IOError, "Pebble backend required")
  var writes: seq[KVWrite] = @[]
  let seq = store.pebble.kv.withWriteBatch(proc (wb: PebbleWriteBatch) =
    body(wb)
    if captureWrites:
      writes = wb.writes()
  , WriteOptions(sync: true))
  if captureWrites: writes else: @[]

proc putTxn(store: EventStore; rec: EventRecord; captureWrites: bool): seq[KVWrite] =
  var copy = rec
  copy.updatedAt = nowEpoch()
  var needsIndexUpdate = true
  result = @[]
  case store.backend
  of sbInMemory:
    store.mem.events[rec.event.eventId] = copy
  of sbFile:
    store.file.writeFileRecord(copy)
  of sbPebble:
    let ids = store.pebble.loadPebbleIndex()
    let isNew = ids.find(rec.event.eventId) == -1
    proc applyBatch(wb: PebbleWriteBatch) =
      wb.put(ksEvent, rec.event.eventId, encodeRecord(copy))
      if copy.status == esPending:
        wb.put(ksIndex, pendingIndexKey(rec.event.eventId), rec.event.eventId)
      else:
        wb.delete(ksIndex, pendingIndexKey(rec.event.eventId))
      if isNew:
        var updated = ids
        updated.add(rec.event.eventId)
        wb.put(ksIndex, EventsIndexKey, $(%updated))
    if captureWrites:
      result = store.commitPebbleBatch(true, applyBatch)
    else:
      discard store.pebble.kv.withWriteBatch(applyBatch)
    needsIndexUpdate = false
  if needsIndexUpdate:
    store.upsertIndex(rec.event.eventId)

proc put*(store: EventStore; rec: EventRecord) =
  discard store.putTxn(rec, false)

proc putWithLog*(store: EventStore; rec: EventRecord): seq[KVWrite] =
  store.putTxn(rec, true)

proc appendSignatureTxn(store: EventStore;
                        eventId: string;
                        sig: SignatureMessage;
                        captureWrites: bool): seq[KVWrite] =
  let existing = store.getRecord(eventId)
  if existing.isNone():
    raise newException(IOError, fmt"event {eventId} not found")
  var rec = existing.get()
  rec.signatures[sig.signerPeer] = sig
  rec.updatedAt = nowEpoch()
  result = @[]
  case store.backend
  of sbInMemory:
    store.mem.events[eventId] = rec
  of sbFile:
    store.file.writeFileRecord(rec)
  of sbPebble:
    proc applyBatch(wb: PebbleWriteBatch) =
      wb.put(ksEvent, rec.event.eventId, encodeRecord(rec))
      wb.put(ksSignature, sig.signerPeer & ":" & eventId, encodeSignature(sig))
    if captureWrites:
      result = store.commitPebbleBatch(true, applyBatch)
    else:
      discard store.pebble.kv.withWriteBatch(applyBatch)

proc appendSignature*(store: EventStore; eventId: string; sig: SignatureMessage) =
  discard store.appendSignatureTxn(eventId, sig, false)

proc appendSignatureWithLog*(store: EventStore;
                             eventId: string;
                             sig: SignatureMessage): seq[KVWrite] =
  store.appendSignatureTxn(eventId, sig, true)

proc updateStatusTxn(store: EventStore;
                     eventId: string;
                     status: EventStatus;
                     captureWrites: bool): seq[KVWrite] =
  let existing = store.getRecord(eventId)
  if existing.isNone():
    raise newException(IOError, fmt"event {eventId} not found")
  var rec = existing.get()
  rec.status = status
  rec.updatedAt = nowEpoch()
  result = @[]
  case store.backend
  of sbInMemory:
    store.mem.events[eventId] = rec
  of sbFile:
    store.file.writeFileRecord(rec)
    if status != esPending:
      store.removePendingIndex(eventId)
  of sbPebble:
    let indexSnapshot =
      if status != esPending:
        store.pebble.loadPebbleIndex()
      else:
        newSeq[string]()
    proc applyBatch(wb: PebbleWriteBatch) =
      wb.put(ksEvent, rec.event.eventId, encodeRecord(rec))
      if status == esPending:
        wb.put(ksIndex, pendingIndexKey(rec.event.eventId), rec.event.eventId)
      else:
        wb.delete(ksIndex, pendingIndexKey(rec.event.eventId))
      if status != esPending:
        let filtered = indexSnapshot.filterIt(it != eventId)
        if filtered.len != indexSnapshot.len:
          wb.put(ksIndex, EventsIndexKey, $(%filtered))
    if captureWrites:
      result = store.commitPebbleBatch(true, applyBatch)
    else:
      discard store.pebble.kv.withWriteBatch(applyBatch)

proc updateStatus*(store: EventStore; eventId: string; status: EventStatus) =
  discard store.updateStatusTxn(eventId, status, false)

proc updateStatusWithLog*(store: EventStore;
                          eventId: string;
                          status: EventStatus): seq[KVWrite] =
  store.updateStatusTxn(eventId, status, true)

proc listPending*(store: EventStore): seq[EventRecord] =
  case store.backend
  of sbInMemory:
    for key, value in store.mem.events:
      if value.status == esPending:
        result.add(value)
  of sbFile:
    let ids = store.file.loadIndex()
    for id in ids:
      let rec = store.file.readFileRecord(id)
      if rec.isSome() and rec.get().status == esPending:
        result.add(rec.get())
  of sbPebble:
    result = store.scanPending(limit = 0).events

proc visibleNow(rec: TaskRecord; nowVal: int64): bool =
  rec.visibleAtMs <= nowVal

proc recordToLease(rec: TaskRecord): TaskLease =
  TaskLease(
    taskId: rec.taskId,
    payload: rec.payload,
    attempts: rec.attempts,
    maxAttempts: rec.maxAttempts,
    receipt: rec.receipt,
    visibleUntilMs: rec.visibleAtMs,
    createdAtMs: rec.createdAtMs,
    updatedAtMs: rec.updatedAtMs
  )

proc getTask*(store: EventStore; taskId: string): Option[TaskRecord] =
  acquire(store.taskLock)
  defer: release(store.taskLock)
  if store.tasks.hasKey(taskId):
    some(store.tasks[taskId])
  else:
    none(TaskRecord)

proc saveTaskTxn(store: EventStore;
                 rec: TaskRecord;
                 captureWrites: bool;
                 previous: Option[TaskRecord] = none(TaskRecord)): seq[KVWrite] =
  result = @[]
  var prev = previous
  if prev.isNone() and store.tasks.hasKey(rec.taskId):
    prev = some(store.tasks[rec.taskId])
  let oldKey =
    if prev.isSome():
      let older = prev.get()
      if older.taskId == rec.taskId:
        taskIndexKey(older)
      else:
        ""
    else:
      ""
  store.tasks[rec.taskId] = rec
  let newKey = taskIndexKey(rec)
  case store.backend
  of sbInMemory:
    discard
  of sbFile:
    store.file.writeFileTask(rec)
  of sbPebble:
    let flushIndex = store.taskIndexDirty
    proc applyBatch(wb: PebbleWriteBatch) =
      wb.put(ksTask, rec.taskId, encodeTask(rec))
      if oldKey.len > 0 and (newKey.len == 0 or newKey != oldKey):
        wb.delete(ksIndex, oldKey)
      if newKey.len > 0:
        wb.put(ksIndex, newKey, rec.taskId)
      if flushIndex:
        wb.put(ksIndex, TasksIndexKey, $(%store.taskIds))
    if flushIndex:
      store.taskIndexDirty = false
    if captureWrites:
      result = store.commitPebbleBatch(true, applyBatch)
    else:
      discard store.pebble.kv.withWriteBatch(applyBatch)

proc saveTaskUnlocked(store: EventStore; rec: TaskRecord) =
  discard store.saveTaskTxn(rec, false)

proc enqueueTaskTxn(store: EventStore;
                    payload: JsonNode;
                    delayMs: int;
                    visibilityTimeoutMs: int;
                    maxAttempts: int;
                    taskId: string;
                    captureWrites: bool): (TaskRecord, seq[KVWrite]) =
  if store.isNil():
    raise newException(IOError, "event store is not initialised")
  let finalPayload =
    if payload.isNil(): newJNull()
    else: payload
  acquire(store.taskLock)
  defer: release(store.taskLock)
  let effectiveDelay = if delayMs < 0: 0 else: delayMs
  let effectiveMax = max(1, maxAttempts)
  let nowVal = nowMs()
  let newId = store.nextTaskId(taskId)
  var rec = TaskRecord(
    taskId: newId,
    payload: finalPayload,
    status: tsReady,
    attempts: 0,
    maxAttempts: effectiveMax,
    visibleAtMs: nowVal + effectiveDelay.int64,
    receipt: "",
    createdAtMs: nowVal,
    updatedAtMs: nowVal,
    lastError: ""
  )
  store.ensureTaskIndexUnlocked(newId)
  let prev =
    if store.tasks.hasKey(newId):
      some(store.tasks[newId])
    else:
      none(TaskRecord)
  if effectiveDelay == 0 and rec.visibleAtMs <= nowVal:
    rec.visibleAtMs = nowVal
  var log = store.saveTaskTxn(rec, captureWrites, prev)
  if not captureWrites:
    log = @[]
  (rec, log)

proc enqueueTask*(store: EventStore;
                  payload: JsonNode;
                  delayMs = 0;
                  visibilityTimeoutMs = DefaultVisibilityMs;
                  maxAttempts = DefaultMaxAttempts;
                  taskId = ""): TaskRecord =
  let (task, _) = store.enqueueTaskTxn(payload, delayMs, visibilityTimeoutMs, maxAttempts, taskId, false)
  task

proc enqueueTaskWithLog*(store: EventStore;
                         payload: JsonNode;
                         delayMs = 0;
                         visibilityTimeoutMs = DefaultVisibilityMs;
                         maxAttempts = DefaultMaxAttempts;
                         taskId = ""): (TaskRecord, seq[KVWrite]) =
  store.enqueueTaskTxn(payload, delayMs, visibilityTimeoutMs, maxAttempts, taskId, true)

proc dequeueTasksTxn(store: EventStore;
                     maxCount: int;
                     visibilityTimeoutMs: int;
                     captureWrites: bool): (seq[TaskLease], seq[KVWrite]) =
  if store.isNil():
    raise newException(IOError, "event store is not initialised")
  if maxCount <= 0:
    return (@[], @[])
  let effectiveVisibility =
    if visibilityTimeoutMs <= 0: DefaultVisibilityMs else: visibilityTimeoutMs
  acquire(store.taskLock)
  defer: release(store.taskLock)
  let nowVal = nowMs()
  var leases: seq[TaskLease] = @[]
  var logs: seq[KVWrite] = @[]
  for id in store.taskIds:
    if not store.tasks.hasKey(id):
      continue
    var rec = store.tasks[id]
    case rec.status
    of tsDone, tsFailed:
      continue
    of tsRunning:
      if rec.visibleAtMs > nowVal:
        continue
      let runningPrev = rec
      if rec.attempts >= rec.maxAttempts:
        rec.status = tsFailed
        rec.updatedAtMs = nowVal
        rec.lastError = "max attempts exceeded"
        rec.receipt = ""
        rec.visibleAtMs = nowVal
        let log = store.saveTaskTxn(rec, captureWrites, some(runningPrev))
        if captureWrites and log.len > 0:
          logs.add(log)
        continue
      rec.status = tsReady
      rec.receipt = ""
      rec.visibleAtMs = nowVal
      rec.updatedAtMs = nowVal
      let log = store.saveTaskTxn(rec, captureWrites, some(runningPrev))
      if captureWrites and log.len > 0:
        logs.add(log)
    of tsReady:
      discard
    if not rec.visibleNow(nowVal):
      continue
    if rec.attempts >= rec.maxAttempts:
      let failPrev = rec
      rec.status = tsFailed
      rec.updatedAtMs = nowVal
      rec.lastError = "max attempts exceeded"
      rec.receipt = ""
      rec.visibleAtMs = nowVal
      let log = store.saveTaskTxn(rec, captureWrites, some(failPrev))
      if captureWrites and log.len > 0:
        logs.add(log)
      continue
    let leasingPrev = rec
    inc rec.attempts
    rec.status = tsRunning
    rec.visibleAtMs = nowVal + effectiveVisibility.int64
    rec.updatedAtMs = nowVal
    rec.receipt = generateReceipt(rec.taskId, rec.attempts, nowVal)
    let log = store.saveTaskTxn(rec, captureWrites, some(leasingPrev))
    if captureWrites and log.len > 0:
      logs.add(log)
    leases.add(rec.recordToLease())
    if leases.len >= maxCount:
      break
  (leases, logs)

proc dequeueTasks*(store: EventStore;
                   maxCount = 1;
                   visibilityTimeoutMs = DefaultVisibilityMs): seq[TaskLease] =
  let (leases, _) = store.dequeueTasksTxn(maxCount, visibilityTimeoutMs, false)
  leases

proc dequeueTasksWithLog*(store: EventStore;
                          maxCount = 1;
                          visibilityTimeoutMs = DefaultVisibilityMs): (seq[TaskLease], seq[KVWrite]) =
  store.dequeueTasksTxn(maxCount, visibilityTimeoutMs, true)

proc ackTaskTxn(store: EventStore;
                taskId: string;
                receipt: string;
                captureWrites: bool): (bool, seq[KVWrite]) =
  if store.isNil():
    return (false, @[])
  acquire(store.taskLock)
  defer: release(store.taskLock)
  if not store.tasks.hasKey(taskId):
    return (false, @[])
  var rec = store.tasks[taskId]
  if rec.status != tsRunning or rec.receipt != receipt:
    return (false, @[])
  let nowVal = nowMs()
  let prev = rec
  rec.status = tsDone
  rec.receipt = ""
  rec.visibleAtMs = nowVal
  rec.updatedAtMs = nowVal
  var log = store.saveTaskTxn(rec, captureWrites, some(prev))
  if not captureWrites:
    log = @[]
  (true, log)

proc ackTask*(store: EventStore; taskId: string; receipt: string): bool =
  let (ok, _) = store.ackTaskTxn(taskId, receipt, false)
  ok

proc ackTaskWithLog*(store: EventStore;
                     taskId: string;
                     receipt: string): (bool, seq[KVWrite]) =
  store.ackTaskTxn(taskId, receipt, true)

proc retryTaskTxn(store: EventStore;
                  taskId: string;
                  receipt: string;
                  delayMs: int;
                  reason: string;
                  resetAttempts: bool;
                  captureWrites: bool): (bool, seq[KVWrite]) =
  if store.isNil():
    return (false, @[])
  acquire(store.taskLock)
  defer: release(store.taskLock)
  if not store.tasks.hasKey(taskId):
    return (false, @[])
  var rec = store.tasks[taskId]
  if receipt.len > 0 and rec.receipt != receipt:
    return (false, @[])
  let nowVal = nowMs()
  let prev = rec
  rec.status = tsReady
  rec.receipt = ""
  if resetAttempts:
    rec.attempts = 0
  rec.visibleAtMs = nowVal + max(0, delayMs).int64
  rec.updatedAtMs = nowVal
  if reason.len > 0:
    rec.lastError = reason
  store.ensureTaskIndexUnlocked(taskId)
  var log = store.saveTaskTxn(rec, captureWrites, some(prev))
  if not captureWrites:
    log = @[]
  (true, log)

proc retryTask*(store: EventStore;
                taskId: string;
                receipt: string = "";
                delayMs = 0;
                reason = "";
                resetAttempts = false): bool =
  let (ok, _) = store.retryTaskTxn(taskId, receipt, delayMs, reason, resetAttempts, false)
  ok

proc retryTaskWithLog*(store: EventStore;
                       taskId: string;
                       receipt: string = "";
                       delayMs = 0;
                       reason = "";
                       resetAttempts = false): (bool, seq[KVWrite]) =
  store.retryTaskTxn(taskId, receipt, delayMs, reason, resetAttempts, true)

proc replayWrites*(store: EventStore;
                   writes: openArray[KVWrite];
                   dedupe = true): pebble_core.SequenceNumber =
  if store.isNil():
    raise newException(IOError, "event store is not initialised")
  if store.backend != sbPebble or store.pebble.kv.isNil():
    raise newException(IOError, "Pebble backend required")
  store.pebble.kv.replayWrites(writes, dedupe = dedupe)

proc close*(store: EventStore) =
  if store.isNil():
    return
  if store.backend == sbPebble and not store.pebble.kv.isNil():
    store.pebble.kv.close()
