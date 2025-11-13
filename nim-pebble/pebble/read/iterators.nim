# 读路径迭代器实现：聚合多源、支持边界与前缀过滤，并提供分段预取与优先级调度。

import std/[algorithm, options, sets, strutils, tables]

import pebble/core/types
import pebble/read/types

type
  PrefetchTask = object
    segmentId: int
    priority: int
    distance: int

  PrefetchScheduler = ref object
    queue: seq[PrefetchTask]
    inFlight: HashSet[int]

  ReadIterator* = ref object
    comparator: Comparator
    allEntries: seq[MaterializedEntry]
    filtered: seq[MaterializedEntry]
    idx: int
    validFlag: bool
    bounds: ReadRange
    prefixFilter: Option[string]
    segmentSize: int
    prefetchDistance: int
    maxCachedSegments: int
    segmentCache: Table[int, seq[MaterializedEntry]]
    recentSegments: seq[int]
    scheduler: PrefetchScheduler

proc removeAt[T](s: var seq[T]; idx: int) =
  if idx < 0 or idx >= s.len:
    return
  for i in idx ..< s.len - 1:
    s[i] = s[i + 1]
  s.setLen(s.len - 1)

proc newPrefetchScheduler(): PrefetchScheduler =
  PrefetchScheduler(queue: @[], inFlight: initHashSet[int]())

proc enqueue(s: PrefetchScheduler; task: PrefetchTask) =
  if s.isNil():
    return
  if not s.inFlight.contains(task.segmentId):
    s.queue.add(task)
    s.inFlight.incl(task.segmentId)

proc isQueued(s: PrefetchScheduler; segmentId: int): bool =
  if s.isNil():
    return false
  s.inFlight.contains(segmentId)

proc nextTask(s: PrefetchScheduler): Option[PrefetchTask] =
  if s.isNil() or s.queue.len == 0:
    return none(PrefetchTask)
  var bestIdx = 0
  var bestTask = s.queue[0]
  for i in 1 ..< s.queue.len:
    let candidate = s.queue[i]
    if candidate.priority < bestTask.priority or
       (candidate.priority == bestTask.priority and candidate.distance < bestTask.distance):
      bestIdx = i
      bestTask = candidate
  s.queue[bestIdx] = s.queue[^1]
  s.queue.setLen(s.queue.len - 1)
  some(bestTask)

proc complete(s: PrefetchScheduler; segmentId: int) =
  if s.isNil():
    return
  if s.inFlight.contains(segmentId):
    s.inFlight.excl(segmentId)

proc materializeEntries(sources: openArray[ReadSource];
                        comparator: Comparator): seq[MaterializedEntry] =
  type Candidate = tuple[priority: int,
                         order: int,
                         value: Option[string],
                         key: Key,
                         kind: ReadSourceKind]
  var best = initTable[string, Candidate]()
  for order, source in sources.pairs():
    for entry in source.entries:
      let keyBytes = entry.key.toBytes()
      let candidate = (priority: source.priority,
                       order: order,
                       value: entry.value,
                       key: entry.key,
                       kind: source.kind)
      if best.hasKey(keyBytes):
        let existing = best[keyBytes]
        if candidate.priority < existing.priority or
           (candidate.priority == existing.priority and candidate.order < existing.order):
          best[keyBytes] = candidate
      else:
        best[keyBytes] = candidate
  for candidate in best.values():
    if candidate.value.isSome():
      result.add(MaterializedEntry(key: candidate.key,
                                   value: candidate.value.get(),
                                   sourcePriority: candidate.priority,
                                   sourceKind: candidate.kind))
  result.sort(proc (a, b: MaterializedEntry): int =
    comparator(a.key, b.key)
  )

proc inRange(range: ReadRange; comparator: Comparator; key: Key): bool =
  if range.lower.isSome():
    if comparator(key, range.lower.get()) < 0:
      return false
  if range.upper.isSome():
    if comparator(key, range.upper.get()) >= 0:
      return false
  true

proc matchesPrefix(prefix: Option[string]; key: Key): bool =
  if prefix.isSome():
    return key.toBytes().startsWith(prefix.get())
  true

proc resetSegments(it: ReadIterator) =
  it.segmentCache = initTable[int, seq[MaterializedEntry]]()
  it.recentSegments.setLen(0)
  it.scheduler = newPrefetchScheduler()

proc segmentIdForIndex(it: ReadIterator; idx: int): int =
  if it.segmentSize <= 0 or idx < 0:
    return -1
  idx div it.segmentSize

proc maxSegmentId(it: ReadIterator): int =
  if it.segmentSize <= 0 or it.filtered.len == 0:
    return -1
  (it.filtered.len - 1) div it.segmentSize

proc segmentPriority(it: ReadIterator; segmentId: int): int =
  if it.segmentSize <= 0:
    return high(int)
  let start = segmentId * it.segmentSize
  if start >= it.filtered.len:
    return high(int)
  let stop = min(it.filtered.len, start + it.segmentSize)
  var best = high(int)
  for i in start ..< stop:
    let prio = it.filtered[i].sourcePriority
    if prio < best:
      best = prio
  if best == high(int):
    return high(int)
  best

proc updateSegmentUsage(it: ReadIterator; segmentId: int) =
  var pos = -1
  for i, seg in it.recentSegments.pairs():
    if seg == segmentId:
      pos = i
      break
  if pos >= 0:
    removeAt(it.recentSegments, pos)
  it.recentSegments.add(segmentId)
  let cacheCap = if it.maxCachedSegments <= 0: 0 else: it.maxCachedSegments
  while it.recentSegments.len > cacheCap and cacheCap > 0:
    let evict = it.recentSegments[0]
    removeAt(it.recentSegments, 0)
    if it.segmentCache.hasKey(evict):
      it.segmentCache.del(evict)

proc loadSegment(it: ReadIterator; segmentId: int) =
  if segmentId < 0 or it.segmentSize <= 0:
    return
  if it.segmentCache.hasKey(segmentId):
    it.updateSegmentUsage(segmentId)
    if not it.scheduler.isNil():
      it.scheduler.complete(segmentId)
    return
  let start = segmentId * it.segmentSize
  if start >= it.filtered.len:
    if not it.scheduler.isNil():
      it.scheduler.complete(segmentId)
    return
  let stop = min(it.filtered.len, start + it.segmentSize)
  var segment: seq[MaterializedEntry] = @[]
  for i in start ..< stop:
    segment.add(it.filtered[i])
  it.segmentCache[segmentId] = segment
  it.updateSegmentUsage(segmentId)
  if not it.scheduler.isNil():
    it.scheduler.complete(segmentId)

proc ensureSegment(it: ReadIterator; segmentId: int) =
  if segmentId < 0 or it.segmentSize <= 0:
    return
  if it.segmentCache.hasKey(segmentId):
    it.updateSegmentUsage(segmentId)
  else:
    it.loadSegment(segmentId)

proc scheduleSegment(it: ReadIterator; segmentId: int; distance: int) =
  if segmentId < 0 or it.segmentSize <= 0:
    return
  if segmentId > it.maxSegmentId():
    return
  if it.segmentCache.hasKey(segmentId):
    it.updateSegmentUsage(segmentId)
    return
  if it.scheduler.isNil():
    return
  if it.scheduler.isQueued(segmentId):
    return
  let priority = it.segmentPriority(segmentId)
  it.scheduler.enqueue(PrefetchTask(segmentId: segmentId,
                                    priority: priority,
                                    distance: distance))

proc runPrefetch(it: ReadIterator; budget: int) =
  if it.scheduler.isNil():
    return
  var remaining = budget
  if remaining <= 0:
    remaining = 1
  while remaining > 0:
    let taskOpt = it.scheduler.nextTask()
    if taskOpt.isNone():
      break
    let task = taskOpt.get()
    it.loadSegment(task.segmentId)
    dec(remaining)

proc prefetchAround(it: ReadIterator; idx: int) =
  if it.prefetchDistance < 0 or it.segmentSize <= 0 or it.filtered.len == 0:
    return
  var baseIdx = idx
  if baseIdx < 0:
    baseIdx = 0
  let baseSegment = it.segmentIdForIndex(baseIdx)
  let limit = it.prefetchDistance
  for offset in -limit .. limit:
    let seg = baseSegment + offset
    if seg < 0 or seg > it.maxSegmentId():
      continue
    let distance = abs(offset)
    it.scheduleSegment(seg, distance)
  let budget = max(1, it.prefetchDistance * 2 + 1)
  it.runPrefetch(budget)
  it.ensureSegment(baseSegment)

proc getSegmentEntries(it: ReadIterator; segmentId: int): seq[MaterializedEntry] =
  if segmentId < 0 or it.segmentSize <= 0:
    return @[]
  if not it.segmentCache.hasKey(segmentId):
    it.loadSegment(segmentId)
  if it.segmentCache.hasKey(segmentId):
    it.updateSegmentUsage(segmentId)
    return it.segmentCache[segmentId]
  @[]

proc withinBounds(it: ReadIterator; key: Key): bool =
  if not inRange(it.bounds, it.comparator, key):
    return false
  if not matchesPrefix(it.prefixFilter, key):
    return false
  true

proc rebuildFiltered(it: ReadIterator) =
  it.filtered.setLen(0)
  for entry in it.allEntries:
    if it.withinBounds(entry.key):
      it.filtered.add(entry)
  it.idx = -1
  it.validFlag = false
  it.resetSegments()
  if it.filtered.len > 0:
    it.prefetchAround(0)

proc initReadIterator*(sources: openArray[ReadSource];
                       config: ReadIteratorConfig): ReadIterator =
  new(result)
  var comp = config.comparator
  if comp.isNil():
    comp = compareBytewise
  result.comparator = comp
  result.bounds = config.bounds
  result.prefixFilter = config.prefix
  result.idx = -1
  result.validFlag = false
  result.segmentSize =
    if config.segmentSize > 0: config.segmentSize else: 64
  result.prefetchDistance =
    if config.prefetchDistance > 0: config.prefetchDistance
    elif config.prefetchDistance == 0: 1
    else: -1
  let minRequiredCache =
    if result.prefetchDistance < 0: 1 else: max(1, result.prefetchDistance * 2 + 1)
  if config.maxCachedSegments > 0:
    result.maxCachedSegments = max(config.maxCachedSegments, minRequiredCache)
  else:
    result.maxCachedSegments = max(8, minRequiredCache)
  result.segmentCache = initTable[int, seq[MaterializedEntry]]()
  result.recentSegments = @[]
  result.scheduler = newPrefetchScheduler()
  result.allEntries = materializeEntries(sources, result.comparator)
  result.rebuildFiltered()

proc count*(it: ReadIterator): int =
  it.filtered.len

proc setRange*(it: ReadIterator; range: ReadRange) =
  it.bounds = range
  it.rebuildFiltered()

proc setBounds*(it: ReadIterator;
                lower: Option[Key];
                upper: Option[Key]) =
  it.setRange(newReadRange(lower, upper))

proc clearBounds*(it: ReadIterator) =
  it.setRange(emptyRange())

proc currentRange*(it: ReadIterator): ReadRange =
  it.bounds

proc setPrefix*(it: ReadIterator; prefix: Option[string]) =
  it.prefixFilter = prefix
  it.rebuildFiltered()

proc clearPrefix*(it: ReadIterator) =
  it.setPrefix(none(string))

proc valid*(it: ReadIterator): bool =
  it.validFlag

proc currentEntry*(it: ReadIterator): MaterializedEntry =
  if not it.valid():
    raise newException(ReadError, "iterator is not positioned on a valid entry")
  it.filtered[it.idx]

proc key*(it: ReadIterator): Key =
  it.currentEntry().key

proc value*(it: ReadIterator): string =
  it.currentEntry().value

proc position(it: ReadIterator; newIdx: int): bool =
  if newIdx >= 0 and newIdx < it.filtered.len:
    it.idx = newIdx
    it.validFlag = true
    it.prefetchAround(it.idx)
    return true
  it.idx = newIdx
  it.validFlag = false
  false

proc first*(it: ReadIterator): bool =
  if it.filtered.len == 0:
    it.idx = -1
    it.validFlag = false
    return false
  it.position(0)

proc last*(it: ReadIterator): bool =
  if it.filtered.len == 0:
    it.idx = -1
    it.validFlag = false
    return false
  it.position(it.filtered.len - 1)

proc next*(it: ReadIterator): bool =
  if not it.valid():
    return it.first()
  it.position(it.idx + 1)

proc prev*(it: ReadIterator): bool =
  if not it.valid():
    return it.last()
  it.position(it.idx - 1)

proc seekGE*(it: ReadIterator; key: Key): bool =
  if it.filtered.len == 0:
    it.idx = -1
    it.validFlag = false
    return false
  var lo = 0
  var hi = it.filtered.len
  while lo < hi:
    let mid = (lo + hi) shr 1
    let cmp = it.comparator(it.filtered[mid].key, key)
    if cmp < 0:
      lo = mid + 1
    else:
      hi = mid
  it.position(lo)

proc seekLT*(it: ReadIterator; key: Key): bool =
  if it.filtered.len == 0:
    it.idx = -1
    it.validFlag = false
    return false
  var lo = 0
  var hi = it.filtered.len
  while lo < hi:
    let mid = (lo + hi) shr 1
    let cmp = it.comparator(it.filtered[mid].key, key)
    if cmp < 0:
      lo = mid + 1
    else:
      hi = mid
  it.position(lo - 1)

proc seekPrefixGE*(it: ReadIterator; prefix: string; key: Key): bool =
  it.setPrefix(some(prefix))
  it.seekGE(key)

iterator range*(it: ReadIterator;
                range: ReadRange = emptyRange();
                prefix: Option[string] = none(string)): MaterializedEntry =
  let effectivePrefix =
    if prefix.isSome(): prefix else: it.prefixFilter
  let effectiveRange =
    if range.lower.isSome() or range.upper.isSome(): range else: it.bounds
  if it.filtered.len > 0:
    var idx = 0
    var currentSegment = -1
    var segmentEntries: seq[MaterializedEntry] = @[]
    var segmentStart = 0
    while idx < it.filtered.len:
      let segId = it.segmentIdForIndex(idx)
      if segId != currentSegment:
        it.prefetchAround(idx)
        currentSegment = segId
        segmentEntries = it.getSegmentEntries(segId)
        segmentStart = segId * it.segmentSize
      var entry: MaterializedEntry
      let localIdx = idx - segmentStart
      if localIdx >= 0 and localIdx < segmentEntries.len:
        entry = segmentEntries[localIdx]
      else:
        entry = it.filtered[idx]
      if not inRange(effectiveRange, it.comparator, entry.key):
        inc idx
        continue
      if not matchesPrefix(effectivePrefix, entry.key):
        inc idx
        continue
      yield entry
      inc idx
