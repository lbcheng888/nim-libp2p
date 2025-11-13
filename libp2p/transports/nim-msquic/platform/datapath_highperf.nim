## 对应 `datapath_raw_*`, `datapath_iouring.c`, `datapath_raw_xdp_*` 的 Nim 高性能路径骨架，涵盖 RSS 与线程亲和配置。

import std/[algorithm, asyncdispatch, hashes, net, nativesockets, strutils, tables]

import ./common
import ./datapath_runtime

type
  RssHashConfig* = object
    key*: seq[uint8]
    queueCount*: int
    enableIpv6*: bool

  WorkerAffinity* = object
    workerId*: int
    cpus*: seq[int]
    applied*: bool
    lastError*: string
    endpoints*: seq[UdpEndpoint]

  RecvHandler = proc(endpoint: UdpEndpoint, msg: UdpMessage) {.closure.}

  PendingSend = ref object
    endpoint*: UdpEndpoint
    request*: UdpSendRequest
    completion*: Future[void]

  RecvEvent = object
    endpoint*: UdpEndpoint
    message*: UdpMessage
    handler*: RecvHandler

  QueueStats* = object
    flushedBatches*: int
    flushedDatagrams*: int
    recvDispatches*: int
    recvMessages*: int
    batchSendmmsgCalls*: int
    batchFallbacks*: int

  QueueRuntime = ref object
    queueId*: int
    pendingSends*: seq[PendingSend]
    pendingRecv*: seq[RecvEvent]
    flushScheduled*: bool
    recvScheduled*: bool

  BatchKey = object
    endpoint: UdpEndpoint
    remoteAddr: string
    remotePort: Port

  HighPerfDatapath* = ref object
    loop*: EventLoop
    datapathType*: DatapathType
    rss*: RssHashConfig
    workers*: seq[WorkerAffinity]
    features*: set[DatapathFeature]
    queueRuntimes*: seq[QueueRuntime]
    maxBatchSize*: int
    batchFlushIntervalMs*: int
    queueStats*: seq[QueueStats]
    recvOriginalHandlers*: Table[UdpEndpoint, RecvHandler]

  DatapathPerfSnapshot* = object
    datapathType*: DatapathType
    queueDepths*: seq[int]
    features*: set[DatapathFeature]
    affinityApplied*: bool
    rssKeyHash*: uint32
    queuePendingSends*: seq[int]
    queuePendingRecvs*: seq[int]
    queueFlushedBatches*: seq[int]
    queueRecvDispatches*: seq[int]
    totalBatchFlushes*: int
    totalBatchDatagrams*: int
    totalRecvDispatch*: int
    totalRecvMessages*: int
    batchSendmmsgCalls*: int
    batchFallbackSends*: int

proc newQueueRuntime(queueId: int): QueueRuntime =
  new(result)
  result.queueId = queueId
  result.pendingSends = @[]
  result.pendingRecv = @[]
  result.flushScheduled = false
  result.recvScheduled = false

proc newPendingSend(endpoint: UdpEndpoint, request: UdpSendRequest): PendingSend =
  new(result)
  result.endpoint = endpoint
  result.request = request
  result.completion = newFuture[void]("HighPerfDatapath.pendingSend")

proc hash(endpoint: UdpEndpoint): Hash =
  ## 利用引用地址为 UdpEndpoint 提供哈希，便于在 Table 中使用。
  hash(cast[pointer](endpoint))

const
  DefaultToeplitzKeyHex = "6d5a56da255b0ec24167253d43a38fb0d0ca2bcbae7b30b477cb2da38030f20c6a42b73bbeac01fa"

proc parseHexByte(hex: string): uint8 =
  uint8(parseHexInt(hex))

proc defaultToeplitzKey*(): seq[uint8] =
  ## 默认使用 MsQuic 提供的 Toeplitz 秘钥。
  result = @[]
  for idx in countup(0, DefaultToeplitzKeyHex.len - 2, 2):
    result.add(parseHexByte(DefaultToeplitzKeyHex[idx .. idx + 1]))

proc normalizeCores(cpus: openArray[int]): seq[int] =
  result = @[]
  for core in cpus:
    if core < 0:
      continue
    if core notin result:
      result.add(core)
  result.sort()

proc newRssHashConfig*(queueCount: Positive, enableIpv6: bool = true,
                       key: openArray[uint8] = defaultToeplitzKey()): RssHashConfig =
  if queueCount <= 0:
    raise newException(ValueError, "队列数量必须为正")
  if key.len < 16:
    raise newException(ValueError, "Toeplitz key 长度不足")
  result.key = @key
  result.queueCount = queueCount
  result.enableIpv6 = enableIpv6

proc updateKey*(config: var RssHashConfig, key: openArray[uint8]) =
  if key.len < 16:
    raise newException(ValueError, "Toeplitz key 长度不足")
  config.key = @key

proc toBitSeq(data: openArray[uint8]): seq[bool] =
  result = newSeq[bool](data.len * 8)
  var idx = 0
  for byte in data:
    for bitOffset in countdown(7, 0):
      result[idx] = ((byte shr bitOffset) and 1'u8) == 1'u8
      inc idx

proc bitsToUint32(bits: openArray[bool], start: int): uint32 =
  var value = 0'u32
  for offset in 0..<32:
    value = value shl 1
    let bitIndex = start + offset
    if bitIndex < bits.len and bits[bitIndex]:
      value = value or 1'u32
  value

proc toeplitzHash(key, data: openArray[uint8]): uint32 =
  let keyBits = toBitSeq(key)
  let dataBits = toBitSeq(data)
  if keyBits.len < dataBits.len + 32:
    raise newException(ValueError, "Toeplitz key 与输入长度不匹配")
  var acc = 0'u32
  for idx in 0..<dataBits.len:
    if dataBits[idx]:
      acc = acc xor bitsToUint32(keyBits, idx)
  result = acc

proc encodeIp(address: string): tuple[bytes: seq[uint8], isIpv6: bool] =
  try:
    let parsed = parseIpAddress(address)
    case parsed.family
    of IpAddressFamily.IPv6:
      result.bytes = @(parsed.address_v6)
      result.isIpv6 = true
    else:
      result.bytes = @(parsed.address_v4)
      result.isIpv6 = false
  except ValueError:
    let raw = uint32(hash(address))
    result.bytes = @[
      uint8((raw shr 24) and 0xff'u32),
      uint8((raw shr 16) and 0xff'u32),
      uint8((raw shr 8) and 0xff'u32),
      uint8(raw and 0xff'u32)]
    result.isIpv6 = false

proc widenToIpv6(bytes: seq[uint8]): seq[uint8] =
  if bytes.len == 16:
    return bytes
  result = newSeq[uint8](16)
  if bytes.len == 4:
    result[10] = 0xff
    result[11] = 0xff
    for i in 0..<4:
      result[12 + i] = bytes[i]
  else:
    let copyLen = (if bytes.len < 16: bytes.len else: 16)
    for i in 0..<copyLen:
      result[16 - copyLen + i] = bytes[i]

proc reduceToIpv4(bytes: seq[uint8]): seq[uint8] =
  if bytes.len == 4:
    return bytes
  result = newSeq[uint8](4)
  if bytes.len >= 4:
    for i in 0..<4:
      result[i] = bytes[bytes.len - 4 + i]
  else:
    for i in 0..<bytes.len:
      result[4 - bytes.len + i] = bytes[i]

proc portToBytes(port: Port): array[2, uint8] =
  let value = uint16(int(port) and 0xffff)
  [uint8((value shr 8) and 0xff'u16), uint8(value and 0xff'u16)]

proc packToeplitzInput(localAddr, remoteAddr: string,
                       localPort, remotePort: Port,
                       config: RssHashConfig): seq[uint8] =
  let src = encodeIp(localAddr)
  let dst = encodeIp(remoteAddr)
  let useIpv6 = config.enableIpv6 and (src.isIpv6 or dst.isIpv6)
  var srcBytes: seq[uint8]
  var dstBytes: seq[uint8]
  if useIpv6:
    srcBytes = widenToIpv6(src.bytes)
    dstBytes = widenToIpv6(dst.bytes)
  else:
    srcBytes = reduceToIpv4(src.bytes)
    dstBytes = reduceToIpv4(dst.bytes)
  let srcPortBytes = portToBytes(localPort)
  let dstPortBytes = portToBytes(remotePort)
  var buffer: seq[uint8] = @[]
  buffer.add(srcBytes)
  buffer.add(dstBytes)
  buffer.add(srcPortBytes)
  buffer.add(dstPortBytes)
  result = buffer

proc computeRssHash*(config: RssHashConfig, localAddr, remoteAddr: string,
                     localPort, remotePort: Port): uint32 =
  let input = packToeplitzInput(localAddr, remoteAddr, localPort, remotePort, config)
  toeplitzHash(config.key, input)

proc queueIndex*(config: RssHashConfig, localAddr, remoteAddr: string,
                 localPort, remotePort: Port): int =
  if config.queueCount <= 1:
    return 0
  int(computeRssHash(config, localAddr, remoteAddr, localPort, remotePort) mod uint32(config.queueCount))

when defined(linux):
  import std/[posix, strformat]

  type
    Socklen = posix.socklen_t
    Iovec {.importc: "struct iovec", header: "<sys/uio.h>".} = object
      iov_base: pointer
      iov_len: csize_t
    Msghdr {.importc: "struct msghdr", header: "<sys/socket.h>".} = object
      msg_name: pointer
      msg_namelen: Socklen
      msg_iov: ptr Iovec
      msg_iovlen: csize_t
      msg_control: pointer
      msg_controllen: csize_t
      msg_flags: cint
    Mmsghdr {.importc: "struct mmsghdr", header: "<sys/socket.h>".} = object
      msg_hdr: Msghdr
      msg_len: cuint

  proc sendmmsg(fd: cint, msgvec: ptr Mmsghdr, vlen: cuint, flags: cint): cint {.importc, header: "<sys/socket.h>".}

  proc initSockaddr(remoteAddr: string, remotePort: Port,
                    storage: var Sockaddr_storage,
                    outLen: var Socklen): bool =
    var hints: Addrinfo
    zeroMem(addr hints, sizeof(hints))
    hints.ai_family = AF_UNSPEC
    hints.ai_socktype = SOCK_DGRAM
    let portStr = $remotePort
    var res: ptr Addrinfo
    if getAddrInfo(remoteAddr, portStr, addr hints, addr res) != 0 or res.isNil:
      return false
    defer:
      if res != nil:
        freeAddrInfo(res)
    copyMem(addr storage, res.ai_addr, res.ai_addrlen)
    outLen = Socklen(res.ai_addrlen)
    result = true

  type CpuSet = array[16, uint64]

  proc cpuZero(mask: var CpuSet) =
    for i in 0..<mask.len:
      mask[i] = 0

  proc cpuSet(mask: var CpuSet, cpu: int) =
    let idx = cpu div 64
    let shift = cpu mod 64
    if idx < mask.len and cpu >= 0:
      mask[idx] = mask[idx] or (1'u64 shl shift)

  proc sched_setaffinity(pid: cint, cpusetsize: csize_t, mask: ptr CpuSet): cint {.importc, header: "<sched.h>".}

when defined(windows):
  import winlean
  type
    DWORD_PTR = culonglong

  proc GetCurrentThread(): winlean.HANDLE {.stdcall, dynlib: "kernel32", importc.}
  proc SetThreadAffinityMask(hThread: winlean.HANDLE, dwThreadAffinityMask: DWORD_PTR): DWORD_PTR {.stdcall, dynlib: "kernel32", importc.}

proc applyCpuAffinity(binding: var WorkerAffinity) =
  binding.applied = false
  binding.lastError.setLen(0)
  if binding.cpus.len == 0:
    binding.lastError = "未配置 CPU 亲和列表"
    return

  when defined(linux):
    var mask: CpuSet
    cpuZero(mask)
    var valid = false
    for cpu in binding.cpus:
      if cpu >= 0 and cpu < mask.len * 64:
        cpuSet(mask, cpu)
        valid = true
    if not valid:
      binding.lastError = "CPU 序号超出可配置范围"
      return
    let res = sched_setaffinity(0, csize_t(sizeof(mask)), addr mask)
    if res == 0:
      binding.applied = true
    else:
      binding.lastError = fmt"sched_setaffinity 失败: errno {errno}"
  elif defined(windows):
    var mask: DWORD_PTR = 0
    for cpu in binding.cpus:
      if cpu >= 0 and cpu < 64:
        mask = mask or (DWORD_PTR(1) shl DWORD_PTR(cpu))
    if mask == 0:
      binding.lastError = "CPU 序号超出 0..63"
      return
    let handle = GetCurrentThread()
    if handle == nil:
      binding.lastError = "获取线程句柄失败"
      return
    if SetThreadAffinityMask(handle, mask) != 0:
      binding.applied = true
    else:
      binding.lastError = "SetThreadAffinityMask 调用失败"
  else:
    binding.lastError = "当前平台不支持线程亲和配置"

proc newHighPerfDatapath*(loop: EventLoop, datapathType: DatapathType,
                          queueCount: Positive,
                          features: set[DatapathFeature] = {},
                          maxBatchSize: Positive = 8,
                          batchFlushIntervalMs: Natural = 1): HighPerfDatapath =
  if loop.isNil:
    raise newException(ValueError, "事件循环不可为 nil")
  new(result)
  result.loop = loop
  result.datapathType = datapathType
  result.features = features
  result.rss = newRssHashConfig(queueCount)
  result.maxBatchSize = int(maxBatchSize)
  result.batchFlushIntervalMs = int(batchFlushIntervalMs)
  result.recvOriginalHandlers = initTable[UdpEndpoint, RecvHandler]()
  result.workers = newSeqOfCap[WorkerAffinity](queueCount)
  result.queueRuntimes = newSeqOfCap[QueueRuntime](queueCount)
  result.queueStats = newSeqOfCap[QueueStats](queueCount)
  for id in 0..<queueCount:
    result.workers.add(WorkerAffinity(
      workerId: id,
      cpus: @[id],
      applied: false,
      lastError: "",
      endpoints: @[]))
    result.queueRuntimes.add(newQueueRuntime(id))
    result.queueStats.add(QueueStats())

proc configureBatching*(datapath: HighPerfDatapath, maxBatchSize: Positive,
                        flushIntervalMs: Natural) =
  if datapath.isNil:
    raise newException(ValueError, "datapath 未初始化")
  datapath.maxBatchSize = int(maxBatchSize)
  datapath.batchFlushIntervalMs = int(flushIntervalMs)

proc queueCount*(datapath: HighPerfDatapath): int =
  datapath.rss.queueCount

proc configureWorkerAffinity*(datapath: HighPerfDatapath, workerId: Natural,
                              cpus: openArray[int]) =
  if workerId >= datapath.workers.len:
    raise newException(ValueError, "workerId 超出范围")
  var binding = datapath.workers[workerId]
  binding.cpus = normalizeCores(cpus)
  applyCpuAffinity(binding)
  datapath.workers[workerId] = binding

proc workerBinding*(datapath: HighPerfDatapath, workerId: Natural): WorkerAffinity =
  if workerId >= datapath.workers.len:
    raise newException(ValueError, "workerId 超出范围")
  datapath.workers[workerId]

proc queueForPacket*(datapath: HighPerfDatapath,
                     srcAddr, dstAddr: string,
                     srcPort, dstPort: Port): int =
  datapath.rss.queueIndex(srcAddr, dstAddr, srcPort, dstPort)

proc queueForEndpoint*(datapath: HighPerfDatapath, endpoint: UdpEndpoint,
                       remoteAddr: string, remotePort: Port): int =
  datapath.queueForPacket(endpoint.localAddress, remoteAddr,
                          endpoint.localPort, remotePort)

proc queueStats*(datapath: HighPerfDatapath, queueId: Natural): QueueStats =
  if datapath.isNil:
    raise newException(ValueError, "datapath 未初始化")
  if queueId >= datapath.queueStats.len:
    raise newException(ValueError, "queueId 超出范围")
  datapath.queueStats[queueId]

proc flushQueue*(datapath: HighPerfDatapath, queueId: Natural): Future[void] {.async.}

proc flushQueueLater(datapath: HighPerfDatapath, queueId: int,
                     delayMs: int) {.async.} =
  if delayMs > 0:
    await sleepAsync(delayMs)
  if queueId >= 0:
    await datapath.flushQueue(Natural(queueId))

proc scheduleBatchFlush(datapath: HighPerfDatapath, queueId: int) =
  if datapath.isNil:
    return
  if queueId < 0 or queueId >= datapath.queueRuntimes.len:
    return
  let runtime = datapath.queueRuntimes[queueId]
  if runtime.isNil or runtime.pendingSends.len == 0:
    return
  if runtime.pendingSends.len >= datapath.maxBatchSize or datapath.batchFlushIntervalMs <= 0:
    asyncCheck datapath.flushQueue(Natural(queueId))
  elif not runtime.flushScheduled:
    runtime.flushScheduled = true
    asyncCheck datapath.flushQueueLater(queueId, datapath.batchFlushIntervalMs)

proc trySendBatch(datapath: HighPerfDatapath, queueId: int,
                  key: BatchKey, items: seq[PendingSend]): int =
  when defined(linux):
    if items.len == 0:
      return 0
    let first = items[0]
    if first.isNil or first.endpoint.isNil or first.endpoint.socket.isNil:
      return 0
    var dest: Sockaddr_storage
    var destLen: Socklen
    if not initSockaddr(key.remoteAddr, key.remotePort, dest, destLen):
      return 0
    var msgVec = newSeq[Mmsghdr](items.len)
    var ioVecs = newSeq[Iovec](items.len)
    var names = newSeq[Sockaddr_storage](items.len)
    for idx, entry in items.pairs:
      let payload = entry.request.payload
      if payload.len == 0:
        return idx
      names[idx] = dest
      ioVecs[idx].iov_base = cast[pointer](unsafeAddr payload[0])
      ioVecs[idx].iov_len = csize_t(payload.len)
      msgVec[idx].msg_hdr.msg_name = cast[pointer](addr names[idx])
      msgVec[idx].msg_hdr.msg_namelen = destLen
      msgVec[idx].msg_hdr.msg_iov = addr ioVecs[idx]
      msgVec[idx].msg_hdr.msg_iovlen = 1
      msgVec[idx].msg_hdr.msg_control = nil
      msgVec[idx].msg_hdr.msg_controllen = 0
      msgVec[idx].msg_hdr.msg_flags = 0
      msgVec[idx].msg_len = 0
    let fd = cint(first.endpoint.socket.fd)
    if fd == cint(osInvalidSocket):
      return 0
    datapath.queueStats[queueId].batchSendmmsgCalls.inc
    let sent = sendmmsg(fd, addr msgVec[0], cuint(items.len), 0)
    if sent <= 0:
      return 0
    return int(sent)
  else:
    discard
  0

proc sendPendingBatch(datapath: HighPerfDatapath, queueId: int,
                      pending: seq[PendingSend]) {.async.} =
  if pending.len == 0:
    return
  var grouped = initTable[BatchKey, seq[PendingSend]]()
  for entry in pending:
    if entry.isNil:
      continue
    if entry.request.payload.len == 0:
      if not entry.completion.finished:
        entry.completion.complete()
      continue
    let key = BatchKey(
      endpoint: entry.endpoint,
      remoteAddr: entry.request.targetAddress,
      remotePort: entry.request.targetPort)
    grouped.mgetOrPut(key, @[]).add(entry)
  for key, items in grouped.mpairs:
    if items.len == 0:
      continue
    var sent = datapath.trySendBatch(queueId, key, items)
    if sent < 0:
      sent = 0
    if sent > items.len:
      sent = items.len
    for idx in 0..<sent:
      let fut = items[idx].completion
      if not fut.finished:
        fut.complete()
    if sent < items.len:
      let remaining = items.len - sent
      datapath.queueStats[queueId].batchFallbacks += remaining
      for idx in sent..<items.len:
        let entry = items[idx]
        try:
          await entry.endpoint.sendDatagram(entry.request)
          if not entry.completion.finished:
            entry.completion.complete()
        except CatchableError as err:
          if not entry.completion.finished:
            entry.completion.fail(err)

proc flushQueue*(datapath: HighPerfDatapath, queueId: Natural): Future[void] {.async.} =
  if datapath.isNil:
    return
  if queueId >= datapath.queueRuntimes.len:
    raise newException(ValueError, "queueId 超出范围")
  let runtime = datapath.queueRuntimes[queueId]
  if runtime.isNil:
    return
  let pending = runtime.pendingSends
  if pending.len == 0:
    runtime.flushScheduled = false
    return
  runtime.pendingSends = @[]
  runtime.flushScheduled = false
  datapath.queueStats[queueId].flushedBatches.inc
  datapath.queueStats[queueId].flushedDatagrams += pending.len
  await datapath.sendPendingBatch(int(queueId), pending)

proc flushAll*(datapath: HighPerfDatapath): Future[void] {.async.} =
  if datapath.isNil:
    return
  for queueId in 0..<datapath.queueRuntimes.len:
    await datapath.flushQueue(Natural(queueId))

proc enqueueSend(datapath: HighPerfDatapath, queueId: int,
                 pending: PendingSend): Future[void] =
  if pending.isNil:
    let fut = newFuture[void]("HighPerfDatapath.enqueueSend.nil")
    fut.complete()
    return fut
  if queueId < 0 or queueId >= datapath.queueRuntimes.len:
    pending.completion.fail(newException(ValueError, "queueId 超出范围"))
    return pending.completion
  let runtime = datapath.queueRuntimes[queueId]
  runtime.pendingSends.add(pending)
  datapath.scheduleBatchFlush(queueId)
  pending.completion

proc runRecvQueue(datapath: HighPerfDatapath, queueId: int) {.async.} =
  if datapath.isNil:
    return
  if queueId < 0 or queueId >= datapath.queueRuntimes.len:
    return
  let runtime = datapath.queueRuntimes[queueId]
  runtime.recvScheduled = false
  if runtime.pendingRecv.len == 0:
    return
  let events = runtime.pendingRecv
  runtime.pendingRecv = @[]
  datapath.queueStats[queueId].recvDispatches.inc
  datapath.queueStats[queueId].recvMessages += events.len
  for event in events:
    if event.handler != nil:
      try:
        event.handler(event.endpoint, event.message)
      except CatchableError:
        discard

proc scheduleRecvDispatch(datapath: HighPerfDatapath, queueId: int) =
  if datapath.isNil:
    return
  if queueId < 0 or queueId >= datapath.queueRuntimes.len:
    return
  let runtime = datapath.queueRuntimes[queueId]
  if runtime.pendingRecv.len == 0:
    return
  if runtime.recvScheduled:
    return
  runtime.recvScheduled = true
  asyncCheck datapath.runRecvQueue(queueId)

proc dispatchInbound(datapath: HighPerfDatapath, endpoint: UdpEndpoint,
                     msg: UdpMessage) =
  if endpoint.isNil:
    return
  var handler: RecvHandler = nil
  if endpoint in datapath.recvOriginalHandlers:
    handler = datapath.recvOriginalHandlers[endpoint]
  if handler.isNil:
    return
  let queue = datapath.queueForEndpoint(endpoint, msg.remoteAddress, msg.remotePort)
  if queue < 0 or queue >= datapath.queueRuntimes.len:
    handler(endpoint, msg)
    return
  let runtime = datapath.queueRuntimes[queue]
  if runtime.isNil:
    handler(endpoint, msg)
    return
  runtime.pendingRecv.add(RecvEvent(
    endpoint: endpoint,
    message: msg,
    handler: handler))
  datapath.scheduleRecvDispatch(queue)

proc attachEndpoint*(datapath: HighPerfDatapath, endpoint: UdpEndpoint,
                     remoteAddr: string, remotePort: Port): int =
  if endpoint.isNil:
    raise newException(ValueError, "端点不可为 nil")
  let queue = datapath.queueForEndpoint(endpoint, remoteAddr, remotePort)
  var binding = datapath.workers[queue]
  if endpoint notin binding.endpoints:
    binding.endpoints.add(endpoint)
  datapath.workers[queue] = binding
  if endpoint notin datapath.recvOriginalHandlers:
    datapath.recvOriginalHandlers[endpoint] = endpoint.handlers.onDatagram
    let dp = datapath
    endpoint.handlers.onDatagram = proc(ep: UdpEndpoint, msg: UdpMessage) {.closure.} =
      dp.dispatchInbound(ep, msg)
  queue

proc routeDatagram*(datapath: HighPerfDatapath, endpoint: UdpEndpoint,
                    remoteAddr: string, remotePort: Port,
                    payload: seq[uint8],
                    ecn: EcnType = ecnNonEct,
                    dscp: DscpType = dscpCs0): Future[void] =
  if endpoint.isNil:
    raise newException(ValueError, "端点不可为 nil")
  let queue = datapath.attachEndpoint(endpoint, remoteAddr, remotePort)
  let request = initSendRequest(
    remoteAddr,
    remotePort,
    payload,
    datapathType = datapath.datapathType,
    ecn = ecn,
    dscp = dscp)
  let pending = newPendingSend(endpoint, request)
  result = datapath.enqueueSend(queue, pending)

proc routeDatagram*(datapath: HighPerfDatapath, endpoint: UdpEndpoint,
                    remoteAddr: string, remotePort: Port,
                    payload: openArray[uint8],
                    ecn: EcnType = ecnNonEct,
                    dscp: DscpType = dscpCs0): Future[void] =
  var buffer = newSeq[uint8](payload.len)
  for i in 0..<payload.len:
    buffer[i] = payload[i]
  result = datapath.routeDatagram(endpoint, remoteAddr, remotePort, buffer, ecn, dscp)

proc snapshot*(datapath: HighPerfDatapath): DatapathPerfSnapshot =
  result.datapathType = datapath.datapathType
  result.features = datapath.features
  result.affinityApplied = true
  result.queueDepths = @[]
  result.queuePendingSends = @[]
  result.queuePendingRecvs = @[]
  result.queueFlushedBatches = @[]
  result.queueRecvDispatches = @[]
  var queueCount = datapath.queueRuntimes.len
  if datapath.workers.len > queueCount:
    queueCount = datapath.workers.len
  var totalBatchFlushes = 0
  var totalBatchDatagrams = 0
  var totalRecvDispatch = 0
  var totalRecvMessages = 0
  var totalSendmmsg = 0
  var totalFallbacks = 0
  for idx in 0..<queueCount:
    if idx < datapath.workers.len:
      let binding = datapath.workers[idx]
      result.queueDepths.add(binding.endpoints.len)
      result.affinityApplied = result.affinityApplied and binding.applied
    else:
      result.queueDepths.add(0)
    let runtime =
      if idx < datapath.queueRuntimes.len: datapath.queueRuntimes[idx]
      else: nil
    result.queuePendingSends.add(if runtime.isNil: 0 else: runtime.pendingSends.len)
    result.queuePendingRecvs.add(if runtime.isNil: 0 else: runtime.pendingRecv.len)
    let stats =
      if idx < datapath.queueStats.len: datapath.queueStats[idx]
      else: QueueStats()
    result.queueFlushedBatches.add(stats.flushedBatches)
    result.queueRecvDispatches.add(stats.recvDispatches)
    totalBatchFlushes += stats.flushedBatches
    totalBatchDatagrams += stats.flushedDatagrams
    totalRecvDispatch += stats.recvDispatches
    totalRecvMessages += stats.recvMessages
    totalSendmmsg += stats.batchSendmmsgCalls
    totalFallbacks += stats.batchFallbacks
  result.totalBatchFlushes = totalBatchFlushes
  result.totalBatchDatagrams = totalBatchDatagrams
  result.totalRecvDispatch = totalRecvDispatch
  result.totalRecvMessages = totalRecvMessages
  result.batchSendmmsgCalls = totalSendmmsg
  result.batchFallbackSends = totalFallbacks
  let keyHash = uint32(hash(datapath.rss.key))
  result.rssKeyHash = keyHash

proc enableFeature*(datapath: HighPerfDatapath, feature: DatapathFeature) =
  datapath.features.incl(feature)

proc disableFeature*(datapath: HighPerfDatapath, feature: DatapathFeature) =
  datapath.features.excl(feature)
