## 最小 UDP datapath 与事件循环骨架，对齐 `datapath_xplat.c` 的端口管理与基本收发逻辑。

import std/[asyncdispatch, asyncnet, monotimes, net, times]

import ./common
import ./datapath_model

const
  ## 默认单次接收缓冲长度，兼容 IPv4/IPv6 MTU。
  DefaultRecvBytes = int(MaxMtu)

type
  EventLoopObj = object
    dispatcher*: PDispatcher
    endpoints*: seq[UdpEndpoint]
    closed*: bool

  EventLoop* = ref EventLoopObj

  UdpEndpointObj = object
    loop*: EventLoop
    socket*: AsyncSocket
    handlers*: UdpEventHandlers
    localAddress*: string
    localPort*: Port
    mtu*: uint16
    datapathType*: DatapathType
    recvTask*: Future[void]
    closed*: bool

  UdpEndpoint* = ref UdpEndpointObj

  UdpMessage* = object
    remoteAddress*: string
    remotePort*: Port
    payload*: seq[uint8]

  UdpEventHandlers* = object
    onDatagram*: proc(endpoint: UdpEndpoint, msg: UdpMessage) {.closure.}
    onError*: proc(endpoint: UdpEndpoint, err: ref CatchableError) {.closure.}

  UdpSendRequest* = object
    targetAddress*: string
    targetPort*: Port
    payload*: seq[uint8]
    metadata*: SendDataCommon

proc newEventLoop*(): EventLoop =
  ## 构造事件循环包装，复用 Nim 全局 dispatcher。
  EventLoop(dispatcher: getGlobalDispatcher(), endpoints: @[], closed: false)

proc initSendRequest*(targetAddress: string, targetPort: Port,
                      payload: openArray[uint8],
                      datapathType: DatapathType = dtNormal,
                      ecn: EcnType = ecnNonEct,
                      dscp: DscpType = dscpCs0): UdpSendRequest =
  ## 构建发送请求，同时补齐元数据字段。
  result.targetAddress = targetAddress
  result.targetPort = targetPort
  result.payload = @payload
  let segment =
    if result.payload.len > high(uint16).int: uint16(high(uint16))
    else: uint16(result.payload.len)
  result.metadata = SendDataCommon(
    datapathType: datapathType,
    ecn: ecn,
    dscp: dscp,
    totalSize: uint32(result.payload.len),
    segmentSize: segment
  )

proc toBinaryString(data: openArray[uint8]): string =
  result = newString(data.len)
  for i, b in data:
    result[i] = char(b)

proc toByteSeq(data: string): seq[uint8] =
  result = newSeqOfCap[uint8](data.len)
  for ch in data:
    result.add(uint8(ch))

proc resolveDomain(address: string): Domain =
  if address.contains(':'):
    result = AF_INET6
  else:
    result = AF_INET

proc removeEndpoint(loop: EventLoop, endpoint: UdpEndpoint) =
  if loop.isNil or endpoint.isNil:
    return
  for idx in countdown(loop.endpoints.high, 0):
    if loop.endpoints[idx] == endpoint:
      loop.endpoints.delete(idx)
      break

proc recvLoop(endpoint: UdpEndpoint) {.async.} =
  ## 常驻接收循环，与 MsQuic datapath worker 行为对应。
  while not endpoint.closed:
    try:
      let (data, peerAddr, port) = await endpoint.socket.recvFrom(DefaultRecvBytes)
      if endpoint.handlers.onDatagram != nil:
        endpoint.handlers.onDatagram(endpoint,
          UdpMessage(
            remoteAddress: peerAddr,
            remotePort: port,
            payload: toByteSeq(data)))
    except CatchableError as err:
      if endpoint.handlers.onError != nil:
        endpoint.handlers.onError(endpoint, err)
      else:
        break

proc bindUdp*(loop: EventLoop, localAddress: string, localPort: Port,
              handlers: UdpEventHandlers = UdpEventHandlers(),
              datapathType: DatapathType = dtNormal): UdpEndpoint =
  ## 绑定 UDP 套接字，建立最小 datapath 上下文。
  if loop.isNil or loop.closed:
    raise newException(ValueError, "事件循环未初始化或已关闭")

  let domain = resolveDomain(localAddress)
  let sock = newAsyncSocket(domain = domain, sockType = SOCK_DGRAM,
                            protocol = IPPROTO_UDP, buffered = false)
  sock.setSockOpt(OptReuseAddr, true)
  if localAddress.len == 0:
    sock.bindAddr(localPort)
  else:
    sock.bindAddr(localPort, localAddress)

  let (boundAddr, boundPort) = sock.getLocalAddr()

  new(result)
  result.loop = loop
  result.socket = sock
  result.handlers = handlers
  result.localAddress = boundAddr
  result.localPort = boundPort
  result.mtu = MaxMtu
  result.datapathType = datapathType
  result.closed = false

  result.recvTask = recvLoop(result)
  asyncCheck result.recvTask

  loop.endpoints.add(result)

proc socketCommon*(endpoint: UdpEndpoint): SocketCommon =
  ## 导出 SocketCommon 视图，便于与蓝图结构协作。
  initSocketCommon(
    endpoint.localAddress,
    "",
    endpoint.datapathType,
    stUdp,
    {},
    endpoint.mtu,
    "udp-datapath")

proc sendDatagram*(endpoint: UdpEndpoint,
                   request: UdpSendRequest): Future[void] {.async.} =
  ## 异步发送单个 UDP 数据报，保持元数据一致性。
  if endpoint.isNil or endpoint.closed:
    raise newException(ValueError, "端点未初始化或已关闭")
  if request.payload.len == 0:
    return

  let payloadString = toBinaryString(request.payload)
  await endpoint.socket.sendTo(
    request.targetAddress,
    request.targetPort,
    payloadString)

proc close*(endpoint: UdpEndpoint) =
  ## 关闭端点，释放套接字与循环引用。
  if endpoint.isNil or endpoint.closed:
    return
  endpoint.closed = true

  if not endpoint.socket.isNil and not endpoint.socket.isClosed():
    endpoint.socket.close()

  if not endpoint.loop.isNil:
    endpoint.loop.removeEndpoint(endpoint)

proc poll*(loop: EventLoop, timeoutMs: int = 50): bool =
  ## 运行一次事件循环，timeout 单位为毫秒；返回是否执行了 poll。
  if loop.isNil or loop.closed:
    return false
  try:
    asyncdispatch.poll(timeoutMs)
    result = true
  except ValueError:
    result = false

proc runUntil*(loop: EventLoop, future: FutureBase,
               timeoutMs: int = 1000): bool =
  ## 驱动事件循环直到 Future 完成或超时。返回 true 表示成功完成。
  if loop.isNil or loop.closed:
    return false
  let hasDeadline = timeoutMs >= 0
  var deadline: MonoTime
  if hasDeadline:
    deadline = getMonoTime() + initDuration(milliseconds = timeoutMs)
  while not future.finished:
    var slice = 50
    if hasDeadline:
      let now = getMonoTime()
      if now >= deadline:
        break
      let remaining = int((deadline - now).inMilliseconds)
      if remaining <= 0:
        break
      slice = min(remaining, 50)
      if slice <= 0:
        slice = 1
    discard loop.poll(slice)
  result = future.finished

proc shutdown*(loop: EventLoop) =
  ## 关闭事件循环并释放所有端点。
  if loop.isNil or loop.closed:
    return
  for endpoint in loop.endpoints:
    endpoint.close()
  loop.endpoints.setLen(0)
  loop.closed = true
