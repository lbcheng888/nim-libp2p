## Android 平台 direct messaging 自测入口。
## 构建两个 GossipSub 节点，建立直连并通过 DirectMessage 发送确认。

import std/[json, os, strutils]
from std/times import getTime, toUnix
import chronos
import stew/byteutils

import libp2p/[builders, multiaddress, peerid, switch]
import libp2p/protocols/pubsub/gossipsub
import libp2p/protocols/dm/dmservice

proc parseDuration(envName: string, defaultMillis: int): Duration =
  ## 从环境变量读取毫秒级超时时间。
  let raw = getEnv(envName)
  if raw.len == 0:
    return defaultMillis.milliseconds
  try:
    return parseInt(raw).milliseconds
  except ValueError:
    return defaultMillis.milliseconds

proc initGossipNode(): GossipSub =
  ## 初始化 GossipSub 节点并挂载到 Switch。
  let (listenAddr, transportType) =
    when defined(libp2p_quic_support):
      ("/ip4/0.0.0.0/udp/0/quic-v1", TransportType.QUIC)
    else:
      ("/ip4/0.0.0.0/tcp/0", TransportType.TCP)
  echo "[direct-dm] init node transport=", $transportType, " listen=", listenAddr

  let addrRes = MultiAddress.init(listenAddr)
  if addrRes.isErr:
    raise newException(ValueError, "invalid listen multiaddr: " & addrRes.error)
  let builder = newStandardSwitchBuilder(
    addrs = addrRes.get(),
    transport = transportType,
    sendSignedPeerRecord = false
  )
  let sw = builder.build()
  let gossip = GossipSub.init(
    switch = sw,
    triggerSelf = false,
    verifySignature = false,
    sign = false,
    parameters = GossipSubParams.init(floodPublish = true)
  )
  sw.mount(gossip)
  return gossip

proc awaitAck(fut: Future[seq[byte]], timeout: Duration): Future[seq[byte]] {.async.} =
  ## 等待 ACK，支持超时。
  if timeout <= 0.milliseconds:
    return await fut
  if await fut.withTimeout(timeout):
    return fut.read()
  raise newException(AsyncTimeoutError, "direct dm ack timed out")

proc parseMultiaddrs(raw: string): seq[MultiAddress] =
  ## 解析以逗号/空白分隔的 multiaddr 列表。
  for token in raw.split({'\n', '\t', ' ', ','}):
    let normalized = token.strip()
    if normalized.len == 0:
      continue
    let addrRes = MultiAddress.init(normalized)
    if addrRes.isErr:
      raise newException(ValueError, "invalid remote multiaddr: " & addrRes.error)
    result.add(addrRes.get())

proc runDirectMessage(payload: string, ackTimeout: Duration, idleDelay: Duration) {.
    async: (raises: [CatchableError, Exception]).} =
  ## 建立节点互联（本地双节点或远程节点），发送 DM 并等待 ACK。
  let remotePeerRaw = getEnv("DM_REMOTE_PEER").strip()
  let remoteAddrsRaw = getEnv("DM_REMOTE_ADDRS").strip()
  let useRemote = remotePeerRaw.len > 0 and remoteAddrsRaw.len > 0

  var listener: GossipSub = nil
  if not useRemote:
    listener = initGossipNode()
  if not useRemote and listener != nil:
    let listenerStart = listener.switch.start()
    if not listenerStart.isNil:
      await listenerStart
    defer:
      try:
        let stopFuture = listener.switch.stop()
        if not stopFuture.isNil:
          await stopFuture
      except CatchableError:
        discard

  let dialer = initGossipNode()

  let dialerStartFuture = dialer.switch.start()
  if not dialerStartFuture.isNil:
    await dialerStartFuture
  defer:
    try:
      let stopFuture = dialer.switch.stop()
      if not stopFuture.isNil:
        await stopFuture
    except CatchableError:
      discard

  if useRemote:
    echo "[direct-dm] remote peer target: ", remotePeerRaw
  elif listener != nil:
    echo "[direct-dm] listener peer: ", $listener.switch.peerInfo.peerId
  echo "[direct-dm] dialer peer: ", $dialer.switch.peerInfo.peerId

  var remotePeerId: PeerId
  var remoteAddrs: seq[MultiAddress]
  if useRemote:
    try:
      remotePeerId = PeerId.init(remotePeerRaw).tryGet()
    except CatchableError as exc:
      raise newException(ValueError, "invalid remote peer id: " & exc.msg)
    remoteAddrs = parseMultiaddrs(remoteAddrsRaw)
    echo "[direct-dm] staging remote peer addrs=", remoteAddrs.len
    if not dialer.switch.peerStore.isNil:
      let addrBook = dialer.switch.peerStore[AddressBook]
      addrBook[remotePeerId] = remoteAddrs
  elif listener != nil:
    echo "[direct-dm] listener addrs: ", listener.switch.peerInfo.addrs.len
  else:
    echo "[direct-dm] dialer has no listener (remote mode unavailable)"

  let dmReceived = newFuture[seq[byte]]("direct-dm-received")

  var listenerSvc: DirectMessageService = nil
  if not useRemote and listener != nil:
    listenerSvc = newDirectMessageService(
      listener.switch,
      listener.peerInfo.peerId,
      proc(msg: DirectMessage) {.async.} =
        if not dmReceived.finished:
          dmReceived.complete(msg.payload)
    )

  let dialerSvc = newDirectMessageService(
    dialer.switch,
    dialer.peerInfo.peerId,
    proc(msg: DirectMessage) {.async.} =
      discard
  )

  if not useRemote and listenerSvc != nil:
    let startFuture = listenerSvc.start()
    if not startFuture.isNil:
      await startFuture
    listener.switch.mount(listenerSvc)
  let dialerStart = dialerSvc.start()
  if not dialerStart.isNil:
    await dialerStart
  dialer.switch.mount(dialerSvc)
  echo "[direct-dm] services mounted"

  if not useRemote and idleDelay > 0.milliseconds:
    echo "[direct-dm] waiting ", idleDelay, " for topic propagation"
    await sleepAsync(idleDelay)

  let mid = "dm-" & $getTime().toUnix()
  var envelope = newJObject()
  envelope["op"] = %"dm"
  envelope["mid"] = %mid
  envelope["from"] = %($dialer.peerInfo.peerId)
  envelope["body"] = %payload
  envelope["timestamp_ms"] = %(int64(getTime().toUnix() * 1000))
  envelope["ackRequested"] = %true
  let messageBytes = $envelope

  let delivery =
    if useRemote:
      await dialerSvc.send(
        remotePeerId,
        messageBytes.toBytes(),
        ackRequested = true,
        messageId = mid,
        timeout = ackTimeout
      )
    else:
      await dialerSvc.send(
        listener.peerInfo.peerId,
        messageBytes.toBytes(),
        ackRequested = true,
        messageId = mid,
        timeout = ackTimeout
      )
  if not delivery[0]:
    raise newException(ValueError, "direct dm send failed: " & delivery[1])

  if useRemote:
    echo "[direct-dm] remote ack succeeded"
  else:
    echo "[direct-dm] awaiting local ack future"
    let received = await dmReceived.awaitAck(ackTimeout)
    let text = string.fromBytes(received)
    try:
      let parsed = parseJson(text)
      let body = parsed["body"].getStr()
      if body != payload:
        raise newException(ValueError, "dm payload mismatch")
    except CatchableError:
      raise newException(ValueError, "dm payload parsing failed")
    echo "[direct-dm] payload delivered: ", payload

when isMainModule:
  let payload = getEnv("DM_PAYLOAD", "hello-direct-dm")
  let ackTimeout = parseDuration("DM_ACK_TIMEOUT_MS", 5000)
  let idleDelay = parseDuration("DM_IDLE_DELAY_MS", 1500)

  try:
    waitFor runDirectMessage(payload, ackTimeout, idleDelay)
  except CatchableError as exc:
    quit("[direct-dm] failed: " & exc.msg, QuitFailure)
