import std/times
import chronos
import stew/byteutils
import libp2p/[builders, multiaddress, switch]
import libp2p/protocols/pubsub/gossipsub
import libp2p/protocols/dm/dmservice

proc newNode(listenAddr: string): GossipSub =
  let listenMa = MultiAddress.init(listenAddr).tryGet()
  let switch = newStandardSwitchBuilder(addrs = listenMa, transport = TransportType.QUIC)
    .withSignedPeerRecord(false)
    .withMaxIn(16)
    .withMaxOut(16)
    .build()

  let gossip = GossipSub.init(
    switch = switch,
    triggerSelf = false,
    verifySignature = false,
    sign = false,
    parameters = GossipSubParams.init(floodPublish = true)
  )
  switch.mount(gossip)
  gossip

proc runDemo() {.async.} =
  let listener = newNode("/ip4/0.0.0.0/udp/0/quic-v1")
  let dialer = newNode("/ip4/0.0.0.0/udp/0/quic-v1")

  await listener.switch.start()
  await dialer.switch.start()
  defer:
    await dialer.switch.stop()
    await listener.switch.stop()

  echo "[mobile-dm] listener peer: ", $listener.switch.peerInfo.peerId
  echo "[mobile-dm] dialer peer: ", $dialer.switch.peerInfo.peerId

  await dialer.switch.connect(
    listener.switch.peerInfo.peerId,
    listener.switch.peerInfo.addrs
  )
  echo "[mobile-dm] connected"

  let dmFuture = newFuture[seq[byte]]("mobile-dm")

  let listenerSvc = newDirectMessageService(
    listener.switch,
    listener.peerInfo.peerId,
    proc(msg: DirectMessage) {.async.} =
      let text = string.fromBytes(msg.payload)
      echo "[mobile-dm] listener received id=", msg.id, " payload=", text
      if not dmFuture.finished:
        dmFuture.complete(msg.payload)
  )

  let dialerSvc = newDirectMessageService(
    dialer.switch,
    dialer.peerInfo.peerId,
    proc(msg: DirectMessage) {.async.} =
      echo "[mobile-dm] dialer handler invoked (unexpected)"
  )

  await listenerSvc.start()
  listener.switch.mount(listenerSvc)
  await dialerSvc.start()
  dialer.switch.mount(dialerSvc)

  let payload = "mobile-dm-" & $getTime().toUnix()
  let bytes = payload.toBytes()
  let mid = "dm-" & $now().toUnix()
  let result = await dialerSvc.send(
    listener.peerInfo.peerId,
    bytes,
    ackRequested = true,
    messageId = mid,
    timeout = chronos.milliseconds(5000)
  )
  echo "[mobile-dm] send delivered=", result[0], " err=", result[1]
  if not result[0]:
    return

  if await dmFuture.withTimeout(chronos.seconds(5)):
    let received = dmFuture.read()
    let text = string.fromBytes(received)
    echo "[mobile-dm] payload echoed: ", text
  else:
    echo "[mobile-dm] ACK timed out"

when isMainModule:
  waitFor runDemo()
