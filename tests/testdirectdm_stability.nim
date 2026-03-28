{.used.}

import chronos
import std/[json, times]
import stew/byteutils
import ./helpers
import ../libp2p/builders
import ../libp2p/multiaddress
import ../libp2p/protocols/dm/dmservice
import ../libp2p/switch

proc buildEnvelope(mid, fromPeer, body: string, ackRequested: bool): seq[byte] =
  var envelope = newJObject()
  envelope["op"] = %"dm"
  envelope["mid"] = %mid
  envelope["from"] = %fromPeer
  envelope["body"] = %body
  envelope["timestamp_ms"] = %(int64(getTime().toUnix() * 1000))
  envelope["ackRequested"] = %ackRequested
  ($envelope).toBytes()

suite "Direct DM stability":
  teardown:
    checkTrackers()

  test "direct dm keeps tsnet tcp ahead of tsnet quic":
    let tcpAddr = MultiAddress.init("/ip4/100.64.0.1/tcp/4001/tsnet").tryGet()
    let quicAddr = MultiAddress.init("/ip4/100.64.0.1/udp/4001/quic-v1/tsnet").tryGet()
    let ordered = orderDirectDialAddrs(@[quicAddr, tcpAddr])
    check ordered.len == 2
    check $ordered[0] == $tcpAddr
    check $ordered[1] == $quicAddr

  asyncTest "direct dm redials from known addresses after disconnect":
    let listener = newStandardSwitch(transport = TransportType.Memory)
    let dialer = newStandardSwitch(transport = TransportType.Memory)
    await listener.start()
    await dialer.start()
    defer:
      await dialer.stop()
      await listener.stop()

    await dialer.connect(
      listener.peerInfo.peerId, listener.peerInfo.addrs
    )

    checkUntilTimeoutCustom(chronos.seconds(5), chronos.milliseconds(50)):
      dialer.connManager.connCount(listener.peerInfo.peerId) == 1
      listener.connManager.connCount(dialer.peerInfo.peerId) == 1
    await sleepAsync(chronos.milliseconds(500))

    let dmReceived = newFuture[seq[byte]]("dm-redial-received")

    let dmService0 = newDirectMessageService(
      listener,
      listener.peerInfo.peerId,
      proc(msg: DirectMessage) {.async.} =
        if not dmReceived.finished:
          dmReceived.complete(msg.payload)
    )

    let dmService1 = newDirectMessageService(
      dialer,
      dialer.peerInfo.peerId,
      proc(msg: DirectMessage) {.async.} =
        discard
    )

    await dmService0.start()
    listener.mount(dmService0)
    await dmService1.start()
    dialer.mount(dmService1)

    await dialer.disconnect(listener.peerInfo.peerId)
    checkUntilTimeoutCustom(chronos.seconds(5), chronos.milliseconds(50)):
      dialer.connManager.connCount(listener.peerInfo.peerId) == 0

    let mid = "dm-redial-" & $getTime().toUnix()
    let body = "hello-dm-redial"
    let delivery = await dmService1.send(
      listener.peerInfo.peerId,
      buildEnvelope(mid, $dialer.peerInfo.peerId, body, true),
      ackRequested = true,
      messageId = mid,
      timeout = chronos.seconds(5)
    )
    check delivery[0]

    let parsed = parseJson(string.fromBytes(await dmReceived))
    check parsed["body"].getStr() == body

    checkUntilTimeoutCustom(chronos.seconds(5), chronos.milliseconds(50)):
      dialer.connManager.connCount(listener.peerInfo.peerId) == 1
    await sleepAsync(chronos.milliseconds(500))
