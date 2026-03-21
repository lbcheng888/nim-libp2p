when defined(libp2p_run_direct_dm_tests):
  {.used.}

  import chronos
  import std/json
  import std/times
  import stew/byteutils
  import ./helpers
  import ./pubsub/utils
  import ../libp2p/protocols/pubsub/gossipsub
  import ../libp2p/protocols/dm/dmservice
  import ../libp2p/protocols/secure/secure
  when defined(libp2p_msquic_experimental):
    import ../libp2p/transports/msquicdriver as msdriver

  type MessageSink = ref object
    futures: seq[Future[seq[byte]]]
    next: int

  proc newMessageSink(expected: int): MessageSink =
    result = MessageSink(futures: @[], next: 0)
    for _ in 0 ..< expected:
      result.futures.add(newFuture[seq[byte]]())

  proc push(sink: MessageSink, payload: seq[byte]) =
    if sink.isNil or sink.next >= sink.futures.len:
      return
    let current = sink.futures[sink.next]
    if not current.finished:
      current.complete(payload)
    inc sink.next

  proc buildEnvelope(mid, fromPeer, body: string, ackRequested: bool): seq[byte] =
    var envelope = newJObject()
    envelope["op"] = %"dm"
    envelope["mid"] = %mid
    envelope["from"] = %fromPeer
    envelope["body"] = %body
    envelope["timestamp_ms"] = %(int64(getTime().toUnix() * 1000))
    envelope["ackRequested"] = %ackRequested
    ($envelope).toBytes()

  suite "UniMaker style direct messaging":
    teardown:
      checkTrackers()

    asyncTest "pubsub delivers direct dm with ack":
      let nodes = generateNodes(
        2,
        gossip = true,
        triggerSelf = false,
        verifySignature = false,
        sign = false,
        floodPublish = true,
      )
      await startNodes(nodes)
      defer:
        await stopNodes(nodes)

      let gossip = nodes.toGossipSub()
      await gossip[1].switch.connect(
        gossip[0].switch.peerInfo.peerId, gossip[0].switch.peerInfo.addrs
      )

      let dmReceived = newFuture[seq[byte]]("dm-received")

      let dmService0 = newDirectMessageService(
        gossip[0].switch,
        gossip[0].peerInfo.peerId,
        proc(msg: DirectMessage) {.async.} =
          if not dmReceived.finished:
            dmReceived.complete(msg.payload)
      )

      let dmService1 = newDirectMessageService(
        gossip[1].switch,
        gossip[1].peerInfo.peerId,
        proc(msg: DirectMessage) {.async.} =
          discard
      )

      await dmService0.start()
      gossip[0].switch.mount(dmService0)
      await dmService1.start()
      gossip[1].switch.mount(dmService1)

      let mid = "dm-" & $getTime().toUnix()
      let body = "hello-dm"
      var envelope = newJObject()
      envelope["op"] = %"dm"
      envelope["mid"] = %mid
      envelope["from"] = %($gossip[1].peerInfo.peerId)
      envelope["body"] = %body
      envelope["timestamp_ms"] = %(int64(getTime().toUnix() * 1000))
      envelope["ackRequested"] = %true
      let payloadBytes = $envelope

      let delivery = await dmService1.send(
        gossip[0].peerInfo.peerId,
        payloadBytes.toBytes(),
        ackRequested = true,
        messageId = mid,
        timeout = chronos.milliseconds(5000)
      )
      check delivery[0]
      let received = await dmReceived
      let parsed = parseJson(string.fromBytes(received))
      check parsed["body"].getStr() == body

    asyncTest "pubsub delivers bidirectional dm with tls preferred secure managers":
      let nodes = generateNodes(
        2,
        secureManagers = [SecureProtocol.Tls, SecureProtocol.Noise],
        gossip = true,
        triggerSelf = false,
        verifySignature = false,
        sign = false,
        floodPublish = true,
      )
      await startNodes(nodes)
      defer:
        await stopNodes(nodes)

      let gossip = nodes.toGossipSub()
      let listener = gossip[0]
      let dialer = gossip[1]

      await dialer.switch.connect(
        listener.switch.peerInfo.peerId, listener.switch.peerInfo.addrs
      )

      checkUntilTimeoutCustom(chronos.seconds(5), chronos.milliseconds(50)):
        dialer.switch.connManager.connCount(listener.switch.peerInfo.peerId) == 1
        listener.switch.connManager.connCount(dialer.switch.peerInfo.peerId) == 1

      let listenerMsgs = newMessageSink(2)
      let dialerMsgs = newMessageSink(2)

      let dmService0 = newDirectMessageService(
        listener.switch,
        listener.peerInfo.peerId,
        proc(msg: DirectMessage) {.async.} =
          listenerMsgs.push(msg.payload)
      )

      let dmService1 = newDirectMessageService(
        dialer.switch,
        dialer.peerInfo.peerId,
        proc(msg: DirectMessage) {.async.} =
          dialerMsgs.push(msg.payload)
      )

      await dmService0.start()
      listener.switch.mount(dmService0)
      await dmService1.start()
      dialer.switch.mount(dmService1)

      let body1 = "dialer->listener ack"
      let mid1 = "dm-bidir-1-" & $getTime().toUnix()
      let send1 = await dmService1.send(
        listener.peerInfo.peerId,
        buildEnvelope(mid1, $dialer.peerInfo.peerId, body1, true),
        ackRequested = true,
        messageId = mid1,
        timeout = chronos.seconds(5)
      )
      check send1[0]
      let parsed1 = parseJson(string.fromBytes(await listenerMsgs.futures[0]))
      check parsed1["body"].getStr() == body1

      let body2 = "listener->dialer no-ack"
      let mid2 = "dm-bidir-2-" & $getTime().toUnix()
      let send2 = await dmService0.send(
        dialer.peerInfo.peerId,
        buildEnvelope(mid2, $listener.peerInfo.peerId, body2, false),
        ackRequested = false,
        messageId = mid2,
        timeout = chronos.seconds(5)
      )
      check send2[0]
      let parsed2 = parseJson(string.fromBytes(await dialerMsgs.futures[0]))
      check parsed2["body"].getStr() == body2

      let body3 = "listener->dialer ack"
      let mid3 = "dm-bidir-3-" & $getTime().toUnix()
      let send3 = await dmService0.send(
        dialer.peerInfo.peerId,
        buildEnvelope(mid3, $listener.peerInfo.peerId, body3, true),
        ackRequested = true,
        messageId = mid3,
        timeout = chronos.seconds(5)
      )
      check send3[0]
      let parsed3 = parseJson(string.fromBytes(await dialerMsgs.futures[1]))
      check parsed3["body"].getStr() == body3

      let body4 = "dialer->listener no-ack"
      let mid4 = "dm-bidir-4-" & $getTime().toUnix()
      let send4 = await dmService1.send(
        listener.peerInfo.peerId,
        buildEnvelope(mid4, $dialer.peerInfo.peerId, body4, false),
        ackRequested = false,
        messageId = mid4,
        timeout = chronos.seconds(5)
      )
      check send4[0]
      let parsed4 = parseJson(string.fromBytes(await listenerMsgs.futures[1]))
      check parsed4["body"].getStr() == body4

      check dialer.switch.connManager.connCount(listener.switch.peerInfo.peerId) == 1
      check listener.switch.connManager.connCount(dialer.switch.peerInfo.peerId) == 1
else:
  import ./helpers
  suite "UniMaker style direct messaging":
    test "direct dm tests disabled":
      skip()

when defined(libp2p_run_direct_dm_tests) and defined(libp2p_msquic_experimental):
  {.used.}

  import chronos
  import std/json
  import std/times
  import stew/byteutils
  import ./helpers
  import ./pubsub/utils
  import ../libp2p/protocols/pubsub/gossipsub
  import ../libp2p/protocols/dm/dmservice
  import ../libp2p/transports/msquicdriver as msdriver

  suite "UniMaker style direct messaging (MsQuic)":
    teardown:
      checkTrackers()

    asyncTest "pubsub delivers direct dm with MsQuic transport":
      let (handle, initErr) = initMsQuicTransportForAsync()
      if initErr.len > 0 or handle.isNil:
        echo "MsQuic runtime unavailable: ", initErr
        skip()
        return
      shutdownMsQuicTransportForAsync(handle)

      let nodes = generateNodes(
        2,
        gossip = true,
        triggerSelf = false,
        verifySignature = false,
        sign = false,
        floodPublish = true,
        transport = TransportType.QUIC,
        transportAddrs = @["/ip4/127.0.0.1/udp/0/quic-v1"]
      )
      await startNodes(nodes)
      defer:
        await stopNodes(nodes)

      let gossip = nodes.toGossipSub()
      await gossip[1].switch.connect(
        gossip[0].switch.peerInfo.peerId, gossip[0].switch.peerInfo.addrs
      )

      let dmReceived = newFuture[seq[byte]]("dm-received")

      let dmService0 = newDirectMessageService(
        gossip[0].switch,
        gossip[0].peerInfo.peerId,
        proc(msg: DirectMessage) {.async.} =
          if not dmReceived.finished:
            dmReceived.complete(msg.payload)
      )

      let dmService1 = newDirectMessageService(
        gossip[1].switch,
        gossip[1].peerInfo.peerId,
        proc(msg: DirectMessage) {.async.} =
          discard
      )

      await dmService0.start()
      gossip[0].switch.mount(dmService0)
      await dmService1.start()
      gossip[1].switch.mount(dmService1)

      let mid = "dm-msquic-" & $getTime().toUnix()
      let body = "hello-dm-msquic"
      var envelope = newJObject()
      envelope["op"] = %"dm"
      envelope["mid"] = %mid
      envelope["from"] = %($gossip[1].peerInfo.peerId)
      envelope["body"] = %body
      envelope["timestamp_ms"] = %(int64(getTime().toUnix() * 1000))
      envelope["ackRequested"] = %true
      let payloadBytes = $envelope

      let delivery = await dmService1.send(
        gossip[0].peerInfo.peerId,
        payloadBytes.toBytes(),
        ackRequested = true,
        messageId = mid,
        timeout = chronos.milliseconds(5000)
      )
      check delivery[0]
      let received = await dmReceived
      let parsed = parseJson(string.fromBytes(received))
      check parsed["body"].getStr() == body
