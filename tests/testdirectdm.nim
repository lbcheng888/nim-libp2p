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
  import ../libp2p/peerstore
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

  proc expectLiveReaderOnlySession(
      svc: DirectMessageService,
      peer: PeerId
  ) =
    let snapshots = svc.peerSessionControlSnapshots(peer)
    check snapshots.len == 1
    if snapshots.len != 1:
      return
    let snapshot = snapshots[0]
    check snapshot.lifecycle == DmSessionLive
    check snapshot.activityDepths[DmActivityReaderAttached] == 1
    check snapshot.activityDepths[DmActivityHandlingPayload] == 0
    check snapshot.activityDepths[DmActivityWritingFrame] == 0
    check snapshot.activityDepths[DmActivityAwaitingAck] == 0

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
      discard gossip[0].switch.peerStore[AddressBook].del(gossip[1].peerInfo.peerId)

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
      check dmService0.activeSessionCountForPeer(dialer.peerInfo.peerId) == 1
      check dmService1.activeSessionCountForPeer(listener.peerInfo.peerId) == 1
      expectLiveReaderOnlySession(dmService0, dialer.peerInfo.peerId)
      expectLiveReaderOnlySession(dmService1, listener.peerInfo.peerId)
      discard listener.switch.peerStore[AddressBook].del(dialer.peerInfo.peerId)
      discard dialer.switch.peerStore[AddressBook].del(listener.peerInfo.peerId)

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
      check dmService0.activeSessionCountForPeer(dialer.peerInfo.peerId) == 1
      check dmService1.activeSessionCountForPeer(listener.peerInfo.peerId) == 1
      expectLiveReaderOnlySession(dmService0, dialer.peerInfo.peerId)
      expectLiveReaderOnlySession(dmService1, listener.peerInfo.peerId)

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
      check dmService0.activeSessionCountForPeer(dialer.peerInfo.peerId) == 1
      check dmService1.activeSessionCountForPeer(listener.peerInfo.peerId) == 1
      expectLiveReaderOnlySession(dmService0, dialer.peerInfo.peerId)
      expectLiveReaderOnlySession(dmService1, listener.peerInfo.peerId)

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
      check dmService0.activeSessionCountForPeer(dialer.peerInfo.peerId) == 1
      check dmService1.activeSessionCountForPeer(listener.peerInfo.peerId) == 1
      expectLiveReaderOnlySession(dmService0, dialer.peerInfo.peerId)
      expectLiveReaderOnlySession(dmService1, listener.peerInfo.peerId)

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
  import ../libp2p/peerstore
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

      let dmReceived0 = newFuture[seq[byte]]("dm-received-0")
      let dmReceived1 = newFuture[seq[byte]]("dm-received-1")

      let dmService0 = newDirectMessageService(
        gossip[0].switch,
        gossip[0].peerInfo.peerId,
        proc(msg: DirectMessage) {.async.} =
          if not dmReceived0.finished:
            dmReceived0.complete(msg.payload)
      )

      let dmService1 = newDirectMessageService(
        gossip[1].switch,
        gossip[1].peerInfo.peerId,
        proc(msg: DirectMessage) {.async.} =
          if not dmReceived1.finished:
            dmReceived1.complete(msg.payload)
      )

      await dmService0.start()
      gossip[0].switch.mount(dmService0)
      await dmService1.start()
      gossip[1].switch.mount(dmService1)
      discard gossip[0].switch.peerStore[AddressBook].del(gossip[1].peerInfo.peerId)

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
      let received0 = await dmReceived0
      let parsed0 = parseJson(string.fromBytes(received0))
      check parsed0["body"].getStr() == body
      check dmService0.activeSessionCountForPeer(gossip[1].peerInfo.peerId) == 1
      check dmService1.activeSessionCountForPeer(gossip[0].peerInfo.peerId) == 1
      discard gossip[0].switch.peerStore[AddressBook].del(gossip[1].peerInfo.peerId)
      discard gossip[1].switch.peerStore[AddressBook].del(gossip[0].peerInfo.peerId)

      let reverseMid = "dm-msquic-reverse-" & $getTime().toUnix()
      let reverseBody = "reverse-msquic"
      let reverseDelivery = await dmService0.send(
        gossip[1].peerInfo.peerId,
        buildEnvelope(reverseMid, $gossip[0].peerInfo.peerId, reverseBody, true),
        ackRequested = true,
        messageId = reverseMid,
        timeout = chronos.milliseconds(5000)
      )
      check reverseDelivery[0]
      let received1 = await dmReceived1
      let parsed1 = parseJson(string.fromBytes(received1))
      check parsed1["body"].getStr() == reverseBody
      check dmService0.activeSessionCountForPeer(gossip[1].peerInfo.peerId) == 1
      check dmService1.activeSessionCountForPeer(gossip[0].peerInfo.peerId) == 1

    asyncTest "warm session reuses verified muxer without reopening dm select":
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
      let listener = gossip[0]
      let dialer = gossip[1]

      await dialer.switch.connect(
        listener.switch.peerInfo.peerId,
        listener.switch.peerInfo.addrs,
      )

      let dmReceived = newFuture[seq[byte]]("dm-msquic-warm-received")
      let listenerSvc = newDirectMessageService(
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

      await listenerSvc.start()
      listener.switch.mount(listenerSvc)
      await dialerSvc.start()
      dialer.switch.mount(dialerSvc)

      let verifiedMuxer = dialer.switch.connManager.selectMuxer(listener.peerInfo.peerId)
      check not verifiedMuxer.isNil
      var warmStages: seq[string] = @[]
      let warm = await dialerSvc.warmSession(
        listener.peerInfo.peerId,
        timeout = chronos.seconds(5),
        preferredAddrs = listener.peerInfo.addrs,
        verifiedMuxer = verifiedMuxer,
        reuseVerifiedLiveConnection = true,
        progress = proc(stage: string, detail: JsonNode) =
          discard detail
          warmStages.add(stage)
      )
      check warm[0]
      check "dm_session_warm_ready" in warmStages
      expectLiveReaderOnlySession(dialerSvc, listener.peerInfo.peerId)

      var sendStages: seq[string] = @[]
      let mid = "dm-msquic-warm-" & $getTime().toUnix()
      let body = "warm-reuse"
      let delivery = await dialerSvc.send(
        listener.peerInfo.peerId,
        buildEnvelope(mid, $dialer.peerInfo.peerId, body, true),
        ackRequested = true,
        messageId = mid,
        timeout = chronos.seconds(5),
        preferredAddrs = listener.peerInfo.addrs,
        verifiedMuxer = verifiedMuxer,
        reuseVerifiedLiveConnection = true,
        progress = proc(stage: string, detail: JsonNode) =
          discard detail
          sendStages.add(stage)
      )
      check delivery[0]
      let parsed = parseJson(string.fromBytes(await dmReceived))
      check parsed["body"].getStr() == body
      check "dm_session_reuse" in sendStages
      check "dm_reader_attach" notin sendStages
      check "dm_conn_live_select_begin" notin sendStages
