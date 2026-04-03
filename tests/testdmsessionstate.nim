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

  suite "Direct DM session state machine":
    teardown:
      checkTrackers()

    asyncTest "acked exchange settles to live reader-only session":
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
      let listener = gossip[0]
      let dialer = gossip[1]

      await dialer.switch.connect(
        listener.switch.peerInfo.peerId,
        listener.switch.peerInfo.addrs,
      )

      let received = newFuture[seq[byte]]("dm-session-state-received")

      let listenerSvc = newDirectMessageService(
        listener.switch,
        listener.peerInfo.peerId,
        proc(msg: DirectMessage) {.async.} =
          if not received.finished:
            received.complete(msg.payload)
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

      let mid = "dm-session-state-" & $getTime().toUnix()
      let body = "state-machine"
      let sent = await dialerSvc.send(
        listener.peerInfo.peerId,
        buildEnvelope(mid, $dialer.peerInfo.peerId, body, true),
        ackRequested = true,
        messageId = mid,
        timeout = chronos.seconds(5),
      )
      check sent[0]

      let parsed = parseJson(string.fromBytes(await received))
      check parsed["body"].getStr() == body

      expectLiveReaderOnlySession(listenerSvc, dialer.peerInfo.peerId)
      expectLiveReaderOnlySession(dialerSvc, listener.peerInfo.peerId)
else:
  import ./helpers

  suite "Direct DM session state machine":
    test "direct dm session state tests disabled":
      skip()
