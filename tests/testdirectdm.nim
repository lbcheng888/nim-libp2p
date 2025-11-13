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
  when defined(libp2p_msquic_experimental):
    import ../libp2p/transports/msquicdriver as msdriver

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
      let (handle, initErr) = msdriver.initMsQuicTransport()
      if initErr.len > 0 or handle.isNil:
        echo "MsQuic runtime unavailable: ", initErr
        skip()
        return
      handle.shutdown()

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
