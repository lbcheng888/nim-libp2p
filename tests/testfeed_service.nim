when defined(libp2p_run_feed_tests):
  {.used.}

  import chronos
  import stew/byteutils
  import ./helpers
  import ./pubsub/utils
  import ../libp2p/protocols/pubsub/gossipsub
  import ../libp2p/protocols/feed/feedservice
  when defined(libp2p_msquic_experimental):
    import ../libp2p/transports/msquicdriver as msdriver

  suite "Feed service":
    teardown:
      checkTrackers()

    asyncTest "feed entry delivered across peers":
      const TestTimeout = 5.seconds
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
      await connectNodes(gossip[1], gossip[0])

      let feedReceived = newFuture[FeedItem]("feed-received")
      var fetchCount = 0
      let feedTopic = toTopic("/content-feed", gossip[1].peerInfo.peerId)

      let service0 = newFeedService(
        gossip[0],
        gossip[0].peerInfo.peerId,
        proc(item: FeedItem) {.async: (raises: []), gcsafe.} =
          if not feedReceived.finished:
            feedReceived.complete(item)
      )
      let service1 = newFeedService(
        gossip[1],
        gossip[1].peerInfo.peerId,
        proc(item: FeedItem) {.async: (raises: []), gcsafe.} = discard
      )
      service0.setContentFetcher(
        proc(entry: FeedEntry) {.async: (raises: []), gcsafe.} =
          inc fetchCount
      )

      await service0.subscribeToPeer(gossip[1].peerInfo.peerId)
      await waitSub(gossip[1], gossip[0], feedTopic)

      var entry = FeedEntry(
        id: "item-1",
        mediaType: "image/jpeg",
        summary: "Sunset",
        cover: "cover".toBytes(),
      )
      check (await service1.publishFeedItem(entry)) > 0
      check (await service1.publishFeedItem(entry)) > 0 # duplicate should be ignored

      check await feedReceived.withTimeout(TestTimeout)
      let received = feedReceived.read()
      check received.entry.id == "item-1"
      check received.entry.mediaType == "image/jpeg"
      check received.entry.summary == "Sunset"
      check received.entry.cover == "cover".toBytes()
      check fetchCount == 1
else:
  import ./helpers
  suite "Feed service":
    test "feed tests disabled":
      skip()

when defined(libp2p_run_feed_tests) and defined(libp2p_msquic_experimental):
  {.used.}

  import chronos
  import stew/byteutils
  import ./helpers
  import ./pubsub/utils
  import ../libp2p/protocols/pubsub/gossipsub
  import ../libp2p/protocols/feed/feedservice
  import ../libp2p/transports/msquicdriver as msdriver

  suite "Feed service (MsQuic)":
    teardown:
      checkTrackers()

    asyncTest "feed entry delivered across peers (MsQuic transport)":
      const TestTimeout = 5.seconds
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
      await connectNodes(gossip[0], gossip[1])
      await connectNodes(gossip[1], gossip[0])

      let feedReceived = newFuture[FeedItem]("feed-received")
      var fetchCount = 0

      let service0 = newFeedService(
        gossip[0],
        gossip[0].peerInfo.peerId,
        proc(item: FeedItem) {.async: (raises: []), gcsafe.} =
          if not feedReceived.finished:
            feedReceived.complete(item)
      )
      let service1 = newFeedService(
        gossip[1],
        gossip[1].peerInfo.peerId,
        proc(item: FeedItem) {.async: (raises: []), gcsafe.} = discard
      )
      service0.setContentFetcher(
        proc(entry: FeedEntry) {.async: (raises: []), gcsafe.} =
          inc fetchCount
      )

      await service0.subscribeToPeer(gossip[1].peerInfo.peerId)
      await waitSub(gossip[1], gossip[0], toTopic("/content-feed", gossip[1].peerInfo.peerId))

      var entry = FeedEntry(
        id: "item-msquic-1",
        mediaType: "image/jpeg",
        summary: "Sunset MsQuic",
        cover: "cover-msquic".toBytes(),
      )
      check (await service1.publishFeedItem(entry)) > 0
      check (await service1.publishFeedItem(entry)) > 0

      check await feedReceived.withTimeout(TestTimeout)
      let received = feedReceived.read()
      check received.entry.id == "item-msquic-1"
      check received.entry.mediaType == "image/jpeg"
      check received.entry.summary == "Sunset MsQuic"
      check received.entry.cover == "cover-msquic".toBytes()
      check fetchCount == 1
