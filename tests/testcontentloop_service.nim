{.used.}

import chronos
import unittest2

import ./helpers
import ./pubsub/utils
import ../libp2p/peerid
import ../libp2p/protocols/pubsub/gossipsub
import ../libp2p/protocols/feed/feedservice
import ../libp2p/services/contentloopservice

suite "Content loop service":
  teardown:
    checkTrackers()

  asyncTest "publishes content, fetches payload, and delivers receipt":
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
    await connectNodes(gossip[0], gossip[1])
    await connectNodes(gossip[1], gossip[0])

    let contentReceived = newFuture[ContentLoopItem]("content-loop-item")
    let receiptReceived = newFuture[ContentReceipt]("content-loop-receipt")

    let consumer = newContentLoopService(
      gossip[0].switch,
      gossip[0],
      gossip[0].peerInfo.peerId,
      handler = proc(item: ContentLoopItem) {.async, gcsafe.} =
        if not contentReceived.finished:
          contentReceived.complete(item)
    )
    let publisher = newContentLoopService(
      gossip[1].switch,
      gossip[1],
      gossip[1].peerInfo.peerId,
      receiptHandler = proc(receipt: ContentReceipt) {.async, gcsafe.} =
        if not receiptReceived.finished:
          receiptReceived.complete(receipt)
    )

    await consumer.start()
    await publisher.start()
    await consumer.subscribeToPeer(gossip[1].peerInfo.peerId)
    await waitSub(gossip[1], gossip[0], toTopic("/content-feed", gossip[1].peerInfo.peerId))

    let payload = @[byte 1, 2, 3, 4, 5]
    check (await publisher.publishContent(
      ContentAnnouncement(
        id: "doc-1",
        mediaType: "application/markdown",
        summary: "document",
        contentKey: "cid://doc-1",
        receiptRequested: true,
      ),
      payload,
    )) > 0

    check await contentReceived.withTimeout(TestTimeout)
    let receivedItem = contentReceived.read()
    check receivedItem.announcement.id == "doc-1"
    check receivedItem.announcement.contentKey == "cid://doc-1"
    check receivedItem.payload == payload
    check receivedItem.fetchError.len == 0
    check receivedItem.fetchedFrom.isSome()
    check receivedItem.fetchedFrom.get() == gossip[1].peerInfo.peerId
    check consumer.hasContent("cid://doc-1")

    check await receiptReceived.withTimeout(TestTimeout)
    let receipt = receiptReceived.read()
    check receipt.id == "doc-1"
    check receipt.contentKey == "cid://doc-1"
    check receipt.status == crFetched
    check receipt.consumer == gossip[0].peerInfo.peerId
    check receipt.producer == gossip[1].peerInfo.peerId

  asyncTest "follows provider and bootstrap hints for fetch and next feed":
    const TestTimeout = 5.seconds
    let nodes = generateNodes(
      3,
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
    await connectNodesStar(gossip)

    let firstItem = newFuture[ContentLoopItem]("content-loop-first")
    let secondItem = newFuture[ContentLoopItem]("content-loop-second")
    var deliveryCount = 0

    let consumer = newContentLoopService(
      gossip[0].switch,
      gossip[0],
      gossip[0].peerInfo.peerId,
      handler = proc(item: ContentLoopItem) {.async, gcsafe.} =
        inc deliveryCount
        if deliveryCount == 1 and not firstItem.finished:
          firstItem.complete(item)
        elif deliveryCount == 2 and not secondItem.finished:
          secondItem.complete(item)
    )
    let announcer = newContentLoopService(
      gossip[1].switch,
      gossip[1],
      gossip[1].peerInfo.peerId,
    )
    let provider = newContentLoopService(
      gossip[2].switch,
      gossip[2],
      gossip[2].peerInfo.peerId,
    )

    await consumer.start()
    await announcer.start()
    await provider.start()

    await consumer.subscribeToPeer(gossip[1].peerInfo.peerId)
    await waitSub(gossip[1], gossip[0], toTopic("/content-feed", gossip[1].peerInfo.peerId))

    provider.storeContent("cid://shared-doc", @[byte 9, 8, 7])

    check (await announcer.publishContent(
      ContentAnnouncement(
        id: "shared-doc",
        mediaType: "application/markdown",
        summary: "shared",
        contentKey: "cid://shared-doc",
        providers: @[gossip[2].peerInfo.peerId],
        bootstrapPeers: @[gossip[2].peerInfo.peerId],
      )
    )) > 0

    check await firstItem.withTimeout(TestTimeout)
    let hinted = firstItem.read()
    check hinted.announcement.id == "shared-doc"
    check hinted.payload == @[byte 9, 8, 7]
    check hinted.fetchError.len == 0
    check hinted.fetchedFrom.isSome()
    check hinted.fetchedFrom.get() == gossip[2].peerInfo.peerId

    await waitSub(gossip[2], gossip[0], toTopic("/content-feed", gossip[2].peerInfo.peerId))

    check (await provider.publishContent(
      ContentAnnouncement(
        id: "shared-doc-2",
        mediaType: "application/markdown",
        summary: "follow-up",
        contentKey: "cid://shared-doc-2",
      ),
      @[byte 6, 6, 6],
    )) > 0

    check await secondItem.withTimeout(TestTimeout)
    let followed = secondItem.read()
    check followed.announcement.id == "shared-doc-2"
    check followed.payload == @[byte 6, 6, 6]
    check followed.fetchError.len == 0
