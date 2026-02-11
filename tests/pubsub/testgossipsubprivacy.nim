# Nim-LibP2P
# Copyright (c) 2025 Status Research & Development GmbH
# Licensed under either of
#  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
#  * MIT license ([LICENSE-MIT](LICENSE-MIT))
# at your option.
# This file may not be copied, modified, or distributed except according to
# those terms.

when defined(libp2p_privacy_gossip):
  {.used.}

  import chronos
  import std/sequtils
  import unittest
  import utils
  import ../../libp2p/protocols/pubsub/[gossipsub, pubsubpeer]
  import ../helpers

  type
    PrivacySetupResult = tuple[
      gossip: TestGossipSub,
      conns: seq[Connection],
      peers: seq[PubSubPeer],
      counters: seq[int]
    ]

  proc setupPrivacyNode(
      peerCount: int,
      topic: string,
      params: GossipSubParams
  ): PrivacySetupResult =
    let gossip = TestGossipSub.init(newStandardSwitch(transport = TransportType.Memory), parameters = params)
    gossip.subscribe(topic, voidTopicHandler)
    gossip.topicParams[topic] = TopicParams.init()
    gossip.mesh[topic] = initHashSet[PubSubPeer]()
    gossip.gossipsub[topic] = initHashSet[PubSubPeer]()
    gossip.fanout[topic] = initHashSet[PubSubPeer]()

    var counters = newSeq[int](peerCount)
    var conns = newSeq[Connection]()
    var peers = newSeq[PubSubPeer]()

    for i in 0 ..< peerCount:
      let idx = i
      proc writeRecorder(data: seq[byte]): Future[void] {.async.} =
        inc counters[idx]
      let conn = TestBufferStream.new(writeRecorder)
      conn.peerId = randomPeerId()
      let peer = gossip.getPubSubPeer(conn.peerId)
      peer.sendConn = conn
      peer.handler = voidPeerHandler
      peers.add(peer)
      conns.add(conn)
      gossip.gossipsub[topic].incl(peer)
      gossip.mesh[topic].incl(peer)
      gossip.fanout[topic].incl(peer)

    (gossip, conns, peers, counters)

  suite "GossipSub privacy":
    const topic = "privacy/test"

    teardown:
      checkTrackers()

    asyncTest "stem phase publish limits recipients":
      var params = GossipSubParams.init()
      params.privacyDandelion = true
      params.privacyStemDuration = 5.minutes
      params.privacyFluffDuration = 5.minutes
      params.privacyRouteTTL = 5.minutes
      params.privacyStemFanout = 1
      params.heartbeatInterval = TEST_GOSSIPSUB_HEARTBEAT_INTERVAL

      var (gossip, conns, _, counters) = setupPrivacyNode(3, topic, params)
      defer:
        await teardownGossipSub(gossip, conns)

      discard await gossip.publish(topic, @[byte 0x01, 0x02])
      await waitForHeartbeat()

      check:
        counters.countIt(it > 0) == 1
        counters.sum == counters.max

    asyncTest "normal publish fanout unchanged without privacy":
      var params = GossipSubParams.init()
      params.privacyDandelion = false
      params.heartbeatInterval = TEST_GOSSIPSUB_HEARTBEAT_INTERVAL

      var (gossip, conns, _, counters) = setupPrivacyNode(3, topic, params)
      defer:
        await teardownGossipSub(gossip, conns)

      discard await gossip.publish(topic, @[byte 0x03, 0x04])
      await waitForHeartbeat()

      check:
        counters.countIt(it > 0) == 3
        counters.sum == 3
else:
  static:
    discard
