{.used.}

# Nim-LibP2P

import std/[sequtils, options, tables, sets]
import unittest2
import chronos

import ../libp2p/protocols/livestream/[protobuf, livestream]
import ../libp2p/protocols/pubsub/[pubsub, pubsubpeer]
import ../libp2p/protocols/pubsub/gossipsub/types
import ../libp2p/protocols/pubsub/rpc/message
import ../libp2p/peerid
import ./utils/async_tests

proc strToBytes(s: string): seq[byte] =
  s.toSeq().mapIt(byte(it))

type
  TestGossipSub = ref object of GossipSub

proc newTestGossipSub(): TestGossipSub =
  new(result)
  result.topics = initTable[string, seq[TopicHandler]]()
  result.peers = initTable[PeerId, PubSubPeer]()
  result.validators = initTable[string, HashSet[ValidatorHandler]]()
  result.knownTopics = initHashSet[string]()
  result.triggerSelf = true

var testPublishTriggered {.threadvar.}: bool

method publish*(
    g: TestGossipSub,
    topic: string,
    data: seq[byte],
    publishParams: Option[PublishParams] = none(PublishParams),
): Future[int] {.async: (raises: []).} =
  testPublishTriggered = true
  var delivered = 0
  if g.topics.hasKey(topic):
    g.topics.withValue(topic, handlers):
      for handler in handlers[]:
        try:
          await handler(topic, data)
        except CatchableError as err:
          doAssert false, "topic handler threw: " & err.msg
      delivered = handlers[].len
  return delivered

suite "livestream helpers":
  test "segment encode decode roundtrip":
    let segment = LiveSegment(
      sequence: 42,
      timestamp: 123456,
      keyframe: true,
      layer: 1,
      payload: @[byte 1, 2, 3, 4],
    )
    let encoded = encode(segment)
    let decoded = decode(encoded)
    check decoded.isSome()
    let seg2 = decoded.get()
    check seg2.sequence == segment.sequence
    check seg2.timestamp == segment.timestamp
    check seg2.keyframe == segment.keyframe
    check seg2.layer == segment.layer
    check seg2.payload == segment.payload
    check seg2.fragmentCount == 0

  test "ack encode decode roundtrip":
    let ack = LiveAck(
      sequence: 100,
      timestamp: 1234,
      peer: "12D3Foo",
      layer: 2,
      bufferedSegments: 5,
      droppedSegments: 3,
      requestedLayer: 1,
      latencyMs: 120,
    )
    let encoded = encodeAck(ack)
    let decoded = decodeAck(encoded)
    check decoded.isSome()
    let ack2 = decoded.get()
    check ack2.sequence == ack.sequence
    check ack2.timestamp == ack.timestamp
    check ack2.peer == ack.peer
    check ack2.layer == ack.layer
    check ack2.bufferedSegments == ack.bufferedSegments
    check ack2.droppedSegments == ack.droppedSegments
    check ack2.requestedLayer == ack.requestedLayer
    check ack2.latencyMs == ack.latencyMs

  asyncTest "subscriber orders out-of-order segments":
    var receivedSeq: seq[uint64] = @[]
    proc handler(seg: LiveSegment): Future[void] {.async.} =
      receivedSeq.add(seg.sequence)

    let sub = LiveStreamSubscriber.new(
      nil, "/live/test", handler, LiveStreamConfig(bufferSize: 8, startSequence: 0, jitterTolerance: 5)
    )

    await sub.injectSegment(LiveSegment(sequence: 2, timestamp: 0, keyframe: false, layer: 0, payload: @[]))
    await sub.injectSegment(LiveSegment(sequence: 1, timestamp: 0, keyframe: false, layer: 0, payload: @[]))
    await sub.injectSegment(LiveSegment(sequence: 3, timestamp: 0, keyframe: false, layer: 0, payload: @[]))

    check receivedSeq == @[1'u64, 2'u64, 3'u64]

  asyncTest "fragmented segments are reassembled":
    var payloads: seq[seq[byte]] = @[]
    proc handler(seg: LiveSegment): Future[void] {.async.} =
      payloads.add(seg.payload)

    let sub = LiveStreamSubscriber.new(
      nil, "/live/fragments", handler, LiveStreamConfig(bufferSize: 8, startSequence: 0, jitterTolerance: 2)
    )

    let data = strToBytes("hello world")
    let part1 = LiveSegment(
      sequence: 1,
      timestamp: 0,
      keyframe: true,
      layer: 0,
      fragmentIndex: 1,
      fragmentCount: 2,
      payload: data[0 ..< 5],
    )
    let part2 = LiveSegment(
      sequence: 1,
      timestamp: 0,
      keyframe: true,
      layer: 0,
      fragmentIndex: 2,
      fragmentCount: 2,
      payload: data[5 ..< data.len],
    )

    await sub.injectSegment(part2)
    await sub.injectSegment(part1)

    check payloads.len == 1
    check payloads[0] == data

  asyncTest "fragment accumulator expires":
    var payloads: seq[seq[byte]] = @[]
    proc handler(seg: LiveSegment): Future[void] {.async.} =
      payloads.add(seg.payload)

    let cfg = LiveStreamConfig(
      bufferSize: 4,
      startSequence: 0,
      jitterTolerance: 2,
      fragmentTimeout: 100.milliseconds,
    )
    let sub = LiveStreamSubscriber.new(nil, "/live/expire", handler, cfg)

    let data = strToBytes("nim-libp2p")
    let part1 = LiveSegment(
      sequence: 7,
      timestamp: 0,
      keyframe: false,
      layer: 0,
      fragmentIndex: 1,
      fragmentCount: 2,
      payload: data[0 ..< 4],
    )
    let part2 = LiveSegment(
      sequence: 7,
      timestamp: 0,
      keyframe: false,
      layer: 0,
      fragmentIndex: 2,
      fragmentCount: 2,
      payload: data[4 ..< data.len],
    )

    await sub.injectSegment(part1)
    await sleepAsync(150.milliseconds)
    await sub.injectSegment(part1)
    await sub.injectSegment(part2)

    check payloads.len == 1
    check payloads[0] == data

  asyncTest "subscriber auto layer adaptation reacts to drops":
    var received: seq[uint64] = @[]
    proc handler(seg: LiveSegment): Future[void] {.async.} =
      received.add(seg.sequence)

    let cfg = LiveStreamConfig(
      bufferSize: 4,
      startSequence: 1,
      jitterTolerance: 0,
      fragmentTimeout: 50.milliseconds,
      ackInterval: 0.milliseconds,
      autoLayer: true,
      minLayer: 1,
      maxLayer: 2,
      preferredLayer: 2,
      layerDowngradeDropThreshold: 0,
      layerUpgradeCooldown: 50.milliseconds,
    )
    let sub = LiveStreamSubscriber.new(nil, "/live/auto", handler, cfg)
    check sub.currentLayer == 2

    let part = LiveSegment(
      sequence: 1,
      timestamp: 0,
      keyframe: true,
      layer: 2,
      fragmentIndex: 1,
      fragmentCount: 2,
      payload: @[byte 1, 2, 3],
    )
    await sub.injectSegment(part)
    await sleepAsync(60.milliseconds)

    await sub.injectSegment(LiveSegment(sequence: 2, timestamp: 0, keyframe: false, layer: 2, payload: @[]))
    check sub.currentLayer == 1
    check received == @[2'u64]

    await sleepAsync(60.milliseconds)
    for seq in 4'u64 .. 7'u64:
      await sub.injectSegment(LiveSegment(sequence: seq, timestamp: 0, keyframe: false, layer: 1, payload: @[]))

    check received == @[2'u64, 4'u64, 5'u64, 6'u64, 7'u64]
    check sub.deliveredSinceLastDecision == 0
    check sub.dropsSinceLastDecision == 0
    check sub.currentLayer == 2

  asyncTest "subscriber multi layer upgrade resets window":
    var seqs: seq[uint64] = @[]
    proc handler(seg: LiveSegment): Future[void] {.async.} =
      seqs.add(seg.sequence)

    let cfg = LiveStreamConfig(
      bufferSize: 2,
      startSequence: 0,
      jitterTolerance: 0,
      fragmentTimeout: 0.milliseconds,
      ackInterval: 0.milliseconds,
      autoLayer: true,
      minLayer: 0,
      maxLayer: 3,
      preferredLayer: 0,
      layerDowngradeDropThreshold: 1,
      layerUpgradeCooldown: 10.milliseconds,
    )
    let sub = LiveStreamSubscriber.new(nil, "/live/multi", handler, cfg)
    check sub.currentLayer == 0

    await sub.injectSegment(
      LiveSegment(sequence: 1, timestamp: 0, keyframe: true, layer: 0, payload: @[])
    )
    await sub.injectSegment(
      LiveSegment(sequence: 2, timestamp: 0, keyframe: false, layer: 0, payload: @[])
    )

    check sub.currentLayer == 1
    check sub.awaitingResync
    check sub.deliveredSinceLastDecision == 0
    check sub.dropsSinceLastAck == 0

    let dropsBefore = sub.dropsSinceLastDecision
    await sub.injectSegment(
      LiveSegment(sequence: 3, timestamp: 0, keyframe: false, layer: 2, payload: @[])
    )
    check sub.dropsSinceLastDecision == dropsBefore + 1
    check sub.dropsSinceLastAck == 0
    check sub.awaitingResync

    await sub.injectSegment(
      LiveSegment(sequence: 4, timestamp: 0, keyframe: true, layer: 1, payload: @[])
    )
    check not sub.awaitingResync
    check sub.deliveredSinceLastDecision == 1
    check sub.dropsSinceLastAck == 0

    await sleepAsync(15.milliseconds)
    await sub.injectSegment(
      LiveSegment(sequence: 5, timestamp: 0, keyframe: false, layer: 1, payload: @[])
    )
    check sub.currentLayer == 2
    check sub.awaitingResync
    check sub.deliveredSinceLastDecision == 0

    await sub.injectSegment(
      LiveSegment(sequence: 6, timestamp: 0, keyframe: false, layer: 2, payload: @[])
    )
    check not sub.awaitingResync
    check sub.deliveredSinceLastDecision == 1
    check sub.dropsSinceLastAck == 0

  asyncTest "subscriber resync downgrade fallback after timeout":
    var received: seq[uint64] = @[]
    proc handler(seg: LiveSegment): Future[void] {.async.} =
      received.add(seg.sequence)

    let cfg = LiveStreamConfig(
      bufferSize: 2,
      startSequence: 0,
      jitterTolerance: 0,
      fragmentTimeout: 0.milliseconds,
      ackInterval: 0.milliseconds,
      autoLayer: true,
      minLayer: 0,
      maxLayer: 2,
      preferredLayer: 0,
      layerDowngradeDropThreshold: 1,
      layerUpgradeCooldown: 20.milliseconds,
    )
    let sub = LiveStreamSubscriber.new(nil, "/live/fallback", handler, cfg)

    await sub.injectSegment(
      LiveSegment(sequence: 1, timestamp: 0, keyframe: true, layer: 0, payload: @[])
    )
    await sub.injectSegment(
      LiveSegment(sequence: 2, timestamp: 0, keyframe: false, layer: 0, payload: @[])
    )
    check sub.currentLayer == 1
    check sub.awaitingResync

    await sub.injectSegment(
      LiveSegment(sequence: 3, timestamp: 0, keyframe: true, layer: 1, payload: @[])
    )
    check not sub.awaitingResync

    await sleepAsync(25.milliseconds)
    await sub.injectSegment(
      LiveSegment(sequence: 4, timestamp: 0, keyframe: false, layer: 1, payload: @[])
    )
    check sub.currentLayer == 2
    check sub.awaitingResync

    await sub.injectSegment(
      LiveSegment(sequence: 5, timestamp: 0, keyframe: false, layer: 3, payload: @[])
    )
    check sub.awaitingResync
    check sub.currentLayer == 2
    check sub.dropsSinceLastDecision == 1

    await sleepAsync(30.milliseconds)
    await sub.injectSegment(
      LiveSegment(sequence: 6, timestamp: 0, keyframe: false, layer: 3, payload: @[])
    )

    check sub.currentLayer == 1
    check sub.awaitingResync
    check sub.dropsSinceLastDecision == 0

  asyncTest "publisher ack handler receives pubsub roundtrip":
    let gossip = newTestGossipSub()
    var ackRecords: seq[LiveAck] = @[]
    proc onAck(ack: LiveAck): Future[void] {.async.} =
      ackRecords.add(ack)

    var pubCfg = LiveStreamConfig.init()
    pubCfg.ackTopic = "/live/ack"
    pubCfg.ackInterval = 0.milliseconds
    pubCfg.autoLayer = false
    pubCfg.bufferSize = 4
    pubCfg.preferredLayer = 1
    pubCfg.minLayer = 0
    pubCfg.maxLayer = 2
    let publisher = LiveStreamPublisher.new(gossip, "/live/topic", pubCfg, onAck)

    var subCfg = pubCfg
    var delivered: seq[seq[byte]] = @[]
    proc subHandler(seg: LiveSegment): Future[void] {.async.} =
      delivered.add(seg.payload)
    let subscriber = LiveStreamSubscriber.new(gossip, "/live/topic", subHandler, subCfg)
    subscriber.start()
    check publisher.g of TestGossipSub
    check gossip.topics.hasKey("/live/topic")
    check gossip.topics.hasKey("/live/ack")

    let payload = @[byte 1, 2, 3, 4]
    testPublishTriggered = false
    let attempted = await publisher.publishSegment(payload, keyframe = true, layer = 1)
    check testPublishTriggered
    check attempted == gossip.topics["/live/topic"].len
    var waitIters = 0
    while ackRecords.len == 0 and delivered.len == 0 and waitIters < 50:
      await sleepAsync(10.milliseconds)
      inc waitIters

    check delivered.len == 1
    check delivered[0] == payload
    check ackRecords.len == 1
    let ack = ackRecords[0]
    check ack.layer == 1
    check ack.droppedSegments == 0
    check ack.requestedLayer == subCfg.preferredLayer
    check ack.bufferedSegments <= subCfg.bufferSize.uint32

  test "publisher stats reflect subscriber resync without drop inflation":
    var cfg = LiveStreamConfig(
      bufferSize: 1,
      startSequence: 0,
      jitterTolerance: 0,
      fragmentTimeout: 0.milliseconds,
      ackInterval: 0.milliseconds,
      autoLayer: true,
      minLayer: 0,
      maxLayer: 3,
      preferredLayer: 0,
      layerDowngradeDropThreshold: 1,
      layerUpgradeCooldown: 10.milliseconds,
    )
    let pub = LiveStreamPublisher.new(nil, "/live/stats", cfg)

    let seg1 = LiveSegment(sequence: 1, timestamp: 0, keyframe: true, layer: 0, payload: @[])
    let seg2 = LiveSegment(sequence: 2, timestamp: 0, keyframe: false, layer: 2, payload: @[])
    let seg3 = LiveSegment(sequence: 3, timestamp: 0, keyframe: true, layer: 1, payload: @[])
    let seg4 = LiveSegment(sequence: 4, timestamp: 0, keyframe: false, layer: 2, payload: @[])
    var received: seq[uint64] = @[]
    proc handler(seg: LiveSegment): Future[void] {.async.} =
      received.add(seg.sequence)

    let sub = LiveStreamSubscriber.new(nil, "/live/stats", handler, cfg)

    waitFor sub.injectSegment(seg1)
    check sub.currentLayer == 1
    check sub.awaitingResync
    check received == @[1'u64]

    waitFor sub.injectSegment(seg2)
    check sub.awaitingResync
    check sub.dropsSinceLastAck == 0
    check received == @[1'u64]

    waitFor sleepAsync(15.milliseconds)

    waitFor sub.injectSegment(seg3)
    check sub.currentLayer == 2
    check sub.awaitingResync
    check received == @[1'u64, 3'u64]

    waitFor sub.injectSegment(seg4)
    check not sub.awaitingResync
    check sub.dropsSinceLastAck == 0
    check received == @[1'u64, 3'u64, 4'u64]

    let ack = LiveAck(
      sequence: seg4.sequence,
      timestamp: 0,
      peer: "peer-sub-1",
      layer: seg4.layer,
      bufferedSegments: sub.buffer.len().uint32,
      droppedSegments: sub.dropsSinceLastAck,
      requestedLayer: sub.currentLayer,
      latencyMs: 0,
    )
    pub.applyAck(ack)
    let stats = pub.stats()
    check stats.totalDroppedSegments == 0
    check stats.suggestedLayer == sub.currentLayer
    check stats.viewers == 1

  test "publisher aggregates viewer metrics":
    var cfg = LiveStreamConfig.init()
    cfg.preferredLayer = 1
    let pub = LiveStreamPublisher.new(nil, "/live/stats", cfg)
    pub.applyAck(
      LiveAck(
        sequence: 1,
        timestamp: 0,
        peer: "peer1",
        layer: 1,
        bufferedSegments: 3,
        droppedSegments: 1,
        requestedLayer: 1,
        latencyMs: 120,
      )
    )
    pub.applyAck(
      LiveAck(
        sequence: 2,
        timestamp: 0,
        peer: "peer2",
        layer: 0,
        bufferedSegments: 2,
        droppedSegments: 0,
        requestedLayer: 0,
        latencyMs: 80,
      )
    )
    check pub.preferredLayer() == 0
    let stats = pub.stats()
    check stats.viewers == 2
    check stats.totalDroppedSegments == 1
    check stats.suggestedLayer == 0
    check stats.averageLatencyMs > 0.0
