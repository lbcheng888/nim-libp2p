# Nim-LibP2P
# Licensed under either of Apache 2.0 or MIT
{.push raises: [].}

import std/[tables, options, sequtils, strutils, math]
import chronos, chronicles

import ../pubsub/gossipsub
import ../pubsub/pubsub
import ./protobuf

logScope:
  topics = "libp2p livestream"

const
  LiveLayerAuto* = high(uint32)

type
  FragmentAccumulator = object
    total*: uint32
    received*: Table[uint32, seq[byte]]
    keyframe*: bool
    layer*: uint32
    timestamp*: uint64
    createdAt*: Moment

  LiveSegmentHandler* = proc(segment: LiveSegment): Future[void] {.
    gcsafe, raises: []
  .}

  LiveAckHandler* = proc(ack: LiveAck): Future[void] {.gcsafe, raises: [].}

  LiveStreamConfig* = object
    bufferSize*: int
    startSequence*: uint64
    jitterTolerance*: uint32
    maxSegmentBytes*: int
    ackTopic*: string
    ackInterval*: chronos.Duration
    fragmentTimeout*: chronos.Duration
    autoLayer*: bool
    minLayer*: uint32
    maxLayer*: uint32
    preferredLayer*: uint32
    layerDowngradeDropThreshold*: uint32
    layerUpgradeCooldown*: chronos.Duration

  LiveViewerMetrics* = object
    latencyMs*: uint64
    bufferedSegments*: uint32
    droppedSegments*: uint32
    requestedLayer*: uint32
    layer*: uint32
    updatedAt*: Moment

  LiveStreamPublisher* = ref object
    g*: GossipSub
    topic*: string
    seq*: uint64
    config*: LiveStreamConfig
    ackHandler*: LiveAckHandler
    ackSubscription*: TopicHandler
    ackTopic*: string
    defaultLayer*: uint32
    layerRequests*: Table[string, uint32]
    viewerMetrics*: Table[string, LiveViewerMetrics]

  LiveStreamSubscriber* = ref object
    g*: GossipSub
    topic*: string
    handler*: LiveSegmentHandler
    config*: LiveStreamConfig
    nextSeq*: uint64
    buffer*: Table[uint64, LiveSegment]
    subscription*: TopicHandler
    ackTopic*: string
    ackInterval*: chronos.Duration
    lastAck*: Moment
    fragments*: Table[uint64, FragmentAccumulator]
    currentLayer*: uint32
    dropsSinceLastDecision*: uint32
    deliveredSinceLastDecision*: uint32
    lastLayerDecision*: Moment
    dropsSinceLastAck*: uint32
    awaitingResync*: bool

  LiveStreamPublisherStats* = object
    viewers*: int
    averageLatencyMs*: float
    averageBufferedSegments*: float
    totalDroppedSegments*: uint64
    suggestedLayer*: uint32

proc init*(T: type LiveStreamConfig): T =
  LiveStreamConfig(
    bufferSize: 256,
    startSequence: 0,
    jitterTolerance: 32,
    maxSegmentBytes: 0,
    ackTopic: "",
    ackInterval: chronos.seconds(2),
    fragmentTimeout: chronos.seconds(5),
    autoLayer: false,
    minLayer: 0,
    maxLayer: 0,
    preferredLayer: 0,
    layerDowngradeDropThreshold: 5,
    layerUpgradeCooldown: chronos.seconds(15),
  )

proc clampLayer(config: LiveStreamConfig, value: uint32): uint32 =
  var res = value
  if config.maxLayer != 0 and res > config.maxLayer:
    res = config.maxLayer
  if res < config.minLayer:
    res = config.minLayer
  res

proc evaluateLayerAdjust(sub: LiveStreamSubscriber) {.gcsafe.}

proc recordDrop(sub: LiveStreamSubscriber, count: uint32 = 1) =
  if sub.awaitingResync:
    sub.dropsSinceLastDecision += count
    let elapsed = Moment.now() - sub.lastLayerDecision
    if elapsed >= sub.config.layerUpgradeCooldown or
        sub.dropsSinceLastDecision > sub.config.layerDowngradeDropThreshold:
      sub.evaluateLayerAdjust()
    return
  sub.dropsSinceLastAck += count
  sub.dropsSinceLastDecision += count

proc recordDelivery(sub: LiveStreamSubscriber) =
  if sub.awaitingResync:
    sub.awaitingResync = false
    sub.dropsSinceLastDecision = 0
    sub.dropsSinceLastAck = 0
  inc sub.deliveredSinceLastDecision

proc evaluateLayerAdjust(sub: LiveStreamSubscriber) {.gcsafe.} =
  if not sub.config.autoLayer:
    sub.dropsSinceLastDecision = 0
    sub.deliveredSinceLastDecision = 0
    sub.awaitingResync = false
    return
  let now = Moment.now()
  let minLayer = sub.config.minLayer
  let maxLayer =
    if sub.config.maxLayer == 0:
      high(uint32)
    else:
      sub.config.maxLayer
  if sub.dropsSinceLastDecision > sub.config.layerDowngradeDropThreshold and sub.currentLayer > minLayer:
    sub.currentLayer = clampLayer(sub.config, sub.currentLayer - 1)
    sub.lastLayerDecision = now
    sub.dropsSinceLastDecision = 0
    sub.deliveredSinceLastDecision = 0
    sub.awaitingResync = true
    return
  if sub.dropsSinceLastDecision == 0 and sub.currentLayer < maxLayer:
    if now - sub.lastLayerDecision >= sub.config.layerUpgradeCooldown and
    sub.deliveredSinceLastDecision >= sub.config.bufferSize.uint32:
      let nextLayer = clampLayer(sub.config, sub.currentLayer + 1)
      if nextLayer > sub.currentLayer:
        sub.currentLayer = nextLayer
        sub.lastLayerDecision = now
        sub.dropsSinceLastDecision = 0
        sub.deliveredSinceLastDecision = 0
        sub.awaitingResync = true

proc handleAck(pub: LiveStreamPublisher, ack: LiveAck): Future[void] {.async.}

proc new*(
    T: type LiveStreamPublisher,
    g: GossipSub,
    topic: string,
    config: LiveStreamConfig = LiveStreamConfig.init(),
    ackHandler: LiveAckHandler = nil,
): T =
  let publisher = LiveStreamPublisher(
    g: g,
    topic: topic,
    seq: config.startSequence,
    config: config,
    ackHandler: ackHandler,
    ackTopic: config.ackTopic,
    defaultLayer: clampLayer(config, config.preferredLayer),
    layerRequests: initTable[string, uint32](),
    viewerMetrics: initTable[string, LiveViewerMetrics](),
  )
  if not publisher.ackTopic.isEmptyOrWhitespace() and not publisher.g.isNil:
    let sub = proc(t: string, data: seq[byte]): Future[void] {.async.} =
      let ackOpt = decodeAck(data)
      if ackOpt.isNone():
        return
      await publisher.handleAck(ackOpt.get())
    publisher.ackSubscription = sub
    g.subscribe(publisher.ackTopic, sub)
  publisher

proc applyAck*(pub: LiveStreamPublisher, ack: LiveAck) =
  if ack.peer.len > 0:
    let requested = clampLayer(pub.config, ack.requestedLayer)
    pub.layerRequests[ack.peer] = requested
    pub.viewerMetrics[ack.peer] = LiveViewerMetrics(
      latencyMs: ack.latencyMs.uint64,
      bufferedSegments: ack.bufferedSegments,
      droppedSegments: ack.droppedSegments,
      requestedLayer: requested,
      layer: ack.layer,
      updatedAt: Moment.now(),
    )

proc preferredLayer*(pub: LiveStreamPublisher): uint32 =
  if pub.layerRequests.len == 0:
    return pub.defaultLayer
  var best = high(uint32)
  for value in pub.layerRequests.values:
    let clamped = clampLayer(pub.config, value)
    if clamped < best:
      best = clamped
  if best == high(uint32):
    return pub.defaultLayer
  clampLayer(pub.config, best)

proc stats*(pub: LiveStreamPublisher): LiveStreamPublisherStats =
  var result = LiveStreamPublisherStats()
  result.suggestedLayer = pub.preferredLayer()
  if pub.viewerMetrics.len == 0:
    return result
  var latencyTotal = 0.0
  var bufferTotal = 0.0
  var dropsTotal: uint64 = 0
  for metric in pub.viewerMetrics.values:
    latencyTotal += float(metric.latencyMs)
    bufferTotal += float(metric.bufferedSegments)
    dropsTotal += metric.droppedSegments
  result.viewers = pub.viewerMetrics.len
  result.averageLatencyMs = latencyTotal / float(result.viewers)
  result.averageBufferedSegments = bufferTotal / float(result.viewers)
  result.totalDroppedSegments = dropsTotal
  result

proc handleAck(pub: LiveStreamPublisher, ack: LiveAck) {.async.} =
  pub.applyAck(ack)
  if not pub.ackHandler.isNil:
    await pub.ackHandler(ack)

proc publishSegment*(
    pub: LiveStreamPublisher,
    payload: seq[byte],
    keyframe = false,
    layer: uint32 = 0,
): Future[int] {.async: (raises: []).} =
  if pub.g.isNil:
    trace "publisher without gossipsub instance", topic = pub.topic
    return 0

  var effectiveLayer = layer
  if layer == LiveLayerAuto:
    effectiveLayer = pub.preferredLayer()
  effectiveLayer = clampLayer(pub.config, effectiveLayer)

  inc pub.seq
  let segment = LiveSegment(
    sequence: pub.seq,
    timestamp: (Moment.now().epochSeconds).uint64,
    keyframe: keyframe,
    layer: effectiveLayer,
    payload: payload,
  )
  if pub.config.maxSegmentBytes > 0 and payload.len > pub.config.maxSegmentBytes:
    var total = (payload.len + pub.config.maxSegmentBytes - 1) div pub.config.maxSegmentBytes
    var sent = 0
    for idx in 0 ..< total:
      let start = idx * pub.config.maxSegmentBytes
      let stop = min(start + pub.config.maxSegmentBytes, payload.len)
      var chunk = segment
      chunk.fragmentIndex = (idx + 1).uint32
      chunk.fragmentCount = total.uint32
      chunk.payload = payload[start ..< stop]
      let encoded = encode(chunk)
      discard await pub.g.publish(pub.topic, encoded)
      inc sent
    return sent

  let encoded = encode(segment)
  trace "publishing live segment", topic = pub.topic, seq = pub.seq, size = payload.len
  let sent = await pub.g.publish(pub.topic, encoded)
  return sent

proc stop*(pub: LiveStreamPublisher) =
  if not pub.g.isNil and not pub.ackSubscription.isNil:
    pub.g.unsubscribe(pub.ackTopic, pub.ackSubscription)
    pub.ackSubscription = nil
  pub.layerRequests.clear()
  pub.viewerMetrics.clear()

proc new*(
    T: type LiveStreamSubscriber,
    g: GossipSub,
    topic: string,
    handler: LiveSegmentHandler,
    config: LiveStreamConfig = LiveStreamConfig.init(),
): T =
  LiveStreamSubscriber(
    g: g,
    topic: topic,
    handler: handler,
    config: config,
    nextSeq: config.startSequence + 1,
    buffer: initTable[uint64, LiveSegment](),
    ackTopic: config.ackTopic,
    ackInterval: config.ackInterval,
    lastAck: Moment.low(),
    fragments: initTable[uint64, FragmentAccumulator](),
    currentLayer: clampLayer(config, config.preferredLayer),
    dropsSinceLastDecision: 0,
    deliveredSinceLastDecision: 0,
    lastLayerDecision: Moment.low(),
    dropsSinceLastAck: 0,
    awaitingResync: false,
  )

proc drainBuffer(sub: LiveStreamSubscriber): Future[void] {.async.} =
  while sub.buffer.hasKey(sub.nextSeq):
    let segment = sub.buffer[sub.nextSeq]
    sub.buffer.del(sub.nextSeq)
    if not sub.handler.isNil:
      await sub.handler(segment)
    sub.recordDelivery()
    inc sub.nextSeq

proc cleanupFragments(sub: LiveStreamSubscriber) =
  let zeroDuration = chronos.seconds(0)
  if sub.config.fragmentTimeout == zeroDuration or sub.config.fragmentTimeout < zeroDuration:
    return
  let now = Moment.now()
  var expired: seq[uint64] = @[]
  for seq, acc in sub.fragments:
    if acc.total == 0:
      continue
    if now - acc.createdAt > sub.config.fragmentTimeout:
      expired.add(seq)
  for seq in expired:
    sub.fragments.del(seq)
    sub.recordDrop()

proc enqueueSegment(
    sub: LiveStreamSubscriber, segment: LiveSegment
): Future[bool] {.async.} =
  if segment.sequence < sub.nextSeq:
    trace "dropping stale live segment", seq = segment.sequence, expected = sub.nextSeq
    sub.recordDrop()
    return false

  if segment.layer > sub.currentLayer:
    trace "ignoring layer above current preference",
      seq = segment.sequence, layer = segment.layer, currentLayer = sub.currentLayer
    sub.recordDrop()
    return false

  let jitterMax = sub.nextSeq + sub.config.jitterTolerance.uint64
  if segment.sequence > sub.nextSeq and sub.buffer.len == 0 and
      segment.sequence > jitterMax:
    let skipped = segment.sequence - sub.nextSeq
    if skipped > 0 and not sub.awaitingResync:
      sub.recordDrop(skipped.uint32)
    sub.nextSeq = segment.sequence

  if sub.buffer.len() >= sub.config.bufferSize:
    let oldest = sub.buffer.keys.toSeq().min()
    sub.buffer.del(oldest)
    sub.recordDrop()
  sub.buffer[segment.sequence] = segment

  if segment.sequence == sub.nextSeq or
      segment.sequence <= sub.nextSeq + sub.config.jitterTolerance.uint64:
    await sub.drainBuffer()
  true

proc assembleFragment(
    sub: LiveStreamSubscriber, segment: LiveSegment
): Option[LiveSegment] =
  var acc = sub.fragments.getOrDefault(segment.sequence)
  if acc.total == 0:
    acc.total = segment.fragmentCount
    acc.keyframe = segment.keyframe
    acc.layer = segment.layer
    acc.timestamp = segment.timestamp
    acc.received = initTable[uint32, seq[byte]]()
    acc.createdAt = Moment.now()
  acc.received[segment.fragmentIndex] = segment.payload
  if acc.received.len < acc.total.int:
    sub.fragments[segment.sequence] = acc
    return none(LiveSegment)
  sub.fragments.del(segment.sequence)
  var payload = newSeq[byte]()
  for idx in 1 .. acc.total:
    acc.received.withValue(idx.uint32, data):
      payload.add(data[])
  var merged = segment
  merged.payload = payload
  merged.fragmentCount = 0
  merged.fragmentIndex = 0
  merged.keyframe = acc.keyframe
  merged.layer = acc.layer
  merged.timestamp = acc.timestamp
  some(merged)

proc sendAck(sub: LiveStreamSubscriber, segment: LiveSegment) {.async.} =
  let now = Moment.now()
  if now - sub.lastAck < sub.ackInterval:
    return
  sub.lastAck = now
  var latencyMs: uint32 = 0
  let nowSeconds = now.epochSeconds.uint64
  if segment.timestamp != 0 and nowSeconds >= segment.timestamp:
    let diff = nowSeconds - segment.timestamp
    if diff <= high(uint32).uint64 div 1000'u64:
      latencyMs = (diff * 1000'u64).uint32
    else:
      latencyMs = high(uint32)
  var ack = LiveAck(
    sequence: segment.sequence,
    timestamp: now.epochSeconds.uint64,
    peer: (if not sub.g.isNil and not sub.g.peerInfo.isNil: $sub.g.peerInfo.peerId else: ""),
    layer: segment.layer,
    bufferedSegments: sub.buffer.len().uint32,
    droppedSegments: sub.dropsSinceLastAck,
    requestedLayer: sub.currentLayer,
    latencyMs: latencyMs,
  )
  if not sub.g.isNil and sub.ackTopic.len > 0:
    discard await sub.g.publish(sub.ackTopic, encodeAck(ack))
  sub.dropsSinceLastAck = 0
  sub.evaluateLayerAdjust()

proc processSegment(
    sub: LiveStreamSubscriber, segment: LiveSegment
): Future[Option[LiveSegment]] {.async.} =
  sub.cleanupFragments()
  var seg = segment
  if segment.fragmentCount > 1:
    let assembled = sub.assembleFragment(segment)
    if assembled.isNone():
      return none(LiveSegment)
    seg = assembled.get()
  let accepted = await sub.enqueueSegment(seg)
  if not accepted:
    return none(LiveSegment)
  some(seg)

proc handleSegment(
    sub: LiveStreamSubscriber, topic: string, payload: seq[byte]
): Future[void] {.async.} =
  let segmentOpt = decode(payload)
  if segmentOpt.isNone():
    trace "failed decoding live segment payload"
    return
  let segment = segmentOpt.get()
  trace "received live segment", topic = topic, seq = segment.sequence
  let delivered = await sub.processSegment(segment)
  if delivered.isSome():
    await sub.sendAck(delivered.get())

proc start*(sub: LiveStreamSubscriber) =
  if sub.g.isNil:
    return
  if sub.subscription != nil:
    return
  sub.subscription = proc(topic: string, data: seq[byte]): Future[void] {.async.} =
    await sub.handleSegment(topic, data)
  sub.g.subscribe(sub.topic, sub.subscription)

proc stop*(sub: LiveStreamSubscriber) =
  if sub.g.isNil:
    return
  if sub.subscription != nil:
    sub.g.unsubscribe(sub.topic, sub.subscription)
    sub.subscription = nil
  sub.buffer.clear()
  sub.lastAck = Moment.low()
  sub.fragments.clear()
  sub.currentLayer = clampLayer(sub.config, sub.config.preferredLayer)
  sub.dropsSinceLastDecision = 0
  sub.deliveredSinceLastDecision = 0
  sub.dropsSinceLastAck = 0
  sub.lastLayerDecision = Moment.low()
  sub.awaitingResync = false

proc injectSegment*(
    sub: LiveStreamSubscriber, segment: LiveSegment
): Future[void] {.async.} =
  let delivered = await sub.processSegment(segment)
  if delivered.isSome():
    await sub.sendAck(delivered.get())
