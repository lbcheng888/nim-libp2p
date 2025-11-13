# Nim-LibP2P Episub router (hybrid GossipSub)
{.push raises: [].}

import std/[tables, sets, sequtils, options, math]
import chronicles, chronos

import ../gossipsub
import ../pubsubpeer
import ../pubsub
import ../rpc/messages
import ../rpc/message
import ../gossipsub/types
import ../peertable
import ./protobuf
import ../../../peerid
import ../../../switch
import ../../../peerstore
import ../../../crypto/crypto
import ../../kademlia/xordistance

logScope:
  topics = "libp2p episub"

type
  LatencyProbeKind = enum
    lpJoin,
    lpGetNodes

  EpisubParams* = object
    activeViewSize*: int
    passiveViewSize*: int
    passiveLowWater*: int
    randomPeers*: int
    nodesResponseCount*: int
    joinTtl*: uint32
    forwardJoinTtl*: uint32
    controlSuffix*: string
    maintenanceInterval*: Duration
    inactiveTimeout*: Duration
    passiveRefillCooldown*: Duration
    passiveRefillFanout*: int
    predictiveReconnectWindow*: Duration
    predictiveReconnectFanout*: int
    latencySmoothing*: float
    latencyReplaceMargin*: Duration
    latencyFallback*: Duration
    latencyInactivityWeight*: float
    passiveEntryTtl*: Duration
    passiveAgeHalfLife*: Duration
    randomWalkLength*: int
    randomWalkFanout*: int
    randomWalkCooldown*: Duration

  EpisubState = object
    active*: HashSet[PeerId]
    passive*: HashSet[PeerId]
    passiveDistances*: Table[PeerId, XorDistance]
    passiveInserted*: Table[PeerId, Moment]
    lastHeard*: Table[PeerId, Moment]
    lastPassiveRefill*: Moment
    lastRandomWalk*: Moment
    predictiveAttempts*: Table[PeerId, Moment]
    controlHandler*: TopicHandler
    latencyEstimates*: Table[PeerId, Duration]
    pendingLatency*: Table[(PeerId, LatencyProbeKind), Moment]

  Episub* = ref object of GossipSub
    epiParams*: EpisubParams
    state*: Table[string, EpisubState]
    maintenanceLoop*: Future[void]
    pxRecords*: Table[PeerId, uint64]

proc shouldApplyPeerRecord(epi: Episub, record: PeerRecord): bool =
  if record.peerId.len == 0:
    return false
  let previous = epi.pxRecords.getOrDefault(record.peerId)
  if previous >= record.seqNo:
    return false
  epi.pxRecords[record.peerId] = record.seqNo
  true

proc mergePeerRecord(epi: Episub, record: PeerRecord) =
  if record.addresses.len == 0:
    return
  let addrs = record.addresses.mapIt(it.address)
  epi.switch.peerStore.addAddressesWithTTL(
    record.peerId, addrs, DefaultDiscoveredAddressTTL
  )

proc ensureState(epi: Episub, topic: string): var EpisubState {.gcsafe.}
proc addPassive(epi: Episub, topic: string, peer: PeerId) {.gcsafe.}
proc sampleActivePeers(
    epi: Episub, st: var EpisubState, count: int
): seq[PeerId] {.gcsafe.}
proc sendControl(
    epi: Episub, topic: string, dest: seq[PeerId], ctrl: EpisubControl
): Future[void] {.gcsafe.}
proc sendControl(
    epi: Episub, topic: string, dest: PeerId, ctrl: EpisubControl
): Future[void] {.gcsafe.}
proc removePeer(
    epi: Episub, topic: string, peer: PeerId, reAddToPassive = true
) {.gcsafe.}
proc pruneExpiredPassive(epi: Episub, topic: string) {.gcsafe.}
proc pruneInactivePeers(epi: Episub, topic: string): seq[PeerId] {.gcsafe.}

proc handlePeerExchange(
    epi: Episub, topic: string, suggestions: seq[RoutingRecordsPair]
) {.gcsafe.} =
  if suggestions.len == 0:
    return
  if not epi.state.hasKey(topic):
    return
  var st = epi.ensureState(topic)
  var added = 0
  for (peerId, recordOpt) in suggestions:
    if peerId.len == 0 or peerId == epi.peerInfo.peerId:
      continue
    recordOpt.withValue(record):
      if epi.shouldApplyPeerRecord(record):
        epi.mergePeerRecord(record)
    let before = st.passive.len
    epi.addPassive(topic, peerId)
    if st.passive.len > before:
      inc added
  if added > 0:
    trace "episub integrated px", topic, added, passive = st.passive.len

proc defaultParams*(): EpisubParams =
  const DefaultActiveView = 7
  const DefaultPassiveView = 42
  EpisubParams(
    activeViewSize: DefaultActiveView,
    passiveViewSize: DefaultPassiveView,
    passiveLowWater: DefaultPassiveView div 2,
    randomPeers: 4,
    nodesResponseCount: 6,
    joinTtl: 3,
    forwardJoinTtl: 2,
    controlSuffix: "/_episub",
    maintenanceInterval: 15.seconds,
    inactiveTimeout: 45.seconds,
    passiveRefillCooldown: 30.seconds,
    passiveRefillFanout: 2,
    predictiveReconnectWindow: 10.seconds,
    predictiveReconnectFanout: 2,
    latencySmoothing: 0.35,
    latencyReplaceMargin: 10.milliseconds,
    latencyFallback: 250.milliseconds,
    latencyInactivityWeight: 0.25,
    passiveEntryTtl: 5.minutes,
    passiveAgeHalfLife: 2.minutes,
    randomWalkLength: 3,
    randomWalkFanout: 2,
    randomWalkCooldown: 30.seconds,
  )

proc controlTopic(epi: Episub, topic: string): string =
  topic & epi.epiParams.controlSuffix

proc ensureState(epi: Episub, topic: string): var EpisubState {.gcsafe.} =
  epi.state.mgetOrPut(
    topic,
    EpisubState(
      active: initHashSet[PeerId](),
      passive: initHashSet[PeerId](),
      passiveDistances: initTable[PeerId, XorDistance](),
      passiveInserted: initTable[PeerId, Moment](),
      lastHeard: initTable[PeerId, Moment](),
      lastPassiveRefill: Moment.low(),
      lastRandomWalk: Moment.low(),
      predictiveAttempts: initTable[PeerId, Moment](),
      controlHandler: nil,
      latencyEstimates: initTable[PeerId, Duration](),
      pendingLatency: initTable[(PeerId, LatencyProbeKind), Moment](),
    ),
  )

proc recordLatencyProbe(
    epi: Episub, topic: string, peer: PeerId, kind: LatencyProbeKind
) =
  if peer.len == 0:
    return
  var st = epi.ensureState(topic)
  st.pendingLatency[(peer, kind)] = Moment.now()

proc updateLatencyEstimate(
    epi: Episub, st: var EpisubState, peer: PeerId, sample: Duration
) =
  if sample <= chronos.ZeroDuration:
    return
  var measurement = sample
  if measurement < 1.milliseconds:
    measurement = 1.milliseconds
  let alpha = clamp(epi.epiParams.latencySmoothing, 0.05, 1.0)
  let prev = st.latencyEstimates.getOrDefault(peer, measurement)
  if st.latencyEstimates.hasKey(peer):
    let prevNs = prev.nanoseconds().float
    let sampleNs = measurement.nanoseconds().float
    let combinedNs = prevNs * (1.0 - alpha) + sampleNs * alpha
    let clampedNs = max(combinedNs, 1.0)
    st.latencyEstimates[peer] = chronos.nanoseconds(int64(round(clampedNs)))
  else:
    st.latencyEstimates[peer] = measurement

proc completeLatencyProbe(
    epi: Episub, topic: string, peer: PeerId, kind: LatencyProbeKind
) =
  if peer.len == 0:
    return
  var st = epi.ensureState(topic)
  let key = (peer, kind)
  if not st.pendingLatency.contains(key):
    return
  let started = st.pendingLatency.getOrDefault(key, Moment.low())
  st.pendingLatency.del(key)
  let sample = Moment.now() - started
  if sample <= chronos.ZeroDuration:
    return
  epi.updateLatencyEstimate(st, peer, sample)

proc computePeerScore(
    epi: Episub, st: EpisubState, peer: PeerId, now: Moment
): float =
  var latency = st.latencyEstimates.getOrDefault(peer, epi.epiParams.latencyFallback)
  if latency <= chronos.ZeroDuration:
    latency = epi.epiParams.latencyFallback
  let latencyMs = latency.nanoseconds().float / 1_000_000.0
  let last = st.lastHeard.getOrDefault(peer, Moment.low())
  var inactivityMs = 0.0
  if last == Moment.low():
    inactivityMs =
      epi.epiParams.inactiveTimeout.nanoseconds().float / 1_000_000.0
  else:
    let elapsed = now - last
    let positive =
      if elapsed > chronos.ZeroDuration: elapsed else: chronos.ZeroDuration
    inactivityMs = positive.nanoseconds().float / 1_000_000.0
  let weight = max(epi.epiParams.latencyInactivityWeight, 0.0)
  latencyMs + inactivityMs * weight

proc randIndex(rng: ref HmacDrbgContext, maxExclusive: int): int =
  if maxExclusive <= 0:
    return 0
  var buf: array[4, byte]
  hmacDrbgGenerate(rng[], buf)
  let val =
    (int(buf[0]) shl 24) or (int(buf[1]) shl 16) or (int(buf[2]) shl 8) or int(buf[3])
  result = (val and 0x7FFFFFFF) mod maxExclusive

proc getPassiveDistance(
    epi: Episub, st: var EpisubState, peer: PeerId
): XorDistance =
  if peer.len == 0:
    return default(XorDistance)
  let existing = st.passiveDistances.getOrDefault(peer, default(XorDistance))
  if st.passiveDistances.hasKey(peer):
    return existing
  let dist = xorDistance(epi.peerInfo.peerId, peer, Opt.none(XorDHasher))
  st.passiveDistances[peer] = dist
  dist

proc evictFarthest(
    epi: Episub, st: var EpisubState
): Option[PeerId] =
  var selected: Option[PeerId]
  var farthest: XorDistance
  for peer in st.passive:
    let dist = epi.getPassiveDistance(st, peer)
    if selected.isNone or farthest < dist:
      farthest = dist
      selected = some(peer)
  if selected.isSome:
    let peer = selected.get()
    st.passive.excl(peer)
    st.passiveDistances.del(peer)
    st.passiveInserted.del(peer)
  selected

proc weightedPassiveSample(
    epi: Episub, st: var EpisubState, count: int
): seq[PeerId] =
  if count <= 0 or st.passive.len == 0:
    return @[]
  var entries: seq[(PeerId, int)] = @[]
  let now = Moment.now()
  for peer in st.passive:
    let dist = epi.getPassiveDistance(st, peer)
    var weight = max(1, dist.leadingZeros())
    let halfLife = epi.epiParams.passiveAgeHalfLife
    if halfLife > 0.seconds and halfLife.nanoseconds() > 0:
      let inserted = st.passiveInserted.getOrDefault(peer, Moment.low())
      if inserted != Moment.low():
        let age = now - inserted
        if age > chronos.ZeroDuration:
          let exponent =
            age.nanoseconds().float / halfLife.nanoseconds().float
          let decay = pow(0.5, exponent)
          if decay < 1.0:
            let adjusted = float(weight) * decay
            if adjusted < 1.0:
              weight = 1
            else:
              let rounded = int(round(adjusted))
              weight = if rounded >= 1: rounded else: 1
    entries.add((peer, weight))
  var pool = entries
  let limit = min(count, pool.len)
  while result.len < limit and pool.len > 0:
    let totalWeight = pool.foldl(a + b[1], 0)
    if totalWeight <= 0:
      for item in pool:
        result.add(item[0])
      break
    let pick = randIndex(epi.rng, totalWeight)
    var cumulative = 0
    var chosenIdx = 0
    for i, item in pool:
      cumulative += item[1]
      if pick < cumulative:
        chosenIdx = i
        break
    result.add(pool[chosenIdx][0])
    pool.delete(chosenIdx)

proc selectRandomPeers(epi: Episub, topic: string, count: int): seq[PeerId] =
  var st = epi.ensureState(topic)
  epi.weightedPassiveSample(st, count)

proc collectRandomWalkTargets(
    epi: Episub, topic: string, visited: var HashSet[PeerId], fanout: int
): seq[PeerId] {.gcsafe.} =
  if fanout <= 0:
    return @[]
  var st = epi.ensureState(topic)
  var selected: seq[PeerId] = @[]
  let weightedCount = max(fanout * 2, fanout)
  if st.passive.len > 0:
    for peer in epi.weightedPassiveSample(st, weightedCount):
      if peer.len == 0 or peer == epi.peerInfo.peerId:
        continue
      if visited.contains(peer) or selected.contains(peer):
        continue
      selected.add(peer)
      if selected.len >= fanout:
        break
  if selected.len < fanout and st.active.len > 0:
    let remaining = fanout - selected.len
    let activeSample = epi.sampleActivePeers(st, max(remaining * 2, remaining))
    for peer in activeSample:
      if peer.len == 0 or peer == epi.peerInfo.peerId:
        continue
      if visited.contains(peer) or selected.contains(peer):
        continue
      selected.add(peer)
      if selected.len >= fanout:
        break
  if selected.len < fanout:
    var fallback = st.passive.toSeq()
    fallback.add(st.active.toSeq())
    fallback = fallback.filterIt(
      it.len > 0 and it != epi.peerInfo.peerId and not visited.contains(it) and not selected.contains(it)
    )
    while selected.len < fanout and fallback.len > 0:
      let idx = randIndex(epi.rng, fallback.len)
      selected.add(fallback[idx])
      fallback.delete(idx)
  for peer in selected:
    visited.incl(peer)
  selected

proc performRandomWalk(epi: Episub, topic: string) {.async, gcsafe.} =
  let steps = max(0, epi.epiParams.randomWalkLength)
  if steps <= 0:
    return
  var st = epi.ensureState(topic)
  if st.passive.len == 0 and st.active.len == 0:
    return
  let cooldown = epi.epiParams.randomWalkCooldown
  let now = Moment.now()
  if cooldown > 0.seconds and st.lastRandomWalk != Moment.low() and
      now - st.lastRandomWalk < cooldown:
    return
  st.lastRandomWalk = now
  let fanout = max(1, epi.epiParams.randomWalkFanout)
  var visited = initHashSet[PeerId]()
  for _ in 0 ..< steps:
    let targets = epi.collectRandomWalkTargets(topic, visited, fanout)
    if targets.len == 0:
      break
    for peer in targets:
      epi.recordLatencyProbe(topic, peer, lpGetNodes)
    await epi.sendControl(
      topic,
      targets,
      EpisubControl(kind: estGetNodes, peer: epi.peerInfo.peerId),
    )

proc sampleActivePeers(
    epi: Episub, st: var EpisubState, count: int
): seq[PeerId] {.gcsafe.} =
  if count <= 0 or st.active.len == 0:
    return @[]
  var pool = st.active.toSeq()
  let limit = min(count, pool.len)
  while result.len < limit and pool.len > 0:
    let idx = randIndex(epi.rng, pool.len)
    result.add(pool[idx])
    pool.delete(idx)

proc sendControl(
    epi: Episub, topic: string, dest: seq[PeerId], ctrl: EpisubControl
) {.async, gcsafe.} =
  if dest.len == 0:
    return
  let payload = encode(ctrl)
  let ctrlTopic = epi.controlTopic(topic)
  epi.broadcast(dest.mapIt(epi.getOrCreatePeer(it, epi.codecs)), RPCMsg(messages: @[
    Message.init(some(epi.peerInfo), payload, ctrlTopic, none(uint64), epi.sign)
  ]), isHighPriority = true)

proc sendControl(
    epi: Episub, topic: string, dest: PeerId, ctrl: EpisubControl
) {.async, gcsafe.} =
  await epi.sendControl(topic, @[dest], ctrl)

proc addActive(epi: Episub, topic: string, peer: PeerId): bool {.gcsafe.} =
  if peer.len == 0 or peer == epi.peerInfo.peerId:
    return false
  var st = epi.ensureState(topic)
  let now = Moment.now()
  if st.active.contains(peer):
    st.lastHeard[peer] = now
    st.passive.excl(peer)
    st.passiveDistances.del(peer)
    return true
  if st.active.len < epi.epiParams.activeViewSize:
    st.active.incl(peer)
    st.lastHeard[peer] = now
    st.passive.excl(peer)
    st.passiveDistances.del(peer)
    return true

  let candidateScore = epi.computePeerScore(st, peer, now)
  var worstPeer = none(PeerId)
  var worstScore = NegInf
  for existing in st.active:
    let score = epi.computePeerScore(st, existing, now)
    if worstPeer.isNone or score > worstScore:
      worstPeer = some(existing)
      worstScore = score
  if worstPeer.isNone():
    return false

  let margin =
    max(epi.epiParams.latencyReplaceMargin.nanoseconds().float / 1_000_000.0, 0.0)
  if candidateScore + margin < worstScore:
    let removed = worstPeer.get()
    trace "episub replacing active peer",
      topic = topic, removed, candidate = peer, scores = (worstScore, candidateScore)
    epi.removePeer(topic, removed)
    var updated = epi.ensureState(topic)
    updated.active.incl(peer)
    updated.lastHeard[peer] = now
    updated.passive.excl(peer)
    updated.passiveDistances.del(peer)
    asyncSpawn epi.sendControl(
      topic,
      removed,
      EpisubControl(kind: estDisconnect, peer: epi.peerInfo.peerId),
    )
    return true

  epi.addPassive(topic, peer)
  false

proc addPassive(epi: Episub, topic: string, peer: PeerId) {.gcsafe.} =
  if peer == epi.peerInfo.peerId:
    return
  var st = epi.ensureState(topic)
  if peer.len == 0:
    return
  let now = Moment.now()
  if peer in st.active:
    return
  if peer in st.passive:
    st.passiveInserted[peer] = now
    discard epi.getPassiveDistance(st, peer)
    return
  if st.passive.len >= epi.epiParams.passiveViewSize:
    let victimOpt = epi.evictFarthest(st)
    if victimOpt.isNone:
      return
  st.passive.incl(peer)
  let _ = epi.getPassiveDistance(st, peer)
  st.passiveInserted[peer] = now

proc ensurePubsubPeer(epi: Episub, peer: PeerId): PubSubPeer =
  if peer.len == 0:
    return nil
  var pubPeer = epi.peers.getOrDefault(peer)
  if pubPeer.isNil:
    try:
      pubPeer = epi.getOrCreatePeer(peer, epi.codecs)
    except CatchableError:
      return nil
  pubPeer

proc promoteToMesh(epi: Episub, topic: string, peer: PeerId) =
  let pubPeer = epi.ensurePubsubPeer(peer)
  if pubPeer.isNil:
    return
  if epi.gossipsub.addPeer(topic, pubPeer):
    discard
  if epi.mesh.addPeer(topic, pubPeer):
    epi.grafted(pubPeer, topic)

proc removePeer(
    epi: Episub, topic: string, peer: PeerId, reAddToPassive = true
) {.gcsafe.} =
  if not epi.state.hasKey(topic):
    return
  var st = epi.ensureState(topic)
  st.active.excl(peer)
  st.lastHeard.del(peer)
  st.passive.excl(peer)
  st.passiveDistances.del(peer)
  st.passiveInserted.del(peer)
  st.predictiveAttempts.del(peer)
  let pubPeer = epi.peers.getOrDefault(peer)
  if not pubPeer.isNil:
    if epi.mesh.hasPeer(topic, pubPeer):
      epi.pruned(pubPeer, topic)
      epi.mesh.removePeer(topic, pubPeer)
    if epi.gossipsub.hasPeer(topic, pubPeer):
      epi.gossipsub.removePeer(topic, pubPeer)
  if reAddToPassive and peer != epi.peerInfo.peerId:
    epi.addPassive(topic, peer)

proc handleJoin(epi: Episub, topic: string, ctrl: EpisubControl) {.async.} =
  if ctrl.peer.len == 0:
    return
  var st = epi.ensureState(topic)
  let attempted = st.active.len < epi.epiParams.activeViewSize or ctrl.ttl == 0
  var accepted = false
  if attempted:
    accepted = epi.addActive(topic, ctrl.peer)
    st = epi.ensureState(topic)
    if accepted:
      epi.promoteToMesh(topic, ctrl.peer)
      await epi.sendControl(
        topic,
        ctrl.peer,
        EpisubControl(kind: estNeighbor, peer: epi.peerInfo.peerId),
      )
    else:
      await epi.sendControl(
        topic,
        ctrl.peer,
        EpisubControl(kind: estDisconnect, peer: epi.peerInfo.peerId),
      )

  if accepted:
    st = epi.ensureState(topic)
    let peers = st.active.toSeq()
    if peers.len > 0:
      await epi.sendControl(
        topic,
        peers,
        EpisubControl(kind: estForwardJoin, peer: ctrl.peer, ttl: epi.epiParams.forwardJoinTtl),
      )
  else:
    st = epi.ensureState(topic)
    if ctrl.ttl > 0:
      let forward = st.active.toSeq()
      if forward.len > 0:
        let idx = randIndex(epi.rng, forward.len)
        await epi.sendControl(
          topic,
          forward[idx],
          EpisubControl(kind: estJoin, peer: ctrl.peer, ttl: ctrl.ttl - 1),
        )

proc handleNeighbor(epi: Episub, topic: string, ctrl: EpisubControl) =
  if ctrl.peer.len == 0:
    return
  if epi.addActive(topic, ctrl.peer):
    epi.promoteToMesh(topic, ctrl.peer)
  else:
    asyncSpawn epi.sendControl(
      topic,
      ctrl.peer,
      EpisubControl(kind: estDisconnect, peer: epi.peerInfo.peerId),
    )

proc handleForwardJoin(epi: Episub, topic: string, ctrl: EpisubControl) {.async.} =
  if ctrl.peer.len == 0:
    return
  epi.addPassive(topic, ctrl.peer)
  if ctrl.ttl > 0:
    let st = epi.ensureState(topic)
    let forward = st.active.toSeq()
    if forward.len > 0:
      let idx = randIndex(epi.rng, forward.len)
      await epi.sendControl(
        topic,
        forward[idx],
        EpisubControl(kind: estForwardJoin, peer: ctrl.peer, ttl: ctrl.ttl - 1),
      )

proc handleGetNodes(epi: Episub, topic: string, ctrl: EpisubControl) {.async.} =
  if ctrl.peer.len == 0:
    return
  epi.pruneExpiredPassive(topic)
  var st = epi.ensureState(topic)
  let limit = epi.epiParams.nodesResponseCount
  let peers =
    if limit <= 0:
      st.passive.toSeq().mapIt(it)
    else:
      epi.weightedPassiveSample(st, limit)
  await epi.sendControl(
    topic,
    ctrl.peer,
    EpisubControl(kind: estNodes, peers: peers),
  )

proc handleNodes(epi: Episub, topic: string, ctrl: EpisubControl) =
  for peer in ctrl.peers:
    epi.addPassive(topic, peer)

proc pruneExpiredPassive(epi: Episub, topic: string) {.gcsafe.} =
  let ttl = epi.epiParams.passiveEntryTtl
  if ttl <= 0.seconds:
    return
  var st = epi.ensureState(topic)
  if st.passive.len == 0:
    return
  let now = Moment.now()
  var removed: seq[PeerId] = @[]
  for peer in st.passive:
    let inserted = st.passiveInserted.getOrDefault(peer, Moment.low())
    if inserted == Moment.low():
      continue
    let elapsed = now - inserted
    if elapsed <= chronos.ZeroDuration:
      continue
    if elapsed >= ttl:
      removed.add(peer)
  if removed.len == 0:
    return
  for peer in removed:
    st.passive.excl(peer)
    st.passiveDistances.del(peer)
    st.passiveInserted.del(peer)
  trace "episub pruned expired passive peers", topic = topic, count = removed.len

proc pruneInactivePeers(epi: Episub, topic: string): seq[PeerId] {.gcsafe.} =
  if epi.epiParams.inactiveTimeout <= 0.seconds:
    return @[]
  let st = epi.ensureState(topic)
  let now = Moment.now()
  let zeroMoment = Moment.low()
  var victims: seq[PeerId] = @[]
  for peer in st.active:
    let last = st.lastHeard.getOrDefault(peer, zeroMoment)
    if last == zeroMoment:
      continue
    if now - last > epi.epiParams.inactiveTimeout:
      victims.add(peer)
  for peer in victims:
    trace "episub pruning inactive peer", topic = topic, peer
    epi.removePeer(topic, peer, reAddToPassive = false)
  result = victims

method onTopicSubscription*(epi: Episub, topic: string, subscribed: bool) =
  procCall GossipSub(epi).onTopicSubscription(topic, subscribed)
  if subscribed:
    var st = epi.ensureState(topic)
    let ctrlTopic = epi.controlTopic(topic)
    let handler = proc(_: string, data: seq[byte]): Future[void] {.async.} =
      let ctrlOpt = decode(data)
      if ctrlOpt.isNone():
        return
      let ctrl = ctrlOpt.get()
      if ctrl.peer.len > 0:
        var inner = epi.ensureState(topic)
        inner.lastHeard[ctrl.peer] = Moment.now()
        inner.predictiveAttempts.del(ctrl.peer)
      case ctrl.kind
      of estJoin:
        await epi.handleJoin(topic, ctrl)
      of estNeighbor:
        epi.completeLatencyProbe(topic, ctrl.peer, lpJoin)
        epi.handleNeighbor(topic, ctrl)
      of estForwardJoin:
        await epi.handleForwardJoin(topic, ctrl)
      of estDisconnect:
        epi.removePeer(topic, ctrl.peer)
      of estGetNodes:
        await epi.handleGetNodes(topic, ctrl)
      of estNodes:
        epi.completeLatencyProbe(topic, ctrl.peer, lpGetNodes)
        epi.handleNodes(topic, ctrl)
    epi.subscribe(ctrlTopic, handler)
    st.controlHandler = handler
    let samples = epi.selectRandomPeers(topic, epi.epiParams.randomPeers)
    for peer in samples:
      epi.recordLatencyProbe(topic, peer, lpJoin)
      asyncSpawn epi.sendControl(
        topic,
        peer,
        EpisubControl(kind: estJoin, peer: epi.peerInfo.peerId, ttl: epi.epiParams.joinTtl),
      )
    if samples.len > 0:
      for peer in samples:
        epi.recordLatencyProbe(topic, peer, lpGetNodes)
      asyncSpawn epi.sendControl(
        topic,
        samples,
        EpisubControl(kind: estGetNodes, peer: epi.peerInfo.peerId),
      )
      st.lastPassiveRefill = Moment.now()
  else:
    if epi.state.hasKey(topic):
      var st = epi.ensureState(topic)
      if not st.controlHandler.isNil:
        epi.unsubscribe(epi.controlTopic(topic), st.controlHandler)
        st.controlHandler = nil
      epi.state.del(topic)

proc maintainViews(epi: Episub) {.async.} =
  while epi.started:
    for topic in epi.state.keys().toSeq():
      epi.pruneExpiredPassive(topic)
      var st = epi.ensureState(topic)
      let pruned = epi.pruneInactivePeers(topic)
      if pruned.len > 0:
        let peers = st.active.toSeq()
        if peers.len > 0:
          for removed in pruned:
            await epi.sendControl(
              topic,
              peers,
              EpisubControl(kind: estDisconnect, peer: removed),
            )
      if st.active.len < epi.epiParams.activeViewSize and st.passive.len > 0:
        let candidate = epi.selectRandomPeers(topic, 1)
        if candidate.len > 0:
          await epi.sendControl(
            topic,
            candidate[0],
            EpisubControl(kind: estNeighbor, peer: epi.peerInfo.peerId),
          )
          epi.recordLatencyProbe(topic, candidate[0], lpJoin)
          await epi.sendControl(
            topic,
            candidate[0],
            EpisubControl(kind: estJoin, peer: epi.peerInfo.peerId, ttl: epi.epiParams.joinTtl),
          )
      if epi.epiParams.inactiveTimeout > 0.seconds and
          epi.epiParams.predictiveReconnectWindow > 0.seconds and
          epi.epiParams.predictiveReconnectFanout > 0:
        var window = epi.epiParams.predictiveReconnectWindow
        if window > epi.epiParams.inactiveTimeout:
          window = epi.epiParams.inactiveTimeout
        if window > 0.seconds and window <= epi.epiParams.inactiveTimeout:
          let now = Moment.now()
          for peer in st.active.toSeq():
            let last = st.lastHeard.getOrDefault(peer, Moment.low())
            if last == Moment.low():
              continue
            let elapsed = now - last
            if elapsed < epi.epiParams.inactiveTimeout - window:
              continue
            if st.predictiveAttempts.hasKey(peer):
              let prev = st.predictiveAttempts[peer]
              if now - prev < window:
                continue
            st.predictiveAttempts[peer] = now
            let replacements = epi.selectRandomPeers(
              topic, epi.epiParams.predictiveReconnectFanout
            )
            if replacements.len == 0:
              if epi.epiParams.passiveRefillFanout > 0 and st.active.len > 0:
                let refillTargets =
                  epi.sampleActivePeers(st, epi.epiParams.passiveRefillFanout)
                if refillTargets.len > 0:
                  st.lastPassiveRefill = now
                  for peer in refillTargets:
                    epi.recordLatencyProbe(topic, peer, lpGetNodes)
                  await epi.sendControl(
                    topic,
                    refillTargets,
                    EpisubControl(kind: estGetNodes, peer: epi.peerInfo.peerId),
                  )
              trace "episub predictive reconnect skipped", topic, peer, reason = "no_passive"
              continue
            await epi.sendControl(
              topic,
              replacements,
              EpisubControl(kind: estNeighbor, peer: epi.peerInfo.peerId),
            )
            for peer in replacements:
              epi.recordLatencyProbe(topic, peer, lpJoin)
            await epi.sendControl(
              topic,
              replacements,
              EpisubControl(
                kind: estJoin, peer: epi.peerInfo.peerId, ttl: epi.epiParams.joinTtl
              ),
            )
      let lowWater = epi.epiParams.passiveLowWater
      if lowWater > 0 and st.passive.len < lowWater and
          epi.epiParams.passiveRefillFanout > 0:
        let cooldown = epi.epiParams.passiveRefillCooldown
        let now = Moment.now()
        if cooldown <= 0.seconds or now - st.lastPassiveRefill >= cooldown:
          let targets = epi.sampleActivePeers(st, epi.epiParams.passiveRefillFanout)
          if targets.len > 0:
            st.lastPassiveRefill = now
            for peer in targets:
              epi.recordLatencyProbe(topic, peer, lpGetNodes)
            await epi.sendControl(
              topic,
              targets,
              EpisubControl(kind: estGetNodes, peer: epi.peerInfo.peerId),
            )
      if epi.epiParams.randomWalkLength > 0:
        await epi.performRandomWalk(topic)
    await sleepAsync(epi.epiParams.maintenanceInterval)

{.push warning[UseBase]: off.}

method start*(epi: Episub): Future[void] {.async: (raises: [CancelledError]).} =
  await procCall GossipSub(epi).start()
  if epi.maintenanceLoop.isNil or epi.maintenanceLoop.finished:
    epi.maintenanceLoop = maintainViews(epi)

method stop*(epi: Episub): Future[void] {.async: (raises: []).} =
  await procCall GossipSub(epi).stop()
  if not epi.maintenanceLoop.isNil:
    epi.maintenanceLoop.cancelSoon()
    epi.maintenanceLoop = nil

{.pop.}

proc new*(
    T: type Episub,
    switch: Switch,
    params: GossipSubParams = GossipSubParams.init(),
    epiParams: EpisubParams = defaultParams(),
): T {.raises: [InitializationError].} =
  var gsParams = params
  if not gsParams.enablePX:
    gsParams.enablePX = true
    trace "episub enabled px integration by default"

  let router = Episub(
    switch: switch,
    peerInfo: switch.peerInfo,
    parameters: gsParams,
    epiParams: epiParams,
    state: initTable[string, EpisubState](),
    pxRecords: initTable[PeerId, uint64](),
  )

  router.routingRecordsHandler.add(
    proc(_: PeerId, topic: string, peers: seq[RoutingRecordsPair]) {.gcsafe.} =
      router.handlePeerExchange(topic, peers)
  )

  router.initPubSub()
  router
