# Nim-LibP2P
# Copyright (c) 2023 Status Research & Development GmbH
# Licensed under either of
#  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
#  * MIT license ([LICENSE-MIT](LICENSE-MIT))
# at your option.
# This file may not be copied, modified, or distributed except according to
# those terms.

{.push raises: [].}

import std/[tables, sequtils, sets]
import pkg/[chronos, chronicles, metrics]
import utils/semaphore as lpSemaphore
import
  peerinfo,
  peerstore,
  stream/connection,
  muxers/muxer,
  errors,
  resourcemanager

logScope:
  topics = "libp2p connmanager"

declareGauge(libp2p_peers, "total connected peers")

const
  MaxConnections* = 50
  MaxConnectionsPerPeer* = 1

type
  TooManyConnectionsError* = object of LPError
  AlreadyExpectingConnectionError* = object of LPError

  PeerPlaneKind* {.pure.} = enum
    ppkLive
    ppkControl

  PeerPlaneState* = object
    ownerMuxerId*: string
    generation*: uint64
    direction*: Direction
    ready*: bool
    busyReason*: string

  ConnEventKind* {.pure.} = enum
    Connected
      # A connection was made and securely upgraded - there may be
      # more than one concurrent connection thus more than one upgrade
      # event per peer.
    Disconnected
      # Peer disconnected - this event is fired once per upgrade
      # when the associated connection is terminated.

  ConnEvent* = object
    case kind*: ConnEventKind
    of ConnEventKind.Connected:
      incoming*: bool
    else:
      discard

  ConnEventHandler* = proc(peerId: PeerId, event: ConnEvent): Future[void] {.
    gcsafe, async: (raises: [CancelledError])
  .}

  PeerEventKind* {.pure.} = enum
    Left
    Joined
    Identified

  PeerEvent* = object
    case kind*: PeerEventKind
    of PeerEventKind.Joined, PeerEventKind.Identified:
      initiator*: bool
    else:
      discard

  PeerEventHandler* = proc(peerId: PeerId, event: PeerEvent): Future[void] {.
    gcsafe, async: (raises: [CancelledError])
  .}

  ConnManager* = ref object of RootObj
    maxConnsPerPeer: int
    inSema*: lpSemaphore.AsyncSemaphore
    outSema*: lpSemaphore.AsyncSemaphore
    muxed: Table[PeerId, seq[Muxer]]
    peerPlanes: Table[PeerId, array[PeerPlaneKind, PeerPlaneState]]
    connEvents: array[ConnEventKind, OrderedSet[ConnEventHandler]]
    peerEvents: array[PeerEventKind, OrderedSet[PeerEventHandler]]
    expectedConnectionsOverLimit*: Table[(PeerId, Direction), Future[Muxer]]
    peerStore*: PeerStore
    resourceManager*: ResourceManager

  ConnectionSlot* = object
    connManager: ConnManager
    direction: Direction

proc isActiveMuxer(muxer: Muxer): bool {.inline, gcsafe, raises: [].} =
  not muxer.isNil and
    not muxer.connection.isNil and
    not muxer.connection.closed and
    not muxer.connection.atEof

proc effectiveMaxConnsPerPeer(c: ConnManager): int {.inline, gcsafe, raises: [].} =
  if c.maxConnsPerPeer <= 0:
    1
  else:
    c.maxConnsPerPeer

proc liveMuxers(c: ConnManager, peerId: PeerId): seq[Muxer] {.gcsafe, raises: [].} =
  c.muxed.getOrDefault(peerId).filterIt(isActiveMuxer(it))

proc planeOwnerId(muxer: Muxer): string {.inline, gcsafe, raises: [].} =
  if muxer.isNil or muxer.connection.isNil:
    ""
  else:
    $muxer.connection.oid

proc selectMuxer*(c: ConnManager, peerId: PeerId, dir: Direction): Muxer {.gcsafe, raises: [].}
proc selectMuxer*(c: ConnManager, peerId: PeerId): Muxer {.gcsafe, raises: [].}

proc selectLivePlaneMuxer(c: ConnManager, peerId: PeerId): Muxer {.gcsafe, raises: [].} =
  var mux = c.selectMuxer(peerId, Direction.Out)
  if mux.isNil:
    mux = c.selectMuxer(peerId, Direction.In)
  mux

proc selectControlPlaneMuxer(c: ConnManager, peerId: PeerId, liveOwner: Muxer): Muxer {.gcsafe, raises: [].} =
  if liveOwner.isNil:
    return nil
  var fallback: Muxer = nil
  for mux in c.liveMuxers(peerId):
    if mux == liveOwner:
      continue
    if mux.connection.dir == Direction.Out:
      return mux
    if fallback.isNil:
      fallback = mux
  if not fallback.isNil:
    return fallback
  liveOwner

proc buildPeerPlaneState(
    previous: PeerPlaneState,
    owner: Muxer,
    busyReason: string,
): PeerPlaneState {.gcsafe, raises: [].} =
  let ownerMuxerId = planeOwnerId(owner)
  let ready = ownerMuxerId.len > 0
  result = PeerPlaneState(
    ownerMuxerId: ownerMuxerId,
    generation: previous.generation,
    direction:
      if ready and not owner.connection.isNil:
        owner.connection.dir
      else:
        previous.direction,
    ready: ready,
    busyReason:
      if ready:
        busyReason
      elif busyReason.len > 0:
        busyReason
      else:
        "no-muxer",
  )
  if previous.ownerMuxerId != result.ownerMuxerId or previous.ready != result.ready or
      previous.direction != result.direction or previous.busyReason != result.busyReason:
    inc result.generation

proc refreshPeerPlanes(c: ConnManager, peerId: PeerId) {.gcsafe, raises: [].} =
  if c.isNil or peerId.len == 0:
    return
  let previous = c.peerPlanes.getOrDefault(peerId)
  let liveOwner = c.selectLivePlaneMuxer(peerId)
  let controlOwner = c.selectControlPlaneMuxer(peerId, liveOwner)
  let controlShared =
    not controlOwner.isNil and planeOwnerId(controlOwner) == planeOwnerId(liveOwner)
  var next = previous
  next[ppkLive] = buildPeerPlaneState(
    previous[ppkLive],
    liveOwner,
    if liveOwner.isNil: "no-live-plane" else: "",
  )
  next[ppkControl] = buildPeerPlaneState(
    previous[ppkControl],
    controlOwner,
    if liveOwner.isNil:
      "no-live-plane"
    elif c.effectiveMaxConnsPerPeer() < 2 or controlShared:
      "shared-live-plane"
    else:
      "",
  )
  c.peerPlanes[peerId] = next

proc planeState*(
    c: ConnManager, peerId: PeerId, kind: PeerPlaneKind
): PeerPlaneState =
  if c.isNil or peerId.len == 0:
    return PeerPlaneState(busyReason: "invalid-peer")
  c.refreshPeerPlanes(peerId)
  c.peerPlanes.getOrDefault(peerId)[kind]

proc reservePlane*(
    c: ConnManager, peerId: PeerId, kind: PeerPlaneKind, dir: Direction
): PeerPlaneState =
  result = c.planeState(peerId, kind)
  if result.ready and result.direction != dir and result.busyReason.len == 0:
    result.busyReason = "direction-mismatch"

proc planeReady*(
    c: ConnManager, peerId: PeerId, kind: PeerPlaneKind
): bool =
  c.planeState(peerId, kind).ready

proc planeSharedWithLive*(
    c: ConnManager, peerId: PeerId, kind: PeerPlaneKind
): bool =
  c.planeState(peerId, kind).busyReason == "shared-live-plane"

proc selectPlaneMuxer*(
    c: ConnManager, peerId: PeerId, kind: PeerPlaneKind
): Muxer =
  if c.isNil or peerId.len == 0:
    return nil
  c.refreshPeerPlanes(peerId)
  let target = c.peerPlanes.getOrDefault(peerId)[kind].ownerMuxerId
  if target.len == 0:
    return nil
  for mux in c.liveMuxers(peerId):
    if planeOwnerId(mux) == target:
      return mux

proc closeMuxer(muxer: Muxer) {.async: (raises: [CancelledError]).}

proc compactPeerMuxers(c: ConnManager, peerId: PeerId): seq[Muxer] =
  let activeMuxers = c.liveMuxers(peerId)
  if activeMuxers.len > 0:
    c.muxed[peerId] = activeMuxers
  else:
    c.muxed.del(peerId)
  activeMuxers

proc evictSameDirectionMuxers(
    c: ConnManager, peerId: PeerId, liveMuxers: var seq[Muxer], dir: Direction
) =
  if c.maxConnsPerPeer <= 0:
    return

  let maxConnsPerPeer = c.effectiveMaxConnsPerPeer()
  if liveMuxers.len < maxConnsPerPeer:
    return

  var evicted: seq[Muxer]
  for existingMuxer in liveMuxers:
    if liveMuxers.len - evicted.len < maxConnsPerPeer:
      break
    if isNil(existingMuxer) or isNil(existingMuxer.connection):
      continue
    if existingMuxer.connection.dir == dir:
      evicted.add(existingMuxer)

  if evicted.len == 0:
    return

  liveMuxers = liveMuxers.filterIt(it notin evicted)
  if liveMuxers.len > 0:
    c.muxed[peerId] = liveMuxers
  else:
    c.muxed.del(peerId)

  debug "Replacing live muxers for peer",
    peerId,
    dir,
    replaced = evicted.len,
    remaining = liveMuxers.len

  for evictedMuxer in evicted:
    asyncSpawn closeMuxer(evictedMuxer)

proc newTooManyConnectionsError(): ref TooManyConnectionsError {.inline.} =
  result = newException(TooManyConnectionsError, "Too many connections")

proc new*(
    C: type ConnManager,
    maxConnsPerPeer = MaxConnectionsPerPeer,
    maxConnections = MaxConnections,
    maxIn = -1,
    maxOut = -1,
): ConnManager =
  var inSema, outSema: lpSemaphore.AsyncSemaphore
  if maxIn > 0 or maxOut > 0:
    inSema = lpSemaphore.newAsyncSemaphore(maxIn)
    outSema = lpSemaphore.newAsyncSemaphore(maxOut)
  elif maxConnections > 0:
    inSema = lpSemaphore.newAsyncSemaphore(maxConnections)
    outSema = inSema
  else:
    raiseAssert "Invalid connection counts!"

  C(
    maxConnsPerPeer: maxConnsPerPeer,
    inSema: inSema,
    outSema: outSema,
    peerPlanes: initTable[PeerId, array[PeerPlaneKind, PeerPlaneState]](),
  )

proc connCount*(c: ConnManager, peerId: PeerId): int =
  c.liveMuxers(peerId).len

proc connectedPeers*(c: ConnManager, dir: Direction): seq[PeerId] =
  var peers = newSeq[PeerId]()
  for peerId, mux in c.muxed:
    if mux.anyIt(isActiveMuxer(it) and it.connection.dir == dir):
      peers.add(peerId)
  return peers

proc getConnections*(c: ConnManager): Table[PeerId, seq[Muxer]] =
  return c.muxed

proc addConnEventHandler*(
    c: ConnManager, handler: ConnEventHandler, kind: ConnEventKind
) =
  ## Add peer event handler - handlers must not raise exceptions!
  ##
  if isNil(handler):
    return
  c.connEvents[kind].incl(handler)

proc removeConnEventHandler*(
    c: ConnManager, handler: ConnEventHandler, kind: ConnEventKind
) =
  c.connEvents[kind].excl(handler)

proc triggerConnEvent*(
    c: ConnManager, peerId: PeerId, event: ConnEvent
) {.async: (raises: [CancelledError]).} =
  try:
    trace "About to trigger connection events", peer = peerId
    if c.connEvents[event.kind].len() > 0:
      trace "triggering connection events", peer = peerId, event = $event.kind
      var connEvents: seq[Future[void]]
      for h in c.connEvents[event.kind]:
        connEvents.add(h(peerId, event))

      checkFutures(await allFinished(connEvents))
  except CancelledError as exc:
    raise exc
  except CatchableError as exc:
    warn "Exception in triggerConnEvent",
      description = exc.msg, peer = peerId, event = $event

proc addPeerEventHandler*(
    c: ConnManager, handler: PeerEventHandler, kind: PeerEventKind
) =
  ## Add peer event handler - handlers must not raise exceptions!
  ##

  if isNil(handler):
    return
  c.peerEvents[kind].incl(handler)

proc removePeerEventHandler*(
    c: ConnManager, handler: PeerEventHandler, kind: PeerEventKind
) =
  c.peerEvents[kind].excl(handler)

proc setResourceManager*(c: ConnManager, manager: ResourceManager) =
  c.resourceManager = manager

proc triggerPeerEvents*(
    c: ConnManager, peerId: PeerId, event: PeerEvent
) {.async: (raises: [CancelledError]).} =
  trace "About to trigger peer events", peer = peerId
  if c.peerEvents[event.kind].len == 0:
    return

  try:
    trace "triggering peer events", peer = peerId, event = $event

    var peerEvents: seq[Future[void]]
    for h in c.peerEvents[event.kind]:
      peerEvents.add(h(peerId, event))

    checkFutures(await allFinished(peerEvents))
  except CancelledError as exc:
    raise exc
  except CatchableError as exc: # handlers should not raise!
    warn "Exception in triggerPeerEvents", description = exc.msg, peer = peerId

proc expectConnection*(
    c: ConnManager, p: PeerId, dir: Direction
): Future[Muxer] {.async: (raises: [AlreadyExpectingConnectionError, CancelledError]).} =
  ## Wait for a peer to connect to us. This will bypass the `MaxConnectionsPerPeer`
  let key = (p, dir)
  if key in c.expectedConnectionsOverLimit:
    raise newException(
      AlreadyExpectingConnectionError,
      "Already expecting an incoming connection from that peer: " & shortLog(p),
    )

  let future = Future[Muxer].Raising([CancelledError]).init()
  c.expectedConnectionsOverLimit[key] = future

  try:
    return await future
  finally:
    c.expectedConnectionsOverLimit.del(key)

proc contains*(c: ConnManager, peerId: PeerId): bool =
  c.connCount(peerId) > 0

proc contains*(c: ConnManager, muxer: Muxer): bool =
  ## checks if a muxer is being tracked by the connection
  ## manager
  ##

  if isNil(muxer):
    return false

  let conn = muxer.connection
  if conn.isNil:
    return false
  return muxer in c.liveMuxers(conn.peerId)

proc closeMuxer(muxer: Muxer) {.async: (raises: [CancelledError]).} =
  trace "Cleaning up muxer", m = muxer

  await muxer.close()
  if not (isNil(muxer.handler)):
    try:
      await muxer.handler
    except CatchableError as exc:
      trace "Exception in close muxer handler", description = exc.msg
  trace "Cleaned up muxer", m = muxer

proc muxCleanup(c: ConnManager, mux: Muxer) {.async: (raises: []).} =
  try:
    trace "Triggering disconnect events", mux
    let peerId = mux.connection.peerId

    let muxers = c.muxed.getOrDefault(peerId).filterIt(it != mux)
    if muxers.len > 0:
      c.muxed[peerId] = muxers
    else:
      c.muxed.del(peerId)
      libp2p_peers.set(c.muxed.len.int64)
      await c.triggerPeerEvents(peerId, PeerEvent(kind: PeerEventKind.Left))

      if not (c.peerStore.isNil):
        c.peerStore.cleanup(peerId)
    c.refreshPeerPlanes(peerId)

    await c.triggerConnEvent(peerId, ConnEvent(kind: ConnEventKind.Disconnected))
  except CatchableError as exc:
    # This is top-level procedure which will work as separate task, so it
    # do not need to propagate CancelledError and should handle other errors
    warn "Unexpected exception peer cleanup handler", mux, description = exc.msg

proc onClose(c: ConnManager, mux: Muxer) {.async: (raises: []).} =
  ## connection close even handler
  ##
  ## triggers the connections resource cleanup
  ##
  try:
    await mux.connection.join()
    trace "Connection closed, cleaning up", mux
  except CatchableError as exc:
    debug "Unexpected exception in connection manager's cleanup",
      description = exc.msg, mux
  finally:
    await c.muxCleanup(mux)

proc selectMuxer*(c: ConnManager, peerId: PeerId, dir: Direction): Muxer {.gcsafe, raises: [].} =
  ## Select a connection for the provided peer and direction
  ##
  let conns = c.liveMuxers(peerId).filterIt(it.connection.dir == dir)

  if conns.len > 0:
    return conns[0]

proc selectMuxer*(c: ConnManager, peerId: PeerId): Muxer {.gcsafe, raises: [].} =
  ## Select a connection for the provided giving priority
  ## to outgoing connections
  ##

  var mux = c.selectMuxer(peerId, Direction.Out)
  if isNil(mux):
    mux = c.selectMuxer(peerId, Direction.In)
  if isNil(mux):
    trace "connection not found", peerId
  return mux

proc selectAlternateMuxer*(
    c: ConnManager, peerId: PeerId, reserved: Muxer
): Muxer =
  ## Select a live muxer different from the reserved one.
  ##
  for mux in c.liveMuxers(peerId):
    if mux != reserved:
      return mux

proc storeMuxer*(c: ConnManager, muxer: Muxer) {.raises: [LPError, TooManyConnectionsError].} =
  ## store the connection and muxer
  ##

  if isNil(muxer):
    raise newException(LPError, "muxer cannot be nil")

  if isNil(muxer.connection):
    raise newException(LPError, "muxer's connection cannot be nil")

  if muxer.connection.closed or muxer.connection.atEof:
    raise newException(LPError, "Connection closed or EOF")

  let
    peerId = muxer.connection.peerId
    dir = muxer.connection.dir
    maxConnsPerPeer = c.effectiveMaxConnsPerPeer()
  var liveMuxers = c.compactPeerMuxers(peerId)
  let hadLivePeer = liveMuxers.len > 0

  if muxer in liveMuxers:
    debug "Duplicate muxer registration skipped", peerId, dir
    return

  var permit: ResourcePermit = nil
  if not c.resourceManager.isNil:
    let permitRes = c.resourceManager.acquireConnection(peerId, dir)
    if permitRes.isErr:
      raise newResourceLimitError(permitRes.error)
    permit = permitRes.get()

  # we use getOrDefault in the if below instead of [] to avoid the KeyError
  try:
    if peerId.len > 0 and liveMuxers.len >= maxConnsPerPeer:
      let key = (peerId, dir)
      let expectedConn = c.expectedConnectionsOverLimit.getOrDefault(key)
      if expectedConn != nil and not expectedConn.finished:
        expectedConn.complete(muxer)
      else:
        c.evictSameDirectionMuxers(peerId, liveMuxers, dir)
        if liveMuxers.len >= maxConnsPerPeer:
          debug "Too many connections for peer",
            conns = liveMuxers.len, peerId, dir

          raise newTooManyConnectionsError()

    liveMuxers.add(muxer)
    c.muxed[peerId] = liveMuxers
    c.refreshPeerPlanes(peerId)
    libp2p_peers.set(c.muxed.len.int64)

    asyncSpawn c.triggerConnEvent(
      peerId, ConnEvent(kind: ConnEventKind.Connected, incoming: dir == Direction.In)
    )

    if not hadLivePeer:
      asyncSpawn c.triggerPeerEvents(
        peerId, PeerEvent(kind: PeerEventKind.Joined, initiator: dir == Direction.Out)
      )
  except TooManyConnectionsError as err:
    if not permit.isNil:
      permit.release()
    raise err
  except LPError as err:
    if not permit.isNil:
      permit.release()
    raise err

  if not permit.isNil:
    c.resourceManager.attachPermit(permit, muxer.connection)

  asyncSpawn c.onClose(muxer)

  trace "Stored muxer", muxer, direction = $muxer.connection.dir, peers = c.muxed.len

proc reindexMuxerPeerId*(
    c: ConnManager, muxer: Muxer, previousPeerId: PeerId, currentPeerId: PeerId
) =
  ## Move an already tracked muxer to its resolved peer id after identify/secure
  ## upgrade finishes. This fixes the case where transports temporarily register a
  ## muxer under an empty/placeholder peer id before the remote identity becomes
  ## known.
  if isNil(c) or isNil(muxer) or isNil(muxer.connection):
    return
  if currentPeerId.len == 0 or previousPeerId == currentPeerId:
    return

  var removed = false
  c.muxed.withValue(previousPeerId, muxers):
    let remaining = muxers[].filterIt(isActiveMuxer(it) and it != muxer)
    if remaining.len > 0:
      muxers[] = remaining
    else:
      c.muxed.del(previousPeerId)
    removed = true
  do:
    discard

  if not removed:
    return

  if currentPeerId.len > 0:
    var currentLiveMuxers = c.compactPeerMuxers(currentPeerId)
    if muxer notin currentLiveMuxers and
        currentLiveMuxers.len >= c.effectiveMaxConnsPerPeer():
      c.evictSameDirectionMuxers(
        currentPeerId,
        currentLiveMuxers,
        muxer.connection.dir
      )

  c.muxed.withValue(currentPeerId, muxers):
    let activeMuxers = muxers[].filterIt(isActiveMuxer(it))
    muxers[] = activeMuxers
    if muxer notin muxers[]:
      muxers[].add(muxer)
  do:
    c.muxed[currentPeerId] = @[muxer]

  c.refreshPeerPlanes(previousPeerId)
  c.refreshPeerPlanes(currentPeerId)
  for stream in muxer.getStreams():
    stream.syncConnectionIdentity(muxer.connection)

  libp2p_peers.set(c.muxed.len.int64)
  trace "Reindexed muxer peer id",
    previousPeerId,
    currentPeerId,
    direction = $muxer.connection.dir,
    peers = c.muxed.len

proc getIncomingSlot*(
    c: ConnManager
): Future[ConnectionSlot] {.async: (raises: [CancelledError]).} =
  await c.inSema.acquire()
  return ConnectionSlot(connManager: c, direction: In)

proc getOutgoingSlot*(
    c: ConnManager, forceDial = false
): ConnectionSlot {.raises: [TooManyConnectionsError].} =
  if forceDial:
    c.outSema.forceAcquire()
  elif not c.outSema.tryAcquire():
    trace "Too many outgoing connections!",
      available = c.outSema.count, max = c.outSema.size
    raise newTooManyConnectionsError()
  return ConnectionSlot(connManager: c, direction: Out)

func semaphore(c: ConnManager, dir: Direction): lpSemaphore.AsyncSemaphore {.inline.} =
  return if dir == In: c.inSema else: c.outSema

proc slotsAvailable*(c: ConnManager, dir: Direction): int =
  return semaphore(c, dir).count

proc release*(cs: ConnectionSlot) =
  semaphore(cs.connManager, cs.direction).release()

proc trackConnection*(cs: ConnectionSlot, conn: Connection) =
  if isNil(conn):
    cs.release()
    return

  proc semaphoreMonitor() {.async: (raises: [CancelledError]).} =
    try:
      await conn.join()
    except CatchableError as exc:
      trace "Exception in semaphore monitor, ignoring", description = exc.msg

    cs.release()

  asyncSpawn semaphoreMonitor()

proc trackMuxer*(cs: ConnectionSlot, mux: Muxer) =
  if isNil(mux):
    cs.release()
    return
  cs.trackConnection(mux.connection)

proc getStream*(
    c: ConnManager, muxer: Muxer
): Future[Connection] {.async: (raises: [LPStreamError, MuxerError, CancelledError]).} =
  ## get a muxed stream for the passed muxer
  ##

  if not (isNil(muxer)):
    return await muxer.newStream()

proc getStream*(
    c: ConnManager, peerId: PeerId
): Future[Connection] {.async: (raises: [LPStreamError, MuxerError, CancelledError]).} =
  ## get a muxed stream for the passed peer from any connection
  ##

  return await c.getStream(c.selectMuxer(peerId))

proc getStream*(
    c: ConnManager, peerId: PeerId, dir: Direction
): Future[Connection] {.async: (raises: [LPStreamError, MuxerError, CancelledError]).} =
  ## get a muxed stream for the passed peer from a connection with `dir`
  ##

  return await c.getStream(c.selectMuxer(peerId, dir))

proc dropPeer*(c: ConnManager, peerId: PeerId) {.async: (raises: [CancelledError]).} =
  ## drop connections and cleanup resources for peer
  ##
  trace "Dropping peer", peerId
  let muxers = c.muxed.getOrDefault(peerId)

  for muxer in muxers:
    await closeMuxer(muxer)

  trace "Peer dropped", peerId

proc close*(c: ConnManager) {.async: (raises: [CancelledError]).} =
  ## cleanup resources for the connection
  ## manager
  ##

  trace "Closing ConnManager"
  let muxed = c.muxed
  c.muxed.clear()
  c.peerPlanes.clear()

  let expected = c.expectedConnectionsOverLimit
  c.expectedConnectionsOverLimit.clear()

  for _, fut in expected:
    await fut.cancelAndWait()

  for _, muxers in muxed:
    for mux in muxers:
      await closeMuxer(mux)

  trace "Closed ConnManager"
