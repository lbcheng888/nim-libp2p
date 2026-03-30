# Nim-LibP2P
# Copyright (c) 2023 Status Research & Development GmbH
# Licensed under either of
#  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
#  * MIT license ([LICENSE-MIT](LICENSE-MIT))
# at your option.
# This file may not be copied, modified, or distributed except according to
# those terms.

## Stores generic informations about peers.
runnableExamples:
  # Will keep info of all connected peers +
  # last 50 disconnected peers
  let peerStore = PeerStore.new(capacity = 50)

  # Create a custom book type
  type MoodBook = ref object of PeerBook[string]

  var somePeerId = PeerId.random().expect("get random key")

  peerStore[MoodBook][somePeerId] = "Happy"
  doAssert peerStore[MoodBook][somePeerId] == "Happy"

{.push raises: [].}

import
  std/[algorithm, tables, sets, options, macros, sequtils, strutils],
  chronos,
  pkg/results,
  ../shim,
  ./crypto/crypto,
  ./protocols/identify,
  ./protocols/protocol,
  ./peerid,
  ./peerinfo,
  ./lsmr,
  ./routing_record,
  ./multiaddress,
  ./stream/connection,
  ./multistream,
  ./muxers/muxer,
  ./bandwidthmanager,
  utility

type
  #################
  # Handler types #
  #################
  PeerBookChangeHandler* = proc(peerId: PeerId) {.gcsafe, raises: [].}

  #########
  # Books #
  #########

  # Each book contains a book (map) and event handler(s)
  BasePeerBook = ref object of RootObj
    changeHandlers: seq[PeerBookChangeHandler]
    deletor: PeerBookChangeHandler

  PeerBook*[T] {.public.} = ref object of BasePeerBook
    book*: Table[PeerId, T]

  SeqPeerBook*[T] = ref object of PeerBook[seq[T]]

  AddressBook* {.public.} = ref object of SeqPeerBook[MultiAddress]
  ProtoBook* {.public.} = ref object of SeqPeerBook[string]
  KeyBook* {.public.} = ref object of PeerBook[PublicKey]

  AgentBook* {.public.} = ref object of PeerBook[string]
  LastSeenBook* {.public.} = ref object of PeerBook[Opt[MultiAddress]]
  ProtoVersionBook* {.public.} = ref object of PeerBook[string]
  SPRBook* {.public.} = ref object of PeerBook[Envelope]
  LsmrBook* {.public.} = ref object of PeerBook[SignedLsmrCoordinateRecord]
  LsmrChainBook* {.public.} = ref object of SeqPeerBook[SignedLsmrCoordinateRecord]
  ActiveLsmrBook* {.public.} = ref object of PeerBook[SignedLsmrCoordinateRecord]
  LsmrMigrationBook* {.public.} = ref object of SeqPeerBook[SignedLsmrMigrationRecord]
  LsmrIsolationBook* {.public.} = ref object of SeqPeerBook[SignedLsmrIsolationEvidence]
  MetadataBook* {.public.} = ref object of PeerBook[Table[string, seq[byte]]]
  BandwidthBook* {.public.} = ref object of PeerBook[BandwidthSnapshot]

  ####################
  # Peer store types #
  ####################
  PeerStore* {.public.} = ref object
    books: Table[string, BasePeerBook]
    identify: Identify
    capacity*: int
    toClean*: seq[PeerId]
    addressExpirations: Table[PeerId, Table[MultiAddress, Moment]]
    identifyLocks: Table[uint64, AsyncLock]

const DefaultDiscoveredAddressTTL* = chronos.minutes(30)

proc new*(
    T: type PeerStore, identify: Identify = nil, capacity = 1000
): PeerStore {.public.} =
  T(
    identify: identify,
    capacity: capacity,
    addressExpirations: initTable[PeerId, Table[MultiAddress, Moment]](),
    identifyLocks: initTable[uint64, AsyncLock](),
  )

#########################
# Generic Peer Book API #
#########################

proc `[]`*[T](peerBook: PeerBook[T], peerId: PeerId): T {.public.} =
  ## Get all known metadata of a provided peer, or default(T) if missing
  peerBook.book.getOrDefault(peerId)

proc `[]=`*[T](peerBook: PeerBook[T], peerId: PeerId, entry: T) {.public.} =
  ## Set metadata for a given peerId.

  peerBook.book[peerId] = entry

  # Notify clients
  for handler in peerBook.changeHandlers:
    handler(peerId)

proc del*[T](peerBook: PeerBook[T], peerId: PeerId): bool {.public.} =
  ## Delete the provided peer from the book. Returns whether the peer was in the book

  if peerId notin peerBook.book:
    return false
  else:
    peerBook.book.del(peerId)
    # Notify clients
    for handler in peerBook.changeHandlers:
      handler(peerId)
    return true

proc contains*[T](peerBook: PeerBook[T], peerId: PeerId): bool {.public.} =
  peerId in peerBook.book

proc addHandler*[T](peerBook: PeerBook[T], handler: PeerBookChangeHandler) {.public.} =
  ## Adds a callback that will be called everytime the book changes
  peerBook.changeHandlers.add(handler)

proc len*[T](peerBook: PeerBook[T]): int {.public.} =
  peerBook.book.len

proc ensureUnique[T](items: var seq[T], value: T) =
  for existing in items:
    if existing == value:
      return
  items.add(value)

proc compareCertifiedRecords(
    aPeer, bPeer: PeerId,
    aRecord, bRecord: SignedLsmrCoordinateRecord,
    targetPath: LsmrPath,
): int =
  let aPrefix = aRecord.data.certifiedPrefix()
  let bPrefix = bRecord.data.certifiedPrefix()
  let aShared = commonPrefixLen(aPrefix, targetPath)
  let bShared = commonPrefixLen(bPrefix, targetPath)
  result = cmp(bShared, aShared)
  if result != 0:
    return
  let aDivergence = divergenceDistance(aPrefix, targetPath)
  let bDivergence = divergenceDistance(bPrefix, targetPath)
  result = cmp(aDivergence, bDivergence)
  if result != 0:
    return
  let aDistance =
    if aPrefix.len == 0 or targetPath.len == 0:
      high(int)
    else:
      lsmrDistance(aPrefix, targetPath)
  let bDistance =
    if bPrefix.len == 0 or targetPath.len == 0:
      high(int)
    else:
      lsmrDistance(bPrefix, targetPath)
  result = cmp(aDistance, bDistance)
  if result != 0:
    return
  result = cmp(bRecord.data.certifiedDepthValue(), aRecord.data.certifiedDepthValue())
  if result != 0:
    return
  result = cmp(countCoverageBits(bRecord.data.coverageMask), countCoverageBits(aRecord.data.coverageMask))
  if result != 0:
    return
  result = cmp(int(bRecord.data.confidence), int(aRecord.data.confidence))
  if result != 0:
    return
  result = cmp($aPeer, $bPeer)

macro getTypeName(t: type): untyped =
  # Generate unique name in form of Module.Type
  let typ = getTypeImpl(t)[1]
  newLit(repr(typ.owner()) & "." & repr(typ))

proc `[]`*[T: BasePeerBook](p: PeerStore, typ: type[T]): T {.public.} =
  ## Get a book from the PeerStore (ex: peerStore[AddressBook])
  let name = getTypeName(T)
  result = cast[T](p.books.getOrDefault(name))
  if result.isNil:
    result = T.new()
    result.deletor = proc(pid: PeerId) =
      discard cast[T](p.books.getOrDefault(name)).del(pid)
    p.books[name] = result

proc compareNeighborPeers(
    peerStore: PeerStore,
    aPeer, bPeer: PeerId,
    targetPath: LsmrPath,
): int =
  if peerStore.isNil or not peerStore[ActiveLsmrBook].contains(aPeer) or
      not peerStore[ActiveLsmrBook].contains(bPeer):
    return cmp($aPeer, $bPeer)
  compareCertifiedRecords(
    aPeer,
    bPeer,
    peerStore[ActiveLsmrBook][aPeer],
    peerStore[ActiveLsmrBook][bPeer],
    targetPath,
  )

proc closestCertifiedPeers*(
    peerStore: PeerStore, targetPath: LsmrPath, limit = 0
): seq[PeerId] {.public.} =
  if peerStore.isNil or targetPath.len == 0:
    return @[]
  var ranked: seq[(PeerId, SignedLsmrCoordinateRecord)] = @[]
  for peerId, record in peerStore[ActiveLsmrBook].book.pairs:
    ranked.add((peerId, record))
  ranked.sort(
    proc(a, b: (PeerId, SignedLsmrCoordinateRecord)): int =
      compareCertifiedRecords(a[0], b[0], a[1], b[1], targetPath)
  )
  let resolvedLimit =
    if limit > 0:
      min(limit, ranked.len)
    else:
      ranked.len
  for idx in 0 ..< resolvedLimit:
    result.add(ranked[idx][0])

proc bestCertifiedPeer(
    peerStore: PeerStore, peerIds: openArray[PeerId], targetPath: LsmrPath
): Opt[PeerId] =
  if peerStore.isNil or targetPath.len == 0:
    return Opt.none(PeerId)
  var ranked: seq[(PeerId, SignedLsmrCoordinateRecord)] = @[]
  for peerId in peerIds:
    if peerStore[ActiveLsmrBook].contains(peerId):
      ranked.add((peerId, peerStore[ActiveLsmrBook][peerId]))
  if ranked.len == 0:
    return Opt.none(PeerId)
  ranked.sort(
    proc(a, b: (PeerId, SignedLsmrCoordinateRecord)): int =
      compareCertifiedRecords(a[0], b[0], a[1], b[1], targetPath)
  )
  Opt.some(ranked[0][0])

proc bestNeighborPeer(
    peerStore: PeerStore, peerIds: openArray[PeerId], targetPath: LsmrPath
): Opt[PeerId] =
  if peerStore.isNil or targetPath.len == 0:
    return Opt.none(PeerId)
  var peers: seq[PeerId] = @[]
  for peerId in peerIds:
    if peerStore[ActiveLsmrBook].contains(peerId):
      peers.add(peerId)
  if peers.len == 0:
    return Opt.none(PeerId)
  peers.sort(proc(a, b: PeerId): int = peerStore.compareNeighborPeers(a, b, targetPath))
  Opt.some(peers[0])

proc bestExactPrefixPeer(
    peerStore: PeerStore, peerIds: openArray[PeerId], targetPath: LsmrPath
): Opt[PeerId] =
  if peerStore.isNil or targetPath.len == 0:
    return Opt.none(PeerId)
  var peers: seq[PeerId] = @[]
  for peerId in peerIds:
    if not peerStore[ActiveLsmrBook].contains(peerId):
      continue
    if peerStore[ActiveLsmrBook][peerId].data.certifiedPrefix() == targetPath:
      peers.add(peerId)
  if peers.len == 0:
    return Opt.none(PeerId)
  peers.sort(proc(a, b: PeerId): int = peerStore.compareNeighborPeers(a, b, targetPath))
  Opt.some(peers[0])

proc neighborsForPrefix*(
    peerStore: PeerStore, prefix: LsmrPath, depth: int
): seq[PeerId] {.public.} =
  if peerStore.isNil or prefix.len == 0 or depth <= 0:
    return @[]
  let required = min(prefix.len, depth)
  for peerId, record in peerStore[ActiveLsmrBook].book.pairs:
    if commonPrefixLen(record.data.certifiedPrefix(), prefix) >= required:
      result.add(peerId)
  result.sort(proc(a, b: PeerId): int = peerStore.compareNeighborPeers(a, b, prefix))

proc topologyNeighborView*(
    peerStore: PeerStore,
    localPeerId: PeerId,
    depth = 0,
    perDirection = 1,
): Opt[TopologyNeighborView] {.public.}

proc routeNextHop*(
    peerStore: PeerStore, localPeerId: PeerId, targetPath: LsmrPath
): Opt[PeerId] {.public.} =
  if peerStore.isNil or targetPath.len == 0 or
      not peerStore[ActiveLsmrBook].contains(localPeerId):
    return Opt.none(PeerId)
  let localPrefix = peerStore[ActiveLsmrBook][localPeerId].data.certifiedPrefix()
  var depth = min(localPrefix.len, targetPath.len)
  while depth > 0:
    let viewOpt = peerStore.topologyNeighborView(localPeerId, depth = depth)
    if viewOpt.isNone():
      dec depth
      continue
    let view = viewOpt.get()
    let shared = commonPrefixLen(view.selfPrefix, targetPath)
    when defined(lsmr_diag):
      echo "lsmr next-hop depth=", depth,
        " local=", $localPeerId,
        " target=", targetPath,
        " selfPrefix=", view.selfPrefix,
        " sameCell=", view.sameCellPeers.mapIt($it).join(","),
        " parents=", view.parentPrefixPeers.mapIt($it).join(",")

    if shared == depth - 1 and targetPath.len >= depth:
      let targetDigit = targetPath[depth - 1]
      for bucket in view.directionalPeers:
        if bucket.directionDigit == targetDigit:
          when defined(lsmr_diag):
            echo "lsmr next-hop bucket depth=", depth,
              " local=", $localPeerId,
              " target=", targetPath,
              " digit=", targetDigit,
              " peers=", bucket.peers.mapIt($it).join(",")
          let nextHop = peerStore.bestNeighborPeer(bucket.peers, targetPath)
          if nextHop.isSome():
            return nextHop

    if shared >= depth:
      let exactHop = peerStore.bestExactPrefixPeer(view.sameCellPeers, targetPath)
      when defined(lsmr_diag):
        echo "lsmr next-hop exact depth=", depth,
          " local=", $localPeerId,
          " target=", targetPath,
          " next=", (if exactHop.isSome(): $exactHop.get() else: "none")
      if exactHop.isSome():
        return exactHop
      let nextHop = peerStore.bestNeighborPeer(view.sameCellPeers, targetPath)
      when defined(lsmr_diag):
        echo "lsmr next-hop same depth=", depth,
          " local=", $localPeerId,
          " target=", targetPath,
          " next=", (if nextHop.isSome(): $nextHop.get() else: "none")
      if nextHop.isSome():
        return nextHop

    if depth > 1:
      let parentHop = peerStore.bestNeighborPeer(view.parentPrefixPeers, targetPath)
      when defined(lsmr_diag):
        echo "lsmr next-hop parent depth=", depth,
          " local=", $localPeerId,
          " target=", targetPath,
          " next=", (if parentHop.isSome(): $parentHop.get() else: "none")
      if parentHop.isSome():
        return parentHop

    dec depth
  Opt.none(PeerId)

proc topologyNeighborView*(
    peerStore: PeerStore,
    localPeerId: PeerId,
    depth = 0,
    perDirection = 1,
): Opt[TopologyNeighborView] {.public.} =
  if peerStore.isNil or not peerStore[ActiveLsmrBook].contains(localPeerId):
    return Opt.none(TopologyNeighborView)
  let localRecord = peerStore[ActiveLsmrBook][localPeerId]
  let localPrefix = localRecord.data.certifiedPrefix()
  if localPrefix.len == 0:
    return Opt.none(TopologyNeighborView)

  let targetDepth =
    if depth > 0:
      min(depth, localPrefix.len)
    else:
      localPrefix.len
  var view = TopologyNeighborView(
    selfPrefix: localPrefix[0 ..< targetDepth],
    certifiedDepth: uint8(targetDepth),
    sameCellPeers: @[],
    parentPrefixPeers: @[],
    directionalPeers: @[],
  )
  let directionLimit = max(1, perDirection)

  proc bucketIndex(directionDigit: uint8): int =
    for idx in 0 ..< view.directionalPeers.len:
      if view.directionalPeers[idx].directionDigit == directionDigit:
        return idx
    -1

  for peerId, record in peerStore[ActiveLsmrBook].book.pairs:
    if peerId == localPeerId:
      continue
    let remotePrefix = record.data.certifiedPrefix()

    let shared = commonPrefixLen(remotePrefix, view.selfPrefix)
    if remotePrefix.len >= targetDepth and shared >= targetDepth:
      view.sameCellPeers.add(peerId)
      continue

    if targetDepth > 1 and remotePrefix.len == targetDepth - 1 and
        pathStartsWith(view.selfPrefix, remotePrefix):
      view.parentPrefixPeers.add(peerId)
      continue

    if remotePrefix.len < targetDepth:
      continue

    if targetDepth > 1 and shared >= targetDepth - 1:
      let directionDigit = remotePrefix[targetDepth - 1]
      if directionDigit == view.selfPrefix[targetDepth - 1]:
        continue
      let idx = bucketIndex(directionDigit)
      if idx < 0:
        view.directionalPeers.add(
          TopologyNeighborBucket(directionDigit: directionDigit, peers: @[peerId])
        )
      elif view.directionalPeers[idx].peers.len < directionLimit:
        view.directionalPeers[idx].peers.add(peerId)

  view.sameCellPeers.sort(
    proc(a, b: PeerId): int = peerStore.compareNeighborPeers(a, b, view.selfPrefix)
  )
  view.parentPrefixPeers.sort(
    proc(a, b: PeerId): int = peerStore.compareNeighborPeers(a, b, view.selfPrefix)
  )
  for idx in 0 ..< view.directionalPeers.len:
    view.directionalPeers[idx].peers.sort(
      proc(a, b: PeerId): int = peerStore.compareNeighborPeers(a, b, view.selfPrefix)
    )
    view.directionalPeers[idx].peers.setLen(
      min(view.directionalPeers[idx].peers.len, directionLimit)
    )
  view.directionalPeers.sort(proc(a, b: TopologyNeighborBucket): int =
    cmp(a.directionDigit, b.directionDigit)
  )

  Opt.some(view)

##################
# Peer Store API #
##################

proc del*(peerStore: PeerStore, peerId: PeerId) {.public.} =
  ## Delete the provided peer from every book.
  for _, book in peerStore.books:
    book.deletor(peerId)
  if peerStore.addressExpirations.hasKey(peerId):
    peerStore.addressExpirations.del(peerId)

proc clearAddressTtl(peerStore: PeerStore, peerId: PeerId) =
  if peerStore.addressExpirations.hasKey(peerId):
    peerStore.addressExpirations.del(peerId)

proc setAddresses*(
    peerStore: PeerStore, peerId: PeerId, addresses: seq[MultiAddress]
) {.public.} =
  ## Replace peer addresses without TTL (permanent entry).
  peerStore[AddressBook][peerId] = addresses
  peerStore.clearAddressTtl(peerId)

proc addAddressesWithTTL*(
    peerStore: PeerStore,
    peerId: PeerId,
    addresses: openArray[MultiAddress],
    ttl: Duration,
    now: Moment = Moment.now(),
) {.public.} =
  ## Add or update addresses for a peer with a time-to-live window.
  if ttl <= chronos.ZeroDuration or addresses.len == 0:
    return

  var ttlMap =
    peerStore.addressExpirations.getOrDefault(
      peerId, initTable[MultiAddress, Moment]()
    )
  var current = peerStore[AddressBook][peerId]
  var changed = false
  for addr in addresses:
    let expires = now + ttl
    ttlMap[addr] = expires
    var exists = false
    for existing in current:
      if existing == addr:
        exists = true
        break
    if not exists:
      current.add(addr)
      changed = true

  peerStore.addressExpirations[peerId] = ttlMap
  if changed:
    peerStore[AddressBook][peerId] = current

proc addAddressWithTTL*(
    peerStore: PeerStore,
    peerId: PeerId,
    address: MultiAddress,
    ttl: Duration,
    now: Moment = Moment.now(),
) {.public.} =
  peerStore.addAddressesWithTTL(peerId, [address], ttl, now)

proc pruneExpiredAddresses(
    peerStore: PeerStore, peerId: PeerId, now: Moment
) =
  if not peerStore.addressExpirations.hasKey(peerId):
    return

  peerStore.addressExpirations.withValue(peerId, ttlMap):
    if ttlMap[].len() == 0:
      peerStore.addressExpirations.del(peerId)
      return

    var addresses = peerStore[AddressBook][peerId]
    if addresses.len == 0:
      peerStore.addressExpirations.del(peerId)
      return

    var filtered: seq[MultiAddress] = @[]
    var removed = false
    for addr in addresses:
      let expiresAt = ttlMap[].getOrDefault(addr, Moment.high())
      if expiresAt <= now:
        if ttlMap[].hasKey(addr):
          ttlMap[].del(addr)
        removed = true
        continue
      filtered.add(addr)

    if filtered.len == 0:
      discard peerStore[AddressBook].del(peerId)
    elif removed or filtered.len != addresses.len:
      peerStore[AddressBook][peerId] = filtered

    if ttlMap[].len == 0:
      peerStore.addressExpirations.del(peerId)

proc getAddresses*(
    peerStore: PeerStore,
    peerId: PeerId,
    pruneExpired = true,
    now: Moment = Moment.now(),
): seq[MultiAddress] {.public.} =
  ## Retrieve known addresses for the peer, optionally pruning expired entries first.
  if pruneExpired:
    peerStore.pruneExpiredAddresses(peerId, now)
  peerStore[AddressBook][peerId]

proc updatePeerInfo*(
    peerStore: PeerStore,
    info: IdentifyInfo,
    observedAddr: Opt[MultiAddress] = Opt.none(MultiAddress),
) =
  if len(info.addrs) > 0:
    peerStore.setAddresses(info.peerId, info.addrs)

  peerStore[LastSeenBook][info.peerId] = observedAddr

  info.pubkey.withValue(pubkey):
    peerStore[KeyBook][info.peerId] = pubkey

  info.agentVersion.withValue(agentVersion):
    peerStore[AgentBook][info.peerId] = agentVersion

  info.protoVersion.withValue(protoVersion):
    peerStore[ProtoVersionBook][info.peerId] = protoVersion

  if info.protos.len > 0:
    peerStore[ProtoBook][info.peerId] = info.protos

  info.signedPeerRecord.withValue(signedPeerRecord):
    peerStore[SPRBook][info.peerId] = signedPeerRecord

  if info.metadata.len > 0:
    peerStore[MetadataBook][info.peerId] = info.metadata
    if info.metadata.hasKey(LsmrCoordinateMetadataKey):
      let encoded = info.metadata.getOrDefault(LsmrCoordinateMetadataKey)
      let decoded = SignedLsmrCoordinateRecord.decode(encoded)
      if decoded.isOk():
        peerStore[LsmrBook][info.peerId] = decoded.get()

  info.bandwidthAnnouncement.withValue(bandwidth):
    peerStore[BandwidthBook][info.peerId] = bandwidth

  let cleanupPos = peerStore.toClean.find(info.peerId)
  if cleanupPos >= 0:
    peerStore.toClean.delete(cleanupPos)

proc cleanup*(peerStore: PeerStore, peerId: PeerId) =
  if peerStore.capacity == 0:
    peerStore.del(peerId)
    return
  elif peerStore.capacity < 0:
    #infinite capacity
    return

  peerStore.toClean.add(peerId)
  while peerStore.toClean.len > peerStore.capacity:
    peerStore.del(peerStore.toClean[0])
    peerStore.toClean.delete(0)

proc identifyTaskKey(muxer: Muxer): uint64 {.inline.} =
  if muxer.isNil or muxer.connection.isNil:
    return 0'u64
  cast[uint64](muxer.connection)

proc getOrCreateIdentifyLock(peerStore: PeerStore, muxer: Muxer): AsyncLock =
  let taskKey = identifyTaskKey(muxer)
  if taskKey == 0'u64:
    return nil
  result = peerStore.identifyLocks.getOrDefault(taskKey)
  if result.isNil:
    result = newAsyncLock()
    peerStore.identifyLocks[taskKey] = result

proc hasIdentifySnapshot(peerStore: PeerStore, peerId: PeerId): bool =
  if peerStore.isNil or peerId.len == 0:
    return false
  try:
    if peerStore[ProtoBook][peerId].len > 0:
      return true
    if peerStore[AgentBook][peerId].len > 0:
      return true
    if peerStore[ProtoVersionBook][peerId].len > 0:
      return true
  except CatchableError:
    discard
  false

proc identifyImpl(
    peerStore: PeerStore, muxer: Muxer
) {.
    async: (
      raises: [
        CancelledError, IdentityNoMatchError, IdentityInvalidMsgError, MultiStreamError,
        LPStreamError, MuxerError,
      ]
    )
.} =
  # new stream for identify
  warn "peerStore.identify newStream begin",
    peerId = muxer.connection.peerId,
    protocol = muxer.connection.protocol,
    negotiated = muxer.connection.negotiatedMuxer
  var stream = await muxer.newStream("identify")
  if stream == nil:
    warn "peerStore.identify newStream nil",
      peerId = muxer.connection.peerId,
      protocol = muxer.connection.protocol,
      negotiated = muxer.connection.negotiatedMuxer
    return
  warn "peerStore.identify newStream done",
    peerId = muxer.connection.peerId,
    streamPeerId = stream.peerId,
    protocol = stream.protocol,
    negotiated = stream.negotiatedMuxer

  try:
    warn "peerStore.identify beginProtocolNegotiation begin",
      peerId = muxer.connection.peerId,
      streamPeerId = stream.peerId,
      codec = peerStore.identify.codec()
    await stream.beginProtocolNegotiation()
    warn "peerStore.identify beginProtocolNegotiation done",
      peerId = muxer.connection.peerId,
      streamPeerId = stream.peerId,
      codec = peerStore.identify.codec()
    warn "peerStore.identify multistream select begin",
      peerId = muxer.connection.peerId,
      streamPeerId = stream.peerId,
      codec = peerStore.identify.codec()
    if (await MultistreamSelect.select(stream, peerStore.identify.codec())):
      warn "peerStore.identify multistream select done",
        peerId = muxer.connection.peerId,
        streamPeerId = stream.peerId,
        codec = peerStore.identify.codec()
      let info = await peerStore.identify.identify(stream, stream.peerId)
      warn "peerStore.identify identify payload done",
        peerId = info.peerId,
        addrs = info.addrs.len,
        protos = info.protos.len
      if not muxer.isNil and not muxer.connection.isNil and info.peerId.len > 0:
        muxer.connection.peerId = info.peerId
      if info.peerId.len > 0:
        stream.peerId = info.peerId

      when defined(libp2p_agents_metrics):
        var
          knownAgent = "unknown"
          shortAgent =
            info.agentVersion.get("").split("/")[0].safeToLowerAscii().get("")
        if KnownLibP2PAgentsSeq.contains(shortAgent):
          knownAgent = shortAgent
        muxer.setShortAgent(knownAgent)

      peerStore.updatePeerInfo(info, stream.observedAddr)
    else:
      warn "peerStore.identify multistream select rejected",
        peerId = muxer.connection.peerId,
        streamPeerId = stream.peerId,
        codec = peerStore.identify.codec()
  finally:
    stream.endProtocolNegotiation()
    await stream.closeWithEOF()

proc identify*(
    peerStore: PeerStore, muxer: Muxer
) {.
    async: (
      raises: [
        CancelledError, IdentityNoMatchError, IdentityInvalidMsgError, MultiStreamError,
        LPStreamError, MuxerError,
      ]
    )
.} =
  if peerStore.isNil or muxer.isNil or muxer.connection.isNil:
    return
  let identifyLock = getOrCreateIdentifyLock(peerStore, muxer)
  let waitedForIdentify =
    if identifyLock.isNil:
      false
    else:
      identifyLock.locked()
  if not identifyLock.isNil:
    await identifyLock.acquire()
  defer:
    if not identifyLock.isNil:
      try:
        identifyLock.release()
      except AsyncLockError as exc:
        raiseAssert "identify lock must have been acquired above: " & exc.msg
  if waitedForIdentify and
      peerStore.hasIdentifySnapshot(muxer.connection.peerId):
    warn "peerStore.identify skip duplicate after wait",
      peerId = muxer.connection.peerId,
      protocol = muxer.connection.protocol,
      negotiated = muxer.connection.negotiatedMuxer
    return
  try:
    await identifyImpl(peerStore, muxer)
  except CancelledError as exc:
    raise exc

proc getMostObservedProtosAndPorts*(self: PeerStore): seq[MultiAddress] =
  return self.identify.observedAddrManager.getMostObservedProtosAndPorts()

proc guessDialableAddr*(self: PeerStore, ma: MultiAddress): MultiAddress =
  return self.identify.observedAddrManager.guessDialableAddr(ma)
