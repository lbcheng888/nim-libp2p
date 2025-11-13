import std/[times, tables, sequtils, sets, options, algorithm]
import chronos
import chronicles
import results
import ./[consts, xordistance, routingtable, lookupstate, requests, keys, protobuf]
import ../protocol
import ../../[peerid, switch, multihash, peerstore]
import ../../delegatedrouting
import ../../record as dhtrecord
import ../../utils/heartbeat
import ../../utility

logScope:
  topics = "kad-dht"

## Currently a string, because for some reason, that's what is chosen at the protobuf level
## TODO: convert between RFC3339 strings and use of integers (i.e. the _correct_ way)
type TimeStamp* = string

type EntryRecord* = object
  value*: seq[byte]
  time*: TimeStamp

const DiscoveredAddrTTL = DefaultDiscoveredAddressTTL

proc init*(
    T: typedesc[EntryRecord], value: Key, time: Opt[TimeStamp]
): EntryRecord {.gcsafe, raises: [].} =
  EntryRecord(value: value, time: time.get(TimeStamp(ts: $times.now().utc)))

type LocalTable* = Table[Key, EntryRecord]

type EntryValidator* = ref object of RootObj
method isValid*(
    self: EntryValidator, key: Key, value: seq[byte]
): bool {.base, raises: [], gcsafe.} =
  doAssert(false, "EntryValidator base not implemented")

type EntrySelector* = ref object of RootObj
method select*(
    self: EntrySelector, key: Key, values: seq[seq[byte]]
): Result[int, string] {.base, raises: [], gcsafe.} =
  doAssert(false, "EntrySelection base not implemented")

type DefaultEntryValidator* = ref object of EntryValidator
method isValid*(
    self: DefaultEntryValidator, key: Key, value: seq[byte]
): bool {.raises: [], gcsafe.} =
  return true

type DefaultEntrySelector* = ref object of EntrySelector
method select*(
    self: DefaultEntrySelector, key: Key, values: seq[seq[byte]]
): Result[int, string] {.raises: [], gcsafe.} =
  return ok(0)

type ProviderRecord = object
  peerId: PeerId
  addrs: seq[MultiAddress]
  expiresAt: DateTime

const DefaultProviderTTL* = chronos.hours(24)

type RecordStoreEntryValidator* = ref object of EntryValidator
  store*: ref dhtrecord.RecordStore

method isValid*(
    self: RecordStoreEntryValidator, key: Key, value: seq[byte]
): bool {.raises: [], gcsafe.} =
  if self.store.isNil:
    return false
  var keyStr = newString(key.len)
  for i, b in key:
    keyStr[i] = char(b)
  let record = dhtrecord.Record(key: keyStr, value: value)
  self.store[].validate(record)

type IpnsRepublisherConfig* = object
  refreshSlack*: chronos.Duration
  checkInterval*: chronos.Duration
  failureBackoff*: chronos.Duration
  lookupTimeoutSeconds*: int
  republish*: bool

const defaultIpnsRepublisherConfig* = IpnsRepublisherConfig(
  refreshSlack: chronos.minutes(1),
  checkInterval: chronos.seconds(30),
  failureBackoff: chronos.minutes(2),
  lookupTimeoutSeconds: 10,
  republish: true,
)

type
  IpnsNamespaceCounters* = object
    refreshSuccesses*: uint64
    refreshFailures*: uint64
    lastSuccess*: Option[DateTime]
    lastFailure*: Option[DateTime]

  IpnsNamespaceSnapshot* = object
    namespace*: string
    activeRecords*: int
    backoffRecords*: int
    earliestNextAttempt*: Option[chronos.Duration]
    refreshSuccesses*: uint64
    refreshFailures*: uint64
    lastSuccess*: Option[DateTime]
    lastFailure*: Option[DateTime]

  IpnsRepublisherSnapshot* = object
    generatedAt*: DateTime
    totalRecords*: int
    overdueRecords*: int
    namespaces*: seq[IpnsNamespaceSnapshot]

proc toTimesDuration(d: chronos.Duration): times.Duration {.inline.} =
  initDuration(nanoseconds = chronos.nanoseconds(d))

proc toChronosDuration(d: times.Duration): chronos.Duration {.inline.} =
  let nanos = d.inNanoseconds
  if nanos <= 0:
    return chronos.nanoseconds(0)
  chronos.nanoseconds(nanos)

type KadDHT* = ref object of LPProtocol
  switch: Switch
  rng: ref HmacDrbgContext
  rtable*: RoutingTable
  maintenanceLoop: Future[void]
  dataTable*: LocalTable
  entryValidator: EntryValidator
  entrySelector: EntrySelector
  recordStore*: ref dhtrecord.RecordStore
  ipnsNamespacePolicies: Table[string, dhtrecord.IpnsNamespacePolicy]
  ipnsNamespaceStats: Table[string, IpnsNamespaceCounters]
  ipnsNextAttempt: Table[string, DateTime]
  ipnsRefreshLoop: Future[void]
  ipnsConfig: IpnsRepublisherConfig
  ipnsEnabled: bool
  providerRecords: Table[Key, seq[ProviderRecord]]
  providerLock: AsyncLock

proc insert*(
    self: var LocalTable, key: Key, value: sink seq[byte], time: TimeStamp
) {.raises: [].} =
  debug "Local table insertion", key, value
  self[key] = EntryRecord(value: value, time: time)

proc get*(self: LocalTable, key: Key): Opt[EntryRecord] {.raises: [].} =
  if not self.hasKey(key):
    return Opt.none(EntryRecord)
  try:
    return Opt.some(self[key])
  except KeyError:
    doAssert false, "checked with hasKey"

const MaxMsgSize = 4096
# Forward declaration
proc findNode*(
  kad: KadDHT, targetId: Key
): Future[seq[PeerId]] {.async: (raises: [CancelledError]).}

proc sendFindNode(
    kad: KadDHT, peerId: PeerId, addrs: seq[MultiAddress], targetId: Key
): Future[Message] {.
    async: (raises: [CancelledError, DialFailedError, ValueError, LPStreamError])
.} =
  let conn = await kad.switch.dial(peerId, addrs, KadCodec)
  defer:
    await conn.close()

  let msg = Message(msgType: MessageType.findNode, key: Opt.some(targetId))
  await conn.writeLp(msg.encode().buffer)

  let reply = Message.decode(await conn.readLp(MaxMsgSize)).tryGet()
  if reply.msgType != MessageType.findNode:
    raise newException(ValueError, "Unexpected message type in reply: " & $reply)

  return reply

proc sendGetValue(
    kad: KadDHT, peerId: PeerId, addrs: seq[MultiAddress], key: Key
): Future[Message] {.
    async: (raises: [CancelledError, DialFailedError, ValueError, LPStreamError])
.} =
  let conn = await kad.switch.dial(peerId, addrs, KadCodec)
  defer:
    await conn.close()

  let msg = Message(msgType: MessageType.getValue, key: Opt.some(key))
  await conn.writeLp(msg.encode().buffer)

  let reply = Message.decode(await conn.readLp(MaxMsgSize)).tryGet()
  if reply.msgType != MessageType.getValue:
    raise newException(
      ValueError, "Unexpected message type in reply: " & $reply.msgType
    )

  return reply

proc sendGetProviders(
    kad: KadDHT, peerId: PeerId, addrs: seq[MultiAddress], key: Key
): Future[Message] {.
    async: (raises: [CancelledError, DialFailedError, ValueError, LPStreamError])
.} =
  let conn = await kad.switch.dial(peerId, addrs, KadCodec)
  defer:
    await conn.close()

  let msg = Message(msgType: MessageType.getProviders, key: Opt.some(key))
  await conn.writeLp(msg.encode().buffer)

  let reply = Message.decode(await conn.readLp(MaxMsgSize)).tryGet()
  if reply.msgType != MessageType.getProviders:
    raise newException(
      ValueError, "Unexpected message type in reply: " & $reply.msgType
    )

  return reply

proc sendAddProvider(
    kad: KadDHT,
    peerId: PeerId,
    addrs: seq[MultiAddress],
    key: Key,
    provider: Peer,
): Future[void] {.
    async: (raises: [CancelledError, DialFailedError, LPStreamError, ValueError])
.} =
  let conn = await kad.switch.dial(peerId, addrs, KadCodec)
  defer:
    await conn.close()

  let msg = Message(
    msgType: MessageType.addProvider,
    key: Opt.some(key),
    providerPeers: @[provider],
  )
  await conn.writeLp(msg.encode().buffer)

  let reply = Message.decode(await conn.readLp(MaxMsgSize)).valueOr:
    return
  if reply.msgType != MessageType.addProvider:
    debug "Unexpected message type in add-provider reply",
      peerId = peerId, received = reply.msgType

proc waitRepliesOrTimeouts(
    pendingFutures: Table[PeerId, Future[Message]]
): Future[(seq[Message], seq[PeerId])] {.async: (raises: [CancelledError]).} =
  await allFutures(pendingFutures.values.toSeq())

  var receivedReplies: seq[Message] = @[]
  var failedPeers: seq[PeerId] = @[]

  for (peerId, replyFut) in pendingFutures.pairs:
    try:
      receivedReplies.add(await replyFut)
    except CatchableError:
      failedPeers.add(peerId)
      error "Could not send find_node to peer", peerId, err = getCurrentExceptionMsg()

  return (receivedReplies, failedPeers)

proc isBestValue(kad: KadDHT, key: Key, value: seq[byte]): bool =
  ## Returns whether `value` is a better value than what we have locally
  ## Always returns `true` if we don't have the value locally

  kad.dataTable.get(key).withValue(existing):
    kad.entrySelector.select(key, @[value, existing.value]).withValue(selectedIdx):
      return selectedIdx == 0

  true

proc checkConvergence(state: LookupState, me: PeerId): bool {.raises: [], gcsafe.} =
  let ready = state.activeQueries == 0
  let noNew = selectAlphaPeers(state).filterIt(me != it).len == 0
  return ready and noNew

proc buildCloserPeers(
    kad: KadDHT, targetId: Key, exclude: PeerId
): seq[Peer] {.gcsafe, raises: [].} =
  let closerKeys =
    kad.rtable.findClosest(targetId, DefaultReplic).filterIt(it != exclude.toKey())
  for peerKey in closerKeys:
    let pid = peerKey.toPeerId().valueOr:
      continue

    let addrs = kad.switch.peerStore.getAddresses(pid)
    if addrs.len == 0:
      continue

    result.add(
      Peer(
        id: pid.getBytes(),
        addrs: addrs,
        connection:
          if kad.switch.isConnected(pid):
            ConnectionType.connected
          else:
            ConnectionType.notConnected,
      )
    )

proc dedupAddresses(addrs: seq[MultiAddress]): seq[MultiAddress] =
  var seen = initHashSet[string]()
  for addr in addrs:
    let key = $addr
    if key notin seen:
      seen.incl(key)
      result.add(addr)

proc cleanupExpiredProvidersLocked(kad: KadDHT, now: DateTime) =
  var emptyKeys: seq[Key]
  for key, records in kad.providerRecords.mpairs:
    var writeIdx = 0
    for rec in records:
      if rec.expiresAt > now:
        records[writeIdx] = rec
        inc writeIdx
    if writeIdx == 0:
      emptyKeys.add(key)
    else:
      records.setLen(writeIdx)
  for key in emptyKeys:
    kad.providerRecords.del(key)

proc addProviderRecord(
    kad: KadDHT,
    key: Key,
    peerId: PeerId,
    addrs: seq[MultiAddress],
    ttl: chronos.Duration,
): Future[void] {.async: (raises: [CancelledError]).} =
  if addrs.len == 0:
    return
  let effectiveTtl =
    if ttl <= chronos.ZeroDuration:
      DefaultProviderTTL
    else:
      ttl
  let deduped = dedupAddresses(addrs)
  if deduped.len == 0:
    return
  let nowUtc = times.now().utc
  let expiresAt = nowUtc + toTimesDuration(effectiveTtl)

  await kad.providerLock.acquire()
  try:
    kad.cleanupExpiredProvidersLocked(nowUtc)
    withMapEntry(kad.providerRecords, key, @[], records):
      var updated = false
      for record in records.mitems:
        if record.peerId == peerId:
          record.addrs = dedupAddresses(record.addrs & deduped)
          record.expiresAt = expiresAt
          updated = true
          break
      if not updated:
        records.add(ProviderRecord(peerId: peerId, addrs: deduped, expiresAt: expiresAt))
  finally:
    try:
      kad.providerLock.release()
    except AsyncLockError:
      discard

  kad.switch.peerStore.addAddressesWithTTL(peerId, deduped, effectiveTtl)

proc getProviderRecords(
    kad: KadDHT, key: Key
): Future[seq[ProviderRecord]] {.async: (raises: [CancelledError]).} =
  let nowUtc = times.now().utc
  await kad.providerLock.acquire()
  var recordsCopy: seq[ProviderRecord] = @[]
  try:
    kad.cleanupExpiredProvidersLocked(nowUtc)
    kad.providerRecords.withValue(key, existing):
      recordsCopy = existing[]
  finally:
    try:
      kad.providerLock.release()
    except AsyncLockError:
      discard
  result = recordsCopy

proc buildProviderPeer(kad: KadDHT, record: ProviderRecord): Peer =
  Peer(
    id: record.peerId.getBytes(),
    addrs: record.addrs,
    connection:
      if kad.switch.isConnected(record.peerId):
        ConnectionType.connected
      else:
        ConnectionType.notConnected,
  )

proc cleanupProviders(kad: KadDHT) {.async: (raises: [CancelledError]).} =
  let nowUtc = times.now().utc
  await kad.providerLock.acquire()
  try:
    kad.cleanupExpiredProvidersLocked(nowUtc)
  finally:
    try:
      kad.providerLock.release()
    except AsyncLockError:
      discard

proc findNode*(
    kad: KadDHT, targetId: Key
): Future[seq[PeerId]] {.async: (raises: [CancelledError]).} =
  ## Iteratively search for the k closest peers to a target ID.

  var initialPeers = kad.rtable.findClosestPeers(targetId, DefaultReplic)
  var state = LookupState.init(targetId, initialPeers, kad.rtable.hasher)
  var addrTable: Table[PeerId, seq[MultiAddress]]
  for p in initialPeers:
    addrTable[p] = kad.switch.peerStore.getAddresses(p)

  while not state.done:
    let toQuery = state.selectAlphaPeers()
    debug "Queries", list = toQuery.mapIt(it.shortLog()), addrTab = addrTable
    var pendingFutures = initTable[PeerId, Future[Message]]()

    for peer in toQuery.filterIt(kad.switch.peerInfo.peerId != it):
      state.markPending(peer)
      let addrs = addrTable.getOrDefault(peer, @[])
      if addrs.len == 0:
        state.markFailed(peer)
        continue
      pendingFutures[peer] =
        kad.sendFindNode(peer, addrs, targetId).wait(chronos.seconds(5))

      state.activeQueries.inc

    let (successfulReplies, timedOutPeers) = await waitRepliesOrTimeouts(pendingFutures)

    for msg in successfulReplies:
      for peer in msg.closerPeers:
        let pid = PeerId.init(peer.id)
        if not pid.isOk:
          error "Invalid PeerId in successful reply", peerId = peer.id
          continue
        addrTable[pid.get()] = peer.addrs
      state.updateShortlist(
        msg,
        proc(p: PeerInfo) {.raises: [].} =
          discard kad.rtable.insert(p.peerId)
          kad.switch.peerStore.addAddressesWithTTL(
            p.peerId, p.addrs, DiscoveredAddrTTL
          )
        ,
        kad.rtable.hasher,
      )

    for timedOut in timedOutPeers:
      state.markFailed(timedOut)

    # Check for covergence: no active queries, and no other peers to be selected
    state.done = checkConvergence(state, kad.switch.peerInfo.peerId)

  return state.selectClosestK()

proc delegatedRoutingLookup(
    kad: KadDHT, key: Key
): Future[Opt[EntryRecord]] {.async: (raises: [CancelledError]).} =
  if kad.switch.delegatedRouting.isNil:
    return Opt.none(EntryRecord)

  var records: seq[DelegatedRoutingRecord]
  try:
    records = await kad.switch.delegatedRouting.getRecords(key)
  except CancelledError as exc:
    raise exc
  except DelegatedRoutingError as exc:
    trace "delegated routing record fallback failed", key = key.shortLog, err = exc.msg
    return Opt.none(EntryRecord)
  except CatchableError as exc:
    trace "unexpected delegated routing failure", key = key.shortLog, err = exc.msg
    return Opt.none(EntryRecord)

  for rec in records:
    if rec.key.len > 0 and rec.key != key:
      continue
    if not kad.entryValidator.isNil and not kad.entryValidator.isValid(key, rec.value):
      continue
    let timeStamp =
      if rec.timeReceived.isSome:
        rec.timeReceived.get()
      else:
        $times.now().utc
    return Opt.some(EntryRecord(value: rec.value, time: timeStamp))

  Opt.none(EntryRecord)

proc getValue*(
    kad: KadDHT, key: Key, timeout: Opt[int] = Opt.none(int)
): Future[Result[EntryRecord, string]] {.async: (raises: [CancelledError]).} =
  var initialPeers = kad.rtable.findClosestPeers(key, DefaultReplic)
  if initialPeers.len == 0:
    return err("no peers available for lookup")

  var state = LookupState.init(key, initialPeers, kad.rtable.hasher)
  var addrTable: Table[PeerId, seq[MultiAddress]]
  for p in initialPeers:
    addrTable[p] = kad.switch.peerStore.getAddresses(p)

  let rpcTimeout =
    if timeout.isSome:
      let secs = timeout.get().int
      let clamped = if secs <= 0: 1 else: secs
      chronos.seconds(clamped)
    else:
      chronos.seconds(5)

  var bestRecord = Opt.none(EntryRecord)

  while not state.done:
    let toQuery = state.selectAlphaPeers()
    if toQuery.len == 0:
      break

    var pendingFutures = initTable[PeerId, Future[Message]]()

    for peer in toQuery.filterIt(kad.switch.peerInfo.peerId != it):
      state.markPending(peer)
      let addrs = addrTable.getOrDefault(peer, @[])
      if addrs.len == 0:
        state.markFailed(peer)
        continue
      pendingFutures[peer] = kad.sendGetValue(peer, addrs, key).wait(rpcTimeout)
      state.activeQueries.inc

    if pendingFutures.len == 0:
      break

    let (successfulReplies, timedOutPeers) = await waitRepliesOrTimeouts(pendingFutures)

    for msg in successfulReplies:
      for peer in msg.closerPeers:
        let pid = PeerId.init(peer.id)
        if not pid.isOk:
          error "Invalid PeerId in get-value reply", peerId = peer.id
          continue
        addrTable[pid.get()] = peer.addrs

      state.updateShortlist(
        msg,
        proc(p: PeerInfo) {.raises: [].} =
          discard kad.rtable.insert(p.peerId)
          kad.switch.peerStore.addAddressesWithTTL(
            p.peerId, p.addrs, DiscoveredAddrTTL
          )
        ,
        kad.rtable.hasher,
      )

      msg.record.withValue(rec):
        if rec.value.isSome:
          let value = rec.value.unsafeGet()
          if not kad.entryValidator.isValid(key, value):
            continue

          let timeStr = rec.timeReceived.get(otherwise = $times.now().utc)
          let candidate = EntryRecord(value: value, time: timeStr)

          if bestRecord.isNone:
            bestRecord = Opt.some(candidate)
          else:
            let selected = kad.entrySelector.select(
              key, @[candidate.value, bestRecord.get().value]
            )
            if selected.isOk and selected.get() == 0:
              bestRecord = Opt.some(candidate)

    for timedOut in timedOutPeers:
      state.markFailed(timedOut)

    if bestRecord.isSome and state.activeQueries == 0:
      break

    if bestRecord.isNone:
      state.done = checkConvergence(state, kad.switch.peerInfo.peerId)

  if bestRecord.isSome:
    let record = bestRecord.get()
    kad.dataTable.insert(key, record.value, record.time)
    if not kad.switch.isNil:
      try:
        await kad.switch.publishDelegatedRecord(key, record.value, record.time)
      except CancelledError as exc:
        raise exc
      except CatchableError as exc:
        trace "failed to cache record in delegated routing store",
          key = key.shortLog, err = exc.msg
    return ok(record)

  let fallbackRecord = await kad.delegatedRoutingLookup(key)
  if fallbackRecord.isSome:
    let record = fallbackRecord.get()
    kad.dataTable.insert(key, record.value, record.time)
    if not kad.switch.isNil:
      try:
        await kad.switch.publishDelegatedRecord(key, record.value, record.time)
      except CancelledError as exc:
        raise exc
      except CatchableError as exc:
        trace "failed to cache fallback record in delegated routing store",
          key = key.shortLog, err = exc.msg
    return ok(record)

  err("value not found")

proc findPeer*(
    kad: KadDHT, peer: PeerId
): Future[Result[PeerInfo, string]] {.async: (raises: [CancelledError]).} =
  ## Walks the key space until it finds candidate addresses for a peer Id

  if kad.switch.peerInfo.peerId == peer:
    # Looking for yourself.
    return ok(kad.switch.peerInfo)

  if kad.switch.isConnected(peer):
    # Return known info about already connected peer
    return ok(PeerInfo(peerId: peer, addrs: kad.switch.peerStore.getAddresses(peer)))

  let foundNodes = await kad.findNode(peer.toKey())
  if not foundNodes.contains(peer):
    return err("peer not found")

  return ok(PeerInfo(peerId: peer, addrs: kad.switch.peerStore.getAddresses(peer)))

proc ping*(
    kad: KadDHT, peerId: PeerId, addrs: seq[MultiAddress]
): Future[bool] {.
    async: (raises: [CancelledError, DialFailedError, ValueError, LPStreamError])
.} =
  let conn = await kad.switch.dial(peerId, addrs, KadCodec)
  defer:
    await conn.close()

  let request = Message(msgType: MessageType.ping)
  await conn.writeLp(request.encode().buffer)

  let reply = Message.decode(await conn.readLp(MaxMsgSize)).tryGet()

  reply == request

proc dispatchPutVal(
    switch: Switch, peer: PeerId, key: Key, value: seq[byte]
) {.async: (raises: [CancelledError, DialFailedError, LPStreamError]).} =
  let conn = await switch.dial(peer, KadCodec)
  defer:
    await conn.close()
  let msg = Message(
    msgType: MessageType.putValue,
    record: Opt.some(protobuf.Record(key: Opt.some(key), value: Opt.some(value))),
  )
  await conn.writeLp(msg.encode().buffer)

  let reply = Message.decode(await conn.readLp(MaxMsgSize)).valueOr:
    # todo log this more meaningfully
    error "PutValue reply decode fail", error = error, conn = conn
    return
  if reply != msg:
    error "Unexpected change between msg and reply: ",
      msg = msg, reply = reply, conn = conn

proc putValue*(
    kad: KadDHT, key: Key, value: seq[byte], timeout: Opt[int]
): Future[Result[void, string]] {.async: (raises: [CancelledError]), gcsafe.} =
  if not kad.entryValidator.isValid(key, value):
    return err("invalid key/value pair")

  if not kad.isBestValue(key, value):
    return err("Value rejected, we have a better one")

  let peers = await kad.findNode(key)

  let rpcBatch = peers.mapIt(kad.switch.dispatchPutVal(it, key, value))

  kad.dataTable.insert(key, value, $times.now().utc)
  try:
    await rpcBatch.allFutures().wait(chronos.seconds(5))
  except AsyncTimeoutError:
    # Dispatch will timeout if any of the calls don't receive a response (which is normal)
    discard

  ok()

proc provide*(
    kad: KadDHT,
    key: Key,
    ttl: chronos.Duration = DefaultProviderTTL,
    addresses: seq[MultiAddress] = @[],
): Future[Result[void, string]] {.async: (raises: [CancelledError]).} =
  let effectiveTtl =
    if ttl <= chronos.ZeroDuration:
      DefaultProviderTTL
    else:
      ttl

  var advAddrs = dedupAddresses(addresses)
  if advAddrs.len == 0:
    try:
      await kad.switch.peerInfo.update()
    except CancelledError as exc:
      raise exc
    except CatchableError as exc:
      trace "Failed to refresh local addresses for provider advertisement",
        err = exc.msg
    advAddrs = dedupAddresses(kad.switch.peerInfo.addrs)

  if advAddrs.len == 0:
    return err("no addresses available for provider advertisement")

  try:
    await kad.addProviderRecord(key, kad.switch.peerInfo.peerId, advAddrs, effectiveTtl)
  except CancelledError as exc:
    raise exc
  except CatchableError as exc:
    trace "Failed to persist local provider record", err = exc.msg

  let providerPeer = Peer(
    id: kad.switch.peerInfo.peerId.getBytes(),
    addrs: advAddrs,
    connection:
      if kad.switch.isConnected(kad.switch.peerInfo.peerId):
        ConnectionType.connected
      else:
        ConnectionType.notConnected,
  )

  let peers = await kad.findNode(key)
  var announceFutures: seq[Future[void]] = @[]
  for peer in peers:
    if peer == kad.switch.peerInfo.peerId:
      continue
    let peerAddrs = kad.switch.peerStore.getAddresses(peer)
    if peerAddrs.len == 0:
      continue
    announceFutures.add(kad.sendAddProvider(peer, peerAddrs, key, providerPeer))

  if announceFutures.len > 0:
    try:
      await announceFutures.allFutures().wait(chronos.seconds(5))
    except AsyncTimeoutError:
      discard
    except CatchableError as exc:
      trace "Provider announcement batch failed", err = exc.msg

  ok()

proc findProviders*(
    kad: KadDHT,
    key: Key,
    maxResults: int = DefaultReplic,
): Future[Result[seq[(PeerId, seq[MultiAddress])], string]] {.
    async: (raises: [CancelledError])
  .} =
  if maxResults <= 0:
    return ok(newSeq[(PeerId, seq[MultiAddress])]())

  var results: seq[(PeerId, seq[MultiAddress])] = @[]
  var indexByPeer = initTable[string, int]()

  let localRecords = await kad.getProviderRecords(key)
  for rec in localRecords:
    let addresses = dedupAddresses(rec.addrs)
    if addresses.len == 0:
      continue
    let keyStr = $rec.peerId
    let existingIdx = indexByPeer.getOrDefault(keyStr, -1)
    if existingIdx >= 0:
      let idx = existingIdx
      results[idx][1] = dedupAddresses(results[idx][1] & addresses)
    else:
      indexByPeer[keyStr] = results.len
      results.add((rec.peerId, addresses))
  if results.len >= maxResults:
    results.setLen(maxResults)
    return ok(results)

  let peers = await kad.findNode(key)
  for peer in peers:
    if results.len >= maxResults:
      break
    if peer == kad.switch.peerInfo.peerId:
      continue

    let peerAddrs = kad.switch.peerStore.getAddresses(peer)
    if peerAddrs.len == 0:
      continue

    var reply: Message
    try:
      reply = await kad.sendGetProviders(peer, peerAddrs, key)
    except CancelledError as exc:
      raise exc
    except CatchableError as exc:
      trace "getProviders RPC failed", peerId = peer, err = exc.msg
      continue

    for providerPeer in reply.providerPeers:
      let pid = PeerId.init(providerPeer.id)
      if pid.isErr:
        trace "Invalid provider id in response", peerId = providerPeer.id
        continue
      let providerId = pid.get()
      let providerAddrs = dedupAddresses(providerPeer.addrs)
      if providerAddrs.len == 0:
        continue

      try:
        await kad.addProviderRecord(key, providerId, providerAddrs, DefaultProviderTTL)
      except CancelledError as exc:
        raise exc
      except CatchableError as exc:
        trace "Failed to cache provider record from response",
          peerId = providerId, err = exc.msg

      let keyStr = $providerId
      let existingIdx = indexByPeer.getOrDefault(keyStr, -1)
      if existingIdx >= 0:
        let idx = existingIdx
        results[idx][1] = dedupAddresses(results[idx][1] & providerAddrs)
      else:
        indexByPeer[keyStr] = results.len
        results.add((providerId, providerAddrs))

      if results.len >= maxResults:
        break

  if results.len > maxResults:
    results.setLen(maxResults)

  ok(results)

proc bootstrap*(
    kad: KadDHT, bootstrapNodes: seq[PeerInfo]
) {.async: (raises: [CancelledError]).} =
  for b in bootstrapNodes:
    try:
      await kad.switch.connect(b.peerId, b.addrs)
      debug "Connected to bootstrap peer", peerId = b.peerId
    except DialFailedError as exc:
      # at some point will want to bubble up a Result[void, SomeErrorEnum]
      error "failed to dial to bootstrap peer", peerId = b.peerId, error = exc.msg
      continue

    let msg =
      try:
        await kad.sendFindNode(b.peerId, b.addrs, kad.rtable.selfId).wait(
          chronos.seconds(5)
        )
      except CatchableError as exc:
        debug "Send find node exception during bootstrap",
          target = b.peerId, addrs = b.addrs, err = exc.msg
        continue
    for peer in msg.closerPeers:
      let p = PeerId.init(peer.id).valueOr:
        debug "Invalid peer id received", error = error
        continue
      discard kad.rtable.insert(p)

      kad.switch.peerStore.addAddressesWithTTL(p, peer.addrs, DiscoveredAddrTTL)

    # bootstrap node replied succesfully. Adding to routing table
    discard kad.rtable.insert(b.peerId)

  let key = PeerId.random(kad.rng).valueOr:
    doAssert(false, "this should never happen")
    return
  discard await kad.findNode(key.toKey())
  info "Bootstrap lookup complete"

proc refreshBuckets(kad: KadDHT) {.async: (raises: [CancelledError]).} =
  for i in 0 ..< kad.rtable.buckets.len:
    if kad.rtable.buckets[i].isStale():
      let randomKey = randomKeyInBucketRange(kad.rtable.selfId, i, kad.rng)
      discard await kad.findNode(randomKey)

proc maintainBuckets(kad: KadDHT) {.async: (raises: [CancelledError]).} =
  heartbeat "refresh buckets", chronos.minutes(10):
    await kad.refreshBuckets()
    await kad.cleanupProviders()

proc handleFindNode(
    kad: KadDHT, conn: Connection, msg: Message
) {.async: (raises: [CancelledError]).} =
  let targetIdBytes = msg.key.valueOr:
    error "FindNode message without key data present", msg = msg, conn = conn
    return
  let targetId = PeerId.init(targetIdBytes).valueOr:
    error "FindNode message without valid key data", msg = msg, conn = conn
    return
  let closerPeers = kad.rtable.findClosest(targetId.toKey(), DefaultReplic)
    # exclude the node requester because telling a peer about itself does not reduce the distance,
    .filterIt(it != conn.peerId.toKey())

  let responsePb = encodeFindNodeReply(closerPeers, kad.switch)
  try:
    await conn.writeLp(responsePb.buffer)
  except LPStreamError as exc:
    debug "Write error when writing kad find-node RPC reply", conn = conn, err = exc.msg
    return

  # Peer is useful. adding to rtable
  discard kad.rtable.insert(conn.peerId)

proc handleGetValue(
    kad: KadDHT, conn: Connection, msg: Message
) {.async: (raises: [CancelledError]).} =
  let key = msg.key.valueOr:
    error "GetValue message without key data present", msg = msg, conn = conn
    return

  var response = Message(msgType: MessageType.getValue)

  kad.dataTable.get(key).withValue(entry):
    if kad.entryValidator.isValid(key, entry.value):
      response.record = Opt.some(
        protobuf.Record(
          key: Opt.some(key),
          value: Opt.some(entry.value),
          timeReceived: Opt.some(entry.time),
        )
      )

  let closerPeers = kad.buildCloserPeers(key, conn.peerId)
  if closerPeers.len > 0:
    response.closerPeers = closerPeers

  try:
    await conn.writeLp(response.encode().buffer)
  except LPStreamError as exc:
    debug "Failed to send get-value RPC reply", conn = conn, err = exc.msg
    return

proc handlePutValue(
    kad: KadDHT, conn: Connection, msg: Message
) {.async: (raises: [CancelledError]).} =
  let record = msg.record.valueOr:
    error "No record in message buffer", msg = msg, conn = conn
    return
  let (key, value) =
    if record.key.isSome() and record.value.isSome():
      (record.key.unsafeGet(), record.value.unsafeGet())
    else:
      error "No key or no value in rpc buffer", msg = msg, conn = conn
      return

  # Value sanitisation done. Start insertion process
  if not kad.entryValidator.isValid(key, value):
    debug "Record is not valid", key, value
    return

  if not kad.isBestValue(key, value):
    error "Dropping received value, we have a better one"
    return

  kad.dataTable.insert(key, value, $times.now().utc)
  # consistent with following link, echo message without change
  # https://github.com/libp2p/js-libp2p/blob/cf9aab5c841ec08bc023b9f49083c95ad78a7a07/packages/kad-dht/src/rpc/handlers/put-value.ts#L22
  try:
    await conn.writeLp(msg.encode().buffer)
  except LPStreamError as exc:
    debug "Failed to send find-node RPC reply", conn = conn, err = exc.msg
    return

proc handleAddProvider(
    kad: KadDHT, conn: Connection, msg: Message
) {.async: (raises: [CancelledError]).} =
  let key = msg.key.valueOr:
    error "AddProvider message without key data present", msg = msg, conn = conn
    return

  if msg.providerPeers.len == 0:
    trace "AddProvider message missing provider entries", conn = conn
  else:
    for peer in msg.providerPeers:
      let pid = PeerId.init(peer.id)
      if pid.isErr:
        trace "Invalid provider peer id received", peerId = peer.id
        continue
      try:
        await kad.addProviderRecord(key, pid.get(), peer.addrs, DefaultProviderTTL)
      except CancelledError as exc:
        raise exc
      except CatchableError as exc:
        trace "Failed to store provider record", peerId = pid.get(), err = exc.msg

  try:
    await conn.writeLp(msg.encode().buffer)
  except LPStreamError as exc:
    debug "Failed to send add-provider RPC reply", conn = conn, err = exc.msg

proc handleGetProviders(
    kad: KadDHT, conn: Connection, msg: Message
) {.async: (raises: [CancelledError]).} =
  let key = msg.key.valueOr:
    error "GetProviders message without key data present", msg = msg, conn = conn
    return

  var response = Message(msgType: MessageType.getProviders, key: Opt.some(key))

  let records = await kad.getProviderRecords(key)
  for rec in records:
    response.providerPeers.add(kad.buildProviderPeer(rec))

  let closerPeers = kad.buildCloserPeers(key, conn.peerId)
  if closerPeers.len > 0:
    response.closerPeers = closerPeers

  try:
    await conn.writeLp(response.encode().buffer)
  except LPStreamError as exc:
    debug "Failed to send get-providers RPC reply", conn = conn, err = exc.msg

proc new*(
    T: typedesc[KadDHT],
    switch: Switch,
    validator: EntryValidator = DefaultEntryValidator(),
    entrySelector: EntrySelector = DefaultEntrySelector(),
    rng: ref HmacDrbgContext = newRng(),
): T {.raises: [].} =
  var rtable = RoutingTable.new(switch.peerInfo.peerId.toKey(), Opt.none(XorDHasher))
  let kad = T(
    rng: rng,
    switch: switch,
    rtable: rtable,
    entryValidator: validator,
    entrySelector: entrySelector,
    recordStore: nil,
    ipnsNamespacePolicies: initTable[string, dhtrecord.IpnsNamespacePolicy](),
    ipnsNamespaceStats: initTable[string, IpnsNamespaceCounters](),
    ipnsNextAttempt: initTable[string, DateTime](),
    ipnsRefreshLoop: nil,
    ipnsConfig: defaultIpnsRepublisherConfig,
    ipnsEnabled: false,
    providerRecords: initTable[Key, seq[ProviderRecord]](),
    providerLock: newAsyncLock(),
  )

  kad.codec = KadCodec
  kad.handler = proc(
      conn: Connection, proto: string
  ) {.async: (raises: [CancelledError]).} =
    defer:
      await conn.close()
    while not conn.atEof:
      let buf =
        try:
          await conn.readLp(MaxMsgSize)
        except LPStreamError as exc:
          debug "Read error when handling kademlia RPC", conn = conn, err = exc.msg
          return
      let msg = Message.decode(buf).valueOr:
        debug "Failed to decode message", err = error
        return

      case msg.msgType
      of MessageType.findNode:
        await kad.handleFindNode(conn, msg)
      of MessageType.putValue:
        await kad.handlePutValue(conn, msg)
      of MessageType.getValue:
        await kad.handleGetValue(conn, msg)
      of MessageType.addProvider:
        await kad.handleAddProvider(conn, msg)
      of MessageType.getProviders:
        await kad.handleGetProviders(conn, msg)
      of MessageType.ping:
        try:
          await conn.writeLp(buf)
        except LPStreamError as exc:
          debug "Failed to send ping reply", conn = conn, err = exc.msg
          return
      else:
        error "Unhandled kad-dht message type", msg = msg
        return
  return kad

proc setSelector*(kad: KadDHT, selector: EntrySelector) =
  doAssert(selector != nil)
  kad.entrySelector = selector

proc setValidator*(kad: KadDHT, validator: EntryValidator) =
  doAssert(validator != nil)
  kad.entryValidator = validator

proc startIpnsLoop(kad: KadDHT) {.raises: [], gcsafe.}

proc fallbackRawKey(normalizedKey: string): seq[byte] =
  ## Convert a normalized IPNS key ("ipns/peerid") back to a DHT key ("/ipns/peerid")
  if normalizedKey.len == 0:
    return @[]
  result = newSeq[byte](normalizedKey.len + 1)
  result[0] = byte('/')
  for i, c in normalizedKey:
    result[i + 1] = byte(c)

proc normalizeNamespace(namespace: string): string =
  var start = 0
  var finish = namespace.len
  while start < finish and namespace[start] == '/':
    inc start
  while finish > start and namespace[finish - 1] == '/':
    dec finish
  if start >= finish:
    return ""
  namespace[start ..< finish]

proc validateIpnsConfig(config: IpnsRepublisherConfig) =
  if config.refreshSlack < chronos.ZeroDuration:
    raise newException(Defect, "IPNS refreshSlack cannot be negative")
  if config.checkInterval <= chronos.ZeroDuration:
    raise newException(Defect, "IPNS checkInterval must be positive")
  if config.failureBackoff < chronos.ZeroDuration:
    raise newException(Defect, "IPNS failureBackoff cannot be negative")
  if config.lookupTimeoutSeconds < 0:
    raise newException(Defect, "IPNS lookupTimeoutSeconds cannot be negative")

proc applyIpnsNamespacePolicies(kad: KadDHT) =
  if kad.recordStore.isNil:
    return
  for namespace, policy in kad.ipnsNamespacePolicies.pairs():
    kad.recordStore[].registerIpnsNamespace(namespace, policy)

proc enableIpns*(
    kad: KadDHT,
    store: ref dhtrecord.RecordStore,
    config: IpnsRepublisherConfig = defaultIpnsRepublisherConfig,
) =
  ## Enable IPNS-aware validation and automatic refresh/republish logic.
  ##
  ## The provided RecordStore must have the IPNS validator registered (via `registerIpnsValidator`).
  doAssert(not store.isNil, "RecordStore reference cannot be nil")
  validateIpnsConfig(config)
  kad.recordStore = store
  kad.ipnsConfig = config
  kad.ipnsEnabled = true
  kad.ipnsNextAttempt.clear()
  kad.applyIpnsNamespacePolicies()
  let validator = RecordStoreEntryValidator(store: store)
  kad.setValidator(validator)
  if kad.started:
    startIpnsLoop(kad)

proc setIpnsNamespacePolicy*(
    kad: KadDHT, namespace: string, policy: dhtrecord.IpnsNamespacePolicy
) =
  dhtrecord.validateIpnsRefreshWeight(policy.refreshWeight)
  let normalized = normalizeNamespace(namespace)
  kad.ipnsNamespacePolicies[normalized] = policy
  if kad.recordStore.isNil:
    return
  kad.recordStore[].registerIpnsNamespace(normalized, policy)

proc clearIpnsNamespacePolicy*(kad: KadDHT, namespace: string) =
  let normalized = normalizeNamespace(namespace)
  if kad.ipnsNamespacePolicies.contains(normalized):
    kad.ipnsNamespacePolicies.del(normalized)
  if kad.recordStore.isNil:
    return
  kad.recordStore[].unregisterIpnsNamespace(normalized)

proc getIpnsNamespacePolicy*(
    kad: KadDHT, namespace: string
): Opt[dhtrecord.IpnsNamespacePolicy] =
  let normalized = normalizeNamespace(namespace)
  kad.ipnsNamespacePolicies.withValue(normalized, policy):
    return Opt.some(policy[])
  if kad.recordStore.isNil:
    return Opt.none(dhtrecord.IpnsNamespacePolicy)
  let storeOpt = kad.recordStore[].getIpnsNamespacePolicy(normalized)
  if storeOpt.isSome:
    return Opt.some(storeOpt.get())
  Opt.none(dhtrecord.IpnsNamespacePolicy)

proc updateIpnsRepublisherConfig*(
    kad: KadDHT, config: IpnsRepublisherConfig
): Future[void] {.async: (raises: [CancelledError]), public.} =
  validateIpnsConfig(config)
  kad.ipnsConfig = config
  if not kad.ipnsRefreshLoop.isNil and not kad.ipnsRefreshLoop.finished():
    await kad.ipnsRefreshLoop.cancelAndWait()
    kad.ipnsRefreshLoop = nil
  if kad.started:
    startIpnsLoop(kad)

proc ipnsLookupTimeout(kad: KadDHT): Opt[int] =
  if kad.ipnsConfig.lookupTimeoutSeconds > 0:
    Opt.some(kad.ipnsConfig.lookupTimeoutSeconds)
  else:
    Opt.none(int)

proc recordIpnsSuccess(kad: KadDHT, namespace: string, ts: DateTime) =
  kad.ipnsNamespaceStats.withValue(namespace, stats):
    inc stats[].refreshSuccesses
    stats[].lastSuccess = some(ts)
  do:
    kad.ipnsNamespaceStats[namespace] = IpnsNamespaceCounters(
      refreshSuccesses: 1,
      refreshFailures: 0,
      lastSuccess: some(ts),
      lastFailure: none(DateTime),
    )

proc recordIpnsFailure(kad: KadDHT, namespace: string, ts: DateTime) =
  kad.ipnsNamespaceStats.withValue(namespace, stats):
    inc stats[].refreshFailures
    stats[].lastFailure = some(ts)
  do:
    kad.ipnsNamespaceStats[namespace] = IpnsNamespaceCounters(
      refreshSuccesses: 0,
      refreshFailures: 1,
      lastSuccess: none(DateTime),
      lastFailure: some(ts),
    )

proc refreshIpnsRecord(
    kad: KadDHT, normalizedKey: string
) {.async: (raises: [CancelledError]).} =
  if kad.recordStore.isNil:
    return
  if not kad.recordStore[].ipnsRecords.contains(normalizedKey):
    return

  var entry: dhtrecord.IpnsCacheEntry
  try:
    entry = kad.recordStore[].ipnsRecords[normalizedKey]
  except KeyError:
    return
  let namespace = entry.namespace
  let cachedRecord = entry.record
  let rawKey =
    if entry.rawKey.len > 0:
      entry.rawKey
    else:
      fallbackRawKey(normalizedKey)
  if rawKey.len == 0:
    return
  let key = Key(rawKey)
  let timeoutOpt = kad.ipnsLookupTimeout()
  let lookup = await kad.getValue(key, timeoutOpt)
  let current = times.now().utc
  if lookup.isErr:
    if kad.ipnsConfig.republish and cachedRecord.len > 0 and entry.validUntil > current:
      let publishRes = await kad.putValue(key, cachedRecord, timeoutOpt)
      if publishRes.isOk:
        kad.ipnsNextAttempt.del(normalizedKey)
        kad.recordIpnsSuccess(namespace, current)
        debug "Republished cached IPNS record after lookup failure", key = normalizedKey
        return
      kad.ipnsNextAttempt[normalizedKey] =
        current + toTimesDuration(kad.ipnsConfig.failureBackoff)
      kad.recordIpnsFailure(namespace, current)
      debug "Failed to republish cached IPNS record after lookup failure",
        key = normalizedKey, err = publishRes.error
      return
    kad.ipnsNextAttempt[normalizedKey] =
      current + toTimesDuration(kad.ipnsConfig.failureBackoff)
    kad.recordIpnsFailure(namespace, current)
    debug "Failed to refresh IPNS record", key = normalizedKey, err = lookup.error
    return

  kad.ipnsNextAttempt.del(normalizedKey)

  if kad.ipnsConfig.republish:
    let record = lookup.get()
    let publishRes = await kad.putValue(key, record.value, timeoutOpt)
    if publishRes.isErr:
      kad.ipnsNextAttempt[normalizedKey] =
        current + toTimesDuration(kad.ipnsConfig.failureBackoff)
      kad.recordIpnsFailure(namespace, current)
      debug "Failed to republish IPNS record",
        key = normalizedKey, err = publishRes.error
      return

  kad.recordIpnsSuccess(namespace, current)

proc ipnsRepublisherSnapshot*(kad: KadDHT): Option[IpnsRepublisherSnapshot] =
  if kad.recordStore.isNil:
    return none(IpnsRepublisherSnapshot)

  let store = kad.recordStore[]

  let now = times.now().utc
  var snapshot = IpnsRepublisherSnapshot(
    generatedAt: now,
    totalRecords: 0,
    overdueRecords: 0,
    namespaces: @[],
  )

  type NamespaceAggregate = tuple[
    active: int,
    backoff: int,
    earliest: Option[chronos.Duration]
  ]

  var aggregates = initTable[string, NamespaceAggregate]()

  for normalized, entry in store.ipnsRecords.pairs:
    inc snapshot.totalRecords
    if entry.cacheUntil <= now:
      inc snapshot.overdueRecords

    let namespace = entry.namespace
    withMapEntry(
      aggregates,
      namespace,
      (active: 0, backoff: 0, earliest: none(chronos.Duration)),
      agg
    ):
      inc agg.active

      if kad.ipnsNextAttempt.contains(normalized):
        let nextAllowed = kad.ipnsNextAttempt.getOrDefault(normalized, now)
        if nextAllowed > now:
          inc agg.backoff
          let waitDur = toChronosDuration(nextAllowed - now)
          if agg.earliest.isNone or waitDur < agg.earliest.get():
            agg.earliest = some(waitDur)

  var seenNamespaces = initHashSet[string]()

  for namespace, agg in aggregates.pairs:
    seenNamespaces.incl(namespace)
    let counters = kad.ipnsNamespaceStats.getOrDefault(namespace, IpnsNamespaceCounters())
    snapshot.namespaces.add(
      IpnsNamespaceSnapshot(
        namespace: namespace,
        activeRecords: agg.active,
        backoffRecords: agg.backoff,
        earliestNextAttempt: agg.earliest,
        refreshSuccesses: counters.refreshSuccesses,
        refreshFailures: counters.refreshFailures,
        lastSuccess: counters.lastSuccess,
        lastFailure: counters.lastFailure,
      )
    )

  for namespace, counters in kad.ipnsNamespaceStats.pairs:
    if namespace in seenNamespaces:
      continue
    snapshot.namespaces.add(
      IpnsNamespaceSnapshot(
        namespace: namespace,
        activeRecords: 0,
        backoffRecords: 0,
        earliestNextAttempt: none(chronos.Duration),
        refreshSuccesses: counters.refreshSuccesses,
        refreshFailures: counters.refreshFailures,
        lastSuccess: counters.lastSuccess,
        lastFailure: counters.lastFailure,
      )
    )

  some(snapshot)

proc ipnsRefreshWeight(kad: KadDHT, namespace: string): float64 =
  let policyOpt = kad.getIpnsNamespacePolicy(namespace)
  if policyOpt.isSome():
    let policy = policyOpt.get()
    if policy.refreshWeight > 0.0:
      return policy.refreshWeight
  dhtrecord.DefaultIpnsRefreshWeight

proc ipnsRefreshRoutine(
    kad: KadDHT
): Future[void] {.async: (raises: [CancelledError, Exception]).} =
  if kad.recordStore.isNil:
    return
  try:
    while kad.started and kad.ipnsEnabled:
      let config = kad.ipnsConfig
      let slack = toTimesDuration(config.refreshSlack)
      let checkInterval = config.checkInterval
      let now = times.now().utc
      var nextSleep = checkInterval
      var keysToRefresh: seq[(string, float64, DateTime)] = @[]
      let keys = toSeq(kad.recordStore[].ipnsRecords.keys())
      for normalized in keys:
        if not kad.recordStore[].ipnsRecords.contains(normalized):
          continue

        var entry: dhtrecord.IpnsCacheEntry
        try:
          entry = kad.recordStore[].ipnsRecords[normalized]
        except KeyError:
          continue

        if entry.cacheUntil <= now:
          kad.recordStore[].ipnsRecords.del(normalized)
          kad.ipnsNextAttempt.del(normalized)
          continue

        if kad.ipnsNextAttempt.contains(normalized):
          var nextAllowed: DateTime
          try:
            nextAllowed = kad.ipnsNextAttempt[normalized]
          except KeyError:
            nextAllowed = now
          if nextAllowed > now:
            let waitDur = toChronosDuration(nextAllowed - now)
            if waitDur < nextSleep:
              nextSleep = waitDur
            continue

        let weight = kad.ipnsRefreshWeight(entry.namespace)

        var weightedSlack = slack
        if slack.inNanoseconds > 0:
          let baseNs = slack.inNanoseconds
          let scaled = float64(baseNs) * weight
          var scaledNs: int64
          if scaled >= float64(high(int64)):
            scaledNs = high(int64)
          elif scaled <= 0.0:
            scaledNs = 0
          else:
            scaledNs = int64(scaled)
          weightedSlack = initDuration(nanoseconds = scaledNs)
          let remaining = entry.cacheUntil - now
          if remaining > initDuration() and weightedSlack > remaining:
            weightedSlack = remaining

        let refreshAt =
          if weightedSlack.inNanoseconds > 0:
            entry.cacheUntil - weightedSlack
          else:
            entry.cacheUntil

        if refreshAt <= now:
          keysToRefresh.add((normalized, weight, entry.cacheUntil))
        else:
          let untilRefresh = refreshAt - now
          let waitDur = toChronosDuration(untilRefresh)
          if waitDur < nextSleep:
            nextSleep = waitDur

      keysToRefresh.sort(
        proc(a, b: (string, float64, DateTime)): int =
          let weightCmp = cmp(b[1], a[1])
          if weightCmp != 0:
            return weightCmp
          cmp(a[2], b[2])
      )

      for keyInfo in keysToRefresh:
        if not kad.started or not kad.ipnsEnabled:
          break
        await kad.refreshIpnsRecord(keyInfo[0])

      if not kad.started or not kad.ipnsEnabled:
        break

      if keysToRefresh.len > 0:
        continue

      if nextSleep <= chronos.ZeroDuration:
        nextSleep = checkInterval
      if nextSleep > checkInterval:
        nextSleep = checkInterval
      await sleepAsync(nextSleep)
  except CancelledError:
    raise
  except CatchableError as exc:
    debug "IPNS refresh loop exited", err = exc.msg

proc startIpnsLoop(kad: KadDHT) {.raises: [], gcsafe.} =
  if not kad.ipnsEnabled or kad.recordStore.isNil:
    return
  if not kad.ipnsRefreshLoop.isNil and not kad.ipnsRefreshLoop.finished():
    return
  try:
    kad.ipnsRefreshLoop = kad.ipnsRefreshRoutine()
  except Exception as exc:
    debug "Failed to start IPNS refresh loop", err = exc.msg

method start*(kad: KadDHT): Future[void] {.async: (raises: [CancelledError]).} =
  if kad.started:
    warn "Starting kad-dht twice"
    return

  kad.maintenanceLoop = kad.maintainBuckets()
  kad.started = true
  startIpnsLoop(kad)

  info "Kad DHT started"

method stop*(kad: KadDHT): Future[void] {.async: (raises: []).} =
  if not kad.started:
    return

  kad.started = false
  kad.maintenanceLoop.cancelSoon()
  kad.maintenanceLoop = nil
  if not kad.ipnsRefreshLoop.isNil:
    await kad.ipnsRefreshLoop.cancelAndWait()
    kad.ipnsRefreshLoop = nil
  kad.ipnsNextAttempt.clear()
