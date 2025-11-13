# Nim-LibP2P
# Copyright (c) 2023 Status Research & Development GmbH
# Licensed under either of
#  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
#  * MIT license ([LICENSE-MIT](LICENSE-MIT))
# at your option.
# This file may not be copied, modified, or distributed except according to
# those terms.

## The switch is the core of libp2p, which brings together the
## transports, the connection manager, the upgrader and other
## parts to allow programs to use libp2p

{.push raises: [].}

import std/[tables, options, sequtils, sets, oids, typetraits]

import chronos, chronicles, metrics

import
  stream/connection,
  transports/transport,
  transports/tcptransport,
  upgrademngrs/upgrade,
  multistream,
  multiaddress,
  cid,
  protocols/protocol,
  protocols/secure/secure,
  protocols/bitswap/bitswap,
  protocols/bitswap/ledger,
  protocols/datatransfer/channelmanager,
  protocols/datatransfer/datatransfer,
  peerinfo,
  utils/semaphore,
  ./muxers/muxer,
  connmanager,
  nameresolving/nameresolver,
  peerid,
  peerstore,
  errors,
  utility,
  connectiongater,
  dialer,
  bandwidthmanager,
  resourcemanager,
  memorymanager,
  delegatedrouting,
  delegatedrouting/store

when defined(libp2p_msquic_experimental):
  import transports/msquictransport as quictransport
elif defined(libp2p_quic_support):
  import transports/quictransport
when not defined(libp2p_disable_datatransfer):
  from protocols/datatransfer/protobuf import DataTransferExtension

export connmanager, upgrade, dialer, peerstore

logScope:
  topics = "libp2p switch"

#TODO: General note - use a finite state machine to manage the different
# steps of connections establishing and upgrading. This makes everything
# more robust and less prone to ordering attacks - i.e. muxing can come if
# and only if the channel has been secured (i.e. if a secure manager has been
# previously provided)

const
  ConcurrentUpgrades* = 4
  BitswapDefaultTopDebtorsLimit* = 20
  BitswapDefaultTopDebtorsMinBytes* = 64'u64 * 1024

type
  Switch* {.public.} = ref object of Dial
    peerInfo*: PeerInfo
    connManager*: ConnManager
    transports*: seq[Transport]
    ms*: MultistreamSelect
    acceptFuts: seq[Future[void]]
    dialer*: Dial
    peerStore*: PeerStore
    nameResolver*: NameResolver
    started: bool
    services*: seq[Service]
    bitswap*: BitswapService
    connectionGater*: ConnectionGater
    bandwidthManager*: BandwidthManager
    resourceManager*: ResourceManager
    memoryManager*: MemoryManager
    delegatedRouting*: DelegatedRoutingClient
    delegatedRoutingStore*: DelegatedRoutingStore
    dataTransferManager*: DataTransferChannelManager
    when defined(libp2p_msquic_experimental) or defined(libp2p_quic_support):
      webtransportRotationTask*: Future[void]
      webtransportRotationInterval*: Duration
      webtransportRotationKeepHistory*: int
      webtransportLastRotation*: Option[Moment]

  UpgradeError* = object of LPError

  Service* = ref object of RootObj
    inUse: bool

method setup*(
    self: Service, switch: Switch
): Future[bool] {.base, async: (raises: [CancelledError]).} =
  if self.inUse:
    warn "service setup has already been called"
    return false
  self.inUse = true
  return true

method run*(self: Service, switch: Switch) {.base, async: (raises: [CancelledError]).} =
  doAssert(false, "[Service.run] abstract method not implemented!")

method stop*(
    self: Service, switch: Switch
): Future[bool] {.base, async: (raises: [CancelledError]).} =
  if not self.inUse:
    warn "service is already stopped"
    return false
  self.inUse = false
  return true

proc addConnEventHandler*(
    s: Switch, handler: ConnEventHandler, kind: ConnEventKind
) {.public.} =
  ## Adds a ConnEventHandler, which will be triggered when
  ## a connection to a peer is created or dropped.
  ## There may be multiple connections per peer.
  ##
  ## The handler should not raise.
  s.connManager.addConnEventHandler(handler, kind)

proc removeConnEventHandler*(
    s: Switch, handler: ConnEventHandler, kind: ConnEventKind
) {.public.} =
  s.connManager.removeConnEventHandler(handler, kind)

proc addPeerEventHandler*(
    s: Switch, handler: PeerEventHandler, kind: PeerEventKind
) {.public.} =
  ## Adds a PeerEventHandler, which will be triggered when
  ## a peer connects or disconnects from us.
  ##
  ## The handler should not raise.
  s.connManager.addPeerEventHandler(handler, kind)

proc removePeerEventHandler*(
    s: Switch, handler: PeerEventHandler, kind: PeerEventKind
) {.public.} =
  s.connManager.removePeerEventHandler(handler, kind)

proc bandwidthStats*(
    s: Switch, peerId: PeerId
): Option[PeerBandwidthStats] {.public.} =
  if s.bandwidthManager.isNil:
    return none(PeerBandwidthStats)
  s.bandwidthManager.stats(peerId)

proc bandwidthProtocolStats*(
    s: Switch, protocol: string
): Option[ProtocolBandwidthStats] {.public.} =
  if s.bandwidthManager.isNil:
    return none(ProtocolBandwidthStats)
  s.bandwidthManager.protocolStats(protocol)

proc bandwidthProtocols*(
    s: Switch
): seq[(string, ProtocolBandwidthStats)] {.public.} =
  if s.bandwidthManager.isNil:
    return @[]
  s.bandwidthManager.protocols()

proc bandwidthTotals*(
    s: Switch
): tuple[inbound: uint64, outbound: uint64] {.public.} =
  if s.bandwidthManager.isNil:
    (0'u64, 0'u64)
  else:
    (s.bandwidthManager.totalInbound, s.bandwidthManager.totalOutbound)

proc bitswapLedger*(s: Switch): BitswapLedger {.public.} =
  if s.bitswap.isNil:
    return nil
  s.bitswap.ledger

proc bitswapLedgerStats*(
    s: Switch, minExchanges: uint64 = 0
): Future[Option[BitswapLedgerStats]] {.async: (raises: [CancelledError]).} =
  let ledger = s.bitswapLedger()
  if ledger.isNil:
    return none(BitswapLedgerStats)
  try:
    let stats = await ledger.aggregate(minExchanges)
    return some(stats)
  except CatchableError as exc:
    debug "bitswap aggregate failed", err = exc.msg
    return none(BitswapLedgerStats)

proc bitswapTopDebtors*(
    s: Switch,
    limit: Positive = Positive(BitswapDefaultTopDebtorsLimit),
    minBytesSent: uint64 = BitswapDefaultTopDebtorsMinBytes,
): Future[seq[(PeerId, float, BitswapPeerLedger)]] {.
    async: (raises: [CancelledError]), public.} =
  ## 返回 Bitswap Ledger 当前记录中按债务比例排序的前若干对等节点。
  let ledger = s.bitswapLedger()
  if ledger.isNil:
    return @[]
  try:
    return await ledger.topDebtors(limit, minBytesSent)
  except CatchableError as exc:
    debug "bitswap top debtors aggregation failed", err = exc.msg
    return @[]

when not defined(libp2p_disable_datatransfer):
  proc dataTransferStats*(
      s: Switch
  ): Future[Option[DataTransferStats]] {.async: (raises: [CancelledError]).} =
    if s.dataTransferManager.isNil:
      return none(DataTransferStats)
    try:
      let stats = await s.dataTransferManager.stats()
      return some(stats)
    except CatchableError as exc:
      debug "data transfer stats collection failed", err = exc.msg
      return none(DataTransferStats)

  proc dataTransferChannel*(
      s: Switch, peerId: PeerId, transferId: uint64
  ): Future[Option[DataTransferChannel]] {.async: (raises: [CancelledError]), public.} =
    ## 获取指定对等节点与传输编号对应的 DataTransfer 通道快照。
    if s.dataTransferManager.isNil:
      return none(DataTransferChannel)
    try:
      return await s.dataTransferManager.getChannel(peerId, transferId)
    except CatchableError as exc:
      debug "data transfer getChannel failed", err = exc.msg,
        peerId = peerId, transferId = transferId
      return none(DataTransferChannel)

  proc dataTransferRemoveChannel*(
      s: Switch, peerId: PeerId, transferId: uint64
  ): Future[bool] {.async: (raises: [CancelledError]), public.} =
    ## 从本地状态表中移除指定 DataTransfer 通道记录。
    if s.dataTransferManager.isNil:
      return false
    try:
      let removed = await s.dataTransferManager.removeChannel(
        peerId, transferId
      )
      return removed
    except CatchableError as exc:
      debug "data transfer removeChannel failed", err = exc.msg,
        peerId = peerId, transferId = transferId
      return false

  proc dataTransferPauseChannel*(
      s: Switch,
      peerId: PeerId,
      transferId: uint64,
      status: Option[string] = none(string),
      extensions: seq[DataTransferExtension] = @[],
  ): Future[Option[DataTransferChannel]] {.async: (
      raises: [CancelledError, DataTransferError]
    ), public.} =
    ## 主动暂停指定 DataTransfer 通道，返回更新后的通道快照。
    if s.dataTransferManager.isNil:
      return none(DataTransferChannel)
    await s.dataTransferManager.pauseChannel(
      peerId, transferId, status, extensions
    )

  proc dataTransferResumeChannel*(
      s: Switch,
      peerId: PeerId,
      transferId: uint64,
      status: Option[string] = none(string),
      extensions: seq[DataTransferExtension] = @[],
  ): Future[Option[DataTransferChannel]] {.async: (
      raises: [CancelledError, DataTransferError]
    ), public.} =
    ## 恢复之前暂停的 DataTransfer 通道，返回更新后的通道快照。
    if s.dataTransferManager.isNil:
      return none(DataTransferChannel)
    await s.dataTransferManager.resumeChannel(
      peerId, transferId, status, extensions
    )

  proc dataTransferCancelChannel*(
      s: Switch,
      peerId: PeerId,
      transferId: uint64,
      status: Option[string] = none(string),
      extensions: seq[DataTransferExtension] = @[],
  ): Future[Option[DataTransferChannel]] {.async: (
      raises: [CancelledError, DataTransferError]
    ), public.} =
    ## 取消指定 DataTransfer 通道，返回更新后的通道快照。
    if s.dataTransferManager.isNil:
      return none(DataTransferChannel)
    await s.dataTransferManager.cancelChannel(
      peerId, transferId, status, extensions
    )

  proc dataTransferCompleteChannel*(
      s: Switch,
      peerId: PeerId,
      transferId: uint64,
      status: Option[string] = none(string),
      extensions: seq[DataTransferExtension] = @[],
  ): Future[Option[DataTransferChannel]] {.async: (
      raises: [CancelledError, DataTransferError]
    ), public.} =
    ## 标记指定 DataTransfer 通道完成，返回更新后的通道快照。
    if s.dataTransferManager.isNil:
      return none(DataTransferChannel)
    await s.dataTransferManager.completeChannel(
      peerId, transferId, status, extensions
    )

  proc dataTransferRestartChannel*(
      s: Switch,
      peerId: PeerId,
      transferId: uint64,
      status: Option[string] = none(string),
      extensions: seq[DataTransferExtension] = @[],
  ): Future[Option[DataTransferChannel]] {.async: (
      raises: [CancelledError, DataTransferError]
    ), public.} =
    ## 为丢失或失败的 DataTransfer 通道触发重启流程。
    if s.dataTransferManager.isNil:
      return none(DataTransferChannel)
    await s.dataTransferManager.restartChannel(
      peerId, transferId, status, extensions
    )

proc publishDelegatedProvider*(
    s: Switch,
    key: Cid,
    record: DelegatedProviderRecord,
    ttl: Duration = DefaultProviderTtl,
): Future[void] {.async: (raises: [CancelledError]).} =
  if s.delegatedRoutingStore.isNil:
    return
  await s.delegatedRoutingStore.addProvider($key, record, ttl)

proc publishDelegatedProvider*(
    s: Switch,
    key: Cid,
    schema: string = "peer",
    addresses: seq[MultiAddress] = @[],
    protocols: seq[string] = @[],
    ttl: Duration = DefaultProviderTtl,
): Future[void] {.async: (raises: [CancelledError]).} =
  var addrs = addresses
  if addrs.len == 0:
    addrs = s.peerInfo.listenAddrs
  let record = DelegatedProviderRecord(
    schema: schema,
    peerId: s.peerInfo.peerId,
    addresses: addrs,
    protocols: protocols,
  )
  await s.publishDelegatedProvider(key, record, ttl)

proc publishDelegatedRecord*(
    s: Switch,
    key: seq[byte],
    value: seq[byte],
    time: Option[string],
    ttl: Duration = DefaultRecordTtl,
): Future[void] {.async: (raises: [CancelledError]).} =
  if s.delegatedRoutingStore.isNil:
    return
  let record = DelegatedRoutingRecord(key: key, value: value, timeReceived: time)
  await s.delegatedRoutingStore.addRecord(record, ttl)

proc publishDelegatedRecord*(
    s: Switch,
    key: seq[byte],
    value: seq[byte],
    time: string,
    ttl: Duration = DefaultRecordTtl,
): Future[void] {.async: (raises: [CancelledError]).} =
  let timeOpt =
    if time.len == 0: none(string) else: some(time)
  await s.publishDelegatedRecord(key, value, timeOpt, ttl)

proc publishDelegatedRecord*(
    s: Switch, record: DelegatedRoutingRecord, ttl: Duration = DefaultRecordTtl
): Future[void] {.async: (raises: [CancelledError]).} =
  if s.delegatedRoutingStore.isNil:
    return
  await s.delegatedRoutingStore.addRecord(record, ttl)

proc exportDelegatedAdvertisements*(
    s: Switch
): Future[seq[string]] {.async.} =
  if s.delegatedRoutingStore.isNil:
    return @[]
  await s.delegatedRoutingStore.exportIpniAdvertisements()

proc hasDelegatedRouting*(s: Switch): bool {.public.} =
  ## Check whether a delegated routing client has been configured on the switch.
  not s.delegatedRouting.isNil

proc findDelegatedProviders*(
    s: Switch, key: Cid
): Future[seq[DelegatedProviderRecord]] {.
    async: (raises: [CancelledError, DelegatedRoutingError])
.} =
  ## Query the configured delegated routing client for providers of a CID.
  if s.delegatedRouting.isNil:
    return @[]
  await s.delegatedRouting.findProviders(key)

proc getDelegatedRecords*(
    s: Switch, key: seq[byte]
): Future[seq[DelegatedRoutingRecord]] {.
    async: (raises: [CancelledError, DelegatedRoutingError])
.} =
  ## Fetch delegated routing records by raw key.
  if s.delegatedRouting.isNil:
    return @[]
  await s.delegatedRouting.getRecords(key)

proc getDelegatedRecords*(
    s: Switch, key: string
): Future[seq[DelegatedRoutingRecord]] {.
    async: (raises: [CancelledError, DelegatedRoutingError])
.} =
  ## Convenience overload accepting a string key.
  var bytes = newSeq[byte](key.len)
  for i, ch in key:
    bytes[i] = byte(ch)
  await s.getDelegatedRecords(bytes)

proc revokeDelegatedProvider*(
    s: Switch,
    key: Cid,
    peerId: PeerId,
    schema: string = ""
): Future[int] {.async: (raises: [CancelledError]).} =
  if s.delegatedRoutingStore.isNil:
    return 0
  return await s.delegatedRoutingStore.removeProvider($key, peerId, schema)

proc revokeDelegatedProvider*(
    s: Switch, key: Cid, schema: string = ""
): Future[int] {.async: (raises: [CancelledError]).} =
  return await s.revokeDelegatedProvider(key, s.peerInfo.peerId, schema)

proc clearDelegatedProviders*(
    s: Switch, key: Cid
): Future[int] {.async: (raises: [CancelledError]).} =
  if s.delegatedRoutingStore.isNil:
    return 0
  return await s.delegatedRoutingStore.clearProviders($key)

proc revokeDelegatedRecord*(
    s: Switch,
    key: seq[byte],
    value: seq[byte]
): Future[int] {.async: (raises: [CancelledError]).} =
  if s.delegatedRoutingStore.isNil:
    return 0
  return await s.delegatedRoutingStore.removeRecord(key, value)

proc clearDelegatedRecords*(
    s: Switch, key: seq[byte]
): Future[int] {.async: (raises: [CancelledError]).} =
  if s.delegatedRoutingStore.isNil:
    return 0
  return await s.delegatedRoutingStore.clearRecords(key)

proc updateBandwidthLimits*(
    s: Switch, limits: BandwidthLimitConfig
) {.async: (raises: [CancelledError]), public.} =
  if s.bandwidthManager.isNil:
    return
  await s.bandwidthManager.updateLimits(limits)

proc setBandwidthProtocolLimit*(
    s: Switch, protocol: string, config: ThrottleConfig
) {.async: (raises: [CancelledError]), public.} =
  if s.bandwidthManager.isNil:
    return
  await s.bandwidthManager.setProtocolLimit(protocol, config)

proc clearBandwidthProtocolLimit*(
    s: Switch, protocol: string
) {.async: (raises: [CancelledError]), public.} =
  if s.bandwidthManager.isNil:
    return
  await s.bandwidthManager.clearProtocolLimit(protocol)

proc setBandwidthProtocolPeerLimit*(
    s: Switch, protocol: string, peerId: PeerId, config: ThrottleConfig
) {.async: (raises: [CancelledError]), public.} =
  if s.bandwidthManager.isNil:
    return
  await s.bandwidthManager.setProtocolPeerLimit(protocol, peerId, config)

proc clearBandwidthProtocolPeerLimit*(
    s: Switch, protocol: string, peerId: PeerId
) {.async: (raises: [CancelledError]), public.} =
  if s.bandwidthManager.isNil:
    return
  await s.bandwidthManager.clearProtocolPeerLimit(protocol, peerId)

proc setBandwidthProtocolGroup*(
    s: Switch, protocol: string, group: string
) {.async: (raises: [CancelledError]), public.} =
  if s.bandwidthManager.isNil:
    return
  await s.bandwidthManager.setProtocolGroup(protocol, group)

proc clearBandwidthProtocolGroup*(s: Switch, protocol: string) {.
    async: (raises: [CancelledError]), public.} =
  if s.bandwidthManager.isNil:
    return
  await s.bandwidthManager.clearProtocolGroup(protocol)

when defined(libp2p_msquic_experimental) or defined(libp2p_quic_support):
  proc quicTransports(s: Switch): seq[QuicTransport] =
    for transp in s.transports:
      if not transp.isNil and transp of QuicTransport:
        result.add(QuicTransport(transp))

  when defined(libp2p_msquic_experimental):
    proc msquicTransportStats*(
        s: Switch
    ): seq[quictransport.MsQuicTransportStats] {.public, gcsafe.} =
      for qt in s.quicTransports():
        result.add(quictransport.collectMsQuicTransportStats(qt))

  proc setWebtransportCerthashHistoryLimit*(
      s: Switch, limit: int
  ) {.public.} =
    for qt in s.quicTransports():
      quictransport.setWebtransportCerthashHistoryLimit(qt, limit)

  proc restoreWebtransportCerthashHistory*(
      s: Switch, history: openArray[string]
  ) {.public.} =
    var cached: seq[string] = @[]
    for hash in history:
      cached.add(hash)
    for qt in s.quicTransports():
      quictransport.loadWebtransportCerthashHistory(qt, cached)

  proc webtransportMaxSessions*(s: Switch): Option[uint32] {.public.} =
    for qt in s.quicTransports():
      return some(quictransport.currentWebtransportMaxSessions(qt))
    none(uint32)

  proc setWebtransportMaxSessions*(
      s: Switch, value: uint32
  ) {.public.} =
    let sanitized = if value == 0'u32: 1'u32 else: value
    for qt in s.quicTransports():
      quictransport.setWebtransportMaxSessions(qt, sanitized)

  proc setWebtransportPath*(s: Switch, path: string) {.public.} =
    for qt in s.quicTransports():
      quictransport.setWebtransportPath(qt, path)

  proc setWebtransportQuery*(s: Switch, query: string) {.public.} =
    for qt in s.quicTransports():
      quictransport.setWebtransportQuery(qt, query)

  proc setWebtransportDraft*(s: Switch, draft: string) {.public.} =
    for qt in s.quicTransports():
      quictransport.setWebtransportDraft(qt, draft)

  proc rotateWebtransportCertificate*(
      s: Switch,
      keepHistory: int = quictransport.DefaultWebtransportCerthashHistory,
  ) {.async: (raises: [transport.TransportError, LPError, CancelledError]), public.} =
    let sanitized = if keepHistory < 1: 1 else: keepHistory
    var rotated = false
    for qt in s.quicTransports():
      await quictransport.rotateCertificate(qt, sanitized)
      rotated = true
    if rotated:
      s.webtransportLastRotation = some(Moment.now())

  proc webtransportCerthash*(s: Switch): Option[string] {.public.} =
    for qt in s.quicTransports():
      var current = ""
      try:
        current = qt.currentWebtransportCerthash()
      except quictransport.QuicTransportError as exc:
        trace "failed to read webtransport certhash", error = exc.msg
        continue
      if current.len > 0:
        return some(current)
    none(string)

  proc webtransportRequestTarget*(s: Switch): Option[string] {.public.} =
    for qt in s.quicTransports():
      let target = qt.webtransportRequestTarget()
      if target.len > 0:
        return some(target)
    none(string)

  proc webtransportDraft*(s: Switch): Option[string] {.public.} =
    for qt in s.quicTransports():
      let draft = qt.webtransportDraft()
      if draft.len > 0:
        return some(draft)
    none(string)

  proc webtransportRotationInterval*(s: Switch): Duration {.public, gcsafe.} =
    ## Return the currently configured WebTransport certificate rotation interval.
    s.webtransportRotationInterval

  proc webtransportRotationKeepHistory*(s: Switch): int {.public, gcsafe.} =
    ## Return the number of historical WebTransport certhash entries to retain.
    s.webtransportRotationKeepHistory

  proc webtransportLastRotation*(s: Switch): Option[Moment] {.public, gcsafe.} =
    ## Return the timestamp of the last successful WebTransport certificate rotation.
    s.webtransportLastRotation

  proc webtransportCerthashHistory*(s: Switch): seq[string] {.public, gcsafe.} =
    var seen = initHashSet[string]()
    for qt in s.quicTransports():
      var history: seq[string] = @[]
      try:
        history = qt.currentWebtransportCerthashHistory()
      except quictransport.QuicTransportError as exc:
        trace "failed to read webtransport certhash history", error = exc.msg
        continue
      for hash in history:
        if hash.len == 0 or hash in seen:
          continue
        seen.incl(hash)
        result.add(hash)

  proc webtransportSessions*(
      s: Switch
  ): seq[quictransport.WebtransportSessionSnapshot] {.public, gcsafe.} =
    for qt in s.quicTransports():
      for snapshot in qt.webtransportSessionSnapshots():
        result.add(snapshot)

  proc webtransportRejectionStats*(
      s: Switch
  ): quictransport.WebtransportRejectionStats {.public, gcsafe.} =
    var combined = quictransport.WebtransportRejectionStats()
    for qt in s.quicTransports():
      let stats = qt.webtransportRejectionStats()
      combined.sessionLimit += stats.sessionLimit
      combined.missingConnectProtocol += stats.missingConnectProtocol
      combined.missingDatagram += stats.missingDatagram
      combined.missingSessionAccept += stats.missingSessionAccept
      combined.connectRejected += stats.connectRejected
      combined.invalidRequest += stats.invalidRequest
    combined

  proc cancelWebtransportRotationTask(s: Switch) {.async: (raises: []).} =
    if not s.webtransportRotationTask.isNil:
      try:
        await s.webtransportRotationTask.cancelAndWait()
      except CancelledError:
        discard
      except CatchableError as exc:
        trace "failed to cancel webtransport rotation task", error = exc.msg
      s.webtransportRotationTask = nil

  proc scheduleWebtransportCertificateRotation*(
      s: Switch,
      interval: Duration,
      keepHistory: int = quictransport.DefaultWebtransportCerthashHistory,
  ) {.async: (raises: [CancelledError]), public.} =
    ## Periodically rotate WebTransport certificates for all QUIC transports.
    ## Passing a non-positive `interval` cancels any existing rotation task.
    let sanitizedHistory = if keepHistory < 1: 1 else: keepHistory
    await s.cancelWebtransportRotationTask()
    if interval <= chronos.ZeroDuration:
      s.webtransportRotationInterval = chronos.ZeroDuration
      s.webtransportRotationKeepHistory = sanitizedHistory
      return

    s.webtransportRotationInterval = interval
    s.webtransportRotationKeepHistory = sanitizedHistory

    proc rotationLoop(): Future[void] {.async: (raises: [CancelledError]).} =
      try:
        while true:
          await sleepAsync(interval)
          try:
            await s.rotateWebtransportCertificate(sanitizedHistory)
          except CancelledError as exc:
            raise exc
          except CatchableError as exc:
            warn "webtransport certificate rotation failed",
              interval = interval, keepHistory = sanitizedHistory, error = exc.msg
      finally:
        s.webtransportRotationTask = nil

    let rotationFuture = rotationLoop()
    let baseFuture = cast[Future[void]](rotationFuture)
    s.webtransportRotationTask = baseFuture
    asyncSpawn baseFuture

proc setBandwidthGroupLimit*(
    s: Switch, group: string, config: ThrottleConfig
) {.async: (raises: [CancelledError]), public.} =
  if s.bandwidthManager.isNil:
    return
  await s.bandwidthManager.setGroupLimit(group, config)

proc clearBandwidthGroupLimit*(s: Switch, group: string) {.
    async: (raises: [CancelledError]), public.} =
  if s.bandwidthManager.isNil:
    return
  await s.bandwidthManager.clearGroupLimit(group)

proc setBandwidthGroupPeerLimit*(
    s: Switch, group: string, peerId: PeerId, config: ThrottleConfig
) {.async: (raises: [CancelledError]), public.} =
  if s.bandwidthManager.isNil:
    return
  await s.bandwidthManager.setGroupPeerLimit(group, peerId, config)

proc clearBandwidthGroupPeerLimit*(
    s: Switch, group: string, peerId: PeerId
) {.async: (raises: [CancelledError]), public.} =
  if s.bandwidthManager.isNil:
    return
  await s.bandwidthManager.clearGroupPeerLimit(group, peerId)

proc memoryUsage*(s: Switch, peerId: PeerId): PeerMemoryStats {.public.} =
  if s.memoryManager.isNil:
    return PeerMemoryStats(total: 0, protocols: initTable[string, int]())
  s.memoryManager.stats(peerId)

proc memoryTotals*(s: Switch): int {.public.} =
  if s.memoryManager.isNil:
    0
  else:
    s.memoryManager.totalUsage()

proc updateMemoryLimits*(s: Switch, limits: MemoryLimitConfig) {.public.} =
  if s.memoryManager.isNil:
    return
  s.memoryManager.updateLimits(limits)

proc setMemoryProtocolLimit*(
    s: Switch, protocol: string, limit: MemoryProtocolLimit
) {.public.} =
  if s.memoryManager.isNil:
    return
  s.memoryManager.setProtocolLimit(protocol, limit)

proc clearMemoryProtocolLimit*(s: Switch, protocol: string) {.public.} =
  if s.memoryManager.isNil:
    return
  s.memoryManager.clearProtocolLimit(protocol)

proc setMemoryProtocolPeerLimit*(
    s: Switch, protocol: string, perPeer: int
) {.public.} =
  if s.memoryManager.isNil:
    return
  s.memoryManager.setProtocolPeerLimit(protocol, perPeer)

proc updateResourceManagerConfig*(
    s: Switch, config: ResourceManagerConfig
) {.public.} =
  if s.resourceManager.isNil:
    return
  s.resourceManager.updateConfig(config)

proc updateResourceManagerConnectionLimits*(
    s: Switch, limits: ConnectionLimits
) {.public.} =
  if s.resourceManager.isNil:
    return
  s.resourceManager.updateConnectionLimits(limits)

proc updateResourceManagerStreamLimits*(
    s: Switch, limits: StreamLimits
) {.public.} =
  if s.resourceManager.isNil:
    return
  s.resourceManager.updateStreamLimits(limits)

proc setResourceProtocolLimit*(
    s: Switch, protocol: string, limit: ProtocolLimit
) {.public.} =
  if s.resourceManager.isNil:
    return
  s.resourceManager.setStreamProtocolLimit(protocol, limit)

proc clearResourceProtocolLimit*(s: Switch, protocol: string) {.public.} =
  if s.resourceManager.isNil:
    return
  s.resourceManager.clearStreamProtocolLimit(protocol)

proc setResourceProtocolPeerLimit*(
    s: Switch,
    protocol: string,
    perPeerTotal = UnlimitedLimit,
    perPeerInbound = UnlimitedLimit,
    perPeerOutbound = UnlimitedLimit,
) {.public.} =
  if s.resourceManager.isNil:
    return
  s.resourceManager.setStreamProtocolPeerLimit(
    protocol, perPeerTotal, perPeerInbound, perPeerOutbound
  )

proc setResourceProtocolGroup*(
    s: Switch, protocol: string, group: string
) {.public.} =
  if s.resourceManager.isNil:
    return
  s.resourceManager.setStreamProtocolGroup(protocol, group)

proc clearResourceProtocolGroup*(s: Switch, protocol: string) {.public.} =
  if s.resourceManager.isNil:
    return
  s.resourceManager.clearStreamProtocolGroup(protocol)

proc setResourceProtocolPool*(
    s: Switch, protocol: string, pool: string
) {.public.} =
  if s.resourceManager.isNil:
    return
  s.resourceManager.setStreamProtocolPool(protocol, pool)

proc clearResourceProtocolPool*(s: Switch, protocol: string) {.public.} =
  if s.resourceManager.isNil:
    return
  s.resourceManager.clearStreamProtocolPool(protocol)

proc setResourceGroupLimit*(
    s: Switch, group: string, limit: ProtocolLimit
) {.public.} =
  if s.resourceManager.isNil:
    return
  s.resourceManager.setStreamGroupLimit(group, limit)

proc setResourceGroupPeerLimit*(
    s: Switch,
    group: string,
    perPeerTotal = UnlimitedLimit,
    perPeerInbound = UnlimitedLimit,
    perPeerOutbound = UnlimitedLimit,
) {.public.} =
  if s.resourceManager.isNil:
    return
  s.resourceManager.setStreamGroupPeerLimit(
    group, perPeerTotal, perPeerInbound, perPeerOutbound
  )

proc clearResourceGroupLimit*(s: Switch, group: string) {.public.} =
  if s.resourceManager.isNil:
    return
  s.resourceManager.clearStreamGroupLimit(group)

proc setResourceSharedPoolLimit*(
    s: Switch, pool: string, limit: SharedPoolLimit
) {.public.} =
  if s.resourceManager.isNil:
    return
  s.resourceManager.setSharedPoolLimit(pool, limit)

proc clearResourceSharedPoolLimit*(s: Switch, pool: string) {.public.} =
  if s.resourceManager.isNil:
    return
  s.resourceManager.clearSharedPoolLimit(pool)

proc resourceSnapshot*(s: Switch): ResourceMetrics {.public.} =
  if s.resourceManager.isNil:
    return ResourceMetrics()
  s.resourceManager.snapshot()

method addTransport*(s: Switch, t: Transport) =
  s.transports &= t
  s.dialer.addTransport(t)

proc connectedPeers*(s: Switch, dir: Direction): seq[PeerId] =
  s.connManager.connectedPeers(dir)

proc isStarted*(s: Switch): bool {.public.} =
  s.started

proc isConnected*(s: Switch, peerId: PeerId): bool {.public.} =
  ## returns true if the peer has one or more
  ## associated connections
  ##

  peerId in s.connManager

proc disconnect*(
    s: Switch, peerId: PeerId
) {.gcsafe, public, async: (raises: [CancelledError]).} =
  ## Disconnect from a peer, waiting for the connection(s) to be dropped
  await s.connManager.dropPeer(peerId)

method connect*(
    s: Switch,
    peerId: PeerId,
    addrs: seq[MultiAddress],
    forceDial = false,
    reuseConnection = true,
    dir = Direction.Out,
): Future[void] {.
    public, async: (raises: [DialFailedError, CancelledError], raw: true)
.} =
  ## Connects to a peer without opening a stream to it

  s.dialer.connect(peerId, addrs, forceDial, reuseConnection, dir)

method connect*(
    s: Switch, address: MultiAddress, allowUnknownPeerId = false
): Future[PeerId] {.async: (raises: [DialFailedError, CancelledError], raw: true).} =
  ## Connects to a peer and retrieve its PeerId
  ##
  ## If the P2P part is missing from the MA and `allowUnknownPeerId` is set
  ## to true, this will discover the PeerId while connecting. This exposes
  ## you to MiTM attacks, so it shouldn't be used without care!

  s.dialer.connect(address, allowUnknownPeerId)

method dial*(
    s: Switch, peerId: PeerId, protos: seq[string]
): Future[Connection] {.
    public, async: (raises: [DialFailedError, CancelledError], raw: true)
.} =
  ## Open a stream to a connected peer with the specified `protos`

  s.dialer.dial(peerId, protos)

proc dial*(
    s: Switch, peerId: PeerId, proto: string
): Future[Connection] {.
    public, async: (raises: [DialFailedError, CancelledError], raw: true)
.} =
  ## Open a stream to a connected peer with the specified `proto`

  dial(s, peerId, @[proto])

method dial*(
    s: Switch,
    peerId: PeerId,
    addrs: seq[MultiAddress],
    protos: seq[string],
    forceDial = false,
): Future[Connection] {.
    public, async: (raises: [DialFailedError, CancelledError], raw: true)
.} =
  ## Connected to a peer and open a stream
  ## with the specified `protos`

  s.dialer.dial(peerId, addrs, protos, forceDial)

proc dial*(
    s: Switch, peerId: PeerId, addrs: seq[MultiAddress], proto: string
): Future[Connection] {.
    public, async: (raises: [DialFailedError, CancelledError], raw: true)
.} =
  ## Connected to a peer and open a stream
  ## with the specified `proto`

  dial(s, peerId, addrs, @[proto])

proc mount*[T: LPProtocol](
    s: Switch, proto: T, matcher: Matcher = nil
) {.gcsafe, raises: [LPError], public.} =
  ## mount a protocol to the switch

  if isNil(proto.handler):
    raise newException(LPError, "Protocol has to define a handle method or proc")

  if proto.codec.len == 0:
    raise newException(LPError, "Protocol has to define a codec string")

  if s.started and not proto.started:
    raise newException(LPError, "Protocol not started")

  s.ms.addHandler(proto.codecs, proto, matcher)
  for codec in proto.codecs:
    var exists = false
    for advertised in s.peerInfo.protocols:
      if advertised == codec:
        exists = true
        break
    if not exists:
      s.peerInfo.protocols.add(codec)

proc upgrader(
    switch: Switch, trans: Transport, conn: Connection
) {.async: (raises: [CancelledError, UpgradeError]).} =
  try:
    let muxed = await trans.upgrade(conn, Opt.none(PeerId))
    if not switch.connectionGater.isNil:
      let secureConn = muxed.connection
      if not switch.connectionGater.allowSecured(
          secureConn.peerId, secureConn.observedAddr
        ):
        trace "Connection denied by connection gater after upgrade",
          conn = secureConn, peer = secureConn.peerId, observedAddr = secureConn.observedAddr
        await muxed.close()
        raise newException(
          UpgradeError, "Connection denied by connection gater after upgrade"
        )
    switch.connManager.storeMuxer(muxed)
    await switch.peerStore.identify(muxed)
    await switch.connManager.triggerPeerEvents(
      muxed.connection.peerId,
      PeerEvent(kind: PeerEventKind.Identified, initiator: false),
    )
  except CancelledError as e:
    raise e
  except CatchableError as e:
    raise newException(UpgradeError, "catchable error upgrader: " & e.msg, e)

proc upgradeMonitor(
    switch: Switch, trans: Transport, conn: Connection, upgrades: AsyncSemaphore
) {.async: (raises: []).} =
  var upgradeSuccessful = false
  try:
    await switch.upgrader(trans, conn).wait(30.seconds)
    trace "Connection upgrade succeeded"
    upgradeSuccessful = true
  except CancelledError:
    trace "Connection upgrade cancelled", conn
  except AsyncTimeoutError:
    trace "Connection upgrade timeout", conn
    libp2p_failed_upgrades_incoming.inc()
  except UpgradeError as e:
    trace "Connection upgrade failed", description = e.msg, conn
    libp2p_failed_upgrades_incoming.inc()
  finally:
    if (not upgradeSuccessful) and (not isNil(conn)):
      await conn.close()
    upgrades.release()

proc accept(s: Switch, transport: Transport) {.async: (raises: []).} =
  ## switch accept loop, ran for every transport
  ##

  let upgrades = newAsyncSemaphore(ConcurrentUpgrades)
  while transport.running:
    var conn: Connection
    try:
      debug "About to accept incoming connection"
      # remember to always release the slot when
      # the upgrade succeeds or fails, this is
      # currently done by the `upgradeMonitor`
      await upgrades.acquire() # first wait for an upgrade slot to become available
      let slot = await s.connManager.getIncomingSlot()
      conn =
        try:
          await transport.accept()
        except CancelledError as exc:
          slot.release()
          raise exc
        except CatchableError as exc:
          slot.release()
          raise
            newException(CatchableError, "failed to accept connection: " & exc.msg, exc)
      slot.trackConnection(conn)
      if isNil(conn):
        # A nil connection means that we might have hit a
        # file-handle limit (or another non-fatal error),
        # we can get one on the next try
        debug "Unable to get a connection"
        upgrades.release()
        continue
      if not s.connectionGater.isNil and
          not s.connectionGater.allowAccept(conn.localAddr, conn.observedAddr):
        trace "Incoming connection denied by connection gater",
          localAddr = conn.localAddr, remoteAddr = conn.observedAddr
        await conn.close()
        slot.release()
        upgrades.release()
        continue

      # set the direction of this bottom level transport
      # in order to be able to consume this information in gossipsub if required
      # gossipsub gives priority to connections we make
      conn.transportDir = Direction.In

      debug "Accepted an incoming connection", conn
      asyncSpawn s.upgradeMonitor(transport, conn, upgrades)
    except CancelledError as exc:
      trace "releasing semaphore on cancellation"
      upgrades.release() # always release the slot
      return
    except CatchableError as exc:
      error "Exception in accept loop, exiting", description = exc.msg
      upgrades.release() # always release the slot
      if not isNil(conn):
        await conn.close()
      return

proc stop*(s: Switch) {.public, async: (raises: [CancelledError]).} =
  ## Stop listening on every transport, and
  ## close every active connections

  trace "Stopping switch"

  s.started = false

  try:
    # Stop accepting incoming connections
    var cancelFuts: seq[Future[void]]
    for fut in s.acceptFuts.mitems:
      let cancel = fut.cancelAndWait()
      if cancel != nil:
        cancelFuts.add cancel
    if cancelFuts.len > 0:
      let grouped = allFutures(cancelFuts)
      if grouped != nil:
        await grouped.wait(1.seconds)
  except CatchableError as exc:
    debug "Cannot cancel accepts", description = exc.msg

  when defined(libp2p_quic_support):
    await s.cancelWebtransportRotationTask()

  for service in s.services:
    discard await service.stop(s)

  # close and cleanup all connections
  await s.connManager.close()

  for transp in s.transports:
    try:
      await transp.stop()
    except CancelledError as exc:
      raise exc
    except CatchableError as exc:
      warn "error cleaning up transports", description = exc.msg

  await s.ms.stop()

  trace "Switch stopped"

template runServiceSetup(service: untyped, switchInst: Switch) =
  when compiles(await service.setup(switchInst)):
    let setupFuture = service.setup(switchInst)
    let serviceType = when compiles(service.type.name): service.type.name else: "unknown"
    if setupFuture.isNil:
      warn "service.setup returned nil Future",
        serviceAddr = cast[int](service), serviceType = serviceType
    else:
      warn "service.setup awaiting",
        serviceAddr = cast[int](service), serviceType = serviceType
      discard await setupFuture
  else:
    let serviceType = when compiles(service.type.name): service.type.name else: "unknown"
    warn "service.setup (sync) invoking",
      serviceAddr = cast[int](service), serviceType = serviceType
    service.setup(switchInst)

proc startAsyncImpl(s: Switch) {.async: (raises: [CancelledError, LPError]).} =
  ## Start listening on every transport

  if s.started:
    warn "Switch has already been started"
    return

  debug "starting switch for peer", peerInfo = s.peerInfo
  var startFuts: seq[Future[void]]
  for t in s.transports:
    let addrs = s.peerInfo.listenAddrs.filterIt(t.handles(it))

    s.peerInfo.listenAddrs.keepItIf(it notin addrs)

    if addrs.len > 0 or t.running:
      let fut = t.start(addrs)
      if fut == nil:
        continue
      startFuts.add(fut)
      if t of TcpTransport:
        await fut
        s.acceptFuts.add(s.accept(t))
        s.peerInfo.listenAddrs &= t.addrs

  # some transports require some services to be running
  # in order to finish their startup process

  if startFuts.len > 0:
    await allFutures(startFuts)
  else:
    await sleepAsync(0.seconds)

  for fut in startFuts:
    if fut.failed:
      await s.stop()
      raise newException(
        LPError, "starting transports failed: " & $fut.error.msg, fut.error
      )

  for t in s.transports: # for each transport
    if t.addrs.len > 0 or t.running:
      if t of TcpTransport:
        continue # already added previously
      s.acceptFuts.add(s.accept(t))
      s.peerInfo.listenAddrs &= t.addrs
      warn "transport ready",
        transportAddr = cast[int](t), addrs = t.addrs.mapIt($it)

  if s.services.len == 0:
    warn "no services to setup"
  else:
    for service in s.services:
      runServiceSetup(service, s)

  await s.peerInfo.update()
  await s.ms.start()
  s.started = true

  when defined(libp2p_quic_support):
    if s.webtransportRotationInterval > chronos.ZeroDuration:
      await s.scheduleWebtransportCertificateRotation(
        s.webtransportRotationInterval, s.webtransportRotationKeepHistory
      )

  debug "Started libp2p node", peer = s.peerInfo

proc start*(s: Switch): Future[void] {.public.} =
  let promise = newFuture[void]("Switch.start")
  proc runner() {.async.} =
    try:
      let fut = startAsyncImpl(s)
      if fut != nil:
        await fut
      promise.complete()
    except CancelledError as exc:
      promise.fail(exc)
    except CatchableError as exc:
      promise.fail(exc)
  asyncSpawn runner()
  promise

proc newSwitch*(
    peerInfo: PeerInfo,
    transports: seq[Transport],
    secureManagers: openArray[Secure] = [],
    connManager: ConnManager,
    ms: MultistreamSelect,
    peerStore: PeerStore,
    nameResolver: NameResolver = nil,
    services = newSeq[Service](),
    connectionGater: ConnectionGater = nil,
    bandwidthManager: BandwidthManager = nil,
    resourceManager: ResourceManager = nil,
    memoryManager: MemoryManager = nil,
    delegatedRouting: DelegatedRoutingClient = nil,
    delegatedRoutingStore: DelegatedRoutingStore = nil,
    bitswap: BitswapService = nil,
): Switch {.raises: [LPError].} =
  if secureManagers.len == 0:
    raise newException(LPError, "Provide at least one secure manager")

  let bwManager = if bandwidthManager.isNil: BandwidthManager.new() else: bandwidthManager
  let rm = resourceManager
  let memMgr = memoryManager
  if not rm.isNil:
    connManager.setResourceManager(rm)
    ms.setResourceManager(rm)

  let switch = Switch(
    peerInfo: peerInfo,
    ms: ms,
    transports: transports,
    connManager: connManager,
    peerStore: peerStore,
    dialer:
      Dialer.new(
        peerInfo.peerId, connManager, peerStore, transports, nameResolver, connectionGater
      ),
    nameResolver: nameResolver,
    services: services,
    connectionGater: connectionGater,
    bandwidthManager: bwManager,
    resourceManager: rm,
    memoryManager: memMgr,
    delegatedRouting: delegatedRouting,
    delegatedRoutingStore: delegatedRoutingStore,
    bitswap: bitswap,
  )

  when defined(libp2p_quic_support):
    switch.webtransportRotationTask = nil
    switch.webtransportRotationInterval = chronos.ZeroDuration
    switch.webtransportRotationKeepHistory = quictransport.DefaultWebtransportCerthashHistory
    switch.webtransportLastRotation = none Moment

  switch.connManager.peerStore = peerStore
  return switch
