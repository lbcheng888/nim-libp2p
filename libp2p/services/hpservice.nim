# Nim-LibP2P
# Copyright (c) 2022 Status Research & Development GmbH
# Licensed under either of
#  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
#  * MIT license ([LICENSE-MIT](LICENSE-MIT))
# at your option.
# This file may not be copied, modified, or distributed except according to
# those terms.

{.push raises: [].}

import std/[sets, strutils, tables, sequtils]

import chronos, chronicles

import ../switch, ../wire, ../multistream, ../protocols/ping, ../multiaddress
import ../stream/connection
import ../muxers/muxer
import ../utils/future as futureutils
import ../protocols/rendezvous
import ../services/autorelayservice
import ../protocols/connectivity/relay/relay
import ../protocols/connectivity/autonat/service
import ../protocols/connectivity/dcutr/[client, core, server]
import ../multicodec

logScope:
  topics = "libp2p hpservice"

type
  HPServiceConfig* = object
    directProbeTimeout*: Duration
    maxProbeCandidates*: int
    keepaliveInterval*: Duration
    relayCloseDelay*: Duration
    upgradeDelay*: Duration
    dcutrConnectTimeout*: Duration
    maxDcutrDialableAddrs*: int

  HPService* = ref object of Service
    newConnectedPeerHandler: PeerEventHandler
    connectionEventHandler: ConnEventHandler
    onNewStatusHandler: StatusAndConfidenceHandler
    config*: HPServiceConfig
    autoRelayService: AutoRelayService
    autonatService: AutonatService
    pingProto: Ping
    upgradeTasks: Table[PeerId, Future[void]]
    keepaliveTasks: Table[PeerId, Future[void]]
    relayCloseTasks: Table[PeerId, Future[void]]

proc init*(
    T: typedesc[HPServiceConfig],
    directProbeTimeout = 2.seconds,
    maxProbeCandidates = 6,
    keepaliveInterval = 15.seconds,
    relayCloseDelay = 2.seconds,
    upgradeDelay = 500.milliseconds,
    dcutrConnectTimeout = 15.seconds,
    maxDcutrDialableAddrs = 8,
): T =
  T(
    directProbeTimeout: directProbeTimeout,
    maxProbeCandidates: maxProbeCandidates,
    keepaliveInterval: keepaliveInterval,
    relayCloseDelay: relayCloseDelay,
    upgradeDelay: upgradeDelay,
    dcutrConnectTimeout: dcutrConnectTimeout,
    maxDcutrDialableAddrs: maxDcutrDialableAddrs,
  )

proc new*(
    T: typedesc[HPService],
    autonatService: AutonatService,
    autoRelayService: AutoRelayService,
    config: HPServiceConfig,
): T =
  T(
    config: config,
    autonatService: autonatService,
    autoRelayService: autoRelayService,
    pingProto: Ping.new(),
    upgradeTasks: initTable[PeerId, Future[void]](),
    keepaliveTasks: initTable[PeerId, Future[void]](),
    relayCloseTasks: initTable[PeerId, Future[void]](),
  )

proc new*(
    T: typedesc[HPService],
    autonatService: AutonatService,
    autoRelayService: AutoRelayService,
    directProbeTimeout = 2.seconds,
    maxProbeCandidates = 6,
    keepaliveInterval = 15.seconds,
): T =
  T.new(
    autonatService,
    autoRelayService,
    HPServiceConfig.init(
      directProbeTimeout = directProbeTimeout,
      maxProbeCandidates = maxProbeCandidates,
      keepaliveInterval = keepaliveInterval,
    ),
  )

proc cancelTask(tasks: var Table[PeerId, Future[void]], peerId: PeerId) {.gcsafe.} =
  let fut = tasks.getOrDefault(peerId)
  if not isNil(fut) and not fut.finished:
    fut.cancelSoon()
  if tasks.hasKey(peerId):
    tasks.del(peerId)

proc takeTasks(tasks: var Table[PeerId, Future[void]]): seq[Future[void]] {.gcsafe.} =
  for _, fut in tasks:
    result.add(fut)
  tasks.clear()

proc isRelayedAddr(address: MultiAddress): bool {.gcsafe.} =
  address.contains(multiCodec("p2p-circuit")).get(false)

proc isQuicAddr(address: MultiAddress): bool {.gcsafe.} =
  let rendered = $address
  rendered.contains("/quic-v1") or rendered.contains("/quic")

proc isTcpAddr(address: MultiAddress): bool {.gcsafe.} =
  ($address).contains("/tcp/")

proc isIpv6Addr(address: MultiAddress): bool {.gcsafe.} =
  let rendered = $address
  rendered.startsWith("/ip6/") or rendered.startsWith("/dns6/")

proc sampleAddrs(addrs: openArray[MultiAddress], limit = 4): seq[string] {.gcsafe.} =
  if limit <= 0:
    return @[]
  for addr in addrs:
    if result.len >= limit:
      break
    result.add($addr)

proc prioritizedHolePunchableAddrs(
    self: HPService, addrs: seq[MultiAddress], publicOnly: bool, limit: int
): seq[MultiAddress] {.gcsafe.} =
  let dialable =
    try:
      getHolePunchableAddrs(addrs)
    except LPError:
      return @[]
  var
    seen = initHashSet[string]()
    ipv6Quic, ipv6Tcp, publicQuic, publicTcp, other: seq[MultiAddress]

  for address in dialable:
    if isRelayedAddr(address):
      continue
    if publicOnly and not address.isPublicMA():
      continue
    let key = $address
    if key in seen:
      continue
    seen.incl(key)
    if isIpv6Addr(address) and isQuicAddr(address):
      ipv6Quic.add(address)
    elif isIpv6Addr(address) and isTcpAddr(address):
      ipv6Tcp.add(address)
    elif isQuicAddr(address):
      publicQuic.add(address)
    elif isTcpAddr(address):
      publicTcp.add(address)
    else:
      other.add(address)

  result = ipv6Quic & ipv6Tcp & publicQuic & publicTcp & other
  if limit > 0 and result.len > limit:
    result.setLen(limit)

proc collectDirectProbeAddrs(
    self: HPService, switch: Switch, peerId: PeerId
): seq[MultiAddress] {.gcsafe.} =
  self.prioritizedHolePunchableAddrs(
    switch.peerStore.getAddresses(peerId),
    publicOnly = true,
    limit = self.config.maxProbeCandidates,
  )

proc collectLocalHolePunchAddrs(self: HPService, switch: Switch): seq[MultiAddress] {.gcsafe.} =
  var natAddrs = switch.peerStore.getMostObservedProtosAndPorts()
  if natAddrs.len == 0:
    natAddrs = switch.peerInfo.listenAddrs.mapIt(switch.peerStore.guessDialableAddr(it))
  self.prioritizedHolePunchableAddrs(
    natAddrs,
    publicOnly = false,
    limit = self.config.maxProbeCandidates,
  )

proc relayConnections(self: HPService, switch: Switch, peerId: PeerId): seq[Connection] {.gcsafe.} =
  switch.connManager
  .getConnections()
  .getOrDefault(peerId)
  .mapIt(it.connection)
  .filterIt(isRelayed(it) and not it.closed)

proc hasRelayConn(self: HPService, switch: Switch, peerId: PeerId): bool {.gcsafe.} =
  self.relayConnections(switch, peerId).len > 0

proc hasDirectConn(self: HPService, switch: Switch, peerId: PeerId): bool {.gcsafe.} =
  let connectionsTable = switch.connManager.getConnections()
  connectionsTable.getOrDefault(peerId).anyIt(not isRelayed(it.connection) and
      not it.connection.closed)

proc selectDirectMuxer(self: HPService, switch: Switch, peerId: PeerId): Muxer {.gcsafe.} =
  let connections = switch.connManager.getConnections().getOrDefault(peerId)
  for muxer in connections:
    if not isRelayed(muxer.connection) and not muxer.connection.closed and
        muxer.connection.dir == Direction.Out:
      return muxer
  for muxer in connections:
    if not isRelayed(muxer.connection) and not muxer.connection.closed:
      return muxer

proc closeRelayConns(
    self: HPService, switch: Switch, peerId: PeerId
): Future[void] {.async: (raises: [CancelledError]), gcsafe.} =
  await sleepAsync(self.config.relayCloseDelay)
  if not self.hasDirectConn(switch, peerId):
    info "Hole punch keeping relay path because direct connection disappeared before relay close",
      peerId
    return
  for relayedConn in self.relayConnections(switch, peerId):
    await relayedConn.close()

proc scheduleRelayClose(self: HPService, switch: Switch, peerId: PeerId) {.gcsafe.} =
  let current = self.relayCloseTasks.getOrDefault(peerId)
  if not isNil(current) and not current.finished:
    return
  let fut = self.closeRelayConns(switch, peerId)
  self.relayCloseTasks[peerId] = fut
  let cleanup = proc(): Future[void] {.async.} =
    try:
      await fut
    except CatchableError:
      discard
  asyncSpawn cleanup()

proc tryStartingDirectConn(
    self: HPService, switch: Switch, peerId: PeerId, addrs: seq[MultiAddress]
): Future[bool] {.async: (raises: [CancelledError]), gcsafe.} =
  proc tryConnect(
      address: MultiAddress
  ): Future[bool] {.async: (raises: [DialFailedError, CancelledError]).} =
    debug "Trying to create direct connection", peerId, address
    await switch.connect(peerId, @[address], true, false)
    debug "Direct connection created", peerId, address
    true

  if addrs.len == 0:
    return false

  let attempts = addrs.mapIt(tryConnect(it))
  try:
    discard await futureutils.anyCompleted(attempts).wait(self.config.directProbeTimeout)
    return self.hasDirectConn(switch, peerId)
  except AsyncTimeoutError as err:
    info "Hole punch direct candidate race timed out", peerId,
      candidateCount = addrs.len,
      candidateAddrs = sampleAddrs(addrs),
      timeout = self.config.directProbeTimeout,
      description = err.msg
    return false
  except AllFuturesFailedError as err:
    info "Hole punch direct candidate race failed", peerId,
      candidateCount = addrs.len,
      candidateAddrs = sampleAddrs(addrs),
      description = err.msg
    return false
  finally:
    for fut in attempts:
      fut.cancel()

proc scheduleUpgrade(self: HPService, switch: Switch, peerId: PeerId) {.gcsafe.}
proc refreshPeerPathState(self: HPService, switch: Switch, peerId: PeerId) {.gcsafe.}

proc ensureDirectKeepalive(self: HPService, switch: Switch, peerId: PeerId) {.gcsafe.} =
  if not self.hasDirectConn(switch, peerId):
    cancelTask(self.keepaliveTasks, peerId)
    return
  let current = self.keepaliveTasks.getOrDefault(peerId)
  if not isNil(current) and not current.finished:
    return

  let loopFut = proc(): Future[void] {.async: (raises: [CancelledError]).} =
    while true:
      await sleepAsync(self.config.keepaliveInterval)
      let directMuxer = self.selectDirectMuxer(switch, peerId)
      if isNil(directMuxer):
        info "Hole punch direct keepalive stopping because no direct connection remains", peerId
        return
      var stream: Connection
      try:
        stream = await switch.connManager.getStream(directMuxer)
        if not await switch.ms.select(stream, PingCodec):
          debug "Hole punch direct keepalive could not negotiate ping protocol", peerId
          continue
        let rtt = await self.pingProto.ping(stream)
        trace "Hole punch direct keepalive succeeded", peerId, rtt
      except CancelledError as err:
        raise err
      except CatchableError as err:
        debug "Hole punch direct keepalive failed", peerId, description = err.msg
        if not self.hasDirectConn(switch, peerId) and self.hasRelayConn(switch, peerId):
          cancelTask(self.relayCloseTasks, peerId)
          self.scheduleUpgrade(switch, peerId)
          return
      finally:
        if not isNil(stream):
          await stream.closeWithEOF()

  let fut = loopFut()
  self.keepaliveTasks[peerId] = fut
  let watcher = proc(): Future[void] {.async.} =
    try:
      await fut
    except CatchableError:
      discard
  asyncSpawn watcher()

proc relayUpgradeWorker(
    self: HPService, switch: Switch, peerId: PeerId
) {.async: (raises: [CancelledError]), gcsafe.} =
  try:
    await sleepAsync(self.config.upgradeDelay) # let identify/address book fill in direct candidates
    if self.hasDirectConn(switch, peerId):
      if self.hasRelayConn(switch, peerId):
        self.ensureDirectKeepalive(switch, peerId)
        self.scheduleRelayClose(switch, peerId)
      else:
        cancelTask(self.keepaliveTasks, peerId)
      return

    let relays = self.relayConnections(switch, peerId)
    if relays.len == 0:
      return
    info "Hole punch relay path observed", peerId, relayCount = relays.len

    let directProbeAddrs = self.collectDirectProbeAddrs(switch, peerId)
    if directProbeAddrs.len > 0:
      info "Hole punch racing direct candidates", peerId,
        candidateCount = directProbeAddrs.len,
        candidateAddrs = sampleAddrs(directProbeAddrs)
      if await self.tryStartingDirectConn(switch, peerId, directProbeAddrs):
        info "Hole punch direct candidate race succeeded before dcutr", peerId,
          candidateCount = directProbeAddrs.len,
          candidateAddrs = sampleAddrs(directProbeAddrs)
        self.ensureDirectKeepalive(switch, peerId)
        self.scheduleRelayClose(switch, peerId)
        return

    let dcutrClient = DcutrClient.new(
      connectTimeout = self.config.dcutrConnectTimeout,
      maxDialableAddrs = self.config.maxDcutrDialableAddrs,
    )
    let natAddrs = self.collectLocalHolePunchAddrs(switch)
    info "Hole punch invoking dcutr", peerId,
      natAddrCount = natAddrs.len,
      natAddrs = sampleAddrs(natAddrs)
    var dcutrOutcome = DcutrLocalUnsupportedAddrs
    var dcutrErr: ref CatchableError = nil
    try:
      dcutrOutcome = await dcutrClient.startSync(switch, peerId, natAddrs)
    except CancelledError as err:
      raise err
    except CatchableError as err:
      dcutrErr = err

    if self.hasDirectConn(switch, peerId):
      info "Hole punch obtained direct connection after dcutr", peerId, dcutrOutcome
      self.ensureDirectKeepalive(switch, peerId)
      self.scheduleRelayClose(switch, peerId)
      return

    if not isNil(dcutrErr):
      raise dcutrErr
    info "Hole punch keeping relay connection after dcutr attempt", peerId, dcutrOutcome
  except CancelledError as err:
    raise err
  except CatchableError as err:
    warn "Hole punching failed during dcutr", peerId, description = err.msg

proc scheduleUpgrade(self: HPService, switch: Switch, peerId: PeerId) {.gcsafe.} =
  let current = self.upgradeTasks.getOrDefault(peerId)
  if not isNil(current) and not current.finished:
    return
  let fut = relayUpgradeWorker(self, switch, peerId)
  self.upgradeTasks[peerId] = fut
  let watcher = proc(): Future[void] {.async.} =
    try:
      await fut
    except CatchableError:
      discard
  asyncSpawn watcher()

proc refreshPeerPathState(self: HPService, switch: Switch, peerId: PeerId) {.gcsafe.} =
  if self.hasDirectConn(switch, peerId):
    if self.hasRelayConn(switch, peerId):
      self.ensureDirectKeepalive(switch, peerId)
      self.scheduleRelayClose(switch, peerId)
    else:
      cancelTask(self.keepaliveTasks, peerId)
      cancelTask(self.relayCloseTasks, peerId)
    return

  cancelTask(self.keepaliveTasks, peerId)
  cancelTask(self.relayCloseTasks, peerId)
  if self.hasRelayConn(switch, peerId):
    self.scheduleUpgrade(switch, peerId)
  else:
    cancelTask(self.upgradeTasks, peerId)

proc newConnectedPeerHandler(
    self: HPService, switch: Switch, peerId: PeerId, event: PeerEvent
) {.async: (raises: [CancelledError]), gcsafe.} =
  self.refreshPeerPathState(switch, peerId)

proc connectionEventHandler(
    self: HPService, switch: Switch, peerId: PeerId, event: ConnEvent
) {.async: (raises: [CancelledError]), gcsafe.} =
  self.refreshPeerPathState(switch, peerId)

method setup*(
    self: HPService, switch: Switch
): Future[bool] {.async: (raises: [CancelledError]).} =
  var hasBeenSetup = await procCall Service(self).setup(switch)
  hasBeenSetup = hasBeenSetup and await self.autonatService.setup(switch)

  if hasBeenSetup:
    try:
      let dcutrProto = Dcutr.new(switch)
      switch.mount(dcutrProto)
    except LPError as err:
      error "Failed to mount Dcutr", description = err.msg

    if PingCodec notin switch.peerInfo.protocols:
      try:
        switch.mount(self.pingProto)
      except LPError as err:
        error "Failed to mount Ping", description = err.msg

    self.newConnectedPeerHandler = proc(
        peerId: PeerId, event: PeerEvent
    ) {.async: (raises: [CancelledError]).} =
      await newConnectedPeerHandler(self, switch, peerId, event)

    self.connectionEventHandler = proc(
        peerId: PeerId, event: ConnEvent
    ) {.async: (raises: [CancelledError]).} =
      await connectionEventHandler(self, switch, peerId, event)

    switch.connManager.addPeerEventHandler(
      self.newConnectedPeerHandler, PeerEventKind.Joined
    )
    switch.connManager.addConnEventHandler(
      self.connectionEventHandler, ConnEventKind.Connected
    )
    switch.connManager.addConnEventHandler(
      self.connectionEventHandler, ConnEventKind.Disconnected
    )

    self.onNewStatusHandler = proc(
        networkReachability: NetworkReachability, confidence: Opt[float]
    ) {.async: (raises: [CancelledError]).} =
      if networkReachability == NetworkReachability.NotReachable and
          not self.autoRelayService.isRunning():
        discard await self.autoRelayService.setup(switch)
      elif networkReachability == NetworkReachability.Reachable and
          self.autoRelayService.isRunning():
        discard await self.autoRelayService.stop(switch)

      for t in switch.transports:
        t.networkReachability = networkReachability

    self.autonatService.statusAndConfidenceHandler(self.onNewStatusHandler)
  return hasBeenSetup

method run*(
    self: HPService, switch: Switch
) {.public, async: (raises: [CancelledError]).} =
  await self.autonatService.run(switch)

method stop*(
    self: HPService, switch: Switch
): Future[bool] {.public, async: (raises: [CancelledError]).} =
  if not isNil(self.newConnectedPeerHandler):
    switch.connManager.removePeerEventHandler(
      self.newConnectedPeerHandler, PeerEventKind.Joined
    )
  if not isNil(self.connectionEventHandler):
    switch.connManager.removeConnEventHandler(
      self.connectionEventHandler, ConnEventKind.Connected
    )
    switch.connManager.removeConnEventHandler(
      self.connectionEventHandler, ConnEventKind.Disconnected
    )
  self.autonatService.statusAndConfidenceHandler(nil)

  let hasBeenStopped = await procCall Service(self).stop(switch)
  if not hasBeenStopped:
    return false

  for fut in takeTasks(self.upgradeTasks):
    if not isNil(fut) and not fut.finished:
      await fut.cancelAndWait()
  for fut in takeTasks(self.keepaliveTasks):
    if not isNil(fut) and not fut.finished:
      await fut.cancelAndWait()
  for fut in takeTasks(self.relayCloseTasks):
    if not isNil(fut) and not fut.finished:
      await fut.cancelAndWait()
  discard await self.autonatService.stop(switch)
  if self.autoRelayService.isRunning():
    discard await self.autoRelayService.stop(switch)
  true
