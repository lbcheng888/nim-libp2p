# Nim-LibP2P
# Copyright (c) 2023 Status Research & Development GmbH
# Licensed under either of
#  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
#  * MIT license ([LICENSE-MIT](LICENSE-MIT))
# at your option.
# This file may not be copied, modified, or distributed except according to
# those terms.

import std/tables

import pkg/[chronos, chronicles, metrics, results]

import
  dial,
  peerid,
  peerinfo,
  peerstore,
  multicodec,
  muxers/muxer,
  multistream,
  connmanager,
  stream/connection,
  transports/transport,
  nameresolving/nameresolver,
  upgrademngrs/upgrade,
  errors,
  connectiongater

export dial, errors, results

logScope:
  topics = "libp2p dialer"

declareCounter(libp2p_total_dial_attempts, "total attempted dials")
declareCounter(libp2p_successful_dials, "dialed successful peers")
declareCounter(libp2p_failed_dials, "failed dials")

type Dialer* = ref object of Dial
  localPeerId*: PeerId
  connManager: ConnManager
  dialLock: Table[PeerId, AsyncLock]
  transports: seq[Transport]
  peerStore: PeerStore
  nameResolver: NameResolver
  gater: ConnectionGater

proc allowsDial(self: Dialer, peer: Opt[PeerId], address: MultiAddress): bool {.inline.} =
  if self.gater.isNil:
    return true
  self.gater.allowDial(peer, address)

method dialAndUpgrade*(
    self: Dialer,
    peerId: Opt[PeerId],
    hostname: string,
    addrs: MultiAddress,
    dir = Direction.Out,
): Future[Muxer] {.async: (raises: [CancelledError, DialFailedError]).} =
  var lastErr: ref CatchableError = nil
  var lastMsg = ""
  var transportMatched = false
  for transport in self.transports: # for each transport
    if transport.handles(addrs): # check if it can dial it
      transportMatched = true
      if not self.allowsDial(peerId, addrs):
        trace "Dial blocked by connection gater",
          addrs, peerId = peerId.get(default(PeerId))
        continue
      trace "Dialing address", addrs, peerId = peerId.get(default(PeerId)), hostname
      let dialed =
        try:
          libp2p_total_dial_attempts.inc()
          await transport.dial(hostname, addrs, peerId)
        except CancelledError as exc:
          trace "Dialing canceled",
            description = exc.msg, peerId = peerId.get(default(PeerId))
          raise exc
        except CatchableError as exc:
          debug "Dialing failed",
            description = exc.msg, peerId = peerId.get(default(PeerId))
          libp2p_failed_dials.inc()
          lastErr = exc
          lastMsg = "dial failed: " & exc.msg
          continue

      libp2p_successful_dials.inc()

      let mux =
        try:
          # This is for the very specific case of a simultaneous dial during DCUtR. In this case, both sides will have
          # an Outbound direction at the transport level. Therefore we update the DCUtR initiator transport direction to Inbound.
          # The if below is more general and might handle other use cases in the future.
          if dialed.dir != dir:
            dialed.dir = dir
          await transport.upgrade(dialed, peerId)
        except CancelledError as exc:
          await dialed.close()
          raise exc
        except CatchableError as exc:
          # If we failed to establish the connection through one transport,
          # we won't succeeded through another - no use in trying again
          await dialed.close()
          debug "Connection upgrade failed",
            description = exc.msg, peerId = peerId.get(default(PeerId))
          if dialed.dir == Direction.Out:
            libp2p_failed_upgrades_outgoing.inc()
          else:
            libp2p_failed_upgrades_incoming.inc()

          lastErr = exc
          lastMsg = "upgrade failed: " & exc.msg
          continue

      if mux.isNil:
        debug "Dial upgrade returned nil, treating as failure",
          direction = $dialed.dir,
          address = $addrs
        try:
          await dialed.close()
        except CatchableError as exc:
          trace "Failed to close dialed connection after nil mux",
            direction = $dialed.dir,
            address = $addrs,
            description = exc.msg
        if dialed.dir == Direction.Out:
          libp2p_failed_upgrades_outgoing.inc()
        else:
          libp2p_failed_upgrades_incoming.inc()
        lastMsg = "upgrade returned nil for address=" & $addrs
        continue
      debug "Dial successful", peerId = mux.connection.peerId
      return mux
  if not transportMatched:
    raise newException(DialFailedError, "no transport handles address: " & $addrs)
  if lastMsg.len == 0:
    raise newException(DialFailedError, "dial failed without error details for address=" & $addrs)
  raise newException(DialFailedError, lastMsg, lastErr)

proc expandDnsAddr(
    self: Dialer, peerId: Opt[PeerId], address: MultiAddress
): Future[seq[(MultiAddress, Opt[PeerId])]] {.
    async: (raises: [CancelledError, MaError, TransportAddressError, LPError])
.} =
  if not DNS.matchPartial(address):
    return @[(address, peerId)]
  if isNil(self.nameResolver):
    info "Can't resolve DNSADDR without NameResolver", ma = address
    return @[]

  trace "Start trying to resolve addresses"
  let
    toResolve =
      if peerId.isSome:
        try:
          address & MultiAddress.init(multiCodec("p2p"), peerId.tryGet()).tryGet()
        except ResultError[void]:
          raiseAssert "checked with if"
      else:
        address
    resolved = await self.nameResolver.resolveDnsAddr(toResolve)

  debug "resolved addresses",
    originalAddresses = toResolve, resolvedAddresses = resolved

  for resolvedAddress in resolved:
    let lastPartRes = resolvedAddress[^1]
    if lastPartRes.isErr:
      continue
    let lastPart = lastPartRes.get()

    if lastPart.protoCode != Result[MultiCodec, string].ok(multiCodec("p2p")):
      result.add((resolvedAddress, peerId))
      continue

    let peerIdBytesRes = lastPart.protoArgument()
    if peerIdBytesRes.isErr:
      continue

    let addrPeerIdRes = PeerId.init(peerIdBytesRes.get())
    if addrPeerIdRes.isErr:
      continue

    let baseAddrRes = resolvedAddress[0 ..^ 2]
    if baseAddrRes.isErr:
      continue

    result.add((baseAddrRes.get(), Opt.some(addrPeerIdRes.get())))

method dialAndUpgrade*(
    self: Dialer, peerId: Opt[PeerId], addrs: seq[MultiAddress], dir = Direction.Out
): Future[Muxer] {.
    async: (raises: [CancelledError, DialFailedError, MaError, TransportAddressError, LPError])
.} =
  debug "Dialing peer", peerId = peerId.get(default(PeerId)), addrs
  var lastErr: ref CatchableError = nil
  var lastMsg = ""

  for rawAddress in addrs:
    # resolve potential dnsaddr
    let addresses = await self.expandDnsAddr(peerId, rawAddress)
    for (expandedAddress, addrPeerId) in addresses:
      # DNS resolution
      let
        hostname = expandedAddress.getHostname()
        resolvedAddresses =
          if isNil(self.nameResolver):
            @[expandedAddress]
          else:
            await self.nameResolver.resolveMAddress(expandedAddress)

      debug "Expanded address and hostname",
        expandedAddress = expandedAddress,
        hostname = hostname,
        resolvedAddresses = resolvedAddresses

      for resolvedAddress in resolvedAddresses:
        if not self.allowsDial(addrPeerId, resolvedAddress):
          trace "Dial blocked by connection gater",
            resolvedAddress, peerId = addrPeerId.get(default(PeerId))
          continue
        try:
          result = await self.dialAndUpgrade(addrPeerId, hostname, resolvedAddress, dir)
          if not isNil(result):
            return result
        except CancelledError as exc:
          raise exc
        except CatchableError as exc:
          lastErr = exc
          lastMsg = exc.msg
          continue

  if lastMsg.len > 0:
    raise newException(DialFailedError, "all dial attempts failed: " & lastMsg, lastErr)

proc tryReusingConnection(self: Dialer, peerId: PeerId): Opt[Muxer] =
  let muxer = self.connManager.selectMuxer(peerId)
  if muxer == nil:
    return Opt.none(Muxer)

  trace "Reusing existing connection", muxer, direction = $muxer.connection.dir
  return Opt.some(muxer)

proc internalConnect(
    self: Dialer,
    peerId: Opt[PeerId],
    addrs: seq[MultiAddress],
    forceDial: bool,
    reuseConnection = true,
    dir = Direction.Out,
): Future[Muxer] {.async: (raises: [DialFailedError, CancelledError]).} =
  if Opt.some(self.localPeerId) == peerId:
    raise newException(DialFailedError, "internalConnect can't dial self!")

  # Ensure there's only one in-flight attempt per peer
  let lockKey = peerId.get(default(PeerId))
  var lock = self.dialLock.getOrDefault(lockKey, AsyncLock(nil))
  if lock.isNil:
    lock = newAsyncLock()
  self.dialLock[lockKey] = lock
  await lock.acquire()
  defer:
    try:
      lock.release()
    except AsyncLockError as e:
      raiseAssert "lock must have been acquired in line above: " & e.msg

  if reuseConnection:
    peerId.withValue(pid):
      self.tryReusingConnection(pid).withValue(mux):
        if self.gater.allowSecured(pid, mux.connection.observedAddr):
          return mux
        trace "Existing connection denied by connection gater, dropping", peerId = pid
        await self.connManager.dropPeer(pid)

  let slot =
    try:
      self.connManager.getOutgoingSlot(forceDial)
    except TooManyConnectionsError as exc:
      raise newException(
        DialFailedError, "failed getOutgoingSlot in internalConnect: " & exc.msg, exc
      )

  let muxed =
    try:
      await self.dialAndUpgrade(peerId, addrs, dir)
    except CancelledError as exc:
      slot.release()
      raise exc
    except CatchableError as exc:
      slot.release()
      raise newException(
        DialFailedError, "failed dialAndUpgrade in internalConnect: " & exc.msg, exc
      )

  slot.trackMuxer(muxed)
  if isNil(muxed): # None of the addresses connected
    raise newException(
      DialFailedError, "Unable to establish outgoing link in internalConnect"
    )

  if not self.gater.allowSecured(muxed.connection.peerId, muxed.connection.observedAddr):
    trace "Connection denied by connection gater after upgrade",
      peerId = muxed.connection.peerId, observedAddr = muxed.connection.observedAddr
    await muxed.close()
    raise newException(
      DialFailedError, "Connection denied by connection gater after upgrade"
    )

  try:
    self.connManager.storeMuxer(muxed)
    await self.peerStore.identify(muxed)
    await self.connManager.triggerPeerEvents(
      muxed.connection.peerId,
      PeerEvent(kind: PeerEventKind.Identified, initiator: true),
    )
    return muxed
  except CancelledError as exc:
    await muxed.close()
    raise exc
  except CatchableError as exc:
    trace "Failed to finish outgoing upgrade", description = exc.msg
    await muxed.close()
    raise newException(
      DialFailedError,
      "Failed to finish outgoing upgrade in internalConnect: " & exc.msg,
      exc,
    )

method connect*(
    self: Dialer,
    peerId: PeerId,
    addrs: seq[MultiAddress],
    forceDial = false,
    reuseConnection = true,
    dir = Direction.Out,
) {.async: (raises: [DialFailedError, CancelledError]).} =
  ## connect remote peer without negotiating
  ## a protocol
  ##

  if self.connManager.connCount(peerId) > 0 and reuseConnection:
    return

  discard
    await self.internalConnect(Opt.some(peerId), addrs, forceDial, reuseConnection, dir)

method connect*(
    self: Dialer, address: MultiAddress, allowUnknownPeerId = false
): Future[PeerId] {.async: (raises: [DialFailedError, CancelledError]).} =
  ## Connects to a peer and retrieve its PeerId

  parseFullAddress(address).toOpt().withValue(fullAddress):
    return (
      await self.internalConnect(Opt.some(fullAddress[0]), @[fullAddress[1]], false)
    ).connection.peerId

  if allowUnknownPeerId == false:
    raise newException(
      DialFailedError, "Address without PeerID and unknown peer id disabled in connect"
    )

  return
    (await self.internalConnect(Opt.none(PeerId), @[address], false)).connection.peerId

method negotiateStream*(
    self: Dialer, conn: Connection, protos: seq[string]
): Future[Connection] {.async: (raises: [CatchableError]).} =
  trace "Negotiating stream", conn, protos
  let selected = await MultistreamSelect.select(conn, protos)
  if not protos.contains(selected):
    await conn.closeWithEOF()
    raise newException(DialFailedError, "Unable to select sub-protocol: " & $protos)
  return conn

method tryDial*(
    self: Dialer, peerId: PeerId, addrs: seq[MultiAddress]
): Future[Opt[MultiAddress]] {.async: (raises: [DialFailedError, CancelledError]).} =
  ## Create a protocol stream in order to check
  ## if a connection is possible.
  ## Doesn't use the Connection Manager to save it.
  ##

  trace "Check if it can dial", peerId, addrs
  try:
    let mux = await self.dialAndUpgrade(Opt.some(peerId), addrs)
    if mux.isNil():
      raise newException(DialFailedError, "No valid multiaddress in tryDial")
    await mux.close()
    return mux.connection.observedAddr
  except CancelledError as exc:
    raise exc
  except CatchableError as exc:
    raise newException(DialFailedError, "tryDial failed: " & exc.msg, exc)

method dial*(
    self: Dialer, peerId: PeerId, protos: seq[string]
): Future[Connection] {.async: (raises: [DialFailedError, CancelledError]).} =
  ## create a protocol stream over an
  ## existing connection
  ##

  trace "Dialing (existing)", peerId, protos

  try:
    let stream = await self.connManager.getStream(peerId)
    if stream.isNil:
      raise newException(
        DialFailedError,
        "Couldn't get muxed stream in dial for peer_id: " & shortLog(peerId),
      )
    return await self.negotiateStream(stream, protos)
  except CancelledError as exc:
    trace "Dial canceled", description = exc.msg
    raise exc
  except CatchableError as exc:
    trace "Error dialing", description = exc.msg
    raise newException(DialFailedError, "failed dial existing: " & exc.msg)

method dial*(
    self: Dialer,
    peerId: PeerId,
    addrs: seq[MultiAddress],
    protos: seq[string],
    forceDial = false,
): Future[Connection] {.async: (raises: [DialFailedError, CancelledError]).} =
  ## create a protocol stream and establish
  ## a connection if one doesn't exist already
  ##

  var
    conn: Muxer
    stream: Connection

  proc cleanup() {.async: (raises: []).} =
    if not (isNil(stream)):
      await stream.closeWithEOF()

    if not (isNil(conn)):
      await conn.close()

  try:
    trace "Dialing (new)", peerId, protos
    conn = await self.internalConnect(Opt.some(peerId), addrs, forceDial)
    trace "Opening stream", conn
    stream = await self.connManager.getStream(conn)

    if isNil(stream):
      raise newException(
        DialFailedError,
        "Couldn't get muxed stream in new dial for remote_peer_id: " & shortLog(peerId),
      )

    return await self.negotiateStream(stream, protos)
  except CancelledError as exc:
    trace "Dial canceled", conn, description = exc.msg
    await cleanup()
    raise exc
  except CatchableError as exc:
    debug "Error dialing", conn, description = exc.msg
    await cleanup()
    raise newException(DialFailedError, "failed new dial: " & exc.msg, exc)

method addTransport*(self: Dialer, t: Transport) =
  self.transports &= t

proc new*(
    T: type Dialer,
    localPeerId: PeerId,
    connManager: ConnManager,
    peerStore: PeerStore,
    transports: seq[Transport],
    nameResolver: NameResolver = nil,
    connectionGater: ConnectionGater = nil,
): Dialer =
  T(
    localPeerId: localPeerId,
    connManager: connManager,
    transports: transports,
    peerStore: peerStore,
    nameResolver: nameResolver,
    gater: connectionGater,
  )
