# Nim-LibP2P
# Copyright (c) 2023-2024 Status Research & Development GmbH
# Licensed under either of
#  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
#  * MIT license ([LICENSE-MIT](LICENSE-MIT))
# at your option.
# This file may not be copied, modified, or distributed except according to
# those terms.

{.push raises: [].}

import std/sequtils
import pkg/[chronos, chronicles, metrics]

import ../upgrademngrs/upgrade, ../muxers/muxer, ../pnet, ../bandwidthmanager,
       ../memorymanager

export Upgrade

logScope:
  topics = "libp2p muxedupgrade"

type MuxedUpgrade* = ref object of Upgrade
  muxers*: seq[MuxerProvider]
  streamHandler*: StreamHandler
  protector*: ConnectionProtector
  bandwidthManager*: BandwidthManager
  memoryManager*: MemoryManager

proc setBandwidthManager*(self: MuxedUpgrade, manager: BandwidthManager) =
  self.bandwidthManager = manager

proc setMemoryManager*(self: MuxedUpgrade, manager: MemoryManager) =
  self.memoryManager = manager

func getMuxerByCodec(self: MuxedUpgrade, muxerName: string): Opt[MuxerProvider] =
  if muxerName.len == 0 or muxerName == "na":
    return Opt.none(MuxerProvider)
  for m in self.muxers:
    if muxerName == m.codec:
      return Opt.some(m)
  Opt.none(MuxerProvider)

proc mux(
    self: MuxedUpgrade, conn: Connection
): Future[Opt[Muxer]] {.
    async: (raises: [CancelledError, LPStreamError, MultiStreamError])
.} =
  ## mux connection
  trace "Muxing connection", conn
  if self.muxers.len == 0:
    warn "no muxers registered, skipping upgrade flow", conn
    return Opt.none(Muxer)

  var muxerName = ""
  var muxerProviderOpt = Opt.none(MuxerProvider)
  if conn.negotiatedMuxer.len > 0:
    muxerProviderOpt = self.getMuxerByCodec(conn.negotiatedMuxer)
    if muxerProviderOpt.isSome:
      muxerName = conn.negotiatedMuxer
      trace "Using preselected muxer from secure handshake",
        conn, muxerName
  if muxerName.len == 0:
    muxerName =
      case conn.dir
      of Direction.Out:
        await self.ms.select(conn, self.muxers.mapIt(it.codec))
      of Direction.In:
        await MultistreamSelect.handle(conn, self.muxers.mapIt(it.codec))
    muxerProviderOpt = self.getMuxerByCodec(muxerName)

  let muxerProvider = muxerProviderOpt.valueOr:
    debug "no muxer available, early exit", conn, muxerName
    return Opt.none(Muxer)

  trace "Found a muxer", conn, muxerName

  # create new muxer for connection
  let muxer = muxerProvider.newMuxer(conn)

  # install stream handler
  muxer.streamHandler = self.streamHandler
  muxer.handler = muxer.handle()
  Opt.some(muxer)

method upgrade*(
    self: MuxedUpgrade, conn: Connection, peerId: Opt[PeerId]
): Future[Muxer] {.async: (raises: [CancelledError, LPError]).} =
  trace "Upgrading connection", conn, direction = conn.dir

  let protectedConn =
    if self.protector.isNil:
      conn
    else:
      self.protector.protect(conn)

  if not self.bandwidthManager.isNil:
    protectedConn.setBandwidthManager(self.bandwidthManager)
  if not self.memoryManager.isNil:
    protectedConn.setMemoryManager(self.memoryManager)

  let sconn = await self.secure(protectedConn, peerId) # secure the connection
  if sconn == nil:
    raise (ref UpgradeFailedError)(msg: "unable to secure connection, stopping upgrade")

  if not self.bandwidthManager.isNil:
    sconn.setBandwidthManager(self.bandwidthManager)
  if not self.memoryManager.isNil:
    sconn.setMemoryManager(self.memoryManager)

  let muxer = (await self.mux(sconn)).valueOr:
    raise (ref UpgradeFailedError)(msg: "a muxer is required for outgoing connections")

  when defined(libp2p_agents_metrics):
    conn.shortAgent = muxer.connection.shortAgent

  if sconn.closed():
    await sconn.close()
    raise (ref UpgradeFailedError)(
      msg: "Connection closed or missing peer info, stopping upgrade"
    )

  trace "Upgraded connection", conn, sconn, direction = conn.dir
  muxer

proc new*(
    T: type MuxedUpgrade,
    muxers: seq[MuxerProvider],
    secureManagers: openArray[Secure] = [],
    ms: MultistreamSelect,
    protector: ConnectionProtector = nil,
): T =
  let upgrader = T(
    muxers: muxers, secureManagers: @secureManagers, ms: ms, protector: protector
  )

  upgrader.streamHandler = proc(conn: Connection) {.async: (raises: []).} =
    trace "Starting stream handler", conn
    try:
      await upgrader.ms.handle(conn) # handle incoming connection
    except CancelledError as exc:
      return
    finally:
      await conn.closeWithEOF()
    trace "Stream handler done", conn

  return upgrader
