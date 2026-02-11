# Nim-LibP2P
# Copyright (c)
# Licensed under either of
#  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
#  * MIT license ([LICENSE-MIT](LICENSE-MIT))
# at your option.

{.push raises: [].}

import std/[tables, sequtils, options, algorithm]
import chronos

import ../../peerid

type
  BitswapLedgerDirection* {.pure.} = enum
    bldSent,
    bldReceived

  BitswapLedgerStats* = object
    peers*: int
    bytesSent*: uint64
    bytesReceived*: uint64
    blocksSent*: uint64
    blocksReceived*: uint64
    haveSent*: uint64
    haveReceived*: uint64
    dontHaveSent*: uint64
    dontHaveReceived*: uint64
    wantsSent*: uint64
    wantsReceived*: uint64
    exchanges*: uint64
    maxDebtRatio*: float

  BitswapLedgerDelta* = object
    bytes*: uint64
    blocks*: uint64
    haveCount*: uint64
    dontHaveCount*: uint64

  BitswapPeerLedger* = object
    bytesSent*: uint64
    bytesReceived*: uint64
    blocksSent*: uint64
    blocksReceived*: uint64
    haveSent*: uint64
    haveReceived*: uint64
    dontHaveSent*: uint64
    dontHaveReceived*: uint64
    wantsSent*: uint64
    wantsReceived*: uint64
    exchanges*: uint64
    lastExchange*: Moment

  BitswapLedger* = ref object
    entries: Table[PeerId, BitswapPeerLedger]
    lock: AsyncLock

proc new*(_: type BitswapLedger): BitswapLedger =
  BitswapLedger(entries: initTable[PeerId, BitswapPeerLedger](), lock: newAsyncLock())

proc ensureEntry(
    ledger: BitswapLedger, peer: PeerId
): BitswapPeerLedger {.inline.} =
  ledger.entries.getOrDefault(peer, BitswapPeerLedger())

proc storeEntry(
    ledger: BitswapLedger, peer: PeerId, entry: BitswapPeerLedger
) {.inline.} =
  ledger.entries[peer] = entry

proc releaseSafe(lock: AsyncLock) =
  try:
    lock.release()
  except AsyncLockError:
    discard

proc recordWants*(
    ledger: BitswapLedger,
    peer: PeerId,
    count: Natural,
    direction: BitswapLedgerDirection,
) {.async: (raises: [CancelledError]).} =
  if ledger.isNil or peer.len == 0 or count == 0:
    return
  await ledger.lock.acquire()
  defer: releaseSafe(ledger.lock)
  var entry = ledger.ensureEntry(peer)
  case direction
  of BitswapLedgerDirection.bldSent:
    entry.wantsSent += uint64(count)
  of BitswapLedgerDirection.bldReceived:
    entry.wantsReceived += uint64(count)
  entry.lastExchange = Moment.now()
  ledger.storeEntry(peer, entry)

proc recordDelta*(
    ledger: BitswapLedger,
    peer: PeerId,
    delta: BitswapLedgerDelta,
    direction: BitswapLedgerDirection,
) {.async: (raises: [CancelledError]).} =
  if ledger.isNil or peer.len == 0:
    return
  await ledger.lock.acquire()
  defer: releaseSafe(ledger.lock)
  var entry = ledger.ensureEntry(peer)
  case direction
  of BitswapLedgerDirection.bldSent:
    entry.bytesSent += delta.bytes
    entry.blocksSent += delta.blocks
    entry.haveSent += delta.haveCount
    entry.dontHaveSent += delta.dontHaveCount
  of BitswapLedgerDirection.bldReceived:
    entry.bytesReceived += delta.bytes
    entry.blocksReceived += delta.blocks
    entry.haveReceived += delta.haveCount
    entry.dontHaveReceived += delta.dontHaveCount
  entry.exchanges += 1
  entry.lastExchange = Moment.now()
  ledger.storeEntry(peer, entry)

proc snapshot*(
    ledger: BitswapLedger, minExchanges: uint64 = 0
): Future[seq[(PeerId, BitswapPeerLedger)]] {.async: (raises: [CancelledError]).} =
  if ledger.isNil:
    return @[]
  await ledger.lock.acquire()
  defer: releaseSafe(ledger.lock)
  ledger.entries.pairs.toSeq.filterIt(it[1].exchanges >= minExchanges)

proc peerLedger*(
    ledger: BitswapLedger, peer: PeerId
): Future[Option[BitswapPeerLedger]] {.async: (raises: [CancelledError]).} =
  if ledger.isNil or peer.len == 0:
    return none(BitswapPeerLedger)
  await ledger.lock.acquire()
  defer: releaseSafe(ledger.lock)
  if ledger.entries.hasKey(peer):
    some(ledger.entries.getOrDefault(peer, BitswapPeerLedger()))
  else:
    none(BitswapPeerLedger)

proc debtRatio*(entry: BitswapPeerLedger): float =
  if entry.bytesSent == 0:
    return 0.0
  if entry.bytesReceived == 0:
    return entry.bytesSent.float
  entry.bytesSent.float / entry.bytesReceived.float

proc aggregate*(
    ledger: BitswapLedger, minExchanges: uint64 = 0
): Future[BitswapLedgerStats] {.async: (raises: [CancelledError]).} =
  var stats = BitswapLedgerStats()
  if ledger.isNil:
    return stats

  let snapshot = await ledger.snapshot(minExchanges)
  stats.peers = snapshot.len
  for (_, entry) in snapshot:
    stats.bytesSent += entry.bytesSent
    stats.bytesReceived += entry.bytesReceived
    stats.blocksSent += entry.blocksSent
    stats.blocksReceived += entry.blocksReceived
    stats.haveSent += entry.haveSent
    stats.haveReceived += entry.haveReceived
    stats.dontHaveSent += entry.dontHaveSent
    stats.dontHaveReceived += entry.dontHaveReceived
    stats.wantsSent += entry.wantsSent
    stats.wantsReceived += entry.wantsReceived
    stats.exchanges += entry.exchanges
    let ratio = entry.debtRatio()
    if ratio > stats.maxDebtRatio:
      stats.maxDebtRatio = ratio

  stats

proc topDebtors*(
    ledger: BitswapLedger, limit: Positive = 5, minBytesSent: uint64 = 0
): Future[seq[(PeerId, float, BitswapPeerLedger)]] {.async.} =
  if ledger.isNil:
    return @[]
  await ledger.lock.acquire()
  defer: releaseSafe(ledger.lock)
  var scored: seq[(PeerId, float, BitswapPeerLedger)] = @[]
  for peer, entry in ledger.entries.pairs:
    if entry.bytesSent < minBytesSent:
      continue
    let ratio = entry.debtRatio()
    scored.add((peer, ratio, entry))
  scored.sort(proc(a, b: (PeerId, float, BitswapPeerLedger)): int =
    cmp(b[1], a[1])
  )
  if scored.len > limit:
    scored = scored[0 ..< limit]
  scored

{.pop.}
