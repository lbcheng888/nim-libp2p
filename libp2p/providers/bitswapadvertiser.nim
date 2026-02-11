# Nim-LibP2P
# Licensed under either of
#  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
#  * MIT license ([LICENSE-MIT](LICENSE-MIT))
# at your option.

{.push raises: [].}

import std/[tables, options]
import chronos, chronicles
import pkg/results

import ../cid
import ../switch
import ../protocols/kademlia/kademlia
import ../protocols/bitswap/store
logScope:
  topics = "libp2p bitswap advertiser"

const
  DefaultBitswapProtocol = "/ipfs/bitswap/1.2.0"
  DefaultAdvertisedCacheSize = 2048
  DefaultDhtAdvertiseTtl = chronos.hours(24)
  DefaultDelegatedAdvertiseTtl = chronos.hours(24)

type
  AutoProviderStats* = object
    attempts*: uint64
    skipped*: uint64
    dhtSuccess*: uint64
    dhtFailure*: uint64
    delegatedSuccess*: uint64
    delegatedFailure*: uint64
    lastAttempt*: Option[Moment]
    lastSuccess*: Option[Moment]
    lastDhtSuccess*: Option[Moment]
    lastDelegatedSuccess*: Option[Moment]

  AutoProviderBlockStore* = ref object of BitswapBlockStore
    base: BitswapBlockStore
    sw: Switch
    kad: KadDHT
    delegatedSchema: string
    delegatedProtocols: seq[string]
    delegatedTtl: Duration
    dhtTtl: Duration
    minInterval: Duration
    maxTracked: int
    advertised: Table[string, Moment]
    lock: AsyncLock
    stats: AutoProviderStats

proc ensureBase(base: BitswapBlockStore) =
  if base.isNil():
    raise newException(Defect, "base bitswap block store must not be nil")

proc cloneBytes(data: seq[byte]): seq[byte] =
  if data.len == 0:
    return @[]
  result = newSeq[byte](data.len)
  if data.len > 0:
    copyMem(addr result[0], unsafeAddr data[0], data.len)

proc cidProviderKey(cid: Cid): Result[seq[byte], string] =
  let mhRes = cid.mhash()
  if mhRes.isErr:
    return err("failed to derive multihash from CID: " & $mhRes.error)
  let mh = mhRes.get()
  ok(mh.data.buffer.cloneBytes())

proc new*(
    _: type AutoProviderBlockStore,
    base: BitswapBlockStore,
    delegatedSchema: string = "bitswap",
    delegatedProtocols: seq[string] = @[DefaultBitswapProtocol],
    delegatedTtl: Duration = DefaultDelegatedAdvertiseTtl,
    dhtTtl: Duration = DefaultDhtAdvertiseTtl,
    minInterval: Duration = 5.minutes,
    maxTracked: int = DefaultAdvertisedCacheSize,
): AutoProviderBlockStore =
  ensureBase(base)
  AutoProviderBlockStore(
    base: base,
    delegatedSchema: delegatedSchema,
    delegatedProtocols: delegatedProtocols,
    delegatedTtl: delegatedTtl,
    dhtTtl: dhtTtl,
    minInterval: minInterval,
    maxTracked: maxTracked,
    advertised: initTable[string, Moment](),
    lock: newAsyncLock(),
    stats: AutoProviderStats(
      attempts: 0,
      skipped: 0,
      dhtSuccess: 0,
      dhtFailure: 0,
      delegatedSuccess: 0,
      delegatedFailure: 0,
      lastAttempt: none(Moment),
      lastSuccess: none(Moment),
      lastDhtSuccess: none(Moment),
      lastDelegatedSuccess: none(Moment),
    ),
  )

proc attachSwitch*(store: AutoProviderBlockStore, sw: Switch) =
  store.sw = sw

proc attachKad*(store: AutoProviderBlockStore, kad: KadDHT) =
  store.kad = kad

proc setMinAdvertiseInterval*(store: AutoProviderBlockStore, interval: Duration) =
  store.minInterval = interval

proc setDelegatedProtocols*(store: AutoProviderBlockStore, protocols: seq[string]) =
  store.delegatedProtocols = protocols

proc underlying*(store: AutoProviderBlockStore): BitswapBlockStore =
  store.base

proc shouldAdvertise(
    store: AutoProviderBlockStore, cid: Cid
): Future[bool] {.async: (raises: [CancelledError]).} =
  if store.sw.isNil and store.kad.isNil:
    return false

  let key = $cid
  let now = Moment.now()
  await store.lock.acquire()
  defer:
    try:
      store.lock.release()
    except AsyncLockError:
      discard

  if store.maxTracked > 0 and store.advertised.len >= store.maxTracked:
    store.advertised.clear()

  if store.minInterval > chronos.ZeroDuration:
    let previous = store.advertised.getOrDefault(key, Moment.low())
    if previous != Moment.low() and now - previous < store.minInterval:
      inc store.stats.skipped
      return false

  store.advertised[key] = now
  inc store.stats.attempts
  store.stats.lastAttempt = some(now)
  true

proc stats*(store: AutoProviderBlockStore): AutoProviderStats =
  store.stats

proc recordDhtResult(
    store: AutoProviderBlockStore, success: bool
) =
  if success:
    inc store.stats.dhtSuccess
  else:
    inc store.stats.dhtFailure

proc recordDelegatedResult(
    store: AutoProviderBlockStore, success: bool
) =
  if success:
    inc store.stats.delegatedSuccess
  else:
    inc store.stats.delegatedFailure

proc recordSuccessTimes(
    store: AutoProviderBlockStore, dhtSuccess, delegatedSuccess: bool
) =
  if not (dhtSuccess or delegatedSuccess):
    return
  let timestamp = Moment.now()
  store.stats.lastSuccess = some(timestamp)
  if dhtSuccess:
    store.stats.lastDhtSuccess = some(timestamp)
  if delegatedSuccess:
    store.stats.lastDelegatedSuccess = some(timestamp)

proc advertise(
    store: AutoProviderBlockStore, cid: Cid
) {.async: (raises: [CancelledError]).} =
  var dhtSuccess = false
  var delegatedSuccess = false

  if not store.kad.isNil:
    let keyRes = cidProviderKey(cid)
    if keyRes.isOk:
      let dhtKey = keyRes.get()
      let res = await store.kad.provide(dhtKey, store.dhtTtl)
      if res.isErr:
        store.recordDhtResult(false)
        trace "kad provider advertisement failed", cid = $cid, error = res.error
      else:
        dhtSuccess = true
        store.recordDhtResult(true)
    else:
      store.recordDhtResult(false)
      trace "failed to derive provider key from cid", cid = $cid, error = keyRes.error

  if not store.sw.isNil:
    try:
      await store.sw.publishDelegatedProvider(
        cid,
        schema = store.delegatedSchema,
        protocols = store.delegatedProtocols,
        ttl = store.delegatedTtl,
      )
      delegatedSuccess = true
      store.recordDelegatedResult(true)
    except CancelledError as exc:
      raise exc
    except CatchableError as exc:
      store.recordDelegatedResult(false)
      trace "delegated provider advertisement failed", cid = $cid, error = exc.msg

  store.recordSuccessTimes(dhtSuccess, delegatedSuccess)

method getBlock*(
    store: AutoProviderBlockStore, cid: Cid
): Future[Option[seq[byte]]] {.async: (raises: [CancelledError]).} =
  await store.base.getBlock(cid)

method putBlock*(
    store: AutoProviderBlockStore, cid: Cid, data: seq[byte]
): Future[void] {.async: (raises: [CancelledError]).} =
  await store.base.putBlock(cid, data)
  if not await store.shouldAdvertise(cid):
    return
  asyncSpawn (proc() {.async.} =
    try:
      await store.advertise(cid)
    except CancelledError as exc:
      raise exc
    except CatchableError as exc:
      trace "async advertisement failed", cid = $cid, error = exc.msg
  )()

method hasBlock*(
    store: AutoProviderBlockStore, cid: Cid
): Future[bool] {.async: (raises: [CancelledError]).} =
  await store.base.hasBlock(cid)

{.pop.}
