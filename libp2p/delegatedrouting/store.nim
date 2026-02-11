# Nim-LibP2P
# Copyright (c)
# Licensed under either of
#  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
#  * MIT license ([LICENSE-MIT](LICENSE-MIT))
# at your option.

{.push raises: [].}

import std/[tables, sequtils, options, json, base64, os]
from std/times import now, utc, toTime, toUnix
import chronos, chronicles
import pkg/results

import ../delegatedrouting
import ../multiaddress
import ../peerid

logScope:
  topics = "libp2p delegatedrouting store"

type
  DelegatedProviderEntry* = object
    record*: DelegatedProviderRecord
    expiresAt*: Moment

  DelegatedRecordEntry* = object
    record*: DelegatedRoutingRecord
    expiresAt*: Option[Moment]

  DelegatedRoutingPersistence* = ref object of RootObj

  DelegatedRoutingStore* = ref object
    lock: AsyncLock
    providerTtl*: Duration
    recordTtl*: Duration
    providers: Table[string, seq[DelegatedProviderEntry]]
    records: Table[string, seq[DelegatedRecordEntry]]
    persistence: DelegatedRoutingPersistence

  DelegatedProviderSnapshot = object
    key: string
    schema: string
    peerId: string
    addresses: seq[string]
    protocols: seq[string]
    remainingNs: int64

  DelegatedRecordSnapshot = object
    key: string         ## base64 of record key
    value: string       ## base64 of record value
    timeReceived: Option[string]
    remainingNs: Option[int64]

  DelegatedRoutingSnapshot = object
    providers: seq[DelegatedProviderSnapshot]
    records: seq[DelegatedRecordSnapshot]

  DelegatedRoutingFilePersistence* = ref object of DelegatedRoutingPersistence
    path: string

const
  DefaultProviderTtl* = 24.hours
  DefaultRecordTtl* = 30.minutes

proc pruneProviders(store: DelegatedRoutingStore, now: Moment) =
  var toDelete: seq[string] = @[]
  for key, entries in store.providers.pairs():
    var filtered: seq[DelegatedProviderEntry] = @[]
    for entry in entries:
      if entry.expiresAt > now:
        filtered.add(entry)
    if filtered.len == 0:
      toDelete.add(key)
    else:
      store.providers[key] = filtered
  for key in toDelete:
    store.providers.del(key)

proc pruneRecords(store: DelegatedRoutingStore, now: Moment) =
  var toDelete: seq[string] = @[]
  for key, entries in store.records.pairs():
    var filtered: seq[DelegatedRecordEntry] = @[]
    for entry in entries:
      if entry.expiresAt.isNone or entry.expiresAt.get() > now:
        filtered.add(entry)
    if filtered.len == 0:
      toDelete.add(key)
    else:
      store.records[key] = filtered
  for key in toDelete:
    store.records.del(key)

method load*(
    persistence: DelegatedRoutingPersistence
): Result[DelegatedRoutingSnapshot, string] {.base, gcsafe.} =
  err("delegated routing persistence load not implemented")

method save*(
    persistence: DelegatedRoutingPersistence,
    snapshot: DelegatedRoutingSnapshot
): Result[void, string] {.base, gcsafe.} =
  err("delegated routing persistence save not implemented")

proc normalizeKey(key: seq[byte]): string =
  result = newString(key.len)
  for i, b in key:
    result[i] = char(b)

proc bytesToString(data: seq[byte]): string =
  result = newString(data.len)
  for i, b in data:
    result[i] = char(b)

proc stringToBytes(data: string): seq[byte] =
  result = newSeq[byte](data.len)
  for i, c in data:
    result[i] = byte(c)

proc fieldOrNil(node: JsonNode, field: string): JsonNode =
  if node.kind != JObject:
    return nil
  for key, value in node.pairs():
    if key == field:
      return value
  nil

proc ensureParentDir(path: string) =
  let dir = splitFile(path).dir
  if dir.len > 0:
    try:
      createDir(dir)
    except CatchableError as exc:
      warn "failed creating persistence directory", dir, error = exc.msg

proc encodeBase64(data: seq[byte]): string =
  if data.len == 0:
    return ""
  base64.encode(bytesToString(data))

proc decodeBase64(data: string): Result[seq[byte], string] =
  try:
    let decoded = base64.decode(data)
    ok(stringToBytes(decoded))
  except ValueError as exc:
    err("base64 decode error: " & exc.msg)

proc snapshot(store: DelegatedRoutingStore, now: Moment): DelegatedRoutingSnapshot =
  var providers: seq[DelegatedProviderSnapshot] = @[]
  for key, entries in store.providers.pairs():
    for entry in entries:
      let remaining = entry.expiresAt - now
      if remaining.nanoseconds() <= 0:
        continue
      var addrStrs: seq[string] = @[]
      for addr in entry.record.addresses:
        addrStrs.add($addr)
      providers.add(
        DelegatedProviderSnapshot(
          key: key,
          schema: entry.record.schema,
          peerId: $entry.record.peerId,
          addresses: addrStrs,
          protocols: entry.record.protocols,
          remainingNs: remaining.nanoseconds(),
        )
      )

  var records: seq[DelegatedRecordSnapshot] = @[]
  for entries in store.records.values():
    for entry in entries:
      var remainingOpt: Option[int64] = none(int64)
      if entry.expiresAt.isSome:
        let remaining = entry.expiresAt.get() - now
        if remaining.nanoseconds() <= 0:
          continue
        remainingOpt = some(remaining.nanoseconds())
      records.add(
        DelegatedRecordSnapshot(
          key: encodeBase64(entry.record.key),
          value: encodeBase64(entry.record.value),
          timeReceived: entry.record.timeReceived,
          remainingNs: remainingOpt,
        )
      )

  DelegatedRoutingSnapshot(providers: providers, records: records)

proc applySnapshot(store: DelegatedRoutingStore, snapshot: DelegatedRoutingSnapshot) =
  var providerTable = initTable[string, seq[DelegatedProviderEntry]]()
  let now = Moment.now()

  for snap in snapshot.providers:
    if snap.remainingNs <= 0:
      continue

    var pid: PeerId
    if not pid.init(snap.peerId):
      warn "skipping persisted delegated provider with invalid peer id",
        peerId = snap.peerId
      continue

    var addrs: seq[MultiAddress] = @[]
    for addrStr in snap.addresses:
      let parsed = MultiAddress.init(addrStr)
      if parsed.isOk:
        addrs.add(parsed.get())
      else:
        warn "skipping persisted delegated provider address",
          address = addrStr, error = parsed.error()

    let expiry = now + chronos.nanoseconds(snap.remainingNs)
    let record = DelegatedProviderRecord(
      schema: snap.schema,
      peerId: pid,
      addresses: addrs,
      protocols: snap.protocols,
    )

    var entries = providerTable.getOrDefault(snap.key, @[])
    var replaced = false
    for entry in entries.mitems():
      if entry.record.schema == record.schema and entry.record.peerId == record.peerId:
        entry.record = record
        entry.expiresAt = expiry
        replaced = true
        break
    if not replaced:
      entries.add(DelegatedProviderEntry(record: record, expiresAt: expiry))
    providerTable[snap.key] = entries

  var recordTable = initTable[string, seq[DelegatedRecordEntry]]()
  for snap in snapshot.records:
    let keyRes = decodeBase64(snap.key)
    if keyRes.isErr:
      warn "skipping persisted delegated record with invalid key", error = keyRes.error
      continue
    let valueRes = decodeBase64(snap.value)
    if valueRes.isErr:
      warn "skipping persisted delegated record with invalid value", error = valueRes.error
      continue

    var expiresAt: Option[Moment] = none(Moment)
    if snap.remainingNs.isSome:
      let remaining = snap.remainingNs.get()
      if remaining <= 0:
        continue
      expiresAt = some(now + chronos.nanoseconds(remaining))

    let record = DelegatedRoutingRecord(
      key: keyRes.get(),
      value: valueRes.get(),
      timeReceived: snap.timeReceived,
    )

    let normalized = normalizeKey(record.key)
    var entries = recordTable.getOrDefault(normalized, @[])
    var replaced = false
    for entry in entries.mitems():
      if entry.record.value == record.value:
        entry.record.timeReceived = record.timeReceived
        entry.expiresAt = expiresAt
        replaced = true
        break
    if not replaced:
      entries.add(DelegatedRecordEntry(record: record, expiresAt: expiresAt))
    recordTable[normalized] = entries

  store.providers = providerTable
  store.records = recordTable

proc restoreFromPersistence(store: DelegatedRoutingStore) =
  if store.persistence.isNil:
    return
  let snapshotRes = store.persistence.load()
  if snapshotRes.isErr:
    warn "failed to load delegated routing persistence", error = snapshotRes.error
    return
  store.applySnapshot(snapshotRes.get())

proc persistLocked(store: DelegatedRoutingStore, now: Moment) =
  if store.persistence.isNil:
    return
  let snapshot = store.snapshot(now)
  let res = store.persistence.save(snapshot)
  if res.isErr:
    warn "failed to save delegated routing persistence", error = res.error

proc new*(
    _: type DelegatedRoutingStore,
    providerTtl: Duration = DefaultProviderTtl,
    recordTtl: Duration = DefaultRecordTtl,
    persistence: DelegatedRoutingPersistence = nil,
): DelegatedRoutingStore =
  result = DelegatedRoutingStore(
    lock: newAsyncLock(),
    providerTtl: providerTtl,
    recordTtl: recordTtl,
    providers: initTable[string, seq[DelegatedProviderEntry]](),
    records: initTable[string, seq[DelegatedRecordEntry]](),
    persistence: persistence,
  )
  result.restoreFromPersistence()

proc parseStringArray(node: JsonNode, field: string): seq[string] =
  let container = fieldOrNil(node, field)
  if container.isNil or container.kind != JArray:
    return @[]
  for item in container:
    if item.kind == JString:
      result.add(item.getStr())

method load*(
    persistence: DelegatedRoutingFilePersistence
): Result[DelegatedRoutingSnapshot, string] =
  if persistence.isNil or persistence.path.len == 0:
    return err("delegated routing persistence path is not configured")
  if not fileExists(persistence.path):
    return ok(DelegatedRoutingSnapshot(providers: @[], records: @[]))

  let data =
    try:
      readFile(persistence.path)
    except CatchableError as exc:
      return err("failed to read delegated routing persistence: " & exc.msg)

  if data.len == 0:
    return ok(DelegatedRoutingSnapshot(providers: @[], records: @[]))

  var root: JsonNode
  try:
    root = parseJson(data)
  except CatchableError as exc:
    return err("failed to parse delegated routing persistence: " & exc.msg)

  if root.kind != JObject:
    return err("delegated routing persistence must be a JSON object")

  var providerSnapshots: seq[DelegatedProviderSnapshot] = @[]
  let providersNode = fieldOrNil(root, "providers")
  if not providersNode.isNil and providersNode.kind == JArray:
    for providerNode in providersNode:
      if providerNode.kind != JObject:
        continue
      let keyNode = fieldOrNil(providerNode, "key")
      let schemaNode = fieldOrNil(providerNode, "schema")
      let peerNode = fieldOrNil(providerNode, "peerId")
      let remainingNode = fieldOrNil(providerNode, "remainingNs")
      if keyNode.isNil or schemaNode.isNil or peerNode.isNil or remainingNode.isNil:
        continue
      if keyNode.kind != JString or schemaNode.kind != JString or peerNode.kind != JString:
        continue
      if remainingNode.kind != JInt:
        continue
      let remaining = remainingNode.getInt().int64
      if remaining <= 0:
        continue
      providerSnapshots.add(
        DelegatedProviderSnapshot(
          key: keyNode.getStr(),
          schema: schemaNode.getStr(),
          peerId: peerNode.getStr(),
          addresses: parseStringArray(providerNode, "addresses"),
          protocols: parseStringArray(providerNode, "protocols"),
          remainingNs: remaining,
        )
      )

  var recordSnapshots: seq[DelegatedRecordSnapshot] = @[]
  let recordsNode = fieldOrNil(root, "records")
  if not recordsNode.isNil and recordsNode.kind == JArray:
    for recordNode in recordsNode:
      if recordNode.kind != JObject:
        continue
      let keyNode = fieldOrNil(recordNode, "key")
      let valueNode = fieldOrNil(recordNode, "value")
      if keyNode.isNil or valueNode.isNil:
        continue
      if keyNode.kind != JString or valueNode.kind != JString:
        continue
      var timeOpt: Option[string] = none(string)
      let timeNode = fieldOrNil(recordNode, "timeReceived")
      if not timeNode.isNil and timeNode.kind == JString:
        timeOpt = some(timeNode.getStr())
      var remainingOpt: Option[int64] = none(int64)
      let remainingNode = fieldOrNil(recordNode, "remainingNs")
      if not remainingNode.isNil and remainingNode.kind == JInt:
        let remaining = remainingNode.getInt().int64
        if remaining > 0:
          remainingOpt = some(remaining)
      recordSnapshots.add(
        DelegatedRecordSnapshot(
          key: keyNode.getStr(),
          value: valueNode.getStr(),
          timeReceived: timeOpt,
          remainingNs: remainingOpt,
        )
      )

  ok(DelegatedRoutingSnapshot(providers: providerSnapshots, records: recordSnapshots))

method save*(
    persistence: DelegatedRoutingFilePersistence,
    snapshot: DelegatedRoutingSnapshot
): Result[void, string] =
  if persistence.isNil or persistence.path.len == 0:
    return err("delegated routing persistence path is not configured")
  var tempPath = ""
  try:
    var root = newJObject()

    var providersNode = newJArray()
    for snap in snapshot.providers:
      var providerNode = newJObject()
      providerNode["key"] = %snap.key
      providerNode["schema"] = %snap.schema
      providerNode["peerId"] = %snap.peerId
      providerNode["remainingNs"] = %snap.remainingNs

      if snap.addresses.len > 0:
        var addrArray = newJArray()
        for addr in snap.addresses:
          addrArray.add(%addr)
        providerNode["addresses"] = addrArray

      if snap.protocols.len > 0:
        var protoArray = newJArray()
        for proto in snap.protocols:
          protoArray.add(%proto)
        providerNode["protocols"] = protoArray

      providersNode.add(providerNode)
    root["providers"] = providersNode

    var recordsNode = newJArray()
    for snap in snapshot.records:
      var recordNode = newJObject()
      recordNode["key"] = %snap.key
      recordNode["value"] = %snap.value
      if snap.timeReceived.isSome:
        recordNode["timeReceived"] = %snap.timeReceived.get()
      else:
        recordNode["timeReceived"] = newJNull()
      if snap.remainingNs.isSome:
        recordNode["remainingNs"] = %snap.remainingNs.get()
      else:
        recordNode["remainingNs"] = newJNull()
      recordsNode.add(recordNode)
    root["records"] = recordsNode

    let serialized = root.pretty()
    ensureParentDir(persistence.path)
    tempPath = persistence.path & ".tmp"
    writeFile(tempPath, serialized)
    moveFile(tempPath, persistence.path)
    ok()
  except CatchableError as exc:
    if tempPath.len > 0 and fileExists(tempPath):
      try:
        removeFile(tempPath)
      except CatchableError:
        discard
    err("failed to save delegated routing persistence: " & exc.msg)
  except Exception as exc:
    if tempPath.len > 0 and fileExists(tempPath):
      try:
        removeFile(tempPath)
      except CatchableError:
        discard
    err("failed to save delegated routing persistence: " & exc.msg)

proc new*(
    _: type DelegatedRoutingFilePersistence, path: string
): DelegatedRoutingFilePersistence {.public.} =
  DelegatedRoutingFilePersistence(path: path)

proc addProvider*(
    store: DelegatedRoutingStore,
    key: string,
    record: DelegatedProviderRecord,
    ttl: Duration = DefaultProviderTtl,
): Future[void] {.async: (raises: [CancelledError]).} =
  if store.isNil:
    return
  await store.lock.acquire()
  defer:
    try:
      store.lock.release()
    except AsyncLockError:
      discard

  let now = Moment.now()
  store.pruneProviders(now)

  let expiry =
    if ttl > 0.nanoseconds: now + ttl else: now + store.providerTtl

  var entries = store.providers.getOrDefault(key, @[])
  var replaced = false
  for entry in entries.mitems():
    if entry.record.schema == record.schema and entry.record.peerId == record.peerId:
      entry.record.addresses = record.addresses
      entry.record.protocols = record.protocols
      entry.expiresAt = expiry
      replaced = true
      break
  if not replaced:
    entries.add(DelegatedProviderEntry(record: record, expiresAt: expiry))
  store.providers[key] = entries
  store.persistLocked(now)

proc addProvider*(
    store: DelegatedRoutingStore,
    key: seq[byte],
    record: DelegatedProviderRecord,
    ttl: Duration = DefaultProviderTtl,
): Future[void] {.async: (raises: [CancelledError]).} =
  await store.addProvider(normalizeKey(key), record, ttl)

proc providerToJson(record: DelegatedProviderRecord): JsonNode =
  result = newJObject()
  result["Schema"] = %record.schema
  result["ID"] = %($record.peerId)
  if record.addresses.len > 0:
    var addrs = newJArray()
    for addr in record.addresses:
      addrs.add(%($addr))
    result["Addrs"] = addrs
  if record.protocols.len > 0:
    var protos = newJArray()
    for proto in record.protocols:
      protos.add(%proto)
    result["Protocols"] = protos

proc buildIpniAdvertisement(
    key: string, entry: DelegatedProviderEntry, now: Moment
): string =
  let computedTtl = (entry.expiresAt - now).nanoseconds()
  let ttlNs = if computedTtl > 0: computedTtl else: 0'i64
  let ttlSeconds = ttlNs div 1_000_000_000'i64
  let contextId = encodeBase64(stringToBytes(key))
  let providerNode = providerToJson(entry.record)
  var root = newJObject()
  root["ContextID"] = %contextId
  root["Provider"] = providerNode
  root["Metadata"] = %""
  root["TTL"] = %ttlSeconds
  root["TTLns"] = %ttlNs
  let updatedAtUnix = now().utc.toTime.toUnix.int64
  let expiresAtUnix = updatedAtUnix + ttlSeconds
  root["UpdatedAt"] = %updatedAtUnix
  root["ExpiresAt"] = %expiresAtUnix
  var entriesNode = newJArray()
  entriesNode.add(
    %*{
      "Key": encodeBase64(stringToBytes(key)),
      "Metadata": "",
      "ContextID": contextId,
    }
  )
  root["Entries"] = entriesNode
  $root

proc exportIpniAdvertisements*(
    store: DelegatedRoutingStore
): Future[seq[string]] {.async.} =
  if store.isNil:
    return @[]

  await store.lock.acquire()
  defer:
    try:
      store.lock.release()
    except AsyncLockError:
      discard

  let now = Moment.now()
  store.pruneProviders(now)
  var lines: seq[string] = @[]
  for key, entries in store.providers.pairs():
    for entry in entries:
      lines.add(buildIpniAdvertisement(key, entry, now))
  lines

proc getProviders*(
    store: DelegatedRoutingStore, key: string
): Future[seq[DelegatedProviderRecord]] {.
    async: (raises: [CancelledError])
.} =
  if store.isNil:
    return @[]
  await store.lock.acquire()
  defer:
    try:
      store.lock.release()
    except AsyncLockError:
      discard

  let now = Moment.now()
  store.pruneProviders(now)

  store.providers.getOrDefault(key, @[]).mapIt(it.record)

proc getProviders*(
    store: DelegatedRoutingStore, key: seq[byte]
): Future[seq[DelegatedProviderRecord]] {.
    async: (raises: [CancelledError])
.} =
  await store.getProviders(normalizeKey(key))

proc removeProvider*(
    store: DelegatedRoutingStore,
    key: string,
    peerId: PeerId = PeerId(),
    schema: string = ""
): Future[int] {.async: (raises: [CancelledError]).} =
  if store.isNil:
    return 0
  await store.lock.acquire()
  defer:
    try:
      store.lock.release()
    except AsyncLockError:
      discard

  let now = Moment.now()
  store.pruneProviders(now)

  var entries = store.providers.getOrDefault(key, @[])
  if entries.len == 0:
    return 0

  var filtered: seq[DelegatedProviderEntry] = @[]
  var removed = 0
  for entry in entries:
    let matchesPeer = peerId.len == 0 or entry.record.peerId == peerId
    let matchesSchema = schema.len == 0 or entry.record.schema == schema
    if matchesPeer and matchesSchema:
      inc removed
    else:
      filtered.add(entry)

  if removed == 0:
    return 0

  if filtered.len == 0:
    store.providers.del(key)
  else:
    store.providers[key] = filtered

  store.persistLocked(now)
  removed

proc removeProvider*(
    store: DelegatedRoutingStore,
    key: seq[byte],
    peerId: PeerId = PeerId(),
    schema: string = ""
): Future[int] {.async: (raises: [CancelledError]).} =
  await store.removeProvider(normalizeKey(key), peerId, schema)

proc clearProviders*(
    store: DelegatedRoutingStore, key: string
): Future[int] {.async: (raises: [CancelledError]).} =
  await store.removeProvider(key)

proc clearProviders*(
    store: DelegatedRoutingStore, key: seq[byte]
): Future[int] {.async: (raises: [CancelledError]).} =
  await store.removeProvider(key)

proc addRecord*(
    store: DelegatedRoutingStore,
    record: DelegatedRoutingRecord,
    ttl: Duration = DefaultRecordTtl,
): Future[void] {.async: (raises: [CancelledError]).} =
  if store.isNil:
    return
  await store.lock.acquire()
  defer:
    try:
      store.lock.release()
    except AsyncLockError:
      discard

  let now = Moment.now()
  store.pruneRecords(now)

  let key = normalizeKey(record.key)
  let expiry =
    if ttl > 0.nanoseconds: some(now + ttl) else: some(now + store.recordTtl)

  var entries = store.records.getOrDefault(key, @[])
  var replaced = false
  for entry in entries.mitems():
    if entry.record.value == record.value:
      entry.record.timeReceived = record.timeReceived
      entry.expiresAt = expiry
      replaced = true
      break
  if not replaced:
    entries.add(DelegatedRecordEntry(record: record, expiresAt: expiry))
  store.records[key] = entries
  store.persistLocked(now)

proc getRecords*(
    store: DelegatedRoutingStore, key: seq[byte]
): Future[seq[DelegatedRoutingRecord]] {.
    async: (raises: [CancelledError])
.} =
  if store.isNil:
    return @[]
  await store.lock.acquire()
  defer:
    try:
      store.lock.release()
    except AsyncLockError:
      discard

  let now = Moment.now()
  store.pruneRecords(now)

  let normalized = normalizeKey(key)
  store.records.getOrDefault(normalized, @[]).mapIt(it.record)

proc removeRecord*(
    store: DelegatedRoutingStore, key: seq[byte], value: seq[byte]
): Future[int] {.async: (raises: [CancelledError]).} =
  if store.isNil:
    return 0
  await store.lock.acquire()
  defer:
    try:
      store.lock.release()
    except AsyncLockError:
      discard

  let now = Moment.now()
  store.pruneRecords(now)

  let normalized = normalizeKey(key)
  var entries = store.records.getOrDefault(normalized, @[])
  if entries.len == 0:
    return 0

  var filtered: seq[DelegatedRecordEntry] = @[]
  var removed = 0
  for entry in entries:
    if entry.record.value == value:
      inc removed
    else:
      filtered.add(entry)

  if removed == 0:
    return 0

  if filtered.len == 0:
    store.records.del(normalized)
  else:
    store.records[normalized] = filtered

  store.persistLocked(now)
  removed

proc clearRecords*(
    store: DelegatedRoutingStore, key: seq[byte]
): Future[int] {.async: (raises: [CancelledError]).} =
  if store.isNil:
    return 0
  await store.lock.acquire()
  defer:
    try:
      store.lock.release()
    except AsyncLockError:
      discard

  let now = Moment.now()
  store.pruneRecords(now)

  let normalized = normalizeKey(key)
  let entries = store.records.getOrDefault(normalized, @[])
  if entries.len == 0:
    return 0

  store.records.del(normalized)
  store.persistLocked(now)
  entries.len

proc clearExpired*(store: DelegatedRoutingStore) {.async: (raises: [CancelledError]).} =
  if store.isNil:
    return
  await store.lock.acquire()
  defer:
    try:
      store.lock.release()
    except AsyncLockError:
      discard

  let now = Moment.now()
  store.pruneProviders(now)
  store.pruneRecords(now)
  store.persistLocked(now)

{.pop.}
