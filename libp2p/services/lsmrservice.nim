{.push raises: [].}

import std/[algorithm, base64, deques, json, options, sequtils, sets, strutils, tables]
from std/times import epochTime

import bearssl/rand
import chronos, chronicles
import results
import stew/byteutils

import ../lsmr
import ../multiaddress
import ../peerid
import ../peerinfo
import ../peerstore
import ../protocols/protocol
import ../stream/connection
import ../switch

logScope:
  topics = "libp2p lsmr"

type
  LsmrControlApplyKind = enum
    lcakSync
    lcakPublish

  LsmrControlApplyItem = object
    kind: LsmrControlApplyKind
    node: JsonNode

  LsmrValidationTrust* {.pure.} = enum
    rejected
    quarantine
    trusted

  LsmrControlSession = ref object
    peerId: PeerId
    conn: Connection
    syncPending: bool
    publishPending: bool
    publishAllKnown: bool
    publishPeers: Table[string, PeerId]
    runner: Future[void]
    lock: AsyncLock
    publishedCoordinateDigests: HashSet[string]
    publishedMigrationDigests: HashSet[string]
    publishedIsolationDigests: HashSet[string]
    publishedCoordinatePayloads: HashSet[string]
    publishedMigrationPayloads: HashSet[string]
    publishedIsolationPayloads: HashSet[string]

  LsmrPublishBatch = object
    request: JsonNode
    publishAllKnown: bool
    publishPeers: seq[PeerId]
    coordinateDigests: seq[string]
    migrationDigests: seq[string]
    isolationDigests: seq[string]
    coordinatePayloads: seq[string]
    migrationPayloads: seq[string]
    isolationPayloads: seq[string]

  LsmrValidationSnapshot* = object
    trust*: LsmrValidationTrust
    reason*: string
    recordSeen*: bool
    attestedPrefix*: LsmrPath
    confidence*: uint8
    witnessCount*: int
    expiresAtMs*: int64
    distance*: int

  LsmrService* = ref object of Service
    switch*: Switch
    config*: LsmrConfig
    rng: ref HmacDrbgContext
    running: bool
    runner: Future[void]
    refreshLock: AsyncLock
    refreshPending: bool
    refreshAllPublish: bool
    refreshPublishPeers: Table[string, PeerId]
    refreshRunner: Future[void]
    nearFieldRecords*: Table[PeerId, NearFieldHandshakeRecord]
    witnessProtocol: LPProtocol
    controlProtocol: LPProtocol
    witnessMounted: bool
    controlMounted: bool
    connectedHandler: ConnEventHandler
    witnessRequests*: int64
    witnessSuccess*: int64
    witnessQuorumSuccess*: int64
    witnessQuorumFailure*: int64
    validationTrusted*: int64
    validationQuarantine*: int64
    validationRejected*: int64
    staleRecords*: int64
    forgedRecords*: int64
    biasReorders*: int64
    controlSessions: Table[string, LsmrControlSession]
    connectFlights: Table[string, Future[bool]]
    dialLocks: Table[string, AsyncLock]
    topologyChangeSeq: uint64
    topologyChangedHook: proc() {.gcsafe, raises: [].}
    overlayPending: bool
    overlayRunner: Future[void]
    lastControlApplyElapsedMs: int64
    maxControlApplyElapsedMs: int64
    slowControlApplyCount: int64
    appliedCoordinatePayloads: HashSet[string]
    appliedMigrationPayloads: HashSet[string]
    appliedIsolationPayloads: HashSet[string]
    applyQueue: Deque[LsmrControlApplyItem]
    applyRunner: Future[void]
    revalidatePending: bool
    revalidateRunner: Future[void]

proc nowMillis(): int64 =
  int64(epochTime() * 1000)

proc controlApplyKindName(kind: LsmrControlApplyKind): string {.gcsafe, raises: [].} =
  case kind
  of lcakSync:
    "sync"
  of lcakPublish:
    "publish"

proc durationToMillis(value: Duration): int64 =
  value.nanoseconds div 1_000_000

proc bytesToString(data: openArray[byte]): string =
  string.fromBytes(@data)

proc stringToBytes(text: string): seq[byte] =
  text.toBytes()

proc pathToJson(path: LsmrPath): JsonNode =
  result = newJArray()
  for digit in path:
    result.add(%int(digit))

proc jsonToPath(node: JsonNode): LsmrPath =
  if node.isNil or node.kind != JArray:
    return @[]
  for item in node.items():
    if item.kind != JInt:
      return @[]
    let value = item.getInt()
    if value < 1 or value > 9:
      return @[]
    result.add(uint8(value))

proc buildPeerAddressEntry(peerId: PeerId, addrs: seq[MultiAddress]): JsonNode =
  let addrsNode = newJArray()
  for addr in addrs:
    addrsNode.add(%($addr))
  result = %*{
    "peerId": $peerId,
    "addrs": addrsNode,
  }

proc localCoordinateRecord*(
    switch: Switch
): Option[SignedLsmrCoordinateRecord] {.gcsafe.}
proc schedulePeerConnect(svc: LsmrService, peerId: PeerId) {.gcsafe, raises: [].}

proc applyPeerAddressEntries(svc: LsmrService, node: JsonNode) =
  if svc.isNil or svc.switch.isNil or svc.switch.peerStore.isNil or node.kind != JArray:
    return
  let selfPeerId =
    if svc.switch.peerInfo.isNil:
      PeerId()
    else:
      svc.switch.peerInfo.peerId
  let canConnect = localCoordinateRecord(svc.switch).isSome()
  for item in node.items():
    if item.kind != JObject or not item.hasKey("peerId") or not item.hasKey("addrs"):
      continue
    let peerIdNode = item.getOrDefault("peerId")
    let addrsNode = item.getOrDefault("addrs")
    if peerIdNode.kind != JString or addrsNode.kind != JArray:
      continue
    let peerId = PeerId.init(peerIdNode.getStr()).valueOr:
      continue
    var addrs: seq[MultiAddress] = @[]
    for raw in addrsNode.items():
      if raw.kind != JString:
        continue
      let parsed = MultiAddress.init(raw.getStr())
      if parsed.isOk():
        addrs.add(parsed.get())
    if addrs.len == 0:
      continue
    var merged = svc.switch.peerStore.getAddresses(peerId)
    var changed = false
    for addr in addrs:
      var found = false
      for existing in merged:
        if existing == addr:
          found = true
          break
      if not found:
        merged.add(addr)
        changed = true
    svc.switch.peerStore.setAddresses(peerId, merged)
    if changed and canConnect and peerId != selfPeerId:
      svc.schedulePeerConnect(peerId)

const
  LsmrControlApplyBatchSize = 1
  LsmrSlowApplyWarnMs = 750'i64

proc decodeSignedRecord(encoded: string): Option[SignedLsmrCoordinateRecord] {.gcsafe.}
proc decodeSignedMigration(encoded: string): Option[SignedLsmrMigrationRecord] {.gcsafe.}
proc decodeSignedIsolation(encoded: string): Option[SignedLsmrIsolationEvidence] {.gcsafe.}
proc sortedCoordinateRecords(
    records: seq[SignedLsmrCoordinateRecord]
): seq[SignedLsmrCoordinateRecord] {.gcsafe.}
proc installMigrationRecord*(
    switch: Switch, record: SignedLsmrMigrationRecord
): bool {.gcsafe.}
proc installIsolationEvidence*(
    switch: Switch, evidence: SignedLsmrIsolationEvidence
): bool {.gcsafe.}
proc promoteCoordinateClosureAsync(
    svc: LsmrService,
    peerIds: seq[PeerId],
): Future[tuple[
    changes: int, changedPeerIds: seq[PeerId], yields: int, maxSliceMs: int64
  ]] {.
    async: (raises: [CancelledError])
.}
proc storeCoordinateRecord(
    switch: Switch,
    config: LsmrConfig,
    record: SignedLsmrCoordinateRecord,
    promoteNow = true,
) {.gcsafe.}
proc scheduleControlApply(
    svc: LsmrService, kind: LsmrControlApplyKind, node: JsonNode
) {.gcsafe, raises: [].}
proc promoteValidatedCoordinateRecord*(
    switch: Switch, config: LsmrConfig, peerId: PeerId
): bool {.gcsafe.}

template controlApplyCheckpoint(processed, yields, maxSliceMs, sliceStartedAt: untyped) =
  inc processed
  if processed mod LsmrControlApplyBatchSize == 0:
    let sliceElapsed = nowMillis() - sliceStartedAt
    if sliceElapsed > maxSliceMs:
      maxSliceMs = sliceElapsed
    inc yields
    await sleepAsync(0.seconds)
    sliceStartedAt = nowMillis()

proc noteControlApplyElapsed(
    svc: LsmrService,
    phase: string,
    appliedCoords, appliedMigrations, appliedIsolations, yieldPoints: int,
    maxSliceMs: int64,
    startedAt: int64,
) {.gcsafe, raises: [].} =
  if svc.isNil or svc.switch.isNil or svc.switch.peerInfo.isNil:
    return
  let totalElapsedMs = nowMillis() - startedAt
  svc.lastControlApplyElapsedMs = maxSliceMs
  if maxSliceMs > svc.maxControlApplyElapsedMs:
    svc.maxControlApplyElapsedMs = maxSliceMs
  when defined(lsmr_diag) or defined(fabric_submit_diag):
    echo "lsmr ", phase, " done self=", $svc.switch.peerInfo.peerId,
      " coords=", appliedCoords,
      " migrations=", appliedMigrations,
      " isolations=", appliedIsolations,
      " active=", svc.switch.peerStore[ActiveLsmrBook].book.len,
      " yields=", yieldPoints,
      " maxSliceMs=", maxSliceMs,
      " totalMs=", totalElapsedMs
  if maxSliceMs >= LsmrSlowApplyWarnMs:
    inc svc.slowControlApplyCount
    echo "lsmr slow-apply self=", $svc.switch.peerInfo.peerId,
      " phase=", phase,
      " coords=", appliedCoords,
      " migrations=", appliedMigrations,
      " isolations=", appliedIsolations,
      " yields=", yieldPoints,
      " maxSliceMs=", maxSliceMs,
      " totalMs=", totalElapsedMs

proc decodeCoordinatePublishRecordNodes(request: JsonNode): seq[JsonNode] =
  let coordinateRecordsNode =
    if request.kind == JObject:
      request.getOrDefault("coordinateRecords")
    else:
      newJNull()
  if coordinateRecordsNode.kind == JArray:
    for item in coordinateRecordsNode.items():
      if item.kind == JString:
        result.add(item)
  else:
    let coordinateRecordNode =
      if request.kind == JObject:
        request.getOrDefault("coordinateRecord")
      else:
        newJNull()
    if coordinateRecordNode.kind == JString:
      result.add(coordinateRecordNode)

proc collectCoordinateRecordsAsync(
    svc: LsmrService,
    items: seq[JsonNode]
): Future[tuple[records: seq[SignedLsmrCoordinateRecord], yields: int, maxSliceMs: int64]] {.
    async: (raises: [CancelledError])
.} =
  var processed = 0
  var sliceStartedAt = nowMillis()
  var seen = initHashSet[string]()
  for item in items:
    let encoded = item.getStr()
    if encoded.len == 0 or encoded in seen or
        (not svc.isNil and encoded in svc.appliedCoordinatePayloads):
      controlApplyCheckpoint(processed, result.yields, result.maxSliceMs, sliceStartedAt)
      continue
    seen.incl(encoded)
    decodeSignedRecord(encoded).withValue(record):
      result.records.add(record)
    controlApplyCheckpoint(processed, result.yields, result.maxSliceMs, sliceStartedAt)
  result.records = result.records.sortedCoordinateRecords()
  let tailElapsed = nowMillis() - sliceStartedAt
  if tailElapsed > result.maxSliceMs:
    result.maxSliceMs = tailElapsed

proc applyMigrationRecordsAsync(
    svc: LsmrService, node: JsonNode
): Future[tuple[applied: int, yields: int, maxSliceMs: int64]] {.async: (raises: [CancelledError]).} =
  if svc.isNil or svc.switch.isNil or node.kind != JArray:
    return
  var processed = 0
  var sliceStartedAt = nowMillis()
  var seen = initHashSet[string]()
  for item in node.items():
    if item.kind == JString:
      let encoded = item.getStr()
      if encoded.len == 0 or encoded in seen or encoded in svc.appliedMigrationPayloads:
        controlApplyCheckpoint(processed, result.yields, result.maxSliceMs, sliceStartedAt)
        continue
      seen.incl(encoded)
      decodeSignedMigration(encoded).withValue(record):
        inc result.applied
        discard installMigrationRecord(svc.switch, record)
        svc.appliedMigrationPayloads.incl(encoded)
      controlApplyCheckpoint(processed, result.yields, result.maxSliceMs, sliceStartedAt)
  let tailElapsed = nowMillis() - sliceStartedAt
  if tailElapsed > result.maxSliceMs:
    result.maxSliceMs = tailElapsed

proc applyIsolationEvidenceAsync(
    svc: LsmrService, node: JsonNode
): Future[tuple[applied: int, yields: int, maxSliceMs: int64]] {.async: (raises: [CancelledError]).} =
  if svc.isNil or svc.switch.isNil or node.kind != JArray:
    return
  var processed = 0
  var sliceStartedAt = nowMillis()
  var seen = initHashSet[string]()
  for item in node.items():
    if item.kind == JString:
      let encoded = item.getStr()
      if encoded.len == 0 or encoded in seen or encoded in svc.appliedIsolationPayloads:
        controlApplyCheckpoint(processed, result.yields, result.maxSliceMs, sliceStartedAt)
        continue
      seen.incl(encoded)
      decodeSignedIsolation(encoded).withValue(evidence):
        inc result.applied
        discard installIsolationEvidence(svc.switch, evidence)
        svc.appliedIsolationPayloads.incl(encoded)
      controlApplyCheckpoint(processed, result.yields, result.maxSliceMs, sliceStartedAt)
  let tailElapsed = nowMillis() - sliceStartedAt
  if tailElapsed > result.maxSliceMs:
    result.maxSliceMs = tailElapsed

proc encodeSignedRecord(record: SignedLsmrCoordinateRecord): string =
  let encoded = record.encode().valueOr:
    return ""
  base64.encode(bytesToString(encoded))

proc encodeSignedMigration(record: SignedLsmrMigrationRecord): string =
  let encoded = record.encode().valueOr:
    return ""
  base64.encode(bytesToString(encoded))

proc encodeSignedIsolation(evidence: SignedLsmrIsolationEvidence): string =
  let encoded = evidence.encode().valueOr:
    return ""
  base64.encode(bytesToString(encoded))

proc decodeSignedRecord(encoded: string): Option[SignedLsmrCoordinateRecord] {.gcsafe.} =
  if encoded.len == 0:
    return none(SignedLsmrCoordinateRecord)
  try:
    let record = SignedLsmrCoordinateRecord.decode(base64.decode(encoded).toBytes()).valueOr:
      return none(SignedLsmrCoordinateRecord)
    some(record)
  except CatchableError:
    none(SignedLsmrCoordinateRecord)

proc decodeSignedMigration(encoded: string): Option[SignedLsmrMigrationRecord] {.gcsafe.} =
  if encoded.len == 0:
    return none(SignedLsmrMigrationRecord)
  try:
    let record = SignedLsmrMigrationRecord.decode(base64.decode(encoded).toBytes()).valueOr:
      return none(SignedLsmrMigrationRecord)
    some(record)
  except CatchableError:
    none(SignedLsmrMigrationRecord)

proc decodeSignedIsolation(encoded: string): Option[SignedLsmrIsolationEvidence] {.gcsafe.} =
  if encoded.len == 0:
    return none(SignedLsmrIsolationEvidence)
  try:
    let evidence = SignedLsmrIsolationEvidence.decode(base64.decode(encoded).toBytes()).valueOr:
      return none(SignedLsmrIsolationEvidence)
    some(evidence)
  except CatchableError:
    none(SignedLsmrIsolationEvidence)

proc findEffectiveAnchor(
    svc: LsmrService, peerId: PeerId
): Option[LsmrAnchor] {.gcsafe.}
proc hasDialableRoute(svc: LsmrService, peerId: PeerId): bool {.gcsafe.}
proc connectedPeerIds(svc: LsmrService): seq[PeerId] {.gcsafe.}
proc knownSyncPeers(svc: LsmrService): seq[PeerId] {.gcsafe.}
proc knowsCoordinatePeer(svc: LsmrService, peerId: PeerId): bool {.gcsafe.}
proc queueControlSync(svc: LsmrService, peerId: PeerId) {.gcsafe.}
proc queueControlPublish(svc: LsmrService, peerId: PeerId) {.gcsafe.}
proc queueControlPublishDelta(
    svc: LsmrService, peerId: PeerId, subjectPeerIds: openArray[PeerId]
) {.gcsafe.}
proc queueCoordinateDelta(
    svc: LsmrService, subjectPeerIds: openArray[PeerId]
) {.gcsafe, raises: [].}
proc queueTopologyDelta(
    svc: LsmrService, publishAllKnown = false
) {.gcsafe, raises: [].}
proc queueTopologyConvergence(
    svc: LsmrService, publishAllKnown = false
) {.gcsafe, raises: [].}
proc scheduleOverlayMaintenance(svc: LsmrService) {.gcsafe, raises: [].}
proc scheduleCoordinateRevalidation(svc: LsmrService) {.gcsafe, raises: [].}
proc scheduleImmediateRefresh(
    svc: LsmrService, publishPeer: Option[PeerId] = none(PeerId)
) {.gcsafe, raises: [].}
proc refreshCoordinateRecord*(
    svc: LsmrService
): Future[bool] {.async: (raises: [CancelledError]).}

proc buildCoordinateSyncResponse(svc: LsmrService): JsonNode {.gcsafe.} =
  let coordinateRecords = newJArray()
  let migrationRecords = newJArray()
  let isolationEvidence = newJArray()
  let peerAddresses = newJArray()
  result = %*{
    "ok": true,
    "coordinateRecords": coordinateRecords,
    "migrationRecords": migrationRecords,
    "isolationEvidence": isolationEvidence,
    "peerAddresses": peerAddresses,
  }
  if svc.isNil or svc.switch.isNil or svc.switch.peerStore.isNil:
    return
  var seenAddrPeers = initHashSet[string]()
  proc addPeerAddrs(peerId: PeerId) =
    if peerId.data.len == 0:
      return
    let key = $peerId
    if key in seenAddrPeers:
      return
    let addrs =
      if peerId == svc.switch.peerInfo.peerId and svc.switch.peerInfo.addrs.len > 0:
        svc.switch.peerInfo.addrs
      else:
        svc.switch.peerStore.getAddresses(peerId)
    if addrs.len == 0:
      return
    seenAddrPeers.incl(key)
    peerAddresses.add(buildPeerAddressEntry(peerId, addrs))
  addPeerAddrs(svc.switch.peerInfo.peerId)
  for _, records in svc.switch.peerStore[LsmrChainBook].book.pairs:
    for record in records.sortedCoordinateRecords():
      let encoded = encodeSignedRecord(record)
      if encoded.len > 0:
        coordinateRecords.add(%encoded)
      addPeerAddrs(record.data.peerId)
  if coordinateRecords.len == 0:
    for _, record in svc.switch.peerStore[ActiveLsmrBook].book.pairs:
      let encoded = encodeSignedRecord(record)
      if encoded.len > 0:
        coordinateRecords.add(%encoded)
      addPeerAddrs(record.data.peerId)
  for _, records in svc.switch.peerStore[LsmrMigrationBook].book.pairs:
    for record in records:
      let encoded = encodeSignedMigration(record)
      if encoded.len > 0:
        migrationRecords.add(%encoded)
  for _, evidenceSet in svc.switch.peerStore[LsmrIsolationBook].book.pairs:
    for evidence in evidenceSet:
      let encoded = encodeSignedIsolation(evidence)
      if encoded.len > 0:
        isolationEvidence.add(%encoded)

proc appendPeerAddress(
    svc: LsmrService,
    peerId: PeerId,
    peerAddresses: JsonNode,
    seenAddrPeers: var HashSet[string],
) {.gcsafe.} =
  if svc.isNil or svc.switch.isNil or svc.switch.peerInfo.isNil or peerId.data.len == 0:
    return
  let key = $peerId
  if key in seenAddrPeers:
    return
  let addrs =
    if peerId == svc.switch.peerInfo.peerId and svc.switch.peerInfo.addrs.len > 0:
      svc.switch.peerInfo.addrs
    else:
      svc.switch.peerStore.getAddresses(peerId)
  if addrs.len == 0:
    return
  seenAddrPeers.incl(key)
  peerAddresses.add(buildPeerAddressEntry(peerId, addrs))

proc fillCoordinatePublishState(
    svc: LsmrService,
    coordinateRecords, migrationRecords, isolationEvidence, peerAddresses: JsonNode,
) {.gcsafe.} =
  let snapshot = svc.buildCoordinateSyncResponse()
  if snapshot.kind != JObject:
    return
  let sourceCoords = snapshot.getOrDefault("coordinateRecords")
  if sourceCoords.kind == JArray:
    for item in sourceCoords.items():
      coordinateRecords.add(item)
  let sourceMigrations = snapshot.getOrDefault("migrationRecords")
  if sourceMigrations.kind == JArray:
    for item in sourceMigrations.items():
      migrationRecords.add(item)
  let sourceIsolations = snapshot.getOrDefault("isolationEvidence")
  if sourceIsolations.kind == JArray:
    for item in sourceIsolations.items():
      isolationEvidence.add(item)
  let sourcePeerAddresses = snapshot.getOrDefault("peerAddresses")
  if sourcePeerAddresses.kind == JArray:
    for item in sourcePeerAddresses.items():
      peerAddresses.add(item)

proc applyCoordinateRecords(
    svc: LsmrService, node: JsonNode
): Future[void] {.async: (raises: [CancelledError]).} =
  if svc.isNil or svc.switch.isNil:
    return
  let startedAt = nowMillis()
  var appliedCoords = 0
  var appliedMigrations = 0
  var appliedIsolations = 0
  var yieldPoints = 0
  var maxSliceMs = 0'i64
  var touchedPeerIds: seq[PeerId] = @[]
  var touchedPeerKeys = initHashSet[string]()
  proc noteTouched(peerId: PeerId) =
    let key = $peerId
    if peerId.data.len == 0 or key in touchedPeerKeys:
      return
    touchedPeerKeys.incl(key)
    touchedPeerIds.add(peerId)
  let peerAddressesNode =
    if node.kind == JObject:
      node.getOrDefault("peerAddresses")
    else:
      newJNull()
  svc.applyPeerAddressEntries(peerAddressesNode)
  let coordinateRecordsNode =
    if node.kind == JObject:
      node.getOrDefault("coordinateRecords")
    else:
      newJNull()
  if coordinateRecordsNode.kind == JArray:
    let decoded = await collectCoordinateRecordsAsync(svc, coordinateRecordsNode.elems)
    yieldPoints += decoded.yields
    if decoded.maxSliceMs > maxSliceMs:
      maxSliceMs = decoded.maxSliceMs
    var processed = 0
    var sliceStartedAt = nowMillis()
    for record in decoded.records:
      inc appliedCoords
      when defined(lsmr_diag):
        echo "lsmr apply coord self=", $svc.switch.peerInfo.peerId,
          " peer=", $record.data.peerId,
          " prefix=", record.data.certifiedPrefix(),
          " epoch=", record.data.epochId
      noteTouched(record.data.peerId)
      storeCoordinateRecord(svc.switch, svc.config, record, promoteNow = false)
      svc.appliedCoordinatePayloads.incl(encodeSignedRecord(record))
      controlApplyCheckpoint(processed, yieldPoints, maxSliceMs, sliceStartedAt)
    let tailElapsed = nowMillis() - sliceStartedAt
    if tailElapsed > maxSliceMs:
      maxSliceMs = tailElapsed
  let migrationRecordsNode =
    if node.kind == JObject:
      node.getOrDefault("migrationRecords")
    else:
      newJNull()
  let migrationResult = await applyMigrationRecordsAsync(svc, migrationRecordsNode)
  appliedMigrations += migrationResult.applied
  yieldPoints += migrationResult.yields
  if migrationResult.maxSliceMs > maxSliceMs:
    maxSliceMs = migrationResult.maxSliceMs
  let isolationEvidenceNode =
    if node.kind == JObject:
      node.getOrDefault("isolationEvidence")
    else:
      newJNull()
  let isolationResult = await applyIsolationEvidenceAsync(svc, isolationEvidenceNode)
  appliedIsolations += isolationResult.applied
  yieldPoints += isolationResult.yields
  if isolationResult.maxSliceMs > maxSliceMs:
    maxSliceMs = isolationResult.maxSliceMs
  if touchedPeerIds.len > 0:
    let promoteResult = await promoteCoordinateClosureAsync(svc, touchedPeerIds)
    yieldPoints += promoteResult.yields
    if promoteResult.maxSliceMs > maxSliceMs:
      maxSliceMs = promoteResult.maxSliceMs
    if promoteResult.changedPeerIds.len > 0:
      svc.queueCoordinateDelta(promoteResult.changedPeerIds)
  svc.noteControlApplyElapsed(
    "apply",
    appliedCoords,
    appliedMigrations,
    appliedIsolations,
    yieldPoints,
    maxSliceMs,
    startedAt,
  )

proc coordinateDigestKey(record: SignedLsmrCoordinateRecord): string {.gcsafe.} =
  if record.data.recordDigest.len > 0:
    record.data.recordDigest
  else:
    encodeSignedRecord(record)

proc migrationDigestKey(record: SignedLsmrMigrationRecord): string {.gcsafe.} =
  if record.data.migrationDigest.len > 0:
    record.data.migrationDigest
  else:
    encodeSignedMigration(record)

proc isolationDigestKey(evidence: SignedLsmrIsolationEvidence): string {.gcsafe.} =
  if evidence.data.evidenceDigest.len > 0:
    evidence.data.evidenceDigest
  else:
    encodeSignedIsolation(evidence)

proc buildCoordinatePublishBatch(
    svc: LsmrService, session: LsmrControlSession
): LsmrPublishBatch {.gcsafe.} =
  let coordinateRecords = newJArray()
  let migrationRecords = newJArray()
  let isolationEvidence = newJArray()
  let peerAddresses = newJArray()
  var publishPrefixes: seq[string] = @[]
  var seenAddrPeers = initHashSet[string]()
  var coordinateDigests: seq[string] = @[]
  var migrationDigests: seq[string] = @[]
  var isolationDigests: seq[string] = @[]
  var coordinatePayloads: seq[string] = @[]
  var migrationPayloads: seq[string] = @[]
  var isolationPayloads: seq[string] = @[]
  var request = %*{
    "op": "coordinate_publish",
    "coordinateRecords": coordinateRecords,
    "migrationRecords": migrationRecords,
    "isolationEvidence": isolationEvidence,
    "peerAddresses": peerAddresses,
  }

  proc addCoordinateRecord(record: SignedLsmrCoordinateRecord) =
    let encoded = encodeSignedRecord(record)
    if encoded.len == 0:
      return
    let digest = coordinateDigestKey(record)
    if (not session.isNil and
        (encoded in session.publishedCoordinatePayloads or
         (digest.len > 0 and digest in session.publishedCoordinateDigests))):
      return
    coordinateRecords.add(%encoded)
    if digest.len > 0:
      coordinateDigests.add(digest)
    coordinatePayloads.add(encoded)
    publishPrefixes.add($record.data.certifiedPrefix())
    svc.appendPeerAddress(record.data.peerId, peerAddresses, seenAddrPeers)

  proc addMigrationRecord(record: SignedLsmrMigrationRecord) =
    let encoded = encodeSignedMigration(record)
    if encoded.len == 0:
      return
    let digest = migrationDigestKey(record)
    if (not session.isNil and
        (encoded in session.publishedMigrationPayloads or
         (digest.len > 0 and digest in session.publishedMigrationDigests))):
      return
    migrationRecords.add(%encoded)
    if digest.len > 0:
      migrationDigests.add(digest)
    migrationPayloads.add(encoded)

  proc addIsolationRecord(evidence: SignedLsmrIsolationEvidence) =
    let encoded = encodeSignedIsolation(evidence)
    if encoded.len == 0:
      return
    let digest = isolationDigestKey(evidence)
    if (not session.isNil and
        (encoded in session.publishedIsolationPayloads or
         (digest.len > 0 and digest in session.publishedIsolationDigests))):
      return
    isolationEvidence.add(%encoded)
    if digest.len > 0:
      isolationDigests.add(digest)
    isolationPayloads.add(encoded)

  if svc.isNil or svc.switch.isNil or svc.switch.peerStore.isNil:
    return LsmrPublishBatch(request: newJNull())

  var publishAllKnown = false
  var publishPeers: seq[PeerId] = @[]
  if not session.isNil:
    publishAllKnown = session.publishAllKnown
    session.publishAllKnown = false
    for _, peerId in session.publishPeers.pairs:
      publishPeers.add(peerId)
    session.publishPeers.clear()
  publishPeers.sort(proc(a, b: PeerId): int = cmp($a, $b))

  proc addSubjectPeerState(peerId: PeerId) =
    if peerId.data.len == 0:
      return
    var chain = svc.switch.peerStore[LsmrChainBook][peerId].sortedCoordinateRecords()
    if chain.len > 0:
      for record in chain:
        addCoordinateRecord(record)
    elif svc.switch.peerStore[ActiveLsmrBook].contains(peerId):
      addCoordinateRecord(svc.switch.peerStore[ActiveLsmrBook][peerId])
    elif svc.switch.peerStore[LsmrBook].contains(peerId):
      addCoordinateRecord(svc.switch.peerStore[LsmrBook][peerId])
    for record in svc.switch.peerStore[LsmrMigrationBook][peerId]:
      addMigrationRecord(record)
    for evidence in svc.switch.peerStore[LsmrIsolationBook][peerId]:
      addIsolationRecord(evidence)
    svc.appendPeerAddress(peerId, peerAddresses, seenAddrPeers)

  if publishAllKnown:
    for _, records in svc.switch.peerStore[LsmrChainBook].book.pairs:
      for record in records.sortedCoordinateRecords():
        addCoordinateRecord(record)
    if coordinateRecords.len == 0:
      for _, record in svc.switch.peerStore[ActiveLsmrBook].book.pairs:
        addCoordinateRecord(record)
    for _, records in svc.switch.peerStore[LsmrMigrationBook].book.pairs:
      for record in records:
        addMigrationRecord(record)
    for _, evidenceSet in svc.switch.peerStore[LsmrIsolationBook].book.pairs:
      for evidence in evidenceSet:
        addIsolationRecord(evidence)
  else:
    for peerId in publishPeers:
      addSubjectPeerState(peerId)
  if coordinateRecords.len == 0 and migrationRecords.len == 0 and isolationEvidence.len == 0:
    request = newJNull()
  when defined(lsmr_diag):
    let selfPeerId =
      if svc.isNil or svc.switch.isNil or svc.switch.peerInfo.isNil:
        PeerId()
      else:
        svc.switch.peerInfo.peerId
    let selfChainLen =
      if svc.isNil or svc.switch.isNil or svc.switch.peerStore.isNil or
          selfPeerId.data.len == 0:
        0
      else:
        svc.switch.peerStore[LsmrChainBook][selfPeerId].len
    echo "lsmr publish-build self=", $svc.switch.peerInfo.peerId,
      " coords=", coordinateRecords.len,
      " prefixes=", publishPrefixes.join(","),
      " selfChainLen=", selfChainLen
  result = LsmrPublishBatch(
    request: request,
    publishAllKnown: publishAllKnown,
    publishPeers: publishPeers,
    coordinateDigests: coordinateDigests,
    migrationDigests: migrationDigests,
    isolationDigests: isolationDigests,
    coordinatePayloads: coordinatePayloads,
    migrationPayloads: migrationPayloads,
    isolationPayloads: isolationPayloads,
  )

proc notePublishedBatch(session: LsmrControlSession, batch: LsmrPublishBatch) {.gcsafe.} =
  if session.isNil:
    return
  for digest in batch.coordinateDigests:
    session.publishedCoordinateDigests.incl(digest)
  for digest in batch.migrationDigests:
    session.publishedMigrationDigests.incl(digest)
  for digest in batch.isolationDigests:
    session.publishedIsolationDigests.incl(digest)
  for payload in batch.coordinatePayloads:
    session.publishedCoordinatePayloads.incl(payload)
  for payload in batch.migrationPayloads:
    session.publishedMigrationPayloads.incl(payload)
  for payload in batch.isolationPayloads:
    session.publishedIsolationPayloads.incl(payload)

proc restoreQueuedPublishState(session: LsmrControlSession, batch: LsmrPublishBatch) {.gcsafe.} =
  if session.isNil:
    return
  if batch.publishAllKnown:
    session.publishAllKnown = true
  else:
    for peerId in batch.publishPeers:
      if peerId.data.len == 0:
        continue
      session.publishPeers[$peerId] = peerId

proc applyCoordinatePublishRequest(
    svc: LsmrService, request: JsonNode
): Future[void] {.async: (raises: [CancelledError]).} =
  if svc.isNil or svc.switch.isNil:
    return
  let startedAt = nowMillis()
  var yieldPoints = 0
  var maxSliceMs = 0'i64
  var touchedPeerIds: seq[PeerId] = @[]
  var touchedPeerKeys = initHashSet[string]()
  proc noteTouched(peerId: PeerId) =
    let key = $peerId
    if peerId.data.len == 0 or key in touchedPeerKeys:
      return
    touchedPeerKeys.incl(key)
    touchedPeerIds.add(peerId)
  let peerAddressesNode =
    if request.kind == JObject:
      request.getOrDefault("peerAddresses")
    else:
      newJNull()
  svc.applyPeerAddressEntries(peerAddressesNode)
  let decoded = await collectCoordinateRecordsAsync(svc, decodeCoordinatePublishRecordNodes(request))
  yieldPoints += decoded.yields
  if decoded.maxSliceMs > maxSliceMs:
    maxSliceMs = decoded.maxSliceMs
  var processed = 0
  var sliceStartedAt = nowMillis()
  for record in decoded.records:
    when defined(lsmr_diag):
      echo "lsmr publish-apply coord self=", $svc.switch.peerInfo.peerId,
        " peer=", $record.data.peerId,
        " prefix=", record.data.certifiedPrefix(),
        " epoch=", record.data.epochId
    noteTouched(record.data.peerId)
    storeCoordinateRecord(svc.switch, svc.config, record, promoteNow = false)
    svc.appliedCoordinatePayloads.incl(encodeSignedRecord(record))
    controlApplyCheckpoint(processed, yieldPoints, maxSliceMs, sliceStartedAt)
  let tailElapsed = nowMillis() - sliceStartedAt
  if tailElapsed > maxSliceMs:
    maxSliceMs = tailElapsed
  let migrationRecordsNode =
    if request.kind == JObject:
      request.getOrDefault("migrationRecords")
    else:
      newJNull()
  let migrationResult = await applyMigrationRecordsAsync(svc, migrationRecordsNode)
  yieldPoints += migrationResult.yields
  if migrationResult.maxSliceMs > maxSliceMs:
    maxSliceMs = migrationResult.maxSliceMs
  let isolationEvidenceNode =
    if request.kind == JObject:
      request.getOrDefault("isolationEvidence")
    else:
      newJNull()
  let isolationResult = await applyIsolationEvidenceAsync(svc, isolationEvidenceNode)
  yieldPoints += isolationResult.yields
  if isolationResult.maxSliceMs > maxSliceMs:
    maxSliceMs = isolationResult.maxSliceMs
  if touchedPeerIds.len > 0:
    let promoteResult = await promoteCoordinateClosureAsync(svc, touchedPeerIds)
    yieldPoints += promoteResult.yields
    if promoteResult.maxSliceMs > maxSliceMs:
      maxSliceMs = promoteResult.maxSliceMs
    if promoteResult.changedPeerIds.len > 0:
      svc.queueCoordinateDelta(promoteResult.changedPeerIds)
  svc.noteControlApplyElapsed(
    "publish-apply",
    decoded.records.len,
    migrationResult.applied,
    isolationResult.applied,
    yieldPoints,
    maxSliceMs,
    startedAt,
  )

proc controlSlowApplyCount*(svc: LsmrService): int64 {.gcsafe, raises: [].} =
  if svc.isNil:
    return 0
  svc.slowControlApplyCount

proc controlLastApplyElapsedMs*(svc: LsmrService): int64 {.gcsafe, raises: [].} =
  if svc.isNil:
    return 0
  svc.lastControlApplyElapsedMs

proc controlMaxApplyElapsedMs*(svc: LsmrService): int64 {.gcsafe, raises: [].} =
  if svc.isNil:
    return 0
  svc.maxControlApplyElapsedMs

proc controlSessionKey(peerId: PeerId): string =
  $peerId

proc getOrCreateDialLock(svc: LsmrService, peerId: PeerId): AsyncLock =
  let key = controlSessionKey(peerId)
  if not svc.dialLocks.hasKey(key):
    svc.dialLocks[key] = newAsyncLock()
  svc.dialLocks.getOrDefault(key)

proc getOrCreateControlSession(
    svc: LsmrService, peerId: PeerId
): LsmrControlSession =
  let key = controlSessionKey(peerId)
  if not svc.controlSessions.hasKey(key):
    svc.controlSessions[key] = LsmrControlSession(
      peerId: peerId,
      conn: nil,
      syncPending: false,
      publishPending: false,
      publishAllKnown: false,
      publishPeers: initTable[string, PeerId](),
      runner: nil,
      lock: newAsyncLock(),
      publishedCoordinateDigests: initHashSet[string](),
      publishedMigrationDigests: initHashSet[string](),
      publishedIsolationDigests: initHashSet[string](),
      publishedCoordinatePayloads: initHashSet[string](),
      publishedMigrationPayloads: initHashSet[string](),
      publishedIsolationPayloads: initHashSet[string](),
    )
  svc.controlSessions.getOrDefault(key)

proc releaseSessionLock(session: LsmrControlSession) =
  if session.isNil or session.lock.isNil:
    return
  try:
    session.lock.release()
  except AsyncLockError:
    discard

proc releaseDialLock(lock: AsyncLock) =
  if lock.isNil:
    return
  try:
    lock.release()
  except AsyncLockError:
    discard

proc releaseRefreshLock(svc: LsmrService) =
  if svc.isNil or svc.refreshLock.isNil:
    return
  try:
    svc.refreshLock.release()
  except AsyncLockError:
    discard

proc closeControlSession(
    svc: LsmrService, session: LsmrControlSession
): Future[void] {.async: (raises: [CancelledError]).} =
  if session.isNil or session.conn.isNil:
    return
  let conn = session.conn
  session.conn = nil
  try:
    await conn.close()
  except CancelledError as exc:
    raise exc
  except CatchableError:
    discard

proc coordinateRecordOrder(
    a, b: SignedLsmrCoordinateRecord
): int =
  result = cmp(a.data.certifiedDepthValue(), b.data.certifiedDepthValue())
  if result != 0:
    return
  result = cmp(a.data.issuedAtMs, b.data.issuedAtMs)
  if result != 0:
    return
  result = cmp($a.data.peerId, $b.data.peerId)

proc coordinatePromotionRecordOrder(
    a, b: SignedLsmrCoordinateRecord
): int =
  result = cmp(a.data.epochId, b.data.epochId)
  if result != 0:
    return
  result = cmp(a.data.certifiedDepthValue(), b.data.certifiedDepthValue())
  if result != 0:
    return
  result = cmp(a.data.issuedAtMs, b.data.issuedAtMs)
  if result != 0:
    return
  return cmp($a.data.peerId, $b.data.peerId)

proc storeCoordinateRecord(
    switch: Switch,
    config: LsmrConfig,
    record: SignedLsmrCoordinateRecord,
    promoteNow = true,
) {.gcsafe.} =
  if switch.isNil or switch.peerStore.isNil:
    return
  let incomingPrefix = record.data.certifiedPrefix()
  let activeDeeperSameEpoch =
    switch.peerStore[ActiveLsmrBook].contains(record.data.peerId) and
    switch.peerStore[ActiveLsmrBook][record.data.peerId].data.epochId == record.data.epochId and
    switch.peerStore[ActiveLsmrBook][record.data.peerId].data.certifiedPrefix() != incomingPrefix and
    pathStartsWith(
      switch.peerStore[ActiveLsmrBook][record.data.peerId].data.certifiedPrefix(),
      incomingPrefix,
    )
  let alreadyKnownSame =
    switch.peerStore[LsmrBook].contains(record.data.peerId) and
    switch.peerStore[LsmrBook][record.data.peerId] == record
  let alreadyActiveSame =
    switch.peerStore[ActiveLsmrBook].contains(record.data.peerId) and
    switch.peerStore[ActiveLsmrBook][record.data.peerId] == record
  var chain = switch.peerStore[LsmrChainBook][record.data.peerId]
  if record notin chain:
    chain.add(record)
    chain.sort(coordinateRecordOrder)
    switch.peerStore[LsmrChainBook][record.data.peerId] = chain
  if activeDeeperSameEpoch:
    switch.peerStore[LsmrBook][record.data.peerId] =
      switch.peerStore[ActiveLsmrBook][record.data.peerId]
    return
  switch.peerStore[LsmrBook][record.data.peerId] = record
  if alreadyKnownSame or alreadyActiveSame:
    return
  if promoteNow:
    discard promoteValidatedCoordinateRecord(switch, config, record.data.peerId)

proc sortedCoordinateRecords(
    records: seq[SignedLsmrCoordinateRecord]
): seq[SignedLsmrCoordinateRecord] {.gcsafe.} =
  result = records
  result.sort(coordinateRecordOrder)

proc sortedPromotionCoordinateRecords(
    records: seq[SignedLsmrCoordinateRecord]
): seq[SignedLsmrCoordinateRecord] {.gcsafe.} =
  result = records
  result.sort(coordinatePromotionRecordOrder)

proc activeCoordinateIdentity(
    switch: Switch, peerId: PeerId
): string {.gcsafe.} =
  if switch.isNil or switch.peerStore.isNil or not switch.peerStore[ActiveLsmrBook].contains(peerId):
    return ""
  let record = switch.peerStore[ActiveLsmrBook][peerId]
  $record.data.epochId & "|" & $record.data.certifiedPrefix() & "|" &
    record.data.recordDigest

proc coordinatePromotionOrder(
    svc: LsmrService, peerIds: openArray[PeerId]
): seq[PeerId] {.gcsafe.} =
  if svc.isNil or svc.switch.isNil or svc.switch.peerStore.isNil:
    return @[]
  var seen = initHashSet[string]()
  var ordered: seq[PeerId] = @[]
  proc addPeerId(peerId: PeerId) =
    let key = $peerId
    if peerId.data.len == 0 or key in seen:
      return
    seen.incl(key)
    ordered.add(peerId)

  for peerId in peerIds:
    addPeerId(peerId)
  for peerId, _ in svc.switch.peerStore[LsmrChainBook].book.pairs:
    addPeerId(peerId)

  ordered.sort(proc(a, b: PeerId): int =
    let aKnown = svc.switch.peerStore[LsmrBook].contains(a)
    let bKnown = svc.switch.peerStore[LsmrBook].contains(b)
    if aKnown != bKnown:
      return cmp(ord(bKnown), ord(aKnown))
    if aKnown and bKnown:
      let aRecord = svc.switch.peerStore[LsmrBook][a]
      let bRecord = svc.switch.peerStore[LsmrBook][b]
      result = cmp(aRecord.data.certifiedDepthValue(), bRecord.data.certifiedDepthValue())
      if result != 0:
        return result
      result = cmp(aRecord.data.epochId, bRecord.data.epochId)
      if result != 0:
        return result
    return cmp($a, $b)
  )
  result = ordered

proc promoteCoordinateClosureAsync(
    svc: LsmrService,
    peerIds: seq[PeerId],
): Future[tuple[
    changes: int, changedPeerIds: seq[PeerId], yields: int, maxSliceMs: int64
  ]] {.
    async: (raises: [CancelledError])
.} =
  if svc.isNil or svc.switch.isNil or svc.switch.peerStore.isNil:
    return
  let orderedPeerIds = svc.coordinatePromotionOrder(peerIds)
  if orderedPeerIds.len == 0:
    return
  var processed = 0
  var sliceStartedAt = nowMillis()
  var changedPeerKeys = initHashSet[string]()
  var changed = true
  var pass = 0
  while changed and pass < max(1, orderedPeerIds.len):
    changed = false
    inc pass
    for peerId in orderedPeerIds:
      let originalLatest =
        if svc.switch.peerStore[LsmrBook].contains(peerId):
          some(svc.switch.peerStore[LsmrBook][peerId])
        else:
          none(SignedLsmrCoordinateRecord)
      var chain = svc.switch.peerStore[LsmrChainBook][peerId].sortedPromotionCoordinateRecords()
      if chain.len == 0 and originalLatest.isSome():
        chain = @[originalLatest.get()]
      for record in chain:
        svc.switch.peerStore[LsmrBook][peerId] = record
        let before = activeCoordinateIdentity(svc.switch, peerId)
        discard promoteValidatedCoordinateRecord(svc.switch, svc.config, peerId)
        let after = activeCoordinateIdentity(svc.switch, peerId)
        if before != after:
          changed = true
          inc result.changes
          let key = $peerId
          if key notin changedPeerKeys:
            changedPeerKeys.incl(key)
            result.changedPeerIds.add(peerId)
        controlApplyCheckpoint(processed, result.yields, result.maxSliceMs, sliceStartedAt)
      if chain.len > 0:
        svc.switch.peerStore[LsmrBook][peerId] = chain[^1]
      elif originalLatest.isSome():
        svc.switch.peerStore[LsmrBook][peerId] = originalLatest.get()
  let tailElapsed = nowMillis() - sliceStartedAt
  if tailElapsed > result.maxSliceMs:
    result.maxSliceMs = tailElapsed

proc encodedLocalRecord(svc: LsmrService): Option[seq[byte]] =
  if svc.isNil or svc.switch.isNil or svc.switch.peerInfo.isNil:
    return none(seq[byte])
  if not svc.switch.peerInfo.metadata.hasKey(LsmrCoordinateMetadataKey):
    return none(seq[byte])
  some(svc.switch.peerInfo.metadata.getOrDefault(LsmrCoordinateMetadataKey))

proc localCoordinateRecord*(switch: Switch): Option[SignedLsmrCoordinateRecord] =
  if switch.isNil or switch.peerInfo.isNil:
    return none(SignedLsmrCoordinateRecord)
  if switch.peerInfo.metadata.hasKey(LsmrCoordinateMetadataKey):
    let decoded =
      SignedLsmrCoordinateRecord.decode(
        switch.peerInfo.metadata.getOrDefault(LsmrCoordinateMetadataKey)
      )
    if decoded.isOk():
      return some(decoded.get())
  if not switch.peerStore.isNil and switch.peerStore[LsmrBook].contains(switch.peerInfo.peerId):
    return some(switch.peerStore[LsmrBook][switch.peerInfo.peerId])
  none(SignedLsmrCoordinateRecord)

proc activeCoordinateRecord*(
    switch: Switch, peerId: PeerId
): Option[SignedLsmrCoordinateRecord] =
  if switch.isNil or switch.peerStore.isNil:
    return none(SignedLsmrCoordinateRecord)
  if switch.peerStore[ActiveLsmrBook].contains(peerId):
    return some(switch.peerStore[ActiveLsmrBook][peerId])
  none(SignedLsmrCoordinateRecord)

proc getLsmrService*(switch: Switch): LsmrService =
  if switch.isNil:
    return nil
  for service in switch.services:
    if service of LsmrService:
      return LsmrService(service)

proc shouldPublishCoordinatePeer(svc: LsmrService, peerId: PeerId): bool {.gcsafe.} =
  if svc.isNil or svc.switch.isNil or peerId.data.len == 0 or
      peerId == svc.switch.peerInfo.peerId:
    return false
  for connectedPeerId in svc.connectedPeerIds():
    if connectedPeerId == peerId:
      return true
  if knowsCoordinatePeer(svc, peerId):
    return true
  if svc.nearFieldRecords.hasKey(peerId):
    return true
  for anchor in svc.config.anchors:
    if anchor.peerId == peerId:
      return true
  false

proc hasDialableRoute(svc: LsmrService, peerId: PeerId): bool {.gcsafe.} =
  if svc.isNil or svc.switch.isNil or peerId.data.len == 0:
    return false
  if svc.switch.isConnected(peerId):
    return true
  if not svc.switch.peerStore.isNil and svc.switch.peerStore.getAddresses(peerId).len > 0:
    return true
  let anchor = svc.findEffectiveAnchor(peerId)
  anchor.isSome() and anchor.get().addrs.len > 0

proc queueTopologyDelta(
    svc: LsmrService, publishAllKnown = false
) {.gcsafe, raises: [].} =
  if svc.isNil or svc.switch.isNil:
    return
  let selfPeerId =
    if svc.switch.peerInfo.isNil:
      PeerId()
    else:
      svc.switch.peerInfo.peerId
  for peerId in svc.knownSyncPeers():
    if peerId.data.len == 0 or peerId == selfPeerId or not svc.hasDialableRoute(peerId):
      continue
    if publishAllKnown or shouldPublishCoordinatePeer(svc, peerId):
      svc.queueControlPublish(peerId)

proc queueTopologyConvergence(
    svc: LsmrService, publishAllKnown = false
) {.gcsafe, raises: [].} =
  if svc.isNil or svc.switch.isNil:
    return
  let selfPeerId =
    if svc.switch.peerInfo.isNil:
      PeerId()
    else:
      svc.switch.peerInfo.peerId
  for peerId in svc.knownSyncPeers():
    if peerId.data.len == 0 or peerId == selfPeerId or not svc.hasDialableRoute(peerId):
      continue
    if publishAllKnown or shouldPublishCoordinatePeer(svc, peerId):
      svc.queueControlSync(peerId)
      svc.queueControlPublish(peerId)

proc notifyTopologyChanged(switch: Switch) {.gcsafe, raises: [].} =
  let svc = getLsmrService(switch)
  if not svc.isNil:
    inc svc.topologyChangeSeq
    svc.scheduleCoordinateRevalidation()
    if localCoordinateRecord(svc.switch).isSome():
      svc.scheduleOverlayMaintenance()
    if not svc.topologyChangedHook.isNil:
      svc.topologyChangedHook()

proc setTopologyChangedHook*(
    svc: LsmrService, hook: proc() {.gcsafe, raises: [].}
) {.gcsafe, raises: [].} =
  if svc.isNil:
    return
  svc.topologyChangedHook = hook

proc setTopologyChangedHook*(
    switch: Switch, hook: proc() {.gcsafe, raises: [].}
) {.gcsafe, raises: [].} =
  let svc = getLsmrService(switch)
  if svc.isNil:
    return
  svc.topologyChangedHook = hook

proc findAnchor(anchors: openArray[LsmrAnchor], peerId: PeerId): Option[LsmrAnchor] =
  for anchor in anchors:
    if anchor.peerId == peerId:
      return some(anchor)
  none(LsmrAnchor)

proc findAnchor(config: LsmrConfig, peerId: PeerId): Option[LsmrAnchor] =
  config.anchors.findAnchor(peerId)

proc providerAllowed(
    config: LsmrConfig, provider: NearFieldBootstrapProvider
): bool =
  if not config.enableNearFieldBootstrap:
    return false
  if config.nearFieldProviders.len == 0:
    return true
  for item in config.nearFieldProviders:
    if item == provider:
      return true
  false

proc mergeUniqueAddrs(target: var seq[MultiAddress], values: seq[MultiAddress]) =
  for value in values:
    var found = false
    for existing in target:
      if existing == value:
        found = true
        break
    if not found:
      target.add(value)

proc effectiveAnchors(svc: LsmrService, nowMs = 0'i64): seq[LsmrAnchor] {.gcsafe.} =
  if svc.isNil:
    return @[]
  let currentMs = if nowMs > 0: nowMs else: nowMillis()
  var merged = initTable[string, LsmrAnchor]()
  var hasExternalRootIssuer = false
  for anchor in svc.config.anchors:
    var enriched = anchor
    if not svc.switch.isNil and not svc.switch.peerStore.isNil:
      let knownAddrs = svc.switch.peerStore.getAddresses(anchor.peerId)
      if knownAddrs.len > 0:
        enriched.addrs.mergeUniqueAddrs(knownAddrs)
    merged[$anchor.peerId] = enriched
    if enriched.canIssueRootCert or enriched.attestedPrefix.len == 0:
      hasExternalRootIssuer = true
  for peerId, record in svc.nearFieldRecords.pairs:
    if record.networkId != svc.config.networkId or record.expiresAtMs <= currentMs:
      continue
    let key = $peerId
    if merged.hasKey(key):
      var existing = merged.getOrDefault(key)
      existing.addrs.mergeUniqueAddrs(record.addrs)
      if existing.operatorId.len == 0:
        existing.operatorId = record.operatorId
      if existing.attestedPrefix.len == 0:
        existing.attestedPrefix = record.attestedPrefix
      if existing.serveDepth == 0'u8:
        existing.serveDepth = record.serveDepth
      if existing.directionMask == 0'u32:
        existing.directionMask = record.directionMask
      existing.canIssueRootCert = existing.canIssueRootCert or record.canIssueRootCert
      if not isValidLsmrDigit(existing.regionDigit):
        existing.regionDigit = record.regionDigit
      merged[key] = existing
    else:
      merged[key] = LsmrAnchor(
        peerId: record.peerId,
        addrs: record.addrs,
        operatorId: record.operatorId,
        regionDigit: record.regionDigit,
        attestedPrefix: record.attestedPrefix,
        serveDepth: record.serveDepth,
        directionMask: record.directionMask,
        canIssueRootCert: record.canIssueRootCert,
      )
    if record.canIssueRootCert:
      hasExternalRootIssuer = true
  if not hasExternalRootIssuer and not svc.switch.isNil and not svc.switch.peerInfo.isNil and
      svc.config.serveWitness and isValidLsmrDigit(svc.config.regionDigit):
    merged[$svc.switch.peerInfo.peerId] = LsmrAnchor(
      peerId: svc.switch.peerInfo.peerId,
      addrs: svc.switch.peerInfo.addrs,
      operatorId: svc.config.operatorId,
      regionDigit: svc.config.regionDigit,
      attestedPrefix: @[svc.config.regionDigit],
      serveDepth: uint8(max(1, svc.config.serveDepth)),
      directionMask: svc.config.directionMask,
      canIssueRootCert: true,
    )
  for _, anchor in merged.pairs:
    result.add(anchor)

proc findEffectiveAnchor(svc: LsmrService, peerId: PeerId): Option[LsmrAnchor] {.gcsafe.} =
  for anchor in svc.effectiveAnchors():
    if anchor.peerId == peerId:
      return some(anchor)
  none(LsmrAnchor)

proc currentEpoch(config: LsmrConfig, nowMs = 0'i64): uint64 =
  let current = if nowMs > 0: nowMs else: nowMillis()
  let ttlMs = max(1'i64, durationToMillis(config.recordTtl))
  uint64(current div ttlMs)

proc rootPrefix(anchor: LsmrAnchor): LsmrPath {.gcsafe.}

proc desiredCertifiedPrefixes(
    svc: LsmrService, nowMs = 0'i64
): seq[LsmrPath] {.gcsafe.} =
  if svc.isNil:
    return @[]
  var rootDigit = 0'u8
  if isValidLsmrDigit(svc.config.regionDigit):
    rootDigit = svc.config.regionDigit
  else:
    let currentMs = if nowMs > 0: nowMs else: nowMillis()
    var operatorRoots = initTable[uint8, HashSet[string]]()
    for anchor in svc.effectiveAnchors(currentMs):
      let root = anchor.rootPrefix()
      if root.len != 1:
        continue
      let operatorId =
        if anchor.operatorId.len > 0:
          anchor.operatorId
        else:
          $anchor.peerId
      operatorRoots.mgetOrPut(root[0], initHashSet[string]()).incl(operatorId)

    var winningDigit = 0'u8
    var winningCount = 0
    var ambiguous = false
    for digit, operators in operatorRoots.pairs:
      if operators.len > winningCount:
        winningDigit = digit
        winningCount = operators.len
        ambiguous = false
      elif operators.len == winningCount and operators.len > 0 and digit != winningDigit:
        ambiguous = true

    if winningCount >= svc.config.minWitnessQuorum and not ambiguous:
      rootDigit = winningDigit
    else:
      localCoordinateRecord(svc.switch).withValue(record):
        let prefix = record.data.certifiedPrefix()
        if prefix.len > 0 and isValidLsmrDigit(prefix[0]):
          rootDigit = prefix[0]

  if not isValidLsmrDigit(rootDigit):
    return @[]

  var prefix = @[rootDigit]
  result.add(prefix)
  let targetDepth = min(max(1, svc.config.serveDepth), 1 + svc.config.localSuffix.len)
  for idx in 0 ..< targetDepth - 1:
    if idx >= svc.config.localSuffix.len or
        not isValidLsmrDigit(svc.config.localSuffix[idx]):
      break
    prefix = prefix & @[svc.config.localSuffix[idx]]
    result.add(prefix)

proc rootPrefix(anchor: LsmrAnchor): LsmrPath {.gcsafe.} =
  if anchor.attestedPrefix.len > 0:
    @[anchor.attestedPrefix[0]]
  elif isValidLsmrDigit(anchor.regionDigit):
    @[anchor.regionDigit]
  else:
    @[]

proc desiredCertifiedPrefixes*(config: LsmrConfig): seq[LsmrPath] =
  if not isValidLsmrDigit(config.regionDigit):
    return @[]
  var prefix = @[config.regionDigit]
  result.add(prefix)
  let targetDepth = min(max(1, config.serveDepth), 1 + config.localSuffix.len)
  for idx in 0 ..< targetDepth - 1:
    if idx >= config.localSuffix.len or not isValidLsmrDigit(config.localSuffix[idx]):
      break
    prefix = prefix & @[config.localSuffix[idx]]
    result.add(prefix)

proc witnessCanIssuePrefix(
    switch: Switch,
    anchors: openArray[LsmrAnchor],
    witness: LsmrWitness,
    targetPrefix: LsmrPath,
): bool =
  if targetPrefix.len == 0 or witness.attestedPrefix != targetPrefix:
    return false
  if targetPrefix.len == 1:
    let anchor = anchors.findAnchor(witness.anchorPeerId)
    if anchor.isNone():
      return false
    let cfg = anchor.get()
    if not cfg.canIssueRootCert and cfg.attestedPrefix.len > 0:
      return false
    return cfg.rootPrefix() == targetPrefix

  let parent = targetPrefix.parentPrefix()
  if switch.isNil or switch.peerStore.isNil:
    return false
  if not switch.peerStore[ActiveLsmrBook].contains(witness.anchorPeerId):
    return false
  let issuerPrefix = switch.peerStore[ActiveLsmrBook][witness.anchorPeerId].data.certifiedPrefix()
  pathStartsWith(issuerPrefix, parent)

proc derivePrefixFromWitnesses*(
    switch: Switch,
    config: LsmrConfig,
    anchors: openArray[LsmrAnchor],
    witnesses: seq[SignedLsmrWitness],
    subjectPeerId: PeerId,
    networkId: string,
    targetPrefix: LsmrPath,
    nowMs = 0'i64,
): tuple[
    trust: LsmrValidationTrust,
    reason: string,
    prefix: LsmrPath,
    confidence: uint8,
    witnessCount: int,
    expiresAtMs: int64,
    coverageMask: uint32,
  ] =
  let currentMs = if nowMs > 0: nowMs else: nowMillis()
  if targetPrefix.len == 0 or not targetPrefix.isValidLsmrPath():
    return (rejected, "invalid_target_prefix", @[], 0'u8, 0, 0'i64, 0'u32)
  if witnesses.len == 0:
    return (quarantine, "missing_witnesses", @[], 0'u8, 0, 0'i64, 0'u32)

  var operatorIds = initHashSet[string]()
  var anchorIds = initHashSet[string]()
  var minExpiry = int64.high
  var coverageMask = 0'u32
  for signedWitness in witnesses:
    if signedWitness.checkValid().isErr:
      return (rejected, "invalid_witness_signature", @[], 0'u8, 0, 0'i64, 0'u32)
    let witness = signedWitness.data
    if witness.subjectPeerId != subjectPeerId:
      return (rejected, "witness_subject_mismatch", @[], 0'u8, 0, 0'i64, 0'u32)
    if witness.networkId != networkId:
      return (rejected, "witness_network_mismatch", @[], 0'u8, 0, 0'i64, 0'u32)
    if witness.attestedPrefix != targetPrefix:
      return (rejected, "witness_prefix_mismatch", @[], 0'u8, 0, 0'i64, 0'u32)
    if not witnessCanIssuePrefix(switch, anchors, witness, targetPrefix):
      return (rejected, "unauthorized_witness_issuer", @[], 0'u8, 0, 0'i64, 0'u32)
    if witness.expiresAtMs <= currentMs:
      continue
    operatorIds.incl(witness.operatorId)
    anchorIds.incl($witness.anchorPeerId)
    minExpiry = min(minExpiry, witness.expiresAtMs)
    coverageMask = coverageMask or expectedCoverageMask(witness.attestedPrefix)

  if anchorIds.len == 0:
    return (quarantine, "stale_witnesses", @[], 0'u8, 0, 0'i64, 0'u32)
  if operatorIds.len < config.minWitnessQuorum:
    return (quarantine, "insufficient_operator_quorum", @[], 0'u8, anchorIds.len, minExpiry, coverageMask)
  if coverageMask != expectedCoverageMask(targetPrefix):
    return (quarantine, "insufficient_coverage", @[], 0'u8, anchorIds.len, minExpiry, coverageMask)

  let confidence = uint8(min(100, operatorIds.len * 35 + targetPrefix.len * 10))
  (trusted, "ok", targetPrefix, confidence, anchorIds.len, minExpiry, coverageMask)

proc derivePrefixFromWitnesses*(
    switch: Switch,
    config: LsmrConfig,
    witnesses: seq[SignedLsmrWitness],
    subjectPeerId: PeerId,
    networkId: string,
    targetPrefix: LsmrPath,
    nowMs = 0'i64,
): tuple[
    trust: LsmrValidationTrust,
    reason: string,
    prefix: LsmrPath,
    confidence: uint8,
    witnessCount: int,
    expiresAtMs: int64,
    coverageMask: uint32,
  ] =
  switch.derivePrefixFromWitnesses(
    config,
    config.anchors,
    witnesses,
    subjectPeerId,
    networkId,
    targetPrefix,
    nowMs,
  )

proc hasMatchingMigrationRecord(
    switch: Switch, record: LsmrCoordinateRecord, certifiedPrefix: LsmrPath
): bool =
  if switch.isNil or switch.peerStore.isNil:
    return false
  for item in switch.peerStore[LsmrMigrationBook][record.peerId]:
    if item.data.networkId != record.networkId or item.data.epochId != record.epochId:
      continue
    if item.data.toPrefix != certifiedPrefix:
      continue
    if item.data.migrationDigest == record.migrationDigest:
      return true
  false

proc validateLsmrPeer*(
    switch: Switch, config: LsmrConfig, peerId: PeerId, nowMs = 0'i64
): LsmrValidationSnapshot {.gcsafe.} =
  result = LsmrValidationSnapshot(
    trust: quarantine,
    reason: "missing_record",
    recordSeen: false,
    attestedPrefix: @[],
    confidence: 0'u8,
    witnessCount: 0,
    expiresAtMs: 0'i64,
    distance: high(int),
  )
  if switch.isNil or switch.peerStore.isNil or peerId.data.len == 0:
    return
  if not switch.peerStore[LsmrBook].contains(peerId):
    return
  result.recordSeen = true
  let record = switch.peerStore[LsmrBook][peerId]
  if record.checkValid().isErr:
    result.trust = rejected
    result.reason = "invalid_coordinate_signature"
    return

  let data = record.data
  let currentMs = if nowMs > 0: nowMs else: nowMillis()
  let knownAnchors =
    block:
      let svc = getLsmrService(switch)
      if svc.isNil:
        config.anchors
      else:
        svc.effectiveAnchors(currentMs)
  result.confidence = data.confidence
  result.witnessCount = data.witnesses.len
  result.expiresAtMs = data.expiresAtMs
  let certifiedPrefix =
    if data.version <= LegacyLsmrVersion:
      data.attestedPrefix
    else:
      data.certifiedPrefix()
  result.attestedPrefix = certifiedPrefix

  if data.networkId != config.networkId:
    result.trust = rejected
    result.reason = "coordinate_network_mismatch"
    return

  if data.expiresAtMs <= currentMs:
    result.trust = quarantine
    result.reason = "stale_record"
    return

  if data.version <= LegacyLsmrVersion:
    result.trust = quarantine
    result.reason = "legacy_record"
    return

  let derived = derivePrefixFromWitnesses(
    switch,
    config,
    knownAnchors,
    data.witnesses,
    data.peerId,
    data.networkId,
    certifiedPrefix,
    currentMs,
  )
  result.trust = derived.trust
  result.reason = derived.reason
  result.attestedPrefix = derived.prefix
  result.confidence = max(result.confidence, derived.confidence)
  result.witnessCount = derived.witnessCount
  result.expiresAtMs = derived.expiresAtMs

  if derived.trust != trusted:
    return
  if certifiedPrefix != derived.prefix:
    result.trust = rejected
    result.reason = "coordinate_prefix_mismatch"
    result.attestedPrefix = @[]
    return
  if countCoverageBits(data.coverageMask) == 0:
    result.trust = quarantine
    result.reason = "insufficient_coverage"
    return
  if data.parentDigest != computeExpectedParentDigest(data.networkId, certifiedPrefix, data.epochId):
    result.trust = rejected
    result.reason = "invalid_parent_digest"
    return
  let staticMigrationDigest =
    computeStaticMigrationDigest(data.peerId, data.networkId, certifiedPrefix, data.epochId)
  if data.migrationDigest != staticMigrationDigest and
      not hasMatchingMigrationRecord(switch, data, certifiedPrefix):
    result.trust = quarantine
    result.reason = "missing_migration_record"
    return
  if data.confidence < derived.confidence:
    result.trust = quarantine
    result.reason = "low_confidence"
    return

  localCoordinateRecord(switch).withValue(localRecord):
    let localPrefix = localRecord.data.certifiedPrefix()
    let localCheck =
      derivePrefixFromWitnesses(
        switch,
        config,
        knownAnchors,
        localRecord.data.witnesses,
        localRecord.data.peerId,
        localRecord.data.networkId,
        localPrefix,
        currentMs,
      )
    if localCheck.trust == trusted:
      result.distance = lsmrDistance(localCheck.prefix, certifiedPrefix)

proc promoteValidatedCoordinateRecord*(
    switch: Switch, config: LsmrConfig, peerId: PeerId
): bool {.gcsafe.} =
  if switch.isNil or switch.peerStore.isNil:
    return false
  if not switch.peerStore[LsmrBook].contains(peerId):
    let removed = switch.peerStore[ActiveLsmrBook].del(peerId)
    if removed:
      switch.notifyTopologyChanged()
    return false
  let incoming = switch.peerStore[LsmrBook][peerId]
  let snapshot = validateLsmrPeer(switch, config, peerId)
  if snapshot.trust != trusted:
    when defined(lsmr_diag):
      echo "lsmr promote reject self=", $switch.peerInfo.peerId,
        " peer=", $peerId,
        " trust=", $snapshot.trust,
        " reason=", snapshot.reason,
        " prefix=", incoming.data.certifiedPrefix(),
        " epoch=", incoming.data.epochId
    let removed = switch.peerStore[ActiveLsmrBook].del(peerId)
    if removed:
      switch.notifyTopologyChanged()
    return false
  if switch.peerStore[ActiveLsmrBook].contains(peerId):
    let active = switch.peerStore[ActiveLsmrBook][peerId]
    let activePrefix = active.data.certifiedPrefix()
    let incomingPrefix = incoming.data.certifiedPrefix()
    if incoming.data.epochId < active.data.epochId or
        (incoming.data.epochId == active.data.epochId and incomingPrefix != activePrefix):
      if incoming.data.epochId == active.data.epochId:
        if pathStartsWith(incomingPrefix, activePrefix):
          discard
        elif pathStartsWith(activePrefix, incomingPrefix):
          return true
        else:
          let evidence = LsmrIsolationEvidence(
            peerId: peerId,
            networkId: incoming.data.networkId,
            epochId: incoming.data.epochId,
            reason: LsmrIsolationReason.lirMultiLocation,
            detail: "multi_location",
            coordinateDigest: incoming.data.recordDigest,
            conflictingDigest: active.data.recordDigest,
            issuedAtMs: nowMillis(),
            witnesses: incoming.data.witnesses,
            evidenceDigest: "",
            version: DefaultLsmrVersion,
          )
          var finalized = evidence
          finalized.evidenceDigest = computeIsolationDigest(finalized)
          let signed = SignedLsmrIsolationEvidence.init(switch.peerInfo.privateKey, finalized).valueOr:
            let removed = switch.peerStore[ActiveLsmrBook].del(peerId)
            if removed:
              switch.notifyTopologyChanged()
            return false
          discard installIsolationEvidence(switch, signed)
          return false
      else:
        let evidence = LsmrIsolationEvidence(
          peerId: peerId,
          networkId: incoming.data.networkId,
          epochId: incoming.data.epochId,
          reason: LsmrIsolationReason.lirReverseEpoch,
          detail: "reverse_epoch",
          coordinateDigest: incoming.data.recordDigest,
          conflictingDigest: active.data.recordDigest,
          issuedAtMs: nowMillis(),
          witnesses: incoming.data.witnesses,
          evidenceDigest: "",
          version: DefaultLsmrVersion,
        )
        var finalized = evidence
        finalized.evidenceDigest = computeIsolationDigest(finalized)
        let signed = SignedLsmrIsolationEvidence.init(switch.peerInfo.privateKey, finalized).valueOr:
          let removed = switch.peerStore[ActiveLsmrBook].del(peerId)
          if removed:
            switch.notifyTopologyChanged()
          return false
        discard installIsolationEvidence(switch, signed)
        return false
  let changed =
    if switch.peerStore[ActiveLsmrBook].contains(peerId):
      switch.peerStore[ActiveLsmrBook][peerId] != incoming
    else:
      true
  switch.peerStore[ActiveLsmrBook][peerId] = incoming
  when defined(lsmr_diag):
    echo "lsmr promote active self=", $switch.peerInfo.peerId,
      " peer=", $peerId,
      " prefix=", incoming.data.certifiedPrefix(),
      " epoch=", incoming.data.epochId,
      " changed=", changed
  if changed:
    switch.notifyTopologyChanged()
  true

proc currentCertifiedPrefix(svc: LsmrService): LsmrPath =
  if svc.isNil or svc.switch.isNil:
    return @[]
  let local = localCoordinateRecord(svc.switch)
  if local.isNone():
    return @[]
  local.get().data.certifiedPrefix()

proc witnessIssuersForPrefix*(
    svc: LsmrService, targetPrefix: LsmrPath, nowMs = 0'i64
): seq[PeerId] =
  if svc.isNil or targetPrefix.len == 0:
    return @[]
  let currentMs = if nowMs > 0: nowMs else: nowMillis()
  if targetPrefix.len == 1:
    for anchor in svc.effectiveAnchors(currentMs):
      if (anchor.canIssueRootCert or anchor.attestedPrefix.len == 0) and anchor.rootPrefix() == targetPrefix:
        result.add(anchor.peerId)
    return

  let parent = targetPrefix.parentPrefix()
  if svc.switch.isNil or svc.switch.peerStore.isNil:
    return @[]
  let selfPeerId =
    if svc.switch.peerInfo.isNil:
      PeerId()
    else:
      svc.switch.peerInfo.peerId
  let localPrefix = svc.currentCertifiedPrefix()
  if selfPeerId.data.len > 0 and
      targetPrefix.len <= max(1, svc.config.serveDepth) and
      localPrefix.len > 0 and
      pathStartsWith(localPrefix, parent):
    result.add(selfPeerId)

  var others: seq[PeerId] = @[]
  for peerId, record in svc.switch.peerStore[ActiveLsmrBook].book.pairs:
    if peerId == selfPeerId:
      continue
    let prefix = record.data.certifiedPrefix()
    if prefix.len > 0 and pathStartsWith(prefix, parent):
      others.add(peerId)
  others.sort(proc (a, b: PeerId): int = cmp($a, $b))
  result.add(others)

proc canWitnessPrefix*(svc: LsmrService, targetPrefix: LsmrPath): bool =
  if svc.isNil or targetPrefix.len == 0 or not targetPrefix.isValidLsmrPath():
    return false
  if targetPrefix.len > max(1, svc.config.serveDepth):
    return false
  let selfPeerId =
    if svc.switch.isNil or svc.switch.peerInfo.isNil:
      PeerId()
    else:
      svc.switch.peerInfo.peerId
  if targetPrefix.len == 1:
    let anchor = svc.findEffectiveAnchor(selfPeerId)
    return anchor.isSome() and
      (anchor.get().canIssueRootCert or anchor.get().attestedPrefix.len == 0) and
      anchor.get().rootPrefix() == targetPrefix
  let localPrefix = svc.currentCertifiedPrefix()
  if localPrefix.len == 0:
    return false
  pathStartsWith(localPrefix, targetPrefix.parentPrefix())

proc buildLocalWitness(
    svc: LsmrService,
    subjectPeerId: PeerId,
    targetPrefix: LsmrPath,
    rttMs: uint32 = 1'u32,
): Option[SignedLsmrWitness] =
  if svc.isNil or svc.switch.isNil or subjectPeerId.data.len == 0:
    return none(SignedLsmrWitness)
  if not svc.canWitnessPrefix(targetPrefix):
    return none(SignedLsmrWitness)
  let issuedAt = nowMillis()
  let expiresAt = issuedAt + durationToMillis(svc.config.recordTtl)
  let witness = LsmrWitness(
    subjectPeerId: subjectPeerId,
    anchorPeerId: svc.switch.peerInfo.peerId,
    networkId: svc.config.networkId,
    regionDigit: targetPrefix[0],
    latencyDigit: targetPrefix[^1],
    rttMs: max(1'u32, rttMs),
    operatorId: svc.config.operatorId,
    issuedAtMs: issuedAt,
    expiresAtMs: expiresAt,
    attestedPrefix: targetPrefix,
    prefixDepth: uint8(targetPrefix.len),
    directionMask: svc.config.directionMask,
    epochId: svc.config.currentEpoch(issuedAt),
    version: DefaultLsmrVersion,
  )
  let signed = SignedLsmrWitness.init(svc.switch.peerInfo.privateKey, witness).valueOr:
    return none(SignedLsmrWitness)
  some(signed)

proc buildLocalMigrationRecord(
    svc: LsmrService,
    fromPrefix, toPrefix: LsmrPath,
    oldRecordDigest: string,
    witnesses: seq[SignedLsmrWitness],
    epochId: uint64,
): Option[SignedLsmrMigrationRecord] =
  if svc.isNil or svc.switch.isNil or fromPrefix.len == 0 or toPrefix.len == 0 or fromPrefix == toPrefix:
    return none(SignedLsmrMigrationRecord)
  var record = LsmrMigrationRecord(
    peerId: svc.switch.peerInfo.peerId,
    networkId: svc.config.networkId,
    epochId: epochId,
    fromPrefix: fromPrefix,
    toPrefix: toPrefix,
    oldRecordDigest: oldRecordDigest,
    newParentDigest: computeExpectedParentDigest(svc.config.networkId, toPrefix, epochId),
    issuedAtMs: nowMillis(),
    witnesses: witnesses,
    migrationDigest: "",
    version: DefaultLsmrVersion,
  )
  record.migrationDigest = computeMigrationDigest(record)
  let signed = SignedLsmrMigrationRecord.init(svc.switch.peerInfo.privateKey, record).valueOr:
    return none(SignedLsmrMigrationRecord)
  some(signed)

proc new*(
    T: typedesc[LsmrService],
    config: LsmrConfig = LsmrConfig.init(),
    rng: ref HmacDrbgContext = newRng(),
): T =
  T(
    config: config,
    rng: rng,
    refreshLock: newAsyncLock(),
    refreshPending: false,
    refreshAllPublish: false,
    refreshPublishPeers: initTable[string, PeerId](),
    refreshRunner: nil,
    nearFieldRecords: initTable[PeerId, NearFieldHandshakeRecord](),
    controlSessions: initTable[string, LsmrControlSession](),
    connectFlights: initTable[string, Future[bool]](),
    dialLocks: initTable[string, AsyncLock](),
    topologyChangeSeq: 0'u64,
    appliedCoordinatePayloads: initHashSet[string](),
    appliedMigrationPayloads: initHashSet[string](),
    appliedIsolationPayloads: initHashSet[string](),
    applyQueue: initDeque[LsmrControlApplyItem](),
  )

proc buildWitnessProtocol(svc: LsmrService): LPProtocol =
  proc handleWitness(conn: Connection, proto: string) {.async: (raises: [CancelledError]).} =
    let requestStartedAt = nowMillis()
    try:
      when defined(lsmr_diag):
        echo "lsmr witness-handler read-request self=", $svc.switch.peerInfo.peerId,
          " peer=", $conn.peerId,
          " ts=", requestStartedAt
      let payload = await conn.readLp(8 * 1024)
      when defined(lsmr_diag):
        echo "lsmr witness-handler read-request-done self=", $svc.switch.peerInfo.peerId,
          " peer=", $conn.peerId,
          " bytes=", payload.len,
          " ts=", nowMillis(),
          " elapsedMs=", nowMillis() - requestStartedAt
      let request = parseJson(bytesToString(payload))
      if request.kind != JObject:
        return
      let networkId =
        if request.hasKey("networkId"):
          request["networkId"].getStr()
        else:
          ""
      if networkId.len > 0 and networkId != svc.config.networkId:
        return
      let targetPrefix =
        if request.hasKey("targetPrefix"):
          jsonToPath(request["targetPrefix"])
        else:
          @[]
      when defined(lsmr_diag):
        echo "lsmr witness-handler request self=", $svc.switch.peerInfo.peerId,
          " peer=", $conn.peerId,
          " target=", targetPrefix,
          " ts=", nowMillis(),
          " elapsedMs=", nowMillis() - requestStartedAt
      if not svc.canWitnessPrefix(targetPrefix):
        when defined(lsmr_diag):
          echo "lsmr witness-handler reject self=", $svc.switch.peerInfo.peerId,
            " peer=", $conn.peerId,
            " target=", targetPrefix,
            " ts=", nowMillis(),
            " elapsedMs=", nowMillis() - requestStartedAt
        return

      var challenge = newSeq[byte](16)
      hmacDrbgGenerate(svc.rng[], challenge)
      let challengeStartedAt = Moment.now()
      when defined(lsmr_diag):
        echo "lsmr witness-handler write-challenge self=", $svc.switch.peerInfo.peerId,
          " peer=", $conn.peerId,
          " target=", targetPrefix,
          " ts=", nowMillis()
      await conn.writeLp(challenge)
      let echoed = await conn.readLp(128)
      when defined(lsmr_diag):
        echo "lsmr witness-handler read-echo self=", $svc.switch.peerInfo.peerId,
          " peer=", $conn.peerId,
          " target=", targetPrefix,
          " bytes=", echoed.len,
          " ts=", nowMillis()
      if echoed != challenge:
        when defined(lsmr_diag):
          echo "lsmr witness-handler echo-mismatch self=", $svc.switch.peerInfo.peerId,
            " peer=", $conn.peerId,
            " target=", targetPrefix,
            " ts=", nowMillis()
        return
      let rttMs = uint32(max(1'i64, durationToMillis(Moment.now() - challengeStartedAt)))
      let signed = svc.buildLocalWitness(conn.peerId, targetPrefix, rttMs).valueOr:
        when defined(lsmr_diag):
          echo "lsmr witness-handler build-miss self=", $svc.switch.peerInfo.peerId,
            " peer=", $conn.peerId,
            " target=", targetPrefix,
            " ts=", nowMillis()
        return
      let encoded = signed.encode().valueOr:
        return
      let response = %*{
        "ok": true,
        "witness": base64.encode(bytesToString(encoded)),
      }
      when defined(lsmr_diag):
        echo "lsmr witness-handler write-response self=", $svc.switch.peerInfo.peerId,
          " peer=", $conn.peerId,
          " target=", targetPrefix,
          " ts=", nowMillis(),
          " elapsedMs=", nowMillis() - requestStartedAt
      await conn.writeLp(stringToBytes($response))
      if knowsCoordinatePeer(svc, conn.peerId) or targetPrefix.len == 1:
        svc.queueControlPublish(conn.peerId)
    except CancelledError as exc:
      raise exc
    except CatchableError as exc:
      when defined(lsmr_diag):
        echo "lsmr witness-handler fail self=", $svc.switch.peerInfo.peerId,
          " peer=", $conn.peerId,
          " err=", exc.msg,
          " ts=", nowMillis(),
          " elapsedMs=", nowMillis() - requestStartedAt
      debug "lsmr witness handler failed", err = exc.msg
    finally:
      await conn.close()

  LPProtocol.new(@[svc.config.witnessCodec], handleWitness)

proc buildControlProtocol(svc: LsmrService): LPProtocol =
  proc handleControl(conn: Connection, proto: string) {.async: (raises: [CancelledError]).} =
    try:
      let payload = await conn.readLp(256 * 1024)
      if payload.len == 0:
        return
      let request = parseJson(bytesToString(payload))
      if request.kind != JObject:
        return
      let op =
        if request.hasKey("op"):
          request["op"].getStr()
        else:
          ""
      case op
      of "coordinate_sync":
        let response = svc.buildCoordinateSyncResponse()
        when defined(lsmr_diag):
          let coordsNode = response.getOrDefault("coordinateRecords")
          let migrationsNode = response.getOrDefault("migrationRecords")
          let isolationsNode = response.getOrDefault("isolationEvidence")
          echo "lsmr sync response self=", $svc.switch.peerInfo.peerId,
            " peer=", $conn.peerId,
            " coords=", (if coordsNode.kind == JArray: coordsNode.len else: 0),
            " migrations=", (if migrationsNode.kind == JArray: migrationsNode.len else: 0),
            " isolations=", (if isolationsNode.kind == JArray: isolationsNode.len else: 0)
        await conn.writeLp(stringToBytes($response))
      of "coordinate_publish":
        when defined(lsmr_diag):
          let coordsNode = request.getOrDefault("coordinateRecords")
          let migrationsNode = request.getOrDefault("migrationRecords")
          let isolationsNode = request.getOrDefault("isolationEvidence")
          echo "lsmr publish request self=", $svc.switch.peerInfo.peerId,
            " peer=", $conn.peerId,
            " coords=", (if coordsNode.kind == JArray: coordsNode.len else: 0),
            " migrations=", (if migrationsNode.kind == JArray: migrationsNode.len else: 0),
            " isolations=", (if isolationsNode.kind == JArray: isolationsNode.len else: 0)
        svc.scheduleControlApply(lcakPublish, request)
        await conn.writeLp(stringToBytes($(%*{"ok": true})))
      of "migration_request":
        let targetPrefix =
          if request.hasKey("targetPrefix"):
            jsonToPath(request["targetPrefix"])
          else:
            @[]
        if not svc.canWitnessPrefix(targetPrefix):
          await conn.writeLp(stringToBytes($(%*{"ok": false})))
        else:
          let epochId = svc.config.currentEpoch()
          let parentDigest = computeExpectedParentDigest(svc.config.networkId, targetPrefix, epochId)
          await conn.writeLp(stringToBytes($(%*{
            "ok": true,
            "parentDigest": parentDigest,
          })))
      of "isolation_broadcast":
        if request.hasKey("evidence"):
          decodeSignedIsolation(request["evidence"].getStr()).withValue(evidence):
            discard installIsolationEvidence(svc.switch, evidence)
        await conn.writeLp(stringToBytes($(%*{"ok": true})))
      else:
        await conn.writeLp(stringToBytes($(%*{"ok": false})))
    except CancelledError as exc:
      raise exc
    except CatchableError as exc:
      if not conn.closed and not conn.atEof:
        debug "lsmr control handler failed", err = exc.msg
    finally:
      await conn.close()

  LPProtocol.new(@[svc.config.controlCodec], handleControl)

proc installLocalCoordinateRecord(
    svc: LsmrService, record: SignedLsmrCoordinateRecord
) =
  let encoded = record.encode().valueOr:
    return
  svc.switch.peerInfo.metadata[LsmrCoordinateMetadataKey] = encoded
  if svc.switch.isNil or svc.switch.peerStore.isNil:
    return
  storeCoordinateRecord(svc.switch, svc.config, record, promoteNow = false)
  discard promoteValidatedCoordinateRecord(svc.switch, svc.config, record.data.peerId)
  when defined(lsmr_diag):
    let selfPeerId =
      if svc.switch.peerInfo.isNil:
        PeerId()
      else:
        svc.switch.peerInfo.peerId
    let selfChainLen =
      if selfPeerId.data.len == 0:
        0
      else:
        svc.switch.peerStore[LsmrChainBook][selfPeerId].len
    echo "lsmr install-local-record self=", $selfPeerId,
      " recordPeer=", $record.data.peerId,
      " prefix=", record.data.certifiedPrefix(),
      " chainLen=", selfChainLen,
      " latest=", (if svc.switch.peerStore[LsmrBook].contains(record.data.peerId): $svc.switch.peerStore[LsmrBook][record.data.peerId].data.certifiedPrefix() else: "[]")

proc installMigrationRecord*(
    switch: Switch, record: SignedLsmrMigrationRecord
): bool {.gcsafe.} =
  if switch.isNil or switch.peerStore.isNil:
    return false
  var existing = switch.peerStore[LsmrMigrationBook][record.data.peerId]
  if record in existing:
    return false
  existing.add(record)
  switch.peerStore[LsmrMigrationBook][record.data.peerId] = existing
  true

proc installIsolationEvidence*(
    switch: Switch, evidence: SignedLsmrIsolationEvidence
): bool {.gcsafe.} =
  if switch.isNil or switch.peerStore.isNil:
    return false
  var existing = switch.peerStore[LsmrIsolationBook][evidence.data.peerId]
  if evidence in existing:
    return false
  existing.add(evidence)
  switch.peerStore[LsmrIsolationBook][evidence.data.peerId] = existing
  let removed = switch.peerStore[ActiveLsmrBook].del(evidence.data.peerId)
  if removed:
    switch.notifyTopologyChanged()
  true

proc installNearFieldHandshake*(
    svc: LsmrService, record: NearFieldHandshakeRecord
): bool =
  if svc.isNil or svc.switch.isNil or record.peerId.data.len == 0:
    return false
  if record.networkId != svc.config.networkId or
      not svc.config.providerAllowed(record.provider):
    return false
  if record.addrs.len == 0:
    return false
  svc.nearFieldRecords[record.peerId] = record
  if not svc.switch.peerStore.isNil:
    let ttl =
      max(1'i64, record.expiresAtMs - max(0'i64, record.issuedAtMs))
    svc.switch.peerStore.addAddressesWithTTL(
      record.peerId,
      record.addrs,
      chronos.milliseconds(int(ttl)),
    )
  if localCoordinateRecord(svc.switch).isSome():
    svc.schedulePeerConnect(record.peerId)
    svc.queueControlPublish(record.peerId)
    svc.queueControlSync(record.peerId)
  else:
    scheduleImmediateRefresh(svc, some(record.peerId))
  if localCoordinateRecord(svc.switch).isSome():
    svc.scheduleOverlayMaintenance()
  true

proc observeNearFieldBootstrap*(
    switch: Switch, observation: NearFieldObservation
): bool =
  let svc = getLsmrService(switch)
  if svc.isNil or observation.peerId.data.len == 0:
    return false
  let observedAt = if observation.observedAtMs > 0: observation.observedAtMs else: nowMillis()
  let ttlMs =
    if observation.ttlMs > 0:
      observation.ttlMs
    else:
      max(1'i64, durationToMillis(svc.config.recordTtl))
  let record = NearFieldHandshakeRecord(
    provider: observation.provider,
    networkId:
      if observation.networkId.len > 0: observation.networkId else: svc.config.networkId,
    peerId: observation.peerId,
    addrs: observation.addrs,
    operatorId: observation.operatorId,
    regionDigit: observation.regionDigit,
    attestedPrefix: observation.attestedPrefix,
    serveDepth: observation.serveDepth,
    directionMask: observation.directionMask,
    canIssueRootCert: observation.canIssueRootCert,
    issuedAtMs: observedAt,
    expiresAtMs: observedAt + ttlMs,
  )
  svc.installNearFieldHandshake(record)

proc connectedPeerIds(svc: LsmrService): seq[PeerId] {.gcsafe.} =
  if svc.isNil or svc.switch.isNil:
    return @[]
  var seen = initHashSet[string]()
  for peerId in svc.switch.connectedPeers(Direction.Out) & svc.switch.connectedPeers(Direction.In):
    let key = $peerId
    if key in seen:
      continue
    seen.incl(key)
    result.add(peerId)

proc knownSyncPeers(svc: LsmrService): seq[PeerId] {.gcsafe.} =
  if svc.isNil or svc.switch.isNil or svc.switch.peerStore.isNil:
    return @[]
  var seen = initHashSet[string]()
  let selfPeerId = svc.switch.peerInfo.peerId

  for peerId in svc.connectedPeerIds():
    if peerId.data.len == 0 or peerId == selfPeerId:
      continue
    let key = $peerId
    if key in seen:
      continue
    seen.incl(key)
    result.add(peerId)
  for anchor in svc.effectiveAnchors():
    let peerId = anchor.peerId
    if peerId.data.len == 0 or peerId == selfPeerId:
      continue
    let key = $peerId
    if key in seen:
      continue
    seen.incl(key)
    result.add(peerId)
  for peerId, _ in svc.switch.peerStore[AddressBook].book.pairs:
    if peerId.data.len == 0 or peerId == selfPeerId:
      continue
    let key = $peerId
    if key in seen:
      continue
    seen.incl(key)
    result.add(peerId)
  for peerId, _ in svc.switch.peerStore[LsmrBook].book.pairs:
    if peerId.data.len == 0 or peerId == selfPeerId:
      continue
    let key = $peerId
    if key in seen:
      continue
    seen.incl(key)
    result.add(peerId)
  for peerId, _ in svc.switch.peerStore[ActiveLsmrBook].book.pairs:
    if peerId.data.len == 0 or peerId == selfPeerId:
      continue
    let key = $peerId
    if key in seen:
      continue
    seen.incl(key)
    result.add(peerId)

proc knowsCoordinatePeer(svc: LsmrService, peerId: PeerId): bool {.gcsafe.} =
  if svc.isNil or svc.switch.isNil or svc.switch.peerStore.isNil or
      peerId.data.len == 0:
    return false
  svc.switch.peerStore[LsmrBook].contains(peerId) or
    svc.switch.peerStore[LsmrChainBook].contains(peerId) or
    svc.switch.peerStore[ActiveLsmrBook].contains(peerId)

proc desiredOverlayPeers(svc: LsmrService): seq[PeerId] {.gcsafe.} =
  if svc.isNil or svc.switch.isNil or svc.switch.peerStore.isNil:
    return @[]
  var seen = initHashSet[string]()
  let selfPeerId = svc.switch.peerInfo.peerId
  let view = svc.switch.peerStore.topologyNeighborView(selfPeerId)
  if view.isSome():
    let item = view.get()
    for peerId in item.sameCellPeers:
      if peerId.data.len == 0 or peerId == selfPeerId:
        continue
      let key = $peerId
      if key in seen:
        continue
      seen.incl(key)
      result.add(peerId)
    for peerId in item.parentPrefixPeers:
      if peerId.data.len == 0 or peerId == selfPeerId:
        continue
      let key = $peerId
      if key in seen:
        continue
      seen.incl(key)
      result.add(peerId)
    for bucket in item.directionalPeers:
      for peerId in bucket.peers:
        if peerId.data.len == 0 or peerId == selfPeerId:
          continue
        let key = $peerId
        if key in seen:
          continue
        seen.incl(key)
        result.add(peerId)
  when defined(lsmr_diag):
    echo "lsmr overlay-plan self=", $selfPeerId,
      " peers=", result.mapIt($it).join(","),
      " active=", svc.switch.peerStore[ActiveLsmrBook].book.len

proc performOverlayConnect(
    svc: LsmrService, peerId: PeerId
): Future[bool] {.async: (raises: [CancelledError]).} =
  if svc.isNil or svc.switch.isNil or peerId.data.len == 0:
    return false
  if svc.switch.isConnected(peerId):
    when defined(lsmr_diag):
      echo "lsmr overlay-connect-skip self=", $svc.switch.peerInfo.peerId,
        " peer=", $peerId,
        " reason=already_connected",
        " ts=", nowMillis()
    return true
  var addrs: seq[MultiAddress] = @[]
  if not svc.switch.peerStore.isNil:
    addrs = svc.switch.peerStore.getAddresses(peerId)
  if addrs.len == 0:
    let anchor = svc.findEffectiveAnchor(peerId)
    if anchor.isSome():
      addrs = anchor.get().addrs
  if addrs.len == 0:
    when defined(lsmr_diag):
      echo "lsmr overlay-connect-miss self=", $svc.switch.peerInfo.peerId,
        " peer=", $peerId,
        " ts=", nowMillis()
    return false
  let startedAt = nowMillis()
  try:
    when defined(lsmr_diag):
      echo "lsmr overlay-connect-begin self=", $svc.switch.peerInfo.peerId,
        " peer=", $peerId,
        " addrs=", addrs.mapIt($it).join(","),
        " ts=", startedAt
    await svc.switch.connect(
      peerId,
      addrs,
      forceDial = false,
      reuseConnection = true,
      dir = Direction.Out,
    )
    when defined(lsmr_diag):
      echo "lsmr overlay-connect-ok self=", $svc.switch.peerInfo.peerId,
        " peer=", $peerId,
        " ts=", nowMillis(),
        " elapsedMs=", nowMillis() - startedAt
    true
  except CancelledError as exc:
    raise exc
  except CatchableError as exc:
    when defined(lsmr_diag):
      echo "lsmr overlay-connect-fail self=", $svc.switch.peerInfo.peerId,
        " peer=", $peerId,
        " err=", exc.msg,
        " ts=", nowMillis(),
        " elapsedMs=", nowMillis() - startedAt
    false

proc connectOverlayPeer(
    svc: LsmrService, peerId: PeerId
): Future[bool] {.async: (raises: [CancelledError]).} =
  if svc.isNil or svc.switch.isNil or peerId.data.len == 0:
    return false
  if svc.switch.isConnected(peerId):
    when defined(lsmr_diag):
      echo "lsmr overlay-connect-skip self=", $svc.switch.peerInfo.peerId,
        " peer=", $peerId,
        " reason=already_connected",
        " ts=", nowMillis()
    return true
  let key = controlSessionKey(peerId)
  if svc.connectFlights.hasKey(key):
    let pending = svc.connectFlights.getOrDefault(key)
    if not pending.isNil and not pending.finished():
      when defined(lsmr_diag):
        echo "lsmr overlay-connect-join self=", $svc.switch.peerInfo.peerId,
          " peer=", $peerId,
          " ts=", nowMillis()
      try:
        return await pending
      except CancelledError as exc:
        raise exc
      except CatchableError as exc:
        when defined(lsmr_diag):
          echo "lsmr overlay-connect-join-fail self=", $svc.switch.peerInfo.peerId,
            " peer=", $peerId,
            " err=", exc.msg,
            " ts=", nowMillis()
        return false
    svc.connectFlights.del(key)
  let pending = svc.performOverlayConnect(peerId)
  svc.connectFlights[key] = pending
  try:
    try:
      return await pending
    except CancelledError as exc:
      raise exc
    except CatchableError as exc:
      when defined(lsmr_diag):
        echo "lsmr overlay-connect-flight-fail self=", $svc.switch.peerInfo.peerId,
          " peer=", $peerId,
          " err=", exc.msg,
          " ts=", nowMillis()
      return false
  finally:
    if svc.connectFlights.hasKey(key):
      let current = svc.connectFlights.getOrDefault(key)
      if current == pending:
        svc.connectFlights.del(key)

proc dialProtocolPeer(
    svc: LsmrService, peerId: PeerId, proto: string
): Future[Connection] {.gcsafe, async: (raises: [CancelledError]).} =
  if svc.isNil or svc.switch.isNil or peerId.data.len == 0:
    return nil
  let startedAt = nowMillis()
  let dialLock = svc.getOrCreateDialLock(peerId)
  try:
    await dialLock.acquire()
    try:
      when defined(lsmr_diag):
        echo "lsmr proto-dial-begin self=", $svc.switch.peerInfo.peerId,
          " peer=", $peerId,
          " proto=", proto,
          " ts=", startedAt
      if not svc.switch.isConnected(peerId):
        when defined(lsmr_diag):
          echo "lsmr proto-dial-connect-begin self=", $svc.switch.peerInfo.peerId,
            " peer=", $peerId,
            " proto=", proto,
            " ts=", nowMillis()
        if not await svc.connectOverlayPeer(peerId):
          when defined(lsmr_diag):
            echo "lsmr proto-dial-connect-miss self=", $svc.switch.peerInfo.peerId,
              " peer=", $peerId,
              " proto=", proto,
              " ts=", nowMillis(),
              " elapsedMs=", nowMillis() - startedAt
          return nil
        when defined(lsmr_diag):
          echo "lsmr proto-dial-connect-ok self=", $svc.switch.peerInfo.peerId,
            " peer=", $peerId,
            " proto=", proto,
            " ts=", nowMillis(),
            " elapsedMs=", nowMillis() - startedAt
      when defined(lsmr_diag):
        echo "lsmr proto-dial-stream-begin self=", $svc.switch.peerInfo.peerId,
          " peer=", $peerId,
          " proto=", proto,
          " ts=", nowMillis()
      let conn = await svc.switch.dial(peerId, @[proto])
      when defined(lsmr_diag):
        echo "lsmr proto-dial-", (if conn.isNil: "miss" else: "ok"),
          " self=", $svc.switch.peerInfo.peerId,
          " peer=", $peerId,
          " proto=", proto,
          " ts=", nowMillis(),
          " elapsedMs=", nowMillis() - startedAt
      conn
    finally:
      releaseDialLock(dialLock)
  except CancelledError as exc:
    releaseDialLock(dialLock)
    raise exc
  except CatchableError as exc:
    releaseDialLock(dialLock)
    when defined(lsmr_diag):
      echo "lsmr proto-dial-fail self=", $svc.switch.peerInfo.peerId,
        " peer=", $peerId,
        " proto=", proto,
        " err=", exc.msg,
        " ts=", nowMillis(),
        " elapsedMs=", nowMillis() - startedAt
    nil

proc schedulePeerConnect(svc: LsmrService, peerId: PeerId) {.gcsafe, raises: [].} =
  if svc.isNil or svc.switch.isNil or not svc.running or peerId.data.len == 0 or
      peerId == svc.switch.peerInfo.peerId:
    return
  proc connectTask() {.async: (raises: []).} =
    if svc.isNil or svc.switch.isNil or not svc.running:
      return
    try:
      discard await svc.connectOverlayPeer(peerId)
    except CancelledError:
      discard
    except CatchableError as exc:
      debug "lsmr bootstrap connect failed", peerId = peerId, err = exc.msg
  asyncSpawn connectTask()

proc maintainOverlayConnections(svc: LsmrService): Future[void] {.async: (raises: []).} =
  if svc.isNil or svc.switch.isNil or not svc.running:
    return
  let desired = svc.desiredOverlayPeers()
  when defined(lsmr_diag):
    echo "lsmr overlay-maintain self=", $svc.switch.peerInfo.peerId,
      " desired=", desired.mapIt($it).join(","),
      " running=", svc.running
  for peerId in desired:
    try:
      discard await svc.connectOverlayPeer(peerId)
    except CancelledError:
      return
    except CatchableError:
      discard

proc runOverlayMaintenance(svc: LsmrService): Future[void] {.async: (raises: []).} =
  while svc.running and svc.overlayPending:
    svc.overlayPending = false
    await svc.maintainOverlayConnections()
  svc.overlayRunner = nil

proc scheduleOverlayMaintenance(svc: LsmrService) {.gcsafe, raises: [].} =
  if svc.isNil or not svc.running:
    return
  svc.overlayPending = true
  if svc.overlayRunner.isNil or svc.overlayRunner.finished():
    svc.overlayRunner = svc.runOverlayMaintenance()
    asyncSpawn svc.overlayRunner

proc runCoordinateRevalidation(svc: LsmrService): Future[void] {.async: (raises: []).} =
  while svc.running and svc.revalidatePending:
    svc.revalidatePending = false
    if svc.isNil or svc.switch.isNil or svc.switch.peerStore.isNil:
      continue
    var peerIds: seq[PeerId] = @[]
    for peerId, _ in svc.switch.peerStore[LsmrChainBook].book.pairs:
      peerIds.add(peerId)
    try:
      let promoteResult = await promoteCoordinateClosureAsync(svc, peerIds)
      if promoteResult.changedPeerIds.len > 0:
        svc.queueCoordinateDelta(promoteResult.changedPeerIds)
    except CancelledError:
      return
    except CatchableError:
      discard
  svc.revalidateRunner = nil

proc scheduleCoordinateRevalidation(svc: LsmrService) {.gcsafe, raises: [].} =
  if svc.isNil or not svc.running:
    return
  svc.revalidatePending = true
  if svc.revalidateRunner.isNil or svc.revalidateRunner.finished():
    svc.revalidateRunner = svc.runCoordinateRevalidation()
    asyncSpawn svc.revalidateRunner

proc runControlApplyQueue(svc: LsmrService): Future[void] {.async: (raises: []).} =
  while svc.running and svc.applyQueue.len > 0:
    let item = svc.applyQueue.popFirst()
    try:
      case item.kind
      of lcakSync:
        await svc.applyCoordinateRecords(item.node)
      of lcakPublish:
        await svc.applyCoordinatePublishRequest(item.node)
    except CancelledError:
      return
    except CatchableError as exc:
      when defined(lsmr_diag):
        echo "lsmr apply-queue-fail self=", $svc.switch.peerInfo.peerId,
          " kind=", controlApplyKindName(item.kind),
          " pending=", svc.applyQueue.len,
          " err=", exc.msg
      debug "lsmr queued control apply failed",
        kind = controlApplyKindName(item.kind), err = exc.msg
  svc.applyRunner = nil

proc scheduleControlApply(
    svc: LsmrService, kind: LsmrControlApplyKind, node: JsonNode
) {.gcsafe, raises: [].} =
  if svc.isNil or not svc.running or node.isNil or node.kind != JObject:
    return
  svc.applyQueue.addLast(LsmrControlApplyItem(kind: kind, node: node))
  when defined(lsmr_diag):
    echo "lsmr apply-queue self=", $svc.switch.peerInfo.peerId,
      " kind=", controlApplyKindName(kind),
      " pending=", svc.applyQueue.len
  if svc.applyRunner.isNil or svc.applyRunner.finished():
    svc.applyRunner = svc.runControlApplyQueue()
    asyncSpawn svc.applyRunner

proc dialControlPeer(svc: LsmrService, peerId: PeerId): Future[Connection] {.gcsafe, async: (raises: [CancelledError]).} =
  await svc.dialProtocolPeer(peerId, svc.config.controlCodec)

proc ensureControlConnection(
    svc: LsmrService, session: LsmrControlSession
): Future[Connection] {.gcsafe, async: (raises: [CancelledError]).} =
  if session.isNil:
    return nil
  await svc.closeControlSession(session)
  when defined(lsmr_diag):
    echo "lsmr control-dial-begin self=", $svc.switch.peerInfo.peerId,
      " peer=", $session.peerId
  session.conn = await svc.dialControlPeer(session.peerId)
  when defined(lsmr_diag):
    echo "lsmr control-dial-", (if session.conn.isNil: "miss" else: "ok"),
      " self=", $svc.switch.peerInfo.peerId,
      " peer=", $session.peerId
  session.conn

proc sendControlRequest(
    svc: LsmrService,
    conn: Connection,
    request: JsonNode,
    maxBytes: int,
    timeout: Duration = 500.milliseconds,
): Future[Option[JsonNode]] {.gcsafe, async: (raises: [CancelledError]).} =
  if conn.isNil:
    return none(JsonNode)
  discard timeout
  let op =
    if request.kind == JObject:
      request.getOrDefault("op").getStr()
    else:
      ""
  try:
    when defined(lsmr_diag):
      echo "lsmr control-send-write-begin self=", $svc.switch.peerInfo.peerId,
        " peer=", $conn.peerId,
        " op=", op
    await conn.writeLp(stringToBytes($request))
    when defined(lsmr_diag):
      echo "lsmr control-send-write-done self=", $svc.switch.peerInfo.peerId,
        " peer=", $conn.peerId,
        " op=", op
      echo "lsmr control-send-read-begin self=", $svc.switch.peerInfo.peerId,
        " peer=", $conn.peerId,
        " op=", op
    let payload = await conn.readLp(maxBytes)
    when defined(lsmr_diag):
      echo "lsmr control-send-read-done self=", $svc.switch.peerInfo.peerId,
        " peer=", $conn.peerId,
        " op=", op,
        " bytes=", payload.len
    let response = parseJson(bytesToString(payload))
    some(response)
  except CancelledError as exc:
    raise exc
  except CatchableError:
    when defined(lsmr_diag):
      echo "lsmr control-send-fail self=", $svc.switch.peerInfo.peerId,
        " peer=", $conn.peerId,
        " op=", op,
        " err=", getCurrentExceptionMsg()
    none(JsonNode)

proc requestWitnessFromPeer*(
    svc: LsmrService, peerId: PeerId, targetPrefix: LsmrPath
): Future[Option[SignedLsmrWitness]] {.async: (raises: [CancelledError]).} =
  if svc.isNil or svc.switch.isNil or targetPrefix.len == 0:
    return none(SignedLsmrWitness)
  let startedAt = nowMillis()
  inc svc.witnessRequests
  if peerId == svc.switch.peerInfo.peerId:
    let localWitness = svc.buildLocalWitness(peerId, targetPrefix)
    if localWitness.isSome():
      inc svc.witnessSuccess
      when defined(lsmr_diag):
        echo "lsmr local witness ok peer=", $peerId, " prefix=", targetPrefix
    elif defined(lsmr_diag):
      echo "lsmr local witness miss peer=", $peerId, " prefix=", targetPrefix
    return localWitness
  try:
    let knownAddrs =
      if svc.switch.peerStore.isNil:
        @[]
      else:
        svc.switch.peerStore.getAddresses(peerId)
    if knownAddrs.len == 0 and svc.findEffectiveAnchor(peerId).isNone() and
        not svc.switch.isConnected(peerId):
      when defined(lsmr_diag):
        echo "lsmr witness no addrs requester=", $svc.switch.peerInfo.peerId,
          " issuer=", $peerId,
          " prefix=", targetPrefix,
          " ts=", startedAt
      return none(SignedLsmrWitness)
    when defined(lsmr_diag):
      echo "lsmr witness dial-begin requester=", $svc.switch.peerInfo.peerId,
        " issuer=", $peerId,
        " prefix=", targetPrefix,
        " ts=", startedAt
    let conn = await svc.dialProtocolPeer(peerId, svc.config.witnessCodec)
    when defined(lsmr_diag):
      echo "lsmr witness dial-", (if conn.isNil: "miss" else: "ok"),
        " requester=", $svc.switch.peerInfo.peerId,
        " issuer=", $peerId,
        " prefix=", targetPrefix,
        " ts=", nowMillis(),
        " elapsedMs=", nowMillis() - startedAt
    if conn.isNil:
      return none(SignedLsmrWitness)
    defer:
      await conn.close()
    let req = %*{
      "op": "witness",
      "networkId": svc.config.networkId,
      "targetPrefix": pathToJson(targetPrefix),
    }
    when defined(lsmr_diag):
      echo "lsmr witness write-request requester=", $svc.switch.peerInfo.peerId,
        " issuer=", $peerId,
        " prefix=", targetPrefix,
        " ts=", nowMillis(),
        " elapsedMs=", nowMillis() - startedAt
    await conn.writeLp(stringToBytes($req))
    when defined(lsmr_diag):
      echo "lsmr witness read-challenge requester=", $svc.switch.peerInfo.peerId,
        " issuer=", $peerId,
        " prefix=", targetPrefix,
        " ts=", nowMillis(),
        " elapsedMs=", nowMillis() - startedAt
    let challenge = await conn.readLp(128)
    when defined(lsmr_diag):
      echo "lsmr witness challenge-bytes requester=", $svc.switch.peerInfo.peerId,
        " issuer=", $peerId,
        " prefix=", targetPrefix,
        " bytes=", challenge.len,
        " ts=", nowMillis(),
        " elapsedMs=", nowMillis() - startedAt
    if challenge.len == 0:
      return none(SignedLsmrWitness)
    when defined(lsmr_diag):
      echo "lsmr witness write-echo requester=", $svc.switch.peerInfo.peerId,
        " issuer=", $peerId,
        " prefix=", targetPrefix,
        " ts=", nowMillis(),
        " elapsedMs=", nowMillis() - startedAt
    await conn.writeLp(challenge)
    when defined(lsmr_diag):
      echo "lsmr witness read-response requester=", $svc.switch.peerInfo.peerId,
        " issuer=", $peerId,
        " prefix=", targetPrefix,
        " ts=", nowMillis(),
        " elapsedMs=", nowMillis() - startedAt
    let payload = await conn.readLp(16 * 1024)
    when defined(lsmr_diag):
      echo "lsmr witness response-bytes requester=", $svc.switch.peerInfo.peerId,
        " issuer=", $peerId,
        " prefix=", targetPrefix,
        " bytes=", payload.len,
        " ts=", nowMillis(),
        " elapsedMs=", nowMillis() - startedAt
    let response = parseJson(bytesToString(payload))
    if response.kind != JObject or not response.hasKey("witness"):
      return none(SignedLsmrWitness)
    let decoded = stringToBytes(base64.decode(response["witness"].getStr()))
    let witness = SignedLsmrWitness.decode(decoded).valueOr:
      return none(SignedLsmrWitness)
    inc svc.witnessSuccess
    when defined(lsmr_diag):
      echo "lsmr remote witness ok requester=", $svc.switch.peerInfo.peerId,
        " issuer=", $peerId,
        " prefix=", targetPrefix,
        " ts=", nowMillis(),
        " elapsedMs=", nowMillis() - startedAt
    some(witness)
  except CancelledError as exc:
    raise exc
  except CatchableError as exc:
    when defined(lsmr_diag):
      echo "lsmr witness request failed requester=", $svc.switch.peerInfo.peerId,
        " issuer=", $peerId,
        " prefix=", targetPrefix,
        " err=", exc.msg,
        " ts=", nowMillis(),
        " elapsedMs=", nowMillis() - startedAt
    debug "lsmr witness request failed", peerId = peerId, err = exc.msg
    none(SignedLsmrWitness)

proc syncCoordinatesOnConn(
    svc: LsmrService,
    conn: Connection,
    session: LsmrControlSession,
    peerId: PeerId,
): Future[bool] {.async: (raises: [CancelledError]).} =
  let response = await sendControlRequest(svc, conn, %*{"op": "coordinate_sync"}, 512 * 1024)
  if response.isNone():
    return false
  let node = response.get()
  let okNode =
    if node.kind == JObject:
      node.getOrDefault("ok")
    else:
      newJNull()
  if okNode.kind != JBool or not okNode.getBool():
    return false
  when defined(lsmr_diag):
    echo "lsmr sync-apply-begin self=", $svc.switch.peerInfo.peerId,
      " peer=", $peerId
  await svc.applyCoordinateRecords(node)
  when defined(lsmr_diag):
    let coordsNode = node.getOrDefault("coordinateRecords")
    let migrationsNode = node.getOrDefault("migrationRecords")
    let isolationsNode = node.getOrDefault("isolationEvidence")
    echo "lsmr sync ok self=", $svc.switch.peerInfo.peerId,
      " peer=", $peerId,
      " coords=", (if coordsNode.kind == JArray: coordsNode.len else: 0),
      " migrations=", (if migrationsNode.kind == JArray: migrationsNode.len else: 0),
      " isolations=", (if isolationsNode.kind == JArray: isolationsNode.len else: 0),
      " applied=true"
  true

proc publishLocalStateOnConn(
    svc: LsmrService,
    conn: Connection,
    session: LsmrControlSession,
    peerId: PeerId,
): Future[bool] {.async: (raises: [CancelledError]).} =
  let batch = svc.buildCoordinatePublishBatch(session)
  if batch.request.isNil or batch.request.kind != JObject:
    if batch.publishAllKnown:
      session.publishAllKnown = false
    when defined(lsmr_diag):
      echo "lsmr publish-skip self=", $svc.switch.peerInfo.peerId,
        " peer=", $peerId,
        " reason=no_delta"
    return true
  let response = await sendControlRequest(svc, conn, batch.request, 8 * 1024)
  if response.isNone():
    session.restoreQueuedPublishState(batch)
    return false
  let node = response.get()
  let okNode =
    if node.kind == JObject:
      node.getOrDefault("ok")
    else:
      newJNull()
  let ok = okNode.kind == JBool and okNode.getBool()
  if ok:
    session.notePublishedBatch(batch)
  else:
    session.restoreQueuedPublishState(batch)
  when defined(lsmr_diag):
    if ok:
      let coordsNode = batch.request.getOrDefault("coordinateRecords")
      let migrationsNode = batch.request.getOrDefault("migrationRecords")
      let isolationsNode = batch.request.getOrDefault("isolationEvidence")
      echo "lsmr publish ok self=", $svc.switch.peerInfo.peerId,
        " peer=", $peerId,
        " coords=", (if coordsNode.kind == JArray: coordsNode.len else: 0),
        " migrations=", (if migrationsNode.kind == JArray: migrationsNode.len else: 0),
        " isolations=", (if isolationsNode.kind == JArray: isolationsNode.len else: 0)
  ok

proc syncCoordinatesFromPeer*(
    svc: LsmrService, peerId: PeerId
): Future[bool] {.async: (raises: [CancelledError]).} =
  if svc.isNil or svc.switch.isNil:
    return false
  let session = svc.getOrCreateControlSession(peerId)
  try:
    await session.lock.acquire()
    try:
      let conn = await svc.ensureControlConnection(session)
      if conn.isNil:
        if not svc.switch.isConnected(peerId):
          svc.schedulePeerConnect(peerId)
        return false
      defer:
        await svc.closeControlSession(session)
      let ok = await svc.syncCoordinatesOnConn(conn, session, peerId)
      return ok
    finally:
      releaseSessionLock(session)
  except CancelledError as exc:
    raise exc
  except CatchableError as exc:
    debug "lsmr coordinate sync failed", peerId = peerId, err = exc.msg
    false

proc publishLocalStateToPeer*(
    svc: LsmrService, peerId: PeerId
): Future[bool] {.async: (raises: [CancelledError]).} =
  if svc.isNil or svc.switch.isNil:
    return false
  let session = svc.getOrCreateControlSession(peerId)
  try:
    await session.lock.acquire()
    try:
      let conn = await svc.ensureControlConnection(session)
      if conn.isNil:
        if not svc.switch.isConnected(peerId):
          svc.schedulePeerConnect(peerId)
        return false
      defer:
        await svc.closeControlSession(session)
      let ok = await svc.publishLocalStateOnConn(conn, session, peerId)
      return ok
    finally:
      releaseSessionLock(session)
  except CancelledError as exc:
    raise exc
  except CatchableError as exc:
    debug "lsmr coordinate publish failed", peerId = peerId, err = exc.msg
    false

proc runControlSession(svc: LsmrService, peerId: PeerId): Future[void] {.async: (raises: [CancelledError]).} =
  let session = svc.getOrCreateControlSession(peerId)
  when defined(lsmr_diag):
    echo "lsmr control-run-start self=", $svc.switch.peerInfo.peerId,
      " peer=", $peerId,
      " syncPending=", session.syncPending,
      " publishPending=", session.publishPending
  while svc.running and (session.syncPending or session.publishPending):
    when defined(lsmr_diag):
      echo "lsmr control-run-step self=", $svc.switch.peerInfo.peerId,
        " peer=", $peerId,
        " syncPending-before=", session.syncPending,
        " publishPending-before=", session.publishPending
    let doPublish = session.publishPending
    let doSync = session.syncPending
    session.publishPending = false
    session.syncPending = false
    try:
      var syncOk = not doSync
      var publishOk = not doPublish
      if doSync:
        syncOk = await svc.syncCoordinatesFromPeer(peerId)
        if not syncOk:
          session.syncPending = true
      if doPublish and session.syncPending:
        session.publishPending = true
        continue
      if doPublish:
        publishOk = await svc.publishLocalStateToPeer(peerId)
        if not publishOk:
          session.publishPending = true
      if not syncOk or not publishOk:
        when defined(lsmr_diag):
          echo "lsmr control-requeue self=", $svc.switch.peerInfo.peerId,
            " peer=", $peerId,
            " syncOk=", syncOk,
            " publishOk=", publishOk,
            " syncPending=", session.syncPending,
            " publishPending=", session.publishPending
        await svc.closeControlSession(session)
        if session.syncPending or session.publishPending:
          await sleepAsync(0.seconds)
          continue
        break
    except CancelledError as exc:
      raise exc
    except CatchableError as exc:
      if doSync:
        session.syncPending = true
      if doPublish:
        session.publishPending = true
      when defined(lsmr_diag):
        echo "lsmr control-run-fail self=", $svc.switch.peerInfo.peerId,
          " peer=", $peerId,
          " err=", exc.msg,
          " syncPending=", session.syncPending,
          " publishPending=", session.publishPending
      debug "lsmr control session failed", peerId = peerId, err = exc.msg
      await svc.closeControlSession(session)
      if session.syncPending or session.publishPending:
        await sleepAsync(0.seconds)
        continue
      break
  when defined(lsmr_diag):
    echo "lsmr control-run-done self=", $svc.switch.peerInfo.peerId,
      " peer=", $peerId,
      " syncPending=", session.syncPending,
      " publishPending=", session.publishPending,
      " running=", svc.running
  await svc.closeControlSession(session)

proc queueControlSync(svc: LsmrService, peerId: PeerId) {.gcsafe.} =
  if svc.isNil or svc.switch.isNil or peerId.data.len == 0 or
      peerId == svc.switch.peerInfo.peerId:
    return
  if not svc.hasDialableRoute(peerId):
    return
  let session = svc.getOrCreateControlSession(peerId)
  session.syncPending = true
  when defined(lsmr_diag):
    echo "lsmr control-queue self=", $svc.switch.peerInfo.peerId,
      " peer=", $peerId,
      " kind=sync",
      " runnerNil=", session.runner.isNil,
      " runnerFinished=", (if session.runner.isNil: true else: session.runner.finished())
  if session.runner.isNil or session.runner.finished():
    session.runner = svc.runControlSession(peerId)

proc queueControlPublish(svc: LsmrService, peerId: PeerId) {.gcsafe.} =
  if svc.isNil or svc.switch.isNil or peerId.data.len == 0 or
      peerId == svc.switch.peerInfo.peerId:
    return
  if not svc.hasDialableRoute(peerId):
    return
  let session = svc.getOrCreateControlSession(peerId)
  session.publishAllKnown = true
  session.publishPeers.clear()
  session.publishPending = true
  when defined(lsmr_diag):
    echo "lsmr control-queue self=", $svc.switch.peerInfo.peerId,
      " peer=", $peerId,
      " kind=publish",
      " runnerNil=", session.runner.isNil,
      " runnerFinished=", (if session.runner.isNil: true else: session.runner.finished())
  if session.runner.isNil or session.runner.finished():
    session.runner = svc.runControlSession(peerId)

proc queueControlPublishDelta(
    svc: LsmrService, peerId: PeerId, subjectPeerIds: openArray[PeerId]
) {.gcsafe.} =
  if svc.isNil or svc.switch.isNil or peerId.data.len == 0 or
      peerId == svc.switch.peerInfo.peerId:
    return
  if not svc.hasDialableRoute(peerId):
    return
  let session = svc.getOrCreateControlSession(peerId)
  if not session.publishAllKnown:
    for subjectPeerId in subjectPeerIds:
      if subjectPeerId.data.len == 0 or subjectPeerId == peerId:
        continue
      session.publishPeers[$subjectPeerId] = subjectPeerId
  session.publishPending = true
  when defined(lsmr_diag):
    echo "lsmr control-queue self=", $svc.switch.peerInfo.peerId,
      " peer=", $peerId,
      " kind=publish-delta",
      " peers=", session.publishPeers.len,
      " full=", session.publishAllKnown,
      " runnerNil=", session.runner.isNil,
      " runnerFinished=", (if session.runner.isNil: true else: session.runner.finished())
  if session.runner.isNil or session.runner.finished():
    session.runner = svc.runControlSession(peerId)

proc queueCoordinateDelta(
    svc: LsmrService, subjectPeerIds: openArray[PeerId]
) {.gcsafe, raises: [].} =
  if svc.isNil or svc.switch.isNil:
    return
  let selfPeerId =
    if svc.switch.peerInfo.isNil:
      PeerId()
    else:
      svc.switch.peerInfo.peerId
  var uniqueSubjects: seq[PeerId] = @[]
  var seen = initHashSet[string]()
  for peerId in subjectPeerIds:
    let key = $peerId
    if peerId.data.len == 0 or peerId == selfPeerId or key in seen:
      continue
    seen.incl(key)
    uniqueSubjects.add(peerId)
  if uniqueSubjects.len == 0:
    return
  for peerId in svc.knownSyncPeers():
    if peerId.data.len == 0 or peerId == selfPeerId or not svc.hasDialableRoute(peerId):
      continue
    svc.queueControlPublishDelta(peerId, uniqueSubjects)

proc runRefreshQueue(svc: LsmrService): Future[void] {.async: (raises: []).} =
  while svc.running and svc.refreshPending:
    svc.refreshPending = false
    let publishAll = svc.refreshAllPublish
    svc.refreshAllPublish = false
    var publishPeers: seq[PeerId] = @[]
    for _, peerId in svc.refreshPublishPeers.pairs:
      publishPeers.add(peerId)
    svc.refreshPublishPeers.clear()
    try:
      discard await svc.refreshCoordinateRecord()
      if localCoordinateRecord(svc.switch).isSome():
        if publishAll:
          for peerId in svc.knownSyncPeers():
            svc.queueControlSync(peerId)
            svc.queueControlPublish(peerId)
        else:
          for peerId in publishPeers:
            svc.queueControlSync(peerId)
            svc.queueControlPublish(peerId)
        svc.scheduleOverlayMaintenance()
    except CancelledError:
      break
    except CatchableError as exc:
      debug "lsmr queued refresh failed", err = exc.msg
  svc.refreshRunner = nil

proc scheduleImmediateRefresh(
    svc: LsmrService, publishPeer: Option[PeerId] = none(PeerId)
) {.gcsafe, raises: [].} =
  if svc.isNil or not svc.running:
    return
  if publishPeer.isSome():
    let peerId = publishPeer.get()
    if not svc.switch.isNil and peerId.data.len > 0 and peerId != svc.switch.peerInfo.peerId:
      svc.refreshPublishPeers[$peerId] = peerId
  else:
    svc.refreshAllPublish = true
  svc.refreshPending = true
  if svc.refreshRunner.isNil or svc.refreshRunner.finished():
    svc.refreshRunner = svc.runRefreshQueue()
    asyncSpawn svc.refreshRunner

proc requestMigrationParentDigest*(
    svc: LsmrService, peerId: PeerId, targetPrefix: LsmrPath
): Future[Option[string]] {.async: (raises: [CancelledError]).} =
  if svc.isNil or svc.switch.isNil or targetPrefix.len == 0:
    return none(string)
  let session = svc.getOrCreateControlSession(peerId)
  try:
    await session.lock.acquire()
    try:
      let conn = await svc.ensureControlConnection(session)
      if conn.isNil:
        return none(string)
      defer:
        await svc.closeControlSession(session)
      let req = %*{
        "op": "migration_request",
        "targetPrefix": pathToJson(targetPrefix),
      }
      let responseOpt = await sendControlRequest(svc, conn, req, 16 * 1024)
      if responseOpt.isNone():
        return none(string)
      let response = responseOpt.get()
      let okNode =
        if response.kind == JObject:
          response.getOrDefault("ok")
        else:
          newJNull()
      if okNode.kind != JBool or not okNode.getBool():
        return none(string)
      let parentDigestNode =
        if response.kind == JObject:
          response.getOrDefault("parentDigest")
        else:
          newJNull()
      if parentDigestNode.kind != JString:
        return none(string)
      some(parentDigestNode.getStr())
    finally:
      releaseSessionLock(session)
  except CancelledError as exc:
    raise exc
  except CatchableError as exc:
    debug "lsmr migration request failed", peerId = peerId, err = exc.msg
    none(string)

proc broadcastIsolationEvidence*(
    svc: LsmrService, evidence: SignedLsmrIsolationEvidence
): Future[int] {.async: (raises: [CancelledError]).} =
  if svc.isNil or svc.switch.isNil:
    return 0
  let payload = %*{
    "op": "isolation_broadcast",
    "evidence": encodeSignedIsolation(evidence),
  }
  for peerId in svc.switch.connectedPeers(Direction.Out) & svc.switch.connectedPeers(Direction.In):
    try:
      let conn = await svc.switch.dial(peerId, @[svc.config.controlCodec])
      defer:
        await conn.close()
      await conn.writeLp(stringToBytes($payload))
      discard await conn.readLp(8 * 1024)
      inc result
    except CancelledError as exc:
      raise exc
    except CatchableError:
      discard

proc refreshCoordinateRecord*(
    svc: LsmrService
): Future[bool] {.async: (raises: [CancelledError]).} =
  if svc.isNil or svc.switch.isNil:
    return false
  await svc.refreshLock.acquire()
  try:
    let anchors = svc.effectiveAnchors()
    let targetPrefixes = svc.desiredCertifiedPrefixes()
    if targetPrefixes.len == 0:
      return false
    var current = localCoordinateRecord(svc.switch)
    when defined(lsmr_diag):
      let currentPrefix =
        if current.isSome():
          current.get().data.certifiedPrefix()
        else:
          @[]
      echo "lsmr refresh-plan peer=", $svc.switch.peerInfo.peerId,
        " current=", currentPrefix,
        " suffix=", svc.config.localSuffix,
        " serveDepth=", svc.config.serveDepth,
        " targets=", targetPrefixes
    var refreshed = false
    for targetPrefix in targetPrefixes:
      if current.isSome():
        let currentPrefix = current.get().data.certifiedPrefix()
        if currentPrefix == targetPrefix or pathStartsWith(currentPrefix, targetPrefix):
          when defined(lsmr_diag):
            echo "lsmr refresh-skip peer=", $svc.switch.peerInfo.peerId,
              " current=", currentPrefix,
              " target=", targetPrefix
          continue

      var witnesses: seq[SignedLsmrWitness] = @[]
      let issuers = svc.witnessIssuersForPrefix(targetPrefix)
      when defined(lsmr_diag):
        var issuerIds: seq[string] = @[]
        for peerId in issuers:
          issuerIds.add($peerId)
        issuerIds.sort()
        echo "lsmr refresh-issuers peer=", $svc.switch.peerInfo.peerId,
          " target=", targetPrefix,
          " issuers=", issuerIds.join(",")
      for issuerPeer in issuers:
        when defined(lsmr_diag):
          echo "lsmr refresh-witness-request peer=", $svc.switch.peerInfo.peerId,
            " issuer=", $issuerPeer,
            " target=", targetPrefix
        let witnessOpt = await svc.requestWitnessFromPeer(issuerPeer, targetPrefix)
        if witnessOpt.isSome():
          witnesses.add(witnessOpt.get())
          when defined(lsmr_diag):
            echo "lsmr refresh-witness-ok peer=", $svc.switch.peerInfo.peerId,
              " issuer=", $issuerPeer,
              " target=", targetPrefix,
              " collected=", witnesses.len
          let provisional = derivePrefixFromWitnesses(
            svc.switch,
            svc.config,
            anchors,
            witnesses,
            svc.switch.peerInfo.peerId,
            svc.config.networkId,
            targetPrefix,
          )
          if provisional.trust == trusted:
            break

      let derived = derivePrefixFromWitnesses(
        svc.switch,
        svc.config,
        anchors,
        witnesses,
        svc.switch.peerInfo.peerId,
        svc.config.networkId,
        targetPrefix,
      )
      when defined(lsmr_diag):
        echo "lsmr refresh peer=", $svc.switch.peerInfo.peerId,
          " target=", targetPrefix,
          " witnesses=", witnesses.len,
          " trust=", $derived.trust,
          " reason=", derived.reason,
          " prefix=", derived.prefix,
          " coverage=", derived.coverageMask
      if derived.trust != trusted:
        inc svc.witnessQuorumFailure
        return false

      let issuedAt = nowMillis()
      let epochId = svc.config.currentEpoch(issuedAt)
      var migrationDigest =
        computeStaticMigrationDigest(
          svc.switch.peerInfo.peerId,
          svc.config.networkId,
          targetPrefix,
          epochId,
        )
      if current.isSome():
        let previousPrefix = current.get().data.certifiedPrefix()
        if previousPrefix != targetPrefix and not pathStartsWith(targetPrefix, previousPrefix):
          let migration = svc.buildLocalMigrationRecord(
            previousPrefix,
            targetPrefix,
            current.get().data.recordDigest,
            witnesses,
            epochId,
          )
          if migration.isNone():
            inc svc.witnessQuorumFailure
            return false
          discard installMigrationRecord(svc.switch, migration.get())
          migrationDigest = migration.get().data.migrationDigest

      var record = LsmrCoordinateRecord(
        peerId: svc.switch.peerInfo.peerId,
        networkId: svc.config.networkId,
        seqNo: uint64(issuedAt),
        attestedPrefix: targetPrefix,
        localSuffix: svc.config.localSuffix,
        epochId: epochId,
        issuedAtMs: issuedAt,
        expiresAtMs: derived.expiresAtMs,
        confidence: derived.confidence,
        witnesses: witnesses,
        parentDigest: computeExpectedParentDigest(svc.config.networkId, targetPrefix, epochId),
        migrationDigest: migrationDigest,
        coverageMask: derived.coverageMask,
        certifiedDepth: uint8(min(255, targetPrefix.len)),
        recordDigest: "",
        heTuParity: 0'u32,
        version: DefaultLsmrVersion,
      )
      record.heTuParity = computeHeTuParity(
        epochId,
        targetPrefix,
        record.coverageMask,
        record.parentDigest,
        record.migrationDigest,
        DefaultLsmrVersion,
      )
      record.recordDigest = computeRecordDigest(record)
      let signed = SignedLsmrCoordinateRecord.init(svc.switch.peerInfo.privateKey, record).valueOr:
        inc svc.witnessQuorumFailure
        return false
      svc.installLocalCoordinateRecord(signed)
      when defined(lsmr_diag):
        echo "lsmr installed local cert peer=", $svc.switch.peerInfo.peerId,
          " prefix=", record.certifiedPrefix(),
          " epoch=", record.epochId
      current = some(signed)
      inc svc.witnessQuorumSuccess
      refreshed = true

    if not refreshed:
      return current.isSome()

    svc.queueCoordinateDelta([svc.switch.peerInfo.peerId])
    return true
  finally:
    svc.releaseRefreshLock()

proc syncLoop(svc: LsmrService): Future[void] {.async: (raises: [CancelledError]).} =
  while svc.running:
    try:
      discard await svc.refreshCoordinateRecord()
    except CancelledError as exc:
      raise exc
    except CatchableError as exc:
      debug "lsmr sync loop failed", err = exc.msg
    await sleepAsync(500)

method setup*(
    svc: LsmrService, switch: Switch
): Future[bool] {.async: (raises: [CancelledError]).} =
  let hasBeenSetup = await procCall Service(svc).setup(switch)
  if hasBeenSetup:
    svc.switch = switch
    if not switch.peerStore.isNil:
      for anchor in svc.config.anchors:
        if anchor.addrs.len > 0:
          switch.peerStore.setAddresses(anchor.peerId, anchor.addrs)
    if svc.rng.isNil:
      svc.rng = newRng()
    if svc.config.serveWitness and svc.witnessProtocol.isNil:
      svc.witnessProtocol = svc.buildWitnessProtocol()
    if svc.controlProtocol.isNil:
      svc.controlProtocol = svc.buildControlProtocol()
    if svc.config.serveWitness and not svc.witnessProtocol.isNil and not svc.witnessProtocol.started:
      await svc.witnessProtocol.start()
    if not svc.controlProtocol.isNil and not svc.controlProtocol.started:
      await svc.controlProtocol.start()
    if svc.config.serveWitness and not svc.witnessProtocol.isNil and not svc.witnessMounted:
      try:
        switch.mount(svc.witnessProtocol)
        svc.witnessMounted = true
      except CatchableError as exc:
        warn "failed to mount lsmr witness protocol", err = exc.msg
    if not svc.controlProtocol.isNil and not svc.controlMounted:
      try:
        switch.mount(svc.controlProtocol)
        svc.controlMounted = true
      except CatchableError as exc:
        warn "failed to mount lsmr control protocol", err = exc.msg
    svc.connectedHandler = proc(
        peerId: PeerId, event: ConnEvent
    ): Future[void] {.async: (raises: [CancelledError]).} =
      if svc.isNil or svc.switch.isNil:
        return
      if event.kind == ConnEventKind.Connected:
        if localCoordinateRecord(svc.switch).isSome():
          svc.queueControlPublish(peerId)
          svc.queueControlSync(peerId)
          svc.scheduleOverlayMaintenance()
        else:
          scheduleImmediateRefresh(svc, some(peerId))
      when defined(lsmr_diag):
        echo "lsmr connected self=", $svc.switch.peerInfo.peerId,
          " peer=", $peerId,
          " knownCoord=", knowsCoordinatePeer(svc, peerId),
          " localCoord=", localCoordinateRecord(svc.switch).isSome()
    switch.addConnEventHandler(svc.connectedHandler, ConnEventKind.Connected)
    await svc.run(switch)
    for peerId in svc.knownSyncPeers():
      if svc.hasDialableRoute(peerId):
        svc.schedulePeerConnect(peerId)
    if localCoordinateRecord(svc.switch).isSome():
      for peerId in svc.knownSyncPeers():
        svc.schedulePeerConnect(peerId)
    scheduleImmediateRefresh(svc)
  hasBeenSetup

method run*(
    svc: LsmrService, switch: Switch
) {.async: (raises: [CancelledError]).} =
  discard switch
  if svc.running:
    return
  svc.running = true
  svc.runner = nil

method stop*(
    svc: LsmrService, switch: Switch
): Future[bool] {.async: (raises: [CancelledError]).} =
  let hasBeenStopped = await procCall Service(svc).stop(switch)
  if hasBeenStopped:
    svc.running = false
    if not svc.runner.isNil:
      await svc.runner.cancelAndWait()
      svc.runner = nil
    if not svc.refreshRunner.isNil and not svc.refreshRunner.finished():
      await svc.refreshRunner.cancelAndWait()
    svc.refreshRunner = nil
    svc.refreshPending = false
    svc.refreshAllPublish = false
    svc.refreshPublishPeers.clear()
    if not svc.overlayRunner.isNil and not svc.overlayRunner.finished():
      await svc.overlayRunner.cancelAndWait()
    svc.overlayRunner = nil
    svc.overlayPending = false
    if not svc.revalidateRunner.isNil and not svc.revalidateRunner.finished():
      await svc.revalidateRunner.cancelAndWait()
    svc.revalidateRunner = nil
    svc.revalidatePending = false
    if not svc.applyRunner.isNil and not svc.applyRunner.finished():
      await svc.applyRunner.cancelAndWait()
    svc.applyRunner = nil
    svc.applyQueue.clear()
    var connectFlights: seq[Future[bool]] = @[]
    for _, future in svc.connectFlights.pairs:
      if not future.isNil:
        connectFlights.add(future)
    for future in connectFlights:
      if not future.finished():
        await future.cancelAndWait()
    svc.connectFlights.clear()
    var sessions: seq[LsmrControlSession] = @[]
    for _, session in svc.controlSessions.pairs:
      sessions.add(session)
    for session in sessions:
      if not session.runner.isNil and not session.runner.finished():
        await session.runner.cancelAndWait()
      await svc.closeControlSession(session)
    svc.controlSessions.clear()
    if not svc.connectedHandler.isNil and not switch.isNil:
      switch.removeConnEventHandler(svc.connectedHandler, ConnEventKind.Connected)
      svc.connectedHandler = nil
    if not svc.witnessProtocol.isNil and svc.witnessProtocol.started:
      await svc.witnessProtocol.stop()
    if not svc.controlProtocol.isNil and svc.controlProtocol.started:
      await svc.controlProtocol.stop()
  hasBeenStopped

proc startLsmrService*(
    switch: Switch,
    config: LsmrConfig = LsmrConfig.init(),
): Future[LsmrService] {.async: (raises: [CancelledError]).} =
  if switch.isNil:
    return nil
  var svc = getLsmrService(switch)
  if svc.isNil:
    svc = LsmrService.new(config = config)
    switch.services.add(svc)
  if switch.isStarted() and svc.switch.isNil:
    discard await svc.setup(switch)
  svc

proc refreshCoordinateRecord*(
    switch: Switch
): Future[bool] {.async: (raises: [CancelledError]).} =
  let svc = await startLsmrService(switch)
  if svc.isNil:
    return false
  await svc.refreshCoordinateRecord()
