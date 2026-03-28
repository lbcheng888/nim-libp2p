{.push raises: [].}

import std/[algorithm, base64, json, options, sequtils, sets, strutils, tables]
from std/times import epochTime

import bearssl/rand
import chronos, chronicles
import results
import stew/byteutils

import ../lsmr
import ../peerid
import ../peerinfo
import ../peerstore
import ../protocols/protocol
import ../stream/connection
import ../switch

logScope:
  topics = "libp2p lsmr"

type
  LsmrValidationTrust* {.pure.} = enum
    rejected
    quarantine
    trusted

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

proc nowMillis(): int64 =
  int64(epochTime() * 1000)

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

proc decodeSignedRecord(encoded: string): Option[SignedLsmrCoordinateRecord] =
  if encoded.len == 0:
    return none(SignedLsmrCoordinateRecord)
  try:
    let record = SignedLsmrCoordinateRecord.decode(base64.decode(encoded).toBytes()).valueOr:
      return none(SignedLsmrCoordinateRecord)
    some(record)
  except CatchableError:
    none(SignedLsmrCoordinateRecord)

proc decodeSignedMigration(encoded: string): Option[SignedLsmrMigrationRecord] =
  if encoded.len == 0:
    return none(SignedLsmrMigrationRecord)
  try:
    let record = SignedLsmrMigrationRecord.decode(base64.decode(encoded).toBytes()).valueOr:
      return none(SignedLsmrMigrationRecord)
    some(record)
  except CatchableError:
    none(SignedLsmrMigrationRecord)

proc decodeSignedIsolation(encoded: string): Option[SignedLsmrIsolationEvidence] =
  if encoded.len == 0:
    return none(SignedLsmrIsolationEvidence)
  try:
    let evidence = SignedLsmrIsolationEvidence.decode(base64.decode(encoded).toBytes()).valueOr:
      return none(SignedLsmrIsolationEvidence)
    some(evidence)
  except CatchableError:
    none(SignedLsmrIsolationEvidence)

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

proc effectiveAnchors(svc: LsmrService, nowMs = 0'i64): seq[LsmrAnchor] =
  if svc.isNil:
    return @[]
  let currentMs = if nowMs > 0: nowMs else: nowMillis()
  var merged = initTable[string, LsmrAnchor]()
  for anchor in svc.config.anchors:
    merged[$anchor.peerId] = anchor
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
  for _, anchor in merged.pairs:
    result.add(anchor)

proc findEffectiveAnchor(svc: LsmrService, peerId: PeerId): Option[LsmrAnchor] =
  for anchor in svc.effectiveAnchors():
    if anchor.peerId == peerId:
      return some(anchor)
  none(LsmrAnchor)

proc currentEpoch(config: LsmrConfig, nowMs = 0'i64): uint64 =
  let current = if nowMs > 0: nowMs else: nowMillis()
  let ttlMs = max(1'i64, durationToMillis(config.recordTtl))
  uint64(current div ttlMs)

proc rootPrefix(anchor: LsmrAnchor): LsmrPath =
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

proc installMigrationRecord*(
    switch: Switch, record: SignedLsmrMigrationRecord
): bool {.gcsafe.}

proc installIsolationEvidence*(
    switch: Switch, evidence: SignedLsmrIsolationEvidence
): bool {.gcsafe.}

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
    discard switch.peerStore[ActiveLsmrBook].del(peerId)
    return false
  let incoming = switch.peerStore[LsmrBook][peerId]
  let snapshot = validateLsmrPeer(switch, config, peerId)
  if snapshot.trust != trusted:
    discard switch.peerStore[ActiveLsmrBook].del(peerId)
    return false
  if switch.peerStore[ActiveLsmrBook].contains(peerId):
    let active = switch.peerStore[ActiveLsmrBook][peerId]
    let activePrefix = active.data.certifiedPrefix()
    let incomingPrefix = incoming.data.certifiedPrefix()
    if incoming.data.epochId < active.data.epochId or
        (incoming.data.epochId == active.data.epochId and incomingPrefix != activePrefix):
      let reason =
        if incoming.data.epochId < active.data.epochId:
          LsmrIsolationReason.lirReverseEpoch
        else:
          LsmrIsolationReason.lirMultiLocation
      let evidence = LsmrIsolationEvidence(
        peerId: peerId,
        networkId: incoming.data.networkId,
        epochId: incoming.data.epochId,
        reason: reason,
        detail: if reason == LsmrIsolationReason.lirReverseEpoch: "reverse_epoch" else: "multi_location",
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
        discard switch.peerStore[ActiveLsmrBook].del(peerId)
        return false
      discard installIsolationEvidence(switch, signed)
      return false
  switch.peerStore[ActiveLsmrBook][peerId] = incoming
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
  for peerId, record in svc.switch.peerStore[ActiveLsmrBook].book.pairs:
    let prefix = record.data.certifiedPrefix()
    if prefix.len > 0 and pathStartsWith(prefix, parent):
      result.add(peerId)

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
    nearFieldRecords: initTable[PeerId, NearFieldHandshakeRecord](),
  )

proc buildWitnessProtocol(svc: LsmrService): LPProtocol =
  proc handleWitness(conn: Connection, proto: string) {.async: (raises: [CancelledError]).} =
    try:
      let payload = await conn.readLp(8 * 1024)
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
      if not svc.canWitnessPrefix(targetPrefix):
        return

      var challenge = newSeq[byte](16)
      hmacDrbgGenerate(svc.rng[], challenge)
      let startedAt = Moment.now()
      await conn.writeLp(challenge)
      let echoed = await conn.readLp(128)
      if echoed != challenge:
        return
      let rttMs = uint32(max(1'i64, durationToMillis(Moment.now() - startedAt)))
      let signed = svc.buildLocalWitness(conn.peerId, targetPrefix, rttMs).valueOr:
        return
      let encoded = signed.encode().valueOr:
        return
      let response = %*{
        "ok": true,
        "witness": base64.encode(bytesToString(encoded)),
      }
      await conn.writeLp(stringToBytes($response))
    except CancelledError as exc:
      raise exc
    except CatchableError as exc:
      debug "lsmr witness handler failed", err = exc.msg
    finally:
      await conn.close()

  LPProtocol.new(@[svc.config.witnessCodec], handleWitness)

proc buildControlProtocol(svc: LsmrService): LPProtocol =
  proc handleControl(conn: Connection, proto: string) {.async: (raises: [CancelledError]).} =
    try:
      let payload = await conn.readLp(256 * 1024)
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
        let coordinateRecords = newJArray()
        let migrationRecords = newJArray()
        let isolationEvidence = newJArray()
        var response = %*{
          "ok": true,
          "coordinateRecords": coordinateRecords,
          "migrationRecords": migrationRecords,
          "isolationEvidence": isolationEvidence,
        }
        if not svc.switch.isNil and not svc.switch.peerStore.isNil:
          for _, record in svc.switch.peerStore[ActiveLsmrBook].book.pairs:
            let encoded = encodeSignedRecord(record)
            if encoded.len > 0:
              coordinateRecords.add(%encoded)
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
        await conn.writeLp(stringToBytes($response))
      of "coordinate_publish":
        if request.hasKey("coordinateRecord"):
          decodeSignedRecord(request["coordinateRecord"].getStr()).withValue(record):
            if not svc.switch.peerStore.isNil:
              svc.switch.peerStore[LsmrBook][record.data.peerId] = record
              discard promoteValidatedCoordinateRecord(svc.switch, svc.config, record.data.peerId)
        if request.hasKey("migrationRecords") and request["migrationRecords"].kind == JArray:
          for item in request["migrationRecords"].items():
            if item.kind == JString:
              decodeSignedMigration(item.getStr()).withValue(record):
                discard installMigrationRecord(svc.switch, record)
        if request.hasKey("isolationEvidence") and request["isolationEvidence"].kind == JArray:
          for item in request["isolationEvidence"].items():
            if item.kind == JString:
              decodeSignedIsolation(item.getStr()).withValue(evidence):
                discard installIsolationEvidence(svc.switch, evidence)
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
        discard
    except CancelledError as exc:
      raise exc
    except CatchableError as exc:
      debug "lsmr control handler failed", err = exc.msg
    finally:
      await conn.close()

  LPProtocol.new(@[svc.config.controlCodec], handleControl)

proc requestWitnessFromPeer*(
    svc: LsmrService, peerId: PeerId, targetPrefix: LsmrPath
): Future[Option[SignedLsmrWitness]] {.async: (raises: [CancelledError]).} =
  if svc.isNil or svc.switch.isNil or targetPrefix.len == 0:
    return none(SignedLsmrWitness)
  inc svc.witnessRequests
  if peerId == svc.switch.peerInfo.peerId:
    let localWitness = svc.buildLocalWitness(peerId, targetPrefix)
    if localWitness.isSome():
      inc svc.witnessSuccess
    return localWitness
  try:
    var conn: Connection = nil
    try:
      conn = await svc.switch.dial(peerId, @[svc.config.witnessCodec])
    except CancelledError as exc:
      raise exc
    except CatchableError as exc:
      let anchorOpt = svc.findEffectiveAnchor(peerId)
      if anchorOpt.isNone() or anchorOpt.get().addrs.len == 0:
        raise exc
      conn = await svc.switch.dial(
        peerId,
        anchorOpt.get().addrs,
        @[svc.config.witnessCodec],
        forceDial = true,
        reuseConnection = true,
      )
    defer:
      await conn.close()
    let req = %*{
      "op": "witness",
      "networkId": svc.config.networkId,
      "targetPrefix": pathToJson(targetPrefix),
    }
    await conn.writeLp(stringToBytes($req))
    let challenge = await conn.readLp(128)
    if challenge.len == 0:
      return none(SignedLsmrWitness)
    await conn.writeLp(challenge)
    let payload = await conn.readLp(16 * 1024)
    let response = parseJson(bytesToString(payload))
    if response.kind != JObject or not response.hasKey("witness"):
      return none(SignedLsmrWitness)
    let decoded = stringToBytes(base64.decode(response["witness"].getStr()))
    let witness = SignedLsmrWitness.decode(decoded).valueOr:
      return none(SignedLsmrWitness)
    inc svc.witnessSuccess
    some(witness)
  except CancelledError as exc:
    raise exc
  except CatchableError as exc:
    debug "lsmr witness request failed", peerId = peerId, err = exc.msg
    none(SignedLsmrWitness)

proc installLocalCoordinateRecord(
    svc: LsmrService, record: SignedLsmrCoordinateRecord
) =
  let encoded = record.encode().valueOr:
    return
  svc.switch.peerInfo.metadata[LsmrCoordinateMetadataKey] = encoded
  if not svc.switch.peerStore.isNil:
    svc.switch.peerStore[LsmrBook][svc.switch.peerInfo.peerId] = record
    svc.switch.peerStore[ActiveLsmrBook][svc.switch.peerInfo.peerId] = record

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
  discard switch.peerStore[ActiveLsmrBook].del(evidence.data.peerId)
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

proc syncCoordinatesFromPeer*(
    svc: LsmrService, peerId: PeerId
): Future[bool] {.async: (raises: [CancelledError]).} =
  if svc.isNil or svc.switch.isNil:
    return false
  try:
    let conn = await svc.switch.dial(peerId, @[svc.config.controlCodec])
    defer:
      await conn.close()
    await conn.writeLp(stringToBytes($(%*{"op": "coordinate_sync"})))
    let payload = await conn.readLp(512 * 1024)
    let response = parseJson(bytesToString(payload))
    if response.kind != JObject or not response.hasKey("ok") or not response["ok"].getBool():
      return false
    if response.hasKey("coordinateRecords") and response["coordinateRecords"].kind == JArray:
      for item in response["coordinateRecords"].items():
        if item.kind == JString:
          decodeSignedRecord(item.getStr()).withValue(record):
            if not svc.switch.peerStore.isNil:
              svc.switch.peerStore[LsmrBook][record.data.peerId] = record
              discard promoteValidatedCoordinateRecord(svc.switch, svc.config, record.data.peerId)
    if response.hasKey("migrationRecords") and response["migrationRecords"].kind == JArray:
      for item in response["migrationRecords"].items():
        if item.kind == JString:
          decodeSignedMigration(item.getStr()).withValue(record):
            discard installMigrationRecord(svc.switch, record)
    if response.hasKey("isolationEvidence") and response["isolationEvidence"].kind == JArray:
      for item in response["isolationEvidence"].items():
        if item.kind == JString:
          decodeSignedIsolation(item.getStr()).withValue(evidence):
            discard installIsolationEvidence(svc.switch, evidence)
    true
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
  let local = localCoordinateRecord(svc.switch)
  if local.isNone():
    return false
  let migrationRecords = newJArray()
  let isolationEvidence = newJArray()
  var req = %*{
    "op": "coordinate_publish",
    "coordinateRecord": encodeSignedRecord(local.get()),
    "migrationRecords": migrationRecords,
    "isolationEvidence": isolationEvidence,
  }
  if not svc.switch.peerStore.isNil:
    for record in svc.switch.peerStore[LsmrMigrationBook][svc.switch.peerInfo.peerId]:
      let encoded = encodeSignedMigration(record)
      if encoded.len > 0:
        migrationRecords.add(%encoded)
    for evidence in svc.switch.peerStore[LsmrIsolationBook][svc.switch.peerInfo.peerId]:
      let encoded = encodeSignedIsolation(evidence)
      if encoded.len > 0:
        isolationEvidence.add(%encoded)
  try:
    let conn = await svc.switch.dial(peerId, @[svc.config.controlCodec])
    defer:
      await conn.close()
    await conn.writeLp(stringToBytes($req))
    discard await conn.readLp(8 * 1024)
    true
  except CancelledError as exc:
    raise exc
  except CatchableError as exc:
    debug "lsmr coordinate publish failed", peerId = peerId, err = exc.msg
    false

proc requestMigrationParentDigest*(
    svc: LsmrService, peerId: PeerId, targetPrefix: LsmrPath
): Future[Option[string]] {.async: (raises: [CancelledError]).} =
  if svc.isNil or svc.switch.isNil or targetPrefix.len == 0:
    return none(string)
  try:
    let conn = await svc.switch.dial(peerId, @[svc.config.controlCodec])
    defer:
      await conn.close()
    let req = %*{
      "op": "migration_request",
      "targetPrefix": pathToJson(targetPrefix),
    }
    await conn.writeLp(stringToBytes($req))
    let payload = await conn.readLp(16 * 1024)
    let response = parseJson(bytesToString(payload))
    if response.kind != JObject or not response.hasKey("ok") or not response["ok"].getBool():
      return none(string)
    if not response.hasKey("parentDigest"):
      return none(string)
    some(response["parentDigest"].getStr())
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
  let anchors = svc.effectiveAnchors()
  let targetPrefixes = svc.config.desiredCertifiedPrefixes()
  if targetPrefixes.len == 0:
    return false
  var current = localCoordinateRecord(svc.switch)
  var refreshed = false
  for targetPrefix in targetPrefixes:
    if current.isSome() and current.get().data.certifiedPrefix() == targetPrefix:
      continue

    var witnesses: seq[SignedLsmrWitness] = @[]
    for issuerPeer in svc.witnessIssuersForPrefix(targetPrefix):
      let witnessOpt = await svc.requestWitnessFromPeer(issuerPeer, targetPrefix)
      if witnessOpt.isSome():
        witnesses.add(witnessOpt.get())

    let derived = derivePrefixFromWitnesses(
      svc.switch,
      svc.config,
      anchors,
      witnesses,
      svc.switch.peerInfo.peerId,
      svc.config.networkId,
      targetPrefix,
    )
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
    current = some(signed)
    inc svc.witnessQuorumSuccess
    refreshed = true

  if not refreshed:
    return current.isSome()

  for peerId in svc.switch.connectedPeers(Direction.Out) & svc.switch.connectedPeers(Direction.In):
    discard await svc.publishLocalStateToPeer(peerId)
  true

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
      discard await svc.syncCoordinatesFromPeer(peerId)
      discard await svc.publishLocalStateToPeer(peerId)
      if svc.witnessIssuersForPrefix(@[svc.config.regionDigit]).anyIt(it == peerId):
        discard await svc.refreshCoordinateRecord()
    switch.addConnEventHandler(svc.connectedHandler, ConnEventKind.Connected)
  hasBeenSetup

method stop*(
    svc: LsmrService, switch: Switch
): Future[bool] {.async: (raises: [CancelledError]).} =
  let hasBeenStopped = await procCall Service(svc).stop(switch)
  if hasBeenStopped:
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
