{.used.}

import std/tables
from std/times import epochTime

import ../libp2p/[crypto/crypto, lsmr, peerstore, switch]

proc nowMillis*(): int64 =
  int64(epochTime() * 1000)

proc makeSignedWitness*(
    anchorKey: PrivateKey,
    subjectPeerId: PeerId,
    networkId: string,
    operatorId: string,
    regionDigit: uint8,
    rttMs: uint32,
    attestedPrefix: LsmrPath = @[],
): SignedLsmrWitness =
  let anchorPeerId = PeerId.init(anchorKey).tryGet()
  let targetPrefix =
    if attestedPrefix.len > 0:
      attestedPrefix
    else:
      @[regionDigit, latencyDigitForRtt(rttMs)]
  let witness = LsmrWitness(
    subjectPeerId: subjectPeerId,
    anchorPeerId: anchorPeerId,
    networkId: networkId,
    regionDigit: regionDigit,
    latencyDigit: targetPrefix[^1],
    rttMs: rttMs,
    operatorId: operatorId,
    issuedAtMs: nowMillis(),
    expiresAtMs: nowMillis() + 30 * 60 * 1000,
    attestedPrefix: targetPrefix,
    prefixDepth: uint8(targetPrefix.len),
    directionMask: 0x1ff'u32,
    epochId: 1'u64,
    version: DefaultLsmrVersion,
  )
  SignedLsmrWitness.init(anchorKey, witness).tryGet()

proc makeSignedCoordinateRecord*(
    peerKey: PrivateKey,
    peerId: PeerId,
    networkId: string,
    attestedPrefix: LsmrPath,
    witnesses: seq[SignedLsmrWitness],
    confidence = 80'u8,
): SignedLsmrCoordinateRecord =
  var record = LsmrCoordinateRecord(
    peerId: peerId,
    networkId: networkId,
    seqNo: uint64(nowMillis()),
    attestedPrefix: attestedPrefix,
    localSuffix: @[],
    epochId: 1'u64,
    issuedAtMs: nowMillis(),
    expiresAtMs: nowMillis() + 30 * 60 * 1000,
    confidence: confidence,
    witnesses: witnesses,
    parentDigest: computeExpectedParentDigest(networkId, attestedPrefix, 1'u64),
    migrationDigest: computeStaticMigrationDigest(peerId, networkId, attestedPrefix, 1'u64),
    coverageMask: expectedCoverageMask(attestedPrefix),
    certifiedDepth: uint8(attestedPrefix.len),
    recordDigest: "",
    heTuParity:
      computeHeTuParity(
        1'u64,
        attestedPrefix,
        expectedCoverageMask(attestedPrefix),
        computeExpectedParentDigest(networkId, attestedPrefix, 1'u64),
        computeStaticMigrationDigest(peerId, networkId, attestedPrefix, 1'u64),
        DefaultLsmrVersion,
      ),
    version: DefaultLsmrVersion,
  )
  record.recordDigest = computeRecordDigest(record)
  SignedLsmrCoordinateRecord.init(peerKey, record).tryGet()

proc makeSignedMigrationRecord*(
    peerKey: PrivateKey,
    peerId: PeerId,
    networkId: string,
    fromPrefix, toPrefix: LsmrPath,
    oldRecordDigest: string,
    witnesses: seq[SignedLsmrWitness],
    epochId = 1'u64,
): SignedLsmrMigrationRecord =
  var record = LsmrMigrationRecord(
    peerId: peerId,
    networkId: networkId,
    epochId: epochId,
    fromPrefix: fromPrefix,
    toPrefix: toPrefix,
    oldRecordDigest: oldRecordDigest,
    newParentDigest: computeExpectedParentDigest(networkId, toPrefix, epochId),
    issuedAtMs: nowMillis(),
    witnesses: witnesses,
    migrationDigest: "",
    version: DefaultLsmrVersion,
  )
  record.migrationDigest = computeMigrationDigest(record)
  SignedLsmrMigrationRecord.init(peerKey, record).tryGet()

proc makeSignedIsolationEvidence*(
    signerKey: PrivateKey,
    subjectPeerId: PeerId,
    networkId: string,
    coordinateDigest, conflictingDigest: string,
    witnesses: seq[SignedLsmrWitness],
    epochId = 1'u64,
): SignedLsmrIsolationEvidence =
  var evidence = LsmrIsolationEvidence(
    peerId: subjectPeerId,
    networkId: networkId,
    epochId: epochId,
    reason: LsmrIsolationReason.lirMultiLocation,
    detail: "test_evidence",
    coordinateDigest: coordinateDigest,
    conflictingDigest: conflictingDigest,
    issuedAtMs: nowMillis(),
    witnesses: witnesses,
    evidenceDigest: "",
    version: DefaultLsmrVersion,
  )
  evidence.evidenceDigest = computeIsolationDigest(evidence)
  SignedLsmrIsolationEvidence.init(signerKey, evidence).tryGet()

proc installCoordinateChainState*(
    sw: Switch,
    records: seq[SignedLsmrCoordinateRecord],
    writeLocalMetadata = false,
) =
  if records.len == 0:
    return
  let tip = records[^1]
  if writeLocalMetadata:
    sw.peerInfo.metadata[LsmrCoordinateMetadataKey] = tip.encode().tryGet()
  sw.peerStore[LsmrChainBook][tip.data.peerId] = records
  sw.peerStore[LsmrBook][tip.data.peerId] = tip
  sw.peerStore[ActiveLsmrBook][tip.data.peerId] = tip
