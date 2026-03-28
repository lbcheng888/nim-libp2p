{.push raises: [].}

import std/[algorithm, options, sequtils, strutils]

import chronos
import results

import multiaddress, peerid, protobuf/minprotobuf, signed_envelope

export peerid, signed_envelope

const
  LsmrCoordinateMetadataKey* = "nim-libp2p.lsmr.coordinate_record"
  DefaultLsmrWitnessCodec* = "/nim-libp2p/lsmr-witness/1.0.0"
  DefaultLsmrControlCodec* = "/nim-libp2p/lsmr-control/1.0.0"
  LegacyLsmrVersion* = 1'u32
  DefaultLsmrVersion* = 2'u32

type
  RoutingPlaneMode* {.pure.} = enum
    legacyOnly
    dualStack
    lsmrOnly

  PrimaryRoutingPlane* {.pure.} = enum
    legacy
    lsmr

  RoutingPlaneStatus* = object
    mode*: RoutingPlaneMode
    primary*: PrimaryRoutingPlane
    shadowMode*: bool
    legacyCandidates*: int
    legacyTrusted*: int
    lsmrActiveCertificates*: int
    lsmrIsolations*: int
    lsmrMigrations*: int

  NearFieldBootstrapProvider* {.pure.} = enum
    nfbpNfc
    nfbpBle

  LsmrPath* = seq[uint8]

  ObjectCoordinate* = object
    objectId*: string
    path*: LsmrPath
    depth*: uint8

  TopologyNeighborBucket* = object
    directionDigit*: uint8
    peers*: seq[PeerId]

  TopologyNeighborView* = object
    selfPrefix*: LsmrPath
    certifiedDepth*: uint8
    sameCellPeers*: seq[PeerId]
    parentPrefixPeers*: seq[PeerId]
    directionalPeers*: seq[TopologyNeighborBucket]

  NearFieldHandshakeRecord* = object
    provider*: NearFieldBootstrapProvider
    networkId*: string
    peerId*: PeerId
    addrs*: seq[MultiAddress]
    operatorId*: string
    regionDigit*: uint8
    attestedPrefix*: LsmrPath
    serveDepth*: uint8
    directionMask*: uint32
    canIssueRootCert*: bool
    issuedAtMs*: int64
    expiresAtMs*: int64

  NearFieldObservation* = object
    provider*: NearFieldBootstrapProvider
    networkId*: string
    peerId*: PeerId
    addrs*: seq[MultiAddress]
    operatorId*: string
    regionDigit*: uint8
    attestedPrefix*: LsmrPath
    serveDepth*: uint8
    directionMask*: uint32
    canIssueRootCert*: bool
    rssi*: int32
    observedAtMs*: int64
    ttlMs*: int64

  LsmrAnchor* = object
    peerId*: PeerId
    addrs*: seq[MultiAddress]
    operatorId*: string
    regionDigit*: uint8
    attestedPrefix*: LsmrPath
    serveDepth*: uint8
    directionMask*: uint32
    canIssueRootCert*: bool

  LsmrConfig* = object
    networkId*: string
    anchors*: seq[LsmrAnchor]
    serveWitness*: bool
    operatorId*: string
    regionDigit*: uint8
    localSuffix*: LsmrPath
    serveDepth*: int
    directionMask*: uint32
    witnessCodec*: string
    controlCodec*: string
    recordTtl*: Duration
    minWitnessQuorum*: int
    enableBootstrapBias*: bool
    softIsolation*: bool
    enableNearFieldBootstrap*: bool
    nearFieldProviders*: seq[NearFieldBootstrapProvider]

  LsmrWitness* = object
    subjectPeerId*: PeerId
    anchorPeerId*: PeerId
    networkId*: string
    regionDigit*: uint8
    latencyDigit*: uint8
    rttMs*: uint32
    operatorId*: string
    issuedAtMs*: int64
    expiresAtMs*: int64
    attestedPrefix*: LsmrPath
    prefixDepth*: uint8
    directionMask*: uint32
    epochId*: uint64
    version*: uint32

  SignedLsmrWitness* = SignedPayload[LsmrWitness]

  LsmrCoordinateRecord* = object
    peerId*: PeerId
    networkId*: string
    seqNo*: uint64
    attestedPrefix*: LsmrPath
    localSuffix*: LsmrPath
    epochId*: uint64
    issuedAtMs*: int64
    expiresAtMs*: int64
    confidence*: uint8
    witnesses*: seq[SignedLsmrWitness]
    parentDigest*: string
    migrationDigest*: string
    coverageMask*: uint32
    certifiedDepth*: uint8
    recordDigest*: string
    heTuParity*: uint32
    version*: uint32

  SignedLsmrCoordinateRecord* = SignedPayload[LsmrCoordinateRecord]

  LsmrIsolationReason* {.pure.} = enum
    lirConflictCoordinate
    lirReverseEpoch
    lirInvalidSignature
    lirInvalidParity
    lirInsufficientCoverage
    lirMigrationOutOfBounds
    lirMultiLocation

  LsmrMigrationRecord* = object
    peerId*: PeerId
    networkId*: string
    epochId*: uint64
    fromPrefix*: LsmrPath
    toPrefix*: LsmrPath
    oldRecordDigest*: string
    newParentDigest*: string
    issuedAtMs*: int64
    witnesses*: seq[SignedLsmrWitness]
    migrationDigest*: string
    version*: uint32

  SignedLsmrMigrationRecord* = SignedPayload[LsmrMigrationRecord]

  LsmrIsolationEvidence* = object
    peerId*: PeerId
    networkId*: string
    epochId*: uint64
    reason*: LsmrIsolationReason
    detail*: string
    coordinateDigest*: string
    conflictingDigest*: string
    issuedAtMs*: int64
    witnesses*: seq[SignedLsmrWitness]
    evidenceDigest*: string
    version*: uint32

  SignedLsmrIsolationEvidence* = SignedPayload[LsmrIsolationEvidence]

proc init*(
    _: type LsmrConfig,
    networkId = "default",
    anchors: seq[LsmrAnchor] = @[],
    serveWitness = false,
    operatorId = "",
    regionDigit = 5'u8,
    localSuffix: LsmrPath = @[],
    serveDepth = 2,
    directionMask = 0x1ff'u32,
    witnessCodec = DefaultLsmrWitnessCodec,
    controlCodec = DefaultLsmrControlCodec,
    recordTtl = 30.minutes,
    minWitnessQuorum = 2,
    enableBootstrapBias = true,
    softIsolation = true,
    enableNearFieldBootstrap = false,
    nearFieldProviders: seq[NearFieldBootstrapProvider] = @[],
): LsmrConfig =
  LsmrConfig(
    networkId: if networkId.len == 0: "default" else: networkId,
    anchors: anchors,
    serveWitness: serveWitness,
    operatorId: operatorId,
    regionDigit: regionDigit,
    localSuffix: localSuffix,
    serveDepth: max(1, serveDepth),
    directionMask: directionMask,
    witnessCodec: if witnessCodec.len == 0: DefaultLsmrWitnessCodec else: witnessCodec,
    controlCodec: if controlCodec.len == 0: DefaultLsmrControlCodec else: controlCodec,
    recordTtl: if recordTtl <= ZeroDuration: 30.minutes else: recordTtl,
    minWitnessQuorum: max(1, minWitnessQuorum),
    enableBootstrapBias: enableBootstrapBias,
    softIsolation: softIsolation,
    enableNearFieldBootstrap: enableNearFieldBootstrap,
    nearFieldProviders: nearFieldProviders,
  )

proc parseNearFieldBootstrapProvider*(value: string): Option[NearFieldBootstrapProvider] =
  case value.toLowerAscii().strip()
  of "nfc":
    some(NearFieldBootstrapProvider.nfbpNfc)
  of "ble", "bluetooth":
    some(NearFieldBootstrapProvider.nfbpBle)
  else:
    none(NearFieldBootstrapProvider)

proc label*(provider: NearFieldBootstrapProvider): string =
  case provider
  of NearFieldBootstrapProvider.nfbpNfc:
    "nfc"
  of NearFieldBootstrapProvider.nfbpBle:
    "ble"

proc objectPathForDigest*(digest: openArray[byte], depth = 4): LsmrPath =
  if depth <= 0 or digest.len == 0:
    return @[]
  result = newSeqOfCap[uint8](depth)
  for idx in 0 ..< depth:
    result.add(uint8((int(digest[idx mod digest.len]) mod 9) + 1))

proc objectPathForText*(digestText: string, depth = 4): LsmrPath =
  if digestText.len == 0 or depth <= 0:
    return @[]
  var digest = newSeq[byte](digestText.len)
  for idx, ch in digestText:
    digest[idx] = byte(ch.ord and 0xff)
  objectPathForDigest(digest, depth)

proc objectCoordinate*(
    objectId: string, digest: openArray[byte], depth = 4
): ObjectCoordinate =
  let resolvedDepth = max(0, depth)
  ObjectCoordinate(
    objectId: objectId,
    path: objectPathForDigest(digest, resolvedDepth),
    depth: uint8(min(255, resolvedDepth)),
  )

proc objectCoordinate*(
    objectId, digestText: string, depth = 4
): ObjectCoordinate =
  let resolvedDepth = max(0, depth)
  ObjectCoordinate(
    objectId: objectId,
    path: objectPathForText(digestText, resolvedDepth),
    depth: uint8(min(255, resolvedDepth)),
  )

proc pathStartsWith*(path, prefix: LsmrPath): bool =
  if prefix.len > path.len:
    return false
  for idx in 0 ..< prefix.len:
    if path[idx] != prefix[idx]:
      return false
  true

proc parentPrefix*(path: LsmrPath): LsmrPath =
  if path.len <= 1:
    return @[]
  path[0 ..< path.len - 1]

proc isValidLsmrDigit*(digit: uint8): bool =
  digit in {1'u8, 2'u8, 3'u8, 4'u8, 5'u8, 6'u8, 7'u8, 8'u8, 9'u8}

proc isValidLsmrPath*(path: LsmrPath): bool =
  for digit in path:
    if not isValidLsmrDigit(digit):
      return false
  true

proc luoShuCoord(digit: uint8): tuple[x: int, y: int] =
  case digit
  of 8'u8: (0, 0)
  of 1'u8: (1, 0)
  of 6'u8: (2, 0)
  of 3'u8: (0, 1)
  of 5'u8: (1, 1)
  of 7'u8: (2, 1)
  of 4'u8: (0, 2)
  of 9'u8: (1, 2)
  of 2'u8: (2, 2)
  else:
    (-1, -1)

proc lsmrDigitDistance*(a, b: uint8): int =
  if not isValidLsmrDigit(a) or not isValidLsmrDigit(b):
    return high(int)
  let ac = luoShuCoord(a)
  let bc = luoShuCoord(b)
  abs(ac.x - bc.x) + abs(ac.y - bc.y)

proc lsmrDistance*(localPrefix, remotePrefix: LsmrPath): int =
  if localPrefix.len == 0 or remotePrefix.len == 0:
    return high(int)
  let count = min(localPrefix.len, remotePrefix.len)
  var total = 0
  for i in 0 ..< count:
    let dist = lsmrDigitDistance(localPrefix[i], remotePrefix[i])
    if dist == high(int):
      return high(int)
    total += dist
  total

proc computeHeTuParity*(
    attestedPrefix, localSuffix: LsmrPath, witnessCount: int, version: uint32
): uint32 =
  var parity = 0x48A1'u32 xor version
  parity = parity xor uint32(witnessCount and 0xffff)
  for idx, digit in attestedPrefix:
    parity = (parity shl 5) xor (parity shr 2) xor uint32((idx + 1) * int(digit))
  for idx, digit in localSuffix:
    parity = (parity shl 3) xor (parity shr 1) xor uint32((idx + 11) * int(digit))
  parity

proc computeHeTuParity*(
    epochId: uint64,
    attestedPrefix: LsmrPath,
    coverageMask: uint32,
    parentDigest, migrationDigest: string,
    version: uint32,
): uint32 =
  var parity = 0x6F39'u32 xor version xor uint32(epochId and 0xffff_ffff'u64)
  parity = parity xor uint32(epochId shr 32)
  parity = parity xor coverageMask
  for idx, digit in attestedPrefix:
    parity = (parity shl 5) xor (parity shr 3) xor uint32((idx + 3) * int(digit))
  for idx, ch in parentDigest:
    parity = (parity shl 3) xor (parity shr 1) xor uint32((idx + 17) * int(ch.ord))
  for idx, ch in migrationDigest:
    parity = (parity shl 4) xor (parity shr 2) xor uint32((idx + 29) * int(ch.ord))
  parity

proc certifiedDepthValue*(record: LsmrCoordinateRecord): int =
  if record.version <= LegacyLsmrVersion:
    record.attestedPrefix.len
  elif record.certifiedDepth == 0'u8:
    min(record.attestedPrefix.len, 1)
  else:
    min(record.attestedPrefix.len, int(record.certifiedDepth))

proc certifiedPrefix*(record: LsmrCoordinateRecord): LsmrPath =
  let depth = record.certifiedDepthValue()
  if depth <= 0:
    return @[]
  record.attestedPrefix[0 ..< depth]

proc countCoverageBits*(mask: uint32): int =
  var value = mask
  while value != 0'u32:
    result += int(value and 1'u32)
    value = value shr 1

proc directionBit*(digit: uint8): uint32 =
  if not isValidLsmrDigit(digit):
    return 0'u32
  1'u32 shl uint32(digit - 1'u8)

proc expectedCoverageMask*(path: LsmrPath): uint32 =
  if path.len == 0:
    return 0'u32
  directionBit(path[^1])

proc commonPrefixLen*(a, b: LsmrPath): int =
  let count = min(a.len, b.len)
  while result < count and a[result] == b[result]:
    inc result

proc divergenceDistance*(candidate, target: LsmrPath): int =
  let shared = commonPrefixLen(candidate, target)
  if shared >= candidate.len or shared >= target.len:
    return 0
  lsmrDigitDistance(candidate[shared], target[shared])

proc digestMix(seed: uint64, value: string): uint64 =
  result = seed xor 0x9E37_79B1_85EB_CA87'u64
  for idx, ch in value:
    result = (result shl 7) xor (result shr 3) xor uint64((idx + 1) * ch.ord)

proc digestMix(seed: uint64, path: LsmrPath): uint64 =
  result = seed xor uint64(path.len)
  for idx, digit in path:
    result = (result shl 5) xor (result shr 2) xor uint64((idx + 11) * int(digit))

proc computeRecordDigest*(record: LsmrCoordinateRecord): string =
  var acc = 0x41D2_5F6B_C3E1_9A77'u64
  acc = digestMix(acc, $record.peerId)
  acc = digestMix(acc, record.networkId)
  acc = digestMix(acc, record.attestedPrefix)
  acc = digestMix(acc, record.parentDigest)
  acc = digestMix(acc, record.migrationDigest)
  acc = acc xor record.seqNo xor record.epochId xor uint64(record.issuedAtMs)
  acc = acc xor uint64(record.expiresAtMs) xor uint64(record.coverageMask)
  acc = acc xor uint64(record.confidence) xor uint64(record.certifiedDepth)
  acc.toHex(16)

proc computeMigrationDigest*(record: LsmrMigrationRecord): string =
  var acc = 0x9C56_2D18_4B73_A110'u64
  acc = digestMix(acc, $record.peerId)
  acc = digestMix(acc, record.networkId)
  acc = digestMix(acc, record.fromPrefix)
  acc = digestMix(acc, record.toPrefix)
  acc = digestMix(acc, record.oldRecordDigest)
  acc = digestMix(acc, record.newParentDigest)
  acc = acc xor record.epochId xor uint64(record.issuedAtMs)
  acc.toHex(16)

proc computeRootParentDigest*(
    networkId: string, certifiedPrefix: LsmrPath, epochId: uint64
): string =
  var acc = 0x17E4_22B9_9356_CAF1'u64
  acc = digestMix(acc, networkId)
  acc = digestMix(acc, certifiedPrefix)
  acc = acc xor epochId xor uint64(certifiedPrefix.len)
  acc.toHex(16)

proc computeExpectedParentDigest*(
    networkId: string, certifiedPrefix: LsmrPath, epochId: uint64
): string =
  if certifiedPrefix.len <= 1:
    return computeRootParentDigest(networkId, certifiedPrefix, epochId)
  computeRootParentDigest(networkId, certifiedPrefix.parentPrefix(), epochId)

proc computeStaticMigrationDigest*(
    peerId: PeerId, networkId: string, certifiedPrefix: LsmrPath, epochId: uint64
): string =
  var acc = 0x7A31_EB54_208C_1FD3'u64
  acc = digestMix(acc, $peerId)
  acc = digestMix(acc, networkId)
  acc = digestMix(acc, certifiedPrefix)
  acc = acc xor epochId xor uint64(certifiedPrefix.len)
  acc.toHex(16)

proc computeIsolationDigest*(evidence: LsmrIsolationEvidence): string =
  var acc = 0xA4E7_5BC2_1098_DF31'u64
  acc = digestMix(acc, $evidence.peerId)
  acc = digestMix(acc, evidence.networkId)
  acc = digestMix(acc, evidence.coordinateDigest)
  acc = digestMix(acc, evidence.conflictingDigest)
  acc = digestMix(acc, evidence.detail)
  acc = acc xor evidence.epochId xor uint64(evidence.issuedAtMs)
  acc = acc xor uint64(ord(evidence.reason))
  acc.toHex(16)

proc encodePath(path: LsmrPath): seq[byte] =
  @path

proc decodePath(data: seq[byte]): LsmrPath =
  @data

proc latencyDigitForRtt*(rttMs: uint32): uint8 =
  if rttMs <= 5'u32:
    5'u8
  elif rttMs <= 15'u32:
    1'u8
  elif rttMs <= 30'u32:
    8'u8
  elif rttMs <= 50'u32:
    3'u8
  elif rttMs <= 80'u32:
    6'u8
  elif rttMs <= 120'u32:
    4'u8
  elif rttMs <= 180'u32:
    7'u8
  elif rttMs <= 250'u32:
    9'u8
  else:
    2'u8

proc sortByRtt*(witnesses: var seq[LsmrWitness]) =
  witnesses.sort(proc(a, b: LsmrWitness): int = cmp(a.rttMs, b.rttMs))

proc payloadDomain*(T: typedesc[LsmrWitness]): string =
  "nim-libp2p-lsmr-witness"

proc payloadType*(T: typedesc[LsmrWitness]): seq[byte] =
  @[(byte) 0x08, (byte) 0x01]

proc encode*(witness: LsmrWitness): seq[byte] =
  var pb = initProtoBuffer()
  pb.write(1, witness.subjectPeerId)
  pb.write(2, witness.anchorPeerId)
  pb.write(3, witness.networkId)
  pb.write(4, uint64(witness.regionDigit))
  pb.write(5, uint64(witness.latencyDigit))
  pb.write(6, uint64(witness.rttMs))
  pb.write(7, witness.operatorId)
  pb.write(8, uint64(witness.issuedAtMs))
  pb.write(9, uint64(witness.expiresAtMs))
  if witness.attestedPrefix.len > 0:
    pb.write(10, encodePath(witness.attestedPrefix))
  pb.write(11, uint64(witness.prefixDepth))
  pb.write(12, uint64(witness.directionMask))
  pb.write(13, witness.epochId)
  pb.write(14, uint64(witness.version))
  pb.finish()
  pb.buffer

proc decode*(
    _: typedesc[LsmrWitness], data: seq[byte]
): Result[LsmrWitness, ProtoError] =
  var pb = initProtoBuffer(data)
  var witness = LsmrWitness()
  var attestedPrefix: seq[byte]
  var regionDigit, latencyDigit, rttMs, issuedAtMs, expiresAtMs, prefixDepth,
      directionMask, epochId, version: uint64
  ?pb.getRequiredField(1, witness.subjectPeerId)
  ?pb.getRequiredField(2, witness.anchorPeerId)
  ?pb.getRequiredField(3, witness.networkId)
  ?pb.getRequiredField(4, regionDigit)
  ?pb.getRequiredField(5, latencyDigit)
  ?pb.getRequiredField(6, rttMs)
  ?pb.getRequiredField(7, witness.operatorId)
  ?pb.getRequiredField(8, issuedAtMs)
  ?pb.getRequiredField(9, expiresAtMs)
  discard ?pb.getField(10, attestedPrefix)
  discard ?pb.getField(11, prefixDepth)
  discard ?pb.getField(12, directionMask)
  discard ?pb.getField(13, epochId)
  discard ?pb.getField(14, version)
  witness.regionDigit = uint8(regionDigit)
  witness.latencyDigit = uint8(latencyDigit)
  witness.rttMs = uint32(rttMs)
  witness.issuedAtMs = int64(issuedAtMs)
  witness.expiresAtMs = int64(expiresAtMs)
  witness.attestedPrefix = decodePath(attestedPrefix)
  witness.prefixDepth = uint8(prefixDepth)
  witness.directionMask = uint32(directionMask)
  witness.epochId = epochId
  witness.version =
    if version == 0'u64:
      LegacyLsmrVersion
    else:
      uint32(version)
  ok(witness)

proc checkValid*(witness: SignedLsmrWitness): Result[void, EnvelopeError] =
  if not witness.data.anchorPeerId.match(witness.envelope.publicKey):
    return err(EnvelopeInvalidSignature)
  if witness.data.subjectPeerId.data.len == 0 or witness.data.anchorPeerId.data.len == 0:
    return err(EnvelopeInvalidProtobuf)
  if not isValidLsmrDigit(witness.data.regionDigit) or
      not isValidLsmrDigit(witness.data.latencyDigit):
    return err(EnvelopeInvalidProtobuf)
  if witness.data.operatorId.len == 0:
    return err(EnvelopeInvalidProtobuf)
  if witness.data.attestedPrefix.len > 0:
    if not witness.data.attestedPrefix.isValidLsmrPath():
      return err(EnvelopeInvalidProtobuf)
    if witness.data.prefixDepth != uint8(witness.data.attestedPrefix.len):
      return err(EnvelopeInvalidProtobuf)
  if witness.data.version >= DefaultLsmrVersion and witness.data.prefixDepth == 0'u8:
    return err(EnvelopeInvalidProtobuf)
  ok()

proc payloadDomain*(T: typedesc[LsmrCoordinateRecord]): string =
  "nim-libp2p-lsmr-coordinate-record"

proc payloadType*(T: typedesc[LsmrCoordinateRecord]): seq[byte] =
  @[(byte) 0x08, (byte) 0x02]

proc encode*(record: LsmrCoordinateRecord): seq[byte] =
  var pb = initProtoBuffer()
  pb.write(1, record.peerId)
  pb.write(2, record.networkId)
  pb.write(3, record.seqNo)
  pb.write(4, encodePath(record.attestedPrefix))
  if record.localSuffix.len > 0:
    pb.write(5, encodePath(record.localSuffix))
  pb.write(6, record.epochId)
  pb.write(7, uint64(record.issuedAtMs))
  pb.write(8, uint64(record.expiresAtMs))
  pb.write(9, uint64(record.confidence))
  for witness in record.witnesses:
    let encoded = witness.encode().valueOr:
      continue
    pb.write(10, encoded)
  if record.parentDigest.len > 0:
    pb.write(11, record.parentDigest)
  if record.migrationDigest.len > 0:
    pb.write(12, record.migrationDigest)
  pb.write(13, uint64(record.coverageMask))
  pb.write(14, uint64(record.certifiedDepth))
  if record.recordDigest.len > 0:
    pb.write(15, record.recordDigest)
  pb.write(16, uint64(record.heTuParity))
  pb.write(17, uint64(record.version))
  pb.finish()
  pb.buffer

proc decode*(
    _: typedesc[LsmrCoordinateRecord], data: seq[byte]
): Result[LsmrCoordinateRecord, ProtoError] =
  var pb = initProtoBuffer(data)
  var record = LsmrCoordinateRecord()
  var attestedPrefix, localSuffix: seq[byte]
  var epochId, issuedAtMs, expiresAtMs, confidence, coverageMask, certifiedDepth,
      parity, version: uint64
  var witnessBlobs: seq[seq[byte]]
  ?pb.getRequiredField(1, record.peerId)
  ?pb.getRequiredField(2, record.networkId)
  ?pb.getRequiredField(3, record.seqNo)
  ?pb.getRequiredField(4, attestedPrefix)
  discard ?pb.getField(5, localSuffix)
  discard ?pb.getField(6, epochId)
  discard ?pb.getField(7, issuedAtMs)
  ?pb.getRequiredField(8, expiresAtMs)
  ?pb.getRequiredField(9, confidence)
  discard ?pb.getRepeatedField(10, witnessBlobs)
  discard ?pb.getField(11, record.parentDigest)
  discard ?pb.getField(12, record.migrationDigest)
  discard ?pb.getField(13, coverageMask)
  discard ?pb.getField(14, certifiedDepth)
  discard ?pb.getField(15, record.recordDigest)
  ?pb.getRequiredField(16, parity)
  discard ?pb.getField(17, version)
  record.attestedPrefix = decodePath(attestedPrefix)
  record.localSuffix = decodePath(localSuffix)
  record.epochId = epochId
  record.issuedAtMs = int64(issuedAtMs)
  record.expiresAtMs = int64(expiresAtMs)
  record.confidence = uint8(confidence)
  record.coverageMask = uint32(coverageMask)
  record.certifiedDepth = uint8(certifiedDepth)
  record.heTuParity = uint32(parity)
  record.version =
    if version == 0'u64:
      LegacyLsmrVersion
    else:
      uint32(version)
  for blob in witnessBlobs:
    let witness = SignedLsmrWitness.decode(blob).valueOr:
      return err(ProtoError.IncorrectBlob)
    record.witnesses.add(witness)
  if record.version <= LegacyLsmrVersion and record.certifiedDepth == 0'u8:
    record.certifiedDepth = uint8(record.attestedPrefix.len)
  if record.version >= DefaultLsmrVersion and record.recordDigest.len == 0:
    record.recordDigest = computeRecordDigest(record)
  ok(record)

proc checkValid*(record: SignedLsmrCoordinateRecord): Result[void, EnvelopeError] =
  if not record.data.peerId.match(record.envelope.publicKey):
    return err(EnvelopeInvalidSignature)
  if record.data.networkId.len == 0:
    return err(EnvelopeInvalidProtobuf)
  if record.data.attestedPrefix.len == 0 or not record.data.attestedPrefix.isValidLsmrPath():
    return err(EnvelopeInvalidProtobuf)
  if not record.data.localSuffix.isValidLsmrPath():
    return err(EnvelopeInvalidProtobuf)
  if record.data.version <= LegacyLsmrVersion:
    if record.data.heTuParity != computeHeTuParity(
        record.data.attestedPrefix,
        record.data.localSuffix,
        record.data.witnesses.len,
        record.data.version,
      ):
      return err(EnvelopeInvalidProtobuf)
  else:
    if record.data.certifiedDepthValue() <= 0 or
        record.data.certifiedDepthValue() > record.data.attestedPrefix.len:
      return err(EnvelopeInvalidProtobuf)
    if record.data.parentDigest.len == 0 or record.data.migrationDigest.len == 0:
      return err(EnvelopeInvalidProtobuf)
    if record.data.coverageMask == 0'u32:
      return err(EnvelopeInvalidProtobuf)
    if record.data.coverageMask != expectedCoverageMask(record.data.certifiedPrefix()):
      return err(EnvelopeInvalidProtobuf)
    if record.data.heTuParity != computeHeTuParity(
        record.data.epochId,
        record.data.attestedPrefix,
        record.data.coverageMask,
        record.data.parentDigest,
        record.data.migrationDigest,
        record.data.version,
      ):
      return err(EnvelopeInvalidProtobuf)
    if record.data.recordDigest.len > 0 and
        record.data.recordDigest != computeRecordDigest(record.data):
      return err(EnvelopeInvalidProtobuf)
  ok()

proc payloadDomain*(T: typedesc[LsmrMigrationRecord]): string =
  "nim-libp2p-lsmr-migration-record"

proc payloadType*(T: typedesc[LsmrMigrationRecord]): seq[byte] =
  @[(byte) 0x08, (byte) 0x03]

proc encode*(record: LsmrMigrationRecord): seq[byte] =
  var pb = initProtoBuffer()
  pb.write(1, record.peerId)
  pb.write(2, record.networkId)
  pb.write(3, record.epochId)
  pb.write(4, encodePath(record.fromPrefix))
  pb.write(5, encodePath(record.toPrefix))
  pb.write(6, record.oldRecordDigest)
  pb.write(7, record.newParentDigest)
  pb.write(8, uint64(record.issuedAtMs))
  for witness in record.witnesses:
    let encoded = witness.encode().valueOr:
      continue
    pb.write(9, encoded)
  pb.write(10, record.migrationDigest)
  pb.write(11, uint64(record.version))
  pb.finish()
  pb.buffer

proc decode*(
    _: typedesc[LsmrMigrationRecord], data: seq[byte]
): Result[LsmrMigrationRecord, ProtoError] =
  var pb = initProtoBuffer(data)
  var record = LsmrMigrationRecord()
  var fromPrefix, toPrefix: seq[byte]
  var issuedAtMs, version: uint64
  var witnessBlobs: seq[seq[byte]]
  ?pb.getRequiredField(1, record.peerId)
  ?pb.getRequiredField(2, record.networkId)
  ?pb.getRequiredField(3, record.epochId)
  ?pb.getRequiredField(4, fromPrefix)
  ?pb.getRequiredField(5, toPrefix)
  ?pb.getRequiredField(6, record.oldRecordDigest)
  ?pb.getRequiredField(7, record.newParentDigest)
  ?pb.getRequiredField(8, issuedAtMs)
  discard ?pb.getRepeatedField(9, witnessBlobs)
  ?pb.getRequiredField(10, record.migrationDigest)
  discard ?pb.getField(11, version)
  record.fromPrefix = decodePath(fromPrefix)
  record.toPrefix = decodePath(toPrefix)
  record.issuedAtMs = int64(issuedAtMs)
  record.version =
    if version == 0'u64:
      DefaultLsmrVersion
    else:
      uint32(version)
  for blob in witnessBlobs:
    let witness = SignedLsmrWitness.decode(blob).valueOr:
      return err(ProtoError.IncorrectBlob)
    record.witnesses.add(witness)
  ok(record)

proc checkValid*(record: SignedLsmrMigrationRecord): Result[void, EnvelopeError] =
  if not record.data.peerId.match(record.envelope.publicKey):
    return err(EnvelopeInvalidSignature)
  if record.data.networkId.len == 0 or
      not record.data.fromPrefix.isValidLsmrPath() or
      not record.data.toPrefix.isValidLsmrPath() or
      record.data.fromPrefix.len == 0 or
      record.data.toPrefix.len == 0:
    return err(EnvelopeInvalidProtobuf)
  if record.data.fromPrefix == record.data.toPrefix:
    return err(EnvelopeInvalidProtobuf)
  let shared = commonPrefixLen(record.data.fromPrefix, record.data.toPrefix)
  if shared + 1 < record.data.fromPrefix.len and shared + 1 < record.data.toPrefix.len:
    return err(EnvelopeInvalidProtobuf)
  if record.data.migrationDigest != computeMigrationDigest(record.data):
    return err(EnvelopeInvalidProtobuf)
  ok()

proc payloadDomain*(T: typedesc[LsmrIsolationEvidence]): string =
  "nim-libp2p-lsmr-isolation-evidence"

proc payloadType*(T: typedesc[LsmrIsolationEvidence]): seq[byte] =
  @[(byte) 0x08, (byte) 0x04]

proc encode*(evidence: LsmrIsolationEvidence): seq[byte] =
  var pb = initProtoBuffer()
  pb.write(1, evidence.peerId)
  pb.write(2, evidence.networkId)
  pb.write(3, evidence.epochId)
  pb.write(4, uint64(ord(evidence.reason)))
  pb.write(5, evidence.detail)
  pb.write(6, evidence.coordinateDigest)
  pb.write(7, evidence.conflictingDigest)
  pb.write(8, uint64(evidence.issuedAtMs))
  for witness in evidence.witnesses:
    let encoded = witness.encode().valueOr:
      continue
    pb.write(9, encoded)
  pb.write(10, evidence.evidenceDigest)
  pb.write(11, uint64(evidence.version))
  pb.finish()
  pb.buffer

proc decode*(
    _: typedesc[LsmrIsolationEvidence], data: seq[byte]
): Result[LsmrIsolationEvidence, ProtoError] =
  var pb = initProtoBuffer(data)
  var evidence = LsmrIsolationEvidence()
  var reason, issuedAtMs, version: uint64
  var witnessBlobs: seq[seq[byte]]
  ?pb.getRequiredField(1, evidence.peerId)
  ?pb.getRequiredField(2, evidence.networkId)
  ?pb.getRequiredField(3, evidence.epochId)
  ?pb.getRequiredField(4, reason)
  ?pb.getRequiredField(5, evidence.detail)
  ?pb.getRequiredField(6, evidence.coordinateDigest)
  discard ?pb.getField(7, evidence.conflictingDigest)
  ?pb.getRequiredField(8, issuedAtMs)
  discard ?pb.getRepeatedField(9, witnessBlobs)
  ?pb.getRequiredField(10, evidence.evidenceDigest)
  discard ?pb.getField(11, version)
  evidence.reason = LsmrIsolationReason(reason)
  evidence.issuedAtMs = int64(issuedAtMs)
  evidence.version =
    if version == 0'u64:
      DefaultLsmrVersion
    else:
      uint32(version)
  for blob in witnessBlobs:
    let witness = SignedLsmrWitness.decode(blob).valueOr:
      return err(ProtoError.IncorrectBlob)
    evidence.witnesses.add(witness)
  ok(evidence)

proc checkValid*(evidence: SignedLsmrIsolationEvidence): Result[void, EnvelopeError] =
  if evidence.data.networkId.len == 0 or evidence.data.coordinateDigest.len == 0:
    return err(EnvelopeInvalidProtobuf)
  if evidence.data.witnesses.len == 0:
    return err(EnvelopeInvalidProtobuf)
  for witness in evidence.data.witnesses:
    if witness.checkValid().isErr:
      return err(EnvelopeInvalidProtobuf)
  if evidence.data.evidenceDigest != computeIsolationDigest(evidence.data):
    return err(EnvelopeInvalidProtobuf)
  ok()
