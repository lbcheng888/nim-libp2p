{.push raises: [].}

import std/[algorithm, options, sequtils, strutils, tables]

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

  LsmrBagua* {.pure.} = enum
    lbgQian
    lbgDui
    lbgLi
    lbgZhen
    lbgXun
    lbgKan
    lbgGen
    lbgKun

  LsmrStorageTier* {.pure.} = enum
    lstEdge
    lstFog
    lstGlobal

  LsmrBaguaKey* = object
    topologyPath*: LsmrPath
    resolution*: uint8
    bagua*: LsmrBagua
    contentLsh*: string

  LsmrBaguaCursor* = object
    topologyPrefix*: LsmrPath
    resolution*: Option[uint8]
    bagua*: Option[LsmrBagua]
    contentPrefix*: string

  LsmrBaguaLeaf* = object
    key*: LsmrBaguaKey
    payload*: seq[byte]
    version*: uint64
    digest*: string

  LsmrBaguaDiffKind* {.pure.} = enum
    lbdMissingLocal
    lbdMissingRemote
    lbdPayloadMismatch

  LsmrBaguaDiff* = object
    key*: LsmrBaguaKey
    kind*: LsmrBaguaDiffKind
    localDigest*: string
    remoteDigest*: string

  LsmrBaguaPrefixNode = ref object
    label: string
    digest: string
    leafCount: int
    hasLeaf: bool
    leaf: LsmrBaguaLeaf
    children: Table[string, LsmrBaguaPrefixNode]

  LsmrBaguaPrefixTree* = object
    root: LsmrBaguaPrefixNode

  ObjectCoordinate* = object
    objectId*: string
    path*: LsmrPath
    depth*: uint8
    contentLsh*: string

  LsmrDaYanMode* {.pure.} = enum
    ldymZhen
    ldymXun

  LsmrDaYanStage* {.pure.} = enum
    ldysKunLocal
    ldysDuiRegional
    ldysLiGlobal

  LsmrDaYanPayloadKind* {.pure.} = enum
    ldypkCsgDelta
    ldypkSemanticFingerprint

  LsmrDaYanFilterResult* {.pure.} = enum
    ldyfAccept
    ldyfBlockMode
    ldyfBlockResolution
    ldyfBlockTopology
    ldyfBlockSemantic
    ldyfBlockGuard

  LsmrDaYanTopic* = object
    center*: LsmrPath
    resolution*: uint8
    semanticFingerprint*: string
    guardDigest*: string
    mode*: LsmrDaYanMode

  LsmrDaYanSubscription* = object
    topologyPrefix*: LsmrPath
    minResolution*: uint8
    maxResolution*: uint8
    semanticPrefix*: string
    guardPrefix*: string
    acceptUrgent*: bool
    acceptBackground*: bool

  LsmrDaYanPeer* = object
    peerId*: PeerId
    prefix*: LsmrPath
    subscriptions*: seq[LsmrDaYanSubscription]

  LsmrDaYanFlowConfig* = object
    regionalFanout*: int
    globalFanout*: int
    minGlobalResolution*: uint8

  LsmrDaYanRoute* = object
    peerId*: PeerId
    stage*: LsmrDaYanStage
    mode*: LsmrDaYanMode
    payloadKind*: LsmrDaYanPayloadKind
    resolution*: uint8
    topologyPrefix*: LsmrPath
    distance*: int
    semanticFingerprint*: string

  LsmrDaYanMessage* = object
    messageId*: string
    originPeerId*: PeerId
    topic*: LsmrDaYanTopic
    stage*: LsmrDaYanStage
    mode*: LsmrDaYanMode
    payloadKind*: LsmrDaYanPayloadKind
    resolution*: uint8
    payload*: seq[byte]
    semanticFingerprint*: string

  LsmrDaYanEnvelope* = object
    messageId*: string
    originPeerId*: PeerId
    topic*: LsmrDaYanTopic
    stage*: LsmrDaYanStage
    mode*: LsmrDaYanMode
    payloadKind*: LsmrDaYanPayloadKind
    resolution*: uint8
    payload*: seq[byte]
    semanticFingerprint*: string

  LsmrDaYanHandler* = proc(message: LsmrDaYanMessage): Future[void] {.
    gcsafe, raises: [], closure
  .}

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

proc contentLshText*(digest: openArray[byte]): string =
  result = newStringOfCap(digest.len * 2)
  for b in digest:
    result.add(int(b).toHex(2))

proc storageBaguas*(tier: LsmrStorageTier): seq[LsmrBagua] =
  case tier
  of LsmrStorageTier.lstEdge:
    @[LsmrBagua.lbgZhen, LsmrBagua.lbgXun]
  of LsmrStorageTier.lstFog:
    @[LsmrBagua.lbgKan, LsmrBagua.lbgLi]
  of LsmrStorageTier.lstGlobal:
    @[LsmrBagua.lbgKun, LsmrBagua.lbgGen]

proc objectCoordinate*(
    objectId: string, digest: openArray[byte], depth = 4
): ObjectCoordinate =
  let resolvedDepth = max(0, depth)
  ObjectCoordinate(
    objectId: objectId,
    path: objectPathForDigest(digest, resolvedDepth),
    depth: uint8(min(255, resolvedDepth)),
    contentLsh: digest.contentLshText(),
  )

proc objectCoordinate*(
    objectId, digestText: string, depth = 4
): ObjectCoordinate =
  let resolvedDepth = max(0, depth)
  ObjectCoordinate(
    objectId: objectId,
    path: objectPathForText(digestText, resolvedDepth),
    depth: uint8(min(255, resolvedDepth)),
    contentLsh: digestText,
  )

proc init*(
    _: type LsmrDaYanTopic,
    center: LsmrPath,
    resolution: uint8,
    semanticFingerprint: string,
    guardDigest = "",
    mode = LsmrDaYanMode.ldymXun,
): LsmrDaYanTopic =
  LsmrDaYanTopic(
    center: center,
    resolution: resolution,
    semanticFingerprint: semanticFingerprint,
    guardDigest: guardDigest,
    mode: mode,
  )

proc init*(
    _: type LsmrDaYanSubscription,
    topologyPrefix: LsmrPath,
    minResolution = 1'u8,
    maxResolution = high(uint8),
    semanticPrefix = "",
    guardPrefix = "",
    acceptUrgent = true,
    acceptBackground = true,
): LsmrDaYanSubscription =
  LsmrDaYanSubscription(
    topologyPrefix: topologyPrefix,
    minResolution: minResolution,
    maxResolution: max(maxResolution, minResolution),
    semanticPrefix: semanticPrefix,
    guardPrefix: guardPrefix,
    acceptUrgent: acceptUrgent,
    acceptBackground: acceptBackground,
  )

proc init*(
    _: type LsmrDaYanPeer,
    peerId: PeerId,
    prefix: LsmrPath,
    subscriptions: seq[LsmrDaYanSubscription] = @[],
): LsmrDaYanPeer =
  LsmrDaYanPeer(peerId: peerId, prefix: prefix, subscriptions: subscriptions)

proc init*(
    _: type LsmrDaYanFlowConfig,
    regionalFanout = 3,
    globalFanout = 2,
    minGlobalResolution = 1'u8,
): LsmrDaYanFlowConfig =
  LsmrDaYanFlowConfig(
    regionalFanout: max(0, regionalFanout),
    globalFanout: max(0, globalFanout),
    minGlobalResolution: max(1'u8, minGlobalResolution),
  )

proc init*(
    _: type LsmrDaYanMessage,
    messageId: string,
    originPeerId: PeerId,
    topic: LsmrDaYanTopic,
    stage: LsmrDaYanStage,
    payloadKind: LsmrDaYanPayloadKind,
    resolution: uint8,
    payload: seq[byte] = @[],
    semanticFingerprint = "",
): LsmrDaYanMessage =
  let normalizedSemantic =
    if semanticFingerprint.len > 0:
      semanticFingerprint
    else:
      topic.semanticFingerprint
  LsmrDaYanMessage(
    messageId: messageId,
    originPeerId: originPeerId,
    topic: topic,
    stage: stage,
    mode: topic.mode,
    payloadKind: payloadKind,
    resolution: resolution,
    payload: payload,
    semanticFingerprint: normalizedSemantic,
  )

proc init*(
    _: type LsmrDaYanEnvelope,
    messageId: string,
    originPeerId: PeerId,
    topic: LsmrDaYanTopic,
    stage: LsmrDaYanStage,
    payloadKind: LsmrDaYanPayloadKind,
    resolution: uint8,
    payload: seq[byte] = @[],
    semanticFingerprint = "",
): LsmrDaYanEnvelope =
  let normalizedSemantic =
    if semanticFingerprint.len > 0:
      semanticFingerprint
    else:
      topic.semanticFingerprint
  LsmrDaYanEnvelope(
    messageId: messageId,
    originPeerId: originPeerId,
    topic: topic,
    stage: stage,
    mode: topic.mode,
    payloadKind: payloadKind,
    resolution: resolution,
    payload: payload,
    semanticFingerprint: normalizedSemantic,
  )

proc baguaKey*(
    coord: ObjectCoordinate, resolution: uint8, bagua: LsmrBagua
): LsmrBaguaKey =
  LsmrBaguaKey(
    topologyPath: coord.path,
    resolution: resolution,
    bagua: bagua,
    contentLsh: coord.contentLsh,
  )

proc baguaKeys*(
    coord: ObjectCoordinate, tier: LsmrStorageTier, resolution = 0'u8
): seq[LsmrBaguaKey] =
  let resolvedResolution =
    if resolution == 0'u8:
      uint8(max(1, int(coord.depth)))
    else:
      resolution
  for bagua in tier.storageBaguas():
    result.add(coord.baguaKey(resolvedResolution, bagua))

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

proc prefixesIntersect*(a, b: LsmrPath): bool =
  pathStartsWith(a, b) or pathStartsWith(b, a)

proc isValidLsmrDigit*(digit: uint8): bool =
  digit in {1'u8, 2'u8, 3'u8, 4'u8, 5'u8, 6'u8, 7'u8, 8'u8, 9'u8}

proc isValidLsmrPath*(path: LsmrPath): bool =
  for digit in path:
    if not isValidLsmrDigit(digit):
      return false
  true

proc luoShuCoord(digit: uint8): tuple[x: int, y: int] =
  case digit
  of 8'u8: (-1, -1)
  of 1'u8: (0, -1)
  of 6'u8: (1, -1)
  of 3'u8: (-1, 0)
  of 5'u8: (0, 0)
  of 7'u8: (1, 0)
  of 4'u8: (-1, 1)
  of 9'u8: (0, 1)
  of 2'u8: (1, 1)
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

proc effectivePrefix*(
    topic: LsmrDaYanTopic, resolution = 0'u8
): LsmrPath =
  let effectiveResolution =
    if resolution == 0'u8:
      topic.resolution
    else:
      resolution
  let takeCount = min(topic.center.len, int(effectiveResolution))
  if takeCount <= 0:
    return @[]
  topic.center[0 ..< takeCount]

proc isValid*(topic: LsmrDaYanTopic): bool =
  topic.resolution > 0'u8 and topic.semanticFingerprint.len > 0 and
    topic.center.isValidLsmrPath() and int(topic.resolution) <= topic.center.len

proc isValid*(subscription: LsmrDaYanSubscription): bool =
  subscription.topologyPrefix.isValidLsmrPath() and
    subscription.minResolution > 0'u8 and
    subscription.maxResolution >= subscription.minResolution and
    (subscription.acceptUrgent or subscription.acceptBackground)

proc isValid*(peer: LsmrDaYanPeer): bool =
  peer.peerId.data.len > 0 and peer.prefix.isValidLsmrPath() and
    peer.subscriptions.allIt(it.isValid())

proc allowsMode(
    subscription: LsmrDaYanSubscription, mode: LsmrDaYanMode
): bool =
  case mode
  of LsmrDaYanMode.ldymZhen:
    subscription.acceptUrgent
  of LsmrDaYanMode.ldymXun:
    subscription.acceptBackground

proc evaluate*(
    subscription: LsmrDaYanSubscription,
    topic: LsmrDaYanTopic,
    resolution = 0'u8,
): LsmrDaYanFilterResult =
  if not subscription.isValid() or not topic.isValid():
    return LsmrDaYanFilterResult.ldyfBlockTopology
  let effectiveResolution =
    if resolution == 0'u8:
      topic.resolution
    else:
      resolution
  if effectiveResolution < subscription.minResolution or
      effectiveResolution > subscription.maxResolution:
    return LsmrDaYanFilterResult.ldyfBlockResolution
  if not subscription.allowsMode(topic.mode):
    return LsmrDaYanFilterResult.ldyfBlockMode
  let prefix = topic.effectivePrefix(effectiveResolution)
  if prefix.len == 0 or
      not prefixesIntersect(prefix, subscription.topologyPrefix):
    return LsmrDaYanFilterResult.ldyfBlockTopology
  if subscription.semanticPrefix.len > 0 and
      not topic.semanticFingerprint.startsWith(subscription.semanticPrefix):
    return LsmrDaYanFilterResult.ldyfBlockSemantic
  if subscription.guardPrefix.len > 0 and
      not topic.guardDigest.startsWith(subscription.guardPrefix):
    return LsmrDaYanFilterResult.ldyfBlockGuard
  LsmrDaYanFilterResult.ldyfAccept

proc matches*(
    subscription: LsmrDaYanSubscription,
    topic: LsmrDaYanTopic,
    resolution = 0'u8,
): bool =
  subscription.evaluate(topic, resolution) == LsmrDaYanFilterResult.ldyfAccept

proc prefixDistance*(path, targetPrefix: LsmrPath): int =
  if path.len == 0 or targetPrefix.len == 0:
    return high(int)
  let count = min(path.len, targetPrefix.len)
  lsmrDistance(path[0 ..< count], targetPrefix[0 ..< count])

proc selectDaYanCandidates(
    peers: openArray[LsmrDaYanPeer],
    topic: LsmrDaYanTopic,
    resolution: uint8,
    exclude: Table[string, bool],
): seq[LsmrDaYanPeer] =
  let targetPrefix = topic.effectivePrefix(resolution)
  for peer in peers:
    if not peer.isValid():
      continue
    if exclude.getOrDefault($peer.peerId):
      continue
    if peer.subscriptions.anyIt(it.matches(topic, resolution)):
      result.add(peer)
  result.sort(proc(a, b: LsmrDaYanPeer): int =
    let sharedA = commonPrefixLen(a.prefix, targetPrefix)
    let sharedB = commonPrefixLen(b.prefix, targetPrefix)
    result = cmp(sharedB, sharedA)
    if result != 0:
      return result
    let distA = prefixDistance(a.prefix, targetPrefix)
    let distB = prefixDistance(b.prefix, targetPrefix)
    result = cmp(distA, distB)
    if result != 0:
      return result
    return cmp($a.peerId, $b.peerId)
  )

proc appendDaYanRoutes(
    routes: var seq[LsmrDaYanRoute],
    peers: openArray[LsmrDaYanPeer],
    topic: LsmrDaYanTopic,
    resolution: uint8,
    stage: LsmrDaYanStage,
    payloadKind: LsmrDaYanPayloadKind,
    limit: int,
    selected: var Table[string, bool],
) =
  if limit == 0:
    return
  let prefix = topic.effectivePrefix(resolution)
  let candidates = selectDaYanCandidates(peers, topic, resolution, selected)
  let capped =
    if limit < 0:
      candidates.len
    else:
      min(limit, candidates.len)
  for idx in 0 ..< capped:
    let peer = candidates[idx]
    selected[$peer.peerId] = true
    routes.add(LsmrDaYanRoute(
      peerId: peer.peerId,
      stage: stage,
      mode: topic.mode,
      payloadKind: payloadKind,
      resolution: resolution,
      topologyPrefix: prefix,
      distance: prefixDistance(peer.prefix, prefix),
      semanticFingerprint: topic.semanticFingerprint,
    ))

proc planDaYanFlow*(
    peers: openArray[LsmrDaYanPeer],
    topic: LsmrDaYanTopic,
    cfg = LsmrDaYanFlowConfig.init(),
    excludePeerId = none(PeerId),
): seq[LsmrDaYanRoute] =
  if not topic.isValid():
    return @[]
  var selected = initTable[string, bool]()
  if excludePeerId.isSome():
    selected[$excludePeerId.get()] = true

  let localPrefix = topic.effectivePrefix()
  var localPeers: seq[LsmrDaYanPeer] = @[]
  for peer in peers:
    if not peer.isValid():
      continue
    if selected.getOrDefault($peer.peerId):
      continue
    if not peer.subscriptions.anyIt(it.matches(topic, topic.resolution)):
      continue
    if commonPrefixLen(peer.prefix, localPrefix) >= int(topic.resolution):
      localPeers.add(peer)
  localPeers.sort(proc(a, b: LsmrDaYanPeer): int = cmp($a.peerId, $b.peerId))
  for peer in localPeers:
    selected[$peer.peerId] = true
    result.add(LsmrDaYanRoute(
      peerId: peer.peerId,
      stage: LsmrDaYanStage.ldysKunLocal,
      mode: topic.mode,
      payloadKind: LsmrDaYanPayloadKind.ldypkCsgDelta,
      resolution: topic.resolution,
      topologyPrefix: localPrefix,
      distance: 0,
      semanticFingerprint: topic.semanticFingerprint,
    ))

  if topic.resolution > 1'u8:
    let regionalResolution = topic.resolution - 1'u8
    result.appendDaYanRoutes(
      peers,
      topic,
      regionalResolution,
      LsmrDaYanStage.ldysDuiRegional,
      LsmrDaYanPayloadKind.ldypkCsgDelta,
      cfg.regionalFanout,
      selected,
    )

    var globalResolution = regionalResolution
    while globalResolution >= cfg.minGlobalResolution and globalResolution > 0'u8:
      result.appendDaYanRoutes(
        peers,
        topic,
        globalResolution,
        LsmrDaYanStage.ldysLiGlobal,
        LsmrDaYanPayloadKind.ldypkSemanticFingerprint,
        cfg.globalFanout,
        selected,
      )
      if globalResolution == cfg.minGlobalResolution or globalResolution == 1'u8:
        break
      dec globalResolution

proc planDaYanRelay*(
    peers: openArray[LsmrDaYanPeer],
    topic: LsmrDaYanTopic,
    stage: LsmrDaYanStage,
    resolution: uint8,
    cfg = LsmrDaYanFlowConfig.init(),
    excludePeerId = none(PeerId),
): seq[LsmrDaYanRoute] =
  if not topic.isValid() or resolution == 0'u8:
    return @[]

  var selected = initTable[string, bool]()
  if excludePeerId.isSome():
    selected[$excludePeerId.get()] = true

  case stage
  of LsmrDaYanStage.ldysKunLocal:
    if resolution > 1'u8:
      result.appendDaYanRoutes(
        peers,
        topic,
        resolution - 1'u8,
        LsmrDaYanStage.ldysDuiRegional,
        LsmrDaYanPayloadKind.ldypkCsgDelta,
        cfg.regionalFanout,
        selected,
      )
    elif resolution >= cfg.minGlobalResolution:
      result.appendDaYanRoutes(
        peers,
        topic,
        resolution,
        LsmrDaYanStage.ldysLiGlobal,
        LsmrDaYanPayloadKind.ldypkSemanticFingerprint,
        cfg.globalFanout,
        selected,
      )
  of LsmrDaYanStage.ldysDuiRegional:
    result.appendDaYanRoutes(
      peers,
      topic,
      resolution,
      LsmrDaYanStage.ldysLiGlobal,
      LsmrDaYanPayloadKind.ldypkSemanticFingerprint,
      cfg.globalFanout,
      selected,
    )
  of LsmrDaYanStage.ldysLiGlobal:
    if resolution > cfg.minGlobalResolution and resolution > 1'u8:
      result.appendDaYanRoutes(
        peers,
        topic,
        resolution - 1'u8,
        LsmrDaYanStage.ldysLiGlobal,
        LsmrDaYanPayloadKind.ldypkSemanticFingerprint,
        cfg.globalFanout,
        selected,
      )

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

proc digestMix(seed: uint64, value: uint64): uint64 =
  (seed shl 5) xor (seed shr 3) xor value xor 0x517C_C1B7_2722_0A95'u64

proc digestMix(seed: uint64, value: openArray[byte]): uint64 =
  result = seed xor 0xD6E8_FD9D_53E1_7D4B'u64 xor uint64(value.len)
  for idx, b in value:
    result = (result shl 7) xor (result shr 2) xor uint64((idx + 1) * int(b))

proc label*(bagua: LsmrBagua): string =
  case bagua
  of LsmrBagua.lbgQian:
    "qian"
  of LsmrBagua.lbgDui:
    "dui"
  of LsmrBagua.lbgLi:
    "li"
  of LsmrBagua.lbgZhen:
    "zhen"
  of LsmrBagua.lbgXun:
    "xun"
  of LsmrBagua.lbgKan:
    "kan"
  of LsmrBagua.lbgGen:
    "gen"
  of LsmrBagua.lbgKun:
    "kun"

proc init*(
    _: type LsmrBaguaKey,
    topologyPath: LsmrPath,
    resolution: uint8,
    bagua: LsmrBagua,
    contentLsh: string,
): LsmrBaguaKey =
  LsmrBaguaKey(
    topologyPath: topologyPath,
    resolution: resolution,
    bagua: bagua,
    contentLsh: contentLsh,
  )

proc init*(
    _: type LsmrBaguaCursor,
    topologyPrefix: LsmrPath = @[],
    resolution = none(uint8),
    bagua = none(LsmrBagua),
    contentPrefix = "",
): LsmrBaguaCursor =
  LsmrBaguaCursor(
    topologyPrefix: topologyPrefix,
    resolution: resolution,
    bagua: bagua,
    contentPrefix: contentPrefix,
  )

proc isValid*(key: LsmrBaguaKey): bool =
  key.resolution > 0'u8 and key.contentLsh.len > 0 and key.topologyPath.isValidLsmrPath()

proc isValid*(cursor: LsmrBaguaCursor): bool =
  if not cursor.topologyPrefix.isValidLsmrPath():
    return false
  if cursor.resolution.isSome() and cursor.resolution.get() == 0'u8:
    return false
  if cursor.bagua.isSome() and cursor.resolution.isNone():
    return false
  if cursor.contentPrefix.len > 0 and
      (cursor.resolution.isNone() or cursor.bagua.isNone()):
    return false
  true

proc cmp(a, b: LsmrBaguaKey): int =
  let count = min(a.topologyPath.len, b.topologyPath.len)
  for idx in 0 ..< count:
    result = cmp(int(a.topologyPath[idx]), int(b.topologyPath[idx]))
    if result != 0:
      return result
  result = cmp(a.topologyPath.len, b.topologyPath.len)
  if result != 0:
    return result
  result = cmp(int(a.resolution), int(b.resolution))
  if result != 0:
    return result
  result = cmp(ord(a.bagua), ord(b.bagua))
  if result != 0:
    return result
  result = cmp(a.contentLsh, b.contentLsh)

proc baguaResolutionLabel(resolution: uint8): string =
  "r:" & $resolution

proc baguaBranchLabel(bagua: LsmrBagua): string =
  "b:" & bagua.label()

proc topologySegmentLabel(digit: uint8): string =
  "t:" & $digit

proc contentSegmentLabel(ch: char): string =
  "c:" & int(ch.ord and 0xff).toHex(2)

proc keySegments(key: LsmrBaguaKey): seq[string] =
  result = newSeqOfCap[string](key.topologyPath.len + key.contentLsh.len + 2)
  for digit in key.topologyPath:
    result.add(topologySegmentLabel(digit))
  result.add(baguaResolutionLabel(key.resolution))
  result.add(baguaBranchLabel(key.bagua))
  for ch in key.contentLsh:
    result.add(contentSegmentLabel(ch))

proc cursorSegments(cursor: LsmrBaguaCursor): seq[string] =
  result = newSeqOfCap[string](cursor.topologyPrefix.len + cursor.contentPrefix.len + 2)
  for digit in cursor.topologyPrefix:
    result.add(topologySegmentLabel(digit))
  if cursor.resolution.isSome():
    result.add(baguaResolutionLabel(cursor.resolution.get()))
  if cursor.bagua.isSome():
    result.add(baguaBranchLabel(cursor.bagua.get()))
  for ch in cursor.contentPrefix:
    result.add(contentSegmentLabel(ch))

proc newBaguaPrefixNode(label: string): LsmrBaguaPrefixNode =
  new(result)
  result.label = label
  result.children = initTable[string, LsmrBaguaPrefixNode]()

proc computeBaguaLeafDigest*(leaf: LsmrBaguaLeaf): string =
  var acc = 0xC2B2_AE35_79B9_1D6D'u64
  acc = digestMix(acc, leaf.key.topologyPath)
  acc = digestMix(acc, uint64(leaf.key.resolution))
  acc = digestMix(acc, uint64(ord(leaf.key.bagua)))
  acc = digestMix(acc, leaf.key.contentLsh)
  acc = digestMix(acc, leaf.payload)
  acc = digestMix(acc, leaf.version)
  acc.toHex(16)

proc refreshBaguaNode(node: LsmrBaguaPrefixNode) =
  if node.isNil:
    return
  var acc = 0x6A09_E667_F3BC_C909'u64
  acc = digestMix(acc, node.label)
  node.leafCount = 0
  if node.hasLeaf:
    inc node.leafCount
    acc = digestMix(acc, node.leaf.digest)
  var labels = toSeq(node.children.keys)
  labels.sort(system.cmp[string])
  for label in labels:
    let child = node.children.getOrDefault(label)
    if child.isNil:
      continue
    node.leafCount += child.leafCount
    acc = digestMix(acc, label)
    acc = digestMix(acc, child.digest)
  node.digest = acc.toHex(16)

proc ensureBaguaRoot(tree: var LsmrBaguaPrefixTree) =
  if tree.root.isNil:
    tree.root = newBaguaPrefixNode("root")
    refreshBaguaNode(tree.root)

proc init*(_: type LsmrBaguaPrefixTree): LsmrBaguaPrefixTree =
  result.ensureBaguaRoot()

proc len*(tree: LsmrBaguaPrefixTree): int =
  if tree.root.isNil:
    return 0
  tree.root.leafCount

proc rootDigest*(tree: LsmrBaguaPrefixTree): string =
  if tree.root.isNil:
    return LsmrBaguaPrefixTree.init().rootDigest()
  tree.root.digest

proc resolveNode(tree: LsmrBaguaPrefixTree, segments: openArray[string]): LsmrBaguaPrefixNode =
  if tree.root.isNil:
    return nil
  result = tree.root
  for label in segments:
    if not result.children.contains(label):
      return nil
    result = result.children.getOrDefault(label)
    if result.isNil:
      return nil

proc contains*(tree: LsmrBaguaPrefixTree, key: LsmrBaguaKey): bool =
  if not key.isValid():
    return false
  let node = tree.resolveNode(key.keySegments())
  not node.isNil and node.hasLeaf

proc get*(tree: LsmrBaguaPrefixTree, key: LsmrBaguaKey): Option[LsmrBaguaLeaf] =
  if not key.isValid():
    return none(LsmrBaguaLeaf)
  let node = tree.resolveNode(key.keySegments())
  if node.isNil or not node.hasLeaf:
    return none(LsmrBaguaLeaf)
  some(node.leaf)

proc put*(
    tree: var LsmrBaguaPrefixTree,
    key: LsmrBaguaKey,
    payload: openArray[byte],
    version = 0'u64,
) =
  if not key.isValid():
    return
  tree.ensureBaguaRoot()
  let segments = key.keySegments()
  var stack = @[tree.root]
  var node = tree.root
  for label in segments:
    var child = node.children.getOrDefault(label)
    if child.isNil:
      child = newBaguaPrefixNode(label)
      node.children[label] = child
    node = child
    stack.add(node)
  node.hasLeaf = true
  node.leaf = LsmrBaguaLeaf(
    key: key,
    payload: @payload,
    version: version,
    digest: "",
  )
  node.leaf.digest = computeBaguaLeafDigest(node.leaf)
  for idx in countdown(stack.high, 0):
    refreshBaguaNode(stack[idx])

proc delete*(tree: var LsmrBaguaPrefixTree, key: LsmrBaguaKey): bool =
  if not key.isValid() or tree.root.isNil:
    return false
  let segments = key.keySegments()
  var nodes = @[tree.root]
  var labels: seq[string] = @[]
  var node = tree.root
  for label in segments:
    if not node.children.contains(label):
      return false
    labels.add(label)
    node = node.children.getOrDefault(label)
    if node.isNil:
      return false
    nodes.add(node)
  if not node.hasLeaf:
    return false
  node.hasLeaf = false
  node.leaf = default(LsmrBaguaLeaf)
  for idx in countdown(nodes.high, 1):
    let current = nodes[idx]
    refreshBaguaNode(current)
    let parent = nodes[idx - 1]
    let label = labels[idx - 1]
    if current.leafCount == 0:
      parent.children.del(label)
    refreshBaguaNode(parent)
  true

proc collectLeaves(node: LsmrBaguaPrefixNode, acc: var seq[LsmrBaguaLeaf]) =
  if node.isNil:
    return
  if node.hasLeaf:
    acc.add(node.leaf)
  var labels = toSeq(node.children.keys)
  labels.sort(system.cmp[string])
  for label in labels:
    collectLeaves(node.children.getOrDefault(label), acc)

proc items*(tree: LsmrBaguaPrefixTree): seq[LsmrBaguaLeaf] =
  if tree.root.isNil:
    return @[]
  collectLeaves(tree.root, result)
  result.sort(proc(a, b: LsmrBaguaLeaf): int = cmp(a.key, b.key))

proc prefixDigest*(tree: LsmrBaguaPrefixTree, cursor: LsmrBaguaCursor): string =
  if not cursor.isValid():
    return ""
  let node = tree.resolveNode(cursor.cursorSegments())
  if node.isNil:
    return ""
  node.digest

proc prefixLeaves*(tree: LsmrBaguaPrefixTree, cursor: LsmrBaguaCursor): seq[LsmrBaguaLeaf] =
  if not cursor.isValid():
    return @[]
  let node = tree.resolveNode(cursor.cursorSegments())
  if node.isNil:
    return @[]
  collectLeaves(node, result)
  result.sort(proc(a, b: LsmrBaguaLeaf): int = cmp(a.key, b.key))

proc appendMissingBaguaLeaves(
    node: LsmrBaguaPrefixNode,
    kind: LsmrBaguaDiffKind,
    result: var seq[LsmrBaguaDiff],
) =
  if node.isNil:
    return
  var leaves: seq[LsmrBaguaLeaf] = @[]
  collectLeaves(node, leaves)
  for leaf in leaves:
    result.add(
      LsmrBaguaDiff(
        key: leaf.key,
        kind: kind,
        localDigest: if kind == LsmrBaguaDiffKind.lbdMissingLocal: "" else: leaf.digest,
        remoteDigest: if kind == LsmrBaguaDiffKind.lbdMissingRemote: "" else: leaf.digest,
      )
    )

proc diffBaguaNodes(
    localNode, remoteNode: LsmrBaguaPrefixNode, result: var seq[LsmrBaguaDiff]
) =
  if localNode.isNil and remoteNode.isNil:
    return
  if localNode.isNil:
    appendMissingBaguaLeaves(remoteNode, LsmrBaguaDiffKind.lbdMissingLocal, result)
    return
  if remoteNode.isNil:
    appendMissingBaguaLeaves(localNode, LsmrBaguaDiffKind.lbdMissingRemote, result)
    return
  if localNode.digest == remoteNode.digest:
    return
  if localNode.hasLeaf or remoteNode.hasLeaf:
    if localNode.hasLeaf and remoteNode.hasLeaf:
      if localNode.leaf.digest != remoteNode.leaf.digest:
        result.add(
          LsmrBaguaDiff(
            key: localNode.leaf.key,
            kind: LsmrBaguaDiffKind.lbdPayloadMismatch,
            localDigest: localNode.leaf.digest,
            remoteDigest: remoteNode.leaf.digest,
          )
        )
    elif localNode.hasLeaf:
      result.add(
        LsmrBaguaDiff(
          key: localNode.leaf.key,
          kind: LsmrBaguaDiffKind.lbdMissingRemote,
          localDigest: localNode.leaf.digest,
          remoteDigest: "",
        )
      )
    else:
      result.add(
        LsmrBaguaDiff(
          key: remoteNode.leaf.key,
          kind: LsmrBaguaDiffKind.lbdMissingLocal,
          localDigest: "",
          remoteDigest: remoteNode.leaf.digest,
        )
      )
  var labels = toSeq(localNode.children.keys)
  for label in remoteNode.children.keys:
    if label notin labels:
      labels.add(label)
  labels.sort(system.cmp[string])
  var uniqueLabels: seq[string] = @[]
  for label in labels:
    if uniqueLabels.len == 0 or uniqueLabels[^1] != label:
      uniqueLabels.add(label)
  for label in uniqueLabels:
    diffBaguaNodes(
      if localNode.children.contains(label): localNode.children.getOrDefault(label) else: nil,
      if remoteNode.children.contains(label): remoteNode.children.getOrDefault(label) else: nil,
      result,
    )

proc diff*(localTree, remoteTree: LsmrBaguaPrefixTree): seq[LsmrBaguaDiff] =
  diffBaguaNodes(localTree.root, remoteTree.root, result)
  result.sort(proc(a, b: LsmrBaguaDiff): int = cmp(a.key, b.key))

proc putObject*(
    tree: var LsmrBaguaPrefixTree,
    coord: ObjectCoordinate,
    tier: LsmrStorageTier,
    payload: openArray[byte],
    version = 0'u64,
    resolution = 0'u8,
): seq[LsmrBaguaKey] =
  let keys = coord.baguaKeys(tier, resolution)
  for key in keys:
    tree.put(key, payload, version)
    result.add(key)

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
