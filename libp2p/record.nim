# Nim-Libp2P
# Generic libp2p record (DHT style key/value with optional signature)

{.push raises: [].}

import std/[tables, options, strutils, times, math, sets]
import pkg/results
import stew/byteutils

import peerid, crypto/crypto, protobuf/minprotobuf, routing_record, utility

export peerid

const
  DefaultIpnsRefreshWeight* = 1.0

type
  Record* = object
    key*: string
    value*: seq[byte]
    author*: PeerId
    signature*: seq[byte]
    timeReceived*: string

  RecordValidator* = proc(key: string, value: seq[byte]): bool {.gcsafe, raises: [].}

  IpnsCacheEntry* = object
    sequence*: Option[uint64]
    validUntil*: DateTime
    cacheUntil*: DateTime
    rawKey*: seq[byte]
    record*: seq[byte]
    lastRefresh*: DateTime
    namespace*: string

  IprsCacheEntry* = object
    peerId*: PeerId
    namespace*: string
    seqNo*: uint64
    record*: SignedPeerRecord
    raw*: seq[byte]
    updatedAt*: DateTime
    expiresAt*: Option[DateTime]

  IprsNamespacePolicy* = object
    ## Governs `/iprs/<namespace>/<peerId>` entries.
    acceptRecords*: bool
    allowUnknownPeers*: bool
    allowedPeers*: HashSet[PeerId]
    maxRecordAge*: Option[Duration]
  IpnsNamespacePolicy* = object
    ## Behavioural policy that applies to `/ipns/<namespace>/<name>` records.
    ## When `namespace` is empty, the policy governs root IPNS keys.
    requirePeerId*: bool
    requirePublicKey*: bool
    maxRecordLifetime*: Option[Duration]
    maxCacheDuration*: Option[Duration]
    refreshWeight*: float64

  RecordStore* = object
    validators*: Table[string, RecordValidator]
    ipnsRecords*: Table[string, IpnsCacheEntry]
    iprsRecords*: Table[string, IprsCacheEntry]
    ipnsNamespaces*: Table[string, IpnsNamespacePolicy]
    defaultIpnsPolicy*: IpnsNamespacePolicy
    iprsNamespaces*: Table[string, IprsNamespacePolicy]
    defaultIprsPolicy*: IprsNamespacePolicy
    acceptUnregisteredIprsNamespaces*: bool

proc validateIpnsRefreshWeight*(weight: float64) =
  let cls = classify(weight)
  if weight <= 0.0 or cls notin {fcZero, fcSubnormal, fcNormal}:
    raise newException(Defect, "IPNS refreshWeight must be a finite value greater than zero")

proc defaultIpnsPolicy(): IpnsNamespacePolicy =
  IpnsNamespacePolicy(
    requirePeerId: false,
    requirePublicKey: false,
    maxRecordLifetime: none(times.Duration),
    maxCacheDuration: none(times.Duration),
    refreshWeight: DefaultIpnsRefreshWeight,
  )

proc defaultIprsNamespacePolicy(): IprsNamespacePolicy =
  IprsNamespacePolicy(
    acceptRecords: true,
    allowUnknownPeers: true,
    allowedPeers: initHashSet[PeerId](),
    maxRecordAge: none(times.Duration),
  )

proc init*(
    _: typedesc[IprsNamespacePolicy],
    acceptRecords: bool = true,
    allowUnknownPeers: bool = true,
    maxRecordAge: Option[Duration] = none(times.Duration),
    allowedPeers: seq[PeerId] = @[],
): IprsNamespacePolicy =
  var set = initHashSet[PeerId]()
  for peer in allowedPeers:
    set.incl(peer)
  IprsNamespacePolicy(
    acceptRecords: acceptRecords,
    allowUnknownPeers: allowUnknownPeers,
    allowedPeers: set,
    maxRecordAge: maxRecordAge,
  )

proc normalizePath(path: string): string =
  var start = 0
  var finish = path.len
  while start < finish and path[start] == '/':
    inc start
  while finish > start and path[finish - 1] == '/':
    dec finish
  if start >= finish:
    return ""
  path[start ..< finish]

proc bytesToString(data: seq[byte]): string =
  result = newString(data.len)
  for i, b in data:
    result[i] = char(b)

proc stringToBytes(data: string): seq[byte] =
  result = newSeq[byte](data.len)
  for i, c in data:
    result[i] = byte(c)

proc dataForSigning(rec: Record): seq[byte] =
  var buffer = newSeq[byte]()
  buffer.add(rec.key.toBytes())
  buffer.add(rec.value)
  if rec.author.data.len > 0:
    buffer.add(rec.author.data)
  buffer

proc encode*(rec: Record): seq[byte] =
  var pb = initProtoBuffer()
  pb.write(1, rec.key)
  pb.write(2, rec.value)
  if rec.author.data.len > 0:
    pb.write(3, rec.author)
  if rec.signature.len > 0:
    pb.write(4, rec.signature)
  if rec.timeReceived.len > 0:
    pb.write(5, rec.timeReceived)
  result = pb.toBytes()

proc decode*(_: typedesc[Record], data: seq[byte]): Result[Record, ProtoError] =
  var pb = initProtoBuffer(data)
  var rec = Record()
  ?pb.getRequiredField(1, rec.key)
  ?pb.getRequiredField(2, rec.value)
  discard ?pb.getField(3, rec.author)
  discard ?pb.getField(4, rec.signature)
  discard ?pb.getField(5, rec.timeReceived)
  ok(rec)

proc sign*(rec: var Record, priv: PrivateKey): Result[void, CryptoError] =
  if rec.author.data.len == 0:
    rec.author = ?PeerId.init(priv).orError(CryptoError.SchemeError)
  let signature = ?priv.sign(dataForSigning(rec))
  rec.signature = signature.getBytes()
  ok()

proc verifySignature*(rec: Record): bool =
  if rec.signature.len == 0:
    return true
  var pub: PublicKey
  if not rec.author.extractPublicKey(pub):
    return false
  var signature: Signature
  if not signature.init(rec.signature):
    return false
  signature.verify(dataForSigning(rec), pub)

proc prepareRecord*(
    key: string,
    value: seq[byte],
    priv: PrivateKey,
    timeReceived = "",
): Result[Record, CryptoError] =
  var rec = Record(key: key, value: value, timeReceived: timeReceived)
  ?rec.sign(priv)
  ok(rec)

proc registerValidator*(store: var RecordStore, prefix: string, validator: RecordValidator) =
  let normalized = normalizePath(prefix)
  store.validators[normalized] = validator

proc unregisterValidator*(store: var RecordStore, prefix: string) =
  let normalized = normalizePath(prefix)
  store.validators.del(normalized)

proc registerPeerRecordValidator*(store: var RecordStore) =
  store.registerValidator("libp2p/peer-record", proc(key: string, value: seq[byte]): bool =
    let normalized = normalizePath(key)
    let parts = normalized.split('/')
    if parts.len < 2 or parts[0] != "libp2p" or parts[1] != "peer-record":
      return false

    let signed = SignedPeerRecord.decode(value).valueOr:
      return false
    if signed.checkValid().isErr:
      return false

    if parts.len >= 3 and parts[2].len > 0 and parts[2] != $signed.data.peerId:
      return false
    true
  )

type
  IpnsValidityType = enum
    ipnsUnknown,
    ipnsEol,
    ipnsTtl

  IpnsRecord = object
    value: seq[byte]
    signature: seq[byte]
    validity: seq[byte]
    validityType: IpnsValidityType
    validityTypeStr: string
    sequence: Option[uint64]
    ttl: Option[Duration]
    publicKey: Option[PublicKey]

proc ipnsDataForSignature(value, validity: seq[byte], validityType: string): seq[byte] =
  value & validity & stringToBytes(validityType)

proc parseEol(validity: string): Option[DateTime] =
  const formats = [
    "yyyy-MM-dd'T'HH:mm:sszzz",
    "yyyy-MM-dd'T'HH:mm:ss'.'fffzzz",
    "yyyy-MM-dd'T'HH:mm:ss'.'ffffffzzz"
  ]
  for fmt in formats:
      try:
        let dt = parse(validity, fmt, utc())
        return options.some(dt)
      except TimeParseError, TimeFormatParseError:
        discard
  return none(DateTime)

proc decodeIpnsRecord(data: seq[byte]): Result[IpnsRecord, string] =
  var pb = initProtoBuffer(data)
  var record = IpnsRecord(validityType: ipnsUnknown)

  while true:
    let field = pb.readFieldNumber()
    if field == 0'u:
      break
    case field
    of 1'u:
      record.value = pb.readBytes()
    of 2'u:
      record.signature = pb.readBytes()
    of 4'u:
      record.validityTypeStr = pb.readString()
      case record.validityTypeStr.toUpperAscii()
      of "EOL":
        record.validityType = ipnsEol
      of "TTL":
        record.validityType = ipnsTtl
      else:
        record.validityType = ipnsUnknown
    of 5'u:
      record.validity = pb.readBytes()
    of 6'u:
      let seqStr = pb.readString()
      try:
        record.sequence = some(uint64(parseBiggestUInt(seqStr)))
      except ValueError:
        return err("invalid ipns sequence")
    of 7'u:
      let ttlStr = pb.readString()
      if ttlStr.len > 0:
        try:
          let ttlNs = parseBiggestUInt(ttlStr)
          record.ttl = options.some(initDuration(nanoseconds = int64(ttlNs)))
        except ValueError:
          return err("invalid ipns ttl")
    of 8'u:
      let pkBytes = pb.readBytes()
      let pubRes = PublicKey.init(pkBytes)
      if pubRes.isErr:
        return err("invalid ipns public key")
      record.publicKey = some(pubRes.get())
    else:
      pb.skipValue()

  if record.value.len == 0 or record.signature.len == 0 or
      record.validityType == ipnsUnknown:
    return err("missing required ipns fields")

  if record.validityType == ipnsEol and record.validity.len == 0:
    return err("missing ipns validity for EOL")

  ok(record)

proc ipnsValidUntil(entry: IpnsRecord, current: DateTime): Option[DateTime] =
  case entry.validityType
  of ipnsEol:
    let eolStr = bytesToString(entry.validity)
    let expiryOpt = parseEol(eolStr)
    if expiryOpt.isNone():
      return none(DateTime)
    let expiry = expiryOpt.get()
    if expiry <= current:
      return none(DateTime)
    return some(expiry)
  of ipnsTtl:
    let ttlStr = bytesToString(entry.validity)
    try:
      let ttlNs = parseBiggestUInt(ttlStr)
      if ttlNs > uint64(high(int64)):
        return none(DateTime)
      let ttlDur = initDuration(nanoseconds = int64(ttlNs))
      let expiry = current + ttlDur
      if expiry <= current:
        return none(DateTime)
      return some(expiry)
    except ValueError, OverflowError:
      return none(DateTime)
  else:
    none(DateTime)

proc ipnsCacheDeadline(
    entry: IpnsRecord, validUntil: DateTime, current: DateTime
): DateTime =
  var deadline = validUntil
  entry.ttl.withValue(ttlDur):
    if ttlDur <= initDuration(nanoseconds = 0):
      return current
    let ttlExpiry = current + ttlDur
    if ttlExpiry < deadline:
      deadline = ttlExpiry
  deadline

proc acceptIpnsEntry(
    store: var RecordStore,
    normalizedKey: string,
    namespace: string,
    entry: IpnsRecord,
    validUntil: DateTime,
    cacheUntil: DateTime,
    current: DateTime,
    rawKey: seq[byte],
    recordValue: seq[byte],
): bool =
  let newSeq = entry.sequence
  let newSeqValue = if newSeq.isSome: newSeq.get() else: 0'u64

  var existingActive = false
  var existing: IpnsCacheEntry

  store.ipnsRecords.withValue(normalizedKey, cached):
    if cached[].cacheUntil > current:
      existing = cached[]
      existingActive = true
    else:
      store.ipnsRecords.del(normalizedKey)

  if existingActive:
    let existingSeq = existing.sequence
    let existingSeqValue = if existingSeq.isSome: existingSeq.get() else: 0'u64

    if newSeq.isSome and existingSeq.isSome:
      if newSeqValue < existingSeqValue:
        return false
      elif newSeqValue == existingSeqValue and
          validUntil <= existing.validUntil and cacheUntil <= existing.cacheUntil:
        return false
    elif newSeq.isSome and not existingSeq.isSome:
      discard
    elif not newSeq.isSome and existingSeq.isSome:
      return false
    else:
      if validUntil <= existing.validUntil and cacheUntil <= existing.cacheUntil:
        return false

  let storedRawKey =
    if rawKey.len > 0:
      rawKey
    elif existingActive:
      existing.rawKey
    else:
      rawKey
  let storedRecord =
    if recordValue.len > 0:
      recordValue
    elif existingActive:
      existing.record
    else:
      recordValue

  store.ipnsRecords[normalizedKey] = IpnsCacheEntry(
    sequence: entry.sequence,
    validUntil: validUntil,
    cacheUntil: cacheUntil,
    rawKey: storedRawKey,
    record: storedRecord,
    lastRefresh: current,
    namespace: namespace,
  )
  true

proc registerIpnsNamespace*(store: var RecordStore, namespace: string, policy: IpnsNamespacePolicy) =
  ## Register or replace a policy for a custom IPNS namespace.
  ## Empty namespace updates the root/default policy.
  validateIpnsRefreshWeight(policy.refreshWeight)
  let normalized = normalizePath(namespace)
  if normalized.len == 0:
    store.defaultIpnsPolicy = policy
    return
  store.ipnsNamespaces[normalized] = policy

proc unregisterIpnsNamespace*(store: var RecordStore, namespace: string) =
  ## Remove a custom namespace policy.
  let normalized = normalizePath(namespace)
  if normalized.len == 0:
    store.defaultIpnsPolicy = defaultIpnsPolicy()
    return
  store.ipnsNamespaces.del(normalized)

proc ipnsPolicy(store: RecordStore, namespace: string): Option[IpnsNamespacePolicy] =
  if namespace.len == 0:
    return some(store.defaultIpnsPolicy)
  store.ipnsNamespaces.withValue(namespace, policy):
    return some(policy)
  none(IpnsNamespacePolicy)

proc getIpnsNamespacePolicy*(store: RecordStore, namespace: string): Option[IpnsNamespacePolicy] =
  ## Retrieve the effective policy for a namespace; empty string yields the default policy.
  let normalized = normalizePath(namespace)
  store.ipnsPolicy(normalized)

proc iprsPolicy(store: RecordStore, namespace: string): Option[IprsNamespacePolicy] =
  let normalized = normalizePath(namespace)
  if normalized.len == 0:
    return some(store.defaultIprsPolicy)
  store.iprsNamespaces.withValue(normalized, policy):
    return some(policy)
  if store.acceptUnregisteredIprsNamespaces:
    return some(store.defaultIprsPolicy)
  none(IprsNamespacePolicy)

proc registerIprsNamespace*(
    store: var RecordStore, namespace: string, policy: IprsNamespacePolicy
) =
  let normalized = normalizePath(namespace)
  if normalized.len == 0:
    store.defaultIprsPolicy = policy
  else:
    store.iprsNamespaces[normalized] = policy

proc unregisterIprsNamespace*(store: var RecordStore, namespace: string) =
  let normalized = normalizePath(namespace)
  if normalized.len == 0:
    store.defaultIprsPolicy = defaultIprsNamespacePolicy()
  else:
    store.iprsNamespaces.del(normalized)

proc setIprsAcceptUnregisteredNamespaces*(
    store: var RecordStore, accept: bool
) =
  store.acceptUnregisteredIprsNamespaces = accept

proc getIprsNamespacePolicy*(
    store: RecordStore, namespace: string
): Option[IprsNamespacePolicy] =
  let normalized = normalizePath(namespace)
  store.iprsPolicy(normalized)

proc addIprsAllowedPeer*(
    store: var RecordStore, namespace: string, peerId: PeerId
) =
  let normalized = normalizePath(namespace)
  if normalized.len == 0:
    store.defaultIprsPolicy.allowedPeers.incl(peerId)
    return
  withMapEntry(
    store.iprsNamespaces, normalized, defaultIprsNamespacePolicy(), entry
  ):
    entry.allowedPeers.incl(peerId)

proc removeIprsAllowedPeer*(
    store: var RecordStore, namespace: string, peerId: PeerId
) =
  let normalized = normalizePath(namespace)
  if normalized.len == 0:
    store.defaultIprsPolicy.allowedPeers.excl(peerId)
    return
  store.iprsNamespaces.withValue(normalized, policy):
    policy[].allowedPeers.excl(peerId)

proc setIprsAllowUnknownPeers*(
    store: var RecordStore, namespace: string, allow: bool
) =
  let normalized = normalizePath(namespace)
  if normalized.len == 0:
    store.defaultIprsPolicy.allowUnknownPeers = allow
    return
  withMapEntry(
    store.iprsNamespaces, normalized, defaultIprsNamespacePolicy(), entry
  ):
    entry.allowUnknownPeers = allow

proc setIprsMaxRecordAge*(
    store: var RecordStore, namespace: string, maxAge: Option[Duration]
) =
  let normalized = normalizePath(namespace)
  if normalized.len == 0:
    store.defaultIprsPolicy.maxRecordAge = maxAge
    return
  withMapEntry(
    store.iprsNamespaces, normalized, defaultIprsNamespacePolicy(), entry
  ):
    entry.maxRecordAge = maxAge

proc registerIpnsValidator*(store: var RecordStore) =
  store.registerValidator("ipns", proc(key: string, value: seq[byte]): bool =
    let normalized = normalizePath(key)
    let parts = normalized.split('/')
    if parts.len < 2 or parts[0] != "ipns":
      return false

    let namespace =
      if parts.len > 2:
        parts[1 ..< parts.len - 1].join("/")
      else:
        ""

    let policyOpt = store.ipnsPolicy(namespace)
    if policyOpt.isNone():
      return false
    let policy = policyOpt.get()

    let entryRes = decodeIpnsRecord(value)
    if entryRes.isErr:
      return false
    let entry = entryRes.get()

    var pubKey: PublicKey
    var hasPubKey = false
    var hasEmbeddedPub = false

    if entry.publicKey.isSome:
      pubKey = entry.publicKey.get()
      hasPubKey = true
      hasEmbeddedPub = true

    let name = parts[^1]
    let peerIdRes = PeerId.init(name)
    if peerIdRes.isOk:
      let pid = peerIdRes.get()
      var derived: PublicKey
      if pid.extractPublicKey(derived):
        if hasEmbeddedPub and not pid.match(pubKey):
          return false
        pubKey = derived
        hasPubKey = true
      elif not hasEmbeddedPub:
        return false
    else:
      if policy.requirePeerId:
        return false
      if not hasPubKey:
        return false

    if policy.requirePublicKey and not hasEmbeddedPub:
      return false

    if not hasPubKey:
      return false

    var sig: Signature
    if not sig.init(entry.signature):
      return false

    let sigData = ipnsDataForSignature(entry.value, entry.validity, entry.validityTypeStr)
    if not sig.verify(sigData, pubKey):
      return false

    let current = now().inZone(utc())
    let validUntilOpt = ipnsValidUntil(entry, current)
    if validUntilOpt.isNone():
      return false
    var validUntil = validUntilOpt.get()
    policy.maxRecordLifetime.withValue(limit):
      let maxExpiry = current + limit
      if validUntil > maxExpiry:
        validUntil = maxExpiry
    if validUntil <= current:
      return false

    var cacheUntil = ipnsCacheDeadline(entry, validUntil, current)
    policy.maxCacheDuration.withValue(limit):
      let maxCache = current + limit
      if cacheUntil > maxCache:
        cacheUntil = maxCache

    if cacheUntil > validUntil:
      cacheUntil = validUntil
    if cacheUntil <= current:
      return false

    let rawKey = stringToBytes(key)

    if not store.acceptIpnsEntry(
        normalized, namespace, entry, validUntil, cacheUntil, current, rawKey, value
      ):
      return false

    true
  )

proc registerIprsValidator*(store: var RecordStore) =
  store.registerValidator("iprs", proc(key: string, value: seq[byte]): bool =
    let normalized = normalizePath(key)
    let parts = normalized.split('/')
    if parts.len < 2 or parts[0] != "iprs":
      return false

    let peerIdStr = parts[^1]
    let peerIdRes = PeerId.init(peerIdStr)
    if peerIdRes.isErr:
      return false
    let peerId = peerIdRes.get()
    var namespace =
      if parts.len > 2:
        parts[1 ..< parts.len - 1].join("/")
      else:
        ""
    namespace = normalizePath(namespace)

    let policyOpt = store.iprsPolicy(namespace)
    if policyOpt.isNone():
      return false
    let policy = policyOpt.get()
    if not policy.acceptRecords:
      return false
    let allowedPeer = policy.allowedPeers.contains(peerId)
    if not policy.allowUnknownPeers and not allowedPeer:
      return false

    let sprRes = SignedPeerRecord.decode(value)
    if sprRes.isErr:
      return false
    let signedRecord = sprRes.get()
    if signedRecord.data.peerId != peerId:
      return false

    let newSeq = signedRecord.data.seqNo
    let current = now().inZone(utc())
    var expiresAt = none(DateTime)
    policy.maxRecordAge.withValue(limit):
      expiresAt = some(current + limit)

    var shouldUpdate = true
    store.iprsRecords.withValue(normalized, cached):
      if newSeq < cached[].seqNo:
        return false
      if newSeq == cached[].seqNo:
        if cached[].raw == value:
          shouldUpdate = false
        else:
          return false

    if shouldUpdate:
      store.iprsRecords[normalized] = IprsCacheEntry(
        peerId: peerId,
        namespace: namespace,
        seqNo: newSeq,
        record: signedRecord,
        raw: value,
        updatedAt: current,
        expiresAt: expiresAt,
      )
    else:
      store.iprsRecords.withValue(normalized, cached):
        cached[].updatedAt = current
        cached[].expiresAt = expiresAt
    true
  )

proc registerPkValidator*(store: var RecordStore) =
  store.registerValidator("pk", proc(key: string, value: seq[byte]): bool =
    let normalized = normalizePath(key)
    let parts = normalized.split('/')
    if parts.len != 2 or parts[0] != "pk":
      return false

    let peerIdRes = PeerId.init(parts[1])
    if peerIdRes.isErr:
      return false
    let pid = peerIdRes.get()

    var pubKey: PublicKey
    if not pubKey.init(value):
      return false

    if not pid.match(pubKey):
      return false

    true
  )

proc getIpnsRecord*(store: RecordStore, key: string): Option[IpnsCacheEntry] =
  let normalized = normalizePath(key)
  var entryOpt = none(IpnsCacheEntry)
  store.ipnsRecords.withValue(normalized, cached):
    entryOpt = some(cached)
  entryOpt

proc getIprsRecord*(store: RecordStore, key: string): Option[IprsCacheEntry] =
  let normalized = normalizePath(key)
  if not store.iprsRecords.hasKey(normalized):
    return none(IprsCacheEntry)
  let current = now().inZone(utc())
  let entry = store.iprsRecords.getOrDefault(normalized, IprsCacheEntry())
  if entry.expiresAt.isSome and entry.expiresAt.get() <= current:
    return none(IprsCacheEntry)
  some(entry)

proc getIprsRecordByPeer*(store: RecordStore, peerId: PeerId): Option[IprsCacheEntry] =
  let current = now().inZone(utc())
  for entry in store.iprsRecords.values:
    if entry.expiresAt.isSome and entry.expiresAt.get() <= current:
      continue
    if entry.peerId == peerId:
      return some(entry)
  none(IprsCacheEntry)

proc revokeIprsRecord*(
    store: var RecordStore, namespace: string, peerId: PeerId
): bool =
  let normalizedNamespace = normalizePath(namespace)
  var key = "iprs"
  if normalizedNamespace.len > 0:
    key.add('/')
    key.add(normalizedNamespace)
  key.add('/')
  key.add($peerId)
  if store.iprsRecords.hasKey(key):
    store.iprsRecords.del(key)
    return true
  false

proc pruneIprsRecords*(
    store: var RecordStore, current: DateTime = now().inZone(utc())
) =
  var expired: seq[string] = @[]
  for key, entry in store.iprsRecords.pairs:
    if entry.expiresAt.isSome and entry.expiresAt.get() <= current:
      expired.add(key)
  for key in expired:
    store.iprsRecords.del(key)

proc init*(_: typedesc[RecordStore]): RecordStore =
  var store = RecordStore(
    validators: initTable[string, RecordValidator](),
    ipnsRecords: initTable[string, IpnsCacheEntry](),
    iprsRecords: initTable[string, IprsCacheEntry](),
    ipnsNamespaces: initTable[string, IpnsNamespacePolicy](),
    defaultIpnsPolicy: defaultIpnsPolicy(),
    iprsNamespaces: initTable[string, IprsNamespacePolicy](),
    defaultIprsPolicy: defaultIprsNamespacePolicy(),
    acceptUnregisteredIprsNamespaces: true,
  )
  store.registerPeerRecordValidator()
  store.registerIpnsValidator()
  store.registerIprsValidator()
  store.registerPkValidator()
  store

proc validate*(store: RecordStore, rec: Record): bool =
  if not rec.verifySignature():
    return false
  let normalizedKey = normalizePath(rec.key)

  var candidate = normalizedKey
  while true:
    if store.validators.contains(candidate):
      store.validators.withValue(candidate, validator):
        return validator(rec.key, rec.value)
    let idx = candidate.rfind('/')
    if idx == -1:
      break
    candidate = candidate[0 ..< idx]

  if store.validators.contains(""):
    store.validators.withValue("", validator):
      return validator(rec.key, rec.value)

  true
